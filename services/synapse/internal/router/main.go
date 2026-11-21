package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/discord"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/registry"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

var (
	_ discord.IngressServer    = (*RouterServer)(nil)
	_ flux.RolloutSink         = (*RouterServer)(nil)
	_ buildmeta.GitHubGetter   = (*RouterServer)(nil)
	_ model.OutboundDispatcher = (*DefaultOutboundDispatcher)(nil)
)

type BotConfig = config.BotConfig

// RouterServer handles webhook requests and orchestrates secret synchronization.
type RouterServer struct {
	cfgSrc                   ConfigSource
	httpClient               *http.Client
	githubClient             *http.Client
	bus                      pubsub.Bus
	registry                 PRRegistryUpdater
	dispatcher               OutboundDispatcher
	onProcessed              func(event, delivery string)
	queueStats               ConsumerStatsQuerier
	gatewayLatencyFn         func() time.Duration
	gitHistory               flux.GitHistoryCache
	discordIngressRetryDelay time.Duration
	discordIngressMaxAge     time.Duration
	alertTracker             alerts.Tracker
	shadowMu                 sync.Mutex
	shadowRestate            *shadowRestateReporter
	authMu                   sync.Mutex
	authenticator            *k8sauth.Authenticator
}

func (s *RouterServer) GetGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	return s.getGitHubJSON(ctx, path, maxBytes, out)
}

func (s *RouterServer) FetchRunLogsArchive(ctx context.Context, repo string, runID int64, attempt int) ([]byte, error) {
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/attempts/%d/logs", repo, runID, attempt)
	req, err := s.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.githubClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("historical execution logs unavailable: status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 16<<20 {
		return nil, fmt.Errorf("historical log archive too large")
	}
	return raw, nil
}

type buildMetadataRegistryAdapter struct {
	inner PRRegistryUpdater
	// srv journals each snapshot into PRObject before the legacy write; nil
	// skips the journal (hermetic tests).
	srv *RouterServer
}

func (a buildMetadataRegistryAdapter) LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (model.PRBuildLifecycle, error) {
	if a.inner == nil {
		return model.PRBuildLifecycle{}, nil
	}
	lc, err := a.inner.LoadPRBuildLifecycle(ctx, repo, prNumber)
	if err != nil {
		return model.PRBuildLifecycle{}, err
	}
	return model.PRBuildLifecycle{
		Status:   lc.Status,
		HeadSHA:  lc.HeadSHA,
		MergeSHA: lc.MergeSHA,
		Started:  lc.Started,
	}, nil
}

func (a buildMetadataRegistryAdapter) RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata model.BuildMetadata) (bool, error) {
	if a.inner == nil {
		return false, nil
	}
	if a.srv != nil {
		// PRObject applies its own revision/order fence, so every snapshot
		// is journaled; a journal failure surfaces so the consumer retries.
		snapshot := metadata
		if err := a.srv.routePREvent(ctx, "HandleBuildMetadata", restateclient.PREvent{
			Repo: repo, PRNumber: prNumber, HeadSHA: metadata.SourceSHA, BuildMetadata: &snapshot,
		}); err != nil {
			return false, err
		}
	}
	return a.inner.RecordPRBuildMetadata(ctx, repo, prNumber, metadata)
}

func (a buildMetadataRegistryAdapter) LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (model.BuildMetadata, bool, error) {
	if a.inner == nil {
		return model.BuildMetadata{}, false, nil
	}
	return a.inner.LoadPRBuildMetadata(ctx, repo, prNumber)
}

func (s *RouterServer) buildConsumer() *buildmeta.Consumer {
	var reg buildmeta.PRMetadataRegistry
	if s.registry != nil {
		reg = buildMetadataRegistryAdapter{inner: s.registry, srv: s}
	}
	return buildmeta.NewConsumer(s, reg, s.cfgSrc, s.resolveLegacyWorkflowRunImages)
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfgSrc ConfigSource, client *http.Client) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	srv := &RouterServer{
		cfgSrc:       cfgSrc,
		httpClient:   tracing.Client(client),
		githubClient: tracing.ExternalClient(client),
	}
	if cfgSrc != nil {
		if cfg := cfgSrc.Current(); cfg != nil && cfg.K8sAuthEnabled {
			srv.authenticator = k8sauth.NewAuthenticator(cfg.K8sAuthConfig)
		}
	}
	return srv
}

func (s *RouterServer) SetEventBus(b pubsub.Bus)                    { s.bus = b }
func (s *RouterServer) EventBus() pubsub.Bus                        { return s.bus }
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater)           { s.registry = reg }
func (s *RouterServer) SetDispatcher(d OutboundDispatcher)          { s.dispatcher = d }
func (s *RouterServer) SetQueueStats(q ConsumerStatsQuerier)        { s.queueStats = q }
func (s *RouterServer) SetGatewayLatencyFn(fn func() time.Duration) { s.gatewayLatencyFn = fn }

func (s *RouterServer) k8sAuthEnabled() bool {
	s.authMu.Lock()
	auth := s.authenticator
	s.authMu.Unlock()
	if auth != nil && auth.Enabled() {
		return true
	}
	if s.cfgSrc != nil {
		if cfg := s.cfgSrc.Current(); cfg != nil && cfg.K8sAuthEnabled {
			return true
		}
	}
	return false
}

func (s *RouterServer) getAuthenticator() *k8sauth.Authenticator {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authenticator != nil {
		return s.authenticator
	}
	if s.cfgSrc != nil {
		if cfg := s.cfgSrc.Current(); cfg != nil && cfg.K8sAuthEnabled {
			s.authenticator = k8sauth.NewAuthenticator(cfg.K8sAuthConfig)
			return s.authenticator
		}
	}
	return nil
}

func (s *RouterServer) isKnownBot(botID string) bool {
	if s.cfgSrc == nil || botID == "" {
		return false
	}
	cfg := s.cfgSrc.Current()
	if cfg == nil {
		return false
	}
	if cfg.BotID == "" && len(cfg.Bots) == 0 {
		return true
	}
	if cfg.BotID != "" && strings.EqualFold(strings.TrimSpace(cfg.BotID), botID) {
		return true
	}
	for b := range cfg.Bots {
		if strings.EqualFold(b, botID) {
			return true
		}
	}
	return false
}

func writeK8sAuthError(w http.ResponseWriter, err error) {
	if valErr, ok := errors.AsType[*k8sauth.ValidationError](err); ok {
		httpkit.WriteJSON(w, valErr.StatusCode(), map[string]string{fieldError: valErr.Error()})
		return
	}
	if unauthErr, ok := errors.AsType[*k8sauth.UnauthorizedError](err); ok {
		httpkit.WriteJSON(w, unauthErr.StatusCode(), map[string]string{fieldError: unauthErr.Error()})
		return
	}
	if forbErr, ok := errors.AsType[*k8sauth.ForbiddenError](err); ok {
		httpkit.WriteJSON(w, forbErr.StatusCode(), map[string]string{fieldError: forbErr.Error()})
		return
	}
	slog.Error("k8sauth internal error", "err", err)
	httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "internal error"})
}

// SetRestateShadow injects the dark shadow reporter (tests and wiring only;
// production lazily builds one from YAML config on first use).
func (s *RouterServer) SetRestateShadow(r *shadowRestateReporter) {
	if s == nil {
		return
	}
	s.shadowMu.Lock()
	defer s.shadowMu.Unlock()
	s.shadowRestate = r
}

// Config returns the current configuration snapshot, implementing discord.IngressServer.
func (s *RouterServer) Config() *config.Config {
	if s == nil || s.cfgSrc == nil {
		return nil
	}
	return s.cfgSrc.Current()
}

// PublishJob publishes a job to JetStream, implementing discord.IngressServer.
func (s *RouterServer) PublishJob(ctx context.Context, args any) error {
	return s.publishJetStreamJob(ctx, args)
}

// ProcessDiscordIngress dispatches a Discord message ingress request to Eri.
func (s *RouterServer) ProcessDiscordIngress(ctx context.Context, args DiscordIngressArgs) error {
	if s.dispatcher == nil {
		return errors.New("dispatcher not configured on server")
	}
	req := DiscordMessageIngressRequest{
		BotUserID: args.BotUserID,
		RoleNames: args.RoleNames,
		Message:   args.Message,
		Agent:     args.Agent,
	}
	return s.dispatcher.DispatchDiscordMessage(ctx, req)
}

// ProcessDiscordInteraction dispatches an application command interaction to Eri.
func (s *RouterServer) ProcessDiscordInteraction(ctx context.Context, args DiscordInteractionArgs) error {
	if s.dispatcher == nil {
		return errors.New("dispatcher not configured on server")
	}
	if args.Agent != "" {
		if ad, ok := s.dispatcher.(interface {
			DispatchDiscordInteractionForAgent(ctx context.Context, agent string, interaction *discordgo.Interaction) error
		}); ok {
			return ad.DispatchDiscordInteractionForAgent(ctx, args.Agent, args.Interaction)
		}
	}
	return s.dispatcher.DispatchDiscordInteraction(ctx, args.Interaction)
}

// startDiscordGateway delegates gateway startup to the internal discord subpackage.
func startDiscordGateway(ctx context.Context, server *RouterServer, _ ...string) (func(), error) {
	return discord.StartDiscordGateway(ctx, server)
}

func readWebhookBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes))
	if err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "failed to read body"})
		return nil, false
	}
	return body, true
}

func readWebhookJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	body, ok := readWebhookBody(w, r, maxBytes)
	if !ok {
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return false
	}
	return true
}

// Routes initializes and returns the HTTP mux for the service.
func (s *RouterServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.Handle("POST /internal/pr/register", tracedIngress(s.handlePRRegister))
	mux.Handle("POST /internal/pr/retry-readiness", tracedIngress(s.handleRetryPRReadiness))
	mux.Handle("POST /api/webhooks/github", tracedIngress(s.handleGitHubWebhook))
	mux.Handle("POST /webhooks/github", tracedIngress(s.handleGitHubWebhook))
	mux.Handle("POST /api/webhooks/flux", tracedIngress(s.handleFluxWebhook))
	mux.Handle("POST /webhooks/flux", tracedIngress(s.handleFluxWebhook))
	mux.Handle("POST /api/webhooks/events", tracedIngress(s.handleEventsWebhook))
	mux.Handle("POST /webhooks/events", tracedIngress(s.handleEventsWebhook))
	mux.Handle("POST /api/v2/alerts", tracedIngress(s.handleAlertsWebhook))
	mux.Handle("POST /api/webhooks/alerts", tracedIngress(s.handleAlertsWebhook))
	mux.Handle("POST /webhooks/alerts", tracedIngress(s.handleAlertsWebhook))
	coverageFlush := httpkit.HandleCoverageFlush("synapse")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func (s *RouterServer) consumerStatsQuerier() ConsumerStatsQuerier {
	if s == nil {
		return nil
	}
	if s.queueStats != nil {
		return s.queueStats
	}
	if csp, ok := s.bus.(ConsumerStatsQuerier); ok {
		return csp
	}
	return nil
}

func (s *RouterServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if q := s.consumerStatsQuerier(); q != nil {
		if err := collectJetStreamQueueMetrics(r.Context(), q, s.discordRouting()); err != nil {
			slog.ErrorContext(r.Context(), "[synapse] [metrics] error collecting jetstream queue metrics", "error", err)
		}
	}
	if s != nil && s.gatewayLatencyFn != nil {
		setDiscordGatewayLatency(s.gatewayLatencyFn().Seconds())
	}
	promhttp.Handler().ServeHTTP(w, r)
}

func (s *RouterServer) resolveHeadlessPRTargetID(ctx context.Context, repo string, prNumber int) string {
	if s.registry != nil {
		if existingTID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNumber); err == nil {
			if norm := model.NormalizeDiscordTargetID(existingTID); isValidDiscordSnowflake(norm) {
				return norm
			}
		}
	}
	if sys := model.NormalizeDiscordTargetID(s.cfgSrc.Current().SystemChannelID); isValidDiscordSnowflake(sys) {
		return sys
	}
	return ""
}

func (s *RouterServer) handlePRRegister(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.registry == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "registry not initialized"})
		return
	}

	var req PRRegisterRequest
	if !readWebhookJSON(w, r, 1<<20, &req) {
		return
	}

	req.Repo = strings.TrimSpace(req.Repo)
	req.Branch = strings.TrimSpace(req.Branch)
	req.HeadSHA = strings.TrimSpace(req.HeadSHA)
	rawTargetID := strings.TrimSpace(req.TargetID)
	req.TargetID = model.NormalizeDiscordTargetID(rawTargetID)
	req.Title = strings.TrimSpace(req.Title)
	req.BotID = strings.TrimSpace(req.BotID)
	req.Agent = strings.TrimSpace(req.Agent)

	if err := s.resolvePRAgent(r.Context(), r, &req); err != nil {
		if errors.Is(err, errBotMismatch) {
			httpkit.WriteJSON(w, http.StatusConflict, map[string]string{fieldError: "bot_id mismatch for local pr_registry"})
			return
		}
		writeK8sAuthError(w, err)
		return
	}

	if valErr := s.validatePRRegisterRequest(r.Context(), &req, rawTargetID); valErr != "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: valErr})
		return
	}

	req.Metadata = injectAgentPRMetadata(req.Metadata, req.Agent)

	targetID, err := s.registry.RegisterPR(r.Context(), req)
	if err != nil {
		if errors.Is(err, model.ErrInvalidTargetID) || strings.Contains(err.Error(), "target_id") {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
		slog.ErrorContext(r.Context(), "[synapse] failed to register PR", "pr", req.PRNumber, "repo", req.Repo, "error", err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "Failed to register PR"})
		return
	}
	// The legacy registry response stays authoritative; PRObject.Register
	// runs beside it so Restate holds the owner and row, and any mismatch is
	// logged and counted.
	s.shadowRegisterPR(r.Context(), req, targetID)

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:   "registered",
		"repo":        req.Repo,
		fieldPRNumber: req.PRNumber,
		"target_id":   targetID,
		"agent":       req.Agent,
	})
}

func (*RouterServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus: "ok",
		"service":   "synapse",
		"time":      time.Now().UTC().Format(time.RFC3339),
	})
}

func deployEventIdempotencyKey(evt DeployEvent, originPrefix string) string {
	if evt.CommitSHA == "" || evt.JobName == "" {
		return randomNonce()
	}
	if originPrefix != "" {
		return fmt.Sprintf("%s:%s:%s:%s:%s", originPrefix, evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
	}
	return fmt.Sprintf("%s:%s:%s:%s", evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
}

func (s *RouterServer) enqueueOrProcessDeployEvent(w http.ResponseWriter, r *http.Request, evt DeployEvent, originPrefix string) {
	s.enrichDeployEventPRAndTarget(r.Context(), &evt)
	logPrefix := firstNonEmpty(originPrefix, "deploy")
	if (evt.Event == eventDeployStarted || evt.Event == eventDeploySuccess) && s.shouldTreatDeploySuccessAsSync(r.Context(), evt) {
		slog.InfoContext(r.Context(), "[synapse] ignoring pre-image event (waiting for image build)", "origin", logPrefix, "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			fieldStatus:   "ignored",
			fieldEvent:    evt.Event,
			fieldJobName:  evt.JobName,
			fieldPRNumber: evt.PRNumber,
		})
		return
	}
	// One effect-owner snapshot per request: stamped into the job when
	// queued, or bound into ctx when processed inline.
	owner := newEventEffectsOwner()
	idempotencyKey := deployEventIdempotencyKey(evt, originPrefix)
	if s.bus != nil {
		if err := s.publishJetStreamJob(r.Context(), DeployWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt, EffectsOwner: owner}); err != nil {
			slog.ErrorContext(r.Context(), "[synapse] failed to publish job to jetstream", "origin", logPrefix, "error", err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to persist webhook"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			fieldStatus:  statusAccepted,
			fieldEvent:   evt.Event,
			fieldJobName: evt.JobName,
		})
		return
	}

	processed, err := s.ProcessDeployEvent(jobContext(r.Context(), owner, eventIdentity("deploy", idempotencyKey)), evt)
	if err != nil {
		slog.ErrorContext(r.Context(), "[synapse] error processing event", "origin", logPrefix, "error", err)
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:   statusAccepted,
		fieldEvent:    processed.Event,
		fieldJobName:  processed.JobName,
		fieldPRNumber: processed.PRNumber,
		"target_id":   processed.TargetID,
	})
}

func (s *RouterServer) resolvePRFromRegistrySHA(ctx context.Context, repo, sha string) (int, string, string, bool) {
	if s.registry == nil {
		return 0, "", "", false
	}
	prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] failed resolving PR by SHA locally", "sha", sha, "error", err)
		return 0, "", "", false
	}
	if prNum <= 0 {
		return 0, "", "", false
	}
	if targetID == "" {
		if tID, _, _, _, errNum := s.registry.ResolvePRByNumber(ctx, repo, prNum); errNum == nil && tID != "" {
			targetID = tID
		}
	}
	return prNum, targetID, branch, true
}

func (s *RouterServer) backfillRegistryTargetID(ctx context.Context, repo string, prNum int, sha string) string {
	if s.registry == nil {
		return ""
	}
	targetID := ""
	if tID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum); err == nil && tID != "" {
		targetID = tID
	}
	if backfillTID, err := s.registry.BackfillPushMergeSHA(ctx, repo, prNum, sha); err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] failed backfilling merge_sha", "sha", sha, "repo", repo, "pr", prNum, "error", err)
	} else if targetID == "" && backfillTID != "" {
		targetID = backfillTID
	}
	return targetID
}

// ResolveSHA resolves a commit SHA to its PR number, target Discord thread ID, and branch.
// It checks local pr_registry first, then falls back to GitHub commit PR lookup and backfills pr_registry.
func (s *RouterServer) ResolveSHA(ctx context.Context, repo, sha string) (int, string, string, error) {
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}
	repo = normalizeRepo(repo)

	if prNum, targetID, branch, ok := s.resolvePRFromRegistrySHA(ctx, repo, sha); ok {
		return prNum, targetID, branch, nil
	}

	prNum, branch, err := s.resolvePRFromCommitSHA(ctx, repo, sha)
	if err != nil || prNum == 0 {
		return 0, "", "", err
	}

	targetID := s.backfillRegistryTargetID(ctx, repo, prNum, sha)
	return prNum, targetID, branch, nil
}

// ResolveThreadForPR resolves the Discord thread snowflake (target_id) for a PR.
func (s *RouterServer) ResolveThreadForPR(ctx context.Context, repo string, prNum int) (string, error) {
	if s.registry == nil || prNum <= 0 {
		return "", nil
	}
	targetID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNum)
	return targetID, err
}

func (s *RouterServer) isValidAlertTarget(targetID string) bool {
	clean := strings.TrimSpace(targetID)
	if isValidDiscordSnowflake(clean) {
		return true
	}
	sys := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	return sys != "" && clean == sys && isValidDiscordChannel(clean)
}

type alertPromptOpts struct {
	silent       bool
	skipIfActive bool
	effort       string
	dispatchID   string
}

func (s *RouterServer) dispatchAlertWithPrompt(ctx context.Context, targetID, label, dmMsg, prompt string, aOpts ...alertPromptOpts) error {
	if s.dispatcher == nil {
		return nil
	}
	var opt alertPromptOpts
	if len(aOpts) > 0 {
		opt = aOpts[0]
	}
	if dmMsg != "" && s.isValidAlertTarget(targetID) {
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: targetID, Content: dmMsg}); err != nil {
			slog.WarnContext(ctx, "[synapse] [dispatcher] error dispatching direct message", "label", label, "error", err)
		} else {
			recordLegacyEffect(ctx, systemEffectKind(restateclient.EffectEriDirectMessage), "", "", 0)
		}
	}
	if opt.silent {
		req := DispatchRequest{
			ID:           opt.dispatchID,
			Source:       "synapse",
			TargetID:     targetID,
			Prompt:       prompt,
			Effort:       opt.effort,
			SkipIfActive: opt.skipIfActive,
		}
		if err := s.dispatcher.DispatchBackground(ctx, req); err != nil {
			slog.ErrorContext(ctx, "[synapse] [dispatcher] error dispatching background prompt", "label", label, "error", err)
			return fmt.Errorf("dispatch background %s prompt: %w", label, err)
		}
		recordLegacyEffect(ctx, systemEffectKind(restateclient.EffectEriBackground), "", "", 0)
		return nil
	}
	if !s.isValidAlertTarget(targetID) {
		return nil
	}
	req := PromptRequest{ChannelID: targetID, Prompt: prompt}
	if err := s.dispatcher.DispatchPrompt(ctx, req); err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error dispatching prompt", "label", label, "error", err)
		return fmt.Errorf("dispatch %s prompt: %w", label, err)
	}
	recordLegacyEffect(ctx, systemEffectKind(restateclient.EffectEriPrompt), "", "", 0)
	return nil
}

// systemEffectKind labels the PR-less alert effects dispatchAlertWithPrompt
// performs. Every caller reaches it only with no PR number (PR deploy and
// sync alerts return before it), so no PRObject key exists and prstate
// never records a counterpart; the system_ prefix keeps them out of the
// per-kind shadow comparison.
func systemEffectKind(kind string) string {
	return "system_" + kind
}

func (s *RouterServer) resolveDeployEventPRNumber(ctx context.Context, evt *DeployEvent) {
	if evt.CommitSHA == "" || evt.PRNumber != 0 {
		return
	}
	prNum, targetID, _, err := s.ResolveSHA(ctx, evt.Repo, evt.CommitSHA)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] failed resolving SHA", "sha", evt.CommitSHA, "error", err)
		return
	}
	if prNum > 0 {
		evt.PRNumber = prNum
		evt.TargetID = firstNonEmpty(evt.TargetID, targetID)
	}
}

func (s *RouterServer) resolveDeployEventTargetID(ctx context.Context, evt *DeployEvent) {
	if evt.PRNumber <= 0 || evt.TargetID != "" {
		return
	}
	targetID, err := s.ResolveThreadForPR(ctx, evt.Repo, evt.PRNumber)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] failed resolving thread for PR", "repo", evt.Repo, "pr", evt.PRNumber, "error", err)
		return
	}
	if targetID != "" {
		evt.TargetID = targetID
	}
}

func (s *RouterServer) enrichDeployEventPRAndTarget(ctx context.Context, evt *DeployEvent) {
	s.resolveDeployEventPRNumber(ctx, evt)
	s.resolveDeployEventTargetID(ctx, evt)
	if evt.TargetID == "" && (evt.Event == eventDeployFailed || evt.Event == eventDeployRollback || evt.Event == eventSyncFailed) {
		evt.TargetID = strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	}
}

func (s *RouterServer) handleDeployStartedRegistry(ctx context.Context, evt DeployEvent) bool {
	if s.registry == nil || evt.PRNumber == 0 || evt.Event != eventDeployStarted || evt.JobName == "" {
		return false
	}
	if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
		slog.InfoContext(ctx, "[synapse] [deploy] skipping deploy_started (waiting for image build)", "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return true
	}
	if len(evt.ExpectedJobs) > 0 {
		if _, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
			slog.WarnContext(ctx, "[synapse] [registry] error recording expected jobs on deploy_started", "repo", evt.Repo, "pr", evt.PRNumber, "error", err)
		}
	}
	if tID, err := s.registry.TransitionDeploying(ctx, evt.Repo, evt.PRNumber, []string{evt.JobName}); err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error transitioning pr to deploying", "repo", evt.Repo, "pr", evt.PRNumber, "job", evt.JobName, "error", err)
	} else if tID == "" {
		slog.InfoContext(ctx, "[synapse] [deploy] skipping deploy_started (job already deployed or PR inactive)", "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return true
	}
	return false
}

func (s *RouterServer) recordDeploySyncExpectedJobs(ctx context.Context, evt DeployEvent) bool {
	if evt.Event != eventSyncSuccess || evt.PRNumber == 0 {
		return false
	}
	syncHasExpected := len(dedupeTrimmedStrings(evt.ExpectedJobs)) > 0
	if s.registry != nil {
		if regHasExpected, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
			slog.WarnContext(ctx, "[synapse] [registry] error recording expected jobs on sync_success", "repo", evt.Repo, "pr", evt.PRNumber, "error", err)
		} else {
			syncHasExpected = syncHasExpected || regHasExpected
		}
	}
	syncHasExpected = syncHasExpected || s.isWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber)
	return syncHasExpected
}

func (s *RouterServer) handleDeployFailedOrRollback(ctx context.Context, evt DeployEvent, prefix string) error {
	targetID := evt.TargetID
	if s.registry != nil && evt.PRNumber > 0 && restateOwnsEffects(ctx) {
		// PRObject applies deploy_failed and publishes the carried fact.
		return s.routeDeployFailed(ctx, evt.Repo, evt.PRNumber, "", evt.CommitSHA, false, s.deployLifecycleFact(ctx, evt, eventDeployFailed, eventDeployFailed))
	}
	if s.registry != nil && evt.PRNumber > 0 {
		if err := s.routeDeployFailed(ctx, evt.Repo, evt.PRNumber, "", evt.CommitSHA, false); err != nil {
			return err
		}
		tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
		if err != nil || !updated {
			return err
		}
		targetID = firstNonEmpty(tID, targetID)
	}
	fact := s.deployLifecycleFact(ctx, evt, eventDeployFailed, eventDeployFailed)
	if evt.PRNumber > 0 {
		if err := s.publishLegacyFact(ctx, fact); err != nil {
			return fmt.Errorf("publish deploy lifecycle event: %w", err)
		}
	} else if err := s.publishPRLifecycleEvent(ctx, fact); err != nil {
		return fmt.Errorf("publish deploy lifecycle event: %w", err)
	}
	if s.registry != nil && evt.PRNumber > 0 {
		if ackErr := s.routeDeployFailed(ctx, evt.Repo, evt.PRNumber, "", evt.CommitSHA, true, fact); ackErr != nil {
			slog.WarnContext(ctx, "[synapse] [restate-cutover] failed journaling deploy_failed ack", "repo", evt.Repo, "pr", evt.PRNumber, "error", ackErr)
		}
		if ackErr := s.registry.AcknowledgeDeployFailedPublished(ctx, evt.Repo, evt.PRNumber); ackErr != nil {
			slog.WarnContext(ctx, "[synapse] [registry] failed acknowledging deploy_failed_published", "repo", evt.Repo, "pr", evt.PRNumber, "error", ackErr)
		}
	}
	if evt.PRNumber > 0 {
		return nil
	}
	if s.cfgSrc != nil {
		targetID = firstNonEmpty(targetID, s.cfgSrc.Current().SystemChannelID)
	}
	actionDesc, fixDesc := "Deployment failed", "deployment failure"
	if evt.Event == eventDeployRollback {
		actionDesc, fixDesc = "Deployment failed and rolled back", "deployment rollback failure"
	}
	dmMsg := fmt.Sprintf("💥 %s%s for %s. Following up...", prefix, actionDesc, evt.JobName)
	var aOpts []alertPromptOpts
	if evt.PRNumber > 0 {
		dmMsg = ""
		aOpts = append(aOpts, alertPromptOpts{silent: true, skipIfActive: true, effort: "medium"})
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("%s for job %s on %s (PR #%d, commit: %s).", actionDesc, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
		evt.Details, "", fmt.Sprintf("Please investigate and fix the %s.", fixDesc),
	)
	return s.dispatchAlertWithPrompt(ctx, targetID, evt.Event, dmMsg, prompt, aOpts...)
}

func (s *RouterServer) handleDeploySyncFailed(ctx context.Context, evt DeployEvent, prefix string) error {
	if err := s.publishDeployLifecycleEvent(ctx, evt, "sync_failed", "deploy_failed"); err != nil {
		return fmt.Errorf("publish deploy lifecycle event: %w", err)
	}
	if evt.PRNumber > 0 {
		return nil
	}
	header := fmt.Sprintf("Git sync failed for repo %s (commit: %s, PR #%d).", evt.Repo, evt.CommitSHA, evt.PRNumber)
	if evt.PRNumber == 0 {
		header = fmt.Sprintf("Git sync failed for repo %s (commit: %s).", evt.Repo, evt.CommitSHA)
	}
	dmMsg := fmt.Sprintf("⚠️ %sGit file sync failed. Following up...", prefix)
	var aOpts []alertPromptOpts
	if evt.PRNumber > 0 {
		dmMsg = ""
		aOpts = append(aOpts, alertPromptOpts{silent: true, skipIfActive: true, effort: "medium"})
	}
	prompt := formatErrorDetailsBlock(header, evt.Details, "", "Please investigate and fix the git sync failure.")
	return s.dispatchAlertWithPrompt(ctx, evt.TargetID, "sync_failed", dmMsg, prompt, aOpts...)
}

// isWaitingForImageBuild routes a deploy_success to the sync path while the
// image build is still pending. Projection-safe: pure routing between two
// live paths, never an effect dedup decision (the publish itself still
// resolves from Restate when authoritative).
func (s *RouterServer) isWaitingForImageBuild(ctx context.Context, repo string, prNumber int) bool {
	repo = normalizeRepo(repo)
	if s == nil || s.registry == nil || repo == "" || prNumber <= 0 || metadataWorkflowPath(repo) == "" {
		return false
	}
	lifecycle, err := s.registry.LoadPRBuildLifecycle(ctx, repo, prNumber)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] error loading build lifecycle", "repo", repo, "pr", prNumber, "error", err)
		return true
	}
	switch strings.ToLower(strings.TrimSpace(lifecycle.Status)) {
	case prOpen, prCIFailed, prConflict, prMerged, prSynced:
	default:
		return false
	}
	if lifecycle.Started {
		return false
	}
	if watched, watchErr := s.registry.HasKustomizationRequests(ctx, repo, prNumber); watchErr == nil && watched {
		return false
	}
	m, found, err := s.registry.LoadPRBuildMetadata(ctx, repo, prNumber)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] error loading build metadata", "repo", repo, "pr", prNumber, "error", err)
		return true
	}
	return !found || m.Kind != buildMetadataFinal || len(m.Selected) > 0
}

func (s *RouterServer) shouldTreatDeploySuccessAsSync(ctx context.Context, evt DeployEvent) bool {
	return evt.SourceKind == kindKustomization && s.isWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber)
}

// deployEventWatcherOwnership reports whether evt belongs to the Kustomization watcher, which is
// the sole terminal deploy authority for every PR whose repo sources a watched Flux Kustomization.
// Ownership holds for started/success alerts when the PR already has recorded Kustomization
// requests, or — so an early pre-image reconcile can never publish terminal success before the
// watcher gate passes — when the repo is the source of at least one watched Kustomization.
// Failure and rollback alerts keep the direct path unless the PR is tracked under a Kustomization.
// A discovery error fails closed: the alert is owned but gated, so the caller must publish nothing
// and record no dedup for it. Projection-safe: pure watcher routing; the
// terminal publish itself resolves from Restate when authoritative.
func (s *RouterServer) deployEventWatcherOwnership(ctx context.Context, evt DeployEvent) (owned bool, err error) {
	if s.registry == nil || evt.PRNumber <= 0 {
		return false, nil
	}
	isFluxKind := evt.SourceKind == kindKustomization || evt.SourceKind == kindHelmRelease
	if !isFluxKind && evt.Event != eventDeploySuccess {
		return false, nil
	}
	tracked, trackErr := s.registry.HasKustomizationRequests(ctx, evt.Repo, evt.PRNumber)
	if trackErr != nil {
		slog.WarnContext(ctx, "[synapse] [deploy] error checking kustomization requests", "repo", evt.Repo, "pr", evt.PRNumber, "error", trackErr)
	} else if tracked {
		if evt.SourceKind == kindKustomization || evt.Event == eventDeploySuccess ||
			(isFluxKind && evt.Event == eventDeployStarted) {
			return true, nil
		}
		return false, nil
	}
	if !isFluxKind {
		return false, nil
	}
	if evt.Event != eventDeployStarted && evt.Event != eventDeploySuccess {
		return false, nil
	}
	if s.dispatcher == nil {
		return false, nil
	}
	refs, discErr := s.dispatcher.DiscoverRepoKustomizations(ctx, evt.Repo)
	if discErr != nil {
		return true, discErr
	}
	return len(refs) > 0, nil
}

// deferOwnedDeployEventToWatcher hands a watcher-owned alert to the Kustomization watcher instead
// of publishing any lifecycle event from the alert, so an ungated Flux reconcile can never emit
// terminal deploy_success or poison dedup before the watcher gate passes. Already-tracked alerts
// defer silently; untracked deploy_started/deploy_success alerts go through deferPRToWatcher, which
// transitions the PR to deploying (publishing the single deploy_started), records the reconcile
// requests, and re-triggers the reconciles. It reports whether the alert was fully handled here.
func (s *RouterServer) deferOwnedDeployEventToWatcher(ctx context.Context, evt DeployEvent) (bool, error) {
	owned, discErr := s.deployEventWatcherOwnership(ctx, evt)
	if !owned {
		return false, nil
	}
	if discErr != nil {
		slog.ErrorContext(ctx, "[synapse] [deploy] kustomization discovery failed; suppressing alert without publishing", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber, "error", discErr)
		return true, nil
	}
	// Projection-safe: watcher-routing deferral only; the terminal publish
	// itself resolves from Restate when authoritative.
	if tracked, trackErr := s.registry.HasKustomizationRequests(ctx, evt.Repo, evt.PRNumber); trackErr == nil && tracked {
		slog.InfoContext(ctx, "[synapse] [deploy] deferring event to the kustomization watcher", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return true, nil
	}
	if evt.Event != eventDeployStarted && evt.Event != eventDeploySuccess {
		slog.InfoContext(ctx, "[synapse] [deploy] deferring event to the kustomization watcher", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return true, nil
	}
	if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
		slog.InfoContext(ctx, "[synapse] [deploy] watcher-owned event waiting for image build", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber)
		return false, nil
	}
	deferred, err := s.deferPRToWatcher(ctx, evt.Repo, evt.PRNumber)
	if err != nil {
		return true, err
	}
	return deferred, nil
}

func (s *RouterServer) ProcessDeployEvent(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	s.enrichDeployEventPRAndTarget(ctx, &evt)

	slog.InfoContext(ctx, "[synapse] [deploy] deploy event", "event", evt.Event, "job", evt.JobName, "repo", evt.Repo, "pr", evt.PRNumber, "commit", evt.CommitSHA, "status", evt.Status, "target_id", evt.TargetID, "deployment_id", evt.DeploymentID, "details", evt.Details)

	s.evaluateFluxEventTriggers(ctx, evt)

	if handled, err := s.deferOwnedDeployEventToWatcher(ctx, evt); handled || err != nil {
		return &evt, err
	}

	if s.handleDeployStartedRegistry(ctx, evt) {
		return &evt, nil
	}

	syncHasExpected := s.recordDeploySyncExpectedJobs(ctx, evt)
	if deferred, err := s.deferSyncToWatcher(ctx, evt, syncHasExpected); deferred || err != nil {
		return &evt, err
	}

	return s.publishUnownedDeployEvent(ctx, evt, syncHasExpected)
}

// publishUnownedDeployEvent publishes the lifecycle outcome for an alert the Kustomization watcher
// does not own: PRs whose repos source no watched Flux Kustomization keep the direct alert path.
func (s *RouterServer) publishUnownedDeployEvent(ctx context.Context, evt DeployEvent, syncHasExpected bool) (*DeployEvent, error) {
	if !s.isValidAlertTarget(evt.TargetID) || s.dispatcher == nil {
		return &evt, nil
	}

	prefix := formatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case eventDeployStarted:
		if err := s.publishDeployLifecycleEvent(ctx, evt, "deploy_started", "deploying"); err != nil {
			return &evt, err
		}
		return &evt, nil
	case eventDeploySuccess:
		if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
			if err := s.handleDeploySyncSuccess(ctx, evt, true); err != nil {
				return &evt, err
			}
			return &evt, nil
		}

		return s.publishUnownedDeploySuccess(ctx, evt)
	case eventDeployRollback, eventDeployFailed:
		return &evt, s.handleDeployFailedOrRollback(ctx, evt, prefix)
	case eventSyncSuccess:
		if err := s.handleDeploySyncSuccess(ctx, evt, syncHasExpected); err != nil {
			return &evt, err
		}
		return &evt, nil
	case eventSyncFailed:
		return &evt, s.handleDeploySyncFailed(ctx, evt, prefix)
	}
	return &evt, nil
}

// publishUnownedDeploySuccess publishes one unowned deploy_success alert:
// authoritative PR dispatches resolve through PRObject; otherwise the
// merged HandleDeploySuccess verdict is journaled with the fact before the
// legacy suppress check and publish (restate-owned events stop there).
func (s *RouterServer) publishUnownedDeploySuccess(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	if s.restateAuthoritative() && evt.PRNumber > 0 {
		return s.publishUnownedDeploySuccessAuthoritative(ctx, evt)
	}
	fact := s.deployLifecycleFact(ctx, evt, eventDeploySuccess, statusDeployed)
	if evt.PRNumber > 0 {
		// Journal before the suppress check: the journal is the ordered
		// record and Restate dedups the repeat; the suppress check below
		// still skips the duplicate legacy publish.
		if err := s.routeDeployDispatched(ctx, evt.Repo, evt.JobName, "", evt.CommitSHA, evt.PRNumber, fact); err != nil {
			return &evt, err
		}
		if restateOwnsEffects(ctx) {
			return &evt, nil
		}
	}
	if s.shouldSuppressDuplicateDeploySuccess(ctx, evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber) {
		slog.InfoContext(ctx, "[synapse] [deploy] suppressing duplicate deploy_success", "repo", evt.Repo, "job", evt.JobName, "pr", evt.PRNumber, "commit", evt.CommitSHA)
		return &evt, nil
	}
	if evt.PRNumber > 0 {
		return &evt, s.publishLegacyFact(ctx, fact)
	}
	if err := s.publishPRLifecycleEvent(ctx, fact); err != nil {
		return &evt, err
	}
	if err := s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, evt.JobName, evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID); err != nil {
		return &evt, err
	}
	// No Restate route: non-PR dispatches have no PRObject key in either
	// mode, and the prompt above is the only effect.
	return &evt, nil
}

// shouldSuppressDuplicateDeploySuccess reports whether a deploy_success
// dispatch is a duplicate. Legacy fallback only: the authoritative path
// resolves from the Restate PRObject and never calls this. It survives
// because the non-authoritative direct-alert path still needs projection
// dedup; deleting it would republish duplicates on the fallback path.
func (s *RouterServer) shouldSuppressDuplicateDeploySuccess(ctx context.Context, repo, _jobName, _commitSHA string, prNum int) bool {
	if s == nil {
		return false
	}
	normRepo := normalizeRepo(repo)

	if prNum > 0 && s.registry != nil {
		deployed, err := s.registry.IsPRDeployed(ctx, normRepo, prNum)
		if err == nil && deployed {
			return true
		}
	}

	return false
}

func (s *RouterServer) handleDeploySyncSuccess(ctx context.Context, evt DeployEvent, syncHasExpected bool) error {
	if evt.PRNumber <= 0 {
		return nil
	}
	if syncHasExpected {
		return s.publishDeployLifecycleEvent(ctx, evt, "sync_success", "synced")
	}
	// Journal before the legacy publish and dedup mark so a journal failure
	// leaves nothing recorded and the redelivery retries the journal. A
	// restate-owned event leaves the publish to PRObject.
	fact := s.deployLifecycleFact(ctx, evt, eventDeploySuccess, statusDeployed)
	if err := s.routeDeployDispatched(ctx, evt.Repo, evt.JobName, "", evt.CommitSHA, evt.PRNumber, fact); err != nil {
		return err
	}
	if restateOwnsEffects(ctx) {
		return nil
	}
	return s.publishLegacyFact(ctx, fact)
}

func (s *RouterServer) isDependabotPRNumber(ctx context.Context, repo string, prNum int) bool {
	if s == nil || s.registry == nil || prNum <= 0 {
		return false
	}
	_, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNum)
	return err == nil && isDependabotBranch(branch)
}

func (s *RouterServer) dispatchDeploymentSuccessPrompt(ctx context.Context, targetID, jobName, repo, commitSHA string, prNum int, refID string) error {
	if prNum > 0 {
		return nil
	}
	if s.dispatcher == nil || !s.isValidAlertTarget(targetID) {
		return nil
	}

	repoPart := ""
	if repo != "" {
		repoPart = " on " + repo
	}
	refPart := ""
	if refID != "" {
		refPart = ", ref: " + refID
	}
	header := fmt.Sprintf("Continuous Delivery deployment completed for job %s%s (PR #%d, commit: %s%s).", jobName, repoPart, prNum, commitSHA, refPart)
	lines := []string{
		header,
		"",
		"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
	}
	return s.dispatchAlertWithPrompt(ctx, targetID, "deploy_success", "", strings.Join(lines, "\n"))
}

func (s *RouterServer) handleFluxWebhook(w http.ResponseWriter, r *http.Request) {
	body, ok := readWebhookBody(w, r, 1<<20)
	if !ok {
		return
	}

	var alert FluxAlertEvent
	if err := json.Unmarshal(body, &alert); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return
	}

	if alertPayloadRepo(alert) == "" {
		alert.Repo = s.resolveFluxAlertSourceRepo(r.Context(), alert)
	}
	deployEvt, ok := translateFluxAlertToDeployEvent(alert)
	if !ok {
		slog.InfoContext(r.Context(), "[synapse] [flux] ignoring unhandled flux alert", "kind", alert.InvolvedObject.Kind, "name", alert.InvolvedObject.Name, "reason", alert.Reason, "severity", alert.Severity)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			fieldStatus: "ignored",
			"message":   "unhandled flux alert event",
		})
		return
	}

	s.enqueueOrProcessDeployEvent(w, r, deployEvt, "flux")
}

func (s *RouterServer) resolveFluxAlertSourceRepo(ctx context.Context, alert FluxAlertEvent) string {
	if s.dispatcher == nil {
		return ""
	}
	repo, err := s.dispatcher.ResolveFluxSourceRepo(ctx, alert.InvolvedObject)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [flux] error resolving source repo", "kind", alert.InvolvedObject.Kind, "namespace", alert.InvolvedObject.Namespace, "name", alert.InvolvedObject.Name, "error", err)
		return ""
	}
	return repo
}

func (s *RouterServer) handleEventsWebhook(w http.ResponseWriter, r *http.Request) {
	body, ok := readWebhookBody(w, r, 1<<20)
	if !ok {
		return
	}

	var req EventWebhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return
	}
	source := strings.ToLower(strings.TrimSpace(req.Source))
	eventType := strings.ToLower(strings.TrimSpace(req.EventType))
	targets := dedupeTrimmedStrings(append([]string{req.Target}, req.Targets...))
	if source == "" || eventType == "" || len(targets) == 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "missing required fields"})
		return
	}

	contextHeader := strings.TrimSpace(req.ContextHeader)
	if contextHeader == "" {
		contextHeader = fmt.Sprintf("External %s event %s on %s", source, eventType, strings.Join(targets, ", "))
		if details := strings.TrimSpace(req.Details); details != "" {
			contextHeader += ": " + details
		}
	}

	if s == nil || s.bus == nil {
		httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "event bus not initialized"})
		return
	}

	if err := s.evaluateEventTriggers(r.Context(), source, eventType, targets, contextHeader); err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to evaluate triggers"})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"source":     source,
		"targets":    targets,
		"event_type": eventType,
	})
}

func (s *RouterServer) evaluateEventTriggers(ctx context.Context, source, eventType string, targets []string, contextHeader string) error {
	if s == nil || s.bus == nil {
		return errors.New("event bus not initialized")
	}
	return alerts.EvaluateEventTriggers(ctx, s.bus, source, eventType, targets, contextHeader)
}

func (s *RouterServer) evaluateFluxEventTriggers(ctx context.Context, evt DeployEvent) {
	if s == nil || s.bus == nil {
		return
	}
	alerts.EvaluateFluxEventTriggers(ctx, s.bus, model.DeployEvent{
		Event:   evt.Event,
		Repo:    evt.Repo,
		Details: evt.Details,
	})
}

func (s *RouterServer) evaluateGitHubEventTriggers(ctx context.Context, res *ResolvedGitHubEvent, body []byte) {
	if s == nil || s.bus == nil || res == nil {
		return
	}
	alerts.EvaluateGitHubEventTriggers(ctx, s.bus, &alerts.ResolvedGitHubEvent{
		Event:      res.Event,
		Action:     res.Action,
		Repo:       res.Repo,
		Branch:     res.Branch,
		Merged:     res.Merged,
		Conclusion: res.Conclusion,
	}, body)
}

func (s *RouterServer) handleAlertsWebhook(w http.ResponseWriter, r *http.Request) {
	var alertsList []VMAlertNotification
	if !readWebhookJSON(w, r, 1<<20, &alertsList) {
		return
	}

	dispatchedCount := 0
	now := time.Now().UTC()
	for _, alert := range alertsList {
		fp := vmAlertFingerprint(alert.Labels)
		if isVMAlertResolved(alert, now) {
			s.alertTracker.ClearFiring(fp)
			continue
		}
		if status := strings.TrimSpace(alert.Status); status != "" && !strings.EqualFold(status, "firing") {
			continue
		}

		if s.dispatcher == nil || s.cfgSrc == nil {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "dispatcher or config not available"})
			return
		}
		channelID := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
		if !s.isValidAlertTarget(channelID) {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "system channel not configured or invalid"})
			return
		}

		if !s.alertTracker.MarkFiring(fp) {
			continue
		}
		msg := formatVMAlertMessage(alert)
		if err := s.dispatcher.DispatchDirectMessage(r.Context(), DirectMessageRequest{ChannelID: channelID, Content: msg}); err != nil {
			s.alertTracker.ClearFiring(fp)
			slog.ErrorContext(r.Context(), "[synapse] [alerts] error dispatching alert", "fingerprint", fp, "error", err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to dispatch alert message"})
			return
		}
		dispatchedCount++
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"dispatched": dispatchedCount,
	})
}

func normalizeJobArgs(args any) any {
	switch a := args.(type) {
	case *GitHubWebhookArgs:
		if a != nil {
			return *a
		}
	case *DeployWebhookArgs:
		if a != nil {
			return *a
		}
	case *DiscordIngressArgs:
		if a != nil {
			return *a
		}
	case *DiscordInteractionArgs:
		if a != nil {
			return *a
		}
	}
	return args
}

func resolveJobQueueAndKind(args any) (string, string) {
	switch a := normalizeJobArgs(args).(type) {
	case DeployWebhookArgs:
		return queueDefault, "deploy_webhook"
	case *DeployWebhookArgs:
		if a != nil {
			return queueDefault, "deploy_webhook"
		}
	}
	return metrics.ResolveJobQueueAndKind(args)
}

func (s *RouterServer) publishJetStreamJob(ctx context.Context, args any) error {
	if s == nil || s.bus == nil {
		return errors.New("event bus not configured")
	}
	subject, msgID := resolveJetStreamJobCoords(args, s.discordRouting())
	if subject == "" {
		return fmt.Errorf("unsupported job args type %T", args)
	}
	queue, kind := resolveJobQueueAndKind(args)
	if err := s.bus.Publish(ctx, subject, msgID, args); err != nil {
		recordJobEnqueued(queue, kind, "error")
		return err
	}
	recordJobEnqueued(queue, kind, "enqueued")
	return nil
}

func setupPostgres(ctx context.Context, server *RouterServer, postgresURL string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := poolConfig.ConnConfig.RuntimeParams["search_path"]; !ok {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = "sidecars, public"
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	server.SetRegistry(registry.NewPostgresPRRegistry(pool))
	log.Println("[synapse] PostgreSQL PR registry initialized")
	return pool, nil
}

// RunServer runs the HTTP server with graceful shutdown and optional ready callback.
// Fields bound at startup (listener, database pool, Discord session, dispatcher)
// are read once from the initial snapshot; synapseRestartRequired guards them.
func RunServer(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config], onReady func(addr string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := cfgSrc.Current()
	server := NewRouterServer(cfgSrc, nil)
	dispatcher := NewDefaultOutboundDispatcher(cfg.BrainURL, nil)
	dispatcher.SetConfigSource(cfgSrc)
	dispatcher.SetFluxReceiverURL(cfg.FluxReceiverURL)
	dispatcher.SetKubeConfig(cfg.KubeAPIServer, cfg.KubeToken)
	server.SetDispatcher(dispatcher)

	stopJetStream, err := initSynapseJetStream(ctx, server, cfg.NATSURL)
	if err != nil {
		return err
	}
	defer stopJetStream()

	var dbPool *pgxpool.Pool
	if cfg.PostgresURL != "" {
		pool, setupPostgresErr := setupPostgres(ctx, server, cfg.PostgresURL)
		if setupPostgresErr != nil {
			return setupPostgresErr
		}
		dbPool = pool
		defer dbPool.Close()
	}
	stopWatcher := server.startKustomizationWatcher(ctx, dispatcher)
	defer stopWatcher()

	// Initialize and launch Discord Gateway consumer
	stopDiscord, err := startDiscordGateway(ctx, server, cfg.DiscordToken)
	if err != nil {
		return fmt.Errorf("start discord gateway: %w", err)
	}
	var stopDiscordOnce sync.Once
	safeStopDiscord := func() {
		stopDiscordOnce.Do(stopDiscord)
	}
	defer safeStopDiscord()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
	if err != nil {
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}

	mux := http.NewServeMux()
	mux.Handle("POST /-/reload", cfgSrc.Handler("synapse"))
	mux.Handle("/", server.Routes())
	httpServer := &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if onReady != nil {
		onReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[synapse] Server listening on %s (KubeAPIServer: %s)",
			ln.Addr().String(), cfg.KubeAPIServer)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errChan:
		runErr = err
	}

	log.Println("[synapse] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()

	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[synapse] HTTP shutdown error: %v", shutErr)
	}

	safeStopDiscord()

	stopWatcher()

	log.Println("[synapse] Server stopped")
	return runErr
}

// RunApp runs synapse until ctx is cancelled or a config reload requests a
// restart, in which case it shuts down gracefully and returns nil so the
// container restarts with the new configuration.
func RunApp(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-cfgSrc.RestartRequested():
			cancel()
		case <-ctx.Done():
		}
	}()
	return RunServer(ctx, cfgSrc, nil)
}
