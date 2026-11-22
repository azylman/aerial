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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/discord"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
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
	dispatcher               OutboundDispatcher
	onProcessed              func(event, delivery string)
	queueStats               ConsumerStatsQuerier
	gatewayLatencyFn         func() time.Duration
	gitHistory               flux.GitHistoryCache
	discordIngressRetryDelay time.Duration
	discordIngressMaxAge     time.Duration
	alertTracker             alerts.Tracker
	authMu                   sync.Mutex
	authenticator            *k8sauth.Authenticator
}

func (s *RouterServer) GetGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	return s.getGitHubJSON(ctx, path, maxBytes, out)
}

// NewRouterServer constructs a new RouterServer instance. Every GitHub REST
// call goes through githubClient, whose githubauth.Transport attaches the
// credential (WithGitHubTokenSource, else the static PAT) and records
// github_requests_by_source_total / github_rate_limit_remaining.
func NewRouterServer(cfgSrc ConfigSource, client *http.Client, opts ...RouterOption) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	var o routerOptions
	for _, opt := range opts {
		opt(&o)
	}
	tokens := o.githubTokens
	if tokens == nil {
		tokens = configPATSource{cfgSrc: cfgSrc}
	}
	ghHTTP := *client
	ghHTTP.Transport = githubauth.NewTransport(client.Transport, tokens, githubAPIHost(cfgSrc), githubauth.WithMetrics(metrics.GitHubAuthMetrics{}))
	srv := &RouterServer{
		cfgSrc:       cfgSrc,
		httpClient:   tracing.Client(client),
		githubClient: tracing.ExternalClient(&ghHTTP),
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
func (s *RouterServer) SetDispatcher(d OutboundDispatcher)          { s.dispatcher = d }
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
		if err := metrics.CollectJetStreamQueueMetrics(r.Context(), q, s.discordRouting()); err != nil {
			slog.ErrorContext(r.Context(), "[synapse] [metrics] error collecting jetstream queue metrics", "error", err)
		}
	}
	if s != nil && s.gatewayLatencyFn != nil {
		metrics.SetDiscordGatewayLatency(s.gatewayLatencyFn().Seconds())
	}
	if s != nil && s.cfgSrc != nil {
		if cfg := s.cfgSrc.Current(); cfg != nil {
			metrics.SetGitHubCredentialConfigured(cfg.GitHubCredential)
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}

// readPRRegisterRequest decodes one register body and trims every field,
// returning the raw target alongside its normalized form.
func readPRRegisterRequest(w http.ResponseWriter, r *http.Request) (PRRegisterRequest, string, bool) {
	var req PRRegisterRequest
	if !readWebhookJSON(w, r, 1<<20, &req) {
		return req, "", false
	}
	req.Repo = strings.TrimSpace(req.Repo)
	req.Branch = strings.TrimSpace(req.Branch)
	req.HeadSHA = strings.TrimSpace(req.HeadSHA)
	rawTargetID := strings.TrimSpace(req.TargetID)
	req.TargetID = model.NormalizeDiscordTargetID(rawTargetID)
	req.Title = strings.TrimSpace(req.Title)
	req.BotID = strings.TrimSpace(req.BotID)
	req.Agent = strings.TrimSpace(req.Agent)
	return req, rawTargetID, true
}

func (s *RouterServer) handlePRRegister(w http.ResponseWriter, r *http.Request) {
	s.handlePRRegisterRestate(w, r)
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
		return model.RandomNonce()
	}
	if originPrefix != "" {
		return fmt.Sprintf("%s:%s:%s:%s:%s", originPrefix, evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
	}
	return fmt.Sprintf("%s:%s:%s:%s", evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
}

func (s *RouterServer) enqueueOrProcessDeployEvent(w http.ResponseWriter, r *http.Request, evt DeployEvent, originPrefix string) {
	// The deploy path resolves the PR from RepoObject and the image wait
	// from PRObject when the job runs; nothing is enriched at accept time.
	logPrefix := strutil.FirstNonEmpty(originPrefix, "deploy")
	idempotencyKey := deployEventIdempotencyKey(evt, originPrefix)
	if s.bus != nil {
		if err := s.publishJetStreamJob(r.Context(), DeployWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt}); err != nil {
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

	processed, err := s.ProcessDeployEvent(restateclient.WithEventIdentity(r.Context(), eventIdentity("deploy", idempotencyKey)), evt)
	if err != nil {
		slog.ErrorContext(r.Context(), "[synapse] error processing event", "origin", logPrefix, "error", err)
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:   statusAccepted,
		fieldEvent:    processed.Event,
		fieldJobName:  processed.JobName,
		fieldPRNumber: processed.PRNumber,
		fieldTargetID: processed.TargetID,
	})
}

func (s *RouterServer) isValidAlertTarget(targetID string) bool {
	clean := strings.TrimSpace(targetID)
	if model.IsValidDiscordSnowflake(clean) {
		return true
	}
	sys := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	return sys != "" && clean == sys && model.IsValidDiscordChannel(clean)
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
	return nil
}

// publishUnownedDeployEvent handles a deploy alert that resolved to no PR.
// No PRObject key exists for it, so synapse publishes the PR-less lifecycle
// fact and prompts the system channel itself; Restate is never involved.
func (s *RouterServer) publishUnownedDeployEvent(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	if !s.isValidAlertTarget(evt.TargetID) || s.dispatcher == nil {
		return &evt, nil
	}
	prefix := model.FormatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case eventDeployStarted:
		return &evt, s.publishPRLifecycleEvent(ctx, ownedDeployFact(evt, eventDeployStarted, statusDeploying, ""))
	case eventDeploySuccess:
		if err := s.publishPRLifecycleEvent(ctx, ownedDeployFact(evt, eventDeploySuccess, statusDeployed, "")); err != nil {
			return &evt, err
		}
		return &evt, s.dispatchDeploymentSuccessPrompt(ctx, evt)
	case eventDeployRollback, eventDeployFailed:
		return &evt, s.handleDeployFailedOrRollback(ctx, evt, prefix)
	case eventSyncFailed:
		return &evt, s.handleDeploySyncFailed(ctx, evt, prefix)
	}
	return &evt, nil
}

func (s *RouterServer) handleDeployFailedOrRollback(ctx context.Context, evt DeployEvent, prefix string) error {
	if err := s.publishPRLifecycleEvent(ctx, ownedDeployFact(evt, eventDeployFailed, eventDeployFailed, "")); err != nil {
		return fmt.Errorf("publish deploy lifecycle event: %w", err)
	}
	targetID := strutil.FirstNonEmpty(evt.TargetID, s.currentConfig().SystemChannelID)
	actionDesc, fixDesc := "Deployment failed", "deployment failure"
	if evt.Event == eventDeployRollback {
		actionDesc, fixDesc = "Deployment failed and rolled back", "deployment rollback failure"
	}
	dmMsg := fmt.Sprintf("💥 %s%s for %s. Following up...", prefix, actionDesc, evt.JobName)
	prompt := model.FormatErrorDetailsBlock(
		fmt.Sprintf("%s for job %s on %s (PR #%d, commit: %s).", actionDesc, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
		evt.Details, "", fmt.Sprintf("Please investigate and fix the %s.", fixDesc),
	)
	return s.dispatchAlertWithPrompt(ctx, targetID, evt.Event, dmMsg, prompt)
}

func (s *RouterServer) handleDeploySyncFailed(ctx context.Context, evt DeployEvent, prefix string) error {
	if err := s.publishPRLifecycleEvent(ctx, ownedDeployFact(evt, eventSyncFailed, eventDeployFailed, "")); err != nil {
		return fmt.Errorf("publish deploy lifecycle event: %w", err)
	}
	header := fmt.Sprintf("Git sync failed for repo %s (commit: %s).", evt.Repo, evt.CommitSHA)
	dmMsg := fmt.Sprintf("⚠️ %sGit file sync failed. Following up...", prefix)
	prompt := model.FormatErrorDetailsBlock(header, evt.Details, "", "Please investigate and fix the git sync failure.")
	return s.dispatchAlertWithPrompt(ctx, evt.TargetID, "sync_failed", dmMsg, prompt)
}

// ProcessDeployEvent normalizes one deploy alert and sends it to Restate.
func (s *RouterServer) ProcessDeployEvent(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	return s.processOwnedDeployEvent(ctx, evt)
}

func (s *RouterServer) dispatchDeploymentSuccessPrompt(ctx context.Context, evt DeployEvent) error {
	if s.dispatcher == nil || !s.isValidAlertTarget(evt.TargetID) {
		return nil
	}
	repoPart := ""
	if evt.Repo != "" {
		repoPart = " on " + evt.Repo
	}
	refPart := ""
	if evt.DeploymentID != "" {
		refPart = ", ref: " + evt.DeploymentID
	}
	header := fmt.Sprintf("Continuous Delivery deployment completed for job %s%s (PR #%d, commit: %s%s).", evt.JobName, repoPart, evt.PRNumber, evt.CommitSHA, refPart)
	lines := []string{
		header,
		"",
		"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
	}
	return s.dispatchAlertWithPrompt(ctx, evt.TargetID, "deploy_success", "", strings.Join(lines, "\n"))
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

	if flux.AlertPayloadRepo(alert) == "" {
		alert.Repo = s.resolveFluxAlertSourceRepo(r.Context(), alert)
	}
	deployEvt, ok := flux.TranslateFluxAlertToDeployEvent(alert)
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
	targets := model.DedupeTrimmedStrings(append([]string{req.Target}, req.Targets...))
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
		fp := alerts.VMAlertFingerprint(alert.Labels)
		if alerts.IsVMAlertResolved(alert, now) {
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
		msg := alerts.FormatVMAlertMessage(alert)
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

func resolveJobQueueAndKind(args any) (string, string) {
	switch a := metrics.NormalizeJobArgs(args).(type) {
	case DeployWebhookArgs:
		return metrics.QueueDefault, "deploy_webhook"
	case *DeployWebhookArgs:
		if a != nil {
			return metrics.QueueDefault, "deploy_webhook"
		}
	}
	return metrics.ResolveJobQueueAndKind(args)
}

func (s *RouterServer) publishJetStreamJob(ctx context.Context, args any) error {
	if s == nil || s.bus == nil {
		return errors.New("event bus not configured")
	}
	subject, msgID := metrics.ResolveJetStreamJobCoords(args, s.discordRouting())
	if subject == "" {
		return fmt.Errorf("unsupported job args type %T", args)
	}
	queue, kind := resolveJobQueueAndKind(args)
	if err := s.bus.Publish(ctx, subject, msgID, args); err != nil {
		metrics.RecordJobEnqueued(queue, kind, "error")
		return err
	}
	metrics.RecordJobEnqueued(queue, kind, "enqueued")
	return nil
}

// RunServer runs the HTTP server with graceful shutdown and optional ready callback.
// Fields bound at startup (listener, Discord session, dispatcher)
// are read once from the initial snapshot; config.RestartRequired guards them.
func RunServer(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config], onReady func(addr string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := cfgSrc.Current()
	if err := config.ValidateRequired(cfg); err != nil {
		return fmt.Errorf("invalid synapse config: %w", err)
	}
	server := NewRouterServer(cfgSrc, nil, WithGitHubTokenSource(newGitHubTokenSource(cfgSrc, gitHubAppEnv{
		AppID:          os.Getenv("GITHUB_APP_ID"),
		InstallationID: os.Getenv("GITHUB_APP_INSTALLATION_ID"),
		PrivateKeyPEM:  os.Getenv("GITHUB_APP_PRIVATE_KEY"),
	})))
	dispatcher := NewDefaultOutboundDispatcher(cfg.BrainURL, nil)
	dispatcher.SetConfigSource(cfgSrc)
	dispatcher.SetKubeConfig(cfg.KubeAPIServer, cfg.KubeToken)
	server.SetDispatcher(dispatcher)

	stopJetStream, err := initSynapseJetStream(ctx, server, cfg.NATSURL)
	if err != nil {
		return err
	}
	defer stopJetStream()

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
