package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RouterServer handles webhook requests and orchestrates secret synchronization.
type RouterServer struct {
	cfgSrc                   ConfigSource
	httpClient               *http.Client
	bus                      pubsub.Bus
	registry                 PRRegistryUpdater
	dispatcher               OutboundDispatcher
	onProcessed              func(event, delivery string)
	nonRoboRevFailedSHAs     sync.Map
	cancelledCheckSHAs       sync.Map
	failedCheckSHAs          sync.Map
	failedCheckRuns          sync.Map
	updatedForCurrentMain    sync.Map
	deploySuccessDispatched  sync.Map
	queueStats               consumerStatsQuerier
	gatewayLatencyFn         func() time.Duration
	commitSHAPRs             sync.Map
	gitHistory               gitHistoryCache
	discordIngressRetryDelay time.Duration
	discordIngressMaxAge     time.Duration
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfgSrc ConfigSource, client *http.Client) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &RouterServer{
		cfgSrc:     cfgSrc,
		httpClient: client,
	}
}

func (s *RouterServer) SetEventBus(b pubsub.Bus)                    { s.bus = b }
func (s *RouterServer) EventBus() pubsub.Bus                        { return s.bus }
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater)           { s.registry = reg }
func (s *RouterServer) SetDispatcher(d OutboundDispatcher)          { s.dispatcher = d }
func (s *RouterServer) SetQueueStats(q consumerStatsQuerier)        { s.queueStats = q }
func (s *RouterServer) SetGatewayLatencyFn(fn func() time.Duration) { s.gatewayLatencyFn = fn }

func readWebhookBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes))
	if err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
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
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
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
	mux.Handle("POST /api/webhooks/github", tracedIngress(s.handleGitHubWebhook))
	mux.Handle("POST /webhooks/github", tracedIngress(s.handleGitHubWebhook))
	mux.Handle("POST /api/webhooks/flux", tracedIngress(s.handleFluxWebhook))
	mux.Handle("POST /webhooks/flux", tracedIngress(s.handleFluxWebhook))
	mux.Handle("POST /api/webhooks/events", tracedIngress(s.handleEventsWebhook))
	mux.Handle("POST /webhooks/events", tracedIngress(s.handleEventsWebhook))
	coverageFlush := httpkit.HandleCoverageFlush("synapse")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func (s *RouterServer) consumerStatsQuerier() consumerStatsQuerier {
	if s == nil {
		return nil
	}
	if s.queueStats != nil {
		return s.queueStats
	}
	if csp, ok := s.bus.(consumerStatsQuerier); ok {
		return csp
	}
	return nil
}

func (s *RouterServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if q := s.consumerStatsQuerier(); q != nil {
		if err := collectJetStreamQueueMetrics(r.Context(), q); err != nil {
			log.Printf("[synapse] [metrics] error collecting jetstream queue metrics: %v", err)
		}
	}
	if s != nil && s.gatewayLatencyFn != nil {
		SetDiscordGatewayLatency(s.gatewayLatencyFn().Seconds())
	}
	promhttp.Handler().ServeHTTP(w, r)
}

func (s *RouterServer) resolveHeadlessPRTargetID(ctx context.Context, repo string, prNumber int) string {
	if s.registry != nil {
		if existingTID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNumber); err == nil {
			if norm := NormalizeDiscordTargetID(existingTID); IsValidDiscordSnowflake(norm) {
				return norm
			}
		}
	}
	if sys := NormalizeDiscordTargetID(s.cfgSrc.Current().SystemChannelID); IsValidDiscordSnowflake(sys) {
		return sys
	}
	return ""
}

func (s *RouterServer) handlePRRegister(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.registry == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "registry not initialized"})
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
	req.TargetID = NormalizeDiscordTargetID(rawTargetID)
	req.Title = strings.TrimSpace(req.Title)

	if req.Repo == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "repo cannot be empty"})
		return
	}
	if req.PRNumber <= 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "pr_number must be greater than 0"})
		return
	}
	if req.Branch == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "branch cannot be empty"})
		return
	}
	if req.HeadSHA == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "head_sha cannot be empty"})
		return
	}
	if rawTargetID == "" || (!IsValidDiscordSnowflake(req.TargetID) && IsHeadlessSessionKey(rawTargetID)) {
		req.TargetID = s.resolveHeadlessPRTargetID(r.Context(), req.Repo, req.PRNumber)
	}
	if !IsValidDiscordSnowflake(req.TargetID) {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
		return
	}

	targetID, err := s.registry.RegisterPR(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrInvalidTargetID) || strings.Contains(err.Error(), "target_id") {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
		log.Printf("[synapse] Failed to register PR #%d (%s): %v", req.PRNumber, req.Repo, err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to register PR"})
		return
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":    "registered",
		"repo":      req.Repo,
		"pr_number": req.PRNumber,
		"target_id": targetID,
	})
}

func (s *RouterServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "synapse",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// Deployment Webhook Payloads and Resolution Types

type DeployEvent struct {
	Event        string    `json:"event"`                   // "deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"
	JobName      string    `json:"job_name"`                // Deployment job or service name (e.g. "eri", "synapse")
	SourceKind   string    `json:"source_kind,omitempty"`   // Kind of the Flux object that emitted the event (e.g. "Kustomization", "GitRepository")
	ExpectedJobs []string  `json:"expected_jobs,omitempty"` // All jobs expected to deploy for this PR/sync
	Repo         string    `json:"repo,omitempty"`          // repository name (e.g. "azylman/aerial", "azylman/aerial-config")
	CommitSHA    string    `json:"commit_sha,omitempty"`    // git commit SHA (head or merge commit)
	PRNumber     int       `json:"pr_number,omitempty"`     // PR number if known
	TargetID     string    `json:"target_id,omitempty"`     // Discord thread / channel snowflake
	Image        string    `json:"image,omitempty"`         // container image (e.g. ghcr.io/azylman/aerial-synapse:latest)
	Digest       string    `json:"digest,omitempty"`        // image digest (sha256:...)
	Status       string    `json:"status"`                  // "started", "success", "failed", "rollback"
	DeploymentID string    `json:"deployment_id,omitempty"` // Deployment identifier or UUID
	Details      string    `json:"details,omitempty"`       // status description or error message
	Timestamp    time.Time `json:"timestamp"`
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
	if (evt.Event == "deploy_started" || evt.Event == "deploy_success") && s.shouldTreatDeploySuccessAsSync(r.Context(), evt) {
		log.Printf("[synapse] [%s] ignoring pre-image %s for %s on %s#%d (waiting for image build)", logPrefix, evt.Event, evt.JobName, evt.Repo, evt.PRNumber)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"status":    "ignored",
			"event":     evt.Event,
			"job_name":  evt.JobName,
			"pr_number": evt.PRNumber,
		})
		return
	}
	if s.bus != nil {
		idempotencyKey := deployEventIdempotencyKey(evt, originPrefix)
		if err := s.publishJetStreamJob(r.Context(), DeployWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt}); err != nil {
			log.Printf("[synapse] [%s] ERROR: failed to publish job to jetstream: %v", logPrefix, err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":   "accepted",
			"event":    evt.Event,
			"job_name": evt.JobName,
		})
		return
	}

	processed, err := s.ProcessDeployEvent(r.Context(), evt)
	if err != nil {
		log.Printf("[synapse] [%s] error processing event: %v", logPrefix, err)
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":    "accepted",
		"event":     processed.Event,
		"job_name":  processed.JobName,
		"pr_number": processed.PRNumber,
		"target_id": processed.TargetID,
	})
}

func (s *RouterServer) resolvePRFromRegistrySHA(ctx context.Context, repo, sha string) (int, string, string, bool) {
	if s.registry == nil {
		return 0, "", "", false
	}
	prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
	if err != nil {
		log.Printf("[synapse] [registry] failed resolving PR by SHA %s locally: %v", sha, err)
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
		log.Printf("[synapse] [registry] failed backfilling merge_sha %s for PR %s#%d: %v", sha, repo, prNum, err)
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
	if IsValidDiscordSnowflake(clean) {
		return true
	}
	sys := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	return sys != "" && clean == sys && IsValidDiscordChannel(clean)
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
			log.Printf("[synapse] [dispatcher] warning dispatching %s direct message: %v", label, err)
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
			log.Printf("[synapse] [dispatcher] error dispatching background %s prompt: %v", label, err)
			return fmt.Errorf("dispatch background %s prompt: %w", label, err)
		}
		return nil
	}
	if !s.isValidAlertTarget(targetID) {
		return nil
	}
	req := PromptRequest{ChannelID: targetID, Prompt: prompt}
	if err := s.dispatcher.DispatchPrompt(ctx, req); err != nil {
		log.Printf("[synapse] [dispatcher] error dispatching %s prompt: %v", label, err)
		return fmt.Errorf("dispatch %s prompt: %w", label, err)
	}
	return nil
}

func (s *RouterServer) resolveDeployEventPRNumber(ctx context.Context, evt *DeployEvent) {
	if evt.CommitSHA == "" || evt.PRNumber != 0 {
		return
	}
	prNum, targetID, _, err := s.ResolveSHA(ctx, evt.Repo, evt.CommitSHA)
	if err != nil {
		log.Printf("[synapse] [deploy] failed resolving SHA %s: %v", evt.CommitSHA, err)
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
		log.Printf("[synapse] [deploy] failed resolving thread for PR %s#%d: %v", evt.Repo, evt.PRNumber, err)
		return
	}
	if targetID != "" {
		evt.TargetID = targetID
	}
}

func (s *RouterServer) enrichDeployEventPRAndTarget(ctx context.Context, evt *DeployEvent) {
	s.resolveDeployEventPRNumber(ctx, evt)
	s.resolveDeployEventTargetID(ctx, evt)
	if evt.TargetID == "" && (evt.Event == "deploy_failed" || evt.Event == "deploy_rollback" || evt.Event == "sync_failed") {
		evt.TargetID = strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	}
}

func (s *RouterServer) handleDeployStartedRegistry(ctx context.Context, evt DeployEvent) bool {
	if s.registry == nil || evt.PRNumber == 0 || evt.Event != "deploy_started" || evt.JobName == "" {
		return false
	}
	if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
		log.Printf("[synapse] [deploy] skipping deploy_started for %s on %s#%d (waiting for image build)", evt.JobName, evt.Repo, evt.PRNumber)
		return true
	}
	if len(evt.ExpectedJobs) > 0 {
		if _, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
			log.Printf("[synapse] [registry] warning recording expected jobs on deploy_started for %s#%d: %v", evt.Repo, evt.PRNumber, err)
		}
	}
	if tID, err := s.registry.TransitionDeploying(ctx, evt.Repo, evt.PRNumber, []string{evt.JobName}); err != nil {
		log.Printf("[synapse] [registry] warning transitioning pr %s#%d to deploying for job %s: %v", evt.Repo, evt.PRNumber, evt.JobName, err)
	} else if tID == "" {
		log.Printf("[synapse] [deploy] skipping deploy_started for %s on %s#%d (job already deployed or PR inactive)", evt.JobName, evt.Repo, evt.PRNumber)
		return true
	}
	return false
}

func (s *RouterServer) recordDeploySyncExpectedJobs(ctx context.Context, evt DeployEvent) bool {
	if evt.Event != "sync_success" || evt.PRNumber == 0 {
		return false
	}
	syncHasExpected := len(dedupeTrimmedStrings(evt.ExpectedJobs)) > 0
	if s.registry != nil {
		if regHasExpected, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
			log.Printf("[synapse] [registry] warning recording expected jobs on sync_success for %s#%d: %v", evt.Repo, evt.PRNumber, err)
		} else {
			syncHasExpected = syncHasExpected || regHasExpected
		}
	}
	syncHasExpected = syncHasExpected || s.isWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber)
	return syncHasExpected
}

func (s *RouterServer) handleDeployFailedOrRollback(ctx context.Context, evt DeployEvent, prefix string) error {
	targetID := evt.TargetID
	if s.registry != nil && evt.PRNumber > 0 {
		tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
		if err != nil || !updated {
			return err
		}
		targetID = firstNonEmpty(tID, targetID)
	}
	_ = s.publishDeployLifecycleEvent(ctx, evt, "deploy_failed", "deploy_failed")
	if evt.PRNumber > 0 && s.bus != nil {
		return nil
	}
	if s.cfgSrc != nil {
		targetID = firstNonEmpty(targetID, s.cfgSrc.Current().SystemChannelID)
	}
	actionDesc, fixDesc := "Deployment failed", "deployment failure"
	if evt.Event == "deploy_rollback" {
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
	_ = s.publishDeployLifecycleEvent(ctx, evt, "sync_failed", "deploy_failed")
	if evt.PRNumber > 0 && s.bus != nil {
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

func (s *RouterServer) isWaitingForImageBuild(ctx context.Context, repo string, prNumber int) bool {
	if s == nil || s.registry == nil || strings.TrimSpace(repo) == "" || prNumber <= 0 {
		return false
	}
	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		return false
	}
	qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var status string
	var metaBytes []byte
	err := pgReg.pool.QueryRow(qCtx, `SELECT status, COALESCE(metadata, '{}'::jsonb) FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, normalizeRepo(repo), prNumber).Scan(&status, &metaBytes)
	if err != nil {
		return false
	}
	normStatus := strings.ToLower(strings.TrimSpace(status))
	if normStatus != "open" && normStatus != "merged" && normStatus != "synced" {
		return false
	}
	var meta map[string]any
	if len(metaBytes) > 0 {
		if uErr := json.Unmarshal(metaBytes, &meta); uErr != nil {
			meta = nil
		}
	}
	if len(extractStringSliceFromMetadata(meta, "started_jobs")) > 0 {
		return false
	}
	checks := extractChecksFromJSONOrMetadata("", meta)
	checkJobs, passedBuildJobs, total, passed, failed := analyzeBuildChecks(checks)
	_ = total + passed + failed + len(passedBuildJobs)
	return len(checkJobs) > 0
}

func (s *RouterServer) shouldTreatDeploySuccessAsSync(ctx context.Context, evt DeployEvent) bool {
	return evt.SourceKind == "Kustomization" && s.isWaitingForImageBuild(ctx, evt.Repo, evt.PRNumber)
}

// isDeployEventOwnedByWatcher reports whether evt concerns a PR whose rollout completion is tracked by the
// Kustomization watcher: such PRs ignore Kustomization alerts and never complete from a deploy_success
// event, since live Kustomization status is the only source of truth for them.
func (s *RouterServer) isDeployEventOwnedByWatcher(ctx context.Context, evt DeployEvent) bool {
	if s.registry == nil || evt.PRNumber <= 0 || (evt.SourceKind != "Kustomization" && evt.Event != "deploy_success") {
		return false
	}
	tracked, err := s.registry.HasKustomizationRequests(ctx, evt.Repo, evt.PRNumber)
	if err != nil {
		log.Printf("[synapse] [deploy] warning checking kustomization requests for %s#%d: %v", evt.Repo, evt.PRNumber, err)
	}
	return tracked
}

func (s *RouterServer) ProcessDeployEvent(ctx context.Context, evt DeployEvent) (*DeployEvent, error) {
	s.enrichDeployEventPRAndTarget(ctx, &evt)

	log.Printf("[synapse] [deploy] event=%s job=%s repo=%s pr=%d commit=%s status=%s target_id=%s deployment_id=%s details=%q",
		evt.Event, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA, evt.Status, evt.TargetID, evt.DeploymentID, evt.Details)

	s.evaluateFluxEventTriggers(ctx, evt)

	if s.isDeployEventOwnedByWatcher(ctx, evt) {
		log.Printf("[synapse] [deploy] deferring %s for %s on %s#%d to the kustomization watcher", evt.Event, evt.JobName, evt.Repo, evt.PRNumber)
		return &evt, nil
	}

	if s.handleDeployStartedRegistry(ctx, evt) {
		return &evt, nil
	}

	syncHasExpected := s.recordDeploySyncExpectedJobs(ctx, evt)
	if deferred, err := s.deferSyncToWatcher(ctx, evt, syncHasExpected); deferred || err != nil {
		return &evt, err
	}

	if !s.isValidAlertTarget(evt.TargetID) || s.dispatcher == nil {
		return &evt, nil
	}

	prefix := formatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case "deploy_started":
		_ = s.publishDeployLifecycleEvent(ctx, evt, "deploy_started", "deploying")
		return &evt, nil
	case "deploy_success":
		if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
			return &evt, s.handleDeploySyncSuccess(ctx, evt, true)
		}

		if s.shouldSuppressDuplicateDeploySuccess(ctx, evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber) {
			log.Printf("[synapse] [deploy] suppressing duplicate deploy_success for repo=%s job=%s pr=%d commit=%s",
				evt.Repo, evt.JobName, evt.PRNumber, evt.CommitSHA)
			return &evt, nil
		}
		_ = s.publishDeployLifecycleEvent(ctx, evt, "deploy_success", "deployed")
		s.recordDeploySuccessDispatched(evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber)
		return &evt, s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, evt.JobName, evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID)
	case "deploy_rollback", "deploy_failed":
		return &evt, s.handleDeployFailedOrRollback(ctx, evt, prefix)
	case "sync_success":
		return &evt, s.handleDeploySyncSuccess(ctx, evt, syncHasExpected)
	case "sync_failed":
		return &evt, s.handleDeploySyncFailed(ctx, evt, prefix)
	}
	return &evt, nil
}

func (s *RouterServer) recordDeploySuccessDispatched(repo, jobName, commitSHA string, prNum int) {
	normRepo := normalizeRepo(repo)
	if prNum > 0 {
		s.deploySuccessDispatched.Store(fmt.Sprintf("pr:%s:%d", normRepo, prNum), true)
	}
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(commitSHA)
	if cleanSHA != "" && cleanJob != "" {
		s.deploySuccessDispatched.Store(fmt.Sprintf("sha:%s:%s:%s", normRepo, cleanJob, cleanSHA), true)
	}
}

func (s *RouterServer) shouldSuppressDuplicateDeploySuccess(ctx context.Context, repo, jobName, commitSHA string, prNum int) bool {
	normRepo := normalizeRepo(repo)
	cleanJob := strings.TrimSpace(jobName)
	cleanSHA := strings.TrimSpace(commitSHA)

	if prNum > 0 {
		if _, ok := s.deploySuccessDispatched.Load(fmt.Sprintf("pr:%s:%d", normRepo, prNum)); ok {
			return true
		}
	}
	if cleanSHA != "" && cleanJob != "" {
		if _, ok := s.deploySuccessDispatched.Load(fmt.Sprintf("sha:%s:%s:%s", normRepo, cleanJob, cleanSHA)); ok {
			return true
		}
	}

	if prNum > 0 && s.registry != nil {
		if pgReg, ok := s.registry.(*PostgresPRRegistry); ok && pgReg != nil && pgReg.pool != nil {
			qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var status string
			err := pgReg.pool.QueryRow(qCtx, `SELECT status FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, normRepo, prNum).Scan(&status)
			if err == nil && strings.EqualFold(strings.TrimSpace(status), "deployed") {
				s.recordDeploySuccessDispatched(repo, jobName, commitSHA, prNum)
				return true
			}
		}
	}

	return false
}

func (s *RouterServer) handleDeploySyncSuccess(ctx context.Context, evt DeployEvent, syncHasExpected bool) error {
	if evt.PRNumber <= 0 {
		return nil
	}
	if syncHasExpected {
		_ = s.publishDeployLifecycleEvent(ctx, evt, "sync_success", "synced")
		return nil
	}
	s.recordDeploySuccessDispatched(evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber)
	_ = s.publishDeployLifecycleEvent(ctx, evt, "deploy_success", "deployed")
	if evt.PRNumber > 0 && s.bus != nil {
		return nil
	}
	return s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, firstNonEmpty(evt.JobName, "git-sync"), evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID)
}

func (s *RouterServer) isDependabotPRNumber(ctx context.Context, repo string, prNum int) bool {
	if s == nil || s.registry == nil || prNum <= 0 {
		return false
	}
	_, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(repo), prNum)
	return err == nil && isDependabotBranch(branch)
}

func (s *RouterServer) dispatchDeploymentSuccessPrompt(ctx context.Context, targetID, jobName, repo, commitSHA string, prNum int, refID string) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}
	if prNum > 0 && s.bus != nil {
		return nil
	}
	if s.isDependabotPRNumber(ctx, repo, prNum) {
		log.Printf("[synapse] [dependabot] suppressing deploy_success /prompt wake for %s#%d (job=%s)", repo, prNum, jobName)
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
	server.SetRegistry(NewPostgresPRRegistry(pool))
	log.Println("[synapse] PostgreSQL PR registry initialized")
	return pool, nil
}

// runServer runs the HTTP server with graceful shutdown and optional ready callback.
// Fields bound at startup (listener, database pool, Discord session, dispatcher)
// are read once from the initial snapshot; synapseRestartRequired guards them.
func runServer(ctx context.Context, cfgSrc *httpkit.Reloadable[Config], onReady func(addr string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := cfgSrc.Current()
	server := NewRouterServer(cfgSrc, nil)
	dispatcher := NewDefaultOutboundDispatcher(cfg.BrainURL, nil)
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
		pool, err := setupPostgres(ctx, server, cfg.PostgresURL)
		if err != nil {
			return err
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[synapse] HTTP shutdown error: %v", shutErr)
	}

	safeStopDiscord()

	stopWatcher()

	log.Println("[synapse] Server stopped")
	return runErr
}

// runApp runs synapse until ctx is cancelled or a config reload requests a
// restart, in which case it shuts down gracefully and returns nil so the
// container restarts with the new configuration.
func runApp(ctx context.Context, cfgSrc *httpkit.Reloadable[Config]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-cfgSrc.RestartRequested():
			cancel()
		case <-ctx.Done():
		}
	}()
	return runServer(ctx, cfgSrc, nil)
}

func main() {
	cfg := LoadConfigFromEnv()
	cfgSrc := httpkit.NewReloadable(&cfg, loadConfigFromEnvStrict, synapseRestartRequired)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	_, flushTracing := tracing.Init(ctx, "synapse", tracing.DefaultConfigPath)

	err := runApp(ctx, cfgSrc)
	cancel()
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[synapse] Error: %v", err)
	}
}
