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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// RouterServer handles webhook requests and orchestrates secret synchronization.
type RouterServer struct {
	cfgSrc                   ConfigSource
	httpClient               *http.Client
	riverClient              RiverInserter
	registry                 PRRegistryUpdater
	dispatcher               OutboundDispatcher
	onProcessed              func(event, delivery string)
	nonRoboRevFailedSHAs     sync.Map
	cancelledCheckSHAs       sync.Map
	failedCheckSHAs          sync.Map
	failedCheckRuns          sync.Map
	updatedForCurrentMain    sync.Map
	buildCompletedDispatched sync.Map
	deploySuccessDispatched  sync.Map
	queueStats               queueStatsQuerier
	gatewayLatencyFn         func() time.Duration
	commitSHAPRs             sync.Map
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfgSrc ConfigSource, client *http.Client, riverClient ...RiverInserter) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	s := &RouterServer{
		cfgSrc:     cfgSrc,
		httpClient: client,
	}
	if len(riverClient) > 0 {
		s.riverClient = riverClient[0]
	}
	return s
}

func (s *RouterServer) SetRiverClient(rc RiverInserter)             { s.riverClient = rc }
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater)           { s.registry = reg }
func (s *RouterServer) SetDispatcher(d OutboundDispatcher)          { s.dispatcher = d }
func (s *RouterServer) SetQueueStats(q queueStatsQuerier)           { s.queueStats = q }
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
	mux.HandleFunc("POST /internal/pr/register", s.handlePRRegister)
	mux.HandleFunc("POST /internal/widget/sync", s.handleInternalWidgetSync)
	mux.HandleFunc("POST /api/webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /api/webhooks/flux", s.handleFluxWebhook)
	mux.HandleFunc("POST /webhooks/flux", s.handleFluxWebhook)
	mux.HandleFunc("POST /api/webhooks/events", s.handleEventsWebhook)
	mux.HandleFunc("POST /webhooks/events", s.handleEventsWebhook)
	coverageFlush := httpkit.HandleCoverageFlush("synapse")
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func (s *RouterServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s != nil && s.queueStats != nil {
		if err := collectRiverQueueMetrics(r.Context(), s.queueStats); err != nil {
			log.Printf("[synapse] [metrics] error collecting river queue metrics: %v", err)
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
	chunkID := strings.TrimSpace(req.VanadisChunkID)

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
	if chunkID == "" {
		if rawTargetID == "" || (!IsValidDiscordSnowflake(req.TargetID) && IsHeadlessSessionKey(rawTargetID)) {
			req.TargetID = s.resolveHeadlessPRTargetID(r.Context(), req.Repo, req.PRNumber)
		}
		if !IsValidDiscordSnowflake(req.TargetID) {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
	}

	targetID, err := s.registry.RegisterPR(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrVanadisChunkNotFound) {
			httpkit.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "vanadis chunk not found"})
			return
		}
		if errors.Is(err, ErrVanadisChunkCancelled) {
			httpkit.WriteJSON(w, http.StatusConflict, map[string]string{"error": "vanadis chunk or build is cancelled"})
			return
		}
		if errors.Is(err, ErrInvalidTargetID) || strings.Contains(err.Error(), "target_id") {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
		log.Printf("[synapse] Failed to register PR #%d (%s): %v", req.PRNumber, req.Repo, err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to register PR"})
		return
	}

	if err := s.SyncPRWidget(r.Context(), req.Repo, req.PRNumber); err != nil {
		log.Printf("[synapse] failed to sync initial PR widget for %s#%d: %v", req.Repo, req.PRNumber, err)
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":    "registered",
		"repo":      req.Repo,
		"pr_number": req.PRNumber,
		"target_id": targetID,
	})
}

func (s *RouterServer) handleInternalWidgetSync(w http.ResponseWriter, r *http.Request) {
	var req WidgetSyncArgs
	if !readWebhookJSON(w, r, 1<<20, &req) {
		return
	}
	if buildID := strings.TrimSpace(req.BuildID); buildID != "" {
		if err := s.SyncBuildWidget(r.Context(), buildID, req.Banner); err != nil {
			log.Printf("[synapse] [widget] failed to sync build widget for %s: %v", buildID, err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to sync build widget"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":   "accepted",
			"build_id": buildID,
		})
		return
	}
	if repo := strings.TrimSpace(req.Repo); repo != "" && req.PRNumber > 0 {
		normRepo := normalizeRepo(repo)
		if err := s.SyncPRWidget(r.Context(), normRepo, req.PRNumber); err != nil {
			log.Printf("[synapse] [widget] failed to sync pr widget for %s#%d: %v", normRepo, req.PRNumber, err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to sync pr widget"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":    "accepted",
			"repo":      normRepo,
			"pr_number": req.PRNumber,
		})
		return
	}
	httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "build_id or repo and pr_number are required"})
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
	if s.riverClient != nil {
		idempotencyKey := deployEventIdempotencyKey(evt, originPrefix)
		if err := s.insertRiverJob(r.Context(), DeployWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt}); err != nil {
			log.Printf("[synapse] [%s] ERROR: failed to insert job into river: %v", logPrefix, err)
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

func (s *RouterServer) setPRWidgetState(ctx context.Context, repo string, prNumber int, status, banner string) {
	if s == nil || s.registry == nil || repo == "" || prNumber <= 0 {
		return
	}
	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		return
	}
	qCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	query := `
		UPDATE pr_registry
		SET status = CASE
		        WHEN $3 = '' OR status IN ('deployed', 'closed') THEN status
		        WHEN $3 = 'synced' AND status = 'deploying' THEN status
		        ELSE $3
		    END,
		    metadata = CASE
		        WHEN $3 = 'deployed' THEN jsonb_set(
		            jsonb_set(
		                CASE
		                    WHEN $4 != '' THEN jsonb_set(COALESCE(metadata, '{}'::jsonb), '{banner}', to_jsonb($4::text), true)
		                    ELSE COALESCE(metadata, '{}'::jsonb) - 'banner'
		                END,
		                '{jobs}', '[]'::jsonb, true
		            ),
		            '{deployed_jobs}', COALESCE(metadata->'started_jobs', metadata->'deployed_jobs', '[]'::jsonb), true
		        )
		        WHEN $4 != '' THEN jsonb_set(COALESCE(metadata, '{}'::jsonb), '{banner}', to_jsonb($4::text), true)
		        ELSE COALESCE(metadata, '{}'::jsonb) - 'banner'
		    END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2
		  AND NOT ($3 = 'deployed' AND status <> 'deployed' AND COALESCE(jsonb_typeof(metadata->'` + kustomizationRequestsKey + `') = 'object', false))`
	if _, err := pgReg.pool.Exec(qCtx, query, normalizeRepo(repo), prNumber, strings.TrimSpace(status), strings.TrimSpace(banner)); err != nil {
		log.Printf("[synapse] [widget] warning updating pr widget state for %s#%d: %v", repo, prNumber, err)
	}
}

func (s *RouterServer) resolvePRWidgetSyncStatus(ctx context.Context, repo string, prNumber int) string {
	if s == nil || s.registry == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		return ""
	}
	qCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	var status string
	if err := pgReg.pool.QueryRow(qCtx, "SELECT status FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1", normalizeRepo(repo), prNumber).Scan(&status); err != nil {
		return ""
	}
	return strings.TrimSpace(status)
}

func (s *RouterServer) SyncPRWidget(ctx context.Context, repo string, prNumber int) error {
	if s == nil || s.dispatcher == nil || s.registry == nil || repo == "" || prNumber <= 0 {
		return nil
	}
	args := WidgetSyncArgs{
		Repo:     normalizeRepo(repo),
		PRNumber: prNumber,
	}
	if s.riverClient != nil {
		args.Status = s.resolvePRWidgetSyncStatus(ctx, args.Repo, prNumber)
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[synapse] [widget] error enqueuing widget sync into river: %v", err)
			return fmt.Errorf("enqueue widget sync: %w", err)
		}
		return nil
	}
	return s.ProcessWidgetSync(ctx, args)
}

func (s *RouterServer) SyncBuildWidget(ctx context.Context, buildID, banner string) error {
	buildID = strings.TrimSpace(buildID)
	banner = strings.TrimSpace(banner)
	if s == nil || s.dispatcher == nil || s.registry == nil || buildID == "" {
		return nil
	}
	args := WidgetSyncArgs{
		BuildID: buildID,
		Banner:  banner,
	}
	if s.riverClient != nil {
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[synapse] [widget] error enqueuing build widget sync into river: %v", err)
			return fmt.Errorf("enqueue build widget sync: %w", err)
		}
		return nil
	}
	return s.ProcessWidgetSync(ctx, args)
}

func (s *RouterServer) ProcessWidgetSync(ctx context.Context, args WidgetSyncArgs) error {
	if s == nil || s.dispatcher == nil || s.registry == nil {
		return nil
	}
	if buildID := strings.TrimSpace(args.BuildID); buildID != "" {
		pgReg, ok := s.registry.(*PostgresPRRegistry)
		if !ok || pgReg == nil {
			return nil
		}
		qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		return s.syncVanadisBuildWidget(qCtx, pgReg, buildID, strings.TrimSpace(args.Banner))
	}

	repo := normalizeRepo(args.Repo)
	prNumber := args.PRNumber
	if repo == "" || prNumber <= 0 {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		targetID, branch, _, _, err := s.registry.ResolvePRByNumber(qCtx, repo, prNumber)
		if err != nil {
			log.Printf("[synapse] [widget] resolve pr error: %v", err)
		}
		if targetID == "" || isDependabotBranch(branch) {
			return nil
		}
		widgetReq := StatusWidgetRequest{
			ChannelID: targetID,
			ID:        fmt.Sprintf("pr-%s-%d", repo, prNumber),
			Title:     branch,
			Status:    "open",
			Chunks: []StatusWidgetChunk{
				{
					Index:    0,
					Repo:     repo,
					Title:    branch,
					Status:   "open",
					PRNumber: prNumber,
				},
			},
		}
		_, err = s.dispatcher.DispatchWidget(qCtx, widgetReq)
		return err
	}

	return s.syncSinglePRWidget(qCtx, pgReg, repo, prNumber, true)
}

// sweepOlderDeployedPRWidgets marks older in-flight PRs deployed once a newer one is. Watcher-tracked
// rollouts still deploying are excluded: only the Kustomization watcher completes them, so their
// completion wake or failure alert is never skipped.
func (s *RouterServer) sweepOlderDeployedPRWidgets(qCtx context.Context, pgReg *PostgresPRRegistry, repo string, prNumber int) {
	sweepQuery := `UPDATE pr_registry SET status = 'deployed', metadata = jsonb_set(jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', '[]'::jsonb, true), '{deployed_jobs}', COALESCE(metadata->'started_jobs', metadata->'deployed_jobs', '[]'::jsonb), true), updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number < $2 AND status IN ('merged', 'synced', 'deploying', 'deploy_failed') AND NOT (status IN ('merged', 'synced') AND COALESCE((metadata->'ci_checks')::text, '') LIKE '%Build & Push (%') AND NOT (status = 'deploying' AND COALESCE(jsonb_typeof(metadata->'` + kustomizationRequestsKey + `') = 'object', false)) RETURNING pr_number`
	rows, sErr := pgReg.pool.Query(qCtx, sweepQuery, repo, prNumber)
	if sErr != nil || rows == nil {
		return
	}
	var olderPRs []int
	for rows.Next() {
		var oldPR int
		if rows.Scan(&oldPR) == nil && oldPR > 0 {
			olderPRs = append(olderPRs, oldPR)
		}
	}
	rows.Close()
	for _, oldPR := range olderPRs {
		if wErr := s.syncSinglePRWidget(qCtx, pgReg, repo, oldPR, false); wErr != nil {
			log.Printf("[synapse] [widget] warning syncing older deployed PR %s#%d: %v", repo, oldPR, wErr)
		}
	}
}

func (s *RouterServer) postVanadisSyncWidget(qCtx context.Context, pgReg *PostgresPRRegistry, payload map[string]any) (bool, error) {
	var syncRes struct {
		Matched bool   `json:"matched"`
		BuildID string `json:"build_id"`
	}
	if err := pgReg.postVanadisJSON(qCtx, "/chunks/sync-pr-widget", "sync chunk widget", "sync vanadis pr widget", payload, &syncRes); err != nil {
		return false, err
	}
	return syncRes.Matched, nil
}

func (s *RouterServer) syncVanadisChunkPRWidget(qCtx context.Context, pgReg *PostgresPRRegistry, repo string, prNumber int, prStatus, prBanner, activeStep string, prFound bool) (bool, error) {
	return s.postVanadisSyncWidget(qCtx, pgReg, map[string]any{
		"repo":        repo,
		"pr_number":   prNumber,
		"pr_status":   prStatus,
		"pr_banner":   prBanner,
		"active_step": activeStep,
		"pr_found":    prFound,
	})
}

func (s *RouterServer) syncVanadisBuildWidget(qCtx context.Context, pgReg *PostgresPRRegistry, buildID, banner string) error {
	_, err := s.postVanadisSyncWidget(qCtx, pgReg, map[string]any{
		"build_id":  buildID,
		"pr_banner": banner,
	})
	return err
}

func shouldSkipWidgetSink(msgID, activeStep, status string) bool {
	if msgID == "" || activeStep == "" {
		return false
	}
	switch status {
	case "open", "merged", "synced", "deploying":
		return true
	default:
		return false
	}
}

func prWidgetBanner(status, banner string) string {
	if status == "failed" || status == "conflict" {
		return banner
	}
	return ""
}

func (s *RouterServer) syncSinglePRWidget(qCtx context.Context, pgReg *PostgresPRRegistry, repo string, prNumber int, allowSweep bool) error {
	prQuery := `SELECT target_id, title, status, status_message_id, created_at, updated_at, COALESCE(metadata->>'banner', ''), COALESCE((metadata->'ci_checks')::text, '{}'), COALESCE(metadata::text, '{}'), COALESCE(branch, '') FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1`
	var targetID, title, prStatus, prMsgID, prBanner, ciChecksJSON, metadataJSON, branch string
	var prCreatedAt, prUpdatedAt time.Time
	prErr := pgReg.pool.QueryRow(qCtx, prQuery, repo, prNumber).Scan(&targetID, &title, &prStatus, &prMsgID, &prCreatedAt, &prUpdatedAt, &prBanner, &ciChecksJSON, &metadataJSON, &branch)
	if prErr != nil && !errors.Is(prErr, pgx.ErrNoRows) {
		return fmt.Errorf("sync pr widget query pr: %w", prErr)
	}
	prFound := prErr == nil
	if prFound && isDependabotBranch(branch) {
		return nil
	}

	if allowSweep && prFound && strings.EqualFold(strings.TrimSpace(prStatus), "deployed") {
		s.sweepOlderDeployedPRWidgets(qCtx, pgReg, repo, prNumber)
	}

	activeStep := formatPRWidgetActiveStep(prStatus, ciChecksJSON, metadataJSON)

	// 1. Ask vanadis to sync widget if this PR belongs to a vanadis chunk
	matched, err := s.syncVanadisChunkPRWidget(qCtx, pgReg, repo, prNumber, prStatus, prBanner, activeStep, prFound)
	if err != nil {
		return err
	}
	if matched {
		return nil
	}

	// 2. Standalone PR in pr_registry
	if !prFound || targetID == "" {
		return nil
	}

	finalStatus := firstNonEmpty(normalizeWidgetStatus(prStatus), "open")
	widgetBanner := prWidgetBanner(finalStatus, prBanner)
	widgetTitle := firstNonEmpty(title, fmt.Sprintf("PR #%d", prNumber))

	widgetReq := StatusWidgetRequest{
		ChannelID: targetID,
		MessageID: prMsgID,
		ID:        fmt.Sprintf("pr-%s-%d", repo, prNumber),
		Title:     widgetTitle,
		Status:    finalStatus,
		Banner:    widgetBanner,
		SkipSink:  shouldSkipWidgetSink(prMsgID, activeStep, finalStatus),
		StartedAt: prCreatedAt,
		Chunks: []StatusWidgetChunk{
			{
				Index:      0,
				Repo:       repo,
				Title:      widgetTitle,
				Status:     finalStatus,
				PRNumber:   prNumber,
				StartedAt:  prCreatedAt,
				UpdatedAt:  prUpdatedAt,
				ActiveStep: activeStep,
			},
		},
	}

	newMsgID, dispErr := s.dispatcher.DispatchWidget(qCtx, widgetReq)
	if dispErr != nil {
		return fmt.Errorf("dispatch pr widget: %w", dispErr)
	}
	if newMsgID != "" && newMsgID != prMsgID {
		updatePRMsgQuery := `UPDATE pr_registry SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE repo = $2 AND pr_number = $3`
		if _, execErr := pgReg.pool.Exec(qCtx, updatePRMsgQuery, newMsgID, repo, prNumber); execErr != nil {
			log.Printf("[synapse] [widget] warning updating pr status_message_id: %v", execErr)
		}
	}
	return nil
}

type vanadisPromptOpts struct {
	silent  bool
	chunkID string
	effort  string
}

func resolvePostgresVanadisOpts(ctx context.Context, pgReg *PostgresPRRegistry, repo string, prNum int, opts *vanadisPromptOpts) {
	if pgReg == nil {
		return
	}
	matched, chunkID, effort, err := pgReg.ResolveVanadisChunkForPR(ctx, repo, prNum)
	if err == nil && matched && strings.TrimSpace(chunkID) != "" {
		opts.chunkID = strings.TrimSpace(chunkID)
		if effort != "" {
			opts.effort = effort
		}
		return
	}
	if persisted := pgReg.getPersistedVanadisChunkID(ctx, repo, prNum); persisted != "" {
		opts.chunkID = persisted
	}
}

func (s *RouterServer) vanadisPromptOpts(ctx context.Context, repo string, prNum int) vanadisPromptOpts {
	if prNum <= 0 {
		return vanadisPromptOpts{}
	}
	opts := vanadisPromptOpts{silent: true, effort: "medium"}
	if s == nil || s.registry == nil {
		return opts
	}
	if pgReg, ok := s.registry.(*PostgresPRRegistry); ok {
		resolvePostgresVanadisOpts(ctx, pgReg, repo, prNum, &opts)
	}
	return opts
}

func (s *RouterServer) isVanadisPR(ctx context.Context, repo string, prNum int) (bool, error) {
	if s.registry == nil || prNum <= 0 {
		return false, nil
	}
	return s.registry.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
}

// advanceVanadisChunkOnMerge starts the next chunk on merge only when it is a
// PR chunk; every other case waits for the deploy path to advance the build.
func (s *RouterServer) advanceVanadisChunkOnMerge(ctx context.Context, repo string, prNumber int) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	advance, err := s.registry.AdvancesVanadisChunkOnMerge(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if !advance {
		return nil
	}
	return s.advanceVanadisChunk(ctx, repo, prNumber)
}

func (s *RouterServer) advanceVanadisChunkOnSync(ctx context.Context, repo string, prNumber int, syncHasExpected bool) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	if syncHasExpected {
		advance, err := s.registry.AdvancesVanadisChunkOnMerge(ctx, repo, prNumber)
		if err != nil {
			return err
		}
		if !advance {
			log.Printf("[synapse] [vanadis] deferring chunk advancement on sync_success for %s#%d until deploy_success", repo, prNumber)
			return nil
		}
	}
	return s.advanceVanadisChunk(ctx, repo, prNumber)
}

func (s *RouterServer) isValidAlertTarget(targetID string) bool {
	clean := strings.TrimSpace(targetID)
	if IsValidDiscordSnowflake(clean) {
		return true
	}
	sys := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	return sys != "" && clean == sys && IsValidDiscordChannel(clean)
}

func isValidVanadisChunkID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 1 || len(id) > 120 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func (s *RouterServer) dispatchAlertWithPrompt(ctx context.Context, targetID, label, dmMsg, prompt string, vOpts ...vanadisPromptOpts) error {
	if s.dispatcher == nil {
		return nil
	}
	var opt vanadisPromptOpts
	if len(vOpts) > 0 {
		opt = vOpts[0]
	}
	if !opt.silent && !s.isValidAlertTarget(targetID) {
		return nil
	}
	if !opt.silent && dmMsg != "" && s.isValidAlertTarget(targetID) {
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: targetID, Content: dmMsg}); err != nil {
			log.Printf("[synapse] [dispatcher] warning dispatching %s direct message: %v", label, err)
		}
	}
	if opt.silent {
		var normTarget string
		if nt := NormalizeDiscordTargetID(targetID); IsValidDiscordSnowflake(nt) {
			normTarget = nt
		}
		var cID string
		if rawCID := strings.TrimSpace(opt.chunkID); isValidVanadisChunkID(rawCID) {
			cID = rawCID
		}
		req := DispatchRequest{
			Source:         "webhook",
			Prompt:         prompt,
			Effort:         firstNonEmpty(opt.effort, "medium"),
			TargetID:       normTarget,
			VanadisChunkID: cID,
		}
		if cID != "" {
			req.SessionID = "vanadis-" + cID
		}
		if err := s.dispatcher.DispatchBackground(ctx, req); err != nil {
			log.Printf("[synapse] [dispatcher] error dispatching %s background dispatch: %v", label, err)
			return fmt.Errorf("dispatch %s background dispatch: %w", label, err)
		}
		return nil
	}
	req := PromptRequest{ChannelID: targetID, Prompt: prompt, Effort: opt.effort}
	if err := s.dispatcher.DispatchPrompt(ctx, req); err != nil {
		log.Printf("[synapse] [dispatcher] error dispatching %s prompt: %v", label, err)
		return fmt.Errorf("dispatch %s prompt: %w", label, err)
	}
	return nil
}

func (s *RouterServer) isBuildCompletedDispatched(buildID string) bool {
	clean := strings.TrimSpace(buildID)
	if clean == "" {
		return false
	}
	_, ok := s.buildCompletedDispatched.Load(clean)
	return ok
}

func (s *RouterServer) advanceVanadisChunk(ctx context.Context, repo string, prNumber int) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	res, err := s.registry.AdvanceVanadisChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if res == nil || !res.Matched {
		return nil
	}
	if res.Advanced {
		s.evaluateVanadisAdvancementTriggers(ctx, repo, prNumber, res)
	}
	if err := s.SyncPRWidget(ctx, repo, prNumber); err != nil {
		log.Printf("[synapse] [vanadis] warning syncing chunk widget for %s#%d: %v", repo, prNumber, err)
	}
	if res.BuildCompleted && !s.isBuildCompletedDispatched(res.BuildID) {
		prompt := formatVanadisBuildCompletedPrompt(res)
		if err := s.dispatchAlertWithPrompt(ctx, res.ThreadID, "vanadis_build_completed", "", prompt); err != nil {
			return err
		}
		if clean := strings.TrimSpace(res.BuildID); clean != "" {
			s.buildCompletedDispatched.Store(clean, true)
		}
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
	targetID = firstNonEmpty(targetID, s.cfgSrc.Current().SystemChannelID)
	actionDesc, fixDesc := "Deployment failed", "deployment failure"
	if evt.Event == "deploy_rollback" {
		actionDesc, fixDesc = "Deployment failed and rolled back", "deployment rollback failure"
	}
	dmMsg := fmt.Sprintf("💥 %s%s for %s. Following up...", prefix, actionDesc, evt.JobName)
	banner := "Deployment failed for " + evt.JobName
	if evt.Event == "deploy_rollback" {
		banner = "Deployment failed and rolled back for " + evt.JobName
	}
	if evt.PRNumber > 0 {
		s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deploy_failed", banner)
		if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err == nil {
			dmMsg = ""
		}
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("%s for job %s on %s (PR #%d, commit: %s).", actionDesc, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA),
		evt.Details, "", fmt.Sprintf("Please investigate and fix the %s.", fixDesc),
	)
	if err := s.dispatchAlertWithPrompt(ctx, targetID, evt.Event, dmMsg, prompt, s.vanadisPromptOpts(ctx, evt.Repo, evt.PRNumber)); err != nil {
		s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deploying", "")
		return err
	}
	return nil
}

func (s *RouterServer) handleDeploySyncFailed(ctx context.Context, evt DeployEvent, prefix string) error {
	header := fmt.Sprintf("Git sync failed for repo %s (commit: %s, PR #%d).", evt.Repo, evt.CommitSHA, evt.PRNumber)
	if evt.PRNumber == 0 {
		header = fmt.Sprintf("Git sync failed for repo %s (commit: %s).", evt.Repo, evt.CommitSHA)
	}
	dmMsg := fmt.Sprintf("⚠️ %sGit file sync failed. Following up...", prefix)
	if evt.PRNumber > 0 {
		s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deploy_failed", "Git file sync failed")
		if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err == nil {
			dmMsg = ""
		}
	}
	prompt := formatErrorDetailsBlock(header, evt.Details, "", "Please investigate and fix the git sync failure.")
	return s.dispatchAlertWithPrompt(ctx, evt.TargetID, "sync_failed", dmMsg, prompt, s.vanadisPromptOpts(ctx, evt.Repo, evt.PRNumber))
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
	_ = passedBuildJobs
	_ = total
	_ = passed
	_ = failed
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

	if !s.isValidAlertTarget(evt.TargetID) || s.dispatcher == nil {
		if evt.Event == "sync_success" && evt.PRNumber > 0 {
			return &evt, s.advanceVanadisChunkOnSync(ctx, evt.Repo, evt.PRNumber, syncHasExpected)
		}
		return &evt, nil
	}

	prefix := formatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case "deploy_started":
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err != nil {
				log.Printf("[synapse] [widget] warning syncing deploy_started widget: %v", err)
				return &evt, err
			}
		}
		return &evt, nil
	case "deploy_success":
		if s.shouldTreatDeploySuccessAsSync(ctx, evt) {
			return &evt, s.handleDeploySyncSuccess(ctx, evt, true)
		}

		if s.shouldSuppressDuplicateDeploySuccess(ctx, evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber) {
			s.reconcileStuckDeployingPRWidget(ctx, evt.Repo, evt.PRNumber)
			log.Printf("[synapse] [deploy] suppressing duplicate deploy_success for repo=%s job=%s pr=%d commit=%s",
				evt.Repo, evt.JobName, evt.PRNumber, evt.CommitSHA)
			return &evt, nil
		}
		s.recordDeploySuccessDispatched(evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber)
		if evt.PRNumber > 0 {
			s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deployed", "")
		}
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

func (s *RouterServer) reconcileStuckDeployingPRWidget(ctx context.Context, repo string, prNum int) {
	if s == nil || s.registry == nil || repo == "" || prNum <= 0 {
		return
	}
	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		return
	}
	normRepo := normalizeRepo(repo)
	qCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	var status string
	err := pgReg.pool.QueryRow(qCtx, `SELECT status FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, normRepo, prNum).Scan(&status)
	if err == nil && strings.EqualFold(strings.TrimSpace(status), "deploying") {
		s.setPRWidgetState(ctx, repo, prNum, "deployed", "")
		if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
			log.Printf("[synapse] [widget] warning syncing reconciled stuck deploying widget for %s#%d: %v", repo, prNum, err)
		}
	}
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
		s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "synced", "")
	} else {
		s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deployed", "")
		s.recordDeploySuccessDispatched(evt.Repo, evt.JobName, evt.CommitSHA, evt.PRNumber)
	}
	if err := s.advanceVanadisChunkOnSync(ctx, evt.Repo, evt.PRNumber, syncHasExpected); err != nil {
		return err
	}
	if syncHasExpected {
		if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err != nil {
			log.Printf("[synapse] [widget] warning syncing sync_success widget for %s#%d: %v", evt.Repo, evt.PRNumber, err)
		}
		return nil
	}
	vanadisPR, err := s.isVanadisPR(ctx, evt.Repo, evt.PRNumber)
	if err != nil {
		return err
	}
	if !vanadisPR {
		return s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, firstNonEmpty(evt.JobName, "git-sync"), evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID)
	}
	if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err != nil {
		log.Printf("[synapse] [widget] warning syncing sync_success widget for %s#%d: %v", evt.Repo, evt.PRNumber, err)
	}
	return nil
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
	if s.isDependabotPRNumber(ctx, repo, prNum) {
		log.Printf("[synapse] [dependabot] suppressing deploy_success /prompt wake for %s#%d (job=%s)", repo, prNum, jobName)
		return nil
	}

	vanadisPR, err := s.isVanadisPR(ctx, repo, prNum)
	if err != nil {
		return err
	}
	if vanadisPR {
		log.Printf("[synapse] [vanadis] suppressing deploy_success /prompt wake and advancing chunk for %s#%d (job=%s)", repo, prNum, jobName)
		return s.advanceVanadisChunk(ctx, repo, prNum)
	}

	if prNum > 0 {
		if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
			log.Printf("[synapse] [widget] warning syncing deploy_success widget: %v", err)
		}
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

func (s *RouterServer) enqueueMergeConflict(ctx context.Context, args MergeConflictArgs) error {
	if s == nil {
		return nil
	}
	args.Repo = normalizeRepo(args.Repo)
	if s.riverClient != nil {
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[synapse] [github] error enqueuing merge conflict into river: %v", err)
			return fmt.Errorf("enqueue merge conflict: %w", err)
		}
		return nil
	}
	return s.ProcessMergeConflict(ctx, args)
}

func (s *RouterServer) ProcessMergeConflict(ctx context.Context, args MergeConflictArgs) error {
	if s == nil {
		return nil
	}
	return s.dispatchConflictPrompt(ctx, args.TargetID, args.Repo, args.Branch, args.HeadSHA, args.PRNumber)
}

func (s *RouterServer) dispatchConflictPrompt(ctx context.Context, targetID, repo, branch, headSHA string, prNum int) error {
	if prNum > 0 && s.registry != nil {
		if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
			s.setPRWidgetState(ctx, repo, prNum, "conflict", "Merge conflict detected")
			if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
				log.Printf("[synapse] [widget] warning syncing conflict widget: %v", err)
			}
		}
	}
	shortRepo := firstNonEmpty(cleanRepoName(repo), "aerial")
	cleanBranch := firstNonEmpty(strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/"), "<branch>")
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", prNum, repo, branch, headSHA, shortRepo, cleanBranch)
	opts := s.vanadisPromptOpts(ctx, repo, prNum)
	opts.silent = true
	if opts.effort == "" {
		opts.effort = "medium"
	}
	return s.dispatchAlertWithPrompt(ctx, targetID, "conflict", "", prompt, opts)
}

func setupRiverWorkerPool(ctx context.Context, server *RouterServer, cfg Config) (*pgxpool.Pool, *river.Client[pgx.Tx], error) {
	if cfg.PostgresURL == "" {
		return nil, nil, nil
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse postgres url: %w", err)
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
		return nil, nil, fmt.Errorf("create pgxpool: %w", err)
	}
	server.SetQueueStats(pool)
	server.SetRegistry(NewPostgresPRRegistry(pool, WithVanadisClient(cfg.VanadisURL, server.httpClient), WithHaroClient(cfg.HaroURL, server.httpClient)))

	workers := river.NewWorkers()
	river.AddWorker(workers, &GitHubWebhookWorker{server: server})
	river.AddWorker(workers, &DeployWebhookWorker{server: server})
	river.AddWorker(workers, &WidgetSyncWorker{server: server})
	river.AddWorker(workers, &EventTriggerWebhookWorker{server: server})
	river.AddWorker(workers, &MergeConflictWorker{server: server})
	river.AddWorker(workers, &DiscordIngressWorker{server: server})
	river.AddWorker(workers, &DiscordInteractionWorker{server: server})

	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault:      {MaxWorkers: 10},
			QueueMergeConflict:      {MaxWorkers: 2},
			QueueWidgetSync:         {MaxWorkers: 1},
			QueueDiscordIngress:     {MaxWorkers: 1},
			QueueDiscordInteraction: {MaxWorkers: 4},
		},
		Workers:             workers,
		SkipUnknownJobCheck: true,
	})
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("create river client: %w", err)
	}
	server.SetRiverClient(rc)

	if err := rc.Start(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("start river client: %w", err)
	}
	log.Println("[synapse] River worker pool started successfully")
	return pool, rc, nil
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

	dbPool, riverClient, err := setupRiverWorkerPool(ctx, server, *cfg)
	if err != nil {
		return err
	}
	if dbPool != nil {
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

	if riverClient != nil {
		if stopErr := riverClient.Stop(shutdownCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			log.Printf("[synapse] River client stop error: %v", stopErr)
		}
	}

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

	err := runApp(ctx, cfgSrc)
	cancel()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[synapse] Error: %v", err)
	}
}
