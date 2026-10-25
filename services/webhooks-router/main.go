package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
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
	cfg                  Config
	httpClient           *http.Client
	riverClient          RiverInserter
	registry             PRRegistryUpdater
	dispatcher           OutboundDispatcher
	onProcessed          func(event, delivery string)
	conflictCheckDelay   time.Duration
	nonRoboRevFailedSHAs sync.Map
}

// NewRouterServer constructs a new RouterServer instance.
func NewRouterServer(cfg Config, client *http.Client, riverClient ...RiverInserter) *RouterServer {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	s := &RouterServer{
		cfg:                cfg,
		httpClient:         client,
		conflictCheckDelay: 2 * time.Second,
	}
	if len(riverClient) > 0 {
		s.riverClient = riverClient[0]
	}
	return s
}

func (s *RouterServer) SetRiverClient(rc RiverInserter)    { s.riverClient = rc }
func (s *RouterServer) SetRegistry(reg PRRegistryUpdater)  { s.registry = reg }
func (s *RouterServer) SetDispatcher(d OutboundDispatcher) { s.dispatcher = d }

func readWebhookBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes))
	if err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return nil, false
	}
	return body, true
}

//nolint:unparam
func readWebhookJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, allowEmpty bool, dst any) bool {
	body, ok := readWebhookBody(w, r, maxBytes)
	if !ok {
		return false
	}
	if len(body) == 0 && allowEmpty {
		return true
	}
	if err := json.Unmarshal(body, dst); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return false
	}
	return true
}

func runBackgroundSync(name string, fn func(ctx context.Context) error) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[webhooks-router] PANIC recovered in %s: %v", name, r)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			log.Printf("[webhooks-router] Background %s failed: %v", name, err)
		}
	}()
}

// Routes initializes and returns the HTTP mux for the service.
func (s *RouterServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /health", s.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /internal/pr/register", s.handlePRRegister)
	mux.HandleFunc("POST /webhooks/infisical", s.handleInfisicalWebhook)
	mux.HandleFunc("POST /webhooks/generic", s.handleGenericWebhook)
	mux.HandleFunc("POST /api/webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("POST /api/webhooks/hangar", s.handleHangarWebhook)
	mux.HandleFunc("POST /webhooks/hangar", s.handleHangarWebhook)
	coverageFlush := httpkit.HandleCoverageFlush("webhooks-router", true)
	mux.HandleFunc("POST /debug/coverage/flush", coverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", coverageFlush)
	return mux
}

func (s *RouterServer) handlePRRegister(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.registry == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "registry not initialized"})
		return
	}

	var req PRRegisterRequest
	if !readWebhookJSON(w, r, 1<<20, false, &req) {
		return
	}

	req.Repo = strings.TrimSpace(req.Repo)
	req.Branch = strings.TrimSpace(req.Branch)
	req.HeadSHA = strings.TrimSpace(req.HeadSHA)
	req.TargetID = strings.TrimSpace(req.TargetID)
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
		if req.TargetID == "" {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id cannot be empty"})
			return
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
		if errors.Is(err, ErrInvalidTargetID) || strings.Contains(err.Error(), "target_id") {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
		log.Printf("[webhooks-router] Failed to register PR #%d (%s): %v", req.PRNumber, req.Repo, err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to register PR"})
		return
	}

	if err := s.ProcessWidgetSync(r.Context(), WidgetSyncArgs{Repo: req.Repo, PRNumber: req.PRNumber}); err != nil {
		log.Printf("[webhooks-router] failed to sync initial PR widget for %s#%d: %v", req.Repo, req.PRNumber, err)
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "registered",
		"repo":      req.Repo,
		"pr_number": req.PRNumber,
		"target_id": targetID,
	})
}

func (s *RouterServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"service": "webhooks-router",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// Hangar Webhook Payloads and Resolution Types

type HangarDeployEvent struct {
	Event        string    `json:"event"`                   // "deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"
	JobName      string    `json:"job_name"`                // Nomad job name (e.g. "webhooks-router", "brain")
	ExpectedJobs []string  `json:"expected_jobs,omitempty"` // All Nomad jobs expected to deploy for this PR/sync
	Repo         string    `json:"repo,omitempty"`          // repository name (e.g. "azylman/aerial", "azylman/aerial-config")
	CommitSHA    string    `json:"commit_sha,omitempty"`    // git commit SHA (head or merge commit)
	PRNumber     int       `json:"pr_number,omitempty"`     // PR number if known
	TargetID     string    `json:"target_id,omitempty"`     // Discord thread / channel snowflake
	Image        string    `json:"image,omitempty"`         // container image (e.g. ghcr.io/azylman/aerial-webhooks-router:latest)
	Digest       string    `json:"digest,omitempty"`        // image digest (sha256:...)
	Status       string    `json:"status"`                  // "started", "success", "failed", "rollback"
	DeploymentID string    `json:"deployment_id,omitempty"` // Nomad deployment UUID
	Details      string    `json:"details,omitempty"`       // status description or error message
	Timestamp    time.Time `json:"timestamp"`
}

func (s *RouterServer) handleHangarWebhook(w http.ResponseWriter, r *http.Request) {
	var evt HangarDeployEvent
	if !readWebhookJSON(w, r, 1<<20, false, &evt) {
		return
	}
	if strings.TrimSpace(evt.Event) == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
		return
	}
	if strings.TrimSpace(evt.JobName) == "" {
		if strings.HasPrefix(evt.Event, "sync_") {
			evt.JobName = "git-sync"
		} else {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
			return
		}
	}

	if s.riverClient != nil {
		idempotencyKey := randomNonce()
		if evt.CommitSHA != "" && evt.JobName != "" {
			idempotencyKey = fmt.Sprintf("%s:%s:%s:%s", evt.Event, evt.Repo, evt.JobName, evt.CommitSHA)
		}
		if err := s.insertRiverJob(r.Context(), HangarWebhookArgs{IdempotencyKey: idempotencyKey, Event: evt}); err != nil {
			log.Printf("[webhooks-router] [hangar] ERROR: failed to insert job into river: %v", err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":   "accepted",
			"event":    evt.Event,
			"job_name": evt.JobName,
		})
		return
	}

	processed, err := s.ProcessHangarEvent(r.Context(), evt)
	if err != nil {
		log.Printf("[webhooks-router] [hangar] error processing hangar event: %v", err)
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "accepted",
		"event":     processed.Event,
		"job_name":  processed.JobName,
		"pr_number": processed.PRNumber,
		"target_id": processed.TargetID,
	})
}

// ResolveSHA resolves a commit SHA to its PR number, target Discord thread ID, and branch.
// It checks local pr_registry first, then falls back to GitHub commit PR lookup and backfills pr_registry.
func (s *RouterServer) ResolveSHA(ctx context.Context, repo, sha string) (int, string, string, error) {
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}
	repo = normalizeRepo(repo)

	if s.registry != nil {
		prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
		if err != nil {
			log.Printf("[webhooks-router] [registry] failed resolving PR by SHA %s locally: %v", sha, err)
		} else if prNum > 0 {
			if targetID == "" {
				if tID, _, _, _, errNum := s.registry.ResolvePRByNumber(ctx, repo, prNum); errNum == nil && tID != "" {
					targetID = tID
				}
			}
			return prNum, targetID, branch, nil
		}
	}

	prNum, branch, err := s.resolvePRFromCommitSHA(ctx, repo, sha)
	if err != nil || prNum == 0 {
		return 0, "", "", err
	}

	var targetID string
	if s.registry != nil {
		if tID, _, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum); err == nil && tID != "" {
			targetID = tID
		}
		if backfillTID, err := s.registry.BackfillPushMergeSHA(ctx, repo, prNum, sha); err != nil {
			log.Printf("[webhooks-router] [registry] failed backfilling merge_sha %s for PR %s#%d: %v", sha, repo, prNum, err)
		} else if targetID == "" && backfillTID != "" {
			targetID = backfillTID
		}
	}
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
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	query := `
		UPDATE pr_registry
		SET status = CASE WHEN $3 != '' AND status NOT IN ('deployed', 'closed') THEN $3 ELSE status END,
		    metadata = CASE
		        WHEN $4 != '' THEN jsonb_set(COALESCE(metadata, '{}'::jsonb), '{banner}', to_jsonb($4::text), true)
		        ELSE COALESCE(metadata, '{}'::jsonb) - 'banner'
		    END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2`
	if _, err := pgReg.pool.Exec(qCtx, query, normalizeRepo(repo), prNumber, strings.TrimSpace(status), strings.TrimSpace(banner)); err != nil {
		log.Printf("[webhooks-router] [widget] warning updating pr widget state for %s#%d: %v", repo, prNumber, err)
	}
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
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[webhooks-router] [widget] error enqueuing widget sync into river: %v", err)
			return fmt.Errorf("enqueue widget sync: %w", err)
		}
		return nil
	}
	return s.ProcessWidgetSync(ctx, args)
}

func (s *RouterServer) ProcessWidgetSync(ctx context.Context, args WidgetSyncArgs) error {
	repo := normalizeRepo(args.Repo)
	prNumber := args.PRNumber
	if s == nil || s.dispatcher == nil || s.registry == nil || repo == "" || prNumber <= 0 {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		targetID, branch, _, _, err := s.registry.ResolvePRByNumber(qCtx, repo, prNumber)
		if err != nil {
			log.Printf("[webhooks-router] [widget] resolve pr error: %v", err)
		}
		if targetID == "" {
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

func (s *RouterServer) sweepOlderDeployedPRWidgets(qCtx context.Context, pgReg *PostgresPRRegistry, repo string, prNumber int) {
	sweepQuery := `UPDATE pr_registry SET status = 'deployed', metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', '[]'::jsonb), updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number < $2 AND status IN ('merged', 'deploying', 'deploy_failed') RETURNING pr_number`
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
			log.Printf("[webhooks-router] [widget] warning syncing older deployed PR %s#%d: %v", repo, oldPR, wErr)
		}
	}
}

func (s *RouterServer) syncSinglePRWidget(qCtx context.Context, pgReg *PostgresPRRegistry, repo string, prNumber int, allowSweep bool) error {
	prQuery := `SELECT target_id, title, status, status_message_id, created_at, updated_at, COALESCE(metadata->>'banner', '') FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1`
	var targetID, title, prStatus, prMsgID, prBanner string
	var prCreatedAt, prUpdatedAt time.Time
	prErr := pgReg.pool.QueryRow(qCtx, prQuery, repo, prNumber).Scan(&targetID, &title, &prStatus, &prMsgID, &prCreatedAt, &prUpdatedAt, &prBanner)
	if prErr != nil && !errors.Is(prErr, pgx.ErrNoRows) {
		return fmt.Errorf("sync pr widget query pr: %w", prErr)
	}
	prFound := prErr == nil

	if allowSweep && prFound && strings.EqualFold(strings.TrimSpace(prStatus), "deployed") {
		s.sweepOlderDeployedPRWidgets(qCtx, pgReg, repo, prNumber)
	}

	// 1. Ask vanadis to sync widget if this PR belongs to a vanadis chunk
	client, vURL := pgReg.getVanadisClient()
	bodyBytes, err := json.Marshal(map[string]any{
		"repo":      repo,
		"pr_number": prNumber,
		"pr_status": prStatus,
		"pr_banner": prBanner,
		"pr_found":  prFound,
	})
	if err != nil {
		return fmt.Errorf("marshal sync chunk widget request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(qCtx, http.MethodPost, vURL+"/chunks/sync-pr-widget", bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create sync chunk widget request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("sync vanadis pr widget: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return fmt.Errorf("sync vanadis pr widget: unexpected status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("sync vanadis pr widget: unexpected status %d: %s", resp.StatusCode, string(body))
	}
	var syncRes struct {
		Matched bool   `json:"matched"`
		BuildID string `json:"build_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&syncRes); err != nil {
		return fmt.Errorf("decode sync chunk widget response: %w", err)
	}
	if syncRes.Matched {
		return nil
	}

	// 2. Standalone PR in pr_registry
	if !prFound || targetID == "" {
		return nil
	}

	finalStatus := firstNonEmpty(normalizeWidgetStatus(prStatus), "open")
	widgetBanner := ""
	if finalStatus == "failed" || finalStatus == "conflict" {
		widgetBanner = prBanner
	}
	widgetTitle := firstNonEmpty(title, fmt.Sprintf("PR #%d", prNumber))

	widgetReq := StatusWidgetRequest{
		ChannelID: targetID,
		MessageID: prMsgID,
		ID:        fmt.Sprintf("pr-%s-%d", repo, prNumber),
		Title:     widgetTitle,
		Status:    finalStatus,
		Banner:    widgetBanner,
		Chunks: []StatusWidgetChunk{
			{
				Index:     0,
				Repo:      repo,
				Title:     widgetTitle,
				Status:    finalStatus,
				PRNumber:  prNumber,
				StartedAt: prCreatedAt,
				UpdatedAt: prUpdatedAt,
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
			log.Printf("[webhooks-router] [widget] warning updating pr status_message_id: %v", execErr)
		}
	}
	return nil
}

type vanadisPromptOpts struct {
	silent  bool
	chunkID string
	effort  string
}

func (s *RouterServer) vanadisPromptOpts(ctx context.Context, repo string, prNum int) vanadisPromptOpts {
	if s == nil || s.registry == nil || prNum <= 0 {
		return vanadisPromptOpts{}
	}
	if pgReg, ok := s.registry.(*PostgresPRRegistry); ok {
		if matched, chunkID, effort, err := pgReg.ResolveVanadisChunkForPR(ctx, repo, prNum); err == nil && matched {
			return vanadisPromptOpts{silent: true, chunkID: chunkID, effort: effort}
		}
		return vanadisPromptOpts{}
	}
	suppressed, err := s.registry.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
	if err != nil {
		log.Printf("[webhooks-router] [vanadis] warning checking Vanadis status for %s#%d: %v", repo, prNum, err)
		return vanadisPromptOpts{}
	}
	if !suppressed {
		return vanadisPromptOpts{}
	}
	return vanadisPromptOpts{silent: true, effort: "medium"}
}

func (s *RouterServer) isVanadisPR(ctx context.Context, repo string, prNum int) bool {
	if s.registry == nil || prNum <= 0 {
		return false
	}
	suppressed, err := s.registry.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
	if err != nil {
		log.Printf("[webhooks-router] [vanadis] warning checking Vanadis deploy suppression for %s#%d: %v", repo, prNum, err)
		return false
	}
	return suppressed
}

func (s *RouterServer) dispatchAlertWithPrompt(ctx context.Context, targetID, label, dmMsg, prompt string, vOpts ...vanadisPromptOpts) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}
	var opt vanadisPromptOpts
	if len(vOpts) > 0 {
		opt = vOpts[0]
	}
	if !opt.silent && dmMsg != "" {
		if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{ChannelID: targetID, Content: dmMsg}); err != nil {
			log.Printf("[webhooks-router] [dispatcher] warning dispatching %s direct message: %v", label, err)
		}
	}
	if opt.chunkID != "" && !strings.Contains(prompt, "Chunk ID: "+opt.chunkID) {
		prompt += "\nChunk ID: " + opt.chunkID
	}
	req := PromptRequest{ChannelID: targetID, Prompt: prompt, Effort: opt.effort, Silent: opt.silent}
	if err := s.dispatcher.DispatchPrompt(ctx, req); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching %s prompt: %v", label, err)
		return fmt.Errorf("dispatch %s prompt: %w", label, err)
	}
	return nil
}

func (s *RouterServer) advanceVanadisChunk(ctx context.Context, repo string, prNumber int) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	res, err := s.registry.AdvanceVanadisChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if res == nil || !res.Matched || !res.Advanced {
		return nil
	}
	s.evaluateVanadisAdvancementTriggers(ctx, repo, prNumber, res)
	if !res.BuildCompleted && res.NextChunkIndex >= 0 {
		if err := s.SyncPRWidget(ctx, repo, prNumber); err != nil {
			log.Printf("[webhooks-router] [vanadis] warning syncing chunk advancement widget for build %s: %v", res.BuildID, err)
		}
		return nil
	}
	if res.BuildCompleted {
		if err := s.SyncPRWidget(ctx, repo, prNumber); err != nil {
			log.Printf("[webhooks-router] [vanadis] warning syncing build completed widget for build %s: %v", res.BuildID, err)
		}
		prompt := formatVanadisBuildCompletedPrompt(res)
		return s.dispatchAlertWithPrompt(ctx, res.ThreadID, "vanadis_build_completed", "", prompt)
	}
	return nil
}

func (s *RouterServer) ProcessHangarEvent(ctx context.Context, evt HangarDeployEvent) (*HangarDeployEvent, error) {
	if evt.CommitSHA != "" && evt.PRNumber == 0 {
		if prNum, targetID, _, err := s.ResolveSHA(ctx, evt.Repo, evt.CommitSHA); err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving SHA %s: %v", evt.CommitSHA, err)
		} else if prNum > 0 {
			evt.PRNumber = prNum
			evt.TargetID = firstNonEmpty(evt.TargetID, targetID)
		}
	} else if evt.PRNumber > 0 && evt.TargetID == "" {
		if targetID, err := s.ResolveThreadForPR(ctx, evt.Repo, evt.PRNumber); err != nil {
			log.Printf("[webhooks-router] [hangar] failed resolving thread for PR %s#%d: %v", evt.Repo, evt.PRNumber, err)
		} else if targetID != "" {
			evt.TargetID = targetID
		}
	}

	log.Printf("[webhooks-router] [hangar] [deploy] event=%s job=%s repo=%s pr=%d commit=%s status=%s target_id=%s deployment_id=%s details=%q",
		evt.Event, evt.JobName, evt.Repo, evt.PRNumber, evt.CommitSHA, evt.Status, evt.TargetID, evt.DeploymentID, evt.Details)

	s.evaluateHangarEventTriggers(ctx, evt)

	if s.registry != nil && evt.PRNumber > 0 && evt.Event == "deploy_started" && evt.JobName != "" {
		if len(evt.ExpectedJobs) > 0 {
			if _, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
				log.Printf("[webhooks-router] [registry] warning recording expected jobs on deploy_started for %s#%d: %v", evt.Repo, evt.PRNumber, err)
			}
		}
		if tID, err := s.registry.TransitionDeploying(ctx, evt.Repo, evt.PRNumber, []string{evt.JobName}); err != nil {
			log.Printf("[webhooks-router] [registry] warning transitioning pr %s#%d to deploying for job %s: %v", evt.Repo, evt.PRNumber, evt.JobName, err)
		} else if tID == "" {
			log.Printf("[webhooks-router] [hangar] skipping deploy_started for %s on %s#%d (job already deployed or PR inactive)", evt.JobName, evt.Repo, evt.PRNumber)
			return &evt, nil
		} else if s.isNomadBatchTemplateHealthy(ctx, evt.JobName) {
			if err := s.HandleNomadJobSuccess(ctx, evt.JobName, firstNonEmpty(evt.DeploymentID, "batch-registered")); err != nil {
				log.Printf("[webhooks-router] [hangar] warning handling batch job success for %s: %v", evt.JobName, err)
			}
			return &evt, nil
		}
	}

	var syncHasExpected bool
	if evt.Event == "sync_success" && evt.PRNumber > 0 {
		syncHasExpected = len(dedupeTrimmedStrings(evt.ExpectedJobs)) > 0
		if s.registry != nil {
			if regHasExpected, err := s.registry.RecordExpectedJobs(ctx, evt.Repo, evt.PRNumber, evt.ExpectedJobs); err != nil {
				log.Printf("[webhooks-router] [registry] warning recording expected jobs on sync_success for %s#%d: %v", evt.Repo, evt.PRNumber, err)
			} else {
				syncHasExpected = syncHasExpected || regHasExpected
			}
		}
	}

	if !IsValidDiscordSnowflake(evt.TargetID) || s.dispatcher == nil {
		if evt.Event == "sync_success" && evt.PRNumber > 0 {
			return &evt, s.advanceVanadisChunk(ctx, evt.Repo, evt.PRNumber)
		}
		return &evt, nil
	}

	prefix := formatDirectMessagePrefix(evt.PRNumber, evt.Repo)
	switch evt.Event {
	case "deploy_started":
		if evt.PRNumber > 0 {
			if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err != nil {
				log.Printf("[webhooks-router] [widget] warning syncing deploy_started widget: %v", err)
				return &evt, err
			}
		}
		return &evt, nil
	case "deploy_success":
		if evt.PRNumber > 0 {
			s.setPRWidgetState(ctx, evt.Repo, evt.PRNumber, "deployed", "")
		}
		return &evt, s.dispatchDeploymentSuccessPrompt(ctx, evt.TargetID, evt.JobName, evt.Repo, evt.CommitSHA, evt.PRNumber, evt.DeploymentID)
	case "deploy_rollback", "deploy_failed":
		targetID := evt.TargetID
		if s.registry != nil && evt.PRNumber > 0 {
			tID, updated, err := s.registry.AtomicTransitionDeployFailed(ctx, evt.Repo, evt.PRNumber)
			if err != nil || !updated {
				return &evt, err
			}
			targetID = firstNonEmpty(tID, targetID)
		}
		actionDesc, fixDesc := "Deployment failed", "deployment failure"
		if evt.Event == "deploy_rollback" {
			actionDesc, fixDesc = "Deployment failed and rolled back", "deployment rollback failure"
		}
		dmMsg := fmt.Sprintf("💥 %s%s for %s. Following up...", prefix, actionDesc, evt.JobName)
		banner := fmt.Sprintf("Deployment failed for %s", evt.JobName)
		if evt.Event == "deploy_rollback" {
			banner = fmt.Sprintf("Deployment failed and rolled back for %s", evt.JobName)
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
		return &evt, s.dispatchAlertWithPrompt(ctx, targetID, evt.Event, dmMsg, prompt, s.vanadisPromptOpts(ctx, evt.Repo, evt.PRNumber))
	case "sync_success":
		if evt.PRNumber > 0 {
			if !syncHasExpected {
				if err := s.SyncPRWidget(ctx, evt.Repo, evt.PRNumber); err != nil {
					log.Printf("[webhooks-router] [widget] warning syncing sync_success widget: %v", err)
				}
			}
			if err := s.advanceVanadisChunk(ctx, evt.Repo, evt.PRNumber); err != nil {
				return &evt, err
			}
		}
		return &evt, nil
	case "sync_failed":
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
		return &evt, s.dispatchAlertWithPrompt(ctx, evt.TargetID, "sync_failed", dmMsg, prompt, s.vanadisPromptOpts(ctx, evt.Repo, evt.PRNumber))
	}
	return &evt, nil
}

func (s *RouterServer) dispatchDeploymentSuccessPrompt(ctx context.Context, targetID, jobName, repo, commitSHA string, prNum int, refID string) error {
	if s.dispatcher == nil || !IsValidDiscordSnowflake(targetID) {
		return nil
	}

	if prNum > 0 {
		if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
			log.Printf("[webhooks-router] [widget] warning syncing deploy_success widget: %v", err)
		}
	}

	if s.isVanadisPR(ctx, repo, prNum) {
		log.Printf("[webhooks-router] [vanadis] suppressing mid-run deploy_success /prompt wake for %s#%d (job=%s)", repo, prNum, jobName)
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

func (s *RouterServer) dispatchConflictPrompt(ctx context.Context, targetID, repo, branch, headSHA string, prNum int) error {
	dmMsg := fmt.Sprintf("⛔ %sMerge conflict detected. Following up...", formatDirectMessagePrefix(prNum, repo))
	if prNum > 0 {
		if s.registry != nil {
			if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
				s.setPRWidgetState(ctx, repo, prNum, "conflict", "Merge conflict detected")
				if err := s.SyncPRWidget(ctx, repo, prNum); err == nil {
					dmMsg = ""
				}
			}
		}
	}
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please checkout the branch, merge origin/main to resolve conflicts, verify locally, and push to update the PR.", prNum, repo, branch, headSHA)
	return s.dispatchAlertWithPrompt(ctx, targetID, "conflict", dmMsg, prompt, s.vanadisPromptOpts(ctx, repo, prNum))
}

// GitHubIssueComment represents a comment on a GitHub issue or pull request.
type GitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

// GitHubPRDetails encapsulates mergeability information returned by GitHub's Pull Request API.
type GitHubPRDetails struct {
	Number         int    `json:"number"`
	Mergeable      *bool  `json:"mergeable"`
	MergeableState string `json:"mergeable_state"`
}

func (s *RouterServer) getGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	apiURL := firstNonEmpty(s.cfg.GitHubAPIURL, "https://api.github.com")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		return fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github api returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return fmt.Errorf("read github api response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal github api response: %w", err)
	}
	return nil
}

func (s *RouterServer) fetchPRDetails(ctx context.Context, repo string, prNumber int) (*GitHubPRDetails, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return nil, fmt.Errorf("invalid repo or prNumber for pr details")
	}
	var details GitHubPRDetails
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls/%d", repo, prNumber), 1<<20, &details); err != nil {
		return nil, err
	}
	return &details, nil
}

func (s *RouterServer) checkOpenPRConflicts(ctx context.Context, repo string) error {
	if s.registry == nil {
		return nil
	}
	openPRs, err := s.registry.ListOpenPRs(ctx, repo)
	if err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var details *GitHubPRDetails
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				delay := s.conflictCheckDelay
				if delay <= 0 {
					delay = 200 * time.Millisecond
				}
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			det, fetchErr := s.fetchPRDetails(ctx, repo, pr.PRNumber)
			if fetchErr != nil {
				log.Printf("[webhooks-router] [github] error fetching PR #%d details for conflict check: %v", pr.PRNumber, fetchErr)
				if strings.Contains(fetchErr.Error(), "403") || strings.Contains(strings.ToLower(fetchErr.Error()), "rate limit") {
					return fetchErr
				}
				break
			}
			details = det
			if details.Mergeable != nil || (details.MergeableState != "" && details.MergeableState != "unknown") {
				break
			}
		}

		if details == nil {
			continue
		}
		if (details.Mergeable != nil && !*details.Mergeable) || details.MergeableState == "dirty" {
			if pr.HeadSHA != "" {
				s.nonRoboRevFailedSHAs.Store(prCheckKey(repo, pr.PRNumber), strings.TrimSpace(pr.HeadSHA))
			}
			targetID, updated, trErr := s.registry.AtomicTransitionConflict(ctx, repo, pr.PRNumber)
			if trErr != nil {
				log.Printf("[webhooks-router] [registry] error transitioning PR #%d to conflict: %v", pr.PRNumber, trErr)
				continue
			}
			if updated {
				targetID = firstNonEmpty(targetID, pr.TargetID)
				if err := s.dispatchConflictPrompt(ctx, targetID, repo, pr.Branch, pr.HeadSHA, pr.PRNumber); err != nil {
					log.Printf("[webhooks-router] [registry] error dispatching conflict prompt for PR #%d: %v", pr.PRNumber, err)
				}
			}
		}
	}
	return nil
}

func (s *RouterServer) dispatchGitPush(ctx context.Context, repo, ref, commit, targetID string, prNum int) error {
	if s.dispatcher == nil || !strings.HasPrefix(ref, "refs/heads/main") || isZeroSHA(commit) {
		return nil
	}
	if err := s.dispatcher.DispatchGitPush(ctx, GitPushEventRequest{
		Repo:     repo,
		Ref:      ref,
		Commit:   commit,
		TargetID: targetID,
		PRNumber: prNum,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching git_push to hangar for %s: %v", repo, err)
		return fmt.Errorf("dispatch git_push to hangar: %w", err)
	}
	return nil
}

func (s *RouterServer) dispatchWorkflowRunImages(ctx context.Context, repo string, runID int64, headBranch, conclusion, headSHA, targetID string, prNum int) error {
	if s.dispatcher == nil || conclusion != "success" || !(headBranch == "main" || strings.HasPrefix(headBranch, "refs/heads/main")) {
		return nil
	}
	images, err := s.resolveWorkflowRunImages(ctx, repo, runID)
	if err != nil {
		log.Printf("[webhooks-router] [dispatcher] error resolving images for workflow run %d (%s): %v", runID, repo, err)
		return fmt.Errorf("resolve workflow run images: %w", err)
	}
	if len(images) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var rawJobs []string
	errCh := make(chan error, len(images))

	for _, img := range images {
		wg.Add(1)
		go func(imageRef string) {
			defer wg.Done()
			matchedJobs, err := s.dispatcher.DispatchImageReady(ctx, ImageReadyEventRequest{
				Image:     imageRef,
				Repo:      repo,
				CommitSHA: headSHA,
				PRNumber:  prNum,
				TargetID:  targetID,
			})
			if err != nil {
				log.Printf("[webhooks-router] [dispatcher] error dispatching image_ready to hangar for %s: %v", imageRef, err)
				errCh <- err
				return
			}
			if len(matchedJobs) > 0 {
				mu.Lock()
				rawJobs = append(rawJobs, matchedJobs...)
				mu.Unlock()
			}
		}(img)
	}
	wg.Wait()
	close(errCh)

	if allMatchedJobs := dedupeTrimmedStrings(rawJobs); len(allMatchedJobs) > 0 && s.registry != nil && prNum > 0 {
		if _, err := s.registry.TransitionDeploying(ctx, repo, prNum, allMatchedJobs); err != nil {
			log.Printf("[webhooks-router] [dispatcher] error transitioning pr %s#%d to deploying: %v", repo, prNum, err)
			return fmt.Errorf("transition pr deploying: %w", err)
		}
	}

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// GitHub Webhook Payloads and Resolution Types

type GitHubPingPayload struct {
	Zen    string `json:"zen"`
	HookID int64  `json:"hook_id"`
	Repo   *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubPRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type GitHubPullRequest struct {
	Number         int         `json:"number"`
	Title          string      `json:"title"`
	Body           string      `json:"body"`
	Head           GitHubPRRef `json:"head"`
	Base           GitHubPRRef `json:"base"`
	Merged         bool        `json:"merged"`
	MergeCommitSHA string      `json:"merge_commit_sha"`
}

const aerialPRMetadataPrefix = "<!-- aerial-pr:"

func ParseAerialPRMetadata(body string) (string, string, bool) {
	idx := strings.LastIndex(body, aerialPRMetadataPrefix)
	if idx < 0 {
		return "", "", false
	}
	rest := body[idx+len(aerialPRMetadataPrefix):]
	endIdx := strings.Index(rest, "-->")
	if endIdx < 0 {
		return "", "", false
	}
	var trailer struct {
		TargetID       string `json:"target_id"`
		VanadisChunkID string `json:"vanadis_chunk_id"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(rest[:endIdx])), &trailer); err != nil {
		return "", "", false
	}
	targetID, chunkID := strings.TrimSpace(trailer.TargetID), strings.TrimSpace(trailer.VanadisChunkID)
	if !IsValidDiscordSnowflake(targetID) {
		targetID = ""
	}
	return targetID, chunkID, targetID != "" || chunkID != ""
}

type GitHubPullRequestPayload struct {
	Action      string            `json:"action"`
	Number      int               `json:"number"`
	PullRequest GitHubPullRequest `json:"pull_request"`
	Repository  struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
}

type GitHubCheckRunPR struct {
	Number int         `json:"number"`
	Head   GitHubPRRef `json:"head"`
	Base   GitHubPRRef `json:"base"`
}

type GitHubCheckRunPayload struct {
	Action   string `json:"action"`
	CheckRun struct {
		ID         int64  `json:"id"`
		Name       string `json:"name"`
		HeadSHA    string `json:"head_sha"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		DetailsURL string `json:"details_url"`
		HTMLURL    string `json:"html_url"`
		Output     struct {
			Title   string `json:"title"`
			Summary string `json:"summary"`
			Text    string `json:"text"`
		} `json:"output"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"check_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubWorkflowRunPayload struct {
	Action      string `json:"action"`
	WorkflowRun struct {
		ID           int64              `json:"id"`
		Name         string             `json:"name"`
		HeadSHA      string             `json:"head_sha"`
		HeadBranch   string             `json:"head_branch"`
		Status       string             `json:"status"`
		Conclusion   string             `json:"conclusion"`
		HTMLURL      string             `json:"html_url"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"workflow_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubWorkflowJobStep struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Number     int    `json:"number"`
}

type GitHubWorkflowJobItem struct {
	ID         int64                   `json:"id"`
	Name       string                  `json:"name"`
	Status     string                  `json:"status"`
	Conclusion string                  `json:"conclusion"`
	Steps      []GitHubWorkflowJobStep `json:"steps,omitempty"`
}

// hasExecutedBuildStep verifies that if steps are present and a build/push step is defined,
// that step actually concluded with success rather than being skipped.
func hasExecutedBuildStep(steps []GitHubWorkflowJobStep) bool {
	hasPushStep := false
	for _, step := range steps {
		nameLower := strings.ToLower(step.Name)
		isPush := (strings.Contains(nameLower, "build") && (strings.Contains(nameLower, "push") || strings.Contains(nameLower, "publish"))) ||
			strings.Contains(nameLower, "docker push") || strings.Contains(nameLower, "push image") || strings.Contains(nameLower, "push docker")
		if isPush {
			hasPushStep = true
			if step.Conclusion == "success" {
				return true
			}
		}
	}
	return !hasPushStep
}

type GitHubWorkflowJobsResponse struct {
	TotalCount int                     `json:"total_count"`
	Jobs       []GitHubWorkflowJobItem `json:"jobs"`
}

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

type GitHubCommit struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

type GitHubPushPayload struct {
	Ref        string        `json:"ref"`
	Before     string        `json:"before"`
	After      string        `json:"after"`
	Deleted    bool          `json:"deleted"`
	HeadCommit *GitHubCommit `json:"head_commit"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type ResolvedGitHubEvent struct {
	Event            string `json:"event"`
	Delivery         string `json:"delivery"`
	Action           string `json:"action,omitempty"`
	Repo             string `json:"repo,omitempty"`
	PRNumber         int    `json:"pr_number,omitempty"`
	Branch           string `json:"branch,omitempty"`
	HeadSHA          string `json:"head_sha,omitempty"`
	MergeSHA         string `json:"merge_sha,omitempty"`
	Merged           bool   `json:"merged,omitempty"`
	TargetID         string `json:"target_id,omitempty"`
	CheckName        string `json:"check_name,omitempty"`
	Status           string `json:"status,omitempty"`
	Conclusion       string `json:"conclusion,omitempty"`
	Sender           string `json:"sender,omitempty"`
	ResolutionSource string `json:"resolution_source,omitempty"`
}

type GitHubCommitPRItem struct {
	Number int    `json:"number"`
	State  string `json:"state"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

func isZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
}

var (
	squashCommitPRRegex = regexp.MustCompile(`\(#([1-9][0-9]*)\)\s*$`)
	mergeCommitPRRegex  = regexp.MustCompile(`^Merge pull request #([1-9][0-9]*)\b`)
)

func extractPRNumberFromCommitMessage(msg string) int {
	firstLine := strings.TrimSpace(strings.SplitN(strings.TrimSpace(msg), "\n", 2)[0])
	for _, re := range []*regexp.Regexp{squashCommitPRRegex, mergeCommitPRRegex} {
		if m := re.FindStringSubmatch(firstLine); len(m) == 2 {
			prNum := 0
			for _, ch := range m[1] {
				prNum = prNum*10 + int(ch-'0')
			}
			return prNum
		}
	}
	return 0
}

func (s *RouterServer) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	event := r.Header.Get("X-GitHub-Event")
	if strings.TrimSpace(event) == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-GitHub-Event header"})
		return
	}
	delivery := r.Header.Get("X-GitHub-Delivery")

	body, ok := readWebhookBody(w, r, 5<<20)
	if !ok {
		return
	}
	if s.riverClient == nil {
		log.Printf("[webhooks-router] [github] ERROR: river client not initialized")
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "river queue not initialized"})
		return
	}
	if err := s.insertRiverJob(r.Context(), GitHubWebhookArgs{Event: event, Delivery: delivery, Body: body}); err != nil {
		log.Printf("[webhooks-router] [github] ERROR: failed to insert job into river: %v", err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
		return
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "accepted",
		"event":    event,
		"delivery": delivery,
	})
}

func (s *RouterServer) resolveEventPR(ctx context.Context, res *ResolvedGitHubEvent, prs []GitHubCheckRunPR, payloadSource string) {
	if len(prs) > 0 {
		res.PRNumber = prs[0].Number
		if res.Branch == "" {
			res.Branch = prs[0].Head.Ref
		}
		res.ResolutionSource = payloadSource
		if s.registry != nil && res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if _, _, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err == nil && targetID != "" {
				res.TargetID = targetID
			}
		}
		return
	}
	if res.HeadSHA == "" || isZeroSHA(res.HeadSHA) {
		return
	}
	if s.registry != nil {
		if prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err != nil {
			log.Printf("[webhooks-router] [registry] %s failed resolving PR by SHA %s locally: %v", res.Event, res.HeadSHA, err)
		} else if prNum > 0 {
			res.PRNumber = prNum
			if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
				res.Branch = firstNonEmpty(branch, res.Branch)
			}
			res.TargetID = targetID
			res.ResolutionSource = "local_pr_registry"
			return
		}
	}
	if prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA); err != nil {
		log.Printf("[webhooks-router] [github] %s failed resolving PR for commit %s: %v", res.Event, res.HeadSHA, err)
	} else {
		res.PRNumber = prNum
		if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
			res.Branch = firstNonEmpty(branch, res.Branch)
		}
		res.ResolutionSource = "commit_sha_lookup"
	}
}

func isRoboRevCheck(checkName string) bool {
	trimmed := strings.TrimSpace(checkName)
	return strings.EqualFold(trimmed, "roborev") || strings.EqualFold(trimmed, "RoboRev Code Review")
}

func prCheckKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s#%d", normalizeRepo(repo), prNumber)
}

func (s *RouterServer) canRecoverRoboRevSuccess(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	if blockedSHA, ok := s.nonRoboRevFailedSHAs.Load(prCheckKey(repo, prNumber)); ok && blockedSHA == headSHA {
		return false
	}
	comment := s.fetchLatestRoboRevComment(ctx, repo, prNumber, headSHA)
	return comment != "" && !strings.Contains(comment, "## roborev: Review Passed")
}

func (s *RouterServer) handleCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if s.registry == nil || (res.Conclusion != "failure" && res.Conclusion != "timed_out") {
		return nil
	}
	if res.PRNumber > 0 && res.HeadSHA != "" && !isRoboRevCheck(res.CheckName) {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
	}
	if res.PRNumber > 0 {
		targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		if updated {
			return s.completeCIFailureTransition(ctx, res, details, checkURL)
		}
		return nil
	}
	if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
		targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		if prNum > 0 && res.PRNumber == 0 {
			res.PRNumber = prNum
			if !isRoboRevCheck(res.CheckName) {
				s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
			}
		}
		if updated {
			return s.completeCIFailureTransition(ctx, res, details, checkURL)
		}
	}
	return nil
}

func (s *RouterServer) ProcessGitHubEvent(ctx context.Context, event, delivery string, body []byte) (res *ResolvedGitHubEvent, err error) {
	if s.onProcessed != nil {
		defer s.onProcessed(event, delivery)
	}
	res = &ResolvedGitHubEvent{Event: event, Delivery: delivery}
	// Triggers are only evaluated after successful processing to prevent destructive trigger claiming during transient errors/River retries.
	defer func() {
		if err == nil {
			s.evaluateGitHubEventTriggers(ctx, res, body)
		}
	}()

	switch event {
	case "ping":
		var p GitHubPingPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal ping payload: %w", err)
		}
		if p.Repo != nil {
			res.Repo = p.Repo.FullName
		}
		log.Printf("[webhooks-router] [github] [ping] hook_id=%d repo=%s zen=%q delivery=%s", p.HookID, res.Repo, p.Zen, delivery)
		return res, nil

	case "pull_request":
		var p GitHubPullRequestPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal pull_request payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.PRNumber = p.Number
		if res.PRNumber == 0 {
			res.PRNumber = p.PullRequest.Number
		}
		res.Branch = p.PullRequest.Head.Ref
		res.HeadSHA = p.PullRequest.Head.SHA
		res.Merged = p.PullRequest.Merged
		// GitHub pull_request webhooks set merge_commit_sha for test merges even on unmerged PRs; only set MergeSHA when merged.
		if p.PullRequest.Merged {
			res.MergeSHA = p.PullRequest.MergeCommitSHA
		}
		res.Sender = p.Sender.Login
		res.ResolutionSource = "pull_request_payload"
		log.Printf("[webhooks-router] [github] [pull_request] repo=%s pr=%d action=%s head_sha=%s merge_sha=%s branch=%s merged=%v sender=%s delivery=%s",
			res.Repo, res.PRNumber, res.Action, res.HeadSHA, res.MergeSHA, res.Branch, p.PullRequest.Merged, res.Sender, delivery)

		if s.registry != nil && res.PRNumber > 0 {
			if trailerTargetID, trailerChunkID, ok := ParseAerialPRMetadata(p.PullRequest.Body); ok {
				// Self-heal DB registration only; initial Discord widget delivery on opened is owned by Brain's /internal/pr/register to avoid duplicate message creation races.
				resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, p.PullRequest.Title, trailerTargetID, trailerChunkID)
				if err != nil {
					return nil, err
				}
				res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, trailerTargetID)
			}
			switch res.Action {
			case "closed":
				if p.PullRequest.Merged {
					targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
					if err != nil {
						return nil, err
					}
					res.TargetID = firstNonEmpty(res.TargetID, targetID)
					hasExpected, err := s.registry.HasExpectedDeployJobs(ctx, res.Repo, res.PRNumber)
					if err != nil {
						log.Printf("[webhooks-router] [registry] warning checking expected deploy jobs for %s#%d: %v", res.Repo, res.PRNumber, err)
					}
					if !hasExpected {
						if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber); err != nil {
							log.Printf("[webhooks-router] [widget] warning syncing pr_merged widget for %s#%d: %v", res.Repo, res.PRNumber, err)
						}
					}
					if !isHangarSyncedRepo(res.Repo) {
						if err := s.advanceVanadisChunk(ctx, res.Repo, res.PRNumber); err != nil {
							return nil, err
						}
					}
				} else {
					targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
					if err != nil {
						return nil, err
					}
					res.TargetID = firstNonEmpty(res.TargetID, targetID)
					if err := s.failVanadisChunk(ctx, res.Repo, res.PRNumber, "pr_closed_unmerged"); err != nil {
						log.Printf("[webhooks-router] [vanadis] warning failing vanadis chunk for closed unmerged PR %s#%d: %v", res.Repo, res.PRNumber, err)
					}
					if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber); err != nil {
						log.Printf("[webhooks-router] [widget] warning syncing pr_closed widget for %s#%d: %v", res.Repo, res.PRNumber, err)
					}
				}
			case "synchronize":
				s.nonRoboRevFailedSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
				targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
				if err != nil {
					return nil, err
				}
				res.TargetID = firstNonEmpty(res.TargetID, targetID)
				if res.TargetID != "" {
					if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber); err != nil {
						log.Printf("[webhooks-router] [widget] warning syncing pr_sync widget for %s#%d: %v", res.Repo, res.PRNumber, err)
					}
				}
			case "auto_merge_disabled":
				if res.HeadSHA != "" {
					s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
				}
				targetID, updated, err := s.registry.AtomicTransitionConflict(ctx, res.Repo, res.PRNumber)
				if err != nil {
					return nil, err
				}
				res.TargetID = firstNonEmpty(res.TargetID, targetID)
				if updated {
					if err := s.dispatchConflictPrompt(ctx, res.TargetID, res.Repo, res.Branch, res.HeadSHA, res.PRNumber); err != nil {
						return nil, err
					}
				}
			}
		}
		return res, nil

	case "check_run":
		var p GitHubCheckRunPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal check_run payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.CheckName = p.CheckRun.Name
		res.HeadSHA = p.CheckRun.HeadSHA
		res.Status = p.CheckRun.Status
		res.Conclusion = p.CheckRun.Conclusion
		s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "check_run_payload")
		log.Printf("[webhooks-router] [github] [check_run] repo=%s check=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		if p.Action == "completed" && res.Conclusion == "success" && res.PRNumber > 0 && strings.EqualFold(strings.TrimSpace(res.CheckName), "roborev") && s.registry != nil && s.canRecoverRoboRevSuccess(ctx, res.Repo, res.PRNumber, res.HeadSHA) {
			targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
			if err != nil {
				return nil, err
			}
			res.TargetID = firstNonEmpty(res.TargetID, targetID)
			if targetID != "" {
				if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber); err != nil {
					log.Printf("[webhooks-router] [widget] warning syncing pr_open widget for %s#%d: %v", res.Repo, res.PRNumber, err)
				}
			}
		}

		details := firstNonEmpty(strings.TrimSpace(p.CheckRun.Output.Text), strings.TrimSpace(p.CheckRun.Output.Summary), strings.TrimSpace(p.CheckRun.Output.Title))
		checkURL := firstNonEmpty(strings.TrimSpace(p.CheckRun.HTMLURL), strings.TrimSpace(p.CheckRun.DetailsURL))
		if err := s.handleCIFailureTransition(ctx, res, details, checkURL); err != nil {
			return nil, err
		}
		return res, nil

	case "workflow_run":
		var p GitHubWorkflowRunPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal workflow_run payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Action = p.Action
		res.CheckName = p.WorkflowRun.Name
		res.HeadSHA = p.WorkflowRun.HeadSHA
		res.Branch = p.WorkflowRun.HeadBranch
		res.Status = p.WorkflowRun.Status
		res.Conclusion = p.WorkflowRun.Conclusion
		s.resolveEventPR(ctx, res, p.WorkflowRun.PullRequests, "workflow_run_payload")
		log.Printf("[webhooks-router] [github] [workflow_run] repo=%s workflow=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
			res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

		details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
		if err := s.handleCIFailureTransition(ctx, res, details, strings.TrimSpace(p.WorkflowRun.HTMLURL)); err != nil {
			return nil, err
		}
		if p.Action == "completed" && p.WorkflowRun.Status == "completed" {
			if err := s.dispatchWorkflowRunImages(ctx, res.Repo, p.WorkflowRun.ID, p.WorkflowRun.HeadBranch, p.WorkflowRun.Conclusion, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
				return nil, err
			}
		}
		return res, nil

	case "push":
		var p GitHubPushPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("unmarshal push payload: %w", err)
		}
		res.Repo = p.Repository.FullName
		res.Branch = p.Ref

		if p.Deleted || isZeroSHA(p.After) {
			res.ResolutionSource = "branch_deleted"
			return res, nil
		}
		if !strings.HasPrefix(p.Ref, "refs/heads/") {
			res.ResolutionSource = "non_branch_ref"
			return res, nil
		}
		res.HeadSHA = p.After
		if p.HeadCommit != nil && p.HeadCommit.ID != "" {
			res.HeadSHA = p.HeadCommit.ID
		}
		if !strings.HasPrefix(p.Ref, "refs/heads/main") {
			res.ResolutionSource = "non_main_branch"
			return res, nil
		}

		s.resolveEventPR(ctx, res, nil, "")
		if res.PRNumber == 0 && p.HeadCommit != nil {
			if prNum := extractPRNumberFromCommitMessage(p.HeadCommit.Message); prNum > 0 {
				res.PRNumber = prNum
				res.ResolutionSource = "commit_message"
			}
		}
		if res.PRNumber > 0 {
			res.MergeSHA = res.HeadSHA
		}
		log.Printf("[webhooks-router] [github] [push] repo=%s ref=%s head_sha=%s resolved_pr=%d branch=%s source=%s target_id=%s delivery=%s",
			res.Repo, p.Ref, res.HeadSHA, res.PRNumber, res.Branch, res.ResolutionSource, res.TargetID, delivery)

		if s.registry != nil && res.PRNumber > 0 && res.MergeSHA != "" {
			targetID, err := s.registry.BackfillPushMergeSHA(ctx, res.Repo, res.PRNumber, res.MergeSHA)
			if err != nil {
				return nil, err
			}
			res.TargetID = firstNonEmpty(res.TargetID, targetID)
		}

		if err := s.dispatchGitPush(ctx, res.Repo, p.Ref, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
			return nil, err
		}
		if res.PRNumber > 0 && !isHangarSyncedRepo(res.Repo) {
			if err := s.advanceVanadisChunk(ctx, res.Repo, res.PRNumber); err != nil {
				return nil, err
			}
		}
		if s.conflictCheckDelay > 0 {
			select {
			case <-time.After(s.conflictCheckDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
			log.Printf("[webhooks-router] [github] error checking open pr conflicts for %s: %v", res.Repo, err)
		}
		return res, nil

	default:
		log.Printf("[webhooks-router] [github] [unhandled] event=%s delivery=%s payload_size=%d bytes", event, delivery, len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}
	var prs []GitHubCommitPRItem
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha)), 1<<20, &prs); err != nil {
		return 0, "", err
	}
	if len(prs) == 0 {
		return 0, "", fmt.Errorf("no pull requests associated with commit %s", sha)
	}
	selected := prs[0]
	for _, item := range prs[1:] {
		if (selected.State != "closed" && item.State == "closed") || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	return selected.Number, selected.Head.Ref, nil
}

func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || runID <= 0 {
		return nil, fmt.Errorf("invalid repo or runID for workflow run images resolution")
	}
	var jobsResp GitHubWorkflowJobsResponse
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=100", repo, runID), 5<<20, &jobsResp); err != nil {
		return nil, err
	}

	owner := "azylman"
	if parts := strings.Split(repo, "/"); len(parts) == 2 {
		owner = strings.ToLower(parts[0])
	}

	cleanRepo := strings.ToLower(repo)
	var rawImages []string
	for _, job := range jobsResp.Jobs {
		if job.Status != "completed" || job.Conclusion != "success" || !hasExecutedBuildStep(job.Steps) {
			continue
		}
		m := buildJobRegex.FindStringSubmatch(job.Name)
		if len(m) < 2 {
			continue
		}
		service := strings.TrimSpace(m[1])
		switch {
		case (strings.HasSuffix(cleanRepo, "aerial-sidecars") || strings.Contains(cleanRepo, "sidecar")) && !strings.HasPrefix(service, "orin-"):
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/aerial-sidecar-%s:latest", owner, service))
		case strings.HasSuffix(cleanRepo, "aerial-sidecars") || strings.Contains(cleanRepo, "sidecar") || strings.HasSuffix(cleanRepo, "mirrormere") || strings.Contains(cleanRepo, "mirrormere"):
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service))
		default:
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/aerial-%s:latest", owner, service))
		}
	}
	return dedupeTrimmedStrings(rawImages), nil
}

// runServer runs the HTTP server with graceful shutdown and optional ready callback.
func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewRouterServer(cfg, nil)
	server.SetDispatcher(NewDefaultOutboundDispatcher(cfg.HangarURL, cfg.BrainURL, nil))

	var riverClient *river.Client[pgx.Tx]
	var dbPool *pgxpool.Pool

	if cfg.PostgresURL != "" {
		poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
		if err != nil {
			return fmt.Errorf("parse postgres url: %w", err)
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
			return fmt.Errorf("create pgxpool: %w", err)
		}
		defer pool.Close()
		dbPool = pool
		server.SetRegistry(NewPostgresPRRegistry(dbPool, WithVanadisClient(cfg.VanadisURL, server.httpClient), WithHaroClient(cfg.HaroURL, server.httpClient)))

		workers := river.NewWorkers()
		river.AddWorker(workers, &GitHubWebhookWorker{server: server})
		river.AddWorker(workers, &HangarWebhookWorker{server: server})
		river.AddWorker(workers, &NomadEventWorker{server: server})
		river.AddWorker(workers, &NomadCrashAlertWorker{server: server})
		river.AddWorker(workers, &WidgetSyncWorker{server: server})

		rc, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {MaxWorkers: 10},
			},
			Workers:             workers,
			SkipUnknownJobCheck: true,
		})
		if err != nil {
			return fmt.Errorf("create river client: %w", err)
		}
		riverClient = rc
		server.SetRiverClient(riverClient)

		if err := riverClient.Start(ctx); err != nil {
			return fmt.Errorf("start river client: %w", err)
		}
		log.Println("[webhooks-router] River worker pool started successfully")
	}

	// Initialize and launch Nomad Event Stream subscriber
	nomadSub := NewNomadStreamSubscriber(server)
	nomadSub.Start(ctx)

	// Reconcile any active deployments from pr_registry (startup sweep)
	go func() {
		sweepCtx, sweepCancel := context.WithTimeout(ctx, 15*time.Second)
		defer sweepCancel()
		if sweepErr := server.ReconcileActiveDeployments(sweepCtx); sweepErr != nil {
			log.Printf("[webhooks-router] [sweep] startup active deployment reconciliation error: %v", sweepErr)
		}
	}()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
	if err != nil {
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}

	httpServer := &http.Server{
		Handler:      server.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if onReady != nil {
		onReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[webhooks-router] Server listening on %s (Infisical: %s, Nomad: %s, TargetVariable: %s)",
			ln.Addr().String(), cfg.InfisicalURL, cfg.NomadAddr, TargetNomadVariable)
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

	log.Println("[webhooks-router] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[webhooks-router] HTTP shutdown error: %v", shutErr)
	}

	if riverClient != nil {
		if stopErr := riverClient.Stop(shutdownCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			log.Printf("[webhooks-router] River client stop error: %v", stopErr)
		}
	}

	log.Println("[webhooks-router] Server stopped")
	return runErr
}

// runApp runs the webhooks-router application.
func runApp(ctx context.Context, cfg Config) error {
	return runServer(ctx, cfg, nil)
}

func main() {
	cfg := LoadConfigFromEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[webhooks-router] Error: %v", err)
	}
}
