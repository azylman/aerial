package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
)

const (
	chunkModeAction          = "action"
	errMissingCallerIdentity = "unauthorized: missing caller identity"
	fieldBuild               = "build"
	fieldStatus              = "status"
	fieldBuildID             = "build_id"
	fieldChunks              = "chunks"
	fieldChunk               = "chunk"
	statusUpdated            = "updated"
	statusCIFailed           = "ci_failed"
	statusFailed             = "failed"
	statusDeployed           = "deployed"
	statusDeploying          = "deploying"
	statusSynced             = "synced"
	statusConflict           = "conflict"
	statusMerged             = "merged"
	statusOpen               = "open"
	statusRunning            = "running"
	statusPending            = "pending"
	statusCompleted          = "completed"
	statusCancelled          = "cancelled"
	statusQueued             = "queued"
	statusCoding             = "coding"
	statusPROpened           = "pr_opened"
	statusDeployFailed       = "deploy_failed"
	statusPassed             = "passed"
	statusNotFound           = "not_found"
)

// VanadisChunkSummary captures a lightweight view of a chunk for advancement outputs.
type VanadisChunkSummary struct {
	ID          string    `json:"id"`
	ChunkIndex  int       `json:"chunk_index"`
	Mode        string    `json:"mode"`
	Repo        string    `json:"repo,omitempty"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

// VanadisAdvancementResult represents the outcome of advancing a chunk via PR merge.
type VanadisAdvancementResult struct {
	Matched          bool                  `json:"matched"`
	Advanced         bool                  `json:"advanced"`
	BuildID          string                `json:"build_id"`
	ThreadID         string                `json:"thread_id"`
	Tier             int                   `json:"tier"`
	Title            string                `json:"title"`
	Objective        string                `json:"objective"`
	BuildStatus      string                `json:"build_status"`
	StatusMessageID  string                `json:"status_message_id,omitempty"`
	MergedChunkIndex int                   `json:"merged_chunk_index"`
	MergedChunkTitle string                `json:"merged_chunk_title"`
	ChunkMode        string                `json:"chunk_mode,omitempty"`
	NextChunkIndex   int                   `json:"next_chunk_index"`
	BuildCompleted   bool                  `json:"build_completed"`
	Chunks           []VanadisChunkSummary `json:"chunks,omitempty"`
}

// AttachChunkPRRequest represents the payload for POST /chunks/attach-pr.
type AttachChunkPRRequest struct {
	ChunkID  string `json:"chunk_id,omitempty"`
	PRNumber int    `json:"pr_number"`
	Repo     string `json:"repo"`
	Branch   string `json:"branch,omitempty"`
	Title    string `json:"title,omitempty"`
	TargetID string `json:"target_id,omitempty"`
	Quiet    bool   `json:"quiet,omitempty"`
	// Agent is the caller agent that owns a new standalone build. Agent
	// callers are always resolved from their ServiceAccount token; the body
	// value is honored only for service principals and unauthenticated mode.
	Agent string `json:"agent,omitempty"`
}

// ChunkPRRequest represents generic repo + pr_number request payloads.
type ChunkPRRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
}

// SyncChunkPRWidgetRequest represents the payload for POST /chunks/sync-pr-widget.
type SyncChunkPRWidgetRequest struct {
	BuildID    string `json:"build_id,omitempty"`
	Repo       string `json:"repo"`
	PRNumber   int    `json:"pr_number"`
	PRStatus   string `json:"pr_status"`
	PRBanner   string `json:"pr_banner"`
	ActiveStep string `json:"active_step,omitempty"`
	PRFound    bool   `json:"pr_found"`
}

// UpdateBuildRequest defines the request payload for updating build metadata.
type UpdateBuildRequest struct {
	Title         string `json:"title,omitempty"`
	Objective     string `json:"objective,omitempty"`
	SharedContext string `json:"shared_context,omitempty"`
}

// UpdateChunkRequest defines the request payload for updating chunk fields before launch.
type UpdateChunkRequest struct {
	ChunkRef      string    `json:"chunk_ref,omitempty"`
	Mode          *string   `json:"mode,omitempty"`
	Repo          string    `json:"repo,omitempty"`
	Title         string    `json:"title,omitempty"`
	Instructions  string    `json:"instructions,omitempty"`
	TargetFiles   []string  `json:"target_files,omitempty"`
	TargetSymbols []string  `json:"target_symbols,omitempty"`
	Effort        string    `json:"effort,omitempty"`
	DependsOn     *[]string `json:"depends_on,omitempty"`
}

// AppendChunksRequest defines the request payload for appending chunks to a running build.
type AppendChunksRequest struct {
	Chunks []CreateChunkInput `json:"chunks"`
}

func (s *VanadisServer) authorizeBuildUpdate(ctx context.Context, buildAgent string) error {
	if !s.authEnabled() {
		return nil
	}
	caller, ok := k8sauth.CallerFromContext(ctx)
	if !ok {
		return &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity}
	}
	return s.auth.Authorize(ctx, caller, "builds", "update", strutil.FirstNonEmpty(buildAgent, agent.Default))
}

// authorizeBuildMutation loads the build and authorizes the caller to update
// it, returning the build so the mutation can route to its runner.
func (s *VanadisServer) authorizeBuildMutation(w http.ResponseWriter, r *http.Request, buildID string) (*VanadisBuild, bool) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return nil, false
	}
	build, _, err := s.store.GetBuild(r.Context(), buildID)
	if err == nil && build == nil {
		err = ErrBuildNotFound
	}
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return nil, false
	}
	if err := s.authorizeBuildUpdate(r.Context(), build.Agent); err != nil {
		k8sauth.WriteError(w, err)
		return nil, false
	}
	return build, true
}

// mutateBuildObject calls a handler on the Build object.
func (s *VanadisServer) mutateBuildObject(ctx context.Context, build *VanadisBuild, handler string, req, out any) error {
	return s.callRestateIngress(ctx, s.restateObjectPath("", build.ID, handler), req, out)
}

const maxTrackedActiveStepBuilds = 256

func activeStepIdempotencyKey(buildID string, req UpdateActiveStepRequest) string {
	buildID = strings.TrimSpace(buildID)
	chunkID := strings.TrimSpace(req.ChunkID)
	stepText := strutil.FirstNonEmpty(strings.TrimSpace(req.ActiveStep), strings.TrimSpace(req.Step))
	if chunkID != "" && (stepText != "" || req.ActiveSteps == nil) {
		h := sha256.Sum256([]byte(stepText))
		return fmt.Sprintf("active-step:%s:%s:%x", buildID, chunkID, h)
	}
	if len(req.ActiveSteps) == 1 {
		for cID, sText := range req.ActiveSteps {
			h := sha256.Sum256([]byte(sText))
			return fmt.Sprintf("active-step:%s:%s:%x", buildID, cID, h)
		}
	}
	if len(req.ActiveSteps) > 1 {
		keys := make([]string, 0, len(req.ActiveSteps))
		for k := range req.ActiveSteps {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteString("=")
			b.WriteString(req.ActiveSteps[k])
			b.WriteString(";")
		}
		h := sha256.Sum256([]byte(b.String()))
		return fmt.Sprintf("active-step:%s:batch:%x", buildID, h)
	}
	if req.ClearAll || len(req.ActiveSteps) == 0 {
		h := sha256.Sum256([]byte("clear-all"))
		return fmt.Sprintf("active-step:%s:clear:%x", buildID, h)
	}
	h := sha256.Sum256([]byte(stepText))
	return fmt.Sprintf("active-step:%s:unknown:%x", buildID, h)
}

func (s *VanadisServer) forwardActiveStep(ctx context.Context, build *VanadisBuild, req UpdateActiveStepRequest) error {
	path := s.restateObjectPath("", build.ID, "UpdateActiveStep")
	idempotencyKey := activeStepIdempotencyKey(build.ID, req)
	return s.callRestateIngressKeyed(ctx, path, idempotencyKey, req, nil)
}

func (s *VanadisServer) clearBuildStepMapLocked(buildID, chunkID string) {
	m := s.stepsByBuild[buildID]
	if m == nil {
		return
	}
	delete(m, chunkID)
	if len(m) > 0 {
		return
	}
	delete(s.stepsByBuild, buildID)
	if tm := s.trailingTimer[buildID]; tm != nil {
		tm.Stop()
		delete(s.trailingTimer, buildID)
	}
	if !s.inFlightBuilds[buildID] {
		delete(s.lastForwardedAt, buildID)
		delete(s.dirtyBuilds, buildID)
	}
}

func (s *VanadisServer) updateBuildStepMapLocked(buildID, chunkID, stepText string) {
	if stepText == "" {
		s.clearBuildStepMapLocked(buildID, chunkID)
		return
	}
	m := s.stepsByBuild[buildID]
	if m == nil {
		m = make(map[string]string)
		s.stepsByBuild[buildID] = m
	}
	m[chunkID] = stepText
}

func (s *VanadisServer) pruneStaleBuildsLocked(now time.Time) {
	if len(s.lastForwardedAt) <= maxTrackedActiveStepBuilds {
		return
	}
	for bID, ts := range s.lastForwardedAt {
		if !s.inFlightBuilds[bID] && (now.Sub(ts) > time.Hour || len(s.lastForwardedAt) > maxTrackedActiveStepBuilds) {
			delete(s.lastForwardedAt, bID)
			delete(s.stepsByBuild, bID)
			delete(s.dirtyBuilds, bID)
			if tm := s.trailingTimer[bID]; tm != nil {
				tm.Stop()
				delete(s.trailingTimer, bID)
			}
		}
	}
}

func (s *VanadisServer) scheduleTrailingFlushLocked(ctx context.Context, build *VanadisBuild, remaining time.Duration) {
	if remaining < 10*time.Millisecond {
		remaining = 10 * time.Millisecond
	}
	if s.trailingTimer == nil {
		s.trailingTimer = make(map[string]*time.Timer)
	}
	if s.trailingTimer[build.ID] == nil {
		b := build
		trailingCtx := context.WithoutCancel(ctx)
		s.trailingTimer[build.ID] = time.AfterFunc(remaining, func() {
			s.flushTrailingActiveStep(trailingCtx, b)
		})
	}
}

func (s *VanadisServer) forwardActiveStepLoop(ctx context.Context, build *VanadisBuild) error {
	for {
		s.activeStepMu.Lock()
		now := time.Now()
		if s.nowFn != nil {
			now = s.nowFn()
		}
		s.lastForwardedAt[build.ID] = now
		delete(s.dirtyBuilds, build.ID)
		activeSteps := s.stepsByBuild[build.ID]
		isClearAll := len(activeSteps) == 0
		req := UpdateActiveStepRequest{
			ActiveSteps: maps.Clone(activeSteps),
			ClearAll:    isClearAll,
		}
		s.activeStepMu.Unlock()

		if err := s.forwardActiveStep(ctx, build, req); err != nil {
			s.activeStepMu.Lock()
			delete(s.inFlightBuilds, build.ID)
			delete(s.dirtyBuilds, build.ID)
			s.activeStepMu.Unlock()
			return err
		}

		s.activeStepMu.Lock()
		if !s.dirtyBuilds[build.ID] {
			delete(s.inFlightBuilds, build.ID)
			if len(s.stepsByBuild[build.ID]) == 0 {
				delete(s.stepsByBuild, build.ID)
				delete(s.lastForwardedAt, build.ID)
				delete(s.dirtyBuilds, build.ID)
			}
			s.activeStepMu.Unlock()
			return nil
		}
		s.activeStepMu.Unlock()
	}
}

func (s *VanadisServer) flushTrailingActiveStep(ctx context.Context, build *VanadisBuild) {
	s.activeStepMu.Lock()
	delete(s.trailingTimer, build.ID)
	if s.inFlightBuilds[build.ID] {
		s.dirtyBuilds[build.ID] = true
		s.activeStepMu.Unlock()
		return
	}
	if len(s.stepsByBuild[build.ID]) == 0 {
		s.activeStepMu.Unlock()
		return
	}
	s.inFlightBuilds[build.ID] = true
	s.activeStepMu.Unlock()

	defer func() {
		s.activeStepMu.Lock()
		delete(s.inFlightBuilds, build.ID)
		s.activeStepMu.Unlock()
	}()

	if err := s.forwardActiveStepLoop(ctx, build); err != nil {
		slog.WarnContext(ctx, "[vanadis] trailing active step forward warning", "build_id", build.ID, "error", err)
	}
}

func (s *VanadisServer) shouldDebounceActiveStepLocked(ctx context.Context, build *VanadisBuild, isClear bool, now time.Time, minInterval time.Duration) bool {
	buildID := build.ID
	if s.inFlightBuilds[buildID] {
		s.dirtyBuilds[buildID] = true
		return true
	}
	if isClear {
		return false
	}
	last := s.lastForwardedAt[buildID]
	if !last.IsZero() && now.Sub(last) < minInterval {
		s.scheduleTrailingFlushLocked(ctx, build, minInterval-now.Sub(last))
		return true
	}
	return false
}

func (s *VanadisServer) processActiveStepUpdate(w http.ResponseWriter, r *http.Request, build *VanadisBuild, req UpdateActiveStepRequest) {
	if err := s.authorizeBuildUpdate(r.Context(), build.Agent); err != nil {
		k8sauth.WriteError(w, err)
		return
	}

	chunkID := strings.TrimSpace(req.ChunkID)
	stepText := strutil.FirstNonEmpty(strings.TrimSpace(req.ActiveStep), strings.TrimSpace(req.Step))

	now := time.Now()
	if s.nowFn != nil {
		now = s.nowFn()
	}
	minInterval := s.minActiveStepInterval
	if minInterval <= 0 {
		minInterval = 2 * time.Second
	}

	s.activeStepMu.Lock()
	if s.stepsByBuild == nil {
		s.stepsByBuild = make(map[string]map[string]string)
	}
	if s.inFlightBuilds == nil {
		s.inFlightBuilds = make(map[string]bool)
	}
	if s.lastForwardedAt == nil {
		s.lastForwardedAt = make(map[string]time.Time)
	}
	if s.dirtyBuilds == nil {
		s.dirtyBuilds = make(map[string]bool)
	}

	s.pruneStaleBuildsLocked(now)

	if stepText != "" && s.stepsByBuild[build.ID] != nil && s.stepsByBuild[build.ID][chunkID] == stepText {
		s.activeStepMu.Unlock()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
		return
	}

	s.updateBuildStepMapLocked(build.ID, chunkID, stepText)

	if s.shouldDebounceActiveStepLocked(r.Context(), build, stepText == "", now, minInterval) {
		s.activeStepMu.Unlock()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
		return
	}

	if tm := s.trailingTimer[build.ID]; tm != nil {
		tm.Stop()
		delete(s.trailingTimer, build.ID)
	}

	s.inFlightBuilds[build.ID] = true
	s.activeStepMu.Unlock()

	defer func() {
		s.activeStepMu.Lock()
		delete(s.inFlightBuilds, build.ID)
		s.activeStepMu.Unlock()
	}()

	if err := s.forwardActiveStepLoop(r.Context(), build); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
}

func (s *VanadisServer) handleChunksActiveStep(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	var req UpdateActiveStepRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ChunkID) == "" {
		writeErr(w, http.StatusBadRequest, "chunk_id is required")
		return
	}
	_, build, err := s.store.GetChunk(r.Context(), req.ChunkID)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	if build == nil {
		writeStoreErr(r.Context(), w, ErrBuildNotFound)
		return
	}
	s.processActiveStepUpdate(w, r, build, req)
}

func (s *VanadisServer) handleBuildActiveStep(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	var req UpdateActiveStepRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ChunkID) == "" {
		writeErr(w, http.StatusBadRequest, "chunk_id is required")
		return
	}
	build, _, err := s.store.GetBuild(r.Context(), id)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	s.processActiveStepUpdate(w, r, build, req)
}

func (s *VanadisServer) handleChunkByID(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.authorizeBuildRead(r.Context(), "get"); err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/chunks/"), "/")
	if trimmed == "" {
		writeErr(w, http.StatusBadRequest, "chunk id is required")
		return
	}
	chunk, build, err := s.store.GetChunk(r.Context(), trimmed)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldChunk: chunk, fieldBuild: build})
}

func decodeChunkPRPost[T any](s *VanadisServer, w http.ResponseWriter, r *http.Request, missingMsg string, valid func(*T) bool) (T, bool) {
	var zero T
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return zero, false
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return zero, false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return zero, false
	}
	var req T
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return zero, false
	}
	if !valid(&req) {
		writeErr(w, http.StatusBadRequest, missingMsg)
		return zero, false
	}
	return req, true
}

func (s *VanadisServer) handleAttachChunkPR(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeChunkPRPost(s, w, r, "chunk_id or repo, and valid pr_number are required", func(req *AttachChunkPRRequest) bool {
		req.ChunkID = strings.TrimSpace(req.ChunkID)
		req.Repo = strings.TrimSpace(req.Repo)
		return (req.ChunkID != "" || req.Repo != "") && req.PRNumber > 0
	})
	if !ok {
		return
	}
	attachAgent, err := s.resolveAttachCallerAgent(r.Context(), req.Agent)
	if err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	req.Agent = attachAgent
	if req.ChunkID != "" && req.PRNumber > 0 && s.store != nil {
		if err := s.store.PreRecordChunkPR(r.Context(), req.ChunkID, req.Repo, req.PRNumber); err != nil {
			slog.WarnContext(r.Context(), "[vanadis] prerecord chunk pr failed", "chunk_id", req.ChunkID, "repo", req.Repo, "pr_number", req.PRNumber, "error", err)
		}
	}
	var resp AttachChunkPRResponse
	idempotencyKey := fmt.Sprintf("attach-pr:%s:%s:%d", req.ChunkID, req.Repo, req.PRNumber)
	if err := s.callRestateIngressKeyed(r.Context(), s.restateRouterPath("RouteAttachPR"), idempotencyKey, req, &resp); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		fieldStatus:  "attached",
		fieldBuildID: resp.BuildID,
		"chunk_id":   resp.ChunkID,
		"thread_id":  resp.ThreadID,
	})
}

// resolveAttachCallerAgent returns the agent that owns, and whose runner
// executes, a standalone build created by an attach-PR call. Agent callers
// are pinned to their own ServiceAccount agent; service principals and
// unauthenticated deployments may name one, defaulting to the default agent.
func (s *VanadisServer) resolveAttachCallerAgent(ctx context.Context, requested string) (string, error) {
	requested = agent.Clean(requested)
	if s.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(ctx)
		if !ok {
			return "", &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity}
		}
		if !caller.IsService {
			requested = agent.Clean(caller.CallerAgent)
		}
	}
	if requested != "" && !k8sauth.IsValidAgentSlug(requested) {
		return "", &k8sauth.ValidationError{Message: fmt.Sprintf("invalid agent %q", requested)}
	}
	return strutil.FirstNonEmpty(requested, agent.Default), nil
}

func validateUpdateChunkInput(buildID, chunkRef string, req UpdateChunkRequest) (string, error) {
	if strings.TrimSpace(buildID) == "" || strings.TrimSpace(chunkRef) == "" {
		return "", fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}
	repo := strings.TrimSpace(req.Repo)
	title := strings.TrimSpace(req.Title)
	instructions := strings.TrimSpace(req.Instructions)
	effortStr := strings.TrimSpace(req.Effort)
	hasField := req.Mode != nil || repo != "" || title != "" || instructions != "" || effortStr != "" || req.TargetFiles != nil || req.TargetSymbols != nil || req.DependsOn != nil
	if !hasField {
		return "", fmt.Errorf("%w: build id, chunk ref, and at least one updated field are required", ErrInvalidBuildInput)
	}
	if req.Mode != nil {
		if strings.TrimSpace(*req.Mode) == "" {
			return "", fmt.Errorf("%w: mode cannot be empty string (must be 'pr' or 'action')", ErrInvalidBuildInput)
		}
		if _, err := buildplan.NormalizeChunkMode(*req.Mode); err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
	}
	if effortStr != "" {
		normEffort, effErr := buildplan.NormalizeChunkEffort(effortStr)
		if effErr != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, effErr)
		}
		return normEffort, nil
	}
	return "", nil
}

func (s *VanadisServer) decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) bool {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return false
	}
	if err = json.Unmarshal(body, target); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return false
	}
	return true
}

func (s *VanadisServer) handleUpdateBuild(w http.ResponseWriter, r *http.Request, buildID string) {
	serveBuildMutation(s, w, r, buildID, "UpdateBuild", func(req *UpdateBuildRequest) error {
		if strings.TrimSpace(req.Title) == "" && strings.TrimSpace(req.Objective) == "" && strings.TrimSpace(req.SharedContext) == "" {
			return fmt.Errorf("%w: at least one of title, objective, or shared_context must be provided", ErrInvalidBuildInput)
		}
		return nil
	}, func(updated VanadisBuild) map[string]any {
		return map[string]any{fieldStatus: statusUpdated, fieldBuild: updated}
	})
}

// serveBuildMutation decodes and prepares a per-build mutation request,
// authorizes it against the stored build, and forwards it to the Build object
// of the runner that executes that build.
func serveBuildMutation[Req, Out any](s *VanadisServer, w http.ResponseWriter, r *http.Request, buildID, handler string, prepare func(*Req) error, respond func(Out) map[string]any) {
	var req Req
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if err := prepare(&req); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	build, ok := s.authorizeBuildMutation(w, r, buildID)
	if !ok {
		return
	}
	var out Out
	if err := s.mutateBuildObject(r.Context(), build, handler, req, &out); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, respond(out))
}

func (s *VanadisServer) handleGetBuildChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if err := s.authorizeBuildRead(r.Context(), "get"); err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	build, chunks, err := s.store.GetBuild(r.Context(), buildID)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	idx := buildplan.ResolveChunkRef(chunks, chunkRef)
	if idx < 0 {
		writeStoreErr(r.Context(), w, ErrChunkNotFound)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldChunk: chunks[idx], fieldBuild: build})
}

func (s *VanadisServer) handleUpdateChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	serveBuildMutation(s, w, r, buildID, "UpdateChunk", func(req *UpdateChunkRequest) error {
		if _, err := validateUpdateChunkInput(buildID, chunkRef, *req); err != nil {
			return err
		}
		req.ChunkRef = chunkRef
		return nil
	}, func(updated VanadisChunk) map[string]any {
		return map[string]any{fieldStatus: statusUpdated, fieldChunk: updated}
	})
}

func (s *VanadisServer) handleAppendChunks(w http.ResponseWriter, r *http.Request, buildID string) {
	serveBuildMutation(s, w, r, buildID, "AppendChunks", func(req *AppendChunksRequest) error {
		if len(req.Chunks) == 0 {
			return fmt.Errorf("%w: chunks cannot be empty", ErrInvalidBuildInput)
		}
		return nil
	}, func(appended []VanadisChunk) map[string]any {
		return map[string]any{fieldStatus: "appended", fieldBuildID: buildID, fieldChunks: appended}
	})
}

func (s *VanadisServer) handleRemoveChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	build, chs, err := s.store.GetBuild(r.Context(), buildID)
	if err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	if err := s.authorizeBuildUpdate(r.Context(), build.Agent); err != nil {
		k8sauth.WriteError(w, err)
		return
	}
	removedID := chunkRef
	if idx := buildplan.ResolveChunkRef(chs, chunkRef); idx >= 0 {
		removedID = chs[idx].ID
	}
	req := RemoveChunkRequest{ChunkRef: chunkRef}
	if err := s.mutateBuildObject(r.Context(), build, "RemoveChunk", req, nil); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	var remaining []VanadisChunk
	if s.store != nil {
		_, chs, err := s.store.GetBuild(r.Context(), buildID)
		if err == nil {
			remaining = chs
		}
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:        "removed",
		fieldBuildID:       buildID,
		"removed_chunk_id": removedID,
		fieldChunks:        remaining,
	})
}
