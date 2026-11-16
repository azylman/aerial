package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/httpkit"
)

const (
	chunkModeAction    = "action"
	fieldBuild         = "build"
	fieldStatus        = "status"
	fieldBuildID       = "build_id"
	fieldChunks        = "chunks"
	fieldChunk         = "chunk"
	statusUpdated      = "updated"
	statusCIFailed     = "ci_failed"
	statusFailed       = "failed"
	statusDeployed     = "deployed"
	statusDeploying    = "deploying"
	statusSynced       = "synced"
	statusConflict     = "conflict"
	statusMerged       = "merged"
	statusOpen         = "open"
	statusRunning      = "running"
	statusPending      = "pending"
	statusCompleted    = "completed"
	statusCancelled    = "cancelled"
	statusQueued       = "queued"
	statusCoding       = "coding"
	statusPROpened     = "pr_opened"
	statusDeployFailed = "deploy_failed"
	statusPassed       = "passed"
	statusNotFound     = "not_found"
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

// VanadisChunkPRLookup represents the result of looking up a chunk by repository and PR number.
type VanadisChunkPRLookup struct {
	Matched bool   `json:"matched"`
	ChunkID string `json:"chunk_id,omitempty"`
	BuildID string `json:"build_id,omitempty"`
	Effort  string `json:"effort,omitempty"`
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

func resolveChunkRef(chunks []VanadisChunk, chunkRef string) int {
	clean := strings.TrimSpace(chunkRef)
	if clean == "" {
		return -1
	}
	for i, c := range chunks {
		if c.ID == clean {
			return i
		}
	}
	if idx, err := strconv.Atoi(clean); err == nil {
		for i, c := range chunks {
			if c.ChunkIndex == idx {
				return i
			}
		}
	}
	return -1
}

func normalizeWidgetStatus(dbStatus string) string {
	switch strings.ToLower(strings.TrimSpace(dbStatus)) {
	case statusCIFailed, statusDeployFailed:
		return statusFailed
	default:
		return strings.ToLower(strings.TrimSpace(dbStatus))
	}
}

func resolveVanadisChunkWidgetStatus(chunkStatus, prDBStatus string) string {
	cStat := normalizeWidgetStatus(chunkStatus)
	switch normalizeWidgetStatus(prDBStatus) {
	case statusDeployed:
		return statusDeployed
	case statusDeploying:
		return statusDeploying
	case statusSynced:
		return statusSynced
	case statusFailed:
		return statusFailed
	case statusConflict:
		return statusConflict
	case statusMerged:
		return statusMerged
	case "closed":
		if cStat == statusMerged {
			return statusMerged
		}
		return statusFailed
	case statusOpen, statusPROpened:
		if cStat == statusMerged {
			return statusMerged
		}
		return statusOpen
	default:
		return firstNonEmpty(cStat, "open")
	}
}

// GetChunk retrieves a VanadisChunk and its parent VanadisBuild by chunkID.
func (s *PostgresVanadisStore) GetChunk(ctx context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, nil, errors.New("chunk id cannot be empty")
	}
	const chunkQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1;`
	rows, err := s.db.Query(ctx, chunkQuery, chunkID)
	if err != nil {
		return nil, nil, fmt.Errorf("query chunk %s: %w", chunkID, err)
	}
	defer rows.Close()
	chunks, err := scanChunks(rows)
	if err != nil {
		return nil, nil, err
	}
	if len(chunks) == 0 {
		return nil, nil, ErrChunkNotFound
	}
	c := chunks[0]
	const buildQuery = `SELECT ` + buildColumns + ` FROM vanadis_builds WHERE id = $1;`
	b, err := scanBuild(s.db.QueryRow(ctx, buildQuery, c.BuildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s for chunk %s: %w", c.BuildID, chunkID, err)
	}
	return &c, &b, nil
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
	trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/chunks/"), "/")
	if trimmed == "" {
		writeErr(w, http.StatusBadRequest, "chunk id is required")
		return
	}
	chunk, build, err := s.store.GetChunk(r.Context(), trimmed)
	if err != nil {
		writeStoreErr(w, err)
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
	var resp AttachChunkPRResponse
	if err := s.callRestateIngress(r.Context(), "/BuildRouter/RouteAttachPR", req, &resp); err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		fieldStatus:  "attached",
		fieldBuildID: resp.BuildID,
		"chunk_id":   resp.ChunkID,
		"thread_id":  resp.ThreadID,
	})
}

func isVanadisChunkStreamingPhase(chunkStatus string) bool {
	switch strings.ToLower(strings.TrimSpace(chunkStatus)) {
	case statusOpen, statusPROpened, statusMerged, statusSynced, statusDeploying:
		return true
	default:
		return false
	}
}

func hasFailedWidgetChunk(chunks []StatusWidgetChunk) bool {
	for _, c := range chunks {
		if c.Status == statusFailed || c.Status == statusConflict {
			return true
		}
	}
	return false
}

func resolveWidgetBanner(widgetStatus string, req SyncChunkPRWidgetRequest) string {
	if (widgetStatus == statusFailed || widgetStatus == statusConflict) && req.PRFound {
		return req.PRBanner
	}
	if req.PRNumber == 0 && strings.TrimSpace(req.PRBanner) != "" {
		return strings.TrimSpace(req.PRBanner)
	}
	return ""
}

func resolveWidgetChunkRepo(c VanadisChunk) string {
	if c.Mode == chunkModeAction {
		if c.PRNumber > 0 && strings.TrimSpace(c.Repo) != "" {
			return c.Repo
		}
		return chunkModeAction
	}
	return firstNonEmpty(c.Repo, "aerial")
}

func buildStatusWidgetChunk(c VanadisChunk, req SyncChunkPRWidgetRequest, widgetStatus *string, deployUIURL ...string) StatusWidgetChunk {
	baseURL := firstNonEmpty(deployUIURL...)
	chunkRepo := resolveWidgetChunkRepo(c)
	chunkStatus := normalizeWidgetStatus(c.Status)
	completedAt := c.CompletedAt
	var activeStep string
	switch {
	case req.PRNumber > 0 && c.PRNumber == req.PRNumber && req.PRFound:
		chunkStatus = resolveVanadisChunkWidgetStatus(c.Status, req.PRStatus)
		if (chunkStatus == statusFailed || chunkStatus == statusConflict) && *widgetStatus != statusCompleted {
			*widgetStatus = statusFailed
		}
		if isVanadisChunkStreamingPhase(chunkStatus) {
			activeStep = strings.TrimSpace(req.ActiveStep)
			if activeStep == "" {
				activeStep = FormatChunkActiveStep(c, baseURL)
			}
			completedAt = time.Time{}
		}
	case chunkStatus == statusCompleted:
		chunkStatus = statusDeployed
	case isVanadisChunkStreamingPhase(chunkStatus):
		activeStep = FormatChunkActiveStep(c, baseURL)
		if chunkStatus == statusMerged && (activeStep == "" || *widgetStatus == statusCompleted) {
			chunkStatus = statusDeployed
			activeStep = ""
		}
	}
	return StatusWidgetChunk{
		ID:          c.ID,
		Index:       c.ChunkIndex,
		Repo:        chunkRepo,
		Title:       c.Title,
		Status:      chunkStatus,
		ActiveStep:  activeStep,
		PRNumber:    c.PRNumber,
		StartedAt:   c.StartedAt,
		CompletedAt: completedAt,
		UpdatedAt:   c.UpdatedAt,
	}
}

func buildSyncWidgetRequest(build VanadisBuild, chunks []VanadisChunk, req SyncChunkPRWidgetRequest, deployUIURL ...string) StatusWidgetRequest {
	widgetStatus := firstNonEmpty(normalizeWidgetStatus(build.Status), "running")
	baseURL := firstNonEmpty(deployUIURL...)
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		wChunks[i] = buildStatusWidgetChunk(c, req, &widgetStatus, baseURL)
	}
	if widgetStatus == statusFailed && req.PRFound && req.PRNumber > 0 && !hasFailedWidgetChunk(wChunks) {
		widgetStatus = statusRunning
	}
	return StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     firstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    widgetStatus,
		Banner:    resolveWidgetBanner(widgetStatus, req),
		SkipSink:  build.Quiet,
		Chunks:    wChunks,
		StartedAt: build.CreatedAt,
	}
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
		if _, err := normalizeChunkMode(*req.Mode); err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
	}
	if effortStr != "" {
		normEffort, effErr := normalizeChunkEffort(effortStr)
		if effErr != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, effErr)
		}
		return normEffort, nil
	}
	return "", nil
}

func applyChunkUpdates(c *VanadisChunk, req UpdateChunkRequest, normEffort string) error {
	newMode := c.Mode
	if req.Mode != nil {
		m, err := normalizeChunkMode(*req.Mode)
		if err != nil {
			return err
		}
		newMode = m
	}
	newRepo := c.Repo
	if repo := strings.TrimSpace(req.Repo); repo != "" {
		newRepo = normalizeRepo(repo)
	} else if newMode == chunkModeAction {
		newRepo = ""
	}
	newTargetFiles := c.TargetFiles
	if req.TargetFiles != nil {
		newTargetFiles = nonNilSlice(req.TargetFiles)
	} else if newMode == chunkModeAction {
		newTargetFiles = []string{}
	}
	newTargetSymbols := c.TargetSymbols
	if req.TargetSymbols != nil {
		newTargetSymbols = nonNilSlice(req.TargetSymbols)
	} else if newMode == chunkModeAction {
		newTargetSymbols = []string{}
	}

	switch newMode {
	case "pr":
		if newRepo == "" {
			return fmt.Errorf("%w: repo is required for pr mode", ErrInvalidBuildInput)
		}
	case chunkModeAction:
		if newRepo != "" {
			return fmt.Errorf("%w: action chunk must have empty repo", ErrInvalidBuildInput)
		}
		if len(newTargetFiles) > 0 {
			return fmt.Errorf("%w: action chunk must not specify target_files", ErrInvalidBuildInput)
		}
		if len(newTargetSymbols) > 0 {
			return fmt.Errorf("%w: action chunk must not specify target_symbols", ErrInvalidBuildInput)
		}
	}

	c.Mode = newMode
	c.Repo = newRepo
	c.TargetFiles = newTargetFiles
	c.TargetSymbols = newTargetSymbols

	if title := strings.TrimSpace(req.Title); title != "" {
		c.Title = title
	}
	if instructions := strings.TrimSpace(req.Instructions); instructions != "" {
		c.Instructions = instructions
	}
	if normEffort != "" {
		c.Effort = normEffort
	}
	if req.DependsOn != nil {
		c.DependsOn = nonNilSlice(*req.DependsOn)
	}
	return nil
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
	var req UpdateBuildRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Title) == "" && strings.TrimSpace(req.Objective) == "" && strings.TrimSpace(req.SharedContext) == "" {
		writeStoreErr(w, fmt.Errorf("%w: at least one of title, objective, or shared_context must be provided", ErrInvalidBuildInput))
		return
	}
	var updated VanadisBuild
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/UpdateBuild", buildID), req, &updated); err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldStatus: statusUpdated, fieldBuild: updated})
}

func (s *VanadisServer) handleGetBuildChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	build, chunks, err := s.store.GetBuild(r.Context(), buildID)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	idx := resolveChunkRef(chunks, chunkRef)
	if idx < 0 {
		writeStoreErr(w, ErrChunkNotFound)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldChunk: chunks[idx], fieldBuild: build})
}

func (s *VanadisServer) handleUpdateChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	var req UpdateChunkRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if _, err := validateUpdateChunkInput(buildID, chunkRef, req); err != nil {
		writeStoreErr(w, err)
		return
	}
	req.ChunkRef = chunkRef
	var updated VanadisChunk
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/UpdateChunk", buildID), req, &updated); err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldStatus: statusUpdated, fieldChunk: updated})
}

func (s *VanadisServer) handleAppendChunks(w http.ResponseWriter, r *http.Request, buildID string) {
	var req AppendChunksRequest
	if !s.decodeJSONBody(w, r, &req) {
		return
	}
	if len(req.Chunks) == 0 {
		writeStoreErr(w, fmt.Errorf("%w: chunks cannot be empty", ErrInvalidBuildInput))
		return
	}
	var appended []VanadisChunk
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/AppendChunks", buildID), req, &appended); err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldStatus: "appended", fieldBuildID: buildID, fieldChunks: appended})
}

func (s *VanadisServer) handleRemoveChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	removedID := chunkRef
	if _, chs, err := s.store.GetBuild(r.Context(), buildID); err == nil {
		if idx := resolveChunkRef(chs, chunkRef); idx >= 0 {
			removedID = chs[idx].ID
		}
	}
	req := RemoveChunkRequest{ChunkRef: chunkRef}
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/RemoveChunk", buildID), req, nil); err != nil {
		writeStoreErr(w, err)
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
