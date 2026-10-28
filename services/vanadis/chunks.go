package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/jackc/pgx/v5"
)

// VanadisChunkSummary captures a lightweight view of a chunk for advancement outputs.
type VanadisChunkSummary struct {
	ID         string    `json:"id"`
	ChunkIndex int       `json:"chunk_index"`
	Repo       string    `json:"repo,omitempty"`
	Title      string    `json:"title"`
	Status     string    `json:"status"`
	PRNumber   int       `json:"pr_number,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
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
	NextChunkIndex   int                   `json:"next_chunk_index"`
	BuildCompleted   bool                  `json:"build_completed"`
	Chunks           []VanadisChunkSummary `json:"chunks,omitempty"`
}

// VanadisChunkPRLookup represents the result of looking up a chunk by repository and PR number.
type VanadisChunkPRLookup struct {
	Matched            bool   `json:"matched"`
	ChunkID            string `json:"chunk_id,omitempty"`
	BuildID            string `json:"build_id,omitempty"`
	Effort             string `json:"effort,omitempty"`
	HasRemainingChunks bool   `json:"has_remaining_chunks"`
}

// AttachChunkPRRequest represents the payload for POST /chunks/attach-pr.
type AttachChunkPRRequest struct {
	ChunkID  string `json:"chunk_id"`
	PRNumber int    `json:"pr_number"`
	Repo     string `json:"repo"`
}

// ChunkPRRequest represents generic repo + pr_number request payloads.
type ChunkPRRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
}

// SyncChunkPRWidgetRequest represents the payload for POST /chunks/sync-pr-widget.
type SyncChunkPRWidgetRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	PRStatus string `json:"pr_status"`
	PRBanner string `json:"pr_banner"`
	PRFound  bool   `json:"pr_found"`
}

func normalizeWidgetStatus(dbStatus string) string {
	switch strings.ToLower(strings.TrimSpace(dbStatus)) {
	case "ci_failed", "deploy_failed":
		return "failed"
	default:
		return strings.ToLower(strings.TrimSpace(dbStatus))
	}
}

func resolveVanadisChunkWidgetStatus(chunkStatus, prDBStatus string) string {
	cStat := normalizeWidgetStatus(chunkStatus)
	if cStat == "failed" {
		return "failed"
	}
	switch normalizeWidgetStatus(prDBStatus) {
	case "deployed":
		return "deployed"
	case "deploying":
		return "deploying"
	case "synced":
		return "synced"
	case "failed":
		return "failed"
	case "conflict":
		return "conflict"
	case "merged":
		return "merged"
	case "closed":
		if cStat == "merged" {
			return "merged"
		}
		return "failed"
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
	const chunkQuery = `SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1;`
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
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at FROM vanadis_builds WHERE id = $1;`
	b, err := scanBuild(s.db.QueryRow(ctx, buildQuery, c.BuildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s for chunk %s: %w", c.BuildID, chunkID, err)
	}
	return &c, &b, nil
}

// GetChunkByPR looks up a chunk and its parent build for a given repo and pr_number.
func (s *PostgresVanadisStore) GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return &VanadisChunkPRLookup{Matched: false}, nil
	}
	const query = `
		SELECT c.id, c.build_id, COALESCE(NULLIF(c.effort, ''), 'medium'),
		       EXISTS (
		           SELECT 1 FROM vanadis_chunks c2
		           WHERE c2.build_id = c.build_id AND c2.chunk_index > c.chunk_index
		       )
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = $1 AND c.pr_number = $2 AND b.status != 'cancelled'
		ORDER BY c.updated_at DESC
		LIMIT 1;`
	var chunkID, buildID, effort string
	var hasRemaining bool
	err := s.db.QueryRow(ctx, query, repo, prNumber).Scan(&chunkID, &buildID, &effort, &hasRemaining)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &VanadisChunkPRLookup{Matched: false}, nil
		}
		return nil, fmt.Errorf("query chunk by pr %s#%d: %w", repo, prNumber, err)
	}
	return &VanadisChunkPRLookup{
		Matched:            true,
		ChunkID:            chunkID,
		BuildID:            buildID,
		Effort:             capChunkEffort(effort),
		HasRemainingChunks: hasRemaining,
	}, nil
}

// AttachChunkPR links a PR number and repository to a chunk, returning the build's thread ID.
func (s *PostgresVanadisStore) AttachChunkPR(ctx context.Context, chunkID string, prNumber int, repo string) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	repo = normalizeRepo(repo)
	if chunkID == "" || prNumber <= 0 {
		return "", errors.New("chunk id and valid pr number are required")
	}
	var threadID string
	var err error
	if repo != "" {
		const qWithRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND c.repo = $3 RETURNING b.thread_id;`
		err = s.db.QueryRow(ctx, qWithRepo, prNumber, chunkID, repo).Scan(&threadID)
	} else {
		const qNoRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 RETURNING b.thread_id;`
		err = s.db.QueryRow(ctx, qNoRepo, prNumber, chunkID).Scan(&threadID)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			checkErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1);`, chunkID).Scan(&exists)
			if checkErr == nil && !exists {
				return "", ErrChunkNotFound
			}
			return "", fmt.Errorf("chunk %s cannot attach pr %d (mismatched repo)", chunkID, prNumber)
		}
		return "", fmt.Errorf("attach chunk pr %s#%d: %w", repo, prNumber, err)
	}
	return threadID, nil
}

func computeNextChunkIndex(chunks []VanadisChunkSummary, currentIdx int) int {
	nextChunkIdx := -1
	fallbackIdx := -1
	for _, cs := range chunks {
		if cs.Status != "merged" {
			if cs.ChunkIndex > currentIdx && (nextChunkIdx == -1 || cs.ChunkIndex < nextChunkIdx) {
				nextChunkIdx = cs.ChunkIndex
			} else if fallbackIdx == -1 {
				fallbackIdx = cs.ChunkIndex
			}
		}
	}
	if nextChunkIdx == -1 {
		return fallbackIdx
	}
	return nextChunkIdx
}

func scanVanadisChunkSummaries(rows pgx.Rows, mergedChunkID string) ([]VanadisChunkSummary, error) {
	var allChunks []VanadisChunkSummary
	for rows.Next() {
		var cs VanadisChunkSummary
		var startedAt sql.NullTime
		if scanErr := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &startedAt, &cs.UpdatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan vanadis chunk summary: %w", scanErr)
		}
		if startedAt.Valid {
			cs.StartedAt = startedAt.Time
		}
		if cs.ID == mergedChunkID {
			cs.Status = "merged"
		}
		allChunks = append(allChunks, cs)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate vanadis chunk summaries: %w", err)
	}
	return allChunks, nil
}

func isTerminalBuildStatus(status string) bool {
	return status == "cancelled" || status == "failed" || status == "completed"
}

func queryAndAdvanceChunks(ctx context.Context, tx pgx.Tx, buildID, chunkID, cStatus string, chunkIdx int, res *VanadisAdvancementResult) (*VanadisAdvancementResult, error) {
	const chunksQuery = `SELECT id, chunk_index, repo, title, status, COALESCE(pr_number, 0), started_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE`
	rows, err := tx.Query(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, fmt.Errorf("query vanadis build chunks for advance: %w", err)
	}
	defer rows.Close()

	allChunks, err := scanVanadisChunkSummaries(rows, chunkID)
	if err != nil {
		return nil, err
	}

	nextChunkIdx := computeNextChunkIndex(allChunks, chunkIdx)
	if nextChunkIdx >= 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit vanadis chunk advance tx: %w", err)
		}
		res.Advanced = cStatus != "merged"
		res.NextChunkIndex = nextChunkIdx
		res.BuildCompleted = false
		res.Chunks = allChunks
		return res, nil
	}

	const updateBuildQuery = `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(ctx, updateBuildQuery, buildID); err != nil {
		return nil, fmt.Errorf("update vanadis build completed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit vanadis build completed tx: %w", err)
	}
	res.Advanced = cStatus != "merged"
	res.BuildStatus = "completed"
	res.NextChunkIndex = -1
	res.BuildCompleted = true
	res.Chunks = allChunks
	return res, nil
}

// AdvanceChunkByPR advances a chunk to merged and computes the next chunk index or marks the build completed.
func (s *PostgresVanadisStore) AdvanceChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin vanadis advance tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] advance tx rollback error: %v", rbErr)
		}
	}()

	const selectChunkQuery = `
		SELECT c.id, c.build_id, c.chunk_index, c.title, c.status, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, b.status, COALESCE(b.status_message_id, '')
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = $1 AND c.pr_number = $2
		ORDER BY c.updated_at DESC LIMIT 1 FOR UPDATE`

	var chunkID, buildID, cTitle, cStatus, threadID, bTitle, objective, bStatus, bStatusMsgID string
	var chunkIdx, tier int
	err = tx.QueryRow(ctx, selectChunkQuery, repo, prNumber).Scan(
		&chunkID, &buildID, &chunkIdx, &cTitle, &cStatus, &threadID, &tier, &bTitle, &objective, &bStatus, &bStatusMsgID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select vanadis chunk by pr: %w", err)
	}

	res := &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         false,
		BuildID:          buildID,
		ThreadID:         threadID,
		Tier:             tier,
		Title:            bTitle,
		Objective:        objective,
		BuildStatus:      bStatus,
		StatusMessageID:  bStatusMsgID,
		MergedChunkIndex: chunkIdx,
		MergedChunkTitle: cTitle,
		NextChunkIndex:   -1,
		BuildCompleted:   bStatus == "completed",
	}

	if isTerminalBuildStatus(bStatus) {
		return res, nil
	}

	if cStatus != "merged" {
		const updateChunkQuery = `UPDATE vanadis_chunks SET status = 'merged', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
		if _, err := tx.Exec(ctx, updateChunkQuery, chunkID); err != nil {
			return nil, fmt.Errorf("update vanadis chunk merged: %w", err)
		}
	}

	return queryAndAdvanceChunks(ctx, tx, buildID, chunkID, cStatus, chunkIdx, res)
}

// FailChunkByPR marks a non-terminal chunk and its running build as failed.
func (s *PostgresVanadisStore) FailChunkByPR(ctx context.Context, repo string, prNumber int) (string, string, bool, error) {
	if s == nil || s.db == nil {
		return "", "", false, errors.New("database connection is nil")
	}
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", "", false, nil
	}
	const query = `
		UPDATE vanadis_chunks c
		SET status = 'failed', updated_at = CURRENT_TIMESTAMP
		FROM vanadis_builds b
		WHERE c.build_id = b.id AND c.repo = $1 AND c.pr_number = $2 AND c.status NOT IN ('merged', 'failed')
		RETURNING c.build_id, c.id;`
	var buildID, chunkID string
	err := s.db.QueryRow(ctx, query, repo, prNumber).Scan(&buildID, &chunkID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("fail chunk by pr %s#%d: %w", repo, prNumber, err)
	}
	const updateBuild = `UPDATE vanadis_builds SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'running';`
	if _, err := s.db.Exec(ctx, updateBuild, buildID); err != nil {
		return "", "", false, fmt.Errorf("fail vanadis build by pr: %w", err)
	}
	return buildID, chunkID, true, nil
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
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk, "build": build})
}

func (s *VanadisServer) handleChunkByPR(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	repo := strings.TrimSpace(r.URL.Query().Get("repo"))
	prNumStr := strings.TrimSpace(r.URL.Query().Get("pr_number"))
	if repo == "" || prNumStr == "" {
		writeErr(w, http.StatusBadRequest, "repo and pr_number are required")
		return
	}
	prNum, err := strconv.Atoi(prNumStr)
	if err != nil || prNum <= 0 {
		writeErr(w, http.StatusBadRequest, "invalid pr_number")
		return
	}
	lookup, err := s.store.GetChunkByPR(r.Context(), repo, prNum)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, lookup)
}

func (s *VanadisServer) handleAttachChunkPR(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req AttachChunkPRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	req.ChunkID = strings.TrimSpace(req.ChunkID)
	if req.ChunkID == "" || req.PRNumber <= 0 {
		writeErr(w, http.StatusBadRequest, "chunk_id and valid pr_number are required")
		return
	}
	threadID, err := s.store.AttachChunkPR(r.Context(), req.ChunkID, req.PRNumber, req.Repo)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		"status":    "attached",
		"chunk_id":  req.ChunkID,
		"thread_id": threadID,
	})
}

func (s *VanadisServer) handleAdvanceChunkPR(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req ChunkPRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	req.Repo = strings.TrimSpace(req.Repo)
	if req.Repo == "" || req.PRNumber <= 0 {
		writeErr(w, http.StatusBadRequest, "repo and valid pr_number are required")
		return
	}
	res, err := s.store.AdvanceChunkByPR(r.Context(), req.Repo, req.PRNumber)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if res != nil && res.Matched && !res.BuildCompleted && res.NextChunkIndex >= 0 {
		if err := s.insertRiverJob(r.Context(), ExecuteChunkArgs{BuildID: res.BuildID, ChunkIndex: res.NextChunkIndex}); err != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue next chunk %d: %v", res.NextChunkIndex, err))
			return
		}
	}
	httpkit.WriteJSON(w, http.StatusOK, res)
}

func (s *VanadisServer) handleFailChunkPR(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req ChunkPRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	req.Repo = strings.TrimSpace(req.Repo)
	if req.Repo == "" || req.PRNumber <= 0 {
		writeErr(w, http.StatusBadRequest, "repo and valid pr_number are required")
		return
	}
	buildID, chunkID, failed, err := s.store.FailChunkByPR(r.Context(), req.Repo, req.PRNumber)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"build_id": buildID,
		"chunk_id": chunkID,
		"failed":   failed,
	})
}

func buildSyncWidgetRequest(build VanadisBuild, chunks []VanadisChunk, req SyncChunkPRWidgetRequest) StatusWidgetRequest {
	widgetStatus := firstNonEmpty(normalizeWidgetStatus(build.Status), "running")
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		chunkRepo := firstNonEmpty(c.Repo, "azylman/aerial")
		chunkStatus := normalizeWidgetStatus(c.Status)
		if c.PRNumber == req.PRNumber && req.PRFound {
			chunkStatus = resolveVanadisChunkWidgetStatus(c.Status, req.PRStatus)
			if (chunkStatus == "failed" || chunkStatus == "conflict") && widgetStatus != "completed" {
				widgetStatus = "failed"
			}
		} else if chunkStatus == "merged" {
			chunkStatus = "deployed"
		}
		wChunks[i] = StatusWidgetChunk{
			Index:     c.ChunkIndex,
			Repo:      chunkRepo,
			Title:     c.Title,
			Status:    chunkStatus,
			PRNumber:  c.PRNumber,
			StartedAt: c.StartedAt,
			UpdatedAt: c.UpdatedAt,
		}
	}
	widgetBanner := ""
	if (widgetStatus == "failed" || widgetStatus == "conflict") && req.PRFound {
		widgetBanner = req.PRBanner
	}
	return StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     firstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    widgetStatus,
		Banner:    widgetBanner,
		Chunks:    wChunks,
	}
}

func (s *VanadisServer) dispatchAndPersistWidget(ctx context.Context, buildID, existingMsgID string, req StatusWidgetRequest) error {
	respBody, err := s.postBrainJSON(ctx, "/discord/widget", req, 4<<10)
	if err != nil {
		return fmt.Errorf("dispatch vanadis widget: %w", err)
	}
	var res struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(respBody, &res); err == nil && res.MessageID != "" && res.MessageID != existingMsgID && s.store != nil {
		if uErr := s.store.UpdateBuildStatusMessageID(ctx, buildID, res.MessageID); uErr != nil {
			log.Printf("[vanadis] warning: failed to update status_message_id for build %s: %v", buildID, uErr)
		}
	}
	return nil
}

func (s *VanadisServer) handleSyncPRWidget(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req SyncChunkPRWidgetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	req.Repo = strings.TrimSpace(req.Repo)
	if req.Repo == "" || req.PRNumber <= 0 {
		writeErr(w, http.StatusBadRequest, "repo and valid pr_number are required")
		return
	}
	lookup, err := s.store.GetChunkByPR(r.Context(), req.Repo, req.PRNumber)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if !lookup.Matched {
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{"matched": false})
		return
	}
	build, chunks, err := s.store.GetBuild(r.Context(), lookup.BuildID)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	widgetReq := buildSyncWidgetRequest(*build, chunks, req)
	if err := s.dispatchAndPersistWidget(r.Context(), build.ID, build.StatusMessageID, widgetReq); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"matched": true, "build_id": build.ID})
}
