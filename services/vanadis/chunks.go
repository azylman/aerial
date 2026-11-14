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
	Matched            bool   `json:"matched"`
	ChunkID            string `json:"chunk_id,omitempty"`
	BuildID            string `json:"build_id,omitempty"`
	Effort             string `json:"effort,omitempty"`
	HasRemainingChunks bool   `json:"has_remaining_chunks"`
	// NextChunkMode is the mode ("pr" or "action") of the chunk AdvanceChunkByPR
	// would start once this chunk merges, or "" when the build would complete.
	NextChunkMode string `json:"next_chunk_mode,omitempty"`
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

// UpdateChunkRequest defines the request payload for mutating an existing pending chunk.
type UpdateChunkRequest struct {
	Mode          *string  `json:"mode,omitempty"`
	Repo          string   `json:"repo,omitempty"`
	Title         string   `json:"title,omitempty"`
	Instructions  string   `json:"instructions,omitempty"`
	TargetFiles   []string `json:"target_files,omitempty"`
	TargetSymbols []string `json:"target_symbols,omitempty"`
	Effort        string   `json:"effort,omitempty"`
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
	case "ci_failed", "deploy_failed":
		return "failed"
	default:
		return strings.ToLower(strings.TrimSpace(dbStatus))
	}
}

func resolveVanadisChunkWidgetStatus(chunkStatus, prDBStatus string) string {
	cStat := normalizeWidgetStatus(chunkStatus)
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
	case "open", "pr_opened":
		if cStat == "merged" {
			return "merged"
		}
		return "open"
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
	const chunkQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1;`
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

// AttachChunkPR links a PR number and repository to a chunk, returning the build's thread ID.
func (s *PostgresVanadisStore) AttachChunkPR(ctx context.Context, chunkID string, prNumber int, repo string) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	candidates := repoMatchCandidates(repo)
	if chunkID == "" || prNumber <= 0 {
		return "", errors.New("chunk id and valid pr number are required")
	}

	var mode string
	chkErr := s.db.QueryRow(ctx, `SELECT mode FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&mode)
	if chkErr != nil {
		if errors.Is(chkErr, pgx.ErrNoRows) {
			return "", ErrChunkNotFound
		}
		return "", fmt.Errorf("check chunk mode %s: %w", chunkID, chkErr)
	}
	if mode == "action" {
		return "", fmt.Errorf("%w: action chunk cannot attach a pull request", ErrBuildConflict)
	}

	var threadID string
	var err error
	if len(candidates) > 0 {
		const qWithRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND c.repo = ANY($3) AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') RETURNING b.thread_id;`
		err = s.db.QueryRow(ctx, qWithRepo, prNumber, chunkID, candidates).Scan(&threadID)
	} else {
		const qNoRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') RETURNING b.thread_id;`
		err = s.db.QueryRow(ctx, qNoRepo, prNumber, chunkID).Scan(&threadID)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", s.resolveAttachChunkPRError(ctx, chunkID, repo, prNumber)
		}
		return "", fmt.Errorf("attach chunk pr %s#%d: %w", repo, prNumber, err)
	}
	return threadID, nil
}

func (s *PostgresVanadisStore) resolveAttachChunkPRError(ctx context.Context, chunkID, repo string, prNumber int) error {
	var exists bool
	checkErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1);`, chunkID).Scan(&exists)
	if checkErr == nil && !exists {
		return fmt.Errorf("%w: %s", ErrChunkNotFound, chunkID)
	}
	if repo == "" {
		return fmt.Errorf("%w: chunk %s or parent build is cancelled", ErrBuildConflict, chunkID)
	}
	var isCancelled bool
	cancErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND (c.status = 'cancelled' OR b.status IN ('cancelled', 'completed')));`, chunkID).Scan(&isCancelled)
	if cancErr == nil && isCancelled {
		return fmt.Errorf("%w: chunk %s or parent build is cancelled", ErrBuildConflict, chunkID)
	}
	return fmt.Errorf("chunk %s cannot attach pr %d (mismatched repo)", chunkID, prNumber)
}

func computeNextChunkIndex(chunks []VanadisChunkSummary, currentIdx int) int {
	nextChunkIdx := -1
	fallbackIdx := -1
	for _, cs := range chunks {
		if !isChunkDoneStatus(cs.Status) {
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
		var startedAt, completedAt sql.NullTime
		if scanErr := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Mode, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &startedAt, &completedAt, &cs.UpdatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan vanadis chunk summary: %w", scanErr)
		}
		if startedAt.Valid {
			cs.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			cs.CompletedAt = completedAt.Time
		}
		if cs.ID == mergedChunkID {
			if cs.Mode == "action" {
				cs.Status = "completed"
			} else {
				cs.Status = "merged"
			}
			if cs.CompletedAt.IsZero() {
				cs.CompletedAt = time.Now().UTC()
			}
		}
		allChunks = append(allChunks, cs)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate vanadis chunk summaries: %w", err)
	}
	return allChunks, nil
}

func isTerminalBuildStatus(status string) bool {
	return status == "cancelled"
}

func advanceToNextChunk(ctx context.Context, tx pgx.Tx, buildID string, nextChunkIdx int, cStatus string, allChunks []VanadisChunkSummary, res *VanadisAdvancementResult) (*VanadisAdvancementResult, error) {
	if res.BuildStatus == "failed" {
		const recoverBuildQuery = `UPDATE vanadis_builds SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
		if _, err := tx.Exec(ctx, recoverBuildQuery, buildID); err != nil {
			return nil, fmt.Errorf("recover vanadis build running: %w", err)
		}
		res.BuildStatus = "running"
	}
	const updateNextChunkQuery = `UPDATE vanadis_chunks SET status = 'running', started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE build_id = $1 AND chunk_index = $2 AND status = 'pending'`
	if _, err := tx.Exec(ctx, updateNextChunkQuery, buildID, nextChunkIdx); err != nil {
		return nil, fmt.Errorf("update next vanadis chunk running: %w", err)
	}
	now := time.Now().UTC()
	for i := range allChunks {
		if allChunks[i].ChunkIndex == nextChunkIdx && allChunks[i].Status == "pending" {
			allChunks[i].Status = "running"
			if allChunks[i].StartedAt.IsZero() {
				allChunks[i].StartedAt = now
			}
			allChunks[i].UpdatedAt = now
			break
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit vanadis chunk advance tx: %w", err)
	}
	res.Advanced = cStatus != "merged"
	res.NextChunkIndex = nextChunkIdx
	res.BuildCompleted = false
	res.Chunks = allChunks
	return res, nil
}

func completeBuild(ctx context.Context, tx pgx.Tx, buildID string, cStatus string, allChunks []VanadisChunkSummary, res *VanadisAdvancementResult) (*VanadisAdvancementResult, error) {
	if res.BuildStatus != "completed" {
		const updateBuildQuery = `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
		if _, err := tx.Exec(ctx, updateBuildQuery, buildID); err != nil {
			return nil, fmt.Errorf("update vanadis build completed: %w", err)
		}
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

func queryAndAdvanceChunks(ctx context.Context, tx pgx.Tx, buildID, chunkID, cStatus string, chunkIdx int, res *VanadisAdvancementResult) (*VanadisAdvancementResult, error) {
	const chunksQuery = `SELECT id, chunk_index, mode, repo, title, status, COALESCE(pr_number, 0), started_at, completed_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE`
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
		return advanceToNextChunk(ctx, tx, buildID, nextChunkIdx, cStatus, allChunks, res)
	}

	return completeBuild(ctx, tx, buildID, cStatus, allChunks, res)
}

// AdvanceChunkByPR advances a chunk to merged and computes the next chunk index or marks the build completed.
func (s *PostgresVanadisStore) AdvanceChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	candidates := repoMatchCandidates(repo)
	if len(candidates) == 0 || prNumber <= 0 {
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
		SELECT c.id, c.build_id, c.chunk_index, c.mode, c.title, c.status, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, b.status, COALESCE(b.status_message_id, '')
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = ANY($1) AND c.pr_number = $2
		ORDER BY c.updated_at DESC LIMIT 1 FOR UPDATE`

	var chunkID, buildID, cTitle, cStatus, threadID, bTitle, objective, bStatus, bStatusMsgID, cMode string
	var chunkIdx, tier int
	err = tx.QueryRow(ctx, selectChunkQuery, candidates, prNumber).Scan(
		&chunkID, &buildID, &chunkIdx, &cMode, &cTitle, &cStatus, &threadID, &tier, &bTitle, &objective, &bStatus, &bStatusMsgID,
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
		const updateChunkQuery = `UPDATE vanadis_chunks SET status = 'merged', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1`
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
	candidates := repoMatchCandidates(repo)
	if len(candidates) == 0 || prNumber <= 0 {
		return "", "", false, nil
	}
	const query = `
		UPDATE vanadis_chunks c
		SET status = 'failed', completed_at = COALESCE(c.completed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
		FROM vanadis_builds b
		WHERE c.build_id = b.id AND c.repo = ANY($1) AND c.pr_number = $2 AND c.status NOT IN ('merged', 'failed')
		RETURNING c.build_id, c.id;`
	var buildID, chunkID string
	err := s.db.QueryRow(ctx, query, candidates, prNumber).Scan(&buildID, &chunkID)
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

// UpdateBuild updates non-empty metadata fields (title, objective, shared_context) of a running VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*VanadisBuild, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	title := strings.TrimSpace(req.Title)
	obj := strings.TrimSpace(req.Objective)
	sharedCtx := strings.TrimSpace(req.SharedContext)
	if buildID == "" || (title == "" && obj == "" && sharedCtx == "") {
		return nil, fmt.Errorf("%w: build id and at least one non-empty field (title, objective, shared_context) are required", ErrInvalidBuildInput)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin update build tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] update build tx rollback error: %v", rbErr)
		}
	}()

	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at FROM vanadis_builds WHERE id = $1 FOR UPDATE;`
	b, err := scanBuild(tx.QueryRow(ctx, buildQuery, buildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build %s: %w", buildID, err)
	}
	if b.Status != "running" {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}

	if title != "" {
		b.Title = title
	}
	if obj != "" {
		b.Objective = obj
	}
	if sharedCtx != "" {
		b.SharedContext = sharedCtx
	}

	const updateQuery = `UPDATE vanadis_builds SET title = $1, objective = $2, shared_context = $3, updated_at = CURRENT_TIMESTAMP WHERE id = $4;`
	if _, err := tx.Exec(ctx, updateQuery, b.Title, b.Objective, b.SharedContext, buildID); err != nil {
		return nil, fmt.Errorf("update build %s: %w", buildID, err)
	}
	b.UpdatedAt = time.Now().UTC()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit update build %s: %w", buildID, err)
	}
	return &b, nil
}

func lockRunningBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) ([]VanadisChunk, error) {
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at FROM vanadis_builds WHERE id = $1 FOR UPDATE;`
	b, err := scanBuild(tx.QueryRow(ctx, buildQuery, buildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build %s: %w", buildID, err)
	}
	if b.Status != "running" {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, b.Status)
	}

	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE;`
	rows, err := tx.Query(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks for build %s: %w", buildID, err)
	}
	defer rows.Close()
	chunks, err := scanChunks(rows)
	if err != nil {
		return nil, err
	}
	return chunks, nil
}

func validateUpdateChunkInput(buildID, chunkRef string, req UpdateChunkRequest) (string, error) {
	if strings.TrimSpace(buildID) == "" || strings.TrimSpace(chunkRef) == "" {
		return "", fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}
	repo := strings.TrimSpace(req.Repo)
	title := strings.TrimSpace(req.Title)
	instructions := strings.TrimSpace(req.Instructions)
	effortStr := strings.TrimSpace(req.Effort)
	hasField := req.Mode != nil || repo != "" || title != "" || instructions != "" || effortStr != "" || req.TargetFiles != nil || req.TargetSymbols != nil
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
	} else if newMode == "action" {
		newRepo = ""
	}
	newTargetFiles := c.TargetFiles
	if req.TargetFiles != nil {
		newTargetFiles = nonNilSlice(req.TargetFiles)
	} else if newMode == "action" {
		newTargetFiles = []string{}
	}
	newTargetSymbols := c.TargetSymbols
	if req.TargetSymbols != nil {
		newTargetSymbols = nonNilSlice(req.TargetSymbols)
	} else if newMode == "action" {
		newTargetSymbols = []string{}
	}

	switch newMode {
	case "pr":
		if newRepo == "" {
			return fmt.Errorf("%w: repo is required for pr mode", ErrInvalidBuildInput)
		}
	case "action":
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
	return nil
}

// UpdateChunk modifies fields of a pending chunk within a running VanadisBuild.
func (s *PostgresVanadisStore) UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	normEffort, err := validateUpdateChunkInput(buildID, chunkRef, req)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin update chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] update chunk tx rollback error: %v", rbErr)
		}
	}()

	chunks, err := lockRunningBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return nil, err
	}

	idx := resolveChunkRef(chunks, chunkRef)
	if idx < 0 {
		return nil, ErrChunkNotFound
	}
	c := chunks[idx]
	if c.Status != "pending" {
		return nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, c.ID, c.Status)
	}

	if err := applyChunkUpdates(&c, req, normEffort); err != nil {
		return nil, err
	}

	const updateChunkQ = `UPDATE vanadis_chunks SET mode = $1, repo = $2, title = $3, instructions = $4, target_files = $5, target_symbols = $6, effort = $7, updated_at = CURRENT_TIMESTAMP WHERE id = $8;`
	if _, err := tx.Exec(ctx, updateChunkQ, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.ID); err != nil {
		return nil, fmt.Errorf("update chunk %s: %w", c.ID, err)
	}
	c.UpdatedAt = time.Now().UTC()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit update chunk %s: %w", c.ID, err)
	}
	return &c, nil
}

func validateAppendChunksInputs(buildID string, inputs []CreateChunkInput) error {
	if strings.TrimSpace(buildID) == "" || len(inputs) == 0 {
		return fmt.Errorf("%w: build id and at least one chunk are required", ErrInvalidBuildInput)
	}
	for i, in := range inputs {
		if strings.TrimSpace(in.Title) == "" || strings.TrimSpace(in.Instructions) == "" {
			return fmt.Errorf("%w: chunk[%d] title and instructions cannot be empty", ErrInvalidBuildInput, i)
		}
		if id := strings.TrimSpace(in.ID); id != "" && !isValidVanadisChunkID(id) {
			return fmt.Errorf("%w: chunk[%d] invalid id %q", ErrInvalidBuildInput, i, id)
		}
		if err := validateChunkModeAndRepo(i, in.Mode, in.Repo, in.TargetFiles, in.TargetSymbols); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
		if _, err := normalizeChunkEffort(in.Effort); err != nil {
			return fmt.Errorf("%w: chunk[%d] %w", ErrInvalidBuildInput, i, err)
		}
	}
	return nil
}

func resolveAppendedChunkID(buildID string, rawID string, nextIdx int, usedIDs map[string]bool) (string, error) {
	if cleanID := strings.TrimSpace(rawID); cleanID != "" {
		if usedIDs[cleanID] {
			return "", fmt.Errorf("%w: duplicate chunk id %q", ErrBuildConflict, cleanID)
		}
		return cleanID, nil
	}
	n := nextIdx + 1
	for {
		candidate := fmt.Sprintf("%s-chunk-%d", buildID, n)
		if !usedIDs[candidate] {
			return candidate, nil
		}
		n++
	}
}

func buildAppendedChunk(buildID string, in CreateChunkInput, nextIdx int, usedIDs map[string]bool, now time.Time) (VanadisChunk, error) {
	resolvedID, err := resolveAppendedChunkID(buildID, in.ID, nextIdx, usedIDs)
	if err != nil {
		return VanadisChunk{}, err
	}
	usedIDs[resolvedID] = true
	mode, err := normalizeChunkMode(in.Mode)
	if err != nil {
		return VanadisChunk{}, fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
	}
	repo := ""
	if mode == "pr" {
		repo = normalizeRepo(in.Repo)
	}
	eff, err := normalizeChunkEffort(in.Effort)
	if err != nil {
		return VanadisChunk{}, err
	}
	return VanadisChunk{
		ID:                 resolvedID,
		BuildID:            buildID,
		ChunkIndex:         nextIdx,
		Mode:               mode,
		Repo:               repo,
		Title:              strings.TrimSpace(in.Title),
		Instructions:       strings.TrimSpace(in.Instructions),
		TargetFiles:        nonNilSlice(in.TargetFiles),
		TargetSymbols:      nonNilSlice(in.TargetSymbols),
		Effort:             eff,
		AcceptanceCriteria: nonNilSlice(in.AcceptanceCriteria),
		Status:             "pending",
		CreatedAt:          now,
		UpdatedAt:          now,
	}, nil
}

// AppendChunks appends new chunks to the end of a running VanadisBuild.
func (s *PostgresVanadisStore) AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if err := validateAppendChunksInputs(buildID, inputs); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin append chunks tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] append chunks tx rollback error: %v", rbErr)
		}
	}()

	existing, err := lockRunningBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return nil, err
	}

	usedIDs := make(map[string]bool, len(existing)+len(inputs))
	for _, c := range existing {
		usedIDs[c.ID] = true
	}

	nextIdx := 0
	if len(existing) > 0 {
		last := existing[len(existing)-1]
		nextIdx = last.ChunkIndex + 1
	}

	now := time.Now().UTC()
	appended := make([]VanadisChunk, 0, len(inputs))
	for _, in := range inputs {
		newChunk, err := buildAppendedChunk(buildID, in, nextIdx, usedIDs, now)
		if err != nil {
			return nil, err
		}
		if err := execInsertVanadisChunk(ctx, tx, buildID, newChunk); err != nil {
			return nil, err
		}
		appended = append(appended, newChunk)
		nextIdx++
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit append chunks for build %s: %w", buildID, err)
	}
	return appended, nil
}

// RemoveChunk deletes a pending chunk from a running VanadisBuild and compacts chunk indices.
func (s *PostgresVanadisStore) RemoveChunk(ctx context.Context, buildID, chunkRef string) (*VanadisChunk, []VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	if buildID == "" || chunkRef == "" {
		return nil, nil, fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin remove chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] remove chunk tx rollback error: %v", rbErr)
		}
	}()

	existing, err := lockRunningBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return nil, nil, err
	}

	removeIdx := resolveChunkRef(existing, chunkRef)
	if removeIdx < 0 {
		return nil, nil, ErrChunkNotFound
	}
	removed := existing[removeIdx]
	if removed.Status != "pending" {
		return nil, nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, removed.ID, removed.Status)
	}
	if len(existing) <= 1 {
		return nil, nil, fmt.Errorf("%w: cannot remove the only chunk of build %s", ErrBuildConflict, buildID)
	}

	const delQ = `DELETE FROM vanadis_chunks WHERE id = $1;`
	if _, err := tx.Exec(ctx, delQ, removed.ID); err != nil {
		return nil, nil, fmt.Errorf("delete chunk %s: %w", removed.ID, err)
	}

	const compactQ = `UPDATE vanadis_chunks SET chunk_index = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`
	for i := removeIdx + 1; i < len(existing); i++ {
		newIdx := existing[i].ChunkIndex - 1
		if _, err := tx.Exec(ctx, compactQ, newIdx, existing[i].ID); err != nil {
			return nil, nil, fmt.Errorf("compact chunk %s index to %d: %w", existing[i].ID, newIdx, err)
		}
		existing[i].ChunkIndex = newIdx
	}

	remaining := make([]VanadisChunk, 0, len(existing)-1)
	remaining = append(remaining, existing[:removeIdx]...)
	remaining = append(remaining, existing[removeIdx+1:]...)

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit remove chunk %s: %w", removed.ID, err)
	}
	return &removed, remaining, nil
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
	req, ok := decodeChunkPRPost(s, w, r, "chunk_id and valid pr_number are required", func(req *AttachChunkPRRequest) bool {
		req.ChunkID = strings.TrimSpace(req.ChunkID)
		return req.ChunkID != "" && req.PRNumber > 0
	})
	if !ok {
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
	req, ok := decodeChunkPRPost(s, w, r, "repo and valid pr_number are required", func(req *ChunkPRRequest) bool {
		req.Repo = strings.TrimSpace(req.Repo)
		return req.Repo != "" && req.PRNumber > 0
	})
	if !ok {
		return
	}
	res, err := s.store.AdvanceChunkByPR(r.Context(), req.Repo, req.PRNumber)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if res != nil && res.Matched {
		if res.Advanced {
			RecordChunkTotal("merged")
		}
		if res.BuildCompleted && res.Advanced {
			RecordBuildTotal("completed")
		}
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
	req, ok := decodeChunkPRPost(s, w, r, "repo and valid pr_number are required", func(req *ChunkPRRequest) bool {
		req.Repo = strings.TrimSpace(req.Repo)
		return req.Repo != "" && req.PRNumber > 0
	})
	if !ok {
		return
	}
	buildID, chunkID, failed, err := s.store.FailChunkByPR(r.Context(), req.Repo, req.PRNumber)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if failed {
		RecordChunkTotal("failed")
		RecordBuildTotal("failed")
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"build_id": buildID,
		"chunk_id": chunkID,
		"failed":   failed,
	})
}

func (s *VanadisServer) dispatchCompletedBuildWake(ctx context.Context, res *VanadisAdvancementResult) {
	if res == nil || strings.TrimSpace(res.ThreadID) == "" {
		return
	}
	prompt := formatBuildCompletedWakePrompt(res)
	if err := s.dispatchWakePrompt(ctx, res.ThreadID, prompt); err != nil {
		log.Printf("[vanadis] warning: failed dispatching completed wake prompt to thread %s: %v", res.ThreadID, err)
	}
}

func isVanadisChunkStreamingPhase(chunkStatus string) bool {
	switch strings.ToLower(strings.TrimSpace(chunkStatus)) {
	case "open", "pr_opened", "merged", "synced", "deploying":
		return true
	default:
		return false
	}
}

func hasFailedWidgetChunk(chunks []StatusWidgetChunk) bool {
	for _, c := range chunks {
		if c.Status == "failed" || c.Status == "conflict" {
			return true
		}
	}
	return false
}

func resolveWidgetBanner(widgetStatus string, req SyncChunkPRWidgetRequest) string {
	if (widgetStatus == "failed" || widgetStatus == "conflict") && req.PRFound {
		return req.PRBanner
	}
	if req.PRNumber == 0 && strings.TrimSpace(req.PRBanner) != "" {
		return strings.TrimSpace(req.PRBanner)
	}
	return ""
}

func buildSyncWidgetRequest(build VanadisBuild, chunks []VanadisChunk, req SyncChunkPRWidgetRequest) StatusWidgetRequest {
	widgetStatus := firstNonEmpty(normalizeWidgetStatus(build.Status), "running")
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		chunkRepo := firstNonEmpty(c.Repo, "aerial")
		if c.Mode == "action" {
			chunkRepo = "action"
		}
		chunkStatus := normalizeWidgetStatus(c.Status)
		completedAt := c.CompletedAt
		var activeStep string
		if req.PRNumber > 0 && c.PRNumber == req.PRNumber && req.PRFound {
			chunkStatus = resolveVanadisChunkWidgetStatus(c.Status, req.PRStatus)
			if (chunkStatus == "failed" || chunkStatus == "conflict") && widgetStatus != "completed" {
				widgetStatus = "failed"
			}
			if isVanadisChunkStreamingPhase(chunkStatus) {
				activeStep = strings.TrimSpace(req.ActiveStep)
				completedAt = time.Time{}
			}
		} else if chunkStatus == "merged" || (c.Mode == "action" && chunkStatus == "completed") {
			chunkStatus = "deployed"
		}
		wChunks[i] = StatusWidgetChunk{
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
	if widgetStatus == "failed" && req.PRFound && req.PRNumber > 0 && !hasFailedWidgetChunk(wChunks) {
		widgetStatus = "running"
	}
	return StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     firstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    widgetStatus,
		Banner:    resolveWidgetBanner(widgetStatus, req),
		Chunks:    wChunks,
		StartedAt: build.CreatedAt,
	}
}

func (s *VanadisServer) recoverBuildOnPRSync(ctx context.Context, build *VanadisBuild, chunks []VanadisChunk, req SyncChunkPRWidgetRequest) {
	if s == nil || s.store == nil || !req.PRFound || req.PRNumber <= 0 || build == nil {
		return
	}
	for i := range chunks {
		c := &chunks[i]
		if c.PRNumber == req.PRNumber && normalizeWidgetStatus(c.Status) == "failed" {
			if isVanadisChunkStreamingPhase(resolveVanadisChunkWidgetStatus(c.Status, req.PRStatus)) {
				if err := s.store.UpdateChunkStatus(ctx, c.ID, "pr_opened"); err == nil {
					c.Status = "pr_opened"
					c.CompletedAt = time.Time{}
				}
			}
		}
	}
	if normalizeWidgetStatus(build.Status) == "failed" {
		hasFailure := false
		for _, c := range chunks {
			norm := normalizeWidgetStatus(c.Status)
			if norm == "failed" || norm == "conflict" {
				hasFailure = true
				break
			}
		}
		if !hasFailure {
			if err := s.store.UpdateBuildStatus(ctx, build.ID, "running"); err == nil {
				build.Status = "running"
			}
		}
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
	req.BuildID = strings.TrimSpace(req.BuildID)
	req.Repo = strings.TrimSpace(req.Repo)
	if req.BuildID != "" {
		build, chunks, err := s.store.GetBuild(r.Context(), req.BuildID)
		if errors.Is(err, ErrBuildNotFound) {
			httpkit.WriteJSON(w, http.StatusOK, map[string]any{"matched": false})
			return
		}
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		s.recoverBuildOnPRSync(r.Context(), build, chunks, req)
		widgetReq := buildSyncWidgetRequest(*build, chunks, req)
		if err := s.dispatchAndPersistWidget(r.Context(), build.ID, build.StatusMessageID, widgetReq); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{"matched": true, "build_id": build.ID})
		return
	}
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
	s.recoverBuildOnPRSync(r.Context(), build, chunks, req)
	widgetReq := buildSyncWidgetRequest(*build, chunks, req)
	if err := s.dispatchAndPersistWidget(r.Context(), build.ID, build.StatusMessageID, widgetReq); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"matched": true, "build_id": build.ID})
}

func (s *VanadisServer) handleUpdateBuild(w http.ResponseWriter, r *http.Request, buildID string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req UpdateBuildRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	updated, err := s.store.UpdateBuild(r.Context(), buildID, req)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if strings.TrimSpace(req.Title) != "" {
		s.syncBuildWidget(r.Context(), buildID, "")
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "updated", "build": updated})
}

func (s *VanadisServer) handleGetBuildChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
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
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunks[idx], "build": build})
}

func (s *VanadisServer) handleUpdateChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req UpdateChunkRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	chunk, err := s.store.UpdateChunk(r.Context(), buildID, chunkRef, req)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.syncBuildWidget(r.Context(), buildID, "")
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "updated", "chunk": chunk})
}

func (s *VanadisServer) handleAppendChunks(w http.ResponseWriter, r *http.Request, buildID string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req AppendChunksRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	appended, err := s.store.AppendChunks(r.Context(), buildID, req.Chunks)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.syncBuildWidget(r.Context(), buildID, "")
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "appended", "build_id": buildID, "chunks": appended})
}

func (s *VanadisServer) handleRemoveChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	removed, remaining, err := s.store.RemoveChunk(r.Context(), buildID, chunkRef)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.syncBuildWidget(r.Context(), buildID, "")
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":           "removed",
		"build_id":         buildID,
		"removed_chunk_id": removed.ID,
		"chunks":           remaining,
	})
}
