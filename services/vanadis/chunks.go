package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/httpkit"
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

// UpdateChunkRequest defines the request payload for mutating an existing pending chunk.
type UpdateChunkRequest struct {
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
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at FROM vanadis_builds WHERE id = $1;`
	b, err := scanBuild(s.db.QueryRow(ctx, buildQuery, c.BuildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s for chunk %s: %w", c.BuildID, chunkID, err)
	}
	return &c, &b, nil
}

// AttachChunkPR links a PR number and repository to an existing chunk, or auto-creates a standalone 1-chunk build when chunk_id is empty.
func (s *PostgresVanadisStore) AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	if s == nil || s.db == nil {
		return "", "", "", errors.New("database connection is nil")
	}
	if req.PRNumber <= 0 {
		return "", "", "", errors.New("chunk id or repo, and valid pr number are required")
	}
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID != "" {
		return s.attachToExistingChunk(ctx, chunkID, req)
	}
	return s.attachStandalonePR(ctx, req)
}

func (s *PostgresVanadisStore) attachToExistingChunk(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	var mode string
	chkErr := s.db.QueryRow(ctx, `SELECT mode FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&mode)
	if chkErr != nil {
		if errors.Is(chkErr, pgx.ErrNoRows) {
			return "", "", "", ErrChunkNotFound
		}
		return "", "", "", fmt.Errorf("check chunk mode %s: %w", chunkID, chkErr)
	}
	if mode == "action" {
		repo := normalizeRepo(req.Repo)
		if repo == "" {
			return "", "", "", errors.New("repo is required to attach PR to action chunk")
		}
		const qAction = `UPDATE vanadis_chunks c SET repo = $1, pr_number = $2, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $3 AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
		var buildID, returnChunkID, threadID string
		err := s.db.QueryRow(ctx, qAction, repo, req.PRNumber, chunkID).Scan(&buildID, &returnChunkID, &threadID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, repo, req.PRNumber)
			}
			return "", "", "", fmt.Errorf("attach action chunk pr %s#%d: %w", repo, req.PRNumber, err)
		}
		return buildID, returnChunkID, threadID, nil
	}

	candidates := repoMatchCandidates(req.Repo)
	var buildID, returnChunkID, threadID string
	var err error
	if len(candidates) > 0 {
		const qWithRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND c.repo = ANY($3) AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
		err = s.db.QueryRow(ctx, qWithRepo, req.PRNumber, chunkID, candidates).Scan(&buildID, &returnChunkID, &threadID)
	} else {
		const qNoRepo = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
		err = s.db.QueryRow(ctx, qNoRepo, req.PRNumber, chunkID).Scan(&buildID, &returnChunkID, &threadID)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, req.Repo, req.PRNumber)
		}
		return "", "", "", fmt.Errorf("attach chunk pr %s#%d: %w", req.Repo, req.PRNumber, err)
	}
	return buildID, returnChunkID, threadID, nil
}

func (s *PostgresVanadisStore) attachStandalonePR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	repo := normalizeRepo(req.Repo)
	if repo == "" {
		return "", "", "", errors.New("repo is required for standalone attach")
	}

	tx, txErr := s.db.Begin(ctx)
	if txErr != nil {
		return "", "", "", fmt.Errorf("begin standalone build tx: %w", txErr)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] standalone build tx rollback error: %v", rbErr)
		}
	}()

	lockKey := fmt.Sprintf("%s#%d", repo, req.PRNumber)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1));`, lockKey); err != nil {
		return "", "", "", fmt.Errorf("acquire standalone attach lock for %s: %w", lockKey, err)
	}

	candidates := repoMatchCandidates(repo)
	const findQ = `SELECT c.build_id, c.id, b.thread_id FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.repo = ANY($1) AND c.pr_number = $2 AND c.status != 'cancelled' AND b.status NOT IN ('cancelled', 'completed') ORDER BY c.created_at ASC LIMIT 1;`
	var existingBuildID, existingChunkID, existingThreadID string
	err := tx.QueryRow(ctx, findQ, candidates, req.PRNumber).Scan(&existingBuildID, &existingChunkID, &existingThreadID)
	if err == nil {
		if cErr := tx.Commit(ctx); cErr != nil {
			return "", "", "", fmt.Errorf("commit standalone existing match tx: %w", cErr)
		}
		return existingBuildID, existingChunkID, existingThreadID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", fmt.Errorf("find existing chunk by pr %s#%d: %w", repo, req.PRNumber, err)
	}

	newBuildID := generateBuildID()
	newChunkID := newBuildID + "-chunk-1"
	threadID := normalizeDiscordThreadID(req.TargetID)
	title := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("PR #%d", req.PRNumber))
	objective := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("Track PR #%d on %s", req.PRNumber, repo))
	isQuiet := req.Quiet || strings.HasPrefix(strings.TrimSpace(req.Branch), "dependabot/")
	now := time.Now().UTC()

	const bQ = `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`
	if _, err := tx.Exec(ctx, bQ, newBuildID, threadID, 0, title, objective, "", "running", "", isQuiet); err != nil {
		return "", "", "", fmt.Errorf("insert standalone vanadis build: %w", err)
	}

	standaloneChunk := VanadisChunk{
		ID:           newChunkID,
		BuildID:      newBuildID,
		ChunkIndex:   0,
		Mode:         "pr",
		Repo:         repo,
		Title:        title,
		Instructions: objective,
		Status:       "pr_opened",
		PRNumber:     req.PRNumber,
		DependsOn:    []string{},
		StartedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := execInsertVanadisChunk(ctx, tx, newBuildID, standaloneChunk); err != nil {
		return "", "", "", fmt.Errorf("insert standalone vanadis chunk: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", "", "", fmt.Errorf("commit standalone build tx: %w", err)
	}
	return newBuildID, newChunkID, threadID, nil
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
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at FROM vanadis_builds WHERE id = $1 FOR UPDATE;`
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

	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE;`
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
	if req.DependsOn != nil {
		c.DependsOn = nonNilSlice(*req.DependsOn)
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

	normDeps, err := validateUpdateChunkDAG(chunks, idx, c)
	if err != nil {
		return nil, err
	}
	c.DependsOn = normDeps

	const updateChunkQ = `UPDATE vanadis_chunks SET mode = $1, repo = $2, title = $3, instructions = $4, target_files = $5, target_symbols = $6, effort = $7, depends_on = $8, updated_at = CURRENT_TIMESTAMP WHERE id = $9;`
	if _, err := tx.Exec(ctx, updateChunkQ, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, nonNilSlice(c.DependsOn), c.ID); err != nil {
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

func buildAppendedChunk(buildID string, in CreateChunkInput, nextIdx int, prevChunkID string, usedIDs map[string]bool, now time.Time) (VanadisChunk, error) {
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
	var deps []string
	switch {
	case in.DependsOn != nil:
		deps = nonNilSlice(in.DependsOn)
	case prevChunkID != "":
		deps = []string{prevChunkID}
	default:
		deps = []string{}
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
		DependsOn:          deps,
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
	var prevChunkID string
	if len(existing) > 0 {
		last := existing[len(existing)-1]
		nextIdx = last.ChunkIndex + 1
		prevChunkID = last.ID
	}

	now := time.Now().UTC()
	appended := make([]VanadisChunk, 0, len(inputs))
	for _, in := range inputs {
		newChunk, err := buildAppendedChunk(buildID, in, nextIdx, prevChunkID, usedIDs, now)
		if err != nil {
			return nil, err
		}
		appended = append(appended, newChunk)
		prevChunkID = newChunk.ID
		nextIdx++
	}

	appended, err = validateAppendChunksDAG(existing, appended)
	if err != nil {
		return nil, err
	}

	for _, chunk := range appended {
		if err := execInsertVanadisChunk(ctx, tx, buildID, chunk); err != nil {
			return nil, err
		}
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

	remaining := make([]VanadisChunk, 0, len(existing)-1)
	remaining = append(remaining, existing[:removeIdx]...)
	remaining = append(remaining, existing[removeIdx+1:]...)

	relinkDependenciesAfterRemoval(remaining, removed.ID, removed.DependsOn)

	const compactQ = `UPDATE vanadis_chunks SET chunk_index = $1, depends_on = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3;`
	for i := range remaining {
		newIdx := i
		remaining[i].ChunkIndex = newIdx
		if _, err := tx.Exec(ctx, compactQ, newIdx, nonNilSlice(remaining[i].DependsOn), remaining[i].ID); err != nil {
			return nil, nil, fmt.Errorf("compact chunk %s index to %d: %w", remaining[i].ID, newIdx, err)
		}
	}

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
	buildID, chunkID, threadID, err := s.store.AttachChunkPR(r.Context(), req)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if strings.TrimSpace(req.ChunkID) == "" && s.restateCfg.Enabled {
		if err := s.launchBuildOnRestate(r.Context(), buildID); err != nil {
			log.Printf("[vanadis] warning: failed to launch standalone build %s on restate: %v", buildID, err)
		}
	}
	s.syncBuildWidget(r.Context(), buildID, "")
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		"status":    "attached",
		"build_id":  buildID,
		"chunk_id":  chunkID,
		"thread_id": threadID,
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

func resolveWidgetChunkRepo(c VanadisChunk) string {
	if c.Mode == "action" {
		if c.PRNumber > 0 && strings.TrimSpace(c.Repo) != "" {
			return c.Repo
		}
		return "action"
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
		if (chunkStatus == "failed" || chunkStatus == "conflict") && *widgetStatus != "completed" {
			*widgetStatus = "failed"
		}
		if isVanadisChunkStreamingPhase(chunkStatus) {
			activeStep = strings.TrimSpace(req.ActiveStep)
			if activeStep == "" {
				activeStep = FormatChunkActiveStep(c, baseURL)
			}
			completedAt = time.Time{}
		}
	case chunkStatus == "completed":
		chunkStatus = "deployed"
	case isVanadisChunkStreamingPhase(chunkStatus):
		activeStep = FormatChunkActiveStep(c, baseURL)
		if chunkStatus == "merged" && (activeStep == "" || *widgetStatus == "completed") {
			chunkStatus = "deployed"
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
		SkipSink:  build.Quiet,
		Chunks:    wChunks,
		StartedAt: build.CreatedAt,
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

func (s *VanadisServer) syncBuildAndNotifyMutation(ctx context.Context, buildID, action string) {
	s.syncBuildWidget(ctx, buildID, "")
	if err := s.notifyRestateDAGMutation(ctx, buildID, action); err != nil {
		log.Printf("[vanadis] warning: failed signaling restate dag mutation on %s for build %s: %v", action, buildID, err)
	}
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
	s.syncBuildAndNotifyMutation(r.Context(), buildID, "update_chunk")
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
	s.syncBuildAndNotifyMutation(r.Context(), buildID, "append_chunks")
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "appended", "build_id": buildID, "chunks": appended})
}

func (s *VanadisServer) handleRemoveChunk(w http.ResponseWriter, r *http.Request, buildID, chunkRef string) {
	removed, remaining, err := s.store.RemoveChunk(r.Context(), buildID, chunkRef)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.syncBuildAndNotifyMutation(r.Context(), buildID, "remove_chunk")
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":           "removed",
		"build_id":         buildID,
		"removed_chunk_id": removed.ID,
		"chunks":           remaining,
	})
}

// ReserveChunkForLaunch transitions a pending or enqueued chunk to running, recording started_at if not already set.
// It preserves later states such as pr_opened, coding, deploying, and deploy_failed, along with terminal statuses.
func (s *PostgresVanadisStore) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk id cannot be empty")
	}
	const q = `UPDATE vanadis_chunks SET status = CASE WHEN status IN ('pending', 'queued') THEN 'running' ELSE status END, started_at = CASE WHEN status IN ('pending', 'queued') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	tag, err := s.db.Exec(ctx, q, chunkID)
	if err != nil {
		return fmt.Errorf("reserve chunk %s for launch: %w", chunkID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	return nil
}
