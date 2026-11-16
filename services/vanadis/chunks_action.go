package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultRepoOwner = "azylman"

func repoMatchCandidates(repo string) []string {
	repo = normalizeRepo(repo)
	if repo == "" {
		return nil
	}
	if !strings.Contains(repo, "/") {
		return []string{repo, defaultRepoOwner + "/" + repo}
	}
	if bare, ok := strings.CutPrefix(repo, defaultRepoOwner+"/"); ok && bare != "" && !strings.Contains(bare, "/") {
		return []string{repo, bare}
	}
	return []string{repo}
}

func validateChunkModeAndRepo(idx int, modeStr, repo string, targetFiles, targetSymbols []string) error {
	mode, err := normalizeChunkMode(modeStr)
	if err != nil {
		return fmt.Errorf("chunk[%d] %w", idx, err)
	}
	switch mode {
	case "pr":
		if normalizeRepo(repo) == "" {
			return fmt.Errorf("chunk[%d] repo is required for pr mode", idx)
		}
	case chunkModeAction:
		if strings.TrimSpace(repo) != "" {
			return fmt.Errorf("chunk[%d] action chunk must have empty repo", idx)
		}
		if len(targetFiles) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_files", idx)
		}
		if len(targetSymbols) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_symbols", idx)
		}
	}
	return nil
}

// actionDispatchID identifies the persisted action dispatch generation.
func actionDispatchID(chunk VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

func actionDispatchMatches(chunk VanadisChunk, dispatchID string) bool {
	if dispatchID == "" {
		return chunk.StallNudges == 0
	}
	return dispatchID == actionDispatchID(chunk)
}

func actionCompletionAllowed(chunk VanadisChunk, buildStatus, dispatchID string) bool {
	return chunk.Mode == chunkModeAction && chunk.Status == statusRunning && buildStatus == statusRunning && chunk.PRNumber == 0 && actionDispatchMatches(chunk, dispatchID)
}

// CompleteActionChunk transitions a running action chunk to completed and returns advancement info.
func (s *PostgresVanadisStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*VanadisAdvancementResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, errors.New("chunk id cannot be empty")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin complete action chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] complete action chunk tx rollback error: %v", rbErr)
		}
	}()

	// Lock the parent first, matching cancellation and retry lock order.
	var b VanadisBuild
	const selectBuildSQL = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, status, COALESCE(status_message_id, '') FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks WHERE id = $1) FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectBuildSQL, chunkID).Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.Status, &b.StatusMessageID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("lock action build: %w", err)
	}
	var cID, bID, cTitle, cStatus, cMode string
	var chunkIdx, prNumber, stallNudges int
	const selectChunkSQL = `SELECT id, build_id, chunk_index, mode, title, status, COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks WHERE id = $1 AND build_id = $2 FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectChunkSQL, chunkID, b.ID).Scan(&cID, &bID, &chunkIdx, &cMode, &cTitle, &cStatus, &prNumber, &stallNudges); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("lock action chunk %s: %w", chunkID, err)
	}

	res := &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         false,
		BuildID:          b.ID,
		ThreadID:         b.ThreadID,
		Tier:             b.Tier,
		Title:            b.Title,
		Objective:        b.Objective,
		BuildStatus:      b.Status,
		StatusMessageID:  b.StatusMessageID,
		MergedChunkIndex: chunkIdx,
		MergedChunkTitle: cTitle,
		ChunkMode:        cMode,
		NextChunkIndex:   -1,
		BuildCompleted:   b.Status == statusCompleted,
	}

	if !actionCompletionAllowed(VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}, b.Status, dispatchID) {
		return res, nil
	}

	const updateChunkSQL = `UPDATE vanadis_chunks SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, updateChunkSQL, cID); err != nil {
		return nil, fmt.Errorf("mark action chunk %s completed: %w", cID, err)
	}

	// If this chunk completed the build (all chunks done), mark the build completed.
	var remainingCount int
	const checkRemainingSQL = `SELECT COUNT(*) FROM vanadis_chunks WHERE build_id = $1 AND id != $2 AND status != 'completed';`
	if err := tx.QueryRow(ctx, checkRemainingSQL, bID, cID).Scan(&remainingCount); err != nil {
		return nil, fmt.Errorf("count remaining chunks: %w", err)
	}
	if remainingCount == 0 {
		const updateBuildSQL = `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
		if _, err := tx.Exec(ctx, updateBuildSQL, bID); err != nil {
			return nil, fmt.Errorf("complete action build: %w", err)
		}
		res.BuildCompleted = true
		res.BuildStatus = statusCompleted
		res.Chunks = queryBuildChunkSummaries(ctx, tx, bID)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit complete action chunk tx: %w", err)
	}

	res.Advanced = true
	return res, nil
}

func queryBuildChunkSummaries(ctx context.Context, tx pgx.Tx, buildID string) []VanadisChunkSummary {
	const q = `SELECT id, chunk_index, mode, repo, title, status, COALESCE(pr_number, 0), started_at, completed_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := tx.Query(ctx, q, buildID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var summaries []VanadisChunkSummary
	for rows.Next() {
		var s VanadisChunkSummary
		var startedAt, completedAt sql.NullTime
		if err := rows.Scan(&s.ID, &s.ChunkIndex, &s.Mode, &s.Repo, &s.Title, &s.Status, &s.PRNumber, &startedAt, &completedAt, &s.UpdatedAt); err != nil {
			return nil
		}
		if startedAt.Valid {
			s.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			s.CompletedAt = completedAt.Time
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil
	}
	return summaries
}

// Non-action failures retain their existing identity-free contract.
func failureDispatchMatches(chunk VanadisChunk, dispatchID string) bool {
	if chunk.Mode != chunkModeAction {
		return true
	}
	// Attaching a PR retains action mode, but PR remediation uses CI/conflict
	// dispatch IDs. Preserve that lifecycle's failure contract without allowing
	// an older action-turn ID to bypass the generation fence.
	if chunk.PRNumber > 0 && prFailureDispatchMatches(chunk.ID, dispatchID) {
		return true
	}
	return actionDispatchMatches(chunk, dispatchID)
}

func prFailureDispatchMatches(chunkID, dispatchID string) bool {
	conflictID := conflictRemediationDispatchID(chunkID, "")
	return dispatchID == "" ||
		strings.HasPrefix(dispatchID, "vanadis-"+chunkID+"-cifail-") ||
		dispatchID == conflictID || strings.HasPrefix(dispatchID, conflictID+"-")
}

func lockFailureBuild(ctx context.Context, tx pgx.Tx, chunkID string) (string, error) {
	var buildID string
	err := tx.QueryRow(ctx, `SELECT id FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks WHERE id = $1) FOR UPDATE`, chunkID).Scan(&buildID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrChunkNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock failure build: %w", err)
	}
	return buildID, nil
}

// FailChunk marks a non-terminal chunk and its parent build as failed.
func (s *PostgresVanadisStore) FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*VanadisBuild, *VanadisChunk, bool, error) {
	if s == nil || s.db == nil {
		return nil, nil, false, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, nil, false, errors.New("chunk id cannot be empty")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, nil, false, fmt.Errorf("begin fail chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] fail chunk tx rollback error: %v", rbErr)
		}
	}()

	// Lock the parent before the chunk, matching completion and cancellation.
	lockedBuildID, err := lockFailureBuild(ctx, tx, chunkID)
	if err != nil {
		return nil, nil, false, err
	}

	const selectChunkQuery = `
		SELECT c.id, c.build_id, c.chunk_index, c.mode, c.repo, c.title, c.instructions, c.target_files, c.target_symbols, COALESCE(c.effort, 'medium'), c.acceptance_criteria, c.status, COALESCE(c.pr_number, 0), c.started_at, c.completed_at, c.created_at, c.updated_at, c.stall_nudges,
		       b.id, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, COALESCE(b.shared_context, ''), b.status, COALESCE(b.status_message_id, ''), b.created_at, b.updated_at
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.id = $1 AND c.build_id = $2
		FOR UPDATE OF c`

	var c VanadisChunk
	var b VanadisBuild
	var startedAt, completedAt sql.NullTime
	err = tx.QueryRow(ctx, selectChunkQuery, chunkID, lockedBuildID).Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions, &c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status, &c.PRNumber, &startedAt, &completedAt, &c.CreatedAt, &c.UpdatedAt, &c.StallNudges,
		&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, false, ErrChunkNotFound
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("select chunk %s for fail: %w", chunkID, err)
	}
	if startedAt.Valid {
		c.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		c.CompletedAt = completedAt.Time
	}
	c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = nonNilSlice(c.TargetFiles), nonNilSlice(c.TargetSymbols), nonNilSlice(c.AcceptanceCriteria)
	c.Effort = capChunkEffort(c.Effort)

	if !failureDispatchMatches(c, dispatchID) {
		return &b, &c, false, nil
	}

	if isChunkMergedOrDoneStatus(c.Status) || c.Status == statusFailed || b.Status == statusCompleted || b.Status == statusCancelled {
		return &b, &c, false, nil
	}

	const updateChunkQuery = `UPDATE vanadis_chunks SET status = 'failed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(ctx, updateChunkQuery, chunkID); err != nil {
		return nil, nil, false, fmt.Errorf("update chunk failed: %w", err)
	}
	const updateBuildQuery = `UPDATE vanadis_builds SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(ctx, updateBuildQuery, b.ID); err != nil {
		return nil, nil, false, fmt.Errorf("update build failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, false, fmt.Errorf("commit fail chunk tx: %w", err)
	}

	now := time.Now().UTC()
	c.Status = statusFailed
	c.CompletedAt = now
	c.UpdatedAt = now
	b.Status = statusFailed
	b.UpdatedAt = now
	return &b, &c, true, nil
}

func formatChunkLedgerLabel(c VanadisChunkSummary) string {
	if c.Mode == chunkModeAction {
		return "(action, completed)"
	}
	cRepo := strings.TrimSpace(c.Repo)
	if cRepo == "" {
		cRepo = "azylman/aerial"
	}
	if c.PRNumber > 0 {
		return fmt.Sprintf("(%s, PR #%d)", cRepo, c.PRNumber)
	}
	return fmt.Sprintf("(%s)", cRepo)
}

func formatBuildCompletedWakePrompt(res *VanadisAdvancementResult) string {
	if res == nil {
		return ""
	}
	total := len(res.Chunks)
	lines := []string{
		fmt.Sprintf("Vanadis build %s completed (Tier %d, %d chunks completed).", res.BuildID, res.Tier, total),
	}
	if obj := strings.TrimSpace(res.Objective); obj != "" {
		lines = append(lines, "Objective: "+obj)
	}
	if total > 0 {
		lines = append(lines, "", "Completed Chunks Ledger:")
		for _, c := range res.Chunks {
			lines = append(lines, fmt.Sprintf("- Chunk %d/%d: %s %s", c.ChunkIndex+1, total, c.Title, formatChunkLedgerLabel(c)))
		}
	}
	lines = append(lines, "", "Directive: Concisely summarize the completed build and deployment, and either suggest the next steps from the discussion or explicitly call out that all work is completed.")
	return strings.Join(lines, "\n")
}

func isShutdownInterruptionReason(reason string) bool {
	norm := strings.ToLower(strings.TrimSpace(reason))
	return norm == "interrupted by shutdown" || norm == "context cancelled during execution"
}

func (s *VanadisServer) handleChunkFailureTransition(ctx context.Context, build *VanadisBuild, chunk *VanadisChunk, reason string) {
	RecordChunkTotal("failed")
	RecordBuildTotal("failed")
	s.publishExternalEvent(ctx, "vanadis_chunk", "failed", []string{chunk.ID}, reason)
	s.publishExternalEvent(ctx, "vanadis_build", "failed", []string{build.ID}, reason)
	if s.restateCfg.RestateIngressURL != "" {
		if rErr := s.reconcileBuildOnRestate(ctx, build.ID, "chunk_failed"); rErr != nil {
			log.Printf("[vanadis] warning: failed reconciling build %s on chunk failure: %v", build.ID, rErr)
		}
	}

	totalChunks := chunk.ChunkIndex + 1
	if _, allChunks, bErr := s.store.GetBuild(ctx, build.ID); bErr == nil && len(allChunks) > 0 {
		totalChunks = len(allChunks)
	}

	banner := fmt.Sprintf("Chunk %d/%d (%s) failed: %s", chunk.ChunkIndex+1, totalChunks, chunk.Title, reason)
	s.syncBuildWidget(ctx, build.ID, banner)

	if strings.TrimSpace(build.ThreadID) != "" {
		prompt := fmt.Sprintf("[VANADIS BUILD FAILED]\nBuild ID: %s\nObjective: %s\n\nChunk %d/%d (%s) failed: %s", build.ID, build.Objective, chunk.ChunkIndex+1, totalChunks, chunk.Title, reason)
		if err := s.dispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
			log.Printf("[vanadis] warning: failed dispatching fail wake prompt to thread %s: %v", build.ThreadID, err)
		}
	}
}

func (s *VanadisServer) failChunkInternal(ctx context.Context, chunkID, reason, dispatchID string) (*VanadisBuild, *VanadisChunk, bool, error) {
	if s == nil || s.store == nil {
		return nil, nil, false, errors.New("vanadis store not configured")
	}
	chunkID = strings.TrimSpace(chunkID)
	reason = strings.TrimSpace(reason)
	if chunkID == "" || reason == "" {
		return nil, nil, false, fmt.Errorf("%w: chunk_id and reason are required", ErrInvalidBuildInput)
	}

	if isShutdownInterruptionReason(reason) {
		chunk, build, err := s.store.GetChunk(ctx, chunkID)
		return build, chunk, false, err
	}

	build, chunk, transitioned, err := s.store.FailChunk(ctx, chunkID, reason, dispatchID)
	if err != nil {
		return nil, nil, false, err
	}

	if transitioned && build != nil && chunk != nil {
		s.handleChunkFailureTransition(ctx, build, chunk, reason)
	}

	return build, chunk, transitioned, nil
}

// RetryActionChunkRequest fences a single operator-requested action retry.
type RetryActionChunkRequest struct {
	BuildID            string `json:"build_id"`
	ChunkID            string `json:"chunk_id"`
	ExpectedDispatchID string `json:"expected_dispatch_id"`
	Reason             string `json:"reason"`
}

type RetryActionChunkResult struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

// RetryActionChunk is intentionally concrete-only until durable intake is wired.
func (s *PostgresVanadisStore) RetryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	if strings.TrimSpace(req.BuildID) == "" || strings.TrimSpace(req.ChunkID) == "" || strings.TrimSpace(req.ExpectedDispatchID) == "" || strings.TrimSpace(req.Reason) == "" {
		return nil, fmt.Errorf("%w: build, chunk, expected dispatch and reason are required", ErrInvalidBuildInput)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] retry action rollback: %v", rbErr)
		}
	}()
	b, chunks, err := lockBuildAndChunks(ctx, tx, req.BuildID)
	if err != nil {
		return nil, err
	}
	c, replay, err := retryActionCandidate(b, chunks, req)
	if err != nil {
		return nil, err
	}
	if !replay {
		// PostgreSQL stores stall_nudges as an integer; never wrap a generation.
		if c.StallNudges >= 2147483647 {
			return nil, fmt.Errorf("%w: generation exhausted", ErrBuildConflict)
		}
		const reset = `UPDATE vanadis_chunks SET status = 'pending', stall_nudges = stall_nudges + 1, started_at = NULL, completed_at = NULL, ci_fail_count = 0, ci_checks = '{}'::jsonb, rollout_state = '{}'::jsonb, banner = '', merge_awakeable_id = '', deploy_awakeable_id = '', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
		if _, err := tx.Exec(ctx, reset, c.ID); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE vanadis_builds SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, req.BuildID); err != nil {
			return nil, err
		}
		c.StallNudges++
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &RetryActionChunkResult{BuildID: req.BuildID, ChunkID: c.ID, DispatchID: actionDispatchID(c), Accepted: true}, nil
}

// lockBuildAndChunks follows the parent-first ordering used by existing mutations.
func lockBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) (VanadisBuild, []VanadisChunk, error) {
	b, err := scanBuild(tx.QueryRow(ctx, `SELECT `+buildColumns+` FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil, ErrBuildNotFound
	}
	if err != nil {
		return b, nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE;`, buildID)
	if err != nil {
		return b, nil, err
	}
	defer rows.Close()
	chunks, err := scanChunks(rows)
	return b, chunks, err
}

func retryActionGraphReady(c VanadisChunk, chunks []VanadisChunk) bool {
	if err := checkChunkDependenciesReady(c, chunks); err != nil {
		return false
	}
	descendants := map[string]bool{c.ID: true}
	for changed := true; changed; {
		changed = false
		for _, other := range chunks {
			if descendants[other.ID] {
				continue
			}
			for _, dep := range other.DependsOn {
				if descendants[dep] {
					descendants[other.ID] = true
					changed = true
					break
				}
			}
		}
	}
	for _, other := range chunks {
		if other.ID == c.ID {
			continue
		}
		if other.Status != statusCompleted && other.Status != statusPending {
			return false
		}
		if descendants[other.ID] && other.Status != statusPending {
			return false
		}
	}
	return true
}

func retryActionCandidate(b VanadisBuild, chunks []VanadisChunk, req RetryActionChunkRequest) (VanadisChunk, bool, error) {
	var selected *VanadisChunk
	for i := range chunks {
		if chunks[i].ID == req.ChunkID {
			selected = &chunks[i]
			break
		}
	}
	if selected == nil {
		return VanadisChunk{}, false, ErrChunkNotFound
	}
	c := *selected
	conflict := func() (VanadisChunk, bool, error) {
		return VanadisChunk{}, false, fmt.Errorf("%w: action retry preconditions changed", ErrBuildConflict)
	}
	if c.BuildID != req.BuildID || c.Mode != chunkModeAction || c.PRNumber != 0 || c.StallNudges < 0 {
		return conflict()
	}
	previous := c
	previous.StallNudges--
	replay := c.StallNudges > 0 && req.ExpectedDispatchID == actionDispatchID(previous) && b.Status == statusRunning && (c.Status == statusPending || c.Status == statusRunning)
	if !replay && (b.Status != statusFailed || c.Status != statusFailed || req.ExpectedDispatchID != actionDispatchID(c)) {
		return conflict()
	}
	// Replay acknowledges the committed generation; scheduler progress is not a new retry.
	if !replay && !retryActionGraphReady(c, chunks) {
		return conflict()
	}
	return c, replay, nil
}
