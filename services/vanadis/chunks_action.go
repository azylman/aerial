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
	case "action":
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

// CompleteActionChunk transitions a running action chunk to completed and advances the build.
func (s *PostgresVanadisStore) CompleteActionChunk(ctx context.Context, chunkID string) (*VanadisAdvancementResult, error) {
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

	var c VanadisChunk
	const selectChunkSQL = `SELECT id, build_id, chunk_index, mode, title, status FROM vanadis_chunks WHERE id = $1 FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectChunkSQL, chunkID).Scan(&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Title, &c.Status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("lock action chunk %s: %w", chunkID, err)
	}

	var b VanadisBuild
	const selectBuildSQL = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, status, COALESCE(status_message_id, '') FROM vanadis_builds WHERE id = $1 FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectBuildSQL, c.BuildID).Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.Status, &b.StatusMessageID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock action build %s: %w", c.BuildID, err)
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
		MergedChunkIndex: c.ChunkIndex,
		MergedChunkTitle: c.Title,
		NextChunkIndex:   -1,
		BuildCompleted:   b.Status == "completed",
	}

	if c.Mode != "action" || c.Status != "running" || b.Status != "running" {
		return res, nil
	}

	const updateChunkSQL = `UPDATE vanadis_chunks SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, updateChunkSQL, c.ID); err != nil {
		return nil, fmt.Errorf("mark action chunk %s completed: %w", c.ID, err)
	}

	return queryAndAdvanceChunks(ctx, tx, b.ID, c.ID, c.Status, c.ChunkIndex, res)
}

// FailChunk marks a non-terminal chunk and its parent build as failed.
func (s *PostgresVanadisStore) FailChunk(ctx context.Context, chunkID, reason string) (*VanadisBuild, *VanadisChunk, bool, error) {
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

	const selectChunkQuery = `
		SELECT c.id, c.build_id, c.chunk_index, c.mode, c.repo, c.title, c.instructions, c.target_files, c.target_symbols, COALESCE(c.effort, 'medium'), c.acceptance_criteria, c.status, COALESCE(c.pr_number, 0), c.started_at, c.completed_at, c.created_at, c.updated_at,
		       b.id, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, COALESCE(b.shared_context, ''), b.status, COALESCE(b.status_message_id, ''), b.created_at, b.updated_at
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.id = $1
		FOR UPDATE`

	var c VanadisChunk
	var b VanadisBuild
	var startedAt, completedAt sql.NullTime
	err = tx.QueryRow(ctx, selectChunkQuery, chunkID).Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions, &c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status, &c.PRNumber, &startedAt, &completedAt, &c.CreatedAt, &c.UpdatedAt,
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

	if isChunkDoneStatus(c.Status) || c.Status == "failed" || b.Status == "completed" || b.Status == "cancelled" {
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
	c.Status = "failed"
	c.CompletedAt = now
	c.UpdatedAt = now
	b.Status = "failed"
	b.UpdatedAt = now
	return &b, &c, true, nil
}

func formatChunkLedgerLabel(c VanadisChunkSummary) string {
	if c.Mode == "action" {
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
	lines = append(lines, "", "All Vanadis build chunks have completed. Review the work and formulate the final substantive deliverable to the user.")
	return strings.Join(lines, "\n")
}

func isShutdownInterruptionReason(reason string) bool {
	norm := strings.ToLower(strings.TrimSpace(reason))
	return norm == "interrupted by shutdown" || norm == "context cancelled during execution"
}

func (s *VanadisServer) failChunkInternal(ctx context.Context, chunkID, reason string) (*VanadisBuild, *VanadisChunk, bool, error) {
	if s.store == nil {
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

	build, chunk, transitioned, err := s.store.FailChunk(ctx, chunkID, reason)
	if err != nil {
		return nil, nil, false, err
	}

	if transitioned && build != nil && chunk != nil {
		RecordChunkTotal("failed")
		RecordBuildTotal("failed")

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

	return build, chunk, transitioned, nil
}
