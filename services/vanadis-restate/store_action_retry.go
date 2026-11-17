package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RetryActionChunkRequest fences a single operator-requested action retry.
type RetryActionChunkRequest struct {
	BuildID                 string  `json:"build_id"`
	ChunkID                 string  `json:"chunk_id"`
	ExpectedDispatchID      string  `json:"expected_dispatch_id"`
	Reason                  string  `json:"reason"`
	ExpectedInstructions    *string `json:"expected_instructions,omitempty"    jsonschema:"Exact stored instructions required for correction; supply with replacement_instructions"`
	ReplacementInstructions *string `json:"replacement_instructions,omitempty" jsonschema:"Corrected nonblank worker instructions atomically saved before retry; supply with expected_instructions"`
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
	if !validRetryInstructionPair(req) {
		return nil, fmt.Errorf("%w: expected_instructions and replacement_instructions must be paired and nonblank", ErrInvalidBuildInput)
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
	b, chunks, err := lockRetryBuildAndChunks(ctx, tx, req.BuildID)
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
		const reset = `UPDATE vanadis_chunks SET instructions = COALESCE($2, instructions), status = 'pending', stall_nudges = stall_nudges + 1, started_at = NULL, completed_at = NULL, ci_fail_count = 0, ci_checks = '{}'::jsonb, rollout_state = '{}'::jsonb, banner = '', merge_awakeable_id = '', deploy_awakeable_id = '', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
		if _, err := tx.Exec(ctx, reset, c.ID, req.ReplacementInstructions); err != nil {
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

// lockRetryBuildAndChunks follows the parent-first ordering used by existing mutations.
func lockRetryBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) (VanadisBuild, []VanadisChunk, error) {
	b, err := scanBuild(tx.QueryRow(ctx, `SELECT `+buildColumns+` FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil, ErrBuildNotFound
	}
	if err != nil {
		return b, nil, err
	}
	rows, err := tx.Query(ctx, selectChunksForUpdateSQL, buildID)
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
	if !replay && !retryInstructionsMatch(req, c) {
		return conflict()
	}
	// Replay acknowledges the committed generation; scheduler progress is not a new retry.
	if !replay && !retryActionGraphReady(c, chunks) {
		return conflict()
	}
	return c, replay, nil
}

func validRetryInstructionPair(req RetryActionChunkRequest) bool {
	if req.ExpectedInstructions == nil && req.ReplacementInstructions == nil {
		return true
	}
	return req.ExpectedInstructions != nil && req.ReplacementInstructions != nil && strings.TrimSpace(*req.ExpectedInstructions) != "" && strings.TrimSpace(*req.ReplacementInstructions) != ""
}

func retryInstructionsMatch(req RetryActionChunkRequest, c VanadisChunk) bool {
	return req.ExpectedInstructions == nil || c.Instructions == *req.ExpectedInstructions
}
