package store

// Action-retry persistence for vanadis-restate.
//
// RetryActionChunk plus retryActionGraphReady, retryActionCandidate,
// validRetryInstructionPair and retryInstructionsMatch moved here from the
// root package so they promote through the embedded *Store. SQL text, retry
// graph rules, transaction locking and instruction matching are byte-identical
// to the root copies they replace.
//
// Dependency-readiness checks delegate to the canonical internal/dag
// helpers; retry-instruction pairing stays authoritative here in internal/store.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// RetryActionChunk is intentionally concrete-only until durable intake is wired.
func (s *Store) RetryActionChunk(ctx context.Context, req RetryActionChunkRequest) (*RetryActionChunkResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	if strings.TrimSpace(req.BuildID) == "" || strings.TrimSpace(req.ChunkID) == "" || strings.TrimSpace(req.ExpectedDispatchID) == "" || strings.TrimSpace(req.Reason) == "" {
		return nil, fmt.Errorf("%w: build, chunk, expected dispatch and reason are required", ErrInvalidBuildInput)
	}
	if !ValidRetryInstructionPair(req) {
		return nil, fmt.Errorf("%w: expected_instructions and replacement_instructions must be paired and nonblank", ErrInvalidBuildInput)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "[vanadis] retry action rollback failed", "error", rbErr)
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
		const reset = `UPDATE vanadis_chunks SET instructions = COALESCE($2, instructions), status = 'pending', stall_nudges = stall_nudges + 1, repo = '', pr_number = NULL, started_at = NULL, completed_at = NULL, ci_fail_count = 0, ci_checks = '{}'::jsonb, rollout_state = '{}'::jsonb, banner = '', merge_awakeable_id = '', deploy_awakeable_id = '', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
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
	return &RetryActionChunkResult{BuildID: req.BuildID, ChunkID: c.ID, DispatchID: model.ActionDispatchID(c), Accepted: true}, nil
}

func retryActionGraphReady(c model.VanadisChunk, chunks []model.VanadisChunk) bool {
	if err := buildplan.CheckChunkDependenciesReady(c, chunks); err != nil {
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
	// Siblings outside the chunk's descendants may keep running: a failed
	// chunk blocks only its dependents.
	for _, other := range chunks {
		if other.ID != c.ID && descendants[other.ID] && other.Status != statusPending {
			return false
		}
	}
	return true
}

func retryActionCandidate(b model.VanadisBuild, chunks []model.VanadisChunk, req RetryActionChunkRequest) (model.VanadisChunk, bool, error) {
	var selected *model.VanadisChunk
	for i := range chunks {
		if chunks[i].ID == req.ChunkID {
			selected = &chunks[i]
			break
		}
	}
	if selected == nil {
		return model.VanadisChunk{}, false, ErrChunkNotFound
	}
	c := *selected
	conflict := func() (model.VanadisChunk, bool, error) {
		return model.VanadisChunk{}, false, fmt.Errorf("%w: action retry preconditions changed", ErrBuildConflict)
	}
	if c.BuildID != req.BuildID || c.Mode != chunkModeAction || c.StallNudges < 0 {
		return conflict()
	}
	previous := c
	previous.StallNudges--
	replay := c.StallNudges > 0 && req.ExpectedDispatchID == model.ActionDispatchID(previous) && buildActive(b.Status) && (c.Status == statusPending || c.Status == statusRunning)
	if !replay && (!buildActive(b.Status) || c.Status != statusFailed || req.ExpectedDispatchID != model.ActionDispatchID(c)) {
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

// ValidRetryInstructionPair reports whether a retry request carries a paired
// nonblank expected/replacement instruction update. Single authoritative copy
// consumed by the build package.
func ValidRetryInstructionPair(req RetryActionChunkRequest) bool {
	if req.ExpectedInstructions == nil && req.ReplacementInstructions == nil {
		return true
	}
	return req.ExpectedInstructions != nil && req.ReplacementInstructions != nil && strings.TrimSpace(*req.ExpectedInstructions) != "" && strings.TrimSpace(*req.ReplacementInstructions) != ""
}

func retryInstructionsMatch(req RetryActionChunkRequest, c model.VanadisChunk) bool {
	return req.ExpectedInstructions == nil || c.Instructions == *req.ExpectedInstructions
}
