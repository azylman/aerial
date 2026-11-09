package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
)

// StalledChunkVerdict is the outcome of claiming a possibly stalled PR-mode chunk.
type StalledChunkVerdict string

const (
	// StalledChunkVerdictNone means the chunk is not stalled (PR linked, failed, not a PR chunk, or build stopped).
	StalledChunkVerdictNone StalledChunkVerdict = "none"
	// StalledChunkVerdictNudge means the chunk stalled for the first time and must be re-dispatched once.
	StalledChunkVerdictNudge StalledChunkVerdict = "nudge"
	// StalledChunkVerdictFail means the chunk stalled again after its single re-dispatch and must be failed.
	StalledChunkVerdictFail StalledChunkVerdict = "fail"
)

const stalledChunkNoticeFmt = "[VANADIS CHUNK STALLED]\nYour previous turn for chunk %s ended without a successful `aerial-pr.sh submit` and without calling the vanadis MCP tool fail_chunk, so no PR was linked. This is the single stall retry: reuse the existing scratch workspace from your prior attempt if it still exists instead of re-initializing, finish the remaining implementation and verification in the foreground, then submit with `--vanadis-chunk %s` or call fail_chunk with the blocker. If this turn also ends without either, the chunk and build will be failed.\n\n"

const stalledChunkFailReason = "chunk stalled twice: its turn ended without submitting a PR or calling fail_chunk, both on the original dispatch and on the stall re-dispatch"

// ProcessStalledChunkCheck runs synchronously when a PR-mode chunk's turn ends. aerial-pr.sh submit links the PR to
// the chunk synchronously (via synapse's PR registration) before the turn can end, so a chunk still running with no
// PR at this point never submitted. It is re-dispatched once with a stall notice, or failed (marking the build
// failed and waking the thread) if it has already been nudged.
func (s *VanadisServer) ProcessStalledChunkCheck(ctx context.Context, chunkID string) error {
	if s == nil || s.store == nil {
		return errors.New("vanadis store not configured")
	}
	verdict, build, chunk, err := s.store.ClaimStalledChunk(ctx, chunkID)
	if errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildNotFound) {
		log.Printf("[vanadis] stalled check: chunk %s no longer exists, skipping", chunkID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim stalled chunk %s: %w", chunkID, err)
	}
	switch verdict {
	case StalledChunkVerdictNudge:
		RecordChunkTotal("stalled")
		if dErr := s.redispatchStalledChunk(ctx, build.ID, chunk.ID); dErr != nil {
			log.Printf("[vanadis] stalled check: re-dispatch of chunk %s failed: %v", chunk.ID, dErr)
			_, _, _, fErr := s.failChunkInternal(ctx, chunk.ID, fmt.Sprintf("chunk stalled (turn ended without submitting a PR or calling fail_chunk) and the stall re-dispatch failed: %v", dErr))
			return fErr
		}
		log.Printf("[vanadis] stalled check: chunk %s re-dispatched with stall notice", chunk.ID)
		return nil
	case StalledChunkVerdictFail:
		_, _, _, fErr := s.failChunkInternal(ctx, chunk.ID, stalledChunkFailReason)
		return fErr
	default:
		return nil
	}
}

func (s *VanadisServer) redispatchStalledChunk(ctx context.Context, buildID, chunkID string) error {
	build, chunks, err := s.store.GetBuild(ctx, buildID)
	if err != nil {
		return fmt.Errorf("get build %s: %w", buildID, err)
	}
	for i := range chunks {
		if chunks[i].ID == chunkID {
			return s.dispatchChunkTurn(ctx, *build, chunks, i, fmt.Sprintf(stalledChunkNoticeFmt, chunkID, chunkID))
		}
	}
	return fmt.Errorf("chunk %s not found in build %s", chunkID, buildID)
}

// ClaimStalledChunk atomically judges whether a PR-mode chunk stalled (still running with no linked PR in a running
// build) and, on its first stall, records the nudge so a second stall yields StalledChunkVerdictFail.
func (s *PostgresVanadisStore) ClaimStalledChunk(ctx context.Context, chunkID string) (StalledChunkVerdict, *VanadisBuild, *VanadisChunk, error) {
	if s == nil || s.db == nil {
		return "", nil, nil, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return "", nil, nil, errors.New("chunk id cannot be empty")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("begin claim stalled chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] claim stalled chunk tx rollback error: %v", rbErr)
		}
	}()

	const selectSQL = `
		SELECT c.id, c.build_id, c.chunk_index, c.mode, c.title, c.status, COALESCE(c.pr_number, 0), c.stall_nudges,
		       b.id, b.thread_id, b.objective, b.status
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.id = $1
		FOR UPDATE`
	var c VanadisChunk
	var b VanadisBuild
	var nudges int
	err = tx.QueryRow(ctx, selectSQL, chunkID).Scan(&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Title, &c.Status, &c.PRNumber, &nudges, &b.ID, &b.ThreadID, &b.Objective, &b.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil, ErrChunkNotFound
	}
	if err != nil {
		return "", nil, nil, fmt.Errorf("lock stalled chunk %s: %w", chunkID, err)
	}

	verdict := judgeStalledChunk(c, b.Status, nudges)
	if verdict != StalledChunkVerdictNudge {
		return verdict, &b, &c, nil
	}
	const nudgeSQL = `UPDATE vanadis_chunks SET stall_nudges = stall_nudges + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $1`
	if _, err := tx.Exec(ctx, nudgeSQL, c.ID); err != nil {
		return "", nil, nil, fmt.Errorf("record stall nudge for chunk %s: %w", c.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", nil, nil, fmt.Errorf("commit claim stalled chunk tx: %w", err)
	}
	return verdict, &b, &c, nil
}

// judgeStalledChunk is the pure stall decision shared by every store implementation.
func judgeStalledChunk(c VanadisChunk, buildStatus string, nudges int) StalledChunkVerdict {
	if c.Mode != "pr" || c.Status != "running" || c.PRNumber != 0 || buildStatus != "running" {
		return StalledChunkVerdictNone
	}
	if nudges == 0 {
		return StalledChunkVerdictNudge
	}
	return StalledChunkVerdictFail
}
