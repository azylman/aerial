package main

import (
	"context"
	"errors"
	"log"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// recoverFailedPR rechecks recovery and subsequent head updates under the same parent-first locks
// as cancellation and action retry. A sibling failure keeps the parent failed.
func (s *PostgresVanadisStore) recoverFailedPR(ctx context.Context, id, head string, at int64) error {
	if strings.TrimSpace(head) == "" {
		return errPRTransitionSkipped
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] PR recovery rollback: %v", rbErr)
		}
	}()
	var buildID string
	if err = tx.QueryRow(ctx, `SELECT build_id FROM vanadis_chunks WHERE id = $1;`, id).Scan(&buildID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errPRTransitionSkipped
		}
		return err
	}
	b, chunks, err := lockRetryBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return err
	}
	if !slices.Contains([]string{statusRunning, statusFailed}, b.Status) {
		return errPRTransitionSkipped
	}
	var selected *VanadisChunk
	reopen := true
	for i := range chunks {
		c := &chunks[i]
		if c.ID == id {
			selected = c
		} else if c.Status == statusFailed || c.Status == statusCancelled {
			reopen = false
		}
	}
	if selected == nil {
		return errPRTransitionSkipped
	}
	c := *selected
	// A legacy failed PR needs a durable baseline before a genuine head change
	// can recover it. This observation must preserve all failure state.
	if missingFailedPRBaseline(c, prHeadVerifiedFromContext(ctx)) {
		return seedFailedPRBaseline(ctx, tx, id, head, at)
	}
	if !failedPRHeadAccepted(c, head, prHeadVerifiedFromContext(ctx)) {
		return errPRTransitionSkipped
	}

	const reset = `UPDATE vanadis_chunks SET status = 'pr_opened', ci_fail_count = 0,
 ci_checks = jsonb_strip_nulls(jsonb_build_object('_head_sha', $2::text,
 '_head_at', CASE WHEN $3::bigint > 0 THEN $3::text ELSE NULL END,
 '_seen_heads', CONCAT_WS(',', NULLIF(ci_checks->>'_seen_heads', ''), NULLIF(ci_checks->>'_head_sha', ''), $2::text))),
 banner = '', completed_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err = tx.Exec(ctx, reset, id, head, at); err != nil {
		return err
	}
	// Builds have no terminal timestamp/banner columns; those belong to chunks.
	if b.Status == statusFailed && reopen {
		if _, err = tx.Exec(ctx, `UPDATE vanadis_builds SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, buildID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func failedPRHeadAccepted(c VanadisChunk, head string, verified bool) bool {
	// A recovered PR must keep tracking new heads while failed siblings block
	// the parent. Post-merge, completed, and cancelled chunks remain untouched.
	switch c.Status {
	case statusFailed, statusPROpened, statusCIFailed, statusConflict:
	default:
		return false
	}

	if !verified || strings.TrimSpace(c.MergeSHA) != "" || c.Mode != "pr" || c.PRNumber <= 0 || c.HeadSHA == "" || strings.EqualFold(c.HeadSHA, head) {
		return false
	}
	return true
}

func missingFailedPRBaseline(c VanadisChunk, verified bool) bool {
	return c.Status == statusFailed && c.HeadSHA == "" && c.Mode == "pr" && c.PRNumber > 0 && strings.TrimSpace(c.MergeSHA) == "" && verified
}

func seedFailedPRBaseline(ctx context.Context, tx pgx.Tx, id, head string, at int64) error {
	const baseline = `UPDATE vanadis_chunks SET ci_checks = COALESCE(ci_checks, '{}'::jsonb) ||
 jsonb_strip_nulls(jsonb_build_object('_head_sha', $2::text,
 '_head_at', CASE WHEN $3::bigint > 0 THEN $3::text ELSE NULL END,
 '_seen_heads', CONCAT_WS(',', NULLIF(ci_checks->>'_seen_heads', ''), $2::text))) WHERE id = $1;`
	if _, err := tx.Exec(ctx, baseline, id, head, at); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return errPRTransitionSkipped
}
