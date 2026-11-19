package store

// PR recovery persistence for vanadis-restate.
//
// recoverFailedPR and its head-acceptance, baseline-seeding helpers moved
// here from pr_lifecycle.go so each store concern compiles as its own module.
// SQL text, head acceptance rules, baseline seeding, and error behavior are
// byte-identical to the copies they replace. ResetPRCheckState (in
// pr_lifecycle.go) remains the only in-package caller; it shares
// PrHeadVerifiedFromContext and lockRetryBuildAndChunks, which stay in
// pr_lifecycle.go.

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// recoverFailedPR rechecks recovery and subsequent head updates under the same parent-first locks
// as cancellation and action retry. A sibling failure keeps the parent failed.
func (s *Store) recoverFailedPR(ctx context.Context, id, head string) error {
	if strings.TrimSpace(head) == "" {
		return ErrPRTransitionSkipped
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "[vanadis] PR recovery rollback failed", "error", rbErr)
		}
	}()
	var buildID string
	if err = tx.QueryRow(ctx, `SELECT build_id FROM vanadis_chunks WHERE id = $1;`, id).Scan(&buildID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrPRTransitionSkipped
		}
		return err
	}
	b, chunks, err := lockRetryBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return err
	}
	if !slices.Contains([]string{statusRunning, statusFailed}, b.Status) {
		return ErrPRTransitionSkipped
	}
	var selected *model.VanadisChunk
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
		return ErrPRTransitionSkipped
	}
	c := *selected
	// A legacy failed PR needs a durable baseline before a genuine head change
	// can recover it. This observation must preserve all failure state.
	if missingFailedPRBaseline(c, PrHeadVerifiedFromContext(ctx)) {
		return seedFailedPRBaseline(ctx, tx, id, head)
	}
	if !failedPRHeadAccepted(c, head, PrHeadVerifiedFromContext(ctx)) {
		return ErrPRTransitionSkipped
	}

	const reset = `UPDATE vanadis_chunks SET status = 'pr_opened',
 ci_fail_count = CASE WHEN status = 'failed' THEN 0 ELSE ci_fail_count END,
 ci_checks = jsonb_build_object('_head_sha', $2::text),
 banner = '', completed_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err = tx.Exec(ctx, reset, id, head); err != nil {
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

func failedPRHeadAccepted(c model.VanadisChunk, head string, verified bool) bool {
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

func missingFailedPRBaseline(c model.VanadisChunk, verified bool) bool {
	return c.Status == statusFailed && c.HeadSHA == "" && c.Mode == "pr" && c.PRNumber > 0 && strings.TrimSpace(c.MergeSHA) == "" && verified
}

func seedFailedPRBaseline(ctx context.Context, tx pgx.Tx, id, head string) error {
	const baseline = `UPDATE vanadis_chunks SET ci_checks = COALESCE(ci_checks, '{}'::jsonb) ||
 jsonb_build_object('_head_sha', $2::text) WHERE id = $1;`
	if _, err := tx.Exec(ctx, baseline, id, head); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return ErrPRTransitionSkipped
}
