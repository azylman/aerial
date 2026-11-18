package store

// PR lifecycle persistence for vanadis-restate.
//
// CompletePRDeployment, UpdatePRLifecycleStatus, RecordPRCheckStatus,
// ResetPRCheckState and IncrementChunkCIFailCount moved here from the root
// package so they promote through the embedded *Store. SQL text, check-state
// JSON, status transition rules and error behavior are byte-identical to the
// root copies they replace.
//
// Helper copies below (prHeadVerifiedFromContext, recoverFailedPR,
// failedPRHeadAccepted, missingFailedPRBaseline, seedFailedPRBaseline,
// lockRetryBuildAndChunks) mirror the frozen root router.go,
// store_pr_recovery.go and store_action_retry.go copies byte-for-byte;
// ResetPRCheckState depends on the recovery fallback, which is its only
// caller, and the lock helper it needs. Later chunks moving the recovery and
// retry modules drop their root copies of these helpers.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// PR lifecycle status vocabulary mirrored from the root package.
// String-compared only, so these local copies preserve behavior exactly;
// the facade-removal chunk unifies them.
const (
	statusPROpened = "pr_opened"
	statusCIFailed = "ci_failed"
	statusConflict = "conflict"
)

func (s *Store) CompletePRDeployment(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk_id cannot be empty")
	}
	var evidence any
	if rolloutState != nil {
		data, err := json.Marshal(rolloutState)
		if err != nil {
			return fmt.Errorf("marshal rollout state: %w", err)
		}
		evidence = data
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks AS c SET status = 'completed', banner = '', rollout_state = COALESCE($1::jsonb, c.rollout_state), completed_at = COALESCE(c.completed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds AS b WHERE c.id = $2 AND b.id = c.build_id AND (c.mode = 'pr' OR (c.mode = 'action' AND COALESCE(c.pr_number, 0) > 0)) AND c.status NOT IN ('failed', 'cancelled') AND b.status != 'cancelled' AND (b.status != 'failed' OR c.status = 'completed');`, evidence, chunkID)
	if err != nil {
		return fmt.Errorf("complete pr deployment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPRTransitionSkipped
	}
	return nil
}

// UpdatePRLifecycleStatus prevents delayed pre-deployment events from reopening
// a terminal chunk. Notifications follow only an applied write.
func (s *Store) UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	if strings.TrimSpace(chunkID) == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = $1, completed_at = CASE WHEN $1 = 'merged' THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status NOT IN ('completed', 'failed', 'cancelled');`, status, strings.TrimSpace(chunkID))
	if err != nil {
		return fmt.Errorf("update pr lifecycle status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPRTransitionSkipped
	}
	return nil
}

func (s *Store) RecordPRCheckStatus(ctx context.Context, chunkID, headSHA, checkName, checkState string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	name := strings.TrimSpace(checkName)
	sha := strings.TrimSpace(headSHA)
	if id == "" || name == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	const q = `UPDATE vanadis_chunks
SET ci_checks = jsonb_set(
        CASE
            WHEN (status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND $4 != '' AND COALESCE(ci_checks->>'_merge_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb) - '_head_sha', '{_merge_sha}', to_jsonb($4::text), true)
            WHEN status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND $4 != '' AND COALESCE(ci_checks->>'_head_sha', '') = ''
                THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_head_sha}', to_jsonb($4::text), true)
            ELSE COALESCE(ci_checks, '{}'::jsonb)
        END,
        ARRAY[$1],
        to_jsonb($2::text),
        true
    ),
    status = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN 'pr_opened'
        ELSE status
    END,
    banner = CASE
        WHEN status = 'ci_failed' AND $2 = 'passed' AND NOT EXISTS (
            SELECT 1 FROM jsonb_each_text(jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true))
            WHERE key NOT LIKE '\_%' AND value = 'failed'
        ) THEN ''
        ELSE banner
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $3
  AND status NOT IN ('deploying', 'deployed', 'completed', 'failed', 'cancelled')
  AND (
      $4 = ''
      OR ((status IN ('merged', 'synced', 'deploy_failed') OR COALESCE(ci_checks->>'_merge_sha', '') != '') AND (
          COALESCE(ci_checks->>'_merge_sha', '') = $4
          OR (COALESCE(ci_checks->>'_merge_sha', '') = '' AND COALESCE(ci_checks->>'_head_sha', '') IS DISTINCT FROM $4)
      ))
      OR (status NOT IN ('merged', 'synced', 'deploy_failed') AND COALESCE(ci_checks->>'_merge_sha', '') = '' AND (
          COALESCE(ci_checks->>'_head_sha', '') = ''
          OR ci_checks->>'_head_sha' = $4
      ))
  );`
	tag, err := s.db.Exec(ctx, q, name, strings.TrimSpace(checkState), id, sha)
	switch {
	case err != nil:
		return fmt.Errorf("record pr check status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrPRTransitionSkipped
	default:
		return nil
	}
}

func (s *Store) ResetPRCheckState(ctx context.Context, chunkID, headSHA, mergeSHA string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	cleanHead := strings.TrimSpace(headSHA)
	cleanMerge := strings.TrimSpace(mergeSHA)
	if cleanMerge != "" {
		const mergeQ = `UPDATE vanadis_chunks
SET ci_checks = CASE
        WHEN COALESCE(ci_checks->>'_merge_sha', '') = $3
            THEN jsonb_strip_nulls(COALESCE(ci_checks, '{}'::jsonb) || jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
        ELSE jsonb_strip_nulls(jsonb_build_object('_head_sha', COALESCE(NULLIF($2::text, ''), NULLIF(ci_checks->>'_head_sha', '')), '_merge_sha', NULLIF($3::text, '')))
    END,
    banner = '',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled');`
		tag, err := s.db.Exec(ctx, mergeQ, id, cleanHead, cleanMerge)
		if err != nil {
			return fmt.Errorf("reset merge check state: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrPRTransitionSkipped
		}
		return nil
	}
	const syncQ = `UPDATE vanadis_chunks AS c
SET ci_checks = CASE
        WHEN $2 != '' AND LOWER(COALESCE(c.ci_checks->>'_head_sha', '')) = LOWER($2) THEN c.ci_checks - '_merge_sha'
        WHEN $2 != '' THEN jsonb_build_object('_head_sha', $2::text)
        ELSE '{}'::jsonb
    END,
    banner = '',
    status = CASE WHEN c.status IN ('ci_failed', 'conflict') THEN 'pr_opened' ELSE c.status END,
    updated_at = CURRENT_TIMESTAMP
FROM vanadis_builds AS b
WHERE c.id = $1
  AND b.id = c.build_id
  AND c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
  AND COALESCE(c.ci_checks->>'_merge_sha', '') = ''
  AND b.status NOT IN ('completed', 'failed', 'cancelled')
  AND ($3::boolean OR $2 = '' OR COALESCE(c.ci_checks->>'_head_sha', '') = '' OR LOWER(c.ci_checks->>'_head_sha') = LOWER($2));`
	tag, err := s.db.Exec(ctx, syncQ, id, cleanHead, PrHeadVerifiedFromContext(ctx))
	if err != nil {
		return fmt.Errorf("reset pr check state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return s.recoverFailedPR(ctx, id, cleanHead)
	}
	return nil
}

func (s *Store) IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return 0, errors.New("chunk_id cannot be empty")
	}
	var cleanSHA string
	if len(headSHA) > 0 {
		cleanSHA = strings.TrimSpace(headSHA[0])
	}
	const q = `UPDATE vanadis_chunks
SET ci_fail_count = ci_fail_count + 1,
    status = 'ci_failed',
    banner = $1,
    ci_checks = CASE WHEN $3 != '' THEN jsonb_set(COALESCE(ci_checks, '{}'::jsonb), '{_remediated_sha}', to_jsonb($3::text), true) ELSE ci_checks END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $2
  AND status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed', 'failed', 'cancelled')
RETURNING ci_fail_count;`
	var failCount int
	err := s.db.QueryRow(ctx, q, strings.TrimSpace(banner), id, cleanSHA).Scan(&failCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrPRTransitionSkipped
	}
	if err != nil {
		return 0, fmt.Errorf("increment chunk ci fail count: %w", err)
	}
	return failCount, nil
}

// PrHeadVerifiedContextKey carries the router-set verified-head marker across
// the package boundary. It is defined here so the moved ResetPRCheckState and
// recoverFailedPR read the same key the root producer writes; the root
// package aliases it to keep existing references compiling unchanged.
type PrHeadVerifiedContextKey struct{}

// PrHeadVerifiedFromContext reports whether ctx carries the verified-head marker.
func PrHeadVerifiedFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	verified, ok := ctx.Value(PrHeadVerifiedContextKey{}).(bool)
	return ok && verified
}

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

// lockRetryBuildAndChunks follows the parent-first ordering used by existing mutations.
func lockRetryBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) (model.VanadisBuild, []model.VanadisChunk, error) {
	b, err := ScanBuild(tx.QueryRow(ctx, `SELECT `+BuildColumns+` FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil, ErrBuildNotFound
	}
	if err != nil {
		return b, nil, err
	}
	rows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return b, nil, err
	}
	defer rows.Close()
	chunks, err := ScanChunks(rows)
	return b, chunks, err
}
