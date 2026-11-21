package store

// Status and action-completion persistence for vanadis-restate.
//
// execUpdateByID, UpdateChunkStatus, UpdateBuildStatus,
// UpdateBuildStatusMessageID, ReserveChunkForLaunch, CompleteActionChunk and
// FailChunk plus actionCompletionAllowed and finishActionBuildIfAllCompleted
// moved here from the root package so they promote through the embedded
// *Store. SQL text, status transition rules and dispatch-ID semantics are
// byte-identical to the root copies they replace.
//
// Action dispatch identity delegates to the canonical internal/model helper.
// ActionCompletionAllowed and ActionFailureAllowed are the single
// authoritative copies consumed by the build package.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Deployment lifecycle vocabulary needed only by the failure-identity
// helpers below; the remaining status vocabulary lives in build_create_get.go.
const (
	statusDeploying    = "deploying"
	statusDeployFailed = "deploy_failed"
)

func (s *Store) execUpdateByID(ctx context.Context, idLabel, id, valLabel, val string, allowEmptyVal bool, query, opLabel string, notFoundErr error) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	cleanID, cleanVal := strings.TrimSpace(id), strings.TrimSpace(val)
	if cleanID == "" || (!allowEmptyVal && cleanVal == "") {
		return fmt.Errorf("%s and %s cannot be empty", idLabel, valLabel)
	}
	cmdTag, err := s.db.Exec(ctx, query, cleanVal, cleanID)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", opLabel, err)
	case cmdTag.RowsAffected() == 0:
		return notFoundErr
	default:
		return nil
	}
}

func (s *Store) UpdateChunkStatus(ctx context.Context, chunkID, status string) error {
	return s.execUpdateByID(ctx, "chunk id", chunkID, "status", status, false, `UPDATE vanadis_chunks SET status = $1, started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, completed_at = CASE WHEN $1 IN ('merged', 'completed', 'failed', 'cancelled') THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis chunk status", ErrChunkNotFound)
}

func (s *Store) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "status", status, false, `UPDATE vanadis_builds SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status", ErrBuildNotFound)
}

func (s *Store) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "message_id", messageID, true, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status_message_id", ErrBuildNotFound)
}

func (s *Store) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = CASE WHEN status IN ('pending', 'queued') THEN 'running' ELSE status END, started_at = CASE WHEN status IN ('pending', 'queued') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, chunkID)
	if err != nil {
		return fmt.Errorf("reserve chunk for launch %s: %w", chunkID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	return nil
}

// maxActionResumes caps the automatic resumes of an action chunk whose turns
// end without a complete_chunk verdict.
const maxActionResumes = 12

// MaxActionResumes exposes maxActionResumes to the lifecycle and prompt layers.
const MaxActionResumes = maxActionResumes

// ActionResumeDispatchID identifies automatic resume k of an action chunk's
// current generation.
func ActionResumeDispatchID(chunk model.VanadisChunk, k int) string {
	return model.ActionDispatchID(chunk) + "-resume-" + strconv.Itoa(k)
}

// ActionResumeIndex maps a current-generation action dispatch ID to its resume
// ordinal: 0 for the launch turn, k for resume 1..maxActionResumes. Any other
// ID, including other generations' IDs, reports false.
func ActionResumeIndex(chunk model.VanadisChunk, dispatchID string) (int, bool) {
	launch := model.ActionDispatchID(chunk)
	if dispatchID == launch {
		return 0, true
	}
	suffix, ok := strings.CutPrefix(dispatchID, launch+"-resume-")
	if !ok {
		return 0, false
	}
	k, err := strconv.Atoi(suffix)
	if err != nil || k < 1 || k > maxActionResumes || strconv.Itoa(k) != suffix {
		return 0, false
	}
	return k, true
}

func currentActionIdentity(chunk model.VanadisChunk, dispatchID string) bool {
	_, ok := ActionResumeIndex(chunk, dispatchID)
	return ok
}

// ActionCompletionAllowed reports whether an action chunk completion applies.
// A chunk with an attached repair PR qualifies once the repair deployed and
// moved it back to running.
func ActionCompletionAllowed(chunk model.VanadisChunk, buildStatus, dispatchID string) bool {
	identityMatches := currentActionIdentity(chunk, dispatchID) || (dispatchID == "" && chunk.StallNudges == 0)
	return chunk.Mode == chunkModeAction && chunk.Status == statusRunning && buildStatus == statusRunning && identityMatches
}

func (s *Store) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*VanadisAdvancementResult, error) {
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

	var b model.VanadisBuild
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
		BuildID:          bID,
		ThreadID:         b.ThreadID,
		Tier:             b.Tier,
		Title:            b.Title,
		Objective:        b.Objective,
		BuildStatus:      b.Status,
		StatusMessageID:  b.StatusMessageID,
		MergedChunkIndex: chunkIdx,
		MergedChunkTitle: cTitle,
		ChunkMode:        cMode,
		NextChunkIndex:   chunkIdx + 1,
		BuildCompleted:   false,
	}

	chunk := model.VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}

	res.CompletionAccepted = actionCompletionAccepted(chunk, b.Status, dispatchID)
	if !ActionCompletionAllowed(model.VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}, b.Status, dispatchID) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit complete action chunk tx: %w", err)
		}
		return res, nil
	}

	const completeChunkSQL = `UPDATE vanadis_chunks SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, completeChunkSQL, chunkID); err != nil {
		return nil, fmt.Errorf("complete action chunk %s: %w", chunkID, err)
	}
	res.Advanced = true

	const countRemainingSQL = `SELECT COUNT(*) FROM vanadis_chunks WHERE build_id = $1 AND id != $2 AND status != 'completed';`
	var remainingCount int
	if err := tx.QueryRow(ctx, countRemainingSQL, bID, chunkID).Scan(&remainingCount); err != nil {
		return nil, fmt.Errorf("count remaining chunks for build %s: %w", bID, err)
	}

	if remainingCount == 0 {
		if err := finishActionBuildIfAllCompleted(ctx, tx, bID, res); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit complete action chunk tx: %w", err)
	}
	return res, nil
}

func finishActionBuildIfAllCompleted(ctx context.Context, tx pgx.Tx, buildID string, res *VanadisAdvancementResult) error {
	const completeBuildSQL = `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, completeBuildSQL, buildID); err != nil {
		return fmt.Errorf("complete action build %s: %w", buildID, err)
	}
	res.BuildCompleted = true
	res.BuildStatus = statusCompleted

	const selectAllChunksSQL = `SELECT id, chunk_index, mode, COALESCE(repo, ''), title, status, COALESCE(pr_number, 0), COALESCE(started_at, created_at), COALESCE(completed_at, updated_at), updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := tx.Query(ctx, selectAllChunksSQL, buildID)
	if err != nil {
		return fmt.Errorf("query chunks for completed action build %s: %w", buildID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cs VanadisChunkSummary
		if err := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Mode, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &cs.StartedAt, &cs.CompletedAt, &cs.UpdatedAt); err != nil {
			return fmt.Errorf("scan chunk summary for build %s: %w", buildID, err)
		}
		res.Chunks = append(res.Chunks, cs)
	}
	return rows.Err()
}

func (s *Store) FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
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

	parentID, lockErr := lockFailureParent(ctx, tx, chunkID)
	if lockErr != nil {
		return nil, nil, false, lockErr
	}

	const selectChunkQuery = `SELECT c.id, c.build_id, c.chunk_index, c.mode, c.repo, c.title, c.instructions, c.target_files, c.target_symbols, COALESCE(c.effort, 'medium'), c.acceptance_criteria, c.status, COALESCE(c.pr_number, 0), c.started_at, c.completed_at, c.created_at, c.updated_at, c.stall_nudges, b.id, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, COALESCE(b.shared_context, ''), b.status, COALESCE(b.status_message_id, ''), b.created_at, b.updated_at FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND c.build_id = $2 FOR UPDATE OF c;`

	var c model.VanadisChunk
	var b model.VanadisBuild
	var startedAt, completedAt sql.NullTime
	err = tx.QueryRow(ctx, selectChunkQuery, chunkID, parentID).Scan(
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

	if failureSkipped(c, b.Status, dispatchID) {
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, false, fmt.Errorf("commit fail chunk tx: %w", err)
		}
		return &b, &c, false, nil
	}

	now := time.Now().UTC()
	c.Status = statusFailed
	c.CompletedAt = now
	c.UpdatedAt = now
	banner := fmt.Sprintf("Chunk %d failed: %s", c.ChunkIndex+1, reason)
	c.Banner = banner

	const failChunkSQL = `UPDATE vanadis_chunks SET status = 'failed', banner = $2, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, failChunkSQL, chunkID, banner); err != nil {
		return nil, nil, false, fmt.Errorf("fail chunk %s: %w", chunkID, err)
	}

	if b.Status != statusFailed && b.Status != statusCancelled {
		b.Status = statusFailed
		b.UpdatedAt = now
		const failBuildSQL = `UPDATE vanadis_builds SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
		if _, err := tx.Exec(ctx, failBuildSQL, b.ID); err != nil {
			return nil, nil, false, fmt.Errorf("fail build %s: %w", b.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, false, fmt.Errorf("commit fail chunk tx: %w", err)
	}
	return &b, &c, true, nil
}

func actionCompletionAccepted(chunk model.VanadisChunk, buildStatus, dispatchID string) bool {
	if chunk.Status == statusCompleted && (buildStatus == statusRunning || buildStatus == statusCompleted) {
		chunk.Status = statusRunning
		buildStatus = statusRunning
	}
	return ActionCompletionAllowed(chunk, buildStatus, dispatchID)
}

// ActionFailureAllowed reports whether an action chunk failure applies.
func ActionFailureAllowed(c model.VanadisChunk, buildStatus, dispatchID string) bool {
	if c.PRNumber == 0 {
		return ActionCompletionAllowed(c, buildStatus, dispatchID)
	}
	// PR attachment retains action mode. PR closure and remediation retain their
	// identities, while old action-turn identities must still match the generation.
	prIdentity := dispatchID == "" || strings.HasPrefix(dispatchID, "vanadis-"+c.ID+"-cifail-") ||
		dispatchID == "vanadis-conflict-"+c.ID || strings.HasPrefix(dispatchID, "vanadis-conflict-"+c.ID+"-")
	return buildStatus == statusRunning && c.Status != statusMerged && c.Status != statusDeploying && c.Status != statusDeployFailed &&
		(prIdentity || currentActionIdentity(c, dispatchID))
}

func failureSkipped(c model.VanadisChunk, buildStatus, dispatchID string) bool {
	return (c.Mode == chunkModeAction && !ActionFailureAllowed(c, buildStatus, dispatchID)) || c.Status == statusFailed || c.Status == statusCompleted || c.Status == statusCancelled
}

func lockFailureParent(ctx context.Context, tx pgx.Tx, chunkID string) (string, error) {
	var parentID string
	err := tx.QueryRow(ctx, `SELECT id FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks WHERE id = $1) FOR UPDATE;`, chunkID).Scan(&parentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrChunkNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock failure build: %w", err)
	}
	return parentID, nil
}
