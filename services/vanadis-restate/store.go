package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	statusPending      = "pending"
	statusRunning      = "running"
	statusCoding       = "coding"
	statusOpen         = "open"
	statusPROpened     = "pr_opened"
	statusMerged       = "merged"
	statusSynced       = "synced"
	statusDeploying    = "deploying"
	statusDeployed     = "deployed"
	statusConflict     = "conflict"
	statusDeployFailed = "deploy_failed"
	statusCIFailed     = "ci_failed"
	chunkModeAction    = "action"
	defaultRepoOwner   = "azylman"
	effortLow          = "low"
	effortMedium       = "medium"
	effortHigh         = "high"
)

// ErrChunkNotFound aliases the store package sentinel so errors.Is identity
// holds for chunk lookups wherever the lookup executes.
var ErrChunkNotFound = store.ErrChunkNotFound

type CreateBuildRequest struct {
	ID            string                   `json:"id"`
	ThreadID      string                   `json:"thread_id"`
	Tier          int                      `json:"tier"`
	Title         string                   `json:"title"`
	Objective     string                   `json:"objective"`
	SharedContext string                   `json:"shared_context,omitempty"`
	Quiet         bool                     `json:"quiet"`
	Chunks        []store.CreateChunkInput `json:"chunks"`
}

type AppendChunksRequest struct {
	Chunks []store.CreateChunkInput `json:"chunks"`
}

type PostgresVanadisStore struct {
	*store.Store

	db store.PgxQueryExecer
}

func NewPostgresVanadisStore(db store.PgxQueryExecer) *PostgresVanadisStore {
	return &PostgresVanadisStore{Store: store.New(db), db: db}
}

func (s *PostgresVanadisStore) execUpdateByID(ctx context.Context, idLabel, id, valLabel, val string, allowEmptyVal bool, query, opLabel string, notFoundErr error) error {
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

func (s *PostgresVanadisStore) UpdateChunkStatus(ctx context.Context, chunkID, status string) error {
	return s.execUpdateByID(ctx, "chunk id", chunkID, "status", status, false, `UPDATE vanadis_chunks SET status = $1, started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, completed_at = CASE WHEN $1 IN ('merged', 'completed', 'failed', 'cancelled') THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis chunk status", ErrChunkNotFound)
}

func (s *PostgresVanadisStore) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "status", status, false, `UPDATE vanadis_builds SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status", store.ErrBuildNotFound)
}

func (s *PostgresVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "message_id", messageID, true, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status_message_id", store.ErrBuildNotFound)
}

func (s *PostgresVanadisStore) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
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

func actionCompletionAllowed(chunk model.VanadisChunk, buildStatus, dispatchID string) bool {
	identityMatches := dispatchID == actionDispatchID(chunk) || (dispatchID == "" && chunk.StallNudges == 0)
	return chunk.Mode == chunkModeAction && chunk.Status == statusRunning && buildStatus == statusRunning && chunk.PRNumber == 0 && identityMatches
}

func (s *PostgresVanadisStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
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

	res := &store.VanadisAdvancementResult{
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
	if !actionCompletionAllowed(model.VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}, b.Status, dispatchID) {
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

func finishActionBuildIfAllCompleted(ctx context.Context, tx pgx.Tx, buildID string, res *store.VanadisAdvancementResult) error {
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
		var cs store.VanadisChunkSummary
		if err := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Mode, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &cs.StartedAt, &cs.CompletedAt, &cs.UpdatedAt); err != nil {
			return fmt.Errorf("scan chunk summary for build %s: %w", buildID, err)
		}
		res.Chunks = append(res.Chunks, cs)
	}
	return rows.Err()
}

func (s *PostgresVanadisStore) FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
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

type chunkPRMatch struct {
	chunkID      string
	buildID      string
	chunkIdx     int
	effort       string
	hasRemaining bool
}

func (s *PostgresVanadisStore) GetChunkByPR(ctx context.Context, repo string, prNumber int) (*store.VanadisChunkPRLookup, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	candidates := store.RepoMatchCandidates(repo)
	if len(candidates) == 0 || prNumber <= 0 {
		return &store.VanadisChunkPRLookup{Matched: false}, nil
	}
	match, found, err := s.findChunkPRMatch(ctx, candidates, prNumber)
	if err != nil || !found {
		return &store.VanadisChunkPRLookup{Matched: false}, err
	}
	return &store.VanadisChunkPRLookup{
		Matched: true,
		ChunkID: match.chunkID,
		BuildID: match.buildID,
		Effort:  store.CapChunkEffort(match.effort),
	}, nil
}

func (s *PostgresVanadisStore) LookupPRChunk(ctx context.Context, repo string, prNumber int) (string, string, error) {
	lookup, err := s.GetChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return "", "", err
	}
	if lookup == nil || !lookup.Matched {
		return "", "", ErrChunkNotFound
	}
	return lookup.BuildID, lookup.ChunkID, nil
}

func (s *PostgresVanadisStore) LookupChunkBuildID(ctx context.Context, chunkID string) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return "", errors.New("chunk id cannot be empty")
	}
	var buildID string
	err := s.db.QueryRow(ctx, `SELECT build_id FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&buildID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrChunkNotFound
		}
		return "", fmt.Errorf("lookup chunk build id: %w", err)
	}
	return buildID, nil
}

func (s *PostgresVanadisStore) LookupUnattachedChunkByThreadAndRepo(ctx context.Context, threadID, repo string) (string, string, error) {
	if s == nil || s.db == nil {
		return "", "", errors.New("database connection is nil")
	}
	threadID = normalizeDiscordThreadID(threadID)
	candidates := store.RepoMatchCandidates(repo)
	if threadID == "" || len(candidates) == 0 {
		return "", "", ErrChunkNotFound
	}
	const query = `SELECT c.build_id, c.id FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE b.thread_id = $1 AND b.status = 'running' AND c.mode = 'pr' AND c.repo = ANY($2) AND COALESCE(c.pr_number, 0) = 0 AND c.status IN ('running', 'coding') AND c.started_at IS NOT NULL ORDER BY c.chunk_index ASC LIMIT 2;`
	rows, err := s.db.Query(ctx, query, threadID, candidates)
	if err != nil {
		return "", "", fmt.Errorf("lookup unattached chunk by thread %s and repo %s: %w", threadID, repo, err)
	}
	defer rows.Close()

	type chunkMatch struct {
		buildID string
		chunkID string
	}
	var matches []chunkMatch
	for rows.Next() {
		var m chunkMatch
		if err := rows.Scan(&m.buildID, &m.chunkID); err != nil {
			return "", "", fmt.Errorf("scan unattached chunk: %w", err)
		}
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return "", "", fmt.Errorf("iterate unattached chunks: %w", err)
	}
	if len(matches) == 1 {
		return matches[0].buildID, matches[0].chunkID, nil
	}
	return "", "", ErrChunkNotFound
}

func (s *PostgresVanadisStore) findChunkPRMatch(ctx context.Context, candidates []string, prNumber int) (chunkPRMatch, bool, error) {
	const query = `SELECT c.id, c.build_id, c.chunk_index, COALESCE(NULLIF(c.effort, ''), 'medium'), EXISTS (SELECT 1 FROM vanadis_chunks c2 WHERE c2.build_id = c.build_id AND c2.chunk_index > c.chunk_index) FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.repo = ANY($1) AND c.pr_number = $2 AND b.status != 'cancelled' ORDER BY c.updated_at DESC LIMIT 1;`
	var m chunkPRMatch
	err := s.db.QueryRow(ctx, query, candidates, prNumber).Scan(&m.chunkID, &m.buildID, &m.chunkIdx, &m.effort, &m.hasRemaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return chunkPRMatch{}, false, nil
	}
	if err != nil {
		return chunkPRMatch{}, false, fmt.Errorf("query chunk by pr candidates %v #%d: %w", candidates, prNumber, err)
	}
	return m, true, nil
}

func (s *PostgresVanadisStore) RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk_id cannot be empty")
	}
	var rolloutBytes []byte
	var err error
	if rolloutState != nil {
		rolloutBytes, err = json.Marshal(rolloutState)
		if err != nil {
			return fmt.Errorf("marshal rollout state: %w", err)
		}
	} else {
		rolloutBytes = []byte("{}")
	}

	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET rollout_state = CASE WHEN status IN ('completed', 'failed', 'cancelled') THEN rollout_state ELSE $1::jsonb END, status = CASE WHEN status IN ('completed', 'failed', 'cancelled') THEN status WHEN $2 != '' THEN $2 ELSE status END, updated_at = CURRENT_TIMESTAMP WHERE id = $3;`, rolloutBytes, strings.TrimSpace(status), chunkID)
	switch {
	case err != nil:
		return fmt.Errorf("record pr rollout state: %w", err)
	case tag.RowsAffected() == 0:
		return ErrChunkNotFound
	default:
		return nil
	}
}

func (s *PostgresVanadisStore) IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string, headSHA ...string) (int, error) {
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
		return 0, errPRTransitionSkipped
	}
	if err != nil {
		return 0, fmt.Errorf("increment chunk ci fail count: %w", err)
	}
	return failCount, nil
}

func (s *PostgresVanadisStore) UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks AS c SET status = CASE WHEN $1 != '' THEN $1 ELSE c.status END, banner = $2, updated_at = CURRENT_TIMESTAMP FROM vanadis_builds AS b WHERE c.id = $3 AND b.id = c.build_id AND c.status NOT IN ('completed', 'failed', 'cancelled') AND b.status NOT IN ('completed', 'failed', 'cancelled') AND ($1 NOT IN ('conflict', 'ci_failed') OR c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed'));`, strings.TrimSpace(status), strings.TrimSpace(banner), id)
	switch {
	case err != nil:
		return fmt.Errorf("update chunk banner and status: %w", err)
	case tag.RowsAffected() == 0:
		return errPRTransitionSkipped
	default:
		return nil
	}
}
