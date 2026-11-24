package store

// PR attachment persistence for vanadis-restate.
//
// AttachChunkPR, attachToExistingChunk, attachPRTransaction,
// attachStandalonePR, resolveAttachChunkPRError, attachmentRecoveryAllowed,
// AttachmentMatches and firstWorkerFailure moved here from the root package
// so they promote through the embedded *Store. Attachment matching,
// standalone PR creation, transaction flow, and error behavior are
// byte-identical to the root copies they replace.
//
// Build ID and repo-name normalization delegate to the canonical
// internal/model helpers. The synced and deployed status constants mirror
// the frozen root store.go copies byte-for-byte; those root files stay
// frozen until the facade-removal chunk unifies both copies.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Synced and deployed lifecycle vocabulary needed only by attachment
// matching below; the remaining status vocabulary lives in
// build_create_get.go, pr_lifecycle.go, and status_action.go.
const (
	statusSynced   = "synced"
	statusDeployed = "deployed"
)

func (s *Store) AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	if s == nil || s.db == nil {
		return "", "", "", errors.New("database connection is nil")
	}
	if req.PRNumber <= 0 {
		return "", "", "", fmt.Errorf("%w: chunk id or repo, and valid pr number are required", ErrInvalidBuildInput)
	}
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID != "" {
		return s.attachToExistingChunk(ctx, chunkID, req)
	}
	return s.attachStandalonePR(ctx, req)
}

type donorChunk struct {
	id           string
	status       string
	progression  int
	ciChecks     []byte
	rolloutState []byte
	banner       string
	completedAt  sql.NullTime
}

func isNonEmptyJSON(b []byte) bool {
	s := strings.TrimSpace(string(b))
	return s != "" && s != "{}" && s != jsonNull
}

type rowQueryer interface {
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

func queryDonorChunk(ctx context.Context, q rowQueryer, candidates []string, prNumber int, excludeChunkID string) (donorChunk, bool, error) {
	if len(candidates) == 0 || prNumber <= 0 {
		return donorChunk{}, false, nil
	}
	const donorQ = `SELECT c.id, c.status,
		CASE c.status
			WHEN 'completed' THEN 9
			WHEN 'deployed' THEN 8
			WHEN 'deploy_failed' THEN 7
			WHEN 'deploying' THEN 6
			WHEN 'synced' THEN 5
			WHEN 'merged' THEN 4
			WHEN 'ci_failed' THEN 3
			WHEN 'conflict' THEN 2
			WHEN 'pr_opened' THEN 1
			ELSE 0
		END AS progression,
		c.ci_checks, c.rollout_state, c.banner, c.completed_at
	FROM vanadis_chunks c
	JOIN vanadis_builds b ON c.build_id = b.id
	WHERE c.repo = ANY($1) AND c.pr_number = $2 AND c.id != $3 AND c.status != 'cancelled' AND b.status != 'cancelled'
	ORDER BY progression DESC, c.updated_at DESC, c.id ASC
	LIMIT 1;`
	var d donorChunk
	err := q.QueryRow(ctx, donorQ, candidates, prNumber, excludeChunkID).Scan(
		&d.id, &d.status, &d.progression, &d.ciChecks, &d.rolloutState, &d.banner, &d.completedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return donorChunk{}, false, nil
	}
	if err != nil {
		return donorChunk{}, false, fmt.Errorf("query donor chunk %v#%d: %w", candidates, prNumber, err)
	}
	return d, true, nil
}

func (s *Store) attachToExistingChunk(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	var mode string
	chkErr := s.db.QueryRow(ctx, `SELECT mode FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&mode)
	if chkErr != nil {
		if errors.Is(chkErr, pgx.ErrNoRows) {
			return "", "", "", ErrChunkNotFound
		}
		return "", "", "", fmt.Errorf("check chunk mode %s: %w", chunkID, chkErr)
	}
	if mode == chunkModeAction {
		return s.attachActionChunkPR(ctx, chunkID, req)
	}
	return s.attachPRTransaction(ctx, chunkID, req)
}

func (s *Store) attachActionChunkPR(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	repo := buildplan.TrimRepo(req.Repo)
	if repo == "" {
		return "", "", "", fmt.Errorf("%w: repo is required to attach PR to action chunk", ErrInvalidBuildInput)
	}
	candidates := RepoMatchCandidates(s.gitHubOwner, repo)
	donor, found, err := queryDonorChunk(ctx, s.db, candidates, req.PRNumber, chunkID)
	if err != nil {
		return "", "", "", err
	}
	if found && (donor.status == statusDeployed || donor.status == statusCompleted) {
		return s.execActionChunkDonorAttach(ctx, chunkID, repo, req.PRNumber, candidates, donor)
	}
	return s.execActionChunkStandardAttach(ctx, chunkID, repo, req.PRNumber, candidates)
}

func (s *Store) execActionChunkDonorAttach(ctx context.Context, chunkID, repo string, prNumber int, candidates []string, donor donorChunk) (string, string, string, error) {
	const qActionDonor = `UPDATE vanadis_chunks c SET repo = $1, pr_number = $2, status = CASE WHEN c.status = 'failed' OR (c.repo = ANY($4) AND c.pr_number = $2 AND c.status IN ('ci_failed', 'conflict', 'merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed')) THEN c.status WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN 'pr_opened' ELSE 'running' END, rollout_state = CASE WHEN $5::jsonb IS NOT NULL THEN $5::jsonb WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN '{}'::jsonb ELSE c.rollout_state END, ci_checks = CASE WHEN $6::jsonb IS NOT NULL THEN $6::jsonb WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN '{}'::jsonb ELSE c.ci_checks END, banner = CASE WHEN c.repo = ANY($4) AND c.pr_number = $2 AND c.status IN ('ci_failed', 'conflict') THEN c.banner ELSE '' END, completed_at = CASE WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN NULL ELSE c.completed_at END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $3 AND c.status != 'cancelled' AND (c.repo = '' OR c.repo = ANY($4)) AND (c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'completed') OR (c.repo = ANY($4) AND c.pr_number = $2)) AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
	var donorRolloutState, donorCIChecks any
	if isNonEmptyJSON(donor.rolloutState) {
		donorRolloutState = donor.rolloutState
	}
	if isNonEmptyJSON(donor.ciChecks) {
		donorCIChecks = donor.ciChecks
	}
	var buildID, returnChunkID, threadID string
	scanErr := s.db.QueryRow(ctx, qActionDonor, repo, prNumber, chunkID, candidates, donorRolloutState, donorCIChecks).Scan(&buildID, &returnChunkID, &threadID)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, repo, prNumber)
		}
		return "", "", "", fmt.Errorf("attach action chunk pr %s#%d: %w", repo, prNumber, scanErr)
	}
	return buildID, returnChunkID, threadID, nil
}

func (s *Store) execActionChunkStandardAttach(ctx context.Context, chunkID, repo string, prNumber int, candidates []string) (string, string, string, error) {
	const qAction = `UPDATE vanadis_chunks c SET repo = $1, pr_number = $2, status = CASE WHEN c.status = 'failed' OR (c.repo = ANY($4) AND c.pr_number = $2 AND c.status IN ('ci_failed', 'conflict', 'merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed')) THEN c.status ELSE 'pr_opened' END, banner = CASE WHEN c.status IN ('ci_failed', 'conflict', 'deploy_failed') AND (NOT (c.repo = ANY($4)) OR c.pr_number IS DISTINCT FROM $2) THEN '' ELSE c.banner END, completed_at = CASE WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN NULL ELSE c.completed_at END, ci_checks = CASE WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN '{}'::jsonb ELSE c.ci_checks END, rollout_state = CASE WHEN c.status = 'deploy_failed' AND c.pr_number IS DISTINCT FROM $2 THEN '{}'::jsonb ELSE c.rollout_state END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $3 AND c.status != 'cancelled' AND (c.repo = '' OR c.repo = ANY($4)) AND (c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'completed') OR (c.repo = ANY($4) AND c.pr_number = $2)) AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
	var buildID, returnChunkID, threadID string
	err := s.db.QueryRow(ctx, qAction, repo, prNumber, chunkID, candidates).Scan(&buildID, &returnChunkID, &threadID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, repo, prNumber)
		}
		return "", "", "", fmt.Errorf("attach action chunk pr %s#%d: %w", repo, prNumber, err)
	}
	return buildID, returnChunkID, threadID, nil
}

func applyDonorPRUpdate(ctx context.Context, tx pgx.Tx, chunkID string, prNumber int, donor donorChunk) error {
	var newStatus string
	if donor.status == statusDeployed || donor.status == statusCompleted {
		newStatus = statusCompleted
	} else {
		newStatus = donor.status
	}
	var newBanner string
	switch donor.status {
	case statusMerged, statusSynced, statusDeploying, statusDeployed, statusCompleted:
		newBanner = ""
	default:
		newBanner = donor.banner
	}
	isPostMerge := false
	switch donor.status {
	case statusMerged, statusSynced, statusDeploying, statusDeployFailed, statusDeployed, statusCompleted:
		isPostMerge = true
	}
	var donorCIChecks, donorRolloutState, donorCompletedAt any
	if isNonEmptyJSON(donor.ciChecks) {
		donorCIChecks = donor.ciChecks
	}
	if isNonEmptyJSON(donor.rolloutState) {
		donorRolloutState = donor.rolloutState
	}
	if donor.completedAt.Valid {
		donorCompletedAt = donor.completedAt.Time
	}
	const updateDonor = `UPDATE vanadis_chunks SET pr_number = $1, status = $3, banner = $4, ci_checks = CASE WHEN $5::jsonb IS NOT NULL THEN $5::jsonb ELSE ci_checks END, rollout_state = CASE WHEN $6::jsonb IS NOT NULL THEN $6::jsonb ELSE rollout_state END, completed_at = CASE WHEN $7::boolean THEN COALESCE(completed_at, $8::timestamptz, CURRENT_TIMESTAMP) ELSE completed_at END, started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $2;`
	_, err := tx.Exec(ctx, updateDonor, prNumber, chunkID, newStatus, newBanner, donorCIChecks, donorRolloutState, isPostMerge, donorCompletedAt)
	return err
}

func prStatusProgression(status string) int {
	switch status {
	case statusCompleted:
		return 9
	case statusDeployed:
		return 8
	case statusDeployFailed:
		return 7
	case statusDeploying:
		return 6
	case statusSynced:
		return 5
	case statusMerged:
		return 4
	case "ci_failed":
		return 3
	case "conflict":
		return 2
	case "pr_opened":
		return 1
	default:
		return 0
	}
}

func findSelectedChunk(chunks []model.VanadisChunk, chunkID string) (*model.VanadisChunk, error) {
	for i := range chunks {
		if chunks[i].ID == chunkID {
			return &chunks[i], nil
		}
	}
	return nil, ErrChunkNotFound
}

func (s *Store) updateChunkAttachment(ctx context.Context, tx pgx.Tx, c model.VanadisChunk, req AttachChunkPRRequest, recoverWorker bool) error {
	candidates := RepoMatchCandidates(s.gitHubOwner, strutil.FirstNonEmpty(buildplan.TrimRepo(req.Repo), c.Repo))
	donor, found, err := queryDonorChunk(ctx, tx, candidates, req.PRNumber, c.ID)
	if err != nil {
		return err
	}
	targetProg := 0
	if c.PRNumber == req.PRNumber {
		targetProg = prStatusProgression(c.Status)
	}
	if found && donor.progression > 1 && donor.progression > targetProg {
		return applyDonorPRUpdate(ctx, tx, c.ID, req.PRNumber, donor)
	}
	// Recovery clears only worker failure evidence; CI budgets and dispatch identity survive.
	const update = `UPDATE vanadis_chunks SET pr_number = $1, status = CASE WHEN $3 THEN 'pr_opened' WHEN status = 'deploy_failed' AND COALESCE(pr_number, 0) != $1 THEN 'pr_opened' WHEN status = 'failed' OR (COALESCE(pr_number, 0) = $1 AND status IN ('ci_failed', 'conflict', 'merged', 'synced', 'deploying', 'deployed', 'deploy_failed', 'completed')) THEN status ELSE 'pr_opened' END, banner = CASE WHEN $3 OR (status IN ('ci_failed', 'conflict', 'deploy_failed') AND COALESCE(pr_number, 0) != $1) THEN '' ELSE banner END, completed_at = CASE WHEN $3 OR (status = 'deploy_failed' AND COALESCE(pr_number, 0) != $1) THEN NULL ELSE completed_at END, ci_checks = CASE WHEN status = 'deploy_failed' AND COALESCE(pr_number, 0) != $1 THEN '{}'::jsonb ELSE ci_checks END, rollout_state = CASE WHEN status = 'deploy_failed' AND COALESCE(pr_number, 0) != $1 THEN '{}'::jsonb ELSE rollout_state END, started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $2;`
	_, err = tx.Exec(ctx, update, req.PRNumber, c.ID, recoverWorker)
	return err
}

// Lock the parent before its chunks, matching lifecycle and retry mutations.
func (s *Store) attachPRTransaction(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if e := tx.Rollback(ctx); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "[vanadis] attachment rollback failed", "error", e)
		}
	}()
	var buildID string
	if err = tx.QueryRow(ctx, `SELECT build_id FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&buildID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = ErrChunkNotFound
		}
		return "", "", "", err
	}
	b, chunks, err := lockRetryBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return "", "", "", err
	}
	selected, err := findSelectedChunk(chunks, chunkID)
	if err != nil {
		return "", "", "", err
	}
	c := *selected
	if !AttachmentMatches(s.gitHubOwner, c, b, req) {
		return "", "", "", ErrBuildConflict
	}

	recoverWorker := firstWorkerFailure(c, req.PRNumber)
	if recoverWorker && !attachmentRecoveryAllowed(b, chunks, c.ID) {
		return "", "", "", ErrBuildConflict
	}

	if err := s.updateChunkAttachment(ctx, tx, c, req, recoverWorker); err != nil {
		return "", "", "", err
	}
	if recoverWorker && b.Status == statusFailed {
		if _, err := tx.Exec(ctx, `UPDATE vanadis_builds SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, b.ID); err != nil {
			return "", "", "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", "", err
	}
	return b.ID, c.ID, b.ThreadID, nil
}

func (s *Store) attachStandalonePR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	repo := buildplan.TrimRepo(req.Repo)
	if repo == "" {
		return "", "", "", fmt.Errorf("%w: repo is required for standalone attach", ErrInvalidBuildInput)
	}
	tx, txErr := s.db.Begin(ctx)
	if txErr != nil {
		return "", "", "", fmt.Errorf("begin standalone build tx: %w", txErr)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "[vanadis] standalone attach tx rollback error", "error", rbErr)
		}
	}()
	candidates := RepoMatchCandidates(s.gitHubOwner, repo)
	const findExisting = `SELECT c.build_id, c.id, b.thread_id FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.repo = ANY($1) AND c.pr_number = $2 AND c.status != 'cancelled' LIMIT 1;`
	var existingBuildID, existingChunkID, existingThreadID string
	fErr := tx.QueryRow(ctx, findExisting, candidates, req.PRNumber).Scan(&existingBuildID, &existingChunkID, &existingThreadID)
	if fErr == nil {
		if cErr := tx.Commit(ctx); cErr != nil {
			return "", "", "", fmt.Errorf("commit existing pr attach: %w", cErr)
		}
		return existingBuildID, existingChunkID, existingThreadID, nil
	}
	if !errors.Is(fErr, pgx.ErrNoRows) {
		return "", "", "", fmt.Errorf("find existing pr attach %s#%d: %w", repo, req.PRNumber, fErr)
	}
	buildID := buildplan.GenerateBuildID()
	chunkID := buildID + "-chunk-1"
	title := strutil.FirstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("PR #%d: %s", req.PRNumber, model.CleanRepoName(repo)))
	threadID := model.NormalizeDiscordThreadID(req.TargetID)
	now := time.Now().UTC()
	build := model.VanadisBuild{
		ID:        buildID,
		Agent:     NormalizeRunnerAgent(req.Agent),
		ThreadID:  threadID,
		Tier:      0,
		Title:     title,
		Objective: title,
		Status:    statusRunning,
		Quiet:     req.Quiet,
		CreatedAt: now,
		UpdatedAt: now,
	}
	chunk := model.VanadisChunk{
		ID:           chunkID,
		BuildID:      buildID,
		ChunkIndex:   0,
		Mode:         "pr",
		Repo:         repo,
		Title:        title,
		Instructions: title,
		Status:       statusPROpened,
		PRNumber:     req.PRNumber,
		Effort:       effortMedium,
		DependsOn:    []string{},
		StartedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	bQ := `INSERT INTO vanadis_builds (id, agent, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`
	if _, err := tx.Exec(ctx, bQ, build.ID, build.Agent, build.ThreadID, build.Tier, build.Title, build.Objective, "", build.Status, "", build.Quiet, now, now); err != nil {
		return "", "", "", fmt.Errorf("insert standalone build %s: %w", buildID, err)
	}
	if err := InsertBuildChunk(ctx, tx, buildID, chunk, InsertChunkSQL); err != nil {
		return "", "", "", fmt.Errorf("insert standalone chunk %s: %w", chunkID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", "", fmt.Errorf("commit standalone pr build: %w", err)
	}
	return buildID, chunkID, threadID, nil
}

func (s *Store) resolveAttachChunkPRError(ctx context.Context, chunkID, repo string, prNumber int) error {
	var exists bool
	checkErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1);`, chunkID).Scan(&exists)
	if checkErr == nil && !exists {
		return fmt.Errorf("%w: %s", ErrChunkNotFound, chunkID)
	}
	var isCancelled bool
	cancErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND (c.status = 'cancelled' OR c.status IN ('merged', 'synced', 'deploying', 'deployed', 'completed') OR b.status IN ('cancelled', 'completed')));`, chunkID).Scan(&isCancelled)
	if cancErr == nil && isCancelled {
		return fmt.Errorf("%w: chunk %s or parent build is in terminal status", ErrBuildConflict, chunkID)
	}
	candidates := RepoMatchCandidates(s.gitHubOwner, repo)
	var repoMismatch bool
	mismatchErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1 AND repo != '' AND NOT (repo = ANY($2)));`, chunkID, candidates).Scan(&repoMismatch)
	if mismatchErr == nil && repoMismatch {
		return fmt.Errorf("%w: chunk %s has mismatching repo", ErrBuildConflict, chunkID)
	}
	return fmt.Errorf("attach pr failed for chunk %s #%d", chunkID, prNumber)
}

func attachmentRecoveryAllowed(b model.VanadisBuild, chunks []model.VanadisChunk, selected string) bool {
	if b.Status != statusRunning && b.Status != statusFailed {
		return false
	}
	for _, sibling := range chunks {
		if sibling.ID != selected && sibling.Status == statusCancelled {
			return false
		}
	}
	return true
}

func AttachmentMatches(owner string, c model.VanadisChunk, b model.VanadisBuild, req AttachChunkPRRequest) bool {
	candidates := RepoMatchCandidates(owner, req.Repo)
	matches := len(candidates) == 0
	for _, repo := range candidates {
		if repo == c.Repo {
			matches = true
		}
	}
	terminalChunk := c.Status == statusMerged || c.Status == statusSynced || c.Status == statusDeploying || c.Status == statusDeployed || c.Status == statusCompleted
	return matches && c.Status != statusCancelled && b.Status != statusCancelled && b.Status != statusCompleted && (!terminalChunk || c.PRNumber == req.PRNumber)
}

func firstWorkerFailure(c model.VanadisChunk, reqPRNumber ...int) bool {
	return c.Mode == "pr" && c.Status == statusFailed && c.CIFailCount == 0 && (c.PRNumber == 0 || (len(reqPRNumber) > 0 && reqPRNumber[0] > 0 && c.PRNumber == reqPRNumber[0]))
}
