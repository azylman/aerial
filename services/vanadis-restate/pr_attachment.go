package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *PostgresVanadisStore) AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
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

func (s *PostgresVanadisStore) attachToExistingChunk(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	var mode string
	chkErr := s.db.QueryRow(ctx, `SELECT mode FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&mode)
	if chkErr != nil {
		if errors.Is(chkErr, pgx.ErrNoRows) {
			return "", "", "", ErrChunkNotFound
		}
		return "", "", "", fmt.Errorf("check chunk mode %s: %w", chunkID, chkErr)
	}
	if mode == chunkModeAction {
		repo := normalizeRepo(req.Repo)
		if repo == "" {
			return "", "", "", fmt.Errorf("%w: repo is required to attach PR to action chunk", ErrInvalidBuildInput)
		}
		const qAction = `UPDATE vanadis_chunks c SET repo = $1, pr_number = $2, status = CASE WHEN c.status IN ('merged', 'deploying', 'completed', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $3 AND c.status != 'cancelled' AND (c.status NOT IN ('merged', 'deploying', 'completed') OR (c.repo = $1 AND c.pr_number = $2)) AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
		var buildID, returnChunkID, threadID string
		err := s.db.QueryRow(ctx, qAction, repo, req.PRNumber, chunkID).Scan(&buildID, &returnChunkID, &threadID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, repo, req.PRNumber)
			}
			return "", "", "", fmt.Errorf("attach action chunk pr %s#%d: %w", repo, req.PRNumber, err)
		}
		return buildID, returnChunkID, threadID, nil
	}
	return s.attachPRTransaction(ctx, chunkID, req)
}

// Lock the parent before its chunks, matching lifecycle and retry mutations.
func (s *PostgresVanadisStore) attachPRTransaction(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if e := tx.Rollback(ctx); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
			log.Printf("[vanadis] attachment rollback: %v", e)
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
	var selected *VanadisChunk
	for i := range chunks {
		if chunks[i].ID == chunkID {
			selected = &chunks[i]
			break
		}
	}
	if selected == nil {
		return "", "", "", ErrChunkNotFound
	}
	c := *selected
	if !attachmentMatches(c, b, req) {
		return "", "", "", ErrBuildConflict
	}

	recoverWorker := firstWorkerFailure(c)
	if recoverWorker && !attachmentRecoveryAllowed(b, chunks, c.ID) {
		return "", "", "", ErrBuildConflict
	}

	// Recovery clears only worker failure evidence; CI budgets and dispatch identity survive.
	const update = `UPDATE vanadis_chunks SET pr_number = $1, status = CASE WHEN $3 THEN 'pr_opened' WHEN status IN ('merged', 'deploying', 'completed', 'failed') THEN status ELSE 'pr_opened' END, banner = CASE WHEN $3 THEN '' ELSE banner END, completed_at = CASE WHEN $3 THEN NULL ELSE completed_at END, started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $2;`
	if _, err := tx.Exec(ctx, update, req.PRNumber, chunkID, recoverWorker); err != nil {
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

func (s *PostgresVanadisStore) attachStandalonePR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	repo := normalizeRepo(req.Repo)
	if repo == "" {
		return "", "", "", fmt.Errorf("%w: repo is required for standalone attach", ErrInvalidBuildInput)
	}
	tx, txErr := s.db.Begin(ctx)
	if txErr != nil {
		return "", "", "", fmt.Errorf("begin standalone build tx: %w", txErr)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] standalone attach tx rollback error: %v", rbErr)
		}
	}()
	candidates := repoMatchCandidates(repo)
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
	buildID := generateBuildID()
	chunkID := buildID + "-chunk-1"
	title := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("PR #%d: %s", req.PRNumber, cleanRepoName(repo)))
	threadID := normalizeDiscordThreadID(req.TargetID)
	now := time.Now().UTC()
	build := VanadisBuild{
		ID:        buildID,
		ThreadID:  threadID,
		Tier:      0,
		Title:     title,
		Objective: title,
		Status:    statusRunning,
		Quiet:     req.Quiet,
		CreatedAt: now,
		UpdatedAt: now,
	}
	chunk := VanadisChunk{
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
	bQ := `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`
	if _, err := tx.Exec(ctx, bQ, build.ID, build.ThreadID, build.Tier, build.Title, build.Objective, "", build.Status, "", build.Quiet, now, now); err != nil {
		return "", "", "", fmt.Errorf("insert standalone build %s: %w", buildID, err)
	}
	if err := insertBuildChunk(ctx, tx, buildID, chunk, insertChunkSQL); err != nil {
		return "", "", "", fmt.Errorf("insert standalone chunk %s: %w", chunkID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", "", fmt.Errorf("commit standalone pr build: %w", err)
	}
	return buildID, chunkID, threadID, nil
}

func (s *PostgresVanadisStore) resolveAttachChunkPRError(ctx context.Context, chunkID, repo string, prNumber int) error {
	var exists bool
	checkErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1);`, chunkID).Scan(&exists)
	if checkErr == nil && !exists {
		return fmt.Errorf("%w: %s", ErrChunkNotFound, chunkID)
	}
	if repo == "" {
		return fmt.Errorf("%w: chunk %s or parent build is cancelled", ErrBuildConflict, chunkID)
	}
	var isCancelled bool
	cancErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND (c.status = 'cancelled' OR c.status IN ('merged', 'deploying', 'completed') OR b.status IN ('cancelled', 'completed')));`, chunkID).Scan(&isCancelled)
	if cancErr == nil && isCancelled {
		return fmt.Errorf("%w: chunk %s or parent build is in terminal status", ErrBuildConflict, chunkID)
	}
	candidates := repoMatchCandidates(repo)
	var repoMismatch bool
	mismatchErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1 AND repo != '' AND NOT (repo = ANY($2)));`, chunkID, candidates).Scan(&repoMismatch)
	if mismatchErr == nil && repoMismatch {
		return fmt.Errorf("%w: chunk %s has mismatching repo", ErrBuildConflict, chunkID)
	}
	return fmt.Errorf("attach pr failed for chunk %s #%d", chunkID, prNumber)
}

func attachmentRecoveryAllowed(b VanadisBuild, chunks []VanadisChunk, selected string) bool {
	if b.Status != statusRunning && b.Status != statusFailed {
		return false
	}
	for _, sibling := range chunks {
		if sibling.ID != selected && (sibling.Status == statusFailed || sibling.Status == statusCancelled) {
			return false
		}
	}
	return true
}

func attachmentMatches(c VanadisChunk, b VanadisBuild, req AttachChunkPRRequest) bool {
	candidates := repoMatchCandidates(req.Repo)
	matches := len(candidates) == 0
	for _, repo := range candidates {
		if repo == c.Repo {
			matches = true
		}
	}
	terminalChunk := c.Status == statusMerged || c.Status == statusDeploying || c.Status == statusCompleted
	return matches && c.Status != statusCancelled && b.Status != statusCancelled && b.Status != statusCompleted && (!terminalChunk || c.PRNumber == req.PRNumber)
}

func firstWorkerFailure(c VanadisChunk) bool {
	return c.Mode == "pr" && c.Status == statusFailed && c.PRNumber == 0 && c.CIFailCount == 0
}
