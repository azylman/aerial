package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrPRNotFound = errors.New("pr record not found")
)

func normalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// UpsertPR inserts or updates a Pull Request tracking record.
func (s *SQLStore) UpsertPR(ctx context.Context, record PRRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	repo := normalizeRepo(record.Repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}
	if record.PRNumber <= 0 {
		return fmt.Errorf("pr_number must be greater than 0")
	}
	branch := strings.TrimSpace(record.Branch)
	if branch == "" {
		return fmt.Errorf("branch cannot be empty")
	}
	headSHA := strings.TrimSpace(record.HeadSHA)
	if headSHA == "" {
		return fmt.Errorf("head_sha cannot be empty")
	}
	targetID := strings.TrimSpace(record.TargetID)
	if targetID == "" {
		return fmt.Errorf("target_id cannot be empty")
	}

	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = "open"
	}
	meta := strings.TrimSpace(record.Metadata)
	if meta == "" {
		meta = "{}"
	}

	query := `
		INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (repo, pr_number) DO UPDATE SET
			branch = EXCLUDED.branch,
			head_sha = EXCLUDED.head_sha,
			target_id = EXCLUDED.target_id,
			title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END,
			status = CASE WHEN EXCLUDED.status != '' THEN EXCLUDED.status ELSE pr_registry.status END,
			metadata = CASE WHEN EXCLUDED.metadata != '{}' THEN EXCLUDED.metadata ELSE pr_registry.metadata END,
			updated_at = CURRENT_TIMESTAMP;
	`

	_, err := s.db.ExecContext(ctx, query, repo, record.PRNumber, branch, headSHA, targetID, record.Title, status, meta)
	if err != nil {
		return fmt.Errorf("failed to upsert pr record: %w", err)
	}
	return nil
}

// GetPRByNumber retrieves a PR record by repository name and PR number.
func (s *SQLStore) GetPRByNumber(ctx context.Context, repo string, prNumber int) (*PRRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, metadata, created_at, updated_at
		FROM pr_registry
		WHERE repo = $1 AND pr_number = $2;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, repo, prNumber).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPRNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pr record: %w", err)
	}
	return &r, nil
}

// GetPRByHeadSHA retrieves the most recent PR record matching repo and head SHA.
func (s *SQLStore) GetPRByHeadSHA(ctx context.Context, repo string, headSHA string) (*PRRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, metadata, created_at, updated_at
		FROM pr_registry
		WHERE repo = $1 AND head_sha = $2
		ORDER BY updated_at DESC
		LIMIT 1;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, repo, headSHA).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPRNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pr record by head_sha: %w", err)
	}
	return &r, nil
}

// GetPRByMergeSHA retrieves the PR record matching repo and squash merge commit SHA.
func (s *SQLStore) GetPRByMergeSHA(ctx context.Context, repo string, mergeSHA string) (*PRRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, metadata, created_at, updated_at
		FROM pr_registry
		WHERE repo = $1 AND merge_sha = $2
		ORDER BY updated_at DESC
		LIMIT 1;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, repo, mergeSHA).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPRNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pr record by merge_sha: %w", err)
	}
	return &r, nil
}

// UpdatePRMergeSHA records the squash merge commit SHA on main and marks the PR merged.
func (s *SQLStore) UpdatePRMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}

	query := `
		UPDATE pr_registry
		SET merge_sha = $1, status = 'merged', updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3;
	`

	res, err := s.db.ExecContext(ctx, query, mergeSHA, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to update pr merge_sha: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrPRNotFound
	}
	return nil
}

// UpdatePRStatus updates the lifecycle status of a PR.
func (s *SQLStore) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}

	query := `
		UPDATE pr_registry
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3;
	`

	res, err := s.db.ExecContext(ctx, query, status, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to update pr status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrPRNotFound
	}
	return nil
}

// AtomicTransitionPRStatus conditionally transitions PR status if it is not already in notStatus (CAS gate).
func (s *SQLStore) AtomicTransitionPRStatus(ctx context.Context, repo string, prNumber int, toStatus, notStatus string) (*PRRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}

	query := `
		UPDATE pr_registry
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3 AND status != $4
		RETURNING id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, metadata, created_at, updated_at;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, toStatus, repo, prNumber, notStatus).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No transition occurred (already in terminal state or not found)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute atomic status transition: %w", err)
	}
	return &r, nil
}

// AtomicTransitionPRStatusByMergeSHA conditionally transitions PR status by merge SHA if not already in notStatus (CAS gate).
func (s *SQLStore) AtomicTransitionPRStatusByMergeSHA(ctx context.Context, repo string, mergeSHA string, toStatus, notStatus string) (*PRRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, fmt.Errorf("repo cannot be empty")
	}

	query := `
		UPDATE pr_registry
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND merge_sha = $3 AND status != $4
		RETURNING id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, metadata, created_at, updated_at;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, toStatus, repo, mergeSHA, notStatus).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No transition occurred (already in terminal state or not found)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute atomic status transition by merge_sha: %w", err)
	}
	return &r, nil
}
