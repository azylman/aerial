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

func validateAndNormalizeUpsertPR(record PRRecord) (PRRecord, error) {
	repo := normalizeRepo(record.Repo)
	if repo == "" {
		return PRRecord{}, fmt.Errorf("repo cannot be empty")
	}
	if record.PRNumber <= 0 {
		return PRRecord{}, fmt.Errorf("pr_number must be greater than 0")
	}
	branch := strings.TrimSpace(record.Branch)
	if branch == "" {
		return PRRecord{}, fmt.Errorf("branch cannot be empty")
	}
	headSHA := strings.TrimSpace(record.HeadSHA)
	if headSHA == "" {
		return PRRecord{}, fmt.Errorf("head_sha cannot be empty")
	}
	targetID := strings.TrimSpace(record.TargetID)
	if targetID == "" {
		return PRRecord{}, fmt.Errorf("target_id cannot be empty")
	}

	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = "open"
	}
	statusMessageID := strings.TrimSpace(record.StatusMessageID)
	meta := strings.TrimSpace(record.Metadata)
	if meta == "" {
		meta = "{}"
	}

	norm := record
	norm.Repo = repo
	norm.Branch = branch
	norm.HeadSHA = headSHA
	norm.TargetID = targetID
	norm.Status = status
	norm.StatusMessageID = statusMessageID
	norm.Metadata = meta
	return norm, nil
}

// UpsertPR inserts or updates a Pull Request tracking record.
func (s *SQLStore) UpsertPR(ctx context.Context, record PRRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	norm, err := validateAndNormalizeUpsertPR(record)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, status_message_id, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (repo, pr_number) DO UPDATE SET
			branch = EXCLUDED.branch,
			head_sha = EXCLUDED.head_sha,
			target_id = EXCLUDED.target_id,
			title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END,
			status = CASE WHEN pr_registry.status IN ('merged', 'deploying', 'deployed') THEN pr_registry.status WHEN EXCLUDED.status != '' THEN EXCLUDED.status ELSE pr_registry.status END,
			status_message_id = CASE WHEN EXCLUDED.status_message_id != '' THEN EXCLUDED.status_message_id ELSE pr_registry.status_message_id END,
			metadata = CASE WHEN EXCLUDED.metadata != '{}' THEN EXCLUDED.metadata ELSE pr_registry.metadata END,
			updated_at = CURRENT_TIMESTAMP;
	`

	_, err = s.db.ExecContext(ctx, query, norm.Repo, norm.PRNumber, norm.Branch, norm.HeadSHA, norm.TargetID, norm.Title, norm.Status, norm.StatusMessageID, norm.Metadata)
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
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, status, title, status_message_id, metadata, created_at, updated_at
		FROM pr_registry
		WHERE repo = $1 AND pr_number = $2;
	`

	var r PRRecord
	err := s.db.QueryRowContext(ctx, query, repo, prNumber).Scan(
		&r.ID, &r.Repo, &r.PRNumber, &r.Branch, &r.HeadSHA, &r.MergeSHA, &r.TargetID, &r.Status, &r.Title, &r.StatusMessageID, &r.Metadata, &r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPRNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query pr record: %w", err)
	}
	return &r, nil
}

// UpdatePRStatusMessageID updates the status_message_id of a PR in pr_registry.
func (s *SQLStore) UpdatePRStatusMessageID(ctx context.Context, repo string, prNumber int, messageID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	repo = normalizeRepo(repo)
	if repo == "" {
		return fmt.Errorf("repo cannot be empty")
	}

	query := `
		UPDATE pr_registry
		SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP
		WHERE repo = $2 AND pr_number = $3;
	`

	res, err := s.db.ExecContext(ctx, query, strings.TrimSpace(messageID), repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to update pr status_message_id: %w", err)
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
