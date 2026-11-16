package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeployingPR holds metadata for a PR currently undergoing deployment rollout.
type DeployingPR struct {
	ID       int64          `json:"id"`
	Repo     string         `json:"repo"`
	PRNumber int            `json:"pr_number"`
	Branch   string         `json:"branch"`
	HeadSHA  string         `json:"head_sha"`
	MergeSHA string         `json:"merge_sha"`
	TargetID string         `json:"target_id"`
	Metadata map[string]any `json:"metadata"`
}

var ErrInvalidTargetID = errors.New("target_id must be a valid Discord snowflake (17-20 digits)")

// PRRegisterRequest defines the request payload for POST /internal/pr/register.
type PRRegisterRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
	Title    string `json:"title,omitempty"`
	Metadata string `json:"metadata,omitempty"`
}

// PRRegistryUpdater defines the interface for updating PR lifecycle state in PostgreSQL pr_registry.
type PRRegistryUpdater interface {
	RegisterPR(ctx context.Context, req PRRegisterRequest) (string, error)
	UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (targetID string, err error)
	UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (targetID string, err error)
	AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (targetID string, prNumber int, updated bool, err error)
	BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	ResolvePRBySHA(ctx context.Context, repo string, sha string) (prNumber int, branch, targetID string, err error)
	ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (targetID string, branch, headSHA, mergeSHA string, err error)
	AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (targetID string, err error)
	AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (JobTransition, error)
	AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (JobTransition, error)
	ListDeployingPRs(ctx context.Context) ([]DeployingPR, error)
	RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error)
	EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID string) (string, error)
	RecordExpectedJobs(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error)
	HasExpectedDeployJobs(ctx context.Context, repo string, prNumber int) (bool, error)
	RecordPRJobState(ctx context.Context, jobName, jobState string) (repo string, prNumber int, updated bool, err error)
	AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error
	RecordKustomizationRequests(ctx context.Context, repo string, prNumber int, requests map[string]kustomizationRequest) error
	MarkKustomizationsDeployed(ctx context.Context, repo string, prNumber int, names []string) (transitioned bool, err error)
	HasKustomizationRequests(ctx context.Context, repo string, prNumber int) (bool, error)
	UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]any) error
}

type pgxQueryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresPRRegistry implements PRRegistryUpdater backed by a pgxpool.Pool.
type PostgresPRRegistry struct {
	pool pgxQueryExecer
}

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
func NewPostgresPRRegistry(pool pgxQueryExecer) *PostgresPRRegistry {
	if typedPool, ok := pool.(*pgxpool.Pool); ok && typedPool == nil {
		pool = nil
	}
	return &PostgresPRRegistry{
		pool: pool,
	}
}

func (r *PostgresPRRegistry) queryRowScan(ctx context.Context, opName, query string, dest []any, args ...any) (bool, error) {
	if r == nil || r.pool == nil {
		return false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.pool.QueryRow(qCtx, query, args...).Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w", opName, err)
	}
	return true, nil
}

func (r *PostgresPRRegistry) execReturningTargetID(ctx context.Context, repo string, prNumber int, opName, query string, extraArgs ...any) (string, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", nil
	}
	args := append([]any{repo, prNumber}, extraArgs...)
	var targetID string
	found, err := r.queryRowScan(ctx, opName, query, []any{&targetID}, args...)
	if err != nil {
		return "", err
	}
	if !found {
		log.Printf("[synapse] [registry] pr not found in registry (untracked): repo=%s pr=%d", repo, prNumber)
		return "", nil
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) execAtomicTransition(ctx context.Context, repo string, prNumber int, opName, query string) (string, bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", false, nil
	}
	var targetID string
	found, err := r.queryRowScan(ctx, opName, query, []any{&targetID}, repo, prNumber)
	if err != nil {
		return "", false, err
	}
	return targetID, found, nil
}

func (r *PostgresPRRegistry) UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	query := `UPDATE pr_registry SET status = CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, merge_sha = CASE WHEN $3 != '' THEN $3 ELSE merge_sha END, metadata = CASE WHEN status = 'deploy_failed' THEN COALESCE(metadata, '{}'::jsonb) WHEN status IN ('merged', 'synced', 'deploying', 'deployed') THEN COALESCE(metadata, '{}'::jsonb) - 'banner' ELSE COALESCE(metadata, '{}'::jsonb) - 'banner' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr merged", query, strings.TrimSpace(mergeSHA))
}

func (r *PostgresPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	query := `UPDATE pr_registry SET status = 'closed', metadata = COALESCE(metadata, '{}'::jsonb) - 'banner', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr closed unmerged", query)
}

func (r *PostgresPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	query := `UPDATE pr_registry SET head_sha = $3, status = 'open', metadata = CASE WHEN head_sha IS DISTINCT FROM $3 THEN CASE WHEN COALESCE((metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks') - 'ci_fail_count' ELSE (COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' END ELSE COALESCE(metadata, '{}'::jsonb) - 'banner' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') AND (head_sha IS DISTINCT FROM $3 OR status IN ('ci_failed', 'conflict') OR COALESCE(metadata, '{}'::jsonb) ? 'banner') RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr sync", query, strings.TrimSpace(headSHA))
}

func (r *PostgresPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	query := `UPDATE pr_registry SET merge_sha = $3, status = CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, metadata = CASE WHEN status = 'deploy_failed' THEN COALESCE(metadata, '{}'::jsonb) WHEN status IN ('merged', 'synced', 'deploying', 'deployed') THEN COALESCE(metadata, '{}'::jsonb) - 'banner' ELSE COALESCE(metadata, '{}'::jsonb) - 'banner' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "backfill push merge_sha", query, strings.TrimSpace(mergeSHA))
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'ci_failed', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition ci_failed", query)
}

func (r *PostgresPRRegistry) AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'conflict', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('conflict', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition conflict", query)
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (string, bool, error) {
	query := `UPDATE pr_registry SET status = 'deploy_failed', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND status IN ('deploying', 'merged', 'synced') RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition deploy_failed", query)
}

func (r *PostgresPRRegistry) RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error) {
	repo = normalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	checkName = strings.TrimSpace(checkName)
	checkState = strings.TrimSpace(checkState)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) || checkName == "" || checkState == "" {
		return false, nil
	}
	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(
			CASE
				WHEN jsonb_typeof(COALESCE(metadata, '{}'::jsonb)->'ci_checks') = 'object'
				THEN COALESCE(metadata, '{}'::jsonb)
				ELSE jsonb_set(COALESCE(metadata, '{}'::jsonb), '{ci_checks}', '{}'::jsonb, true)
			END,
			ARRAY['ci_checks', $4],
			to_jsonb($5::text),
			true
		),
		updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1
		  AND pr_number = $2
		  AND ((status IN ('open', 'ci_failed', 'conflict') AND head_sha = $3) OR (status IN ('merged', 'synced') AND merge_sha = $3))
		  AND COALESCE(metadata->'ci_checks'->>$4, '') IS DISTINCT FROM $5
		RETURNING TRUE;`
	var updated bool
	found, err := r.queryRowScan(ctx, "record pr check status", query, []any{&updated}, repo, prNumber, headSHA, checkName, checkState)
	if err != nil || !found {
		return false, err
	}
	return updated, nil
}

func (r *PostgresPRRegistry) RegisterPR(ctx context.Context, req PRRegisterRequest) (string, error) {
	if r == nil || r.pool == nil {
		return "", errors.New("pr registry uninitialized")
	}
	repo := normalizeRepo(req.Repo)
	if repo == "" {
		return "", errors.New("repo cannot be empty")
	}
	if req.PRNumber <= 0 {
		return "", errors.New("pr_number must be greater than 0")
	}
	branch := strings.TrimSpace(req.Branch)
	if branch == "" {
		return "", errors.New("branch cannot be empty")
	}
	headSHA := strings.TrimSpace(req.HeadSHA)
	if headSHA == "" {
		return "", errors.New("head_sha cannot be empty")
	}
	targetID := NormalizeDiscordTargetID(req.TargetID)
	title := strings.TrimSpace(req.Title)
	meta := strings.TrimSpace(req.Metadata)
	if meta == "" {
		meta = "{}"
	}

	if targetID == "" {
		return "", errors.New("target_id cannot be empty")
	}
	if !IsValidDiscordSnowflake(targetID) {
		return "", ErrInvalidTargetID
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	upsertQuery := `INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, 'open', $7::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (repo, pr_number) DO UPDATE SET branch = EXCLUDED.branch, head_sha = EXCLUDED.head_sha, target_id = EXCLUDED.target_id, title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, status = CASE WHEN pr_registry.status IN ('merged', 'synced', 'deploying', 'deployed') THEN pr_registry.status ELSE 'open' END, metadata = CASE WHEN EXCLUDED.metadata != '{}'::jsonb THEN (CASE WHEN pr_registry.status NOT IN ('merged', 'synced', 'deploying', 'deployed') THEN CASE WHEN pr_registry.head_sha IS DISTINCT FROM EXCLUDED.head_sha THEN CASE WHEN COALESCE((pr_registry.metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks') - 'ci_fail_count' ELSE (COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) END) || EXCLUDED.metadata ELSE CASE WHEN pr_registry.status NOT IN ('merged', 'synced', 'deploying', 'deployed') THEN CASE WHEN pr_registry.head_sha IS DISTINCT FROM EXCLUDED.head_sha THEN CASE WHEN COALESCE((pr_registry.metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks') - 'ci_fail_count' ELSE (COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner' END ELSE pr_registry.metadata END END, updated_at = CURRENT_TIMESTAMP;`
	if _, err := r.pool.Exec(qCtx, upsertQuery, repo, req.PRNumber, branch, headSHA, targetID, title, meta); err != nil {
		return "", fmt.Errorf("register pr: %w", err)
	}

	return targetID, nil
}

func (r *PostgresPRRegistry) EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID string) (string, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", nil
	}
	targetID = NormalizeDiscordTargetID(targetID)
	if !IsValidDiscordSnowflake(targetID) {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	upsertQuery := `INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, 'open', '{}'::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (repo, pr_number) DO UPDATE SET branch = CASE WHEN EXCLUDED.branch != '' THEN EXCLUDED.branch ELSE pr_registry.branch END, head_sha = CASE WHEN pr_registry.head_sha = '' THEN EXCLUDED.head_sha ELSE pr_registry.head_sha END, target_id = CASE WHEN pr_registry.target_id = '' THEN EXCLUDED.target_id ELSE pr_registry.target_id END, title = CASE WHEN pr_registry.title = '' THEN EXCLUDED.title ELSE pr_registry.title END, metadata = CASE WHEN EXCLUDED.metadata != '{}'::jsonb THEN COALESCE(pr_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata ELSE pr_registry.metadata END, updated_at = CURRENT_TIMESTAMP;`
	if _, err := r.pool.Exec(qCtx, upsertQuery, repo, prNumber, strings.TrimSpace(branch), strings.TrimSpace(headSHA), targetID, strings.TrimSpace(title)); err != nil {
		return "", fmt.Errorf("ensure pr registered: %w", err)
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (string, int, bool, error) {
	repo = normalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	if repo == "" || headSHA == "" || isZeroSHA(headSHA) {
		return "", 0, false, nil
	}

	query := `
		UPDATE pr_registry SET status = 'ci_failed', updated_at = CURRENT_TIMESTAMP
		WHERE id = (SELECT id FROM pr_registry WHERE repo = $1 AND head_sha = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') ORDER BY updated_at DESC LIMIT 1)
		RETURNING target_id, pr_number;`
	var targetID string
	var prNumber int
	found, err := r.queryRowScan(ctx, "atomic transition ci_failed by head_sha", query, []any{&targetID, &prNumber}, repo, headSHA)
	if err != nil {
		return "", 0, false, err
	}
	return targetID, prNumber, found, nil
}

func (r *PostgresPRRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	repo = normalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", "", nil
	}

	query := `SELECT pr_number, branch, target_id FROM pr_registry WHERE repo = $1 AND (merge_sha = $2 OR head_sha = $2) ORDER BY updated_at DESC LIMIT 1;`
	var prNum int
	var branch, targetID string
	found, err := r.queryRowScan(ctx, "resolve pr by sha", query, []any{&prNum, &branch, &targetID}, repo, sha)
	if err != nil {
		return 0, "", "", err
	}
	if !found {
		return 0, "", "", nil
	}
	return prNum, branch, targetID, nil
}

func (r *PostgresPRRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", "", "", "", nil
	}

	query := `SELECT target_id, branch, head_sha, COALESCE(merge_sha, '') FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`
	var targetID, branch, headSHA, mergeSHA string
	found, err := r.queryRowScan(ctx, "resolve pr by number", query, []any{&targetID, &branch, &headSHA, &mergeSHA}, repo, prNumber)
	if err != nil {
		return "", "", "", "", err
	}
	if !found {
		return "", "", "", "", nil
	}
	return targetID, branch, headSHA, mergeSHA, nil
}

func (r *PostgresPRRegistry) TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (string, error) {
	cleanJobs := dedupeTrimmedStrings(jobs)
	if len(cleanJobs) == 0 {
		return "", nil
	}
	query := `
		UPDATE pr_registry
		SET status = 'deploying',
			metadata = jsonb_set(
				jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', (SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || to_jsonb($3::text[])) AS elem WHERE NOT (COALESCE(metadata->'deployed_jobs', '[]'::jsonb) @> jsonb_build_array(elem)))),
				'{started_jobs}',
				(SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM jsonb_array_elements(COALESCE(metadata->'started_jobs', '[]'::jsonb) || to_jsonb($3::text[])) AS elem)
			),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status NOT IN ('closed', 'conflict')
		  AND EXISTS (SELECT 1 FROM jsonb_array_elements(to_jsonb($3::text[])) AS req_elem WHERE NOT (COALESCE(metadata->'deployed_jobs', '[]'::jsonb) @> jsonb_build_array(req_elem)))
		RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "transition deploying", query, cleanJobs)
}

func (r *PostgresPRRegistry) RecordExpectedJobs(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error) {
	repo = normalizeRepo(repo)
	cleanJobs := dedupeTrimmedStrings(jobs)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	if len(cleanJobs) == 0 {
		return r.HasExpectedDeployJobs(ctx, repo, prNumber)
	}
	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{jobs}', (SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || to_jsonb($3::text[])) AS elem WHERE NOT (COALESCE(metadata->'deployed_jobs', '[]'::jsonb) @> jsonb_build_array(elem)))),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status NOT IN ('closed', 'conflict')
		RETURNING TRUE;`
	var hasExpected bool
	found, err := r.queryRowScan(ctx, "record expected jobs", query, []any{&hasExpected}, repo, prNumber, cleanJobs)
	if err != nil || !found {
		return false, err
	}
	return hasExpected, nil
}

func (r *PostgresPRRegistry) HasExpectedDeployJobs(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	query := `
		SELECT (
			CASE WHEN jsonb_typeof(metadata->'jobs') = 'array' THEN jsonb_array_length(metadata->'jobs') ELSE 0 END > 0
			OR CASE WHEN jsonb_typeof(metadata->'deployed_jobs') = 'array' THEN jsonb_array_length(metadata->'deployed_jobs') ELSE 0 END > 0
			OR status IN ('deploying', 'deployed')
		)
		FROM pr_registry
		WHERE repo = $1 AND pr_number = $2
		LIMIT 1;`
	var hasExpected bool
	found, err := r.queryRowScan(ctx, "has expected deploy jobs", query, []any{&hasExpected}, repo, prNumber)
	if err != nil || !found {
		return false, err
	}
	return hasExpected, nil
}

func (r *PostgresPRRegistry) RecordPRJobState(ctx context.Context, jobName, jobState string) (string, int, bool, error) {
	jobName = strings.TrimSpace(jobName)
	jobState = strings.TrimSpace(jobState)
	if r == nil || r.pool == nil || jobName == "" || jobState == "" {
		return "", 0, false, nil
	}
	query := `
		WITH target AS (
			SELECT id
			FROM pr_registry
			WHERE status = 'deploying'
			  AND metadata->'jobs' ? $1
			  AND metadata->'started_jobs' ? $1
			  AND COALESCE(metadata->'job_states'->>$1, '') IS DISTINCT FROM $2
			ORDER BY updated_at DESC
			LIMIT 1
		)
		UPDATE pr_registry
		SET metadata = jsonb_set(
			CASE
				WHEN jsonb_typeof(COALESCE(metadata, '{}'::jsonb)->'job_states') = 'object'
				THEN COALESCE(metadata, '{}'::jsonb)
				ELSE jsonb_set(COALESCE(metadata, '{}'::jsonb), '{job_states}', '{}'::jsonb, true)
			END,
			ARRAY['job_states', $1],
			to_jsonb($2::text),
			true
		),
		updated_at = CURRENT_TIMESTAMP
		FROM target
		WHERE pr_registry.id = target.id
		RETURNING pr_registry.repo, pr_registry.pr_number;`
	var repoOut string
	var prNum int
	found, err := r.queryRowScan(ctx, "record pr job state", query, []any{&repoOut, &prNum}, jobName, jobState)
	if err != nil || !found {
		return "", 0, false, err
	}
	return repoOut, prNum, true, nil
}

// JobTransition is the outcome of a per-job deploy status transition: the PR it
// matched and whether the registry row changed.
type JobTransition struct {
	TargetID string
	PRNumber int
	MergeSHA string
	Repo     string
	Updated  bool
}

func (r *PostgresPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (JobTransition, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return JobTransition{}, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(qCtx)
	if err != nil {
		return JobTransition{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(qCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[synapse] [registry] tx rollback error: %v", rbErr)
		}
	}()

	selectQuery := `
		SELECT id, target_id, pr_number, COALESCE(NULLIF(merge_sha, ''), head_sha), repo, COALESCE(metadata, '{}'::jsonb)
		FROM pr_registry
		WHERE status IN ('deploying', 'merged', 'synced') AND metadata->'jobs' ? $1 AND metadata->'started_jobs' ? $1
		ORDER BY updated_at DESC LIMIT 1 FOR UPDATE`

	var id int64
	var targetID, mergeSHA, repoOut string
	var prNum int
	var metaBytes []byte
	err = tx.QueryRow(qCtx, selectQuery, jobName).Scan(&id, &targetID, &prNum, &mergeSHA, &repoOut, &metaBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobTransition{}, nil
	}
	if err != nil {
		return JobTransition{}, fmt.Errorf("select deploying pr: %w", err)
	}

	var meta map[string]any
	if len(metaBytes) > 0 {
		if err = json.Unmarshal(metaBytes, &meta); err != nil {
			log.Printf("[synapse] [registry] failed to unmarshal metadata for job deploy: %v", err)
		}
	}

	var remaining []string
	for _, j := range extractJobsFromMetadata(meta) {
		if !strings.EqualFold(j, jobName) {
			remaining = append(remaining, j)
		}
	}
	deployedJobs := dedupeTrimmedStrings(append(extractStringSliceFromMetadata(meta, "deployed_jobs"), jobName))

	if len(remaining) > 0 {
		updateQuery := `UPDATE pr_registry SET status = 'deploying', metadata = jsonb_set(jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', to_jsonb($1::text[])), '{deployed_jobs}', to_jsonb($2::text[])), updated_at = CURRENT_TIMESTAMP WHERE id = $3 AND status IN ('deploying', 'merged', 'synced')`
		if _, err = tx.Exec(qCtx, updateQuery, remaining, deployedJobs, id); err != nil {
			return JobTransition{}, fmt.Errorf("update remaining jobs: %w", err)
		}
		if err = tx.Commit(qCtx); err != nil {
			return JobTransition{}, fmt.Errorf("commit remaining jobs tx: %w", err)
		}
		return JobTransition{TargetID: targetID, PRNumber: prNum, MergeSHA: mergeSHA, Repo: repoOut}, nil
	}

	updateQuery := `UPDATE pr_registry SET status = 'deployed', metadata = jsonb_set(jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', '[]'::jsonb), '{deployed_jobs}', to_jsonb($1::text[])), updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status IN ('deploying', 'merged', 'synced')`
	tag, err := tx.Exec(qCtx, updateQuery, deployedJobs, id)
	if err != nil {
		return JobTransition{}, fmt.Errorf("update pr deployed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return JobTransition{}, nil
	}
	if err := tx.Commit(qCtx); err != nil {
		return JobTransition{}, fmt.Errorf("commit pr deployed tx: %w", err)
	}
	return JobTransition{TargetID: targetID, PRNumber: prNum, MergeSHA: mergeSHA, Repo: repoOut, Updated: true}, nil
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (JobTransition, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return JobTransition{}, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	selectQuery := `
		WITH latest AS (
			SELECT id, target_id, pr_number, COALESCE(merge_sha, '') AS merge_sha, repo, status
			FROM pr_registry
			WHERE (status IN ('deploying', 'merged', 'synced', 'deploy_failed') AND metadata->'jobs' ? $1)
			   OR (status = 'deployed' AND metadata->'deployed_jobs' ? $1)
			ORDER BY updated_at DESC, id DESC
			LIMIT 1
		)
		SELECT id, target_id, pr_number, merge_sha, repo
		FROM latest
		WHERE status IN ('deploying', 'merged', 'synced', 'deploy_failed');`
	var id int64
	var targetID, mergeSHA, repoOut string
	var prNum int
	err := r.pool.QueryRow(qCtx, selectQuery, jobName).Scan(&id, &targetID, &prNum, &mergeSHA, &repoOut)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobTransition{}, nil
	}
	if err != nil {
		return JobTransition{}, fmt.Errorf("select deploying pr for failure: %w", err)
	}

	updateQuery := `UPDATE pr_registry SET status = 'deploy_failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status IN ('deploying', 'merged', 'synced');`
	tag, err := r.pool.Exec(qCtx, updateQuery, id)
	if err != nil {
		return JobTransition{}, fmt.Errorf("update pr deploy_failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return JobTransition{TargetID: targetID, PRNumber: prNum, MergeSHA: mergeSHA, Repo: repoOut}, nil
	}
	return JobTransition{TargetID: targetID, PRNumber: prNum, MergeSHA: mergeSHA, Repo: repoOut, Updated: true}, nil
}

func (r *PostgresPRRegistry) ListDeployingPRs(ctx context.Context) ([]DeployingPR, error) {
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, COALESCE(metadata, '{}'::jsonb)
		FROM pr_registry
		WHERE status = 'deploying' OR (status IN ('merged', 'synced') AND CASE WHEN jsonb_typeof(metadata->'jobs') = 'array' THEN jsonb_array_length(metadata->'jobs') ELSE 0 END > 0)
		ORDER BY id ASC;`
	rows, err := r.pool.Query(qCtx, query)
	if err != nil {
		return nil, fmt.Errorf("list deploying prs: %w", err)
	}
	defer rows.Close()

	var prs []DeployingPR
	for rows.Next() {
		var p DeployingPR
		var metaBytes []byte
		if err := rows.Scan(&p.ID, &p.Repo, &p.PRNumber, &p.Branch, &p.HeadSHA, &p.MergeSHA, &p.TargetID, &metaBytes); err != nil {
			return nil, fmt.Errorf("scan deploying pr: %w", err)
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &p.Metadata); err != nil {
				log.Printf("[synapse] [registry] failed to unmarshal metadata for pr %s#%d: %v", p.Repo, p.PRNumber, err)
			}
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
}

func (r *PostgresPRRegistry) AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error {
	repo = normalizeRepo(repo)
	cleanJobs := dedupeTrimmedStrings(jobs)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || len(cleanJobs) == 0 {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(
			jsonb_set(
				COALESCE(metadata, '{}'::jsonb),
				'{jobs}',
				(SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
				 FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || to_jsonb($3::text[])) AS elem)
			),
			'{deployed_jobs}',
			(SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
			 FROM jsonb_array_elements(COALESCE(metadata->'deployed_jobs', '[]'::jsonb)) AS elem
			 WHERE NOT (elem #>> '{}' = ANY($3::text[])))
		),
		updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2;`
	if _, err := r.pool.Exec(qCtx, query, repo, prNumber, cleanJobs); err != nil {
		return fmt.Errorf("append pr jobs: %w", err)
	}
	return nil
}

const (
	kustomizationRequestsKey  = "kustomization_requests"
	deployedKustomizationsKey = "deployed_kustomizations"
)

// kustomizationRequest is the reconcile request recorded for one Flux Kustomization of a PR rollout.
type kustomizationRequest struct {
	Namespace   string `json:"namespace"`
	RequestedAt string `json:"requested_at"`
}

// RecordKustomizationRequests replaces the PR's kustomization_requests wholesale and clears
// deployed_kustomizations so completion is tracked only against the latest reconcile requests.
func (r *PostgresPRRegistry) RecordKustomizationRequests(ctx context.Context, repo string, prNumber int, requests map[string]kustomizationRequest) error {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || len(requests) == 0 {
		return nil
	}
	reqBytes, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("marshal kustomization requests: %w", err)
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb) - '` + deployedKustomizationsKey + `', '{` + kustomizationRequestsKey + `}', $3::jsonb),
			updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2;`
	if _, err := r.pool.Exec(qCtx, query, repo, prNumber, reqBytes); err != nil {
		return fmt.Errorf("record kustomization requests: %w", err)
	}
	return nil
}

// MarkKustomizationsDeployed unions names (those recorded in kustomization_requests) into
// deployed_kustomizations of a deploying PR and flips it to deployed once every recorded Kustomization
// is present. The row lock taken by FOR UPDATE serializes concurrent callers on the latest row version,
// so exactly one caller observes the transition and gets true.
func (r *PostgresPRRegistry) MarkKustomizationsDeployed(ctx context.Context, repo string, prNumber int, names []string) (bool, error) {
	repo = normalizeRepo(repo)
	names = dedupeTrimmedStrings(names)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || len(names) == 0 {
		return false, nil
	}
	query := `
		WITH cur AS (
			SELECT id, metadata AS meta
			FROM pr_registry
			WHERE repo = $1 AND pr_number = $2 AND status = 'deploying'
			  AND jsonb_typeof(metadata->'` + kustomizationRequestsKey + `') = 'object'
			FOR UPDATE
		), next AS (
			SELECT id, meta, (
				SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
				FROM jsonb_array_elements(
					CASE WHEN jsonb_typeof(meta->'` + deployedKustomizationsKey + `') = 'array' THEN meta->'` + deployedKustomizationsKey + `' ELSE '[]'::jsonb END
					|| (SELECT COALESCE(jsonb_agg(n), '[]'::jsonb) FROM unnest($3::text[]) AS n WHERE meta->'` + kustomizationRequestsKey + `' ? n)
				) AS elem
			) AS deployed
			FROM cur
		)
		UPDATE pr_registry p
		SET metadata = jsonb_set(n.meta, '{` + deployedKustomizationsKey + `}', n.deployed, true),
		    status = CASE
		        WHEN NOT EXISTS (SELECT 1 FROM jsonb_object_keys(n.meta->'` + kustomizationRequestsKey + `') AS k WHERE NOT n.deployed ? k) THEN 'deployed'
		        ELSE p.status
		    END,
		    updated_at = CURRENT_TIMESTAMP
		FROM next n
		WHERE p.id = n.id
		RETURNING p.status = 'deployed';`
	var transitioned bool
	if _, err := r.queryRowScan(ctx, "mark kustomizations deployed", query, []any{&transitioned}, repo, prNumber, names); err != nil {
		return false, err
	}
	return transitioned, nil
}

// HasKustomizationRequests reports whether the PR's completion is tracked by the Kustomization watcher.
func (r *PostgresPRRegistry) HasKustomizationRequests(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	query := `SELECT COALESCE(jsonb_typeof(metadata->'` + kustomizationRequestsKey + `') = 'object', false) FROM pr_registry WHERE repo = $1 AND pr_number = $2;`
	var tracked bool
	if _, err := r.queryRowScan(ctx, "has kustomization requests", query, []any{&tracked}, repo, prNumber); err != nil {
		return false, err
	}
	return tracked, nil
}

func (r *PostgresPRRegistry) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]any) error {
	repo = normalizeRepo(repo)
	status = strings.TrimSpace(status)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || status == "" {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var metaBytes []byte
	if metadata != nil {
		var err error
		metaBytes, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}

	query := `
		UPDATE pr_registry
		SET status = $3,
		    metadata = CASE
		        WHEN $4::jsonb IS NOT NULL AND $4::jsonb != '{}'::jsonb
		        THEN COALESCE(metadata, '{}'::jsonb) || $4::jsonb
		        ELSE COALESCE(metadata, '{}'::jsonb)
		    END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2;`
	if _, err := r.pool.Exec(qCtx, query, repo, prNumber, status, metaBytes); err != nil {
		return fmt.Errorf("update pr status: %w", err)
	}
	return nil
}

// extractStringSliceFromMetadata extracts a slice of strings stored under key in metadata JSONB.
func extractStringSliceFromMetadata(meta map[string]any, key string) []string {
	if meta == nil {
		return nil
	}
	var raw []string
	if rawVal, ok := meta[key]; ok {
		switch v := rawVal.(type) {
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					raw = append(raw, s)
				}
			}
		case []string:
			raw = append(raw, v...)
		}
	}
	return dedupeTrimmedStrings(raw)
}

// extractJobsFromMetadata extracts candidate job names stored in the PR registry metadata JSONB.
func extractJobsFromMetadata(meta map[string]any) []string {
	return extractStringSliceFromMetadata(meta, "jobs")
}
