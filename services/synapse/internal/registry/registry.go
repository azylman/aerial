package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

type PGXQueryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresPRRegistry implements model.PRRegistryUpdater and model.ReadinessRetryStore backed by a pgxpool.Pool.
type PostgresPRRegistry struct {
	pool PGXQueryExecer
}

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
func NewPostgresPRRegistry(pool PGXQueryExecer) *PostgresPRRegistry {
	if typedPool, ok := pool.(*pgxpool.Pool); ok && typedPool == nil {
		pool = nil
	}
	return &PostgresPRRegistry{
		pool: pool,
	}
}

var (
	_ model.PRRegistryUpdater   = (*PostgresPRRegistry)(nil)
	_ model.ReadinessRetryStore = (*PostgresPRRegistry)(nil)
)

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
	repo = model.NormalizeRepo(repo)
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
		slog.InfoContext(ctx, "[synapse] [registry] pr not found in registry (untracked)", "repo", repo, "pr", prNumber)
		return "", nil
	}
	return targetID, nil
}

func (r *PostgresPRRegistry) execAtomicTransition(ctx context.Context, repo string, prNumber int, opName, query string) (string, bool, error) {
	repo = model.NormalizeRepo(repo)
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
	query := `UPDATE pr_registry SET head_sha = $3, status = 'open', metadata = CASE WHEN head_sha IS DISTINCT FROM $3 THEN CASE WHEN COALESCE((metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata') - 'ci_fail_count' ELSE (COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata' END ELSE COALESCE(metadata, '{}'::jsonb) - 'banner' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') AND (head_sha IS DISTINCT FROM $3 OR status IN ('ci_failed', 'conflict') OR COALESCE(metadata, '{}'::jsonb) ? 'banner') RETURNING target_id;`
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
	query := `
		UPDATE pr_registry
		SET status = 'deploy_failed',
		    metadata = CASE
		        WHEN status <> 'deploy_failed' THEN jsonb_set(COALESCE(metadata, '{}'::jsonb), '{deploy_failed_published}', 'false'::jsonb)
		        ELSE metadata
		    END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2
		  AND (
		    status IN ('deploying', 'merged', 'synced')
		    OR (status = 'deploy_failed' AND COALESCE((metadata->>'deploy_failed_published')::boolean, false) = false)
		  )
		RETURNING target_id;`
	return r.execAtomicTransition(ctx, repo, prNumber, "atomic transition deploy_failed", query)
}

func (r *PostgresPRRegistry) RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error) {
	repo = model.NormalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	checkName = strings.TrimSpace(checkName)
	checkState = strings.TrimSpace(checkState)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) || checkName == "" || checkState == "" {
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

func (r *PostgresPRRegistry) RegisterPR(ctx context.Context, req model.PRRegisterRequest) (string, error) {
	if r == nil || r.pool == nil {
		return "", errors.New("pr registry uninitialized")
	}
	repo := model.NormalizeRepo(req.Repo)
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
	targetID := model.NormalizeDiscordTargetID(req.TargetID)
	title := strings.TrimSpace(req.Title)
	meta := strings.TrimSpace(req.Metadata)
	if meta == "" {
		meta = "{}"
	}

	if targetID == "" {
		return "", errors.New("target_id cannot be empty")
	}
	if !model.IsValidDiscordSnowflake(targetID) {
		return "", model.ErrInvalidTargetID
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	upsertQuery := `INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, 'open', $7::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (repo, pr_number) DO UPDATE SET branch = EXCLUDED.branch, head_sha = EXCLUDED.head_sha, target_id = EXCLUDED.target_id, title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, status = CASE WHEN pr_registry.status IN ('merged', 'synced', 'deploying', 'deployed') THEN pr_registry.status ELSE 'open' END, metadata = CASE WHEN EXCLUDED.metadata != '{}'::jsonb THEN (CASE WHEN pr_registry.status NOT IN ('merged', 'synced', 'deploying', 'deployed') THEN CASE WHEN pr_registry.head_sha IS DISTINCT FROM EXCLUDED.head_sha THEN CASE WHEN COALESCE((pr_registry.metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata') - 'ci_fail_count' ELSE (COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) END) || EXCLUDED.metadata ELSE CASE WHEN pr_registry.status NOT IN ('merged', 'synced', 'deploying', 'deployed') THEN CASE WHEN pr_registry.head_sha IS DISTINCT FROM EXCLUDED.head_sha THEN CASE WHEN COALESCE((pr_registry.metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata') - 'ci_fail_count' ELSE (COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner') - 'ci_checks' - 'build_metadata' END ELSE COALESCE(pr_registry.metadata, '{}'::jsonb) - 'banner' END ELSE pr_registry.metadata END END, updated_at = CURRENT_TIMESTAMP;`
	if _, err := r.pool.Exec(qCtx, upsertQuery, repo, req.PRNumber, branch, headSHA, targetID, title, meta); err != nil {
		return "", fmt.Errorf("register pr: %w", err)
	}

	return targetID, nil
}

func (r *PostgresPRRegistry) EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID string) (string, error) {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", nil
	}
	targetID = model.NormalizeDiscordTargetID(targetID)
	if !model.IsValidDiscordSnowflake(targetID) {
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
	repo = model.NormalizeRepo(repo)
	headSHA = strings.TrimSpace(headSHA)
	if repo == "" || headSHA == "" || model.IsZeroSHA(headSHA) {
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
	repo = model.NormalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" || model.IsZeroSHA(sha) {
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
	repo = model.NormalizeRepo(repo)
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
	cleanJobs := model.DedupeTrimmedStrings(jobs)
	if len(cleanJobs) == 0 {
		return "", nil
	}
	query := `
		UPDATE pr_registry
		SET status = 'deploying',
			metadata = jsonb_set(
				jsonb_set((COALESCE(metadata, '{}'::jsonb) - 'banner') - 'deploy_failed_published', '{jobs}', (SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM jsonb_array_elements(COALESCE(metadata->'jobs', '[]'::jsonb) || to_jsonb($3::text[])) AS elem WHERE NOT (COALESCE(metadata->'deployed_jobs', '[]'::jsonb) @> jsonb_build_array(elem)))),
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
	repo = model.NormalizeRepo(repo)
	cleanJobs := model.DedupeTrimmedStrings(jobs)
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
	repo = model.NormalizeRepo(repo)
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

func (r *PostgresPRRegistry) ListDeployingPRs(ctx context.Context) ([]model.DeployingPR, error) {
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		SELECT id, repo, pr_number, branch, head_sha, COALESCE(merge_sha, ''), target_id, COALESCE(metadata, '{}'::jsonb)
		FROM pr_registry
		WHERE status = 'deploying'
		   OR (status = 'deploy_failed' AND CASE WHEN jsonb_typeof(metadata->'kustomization_requests') = 'object' THEN metadata->'kustomization_requests' <> '{}'::jsonb ELSE false END)
		   OR (status = 'deployed' AND COALESCE((metadata->>'deploy_success_pending')::boolean, false) = true)
		   OR (status IN ('merged', 'synced') AND CASE WHEN jsonb_typeof(metadata->'jobs') = 'array' THEN jsonb_array_length(metadata->'jobs') ELSE 0 END > 0)
		ORDER BY id ASC;`
	rows, err := r.pool.Query(qCtx, query)
	if err != nil {
		return nil, fmt.Errorf("list deploying prs: %w", err)
	}
	defer rows.Close()

	var prs []model.DeployingPR
	for rows.Next() {
		var p model.DeployingPR
		var metaBytes []byte
		if err := rows.Scan(&p.ID, &p.Repo, &p.PRNumber, &p.Branch, &p.HeadSHA, &p.MergeSHA, &p.TargetID, &metaBytes); err != nil {
			return nil, fmt.Errorf("scan deploying pr: %w", err)
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &p.Metadata); err != nil {
				slog.WarnContext(ctx, "[synapse] [registry] failed to unmarshal metadata", "repo", p.Repo, "pr", p.PRNumber, "error", err)
			}
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
}

func (r *PostgresPRRegistry) AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error {
	repo = model.NormalizeRepo(repo)
	cleanJobs := model.DedupeTrimmedStrings(jobs)
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

func (r *PostgresPRRegistry) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]any) error {
	repo = model.NormalizeRepo(repo)
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

func (r *PostgresPRRegistry) AcknowledgeDeployFailedPublished(ctx context.Context, repo string, prNumber int) error {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{deploy_failed_published}', 'true'::jsonb),
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2 AND status = 'deploy_failed';`
	if _, err := r.pool.Exec(qCtx, query, repo, prNumber); err != nil {
		return fmt.Errorf("acknowledge deploy failed published: %w", err)
	}
	return nil
}

// RecordPRBuildMetadata atomically fences snapshots by PR revision and workflow
// order. This additive API deliberately does not update PR lifecycle status.
func (r *PostgresPRRegistry) RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata model.BuildMetadata) (bool, error) {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return false, err
	}
	if _, err = model.ParseStoredBuildMetadata(raw); err != nil {
		return false, err
	}
	repo = model.NormalizeRepo(repo)
	if repo != metadata.Repository || prNumber <= 0 || (metadata.PRNumber != nil && *metadata.PRNumber != prNumber) {
		return false, fmt.Errorf("metadata registry identity mismatch")
	}
	if r == nil || r.pool == nil {
		return false, nil
	}
	phase := 1
	if metadata.Kind == model.BuildMetadataFinal {
		phase = 2
	}
	if metadata.Kind == model.BuildMetadataResult {
		return false, fmt.Errorf("result is not a registry snapshot")
	}
	query := `UPDATE pr_registry SET metadata=jsonb_set(COALESCE(metadata,'{}'::jsonb),'{build_metadata}',$4::jsonb), updated_at=CURRENT_TIMESTAMP
 WHERE repo=$1 AND pr_number=$2
 AND ((status IN ('open','ci_failed','conflict') AND head_sha=$3) OR (status IN ('merged','synced') AND merge_sha=$3))
 AND (metadata->'build_metadata' IS NULL OR
 (COALESCE((metadata->'build_metadata'->>'run_number')::bigint,0),COALESCE((metadata->'build_metadata'->>'run_attempt')::int,0)) < ($5::bigint,$6::int) OR
 ((metadata->'build_metadata'->>'run_id')::bigint=$7 AND (metadata->'build_metadata'->>'run_attempt')::int=$6
 AND metadata->'build_metadata'->>'workflow_path'=$8
 AND metadata->'build_metadata'->>'execution_sha'=$9
 AND metadata->'build_metadata'->'selected'=$4::jsonb->'selected'
 AND COALESCE((metadata->'build_metadata'->>'version')::int,0)<=$10
 AND CASE WHEN metadata->'build_metadata'->>'kind'='final' THEN 2 ELSE 1 END <= $11))
 AND (metadata->'build_metadata' IS NULL OR (metadata->'build_metadata'->>'run_id')::bigint <> $7 OR (
 (metadata->'build_metadata'->>'run_number')::bigint=$5
 AND metadata->'build_metadata'->>'source_sha'=$3
 AND metadata->'build_metadata'->>'execution_sha'=$9
 AND metadata->'build_metadata'->>'workflow_path'=$8
 AND metadata->'build_metadata'->'event'=$4::jsonb->'event'
 AND metadata->'build_metadata'->'pr_number'=$4::jsonb->'pr_number'
 AND metadata->'build_metadata'->'selected'=$4::jsonb->'selected'))
 AND metadata->'build_metadata' IS DISTINCT FROM $4::jsonb RETURNING TRUE;`
	var updated bool
	found, err := r.queryRowScan(ctx, "record build metadata", query, []any{&updated}, repo, prNumber, metadata.SourceSHA, raw, metadata.RunNumber, metadata.RunAttempt, metadata.RunID, metadata.WorkflowPath, metadata.ExecutionSHA, metadata.Version, phase)
	return found && updated, err
}

// LoadPRBuildMetadata revalidates identity even after head-change invalidation.
func (r *PostgresPRRegistry) LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (model.BuildMetadata, bool, error) {
	var m model.BuildMetadata
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return m, false, nil
	}
	var raw []byte
	found, err := r.queryRowScan(ctx, "load build metadata", `SELECT metadata->'build_metadata' FROM pr_registry WHERE repo=$1 AND pr_number=$2 AND ((status IN ('open','ci_failed','conflict') AND head_sha=metadata->'build_metadata'->>'source_sha') OR (status IN ('merged','synced') AND merge_sha=metadata->'build_metadata'->>'source_sha'));`, []any{&raw}, repo, prNumber)
	if err != nil || !found || len(raw) == 0 {
		return m, false, err
	}
	m, err = model.ParseStoredBuildMetadata(raw)
	if err != nil {
		return m, false, err
	}
	if m.Repository != repo || (m.PRNumber != nil && *m.PRNumber != prNumber) {
		return m, false, fmt.Errorf("stored metadata identity mismatch")
	}
	return m, true, nil
}

// LoadPRBuildLifecycle returns the PR's status, revisions and whether any
// deployment job has started. A missing PR yields a zero lifecycle.
func (r *PostgresPRRegistry) LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (model.PRBuildLifecycle, error) {
	var state model.PRBuildLifecycle
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return state, nil
	}
	query := `SELECT status, COALESCE(head_sha, ''), COALESCE(merge_sha, ''),
 CASE WHEN jsonb_typeof(metadata->'started_jobs') = 'array' THEN jsonb_array_length(metadata->'started_jobs') > 0 ELSE false END
 FROM pr_registry WHERE repo = $1 AND pr_number = $2;`
	_, err := r.queryRowScan(ctx, "load build lifecycle", query, []any{&state.Status, &state.HeadSHA, &state.MergeSHA, &state.Started}, repo, prNumber)
	return state, err
}

// LoadRawPRMetadata returns the unmarshaled raw metadata jsonb for a PR and whether the row exists.
func (r *PostgresPRRegistry) LoadRawPRMetadata(ctx context.Context, repo string, prNumber int) (map[string]any, bool, error) {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return map[string]any{}, false, nil
	}
	var raw []byte
	found, err := r.queryRowScan(ctx, "load raw pr metadata", `SELECT COALESCE(metadata, '{}'::jsonb) FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, []any{&raw}, repo, prNumber)
	if err != nil || !found {
		return map[string]any{}, false, err
	}
	if len(raw) == 0 {
		return map[string]any{}, true, nil
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, true, fmt.Errorf("load raw pr metadata: %w", err)
	}
	if meta == nil {
		meta = map[string]any{}
	}
	return meta, true, nil
}

// IsPRDeployed reports whether the PR status is deployed (case-insensitive).
func (r *PostgresPRRegistry) IsPRDeployed(ctx context.Context, repo string, prNumber int) (bool, error) {
	repo = model.NormalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return false, nil
	}
	var status string
	found, err := r.queryRowScan(ctx, "is pr deployed", `SELECT status FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, []any{&status}, repo, prNumber)
	if err != nil || !found {
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(status), "deployed"), nil
}
