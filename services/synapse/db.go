package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisteredPR holds basic identification for an active pull request.
type RegisteredPR struct {
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	TargetID string `json:"target_id"`
	HeadSHA  string `json:"head_sha"`
}

// DeployingPR holds metadata for a PR currently undergoing deployment rollout.
type DeployingPR struct {
	ID       int64                  `json:"id"`
	Repo     string                 `json:"repo"`
	PRNumber int                    `json:"pr_number"`
	Branch   string                 `json:"branch"`
	HeadSHA  string                 `json:"head_sha"`
	MergeSHA string                 `json:"merge_sha"`
	TargetID string                 `json:"target_id"`
	Metadata map[string]interface{} `json:"metadata"`
}

// VanadisChunkSummary holds condensed status metadata for a single chunk in a Vanadis build.
type VanadisChunkSummary struct {
	ID          string    `json:"id"`
	ChunkIndex  int       `json:"chunk_index"`
	Repo        string    `json:"repo,omitempty"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

// VanadisAdvancementResult describes the outcome of advancing a Vanadis build chunk upon PR merge.
type VanadisAdvancementResult struct {
	Matched          bool                  `json:"matched"`
	Advanced         bool                  `json:"advanced"`
	BuildID          string                `json:"build_id"`
	ThreadID         string                `json:"thread_id"`
	Tier             int                   `json:"tier"`
	Title            string                `json:"title"`
	Objective        string                `json:"objective"`
	BuildStatus      string                `json:"build_status"`
	StatusMessageID  string                `json:"status_message_id,omitempty"`
	MergedChunkIndex int                   `json:"merged_chunk_index"`
	MergedChunkTitle string                `json:"merged_chunk_title"`
	NextChunkIndex   int                   `json:"next_chunk_index"`
	BuildCompleted   bool                  `json:"build_completed"`
	Chunks           []VanadisChunkSummary `json:"chunks,omitempty"`
}

var (
	ErrVanadisChunkNotFound = errors.New("vanadis chunk not found")
	ErrInvalidTargetID      = errors.New("target_id must be a valid Discord snowflake (17-20 digits)")
)

// PRRegisterRequest defines the request payload for POST /internal/pr/register.
type PRRegisterRequest struct {
	Repo           string `json:"repo"`
	PRNumber       int    `json:"pr_number"`
	Branch         string `json:"branch"`
	HeadSHA        string `json:"head_sha"`
	TargetID       string `json:"target_id"`
	Title          string `json:"title,omitempty"`
	Metadata       string `json:"metadata,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
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
	ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error)
	AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (targetID string, err error)
	AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (targetID string, prNumber int, mergeSHA, repo string, updated bool, err error)
	AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (targetID string, prNumber int, mergeSHA, repo string, updated bool, err error)
	ListDeployingPRs(ctx context.Context) ([]DeployingPR, error)
	AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error)
	ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error)
	IsFinalVanadisChunk(ctx context.Context, repo string, prNumber int) (bool, error)
	FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error)
	FailVanadisChunkDetailsByPR(ctx context.Context, repo string, prNumber int) (buildID, chunkID string, failed bool, err error)
	IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error)
	RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error)
	EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID, vanadisChunkID string) (string, error)
	RecordExpectedJobs(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error)
	HasExpectedDeployJobs(ctx context.Context, repo string, prNumber int) (bool, error)
	RecordPRJobState(ctx context.Context, jobName, jobState string) (repo string, prNumber int, updated bool, err error)
	CheckEventTriggers(ctx context.Context, source string, targets []string, eventType string) (bool, error)
	FireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) ([]ClaimedEventTrigger, error)
	AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error
	UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]interface{}) error
}

type pgxQueryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresPRRegistry implements PRRegistryUpdater backed by a pgxpool.Pool.
type PostgresPRRegistry struct {
	pool              pgxQueryExecer
	vanadisURL        string
	haroURL           string
	httpClient        *http.Client
	vanadisRetryDelay time.Duration
	vanadisRetrySleep func(ctx context.Context, d time.Duration) error
}

// NewPostgresPRRegistry constructs a new PostgresPRRegistry.
func NewPostgresPRRegistry(pool pgxQueryExecer, opts ...func(*PostgresPRRegistry)) *PostgresPRRegistry {
	if typedPool, ok := pool.(*pgxpool.Pool); ok && typedPool == nil {
		pool = nil
	}
	r := &PostgresPRRegistry{
		pool:              pool,
		vanadisURL:        "http://vanadis.aerial",
		haroURL:           "http://haro.aerial",
		httpClient:        &http.Client{Timeout: 10 * time.Second},
		vanadisRetryDelay: 500 * time.Millisecond,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

// WithVanadisClient configures the Vanadis URL and HTTP client for PostgresPRRegistry.
func WithVanadisClient(vanadisURL string, client *http.Client) func(*PostgresPRRegistry) {
	return func(r *PostgresPRRegistry) {
		if r == nil {
			return
		}
		if vanadisURL != "" {
			r.vanadisURL = strings.TrimRight(vanadisURL, "/")
		}
		if client != nil {
			r.httpClient = client
		}
	}
}

func (r *PostgresPRRegistry) getVanadisClient() (*http.Client, string) {
	if r == nil {
		return &http.Client{Timeout: 10 * time.Second}, "http://vanadis.aerial"
	}
	client := r.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	vURL := strings.TrimRight(r.vanadisURL, "/")
	if vURL == "" {
		vURL = "http://vanadis.aerial"
	}
	return client, vURL
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
	query := `UPDATE pr_registry SET status = CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, merge_sha = CASE WHEN $3 != '' THEN $3 ELSE merge_sha END, metadata = CASE WHEN status = 'deploy_failed' THEN COALESCE(metadata, '{}'::jsonb) WHEN status IN ('merged', 'synced', 'deploying', 'deployed') THEN COALESCE(metadata, '{}'::jsonb) - 'banner' ELSE (COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr merged", query, strings.TrimSpace(mergeSHA))
}

func (r *PostgresPRRegistry) UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (string, error) {
	query := `UPDATE pr_registry SET status = 'closed', metadata = COALESCE(metadata, '{}'::jsonb) - 'banner', updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr closed unmerged", query)
}

func (r *PostgresPRRegistry) UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	query := `UPDATE pr_registry SET head_sha = $3, status = 'open', metadata = CASE WHEN head_sha IS DISTINCT FROM $3 THEN (COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' ELSE COALESCE(metadata, '{}'::jsonb) - 'banner' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed') AND (head_sha IS DISTINCT FROM $3 OR status IN ('ci_failed', 'conflict')) RETURNING target_id;`
	return r.execReturningTargetID(ctx, repo, prNumber, "update pr sync", query, strings.TrimSpace(headSHA))
}

func (r *PostgresPRRegistry) BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (string, error) {
	query := `UPDATE pr_registry SET merge_sha = $3, status = CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END, metadata = CASE WHEN status = 'deploy_failed' THEN COALESCE(metadata, '{}'::jsonb) WHEN status IN ('merged', 'synced', 'deploying', 'deployed') THEN COALESCE(metadata, '{}'::jsonb) - 'banner' ELSE (COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks' END, updated_at = CURRENT_TIMESTAMP WHERE repo = $1 AND pr_number = $2 RETURNING target_id;`
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

func (r *PostgresPRRegistry) IncrementCIFailCount(ctx context.Context, repo string, prNumber int) (int, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return 0, nil
	}
	query := `
		UPDATE pr_registry
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{ci_fail_count}', to_jsonb(COALESCE((metadata->>'ci_fail_count')::int, 0) + 1), true),
		    updated_at = CURRENT_TIMESTAMP
		WHERE repo = $1 AND pr_number = $2
		RETURNING (metadata->>'ci_fail_count')::int;`
	var count int
	found, err := r.queryRowScan(ctx, "increment ci_fail_count", query, []any{&count}, repo, prNumber)
	if err != nil || !found {
		return 0, err
	}
	return count, nil
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
		  AND ((status = 'open' AND head_sha = $3) OR (status IN ('merged', 'synced') AND merge_sha = $3))
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
	chunkID := strings.TrimSpace(req.VanadisChunkID)
	meta := strings.TrimSpace(req.Metadata)
	if meta == "" {
		meta = "{}"
	}

	if chunkID != "" {
		threadID, err := r.attachVanadisChunkPR(ctx, repo, chunkID, req.PRNumber)
		if err != nil {
			return "", err
		}
		if normThread := NormalizeDiscordTargetID(threadID); IsValidDiscordSnowflake(normThread) {
			targetID = normThread
		}
	}

	if targetID == "" {
		return "", errors.New("target_id cannot be empty")
	}
	if !IsValidDiscordSnowflake(targetID) {
		return "", ErrInvalidTargetID
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	upsertQuery := `INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, 'open', CASE WHEN $8 != '' THEN jsonb_set($7::jsonb, '{vanadis_chunk_id}', to_jsonb($8::text), true) ELSE $7::jsonb END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (repo, pr_number) DO UPDATE SET branch = EXCLUDED.branch, head_sha = EXCLUDED.head_sha, target_id = EXCLUDED.target_id, title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE pr_registry.title END, status = CASE WHEN pr_registry.status IN ('merged', 'synced', 'deploying', 'deployed') THEN pr_registry.status ELSE 'open' END, metadata = CASE WHEN EXCLUDED.metadata != '{}'::jsonb THEN COALESCE(pr_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata ELSE pr_registry.metadata END, updated_at = CURRENT_TIMESTAMP;`
	if _, err := r.pool.Exec(qCtx, upsertQuery, repo, req.PRNumber, branch, headSHA, targetID, title, meta, chunkID); err != nil {
		return "", fmt.Errorf("register pr: %w", err)
	}

	return targetID, nil
}

func (r *PostgresPRRegistry) attachVanadisChunkPR(ctx context.Context, repo, chunkID string, prNumber int) (string, error) {
	client, vURL := r.getVanadisClient()
	bodyBytes, err := json.Marshal(map[string]any{
		"chunk_id":  chunkID,
		"pr_number": prNumber,
		"repo":      repo,
	})
	if err != nil {
		return "", fmt.Errorf("marshal attach chunk request: %w", err)
	}
	var attachRes struct {
		ThreadID string `json:"thread_id"`
	}
	err = r.doHTTPWithRetry(ctx, client, "attach vanadis chunk pr", func() (*http.Request, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, vURL+"/chunks/attach-pr", bytes.NewReader(bodyBytes))
		if reqErr != nil {
			return nil, reqErr
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, func(resp *http.Response) error {
		if resp.StatusCode == http.StatusNotFound {
			return ErrVanadisChunkNotFound
		}
		if resp.StatusCode != http.StatusOK {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if readErr != nil {
				return fmt.Errorf("attach vanadis chunk pr: unexpected status %d (read error: %w)", resp.StatusCode, readErr)
			}
			return fmt.Errorf("attach vanadis chunk pr: unexpected status %d: %s", resp.StatusCode, string(body))
		}
		if err := json.NewDecoder(resp.Body).Decode(&attachRes); err != nil {
			return fmt.Errorf("decode attach chunk response: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(attachRes.ThreadID), nil
}

func (r *PostgresPRRegistry) EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID, vanadisChunkID string) (string, error) {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return "", nil
	}
	targetID, vanadisChunkID = NormalizeDiscordTargetID(targetID), strings.TrimSpace(vanadisChunkID)
	if vanadisChunkID != "" {
		threadID, err := r.attachVanadisChunkPR(ctx, repo, vanadisChunkID, prNumber)
		if err == nil {
			if normThread := NormalizeDiscordTargetID(threadID); IsValidDiscordSnowflake(normThread) {
				targetID = normThread
			}
		} else if !errors.Is(err, ErrVanadisChunkNotFound) {
			return "", err
		}
	}
	if !IsValidDiscordSnowflake(targetID) {
		return "", nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	upsertQuery := `INSERT INTO pr_registry (repo, pr_number, branch, head_sha, target_id, title, status, metadata, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, 'open', CASE WHEN $7 != '' THEN jsonb_build_object('vanadis_chunk_id', $7::text) ELSE '{}'::jsonb END, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (repo, pr_number) DO UPDATE SET branch = CASE WHEN EXCLUDED.branch != '' THEN EXCLUDED.branch ELSE pr_registry.branch END, head_sha = CASE WHEN pr_registry.head_sha = '' THEN EXCLUDED.head_sha ELSE pr_registry.head_sha END, target_id = CASE WHEN pr_registry.target_id = '' THEN EXCLUDED.target_id ELSE pr_registry.target_id END, title = CASE WHEN pr_registry.title = '' THEN EXCLUDED.title ELSE pr_registry.title END, metadata = CASE WHEN EXCLUDED.metadata != '{}'::jsonb THEN COALESCE(pr_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata ELSE pr_registry.metadata END, updated_at = CURRENT_TIMESTAMP;`
	if _, err := r.pool.Exec(qCtx, upsertQuery, repo, prNumber, strings.TrimSpace(branch), strings.TrimSpace(headSHA), targetID, strings.TrimSpace(title), vanadisChunkID); err != nil {
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

func (r *PostgresPRRegistry) ListOpenPRs(ctx context.Context, repo string) ([]RegisteredPR, error) {
	repo = normalizeRepo(repo)
	if repo == "" {
		return nil, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT pr_number, branch, target_id, head_sha FROM pr_registry WHERE repo = $1 AND COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'synced', 'closed', 'conflict', 'deploying', 'deployed', 'deploy_failed') ORDER BY pr_number ASC;`
	rows, err := r.pool.Query(qCtx, query, repo)
	if err != nil {
		return nil, fmt.Errorf("list open prs: %w", err)
	}
	defer rows.Close()

	var prs []RegisteredPR
	for rows.Next() {
		var p RegisteredPR
		if err := rows.Scan(&p.PRNumber, &p.Branch, &p.TargetID, &p.HeadSHA); err != nil {
			return nil, fmt.Errorf("scan open pr: %w", err)
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
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

func (r *PostgresPRRegistry) AtomicTransitionDeployedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "", 0, "", "", false, nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(qCtx)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("begin tx: %w", err)
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
		return "", 0, "", "", false, nil
	}
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("select deploying pr: %w", err)
	}

	var meta map[string]interface{}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
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
		if _, err := tx.Exec(qCtx, updateQuery, remaining, deployedJobs, id); err != nil {
			return "", 0, "", "", false, fmt.Errorf("update remaining jobs: %w", err)
		}
		if err := tx.Commit(qCtx); err != nil {
			return "", 0, "", "", false, fmt.Errorf("commit remaining jobs tx: %w", err)
		}
		return targetID, prNum, mergeSHA, repoOut, false, nil
	}

	updateQuery := `UPDATE pr_registry SET status = 'deployed', metadata = jsonb_set(jsonb_set(COALESCE(metadata, '{}'::jsonb) - 'banner', '{jobs}', '[]'::jsonb), '{deployed_jobs}', to_jsonb($1::text[])), updated_at = CURRENT_TIMESTAMP WHERE id = $2 AND status IN ('deploying', 'merged', 'synced')`
	tag, err := tx.Exec(qCtx, updateQuery, deployedJobs, id)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("update pr deployed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", 0, "", "", false, nil
	}
	if err := tx.Commit(qCtx); err != nil {
		return "", 0, "", "", false, fmt.Errorf("commit pr deployed tx: %w", err)
	}
	return targetID, prNum, mergeSHA, repoOut, true, nil
}

func (r *PostgresPRRegistry) AtomicTransitionDeployFailedByJob(ctx context.Context, jobName string) (string, int, string, string, bool, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return "", 0, "", "", false, nil
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
		return "", 0, "", "", false, nil
	}
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("select deploying pr for failure: %w", err)
	}

	updateQuery := `UPDATE pr_registry SET status = 'deploy_failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND status IN ('deploying', 'merged', 'synced');`
	tag, err := r.pool.Exec(qCtx, updateQuery, id)
	if err != nil {
		return "", 0, "", "", false, fmt.Errorf("update pr deploy_failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return targetID, prNum, mergeSHA, repoOut, false, nil
	}
	return targetID, prNum, mergeSHA, repoOut, true, nil
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

func (r *PostgresPRRegistry) AdvanceVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error) {
	repo = normalizeRepo(repo)
	if r == nil || repo == "" || prNumber <= 0 {
		return &VanadisAdvancementResult{Matched: false}, nil
	}
	client, vURL := r.getVanadisClient()
	bodyBytes, err := json.Marshal(map[string]any{
		"repo":      repo,
		"pr_number": prNumber,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal advance chunk request: %w", err)
	}
	var res VanadisAdvancementResult
	err = r.doHTTPWithRetry(ctx, client, "advance vanadis chunk by pr", func() (*http.Request, error) {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, vURL+"/chunks/advance-pr", bytes.NewReader(bodyBytes))
		if reqErr != nil {
			return nil, reqErr
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if readErr != nil {
				return fmt.Errorf("advance vanadis chunk by pr: unexpected status %d (read error: %w)", resp.StatusCode, readErr)
			}
			return fmt.Errorf("advance vanadis chunk by pr: unexpected status %d: %s", resp.StatusCode, string(body))
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return fmt.Errorf("decode advance chunk response: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

type vanadisChunkPRLookup struct {
	Matched            bool   `json:"matched"`
	ChunkID            string `json:"chunk_id,omitempty"`
	BuildID            string `json:"build_id,omitempty"`
	Effort             string `json:"effort,omitempty"`
	HasRemainingChunks bool   `json:"has_remaining_chunks"`
}

func isTransientVanadisStatus(code int) bool {
	return code == http.StatusInternalServerError ||
		code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusGatewayTimeout ||
		code == http.StatusTooManyRequests
}

func (r *PostgresPRRegistry) sleepRetry(ctx context.Context, delay time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if r != nil && r.vanadisRetrySleep != nil {
		return r.vanadisRetrySleep(ctx, delay)
	}
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *PostgresPRRegistry) doHTTPWithRetry(ctx context.Context, client *http.Client, opName string, newReq func() (*http.Request, error), handleResp func(resp *http.Response) error) error {
	const maxAttempts = 3
	delay := 500 * time.Millisecond
	if r != nil && r.vanadisRetryDelay >= 0 {
		delay = r.vanadisRetryDelay
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		httpReq, err := newReq()
		if err != nil {
			return fmt.Errorf("create %s request: %w", opName, err)
		}
		resp, err := client.Do(httpReq)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("%s: %w", opName, err)
			if attempt+1 >= maxAttempts {
				return lastErr
			}
			if sleepErr := r.sleepRetry(ctx, delay); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		if isTransientVanadisStatus(resp.StatusCode) {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			if readErr != nil {
				lastErr = fmt.Errorf("%s: unexpected status %d (read error: %w)", opName, resp.StatusCode, readErr)
			} else {
				lastErr = fmt.Errorf("%s: unexpected status %d: %s", opName, resp.StatusCode, string(body))
			}
			if attempt+1 >= maxAttempts {
				return lastErr
			}
			if sleepErr := r.sleepRetry(ctx, delay); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		err = handleResp(resp)
		resp.Body.Close()
		return err
	}
	return lastErr
}

func (r *PostgresPRRegistry) getChunkByPR(ctx context.Context, repo string, prNumber int) (*vanadisChunkPRLookup, error) {
	repo = normalizeRepo(repo)
	if r == nil || repo == "" || prNumber <= 0 {
		return &vanadisChunkPRLookup{Matched: false}, nil
	}
	client, vURL := r.getVanadisClient()
	endpoint := fmt.Sprintf("%s/chunks/by-pr?repo=%s&pr_number=%d", vURL, url.QueryEscape(repo), prNumber)
	var lookup vanadisChunkPRLookup
	err := r.doHTTPWithRetry(ctx, client, "get chunk by pr", func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	}, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if readErr != nil {
				return fmt.Errorf("get chunk by pr: unexpected status %d (read error: %w)", resp.StatusCode, readErr)
			}
			return fmt.Errorf("get chunk by pr: unexpected status %d: %s", resp.StatusCode, string(body))
		}
		if err := json.NewDecoder(resp.Body).Decode(&lookup); err != nil {
			return fmt.Errorf("decode get chunk by pr response: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &lookup, nil
}

func (r *PostgresPRRegistry) ResolveVanadisChunkForPR(ctx context.Context, repo string, prNumber int) (bool, string, string, error) {
	lookup, err := r.getChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return false, "", "", err
	}
	if lookup == nil || !lookup.Matched {
		return false, "", "", nil
	}
	cleanEffort := "medium"
	if strings.EqualFold(strings.TrimSpace(lookup.Effort), "low") {
		cleanEffort = "low"
	}
	return true, strings.TrimSpace(lookup.ChunkID), cleanEffort, nil
}

func (r *PostgresPRRegistry) getPersistedVanadisChunkID(ctx context.Context, repo string, prNumber int) string {
	repo = normalizeRepo(repo)
	if r == nil || r.pool == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	const query = `SELECT COALESCE(metadata->>'vanadis_chunk_id', '') FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`
	var chunkID string
	found, err := r.queryRowScan(ctx, "get persisted vanadis_chunk_id", query, []any{&chunkID}, repo, prNumber)
	if err != nil || !found {
		return ""
	}
	return strings.TrimSpace(chunkID)
}

func (r *PostgresPRRegistry) hasPersistedVanadisChunkID(ctx context.Context, repo string, prNumber int) bool {
	return r.getPersistedVanadisChunkID(ctx, repo, prNumber) != ""
}

func (r *PostgresPRRegistry) ShouldSuppressVanadisDeployWake(ctx context.Context, repo string, prNumber int) (bool, error) {
	lookup, err := r.getChunkByPR(ctx, repo, prNumber)
	if err != nil {
		if r.hasPersistedVanadisChunkID(ctx, repo, prNumber) {
			return true, nil
		}
		return false, err
	}
	if lookup == nil || !lookup.Matched {
		return false, nil
	}
	return true, nil
}

func (r *PostgresPRRegistry) IsFinalVanadisChunk(ctx context.Context, repo string, prNumber int) (bool, error) {
	lookup, err := r.getChunkByPR(ctx, repo, prNumber)
	if err != nil {
		return false, err
	}
	if lookup == nil || !lookup.Matched {
		return false, nil
	}
	return !lookup.HasRemainingChunks, nil
}

func (r *PostgresPRRegistry) FailVanadisChunkByPR(ctx context.Context, repo string, prNumber int) (bool, error) {
	_, _, failed, err := r.FailVanadisChunkDetailsByPR(ctx, repo, prNumber)
	return failed, err
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

func (r *PostgresPRRegistry) UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]interface{}) error {
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
