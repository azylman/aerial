package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
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
