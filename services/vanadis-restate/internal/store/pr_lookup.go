package store

// PR lookup persistence for vanadis-restate.
//
// GetChunkByPR, LookupPRChunk, LookupChunkBuildID,
// LookupUnattachedChunkByThreadAndRepo, findChunkPRMatch, RecordPRRolloutState
// and UpdateChunkBannerAndStatus plus the chunkPRMatch helper moved here from
// the root package so they promote through the embedded *Store. SQL text,
// repo normalization, rollout JSON encoding and error behavior are
// byte-identical to the root copies they replace.
//
// Thread normalization delegates to the canonical internal/model helper.
// ErrPRTransitionSkipped is defined here as the single sentinel for skipped
// lifecycle transitions; the root package aliases it so errors.Is identity
// holds wherever the lookup executes.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// ErrPRTransitionSkipped means the persisted row rejected a stale lifecycle transition.
var ErrPRTransitionSkipped = errors.New("pr lifecycle transition not applied")

type chunkPRMatch struct {
	chunkID      string
	buildID      string
	runnerAgent  string
	chunkIdx     int
	effort       string
	hasRemaining bool
}

func (s *Store) GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	candidates := RepoMatchCandidates(repo)
	if len(candidates) == 0 || prNumber <= 0 {
		return &VanadisChunkPRLookup{Matched: false}, nil
	}
	match, found, err := s.findChunkPRMatch(ctx, candidates, prNumber)
	if err != nil || !found {
		return &VanadisChunkPRLookup{Matched: false}, err
	}
	return &VanadisChunkPRLookup{
		Matched: true,
		ChunkID: match.chunkID,
		BuildID: match.buildID,
		Effort:  CapChunkEffort(match.effort),
	}, nil
}

// LookupPRChunk resolves the build route and chunk that own a repo PR.
func (s *Store) LookupPRChunk(ctx context.Context, repo string, prNumber int) (BuildRoute, string, error) {
	if s == nil || s.db == nil {
		return BuildRoute{}, "", errors.New("database connection is nil")
	}
	candidates := RepoMatchCandidates(repo)
	if len(candidates) == 0 || prNumber <= 0 {
		return BuildRoute{}, "", ErrChunkNotFound
	}
	match, found, err := s.findChunkPRMatch(ctx, candidates, prNumber)
	if err != nil {
		return BuildRoute{}, "", err
	}
	if !found {
		return BuildRoute{}, "", ErrChunkNotFound
	}
	return BuildRoute{BuildID: match.buildID, RunnerAgent: NormalizeRunnerAgent(match.runnerAgent)}, match.chunkID, nil
}

// LookupChunkBuildID resolves the build route that owns a chunk.
func (s *Store) LookupChunkBuildID(ctx context.Context, chunkID string) (BuildRoute, error) {
	if s == nil || s.db == nil {
		return BuildRoute{}, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return BuildRoute{}, errors.New("chunk id cannot be empty")
	}
	var route BuildRoute
	err := s.db.QueryRow(ctx, `SELECT c.build_id, COALESCE(NULLIF(b.runner_agent, ''), 'aerial') FROM vanadis_chunks c JOIN vanadis_builds b ON b.id = c.build_id WHERE c.id = $1;`, chunkID).Scan(&route.BuildID, &route.RunnerAgent)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BuildRoute{}, ErrChunkNotFound
		}
		return BuildRoute{}, fmt.Errorf("lookup chunk build id: %w", err)
	}
	route.RunnerAgent = NormalizeRunnerAgent(route.RunnerAgent)
	return route, nil
}

// LookupUnattachedChunkByThreadAndRepo resolves the single running unattached
// PR chunk for a thread and repo, with its build route.
func (s *Store) LookupUnattachedChunkByThreadAndRepo(ctx context.Context, threadID, repo string) (BuildRoute, string, error) {
	if s == nil || s.db == nil {
		return BuildRoute{}, "", errors.New("database connection is nil")
	}
	threadID = model.NormalizeDiscordThreadID(threadID)
	candidates := RepoMatchCandidates(repo)
	if threadID == "" || len(candidates) == 0 {
		return BuildRoute{}, "", ErrChunkNotFound
	}
	const query = `SELECT c.build_id, c.id, COALESCE(NULLIF(b.runner_agent, ''), 'aerial') FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE b.thread_id = $1 AND b.status != 'cancelled' AND c.mode = 'pr' AND c.repo = ANY($2) AND COALESCE(c.pr_number, 0) = 0 AND c.status IN ('running', 'coding') AND c.started_at IS NOT NULL ORDER BY c.chunk_index ASC LIMIT 2;`
	rows, err := s.db.Query(ctx, query, threadID, candidates)
	if err != nil {
		return BuildRoute{}, "", fmt.Errorf("lookup unattached chunk by thread %s and repo %s: %w", threadID, repo, err)
	}
	defer rows.Close()

	type chunkMatch struct {
		buildID string
		chunkID string
		runner  string
	}
	var matches []chunkMatch
	for rows.Next() {
		var m chunkMatch
		if err := rows.Scan(&m.buildID, &m.chunkID, &m.runner); err != nil {
			return BuildRoute{}, "", fmt.Errorf("scan unattached chunk: %w", err)
		}
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return BuildRoute{}, "", fmt.Errorf("iterate unattached chunks: %w", err)
	}
	if len(matches) == 1 {
		return BuildRoute{BuildID: matches[0].buildID, RunnerAgent: NormalizeRunnerAgent(matches[0].runner)}, matches[0].chunkID, nil
	}
	return BuildRoute{}, "", ErrChunkNotFound
}

func (s *Store) findChunkPRMatch(ctx context.Context, candidates []string, prNumber int) (chunkPRMatch, bool, error) {
	const query = `SELECT c.id, c.build_id, c.chunk_index, COALESCE(NULLIF(c.effort, ''), 'medium'), EXISTS (SELECT 1 FROM vanadis_chunks c2 WHERE c2.build_id = c.build_id AND c2.chunk_index > c.chunk_index), COALESCE(NULLIF(b.runner_agent, ''), 'aerial') FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.repo = ANY($1) AND c.pr_number = $2 AND b.status != 'cancelled' ORDER BY c.updated_at DESC LIMIT 1;`
	var m chunkPRMatch
	err := s.db.QueryRow(ctx, query, candidates, prNumber).Scan(&m.chunkID, &m.buildID, &m.chunkIdx, &m.effort, &m.hasRemaining, &m.runnerAgent)
	if errors.Is(err, pgx.ErrNoRows) {
		return chunkPRMatch{}, false, nil
	}
	if err != nil {
		return chunkPRMatch{}, false, fmt.Errorf("query chunk by pr candidates %v #%d: %w", candidates, prNumber, err)
	}
	return m, true, nil
}

func (s *Store) RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error {
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

func (s *Store) UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks AS c SET status = CASE WHEN $1 != '' THEN $1 ELSE c.status END, banner = $2, updated_at = CURRENT_TIMESTAMP FROM vanadis_builds AS b WHERE c.id = $3 AND b.id = c.build_id AND c.status NOT IN ('completed', 'failed', 'cancelled') AND b.status != 'cancelled' AND ($1 NOT IN ('conflict', 'ci_failed') OR c.status NOT IN ('merged', 'synced', 'deploying', 'deployed', 'deploy_failed'));`, strings.TrimSpace(status), strings.TrimSpace(banner), id)
	switch {
	case err != nil:
		return fmt.Errorf("update chunk banner and status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrPRTransitionSkipped
	default:
		return nil
	}
}
