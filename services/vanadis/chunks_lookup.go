package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const defaultRepoOwner = "azylman"

func repoMatchCandidates(repo string) []string {
	repo = normalizeRepo(repo)
	if repo == "" {
		return nil
	}
	if !strings.Contains(repo, "/") {
		return []string{repo, defaultRepoOwner + "/" + repo}
	}
	if bare, ok := strings.CutPrefix(repo, defaultRepoOwner+"/"); ok && bare != "" && !strings.Contains(bare, "/") {
		return []string{repo, bare}
	}
	return []string{repo}
}

// chunkPRMatch is the newest non-cancelled chunk tied to a repo and PR number.
type chunkPRMatch struct {
	chunkID, buildID, effort string
	chunkIdx                 int
	hasRemaining             bool
}

// GetChunkByPR looks up a chunk and its parent build for a given repo and pr_number.
func (s *PostgresVanadisStore) GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	candidates := repoMatchCandidates(repo)
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
		Effort:  capChunkEffort(match.effort),
	}, nil
}

func (s *PostgresVanadisStore) findChunkPRMatch(ctx context.Context, candidates []string, prNumber int) (chunkPRMatch, bool, error) {
	const query = `
		SELECT c.id, c.build_id, c.chunk_index, COALESCE(NULLIF(c.effort, ''), 'medium'),
		       EXISTS (
		           SELECT 1 FROM vanadis_chunks c2
		           WHERE c2.build_id = c.build_id AND c2.chunk_index > c.chunk_index
		       )
		FROM vanadis_chunks c
		JOIN vanadis_builds b ON c.build_id = b.id
		WHERE c.repo = ANY($1) AND c.pr_number = $2 AND b.status != 'cancelled'
		ORDER BY c.updated_at DESC
		LIMIT 1;`
	var m chunkPRMatch
	err := s.db.QueryRow(ctx, query, candidates, prNumber).Scan(&m.chunkID, &m.buildID, &m.chunkIdx, &m.effort, &m.hasRemaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, false, nil
	}
	if err != nil {
		return m, false, fmt.Errorf("query chunk by pr %v#%d: %w", candidates, prNumber, err)
	}
	return m, true, nil
}
