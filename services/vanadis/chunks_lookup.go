package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// chunkPRMatch is the newest non-cancelled chunk tied to a repo and PR number.
type chunkPRMatch struct {
	chunkID, buildID, effort string
	chunkIdx                 int
	hasRemaining             bool
}

// GetChunkByPR looks up a chunk and its parent build for a given repo and pr_number,
// reporting the mode of the chunk that merging this PR would start next.
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
	allChunks, err := s.listChunkSummaries(ctx, match.buildID, match.chunkID)
	if err != nil {
		return nil, fmt.Errorf("list build chunks for pr %s#%d: %w", repo, prNumber, err)
	}
	return &VanadisChunkPRLookup{
		Matched:            true,
		ChunkID:            match.chunkID,
		BuildID:            match.buildID,
		Effort:             capChunkEffort(match.effort),
		HasRemainingChunks: match.hasRemaining,
		NextChunkMode:      nextChunkModeAfter(allChunks, match.chunkID, match.chunkIdx),
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

// listChunkSummaries reads a build's chunks without locking, counting doneChunkID as done.
func (s *PostgresVanadisStore) listChunkSummaries(ctx context.Context, buildID, doneChunkID string) ([]VanadisChunkSummary, error) {
	const chunksQuery = `SELECT id, chunk_index, mode, repo, title, status, COALESCE(pr_number, 0), started_at, completed_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC`
	rows, err := s.db.Query(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVanadisChunkSummaries(rows, doneChunkID)
}

// nextChunkModeAfter reports the mode of the chunk computeNextChunkIndex would
// start once doneChunkID is counted as done, or "" when the build would complete.
func nextChunkModeAfter(chunks []VanadisChunkSummary, doneChunkID string, doneIdx int) string {
	marked := make([]VanadisChunkSummary, len(chunks))
	copy(marked, chunks)
	for i := range marked {
		if marked[i].ID == doneChunkID {
			marked[i].Status = "merged"
		}
	}
	nextIdx := computeNextChunkIndex(marked, doneIdx)
	for _, c := range marked {
		if c.ChunkIndex == nextIdx {
			return c.Mode
		}
	}
	return ""
}
