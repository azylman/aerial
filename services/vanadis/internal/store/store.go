// Package store holds the Vanadis PostgreSQL persistence contract, the pgx
// implementation, and the row-scan helpers shared by the service packages.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis/internal/model"
)

const jsonNull = "null"

// VanadisChunkPRLookup represents the result of looking up a chunk by repository and PR number.
type VanadisChunkPRLookup struct {
	Matched bool   `json:"matched"`
	ChunkID string `json:"chunk_id,omitempty"`
	BuildID string `json:"build_id,omitempty"`
	Effort  string `json:"effort,omitempty"`
}

// VanadisStore defines the persistence contract for Vanadis builds and chunks.
type VanadisStore interface {
	GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error)
	ListBuilds(ctx context.Context, limit int) ([]model.VanadisBuildEntry, error)
	GetChunk(ctx context.Context, chunkID string) (*model.VanadisChunk, *model.VanadisBuild, error)
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
}

// QueryExecer abstracts the pgx query surface consumed by PostgresVanadisStore.
type QueryExecer interface {
	Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresVanadisStore implements VanadisStore backed by PostgreSQL (pgxpool.Pool).
type PostgresVanadisStore struct {
	db QueryExecer
}

// NewPostgresVanadisStore constructs a new PostgresVanadisStore.
func NewPostgresVanadisStore(db QueryExecer) *PostgresVanadisStore {
	return &PostgresVanadisStore{db: db}
}

// RowScanner abstracts a single scannable row.
type RowScanner interface {
	Scan(dest ...any) error
}

// BuildColumns is the vanadis_builds SELECT list ScanBuild consumes, in its destination order.
// Every query whose rows go through ScanBuild must select exactly these columns.
const BuildColumns = `id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at`

// ScanBuild scans one row selected with BuildColumns.
func ScanBuild(s RowScanner) (model.VanadisBuild, error) {
	var b model.VanadisBuild
	err := s.Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.Quiet, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

// ScanChunks scans ordered vanadis_chunks rows into VanadisChunk values.
func ScanChunks(rows pgx.Rows) ([]model.VanadisChunk, error) {
	chunks := make([]model.VanadisChunk, 0)
	for rows.Next() {
		var c model.VanadisChunk
		var startedAt, completedAt sql.NullTime
		var rawCIChecks, rawRolloutState []byte
		if err := rows.Scan(
			&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions,
			&c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status,
			&c.PRNumber, &c.StallNudges, &c.DependsOn, &c.CIFailCount, &rawCIChecks,
			&rawRolloutState, &c.Banner, &c.MergeAwakeableID, &c.DeployAwakeableID,
			&startedAt, &completedAt, &c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		if startedAt.Valid {
			c.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			c.CompletedAt = completedAt.Time
		}
		c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = model.NonNilSlice(c.TargetFiles), model.NonNilSlice(c.TargetSymbols), model.NonNilSlice(c.AcceptanceCriteria)
		c.DependsOn = model.NonNilSlice(c.DependsOn)
		c.Effort = model.CapChunkEffort(c.Effort)
		if len(rawCIChecks) > 0 && string(rawCIChecks) != jsonNull && string(rawCIChecks) != "{}" {
			var checks map[string]string
			if err := json.Unmarshal(rawCIChecks, &checks); err == nil {
				c.CIChecks = checks
			}
		}
		if len(rawRolloutState) > 0 && string(rawRolloutState) != jsonNull && string(rawRolloutState) != "{}" {
			var rs pubsub.PRRolloutState
			if err := json.Unmarshal(rawRolloutState, &rs); err == nil {
				c.RolloutState = &rs
			}
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating vanadis chunks: %w", err)
	}
	return chunks, nil
}

// GetBuild retrieves a VanadisBuild and its ordered VanadisChunks by buildID.
func (s *PostgresVanadisStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil, nil, errors.New("build id cannot be empty")
	}
	const buildQuery = `SELECT ` + BuildColumns + ` FROM vanadis_builds WHERE id = $1;`
	b, err := ScanBuild(s.db.QueryRow(ctx, buildQuery, buildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, model.ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s: %w", buildID, err)
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := s.db.Query(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, nil, fmt.Errorf("query chunks for build %s: %w", buildID, err)
	}
	defer rows.Close()
	chunks, err := ScanChunks(rows)
	if err != nil {
		return nil, nil, err
	}
	return &b, chunks, nil
}

// ListBuilds queries recent Vanadis builds with their ordered chunks, summarized as VanadisBuildEntry items.
func (s *PostgresVanadisStore) ListBuilds(ctx context.Context, limit int) ([]model.VanadisBuildEntry, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const buildQuery = `SELECT ` + BuildColumns + ` FROM vanadis_builds ORDER BY created_at DESC, id DESC LIMIT $1;`
	rows, err := s.db.Query(ctx, buildQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis builds: %w", err)
	}
	defer rows.Close()
	var builds []model.VanadisBuild
	var buildIDs []string
	for rows.Next() {
		b, scanBuildErr := ScanBuild(rows)
		if scanBuildErr != nil {
			return nil, fmt.Errorf("failed to scan vanadis build: %w", scanBuildErr)
		}
		builds = append(builds, b)
		buildIDs = append(buildIDs, b.ID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating vanadis builds: %w", err)
	}
	if len(builds) == 0 {
		return []model.VanadisBuildEntry{}, nil
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = ANY($1) ORDER BY build_id, chunk_index ASC;`
	cRows, err := s.db.Query(ctx, chunksQuery, buildIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis chunks: %w", err)
	}
	defer cRows.Close()
	allChunks, err := ScanChunks(cRows)
	if err != nil {
		return nil, err
	}
	chunksByBuild := make(map[string][]model.VanadisChunk, len(builds))
	for _, c := range allChunks {
		chunksByBuild[c.BuildID] = append(chunksByBuild[c.BuildID], c)
	}
	entries := make([]model.VanadisBuildEntry, len(builds))
	for i, b := range builds {
		entries[i] = model.SummarizeBuildEntry(b, model.NonNilSlice(chunksByBuild[b.ID]))
	}
	return entries, nil
}

// GetChunk retrieves a VanadisChunk and its parent VanadisBuild by chunkID.
func (s *PostgresVanadisStore) GetChunk(ctx context.Context, chunkID string) (*model.VanadisChunk, *model.VanadisBuild, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, nil, errors.New("chunk id cannot be empty")
	}
	const chunkQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1;`
	rows, err := s.db.Query(ctx, chunkQuery, chunkID)
	if err != nil {
		return nil, nil, fmt.Errorf("query chunk %s: %w", chunkID, err)
	}
	defer rows.Close()
	chunks, err := ScanChunks(rows)
	if err != nil {
		return nil, nil, err
	}
	if len(chunks) == 0 {
		return nil, nil, model.ErrChunkNotFound
	}
	c := chunks[0]
	const buildQuery = `SELECT ` + BuildColumns + ` FROM vanadis_builds WHERE id = $1;`
	b, err := ScanBuild(s.db.QueryRow(ctx, buildQuery, c.BuildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, model.ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s for chunk %s: %w", c.BuildID, chunkID, err)
	}
	return &c, &b, nil
}

const defaultRepoOwner = "azylman"

// RepoMatchCandidates returns the repo spellings a PR lookup should match.
func RepoMatchCandidates(repo string) []string {
	repo = model.NormalizeRepo(repo)
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
		Effort:  model.CapChunkEffort(match.effort),
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
