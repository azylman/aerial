package store

// Build creation and retrieval for vanadis-restate.
//
// CreateBuild, GetBuild, InsertBuildChunk, knownCreationConflict and
// sameBuildCreation moved here from the root package so they promote through
// the embedded *Store. SQL text, conflict semantics and validation behavior
// are byte-identical to the root copies they replace.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Status vocabulary mirrored from the root package (main.go, store.go).
// String-compared only, so these local copies preserve behavior exactly;
// the facade-removal chunk unifies them.
const (
	statusPending   = "pending"
	statusRunning   = "running"
	statusCoding    = "coding"
	statusMerged    = "merged"
	statusCompleted = "completed"
	statusFailed    = "failed"
	statusCancelled = "cancelled"
)

// InsertChunkSQL inserts one chunk row.
const InsertChunkSQL = `INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`

// SelectChunksForUpdateSQL locks a build's chunks in index order.
const SelectChunksForUpdateSQL = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE;`

var (
	ErrBuildNotFound = errors.New("vanadis build not found")
	ErrBuildConflict = errors.New("vanadis build state conflict")
	// ErrInvalidBuildInput aliases the canonical model sentinel so errors.Is
	// identity holds wherever build input validation executes.
	ErrInvalidBuildInput = model.ErrInvalidBuildInput
)

// CreateBuild inserts a build and its chunks atomically. A replay of an
// identical creation commits as a no-op; a conflicting replay fails.
func (s *Store) CreateBuild(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	buildID, threadID, objective := strings.TrimSpace(build.ID), strings.TrimSpace(build.ThreadID), strings.TrimSpace(build.Objective)
	if err := buildplan.ValidateCreateBuildInputs(buildID, threadID, objective, chunks); err != nil {
		return err
	}
	normChunks, err := buildplan.NormalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] tx rollback error: %v", rbErr)
		}
	}()
	bQ := `INSERT INTO vanadis_builds (id, agent, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at, runner_agent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (id) DO NOTHING;`
	now := time.Now().UTC()
	bTitle := FirstNonEmpty(strings.TrimSpace(build.Title), objective)
	agent := strings.TrimSpace(build.Agent)
	if agent == "" {
		agent = defaultAgent
	}
	tag, err := tx.Exec(ctx, bQ, buildID, agent, threadID, build.Tier, bTitle, objective, strings.TrimSpace(build.SharedContext), FirstNonEmpty(build.Status, statusPending), strings.TrimSpace(build.StatusMessageID), build.Quiet, now, now, NormalizeRunnerAgent(build.RunnerAgent))
	if err != nil {
		return fmt.Errorf("failed to insert vanadis build: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// ON CONFLICT waits for an ambiguous prior commit without aborting this
		// transaction. Lock the build before comparing its complete chunk set.
		existing, err := ScanBuild(tx.QueryRow(ctx, "SELECT "+BuildColumns+" FROM vanadis_builds WHERE id = $1 FOR UPDATE;", buildID))
		if err != nil {
			return fmt.Errorf("read existing build: %w", err)
		}
		rows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
		if err != nil {
			return fmt.Errorf("read existing chunks: %w", err)
		}
		persisted, err := ScanChunks(rows)
		rows.Close()
		if err != nil {
			return err
		}
		if !sameBuildCreation(existing, persisted, build, normChunks) {
			return fmt.Errorf("%w: build %q already exists with different creation data", ErrBuildConflict, buildID)
		}
		return tx.Commit(ctx)
	}
	for _, c := range normChunks {
		if err := InsertBuildChunk(ctx, tx, buildID, c, InsertChunkSQL); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// GetBuild loads a build and its chunks in index order.
func (s *Store) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil, nil, errors.New("build id cannot be empty")
	}
	bQ := `SELECT ` + BuildColumns + ` FROM vanadis_builds WHERE id = $1;`
	row := s.db.QueryRow(ctx, bQ, buildID)
	b, err := ScanBuild(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s: %w", buildID, err)
	}
	cQ := `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := s.db.Query(ctx, cQ, buildID)
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

// InsertBuildChunk validates and inserts one chunk row in tx. Exported so the
// still-rooted append and attachment paths can call it until their chunks move.
func InsertBuildChunk(ctx context.Context, tx pgx.Tx, buildID string, c model.VanadisChunk, cQ string) error {
	cMode, err := buildplan.NormalizeChunkMode(c.Mode)
	if err != nil {
		return fmt.Errorf("chunk %s: %w", c.ID, err)
	}
	cRepo := ""
	if cMode == "pr" {
		cRepo = buildplan.NormalizeRepo(c.Repo)
		if cRepo == "" {
			return fmt.Errorf("chunk %s: repo is required for pr mode", c.ID)
		}
	} else {
		if strings.TrimSpace(c.Repo) != "" {
			return fmt.Errorf("chunk %s: action chunk must have empty repo", c.ID)
		}
		if len(c.TargetFiles) > 0 || len(c.TargetSymbols) > 0 {
			return fmt.Errorf("chunk %s: action chunk must not specify target files or symbols", c.ID)
		}
	}
	cStatus := FirstNonEmpty(strings.TrimSpace(c.Status), "pending")
	cEffort, err := buildplan.NormalizeChunkEffort(c.Effort)
	if err != nil {
		return fmt.Errorf("chunk %s: %w", c.ID, err)
	}
	var prNum, startedAt, completedAt any
	if c.PRNumber > 0 {
		prNum = c.PRNumber
	}
	if !c.StartedAt.IsZero() {
		startedAt = c.StartedAt
	} else if cStatus == statusRunning || cStatus == statusCoding {
		startedAt = time.Now().UTC()
	}
	if !c.CompletedAt.IsZero() {
		completedAt = c.CompletedAt
	} else if buildplan.IsTerminalChunkStatus(cStatus) {
		completedAt = time.Now().UTC()
	}
	ciChecksJSON := marshalJSONBOrEmpty(c.CIChecks)
	rolloutStateJSON := marshalJSONBOrEmpty(c.RolloutState)
	if _, err := tx.Exec(ctx, cQ,
		strings.TrimSpace(c.ID), buildID, c.ChunkIndex, cMode, cRepo,
		strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions),
		NonNilSlice(c.TargetFiles), NonNilSlice(c.TargetSymbols), cEffort,
		NonNilSlice(c.AcceptanceCriteria), cStatus, prNum, c.StallNudges,
		NonNilSlice(c.DependsOn), c.CIFailCount, ciChecksJSON, rolloutStateJSON,
		strings.TrimSpace(c.Banner), strings.TrimSpace(c.MergeAwakeableID),
		strings.TrimSpace(c.DeployAwakeableID), startedAt, completedAt,
	); err != nil {
		if knownCreationConflict(err, "vanadis_chunks_pkey", "uq_vanadis_chunks_build_index") {
			return fmt.Errorf("%w: chunk %q in build %q already exists", ErrBuildConflict, c.ID, buildID)
		}
		return fmt.Errorf("failed to insert vanadis chunk %s: %w", c.ID, err)
	}
	return nil
}

// Only constraints owned by build creation are deterministic conflicts.
// Unrecognized database failures retain their original retryable error chain.
func knownCreationConflict(err error, constraints ...string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return slices.Contains(constraints, pgErr.ConstraintName)
}

// Compare immutable request fields only: execution may already have advanced
// after a successful commit whose acknowledgement was lost.
func sameBuildCreation(a model.VanadisBuild, ac []model.VanadisChunk, b model.VanadisBuild, bc []model.VanadisChunk) bool {
	buildKey := func(v model.VanadisBuild) model.VanadisBuild {
		agent := strings.TrimSpace(v.Agent)
		if agent == "" {
			agent = "aerial"
		}
		return model.VanadisBuild{
			ID: strings.TrimSpace(v.ID), Agent: agent, RunnerAgent: NormalizeRunnerAgent(v.RunnerAgent), ThreadID: strings.TrimSpace(v.ThreadID), Tier: v.Tier,
			Title:     FirstNonEmpty(strings.TrimSpace(v.Title), strings.TrimSpace(v.Objective)),
			Objective: strings.TrimSpace(v.Objective), SharedContext: strings.TrimSpace(v.SharedContext), Quiet: v.Quiet,
		}
	}
	if buildKey(a) != buildKey(b) || len(ac) != len(bc) {
		return false
	}
	chunkKey := func(c model.VanadisChunk) (model.VanadisChunk, error) {
		mode, err := buildplan.NormalizeChunkMode(c.Mode)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		effort, err := buildplan.NormalizeChunkEffort(c.Effort)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		repo := ""
		if mode == "pr" {
			repo = buildplan.NormalizeRepo(c.Repo)
		}
		return model.VanadisChunk{
			ID: strings.TrimSpace(c.ID), ChunkIndex: c.ChunkIndex, Mode: mode, Repo: repo,
			Title: strings.TrimSpace(c.Title), Instructions: strings.TrimSpace(c.Instructions),
			TargetFiles: NonNilSlice(c.TargetFiles), TargetSymbols: NonNilSlice(c.TargetSymbols),
			Effort: effort, AcceptanceCriteria: NonNilSlice(c.AcceptanceCriteria), DependsOn: NonNilSlice(c.DependsOn),
		}, nil
	}
	byID := make(map[string]model.VanadisChunk, len(ac))
	for _, c := range ac {
		key, err := chunkKey(c)
		if err != nil {
			return false
		}
		byID[c.ID] = key
	}
	for _, c := range bc {
		old, ok := byID[strings.TrimSpace(c.ID)]
		key, err := chunkKey(c)
		if err != nil || !ok || !reflect.DeepEqual(old, key) {
			return false
		}
		delete(byID, strings.TrimSpace(c.ID))
	}
	return len(byID) == 0
}

func marshalJSONBOrEmpty(v any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 || string(b) == jsonNull {
		return []byte("{}")
	}
	return b
}
