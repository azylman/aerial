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
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	ErrBuildNotFound     = errors.New("vanadis build not found")
	ErrBuildConflict     = errors.New("vanadis build state conflict")
	ErrInvalidBuildInput = errors.New("invalid vanadis build input")
)

// CreateBuild inserts a build and its chunks atomically. A replay of an
// identical creation commits as a no-op; a conflicting replay fails.
func (s *Store) CreateBuild(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	buildID, threadID, objective := strings.TrimSpace(build.ID), strings.TrimSpace(build.ThreadID), strings.TrimSpace(build.Objective)
	if err := validateCreateBuildInputs(buildID, threadID, objective, chunks); err != nil {
		return err
	}
	normChunks, err := normalizeAndValidateBuildDAG(chunks)
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
	bQ := `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (id) DO NOTHING;`
	now := time.Now().UTC()
	bTitle := FirstNonEmpty(strings.TrimSpace(build.Title), objective)
	tag, err := tx.Exec(ctx, bQ, buildID, threadID, build.Tier, bTitle, objective, strings.TrimSpace(build.SharedContext), FirstNonEmpty(build.Status, statusPending), strings.TrimSpace(build.StatusMessageID), build.Quiet, now, now)
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
	cMode, err := NormalizeChunkMode(c.Mode)
	if err != nil {
		return fmt.Errorf("chunk %s: %w", c.ID, err)
	}
	cRepo := ""
	if cMode == "pr" {
		cRepo = NormalizeRepo(c.Repo)
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
	cEffort, err := NormalizeChunkEffort(c.Effort)
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
	} else if isTerminalChunkStatus(cStatus) {
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
		return model.VanadisBuild{
			ID: strings.TrimSpace(v.ID), ThreadID: strings.TrimSpace(v.ThreadID), Tier: v.Tier,
			Title:     FirstNonEmpty(strings.TrimSpace(v.Title), strings.TrimSpace(v.Objective)),
			Objective: strings.TrimSpace(v.Objective), SharedContext: strings.TrimSpace(v.SharedContext), Quiet: v.Quiet,
		}
	}
	if buildKey(a) != buildKey(b) || len(ac) != len(bc) {
		return false
	}
	chunkKey := func(c model.VanadisChunk) (model.VanadisChunk, error) {
		mode, err := NormalizeChunkMode(c.Mode)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		effort, err := NormalizeChunkEffort(c.Effort)
		if err != nil {
			return model.VanadisChunk{}, err
		}
		repo := ""
		if mode == "pr" {
			repo = NormalizeRepo(c.Repo)
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

// Creation-time validation mirrors the root dag.go helpers byte-for-byte.
// dag.go stays frozen until the facade-removal chunk unifies both copies.
func normalizeAndValidateBuildDAG(chunks []model.VanadisChunk) ([]model.VanadisChunk, error) {
	if len(chunks) == 0 {
		return chunks, nil
	}
	out := make([]model.VanadisChunk, len(chunks))
	copy(out, chunks)

	idMap := make(map[string]int, len(out))
	for i, c := range out {
		cid := strings.TrimSpace(c.ID)
		if cid == "" {
			return nil, fmt.Errorf("%w: chunk[%d] id cannot be empty", ErrInvalidBuildInput, i)
		}
		if _, exists := idMap[cid]; exists {
			return nil, fmt.Errorf("%w: duplicate chunk id %q", ErrInvalidBuildInput, cid)
		}
		idMap[cid] = i
	}

	if err := resolveChunkDependencies(out, idMap); err != nil {
		return nil, err
	}

	if err := checkKahnDAGCycles(out, idMap); err != nil {
		return nil, err
	}

	reach := buildTransitiveClosure(out, idMap)
	if err := validateConcurrentDisjointPRChunks(out, reach); err != nil {
		return nil, err
	}

	return out, nil
}

func resolveChunkDependencies(out []model.VanadisChunk, idMap map[string]int) error {
	for i := range out {
		if out[i].DependsOn == nil {
			return fmt.Errorf("%w: chunk %q must explicitly specify depends_on", ErrInvalidBuildInput, out[i].ID)
		}
		normalizedDeps := make([]string, 0, len(out[i].DependsOn))
		seenDeps := make(map[string]bool, len(out[i].DependsOn))
		for _, rawDep := range out[i].DependsOn {
			dep := strings.TrimSpace(rawDep)
			if dep == "" {
				return fmt.Errorf("%w: chunk %q has empty dependency reference", ErrInvalidBuildInput, out[i].ID)
			}
			var targetID string
			if idx, ok := idMap[dep]; ok {
				targetID = out[idx].ID
			} else if idx, err := strconv.Atoi(dep); err == nil && idx >= 0 && idx < len(out) {
				targetID = out[idx].ID
			} else {
				return fmt.Errorf("%w: chunk %q references unknown dependency %q", ErrInvalidBuildInput, out[i].ID, dep)
			}
			if targetID == out[i].ID {
				return fmt.Errorf("%w: chunk %q cannot depend on itself", ErrInvalidBuildInput, out[i].ID)
			}
			if !seenDeps[targetID] {
				seenDeps[targetID] = true
				normalizedDeps = append(normalizedDeps, targetID)
			}
		}
		out[i].DependsOn = normalizedDeps
	}
	return nil
}

func checkKahnDAGCycles(out []model.VanadisChunk, idMap map[string]int) error {
	N := len(out)
	inDegree := make([]int, N)
	adj := make([][]int, N)
	for v := range N {
		inDegree[v] = len(out[v].DependsOn)
		for _, depID := range out[v].DependsOn {
			u := idMap[depID]
			adj[u] = append(adj[u], v)
		}
	}

	queue := make([]int, 0, N)
	for i := range N {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	visitedCount := 0
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		visitedCount++
		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	if visitedCount != N {
		return fmt.Errorf("%w: cycle detected in chunk dependencies", ErrInvalidBuildInput)
	}
	return nil
}

func buildTransitiveClosure(out []model.VanadisChunk, idMap map[string]int) [][]bool {
	N := len(out)
	reach := make([][]bool, N)
	for i := range N {
		reach[i] = make([]bool, N)
	}
	for i := range N {
		for _, depID := range out[i].DependsOn {
			u := idMap[depID]
			reach[u][i] = true
		}
	}
	for k := range N {
		for i := range N {
			if reach[i][k] {
				for j := range N {
					if reach[k][j] {
						reach[i][j] = true
					}
				}
			}
		}
	}
	return reach
}

func validateConcurrentDisjointPRChunks(out []model.VanadisChunk, reach [][]bool) error {
	for i, chunkI := range out {
		if isTerminalChunkStatus(chunkI.Status) {
			continue
		}
		modeI, err := NormalizeChunkMode(chunkI.Mode)
		if err != nil || modeI != "pr" {
			continue
		}
		repoI := NormalizeRepo(chunkI.Repo)
		for j := i + 1; j < len(out); j++ {
			chunkJ := out[j]
			if isTerminalChunkStatus(chunkJ.Status) {
				continue
			}
			modeJ, err := NormalizeChunkMode(chunkJ.Mode)
			if err != nil || modeJ != "pr" {
				continue
			}
			repoJ := NormalizeRepo(chunkJ.Repo)
			if !strings.EqualFold(repoI, repoJ) {
				continue
			}
			if (i < len(reach) && j < len(reach[i]) && reach[i][j]) || (j < len(reach) && i < len(reach[j]) && reach[j][i]) {
				continue
			}
			if err := checkDisjointFiles(chunkI, chunkJ, repoI); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkDisjointFiles(a, b model.VanadisChunk, repo string) error {
	if len(a.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", ErrInvalidBuildInput, a.ID, repo)
	}
	if len(b.TargetFiles) == 0 {
		return fmt.Errorf("%w: concurrent PR chunk %q on repo %q must specify target_files", ErrInvalidBuildInput, b.ID, repo)
	}
	filesA := make(map[string]bool, len(a.TargetFiles))
	for _, f := range a.TargetFiles {
		normF := filepath.Clean(strings.TrimSpace(f))
		if normF != "" && normF != "." {
			filesA[normF] = true
		}
	}
	for _, f := range b.TargetFiles {
		normF := filepath.Clean(strings.TrimSpace(f))
		if normF != "" && normF != "." && filesA[normF] {
			return fmt.Errorf("%w: concurrent PR chunks %q and %q on repo %q have overlapping target_files: %s", ErrInvalidBuildInput, a.ID, b.ID, repo, normF)
		}
	}
	return nil
}

func validateCreateBuildInputs(buildID, threadID, objective string, chunks []model.VanadisChunk) error {
	if buildID == "" || threadID == "" || objective == "" {
		return errors.New("build id, thread_id, and objective cannot be empty")
	}
	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] id, title, and instructions cannot be empty", i)
		}
		if !IsValidVanadisChunkID(strings.TrimSpace(c.ID)) {
			return fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if err := ValidateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); err != nil {
			return err
		}
		if _, err := NormalizeChunkEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
	}
	if _, err := normalizeAndValidateBuildDAG(chunks); err != nil {
		return err
	}
	return nil
}

func isTerminalChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusMerged, statusCompleted, statusFailed, statusCancelled:
		return true
	default:
		return false
	}
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
