package store

// Build mutation for vanadis-restate.
//
// UpdateBuild, UpdateChunk, AppendChunks and RemoveChunk plus their private
// helpers moved here from the root package so they promote through the
// embedded *Store. SQL text, validation rules, chunk index semantics and
// error behavior are byte-identical to the root copies they replace.
//
// DAG validation and dependency relinking live in internal/dag, the single
// copy of the build-DAG rules shared by store and build.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// ErrChunkNotFound reports a missing vanadis chunk.
var ErrChunkNotFound = errors.New("vanadis chunk not found")

func (s *Store) UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*model.VanadisBuild, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	title := strings.TrimSpace(req.Title)
	obj := strings.TrimSpace(req.Objective)
	sharedCtx := strings.TrimSpace(req.SharedContext)
	if buildID == "" || (title == "" && obj == "" && sharedCtx == "") {
		return nil, fmt.Errorf("%w: build id and at least one non-empty field (title, objective, shared_context) are required", ErrInvalidBuildInput)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin update build tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] update build tx rollback error: %v", rbErr)
		}
	}()

	var currentStatus string
	checkErr := tx.QueryRow(ctx, `SELECT status FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID).Scan(&currentStatus)
	if checkErr != nil {
		if errors.Is(checkErr, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("check build status: %w", checkErr)
	}
	if currentStatus != statusRunning && currentStatus != statusFailed {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running or failed)", ErrBuildConflict, buildID, currentStatus)
	}

	const updateQ = `UPDATE vanadis_builds SET title = CASE WHEN $2 != '' THEN $2 ELSE title END, objective = CASE WHEN $3 != '' THEN $3 ELSE objective END, shared_context = CASE WHEN $4 != '' THEN $4 ELSE shared_context END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, execErr := tx.Exec(ctx, updateQ, buildID, title, obj, sharedCtx); execErr != nil {
		return nil, fmt.Errorf("update build %s: %w", buildID, execErr)
	}

	fetchQ := `SELECT ` + BuildColumns + ` FROM vanadis_builds WHERE id = $1;`
	updated, err := ScanBuild(tx.QueryRow(ctx, fetchQ, buildID))
	if err != nil {
		return nil, fmt.Errorf("fetch updated build: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit update build tx: %w", err)
	}
	return &updated, nil
}

func validateUpdateChunkInput(buildID, chunkRef string, req UpdateChunkRequest) (string, error) {
	if buildID == "" || chunkRef == "" {
		return "", fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}
	hasField := req.Mode != nil || strings.TrimSpace(req.Repo) != "" ||
		strings.TrimSpace(req.Title) != "" || strings.TrimSpace(req.Instructions) != "" ||
		len(req.TargetFiles) > 0 || len(req.TargetSymbols) > 0 ||
		strings.TrimSpace(req.Effort) != "" || req.DependsOn != nil
	if !hasField {
		return "", fmt.Errorf("%w: build id, chunk ref, and at least one updated field are required", ErrInvalidBuildInput)
	}
	if req.Mode != nil {
		if strings.TrimSpace(*req.Mode) == "" {
			return "", fmt.Errorf("%w: mode cannot be empty string (must be 'pr' or 'action')", ErrInvalidBuildInput)
		}
		if _, err := buildplan.NormalizeChunkMode(*req.Mode); err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
	}
	var normEffort string
	if strings.TrimSpace(req.Effort) != "" {
		var effErr error
		normEffort, effErr = buildplan.NormalizeChunkEffort(req.Effort)
		if effErr != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, effErr)
		}
	}
	return normEffort, nil
}

func resolveChunkRef(chunks []model.VanadisChunk, chunkRef string) int {
	clean := strings.TrimSpace(chunkRef)
	if clean == "" {
		return -1
	}
	for i, c := range chunks {
		if c.ID == clean {
			return i
		}
	}
	if idx, err := strconv.Atoi(clean); err == nil {
		for i, c := range chunks {
			if c.ChunkIndex == idx {
				return i
			}
		}
	}
	return -1
}

func applyChunkModeUpdates(chunk *model.VanadisChunk, req UpdateChunkRequest, effectiveMode, effectiveRepo string) error {
	if effectiveMode == "pr" {
		if effectiveRepo == "" {
			return fmt.Errorf("%w: repo is required for pr mode", ErrInvalidBuildInput)
		}
		chunk.Mode = effectiveMode
		chunk.Repo = effectiveRepo
		return nil
	}
	if strings.TrimSpace(req.Repo) != "" {
		return fmt.Errorf("%w: action chunk must have empty repo", ErrInvalidBuildInput)
	}
	if len(req.TargetFiles) > 0 {
		return fmt.Errorf("%w: action chunk must not specify target_files", ErrInvalidBuildInput)
	}
	if len(req.TargetSymbols) > 0 {
		return fmt.Errorf("%w: action chunk must not specify target_symbols", ErrInvalidBuildInput)
	}
	chunk.Mode = effectiveMode
	chunk.Repo = ""
	chunk.TargetFiles = []string{}
	chunk.TargetSymbols = []string{}
	return nil
}

func applyChunkUpdates(chunk *model.VanadisChunk, req UpdateChunkRequest, normEffort string) error {
	effectiveMode := chunk.Mode
	if req.Mode != nil {
		effectiveMode = *req.Mode
	}
	effectiveRepo := chunk.Repo
	if strings.TrimSpace(req.Repo) != "" {
		effectiveRepo = buildplan.NormalizeRepo(req.Repo)
	}
	normMode, modeErr := buildplan.NormalizeChunkMode(effectiveMode)
	if modeErr != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBuildInput, modeErr)
	}
	if err := applyChunkModeUpdates(chunk, req, normMode, effectiveRepo); err != nil {
		return err
	}
	if strings.TrimSpace(req.Title) != "" {
		chunk.Title = strings.TrimSpace(req.Title)
	}
	if strings.TrimSpace(req.Instructions) != "" {
		chunk.Instructions = strings.TrimSpace(req.Instructions)
	}
	if len(req.TargetFiles) > 0 {
		chunk.TargetFiles = req.TargetFiles
	}
	if len(req.TargetSymbols) > 0 {
		chunk.TargetSymbols = req.TargetSymbols
	}
	if normEffort != "" {
		chunk.Effort = normEffort
	}
	if req.DependsOn != nil {
		chunk.DependsOn = NonNilSlice(*req.DependsOn)
	}
	return nil
}

func (s *Store) UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*model.VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	normEffort, err := validateUpdateChunkInput(buildID, chunkRef, req)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin update chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] update chunk tx rollback error: %v", rbErr)
		}
	}()

	var buildStatus string
	bErr := tx.QueryRow(ctx, `SELECT status FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID).Scan(&buildStatus)
	if bErr != nil {
		if errors.Is(bErr, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build for update chunk: %w", bErr)
	}
	if buildStatus != statusRunning && buildStatus != statusFailed {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running or failed)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks for update: %w", err)
	}
	existingChunks, err := ScanChunks(cRows)
	cRows.Close()
	if err != nil {
		return nil, err
	}

	targetIdx := resolveChunkRef(existingChunks, chunkRef)
	if targetIdx < 0 {
		return nil, ErrChunkNotFound
	}
	targetChunk := existingChunks[targetIdx]
	if targetChunk.Status != statusPending {
		return nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, targetChunk.ID, targetChunk.Status)
	}

	updatedCandidate := targetChunk
	if applyErr := applyChunkUpdates(&updatedCandidate, req, normEffort); applyErr != nil {
		return nil, applyErr
	}

	normalizedDeps, err := buildplan.ValidateUpdateChunkDAG(existingChunks, targetIdx, updatedCandidate)
	if err != nil {
		return nil, err
	}
	updatedCandidate.DependsOn = normalizedDeps

	const updQ = `UPDATE vanadis_chunks SET mode = $3, repo = $4, title = $5, instructions = $6, target_files = $7, target_symbols = $8, effort = $9, depends_on = $10, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND build_id = $2;`
	_, err = tx.Exec(ctx, updQ, updatedCandidate.ID, buildID, updatedCandidate.Mode, updatedCandidate.Repo, updatedCandidate.Title, updatedCandidate.Instructions, NonNilSlice(updatedCandidate.TargetFiles), NonNilSlice(updatedCandidate.TargetSymbols), updatedCandidate.Effort, NonNilSlice(updatedCandidate.DependsOn))
	if err != nil {
		return nil, fmt.Errorf("execute update chunk %s: %w", updatedCandidate.ID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit update chunk tx: %w", err)
	}
	return &updatedCandidate, nil
}

func validateAppendChunksInputs(buildID string, inputs []CreateChunkInput) error {
	if buildID == "" || len(inputs) == 0 {
		return fmt.Errorf("%w: build id and at least one chunk are required", ErrInvalidBuildInput)
	}
	for i, inp := range inputs {
		if strings.TrimSpace(inp.Title) == "" || strings.TrimSpace(inp.Instructions) == "" {
			return fmt.Errorf("%w: chunk[%d] title and instructions cannot be empty", ErrInvalidBuildInput, i)
		}
		if id := strings.TrimSpace(inp.ID); id != "" && !buildplan.IsValidVanadisChunkID(id) {
			return fmt.Errorf("%w: chunk[%d] invalid id %q", ErrInvalidBuildInput, i, id)
		}
		if err := buildplan.ValidateChunkModeAndRepo(i, inp.Mode, inp.Repo, inp.TargetFiles, inp.TargetSymbols); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
		if _, err := buildplan.NormalizeChunkEffort(inp.Effort); err != nil {
			return fmt.Errorf("%w: chunk[%d] %w", ErrInvalidBuildInput, i, err)
		}
	}
	return nil
}

func buildAppendedChunks(buildID string, existingIDs map[string]bool, startIndex int, inputs []CreateChunkInput, now time.Time) ([]model.VanadisChunk, error) {
	newChunks := make([]model.VanadisChunk, len(inputs))
	for i, inp := range inputs {
		cid := strings.TrimSpace(inp.ID)
		if cid == "" {
			cid = fmt.Sprintf("%s-chunk-%d", buildID, startIndex+i+1)
		}
		if existingIDs[cid] {
			return nil, fmt.Errorf("%w: duplicate chunk id %q in build %s", ErrBuildConflict, cid, buildID)
		}
		existingIDs[cid] = true
		cMode, err := buildplan.NormalizeChunkMode(inp.Mode)
		if err != nil {
			return nil, err
		}
		cEffort, err := buildplan.NormalizeChunkEffort(inp.Effort)
		if err != nil {
			return nil, err
		}
		cRepo := buildplan.NormalizeRepo(inp.Repo)
		if cMode == chunkModeAction {
			cRepo = ""
		}
		newChunks[i] = model.VanadisChunk{
			ID:                 cid,
			BuildID:            buildID,
			ChunkIndex:         startIndex + i,
			Mode:               cMode,
			Repo:               cRepo,
			Title:              strings.TrimSpace(inp.Title),
			Instructions:       strings.TrimSpace(inp.Instructions),
			TargetFiles:        NonNilSlice(inp.TargetFiles),
			TargetSymbols:      NonNilSlice(inp.TargetSymbols),
			Effort:             cEffort,
			AcceptanceCriteria: NonNilSlice(inp.AcceptanceCriteria),
			Status:             statusPending,
			DependsOn:          inp.DependsOn,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
	}
	return newChunks, nil
}

func (s *Store) AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]model.VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if err := validateAppendChunksInputs(buildID, inputs); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin append chunks tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] append chunks tx rollback error: %v", rbErr)
		}
	}()

	var buildStatus string
	bErr := tx.QueryRow(ctx, `SELECT status FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID).Scan(&buildStatus)
	if bErr != nil {
		if errors.Is(bErr, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build for append chunks: %w", bErr)
	}
	if buildStatus != statusRunning && buildStatus != statusFailed {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running or failed)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks for append: %w", err)
	}
	existingChunks, err := ScanChunks(cRows)
	cRows.Close()
	if err != nil {
		return nil, err
	}

	existingIDs := make(map[string]bool, len(existingChunks))
	for _, c := range existingChunks {
		existingIDs[c.ID] = true
	}

	newChunks, err := buildAppendedChunks(buildID, existingIDs, len(existingChunks), inputs, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	validatedNew, err := buildplan.ValidateAppendChunksDAG(existingChunks, newChunks)
	if err != nil {
		return nil, err
	}

	for _, c := range validatedNew {
		if err := InsertBuildChunk(ctx, tx, buildID, c, InsertChunkSQL); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit append chunks tx: %w", err)
	}
	return validatedNew, nil
}

func (s *Store) RemoveChunk(ctx context.Context, buildID, chunkRef string) (*model.VanadisChunk, []model.VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID, chunkRef = strings.TrimSpace(buildID), strings.TrimSpace(chunkRef)
	if buildID == "" || chunkRef == "" {
		return nil, nil, fmt.Errorf("%w: build id and chunk ref are required", ErrInvalidBuildInput)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin remove chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] remove chunk tx rollback error: %v", rbErr)
		}
	}()

	existingChunks, err := lockBuildAndChunks(ctx, tx, buildID)
	if err != nil {
		return nil, nil, err
	}

	targetIdx := resolveChunkRef(existingChunks, chunkRef)
	if targetIdx < 0 {
		return nil, nil, ErrChunkNotFound
	}
	removed := existingChunks[targetIdx]
	if removed.Status != statusPending {
		return nil, nil, fmt.Errorf("%w: chunk %s has status %q (must be pending)", ErrBuildConflict, removed.ID, removed.Status)
	}
	if len(existingChunks) <= 1 {
		return nil, nil, fmt.Errorf("%w: cannot remove the only chunk of build %s", ErrBuildConflict, buildID)
	}

	remaining := make([]model.VanadisChunk, 0, len(existingChunks)-1)
	remaining = append(remaining, existingChunks[:targetIdx]...)
	remaining = append(remaining, existingChunks[targetIdx+1:]...)

	buildplan.RelinkDependenciesAfterRemoval(remaining, removed.ID, removed.DependsOn)

	validatedRemaining, err := buildplan.NormalizeAndValidateBuildDAG(remaining)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: removing chunk causes invalid DAG: %w", ErrBuildConflict, err)
	}

	delTag, err := tx.Exec(ctx, `DELETE FROM vanadis_chunks WHERE id = $1 AND build_id = $2;`, removed.ID, buildID)
	if err != nil {
		return nil, nil, fmt.Errorf("delete chunk %s: %w", removed.ID, err)
	}
	if delTag.RowsAffected() == 0 {
		return nil, nil, ErrChunkNotFound
	}

	if err := reindexRemainingChunks(ctx, tx, buildID, validatedRemaining); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit remove chunk tx: %w", err)
	}
	return &removed, validatedRemaining, nil
}

func lockBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) ([]model.VanadisChunk, error) {
	var buildStatus string
	bErr := tx.QueryRow(ctx, `SELECT status FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID).Scan(&buildStatus)
	if bErr != nil {
		if errors.Is(bErr, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build: %w", bErr)
	}
	if buildStatus != statusRunning && buildStatus != statusFailed {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running or failed)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks: %w", err)
	}
	chunks, err := ScanChunks(cRows)
	cRows.Close()
	return chunks, err
}

func reindexRemainingChunks(ctx context.Context, tx pgx.Tx, buildID string, chunks []model.VanadisChunk) error {
	const updQ = `UPDATE vanadis_chunks SET chunk_index = $3, depends_on = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND build_id = $2;`
	for i := range chunks {
		chunks[i].ChunkIndex = i
		if _, err := tx.Exec(ctx, updQ, chunks[i].ID, buildID, i, NonNilSlice(chunks[i].DependsOn)); err != nil {
			return fmt.Errorf("reindex chunk %s: %w", chunks[i].ID, err)
		}
	}
	return nil
}
