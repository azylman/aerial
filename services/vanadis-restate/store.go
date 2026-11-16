package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	statusPending            = "pending"
	statusRunning            = "running"
	statusCoding             = "coding"
	statusOpen               = "open"
	statusPROpened           = "pr_opened"
	statusMerged             = "merged"
	statusSynced             = "synced"
	statusDeploying          = "deploying"
	statusDeployed           = "deployed"
	statusConflict           = "conflict"
	statusDeployFailed       = "deploy_failed"
	statusCIFailed           = "ci_failed"
	chunkModeAction          = "action"
	defaultRepoOwner         = "azylman"
	effortLow                = "low"
	effortMedium             = "medium"
	effortHigh               = "high"
	insertChunkSQL           = `INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`
	selectChunksForUpdateSQL = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC FOR UPDATE;`
)

var (
	ErrBuildNotFound     = errors.New("vanadis build not found")
	ErrChunkNotFound     = errors.New("vanadis chunk not found")
	ErrBuildConflict     = errors.New("vanadis build state conflict")
	ErrInvalidBuildInput = errors.New("invalid vanadis build input")
)

type pgxQueryExecer interface {
	Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

type rowScanner interface {
	Scan(dest ...any) error
}

type VanadisChunkSummary struct {
	ID          string    `json:"id"`
	ChunkIndex  int       `json:"chunk_index"`
	Mode        string    `json:"mode"`
	Repo        string    `json:"repo,omitempty"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	PRNumber    int       `json:"pr_number,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type VanadisAdvancementResult struct {
	CompletionAccepted bool                  `json:"completion_accepted,omitempty"`
	Matched            bool                  `json:"matched"`
	Advanced           bool                  `json:"advanced"`
	BuildID            string                `json:"build_id"`
	ThreadID           string                `json:"thread_id"`
	Tier               int                   `json:"tier"`
	Title              string                `json:"title"`
	Objective          string                `json:"objective"`
	BuildStatus        string                `json:"build_status"`
	StatusMessageID    string                `json:"status_message_id,omitempty"`
	MergedChunkIndex   int                   `json:"merged_chunk_index"`
	MergedChunkTitle   string                `json:"merged_chunk_title"`
	ChunkMode          string                `json:"chunk_mode,omitempty"`
	NextChunkIndex     int                   `json:"next_chunk_index"`
	BuildCompleted     bool                  `json:"build_completed"`
	Chunks             []VanadisChunkSummary `json:"chunks,omitempty"`
}

type VanadisChunkPRLookup struct {
	Matched bool   `json:"matched"`
	ChunkID string `json:"chunk_id,omitempty"`
	BuildID string `json:"build_id,omitempty"`
	Effort  string `json:"effort,omitempty"`
}

type AttachChunkPRRequest struct {
	ChunkID  string `json:"chunk_id,omitempty"`
	PRNumber int    `json:"pr_number"`
	Repo     string `json:"repo"`
	Branch   string `json:"branch,omitempty"`
	Title    string `json:"title,omitempty"`
	TargetID string `json:"target_id,omitempty"`
	Quiet    bool   `json:"quiet,omitempty"`
}

type CreateChunkInput struct {
	ID                 string   `json:"id"`
	Mode               string   `json:"mode,omitempty"`
	Repo               string   `json:"repo,omitempty"`
	Title              string   `json:"title"`
	Instructions       string   `json:"instructions"`
	TargetFiles        []string `json:"target_files"`
	TargetSymbols      []string `json:"target_symbols"`
	Effort             string   `json:"effort,omitempty"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
	DependsOn          []string `json:"depends_on,omitempty"`
}

type CreateBuildRequest struct {
	ID            string             `json:"id"`
	ThreadID      string             `json:"thread_id"`
	Tier          int                `json:"tier"`
	Title         string             `json:"title"`
	Objective     string             `json:"objective"`
	SharedContext string             `json:"shared_context,omitempty"`
	Quiet         bool               `json:"quiet"`
	Chunks        []CreateChunkInput `json:"chunks"`
}

type AppendChunksRequest struct {
	Chunks []CreateChunkInput `json:"chunks"`
}

type UpdateChunkRequest struct {
	ChunkRef      string    `json:"chunk_ref,omitempty"`
	Mode          *string   `json:"mode,omitempty"`
	Repo          string    `json:"repo,omitempty"`
	Title         string    `json:"title,omitempty"`
	Instructions  string    `json:"instructions,omitempty"`
	TargetFiles   []string  `json:"target_files,omitempty"`
	TargetSymbols []string  `json:"target_symbols,omitempty"`
	Effort        string    `json:"effort,omitempty"`
	DependsOn     *[]string `json:"depends_on,omitempty"`
}

type UpdateBuildRequest struct {
	Title         string `json:"title,omitempty"`
	Objective     string `json:"objective,omitempty"`
	SharedContext string `json:"shared_context,omitempty"`
}

type VanadisStore interface {
	CreateBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error
	GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error)
	UpdateChunkStatus(ctx context.Context, chunkID, status string) error
	ReserveChunkForLaunch(ctx context.Context, chunkID string) error
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
	LookupPRChunk(ctx context.Context, repo string, prNumber int) (buildID, chunkID string, err error)
	LookupChunkBuildID(ctx context.Context, chunkID string) (buildID string, err error)
	AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (buildID, chunkID, threadID string, err error)
	CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*VanadisAdvancementResult, error)
	FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*VanadisBuild, *VanadisChunk, bool, error)
	UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*VanadisBuild, error)
	UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*VanadisChunk, error)
	AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]VanadisChunk, error)
	RemoveChunk(ctx context.Context, buildID, chunkRef string) (*VanadisChunk, []VanadisChunk, error)
	RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error
	RecordPRCheckStatus(ctx context.Context, chunkID, checkName, checkState string) error
	CompletePRDeployment(ctx context.Context, chunkID string, rolloutState *pubsub.PRRolloutState) error
	UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error
	UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error
}

type PostgresVanadisStore struct {
	db pgxQueryExecer
}

func NewPostgresVanadisStore(db pgxQueryExecer) *PostgresVanadisStore {
	return &PostgresVanadisStore{db: db}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func normalizeRepo(repo string) string {
	return strings.TrimSpace(repo)
}

func normalizeChunkMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "pr":
		return "pr", nil
	case chunkModeAction:
		return chunkModeAction, nil
	default:
		return "", fmt.Errorf("invalid chunk mode %q: must be 'pr' or 'action'", raw)
	}
}

func normalizeChunkEffort(effort string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "", effortMedium, "high":
		return effortMedium, nil
	case effortLow:
		return effortLow, nil
	default:
		return "", fmt.Errorf("invalid chunk effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

func capChunkEffort(effort string) string {
	eff, err := normalizeChunkEffort(effort)
	if err != nil {
		return effortMedium
	}
	return eff
}

func isValidVanadisChunkID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 1 || len(id) > 120 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

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

func validateChunkModeAndRepo(idx int, modeStr, repo string, targetFiles, targetSymbols []string) error {
	mode, err := normalizeChunkMode(modeStr)
	if err != nil {
		return fmt.Errorf("chunk[%d] %w", idx, err)
	}
	switch mode {
	case "pr":
		if normalizeRepo(repo) == "" {
			return fmt.Errorf("chunk[%d] repo is required for pr mode", idx)
		}
	case chunkModeAction:
		if strings.TrimSpace(repo) != "" {
			return fmt.Errorf("chunk[%d] action chunk must have empty repo", idx)
		}
		if len(targetFiles) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_files", idx)
		}
		if len(targetSymbols) > 0 {
			return fmt.Errorf("chunk[%d] action chunk must not specify target_symbols", idx)
		}
	}
	return nil
}

const buildColumns = `id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at`

func scanBuild(s rowScanner) (VanadisBuild, error) {
	var b VanadisBuild
	err := s.Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.Quiet, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

func scanChunks(rows pgx.Rows) ([]VanadisChunk, error) {
	chunks := make([]VanadisChunk, 0)
	for rows.Next() {
		var c VanadisChunk
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
		c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = nonNilSlice(c.TargetFiles), nonNilSlice(c.TargetSymbols), nonNilSlice(c.AcceptanceCriteria)
		c.DependsOn = nonNilSlice(c.DependsOn)
		c.Effort = capChunkEffort(c.Effort)
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

func (s *PostgresVanadisStore) execUpdateByID(ctx context.Context, idLabel, id, valLabel, val string, allowEmptyVal bool, query, opLabel string, notFoundErr error) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	cleanID, cleanVal := strings.TrimSpace(id), strings.TrimSpace(val)
	if cleanID == "" || (!allowEmptyVal && cleanVal == "") {
		return fmt.Errorf("%s and %s cannot be empty", idLabel, valLabel)
	}
	cmdTag, err := s.db.Exec(ctx, query, cleanVal, cleanID)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", opLabel, err)
	case cmdTag.RowsAffected() == 0:
		return notFoundErr
	default:
		return nil
	}
}

func (s *PostgresVanadisStore) UpdateChunkStatus(ctx context.Context, chunkID, status string) error {
	return s.execUpdateByID(ctx, "chunk id", chunkID, "status", status, false, `UPDATE vanadis_chunks SET status = $1, started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, completed_at = CASE WHEN $1 IN ('merged', 'completed', 'failed', 'cancelled') THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis chunk status", ErrChunkNotFound)
}

func (s *PostgresVanadisStore) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "status", status, false, `UPDATE vanadis_builds SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status", ErrBuildNotFound)
}

func (s *PostgresVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "message_id", messageID, true, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status_message_id", ErrBuildNotFound)
}

func (s *PostgresVanadisStore) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = CASE WHEN status IN ('pending', 'queued') THEN 'running' ELSE status END, started_at = CASE WHEN status IN ('pending', 'queued') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, chunkID)
	if err != nil {
		return fmt.Errorf("reserve chunk for launch %s: %w", chunkID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	return nil
}

func insertBuildChunk(ctx context.Context, tx pgx.Tx, buildID string, c VanadisChunk, cQ string) error {
	cMode, err := normalizeChunkMode(c.Mode)
	if err != nil {
		return fmt.Errorf("chunk %s: %w", c.ID, err)
	}
	cRepo := ""
	if cMode == "pr" {
		cRepo = normalizeRepo(c.Repo)
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
	cStatus := firstNonEmpty(strings.TrimSpace(c.Status), "pending")
	cEffort, err := normalizeChunkEffort(c.Effort)
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
		nonNilSlice(c.TargetFiles), nonNilSlice(c.TargetSymbols), cEffort,
		nonNilSlice(c.AcceptanceCriteria), cStatus, prNum, c.StallNudges,
		nonNilSlice(c.DependsOn), c.CIFailCount, ciChecksJSON, rolloutStateJSON,
		strings.TrimSpace(c.Banner), strings.TrimSpace(c.MergeAwakeableID),
		strings.TrimSpace(c.DeployAwakeableID), startedAt, completedAt,
	); err != nil {
		return fmt.Errorf("failed to insert vanadis chunk %s: %w", c.ID, err)
	}
	return nil
}

func (s *PostgresVanadisStore) CreateBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
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
	bQ := `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`
	now := time.Now().UTC()
	bTitle := firstNonEmpty(strings.TrimSpace(build.Title), objective)
	if _, err = tx.Exec(ctx, bQ, buildID, threadID, build.Tier, bTitle, objective, strings.TrimSpace(build.SharedContext), firstNonEmpty(build.Status, statusPending), strings.TrimSpace(build.StatusMessageID), build.Quiet, now, now); err != nil {
		return fmt.Errorf("failed to insert vanadis build: %w", err)
	}
	for _, c := range normChunks {
		if err := insertBuildChunk(ctx, tx, buildID, c, insertChunkSQL); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresVanadisStore) GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil, nil, errors.New("build id cannot be empty")
	}
	bQ := `SELECT ` + buildColumns + ` FROM vanadis_builds WHERE id = $1;`
	row := s.db.QueryRow(ctx, bQ, buildID)
	b, err := scanBuild(row)
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
	chunks, err := scanChunks(rows)
	if err != nil {
		return nil, nil, err
	}
	return &b, chunks, nil
}

func (s *PostgresVanadisStore) AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	if s == nil || s.db == nil {
		return "", "", "", errors.New("database connection is nil")
	}
	if req.PRNumber <= 0 {
		return "", "", "", errors.New("chunk id or repo, and valid pr number are required")
	}
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID != "" {
		return s.attachToExistingChunk(ctx, chunkID, req)
	}
	return s.attachStandalonePR(ctx, req)
}

func (s *PostgresVanadisStore) attachToExistingChunk(ctx context.Context, chunkID string, req AttachChunkPRRequest) (string, string, string, error) {
	var mode string
	chkErr := s.db.QueryRow(ctx, `SELECT mode FROM vanadis_chunks WHERE id = $1;`, chunkID).Scan(&mode)
	if chkErr != nil {
		if errors.Is(chkErr, pgx.ErrNoRows) {
			return "", "", "", ErrChunkNotFound
		}
		return "", "", "", fmt.Errorf("check chunk mode %s: %w", chunkID, chkErr)
	}
	if mode == chunkModeAction {
		repo := normalizeRepo(req.Repo)
		if repo == "" {
			return "", "", "", errors.New("repo is required to attach PR to action chunk")
		}
		const qAction = `UPDATE vanadis_chunks c SET repo = $1, pr_number = $2, status = CASE WHEN c.status IN ('merged', 'deploying', 'completed', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $3 AND c.status != 'cancelled' AND (c.status NOT IN ('merged', 'deploying', 'completed') OR (c.repo = $1 AND c.pr_number = $2)) AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
		var buildID, returnChunkID, threadID string
		err := s.db.QueryRow(ctx, qAction, repo, req.PRNumber, chunkID).Scan(&buildID, &returnChunkID, &threadID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, repo, req.PRNumber)
			}
			return "", "", "", fmt.Errorf("attach action chunk pr %s#%d: %w", repo, req.PRNumber, err)
		}
		return buildID, returnChunkID, threadID, nil
	}
	repoCandidates := repoMatchCandidates(req.Repo)
	const qPR = `UPDATE vanadis_chunks c SET pr_number = $1, status = CASE WHEN c.status IN ('merged', 'deploying', 'completed', 'failed') THEN c.status ELSE 'pr_opened' END, started_at = COALESCE(c.started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP FROM vanadis_builds b WHERE c.build_id = b.id AND c.id = $2 AND (cardinality($3::text[]) = 0 OR c.repo = ANY($3)) AND c.status != 'cancelled' AND (c.status NOT IN ('merged', 'deploying', 'completed') OR c.pr_number = $1) AND b.status NOT IN ('cancelled', 'completed') RETURNING b.id, c.id, b.thread_id;`
	var buildID, returnChunkID, threadID string
	err := s.db.QueryRow(ctx, qPR, req.PRNumber, chunkID, repoCandidates).Scan(&buildID, &returnChunkID, &threadID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", "", s.resolveAttachChunkPRError(ctx, chunkID, req.Repo, req.PRNumber)
		}
		return "", "", "", fmt.Errorf("attach pr %s#%d: %w", req.Repo, req.PRNumber, err)
	}
	return buildID, returnChunkID, threadID, nil
}

func (s *PostgresVanadisStore) attachStandalonePR(ctx context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	repo := normalizeRepo(req.Repo)
	if repo == "" {
		return "", "", "", errors.New("repo is required for standalone attach")
	}
	tx, txErr := s.db.Begin(ctx)
	if txErr != nil {
		return "", "", "", fmt.Errorf("begin standalone build tx: %w", txErr)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] standalone attach tx rollback error: %v", rbErr)
		}
	}()
	candidates := repoMatchCandidates(repo)
	const findExisting = `SELECT c.build_id, c.id, b.thread_id FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.repo = ANY($1) AND c.pr_number = $2 AND c.status != 'cancelled' LIMIT 1;`
	var existingBuildID, existingChunkID, existingThreadID string
	fErr := tx.QueryRow(ctx, findExisting, candidates, req.PRNumber).Scan(&existingBuildID, &existingChunkID, &existingThreadID)
	if fErr == nil {
		if cErr := tx.Commit(ctx); cErr != nil {
			return "", "", "", fmt.Errorf("commit existing pr attach: %w", cErr)
		}
		return existingBuildID, existingChunkID, existingThreadID, nil
	}
	if !errors.Is(fErr, pgx.ErrNoRows) {
		return "", "", "", fmt.Errorf("find existing pr attach %s#%d: %w", repo, req.PRNumber, fErr)
	}
	buildID := generateBuildID()
	chunkID := buildID + "-chunk-1"
	title := firstNonEmpty(strings.TrimSpace(req.Title), fmt.Sprintf("PR #%d: %s", req.PRNumber, cleanRepoName(repo)))
	threadID := normalizeDiscordThreadID(req.TargetID)
	now := time.Now().UTC()
	build := VanadisBuild{
		ID:        buildID,
		ThreadID:  threadID,
		Tier:      0,
		Title:     title,
		Objective: title,
		Status:    statusRunning,
		Quiet:     req.Quiet,
		CreatedAt: now,
		UpdatedAt: now,
	}
	chunk := VanadisChunk{
		ID:           chunkID,
		BuildID:      buildID,
		ChunkIndex:   0,
		Mode:         "pr",
		Repo:         repo,
		Title:        title,
		Instructions: title,
		Status:       statusPROpened,
		PRNumber:     req.PRNumber,
		Effort:       effortMedium,
		DependsOn:    []string{},
		StartedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	bQ := `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`
	if _, err := tx.Exec(ctx, bQ, build.ID, build.ThreadID, build.Tier, build.Title, build.Objective, "", build.Status, "", build.Quiet, now, now); err != nil {
		return "", "", "", fmt.Errorf("insert standalone build %s: %w", buildID, err)
	}
	if err := insertBuildChunk(ctx, tx, buildID, chunk, insertChunkSQL); err != nil {
		return "", "", "", fmt.Errorf("insert standalone chunk %s: %w", chunkID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", "", fmt.Errorf("commit standalone pr build: %w", err)
	}
	return buildID, chunkID, threadID, nil
}

func (s *PostgresVanadisStore) resolveAttachChunkPRError(ctx context.Context, chunkID, repo string, prNumber int) error {
	var exists bool
	checkErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1);`, chunkID).Scan(&exists)
	if checkErr == nil && !exists {
		return fmt.Errorf("%w: %s", ErrChunkNotFound, chunkID)
	}
	if repo == "" {
		return fmt.Errorf("%w: chunk %s or parent build is cancelled", ErrBuildConflict, chunkID)
	}
	var isCancelled bool
	cancErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND (c.status = 'cancelled' OR c.status IN ('merged', 'deploying', 'completed') OR b.status IN ('cancelled', 'completed')));`, chunkID).Scan(&isCancelled)
	if cancErr == nil && isCancelled {
		return fmt.Errorf("%w: chunk %s or parent build is in terminal status", ErrBuildConflict, chunkID)
	}
	candidates := repoMatchCandidates(repo)
	var repoMismatch bool
	mismatchErr := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vanadis_chunks WHERE id = $1 AND repo != '' AND NOT (repo = ANY($2)));`, chunkID, candidates).Scan(&repoMismatch)
	if mismatchErr == nil && repoMismatch {
		return fmt.Errorf("%w: chunk %s has mismatching repo", ErrBuildConflict, chunkID)
	}
	return fmt.Errorf("attach pr failed for chunk %s #%d", chunkID, prNumber)
}

func (s *PostgresVanadisStore) UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*VanadisBuild, error) {
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
	if currentStatus != statusRunning {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, currentStatus)
	}

	const updateQ = `UPDATE vanadis_builds SET title = CASE WHEN $2 != '' THEN $2 ELSE title END, objective = CASE WHEN $3 != '' THEN $3 ELSE objective END, shared_context = CASE WHEN $4 != '' THEN $4 ELSE shared_context END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, updateQ, buildID, title, obj, sharedCtx); err != nil {
		return nil, fmt.Errorf("update build %s: %w", buildID, err)
	}

	var updated VanadisBuild
	fetchQ := `SELECT ` + buildColumns + ` FROM vanadis_builds WHERE id = $1;`
	if err := tx.QueryRow(ctx, fetchQ, buildID).Scan(&updated.ID, &updated.ThreadID, &updated.Tier, &updated.Title, &updated.Objective, &updated.SharedContext, &updated.Status, &updated.StatusMessageID, &updated.Quiet, &updated.CreatedAt, &updated.UpdatedAt); err != nil {
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
		if _, err := normalizeChunkMode(*req.Mode); err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
	}
	var normEffort string
	if strings.TrimSpace(req.Effort) != "" {
		var effErr error
		normEffort, effErr = normalizeChunkEffort(req.Effort)
		if effErr != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidBuildInput, effErr)
		}
	}
	return normEffort, nil
}

func resolveChunkRef(chunks []VanadisChunk, chunkRef string) int {
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

func applyChunkModeUpdates(chunk *VanadisChunk, req UpdateChunkRequest, effectiveMode, effectiveRepo string) error {
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

func applyChunkUpdates(chunk *VanadisChunk, req UpdateChunkRequest, normEffort string) error {
	effectiveMode := chunk.Mode
	if req.Mode != nil {
		effectiveMode = *req.Mode
	}
	effectiveRepo := chunk.Repo
	if strings.TrimSpace(req.Repo) != "" {
		effectiveRepo = normalizeRepo(req.Repo)
	}
	normMode, modeErr := normalizeChunkMode(effectiveMode)
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
		chunk.DependsOn = nonNilSlice(*req.DependsOn)
	}
	return nil
}

func (s *PostgresVanadisStore) UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*VanadisChunk, error) {
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
	if buildStatus != statusRunning {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, selectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks for update: %w", err)
	}
	existingChunks, err := scanChunks(cRows)
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

	normalizedDeps, err := validateUpdateChunkDAG(existingChunks, targetIdx, updatedCandidate)
	if err != nil {
		return nil, err
	}
	updatedCandidate.DependsOn = normalizedDeps

	const updQ = `UPDATE vanadis_chunks SET mode = $3, repo = $4, title = $5, instructions = $6, target_files = $7, target_symbols = $8, effort = $9, depends_on = $10, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND build_id = $2;`
	_, err = tx.Exec(ctx, updQ, updatedCandidate.ID, buildID, updatedCandidate.Mode, updatedCandidate.Repo, updatedCandidate.Title, updatedCandidate.Instructions, nonNilSlice(updatedCandidate.TargetFiles), nonNilSlice(updatedCandidate.TargetSymbols), updatedCandidate.Effort, nonNilSlice(updatedCandidate.DependsOn))
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
		if id := strings.TrimSpace(inp.ID); id != "" && !isValidVanadisChunkID(id) {
			return fmt.Errorf("%w: chunk[%d] invalid id %q", ErrInvalidBuildInput, i, id)
		}
		if err := validateChunkModeAndRepo(i, inp.Mode, inp.Repo, inp.TargetFiles, inp.TargetSymbols); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidBuildInput, err)
		}
		if _, err := normalizeChunkEffort(inp.Effort); err != nil {
			return fmt.Errorf("%w: chunk[%d] %w", ErrInvalidBuildInput, i, err)
		}
	}
	return nil
}

func buildAppendedChunks(buildID string, existingIDs map[string]bool, startIndex int, inputs []CreateChunkInput, now time.Time) ([]VanadisChunk, error) {
	newChunks := make([]VanadisChunk, len(inputs))
	for i, inp := range inputs {
		cid := strings.TrimSpace(inp.ID)
		if cid == "" {
			cid = fmt.Sprintf("%s-chunk-%d", buildID, startIndex+i+1)
		}
		if existingIDs[cid] {
			return nil, fmt.Errorf("%w: duplicate chunk id %q in build %s", ErrBuildConflict, cid, buildID)
		}
		existingIDs[cid] = true
		cMode, err := normalizeChunkMode(inp.Mode)
		if err != nil {
			return nil, err
		}
		cEffort, err := normalizeChunkEffort(inp.Effort)
		if err != nil {
			return nil, err
		}
		cRepo := normalizeRepo(inp.Repo)
		if cMode == chunkModeAction {
			cRepo = ""
		}
		newChunks[i] = VanadisChunk{
			ID:                 cid,
			BuildID:            buildID,
			ChunkIndex:         startIndex + i,
			Mode:               cMode,
			Repo:               cRepo,
			Title:              strings.TrimSpace(inp.Title),
			Instructions:       strings.TrimSpace(inp.Instructions),
			TargetFiles:        nonNilSlice(inp.TargetFiles),
			TargetSymbols:      nonNilSlice(inp.TargetSymbols),
			Effort:             cEffort,
			AcceptanceCriteria: nonNilSlice(inp.AcceptanceCriteria),
			Status:             statusPending,
			DependsOn:          inp.DependsOn,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
	}
	return newChunks, nil
}

func (s *PostgresVanadisStore) AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]VanadisChunk, error) {
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
	if buildStatus != statusRunning {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, selectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks for append: %w", err)
	}
	existingChunks, err := scanChunks(cRows)
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

	validatedNew, err := validateAppendChunksDAG(existingChunks, newChunks)
	if err != nil {
		return nil, err
	}

	for _, c := range validatedNew {
		if err := insertBuildChunk(ctx, tx, buildID, c, insertChunkSQL); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit append chunks tx: %w", err)
	}
	return validatedNew, nil
}

func (s *PostgresVanadisStore) RemoveChunk(ctx context.Context, buildID, chunkRef string) (*VanadisChunk, []VanadisChunk, error) {
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

	remaining := make([]VanadisChunk, 0, len(existingChunks)-1)
	remaining = append(remaining, existingChunks[:targetIdx]...)
	remaining = append(remaining, existingChunks[targetIdx+1:]...)

	relinkDependenciesAfterRemoval(remaining, removed.ID, removed.DependsOn)

	validatedRemaining, err := normalizeAndValidateBuildDAG(remaining)
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

func lockBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) ([]VanadisChunk, error) {
	var buildStatus string
	bErr := tx.QueryRow(ctx, `SELECT status FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID).Scan(&buildStatus)
	if bErr != nil {
		if errors.Is(bErr, pgx.ErrNoRows) {
			return nil, ErrBuildNotFound
		}
		return nil, fmt.Errorf("lock build: %w", bErr)
	}
	if buildStatus != statusRunning {
		return nil, fmt.Errorf("%w: build %s has status %q (must be running)", ErrBuildConflict, buildID, buildStatus)
	}

	cRows, err := tx.Query(ctx, selectChunksForUpdateSQL, buildID)
	if err != nil {
		return nil, fmt.Errorf("lock chunks: %w", err)
	}
	chunks, err := scanChunks(cRows)
	cRows.Close()
	return chunks, err
}

func reindexRemainingChunks(ctx context.Context, tx pgx.Tx, buildID string, chunks []VanadisChunk) error {
	const updQ = `UPDATE vanadis_chunks SET chunk_index = $3, depends_on = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND build_id = $2;`
	for i := range chunks {
		chunks[i].ChunkIndex = i
		if _, err := tx.Exec(ctx, updQ, chunks[i].ID, buildID, i, nonNilSlice(chunks[i].DependsOn)); err != nil {
			return fmt.Errorf("reindex chunk %s: %w", chunks[i].ID, err)
		}
	}
	return nil
}

func actionCompletionAllowed(chunk VanadisChunk, buildStatus, dispatchID string) bool {
	identityMatches := dispatchID == actionDispatchID(chunk) || (dispatchID == "" && chunk.StallNudges == 0)
	return chunk.Mode == chunkModeAction && chunk.Status == statusRunning && buildStatus == statusRunning && chunk.PRNumber == 0 && identityMatches
}

func (s *PostgresVanadisStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*VanadisAdvancementResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, errors.New("chunk id cannot be empty")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin complete action chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] complete action chunk tx rollback error: %v", rbErr)
		}
	}()

	var b VanadisBuild
	const selectBuildSQL = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, status, COALESCE(status_message_id, '') FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks WHERE id = $1) FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectBuildSQL, chunkID).Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.Status, &b.StatusMessageID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("lock action build: %w", err)
	}
	var cID, bID, cTitle, cStatus, cMode string
	var chunkIdx, prNumber, stallNudges int
	const selectChunkSQL = `SELECT id, build_id, chunk_index, mode, title, status, COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks WHERE id = $1 AND build_id = $2 FOR UPDATE;`
	if err := tx.QueryRow(ctx, selectChunkSQL, chunkID, b.ID).Scan(&cID, &bID, &chunkIdx, &cMode, &cTitle, &cStatus, &prNumber, &stallNudges); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("lock action chunk %s: %w", chunkID, err)
	}

	res := &VanadisAdvancementResult{
		Matched:          true,
		Advanced:         false,
		BuildID:          bID,
		ThreadID:         b.ThreadID,
		Tier:             b.Tier,
		Title:            b.Title,
		Objective:        b.Objective,
		BuildStatus:      b.Status,
		StatusMessageID:  b.StatusMessageID,
		MergedChunkIndex: chunkIdx,
		MergedChunkTitle: cTitle,
		ChunkMode:        cMode,
		NextChunkIndex:   chunkIdx + 1,
		BuildCompleted:   false,
	}

	chunk := VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}
	res.CompletionAccepted = actionCompletionAccepted(chunk, b.Status, dispatchID)
	if !actionCompletionAllowed(VanadisChunk{ID: cID, Mode: cMode, Status: cStatus, PRNumber: prNumber, StallNudges: stallNudges}, b.Status, dispatchID) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit complete action chunk tx: %w", err)
		}
		return res, nil
	}

	const completeChunkSQL = `UPDATE vanadis_chunks SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, completeChunkSQL, chunkID); err != nil {
		return nil, fmt.Errorf("complete action chunk %s: %w", chunkID, err)
	}
	res.Advanced = true

	const countRemainingSQL = `SELECT COUNT(*) FROM vanadis_chunks WHERE build_id = $1 AND id != $2 AND status != 'completed';`
	var remainingCount int
	if err := tx.QueryRow(ctx, countRemainingSQL, bID, chunkID).Scan(&remainingCount); err != nil {
		return nil, fmt.Errorf("count remaining chunks for build %s: %w", bID, err)
	}

	if remainingCount == 0 {
		if err := finishActionBuildIfAllCompleted(ctx, tx, bID, res); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit complete action chunk tx: %w", err)
	}
	return res, nil
}

func finishActionBuildIfAllCompleted(ctx context.Context, tx pgx.Tx, buildID string, res *VanadisAdvancementResult) error {
	const completeBuildSQL = `UPDATE vanadis_builds SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, completeBuildSQL, buildID); err != nil {
		return fmt.Errorf("complete action build %s: %w", buildID, err)
	}
	res.BuildCompleted = true
	res.BuildStatus = statusCompleted

	const selectAllChunksSQL = `SELECT id, chunk_index, mode, COALESCE(repo, ''), title, status, COALESCE(pr_number, 0), COALESCE(started_at, created_at), COALESCE(completed_at, updated_at), updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := tx.Query(ctx, selectAllChunksSQL, buildID)
	if err != nil {
		return fmt.Errorf("query chunks for completed action build %s: %w", buildID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cs VanadisChunkSummary
		if err := rows.Scan(&cs.ID, &cs.ChunkIndex, &cs.Mode, &cs.Repo, &cs.Title, &cs.Status, &cs.PRNumber, &cs.StartedAt, &cs.CompletedAt, &cs.UpdatedAt); err != nil {
			return fmt.Errorf("scan chunk summary for build %s: %w", buildID, err)
		}
		res.Chunks = append(res.Chunks, cs)
	}
	return rows.Err()
}

func (s *PostgresVanadisStore) FailChunk(ctx context.Context, chunkID, reason, dispatchID string) (*VanadisBuild, *VanadisChunk, bool, error) {
	if s == nil || s.db == nil {
		return nil, nil, false, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, nil, false, errors.New("chunk id cannot be empty")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, nil, false, fmt.Errorf("begin fail chunk tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			log.Printf("[vanadis] fail chunk tx rollback error: %v", rbErr)
		}
	}()

	parentID, lockErr := lockFailureParent(ctx, tx, chunkID)
	if lockErr != nil {
		return nil, nil, false, lockErr
	}

	const selectChunkQuery = `SELECT c.id, c.build_id, c.chunk_index, c.mode, c.repo, c.title, c.instructions, c.target_files, c.target_symbols, COALESCE(c.effort, 'medium'), c.acceptance_criteria, c.status, COALESCE(c.pr_number, 0), c.started_at, c.completed_at, c.created_at, c.updated_at, c.stall_nudges, b.id, b.thread_id, b.tier, COALESCE(NULLIF(b.title, ''), b.objective), b.objective, COALESCE(b.shared_context, ''), b.status, COALESCE(b.status_message_id, ''), b.created_at, b.updated_at FROM vanadis_chunks c JOIN vanadis_builds b ON c.build_id = b.id WHERE c.id = $1 AND c.build_id = $2 FOR UPDATE OF c;`

	var c VanadisChunk
	var b VanadisBuild
	var startedAt, completedAt sql.NullTime
	err = tx.QueryRow(ctx, selectChunkQuery, chunkID, parentID).Scan(
		&c.ID, &c.BuildID, &c.ChunkIndex, &c.Mode, &c.Repo, &c.Title, &c.Instructions, &c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status, &c.PRNumber, &startedAt, &completedAt, &c.CreatedAt, &c.UpdatedAt, &c.StallNudges,
		&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, false, ErrChunkNotFound
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("select chunk %s for fail: %w", chunkID, err)
	}
	if startedAt.Valid {
		c.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		c.CompletedAt = completedAt.Time
	}

	if failureSkipped(c, b.Status, dispatchID) {
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, false, fmt.Errorf("commit fail chunk tx: %w", err)
		}
		return &b, &c, false, nil
	}

	now := time.Now().UTC()
	c.Status = statusFailed
	c.CompletedAt = now
	c.UpdatedAt = now
	banner := fmt.Sprintf("Chunk %d failed: %s", c.ChunkIndex+1, reason)
	c.Banner = banner

	const failChunkSQL = `UPDATE vanadis_chunks SET status = 'failed', banner = $2, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := tx.Exec(ctx, failChunkSQL, chunkID, banner); err != nil {
		return nil, nil, false, fmt.Errorf("fail chunk %s: %w", chunkID, err)
	}

	if b.Status != statusFailed && b.Status != statusCancelled {
		b.Status = statusFailed
		b.UpdatedAt = now
		const failBuildSQL = `UPDATE vanadis_builds SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
		if _, err := tx.Exec(ctx, failBuildSQL, b.ID); err != nil {
			return nil, nil, false, fmt.Errorf("fail build %s: %w", b.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, false, fmt.Errorf("commit fail chunk tx: %w", err)
	}
	return &b, &c, true, nil
}

type chunkPRMatch struct {
	chunkID      string
	buildID      string
	chunkIdx     int
	effort       string
	hasRemaining bool
}

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

func (s *PostgresVanadisStore) UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = CASE WHEN $1 != '' THEN $1 ELSE status END, banner = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3;`, strings.TrimSpace(status), strings.TrimSpace(banner), id)
	switch {
	case err != nil:
		return fmt.Errorf("update chunk banner and status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrChunkNotFound
	default:
		return nil
	}
}

func (s *PostgresVanadisStore) RecordPRCheckStatus(ctx context.Context, chunkID, checkName, checkState string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	name := strings.TrimSpace(checkName)
	if id == "" || name == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET ci_checks = jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true), updated_at = CURRENT_TIMESTAMP WHERE id = $3;`, name, strings.TrimSpace(checkState), id)
	switch {
	case err != nil:
		return fmt.Errorf("record pr check status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrChunkNotFound
	default:
		return nil
	}
}
