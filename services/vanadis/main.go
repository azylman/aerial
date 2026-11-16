package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

const (
	effortLow    = "low"
	effortMedium = "medium"
)

var (
	// ErrBuildNotFound indicates a requested VanadisBuild does not exist in PostgreSQL.
	ErrBuildNotFound = errors.New("vanadis build not found")
	// ErrChunkNotFound indicates a requested VanadisChunk does not exist in PostgreSQL.
	ErrChunkNotFound = errors.New("vanadis chunk not found")
	// ErrBuildConflict indicates a VanadisBuild or chunk state conflict (e.g. not running or not pending).
	ErrBuildConflict = errors.New("vanadis build state conflict")
	// ErrInvalidBuildInput indicates invalid inputs for a VanadisBuild or chunk mutation.
	ErrInvalidBuildInput = errors.New("invalid vanadis build input")
)

// Config holds runtime configuration for the Vanadis orchestrator service.
type Config struct {
	Port         string
	PostgresURL  string
	BrainURL     string
	EriURL       string
	NATSURL      string
	GitHubAPIURL string
	GitHubToken  string
	ConfigPath   string
	DeployUIURL  string
}

// VanadisYAMLConfig defines the declarative file schema for /config/vanadis.yaml.
type VanadisYAMLConfig struct {
	Port         string `yaml:"port"`
	PostgresURL  string `yaml:"postgres_url"`
	EriURL       string `yaml:"eri_url"`
	NATSURL      string `yaml:"nats_url"`
	GitHubAPIURL string `yaml:"github_api_url"`
	GitHubToken  string `yaml:"github_token"`
	DeployUIURL  string `yaml:"deploy_ui_url"`
}

var vanadisFallbackConfigPath = "/config/vanadis.yaml"

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

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg VanadisYAMLConfig
	target := strings.TrimSpace(configPath)
	if fi, err := os.Stat(target); err != nil || fi.Size() == 0 {
		target = ""
		if fi, err := os.Stat(vanadisFallbackConfigPath); err == nil && fi.Size() > 0 {
			target = vanadisFallbackConfigPath
		}
	}
	if target != "" {
		if data, err := os.ReadFile(target); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse yaml config at %s: %v", target, err)
			}
		}
	}
	pgURL := firstNonEmpty(httpkit.BuildPostgresDSN(os.Getenv, "aerial"), strings.TrimSpace(fileCfg.PostgresURL))
	eriURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("ERI_URL"), fileCfg.EriURL)), "/")
	return Config{
		Port:         firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4025"),
		PostgresURL:  pgURL,
		BrainURL:     eriURL,
		EriURL:       eriURL,
		NATSURL:      firstNonEmpty(os.Getenv("NATS_URL"), strings.TrimSpace(fileCfg.NATSURL)),
		GitHubAPIURL: strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("GITHUB_API_URL"), fileCfg.GitHubAPIURL, "https://api.github.com")), "/"),
		GitHubToken:  firstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GITHUB_PAT"), fileCfg.GitHubToken),
		ConfigPath:   strings.TrimSpace(configPath),
		DeployUIURL:  strings.TrimRight(strings.TrimSpace(fileCfg.DeployUIURL), "/"),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config { return LoadConfig(os.Getenv("CONFIG_PATH")) }

// ExecuteChunkArgs defines the job payload for executing a single Vanadis build chunk.
type ExecuteChunkArgs struct {
	BuildID    string `json:"build_id"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkID    string `json:"chunk_id,omitempty"`
}

func capChunkEffort(effort string) string {
	if strings.EqualFold(strings.TrimSpace(effort), "low") {
		return effortLow
	}
	return effortMedium
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

// VanadisBuild represents a multi-chunk autonomous engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Title           string    `json:"title"`
	Objective       string    `json:"objective"`
	SharedContext   string    `json:"shared_context,omitempty"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	Quiet           bool      `json:"quiet"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID                 string                 `json:"id"`
	BuildID            string                 `json:"build_id"`
	ChunkIndex         int                    `json:"chunk_index"`
	Mode               string                 `json:"mode"`
	Repo               string                 `json:"repo,omitempty"`
	Title              string                 `json:"title"`
	Instructions       string                 `json:"instructions"`
	TargetFiles        []string               `json:"target_files"`
	TargetSymbols      []string               `json:"target_symbols"`
	Effort             string                 `json:"effort"`
	AcceptanceCriteria []string               `json:"acceptance_criteria"`
	Status             string                 `json:"status"`
	PRNumber           int                    `json:"pr_number,omitempty"`
	StallNudges        int                    `json:"stall_nudges,omitempty"`
	DependsOn          []string               `json:"depends_on"`
	CIFailCount        int                    `json:"ci_fail_count"`
	CIChecks           map[string]string      `json:"ci_checks,omitempty"`
	RolloutState       *pubsub.PRRolloutState `json:"rollout_state,omitempty"`
	Banner             string                 `json:"banner,omitempty"`
	MergeAwakeableID   string                 `json:"merge_awakeable_id,omitempty"`
	DeployAwakeableID  string                 `json:"deploy_awakeable_id,omitempty"`
	StartedAt          time.Time              `json:"started_at,omitempty"`
	CompletedAt        time.Time              `json:"completed_at,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
}

// VanadisBuildEntry represents a summarized VanadisBuild including overall queue state and chunk aggregates.
type VanadisBuildEntry struct {
	VanadisBuild

	QueueState      string         `json:"queue_state"`
	TotalChunks     int            `json:"total_chunks"`
	CompletedChunks int            `json:"completed_chunks"`
	RunningChunks   int            `json:"running_chunks"`
	PendingChunks   int            `json:"pending_chunks"`
	FailedChunks    int            `json:"failed_chunks"`
	Chunks          []VanadisChunk `json:"chunks"`
}

// ClassifyBuildQueueState computes the overall queue state ("running", "queued", or "completed") based on buildStatus and child chunks.
func ClassifyBuildQueueState(buildStatus string, chunks []VanadisChunk) string {
	switch strings.ToLower(strings.TrimSpace(buildStatus)) {
	case statusCompleted, statusMerged, statusFailed, statusCancelled, "canceled":
		return statusCompleted
	case statusRunning:
		for _, c := range chunks {
			if strings.ToLower(strings.TrimSpace(c.Status)) != statusPending {
				return statusRunning
			}
		}
		if len(chunks) > 0 {
			return statusQueued
		}
		return statusRunning
	default:
		return statusQueued
	}
}

// SummarizeBuildEntry constructs a VanadisBuildEntry with non-nil chunks, chunk status counts, and computed queue state.
func SummarizeBuildEntry(build VanadisBuild, chunks []VanadisChunk) VanadisBuildEntry {
	chunks = nonNilSlice(chunks)
	var completed, running, pending, failed int
	for _, c := range chunks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case statusCompleted, statusMerged:
			completed++
		case statusRunning, statusCoding, statusPROpened, statusDeploying:
			running++
		case statusFailed:
			failed++
		case statusPending:
			pending++
		}
	}
	return VanadisBuildEntry{
		VanadisBuild: build, QueueState: ClassifyBuildQueueState(build.Status, chunks),
		TotalChunks: len(chunks), CompletedChunks: completed, RunningChunks: running, PendingChunks: pending, FailedChunks: failed, Chunks: chunks,
	}
}

// VanadisStore defines the persistence contract for Vanadis builds and chunks.
type VanadisStore interface {
	CreateBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error
	GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error)
	ListBuilds(ctx context.Context, limit int) ([]VanadisBuildEntry, error)
	UpdateChunkStatus(ctx context.Context, chunkID, status string) error
	ReserveChunkForLaunch(ctx context.Context, chunkID string) error
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error
	GetChunk(ctx context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error)
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
	AttachChunkPR(ctx context.Context, req AttachChunkPRRequest) (buildID, chunkID, threadID string, err error)
	CompleteActionChunk(ctx context.Context, chunkID string) (*VanadisAdvancementResult, error)
	ClaimStalledChunk(ctx context.Context, chunkID string, dispatchID ...string) (StalledChunkVerdict, *VanadisBuild, *VanadisChunk, error)
	FailChunk(ctx context.Context, chunkID, reason string) (*VanadisBuild, *VanadisChunk, bool, error)
	FailChunkByPR(ctx context.Context, repo string, prNumber int) (buildID, chunkID string, failed bool, err error)
	UpdateBuild(ctx context.Context, buildID string, req UpdateBuildRequest) (*VanadisBuild, error)
	UpdateChunk(ctx context.Context, buildID, chunkRef string, req UpdateChunkRequest) (*VanadisChunk, error)
	AppendChunks(ctx context.Context, buildID string, inputs []CreateChunkInput) ([]VanadisChunk, error)
	RemoveChunk(ctx context.Context, buildID, chunkRef string) (*VanadisChunk, []VanadisChunk, error)
	RecordPRCheckStatus(ctx context.Context, chunkID, checkName, checkState string) error
	RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error
	ResetChunkForSynchronize(ctx context.Context, chunkID, buildID string) error
	IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string) (int, error)
	UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error
}

type pgxQueryExecer interface {
	Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresVanadisStore implements VanadisStore backed by PostgreSQL (pgxpool.Pool).
type PostgresVanadisStore struct {
	db pgxQueryExecer
}

// NewPostgresVanadisStore constructs a new PostgresVanadisStore.
func NewPostgresVanadisStore(db pgxQueryExecer) *PostgresVanadisStore {
	return &PostgresVanadisStore{db: db}
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

func isChunkDoneStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusMerged, statusCompleted:
		return true
	default:
		return false
	}
}

func isChunkMergedOrDoneStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusMerged, statusDeploying, statusCompleted:
		return true
	default:
		return false
	}
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

func execInsertVanadisChunk(ctx context.Context, tx pgx.Tx, buildID string, c VanadisChunk) error {
	const cQ = `INSERT INTO vanadis_chunks (
		id, build_id, chunk_index, mode, repo, title, instructions,
		target_files, target_symbols, effort, acceptance_criteria, status,
		pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks,
		rollout_state, banner, merge_awakeable_id, deploy_awakeable_id,
		started_at, completed_at, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7,
		$8, $9, $10, $11, $12,
		$13, $14, $15, $16, $17,
		$18, $19, $20, $21,
		$22, $23, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
	);`
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

// CreateBuild inserts a VanadisBuild and all of its ordered VanadisChunks in a single transaction.
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
	chunks = normChunks
	tier := build.Tier
	if tier <= 0 {
		tier = 2
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
	const bQ = `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`
	if _, err := tx.Exec(ctx, bQ, buildID, threadID, tier, firstNonEmpty(strings.TrimSpace(build.Title), objective), objective, strings.TrimSpace(build.SharedContext), firstNonEmpty(strings.TrimSpace(build.Status), "running"), strings.TrimSpace(build.StatusMessageID), build.Quiet); err != nil {
		return fmt.Errorf("failed to insert vanadis build: %w", err)
	}
	for _, c := range chunks {
		if err := execInsertVanadisChunk(ctx, tx, buildID, c); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// GetBuild retrieves a VanadisBuild and its ordered VanadisChunks by buildID.
func (s *PostgresVanadisStore) GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil, errors.New("database connection is nil")
	}
	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return nil, nil, errors.New("build id cannot be empty")
	}
	const buildQuery = `SELECT ` + buildColumns + ` FROM vanadis_builds WHERE id = $1;`
	b, err := scanBuild(s.db.QueryRow(ctx, buildQuery, buildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s: %w", buildID, err)
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
	rows, err := s.db.Query(ctx, chunksQuery, buildID)
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

// ListBuilds queries recent Vanadis builds with their ordered chunks, summarized as VanadisBuildEntry items.
func (s *PostgresVanadisStore) ListBuilds(ctx context.Context, limit int) ([]VanadisBuildEntry, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("database connection is nil")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const buildQuery = `SELECT ` + buildColumns + ` FROM vanadis_builds ORDER BY created_at DESC, id DESC LIMIT $1;`
	rows, err := s.db.Query(ctx, buildQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis builds: %w", err)
	}
	defer rows.Close()
	var builds []VanadisBuild
	var buildIDs []string
	for rows.Next() {
		b, scanBuildErr := scanBuild(rows)
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
		return []VanadisBuildEntry{}, nil
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), COALESCE(stall_nudges, 0), depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = ANY($1) ORDER BY build_id, chunk_index ASC;`
	cRows, err := s.db.Query(ctx, chunksQuery, buildIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis chunks: %w", err)
	}
	defer cRows.Close()
	allChunks, err := scanChunks(cRows)
	if err != nil {
		return nil, err
	}
	chunksByBuild := make(map[string][]VanadisChunk, len(builds))
	for _, c := range allChunks {
		chunksByBuild[c.BuildID] = append(chunksByBuild[c.BuildID], c)
	}
	entries := make([]VanadisBuildEntry, len(builds))
	for i, b := range builds {
		entries[i] = SummarizeBuildEntry(b, nonNilSlice(chunksByBuild[b.ID]))
	}
	return entries, nil
}

func (s *PostgresVanadisStore) execUpdateByID(ctx context.Context, idLabel, id, valLabel, val string, allowEmptyVal bool, query, opLabel string, notFoundErr error) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id, val = strings.TrimSpace(id), strings.TrimSpace(val)
	if id == "" || (!allowEmptyVal && val == "") {
		lbl := idLabel
		if id != "" {
			lbl = valLabel
		}
		return fmt.Errorf("%s cannot be empty", lbl)
	}
	tag, err := s.db.Exec(ctx, query, val, id)
	if err != nil {
		return fmt.Errorf("%s: %w", opLabel, err)
	}
	if tag.RowsAffected() == 0 {
		return notFoundErr
	}
	return nil
}

// UpdateChunkStatus updates the lifecycle status of a VanadisChunk.
func (s *PostgresVanadisStore) UpdateChunkStatus(ctx context.Context, chunkID, status string) error {
	return s.execUpdateByID(ctx, "chunk id", chunkID, "status", status, false, `UPDATE vanadis_chunks SET status = $1, started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, completed_at = CASE WHEN $1 IN ('merged', 'completed', 'failed', 'cancelled') THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE NULL END, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis chunk status", ErrChunkNotFound)
}

// UpdateBuildStatus updates the lifecycle status of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "status", status, false, `UPDATE vanadis_builds SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status", ErrBuildNotFound)
}

// UpdateBuildStatusMessageID updates the status_message_id of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "message_id", messageID, true, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status_message_id", ErrBuildNotFound)
}

// BrainDispatchRequest represents the JSON payload dispatched to Brain's POST /dispatch endpoint.
type BrainDispatchRequest struct {
	ID             string `json:"id,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	ID            string    `json:"id,omitempty"`
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	ActiveStep    string    `json:"active_step,omitempty"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	SkipSink  bool                `json:"skip_sink,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
	StartedAt time.Time           `json:"started_at,omitempty"`
}

// VanadisServer manages HTTP endpoints and JetStream worker coordination for Vanadis builds.
type VanadisServer struct {
	cfg        Config
	restateCfg RestateConfig
	httpClient *http.Client
	store      VanadisStore
	bus        pubsub.Bus
	queueStats consumerStatsQuerier

	launchMu      sync.Mutex
	localLaunched map[string]map[string]bool
}

// NewVanadisServer constructs a new VanadisServer instance.
func NewVanadisServer(cfg Config, store VanadisStore, httpClient *http.Client) *VanadisServer {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	// Traced so eri turns join the chunk dispatch span (and widget/wake posts carry traceparent).
	return &VanadisServer{cfg: cfg, restateCfg: LoadRestateConfig(os.Getenv("CONFIG_PATH")), httpClient: tracing.Client(httpClient), store: store}
}

// SetStore updates the VanadisStore backing the server.
func (s *VanadisServer) SetStore(store VanadisStore) { s.store = store }

// SetBus sets the pubsub.Bus for event emission and consumption.
func (s *VanadisServer) SetBus(bus pubsub.Bus) { s.bus = bus }

func (s *VanadisServer) consumerStatsQuerier() consumerStatsQuerier {
	if s == nil {
		return nil
	}
	if s.queueStats != nil {
		return s.queueStats
	}
	if csp, ok := s.bus.(consumerStatsQuerier); ok {
		return csp
	}
	return nil
}

func (s *VanadisServer) postBrainJSON(ctx context.Context, path string, payload any, errReadLimit int64) ([]byte, error) {
	if s == nil || s.httpClient == nil {
		return nil, errors.New("http client is nil")
	}
	brainURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(s.cfg.BrainURL, s.cfg.EriURL)), "/")
	if brainURL == "" {
		return nil, errors.New("brain URL is not configured")
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if errReadLimit <= 0 {
			errReadLimit = 4 << 10
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, errReadLimit))
		if readErr != nil {
			return nil, fmt.Errorf("%s returned status %d (read body error: %w)", path, resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("%s returned status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return io.ReadAll(resp.Body)
}

func (s *VanadisServer) syncBuildWidget(ctx context.Context, buildID, banner string) {
	if s == nil || s.bus == nil {
		return
	}
	cleanBuildID := strings.TrimSpace(buildID)
	if cleanBuildID == "" {
		return
	}
	cleanBanner := strings.TrimSpace(banner)
	args := pubsub.WidgetSyncJobArgs{
		BuildID: cleanBuildID,
		Banner:  cleanBanner,
	}
	msgID := fmt.Sprintf("widget:build:%s:%d", cleanBuildID, time.Now().UnixNano())
	if err := s.bus.Publish(ctx, pubsub.SubjectBuildWidgetSyncCoalesced(cleanBuildID), msgID, args); err != nil {
		log.Printf("[vanadis] warning: publish coalesced widget sync failed for build %s: %v", cleanBuildID, err)
	}
}

func (s *VanadisServer) dispatchWakePrompt(ctx context.Context, threadID, prompt string) error {
	if s == nil || s.httpClient == nil {
		return nil
	}
	cleanThreadID := strings.TrimSpace(threadID)
	cleanPrompt := strings.TrimSpace(prompt)
	if cleanThreadID == "" || cleanPrompt == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": cleanThreadID,
		"prompt":     cleanPrompt,
	}
	_, err := s.postBrainJSON(ctx, "/prompt", payload, 4<<10)
	if err != nil {
		log.Printf("[vanadis] warning: failed to dispatch wake prompt to thread %s: %v", cleanThreadID, err)
		return err
	}
	return nil
}

func (s *VanadisServer) enqueueExecuteChunk(ctx context.Context, args ExecuteChunkArgs) error {
	if s.bus == nil {
		return errors.New("event bus not configured")
	}
	msgID := fmt.Sprintf("vanadis:chunk:%s:%d", args.BuildID, args.ChunkIndex)
	if strings.TrimSpace(args.ChunkID) != "" {
		msgID = fmt.Sprintf("vanadis:chunk:%s:%s", args.BuildID, strings.TrimSpace(args.ChunkID))
	}
	return s.bus.Publish(ctx, "aerial.jobs.vanadis.chunk", msgID, args)
}

func settleFailedChunk(msg pubsub.Message, isFinalAttempt bool) {
	if isFinalAttempt {
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[vanadis] warning: error terming chunk job: %v", termErr)
		}
		return
	}
	delay := time.Duration(min(max(msg.NumDelivered(), 1), 3)) * 5 * time.Second
	if nakErr := msg.NakWithDelay(delay); nakErr != nil {
		log.Printf("[vanadis] warning: error naking chunk job: %v", nakErr)
	}
}

// HandleExecuteChunkJob processes an aerial.jobs.vanadis.chunk execution message from JetStream.
func (s *VanadisServer) HandleExecuteChunkJob(ctx context.Context, msg pubsub.Message) error {
	var args ExecuteChunkArgs
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[vanadis] failed to unmarshal chunk job payload: %v", err)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[vanadis] warning: error terming chunk job: %v", termErr)
		}
		return nil
	}
	isFinalAttempt := msg.NumDelivered() >= 3
	err := s.ProcessExecuteChunk(ctx, args.BuildID, args.ChunkIndex, isFinalAttempt, args.ChunkID)
	if err != nil {
		settleFailedChunk(msg, isFinalAttempt)
	}
	return err
}

// StartJobConsumers subscribes the vanadis durable queue consumers to the configured bus.
func (s *VanadisServer) StartJobConsumers(ctx context.Context) error {
	if s.bus == nil {
		return nil
	}
	if _, err := s.bus.ConsumeQueue(ctx, "aerial.jobs.vanadis.chunk", "vanadis-chunk", s.HandleExecuteChunkJob); err != nil {
		return err
	}
	log.Println("[vanadis] Subscribed vanadis-chunk queue consumer to aerial.jobs.vanadis.chunk")
	if _, err := s.bus.ConsumeQueue(ctx, "aerial.coalesced.widget_sync.>", "vanadis-widget", s.HandleCoalescedWidgetMessage); err != nil {
		return err
	}
	log.Printf("[vanadis] Subscribed vanadis-widget queue consumer to %s", "aerial.coalesced.widget_sync.>")
	return nil
}

func (s *VanadisServer) handleFinalDispatchFailure(ctx context.Context, build *VanadisBuild, chunk VanadisChunk, totalChunks int, dispatchErr error) {
	if cErr := s.store.UpdateChunkStatus(ctx, chunk.ID, "failed"); cErr != nil {
		log.Printf("[vanadis] failed marking chunk %s failed on final attempt: %v", chunk.ID, cErr)
	}
	if bErr := s.store.UpdateBuildStatus(ctx, build.ID, "failed"); bErr != nil {
		log.Printf("[vanadis] failed marking build %s failed on final attempt: %v", build.ID, bErr)
	}
	RecordChunkTotal("failed")
	RecordBuildTotal("failed")
	attempt := 1
	s.syncBuildWidget(ctx, build.ID, fmt.Sprintf("Chunk %d/%d (%s) failed to dispatch to Brain after %d attempts: %v", chunk.ChunkIndex+1, totalChunks, chunk.Title, attempt, dispatchErr))
}

// ProcessExecuteChunk loads build state, formats the chunk prompt, and dispatches it to Brain.
func (s *VanadisServer) ProcessExecuteChunk(ctx context.Context, buildID string, chunkIndex int, isFinalAttempt bool, targetChunkID ...string) error {
	if s == nil || s.store == nil {
		return errors.New("vanadis store not configured")
	}
	build, chunks, err := s.store.GetBuild(ctx, buildID)
	if errors.Is(err, ErrBuildNotFound) {
		log.Printf("[vanadis] build %s not found, skipping chunk %d", buildID, chunkIndex)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get build %s: %w", buildID, err)
	}
	if build.Status != statusRunning {
		log.Printf("[vanadis] build %s has status %q (not running), skipping chunk %d", buildID, build.Status, chunkIndex)
		return nil
	}
	var desiredID string
	if len(targetChunkID) > 0 {
		desiredID = strings.TrimSpace(targetChunkID[0])
	}
	activeIdx, err := findTargetChunkIndex(chunks, chunkIndex, desiredID, buildID)
	if err != nil {
		return err
	}
	chunk := chunks[activeIdx]
	if chunk.Status == statusPROpened || isChunkMergedOrDoneStatus(chunk.Status) {
		log.Printf("[vanadis] chunk %s (index %d) already in status %q, skipping", chunk.ID, chunkIndex, chunk.Status)
		return nil
	}
	if err := checkChunkDependenciesReady(chunk, chunks); err != nil {
		return err
	}
	if err := s.store.UpdateChunkStatus(ctx, chunk.ID, "running"); err != nil {
		return fmt.Errorf("update chunk %s status to running: %w", chunk.ID, err)
	}
	now := time.Now().UTC()
	chunks[activeIdx].Status = statusRunning
	if chunks[activeIdx].StartedAt.IsZero() {
		chunks[activeIdx].StartedAt = now
	}
	chunks[activeIdx].UpdatedAt = now
	if err := s.dispatchChunkTurn(ctx, *build, chunks, activeIdx, ""); err != nil {
		if isFinalAttempt {
			s.handleFinalDispatchFailure(ctx, build, chunk, len(chunks), err)
		}
		return err
	}
	RecordChunkTotal("dispatched")
	return nil
}

func normalizeDiscordThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func isNumericSnowflake(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// CreateChunkInput defines a single chunk in a CreateBuildRequest.
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

// CreateBuildRequest defines the JSON request body for POST /builds.
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

func generateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

type rowScanner interface {
	Scan(dest ...any) error
}

// buildColumns is the vanadis_builds SELECT list scanBuild consumes, in its destination order.
// Every query whose rows go through scanBuild must select exactly these columns.
const buildColumns = `id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, quiet, created_at, updated_at`

// scanBuild scans one row selected with buildColumns.
func scanBuild(s rowScanner) (VanadisBuild, error) {
	var b VanadisBuild
	err := s.Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.Quiet, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

func writeStoreErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrInvalidBuildInput):
		status = http.StatusBadRequest
	case errors.Is(err, ErrBuildNotFound) || errors.Is(err, ErrChunkNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrBuildConflict):
		status = http.StatusConflict
	}
	writeErr(w, status, err.Error())
}

// SetupMux configures and returns the HTTP ServeMux for a VanadisServer.
func SetupMux(server *VanadisServer) *http.ServeMux {
	mux := http.NewServeMux()
	if server != nil {
		mux.HandleFunc("GET /metrics", server.handleMetrics)
	} else {
		mux.Handle("GET /metrics", promhttp.Handler())
	}
	coverageFlush := httpkit.HandleCoverageFlush("vanadis")
	mcpHandler := tracing.Middleware("vanadis", server.newMCPHandler()).ServeHTTP
	for p, h := range map[string]http.HandlerFunc{
		"GET /health": server.handleHealth, "GET /healthz": server.handleHealth,
		"GET /builds": server.handleListBuilds, "GET /runs": server.handleListBuilds,
		"POST /builds": server.handleCreateBuild, "POST /runs": server.handleCreateBuild,
		"/builds/": server.handleBuildByID, "/runs/": server.handleBuildByID,
		"/chunks/attach-pr": server.handleAttachChunkPR,
		"/chunks/":          server.handleChunkByID,
		"POST /internal/restate/builds/{id}/transition": server.handleRestateBuildTransition,
		"/mcp":                       mcpHandler,
		"/sse":                       mcpHandler,
		"POST /debug/coverage/flush": coverageFlush, "GET /debug/coverage/flush": coverageFlush,
	} {
		mux.HandleFunc(p, h)
	}
	return mux
}

// Routes returns the HTTP handler for the server.
func (s *VanadisServer) Routes() http.Handler { return SetupMux(s) }

func (*VanadisServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
}

func parseCreateBuildChunks(buildID string, now time.Time, rawChunks []CreateChunkInput) ([]VanadisChunk, error) {
	chunks := make([]VanadisChunk, len(rawChunks))
	for i, c := range rawChunks {
		title, instructions := strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions)
		if title == "" || instructions == "" {
			return nil, fmt.Errorf("chunk[%d] title and instructions are required", i)
		}
		chunkID := firstNonEmpty(strings.TrimSpace(c.ID), fmt.Sprintf("%s-chunk-%d", buildID, i+1))
		if !isValidVanadisChunkID(chunkID) {
			return nil, fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		mode, err := normalizeChunkMode(c.Mode)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] %w", i, err)
		}
		repo := ""
		switch mode {
		case "pr":
			repo = normalizeRepo(c.Repo)
			if repo == "" {
				return nil, fmt.Errorf("chunk[%d] repo is required for pr mode", i)
			}
		case chunkModeAction:
			if strings.TrimSpace(c.Repo) != "" {
				return nil, fmt.Errorf("chunk[%d] action chunk must have empty repo", i)
			}
			if len(c.TargetFiles) > 0 {
				return nil, fmt.Errorf("chunk[%d] action chunk must not specify target_files", i)
			}
			if len(c.TargetSymbols) > 0 {
				return nil, fmt.Errorf("chunk[%d] action chunk must not specify target_symbols", i)
			}
		}
		effort, err := normalizeChunkEffort(c.Effort)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] invalid effort: %w", i, err)
		}
		status := statusPending
		var startedAt time.Time
		if i == 0 {
			status = statusRunning
			startedAt = now
		}
		chunks[i] = VanadisChunk{
			ID: chunkID, BuildID: buildID, ChunkIndex: i, Mode: mode,
			Repo: repo, Title: title, Instructions: instructions,
			TargetFiles: nonNilSlice(c.TargetFiles), TargetSymbols: nonNilSlice(c.TargetSymbols), Effort: effort,
			AcceptanceCriteria: nonNilSlice(c.AcceptanceCriteria), Status: status, DependsOn: c.DependsOn, StartedAt: startedAt, CreatedAt: now, UpdatedAt: now,
		}
	}
	return normalizeAndValidateBuildDAG(chunks)
}

func (s *VanadisServer) handleCreateBuild(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req CreateBuildRequest
	if err = json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	threadID, objective := normalizeDiscordThreadID(req.ThreadID), strings.TrimSpace(req.Objective)
	switch {
	case !isNumericSnowflake(threadID):
		writeErr(w, http.StatusBadRequest, "thread_id is required and must be a numeric Discord snowflake")
		return
	case objective == "":
		writeErr(w, http.StatusBadRequest, "objective is required")
		return
	case len(req.Chunks) == 0:
		writeErr(w, http.StatusBadRequest, "at least one chunk is required")
		return
	}
	buildID := firstNonEmpty(strings.TrimSpace(req.ID), generateBuildID())
	tier := req.Tier
	if tier <= 0 {
		tier = 2
	}
	now := time.Now().UTC()
	build := VanadisBuild{
		ID: buildID, ThreadID: threadID, Tier: tier, Title: firstNonEmpty(strings.TrimSpace(req.Title), objective),
		Objective: objective, SharedContext: strings.TrimSpace(req.SharedContext), Status: statusRunning, Quiet: req.Quiet, CreatedAt: now, UpdatedAt: now,
	}
	chunks, err := parseCreateBuildChunks(buildID, now, req.Chunks)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.CreateBuild(r.Context(), build, chunks); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("failed to create build: %v", err))
		return
	}
	s.syncBuildWidget(r.Context(), build.ID, "")
	if err := s.launchCreatedBuild(r.Context(), buildID); err != nil {
		if statusErr := s.store.UpdateBuildStatus(r.Context(), buildID, "failed"); statusErr != nil {
			log.Printf("[vanadis] warning marking build %s failed after enqueue error: %v", buildID, statusErr)
		}
		RecordBuildTotal("failed")
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue chunk 0: %v", err))
		return
	}
	RecordBuildTotal("created")
	httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
		fieldStatus: "accepted", fieldBuildID: build.ID, "thread_id": build.ThreadID, "tier": build.Tier,
		"status_message_id": build.StatusMessageID, "chunk_count": len(chunks), "first_chunk_id": chunks[0].ID, fieldChunks: chunks,
	})
}

func isCancellableChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusPending, statusRunning, statusCoding:
		return true
	default:
		return false
	}
}

func (s *VanadisServer) cancelBuildChunksAndPrompts(ctx context.Context, buildID string, existing *VanadisBuild, existingChunks []VanadisChunk) {
	now := time.Now().UTC()
	for i := range existingChunks {
		if !isCancellableChunkStatus(existingChunks[i].Status) {
			continue
		}
		if s.store != nil {
			if err := s.store.UpdateChunkStatus(ctx, existingChunks[i].ID, "cancelled"); err != nil {
				log.Printf("[vanadis] warning: failed marking chunk %s cancelled: %v", existingChunks[i].ID, err)
			}
		}
		RecordChunkTotal("cancelled")
		existingChunks[i].Status = statusCancelled
		existingChunks[i].CompletedAt = now
		existingChunks[i].UpdatedAt = now
	}
	if _, err := s.postBrainJSON(ctx, "/dispatch/cancel", map[string]string{fieldBuildID: buildID}, 4<<10); err != nil {
		log.Printf("[vanadis] warning: failed cancelling pending brain dispatches for build %s: %v", buildID, err)
	}
	if existing != nil {
		existing.Status = statusCancelled
		s.syncBuildWidget(ctx, buildID, "")
	}
}

func (s *VanadisServer) handleBuildCancel(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	existing, existingChunks, gErr := s.store.GetBuild(r.Context(), id)
	if gErr == nil && existing != nil && (existing.Status == statusCompleted || existing.Status == statusFailed) {
		writeErr(w, http.StatusConflict, fmt.Sprintf("cannot cancel build in terminal status %q", existing.Status))
		return
	}
	if err := s.store.UpdateBuildStatus(r.Context(), id, "cancelled"); err != nil {
		writeStoreErr(w, err)
		return
	}
	s.cancelBuildChunksAndPrompts(r.Context(), id, existing, existingChunks)
	if err := s.reconcileBuildOnRestate(r.Context(), id, "cancelled"); err != nil {
		log.Printf("[vanadis] warning: failed signaling restate cancel for build %s: %v", id, err)
	}
	RecordBuildTotal("cancelled")
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: statusCancelled, fieldBuildID: id})
}

func (s *VanadisServer) handleBuildRoot(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet:
		build, chunks, err := s.store.GetBuild(r.Context(), id)
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{fieldBuild: build, fieldChunks: chunks, "entry": SummarizeBuildEntry(*build, chunks)})
	case http.MethodPatch:
		s.handleUpdateBuild(w, r, id)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *VanadisServer) handleBuildChunkRoutes(w http.ResponseWriter, r *http.Request, id, chunkRef string) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetBuildChunk(w, r, id, chunkRef)
	case http.MethodPatch:
		s.handleUpdateChunk(w, r, id, chunkRef)
	case http.MethodDelete:
		s.handleRemoveChunk(w, r, id, chunkRef)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *VanadisServer) handleBuildByID(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	raw := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/runs/"), "/builds/")
	hasTrailingSlash := strings.HasSuffix(raw, "/")
	clean := strings.TrimSuffix(raw, "/")
	if clean == "" {
		writeErr(w, http.StatusBadRequest, "build id is required")
		return
	}
	parts := strings.Split(clean, "/")
	id := strings.TrimSpace(parts[0])
	if id == "" {
		writeErr(w, http.StatusBadRequest, "build id is required")
		return
	}
	switch len(parts) {
	case 1:
		s.handleBuildRoot(w, r, id)
	case 2:
		switch parts[1] {
		case "cancel":
			s.handleBuildCancel(w, r, id)
		case fieldChunks:
			if hasTrailingSlash && (r.Method == http.MethodGet || r.Method == http.MethodPatch || r.Method == http.MethodDelete) {
				writeErr(w, http.StatusBadRequest, "chunk_ref is required")
				return
			}
			if r.Method != http.MethodPost {
				writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			s.handleAppendChunks(w, r, id)
		default:
			writeErr(w, http.StatusNotFound, "not found")
		}
	case 3:
		if parts[1] == fieldChunks {
			chunkRef := strings.TrimSpace(parts[2])
			if chunkRef == "" {
				writeErr(w, http.StatusBadRequest, "chunk_ref is required")
				return
			}
			s.handleBuildChunkRoutes(w, r, id, chunkRef)
			return
		}
		writeErr(w, http.StatusNotFound, "not found")
	default:
		writeErr(w, http.StatusNotFound, "not found")
	}
}

func (s *VanadisServer) handleListBuilds(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := 50
	if parsed, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit"))); err == nil && parsed > 0 {
		limit = parsed
	}
	allBuilds, err := s.store.ListBuilds(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	var running, queued, completed int
	qFilter, sFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("queue_state"))), strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	filtered := make([]VanadisBuildEntry, 0, len(allBuilds))
	for _, b := range allBuilds {
		switch b.QueueState {
		case statusRunning:
			running++
		case statusQueued:
			queued++
		case statusCompleted:
			completed++
		}
		if (qFilter == "" || strings.EqualFold(b.QueueState, qFilter)) && (sFilter == "" || strings.EqualFold(b.QueueState, sFilter) || strings.EqualFold(b.Status, sFilter)) {
			filtered = append(filtered, b)
		}
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus: "ok", "total": len(filtered), "running_count": running,
		"queued_count": queued, "completed_count": completed, "builds": filtered,
	})
}

var newPgxPoolFn = pgxpool.NewWithConfig

func initPostgres(ctx context.Context, server *VanadisServer, pgURL string) (func(context.Context), error) {
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns, poolConfig.MinConns = 10, 2
	pool, err := newPgxPoolFn(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	server.SetStore(NewPostgresVanadisStore(pool))
	return func(context.Context) {
		pool.Close()
	}, nil
}

var newVanadisBusFn = func(ctx context.Context, url string) (pubsub.Bus, error) {
	return pubsub.NewJetStreamBus(ctx, url)
}

func initVanadisJetStream(ctx context.Context, server *VanadisServer, natsURL string) (func(), error) {
	return initVanadisJetStreamWithFactory(ctx, server, natsURL, newVanadisBusFn)
}

func initVanadisJetStreamWithFactory(ctx context.Context, server *VanadisServer, natsURL string, factory func(ctx context.Context, url string) (pubsub.Bus, error)) (func(), error) {
	natsURL = strings.TrimSpace(natsURL)
	if natsURL == "" {
		log.Println("[vanadis] warning: nats_url is not configured; event bus disabled")
		return func() {}, nil
	}
	b, err := factory(ctx, natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect to nats at %s: %w", natsURL, err)
	}
	server.SetBus(b)
	if err := pubsub.EnsureCoreStreams(ctx, b); err != nil {
		if closeErr := b.Close(); closeErr != nil {
			log.Printf("[vanadis] warning: failed closing bus: %v", closeErr)
		}
		server.SetBus(nil)
		return nil, fmt.Errorf("ensure core streams: %w", err)
	}
	if _, err := b.ConsumeQueue(ctx, "aerial.dispatch.*", "vanadis-dispatch", server.HandleDispatchOutcomeEvent); err != nil {
		if closeErr := b.Close(); closeErr != nil {
			log.Printf("[vanadis] warning: failed closing bus: %v", closeErr)
		}
		server.SetBus(nil)
		return nil, fmt.Errorf("subscribe vanadis-dispatch: %w", err)
	}
	log.Println("[vanadis] Subscribed vanadis-dispatch queue consumer to aerial.dispatch.*")
	if _, err := b.ConsumeQueue(ctx, "aerial.pr.>", "vanadis-pr-events", server.HandlePRLifecycleEvent); err != nil {
		if closeErr := b.Close(); closeErr != nil {
			log.Printf("[vanadis] warning: failed closing bus: %v", closeErr)
		}
		server.SetBus(nil)
		return nil, fmt.Errorf("subscribe vanadis-pr-events: %w", err)
	}
	log.Println("[vanadis] Subscribed vanadis-pr-events queue consumer to aerial.pr.>")
	if err := server.StartJobConsumers(ctx); err != nil {
		if closeErr := b.Close(); closeErr != nil {
			log.Printf("[vanadis] warning: failed closing bus: %v", closeErr)
		}
		server.SetBus(nil)
		return nil, fmt.Errorf("start vanadis job consumers: %w", err)
	}
	return func() {
		if closeErr := b.Close(); closeErr != nil {
			log.Printf("[vanadis] bus close error: %v", closeErr)
		}
	}, nil
}

func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewVanadisServer(cfg, nil, nil)
	var cleanupDB func(context.Context)
	if cfg.PostgresURL != "" {
		cleanup, err := initPostgres(ctx, server, cfg.PostgresURL)
		if err != nil {
			return err
		}
		cleanupDB = cleanup
		log.Println("[vanadis] PostgreSQL store initialized")
	}

	cleanupBus, err := initVanadisJetStream(ctx, server, cfg.NATSURL)
	if err != nil {
		if cleanupDB != nil {
			cleanupDB(context.WithoutCancel(ctx))
		}
		return err
	}
	defer cleanupBus()

	restateCtx, restateCancel := context.WithCancel(ctx)
	defer restateCancel()
	if _, err = startRestateServer(restateCtx, server); err != nil && !errors.Is(err, ErrRestateDisabled) {
		log.Printf("[vanadis] warning: failed starting restate server: %v", err)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
	if err != nil {
		if cleanupDB != nil {
			cleanupDB(context.WithoutCancel(ctx))
		}
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}
	httpServer := &http.Server{Handler: server.Routes(), ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	if onReady != nil {
		onReady(ln.Addr().String())
	}
	errChan := make(chan error, 1)
	go func() {
		log.Printf("[vanadis] Server listening on %s (BrainURL: %s)", ln.Addr().String(), cfg.BrainURL)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errChan:
	}
	log.Println("[vanadis] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer shutdownCancel()
	if shutErr := httpServer.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[vanadis] HTTP shutdown error: %v", shutErr)
	}
	if cleanupDB != nil {
		cleanupDB(shutdownCtx)
	}
	log.Println("[vanadis] Server stopped")
	return runErr
}

// RunApp runs the Vanadis orchestrator application until the context is canceled.
func RunApp(ctx context.Context, cfg Config) error { return runServer(ctx, cfg, nil) }

func main() {
	cfg := LoadConfigFromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	_, flushTracing := tracing.Init(ctx, "vanadis", tracing.DefaultConfigPath)

	err := RunApp(ctx, cfg)
	cancel()
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[vanadis] Error: %v", err)
	}
}
