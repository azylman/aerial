package main

import (
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
	jsonNull     = "null"
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
	DispatchID string `json:"dispatch_id,omitempty"`
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
	GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error)
	ListBuilds(ctx context.Context, limit int) ([]VanadisBuildEntry, error)
	GetChunk(ctx context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error)
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
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

const (
	serviceName = "vanadis"
)

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

// VanadisServer manages HTTP endpoints and JetStream worker coordination for Vanadis builds.
type VanadisServer struct {
	cfg        Config
	restateCfg RestateConfig
	httpClient *http.Client
	store      VanadisStore
	queueStats consumerStatsQuerier
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

func (s *VanadisServer) consumerStatsQuerier() consumerStatsQuerier {
	if s == nil {
		return nil
	}
	return s.queueStats
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
	DependsOn          []string `json:"depends_on"`
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
		"/chunks/attach-pr":          server.handleAttachChunkPR,
		"/chunks/":                   server.handleChunkByID,
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
		if valErr := validateChunkModeAndRepo(i, c.Mode, c.Repo, c.TargetFiles, c.TargetSymbols); valErr != nil {
			return nil, valErr
		}
		repo := ""
		if mode == "pr" {
			repo = normalizeRepo(c.Repo)
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
	req.ID = buildID
	req.ThreadID = threadID
	req.Tier = tier
	req.Objective = objective
	if strings.TrimSpace(req.Title) == "" {
		req.Title = objective
	}

	chunks, err := parseCreateBuildChunks(buildID, now, req.Chunks)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	var created VanadisBuild
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/Create", buildID), req, &created); err != nil {
		writeStoreErr(w, err)
		return
	}

	RecordBuildTotal("created")
	httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
		fieldStatus: "accepted", fieldBuildID: created.ID, "thread_id": created.ThreadID, "tier": created.Tier,
		"status_message_id": created.StatusMessageID, "chunk_count": len(chunks), "first_chunk_id": chunks[0].ID, fieldChunks: chunks,
	})
}

func (s *VanadisServer) handleBuildCancel(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var cancelled VanadisBuild
	if err := s.callRestateIngress(r.Context(), fmt.Sprintf("/Build/%s/Cancel", id), CancelBuildRequest{}, &cancelled); err != nil {
		writeStoreErr(w, err)
		return
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
