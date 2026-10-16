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
	"runtime/coverage"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"gopkg.in/yaml.v3"
)

// VanadisQueueName is the dedicated River queue name for Vanadis chunk execution jobs.
const VanadisQueueName = "vanadis"

var (
	// ErrBuildNotFound indicates a requested VanadisBuild does not exist in PostgreSQL.
	ErrBuildNotFound = errors.New("vanadis build not found")
	// ErrChunkNotFound indicates a requested VanadisChunk does not exist in PostgreSQL.
	ErrChunkNotFound = errors.New("vanadis chunk not found")
)

// Config holds runtime configuration for the Vanadis orchestrator service.
type Config struct {
	Port        string
	PostgresURL string
	BrainURL    string
}

// VanadisYAMLConfig defines the declarative file schema for /config/vanadis.yaml.
type VanadisYAMLConfig struct {
	Port        string `yaml:"port"`
	PostgresURL string `yaml:"postgres_url"`
	BrainURL    string `yaml:"brain_url"`
}

var (
	vanadisFallbackConfigPath = "/config/vanadis.yaml"
	lookupHostFn              = func(ctx context.Context, host string) ([]string, error) {
		return net.DefaultResolver.LookupHost(ctx, host)
	}
)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
func LoadConfig(configPath string) Config {
	var fileCfg VanadisYAMLConfig
	targetPath := strings.TrimSpace(configPath)
	if targetPath != "" {
		if fi, err := os.Stat(targetPath); err != nil || fi.Size() == 0 {
			targetPath = ""
		}
	}
	if targetPath == "" {
		if fi, err := os.Stat(vanadisFallbackConfigPath); err == nil && fi.Size() > 0 {
			targetPath = vanadisFallbackConfigPath
		}
	}
	if targetPath != "" {
		if data, err := os.ReadFile(targetPath); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[vanadis] Warning: failed to parse yaml config at %s: %v", targetPath, err)
			}
		}
	}

	pgURL := firstNonEmpty(os.Getenv("POSTGRES_URL"), os.Getenv("DATABASE_URL"), fileCfg.PostgresURL)
	if pgURL == "" {
		hostResolves := func(host string) bool {
			ctxLookup, cancelLookup := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelLookup()
			_, err := lookupHostFn(ctxLookup, host)
			return err == nil
		}
		if hostResolves("aerial-postgres") {
			pgURL = "postgres://aerial:aerial_secure_pass@aerial-postgres:5432/aerial?sslmode=disable"
		} else if hostResolves("postgres") {
			pgURL = "postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable"
		} else {
			pgURL = "postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable"
		}
	}

	return Config{
		Port:        firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4025"),
		PostgresURL: pgURL,
		BrainURL:    strings.TrimRight(firstNonEmpty(os.Getenv("BRAIN_INTERNAL_URL"), os.Getenv("BRAIN_URL"), fileCfg.BrainURL, "http://127.0.0.1:8088"), "/"),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config {
	return LoadConfig(os.Getenv("CONFIG_PATH"))
}

// ExecuteChunkArgs defines the River job payload for executing a single Vanadis build chunk.
type ExecuteChunkArgs struct {
	BuildID    string `json:"build_id" river:"unique"`
	ChunkIndex int    `json:"chunk_index" river:"unique"`
}

// Kind returns the River job kind identifier for ExecuteChunkArgs.
func (ExecuteChunkArgs) Kind() string { return "vanadis_execute_chunk" }

// InsertOpts returns the River queue and deduplication options for ExecuteChunkArgs.
func (ExecuteChunkArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       VanadisQueueName,
		MaxAttempts: 3,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
}

// RiverInserter defines the interface for inserting jobs into River.
type RiverInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

// normalizeChunkEffort trims and lowercases effort, defaulting "" to "low" and validating "low", "medium", "high".
func normalizeChunkEffort(effort string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(effort))
	if trimmed == "" {
		return "low", nil
	}
	switch trimmed {
	case "low", "medium", "high":
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid chunk effort %q: must be 'low', 'medium', or 'high'", effort)
	}
}

// VanadisBuild represents a multi-chunk autonomous engineering build run.
type VanadisBuild struct {
	ID              string    `json:"id"`
	Repo            string    `json:"repo"`
	ThreadID        string    `json:"thread_id"`
	Tier            int       `json:"tier"`
	Title           string    `json:"title"`
	Objective       string    `json:"objective"`
	SharedContext   string    `json:"shared_context,omitempty"`
	Status          string    `json:"status"`
	StatusMessageID string    `json:"status_message_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
type VanadisChunk struct {
	ID                 string    `json:"id"`
	BuildID            string    `json:"build_id"`
	ChunkIndex         int       `json:"chunk_index"`
	Repo               string    `json:"repo,omitempty"`
	Title              string    `json:"title"`
	Instructions       string    `json:"instructions"`
	TargetFiles        []string  `json:"target_files"`
	TargetSymbols      []string  `json:"target_symbols"`
	Effort             string    `json:"effort"`
	AcceptanceCriteria []string  `json:"acceptance_criteria"`
	Status             string    `json:"status"`
	PRNumber           int       `json:"pr_number,omitempty"`
	DiffStat           string    `json:"diff_stat,omitempty"`
	StartedAt          time.Time `json:"started_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// VanadisStore defines the persistence contract for Vanadis builds and chunks.
type VanadisStore interface {
	CreateBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error
	GetBuild(ctx context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error)
	UpdateChunkStatus(ctx context.Context, chunkID, status string) error
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error
}

type pgxQueryExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
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

// normalizeRepo ensures repository names are normalized with the owner prefix.
func normalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// CreateBuild inserts a VanadisBuild and all of its ordered VanadisChunks in a single transaction.
func (s *PostgresVanadisStore) CreateBuild(ctx context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}

	buildID := strings.TrimSpace(build.ID)
	if buildID == "" {
		return errors.New("build id cannot be empty")
	}
	repo := normalizeRepo(build.Repo)
	if repo == "" {
		return errors.New("repo cannot be empty")
	}
	threadID := strings.TrimSpace(build.ThreadID)
	if threadID == "" {
		return errors.New("thread_id cannot be empty")
	}
	objective := strings.TrimSpace(build.Objective)
	if objective == "" {
		return errors.New("objective cannot be empty")
	}
	buildTitle := firstNonEmpty(strings.TrimSpace(build.Title), objective)
	tier := build.Tier
	if tier <= 0 {
		tier = 2
	}
	status := strings.TrimSpace(build.Status)
	if status == "" {
		status = "running"
	}

	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" {
			return fmt.Errorf("chunk[%d] id cannot be empty", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if strings.TrimSpace(c.Title) == "" {
			return fmt.Errorf("chunk[%d] title cannot be empty", i)
		}
		if strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] instructions cannot be empty", i)
		}
		if _, err := normalizeChunkEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
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

	statusMessageID := strings.TrimSpace(build.StatusMessageID)
	sharedContext := strings.TrimSpace(build.SharedContext)
	buildQuery := `
		INSERT INTO vanadis_builds (id, repo, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`
	if _, err := tx.Exec(ctx, buildQuery, buildID, repo, threadID, tier, buildTitle, objective, sharedContext, status, statusMessageID); err != nil {
		return fmt.Errorf("failed to insert vanadis build: %w", err)
	}

	chunkQuery := `
		INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, diff_stat, started_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`
	for _, c := range chunks {
		chunkID := strings.TrimSpace(c.ID)
		title := strings.TrimSpace(c.Title)
		instructions := strings.TrimSpace(c.Instructions)
		chunkRepo := normalizeRepo(c.Repo)
		if chunkRepo == "" {
			chunkRepo = repo
		}
		cStatus := strings.TrimSpace(c.Status)
		if cStatus == "" {
			cStatus = "pending"
		}
		cEffort, err := normalizeChunkEffort(c.Effort)
		if err != nil {
			return fmt.Errorf("chunk %s: %w", chunkID, err)
		}
		var prNum any
		if c.PRNumber > 0 {
			prNum = c.PRNumber
		}
		var startedAt any
		if !c.StartedAt.IsZero() {
			startedAt = c.StartedAt
		} else if cStatus == "running" || cStatus == "coding" {
			startedAt = time.Now().UTC()
		}
		files := c.TargetFiles
		if files == nil {
			files = []string{}
		}
		symbols := c.TargetSymbols
		if symbols == nil {
			symbols = []string{}
		}
		criteria := c.AcceptanceCriteria
		if criteria == nil {
			criteria = []string{}
		}
		if _, err := tx.Exec(ctx, chunkQuery,
			chunkID, buildID, c.ChunkIndex, chunkRepo, title, instructions, files, symbols, cEffort, criteria, cStatus, prNum, c.DiffStat, startedAt,
		); err != nil {
			return fmt.Errorf("failed to insert vanadis chunk %s: %w", chunkID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
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

	buildQuery := `
		SELECT id, repo, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at
		FROM vanadis_builds
		WHERE id = $1;
	`
	var b VanadisBuild
	err := s.db.QueryRow(ctx, buildQuery, buildID).Scan(
		&b.ID, &b.Repo, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrBuildNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis build: %w", err)
	}

	chunksQuery := `
		SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'low'), acceptance_criteria, status, COALESCE(pr_number, 0), diff_stat, started_at, created_at, updated_at
		FROM vanadis_chunks
		WHERE build_id = $1
		ORDER BY chunk_index ASC;
	`
	rows, err := s.db.Query(ctx, chunksQuery, buildID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query vanadis chunks: %w", err)
	}
	defer rows.Close()

	chunks := make([]VanadisChunk, 0)
	for rows.Next() {
		var c VanadisChunk
		var startedAt sql.NullTime
		if err := rows.Scan(
			&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, &c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status, &c.PRNumber, &c.DiffStat, &startedAt, &c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		if startedAt.Valid {
			c.StartedAt = startedAt.Time
		}
		if c.TargetFiles == nil {
			c.TargetFiles = []string{}
		}
		if c.TargetSymbols == nil {
			c.TargetSymbols = []string{}
		}
		if c.AcceptanceCriteria == nil {
			c.AcceptanceCriteria = []string{}
		}
		if strings.TrimSpace(c.Effort) == "" {
			c.Effort = "low"
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating vanadis chunks: %w", err)
	}

	return &b, chunks, nil
}

// UpdateChunkStatus updates the lifecycle status of a VanadisChunk.
func (s *PostgresVanadisStore) UpdateChunkStatus(ctx context.Context, chunkID, status string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}

	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk id cannot be empty")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("status cannot be empty")
	}

	query := `
		UPDATE vanadis_chunks
		SET status = $1,
		    started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	tag, err := s.db.Exec(ctx, query, status, chunkID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis chunk status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	return nil
}

// UpdateBuildStatus updates the lifecycle status of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}

	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("build id cannot be empty")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("status cannot be empty")
	}

	query := `
		UPDATE vanadis_builds
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	tag, err := s.db.Exec(ctx, query, status, buildID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis build status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrBuildNotFound
	}
	return nil
}

// UpdateBuildStatusMessageID updates the status_message_id of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}

	buildID = strings.TrimSpace(buildID)
	if buildID == "" {
		return errors.New("build id cannot be empty")
	}

	query := `
		UPDATE vanadis_builds
		SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2;
	`
	tag, err := s.db.Exec(ctx, query, strings.TrimSpace(messageID), buildID)
	if err != nil {
		return fmt.Errorf("failed to update vanadis build status_message_id: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrBuildNotFound
	}
	return nil
}

// FormatChunkPrompt renders a structured, context-rich prompt for executing a specific Vanadis chunk.
func FormatChunkPrompt(build VanadisBuild, chunks []VanadisChunk, activeIdx int) (string, error) {
	if len(chunks) == 0 {
		return "", errors.New("chunks slice cannot be empty")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return "", fmt.Errorf("active chunk index %d out of range [0, %d)", activeIdx, len(chunks))
	}

	total := len(chunks)
	active := chunks[activeIdx]
	activeRepo := firstNonEmpty(normalizeRepo(active.Repo), normalizeRepo(build.Repo), "azylman/aerial")

	var sb strings.Builder
	sb.WriteString("[VANADIS BUILD CHUNK EXECUTION]\n")
	fmt.Fprintf(&sb, "Build ID: %s\n", build.ID)
	if bTitle := strings.TrimSpace(build.Title); bTitle != "" {
		fmt.Fprintf(&sb, "Build Title: %s\n", bTitle)
	}
	fmt.Fprintf(&sb, "Repository: %s\n", activeRepo)
	fmt.Fprintf(&sb, "Tier: %d\n", build.Tier)
	fmt.Fprintf(&sb, "Overall Objective: %s\n\n", build.Objective)

	if sharedCtx := strings.TrimSpace(build.SharedContext); sharedCtx != "" {
		fmt.Fprintf(&sb, "=== Shared Build Context ===\n%s\n\n", sharedCtx)
	}

	sb.WriteString("=== Completed Chunks Ledger ===\n")
	if activeIdx == 0 {
		sb.WriteString("(none — this is the first chunk)\n\n")
	} else {
		for k := 0; k < activeIdx; k++ {
			c := chunks[k]
			cRepo := firstNonEmpty(normalizeRepo(c.Repo), normalizeRepo(build.Repo), "azylman/aerial")
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s (%s, PR #%d)\n", k+1, total, c.Title, cRepo, c.PRNumber)
			if diff := strings.TrimSpace(c.DiffStat); diff != "" {
				fmt.Fprintf(&sb, "  DiffStat: %s\n", diff)
			}
		}
		sb.WriteString("\n")
	}

	targetFilesStr := "(none specified)"
	if len(active.TargetFiles) > 0 {
		targetFilesStr = strings.Join(active.TargetFiles, ", ")
	}
	fmt.Fprintf(&sb, "=== Active Chunk (Chunk %d of %d) ===\n", activeIdx+1, total)
	fmt.Fprintf(&sb, "Chunk ID: %s\n", active.ID)
	fmt.Fprintf(&sb, "Title: %s\n", active.Title)
	fmt.Fprintf(&sb, "Repository: %s\n", activeRepo)
	fmt.Fprintf(&sb, "Target Files: %s\n", targetFilesStr)
	if len(active.TargetSymbols) > 0 {
		fmt.Fprintf(&sb, "Target Symbols: %s\n", strings.Join(active.TargetSymbols, ", "))
	}
	fmt.Fprintf(&sb, "Instructions:\n%s\n\n", active.Instructions)

	if len(active.AcceptanceCriteria) > 0 {
		var criteriaLines []string
		for _, crit := range active.AcceptanceCriteria {
			if trimmed := strings.TrimSpace(crit); trimmed != "" {
				criteriaLines = append(criteriaLines, fmt.Sprintf("- %s", trimmed))
			}
		}
		if len(criteriaLines) > 0 {
			sb.WriteString("=== Acceptance Criteria ===\n")
			for _, line := range criteriaLines {
				sb.WriteString(line)
				sb.WriteByte('\n')
			}
			sb.WriteByte('\n')
		}
	}

	sb.WriteString("=== Remaining Chunks ===\n")
	if activeIdx+1 >= total {
		sb.WriteString("(none — this is the final chunk)\n\n")
	} else {
		sb.WriteString("DO NOT implement these remaining chunks in this turn—they are listed strictly for architectural boundary awareness.\n")
		for k := activeIdx + 1; k < total; k++ {
			c := chunks[k]
			cRepo := firstNonEmpty(normalizeRepo(c.Repo), normalizeRepo(build.Repo), "azylman/aerial")
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s (%s)\n", k+1, total, c.Title, cRepo)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("=== Mandatory Execution Contract ===\n")
	fmt.Fprintf(&sb, "1. Initialize a fresh scratch workspace off `origin/main`: `/share/aerial/scripts/aerial-pr.sh init %s`\n", activeRepo)
	sb.WriteString("2. Implement ONLY this chunk following TDD, run package tests and `./scripts/verify.sh --staged`, and write `PR_DESCRIPTION.md`.\n")
	sb.WriteString("3. Inspect `Target Files` and `Target Symbols` directly rather than running broad repo-wide searches.\n")
	fmt.Fprintf(&sb, "4. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`\n", active.ID)
	sb.WriteString("5. Terminate turn immediately after submission.\n")

	return sb.String(), nil
}

// ExecuteChunkWorker processes Vanadis chunk execution jobs from River.
type ExecuteChunkWorker struct {
	river.WorkerDefaults[ExecuteChunkArgs]
	server *VanadisServer
}

// Work executes a River job for a single Vanadis build chunk.
func (w *ExecuteChunkWorker) Work(ctx context.Context, job *river.Job[ExecuteChunkArgs]) error {
	if w == nil || w.server == nil {
		return errors.New("vanadis server not configured on worker")
	}
	isFinalAttempt := false
	if job != nil && job.JobRow != nil && job.MaxAttempts > 0 {
		isFinalAttempt = job.Attempt >= job.MaxAttempts
	}
	return w.server.ProcessExecuteChunk(ctx, job.Args.BuildID, job.Args.ChunkIndex, isFinalAttempt)
}

// BrainPromptRequest represents the JSON payload dispatched to Brain's POST /prompt endpoint.
type BrainPromptRequest struct {
	ChannelID      string `json:"channel_id"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	Silent         bool   `json:"silent,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DiffStat      string    `json:"diff_stat,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
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
	Chunks    []StatusWidgetChunk `json:"chunks"`
}

// VanadisServer manages HTTP endpoints and River worker coordination for Vanadis builds.
type VanadisServer struct {
	cfg         Config
	httpClient  *http.Client
	store       VanadisStore
	riverClient RiverInserter
}

// NewVanadisServer constructs a new VanadisServer instance.
func NewVanadisServer(cfg Config, store VanadisStore, riverClient RiverInserter, httpClient *http.Client) *VanadisServer {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &VanadisServer{
		cfg:         cfg,
		httpClient:  httpClient,
		store:       store,
		riverClient: riverClient,
	}
}

// SetStore updates the VanadisStore backing the server.
func (s *VanadisServer) SetStore(store VanadisStore) { s.store = store }

// SetRiverClient updates the RiverInserter backing the server.
func (s *VanadisServer) SetRiverClient(rc RiverInserter) { s.riverClient = rc }

func (s *VanadisServer) syncBuildWidget(ctx context.Context, build VanadisBuild, chunks []VanadisChunk, banner string) {
	if s == nil || s.httpClient == nil {
		return
	}
	brainURL := strings.TrimRight(firstNonEmpty(s.cfg.BrainURL, "http://127.0.0.1:8088"), "/")
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		wChunks[i] = StatusWidgetChunk{
			Index:     c.ChunkIndex,
			Repo:      c.Repo,
			Title:     c.Title,
			Status:    c.Status,
			PRNumber:  c.PRNumber,
			DiffStat:  c.DiffStat,
			StartedAt: c.StartedAt,
			UpdatedAt: c.UpdatedAt,
		}
	}

	payload := StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     firstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    build.Status,
		Banner:    banner,
		Chunks:    wChunks,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[vanadis] warning: failed to marshal widget request for build %s: %v", build.ID, err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+"/discord/widget", bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("[vanadis] warning: failed to create widget request for build %s: %v", build.ID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[vanadis] warning: post discord widget failed for build %s: %v", build.ID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		if readErr != nil {
			log.Printf("[vanadis] warning: discord widget returned status %d for build %s (read error: %v)", resp.StatusCode, build.ID, readErr)
			return
		}
		log.Printf("[vanadis] warning: discord widget returned status %d for build %s: %s", resp.StatusCode, build.ID, strings.TrimSpace(string(respBody)))
		return
	}

	var res struct {
		Status    string `json:"status"`
		MessageID string `json:"message_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Printf("[vanadis] warning: failed to decode widget response for build %s: %v", build.ID, err)
		return
	}

	if res.MessageID != "" && res.MessageID != build.StatusMessageID && s.store != nil {
		if err := s.store.UpdateBuildStatusMessageID(ctx, build.ID, res.MessageID); err != nil {
			log.Printf("[vanadis] warning: failed to persist status_message_id for build %s: %v", build.ID, err)
		}
	}
}

func (s *VanadisServer) insertRiverJob(ctx context.Context, args ExecuteChunkArgs) error {
	if s.riverClient == nil {
		return errors.New("river client not configured")
	}
	opts := args.InsertOpts()
	_, err := s.riverClient.Insert(ctx, args, &opts)
	return err
}

// ProcessExecuteChunk loads build state, formats the chunk prompt, and dispatches it to Brain.
func (s *VanadisServer) ProcessExecuteChunk(ctx context.Context, buildID string, chunkIndex int, isFinalAttempt bool) error {
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
	if build.Status != "running" {
		log.Printf("[vanadis] build %s has status %q (not running), skipping chunk %d", buildID, build.Status, chunkIndex)
		return nil
	}

	activeIdx := -1
	for i := range chunks {
		if chunks[i].ChunkIndex == chunkIndex {
			activeIdx = i
			break
		}
	}
	if activeIdx < 0 {
		return fmt.Errorf("chunk index %d not found in build %s", chunkIndex, buildID)
	}

	chunk := chunks[activeIdx]
	if chunk.Status == "pr_opened" || chunk.Status == "merged" {
		log.Printf("[vanadis] chunk %s (index %d) already in status %q, skipping", chunk.ID, chunkIndex, chunk.Status)
		return nil
	}

	if err := s.store.UpdateChunkStatus(ctx, chunk.ID, "running"); err != nil {
		return fmt.Errorf("update chunk %s status to running: %w", chunk.ID, err)
	}
	now := time.Now().UTC()
	chunks[activeIdx].Status = "running"
	if chunks[activeIdx].StartedAt.IsZero() {
		chunks[activeIdx].StartedAt = now
	}
	chunks[activeIdx].UpdatedAt = now
	s.syncBuildWidget(ctx, *build, chunks, "")

	prompt, err := FormatChunkPrompt(*build, chunks, activeIdx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}

	reqPayload := BrainPromptRequest{
		ChannelID:      build.ThreadID,
		Prompt:         prompt,
		Effort:         firstNonEmpty(strings.TrimSpace(chunk.Effort), "low"),
		Silent:         true,
		VanadisChunkID: strings.TrimSpace(chunk.ID),
	}

	if err := s.dispatchBrainPrompt(ctx, reqPayload); err != nil {
		if isFinalAttempt {
			if cErr := s.store.UpdateChunkStatus(ctx, chunk.ID, "failed"); cErr != nil {
				log.Printf("[vanadis] failed marking chunk %s failed on final attempt: %v", chunk.ID, cErr)
			}
			if bErr := s.store.UpdateBuildStatus(ctx, build.ID, "failed"); bErr != nil {
				log.Printf("[vanadis] failed marking build %s failed on final attempt: %v", build.ID, bErr)
			}
			build.Status = "failed"
			chunks[activeIdx].Status = "failed"
			chunks[activeIdx].UpdatedAt = time.Now().UTC()
			failBanner := fmt.Sprintf("⚠️ Chunk %d execution failed: %v", chunkIndex+1, err)
			s.syncBuildWidget(ctx, *build, chunks, failBanner)
		}
		return err
	}

	return nil
}

func (s *VanadisServer) dispatchBrainPrompt(ctx context.Context, payload BrainPromptRequest) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal brain prompt request: %w", err)
	}
	brainURL := strings.TrimRight(firstNonEmpty(s.cfg.BrainURL, "http://127.0.0.1:8088"), "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+"/prompt", bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create brain prompt request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post brain prompt failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return fmt.Errorf("brain prompt returned status %d (read body error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("brain prompt returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		log.Printf("[vanadis] warning draining response body: %v", err)
	}
	return nil
}

func isNumericSnowflake(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// CreateChunkInput defines a single chunk in a CreateBuildRequest.
type CreateChunkInput struct {
	ID                 string   `json:"id"`
	Repo               string   `json:"repo,omitempty"`
	Title              string   `json:"title"`
	Instructions       string   `json:"instructions"`
	TargetFiles        []string `json:"target_files"`
	TargetSymbols      []string `json:"target_symbols"`
	Effort             string   `json:"effort,omitempty"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
}

// CreateBuildRequest defines the JSON request body for POST /builds.
type CreateBuildRequest struct {
	ID            string             `json:"id"`
	Repo          string             `json:"repo"`
	ThreadID      string             `json:"thread_id"`
	Tier          int                `json:"tier"`
	Title         string             `json:"title"`
	Objective     string             `json:"objective"`
	SharedContext string             `json:"shared_context,omitempty"`
	Chunks        []CreateChunkInput `json:"chunks"`
}

func generateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[vanadis] JSON encode error: %v", err)
	}
}

// SetupMux configures and returns the HTTP ServeMux for a VanadisServer.
func SetupMux(server *VanadisServer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.handleHealth)
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /builds", server.handleCreateBuild)
	mux.HandleFunc("POST /runs", server.handleCreateBuild)
	mux.HandleFunc("/builds/", server.handleBuildByID)
	mux.HandleFunc("/runs/", server.handleBuildByID)
	mux.HandleFunc("POST /debug/coverage/flush", server.handleCoverageFlush)
	mux.HandleFunc("GET /debug/coverage/flush", server.handleCoverageFlush)
	return mux
}

// Routes returns the HTTP handler for the server.
func (s *VanadisServer) Routes() http.Handler {
	return SetupMux(s)
}

func (s *VanadisServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

var (
	writeCountersDirFn = coverage.WriteCountersDir
	writeMetaDirFn     = coverage.WriteMetaDir
)

func (s *VanadisServer) handleCoverageFlush(w http.ResponseWriter, _ *http.Request) {
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		if err := writeMetaDirFn(dir); err != nil {
			log.Printf("[vanadis] coverage meta flush error: %v", err)
		}
		if err := writeCountersDirFn(dir); err != nil {
			log.Printf("[vanadis] coverage flush error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "flushed"})
}

func (s *VanadisServer) handleCreateBuild(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "vanadis store not configured"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
		return
	}

	var req CreateBuildRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return
	}

	repo := normalizeRepo(req.Repo)
	if repo == "" && len(req.Chunks) > 0 {
		repo = normalizeRepo(req.Chunks[0].Repo)
	}
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "repo is required"})
		return
	}
	threadID := strings.TrimSpace(req.ThreadID)
	if !isNumericSnowflake(threadID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "thread_id is required and must be a numeric Discord snowflake"})
		return
	}
	objective := strings.TrimSpace(req.Objective)
	if objective == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "objective is required"})
		return
	}
	if len(req.Chunks) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "at least one chunk is required"})
		return
	}

	buildID := strings.TrimSpace(req.ID)
	if buildID == "" {
		buildID = generateBuildID()
	}
	tier := req.Tier
	if tier <= 0 {
		tier = 2
	}

	now := time.Now().UTC()
	build := VanadisBuild{
		ID:            buildID,
		Repo:          repo,
		ThreadID:      threadID,
		Tier:          tier,
		Title:         firstNonEmpty(strings.TrimSpace(req.Title), objective),
		Objective:     objective,
		SharedContext: strings.TrimSpace(req.SharedContext),
		Status:        "running",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	chunks := make([]VanadisChunk, len(req.Chunks))
	for i, c := range req.Chunks {
		title := strings.TrimSpace(c.Title)
		if title == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("chunk[%d] title is required", i)})
			return
		}
		instructions := strings.TrimSpace(c.Instructions)
		if instructions == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("chunk[%d] instructions are required", i)})
			return
		}
		normalizedEffort, err := normalizeChunkEffort(c.Effort)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("chunk[%d] invalid effort: %v", i, err)})
			return
		}
		chunkID := strings.TrimSpace(c.ID)
		if chunkID == "" {
			chunkID = fmt.Sprintf("%s-chunk-%d", buildID, i+1)
		}
		chunkRepo := normalizeRepo(c.Repo)
		if chunkRepo == "" {
			chunkRepo = repo
		}
		files := c.TargetFiles
		if files == nil {
			files = []string{}
		}
		symbols := c.TargetSymbols
		if symbols == nil {
			symbols = []string{}
		}
		criteria := c.AcceptanceCriteria
		if criteria == nil {
			criteria = []string{}
		}
		chunks[i] = VanadisChunk{
			ID:                 chunkID,
			BuildID:            buildID,
			ChunkIndex:         i,
			Repo:               chunkRepo,
			Title:              title,
			Instructions:       instructions,
			TargetFiles:        files,
			TargetSymbols:      symbols,
			Effort:             normalizedEffort,
			AcceptanceCriteria: criteria,
			Status:             "pending",
			CreatedAt:          now,
			UpdatedAt:          now,
		}
	}

	if err := s.store.CreateBuild(r.Context(), build, chunks); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to create build: %v", err)})
		return
	}

	s.syncBuildWidget(r.Context(), build, chunks, "")
	if updatedBuild, _, gErr := s.store.GetBuild(r.Context(), buildID); gErr == nil && updatedBuild != nil {
		build.StatusMessageID = updatedBuild.StatusMessageID
	}

	if err := s.insertRiverJob(r.Context(), ExecuteChunkArgs{BuildID: buildID, ChunkIndex: 0}); err != nil {
		if statusErr := s.store.UpdateBuildStatus(r.Context(), buildID, "failed"); statusErr != nil {
			log.Printf("[vanadis] warning marking build %s failed after enqueue error: %v", buildID, statusErr)
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to enqueue chunk 0: %v", err)})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":            "accepted",
		"build_id":          build.ID,
		"repo":              build.Repo,
		"thread_id":         build.ThreadID,
		"tier":              build.Tier,
		"status_message_id": build.StatusMessageID,
		"chunk_count":       len(chunks),
		"first_chunk_id":    chunks[0].ID,
		"chunks":            chunks,
	})
}

func (s *VanadisServer) handleBuildByID(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "vanadis store not configured"})
		return
	}

	trimmedPath := strings.TrimPrefix(r.URL.Path, "/builds/")
	trimmedPath = strings.TrimPrefix(trimmedPath, "/runs/")
	trimmedPath = strings.Trim(trimmedPath, "/")
	if trimmedPath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "build id is required"})
		return
	}

	if strings.HasSuffix(trimmedPath, "/cancel") {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		buildID := strings.TrimSpace(strings.TrimSuffix(trimmedPath, "/cancel"))
		if buildID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "build id is required"})
			return
		}
		existing, existingChunks, gErr := s.store.GetBuild(r.Context(), buildID)
		if gErr == nil && existing != nil && (existing.Status == "completed" || existing.Status == "failed") {
			writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("cannot cancel build in terminal status %q", existing.Status)})
			return
		}
		if err := s.store.UpdateBuildStatus(r.Context(), buildID, "cancelled"); err != nil {
			if errors.Is(err, ErrBuildNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "build not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if existing != nil {
			existing.Status = "cancelled"
			s.syncBuildWidget(r.Context(), *existing, existingChunks, "")
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":   "cancelled",
			"build_id": buildID,
		})
		return
	}

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	build, chunks, err := s.store.GetBuild(r.Context(), trimmedPath)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "build not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"build":  build,
		"chunks": chunks,
	})
}

type riverLifecycleClient interface {
	RiverInserter
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

var (
	newPgxPoolFn     = pgxpool.NewWithConfig
	newRiverClientFn = func(dbPool *pgxpool.Pool, workers *river.Workers) (riverLifecycleClient, error) {
		return river.NewClient(riverpgxv5.New(dbPool), &river.Config{
			Queues: map[string]river.QueueConfig{
				VanadisQueueName: {MaxWorkers: 5},
			},
			Workers: workers,
		})
	}
)

func initPostgresAndRiver(ctx context.Context, server *VanadisServer, pgURL string) (func(context.Context), error) {
	poolConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2

	pool, err := newPgxPoolFn(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	server.SetStore(NewPostgresVanadisStore(pool))

	workers := river.NewWorkers()
	river.AddWorker(workers, &ExecuteChunkWorker{server: server})

	rc, err := newRiverClientFn(pool, workers)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create river client: %w", err)
	}
	server.SetRiverClient(rc)

	if err := rc.Start(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("start river client: %w", err)
	}

	cleanup := func(stopCtx context.Context) {
		if stopErr := rc.Stop(stopCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			log.Printf("[vanadis] River client stop error: %v", stopErr)
		}
		pool.Close()
	}
	return cleanup, nil
}

func runServer(ctx context.Context, cfg Config, onReady func(addr string)) error {
	server := NewVanadisServer(cfg, nil, nil, nil)

	var cleanupDB func(context.Context)
	if cfg.PostgresURL != "" {
		cleanup, err := initPostgresAndRiver(ctx, server, cfg.PostgresURL)
		if err != nil {
			return err
		}
		cleanupDB = cleanup
		log.Println("[vanadis] River worker pool started successfully")
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.Port)
	if err != nil {
		if cleanupDB != nil {
			cleanupDB(context.Background())
		}
		return fmt.Errorf("listening on port %s: %w", cfg.Port, err)
	}

	httpServer := &http.Server{
		Handler:      server.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if onReady != nil {
		onReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[vanadis] Server listening on %s (BrainURL: %s)",
			ln.Addr().String(), cfg.BrainURL)
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errChan:
		runErr = err
	}

	log.Println("[vanadis] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
func RunApp(ctx context.Context, cfg Config) error {
	return runServer(ctx, cfg, nil)
}

func main() {
	cfg := LoadConfigFromEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[vanadis] Error: %v", err)
	}
}
