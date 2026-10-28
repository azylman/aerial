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
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"

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
	pgURL := firstNonEmpty(os.Getenv("POSTGRES_URL"), os.Getenv("DATABASE_URL"), fileCfg.PostgresURL)
	if pgURL == "" {
		host := "127.0.0.1"
		for _, h := range []string{"aerial-postgres", "postgres"} {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := lookupHostFn(ctx, h)
			cancel()
			if err == nil {
				host = h
				break
			}
		}
		pgURL = fmt.Sprintf("postgres://aerial:aerial_secure_pass@%s:5432/aerial?sslmode=disable", host)
	}
	return Config{
		Port:        firstNonEmpty(os.Getenv("PORT"), fileCfg.Port, "4025"),
		PostgresURL: pgURL,
		BrainURL:    strings.TrimRight(firstNonEmpty(os.Getenv("BRAIN_INTERNAL_URL"), os.Getenv("BRAIN_URL"), fileCfg.BrainURL, "http://127.0.0.1:8088"), "/"),
	}
}

// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
func LoadConfigFromEnv() Config { return LoadConfig(os.Getenv("CONFIG_PATH")) }

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

func capChunkEffort(effort string) string {
	if strings.EqualFold(strings.TrimSpace(effort), "low") {
		return "low"
	}
	return "medium"
}

func normalizeChunkEffort(effort string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "", "medium", "high":
		return "medium", nil
	case "low":
		return "low", nil
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
	StartedAt          time.Time `json:"started_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
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
	case "completed", "merged", "failed", "cancelled", "canceled":
		return "completed"
	case "running":
		for _, c := range chunks {
			if strings.ToLower(strings.TrimSpace(c.Status)) != "pending" {
				return "running"
			}
		}
		if len(chunks) > 0 {
			return "queued"
		}
		return "running"
	default:
		return "queued"
	}
}

// SummarizeBuildEntry constructs a VanadisBuildEntry with non-nil chunks, chunk status counts, and computed queue state.
func SummarizeBuildEntry(build VanadisBuild, chunks []VanadisChunk) VanadisBuildEntry {
	chunks = nonNilSlice(chunks)
	var completed, running, pending, failed int
	for _, c := range chunks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case "completed", "merged":
			completed++
		case "running", "coding", "pr_opened", "deploying":
			running++
		case "failed":
			failed++
		case "pending":
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
	UpdateBuildStatus(ctx context.Context, buildID, status string) error
	UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error
	GetChunk(ctx context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error)
	GetChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisChunkPRLookup, error)
	AttachChunkPR(ctx context.Context, chunkID string, prNumber int, repo string) (string, error)
	AdvanceChunkByPR(ctx context.Context, repo string, prNumber int) (*VanadisAdvancementResult, error)
	FailChunkByPR(ctx context.Context, repo string, prNumber int) (buildID, chunkID string, failed bool, err error)
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

func normalizeRepo(repo string) string {
	if repo = strings.TrimSpace(repo); repo == "" || strings.Contains(repo, "/") {
		return repo
	}
	return "azylman/" + repo
}

func isValidVanadisChunkID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 1 || len(id) > 120 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func validateCreateBuildInputs(buildID, threadID, objective string, chunks []VanadisChunk) error {
	if buildID == "" || threadID == "" || objective == "" {
		return errors.New("build id, thread_id, and objective cannot be empty")
	}
	for i, c := range chunks {
		if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Instructions) == "" {
			return fmt.Errorf("chunk[%d] id, title, and instructions cannot be empty", i)
		}
		if !isValidVanadisChunkID(strings.TrimSpace(c.ID)) {
			return fmt.Errorf("chunks[%d].id must be 1..120 characters of [a-zA-Z0-9_-]", i)
		}
		if cb := strings.TrimSpace(c.BuildID); cb != "" && cb != buildID {
			return fmt.Errorf("chunk[%d] build_id %q does not match build id %q", i, cb, buildID)
		}
		if _, err := normalizeChunkEffort(c.Effort); err != nil {
			return fmt.Errorf("chunk[%d] %w", i, err)
		}
	}
	return nil
}

func execInsertVanadisChunk(ctx context.Context, tx pgx.Tx, buildID string, c VanadisChunk) error {
	const cQ = `INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, started_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`
	cStatus := firstNonEmpty(strings.TrimSpace(c.Status), "pending")
	cEffort, err := normalizeChunkEffort(c.Effort)
	if err != nil {
		return fmt.Errorf("chunk %s: %w", c.ID, err)
	}
	var prNum, startedAt any
	if c.PRNumber > 0 {
		prNum = c.PRNumber
	}
	if !c.StartedAt.IsZero() {
		startedAt = c.StartedAt
	} else if cStatus == "running" || cStatus == "coding" {
		startedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, cQ, strings.TrimSpace(c.ID), buildID, c.ChunkIndex, firstNonEmpty(normalizeRepo(c.Repo), "azylman/aerial"), strings.TrimSpace(c.Title), strings.TrimSpace(c.Instructions), nonNilSlice(c.TargetFiles), nonNilSlice(c.TargetSymbols), cEffort, nonNilSlice(c.AcceptanceCriteria), cStatus, prNum, startedAt); err != nil {
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
	const bQ = `INSERT INTO vanadis_builds (id, thread_id, tier, title, objective, shared_context, status, status_message_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);`
	if _, err := tx.Exec(ctx, bQ, buildID, threadID, tier, firstNonEmpty(strings.TrimSpace(build.Title), objective), objective, strings.TrimSpace(build.SharedContext), firstNonEmpty(strings.TrimSpace(build.Status), "running"), strings.TrimSpace(build.StatusMessageID)); err != nil {
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
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at FROM vanadis_builds WHERE id = $1;`
	b, err := scanBuild(s.db.QueryRow(ctx, buildQuery, buildID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBuildNotFound
		}
		return nil, nil, fmt.Errorf("query build %s: %w", buildID, err)
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index ASC;`
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
		var startedAt sql.NullTime
		if err := rows.Scan(&c.ID, &c.BuildID, &c.ChunkIndex, &c.Repo, &c.Title, &c.Instructions, &c.TargetFiles, &c.TargetSymbols, &c.Effort, &c.AcceptanceCriteria, &c.Status, &c.PRNumber, &startedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan vanadis chunk: %w", err)
		}
		if startedAt.Valid {
			c.StartedAt = startedAt.Time
		}
		c.TargetFiles, c.TargetSymbols, c.AcceptanceCriteria = nonNilSlice(c.TargetFiles), nonNilSlice(c.TargetSymbols), nonNilSlice(c.AcceptanceCriteria)
		c.Effort = capChunkEffort(c.Effort)
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
	const buildQuery = `SELECT id, thread_id, tier, COALESCE(NULLIF(title, ''), objective), objective, COALESCE(shared_context, ''), status, status_message_id, created_at, updated_at FROM vanadis_builds ORDER BY created_at DESC, id DESC LIMIT $1;`
	rows, err := s.db.Query(ctx, buildQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query vanadis builds: %w", err)
	}
	defer rows.Close()
	var builds []VanadisBuild
	var buildIDs []string
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan vanadis build: %w", err)
		}
		builds = append(builds, b)
		buildIDs = append(buildIDs, b.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating vanadis builds: %w", err)
	}
	if len(builds) == 0 {
		return []VanadisBuildEntry{}, nil
	}
	const chunksQuery = `SELECT id, build_id, chunk_index, repo, title, instructions, target_files, target_symbols, COALESCE(effort, 'medium'), acceptance_criteria, status, COALESCE(pr_number, 0), started_at, created_at, updated_at FROM vanadis_chunks WHERE build_id = ANY($1) ORDER BY build_id, chunk_index ASC;`
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
	return s.execUpdateByID(ctx, "chunk id", chunkID, "status", status, false, `UPDATE vanadis_chunks SET status = $1, started_at = CASE WHEN $1 IN ('running', 'coding') THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis chunk status", ErrChunkNotFound)
}

// UpdateBuildStatus updates the lifecycle status of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatus(ctx context.Context, buildID, status string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "status", status, false, `UPDATE vanadis_builds SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status", ErrBuildNotFound)
}

// UpdateBuildStatusMessageID updates the status_message_id of a VanadisBuild.
func (s *PostgresVanadisStore) UpdateBuildStatusMessageID(ctx context.Context, buildID, messageID string) error {
	return s.execUpdateByID(ctx, "build id", buildID, "message_id", messageID, true, `UPDATE vanadis_builds SET status_message_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;`, "failed to update vanadis build status_message_id", ErrBuildNotFound)
}

// FormatChunkPrompt renders a structured, context-rich prompt for executing a specific Vanadis chunk.
func FormatChunkPrompt(build VanadisBuild, chunks []VanadisChunk, activeIdx int) (string, error) {
	return formatChunkPromptWithShareRoot(build, chunks, activeIdx, defaultShareRoot)
}

func formatChunkPromptWithShareRoot(build VanadisBuild, chunks []VanadisChunk, activeIdx int, shareRoot string) (string, error) {
	if len(chunks) == 0 {
		return "", errors.New("chunks slice cannot be empty")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return "", fmt.Errorf("active chunk index %d out of range [0, %d)", activeIdx, len(chunks))
	}
	total := len(chunks)
	active := chunks[activeIdx]
	activeRepo := firstNonEmpty(normalizeRepo(active.Repo), "azylman/aerial")
	var sb strings.Builder
	fmt.Fprintf(&sb, "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: %s\n", build.ID)
	if bTitle := strings.TrimSpace(build.Title); bTitle != "" {
		fmt.Fprintf(&sb, "Build Title: %s\n", bTitle)
	}
	fmt.Fprintf(&sb, "Repository: %s\nTier: %d\n\n=== Build Objective ===\n%s\n\n", activeRepo, build.Tier, strings.TrimSpace(build.Objective))
	if sharedCtx := strings.TrimSpace(build.SharedContext); sharedCtx != "" {
		fmt.Fprintf(&sb, "=== Shared Build Context ===\n%s\n\n", sharedCtx)
	}
	sb.WriteString("=== Completed Chunks Ledger ===\n")
	if activeIdx == 0 {
		sb.WriteString("(none — this is the first chunk)\n\n")
	} else {
		for k, c := range chunks[:activeIdx] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s (%s, PR #%d)\n", k+1, total, c.Title, firstNonEmpty(normalizeRepo(c.Repo), "azylman/aerial"), c.PRNumber)
		}
		sb.WriteString("\n")
	}
	targetFilesStr := firstNonEmpty(strings.Join(active.TargetFiles, ", "), "(none specified)")
	fmt.Fprintf(&sb, "=== Active Chunk (Chunk %d of %d) ===\nChunk ID: %s\nTitle: %s\nRepository: %s\nTarget Files: %s\n", activeIdx+1, total, active.ID, active.Title, activeRepo, targetFilesStr)
	if len(active.TargetSymbols) > 0 {
		fmt.Fprintf(&sb, "Target Symbols: %s\n", strings.Join(active.TargetSymbols, ", "))
	}
	spanLines, allResolved := ResolveSymbolSpans(shareRoot, activeRepo, active.TargetFiles, active.TargetSymbols)
	if len(spanLines) > 0 {
		fmt.Fprintf(&sb, "Target Symbol Spans:\n%s\n", strings.Join(spanLines, "\n"))
	}
	fmt.Fprintf(&sb, "Instructions:\n%s\n\n=== Remaining Chunks ===\n", active.Instructions)
	if activeIdx+1 >= total {
		sb.WriteString("(none — this is the final chunk)\n\n")
	} else {
		sb.WriteString("DO NOT implement these remaining chunks in this turn—they are listed strictly for architectural boundary awareness.\n")
		for k, c := range chunks[activeIdx+1:] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s (%s)\n", activeIdx+2+k, total, c.Title, firstNonEmpty(normalizeRepo(c.Repo), "azylman/aerial"))
		}
		sb.WriteString("\n")
	}
	fmt.Fprintf(&sb, "=== Mandatory Execution Contract ===\n1. Initialize a fresh scratch workspace off `origin/main`: `/share/aerial/scripts/aerial-pr.sh init %s`\n2. Implement ONLY this chunk following TDD, optimizing all design and implementation choices for the `Build Objective` and `Shared Build Context` so the change achieves the objective in substance and integrates cleanly and idiomatically into the existing architecture without superficial workarounds, dead scaffolding, or bolted-on catch-all blocks, run package tests and `./scripts/verify.sh --staged`, and write `PR_DESCRIPTION.md`.\n   If any detail in chunk `Instructions` is ambiguous, underspecified, or conflicts with the `Build Objective`, the `Build Objective` takes precedence—adapt the implementation to satisfy the `Build Objective`.\n3. Inspect `Target Files` and `Target Symbols` directly rather than running broad repo-wide searches: resolve live symbol line offsets first and `view_file` ONLY those targeted `StartLine`–`EndLine` slices instead of reading entire files from line 1.\n", activeRepo)
	if !allResolved {
		if grepCmd := formatSymbolGrepCommand(active.TargetFiles, active.TargetSymbols); grepCmd != "" {
			fmt.Fprintf(&sb, "   Run after init: `%s`\n", grepCmd)
		}
	}
	fmt.Fprintf(&sb, "4. NEVER spawn or invoke subagents (`invoke_subagent`, `define_subagent`): Vanadis build chunks are already isolated leaf tasks—execute all file edits, tests, and verification directly in this turn.\n5. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`\n6. Terminate turn immediately after submission.\n", active.ID)
	return sb.String(), nil
}

func formatSymbolGrepCommand(targetFiles, targetSymbols []string) string {
	var syms, files []string
	for _, s := range targetSymbols {
		if s = strings.TrimSpace(s); s != "" {
			syms = append(syms, s)
		}
	}
	for _, f := range targetFiles {
		if f = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(f), "./"), "/"); f != "" {
			files = append(files, fmt.Sprintf(`"$SCRATCH_DIR/%s"`, f))
		}
	}
	if len(syms) == 0 || len(files) == 0 {
		return ""
	}
	return fmt.Sprintf("grep -n -E %q %s", strings.Join(syms, "|"), strings.Join(files, " "))
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

// BrainDispatchRequest represents the JSON payload dispatched to Brain's POST /dispatch endpoint.
type BrainDispatchRequest struct {
	ID        string `json:"id,omitempty"`
	Source    string `json:"source"`
	SessionID string `json:"session_id,omitempty"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort,omitempty"`
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	PRNumber      int       `json:"pr_number,omitempty"`
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
	return &VanadisServer{cfg: cfg, httpClient: httpClient, store: store, riverClient: riverClient}
}

// SetStore updates the VanadisStore backing the server.
func (s *VanadisServer) SetStore(store VanadisStore) { s.store = store }

// SetRiverClient updates the RiverInserter backing the server.
func (s *VanadisServer) SetRiverClient(rc RiverInserter) { s.riverClient = rc }

func (s *VanadisServer) postBrainJSON(ctx context.Context, path string, payload any, errReadLimit int64) ([]byte, error) {
	if s == nil || s.httpClient == nil {
		return nil, errors.New("http client is nil")
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	brainURL := strings.TrimRight(firstNonEmpty(s.cfg.BrainURL, "http://127.0.0.1:8088"), "/")
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

func (s *VanadisServer) syncBuildWidget(ctx context.Context, build VanadisBuild, chunks []VanadisChunk, banner string) {
	if s == nil || s.httpClient == nil {
		return
	}
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		chunkStatus := normalizeWidgetStatus(c.Status)
		if chunkStatus == "merged" {
			chunkStatus = "deployed"
		}
		wChunks[i] = StatusWidgetChunk{
			Index:     c.ChunkIndex,
			Repo:      firstNonEmpty(c.Repo, "azylman/aerial"),
			Title:     c.Title,
			Status:    chunkStatus,
			PRNumber:  c.PRNumber,
			StartedAt: c.StartedAt,
			UpdatedAt: c.UpdatedAt,
		}
	}
	payload := StatusWidgetRequest{
		ChannelID: build.ThreadID, MessageID: build.StatusMessageID, ID: build.ID, Tier: build.Tier,
		Title: firstNonEmpty(strings.TrimSpace(build.Title), build.Objective), Status: build.Status, Banner: banner, Chunks: wChunks,
	}
	respBody, err := s.postBrainJSON(ctx, "/discord/widget", payload, 4<<10)
	if err != nil {
		log.Printf("[vanadis] warning: post discord widget failed for build %s: %v", build.ID, err)
		return
	}
	var res struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(respBody, &res); err == nil && res.MessageID != "" && res.MessageID != build.StatusMessageID && s.store != nil {
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
	reqPayload := BrainDispatchRequest{
		Source:    "vanadis",
		SessionID: "vanadis-" + strings.TrimSpace(chunk.ID),
		Prompt:    prompt,
		Effort:    capChunkEffort(chunk.Effort),
	}
	if err := s.dispatchBrainDispatch(ctx, reqPayload); err != nil {
		if isFinalAttempt {
			if cErr := s.store.UpdateChunkStatus(ctx, chunk.ID, "failed"); cErr != nil {
				log.Printf("[vanadis] failed marking chunk %s failed on final attempt: %v", chunk.ID, cErr)
			}
			if bErr := s.store.UpdateBuildStatus(ctx, build.ID, "failed"); bErr != nil {
				log.Printf("[vanadis] failed marking build %s failed on final attempt: %v", build.ID, bErr)
			}
			build.Status, chunks[activeIdx].Status, chunks[activeIdx].UpdatedAt = "failed", "failed", time.Now().UTC()
			s.syncBuildWidget(ctx, *build, chunks, fmt.Sprintf("⚠️ Chunk %d execution failed: %v", chunkIndex+1, err))
		}
		return err
	}
	return nil
}

func (s *VanadisServer) dispatchBrainDispatch(ctx context.Context, payload BrainDispatchRequest) error {
	_, err := s.postBrainJSON(ctx, "/dispatch", payload, 64<<10)
	return err
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanBuild(s rowScanner) (VanadisBuild, error) {
	var b VanadisBuild
	err := s.Scan(&b.ID, &b.ThreadID, &b.Tier, &b.Title, &b.Objective, &b.SharedContext, &b.Status, &b.StatusMessageID, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

func writeStoreErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, ErrBuildNotFound) || errors.Is(err, ErrChunkNotFound) {
		status = http.StatusNotFound
	}
	writeErr(w, status, err.Error())
}

// SetupMux configures and returns the HTTP ServeMux for a VanadisServer.
func SetupMux(server *VanadisServer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	coverageFlush := httpkit.HandleCoverageFlush("vanadis")
	for p, h := range map[string]http.HandlerFunc{
		"GET /health": server.handleHealth, "GET /healthz": server.handleHealth,
		"GET /builds": server.handleListBuilds, "GET /runs": server.handleListBuilds,
		"POST /builds": server.handleCreateBuild, "POST /runs": server.handleCreateBuild,
		"/builds/": server.handleBuildByID, "/runs/": server.handleBuildByID,
		"/chunks/by-pr":              server.handleChunkByPR,
		"/chunks/attach-pr":          server.handleAttachChunkPR,
		"/chunks/advance-pr":         server.handleAdvanceChunkPR,
		"/chunks/fail-pr":            server.handleFailChunkPR,
		"/chunks/sync-pr-widget":     server.handleSyncPRWidget,
		"/chunks/":                   server.handleChunkByID,
		"POST /debug/coverage/flush": coverageFlush, "GET /debug/coverage/flush": coverageFlush,
	} {
		mux.HandleFunc(p, h)
	}
	return mux
}

// Routes returns the HTTP handler for the server.
func (s *VanadisServer) Routes() http.Handler { return SetupMux(s) }

func (s *VanadisServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func parseCreateBuildChunks(buildID, repo string, now time.Time, rawChunks []CreateChunkInput) ([]VanadisChunk, error) {
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
		effort, err := normalizeChunkEffort(c.Effort)
		if err != nil {
			return nil, fmt.Errorf("chunk[%d] invalid effort: %w", i, err)
		}
		chunks[i] = VanadisChunk{
			ID: chunkID, BuildID: buildID, ChunkIndex: i,
			Repo: firstNonEmpty(normalizeRepo(c.Repo), repo), Title: title, Instructions: instructions,
			TargetFiles: nonNilSlice(c.TargetFiles), TargetSymbols: nonNilSlice(c.TargetSymbols), Effort: effort,
			AcceptanceCriteria: nonNilSlice(c.AcceptanceCriteria), Status: "pending", CreatedAt: now, UpdatedAt: now,
		}
	}
	return chunks, nil
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
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	repo := normalizeRepo(req.Repo)
	if repo == "" && len(req.Chunks) > 0 {
		repo = normalizeRepo(req.Chunks[0].Repo)
	}
	threadID, objective := normalizeDiscordThreadID(req.ThreadID), strings.TrimSpace(req.Objective)
	switch {
	case repo == "":
		writeErr(w, http.StatusBadRequest, "repo is required")
		return
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
		Objective: objective, SharedContext: strings.TrimSpace(req.SharedContext), Status: "running", CreatedAt: now, UpdatedAt: now,
	}
	chunks, err := parseCreateBuildChunks(buildID, repo, now, req.Chunks)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.CreateBuild(r.Context(), build, chunks); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("failed to create build: %v", err))
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
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue chunk 0: %v", err))
		return
	}
	httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status": "accepted", "build_id": build.ID, "thread_id": build.ThreadID, "tier": build.Tier,
		"status_message_id": build.StatusMessageID, "chunk_count": len(chunks), "first_chunk_id": chunks[0].ID, "chunks": chunks,
	})
}

func isCancellableChunkStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending", "running", "coding":
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
		existingChunks[i].Status = "cancelled"
		existingChunks[i].UpdatedAt = now
	}
	if _, err := s.postBrainJSON(ctx, "/dispatch/cancel", map[string]string{"build_id": buildID}, 4<<10); err != nil {
		log.Printf("[vanadis] warning: failed cancelling pending brain dispatches for build %s: %v", buildID, err)
	}
	if existing != nil {
		existing.Status = "cancelled"
		s.syncBuildWidget(ctx, *existing, existingChunks, "")
	}
}

func (s *VanadisServer) handleBuildByID(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusInternalServerError, "vanadis store not configured")
		return
	}
	trimmed := strings.Trim(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/builds/"), "/runs/"), "/")
	if trimmed == "" {
		writeErr(w, http.StatusBadRequest, "build id is required")
		return
	}
	if id, isCancel := strings.CutSuffix(trimmed, "/cancel"); isCancel {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if id = strings.TrimSpace(id); id == "" {
			writeErr(w, http.StatusBadRequest, "build id is required")
			return
		}
		existing, existingChunks, gErr := s.store.GetBuild(r.Context(), id)
		if gErr == nil && existing != nil && (existing.Status == "completed" || existing.Status == "failed") {
			writeErr(w, http.StatusConflict, fmt.Sprintf("cannot cancel build in terminal status %q", existing.Status))
			return
		}
		if err := s.store.UpdateBuildStatus(r.Context(), id, "cancelled"); err != nil {
			writeStoreErr(w, err)
			return
		}
		s.cancelBuildChunksAndPrompts(r.Context(), id, existing, existingChunks)
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "cancelled", "build_id": id})
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	build, chunks, err := s.store.GetBuild(r.Context(), trimmed)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{"build": build, "chunks": chunks, "entry": SummarizeBuildEntry(*build, chunks)})
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
		case "running":
			running++
		case "queued":
			queued++
		case "completed":
			completed++
		}
		if (qFilter == "" || strings.EqualFold(b.QueueState, qFilter)) && (sFilter == "" || strings.EqualFold(b.QueueState, sFilter) || strings.EqualFold(b.Status, sFilter)) {
			filtered = append(filtered, b)
		}
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "total": len(filtered), "running_count": running,
		"queued_count": queued, "completed_count": completed, "builds": filtered,
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
	poolConfig.MaxConns, poolConfig.MinConns = 10, 2
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
	return func(stopCtx context.Context) {
		if stopErr := rc.Stop(stopCtx); stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			log.Printf("[vanadis] River client stop error: %v", stopErr)
		}
		pool.Close()
	}, nil
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
func RunApp(ctx context.Context, cfg Config) error { return runServer(ctx, cfg, nil) }

func main() {
	cfg := LoadConfigFromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := RunApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[vanadis] Error: %v", err)
	}
}
