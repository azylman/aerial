package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis/internal/config"
	vanadismcp "github.com/azylman/aerial/services/vanadis/internal/mcp"
	"github.com/azylman/aerial/services/vanadis/internal/model"
	"github.com/azylman/aerial/services/vanadis/internal/store"
)

type (
	// Config holds runtime configuration for the Vanadis orchestrator service.
	Config = config.Config
	// VanadisYAMLConfig defines the declarative file schema for /config/vanadis.yaml.
	VanadisYAMLConfig = config.VanadisYAMLConfig
)

var (
	// ErrBuildNotFound indicates a requested VanadisBuild does not exist in PostgreSQL.
	ErrBuildNotFound = model.ErrBuildNotFound
	// ErrChunkNotFound indicates a requested VanadisChunk does not exist in PostgreSQL.
	ErrChunkNotFound = model.ErrChunkNotFound
	// ErrBuildConflict indicates a VanadisBuild or chunk state conflict (e.g. not running or not pending).
	ErrBuildConflict = model.ErrBuildConflict
	// ErrInvalidBuildInput indicates invalid inputs for a VanadisBuild or chunk mutation.
	ErrInvalidBuildInput = model.ErrInvalidBuildInput

	firstNonEmpty = model.FirstNonEmpty

	// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
	LoadConfig = config.LoadConfig
	// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
	LoadConfigFromEnv = config.LoadConfigFromEnv
)

// ExecuteChunkArgs defines the job payload for executing a single Vanadis build chunk.
type ExecuteChunkArgs struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkID    string `json:"chunk_id,omitempty"`
}

type (
	// VanadisBuild represents a multi-chunk autonomous engineering build run.
	VanadisBuild = model.VanadisBuild
	// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
	VanadisChunk = model.VanadisChunk
	// VanadisBuildEntry represents a summarized VanadisBuild including overall queue state and chunk aggregates.
	VanadisBuildEntry = model.VanadisBuildEntry
)

var (
	capChunkEffort = model.CapChunkEffort
	// NormalizeChunkEffort validates and normalizes a chunk effort value.
	NormalizeChunkEffort = model.NormalizeChunkEffort
	normalizeChunkEffort = model.NormalizeChunkEffort
	// ClassifyBuildQueueState computes the overall queue state ("running", "queued", or "completed") based on buildStatus and child chunks.
	ClassifyBuildQueueState = model.ClassifyBuildQueueState
	// SummarizeBuildEntry constructs a VanadisBuildEntry with non-nil chunks, chunk status counts, and computed queue state.
	SummarizeBuildEntry = model.SummarizeBuildEntry
)

type (
	// VanadisStore defines the persistence contract for Vanadis builds and chunks.
	VanadisStore = store.VanadisStore
	// PostgresVanadisStore implements VanadisStore backed by PostgreSQL (pgxpool.Pool).
	PostgresVanadisStore = store.PostgresVanadisStore
)

var (
	// NewPostgresVanadisStore constructs a new PostgresVanadisStore.
	NewPostgresVanadisStore = store.NewPostgresVanadisStore
	// ScanBuild scans one row selected with BuildColumns.
	ScanBuild = store.ScanBuild
	// ScanChunks scans ordered vanadis_chunks rows into VanadisChunk values.
	ScanChunks = store.ScanChunks
)

var (
	// NormalizeRepo trims whitespace from a repository reference.
	NormalizeRepo = model.NormalizeRepo
	normalizeRepo = model.NormalizeRepo

	normalizeChunkMode = model.NormalizeChunkMode

	// ValidateChunkModeAndRepo validates the mode/repo/target combination for chunk idx.
	ValidateChunkModeAndRepo = model.ValidateChunkModeAndRepo

	isValidVanadisChunkID = model.IsValidVanadisChunkID
)

// VanadisServer manages HTTP endpoints and JetStream worker coordination for Vanadis builds.
type VanadisServer struct {
	cfg        Config
	restateCfg RestateConfig
	httpClient *http.Client
	store      VanadisStore
	queueStats consumerStatsQuerier
}

// NewVanadisServer constructs a new VanadisServer instance.
func NewVanadisServer(cfg Config, st VanadisStore, httpClient *http.Client) *VanadisServer {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	// Traced so eri turns join the chunk dispatch span (and widget/wake posts carry traceparent).
	return &VanadisServer{cfg: cfg, restateCfg: LoadRestateConfig(os.Getenv("CONFIG_PATH")), httpClient: tracing.Client(httpClient), store: st}
}

// SetStore updates the VanadisStore backing the server.
func (s *VanadisServer) SetStore(st VanadisStore) { s.store = st }

// mcpHandler builds the L2 MCP handler from live server dependencies.
func (s *VanadisServer) mcpHandler() *vanadismcp.Handler {
	if s == nil {
		return vanadismcp.NewHandler(nil, NewClient(RestateConfig{}, nil), false)
	}
	return vanadismcp.NewHandler(s.store, s.restateClient(), s.restateCfg.Enabled)
}

// newMCPHandler constructs and returns the Streamable HTTP MCP server handler.
func (s *VanadisServer) newMCPHandler() http.HandlerFunc {
	return s.mcpHandler().NewHTTPHandler()
}

// handleRetryActionChunk serves POST /builds/{id}/retry-action.
func (s *VanadisServer) handleRetryActionChunk(w http.ResponseWriter, r *http.Request) {
	s.mcpHandler().HandleRetryActionChunk(w, r)
}

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

type (
	// CreateChunkInput defines a single chunk in a CreateBuildRequest.
	CreateChunkInput = model.CreateChunkInput
	// CreateBuildRequest defines the JSON request body for POST /builds.
	CreateBuildRequest = model.CreateBuildRequest
)

var (
	// GenerateBuildID generates a unique build identifier.
	GenerateBuildID = model.GenerateBuildID
	generateBuildID = model.GenerateBuildID
)

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

// writeStoreErr maps a store or Restate error onto its HTTP status and writes it. Server-side
// failures (5xx) are logged with ctx so the line carries the request's trace_id and span_id.
func writeStoreErr(ctx context.Context, w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrRestateTimeout):
		status = http.StatusGatewayTimeout
	case errors.Is(err, ErrInvalidBuildInput):
		status = http.StatusBadRequest
	case errors.Is(err, ErrBuildNotFound) || errors.Is(err, ErrChunkNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrBuildConflict):
		status = http.StatusConflict
	}
	if status >= http.StatusInternalServerError {
		slog.ErrorContext(ctx, "[vanadis] store request failed", "status", status, "error", err)
	}
	writeErr(w, status, err.Error())
}

// SetupMux configures the HTTP routes for a VanadisServer, wrapped once in a server span.
func SetupMux(server *VanadisServer) http.Handler {
	mux := http.NewServeMux()
	if server != nil {
		mux.HandleFunc("GET /metrics", server.handleMetrics)
	} else {
		mux.Handle("GET /metrics", promhttp.Handler())
	}
	coverageFlush := httpkit.HandleCoverageFlush("vanadis")
	mcpHandler := server.newMCPHandler().ServeHTTP
	for p, h := range map[string]http.HandlerFunc{
		"GET /health": server.handleHealth, "GET /healthz": server.handleHealth,
		"GET /builds": server.handleListBuilds, "GET /runs": server.handleListBuilds,
		"POST /builds": server.handleCreateBuild, "POST /runs": server.handleCreateBuild,
		"/builds/": server.handleBuildByID, "/runs/": server.handleBuildByID,
		"/chunks/attach-pr":              server.handleAttachChunkPR,
		"/chunks/":                       server.handleChunkByID,
		"POST /builds/{id}/retry-action": server.handleRetryActionChunk,
		"/mcp":                           mcpHandler, "/sse": mcpHandler,
		"POST /debug/coverage/flush": coverageFlush, "GET /debug/coverage/flush": coverageFlush,
	} {
		mux.HandleFunc(p, h)
	}
	return tracing.Middleware("vanadis", mux)
}

// Routes returns the HTTP handler for the server.
func (s *VanadisServer) Routes() http.Handler { return SetupMux(s) }

func (*VanadisServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
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
	if err := s.callRestateIngress(r.Context(), s.restateObjectPath(buildID, "Create"), req, &created); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	slog.InfoContext(r.Context(), "[vanadis] build created", "build_id", created.ID, "thread_id", created.ThreadID, "tier", created.Tier, "chunk_count", len(chunks))

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
	if err := s.callRestateIngress(r.Context(), s.restateObjectPath(id, "Cancel"), CancelBuildRequest{}, &cancelled); err != nil {
		writeStoreErr(r.Context(), w, err)
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
			writeStoreErr(r.Context(), w, err)
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
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	err := RunApp(ctx, cfg)
	cancel()
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[vanadis] Error: %v", err)
	}
}
