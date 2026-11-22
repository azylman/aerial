// Package server holds the L3 Vanadis HTTP service: VanadisServer, routes and
// handlers, PostgreSQL initialization, and the RunApp lifecycle (over the
// unexported runServer).
// It is the only package that wires L0 config/model, L1 store/restate/
// metrics, and the L2 MCP handler into HTTP endpoints.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
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
	// RestateConfig holds Restate durable workflow runtime configuration for Vanadis.
	RestateConfig = config.RestateConfig
)

var (
	// LoadConfig loads configuration from an optional YAML file (CONFIG_PATH) with environment variable overrides.
	LoadConfig = config.LoadConfig
	// LoadConfigFromEnv initializes configuration from environment variables with optional CONFIG_PATH fallback.
	LoadConfigFromEnv = config.LoadConfigFromEnv
	// LoadRestateConfig loads Restate configuration from an optional YAML file with environment variable overrides.
	LoadRestateConfig = config.LoadRestateConfig
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
)

type (
	// VanadisBuild represents a multi-chunk autonomous engineering build run.
	VanadisBuild = model.VanadisBuild
	// VanadisChunk represents an individual ordered task chunk within a VanadisBuild.
	VanadisChunk = model.VanadisChunk
	// VanadisBuildEntry represents a summarized VanadisBuild including overall queue state and chunk aggregates.
	VanadisBuildEntry = model.VanadisBuildEntry
	// CreateChunkInput defines a single chunk in a CreateBuildRequest.
	CreateChunkInput = model.CreateChunkInput
	// CreateBuildRequest defines the JSON request body for POST /builds.
	CreateBuildRequest = model.CreateBuildRequest
	// UpdateActiveStepRequest carries the target chunk and active step description.
	UpdateActiveStepRequest = model.UpdateActiveStepRequest
)

var (
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

// ExecuteChunkArgs defines the job payload for executing a single Vanadis build chunk.
type ExecuteChunkArgs struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	BuildID    string `json:"build_id"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkID    string `json:"chunk_id,omitempty"`
}

// VanadisServer manages HTTP endpoints and JetStream worker coordination for Vanadis builds.
type VanadisServer struct {
	cfg        Config
	restateCfg RestateConfig
	httpClient *http.Client
	store      VanadisStore
	queueStats consumerStatsQuerier
	auth       *k8sauth.Authenticator
}

// NewVanadisServer constructs a new VanadisServer instance. restateCfg is
// resolved once by app wiring (RunApp); the server never reads env itself.
func NewVanadisServer(cfg Config, restateCfg RestateConfig, st VanadisStore, httpClient *http.Client, auth ...*k8sauth.Authenticator) *VanadisServer {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	var a *k8sauth.Authenticator
	if len(auth) > 0 {
		a = auth[0]
	} else if cfg.Auth.Enabled {
		a = k8sauth.NewAuthenticator(cfg.Auth)
	}
	// Traced so eri turns join the chunk dispatch span (and widget/wake posts carry traceparent).
	return &VanadisServer{
		cfg:        cfg,
		restateCfg: restateCfg,
		httpClient: tracing.Client(httpClient),
		store:      st,
		auth:       a,
	}
}

// SetStore updates the VanadisStore backing the server.
func (s *VanadisServer) SetStore(st VanadisStore) { s.store = st }

func (s *VanadisServer) authEnabled() bool {
	return s != nil && s.auth != nil && s.auth.Enabled()
}

// mcpHandler builds the L2 MCP handler from live server dependencies.
func (s *VanadisServer) mcpHandler() *vanadismcp.Handler {
	if s == nil {
		return vanadismcp.NewHandler(nil, NewClient(RestateConfig{}, nil), false)
	}
	return vanadismcp.NewHandler(s.store, s.restateClient(), s.restateCfg.Enabled, s.auth)
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
	default:
		if _, ok := errors.AsType[interface {
			error
			StatusCode() int
		}](err); ok {
			k8sauth.WriteError(w, err)
			return
		}
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
	// The MCP handler resolves live server dependencies per request so a
	// store or Restate config replaced after Routes() is observed by /mcp
	// and /sse, matching the per-request handleRetryActionChunk wrapper.
	mcpHandler := func(w http.ResponseWriter, r *http.Request) {
		server.newMCPHandler().ServeHTTP(w, r)
	}
	for p, h := range map[string]http.HandlerFunc{
		"GET /health": server.handleHealth, "GET /healthz": server.handleHealth,
		"GET /builds": server.handleListBuilds, "GET /runs": server.handleListBuilds,
		"POST /builds": server.handleCreateBuild, "POST /runs": server.handleCreateBuild,
		"/builds/": server.handleBuildByID, "/runs/": server.handleBuildByID,
		"POST /chunks/active-step":       server.handleChunksActiveStep,
		"/chunks/attach-pr":              server.handleAttachChunkPR,
		"/chunks/":                       server.handleChunkByID,
		"POST /builds/{id}/retry-action": server.handleRetryActionChunk,
		"/mcp":                           mcpHandler, "/sse": mcpHandler,
		"POST /debug/coverage/flush": coverageFlush, "GET /debug/coverage/flush": coverageFlush,
	} {
		mux.HandleFunc(p, h)
	}
	var auth *k8sauth.Authenticator
	if server != nil {
		auth = server.auth
	}
	return tracing.Middleware("vanadis", auth.Middleware(k8sauth.DefaultExemptPaths...)(mux))
}

// Routes returns the HTTP handler for the server.
func (s *VanadisServer) Routes() http.Handler { return SetupMux(s) }

func (*VanadisServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: "ok"})
}

func (s *VanadisServer) authorizeBuildRead(ctx context.Context, action string) error {
	if !s.authEnabled() {
		return nil
	}
	caller, ok := k8sauth.CallerFromContext(ctx)
	if !ok {
		return &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity}
	}
	return s.auth.Authorize(ctx, caller, "builds", action, "*")
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
	threadID, objective := discordutil.NormalizeThreadID(req.ThreadID), strings.TrimSpace(req.Objective)
	switch {
	case !discordutil.IsSnowflake(threadID):
		writeErr(w, http.StatusBadRequest, "thread_id is required and must be a numeric Discord snowflake")
		return
	case objective == "":
		writeErr(w, http.StatusBadRequest, "objective is required")
		return
	case len(req.Chunks) == 0:
		writeErr(w, http.StatusBadRequest, "at least one chunk is required")
		return
	}
	buildID := strutil.FirstNonEmpty(req.ID, buildplan.GenerateBuildID())
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

	chunks, err := model.ParseCreateBuildChunks(buildID, now, req.Chunks)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	var targetAgent string
	if s.authEnabled() {
		caller, ok := k8sauth.CallerFromContext(r.Context())
		if !ok {
			k8sauth.WriteError(w, &k8sauth.UnauthorizedError{Message: errMissingCallerIdentity})
			return
		}
		resolved, err := s.auth.AuthorizeTarget(r.Context(), caller, "builds", "create", req.Agent)
		if err != nil {
			k8sauth.WriteError(w, err)
			return
		}
		targetAgent = resolved
	} else {
		targetAgent = strings.TrimSpace(req.Agent)
		if targetAgent == "" {
			targetAgent = "aerial"
		}
	}
	req.Agent = targetAgent

	var created VanadisBuild
	if err := s.callRestateIngress(r.Context(), s.restateObjectPath(targetAgent, buildID, "Create"), req, &created); err != nil {
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
	build, ok := s.authorizeBuildMutation(w, r, id)
	if !ok {
		return
	}
	var cancelled VanadisBuild
	if err := s.mutateBuildObject(r.Context(), build, "Cancel", CancelBuildRequest{}, &cancelled); err != nil {
		writeStoreErr(r.Context(), w, err)
		return
	}
	RecordBuildTotal("cancelled")
	httpkit.WriteJSON(w, http.StatusOK, map[string]string{fieldStatus: statusCancelled, fieldBuildID: id})
}

func (s *VanadisServer) handleBuildRoot(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet:
		if err := s.authorizeBuildRead(r.Context(), "get"); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
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
		case "active-step":
			s.handleBuildActiveStep(w, r, id)
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
	if err := s.authorizeBuildRead(r.Context(), "list"); err != nil {
		k8sauth.WriteError(w, err)
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
