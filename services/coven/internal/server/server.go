package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// Server encapsulates the Streamable HTTP MCP server and auxiliary HTTP endpoints.
type Server struct {
	handler       *ToolHandler
	mcpServer     *mcp.Server
	opts          *mcp.StreamableHTTPOptions
	streamHandler http.Handler
	role          string
	stack         string
	auth          *k8sauth.Authenticator
	extractFn     func(ctx context.Context, threadID, requestID string) error
}

// serverInstructions is sent on initialize so clients that defer tool schemas
// still learn the search-then-store workflow and correction semantics.
const serverInstructions = `Run memory_search before memory_store; results carry the fact IDs other tools take.
memory_store already dedupes: text at >= 0.85 cosine similarity to an existing fact only reinforces that fact and keeps its old wording. So to correct or update a fact, call memory_store with supersedes_id set to the ID from memory_search; that rewrites it in place. Do not forget + store, which loses reinforcement history and may still collide with a neighbor.
Never store secrets, tokens or credentials, including in supersedes_id updates (they skip the admission gate).`

// NewServer creates and initializes the Server.
func NewServer(handler *ToolHandler, extractFn ...func(ctx context.Context, threadID, requestID string) error) *Server {
	if handler == nil {
		panic("server: handler cannot be nil")
	}

	opts := &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "coven",
		Version: "1.0.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})

	fn := func(ctx context.Context, threadID, _ string) error { return handler.Extract(ctx, threadID) }
	if len(extractFn) > 0 {
		fn = extractFn[0]
	}

	var auth *k8sauth.Authenticator
	stack := config.DefaultStack
	if handler.cfg != nil {
		if handler.cfg.Auth.Enabled {
			auth = k8sauth.NewAuthenticator(handler.cfg.Auth)
		}
		stack = handler.cfg.EffectiveStack()
	}

	s := &Server{
		handler:   handler,
		mcpServer: mcpServer,
		opts:      opts,
		role:      handler.Role(),
		stack:     stack,
		auth:      auth,
		extractFn: fn,
	}
	s.registerTools()

	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.mcpServer
	}, s.opts)

	return s
}

func (s *Server) authEnabled() bool {
	return s.auth != nil && s.auth.Enabled()
}

func (s *Server) registerTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "memory_search",
		Description: "Perform hybrid RRF and MMR diversity search over persisted semantic facts.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MemorySearchArgs) (*mcp.CallToolResult, MemorySearchOutput, error) {
		if s.authEnabled() {
			caller, _ := k8sauth.CallerFromContext(ctx)
			if err := s.auth.Authorize(ctx, caller, "memories", "get", ""); err != nil {
				return nil, MemorySearchOutput{}, err
			}
		}
		out, err := s.handler.MemorySearch(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "memory_store",
		Description: "Persist, update, or reinforce a semantic fact in long-term memory. Subject to admission gate constraints (15 to 300 characters, standalone third-person durable fact, no URLs, markdown code blocks, or ephemeral turn chatter). Facts with >=0.85 cosine similarity to an existing memory automatically reinforce it.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MemoryStoreArgs) (*mcp.CallToolResult, MemoryStoreOutput, error) {
		if s.authEnabled() {
			caller, _ := k8sauth.CallerFromContext(ctx)
			resolved, err := s.auth.AuthorizeTarget(ctx, caller, "memories", "create", args.TargetAgent)
			if err != nil {
				return nil, MemoryStoreOutput{}, err
			}
			args.TargetAgent = resolved
		}
		out, err := s.handler.MemoryStore(ctx, args)
		return nil, out, err
	})
}

// Routes constructs the HTTP router.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("coven"))
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/admin/extract", s.handleAdminExtract)

	if strings.EqualFold(s.role, "extractor") {
		return s.auth.Middleware(k8sauth.DefaultExemptPaths...)(mux)
	}

	mux.HandleFunc("/facts", s.handleFacts)
	mux.HandleFunc("/context", s.handleContext)
	mux.Handle("/consolidate", tracing.Middleware("coven", http.HandlerFunc(s.handleConsolidate)))

	mcpBridge := tracing.Middleware("coven", http.HandlerFunc(s.handleMCP))
	mux.Handle("/mcp", mcpBridge)
	mux.Handle("/sse", mcpBridge)
	mux.Handle("/", mcpBridge)

	return s.auth.Middleware(k8sauth.DefaultExemptPaths...)(mux)
}

func (*Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "coven", []byte(`{"status":"ok"}`))
}

func (s *Server) handleFacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "memories", "get", ""); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

	q := r.URL.Query().Get("q")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 0
	}
	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		offset = 0
	}

	filter := store.FactsFilter{
		Query:  q,
		Limit:  limit,
		Offset: offset,
	}

	res, err := s.handler.GetFacts(r.Context(), filter)
	if err != nil {
		log.Printf("[coven] error getting facts: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("[coven] json encode error: %v", err)
	}
}

func (s *Server) handleContext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "memories", "get", ""); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

	q := r.URL.Query().Get("q")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 0
	}

	res, err := s.handler.GetContext(r.Context(), q, limit)
	if err != nil {
		log.Printf("[coven] error getting context: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "text/plain") && !strings.Contains(accept, "application/json") {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		httpkit.WriteResponse(w, r, "coven", []byte(res.Context))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("[coven] json encode error: %v", err)
	}
}

func (s *Server) handleConsolidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "memories", "update", ""); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

	consolidated, err := s.handler.Consolidate(context.WithoutCancel(r.Context()))
	if err != nil {
		log.Printf("[memory-mcp] error consolidating facts: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "coven", []byte(fmt.Sprintf(`{"status":"ok","consolidated":%d}`, consolidated)))
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	httpkit.ServeMCPStream(w, r, s.streamHandler, "Aerial Memory MCP Server (Streamable HTTP) Ready\n", "memory_memory_", "memory_")
}

func (s *Server) handleAdminExtract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "memories", "update", ""); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

	threadID := strings.TrimSpace(r.URL.Query().Get("thread_id"))
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if threadID == "" && r.Body != nil {
		var body struct {
			ThreadID  string `json:"thread_id"`
			RequestID string `json:"request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			threadID = strings.TrimSpace(body.ThreadID)
			if requestID == "" {
				requestID = strings.TrimSpace(body.RequestID)
			}
		}
	}

	if threadID == "" {
		http.Error(w, `{"error":"thread_id is required"}`, http.StatusBadRequest)
		return
	}

	if err := s.extractFn(r.Context(), threadID, requestID); err != nil {
		log.Printf("[coven] admin extract error: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "coven", []byte(`{"status":"ok"}`))
}
