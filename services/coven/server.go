package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server encapsulates the Streamable HTTP MCP server and auxiliary HTTP endpoints.
type Server struct {
	handler       *ToolHandler
	mcpServer     *mcp.Server
	opts          *mcp.StreamableHTTPOptions
	streamHandler http.Handler
}

// NewServer creates and initializes the Server.
func NewServer(handler *ToolHandler) *Server {
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
	}, nil)

	s := &Server{
		handler:   handler,
		mcpServer: mcpServer,
		opts:      opts,
	}
	s.registerTools()

	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.mcpServer
	}, s.opts)

	return s
}

func (s *Server) registerTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "memory_search",
		Description: "Perform hybrid RRF and MMR diversity search over persisted semantic facts.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MemorySearchArgs) (*mcp.CallToolResult, MemorySearchOutput, error) {
		out, err := s.handler.MemorySearch(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "memory_store",
		Description: "Persist, update, or reinforce a semantic fact in long-term memory.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MemoryStoreArgs) (*mcp.CallToolResult, MemoryStoreOutput, error) {
		out, err := s.handler.MemoryStore(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "memory_forget",
		Description: "Delete an existing semantic fact by its ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args MemoryForgetArgs) (*mcp.CallToolResult, MemoryForgetOutput, error) {
		out, err := s.handler.MemoryForget(ctx, args)
		return nil, out, err
	})
}

// Routes constructs the HTTP router.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("coven", false))
	mux.HandleFunc("/facts", s.handleFacts)
	mux.HandleFunc("/context", s.handleContext)
	mux.HandleFunc("/consolidate", s.handleConsolidate)

	mcpBridge := http.HandlerFunc(s.handleMCP)
	mux.Handle("/mcp", mcpBridge)
	mux.Handle("/sse", mcpBridge)
	mux.Handle("/", mcpBridge)

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
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

	q := r.URL.Query().Get("q")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 0
	}
	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		offset = 0
	}

	filter := FactsFilter{
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
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}
	if r.URL.Path == "/debug/coverage/flush" {
		httpkit.HandleCoverageFlush("coven", false)(w, r)
		return
	}
	if r.URL.Path == "/facts" {
		s.handleFacts(w, r)
		return
	}
	if r.URL.Path == "/context" {
		s.handleContext(w, r)
		return
	}
	if r.URL.Path == "/consolidate" {
		s.handleConsolidate(w, r)
		return
	}

	httpkit.ServeMCPStream(w, r, s.streamHandler, "Aerial Memory MCP Server (Streamable HTTP) Ready\n", "memory_memory_", "memory_")
}
