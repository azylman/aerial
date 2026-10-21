package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/coverage"
	"strconv"
	"strings"
	"syscall"

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
		Name:    "memory-mcp",
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
	mux.HandleFunc("/debug/coverage/flush", s.handleCoverageFlush)
	mux.HandleFunc("/facts", s.handleFacts)
	mux.HandleFunc("/context", s.handleContext)

	mcpBridge := http.HandlerFunc(s.handleMCP)
	mux.Handle("/mcp", mcpBridge)
	mux.Handle("/sse", mcpBridge)
	mux.Handle("/", mcpBridge)

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeResponse(w, r, []byte(`{"status":"ok"}`))
}

var (
	writeCountersDirFn = coverage.WriteCountersDir
	writeMetaDirFn     = coverage.WriteMetaDir
)

func (s *Server) handleCoverageFlush(w http.ResponseWriter, r *http.Request) {
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		if err := writeMetaDirFn(dir); err != nil {
			log.Printf("[memory-mcp] coverage meta flush error: %v", err)
		}
		if err := writeCountersDirFn(dir); err != nil {
			log.Printf("[memory-mcp] coverage flush error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	writeResponse(w, r, []byte("ok\n"))
}

func (s *Server) handleFacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query().Get("q")
	category := r.URL.Query().Get("category")
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 0
	}
	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		offset = 0
	}

	filter := FactsFilter{
		Category: category,
		Query:    q,
		Limit:    limit,
		Offset:   offset,
	}

	res, err := s.handler.GetFacts(r.Context(), filter)
	if err != nil {
		log.Printf("[memory-mcp] error getting facts: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("[memory-mcp] json encode error: %v", err)
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
		log.Printf("[memory-mcp] error getting context: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "text/plain") && !strings.Contains(accept, "application/json") {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		writeResponse(w, r, []byte(res.Context))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("[memory-mcp] json encode error: %v", err)
	}
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}
	if r.URL.Path == "/debug/coverage/flush" {
		s.handleCoverageFlush(w, r)
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

	// Handle GET probe for streamable HTTP / SSE
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		writeResponse(w, r, []byte("MCP Memory Service Ready\n"))
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	defer closeWarn(r.Body, "request body")
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
		return
	}

	trimmed := strings.TrimSpace(string(bodyBytes))
	if trimmed == "" {
		http.Error(w, `{"error":"Empty request body"}`, http.StatusBadRequest)
		return
	}

	// Normalize doubly prefixed "name":"memory_memory_<tool>" to "name":"memory_<tool>"
	if strings.Contains(string(bodyBytes), "memory_memory_") {
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":"memory_memory_`), []byte(`"name":"memory_`))
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name": "memory_memory_`), []byte(`"name": "memory_`))
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":  "memory_memory_`), []byte(`"name": "memory_`))
	}

	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.ContentLength = int64(len(bodyBytes))

	r.Header.Set("Accept", "application/json, text/event-stream")
	if r.Header.Get("Content-Type") == "" {
		r.Header.Set("Content-Type", "application/json")
	}

	s.streamHandler.ServeHTTP(w, r)
}

func isClientDisconnect(r *http.Request, err error) bool {
	if err == nil {
		return false
	}
	if r != nil && r.Context().Err() != nil {
		return true
	}
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed)
}

func writeResponse(w http.ResponseWriter, r *http.Request, data []byte) {
	if _, err := w.Write(data); err != nil {
		if !isClientDisconnect(r, err) {
			log.Printf("[memory-mcp] response write error: %v", err)
		}
	}
}
