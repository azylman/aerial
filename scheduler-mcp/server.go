package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Server struct {
	handler       *ToolHandler
	mcpServer     *mcp.Server
	opts          *mcp.StreamableHTTPOptions
	streamHandler http.Handler
}

func NewServer(handler *ToolHandler) *Server {
	if handler == nil {
		panic("server: handler cannot be nil")
	}

	opts := &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "scheduler-mcp",
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
		Name:        "schedule_recurring",
		Description: "Register a persistent recurring schedule. Fresh Discord threads will be created in the target channel on each run.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ScheduleRecurringArgs) (*mcp.CallToolResult, ScheduleRecurringOutput, error) {
		out, err := s.handler.ScheduleRecurring(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "update_cron_schedule",
		Description: "Update an existing recurring cron schedule's effort tier, cron expression, prompt, title prefix, or timezone.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args UpdateCronScheduleArgs) (*mcp.CallToolResult, UpdateCronScheduleOutput, error) {
		out, err := s.handler.UpdateCronSchedule(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_once",
		Description: "Register a persistent one-shot reminder that triggers once at a designated time or relative duration.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ScheduleOnceArgs) (*mcp.CallToolResult, ScheduleOnceOutput, error) {
		out, err := s.handler.ScheduleOnce(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "list_schedules",
		Description: "List all active recurring schedules and pending reminders.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ListSchedulesArgs) (*mcp.CallToolResult, ListSchedulesOutput, error) {
		out, err := s.handler.ListSchedules(ctx, args)
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "cancel_schedule",
		Description: "Cancel and delete an existing schedule by schedule ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args CancelScheduleArgs) (*mcp.CallToolResult, CancelScheduleOutput, error) {
		out, err := s.handler.CancelSchedule(ctx, args)
		return nil, out, err
	})
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)

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

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// If GET /health hits root handler
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}

	// Handle SSE / Streamable HTTP GET probe
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		writeResponse(w, r, []byte("MCP Scheduler Service Ready\n"))
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Read and normalize body
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

	// Intercept scheduler_ alias prefix in tool calls:
	// Rewrite "name": "scheduler_<tool>" to "name": "<tool>"
	// or "name":"scheduler_<tool>" to "name":"<tool>"
	if strings.Contains(string(bodyBytes), "scheduler_") {
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":"scheduler_`), []byte(`"name":"`))
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name": "scheduler_`), []byte(`"name": "`))
		bodyBytes = bytes.ReplaceAll(bodyBytes, []byte(`"name":  "scheduler_`), []byte(`"name": "`))
	}

	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.ContentLength = int64(len(bodyBytes))

	// Normalize headers to ensure maximum compatibility with single-shot curl clients:
	// 1. Accept must contain both application/json and text/event-stream for Streamable HTTP
	r.Header.Set("Accept", "application/json, text/event-stream")

	// 2. Ensure Content-Type is set to application/json
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
			log.Printf("[Scheduler Server] Failed to write response: %v", err)
		}
	}
}
