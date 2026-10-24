package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/coverage"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Server struct {
	handler        *ToolHandler
	mcpServer      *mcp.Server
	opts           *mcp.StreamableHTTPOptions
	streamHandler  http.Handler
	schedulesCache struct {
		mu        sync.Mutex
		expiresAt time.Time
		resp      *SchedulesResponse
	}
}

// InvalidateSchedulesCache flushes the cached schedules response.
func (s *Server) InvalidateSchedulesCache() {
	s.schedulesCache.mu.Lock()
	s.schedulesCache.resp = nil
	s.schedulesCache.expiresAt = time.Time{}
	s.schedulesCache.mu.Unlock()
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
		Name:    "haro",
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
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "update_cron_schedule",
		Description: "Update an existing recurring cron schedule's effort tier, cron expression, prompt, title prefix, or timezone.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args UpdateCronScheduleArgs) (*mcp.CallToolResult, UpdateCronScheduleOutput, error) {
		out, err := s.handler.UpdateCronSchedule(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_once",
		Description: "Register a persistent one-shot reminder that triggers once at a designated time or relative duration.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ScheduleOnceArgs) (*mcp.CallToolResult, ScheduleOnceOutput, error) {
		out, err := s.handler.ScheduleOnce(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "list_schedules",
		Description: "List all active recurring schedules and pending reminders.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ListSchedulesArgs) (*mcp.CallToolResult, ListSchedulesOutput, error) {
		out, err := s.handler.ListSchedules(ctx, args)
		if err != nil {
			return nil, out, err
		}
		res := &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: out.Summary},
			},
		}
		return res, out, nil
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "cancel_schedule",
		Description: "Cancel and delete an existing schedule by schedule ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args CancelScheduleArgs) (*mcp.CallToolResult, CancelScheduleOutput, error) {
		out, err := s.handler.CancelSchedule(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_heartbeat",
		Description: "Register a persistent heartbeat monitoring schedule evaluated periodically by Haro.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		res, err := HandleScheduleHeartbeat(ctx, s.handler.db, s.handler.timezone, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "update_heartbeat",
		Description: "Update an existing heartbeat monitoring schedule.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		res, err := HandleUpdateHeartbeat(ctx, s.handler.db, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "watch_event",
		Description: "Register a deterministic one-shot event watch trigger fired when an external event occurs.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		res, err := HandleWatchEvent(ctx, s.handler.db, args)
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "list_event_watches",
		Description: "List active deterministic event watch triggers.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		res, err := HandleListEventWatches(ctx, s.handler.db, args)
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "cancel_event_watch",
		Description: "Cancel an active deterministic event watch trigger by watch ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		res, err := HandleCancelEventWatch(ctx, s.handler.db, args)
		return res, nil, err
	})
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/debug/coverage/flush", s.handleCoverageFlush)
	mux.HandleFunc("/schedules", s.handleSchedules)
	mux.HandleFunc("/schedules/runs", s.handleScheduleRuns)
	mux.HandleFunc("/schedules/oneshot", s.handleScheduleOneShot)

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
			log.Printf("[haro] coverage meta flush error: %v", err)
		}
		if err := writeCountersDirFn(dir); err != nil {
			log.Printf("[haro] coverage flush error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	writeResponse(w, r, []byte("ok\n"))
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// If GET /health hits root handler
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}
	if r.URL.Path == "/debug/coverage/flush" {
		s.handleCoverageFlush(w, r)
		return
	}
	if r.URL.Path == "/schedules" {
		s.handleSchedules(w, r)
		return
	}
	if r.URL.Path == "/schedules/runs" {
		s.handleScheduleRuns(w, r)
		return
	}
	if r.URL.Path == "/schedules/oneshot" {
		s.handleScheduleOneShot(w, r)
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

func writeJSON(w http.ResponseWriter, status int, data any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[Scheduler Server] Failed to encode JSON response: %v", err)
	}
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return
	}

	s.schedulesCache.mu.Lock()
	now := time.Now().UTC()
	if s.schedulesCache.resp != nil && now.Before(s.schedulesCache.expiresAt) {
		cached := *s.schedulesCache.resp
		cached.SystemTime = now
		s.schedulesCache.mu.Unlock()
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		writeJSON(w, http.StatusOK, cached)
		return
	}
	s.schedulesCache.mu.Unlock()

	ctx := r.Context()
	db := s.handler.db

	metrics, err := GetScheduleSummaryMetrics(ctx, db)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	crons, err := ListCronSchedules(ctx, db, "")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	heartbeats, err := ListHeartbeatSchedules(ctx, db, "")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	oneshots, err := ListOneShotSchedules(ctx, db, "")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	resp := SchedulesResponse{
		Status:     "ok",
		SystemTime: now,
		Summary:    metrics,
		Crons:      FormatCronSchedulesWithDesc(crons),
		Heartbeats: FormatHeartbeatSchedulesWithDesc(heartbeats),
		OneShots:   FormatOneShotSchedulesJSON(oneshots),
	}

	s.schedulesCache.mu.Lock()
	s.schedulesCache.resp = &resp
	s.schedulesCache.expiresAt = now.Add(5 * time.Second)
	s.schedulesCache.mu.Unlock()

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScheduleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return
	}

	q := r.URL.Query()
	limit := 50
	if lStr := q.Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil {
			limit = l
		}
	}
	offset := 0
	if oStr := q.Get("offset"); oStr != "" {
		if o, err := strconv.Atoi(oStr); err == nil {
			offset = o
		}
	}
	scheduleID := strings.TrimSpace(q.Get("schedule_id"))
	status := strings.TrimSpace(q.Get("status"))

	runs, total, err := GetScheduleRunsPaginated(r.Context(), s.handler.db, limit, offset, scheduleID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	for i := range runs {
		runs[i].Prompt = SanitizeScheduleText(runs[i].Prompt)
		runs[i].Error = SanitizeScheduleText(runs[i].Error)
		runs[i].Title = SanitizeScheduleText(runs[i].Title)
	}

	if limit <= 0 {
		limit = 50
	} else if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	resp := ScheduleRunsResponse{
		Status: "ok",
		Total:  total,
		Limit:  limit,
		Offset: offset,
		Runs:   runs,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScheduleOneShot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return
	}

	var req CreateOneShotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return
	}

	threadID := strings.TrimSpace(req.ThreadID)
	prompt := strings.TrimSpace(req.Prompt)
	if threadID == "" || prompt == "" || req.RunAt.IsZero() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "thread_id, prompt, and run_at are required"})
		return
	}

	schedID := strings.TrimSpace(req.ID)
	if schedID == "" {
		schedID = uuid.New().String()
	}

	effort := normalizeScheduleEffort(req.Effort)

	sched := OneShotSchedule{
		ID:        schedID,
		ThreadID:  threadID,
		Prompt:    prompt,
		Effort:    effort,
		RunAt:     req.RunAt.UTC(),
		CreatedAt: time.Now().UTC(),
	}

	if err := InsertOneShotSchedule(r.Context(), s.handler.db, sched); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.InvalidateSchedulesCache()

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"id":     schedID,
	})
}
