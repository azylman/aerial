package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Server struct {
	handler        *ToolHandler
	dispatcher     PromptDispatcher
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

func NewServer(handler *ToolHandler, dispatcher ...PromptDispatcher) *Server {
	if handler == nil {
		panic("server: handler cannot be nil")
	}

	var disp PromptDispatcher
	if len(dispatcher) > 0 && dispatcher[0] != nil {
		disp = dispatcher[0]
	} else if handler.cfg != nil {
		disp = NewHTTPPromptDispatcher(handler.cfg.BrainURL)
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
		handler:    handler,
		dispatcher: disp,
		mcpServer:  mcpServer,
		opts:       opts,
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
		Name:        "get_schedule",
		Description: "Get full details (including prompt) for a single recurring, one-shot, or heartbeat schedule by schedule ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args GetScheduleArgs) (*mcp.CallToolResult, GetScheduleOutput, error) {
		out, err := s.handler.GetSchedule(ctx, args)
		return nil, out, err
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
		var extraSources []string
		if s.handler != nil && s.handler.cfg != nil {
			extraSources = s.handler.cfg.ExtraEventSources
		}
		res, err := HandleWatchEvent(ctx, s.handler.db, args, extraSources...)
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
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("haro"))
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/schedules", s.handleSchedules)
	mux.HandleFunc("/schedules/runs", s.handleScheduleRuns)
	mux.HandleFunc("/schedules/oneshot", s.handleScheduleOneShot)
	mux.HandleFunc("/event-triggers/check", s.handleCheckEventTriggers)
	mux.HandleFunc("/event-triggers/claim", s.handleClaimEventTriggers)
	mux.HandleFunc("/event-triggers/fire", s.handleFireEventTriggers)

	mcpBridge := http.HandlerFunc(s.handleMCP)
	mux.Handle("/mcp", mcpBridge)
	mux.Handle("/sse", mcpBridge)
	mux.Handle("/", mcpBridge)

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "haro", []byte(`{"status":"ok"}`))
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// If GET /health hits root handler
	if r.URL.Path == "/health" {
		s.handleHealth(w, r)
		return
	}
	if r.URL.Path == "/debug/coverage/flush" {
		httpkit.HandleCoverageFlush("haro")(w, r)
		return
	}
	if r.URL.Path == "/metrics" {
		s.handleMetrics(w, r)
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
	if r.URL.Path == "/event-triggers/check" {
		s.handleCheckEventTriggers(w, r)
		return
	}
	if r.URL.Path == "/event-triggers/claim" {
		s.handleClaimEventTriggers(w, r)
		return
	}
	if r.URL.Path == "/event-triggers/fire" {
		s.handleFireEventTriggers(w, r)
		return
	}

	httpkit.ServeMCPStream(w, r, s.streamHandler, "Aerial Scheduler MCP Server (Streamable HTTP) Ready\n", "scheduler_", "")
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return
	}

	s.schedulesCache.mu.Lock()
	now := time.Now().UTC()
	if s.schedulesCache.resp != nil && now.Before(s.schedulesCache.expiresAt) {
		cached := *s.schedulesCache.resp
		cached.SystemTime = now
		s.schedulesCache.mu.Unlock()
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		httpkit.WriteJSON(w, http.StatusOK, cached)
		return
	}
	s.schedulesCache.mu.Unlock()

	ctx := r.Context()
	db := s.handler.db

	metrics, err := GetScheduleSummaryMetrics(ctx, db)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	crons, err := ListCronSchedules(ctx, db, "")
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	heartbeats, err := ListHeartbeatSchedules(ctx, db, "")
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	oneshots, err := ListOneShotSchedules(ctx, db, "")
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScheduleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
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
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
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

	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScheduleOneShot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.db == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return
	}

	var req CreateOneShotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return
	}

	threadID := strings.TrimSpace(req.ThreadID)
	prompt := strings.TrimSpace(req.Prompt)
	if threadID == "" || prompt == "" || req.RunAt.IsZero() {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "thread_id, prompt, and run_at are required"})
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
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.InvalidateSchedulesCache()

	httpkit.WriteJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"id":     schedID,
	})
}

type eventTriggerActionRequest struct {
	Source        string   `json:"source"`
	Target        string   `json:"target,omitempty"`
	Targets       []string `json:"targets,omitempty"`
	EventType     string   `json:"event_type"`
	ContextHeader string   `json:"context_header,omitempty"`
}

func (s *Server) parseEventTriggerActionRequest(w http.ResponseWriter, r *http.Request) (eventTriggerActionRequest, bool) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return eventTriggerActionRequest{}, false
	}
	if s.handler == nil || s.handler.db == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "database not initialized"})
		return eventTriggerActionRequest{}, false
	}
	var req eventTriggerActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return eventTriggerActionRequest{}, false
	}
	req.Source = strings.ToLower(strings.TrimSpace(req.Source))
	req.EventType = strings.ToLower(strings.TrimSpace(req.EventType))
	rawTargets := append([]string{req.Target}, req.Targets...)
	var cleanTargets []string
	for _, t := range rawTargets {
		if clean := strings.TrimSpace(t); clean != "" {
			cleanTargets = append(cleanTargets, clean)
		}
	}
	req.Targets = cleanTargets
	if req.Source == "" || req.EventType == "" || len(req.Targets) == 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "source, event_type, and targets are required"})
		return eventTriggerActionRequest{}, false
	}
	return req, true
}

func (s *Server) handleCheckEventTriggers(w http.ResponseWriter, r *http.Request) {
	req, ok := s.parseEventTriggerActionRequest(w, r)
	if !ok {
		return
	}
	matched, err := CheckEventTriggers(r.Context(), s.handler.db, req.Source, req.Targets, req.EventType)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"matched": matched,
	})
}

func (s *Server) handleClaimEventTriggers(w http.ResponseWriter, r *http.Request) {
	req, ok := s.parseEventTriggerActionRequest(w, r)
	if !ok {
		return
	}
	triggers, err := ClaimEventTriggers(r.Context(), s.handler.db, req.Source, req.Targets, req.EventType)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if triggers == nil {
		triggers = []EventTrigger{}
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"triggers": triggers,
	})
}

func (s *Server) handleFireEventTriggers(w http.ResponseWriter, r *http.Request) {
	req, ok := s.parseEventTriggerActionRequest(w, r)
	if !ok {
		return
	}
	if s.dispatcher == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "dispatcher not initialized"})
		return
	}
	triggers, err := ClaimEventTriggers(r.Context(), s.handler.db, req.Source, req.Targets, req.EventType)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	fired := make([]EventTrigger, 0, len(triggers))
	var firstDispatchErr error
	for _, trig := range triggers {
		prompt := FormatEventTriggerPrompt(req.ContextHeader, trig.Prompt)
		start := time.Now()
		_, dispErr := s.dispatcher.DispatchPrompt(r.Context(), PromptDispatchRequest{
			ChannelID: trig.ChannelID,
			Prompt:    prompt,
			Effort:    trig.Effort,
		})
		RecordScheduleDispatchDuration("event_watch", time.Since(start).Seconds())
		if dispErr != nil {
			RecordScheduleRun("event_watch", "failed")
			if reinsertErr := InsertEventTrigger(context.WithoutCancel(r.Context()), s.handler.db, trig); reinsertErr != nil {
				log.Printf("[Haro] failed to re-insert event trigger %s after dispatch error: %v", trig.ID, reinsertErr)
			}
			if firstDispatchErr == nil {
				firstDispatchErr = dispErr
			}
			continue
		}
		RecordScheduleRun("event_watch", "completed")
		fired = append(fired, trig)
	}
	if firstDispatchErr != nil {
		httpkit.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": firstDispatchErr.Error()})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"fired":    len(fired),
		"triggers": fired,
	})
}
