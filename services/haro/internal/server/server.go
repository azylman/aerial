// Package server is the Layer-4 HTTP/MCP server package for haro: the
// Server type, MCP tool registration, HTTP routes (health, MCP bridge,
// schedules, schedule runs, metrics), one-shot job ingestion, and external
// event-trigger firing. It imports Layer-0 packages internal/model (row
// types, constants, pure helpers) and internal/config (Config,
// DefaultTimezone, RegisterConfigTokens), Layer-1 internal/metrics
// (collectors, record helpers), Layer-2 internal/store (persistence), and
// Layer-3 internal/tools (ToolHandler) plus internal/dispatch (dispatcher,
// TraceFire) only. Root server.go keeps zero-logic aliases until the
// haro-07b purge. Server never imports internal/ticker: ticker wiring stays
// in main.go. SQL text, error strings, cache semantics, and dispatch
// payloads are byte-identical to the root implementation this package was
// extracted from.
package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/metrics"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

type cacheEntry struct {
	expiresAt time.Time
	resp      *model.SchedulesResponse
}

// Server serves the haro MCP endpoint and schedule inspection HTTP routes.
// The handler field is the Layer-3 tool handler (DB/Cfg access); the
// dispatcher defaults from Cfg.EriURL and may carry a JetStream event bus.
type Server struct {
	handler        *tools.ToolHandler
	dispatcher     dispatch.PromptDispatcher
	mcpServer      *mcp.Server
	opts           *mcp.StreamableHTTPOptions
	streamHandler  http.Handler
	auth           *k8sauth.Authenticator
	schedulesCache struct {
		mu      sync.Mutex
		entries map[string]cacheEntry
	}
}

// InvalidateSchedulesCache flushes the cached schedules response.
func (s *Server) InvalidateSchedulesCache() {
	s.schedulesCache.mu.Lock()
	s.schedulesCache.entries = make(map[string]cacheEntry)
	s.schedulesCache.mu.Unlock()
}

// serverInstructions is sent on initialize so clients that defer tool schemas
// still learn cross-tool routing and ID provenance.
const serverInstructions = `Pick the schedule type by delivery semantics:
- schedule_recurring: unconditional message on every cron tick.
- schedule_heartbeat: silent periodic polling that messages only when state materially changes.
- schedule_once: one-time delayed reminder.
- watch_event: fires on an external event (vanadis_build, vanadis_chunk, flux, github, plus configured sources); one-shot unless persistent=true.
Create calls return the ID other tools need. list_schedules omits event watches; use list_event_watches / cancel_event_watch.
Prefer update_cron_schedule or update_heartbeat over cancel + recreate so the ID stays stable; get_schedule returns the full prompt. cancel_schedule deletes permanently.`

func NewServer(handler *tools.ToolHandler, dispatcher ...dispatch.PromptDispatcher) *Server {
	if handler == nil {
		panic("server: handler cannot be nil")
	}
	if handler.Cfg != nil {
		config.RegisterConfigTokens(handler.Cfg)
	}

	var disp dispatch.PromptDispatcher
	if len(dispatcher) > 0 && dispatcher[0] != nil {
		disp = dispatcher[0]
	} else if handler.Cfg != nil && strings.TrimSpace(handler.Cfg.EriURL) != "" {
		disp = dispatch.NewHTTPPromptDispatcher(handler.Cfg.EriURL, handler.Cfg.RestateURL, handler.Cfg.AgentName)
	}

	opts := &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	}

	mcpServer := mcp.NewServer(&mcp.Implementation{
		Name:    "haro",
		Version: "1.0.0",
	}, &mcp.ServerOptions{Instructions: serverInstructions})

	var auth *k8sauth.Authenticator
	if handler.Cfg != nil && handler.Cfg.Auth.Enabled {
		auth = k8sauth.NewAuthenticator(handler.Cfg.Auth)
	}

	s := &Server{
		handler:    handler,
		dispatcher: disp,
		mcpServer:  mcpServer,
		opts:       opts,
		auth:       auth,
	}
	s.schedulesCache.entries = make(map[string]cacheEntry)
	s.registerTools()

	s.streamHandler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.mcpServer
	}, s.opts)

	return s
}

func (s *Server) authEnabled() bool {
	return s.auth != nil && s.auth.Enabled()
}

func (s *Server) authorizeScheduleTarget(ctx context.Context, agent string) (string, error) {
	if !s.authEnabled() {
		return agent, nil
	}
	caller, _ := k8sauth.CallerFromContext(ctx)
	return s.auth.AuthorizeTarget(ctx, caller, "schedules", "create", agent)
}

func (s *Server) authorizeScheduleVerb(ctx context.Context, verb, agent string) error {
	if !s.authEnabled() {
		return nil
	}
	caller, _ := k8sauth.CallerFromContext(ctx)
	return s.auth.Authorize(ctx, caller, "schedules", verb, agent)
}

func (s *Server) registerTools() {
	s.registerScheduleCreationTools()
	s.registerScheduleUpdateTools()
	s.registerScheduleCancelTool()
	s.registerScheduleQueryTools()
	s.registerEventTools()
}

func (s *Server) registerScheduleCreationTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_recurring",
		Description: "Register a persistent recurring schedule for unconditional user-facing messages on every tick. Fresh Discord threads will be created in the target channel on each run.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.ScheduleRecurringArgs) (*mcp.CallToolResult, tools.ScheduleRecurringOutput, error) {
		resolved, err := s.authorizeScheduleTarget(ctx, args.Agent)
		if err != nil {
			return nil, tools.ScheduleRecurringOutput{}, err
		}
		args.Agent = resolved
		out, err := s.handler.ScheduleRecurring(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_once",
		Description: "Register a persistent one-shot reminder that triggers once at a designated time or relative duration for one-time delayed reminders.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.ScheduleOnceArgs) (*mcp.CallToolResult, tools.ScheduleOnceOutput, error) {
		targetAgent, authErr := s.authorizeScheduleTarget(ctx, args.Agent)
		if authErr != nil {
			return nil, tools.ScheduleOnceOutput{}, authErr
		}
		args.Agent = targetAgent
		res, opErr := s.handler.ScheduleOnce(ctx, args)
		if opErr != nil {
			return nil, tools.ScheduleOnceOutput{}, opErr
		}
		s.InvalidateSchedulesCache()
		return nil, res, nil
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "schedule_heartbeat",
		Description: "Register a persistent heartbeat monitoring schedule evaluated periodically by Haro for silent background polling and edge-triggered state tracking (such as flight status, permit updates, or external API watches) that only messages Discord when state materially changes, whereas schedule_recurring is for unconditional user-facing messages on every tick and schedule_once is for one-time delayed reminders.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.ScheduleHeartbeatArgs) (*mcp.CallToolResult, any, error) {
		resolved, err := s.authorizeScheduleTarget(ctx, args.Agent)
		if err != nil {
			return nil, nil, err
		}
		args.Agent = resolved
		res, err := tools.HandleScheduleHeartbeat(ctx, s.handler.DB, s.handler.Timezone, args.ToMap(), s.handler.RestateIngressURL())
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return res, nil, err
	})
}

func (s *Server) registerScheduleUpdateTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "update_cron_schedule",
		Description: "Update an existing recurring cron schedule's effort tier, cron expression, prompt, title prefix, or timezone.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.UpdateCronScheduleArgs) (*mcp.CallToolResult, tools.UpdateCronScheduleOutput, error) {
		if s.authEnabled() {
			sched, err := store.GetCronSchedule(ctx, s.handler.DB, strings.TrimSpace(args.ScheduleID))
			if err != nil {
				return nil, tools.UpdateCronScheduleOutput{}, err
			}
			if err := s.authorizeScheduleVerb(ctx, "update", sched.Agent); err != nil {
				return nil, tools.UpdateCronScheduleOutput{}, err
			}
		}
		out, err := s.handler.UpdateCronSchedule(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "update_heartbeat",
		Description: "Update an existing heartbeat monitoring schedule.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.UpdateHeartbeatArgs) (*mcp.CallToolResult, any, error) {
		if s.authEnabled() {
			id := strings.TrimSpace(args.ScheduleID)
			if id == "" {
				id = strings.TrimSpace(args.ID)
			}
			sched, err := store.GetHeartbeatSchedule(ctx, s.handler.DB, id)
			if err != nil {
				return nil, nil, err
			}
			if err := s.authorizeScheduleVerb(ctx, "update", sched.Agent); err != nil {
				return nil, nil, err
			}
		}
		res, err := tools.HandleUpdateHeartbeat(ctx, s.handler.DB, args.ToMap(), s.handler.RestateIngressURL())
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return res, nil, err
	})
}

func (s *Server) registerScheduleCancelTool() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "cancel_schedule",
		Description: "Cancel and delete an existing schedule by schedule ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.CancelScheduleArgs) (*mcp.CallToolResult, tools.CancelScheduleOutput, error) {
		if s.authEnabled() {
			agent, err := store.GetScheduleAgent(ctx, s.handler.DB, strings.TrimSpace(args.ScheduleID))
			if err != nil {
				return nil, tools.CancelScheduleOutput{}, err
			}
			if err := s.authorizeScheduleVerb(ctx, "delete", agent); err != nil {
				return nil, tools.CancelScheduleOutput{}, err
			}
		}
		out, err := s.handler.CancelSchedule(ctx, args)
		if err == nil {
			s.InvalidateSchedulesCache()
		}
		return nil, out, err
	})
}

func (s *Server) registerScheduleQueryTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "list_schedules",
		Description: "List all active recurring schedules and pending reminders. Note: event watches are listed via list_event_watches.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.ListSchedulesArgs) (*mcp.CallToolResult, tools.ListSchedulesOutput, error) {
		if err := s.authorizeScheduleVerb(ctx, "list", args.Agent); err != nil {
			return nil, tools.ListSchedulesOutput{}, err
		}
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
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.GetScheduleArgs) (*mcp.CallToolResult, tools.GetScheduleOutput, error) {
		if err := s.authorizeScheduleVerb(ctx, "get", ""); err != nil {
			return nil, tools.GetScheduleOutput{}, err
		}
		out, err := s.handler.GetSchedule(ctx, args)
		return nil, out, err
	})
}

func (s *Server) registerEventTools() {
	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "watch_event",
		Description: "Register a deterministic one-shot event watch trigger fired when an external event occurs.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.WatchEventArgs) (*mcp.CallToolResult, any, error) {
		resolved, err := s.authorizeScheduleTarget(ctx, args.Agent)
		if err != nil {
			return nil, nil, err
		}
		args.Agent = resolved
		var extraSources []string
		if s.handler != nil && s.handler.Cfg != nil {
			extraSources = s.handler.Cfg.ExtraEventSources
		}
		res, err := tools.HandleWatchEvent(ctx, s.handler.DB, args.ToMap(), extraSources...)
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "list_event_watches",
		Description: "List active deterministic event watch triggers.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.ListEventWatchesArgs) (*mcp.CallToolResult, any, error) {
		if err := s.authorizeScheduleVerb(ctx, "list", args.Agent); err != nil {
			return nil, nil, err
		}
		res, err := tools.HandleListEventWatches(ctx, s.handler.DB, args.ToMap())
		return res, nil, err
	})

	mcp.AddTool(s.mcpServer, &mcp.Tool{
		Name:        "cancel_event_watch",
		Description: "Cancel an active deterministic event watch trigger by watch ID.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args tools.CancelEventWatchArgs) (*mcp.CallToolResult, any, error) {
		if s.authEnabled() {
			id := strings.TrimSpace(args.WatchID)
			if id == "" {
				id = strings.TrimSpace(args.ID)
			}
			if id == "" {
				id = strings.TrimSpace(args.ScheduleID)
			}
			trig, err := store.GetEventTrigger(ctx, s.handler.DB, id)
			if err != nil {
				return nil, nil, err
			}
			if err := s.authorizeScheduleVerb(ctx, "delete", trig.Agent); err != nil {
				return nil, nil, err
			}
		}
		res, err := tools.HandleCancelEventWatch(ctx, s.handler.DB, args.ToMap())
		return res, nil, err
	})
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleHealth)
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("haro"))
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/schedules", s.handleSchedules)
	mux.HandleFunc("/schedules/runs", s.handleScheduleRuns)

	mcpBridge := tracing.Middleware("haro", http.HandlerFunc(s.handleMCP))
	mux.Handle("/mcp", mcpBridge)
	mux.Handle("/sse", mcpBridge)
	mux.Handle("/", mcpBridge)

	return s.auth.Middleware(k8sauth.DefaultExemptPaths...)(mux)
}

func (*Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "haro", []byte(`{"status":"ok"}`))
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// If GET /health, /healthz, or /readyz hits root handler
	if r.URL.Path == "/health" || r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
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

	httpkit.ServeMCPStream(w, r, s.streamHandler, "Aerial Scheduler MCP Server (Streamable HTTP) Ready\n", "scheduler_", "")
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{model.FieldError: "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.DB == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: "database not initialized"})
		return
	}

	agentFilter := strings.TrimSpace(r.URL.Query().Get("agent"))

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "schedules", "list", agentFilter); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

	s.schedulesCache.mu.Lock()
	now := time.Now().UTC()
	if entry, ok := s.schedulesCache.entries[agentFilter]; ok && entry.resp != nil && now.Before(entry.expiresAt) {
		cached := *entry.resp
		cached.SystemTime = now
		s.schedulesCache.mu.Unlock()
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		httpkit.WriteJSON(w, http.StatusOK, cached)
		return
	}
	s.schedulesCache.mu.Unlock()

	ctx := r.Context()
	db := s.handler.DB

	summary, err := store.GetScheduleSummaryMetrics(ctx, db, agentFilter)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: err.Error()})
		return
	}

	crons, err := store.ListCronSchedules(ctx, db, "", agentFilter)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: err.Error()})
		return
	}

	heartbeats, err := store.ListHeartbeatSchedules(ctx, db, "", agentFilter)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: err.Error()})
		return
	}

	oneshots, err := store.ListOneShotSchedules(ctx, db, "", agentFilter)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: err.Error()})
		return
	}

	resp := model.SchedulesResponse{
		Status:     "ok",
		SystemTime: now,
		Summary:    summary,
		Crons:      model.FormatCronSchedulesWithDesc(crons),
		Heartbeats: model.FormatHeartbeatSchedulesWithDesc(heartbeats),
		OneShots:   model.FormatOneShotSchedulesJSON(oneshots),
	}

	s.schedulesCache.mu.Lock()
	if s.schedulesCache.entries == nil {
		s.schedulesCache.entries = make(map[string]cacheEntry)
	}
	s.schedulesCache.entries[agentFilter] = cacheEntry{
		resp:      &resp,
		expiresAt: now.Add(5 * time.Second),
	}
	s.schedulesCache.mu.Unlock()

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScheduleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{model.FieldError: "method not allowed"})
		return
	}

	if s.handler == nil || s.handler.DB == nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: "database not initialized"})
		return
	}

	q := r.URL.Query()
	agentFilter := strings.TrimSpace(q.Get("agent"))

	if s.authEnabled() {
		caller, _ := k8sauth.CallerFromContext(r.Context())
		if err := s.auth.Authorize(r.Context(), caller, "schedules", "list", agentFilter); err != nil {
			k8sauth.WriteError(w, err)
			return
		}
	}

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

	runs, total, err := store.GetScheduleRunsPaginated(r.Context(), s.handler.DB, limit, offset, scheduleID, status, agentFilter)
	if err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{model.FieldError: err.Error()})
		return
	}

	if limit <= 0 {
		limit = 50
	} else if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	resp := model.ScheduleRunsResponse{
		Status: "ok",
		Total:  total,
		Limit:  limit,
		Offset: offset,
		Runs:   runs,
	}

	httpkit.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.handler != nil && s.handler.DB != nil {
		if summary, err := store.GetScheduleSummaryMetrics(r.Context(), s.handler.DB); err == nil {
			metrics.SchedulesActive.WithLabelValues(model.ScheduleTypeCron).Set(float64(summary.CronCount))
			metrics.SchedulesActive.WithLabelValues(model.ScheduleTypeHeartbeat).Set(float64(summary.HeartbeatCount))
			metrics.SchedulesActive.WithLabelValues(model.ScheduleTypeOneShot).Set(float64(summary.OneShotCount))
		}
		if triggers, err := store.ListEventTriggers(r.Context(), s.handler.DB, ""); err == nil {
			metrics.SchedulesActive.WithLabelValues(model.ScheduleTypeEventWatch).Set(float64(len(triggers)))
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}

// HandleOneShotJob ingests a durable HaroOneShotJobArgs and inserts it idempotently into the database.
func HandleOneShotJob(ctx context.Context, server *Server, database *sql.DB, msg pubsub.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	dbHandle := database
	if dbHandle == nil && server != nil && server.handler != nil {
		dbHandle = server.handler.DB
	}
	if dbHandle == nil {
		return errors.New("database not initialized")
	}

	var args struct {
		pubsub.HaroOneShotJobArgs

		Agent string `json:"agent,omitempty"`
	}
	if err := json.Unmarshal(msg.Data(), &args); err != nil {
		log.Printf("[Haro] warning: failed to unmarshal one-shot job: %v", err)
		return nil
	}

	threadID := strings.TrimSpace(args.ThreadID)
	prompt := strings.TrimSpace(args.Prompt)
	if threadID == "" || prompt == "" || args.RunAt.IsZero() {
		log.Printf("[Haro] warning: invalid one-shot job payload: thread_id, prompt, and run_at are required")
		return nil
	}

	schedID := strings.TrimSpace(args.ID)
	if schedID == "" {
		schedID = uuid.New().String()
	}

	sched := model.OneShotSchedule{
		ID:        schedID,
		ThreadID:  threadID,
		Prompt:    prompt,
		Effort:    store.NormalizeScheduleEffort(args.Effort),
		RunAt:     args.RunAt.UTC(),
		CreatedAt: time.Now().UTC(),
		Agent:     model.NormalizeAgent(args.Agent),
	}

	if err := store.InsertOneShotSchedule(ctx, dbHandle, sched); err != nil {
		log.Printf("[Haro] failed to insert one-shot schedule %s: %v", schedID, err)
		return err
	}

	if server != nil {
		server.InvalidateSchedulesCache()
	}

	return nil
}

func (s *Server) recoverFailedEventTrigger(ctx context.Context, trig model.EventTrigger) {
	bgCtx := context.WithoutCancel(ctx)
	if !trig.Persistent {
		if err := store.InsertEventTrigger(bgCtx, s.handler.DB, trig); err != nil {
			log.Printf("[Haro] failed to re-insert event trigger %s after dispatch error: %v", trig.ID, err)
		}
		return
	}
	if _, err := s.handler.DB.ExecContext(bgCtx, "UPDATE event_triggers SET updated_at = NULL WHERE id = $1", trig.ID); err != nil {
		log.Printf("[Haro] failed to reset updated_at for persistent trigger %s: %v", trig.ID, err)
	}
}

func (s *Server) fireClaimedTrigger(ctx context.Context, trig model.EventTrigger, contextHeader string, eventID string) error {
	if s.dispatcher == nil {
		s.recoverFailedEventTrigger(ctx, trig)
		return errors.New("dispatcher not initialized")
	}
	prompt := model.FormatEventTriggerPrompt(contextHeader, trig.Prompt)
	msgID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("event_watch:"+strings.TrimSpace(trig.ID)+":"+strings.TrimSpace(eventID))).String()
	start := time.Now()
	_, dispErr := dispatch.TraceFire(ctx, model.ScheduleTypeEventWatch, trig.ID, func(dispCtx context.Context) (dispatch.PromptDispatchResponse, error) {
		return s.dispatcher.DispatchPrompt(dispCtx, dispatch.PromptDispatchRequest{
			ChannelID: trig.ChannelID,
			MessageID: msgID,
			Prompt:    prompt,
			Effort:    trig.Effort,
			Agent:     trig.Agent,
		})
	})
	metrics.RecordScheduleDispatchDuration(model.ScheduleTypeEventWatch, time.Since(start).Seconds())
	if dispErr != nil {
		metrics.RecordScheduleRun(model.ScheduleTypeEventWatch, model.StatusFailed)
		s.recoverFailedEventTrigger(ctx, trig)
		return dispErr
	}
	metrics.RecordScheduleRun(model.ScheduleTypeEventWatch, model.StatusCompleted)
	return nil
}

func (s *Server) HandleExternalEvent(ctx context.Context, ev pubsub.ExternalEvent) error {
	if ev.Source == "healthcheck" || len(ev.Targets) == 0 {
		return nil
	}
	if s.handler == nil || s.handler.DB == nil {
		return errors.New("database not initialized")
	}
	triggers, err := store.ClaimEventTriggers(ctx, s.handler.DB, ev.Source, ev.Targets, ev.EventType)
	if err != nil {
		return err
	}
	for _, trig := range triggers {
		if fireErr := s.fireClaimedTrigger(ctx, trig, ev.ContextHeader, ev.EventID); fireErr != nil {
			log.Printf("[Haro] error firing claimed event trigger %s (%s): %v", trig.ID, trig.Source, fireErr)
		}
	}
	return nil
}
