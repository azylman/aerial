package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/strutil"
)

const (
	searchModeAuto     = "auto"
	searchModeSessions = "sessions"
	searchModeSteps    = "steps"
)

// Server exposes the transcripts read API: search, steps, stats, watermarks.
type Server struct {
	store   *Store
	auth    *k8sauth.Authenticator
	mux     *http.ServeMux
	handler http.Handler
}

// NewServer wires the read API with constructor injection.
func NewServer(store *Store, auth ...*k8sauth.Authenticator) *Server {
	var a *k8sauth.Authenticator
	if len(auth) > 0 {
		a = auth[0]
	}
	s := &Server{store: store, auth: a, mux: http.NewServeMux()}
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("transcripts"))

	s.mux.HandleFunc("/watermark", s.handleWatermarks)
	s.mux.HandleFunc("/watermarks", s.handleWatermarks)
	s.mux.HandleFunc("/v1/watermarks", s.handleWatermarks)

	s.mux.HandleFunc("/search", s.handleSearch)
	s.mux.HandleFunc("/transcripts/search", s.handleSearch)
	s.mux.HandleFunc("/api/transcripts/search", s.handleSearch)

	s.mux.HandleFunc("/steps", s.handleSteps)
	s.mux.HandleFunc("/transcripts/steps", s.handleSteps)
	s.mux.HandleFunc("/api/transcripts/steps", s.handleSteps)

	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/transcripts/stats", s.handleStats)
	s.mux.HandleFunc("/api/transcripts/stats", s.handleStats)

	s.mux.HandleFunc("/transcripts/threads/", s.handleThreadTrajectory)
	s.handler = s.auth.Middleware(k8sauth.DefaultExemptPaths...)(s.mux)
	return s
}

// Handler serves the read API routes.
func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request, requestedAgent string) (effectiveAgent string, ok bool) {
	target := strings.TrimSpace(requestedAgent)
	if s.auth == nil || !s.auth.Enabled() {
		return target, true
	}
	caller, okCaller := k8sauth.CallerFromContext(r.Context())
	if !okCaller {
		writeError(w, http.StatusUnauthorized, "unauthorized: missing caller identity")
		return "", false
	}
	resolved, err := s.auth.AuthorizeTarget(r.Context(), caller, "transcripts", "get", target)
	if err != nil && target == "" && caller.CallerAgent != "" {
		if _, isForbidden := errors.AsType[*k8sauth.ForbiddenError](err); isForbidden {
			resolved, err = s.auth.AuthorizeTarget(r.Context(), caller, "transcripts", "get", caller.CallerAgent)
		}
	}
	if err != nil {
		k8sauth.WriteError(w, err)
		return "", false
	}
	return resolved, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[Transcripts] warning: failed to encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func queryInt(r *http.Request, key string, def int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	if err := s.store.checkDB(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database is not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	requestedStack := strutil.FirstNonEmpty(q.Get("agent"), q.Get("stack"))
	stack, ok := s.authorize(w, r, requestedStack)
	if !ok {
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	queryText := strutil.FirstNonEmpty(q.Get("q"), q.Get("query"))
	mode := strings.ToLower(strings.TrimSpace(q.Get("mode")))
	if mode == "" {
		mode = searchModeAuto
	}
	session := strings.TrimSpace(q.Get("session"))
	tool := strings.TrimSpace(q.Get("tool"))
	limit := queryInt(r, "limit", 20)

	var sinceTimes []time.Time
	if sinceStr := strings.TrimSpace(q.Get("since")); sinceStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, sinceStr); err == nil {
			sinceTimes = append(sinceTimes, t)
		} else if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			sinceTimes = append(sinceTimes, t)
		}
	}

	sessions := []SessionResult{}
	steps := []StepResult{}

	if (mode == searchModeAuto || mode == searchModeSessions) && queryText != "" {
		res, err := s.store.SearchSessions(r.Context(), stack, queryText, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "search sessions failed")
			return
		}
		if res != nil {
			sessions = res
		}
	}

	if mode == searchModeAuto || mode == searchModeSteps {
		res, err := s.store.SearchSteps(r.Context(), stack, queryText, session, tool, limit, sinceTimes...)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "search steps failed")
			return
		}
		if res != nil {
			steps = res
		}
	}

	total := len(sessions) + len(steps)
	writeJSON(w, http.StatusOK, map[string]any{
		"query":            queryText,
		"mode":             mode,
		searchModeSessions: sessions,
		searchModeSteps:    steps,
		"total":            total,
	})
}

func (s *Server) handleStepWindow(w http.ResponseWriter, r *http.Request, stack string) {
	q := r.URL.Query()
	session := strings.TrimSpace(q.Get("session"))
	if session == "" {
		writeError(w, http.StatusBadRequest, "session is required")
		return
	}
	rawStep := strings.TrimSpace(q.Get("step"))
	if rawStep == "" {
		writeError(w, http.StatusBadRequest, "step is required")
		return
	}
	step, err := strconv.Atoi(rawStep)
	if err != nil || step < 0 {
		writeError(w, http.StatusBadRequest, "invalid step parameter")
		return
	}
	contextVal := 2
	if q.Has("context") {
		cVal, ctxErr := strconv.Atoi(strings.TrimSpace(q.Get("context")))
		if ctxErr != nil {
			writeError(w, http.StatusBadRequest, "invalid context parameter")
			return
		}
		contextVal = cVal
	}
	if contextVal < 0 {
		contextVal = 0
	} else if contextVal > 20 {
		contextVal = 20
	}
	startStep := max(0, step-contextVal)
	endStep := step + contextVal

	steps, err := s.store.GetSteps(r.Context(), stack, session, startStep, endStep)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get steps failed")
		return
	}
	if steps == nil {
		steps = []StepResult{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":    session,
		"step":          step,
		"context":       contextVal,
		searchModeSteps: steps,
	})
}

func (s *Server) handleSteps(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	requestedStack := strutil.FirstNonEmpty(q.Get("agent"), q.Get("stack"))
	stack, ok := s.authorize(w, r, requestedStack)
	if !ok {
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	if r.URL.Query().Has("step") {
		s.handleStepWindow(w, r, stack)
		return
	}
	steps, err := s.store.GetSteps(r.Context(), stack, q.Get("session"), queryInt(r, "start", 0), queryInt(r, "end", 1<<30))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get steps failed")
		return
	}
	if steps == nil {
		steps = []StepResult{}
	}
	writeJSON(w, http.StatusOK, map[string]any{searchModeSteps: steps})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	requestedStack := strutil.FirstNonEmpty(r.URL.Query().Get("agent"), r.URL.Query().Get("stack"))
	stack, ok := s.authorize(w, r, requestedStack)
	if !ok {
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	stats, err := s.store.GetStats(r.Context(), stack)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get stats failed")
		return
	}
	if stats == nil {
		stats = []StackStats{}
	}
	totalSessions := 0
	totalSteps := 0
	for _, st := range stats {
		totalSessions += st.Sessions
		totalSteps += st.Steps
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total_sessions": totalSessions,
		"total_steps":    totalSteps,
		"stacks":         stats,
		"stats":          stats,
		"agents":         stats,
		"status":         "ok",
	})
}

func (s *Server) handleWatermarks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	requestedStack := strutil.FirstNonEmpty(q.Get("agent"), q.Get("stack"))
	stack, ok := s.authorize(w, r, requestedStack)
	if !ok {
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	sessionID := strutil.FirstNonEmpty(q.Get("session_id"), q.Get("session"))
	if sessionID != "" {
		step, err := s.store.GetWatermark(r.Context(), stack, sessionID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "get watermark failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"last_indexed_step": step})
		return
	}
	marks, err := s.store.GetWatermarks(r.Context(), stack)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get watermarks failed")
		return
	}
	if marks == nil {
		marks = []SessionWatermark{}
	}
	sessionsMap := make(map[string]int)
	for _, m := range marks {
		sessionsMap[m.SessionID] = m.LastIndexedStep
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"watermarks": marks,
		"sessions":   sessionsMap,
	})
}

func (s *Server) handleThreadTrajectory(w http.ResponseWriter, r *http.Request) {
	requestedStack := strutil.FirstNonEmpty(r.URL.Query().Get("agent"), r.URL.Query().Get("stack"))
	stack, ok := s.authorize(w, r, requestedStack)
	if !ok {
		return
	}
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/transcripts/threads/")
	if path == "trajectory" || path == "/trajectory" {
		writeError(w, http.StatusBadRequest, "thread_id is required")
		return
	}
	if !strings.HasSuffix(path, "/trajectory") {
		http.NotFound(w, r)
		return
	}
	threadID := strings.TrimSuffix(path, "/trajectory")
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		writeError(w, http.StatusBadRequest, "thread_id is required")
		return
	}
	limit := queryInt(r, "limit", 50)
	facts, tools, err := s.store.GetThreadTrajectory(r.Context(), stack, threadID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get thread trajectory failed")
		return
	}
	if facts == nil {
		facts = []string{}
	}
	if tools == nil {
		tools = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"retrieved_facts": facts,
		"tool_calls":      tools,
	})
}
