package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// Server exposes the transcripts read API: search, steps, stats, watermarks.
type Server struct {
	store *Store
	mux   *http.ServeMux
}

// NewServer wires the read API with constructor injection.
func NewServer(store *Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/search", s.handleSearch)
	s.mux.HandleFunc("/steps", s.handleSteps)
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/watermarks", s.handleWatermarks)
	return s
}

// Handler serves the read API routes.
func (s *Server) Handler() http.Handler {
	return s.mux
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
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	q := r.URL.Query()
	sessions, err := s.store.SearchSessions(r.Context(), q.Get("stack"), q.Get("q"), queryInt(r, "limit", 20))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search sessions failed")
		return
	}
	steps, err := s.store.SearchSteps(r.Context(), q.Get("stack"), q.Get("q"), q.Get("session"), q.Get("tool"), queryInt(r, "limit", 20))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search steps failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions, "steps": steps})
}

func (s *Server) handleSteps(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	q := r.URL.Query()
	steps, err := s.store.GetSteps(r.Context(), q.Get("stack"), q.Get("session"), queryInt(r, "start", 0), queryInt(r, "end", 1<<30))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get steps failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"steps": steps})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	stats, err := s.store.GetStats(r.Context(), r.URL.Query().Get("stack"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get stats failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
}

func (s *Server) handleWatermarks(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store is nil")
		return
	}
	marks, err := s.store.GetWatermarks(r.Context(), r.URL.Query().Get("stack"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get watermarks failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"watermarks": marks})
}
