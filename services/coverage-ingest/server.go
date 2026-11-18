package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"time"
)

// Server is the always-on collector: it pulls every configured target on an interval
// and accepts coverage archives pushed by short-lived jobs.
type Server struct {
	cfg    atomic.Pointer[Config]
	client *http.Client
	runner CovdataRunner
	store  CoverageStore

	// sem is a one-slot semaphore serializing every ingestService call across pull
	// rounds and pushes; waiting on it honors ctx so shutdown never hangs behind it.
	sem chan struct{}
}

// NewServer returns a Server serving cfg until a reload replaces it.
func NewServer(cfg Config, client *http.Client, runner CovdataRunner, store CoverageStore) *Server {
	s := &Server{client: client, runner: runner, store: store, sem: make(chan struct{}, 1)}
	s.cfg.Store(&cfg)
	return s
}

// Config returns the current configuration snapshot.
func (s *Server) Config() Config {
	return *s.cfg.Load()
}

// Handler serves GET /health and POST /v1/coverage/{service}.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeOK(w)
	})
	mux.HandleFunc("POST /v1/coverage/{service}", s.handlePush)
	return mux
}

// handlePush ingests one pushed flush archive for an allowlisted service. Nothing
// outlives the request: the archive is extracted into a temp dir removed on return.
func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	cfg := s.Config()
	if !slices.Contains(cfg.PushServices, service) {
		http.Error(w, fmt.Sprintf("service %q may not push coverage", service), http.StatusNotFound)
		return
	}
	if r.ContentLength > cfg.MaxPushBytes {
		http.Error(w, fmt.Sprintf("archive exceeds %d bytes", cfg.MaxPushBytes), http.StatusRequestEntityTooLarge)
		return
	}
	dir, err := os.MkdirTemp("", "coverage-push-*")
	if err != nil {
		log.Printf("[coverage-ingest] push %s: create dir: %v", service, err)
		http.Error(w, "create work dir failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			log.Printf("[coverage-ingest] cleanup %s: %v", dir, rmErr)
		}
	}()
	body := http.MaxBytesReader(w, r.Body, cfg.MaxPushBytes)
	n, err := ExtractFlushArchive(body, dir)
	if err == nil {
		// The tar reader stops at the end marker; drain so trailing bytes still count
		// against the cap before anything is ingested.
		_, err = io.Copy(io.Discard, body)
	}
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		http.Error(w, fmt.Sprintf("archive exceeds %d bytes", cfg.MaxPushBytes), http.StatusRequestEntityTooLarge)
		return
	case err != nil:
		log.Printf("[coverage-ingest] push %s: rejected archive: %v", service, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case n == 0:
		http.Error(w, "archive has no covcounters files", http.StatusBadRequest)
		return
	}
	if err := s.ingestService(r.Context(), service, []string{dir}); err != nil {
		log.Printf("[coverage-ingest] push %s failed: %v", service, err)
		http.Error(w, "ingest failed", http.StatusInternalServerError)
		return
	}
	writeOK(w)
}

// writeOK writes a plain-text success body; a failed write only means the client left.
func writeOK(w http.ResponseWriter) {
	if _, err := w.Write([]byte("ok\n")); err != nil {
		log.Printf("[coverage-ingest] write response: %v", err)
	}
}

// PullLoop runs a pull round now and then every PullInterval until ctx ends. Before
// each later round it reloads configPath; a file that fails to read or parse is logged
// and the last good snapshot stays in effect. Round failures are logged, never fatal.
func (s *Server) PullLoop(ctx context.Context, configPath string) {
	interval := s.Config().PullInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.pullRound(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.reload(configPath)
		if next := s.Config().PullInterval; next != interval {
			interval = next
			ticker.Reset(interval)
		}
	}
}

// pullRound ingests every target in the current snapshot once.
func (s *Server) pullRound(ctx context.Context) {
	cfg := s.Config()
	if len(cfg.Targets) == 0 {
		return
	}
	if err := s.Ingest(ctx, cfg); err != nil {
		log.Printf("[coverage-ingest] pull round finished with failures: %v", err)
		return
	}
	log.Printf("[coverage-ingest] pulled %d targets", len(cfg.Targets))
}

// reload replaces the snapshot with configPath's contents when they parse.
func (s *Server) reload(configPath string) {
	data, err := os.ReadFile(configPath)
	if err == nil {
		var cfg Config
		if cfg, err = ParseConfig(data); err == nil {
			s.cfg.Store(&cfg)
			return
		}
	}
	log.Printf("[coverage-ingest] keeping last good config: %v", err)
}
