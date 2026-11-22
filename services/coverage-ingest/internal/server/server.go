package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/coverage-ingest/internal/config"
	"github.com/azylman/aerial/services/coverage-ingest/internal/store"
)

// shutdownTimeout bounds how long in-flight pushes may finish after SIGTERM; after it,
// their contexts are cancelled and their connections closed.
const shutdownTimeout = 30 * time.Second

// Server is the always-on collector: it pulls every configured target on an interval
// and accepts coverage archives pushed by short-lived jobs.
type Server struct {
	cfg    atomic.Pointer[config.Config]
	client *http.Client
	runner store.CovdataRunner
	store  store.CoverageStore

	// sem is a one-slot semaphore serializing every ingestService call across pull
	// rounds and pushes; waiting on it honors ctx so shutdown never hangs behind it.
	sem chan struct{}
}

// NewServer returns a Server serving cfg until a reload replaces it.
func NewServer(cfg config.Config, client *http.Client, runner store.CovdataRunner, st store.CoverageStore) *Server {
	s := &Server{client: client, runner: runner, store: st, sem: make(chan struct{}, 1)}
	s.cfg.Store(&cfg)
	return s
}

// Config returns the current configuration snapshot.
func (s *Server) Config() config.Config {
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
	n, err := config.ExtractFlushArchive(body, dir)
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

// PullLoop prunes retired services and runs a pull round now and then every PullInterval until ctx ends. Before
// each later round it reloads configPath; a file that fails to read or parse is logged
// and the last good snapshot stays in effect. Round failures are logged, never fatal.
func (s *Server) PullLoop(ctx context.Context, configPath string) {
	interval := s.Config().PullInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// The startup snapshot was loaded and validated by the caller.
	loaded := true
	for {
		if loaded {
			s.pruneRetired(ctx)
		}
		s.pullRound(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		loaded = s.reload(configPath)
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

// pruneRetired deletes rows of services the current snapshot no longer collects. It
// refuses when the keep set is empty; failures are logged, never fatal.
func (s *Server) pruneRetired(ctx context.Context) {
	keep, err := config.PruneKeepSet(s.Config())
	if err != nil {
		log.Printf("[coverage-ingest] skipping prune: %v", err)
		return
	}
	pruned, err := s.store.PruneServices(ctx, keep)
	if err != nil {
		log.Printf("[coverage-ingest] prune failed: %v", err)
		return
	}
	for _, service := range slices.Sorted(maps.Keys(pruned)) {
		log.Printf("[coverage-ingest] pruned %d rows of retired service %s", pruned[service], service)
	}
}

// reload replaces the snapshot with configPath's contents when they parse and reports
// whether it did.
func (s *Server) reload(configPath string) bool {
	data, err := os.ReadFile(configPath)
	if err == nil {
		var cfg config.Config
		if cfg, err = config.ParseConfig(data); err == nil {
			s.cfg.Store(&cfg)
			return true
		}
	}
	log.Printf("[coverage-ingest] keeping last good config: %v", err)
	return false
}

// Run parses flags and loads config, then serves pushes and runs the pull loop until
// ctx ends. Database settings come from the standard libpq PG* environment (PGHOST,
// PGUSER, PGPASSWORD, PGDATABASE, ...).
func Run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("coverage-ingest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to the coverage-ingest YAML config")
	goBin := fs.String("go", "go", "go binary used for `go tool covdata`")
	errLog := log.New(stderr, "coverage-ingest: ", 0)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		errLog.Print("-config is required")
		return 2
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		errLog.Printf("read config: %v", err)
		return 1
	}
	cfg, err := config.ParseConfig(data)
	if err != nil {
		errLog.Print(err)
		return 1
	}
	pool, err := pgxpool.New(ctx, "")
	if err != nil {
		errLog.Printf("postgres config: %v", err)
		return 1
	}
	defer pool.Close()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		errLog.Printf("listen: %v", err)
		return 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := NewServer(cfg, &http.Client{}, store.ExecCovdata{GoBin: *goBin}, store.PostgresStore{DB: pool})
	// Request contexts outlive ctx so in-flight pushes may finish during the grace period,
	// and are cancelled explicitly once it expires.
	reqCtx, cancelReqs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelReqs()
	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return reqCtx },
	}
	pullDone := make(chan struct{})
	go func() {
		defer close(pullDone)
		srv.PullLoop(ctx, *configPath)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	log.Printf("[coverage-ingest] serving pushes on %s, pulling %d targets every %s", ln.Addr(), len(cfg.Targets), cfg.PullInterval)

	code := 0
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		errLog.Printf("serve: %v", err)
		code = 1
	}
	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancelShutdown()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errLog.Printf("shutdown: %v", err)
		code = 1
		cancelReqs()
		if cErr := httpSrv.Close(); cErr != nil {
			errLog.Printf("close: %v", cErr)
		}
	}
	<-pullDone
	return code
}
