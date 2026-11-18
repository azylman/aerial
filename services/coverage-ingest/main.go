// Command coverage-ingest is the always-on production coverage collector. It pulls
// in-memory Go coverage counters from every configured /debug/coverage/flush target on
// an interval, accepts flush archives pushed by short-lived jobs, merges counters per
// service, and upserts production_code_coverage.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// shutdownTimeout bounds how long in-flight pushes may finish after SIGTERM; after it,
// their contexts are cancelled and their connections closed.
const shutdownTimeout = 30 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

// run parses flags and loads config, then serves pushes and runs the pull loop until
// ctx ends. Database settings come from the standard libpq PG* environment (PGHOST,
// PGUSER, PGPASSWORD, PGDATABASE, ...).
func run(ctx context.Context, args []string, stderr io.Writer) int {
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
	cfg, err := ParseConfig(data)
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
	srv := NewServer(cfg, &http.Client{}, ExecCovdata{GoBin: *goBin}, PostgresStore{DB: pool})
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
