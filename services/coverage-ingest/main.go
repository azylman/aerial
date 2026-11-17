// Command coverage-ingest is a one-shot collector, run by a CronJob, that pulls
// in-memory Go coverage counters from every configured /debug/coverage/flush target,
// merges them per service, and upserts production_code_coverage.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

// run parses flags, loads config, and ingests once. Database settings come from the
// standard libpq PG* environment (PGHOST, PGUSER, PGPASSWORD, PGDATABASE, ...).
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

	if err := Ingest(ctx, cfg, &http.Client{}, ExecCovdata{GoBin: *goBin}, PostgresStore{DB: pool}); err != nil {
		log.Printf("[coverage-ingest] finished with failures: %v", err)
		return 1
	}
	log.Printf("[coverage-ingest] ingested %d targets", len(cfg.Targets))
	return 0
}
