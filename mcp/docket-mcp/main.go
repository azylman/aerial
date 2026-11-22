package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
	"github.com/azylman/aerial/mcp/docket-mcp/internal/server"
	"github.com/azylman/aerial/pkg/tracing"
)

var (
	sqlOpenFn   = sql.Open
	runServerFn = runServer
	exitFn      = log.Fatalf
)

func runServer(ctx context.Context, cfg *config.Config) error {
	return server.RunServer(ctx, cfg, sqlOpenFn)
}

func main() {
	cfg, err := config.LoadConfig(config.ResolveConfigPath(), os.Getenv)
	if err != nil {
		exitFn("[docket-mcp] Failed to load config: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "docket-mcp", tracing.DefaultConfigPath)
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	err = runServerFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[docket-mcp] Server exited with error: %v", err)
	}
}
