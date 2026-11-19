// Command vanadis is the thin entrypoint for the Vanadis orchestrator service.
// All HTTP routes, handlers, and lifecycle logic live in internal/server;
// this file only wires injectable dependencies for hermetic testing.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/server"
)

var (
	loadConfigFn  = config.LoadConfigFromEnv
	runAppFn      = server.RunApp
	exitFn        = os.Exit
	initTracingFn = tracing.Init
)

func run() int {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := initTracingFn(ctx, "vanadis", tracing.DefaultConfigPath)
	defer flushTracing()
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))
	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("[vanadis] Error: %v", err)
		return 1
	}
	return 0
}

func main() { exitFn(run()) }
