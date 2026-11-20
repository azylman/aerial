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
	"github.com/azylman/aerial/services/eri-restate/internal/app"
)

var (
	loadConfigFn  = app.LoadConfigFromEnv
	runAppFn      = app.RunApp
	exitFn        = log.Fatalf
	initTracingFn = tracing.Init
)

func main() {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := initTracingFn(ctx, "eri-restate", tracing.DefaultConfigPath)
	defer flushTracing()
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[eri-restate] Error: %v", err)
	}
}
