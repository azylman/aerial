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
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/server"
)

var (
	configPath = config.DefaultConfigPath
	runAppFn   = server.RunApp
	exitFn     = log.Fatalf
)

func main() {
	runMain(os.Getenv, tracing.DefaultConfigPath)
}

// runMain loads configuration through getenv, initializes tracing from
// tracingConfigPath and runs the app until a signal arrives.
func runMain(getenv func(string) string, tracingConfigPath string) {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	resolvedPath := config.ResolveConfigPath(configPath)
	cfg, err := config.LoadConfig(resolvedPath, getenv)
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "coven", tracingConfigPath)

	err = runAppFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
