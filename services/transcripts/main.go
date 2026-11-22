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
	"github.com/azylman/aerial/services/transcripts/internal/config"
	"github.com/azylman/aerial/services/transcripts/internal/server"
)

var (
	configPath   = config.DefaultConfigPath
	loadConfigFn = config.LoadConfig
	runAppFn     = server.RunApp
	exitFn       = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))))
	cfg, err := loadConfigFn(config.ResolveConfigPath(configPath))
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "transcripts", tracing.DefaultConfigPath)
	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		flushTracing()
		exitFn("Server failure: %v", err)
		return
	}
	flushTracing()
}
