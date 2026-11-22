package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/console/internal/config"
	"github.com/azylman/aerial/services/console/internal/server"
)

var (
	defaultConfigPath = config.DefaultConfigPath
	loadConfigFn      = config.LoadConfig
	runConsoleFn      = server.RunConsoleServer
	exitFn            = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	resolvedPath := config.ResolveConfigPath(defaultConfigPath)
	cfg, err := loadConfigFn(resolvedPath)
	if err != nil {
		exitFn("failed to load config: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := tracing.Init(ctx, "console", tracing.DefaultConfigPath)
	err = runConsoleFn(ctx, cfg)
	flushTracing()
	if err != nil {
		exitFn("console failed: %v", err)
	}
}
