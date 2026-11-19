package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-nats/internal/app"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

var (
	loadConfigFn  = config.LoadConfigFromEnv
	runAppFn      = app.RunApp
	exitFn        = log.Fatalf
	initTracingFn = tracing.Init
)

func runMain() error {
	cfg := loadConfigFn()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := initTracingFn(ctx, "vanadis-nats", tracing.DefaultConfigPath)
	defer flushTracing()
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	if err := runAppFn(ctx, cfg); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := runMain(); err != nil {
		exitFn("[vanadis-nats] Error: %v", err)
	}
}
