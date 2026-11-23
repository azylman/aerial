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
	_ "time/tzdata"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/internal/server"
	"github.com/azylman/aerial/services/eri/pkg/bootstrap"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

var (
	loadConfigFn = config.New
	runAppFn     = server.RunBrainApp
	fatalfFn     = log.Fatalf
	tracingPath  = tracing.DefaultConfigPath
)

func runMain(
	ctx context.Context,
	loadCfg func() (*config.Config, error),
	runApp func(context.Context, *config.Config, ...server.BrainAppOption) error,
	tracePath string,
) error {
	cfg, err := loadCfg()
	if err != nil {
		return err
	}

	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = bootstrap.DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = bootstrap.DefaultDataDir()
	}
	if cur.Port == "" {
		cur.Port = "8080"
	}
	cfg.Update(cur)

	withTracing, shutdownTracing := server.SetupTracing(ctx, tracePath)
	defer shutdownTracing()

	if err := runApp(ctx, cfg, withTracing); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runMain(ctx, loadConfigFn, runAppFn, tracingPath); err != nil {
		fatalfFn("Server failed: %v", err)
	}
}
