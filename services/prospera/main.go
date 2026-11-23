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

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/server"
)

var (
	configPath   = config.DefaultConfigPath
	loadConfigFn = config.LoadConfigFile
	runAppFn     = server.RunApp
	exitFn       = log.Fatalf
)

func main() {
	runMain(os.Getenv, tracing.DefaultConfigPath)
}

// runMain loads config through getenv, initializes tracing from
// tracingConfigPath and runs the app until a signal or restart request.
func runMain(getenv func(string) string, tracingConfigPath string) {
	resolvedPath := config.ResolveConfigPath(configPath)
	cfg, err := loadConfigFn(resolvedPath, getenv)
	if err != nil {
		exitFn("failed to load config: %v", err)
		return
	}
	cfgSrc := httpkit.NewReloadable(cfg, func() (*config.Config, error) {
		return loadConfigFn(resolvedPath, getenv)
	}, config.RestartRequired)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		select {
		case <-cfgSrc.RestartRequested():
			stop()
		case <-ctx.Done():
		}
	}()

	_, flushTracing := tracing.Init(ctx, "prospera", tracingConfigPath)
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	err = runAppFn(ctx, cfgSrc)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("prospera exited with error: %v", err)
	}
}
