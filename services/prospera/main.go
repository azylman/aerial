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

var prosperaRestartRequired = config.RestartRequired

var (
	configPath   = config.DefaultConfigPath
	loadConfigFn = config.LoadConfig
	runAppFn     = server.RunApp
	exitFn       = log.Fatalf
)

func main() {
	resolvedPath := config.ResolveConfigPath(configPath)
	cfg, err := loadConfigFn(resolvedPath)
	if err != nil {
		exitFn("failed to load config: %v", err)
		return
	}
	cfgSrc := httpkit.NewReloadable(cfg, func() (*config.Config, error) {
		return loadConfigFn(resolvedPath)
	}, prosperaRestartRequired)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		select {
		case <-cfgSrc.RestartRequested():
			stop()
		case <-ctx.Done():
		}
	}()

	_, flushTracing := tracing.Init(ctx, "prospera", tracing.DefaultConfigPath)
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	err = runAppFn(ctx, cfgSrc)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("prospera exited with error: %v", err)
	}
}
