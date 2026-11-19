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
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/router"
)

var (
	loadConfigFn       = config.LoadConfigFromEnv
	loadConfigStrictFn = config.LoadConfigFromEnvStrict
	initTracingFn      = tracing.Init
	runAppFn           = router.RunApp
	fatalfFn           = log.Fatalf
)

func runMain() error {
	cfg := loadConfigFn()
	cfgSrc := httpkit.NewReloadable(&cfg, loadConfigStrictFn, config.RestartRequired)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := initTracingFn(ctx, "synapse", tracing.DefaultConfigPath)
	defer flushTracing()

	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	err := runAppFn(ctx, cfgSrc)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func main() {
	if err := runMain(); err != nil && !errors.Is(err, context.Canceled) {
		fatalfFn("[synapse] Error: %v", err)
	}
}
