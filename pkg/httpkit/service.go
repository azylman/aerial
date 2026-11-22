package httpkit

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/azylman/aerial/pkg/tracing"
)

// ServiceSpec describes the bootstrap of a config-driven HTTP service main().
type ServiceSpec[T any] struct {
	// Name is the tracing service name.
	Name string
	// ConfigPath is the already-resolved path handed to Load.
	ConfigPath string
	// TracingConfigPath is the tracing config file passed to tracing.Init.
	TracingConfigPath string
	// Load parses the service config from ConfigPath.
	Load func(path string) (T, error)
	// Run serves until ctx is cancelled.
	Run func(ctx context.Context, cfg T) error
	// Exit reports a fatal error (log.Fatalf in production).
	Exit func(format string, args ...any)
}

// RunService installs the JSON slog handler, loads config, starts tracing and
// runs the service until SIGINT/SIGTERM. Load failures and Run errors other
// than http.ErrServerClosed are reported through Exit.
func RunService[T any](spec ServiceSpec[T]) {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))))
	cfg, err := spec.Load(spec.ConfigPath)
	if err != nil {
		spec.Exit("Fatal configuration error: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, spec.Name, spec.TracingConfigPath)
	if err := spec.Run(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		flushTracing()
		spec.Exit("Server failure: %v", err)
		return
	}
	flushTracing()
}
