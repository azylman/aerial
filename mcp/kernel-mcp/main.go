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

	"github.com/azylman/aerial/mcp/kernel-mcp/internal/config"
	"github.com/azylman/aerial/mcp/kernel-mcp/internal/server"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
)

var (
	runServerFn = server.RunServer
	exitFn      = log.Fatalf
)

func main() {
	cfg, err := config.LoadConfig(httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", "/local/config.yaml", "/share/aerial-config/config.yaml", "config.yaml"), os.Getenv)
	if err != nil {
		exitFn("[kernel-mcp] Configuration error: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, flushTracing := tracing.Init(ctx, "kernel-mcp", tracing.DefaultConfigPath)
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))
	err = runServerFn(ctx, cfg)
	flushTracing()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[kernel-mcp] Server error: %v", err)
	}
}
