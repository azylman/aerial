package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/azylman/aerial/services/coverage-ingest/internal/server"
)

var (
	runFn  = server.Run
	exitFn = os.Exit
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exitFn(runFn(ctx, os.Args[1:], os.Stderr))
}
