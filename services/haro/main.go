package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// BrainPromptRequest is an alias for PromptDispatchRequest used when dispatching prompts to Brain / Eri.
type BrainPromptRequest = PromptDispatchRequest

// RunApp bootstraps the scheduler MCP HTTP server with graceful shutdown.
var openDBFn = NewDB

func RunApp(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}

	database, err := openDBFn(cfg)
	if err != nil {
		return err
	}
	defer closeWarn(database, "database on shutdown")

	// Invariant: ticker goroutine must be bound to a child context and strictly
	// awaited on all exit paths before closing the database to prevent goroutine leaks
	// and use-after-close on database shutdown.
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	var tickerWg sync.WaitGroup
	tickerWg.Add(1)
	go func() {
		defer tickerWg.Done()
		ticker := NewTicker(database, NewHTTPPromptDispatcher(cfg.BrainURL), 30*time.Second)
		ticker.Start(tickerCtx)
	}()
	defer func() {
		tickerCancel()
		tickerWg.Wait()
	}()

	toolHandler := NewToolHandler(cfg, database)
	server := NewServer(toolHandler)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: server.Routes(),
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("Scheduler MCP Server listening on port %s (db: %s)", port, cfg.DatabaseURL)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
	case err := <-errChan:
		return err
	}

	log.Println("Shutting down Scheduler MCP server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		return err
	}
	log.Println("Scheduler MCP server stopped cleanly")
	return nil
}

var (
	configPath = defaultConfigPath
	runAppFn   = RunApp
	exitFn     = log.Fatalf
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	resolvedPath := ResolveConfigPath(configPath)
	cfg, err := LoadConfig(resolvedPath)
	if err != nil {
		exitFn("Fatal configuration error: %v", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runAppFn(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("Server failure: %v", err)
	}
}
