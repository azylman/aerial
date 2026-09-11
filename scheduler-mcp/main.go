package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// RunApp bootstraps the scheduler MCP HTTP server with graceful shutdown.
func RunApp(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("app: config cannot be nil")
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return fmt.Errorf("app: port cannot be empty")
	}

	database, err := InitDB(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = database.Close()
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		return err
	}
	log.Println("Scheduler MCP server stopped cleanly")
	return nil
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunApp(ctx, cfg); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failure: %v", err)
	}
}

