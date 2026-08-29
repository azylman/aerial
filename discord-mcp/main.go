package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4001"
	}

	upstreamPort := os.Getenv("UPSTREAM_PORT")
	if upstreamPort == "" {
		upstreamPort = "4005"
	}

	nodeBin := os.Getenv("NODE_BIN")
	if nodeBin == "" {
		nodeBin = "node"
	}

	appPath := os.Getenv("APP_PATH")
	if appPath == "" {
		appPath = "build/app.js"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Launch upstream Node.js MCP server
	log.Printf("[Discord-MCP] Starting upstream %s on 127.0.0.1:%s...", appPath, upstreamPort)
	cmd := exec.CommandContext(ctx, nodeBin, appPath, "--transport", "http", "--port", upstreamPort)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Fatalf("[Discord-MCP] Failed to start upstream Node MCP server: %v", err)
	}

	// 2. Poll upstream readiness
	upstreamBase := fmt.Sprintf("http://127.0.0.1:%s", upstreamPort)
	ready := false
	for i := 0; i < 30; i++ {
		time.Sleep(500 * time.Millisecond)
		resp, err := http.Get(upstreamBase + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
	}
	if !ready {
		log.Printf("[Discord-MCP] Warning: upstream /health did not return 200 within 15s. Continuing to start proxy.")
	} else {
		log.Printf("[Discord-MCP] Upstream Node server is healthy and ready.")
	}

	// 3. Initialize Proxy Handler
	proxyHandler, err := NewProxyHandler(upstreamBase, BlockedToolNames)
	if err != nil {
		log.Fatalf("[Discord-MCP] Failed to initialize proxy handler: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","wrapper":"aerial-discord-mcp","blocked_tools":["discord_send","send_message","discord_create_thread"]}`))
	})
	mux.Handle("/mcp", proxyHandler)
	mux.Handle("/", proxyHandler)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Discord-MCP] Listening on port %s (proxying to %s with filtered tools)...", port, upstreamBase)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Discord-MCP] HTTP server error: %v", err)
		}
	}()

	<-stopChan
	log.Println("[Discord-MCP] Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	_ = srv.Shutdown(shutdownCtx)
	cancel()

	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(500 * time.Millisecond)
		_ = cmd.Process.Kill()
	}
	log.Println("[Discord-MCP] Server stopped.")
}
