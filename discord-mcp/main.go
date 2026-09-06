package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// PollUpstream checks if upstream TCP port is accepting connections.
func PollUpstream(upstreamPort string, maxAttempts int, delay time.Duration) bool {
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(delay)
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+upstreamPort, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}

// StartProxyServer sets up the HTTP proxy mux and starts the server.
func StartProxyServer(port, upstreamBase string) (*http.Server, error) {
	proxyHandler, err := NewProxyHandler(upstreamBase, BlockedToolNames)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize proxy handler: %w", err)
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

	return srv, nil
}

// RunProxyApp starts the upstream node process and proxy server with graceful shutdown.
func RunProxyApp(ctx context.Context, port, upstreamPort, nodeBin, appPath string) error {
	if port == "" {
		port = "4001"
	}
	if upstreamPort == "" {
		upstreamPort = "4005"
	}
	if nodeBin == "" {
		nodeBin = "node"
	}
	if appPath == "" {
		appPath = "build/app.js"
	}

	nodeEnv := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "PORT=") {
			nodeEnv = append(nodeEnv, e)
		}
	}
	nodeEnv = append(nodeEnv, "PORT="+upstreamPort)

	cmd := exec.CommandContext(ctx, nodeBin, appPath, "--transport", "http", "--port", upstreamPort)
	cmd.Env = nodeEnv
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start upstream Node MCP server: %w", err)
	}

	upstreamBase := fmt.Sprintf("http://127.0.0.1:%s", upstreamPort)
	PollUpstream(upstreamPort, 20, 50*time.Millisecond)

	srv, err := StartProxyServer(port, upstreamBase)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[Discord-MCP] Listening on port %s (proxying to %s with filtered tools)...", port, upstreamBase)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
	case err := <-errChan:
		return err
	}

	log.Println("[Discord-MCP] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	_ = srv.Shutdown(shutdownCtx)

	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(50 * time.Millisecond)
		_ = cmd.Process.Kill()
	}
	log.Println("[Discord-MCP] Server stopped.")
	return nil
}

func main() {
	port := os.Getenv("PORT")
	upstreamPort := os.Getenv("UPSTREAM_PORT")
	nodeBin := os.Getenv("NODE_BIN")
	appPath := os.Getenv("APP_PATH")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunProxyApp(ctx, port, upstreamPort, nodeBin, appPath); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[Discord-MCP] Error: %v", err)
	}
}


