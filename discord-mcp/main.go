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

// Config defines configuration settings for discord-mcp proxy and upstream server.
type Config struct {
	Port         string
	UpstreamPort string
	NodeBin      string
	AppPath      string
}

// DefaultConfig returns production default values for discord-mcp.
func DefaultConfig() *Config {
	return &Config{
		Port:         "4001",
		UpstreamPort: "4005",
		NodeBin:      "node",
		AppPath:      "build/app.js",
	}
}

// NewConfigFromLookup populates Config using lookup with fallback to DefaultConfig.
// Nil lookup is safely guarded.
func NewConfigFromLookup(lookup func(string) string) *Config {
	if lookup == nil {
		return DefaultConfig()
	}
	cfg := DefaultConfig()
	if p := strings.TrimSpace(lookup("PORT")); p != "" {
		cfg.Port = p
	}
	if up := strings.TrimSpace(lookup("UPSTREAM_PORT")); up != "" {
		cfg.UpstreamPort = up
	}
	if nb := strings.TrimSpace(lookup("NODE_BIN")); nb != "" {
		cfg.NodeBin = nb
	}
	if ap := strings.TrimSpace(lookup("APP_PATH")); ap != "" {
		cfg.AppPath = ap
	}
	return cfg
}

// PollUpstream checks if upstream TCP port is accepting connections.
// It checks attempt 0 immediately before sleeping and supports context preemption.
func PollUpstream(ctx context.Context, upstreamPort string, maxAttempts int, delay time.Duration) bool {
	for i := 0; i < maxAttempts; i++ {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+upstreamPort, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		if i < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(delay):
			}
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
		_, _ = w.Write(BuildHealthResponse("aerial-discord-mcp", DefaultBlockedToolList))
	})
	mux.Handle("/mcp", proxyHandler)
	mux.Handle("/", proxyHandler)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return srv, nil
}

var (
	pollUpstreamFn     = PollUpstream
	execCommandContext = exec.CommandContext
	runProxyApp        = RunProxyApp
	exitFn             = log.Fatalf
	onServerReady      func(addr string)
)

// RunProxyApp starts the upstream node process and proxy server with graceful shutdown.
func RunProxyApp(ctx context.Context, cfg *Config) error {
	if err := ValidateConfig(cfg); err != nil {
		return err
	}

	nodeEnv := BuildNodeEnv(os.Environ(), cfg.UpstreamPort)

	cmd := execCommandContext(ctx, cfg.NodeBin, cfg.AppPath, "--transport", "http", "--port", cfg.UpstreamPort)
	cmd.Env = nodeEnv
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start upstream Node MCP server: %w", err)
	}

	upstreamBase := FormatUpstreamURL(cfg.UpstreamPort)
	pollUpstreamFn(ctx, cfg.UpstreamPort, 20, 50*time.Millisecond)

	ln, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		return err
	}
	defer ln.Close()

	srv, err := StartProxyServer(cfg.Port, upstreamBase)
	if err != nil {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		return err
	}

	if onServerReady != nil {
		onServerReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[Discord-MCP] Listening on %s (proxying to %s with filtered tools)...", ln.Addr().String(), upstreamBase)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errChan:
		runErr = err
	}

	log.Println("[Discord-MCP] Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	_ = srv.Shutdown(shutdownCtx)

	if cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	log.Println("[Discord-MCP] Server stopped.")
	return runErr
}

func main() {
	cfg := NewConfigFromLookup(os.Getenv)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runProxyApp(ctx, cfg); err != nil && err != http.ErrServerClosed {
		exitFn("[Discord-MCP] Error: %v", err)
	}
}
