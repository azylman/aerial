package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the canonical in-container configuration path.
const DefaultConfigPath = "/config/config.yaml"

// Config defines configuration settings for discord-mcp proxy and upstream server.
type Config struct {
	Port         string
	UpstreamPort string
	NodeBin      string
	AppPath      string
}

// DiscordYAMLConfig defines the declarative file schema for /config/config.yaml.
type DiscordYAMLConfig struct {
	Port         string `yaml:"port"`
	UpstreamPort string `yaml:"upstream_port"`
	NodeBin      string `yaml:"node_bin"`
	AppPath      string `yaml:"app_path"`
}

// LoadConfigFile loads and validates the YAML config from path.
// Fails fast if the file is missing, malformed, or required fields are absent.
func LoadConfigFile(path string) (*Config, error) {
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, fmt.Errorf("config: path cannot be empty")
	}
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", cleanPath, err)
	}

	var ycfg DiscordYAMLConfig
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", cleanPath, err)
	}

	port := strings.TrimSpace(ycfg.Port)
	if port == "" {
		return nil, fmt.Errorf("config: port is required in %s", cleanPath)
	}
	upstreamPort := strings.TrimSpace(ycfg.UpstreamPort)
	if upstreamPort == "" {
		return nil, fmt.Errorf("config: upstream_port is required in %s", cleanPath)
	}

	nodeBin := strings.TrimSpace(ycfg.NodeBin)
	if nodeBin == "" {
		nodeBin = "node"
	}
	appPath := strings.TrimSpace(ycfg.AppPath)
	if appPath == "" {
		appPath = "build/app.js"
	}

	return &Config{
		Port:         port,
		UpstreamPort: upstreamPort,
		NodeBin:      nodeBin,
		AppPath:      appPath,
	}, nil
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
			if closeErr := conn.Close(); closeErr != nil {
				log.Printf("[Discord-MCP] Warning closing probe connection: %v", closeErr)
			}
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
		writeProxyResponse(w, r, BuildHealthResponse("aerial-discord-mcp", DefaultBlockedToolList))
	})
	mux.Handle("/mcp", proxyHandler)
	mux.Handle("/", proxyHandler)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return srv, nil
}

func killProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		log.Printf("[Discord-MCP] Warning killing process: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return
		}
		if !errors.Is(err, os.ErrProcessDone) {
			log.Printf("[Discord-MCP] Warning waiting for process: %v", err)
		}
	}
}

var (
	pollUpstreamFn     = PollUpstream
	execCommandContext = exec.CommandContext
	runProxyApp        = RunProxyApp
	exitFn             = log.Fatalf
	onServerReady      func(addr string)
	configPath         = DefaultConfigPath
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
		killProcess(cmd)
		return err
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.Printf("[Discord-MCP] Error closing listener: %v", closeErr)
		}
	}()

	srv, err := StartProxyServer(cfg.Port, upstreamBase)
	if err != nil {
		killProcess(cmd)
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

	if shutErr := srv.Shutdown(shutdownCtx); shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		log.Printf("[Discord-MCP] Server shutdown error: %v", shutErr)
	}

	killProcess(cmd)
	log.Println("[Discord-MCP] Server stopped.")
	return runErr
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := LoadConfigFile(configPath)
	if err != nil {
		exitFn("[Discord-MCP] Fatal configuration error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runProxyApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitFn("[Discord-MCP] Error: %v", err)
	}
}
