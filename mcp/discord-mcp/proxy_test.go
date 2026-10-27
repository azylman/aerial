package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

func init() {
	flag.String("transport", "", "mock transport flag for TestHelperProcess")
	flag.String("port", "", "mock port flag for TestHelperProcess")
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	select {}
}

type errReader struct{}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated read error")
}

type mockErrWriter struct {
	header http.Header
	err    error
}

func (m *mockErrWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *mockErrWriter) Write(p []byte) (int, error) {
	return 0, m.err
}

func (m *mockErrWriter) WriteHeader(statusCode int) {}

type mockErrorCloserReader struct {
	io.Reader
}

func (m *mockErrorCloserReader) Close() error {
	return errors.New("simulated closer error")
}

type mockRoundTripper struct {
	resp *http.Response
	err  error
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func setupMockDiscordUpstream(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Mock-Hijack") == "1" {
			w.Header().Set("Content-Length", "500")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				_ = conn.Close()
			}
			return
		}
		if r.Header.Get("X-Mock-Invalid-JSON") == "1" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{invalid-tools-list"))
			return
		}
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}

		body, _ := io.ReadAll(r.Body)
		var req JSONRPCRequest
		_ = json.Unmarshal(body, &req)

		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"discord_send"},{"name":"discord_read_messages"}]}}`))
		case "tools/call":
			if strings.Contains(string(body), "discord_search_messages") {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"total_results\":10,\"messages\":[[{\"id\":\"m1\",\"timestamp\":\"2026-10-25T00:00:00Z\",\"channel_id\":\"c1\",\"content\":\"hello\",\"hit\":true,\"author\":{\"id\":\"u1\",\"username\":\"devuser\",\"global_name\":\"Dev\"}}]],\"threads\":[{\"id\":\"t1\"}]}"}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"executed"}]}}`))
		default:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestProxyHandler_ServeHTTP(t *testing.T) {
	mockUpstream := setupMockDiscordUpstream(t)
	handler, err := NewProxyHandler(mockUpstream.URL, BlockedToolNames)
	if err != nil {
		t.Fatalf("failed to create proxy handler: %v", err)
	}

	offlineHandler, err := NewProxyHandler("http://127.0.0.1:59995", BlockedToolNames)
	if err != nil {
		t.Fatalf("failed to create offline handler: %v", err)
	}

	tests := []struct {
		name           string
		handler        *ProxyHandler
		method         string
		path           string
		body           io.Reader
		header         map[string]string
		useCloseErr    bool
		wantStatusCode int
		wantContains   string
		wantAbsent     string
	}{
		{"tools/list filters discord_send", handler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":100,"method":"tools/list"}`), nil, false, http.StatusOK, "discord_read_messages", "discord_send"},
		{"tools/call blocked for discord_send", handler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":200,"method":"tools/call","params":{"name":"discord_send","arguments":{"message":"test"}}}`), nil, false, http.StatusOK, "disabled", ""},
		{"tools/call allowed for discord_read_messages", handler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":300,"method":"tools/call","params":{"name":"discord_read_messages","arguments":{"channelId":"123"}}}`), nil, false, http.StatusOK, "executed", ""},
		{"tools/call shapes discord_search_messages", handler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":301,"method":"tools/call","params":{"name":"discord_search_messages","arguments":{"guildId":"123","offset":2}}}`), nil, false, http.StatusOK, "has_more", "threads"},
		{"GET /health reverse proxies to upstream", handler, http.MethodGet, "/health", nil, nil, false, http.StatusOK, "ok", ""},
		{"upstream returns invalid JSON for tools/list", handler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`), map[string]string{"X-Mock-Invalid-JSON": "1"}, false, http.StatusOK, "{invalid-tools-list", ""},
		{"offline upstream returns BadGateway", offlineHandler, http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`), nil, false, http.StatusBadGateway, "", ""},
		{"request body read error returns BadRequest", handler, http.MethodPost, "/mcp", &errReader{}, nil, false, http.StatusBadRequest, "", ""},
		{"upstream body read error returns BadGateway", handler, http.MethodPost, "/mcp", strings.NewReader(`{}`), map[string]string{"X-Mock-Hijack": "1"}, false, http.StatusBadGateway, "", ""},
		{"upstream body close error handled cleanly", handler, http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","method":"tools/list"}`), nil, true, http.StatusOK, "", ""},
		{"invalid HTTP method returns error", handler, "INVALID METHOD WITH SPACES", "/mcp", nil, nil, false, http.StatusBadRequest, "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.handler
			if tc.useCloseErr {
				hCopy := *h
				hCopy.httpClient.Transport = &mockRoundTripper{
					resp: &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       &mockErrorCloserReader{Reader: strings.NewReader(`{"jsonrpc":"2.0","result":{"tools":[]}}`)},
					},
				}
				h = &hCopy
			}

			req := httptest.NewRequest(http.MethodPost, tc.path, tc.body)
			req.Method = tc.method
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if tc.wantStatusCode == http.StatusBadRequest && tc.method == "INVALID METHOD WITH SPACES" {
				if rec.Code < 400 {
					t.Errorf("expected error status >= 400, got %d", rec.Code)
				}
				return
			}

			if rec.Code != tc.wantStatusCode {
				t.Errorf("status code got %d, want %d", rec.Code, tc.wantStatusCode)
			}
			bodyStr := rec.Body.String()
			if tc.wantContains != "" && !strings.Contains(bodyStr, tc.wantContains) {
				t.Errorf("expected body to contain %q, got: %s", tc.wantContains, bodyStr)
			}
			if tc.wantAbsent != "" && strings.Contains(bodyStr, tc.wantAbsent) {
				t.Errorf("expected body to NOT contain %q, got: %s", tc.wantAbsent, bodyStr)
			}
		})
	}
}

func TestNewProxyHandler_InvalidURL(t *testing.T) {
	if _, err := NewProxyHandler("http://invalid::url", BlockedToolNames); err == nil {
		t.Error("expected error for invalid URL in NewProxyHandler")
	}
}

func TestLoadConfigFile_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	fullYaml := filepath.Join(tmpDir, "full.yaml")
	_ = os.WriteFile(fullYaml, []byte("port: \"4001\"\nupstream_port: \"4025\"\nnode_bin: \"/custom/node\"\napp_path: \"/custom/app.js\"\n"), 0o644)
	minYaml := filepath.Join(tmpDir, "min.yaml")
	_ = os.WriteFile(minYaml, []byte("port: \"4001\"\nupstream_port: \"4025\"\n"), 0o644)
	badPortYaml := filepath.Join(tmpDir, "badport.yaml")
	_ = os.WriteFile(badPortYaml, []byte("upstream_port: \"4025\"\n"), 0o644)
	badUpYaml := filepath.Join(tmpDir, "badup.yaml")
	_ = os.WriteFile(badUpYaml, []byte("port: \"4001\"\n"), 0o644)

	tests := []struct {
		name         string
		path         string
		wantErr      bool
		wantPort     string
		wantUpstream string
		wantNodeBin  string
		wantAppPath  string
	}{
		{"full config", fullYaml, false, "4001", "4025", "/custom/node", "/custom/app.js"},
		{"minimal config defaults", minYaml, false, "4001", "4025", "node", "build/app.js"},
		{"missing file", filepath.Join(tmpDir, "nonexistent.yaml"), true, "", "", "", ""},
		{"missing port", badPortYaml, true, "", "", "", ""},
		{"missing upstream_port", badUpYaml, true, "", "", "", ""},
		{"empty path", "", true, "", "", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFile(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Port != tc.wantPort || cfg.UpstreamPort != tc.wantUpstream || cfg.NodeBin != tc.wantNodeBin || cfg.AppPath != tc.wantAppPath {
				t.Errorf("unexpected config: %+v", cfg)
			}
		})
	}
}

func TestPollUpstream_TableDriven(t *testing.T) {
	lOpen, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lOpen.Close()
	_, openPort, _ := net.SplitHostPort(lOpen.Addr().String())

	lClosed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	_, closedPort, _ := net.SplitHostPort(lClosed.Addr().String())
	_ = lClosed.Close()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name     string
		ctx      context.Context
		port     string
		attempts int
		delay    time.Duration
		want     bool
	}{
		{"open port succeeds", context.Background(), openPort, 5, 10 * time.Millisecond, true},
		{"closed port fails", context.Background(), closedPort, 2, 5 * time.Millisecond, false},
		{"canceled context fails", canceledCtx, openPort, 5, 10 * time.Millisecond, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := PollUpstream(tc.ctx, tc.port, tc.attempts, tc.delay); got != tc.want {
				t.Errorf("PollUpstream got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStartProxyServer_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		port        string
		upstreamURL string
		wantErr     bool
		checkHealth bool
	}{
		{"valid port and upstream", "0", "http://127.0.0.1:59992", false, true},
		{"invalid upstream URL", "59993", "http://invalid::url", true, false},
		{"invalid scheme", "8080", "://invalid-scheme\x7f", true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := StartProxyServer(tc.port, tc.upstreamURL)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.checkHealth {
				rec := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodGet, "/health", nil)
				srv.Handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "aerial-discord-mcp") {
					t.Errorf("unexpected health response: %d, body: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

func TestRunProxyApp_TableDriven(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	oldPoller := pollUpstreamFn
	pollUpstreamFn = func(ctx context.Context, port string, maxAttempts int, delay time.Duration) bool {
		return true
	}
	t.Cleanup(func() { pollUpstreamFn = oldPoller })

	tests := []struct {
		name        string
		cfg         *Config
		isLifecycle bool
		wantErrSub  string
	}{
		{"nil config error", nil, false, "config cannot be nil"},
		{"validation error on empty port", mkCfg("", "4005", os.Args[0], "-test.run=TestHelperProcess"), false, "port cannot be empty"},
		{"upstream process start failure", mkCfg("0", "59986", "/non/existent/bin", "-test.run=TestHelperProcess"), false, "failed to start upstream Node MCP server"},
		{"listen failure on invalid port", mkCfg("-1", "59985", os.Args[0], "-test.run=TestHelperProcess"), false, "invalid port"},
		{"start proxy server failure on invalid upstream", mkCfg("0", ":\x7f-invalid-port", os.Args[0], "-test.run=TestHelperProcess"), false, "failed to initialize proxy handler"},
		{"clean lifecycle with cancellation", mkCfg("0", "59988", os.Args[0], "-test.run=TestHelperProcess"), true, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.isLifecycle {
				err := RunProxyApp(context.Background(), tc.cfg)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Errorf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
				return
			}

			readyCh := make(chan struct{}, 1)
			oldReady := onServerReady
			onServerReady = func(addr string) {
				select {
				case readyCh <- struct{}{}:
				default:
				}
			}
			t.Cleanup(func() { onServerReady = oldReady })

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() {
				errCh <- RunProxyApp(ctx, tc.cfg)
			}()

			select {
			case <-readyCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for ready")
			}

			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Errorf("unexpected error on clean shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for shutdown")
			}
		})
	}
}

func TestWriteProxyResponse_TableDriven(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name      string
		ctx       context.Context
		writerErr error
		wantBody  string
	}{
		{"successful write", context.Background(), nil, "ok"},
		{"context canceled suppressed", canceledCtx, errors.New("network failure"), ""},
		{"epipe error suppressed", context.Background(), syscall.EPIPE, ""},
		{"generic write error handled", context.Background(), errors.New("generic"), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(tc.ctx)
			if tc.writerErr == nil {
				rec := httptest.NewRecorder()
				httpkit.WriteResponse(rec, req, "discord-mcp", []byte("ok"))
				if rec.Body.String() != tc.wantBody {
					t.Errorf("got %s, want %s", rec.Body.String(), tc.wantBody)
				}
			} else {
				w := &mockErrWriter{err: tc.writerErr}
				httpkit.WriteResponse(w, req, "discord-mcp", []byte("ignored"))
			}
		})
	}
}

func TestKillProcess_TableDriven(t *testing.T) {
	cmdRunning := exec.Command("sleep", "1")
	_ = cmdRunning.Start()
	t.Cleanup(func() {
		if cmdRunning.Process != nil {
			_ = cmdRunning.Process.Kill()
		}
	})

	cmdFinished := exec.Command("true")
	_ = cmdFinished.Run()

	tests := []struct {
		name string
		cmd  *exec.Cmd
	}{
		{"nil cmd", nil},
		{"cmd without process", exec.Command("true")},
		{"running process", cmdRunning},
		{"finished process", cmdFinished},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			killProcess(tc.cmd)
		})
	}
}

func TestIsClientDisconnect_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"ECONNRESET", syscall.ECONNRESET, true},
		{"net.ErrClosed", net.ErrClosed, true},
		{"generic error", errors.New("other"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := httpkit.IsClientDisconnect(nil, tc.err); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCoverageFlushEndpoint_TableDriven(t *testing.T) {
	srv, err := StartProxyServer("0", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatalf("failed to initialize proxy server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	tests := []struct {
		name       string
		metaErr    error
		counterErr error
		wantStatus int
	}{
		{"flush success", nil, nil, http.StatusOK},
		{"flush meta error ignored", errors.New("meta err"), nil, http.StatusOK},
		{"flush counter error returns 500", nil, errors.New("counter err"), http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldFn, oldMetaFn := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
			t.Cleanup(func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldFn, oldMetaFn })

			httpkit.WriteCountersDirFn = func(dir string) error { return tc.counterErr }
			httpkit.WriteMetaDirFn = func(dir string) error { return tc.metaErr }

			resp, err := http.Get(ts.URL + "/debug/coverage/flush")
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Errorf("status got %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}

func TestMain_Execution_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4001\"\nupstream_port: \"4025\"\n"), 0o644)

	tests := []struct {
		name     string
		runErr   error
		wantRun  bool
		wantExit bool
	}{
		{"success path", nil, true, false},
		{"server closed error", http.ErrServerClosed, true, false},
		{"fatal error calls exit", errors.New("fatal"), true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRun := runProxyApp
			oldExit := exitFn
			oldPath := configPath
			t.Cleanup(func() {
				runProxyApp = oldRun
				exitFn = oldExit
				configPath = oldPath
			})

			configPath = testCfg
			t.Setenv("CONFIG_PATH", testCfg)

			runCalled := false
			exitCalled := false
			runProxyApp = func(ctx context.Context, cfg *Config) error {
				runCalled = true
				return tc.runErr
			}
			exitFn = func(format string, v ...interface{}) {
				exitCalled = true
			}

			main()
			if runCalled != tc.wantRun {
				t.Errorf("runCalled got %v, want %v", runCalled, tc.wantRun)
			}
			if exitCalled != tc.wantExit {
				t.Errorf("exitCalled got %v, want %v", exitCalled, tc.wantExit)
			}
		})
	}
}
