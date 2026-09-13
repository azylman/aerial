package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
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

func TestFilterToolsResponse(t *testing.T) {
	mockResponse := `{
		"jsonrpc": "2.0",
		"id": 1,
		"result": {
			"tools": [
				{
					"name": "discord_send",
					"description": "Send a message to a Discord channel",
					"inputSchema": {"type": "object"}
				},
				{
					"name": "discord_read_messages",
					"description": "Read messages from a Discord channel",
					"inputSchema": {"type": "object"}
				},
				{
					"name": "discord_search_messages",
					"description": "Search messages in a server",
					"inputSchema": {"type": "object"}
				},
				{
					"name": "send_message",
					"description": "Another send message tool",
					"inputSchema": {"type": "object"}
				},
				{
					"name": "discord_get_server_info",
					"description": "Get server info",
					"inputSchema": {"type": "object"}
				}
			]
		}
	}`

	filteredBytes, err := FilterToolsResponse([]byte(mockResponse), BlockedToolNames)
	if err != nil {
		t.Fatalf("unexpected error filtering tools response: %v", err)
	}

	var resp JSONRPCResponse
	if err := json.Unmarshal(filteredBytes, &resp); err != nil {
		t.Fatalf("failed to unmarshal filtered response: %v", err)
	}

	var listResult ToolsListResult
	if err := json.Unmarshal(resp.Result, &listResult); err != nil {
		t.Fatalf("failed to unmarshal tools result: %v", err)
	}

	for _, tool := range listResult.Tools {
		if BlockedToolNames[tool.Name] {
			t.Errorf("expected tool %q to be filtered out, but found in result", tool.Name)
		}
	}

	expectedCount := 3 // read_messages, search_messages, get_server_info
	if len(listResult.Tools) != expectedCount {
		t.Errorf("expected %d tools, got %d", expectedCount, len(listResult.Tools))
	}
}

func TestIsBlockedToolCall(t *testing.T) {
	t.Run("Blocked discord_send", func(t *testing.T) {
		req := `{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"discord_send","arguments":{"message":"hello"}}}`
		isBlocked, id, name, err := IsBlockedToolCall([]byte(req), BlockedToolNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !isBlocked {
			t.Errorf("expected isBlocked=true for discord_send, got false")
		}
		if name != "discord_send" {
			t.Errorf("expected name=discord_send, got %q", name)
		}
		if id != float64(42) {
			t.Errorf("expected id=42, got %v", id)
		}
	})

	t.Run("Allowed discord_read_messages", func(t *testing.T) {
		req := `{"jsonrpc":"2.0","id":"abc-123","method":"tools/call","params":{"name":"discord_read_messages","arguments":{"channelId":"123"}}}`
		isBlocked, id, name, err := IsBlockedToolCall([]byte(req), BlockedToolNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if isBlocked {
			t.Errorf("expected isBlocked=false for discord_read_messages, got true")
		}
		if name != "discord_read_messages" {
			t.Errorf("expected name=discord_read_messages, got %q", name)
		}
		if id != "abc-123" {
			t.Errorf("expected id='abc-123', got %v", id)
		}
	})

	t.Run("Non-tool call method", func(t *testing.T) {
		req := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`
		isBlocked, _, _, err := IsBlockedToolCall([]byte(req), BlockedToolNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if isBlocked {
			t.Errorf("expected isBlocked=false for initialize, got true")
		}
	})
}

func TestProxyHandlerEndToEnd(t *testing.T) {
	// Mock upstream server
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req JSONRPCRequest
		_ = json.Unmarshal(body, &req)

		switch req.Method {
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "discord_send", "description": "send"},
						{"name": "discord_read_messages", "description": "read"}
					]
				}
			}`))
		case "tools/call":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {"content": [{"type": "text", "text": "executed"}]}
			}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc": "2.0", "id": 1, "result": "ok"}`))
		}
	}))
	defer mockUpstream.Close()

	proxyHandler, err := NewProxyHandler(mockUpstream.URL, BlockedToolNames)
	if err != nil {
		t.Fatalf("failed to create proxy handler: %v", err)
	}

	proxyServer := httptest.NewServer(proxyHandler)
	defer proxyServer.Close()

	t.Run("tools/list filters out discord_send", func(t *testing.T) {
		reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/list"}`
		resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(respBytes), "discord_send") {
			t.Errorf("response should not contain discord_send, got: %s", string(respBytes))
		}
		if !strings.Contains(string(respBytes), "discord_read_messages") {
			t.Errorf("response should contain discord_read_messages, got: %s", string(respBytes))
		}
	})

	t.Run("tools/call blocked for discord_send", func(t *testing.T) {
		reqBody := `{"jsonrpc":"2.0","id":200,"method":"tools/call","params":{"name":"discord_send","arguments":{"message":"test"}}}`
		resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(resp.Body)
		var jsonResp JSONRPCResponse
		if err := json.Unmarshal(respBytes, &jsonResp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if jsonResp.Error == nil {
			t.Fatalf("expected JSON-RPC error, got nil: %s", string(respBytes))
		}
		if !strings.Contains(jsonResp.Error.Message, "disabled") {
			t.Errorf("expected disabled error message, got: %s", jsonResp.Error.Message)
		}
	})

	t.Run("tools/call allowed for discord_read_messages", func(t *testing.T) {
		reqBody := `{"jsonrpc":"2.0","id":300,"method":"tools/call","params":{"name":"discord_read_messages","arguments":{"channelId":"123"}}}`
		resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(resp.Body)
		var jsonResp JSONRPCResponse
		if err := json.Unmarshal(respBytes, &jsonResp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if jsonResp.Error != nil {
			t.Fatalf("unexpected error: %v", jsonResp.Error)
		}
	})

	t.Run("GET request proxies via reverseProxy", func(t *testing.T) {
		resp, err := http.Get(proxyServer.URL + "/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 from proxy health check: %v, err: %v", resp, err)
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
	})
}

func TestProxy_EdgeCasesAndErrors(t *testing.T) {
	// 1. IsBlockedToolCall invalid JSON
	if _, _, _, err := IsBlockedToolCall([]byte("{bad json"), BlockedToolNames); err == nil {
		t.Error("expected error for malformed json in IsBlockedToolCall")
	}

	// 2. IsBlockedToolCall params malformed
	if _, _, _, err := IsBlockedToolCall([]byte(`{"jsonrpc":"2.0","method":"tools/call","params":"not-an-object"}`), BlockedToolNames); err == nil {
		t.Error("expected error for malformed params in IsBlockedToolCall")
	}

	// 3. FilterToolsResponse edge cases
	if _, err := FilterToolsResponse([]byte("{bad json"), BlockedToolNames); err == nil {
		t.Error("expected error for bad json in FilterToolsResponse")
	}
	rawResp, err := FilterToolsResponse([]byte(`{"jsonrpc":"2.0","id":1}`), BlockedToolNames)
	if err != nil || !strings.Contains(string(rawResp), `"jsonrpc":"2.0"`) {
		t.Errorf("expected raw response for empty result, got %s, err: %v", string(rawResp), err)
	}
	if _, err := FilterToolsResponse([]byte(`{"jsonrpc":"2.0","result":"not-tools-object"}`), BlockedToolNames); err == nil {
		t.Error("expected error for invalid tools list result format")
	}

	// 4. NewProxyHandler invalid URL
	if _, err := NewProxyHandler("http://invalid::url", BlockedToolNames); err == nil {
		t.Error("expected error for invalid URL in NewProxyHandler")
	}

	// 5. Upstream offline returns BadGateway
	handler, err := NewProxyHandler("http://127.0.0.1:59995", BlockedToolNames)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	offlineSrv := httptest.NewServer(handler)
	defer offlineSrv.Close()

	resp, err := http.Post(offlineSrv.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502 Bad Gateway from offline upstream, got %d", resp.StatusCode)
	}

	// 6. PollUpstream
	if PollUpstream(context.Background(), "59994", 2, 5*time.Millisecond) {
		t.Error("expected false for closed port in PollUpstream")
	}

	// PollUpstream success
	l, err := net.Listen("tcp", "127.0.0.1:59991")
	if err == nil {
		defer l.Close()
		if !PollUpstream(context.Background(), "59991", 5, 10*time.Millisecond) {
			t.Error("expected true for open port in PollUpstream")
		}
	}

	// PollUpstream canceled context
	canceledCtx, cancelPoll := context.WithCancel(context.Background())
	cancelPoll()
	if PollUpstream(canceledCtx, "59991", 5, 10*time.Millisecond) {
		t.Error("expected false when context is canceled in PollUpstream")
	}

	// 7. StartProxyServer with valid and invalid URL + probe /health
	srv, err := StartProxyServer("59993", "http://127.0.0.1:59992")
	if err != nil {
		t.Fatalf("failed to start proxy server: %v", err)
	}
	healthRecorder := httptest.NewRecorder()
	healthReq, _ := http.NewRequest(http.MethodGet, "/health", nil)
	srv.Handler.ServeHTTP(healthRecorder, healthReq)
	if healthRecorder.Code != http.StatusOK || !strings.Contains(healthRecorder.Body.String(), "aerial-discord-mcp") {
		t.Errorf("unexpected health response: %v, body: %s", healthRecorder.Code, healthRecorder.Body.String())
	}

	if _, err := StartProxyServer("59993", "http://invalid::url"); err == nil {
		t.Error("expected error starting proxy server with invalid url")
	}

	// 8. RunProxyApp with mock process and cancellation
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	oldPoller := pollUpstreamFn
	pollUpstreamFn = func(ctx context.Context, port string, maxAttempts int, delay time.Duration) bool {
		return true
	}
	defer func() { pollUpstreamFn = oldPoller }()

	readyCh := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		select {
		case <-readyCh:
		default:
			close(readyCh)
		}
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunProxyApp(ctx, &Config{
			Port:         "0",
			UpstreamPort: "59988",
			NodeBin:      os.Args[0],
			AppPath:      "-test.run=TestHelperProcess",
		})
	}()

	select {
	case <-readyCh:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for RunProxyApp to become ready")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from RunProxyApp shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunProxyApp shutdown")
	}

	// 9. RunProxyApp startup error (invalid binary and invalid port)
	if err := RunProxyApp(context.Background(), &Config{
		Port:         "59987",
		UpstreamPort: "59986",
		NodeBin:      "/non/existent/bin",
		AppPath:      "-test.run=TestHelperProcess",
	}); err == nil || !strings.Contains(err.Error(), "failed to start upstream Node MCP server") {
		t.Errorf("expected 'failed to start upstream Node MCP server', got %v", err)
	}
	if err := RunProxyApp(context.Background(), &Config{
		Port:         "-1",
		UpstreamPort: "59985",
		NodeBin:      os.Args[0],
		AppPath:      "-test.run=TestHelperProcess",
	}); err == nil {
		t.Error("expected error running proxy app with invalid port")
	}

	// 10. Upstream returns invalid JSON for tools/list
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-tools-list"))
	}))
	defer badUpstream.Close()

	badHandler, _ := NewProxyHandler(badUpstream.URL, BlockedToolNames)
	badSrv := httptest.NewServer(badHandler)
	defer badSrv.Close()

	badListResp, err := http.Post(badSrv.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	defer badListResp.Body.Close()
	if badListResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for forwarded raw response, got %d", badListResp.StatusCode)
	}

	// 11. RunProxyApp nil config error
	if err := RunProxyApp(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "discord-mcp: config cannot be nil") {
		t.Errorf("expected nil config error, got %v", err)
	}

	// 12. ServeHTTP with body read error
	errReq, _ := http.NewRequest(http.MethodPost, "/mcp", &errReader{})
	errRec := httptest.NewRecorder()
	handler.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for body read error, got %d", errRec.Code)
	}
}

func TestRunProxyApp_ValidationErrors(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *Config
		expectedErr string
	}{
		{
			name:        "nil config",
			cfg:         nil,
			expectedErr: "discord-mcp: config cannot be nil",
		},
		{
			name:        "empty struct",
			cfg:         &Config{},
			expectedErr: "discord-mcp: port cannot be empty",
		},
		{
			name: "empty port",
			cfg: &Config{
				Port:         "",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: port cannot be empty",
		},
		{
			name: "whitespace port",
			cfg: &Config{
				Port:         "   ",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: port cannot be empty",
		},
		{
			name: "empty upstream port",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: upstreamPort cannot be empty",
		},
		{
			name: "whitespace upstream port",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "   ",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: upstreamPort cannot be empty",
		},
		{
			name: "empty node bin",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: nodeBin cannot be empty",
		},
		{
			name: "whitespace node bin",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "   ",
				AppPath:      "build/app.js",
			},
			expectedErr: "discord-mcp: nodeBin cannot be empty",
		},
		{
			name: "empty app path",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "",
			},
			expectedErr: "discord-mcp: appPath cannot be empty",
		},
		{
			name: "whitespace app path",
			cfg: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "   ",
			},
			expectedErr: "discord-mcp: appPath cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := RunProxyApp(context.Background(), tc.cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.expectedErr)
			}
			if !strings.Contains(err.Error(), tc.expectedErr) {
				t.Errorf("expected error containing %q, got %q", tc.expectedErr, err.Error())
			}
		})
	}
}

func TestNewConfigFromLookup_TableDriven(t *testing.T) {
	testCases := []struct {
		name     string
		lookup   func(string) string
		expected *Config
	}{
		{
			name:     "nil lookup uses defaults",
			lookup:   nil,
			expected: DefaultConfig(),
		},
		{
			name: "empty lookup uses defaults",
			lookup: func(k string) string {
				return ""
			},
			expected: DefaultConfig(),
		},
		{
			name: "whitespace-only lookup values use defaults",
			lookup: func(k string) string {
				return "   "
			},
			expected: DefaultConfig(),
		},
		{
			name: "override port only with trimming",
			lookup: func(k string) string {
				if k == "PORT" {
					return "  5001  "
				}
				return ""
			},
			expected: &Config{
				Port:         "5001",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
		},
		{
			name: "override upstream port only",
			lookup: func(k string) string {
				if k == "UPSTREAM_PORT" {
					return "5005"
				}
				return ""
			},
			expected: &Config{
				Port:         "4001",
				UpstreamPort: "5005",
				NodeBin:      "node",
				AppPath:      "build/app.js",
			},
		},
		{
			name: "override node bin only",
			lookup: func(k string) string {
				if k == "NODE_BIN" {
					return "/usr/local/bin/node"
				}
				return ""
			},
			expected: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "/usr/local/bin/node",
				AppPath:      "build/app.js",
			},
		},
		{
			name: "override app path only",
			lookup: func(k string) string {
				if k == "APP_PATH" {
					return "dist/server.js"
				}
				return ""
			},
			expected: &Config{
				Port:         "4001",
				UpstreamPort: "4005",
				NodeBin:      "node",
				AppPath:      "dist/server.js",
			},
		},
		{
			name: "override all fields with trimming",
			lookup: func(k string) string {
				switch k {
				case "PORT":
					return " 9001 "
				case "UPSTREAM_PORT":
					return " 9005 "
				case "NODE_BIN":
					return " /opt/node/bin/node "
				case "APP_PATH":
					return " /opt/app/index.js "
				default:
					return ""
				}
			},
			expected: &Config{
				Port:         "9001",
				UpstreamPort: "9005",
				NodeBin:      "/opt/node/bin/node",
				AppPath:      "/opt/app/index.js",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfigFromLookup(tc.lookup)
			if cfg == nil {
				t.Fatal("expected non-nil config")
			}
			if cfg.Port != tc.expected.Port {
				t.Errorf("expected Port %q, got %q", tc.expected.Port, cfg.Port)
			}
			if cfg.UpstreamPort != tc.expected.UpstreamPort {
				t.Errorf("expected UpstreamPort %q, got %q", tc.expected.UpstreamPort, cfg.UpstreamPort)
			}
			if cfg.NodeBin != tc.expected.NodeBin {
				t.Errorf("expected NodeBin %q, got %q", tc.expected.NodeBin, cfg.NodeBin)
			}
			if cfg.AppPath != tc.expected.AppPath {
				t.Errorf("expected AppPath %q, got %q", tc.expected.AppPath, cfg.AppPath)
			}
		})
	}
}

type errReader struct{}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated read error")
}

func TestServeHTTP_ToolsListFilterError(t *testing.T) {
	// Upstream returns 200 OK with malformed JSON for tools/list
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer mockUpstream.Close()

	handler, err := NewProxyHandler(mockUpstream.URL, BlockedToolNames)
	if err != nil {
		t.Fatalf("NewProxyHandler failed: %v", err)
	}

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK even when filter fails, got %d", rec.Code)
	}
	if rec.Body.String() != `{not valid json` {
		t.Errorf("expected original body when filter fails, got %q", rec.Body.String())
	}
}

func TestStartProxyServer_InvalidUpstream(t *testing.T) {
	_, err := StartProxyServer("8080", "://invalid-scheme\x7f")
	if err == nil {
		t.Error("expected error for invalid upstream URL")
	}
}

func TestFilterToolsResponse_InvalidJSON(t *testing.T) {
	raw := []byte(`{invalid-json`)
	filtered, err := FilterToolsResponse(raw, BlockedToolNames)
	if err == nil {
		t.Error("expected error for invalid JSON in FilterToolsResponse")
	}
	if string(filtered) != string(raw) {
		t.Errorf("expected original bytes returned on error, got %s", string(filtered))
	}
}

func TestServeHTTP_InvalidMethod(t *testing.T) {
	handler, err := NewProxyHandler("http://127.0.0.1:4005", BlockedToolNames)
	if err != nil {
		t.Fatalf("NewProxyHandler failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/mcp", nil)
	req.Method = "INVALID METHOD WITH SPACES"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code < 500 {
		t.Errorf("expected 5xx error on invalid method, got %d", rec.Code)
	}
}

func TestRunProxyApp_StartProxyServerFailure(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	oldPoller := pollUpstreamFn
	pollUpstreamFn = func(ctx context.Context, port string, maxAttempts int, delay time.Duration) bool {
		return true
	}
	defer func() { pollUpstreamFn = oldPoller }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := &Config{
		Port:         "0",
		UpstreamPort: ":\x7f-invalid-port",
		NodeBin:      os.Args[0],
		AppPath:      "-test.run=TestHelperProcess",
	}

	err := RunProxyApp(ctx, cfg)
	if err == nil {
		t.Error("expected error when StartProxyServer fails due to invalid upstream URL")
	}
}

func TestServeHTTP_UpstreamBodyReadError(t *testing.T) {
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "500")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
		}
	}))
	defer mockUpstream.Close()

	handler, err := NewProxyHandler(mockUpstream.URL, BlockedToolNames)
	if err != nil {
		t.Fatalf("NewProxyHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502 on read error, got %d", rec.Code)
	}
}

func TestMain_Execution(t *testing.T) {
	oldRun := runProxyApp
	oldExit := exitFn
	defer func() {
		runProxyApp = oldRun
		exitFn = oldExit
	}()

	// 1. Success path
	runCalled := false
	runProxyApp = func(ctx context.Context, cfg *Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runProxyApp to be called")
	}

	// 2. Server closed error (should not exit)
	exitCalled := false
	exitFn = func(format string, v ...interface{}) {
		exitCalled = true
	}
	runProxyApp = func(ctx context.Context, cfg *Config) error {
		return http.ErrServerClosed
	}
	main()
	if exitCalled {
		t.Error("expected exitFn not to be called for ErrServerClosed")
	}

	// 3. Error path (calls exitFn)
	runProxyApp = func(ctx context.Context, cfg *Config) error {
		return fmt.Errorf("simulated fatal startup error")
	}
	main()
	if !exitCalled {
		t.Error("expected exitFn to be called on fatal error")
	}
}

func TestRunProxyApp_ListenFailure(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	oldPoller := pollUpstreamFn
	pollUpstreamFn = func(ctx context.Context, port string, maxAttempts int, delay time.Duration) bool {
		return true
	}
	defer func() { pollUpstreamFn = oldPoller }()

	cfg := &Config{
		Port:         "-1",
		UpstreamPort: "59985",
		NodeBin:      os.Args[0],
		AppPath:      "-test.run=TestHelperProcess",
	}

	err := RunProxyApp(context.Background(), cfg)
	if err == nil {
		t.Error("expected error from invalid port")
	}
}



