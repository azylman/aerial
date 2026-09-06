package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
	if PollUpstream("59994", 2, 5*time.Millisecond) {
		t.Error("expected false for closed port in PollUpstream")
	}

	// PollUpstream success
	l, err := net.Listen("tcp", "127.0.0.1:59991")
	if err == nil {
		defer l.Close()
		if !PollUpstream("59991", 5, 10*time.Millisecond) {
			t.Error("expected true for open port in PollUpstream")
		}
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

	// 8. RunProxyApp with mock node process and cancellation
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunProxyApp(ctx, "59989", "59988", "sh", "-c")
	}()

	time.Sleep(50 * time.Millisecond)
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
	if err := RunProxyApp(context.Background(), "59987", "59986", "/non/existent/bin", ""); err == nil {
		t.Error("expected error running proxy app with non-existent binary")
	}
	if err := RunProxyApp(context.Background(), "-1", "59985", "sh", "-c"); err == nil {
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

	// 11. RunProxyApp with empty string defaults
	ctxImm, cancelImm := context.WithCancel(context.Background())
	cancelImm()
	_ = RunProxyApp(ctxImm, "", "", "sh", "-c")

	// 12. ServeHTTP with body read error
	errReq, _ := http.NewRequest(http.MethodPost, "/mcp", &errReader{})
	errRec := httptest.NewRecorder()
	handler.ServeHTTP(errRec, errReq)
	if errRec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for body read error, got %d", errRec.Code)
	}
}

type errReader struct{}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated read error")
}



