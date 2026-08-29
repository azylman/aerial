package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
}