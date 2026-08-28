package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func setupTestServer(t *testing.T) (*Server, *httptest.Server) {
	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	handler := NewToolHandler(db)
	server := NewServer(handler)
	ts := httptest.NewServer(server.Routes())
	return server, ts
}

func TestHealthEndpoint(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Failed to decode health body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("Expected status ok, got %v", body)
	}
}

func TestMCPInitialize(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /mcp initialize failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var jsonResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		t.Fatalf("Failed to decode JSON-RPC response: %v", err)
	}
	if jsonResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", jsonResp.Error)
	}

	resMap, ok := jsonResp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result map, got %+v", jsonResp.Result)
	}
	if resMap["protocolVersion"] != "2024-11-05" {
		t.Errorf("Expected protocolVersion 2024-11-05, got %v", resMap["protocolVersion"])
	}
}

func TestMCPToolsList(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	reqBody := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /mcp tools/list failed: %v", err)
	}
	defer resp.Body.Close()

	var jsonResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		t.Fatalf("Failed to decode JSON-RPC response: %v", err)
	}

	resMap := jsonResp.Result.(map[string]interface{})
	tools := resMap["tools"].([]interface{})
	if len(tools) != 4 {
		t.Fatalf("Expected 4 tools, got %d", len(tools))
	}
}

func TestMCPToolsCallLifecycle(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	// 1. Call schedule_recurring via namespaced tool name
	recReq := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"scheduler_schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"weekly plan"}}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(recReq))
	if err != nil {
		t.Fatalf("Call schedule_recurring failed: %v", err)
	}
	defer resp.Body.Close()

	var recResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&recResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if recResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", recResp.Error)
	}
	resMap := recResp.Result.(map[string]interface{})
	content := resMap["content"].([]interface{})
	if len(content) == 0 {
		t.Fatalf("Expected content in response")
	}

	// 2. Call schedule_once via direct tool name
	onceReq := `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"take out trash"}}}`
	resp2, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(onceReq))
	if err != nil {
		t.Fatalf("Call schedule_once failed: %v", err)
	}
	defer resp2.Body.Close()

	var onceResp JSONRPCResponse
	if err := json.NewDecoder(resp2.Body).Decode(&onceResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if onceResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", onceResp.Error)
	}

	// 3. Call list_schedules
	listReq := `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`
	resp3, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(listReq))
	if err != nil {
		t.Fatalf("Call list_schedules failed: %v", err)
	}
	defer resp3.Body.Close()

	var listResp JSONRPCResponse
	if err := json.NewDecoder(resp3.Body).Decode(&listResp); err != nil {
		t.Fatalf("Failed to decode list response: %v", err)
	}
	if listResp.Error != nil {
		t.Fatalf("Unexpected RPC error on list: %+v", listResp.Error)
	}

	// 4. Call unknown tool
	unknownReq := `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"non_existent_tool","arguments":{}}}`
	resp4, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(unknownReq))
	if err != nil {
		t.Fatalf("Call unknown tool failed: %v", err)
	}
	defer resp4.Body.Close()

	var unknownResp JSONRPCResponse
	_ = json.NewDecoder(resp4.Body).Decode(&unknownResp)
	if unknownResp.Error == nil || unknownResp.Error.Code != -32601 {
		t.Errorf("Expected -32601 error for unknown tool, got %+v", unknownResp)
	}
}

func TestMCPUnknownMethodAndNotifications(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	// Notification (no id)
	notifReq := `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(notifReq))
	if err != nil {
		t.Fatalf("Notification request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 204 or 200 for notification, got %d", resp.StatusCode)
	}

	// Unknown method with id
	unknownReq := `{"jsonrpc":"2.0","id":99,"method":"random/unknown"}`
	resp2, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(unknownReq))
	if err != nil {
		t.Fatalf("Unknown method failed: %v", err)
	}
	defer resp2.Body.Close()

	var unknownResp JSONRPCResponse
	_ = json.NewDecoder(resp2.Body).Decode(&unknownResp)
	if unknownResp.Error == nil || unknownResp.Error.Code != -32601 {
		t.Errorf("Expected method not found error, got %+v", unknownResp)
	}
}
