package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMockMCPLifecycle(t *testing.T) {
	var notifyReceived atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-lifecycle-123")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"protocolVersion": "2024-11-05",
					"capabilities":    map[string]interface{}{},
					"serverInfo":      map[string]interface{}{"name": "test-srv", "version": "1.0"},
				},
			})
		case "notifications/initialized":
			if r.Header.Get("mcp-session-id") != "sess-lifecycle-123" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			notifyReceived.Store(true)
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			if r.Header.Get("mcp-session-id") != "sess-lifecycle-123" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"tools": []Tool{
						{Name: "tool1", Description: "First tool"},
						{Name: "tool2", Description: "Second tool"},
					},
				},
			})
		case "tools/call":
			if r.Header.Get("mcp-session-id") != "sess-lifecycle-123" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"content": []map[string]interface{}{
						{"type": "text", "text": "hello from tool1"},
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "srv1", ServerURL: ts.URL},
	}, nil)

	ctx := context.Background()

	// 1. EnsureSession
	sessID, err := d.EnsureSession(ctx, "srv1")
	if err != nil {
		t.Fatalf("unexpected EnsureSession error: %v", err)
	}
	if sessID != "sess-lifecycle-123" {
		t.Fatalf("expected sess-lifecycle-123, got %q", sessID)
	}
	if !notifyReceived.Load() {
		t.Fatalf("expected notifications/initialized to be received")
	}

	// 2. ListTools
	tools, err := d.ListTools(ctx)
	if err != nil {
		t.Fatalf("unexpected ListTools error: %v", err)
	}
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(tools))
	}
	if tools[0].Name != "tool1" || tools[1].Name != "tool2" {
		t.Fatalf("unexpected tools returned: %+v", tools)
	}

	// 3. Execute
	res, err := d.Execute(ctx, "tool1", map[string]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected Execute error: %v", err)
	}
	if res != "hello from tool1" {
		t.Fatalf("expected 'hello from tool1', got %q", res)
	}
}

func TestExecute_CallsEnsureSessionUpfront_No400(t *testing.T) {
	var callReceived atomic.Bool
	var had400 atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-upfront-ok")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/call":
			// If session ID header is missing, return 400 Bad Request
			if r.Header.Get("mcp-session-id") != "sess-upfront-ok" {
				had400.Store(true)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			callReceived.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"content": []map[string]interface{}{
						{"type": "text", "text": "upfront session success"},
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	srv := ServerConfig{Name: "strict-srv", ServerURL: ts.URL}
	d := NewDispatcher([]ServerConfig{srv}, nil)

	// Pre-seed route so ListTools is bypassed, testing Execute's upfront session check
	d.mu.Lock()
	d.toolRoutes["strict_tool"] = srv
	d.mu.Unlock()

	// Ensure sessionIDs map is currently empty
	if len(d.sessionIDs) != 0 {
		t.Fatalf("expected empty sessionIDs before execute")
	}

	ctx := context.Background()
	res, err := d.Execute(ctx, "strict_tool", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected Execute error: %v", err)
	}

	if had400.Load() {
		t.Fatalf("server received tools/call without session ID (400 Bad Request occurred)")
	}
	if !callReceived.Load() {
		t.Fatalf("expected tools/call to be executed successfully")
	}
	if res != "upfront session success" {
		t.Fatalf("expected 'upfront session success', got %q", res)
	}
}

func TestExecute_SessionRetryOn400OrMissingSessionID(t *testing.T) {
	var initCalls atomic.Int32
	var callCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			calls := initCalls.Add(1)
			sessID := fmt.Sprintf("sess-epoch-%d", calls+1)
			w.Header().Set("mcp-session-id", sessID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/call":
			calls := callCount.Add(1)
			// First call rejects with 400 Bad Request / missing session id
			if calls == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "session expired: missing session id"}`))
				return
			}
			// Second call (retry) with new session succeeds
			if r.Header.Get("mcp-session-id") != "sess-epoch-2" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "unexpected session id"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"content": []map[string]interface{}{
						{"type": "text", "text": "recovered after retry"},
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	srv := ServerConfig{Name: "retry-srv", ServerURL: ts.URL}
	d := NewDispatcher([]ServerConfig{srv}, nil)

	// Pre-seed route and an old session ID
	d.mu.Lock()
	d.toolRoutes["retry_tool"] = srv
	d.sessionIDs[srv.Name] = "sess-epoch-1"
	d.mu.Unlock()

	ctx := context.Background()
	res, err := d.Execute(ctx, "retry_tool", nil)
	if err != nil {
		t.Fatalf("unexpected Execute error on retry: %v", err)
	}

	if res != "recovered after retry" {
		t.Fatalf("expected 'recovered after retry', got %q", res)
	}
	if initCalls.Load() != 1 { // one new initialize during retry
		t.Fatalf("expected 1 initialize call during retry, got %d", initCalls.Load())
	}
	if callCount.Load() != 2 {
		t.Fatalf("expected 2 tools/call attempts, got %d", callCount.Load())
	}

	d.mu.RLock()
	finalSess := d.sessionIDs[srv.Name]
	d.mu.RUnlock()
	if finalSess != "sess-epoch-2" {
		t.Fatalf("expected final session ID to be sess-epoch-2, got %q", finalSess)
	}
}

func TestExecute_SessionRetryOnJSONRPCError32600(t *testing.T) {
	var callCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-fresh-32600")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/call":
			calls := callCount.Add(1)
			if calls == 1 {
				// Return 200 OK with JSON-RPC error code -32600
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"error": map[string]interface{}{
						"code":    -32600,
						"message": "Invalid request: missing session id",
					},
				})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]interface{}{
					"content": []map[string]interface{}{
						{"type": "text", "text": "recovered from -32600"},
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	srv := ServerConfig{Name: "rpc-error-srv", ServerURL: ts.URL}
	d := NewDispatcher([]ServerConfig{srv}, nil)

	d.mu.Lock()
	d.toolRoutes["err_tool"] = srv
	d.sessionIDs[srv.Name] = "sess-old"
	d.mu.Unlock()

	ctx := context.Background()
	res, err := d.Execute(ctx, "err_tool", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "recovered from -32600" {
		t.Fatalf("expected 'recovered from -32600', got %q", res)
	}
}

func TestPrime_PopulatesSessionIDsAndToolRoutes(t *testing.T) {
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-prime-1")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "tool_alpha", Description: "Alpha tool"}},
				},
			})
		}
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-prime-2")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "tool_beta", Description: "Beta tool"}},
				},
			})
		}
	}))
	defer ts2.Close()

	// Third server that fails to ensure Prime is best-effort and logs warnings without aborting
	tsFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer tsFail.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "srv1", ServerURL: ts1.URL},
		{Name: "srv2", ServerURL: ts2.URL},
		{Name: "srvFail", ServerURL: tsFail.URL},
	}, nil)

	ctx := context.Background()
	err := d.Prime(ctx)
	if err != nil {
		t.Fatalf("expected Prime to succeed (best-effort), got: %v", err)
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.sessionIDs["srv1"] != "sess-prime-1" {
		t.Errorf("expected sess-prime-1 for srv1, got %q", d.sessionIDs["srv1"])
	}
	if d.sessionIDs["srv2"] != "sess-prime-2" {
		t.Errorf("expected sess-prime-2 for srv2, got %q", d.sessionIDs["srv2"])
	}
	if _, ok := d.toolRoutes["tool_alpha"]; !ok {
		t.Errorf("expected tool_alpha in toolRoutes")
	}
	if _, ok := d.toolRoutes["tool_beta"]; !ok {
		t.Errorf("expected tool_beta in toolRoutes")
	}
}

func TestAllowedToolsFiltering(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{
						{Name: "allow_1"},
						{Name: "block_2"},
						{Name: "allow_3"},
					},
				},
			})
		case "tools/call":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"content": []map[string]interface{}{{"type": "text", "text": "allowed call ok"}},
				},
			})
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "filter-srv", ServerURL: ts.URL},
	}, nil, []string{"allow_1", "allow_3"})

	ctx := context.Background()

	// ListTools should only include allowed tools
	tools, err := d.ListTools(ctx)
	if err != nil {
		t.Fatalf("unexpected ListTools error: %v", err)
	}
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(tools))
	}
	for _, tool := range tools {
		if tool.Name == "block_2" {
			t.Fatalf("block_2 should not be returned by ListTools")
		}
	}

	// Executing blocked tool should fail immediately
	_, err = d.Execute(ctx, "block_2", nil)
	if err == nil || !strings.Contains(err.Error(), "not in the allowed tools list") {
		t.Fatalf("expected allowed tools list error, got %v", err)
	}

	// Executing allowed tool should succeed
	res, err := d.Execute(ctx, "allow_1", nil)
	if err != nil {
		t.Fatalf("unexpected Execute error: %v", err)
	}
	if res != "allowed call ok" {
		t.Fatalf("expected 'allowed call ok', got %q", res)
	}
}

func TestConcurrentExecute(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("mcp-session-id", "sess-concurrent")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "concur_tool"}},
				},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]interface{})
			args, _ := params["arguments"].(map[string]interface{})
			idx := args["idx"]
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"content": []map[string]interface{}{{"type": "text", "text": fmt.Sprintf("result-%v", idx)}},
				},
			})
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "concurrent-srv", ServerURL: ts.URL},
	}, nil)

	ctx := context.Background()
	if err := d.Prime(ctx); err != nil {
		t.Fatalf("unexpected Prime error: %v", err)
	}

	var wg sync.WaitGroup
	workers := 25
	errChan := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := d.Execute(ctx, "concur_tool", map[string]interface{}{"idx": idx})
			if err != nil {
				errChan <- fmt.Errorf("worker %d error: %w", idx, err)
				return
			}
			expected := fmt.Sprintf("result-%d", idx)
			if res != expected {
				errChan <- fmt.Errorf("worker %d got %q, expected %q", idx, res, expected)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Fatal(err)
	}
}

func TestExecute_UserGoogleEmailNormalization(t *testing.T) {
	var receivedArgs map[string]interface{}
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "email_tool"}},
				},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]interface{})
			args, _ := params["arguments"].(map[string]interface{})
			mu.Lock()
			receivedArgs = args
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": "ok",
			})
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "email-srv", ServerURL: ts.URL},
	}, nil)

	ctx := context.Background()

	testCases := []struct {
		inputEmail string
		wantEmail  string
		wantExist  bool
	}{
		{"me", "", false},
		{"PRIMARY", "", false},
		{"default", "", false},
		{"", "", false},
		{"alex@domain.com", "alex@domain.com", true},
	}

	for _, tc := range testCases {
		origMap := map[string]interface{}{
			"user_google_email": tc.inputEmail,
			"query":             "search term",
		}
		_, err := d.Execute(ctx, "email_tool", origMap)
		if err != nil {
			t.Fatalf("unexpected Execute error: %v", err)
		}

		// Ensure original caller map was not mutated
		if origMap["user_google_email"] != tc.inputEmail {
			t.Errorf("original map was mutated for email %q", tc.inputEmail)
		}

		mu.Lock()
		gotVal, exists := receivedArgs["user_google_email"]
		mu.Unlock()

		if exists != tc.wantExist {
			t.Errorf("input %q: got exists=%v, want %v", tc.inputEmail, exists, tc.wantExist)
		}
		if tc.wantExist && gotVal != tc.wantEmail {
			t.Errorf("input %q: got email %v, want %q", tc.inputEmail, gotVal, tc.wantEmail)
		}
	}
}

func TestSSEStreamFormatParsing(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"sse_tool\",\"description\":\"SSE tool\"}]}}\n\n"))
		case "tools/call":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"sse output\"}]}}\n\n"))
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{
		{Name: "sse-srv", ServerURL: ts.URL},
	}, nil)

	ctx := context.Background()
	tools, err := d.ListTools(ctx)
	if err != nil {
		t.Fatalf("unexpected ListTools error: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "sse_tool" {
		t.Fatalf("expected sse_tool, got %+v", tools)
	}

	res, err := d.Execute(ctx, "sse_tool", nil)
	if err != nil {
		t.Fatalf("unexpected Execute error: %v", err)
	}
	if res != "sse output" {
		t.Fatalf("expected 'sse output', got %q", res)
	}
}

func TestDispatcher_ErrorBranches(t *testing.T) {
	var nilDispatcher *DefaultDispatcher
	ctx := context.Background()

	// 1. Nil dispatcher checks
	if _, err := nilDispatcher.EnsureSession(ctx, "any"); err == nil {
		t.Errorf("expected error for nil dispatcher EnsureSession")
	}
	if tools, err := nilDispatcher.ListTools(ctx); err != nil || tools != nil {
		t.Errorf("expected nil tools and nil error for nil dispatcher ListTools")
	}
	if _, err := nilDispatcher.Execute(ctx, "tool", nil); err == nil {
		t.Errorf("expected error for nil dispatcher Execute")
	}
	if err := nilDispatcher.Prime(ctx); err != nil {
		t.Errorf("expected nil error for nil dispatcher Prime")
	}

	// 2. Unknown server in EnsureSession
	d := NewDispatcher([]ServerConfig{{Name: "srv1", ServerURL: "http://example.com"}}, nil)
	if _, err := d.EnsureSession(ctx, "unknown"); err == nil {
		t.Errorf("expected error for unknown server in EnsureSession")
	}

	// 3. Unknown tool in Execute
	if _, err := d.Execute(ctx, "nonexistent", nil); err == nil {
		t.Errorf("expected error for unknown tool in Execute")
	}

	// 4. Server returns HTTP 500 on tools/call
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal crash"))
	}))
	defer tsErr.Close()

	d500 := NewDispatcher([]ServerConfig{{Name: "srv500", ServerURL: tsErr.URL}}, nil)
	d500.mu.Lock()
	d500.toolRoutes["crash_tool"] = ServerConfig{Name: "srv500", ServerURL: tsErr.URL}
	d500.mu.Unlock()

	if _, err := d500.Execute(ctx, "crash_tool", nil); err == nil {
		t.Errorf("expected error for 500 status in Execute")
	}

	// 5. Tool returns isError: true
	tsToolErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0", "id": req["id"],
			"result": map[string]interface{}{
				"content": []map[string]interface{}{{"type": "text", "text": "bad arguments"}},
				"isError": true,
			},
		})
	}))
	defer tsToolErr.Close()

	dToolErr := NewDispatcher([]ServerConfig{{Name: "srvToolErr", ServerURL: tsToolErr.URL}}, nil)
	dToolErr.mu.Lock()
	dToolErr.toolRoutes["err_tool"] = ServerConfig{Name: "srvToolErr", ServerURL: tsToolErr.URL}
	dToolErr.mu.Unlock()

	res, err := dToolErr.Execute(ctx, "err_tool", nil)
	if err == nil || !strings.Contains(err.Error(), "bad arguments") {
		t.Errorf("expected isError tool execution error, got: %v", err)
	}
	if res != "bad arguments" {
		t.Errorf("expected res text 'bad arguments', got %q", res)
	}
}

func TestListTools_RetryOn400MissingSessionID(t *testing.T) {
	var initCalls atomic.Int32
	var listCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			calls := initCalls.Add(1)
			sessID := fmt.Sprintf("sess-list-%d", calls+1)
			w.Header().Set("mcp-session-id", sessID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			calls := listCalls.Add(1)
			if calls == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			if r.Header.Get("mcp-session-id") != "sess-list-2" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "wrong session id"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "retried_tool"}},
				},
			})
		}
	}))
	defer ts.Close()

	srv := ServerConfig{Name: "list-retry-srv", ServerURL: ts.URL}
	d := NewDispatcher([]ServerConfig{srv}, nil)

	d.mu.Lock()
	d.sessionIDs[srv.Name] = "sess-list-1"
	d.mu.Unlock()

	ctx := context.Background()
	tools, err := d.ListTools(ctx)
	if err != nil {
		t.Fatalf("unexpected ListTools error: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "retried_tool" {
		t.Fatalf("expected retried_tool, got %+v", tools)
	}
	if initCalls.Load() != 1 {
		t.Errorf("expected 1 initialize call during retry, got %d", initCalls.Load())
	}
}

func TestExecute_PlainStringAndRawResult(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []Tool{{Name: "string_tool"}, {Name: "raw_tool"}},
				},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]interface{})
			name, _ := params["name"].(string)
			if name == "string_tool" {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"jsonrpc": "2.0", "id": req["id"], "result": "plain string result",
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"status": "raw-ok"},
				})
			}
		}
	}))
	defer ts.Close()

	d := NewDispatcher([]ServerConfig{{Name: "result-srv", ServerURL: ts.URL}}, nil)
	ctx := context.Background()

	res1, err := d.Execute(ctx, "string_tool", nil)
	if err != nil {
		t.Fatalf("unexpected Execute error for string_tool: %v", err)
	}
	if res1 != "plain string result" {
		t.Errorf("expected 'plain string result', got %q", res1)
	}

	res2, err := d.Execute(ctx, "raw_tool", nil)
	if err != nil {
		t.Fatalf("unexpected Execute error for raw_tool: %v", err)
	}
	if !strings.Contains(res2, "raw-ok") {
		t.Errorf("expected json containing raw-ok, got %q", res2)
	}
}

func TestDispatcher_CoverageEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Nil dispatcher checks
	var nilDisp *DefaultDispatcher
	if _, err := nilDisp.EnsureSession(ctx, "test"); err == nil {
		t.Errorf("expected error from nilDisp.EnsureSession")
	}
	if _, err := nilDisp.ListTools(ctx); err != nil {
		t.Errorf("expected nil error from nilDisp.ListTools, got %v", err)
	}
	if _, err := nilDisp.Execute(ctx, "test", nil); err == nil {
		t.Errorf("expected error from nilDisp.Execute")
	}
	if err := nilDisp.Prime(ctx); err != nil {
		t.Errorf("expected nil error from nilDisp.Prime, got %v", err)
	}
	nilDisp.RegisterToolRoute("test", ServerConfig{})

	// 2. RegisterToolRoute on valid dispatcher
	d := NewDispatcher([]ServerConfig{{Name: "srv1", ServerURL: "http://127.0.0.1:9999"}}, nil)
	d.RegisterToolRoute("custom_tool", ServerConfig{Name: "srv1", ServerURL: "http://127.0.0.1:9999"})

	// 3. Unknown server EnsureSession
	if _, err := d.EnsureSession(ctx, "nonexistent"); err == nil {
		t.Errorf("expected error for unknown server in EnsureSession")
	}

	// 4. Server with non-200 initialize response
	errTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal server error"))
	}))
	defer errTs.Close()

	dErr := NewDispatcher([]ServerConfig{{Name: "err-srv", ServerURL: errTs.URL}}, nil)
	if _, err := dErr.EnsureSession(ctx, "err-srv"); err == nil {
		t.Errorf("expected error when initialize returns 500")
	}

	// 5. Tool not found after ListTools
	if _, err := dErr.Execute(ctx, "nonexistent_tool", nil); err == nil {
		t.Errorf("expected error for nonexistent_tool")
	}

	// 6. Server returning empty URL in Prime and ListTools
	emptyURLDisp := NewDispatcher([]ServerConfig{{Name: "empty", ServerURL: ""}}, nil)
	if tools, err := emptyURLDisp.ListTools(ctx); err != nil || len(tools) != 0 {
		t.Errorf("expected empty tools from empty URL server, got %v, err=%v", tools, err)
	}
	if err := emptyURLDisp.Prime(ctx); err != nil {
		t.Errorf("expected nil error from Prime with empty URL server, got %v", err)
	}

	// 7. Missing session ID detection helpers
	if !isMissingSessionIDErr(http.StatusBadRequest, nil, nil) {
		t.Errorf("expected true for 400 status")
	}
	if !isMissingSessionIDErr(http.StatusOK, []byte("missing session id in body"), nil) {
		t.Errorf("expected true for missing session id in body")
	}
	if !isMissingSessionIDErr(http.StatusOK, nil, &jsonRPCResponse{Error: &jsonRPCError{Code: -32600, Message: "error"}}) {
		t.Errorf("expected true for code -32600")
	}
	if !isMissingSessionIDErr(http.StatusOK, nil, &jsonRPCResponse{Error: &jsonRPCError{Code: -1, Message: "Missing Session ID error"}}) {
		t.Errorf("expected true for message with missing session id")
	}
	if isMissingSessionIDErr(http.StatusOK, []byte("ok"), nil) {
		t.Errorf("expected false for nominal response")
	}

	// 8. Server with tools/call returning non-200 error
	callErrTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-1")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{
						{"name": "fail_tool", "description": "fails"},
					},
				},
			})
			return
		}
		if method == "tools/call" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("tool failure"))
			return
		}
	}))
	defer callErrTs.Close()

	callErrDisp := NewDispatcher([]ServerConfig{{Name: "call-err", ServerURL: callErrTs.URL}}, nil)
	if _, err := callErrDisp.Execute(ctx, "fail_tool", nil); err == nil {
		t.Errorf("expected error when tools/call returns 500")
	}
}

func TestDispatcher_ErrorAndRetryEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. tools/list returning JSON-RPC error and invalid JSON
	listErrTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-list-err")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "error": map[string]interface{}{
					"code": -32000, "message": "server error listing tools",
				},
			})
			return
		}
	}))
	defer listErrTs.Close()

	listErrDisp := NewDispatcher([]ServerConfig{{Name: "list-err", ServerURL: listErrTs.URL}}, nil)
	if tools, err := listErrDisp.ListTools(ctx); err != nil || len(tools) != 0 {
		t.Errorf("expected empty tools on list error, got %v, err=%v", tools, err)
	}

	// 2. tools/list returning invalid JSON
	badJSONTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-bad-json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{invalid-json"))
			return
		}
	}))
	defer badJSONTs.Close()

	badJSONDisp := NewDispatcher([]ServerConfig{{Name: "bad-json", ServerURL: badJSONTs.URL}}, nil)
	if tools, err := badJSONDisp.ListTools(ctx); err != nil || len(tools) != 0 {
		t.Errorf("expected empty tools on bad json, got %v, err=%v", tools, err)
	}

	// 3. tools/call returning JSON-RPC error
	callRPCErrTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-call-rpc-err")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{{"name": "rpc_err_tool"}},
				},
			})
			return
		}
		if method == "tools/call" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "error": map[string]interface{}{
					"code": -32603, "message": "internal tool failure",
				},
			})
			return
		}
	}))
	defer callRPCErrTs.Close()

	callRPCErrDisp := NewDispatcher([]ServerConfig{{Name: "call-rpc-err", ServerURL: callRPCErrTs.URL}}, nil)
	if _, err := callRPCErrDisp.Execute(ctx, "rpc_err_tool", nil); err == nil {
		t.Errorf("expected error on JSON-RPC error response from tools/call")
	}

	// 4. tools/call returning 400 where EnsureSession fails on retry
	var initCount int
	retryFailTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			initCount++
			if initCount > 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("re-init failed"))
				return
			}
			w.Header().Set("mcp-session-id", "sess-first")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{{"name": "retry_fail_tool"}},
				},
			})
			return
		}
		if method == "tools/call" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("missing session id"))
			return
		}
	}))
	defer retryFailTs.Close()

	retryFailDisp := NewDispatcher([]ServerConfig{{Name: "retry-fail", ServerURL: retryFailTs.URL}}, nil)
	if _, err := retryFailDisp.Execute(ctx, "retry_fail_tool", nil); err == nil {
		t.Errorf("expected error when EnsureSession fails on 400 retry")
	}

	// 5. tools/list returning 400 where EnsureSession fails on retry
	initCountList := 0
	listRetryFailTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			initCountList++
			if initCountList > 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("re-init failed"))
				return
			}
			w.Header().Set("mcp-session-id", "sess-first-list")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("missing session id"))
			return
		}
	}))
	defer listRetryFailTs.Close()

	listRetryFailDisp := NewDispatcher([]ServerConfig{{Name: "list-retry-fail", ServerURL: listRetryFailTs.URL}}, nil)
	if tools, err := listRetryFailDisp.ListTools(ctx); err != nil || len(tools) != 0 {
		t.Errorf("expected empty tools when list retry fails, got %v, err=%v", tools, err)
	}
}

func TestDispatcher_AdditionalCoverageTargeted(t *testing.T) {
	ctx := context.Background()

	// 1. EnsureSession caching hits (both fast path and mutex re-check)
	tsCached := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("mcp-session-id", "sess-cached-hit")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
		})
	}))
	defer tsCached.Close()

	dCached := NewDispatcher([]ServerConfig{{Name: "cached-srv", ServerURL: tsCached.URL}}, nil)
	s1, err := dCached.EnsureSession(ctx, "cached-srv")
	if err != nil || s1 != "sess-cached-hit" {
		t.Fatalf("first EnsureSession failed: %v", err)
	}
	// Second call hits fast-path cached read lock
	s2, err := dCached.EnsureSession(ctx, "cached-srv")
	if err != nil || s2 != s1 {
		t.Fatalf("second EnsureSession failed: %v", err)
	}

	// 2. getServerMutex concurrent initialization
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dCached.getServerMutex("concurrent-srv")
		}()
	}
	wg.Wait()

	// 3. Tools with empty name in list results and multiple text content blocks in call
	tsTools := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-tools")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{
						{"name": ""}, // empty name to hit continue
						{"name": "multi_text_tool", "description": "returns multiple text lines"},
					},
				},
			})
			return
		}
		if method == "tools/call" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"content": []map[string]interface{}{
						{"type": "text", "text": "Line 1"},
						{"type": "text", "text": "Line 2"},
					},
				},
			})
			return
		}
	}))
	defer tsTools.Close()

	dTools := NewDispatcher([]ServerConfig{{Name: "tools-srv", ServerURL: tsTools.URL}}, nil)
	tools, err := dTools.ListTools(ctx)
	if err != nil || len(tools) != 1 || tools[0].Name != "multi_text_tool" {
		t.Fatalf("unexpected ListTools result: %v, err=%v", tools, err)
	}

	res, err := dTools.Execute(ctx, "multi_text_tool", nil)
	if err != nil || res != "Line 1\nLine 2" {
		t.Fatalf("unexpected Execute multi text result: %q, err=%v", res, err)
	}

	// 4. EnsureSession warning log before Execute when EnsureSession fails
	tsInitFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("init failed"))
	}))
	defer tsInitFail.Close()

	dInitFail := NewDispatcher([]ServerConfig{{Name: "fail-init-srv", ServerURL: tsInitFail.URL}}, nil)
	dInitFail.RegisterToolRoute("fail_init_tool", ServerConfig{Name: "fail-init-srv", ServerURL: tsInitFail.URL})
	_, _ = dInitFail.Execute(ctx, "fail_init_tool", nil) // triggers EnsureSession failure log before Execute

	// 5. Prime warning log when server prime fails
	_ = dInitFail.Prime(ctx) // triggers Prime warning log
}

func TestDispatcher_NetworkAndHeaderBranches(t *testing.T) {
	ctx := context.Background()

	// 1. Invalid URL triggering http.NewRequestWithContext error
	invDisp := NewDispatcher([]ServerConfig{
		{Name: "invalid-url", ServerURL: "://invalid-url-path\x7f", Headers: map[string]string{"H1": "V1"}},
	}, nil)
	if _, err := invDisp.EnsureSession(ctx, "invalid-url"); err == nil {
		t.Errorf("expected error from invalid URL in EnsureSession")
	}
	if _, err := invDisp.ListTools(ctx); err != nil {
		t.Errorf("expected nil error on ListTools with invalid URL, got %v", err)
	}
	invDisp.RegisterToolRoute("inv_tool", ServerConfig{Name: "invalid-url", ServerURL: "://invalid-url-path\x7f"})
	if _, err := invDisp.Execute(ctx, "inv_tool", nil); err == nil {
		t.Errorf("expected error from invalid URL in Execute")
	}

	// 2. Closed port triggering httpClient.Do error
	closedDisp := NewDispatcher([]ServerConfig{
		{Name: "closed-srv", ServerURL: "http://127.0.0.1:59999", Headers: map[string]string{"H2": "V2"}},
	}, &http.Client{Timeout: 50 * time.Millisecond})
	if _, err := closedDisp.EnsureSession(ctx, "closed-srv"); err == nil {
		t.Errorf("expected Do error in EnsureSession")
	}
	if _, err := closedDisp.ListTools(ctx); err != nil {
		t.Errorf("expected nil error in ListTools with closed srv, got %v", err)
	}
	closedDisp.RegisterToolRoute("closed_tool", ServerConfig{Name: "closed-srv", ServerURL: "http://127.0.0.1:59999", Headers: map[string]string{"H2": "V2"}})
	if _, err := closedDisp.Execute(ctx, "closed_tool", nil); err == nil {
		t.Errorf("expected Do error in Execute")
	}
}

func TestDispatcher_FinalCoverageBooster(t *testing.T) {
	ctx := context.Background()

	// 1. tools/call with invalid JSON in response
	badCallTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-bad-call")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{{"name": "bad_call_tool"}},
				},
			})
			return
		}
		if method == "tools/call" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{invalid-json-call"))
			return
		}
	}))
	defer badCallTs.Close()

	dBadCall := NewDispatcher([]ServerConfig{{Name: "bad-call-srv", ServerURL: badCallTs.URL}}, nil)
	if _, err := dBadCall.Execute(ctx, "bad_call_tool", nil); err == nil {
		t.Errorf("expected error on invalid JSON from tools/call")
	}

	// 2. tools/list with non-struct Result
	badListResultTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-bad-list")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": "invalid-tools-structure",
			})
			return
		}
	}))
	defer badListResultTs.Close()

	dBadList := NewDispatcher([]ServerConfig{{Name: "bad-list-srv", ServerURL: badListResultTs.URL}}, nil)
	if tools, err := dBadList.ListTools(ctx); err != nil || len(tools) != 0 {
		t.Errorf("expected empty tools on unmarshal error, got %v, err=%v", tools, err)
	}

	// 3. Retry with headers and closed server on tools/call retry
	var callRetryCount int
	var retryDoTs *httptest.Server
	retryDoTs = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-retry-do")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{
					"tools": []map[string]interface{}{{"name": "retry_do_tool"}},
				},
			})
			return
		}
		if method == "tools/call" {
			callRetryCount++
			if callRetryCount == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("missing session id"))
				return
			}
			// Close server on retry
			retryDoTs.CloseClientConnections()
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer retryDoTs.Close()

	dRetryDo := NewDispatcher([]ServerConfig{{
		Name:      "retry-do-srv",
		ServerURL: retryDoTs.URL,
		Headers:   map[string]string{"X-Retry": "true"},
	}}, nil)
	// Seed session ID so first call hits 400
	dRetryDo.sessionIDs["retry-do-srv"] = "old-sess"
	dRetryDo.toolRoutes["retry_do_tool"] = dRetryDo.servers[0]
	_, _ = dRetryDo.Execute(ctx, "retry_do_tool", nil)

	// 4. EnsureSession non-canonical header lookup loop
	tsNonCanon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header()["mcp-session-id"] = []string{"sess-non-canon"}
		delete(w.Header(), "Mcp-Session-Id")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
		})
	}))
	defer tsNonCanon.Close()

	dNonCanon := NewDispatcher([]ServerConfig{{Name: "non-canon", ServerURL: tsNonCanon.URL}}, nil)
	sNonCanon, err := dNonCanon.EnsureSession(ctx, "non-canon")
	if err != nil || sNonCanon != "sess-non-canon" {
		t.Fatalf("EnsureSession with non-canonical header failed: %q, err=%v", sNonCanon, err)
	}
}

func TestDispatcher_FinalTargetedPush(t *testing.T) {
	ctx := context.Background()

	// 1. tools/list retry with headers and connection closed on retry
	var listRetryCount int
	var listRetryDoTs *httptest.Server
	listRetryDoTs = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("mcp-session-id", "sess-list-retry-do")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req["id"], "result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})
			return
		}
		if method == "tools/list" {
			listRetryCount++
			if listRetryCount == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("missing session id"))
				return
			}
			listRetryDoTs.CloseClientConnections()
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer listRetryDoTs.Close()

	dListRetryDo := NewDispatcher([]ServerConfig{{
		Name:      "list-retry-do-srv",
		ServerURL: listRetryDoTs.URL,
		Headers:   map[string]string{"X-Retry": "true"},
	}}, nil)
	dListRetryDo.sessionIDs["list-retry-do-srv"] = "old-sess"
	_, _ = dListRetryDo.ListTools(ctx)

	// 2. JIT ListTools warning log in Execute when tool is not found and ListTools fails
	jitFailTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer jitFailTs.Close()

	dJitFail := NewDispatcher([]ServerConfig{{Name: "jit-fail", ServerURL: jitFailTs.URL}}, nil)
	_, _ = dJitFail.Execute(ctx, "nonexistent", nil)
}







