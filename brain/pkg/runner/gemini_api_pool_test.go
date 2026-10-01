package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGeminiAPIInterfaces_Satisfaction(t *testing.T) {
	t.Parallel()

	var _ AgentPool = (*GeminiAPIPool)(nil)
	var _ AgentSession = (*GeminiAPISession)(nil)
}

func TestGeminiAPIPool_StreamingSuccess(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Errorf("expected flusher")
			return
		}

		// First SSE chunk with text delta
		chunk1 := `data: {"candidates":[{"content":{"parts":[{"text":"Hello "}],"role":"model"}}],"index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk1))
		flusher.Flush()

		// Second SSE chunk with text delta and usageMetadata
		chunk2 := `data: {"candidates":[{"content":{"parts":[{"text":"world!"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":6,"totalTokenCount":18}}` + "\n\n"
		_, _ = w.Write([]byte(chunk2))
		flusher.Flush()
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	ctx := context.Background()
	sess, err := pool.GetOrCreateSession(ctx, "kiosk-living-room", "")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-stream-1",
		Prompt:    "Say hello",
		Sink:      sink,
		CreatedAt: time.Now(),
		Ctx:       ctx,
	}

	if err := sess.Send("Say hello", turn); err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if !sink.started {
		t.Errorf("expected sink.started to be true")
	}

	expectedDeltas := []string{"Hello ", "world!"}
	if len(sink.deltas) != len(expectedDeltas) {
		t.Fatalf("expected %d deltas, got %d: %v", len(expectedDeltas), len(sink.deltas), sink.deltas)
	}
	for i, d := range expectedDeltas {
		if sink.deltas[i] != d {
			t.Errorf("delta[%d]: expected %q, got %q", i, d, sink.deltas[i])
		}
	}

	if sink.result == nil {
		t.Fatalf("expected non-nil sink.result")
	}
	if sink.result.Response != "Hello world!" {
		t.Errorf("expected response 'Hello world!', got %q", sink.result.Response)
	}
	if sink.result.Usage.InputTokens != 12 {
		t.Errorf("expected InputTokens 12, got %d", sink.result.Usage.InputTokens)
	}
	if sink.result.Usage.OutputTokens != 6 {
		t.Errorf("expected OutputTokens 6, got %d", sink.result.Usage.OutputTokens)
	}
	if sink.result.Usage.TotalTokens != 18 {
		t.Errorf("expected TotalTokens 18, got %d", sink.result.Usage.TotalTokens)
	}

	// Verify history updated
	geminiSess := sess.(*GeminiAPISession)
	history := geminiSess.History()
	if len(history) != 2 {
		t.Fatalf("expected history length 2, got %d", len(history))
	}
	if history[0].Role != "user" || history[0].Parts[0].Text != "Say hello" {
		t.Errorf("unexpected user message in history: %+v", history[0])
	}
	if history[1].Role != "model" || history[1].Parts[0].Text != "Hello world!" {
		t.Errorf("unexpected model message in history: %+v", history[1])
	}
}

func TestGeminiAPIPool_ZeroThinkingBudget(t *testing.T) {
	t.Parallel()

	var receivedBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		receivedBody = body

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Thinking budget tested\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:       "key-abc",
		Model:        "gemini-2.5-flash",
		BaseURL:      ts.URL,
		HTTPClient:   ts.Client(),
		SystemPrompt: "You are Aerial.",
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-device", "")
	if err != nil {
		t.Fatalf("unexpected error getting session: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-zero-thinking",
		Prompt:    "Check thinking budget",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	if err := sess.Send("Check thinking budget", turn); err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	// Parse received JSON body and inspect thinkingConfig
	var parsed struct {
		Contents []struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"contents"`
		SystemInstruction struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"systemInstruction"`
		GenerationConfig struct {
			ThinkingConfig struct {
				ThinkingBudget int `json:"thinkingBudget"`
			} `json:"thinkingConfig"`
		} `json:"generationConfig"`
	}

	if err := json.Unmarshal(receivedBody, &parsed); err != nil {
		t.Fatalf("failed to unmarshal request body: %v\nbody was: %s", err, string(receivedBody))
	}

	// Confirm thinkingBudget: 0 is present
	if parsed.GenerationConfig.ThinkingConfig.ThinkingBudget != 0 {
		t.Errorf("expected thinkingBudget 0, got %d", parsed.GenerationConfig.ThinkingConfig.ThinkingBudget)
	}

	// Also verify literal JSON string contains "thinkingBudget": 0 or "thinkingBudget":0
	rawJSON := string(receivedBody)
	if !strings.Contains(rawJSON, `"thinkingBudget":0`) && !strings.Contains(rawJSON, `"thinkingBudget": 0`) {
		t.Errorf("expected JSON to contain thinkingBudget: 0, got %s", rawJSON)
	}

	// Verify system instruction is present
	if len(parsed.SystemInstruction.Parts) == 0 || parsed.SystemInstruction.Parts[0].Text != "You are Aerial." {
		t.Errorf("expected systemInstruction 'You are Aerial.', got %+v", parsed.SystemInstruction)
	}
}

func TestGeminiAPIPool_SlidingWindowHistory(t *testing.T) {
	t.Parallel()

	turnCounter := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&turnCounter, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		respPayload := fmt.Sprintf("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Response %d\"}],\"role\":\"model\"}}]}\n\n", current)
		_, _ = w.Write([]byte(respPayload))
	}))
	defer ts.Close()

	// Default MaxHistoryTurns = 10, meaning max 20 messages (10 user + 10 model)
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "history-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxHistoryTurns: 10,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "history-device", "")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}

	// Send 15 turns sequentially
	for i := 1; i <= 15; i++ {
		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID:    fmt.Sprintf("turn-%d", i),
			Prompt:    fmt.Sprintf("Prompt %d", i),
			Sink:      sink,
			CreatedAt: time.Now(),
		}
		if err := sess.Send(fmt.Sprintf("Prompt %d", i), turn); err != nil {
			t.Fatalf("turn %d failed: %v", i, err)
		}
	}

	geminiSess := sess.(*GeminiAPISession)
	history := geminiSess.History()

	// 10 turns * 2 = 20 messages maximum
	expectedMaxMessages := 10 * 2
	if len(history) != expectedMaxMessages {
		t.Fatalf("expected history to be pruned to %d messages, got %d", expectedMaxMessages, len(history))
	}

	// The first message in history should now be turn 6 (turns 1-5 pruned)
	if history[0].Role != "user" || history[0].Parts[0].Text != "Prompt 6" {
		t.Errorf("expected first user prompt to be 'Prompt 6', got %q", history[0].Parts[0].Text)
	}

	// The last message in history should be turn 15 model response
	lastIdx := len(history) - 1
	if history[lastIdx].Role != "model" || history[lastIdx].Parts[0].Text != "Response 15" {
		t.Errorf("expected last model response to be 'Response 15', got %q", history[lastIdx].Parts[0].Text)
	}
}

func TestGeminiAPIPool_ContextCancellation(t *testing.T) {
	t.Parallel()

	releaseServer := make(chan struct{})
	defer close(releaseServer)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if ok {
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Partial\"}],\"role\":\"model\"}}]}\n\n"))
			flusher.Flush()
		}

		// Block until client disconnects or test finishes
		select {
		case <-r.Context().Done():
		case <-releaseServer:
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "cancel-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "cancel-device", "")
	if err != nil {
		t.Fatalf("unexpected error getting session: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sink := newMockTurnSink()

	turn := &TurnContext{
		TurnID:    "turn-cancel",
		Prompt:    "Prompt to cancel",
		Sink:      sink,
		CreatedAt: time.Now(),
		Ctx:       ctx,
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- sess.Send("Prompt to cancel", turn)
	}()

	// Wait for partial chunk to trigger turn started
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		started := sink.started
		sink.mu.Unlock()
		if started {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel context mid-stream
	cancel()

	select {
	case sendErr := <-doneCh:
		if sendErr == nil {
			t.Fatalf("expected error from cancelled Send, got nil")
		}
		sink.mu.Lock()
		sinkErr := sink.err
		sink.mu.Unlock()
		if sinkErr == nil {
			t.Errorf("expected error delivered to sink.OnError")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for Send to terminate after context cancellation")
	}
}

func TestGeminiAPIPool_RateLimit429(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "ratelimit-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "rate-limited-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-429",
		Prompt:    "Hello under limit",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	sendErr := sess.Send("Hello under limit", turn)
	if sendErr == nil {
		t.Fatalf("expected error on 429, got nil")
	}

	expectedMsg := "rate limit exceeded: please try again shortly"
	if sendErr.Error() != expectedMsg {
		t.Errorf("expected Send error %q, got %q", expectedMsg, sendErr.Error())
	}

	sink.mu.Lock()
	sinkErr := sink.err
	sink.mu.Unlock()
	if sinkErr == nil {
		t.Fatalf("expected sink.OnError to be called")
	}
	if sinkErr.Error() != expectedMsg {
		t.Errorf("expected sink error %q, got %q", expectedMsg, sinkErr.Error())
	}
}

func TestGeminiAPIPool_EmptyPrompt(t *testing.T) {
	t.Parallel()

	var requestCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "device-empty", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	testCases := []string{"", "   ", "\t\n\r  "}
	for _, tc := range testCases {
		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID:    "turn-empty",
			Prompt:    tc,
			Sink:      sink,
			CreatedAt: time.Now(),
		}

		err := sess.Send(tc, turn)
		if err == nil {
			t.Errorf("expected error for empty prompt %q, got nil", tc)
		}
		if atomic.LoadInt32(&requestCount) != 0 {
			t.Errorf("expected no HTTP requests made for empty prompt, got %d", atomic.LoadInt32(&requestCount))
		}
		sink.mu.Lock()
		if sink.err == nil {
			t.Errorf("expected sink.OnError to be called for empty prompt %q", tc)
		}
		sink.mu.Unlock()
	}
}

func TestGeminiAPIPool_PrewarmedAndClose(t *testing.T) {
	t.Parallel()

	targets := []string{"living-room", "kitchen", "office"}
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:           "prewarm-key",
		Model:            "gemini-2.5-flash",
		PrewarmedTargets: targets,
	})

	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}

	// Verify all prewarmed targets are initialized
	for _, target := range targets {
		sess, err := pool.GetOrCreateSession(ctx, target, "")
		if err != nil {
			t.Errorf("expected session for prewarmed target %s, got error: %v", target, err)
		}
		if sess.SessionID() != target {
			t.Errorf("expected session ID %s, got %s", target, sess.SessionID())
		}
	}

	// Close pool
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	// Verify GetOrCreateSession on closed pool fails
	if _, err := pool.GetOrCreateSession(ctx, "new-target", ""); err == nil {
		t.Errorf("expected error on closed pool GetOrCreateSession, got nil")
	}

	// Verify Initialize on closed pool fails
	if err := pool.Initialize(ctx); err == nil {
		t.Errorf("expected error on closed pool Initialize, got nil")
	}

	// Verify Send on existing session of closed pool fails
	livingRoomSess := pool.sessions["living-room"]
	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-closed",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now(),
	}
	if err := livingRoomSess.Send("hello", turn); err == nil {
		t.Errorf("expected error calling Send on closed pool session, got nil")
	}
}

func TestGeminiAPIPool_SanitizeAPIKey(t *testing.T) {
	t.Parallel()

	secretKey := "AIzaSySuperSecretKey123456789"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Server returns 500 with body containing the key
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"internal error occurred with key %s"}`, secretKey)))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     secretKey,
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "sanitize-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-sanitize",
		Prompt:    "trigger error",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	sendErr := sess.Send("trigger error", turn)
	if sendErr == nil {
		t.Fatalf("expected error from server 500, got nil")
	}

	// Verify Send error does not contain secret key
	if strings.Contains(sendErr.Error(), secretKey) {
		t.Errorf("send error contains unredacted API key: %s", sendErr.Error())
	}
	if !strings.Contains(sendErr.Error(), "[REDACTED]") {
		t.Errorf("send error expected to contain [REDACTED], got: %s", sendErr.Error())
	}

	// Verify sink.OnError does not contain secret key
	sink.mu.Lock()
	sinkErr := sink.err
	sink.mu.Unlock()
	if sinkErr == nil {
		t.Fatalf("expected error delivered to sink")
	}
	if strings.Contains(sinkErr.Error(), secretKey) {
		t.Errorf("sink error contains unredacted API key: %s", sinkErr.Error())
	}
	if !strings.Contains(sinkErr.Error(), "[REDACTED]") {
		t.Errorf("sink error expected to contain [REDACTED], got: %s", sinkErr.Error())
	}

	// Also directly test pool.sanitizeError with query param in error message
	queryErr := fmt.Errorf("connection failed to https://generativelanguage.googleapis.com/v1beta/models/gemini:stream?key=%s&alt=sse: dial tcp timeout", secretKey)
	sanitized := pool.sanitizeError(queryErr)
	if strings.Contains(sanitized.Error(), secretKey) {
		t.Errorf("sanitized query error still contains secret key: %s", sanitized.Error())
	}
	if !strings.Contains(sanitized.Error(), "key=[REDACTED]") {
		t.Errorf("sanitized query error expected to contain 'key=[REDACTED]', got: %s", sanitized.Error())
	}
}

func TestGeminiAPIPool_Accessors(t *testing.T) {
	t.Parallel()

	var nilPool *GeminiAPIPool
	if nilPool.Model() != "" {
		t.Errorf("expected empty string from nilPool.Model()")
	}
	if nilPool.APIKey() != "" {
		t.Errorf("expected empty string from nilPool.APIKey()")
	}

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "my-api-key",
		Model:  "gemini-2.5-pro",
	})
	if pool.Model() != "gemini-2.5-pro" {
		t.Errorf("expected model 'gemini-2.5-pro', got %q", pool.Model())
	}
	if pool.APIKey() != "my-api-key" {
		t.Errorf("expected APIKey 'my-api-key', got %q", pool.APIKey())
	}

	sess, err := pool.GetOrCreateSession(context.Background(), "target-123", "")
	if err != nil {
		t.Fatalf("unexpected error creating session: %v", err)
	}
	geminiSess, ok := sess.(*GeminiAPISession)
	if !ok {
		t.Fatalf("expected *GeminiAPISession, got %T", sess)
	}
	if geminiSess.TargetKey() != "target-123" {
		t.Errorf("expected TargetKey 'target-123', got %q", geminiSess.TargetKey())
	}
}

func TestMCPDispatcher_ListDeclarations_JSONAndSSE(t *testing.T) {
	t.Parallel()

	// Server 1: standard JSON response (e.g. scheduler-mcp)
	tsJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"result": {
				"tools": [
					{
						"name": "schedule_once",
						"description": "Schedule a reminder",
						"inputSchema": {
							"$schema": "http://json-schema.org/draft-07/schema#",
							"type": "object",
							"properties": {"prompt": {"type": "string"}},
							"required": ["prompt"]
						}
					}
				]
			}
		}`))
	}))
	defer tsJSON.Close()

	// Server 2: SSE Streamable HTTP response (e.g. ha-mcp)
	tsSSE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"ha_call_write_tool\",\"description\":\"Call HA service\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"entity_id\":{\"type\":\"string\"}}}}]}}\n\n"))
	}))
	defer tsSSE.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "scheduler", ServerURL: tsJSON.URL},
		{Name: "ha-mcp", ServerURL: tsSSE.URL, Headers: map[string]string{"Authorization": "Bearer token123"}},
	}, tsJSON.Client())

	ctx := context.Background()
	decls, err := dispatcher.ListDeclarations(ctx)
	if err != nil {
		t.Fatalf("unexpected ListDeclarations error: %v", err)
	}

	if len(decls) != 2 {
		t.Fatalf("expected 2 declarations, got %d: %+v", len(decls), decls)
	}

	names := make(map[string]bool)
	for _, d := range decls {
		names[d.Name] = true
		if _, hasSchema := d.Parameters["$schema"]; hasSchema {
			t.Errorf("expected $schema to be stripped from parameters in tool %s", d.Name)
		}
	}

	if !names["schedule_once"] || !names["ha_call_write_tool"] {
		t.Errorf("missing expected tools in declarations: %v", names)
	}
}

func TestMCPDispatcher_Execute_SuccessAndError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name      string                 `json:"name"`
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "good_tool", "description": "Good"},
						{"name": "error_tool", "description": "Error"},
						{"name": "raw_tool", "description": "Raw"}
					]
				}
			}`))
			return
		}

		if req.Method == "tools/call" {
			switch req.Params.Name {
			case "good_tool":
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"Light turned on\"}]}}\n\n"))
			case "error_tool":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[{"type":"text","text":"Device not reachable"}]}}`))
			case "raw_tool":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`))
			default:
				http.Error(w, "unknown tool", http.StatusNotFound)
			}
		}
	}))
	defer ts.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "test-mcp", ServerURL: ts.URL},
	}, ts.Client())

	ctx := context.Background()

	// Good tool execution
	out, err := dispatcher.Execute(ctx, "good_tool", map[string]interface{}{"entity": "light.1"})
	if err != nil {
		t.Fatalf("unexpected error executing good_tool: %v", err)
	}
	if out != "Light turned on" {
		t.Errorf("expected 'Light turned on', got %q", out)
	}

	// Tool returning isError: true
	outErr, err := dispatcher.Execute(ctx, "error_tool", nil)
	if err == nil {
		t.Fatalf("expected error from error_tool, got nil")
	}
	if !strings.Contains(err.Error(), "Device not reachable") {
		t.Errorf("expected error message to contain 'Device not reachable', got %v", err)
	}
	if outErr != "Device not reachable" {
		t.Errorf("expected output 'Device not reachable', got %q", outErr)
	}

	// Tool returning raw JSON without content array
	rawOut, err := dispatcher.Execute(ctx, "raw_tool", nil)
	if err != nil {
		t.Fatalf("unexpected error executing raw_tool: %v", err)
	}
	if !strings.Contains(rawOut, `"status":"ok"`) {
		t.Errorf("expected raw JSON containing status: ok, got %q", rawOut)
	}

	// Unknown tool
	_, err = dispatcher.Execute(ctx, "non_existent_tool", nil)
	if err == nil {
		t.Fatalf("expected error for non_existent_tool, got nil")
	}
}

func TestGeminiAPIPool_WithMCPToolCalling(t *testing.T) {
	t.Parallel()

	// Mock MCP Server
	tsMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name      string                 `json:"name"`
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{
							"name": "ha_call_write_tool",
							"description": "Control Home Assistant devices",
							"inputSchema": {
								"type": "object",
								"properties": {
									"domain": {"type": "string"},
									"service": {"type": "string"}
								}
							}
						}
					]
				}
			}`))
			return
		}

		if req.Method == "tools/call" && req.Params.Name == "ha_call_write_tool" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"content": [{"type": "text", "text": "Service called successfully"}]
				}
			}`))
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer tsMCP.Close()

	// Mock Gemini Server:
	// Turn 1: receives user prompt, returns FunctionCall for ha_call_write_tool
	// Turn 2: receives FunctionResponse, returns final text response
	geminiReqCount := int32(0)
	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := atomic.AddInt32(&geminiReqCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if reqNum == 1 {
			// First call: Gemini returns function call
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"ha_call_write_tool","args":{"domain":"light","service":"turn_off"}}}],"role":"model"}}],"index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		} else {
			// Second call: Gemini returns final conversational answer
			chunk := `data: {"candidates":[{"content":{"parts":[{"text":"I've turned off the light."}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":25,"candidatesTokenCount":10,"totalTokenCount":35}}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		}
	}))
	defer tsGemini.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{
			{Name: "ha-mcp", ServerURL: tsMCP.URL},
		},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-mcp-1",
		Prompt:    "turn off the light",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	if err := sess.Send("turn off the light", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if len(sink.toolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d: %v", len(sink.toolCalls), sink.toolCalls)
	}
	if !strings.HasPrefix(sink.toolCalls[0], "ha_call_write_tool:") {
		t.Errorf("expected tool call starting with 'ha_call_write_tool:', got %q", sink.toolCalls[0])
	}
	if sink.result == nil || sink.result.Response != "I've turned off the light." {
		t.Errorf("expected final response 'I've turned off the light.', got %+v", sink.result)
	}
}

func TestGeminiAPIPool_WithMCPToolCalling_ErrorRecovery(t *testing.T) {
	t.Parallel()

	tsMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"failing_tool","description":"fails"}]}}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("MCP server crash"))
	}))
	defer tsMCP.Close()

	geminiReqCount := int32(0)
	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := atomic.AddInt32(&geminiReqCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if reqNum == 1 {
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"failing_tool","args":{}}}],"role":"model"}}],"index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		} else {
			// Model explains the tool failure gracefully
			chunk := `data: {"candidates":[{"content":{"parts":[{"text":"Sorry, that device is currently unreachable."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		}
	}))
	defer tsGemini.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{
			{Name: "broken-mcp", ServerURL: tsMCP.URL},
		},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-err", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-err-recovery",
		Prompt: "trigger failing device",
		Sink:   sink,
	}

	if err := sess.Send("trigger failing device", turn); err != nil {
		t.Fatalf("unexpected error from Send: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.result == nil || !strings.Contains(sink.result.Response, "unreachable") {
		t.Errorf("expected recovery response containing 'unreachable', got %+v", sink.result)
	}
}

func TestGeminiAPISession_TranscriptPersistenceAndHydration(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Persisted response\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
		DataDir:    tmpDir,
	})
	defer pool.Close()

	sessID := "persisted-session"
	sess, err := pool.GetOrCreateSession(context.Background(), sessID, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-persist-1",
		Prompt: "First prompt",
		Sink:   sink,
	}

	if err := sess.Send("First prompt", turn); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}

	// Verify transcript file written to disk
	transcriptFile := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(transcriptFile)
	if err != nil {
		t.Fatalf("failed to read transcript file %s: %v", transcriptFile, err)
	}
	content := string(data)
	if !strings.Contains(content, "First prompt") || !strings.Contains(content, "Persisted response") {
		t.Errorf("transcript missing expected turns: %s", content)
	}

	// Test hydration: create a brand new session with empty in-memory history pointing to same dataDir
	freshSession := &GeminiAPISession{
		targetKey: sessID,
		sessionID: sessID,
		pool:      pool,
		dataDir:   tmpDir,
	}
	freshSession.hydrateHistory()

	hist := freshSession.History()
	if len(hist) != 2 {
		t.Fatalf("expected 2 hydrated history messages, got %d: %+v", len(hist), hist)
	}
	if hist[0].Role != "user" || hist[0].Parts[0].Text != "First prompt" {
		t.Errorf("unexpected first history message: %+v", hist[0])
	}
	if hist[1].Role != "model" || hist[1].Parts[0].Text != "Persisted response" {
		t.Errorf("unexpected second history message: %+v", hist[1])
	}
}

func TestGeminiAPIPool_EdgeCasesAndCoverage(t *testing.T) {
	t.Parallel()

	// 1. sanitizeError with nil
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{})
	if err := pool.sanitizeError(nil); err != nil {
		t.Errorf("expected sanitizeError(nil) == nil, got %v", err)
	}

	// 2. SessionID fallback when sessionID is empty
	sess := &GeminiAPISession{targetKey: "fallback-target"}
	if sess.SessionID() != "fallback-target" {
		t.Errorf("expected 'fallback-target', got %q", sess.SessionID())
	}
	if sess.transcriptPath() != "" {
		t.Errorf("expected empty transcript path when dataDir is empty")
	}

	// 3. cleanParameters nil and properties default
	params := cleanParameters(nil)
	if params["type"] != "OBJECT" {
		t.Errorf("expected OBJECT type for nil parameters")
	}
	emptyParams := cleanParameters(map[string]interface{}{"description": "test"})
	if emptyParams["type"] != "OBJECT" {
		t.Errorf("expected default OBJECT type")
	}

	// 4. parseJSONRPCBody errors
	if _, err := parseJSONRPCBody([]byte("")); err == nil {
		t.Errorf("expected error for empty body")
	}
	if _, err := parseJSONRPCBody([]byte("not json")); err == nil {
		t.Errorf("expected error for non-json body")
	}

	// 5. DefaultMCPDispatcher with nil or empty
	var nilDispatcher *DefaultMCPDispatcher
	if _, err := nilDispatcher.Execute(context.Background(), "tool", nil); err == nil {
		t.Errorf("expected error from nil dispatcher Execute")
	}
	decls, err := nilDispatcher.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Errorf("expected nil/empty from nil dispatcher ListDeclarations")
	}

	emptyDispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "empty-url", ServerURL: ""},
	}, nil)
	decls, err = emptyDispatcher.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Errorf("expected empty declarations for empty ServerURL")
	}
}

func TestMCPDispatcher_ListDeclarations_ErrorPaths(t *testing.T) {
	t.Parallel()

	// 1. Server returns HTTP 500
	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer ts500.Close()

	// 2. Server returns JSON-RPC error
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`))
	}))
	defer tsErr.Close()

	// 3. Server returns invalid JSON
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"invalid_shape"}`))
	}))
	defer tsBadJSON.Close()

	// 4. Server returns tool with empty name
	tsEmptyName := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"","description":"empty"}]}}`))
	}))
	defer tsEmptyName.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "srv-500", ServerURL: ts500.URL},
		{Name: "srv-err", ServerURL: tsErr.URL},
		{Name: "srv-bad-json", ServerURL: tsBadJSON.URL},
		{Name: "srv-empty-name", ServerURL: tsEmptyName.URL},
		{Name: "srv-unreachable", ServerURL: "http://127.0.0.1:0"},
	}, ts500.Client())

	decls, err := dispatcher.ListDeclarations(context.Background())
	if err != nil {
		t.Fatalf("expected graceful continuation on server errors, got: %v", err)
	}
	if len(decls) != 0 {
		t.Errorf("expected 0 valid declarations from error servers, got %d", len(decls))
	}
}

func TestMCPDispatcher_Execute_ErrorPaths(t *testing.T) {
	t.Parallel()

	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal crash", http.StatusInternalServerError)
	}))
	defer ts500.Close()

	tsRPCErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"tool execution failed"}}`))
	}))
	defer tsRPCErr.Close()

	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not a json response`))
	}))
	defer tsBadJSON.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "crash-srv", ServerURL: ts500.URL},
		{Name: "rpc-err-srv", ServerURL: tsRPCErr.URL},
		{Name: "bad-json-srv", ServerURL: tsBadJSON.URL},
		{Name: "unreachable", ServerURL: "http://127.0.0.1:0"},
	}, ts500.Client())

	dispatcher.toolRoutes["crash_tool"] = MCPServerConfig{Name: "crash-srv", ServerURL: ts500.URL}
	dispatcher.toolRoutes["rpc_err_tool"] = MCPServerConfig{Name: "rpc-err-srv", ServerURL: tsRPCErr.URL}
	dispatcher.toolRoutes["bad_json_tool"] = MCPServerConfig{Name: "bad-json-srv", ServerURL: tsBadJSON.URL}
	dispatcher.toolRoutes["unreachable_tool"] = MCPServerConfig{Name: "unreachable", ServerURL: "http://127.0.0.1:0"}

	ctx := context.Background()

	// 1. 500 error
	if _, err := dispatcher.Execute(ctx, "crash_tool", nil); err == nil {
		t.Errorf("expected error from 500 server")
	}

	// 2. JSON-RPC error
	if _, err := dispatcher.Execute(ctx, "rpc_err_tool", nil); err == nil {
		t.Errorf("expected error from rpc_err_tool")
	}

	// 3. Bad JSON
	if _, err := dispatcher.Execute(ctx, "bad_json_tool", nil); err == nil {
		t.Errorf("expected error from bad_json_tool")
	}

	// 4. Unreachable
	if _, err := dispatcher.Execute(ctx, "unreachable_tool", nil); err == nil {
		t.Errorf("expected error from unreachable tool")
	}
}

func TestGeminiAPIPool_Send_WithNilSinkAndErrors(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "nil-sink-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Send with nil turn.Sink when server returns error
	turn := &TurnContext{
		TurnID: "turn-nil-sink",
		Prompt: "hello",
		Sink:   nil,
	}
	err = sess.Send("hello", turn)
	if err == nil {
		t.Fatalf("expected error from 500 server, got nil")
	}

	// Send with cancelled context and nil turn.Sink
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	turnCancelled := &TurnContext{
		TurnID: "turn-cancel-nil-sink",
		Prompt: "hello",
		Sink:   nil,
		Ctx:    ctx,
	}
	err = sess.Send("hello", turnCancelled)
	if err == nil {
		t.Fatalf("expected error from cancelled context, got nil")
	}
}

func TestGeminiAPIPool_Send_SSEMalformedChunk(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {malformed_json\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "malformed-sse", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-malformed",
		Prompt: "trigger malformed",
		Sink:   sink,
	}

	err = sess.Send("trigger malformed", turn)
	if err == nil {
		t.Fatalf("expected error on malformed SSE chunk, got nil")
	}
}

type errMockDispatcher struct{}

func (d *errMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return nil, errors.New("declarations query failed")
}

func (d *errMockDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	return "", errors.New("execute failed")
}

func TestGeminiAPIPool_Send_DispatcherErrors(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Fallback response\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:        "test-key",
		Model:         "gemini-2.5-flash",
		BaseURL:       ts.URL,
		HTTPClient:    ts.Client(),
		MCPDispatcher: &errMockDispatcher{},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "dispatcher-err-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-dispatch-err",
		Prompt: "hello despite dispatcher error",
		Sink:   sink,
	}

	err = sess.Send("hello despite dispatcher error", turn)
	if err != nil {
		t.Fatalf("expected successful send with fallback, got error: %v", err)
	}
}

func TestGeminiAPISession_HistoryPruningLongTranscript(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	sessID := "long-transcript-sess"
	p := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs", "transcript.jsonl")
	_ = os.MkdirAll(filepath.Dir(p), 0755)

	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("failed to create transcript: %v", err)
	}
	for i := 1; i <= 30; i++ {
		userLine, _ := json.Marshal(map[string]interface{}{
			"type":    "USER_INPUT",
			"content": fmt.Sprintf("prompt %d", i),
		})
		modelLine, _ := json.Marshal(map[string]interface{}{
			"type":    "PLANNER_RESPONSE",
			"content": fmt.Sprintf("reply %d", i),
		})
		_, _ = f.Write(append(userLine, '\n'))
		_, _ = f.Write(append(modelLine, '\n'))
	}
	_ = f.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		DataDir: tmpDir,
	})
	sess := &GeminiAPISession{
		targetKey: sessID,
		sessionID: sessID,
		pool:      pool,
		dataDir:   tmpDir,
	}
	sess.hydrateHistory()

	hist := sess.History()
	if len(hist) != 20 {
		t.Fatalf("expected history to be clamped to 20 messages, got %d", len(hist))
	}
	if hist[0].Parts[0].Text != "prompt 21" {
		t.Errorf("expected first message after pruning to be 'prompt 21', got %q", hist[0].Parts[0].Text)
	}
}

func TestGeminiAPISession_AppendTranscript_BranchCoverage(t *testing.T) {
	t.Parallel()

	// 1. Empty dataDir -> immediate return (p == "")
	sess1 := &GeminiAPISession{dataDir: ""}
	sess1.appendTranscript("prompt", "response")

	// 2. Uncreatable directory -> os.MkdirAll error
	sess2 := &GeminiAPISession{
		sessionID: "test-err-dir",
		dataDir:   "/dev/null/forbidden",
	}
	sess2.appendTranscript("prompt", "response")

	// 3. File exists as directory -> os.OpenFile error
	tmp := t.TempDir()
	sess3 := &GeminiAPISession{
		sessionID: "test-err-file",
		dataDir:   tmp,
	}
	p := sess3.transcriptPath()
	_ = os.MkdirAll(p, 0755)
	sess3.appendTranscript("prompt", "response")
}

func TestMCPDispatcher_AllEdgeCases(t *testing.T) {
	t.Parallel()

	// 1. Nil dispatcher ListDeclarations
	var nilDisp *DefaultMCPDispatcher
	decls, err := nilDisp.ListDeclarations(context.Background())
	if err != nil || decls != nil {
		t.Fatalf("expected nil, nil for nil dispatcher")
	}

	// 2. Empty server list ListDeclarations
	emptyDisp := NewDefaultMCPDispatcher(nil, nil)
	decls, err = emptyDisp.ListDeclarations(context.Background())
	if err != nil || decls != nil {
		t.Fatalf("expected nil, nil for empty server list")
	}

	// 3. Server with empty ServerURL
	skipDisp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "empty-url", ServerURL: "   "},
	}, http.DefaultClient)
	decls, err = skipDisp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Fatalf("expected empty decls for empty server URL")
	}

	// 4. Server with non-200 status code
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer errServer.Close()

	errDisp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "err-srv", ServerURL: errServer.URL},
	}, errServer.Client())
	decls, err = errDisp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Fatalf("expected empty decls on server error")
	}

	// 5. Server returning JSON-RPC error
	rpcErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}`))
	}))
	defer rpcErrServer.Close()

	rpcErrDisp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "rpc-err-srv", ServerURL: rpcErrServer.URL},
	}, rpcErrServer.Client())
	decls, err = rpcErrDisp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Fatalf("expected empty decls on rpc error")
	}

	// 6. Server returning unparseable tools result & tool with empty name
	malformedToolsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"result": {
				"tools": [
					{"name": "", "description": "unnamed tool"},
					{"name": "valid_tool", "description": "valid"}
				]
			}
		}`))
	}))
	defer malformedToolsServer.Close()

	malformedDisp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "malformed-tools", ServerURL: malformedToolsServer.URL},
	}, malformedToolsServer.Client())
	decls, err = malformedDisp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 1 {
		t.Fatalf("expected exactly 1 decl, got %d", len(decls))
	}

	// 7. Execute on nil dispatcher
	_, err = nilDisp.Execute(context.Background(), "any_tool", nil)
	if err == nil {
		t.Fatalf("expected error executing on nil dispatcher")
	}

	// 8. Execute with unknown tool that fails JIT discovery
	_, err = malformedDisp.Execute(context.Background(), "completely_unknown_tool", nil)
	if err == nil {
		t.Fatalf("expected error executing unknown tool")
	}

	// 9. Execute with nil args on server returning HTTP 500
	errDisp.toolRoutes["err_tool"] = MCPServerConfig{Name: "err-srv", ServerURL: errServer.URL}
	_, err = errDisp.Execute(context.Background(), "err_tool", nil)
	if err == nil {
		t.Fatalf("expected error executing on error server")
	}

	// 10. Execute on server returning JSON-RPC error
	rpcErrDisp.toolRoutes["failing_tool"] = MCPServerConfig{Name: "rpc-err", ServerURL: rpcErrServer.URL}
	_, err = rpcErrDisp.Execute(context.Background(), "failing_tool", nil)
	if err == nil {
		t.Fatalf("expected error on rpc error response")
	}
}

func TestGeminiAPISession_Send_ToolCallWithoutDispatcher(t *testing.T) {
	t.Parallel()

	step := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		step++
		if step == 1 {
			// Model issues function call, but no dispatcher is configured on pool
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"mystery_tool\",\"args\":{\"x\":1}}}]}}]}\n\n"))
		} else {
			// Final response after tool execution error returned
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Tool failed as expected\"}]}}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":5}}\n\n"))
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})

	sess, err := pool.GetOrCreateSession(context.Background(), "no-disp-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-no-disp",
		Prompt: "call tool please",
		Sink:   sink,
	}

	err = sess.Send("call tool please", turn)
	if err != nil {
		t.Fatalf("expected success with error recovery, got: %v", err)
	}
	if sink.result == nil || sink.result.Response != "Tool failed as expected" {
		t.Errorf("expected final response 'Tool failed as expected', got: %v", sink.result)
	}
}

func TestGeminiAPISession_Send_MalformedSSEChunk(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {malformed_json\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})

	sess, err := pool.GetOrCreateSession(context.Background(), "malformed-sse-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-malformed-sse",
		Prompt: "trigger error",
		Sink:   sink,
	}

	err = sess.Send("trigger error", turn)
	if err == nil {
		t.Fatalf("expected error on malformed SSE chunk")
	}
}

func TestMCPDispatcher_HeadersAndMultiPart(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom-Auth") != "Bearer secret123" {
			http.Error(w, "missing header", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"result": {
				"content": [
					{"type": "text", "text": "line one"},
					{"type": "text", "text": "line two"}
				]
			}
		}`))
	}))
	defer ts.Close()

	disp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{
			Name:      "auth-srv",
			ServerURL: ts.URL,
			Headers:   map[string]string{"X-Custom-Auth": "Bearer secret123"},
		},
	}, ts.Client())
	disp.toolRoutes["multi_tool"] = MCPServerConfig{
		Name:      "auth-srv",
		ServerURL: ts.URL,
		Headers:   map[string]string{"X-Custom-Auth": "Bearer secret123"},
	}

	res, err := disp.Execute(context.Background(), "multi_tool", map[string]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
	expected := "line one\nline two"
	if res != expected {
		t.Errorf("expected %q, got %q", expected, res)
	}
}

func TestGeminiAPISession_HydrateHistory_BlankLines(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	sessID := "blank-lines-sess"
	p := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs", "transcript.jsonl")
	_ = os.MkdirAll(filepath.Dir(p), 0755)

	content := "\n\n  \n{\"type\":\"USER_INPUT\",\"content\":\"hello\"}\n\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"world\"}\n\n"
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}

	sess := &GeminiAPISession{
		targetKey: sessID,
		sessionID: sessID,
		dataDir:   tmpDir,
	}
	sess.hydrateHistory()

	hist := sess.History()
	if len(hist) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(hist))
	}
}

func TestGeminiAPISession_Send_NetworkAndContextErrors(t *testing.T) {
	t.Parallel()

	t.Run("PreCancelledContext", func(t *testing.T) {
		t.Parallel()
		pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey: "test-key",
		})
		sess, err := pool.GetOrCreateSession(context.Background(), "precancel-device", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID: "turn-cancel",
			Prompt: "hi",
			Ctx:    ctx,
			Sink:   sink,
		}
		err = sess.Send("hi", turn)
		if err == nil {
			t.Fatalf("expected error on pre-cancelled context")
		}
		if sink.err == nil {
			t.Errorf("expected sink.OnError to be called")
		}
	})

	t.Run("NetworkDoError", func(t *testing.T) {
		t.Parallel()
		pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey:  "test-key",
			BaseURL: "http://127.0.0.1:1", // closed port causes Do() failure
		})
		sess, err := pool.GetOrCreateSession(context.Background(), "net-err-device", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID: "turn-net-err",
			Prompt: "hi",
			Sink:   sink,
		}
		err = sess.Send("hi", turn)
		if err == nil {
			t.Fatalf("expected error on network failure")
		}
		if sink.err == nil {
			t.Errorf("expected sink.OnError to be called")
		}
	})

	t.Run("CancelledDuringStream", func(t *testing.T) {
		t.Parallel()
		streamStarted := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			close(streamStarted)
			<-r.Context().Done()
		}))
		defer ts.Close()

		pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey:     "test-key",
			BaseURL:    ts.URL,
			HTTPClient: ts.Client(),
		})
		sess, err := pool.GetOrCreateSession(context.Background(), "stream-cancel-device", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-streamStarted
			cancel()
		}()

		sink := newMockTurnSink()
		turn := &TurnContext{
			TurnID: "turn-stream-cancel",
			Prompt: "hi",
			Ctx:    ctx,
			Sink:   sink,
		}
		err = sess.Send("hi", turn)
		if err == nil {
			t.Fatalf("expected error on cancelled stream")
		}
		if sink.err == nil {
			t.Errorf("expected sink.OnError to be called")
		}
	})
}

func TestGeminiAPIPool_GetOrCreateSession_CustomSessionID(t *testing.T) {
	t.Parallel()
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "test-key",
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "custom-target", "custom-sess-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sess.SessionID() != "custom-sess-uuid" {
		t.Errorf("expected session ID 'custom-sess-uuid', got %q", sess.SessionID())
	}
}

func TestGeminiAPIPool_MarkDirty(t *testing.T) {
	t.Parallel()

	// Nil receiver safety
	var nilPool *GeminiAPIPool
	nilPool.MarkDirty() // Should not panic

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "test-key",
		Model:  "gemini-2.5-flash",
	})
	defer pool.Close()

	ctx := context.Background()
	sess1, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-1")
	if err != nil {
		t.Fatalf("unexpected error creating session: %v", err)
	}

	// Session reuse before MarkDirty
	sess1Again, err := pool.GetOrCreateSession(ctx, "kiosk-1", "")
	if err != nil {
		t.Fatalf("unexpected error getting existing session: %v", err)
	}
	if sess1 != sess1Again {
		t.Fatalf("expected identical session instance before MarkDirty")
	}

	// Trigger MarkDirty
	pool.MarkDirty()

	// After MarkDirty, requesting the targetKey should yield a brand new session instance
	sess1AfterDirty, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-new")
	if err != nil {
		t.Fatalf("unexpected error getting session after MarkDirty: %v", err)
	}
	if sess1 == sess1AfterDirty {
		t.Fatalf("expected new session instance after MarkDirty, but got same instance")
	}
	if sess1AfterDirty.SessionID() != "sess-new" {
		t.Errorf("expected new session ID 'sess-new', got %q", sess1AfterDirty.SessionID())
	}

	// MarkDirty on closed pool
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected error closing pool: %v", err)
	}
	pool.MarkDirty() // Should not panic
}

func TestGeminiAPIPool_UpdatePrewarmedTargets(t *testing.T) {
	t.Parallel()

	// Nil receiver safety
	var nilPool *GeminiAPIPool
	nilPool.UpdatePrewarmedTargets([]string{"target-x"}) // Should not panic
	if targets := nilPool.PrewarmedTargets(); targets != nil {
		t.Errorf("expected nil PrewarmedTargets for nil pool, got %v", targets)
	}

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:           "test-key",
		Model:            "gemini-2.5-flash",
		PrewarmedTargets: []string{"initial-1", "initial-2"},
	})
	defer pool.Close()

	initialTargets := pool.PrewarmedTargets()
	if len(initialTargets) != 2 || initialTargets[0] != "initial-1" || initialTargets[1] != "initial-2" {
		t.Fatalf("unexpected initial targets: %v", initialTargets)
	}

	newTargets := []string{"target-alpha", "target-beta", "target-gamma"}
	pool.UpdatePrewarmedTargets(newTargets)

	updated := pool.PrewarmedTargets()
	if len(updated) != 3 || updated[0] != "target-alpha" || updated[1] != "target-beta" || updated[2] != "target-gamma" {
		t.Fatalf("unexpected updated targets: %v", updated)
	}

	// Defensively check that mutating caller's slice doesn't mutate pool's internal state
	poolCopy := pool.PrewarmedTargets()
	poolCopy[0] = "mutated"
	if pool.PrewarmedTargets()[0] == "mutated" {
		t.Errorf("expected PrewarmedTargets to return a defensive copy")
	}

	// Closed pool safety
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected error closing pool: %v", err)
	}
	pool.UpdatePrewarmedTargets([]string{"should-not-apply"})
	if pool.PrewarmedTargets()[0] == "should-not-apply" {
		t.Errorf("UpdatePrewarmedTargets should be noop on closed pool")
	}
}

func TestCleanParameters(t *testing.T) {
	t.Parallel()

	t.Run("Strips_AdditionalProperties_And_Disallowed_Keys", func(t *testing.T) {
		t.Parallel()
		input := map[string]interface{}{
			"$schema":              "http://json-schema.org/draft-07/schema#",
			"title":                "TestSchema",
			"type":                 "object",
			"additionalProperties": false,
			"$ref":                 "#/definitions/foo",
			"$id":                  "https://example.com/schema.json",
			"definitions":          map[string]interface{}{"foo": "bar"},
			"default":              "default_val",
			"properties": map[string]interface{}{
				"prompt": map[string]interface{}{
					"type":                 "string",
					"title":                "Prompt Title",
					"additionalProperties": false,
					"default":              "hello",
					"$id":                  "prompt-id",
				},
				"items_list": map[string]interface{}{
					"type":                 "array",
					"additionalProperties": false,
					"items": map[string]interface{}{
						"type":                 "object",
						"additionalProperties": false,
						"title":                "Item Object",
						"properties": map[string]interface{}{
							"sub": map[string]interface{}{
								"type":                 "string",
								"additionalProperties": false,
							},
						},
					},
				},
				"union_prop": map[string]interface{}{
					"anyOf": []interface{}{
						map[string]interface{}{
							"type":                 "object",
							"additionalProperties": false,
							"properties": map[string]interface{}{
								"branch1": map[string]interface{}{
									"type":                 "integer",
									"additionalProperties": false,
								},
							},
						},
						map[string]interface{}{
							"type":                 "object",
							"additionalProperties": false,
							"properties": map[string]interface{}{
								"branch2": map[string]interface{}{
									"type":                 "boolean",
									"additionalProperties": false,
								},
							},
						},
					},
				},
			},
		}

		res := cleanParameters(input)

		// Helper to recursively check that no disallowed keys exist
		disallowed := []string{"additionalproperties", "$schema", "title", "default", "$ref", "$id", "definitions"}
		var checkKeys func(m map[string]interface{}, path string)
		checkKeys = func(m map[string]interface{}, path string) {
			for k, v := range m {
				for _, dis := range disallowed {
					if strings.EqualFold(k, dis) {
						t.Errorf("found disallowed key %q at path %s", k, path)
					}
				}
				if childMap, ok := v.(map[string]interface{}); ok {
					checkKeys(childMap, path+"."+k)
				} else if childSlice, ok := v.([]interface{}); ok {
					for idx, elem := range childSlice {
						if elemMap, ok := elem.(map[string]interface{}); ok {
							checkKeys(elemMap, fmt.Sprintf("%s.%s[%d]", path, k, idx))
						}
					}
				}
			}
		}

		checkKeys(res, "root")

		// Verify structure remains intact
		if res["type"] != "OBJECT" {
			t.Errorf("expected root type OBJECT, got %v", res["type"])
		}
		props, ok := res["properties"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected properties map, got %T", res["properties"])
		}
		promptProp := props["prompt"].(map[string]interface{})
		if promptProp["type"] != "STRING" {
			t.Errorf("expected prompt type STRING, got %v", promptProp["type"])
		}
	})

	t.Run("Polymorphic_NullString", func(t *testing.T) {
		t.Parallel()
		input := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"optional_text": map[string]interface{}{
					"type": []interface{}{"null", "string"},
				},
				"reversed_null": map[string]interface{}{
					"type": []string{"string", "null"},
				},
			},
		}
		res := cleanParameters(input)
		props := res["properties"].(map[string]interface{})

		opt := props["optional_text"].(map[string]interface{})
		if opt["type"] != "STRING" {
			t.Errorf("expected optional_text type STRING, got %v", opt["type"])
		}
		if opt["nullable"] != true {
			t.Errorf("expected optional_text nullable true, got %v", opt["nullable"])
		}

		rev := props["reversed_null"].(map[string]interface{})
		if rev["type"] != "STRING" {
			t.Errorf("expected reversed_null type STRING, got %v", rev["type"])
		}
		if rev["nullable"] != true {
			t.Errorf("expected reversed_null nullable true, got %v", rev["nullable"])
		}
	})

	t.Run("Polymorphic_NullArray", func(t *testing.T) {
		t.Parallel()
		input := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"optional_list": map[string]interface{}{
					"type": []interface{}{"null", "array"},
					"items": map[string]interface{}{
						"type": "string",
					},
				},
			},
		}
		res := cleanParameters(input)
		props := res["properties"].(map[string]interface{})
		opt := props["optional_list"].(map[string]interface{})
		if opt["type"] != "ARRAY" {
			t.Errorf("expected optional_list type ARRAY, got %v", opt["type"])
		}
		if opt["nullable"] != true {
			t.Errorf("expected optional_list nullable true, got %v", opt["nullable"])
		}
		items := opt["items"].(map[string]interface{})
		if items["type"] != "STRING" {
			t.Errorf("expected items type STRING, got %v", items["type"])
		}
	})

	t.Run("Flatten_Simple_Nullable_Unions", func(t *testing.T) {
		t.Parallel()
		input := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"anyof_field": map[string]interface{}{
					"description": "AnyOf field",
					"anyOf": []interface{}{
						map[string]interface{}{"type": "string"},
						map[string]interface{}{"type": "null"},
					},
				},
				"oneof_field": map[string]interface{}{
					"description": "OneOf field",
					"oneOf": []interface{}{
						map[string]interface{}{"type": "null"},
						map[string]interface{}{"type": "integer"},
					},
				},
			},
		}
		res := cleanParameters(input)
		props := res["properties"].(map[string]interface{})

		anyOfField := props["anyof_field"].(map[string]interface{})
		if anyOfField["type"] != "STRING" {
			t.Errorf("expected anyof_field type STRING, got %v", anyOfField["type"])
		}
		if anyOfField["nullable"] != true {
			t.Errorf("expected anyof_field nullable true, got %v", anyOfField["nullable"])
		}
		if _, exists := anyOfField["anyOf"]; exists {
			t.Errorf("expected anyOf to be removed after flattening")
		}

		oneOfField := props["oneof_field"].(map[string]interface{})
		if oneOfField["type"] != "INTEGER" {
			t.Errorf("expected oneof_field type INTEGER, got %v", oneOfField["type"])
		}
		if oneOfField["nullable"] != true {
			t.Errorf("expected oneof_field nullable true, got %v", oneOfField["nullable"])
		}
		if _, exists := oneOfField["oneOf"]; exists {
			t.Errorf("expected oneOf to be removed after flattening")
		}
	})

	t.Run("DeeplyNestedSchema_Preserved", func(t *testing.T) {
		t.Parallel()
		// Build a 20-level deeply nested schema object
		root := map[string]interface{}{
			"type": "object",
			"additionalProperties": false,
		}
		curr := root
		for i := 0; i < 20; i++ {
			child := map[string]interface{}{
				"type": "object",
				"additionalProperties": false,
			}
			curr["properties"] = map[string]interface{}{
				"nested": child,
			}
			curr = child
		}
		curr["properties"] = map[string]interface{}{
			"leaf": map[string]interface{}{
				"type": "string",
				"additionalProperties": false,
			},
		}

		res := cleanParameters(root)
		if res == nil {
			t.Fatalf("expected non-nil result from deeply nested schema")
		}

		// Traverse and verify all 20 levels are preserved without truncation
		probe := res
		for level := 0; level < 20; level++ {
			if _, hasAddl := probe["additionalProperties"]; hasAddl {
				t.Fatalf("level %d still had additionalProperties", level)
			}
			props, ok := probe["properties"].(map[string]interface{})
			if !ok {
				t.Fatalf("level %d missing properties map", level)
			}
			child, ok := props["nested"].(map[string]interface{})
			if !ok {
				t.Fatalf("level %d missing nested child", level)
			}
			probe = child
		}
		leafProps, ok := probe["properties"].(map[string]interface{})
		if !ok {
			t.Fatalf("leaf parent missing properties")
		}
		leaf, ok := leafProps["leaf"].(map[string]interface{})
		if !ok || leaf["type"] != "STRING" {
			t.Fatalf("leaf not preserved at depth 20: %+v", leaf)
		}
	})

	t.Run("Defaults_EmptySchemas", func(t *testing.T) {
		t.Parallel()

		nilRes := cleanParameters(nil)
		if nilRes["type"] != "OBJECT" {
			t.Errorf("expected nil input to default to OBJECT, got %v", nilRes["type"])
		}
		if nilRes["properties"] == nil {
			t.Errorf("expected non-nil properties map for nil input")
		}

		emptyRes := cleanParameters(map[string]interface{}{})
		if emptyRes["type"] != "OBJECT" {
			t.Errorf("expected empty input to default to OBJECT, got %v", emptyRes["type"])
		}
		if emptyRes["properties"] == nil {
			t.Errorf("expected non-nil properties map for empty input")
		}

		nestedEmpty := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"empty_child": map[string]interface{}{},
			},
		}
		nestedRes := cleanParameters(nestedEmpty)
		props := nestedRes["properties"].(map[string]interface{})
		child := props["empty_child"].(map[string]interface{})
		if child["type"] != "OBJECT" {
			t.Errorf("expected empty child to default to OBJECT, got %v", child["type"])
		}
	})
}

func TestDefaultMCPDispatcher_SessionHandshake(t *testing.T) {
	t.Parallel()

	initCount := int32(0)
	notifyCount := int32(0)
	listCount := int32(0)
	callCount := int32(0)

	const validSessID = "sess-handshake-12345"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string                 `json:"method"`
			ID     interface{}            `json:"id"`
			Params map[string]interface{} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		switch req.Method {
		case "initialize":
			atomic.AddInt32(&initCount, 1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("mcp-session-id", validSessID)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{}}}`))

		case "notifications/initialized":
			atomic.AddInt32(&notifyCount, 1)
			sessHdr := r.Header.Get("mcp-session-id")
			if sessHdr != validSessID {
				http.Error(w, "missing or invalid session header", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		case "tools/list":
			atomic.AddInt32(&listCount, 1)
			sessHdr := r.Header.Get("mcp-session-id")
			if sessHdr != validSessID {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Bad Request: Missing session ID"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "test_tool", "description": "A test tool", "inputSchema": {"type": "object"}}
					]
				}
			}`))

		case "tools/call":
			atomic.AddInt32(&callCount, 1)
			sessHdr := r.Header.Get("mcp-session-id")
			if sessHdr != validSessID {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Bad Request: Missing session ID"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"content": [{"type": "text", "text": "Executed successfully"}]
				}
			}`))

		default:
			http.Error(w, "unknown method", http.StatusNotFound)
		}
	}))
	defer ts.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "stateful-mcp", ServerURL: ts.URL},
	}, ts.Client())

	ctx := context.Background()

	// 1. Initial tools/list triggers 400, then initialize handshake, sets mcp-session-id, receives notifications/initialized, retries and succeeds
	decls, err := dispatcher.ListDeclarations(ctx)
	if err != nil {
		t.Fatalf("ListDeclarations failed: %v", err)
	}
	if len(decls) != 1 || decls[0].Name != "test_tool" {
		t.Fatalf("expected 1 declaration 'test_tool', got %+v", decls)
	}

	if atomic.LoadInt32(&initCount) != 1 {
		t.Errorf("expected exactly 1 initialize call, got %d", atomic.LoadInt32(&initCount))
	}
	if atomic.LoadInt32(&notifyCount) != 1 {
		t.Errorf("expected exactly 1 notification call, got %d", atomic.LoadInt32(&notifyCount))
	}
	// Initial tools/list failed with 400, then retried and succeeded -> total 2 tools/list calls
	if atomic.LoadInt32(&listCount) != 2 {
		t.Errorf("expected 2 tools/list calls (initial + retry), got %d", atomic.LoadInt32(&listCount))
	}

	// 2. Subsequent tool execution reuses cached session ID without extra initialize
	out, err := dispatcher.Execute(ctx, "test_tool", map[string]interface{}{})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if out != "Executed successfully" {
		t.Errorf("expected 'Executed successfully', got %q", out)
	}
	if atomic.LoadInt32(&initCount) != 1 {
		t.Errorf("expected initialize count to remain 1 (reusing session), got %d", atomic.LoadInt32(&initCount))
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("expected 1 tools/call call, got %d", atomic.LoadInt32(&callCount))
	}

	// 3. Concurrent tool executions are thread-safe and share session without race
	const concurrency = 10
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			res, e := dispatcher.Execute(ctx, "test_tool", nil)
			if e != nil {
				errCh <- e
				return
			}
			if res != "Executed successfully" {
				errCh <- fmt.Errorf("unexpected result: %q", res)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < concurrency; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("concurrent execution failed: %v", e)
		}
	}

	// Still only 1 initialize call should have occurred
	if atomic.LoadInt32(&initCount) != 1 {
		t.Errorf("expected initialize count to still be 1 after concurrent calls, got %d", atomic.LoadInt32(&initCount))
	}
}

func TestGeminiAPIPool_MultipleToolCallsGrouping(t *testing.T) {
	t.Parallel()

	tsMCP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "tool_a", "description": "Tool A"},
						{"name": "tool_b", "description": "Tool B"}
					]
				}
			}`))
			return
		}

		if req.Method == "tools/call" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Result of %s"}]}}`, req.Params.Name)))
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer tsMCP.Close()

	geminiReqCount := int32(0)
	var capturedTurn2Contents []geminiContent

	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := atomic.AddInt32(&geminiReqCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if reqNum == 1 {
			// First call: Gemini returns two function calls in a single candidate turn, with thoughtSignature on only the first part
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"tool_a","args":{"arg":"1"}},"thoughtSignature":"sig_shared"},{"functionCall":{"name":"tool_b","args":{"arg":"2"}}}],"role":"model"}}],"index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		} else {
			// Second call: read body to verify grouping
			body, _ := io.ReadAll(r.Body)
			var streamReq geminiStreamRequest
			_ = json.Unmarshal(body, &streamReq)
			capturedTurn2Contents = streamReq.Contents

			chunk := `data: {"candidates":[{"content":{"parts":[{"text":"Both tools finished."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		}
	}))
	defer tsGemini.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{
			{Name: "multi-tool-mcp", ServerURL: tsMCP.URL},
		},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-multi", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-multi-tool",
		Prompt: "run tools",
		Sink:   sink,
	}

	if err := sess.Send("run tools", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if len(sink.toolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %d: %v", len(sink.toolCalls), sink.toolCalls)
	}
	if sink.result == nil || sink.result.Response != "Both tools finished." {
		t.Errorf("expected final response 'Both tools finished.', got %+v", sink.result)
	}

	// Verify that Turn 2 request grouped calls and responses:
	// Contents should be:
	// [0] user: "run tools"
	// [1] model: parts with FunctionCall tool_a and FunctionCall tool_b (grouped in 1 message, preserving thoughtSignature)
	// [2] user: parts with FunctionResponse tool_a and FunctionResponse tool_b (grouped in 1 message)
	if len(capturedTurn2Contents) != 3 {
		t.Fatalf("expected 3 contents in Turn 2 request, got %d: %+v", len(capturedTurn2Contents), capturedTurn2Contents)
	}

	modelContent := capturedTurn2Contents[1]
	if modelContent.Role != "model" {
		t.Errorf("expected content[1].Role == 'model', got %q", modelContent.Role)
	}
	if len(modelContent.Parts) != 2 {
		t.Fatalf("expected 2 parts in modelContent, got %d", len(modelContent.Parts))
	}
	if modelContent.Parts[0].FunctionCall == nil || modelContent.Parts[0].FunctionCall.Name != "tool_a" {
		t.Errorf("expected modelContent.Parts[0] to be tool_a, got %+v", modelContent.Parts[0])
	}
	if modelContent.Parts[0].ThoughtSignature != "sig_shared" {
		t.Errorf("expected modelContent.Parts[0].ThoughtSignature == 'sig_shared', got %q", modelContent.Parts[0].ThoughtSignature)
	}
	if modelContent.Parts[1].FunctionCall == nil || modelContent.Parts[1].FunctionCall.Name != "tool_b" {
		t.Errorf("expected modelContent.Parts[1] to be tool_b, got %+v", modelContent.Parts[1])
	}
	if modelContent.Parts[1].ThoughtSignature != "sig_shared" {
		t.Errorf("expected modelContent.Parts[1].ThoughtSignature == 'sig_shared' (propagated), got %q", modelContent.Parts[1].ThoughtSignature)
	}

	funcContent := capturedTurn2Contents[2]
	if funcContent.Role != "user" {
		t.Errorf("expected content[2].Role == 'user', got %q", funcContent.Role)
	}
	if len(funcContent.Parts) != 2 {
		t.Fatalf("expected 2 parts in funcContent, got %d", len(funcContent.Parts))
	}
	if funcContent.Parts[0].FunctionResponse == nil || funcContent.Parts[0].FunctionResponse.Name != "tool_a" {
		t.Errorf("expected funcContent.Parts[0] to be tool_a, got %+v", funcContent.Parts[0])
	}
	if funcContent.Parts[1].FunctionResponse == nil || funcContent.Parts[1].FunctionResponse.Name != "tool_b" {
		t.Errorf("expected funcContent.Parts[1] to be tool_b, got %+v", funcContent.Parts[1])
	}
}

func TestGeminiAPIPool_EmptyResponseHistoryNotPoisoned(t *testing.T) {
	t.Parallel()

	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Empty chunk with STOP finish reason
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":""}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
	}))
	defer tsGemini.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-empty", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-empty",
		Prompt: "hello",
		Sink:   sink,
	}

	if err := sess.Send("hello", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	apiSess, ok := sess.(*GeminiAPISession)
	if !ok {
		t.Fatalf("expected *GeminiAPISession, got %T", sess)
	}

	// Verify history is empty and not poisoned with empty model parts
	if len(apiSess.history) != 0 {
		t.Errorf("expected history to be empty, got %d items: %+v", len(apiSess.history), apiSess.history)
	}
}

func TestGeminiAPIPool_CleanParametersAndSchemaCoverage(t *testing.T) {
	t.Parallel()

	// 1. Nil schema
	res := cleanParameters(nil)
	if res["type"] != "OBJECT" {
		t.Errorf("expected OBJECT, got %v", res["type"])
	}

	// 2. Schema with empty type or missing type
	res2 := cleanParameters(map[string]interface{}{})
	if res2["type"] != "OBJECT" {
		t.Errorf("expected OBJECT, got %v", res2["type"])
	}

	// 3. Various types and polymorphic type arrays
	testCases := []struct {
		input    map[string]interface{}
		expected string
		nullable bool
	}{
		{
			input:    map[string]interface{}{"type": "int"},
			expected: "INTEGER",
		},
		{
			input:    map[string]interface{}{"type": "bool"},
			expected: "BOOLEAN",
		},
		{
			input:    map[string]interface{}{"type": "float"},
			expected: "NUMBER",
		},
		{
			input:    map[string]interface{}{"type": "double"},
			expected: "NUMBER",
		},
		{
			input:    map[string]interface{}{"type": "null"},
			expected: "OBJECT",
			nullable: true,
		},
		{
			input:    map[string]interface{}{"type": []interface{}{"null", "string"}},
			expected: "STRING",
			nullable: true,
		},
		{
			input:    map[string]interface{}{"type": []interface{}{"null"}},
			expected: "OBJECT",
			nullable: true,
		},
		{
			input:    map[string]interface{}{"type": []string{"null", "int"}},
			expected: "INTEGER",
			nullable: true,
		},
		{
			input:    map[string]interface{}{"type": []string{"null"}},
			expected: "OBJECT",
			nullable: true,
		},
	}

	for _, tc := range testCases {
		cleaned := cleanParameters(tc.input)
		if cleaned["type"] != tc.expected {
			t.Errorf("input %v: expected type %q, got %q", tc.input, tc.expected, cleaned["type"])
		}
		if tc.nullable && cleaned["nullable"] != true {
			t.Errorf("input %v: expected nullable true, got %v", tc.input, cleaned["nullable"])
		}
	}

	// 4. Nested properties and items
	complexSchema := map[string]interface{}{
		"$schema":              "http://json-schema.org/draft-07/schema#",
		"title":                "TestSchema",
		"additionalProperties": false,
		"default":              "val",
		"$ref":                 "#/defs",
		"$id":                  "id1",
		"definitions":          map[string]interface{}{},
		"type":                 "object",
		"properties": map[string]interface{}{
			"str_field": map[string]interface{}{
				"type": "string",
			},
			"raw_field": "not-a-map",
			"items_slice": map[string]interface{}{
				"type": "array",
				"items": []interface{}{
					map[string]interface{}{"type": "int"},
					"not-a-map",
				},
			},
			"items_map": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "bool",
				},
			},
		},
		"allOf": []map[string]interface{}{
			{"type": "string"},
		},
		"anyOf": []interface{}{
			map[string]interface{}{"type": "null"},
			map[string]interface{}{"type": "string"},
		},
	}

	cleanedComplex := cleanParameters(complexSchema)
	if _, exists := cleanedComplex["$schema"]; exists {
		t.Errorf("expected $schema to be stripped")
	}
	if _, exists := cleanedComplex["additionalProperties"]; exists {
		t.Errorf("expected additionalProperties to be stripped")
	}

	// 5. Test oneOf nullable flattening
	oneOfSchema := map[string]interface{}{
		"oneOf": []interface{}{
			map[string]interface{}{"type": "int"},
			map[string]interface{}{"type": "null"},
		},
	}
	cleanedOneOf := cleanParameters(oneOfSchema)
	if cleanedOneOf["type"] != "INTEGER" || cleanedOneOf["nullable"] != true {
		t.Errorf("expected INTEGER nullable, got %+v", cleanedOneOf)
	}

	// 6. Test oneOf with []map[string]interface{}
	oneOfMapSchema := map[string]interface{}{
		"oneOf": []map[string]interface{}{
			{"type": "null"},
			{"type": "bool"},
		},
	}
	cleanedOneOfMap := cleanParameters(oneOfMapSchema)
	if cleanedOneOfMap["type"] != "BOOLEAN" || cleanedOneOfMap["nullable"] != true {
		t.Errorf("expected BOOLEAN nullable, got %+v", cleanedOneOfMap)
	}

	// 7. Test unflattenable anyOf (3 items)
	threeItemsAnyOf := map[string]interface{}{
		"anyOf": []interface{}{
			map[string]interface{}{"type": "string"},
			map[string]interface{}{"type": "int"},
			map[string]interface{}{"type": "null"},
		},
	}
	cleanedThree := cleanParameters(threeItemsAnyOf)
	if _, exists := cleanedThree["anyOf"]; !exists {
		t.Errorf("expected anyOf to remain when len != 2")
	}

	// 8. Test isMissingSessionIDErr helper
	if !isMissingSessionIDErr(400, nil, nil) {
		t.Errorf("expected true for 400 status")
	}
	if !isMissingSessionIDErr(200, []byte("missing session ID"), nil) {
		t.Errorf("expected true for body containing missing session id")
	}
	rpcErr := &jsonRPCResponse{
		Error: &jsonRPCError{
			Code:    -32600,
			Message: "Bad Request",
		},
	}
	if !isMissingSessionIDErr(200, nil, rpcErr) {
		t.Errorf("expected true for RPC error code -32600")
	}
	rpcMsgErr := &jsonRPCResponse{
		Error: &jsonRPCError{
			Code:    -32000,
			Message: "Missing session ID required",
		},
	}
	if !isMissingSessionIDErr(200, nil, rpcMsgErr) {
		t.Errorf("expected true for RPC error message containing missing session id")
	}
	if isMissingSessionIDErr(500, []byte("internal server error"), nil) {
		t.Errorf("expected false for 500 internal server error")
	}
}

func TestMCPDispatcher_Execute_MissingSessionRetry(t *testing.T) {
	t.Parallel()

	initCalls := int32(0)
	execCalls := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		switch req.Method {
		case "initialize":
			atomic.AddInt32(&initCalls, 1)
			w.Header().Set("mcp-session-id", "new-sess-123")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"test_exec","description":"Test Exec"}]}}`))
		case "tools/call":
			callNum := atomic.AddInt32(&execCalls, 1)
			sess := r.Header.Get("mcp-session-id")
			if callNum == 1 || sess != "new-sess-123" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Missing session ID"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Success after retry"}]}}`))
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer ts.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "exec-retry-srv", ServerURL: ts.URL},
	}, ts.Client())

	ctx := context.Background()
	res, err := dispatcher.Execute(ctx, "test_exec", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "Success after retry" {
		t.Errorf("expected 'Success after retry', got %q", res)
	}
	if atomic.LoadInt32(&initCalls) != 1 {
		t.Errorf("expected 1 initialize call, got %d", atomic.LoadInt32(&initCalls))
	}
	if atomic.LoadInt32(&execCalls) != 2 {
		t.Errorf("expected 2 tools/call calls (initial fail + retry), got %d", atomic.LoadInt32(&execCalls))
	}
}

func TestMCPDispatcher_ListDeclarations_ErrorAndRetryBranches(t *testing.T) {
	t.Parallel()

	// 1. Dispatcher is nil
	var nilDisp *DefaultMCPDispatcher
	nilDecls, err := nilDisp.ListDeclarations(context.Background())
	if err != nil || nilDecls != nil {
		t.Errorf("expected nil, nil for nil dispatcher, got %v, %v", nilDecls, err)
	}

	// 2. Server where retry fails
	tsRetryFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-32600,"message":"Missing session ID"}`))
	}))
	defer tsRetryFail.Close()

	dispFail := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "retry-fail-srv", ServerURL: tsRetryFail.URL},
	}, tsRetryFail.Client())

	decls, err := dispFail.ListDeclarations(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(decls) != 0 {
		t.Errorf("expected 0 decls from failing srv, got %d", len(decls))
	}

	// 3. Execute with unknown tool
	if _, err := dispFail.Execute(context.Background(), "non_existent_tool", nil); err == nil {
		t.Errorf("expected error executing non-existent tool")
	}
}

func TestGeminiAPIPool_NormalizeGoogleUserEmail(t *testing.T) {
	t.Parallel()

	var receivedParams map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req struct {
			Method string `json:"method"`
			Params struct {
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "tools/list" {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"get_events","description":"test","inputSchema":{"type":"object"}}]}}`))
			return
		}
		receivedParams = req.Params.Arguments
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"ok"}]}}`))
	}))
	defer ts.Close()

	disp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "google", ServerURL: ts.URL},
	}, ts.Client())

	ctx := context.Background()
	_, _ = disp.ListDeclarations(ctx)

	// Case 1: user_google_email is "primary" -> should be deleted
	receivedParams = nil
	_, err := disp.Execute(ctx, "get_events", map[string]interface{}{
		"user_google_email": "primary",
		"calendar_id":       "primary",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, exists := receivedParams["user_google_email"]; exists {
		t.Errorf("expected user_google_email to be deleted when 'primary', got: %v", receivedParams["user_google_email"])
	}
	if receivedParams["calendar_id"] != "primary" {
		t.Errorf("expected calendar_id to remain 'primary', got: %v", receivedParams["calendar_id"])
	}

	// Case 2: user_google_email is "me" -> should be deleted
	receivedParams = nil
	_, _ = disp.Execute(ctx, "get_events", map[string]interface{}{
		"user_google_email": "me",
	})
	if _, exists := receivedParams["user_google_email"]; exists {
		t.Errorf("expected user_google_email to be deleted when 'me', got: %v", receivedParams["user_google_email"])
	}

	// Case 3: valid email address -> should be preserved
	receivedParams = nil
	_, _ = disp.Execute(ctx, "get_events", map[string]interface{}{
		"user_google_email": "user@example.com",
	})
	if receivedParams["user_google_email"] != "user@example.com" {
		t.Errorf("expected user_google_email to be preserved, got: %v", receivedParams["user_google_email"])
	}
}

func TestGeminiAPIPool_LoadVoiceRulesFromDataDir(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	rulesDir := filepath.Join(tmpDir, "runtimes", "voice", ".gemini", "config", "rules")
	if err := os.MkdirAll(rulesDir, 0755); err != nil {
		t.Fatalf("failed to create rules dir: %v", err)
	}
	ruleContent := "# Test Voice Rules\nBe succinct and direct."
	if err := os.WriteFile(filepath.Join(rulesDir, "00_test.md"), []byte(ruleContent), 0644); err != nil {
		t.Fatalf("failed to write test rule file: %v", err)
	}

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		DataDir: tmpDir,
	})
	defer pool.Close()

	if !strings.Contains(pool.cfg.SystemPrompt, ruleContent) {
		t.Errorf("expected pool.cfg.SystemPrompt to contain rule content, got: %q", pool.cfg.SystemPrompt)
	}
}

type fallbackMockDispatcher struct{}

func (d *fallbackMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return []geminiFunctionDeclaration{
		{Name: "test_tool", Description: "a test tool"},
	}, nil
}

func (d *fallbackMockDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	return `{"status":"ok"}`, nil
}

func TestGeminiAPIPool_FallbackTextWhenToolIterationsExhausted(t *testing.T) {
	t.Parallel()

	requestCount := 0
	var finalReqBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		requestCount++

		body, _ := io.ReadAll(r.Body)

		if requestCount == 1 {
			// First call: returns a function call with empty text
			sse := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"test_tool\",\"args\":{\"foo\":\"bar\"}}}]}}]}\n\n"
			_, _ = w.Write([]byte(sse))
		} else if requestCount == 2 {
			// Second call: tool loop iteration returns empty response without text or tool calls
			sse := "data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}\n\n"
			_, _ = w.Write([]byte(sse))
		} else {
			// Third call: final fallback call (without tools in request)
			finalReqBody = body
			sse := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Here is your calendar summary.\"}]}}]}\n\n"
			_, _ = w.Write([]byte(sse))
		}
	}))
	defer ts.Close()

	disp := &fallbackMockDispatcher{}

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:        "test-key",
		Model:         "gemini-2.5-flash",
		BaseURL:       ts.URL,
		HTTPClient:    ts.Client(),
		MCPDispatcher: disp,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-exhaust-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID:    "turn-exhaust",
		Prompt:    "what is on my calendar?",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	if err := sess.Send("what is on my calendar?", turn); err != nil {
		t.Fatalf("unexpected error from Send: %v", err)
	}

	sink.mu.Lock()
	collectedText := strings.Join(sink.deltas, "")
	sink.mu.Unlock()

	if !strings.Contains(collectedText, "Here is your calendar summary.") {
		t.Errorf("expected fallback text in sink, got: %q", collectedText)
	}

	// Verify final request body does not have "tools" declared
	if strings.Contains(string(finalReqBody), "\"tools\":[") {
		t.Errorf("expected final fallback request body to not include tools, got: %s", string(finalReqBody))
	}
}

func newAllowedToolsMockServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name      string                 `json:"name"`
				Arguments map[string]interface{} `json:"arguments"`
			} `json:"params"`
		}
		_ = json.Unmarshal(body, &req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "ha_call_read_tool", "description": "Read HA entities"},
						{"name": "get_events", "description": "Get calendar events"},
						{"name": "batch_modify_gmail_message_labels", "description": "Batch modify labels"}
					]
				}
			}`))
			return
		}

		if req.Method == "tools/call" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jsonrpc": "2.0",
				"id": 2,
				"result": {
					"content": [{"type": "text", "text": "executed: ` + req.Params.Name + `"}]
				}
			}`))
			return
		}

		http.Error(w, "unknown method", http.StatusBadRequest)
	}))
}

func TestDefaultMCPDispatcher_AllowedToolsFilteringAndExecution(t *testing.T) {
	t.Parallel()

	t.Run("FilteringAndGatingActive", func(t *testing.T) {
		t.Parallel()
		ts := newAllowedToolsMockServer()
		defer ts.Close()

		ctx := context.Background()
		disp := NewDefaultMCPDispatcher(
			[]MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}},
			ts.Client(),
			[]string{"ha_call_read_tool", "get_events"},
		)

		decls, err := disp.ListDeclarations(ctx)
		if err != nil {
			t.Fatalf("unexpected error listing declarations: %v", err)
		}
		if len(decls) != 2 {
			t.Fatalf("expected exactly 2 allowed declarations, got %d", len(decls))
		}
		names := map[string]bool{decls[0].Name: true, decls[1].Name: true}
		if !names["ha_call_read_tool"] || !names["get_events"] {
			t.Errorf("expected ha_call_read_tool and get_events, got: %v", names)
		}
		if names["batch_modify_gmail_message_labels"] {
			t.Errorf("disallowed tool batch_modify_gmail_message_labels was not filtered out")
		}

		// Allowed execution should succeed
		res, err := disp.Execute(ctx, "ha_call_read_tool", nil)
		if err != nil {
			t.Fatalf("unexpected error executing allowed tool: %v", err)
		}
		if !strings.Contains(res, "executed: ha_call_read_tool") {
			t.Errorf("unexpected execute result: %q", res)
		}

		// Disallowed execution must be gated and rejected
		_, err = disp.Execute(ctx, "batch_modify_gmail_message_labels", nil)
		if err == nil {
			t.Fatalf("expected error executing disallowed tool, got nil")
		}
		if !strings.Contains(err.Error(), "not in the allowed voice tools list") {
			t.Errorf("expected 'not in the allowed voice tools list' error, got: %v", err)
		}
	})

	t.Run("OmittedOrNilAllowedToolsAllowsAll", func(t *testing.T) {
		t.Parallel()
		ts := newAllowedToolsMockServer()
		defer ts.Close()

		ctx := context.Background()
		// Omitted or nil allowedTools allows all tools (backward compatibility)
		disp := NewDefaultMCPDispatcher(
			[]MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}},
			ts.Client(),
			nil,
		)

		decls, err := disp.ListDeclarations(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(decls) != 3 {
			t.Fatalf("expected 3 declarations with unconstrained allowlist, got %d", len(decls))
		}

		res, err := disp.Execute(ctx, "batch_modify_gmail_message_labels", nil)
		if err != nil {
			t.Fatalf("unexpected error executing tool: %v", err)
		}
		if !strings.Contains(res, "executed: batch_modify_gmail_message_labels") {
			t.Errorf("unexpected execute result: %q", res)
		}
	})

	t.Run("ExplicitEmptyAllowedToolsBlocksAll", func(t *testing.T) {
		t.Parallel()
		ts := newAllowedToolsMockServer()
		defer ts.Close()

		ctx := context.Background()
		// Explicit empty slice ([]string{}) blocks all tools (lockdown)
		disp := NewDefaultMCPDispatcher(
			[]MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}},
			ts.Client(),
			[]string{},
		)

		decls, err := disp.ListDeclarations(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(decls) != 0 {
			t.Fatalf("expected 0 declarations for explicit empty allowlist, got %d", len(decls))
		}

		_, err = disp.Execute(ctx, "ha_call_read_tool", nil)
		if err == nil {
			t.Fatalf("expected error executing tool when allowlist is empty, got nil")
		}
		if !strings.Contains(err.Error(), "not in the allowed voice tools list") {
			t.Errorf("expected 'not in the allowed voice tools list' error, got: %v", err)
		}
	})

	t.Run("NewGeminiAPIPoolWiring", func(t *testing.T) {
		t.Parallel()
		ts := newAllowedToolsMockServer()
		defer ts.Close()

		ctx := context.Background()
		pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey:       "key",
			Model:        "gemini-2.5-flash",
			HTTPClient:   ts.Client(),
			MCPServers:   []MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}},
			AllowedTools: []string{"get_events"},
		})
		defer pool.Close()

		disp, ok := pool.cfg.MCPDispatcher.(*DefaultMCPDispatcher)
		if !ok {
			t.Fatalf("expected DefaultMCPDispatcher in pool")
		}
		decls, err := disp.ListDeclarations(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(decls) != 1 || decls[0].Name != "get_events" {
			t.Errorf("expected 1 declaration for get_events, got: %v", decls)
		}
	})
}

func TestGeminiAPIPool_NilReceiverAndDefaultClient(t *testing.T) {
	t.Parallel()

	var nilPool *GeminiAPIPool
	if nilPool.Model() != "" {
		t.Errorf("expected empty Model() for nil pool")
	}
	if nilPool.APIKey() != "" {
		t.Errorf("expected empty APIKey() for nil pool")
	}
	if targets := nilPool.PrewarmedTargets(); targets != nil {
		t.Errorf("expected nil PrewarmedTargets() for nil pool")
	}

	// Default HTTPClient coverage
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "key",
		Model:  "gemini-2.5-flash",
	})
	if pool.cfg.HTTPClient == nil {
		t.Errorf("expected default HTTPClient to be initialized")
	}
	_ = pool.Close()
}

func TestGeminiAPISession_Send_WithMemoryRetriever(t *testing.T) {
	t.Parallel()

	retriever := func(ctx context.Context, query string) (string, error) {
		return "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>", nil
	}

	var receivedBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		receivedBody = b
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Got your preference!\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MemoryRetriever: retriever,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-sess-memory", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	geminiSess := sess.(*GeminiAPISession)
	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-1",
		Prompt: "What is my theme preference?",
		Sink:   sink,
	}
	err = sess.Send("What is my theme preference?", turn)
	if err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	// 1. Verify payload sent to API endpoint contains <retrieved_memory>
	var req geminiStreamRequest
	if err := json.Unmarshal(receivedBody, &req); err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}
	if len(req.Contents) == 0 || len(req.Contents[0].Parts) == 0 {
		t.Fatalf("expected at least 1 content part in request")
	}
	firstText := req.Contents[0].Parts[0].Text
	if !strings.Contains(firstText, "<retrieved_memory>") || !strings.Contains(firstText, "User prefers dark mode") {
		t.Errorf("expected payload to contain retrieved memory, got: %s", firstText)
	}
	if !strings.Contains(firstText, "What is my theme preference?") {
		t.Errorf("expected payload to contain user prompt, got: %s", firstText)
	}

	// 2. Verify s.History() contains clean prompt without <retrieved_memory> AND contains resText!
	hist := geminiSess.History()
	if len(hist) != 2 {
		t.Fatalf("expected 2 history items, got %d", len(hist))
	}
	if hist[0].Role != "user" || strings.Contains(hist[0].Parts[0].Text, "<retrieved_memory>") || hist[0].Parts[0].Text != "What is my theme preference?" {
		t.Errorf("expected clean prompt in user history, got: %+v", hist[0])
	}
	if hist[1].Role != "model" || hist[1].Parts[0].Text != "Got your preference!" {
		t.Errorf("expected model response in history, got: %+v", hist[1])
	}
}

func TestGeminiAPISession_Send_MemoryRetriever_NoDoubleInject(t *testing.T) {
	t.Parallel()

	retrieverCalled := false
	retriever := func(ctx context.Context, query string) (string, error) {
		retrieverCalled = true
		return "<retrieved_memory>\n- Extra memory\n</retrieved_memory>", nil
	}

	var receivedBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = b
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Acknowledged\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MemoryRetriever: retriever,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-sess-no-double", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	promptWithMemory := "<retrieved_memory>\n- Already present\n</retrieved_memory>\n\nHello"
	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-no-double",
		Prompt: promptWithMemory,
		Sink:   sink,
	}
	if err := sess.Send(promptWithMemory, turn); err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	if retrieverCalled {
		t.Errorf("expected MemoryRetriever not to be called when prompt already has <retrieved_memory>")
	}
	var noDoubleReq geminiStreamRequest
	if err := json.Unmarshal(receivedBody, &noDoubleReq); err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}
	if len(noDoubleReq.Contents) == 0 || len(noDoubleReq.Contents[0].Parts) == 0 {
		t.Fatalf("expected at least 1 content part in request")
	}
	firstReqText := noDoubleReq.Contents[0].Parts[0].Text
	if strings.Count(firstReqText, "<retrieved_memory>") != 1 {
		t.Errorf("expected exactly 1 <retrieved_memory> block, got: %s", firstReqText)
	}
}


