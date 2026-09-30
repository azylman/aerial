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

func TestGeminiVoiceInterfaces_Satisfaction(t *testing.T) {
	t.Parallel()

	var _ VoiceProcessPool = (*GeminiVoicePool)(nil)
	var _ VoiceSession = (*GeminiVoiceSession)(nil)
}

func TestGeminiVoicePool_StreamingSuccess(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	ctx := context.Background()
	sess, err := pool.GetOrCreateSession(ctx, "kiosk-living-room")
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
	geminiSess := sess.(*GeminiVoiceSession)
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

func TestGeminiVoicePool_ZeroThinkingBudget(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:       "key-abc",
		Model:        "gemini-2.5-flash",
		BaseURL:      ts.URL,
		HTTPClient:   ts.Client(),
		SystemPrompt: "You are Aerial.",
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-device")
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

func TestGeminiVoicePool_SlidingWindowHistory(t *testing.T) {
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
	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:          "history-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxHistoryTurns: 10,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "history-device")
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

	geminiSess := sess.(*GeminiVoiceSession)
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

func TestGeminiVoicePool_ContextCancellation(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "cancel-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "cancel-device")
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

func TestGeminiVoicePool_RateLimit429(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`))
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "ratelimit-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "rate-limited-device")
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

func TestGeminiVoicePool_EmptyPrompt(t *testing.T) {
	t.Parallel()

	var requestCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "device-empty")
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

func TestGeminiVoicePool_PrewarmedAndClose(t *testing.T) {
	t.Parallel()

	targets := []string{"living-room", "kitchen", "office"}
	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
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
		sess, err := pool.GetOrCreateSession(ctx, target)
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
	if _, err := pool.GetOrCreateSession(ctx, "new-target"); err == nil {
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

func TestGeminiVoicePool_SanitizeAPIKey(t *testing.T) {
	t.Parallel()

	secretKey := "AIzaSySuperSecretKey123456789"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Server returns 500 with body containing the key
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"internal error occurred with key %s"}`, secretKey)))
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     secretKey,
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "sanitize-device")
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

func TestGeminiVoicePool_Accessors(t *testing.T) {
	t.Parallel()

	var nilPool *GeminiVoicePool
	if nilPool.Model() != "" {
		t.Errorf("expected empty string from nilPool.Model()")
	}
	if nilPool.APIKey() != "" {
		t.Errorf("expected empty string from nilPool.APIKey()")
	}

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey: "my-api-key",
		Model:  "gemini-2.5-pro",
	})
	if pool.Model() != "gemini-2.5-pro" {
		t.Errorf("expected model 'gemini-2.5-pro', got %q", pool.Model())
	}
	if pool.APIKey() != "my-api-key" {
		t.Errorf("expected APIKey 'my-api-key', got %q", pool.APIKey())
	}

	sess, err := pool.GetOrCreateSession(context.Background(), "target-123")
	if err != nil {
		t.Fatalf("unexpected error creating session: %v", err)
	}
	geminiSess, ok := sess.(*GeminiVoiceSession)
	if !ok {
		t.Fatalf("expected *GeminiVoiceSession, got %T", sess)
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

func TestGeminiVoicePool_WithMCPToolCalling(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{
			{Name: "ha-mcp", ServerURL: tsMCP.URL},
		},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk")
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

func TestGeminiVoicePool_WithMCPToolCalling_ErrorRecovery(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    tsGemini.URL,
		HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{
			{Name: "broken-mcp", ServerURL: tsMCP.URL},
		},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-err")
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

func TestGeminiVoiceSession_TranscriptPersistenceAndHydration(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Persisted response\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
		DataDir:    tmpDir,
	})
	defer pool.Close()

	sessID := "persisted-session"
	sess, err := pool.GetOrCreateSession(context.Background(), sessID)
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
	freshSession := &GeminiVoiceSession{
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

func TestGeminiVoicePool_EdgeCasesAndCoverage(t *testing.T) {
	t.Parallel()

	// 1. sanitizeError with nil
	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{})
	if err := pool.sanitizeError(nil); err != nil {
		t.Errorf("expected sanitizeError(nil) == nil, got %v", err)
	}

	// 2. SessionID fallback when sessionID is empty
	sess := &GeminiVoiceSession{targetKey: "fallback-target"}
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

func TestGeminiVoicePool_Send_WithNilSinkAndErrors(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "nil-sink-device")
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

func TestGeminiVoicePool_Send_SSEMalformedChunk(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {malformed_json\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "malformed-sse")
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

func TestGeminiVoicePool_Send_DispatcherErrors(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Fallback response\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		APIKey:        "test-key",
		Model:         "gemini-2.5-flash",
		BaseURL:       ts.URL,
		HTTPClient:    ts.Client(),
		MCPDispatcher: &errMockDispatcher{},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "dispatcher-err-device")
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

func TestGeminiVoiceSession_HistoryPruningLongTranscript(t *testing.T) {
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

	pool := NewGeminiVoicePool(GeminiVoicePoolConfig{
		DataDir: tmpDir,
	})
	sess := &GeminiVoiceSession{
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


