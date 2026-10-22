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

func (s *GeminiAPISession) TargetKey() string { return s.targetKey }

func (s *GeminiAPISession) History() []geminiContent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]geminiContent(nil), s.history...)
}

func TestGeminiAPIInterfaces_Satisfaction(t *testing.T) {
	t.Parallel()

	var _ AgentPool = (*GeminiAPIPool)(nil)
	var _ AgentSession = (*GeminiAPISession)(nil)
}

func TestGeminiAPIPool_ContextCancellation(t *testing.T) {
	t.Parallel()
	releaseServer, flushedCh := make(chan struct{}), make(chan struct{})
	defer close(releaseServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Partial\"}],\"role\":\"model\"}}]}\n\n"))
			f.Flush()
			select {
			case <-flushedCh:
			default:
				close(flushedCh)
			}
		}
		select {
		case <-r.Context().Done():
		case <-releaseServer:
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "cancel-key", Model: "gemini-2.5-flash", BaseURL: ts.URL, HTTPClient: ts.Client()})
	defer pool.Close()
	sess, err := pool.GetOrCreateSession(context.Background(), "cancel-device", "")
	if err != nil {
		t.Fatalf("unexpected error getting session: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sink, doneCh := newMockTurnSink(), make(chan error, 1)
	go func() {
		doneCh <- sess.Send("Prompt to cancel", &TurnContext{TurnID: "turn-cancel", Prompt: "Prompt to cancel", Sink: sink, CreatedAt: time.Now(), Ctx: ctx})
	}()

	select {
	case <-flushedCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for initial flush signal")
	}
	cancel()

	select {
	case sendErr := <-doneCh:
		if sendErr == nil {
			t.Fatalf("expected error from cancelled Send, got nil")
		}
		sink.mu.Lock()
		defer sink.mu.Unlock()
		if sink.err == nil {
			t.Errorf("expected error delivered to sink.OnError")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for Send to terminate after context cancellation")
	}
}

type sendTestCase struct {
	name, serverMode, device, prompt, expectErrMsg, expectSinkErrMsg, expectSinkResult, expectHistFirst, expectHistLast, verifySanitizeKey string
	cfg                                                                                                                                    GeminiAPIPoolConfig
	preCancelCtx, badURL, cancelOnFlush, nilSink, expectErr, checkThinkingBody, expectEmptyHist                                            bool
	expectDeltas                                                                                                                           []string
	expectPromptToken, expectCandToken, expectTotalToken, runSequential, expectMaxHistory                                                  int
}

func makeSendSuiteHandler(tc sendTestCase, lastReqBody *[]byte) http.HandlerFunc {
	if tc.serverMode == "" {
		return nil
	}
	var turnCounter int32
	return func(w http.ResponseWriter, r *http.Request) {
		if tc.checkThinkingBody && lastReqBody != nil {
			*lastReqBody, _ = io.ReadAll(r.Body)
		}
		switch tc.serverMode {
		case "streaming_success":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Hello "}],"role":"model"}}],"index":0}` + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"world!"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":6,"totalTokenCount":18}}` + "\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		case "zero_thinking":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Thinking budget tested"}],"role":"model"}}]}` + "\n\n"))
		case "sliding_window":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fmt.Sprintf(`data: {"candidates":[{"content":{"parts":[{"text":"Response %d"}],"role":"model"}}]}`+"\n\n", atomic.AddInt32(&turnCounter, 1))))
		case "rate_limit_429":
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`))
		case "sanitize_key":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal error occurred with key AIzaSySuperSecretKey123456789"}`))
		case "server_error":
			http.Error(w, "server error", http.StatusInternalServerError)
		case "malformed_sse":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {malformed_json\n\n"))
		case "cancel_on_flush":
			w.Header().Set("Content-Type", "text/event-stream")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case "empty_response":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":""}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"))
		}
	}
}

func TestGeminiAPIPool_Send_Suite(t *testing.T) {
	t.Parallel()
	tests := []sendTestCase{
		{name: "StreamingSuccess", device: "kiosk-living-room", prompt: "Say hello", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, serverMode: "streaming_success", expectDeltas: []string{"Hello ", "world!"}, expectSinkResult: "Hello world!", expectPromptToken: 12, expectCandToken: 6, expectTotalToken: 18},
		{name: "ZeroThinkingBudget", device: "voice-device", prompt: "Check thinking budget", cfg: GeminiAPIPoolConfig{APIKey: "key-abc", Model: "gemini-2.5-flash", SystemPrompt: "You are Aerial."}, serverMode: "zero_thinking", checkThinkingBody: true, expectSinkResult: "Thinking budget tested"},
		{name: "SlidingWindowHistory", device: "history-device", prompt: "Prompt", cfg: GeminiAPIPoolConfig{APIKey: "history-key", Model: "gemini-2.5-flash", MaxHistoryTurns: 10}, serverMode: "sliding_window", runSequential: 15, expectMaxHistory: 20, expectHistFirst: "Prompt 6", expectHistLast: "Response 15"},
		{name: "RateLimit429", device: "rate-limited-device", prompt: "Hello under limit", cfg: GeminiAPIPoolConfig{APIKey: "ratelimit-key", Model: "gemini-2.5-flash"}, serverMode: "rate_limit_429", expectErr: true, expectErrMsg: "rate limit exceeded: please try again shortly", expectSinkErrMsg: "rate limit exceeded: please try again shortly"},
		{name: "EmptyPrompt_Empty", device: "device-empty-1", prompt: "", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, expectErr: true},
		{name: "EmptyPrompt_Whitespace", device: "device-empty-2", prompt: "   \t\n\r  ", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, expectErr: true},
		{name: "SanitizeAPIKey", device: "sanitize-device", prompt: "trigger error", cfg: GeminiAPIPoolConfig{APIKey: "AIzaSySuperSecretKey123456789", Model: "gemini-2.5-flash"}, serverMode: "sanitize_key", expectErr: true, verifySanitizeKey: "AIzaSySuperSecretKey123456789"},
		{name: "Send_WithNilSinkAndServerError", device: "nil-sink-device", prompt: "hello", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, serverMode: "server_error", nilSink: true, expectErr: true},
		{name: "Send_WithNilSinkAndCancelledCtx", device: "nil-sink-cancel", prompt: "hello", preCancelCtx: true, cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, nilSink: true, expectErr: true},
		{name: "Send_SSEMalformedChunk", device: "malformed-sse", prompt: "trigger malformed", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, serverMode: "malformed_sse", expectErr: true},
		{name: "Send_PreCancelledContext", device: "precancel-device", prompt: "hi", preCancelCtx: true, cfg: GeminiAPIPoolConfig{APIKey: "test-key"}, expectErr: true},
		{name: "Send_NetworkDoError", device: "net-err-device", prompt: "hi", badURL: true, cfg: GeminiAPIPoolConfig{APIKey: "test-key"}, expectErr: true},
		{name: "Send_CancelledDuringStream", device: "stream-cancel-device", prompt: "hi", cancelOnFlush: true, cfg: GeminiAPIPoolConfig{APIKey: "test-key"}, serverMode: "cancel_on_flush", expectErr: true},
		{name: "EmptyResponse_NotPoisoned", device: "voice-kiosk-empty", prompt: "empty test", cfg: GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"}, serverMode: "empty_response", expectEmptyHist: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var lastReqBody []byte
			var ts *httptest.Server
			if h := makeSendSuiteHandler(tc, &lastReqBody); h != nil {
				ts = httptest.NewServer(h)
				defer ts.Close()
			}
			cfg := tc.cfg
			if ts != nil {
				cfg.BaseURL, cfg.HTTPClient = ts.URL, ts.Client()
			} else if tc.badURL {
				cfg.BaseURL = "http://127.0.0.1:1"
			}
			pool := NewGeminiAPIPool(cfg)
			defer pool.Close()
			ctx := context.Background()
			sess, err := pool.GetOrCreateSession(ctx, tc.device, "")
			if err != nil {
				t.Fatalf("unexpected GetOrCreateSession error: %v", err)
			}

			if tc.runSequential > 0 {
				for i := 1; i <= tc.runSequential; i++ {
					p := fmt.Sprintf("%s %d", tc.prompt, i)
					sink := newMockTurnSink()
					if err := sess.Send(p, &TurnContext{TurnID: fmt.Sprintf("turn-%d", i), Prompt: p, Sink: sink}); err != nil {
						t.Fatalf("turn %d failed: %v", i, err)
					}
				}
				hist := sess.(*GeminiAPISession).History()
				if len(hist) != tc.expectMaxHistory || (tc.expectHistFirst != "" && hist[0].Parts[0].Text != tc.expectHistFirst) || (tc.expectHistLast != "" && hist[len(hist)-1].Parts[0].Text != tc.expectHistLast) {
					t.Errorf("history mismatch: len=%d, first=%q, last=%q", len(hist), hist[0].Parts[0].Text, hist[len(hist)-1].Parts[0].Text)
				}
				return
			}

			turnCtx := ctx
			if tc.preCancelCtx {
				cCtx, cancel := context.WithCancel(ctx)
				cancel()
				turnCtx = cCtx
			} else if tc.cancelOnFlush {
				cCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(20*time.Millisecond, cancel)
				turnCtx = cCtx
			}

			var sink *mockTurnSink
			var sinkInterface TurnSink
			if !tc.nilSink {
				sink = newMockTurnSink()
				sinkInterface = sink
			}

			sendErr := sess.Send(tc.prompt, &TurnContext{TurnID: "turn-suite", Prompt: tc.prompt, Sink: sinkInterface, CreatedAt: time.Now(), Ctx: turnCtx})
			if tc.expectErr {
				if sendErr == nil || (tc.expectErrMsg != "" && sendErr.Error() != tc.expectErrMsg) {
					t.Fatalf("expected error %q, got: %v", tc.expectErrMsg, sendErr)
				}
				if sink != nil {
					sink.mu.Lock()
					sErr := sink.err
					sink.mu.Unlock()
					if sErr == nil || (tc.expectSinkErrMsg != "" && sErr.Error() != tc.expectSinkErrMsg) {
						t.Errorf("sink error mismatch: expected %q, got %v", tc.expectSinkErrMsg, sErr)
					}
				}
				if tc.verifySanitizeKey != "" {
					if strings.Contains(sendErr.Error(), tc.verifySanitizeKey) || !strings.Contains(sendErr.Error(), "[REDACTED]") {
						t.Errorf("send error contains unredacted API key: %s", sendErr.Error())
					}
					sanitized := pool.sanitizeError(fmt.Errorf("connection failed to https://generativelanguage.googleapis.com/v1beta/models/gemini:stream?key=%s&alt=sse: timeout", tc.verifySanitizeKey))
					if strings.Contains(sanitized.Error(), tc.verifySanitizeKey) || !strings.Contains(sanitized.Error(), "key=[REDACTED]") {
						t.Errorf("sanitized query error still contains secret key: %s", sanitized.Error())
					}
				}
				return
			}

			if sendErr != nil {
				t.Fatalf("unexpected Send error: %v", sendErr)
			}
			if tc.expectEmptyHist {
				if hist := sess.(*GeminiAPISession).History(); len(hist) != 0 {
					t.Errorf("expected empty history, got %d", len(hist))
				}
				return
			}
			if sink != nil {
				sink.mu.Lock()
				defer sink.mu.Unlock()
				if !sink.started || (tc.expectSinkResult != "" && (sink.result == nil || sink.result.Response != tc.expectSinkResult)) {
					t.Errorf("sink state mismatch: started=%v, result=%+v", sink.started, sink.result)
				}
				if len(tc.expectDeltas) > 0 && len(sink.deltas) != len(tc.expectDeltas) {
					t.Fatalf("expected %d deltas, got %d: %v", len(tc.expectDeltas), len(sink.deltas), sink.deltas)
				}
				if (tc.expectPromptToken > 0 && sink.result.Usage.InputTokens != tc.expectPromptToken) ||
					(tc.expectCandToken > 0 && sink.result.Usage.OutputTokens != tc.expectCandToken) ||
					(tc.expectTotalToken > 0 && sink.result.Usage.TotalTokens != tc.expectTotalToken) {
					t.Errorf("usage mismatch: %+v", sink.result.Usage)
				}
			}
			if tc.checkThinkingBody {
				rawJSON := string(lastReqBody)
				if (!strings.Contains(rawJSON, `"thinkingBudget":0`) && !strings.Contains(rawJSON, `"thinkingBudget": 0`)) || !strings.Contains(rawJSON, "You are Aerial.") {
					t.Errorf("expected thinkingBudget: 0 and systemInstruction, got %s", rawJSON)
				}
			}
		})
	}
}

func TestGeminiAPIPool_AccessorsAndNilPool(t *testing.T) {
	t.Parallel()
	var nilPool *GeminiAPIPool
	if nilPool.PrewarmedTargets() != nil || nilPool.sanitizeError(nil) != nil {
		t.Errorf("expected empty/nil from nilPool accessors")
	}
	nilPool.MarkDirty()
	nilPool.UpdatePrewarmedTargets([]string{"t"})

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "my-api-key", Model: "gemini-2.5-pro", PrewarmedTargets: []string{"initial-1", "initial-2"},
	})
	defer pool.Close()
	if pool.cfg.HTTPClient == nil {
		t.Errorf("expected non-nil HTTPClient")
	}
	sess, err := pool.GetOrCreateSession(context.Background(), "target-123", "custom-uuid")
	if err != nil || sess.(*GeminiAPISession).TargetKey() != "target-123" || sess.SessionID() != "custom-uuid" {
		t.Errorf("unexpected session: %v, %v", sess, err)
	}
	targets := pool.PrewarmedTargets()
	if len(targets) != 2 || targets[0] != "initial-1" {
		t.Fatalf("unexpected prewarmed targets: %v", targets)
	}
	targets[0] = "mutated"
	if pool.PrewarmedTargets()[0] == "mutated" {
		t.Errorf("expected defensive copy for PrewarmedTargets")
	}
	pool.UpdatePrewarmedTargets([]string{"alpha", "beta"})
	if targets := pool.PrewarmedTargets(); len(targets) != 2 || targets[0] != "alpha" {
		t.Errorf("failed to update prewarmed targets: %v", targets)
	}
}

func TestGeminiAPIPool_MarkDirtySessionRecreation(t *testing.T) {
	t.Parallel()
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash"})
	defer pool.Close()
	ctx := context.Background()
	s1, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if s1Again, err := pool.GetOrCreateSession(ctx, "kiosk-1", ""); err != nil || s1 != s1Again {
		t.Fatalf("expected identical session instance before MarkDirty")
	}
	pool.MarkDirty()
	if s1After, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-2"); err != nil || s1 == s1After || s1After.SessionID() != "sess-2" {
		t.Fatalf("expected new session after MarkDirty")
	}
}

func TestGeminiAPIPool_PrewarmedAndClose(t *testing.T) {
	t.Parallel()
	prewarmedTargets := []string{"living-room", "kitchen", "office"}
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash", PrewarmedTargets: prewarmedTargets})
	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
	for _, target := range prewarmedTargets {
		if sess, err := pool.GetOrCreateSession(ctx, target, ""); err != nil || sess.SessionID() != target {
			t.Errorf("unexpected prewarmed session for %s: %v", target, err)
		}
	}
	sessBeforeClose := pool.sessions["living-room"]
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if _, err := pool.GetOrCreateSession(ctx, "new-target", ""); err == nil {
		t.Errorf("expected error on closed pool GetOrCreateSession")
	}
	if err := pool.Initialize(ctx); err == nil {
		t.Errorf("expected error on closed pool Initialize")
	}
	pool.UpdatePrewarmedTargets([]string{"noop"})
	pool.MarkDirty()
	if err := sessBeforeClose.Send("hello", &TurnContext{TurnID: "turn-closed", Prompt: "hello", Sink: newMockTurnSink()}); err == nil {
		t.Errorf("expected error calling Send on closed pool session")
	}
}

func TestGeminiAPISession_EdgeCases(t *testing.T) {
	t.Parallel()
	sess := &GeminiAPISession{targetKey: "fallback-target"}
	if sess.SessionID() != "fallback-target" || sess.transcriptPath() != "" {
		t.Errorf("expected fallback session ID and empty transcript path")
	}
}

type errMockDispatcher struct{}

func (d *errMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return nil, errors.New("declarations query failed")
}

func (d *errMockDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	return "", errors.New("execute failed")
}

type toolCallingTestCase struct {
	name, mcpMode, geminiMode, expectToolCall, expectResponseSub string
	useBrokenDisp, omitDisp, expectErr                           bool
}

func makeToolCallingMCPHandler(mode string) http.HandlerFunc {
	switch mode {
	case "ha_light":
		return func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Method string `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			w.Header().Set("Content-Type", "application/json")
			if req.Method == "tools/list" {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"ha_call_write_tool","description":"Call HA service","inputSchema":{"type":"object","properties":{"entity_id":{"type":"string"}}}}]}}`))
			} else if req.Method == "tools/call" {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"light.living_room turned off"}]}}`))
			}
		}
	case "crash_on_call":
		return func(w http.ResponseWriter, r *http.Request) {
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
		}
	default:
		return nil
	}
}

func makeToolCallingGeminiHandler(mode string) http.HandlerFunc {
	var callCount int32
	return func(w http.ResponseWriter, r *http.Request) {
		step := atomic.AddInt32(&callCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		switch mode {
		case "tool_then_text":
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"ha_call_write_tool","args":{"entity_id":"light.living_room"}}}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
			if step != 1 {
				chunk = `data: {"candidates":[{"content":{"parts":[{"text":"I've turned off the light."}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":25,"candidatesTokenCount":10,"totalTokenCount":35}}` + "\n\n"
			}
			_, _ = w.Write([]byte(chunk))
		case "tool_error_then_text":
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"failing_tool","args":{}}}],"role":"model"}}],"index":0}` + "\n\n"
			if step != 1 {
				chunk = `data: {"candidates":[{"content":{"parts":[{"text":"Sorry, that device is currently unreachable."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
			}
			_, _ = w.Write([]byte(chunk))
		case "mystery_tool":
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"mystery_tool","args":{"x":1}}}]}}]}` + "\n\n"
			if step != 1 {
				chunk = `data: {"candidates":[{"content":{"parts":[{"text":"Tool failed as expected"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5}}` + "\n\n"
			}
			_, _ = w.Write([]byte(chunk))
		case "fallback_only":
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Fallback response"}],"role":"model"}}]}` + "\n\n"))
		}
	}
}

func TestGeminiAPIPool_ToolCalling_Suite(t *testing.T) {
	t.Parallel()
	tests := []toolCallingTestCase{
		{name: "SuccessToolCallCycle", mcpMode: "ha_light", geminiMode: "tool_then_text", expectToolCall: "ha_call_write_tool:", expectResponseSub: "I've turned off the light."},
		{name: "ToolErrorRecovery", mcpMode: "crash_on_call", geminiMode: "tool_error_then_text", expectResponseSub: "unreachable"},
		{name: "ToolCallWithoutDispatcher", geminiMode: "mystery_tool", omitDisp: true, expectResponseSub: "Tool failed as expected"},
		{name: "DispatcherErrorsFallback", geminiMode: "fallback_only", useBrokenDisp: true, expectResponseSub: "Fallback response"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var tsMCP *httptest.Server
			if h := makeToolCallingMCPHandler(tc.mcpMode); h != nil {
				tsMCP = httptest.NewServer(h)
				defer tsMCP.Close()
			}
			tsGemini := httptest.NewServer(makeToolCallingGeminiHandler(tc.geminiMode))
			defer tsGemini.Close()

			cfg := GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: tsGemini.URL, HTTPClient: tsGemini.Client()}
			if tsMCP != nil {
				cfg.MCPServers = []MCPServerConfig{{Name: "test-mcp", ServerURL: tsMCP.URL}}
			} else if tc.useBrokenDisp {
				cfg.MCPDispatcher = &errMockDispatcher{}
			}

			pool := NewGeminiAPIPool(cfg)
			defer pool.Close()
			sess, err := pool.GetOrCreateSession(context.Background(), "tool-session", "")
			if err != nil {
				t.Fatalf("unexpected GetOrCreateSession error: %v", err)
			}

			sink := newMockTurnSink()
			err = sess.Send("trigger action", &TurnContext{TurnID: "turn-tool-test", Prompt: "trigger action", Sink: sink, CreatedAt: time.Now()})
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error=%v, got: %v", tc.expectErr, err)
			}
			sink.mu.Lock()
			defer sink.mu.Unlock()
			if tc.expectToolCall != "" && (len(sink.toolCalls) == 0 || !strings.HasPrefix(sink.toolCalls[0], tc.expectToolCall)) {
				t.Errorf("expected tool call starting with %q, got %v", tc.expectToolCall, sink.toolCalls)
			}
			if tc.expectResponseSub != "" && (sink.result == nil || !strings.Contains(sink.result.Response, tc.expectResponseSub)) {
				t.Errorf("expected result containing %q, got %+v", tc.expectResponseSub, sink.result)
			}
		})
	}
}

func TestGeminiAPISession_PersistenceAndHydrationCycle(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Persisted response\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: ts.URL, HTTPClient: ts.Client(), DataDir: tmpDir})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "persisted-session", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := sess.Send("First prompt", &TurnContext{TurnID: "turn-persist-1", Prompt: "First prompt", Sink: newMockTurnSink()}); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}
	transcriptFile := filepath.Join(tmpDir, "brain", "persisted-session", ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(transcriptFile)
	if err != nil || !strings.Contains(string(data), "First prompt") || !strings.Contains(string(data), "Persisted response") {
		t.Fatalf("transcript missing expected turns: %s (%v)", string(data), err)
	}

	freshSession := &GeminiAPISession{targetKey: "persisted-session", sessionID: "persisted-session", pool: pool, dataDir: tmpDir}
	freshSession.hydrateHistory()
	if hist := freshSession.History(); len(hist) != 2 || hist[0].Parts[0].Text != "First prompt" || hist[1].Parts[0].Text != "Persisted response" {
		t.Errorf("unexpected hydrated history: %+v", hist)
	}
	// Additional transcript edge branches
	(&GeminiAPISession{dataDir: ""}).appendTranscript("p", "r")
	(&GeminiAPISession{sessionID: "test-err-dir", dataDir: "/dev/null/forbidden"}).appendTranscript("p", "r")
	forbiddenFileSess := &GeminiAPISession{sessionID: "test-err-file", dataDir: tmpDir}
	_ = os.MkdirAll(forbiddenFileSess.transcriptPath(), 0o755)
	forbiddenFileSess.appendTranscript("p", "r")
}

func TestGeminiAPISession_TranscriptPersistence_Suite(t *testing.T) {
	t.Parallel()
	fileContent30 := func() string {
		var sb strings.Builder
		for i := 1; i <= 30; i++ {
			u, _ := json.Marshal(map[string]interface{}{"type": "USER_INPUT", "content": fmt.Sprintf("prompt %d", i)})
			m, _ := json.Marshal(map[string]interface{}{"type": "PLANNER_RESPONSE", "content": fmt.Sprintf("reply %d", i)})
			sb.WriteString(string(u) + "\n" + string(m) + "\n")
		}
		return sb.String()
	}()
	tests := []struct {
		name, fileContent, firstText string
		expectedLen                  int
	}{
		{name: "Clamping30TurnsTo20Messages", fileContent: fileContent30, expectedLen: 20, firstText: "prompt 21"},
		{name: "BlankLinesAndSpacing", fileContent: "\n\n  \n{\"type\":\"USER_INPUT\",\"content\":\"hello\"}\n\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"world\"}\n\n", expectedLen: 2, firstText: "hello"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			p := filepath.Join(tmpDir, "brain", "sess-"+tc.name, ".system_generated", "logs", "transcript.jsonl")
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			_ = os.WriteFile(p, []byte(tc.fileContent), 0o644)
			sess := &GeminiAPISession{targetKey: "sess-" + tc.name, sessionID: "sess-" + tc.name, pool: NewGeminiAPIPool(GeminiAPIPoolConfig{DataDir: tmpDir}), dataDir: tmpDir}
			sess.hydrateHistory()
			hist := sess.History()
			if len(hist) != tc.expectedLen || (tc.firstText != "" && hist[0].Parts[0].Text != tc.firstText) {
				t.Errorf("%s: unexpected hist: len=%d, first=%+v", tc.name, len(hist), hist)
			}
		})
	}
}

func TestCleanParameters(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                                       string
		input                                      map[string]interface{}
		expectedType                               string
		nullable, checkNonNilProps, checkAnyOfPres bool
	}{
		{name: "NilSchema", input: nil, expectedType: "OBJECT", checkNonNilProps: true},
		{name: "EmptySchema", input: map[string]interface{}{}, expectedType: "OBJECT", checkNonNilProps: true},
		{name: "IntType", input: map[string]interface{}{"type": "int"}, expectedType: "INTEGER"},
		{name: "BoolType", input: map[string]interface{}{"type": "bool"}, expectedType: "BOOLEAN"},
		{name: "FloatType", input: map[string]interface{}{"type": "float"}, expectedType: "NUMBER"},
		{name: "DoubleType", input: map[string]interface{}{"type": "double"}, expectedType: "NUMBER"},
		{name: "NullType", input: map[string]interface{}{"type": "null"}, expectedType: "OBJECT", nullable: true},
		{name: "PolymorphicNullString", input: map[string]interface{}{"type": []interface{}{"null", "string"}}, expectedType: "STRING", nullable: true},
		{name: "PolymorphicNullOnly", input: map[string]interface{}{"type": []interface{}{"null"}}, expectedType: "OBJECT", nullable: true},
		{name: "StringSliceNullInt", input: map[string]interface{}{"type": []string{"null", "int"}}, expectedType: "INTEGER", nullable: true},
		{name: "StringSliceNullOnly", input: map[string]interface{}{"type": []string{"null"}}, expectedType: "OBJECT", nullable: true},
		{name: "OneOfFlatteningInterface", input: map[string]interface{}{"oneOf": []interface{}{map[string]interface{}{"type": "int"}, map[string]interface{}{"type": "null"}}}, expectedType: "INTEGER", nullable: true},
		{name: "OneOfFlatteningMapSlice", input: map[string]interface{}{"oneOf": []map[string]interface{}{{"type": "null"}, {"type": "bool"}}}, expectedType: "BOOLEAN", nullable: true},
		{name: "AnyOfFlatteningNullSecondWithTypeOverwrite", input: map[string]interface{}{"type": "string", "anyOf": []interface{}{map[string]interface{}{"type": "int", "description": "integer value"}, map[string]interface{}{"type": "null"}}}, expectedType: "INTEGER", nullable: true},
		{name: "OneOfFlatteningMapSliceNullSecond", input: map[string]interface{}{"oneOf": []map[string]interface{}{{"type": "bool"}, {"type": "null"}}}, expectedType: "BOOLEAN", nullable: true},
		{name: "UnflattenableTwoNonNulls", input: map[string]interface{}{"anyOf": []interface{}{map[string]interface{}{"type": "string"}, map[string]interface{}{"type": "int"}}}, expectedType: "OBJECT", checkAnyOfPres: true},
		{name: "UnflattenableThreeAnyOf", input: map[string]interface{}{"anyOf": []interface{}{map[string]interface{}{"type": "string"}, map[string]interface{}{"type": "int"}, map[string]interface{}{"type": "null"}}}, expectedType: "OBJECT", checkAnyOfPres: true},
		{name: "NilInput", input: nil, expectedType: "OBJECT", checkNonNilProps: true},
		{name: "InvalidRequiredTypeInSchema", input: map[string]interface{}{"required": []interface{}{123}}, expectedType: "OBJECT"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cleaned := cleanParameters(tc.input)
			if cleaned["type"] != tc.expectedType {
				t.Errorf("%s: expected type %q, got %q", tc.name, tc.expectedType, cleaned["type"])
			}
			if tc.nullable && cleaned["nullable"] != true {
				t.Errorf("%s: expected nullable true", tc.name)
			}
			if tc.checkNonNilProps && cleaned["properties"] == nil {
				t.Errorf("%s: expected non-nil properties map", tc.name)
			}
			if tc.checkAnyOfPres && cleaned["anyOf"] == nil {
				t.Errorf("%s: expected anyOf preserved", tc.name)
			}
		})
	}
}

func TestCleanParameters_StripsDisallowedKeys(t *testing.T) {
	t.Parallel()
	complexSchema := map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#", "title": "TestSchema", "type": "object",
		"additionalProperties": false, "$ref": "#/defs", "$id": "id1", "definitions": map[string]interface{}{}, "default": "val",
		"properties": map[string]interface{}{
			"prompt":        map[string]interface{}{"type": "string", "title": "Prompt Title", "additionalProperties": false},
			"raw_field":     "not-a-map",
			"items_slice":   map[string]interface{}{"type": "array", "items": []interface{}{map[string]interface{}{"type": "int", "additionalProperties": false}, "not-a-map"}},
			"items_map":     map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "bool"}},
			"empty_child":   map[string]interface{}{},
			"optional_list": map[string]interface{}{"type": []interface{}{"null", "array"}, "items": map[string]interface{}{"type": "string"}},
			"reversed_null": map[string]interface{}{"type": []string{"string", "null"}},
			"branch_field":  map[string]interface{}{"anyOf": []interface{}{map[string]interface{}{"type": "null"}, map[string]interface{}{"type": "string"}}},
		},
		"allOf": []map[string]interface{}{{"type": "string"}},
	}
	cleaned := cleanParameters(complexSchema)
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
	checkKeys(cleaned, "root")
	props := cleaned["properties"].(map[string]interface{})
	if cleaned["type"] != "OBJECT" || props["prompt"].(map[string]interface{})["type"] != "STRING" || props["empty_child"].(map[string]interface{})["type"] != "OBJECT" {
		t.Errorf("cleaned schema types mismatch: %+v", cleaned)
	}
	if optList := props["optional_list"].(map[string]interface{}); optList["type"] != "ARRAY" || optList["nullable"] != true {
		t.Errorf("expected optional_list ARRAY nullable: %+v", optList)
	}
	if revNull := props["reversed_null"].(map[string]interface{}); revNull["type"] != "STRING" || revNull["nullable"] != true {
		t.Errorf("expected reversed_null STRING nullable: %+v", revNull)
	}
	// Deeply nested schema check (20 levels)
	root := map[string]interface{}{"type": "object", "additionalProperties": false}
	curr := root
	for i := 0; i < 20; i++ {
		child := map[string]interface{}{"type": "object", "additionalProperties": false}
		curr["properties"] = map[string]interface{}{"nested": child}
		curr = child
	}
	curr["properties"] = map[string]interface{}{"leaf": map[string]interface{}{"type": "string", "additionalProperties": false}}
	probe := cleanParameters(root)
	for level := 0; level < 20; level++ {
		if _, hasAddl := probe["additionalProperties"]; hasAddl {
			t.Fatalf("level %d had additionalProperties", level)
		}
		probe = probe["properties"].(map[string]interface{})["nested"].(map[string]interface{})
	}
	if leaf := probe["properties"].(map[string]interface{})["leaf"].(map[string]interface{}); leaf["type"] != "STRING" {
		t.Fatalf("leaf not preserved: %+v", leaf)
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
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "tools/list" {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"tool_a"},{"name":"tool_b"}]}}`))
		} else if req.Method == "tools/call" {
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Result of %s"}]}}`, req.Params.Name)))
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer tsMCP.Close()

	geminiReqCount := int32(0)
	var capturedTurn2Contents []geminiContent
	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := atomic.AddInt32(&geminiReqCount, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		if reqNum == 1 {
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"tool_a\",\"args\":{\"arg\":\"1\"}},\"thoughtSignature\":\"sig_shared\"},{\"functionCall\":{\"name\":\"tool_b\",\"args\":{\"arg\":\"2\"}}}],\"role\":\"model\"}}],\"index\":0}\n\n"))
		} else {
			body, _ := io.ReadAll(r.Body)
			var streamReq geminiStreamRequest
			_ = json.Unmarshal(body, &streamReq)
			capturedTurn2Contents = streamReq.Contents
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Both tools finished.\"}],\"role\":\"model\"}}],\"finishReason\":\"STOP\",\"index\":0}\n\n"))
		}
	}))
	defer tsGemini.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: tsGemini.URL, HTTPClient: tsGemini.Client(),
		MCPServers: []MCPServerConfig{{Name: "multi-tool-mcp", ServerURL: tsMCP.URL}},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-multi", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sink := newMockTurnSink()
	if err := sess.Send("run tools", &TurnContext{TurnID: "turn-multi-tool", Prompt: "run tools", Sink: sink}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.toolCalls) != 2 || sink.result == nil || sink.result.Response != "Both tools finished." || len(capturedTurn2Contents) != 3 {
		t.Fatalf("unexpected sink state: calls=%d, result=%+v, turn2Contents=%d", len(sink.toolCalls), sink.result, len(capturedTurn2Contents))
	}
	modelContent, funcContent := capturedTurn2Contents[1], capturedTurn2Contents[2]
	if modelContent.Role != "model" || len(modelContent.Parts) != 2 || modelContent.Parts[0].ThoughtSignature != "sig_shared" || modelContent.Parts[1].ThoughtSignature != "sig_shared" {
		t.Errorf("modelContent grouping failed: %+v", modelContent)
	}
	if funcContent.Role != "user" || len(funcContent.Parts) != 2 || funcContent.Parts[0].FunctionResponse.Name != "tool_a" || funcContent.Parts[1].FunctionResponse.Name != "tool_b" {
		t.Errorf("funcContent grouping failed: %+v", funcContent)
	}
}

func TestGeminiAPIPool_LoadVoiceRulesFromDataDir(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	rulesDir := filepath.Join(tmpDir, "runtimes", "voice", ".gemini", "config", "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("failed to create rules dir: %v", err)
	}
	ruleContent := "# Test Voice Rules\nBe succinct and direct."
	if err := os.WriteFile(filepath.Join(rulesDir, "00_test.md"), []byte(ruleContent), 0o644); err != nil {
		t.Fatalf("failed to write test rule file: %v", err)
	}
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{DataDir: tmpDir})
	defer pool.Close()
	if !strings.Contains(pool.cfg.SystemPrompt, ruleContent) {
		t.Errorf("expected rule content, got: %q", pool.cfg.SystemPrompt)
	}
}

type fallbackMockDispatcher struct{}

func (d *fallbackMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return []geminiFunctionDeclaration{{Name: "test_tool", Description: "a test tool"}}, nil
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
		switch requestCount {
		case 1:
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"test_tool","args":{"foo":"bar"}}}]}}]}` + "\n\n"))
		case 2:
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[]},"finishReason":"STOP"}]}` + "\n\n"))
		default:
			finalReqBody = body
			_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Here is your calendar summary."}]}}]}` + "\n\n"))
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: ts.URL, HTTPClient: ts.Client(), MCPDispatcher: &fallbackMockDispatcher{}})
	defer pool.Close()
	sess, err := pool.GetOrCreateSession(context.Background(), "test-exhaust-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sink := newMockTurnSink()
	if err := sess.Send("what is on my calendar?", &TurnContext{TurnID: "turn-exhaust", Prompt: "what is on my calendar?", Sink: sink, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	sink.mu.Lock()
	collectedText := strings.Join(sink.deltas, "")
	sink.mu.Unlock()
	if !strings.Contains(collectedText, "Here is your calendar summary.") || strings.Contains(string(finalReqBody), "\"tools\":[") {
		t.Errorf("expected fallback text without tools, got text=%q, body=%s", collectedText, string(finalReqBody))
	}
}

func newAllowedToolsMockServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "tools/list" {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"ha_call_read_tool","description":"Read HA"},{"name":"get_events","description":"Get calendar"},{"name":"batch_modify_gmail_message_labels","description":"Gmail"}]}}`))
			return
		}
		if req.Method == "tools/call" {
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"executed: %s"}]}}`, req.Params.Name)))
			return
		}
		http.Error(w, "unknown method", http.StatusBadRequest)
	}))
}

func TestDefaultMCPDispatcher_AllowedToolsFilteringAndExecution(t *testing.T) {
	t.Parallel()
	var nilDisp *DefaultMCPDispatcher
	if decls, err := nilDisp.ListDeclarations(context.Background()); err != nil || decls != nil {
		t.Errorf("expected nil decls")
	}
	if _, err := nilDisp.Execute(context.Background(), "t", nil); err == nil {
		t.Errorf("expected err on nil Execute")
	}
	if err := nilDisp.Prime(context.Background()); err != nil {
		t.Errorf("expected nil Prime err")
	}

	poolTS := newAllowedToolsMockServer()
	t.Cleanup(poolTS.Close)
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: "http://127.0.0.1:1",
		MCPServers:   []MCPServerConfig{{Name: "test-mcp", ServerURL: poolTS.URL}},
		AllowedTools: []string{"ha_call_read_tool"},
	})
	t.Cleanup(func() { _ = pool.Close() })
	if pool.cfg.MCPDispatcher == nil {
		t.Fatalf("expected DefaultMCPDispatcher auto-wired")
	}
	if decls, err := pool.cfg.MCPDispatcher.ListDeclarations(context.Background()); err != nil || len(decls) != 1 || decls[0].Name != "ha_call_read_tool" {
		t.Fatalf("unexpected decls: %v, %v", decls, err)
	}

	cases := []struct {
		name, testTool, errSubstr string
		allowedTools              []string
		expectedDecls             int
		passNilSlice, expectErr   bool
	}{
		{name: "FilteringAndGatingActive", allowedTools: []string{"ha_call_read_tool", "get_events"}, expectedDecls: 2, testTool: "ha_call_read_tool"},
		{name: "FilteringRejection", allowedTools: []string{"ha_call_read_tool", "get_events"}, expectedDecls: 2, testTool: "batch_modify_gmail_message_labels", expectErr: true, errSubstr: "not in the allowed voice tools list"},
		{name: "OmittedOrNilAllowedToolsAllowsAll", passNilSlice: true, expectedDecls: 3, testTool: "batch_modify_gmail_message_labels"},
		{name: "ExplicitEmptyAllowedToolsBlocksAll", allowedTools: []string{}, expectedDecls: 0, testTool: "ha_call_read_tool", expectErr: true, errSubstr: "not in the allowed voice tools list"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newAllowedToolsMockServer()
			t.Cleanup(ts.Close)
			slice := tc.allowedTools
			if tc.passNilSlice {
				slice = nil
			}
			disp := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}}, ts.Client(), slice)
			ctx := context.Background()
			decls, err := disp.ListDeclarations(ctx)
			if err != nil || len(decls) != tc.expectedDecls {
				t.Fatalf("ListDeclarations failed: decls=%d (expected %d), err=%v", len(decls), tc.expectedDecls, err)
			}
			res, err := disp.Execute(ctx, tc.testTool, nil)
			if tc.expectErr {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("expected error containing %q, got err=%v", tc.errSubstr, err)
				}
			} else if err != nil || !strings.Contains(res, "executed: "+tc.testTool) {
				t.Fatalf("expected successful execution of %q, got res=%q, err=%v", tc.testTool, res, err)
			}
		})
	}
}

func TestGeminiAPISession_Send_Retrievers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, preSeededPrompt, normalPrompt, retrieverSnippet, modelResponse, expectedWireTag, expectedContent, expectedHistoryClean string
		isAmbient                                                                                                                    bool
	}{
		{name: "MemoryRetriever_Normal", normalPrompt: "What is my theme preference?", retrieverSnippet: "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>", modelResponse: "Got your preference!", expectedWireTag: "<retrieved_memory>", expectedContent: "User prefers dark mode", expectedHistoryClean: "What is my theme preference?"},
		{name: "MemoryRetriever_NoDoubleInject", preSeededPrompt: "<retrieved_memory>\n- Already present\n</retrieved_memory>\n\nHello", retrieverSnippet: "<retrieved_memory>\n- Extra memory\n</retrieved_memory>", modelResponse: "Acknowledged", expectedWireTag: "<retrieved_memory>", expectedContent: "Already present", expectedHistoryClean: "Already present"},
		{name: "AmbientContextRetriever_Normal", isAmbient: true, normalPrompt: "turn off the living room lights", retrieverSnippet: "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>", modelResponse: "Turned them off!", expectedWireTag: "<ambient_context>", expectedContent: "Living Room: lights are ON", expectedHistoryClean: "turn off the living room lights"},
		{name: "AmbientContextRetriever_NoDoubleInject", isAmbient: true, preSeededPrompt: "<ambient_context>\nExisting ambient\n</ambient_context>\n\nHello", retrieverSnippet: "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>", modelResponse: "Acknowledged", expectedWireTag: "<ambient_context>", expectedContent: "Existing ambient", expectedHistoryClean: "Existing ambient"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			retrieverCalled := false
			var receivedBody []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedBody, _ = io.ReadAll(r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(fmt.Sprintf("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":%q}],\"role\":\"model\"}}]}\n\n", tc.modelResponse)))
			}))
			defer ts.Close()

			poolCfg := GeminiAPIPoolConfig{APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: ts.URL, HTTPClient: ts.Client()}
			if tc.isAmbient {
				poolCfg.AmbientContextRetriever = func(ctx context.Context) (string, error) {
					retrieverCalled = true
					return tc.retrieverSnippet, nil
				}
			} else {
				poolCfg.MemoryRetriever = func(ctx context.Context, query string) (string, error) {
					retrieverCalled = true
					return tc.retrieverSnippet, nil
				}
			}
			pool := NewGeminiAPIPool(poolCfg)
			defer pool.Close()
			sess, err := pool.GetOrCreateSession(context.Background(), "test-sess", "")
			if err != nil {
				t.Fatalf("unexpected GetOrCreateSession error: %v", err)
			}
			prompt := tc.normalPrompt
			if tc.preSeededPrompt != "" {
				prompt = tc.preSeededPrompt
			}
			sink := newMockTurnSink()
			if err := sess.Send(prompt, &TurnContext{TurnID: "turn-retriever", Prompt: prompt, Sink: sink}); err != nil {
				t.Fatalf("Send failed: %v", err)
			}
			if tc.preSeededPrompt != "" && retrieverCalled {
				t.Errorf("expected retriever NOT to be called when tag already present")
			}
			var req geminiStreamRequest
			if err := json.Unmarshal(receivedBody, &req); err != nil || len(req.Contents) == 0 || len(req.Contents[0].Parts) == 0 {
				t.Fatalf("failed to unmarshal request or empty parts: %v", err)
			}
			wireText := req.Contents[0].Parts[0].Text
			if strings.Count(wireText, tc.expectedWireTag) != 1 || !strings.Contains(wireText, tc.expectedContent) {
				t.Errorf("wireText mismatch for %s: %s", tc.expectedWireTag, wireText)
			}
			hist := sess.(*GeminiAPISession).History()
			if len(hist) != 2 || hist[0].Role != "user" || !strings.Contains(hist[0].Parts[0].Text, tc.expectedHistoryClean) || hist[1].Parts[0].Text != tc.modelResponse {
				t.Errorf("history mismatch: %+v", hist)
			}
			if tc.preSeededPrompt == "" && strings.Contains(hist[0].Parts[0].Text, tc.expectedWireTag) {
				t.Errorf("history should not contain %s when dynamically injected", tc.expectedWireTag)
			}
		})
	}
}

func TestGeminiAPIPool_AcquireLease_ConcurrencySerialization(t *testing.T) {
	t.Parallel()
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:            "test-key",
		Model:             "gemini-2.5-flash",
		TargetLockTimeout: 100 * time.Millisecond,
	})
	defer pool.Close()

	ctx := context.Background()
	lease1, err := pool.AcquireLease(ctx, "target-conc")
	if err != nil {
		t.Fatalf("lease1 AcquireLease error: %v", err)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err2 := pool.AcquireLease(ctxTimeout, "target-conc"); err2 == nil {
		t.Fatalf("expected lease2 AcquireLease to time out while lease1 is held")
	}

	if err := lease1.Release(); err != nil {
		t.Fatalf("lease1 release error: %v", err)
	}

	lease3, err3 := pool.AcquireLease(ctx, "target-conc")
	if err3 != nil {
		t.Fatalf("lease3 AcquireLease error: %v", err3)
	}
	_ = lease3.Release()
}

func TestGeminiAPIPool_AcquireLease_TableDriven(t *testing.T) {
	t.Parallel()
	simulatedDBErr := errors.New("simulated DB latch timeout")
	const (
		okChunk            = `data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		leaseChunk         = `data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		leaseUsageChunk    = `data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}` + "\n\n"
		quota429Chunk      = `{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`
		retryChunk         = `data: {"candidates":[{"content":{"parts":[{"text":"Retry succeeded."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		rateLimitConvChunk = `data: {"candidates":[{"content":{"parts":[{"text":"I hit an external rate limit reached earlier, but cached the response."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
	)

	cases := []struct {
		name                                                                                                                           string
		nilPool, emptyTarget, useChannelFallback, expectInitCold, expectExecErr, expectRotation, expectEvictedOnFail, expectNilTurnErr bool
		maxTurns                                                                                                                       int
		turnPrompts                                                                                                                    []string
		responseStatusCodes                                                                                                            []int
		responseChunks                                                                                                                 []string
		onRotateErr, expectReleaseErr                                                                                                  error
	}{
		{name: "Validation_NilPool", nilPool: true},
		{name: "Validation_EmptyTarget", emptyTarget: true},
		{name: "BasicExecution_ColdAndWarmAndNilTurn", maxTurns: 10, turnPrompts: []string{"hello", "hello again"}, responseChunks: []string{leaseUsageChunk, leaseChunk}, expectInitCold: true, expectNilTurnErr: true},
		{name: "ColdExecution_ChannelInstructionsFallback", maxTurns: 10, turnPrompts: []string{"hello chan"}, responseChunks: []string{leaseChunk}, useChannelFallback: true, expectInitCold: true},
		{name: "RotationAndPersistence_Success", maxTurns: 1, turnPrompts: []string{"first message"}, expectRotation: true},
		{name: "PersistenceFailureRollback", maxTurns: 1, turnPrompts: []string{"trigger rotation"}, onRotateErr: simulatedDBErr, expectReleaseErr: simulatedDBErr, expectEvictedOnFail: true},
		{name: "CancelRotation_On429QuotaExhausted", maxTurns: 1, turnPrompts: []string{"first message"}, responseStatusCodes: []int{http.StatusTooManyRequests}, responseChunks: []string{quota429Chunk}, expectExecErr: true},
		{name: "ClearsCancelRotationOnSuccessfulRetry", maxTurns: 1, turnPrompts: []string{"attempt 1", "attempt 2"}, responseStatusCodes: []int{http.StatusTooManyRequests, http.StatusOK}, responseChunks: []string{quota429Chunk, retryChunk}, expectRotation: true},
		{name: "ConversationalRateLimitTextDoesNotCancelRotation", maxTurns: 1, turnPrompts: []string{"check status"}, responseChunks: []string{rateLimitConvChunk}, expectRotation: true},
		{name: "DefaultMaxSessionTurnsFallback", maxTurns: 0, turnPrompts: []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"}, expectRotation: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.nilPool {
				var nilPool *GeminiAPIPool
				if _, err := nilPool.AcquireLease(ctx, "target"); err == nil {
					t.Error("expected error from nil pool AcquireLease")
				}
				return
			}
			if tc.emptyTarget {
				pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test"})
				defer pool.Close()
				if _, err := pool.AcquireLease(ctx, "   "); err == nil {
					t.Error("expected error from empty targetKey AcquireLease")
				}
				return
			}

			var callIdx int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				idx := int(atomic.AddInt32(&callIdx, 1) - 1)
				status, chunk := http.StatusOK, okChunk
				if idx < len(tc.responseStatusCodes) && tc.responseStatusCodes[idx] != 0 {
					status = tc.responseStatusCodes[idx]
				}
				if idx < len(tc.responseChunks) && tc.responseChunks[idx] != "" {
					chunk = tc.responseChunks[idx]
				}
				if status == http.StatusOK {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				w.WriteHeader(status)
				flusher, _ := w.(http.Flusher)
				_, _ = w.Write([]byte(chunk))
				if flusher != nil {
					flusher.Flush()
				}
			}))
			defer ts.Close()

			var rotatedOld, rotatedNew string
			var rotationCount int
			cfg := GeminiAPIPoolConfig{
				APIKey: "test-key", Model: "gemini-2.5-flash", BaseURL: ts.URL, HTTPClient: ts.Client(), MaxSessionTurns: tc.maxTurns,
				GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
					return SessionRecord{ActiveSessionID: "550e8400-e29b-41d4-a716-446655440001", PreviousSessionID: "550e8400-e29b-41d4-a716-446655440000", TurnCount: 0}, nil
				},
				OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
					rotationCount++
					rotatedOld, rotatedNew = oldSessionID, newSessionID
					return tc.onRotateErr
				},
			}

			pool := NewGeminiAPIPool(cfg)
			defer pool.Close()
			targetKey := "target-" + tc.name
			lease, err := pool.AcquireLease(ctx, targetKey)
			if err != nil {
				t.Fatalf("AcquireLease failed: %v", err)
			}
			if tc.expectInitCold && (!lease.IsCold() || lease.TurnCount() != 0 || lease.PreviousSessionID() != "550e8400-e29b-41d4-a716-446655440000" || lease.SessionID() != "550e8400-e29b-41d4-a716-446655440001") {
				t.Errorf("initial lease state mismatch: cold=%v, count=%d, prev=%s, sess=%s", lease.IsCold(), lease.TurnCount(), lease.PreviousSessionID(), lease.SessionID())
			}
			initSessID := lease.SessionID()

			for i, prompt := range tc.turnPrompts {
				turnCtx := &TurnContext{TurnID: fmt.Sprintf("turn-%d", i+1), Prompt: prompt, Ctx: ctx}
				if tc.useChannelFallback {
					turnCtx.ChannelInstructions = "channel guideline"
				} else if i == 0 && tc.expectInitCold {
					turnCtx.ScopeInstructions, turnCtx.CoordinationContext, turnCtx.ThreadSummary = "scope rule", "coord info", "summary of thread"
				}
				res, execErr := lease.Execute(ctx, turnCtx)
				if execErr != nil {
					if !tc.expectExecErr && i != 0 {
						t.Fatalf("turn %d Execute failed: %v", i+1, execErr)
					}
					if tc.expectExecErr && !IsQuotaPause(execErr.Error(), "") && !IsCapacityBlip(execErr.Error(), "") {
						t.Errorf("expected quota/capacity error, got: %v", execErr)
					}
				} else if res == nil {
					t.Errorf("turn %d expected non-nil response", i+1)
				}
			}

			if tc.expectNilTurnErr {
				if _, err := lease.Execute(ctx, nil); err == nil {
					t.Errorf("expected error on nil turn context")
				}
			}

			relErr := lease.Release()
			if tc.expectReleaseErr != nil {
				if !errors.Is(relErr, tc.expectReleaseErr) {
					t.Errorf("expected release error %v, got %v", tc.expectReleaseErr, relErr)
				}
			} else if relErr != nil && !tc.expectExecErr {
				t.Errorf("unexpected release error: %v", relErr)
			}

			if tc.expectRotation {
				if rotationCount == 0 || rotatedOld != initSessID || rotatedNew == "" || rotatedNew == rotatedOld {
					t.Errorf("rotation mismatch: count=%d, old=%s, new=%s, init=%s", rotationCount, rotatedOld, rotatedNew, initSessID)
				}
			} else if tc.expectExecErr && rotationCount > 0 {
				t.Errorf("expected NO rotation on quota pause, got %d", rotationCount)
			}

			if tc.expectEvictedOnFail {
				pool.mu.RLock()
				_, exists := pool.sessions[targetKey]
				pool.mu.RUnlock()
				if exists {
					t.Errorf("expected session evicted after latch rollback")
				}
			}
		})
	}
}

func TestGeminiSessionLease_IsEphemeralAndMarkDirty(t *testing.T) {
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{})
	defer pool.Close()
	lease, err := pool.AcquireLease(context.Background(), "target-1")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Release()
	if lease.IsEphemeral() {
		t.Errorf("expected IsEphemeral to be false for gemini lease")
	}

	pool.mu.RLock()
	_, exists := pool.sessions["target-1"]
	pool.mu.RUnlock()
	if !exists {
		t.Fatalf("expected session to exist before MarkDirty")
	}

	lease.MarkDirty()
	pool.mu.RLock()
	_, existsAfter := pool.sessions["target-1"]
	pool.mu.RUnlock()
	if existsAfter {
		t.Errorf("expected session removed from pool after lease.MarkDirty()")
	}

	var nilLease *geminiSessionLease
	nilLease.MarkDirty()
}
