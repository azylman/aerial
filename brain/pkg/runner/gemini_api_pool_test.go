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

func TestGeminiAPIPool_ContextCancellation(t *testing.T) {
	t.Parallel()

	releaseServer := make(chan struct{})
	defer close(releaseServer)
	flushedCh := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if ok {
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Partial\"}],\"role\":\"model\"}}]}\n\n"))
			flusher.Flush()
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
		sinkErr := sink.err
		sink.mu.Unlock()
		if sinkErr == nil {
			t.Errorf("expected error delivered to sink.OnError")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for Send to terminate after context cancellation")
	}
}

func TestGeminiAPIPool_Send_Suite(t *testing.T) {
	t.Parallel()

	type sendTestCase struct {
		name              string
		serverHandler     http.HandlerFunc
		cfg               GeminiAPIPoolConfig
		device            string
		prompt            string
		preCancelCtx      bool
		badURL            bool
		cancelOnFlush     bool
		nilSink           bool
		expectErr         bool
		expectErrMsg      string
		expectSinkErrMsg  string
		expectDeltas      []string
		expectSinkResult  string
		expectPromptToken int
		expectCandToken   int
		expectTotalToken  int
		checkThinkingBody bool
		runSequential     int
		expectMaxHistory  int
		expectHistFirst   string
		expectHistLast    string
		verifySanitizeKey string
	}

	tests := []sendTestCase{
		{
			name:   "StreamingSuccess",
			device: "kiosk-living-room",
			prompt: "Say hello",
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				flusher, _ := w.(http.Flusher)
				_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Hello "}],"role":"model"}}],"index":0}` + "\n\n"))
				flusher.Flush()
				_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"world!"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":6,"totalTokenCount":18}}` + "\n\n"))
				flusher.Flush()
			},
			expectDeltas:      []string{"Hello ", "world!"},
			expectSinkResult:  "Hello world!",
			expectPromptToken: 12,
			expectCandToken:   6,
			expectTotalToken:  18,
		},
		{
			name:   "ZeroThinkingBudget",
			device: "voice-device",
			prompt: "Check thinking budget",
			cfg: GeminiAPIPoolConfig{
				APIKey:       "key-abc",
				Model:        "gemini-2.5-flash",
				SystemPrompt: "You are Aerial.",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Thinking budget tested\"}],\"role\":\"model\"}}]}\n\n"))
			},
			checkThinkingBody: true,
			expectSinkResult:  "Thinking budget tested",
		},
		{
			name:   "SlidingWindowHistory",
			device: "history-device",
			prompt: "Prompt",
			cfg: GeminiAPIPoolConfig{
				APIKey:          "history-key",
				Model:           "gemini-2.5-flash",
				MaxHistoryTurns: 10,
			},
			serverHandler: func() http.HandlerFunc {
				var turnCounter int32
				return func(w http.ResponseWriter, r *http.Request) {
					current := atomic.AddInt32(&turnCounter, 1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(fmt.Sprintf("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Response %d\"}],\"role\":\"model\"}}]}\n\n", current)))
				}
			}(),
			runSequential:    15,
			expectMaxHistory: 20,
			expectHistFirst:  "Prompt 6",
			expectHistLast:   "Response 15",
		},
		{
			name:   "RateLimit429",
			device: "rate-limited-device",
			prompt: "Hello under limit",
			cfg: GeminiAPIPoolConfig{
				APIKey: "ratelimit-key",
				Model:  "gemini-2.5-flash",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}`))
			},
			expectErr:        true,
			expectErrMsg:     "rate limit exceeded: please try again shortly",
			expectSinkErrMsg: "rate limit exceeded: please try again shortly",
		},
		{
			name:   "EmptyPrompt_Empty",
			device: "device-empty-1",
			prompt: "",
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			expectErr: true,
		},
		{
			name:   "EmptyPrompt_Whitespace",
			device: "device-empty-2",
			prompt: "   \t\n\r  ",
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			expectErr: true,
		},
		{
			name:   "SanitizeAPIKey",
			device: "sanitize-device",
			prompt: "trigger error",
			cfg: GeminiAPIPoolConfig{
				APIKey: "AIzaSySuperSecretKey123456789",
				Model:  "gemini-2.5-flash",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal error occurred with key AIzaSySuperSecretKey123456789"}`))
			},
			expectErr:         true,
			verifySanitizeKey: "AIzaSySuperSecretKey123456789",
		},
		{
			name:   "Send_WithNilSinkAndServerError",
			device: "nil-sink-device",
			prompt: "hello",
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "server error", http.StatusInternalServerError)
			},
			nilSink:   true,
			expectErr: true,
		},
		{
			name:         "Send_WithNilSinkAndCancelledCtx",
			device:       "nil-sink-cancel",
			prompt:       "hello",
			preCancelCtx: true,
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			nilSink:   true,
			expectErr: true,
		},
		{
			name:   "Send_SSEMalformedChunk",
			device: "malformed-sse",
			prompt: "trigger malformed",
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
				Model:  "gemini-2.5-flash",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data: {malformed_json\n\n"))
			},
			expectErr: true,
		},
		{
			name:         "Send_PreCancelledContext",
			device:       "precancel-device",
			prompt:       "hi",
			preCancelCtx: true,
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
			},
			expectErr: true,
		},
		{
			name:   "Send_NetworkDoError",
			device: "net-err-device",
			prompt: "hi",
			badURL: true,
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
			},
			expectErr: true,
		},
		{
			name:          "Send_CancelledDuringStream",
			device:        "stream-cancel-device",
			prompt:        "hi",
			cancelOnFlush: true,
			cfg: GeminiAPIPoolConfig{
				APIKey: "test-key",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				<-r.Context().Done()
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var lastReqBody []byte
			var ts *httptest.Server
			if tc.serverHandler != nil {
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tc.checkThinkingBody {
						lastReqBody, _ = io.ReadAll(r.Body)
					}
					tc.serverHandler(w, r)
				}))
				defer ts.Close()
			}

			cfg := tc.cfg
			if ts != nil {
				cfg.BaseURL = ts.URL
				cfg.HTTPClient = ts.Client()
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
					turn := &TurnContext{
						TurnID:    fmt.Sprintf("turn-%d", i),
						Prompt:    p,
						Sink:      sink,
						CreatedAt: time.Now(),
					}
					if err := sess.Send(p, turn); err != nil {
						t.Fatalf("turn %d failed: %v", i, err)
					}
				}
				geminiSess := sess.(*GeminiAPISession)
				hist := geminiSess.History()
				if len(hist) != tc.expectMaxHistory {
					t.Fatalf("expected history len %d, got %d", tc.expectMaxHistory, len(hist))
				}
				if tc.expectHistFirst != "" && hist[0].Parts[0].Text != tc.expectHistFirst {
					t.Errorf("expected first hist message %q, got %q", tc.expectHistFirst, hist[0].Parts[0].Text)
				}
				if tc.expectHistLast != "" && hist[len(hist)-1].Parts[0].Text != tc.expectHistLast {
					t.Errorf("expected last hist message %q, got %q", tc.expectHistLast, hist[len(hist)-1].Parts[0].Text)
				}
				return
			}

			turnCtx := ctx
			var cancel context.CancelFunc
			if tc.preCancelCtx {
				turnCtx, cancel = context.WithCancel(ctx)
				cancel()
			} else if tc.cancelOnFlush {
				turnCtx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(20*time.Millisecond, cancel)
			}

			var sink *mockTurnSink
			var sinkInterface TurnSink
			if !tc.nilSink {
				sink = newMockTurnSink()
				sinkInterface = sink
			}

			turn := &TurnContext{
				TurnID:    "turn-suite",
				Prompt:    tc.prompt,
				Sink:      sinkInterface,
				CreatedAt: time.Now(),
				Ctx:       turnCtx,
			}

			sendErr := sess.Send(tc.prompt, turn)
			if tc.expectErr {
				if sendErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.expectErrMsg != "" && sendErr.Error() != tc.expectErrMsg {
					t.Errorf("expected Send error %q, got %q", tc.expectErrMsg, sendErr.Error())
				}
				if sink != nil {
					sink.mu.Lock()
					sErr := sink.err
					sink.mu.Unlock()
					if sErr == nil {
						t.Errorf("expected sink.OnError to be called")
					} else if tc.expectSinkErrMsg != "" && sErr.Error() != tc.expectSinkErrMsg {
						t.Errorf("expected sink error %q, got %q", tc.expectSinkErrMsg, sErr.Error())
					}
				}
				if tc.verifySanitizeKey != "" {
					if strings.Contains(sendErr.Error(), tc.verifySanitizeKey) {
						t.Errorf("send error contains unredacted API key: %s", sendErr.Error())
					}
					if !strings.Contains(sendErr.Error(), "[REDACTED]") {
						t.Errorf("send error expected to contain [REDACTED], got: %s", sendErr.Error())
					}
					if sink != nil {
						sink.mu.Lock()
						sErr := sink.err
						sink.mu.Unlock()
						if strings.Contains(sErr.Error(), tc.verifySanitizeKey) {
							t.Errorf("sink error contains unredacted API key: %s", sErr.Error())
						}
					}
					queryErr := fmt.Errorf("connection failed to https://generativelanguage.googleapis.com/v1beta/models/gemini:stream?key=%s&alt=sse: timeout", tc.verifySanitizeKey)
					sanitized := pool.sanitizeError(queryErr)
					if strings.Contains(sanitized.Error(), tc.verifySanitizeKey) {
						t.Errorf("sanitized query error still contains secret key: %s", sanitized.Error())
					}
					if !strings.Contains(sanitized.Error(), "key=[REDACTED]") {
						t.Errorf("sanitized query error expected to contain 'key=[REDACTED]', got: %s", sanitized.Error())
					}
				}
				return
			}

			if sendErr != nil {
				t.Fatalf("unexpected Send error: %v", err)
			}

			if sink != nil {
				sink.mu.Lock()
				defer sink.mu.Unlock()
				if !sink.started {
					t.Errorf("expected sink.started to be true")
				}
				if len(tc.expectDeltas) > 0 {
					if len(sink.deltas) != len(tc.expectDeltas) {
						t.Fatalf("expected %d deltas, got %d: %v", len(tc.expectDeltas), len(sink.deltas), sink.deltas)
					}
					for i, d := range tc.expectDeltas {
						if sink.deltas[i] != d {
							t.Errorf("delta[%d]: expected %q, got %q", i, d, sink.deltas[i])
						}
					}
				}
				if tc.expectSinkResult != "" {
					if sink.result == nil || sink.result.Response != tc.expectSinkResult {
						t.Errorf("expected sink response %q, got %+v", tc.expectSinkResult, sink.result)
					}
				}
				if tc.expectPromptToken > 0 && sink.result.Usage.InputTokens != tc.expectPromptToken {
					t.Errorf("expected prompt tokens %d, got %d", tc.expectPromptToken, sink.result.Usage.InputTokens)
				}
				if tc.expectCandToken > 0 && sink.result.Usage.OutputTokens != tc.expectCandToken {
					t.Errorf("expected completion tokens %d, got %d", tc.expectCandToken, sink.result.Usage.OutputTokens)
				}
				if tc.expectTotalToken > 0 && sink.result.Usage.TotalTokens != tc.expectTotalToken {
					t.Errorf("expected total tokens %d, got %d", tc.expectTotalToken, sink.result.Usage.TotalTokens)
				}
			}

			if tc.checkThinkingBody {
				rawJSON := string(lastReqBody)
				if !strings.Contains(rawJSON, `"thinkingBudget":0`) && !strings.Contains(rawJSON, `"thinkingBudget": 0`) {
					t.Errorf("expected JSON to contain thinkingBudget: 0, got %s", rawJSON)
				}
				if !strings.Contains(rawJSON, "You are Aerial.") {
					t.Errorf("expected JSON to contain systemInstruction, got %s", rawJSON)
				}
			}
		})
	}
}

func TestGeminiAPIPool_AccessorsAndNilPool(t *testing.T) {
	t.Parallel()

	// Nil pool accessors
	var nilPool *GeminiAPIPool
	if nilPool.Model() != "" || nilPool.APIKey() != "" {
		t.Errorf("expected empty string from nilPool accessors")
	}
	nilPool.MarkDirty()
	nilPool.UpdatePrewarmedTargets([]string{"t"})
	if targets := nilPool.PrewarmedTargets(); targets != nil {
		t.Errorf("expected nil PrewarmedTargets for nil pool, got %v", targets)
	}
	if err := nilPool.sanitizeError(nil); err != nil {
		t.Errorf("expected sanitizeError(nil) == nil, got %v", err)
	}

	// Pool accessors and sessions
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:           "my-api-key",
		Model:            "gemini-2.5-pro",
		PrewarmedTargets: []string{"initial-1", "initial-2"},
	})
	defer pool.Close()

	if pool.Model() != "gemini-2.5-pro" || pool.APIKey() != "my-api-key" {
		t.Errorf("unexpected accessors: %q %q", pool.Model(), pool.APIKey())
	}
	sess, err := pool.GetOrCreateSession(context.Background(), "target-123", "custom-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	geminiSess, ok := sess.(*GeminiAPISession)
	if !ok || geminiSess.TargetKey() != "target-123" || geminiSess.SessionID() != "custom-uuid" {
		t.Errorf("unexpected session attributes: %v", sess)
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
	if len(pool.PrewarmedTargets()) != 2 || pool.PrewarmedTargets()[0] != "alpha" {
		t.Errorf("failed to update prewarmed targets")
	}
}

func TestGeminiAPIPool_MarkDirtySessionRecreation(t *testing.T) {
	t.Parallel()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey: "test-key",
		Model:  "gemini-2.5-flash",
	})
	defer pool.Close()

	ctx := context.Background()
	s1, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	s1Again, err := pool.GetOrCreateSession(ctx, "kiosk-1", "")
	if err != nil || s1 != s1Again {
		t.Fatalf("expected identical session instance before MarkDirty")
	}

	pool.MarkDirty()
	s1After, err := pool.GetOrCreateSession(ctx, "kiosk-1", "sess-2")
	if err != nil || s1 == s1After || s1After.SessionID() != "sess-2" {
		t.Fatalf("expected new session after MarkDirty")
	}
}

func TestGeminiAPIPool_PrewarmedAndClose(t *testing.T) {
	t.Parallel()

	prewarmedTargets := []string{"living-room", "kitchen", "office"}
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:           "test-key",
		Model:            "gemini-2.5-flash",
		PrewarmedTargets: prewarmedTargets,
	})
	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
	for _, target := range prewarmedTargets {
		sess, err := pool.GetOrCreateSession(ctx, target, "")
		if err != nil || sess.SessionID() != target {
			t.Errorf("unexpected prewarmed session for %s: %v", target, err)
		}
	}

	sessBeforeClose := pool.sessions["living-room"]
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	// Operations on closed pool
	if _, err := pool.GetOrCreateSession(ctx, "new-target", ""); err == nil {
		t.Errorf("expected error on closed pool GetOrCreateSession")
	}
	if err := pool.Initialize(ctx); err == nil {
		t.Errorf("expected error on closed pool Initialize")
	}
	pool.UpdatePrewarmedTargets([]string{"noop"})
	pool.MarkDirty()

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-closed", Prompt: "hello", Sink: sink}
	if err := sessBeforeClose.Send("hello", turn); err == nil {
		t.Errorf("expected error calling Send on closed pool session")
	}
}

func TestGeminiAPISession_EdgeCasesAndJSONRPCBody(t *testing.T) {
	t.Parallel()

	sess := &GeminiAPISession{targetKey: "fallback-target"}
	if sess.SessionID() != "fallback-target" {
		t.Errorf("expected fallback session ID")
	}
	if sess.transcriptPath() != "" {
		t.Errorf("expected empty transcript path when dataDir is empty")
	}

	rpcTests := []struct {
		body      string
		expectErr bool
	}{
		{body: "", expectErr: true},
		{body: "not json", expectErr: true},
		{body: `{"jsonrpc":"2.0","result":{}}`, expectErr: false},
	}
	for _, rt := range rpcTests {
		res, err := parseJSONRPCBody([]byte(rt.body))
		if (err != nil) != rt.expectErr || (err == nil && res == nil) {
			t.Errorf("body %q: got res=%v, err=%v", rt.body, res, err)
		}
	}
}

type errMockDispatcher struct{}

func (d *errMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return nil, errors.New("declarations query failed")
}

func (d *errMockDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	return "", errors.New("execute failed")
}

func TestMCPDispatcher_ListDeclarationsJSONAndSSE(t *testing.T) {
	t.Parallel()

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

	tsSSE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"ha_call_write_tool\",\"description\":\"Call HA service\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"entity_id\":{\"type\":\"string\"}}}}]}}\n\n"))
	}))
	defer tsSSE.Close()

	disp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "scheduler", ServerURL: tsJSON.URL},
		{Name: "ha-mcp", ServerURL: tsSSE.URL, Headers: map[string]string{"Authorization": "Bearer token123"}},
	}, tsJSON.Client())

	decls, err := disp.ListDeclarations(context.Background())
	if err != nil {
		t.Fatalf("unexpected ListDeclarations error: %v", err)
	}
	if len(decls) != 2 {
		t.Fatalf("expected 2 declarations, got %d", len(decls))
	}
	names := map[string]bool{}
	for _, d := range decls {
		names[d.Name] = true
		if _, hasSchema := d.Parameters["$schema"]; hasSchema {
			t.Errorf("expected $schema to be stripped from %s", d.Name)
		}
	}
	if !names["schedule_once"] || !names["ha_call_write_tool"] {
		t.Errorf("missing expected tools: %v", names)
	}
}

func TestMCPDispatcher_ExecutePaths(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Method == "tools/list" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"good_tool"},{"name":"error_tool"},{"name":"raw_tool"}]}}`))
			return
		}
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
	}))
	defer ts.Close()

	disp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "test-mcp", ServerURL: ts.URL},
	}, ts.Client())

	ctx := context.Background()

	// Good tool
	out, err := disp.Execute(ctx, "good_tool", map[string]interface{}{"entity": "light.1"})
	if err != nil || out != "Light turned on" {
		t.Errorf("unexpected execute good_tool result: %q, %v", out, err)
	}

	// Error tool
	outErr, err := disp.Execute(ctx, "error_tool", nil)
	if err == nil || !strings.Contains(err.Error(), "Device not reachable") || outErr != "Device not reachable" {
		t.Errorf("unexpected error_tool result: %q, %v", outErr, err)
	}

	// Raw tool
	rawOut, err := disp.Execute(ctx, "raw_tool", nil)
	if err != nil || !strings.Contains(rawOut, `"status":"ok"`) {
		t.Errorf("unexpected raw_tool result: %q, %v", rawOut, err)
	}

	// Unknown tool
	if _, err := disp.Execute(ctx, "non_existent", nil); err == nil {
		t.Errorf("expected error for non_existent tool")
	}
}

func TestMCPDispatcher_DeclarationsAndExecutionEdgeCases(t *testing.T) {
	t.Parallel()

	ts500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer ts500.Close()

	tsRPCErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`))
	}))
	defer tsRPCErr.Close()

	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not a json response`))
	}))
	defer tsBadJSON.Close()

	tsEmptyName := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"","description":"empty"},{"name":"valid_t","description":"ok"}]}}`))
	}))
	defer tsEmptyName.Close()

	// Nil and empty dispatcher checks
	var nilDisp *DefaultMCPDispatcher
	if decls, err := nilDisp.ListDeclarations(context.Background()); err != nil || decls != nil {
		t.Errorf("expected nil decls from nil dispatcher")
	}
	if _, err := nilDisp.Execute(context.Background(), "tool", nil); err == nil {
		t.Errorf("expected error executing on nil dispatcher")
	}

	emptyDisp := NewDefaultMCPDispatcher(nil, nil)
	if decls, err := emptyDisp.ListDeclarations(context.Background()); err != nil || decls != nil {
		t.Errorf("expected nil decls from empty dispatcher")
	}

	skipDisp := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "empty-url", ServerURL: "   "}}, http.DefaultClient)
	if decls, err := skipDisp.ListDeclarations(context.Background()); err != nil || len(decls) != 0 {
		t.Errorf("expected empty decls for empty server URL")
	}

	// Dispatcher with various error servers
	errDisp := NewDefaultMCPDispatcher([]MCPServerConfig{
		{Name: "srv-500", ServerURL: ts500.URL},
		{Name: "srv-rpc-err", ServerURL: tsRPCErr.URL},
		{Name: "srv-bad-json", ServerURL: tsBadJSON.URL},
		{Name: "srv-empty-name", ServerURL: tsEmptyName.URL},
		{Name: "srv-unreachable", ServerURL: "http://127.0.0.1:0"},
	}, ts500.Client())

	decls, err := errDisp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 1 || decls[0].Name != "valid_t" {
		t.Fatalf("expected 1 valid declaration valid_t, got %d decls (%v)", len(decls), err)
	}

	// Execution error table
	execErrorTests := []struct {
		toolName   string
		serverName string
		serverURL  string
	}{
		{"crash_tool", "srv-500", ts500.URL},
		{"rpc_err_tool", "srv-rpc-err", tsRPCErr.URL},
		{"bad_json_tool", "srv-bad-json", tsBadJSON.URL},
		{"unreachable_tool", "srv-unreachable", "http://127.0.0.1:0"},
	}
	for _, et := range execErrorTests {
		errDisp.toolRoutes[et.toolName] = MCPServerConfig{Name: et.serverName, ServerURL: et.serverURL}
		if _, err := errDisp.Execute(context.Background(), et.toolName, nil); err == nil {
			t.Errorf("expected error executing %s on %s", et.toolName, et.serverURL)
		}
	}
}

func TestMCPDispatcher_HeadersAndMultiPartContent(t *testing.T) {
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
		{Name: "auth-srv", ServerURL: ts.URL, Headers: map[string]string{"X-Custom-Auth": "Bearer secret123"}},
	}, ts.Client())
	disp.toolRoutes["multi_tool"] = MCPServerConfig{Name: "auth-srv", ServerURL: ts.URL, Headers: map[string]string{"X-Custom-Auth": "Bearer secret123"}}

	res, err := disp.Execute(context.Background(), "multi_tool", map[string]interface{}{"foo": "bar"})
	if err != nil || res != "line one\nline two" {
		t.Errorf("unexpected multi-part execution: %q, %v", res, err)
	}
}

func TestGeminiAPIPool_ToolCalling_Suite(t *testing.T) {
	t.Parallel()

	type toolCallingTestCase struct {
		name              string
		mcpHandler        http.HandlerFunc
		geminiHandler     func() http.HandlerFunc
		useBrokenDisp     bool
		omitDisp          bool
		expectToolCall    string
		expectResponseSub string
		expectErr         bool
	}

	tests := []toolCallingTestCase{
		{
			name: "SuccessToolCallCycle",
			mcpHandler: func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string `json:"method"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				if req.Method == "tools/list" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"ha_call_write_tool","description":"Call HA service","inputSchema":{"type":"object","properties":{"entity_id":{"type":"string"}}}}]}}`))
					return
				}
				if req.Method == "tools/call" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"light.living_room turned off"}]}}`))
				}
			},
			geminiHandler: func() http.HandlerFunc {
				var callCount int32
				return func(w http.ResponseWriter, r *http.Request) {
					step := atomic.AddInt32(&callCount, 1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					if step == 1 {
						_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"ha_call_write_tool","args":{"entity_id":"light.living_room"}}}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"))
					} else {
						_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"I've turned off the light."}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":25,"candidatesTokenCount":10,"totalTokenCount":35}}` + "\n\n"))
					}
				}
			},
			expectToolCall:    "ha_call_write_tool:",
			expectResponseSub: "I've turned off the light.",
		},
		{
			name: "ToolErrorRecovery",
			mcpHandler: func(w http.ResponseWriter, r *http.Request) {
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
			},
			geminiHandler: func() http.HandlerFunc {
				var callCount int32
				return func(w http.ResponseWriter, r *http.Request) {
					step := atomic.AddInt32(&callCount, 1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					if step == 1 {
						_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"failing_tool","args":{}}}],"role":"model"}}],"index":0}` + "\n\n"))
					} else {
						_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":"Sorry, that device is currently unreachable."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"))
					}
				}
			},
			expectResponseSub: "unreachable",
		},
		{
			name: "ToolCallWithoutDispatcher",
			geminiHandler: func() http.HandlerFunc {
				var step int32
				return func(w http.ResponseWriter, r *http.Request) {
					s := atomic.AddInt32(&step, 1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					if s == 1 {
						_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"mystery_tool\",\"args\":{\"x\":1}}}]}}]}\n\n"))
					} else {
						_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Tool failed as expected\"}]}}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":5}}\n\n"))
					}
				}
			},
			omitDisp:          true,
			expectResponseSub: "Tool failed as expected",
		},
		{
			name: "DispatcherErrorsFallback",
			geminiHandler: func() http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Fallback response\"}],\"role\":\"model\"}}]}\n\n"))
				}
			},
			useBrokenDisp:     true,
			expectResponseSub: "Fallback response",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var tsMCP *httptest.Server
			if tc.mcpHandler != nil {
				tsMCP = httptest.NewServer(tc.mcpHandler)
				defer tsMCP.Close()
			}

			tsGemini := httptest.NewServer(tc.geminiHandler())
			defer tsGemini.Close()

			cfg := GeminiAPIPoolConfig{
				APIKey:     "test-key",
				Model:      "gemini-2.5-flash",
				BaseURL:    tsGemini.URL,
				HTTPClient: tsGemini.Client(),
			}

			if tsMCP != nil {
				cfg.MCPServers = []MCPServerConfig{
					{Name: "test-mcp", ServerURL: tsMCP.URL},
				}
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
			turn := &TurnContext{
				TurnID:    "turn-tool-test",
				Prompt:    "trigger action",
				Sink:      sink,
				CreatedAt: time.Now(),
			}

			err = sess.Send("trigger action", turn)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error=%v, got: %v", tc.expectErr, err)
			}

			sink.mu.Lock()
			defer sink.mu.Unlock()

			if tc.expectToolCall != "" {
				if len(sink.toolCalls) == 0 || !strings.HasPrefix(sink.toolCalls[0], tc.expectToolCall) {
					t.Errorf("expected tool call starting with %q, got %v", tc.expectToolCall, sink.toolCalls)
				}
			}
			if tc.expectResponseSub != "" {
				if sink.result == nil || !strings.Contains(sink.result.Response, tc.expectResponseSub) {
					t.Errorf("expected result containing %q, got %+v", tc.expectResponseSub, sink.result)
				}
			}
		})
	}
}

func TestGeminiAPISession_PersistenceAndHydrationCycle(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Persisted response\"}],\"role\":\"model\"}}]}" + "\n\n"))
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

	// Hydrate into new session
	freshSession := &GeminiAPISession{
		targetKey: sessID,
		sessionID: sessID,
		pool:      pool,
		dataDir:   tmpDir,
	}
	freshSession.hydrateHistory()

	hist := freshSession.History()
	if len(hist) != 2 || hist[0].Parts[0].Text != "First prompt" || hist[1].Parts[0].Text != "Persisted response" {
		t.Errorf("unexpected hydrated history: %+v", hist)
	}
}

func TestGeminiAPISession_AppendTranscriptBranches(t *testing.T) {
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

func TestGeminiAPISession_TranscriptPersistence_Suite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fileContent string
		expectedLen int
		firstText   string
	}{
		{
			name: "Clamping30TurnsTo20Messages",
			fileContent: func() string {
				var sb strings.Builder
				for i := 1; i <= 30; i++ {
					u, _ := json.Marshal(map[string]interface{}{"type": "USER_INPUT", "content": fmt.Sprintf("prompt %d", i)})
					m, _ := json.Marshal(map[string]interface{}{"type": "PLANNER_RESPONSE", "content": fmt.Sprintf("reply %d", i)})
					sb.WriteString(string(u) + "\n" + string(m) + "\n")
				}
				return sb.String()
			}(),
			expectedLen: 20,
			firstText:   "prompt 21",
		},
		{
			name:        "BlankLinesAndSpacing",
			fileContent: "\n\n  \n{\"type\":\"USER_INPUT\",\"content\":\"hello\"}\n\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"world\"}\n\n",
			expectedLen: 2,
			firstText:   "hello",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			sessID := "sess-" + tc.name
			p := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs", "transcript.jsonl")
			_ = os.MkdirAll(filepath.Dir(p), 0755)
			_ = os.WriteFile(p, []byte(tc.fileContent), 0644)

			pool := NewGeminiAPIPool(GeminiAPIPoolConfig{DataDir: tmpDir})
			sess := &GeminiAPISession{
				targetKey: sessID,
				sessionID: sessID,
				pool:      pool,
				dataDir:   tmpDir,
			}
			sess.hydrateHistory()

			hist := sess.History()
			if len(hist) != tc.expectedLen {
				t.Fatalf("expected len %d, got %d", tc.expectedLen, len(hist))
			}
			if tc.firstText != "" && hist[0].Parts[0].Text != tc.firstText {
				t.Errorf("expected first text %q, got %q", tc.firstText, hist[0].Parts[0].Text)
			}
		})
	}
}

func TestIsMissingSessionIDErr_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		body     []byte
		rpcErr   *jsonRPCResponse
		expected bool
	}{
		{name: "Status400", status: 400, expected: true},
		{name: "MissingSessionBody", status: 200, body: []byte("missing session ID"), expected: true},
		{
			name:     "RPCErrorBadRequest",
			status:   200,
			rpcErr:   &jsonRPCResponse{Error: &jsonRPCError{Code: -32600, Message: "Bad Request"}},
			expected: true,
		},
		{
			name:     "RPCErrorMissingSessionMsg",
			status:   200,
			rpcErr:   &jsonRPCResponse{Error: &jsonRPCError{Code: -32000, Message: "Missing session ID required"}},
			expected: true,
		},
		{name: "InternalServerError", status: 500, body: []byte("internal server error"), expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := isMissingSessionIDErr(tc.status, tc.body, tc.rpcErr)
			if res != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, res)
			}
		})
	}
}

func TestCleanParameters(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		input            map[string]interface{}
		expectedType     string
		nullable         bool
		checkNonNilProps bool
		checkAnyOfPres   bool
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
		{
			name: "OneOfFlatteningInterface",
			input: map[string]interface{}{
				"oneOf": []interface{}{
					map[string]interface{}{"type": "int"},
					map[string]interface{}{"type": "null"},
				},
			},
			expectedType: "INTEGER",
			nullable:     true,
		},
		{
			name: "OneOfFlatteningMapSlice",
			input: map[string]interface{}{
				"oneOf": []map[string]interface{}{
					{"type": "null"},
					{"type": "bool"},
				},
			},
			expectedType: "BOOLEAN",
			nullable:     true,
		},
		{
			name: "AnyOfFlatteningNullSecondWithTypeOverwrite",
			input: map[string]interface{}{
				"type": "string",
				"anyOf": []interface{}{
					map[string]interface{}{"type": "int", "description": "integer value"},
					map[string]interface{}{"type": "null"},
				},
			},
			expectedType: "INTEGER",
			nullable:     true,
		},
		{
			name: "OneOfFlatteningMapSliceNullSecond",
			input: map[string]interface{}{
				"oneOf": []map[string]interface{}{
					{"type": "bool"},
					{"type": "null"},
				},
			},
			expectedType: "BOOLEAN",
			nullable:     true,
		},
		{
			name: "UnflattenableTwoNonNulls",
			input: map[string]interface{}{
				"anyOf": []interface{}{
					map[string]interface{}{"type": "string"},
					map[string]interface{}{"type": "int"},
				},
			},
			expectedType:   "OBJECT",
			checkAnyOfPres: true,
		},
		{
			name: "UnflattenableThreeAnyOf",
			input: map[string]interface{}{
				"anyOf": []interface{}{
					map[string]interface{}{"type": "string"},
					map[string]interface{}{"type": "int"},
					map[string]interface{}{"type": "null"},
				},
			},
			expectedType:   "OBJECT",
			checkAnyOfPres: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cleaned := cleanParameters(tc.input)

			if cleaned["type"] != tc.expectedType {
				t.Errorf("%s: expected type %q, got %q", tc.name, tc.expectedType, cleaned["type"])
			}
			if tc.nullable && cleaned["nullable"] != true {
				t.Errorf("%s: expected nullable true, got %v", tc.name, cleaned["nullable"])
			}
			if tc.checkNonNilProps && cleaned["properties"] == nil {
				t.Errorf("%s: expected non-nil properties map", tc.name)
			}
			if tc.checkAnyOfPres {
				if _, exists := cleaned["anyOf"]; !exists {
					t.Errorf("%s: expected anyOf to remain preserved", tc.name)
				}
			}
		})
	}
}

func TestCleanParameters_StripsDisallowedKeys(t *testing.T) {
	t.Parallel()

	complexSchema := map[string]interface{}{
		"$schema":              "http://json-schema.org/draft-07/schema#",
		"title":                "TestSchema",
		"type":                 "object",
		"additionalProperties": false,
		"$ref":                 "#/defs",
		"$id":                  "id1",
		"definitions":          map[string]interface{}{},
		"default":              "val",
		"properties": map[string]interface{}{
			"prompt": map[string]interface{}{
				"type":                 "string",
				"title":                "Prompt Title",
				"additionalProperties": false,
			},
			"raw_field": "not-a-map",
			"items_slice": map[string]interface{}{
				"type": "array",
				"items": []interface{}{
					map[string]interface{}{"type": "int", "additionalProperties": false},
					"not-a-map",
				},
			},
			"items_map": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "bool",
				},
			},
			"empty_child": map[string]interface{}{},
			"optional_list": map[string]interface{}{
				"type": []interface{}{"null", "array"},
				"items": map[string]interface{}{
					"type": "string",
				},
			},
			"reversed_null": map[string]interface{}{
				"type": []string{"string", "null"},
			},
			"branch_field": map[string]interface{}{
				"anyOf": []interface{}{
					map[string]interface{}{"type": "null"},
					map[string]interface{}{"type": "string"},
				},
			},
		},
		"allOf": []map[string]interface{}{
			{"type": "string"},
		},
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

	if cleaned["type"] != "OBJECT" {
		t.Errorf("expected root type OBJECT, got %v", cleaned["type"])
	}
	props := cleaned["properties"].(map[string]interface{})
	prompt := props["prompt"].(map[string]interface{})
	if prompt["type"] != "STRING" {
		t.Errorf("expected prompt type STRING, got %v", prompt["type"])
	}
	emptyChild := props["empty_child"].(map[string]interface{})
	if emptyChild["type"] != "OBJECT" {
		t.Errorf("expected empty_child type OBJECT, got %v", emptyChild["type"])
	}
	optList := props["optional_list"].(map[string]interface{})
	if optList["type"] != "ARRAY" || optList["nullable"] != true {
		t.Errorf("expected optional_list ARRAY nullable, got %+v", optList)
	}
	revNull := props["reversed_null"].(map[string]interface{})
	if revNull["type"] != "STRING" || revNull["nullable"] != true {
		t.Errorf("expected reversed_null STRING nullable, got %+v", revNull)
	}
}

func TestCleanParameters_DeeplyNestedSchema(t *testing.T) {
	t.Parallel()

	deeplyNestedSchema := func() map[string]interface{} {
		root := map[string]interface{}{
			"type":                 "object",
			"additionalProperties": false,
		}
		curr := root
		for i := 0; i < 20; i++ {
			child := map[string]interface{}{
				"type":                 "object",
				"additionalProperties": false,
			}
			curr["properties"] = map[string]interface{}{
				"nested": child,
			}
			curr = child
		}
		curr["properties"] = map[string]interface{}{
			"leaf": map[string]interface{}{
				"type":                 "string",
				"additionalProperties": false,
			},
		}
		return root
	}()

	cleaned := cleanParameters(deeplyNestedSchema)
	if cleaned == nil {
		t.Fatalf("expected non-nil result from deeply nested schema")
	}
	probe := cleaned
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
}

func TestDefaultMCPDispatcher_SessionHandshake(t *testing.T) {
	t.Parallel()

	var initCount, notifyCount, listCount, callCount int32
	const validSessID = "sess-handshake-12345"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		sessHdr := r.Header.Get("mcp-session-id")
		switch req.Method {
		case "initialize":
			atomic.AddInt32(&initCount, 1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("mcp-session-id", validSessID)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{}}}`))
		case "notifications/initialized":
			atomic.AddInt32(&notifyCount, 1)
			if sessHdr != validSessID {
				http.Error(w, "bad session", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			atomic.AddInt32(&listCount, 1)
			if sessHdr != validSessID {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Bad Request: Missing session ID"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"test_tool","description":"A test tool","inputSchema":{"type":"object"}}]}}`))
		case "tools/call":
			atomic.AddInt32(&callCount, 1)
			if sessHdr != validSessID {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Bad Request: Missing session ID"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Executed successfully"}]}}`))
		default:
			http.Error(w, "unknown method", http.StatusNotFound)
		}
	}))
	defer ts.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "stateful-mcp", ServerURL: ts.URL}}, ts.Client())
	ctx := context.Background()

	decls, err := dispatcher.ListDeclarations(ctx)
	if err != nil || len(decls) != 1 || decls[0].Name != "test_tool" {
		t.Fatalf("ListDeclarations failed: decls=%+v, err=%v", decls, err)
	}
	if initCount != 1 || notifyCount != 1 || listCount != 2 {
		t.Errorf("expected counts 1/1/2, got init=%d, notify=%d, list=%d", initCount, notifyCount, listCount)
	}

	out, err := dispatcher.Execute(ctx, "test_tool", map[string]interface{}{})
	if err != nil || out != "Executed successfully" || callCount != 1 {
		t.Fatalf("Execute failed: out=%q, callCount=%d, err=%v", out, callCount, err)
	}

	const concurrency = 10
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			res, e := dispatcher.Execute(ctx, "test_tool", nil)
			if e != nil || res != "Executed successfully" {
				errCh <- fmt.Errorf("res=%q, err=%w", res, e)
			} else {
				errCh <- nil
			}
		}()
	}
	for i := 0; i < concurrency; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("concurrent execution failed: %v", e)
		}
	}
	if initCount != 1 {
		t.Errorf("expected initialize count 1 after concurrency, got %d", initCount)
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
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"tool_a","description":"Tool A"},{"name":"tool_b","description":"Tool B"}]}}`))
			return
		}
		if req.Method == "tools/call" {
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
		if reqNum == 1 {
			chunk := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"tool_a","args":{"arg":"1"}},"thoughtSignature":"sig_shared"},{"functionCall":{"name":"tool_b","args":{"arg":"2"}}}],"role":"model"}}],"index":0}` + "\n\n"
			_, _ = w.Write([]byte(chunk))
		} else {
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
		MCPServers: []MCPServerConfig{{Name: "multi-tool-mcp", ServerURL: tsMCP.URL}},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "voice-kiosk-multi", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-multi-tool", Prompt: "run tools", Sink: sink}
	if err := sess.Send("run tools", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.toolCalls) != 2 || sink.result == nil || sink.result.Response != "Both tools finished." {
		t.Fatalf("unexpected sink state: calls=%d, result=%+v", len(sink.toolCalls), sink.result)
	}
	if len(capturedTurn2Contents) != 3 {
		t.Fatalf("expected 3 contents in Turn 2, got %d", len(capturedTurn2Contents))
	}
	modelContent := capturedTurn2Contents[1]
	if modelContent.Role != "model" || len(modelContent.Parts) != 2 || modelContent.Parts[0].ThoughtSignature != "sig_shared" || modelContent.Parts[1].ThoughtSignature != "sig_shared" {
		t.Errorf("modelContent grouping failed: %+v", modelContent)
	}
	funcContent := capturedTurn2Contents[2]
	if funcContent.Role != "user" || len(funcContent.Parts) != 2 || funcContent.Parts[0].FunctionResponse.Name != "tool_a" || funcContent.Parts[1].FunctionResponse.Name != "tool_b" {
		t.Errorf("funcContent grouping failed: %+v", funcContent)
	}
}

func TestGeminiAPIPool_EmptyResponseHistoryNotPoisoned(t *testing.T) {
	t.Parallel()

	tsGemini := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"candidates":[{"content":{"parts":[{"text":""}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"))
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
	turn := &TurnContext{TurnID: "turn-empty", Prompt: "hello", Sink: sink}
	if err := sess.Send("hello", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	apiSess := sess.(*GeminiAPISession)
	if len(apiSess.history) != 0 {
		t.Errorf("expected empty history, got %d items: %+v", len(apiSess.history), apiSess.history)
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
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{DataDir: tmpDir})
	defer pool.Close()
	if !strings.Contains(pool.cfg.SystemPrompt, ruleContent) {
		t.Errorf("expected system prompt to contain rule content, got: %q", pool.cfg.SystemPrompt)
	}
}

func TestGeminiAPIPool_NilReceiverAndDefaultClient(t *testing.T) {
	t.Parallel()

	var nilPool *GeminiAPIPool
	if nilPool.Model() != "" || nilPool.APIKey() != "" || nilPool.PrewarmedTargets() != nil {
		t.Errorf("nilPool getters returned unexpected values")
	}
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "key", Model: "gemini-2.5-flash"})
	if pool.cfg.HTTPClient == nil {
		t.Errorf("expected default HTTPClient to be initialized")
	}
	_ = pool.Close()
}

func TestGeminiAPIPool_SessionRotator(t *testing.T) {
	t.Parallel()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "dummy"})
	defer pool.Close()
	ctx := context.Background()
	sess, err := pool.GetOrCreateSession(ctx, "target-rot", "")
	if err != nil {
		t.Fatalf("GetOrCreateSession failed: %v", err)
	}
	if should, _ := pool.ShouldRotateSession(sess); should {
		t.Errorf("expected ShouldRotateSession to return false")
	}
	rotSess, err := pool.RotateSession(ctx, "target-rot")
	if err != nil || rotSess == nil {
		t.Fatalf("RotateSession failed: sess=%v, err=%v", rotSess, err)
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
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"test_tool\",\"args\":{\"foo\":\"bar\"}}}]}}]}" + "\n\n"))
		case 2:
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}" + "\n\n"))
		default:
			finalReqBody = body
			_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Here is your calendar summary.\"}]}}]}" + "\n\n"))
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:        "test-key",
		Model:         "gemini-2.5-flash",
		BaseURL:       ts.URL,
		HTTPClient:    ts.Client(),
		MCPDispatcher: &fallbackMockDispatcher{},
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-exhaust-device", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "turn-exhaust", Prompt: "what is on my calendar?", Sink: sink, CreatedAt: time.Now()}
	if err := sess.Send("what is on my calendar?", turn); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	sink.mu.Lock()
	collectedText := strings.Join(sink.deltas, "")
	sink.mu.Unlock()

	if !strings.Contains(collectedText, "Here is your calendar summary.") {
		t.Errorf("expected fallback text in sink, got: %q", collectedText)
	}
	if strings.Contains(string(finalReqBody), "\"tools\":[") {
		t.Errorf("expected fallback body without tools, got: %s", string(finalReqBody))
	}
}

func TestMCPDispatcher_MissingSessionRetry(t *testing.T) {
	t.Parallel()

	var initCalls, execCalls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "initialize":
			atomic.AddInt32(&initCalls, 1)
			w.Header().Set("mcp-session-id", "new-sess-123")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"test_exec","description":"Test Exec"}]}}`))
		case "tools/call":
			callNum := atomic.AddInt32(&execCalls, 1)
			if callNum == 1 || r.Header.Get("mcp-session-id") != "new-sess-123" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":-32600,"message":"Missing session ID"}`))
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Success after retry"}]}}`))
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer ts.Close()

	dispatcher := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "exec-retry-srv", ServerURL: ts.URL}}, ts.Client())
	res, err := dispatcher.Execute(context.Background(), "test_exec", nil)
	if err != nil || res != "Success after retry" {
		t.Fatalf("Execute failed: res=%q, err=%v", res, err)
	}
	if initCount := atomic.LoadInt32(&initCalls); initCount < 1 || initCount > 2 {
		t.Errorf("expected 1-2 init calls, got %d", initCount)
	}
	if atomic.LoadInt32(&execCalls) != 2 {
		t.Errorf("expected 2 exec calls, got %d", atomic.LoadInt32(&execCalls))
	}
}

func TestMCPDispatcher_ListDeclarationsErrorAndRetry(t *testing.T) {
	t.Parallel()

	var nilDisp *DefaultMCPDispatcher
	nilDecls, err := nilDisp.ListDeclarations(context.Background())
	if err != nil || nilDecls != nil {
		t.Errorf("expected nil, nil for nil dispatcher, got %v, %v", nilDecls, err)
	}

	tsRetryFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-32600,"message":"Missing session ID"}`))
	}))
	defer tsRetryFail.Close()

	dispFail := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "retry-fail-srv", ServerURL: tsRetryFail.URL}}, tsRetryFail.Client())
	decls, err := dispFail.ListDeclarations(context.Background())
	if err != nil || len(decls) != 0 {
		t.Errorf("expected 0 decls from failing srv, got %d, err=%v", len(decls), err)
	}
	if _, err := dispFail.Execute(context.Background(), "non_existent_tool", nil); err == nil {
		t.Errorf("expected error executing non-existent tool")
	}
}

func TestMCPDispatcher_NormalizeGoogleUserEmail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		inputEmail      string
		calendarID      string
		expectedEmail   string
		shouldHaveEmail bool
	}{
		{name: "Primary", inputEmail: "primary", calendarID: "primary", shouldHaveEmail: false},
		{name: "Me", inputEmail: "me", shouldHaveEmail: false},
		{name: "UserEmail", inputEmail: "user@example.com", expectedEmail: "user@example.com", shouldHaveEmail: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

			disp := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "google", ServerURL: ts.URL}}, ts.Client())
			ctx := context.Background()
			_, _ = disp.ListDeclarations(ctx)

			args := map[string]interface{}{"user_google_email": tc.inputEmail}
			if tc.calendarID != "" {
				args["calendar_id"] = tc.calendarID
			}
			if _, err := disp.Execute(ctx, "get_events", args); err != nil {
				t.Fatalf("Execute failed for %s: %v", tc.inputEmail, err)
			}
			val, exists := receivedParams["user_google_email"]
			if exists != tc.shouldHaveEmail {
				t.Errorf("case %q: expected exists=%v, got %v", tc.inputEmail, tc.shouldHaveEmail, exists)
			}
			if tc.shouldHaveEmail && val != tc.expectedEmail {
				t.Errorf("case %q: expected %q, got %v", tc.inputEmail, tc.expectedEmail, val)
			}
			if tc.calendarID != "" && receivedParams["calendar_id"] != tc.calendarID {
				t.Errorf("case %q: expected calendar_id=%q, got %v", tc.inputEmail, tc.calendarID, receivedParams["calendar_id"])
			}
		})
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

func TestDefaultMCPDispatcher_NewGeminiAPIPoolWiring(t *testing.T) {
	t.Parallel()
	ts := newAllowedToolsMockServer()
	defer ts.Close()

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
	decls, err := disp.ListDeclarations(context.Background())
	if err != nil || len(decls) != 1 || decls[0].Name != "get_events" {
		t.Errorf("expected 1 declaration for get_events, got: %+v, err=%v", decls, err)
	}
}

func TestDefaultMCPDispatcher_AllowedToolsFilteringAndExecution(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		allowedTools  []string
		passNilSlice  bool
		expectedDecls int
		testTool      string
		expectErr     bool
		errSubstr     string
	}{
		{
			name:          "FilteringAndGatingActive",
			allowedTools:  []string{"ha_call_read_tool", "get_events"},
			expectedDecls: 2,
			testTool:      "ha_call_read_tool",
			expectErr:     false,
		},
		{
			name:          "FilteringRejection",
			allowedTools:  []string{"ha_call_read_tool", "get_events"},
			expectedDecls: 2,
			testTool:      "batch_modify_gmail_message_labels",
			expectErr:     true,
			errSubstr:     "not in the allowed voice tools list",
		},
		{
			name:          "OmittedOrNilAllowedToolsAllowsAll",
			passNilSlice:  true,
			expectedDecls: 3,
			testTool:      "batch_modify_gmail_message_labels",
			expectErr:     false,
		},
		{
			name:          "ExplicitEmptyAllowedToolsBlocksAll",
			allowedTools:  []string{},
			expectedDecls: 0,
			testTool:      "ha_call_read_tool",
			expectErr:     true,
			errSubstr:     "not in the allowed voice tools list",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newAllowedToolsMockServer()
			defer ts.Close()

			var slice []string
			if !tc.passNilSlice {
				slice = tc.allowedTools
			}
			disp := NewDefaultMCPDispatcher([]MCPServerConfig{{Name: "test-mcp", ServerURL: ts.URL}}, ts.Client(), slice)
			ctx := context.Background()

			decls, err := disp.ListDeclarations(ctx)
			if err != nil {
				t.Fatalf("ListDeclarations failed: %v", err)
			}
			if len(decls) != tc.expectedDecls {
				t.Errorf("expected %d decls, got %d", tc.expectedDecls, len(decls))
			}

			res, err := disp.Execute(ctx, tc.testTool, nil)
			if tc.expectErr {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("expected error containing %q, got err=%v", tc.errSubstr, err)
				}
			} else {
				if err != nil || !strings.Contains(res, "executed: "+tc.testTool) {
					t.Fatalf("expected successful execution of %q, got res=%q, err=%v", tc.testTool, res, err)
				}
			}
		})
	}
}

func TestGeminiAPISession_Send_Retrievers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                 string
		isAmbient            bool
		preSeededPrompt      string
		normalPrompt         string
		retrieverSnippet     string
		modelResponse        string
		expectedWireTag      string
		expectedContent      string
		expectedHistoryClean string
	}{
		{
			name:                 "MemoryRetriever_Normal",
			isAmbient:            false,
			normalPrompt:         "What is my theme preference?",
			retrieverSnippet:     "<retrieved_memory>\n- User prefers dark mode\n</retrieved_memory>",
			modelResponse:        "Got your preference!",
			expectedWireTag:      "<retrieved_memory>",
			expectedContent:      "User prefers dark mode",
			expectedHistoryClean: "What is my theme preference?",
		},
		{
			name:                 "MemoryRetriever_NoDoubleInject",
			isAmbient:            false,
			preSeededPrompt:      "<retrieved_memory>\n- Already present\n</retrieved_memory>\n\nHello",
			retrieverSnippet:     "<retrieved_memory>\n- Extra memory\n</retrieved_memory>",
			modelResponse:        "Acknowledged",
			expectedWireTag:      "<retrieved_memory>",
			expectedContent:      "Already present",
			expectedHistoryClean: "Already present",
		},
		{
			name:                 "AmbientContextRetriever_Normal",
			isAmbient:            true,
			normalPrompt:         "turn off the living room lights",
			retrieverSnippet:     "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>",
			modelResponse:        "Turned them off!",
			expectedWireTag:      "<ambient_context>",
			expectedContent:      "Living Room: lights are ON",
			expectedHistoryClean: "turn off the living room lights",
		},
		{
			name:                 "AmbientContextRetriever_NoDoubleInject",
			isAmbient:            true,
			preSeededPrompt:      "<ambient_context>\nExisting ambient\n</ambient_context>\n\nHello",
			retrieverSnippet:     "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>",
			modelResponse:        "Acknowledged",
			expectedWireTag:      "<ambient_context>",
			expectedContent:      "Existing ambient",
			expectedHistoryClean: "Existing ambient",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			retrieverCalled := false
			var receivedBody []byte

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				receivedBody = b
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(fmt.Sprintf(`data: {"candidates":[{"content":{"parts":[{"text":%q}],"role":"model"}}]}

`, tc.modelResponse)))
			}))
			defer ts.Close()

			poolCfg := GeminiAPIPoolConfig{
				APIKey:     "test-key",
				Model:      "gemini-2.5-flash",
				BaseURL:    ts.URL,
				HTTPClient: ts.Client(),
			}

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
			turn := &TurnContext{TurnID: "turn-retriever", Prompt: prompt, Sink: sink}
			if err := sess.Send(prompt, turn); err != nil {
				t.Fatalf("Send failed: %v", err)
			}

			if tc.preSeededPrompt != "" && retrieverCalled {
				t.Errorf("expected retriever NOT to be called when tag already present")
			}

			var req geminiStreamRequest
			if err := json.Unmarshal(receivedBody, &req); err != nil {
				t.Fatalf("failed to unmarshal request: %v", err)
			}
			if len(req.Contents) == 0 || len(req.Contents[0].Parts) == 0 {
				t.Fatalf("expected content parts in request")
			}
			wireText := req.Contents[0].Parts[0].Text
			if strings.Count(wireText, tc.expectedWireTag) != 1 {
				t.Errorf("expected exactly 1 %s, got: %s", tc.expectedWireTag, wireText)
			}
			if !strings.Contains(wireText, tc.expectedContent) {
				t.Errorf("expected wireText to contain %q, got: %s", tc.expectedContent, wireText)
			}

			hist := sess.(*GeminiAPISession).History()
			if len(hist) != 2 {
				t.Fatalf("expected 2 history items, got %d", len(hist))
			}
			if hist[0].Role != "user" || !strings.Contains(hist[0].Parts[0].Text, tc.expectedHistoryClean) {
				t.Errorf("expected history to contain clean prompt %q, got: %+v", tc.expectedHistoryClean, hist[0])
			}
			if tc.preSeededPrompt == "" && strings.Contains(hist[0].Parts[0].Text, tc.expectedWireTag) {
				t.Errorf("history should not contain %s when dynamically injected", tc.expectedWireTag)
			}
			if hist[1].Role != "model" || hist[1].Parts[0].Text != tc.modelResponse {
				t.Errorf("expected model history %q, got: %+v", tc.modelResponse, hist[1])
			}
		})
	}
}

func TestParseJSONRPCBody_Coverage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		body      []byte
		expectErr bool
	}{
		{name: "nil body", body: nil, expectErr: true},
		{name: "whitespace body", body: []byte("   \n\t  "), expectErr: true},
		{name: "standard json", body: []byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`), expectErr: false},
		{name: "sse valid", body: []byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[]}}\n\n"), expectErr: false},
		{name: "sse non-json data", body: []byte("event: message\ndata: not json\n\n"), expectErr: true},
		{name: "completely invalid json", body: []byte("not json at all"), expectErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, err := parseJSONRPCBody(tc.body)
			if tc.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.expectErr && (err != nil || resp == nil) {
				t.Errorf("expected successful parse, got resp=%v, err=%v", resp, err)
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

	cases := []struct {
		name                string
		nilPool             bool
		emptyTarget         bool
		maxTurns            int
		turnPrompts         []string
		responseStatusCodes []int
		responseChunks      []string
		useChannelFallback  bool
		onRotateErr         error
		expectInitCold      bool
		expectExecErr       bool
		expectReleaseErr    error
		expectRotation      bool
		expectEvictedOnFail bool
		expectNilTurnErr    bool
	}{
		{
			name:    "Validation_NilPool",
			nilPool: true,
		},
		{
			name:        "Validation_EmptyTarget",
			emptyTarget: true,
		},
		{
			name:                "BasicExecution_ColdAndWarmAndNilTurn",
			maxTurns:            10,
			turnPrompts:         []string{"hello", "hello again"},
			responseStatusCodes: []int{http.StatusOK, http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
			expectInitCold:   true,
			expectNilTurnErr: true,
		},
		{
			name:                "ColdExecution_ChannelInstructionsFallback",
			maxTurns:            10,
			turnPrompts:         []string{"hello chan"},
			responseStatusCodes: []int{http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
			useChannelFallback: true,
			expectInitCold:     true,
		},
		{
			name:                "RotationAndPersistence_Success",
			maxTurns:            1,
			turnPrompts:         []string{"first message"},
			responseStatusCodes: []int{http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
			expectRotation: true,
		},
		{
			name:                "PersistenceFailureRollback",
			maxTurns:            1,
			turnPrompts:         []string{"trigger rotation"},
			responseStatusCodes: []int{http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
		onRotateErr:         simulatedDBErr,
		expectReleaseErr:    simulatedDBErr,
		expectEvictedOnFail: true,
		},
		{
			name:                "CancelRotation_On429QuotaExhausted",
			maxTurns:            1,
			turnPrompts:         []string{"first message"},
			responseStatusCodes: []int{http.StatusTooManyRequests},
			responseChunks: []string{
				`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`,
			},
		expectExecErr:  true,
		expectRotation: false,
		},
		{
			name:                "ClearsCancelRotationOnSuccessfulRetry",
			maxTurns:            1,
			turnPrompts:         []string{"attempt 1", "attempt 2"},
			responseStatusCodes: []int{http.StatusTooManyRequests, http.StatusOK},
			responseChunks: []string{
				`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`,
				`data: {"candidates":[{"content":{"parts":[{"text":"Retry succeeded."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
		expectRotation: true,
		},
		{
			name:                "ConversationalRateLimitTextDoesNotCancelRotation",
			maxTurns:            1,
			turnPrompts:         []string{"check status"},
			responseStatusCodes: []int{http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"I hit an external rate limit reached earlier, but cached the response."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
		expectRotation: true,
		},
		{
			name:                "DefaultMaxSessionTurnsFallback",
			maxTurns:            0, // defaults to 8
			turnPrompts:         []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"},
			responseStatusCodes: []int{http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK, http.StatusOK},
			responseChunks: []string{
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
				`data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n",
			},
		expectRotation: true,
		},
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
				status := http.StatusOK
				if idx < len(tc.responseStatusCodes) {
					status = tc.responseStatusCodes[idx]
				}
				chunk := ""
				if idx < len(tc.responseChunks) {
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
				APIKey:          "test-key",
				Model:           "gemini-2.5-flash",
				BaseURL:         ts.URL,
				HTTPClient:      ts.Client(),
				MaxSessionTurns: tc.maxTurns,
				GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
					return SessionRecord{
						ActiveSessionID:   "550e8400-e29b-41d4-a716-446655440001",
						PreviousSessionID: "550e8400-e29b-41d4-a716-446655440000",
						TurnCount:         0,
					}, nil
				},
				OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
					rotationCount++
					rotatedOld = oldSessionID
					rotatedNew = newSessionID
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

			if tc.expectInitCold {
				if !lease.IsCold() || lease.TurnCount() != 0 || lease.PreviousSessionID() != "550e8400-e29b-41d4-a716-446655440000" || lease.SessionID() != "550e8400-e29b-41d4-a716-446655440001" {
					t.Errorf("initial lease state mismatch: cold=%v, count=%d, prev=%s, sess=%s", lease.IsCold(), lease.TurnCount(), lease.PreviousSessionID(), lease.SessionID())
				}
			}

			initSessID := lease.SessionID()

			for i, prompt := range tc.turnPrompts {
				turnCtx := &TurnContext{
					TurnID: fmt.Sprintf("turn-%d", i+1),
					Prompt: prompt,
					Ctx:    ctx,
				}
				if tc.useChannelFallback {
					turnCtx.ChannelInstructions = "channel guideline"
				} else if i == 0 && tc.expectInitCold {
					turnCtx.ScopeInstructions = "scope rule"
					turnCtx.CoordinationContext = "coord info"
					turnCtx.ThreadSummary = "summary of thread"
				}

				res, execErr := lease.Execute(ctx, turnCtx)
				if execErr != nil {
					if !tc.expectExecErr && i != 0 {
						t.Fatalf("turn %d Execute failed: %v", i+1, execErr)
					}
					if tc.expectExecErr {
						if !IsQuotaPause(execErr.Error(), "") && !IsCapacityBlip(execErr.Error(), "") {
							t.Errorf("expected quota/capacity error, got: %v", execErr)
						}
					}
				} else {
					if res == nil {
						t.Errorf("turn %d expected non-nil response", i+1)
					}
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
				if rotationCount == 0 {
					t.Errorf("expected rotation, got 0")
				}
				if rotatedOld != initSessID {
					t.Errorf("expected rotatedOld %s, got %s", initSessID, rotatedOld)
				}
				if rotatedNew == "" || rotatedNew == rotatedOld {
					t.Errorf("expected new session ID, got %s", rotatedNew)
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
		t.Errorf("expected session to be removed from pool after lease.MarkDirty()")
	}

	var nilLease *geminiSessionLease
	nilLease.MarkDirty()
}
