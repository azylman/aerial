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

func TestGeminiAPIPool_LifecycleAndAccessors_Suite(t *testing.T) {
	t.Parallel()

	t.Run("NilPoolAccessors", func(t *testing.T) {
		t.Parallel()
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
	})

	t.Run("PoolAccessorsAndSessions", func(t *testing.T) {
		t.Parallel()
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

		// PrewarmedTargets check and defensive copy mutation
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
	})

	t.Run("MarkDirty_SessionRecreation", func(t *testing.T) {
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
	})

	t.Run("PrewarmedAndCloseOperations", func(t *testing.T) {
		t.Parallel()
		targets := []string{"living-room", "kitchen", "office"}
		pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
			APIKey:           "test-key",
			Model:            "gemini-2.5-flash",
			PrewarmedTargets: targets,
		})
		ctx := context.Background()
		if err := pool.Initialize(ctx); err != nil {
			t.Fatalf("unexpected Initialize error: %v", err)
		}
		for _, target := range targets {
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
	})

	t.Run("SessionEdgeCasesAndJSONRPCBody", func(t *testing.T) {
		t.Parallel()
		sess := &GeminiAPISession{targetKey: "fallback-target"}
		if sess.SessionID() != "fallback-target" {
			t.Errorf("expected fallback session ID")
		}
		if sess.transcriptPath() != "" {
			t.Errorf("expected empty transcript path when dataDir is empty")
		}

		// parseJSONRPCBody test table
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
	})
}

type errMockDispatcher struct{}

func (d *errMockDispatcher) ListDeclarations(ctx context.Context) ([]geminiFunctionDeclaration, error) {
	return nil, errors.New("declarations query failed")
}

func (d *errMockDispatcher) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	return "", errors.New("execute failed")
}

func TestMCPDispatcher_Suite(t *testing.T) {
	t.Parallel()

	t.Run("ListDeclarations_JSONAndSSE", func(t *testing.T) {
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
	})

	t.Run("Execute_SuccessAndErrorPaths", func(t *testing.T) {
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
	})

	t.Run("Dispatcher_DeclarationsAndExecutionEdgeCases", func(t *testing.T) {
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
			t.Fatalf("expected 1 valid declaration 'valid_t', got %d decls (%v)", len(decls), err)
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
	})

	t.Run("HeadersAndMultiPartContent", func(t *testing.T) {
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
	})
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

func TestGeminiAPISession_TranscriptPersistence_Suite(t *testing.T) {
	t.Parallel()

	t.Run("PersistenceAndHydrationCycle", func(t *testing.T) {
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
	})

	t.Run("HydrationPruningAndBlankLines", func(t *testing.T) {
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
	})

	t.Run("AppendTranscript_BranchCoverage", func(t *testing.T) {
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
	})
}

func TestCleanParameters(t *testing.T) {
	t.Parallel()

	t.Run("DeclarativeSchemaCleaningAndPolymorphism", func(t *testing.T) {
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
		}
	})

	t.Run("Strips_Disallowed_Keys_And_Preserves_Structure", func(t *testing.T) {
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

		res := cleanParameters(complexSchema)

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

		if res["type"] != "OBJECT" {
			t.Errorf("expected root type OBJECT, got %v", res["type"])
		}
		props := res["properties"].(map[string]interface{})
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
	})

	t.Run("DeeplyNestedSchema_Preserved", func(t *testing.T) {
		t.Parallel()
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

		res := cleanParameters(root)
		if res == nil {
			t.Fatalf("expected non-nil result from deeply nested schema")
		}

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

	t.Run("IsMissingSessionIDErr_Declarative", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			status    int
			body      []byte
			rpcErr    *jsonRPCResponse
			expected  bool
		}{
			{status: 400, expected: true},
			{status: 200, body: []byte("missing session ID"), expected: true},
			{
				status:   200,
				rpcErr:   &jsonRPCResponse{Error: &jsonRPCError{Code: -32600, Message: "Bad Request"}},
				expected: true,
			},
			{
				status:   200,
				rpcErr:   &jsonRPCResponse{Error: &jsonRPCError{Code: -32000, Message: "Missing session ID required"}},
				expected: true,
			},
			{status: 500, body: []byte("internal server error"), expected: false},
		}

		for i, tc := range tests {
			res := isMissingSessionIDErr(tc.status, tc.body, tc.rpcErr)
			if res != tc.expected {
				t.Errorf("case %d: expected %v, got %v", i, tc.expected, res)
			}
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
	if initCount := atomic.LoadInt32(&initCalls); initCount < 1 || initCount > 2 {
		t.Errorf("expected 1 or 2 initialize calls, got %d", initCount)
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

func TestGeminiAPISession_Send_WithAmbientContextRetriever(t *testing.T) {
	t.Parallel()

	retriever := func(ctx context.Context) (string, error) {
		return "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>", nil
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
		_, _ = w.Write([]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Turned them off!\"}],\"role\":\"model\"}}]}\n\n"))
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:                  "test-key",
		Model:                   "gemini-2.5-flash",
		BaseURL:                 ts.URL,
		HTTPClient:              ts.Client(),
		AmbientContextRetriever: retriever,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-sess-ambient", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	geminiSess := sess.(*GeminiAPISession)
	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-amb-1",
		Prompt: "turn off the living room lights",
		Sink:   sink,
	}
	err = sess.Send("turn off the living room lights", turn)
	if err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	// 1. Verify wire payload sent to API contains <ambient_context>
	var req geminiStreamRequest
	if err := json.Unmarshal(receivedBody, &req); err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}
	if len(req.Contents) == 0 || len(req.Contents[0].Parts) == 0 {
		t.Fatalf("expected at least 1 content part in request")
	}
	firstText := req.Contents[0].Parts[0].Text
	if !strings.Contains(firstText, "<ambient_context>") || !strings.Contains(firstText, "Living Room: lights are ON") {
		t.Errorf("expected payload to contain ambient context, got: %s", firstText)
	}
	if !strings.Contains(firstText, "turn off the living room lights") {
		t.Errorf("expected payload to contain user prompt, got: %s", firstText)
	}

	// 2. Verify s.History() contains clean prompt without <ambient_context>
	hist := geminiSess.History()
	if len(hist) != 2 {
		t.Fatalf("expected 2 history items, got %d", len(hist))
	}
	if hist[0].Role != "user" || strings.Contains(hist[0].Parts[0].Text, "<ambient_context>") || hist[0].Parts[0].Text != "turn off the living room lights" {
		t.Errorf("expected clean prompt in user history, got: %+v", hist[0])
	}
	if hist[1].Role != "model" || hist[1].Parts[0].Text != "Turned them off!" {
		t.Errorf("expected model response in history, got: %+v", hist[1])
	}
}

func TestGeminiAPISession_Send_AmbientContextRetriever_NoDoubleInject(t *testing.T) {
	t.Parallel()

	retrieverCalled := false
	retriever := func(ctx context.Context) (string, error) {
		retrieverCalled = true
		return "<ambient_context>\nLiving Room: lights are ON\n</ambient_context>", nil
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
		APIKey:                  "test-key",
		Model:                   "gemini-2.5-flash",
		BaseURL:                 ts.URL,
		HTTPClient:              ts.Client(),
		AmbientContextRetriever: retriever,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "test-sess-amb-no-double", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	promptWithAmbient := "<ambient_context>\nExisting ambient\n</ambient_context>\n\nHello"
	sink := newMockTurnSink()
	turn := &TurnContext{
		TurnID: "turn-amb-no-double",
		Prompt: promptWithAmbient,
		Sink:   sink,
	}
	if err := sess.Send(promptWithAmbient, turn); err != nil {
		t.Fatalf("unexpected Send error: %v", err)
	}

	if retrieverCalled {
		t.Errorf("expected AmbientContextRetriever not to be called when prompt already has <ambient_context>")
	}
	var noDoubleReq geminiStreamRequest
	if err := json.Unmarshal(receivedBody, &noDoubleReq); err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}
	if len(noDoubleReq.Contents) == 0 || len(noDoubleReq.Contents[0].Parts) == 0 {
		t.Fatalf("expected at least 1 content part in request")
	}
	firstReqText := noDoubleReq.Contents[0].Parts[0].Text
	if strings.Count(firstReqText, "<ambient_context>") != 1 {
		t.Errorf("expected exactly 1 <ambient_context> block, got: %s", firstReqText)
	}
}

func TestParseJSONRPCBody_Coverage(t *testing.T) {
	t.Parallel()

	// 1. Empty body
	if _, err := parseJSONRPCBody(nil); err == nil {
		t.Errorf("expected error on nil body")
	}
	if _, err := parseJSONRPCBody([]byte("   \n\t  ")); err == nil {
		t.Errorf("expected error on whitespace body")
	}

	// 2. Standard JSON
	resp, err := parseJSONRPCBody([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}`))
	if err != nil {
		t.Fatalf("unexpected error on standard json: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}

	// 3. SSE Stream with data: prefix and valid JSON
	sseBody := []byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[]}}\n\n")
	respSSE, err := parseJSONRPCBody(sseBody)
	if err != nil {
		t.Fatalf("unexpected error on SSE body: %v", err)
	}
	if respSSE == nil {
		t.Fatalf("expected non-nil response from SSE")
	}

	// 4. SSE Stream with non-JSON data line falling through to standard unmarshal error
	sseInvalid := []byte("event: message\ndata: not json\n\n")
	if _, err := parseJSONRPCBody(sseInvalid); err == nil {
		t.Errorf("expected error on invalid SSE json data")
	}

	// 5. Standard invalid JSON
	if _, err := parseJSONRPCBody([]byte("not json at all")); err == nil {
		t.Errorf("expected error on completely invalid json")
	}
}

func TestGeminiAPIPool_SessionRotator(t *testing.T) {
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "dummy"})
	defer pool.Close()

	ctx := context.Background()
	sess, err := pool.GetOrCreateSession(ctx, "target-rot", "")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}

	if should, _ := pool.ShouldRotateSession(sess); should {
		t.Errorf("expected ShouldRotateSession to return false")
	}

	rotSess, err := pool.RotateSession(ctx, "target-rot")
	if err != nil {
		t.Fatalf("unexpected RotateSession error: %v", err)
	}
	if rotSess == nil {
		t.Fatal("expected non-nil rotated session")
	}
}

func TestGeminiAPIPool_AcquireLease_BasicExecution(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"hello from lease"}],"role":"model"}}],"finishReason":"STOP","index":0,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer ts.Close()

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{
				ActiveSessionID:   "550e8400-e29b-41d4-a716-446655440001",
				PreviousSessionID: "550e8400-e29b-41d4-a716-446655440000",
				TurnCount:         0,
			}, nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-lease-1")
	if err != nil {
		t.Fatalf("unexpected AcquireLease error: %v", err)
	}

	if !lease.IsCold() {
		t.Errorf("expected IsCold to be true")
	}
	if lease.PreviousSessionID() != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected prev session %q, got %q", "550e8400-e29b-41d4-a716-446655440000", lease.PreviousSessionID())
	}
	if lease.SessionID() != "550e8400-e29b-41d4-a716-446655440001" {
		t.Errorf("expected session %q, got %q", "550e8400-e29b-41d4-a716-446655440001", lease.SessionID())
	}
	if lease.TurnCount() != 0 {
		t.Errorf("expected TurnCount 0, got %d", lease.TurnCount())
	}

	// 1. Cold execution with ScopeInstructions, CoordinationContext, ThreadSummary
	turnCtx := &TurnContext{
		TurnID:              "turn-1",
		Prompt:              "hello",
		ScopeInstructions:   "scope rule",
		CoordinationContext: "coord info",
		ThreadSummary:       "summary of thread",
		Ctx:                 ctx,
	}

	res, execErr := lease.Execute(ctx, turnCtx)
	if execErr != nil {
		t.Fatalf("unexpected Execute error: %v", execErr)
	}
	if res == nil || res.Response != "hello from lease" {
		t.Errorf("expected 'hello from lease', got %+v", res)
	}
	if lease.IsCold() {
		t.Errorf("expected IsCold to be false after execution")
	}
	if lease.TurnCount() != 1 {
		t.Errorf("expected TurnCount 1, got %d", lease.TurnCount())
	}

	// 2. Warm execution
	turnCtx2 := &TurnContext{
		TurnID: "turn-2",
		Prompt: "hello again",
		Ctx:    ctx,
	}
	res2, execErr2 := lease.Execute(ctx, turnCtx2)
	if execErr2 != nil {
		t.Fatalf("unexpected warm Execute error: %v", execErr2)
	}
	if res2 == nil || res2.Response != "hello from lease" {
		t.Errorf("expected 'hello from lease', got %+v", res2)
	}
	if lease.TurnCount() != 2 {
		t.Errorf("expected TurnCount 2, got %d", lease.TurnCount())
	}

	// 3. Error branch: nil turn context
	if _, err := lease.Execute(ctx, nil); err == nil {
		t.Error("expected error when executing nil turn context")
	}

	if relErr := lease.Release(); relErr != nil {
		t.Errorf("unexpected Release error: %v", relErr)
	}

	// 4. Cold execution with ChannelInstructions fallback
	leaseChan, err := pool.AcquireLease(ctx, "target-chan-fallback")
	if err != nil {
		t.Fatalf("unexpected AcquireLease error: %v", err)
	}
	turnChan := &TurnContext{
		TurnID:              "turn-chan",
		Prompt:              "hello chan",
		ChannelInstructions: "channel guideline",
		Ctx:                 ctx,
	}
	if _, err := leaseChan.Execute(ctx, turnChan); err != nil {
		t.Fatalf("unexpected Execute error with channel instructions: %v", err)
	}
	_ = leaseChan.Release()
}

func TestGeminiAPIPool_AcquireLease_RotationAndPersistence(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer ts.Close()

	var rotatedOld, rotatedNew string
	var rotationCalled bool

	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCalled = true
			rotatedOld = oldSessionID
			rotatedNew = newSessionID
			return nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-rot-1")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	turnCtx := &TurnContext{
		TurnID: "turn-1",
		Prompt: "first message",
		Ctx:    ctx,
	}

	if _, execErr := lease.Execute(ctx, turnCtx); execErr != nil {
		t.Fatalf("Execute error: %v", execErr)
	}

	initSessID := lease.SessionID()
	if relErr := lease.Release(); relErr != nil {
		t.Fatalf("Release error: %v", relErr)
	}

	if !rotationCalled {
		t.Errorf("expected OnSessionRotated to be called")
	}
	if rotatedOld != initSessID {
		t.Errorf("expected rotatedOld %q, got %q", initSessID, rotatedOld)
	}
	if rotatedNew == "" || rotatedNew == rotatedOld {
		t.Errorf("expected fresh rotatedNew, got %q", rotatedNew)
	}
}

func TestGeminiAPIPool_AcquireLease_PersistenceFailureRollback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer ts.Close()

	expectedErr := errors.New("simulated DB latch timeout")
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			return expectedErr
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-fail-rot")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	turnCtx := &TurnContext{
		TurnID: "turn-1",
		Prompt: "trigger rotation",
		Ctx:    ctx,
	}

	if _, execErr := lease.Execute(ctx, turnCtx); execErr != nil {
		t.Fatalf("Execute error: %v", execErr)
	}

	relErr := lease.Release()
	if !errors.Is(relErr, expectedErr) {
		t.Errorf("expected Release to propagate DB error %v, got %v", expectedErr, relErr)
	}

	// Session should be evicted from pool
	pool.mu.RLock()
	_, exists := pool.sessions["target-fail-rot"]
	pool.mu.RUnlock()
	if exists {
		t.Errorf("expected session to be evicted from pool after DB latch failure")
	}
}

func TestGeminiAPIPool_AcquireLease_CancelRotation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`))
	}))
	defer ts.Close()

	var rotationCalled bool
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCalled = true
			return nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-cancel-rot")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	turnCtx := &TurnContext{
		TurnID: "turn-1",
		Prompt: "first message",
		Ctx:    ctx,
	}

	_, execErr := lease.Execute(ctx, turnCtx)
	if execErr == nil {
		t.Fatalf("expected error from Execute on 429 response, got nil")
	}
	if !IsQuotaPause(execErr.Error(), "") && !IsCapacityBlip(execErr.Error(), "") {
		t.Fatalf("expected IsQuotaPause or IsCapacityBlip on Execute error, got: %v", execErr)
	}

	if relErr := lease.Release(); relErr != nil {
		t.Fatalf("Release error: %v", relErr)
	}

	if rotationCalled {
		t.Error("expected OnSessionRotated NOT to be called when turn encountered quota pause")
	}
}

func TestGeminiAPIPool_AcquireLease_ClearsCancelRotationOnSuccessfulRetry(t *testing.T) {
	var callCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := atomic.AddInt32(&callCount, 1)
		if call == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`))
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"Retry succeeded."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		flusher.Flush()
	}))
	defer ts.Close()

	var rotationCalled bool
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCalled = true
			return nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-retry-clear")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	turnCtx1 := &TurnContext{
		TurnID: "turn-1",
		Prompt: "first attempt",
		Ctx:    ctx,
	}

	// Attempt 1 fails with 429 -> sets cancelRotation
	_, execErr1 := lease.Execute(ctx, turnCtx1)
	if execErr1 == nil {
		t.Fatalf("expected error from Execute on attempt 1, got nil")
	}

	// Attempt 2 succeeds on same lease -> clears cancelRotation
	turnCtx2 := &TurnContext{
		TurnID: "turn-1-retry",
		Prompt: "second attempt",
		Ctx:    ctx,
	}
	res2, execErr2 := lease.Execute(ctx, turnCtx2)
	if execErr2 != nil {
		t.Fatalf("attempt 2 Execute error: %v", execErr2)
	}
	if res2 == nil || res2.Response != "Retry succeeded." {
		t.Fatalf("unexpected res2: %v", res2)
	}

	if relErr := lease.Release(); relErr != nil {
		t.Fatalf("Release error: %v", relErr)
	}

	if !rotationCalled {
		t.Error("expected OnSessionRotated to be called when retry on same lease succeeded")
	}
}

func TestGeminiAPIPool_AcquireLease_ConversationalRateLimitTextDoesNotCancelRotation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"I hit an external rate limit reached earlier, but cached the response."}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		flusher.Flush()
	}))
	defer ts.Close()

	var rotationCalled bool
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:          "test-key",
		Model:           "gemini-2.5-flash",
		BaseURL:         ts.URL,
		HTTPClient:      ts.Client(),
		MaxSessionTurns: 1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCalled = true
			return nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-conversational")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	turnCtx := &TurnContext{
		TurnID: "turn-1",
		Prompt: "check status",
		Ctx:    ctx,
	}

	res, execErr := lease.Execute(ctx, turnCtx)
	if execErr != nil {
		t.Fatalf("unexpected Execute error: %v", execErr)
	}
	if res == nil || !strings.Contains(res.Response, "rate limit reached") {
		t.Fatalf("unexpected res: %v", res)
	}

	if relErr := lease.Release(); relErr != nil {
		t.Fatalf("Release error: %v", relErr)
	}

	if !rotationCalled {
		t.Error("expected OnSessionRotated to be called despite conversational text containing rate limit phrasing")
	}
}

func TestGeminiAPIPool_AcquireLease_DefaultMaxSessionTurnsFallback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		chunk := `data: {"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"}}],"finishReason":"STOP","index":0}` + "\n\n"
		_, _ = w.Write([]byte(chunk))
		flusher.Flush()
	}))
	defer ts.Close()

	var rotationCount int
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{
		APIKey:     "test-key",
		Model:      "gemini-2.5-flash",
		BaseURL:    ts.URL,
		HTTPClient: ts.Client(),
		// MaxSessionTurns omitted -> should default to DefaultMaxSessionTurns (8)
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCount++
			return nil
		},
	})
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "target-default-turns")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	// TurnCount starts at 0. Run 7 turns: turnCount becomes 7 (< 8) -> should not rotate
	for i := 0; i < 7; i++ {
		turnCtx := &TurnContext{
			TurnID: fmt.Sprintf("turn-%d", i+1),
			Prompt: "turn",
			Ctx:    ctx,
		}
		if _, execErr := lease.Execute(ctx, turnCtx); execErr != nil {
			t.Fatalf("turn %d Execute error: %v", i+1, execErr)
		}
	}

	// Set turnCount to 8 on 8th turn:
	turnCtx8 := &TurnContext{
		TurnID: "turn-8",
		Prompt: "turn 8",
		Ctx:    ctx,
	}
	if _, execErr := lease.Execute(ctx, turnCtx8); execErr != nil {
		t.Fatalf("turn 8 Execute error: %v", execErr)
	}

	if relErr := lease.Release(); relErr != nil {
		t.Fatalf("Release error: %v", relErr)
	}

	if rotationCount != 1 {
		t.Errorf("expected 1 rotation at DefaultMaxSessionTurns (8), got %d", rotationCount)
	}
}

func TestGeminiAPIPool_AcquireLease_ConcurrencySerialization(t *testing.T) {
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

	// lease2 should time out while lease1 is held
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err2 := pool.AcquireLease(ctxTimeout, "target-conc")
	if err2 == nil {
		t.Fatalf("expected lease2 AcquireLease to time out while lease1 is held")
	}

	// Release lease1
	if err := lease1.Release(); err != nil {
		t.Fatalf("lease1 release error: %v", err)
	}

	// lease3 should now succeed
	lease3, err3 := pool.AcquireLease(ctx, "target-conc")
	if err3 != nil {
		t.Fatalf("lease3 AcquireLease error after lease1 release: %v", err3)
	}
	_ = lease3.Release()
}

func TestGeminiAPIPool_AcquireLease_Validation(t *testing.T) {
	ctx := context.Background()

	// Nil receiver
	var nilPool *GeminiAPIPool
	if _, err := nilPool.AcquireLease(ctx, "target"); err == nil {
		t.Error("expected error from nil pool AcquireLease")
	}

	// Empty targetKey
	pool := NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "test"})
	defer pool.Close()

	if _, err := pool.AcquireLease(ctx, "   "); err == nil {
		t.Error("expected error from empty targetKey AcquireLease")
	}
}
