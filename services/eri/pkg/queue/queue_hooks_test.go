package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestProcessBurst_RecordsTokensTelemetry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		model        string
		threadID     string
		channelName  string
		mockOutput   string
		wantChannel  string
		wantDeltas   map[string]float64
		wantExcludes string
	}{
		{name: "NamedChannel", model: "test-token-model", threadID: "chan-token-test-1", mockOutput: `{"conversation_id":"c1111111-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":150,"output_tokens":75,"thinking_tokens":30,"cache_read_tokens":10,"total_tokens":265}}`, wantChannel: "chan-token-test-1", wantDeltas: map[string]float64{"input": 150, "output": 75, "thinking": 30, "cache_read": 10, "total": 265}},
		{name: "SnowflakeChannelSanitized", model: "test-snowflake-model", threadID: "1534436119888793750", mockOutput: `{"conversation_id":"c2222222-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":100,"output_tokens":50,"thinking_tokens":20,"cache_read_tokens":5,"total_tokens":175}}`, wantChannel: "unknown", wantDeltas: map[string]float64{"input": 100, "output": 50, "thinking": 20, "cache_read": 5, "total": 175}, wantExcludes: "1534436119888793750"},
		{name: "BurstChannelNameFallback", model: "test-burst-chan-model", threadID: "low_effort:1534436119888793750", channelName: "#aerial-dev", mockOutput: `{"conversation_id":"c3333333-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":80,"output_tokens":20,"total_tokens":100}}`, wantChannel: "aerial-dev", wantDeltas: map[string]float64{"input": 80, "output": 20, "thinking": 0, "cache_read": 0, "total": 100}, wantExcludes: "low_effort:1534436119888793750"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			pool := New(nil, WorkerPoolConfig{
				Store: store,
				Model: tc.model,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.mockOutput, "", 0, nil
				},
			})
			defer pool.Stop()

			// TokensTotal is process-global and accumulates across -count runs,
			// so assert only the delta this row's burst produced.
			baseline := make(map[string]float64, len(tc.wantDeltas))
			for tokenType := range tc.wantDeltas {
				baseline[tokenType] = testutil.ToFloat64(metrics.TokensTotal.WithLabelValues(tokenType, tc.model, tc.wantChannel))
			}

			t0 := time.Now().UTC()
			msg := db.Message{
				ID:          "msg-token-" + tc.name,
				ThreadID:    tc.threadID,
				ChannelName: tc.channelName,
				Content:     "<@aerial> Question Token Test",
				Status:      db.StatusPending,
				CreatedAt:   t0,
				UpdatedAt:   t0,
			}
			require.NoError(t, insertMessage(store, msg))
			if lt := runLiveBurst(context.Background(), pool, tc.threadID, "turn-1", msg.ID); lt.err != nil || lt.outcome != OutcomeCompleted {
				t.Fatalf("turn = (%s, %v), want completed", lt.outcome, lt.err)
			}

			for tokenType, want := range tc.wantDeltas {
				got := testutil.ToFloat64(metrics.TokensTotal.WithLabelValues(tokenType, tc.model, tc.wantChannel)) - baseline[tokenType]
				if got != want {
					t.Errorf("tokens_total{type=%q,model=%q,channel=%q} delta = %v, want %v", tokenType, tc.model, tc.wantChannel, got, want)
				}
			}

			rec := httptest.NewRecorder()
			metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
			if body := rec.Body.String(); tc.wantExcludes != "" && strings.Contains(body, tc.wantExcludes) {
				t.Errorf("metrics body should not contain %q:\n%s", tc.wantExcludes, body)
			}
		})
	}
}

func TestThreadStatusQueueIntegration(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	threadID := "thread-status-test-123"
	var finalDelivered atomic.Int32

	sess, err := discordgo.New("Bot mock_token")
	require.NoError(t, err)
	sess.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		msgJSON := `{"id":"mock-status-msg-123","channel_id":"thread-status-test-123","content":"mock status"}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(msgJSON)),
		}, nil
	})
	sess.State = discordgo.NewState()
	require.NoError(t, sess.State.GuildAdd(&discordgo.Guild{ID: "guild-1"}))
	require.NoError(t, sess.State.ChannelAdd(&discordgo.Channel{
		ID: threadID, GuildID: "guild-1", Name: "status-thread", Type: discordgo.ChannelTypeGuildPublicThread, ParentID: "parent-chan-1",
	}))

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			finalDelivered.Add(1)
			return nil
		},
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if opts.StepUpdateHandler != nil {
				opts.StepUpdateHandler(&runner.StepUpdateEvent{StepType: "tool", ToolName: "view_file", State: "ACTIVE"})
			}
			return `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"Hello from thread!"}`, "", 0, nil
		},
	})
	pool.SetDiscordSession(sess)

	msg := db.Message{ID: "msg-thread-1", ThreadID: threadID, GuildID: "guild-1", AuthorID: "user-1", Content: "check my file", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	require.NoError(t, insertMessage(store, msg))
	if lt := runLiveBurst(context.Background(), pool, threadID, "turn-1", msg.ID); lt.err != nil || lt.outcome != OutcomeCompleted {
		t.Fatalf("turn = (%s, %v), want completed", lt.outcome, lt.err)
	}

	if finalDelivered.Load() != 1 {
		t.Errorf("Expected 1 final message delivered, got %d", finalDelivered.Load())
	}
}

func TestWorkerPool_HermeticByDefaultWithoutRetriever(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	var runnerCalled bool
	poolWithDB := NewWorkerPool(WorkerPoolConfig{
		Store: store,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			runnerCalled = true
			return mockJSONResponse("sess-test-1", "Test Response"), "", 0, nil
		},
		DeliveryFunc: noopDelivery,
		TypingFunc:   noopTyping,
	})
	msg := db.Message{ID: "msg-hermetic-1", ThreadID: "thread-hermetic-1", AuthorID: "user-1", Content: "Hermetic test without retriever", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	require.NoError(t, insertMessage(store, msg))
	if lt := runLiveBurst(context.Background(), poolWithDB, msg.ThreadID, "turn-1", msg.ID); lt.err != nil || lt.outcome != OutcomeCompleted {
		t.Fatalf("turn = (%s, %v), want completed", lt.outcome, lt.err)
	}
	if !runnerCalled {
		t.Errorf("Expected runner to be called cleanly without error")
	}
}

func TestIsRateLimitError_ExtendedKeywords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		errDetail, extra string
		want             bool
	}{
		{errDetail: "Error 503", want: true},
		{errDetail: "rate limit exceeded", want: true},
		{errDetail: "RESOURCE_EXHAUSTED", want: true},
		{errDetail: "HTTP 429", want: true},
		{errDetail: "high demand, please wait", want: true},
		{errDetail: "Daily quota exceeded", want: true},
		{errDetail: "Too Many Requests", want: true},
		{errDetail: "model is overloaded", want: true},
		{errDetail: "503 Service Unavailable", want: true},
		{errDetail: "resource has been exhausted by caller", want: true},
		{errDetail: "Individual quota reached for user", want: true},
		{errDetail: "Please upgrade your subscription to continue", want: true},
		{errDetail: "generic error", extra: "upstream returned 429 Too Many Requests", want: true},
		{errDetail: "write: disk quota exceeded", want: false},
		{errDetail: "syntax error on line 42", want: false},
		{errDetail: "", want: false},
	}
	for _, tt := range tests {
		if got := isRateLimitError(tt.errDetail, tt.extra); got != tt.want {
			t.Errorf("isRateLimitError(%q, %q) = %t, want %t", tt.errDetail, tt.extra, got, tt.want)
		}
	}
}

func TestWorkerPool_NotifierInvocationMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, threadID, seedSessionID, runnerStderr, notifierReturn, expectDeliveredNotice, expectedDescContains string
		maxAttempts, runnerExitCode, wantMaxDescLen                                                              int
		runnerErr, deliveryErr                                                                                   error
		expectNotifierCalled, expectUnavailableMsg                                                               bool
		wantOutcome                                                                                              TurnOutcome
	}{
		{name: "WatchdogExhaustion_InvokesNotifierFunc", threadID: "thread-wd-1", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded [watchdog]", runnerErr: errors.New("watchdog timeout"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "execution timed out", notifierReturn: "DEADASS TIMED OUT 💀", expectDeliveredNotice: "DEADASS TIMED OUT 💀", wantOutcome: OutcomeTimeout},
		{name: "NonTransient_InvokesNotifierFunc", threadID: "thread-nt-1", maxAttempts: 3, runnerStderr: "Error: unknown flag: --bogus-flag", runnerErr: errors.New("exit status 1"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "non-transient error", notifierReturn: "UNACCEPTABLE GARBAGE DETECTED 🔥", expectDeliveredNotice: "UNACCEPTABLE GARBAGE DETECTED 🔥", wantOutcome: OutcomeFailed},
		{name: "RetryExhaustion_InvokesNotifierFunc", threadID: "thread-exhaust-1", maxAttempts: 2, runnerStderr: "connection reset by peer", runnerErr: errors.New("network drop"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "exhausting 2 attempts", notifierReturn: "ROASTED TO ASHES 🤘", expectDeliveredNotice: "ROASTED TO ASHES 🤘", wantOutcome: OutcomeFailed},
		{name: "RateLimitBypassesNotifierFunc", threadID: "thread-rl-1", maxAttempts: 1, runnerStderr: "Error 429: Too Many Requests", runnerErr: errors.New("rate limit"), runnerExitCode: 1, expectUnavailableMsg: true, wantOutcome: OutcomeFailed},
		{name: "Watchdog_RateLimitBypassesNotifier", threadID: "thread-wd-rl", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded: [watchdog] Resource has been exhausted (quota limit reached)", runnerErr: context.DeadlineExceeded, runnerExitCode: 124, expectUnavailableMsg: true, wantOutcome: OutcomeTimeout},
		{name: "SessionCorruption_RateLimitBypassesNotifier", threadID: "thread-sc-rl", maxAttempts: 1, seedSessionID: "sess-existing-1", runnerStderr: "conversation not found: 429 rate limit exceeded", runnerErr: errors.New("rate limited corruption"), runnerExitCode: 1, expectUnavailableMsg: true, wantOutcome: OutcomeFailed},
		{name: "NonTransient_LongSnippetAndDeliveryErr", threadID: "thread-nt-long", maxAttempts: 1, runnerStderr: strings.Repeat("VeryBadError-", 50), runnerErr: errors.New("fatal command syntax"), runnerExitCode: 1, deliveryErr: errors.New("discord delivery network failed"), expectNotifierCalled: true, wantMaxDescLen: 350, notifierReturn: "DEADASS BROKEN 💀", wantOutcome: OutcomeFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			if tt.seedSessionID != "" {
				require.NoError(t, rotateSessionID(store, tt.threadID, tt.seedSessionID))
			}
			var capturedDesc, deliveredNotif string
			var mu sync.Mutex
			notifierCalled := false

			pool := NewWorkerPool(WorkerPoolConfig{
				Store: store, MaxAttempts: tt.maxAttempts, BackoffBase: 5 * time.Millisecond,
				RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
					return "", tt.runnerStderr, tt.runnerExitCode, tt.runnerErr
				},
				NotifierFunc: func(agyBin, apiKey, desc string) string {
					mu.Lock()
					notifierCalled, capturedDesc = true, desc
					mu.Unlock()
					return tt.notifierReturn
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					if tt.deliveryErr != nil {
						return tt.deliveryErr
					}
					mu.Lock()
					deliveredNotif = text
					mu.Unlock()
					return nil
				},
			})
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tt.name, ThreadID: tt.threadID, Content: "test content", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
			require.NoError(t, insertMessage(store, msg))
			mustRunLiveBurst(t, pool, tt.threadID, msg.ID, tt.wantOutcome)

			mu.Lock()
			defer mu.Unlock()
			if notifierCalled != tt.expectNotifierCalled {
				t.Errorf("notifierCalled=%t, want %t", notifierCalled, tt.expectNotifierCalled)
			}
			if tt.expectedDescContains != "" && !strings.Contains(capturedDesc, tt.expectedDescContains) {
				t.Errorf("desc missing %q", tt.expectedDescContains)
			}
			if tt.wantMaxDescLen > 0 && len([]rune(capturedDesc)) > tt.wantMaxDescLen {
				t.Errorf("desc len %d > %d", len([]rune(capturedDesc)), tt.wantMaxDescLen)
			}
			if tt.expectDeliveredNotice != "" && deliveredNotif != tt.expectDeliveredNotice {
				t.Errorf("delivered notice=%q, want %q", deliveredNotif, tt.expectDeliveredNotice)
			}
			if tt.expectUnavailableMsg && deliveredNotif != notifier.ModelUnavailableMessage() {
				t.Errorf("delivered=%q, want %q", deliveredNotif, notifier.ModelUnavailableMessage())
			}
		})
	}
}

type mockWebhookDispatcher struct {
	callWakeHookFunc     func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error)
	callPreTurnHookFunc  func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error)
	callPostTurnHookFunc func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error)
}

func (m *mockWebhookDispatcher) CallWakeHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
	if m.callWakeHookFunc != nil {
		return m.callWakeHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

func (m *mockWebhookDispatcher) CallPreTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
	if m.callPreTurnHookFunc != nil {
		return m.callPreTurnHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

func (m *mockWebhookDispatcher) CallPostTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
	if m.callPostTurnHookFunc != nil {
		return m.callPostTurnHookFunc(ctx, endpoint, req)
	}
	return nil, nil
}

// wakeHookEval captures what one evaluateAmbientWake run observed.
type wakeHookEval struct {
	shouldExit, hookCalled, classifierCalled bool
	te                                       *turnExecution
	store                                    db.Store
	capturedReq                              *WakeRequest
}

func runWakeHookEval(t *testing.T, hookResp *WakeResponse, hookErr error, timeoutAction, classifierResp string, inputMsgs []db.Message, emptyEffID bool, schedRunID string) wakeHookEval {
	t.Helper()
	store := testStore(t)
	now := time.Now().UTC()
	var classifierCalled, hookCalled bool
	var cls *classifier.Classifier
	if classifierResp != "" {
		cls = classifier.New(nil)
		cls.Model = "test-flash"
		cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
			classifierCalled = true
			return classifierResp, nil
		}
	}
	var capturedReq *WakeRequest
	pool := NewWorkerPool(WorkerPoolConfig{Store: store, Classifier: cls, WebhookDispatcher: &mockWebhookDispatcher{callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
		hookCalled = true
		capturedReq = req
		return hookResp, hookErr
	}}})
	msgs := make([]db.Message, len(inputMsgs))
	for i, m := range inputMsgs {
		m.CreatedAt = now.Add(time.Duration(i) * time.Second)
		msgs[i] = m
		require.NoError(t, insertMessage(store, m))
	}
	effID := msgs[0].ThreadID
	if emptyEffID {
		effID = ""
	}
	te := &turnExecution{
		pool: pool, burst: msgs, threadID: msgs[0].ThreadID, effectiveID: effID, triggerType: "message", execStart: now,
		policy: config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: new(0.80), Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake", TimeoutMs: 1000, OnTimeoutAction: timeoutAction}}},
	}
	shouldExit := te.evaluateAmbientWake()
	return wakeHookEval{shouldExit: shouldExit, hookCalled: hookCalled, classifierCalled: classifierCalled, te: te, store: store, capturedReq: capturedReq}
}

type onWakeHookCase struct {
	name, timeoutAction, classifierResp, schedRunID string
	msgs                                            []db.Message
	hookResp                                        *WakeResponse
	hookErr                                         error
	isSched, wantExit, wantClassifier               bool
	wantTurns                                       int
}

func assertWakeHookExit(t *testing.T, tc onWakeHookCase, te *turnExecution, store db.Store) {
	t.Helper()
	if te.wakeIdx != -1 {
		t.Errorf("wakeIdx=%d, want -1", te.wakeIdx)
	}
	saved, err := getMessage(store, tc.msgs[0].ID)
	require.NoError(t, err)
	if saved == nil || saved.Status != db.StatusCompleted {
		t.Fatalf("saved msg = %+v", saved)
	}
}

func assertWakeHookNoExit(t *testing.T, tc onWakeHookCase, te *turnExecution, store db.Store) {
	t.Helper()
	if te.wakeIdx != 0 || len(te.wakeInfos) == 0 || !te.wakeInfos[0].IsWake {
		t.Errorf("wakeIdx=%d wakeInfos=%+v", te.wakeIdx, te.wakeInfos)
	}
	if tc.wantTurns > 0 {
		turnCount, err := getSessionTurnCount(store, tc.msgs[0].ThreadID)
		require.NoError(t, err)
		if turnCount != tc.wantTurns {
			t.Errorf("turnCount = %d, want %d", turnCount, tc.wantTurns)
		}
	}
}

func assertWakeHookExecution(t *testing.T, tc onWakeHookCase, te *turnExecution, store db.Store) {
	t.Helper()
	if tc.wantExit {
		assertWakeHookExit(t, tc, te, store)
		return
	}
	assertWakeHookNoExit(t, tc, te, store)
}

func runOnWakeHookCase(t *testing.T, tc onWakeHookCase) {
	t.Helper()
	got := runWakeHookEval(t, tc.hookResp, tc.hookErr, tc.timeoutAction, tc.classifierResp, tc.msgs, tc.isSched, tc.schedRunID)
	if got.shouldExit != tc.wantExit || !got.hookCalled || got.classifierCalled != tc.wantClassifier {
		t.Fatalf("%s: got exit=%v hook=%v classifier=%v (want exit=%v classifier=%v)", tc.name, got.shouldExit, got.hookCalled, got.classifierCalled, tc.wantExit, tc.wantClassifier)
	}
	assertWakeHookExecution(t, tc, got.te, got.store)
	if got.capturedReq == nil || got.capturedReq.ChannelID == "" || got.capturedReq.Timestamp.IsZero() {
		t.Errorf("capturedReq invalid: %+v", got.capturedReq)
	}
}

func TestOnWakeHook_Lifecycle(t *testing.T) {
	t.Parallel()
	tests := []onWakeHookCase{
		{name: "WakeOverride", msgs: []db.Message{{ID: "m-w1", ThreadID: "ch-gen", Content: "hi", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "wake", Reason: "hook_priority_wake"}, classifierResp: `{"confidence": 0.10}`, wantTurns: 1},
		{name: "DropOverride", msgs: []db.Message{{ID: "m-d1", ThreadID: "ch-gen", Content: "ambient", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "drop", Reason: "hook_drop_override"}, classifierResp: `{"confidence": 0.95}`, wantExit: true},
		{name: "TimeoutFallback_Classify", msgs: []db.Message{{ID: "m-c1", ThreadID: "ch-gen", Content: "hello world", Status: db.StatusProcessing}}, hookErr: context.DeadlineExceeded, timeoutAction: "classify", classifierResp: `{"confidence": 0.92, "reason": "classified wake"}`, wantClassifier: true},
		{name: "TimeoutFallback_Wake", msgs: []db.Message{{ID: "m-tw1", ThreadID: "ch-gen", Content: "err-wake", Status: db.StatusProcessing}}, hookErr: errors.New("conn refused"), timeoutAction: "wake", classifierResp: `{"confidence": 0.10}`},
		{name: "TimeoutFallback_Drop", msgs: []db.Message{{ID: "m-td1", ThreadID: "ch-gen", Content: "err-drop", Status: db.StatusProcessing}}, hookErr: errors.New("timeout"), timeoutAction: "drop", classifierResp: `{"confidence": 0.95}`, wantExit: true},
		{name: "DropWithScheduleRun", msgs: []db.Message{{ID: "m-s1", ThreadID: "ch-sched", Content: "sched noise", Status: db.StatusProcessing}}, hookResp: &WakeResponse{Override: "drop"}, isSched: true, schedRunID: "sched-run-amb", wantExit: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runOnWakeHookCase(t, tc)
		})
	}
}

func TestOnWakeHook_FullProcessBurst(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	now := time.Now().UTC()
	var mu sync.Mutex
	var runnerCalls, deliveryCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""),
		WebhookDispatcher: &mockWebhookDispatcher{callWakeHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
			if req.Message.Content == "wake message" {
				return &WakeResponse{Override: "wake"}, nil
			}
			return &WakeResponse{Override: "drop"}, nil
		}},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			mu.Unlock()
			return mockJSONResponse(sessionID, "Bot response"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveryCalls++
			mu.Unlock()
			return nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "classifier", AmbientWakeThreshold: new(0.80), Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake"}}}
		},
	})

	msgDrop, msgWake := db.Message{ID: "msg-burst-drop", ThreadID: "chan-full-test", AuthorName: "user-foxtrot", Content: "drop message", Status: db.StatusPending, CreatedAt: now}, db.Message{ID: "msg-burst-wake", ThreadID: "chan-full-test", AuthorName: "user-golf", Content: "wake message", Status: db.StatusPending, CreatedAt: now.Add(time.Second)}
	require.NoError(t, insertMessage(store, msgDrop))
	if lt := runLiveBurst(context.Background(), pool, msgDrop.ThreadID, "turn-drop", msgDrop.ID); lt.err != nil {
		t.Fatalf("drop turn err = %v", lt.err)
	}
	savedDrop, err := getMessage(store, "msg-burst-drop")
	require.NoError(t, err)
	if savedDrop == nil || savedDrop.Status != db.StatusCompleted || !strings.Contains(savedDrop.ErrorMessage, "[AMBIENT") {
		t.Errorf("expected drop message completed as ambient, got %+v", savedDrop)
	}
	require.NoError(t, insertMessage(store, msgWake))
	if lt := runLiveBurst(context.Background(), pool, msgWake.ThreadID, "turn-wake", msgWake.ID); lt.err != nil || lt.outcome != OutcomeCompleted {
		t.Fatalf("wake turn = (%s, %v), want completed", lt.outcome, lt.err)
	}
	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 1 || deliveryCalls != 1 {
		t.Errorf("runnerCalls=%d, deliveryCalls=%d, want 1, 1", runnerCalls, deliveryCalls)
	}
	savedWake, err := getMessage(store, "msg-burst-wake")
	require.NoError(t, err)
	if savedWake == nil || savedWake.Status != db.StatusCompleted || savedWake.ResponseText != "Bot response" {
		t.Errorf("expected wake message completed with response, got %+v", savedWake)
	}
}

func TestOnWakeHook_AccessorsAndFallbacks(t *testing.T) {
	t.Parallel()
	var nilPool *WorkerPool
	if nilPool.WebhookDispatcher() != nil {
		t.Errorf("expected nil for nilPool.WebhookDispatcher")
	}
	nilPool.SetWebhookDispatcher(nil)

	poolDef := NewWorkerPool(WorkerPoolConfig{})
	if poolDef.WebhookDispatcher() == nil {
		t.Errorf("expected non-nil default dispatcher")
	}
	mockDisp := &mockWebhookDispatcher{}
	poolDef.SetWebhookDispatcher(mockDisp)
	if poolDef.WebhookDispatcher() != mockDisp {
		t.Errorf("expected mockDisp after SetWebhookDispatcher")
	}
	poolDef.SetWebhookDispatcher(nil)
	if poolDef.WebhookDispatcher() == nil {
		t.Errorf("expected non-nil default dispatcher after SetWebhookDispatcher(nil)")
	}

	teNil := &turnExecution{pool: nil, burst: []db.Message{{ID: "m-nil"}}, policy: config.ChannelPolicy{Mode: "channel", Hooks: config.ChannelHooksConfig{OnWake: &config.WebhookEndpoint{URL: "http://sidecar.local/wake"}}}}
	teNil.markAmbientBurst()
}

// runPreTurnHookBurst drives one burst turn through the TurnRunner steps. With rekick set, a turn
// that prep rescheduled is driven once more, as the coordinator does after the delay.
func runPreTurnHookBurst(t *testing.T, hookResps []*PreTurnResponse, hookErr error, timeoutAction string, initialRetry int, schedRunID string, rekick bool) (int, int, string, *PreTurnRequest, *db.Message) {
	t.Helper()
	store := testStore(t)
	var mu sync.Mutex
	var runnerCalls, hookCalls int
	var capturedPrompt string
	var capturedReq *PreTurnRequest

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""), MaxAttempts: 3,
		RetryDelayOverride: 10 * time.Millisecond, DeliveryFunc: noopDelivery,
		WebhookDispatcher: &mockWebhookDispatcher{callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
			mu.Lock()
			hookCalls++
			cnt := hookCalls
			capturedReq = req
			mu.Unlock()
			if hookErr != nil {
				return nil, hookErr
			}
			idx := min(cnt-1, len(hookResps)-1)
			return hookResps[idx], nil
		}},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			mu.Lock()
			runnerCalls++
			capturedPrompt = prompt
			mu.Unlock()
			return mockJSONResponse(sessionID, "Turn proceeded"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn", TimeoutMs: 1500, OnTimeoutAction: timeoutAction},
			}}
		},
	})
	defer pool.Stop()
	now := time.Now().UTC()
	msg := db.Message{ID: "msg-preturn-test", ThreadID: "chan-preturn", AuthorName: "user-alpha", Content: "test prompt for pre-turn", Status: db.StatusPending, RetryCount: initialRetry, CreatedAt: now}
	require.NoError(t, insertMessage(store, msg))
	lt := runLiveBurst(context.Background(), pool, msg.ThreadID, "turn-1", msg.ID)
	if lt.err != nil {
		t.Fatalf("turn err = %v", lt.err)
	}
	if rekick && lt.outcome == OutcomeReschedule {
		if lt = runLiveBurst(context.Background(), pool, msg.ThreadID, "turn-2", msg.ID); lt.err != nil {
			t.Fatalf("rekicked turn err = %v", lt.err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	saved, err := getMessage(store, "msg-preturn-test")
	require.NoError(t, err)
	return hookCalls, runnerCalls, capturedPrompt, capturedReq, saved
}

type preTurnHookCase struct {
	name, timeoutAction, schedRunID, wantStatus, wantErrSub, wantPromptSub string
	hookResps                                                              []*PreTurnResponse
	hookErr                                                                error
	initialRetry, wantHookCalls, wantRunner                                int
	rekick                                                                 bool
}

var preTurnHookCases = []preTurnHookCase{
	{name: "Rejected_DropAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "drop", Reason: "resource busy"}}, wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "resource busy"},
	{name: "TimeoutFallback_Drop", hookErr: context.DeadlineExceeded, timeoutAction: "drop", wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "pre_turn_timeout_drop"},
	{name: "TimeoutFallback_Proceed", hookErr: errors.New("webhook failed"), timeoutAction: "proceed", wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted},
	{name: "TimeoutFallback_Retry", hookErr: errors.New("gateway timeout"), timeoutAction: "retry", wantHookCalls: 1, wantStatus: db.StatusPending},
	{name: "ApprovedWithInjectedContext", hookResps: []*PreTurnResponse{{Allow: true, InjectedContext: "system instructions: be concise", Metadata: map[string]any{"lease_token": "token-123"}}}, wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted, wantPromptSub: "<COORDINATION_CONTEXT>\nsystem instructions: be concise\n</COORDINATION_CONTEXT>"},
	{name: "RejectedRetryAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1, Reason: "sidecar lock wait"}, {Allow: true}}, rekick: true, wantHookCalls: 2, wantRunner: 1, wantStatus: db.StatusCompleted},
	{name: "RetryExhaustionWithScheduleRun", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1}}, initialRetry: 2, schedRunID: "run-exhaust-1", wantHookCalls: 1, wantRunner: 0, wantStatus: db.StatusFailed, wantErrSub: "[EXHAUSTED_PRE_TURN_RETRIES]"},
}

func assertPreTurnHookCase(t *testing.T, tc preTurnHookCase, hc, rc int, cp string, cr *PreTurnRequest, saved *db.Message) {
	t.Helper()
	if hc != tc.wantHookCalls || rc != tc.wantRunner {
		t.Fatalf("%s: hookCalls=%d runnerCalls=%d", tc.name, hc, rc)
	}
	if saved == nil || saved.Status != tc.wantStatus {
		t.Fatalf("%s: saved status=%+v, want %s", tc.name, saved, tc.wantStatus)
	}
	if tc.wantErrSub != "" && !strings.Contains(saved.ErrorMessage, tc.wantErrSub) {
		t.Errorf("%s: ErrorMessage=%q, want %q", tc.name, saved.ErrorMessage, tc.wantErrSub)
	}
	if tc.wantPromptSub != "" && !strings.Contains(cp, tc.wantPromptSub) {
		t.Errorf("%s: prompt missing %q", tc.name, tc.wantPromptSub)
	}
	if cr == nil || cr.Timestamp.IsZero() {
		t.Errorf("%s: invalid captured request: %+v", tc.name, cr)
	}
}

func TestPreTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	for _, tc := range preTurnHookCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hc, rc, cp, cr, saved := runPreTurnHookBurst(t, tc.hookResps, tc.hookErr, tc.timeoutAction, tc.initialRetry, tc.schedRunID, tc.rekick)
			assertPreTurnHookCase(t, tc, hc, rc, cp, cr, saved)
		})
	}
}

func TestPreTurnHook_Helpers(t *testing.T) {
	t.Parallel()
	if reqNil := buildPreTurnRequest(nil); reqNil == nil || reqNil.Timestamp.IsZero() {
		t.Errorf("expected valid from nil te")
	}
	msgA, msgB := db.Message{ID: "mA", RetryCount: 1, Content: "promptA"}, db.Message{ID: "mB", RetryCount: 3, Content: "promptB"}
	if req := buildPreTurnRequest(&turnExecution{threadID: "t-fallback", burst: []db.Message{msgA, msgB}}); req.ChannelID != "t-fallback" || req.RetryCount != 3 {
		t.Errorf("req = %+v", req)
	}
	teNoPool := &turnExecution{pool: nil, burst: []db.Message{msgA}}
	teNoPool.abortBurst("test drop nil pool")

	store := testStore(t)
	completedStatus := ""
	pool := NewWorkerPool(WorkerPoolConfig{Store: store, OnMessageCompleted: func(msg db.Message, status string) { completedStatus = status }})
	schedMsg := db.Message{ID: "msg-sched-abort", ThreadID: "chan-sched", Status: db.StatusPending}
	require.NoError(t, insertMessage(store, schedMsg))
	(&turnExecution{pool: pool, burst: []db.Message{schedMsg}, threadID: "chan-sched"}).abortBurst("manual abort")
	if completedStatus != db.StatusCompleted {
		t.Errorf("completedStatus=%s, want COMPLETED", completedStatus)
	}
}

func runPostTurnHookBurst(t *testing.T, preResp *PreTurnResponse, runnerStderr string, runnerErr error, runnerPanic string) (*PostTurnRequest, string) {
	t.Helper()
	store := testStore(t)
	var mu sync.Mutex
	var postCalled bool
	var postCallCount int
	var postReqReceived *PostTurnRequest
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				if preResp != nil {
					return preResp, nil
				}
				return &PreTurnResponse{Allow: true}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				mu.Lock()
				postCalled = true
				postCallCount++
				reqCopy := *req
				postReqReceived = &reqCopy
				mu.Unlock()
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			if runnerPanic != "" {
				panic(runnerPanic)
			}
			if runnerErr != nil {
				return "", runnerStderr, 1, runnerErr
			}
			// The post_turn usage comes from the recorded row with the longest duration_ms, so the
			// mock turn spans at least one millisecond of wall clock.
			for start := time.Now(); time.Since(start) < time.Millisecond; {
				runtime.Gosched()
			}
			return mockJSONResponseWithUsage(sessionID, "Clean success response from runner", expectedUsage), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	defer pool.Stop()
	msg := db.Message{ID: "msg-postturn-test", ThreadID: "chan-postturn-test", AuthorName: "user-alpha", Content: "run postturn test", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	if err := insertMessage(store, msg); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if lt := runLiveBurst(context.Background(), pool, msg.ThreadID, "turn-1", msg.ID); lt.err != nil {
		t.Fatalf("turn err = %v", lt.err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !postCalled || postReqReceived == nil {
		t.Fatalf("expected post_turn hook to be called")
	}
	if postCallCount != 1 {
		t.Fatalf("expected post_turn hook to be called exactly once, got %d", postCallCount)
	}
	var msgStatus string
	if dbMsg, err := store.GetMessage(context.Background(), msg.ID); err == nil && dbMsg != nil {
		msgStatus = dbMsg.Status
	}
	return postReqReceived, msgStatus
}

func TestPostTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}
	tests := []struct {
		name, runnerStderr, wantStatus, wantErrSub, runnerPanic string
		preResp                                                 *PreTurnResponse
		runnerErr                                               error
		checkMeta                                               bool
		wantMetaKey                                             string
		wantMetaVal                                             string
		wantMsgStatus                                           string
	}{
		{
			name:          "SuccessfulTurnWithMetadata",
			preResp:       &PreTurnResponse{Allow: true, Metadata: map[string]any{"lease_id": "lease-abc", "turn_epoch": float64(42)}},
			wantStatus:    "success",
			checkMeta:     true,
			wantMsgStatus: db.StatusCompleted,
		},
		{
			name:          "FailedTurn_AgyError",
			runnerStderr:  "non-transient runner failed: syntax error in script",
			runnerErr:     fmt.Errorf("exit status 1"),
			wantStatus:    "failed",
			wantErrSub:    "syntax error",
			wantMsgStatus: db.StatusFailed,
		},
		{
			name:          "RunnerPanicRecovery",
			runnerPanic:   "simulated runner crash inside runner function",
			preResp:       &PreTurnResponse{Allow: true, Metadata: map[string]any{"lease_id": "lease-panic"}},
			wantStatus:    "failed",
			wantErrSub:    "simulated runner crash",
			wantMetaKey:   "lease_id",
			wantMetaVal:   "lease-panic",
			wantMsgStatus: db.StatusFailed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, msgStatus := runPostTurnHookBurst(t, tc.preResp, tc.runnerStderr, tc.runnerErr, tc.runnerPanic)
			if req.ChannelID != "chan-postturn-test" || req.ThreadID != "chan-postturn-test" || req.DurationMs < 0 {
				t.Errorf("postReq metadata: %+v", req)
			}
			if req.Status != tc.wantStatus {
				t.Errorf("Status=%q, want %q", req.Status, tc.wantStatus)
			}
			if tc.checkMeta && (req.ResponseText != "Clean success response from runner" || req.TokenUsage != expectedUsage || req.Metadata["lease_id"] != "lease-abc") {
				t.Errorf("payload mismatch: resp=%q usage=%+v meta=%+v", req.ResponseText, req.TokenUsage, req.Metadata)
			}
			if tc.wantErrSub != "" && !strings.Contains(req.Error, tc.wantErrSub) {
				t.Errorf("Error=%q, want %q", req.Error, tc.wantErrSub)
			}
			if tc.wantMetaKey != "" && req.Metadata[tc.wantMetaKey] != tc.wantMetaVal {
				t.Errorf("Metadata[%q]=%v, want %v", tc.wantMetaKey, req.Metadata[tc.wantMetaKey], tc.wantMetaVal)
			}
			if tc.wantMsgStatus != "" && msgStatus != tc.wantMsgStatus {
				t.Errorf("Message status=%q, want %q", msgStatus, tc.wantMsgStatus)
			}
		})
	}
}

func TestPostTurnHook_Helpers(t *testing.T) {
	t.Parallel()
	if reqNil := buildPostTurnRequest(nil); reqNil == nil || reqNil.Timestamp.IsZero() {
		t.Errorf("expected valid from nil te")
	}
	reqFallback := buildPostTurnRequest(&turnExecution{
		threadID: "t-fallback", turnStatus: "success", turnResponseText: "fallback response",
		execStart: time.Now().UTC().Add(-50 * time.Millisecond), hookMetadata: map[string]any{"key": "val"},
	})
	if reqFallback.ChannelID != "t-fallback" || reqFallback.DurationMs <= 0 || reqFallback.Metadata["key"] != "val" {
		t.Errorf("reqFallback = %+v", reqFallback)
	}
	if reqEmpty := buildPostTurnRequest(&turnExecution{threadID: "t-empty"}); reqEmpty.Status != "failed" {
		t.Errorf("default status failed, got %s", reqEmpty.Status)
	}

	store := testStore(t)
	var postHookFired bool
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				return &PreTurnResponse{Allow: false, Action: "drop", Reason: "denied"}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				postHookFired = true
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msgDrop := db.Message{ID: "msg-preturn-dropped", ThreadID: "chan-preturn-dropped", AuthorName: "user-drop", Content: "drop me", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	require.NoError(t, insertMessage(store, msgDrop))
	if lt := runLiveBurst(context.Background(), pool, msgDrop.ThreadID, "turn-1", msgDrop.ID); lt.err != nil {
		t.Fatalf("turn err = %v", lt.err)
	}
	if postHookFired {
		t.Errorf("post_turn hook must NOT fire when pre_turn dropped turn")
	}

	var postHookCompletedFired bool
	poolCompleted := NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				postHookCompletedFired = true
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msgCompleted := db.Message{ID: "msg-already-completed", ThreadID: "chan-completed", Status: db.StatusCompleted, ResponseText: "done", CreatedAt: time.Now().UTC()}
	if err := insertMessage(store, msgCompleted); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if err := poolCompleted.FailBurstTurn(context.Background(), "turn-completed", []string{msgCompleted.ID}, "late error", map[string]string{"lease_id": "test"}); err != nil {
		t.Fatalf("FailBurstTurn error: %v", err)
	}
	if postHookCompletedFired {
		t.Errorf("post_turn hook must NOT fire when messages are already terminal")
	}

	var postHookReplayCount int
	poolReplay := NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				postHookReplayCount++
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msgActive := db.Message{ID: "msg-replay-active", ThreadID: "chan-replay", Status: db.StatusProcessing, RetryCount: 1, CreatedAt: time.Now().UTC()}
	if err := insertMessage(store, msgActive); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if err := poolReplay.FailBurstTurn(context.Background(), "turn-replay", []string{msgActive.ID}, "runner panic", map[string]string{"lease_id": "test"}); err != nil {
		t.Fatalf("FailBurstTurn error: %v", err)
	}
	if postHookReplayCount != 1 {
		t.Fatalf("expected post_turn to fire once, got %d", postHookReplayCount)
	}
	if err := poolReplay.FailBurstTurn(context.Background(), "turn-replay", []string{msgActive.ID}, "runner panic", map[string]string{"lease_id": "test"}); err != nil {
		t.Fatalf("FailBurstTurn error: %v", err)
	}
	if postHookReplayCount != 1 {
		t.Errorf("post_turn hook must NOT fire again on replay, count = %d", postHookReplayCount)
	}
}

func TestWorkerPool_MultiTurn_AlwaysDeliversFinalOutputText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, convID, msgID, threadID, transcriptContent, runnerResponse string
	}{
		{name: "AlwaysDeliversFinalOutputText", convID: "c2222222-2222-3333-4444-555555555555", msgID: "msg-delivered-1", threadID: "thread-delivered-1", transcriptContent: "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"Check sync\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Checking status now...\",\"tool_calls\":[{\"name\":\"status\"}]}\n{\"step_index\":2,\"source\":\"SYSTEM\",\"type\":\"USER_INPUT\",\"content\":\"All in sync\"}\n{\"step_index\":3,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Wait for background task task-645 to complete.\"}\n", runnerResponse: "Wait for background task task-645 to complete."},
		{name: "TranscriptSilent_PreservesSubstantiveRunnerResponse", convID: "c3333333-3333-4444-5555-666666666666", msgID: "msg-substantive-fallback-1", threadID: "thread-substantive-fallback-1", transcriptContent: "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"Check deployment\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"Checking status now...\",\"tool_calls\":[{\"name\":\"status\"}]}\n{\"step_index\":2,\"source\":\"SYSTEM\",\"type\":\"USER_INPUT\",\"content\":\"All containers deployed\"}\n{\"step_index\":3,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"   \"}\n", runnerResponse: "Continuous Deployment for PR #322 has completed successfully!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			tempDir := t.TempDir()
			sessDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", tc.convID, ".system_generated", "logs")
			require.NoError(t, os.MkdirAll(sessDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(tc.transcriptContent), 0o644))

			var deliveredText string
			var mu sync.Mutex
			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tempDir, ""), Store: store, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return mockMultiTurnJSONResponse(tc.convID, tc.runnerResponse, 2), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return nil
				},
				TypingFunc: noopTyping,
			})
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Check", Status: db.StatusPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			require.NoError(t, insertMessage(store, msg))
			if lt := runLiveBurst(context.Background(), pool, tc.threadID, "turn-1", tc.msgID); lt.err != nil || lt.outcome != OutcomeCompleted {
				t.Fatalf("turn = (%s, %v), want completed", lt.outcome, lt.err)
			}

			mu.Lock()
			defer mu.Unlock()
			if deliveredText != tc.runnerResponse {
				t.Errorf("deliveredText = %q, want %q", deliveredText, tc.runnerResponse)
			}
			if dbMsg, err := getMessage(store, tc.msgID); err != nil || dbMsg == nil || dbMsg.Status != db.StatusCompleted || dbMsg.ResponseText != tc.runnerResponse {
				t.Errorf("DB response mismatch: %+v (err %v)", dbMsg, err)
			}
		})
	}
}

type sessionRotationCase struct {
	name, mode, threadID                                                   string
	saveInitialSession, checkStoreSession                                  bool
	seedTurns, preTranscriptSteps, preTranscriptBytes, postTranscriptSteps int
	postTranscriptBytes, wantAttempts                                      int
}

type sessionPlainRunner = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error)

func setupSessionRotationRunner(tc sessionRotationCase, sessionID, tPath string, attempts *[]string, mu *sync.Mutex) sessionPlainRunner {
	runnerPlain := func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		mu.Lock()
		*attempts = append(*attempts, sID)
		mu.Unlock()
		if tc.postTranscriptSteps > 0 {
			if err := os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.postTranscriptSteps)), 0o644); err != nil {
				return "", "", 1, err
			}
		}
		if tc.postTranscriptBytes > 0 {
			if err := os.WriteFile(tPath, make([]byte, tc.postTranscriptBytes), 0o644); err != nil {
				return "", "", 1, err
			}
		}
		retID := sID
		if retID == "" {
			retID = sessionID
		}
		return mockJSONResponse(retID, "OK"), "", 0, nil
	}
	return runnerPlain
}

func runSessionRotationCase(t *testing.T, tc sessionRotationCase) {
	t.Helper()
	t.Parallel()
	store := testStore(t)
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, "")

	sessionID := "sess-" + strings.ToLower(tc.name)
	sessDir, err := ensureTestSessionDir(sessMgr, sessionID)
	require.NoError(t, err)
	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")

	if tc.saveInitialSession {
		require.NoError(t, saveSessionID(store, tc.threadID, sessionID))
	}
	for range tc.seedTurns {
		_, err := incrementSessionTurnCount(store, tc.threadID)
		require.NoError(t, err)
	}
	if tc.preTranscriptSteps > 0 {
		require.NoError(t, os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.preTranscriptSteps)), 0o644))
	}
	if tc.preTranscriptBytes > 0 {
		require.NoError(t, os.WriteFile(tPath, bytes.Repeat([]byte("A"), tc.preTranscriptBytes), 0o644))
	}

	var mu sync.Mutex
	var attempts []string

	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: tc.mode}}})
	poolCfg := WorkerPoolConfig{
		SessionManager: sessMgr,
		Store:          store,
		BackoffBase:    5 * time.Millisecond,
		MaxAttempts:    2,
		DeliveryFunc:   noopDelivery,
		TypingFunc:     noopTyping,
		RunnerFunc:     setupSessionRotationRunner(tc, sessionID, tPath, &attempts, &mu),
	}

	pool := New(appCfg, poolCfg)
	defer pool.Stop()
	msg := db.Message{ID: "msg-rot-" + tc.name, ThreadID: tc.threadID, Content: "<@aerial> run", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	require.NoError(t, insertMessage(store, msg))
	if lt := runLiveBurst(context.Background(), pool, tc.threadID, "turn-1", msg.ID); lt.err != nil || lt.outcome != OutcomeCompleted {
		t.Fatalf("turn = (%s, %v), want completed", lt.outcome, lt.err)
	}

	mu.Lock()
	attCopy := append([]string(nil), attempts...)
	mu.Unlock()
	if tc.wantAttempts > 0 {
		if len(attCopy) != tc.wantAttempts {
			t.Fatalf("len(attempts) = %d (%v), want %d", len(attCopy), attCopy, tc.wantAttempts)
		}
		for i, got := range attCopy {
			if got != sessionID {
				t.Errorf("attempts[%d] = %q, want %q", i, got, sessionID)
			}
		}
	}
	if tc.checkStoreSession {
		curSess, err := getSessionID(store, tc.threadID)
		require.NoError(t, err)
		if curSess != sessionID {
			t.Errorf("store session = %q, want %q", curSess, sessionID)
		}
	}
}

func TestSession_RotationThresholds(t *testing.T) {
	t.Parallel()
	tests := []sessionRotationCase{
		{name: "TurnLimit_PreFlight", mode: "thread", threadID: "thread-rotate-test", saveInitialSession: true, seedTurns: 10, wantAttempts: 1, checkStoreSession: true},
		{name: "StepLimit_PreFlight", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, seedTurns: 1, preTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 1, checkStoreSession: true},
		{name: "ByteLimit_PreFlight", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, seedTurns: 1, preTranscriptBytes: session.DefaultMaxTranscriptBytes + 1024, wantAttempts: 1, checkStoreSession: true},
		{name: "StepLimit_PostExecution", mode: "thread", threadID: "thread-rot-threshold", postTranscriptSteps: session.DefaultMaxSessionSteps, checkStoreSession: true},
		{name: "ChannelMode_PreFlight_ByteLimit", mode: "channel", threadID: "chan-byte-test", saveInitialSession: true, seedTurns: 1, preTranscriptBytes: session.DefaultMaxTranscriptBytes + 1024, wantAttempts: 1},
		{name: "ChannelMode_PreFlight_StepLimit", mode: "channel", threadID: "chan-step-test", saveInitialSession: true, seedTurns: 1, preTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 1},
		{name: "PostExecution_ByteLimit", mode: "thread", threadID: "thread-post-byte-test", saveInitialSession: true, seedTurns: 1, postTranscriptBytes: session.DefaultMaxTranscriptBytes + 500, checkStoreSession: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runSessionRotationCase(t, tc) })
	}
}

func TestWorkerPool_DeliverDirect(t *testing.T) {
	t.Parallel()
	dummySession := &discordgo.Session{}
	customErr := errors.New("custom delivery error")
	tests := []struct {
		name, channelID, text, expectErrContains string
		nilPool, nilSession                      bool
		funcErr, expectErr                       error
	}{
		{name: "nil pool returns error", nilPool: true, channelID: "123", text: "hello", expectErrContains: "worker pool is nil"},
		{name: "nil session with nil DeliveryFunc returns error", nilSession: true, channelID: "123", text: "hello", expectErrContains: "discord session is not connected"},
		{name: "injected DeliveryFunc invoked", channelID: "123", text: "test message"},
		{name: "session error propagation", funcErr: customErr, channelID: "123", text: "test message", expectErr: customErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool || tc.nilSession {
				var p *WorkerPool
				if !tc.nilPool {
					p = &WorkerPool{}
				}
				if err := p.DeliverDirect(tc.channelID, tc.text); err == nil || err.Error() != tc.expectErrContains {
					t.Fatalf("expected error %q, got %v", tc.expectErrContains, err)
				}
				return
			}
			var gotChan, gotText string
			var gotSess *discordgo.Session
			p := &WorkerPool{cfg: WorkerPoolConfig{
				DiscordSession: dummySession,
				DeliveryFunc: func(s *discordgo.Session, cID, txt string) error {
					gotSess, gotChan, gotText = s, cID, txt
					return tc.funcErr
				},
			}}
			err := p.DeliverDirect(tc.channelID, tc.text)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				return
			}
			if err != nil || gotSess != dummySession || gotChan != tc.channelID || gotText != tc.text {
				t.Fatalf("unexpected delivery: err=%v chan=%q text=%q", err, gotChan, gotText)
			}
		})
	}
}

// mustRunLiveBurst drives a one-message burst turn through the TurnRunner steps and fails the
// test unless it ends with want.
func mustRunLiveBurst(t *testing.T, pool *WorkerPool, threadID, msgID string, want TurnOutcome) {
	t.Helper()
	if lt := runLiveBurst(context.Background(), pool, threadID, "turn-"+msgID, msgID); lt.err != nil || lt.outcome != want {
		t.Fatalf("turn %s = (%s, %v), want %s", msgID, lt.outcome, lt.err, want)
	}
}
