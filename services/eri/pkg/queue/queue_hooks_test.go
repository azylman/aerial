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

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/notifier"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/bwmarrin/discordgo"
)

func TestProcessBurst_RecordsTokensTelemetry(t *testing.T) {
	cases := []struct {
		name         string
		model        string
		threadID     string
		channelName  string
		mockOutput   string
		wantContains []string
		wantExcludes string
	}{
		{name: "NamedChannel", model: "test-token-model", threadID: "chan-token-test-1", mockOutput: `{"conversation_id":"c1111111-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":150,"output_tokens":75,"thinking_tokens":30,"cache_read_tokens":10,"total_tokens":265}}`, wantContains: []string{`aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="total"} 265`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="input"} 150`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="output"} 75`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="thinking"} 30`, `aerial_brain_tokens_total{channel="chan-token-test-1",model="test-token-model",type="cache_read"} 10`}},
		{name: "SnowflakeChannelSanitized", model: "test-snowflake-model", threadID: "1534436119888793750", mockOutput: `{"conversation_id":"c2222222-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":100,"output_tokens":50,"thinking_tokens":20,"cache_read_tokens":5,"total_tokens":175}}`, wantContains: []string{`aerial_brain_tokens_total{channel="unknown",model="test-snowflake-model",type="total"} 175`}, wantExcludes: "1534436119888793750"},
		{name: "BurstChannelNameFallback", model: "test-burst-chan-model", threadID: "low_effort:1534436119888793750", channelName: "#aerial-dev", mockOutput: `{"conversation_id":"c3333333-1111-2222-3333-444444444444","status":"SUCCESS","response":"Done!","usage":{"input_tokens":80,"output_tokens":20,"total_tokens":100}}`, wantContains: []string{`aerial_brain_tokens_total{channel="aerial-dev",model="test-burst-chan-model",type="total"} 100`}, wantExcludes: "low_effort:1534436119888793750"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := setupTestStore(t)
			pool := New(nil, WorkerPoolConfig{
				Store: store,
				Model: tc.model,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.mockOutput, "", 0, nil
				},
			})
			pool.Start()
			defer pool.Stop()

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
			_ = insertMessage(store, msg)
			pool.processBurst([]db.Message{msg})

			rec := httptest.NewRecorder()
			metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			for _, sub := range tc.wantContains {
				if !strings.Contains(body, sub) {
					t.Errorf("metrics body missing %q:\n%s", sub, body)
				}
			}
			if tc.wantExcludes != "" && strings.Contains(body, tc.wantExcludes) {
				t.Errorf("metrics body should not contain %q:\n%s", tc.wantExcludes, body)
			}
		})
	}
}

func TestThreadStatusQueueIntegration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	threadID := "thread-status-test-123"
	var finalDelivered atomic.Int32

	sess, _ := discordgo.New("Bot mock_token")
	sess.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		msgJSON := `{"id":"mock-status-msg-123","channel_id":"thread-status-test-123","content":"mock status"}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(msgJSON)),
		}, nil
	})
	sess.State = discordgo.NewState()
	_ = sess.State.ChannelAdd(&discordgo.Channel{
		ID: threadID, Name: "status-thread", Type: discordgo.ChannelTypeGuildPublicThread, ParentID: "parent-chan-1",
	})

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
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

	msg := db.Message{ID: "msg-thread-1", ThreadID: threadID, AuthorID: "user-1", Content: "check my file", CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})

	if finalDelivered.Load() != 1 {
		t.Errorf("Expected 1 final message delivered, got %d", finalDelivered.Load())
	}
}

func TestWorkerPool_HermeticByDefaultWithoutRetriever(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
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
	msg := db.Message{ID: "msg-hermetic-1", ThreadID: "thread-hermetic-1", AuthorID: "user-1", Content: "Hermetic test without retriever", CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	poolWithDB.processBurst([]db.Message{msg})
	if !runnerCalled {
		t.Errorf("Expected runner to be called cleanly without error")
	}
}

func TestProcessBurst_GracefulShutdown_ResetsToPendingWithoutRestartPenalty(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	msg := db.Message{ID: "msg-graceful-shutdown", ThreadID: "thread-graceful", Content: "task interrupted by deployment", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool := NewWorkerPool(WorkerPoolConfig{Store: store})
	pool.ctx = ctx
	pool.processBurst([]db.Message{msg})

	updated, err := getMessage(store, "msg-graceful-shutdown")
	if err != nil || updated == nil || updated.Status != db.StatusPending || updated.RestartCount != 0 {
		t.Fatalf("updated = %+v (err=%v), want PENDING with restart_count=0", updated, err)
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
	}{
		{name: "WatchdogExhaustion_InvokesNotifierFunc", threadID: "thread-wd-1", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded [watchdog]", runnerErr: errors.New("watchdog timeout"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "execution timed out", notifierReturn: "DEADASS TIMED OUT 💀", expectDeliveredNotice: "DEADASS TIMED OUT 💀"},
		{name: "NonTransient_InvokesNotifierFunc", threadID: "thread-nt-1", maxAttempts: 3, runnerStderr: "Error: unknown flag: --bogus-flag", runnerErr: errors.New("exit status 1"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "non-transient error", notifierReturn: "UNACCEPTABLE GARBAGE DETECTED 🔥", expectDeliveredNotice: "UNACCEPTABLE GARBAGE DETECTED 🔥"},
		{name: "RetryExhaustion_InvokesNotifierFunc", threadID: "thread-exhaust-1", maxAttempts: 2, runnerStderr: "connection reset by peer", runnerErr: errors.New("network drop"), runnerExitCode: 1, expectNotifierCalled: true, expectedDescContains: "exhausting 2 attempts", notifierReturn: "ROASTED TO ASHES 🤘", expectDeliveredNotice: "ROASTED TO ASHES 🤘"},
		{name: "RateLimitBypassesNotifierFunc", threadID: "thread-rl-1", maxAttempts: 1, runnerStderr: "Error 429: Too Many Requests", runnerErr: errors.New("rate limit"), runnerExitCode: 1, expectUnavailableMsg: true},
		{name: "Watchdog_RateLimitBypassesNotifier", threadID: "thread-wd-rl", maxAttempts: 1, runnerStderr: "inactivity timeout exceeded: [watchdog] Resource has been exhausted (quota limit reached)", runnerErr: context.DeadlineExceeded, runnerExitCode: 124, expectUnavailableMsg: true},
		{name: "SessionCorruption_RateLimitBypassesNotifier", threadID: "thread-sc-rl", maxAttempts: 1, seedSessionID: "sess-existing-1", runnerStderr: "conversation not found: 429 rate limit exceeded", runnerErr: errors.New("rate limited corruption"), runnerExitCode: 1, expectUnavailableMsg: true},
		{name: "NonTransient_LongSnippetAndDeliveryErr", threadID: "thread-nt-long", maxAttempts: 1, runnerStderr: strings.Repeat("VeryBadError-", 50), runnerErr: errors.New("fatal command syntax"), runnerExitCode: 1, deliveryErr: errors.New("discord delivery network failed"), expectNotifierCalled: true, wantMaxDescLen: 350, notifierReturn: "DEADASS BROKEN 💀"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, doneCh := setupTestStore(t), make(chan struct{}, 1)
			if tt.seedSessionID != "" {
				_ = rotateSessionID(store, tt.threadID, tt.seedSessionID)
			}
			var capturedDesc, deliveredNotif string
			var mu sync.Mutex
			notifierCalled := false

			pool := NewWorkerPool(WorkerPoolConfig{
				Store: store, MaxAttempts: tt.maxAttempts, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
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
				OnMessageCompleted: func(msg db.Message, finalStatus string) { doneCh <- struct{}{} },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: "msg-" + tt.name, ThreadID: tt.threadID, Content: "test content"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, tt.name)

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

type workerPoolCancellationCase struct {
	name, threadID, scheduleRunID, stderr string
	maxAttempts, exitCode                 int
	backoffBase                           time.Duration
	err                                   error
	expectDeliveredCheck                  bool
}

func runWorkerPoolCancellationCase(t *testing.T, tt workerPoolCancellationCase) {
	t.Helper()
	store := setupTestStore(t)
	var pool *WorkerPool
	deliveredCalled := false
	var mu sync.Mutex

	pool = NewWorkerPool(WorkerPoolConfig{
		Store: store, MaxAttempts: tt.maxAttempts, TimeoutMinutes: 1, BackoffBase: tt.backoffBase,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			if tt.maxAttempts > 1 {
				go func() {
					for {
						select {
						case <-pool.ctx.Done():
							return
						default:
						}
						if m, _ := getMessage(store, "msg-"+tt.name); m != nil && m.RetryCount > 0 {
							pool.cancel()
							return
						}
						runtime.Gosched()
					}
				}()
			} else {
				pool.cancel()
			}
			return "", tt.stderr, tt.exitCode, tt.err
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
			mu.Lock()
			deliveredCalled = true
			mu.Unlock()
			return nil
		},
	})
	pool.Start()
	defer pool.Stop()

	msgID := "msg-" + tt.name
	msg := db.Message{ID: msgID, ThreadID: tt.threadID, Content: "cancel test"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)
	waitPoolWorkersDone(t, pool, 2*time.Second)

	if tt.expectDeliveredCheck {
		mu.Lock()
		called := deliveredCalled
		mu.Unlock()
		if called {
			t.Errorf("DeliveryFunc should not be called when pool is cancelled")
		}
	}
	if dbMsg, _ := getMessage(store, msgID); dbMsg != nil && dbMsg.Status != db.StatusProcessing {
		t.Errorf("Expected status PROCESSING after cancellation, got: %s", dbMsg.Status)
	}
}

func TestWorkerPool_CancellationDuringBackoffSuite(t *testing.T) {
	t.Parallel()
	tests := []workerPoolCancellationCase{
		{name: "Watchdog_CancelledDuringBackoff", threadID: "thread-wd-cancel", scheduleRunID: "run-wd-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "inactivity timeout exceeded: [watchdog] hung process", exitCode: 124, err: context.DeadlineExceeded},
		{name: "Watchdog_CancelledPostWatchdog", threadID: "thread-wd-post-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "inactivity timeout exceeded: [watchdog] single attempt cancel", exitCode: 124, err: context.DeadlineExceeded, expectDeliveredCheck: true},
		{name: "NonTransient_CancelledDuringTurn", threadID: "thread-nt-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "Fatal: invalid prompt parameters", exitCode: 2, err: errors.New("non transient err"), expectDeliveredCheck: true},
		{name: "RetryExhaustion_CancelledWithScheduleRunID", threadID: "thread-exh-cancel", scheduleRunID: "run-exh-cancel", maxAttempts: 1, backoffBase: 5 * time.Millisecond, stderr: "connection timed out", exitCode: 1, err: errors.New("timeout")},
		{name: "RateLimit_CancelledDuringBackoff", threadID: "thread-rl-cancel", scheduleRunID: "run-rl-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "Error 429: Resource has been exhausted (quota exceeded)", exitCode: 1, err: errors.New("rate limited")},
		{name: "Transient_CancelledDuringBackoff", threadID: "thread-tr-cancel", scheduleRunID: "run-tr-cancel", maxAttempts: 3, backoffBase: 500 * time.Millisecond, stderr: "connection reset by peer", exitCode: 1, err: errors.New("transient network drop")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runWorkerPoolCancellationCase(t, tt)
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

func runWakeHookEval(t *testing.T, hookResp *WakeResponse, hookErr error, timeoutAction, classifierResp string, inputMsgs []db.Message, emptyEffID bool, schedRunID string) (bool, bool, bool, *turnExecution, db.Store, *WakeRequest) {
	t.Helper()
	store, now := setupTestStore(t), time.Now().UTC()
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
		_ = insertMessage(store, m)
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
	return shouldExit, hookCalled, classifierCalled, te, store, capturedReq
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
	if saved, _ := getMessage(store, tc.msgs[0].ID); saved == nil || saved.Status != db.StatusCompleted {
		t.Fatalf("saved msg = %+v", saved)
	}
}

func assertWakeHookNoExit(t *testing.T, tc onWakeHookCase, te *turnExecution, store db.Store) {
	t.Helper()
	if te.wakeIdx != 0 || len(te.wakeInfos) == 0 || !te.wakeInfos[0].IsWake {
		t.Errorf("wakeIdx=%d wakeInfos=%+v", te.wakeIdx, te.wakeInfos)
	}
	if tc.wantTurns > 0 {
		if turnCount, _ := getSessionTurnCount(store, tc.msgs[0].ThreadID); turnCount != tc.wantTurns {
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
	shouldExit, hookCalled, classifierCalled, te, store, capturedReq := runWakeHookEval(t, tc.hookResp, tc.hookErr, tc.timeoutAction, tc.classifierResp, tc.msgs, tc.isSched, tc.schedRunID)
	if shouldExit != tc.wantExit || !hookCalled || classifierCalled != tc.wantClassifier {
		t.Fatalf("%s: got exit=%v hook=%v classifier=%v (want exit=%v classifier=%v)", tc.name, shouldExit, hookCalled, classifierCalled, tc.wantExit, tc.wantClassifier)
	}
	assertWakeHookExecution(t, tc, te, store)
	if capturedReq == nil || capturedReq.ChannelID == "" || capturedReq.Timestamp.IsZero() {
		t.Errorf("capturedReq invalid: %+v", capturedReq)
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
	store, now := setupTestStore(t), time.Now().UTC()
	var mu sync.Mutex
	var runnerCalls, deliveryCalls int
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""), TimeoutMinutes: 1,
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
	_ = insertMessage(store, msgDrop)
	pool.processBurst([]db.Message{msgDrop})
	if savedDrop, _ := getMessage(store, "msg-burst-drop"); savedDrop == nil || savedDrop.Status != db.StatusCompleted || !strings.Contains(savedDrop.ErrorMessage, "[AMBIENT") {
		t.Errorf("expected drop message completed as ambient, got %+v", savedDrop)
	}
	_ = insertMessage(store, msgWake)
	pool.processBurst([]db.Message{msgWake})
	mu.Lock()
	defer mu.Unlock()
	if runnerCalls != 1 || deliveryCalls != 1 {
		t.Errorf("runnerCalls=%d, deliveryCalls=%d, want 1, 1", runnerCalls, deliveryCalls)
	}
	if savedWake, _ := getMessage(store, "msg-burst-wake"); savedWake == nil || savedWake.Status != db.StatusCompleted || savedWake.ResponseText != "Bot response" {
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

func runPreTurnHookBurst(t *testing.T, hookResps []*PreTurnResponse, hookErr error, timeoutAction string, initialRetry int, schedRunID string, waitAsync bool) (int, int, string, *PreTurnRequest, *db.Message, db.Store) {
	t.Helper()
	store, doneCh := setupTestStore(t), make(chan struct{})
	var mu sync.Mutex
	var runnerCalls, hookCalls int
	var capturedPrompt string
	var capturedReq *PreTurnRequest

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, SessionManager: session.New(t.TempDir(), ""), TimeoutMinutes: 1, MaxAttempts: 3,
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
		OnMessageCompleted: func(m db.Message, status string) {
			if m.ID == "msg-preturn-test" {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
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
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})
	if waitAsync {
		waitForCompletion(t, doneCh, 5*time.Second, "preturn-wait")
	}

	mu.Lock()
	defer mu.Unlock()
	saved, _ := getMessage(store, "msg-preturn-test")
	return hookCalls, runnerCalls, capturedPrompt, capturedReq, saved, store
}

func TestPreTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, timeoutAction, schedRunID, wantStatus, wantErrSub, wantPromptSub string
		hookResps                                                              []*PreTurnResponse
		hookErr                                                                error
		initialRetry, wantHookCalls, wantRunner                                int
		waitAsync                                                              bool
	}{
		{name: "Rejected_DropAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "drop", Reason: "resource busy"}}, wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "resource busy"},
		{name: "TimeoutFallback_Drop", hookErr: context.DeadlineExceeded, timeoutAction: "drop", wantHookCalls: 1, wantStatus: db.StatusCompleted, wantErrSub: "pre_turn_timeout_drop"},
		{name: "TimeoutFallback_Proceed", hookErr: errors.New("webhook failed"), timeoutAction: "proceed", wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted},
		{name: "TimeoutFallback_Retry", hookErr: errors.New("gateway timeout"), timeoutAction: "retry", wantHookCalls: 1, wantStatus: db.StatusPending},
		{name: "ApprovedWithInjectedContext", hookResps: []*PreTurnResponse{{Allow: true, InjectedContext: "system instructions: be concise", Metadata: map[string]any{"lease_token": "token-123"}}}, wantHookCalls: 1, wantRunner: 1, wantStatus: db.StatusCompleted, wantPromptSub: "<COORDINATION_CONTEXT>\nsystem instructions: be concise\n</COORDINATION_CONTEXT>"},
		{name: "RejectedRetryAction", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1, Reason: "sidecar lock wait"}, {Allow: true}}, waitAsync: true, wantHookCalls: 2, wantRunner: 1, wantStatus: db.StatusCompleted},
		{name: "RetryExhaustionWithScheduleRun", hookResps: []*PreTurnResponse{{Allow: false, Action: "retry", RetryAfterSeconds: 1}}, initialRetry: 2, schedRunID: "run-exhaust-1", wantHookCalls: 1, wantRunner: 0, wantStatus: db.StatusFailed, wantErrSub: "[EXHAUSTED_PRE_TURN_RETRIES]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hc, rc, cp, cr, saved, _ := runPreTurnHookBurst(t, tc.hookResps, tc.hookErr, tc.timeoutAction, tc.initialRetry, tc.schedRunID, tc.waitAsync)
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
		})
	}
}

func TestPreTurnHook_HelperEdgeCases(t *testing.T) {
	t.Parallel()
	if reqNil := buildPreTurnRequest(nil); reqNil == nil || reqNil.Timestamp.IsZero() {
		t.Errorf("expected valid from nil te")
	}
	msgA, msgB := db.Message{ID: "mA", RetryCount: 1, Content: "promptA"}, db.Message{ID: "mB", RetryCount: 3, Content: "promptB"}
	if req := buildPreTurnRequest(&turnExecution{threadID: "t-fallback", burst: []db.Message{msgA, msgB}}); req.ChannelID != "t-fallback" || req.RetryCount != 3 {
		t.Errorf("req = %+v", req)
	}
	teNoPool := &turnExecution{pool: nil, burst: []db.Message{msgA}}
	teNoPool.rescheduleBurst(0)
	teNoPool.abortBurst("test drop nil pool")

	store, completedStatus := setupTestStore(t), ""
	pool := NewWorkerPool(WorkerPoolConfig{Store: store, OnMessageCompleted: func(msg db.Message, status string) { completedStatus = status }})
	schedMsg := db.Message{ID: "msg-sched-abort", ThreadID: "chan-sched", Status: db.StatusPending}
	_ = insertMessage(store, schedMsg)
	(&turnExecution{pool: pool, burst: []db.Message{schedMsg}, threadID: "chan-sched"}).abortBurst("manual abort")
	if completedStatus != db.StatusCompleted {
		t.Errorf("completedStatus=%s, want COMPLETED", completedStatus)
	}
}

func runPostTurnHookBurst(t *testing.T, preResp *PreTurnResponse, runnerStderr string, runnerErr error, runnerPanic string) *PostTurnRequest {
	t.Helper()
	store := setupTestStore(t)
	var mu sync.Mutex
	var postCalled bool
	var postReqReceived *PostTurnRequest
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
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
			return mockJSONResponseWithUsage(sessionID, "Clean success response from runner", expectedUsage), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})
	msg := db.Message{ID: "msg-postturn-test", ThreadID: "chan-postturn-test", AuthorName: "user-alpha", Content: "run postturn test", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = insertMessage(store, msg)
	pool.processBurst([]db.Message{msg})

	mu.Lock()
	defer mu.Unlock()
	if !postCalled || postReqReceived == nil {
		t.Fatalf("expected post_turn hook to be called")
	}
	return postReqReceived
}

func TestPostTurnHook_Lifecycle(t *testing.T) {
	t.Parallel()
	expectedUsage := runner.TokenUsage{InputTokens: 100, OutputTokens: 50, ThinkingTokens: 10, CacheReadTokens: 5, TotalTokens: 150}
	tests := []struct {
		name, runnerStderr, runnerPanic, wantStatus, wantErrSub string
		preResp                                                 *PreTurnResponse
		runnerErr                                               error
		checkMeta                                               bool
	}{
		{name: "SuccessfulTurnWithMetadata", preResp: &PreTurnResponse{Allow: true, Metadata: map[string]any{"lease_id": "lease-abc", "turn_epoch": float64(42)}}, wantStatus: "success", checkMeta: true},
		{name: "FailedTurn_AgyError", runnerStderr: "non-transient runner failed: syntax error in script", runnerErr: fmt.Errorf("exit status 1"), wantStatus: "failed", wantErrSub: "syntax error"},
		{name: "RunnerPanicRecovery", runnerPanic: "simulated runner crash inside runner function", wantStatus: "failed", wantErrSub: "simulated runner crash"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := runPostTurnHookBurst(t, tc.preResp, tc.runnerStderr, tc.runnerErr, tc.runnerPanic)
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
		})
	}
}

func TestPostTurnHook_SerializedRelease(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	var mu sync.Mutex
	turn1PostStarted, turn2Launched := make(chan struct{}), make(chan struct{})
	turn1PostFinished, turn2PreCalledWhileTurn1InPost, turn2PreCalled := false, false, false

	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
		WebhookDispatcher: &mockWebhookDispatcher{
			callPreTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
				mu.Lock()
				if req.ThreadID == "chan-serialized" && len(req.Messages) > 0 && req.Messages[0].ID == "msg-turn-2" {
					turn2PreCalled = true
					if !turn1PostFinished {
						turn2PreCalledWhileTurn1InPost = true
					}
				}
				mu.Unlock()
				return &PreTurnResponse{Allow: true}, nil
			},
			callPostTurnHookFunc: func(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
				if req.ThreadID == "chan-serialized" {
					select {
					case <-turn1PostStarted:
					default:
						close(turn1PostStarted)
						<-turn2Launched
						for range 64 {
							runtime.Gosched()
						}
						mu.Lock()
						turn1PostFinished = true
						mu.Unlock()
					}
				}
				return &PostTurnResponse{Acknowledged: true}, nil
			},
		},
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return mockJSONResponse(sessionID, "ok"), "", 0, nil
		},
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{Mode: "channel", WakeMode: "all", Hooks: config.ChannelHooksConfig{
				PreTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/pre-turn"}, PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post-turn"},
			}}
		},
	})

	now := time.Now().UTC()
	msg1, msg2 := db.Message{ID: "msg-turn-1", ThreadID: "chan-serialized", AuthorName: "user-one", Content: "turn 1", Status: db.StatusPending, CreatedAt: now}, db.Message{ID: "msg-turn-2", ThreadID: "chan-serialized", AuthorName: "user-two", Content: "turn 2", Status: db.StatusPending, CreatedAt: now.Add(time.Millisecond)}
	_ = insertMessage(store, msg1)
	_ = insertMessage(store, msg2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pool.processBurst([]db.Message{msg1})
	}()
	<-turn1PostStarted
	go func() {
		defer wg.Done()
		close(turn2Launched)
		pool.processBurst([]db.Message{msg2})
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if !turn2PreCalled || turn2PreCalledWhileTurn1InPost {
		t.Errorf("turn2PreCalled=%v, turn2PreCalledWhileTurn1InPost=%v", turn2PreCalled, turn2PreCalledWhileTurn1InPost)
	}
}

func TestPostTurnHook_HelperEdgeCases(t *testing.T) {
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

	store := setupTestStore(t)
	var postHookFired bool
	pool := NewWorkerPool(WorkerPoolConfig{
		Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
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
	_ = insertMessage(store, msgDrop)
	pool.processBurst([]db.Message{msgDrop})
	if postHookFired {
		t.Errorf("post_turn hook must NOT fire when pre_turn dropped turn")
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
			store, tempDir := setupTestStore(t), t.TempDir()
			sessDir := filepath.Join(tempDir, ".gemini", "antigravity-cli", "brain", tc.convID, ".system_generated", "logs")
			_ = os.MkdirAll(sessDir, 0o755)
			_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(tc.transcriptContent), 0o644)

			var deliveredText string
			var mu sync.Mutex
			doneCh := make(chan struct{})
			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: session.New(tempDir, ""), Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 1,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return mockMultiTurnJSONResponse(tc.convID, tc.runnerResponse, 2), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return nil
				},
				TypingFunc:         noopTyping,
				OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, GuildID: "guild-1", AuthorID: "user-1", AuthorName: "User", Content: "Check", Status: db.StatusPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)
			waitForCompletion(t, doneCh, 3*time.Second, tc.name)

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
	name, mode, threadID                                                                string
	saveInitialSession, useWatchdog, checkStoreSession                                  bool
	seedTurns, preTranscriptSteps, preTranscriptBytes, postTranscriptSteps              int
	postTranscriptBytes, watchdogTranscriptSteps, watchdogTranscriptBytes, wantAttempts int
}

type (
	sessionPlainRunner = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error)
	sessionOptRunner   = func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error)
)

func writeWatchdogTranscript(tPath string, tc sessionRotationCase) {
	if tc.watchdogTranscriptSteps > 0 {
		_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.watchdogTranscriptSteps)), 0o644)
	}
	if tc.watchdogTranscriptBytes > 0 {
		_ = os.WriteFile(tPath, make([]byte, tc.watchdogTranscriptBytes), 0o644)
	}
}

func setupSessionRotationRunner(tc sessionRotationCase, sessionID, tPath string, attempts *[]string, mu *sync.Mutex) (sessionPlainRunner, sessionOptRunner) {
	if tc.useWatchdog {
		runnerOpt := func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			*attempts = append(*attempts, sID)
			curAttempt := len(*attempts)
			mu.Unlock()
			if curAttempt == 1 {
				writeWatchdogTranscript(tPath, tc)
				return "", "inactivity timeout exceeded [watchdog]", 124, errors.New("watchdog timeout")
			}
			return mockJSONResponse(sID, "Recovered on warm retry"), "", 0, nil
		}
		return nil, runnerOpt
	}

	runnerPlain := func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
		mu.Lock()
		*attempts = append(*attempts, sID)
		mu.Unlock()
		if tc.postTranscriptSteps > 0 {
			_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.postTranscriptSteps)), 0o644)
		}
		if tc.postTranscriptBytes > 0 {
			_ = os.WriteFile(tPath, make([]byte, tc.postTranscriptBytes), 0o644)
		}
		retID := sID
		if retID == "" {
			retID = sessionID
		}
		return mockJSONResponse(retID, "OK"), "", 0, nil
	}
	return runnerPlain, nil
}

func runSessionRotationCase(t *testing.T, tc sessionRotationCase) {
	t.Helper()
	t.Parallel()
	store, tmpDir := setupTestStore(t), t.TempDir()
	sessMgr := session.New(tmpDir, "")

	sessionID := "sess-" + strings.ToLower(tc.name)
	sessDir, _ := ensureTestSessionDir(sessMgr, sessionID)
	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")

	if tc.saveInitialSession {
		_ = saveSessionID(store, tc.threadID, sessionID)
	}
	for range tc.seedTurns {
		_, _ = incrementSessionTurnCount(store, tc.threadID)
	}
	if tc.preTranscriptSteps > 0 {
		_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tc.preTranscriptSteps)), 0o644)
	}
	if tc.preTranscriptBytes > 0 {
		_ = os.WriteFile(tPath, bytes.Repeat([]byte("A"), tc.preTranscriptBytes), 0o644)
	}

	var mu sync.Mutex
	var attempts []string
	doneCh := make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: tc.mode}}})
	runnerPlain, runnerOpt := setupSessionRotationRunner(tc, sessionID, tPath, &attempts, &mu)
	poolCfg := WorkerPoolConfig{
		SessionManager:        sessMgr,
		Store:                 store,
		TimeoutMinutes:        1,
		BackoffBase:           5 * time.Millisecond,
		MaxAttempts:           2,
		DeliveryFunc:          noopDelivery,
		TypingFunc:            noopTyping,
		RunnerFunc:            runnerPlain,
		RunnerWithOptionsFunc: runnerOpt,
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	}

	pool := New(appCfg, poolCfg)
	pool.Start()
	defer pool.Stop()
	msg := db.Message{ID: "msg-rot-" + tc.name, ThreadID: tc.threadID, Content: "<@aerial> run"}
	_ = insertMessage(store, msg)
	pool.Enqueue(msg)
	waitForCompletion(t, doneCh, 3*time.Second, tc.name)

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
		if curSess, _ := getSessionID(store, tc.threadID); curSess != sessionID {
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
		{name: "WatchdogCircuitBreaker_OnBloat", mode: "thread", threadID: "thread-rot-threshold", saveInitialSession: true, useWatchdog: true, watchdogTranscriptBytes: session.DefaultMaxTranscriptBytes + 2048, wantAttempts: 2},
		{name: "ChannelMode_PreFlight_ByteLimit", mode: "channel", threadID: "chan-byte-test", saveInitialSession: true, seedTurns: 1, preTranscriptBytes: session.DefaultMaxTranscriptBytes + 1024, wantAttempts: 1},
		{name: "ChannelMode_PreFlight_StepLimit", mode: "channel", threadID: "chan-step-test", saveInitialSession: true, seedTurns: 1, preTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 1},
		{name: "PostExecution_ByteLimit", mode: "thread", threadID: "thread-post-byte-test", saveInitialSession: true, seedTurns: 1, postTranscriptBytes: session.DefaultMaxTranscriptBytes + 500, checkStoreSession: true},
		{name: "Watchdog_StepLimit", mode: "thread", threadID: "thread-wd-step", saveInitialSession: true, seedTurns: 1, useWatchdog: true, watchdogTranscriptSteps: session.DefaultMaxSessionSteps, wantAttempts: 2},
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
