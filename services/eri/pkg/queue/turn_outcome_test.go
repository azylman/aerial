package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestWorkerPool_CapacityBlipMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, runnerErr, wantFinalStatus, wantErrContains, wantDeliveryContains string
		maxAttempts, wantAttempts                                               int
		presetGlobalLockSec                                                     int64
		deliveryErr                                                             error
		checkGlobalLockZero, checkNoSchedules                                   bool
	}{
		{name: "LocalRetry_Success", maxAttempts: 2, runnerErr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", wantAttempts: 2, wantFinalStatus: db.StatusCompleted, wantDeliveryContains: "Recovered on attempt 2!", checkGlobalLockZero: true},
		{name: "Exhaustion_LocalFailure", maxAttempts: 1, runnerErr: "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", wantAttempts: 1, wantFinalStatus: db.StatusFailed, wantErrContains: "[CAPACITY_EXHAUSTED", wantDeliveryContains: "capacity", checkGlobalLockZero: true, checkNoSchedules: true},
		{name: "WithoutCountdown_Fallback70s", maxAttempts: 1, runnerErr: "API error: RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model.", wantAttempts: 1, wantFinalStatus: db.StatusFailed, wantErrContains: "[CAPACITY_EXHAUSTED", wantDeliveryContains: "capacity", checkGlobalLockZero: true, checkNoSchedules: true},
		{name: "GlobalQuotaLockout_PreCheckFailFast", maxAttempts: 2, presetGlobalLockSec: 300, wantAttempts: 0, wantFinalStatus: db.StatusFailed, wantErrContains: "global quota pause active", wantDeliveryContains: "quota", deliveryErr: errors.New("discord delivery error"), checkNoSchedules: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			var mu sync.Mutex
			var attemptCount int
			var deliveredText string
			doneCh := make(chan struct{}, 1)
			appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
			haroSrv, getOneShots := newMockHaroOneShotServer(t)
			pool := New(appCfg, WorkerPoolConfig{
				SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
				RetryDelayOverride: 10 * time.Millisecond, MaxAttempts: tt.maxAttempts, HaroURL: haroSrv.URL,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					mu.Lock()
					attemptCount++
					cur := attemptCount
					mu.Unlock()
					if cur == 1 {
						return "", tt.runnerErr, 1, errors.New("exit code 1")
					}
					return mockJSONResponse(uuid.New().String(), "Recovered on attempt 2!"), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					mu.Lock()
					deliveredText = text
					mu.Unlock()
					return tt.deliveryErr
				},
				TypingFunc: func(s *discordgo.Session, channelID string) func() { return func() {} },
				OnMessageCompleted: func(msg db.Message, finalStatus string) {
					select {
					case doneCh <- struct{}{}:
					default:
					}
				},
			})
			pool.Start()
			defer pool.Stop()

			if tt.presetGlobalLockSec > 0 {
				pool.quotaLockedUntil.Store(time.Now().Add(time.Duration(tt.presetGlobalLockSec) * time.Second).Unix())
			}

			threadID, msgID := "thread-"+strings.ToLower(tt.name), "msg-"+strings.ToLower(tt.name)
			msg := db.Message{ID: msgID, ThreadID: threadID, Content: "hello " + tt.name}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for message completed")
			}
			pool.Stop()

			mu.Lock()
			attempts, delivered := attemptCount, deliveredText
			mu.Unlock()
			if attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
			dbMsg, _ := store.GetMessage(context.Background(), msgID)
			if dbMsg.Status != tt.wantFinalStatus {
				t.Errorf("status = %s, want %s", dbMsg.Status, tt.wantFinalStatus)
			}
			if tt.wantErrContains != "" && !strings.Contains(dbMsg.ErrorMessage, tt.wantErrContains) {
				t.Errorf("expected error %q in %q", tt.wantErrContains, dbMsg.ErrorMessage)
			}
			if tt.wantDeliveryContains != "" && !strings.Contains(delivered, tt.wantDeliveryContains) {
				t.Errorf("expected delivery %q in %q", tt.wantDeliveryContains, delivered)
			}
			if tt.checkGlobalLockZero && pool.quotaLockedUntil.Load() > 0 {
				t.Errorf("expected quotaLockedUntil == 0")
			}
			if tt.checkNoSchedules {
				s := getOneShots(threadID)
				if len(s) > 0 {
					t.Errorf("expected 0 schedules, got %d", len(s))
				}
			}
		})
	}
}

func TestWorkerPool_CapacityBlip_DoesNotBlockConcurrentThreads(t *testing.T) {
	t.Parallel()
	store, tmpDir := setupTestStore(t), t.TempDir()
	var mu sync.Mutex
	var threadAAttempts int
	var threadACompleted, threadBCompleted time.Time
	doneA, doneB := make(chan struct{}, 1), make(chan struct{}, 1)

	appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
	pool := New(appCfg, WorkerPoolConfig{
		SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond,
		RetryDelayOverride: 10 * time.Millisecond, MaxAttempts: 2,
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			if strings.Contains(prompt, "Thread A") {
				mu.Lock()
				threadAAttempts++
				att := threadAAttempts
				mu.Unlock()
				if att == 1 {
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
				}
				return mockJSONResponse(uuid.New().String(), "Thread A success"), "", 0, nil
			}
			return mockJSONResponse(uuid.New().String(), "Thread B success"), "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
		TypingFunc:   func(s *discordgo.Session, channelID string) func() { return func() {} },
		OnMessageCompleted: func(msg db.Message, finalStatus string) {
			mu.Lock()
			defer mu.Unlock()
			if msg.ThreadID == "thread-A" && finalStatus == db.StatusCompleted {
				threadACompleted = time.Now()
				select {
				case doneA <- struct{}{}:
				default:
				}
			} else if msg.ThreadID == "thread-B" && finalStatus == db.StatusCompleted {
				threadBCompleted = time.Now()
				select {
				case doneB <- struct{}{}:
				default:
				}
			}
		},
	})
	pool.Start()
	defer pool.Stop()

	msgA, msgB := db.Message{ID: "msg-thread-A", ThreadID: "thread-A", Content: "Thread A prompt"}, db.Message{ID: "msg-thread-B", ThreadID: "thread-B", Content: "Thread B prompt"}
	_ = insertMessage(store, msgA)
	_ = insertMessage(store, msgB)
	pool.Enqueue(msgA)
	pool.Enqueue(msgB)

	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for Thread B completed")
	}
	select {
	case <-doneA:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for Thread A completed")
	}
	if lockedUntil := pool.quotaLockedUntil.Load(); lockedUntil > 0 {
		t.Errorf("Expected quotaLockedUntil to remain 0, got %d", lockedUntil)
	}
	mu.Lock()
	aDone, bDone := threadACompleted.IsZero(), threadBCompleted.IsZero()
	mu.Unlock()
	if aDone || bDone {
		t.Errorf("Expected both threads to complete")
	}
	dbMsgB, _ := store.GetMessage(context.Background(), "msg-thread-B")
	if dbMsgB == nil || dbMsgB.Status != db.StatusCompleted {
		t.Errorf("Expected Thread B StatusCompleted")
	}
}

func TestWorkerPool_CapacityBlip_ContextCancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, msgID, threadID string
		checkTyping           bool
	}{
		{name: "ImmediateBackoff", msgID: "msg-cap-cancel", threadID: "thread-cap-cancel"},
		{name: "ProgressiveBackoff", msgID: "msg-cap-prog-cancel", threadID: "thread-cap-prog-cancel", checkTyping: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpDir := setupTestStore(t), t.TempDir()
			startedCh := make(chan struct{}, 1)
			var typingMu sync.Mutex
			var typingActive, typingStopped bool
			appCfg := config.NewFromData(&config.ConfigData{Channels: map[string]config.ChannelPolicy{"default": {Mode: "thread"}}})
			pool := New(appCfg, WorkerPoolConfig{
				SessionManager: session.New(tmpDir, tmpDir), Store: store, TimeoutMinutes: 1, BackoffBase: 5 * time.Millisecond, MaxAttempts: 2,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					select {
					case startedCh <- struct{}{}:
					default:
					}
					return "", "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", 1, errors.New("exit code 1")
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error { return nil },
				TypingFunc: func(s *discordgo.Session, channelID string) func() {
					typingMu.Lock()
					typingActive = true
					typingMu.Unlock()
					return func() {
						typingMu.Lock()
						typingStopped = true
						typingMu.Unlock()
					}
				},
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, Content: "cancel me"}
			_ = insertMessage(store, msg)
			pool.Enqueue(msg)

			select {
			case <-startedCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for attempt 1")
			}
			pool.cancel()
			waitPoolWorkersDone(t, pool, 3*time.Second)

			if tc.checkTyping {
				typingMu.Lock()
				active, stopped := typingActive, typingStopped
				typingMu.Unlock()
				if !active || !stopped {
					t.Errorf("expected typing active and then stopped on cancel")
				}
			}
			dbMsg, _ := store.GetMessage(context.Background(), tc.msgID)
			if dbMsg == nil || dbMsg.Status != db.StatusPending || !strings.Contains(dbMsg.ErrorMessage, "interrupted by graceful deployment") {
				t.Errorf("expected StatusPending with interrupted message")
			}
			if pool.quotaLockedUntil.Load() > 0 {
				t.Errorf("expected quotaLockedUntil == 0")
			}
		})
	}
}

func TestWorkerPool_ProgressiveCapacityBackoff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		attempt              int
		resetDur             time.Duration
		minFloor, maxCeiling time.Duration
	}{
		{name: "Attempt1_Floor30s", attempt: 1, resetDur: 0, minFloor: 30 * time.Second, maxCeiling: 33 * time.Second},
		{name: "Attempt2_Floor60s", attempt: 2, resetDur: 0, minFloor: 60 * time.Second, maxCeiling: 63 * time.Second},
		{name: "ResetDurOverride_75s", attempt: 1, resetDur: 75 * time.Second, minFloor: 75 * time.Second, maxCeiling: 78 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 20; i++ {
				d := calculateCapacityBackoff(tt.attempt, tt.resetDur)
				if d < tt.minFloor || d > tt.maxCeiling {
					t.Fatalf("backoff expected between %v and %v, got %v", tt.minFloor, tt.maxCeiling, d)
				}
			}
		})
	}
}

func TestDiscordTurnSink_Callbacks(t *testing.T) {
	t.Parallel()
	sink := newDiscordTurnSink(nil)
	sink.OnTurnStarted()
	sink.OnThinking()
	sink.OnTextDelta("some text")
	sink.OnError(errors.New("test error"))
	res, err := sink.Wait(context.Background())
	if err != nil || res.Response != "some text" {
		t.Fatalf("unexpected error/response: %v, %q", err, res.Response)
	}

	sinkNoDeltas := newDiscordTurnSink(nil)
	expectedErr := errors.New("hard failure")
	sinkNoDeltas.OnError(expectedErr)
	if _, err := sinkNoDeltas.Wait(context.Background()); err == nil || !errors.Is(err, expectedErr) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDiscordTurnSink_StatusUpdaterLifecycle(t *testing.T) {
	t.Parallel()
	updater := NewStatusUpdater(nil, "thread-123", true)
	updater.sendFunc = func(c, text string) (string, error) { return "msg-1", nil }
	updater.editFunc = func(c, m, text string) error { return nil }
	updater.deleteFunc = func(c, m string) error { return nil }
	defer updater.Stop()

	sink := newDiscordTurnSink(updater)
	assertUpdater := func(step, wantPhase, wantTool, wantCmd string) {
		t.Helper()
		updater.mu.Lock()
		defer updater.mu.Unlock()
		if updater.phase != wantPhase {
			t.Errorf("[%s] phase = %q, want %q", step, updater.phase, wantPhase)
		}
		if wantTool != "" && updater.activeTool != wantTool {
			t.Errorf("[%s] activeTool = %q, want %q", step, updater.activeTool, wantTool)
		}
		if wantCmd != "" && updater.activeCommand != wantCmd {
			t.Errorf("[%s] activeCommand = %q, want %q", step, updater.activeCommand, wantCmd)
		}
	}

	sink.OnThinking()
	assertUpdater("thinking", "thinking", "", "")
	sink.OnToolCall("run_command", "echo hello")
	assertUpdater("tool_call", "tool", "run_command", "echo")
	sink.OnToolCompleted("run_command", "native", 100*time.Millisecond, "ok")
	assertUpdater("tool_completed", "thinking", "", "")
	sink.OnTextDelta("Streaming response token")
	assertUpdater("text_delta", "responding", "", "")
	sink.OnThinking()
	assertUpdater("thinking", "thinking", "", "")
	sink.OnTextDelta("   \n\t  ")
	assertUpdater("whitespace_delta", "thinking", "", "")
	sink.OnToolCall("get_me", "")
	assertUpdater("tool_call", "tool", "get_me", "")
	sink.OnToolCompleted("get_me", "github", 50*time.Millisecond, "error")
	assertUpdater("tool_error", "thinking", "", "")

	nilSink := newDiscordTurnSink(nil)
	nilSink.OnThinking()
	nilSink.OnToolCall("run_command", "ls")
	nilSink.OnToolCompleted("run_command", "native", time.Millisecond, "ok")
	nilSink.OnTextDelta("delta")
	nilSink.OnSkillActivated("skill", "source")
}

func TestWorker_DualPool_YieldTrapFallback(t *testing.T) {
	t.Parallel()
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	sessID := "f1111111-2222-3333-4444-555555555555"
	sessDir := filepath.Join(tempData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0o644)

	var turnCount atomic.Int32
	primarySpawner := newScriptedDaemonSpawner(sessID,
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"waiting for bg task\",\"usage\":{\"total_tokens\":5}}}\n",
		"{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"all tasks done\",\"usage\":{\"total_tokens\":5}}}\n",
	)
	oldSpawn := primarySpawner.SpawnFn
	primarySpawner.SpawnFn = func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
		inW, outR, errR, handle, err := oldSpawn(ctx, cfg)
		return inW, outR, errR, handle, err
	}
	primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer primaryPool.Close()

	lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, newScriptedDaemonSpawner("d1111111-2222-3333-4444-555555555555"))
	defer lowPool.Close()

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
	threadID := "thread-yield-fallback"
	_ = store.SaveSessionID(context.Background(), threadID, sessID)

	taskDir := filepath.Join(tempData, "brain", "d1111111-2222-3333-4444-555555555555", ".system_generated", "tasks")
	_ = os.MkdirAll(taskDir, 0o755)
	_ = os.WriteFile(filepath.Join(taskDir, "bg-task-in-low-pool.log"), []byte("done"), 0o644)

	doneCh := make(chan struct{})
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
		SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1,
		OnMessageCompleted: func(msg db.Message, status string) {
			if status == db.StatusCompleted {
				turnCount.Add(1)
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
		},
	})
	defer pool.Stop()

	ctx := context.Background()
	if _, err := lowPool.GetOrCreate(ctx, threadID, ""); err != nil {
		t.Fatalf("failed to create low daemon: %v", err)
	}

	msg := db.Message{ID: "msg-yield-fb-1", ThreadID: threadID, Content: "run command and wait", Status: db.StatusPending, CreatedAt: time.Now()}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message completion")
	}
	if tc := int(turnCount.Load()); tc < 1 {
		t.Errorf("expected auto-resumption from yield-trap detection, turnCount=%d", tc)
	}
}

func TestWorker_QuotaPause_PreservesEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, effort, expectedEffort string
	}{
		{name: "LowEffort", effort: "low", expectedEffort: "low"},
		{name: "MediumEffort", effort: "medium", expectedEffort: "medium"},
		{name: "HighEffort", effort: "high", expectedEffort: "high"},
		{name: "DefaultEffort", effort: "", expectedEffort: "high"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			quotaErrSpawner := newScriptedDaemonSpawner("11111111-2222-3333-4444-555555555555",
				`{"event":"result","result":{"status":"ERROR","exit_code":1,"error":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.","stderr":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m."}}`+"\n",
			)
			primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, quotaErrSpawner)
			defer primaryPool.Close()
			lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, quotaErrSpawner)
			defer lowPool.Close()

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir, d.LowEffortModel = tempData, "gemini-3.8-flash-low" })
			doneCh := make(chan struct{})
			haroSrv, getOneShots := newMockHaroOneShotServer(t)
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool: primaryPool, LowEffortProcessPool: lowPool,
				SessionManager: session.New(tmpHome, tempData), Store: store, TimeoutMinutes: 1, MaxAttempts: 1,
				HaroURL: haroSrv.URL,
				OnMessageCompleted: func(msg db.Message, status string) {
					if status == db.StatusFailed {
						select {
						case <-doneCh:
						default:
							close(doneCh)
						}
					}
				},
			})
			defer pool.Stop()

			threadID := "thread-quota-" + tt.name
			msg := db.Message{ID: "msg-quota-" + tt.name, ThreadID: threadID, Content: "run task", Effort: tt.effort, Status: db.StatusPending, CreatedAt: time.Now()}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for message completion")
			}
			schedules := getOneShots(threadID)
			if len(schedules) != 1 || schedules[0].Effort != tt.expectedEffort {
				t.Fatalf("expected 1 schedule with effort %q, got %v", tt.expectedEffort, schedules)
			}
		})
	}
}

func TestYieldTrap_Scenarios(t *testing.T) {
	tests := []struct {
		name, threadID, sessionID, msgID, metricOutcome, metricSignature, expectDeliveredText, expectedSecondPrompt string
		maxYields                                                                                                   int
		expectInvocations                                                                                           int32
		verifyPromptsSeen                                                                                           bool
	}{
		{
			name: "auto-resumption on stderr signature success", threadID: "thread-yield-auto", sessionID: "f2222222-3333-4444-5555-666666666666", msgID: "msg-yield-1",
			maxYields: 1, expectInvocations: 2, expectDeliveredText: "Job completed after resumption!", metricOutcome: "resumed", metricSignature: "stderr_signature", verifyPromptsSeen: true, expectedSecondPrompt: YieldTrapResumePrompt,
		},
		{
			name: "circuit breaker trips after max resumes", threadID: "thread-yield-cb", sessionID: "f3333333-4444-5555-6666-777777777777", msgID: "msg-circuit-1",
			maxYields: 99, expectInvocations: 3, expectDeliveredText: "Still waiting turn 3", metricOutcome: "circuit_breaker", metricSignature: "stderr_signature",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, tmpHome := setupTestStore(t), t.TempDir()
			var runnerInvocations atomic.Int32
			var promptsSeenMu, deliveredMu sync.Mutex
			var promptsSeen []string
			var deliveredText string
			doneCh := make(chan struct{})

			sessMgr := session.New(tmpHome, "")
			sessDir, _ := ensureTestSessionDir(sessMgr, tc.sessionID)
			_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
			_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0o644)
			now := time.Now()
			_ = os.Chtimes(sessDir, now, now)

			pool := NewWorkerPool(WorkerPoolConfig{
				SessionManager: sessMgr, Store: store, TimeoutMinutes: 1, BackoffBase: 10 * time.Millisecond, MaxAttempts: 3,
				RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					inv := runnerInvocations.Add(1)
					promptsSeenMu.Lock()
					promptsSeen = append(promptsSeen, prompt)
					promptsSeenMu.Unlock()
					_ = os.Chtimes(sessDir, time.Now(), time.Now())
					if int(inv) <= tc.maxYields {
						msg := fmt.Sprintf("Still waiting turn %d", inv)
						if tc.maxYields == 1 {
							msg = "Waiting on background task task-444"
						}
						return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}`, tc.sessionID, msg), "terminating 1 background task(s) on exit\n", 0, nil
					}
					return fmt.Sprintf(`{"conversation_id":%q,"status":"SUCCESS","response":%q,"usage":{"input_tokens":150,"output_tokens":35,"total_tokens":185}}`, tc.sessionID, tc.expectDeliveredText), "", 0, nil
				},
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					deliveredMu.Lock()
					deliveredText = text
					deliveredMu.Unlock()
					return nil
				},
				TypingFunc:         func(s *discordgo.Session, channelID string) func() { return func() {} },
				OnMessageCompleted: func(msg db.Message, finalStatus string) { close(doneCh) },
			})
			pool.Start()
			defer pool.Stop()

			msg := db.Message{ID: tc.msgID, ThreadID: tc.threadID, GuildID: "guild-1", AuthorID: "user-1", AuthorName: "test-user", Content: "Run task", Status: db.StatusPending, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			_ = insertMessage(store, msg)
			model := pool.GetRuntimeConfig()
			initialMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))
			pool.Enqueue(msg)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for message processing")
			}

			if runnerInvocations.Load() != tc.expectInvocations {
				t.Fatalf("invocations = %d, want %d", runnerInvocations.Load(), tc.expectInvocations)
			}
			if tc.verifyPromptsSeen {
				promptsSeenMu.Lock()
				seenCount := len(promptsSeen)
				secondPrompt := ""
				if seenCount > 1 {
					secondPrompt = promptsSeen[1]
				}
				promptsSeenMu.Unlock()
				if seenCount != int(tc.expectInvocations) || secondPrompt != tc.expectedSecondPrompt {
					t.Errorf("prompts mismatch: count=%d second=%q", seenCount, secondPrompt)
				}
			}
			deliveredMu.Lock()
			finalText := deliveredText
			deliveredMu.Unlock()
			if finalText != tc.expectDeliveredText {
				t.Errorf("delivered = %q, want %q", finalText, tc.expectDeliveredText)
			}
			newMetric := testutil.ToFloat64(metrics.YieldTrapEventsTotal.WithLabelValues(tc.metricOutcome, tc.metricSignature, model))
			if newMetric != initialMetric+1 {
				t.Errorf("metric delta = %.0f, want 1", newMetric-initialMetric)
			}
		})
	}
}

func TestWorker_DeliverFailureNotice_And_QuotaPause_Silent_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		silent       bool
		wantDelivery bool
	}{
		{"standard burst triggers delivery", false, true},
		{"silent burst suppresses delivery", true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var deliveryCount int
			pool := &WorkerPool{
				cfg: WorkerPoolConfig{
					DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
						deliveryCount++
						return nil
					},
					NotifierFunc: func(agyBin, apiKey, desc string) string { return "notice: " + desc },
				},
			}
			var d *db.Dispatch
			if tc.silent {
				d = &db.Dispatch{ID: "d-silent", Source: "webhook"}
			}
			te := &turnExecution{
				pool:     pool,
				threadID: "1542423172400291873",
				dispatch: d,
				burst: []db.Message{
					{
						ID: "msg-1",
					},
				},
			}

			te.deliverFailureNotice("agy", "key", "fatal error", "stderr", "prefix", "test")
			if tc.wantDelivery && deliveryCount == 0 {
				t.Errorf("expected delivery for non-silent turn, got none")
			}
			if !tc.wantDelivery && deliveryCount > 0 {
				t.Errorf("expected zero delivery for silent turn, got %d", deliveryCount)
			}
		})
	}
}

func TestWorker_GlobalQuotaLockout_SilentAndDeliveryTarget_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		threadID       string
		channelID      string
		silent         bool
		wantDelivered  bool
		wantTargetChan string
	}{
		{
			name:           "silent turn hitting global quota lockout suppresses quota message",
			threadID:       "1542423172400291873",
			channelID:      "1542423172400291873",
			silent:         true,
			wantDelivered:  false,
			wantTargetChan: "",
		},
		{
			name:           "non-silent turn hitting global quota lockout delivers to deliveryTargetID snowflake",
			threadID:       "1542423172400291873",
			channelID:      "1542423172400291873",
			silent:         false,
			wantDelivered:  true,
			wantTargetChan: "1542423172400291873",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var deliveredMu sync.Mutex
			var deliveredTo string
			var deliveredText string

			store := dbtest.NewFakeStore()
			pool := New(config.NewFromData(&config.ConfigData{}), WorkerPoolConfig{
				Store: store,
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					deliveredMu.Lock()
					deliveredTo = channelID
					deliveredText = text
					deliveredMu.Unlock()
					return nil
				},
			})
			pool.quotaLockedUntil.Store(time.Now().Add(10 * time.Minute).Unix())

			msg := db.Message{
				ID:        "msg-quota-lockout",
				ThreadID:  tc.threadID,
				Content:   "execute prompt",
				Status:    db.StatusProcessing,
				ChannelID: tc.channelID,
			}
			_ = store.InsertMessage(context.Background(), msg)

			var d *db.Dispatch
			if tc.silent {
				d = &db.Dispatch{ID: "d-silent", Source: "webhook"}
			}

			te := &turnExecution{
				pool:        pool,
				threadID:    tc.threadID,
				triggerType: "http",
				dispatch:    d,
				burst:       []db.Message{msg},
				execStart:   time.Now().UTC(),
				turnPrompt:  "execute prompt",
				stopTyping:  func() {},
			}

			te.executeWithRetries()

			deliveredMu.Lock()
			wasDelivered := deliveredTo != ""
			gotTarget := deliveredTo
			deliveredMu.Unlock()

			if wasDelivered != tc.wantDelivered {
				t.Fatalf("wasDelivered = %v, want %v (text=%q)", wasDelivered, tc.wantDelivered, deliveredText)
			}
			if tc.wantDelivered && gotTarget != tc.wantTargetChan {
				t.Fatalf("deliveredTo = %q, want %q", gotTarget, tc.wantTargetChan)
			}
		})
	}
}

func TestWorker_DeliverSuccessAndFinalize_CustomSessionTarget_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		threadID       string
		channelID      string
		silent         bool
		wantDelivered  bool
		wantTargetChan string
	}{
		{
			name:           "custom session non-silent delivers to snowflake channelID",
			threadID:       "1542423172400291873",
			channelID:      "1542423172400291873",
			silent:         false,
			wantDelivered:  true,
			wantTargetChan: "1542423172400291873",
		},
		{
			name:           "custom session silent suppresses delivery",
			threadID:       "1542423172400291873",
			channelID:      "1542423172400291873",
			silent:         true,
			wantDelivered:  false,
			wantTargetChan: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var deliveredTo string
			store := dbtest.NewFakeStore()
			pool := New(config.NewFromData(&config.ConfigData{}), WorkerPoolConfig{
				Store: store,
				DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
					deliveredTo = channelID
					return nil
				},
			})

			msg := db.Message{
				ID:        "msg-succ",
				ThreadID:  tc.threadID,
				Status:    db.StatusProcessing,
				ChannelID: tc.channelID,
			}
			_ = store.InsertMessage(context.Background(), msg)

			var d *db.Dispatch
			if tc.silent {
				d = &db.Dispatch{ID: "d-silent", Source: "webhook"}
			}

			te := &turnExecution{
				pool:        pool,
				threadID:    tc.threadID,
				triggerType: "http",
				dispatch:    d,
				burst:       []db.Message{msg},
				execStart:   time.Now().UTC(),
				stopTyping:  func() {},
			}

			resp := &runner.AgyResponse{
				Response: "Task completed successfully",
			}
			ok, _ := te.deliverSuccessAndFinalize(resp, runner.TokenUsage{}, "test-model", 1, 1)
			if !ok {
				t.Fatalf("deliverSuccessAndFinalize returned false")
			}
			if tc.wantDelivered && deliveredTo != tc.wantTargetChan {
				t.Fatalf("deliveredTo = %q, want %q", deliveredTo, tc.wantTargetChan)
			}
			if !tc.wantDelivered && deliveredTo != "" {
				t.Fatalf("deliveredTo = %q, want empty", deliveredTo)
			}
		})
	}
}
