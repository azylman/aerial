package queue

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

type retryNotifyStore struct {
	db.Store

	onRetry func()
}

func (s *retryNotifyStore) IncrementMessageRetry(ctx context.Context, id, errMsg string) error {
	err := s.Store.IncrementMessageRetry(ctx, id, errMsg)
	if s.onRetry != nil {
		s.onRetry()
	}
	return err
}

type workerInterruptTestCase struct {
	name               string
	drainQueue         bool
	reset              bool
	firstAttemptErr    string
	enqueueSecondMsg   bool
	backoffBase        time.Duration
	wantMsg1Status     string
	wantMsg1Err        string
	wantMsg1Retries    int
	wantMsg1Attempts   int
	wantMsg2Status     string
	wantMsg2Err        string
	wantMsg2Resp       string
	wantSessionRotated bool
}

func assertInterruptThreadMessages(ctx context.Context, t *testing.T, tc workerInterruptTestCase, store db.Store, msg1ID, msg2ID, threadID, initialSessID string, msg1Attempts int32) {
	t.Helper()
	gotMsg1, err := store.GetMessage(ctx, msg1ID)
	if err != nil || gotMsg1 == nil {
		t.Fatalf("failed to get msg1: %v", err)
	}
	if gotMsg1.Status != tc.wantMsg1Status {
		t.Errorf("msg1 status = %q, want %q", gotMsg1.Status, tc.wantMsg1Status)
	}
	if gotMsg1.ErrorMessage != tc.wantMsg1Err {
		t.Errorf("msg1 error = %q, want %q", gotMsg1.ErrorMessage, tc.wantMsg1Err)
	}
	if gotMsg1.RetryCount != tc.wantMsg1Retries {
		t.Errorf("msg1 retry_count = %d, want %d", gotMsg1.RetryCount, tc.wantMsg1Retries)
	}
	if int(msg1Attempts) != tc.wantMsg1Attempts {
		t.Errorf("msg1 attempts = %d, want %d", msg1Attempts, tc.wantMsg1Attempts)
	}

	if tc.enqueueSecondMsg {
		gotMsg2, err := store.GetMessage(ctx, msg2ID)
		if err != nil || gotMsg2 == nil {
			t.Fatalf("failed to get msg2: %v", err)
		}
		if gotMsg2.Status != tc.wantMsg2Status {
			t.Errorf("msg2 status = %q, want %q", gotMsg2.Status, tc.wantMsg2Status)
		}
		if gotMsg2.ErrorMessage != tc.wantMsg2Err {
			t.Errorf("msg2 error = %q, want %q", gotMsg2.ErrorMessage, tc.wantMsg2Err)
		}
		if gotMsg2.ResponseText != tc.wantMsg2Resp {
			t.Errorf("msg2 response = %q, want %q", gotMsg2.ResponseText, tc.wantMsg2Resp)
		}
	}

	gotSess, _ := store.GetSessionID(ctx, threadID)
	if tc.wantSessionRotated {
		if gotSess == "" || gotSess == initialSessID {
			t.Errorf("expected session ID to rotate away from %q, got %q", initialSessID, gotSess)
		}
	} else if gotSess != initialSessID {
		t.Errorf("expected session ID %q to be preserved, got %q", initialSessID, gotSess)
	}
}

func makeInterruptTestSpawner(tc workerInterruptTestCase, initialSessID string, msg1Attempts, spawnCount *atomic.Int32, signalReady func()) runner.DaemonSpawner {
	return &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			n := spawnCount.Add(1)
			sessID := cfg.SessionID
			if sessID == "" {
				if n == 1 {
					sessID = initialSessID
				} else {
					sessID = fmt.Sprintf("sess-rotated-%d", n)
				}
			}
			go func() {
				defer outW.Close()
				defer errW.Close()
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID)
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.Contains(line, "first turn prompt") {
						att := msg1Attempts.Add(1)
						if tc.firstAttemptErr != "" && att == 1 {
							_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":%q,\"conversation_id\":%q}}\n", tc.firstAttemptErr, sessID)
							continue
						}
						if tc.firstAttemptErr == "" && att == 1 {
							signalReady()
							_, _ = io.Copy(io.Discard, inR)
							return
						}
					}
					_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"second turn ok\",\"conversation_id\":%q,\"usage\":{\"total_tokens\":5}}}\n", sessID)
				}
			}()
			return inW, outR, errR, newMockProcessHandle(34567), nil
		},
	}
}

func runWorkerInterruptCase(t *testing.T, tc workerInterruptTestCase) {
	t.Helper()
	baseStore := setupTestStore(t)
	tmpHome, tempData := t.TempDir(), t.TempDir()
	initialSessID := "11111111-2222-3333-4444-555555555555"
	threadID := "thread-interrupt-" + strings.ToLower(tc.name)

	interruptReadyCh := make(chan struct{})
	var readyOnce sync.Once
	signalReady := func() {
		readyOnce.Do(func() { close(interruptReadyCh) })
	}

	store := &retryNotifyStore{
		Store: baseStore,
		onRetry: func() {
			if tc.firstAttemptErr != "" {
				signalReady()
			}
		},
	}
	_ = store.SaveSessionID(context.Background(), threadID, initialSessID)

	var msg1Attempts atomic.Int32
	var spawnCount atomic.Int32
	spawner := makeInterruptTestSpawner(tc, initialSessID, &msg1Attempts, &spawnCount, signalReady)

	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, spawner)
	defer procPool.Close()

	expectedCompletions := 1
	if tc.enqueueSecondMsg {
		expectedCompletions = 2
	}
	var completedCount atomic.Int32
	doneCh := make(chan struct{})

	cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData })
	pool := New(cfg, WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		MaxAttempts:    3,
		BackoffBase:    tc.backoffBase,
		OnMessageCompleted: func(msg db.Message, status string) {
			if int(completedCount.Add(1)) >= expectedCompletions {
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
	msg1 := db.Message{ID: "msg-1-" + tc.name, ThreadID: threadID, Content: "first turn prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
	_ = store.InsertMessage(ctx, msg1)
	pool.Enqueue(msg1)

	select {
	case <-interruptReadyCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for turn 1 to reach interrupt point")
	}

	msg2ID := "msg-2-" + tc.name
	if tc.enqueueSecondMsg {
		msg2 := db.Message{ID: msg2ID, ThreadID: threadID, Content: "second turn prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
		_ = store.InsertMessage(ctx, msg2)
		pool.Enqueue(msg2)
	}
	if tc.drainQueue {
		_ = store.InsertMessage(ctx, db.Message{ID: "msg-db-pending-" + tc.name, ThreadID: threadID, Content: "unclaimed pending", Status: db.StatusPending, CreatedAt: time.Now().UTC()})
	}

	pool.InterruptThread(ctx, threadID, tc.drainQueue, tc.reset)

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for messages to finalize after InterruptThread")
	}

	assertInterruptThreadMessages(ctx, t, tc, store, msg1.ID, msg2ID, threadID, initialSessID, msg1Attempts.Load())
}

func TestWorkerPool_InterruptThread(t *testing.T) {
	t.Parallel()
	tests := []workerInterruptTestCase{
		{
			name:               "ActiveTurn_DrainQueue_SuppressesRetryAndPreservesSession",
			drainQueue:         true,
			reset:              false,
			firstAttemptErr:    "",
			enqueueSecondMsg:   true,
			backoffBase:        20 * time.Millisecond,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    0,
			wantMsg1Attempts:   1,
			wantMsg2Status:     db.StatusFailed,
			wantMsg2Err:        InterruptedByOperatorReason,
			wantSessionRotated: false,
		},
		{
			name:               "DuringRetryBackoff_WithReset_AbortsSleepAndRotatesSession",
			drainQueue:         true,
			reset:              true,
			firstAttemptErr:    "503 service unavailable",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: true,
		},
		{
			name:               "DuringRetryBackoffNoReset",
			drainQueue:         false,
			reset:              false,
			firstAttemptErr:    "503 service unavailable",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: false,
		},
		{
			name:               "DuringCapacityBlipBackoff_AbortsSleepAndFails",
			drainQueue:         true,
			reset:              false,
			firstAttemptErr:    "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.",
			enqueueSecondMsg:   false,
			backoffBase:        30 * time.Second,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    1,
			wantMsg1Attempts:   1,
			wantSessionRotated: false,
		},
		{
			name:               "ActiveTurn_NoDrain_AbortsActiveTurnAndRunsQueuedMessage",
			drainQueue:         false,
			reset:              false,
			firstAttemptErr:    "",
			enqueueSecondMsg:   true,
			backoffBase:        20 * time.Millisecond,
			wantMsg1Status:     db.StatusFailed,
			wantMsg1Err:        InterruptedByOperatorReason,
			wantMsg1Retries:    0,
			wantMsg1Attempts:   1,
			wantMsg2Status:     db.StatusCompleted,
			wantMsg2Resp:       "second turn ok",
			wantSessionRotated: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWorkerInterruptCase(t, tc)
		})
	}
}

func TestTurnExecution_IsDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want bool
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: false,
		},
		{
			name: "turn without dispatch",
			te:   &turnExecution{threadID: "123"},
			want: false,
		},
		{
			name: "turn with dispatch",
			te:   &turnExecution{threadID: "123", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.isDispatch(); got != tc.want {
				t.Errorf("isDispatch() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_LeaseKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want string
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: "",
		},
		{
			name: "dispatch turn uses threadID as lease key",
			te:   &turnExecution{threadID: "task-dispatch-1", dispatch: &db.Dispatch{ID: "disp-1"}},
			want: "task-dispatch-1",
		},
		{
			name: "discord turn uses discord prefix",
			te:   &turnExecution{threadID: "123456789"},
			want: "discord-123456789",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.leaseKey(); got != tc.want {
				t.Errorf("leaseKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_InitialRetryCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		te   *turnExecution
		want int
	}{
		{
			name: "nil receiver",
			te:   nil,
			want: 0,
		},
		{
			name: "dispatch turn uses dispatch retry count",
			te:   &turnExecution{dispatch: &db.Dispatch{ID: "d1", RetryCount: 3}},
			want: 3,
		},
		{
			name: "burst turn uses max burst retry count",
			te: &turnExecution{burst: []db.Message{
				{ID: "m1", RetryCount: 1},
				{ID: "m2", RetryCount: 4},
				{ID: "m3", RetryCount: 2},
			}},
			want: 4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.initialRetryCount(); got != tc.want {
				t.Errorf("initialRetryCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_TurnEffortAndModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		te         *turnExecution
		wantEffort string
		wantModel  string
	}{
		{
			name:       "nil receiver",
			te:         nil,
			wantEffort: "",
			wantModel:  "",
		},
		{
			name: "dispatch turn with explicit model and effort",
			te: &turnExecution{
				dispatch: &db.Dispatch{ID: "d1", Model: "gemini-custom", Effort: "high", SessionID: "sess-1"},
			},
			wantEffort: "high",
			wantModel:  "gemini-custom",
		},
		{
			name: "dispatch turn ephemeral defaults to medium effort",
			te: &turnExecution{
				isEphemeral: true,
				dispatch:    &db.Dispatch{ID: "d2"},
			},
			wantEffort: "medium",
			wantModel:  "",
		},
		{
			name: "burst turn with effort and model in messages",
			te: &turnExecution{
				burst: []db.Message{
					{ID: "m1", Model: "model-alpha", Effort: "medium"},
				},
			},
			wantEffort: "medium",
			wantModel:  "model-alpha",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotEffort, gotModel := tc.te.turnEffortAndModel()
			if gotEffort != tc.wantEffort || gotModel != tc.wantModel {
				t.Errorf("turnEffortAndModel() = (%q, %q), want (%q, %q)", gotEffort, gotModel, tc.wantEffort, tc.wantModel)
			}
		})
	}
}

func TestTurnExecution_OwnsTurnItem(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		te     *turnExecution
		itemID string
		want   bool
	}{
		{
			name:   "nil receiver",
			te:     nil,
			itemID: "item-1",
			want:   false,
		},
		{
			name:   "dispatch matching id",
			te:     &turnExecution{dispatch: &db.Dispatch{ID: "d-123"}},
			itemID: "d-123",
			want:   true,
		},
		{
			name:   "dispatch mismatching id",
			te:     &turnExecution{dispatch: &db.Dispatch{ID: "d-123"}},
			itemID: "d-999",
			want:   false,
		},
		{
			name:   "burst message matching id",
			te:     &turnExecution{burst: []db.Message{{ID: "m-100"}, {ID: "m-101"}}},
			itemID: "m-101",
			want:   true,
		},
		{
			name:   "burst message mismatching id",
			te:     &turnExecution{burst: []db.Message{{ID: "m-100"}}},
			itemID: "m-200",
			want:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.te.ownsTurnItem(tc.itemID); got != tc.want {
				t.Errorf("ownsTurnItem(%q) = %t, want %t", tc.itemID, got, tc.want)
			}
		})
	}
}

func TestTurnExecution_IncrementTurnRetries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		isDispatch    bool
		isNil         bool
		wantDispRetry int
		wantMsgRetry  int
	}{
		{
			name:       "nil receiver",
			isNil:      true,
			isDispatch: false,
		},
		{
			name:          "dispatch turn increments dispatch retry in store",
			isDispatch:    true,
			wantDispRetry: 1,
		},
		{
			name:         "burst turn increments message retry in store",
			isDispatch:   false,
			wantMsgRetry: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.isNil {
				var nilTE *turnExecution
				nilTE.incrementTurnRetries("nil error")
				return
			}
			store := dbtest.NewFakeStore()
			ctx := context.Background()
			pool := New(config.NewFromData(&config.ConfigData{}), WorkerPoolConfig{Store: store})
			te := &turnExecution{pool: pool}
			if tc.isDispatch {
				d := db.Dispatch{ID: "d-retry-1", Status: db.StatusProcessing}
				_, _ = store.InsertDispatch(ctx, d)
				te.dispatch = &d
			} else {
				m := db.Message{ID: "m-retry-1", ThreadID: "t-1", Status: db.StatusProcessing}
				_ = store.InsertMessage(ctx, m)
				te.burst = []db.Message{m}
			}
			te.incrementTurnRetries("timeout error")
			if tc.isDispatch {
				gotD, _ := store.GetDispatch(ctx, "d-retry-1")
				if gotD != nil && gotD.RetryCount != tc.wantDispRetry {
					t.Errorf("dispatch retry count = %d, want %d", gotD.RetryCount, tc.wantDispRetry)
				}
			} else {
				gotM, _ := store.GetMessage(ctx, "m-retry-1")
				if gotM != nil && gotM.RetryCount != tc.wantMsgRetry {
					t.Errorf("message retry count = %d, want %d", gotM.RetryCount, tc.wantMsgRetry)
				}
			}
		})
	}
}

func TestResolveBurstModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		burst []db.Message
		want  string
	}{
		{
			name:  "empty burst",
			burst: nil,
			want:  "",
		},
		{
			name: "first message with model",
			burst: []db.Message{
				{ID: "1", Model: "gemini-pro"},
				{ID: "2", Model: "gemini-flash"},
			},
			want: "gemini-pro",
		},
		{
			name: "second message has model",
			burst: []db.Message{
				{ID: "1"},
				{ID: "2", Model: " gemini-flash "},
			},
			want: "gemini-flash",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveBurstModel(tc.burst); got != tc.want {
				t.Errorf("resolveBurstModel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTurnExecution_PrepareAttemptPrompt(t *testing.T) {
	t.Parallel()
	sessUUID := "44444444-4444-4444-4444-444444444444"
	origPrompt := "Execute task"
	contPrompt := fmt.Sprintf(ContinuationPromptTemplate, origPrompt)

	tests := []struct {
		name          string
		attempt       int
		promptToSend  string
		lastErrDetail string
		lastStderr    string
		hasSessionID  bool
		hasOnDiskSess bool
		isDispatch    bool
		restartCount  int
		errorMessage  string
		burst         []db.Message
		wantPrompt    string
	}{
		{
			name:         "empty prompt defaults to turn prompt",
			attempt:      1,
			promptToSend: "",
			wantPrompt:   origPrompt,
		},
		{
			name:         "first attempt normal dispatch returns original prompt",
			attempt:      1,
			promptToSend: origPrompt,
			isDispatch:   true,
			wantPrompt:   origPrompt,
		},
		{
			name:          "attempt 2 with context deadline exceeded and on-disk session returns continuation template",
			attempt:       2,
			promptToSend:  origPrompt,
			lastErrDetail: "context deadline exceeded",
			hasSessionID:  true,
			hasOnDiskSess: true,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted discord burst attempt 1 with on-disk session returns continuation template",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: true,
			isDispatch:    false,
			burst:         []db.Message{{ID: "m1", RestartCount: 1}},
			wantPrompt:    contPrompt,
		},
		{
			name:          "restarted dispatch attempt 1 without on-disk session returns original prompt",
			attempt:       1,
			promptToSend:  origPrompt,
			hasSessionID:  true,
			hasOnDiskSess: false,
			isDispatch:    true,
			restartCount:  1,
			wantPrompt:    origPrompt,
		},
		{
			name:         "custom prompt to send preserved without modification",
			attempt:      1,
			promptToSend: YieldTrapResumePrompt,
			isDispatch:   true,
			restartCount: 1,
			wantPrompt:   YieldTrapResumePrompt,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpHome := t.TempDir()
			tempData := t.TempDir()
			mgr := session.New(tmpHome, tempData)
			if tc.hasOnDiskSess {
				sessDir, _ := ensureTestSessionDir(mgr, sessUUID)
				_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
				_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte("{\"step_index\":0}\n"), 0o644)
			}

			pool := &WorkerPool{sessionMgr: mgr}
			te := &turnExecution{
				pool:       pool,
				turnPrompt: origPrompt,
				burst:      tc.burst,
			}
			if tc.hasSessionID {
				te.currentSessionID = sessUUID
			}
			if tc.isDispatch {
				te.dispatch = &db.Dispatch{
					ID:           "disp-test-1",
					RestartCount: tc.restartCount,
					ErrorMessage: tc.errorMessage,
				}
			}

			got := te.prepareAttemptPrompt(tc.attempt, tc.promptToSend, tc.lastErrDetail, tc.lastStderr)
			if got != tc.wantPrompt {
				t.Errorf("prepareAttemptPrompt() = %q, want %q", got, tc.wantPrompt)
			}
		})
	}
}

type statefulEvictionTestCase struct {
	name                   string
	sessionID              string
	restartCount           int
	hasExistingTranscript  bool
	yieldTrap              bool
	wantStatus             string
	wantEvictedFromPool    bool
	wantSessionInStore     bool
	wantContinuationPrompt bool
	wantYieldTrapPrompt    bool
}

func assertStatefulEvictionResults(ctx context.Context, t *testing.T, tc statefulEvictionTestCase, store db.Store, dispID, sessUUID string, pool *WorkerPool, promptsSeen []string) {
	t.Helper()
	d, err := store.GetDispatch(ctx, dispID)
	if err != nil || d == nil {
		t.Fatalf("failed to fetch dispatch: %v", err)
	}
	if d.Status != tc.wantStatus {
		t.Fatalf("dispatch status = %s, want %s", d.Status, tc.wantStatus)
	}

	if tc.wantSessionInStore {
		gotSess, _ := store.GetSessionID(ctx, tc.sessionID)
		if gotSess != sessUUID {
			t.Errorf("store session ID = %q, want %q", gotSess, sessUUID)
		}
	}

	if tc.wantEvictedFromPool {
		if testPool, ok := pool.AgentPool().(*testRunnerAgentPool); ok {
			if testPool.HasSession(tc.sessionID) {
				t.Errorf("expected session %q to be evicted from active pool", tc.sessionID)
			}
		}
	}

	if tc.wantContinuationPrompt {
		if len(promptsSeen) == 0 || !strings.Contains(promptsSeen[0], "Your previous execution timed out or was interrupted while working") {
			t.Errorf("first attempt prompt does not contain continuation template, got:\n%s", promptsSeen[0])
		}
	}

	if tc.wantYieldTrapPrompt {
		if len(promptsSeen) < 2 || promptsSeen[1] != YieldTrapResumePrompt {
			t.Errorf("second attempt prompt = %q, want %q", promptsSeen[1], YieldTrapResumePrompt)
		}
	}
}

func runStatefulEvictionCase(t *testing.T, tc statefulEvictionTestCase) {
	t.Helper()
	store := dbtest.NewFakeStore()
	ctx := context.Background()
	tmpHome := t.TempDir()
	tempData := t.TempDir()
	sessMgr := session.New(tmpHome, tempData)

	sessUUID := "33333333-3333-3333-3333-333333333333"
	if tc.hasExistingTranscript {
		sessDir, _ := ensureTestSessionDir(sessMgr, sessUUID)
		_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs"), 0o755)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(`{"step_index":0}
`), 0o644)
		_ = store.SaveSessionID(ctx, tc.sessionID, sessUUID)
	}

	var mu sync.Mutex
	var promptsSeen []string
	var invCount atomic.Int32
	doneCh := make(chan struct{})

	origPrompt := "[VANADIS BUILD CHUNK EXECUTION] Chunk execution test"

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		SessionManager: sessMgr,
		TimeoutMinutes: 1,
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			mu.Lock()
			promptsSeen = append(promptsSeen, prompt)
			mu.Unlock()

			inv := invCount.Add(1)
			if tc.yieldTrap && inv == 1 {
				return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"Waiting for task to complete"}`, "terminating 1 background task(s)", 0, nil
			}
			defer close(doneCh)
			return `{"conversation_id":"` + sessUUID + `","status":"SUCCESS","response":"chunk executed successfully"}`, "", 0, nil
		},
	})
	defer pool.Stop()

	disp := db.Dispatch{
		ID:           uuid.New().String(),
		Source:       "vanadis",
		SessionID:    tc.sessionID,
		Prompt:       origPrompt,
		RestartCount: tc.restartCount,
		Status:       db.StatusPending,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_, _ = store.InsertDispatch(ctx, disp)
	pool.EnqueueDispatch(disp)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for dispatch execution")
	}

	waitPoolIdle(t, pool, 3*time.Second)
	pool.Stop()

	mu.Lock()
	seen := append([]string(nil), promptsSeen...)
	mu.Unlock()

	assertStatefulEvictionResults(ctx, t, tc, store, disp.ID, sessUUID, pool, seen)
}

func TestWorker_ProcessDispatch_StatefulEvictionAndRestartContinuation(t *testing.T) {
	t.Parallel()
	tests := []statefulEvictionTestCase{
		{
			name:                   "stateful dispatch evicts active pool session while preserving conversation in store",
			sessionID:              "vanadis-chunk-1",
			restartCount:           0,
			hasExistingTranscript:  false,
			yieldTrap:              false,
			wantStatus:             db.StatusCompleted,
			wantEvictedFromPool:    true,
			wantSessionInStore:     true,
			wantContinuationPrompt: false,
			wantYieldTrapPrompt:    false,
		},
		{
			name:                   "restarted stateful dispatch sends continuation prompt and transitions to yield trap resume",
			sessionID:              "vanadis-chunk-2",
			restartCount:           1,
			hasExistingTranscript:  true,
			yieldTrap:              true,
			wantStatus:             db.StatusCompleted,
			wantEvictedFromPool:    true,
			wantSessionInStore:     true,
			wantContinuationPrompt: true,
			wantYieldTrapPrompt:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStatefulEvictionCase(t, tc)
		})
	}
}
