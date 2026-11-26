package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestUnifiedProcessPool_SessionPropagation_Suite(t *testing.T) {
	tests := []struct {
		name, inputSessID, secondSessID, expectCapturedSess, expectSessID string
		initialTurns, expectSpawns                                        int
		expectSecondDiff                                                  bool
	}{
		{name: "PropagatesCustomSessionID", inputSessID: "11111111-2222-3333-4444-555555555555", secondSessID: "11111111-2222-3333-4444-555555555555", expectCapturedSess: "11111111-2222-3333-4444-555555555555", expectSessID: "11111111-2222-3333-4444-555555555555", expectSpawns: 1},
		{name: "FreshSessionOnEmptyID", inputSessID: "", expectCapturedSess: "", expectSessID: "00000000-0000-0000-0000-000000000001", expectSpawns: 1},
		{name: "MismatchedSessionIDRotates", inputSessID: "11111111-1111-1111-1111-111111111111", secondSessID: "22222222-2222-2222-2222-222222222222", expectCapturedSess: "11111111-1111-1111-1111-111111111111", expectSessID: "11111111-1111-1111-1111-111111111111", expectSecondDiff: true, expectSpawns: 2},
		{name: "PropagatesInitialTurnCount", inputSessID: "44444444-4444-4444-4444-444444444444", initialTurns: 5, expectCapturedSess: "44444444-4444-4444-4444-444444444444", expectSessID: "44444444-4444-4444-4444-444444444444", expectSpawns: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedCfg DaemonConfig
			var spawnCount atomic.Int32
			mock := newPipeMockSpawner(pipeMockSpawnerOpts{
				onSpawn: func(cfg DaemonConfig) {
					spawnCount.Add(1)
					capturedCfg = cfg
				},
			})
			pool := NewUnifiedProcessPool(PoolConfig{}, mock)
			defer pool.Close()

			ctx := context.Background()
			var daemon *StreamingDaemon
			var err error
			if tc.initialTurns > 0 {
				daemon, err = pool.GetOrCreate(ctx, "target-prop", tc.inputSessID, tc.initialTurns)
			} else {
				daemon, err = pool.GetOrCreate(ctx, "target-prop", tc.inputSessID)
			}
			if err != nil {
				t.Fatalf("GetOrCreate failed: %v", err)
			}

			if capturedCfg.SessionID != tc.expectCapturedSess || daemon.SessionID() != tc.expectSessID {
				t.Errorf("session ID check failed: captured=%q daemon=%q", capturedCfg.SessionID, daemon.SessionID())
			}
			if tc.initialTurns > 0 && daemon.TurnCount() != tc.initialTurns {
				t.Errorf("daemon TurnCount expected %d, got %d", tc.initialTurns, daemon.TurnCount())
			}
			if tc.secondSessID != "" {
				d2, err2 := pool.GetOrCreate(ctx, "target-prop", tc.secondSessID)
				if err2 != nil || (tc.expectSecondDiff && d2 == daemon) || (!tc.expectSecondDiff && d2 != daemon) {
					t.Fatalf("second call mismatch: d2=%p daemon=%p err=%v", d2, daemon, err2)
				}
			}
			if int(spawnCount.Load()) != tc.expectSpawns {
				t.Errorf("expected %d total spawns, got %d", tc.expectSpawns, spawnCount.Load())
			}
		})
	}
}

type acquireLeaseSuiteCase struct {
	name, customExistingSessID, respPayload, respErr, respStatus, expectPrevSess, expectSubstantive string
	existingTurnCount, maxTurns, expectTurnCount                                                    int
	onRotatedErr                                                                                    error
	twoAttemptsThrottle, expectCold                                                                 bool
	expectExecErr, expectRotated                                                                    bool
}

func executeLeaseTurn(ctx context.Context, t *testing.T, lease SessionLease, tc acquireLeaseSuiteCase) (*TurnResult, error) {
	t.Helper()
	turnCtx := &TurnContext{TurnID: "turn-1", Prompt: "prompt-1", Ctx: ctx}
	res, execErr := lease.Execute(ctx, turnCtx)
	if tc.twoAttemptsThrottle {
		if execErr == nil {
			t.Fatal("expected attempt 1 to fail on throttle")
		}
		turnCtx2 := &TurnContext{TurnID: "turn-1-retry", Prompt: "attempt 2", Ctx: ctx}
		return lease.Execute(ctx, turnCtx2)
	}
	return res, execErr
}

func assertLeaseExecutionState(t *testing.T, res *TurnResult, execErr error, lease SessionLease, tc acquireLeaseSuiteCase) {
	t.Helper()
	if tc.expectExecErr {
		if execErr == nil {
			t.Fatalf("expected Execute error, got nil")
		}
	} else if execErr != nil {
		t.Fatalf("unexpected Execute error: %v", execErr)
	}
	if tc.expectSubstantive != "" && (res == nil || !strings.Contains(res.Response, tc.expectSubstantive)) {
		t.Errorf("expected response containing %q, got: %+v", tc.expectSubstantive, res)
	}
	if tc.expectCold && lease.IsCold() {
		t.Errorf("expected IsCold to be false after execution")
	}
	if tc.expectTurnCount > 0 && lease.TurnCount() != tc.expectTurnCount {
		t.Errorf("expected TurnCount %d, got %d", tc.expectTurnCount, lease.TurnCount())
	}
}

func assertLeaseReleaseAndRotation(t *testing.T, pool *UnifiedProcessPool, lease SessionLease, targetKey, pinnedSessID string, rotatedOld, rotatedNew *string, rotationCalled *bool, tc acquireLeaseSuiteCase) {
	t.Helper()
	relErr := lease.Release()
	waitPoolBackground(pool)
	if tc.onRotatedErr != nil {
		if relErr != nil {
			t.Fatalf("expected nil Release error, got: %v", relErr)
		}
		if poolHasDaemon(pool, targetKey) {
			t.Errorf("expected daemon evicted")
		}
		return
	}
	if relErr != nil {
		t.Fatalf("unexpected Release error: %v", relErr)
	}

	if tc.expectRotated {
		if !*rotationCalled || *rotatedOld != pinnedSessID || *rotatedNew == "" || *rotatedNew == *rotatedOld {
			t.Errorf("expected valid rotation: called=%v old=%q new=%q", *rotationCalled, *rotatedOld, *rotatedNew)
		}
	} else if *rotationCalled {
		t.Errorf("expected rotation NOT to be called")
	}
}

func runAcquireLeaseSuiteCase(t *testing.T, tc acquireLeaseSuiteCase) {
	t.Helper()
	ctx := context.Background()

	var rotationCalled bool
	var rotatedOld, rotatedNew string
	pinnedSessID := tc.customExistingSessID
	if pinnedSessID == "" {
		pinnedSessID = uuid.New().String()
	}

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:        350,
		initSessID: pinnedSessID,
		onUser: func(attempt int, line string) (string, string, string) {
			if tc.twoAttemptsThrottle {
				if attempt == 1 {
					return "", "RESOURCE_EXHAUSTED: quota exceeded. Resets in 5s.", "ERROR"
				}
				return "Retry succeeded with substantive output.", "", "DONE"
			}
			return tc.respPayload, tc.respErr, tc.respStatus
		},
	})

	poolCfg := PoolConfig{
		MaxSessionTurns: tc.maxTurns,
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{ActiveSessionID: pinnedSessID, PreviousSessionID: tc.expectPrevSess, TurnCount: tc.existingTurnCount}, nil
		},
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			rotationCalled = true
			rotatedOld, rotatedNew = oldSessionID, newSessionID
			return tc.onRotatedErr
		},
	}

	pool := NewUnifiedProcessPool(poolCfg, mock)
	defer pool.Close()

	targetKey := "thread-lease-" + uuid.New().String()
	lease, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("unexpected AcquireLease error: %v", err)
	}
	if tc.expectCold && !lease.IsCold() {
		t.Errorf("expected IsCold before execution to be true")
	}
	if tc.expectPrevSess != "" && lease.PreviousSessionID() != tc.expectPrevSess {
		t.Errorf("expected prev session %q, got %q", tc.expectPrevSess, lease.PreviousSessionID())
	}
	if lease.SessionID() != pinnedSessID {
		t.Errorf("expected session %q, got %q", pinnedSessID, lease.SessionID())
	}
	if rotationCalled {
		t.Errorf("rotation must NOT be called before release")
	}

	res, execErr := executeLeaseTurn(ctx, t, lease, tc)
	assertLeaseExecutionState(t, res, execErr, lease, tc)
	assertLeaseReleaseAndRotation(t, pool, lease, targetKey, pinnedSessID, &rotatedOld, &rotatedNew, &rotationCalled, tc)
}

func TestUnifiedProcessPool_AcquireLease_ValidationAndLocking(t *testing.T) {
	ctx := context.Background()

	t.Run("Validation_NilPool", func(t *testing.T) {
		var nilPool *UnifiedProcessPool
		if _, err := nilPool.AcquireLease(ctx, "target"); err == nil {
			t.Error("expected error from nil pool AcquireLease")
		}
	})

	t.Run("Validation_EmptyTarget", func(t *testing.T) {
		pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
		defer pool.Close()
		if _, err := pool.AcquireLease(ctx, "   "); err == nil {
			t.Error("expected error from empty targetKey AcquireLease")
		}
	})

	t.Run("ConcurrencySerialization", func(t *testing.T) {
		pinnedSessID := uuid.New().String()
		mock := newPipeMockSpawner(pipeMockSpawnerOpts{
			pid:        350,
			initSessID: pinnedSessID,
			onUser: func(attempt int, line string) (string, string, string) {
				return "ok", "", ""
			},
		})

		poolCfg := PoolConfig{
			TargetLockTimeout: 200 * time.Millisecond,
			GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
				return SessionRecord{ActiveSessionID: pinnedSessID}, nil
			},
		}

		pool := NewUnifiedProcessPool(poolCfg, mock)
		defer pool.Close()

		targetKey := "thread-lease-" + uuid.New().String()
		lease, err := pool.AcquireLease(ctx, targetKey)
		if err != nil {
			t.Fatalf("unexpected AcquireLease error: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if _, cErr := pool.AcquireLease(ctxTimeout, targetKey); cErr == nil {
			t.Fatalf("expected concurrent AcquireLease timeout")
		}

		turnCtx := &TurnContext{TurnID: "turn-1", Prompt: "prompt-1", Ctx: ctx}
		if _, execErr := lease.Execute(ctx, turnCtx); execErr != nil {
			t.Fatalf("unexpected Execute error: %v", execErr)
		}

		if err := lease.Release(); err != nil {
			t.Fatalf("unexpected Release error: %v", err)
		}

		lease3, err3 := pool.AcquireLease(ctx, targetKey)
		if err3 != nil {
			t.Fatalf("lease3 error: %v", err3)
		}
		if err := lease3.Release(); err != nil {
			t.Fatalf("lease3 release error: %v", err)
		}
	})
}

func TestUnifiedProcessPool_AcquireLease_Suite(t *testing.T) {
	tests := []acquireLeaseSuiteCase{
		{name: "BasicExecution", customExistingSessID: "550e8400-e29b-41d4-a716-446655440001", expectPrevSess: "550e8400-e29b-41d4-a716-446655440000", respPayload: "lease reply", expectTurnCount: 1, expectSubstantive: "lease reply", expectCold: true},
		{name: "RotationAndPersistence", maxTurns: 1, respPayload: "ok", expectRotated: true},
		{name: "PersistenceFailureRollback", maxTurns: 1, respPayload: "ok", onRotatedErr: errors.New("simulated DB latch timeout")},
		{name: "CancelRotationOnQuotaExhausted", maxTurns: 1, respErr: "RESOURCE_EXHAUSTED: quota exceeded. Resets in 30s.", respStatus: "ERROR", expectExecErr: true, expectRotated: false},
		{name: "RotatesWhenResponseSubstantiveDespiteStderrThrottle", maxTurns: 1, respPayload: "Task completed successfully and verified.", respErr: "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", respStatus: "DONE", expectRotated: true, expectSubstantive: "Task completed successfully and verified."},
		{name: "ClearsCancelRotationOnSuccessfulRetry", maxTurns: 1, twoAttemptsThrottle: true, expectRotated: true, expectSubstantive: "Retry succeeded with substantive output."},
		{name: "ConversationalRateLimitTextDoesNotCancelRotation", maxTurns: 1, respPayload: "I hit an external rate limit reached earlier, but I cached the result.", respStatus: "DONE", expectRotated: true, expectSubstantive: "rate limit reached"},
		{name: "EmptyResponse_NonQuota_MarksDirty", maxTurns: 1, respPayload: "", respStatus: "DONE", expectRotated: true},
		{name: "EmptyResponse_Quota_CancelsRotation", maxTurns: 1, respPayload: "", respErr: "RESOURCE_EXHAUSTED: quota exceeded. Resets in 30s.", respStatus: "DONE", expectRotated: false},
		{name: "PropagatesInitialTurnCountAndRotates", existingTurnCount: DefaultMaxSessionTurns - 1, respPayload: "ok", expectRotated: true, expectTurnCount: DefaultMaxSessionTurns},
		{name: "DefersRotationToTurnEndEvenIfExceeded", existingTurnCount: DefaultMaxSessionTurns, respPayload: "ok", expectRotated: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAcquireLeaseSuiteCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_LeaseTurnSinkWrapper_AllMethods(t *testing.T) {
	inner := newMockTurnSink()
	buf := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	wrapper := &leaseTurnSinkWrapper{inner: inner, buf: buf}

	wrapper.OnTurnStarted()
	wrapper.OnThinking()
	wrapper.OnStepStarted(1)
	wrapper.OnToolCall(ToolCall{Name: "bash", Command: "ls"})
	wrapper.OnToolCompleted(ToolCall{Name: "bash"}, "native", 10*time.Millisecond, "ok")
	wrapper.OnSkillActivated("self-improvement", "discord")
	wrapper.OnTextDelta("delta")
	res := &TurnResult{Response: "ok"}
	wrapper.OnResult(res)
	testErr := errors.New("test error")
	wrapper.OnError(testErr)

	if !inner.started || inner.thinking != 1 {
		t.Errorf("expected inner.started and inner.thinking == 1")
	}

	nilWrapper := &leaseTurnSinkWrapper{inner: nil, buf: NewBufferingTurnSink(BufferingTurnSinkConfig{})}
	nilWrapper.OnTurnStarted()
	nilWrapper.OnThinking()
	nilWrapper.OnStepStarted(1)
	nilWrapper.OnToolCall(ToolCall{Name: "bash", Command: "ls"})
	nilWrapper.OnToolCompleted(ToolCall{Name: "bash"}, "native", 10*time.Millisecond, "ok")
	nilWrapper.OnSkillActivated("self-improvement", "discord")
	nilWrapper.OnTextDelta("delta")
	nilWrapper.OnResult(res)
	nilWrapper.OnError(testErr)
}

func TestUnifiedProcessPool_SetSessionCallbacks(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	defer pool.Close()
	getRec := func(ctx context.Context, targetKey string) (SessionRecord, error) {
		return SessionRecord{ActiveSessionID: "s1"}, nil
	}
	onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error { return nil }
	pool.SetSessionCallbacks(getRec, onRot)
	if pool.cfg.GetSessionRecord == nil || pool.cfg.OnSessionRotated == nil {
		t.Fatalf("expected callbacks to be registered on pool")
	}
	var nilPool *UnifiedProcessPool
	nilPool.SetSessionCallbacks(getRec, onRot)
}

func verifyThrowawayTurnSink(t *testing.T) {
	t.Helper()
	throwaway := NewThrowawayTurnSink()
	throwaway.OnTurnStarted()
	throwaway.OnThinking()
	throwaway.OnToolCall(ToolCall{Name: "tool", Command: "cmd"})
	throwaway.OnToolCompleted(ToolCall{Name: "tool"}, "srv", 0, "ok")
	throwaway.OnSkillActivated("skill", "src")
	throwaway.OnTextDelta("delta")
	throwaway.OnResult(&TurnResult{Response: "ok"})
	throwaway.OnError(errors.New("err"))
}

func verifyHandshakeRetryPool(ctx context.Context, t *testing.T, pinnedSessID string) {
	t.Helper()
	var retryAttempts atomic.Int32
	retryMock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:               700,
		initSessID:        pinnedSessID,
		skipInitOnAttempt: 1,
		onSpawn:           func(cfg DaemonConfig) { retryAttempts.Add(1) },
	})
	// Attempt 1 never sends init and always times out; the timeout must still leave attempt 2
	// room to hand-shake on a loaded -race CI runner (5ms flaked there).
	retryPool := NewUnifiedProcessPool(PoolConfig{HandshakeTimeout: 250 * time.Millisecond}, retryMock)
	defer retryPool.Close()
	if dRetry, rErr := retryPool.GetOrCreate(ctx, "thread-handshake-retry", ""); rErr != nil || dRetry == nil || retryAttempts.Load() != 2 {
		t.Fatalf("expected handshake timeout retry: d=%v err=%v attempts=%d", dRetry, rErr, retryAttempts.Load())
	}
}

func verifyVanadisWarmupLease(ctx context.Context, t *testing.T, pool *UnifiedProcessPool) {
	t.Helper()
	lease0, err := pool.AcquireLease(ctx, "thread-vanadis")
	if err != nil || lease0.IsEphemeral() || lease0.PreviousSessionID() != "prev-sess-1" || !lease0.IsCold() || lease0.TurnCount() != 0 {
		t.Fatalf("unexpected lease0: err=%v", err)
	}
	if _, err := lease0.Execute(ctx, nil); err == nil {
		t.Fatalf("expected error for nil TurnContext")
	}
	if _, err := lease0.Execute(ctx, &TurnContext{TurnID: "t0", Prompt: "warmup", ChannelInstructions: "chan-rules", ThreadSummary: "summary-text", CoordinationContext: "coord-text"}); err != nil {
		t.Fatalf("Execute 0 error: %v", err)
	}
	if err := lease0.Release(); err != nil {
		t.Fatalf("lease0.Release: %v", err)
	}
}

func TestUnifiedProcessPool_VanadisChunkIDEnvAndRefresh(t *testing.T) {
	setTurnSATokenSrc(t, "token")
	filteredEnv := filterVanadisChunkEnv([]string{"NO_EQUALS_ENTRY", "AERIAL_VANADIS_CHUNK_ID=stale", "VANADIS_CHUNK_ID=stale2", "KEEP_ME=1"})
	if len(filteredEnv) != 2 || filteredEnv[0] != "NO_EQUALS_ENTRY" || filteredEnv[1] != "KEEP_ME=1" {
		t.Fatalf("unexpected filteredEnv: %v", filteredEnv)
	}

	verifyThrowawayTurnSink(t)

	var mu sync.Mutex
	var spawnedEnvs [][]string
	var spawnedSessions []string
	pinnedSessID := uuid.New().String()
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) {
			mu.Lock()
			spawnedEnvs = append(spawnedEnvs, append([]string(nil), cfg.Env...))
			spawnedSessions = append(spawnedSessions, cfg.SessionID)
			mu.Unlock()
		},
		onUser: func(attempt int, line string) (string, string, string) { return "ok", "", "SUCCESS" },
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir: t.TempDir(),
		Env:           []string{"AERIAL_VANADIS_CHUNK_ID=stale-parent", "BASE_VAR=1"},
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{ActiveSessionID: pinnedSessID, PreviousSessionID: "prev-sess-1", TurnCount: 0}, nil
		},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	verifyVanadisWarmupLease(ctx, t, pool)

	for i, chunk := range []string{"chunk 1", "chunk 2"} {
		l, err := pool.AcquireLease(ctx, "thread-vanadis")
		if err != nil || (i == 0 && l.SessionID() != pinnedSessID) {
			t.Fatalf("AcquireLease %d error: %v", i+1, err)
		}
		if _, err := l.Execute(ctx, &TurnContext{TurnID: fmt.Sprintf("t%d", i+1), Prompt: chunk, Ctx: ctx}); err != nil {
			t.Fatalf("Execute %d error: %v", i+1, err)
		}
		if err := l.Release(); err != nil {
			t.Fatalf("l.Release %d error: %v", i+1, err)
		}
	}

	d3, err := pool.GetOrCreate(ctx, "thread-vanadis", "")
	if err != nil || d3.SessionID() != pinnedSessID {
		t.Fatalf("GetOrCreate 3 failed: err=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(spawnedEnvs) != 1 || strings.Contains(strings.Join(spawnedEnvs[0], "\n"), "AERIAL_VANADIS_CHUNK_ID=") || spawnedSessions[0] != pinnedSessID {
		t.Fatalf("spawn env/session validation failed: envs=%v sess=%v", spawnedEnvs, spawnedSessions)
	}

	verifyHandshakeRetryPool(ctx, t, pinnedSessID)
}

func TestUnifiedProcessPool_PrewarmAndLeaseResilience(t *testing.T) {
	setTurnSATokenSrc(t, "token")
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:    900,
		onUser: func(att int, line string) (string, string, string) { return "ok", "", "SUCCESS" },
	})
	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir:    t.TempDir(),
		PrewarmedTargets: []string{"discord:worker-0", "discord:worker-1"},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "discord:worker-0")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	dBefore, ok := pool.Get("discord:worker-0")
	if !ok || dBefore == nil || pool.prewarmTarget(ctx, "   ") != nil || pool.Initialize(ctx) != nil {
		t.Fatalf("initial prewarm/lease state invalid")
	}
	waitPoolBackground(pool)
	if dAfterInit, okInit := pool.Get("discord:worker-0"); !okInit || dAfterInit != dBefore || dBefore.State() == StateClosed || !poolHasDaemon(pool, "discord:worker-1") {
		t.Fatalf("expected worker-0 untouched and worker-1 pre-warmed after Initialize")
	}

	pool.MarkDirty()
	waitPoolBackground(pool)
	if err = pool.prewarmTarget(ctx, "discord:worker-0"); err != nil || dBefore.State() == StateClosed {
		t.Fatalf("MarkDirty or prewarmTarget while locked failed: %v", err)
	}

	_ = dBefore.Close()
	res, err := lease.Execute(ctx, &TurnContext{TurnID: "retry-2", Prompt: "retry", Ctx: ctx})
	if err != nil || res == nil || res.Response != "ok" {
		t.Fatalf("expected lease.Execute refresh on retry: res=%+v err=%v", res, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("lease.Release: %v", err)
	}

	dWorker1, okWorker1 := pool.Get("discord:worker-1")
	if !okWorker1 || dWorker1 == nil {
		t.Fatalf("expected discord:worker-1 to exist")
	}
	verifyEvictedDaemonInflightClose(t, dWorker1)
}

func verifyEvictedDaemonInflightClose(t *testing.T, dWorker1 *StreamingDaemon) {
	t.Helper()
	dWorker1.inflightMu.Lock()
	dWorker1.inflight = append(dWorker1.inflight, &TurnContext{TurnID: "inflight-1"})
	dWorker1.inflightMu.Unlock()
	closeEvictedDaemon(dWorker1, "discord:worker-1")
	if dWorker1.State() == StateClosed {
		t.Fatalf("expected closeEvictedDaemon not to close daemon while inflight > 0")
	}
	dWorker1.inflightMu.Lock()
	dWorker1.inflight = nil
	cb := dWorker1.onTurnFinished
	dWorker1.inflightMu.Unlock()
	if cb == nil {
		t.Fatalf("expected onTurnFinished callback on in-flight evicted daemon")
	}
	cb(dWorker1)
	if dWorker1.State() != StateClosed {
		t.Fatalf("expected evicted daemon to close once onTurnFinished fires")
	}
	closeEvictedDaemon(nil, "discord:worker-1")
}

func TestUnifiedSessionLease_ExecuteFailure_MarksDirty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:    401,
		onUser: func(att int, line string) (string, string, string) { return "", "simulated failure", "ERROR" },
	})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	lease, err := pool.AcquireLease(ctx, "target-exec-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Release()

	if _, execErr := lease.Execute(ctx, &TurnContext{TurnID: "t1", Prompt: "p", Ctx: ctx}); execErr == nil {
		t.Fatalf("expected Execute error, got nil")
	}
	if !requireType[*unifiedSessionLease](t, lease).daemon.IsDirty() {
		t.Errorf("expected daemon to be marked dirty after execution failure")
	}
}

func TestUnifiedSessionLease_DirectMarkDirty(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
	defer pool.Close()
	lease, err := pool.AcquireLease(context.Background(), "target-mark-dirty")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Release()
	uLease := requireType[*unifiedSessionLease](t, lease)
	if uLease.daemon.IsDirty() {
		t.Fatalf("expected daemon to not be dirty initially")
	}
	if uLease.ShouldRotate() {
		t.Errorf("expected ShouldRotate() to be false on clean lease")
	}
	lease.MarkDirty()
	if !uLease.daemon.IsDirty() {
		t.Errorf("expected daemon to be dirty after lease.MarkDirty()")
	}
	if !uLease.ShouldRotate() {
		t.Errorf("expected ShouldRotate() to be true after lease.MarkDirty()")
	}
	var nilLease *unifiedSessionLease
	nilLease.MarkDirty()
	if nilLease.ShouldRotate() {
		t.Errorf("expected ShouldRotate() to be false on nil lease")
	}
}

func TestUnifiedSessionLease_Release_RotationFailurePaths(t *testing.T) {
	ctx := context.Background()
	pool1 := NewUnifiedProcessPool(PoolConfig{MaxSessionTurns: 1}, &MockDaemonSpawner{})
	lease1, err := pool1.AcquireLease(ctx, "target-rot-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	requireType[*unifiedSessionLease](t, lease1).turnCount = 2
	pool1.Close()
	if relErr := lease1.Release(); relErr == nil {
		t.Errorf("expected Release error when RotateDaemon fails on closed pool")
	}

	cbErr := errors.New("persistence failed")
	pool2 := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns:  1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error { return cbErr },
	}, &MockDaemonSpawner{})
	defer pool2.Close()
	lease2, err := pool2.AcquireLease(ctx, "target-cb-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	requireType[*unifiedSessionLease](t, lease2).turnCount = 2
	if relErr := lease2.Release(); relErr != nil {
		t.Errorf("expected nil Release error, got %v", relErr)
	}
	waitPoolBackground(pool2)
	if poolHasDaemon(pool2, "target-cb-fail") {
		t.Errorf("expected daemon evicted on callback failure")
	}
}

func TestUnifiedProcessPool_EvictSessionAndTranscriptRescuer(t *testing.T) {
	t.Parallel()
	var nilUnified *UnifiedProcessPool
	if err := nilUnified.EvictSession("target-1"); err != nil {
		t.Errorf("expected nil error on nil UnifiedProcessPool.EvictSession")
	}
	mock := createTestSpawner()
	uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPool.Close()
	if err := uPool.EvictSession("nonexistent"); err != nil {
		t.Errorf("expected nil error on nonexistent target eviction: %v", err)
	}
	sess, err := uPool.GetOrCreate(context.Background(), "target-evict-live", "")
	if err != nil || sess == nil || uPool.EvictSession("target-evict-live") != nil || poolHasDaemon(uPool, "target-evict-live") {
		t.Fatalf("unexpected error in evict flow: sess=%v err=%v", sess, err)
	}
	var nilPool *UnifiedProcessPool
	nilPool.SetTranscriptRescuer(func(ctx context.Context, convID string, since time.Time) string { return "" })

	uPoolRescuer := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPoolRescuer.Close()
	if _, err := uPoolRescuer.GetOrCreate(context.Background(), "target-rescuer", ""); err != nil {
		t.Fatalf("GetOrCreate target-rescuer failed: %v", err)
	}
	rescuerCalled := false
	uPoolRescuer.SetTranscriptRescuer(func(ctx context.Context, convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})
	if uPoolRescuer.cfg.TranscriptRescuer == nil || !rescuerCalled && uPoolRescuer.cfg.TranscriptRescuer(context.Background(), "conv-1", time.Now()) != "rescued" || !rescuerCalled {
		t.Errorf("expected rescuer callback to be set and invoked")
	}
}

func TestIsDaemonMatch_Comprehensive(t *testing.T) {
	t.Parallel()
	dClosed, dDirty := &StreamingDaemon{state: StateClosed}, &StreamingDaemon{dirty: true, state: StateReady}
	dReady, dEmpty := &StreamingDaemon{state: StateReady, sessionID: "sess-abc"}, &StreamingDaemon{state: StateReady, sessionID: ""}
	tests := []struct {
		d    *StreamingDaemon
		sess string
		want bool
	}{
		{nil, "", false},
		{dClosed, "", false},
		{dDirty, "", false},
		{dReady, "", true},
		{dEmpty, "sess-target", true},
		{dReady, "sess-abc", true},
		{dReady, "sess-xyz", false},
	}
	for _, tc := range tests {
		if got := isDaemonMatch(tc.d, tc.sess); got != tc.want {
			t.Errorf("isDaemonMatch(%v, %q) = %v, want %v", tc.d, tc.sess, got, tc.want)
		}
	}
}

// Compile-time interface checks for pool/session implementations and the
// mocks other tests in this package substitute for them.
var (
	_ AgentPool    = (*UnifiedProcessPool)(nil)
	_ AgentSession = (*StreamingDaemon)(nil)
	_ AgentPool    = (*mockVoicePool)(nil)
	_ AgentSession = (*mockVoiceSession)(nil)
)

func runUnifiedSessionLeaseWriters(wg *sync.WaitGroup, lease *unifiedSessionLease, writers, iterations int) {
	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range iterations {
				lease.mu.Lock()
				lease.sessionID = fmt.Sprintf("sess-%d-%d", idx, j)
				lease.mu.Unlock()
			}
		}(i)
	}
}

func runUnifiedSessionLeaseReaders(t *testing.T, wg *sync.WaitGroup, lease *unifiedSessionLease, readers, iterations int) {
	t.Helper()
	for range readers {
		wg.Go(func() {
			for range iterations {
				if id := lease.SessionID(); id == "" {
					t.Errorf("expected non-empty session ID")
				}
				lease.MarkDirty()
			}
		})
	}
}

func TestUnifiedSessionLease_ThreadSafety(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initialID  string
		writers    int
		readers    int
		iterations int
	}{
		{name: "concurrent reads and writes", initialID: "init-id", writers: 4, readers: 8, iterations: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lease := &unifiedSessionLease{sessionID: tc.initialID}
			var wg sync.WaitGroup
			runUnifiedSessionLeaseReaders(t, &wg, lease, tc.readers, tc.iterations)
			runUnifiedSessionLeaseWriters(&wg, lease, tc.writers, tc.iterations)
			wg.Wait()
		})
	}
}

func verifyDaemonSpawnForSession(t *testing.T, poolCfg PoolConfig, target, sessionID string) DaemonConfig {
	t.Helper()
	var mu sync.Mutex
	var spawnCfg DaemonConfig
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(c DaemonConfig) {
			mu.Lock()
			spawnCfg = c
			mu.Unlock()
		},
	})
	pool := NewUnifiedProcessPool(poolCfg, mock)
	defer pool.Close()

	ctx := context.Background()
	daemon, err := pool.GetOrCreate(ctx, target, sessionID)
	if err != nil || daemon == nil {
		t.Fatalf("unexpected GetOrCreate error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return spawnCfg
}

// seedEngineSessionFiles writes one on-disk session per resumable engine under root (the
// session manager's data dir): a codex rollout and a muse session log.
func seedEngineSessionFiles(t *testing.T, root, codexSessID, museSessID string) {
	t.Helper()
	files := map[string]string{
		filepath.Join(root, "codex", "sessions", "2026", "11", "15", "rollout-20261115-"+codexSessID+".jsonl"): `{"type":"response_item","payload":{"type":"message","role":"user","content":"hello"}}`,
		filepath.Join(root, "muse", "sessions", museSessID+".jsonl"):                                           `{"version":1,"payload_type":"runtime.session","payload":{"type":"event","event":{"kind":"assistant_message_committed"}}}`,
	}
	for path, body := range files {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create session dir: %v", err)
		}
		if err := os.WriteFile(path, []byte(body+"\n"), 0o644); err != nil {
			t.Fatalf("write session file: %v", err)
		}
	}
}

func TestUnifiedProcessPool_BuildDaemonConfig(t *testing.T) {
	t.Parallel()
	const codexSessID, museSessID = "th_existing_on_disk_123", "018e3f94-9b56-7890-a123-456789abcdef"
	tmpDir := t.TempDir()
	seedEngineSessionFiles(t, tmpDir, codexSessID, museSessID)
	sm := session.New(tmpDir, tmpDir)

	tests := []struct {
		name        string
		engine      string
		sessionID   string
		wantResume  bool
		wantNewUUID bool
	}{
		{name: "claude empty session id mints a uuid", engine: "claude", wantNewUUID: true},
		{name: "codex empty session id does not resume", engine: "codex"},
		{name: "codex unknown session id does not resume", engine: "codex", sessionID: "th_non_existent_999"},
		{name: "codex session on disk resumes", engine: "codex", sessionID: codexSessID, wantResume: true},
		{name: "muse empty session id does not resume", engine: "muse"},
		{name: "muse unknown session id does not resume", engine: "muse", sessionID: "018e3f94-0000-0000-0000-000000000000"},
		{name: "muse session on disk resumes", engine: "muse", sessionID: museSessID, wantResume: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := verifyDaemonSpawnForSession(t, PoolConfig{
				Engine:         tc.engine,
				ClaudeBin:      "/custom/bin/claude",
				CodexBin:       "/custom/bin/codex",
				MuseBin:        "/custom/bin/muse",
				SessionManager: sm,
			}, "target-"+tc.name, tc.sessionID)

			if got.Engine != tc.engine || got.ClaudeBin != "/custom/bin/claude" || got.CodexBin != "/custom/bin/codex" || got.MuseBin != "/custom/bin/muse" {
				t.Errorf("engine/bins = %q %q %q %q, want %q and the configured bins", got.Engine, got.ClaudeBin, got.CodexBin, got.MuseBin, tc.engine)
			}
			if got.ResumeSession != tc.wantResume {
				t.Errorf("ResumeSession = %v, want %v", got.ResumeSession, tc.wantResume)
			}
			if tc.sessionID != "" && got.SessionID != tc.sessionID {
				t.Errorf("SessionID = %q, want %q", got.SessionID, tc.sessionID)
			}
			if gotUUID := tc.sessionID == "" && IsValidUUID(got.SessionID); gotUUID != tc.wantNewUUID {
				t.Errorf("SessionID = %q, want minted uuid %v", got.SessionID, tc.wantNewUUID)
			}
		})
	}
}

func TestUnifiedProcessPool_UpdateEngine(t *testing.T) {
	t.Parallel()
	var nilPool *UnifiedProcessPool
	nilPool.UpdateEngine("codex", "/bin/claude") // nil receiver is a no-op, not a panic

	tests := []struct {
		name    string
		initial PoolConfig
		args    []string
		want    [4]string // engine, claude, codex, muse
	}{
		{"codex sets claude and codex bins", PoolConfig{Engine: "agy"}, []string{"codex", "/bin/claude", "/bin/codex"}, [4]string{"codex", "/bin/claude", "/bin/codex", ""}},
		{"muse sets every bin", PoolConfig{Engine: "agy"}, []string{"muse", "/bin/claude", "/bin/codex", "/bin/muse"}, [4]string{"muse", "/bin/claude", "/bin/codex", "/bin/muse"}},
		{"omitted codex and muse bins are preserved", PoolConfig{Engine: "muse", CodexBin: "/bin/codex", MuseBin: "/bin/muse"}, []string{"claude", "/usr/local/bin/claude"}, [4]string{"claude", "/usr/local/bin/claude", "/bin/codex", "/bin/muse"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := NewUnifiedProcessPool(tc.initial, NewMockSpawner())
			defer pool.Close()

			pool.UpdateEngine(tc.args[0], tc.args[1], tc.args[2:]...)
			pool.mu.RLock()
			got := [4]string{pool.cfg.Engine, pool.cfg.ClaudeBin, pool.cfg.CodexBin, pool.cfg.MuseBin}
			pool.mu.RUnlock()
			if got != tc.want {
				t.Errorf("engine/claude/codex/muse = %q, want %q", got, tc.want)
			}
		})
	}
}

func runReleaseUpkeepContextCase(t *testing.T, closeDuring bool) {
	t.Helper()
	entered, proceed := make(chan struct{}), make(chan struct{})
	finalErrCh := make(chan error, 1)
	cfg := PoolConfig{MaxSessionTurns: 1, OnSessionRotated: func(ctx context.Context, _, _, _ string) error {
		if ctx.Err() != nil {
			t.Error("upkeep inherited request cancellation")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 35*time.Second {
			t.Error("missing bounded upkeep deadline")
		}
		close(entered)
		if closeDuring {
			<-ctx.Done()
		} else {
			<-proceed
		}
		err := ctx.Err()
		finalErrCh <- err
		return err
	}}
	pool := NewUnifiedProcessPool(cfg, &MockDaemonSpawner{})
	defer pool.Close()
	request, cancel := context.WithCancel(context.Background())
	lease, err := pool.AcquireLease(request, "target")
	if err != nil {
		t.Fatal(err)
	}
	requireType[*unifiedSessionLease](t, lease).turnCount = 2
	cancel()
	done := make(chan error, 1)
	go func() { done <- lease.Release() }()
	<-entered
	if err := <-done; err != nil {
		t.Fatalf("release error = %v", err)
	}
	if closeDuring {
		pool.Close()
	} else {
		close(proceed)
	}
	waitPoolBackground(pool)
	finalErr := <-finalErrCh
	if closeDuring {
		if !errors.Is(finalErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", finalErr)
		}
	} else if finalErr != nil {
		t.Fatalf("expected nil final error, got %v", finalErr)
	}
}

func TestUnifiedProcessPool_ReleaseUpkeepContext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		closeDuring bool
	}{{"request canceled", false}, {"pool closed", true}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runReleaseUpkeepContextCase(t, tc.closeDuring)
		})
	}
}

func TestUnifiedProcessPool_StartupCancellation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		preCancel bool
		failure   error
	}{{"before first attempt", true, syscall.ETXTBSY}, {"after transient failure", false, syscall.ETXTBSY}, {"after permanent failure", false, syscall.EINVAL}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.preCancel {
				cancel()
			}
			processContexts := make(chan context.Context, 2)
			spawner := &DefaultDaemonSpawner{RetryDelays: []time.Duration{time.Hour}, spawnOnceFn: func(lifetime context.Context, _ string, _ []string, _ DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				processContexts <- lifetime
				cancel()
				return nil, nil, nil, nil, tc.failure
			}}
			pool := NewUnifiedProcessPool(PoolConfig{}, spawner)
			defer pool.Close()
			_, err := pool.spawnStreamingDaemonWithRetry(ctx, "target", "", nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("startup error = %v, want canceled", err)
			}
			if !tc.preCancel && (<-processContexts).Err() != nil {
				t.Fatal("process lifetime inherited startup cancellation")
			}
		})
	}
}

// requireType asserts v's concrete type, failing the test with the actual
// type on mismatch.
func requireType[T any](t *testing.T, v any) T {
	t.Helper()
	got, ok := v.(T)
	if !ok {
		var want T
		t.Fatalf("value is %T, want %T", v, want)
	}
	return got
}

func TestIsStaleIsolatedSession(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Scenario 1: Symlinked conversations directory
	symlinkHome := filepath.Join(tmpDir, "symlink-home")
	realStorage := filepath.Join(tmpDir, "shared-storage")
	if err := os.MkdirAll(realStorage, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(symlinkHome, ".gemini", "antigravity-cli"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realStorage, filepath.Join(symlinkHome, ".gemini", "antigravity-cli", "conversations")); err != nil {
		t.Fatal(err)
	}

	// Scenario 2: Real isolated directory without session files
	isolatedEmptyHome := filepath.Join(tmpDir, "isolated-empty-home")
	if err := os.MkdirAll(filepath.Join(isolatedEmptyHome, ".gemini", "antigravity-cli", "conversations"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(isolatedEmptyHome, ".gemini", "antigravity-cli", "brain"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Scenario 3: Real isolated directory with pb file
	isolatedPbHome := filepath.Join(tmpDir, "isolated-pb-home")
	convPbDir := filepath.Join(isolatedPbHome, ".gemini", "antigravity-cli", "conversations")
	if err := os.MkdirAll(convPbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(convPbDir, "sess-1.pb"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Scenario 4: Real isolated directory with db file
	isolatedDbHome := filepath.Join(tmpDir, "isolated-db-home")
	convDbDir := filepath.Join(isolatedDbHome, ".gemini", "antigravity-cli", "conversations")
	if err := os.MkdirAll(convDbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(convDbDir, "sess-2.db"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Scenario 5: Real isolated directory with brain/<id> dir
	isolatedBrainHome := filepath.Join(tmpDir, "isolated-brain-home")
	brainSubDir := filepath.Join(isolatedBrainHome, ".gemini", "antigravity-cli", "brain", "sess-3")
	if err := os.MkdirAll(brainSubDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Scenario 6: Symlinked voice-sessions directory without session file (stale)
	voiceSessionsEmptyTarget := filepath.Join(tmpDir, "voice-sessions", "sess-empty")
	if err := os.MkdirAll(filepath.Join(voiceSessionsEmptyTarget, "conversations"), 0o755); err != nil {
		t.Fatal(err)
	}
	voiceSymlinkEmptyHome := filepath.Join(tmpDir, "voice-symlink-empty-home")
	if err := os.MkdirAll(filepath.Join(voiceSymlinkEmptyHome, ".gemini", "antigravity-cli"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(voiceSessionsEmptyTarget, "conversations"), filepath.Join(voiceSymlinkEmptyHome, ".gemini", "antigravity-cli", "conversations")); err != nil {
		t.Fatal(err)
	}

	// Scenario 7: Symlinked voice-sessions directory with session file present (not stale)
	voiceSessionsPresentTarget := filepath.Join(tmpDir, "voice-sessions", "sess-present", "conversations")
	if err := os.MkdirAll(voiceSessionsPresentTarget, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(voiceSessionsPresentTarget, "sess-voice.pb"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	voiceSymlinkPresentHome := filepath.Join(tmpDir, "voice-symlink-present-home")
	if err := os.MkdirAll(filepath.Join(voiceSymlinkPresentHome, ".gemini", "antigravity-cli"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(voiceSessionsPresentTarget, filepath.Join(voiceSymlinkPresentHome, ".gemini", "antigravity-cli", "conversations")); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		homeDir   string
		sessionID string
		wantStale bool
	}{
		{name: "empty home dir", homeDir: "", sessionID: "sess-1", wantStale: false},
		{name: "empty session id", homeDir: isolatedEmptyHome, sessionID: "", wantStale: false},
		{name: "non-existent home dir", homeDir: filepath.Join(tmpDir, "non-existent"), sessionID: "sess-1", wantStale: false},
		{name: "symlinked conversations dir", homeDir: symlinkHome, sessionID: "sess-1", wantStale: false},
		{name: "isolated empty dir lacking session", homeDir: isolatedEmptyHome, sessionID: "sess-unknown", wantStale: true},
		{name: "isolated dir with pb file present", homeDir: isolatedPbHome, sessionID: "sess-1", wantStale: false},
		{name: "isolated dir with db file present", homeDir: isolatedDbHome, sessionID: "sess-2", wantStale: false},
		{name: "isolated dir with brain dir present", homeDir: isolatedBrainHome, sessionID: "sess-3", wantStale: false},
		{name: "symlinked voice-sessions dir lacking session", homeDir: voiceSymlinkEmptyHome, sessionID: "sess-voice-missing", wantStale: true},
		{name: "symlinked voice-sessions dir with session file present", homeDir: voiceSymlinkPresentHome, sessionID: "sess-voice", wantStale: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isStaleIsolatedSession(tc.homeDir, tc.sessionID)
			if got != tc.wantStale {
				t.Fatalf("isStaleIsolatedSession(%q, %q) = %v, want %v", tc.homeDir, tc.sessionID, got, tc.wantStale)
			}
		})
	}
}

func TestUnifiedProcessPool_ResolveTargetSessionRecord(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		engine        string
		targetKey     string
		recordSession string
		recordTurns   int
		hasFileOnDisk bool
		wantSession   string
		wantTurns     int
	}{
		{
			name:          "empty targetKey returns empty record",
			targetKey:     "  ",
			recordSession: "sess-should-not-reach",
			recordTurns:   7,
			hasFileOnDisk: true,
			wantSession:   "",
			wantTurns:     0,
		},
		{
			name:          "targetKey containing colon skips getRec",
			targetKey:     "ephemeral:worker-0",
			recordSession: "sess-ephem",
			recordTurns:   3,
			hasFileOnDisk: true,
			wantSession:   "",
			wantTurns:     0,
		},
		{
			name:          "valid session on disk preserved",
			targetKey:     "living-room",
			recordSession: "sess-valid",
			recordTurns:   5,
			hasFileOnDisk: true,
			wantSession:   "sess-valid",
			wantTurns:     5,
		},
		{
			name:          "stale session lacking on disk cleared",
			targetKey:     "kitchen",
			recordSession: "sess-stale",
			recordTurns:   8,
			hasFileOnDisk: false,
			wantSession:   "",
			wantTurns:     0,
		},
		{
			name:          "non-agy engine preserves voice session without agy artifacts",
			engine:        "claude",
			targetKey:     "office",
			recordSession: "sess-claude",
			recordTurns:   4,
			hasFileOnDisk: false,
			wantSession:   "sess-claude",
			wantTurns:     4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subTmp := t.TempDir()
			isolatedHome := filepath.Join(subTmp, "voice-home")
			convDir := filepath.Join(isolatedHome, ".gemini", "antigravity-cli", "conversations")
			if err := os.MkdirAll(convDir, 0o755); err != nil {
				t.Fatal(err)
			}

			if tc.hasFileOnDisk && tc.recordSession != "" {
				if err := os.WriteFile(filepath.Join(convDir, tc.recordSession+".pb"), []byte("data"), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			pool := NewUnifiedProcessPool(PoolConfig{
				Engine:        tc.engine,
				GeminiHomeDir: isolatedHome,
				GetSessionRecord: func(ctx context.Context, key string) (SessionRecord, error) {
					return SessionRecord{
						ActiveSessionID: tc.recordSession,
						TurnCount:       tc.recordTurns,
					}, nil
				},
			}, &MockDaemonSpawner{})
			defer pool.Close()

			got := pool.resolveTargetSessionRecord(context.Background(), tc.targetKey)
			if got.ActiveSessionID != tc.wantSession || got.TurnCount != tc.wantTurns {
				t.Fatalf("expected session=%q turns=%d, got session=%q turns=%d", tc.wantSession, tc.wantTurns, got.ActiveSessionID, got.TurnCount)
			}
		})
	}
}

type supersededSessionCase struct {
	name            string
	storeSessID     string
	storeLeaseGen   int64
	simulateReadErr bool
}

func runSupersededSessionCase(t *testing.T, tc supersededSessionCase) {
	t.Helper()
	ctx := context.Background()
	var mu sync.Mutex
	storeRecord := SessionRecord{ActiveSessionID: "00000000-0000-0000-0000-000000000001", TurnCount: 2, LeaseGeneration: 1}
	var onRotatedCalled atomic.Bool
	spawnStarted := make(chan struct{})
	proceedSpawn := make(chan struct{})
	var spawnCount atomic.Int32

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:        450,
		initSessID: "00000000-0000-0000-0000-000000000001",
		echoRes:    "ok",
		onSpawn: func(cfg DaemonConfig) {
			if spawnCount.Add(1) == 2 {
				close(spawnStarted)
				<-proceedSpawn
			}
		},
	})

	var readErrActive atomic.Bool
	pool := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns: 1,
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			if readErrActive.Load() {
				return SessionRecord{}, errors.New("simulated store read error")
			}
			mu.Lock()
			defer mu.Unlock()
			return storeRecord, nil
		},
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			onRotatedCalled.Store(true)
			return nil
		},
	}, mock)
	defer pool.Close()

	const targetKey = "target-superseded"
	lease, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	requireType[*unifiedSessionLease](t, lease).turnCount = 2

	if err := lease.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	<-spawnStarted
	if tc.simulateReadErr {
		readErrActive.Store(true)
	}
	mu.Lock()
	if tc.storeSessID != "" {
		storeRecord.ActiveSessionID = tc.storeSessID
	}
	if tc.storeLeaseGen > 0 {
		storeRecord.LeaseGeneration = tc.storeLeaseGen
	}
	mu.Unlock()

	close(proceedSpawn)
	waitPoolBackground(pool)

	if onRotatedCalled.Load() {
		t.Errorf("expected OnSessionRotated NOT to be called when session record is superseded")
	}
	if poolHasDaemon(pool, targetKey) {
		t.Errorf("expected daemon to be evicted when session was superseded")
	}
}

func TestUnifiedProcessPool_SupersededSessionDuringBackgroundRotation(t *testing.T) {
	t.Parallel()
	tests := []supersededSessionCase{
		{
			name:        "different session rotated by other pod",
			storeSessID: "00000000-0000-0000-0000-000000000099",
		},
		{
			name:          "newer lease generation acquired by other pod",
			storeLeaseGen: 2,
		},
		{
			name:            "store read error prevents rotation save",
			simulateReadErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSupersededSessionCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_WaitingTurnDuringBackgroundRotationGetsNewSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	storeRecord := SessionRecord{ActiveSessionID: "00000000-0000-0000-0000-000000000001", TurnCount: 2}
	var onRotatedCalled atomic.Bool
	spawnStarted := make(chan struct{})
	proceedSpawn := make(chan struct{})
	var spawnCount atomic.Int32

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:        452,
		initSessID: "00000000-0000-0000-0000-000000000001",
		echoRes:    "ok",
		onSpawn: func(cfg DaemonConfig) {
			if spawnCount.Add(1) == 2 {
				close(spawnStarted)
				<-proceedSpawn
			}
		},
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns: 2,
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			mu.Lock()
			defer mu.Unlock()
			return storeRecord, nil
		},
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			onRotatedCalled.Store(true)
			mu.Lock()
			defer mu.Unlock()
			storeRecord.ActiveSessionID = newSessionID
			storeRecord.PreviousSessionID = oldSessionID
			storeRecord.TurnCount = 0
			return nil
		},
	}, mock)
	defer pool.Close()

	const targetKey = "target-waiting-turn"
	lease1, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("AcquireLease 1 failed: %v", err)
	}
	requireType[*unifiedSessionLease](t, lease1).turnCount = 2

	if err := lease1.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	<-spawnStarted

	// Simulate incoming Discord message: PrepareTurn increments turn_count in store before acquiring lease
	mu.Lock()
	storeRecord.TurnCount = 3
	mu.Unlock()

	// Launch second AcquireLease in goroutine; it must block waiting for targetLock held by background rotation
	lease2Chan := make(chan SessionLease, 1)
	errChan := make(chan error, 1)
	go func() {
		l2, l2Err := pool.AcquireLease(ctx, targetKey)
		if l2Err != nil {
			errChan <- l2Err
			return
		}
		lease2Chan <- l2
	}()

	// Wait until the second caller is known to be queued on the target lock (refs reaches 2).
	for range 10000 {
		pool.targetMu.Lock()
		tl := pool.targetLocks[targetKey]
		var refs int32
		if tl != nil {
			refs = tl.refs
		}
		pool.targetMu.Unlock()
		if refs >= 2 {
			break
		}
		runtime.Gosched()
	}
	// Assert neither lease2Chan nor errChan has fired yet (caller is still blocked).
	select {
	case l2 := <-lease2Chan:
		t.Fatalf("expected AcquireLease to block on targetLock, but received lease early: %v", l2)
	case l2Err := <-errChan:
		t.Fatalf("expected AcquireLease to block on targetLock, but failed early: %v", l2Err)
	default:
	}

	// Unblock spawn and allow rotation to finish
	close(proceedSpawn)
	waitPoolBackground(pool)

	if !onRotatedCalled.Load() {
		t.Errorf("expected OnSessionRotated to be called when turn count was incremented by waiting turn")
	}

	select {
	case l2Err := <-errChan:
		t.Fatalf("waiting AcquireLease failed: %v", l2Err)
	case l2 := <-lease2Chan:
		defer l2.Release()
		if l2.SessionID() != "00000000-0000-0000-0000-000000000002" {
			t.Errorf("expected waiting lease to get new session %q, got %q", "00000000-0000-0000-0000-000000000002", l2.SessionID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lease2 to unblock")
	}
}

func TestUnifiedProcessPool_ColdLease_RotationSucceedsWithTurnIncrement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var mu sync.Mutex
	storeRecord := SessionRecord{ActiveSessionID: "00000000-0000-0000-0000-000000000001", TurnCount: 1}
	var onRotatedCalled atomic.Bool
	var rotatedOld, rotatedNew string

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:        451,
		initSessID: "00000000-0000-0000-0000-000000000001",
		echoRes:    "ok",
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns: 1,
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			mu.Lock()
			defer mu.Unlock()
			return storeRecord, nil
		},
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
			onRotatedCalled.Store(true)
			mu.Lock()
			defer mu.Unlock()
			rotatedOld, rotatedNew = oldSessionID, newSessionID
			return nil
		},
	}, mock)
	defer pool.Close()

	const targetKey = "target-cold-lease-rot"
	lease, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if !lease.IsCold() {
		t.Fatalf("expected lease to be cold when store has TurnCount=1")
	}

	// Simulate turn execution: store turn count is incremented once by its own turn before release
	mu.Lock()
	storeRecord.TurnCount = 2
	mu.Unlock()
	lease.MarkDirty()

	if err := lease.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	waitPoolBackground(pool)

	if !onRotatedCalled.Load() {
		t.Errorf("expected OnSessionRotated to be called for cold lease with no later use")
	}
	if rotatedOld != "00000000-0000-0000-0000-000000000001" || rotatedNew == "" {
		t.Errorf("expected rotation from old to new session ID, got old=%q new=%q", rotatedOld, rotatedNew)
	}
	if !poolHasDaemon(pool, targetKey) {
		t.Errorf("expected newly rotated daemon to be retained in pool")
	}
}
