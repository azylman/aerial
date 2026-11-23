package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

func (p *InterchangeablePool) Workers() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.workers))
	copy(out, p.workers)
	return out
}

func (p *InterchangeablePool) AvailableCount() int {
	if p == nil {
		return 0
	}
	return len(p.available)
}

func (p *InterchangeablePool) WaitBackground() {
	if p == nil {
		return
	}
	p.bgWg.Wait()
	if p.underlying != nil {
		p.underlying.WaitBackground()
	}
}

func handleTestSpawnerInput(inR io.Reader, outW io.WriteCloser, sessUUID string, id int64, onPrompt []func(prompt string)) {
	defer outW.Close()
	if _, err := fmt.Fprintf(outW, `{"event":"init","conversation_id":%q}`+"\n", sessUUID); err != nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := inR.Read(buf)
		if err != nil {
			return
		}
		if n <= 0 {
			continue
		}
		prompt := string(buf[:n])
		for _, fn := range onPrompt {
			if fn != nil {
				fn(prompt)
			}
		}
		resp := fmt.Sprintf("response-to-%d", id)
		if strings.Contains(prompt, "custom-prompt") {
			resp = "custom-result-ok"
		}
		if _, err := fmt.Fprintf(outW, `{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", resp); err != nil {
			return
		}
	}
}

// Helper mock spawner that generates deterministic sequential session UUIDs
// and responds to user prompts with structured result events.
func createTestSpawner(onPrompt ...func(prompt string)) *MockDaemonSpawner {
	var spawnCount atomic.Int64
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			id := spawnCount.Add(1)
			sessUUID := fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", id)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			go handleTestSpawnerInput(inR, outW, sessUUID, id, onPrompt)
			return inW, outR, io.NopCloser(strings.NewReader("")), &MockProcessHandle{pid: int(id * 100)}, nil
		},
	}
}

func newTestInterchangeablePool(t *testing.T, workers int, prefix, model string, spawner DaemonSpawner) *InterchangeablePool {
	t.Helper()
	if spawner == nil {
		spawner = createTestSpawner()
	}
	cfg := PoolConfig{}
	if model != "" {
		cfg.DefaultModel = model
	}
	underlying := NewUnifiedProcessPool(cfg, spawner)
	t.Cleanup(func() { _ = underlying.Close() })
	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: workers, KeyPrefix: prefix})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func TestInterchangeablePool_InitializationAndConfig(t *testing.T) {
	var spawns atomic.Int32
	mock := createTestSpawner()
	orig := mock.SpawnFn
	mock.SpawnFn = func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		spawns.Add(1)
		return orig(ctx, cfg)
	}
	pool := newTestInterchangeablePool(t, 3, "custom:worker", "test-model", mock)
	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	pool.Underlying().WaitBackground()
	workers := pool.Workers()
	if len(workers) != 3 || pool.AvailableCount() != 3 || spawns.Load() < 3 || workers[0] != "custom:worker-0" || workers[1] != "custom:worker-1" || workers[2] != "custom:worker-2" {
		t.Fatalf("unexpected pool state or worker keys: %v", workers)
	}
	defPool := newTestInterchangeablePool(t, 0, "", "", nil)
	defWorkers := defPool.Workers()
	if len(defWorkers) != 2 || defWorkers[0] != "ephemeral:worker-0" || defWorkers[1] != "ephemeral:worker-1" {
		t.Fatalf("unexpected default worker keys: %v", defWorkers)
	}
}

func TestInterchangeablePool_LeaseAutoRotateAndRelease(t *testing.T) {
	pool := newTestInterchangeablePool(t, 1, "", "", nil)
	ctx := context.Background()

	l1, err1 := pool.AcquireLease(ctx, "target-1")
	if err1 != nil {
		t.Fatalf("AcquireLease l1: %v", err1)
	}
	res1, execErr1 := l1.Execute(ctx, &TurnContext{TurnID: "turn-1", Prompt: "hello 1", Ctx: ctx})
	if execErr1 != nil {
		t.Fatalf("l1.Execute: %v", execErr1)
	}
	if relErr1 := l1.Release(); relErr1 != nil {
		t.Fatalf("l1.Release: %v", relErr1)
	}
	pool.WaitBackground()
	l2, err2 := pool.AcquireLease(ctx, "target-1")
	if err2 != nil {
		t.Fatalf("AcquireLease l2: %v", err2)
	}
	res2, execErr2 := l2.Execute(ctx, &TurnContext{TurnID: "turn-2", Prompt: "hello 2", Ctx: ctx})
	if execErr2 != nil {
		t.Fatalf("l2.Execute: %v", execErr2)
	}
	if relErr2 := l2.Release(); relErr2 != nil {
		t.Fatalf("l2.Release: %v", relErr2)
	}
	pool.WaitBackground()

	if l1.SessionID() == "" || l2.SessionID() == "" || l1.SessionID() == l2.SessionID() || res1.Response != "response-to-1" || res2.Response != "response-to-2" || pool.AvailableCount() != 1 {
		t.Fatalf("unexpected turn responses or availability: id=(%q, %q), res=(%v, %v)", l1.SessionID(), l2.SessionID(), res1, res2)
	}

	errMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			_ = inR.Close()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"550e8400-e29b-41d4-a716-446655440002"}` + "\n"))
			}()
			return inW, outR, io.NopCloser(strings.NewReader("")), &MockProcessHandle{pid: 500}, nil
		},
	}
	errPool := newTestInterchangeablePool(t, 1, "", "", errMock)
	if _, ephErr := errPool.ExecuteEphemeral(ctx, "target", "trigger write error"); ephErr == nil {
		t.Fatal("expected write error")
	}
	errPool.WaitBackground()

	lNilCtx, acqNilErr := pool.AcquireLease(ctx, "target-nil")
	if acqNilErr != nil {
		t.Fatalf("AcquireLease lNilCtx: %v", acqNilErr)
	}
	if _, nilExecErr := lNilCtx.Execute(ctx, nil); nilExecErr == nil {
		t.Fatal("expected nil TurnContext error")
	}
	if relNilErr := lNilCtx.Release(); relNilErr != nil {
		t.Fatalf("lNilCtx.Release: %v", relNilErr)
	}
	pool.WaitBackground()

	if errPool.AvailableCount() != 1 || pool.AvailableCount() != 1 {
		t.Errorf("expected workers released after send error and nil turnCtx")
	}
}

func verifyOnDemandLease(ctx context.Context, t *testing.T, pool *InterchangeablePool, targetKey, workerKey, prompt string) {
	t.Helper()
	lease, err := pool.AcquireLease(ctx, targetKey)
	if err != nil {
		t.Fatalf("AcquireLease %s error: %v", targetKey, err)
	}
	if d, ok := pool.Underlying().Get(workerKey); !ok || d == nil {
		t.Fatalf("expected on-demand worker %s active", workerKey)
	}
	if _, err := lease.Execute(ctx, &TurnContext{TurnID: "turn-" + targetKey, Prompt: prompt, Ctx: ctx}); err != nil {
		t.Fatalf("lease %s Execute error: %v", targetKey, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("lease %s Release: %v", targetKey, err)
	}
	pool.WaitBackground()
	if dAfter, ok := pool.Underlying().Get(workerKey); ok || dAfter != nil {
		t.Fatalf("expected on-demand worker %s evicted", workerKey)
	}
}

func TestInterchangeablePool_ConcurrencyAndSaturation(t *testing.T) {
	turnBlockers := make(map[string]chan struct{})
	var mu sync.Mutex

	mock := createTestSpawner(func(prompt string) {
		mu.Lock()
		var blocker chan struct{}
		for k, ch := range turnBlockers {
			if strings.Contains(prompt, k) {
				blocker = ch
				break
			}
		}
		mu.Unlock()
		if blocker != nil {
			<-blocker
		}
	})

	pool := newTestInterchangeablePool(t, 2, "", "", mock)
	ctx := context.Background()

	b1, b2 := make(chan struct{}), make(chan struct{})
	mu.Lock()
	turnBlockers["prompt-1"], turnBlockers["prompt-2"] = b1, b2
	mu.Unlock()

	l1, err := pool.AcquireLease(ctx, "c1")
	if err != nil {
		t.Fatalf("l1 AcquireLease error: %v", err)
	}
	l2, err := pool.AcquireLease(ctx, "c2")
	if err != nil {
		t.Fatalf("l2 AcquireLease error: %v", err)
	}

	if pool.AvailableCount() != 0 {
		t.Fatalf("expected 0 available, got %d", pool.AvailableCount())
	}

	verifyOnDemandLease(ctx, t, pool, "c3", "ephemeral:worker-ondemand-1", "prompt-3")
	verifyOnDemandLease(ctx, t, pool, "c4", "ephemeral:worker-ondemand-2", "custom-prompt")

	close(b1)
	if _, err := l1.Execute(ctx, &TurnContext{TurnID: "t1", Prompt: "prompt-1", Ctx: ctx}); err != nil {
		t.Fatalf("l1.Execute: %v", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("l1.Release: %v", err)
	}
	close(b2)
	if _, err := l2.Execute(ctx, &TurnContext{TurnID: "t2", Prompt: "prompt-2", Ctx: ctx}); err != nil {
		t.Fatalf("l2.Execute: %v", err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("l2.Release: %v", err)
	}
	pool.WaitBackground()
	if pool.AvailableCount() != 2 {
		t.Errorf("expected 2 available workers, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ContextCancellation(t *testing.T) {
	pool := newTestInterchangeablePool(t, 1, "", "", nil)
	ctxCancelled, cancelTimeout := context.WithCancel(context.Background())
	cancelTimeout()
	if _, err2 := pool.AcquireLease(ctxCancelled, "c2"); !errors.Is(err2, context.Canceled) {
		t.Errorf("expected canceled error, got: %v", err2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mockSlow := createTestSpawner(func(string) {
		cancel()
		<-t.Context().Done()
	})
	poolSlow := newTestInterchangeablePool(t, 1, "", "", mockSlow)
	lSlow, err := poolSlow.AcquireLease(ctx, "c-cancel")
	if err != nil {
		t.Fatalf("AcquireLease lSlow: %v", err)
	}
	if _, err := lSlow.Execute(ctx, &TurnContext{TurnID: "t-cancel", Prompt: "long running", Ctx: ctx}); err == nil {
		t.Fatal("expected canceled error")
	}
	if err := lSlow.Release(); err != nil {
		t.Fatalf("lSlow.Release: %v", err)
	}
	poolSlow.WaitBackground()
	if poolSlow.AvailableCount() != 1 {
		t.Errorf("expected worker returned after cancelled turn, got %d", poolSlow.AvailableCount())
	}
}

func TestInterchangeablePool_BoundedOverflowAndQueueing(t *testing.T) {
	t.Parallel()
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	t.Cleanup(func() { _ = underlying.Close() })

	// Pool with 1 prewarmed worker and MaxOverflow: 1
	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
		MaxOverflow: 1,
		KeyPrefix:   "test:overflow",
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx := context.Background()

	// 1. Lease 1 takes the 1 prewarmed worker.
	l1, err := pool.AcquireLease(ctx, "req-1")
	if err != nil {
		t.Fatalf("unexpected error acquiring l1: %v", err)
	}
	if isl1, ok := l1.(*interchangeableSessionLease); !ok || isl1.IsOverflow() {
		t.Errorf("expected l1 to not be overflow")
	}

	// 2. Lease 2 takes the 1 overflow slot.
	l2, err := pool.AcquireLease(ctx, "req-2")
	if err != nil {
		t.Fatalf("unexpected error acquiring l2: %v", err)
	}
	if isl2, ok := l2.(*interchangeableSessionLease); !ok || !isl2.IsOverflow() {
		t.Errorf("expected l2 to be overflow")
	}

	// 3. Lease 3 with timeout should block and fail with DeadlineExceeded because both slots are busy.
	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancelTimeout()
	_, err = pool.AcquireLease(ctxTimeout, "req-3")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded for l3, got: %v", err)
	}

	// 4. Release overflow lease l2. Now an overflow slot opens up.
	if err = l2.Release(); err != nil {
		t.Fatalf("unexpected error releasing l2: %v", err)
	}

	// 5. Now lease 4 can successfully acquire the overflow slot.
	l4, err := pool.AcquireLease(ctx, "req-4")
	if err != nil {
		t.Fatalf("unexpected error acquiring l4: %v", err)
	}
	if isl4, ok := l4.(*interchangeableSessionLease); !ok || !isl4.IsOverflow() {
		t.Errorf("expected l4 to be overflow")
	}
	if err := l4.Release(); err != nil {
		t.Fatalf("l4.Release: %v", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("l1.Release: %v", err)
	}
	pool.WaitBackground()
}

func TestInterchangeablePool_BoundedOverflow_ContextCancellation(t *testing.T) {
	t.Parallel()
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	t.Cleanup(func() { _ = underlying.Close() })

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
		MaxOverflow: 1,
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx := context.Background()
	l1, err := pool.AcquireLease(ctx, "req-1")
	if err != nil {
		t.Fatalf("l1 error: %v", err)
	}
	defer l1.Release()
	l2, err := pool.AcquireLease(ctx, "req-2")
	if err != nil {
		t.Fatalf("l2 error: %v", err)
	}
	defer l2.Release()

	cancelCtx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() {
		_, err := pool.AcquireLease(cancelCtx, "req-wait")
		doneCh <- err
	}()

	cancel()

	select {
	case err := <-doneCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cancelled lease acquisition")
	}
}

func TestInterchangeablePool_BoundedOverflow_WaitingForWorkerReturn(t *testing.T) {
	t.Parallel()
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	t.Cleanup(func() { _ = underlying.Close() })

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
		MaxOverflow: 1,
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx := context.Background()
	l1, err := pool.AcquireLease(ctx, "req-1")
	if err != nil {
		t.Fatalf("l1: %v", err)
	}
	l2, err := pool.AcquireLease(ctx, "req-2")
	if err != nil {
		t.Fatalf("l2: %v", err)
	}
	defer l2.Release()

	l3Ch := make(chan SessionLease, 1)
	errCh := make(chan error, 1)
	go func() {
		l, err := pool.AcquireLease(ctx, "req-3")
		if err != nil {
			errCh <- err
			return
		}
		l3Ch <- l
	}()

	if err := l1.Release(); err != nil {
		t.Fatalf("l1.Release: %v", err)
	}

	select {
	case l3 := <-l3Ch:
		if isl, ok := l3.(*interchangeableSessionLease); !ok || isl.IsOverflow() {
			t.Errorf("expected l3 to acquire prewarmed worker, got overflow: %v", isl.IsOverflow())
		}
		if err := l3.Release(); err != nil {
			t.Fatalf("l3.Release: %v", err)
		}
	case err := <-errCh:
		t.Fatalf("unexpected error for l3: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for l3 to acquire returned prewarmed worker")
	}
}

func TestInterchangeablePool_BoundedOverflow_PoolCloseWhileWaiting(t *testing.T) {
	t.Parallel()
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	t.Cleanup(func() { _ = underlying.Close() })

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
		MaxOverflow: 1,
	})

	ctx := context.Background()
	l1, err := pool.AcquireLease(ctx, "req-1")
	if err != nil {
		t.Fatalf("l1: %v", err)
	}
	defer l1.Release()
	l2, err := pool.AcquireLease(ctx, "req-2")
	if err != nil {
		t.Fatalf("l2: %v", err)
	}
	defer l2.Release()

	errCh := make(chan error, 1)
	go func() {
		_, err := pool.AcquireLease(ctx, "req-3")
		errCh <- err
	}()

	_ = pool.Close()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("expected closed error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pool close to unblock waiter")
	}
}

func TestTurnError_Unwrap(t *testing.T) {
	t.Parallel()
	baseErr := errors.New("underlying error")
	te := &TurnError{Err: baseErr, Usage: AgyUsage{InputTokens: 5}}
	if !errors.Is(te, baseErr) {
		t.Fatalf("expected errors.Is match")
	}
	if u := te.Unwrap(); !errors.Is(u, baseErr) {
		t.Fatalf("expected Unwrap = %v", baseErr)
	}
}

func checkInterchangeableNilPool(ctx context.Context, t *testing.T) {
	t.Helper()
	var nilPool *InterchangeablePool
	_, ngErr := nilPool.AcquireLease(ctx, "t")
	_, neErr := nilPool.ExecuteEphemeral(ctx, "t", "p")
	_, nlErr := nilPool.EphemeralLLMFunc("t")(ctx, "m", "p")
	nilPool.SetTranscriptRescuer(nil)
	nilPool.UpdateModel("claude-sonnet-5-5-low")
	nilPool.UpdateEngine("claude", "claude")
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	nilPool.SetSessionManager(nil)
	nilPool.returnWorker("ephemeral:worker-0")
	if nilPool.Close() != nil || nilPool.Workers() != nil || nilPool.AvailableCount() != 0 || nilPool.Initialize(ctx) == nil || ngErr == nil || neErr == nil || nlErr == nil {
		t.Error("unexpected nilPool accessor returns or missing errors")
	}
}

func checkInterchangeableClosedAndFailurePools(ctx context.Context, t *testing.T) {
	t.Helper()
	poolWaiting := newTestInterchangeablePool(t, 1, "", "", nil)
	wLease, err := poolWaiting.AcquireLease(ctx, "t1")
	if err != nil {
		t.Fatalf("AcquireLease wLease: %v", err)
	}
	wOverflowLease, err := poolWaiting.AcquireLease(ctx, "t2")
	if err != nil {
		t.Fatalf("AcquireLease wOverflowLease: %v", err)
	}
	_ = poolWaiting.Close()
	if err := wLease.Release(); err != nil {
		t.Fatalf("wLease.Release: %v", err)
	}
	if err := wOverflowLease.Release(); err != nil {
		t.Fatalf("wOverflowLease.Release: %v", err)
	}
	poolWaiting.evictOverflow("ephemeral:worker-ondemand-closed")
	runtime.Gosched()

	_, wgErr := poolWaiting.AcquireLease(ctx, "target")
	_, weErr := poolWaiting.ExecuteEphemeral(ctx, "target", "prompt")
	if wgErr == nil || !strings.Contains(wgErr.Error(), "closed") || poolWaiting.Close() != nil || poolWaiting.Initialize(ctx) == nil || weErr == nil {
		t.Errorf("expected closed error on closed poolWaiting: wgErr=%v, weErr=%v", wgErr, weErr)
	}

	poolNoUnderlying := NewInterchangeablePool(nil, InterchangeablePoolConfig{WorkerCount: 1})
	defer poolNoUnderlying.Close()
	_, nuErr := poolNoUnderlying.AcquireLease(ctx, "t")
	failSpawner := &MockDaemonSpawner{SpawnFn: func(context.Context, DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return nil, nil, nil, nil, errors.New("spawn error")
	}}
	failPool := newTestInterchangeablePool(t, 1, "", "", failSpawner)
	_, fErr := failPool.AcquireLease(ctx, "t")
	if poolNoUnderlying.Initialize(ctx) != nil || nuErr == nil || fErr == nil || failPool.AvailableCount() != 1 {
		t.Errorf("expected underlying/spawner errors: nuErr=%v, fErr=%v, avail=%d", nuErr, fErr, failPool.AvailableCount())
	}
}

func TestInterchangeablePool_DelegationAndErrorGuards(t *testing.T) {
	pool := newTestInterchangeablePool(t, 2, "", "gemini-2.5-flash", nil)
	underlying := pool.Underlying()
	ctx := context.Background()

	res, eErr := pool.ExecuteEphemeral(ctx, "any-target", "custom-prompt-1")
	res2, lErr := pool.EphemeralLLMFunc("any-target")(ctx, "model", "custom-prompt-2")
	if eErr != nil || res != "custom-result-ok" || lErr != nil || res2 != "custom-result-ok" {
		t.Fatalf("unexpected ephemeral: res=(%q, %v), llm=(%q, %v)", res, eErr, res2, lErr)
	}
	pool.WaitBackground()

	pool.SetTranscriptRescuer(func(context.Context, string, time.Time) string { return "rescued" })
	pool.UpdateModel("claude-sonnet-5-5-low")
	pool.UpdateEngine("claude", "/usr/bin/claude")
	pool.MarkDirty()
	pool.WaitBackground()
	if underlying.cfg.Model != "claude-sonnet-5-5-low" {
		t.Fatalf("expected underlying model updated to claude-sonnet-5-5-low, got %q", underlying.cfg.Model)
	}
	if underlying.cfg.Engine != "claude" || underlying.cfg.ClaudeBin != "/usr/bin/claude" {
		t.Fatalf("expected underlying engine updated, got %q %q", underlying.cfg.Engine, underlying.cfg.ClaudeBin)
	}
	lease, sErr := pool.AcquireLease(ctx, "any")
	pool.SetSessionManager(nil)
	if sErr != nil {
		t.Fatalf("unexpected AcquireLease error: %v", sErr)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("lease.Release: %v", err)
	}
	pool.WaitBackground()

	checkInterchangeableNilPool(ctx, t)
	checkInterchangeableClosedAndFailurePools(ctx, t)

	var nilDaemon *StreamingDaemon
	if nilDaemon.SessionID() != "" {
		t.Error("unexpected nil daemon SessionID")
	}
}

func checkInterchangeableNilLease(ctx context.Context, t *testing.T) {
	t.Helper()
	var nilLease *interchangeableSessionLease
	nilLease.MarkDirty()
	_, nlExec := nilLease.Execute(ctx, nil)
	if nilLease.SessionID() != "" || nilLease.TurnCount() != 0 || nilLease.PreviousSessionID() != "" || nilLease.IsCold() || !nilLease.IsEphemeral() || nlExec == nil || nilLease.Release() != nil {
		t.Error("unexpected nilLease state")
	}
}

func checkInterchangeableInvalidAcquires(ctx context.Context, t *testing.T) {
	t.Helper()
	var nilPool *InterchangeablePool
	closedPool := newTestInterchangeablePool(t, 1, "", "", nil)
	if err := closedPool.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	_ = closedPool.Close()

	poolCancelled := newTestInterchangeablePool(t, 1, "", "", nil)
	<-poolCancelled.available
	ctxCancelled, cancel := context.WithCancel(ctx)
	cancel()

	pNoUnder, pClosedFlag := &InterchangeablePool{available: make(chan string, 1)}, &InterchangeablePool{available: make(chan string, 1)}
	pNoUnder.available <- "w1"
	pClosedFlag.closed.Store(true)
	pClosedFlag.available <- "w1"

	for name, p := range map[string]*InterchangeablePool{"nil": nilPool, "closed": closedPool, "cancelled": poolCancelled, "noUnder": pNoUnder, "closedFlag": pClosedFlag} {
		c := ctx
		if name == "cancelled" {
			c = ctxCancelled
		}
		if _, aErr := p.AcquireLease(c, "k"); aErr == nil {
			t.Errorf("expected error from %s pool AcquireLease", name)
		}
	}
}

func TestInterchangeablePool_AcquireLeaseAndMetadata(t *testing.T) {
	pool := newTestInterchangeablePool(t, 2, "", "", nil)
	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("Initialize error: %v", err)
	}

	lease, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}
	firstSessionID := lease.SessionID()
	lease.MarkDirty()
	if firstSessionID == "" || lease.PreviousSessionID() != "" || lease.IsCold() || !lease.IsEphemeral() || lease.TurnCount() < 0 {
		t.Fatalf("unexpected lease metadata: id=%q, prev=%q, cold=%v, eph=%v", firstSessionID, lease.PreviousSessionID(), lease.IsCold(), lease.IsEphemeral())
	}

	res, execErr := lease.Execute(ctx, &TurnContext{TurnID: "turn-1", Prompt: "custom-prompt", Ctx: ctx})
	availBefore := pool.AvailableCount()
	relErr1 := lease.Release()
	pool.WaitBackground()
	relErr2 := lease.Release()
	if execErr != nil || res == nil || res.Response != "custom-result-ok" || relErr1 != nil || relErr2 != nil || pool.AvailableCount() != availBefore+1 {
		t.Fatalf("unexpected Execute/Release: execErr=%v, res=%v, rel=(%v, %v), avail=%d", execErr, res, relErr1, relErr2, pool.AvailableCount())
	}

	leaseSum, sumErr := pool.AcquireLease(ctx, "target-key")
	if sumErr != nil {
		t.Fatalf("AcquireLease leaseSum: %v", sumErr)
	}
	if _, execSumErr := leaseSum.Execute(ctx, &TurnContext{TurnID: "turn-sum", Prompt: "custom-prompt", ThreadSummary: "should-be-stripped", Ctx: ctx}); execSumErr != nil {
		t.Fatalf("leaseSum.Execute: %v", execSumErr)
	}
	if err := leaseSum.Release(); err != nil {
		t.Fatalf("leaseSum.Release: %v", err)
	}
	pool.WaitBackground()

	verifyInterchangeableLeaseRotation(ctx, t, pool, firstSessionID)

	checkInterchangeableNilLease(ctx, t)
	checkInterchangeableInvalidAcquires(ctx, t)
}

func verifyInterchangeableLeaseRotation(ctx context.Context, t *testing.T, pool *InterchangeablePool, firstSessionID string) {
	t.Helper()
	lease2, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("AcquireLease lease2: %v", err)
	}
	lease3, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("AcquireLease lease3: %v", err)
	}
	if err := lease2.Release(); err != nil {
		t.Fatalf("lease2.Release: %v", err)
	}
	if err := lease3.Release(); err != nil {
		t.Fatalf("lease3.Release: %v", err)
	}
	pool.WaitBackground()
	if lease2.SessionID() == firstSessionID || lease3.SessionID() == firstSessionID {
		t.Errorf("expected rotated session IDs: first=%q, l2=%q, l3=%q", firstSessionID, lease2.SessionID(), lease3.SessionID())
	}
}

// replySpawner returns a spawner whose daemons announce a session and answer every prompt
// with reply.
func replySpawner(reply string) *MockDaemonSpawner {
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			go func() {
				defer outW.Close()
				if _, err := fmt.Fprintln(outW, `{"event":"init","conversation_id":"550e8400-e29b-41d4-a716-000000000001"}`); err != nil {
					return
				}
				buf := make([]byte, 4096)
				for {
					if _, err := inR.Read(buf); err != nil {
						return
					}
					if _, err := fmt.Fprintln(outW, reply); err != nil {
						return
					}
				}
			}()
			return inW, outR, io.NopCloser(strings.NewReader("")), &MockProcessHandle{pid: 100}, nil
		},
	}
}

type wantEphemeralSpan struct {
	name       string
	parent     string // name of the parent span
	attrs      []attribute.KeyValue
	statusCode codes.Code
	statusDesc string
}

func TestInterchangeablePool_ExecuteEphemeralSpans(t *testing.T) {
	t.Parallel()
	const (
		okReply  = `{"event":"result","result":{"status":"SUCCESS","response":"a caption","usage":{"input_tokens":120,"output_tokens":30,"cache_read_tokens":80}}}`
		errReply = `{"event":"result","result":{"status":"ERROR","error":"model overloaded"}}`
	)
	usage := []attribute.KeyValue{
		attribute.Int("gen_ai.usage.input_tokens", 120), attribute.Int("gen_ai.usage.output_tokens", 30),
		attribute.Int("gen_ai.usage.cache_read.input_tokens", 80), attribute.Int("gen_ai.usage.cache_creation.input_tokens", 0),
	}
	zeroUsage := []attribute.KeyValue{
		attribute.Int("gen_ai.usage.input_tokens", 0), attribute.Int("gen_ai.usage.output_tokens", 0),
		attribute.Int("gen_ai.usage.cache_read.input_tokens", 0), attribute.Int("gen_ai.usage.cache_creation.input_tokens", 0),
	}
	agentAttrs := func(target string, u []attribute.KeyValue) []attribute.KeyValue {
		return append([]attribute.KeyValue{
			attribute.String("gen_ai.operation.name", "invoke_agent"), attribute.String("aerial.ephemeral.target", target),
			attribute.String("gen_ai.request.model", "claude-sonnet-5-5"), attribute.String("gen_ai.provider.name", "anthropic"),
		}, u...)
	}
	promptAttr := attribute.String("gen_ai.input.messages", `[{"role":"user","parts":[{"type":"text","content":"describe this frame"}]}]`)
	captionAttr := attribute.String("gen_ai.output.messages", `[{"role":"assistant","parts":[{"type":"text","content":"a caption"}]}]`)
	emptyReplyAttr := attribute.String("gen_ai.output.messages", `[{"role":"assistant","parts":[{"type":"text","content":""}]}]`)
	tests := []struct {
		name       string
		target     string
		poolClosed bool
		capture    bool // pool records prompt/response content
		reply      string
		wantResp   string
		wantErr    string
		wantDeltas map[string]float64
		wantSpans  []wantEphemeralSpan
	}{
		{
			name: "success nests invoke_agent under caller and chat under invoke_agent", target: "vision:caption-success", reply: okReply, wantResp: "a caption",
			wantDeltas: map[string]float64{"input": 120, "output": 30, "cache_read": 80, "total": 150},
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs("vision:caption-success", usage)},
				{name: "chat", parent: "invoke_agent", attrs: append([]attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, usage...)},
			},
		},
		{
			name: "captured content on success", target: "vision:caption-capture", capture: true, reply: okReply, wantResp: "a caption",
			wantDeltas: map[string]float64{"input": 120, "output": 30, "cache_read": 80, "total": 150},
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: append(agentAttrs("vision:caption-capture", usage), promptAttr, captionAttr)},
				{name: "chat", parent: "invoke_agent", attrs: append([]attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, usage...)},
			},
		},
		{
			name: "captured content on error keeps the prompt", target: "vision:caption-err-capture", capture: true, reply: errReply, wantErr: "model overloaded",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: append(agentAttrs("vision:caption-err-capture", zeroUsage), promptAttr, emptyReplyAttr), statusCode: codes.Error, statusDesc: "model overloaded"},
				{name: "chat", parent: "invoke_agent", attrs: []attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, statusCode: codes.Error, statusDesc: "model overloaded"},
			},
		},
		{
			name: "lease acquire error", target: "vision:caption-lease-err", poolClosed: true,
			wantErr: "failed to acquire leased session: interchangeable pool is closed",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs("vision:caption-lease-err", zeroUsage), statusCode: codes.Error, statusDesc: "failed to acquire leased session: interchangeable pool is closed"},
			},
		},
		{
			name: "execute error", target: "vision:caption-exec-err", reply: errReply, wantErr: "model overloaded",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs("vision:caption-exec-err", zeroUsage), statusCode: codes.Error, statusDesc: "model overloaded"},
				{name: "chat", parent: "invoke_agent", attrs: []attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, statusCode: codes.Error, statusDesc: "model overloaded"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := newTestInterchangeablePool(t, 1, "", "claude-sonnet-5-5", replySpawner(tc.reply))
			pool.capture = tc.capture
			if tc.poolClosed {
				_ = pool.Close()
			}
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			ctx, caller := tp.Tracer("test").Start(context.Background(), "caller")
			defer caller.End()

			targetKey := tc.target
			if targetKey == "" {
				targetKey = "vision:caption"
			}
			channelLabel := metrics.SanitizeChannelLabel(targetKey)

			baseline := make(map[string]float64, len(tc.wantDeltas))
			for tokenType := range tc.wantDeltas {
				baseline[tokenType] = testutil.ToFloat64(metrics.TokensTotal.WithLabelValues(tokenType, "claude-sonnet-5-5", channelLabel))
			}

			resp, err := pool.ExecuteEphemeral(ctx, targetKey, "describe this frame")
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if resp != tc.wantResp || gotErr != tc.wantErr {
				t.Fatalf("ExecuteEphemeral = (%q, %q), want (%q, %q)", resp, gotErr, tc.wantResp, tc.wantErr)
			}
			for tokenType, want := range tc.wantDeltas {
				got := testutil.ToFloat64(metrics.TokensTotal.WithLabelValues(tokenType, "claude-sonnet-5-5", channelLabel)) - baseline[tokenType]
				if got != want {
					t.Errorf("tokens_total{type=%q,model=%q,channel=%q} delta = %v, want %v", tokenType, "claude-sonnet-5-5", channelLabel, got, want)
				}
			}
			assertEphemeralSpans(t, rec.Ended(), caller.SpanContext().SpanID(), tc.wantSpans)
		})
	}
}

// assertEphemeralSpans checks that the ended invoke_agent and chat spans are exactly want, with
// the listed parents, attribute sets and statuses.
func assertEphemeralSpans(t *testing.T, ended []sdktrace.ReadOnlySpan, callerID trace.SpanID, want []wantEphemeralSpan) {
	t.Helper()
	ids := map[string]trace.SpanID{"caller": callerID}
	got := map[string]sdktrace.ReadOnlySpan{}
	var names []string
	for _, s := range ended {
		if s.Name() == OperationInvokeAgent || s.Name() == OperationChat {
			names = append(names, s.Name())
			got[s.Name()] = s
			ids[s.Name()] = s.SpanContext().SpanID()
		}
	}
	wantNames := make([]string, 0, len(want))
	for _, w := range want {
		wantNames = append(wantNames, w.name)
	}
	if fmt.Sprint(sortedCopy(names)) != fmt.Sprint(sortedCopy(wantNames)) {
		t.Fatalf("ended spans = %v, want %v", names, wantNames)
	}
	for _, w := range want {
		s := got[w.name]
		if s.Parent().SpanID() != ids[w.parent] {
			t.Errorf("%s parent = %s, want %s (%s)", w.name, s.Parent().SpanID(), ids[w.parent], w.parent)
		}
		if gotAttrs, wantAttrs := attribute.NewSet(s.Attributes()...), attribute.NewSet(w.attrs...); !gotAttrs.Equals(&wantAttrs) {
			t.Errorf("%s attrs = %v, want %v", w.name, gotAttrs.Encoded(attribute.DefaultEncoder()), wantAttrs.Encoded(attribute.DefaultEncoder()))
		}
		if s.Status().Code != w.statusCode || s.Status().Description != w.statusDesc {
			t.Errorf("%s status = (%v, %q), want (%v, %q)", w.name, s.Status().Code, s.Status().Description, w.statusCode, w.statusDesc)
		}
	}
}

func sortedCopy(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

func TestInterchangeablePool_Model(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pool *InterchangeablePool
		want string
	}{
		{name: "nil underlying", pool: &InterchangeablePool{}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.pool.Model(); got != tc.want {
				t.Errorf("Model() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInterchangeablePool_UnderlyingAndSessionCallbacks(t *testing.T) {
	t.Parallel()
	var nilPool *InterchangeablePool
	if nilPool.Underlying() != nil {
		t.Errorf("expected nil Underlying from nil pool")
	}
	nilPool.SetSessionCallbacks(nil, nil)

	pool := newTestInterchangeablePool(t, 2, "test", "test-model", nil)
	underlying := pool.Underlying()
	if got := pool.Underlying(); got != underlying {
		t.Errorf("Underlying() = %p, want %p", got, underlying)
	}

	getFn := func(ctx context.Context, targetKey string) (SessionRecord, error) {
		return SessionRecord{ActiveSessionID: "sess-123"}, nil
	}
	onRotFn := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
		return nil
	}
	pool.SetSessionCallbacks(getFn, onRotFn)
	underlying.mu.Lock()
	hasGet := underlying.cfg.GetSessionRecord != nil
	hasRot := underlying.cfg.OnSessionRotated != nil
	underlying.mu.Unlock()
	if !hasGet || !hasRot {
		t.Errorf("expected SetSessionCallbacks to configure underlying callbacks: get=%v rot=%v", hasGet, hasRot)
	}
}
