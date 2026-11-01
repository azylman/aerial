package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
			go func() {
				defer outW.Close()
				_, _ = fmt.Fprintf(outW, `{"event":"init","conversation_id":%q}`+"\n", sessUUID)
				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if n > 0 {
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
						_, _ = fmt.Fprintf(outW, `{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", resp)
					}
				}
			}()
			return inW, outR, io.NopCloser(strings.NewReader("")), &MockProcessHandle{pid: int(id * 100)}, nil
		},
	}
}

func newTestInterchangeablePool(t *testing.T, workers int, prefix, model string, spawner DaemonSpawner) (*InterchangeablePool, *UnifiedProcessPool) {
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
	return pool, underlying
}

func TestInterchangeablePool_InitializationAndConfig(t *testing.T) {
	var spawns atomic.Int32
	mock := createTestSpawner()
	orig := mock.SpawnFn
	mock.SpawnFn = func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		spawns.Add(1)
		return orig(ctx, cfg)
	}
	pool, underlying := newTestInterchangeablePool(t, 3, "custom:worker", "test-model", mock)
	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	underlying.WaitBackground()
	workers := pool.Workers()
	if len(workers) != 3 || pool.AvailableCount() != 3 || spawns.Load() < 3 || workers[0] != "custom:worker-0" || workers[1] != "custom:worker-1" || workers[2] != "custom:worker-2" {
		t.Fatalf("unexpected pool state or worker keys: %v", workers)
	}
	defPool, _ := newTestInterchangeablePool(t, 0, "", "", nil)
	defWorkers := defPool.Workers()
	if len(defWorkers) != 2 || defWorkers[0] != "ephemeral:worker-0" || defWorkers[1] != "ephemeral:worker-1" {
		t.Fatalf("unexpected default worker keys: %v", defWorkers)
	}
}

func TestInterchangeablePool_LeaseAutoRotateAndRelease(t *testing.T) {
	pool, _ := newTestInterchangeablePool(t, 1, "", "", nil)
	ctx := context.Background()

	l1, err1 := pool.AcquireLease(ctx, "target-1")
	res1, _ := l1.Execute(ctx, &TurnContext{TurnID: "turn-1", Prompt: "hello 1", Ctx: ctx})
	_ = l1.Release()
	pool.WaitBackground()
	l2, err2 := pool.AcquireLease(ctx, "target-1")
	res2, _ := l2.Execute(ctx, &TurnContext{TurnID: "turn-2", Prompt: "hello 2", Ctx: ctx})
	_ = l2.Release()
	pool.WaitBackground()

	if err1 != nil || err2 != nil || l1.SessionID() == "" || l2.SessionID() == "" || l1.SessionID() == l2.SessionID() || res1.Response != "response-to-1" || res2.Response != "response-to-2" || pool.AvailableCount() != 1 {
		t.Fatalf("unexpected turn responses or availability: (%v, %v), id=(%q, %q), res=(%v, %v)", err1, err2, l1.SessionID(), l2.SessionID(), res1, res2)
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
	errPool, _ := newTestInterchangeablePool(t, 1, "", "", errMock)
	_, _ = errPool.ExecuteEphemeral(ctx, "target", "trigger write error")
	errPool.WaitBackground()

	lNilCtx, _ := pool.AcquireLease(ctx, "target-nil")
	_, _ = lNilCtx.Execute(ctx, nil)
	_ = lNilCtx.Release()
	pool.WaitBackground()

	if errPool.AvailableCount() != 1 || pool.AvailableCount() != 1 {
		t.Errorf("expected workers released after send error and nil turnCtx")
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

	pool, _ := newTestInterchangeablePool(t, 2, "", "", mock)
	ctx := context.Background()

	b1, b2 := make(chan struct{}), make(chan struct{})
	mu.Lock()
	turnBlockers["prompt-1"], turnBlockers["prompt-2"] = b1, b2
	mu.Unlock()

	l1, _ := pool.AcquireLease(ctx, "c1")
	l2, _ := pool.AcquireLease(ctx, "c2")

	if pool.AvailableCount() != 0 {
		t.Fatalf("expected 0 available, got %d", pool.AvailableCount())
	}

	lease3, err3 := pool.AcquireLease(ctx, "c3")
	if err3 != nil {
		t.Fatalf("caller 3 expected overflow: %v", err3)
	}
	if d3, ok := pool.Get("ephemeral:worker-ondemand-1"); !ok || d3 == nil {
		t.Fatalf("expected on-demand worker active")
	}
	if _, err := lease3.Execute(ctx, &TurnContext{TurnID: "t3", Prompt: "prompt-3", Ctx: ctx}); err != nil {
		t.Fatalf("caller 3 result error: %v", err)
	}
	_ = lease3.Release()
	pool.WaitBackground()
	if d3After, ok := pool.Get("ephemeral:worker-ondemand-1"); ok || d3After != nil {
		t.Fatalf("expected on-demand worker evicted")
	}

	lease4, err4 := pool.AcquireLease(ctx, "c4")
	if err4 != nil {
		t.Fatalf("caller 4 AcquireLease error: %v", err4)
	}
	if d4, ok := pool.Get("ephemeral:worker-ondemand-2"); !ok || d4 == nil {
		t.Fatalf("expected on-demand worker active")
	}
	if _, err := lease4.Execute(ctx, &TurnContext{TurnID: "t4", Prompt: "custom-prompt", Ctx: ctx}); err != nil {
		t.Fatalf("caller 4 Execute error: %v", err)
	}
	_ = lease4.Release()
	pool.WaitBackground()
	if d4After, ok := pool.Get("ephemeral:worker-ondemand-2"); ok || d4After != nil {
		t.Fatalf("expected on-demand worker evicted")
	}

	close(b1)
	_, _ = l1.Execute(ctx, &TurnContext{TurnID: "t1", Prompt: "prompt-1", Ctx: ctx})
	_ = l1.Release()
	close(b2)
	_, _ = l2.Execute(ctx, &TurnContext{TurnID: "t2", Prompt: "prompt-2", Ctx: ctx})
	_ = l2.Release()
	pool.WaitBackground()
	if pool.AvailableCount() != 2 {
		t.Errorf("expected 2 available workers, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ContextCancellation(t *testing.T) {
	pool, _ := newTestInterchangeablePool(t, 1, "", "", nil)
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
	poolSlow, _ := newTestInterchangeablePool(t, 1, "", "", mockSlow)
	lSlow, _ := poolSlow.AcquireLease(ctx, "c-cancel")
	_, _ = lSlow.Execute(ctx, &TurnContext{TurnID: "t-cancel", Prompt: "long running", Ctx: ctx})
	_ = lSlow.Release()
	poolSlow.WaitBackground()
	if poolSlow.AvailableCount() != 1 {
		t.Errorf("expected worker returned after cancelled turn, got %d", poolSlow.AvailableCount())
	}
}

func TestInterchangeablePool_DelegationAndErrorGuards(t *testing.T) {
	pool, underlying := newTestInterchangeablePool(t, 2, "", "gemini-2.5-flash", nil)
	ctx := context.Background()

	res, eErr := pool.ExecuteEphemeral(ctx, "any-target", "custom-prompt-1")
	res2, lErr := pool.EphemeralLLMFunc("any-target")(ctx, "model", "custom-prompt-2")
	if eErr != nil || res != "custom-result-ok" || lErr != nil || res2 != "custom-result-ok" {
		t.Fatalf("unexpected ephemeral: res=(%q, %v), llm=(%q, %v)", res, eErr, res2, lErr)
	}
	pool.WaitBackground()

	pool.SetTranscriptRescuer(func(string, time.Time) string { return "rescued" })
	pool.MarkDirty()
	pool.WaitBackground()
	if pool.Underlying() != underlying {
		t.Fatalf("unexpected Underlying: %v", pool.Underlying())
	}
	if d, ok := pool.Get("non-existent"); ok || d != nil {
		t.Errorf("expected missing worker false/nil, got (%v, %v)", d, ok)
	}
	if d0, ok0 := pool.Get("ephemeral:worker-0"); !ok0 || d0 == nil {
		t.Errorf("expected worker-0 lookup success, got (%v, %v)", d0, ok0)
	}

	lease, sErr := pool.AcquireLease(ctx, "any")
	pool.SetSessionManager(nil)
	pool.SetSessionCallbacks(nil, nil)
	if sErr != nil {
		t.Fatalf("unexpected AcquireLease error: %v", sErr)
	}
	_ = lease.Release()
	pool.WaitBackground()

	var nilPool *InterchangeablePool
	_, ngErr := nilPool.AcquireLease(ctx, "t")
	_, neErr := nilPool.ExecuteEphemeral(ctx, "t", "p")
	_, nlErr := nilPool.EphemeralLLMFunc("t")(ctx, "m", "p")
	nd, ndOk := nilPool.Get("t")
	nilPool.SetTranscriptRescuer(nil)
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	nilPool.SetSessionManager(nil)
	nilPool.SetSessionCallbacks(nil, nil)
	nilPool.returnWorker("ephemeral:worker-0")
	if nilPool.Close() != nil || nilPool.Workers() != nil || nilPool.AvailableCount() != 0 || nilPool.Underlying() != nil || nilPool.Initialize(ctx) == nil || ngErr == nil || ndOk || nd != nil || neErr == nil || nlErr == nil {
		t.Error("unexpected nilPool accessor returns or missing errors")
	}

	poolWaiting, _ := newTestInterchangeablePool(t, 1, "", "", nil)
	wLease, _ := poolWaiting.AcquireLease(ctx, "t1")
	wOverflowLease, _ := poolWaiting.AcquireLease(ctx, "t2")
	_ = poolWaiting.Close()
	_ = wLease.Release()
	_ = wOverflowLease.Release()
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
	failPool, _ := newTestInterchangeablePool(t, 1, "", "", failSpawner)
	_, fErr := failPool.AcquireLease(ctx, "t")
	if poolNoUnderlying.Initialize(ctx) != nil || nuErr == nil || fErr == nil || failPool.AvailableCount() != 1 {
		t.Errorf("expected underlying/spawner errors: nuErr=%v, fErr=%v, avail=%d", nuErr, fErr, failPool.AvailableCount())
	}

	var nilDaemon *StreamingDaemon
	if nilDaemon.SessionID() != "" {
		t.Error("unexpected nil daemon SessionID")
	}
}

func TestInterchangeablePool_AcquireLeaseAndMetadata(t *testing.T) {
	pool, _ := newTestInterchangeablePool(t, 2, "", "", nil)
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

	leaseSum, _ := pool.AcquireLease(ctx, "target-key")
	_, _ = leaseSum.Execute(ctx, &TurnContext{TurnID: "turn-sum", Prompt: "custom-prompt", ThreadSummary: "should-be-stripped", Ctx: ctx})
	_ = leaseSum.Release()
	pool.WaitBackground()

	lease2, _ := pool.AcquireLease(ctx, "target-key")
	lease3, _ := pool.AcquireLease(ctx, "target-key")
	_ = lease2.Release()
	_ = lease3.Release()
	pool.WaitBackground()
	if lease2.SessionID() == firstSessionID || lease3.SessionID() == firstSessionID {
		t.Errorf("expected rotated session IDs: first=%q, l2=%q, l3=%q", firstSessionID, lease2.SessionID(), lease3.SessionID())
	}

	var nilLease *interchangeableSessionLease
	nilLease.MarkDirty()
	_, nlExec := nilLease.Execute(ctx, nil)
	if nilLease.SessionID() != "" || nilLease.TurnCount() != 0 || nilLease.PreviousSessionID() != "" || nilLease.IsCold() || !nilLease.IsEphemeral() || nlExec == nil || nilLease.Release() != nil {
		t.Error("unexpected nilLease state")
	}

	var nilPool *InterchangeablePool
	closedPool, _ := newTestInterchangeablePool(t, 1, "", "", nil)
	_ = closedPool.Initialize(ctx)
	_ = closedPool.Close()

	poolCancelled, _ := newTestInterchangeablePool(t, 1, "", "", nil)
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
