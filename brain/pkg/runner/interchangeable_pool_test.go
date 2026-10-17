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
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", resp)))
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

func sendInterchangeableTurn(t *testing.T, ctx context.Context, sess AgentSession, turnID, prompt string) string {
	t.Helper()
	sink := NewThrowawayTurnSink()
	turn := &TurnContext{TurnID: turnID, SessionID: sess.SessionID(), Sink: sink, CreatedAt: time.Now(), Ctx: ctx}
	if err := sess.Send(prompt, turn); err != nil {
		t.Fatalf("sendInterchangeableTurn (%s) Send failed: %v", turnID, err)
	}
	res, err := sink.ResultContext(ctx)
	if err != nil {
		t.Fatalf("sendInterchangeableTurn (%s) ResultContext failed: %v", turnID, err)
	}
	return res
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
	if len(workers) != 3 || pool.AvailableCount() != 3 || spawns.Load() < 3 {
		t.Fatalf("unexpected pool state: workers=%v, avail=%d, spawns=%d", workers, pool.AvailableCount(), spawns.Load())
	}
	if workers[0] != "custom:worker-0" || workers[1] != "custom:worker-1" || workers[2] != "custom:worker-2" {
		t.Fatalf("unexpected custom worker keys: %v", workers)
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

	// Turns 1 & 2 auto-rotate
	s1, err1 := pool.GetOrCreateSession(ctx, "target-1", "")
	res1 := sendInterchangeableTurn(t, ctx, s1, "turn-1", "hello 1")
	pool.WaitBackground()
	s2, err2 := pool.GetOrCreateSession(ctx, "target-1", "")
	res2 := sendInterchangeableTurn(t, ctx, s2, "turn-2", "hello 2")
	pool.WaitBackground()

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected GetOrCreateSession errors: err1=%v, err2=%v", err1, err2)
	}
	if s1.SessionID() == "" || s2.SessionID() == "" || s1.SessionID() == s2.SessionID() {
		t.Fatalf("expected rotated non-empty session IDs, got id1=%q, id2=%q", s1.SessionID(), s2.SessionID())
	}
	if res1 != "response-to-1" || res2 != "response-to-2" || pool.AvailableCount() != 1 {
		t.Fatalf("unexpected turn responses or availability: res=(%q, %q), avail=%d", res1, res2, pool.AvailableCount())
	}

	// Send error & nil turn context release worker
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
	errSess, _ := errPool.GetOrCreateSession(ctx, "target", "")
	_ = errSess.Send("trigger write error", &TurnContext{TurnID: "t-err", Sink: NewThrowawayTurnSink()})
	errPool.WaitBackground()

	sessNilCtx, _ := pool.GetOrCreateSession(ctx, "target-nil", "")
	_ = sessNilCtx.Send("nil turn context", nil)
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

	s1, _ := pool.GetOrCreateSession(ctx, "c1", "")
	sink1 := NewThrowawayTurnSink()
	_ = s1.Send("prompt-1", &TurnContext{TurnID: "t1", Sink: sink1, Ctx: ctx})

	s2, _ := pool.GetOrCreateSession(ctx, "c2", "")
	sink2 := NewThrowawayTurnSink()
	_ = s2.Send("prompt-2", &TurnContext{TurnID: "t2", Sink: sink2, Ctx: ctx})

	if pool.AvailableCount() != 0 {
		t.Fatalf("expected 0 available, got %d", pool.AvailableCount())
	}

	caller3Acquired, caller3Err := make(chan AgentSession, 1), make(chan error, 1)
	go func() {
		s3, err := pool.GetOrCreateSession(ctx, "c3", "")
		if err != nil {
			caller3Err <- err
			return
		}
		caller3Acquired <- s3
	}()

	select {
	case <-caller3Acquired:
		t.Fatal("caller 3 should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(b1)
	_, _ = sink1.ResultContext(ctx)

	select {
	case s3 := <-caller3Acquired:
		sink3 := NewThrowawayTurnSink()
		_ = s3.Send("prompt-3", &TurnContext{TurnID: "t3", Sink: sink3, Ctx: ctx})
		_, _ = sink3.ResultContext(ctx)
	case err := <-caller3Err:
		t.Fatalf("caller 3 error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for caller 3")
	}

	close(b2)
	_, _ = sink2.ResultContext(ctx)
	pool.WaitBackground()
	if pool.AvailableCount() != 2 {
		t.Errorf("expected 2 available workers, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ContextCancellation(t *testing.T) {
	// Case 1: Cancellation while waiting in line
	pool, _ := newTestInterchangeablePool(t, 1, "", "", nil)
	s1, err := pool.GetOrCreateSession(context.Background(), "c1", "")
	if err != nil {
		t.Fatalf("leasing worker 1: %v", err)
	}
	defer func() {
		if closer, ok := s1.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelTimeout()
	if _, err2 := pool.GetOrCreateSession(ctxTimeout, "c2", ""); !errors.Is(err2, context.DeadlineExceeded) && !errors.Is(err2, context.Canceled) {
		t.Errorf("expected deadline/canceled error, got: %v", err2)
	}

	// Case 2: Cancellation while turn is in-flight
	mockSlow := createTestSpawner(func(string) { select {} })
	poolSlow, _ := newTestInterchangeablePool(t, 1, "", "", mockSlow)
	ctx, cancel := context.WithCancel(context.Background())
	sSlow, _ := poolSlow.GetOrCreateSession(ctx, "c-cancel", "")
	sink := NewThrowawayTurnSink()
	_ = sSlow.Send("long running", &TurnContext{TurnID: "t-cancel", Sink: sink, Ctx: ctx})
	cancel()
	_, _ = sink.ResultContext(context.Background())
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
		t.Fatalf("unexpected ephemeral execution: res=(%q, %v), llm=(%q, %v)", res, eErr, res2, lErr)
	}

	pool.SetTranscriptRescuer(func(string, time.Time) string { return "rescued" })
	pool.MarkDirty()
	pool.WaitBackground()
	if pool.Underlying() != underlying || pool.Model() != "gemini-2.5-flash" {
		t.Fatalf("unexpected Underlying/Model delegation: %v, %q", pool.Underlying(), pool.Model())
	}
	if d, ok := pool.Get("non-existent-worker"); ok || d != nil {
		t.Errorf("expected missing worker lookup to return false/nil, got (%v, %v)", d, ok)
	}
	if d0, ok0 := pool.Get("ephemeral:worker-0"); !ok0 || d0 == nil {
		t.Errorf("expected existing worker-0 lookup to succeed, got (%v, %v)", d0, ok0)
	}

	sess, sErr := pool.GetOrCreateSession(ctx, "any", "")
	should, _ := pool.ShouldRotateSession(sess)
	rotSess, rErr := pool.RotateSession(ctx, "any")
	pool.SetSessionManager(nil)
	pool.SetSessionCallbacks(nil, nil)
	if sErr != nil || should || rErr != nil || rotSess != nil {
		t.Fatalf("unexpected session rotation delegation: sErr=%v, should=%v, rErr=%v, rotSess=%v", sErr, should, rErr, rotSess)
	}

	// Closed pool and nil receiver safety
	var nilPool *InterchangeablePool
	_, ngErr := nilPool.GetOrCreateSession(ctx, "target", "")
	_, neErr := nilPool.ExecuteEphemeral(ctx, "target", "prompt")
	_, nlErr := nilPool.EphemeralLLMFunc("target")(ctx, "model", "prompt")
	nd, ndOk := nilPool.Get("target")
	nilPool.SetTranscriptRescuer(nil)
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	nilPool.SetSessionManager(nil)
	nilPool.SetSessionCallbacks(nil, nil)
	if nilPool.Close() != nil || nilPool.Workers() != nil || nilPool.AvailableCount() != 0 || nilPool.Underlying() != nil || nilPool.Model() != "" {
		t.Error("unexpected nilPool accessor return values")
	}
	if nilPool.Initialize(ctx) == nil || ngErr == nil || ndOk || nd != nil || neErr == nil || nlErr == nil {
		t.Error("expected errors on nilPool operations")
	}

	// Pool closed while waiting in line
	poolWaiting, _ := newTestInterchangeablePool(t, 1, "", "", nil)
	wSess, _ := poolWaiting.GetOrCreateSession(ctx, "t1", "")
	defer func() {
		if closer, ok := wSess.(io.Closer); ok {
			_ = closer.Close()
		}
	}()
	errChan := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err2 := poolWaiting.GetOrCreateSession(context.Background(), "t2", "")
		errChan <- err2
	}()
	<-started
	runtime.Gosched()
	_ = poolWaiting.Close()

	var waitErr error
	select {
	case waitErr = <-errChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for caller")
	}
	_, wgErr := poolWaiting.GetOrCreateSession(ctx, "target", "")
	_, weErr := poolWaiting.ExecuteEphemeral(ctx, "target", "prompt")
	if waitErr == nil || !strings.Contains(waitErr.Error(), "closed") || poolWaiting.Close() != nil {
		t.Errorf("expected closed error for waiting caller, got %v", waitErr)
	}
	if wgErr == nil || poolWaiting.Initialize(ctx) == nil || weErr == nil {
		t.Error("expected closed errors on subsequent poolWaiting calls")
	}

	// Nil underlying & spawner error
	poolNoUnderlying := NewInterchangeablePool(nil, InterchangeablePoolConfig{WorkerCount: 1})
	defer poolNoUnderlying.Close()
	_, nuErr := poolNoUnderlying.GetOrCreateSession(ctx, "t", "")
	failSpawner := &MockDaemonSpawner{SpawnFn: func(context.Context, DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		return nil, nil, nil, nil, errors.New("spawn error")
	}}
	failPool, _ := newTestInterchangeablePool(t, 1, "", "", failSpawner)
	_, fErr := failPool.GetOrCreateSession(ctx, "t", "")
	if poolNoUnderlying.Initialize(ctx) != nil || nuErr == nil || fErr == nil || failPool.AvailableCount() != 1 {
		t.Errorf("expected underlying/spawner errors and worker return: nuErr=%v, fErr=%v, avail=%d", nuErr, fErr, failPool.AvailableCount())
	}

	// Nil session/daemon guards
	nilSess := &leasedSession{}
	var nilDaemon *StreamingDaemon
	closedPool := NewUnifiedProcessPool(PoolConfig{}, createTestSpawner())
	_ = closedPool.Close()
	untypedSess, cpErr := closedPool.GetOrCreateSession(ctx, "target", "")
	if nilSess.SessionID() != "" || nilSess.Send("p", nil) == nil || nilSess.Send("p", &TurnContext{}) == nil || nilDaemon.SessionID() != "" || cpErr == nil || untypedSess != nil {
		t.Error("unexpected nil session/daemon or closed UnifiedProcessPool guard behavior")
	}
}

func TestReleaseTurnSink_ForwardingAndIdempotency(t *testing.T) {
	mockSink := newMockTurnSink()
	var released atomic.Int32
	wrapped := &releaseTurnSink{inner: mockSink, release: func() { released.Add(1) }}

	dispatch := func(s *releaseTurnSink) {
		s.OnTurnStarted()
		s.OnStepStarted(1)
		s.OnThinking()
		s.OnToolCall("my-tool", "my-cmd")
		s.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
		s.OnSkillActivated("self-improvement", "discord")
		s.OnTextDelta("delta-chunk")
		s.OnResult(&TurnResult{Response: "SUCCESS"})
		s.OnError(errors.New("err"))
	}
	dispatch(wrapped)

	mockSink.mu.Lock()
	allEventsOk := mockSink.started && mockSink.thinking == 1 && len(mockSink.toolCalls) == 1 &&
		len(mockSink.completedTools) == 1 && len(mockSink.activatedSkills) == 1 && len(mockSink.deltas) == 1 &&
		mockSink.result != nil && mockSink.result.Response == "SUCCESS" && mockSink.err != nil
	mockSink.mu.Unlock()

	if !allEventsOk || released.Load() != 1 {
		t.Errorf("expected all events forwarded and release once, ok=%v, rel=%d", allEventsOk, released.Load())
	}

	dispatch(&releaseTurnSink{inner: nil, release: func() {}})
}

func TestInterchangeablePool_AcquireLease(t *testing.T) {
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
	_ = lease.TurnCount()
	if firstSessionID == "" || lease.PreviousSessionID() != "" || lease.IsCold() || !lease.IsEphemeral() {
		t.Fatalf("unexpected initial lease metadata: id=%q, prev=%q, cold=%v, eph=%v", firstSessionID, lease.PreviousSessionID(), lease.IsCold(), lease.IsEphemeral())
	}

	res, execErr := lease.Execute(ctx, &TurnContext{TurnID: "turn-1", Prompt: "custom-prompt", Ctx: ctx})
	availBefore := pool.AvailableCount()
	relErr1 := lease.Release()
	pool.WaitBackground()
	relErr2 := lease.Release() // double release idempotency
	if execErr != nil || res == nil || res.Response != "custom-result-ok" || relErr1 != nil || relErr2 != nil || pool.AvailableCount() != availBefore+1 {
		t.Fatalf("unexpected lease Execute/Release outcome: execErr=%v, res=%v, rel=(%v, %v), avail=%d", execErr, res, relErr1, relErr2, pool.AvailableCount())
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
		t.Errorf("expected rotated session IDs on subsequent leases, got first=%q, l2=%q, l3=%q", firstSessionID, lease2.SessionID(), lease3.SessionID())
	}

	// Nil lease & error branches
	var nilLease *interchangeableSessionLease
	_, nlExec := nilLease.Execute(ctx, nil)
	if nilLease.SessionID() != "" || nilLease.TurnCount() != 0 || nilLease.PreviousSessionID() != "" ||
		nilLease.IsCold() || !nilLease.IsEphemeral() || nlExec == nil || nilLease.Release() != nil {
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

	for name, p := range map[string]*InterchangeablePool{
		"nil": nilPool, "closed": closedPool, "cancelled": poolCancelled, "noUnder": pNoUnder, "closedFlag": pClosedFlag,
	} {
		c := ctx
		if name == "cancelled" {
			c = ctxCancelled
		}
		if _, aErr := p.AcquireLease(c, "k"); aErr == nil {
			t.Errorf("expected error from %s pool AcquireLease", name)
		}
	}
}
