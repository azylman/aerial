package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper mock spawner that generates deterministic sequential session UUIDs
// and responds to user prompts with structured result events.
func createTestSpawner() *MockDaemonSpawner {
	var spawnCount atomic.Int64
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			id := spawnCount.Add(1)
			sessUUID := fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", id)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				// Write init event
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessUUID)

				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if n > 0 {
						promptText := string(buf[:n])
						// Respond with SUCCESS result
						resp := fmt.Sprintf("response-to-%d", id)
						if strings.Contains(promptText, "custom-prompt") {
							resp = "custom-result-ok"
						}
						resEvent := fmt.Sprintf("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":%q}}\n", resp)
						_, _ = outW.Write([]byte(resEvent))
					}
				}
			}()

			return inW, outR, errR, &MockProcessHandle{pid: int(id * 100)}, nil
		},
	}
}

func TestInterchangeablePool_InitializationAndPrewarming(t *testing.T) {
	var spawns atomic.Int32
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			id := spawns.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"session_id\":%q}\n", fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", id))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: int(id * 100)}, nil
		},
	}

	underlying := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "test-model",
	}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 3,
		KeyPrefix:   "custom:worker",
	})
	defer pool.Close()

	workers := pool.Workers()
	if len(workers) != 3 {
		t.Fatalf("expected 3 workers, got %d", len(workers))
	}
	expected := []string{"custom:worker-0", "custom:worker-1", "custom:worker-2"}
	for i, w := range workers {
		if w != expected[i] {
			t.Errorf("worker[%d]: expected %s, got %s", i, expected[i], w)
		}
	}
	if pool.AvailableCount() != 3 {
		t.Errorf("expected 3 available workers, got %d", pool.AvailableCount())
	}

	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("pool.Initialize failed: %v", err)
	}

	// Prewarming runs asynchronously in UnifiedProcessPool.Initialize
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && spawns.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if spawns.Load() < 3 {
		t.Errorf("expected at least 3 prewarmed spawns, got %d", spawns.Load())
	}
}

func TestInterchangeablePool_DefaultConfig(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 0,
		KeyPrefix:   "",
	})
	defer pool.Close()

	workers := pool.Workers()
	if len(workers) != 2 {
		t.Fatalf("expected default 2 workers, got %d", len(workers))
	}
	if workers[0] != "ephemeral:worker-0" || workers[1] != "ephemeral:worker-1" {
		t.Errorf("unexpected worker names: %v", workers)
	}
}

func TestInterchangeablePool_SequentialLeaseAndAutoRotate(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	ctx := context.Background()

	// Turn 1
	sess1, err := pool.GetOrCreateSession(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("first GetOrCreateSession failed: %v", err)
	}
	sessID1 := sess1.SessionID()
	if sessID1 == "" {
		t.Fatal("expected non-empty SessionID on first lease")
	}

	sink1 := NewThrowawayTurnSink()
	turn1 := &TurnContext{
		TurnID:    "turn-1",
		SessionID: sessID1,
		Sink:      sink1,
		CreatedAt: time.Now(),
		Ctx:       ctx,
	}
	if err := sess1.Send("hello 1", turn1); err != nil {
		t.Fatalf("first Send failed: %v", err)
	}
	res1, err := sink1.ResultContext(ctx)
	if err != nil {
		t.Fatalf("sink1 ResultContext failed: %v", err)
	}
	if res1 != "response-to-1" {
		t.Errorf("expected response-to-1, got %q", res1)
	}

	// Wait for background rotation to finish and return worker to available
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pool.AvailableCount() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if pool.AvailableCount() != 1 {
		t.Fatalf("expected worker to be returned to pool after rotation, available=%d", pool.AvailableCount())
	}

	// Turn 2
	sess2, err := pool.GetOrCreateSession(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("second GetOrCreateSession failed: %v", err)
	}
	sessID2 := sess2.SessionID()
	if sessID2 == "" {
		t.Fatal("expected non-empty SessionID on second lease")
	}
	if sessID2 == sessID1 {
		t.Errorf("expected fresh rotated session ID, got same: %s", sessID2)
	}

	sink2 := NewThrowawayTurnSink()
	turn2 := &TurnContext{
		TurnID:    "turn-2",
		SessionID: sessID2,
		Sink:      sink2,
		CreatedAt: time.Now(),
		Ctx:       ctx,
	}
	if err := sess2.Send("hello 2", turn2); err != nil {
		t.Fatalf("second Send failed: %v", err)
	}
	res2, err := sink2.ResultContext(ctx)
	if err != nil {
		t.Fatalf("sink2 ResultContext failed: %v", err)
	}
	if res2 != "response-to-2" {
		t.Errorf("expected response-to-2, got %q", res2)
	}
}

func TestInterchangeablePool_ConcurrencyAndSaturation(t *testing.T) {
	// Custom spawner where turn completion is gated by a release channel per turn
	turnBlockers := make(map[string]chan struct{})
	var mu sync.Mutex

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			sessUUID := fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", time.Now().UnixNano()%1000000000000)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessUUID)

				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if n > 0 {
						prompt := string(buf[:n])
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
						_, _ = outW.Write([]byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n"))
					}
				}
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 300}, nil
		},
	}

	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})
	defer pool.Close()

	ctx := context.Background()

	// Setup blockers for turn 1 and turn 2
	b1 := make(chan struct{})
	b2 := make(chan struct{})
	mu.Lock()
	turnBlockers["prompt-1"] = b1
	turnBlockers["prompt-2"] = b2
	mu.Unlock()

	// Caller 1 leases worker 1
	sess1, err1 := pool.GetOrCreateSession(ctx, "caller-1", "")
	if err1 != nil {
		t.Fatalf("caller 1 lease failed: %v", err1)
	}
	sink1 := NewThrowawayTurnSink()
	turn1 := &TurnContext{TurnID: "t1", Sink: sink1, Ctx: ctx}
	if err := sess1.Send("prompt-1", turn1); err != nil {
		t.Fatalf("caller 1 send failed: %v", err)
	}

	// Caller 2 leases worker 2
	sess2, err2 := pool.GetOrCreateSession(ctx, "caller-2", "")
	if err2 != nil {
		t.Fatalf("caller 2 lease failed: %v", err2)
	}
	sink2 := NewThrowawayTurnSink()
	turn2 := &TurnContext{TurnID: "t2", Sink: sink2, Ctx: ctx}
	if err := sess2.Send("prompt-2", turn2); err != nil {
		t.Fatalf("caller 2 send failed: %v", err)
	}

	if pool.AvailableCount() != 0 {
		t.Fatalf("expected 0 available workers, got %d", pool.AvailableCount())
	}

	// Caller 3 tries to lease - must block until one finishes
	caller3Acquired := make(chan AgentSession, 1)
	caller3Err := make(chan error, 1)
	go func() {
		s3, err := pool.GetOrCreateSession(ctx, "caller-3", "")
		if err != nil {
			caller3Err <- err
			return
		}
		caller3Acquired <- s3
	}()

	// Ensure caller 3 is blocked
	select {
	case <-caller3Acquired:
		t.Fatal("caller 3 should have blocked because pool was saturated")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Unblock turn 1
	close(b1)
	if _, err := sink1.ResultContext(ctx); err != nil {
		t.Fatalf("sink 1 failed: %v", err)
	}

	// Caller 3 should now unblock and acquire the rotated worker
	select {
	case s3 := <-caller3Acquired:
		if s3 == nil {
			t.Fatal("caller 3 received nil session")
		}
		// Complete caller 3
		sink3 := NewThrowawayTurnSink()
		_ = s3.Send("prompt-3", &TurnContext{TurnID: "t3", Sink: sink3, Ctx: ctx})
		_, _ = sink3.ResultContext(ctx)
	case err := <-caller3Err:
		t.Fatalf("caller 3 received unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for caller 3 to acquire released worker")
	}

	// Unblock turn 2
	close(b2)
	if _, err := sink2.ResultContext(ctx); err != nil {
		t.Fatalf("sink 2 failed: %v", err)
	}

	// Wait for pool to settle
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pool.AvailableCount() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if pool.AvailableCount() != 2 {
		t.Errorf("expected 2 available workers after all turns, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ContextCancellationWhileWaiting(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	// Caller 1 leases the only worker and holds it
	sess1, err := pool.GetOrCreateSession(context.Background(), "caller-1", "")
	if err != nil {
		t.Fatalf("failed leasing worker 1: %v", err)
	}
	defer func() {
		if closer, ok := sess1.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	// Caller 2 tries to lease with a rapidly expiring context
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err2 := pool.GetOrCreateSession(ctxTimeout, "caller-2", "")
	if !errors.Is(err2, context.DeadlineExceeded) && !errors.Is(err2, context.Canceled) {
		t.Errorf("expected deadline exceeded or canceled error, got: %v", err2)
	}
}

func TestInterchangeablePool_ContextCancellationWhileTurnInFlight(t *testing.T) {
	// Spawner where prompt never finishes unless canceled
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440001\"}\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 400}, nil
		},
	}

	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sess, err := pool.GetOrCreateSession(ctx, "caller-cancel", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}

	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{
		TurnID: "t-cancel",
		Sink:   sink,
		Ctx:    ctx,
	}
	if err := sess.Send("long running", turnCtx); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Cancel caller context while turn is executing
	cancel()

	// Wait for safety goroutine to trigger rotation and release worker back to pool
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pool.AvailableCount() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if pool.AvailableCount() != 1 {
		t.Fatalf("expected worker to be released back to pool after context cancellation, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ExecuteEphemeral_And_EphemeralLLMFunc(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "test-model-ephemeral",
	}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})
	defer pool.Close()

	ctx := context.Background()

	// 1. Test ExecuteEphemeral
	res, err := pool.ExecuteEphemeral(ctx, "any-target", "custom-prompt-1")
	if err != nil {
		t.Fatalf("ExecuteEphemeral failed: %v", err)
	}
	if res != "custom-result-ok" {
		t.Errorf("expected 'custom-result-ok', got %q", res)
	}

	// 2. Test EphemeralLLMFunc
	llmFunc := pool.EphemeralLLMFunc("any-target")
	res2, err := llmFunc(ctx, "model", "custom-prompt-2")
	if err != nil {
		t.Fatalf("EphemeralLLMFunc failed: %v", err)
	}
	if res2 != "custom-result-ok" {
		t.Errorf("expected 'custom-result-ok', got %q", res2)
	}

	// 3. Test ExecuteEphemeral error branches
	var nilPool *InterchangeablePool
	if _, err := nilPool.ExecuteEphemeral(ctx, "target", "prompt"); err == nil {
		t.Error("expected error from nilPool.ExecuteEphemeral")
	}
	nilLLM := nilPool.EphemeralLLMFunc("target")
	if _, err := nilLLM(ctx, "model", "prompt"); err == nil {
		t.Error("expected error from nilLLM")
	}

	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.ExecuteEphemeral(ctxCancelled, "target", "prompt"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestInterchangeablePool_ClosedPool(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})

	if err := pool.Close(); err != nil {
		t.Fatalf("pool.Close failed: %v", err)
	}

	// Repeated Close is safe
	if err := pool.Close(); err != nil {
		t.Errorf("repeated pool.Close failed: %v", err)
	}

	ctx := context.Background()
	if _, err := pool.GetOrCreateSession(ctx, "target", ""); err == nil {
		t.Error("expected error on GetOrCreateSession on closed pool")
	}

	if err := pool.Initialize(ctx); err == nil {
		t.Error("expected error on Initialize on closed pool")
	}

	if _, err := pool.ExecuteEphemeral(ctx, "target", "prompt"); err == nil {
		t.Error("expected error on ExecuteEphemeral on closed pool")
	}

	// Nil pool checks
	var nilPool *InterchangeablePool
	if err := nilPool.Close(); err != nil {
		t.Errorf("nilPool.Close returned error: %v", err)
	}
	if err := nilPool.Initialize(ctx); err == nil {
		t.Error("expected error on nilPool.Initialize")
	}
	if _, err := nilPool.GetOrCreateSession(ctx, "target", ""); err == nil {
		t.Error("expected error on nilPool.GetOrCreateSession")
	}
	if nilPool.Workers() != nil {
		t.Errorf("expected nil workers from nil pool")
	}
	if nilPool.AvailableCount() != 0 {
		t.Errorf("expected 0 available from nil pool")
	}
}

func TestStreamingDaemon_SessionID_NilReceiver(t *testing.T) {
	var d *StreamingDaemon
	sessID := d.SessionID()
	if sessID != "" {
		t.Errorf("expected empty string for nil daemon SessionID(), got %q", sessID)
	}
}

func TestUnifiedProcessPool_GetOrCreateSession_UntypedNil(t *testing.T) {
	mock := createTestSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	_ = pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "target", "")
	if err == nil {
		t.Fatal("expected error from closed pool GetOrCreateSession")
	}
	// Verify sess is true nil interface (not typed nil)
	if sess != nil {
		t.Errorf("expected untyped nil interface, got non-nil: %T %v", sess, sess)
	}
}

func TestInterchangeablePool_SendError_ReleasesWorker(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			// Close inW immediately to cause Send write error
			_ = inR.Close()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440002\"}\n"))
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 500}, nil
		},
	}

	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "target", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}

	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{TurnID: "t-err", Sink: sink}
	// Send should fail because stdin is closed
	_ = sess.Send("trigger write error", turnCtx)

	// Worker should be rotated and returned to pool despite error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pool.AvailableCount() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if pool.AvailableCount() != 1 {
		t.Errorf("expected worker to be released after send error, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_TurnCtxNil_ReleasesWorker(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	sess, err := pool.GetOrCreateSession(context.Background(), "target", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}

	// Send with nil TurnContext defers release immediately
	if err := sess.Send("nil turn context", nil); err != nil {
		t.Fatalf("send with nil turn context failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && pool.AvailableCount() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if pool.AvailableCount() != 1 {
		t.Errorf("expected worker to be released after Send with nil turnCtx, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_DelegationMethods(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})
	defer pool.Close()

	if pool.Underlying() != underlying {
		t.Errorf("expected Underlying() to match underlying pool")
	}
	if pool.Model() != "gemini-2.5-flash" {
		t.Errorf("expected model 'gemini-2.5-flash', got %q", pool.Model())
	}

	pool.SetTranscriptRescuer(func(convID string, since time.Time) string {
		return "rescued"
	})
	pool.MarkDirty()
	pool.WaitBackground()

	// Non-existent target
	d, ok := pool.Get("non-existent-worker")
	if ok || d != nil {
		t.Errorf("expected non-existent-worker to not exist, got ok=%v", ok)
	}

	// Worker-0 was prewarmed by MarkDirty
	d0, ok0 := pool.Get("ephemeral:worker-0")
	if !ok0 || d0 == nil {
		t.Errorf("expected worker-0 to be prewarmed after MarkDirty, got ok=%v", ok0)
	}

	// Nil pool delegation checks
	var nilPool *InterchangeablePool
	if nilPool.Underlying() != nil {
		t.Errorf("expected nil Underlying from nil pool")
	}
	if nilPool.Model() != "" {
		t.Errorf("expected empty Model from nil pool")
	}
	nilPool.SetTranscriptRescuer(nil)
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	if d, ok := nilPool.Get("target"); ok || d != nil {
		t.Errorf("expected nil from nilPool.Get")
	}
}

func TestReleaseTurnSink_AllEvents(t *testing.T) {
	var (
		started   atomic.Bool
		thinking  atomic.Bool
		toolName  string
		textDelta string
		resultOk  atomic.Bool
		errorOk   atomic.Bool
		released  atomic.Int32
	)

	mockSink := &testEventSink{
		onStarted:  func() { started.Store(true) },
		onThinking: func() { thinking.Store(true) },
		onTool:     func(tn, cmd string) { toolName = tn },
		onDelta:    func(d string) { textDelta = d },
		onResult:   func(r *TurnResult) { resultOk.Store(true) },
		onError:    func(err error) { errorOk.Store(true) },
	}

	releaseFn := func() {
		released.Add(1)
	}

	wrapped := &releaseTurnSink{
		inner:   mockSink,
		release: releaseFn,
	}

	wrapped.OnTurnStarted()
	if !started.Load() {
		t.Error("OnTurnStarted was not forwarded")
	}

	wrapped.OnThinking()
	if !thinking.Load() {
		t.Error("OnThinking was not forwarded")
	}

	wrapped.OnToolCall("my-tool", "my-cmd")
	if toolName != "my-tool" {
		t.Errorf("expected toolName 'my-tool', got %q", toolName)
	}

	wrapped.OnTextDelta("delta-chunk")
	if textDelta != "delta-chunk" {
		t.Errorf("expected delta 'delta-chunk', got %q", textDelta)
	}

	wrapped.OnResult(&TurnResult{Response: "SUCCESS"})
	if !resultOk.Load() {
		t.Error("OnResult was not forwarded")
	}
	if released.Load() != 1 {
		t.Errorf("expected release called once, got %d", released.Load())
	}

	// Repeated call to OnError should not release again due to releaseOnce
	wrapped.OnError(errors.New("err"))
	if !errorOk.Load() {
		t.Error("OnError was not forwarded")
	}
	if released.Load() != 1 {
		t.Errorf("expected release to remain 1, got %d", released.Load())
	}

	// Nil inner sink should not panic
	nilInner := &releaseTurnSink{inner: nil, release: func() {}}
	nilInner.OnTurnStarted()
	nilInner.OnThinking()
	nilInner.OnToolCall("t", "c")
	nilInner.OnTextDelta("d")
	nilInner.OnResult(&TurnResult{})
	nilInner.OnError(errors.New("err"))
}

type testEventSink struct {
	onStarted  func()
	onThinking func()
	onTool     func(name, cmd string)
	onDelta    func(d string)
	onResult   func(r *TurnResult)
	onError    func(err error)
}

func (s *testEventSink) OnTurnStarted() {
	if s.onStarted != nil {
		s.onStarted()
	}
}

func (s *testEventSink) OnThinking() {
	if s.onThinking != nil {
		s.onThinking()
	}
}

func (s *testEventSink) OnToolCall(name, cmd string) {
	if s.onTool != nil {
		s.onTool(name, cmd)
	}
}

func (s *testEventSink) OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string) {}

func (s *testEventSink) OnSkillActivated(skillName, source string) {}

func (s *testEventSink) OnTextDelta(delta string) {
	if s.onDelta != nil {
		s.onDelta(delta)
	}
}

func (s *testEventSink) OnResult(res *TurnResult) {
	if s.onResult != nil {
		s.onResult(res)
	}
}

func (s *testEventSink) OnError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

func TestInterchangeablePool_ExtraCoverage(t *testing.T) {
	// 1. Pool closed while waiting in line
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: 1})
	sess1, err := pool.GetOrCreateSession(context.Background(), "t1", "")
	if err != nil {
		t.Fatalf("sess1 failed: %v", err)
	}
	defer func() {
		if closer, ok := sess1.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	errChan := make(chan error, 1)
	go func() {
		_, err2 := pool.GetOrCreateSession(context.Background(), "t2", "")
		errChan <- err2
	}()

	time.Sleep(20 * time.Millisecond)
	_ = pool.Close()

	select {
	case err2 := <-errChan:
		if err2 == nil || !strings.Contains(err2.Error(), "closed") {
			t.Errorf("expected closed error, got: %v", err2)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for waiting caller to receive pool closed error")
	}

	// 2. Underlying pool is nil
	poolNoUnderlying := NewInterchangeablePool(nil, InterchangeablePoolConfig{WorkerCount: 1})
	defer poolNoUnderlying.Close()
	if err := poolNoUnderlying.Initialize(context.Background()); err != nil {
		t.Errorf("expected nil error on Initialize when underlying is nil, got: %v", err)
	}
	if _, err := poolNoUnderlying.GetOrCreateSession(context.Background(), "t", ""); err == nil {
		t.Error("expected error when underlying pool is nil")
	}

	// 3. Underlying GetOrCreate returns error
	failMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("spawn error")
		},
	}
	failUnderlying := NewUnifiedProcessPool(PoolConfig{}, failMock)
	defer failUnderlying.Close()
	failPool := NewInterchangeablePool(failUnderlying, InterchangeablePoolConfig{WorkerCount: 1})
	defer failPool.Close()

	if _, err := failPool.GetOrCreateSession(context.Background(), "t", ""); err == nil {
		t.Error("expected spawn error from GetOrCreateSession")
	}
	// Verify worker was returned to available
	if failPool.AvailableCount() != 1 {
		t.Errorf("expected worker to be returned after spawn error, got available=%d", failPool.AvailableCount())
	}

	// 4. leasedSession nil daemon guards
	nilSess := &leasedSession{}
	if sessID := nilSess.SessionID(); sessID != "" {
		t.Errorf("expected empty SessionID on nil daemon leasedSession, got %q", sessID)
	}
	if err := nilSess.Send("prompt", nil); err == nil {
		t.Error("expected error sending to nil daemon leasedSession with nil turnCtx")
	}
	if err := nilSess.Send("prompt", &TurnContext{}); err == nil {
		t.Error("expected error sending to nil daemon leasedSession with turnCtx")
	}
}

func TestReleaseTurnSink_ToolAndSkillForwarding(t *testing.T) {
	mockInner := &mockTurnSink{}
	rs := &releaseTurnSink{
		inner: mockInner,
	}
	rs.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	if len(mockInner.completedTools) != 1 {
		t.Errorf("expected 1 completed tool forwarded, got %d", len(mockInner.completedTools))
	}
	rs.OnSkillActivated("self-improvement", "discord")
	if len(mockInner.activatedSkills) != 1 {
		t.Errorf("expected 1 activated skill forwarded, got %d", len(mockInner.activatedSkills))
	}

	// nil inner does not panic
	nilRS := &releaseTurnSink{inner: nil}
	nilRS.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	nilRS.OnSkillActivated("self-improvement", "discord")
}


func TestInterchangeablePool_SessionRotator(t *testing.T) {
	mockSpawner := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 100}, nil
		},
	}
	upp := NewUnifiedProcessPool(PoolConfig{}, mockSpawner)
	defer upp.Close()

	pool := NewInterchangeablePool(upp, InterchangeablePoolConfig{WorkerCount: 1})
	defer pool.Close()

	ctx := context.Background()
	sess, err := pool.GetOrCreateSession(ctx, "any", "")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}

	if should, _ := pool.ShouldRotateSession(sess); should {
		t.Errorf("expected ShouldRotateSession to return false")
	}

	rotSess, err := pool.RotateSession(ctx, "any")
	if err != nil {
		t.Fatalf("unexpected RotateSession error: %v", err)
	}
	if rotSess != nil {
		t.Errorf("expected RotateSession to return nil for InterchangeablePool")
	}
}
