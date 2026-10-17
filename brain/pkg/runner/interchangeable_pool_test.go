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
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessUUID)

				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if n > 0 {
						promptText := string(buf[:n])
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

func TestInterchangeablePool_InitializationAndConfig(t *testing.T) {
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

	underlying.WaitBackground()
	if spawns.Load() < 3 {
		t.Errorf("expected at least 3 prewarmed spawns, got %d", spawns.Load())
	}

	// Default config behavior
	spawnerDefault := createTestSpawner()
	underlyingDefault := NewUnifiedProcessPool(PoolConfig{}, spawnerDefault)
	defer underlyingDefault.Close()

	poolDefault := NewInterchangeablePool(underlyingDefault, InterchangeablePoolConfig{
		WorkerCount: 0,
		KeyPrefix:   "",
	})
	defer poolDefault.Close()

	defWorkers := poolDefault.Workers()
	if len(defWorkers) != 2 {
		t.Fatalf("expected default 2 workers, got %d", len(defWorkers))
	}
	if defWorkers[0] != "ephemeral:worker-0" || defWorkers[1] != "ephemeral:worker-1" {
		t.Errorf("unexpected worker names: %v", defWorkers)
	}
}

func TestInterchangeablePool_LeaseAutoRotateAndRelease(t *testing.T) {
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

	pool.WaitBackground()
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
	pool.WaitBackground()

	// Send error releases worker
	errMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			_ = inR.Close()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte("{\"event\":\"init\",\"session_id\":\"550e8400-e29b-41d4-a716-446655440002\"}\n"))
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 500}, nil
		},
	}
	errUnderlying := NewUnifiedProcessPool(PoolConfig{}, errMock)
	defer errUnderlying.Close()
	errPool := NewInterchangeablePool(errUnderlying, InterchangeablePoolConfig{WorkerCount: 1})
	defer errPool.Close()

	errSess, err := errPool.GetOrCreateSession(ctx, "target", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	_ = errSess.Send("trigger write error", &TurnContext{TurnID: "t-err", Sink: NewThrowawayTurnSink()})
	errPool.WaitBackground()
	if errPool.AvailableCount() != 1 {
		t.Errorf("expected worker to be released after send error, got %d", errPool.AvailableCount())
	}

	// Nil turn context releases worker immediately
	sessNilCtx, err := pool.GetOrCreateSession(ctx, "target-nil", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if err := sessNilCtx.Send("nil turn context", nil); err != nil {
		t.Fatalf("send with nil turn context failed: %v", err)
	}
	pool.WaitBackground()
	if pool.AvailableCount() != 1 {
		t.Errorf("expected worker to be released after Send with nil turnCtx, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ConcurrencyAndSaturation(t *testing.T) {
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

	b1 := make(chan struct{})
	b2 := make(chan struct{})
	mu.Lock()
	turnBlockers["prompt-1"] = b1
	turnBlockers["prompt-2"] = b2
	mu.Unlock()

	sess1, err1 := pool.GetOrCreateSession(ctx, "caller-1", "")
	if err1 != nil {
		t.Fatalf("caller 1 lease failed: %v", err1)
	}
	sink1 := NewThrowawayTurnSink()
	turn1 := &TurnContext{TurnID: "t1", Sink: sink1, Ctx: ctx}
	if err := sess1.Send("prompt-1", turn1); err != nil {
		t.Fatalf("caller 1 send failed: %v", err)
	}

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

	select {
	case <-caller3Acquired:
		t.Fatal("caller 3 should have blocked because pool was saturated")
	case <-time.After(50 * time.Millisecond):
	}

	close(b1)
	if _, err := sink1.ResultContext(ctx); err != nil {
		t.Fatalf("sink 1 failed: %v", err)
	}

	select {
	case s3 := <-caller3Acquired:
		if s3 == nil {
			t.Fatal("caller 3 received nil session")
		}
		sink3 := NewThrowawayTurnSink()
		_ = s3.Send("prompt-3", &TurnContext{TurnID: "t3", Sink: sink3, Ctx: ctx})
		_, _ = sink3.ResultContext(ctx)
	case err := <-caller3Err:
		t.Fatalf("caller 3 received unexpected error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for caller 3 to acquire released worker")
	}

	close(b2)
	if _, err := sink2.ResultContext(ctx); err != nil {
		t.Fatalf("sink 2 failed: %v", err)
	}

	pool.WaitBackground()
	if pool.AvailableCount() != 2 {
		t.Errorf("expected 2 available workers after all turns, got %d", pool.AvailableCount())
	}
}

func TestInterchangeablePool_ContextCancellation(t *testing.T) {
	// Case 1: Cancellation while waiting in line
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer pool.Close()

	sess1, err := pool.GetOrCreateSession(context.Background(), "caller-1", "")
	if err != nil {
		t.Fatalf("failed leasing worker 1: %v", err)
	}
	defer func() {
		if closer, ok := sess1.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelTimeout()

	_, err2 := pool.GetOrCreateSession(ctxTimeout, "caller-2", "")
	if !errors.Is(err2, context.DeadlineExceeded) && !errors.Is(err2, context.Canceled) {
		t.Errorf("expected deadline exceeded or canceled error, got: %v", err2)
	}

	// Case 2: Cancellation while turn is in-flight
	mockSlow := &MockDaemonSpawner{
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

	underlyingSlow := NewUnifiedProcessPool(PoolConfig{}, mockSlow)
	defer underlyingSlow.Close()

	poolSlow := NewInterchangeablePool(underlyingSlow, InterchangeablePoolConfig{
		WorkerCount: 1,
	})
	defer poolSlow.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sessSlow, err := poolSlow.GetOrCreateSession(ctx, "caller-cancel", "")
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}

	sink := NewThrowawayTurnSink()
	turnCtx := &TurnContext{
		TurnID: "t-cancel",
		Sink:   sink,
		Ctx:    ctx,
	}
	if err := sessSlow.Send("long running", turnCtx); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	cancel()
	<-sessSlow.(*leasedSession).completed
	poolSlow.WaitBackground()

	if poolSlow.AvailableCount() != 1 {
		t.Fatalf("expected worker to be released back to pool after context cancellation, got %d", poolSlow.AvailableCount())
	}
}

func TestInterchangeablePool_DelegationAndErrorGuards(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mock)
	defer underlying.Close()

	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})
	defer pool.Close()

	ctx := context.Background()

	// ExecuteEphemeral & EphemeralLLMFunc
	res, err := pool.ExecuteEphemeral(ctx, "any-target", "custom-prompt-1")
	if err != nil {
		t.Fatalf("ExecuteEphemeral failed: %v", err)
	}
	if res != "custom-result-ok" {
		t.Errorf("expected 'custom-result-ok', got %q", res)
	}

	llmFunc := pool.EphemeralLLMFunc("any-target")
	res2, err := llmFunc(ctx, "model", "custom-prompt-2")
	if err != nil {
		t.Fatalf("EphemeralLLMFunc failed: %v", err)
	}
	if res2 != "custom-result-ok" {
		t.Errorf("expected 'custom-result-ok', got %q", res2)
	}

	// Delegation methods
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

	d, ok := pool.Get("non-existent-worker")
	if ok || d != nil {
		t.Errorf("expected non-existent-worker to not exist, got ok=%v", ok)
	}
	d0, ok0 := pool.Get("ephemeral:worker-0")
	if !ok0 || d0 == nil {
		t.Errorf("expected worker-0 to be prewarmed after MarkDirty, got ok=%v", ok0)
	}

	// SessionRotator
	sess, err := pool.GetOrCreateSession(ctx, "any", "")
	if err != nil {
		t.Fatalf("unexpected GetOrCreateSession error: %v", err)
	}
	if should, _ := pool.ShouldRotateSession(sess); should {
		t.Errorf("expected ShouldRotateSession to return false")
	}
	rotSess, err := pool.RotateSession(ctx, "any")
	if err != nil || rotSess != nil {
		t.Errorf("expected RotateSession to return nil, nil")
	}

	// SetSessionCallbacks
	pool.SetSessionManager(nil)
	pool.SetSessionCallbacks(nil, nil)

	// Closed pool and nil receiver safety
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
	if nilPool.Workers() != nil || nilPool.AvailableCount() != 0 || nilPool.Underlying() != nil || nilPool.Model() != "" {
		t.Errorf("unexpected nilPool state")
	}
	nilPool.SetTranscriptRescuer(nil)
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	nilPool.SetSessionManager(nil)
	nilPool.SetSessionCallbacks(nil, nil)
	if d, ok := nilPool.Get("target"); ok || d != nil {
		t.Errorf("expected nil from nilPool.Get")
	}
	if _, err := nilPool.ExecuteEphemeral(ctx, "target", "prompt"); err == nil {
		t.Error("expected error from nilPool.ExecuteEphemeral")
	}
	if _, err := nilPool.EphemeralLLMFunc("target")(ctx, "model", "prompt"); err == nil {
		t.Error("expected error from nilLLM")
	}

	// Pool closed while waiting in line
	poolWaiting := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: 1})
	wSess, err := poolWaiting.GetOrCreateSession(ctx, "t1", "")
	if err != nil {
		t.Fatalf("sess1 failed: %v", err)
	}
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

	select {
	case err2 := <-errChan:
		if err2 == nil || !strings.Contains(err2.Error(), "closed") {
			t.Errorf("expected closed error, got: %v", err2)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for waiting caller to receive pool closed error")
	}

	if err := poolWaiting.Close(); err != nil {
		t.Errorf("repeated pool.Close failed: %v", err)
	}
	if _, err := poolWaiting.GetOrCreateSession(ctx, "target", ""); err == nil {
		t.Error("expected error on GetOrCreateSession on closed pool")
	}
	if err := poolWaiting.Initialize(ctx); err == nil {
		t.Error("expected error on Initialize on closed pool")
	}
	if _, err := poolWaiting.ExecuteEphemeral(ctx, "target", "prompt"); err == nil {
		t.Error("expected error on ExecuteEphemeral on closed pool")
	}

	// Nil underlying pool
	poolNoUnderlying := NewInterchangeablePool(nil, InterchangeablePoolConfig{WorkerCount: 1})
	defer poolNoUnderlying.Close()
	if err := poolNoUnderlying.Initialize(ctx); err != nil {
		t.Errorf("expected nil error on Initialize when underlying is nil, got: %v", err)
	}
	if _, err := poolNoUnderlying.GetOrCreateSession(ctx, "t", ""); err == nil {
		t.Error("expected error when underlying pool is nil")
	}

	// Underlying GetOrCreate error
	failMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("spawn error")
		},
	}
	failUnderlying := NewUnifiedProcessPool(PoolConfig{}, failMock)
	defer failUnderlying.Close()
	failPool := NewInterchangeablePool(failUnderlying, InterchangeablePoolConfig{WorkerCount: 1})
	defer failPool.Close()

	if _, err := failPool.GetOrCreateSession(ctx, "t", ""); err == nil {
		t.Error("expected spawn error from GetOrCreateSession")
	}
	if failPool.AvailableCount() != 1 {
		t.Errorf("expected worker to be returned after spawn error, got available=%d", failPool.AvailableCount())
	}

	// leasedSession nil daemon guards
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

	// StreamingDaemon nil SessionID and UnifiedProcessPool UntypedNil
	var nilDaemon *StreamingDaemon
	if nilDaemon.SessionID() != "" {
		t.Errorf("expected empty string for nil daemon SessionID()")
	}

	closedPool := NewUnifiedProcessPool(PoolConfig{}, mock)
	_ = closedPool.Close()
	untypedSess, err := closedPool.GetOrCreateSession(ctx, "target", "")
	if err == nil {
		t.Fatal("expected error from closed pool GetOrCreateSession")
	}
	if untypedSess != nil {
		t.Errorf("expected untyped nil interface, got non-nil: %T %v", untypedSess, untypedSess)
	}
}

func TestReleaseTurnSink_ForwardingAndIdempotency(t *testing.T) {
	mockSink := newMockTurnSink()
	var released atomic.Int32
	releaseFn := func() {
		released.Add(1)
	}

	wrapped := &releaseTurnSink{
		inner:   mockSink,
		release: releaseFn,
	}

	wrapped.OnTurnStarted()
	mockSink.mu.Lock()
	if !mockSink.started {
		t.Error("OnTurnStarted was not forwarded")
	}
	mockSink.mu.Unlock()

	wrapped.OnThinking()
	mockSink.mu.Lock()
	if mockSink.thinking != 1 {
		t.Error("OnThinking was not forwarded")
	}
	mockSink.mu.Unlock()

	wrapped.OnToolCall("my-tool", "my-cmd")
	mockSink.mu.Lock()
	if len(mockSink.toolCalls) != 1 || mockSink.toolCalls[0] != "my-tool:my-cmd" {
		t.Errorf("expected toolCall 'my-tool:my-cmd', got %v", mockSink.toolCalls)
	}
	mockSink.mu.Unlock()

	wrapped.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	mockSink.mu.Lock()
	if len(mockSink.completedTools) != 1 {
		t.Errorf("expected 1 completed tool forwarded, got %d", len(mockSink.completedTools))
	}
	mockSink.mu.Unlock()

	wrapped.OnSkillActivated("self-improvement", "discord")
	mockSink.mu.Lock()
	if len(mockSink.activatedSkills) != 1 {
		t.Errorf("expected 1 activated skill forwarded, got %d", len(mockSink.activatedSkills))
	}
	mockSink.mu.Unlock()

	wrapped.OnTextDelta("delta-chunk")
	mockSink.mu.Lock()
	if len(mockSink.deltas) != 1 || mockSink.deltas[0] != "delta-chunk" {
		t.Errorf("expected delta 'delta-chunk', got %v", mockSink.deltas)
	}
	mockSink.mu.Unlock()

	wrapped.OnResult(&TurnResult{Response: "SUCCESS"})
	mockSink.mu.Lock()
	if mockSink.result == nil || mockSink.result.Response != "SUCCESS" {
		t.Error("OnResult was not forwarded")
	}
	mockSink.mu.Unlock()
	if released.Load() != 1 {
		t.Errorf("expected release called once, got %d", released.Load())
	}

	// Repeated call to OnError should not release again due to releaseOnce
	wrapped.OnError(errors.New("err"))
	mockSink.mu.Lock()
	if mockSink.err == nil {
		t.Error("OnError was not forwarded")
	}
	mockSink.mu.Unlock()
	if released.Load() != 1 {
		t.Errorf("expected release to remain 1, got %d", released.Load())
	}

	// Nil inner sink should not panic
	nilInner := &releaseTurnSink{inner: nil, release: func() {}}
	nilInner.OnTurnStarted()
	nilInner.OnThinking()
	nilInner.OnToolCall("t", "c")
	nilInner.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	nilInner.OnSkillActivated("self-improvement", "discord")
	nilInner.OnTextDelta("d")
	nilInner.OnResult(&TurnResult{})
	nilInner.OnError(errors.New("err"))
}

func TestInterchangeablePool_AcquireLease(t *testing.T) {
	mock := createTestSpawner()
	underlying := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer underlying.Close()
	pool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{
		WorkerCount: 2,
	})
	defer pool.Close()
	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}

	lease, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("AcquireLease error: %v", err)
	}

	firstSessionID := lease.SessionID()
	if firstSessionID == "" {
		t.Error("expected non-empty SessionID")
	}
	if lease.PreviousSessionID() != "" {
		t.Errorf("expected empty PreviousSessionID on ephemeral lease, got %q", lease.PreviousSessionID())
	}
	if lease.IsCold() {
		t.Error("expected IsCold() == false on ephemeral lease")
	}
	if !lease.IsEphemeral() {
		t.Error("expected IsEphemeral() == true on interchangeable lease")
	}
	_ = lease.TurnCount()

	turnCtx := &TurnContext{
		TurnID: "turn-1",
		Prompt: "custom-prompt",
		Ctx:    ctx,
	}
	res, execErr := lease.Execute(ctx, turnCtx)
	if execErr != nil {
		t.Fatalf("unexpected Execute error: %v", execErr)
	}
	if res == nil || res.Response != "custom-result-ok" {
		t.Errorf("expected 'custom-result-ok', got %+v", res)
	}

	availBefore := pool.AvailableCount()
	if err := lease.Release(); err != nil {
		t.Fatalf("unexpected Release error: %v", err)
	}
	pool.WaitBackground()
	if pool.AvailableCount() != availBefore+1 {
		t.Errorf("expected worker to be returned to pool after rotation, avail: %d", pool.AvailableCount())
	}

	// Idempotent double release
	if err := lease.Release(); err != nil {
		t.Errorf("unexpected error on second Release: %v", err)
	}

	// Verify ThreadSummary is stripped on Execute
	leaseSum, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("AcquireLease for summary check error: %v", err)
	}
	_, _ = leaseSum.Execute(ctx, &TurnContext{
		TurnID:        "turn-sum",
		Prompt:        "custom-prompt",
		ThreadSummary: "should-be-stripped",
		Ctx:           ctx,
	})
	_ = leaseSum.Release()
	pool.WaitBackground()

	// Nil safety on interchangeableSessionLease
	var nilLease *interchangeableSessionLease
	if nilLease.SessionID() != "" || nilLease.TurnCount() != 0 || nilLease.PreviousSessionID() != "" || nilLease.IsCold() || !nilLease.IsEphemeral() {
		t.Error("unexpected nilLease state")
	}
	if _, err := nilLease.Execute(ctx, nil); err == nil {
		t.Error("expected error from nilLease.Execute")
	}
	if err := nilLease.Release(); err != nil {
		t.Errorf("unexpected error from nilLease.Release: %v", err)
	}

	// Rotated sessions on re-acquire
	lease2, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("second AcquireLease error: %v", err)
	}
	lease3, err := pool.AcquireLease(ctx, "target-key")
	if err != nil {
		t.Fatalf("third AcquireLease error: %v", err)
	}
	if lease2.SessionID() == firstSessionID || lease3.SessionID() == firstSessionID {
		t.Errorf("expected first worker session %s to have been rotated upon Release, got %s and %s", firstSessionID, lease2.SessionID(), lease3.SessionID())
	}
	_ = lease2.Release()
	_ = lease3.Release()
	pool.WaitBackground()

	// Error branches
	var nilPool *InterchangeablePool
	if _, err := nilPool.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error from nil pool AcquireLease")
	}

	closedPool := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: 1})
	_ = closedPool.Initialize(ctx)
	_ = closedPool.Close()
	if _, err := closedPool.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error from closed pool AcquireLease")
	}

	poolCancelled := NewInterchangeablePool(underlying, InterchangeablePoolConfig{WorkerCount: 1})
	defer poolCancelled.Close()
	<-poolCancelled.available
	ctxCancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := poolCancelled.AcquireLease(ctxCancelled, "k"); err == nil {
		t.Error("expected error from cancelled context AcquireLease")
	}

	poolNoUnderlying := &InterchangeablePool{available: make(chan string, 1)}
	poolNoUnderlying.available <- "w1"
	if _, err := poolNoUnderlying.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error when underlying pool is nil")
	}

	poolClosedFlag := &InterchangeablePool{available: make(chan string, 1)}
	poolClosedFlag.closed.Store(true)
	poolClosedFlag.available <- "w1"
	if _, err := poolClosedFlag.AcquireLease(ctx, "k"); err == nil {
		t.Error("expected error when pool closed flag is set")
	}
}
