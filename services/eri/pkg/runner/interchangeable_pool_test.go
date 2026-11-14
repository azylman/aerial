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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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
	_, _ = fmt.Fprintf(outW, `{"event":"init","conversation_id":%q}`+"\n", sessUUID)
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
		_, _ = fmt.Fprintf(outW, `{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", resp)
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

func checkInterchangeableNilPool(ctx context.Context, t *testing.T) {
	t.Helper()
	var nilPool *InterchangeablePool
	_, ngErr := nilPool.AcquireLease(ctx, "t")
	_, neErr := nilPool.ExecuteEphemeral(ctx, "t", "p")
	_, nlErr := nilPool.EphemeralLLMFunc("t")(ctx, "m", "p")
	nd, ndOk := nilPool.Get("t")
	nilPool.SetTranscriptRescuer(nil)
	nilPool.UpdateModel("claude-sonnet-5-5-low")
	nilPool.UpdateEngine("claude", "claude")
	nilPool.MarkDirty()
	nilPool.WaitBackground()
	nilPool.SetSessionManager(nil)
	nilPool.SetSessionCallbacks(nil, nil)
	nilPool.returnWorker("ephemeral:worker-0")
	if nilPool.Close() != nil || nilPool.Workers() != nil || nilPool.AvailableCount() != 0 || nilPool.Underlying() != nil || nilPool.Initialize(ctx) == nil || ngErr == nil || ndOk || nd != nil || neErr == nil || nlErr == nil {
		t.Error("unexpected nilPool accessor returns or missing errors")
	}
}

func checkInterchangeableClosedAndFailurePools(ctx context.Context, t *testing.T) {
	t.Helper()
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

	checkInterchangeableNilLease(ctx, t)
	checkInterchangeableInvalidAcquires(ctx, t)
}

// promptDeadlineCtx reports context.DeadlineExceeded once expired is closed, so a test can
// make a run's deadline pass at a chosen point without waiting on a clock.
type promptDeadlineCtx struct {
	context.Context

	expired <-chan struct{}
}

func (c promptDeadlineCtx) Done() <-chan struct{} { return c.expired }

func (c promptDeadlineCtx) Err() error {
	select {
	case <-c.expired:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// replySpawner returns a spawner whose daemons announce a session, close prompted on the first
// prompt, and answer every prompt with reply (nothing when reply is empty).
func replySpawner(reply string, prompted chan struct{}) *MockDaemonSpawner {
	var once sync.Once
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = fmt.Fprintln(outW, `{"event":"init","conversation_id":"550e8400-e29b-41d4-a716-000000000001"}`)
				buf := make([]byte, 4096)
				for {
					if _, err := inR.Read(buf); err != nil {
						return
					}
					once.Do(func() { close(prompted) })
					if reply != "" {
						_, _ = fmt.Fprintln(outW, reply)
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
	agentAttrs := func(u []attribute.KeyValue) []attribute.KeyValue {
		return append([]attribute.KeyValue{
			attribute.String("gen_ai.operation.name", "invoke_agent"), attribute.String("aerial.ephemeral.target", "vision:caption"),
			attribute.String("gen_ai.request.model", "claude-sonnet-5-5"), attribute.String("gen_ai.provider.name", "anthropic"),
		}, u...)
	}
	tests := []struct {
		name       string
		traced     bool // ctx carries the caller's span
		poolClosed bool
		reply      string
		deadline   bool // ctx's deadline passes once the daemon receives the prompt
		wantResp   string
		wantErr    string
		wantSpans  []wantEphemeralSpan
	}{
		{
			name: "success nests invoke_agent under caller and chat under invoke_agent", traced: true, reply: okReply, wantResp: "a caption",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs(usage)},
				{name: "chat", parent: "invoke_agent", attrs: append([]attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, usage...)},
			},
		},
		{name: "ctx without a span records nothing", reply: okReply, wantResp: "a caption"},
		{
			name: "lease acquire error", traced: true, poolClosed: true,
			wantErr: "failed to acquire leased session: interchangeable pool is closed",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs(zeroUsage), statusCode: codes.Error, statusDesc: "failed to acquire leased session: interchangeable pool is closed"},
			},
		},
		{
			name: "execute error", traced: true, reply: errReply, wantErr: "model overloaded",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs(zeroUsage), statusCode: codes.Error, statusDesc: "model overloaded"},
				{name: "chat", parent: "invoke_agent", attrs: []attribute.KeyValue{attribute.String("gen_ai.operation.name", "chat")}, statusCode: codes.Error, statusDesc: "model overloaded"},
			},
		},
		{
			name: "ctx deadline", traced: true, deadline: true, wantErr: "context deadline exceeded",
			wantSpans: []wantEphemeralSpan{
				{name: "invoke_agent", parent: "caller", attrs: agentAttrs(zeroUsage), statusCode: codes.Error, statusDesc: "context deadline exceeded"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompted := make(chan struct{})
			pool, _ := newTestInterchangeablePool(t, 1, "", "claude-sonnet-5-5", replySpawner(tc.reply, prompted))
			if tc.poolClosed {
				_ = pool.Close()
			}
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			var expired <-chan struct{}
			if tc.deadline {
				expired = prompted
			}
			ctx := context.Context(promptDeadlineCtx{Context: context.Background(), expired: expired})
			var callerID trace.SpanID
			if tc.traced {
				var caller trace.Span
				ctx, caller = tp.Tracer("test").Start(ctx, "caller")
				defer caller.End()
				callerID = caller.SpanContext().SpanID()
			}

			resp, err := pool.ExecuteEphemeral(ctx, "vision:caption", "describe this frame")
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if resp != tc.wantResp || gotErr != tc.wantErr {
				t.Fatalf("ExecuteEphemeral = (%q, %q), want (%q, %q)", resp, gotErr, tc.wantResp, tc.wantErr)
			}
			assertEphemeralSpans(t, rec.Ended(), callerID, tc.wantSpans)
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
		{name: "nil pool", pool: nil, want: ""},
		{name: "nil underlying", pool: &InterchangeablePool{}, want: ""},
		{name: "configured model", pool: NewInterchangeablePool(NewUnifiedProcessPool(PoolConfig{Model: "gemini-3-flash"}, createTestSpawner()), InterchangeablePoolConfig{}), want: "gemini-3-flash"},
		{name: "default model fallback", pool: NewInterchangeablePool(NewUnifiedProcessPool(PoolConfig{DefaultModel: "claude-haiku-5-5"}, createTestSpawner()), InterchangeablePoolConfig{}), want: "claude-haiku-5-5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.pool != nil {
				t.Cleanup(func() { _ = tc.pool.Close() })
			}
			if got := tc.pool.Model(); got != tc.want {
				t.Errorf("Model() = %q, want %q", got, tc.want)
			}
		})
	}
}
