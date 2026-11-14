package queue

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

const (
	traceA      = "4bf92f3577b34da6a3ce929d0e0e4736"
	spanA       = "00f067aa0ba902b7"
	traceparA   = "00-" + traceA + "-" + spanA + "-01"
	traceB      = "5bf92f3577b34da6a3ce929d0e0e4737"
	traceparB   = "00-" + traceB + "-00f067aa0ba902b8-01"
	traceC      = "6bf92f3577b34da6a3ce929d0e0e4738"
	traceparC   = "00-" + traceC + "-00f067aa0ba902b9-01"
	malformedTP = "00-not-a-trace-01"
)

type startTurnSpanCase struct {
	name         string
	items        []queuedTurnItem
	wantTrace    string // "" means a fresh root trace
	wantLinks    []string
	wantWait     bool
	wantStart    time.Time
	agentName    string
	wantAgent    bool
	wantRestarts int
	wantRetries  int
	wantResumed  bool
	wantFirstMs  int64 // aerial.queue.first_enqueued_unix_ms; 0 means absent
}

func assertTurnSpanAttrs(t *testing.T, tc startTurnSpanCase, root sdktrace.ReadOnlySpan) {
	t.Helper()
	have := attrMap(root.Attributes())
	if have[runner.AttrGenAIConversationID] != attribute.StringValue("thread-1") || have[runner.AttrGenAIOperationName] != attribute.StringValue("invoke_agent") {
		t.Errorf("root attrs = %v", root.Attributes())
	}
	if _, ok := have[runner.AttrGenAIAgentName]; ok != tc.wantAgent {
		t.Errorf("agent name present = %v, want %v", ok, tc.wantAgent)
	}
	if have[attrRestartCount] != attribute.IntValue(tc.wantRestarts) || have[attrRetryCount] != attribute.IntValue(tc.wantRetries) {
		t.Errorf("restart/retry attrs = %v/%v, want %d/%d", have[attrRestartCount], have[attrRetryCount], tc.wantRestarts, tc.wantRetries)
	}
	if resumed, ok := have[attrResumed]; ok != tc.wantResumed || (ok && !resumed.AsBool()) {
		t.Errorf("resumed attr = %v (present %v), want present %v", resumed, ok, tc.wantResumed)
	}
	firstMs, ok := have[attrFirstEnqueuedMs]
	if ok != (tc.wantFirstMs != 0) || (ok && firstMs.AsInt64() != tc.wantFirstMs) {
		t.Errorf("first_enqueued attr = %v (present %v), want %d", firstMs, ok, tc.wantFirstMs)
	}
}

func runStartTurnSpanCase(t *testing.T, tc startTurnSpanCase, pickup time.Time) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := startTurnSpan(context.Background(), tp.Tracer("test"), turnSpanSpec{items: tc.items, conversationID: "thread-1", agentName: tc.agentName, triggerType: "discord", pickup: pickup})
	if trace.SpanFromContext(ctx) != span {
		t.Fatalf("returned context does not carry the turn span")
	}
	span.End()

	var root, wait sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.Name() {
		case runner.OperationInvokeAgent:
			root = s
		case spanQueueWait:
			wait = s
		}
	}
	if root == nil {
		t.Fatalf("no invoke_agent span ended")
	}
	gotTrace := root.SpanContext().TraceID().String()
	if tc.wantTrace != "" && (gotTrace != tc.wantTrace || root.Parent().SpanID().String() != spanA) {
		t.Errorf("root trace/parent = %s/%s, want %s/%s", gotTrace, root.Parent().SpanID(), tc.wantTrace, spanA)
	}
	if tc.wantTrace == "" && root.Parent().IsValid() {
		t.Errorf("root has parent %v, want none", root.Parent())
	}
	links := make([]string, 0, len(root.Links()))
	for _, l := range root.Links() {
		links = append(links, l.SpanContext.TraceID().String())
	}
	if fmt.Sprint(links) != fmt.Sprint(tc.wantLinks) {
		t.Errorf("links = %v, want %v", links, tc.wantLinks)
	}
	if !root.StartTime().Equal(tc.wantStart) {
		t.Errorf("root start = %v, want %v", root.StartTime(), tc.wantStart)
	}
	if (wait != nil) != tc.wantWait {
		t.Fatalf("queue.wait present = %v, want %v", wait != nil, tc.wantWait)
	}
	if wait != nil && (!wait.StartTime().Equal(tc.wantStart) || !wait.EndTime().Equal(pickup) || wait.Parent().SpanID() != root.SpanContext().SpanID()) {
		t.Errorf("queue.wait = %v..%v parent %v, want %v..%v under root", wait.StartTime(), wait.EndTime(), wait.Parent().SpanID(), tc.wantStart, pickup)
	}
	assertTurnSpanAttrs(t, tc, root)
}

func TestStartTurnSpan(t *testing.T) {
	t.Parallel()
	pickup := time.Date(2026, 11, 13, 8, 0, 10, 0, time.UTC)
	enq := pickup.Add(-3 * time.Second)
	poolStart := pickup.Add(-5 * time.Second)
	firstEnq := pickup.Add(-40 * time.Minute)
	staleUpdate := firstEnq.Add(time.Minute) // recovery bumps RestartCount without touching UpdatedAt
	tests := []startTurnSpanCase{
		{name: "single message parents to its traceparent", items: []queuedTurnItem{{traceparent: traceparA, createdAt: enq}}, wantTrace: traceA, wantWait: true, wantStart: enq, agentName: "aerial", wantAgent: true},
		{name: "burst links the rest", items: []queuedTurnItem{{traceparent: traceparA, createdAt: enq}, {traceparent: traceparB, createdAt: enq}, {traceparent: malformedTP}, {traceparent: traceparC}}, wantTrace: traceA, wantLinks: []string{traceB, traceC}, wantWait: true, wantStart: enq},
		{name: "untraced first message starts a root and links the others", items: []queuedTurnItem{{createdAt: enq}, {traceparent: traceparB}}, wantLinks: []string{traceB}, wantWait: true, wantStart: enq},
		{name: "no enqueue time skips queue wait", items: []queuedTurnItem{{traceparent: traceparA}}, wantTrace: traceA, wantStart: pickup},
		{name: "clock skew skips queue wait", items: []queuedTurnItem{{traceparent: traceparA, createdAt: pickup.Add(time.Second)}}, wantTrace: traceA, wantStart: pickup},
		{name: "first attempt dispatch waits from creation", items: []queuedTurnItem{dispatchTraceItem(db.Dispatch{Traceparent: traceparA, CreatedAt: enq, UpdatedAt: enq, RetryCount: 1}, poolStart)}, wantTrace: traceA, wantWait: true, wantStart: enq, wantRetries: 1},
		{name: "resumed dispatch with stale updated_at waits from pool start", items: []queuedTurnItem{dispatchTraceItem(db.Dispatch{Traceparent: traceparA, CreatedAt: firstEnq, UpdatedAt: staleUpdate, RestartCount: 1, RetryCount: 2}, poolStart)}, wantTrace: traceA, wantWait: true, wantStart: poolStart, wantRestarts: 1, wantRetries: 2, wantResumed: true, wantFirstMs: firstEnq.UnixMilli()},
		{name: "resumed burst waits from its requeue", items: burstTraceItems([]db.Message{{Traceparent: traceparA, CreatedAt: firstEnq, UpdatedAt: enq, RestartCount: 2}, {Traceparent: traceparB, CreatedAt: firstEnq}}, poolStart), wantTrace: traceA, wantLinks: []string{traceB}, wantWait: true, wantStart: enq, wantRestarts: 2, wantResumed: true, wantFirstMs: firstEnq.UnixMilli()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStartTurnSpanCase(t, tc, pickup)
		})
	}
}

func attrMap(kvs []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestTurnSpanEndAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		model        string
		status       string
		wantProvider string
	}{
		{name: "claude", model: "claude-opus-5-5", status: "success", wantProvider: "anthropic"},
		{name: "gemini", model: "gemini-3-pro", status: "failed", wantProvider: "gcp.gemini"},
		{name: "unknown model has no provider", model: "local-gguf", status: "", wantProvider: ""},
		{name: "no model", model: "", status: "success", wantProvider: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			have := attrMap(turnSpanEndAttributes(tc.model, runner.AgyUsage{InputTokens: 9, OutputTokens: 4, CacheReadTokens: 2, CacheCreationTokens: 1}, tc.status))
			if have[runner.AttrGenAIUsageInputTokens] != attribute.IntValue(9) || have[runner.AttrGenAIUsageOutputTokens] != attribute.IntValue(4) ||
				have[runner.AttrGenAIUsageCacheRead] != attribute.IntValue(2) || have[runner.AttrGenAIUsageCacheCreation] != attribute.IntValue(1) {
				t.Errorf("usage attrs = %v", have)
			}
			if got := have[runner.AttrGenAIProviderName].AsString(); got != tc.wantProvider {
				t.Errorf("provider = %q, want %q", got, tc.wantProvider)
			}
			if got := have[runner.AttrGenAIRequestModel].AsString(); got != tc.model {
				t.Errorf("model = %q, want %q", got, tc.model)
			}
			if got := have[attrTurnStatus].AsString(); got != tc.status {
				t.Errorf("status = %q, want %q", got, tc.status)
			}
		})
	}
}

// rootEndSignal forwards ended spans to rec and closes done when the turn's root span ends.
type rootEndSignal struct {
	*tracetest.SpanRecorder

	done chan struct{}
}

func (r rootEndSignal) OnEnd(s sdktrace.ReadOnlySpan) {
	r.SpanRecorder.OnEnd(s)
	if s.Name() == runner.OperationInvokeAgent && (!s.Parent().IsValid() || s.Parent().IsRemote()) {
		close(r.done)
	}
}

// agyToolTurnSpawner fakes an agy daemon that runs one tool per turn; onTurn (optional) runs
// while each turn is executing, after the tool's start has been read and before it completes.
func agyToolTurnSpawner(onTurn func()) *mockDaemonSpawner {
	return &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, `{"event":"init","conversation_id":%q}`+"\n", "c1111111-2222-3333-4444-555555555555")
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte(`{"event":"step_update","step_update":{"step_index":1,"type":"tool_call","tool_name":"run_command"}}` + "\n"))
					if onTurn != nil {
						onTurn()
					}
					_, _ = outW.Write([]byte(`{"event":"step_update","step_update":{"step_index":1,"type":"tool_call","state":"DONE"}}` + "\n"))
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"done","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}` + "\n"))
				}
			}()
			return inW, outR, errR, newMockProcessHandle(12347), nil
		},
	}
}

type processBurstTraceCase struct {
	name        string
	traceparent string
	wantTrace   string
}

func assertProcessBurstHierarchy(t *testing.T, sig rootEndSignal, byName map[string]sdktrace.ReadOnlySpan, root sdktrace.ReadOnlySpan, rootID trace.SpanID) {
	t.Helper()
	for child, parent := range map[string]string{spanTurnSetup: runner.OperationInvokeAgent, spanLease: spanTurnSetup, "session.lock_wait": spanLease, "daemon.spawn": spanLease, spanPrompt: spanTurnSetup, spanPromptChannelSnapshot: spanPrompt, spanPromptScopeInstr: spanPrompt} {
		s, ok := byName[child]
		if !ok {
			t.Errorf("missing startup span %q", child)
			continue
		}
		if p, ok := byName[parent]; !ok || s.Parent().SpanID() != p.SpanContext().SpanID() {
			t.Errorf("startup span %q not parented under %q", child, parent)
		}
	}
	if setup, chat := byName[spanTurnSetup], byName[runner.OperationChat]; setup != nil && chat != nil && setup.EndTime().After(chat.StartTime()) {
		t.Errorf("turn.setup ends %v after chat starts %v", setup.EndTime(), chat.StartTime())
	}
	if lease := byName[spanLease]; lease != nil {
		leaseAttrs := attrMap(lease.Attributes())
		if leaseAttrs[attrNewConversation] != attribute.BoolValue(true) || leaseAttrs[attrSessionEphemeral] != attribute.BoolValue(false) {
			t.Errorf("session.lease attrs = %v", lease.Attributes())
		}
	}
	for _, child := range []string{spanQueueWait, runner.OperationChat, "execute_tool run_command", spanDeliver} {
		s, ok := byName[child]
		if !ok {
			t.Errorf("missing span %q (got %v)", child, sig.Ended())
			continue
		}
		if s.Parent().SpanID() != rootID || s.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Errorf("span %q not parented under the turn root", child)
		}
	}
}

func runProcessBurstTraceCase(t *testing.T, tc processBurstTraceCase, i int) {
	t.Helper()
	store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, agyToolTurnSpawner(nil))
	defer procPool.Close()

	sig := rootEndSignal{SpanRecorder: tracetest.NewSpanRecorder(), done: make(chan struct{})}
	logs := &syncBuffer{}
	pool := New(newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData }), WorkerPoolConfig{
		ProcessPool:    procPool,
		SessionManager: session.New(tmpHome, tempData),
		Store:          store,
		TimeoutMinutes: 1,
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sig)),
		Logger:         slog.New(tracing.SlogHandler(slog.NewJSONHandler(logs, nil))),
		AgentName:      "aerial",
	})
	defer pool.Stop()

	threadID := fmt.Sprintf("trace-thread-%d", i)
	msg := db.Message{ID: fmt.Sprintf("trace-msg-%d", i), ThreadID: threadID, Content: "hi", Status: db.StatusPending, Traceparent: tc.traceparent, CreatedAt: time.Now().Add(-2 * time.Second)}
	_ = store.InsertMessage(context.Background(), msg)
	pool.Enqueue(msg)
	select {
	case <-sig.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("turn span never ended")
	}

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range sig.Ended() {
		byName[s.Name()] = s
	}
	root := byName[runner.OperationInvokeAgent]
	rootID := root.SpanContext().SpanID()
	assertProcessBurstHierarchy(t, sig, byName, root, rootID)

	if tc.wantTrace != "" && root.SpanContext().TraceID().String() != tc.wantTrace {
		t.Errorf("root trace = %s, want %s", root.SpanContext().TraceID(), tc.wantTrace)
	}
	have := attrMap(root.Attributes())
	if have[runner.AttrGenAIUsageInputTokens] != attribute.IntValue(7) || have[runner.AttrGenAIUsageOutputTokens] != attribute.IntValue(3) ||
		have[runner.AttrGenAIAgentName] != attribute.StringValue("aerial") || have[runner.AttrGenAIProviderName] != attribute.StringValue("gcp.gemini") {
		t.Errorf("root attrs = %v", root.Attributes())
	}
	if root.Status().Code == codes.Error {
		t.Errorf("root status = %v, want ok", root.Status())
	}
	wantLog := fmt.Sprintf(`"msg":"[WorkerPool] 1 message(s) in thread %s completed successfully on attempt 1/3","trace_id":"%s"`, threadID, root.SpanContext().TraceID())
	if !strings.Contains(logs.String(), wantLog) {
		t.Errorf("turn log lines lack the turn trace_id; want %s in:\n%s", wantLog, logs.String())
	}
}

func TestProcessBurst_EmitsTurnTrace(t *testing.T) {
	t.Parallel()
	tests := []processBurstTraceCase{
		{name: "continues ingress trace", traceparent: traceparA, wantTrace: traceA},
		{name: "untraced message starts a root", traceparent: ""},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runProcessBurstTraceCase(t, tc, i)
		})
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestProcessBurst_RegistersActiveTurnDuringExecution(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		target string
	}{
		{name: "discord runtime turn", target: "discord"},
		{name: "ephemeral runtime turn", target: "ephemeral"},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			registry := runner.NewActiveTurnRegistry()
			duringTurn := make(chan trace.SpanContext, 1)
			claimedTool := make(chan trace.SpanContext, 1)
			spawner := agyToolTurnSpawner(func() {
				daemon, _ := registry.SoleDaemonInTarget(tc.target)
				sc, _ := registry.Get(daemon)
				duringTurn <- sc
				tool, _ := registry.ToolSpan(context.Background(), daemon, "", "", "step-1")
				claimedTool <- tool
			})
			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
				DefaultModel:  "gemini-2.5-flash",
				GeminiHomeDir: filepath.Join(tempData, "runtimes", tc.target),
			}, spawner)
			defer procPool.Close()

			sig := rootEndSignal{SpanRecorder: tracetest.NewSpanRecorder(), done: make(chan struct{})}
			pool := New(newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData }), WorkerPoolConfig{
				ProcessPool:    procPool,
				SessionManager: session.New(tmpHome, tempData),
				Store:          store,
				TimeoutMinutes: 1,
				TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sig)),
				ActiveTurns:    registry,
			})
			defer pool.Stop()
			if pool.ActiveTurns() != registry {
				t.Fatalf("ActiveTurns() is not the injected registry")
			}

			msg := db.Message{ID: fmt.Sprintf("active-msg-%d", i), ThreadID: fmt.Sprintf("active-thread-%d", i), Content: "hi", Status: db.StatusPending, CreatedAt: time.Now()}
			_ = store.InsertMessage(context.Background(), msg)
			pool.Enqueue(msg)
			select {
			case <-sig.done:
			case <-time.After(10 * time.Second):
				t.Fatalf("turn span never ended")
			}

			var root, tool sdktrace.ReadOnlySpan
			for _, s := range sig.Ended() {
				switch s.Name() {
				case runner.OperationInvokeAgent:
					root = s
				case "execute_tool run_command":
					tool = s
				}
			}
			if got := <-duringTurn; !got.Equal(root.SpanContext()) {
				t.Errorf("registered span during turn = %v, want turn root %v", got.SpanID(), root.SpanContext().SpanID())
			}
			if got := <-claimedTool; tool == nil || !got.Equal(tool.SpanContext()) {
				t.Errorf("claimed tool span during turn = %v, want the execute_tool run_command span", got.SpanID())
			}
			if daemon, ok := registry.SoleDaemonInTarget(tc.target); ok {
				t.Errorf("turn still registered after completion: %q", daemon)
			}
		})
	}
}

// TestSessionCorruptionRetry_ReregistersActiveTurn proves the registry follows the turn onto
// the replacement lease: the interchangeable pool hands the retry a different worker daemon.
func TestSessionCorruptionRetry_ReregistersActiveTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		workers int
	}{
		{name: "replacement lease on another worker daemon", workers: 2},
		{name: "replacement lease on the same daemon", workers: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			home := filepath.Join(t.TempDir(), "runtimes", "ephemeral")
			procPool := runner.NewInterchangeablePool(
				runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash", GeminiHomeDir: home}, agyToolTurnSpawner(nil)),
				runner.InterchangeablePoolConfig{WorkerCount: tc.workers, KeyPrefix: "ephemeral:worker"})
			defer procPool.Close()
			pool := New(nil, WorkerPoolConfig{Store: dbtest.NewFakeStore(), RetryDelayOverride: time.Nanosecond})
			lease, err := procPool.AcquireLease(context.Background(), "dispatch-1")
			if err != nil {
				t.Fatalf("AcquireLease: %v", err)
			}
			_, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), runner.OperationInvokeAgent)
			te := &turnExecution{pool: pool, threadID: "dispatch-1", burst: []db.Message{{ID: "m-1", ThreadID: "dispatch-1"}}, activePool: procPool, lease: lease, turnSpan: span}
			unmark := te.markTurnActive()
			oldDaemon := lease.(runner.ActiveTurnOwner).ActiveTurn().DaemonID

			te.handleSessionCorruptionRetry(1, 2, "", "", "corrupted", "", "model-1")
			newDaemon := te.lease.(runner.ActiveTurnOwner).ActiveTurn().DaemonID
			if got, ok := pool.ActiveTurns().Get(newDaemon); !ok || !got.Equal(span.SpanContext()) {
				t.Errorf("replacement daemon %q not registered under the turn span", newDaemon)
			}
			if _, ok := pool.ActiveTurns().Get(oldDaemon); ok && oldDaemon != newDaemon {
				t.Errorf("released daemon %q still registered", oldDaemon)
			}
			unmark()
			_ = te.lease.Release()
			if _, ok := pool.ActiveTurns().SoleDaemonInTarget("ephemeral"); ok {
				t.Errorf("turn still registered after clear")
			}
		})
	}
}

func TestLateSinkUsage(t *testing.T) {
	t.Parallel()
	killed := runner.AgyUsage{InputTokens: 34606, OutputTokens: 198, CacheReadTokens: 391188, CacheCreationTokens: 34588, TotalTokens: 34804}
	closedOnKill := runner.NewTracingTurnSink(context.Background(), nil)
	closedOnKill.OnError(&runner.TurnError{Err: errors.New("daemon closed while turn was in-flight"), Usage: killed})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		sink *runner.TracingTurnSink
		ctx  context.Context
		want runner.TokenUsage
	}{
		{"no attempt ran", nil, context.Background(), runner.TokenUsage{}},
		{"killed turn reports consumed usage", closedOnKill, context.Background(), killed},
		{"turn never closed before the grace ran out", runner.NewTracingTurnSink(context.Background(), nil), cancelled, runner.TokenUsage{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{traceSink: tc.sink}
			if got := te.lateSinkUsage(tc.ctx); got != tc.want {
				t.Errorf("lateSinkUsage = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// overflowTestLease is a lease served by an InterchangeablePool worker.
type overflowTestLease struct {
	testSessionLease

	overflow bool
}

func (l *overflowTestLease) IsOverflow() bool { return l.overflow }

func TestLeaseSpanAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		lease     runner.SessionLease
		ephemeral bool
		want      map[attribute.Key]attribute.Value
	}{
		{name: "new conversation", lease: &testSessionLease{turnCount: 1}, want: map[attribute.Key]attribute.Value{attrNewConversation: attribute.BoolValue(true), attrSessionEphemeral: attribute.BoolValue(false)}},
		{name: "resumed conversation", lease: &testSessionLease{turnCount: 7}, want: map[attribute.Key]attribute.Value{attrNewConversation: attribute.BoolValue(false), attrSessionEphemeral: attribute.BoolValue(false)}},
		{name: "fixed interchangeable worker", lease: &overflowTestLease{}, ephemeral: true, want: map[attribute.Key]attribute.Value{attrNewConversation: attribute.BoolValue(true), attrSessionEphemeral: attribute.BoolValue(true), attrSessionOverflow: attribute.BoolValue(false)}},
		{name: "on-demand overflow worker", lease: &overflowTestLease{overflow: true}, ephemeral: true, want: map[attribute.Key]attribute.Value{attrNewConversation: attribute.BoolValue(true), attrSessionEphemeral: attribute.BoolValue(true), attrSessionOverflow: attribute.BoolValue(true)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := attrMap(leaseSpanAttributes(tc.lease, tc.ephemeral)); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("leaseSpanAttributes = %v, want %v", got, tc.want)
			}
		})
	}
}

// wantPromptSpan describes one expected span: its parent span name ("" skips the parent check),
// a subset of its attributes, and whether it carries an error status.
type wantPromptSpan struct {
	name, parent string
	attrs        []attribute.KeyValue
	errored      bool
}

// newPromptBuildRecorder returns a tracer provider recording into rec and a ctx carrying an open
// prompt.build span from it.
func newPromptBuildRecorder() (*tracetest.SpanRecorder, *sdktrace.TracerProvider, context.Context, trace.Span) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, root := tp.Tracer("test").Start(context.Background(), spanPrompt) //nolint:spancheck // the caller ends the span
	return rec, tp, ctx, root                                              //nolint:spancheck // the caller ends the span
}

// assertPromptSpans checks every wanted span exists exactly once with the wanted parent, attributes
// and status, and that no absent span was recorded.
func assertPromptSpans(t *testing.T, ended []sdktrace.ReadOnlySpan, want []wantPromptSpan, absent []string) {
	t.Helper()
	byName, counts := map[string]sdktrace.ReadOnlySpan{}, map[string]int{}
	for _, s := range ended {
		byName[s.Name()] = s
		counts[s.Name()]++
	}
	for _, w := range want {
		s, ok := byName[w.name]
		if !ok {
			t.Errorf("missing span %q", w.name)
			continue
		}
		if counts[w.name] != 1 {
			t.Errorf("span %q recorded %d times, want 1", w.name, counts[w.name])
		}
		if w.parent != "" {
			if p, ok := byName[w.parent]; !ok || s.Parent().SpanID() != p.SpanContext().SpanID() {
				t.Errorf("span %q not parented under %q", w.name, w.parent)
			}
		}
		have := attrMap(s.Attributes())
		for _, kv := range w.attrs {
			if got, ok := have[kv.Key]; !ok || got != kv.Value {
				t.Errorf("span %q attr %s = %v (present %t), want %v", w.name, kv.Key, got.String(), ok, kv.Value.String())
			}
		}
		if gotErr := s.Status().Code == codes.Error; gotErr != w.errored {
			t.Errorf("span %q errored = %t, want %t (status %v)", w.name, gotErr, w.errored, s.Status())
		}
	}
	for _, name := range absent {
		if counts[name] != 0 {
			t.Errorf("unexpected span %q", name)
		}
	}
}
