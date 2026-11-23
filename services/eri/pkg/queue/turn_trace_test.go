package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
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

func TestEndTurnSpanContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		capture    bool // WorkerPoolConfig.TraceContent
		wantInput  string
		wantOutput string
	}{
		{
			name: "capture on records prompt and response", capture: true,
			wantInput:  `[{"role":"user","parts":[{"type":"text","content":"what's the weather?"}]}]`,
			wantOutput: `[{"role":"assistant","parts":[{"type":"text","content":"Hello from mock voice."}]}]`,
		},
		{name: "capture off records no content"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			pool := New(nil, WorkerPoolConfig{TraceContent: tc.capture, TracerProvider: tp})
			pool.voiceProcessPool = &queueMockVoiceProcessPool{sessions: make(map[string]*queueMockVoiceSession)}
			defer pool.Stop()
			if _, _, err := pool.ExecuteVoiceTurn(context.Background(), "what's the weather?", "kiosk", nil); err != nil {
				t.Fatalf("ExecuteVoiceTurn: %v", err)
			}
			var roots []sdktrace.ReadOnlySpan
			for _, s := range rec.Ended() {
				if s.Name() == runner.OperationInvokeAgent {
					roots = append(roots, s)
				}
			}
			if len(roots) != 1 {
				t.Fatalf("ended invoke_agent spans = %d, want 1", len(roots))
			}
			have := attrMap(roots[0].Attributes())
			if got := have[tracing.AttrGenAIInputMessages].AsString(); got != tc.wantInput {
				t.Errorf("input = %q, want %q", got, tc.wantInput)
			}
			if got := have[tracing.AttrGenAIOutputMessages].AsString(); got != tc.wantOutput {
				t.Errorf("output = %q, want %q", got, tc.wantOutput)
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
