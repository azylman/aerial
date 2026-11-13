package queue

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
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

func TestStartTurnSpan(t *testing.T) {
	t.Parallel()
	pickup := time.Date(2026, 11, 13, 8, 0, 10, 0, time.UTC)
	enq := pickup.Add(-3 * time.Second)
	tests := []struct {
		name      string
		items     []queuedTurnItem
		wantTrace string // "" means a fresh root trace
		wantLinks []string
		wantWait  bool
		wantStart time.Time
		agentName string
		wantAgent bool
	}{
		{name: "single message parents to its traceparent", items: []queuedTurnItem{{traceparent: traceparA, createdAt: enq}}, wantTrace: traceA, wantWait: true, wantStart: enq, agentName: "aerial", wantAgent: true},
		{name: "burst links the rest", items: []queuedTurnItem{{traceparent: traceparA, createdAt: enq}, {traceparent: traceparB, createdAt: enq}, {traceparent: malformedTP}, {traceparent: traceparC}}, wantTrace: traceA, wantLinks: []string{traceB, traceC}, wantWait: true, wantStart: enq},
		{name: "untraced first message starts a root and links the others", items: []queuedTurnItem{{createdAt: enq}, {traceparent: traceparB}}, wantLinks: []string{traceB}, wantWait: true, wantStart: enq},
		{name: "no enqueue time skips queue wait", items: []queuedTurnItem{{traceparent: traceparA}}, wantTrace: traceA, wantStart: pickup},
		{name: "clock skew skips queue wait", items: []queuedTurnItem{{traceparent: traceparA, createdAt: pickup.Add(time.Second)}}, wantTrace: traceA, wantStart: pickup},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			if wait != nil && (!wait.StartTime().Equal(enq) || !wait.EndTime().Equal(pickup) || wait.Parent().SpanID() != root.SpanContext().SpanID()) {
				t.Errorf("queue.wait = %v..%v parent %v, want %v..%v under root", wait.StartTime(), wait.EndTime(), wait.Parent().SpanID(), enq, pickup)
			}
			have := attrMap(root.Attributes())
			if have[runner.AttrGenAIConversationID] != attribute.StringValue("thread-1") || have[runner.AttrGenAIOperationName] != attribute.StringValue("invoke_agent") {
				t.Errorf("root attrs = %v", root.Attributes())
			}
			if _, ok := have[runner.AttrGenAIAgentName]; ok != tc.wantAgent {
				t.Errorf("agent name present = %v, want %v", ok, tc.wantAgent)
			}
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

func agyToolTurnSpawner() *mockDaemonSpawner {
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
					_, _ = outW.Write([]byte(`{"event":"step_update","step_update":{"step_index":1,"type":"tool_call","state":"DONE"}}` + "\n"))
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"done","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}` + "\n"))
				}
			}()
			return inW, outR, errR, newMockProcessHandle(12347), nil
		},
	}
}

func TestProcessBurst_EmitsTurnTrace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traceparent string
		wantTrace   string
	}{
		{name: "continues ingress trace", traceparent: traceparA, wantTrace: traceA},
		{name: "untraced message starts a root", traceparent: ""},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, tmpHome, tempData := setupTestStore(t), t.TempDir(), t.TempDir()
			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, agyToolTurnSpawner())
			defer procPool.Close()

			sig := rootEndSignal{SpanRecorder: tracetest.NewSpanRecorder(), done: make(chan struct{})}
			pool := New(newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData }), WorkerPoolConfig{
				ProcessPool:    procPool,
				SessionManager: session.New(tmpHome, tempData),
				Store:          store,
				TimeoutMinutes: 1,
				TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sig)),
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
			if tc.wantTrace != "" && root.SpanContext().TraceID().String() != tc.wantTrace {
				t.Errorf("root trace = %s, want %s", root.SpanContext().TraceID(), tc.wantTrace)
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
			have := attrMap(root.Attributes())
			if have[runner.AttrGenAIUsageInputTokens] != attribute.IntValue(7) || have[runner.AttrGenAIUsageOutputTokens] != attribute.IntValue(3) ||
				have[runner.AttrGenAIAgentName] != attribute.StringValue("aerial") || have[runner.AttrGenAIProviderName] != attribute.StringValue("gcp.gemini") {
				t.Errorf("root attrs = %v", root.Attributes())
			}
			if root.Status().Code == codes.Error {
				t.Errorf("root status = %v, want ok", root.Status())
			}
		})
	}
}
