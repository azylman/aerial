package runner

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type startupSpanTestCase struct {
	name         string
	opts         pipeMockSpawnerOpts
	engine       string
	inTurn       bool
	wantErr      bool
	wantSpans    []string
	wantEngine   string
	wantAttempts int
	wantSpawnErr bool
}

func checkStartupSpanAttributes(t *testing.T, s sdktrace.ReadOnlySpan, tc startupSpanTestCase) {
	t.Helper()
	if s.Name() != spanDaemonSpawn {
		return
	}
	have := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		have[kv.Key] = kv.Value
	}
	if have[attrAgentEngine] != attribute.StringValue(tc.wantEngine) || have[attrDaemonAttempts] != attribute.IntValue(tc.wantAttempts) {
		t.Errorf("daemon.spawn attrs = %v, want engine %s attempts %d", s.Attributes(), tc.wantEngine, tc.wantAttempts)
	}
	if (s.Status().Code == codes.Error) != tc.wantSpawnErr {
		t.Errorf("daemon.spawn status = %v, want error %v", s.Status(), tc.wantSpawnErr)
	}
}

func runStartupSpansCase(t *testing.T, tc startupSpanTestCase, idx int) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx := context.Background()
	var turn trace.Span
	if tc.inTurn {
		ctx, turn = tp.Tracer("test").Start(ctx, "turn")
	}
	pool := NewUnifiedProcessPool(PoolConfig{Engine: tc.engine, HandshakeTimeout: 50 * time.Millisecond}, newPipeMockSpawner(tc.opts))
	defer pool.Close()

	lease, err := pool.AcquireLease(ctx, fmt.Sprintf("startup-%d", idx))
	if (err != nil) != tc.wantErr {
		t.Fatalf("AcquireLease err = %v, want error %v", err, tc.wantErr)
	}
	if lease != nil {
		_ = lease.Release()
	}
	if turn != nil {
		turn.End()
	}

	var got []string
	for _, s := range rec.Ended() {
		if s.Name() == "turn" {
			continue
		}
		got = append(got, s.Name())
		if s.Parent().SpanID() != turn.SpanContext().SpanID() {
			t.Errorf("span %q parent = %v, want the turn span", s.Name(), s.Parent().SpanID())
		}
		checkStartupSpanAttributes(t, s, tc)
	}
	slices.Sort(got)
	if fmt.Sprint(got) != fmt.Sprint(tc.wantSpans) {
		t.Errorf("spans = %v, want %v", got, tc.wantSpans)
	}
}

func TestUnifiedProcessPool_StartupSpans(t *testing.T) {
	t.Parallel()
	tests := []startupSpanTestCase{
		{name: "turn lease records lock wait and agy spawn", inTurn: true, wantSpans: []string{spanDaemonSpawn, spanSessionLockWait}, wantEngine: "agy", wantAttempts: 1},
		{name: "handshake timeout retry counts both attempts", opts: pipeMockSpawnerOpts{skipInitOnAttempt: 1}, inTurn: true, wantSpans: []string{spanDaemonSpawn, spanSessionLockWait}, wantEngine: "agy", wantAttempts: 2},
		{name: "claude spawn covers the fork", engine: "claude", inTurn: true, wantSpans: []string{spanDaemonSpawn, spanSessionLockWait}, wantEngine: "claude", wantAttempts: 1},
		{name: "spawn failure marks the span", opts: pipeMockSpawnerOpts{spawnErrOn: 1}, inTurn: true, wantErr: true, wantSpans: []string{spanDaemonSpawn, spanSessionLockWait}, wantEngine: "agy", wantAttempts: 1, wantSpawnErr: true},
		{name: "background lease without a turn records nothing"},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStartupSpansCase(t, tc, i)
		})
	}
}

func TestStreamingDaemon_RecordDaemonInit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		line       string
		inflight   bool
		wantEvents []string
	}{
		{name: "claude init marks the in-flight turn", line: `{"type":"system","subtype":"init","session_id":"11111111-2222-3333-4444-555555555555"}`, inflight: true, wantEvents: []string{eventDaemonInit}},
		{name: "other system messages are ignored", line: `{"type":"system","subtype":"status"}`, inflight: true},
		{name: "init with no in-flight turn records nothing", line: `{"type":"system","subtype":"init","session_id":"11111111-2222-3333-4444-555555555555"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			ctx, turn := tp.Tracer("test").Start(context.Background(), "turn")
			d := &StreamingDaemon{cfg: DaemonConfig{Engine: "claude"}, claudeToolIDs: make(map[string]int)}
			if tc.inflight {
				enqueueInflightTurn(d, "t1", newMockTurnSink(), func(tc *TurnContext) { tc.Ctx = ctx })
			}
			d.dispatchNDJSONLine(tc.line)
			turn.End()

			events := rec.Ended()[0].Events()
			got := make([]string, 0, len(events))
			for _, ev := range events {
				got = append(got, ev.Name)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.wantEvents) {
				t.Errorf("turn events = %v, want %v", got, tc.wantEvents)
			}
		})
	}
}
