package queue

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/discord"
)

// lockWaitSignal records spans and signals each widget.lock_wait start, so a test can release a
// held lock only once the waiter has begun waiting.
type lockWaitSignal struct {
	*tracetest.SpanRecorder

	started chan struct{}
}

func (r lockWaitSignal) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	r.SpanRecorder.OnStart(ctx, s)
	if s.Name() == spanWidgetLockWait {
		r.started <- struct{}{}
	}
}

// newWidgetTracePool returns a pool whose widget deliveries succeed, its span recorder, and a
// context carrying an open turn span (traced) or no span at all.
func newWidgetTracePool(t *testing.T, traced bool) (*WorkerPool, lockWaitSignal, context.Context) {
	t.Helper()
	rec := lockWaitSignal{SpanRecorder: tracetest.NewSpanRecorder(), started: make(chan struct{}, 8)}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	p := &WorkerPool{cfg: WorkerPoolConfig{TracerProvider: tp}}
	p.SetWidgetDeliveryFunc(func(req discord.StatusWidgetRequest) (string, error) { return "msg-1", nil })
	ctx := context.Background()
	if traced {
		ctx, _ = tp.Tracer("test").Start(ctx, "turn") //nolint:spancheck // left open so only widget spans end
	}
	return p, rec, ctx
}

// endedWidgetSpans returns ended span names in end order and attributes by name, failing on any
// span that is not inside ctx's trace (an orphan root).
func endedWidgetSpans(ctx context.Context, t *testing.T, rec lockWaitSignal) ([]string, map[string]map[string]string) {
	t.Helper()
	traceID := trace.SpanContextFromContext(ctx).TraceID()
	ended := rec.Ended()
	names := make([]string, 0, len(ended))
	attrs := map[string]map[string]string{}
	for _, s := range ended {
		if !s.Parent().IsValid() || s.SpanContext().TraceID() != traceID {
			t.Errorf("span %s is not attached to the turn trace", s.Name())
		}
		names = append(names, s.Name())
		m := map[string]string{}
		for _, kv := range s.Attributes() {
			m[string(kv.Key)] = kv.Value.String()
		}
		attrs[s.Name()] = m
	}
	return names, attrs
}

func TestWorkerPool_LockWidgetDelivery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		traced     bool
		heldBy     widgetLockPath
		wantSpans  []string
		wantHolder map[string]string
	}{
		{name: "uncontended wait names no holder", traced: true, wantSpans: []string{spanWidgetLockWait}, wantHolder: map[string]string{}},
		{name: "wait behind a step edit names it", traced: true, heldBy: widgetLockStep, wantSpans: []string{spanWidgetLockWait}, wantHolder: map[string]string{string(attrWidgetLockHolder): "step"}},
		{name: "wait behind a finish names it", traced: true, heldBy: widgetLockFinish, wantSpans: []string{spanWidgetLockWait}, wantHolder: map[string]string{string(attrWidgetLockHolder): "finish"}},
		{name: "untraced wait opens no root span", traced: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, rec, ctx := newWidgetTracePool(t, tc.traced)
			if tc.heldBy != "" {
				p.lockWidgetDelivery(context.Background(), tc.heldBy)
			}
			acquired := make(chan struct{})
			go func() {
				p.lockWidgetDelivery(ctx, widgetLockDeliver)
				close(acquired)
			}()
			if tc.heldBy != "" {
				<-rec.started
				p.unlockWidgetDelivery()
			}
			<-acquired
			if got := p.widgetLockHolder.Load(); got == nil || *got != widgetLockDeliver {
				t.Errorf("holder while locked = %v, want deliver", got)
			}
			p.unlockWidgetDelivery()
			if got := p.widgetLockHolder.Load(); got != nil {
				t.Errorf("holder after unlock = %v, want none", *got)
			}

			names, attrs := endedWidgetSpans(ctx, t, rec)
			if fmt.Sprint(names) != fmt.Sprint(tc.wantSpans) {
				t.Fatalf("spans = %v, want %v", names, tc.wantSpans)
			}
			if tc.wantHolder != nil && fmt.Sprint(attrs[spanWidgetLockWait]) != fmt.Sprint(tc.wantHolder) {
				t.Errorf("lock_wait attrs = %v, want %v", attrs[spanWidgetLockWait], tc.wantHolder)
			}
		})
	}
}

func TestWorkerPool_DeliverStatusWidgetSpans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traced      bool
		req         discord.StatusWidgetRequest
		wantSpans   []string
		wantDeliver map[string]string
	}{
		{
			name:   "live step edit",
			traced: true,
			req: discord.StatusWidgetRequest{ChannelID: "chan-1", Chunks: []discord.StatusWidgetChunk{
				{ID: "c1", Status: "running", ActiveStep: "editing widget.go"}, {ID: "c2", Status: "pending"},
			}},
			wantSpans:   []string{spanWidgetLockWait, spanWidgetDeliver},
			wantDeliver: map[string]string{string(attrWidgetHasStep): "true", string(attrWidgetChunks): "2"},
		},
		{
			name:        "card without chunks",
			traced:      true,
			req:         discord.StatusWidgetRequest{ChannelID: "chan-1"},
			wantSpans:   []string{spanWidgetLockWait, spanWidgetDeliver},
			wantDeliver: map[string]string{string(attrWidgetHasStep): "false", string(attrWidgetChunks): "0"},
		},
		{name: "untraced request opens no root span", req: discord.StatusWidgetRequest{ChannelID: "chan-1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, rec, ctx := newWidgetTracePool(t, tc.traced)
			if _, err := p.DeliverStatusWidgetWithContext(ctx, tc.req); err != nil {
				t.Fatalf("deliver: %v", err)
			}
			names, attrs := endedWidgetSpans(ctx, t, rec)
			if fmt.Sprint(names) != fmt.Sprint(tc.wantSpans) {
				t.Fatalf("spans = %v, want %v", names, tc.wantSpans)
			}
			if tc.wantDeliver != nil && fmt.Sprint(attrs[spanWidgetDeliver]) != fmt.Sprint(tc.wantDeliver) {
				t.Errorf("deliver attrs = %v, want %v", attrs[spanWidgetDeliver], tc.wantDeliver)
			}
		})
	}
}

func TestWorkerPool_FlushChunkActiveStepSpans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traced      bool
		flushTurn   string
		wantSpans   []string
		wantDeliver map[string]string
	}{
		{
			name: "owned step edit is delivered", traced: true, flushTurn: "t1",
			wantSpans:   []string{spanWidgetLockWait, spanWidgetDeliver},
			wantDeliver: map[string]string{string(attrWidgetHasStep): "true", string(attrWidgetChunks): "1"},
		},
		{
			name: "step from another turn records only the lock", traced: true, flushTurn: "t-other",
			wantSpans:   []string{spanWidgetLockWait, spanWidgetDeliver},
			wantDeliver: map[string]string{},
		},
		{name: "untraced flush opens no root span", flushTurn: "t1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, rec, ctx := newWidgetTracePool(t, tc.traced)
			p.widgetMu.Lock()
			p.ensureWidgetMapsLocked()
			p.widgetsByID["b1"] = discord.StatusWidgetRequest{ID: "b1", ChannelID: "chan-1", MessageID: "msg-1", Chunks: []discord.StatusWidgetChunk{{ID: "c1", Status: "running"}}}
			p.widgetMu.Unlock()
			p.StartChunkStepTurn(context.Background(), "c1", "b1", 0, "t1")

			if err := p.FlushChunkActiveStep(ctx, "c1", tc.flushTurn, "running tests"); err != nil {
				t.Fatalf("flush: %v", err)
			}
			names, attrs := endedWidgetSpans(ctx, t, rec)
			if fmt.Sprint(names) != fmt.Sprint(tc.wantSpans) {
				t.Fatalf("spans = %v, want %v", names, tc.wantSpans)
			}
			if tc.wantDeliver != nil && fmt.Sprint(attrs[spanWidgetDeliver]) != fmt.Sprint(tc.wantDeliver) {
				t.Errorf("deliver attrs = %v, want %v", attrs[spanWidgetDeliver], tc.wantDeliver)
			}
		})
	}
}

func TestWorkerPool_FinishChunkStepTurnSpans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traced      bool
		finishTurn  string
		wantParents map[string]string
		wantDeliver map[string]string
	}{
		{
			name: "step clear is delivered under its own span", traced: true, finishTurn: "t1",
			wantParents: map[string]string{spanWidgetDeliver: "turn", spanWidgetLockWait: spanWidgetDeliver},
			wantDeliver: map[string]string{string(attrWidgetHasStep): "false", string(attrWidgetChunks): "1"},
		},
		{
			name: "finish from another turn records only the lock", traced: true, finishTurn: "t-other",
			wantParents: map[string]string{spanWidgetDeliver: "turn", spanWidgetLockWait: spanWidgetDeliver},
			wantDeliver: map[string]string{},
		},
		{name: "untraced finish opens no root span", finishTurn: "t1", wantParents: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, rec, ctx := newWidgetTracePool(t, tc.traced)
			p.widgetMu.Lock()
			p.ensureWidgetMapsLocked()
			p.widgetsByID["b1"] = discord.StatusWidgetRequest{ID: "b1", ChannelID: "chan-1", MessageID: "msg-1", Chunks: []discord.StatusWidgetChunk{{ID: "c1", Status: "running"}}}
			p.widgetMu.Unlock()
			p.StartChunkStepTurn(context.Background(), "c1", "b1", 0, "t1")
			if err := p.FlushChunkActiveStep(context.Background(), "c1", "t1", "running tests"); err != nil {
				t.Fatalf("flush: %v", err)
			}

			p.FinishChunkStepTurn(ctx, "c1", tc.finishTurn)

			_, attrs := endedWidgetSpans(ctx, t, rec)
			names := map[trace.SpanID]string{trace.SpanContextFromContext(ctx).SpanID(): "turn"}
			for _, s := range rec.Ended() {
				names[s.SpanContext().SpanID()] = s.Name()
			}
			parents := map[string]string{}
			for _, s := range rec.Ended() {
				parents[s.Name()] = names[s.Parent().SpanID()]
			}
			if fmt.Sprint(parents) != fmt.Sprint(tc.wantParents) {
				t.Fatalf("span parents = %v, want %v", parents, tc.wantParents)
			}
			if tc.wantDeliver != nil && fmt.Sprint(attrs[spanWidgetDeliver]) != fmt.Sprint(tc.wantDeliver) {
				t.Errorf("deliver attrs = %v, want %v", attrs[spanWidgetDeliver], tc.wantDeliver)
			}
		})
	}
}

func TestWorkerPool_EnsureChunkWidgetHydratedSpans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		traced    bool
		wantSpans []string
	}{
		{name: "traced cold hydrate", traced: true, wantSpans: []string{spanWidgetHydrate}},
		{name: "untraced hydrate opens no root span"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bodies := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				bodies <- r.URL.Path + " " + string(b)
			}))
			t.Cleanup(srv.Close)
			p, rec, ctx := newWidgetTracePool(t, tc.traced)
			p.cfg.SynapseURL = srv.URL

			p.ensureChunkWidgetHydrated(ctx, "Build ID: b1", "c1", "b1")

			if got, want := <-bodies, `/internal/widget/sync {"build_id":"b1"}`; got != want {
				t.Errorf("sync request = %q, want %q", got, want)
			}
			names, _ := endedWidgetSpans(ctx, t, rec)
			if fmt.Sprint(names) != fmt.Sprint(tc.wantSpans) {
				t.Errorf("spans = %v, want %v", names, tc.wantSpans)
			}
		})
	}
}
