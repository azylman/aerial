package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

// recordedParent starts and ends a span on its own in-memory recorder and returns a context
// carrying it plus the trace id the recorder saw.
func recordedParent(t *testing.T) (context.Context, string) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	span.End()
	return ctx, sr.Ended()[0].SpanContext().TraceID().String()
}

// traceparentContext parses a captured traceparent header into its span context.
func traceparentContext(tp string) trace.SpanContext {
	return trace.SpanContextFromContext(tracing.ContextWithTraceparent(context.Background(), tp))
}

// brainStub answers every request with status and records the last traceparent it saw.
func brainStub(t *testing.T, status int, got *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Header.Get("Traceparent")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"status":"queued"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

type postBrainJSONTraceCase struct {
	name       string
	brainURL   string
	withParent bool
	wantErr    bool
}

func runPostBrainJSONTraceCase(t *testing.T, tc postBrainJSONTraceCase) {
	t.Helper()
	var got string
	srv := brainStub(t, http.StatusOK, &got)
	parentCtx, parentTraceID := recordedParent(t)
	ctx := context.Background()
	if tc.withParent {
		ctx = parentCtx
	}
	brainURL := srv.URL
	if tc.wantErr {
		brainURL = tc.brainURL
	}
	s := NewVanadisServer(Config{BrainURL: brainURL}, nil, srv.Client())
	res, err := s.postBrainJSON(ctx, "/dispatch", BrainDispatchRequest{Source: "vanadis"}, 0)
	if tc.wantErr {
		if err == nil {
			t.Fatal("expected error for unconfigured brain URL, got nil")
		}
		if res != nil || got != "" {
			t.Fatalf("expected nil bytes and zero network I/O, got res=%s got=%q", string(res), got)
		}
		return
	}
	if err != nil {
		t.Fatalf("postBrainJSON: %v", err)
	}
	sc := traceparentContext(got)
	if !sc.IsValid() {
		t.Fatalf("request carried no valid traceparent: %q", got)
	}
	if (sc.TraceID().String() == parentTraceID) != tc.withParent {
		t.Errorf("trace id = %s, parent %s, want continues=%v", sc.TraceID(), parentTraceID, tc.withParent)
	}
}

func TestPostBrainJSON_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []postBrainJSONTraceCase{
		{name: "unconfigured brain and eri URL returns immediate error", wantErr: true},
		{name: "dispatch continues the caller's trace", withParent: true},
		{name: "starts a trace when the caller has none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runPostBrainJSONTraceCase(t, tc)
		})
	}
}

type dispatchChunkTurnSpanCase struct {
	name       string
	status     int
	wantErr    bool
	wantStatus codes.Code
}

func runDispatchChunkTurnSpanCase(t *testing.T, tc dispatchChunkTurnSpanCase) {
	t.Helper()
	var got string
	srv := brainStub(t, tc.status, &got)
	build := mkBuild("build-trace", "1555000111", "Trace Build", "Trace Build", "")
	chunks := []VanadisChunk{mkChunk("build-trace-chunk-1", "build-trace", 0, "Task 1", "Do it", "", "running")}
	s := NewVanadisServer(Config{BrainURL: srv.URL}, nil, srv.Client())
	if err := s.dispatchChunkTurn(context.Background(), build, chunks, 0, ""); (err != nil) != tc.wantErr {
		t.Fatalf("dispatchChunkTurn err = %v, wantErr %v", err, tc.wantErr)
	}
	// eri's request is a child of the client span, which is a child of the chunk span.
	reqTraceID := traceparentContext(got).TraceID()
	var span sdktrace.ReadOnlySpan
	for _, s := range spanRecorder.Ended() {
		if s.Name() == "vanadis.dispatch_chunk" && s.SpanContext().TraceID() == reqTraceID {
			span = s
		}
	}
	if span == nil {
		t.Fatalf("no chunk dispatch span in eri request trace %s", reqTraceID)
	}
	if span.Parent().IsValid() || span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("chunk span parent valid=%v kind=%v, want root producer", span.Parent().IsValid(), span.SpanKind())
	}
	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	if len(attrs) != 3 || attrs["vanadis.build.id"] != "build-trace" || attrs["vanadis.chunk.id"] != "build-trace-chunk-1" || attrs["vanadis.chunk.index"] != "1" {
		t.Errorf("attributes = %v, want only build id, chunk id and index", attrs)
	}
	if span.Status().Code != tc.wantStatus || (tc.wantStatus == codes.Error && span.Status().Description != "dispatch failed") {
		t.Errorf("status = %+v, want %v", span.Status(), tc.wantStatus)
	}
}

func TestDispatchChunkTurn_Span(t *testing.T) {
	t.Parallel()
	tests := []dispatchChunkTurnSpanCase{
		{name: "chunk dispatch is the parent of eri's request", status: http.StatusAccepted, wantStatus: codes.Unset},
		{name: "rejected dispatch marks the span with a generic status", status: http.StatusBadRequest, wantErr: true, wantStatus: codes.Error},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchChunkTurnSpanCase(t, tc)
		})
	}
}

func TestDispatchChunkTurn_FormatPromptError(t *testing.T) {
	t.Parallel()
	s := NewVanadisServer(Config{}, nil, nil)
	err := s.dispatchChunkTurn(context.Background(), VanadisBuild{}, []VanadisChunk{}, 0, "")
	if err == nil {
		t.Fatal("expected error on empty chunks")
	}
}

func TestCheckChunkDependenciesReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunk     VanadisChunk
		allChunks []VanadisChunk
		wantErr   bool
	}{
		{
			name:      "pr mode dependency completed",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "completed"}},
			wantErr:   false,
		},
		{
			name:      "pr mode dependency merged",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "merged"}},
			wantErr:   false,
		},
		{
			name:      "pr mode dependency deploying",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "deploying"}},
			wantErr:   false,
		},
		{
			name:      "pr mode dependency deploy_failed",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "deploy_failed"}},
			wantErr:   false,
		},
		{
			name:      "pr mode dependency not ready",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "coding"}},
			wantErr:   true,
		},
		{
			name:      "action mode dependency merged is not enough",
			chunk:     VanadisChunk{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "merged"}},
			wantErr:   true,
		},
		{
			name:      "action mode dependency completed succeeds",
			chunk:     VanadisChunk{ID: "c2", Mode: "action", DependsOn: []string{"c1"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "action", Status: "completed"}},
			wantErr:   false,
		},
		{
			name:      "missing dependency",
			chunk:     VanadisChunk{ID: "c2", Mode: "pr", DependsOn: []string{"c_missing"}},
			allChunks: []VanadisChunk{{ID: "c1", Mode: "pr", Status: "completed"}},
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkChunkDependenciesReady(tc.chunk, tc.allChunks)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkChunkDependenciesReady() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
