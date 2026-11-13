package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azylman/aerial/pkg/tracing"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

func TestPostBrainJSON_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		path       string
		withParent bool
	}{
		{name: "dispatch continues the caller's trace", path: "/dispatch", withParent: true},
		{name: "wake prompt continues the caller's trace", path: "/prompt", withParent: true},
		{name: "starts a trace when the caller has none", path: "/dispatch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := brainStub(t, http.StatusOK, &got)
			parentCtx, parentTraceID := recordedParent(t)
			ctx := context.Background()
			if tc.withParent {
				ctx = parentCtx
			}
			s := NewVanadisServer(Config{BrainURL: srv.URL}, nil, nil, srv.Client())
			if _, err := s.postBrainJSON(ctx, tc.path, BrainDispatchRequest{Source: "vanadis"}, 0); err != nil {
				t.Fatalf("postBrainJSON: %v", err)
			}
			sc := traceparentContext(got)
			if !sc.IsValid() {
				t.Fatalf("request carried no valid traceparent: %q", got)
			}
			if (sc.TraceID().String() == parentTraceID) != tc.withParent {
				t.Errorf("trace id = %s, parent %s, want continues=%v", sc.TraceID(), parentTraceID, tc.withParent)
			}
		})
	}
}

func TestDispatchChunkTurn_Span(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		wantErr    bool
		wantStatus codes.Code
	}{
		{name: "chunk dispatch is the parent of eri's request", status: http.StatusAccepted, wantStatus: codes.Unset},
		{name: "rejected dispatch marks the span with a generic status", status: http.StatusBadRequest, wantErr: true, wantStatus: codes.Error},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := brainStub(t, tc.status, &got)
			build := mkBuild("build-trace", "1555000111", "Trace Build", "Trace Build", "")
			chunks := []VanadisChunk{mkChunk("build-trace-chunk-1", "build-trace", 0, "Task 1", "Do it", "", "running")}
			s := NewVanadisServer(Config{BrainURL: srv.URL}, nil, nil, srv.Client())
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
		})
	}
}
