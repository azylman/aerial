package main

import (
	"context"
	"errors"
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

func traceparentTraceID(tp string) string {
	sc := trace.SpanContextFromContext(tracing.ContextWithTraceparent(context.Background(), tp))
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

func TestPostBrainJSON_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
	}{
		{name: "continues the fire span's trace", withParent: true},
		{name: "starts a trace when the caller has none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Traceparent")
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			}))
			defer srv.Close()
			parentCtx, parentTraceID := recordedParent(t)
			ctx := context.Background()
			if tc.withParent {
				ctx = parentCtx
			}
			if _, err := postBrainJSON[DispatchRequest, DispatchResponse](ctx, NewHTTPPromptDispatcher(srv.URL), "/dispatch", "dispatch", DispatchRequest{Source: "heartbeat"}); err != nil {
				t.Fatalf("postBrainJSON: %v", err)
			}
			id := traceparentTraceID(got)
			if id == "" {
				t.Fatalf("request carried no valid traceparent: %q", got)
			}
			if (id == parentTraceID) != tc.withParent {
				t.Errorf("trace id = %s, parent %s, want continues=%v", id, parentTraceID, tc.withParent)
			}
		})
	}
}

type traceFireTestCase struct {
	name         string
	scheduleType string
	scheduleID   string
	withParent   bool
	dispatchErr  error
	wantStatus   codes.Code
}

func findSpanBySpanID(spanID trace.SpanID) sdktrace.ReadOnlySpan {
	for _, s := range spanRecorder.Ended() {
		if s.SpanContext().SpanID() == spanID {
			return s
		}
	}
	return nil
}

func assertSpanStatusAndAttrs(t *testing.T, span sdktrace.ReadOnlySpan, tc traceFireTestCase) {
	t.Helper()
	if span.Status().Code != tc.wantStatus || (tc.wantStatus == codes.Error && span.Status().Description != "dispatch failed") {
		t.Errorf("status = %+v, want %v with generic description", span.Status(), tc.wantStatus)
	}
	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	want := map[string]string{"haro.schedule.type": tc.scheduleType, "haro.schedule.id": tc.scheduleID}
	if len(attrs) != len(want) || attrs["haro.schedule.type"] != want["haro.schedule.type"] || attrs["haro.schedule.id"] != want["haro.schedule.id"] {
		t.Errorf("attributes = %v, want exactly %v", attrs, want)
	}
}

func runTraceFireCase(t *testing.T, tc traceFireTestCase) {
	t.Helper()
	parentCtx, parentTraceID := recordedParent(t)
	ctx := context.Background()
	if tc.withParent {
		ctx = parentCtx
	}
	var dispatchSC trace.SpanContext
	_, err := traceFire(ctx, tc.scheduleType, tc.scheduleID, func(ctx context.Context) (DispatchResponse, error) {
		dispatchSC = trace.SpanContextFromContext(ctx)
		return DispatchResponse{}, tc.dispatchErr
	})
	if !errors.Is(err, tc.dispatchErr) {
		t.Fatalf("traceFire err = %v, want %v", err, tc.dispatchErr)
	}
	span := findSpanBySpanID(dispatchSC.SpanID())
	if span == nil {
		t.Fatal("fire span was not recorded")
	}
	if span.Name() != "haro.fire "+tc.scheduleType || span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span = %q kind %v", span.Name(), span.SpanKind())
	}
	if (span.SpanContext().TraceID().String() == parentTraceID) != tc.withParent {
		t.Errorf("trace id = %s, parent %s, want continues=%v", span.SpanContext().TraceID(), parentTraceID, tc.withParent)
	}
	assertSpanStatusAndAttrs(t, span, tc)
}

func TestTraceFire(t *testing.T) {
	t.Parallel()
	tests := []traceFireTestCase{
		{name: "cron fire roots a trace", scheduleType: "cron", scheduleID: "cron-1", wantStatus: codes.Unset},
		{name: "event trigger continues the webhook trace", scheduleType: "event_watch", scheduleID: "trig-1", withParent: true, wantStatus: codes.Unset},
		{name: "dispatch failure marks the span without error text", scheduleType: "heartbeat", scheduleID: "hb-1", dispatchErr: errors.New("brain /dispatch returned HTTP 500: secret prompt echo"), wantStatus: codes.Error},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceFireCase(t, tc)
		})
	}
}
