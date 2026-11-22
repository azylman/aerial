package server

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// testSpans records every span ended by the package under test. Tests look spans up by the
// span ID they observe in a ctx, so lookups stay correct under t.Parallel().
var testSpans = tracetest.NewSpanRecorder()

func installTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(testSpans)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// endedSpan returns the ended span with the given span ID, or nil.
func endedSpan(id trace.SpanID) sdktrace.ReadOnlySpan {
	for _, s := range testSpans.Ended() {
		if s.SpanContext().SpanID() == id {
			return s
		}
	}
	return nil
}

// endedSpanSummary renders the span's name, root-ness and attribute keys for table assertions.
func endedSpanSummary(id trace.SpanID) (name string, root bool, attrs map[string]string) {
	s := endedSpan(id)
	if s == nil {
		return "", false, nil
	}
	attrs = map[string]string{}
	for _, kv := range s.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	return s.Name(), !s.Parent().IsValid(), attrs
}

func TestEndSchedulerSpan(t *testing.T) {
	t.Parallel()
	endSchedulerSpan(nil, nil)
	endSchedulerSpan(nil, errors.New("err"))

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	_, spanErr := tracer.Start(context.Background(), "test-err")
	endSchedulerSpan(spanErr, errors.New("failure"))

	_, spanOK := tracer.Start(context.Background(), "test-ok")
	endSchedulerSpan(spanOK, nil)

	ended := sr.Ended()
	if len(ended) != 2 {
		t.Fatalf("expected 2 ended spans, got %d", len(ended))
	}
	if ended[0].Status().Code != codes.Error {
		t.Errorf("spanErr status = %v, want %v", ended[0].Status().Code, codes.Error)
	}
	if ended[0].Status().Description != "scheduled run failed" {
		t.Errorf("spanErr description = %q, want 'scheduled run failed'", ended[0].Status().Description)
	}
	if ended[1].Status().Code != codes.Unset {
		t.Errorf("spanOK status = %v, want %v", ended[1].Status().Code, codes.Unset)
	}
}
