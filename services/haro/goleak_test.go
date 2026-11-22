package main

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/azylman/aerial/pkg/hermetictest"
)

// spanRecorder captures every span ended through the global provider; tests filter by trace id.
var spanRecorder = tracetest.NewSpanRecorder()

// installTestTracing mirrors tracing.Init with the in-memory recorder instead of an exporter.
// Tests that run main() (which installs the real, disabled provider) reinstall it on cleanup.
func installTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

func TestMain(m *testing.M) {
	installTestTracing()
	hermetictest.Main(m)
}
