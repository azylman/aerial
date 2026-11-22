package router

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/azylman/aerial/pkg/hermetictest"
)

func TestMain(m *testing.M) {
	// A recording (never exporting) global provider, as production installs via tracing.Init,
	// so outbound clients create real child spans whose traceparent tests can assert on.
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	hermetictest.Main(m)
}
