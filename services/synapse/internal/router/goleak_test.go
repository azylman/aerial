package router

import (
	"log/slog"
	"net/http"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// A recording (never exporting) global provider, as production installs via tracing.Init,
	// so outbound clients create real child spans whose traceparent tests can assert on.
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// Route [synapse-effect] lines into the parity sink bound to each test's
	// ctx; every other record logs as before.
	slog.SetDefault(slog.New(effectCaptureHandler{next: slog.NewTextHandler(os.Stderr, nil)}))
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
	}))
}
