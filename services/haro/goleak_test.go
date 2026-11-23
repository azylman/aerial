package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/pgtest"
)

// spanRecorder captures every span ended through the global provider; tests filter by trace id.
var spanRecorder = tracetest.NewSpanRecorder()

// installTestTracing mirrors tracing.Init with the in-memory recorder instead of an exporter.
// Tests that run main() (which installs the real, disabled provider) reinstall it on cleanup.
func installTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// TestMain is hermetictest.Main around pgtest.Main: both take the *testing.M,
// so the transport guard, OTEL scrub and leak check are applied here around
// the per-package Postgres server instead.
func TestMain(m *testing.M) {
	installTestTracing()
	hermetictest.Install()
	for _, kv := range os.Environ() {
		if key, _, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(key, "OTEL_") {
			_ = os.Unsetenv(key)
		}
	}
	code := pgtest.Main(m)
	http.DefaultClient.CloseIdleConnections()
	if code == 0 {
		if err := goleak.Find(); err != nil {
			log.Printf("goleak: %v", err)
			code = 1
		}
	}
	os.Exit(code)
}
