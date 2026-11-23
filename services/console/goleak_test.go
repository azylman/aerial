package main

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/azylman/aerial/pkg/hermetictest"
)

// spanRecorder captures every span the console records under test.
var spanRecorder = tracetest.NewSpanRecorder()

// testSATokenPath is the temp service-account token init points AERIAL_SA_TOKEN_PATH at.
var testSATokenPath string

// installTestTracing installs sampled spans and W3C propagation.
func installTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

func init() {
	hermetictest.Install()
	f, err := os.CreateTemp("", "console-test-sa-token-*")
	if err != nil {
		return
	}
	_, _ = f.WriteString("test-sa-token")
	_ = f.Close()
	testSATokenPath = f.Name()
	_ = os.Setenv("AERIAL_SA_TOKEN_PATH", testSATokenPath)
}

func TestMain(m *testing.M) {
	installTestTracing()
	hermetictest.Main(m,
		hermetictest.WithCleanup(func() {
			if testSATokenPath != "" {
				_ = os.Remove(testSATokenPath)
			}
		}),
	)
}
