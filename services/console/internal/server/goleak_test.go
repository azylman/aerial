package server

import (
	"net/http"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
)

// spanRecorder captures every span the console records under test.
var spanRecorder = tracetest.NewSpanRecorder()

// testSATokenPath is the temp service-account token init points AERIAL_SA_TOKEN_PATH at.
var testSATokenPath string

// installTestTracing installs sampled spans and W3C propagation, as in production. Tests that run
// main() restore it, since tracing.Init replaces the globals.
func installTestTracing() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// init runs before TestMain, so it installs the loopback guard itself before the brain and
// vanadis clients capture http.DefaultTransport.
func init() {
	hermetictest.Install()
	// Always use a temp token so a pod-provided AERIAL_SA_TOKEN_PATH is never read.
	f, err := os.CreateTemp("", "console-test-sa-token-*")
	if err != nil {
		return
	}
	_, _ = f.WriteString("test-sa-token")
	_ = f.Close()
	testSATokenPath = f.Name()
	_ = os.Setenv("AERIAL_SA_TOKEN_PATH", testSATokenPath)
	if brainHTTPClient != nil {
		brainHTTPClient = tracing.Client(&http.Client{
			Timeout:   5 * time.Second,
			Transport: k8sauth.NewServiceAccountTransport(http.DefaultTransport, testSATokenPath),
		})
	}
	vanadisHTTPClient = newVanadisHTTPClient(k8sauth.NewTokenSource(testSATokenPath, time.Now))
}

func TestMain(m *testing.M) {
	installTestTracing()
	hermetictest.Main(m,
		hermetictest.WithCleanup(closeConsoleTestClients),
		hermetictest.WithCleanup(func() {
			if testSATokenPath != "" {
				_ = os.Remove(testSATokenPath)
			}
		}),
	)
}

// closeConsoleTestClients drops idle connections on the console's package-level clients so
// goleak sees no lingering transport goroutines.
func closeConsoleTestClients() {
	if brainHTTPClient != nil {
		brainHTTPClient.CloseIdleConnections()
	}
	if k8sHTTPClient != nil {
		k8sHTTPClient.CloseIdleConnections()
	}
	if vanadisHTTPClient != nil {
		vanadisHTTPClient.CloseIdleConnections()
	}
	if globalGHPoller != nil && globalGHPoller.client != nil {
		globalGHPoller.client.CloseIdleConnections()
	}
}
