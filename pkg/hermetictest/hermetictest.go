// Package hermetictest keeps Go tests hermetic: it blocks outbound HTTP to anything
// but loopback, refuses the reserved live-daemon ports, strips ambient OTEL_*
// settings and runs goleak around the whole test binary.
//
// It imports no aerial package so every pkg and service package can adopt it
// without an import cycle.
package hermetictest

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"
)

// reservedPorts are the ports live Aerial daemons listen on; a test dialing one
// on loopback would talk to a real process on a dev host or pod.
var reservedPorts = map[string]bool{
	"8080": true, "8088": true, "4020": true, "8081": true, "8082": true,
	"8083": true, "8090": true, "8070": true, "9070": true,
}

// Transport rejects requests to non-loopback hosts and to reserved live-daemon
// ports, and forwards everything else to Base (http.DefaultTransport when nil;
// never install a nil-Base Transport as http.DefaultTransport itself).
type Transport struct {
	Base http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("hermetic test guard: nil request or URL")
	}
	host := req.URL.Hostname()
	if host != "127.0.0.1" && host != "::1" && host != "localhost" {
		return nil, fmt.Errorf("hermetic test guard: blocked non-loopback HTTP call to %s", req.URL.Host)
	}
	if port := req.URL.Port(); reservedPorts[port] {
		return nil, fmt.Errorf("hermetic test guard: blocked dial to live daemon port %s on %s", port, host)
	}
	return t.base().RoundTrip(req)
}

// CloseIdleConnections closes idle connections held by the wrapped transport.
func (t *Transport) CloseIdleConnections() {
	if cl, ok := t.base().(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

// Install wraps http.DefaultTransport in a Transport. It is idempotent; the
// returned restore func puts back the transport it replaced (a no-op when the
// guard was already installed).
func Install() (restore func()) {
	if _, ok := http.DefaultTransport.(*Transport); ok {
		return func() {}
	}
	prev := http.DefaultTransport
	http.DefaultTransport = &Transport{Base: prev}
	return func() { http.DefaultTransport = prev }
}

// Option configures Main.
type Option func(*config)

type config struct {
	cleanups []func()
	ignores  []goleak.Option
}

// WithCleanup registers fn to run after m.Run and before the leak check.
func WithCleanup(fn func()) Option {
	return func(c *config) { c.cleanups = append(c.cleanups, fn) }
}

// WithIgnore adds package-specific goleak options (e.g. goleak.IgnoreTopFunction).
func WithIgnore(opts ...goleak.Option) Option {
	return func(c *config) { c.ignores = append(c.ignores, opts...) }
}

// Main is a drop-in TestMain body (m is the *testing.M, or a wrapper whose Run
// sets up per-package fixtures such as pgtest.Main around it): it installs the transport guard, unsets every
// OTEL_* environment variable, runs the tests, runs the registered cleanups,
// closes idle HTTP connections and verifies no goroutines leaked.
func Main(m goleak.TestingM, opts ...Option) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	Install()
	if err := unsetOTELEnv(os.Environ(), os.Unsetenv); err != nil {
		panic(fmt.Sprintf("hermetictest: unset OTEL env: %v", err))
	}
	goleak.VerifyTestMain(runner{m: m, cleanups: c.cleanups}, c.ignores...)
}

// runner runs the tests and then the post-run cleanups, so they finish before
// goleak looks for leaked goroutines.
type runner struct {
	m        goleak.TestingM
	cleanups []func()
}

func (r runner) Run() int {
	code := r.m.Run()
	for _, fn := range r.cleanups {
		fn()
	}
	closeIdleConnections()
	return code
}

func closeIdleConnections() {
	http.DefaultClient.CloseIdleConnections()
	if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
}

// unsetOTELEnv removes every OTEL_* variable from environ via unset.
func unsetOTELEnv(environ []string, unset func(string) error) error {
	var errs []error
	for _, kv := range environ {
		if key, _, _ := strings.Cut(kv, "="); strings.HasPrefix(key, "OTEL_") {
			errs = append(errs, unset(key))
		}
	}
	return errors.Join(errs...)
}

// NewRecorder returns a tracer provider that samples every span into the
// returned in-memory recorder. Callers shut the provider down when done.
func NewRecorder() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	return tp, sr
}
