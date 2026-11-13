// Package tracing is the single home of OpenTelemetry setup for every Aerial Go service.
//
// Services load their settings with LoadConfig, install the global tracer provider and
// W3C TraceContext propagator with Setup, wrap inbound handlers with Middleware and
// outbound clients with Transport. Span attributes never carry request content: no
// prompts, responses, tool arguments, credentials, URL paths or query strings.
package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"gopkg.in/yaml.v3"
)

// ScopeName is the instrumentation scope used for spans created by this package.
const ScopeName = "github.com/azylman/aerial/pkg/tracing"

// Attribute keys shared by Aerial services.
const (
	AttrStack         = attribute.Key("aerial.stack")
	AttrMCPMethodName = attribute.Key("mcp.method.name")
	AttrGenAIToolName = attribute.Key("gen_ai.tool.name")
)

// Config is the content of /config/tracing/tracing.yaml.
type Config struct {
	Enabled     bool    `yaml:"enabled"`
	Endpoint    string  `yaml:"endpoint"`
	SampleRatio float64 `yaml:"sample_ratio"`
	Stack       string  `yaml:"stack"`
}

// LoadConfig reads a tracing config file. A missing file yields a disabled Config and no
// error so services run untraced until the config is delivered. Malformed YAML, or an
// enabled config without an endpoint, returns an error; callers log it and run disabled.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read tracing config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse tracing config: %w", err)
	}
	if cfg.Enabled && strings.TrimSpace(cfg.Endpoint) == "" {
		return Config{}, errors.New("tracing config: endpoint is required when enabled")
	}
	if cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return Config{}, fmt.Errorf("tracing config: sample_ratio %v must be within [0, 1]", cfg.SampleRatio)
	}
	return cfg, nil
}

// sampleRatio treats an unset (zero) ratio as "sample everything"; disabling is done with enabled: false.
func (c Config) sampleRatio() float64 {
	if c.SampleRatio <= 0 {
		return 1
	}
	return c.SampleRatio
}

// Setup installs the global W3C TraceContext propagator and tracer provider for service.
// A disabled config installs a noop provider: no spans are exported, but inbound
// traceparent headers are still extracted and forwarded on outbound calls. The returned
// shutdown flushes pending spans and must be called on SIGTERM.
func Setup(ctx context.Context, service string, cfg Config) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(cfg.Endpoint))
	if err != nil {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, fmt.Errorf("create otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.sampleRatio()))),
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", service),
			AttrStack.String(cfg.Stack),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Traceparent renders the span context carried by ctx as a W3C traceparent header value,
// or "" when ctx carries no valid span context. Used to persist trace context on queued work.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Middleware extracts inbound trace context and wraps next in a server span named
// "<METHOD> <route pattern>" once the mux has routed the request (operation when no
// route matched). Attributes never include the URL path or query string.
func Middleware(operation string, next http.Handler) http.Handler {
	return newMiddleware(otel.GetTracerProvider(), otel.GetTextMapPropagator(), operation, next)
}

func newMiddleware(tp trace.TracerProvider, prop propagation.TextMapPropagator, operation string, next http.Handler) http.Handler {
	tracer := tp.Tracer(ScopeName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, operation,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method)),
		)
		defer span.End()

		rec := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		req := r.WithContext(ctx)
		next.ServeHTTP(rec, req)

		if route := routeFromPattern(req.Pattern); route != "" {
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
		span.SetAttributes(attribute.Int("http.response.status_code", rec.status))
		if rec.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, "")
		}
	})
}

// routeFromPattern strips the optional method and host from a ServeMux pattern.
func routeFromPattern(pattern string) string {
	if _, after, ok := strings.Cut(pattern, " "); ok {
		pattern = strings.TrimSpace(after)
	}
	if i := strings.IndexByte(pattern, '/'); i > 0 {
		pattern = pattern[i:]
	}
	return pattern
}

type statusWriter struct {
	http.ResponseWriter

	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Transport wraps base (http.DefaultTransport when nil) with client spans and traceparent
// injection. The url.full attribute is reduced to scheme://host.
func Transport(base http.RoundTripper) http.RoundTripper {
	return newTransport(otel.GetTracerProvider(), otel.GetTextMapPropagator(), base)
}

func newTransport(tp trace.TracerProvider, prop propagation.TextMapPropagator, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{base: base, tracer: tp.Tracer(ScopeName), prop: prop}
}

type transport struct {
	base   http.RoundTripper
	tracer trace.Tracer
	prop   propagation.TextMapPropagator
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, span := t.tracer.Start(r.Context(), r.Method,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("server.address", r.URL.Hostname()),
			attribute.String("url.full", originOnly(r.URL)),
		),
	)
	defer span.End()

	out := r.Clone(ctx)
	t.prop.Inject(ctx, propagation.HeaderCarrier(out.Header))
	resp, err := t.base.RoundTrip(out)
	if err != nil {
		span.SetStatus(codes.Error, Redact(transportErrorMessage(err)))
		return resp, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, "")
	}
	return resp, nil
}

func originOnly(u *url.URL) string {
	if u == nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
}

// transportErrorMessage drops the request URL that *url.Error embeds in its message.
func transportErrorMessage(err error) string {
	var uerr *url.Error
	if errors.As(err, &uerr) && uerr.Err != nil {
		return uerr.Op + ": " + uerr.Err.Error()
	}
	return err.Error()
}

// MCPRequestAttrs extracts the JSON-RPC method (and the tool name for tools/call) from an
// MCP request body. Arguments and every other param are never returned.
func MCPRequestAttrs(body []byte) []attribute.KeyValue {
	var req struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Method == "" {
		return nil
	}
	attrs := []attribute.KeyValue{AttrMCPMethodName.String(req.Method)}
	if req.Method != "tools/call" {
		return attrs
	}
	var call struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(req.Params, &call) == nil && call.Name != "" {
		attrs = append(attrs, AttrGenAIToolName.String(call.Name))
	}
	return attrs
}

// Redact masks GitHub PATs, bearer tokens, basic auth credentials and other known secrets
// before a string becomes a span status or attribute.
func Redact(s string) string {
	return sanitizer.SanitizeString(s)
}
