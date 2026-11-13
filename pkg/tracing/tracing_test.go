package tracing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	parentTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	parentSpanID      = "00f067aa0ba902b7"
	parentTraceparent = "00-" + parentTraceID + "-" + parentSpanID + "-01"
)

func newRecorder(t *testing.T) (*tracetest.SpanRecorder, trace.TracerProvider) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return sr, tp
}

func attrMap(kvs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string // "" means the file is absent
		want    Config
		wantErr bool
	}{
		{name: "missing file is disabled", want: Config{}},
		{
			name: "valid", content: "enabled: true\nendpoint: http://vt.aerial/insert/opentelemetry/v1/traces\nsample_ratio: 0.5\nstack: aerial\n",
			want: Config{Enabled: true, Endpoint: "http://vt.aerial/insert/opentelemetry/v1/traces", SampleRatio: 0.5, Stack: "aerial"},
		},
		{name: "explicitly disabled", content: "enabled: false\n", want: Config{}},
		{name: "malformed yaml", content: "enabled: [true\n", wantErr: true},
		{name: "enabled without endpoint", content: "enabled: true\n", wantErr: true},
		{name: "ratio above one", content: "enabled: true\nendpoint: http://x\nsample_ratio: 2\n", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "tracing.yaml")
			if tc.content != "" {
				if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := LoadConfig(path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("LoadConfig = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadConfig_Unreadable(t *testing.T) {
	t.Parallel()
	if _, err := LoadConfig(t.TempDir()); err == nil {
		t.Fatal("expected error reading a directory")
	}
}

func TestConfigSampleRatio(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   float64
		want float64
	}{
		{name: "unset samples everything", in: 0, want: 1},
		{name: "explicit ratio", in: 0.25, want: 0.25},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := (Config{SampleRatio: tc.in}).sampleRatio(); got != tc.want {
				t.Errorf("sampleRatio = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSetup mutates the global provider and propagator, so it is not parallel.
func TestSetup(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		wantErr      bool
		wantForwards bool // inbound traceparent survives on the extracted context
	}{
		{name: "disabled forwards context", cfg: Config{}, wantForwards: true},
		{name: "enabled", cfg: Config{Enabled: true, Endpoint: "http://127.0.0.1:1/v1/traces", Stack: "aerial"}, wantForwards: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shutdown, err := Setup(context.Background(), "eri", tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Setup err = %v, wantErr %v", err, tc.wantErr)
			}
			defer func() {
				if err := shutdown(context.Background()); err != nil {
					t.Errorf("shutdown: %v", err)
				}
			}()
			var seen string
			h := Middleware("test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = trace.SpanContextFromContext(r.Context()).TraceID().String()
			}))
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			// Unsampled parent: ParentBased drops the span, so nothing is ever exported.
			req.Header.Set("Traceparent", "00-"+parentTraceID+"-"+parentSpanID+"-00")
			h.ServeHTTP(httptest.NewRecorder(), req)
			if (seen == parentTraceID) != tc.wantForwards {
				t.Errorf("trace id in handler = %q, want forwarded=%v", seen, tc.wantForwards)
			}
		})
	}
}

func TestTraceparent(t *testing.T) {
	t.Parallel()
	remote := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parentTraceparent})
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "no span context", ctx: context.Background(), want: ""},
		{name: "remote span context", ctx: remote, want: parentTraceparent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Traceparent(tc.ctx); got != tc.want {
				t.Errorf("Traceparent = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		pattern     string
		target      string
		traceparent string
		status      int
		wantName    string
		wantRoute   string
		wantParent  bool
		wantError   bool
	}{
		{
			name: "routed with parent", pattern: "/prompt", target: "/prompt?token=ghp_secret", traceparent: parentTraceparent,
			status: http.StatusAccepted, wantName: "POST /prompt", wantRoute: "/prompt", wantParent: true,
		},
		{
			name: "method pattern without parent", pattern: "POST /tasks/{id}", target: "/tasks/42?q=1",
			status: http.StatusOK, wantName: "POST /tasks/{id}", wantRoute: "/tasks/{id}",
		},
		{
			name: "server error", pattern: "/dispatch", target: "/dispatch", status: http.StatusBadGateway,
			wantName: "POST /dispatch", wantRoute: "/dispatch", wantError: true,
		},
		{
			name: "unmatched keeps operation name", pattern: "/prompt", target: "/nope", status: http.StatusNotFound,
			wantName: "eri",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sr, tp := newRecorder(t)
			var handlerTP string
			mux := http.NewServeMux()
			mux.HandleFunc(tc.pattern, func(w http.ResponseWriter, r *http.Request) {
				handlerTP = Traceparent(r.Context())
				w.WriteHeader(tc.status)
			})
			req := httptest.NewRequest(http.MethodPost, tc.target, nil)
			if tc.traceparent != "" {
				req.Header.Set("Traceparent", tc.traceparent)
			}
			newMiddleware(tp, propagation.TraceContext{}, "eri", mux).ServeHTTP(httptest.NewRecorder(), req)

			spans := sr.Ended()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			s := spans[0]
			if s.Name() != tc.wantName || s.SpanKind() != trace.SpanKindServer {
				t.Errorf("span = %q kind %v, want %q server", s.Name(), s.SpanKind(), tc.wantName)
			}
			if got := s.Parent().TraceID().String() == parentTraceID; got != tc.wantParent {
				t.Errorf("parent trace = %s, want parented=%v", s.Parent().TraceID(), tc.wantParent)
			}
			if (s.Status().Code == codes.Error) != tc.wantError {
				t.Errorf("status = %v, wantError %v", s.Status(), tc.wantError)
			}
			attrs := attrMap(s.Attributes())
			if attrs["http.route"] != tc.wantRoute {
				t.Errorf("http.route = %q, want %q", attrs["http.route"], tc.wantRoute)
			}
			for k, v := range attrs {
				if strings.Contains(v, "?") || strings.Contains(v, "ghp_") || strings.HasPrefix(k, "url.") {
					t.Errorf("attribute %s=%q leaks URL content", k, v)
				}
			}
			if tc.status != http.StatusNotFound && !strings.Contains(handlerTP, s.SpanContext().SpanID().String()) {
				t.Errorf("handler traceparent %q does not carry server span %s", handlerTP, s.SpanContext().SpanID())
			}
		})
	}
}

func TestStatusWriter(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	w := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	_, _ = w.Write([]byte("hi"))
	w.WriteHeader(http.StatusTeapot) // superfluous after a write; must not change recorded status
	w.Flush()
	if w.status != http.StatusOK || !rec.Flushed || w.Unwrap() != rec {
		t.Errorf("status=%d flushed=%v", w.status, rec.Flushed)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		url        string
		respStatus int
		respErr    error
		wantError  bool
		wantDesc   string
	}{
		{name: "ok strips path and query", url: "https://api.github.com/repos/x/y?access_token=ghp_abcdefghijklmnopqrstuvwxyz0123456789", respStatus: http.StatusOK},
		{name: "server error", url: "http://haro.aerial/dispatch", respStatus: http.StatusServiceUnavailable, wantError: true},
		{
			name: "transport error is redacted", url: "http://user:hunter2@coven.aerial/v1?q=secret",
			respErr: errors.New("dial failed Authorization: Bearer abcdefghijklmnopqrstuvwxyz"), wantError: true,
			wantDesc: "dial failed Authorization: [REDACTED]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sr, tp := newRecorder(t)
			var gotTP string
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotTP = r.Header.Get("Traceparent")
				if tc.respErr != nil {
					return nil, tc.respErr
				}
				return &http.Response{StatusCode: tc.respStatus, Body: http.NoBody, Request: r}, nil
			})
			ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parentTraceparent})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: newTransport(tp, propagation.TraceContext{}, base)}
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			if req.Header.Get("Traceparent") != "" {
				t.Error("caller's request was mutated")
			}

			spans := sr.Ended()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			s := spans[0]
			if s.SpanKind() != trace.SpanKindClient || s.Parent().SpanID().String() != parentSpanID {
				t.Errorf("span kind %v parent %s", s.SpanKind(), s.Parent().SpanID())
			}
			if want := "00-" + parentTraceID + "-" + s.SpanContext().SpanID().String() + "-01"; gotTP != want {
				t.Errorf("injected traceparent = %q, want %q", gotTP, want)
			}
			if (s.Status().Code == codes.Error) != tc.wantError || s.Status().Description != tc.wantDesc {
				t.Errorf("status = %+v, want error=%v desc=%q", s.Status(), tc.wantError, tc.wantDesc)
			}
			full := attrMap(s.Attributes())["url.full"]
			if strings.Count(full, "/") != 2 || strings.ContainsAny(full, "?@") {
				t.Errorf("url.full = %q, want scheme://host only", full)
			}
		})
	}
}

func TestTransport_DefaultBase(t *testing.T) {
	t.Parallel()
	if rt := Transport(nil).(*transport); rt.base != http.DefaultTransport {
		t.Errorf("base = %T, want http.DefaultTransport", rt.base)
	}
	if originOnly(nil) != "" {
		t.Error("originOnly(nil) should be empty")
	}
}

func TestMCPRequestAttrs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want map[string]string
	}{
		{
			name: "tools/call", body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"pods_list","arguments":{"namespace":"secret-ns","token":"ghp_x"}}}`,
			want: map[string]string{"mcp.method.name": "tools/call", "gen_ai.tool.name": "pods_list"},
		},
		{
			name: "initialize", body: `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"name":"not-a-tool","clientInfo":{"name":"eri"}}}`,
			want: map[string]string{"mcp.method.name": "initialize"},
		},
		{
			name: "tools/list", body: `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
			want: map[string]string{"mcp.method.name": "tools/list"},
		},
		{
			name: "tools/call with array params", body: `{"jsonrpc":"2.0","method":"tools/call","params":["x"]}`,
			want: map[string]string{"mcp.method.name": "tools/call"},
		},
		{name: "malformed body", body: `{"method":`, want: map[string]string{}},
		{name: "response without method", body: `{"jsonrpc":"2.0","id":1,"result":{}}`, want: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := attrMap(MCPRequestAttrs([]byte(tc.body)))
			if len(got) != len(tc.want) {
				t.Fatalf("attrs = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
			for k, v := range got {
				if strings.Contains(v, "secret-ns") || strings.Contains(v, "ghp_") || strings.Contains(k, "argument") {
					t.Errorf("arguments leaked: %s=%q", k, v)
				}
			}
		})
	}
}

func TestRedact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "github PAT leak", in: "push failed for ghp_abcdefghijklmnopqrstuvwxyz0123456789", want: "push failed for [REDACTED]"},
		{name: "fine-grained PAT", in: "token github_pat_11ABCDEFG0123456789_abcdefghij", want: "token [REDACTED]"},
		{name: "bearer token", in: "Authorization: Bearer abcdefghijklmnopqrstuvwxyz", want: "Authorization: [REDACTED]"},
		{name: "basic auth header", in: "Authorization: Basic dXNlcjpodW50ZXIy", want: "Authorization: [REDACTED]"},
		{name: "basic auth in url", in: "GET https://user:hunter2@example.com/repo", want: "GET https://user:[REDACTED]@example.com/repo"},
		{name: "clean string", in: "context deadline exceeded", want: "context deadline exceeded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Redact(tc.in); got != tc.want {
				t.Errorf("Redact(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRouteFromPattern(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in, want string }{
		{name: "path only", in: "/prompt", want: "/prompt"},
		{name: "method and path", in: "GET /tasks/{id}", want: "/tasks/{id}"},
		{name: "method host and path", in: "POST eri.aerial/dispatch", want: "/dispatch"},
		{name: "unrouted", in: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := routeFromPattern(tc.in); got != tc.want {
				t.Errorf("routeFromPattern(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTransportErrorMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "url error drops url", err: &url.Error{Op: "Get", URL: "https://h/p?token=x", Err: errors.New("timeout")}, want: "Get: timeout"},
		{name: "plain error", err: errors.New("boom"), want: "boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := transportErrorMessage(tc.err); got != tc.want {
				t.Errorf("transportErrorMessage = %q, want %q", got, tc.want)
			}
		})
	}
}
