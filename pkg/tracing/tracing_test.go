package tracing

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/azylman/aerial/pkg/hermetictest"
)

func TestMain(m *testing.M) {
	hermetictest.Main(m)
}

const (
	parentTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	parentSpanID      = "00f067aa0ba902b7"
	parentTraceparent = "00-" + parentTraceID + "-" + parentSpanID + "-01"
)

func newRecorder(t *testing.T) (*tracetest.SpanRecorder, trace.TracerProvider) {
	t.Helper()
	tp, sr := hermetictest.NewRecorder()
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
		file    string // path under the temp dir; "" means tracing.yaml, "." the directory itself
		content string // "" means the file is absent
		want    Config
		wantErr bool
	}{
		{name: "missing file is disabled", want: Config{}},
		{name: "unreadable path (a directory)", file: ".", wantErr: true},
		{
			name: "agent config key", content: "enabled: true\nendpoint: http://vt.aerial/insert/opentelemetry/v1/traces\nsample_ratio: 0.5\nagent: aerial\n",
			want: Config{Enabled: true, Endpoint: "http://vt.aerial/insert/opentelemetry/v1/traces", SampleRatio: 0.5, Agent: "aerial"},
		},
		{
			name: "valid", content: "enabled: true\nendpoint: http://vt.aerial/insert/opentelemetry/v1/traces\nsample_ratio: 0.5\nstack: aerial\n",
			want: Config{Enabled: true, Endpoint: "http://vt.aerial/insert/opentelemetry/v1/traces", SampleRatio: 0.5, Stack: "aerial"},
		},
		{name: "explicitly disabled", content: "enabled: false\n", want: Config{}},
		{
			name: "capture_content on", content: "enabled: true\nendpoint: http://x\ncapture_content: true\n",
			want: Config{Enabled: true, Endpoint: "http://x", CaptureContent: true},
		},
		{name: "capture_content absent is off", content: "enabled: true\nendpoint: http://x\n", want: Config{Enabled: true, Endpoint: "http://x"}},
		{name: "malformed yaml", content: "enabled: [true\n", wantErr: true},
		{name: "enabled without endpoint", content: "enabled: true\n", wantErr: true},
		{name: "ratio above one", content: "enabled: true\nendpoint: http://x\nsample_ratio: 2\n", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			file := tc.file
			if file == "" {
				file = "tracing.yaml"
			}
			path := filepath.Join(t.TempDir(), file)
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

func TestContentAttr(t *testing.T) {
	t.Parallel()
	// 16384 runes exactly fit; one more truncates. "é" checks the cap counts runes, not bytes.
	fits := strings.Repeat("é", 16384)
	over := "AB" + strings.Repeat("é", 16383)
	tests := []struct {
		name    string
		key     attribute.Key
		role    string
		text    string
		wantKey string
		want    string
	}{
		{
			name: "user prompt", key: AttrGenAIInputMessages, role: RoleUser, text: "hi <@123> & bye",
			wantKey: "gen_ai.input.messages",
			want:    `[{"role":"user","parts":[{"type":"text","content":"hi <@123> & bye"}]}]`,
		},
		{
			name: "assistant reply", key: AttrGenAIOutputMessages, role: RoleAssistant, text: "line1\n\"quoted\"",
			wantKey: "gen_ai.output.messages",
			want:    `[{"role":"assistant","parts":[{"type":"text","content":"line1\n\"quoted\""}]}]`,
		},
		{
			name: "empty text", key: AttrGenAIOutputMessages, role: RoleAssistant, text: "",
			wantKey: "gen_ai.output.messages",
			want:    `[{"role":"assistant","parts":[{"type":"text","content":""}]}]`,
		},
		{
			name: "input at the cap is kept whole", key: AttrGenAIInputMessages, role: RoleUser, text: fits,
			wantKey: "gen_ai.input.messages",
			want:    `[{"role":"user","parts":[{"type":"text","content":"` + fits + `"}]}]`,
		},
		{
			name: "input over the cap keeps the tail", key: AttrGenAIInputMessages, role: RoleUser, text: over,
			wantKey: "gen_ai.input.messages",
			want:    `[{"role":"user","parts":[{"type":"text","content":"[truncated]…` + strings.Repeat("é", 16372) + `"}]}]`,
		},
		{
			name: "output over the cap keeps the head", key: AttrGenAIOutputMessages, role: RoleAssistant, text: over,
			wantKey: "gen_ai.output.messages",
			want:    `[{"role":"assistant","parts":[{"type":"text","content":"AB` + strings.Repeat("é", 16370) + `…[truncated]"}]}]`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ContentAttr(tc.key, tc.role, tc.text)
			if string(got.Key) != tc.wantKey {
				t.Errorf("key = %q, want %q", got.Key, tc.wantKey)
			}
			if got.Value.AsString() != tc.want {
				t.Errorf("value = %.200q..., want %.200q...", got.Value.AsString(), tc.want)
			}
		})
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

// setupEnvKeys are the OTEL variables the SDK reads; TestSetup blanks them so the ambient env cannot change a row.
var setupEnvKeys = []string{
	"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL",
	"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
	"OTEL_EXPORTER_OTLP_TIMEOUT", "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "OTEL_EXPORTER_OTLP_COMPRESSION",
	"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
	"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "OTEL_SERVICE_NAME",
}

func blankSetupEnv(t *testing.T) {
	t.Helper()
	for _, k := range setupEnvKeys {
		t.Setenv(k, "")
	}
}

// TestSetup mutates the global provider, propagator and environment, so it is not parallel.
func TestSetup(t *testing.T) {
	tests := []struct {
		name            string
		cfg             Config
		env             map[string]string
		wantForwards    bool // inbound traceparent survives on the extracted context
		wantRootSampled bool // a new root span is recorded and sampled
		// envEndpointCollector points OTEL_EXPORTER_OTLP_TRACES_ENDPOINT at a local collector;
		// wantCollectorHit expects an ended root span to be exported there on shutdown.
		envEndpointCollector bool
		wantCollectorHit     bool
	}{
		{name: "disabled forwards context", cfg: Config{}, wantForwards: true},
		{
			name: "enabled", cfg: Config{Enabled: true, Endpoint: "http://127.0.0.1:1/v1/traces", Stack: "aerial"},
			wantForwards: true, wantRootSampled: true,
		},
		{
			name: "yaml ratio sampler", cfg: Config{Enabled: true, Endpoint: "http://127.0.0.1:1/v1/traces", SampleRatio: 0.0000001},
			wantForwards: true,
		},
		{
			name: "OTEL_TRACES_SAMPLER replaces yaml sampler",
			cfg:  Config{Enabled: true, Endpoint: "http://127.0.0.1:1/v1/traces", SampleRatio: 0.0000001},
			env:  map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_always_on"}, wantForwards: true, wantRootSampled: true,
		},
		{
			name: "OTEL endpoint replaces yaml endpoint", cfg: Config{Enabled: true, Endpoint: "http://127.0.0.1:1/v1/traces"},
			envEndpointCollector: true, wantForwards: true, wantRootSampled: true, wantCollectorHit: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blankSetupEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			hits := new(atomic.Int32)
			if tc.envEndpointCollector {
				hits = startEnvCollector(t)
			}
			shutdown, err := Setup(context.Background(), "eri", tc.cfg)
			if err != nil {
				t.Fatalf("Setup err = %v, want nil", err)
			}
			seen, seenBaggage := serveThroughGlobalMiddleware()
			if (seen == parentTraceID) != tc.wantForwards {
				t.Errorf("trace id in handler = %q, want forwarded=%v", seen, tc.wantForwards)
			}
			if seenBaggage != "aerial" {
				t.Errorf("baggage in handler = %q, want %q", seenBaggage, "aerial")
			}
			// Only ended when a local collector is listening, so nothing is queued for a dead endpoint.
			_, span := Tracer().Start(context.Background(), "root")
			if span.SpanContext().IsSampled() != tc.wantRootSampled {
				t.Errorf("root span sampled = %v, want %v", span.SpanContext().IsSampled(), tc.wantRootSampled)
			}
			if tc.envEndpointCollector {
				span.End()
			}
			if err := shutdown(context.Background()); err != nil {
				t.Errorf("shutdown: %v", err)
			}
			if got := hits.Load() > 0; got != tc.wantCollectorHit {
				t.Errorf("collector hit = %v, want %v", got, tc.wantCollectorHit)
			}
		})
	}
}

// startEnvCollector points OTEL_EXPORTER_OTLP_TRACES_ENDPOINT at a local OTLP collector and
// returns its count of trace export requests.
func startEnvCollector(t *testing.T) *atomic.Int32 {
	t.Helper()
	hits := new(atomic.Int32)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/traces" {
			hits.Add(1)
		}
	}))
	t.Cleanup(collector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	return hits
}

// serveThroughGlobalMiddleware sends a request with an unsampled parent and baggage through
// Middleware built from the global provider and propagator, returning the trace ID and
// "tenant" baggage the handler saw. ParentBased drops the unsampled span, so nothing exports.
func serveThroughGlobalMiddleware() (traceID, tenant string) {
	h := Middleware("test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID = trace.SpanContextFromContext(r.Context()).TraceID().String()
		tenant = baggage.FromContext(r.Context()).Member("tenant").Value()
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", http.NoBody)
	req.Header.Set("Traceparent", "00-"+parentTraceID+"-"+parentSpanID+"-00")
	req.Header.Set("Baggage", "tenant=aerial")
	h.ServeHTTP(httptest.NewRecorder(), req)
	return traceID, tenant
}

func TestEnvOverrides(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  map[string]string
		want sdkEnvOverrides
	}{
		{name: "nothing set keeps yaml", env: map[string]string{}},
		{name: "traces endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://vt:10428/v1/traces"}, want: sdkEnvOverrides{endpoint: true}},
		{name: "generic endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://vt:10428"}, want: sdkEnvOverrides{endpoint: true}},
		{name: "blank endpoint ignored", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "  "}},
		{name: "sampler", env: map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_always_on"}, want: sdkEnvOverrides{sampler: true}},
		{name: "sampler arg alone keeps yaml", env: map[string]string{"OTEL_TRACES_SAMPLER_ARG": "0.5"}},
		{
			name: "both", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://vt:10428", "OTEL_TRACES_SAMPLER": "always_off"},
			want: sdkEnvOverrides{endpoint: true, sampler: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := envOverrides(func(k string) string { return tc.env[k] }); got != tc.want {
				t.Errorf("envOverrides = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestNewResource sets process environment (t.Setenv), so it is not parallel.
func TestNewResource(t *testing.T) {
	tests := []struct {
		name       string
		agent      string
		stack      string
		env        map[string]string
		want       map[string]string
		wantAbsent []string
		wantErr    bool
	}{
		{
			name: "agent becomes service.namespace and aerial.agent", agent: "agent-alpha",
			want: map[string]string{
				"service.name": "haro", "service.namespace": "agent-alpha", "aerial.agent": "agent-alpha", "aerial.stack": "agent-alpha",
				"telemetry.sdk.language": "go", "telemetry.sdk.name": "opentelemetry",
			},
		},
		{
			name: "stack fallback becomes service.namespace and aerial.agent", stack: "agent-alpha",
			want: map[string]string{
				"service.name": "haro", "service.namespace": "agent-alpha", "aerial.agent": "agent-alpha", "aerial.stack": "agent-alpha",
				"telemetry.sdk.language": "go", "telemetry.sdk.name": "opentelemetry",
			},
		},
		{
			name: "agent takes precedence over stack", agent: "agent-beta", stack: "agent-alpha",
			want: map[string]string{
				"service.name": "haro", "service.namespace": "agent-beta", "aerial.agent": "agent-beta", "aerial.stack": "agent-beta",
				"telemetry.sdk.language": "go", "telemetry.sdk.name": "opentelemetry",
			},
		},
		{
			name: "empty agent and stack omits namespace and aerial.agent",
			want: map[string]string{"service.name": "haro", "aerial.stack": ""}, wantAbsent: []string{"service.namespace", "aerial.agent"},
		},
		{
			name: "OTEL_RESOURCE_ATTRIBUTES added", agent: "aerial",
			env: map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "k8s.pod.name=haro-7d9,k8s.namespace.name=default,k8s.node.name=node-1,service.version=v1.2.3"},
			want: map[string]string{
				"service.name": "haro", "service.namespace": "aerial", "aerial.agent": "aerial", "aerial.stack": "aerial", "k8s.pod.name": "haro-7d9",
				"k8s.namespace.name": "default", "k8s.node.name": "node-1", "service.version": "v1.2.3",
			},
		},
		{
			name: "OTEL_SERVICE_NAME overrides service.name", agent: "agent-beta",
			env:  map[string]string{"OTEL_SERVICE_NAME": "haro-canary"},
			want: map[string]string{"service.name": "haro-canary", "service.namespace": "agent-beta", "aerial.agent": "agent-beta", "aerial.stack": "agent-beta"},
		},
		{
			name: "malformed attributes keep the rest", agent: "aerial",
			env:     map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "k8s.pod.name=haro-7d9,broken"},
			want:    map[string]string{"service.name": "haro", "service.namespace": "aerial", "aerial.agent": "aerial", "aerial.stack": "aerial", "k8s.pod.name": "haro-7d9"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tc.env["OTEL_RESOURCE_ATTRIBUTES"])
			t.Setenv("OTEL_SERVICE_NAME", tc.env["OTEL_SERVICE_NAME"])
			res, err := newResource(context.Background(), "haro", Config{Agent: tc.agent, Stack: tc.stack})
			if (err != nil) != tc.wantErr {
				t.Fatalf("newResource err = %v, wantErr %v", err, tc.wantErr)
			}
			attrs := attrMap(res.Attributes())
			for k, v := range tc.want {
				if got, ok := attrs[k]; !ok || got != v {
					t.Errorf("resource %s = %q (present %v), want %q", k, got, ok, v)
				}
			}
			for _, k := range tc.wantAbsent {
				if _, ok := attrs[k]; ok {
					t.Errorf("resource has %s, want absent", k)
				}
			}
			if res.SchemaURL() != "https://opentelemetry.io/schemas/1.43.0" {
				t.Errorf("schema URL = %q, want %q", res.SchemaURL(), "https://opentelemetry.io/schemas/1.43.0")
			}
		})
	}
}

func TestInit(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	tests := []struct {
		name        string
		path        string
		wantEnabled bool
		wantStack   string
	}{
		{name: "missing file runs disabled", path: filepath.Join(dir, "absent.yaml")},
		{name: "malformed yaml runs disabled", path: write("bad.yaml", "enabled: [")},
		{name: "enabled config exports", path: write("ok.yaml", "enabled: true\nendpoint: http://127.0.0.1:1/v1/traces\nstack: agent-gamma\n"), wantEnabled: true, wantStack: "agent-gamma"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, flush := Init(context.Background(), "haro", tc.path)
			// Nothing is sampled-and-ended here, so the flush never reaches the dead endpoint.
			defer flush()
			if cfg.Enabled != tc.wantEnabled || cfg.Stack != tc.wantStack {
				t.Errorf("Init cfg = %+v, want enabled=%v stack=%q", cfg, tc.wantEnabled, tc.wantStack)
			}
			// Left unended so nothing is ever queued for export to the dead endpoint.
			if _, span := Tracer().Start(context.Background(), "probe"); span.SpanContext().IsValid() != tc.wantEnabled {
				t.Errorf("Tracer span valid = %v, want %v", span.SpanContext().IsValid(), tc.wantEnabled)
			}
			if got := Traceparent(otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier{"traceparent": parentTraceparent})); got != parentTraceparent {
				t.Errorf("global propagator did not round-trip traceparent: %q", got)
			}
		})
	}
}

func TestContextWithTraceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traceparent string
		wantTraceID string
	}{
		{name: "empty leaves context untouched", traceparent: "", wantTraceID: "00000000000000000000000000000000"},
		{name: "malformed is ignored", traceparent: "00-nothex-00f067aa0ba902b7-01", wantTraceID: "00000000000000000000000000000000"},
		{name: "valid becomes remote parent", traceparent: parentTraceparent, wantTraceID: parentTraceID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sc := trace.SpanContextFromContext(ContextWithTraceparent(context.Background(), tc.traceparent))
			if sc.TraceID().String() != tc.wantTraceID {
				t.Errorf("trace id = %s, want %s", sc.TraceID(), tc.wantTraceID)
			}
			if sc.IsValid() && !sc.IsRemote() {
				t.Error("span context should be remote")
			}
		})
	}
}

// TestClientWrappers covers the shared contract of Client, ExternalClient, Transport and
// ExternalTransport: each wraps its base (http.DefaultTransport when nil) in *transport with the
// wrapper's propagate mode, and the client forms return a copy that keeps timeout and settings.
// Transport rows wrap the result in a bare client so every row asserts the same shape.
func TestClientWrappers(t *testing.T) {
	t.Parallel()
	base := &http.Transport{}
	internal := &http.Client{Timeout: 7 * time.Second, Transport: base}
	external := &http.Client{Timeout: 20 * time.Second, Transport: base}
	tests := []struct {
		name          string
		in            *http.Client
		got           *http.Client
		wantTimeout   time.Duration
		wantBase      http.RoundTripper
		wantPropagate bool
	}{
		{name: "Client nil gets default transport", got: Client(nil), wantBase: http.DefaultTransport, wantPropagate: true},
		{name: "Client keeps settings and base", in: internal, got: Client(internal), wantTimeout: 7 * time.Second, wantBase: base, wantPropagate: true},
		{name: "ExternalClient nil gets default transport", got: ExternalClient(nil), wantBase: http.DefaultTransport},
		{name: "ExternalClient keeps settings and base", in: external, got: ExternalClient(external), wantTimeout: 20 * time.Second, wantBase: base},
		{name: "Transport nil gets default transport", got: &http.Client{Transport: Transport(nil)}, wantBase: http.DefaultTransport, wantPropagate: true},
		{name: "ExternalTransport nil gets default transport", got: &http.Client{Transport: ExternalTransport(nil)}, wantBase: http.DefaultTransport},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.in != nil && tc.got == tc.in {
				t.Fatal("wrapper must return a copy")
			}
			if tc.in != nil && tc.in.Transport != base {
				t.Error("input client transport was mutated")
			}
			rt, ok := tc.got.Transport.(*transport)
			if !ok {
				t.Fatalf("transport = %T, want *transport", tc.got.Transport)
			}
			if tc.got.Timeout != tc.wantTimeout || rt.base != tc.wantBase || rt.propagate != tc.wantPropagate {
				t.Errorf("timeout %v base %T propagate %v, want %v %T %v", tc.got.Timeout, rt.base, rt.propagate, tc.wantTimeout, tc.wantBase, tc.wantPropagate)
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
			if got := TraceparentFromContext(tc.ctx); got != tc.want {
				t.Errorf("TraceparentFromContext = %q, want %q", got, tc.want)
			}
		})
	}
}

type middlewareTestCase struct {
	name        string
	pattern     string
	target      string
	traceparent string
	preRouted   string
	rename      string
	// opaqueNext hides the mux behind a plain handler, so routing is only visible afterwards.
	opaqueNext bool
	status     int
	wantName   string
	wantRoute  string
	wantParent bool
	wantError  bool
	wantNoSpan bool // probe path: served with no span and no extracted context
}

func runMiddlewareCase(t *testing.T, tc middlewareTestCase) {
	t.Helper()
	sr, tp := newRecorder(t)
	var handlerTP string
	mux := http.NewServeMux()
	mux.HandleFunc(tc.pattern, func(w http.ResponseWriter, r *http.Request) {
		handlerTP = Traceparent(r.Context())
		if tc.rename != "" {
			trace.SpanFromContext(r.Context()).SetName(tc.rename)
		}
		w.WriteHeader(tc.status)
	})
	req := httptest.NewRequest(http.MethodPost, tc.target, http.NoBody)
	req.Pattern = tc.preRouted
	if tc.traceparent != "" {
		req.Header.Set("Traceparent", tc.traceparent)
	}
	var next http.Handler = mux
	if tc.opaqueNext {
		next = http.HandlerFunc(mux.ServeHTTP)
	}
	rec := httptest.NewRecorder()
	newMiddleware(tp, propagation.TraceContext{}, "eri", next).ServeHTTP(rec, req)
	if rec.Code != tc.status {
		t.Errorf("response status = %d, want %d", rec.Code, tc.status)
	}
	if tc.wantNoSpan {
		if n := len(sr.Ended()) + len(sr.Started()); n != 0 {
			t.Errorf("probe recorded %d spans, want 0", n)
		}
		if handlerTP != "" {
			t.Errorf("probe handler saw trace context %q, want none", handlerTP)
		}
		return
	}
	assertMiddlewareSpan(t, tc, sr, handlerTP)
}

func assertMiddlewareSpan(t *testing.T, tc middlewareTestCase, sr *tracetest.SpanRecorder, handlerTP string) {
	t.Helper()
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
}

func TestMiddleware(t *testing.T) {
	t.Parallel()
	tests := []middlewareTestCase{
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
		{
			name: "pre-routed handler rename sticks", pattern: "/mcp", target: "/mcp", preRouted: "/mcp", rename: "mcp tools/call",
			status: http.StatusOK, wantName: "mcp tools/call", wantRoute: "/mcp",
		},
		{
			name: "whole mux routes up front so handler rename sticks", pattern: "/mcp", target: "/mcp", rename: "mcp tools/call",
			status: http.StatusOK, wantName: "mcp tools/call", wantRoute: "/mcp",
		},
		{
			name: "opaque handler post-routed name replaces handler rename", pattern: "/mcp", target: "/mcp", rename: "mcp tools/call", opaqueNext: true,
			status: http.StatusOK, wantName: "POST /mcp", wantRoute: "/mcp",
		},
		{
			name: "opaque handler unmatched keeps operation name", pattern: "/prompt", target: "/nope", opaqueNext: true,
			status: http.StatusNotFound, wantName: "eri",
		},
		{name: "health probe untraced", pattern: "/health", target: "/health", traceparent: parentTraceparent, status: http.StatusOK, wantNoSpan: true},
		{name: "healthz probe untraced", pattern: "/healthz", target: "/healthz", traceparent: parentTraceparent, status: http.StatusOK, wantNoSpan: true},
		{name: "readyz probe untraced", pattern: "/readyz", target: "/readyz", traceparent: parentTraceparent, status: http.StatusServiceUnavailable, wantNoSpan: true},
		{name: "livez probe untraced", pattern: "/livez", target: "/livez", status: http.StatusOK, wantNoSpan: true},
		{name: "metrics scrape untraced", pattern: "/metrics", target: "/metrics", traceparent: parentTraceparent, status: http.StatusOK, wantNoSpan: true},
		{
			name: "probe-like prefix still traced", pattern: "/healthz/deep", target: "/healthz/deep", status: http.StatusOK,
			wantName: "POST /healthz/deep", wantRoute: "/healthz/deep",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runMiddlewareCase(t, tc)
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

// hijackRecorder is a ResponseRecorder that also implements http.Hijacker, like net/http's
// server writer does for websocket upgrades.
type hijackRecorder struct{ *httptest.ResponseRecorder }

func (hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) { return nil, nil, nil }

func TestStatusWriterHijack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		hijackable  bool
		priorStatus int
		wantStatus  int
		wantErr     bool
	}{
		{name: "upgrade records switching protocols", hijackable: true, wantStatus: 101},
		{name: "hijack after a written header keeps that status", hijackable: true, priorStatus: 418, wantStatus: 418},
		{name: "writer without hijack support reports an error", wantStatus: 200, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var base http.ResponseWriter = httptest.NewRecorder()
			if tc.hijackable {
				base = hijackRecorder{httptest.NewRecorder()}
			}
			w := &statusWriter{ResponseWriter: base, status: http.StatusOK}
			if tc.priorStatus != 0 {
				w.WriteHeader(tc.priorStatus)
			}
			var hijacker http.Hijacker = w
			_, _, err := hijacker.Hijack()
			if (err != nil) != tc.wantErr || w.status != tc.wantStatus {
				t.Errorf("Hijack err = %v status = %d, want err=%v status=%d", err, w.status, tc.wantErr, tc.wantStatus)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type transportTestCase struct {
	name       string
	url        string
	respStatus int
	respErr    error
	wantError  bool
	wantDesc   string
}

func runTransportCase(t *testing.T, tc transportTestCase) {
	t.Helper()
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tc.url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: newTransport(tp, propagation.TraceContext{}, base, true)}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	if req.Header.Get("Traceparent") != "" {
		t.Error("caller's request was mutated")
	}

	assertTransportSpan(t, tc, sr, gotTP)
}

func assertTransportSpan(t *testing.T, tc transportTestCase, sr *tracetest.SpanRecorder, gotTP string) {
	t.Helper()
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
}

func TestTransport(t *testing.T) {
	t.Parallel()
	tests := []transportTestCase{
		{name: "ok strips path and query", url: "https://api.github.com/repos/x/y?access_token=dummy-secret-value", respStatus: http.StatusOK},
		{name: "server error", url: "http://haro.aerial/dispatch", respStatus: http.StatusServiceUnavailable, wantError: true},
		{
			name: "transport error drops url", url: "http://user:hunter2@coven.aerial/v1?q=secret",
			respErr: errors.New("dial failed"), wantError: true,
			wantDesc: "dial failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTransportCase(t, tc)
		})
	}
}

func TestTransportCloseIdleConnections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		propagate bool
	}{
		{name: "internal client closes the base transport's idle connections", propagate: true},
		{name: "external client closes the base transport's idle connections"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			closed := make(chan struct{})
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateClosed {
					close(closed)
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)
			base := &http.Transport{}
			client := &http.Client{Transport: newTransport(noop.NewTracerProvider(), propagation.TraceContext{}, base, tc.propagate)}
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			client.CloseIdleConnections()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("idle keep-alive connection was not closed")
			}
		})
	}
}

func TestExternalTransport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		parent     bool
		status     int
		path       string
		wantSpans  int
		wantStatus string
		wantError  bool
	}{
		{name: "traced request records a client span", parent: true, status: http.StatusOK, path: "/api/v10/channels/1/messages?wait=true", wantSpans: 1, wantStatus: "200"},
		{name: "rate limit is recorded but not an error", parent: true, status: http.StatusTooManyRequests, path: "/api/v10/channels/1/messages/2", wantSpans: 1, wantStatus: "429"},
		{name: "server error marks the span", parent: true, status: http.StatusBadGateway, path: "/api/v10/gateway", wantSpans: 1, wantStatus: "502", wantError: true},
		{name: "background request opens no root span", parent: false, status: http.StatusOK, path: "/api/v10/users/@me", wantSpans: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			headers := make(chan http.Header, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Clone()
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)
			sr, tp := newRecorder(t)
			ctx := context.Background()
			if tc.parent {
				ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": parentTraceparent})
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPatch, srv.URL+tc.path, http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: newTransport(tp, propagation.TraceContext{}, nil, false)}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()

			got := <-headers
			if got.Get("Traceparent") != "" || got.Get("Tracestate") != "" {
				t.Errorf("outbound trace headers leaked: traceparent=%q tracestate=%q", got.Get("Traceparent"), got.Get("Tracestate"))
			}
			spans := sr.Ended()
			if len(spans) != tc.wantSpans {
				t.Fatalf("got %d spans, want %d", len(spans), tc.wantSpans)
			}
			for _, s := range spans {
				assertPatchClientSpan(t, s, srv.URL, tc.wantStatus, tc.wantError)
			}
		})
	}
}

// assertPatchClientSpan checks that s is a client span parented to
// parentSpanID that recorded a PATCH to wantURL with the given response status.
func assertPatchClientSpan(t *testing.T, s sdktrace.ReadOnlySpan, wantURL, wantStatus string, wantError bool) {
	t.Helper()
	if s.SpanKind() != trace.SpanKindClient || s.Parent().SpanID().String() != parentSpanID {
		t.Errorf("span kind %v parent %s", s.SpanKind(), s.Parent().SpanID())
	}
	attrs := attrMap(s.Attributes())
	if attrs["http.response.status_code"] != wantStatus || attrs["http.request.method"] != http.MethodPatch || attrs["url.full"] != wantURL {
		t.Errorf("attrs = %v, want status %s method PATCH url.full %s", attrs, wantStatus, wantURL)
	}
	if (s.Status().Code == codes.Error) != wantError {
		t.Errorf("status = %+v, want error=%v", s.Status(), wantError)
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

func TestOriginOnly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   *url.URL
		want string
	}{
		{name: "nil url is empty", in: nil, want: ""},
		{name: "path query and userinfo dropped", in: &url.URL{Scheme: "https", User: url.User("u"), Host: "h:8443", Path: "/p", RawQuery: "token=x"}, want: "https://h:8443"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := originOnly(tc.in); got != tc.want {
				t.Errorf("originOnly = %q, want %q", got, tc.want)
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

func TestSlogHandler(t *testing.T) {
	t.Parallel()
	tid, err := trace.TraceIDFromHex(parentTraceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	sid, err := trace.SpanIDFromHex(parentSpanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	spanCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	tests := []struct {
		name    string
		ctx     context.Context
		group   string
		want    []string
		wantNot []string
	}{
		{name: "active span adds ids", ctx: spanCtx, want: []string{`"trace_id":"` + parentTraceID + `"`, `"span_id":"` + parentSpanID + `"`, `"k":"v"`}},
		{name: "no span leaves record untouched", ctx: context.Background(), want: []string{`"k":"v"`}, wantNot: []string{"trace_id", "span_id"}},
		{name: "groups are preserved", ctx: spanCtx, group: "g", want: []string{`"g":{`, `"trace_id":"` + parentTraceID + `"`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf strings.Builder
			h := SlogHandler(slog.NewJSONHandler(&buf, nil)).WithAttrs([]slog.Attr{slog.String("k", "v")})
			if tc.group != "" {
				h = h.WithGroup(tc.group)
			}
			slog.New(h).InfoContext(tc.ctx, "msg")
			for _, w := range tc.want {
				if !strings.Contains(buf.String(), w) {
					t.Errorf("log %s missing %s", buf.String(), w)
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(buf.String(), w) {
					t.Errorf("log %s unexpectedly has %s", buf.String(), w)
				}
			}
		})
	}
}
