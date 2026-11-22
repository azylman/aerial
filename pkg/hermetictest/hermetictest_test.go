package hermetictest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.invalid:4318")
	Main(m,
		WithCleanup(http.DefaultClient.CloseIdleConnections),
		WithIgnore(goleak.IgnoreTopFunction("internal/poll.runtime_pollWait")),
	)
}

// recordingTransport is a declarative base transport: it records the hosts it
// was asked for and the number of CloseIdleConnections calls.
type recordingTransport struct {
	status int
	hosts  []string
	closed int
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.hosts = append(r.hosts, req.URL.Host)
	return &http.Response{StatusCode: r.status, Body: http.NoBody, Request: req}, nil
}

func (r *recordingTransport) CloseIdleConnections() { r.closed++ }

func TestHermeticTransport_BlocksNonLoopbackAndReservedPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		url        string // "" sends a nil request
		wantErr    string
		wantStatus int
		wantHosts  []string
	}{
		{name: "nil request", wantErr: "hermetic test guard: nil request or URL"},
		{name: "external host", url: "http://example.com/x", wantErr: "hermetic test guard: blocked non-loopback HTTP call to example.com"},
		{name: "external host with port", url: "https://api.example.com:8443/x", wantErr: "blocked non-loopback HTTP call to api.example.com:8443"},
		{name: "cluster dns name", url: "http://eri.aerial:8080/", wantErr: "blocked non-loopback HTTP call to eri.aerial:8080"},
		{name: "reserved 8080", url: "http://localhost:8080/", wantErr: "hermetic test guard: blocked dial to live daemon port 8080 on localhost"},
		{name: "reserved 8088", url: "http://127.0.0.1:8088/", wantErr: "blocked dial to live daemon port 8088 on 127.0.0.1"},
		{name: "reserved 4020", url: "http://localhost:4020/", wantErr: "blocked dial to live daemon port 4020"},
		{name: "reserved 8081", url: "http://localhost:8081/", wantErr: "blocked dial to live daemon port 8081"},
		{name: "reserved 8082", url: "http://localhost:8082/", wantErr: "blocked dial to live daemon port 8082"},
		{name: "reserved 8083", url: "http://localhost:8083/", wantErr: "blocked dial to live daemon port 8083"},
		{name: "reserved 8090", url: "http://localhost:8090/", wantErr: "blocked dial to live daemon port 8090"},
		{name: "reserved 8070", url: "http://localhost:8070/", wantErr: "blocked dial to live daemon port 8070"},
		{name: "reserved 9070 ipv6", url: "http://[::1]:9070/", wantErr: "blocked dial to live daemon port 9070 on ::1"},
		{name: "loopback ephemeral port", url: "http://127.0.0.1:40123/x", wantStatus: http.StatusTeapot, wantHosts: []string{"127.0.0.1:40123"}},
		{name: "localhost no port", url: "http://localhost/x", wantStatus: http.StatusTeapot, wantHosts: []string{"localhost"}},
		{name: "ipv6 loopback", url: "http://[::1]:40124/x", wantStatus: http.StatusTeapot, wantHosts: []string{"[::1]:40124"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := &recordingTransport{status: http.StatusTeapot}
			tr := &Transport{Base: base}
			var req *http.Request
			if tc.url != "" {
				var err error
				req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, tc.url, http.NoBody)
				if err != nil {
					t.Fatalf("NewRequest: %v", err)
				}
			}
			resp, err := tr.RoundTrip(req)
			gotStatus := 0
			if resp != nil {
				gotStatus = resp.StatusCode
				_ = resp.Body.Close()
			}
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if tc.wantErr == "" && gotErr != "" || !strings.Contains(gotErr, tc.wantErr) {
				t.Fatalf("err = %q, want containing %q", gotErr, tc.wantErr)
			}
			if gotStatus != tc.wantStatus {
				t.Fatalf("status = %d, want %d", gotStatus, tc.wantStatus)
			}
			if strings.Join(base.hosts, ",") != strings.Join(tc.wantHosts, ",") {
				t.Fatalf("base saw hosts %v, want %v", base.hosts, tc.wantHosts)
			}
		})
	}
}

func TestTransportCloseIdleConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		base       *recordingTransport
		wantClosed int
	}{
		{name: "closes base", base: &recordingTransport{}, wantClosed: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := &Transport{}
			if tc.base != nil {
				tr.Base = tc.base
			}
			tr.CloseIdleConnections()
			if tc.base != nil && tc.base.closed != tc.wantClosed {
				t.Fatalf("closed = %d, want %d", tc.base.closed, tc.wantClosed)
			}
		})
	}
}

// TestInstallAndDefaultBase is not parallel: it swaps http.DefaultTransport.
func TestInstallAndDefaultBase(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer srv.Close()

	installed, ok := http.DefaultTransport.(*Transport)
	if !ok {
		t.Fatalf("Main did not install the guard: DefaultTransport is %T", http.DefaultTransport)
	}
	Install()()
	if http.DefaultTransport != installed {
		t.Fatal("Install on an installed guard must be a no-op")
	}

	http.DefaultTransport = installed.Base
	restore := Install()
	if _, ok := http.DefaultTransport.(*Transport); !ok {
		t.Fatalf("Install did not wrap DefaultTransport: %T", http.DefaultTransport)
	}
	restore()
	if http.DefaultTransport != installed.Base {
		t.Fatal("restore did not put back the previous transport")
	}
	http.DefaultTransport = installed

	resp, err := (&http.Client{Transport: &Transport{}}).Get(srv.URL)
	if err != nil {
		t.Fatalf("nil-base Transport through default: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if v, set := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT"); set {
		t.Fatalf("Main left OTEL_EXPORTER_OTLP_ENDPOINT=%q set", v)
	}
}

type fakeTestingM struct {
	code  int
	order *[]string
}

func (f fakeTestingM) Run() int {
	*f.order = append(*f.order, "run")
	return f.code
}

func TestRunner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		code      int
		cleanups  int
		wantOrder string
	}{
		{name: "no cleanups", code: 0, wantOrder: "run"},
		{name: "cleanups after run in order", code: 0, cleanups: 2, wantOrder: "run,cleanup0,cleanup1"},
		{name: "failing run still cleans up", code: 3, cleanups: 1, wantOrder: "run,cleanup0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var order []string
			r := runner{m: fakeTestingM{code: tc.code, order: &order}}
			for i := range tc.cleanups {
				r.cleanups = append(r.cleanups, func() { order = append(order, "cleanup"+strconv.Itoa(i)) })
			}
			if got := r.Run(); got != tc.code {
				t.Fatalf("Run() = %d, want %d", got, tc.code)
			}
			if got := strings.Join(order, ","); got != tc.wantOrder {
				t.Fatalf("order = %q, want %q", got, tc.wantOrder)
			}
		})
	}
}

// fakeUnsetter records the keys it is asked to unset and fails on failKey.
type fakeUnsetter struct {
	failKey string
	keys    []string
}

func (f *fakeUnsetter) unset(key string) error {
	f.keys = append(f.keys, key)
	if key == f.failKey {
		return errors.New("unset " + key + " failed")
	}
	return nil
}

func TestUnsetOTELEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		environ  []string
		failKey  string
		wantKeys []string
		wantErr  string
	}{
		{name: "no otel vars", environ: []string{"HOME=/tmp", "PATH=/bin"}},
		{name: "only otel vars unset", environ: []string{"OTEL_SDK_DISABLED=true", "HOME=/tmp", "OTEL_EXPORTER_OTLP_ENDPOINT=http://x=y"}, wantKeys: []string{"OTEL_SDK_DISABLED", "OTEL_EXPORTER_OTLP_ENDPOINT"}},
		{name: "lowercase prefix kept", environ: []string{"otel_x=1", "XOTEL_Y=1"}},
		{name: "unset error reported", environ: []string{"OTEL_A=1", "OTEL_B=2"}, failKey: "OTEL_A", wantKeys: []string{"OTEL_A", "OTEL_B"}, wantErr: "unset OTEL_A failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeUnsetter{failKey: tc.failKey}
			gotErr := ""
			if err := unsetOTELEnv(tc.environ, f.unset); err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Fatalf("err = %q, want %q", gotErr, tc.wantErr)
			}
			if strings.Join(f.keys, ",") != strings.Join(tc.wantKeys, ",") {
				t.Fatalf("unset keys = %v, want %v", f.keys, tc.wantKeys)
			}
		})
	}
}

// TestNewRecorder checks the AlwaysSample contract: a span whose remote parent
// was not sampled is still recorded (the SDK default ParentBased sampler drops it).
func TestNewRecorder(t *testing.T) {
	t.Parallel()

	tp, sr := NewRecorder()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x4b, 0xf9},
		SpanID:  trace.SpanID{0x01},
		Remote:  true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	_, span := tp.Tracer("hermetictest").Start(ctx, "op")
	span.End()
	ended := sr.Ended()
	if len(ended) != 1 || ended[0].Name() != "op" || !ended[0].SpanContext().IsSampled() {
		t.Fatalf("ended spans = %v, want one sampled span named op under an unsampled parent", ended)
	}
}
