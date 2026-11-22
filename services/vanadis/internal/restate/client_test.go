package restate

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/model"
)

type mapErrorCase struct {
	name       string
	statusCode int
	msg        string
	wantErrIs  error
	wantSubstr string
}

func TestMapRestateError(t *testing.T) {
	t.Parallel()
	tests := []mapErrorCase{
		{name: "bad request keeps message", statusCode: http.StatusBadRequest, msg: "repo is required to attach PR to action chunk", wantErrIs: model.ErrInvalidBuildInput, wantSubstr: "repo is required to attach PR to action chunk"},
		{name: "bad request blank message defaults to status text", statusCode: http.StatusBadRequest, msg: "   ", wantErrIs: model.ErrInvalidBuildInput, wantSubstr: "Bad Request"},
		{name: "not found chunk maps to chunk not found", statusCode: http.StatusNotFound, msg: "chunk-123 not found in build", wantErrIs: model.ErrChunkNotFound, wantSubstr: "chunk-123 not found in build"},
		{name: "not found build maps to build not found", statusCode: http.StatusNotFound, msg: "build-abc not found", wantErrIs: model.ErrBuildNotFound, wantSubstr: "build-abc not found"},
		{name: "conflict maps to build conflict", statusCode: http.StatusConflict, msg: "build cannot be mutated in current status", wantErrIs: model.ErrBuildConflict, wantSubstr: "build cannot be mutated in current status"},
		{name: "server error keeps ingress prefix", statusCode: http.StatusInternalServerError, msg: "restate execution failure", wantSubstr: "restate ingress error (500): restate execution failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := MapRestateError(tc.statusCode, tc.msg)
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Errorf("errors.Is(err, %v) = false, err: %v", tc.wantErrIs, err)
			}
			if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("err %q does not contain %q", err.Error(), tc.wantSubstr)
			}
		})
	}
}

type sampleOutput struct {
	Status  string `json:"status"`
	BuildID string `json:"build_id"`
}

func mockRestateProxyServerHandler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ok":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","build_id":"b100"}`))
	case "/empty-ok":
		w.WriteHeader(http.StatusOK)
	case "/invalid-json":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid-json`))
	case "/error-json-message":
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"repo is required to attach PR to action chunk"}`))
	case "/error-json-error":
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"conflict error text"}`))
	case "/error-raw":
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`build not found`))
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

type callIngressCase struct {
	name        string
	serverURL   string
	path        string
	usePayload  bool
	useOut      bool
	wantDecoded bool
	wantErr     bool
	wantErrIs   error
	wantSubstr  string
}

func TestCallIngress(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(mockRestateProxyServerHandler))
	t.Cleanup(ts.Close)

	tests := []callIngressCase{
		{name: "missing ingress url", path: "/ok", wantErr: true, wantSubstr: "restate ingress URL not configured"},
		{name: "successful call with out decoded", serverURL: ts.URL, path: "/ok", usePayload: true, useOut: true, wantDecoded: true},
		{name: "successful call empty response nil out", serverURL: ts.URL, path: "/empty-ok"},
		{name: "successful empty response with out stays zero", serverURL: ts.URL, path: "/empty-ok", useOut: true},
		{name: "unmarshal response error", serverURL: ts.URL, path: "/invalid-json", useOut: true, wantErr: true, wantSubstr: "unmarshal restate response"},
		{name: "error status with message field", serverURL: ts.URL, path: "/error-json-message", wantErr: true, wantErrIs: model.ErrInvalidBuildInput, wantSubstr: "repo is required to attach PR to action chunk"},
		{name: "error status with error field", serverURL: ts.URL, path: "/error-json-error", wantErr: true, wantErrIs: model.ErrBuildConflict, wantSubstr: "conflict error text"},
		{name: "error status with raw text", serverURL: ts.URL, path: "/error-raw", wantErr: true, wantErrIs: model.ErrBuildNotFound, wantSubstr: "build not found"},
		{name: "network failure", serverURL: "http://127.0.0.1:1", path: "/ok", wantErr: true, wantSubstr: "call restate ingress"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCallIngressCase(t, ts, tc)
		})
	}
}

func ingressClient(ts *httptest.Server, tc callIngressCase) *Client {
	return NewClient(config.RestateConfig{RestateIngressURL: tc.serverURL}, ts.Client())
}

func checkCallIngressErr(t *testing.T, err error, tc callIngressCase) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
		t.Errorf("errors.Is(err, %v) = false, err: %v", tc.wantErrIs, err)
	}
	if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
		t.Errorf("err %q does not contain %q", err.Error(), tc.wantSubstr)
	}
}

func runCallIngressCase(t *testing.T, ts *httptest.Server, tc callIngressCase) {
	t.Helper()
	var payload any
	if tc.usePayload {
		payload = map[string]string{"foo": "bar"}
	}
	var out *sampleOutput
	if tc.useOut {
		out = &sampleOutput{}
	}
	err := ingressClient(ts, tc).CallIngress(context.Background(), tc.path, payload, out)
	if tc.wantErr {
		checkCallIngressErr(t, err, tc)
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantDecoded && (out.Status != "success" || out.BuildID != "b100") {
		t.Errorf("unexpected decoded output: %+v", out)
	}
}

type errorReadCloser struct{ err error }

func (e *errorReadCloser) Read([]byte) (int, error) { return 0, e.err }
func (*errorReadCloser) Close() error               { return nil }

type scriptTransport struct {
	t             *testing.T
	err           error
	status        int
	body          string
	bodyErr       error
	checkDeadline bool
}

func (d scriptTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if d.checkDeadline {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 10*time.Second {
			d.t.Error("upstream request must have at most ten seconds to respond")
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	var body io.ReadCloser
	if d.bodyErr != nil {
		body = &errorReadCloser{err: d.bodyErr}
	} else {
		body = io.NopCloser(strings.NewReader(d.body))
	}
	return &http.Response{StatusCode: d.status, Body: body, Header: make(http.Header)}, nil
}

type timeoutCase struct {
	name       string
	cause      error
	cancelled  bool
	wantErrIs  error
	wantSubstr string
}

func TestCallIngressTimeout(t *testing.T) {
	t.Parallel()
	tests := []timeoutCase{
		{name: "deadline exceeded maps to timeout", cause: context.DeadlineExceeded, wantErrIs: ErrRestateTimeout, wantSubstr: "acceptance unknown"},
		{name: "parent cancellation passes through", cause: context.Canceled, cancelled: true, wantSubstr: "context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(config.RestateConfig{RestateIngressURL: "http://restate.invalid"}, &http.Client{Transport: scriptTransport{t: t, err: tc.cause, checkDeadline: !tc.cancelled}})
			ctx := context.Background()
			var cancel context.CancelFunc
			if tc.cancelled {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			err := c.CallIngress(ctx, "/Build/example/Create", nil, nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Errorf("errors.Is(err, %v) = false, err: %v", tc.wantErrIs, err)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("err %q does not contain %q", err.Error(), tc.wantSubstr)
			}
		})
	}
}

type retryActionCase struct {
	name       string
	status     int
	body       string
	transport  string
	wantErr    bool
	wantErrIs  error
	wantSubstr string
	wantAccept bool
}

func TestCallRetryAction(t *testing.T) {
	t.Parallel()
	tests := []retryActionCase{
		{name: "missing ingress url", wantErr: true, wantSubstr: "restate ingress URL not configured"},
		{name: "bad request maps to invalid input", status: http.StatusBadRequest, body: `{"message":"retry rejected"}`, wantErr: true, wantErrIs: model.ErrInvalidBuildInput},
		{name: "not found maps to build not found", status: http.StatusNotFound, body: `{}`, wantErr: true, wantErrIs: model.ErrBuildNotFound},
		{name: "conflict maps to build conflict", status: http.StatusConflict, body: `{}`, wantErr: true, wantErrIs: model.ErrBuildConflict},
		{name: "server error keeps status", status: http.StatusInternalServerError, body: `boom`, wantErr: true, wantSubstr: "durable retry returned HTTP 500"},
		{name: "decode error propagates", status: http.StatusOK, body: `{invalid`, wantErr: true, wantSubstr: "decode durable retry"},
		{name: "network failure wraps", transport: "down", wantErr: true, wantSubstr: "durable retry request"},
		{name: "success decodes acknowledgement", status: http.StatusOK, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"d1","accepted":true}`, wantAccept: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCallRetryActionCase(t, tc)
		})
	}
}

type retryAck struct {
	BuildID    string `json:"build_id"`
	ChunkID    string `json:"chunk_id"`
	DispatchID string `json:"dispatch_id"`
	Accepted   bool   `json:"accepted"`
}

func retryActionClient(t *testing.T, tc retryActionCase, serverURL string) *Client {
	t.Helper()
	if tc.transport != "down" {
		return NewClient(config.RestateConfig{RestateIngressURL: serverURL}, nil)
	}
	return NewClient(config.RestateConfig{RestateIngressURL: serverURL}, &http.Client{Transport: scriptTransport{t: t, err: errors.New("dial down")}})
}

func checkRetryActionErr(t *testing.T, err error, tc retryActionCase) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
		t.Errorf("errors.Is(err, %v) = false, err: %v", tc.wantErrIs, err)
	}
	if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
		t.Errorf("err %q does not contain %q", err.Error(), tc.wantSubstr)
	}
}

func runCallRetryActionCase(t *testing.T, tc retryActionCase) {
	t.Helper()
	var serverURL string
	if tc.transport == "down" {
		serverURL = "http://127.0.0.1:1"
	}
	if tc.status != 0 {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Idempotency-Key") == "" || r.Header.Get("Content-Type") != "application/json" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.URL.Path != "/Build-agent-alpha/b1/RetryActionChunk" {
				w.WriteHeader(http.StatusTeapot)
				return
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		defer ts.Close()
		serverURL = ts.URL
	}
	var out retryAck
	err := retryActionClient(t, tc, serverURL).CallRetryAction(context.Background(), "Build-agent-alpha", "b1", []byte(`{"build_id":"b1"}`), "retry-action-key", &out)
	if tc.wantErr {
		checkRetryActionErr(t, err, tc)
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantAccept && (!out.Accepted || out.BuildID != "b1") {
		t.Errorf("unexpected acknowledgement: %+v", out)
	}
}

type scopedNameCase struct {
	name           string
	clientAgent    string
	runnerAgent    string
	buildID        string
	wantRouter     string
	wantObjectPath string
	wantRouterPath string
}

func TestClientScopedNames(t *testing.T) {
	t.Parallel()
	tests := []scopedNameCase{
		{name: "empty runner targets legacy build", clientAgent: "", runnerAgent: "", buildID: "b1", wantRouter: "BuildRouter", wantObjectPath: "/Build/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "aerial runner targets legacy build", clientAgent: "aerial", runnerAgent: "aerial", buildID: "b1", wantRouter: "BuildRouter", wantObjectPath: "/Build/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "sibling runner targets its build service while router stays shared", clientAgent: "aerial", runnerAgent: "agent-beta", buildID: "b1", wantRouter: "BuildRouter", wantObjectPath: "/Build-agent-beta/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "default runner ignores scoped client agent", clientAgent: "agent-alpha", runnerAgent: "aerial", buildID: "b1", wantRouter: "BuildRouter-agent-alpha", wantObjectPath: "/Build/b1/Create", wantRouterPath: "/BuildRouter-agent-alpha/RouteFailChunk"},
		{name: "mixed case runner normalized", clientAgent: "aerial", runnerAgent: "Agent-Beta", buildID: "b1", wantRouter: "BuildRouter", wantObjectPath: "/Build-agent-beta/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "special characters in buildID are escaped", clientAgent: "aerial", runnerAgent: "agent-beta", buildID: "build/1#test:special", wantRouter: "BuildRouter", wantObjectPath: "/Build-agent-beta/build%2F1%23test:special/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(config.RestateConfig{AgentName: tc.clientAgent}, nil)
			if got := c.BuildRouterServiceName(); got != tc.wantRouter {
				t.Errorf("BuildRouterServiceName() = %q, want %q", got, tc.wantRouter)
			}
			if got := c.ObjectPath(buildplan.BuildServiceName(tc.runnerAgent), tc.buildID, "Create"); got != tc.wantObjectPath {
				t.Errorf("ObjectPath() = %q, want %q", got, tc.wantObjectPath)
			}
			if got := c.RouterPath("RouteFailChunk"); got != tc.wantRouterPath {
				t.Errorf("RouterPath() = %q, want %q", got, tc.wantRouterPath)
			}
		})
	}
}

type ingressURLCase struct {
	name       string
	envURL     string
	configured string
	want       string
}

// TestIngressURL pins that the client reports only its configured ingress and
// never falls back to RESTATE_INGRESS_URL, so tests cannot be routed to a live
// Restate ingress. Each row sets a hostile env value, so it is not parallel.
func TestIngressURL(t *testing.T) {
	tests := []ingressURLCase{
		{name: "empty config ignores env ingress", envURL: "http://env.example", configured: "", want: ""},
		{name: "configured url wins over env ingress", envURL: "http://env.example", configured: "http://restate.test:8080/", want: "http://restate.test:8080"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RESTATE_INGRESS_URL", tc.envURL)
			got := NewClient(config.RestateConfig{RestateIngressURL: tc.configured}, nil).IngressURL()
			if got != tc.want {
				t.Fatalf("IngressURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCallIngressKeyed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		key     string
		wantKey string
	}{
		{"sends key", "complete:d1", "complete:d1"},
		{"omits empty key", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Idempotency-Key")
			}))
			defer ts.Close()
			c := NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client())
			if err := c.CallIngressKeyed(context.Background(), "/Build/b/CompleteChunk", tc.key, nil, nil); err != nil {
				t.Fatal(err)
			}
			if got != tc.wantKey {
				t.Fatalf("Idempotency-Key = %q, want %q", got, tc.wantKey)
			}
		})
	}
}
