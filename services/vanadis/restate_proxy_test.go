package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMapRestateError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		msg        string
		wantErrIs  error
		wantSubstr string
	}{
		{
			name:       "400 with custom message",
			statusCode: http.StatusBadRequest,
			msg:        "repo is required to attach PR to action chunk",
			wantErrIs:  ErrInvalidBuildInput,
			wantSubstr: "repo is required to attach PR to action chunk",
		},
		{
			name:       "400 empty message defaults to status text",
			statusCode: http.StatusBadRequest,
			msg:        "   ",
			wantErrIs:  ErrInvalidBuildInput,
			wantSubstr: "Bad Request",
		},
		{
			name:       "404 chunk error",
			statusCode: http.StatusNotFound,
			msg:        "chunk-123 not found in build",
			wantErrIs:  ErrChunkNotFound,
			wantSubstr: "chunk-123 not found in build",
		},
		{
			name:       "404 build error",
			statusCode: http.StatusNotFound,
			msg:        "build-abc not found",
			wantErrIs:  ErrBuildNotFound,
			wantSubstr: "build-abc not found",
		},
		{
			name:       "409 conflict error",
			statusCode: http.StatusConflict,
			msg:        "build cannot be mutated in current status",
			wantErrIs:  ErrBuildConflict,
			wantSubstr: "build cannot be mutated in current status",
		},
		{
			name:       "500 internal server error",
			statusCode: http.StatusInternalServerError,
			msg:        "restate execution failure",
			wantSubstr: "restate ingress error (500): restate execution failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := mapRestateError(tc.statusCode, tc.msg)
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

func checkCallRestateError(t *testing.T, err error, wantErrIs error, wantSubstr string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if wantErrIs != nil && !errors.Is(err, wantErrIs) {
		t.Errorf("errors.Is(err, %v) = false, err: %v", wantErrIs, err)
	}
	if wantSubstr != "" && !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("err %q does not contain %q", err.Error(), wantSubstr)
	}
}

func TestCallRestateIngress(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(mockRestateProxyServerHandler))
	defer ts.Close()

	tests := []struct {
		name       string
		serverURL  string
		envURL     string
		nilServer  bool
		path       string
		payload    any
		useOut     bool
		wantErr    bool
		wantErrIs  error
		wantSubstr string
	}{
		{
			name:       "missing ingress url",
			serverURL:  "",
			path:       "/ok",
			wantErr:    true,
			wantSubstr: "restate ingress URL not configured",
		},
		{
			name:       "successful call with out decoded",
			serverURL:  ts.URL,
			path:       "/ok",
			payload:    map[string]string{"foo": "bar"},
			useOut:     true,
			wantErr:    false,
			wantSubstr: "",
		},
		{
			name:      "successful call empty response nil out",
			serverURL: ts.URL,
			path:      "/empty-ok",
			payload:   nil,
			useOut:    false,
			wantErr:   false,
		},
		{
			name:       "unmarshal response error",
			serverURL:  ts.URL,
			path:       "/invalid-json",
			useOut:     true,
			wantErr:    true,
			wantSubstr: "unmarshal restate response",
		},
		{
			name:       "error status with message field",
			serverURL:  ts.URL,
			path:       "/error-json-message",
			wantErr:    true,
			wantErrIs:  ErrInvalidBuildInput,
			wantSubstr: "repo is required to attach PR to action chunk",
		},
		{
			name:       "error status with error field",
			serverURL:  ts.URL,
			path:       "/error-json-error",
			wantErr:    true,
			wantErrIs:  ErrBuildConflict,
			wantSubstr: "conflict error text",
		},
		{
			name:       "error status with raw text",
			serverURL:  ts.URL,
			path:       "/error-raw",
			wantErr:    true,
			wantErrIs:  ErrBuildNotFound,
			wantSubstr: "build not found",
		},
		{
			name:       "network failure",
			serverURL:  "http://127.0.0.1:1",
			path:       "/ok",
			wantErr:    true,
			wantSubstr: "call restate ingress",
		},
		{
			name:      "nil server uses env url",
			nilServer: true,
			envURL:    ts.URL,
			path:      "/empty-ok",
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envURL != "" {
				t.Setenv("RESTATE_INGRESS_URL", tc.envURL)
			}
			var srv *VanadisServer
			if !tc.nilServer {
				srv = NewVanadisServer(Config{}, nil, ts.Client())
				srv.restateCfg.RestateIngressURL = tc.serverURL
			}

			var out *sampleOutput
			if tc.useOut {
				out = &sampleOutput{}
			}

			err := srv.callRestateIngress(context.Background(), tc.path, tc.payload, out)
			if tc.wantErr {
				checkCallRestateError(t, err, tc.wantErrIs, tc.wantSubstr)
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.useOut && (out.Status != "success" || out.BuildID != "b100") {
				t.Errorf("unexpected decoded output: %+v", out)
			}
		})
	}
}

type deadlineTransport struct {
	t   *testing.T
	err error
}

func (d deadlineTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	deadline, ok := r.Context().Deadline()
	if !ok || time.Until(deadline) > 10*time.Second {
		d.t.Error("upstream request must have at most ten seconds to respond")
	}
	return nil, d.err
}

func TestRestateDeadlineResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cause   error
		want    int
		message string
	}{
		{"deadline", context.DeadlineExceeded, 504, "acceptance unknown"},
		{"parent cancellation", context.Canceled, 500, "context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &VanadisServer{httpClient: &http.Client{Transport: deadlineTransport{t, tc.cause}}, restateCfg: RestateConfig{RestateIngressURL: "http://restate.invalid"}}
			err := s.callRestateIngress(context.Background(), "/Build/example/Create", nil, nil)
			rec := httptest.NewRecorder()
			writeStoreErr(context.Background(), rec, err)
			if rec.Code != tc.want || !strings.Contains(rec.Body.String(), tc.message) {
				t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestNilServerServiceNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		service string
		want    string
	}{
		{name: "build defaults to legacy", service: "build", want: "Build"},
		{name: "router defaults to legacy", service: "router", want: "BuildRouter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s *VanadisServer
			got := s.buildRouterServiceName()
			if tc.service == "build" {
				got = s.buildServiceName()
			}
			if got != tc.want {
				t.Errorf("nil server %s service name = %q, want %q", tc.service, got, tc.want)
			}
		})
	}
}

func TestRestateScopedNames_Derivation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		agent          string
		wantBuild      string
		wantRouter     string
		wantObjectPath string
		wantRouterPath string
	}{
		{name: "empty defaults to legacy", agent: "", wantBuild: "Build", wantRouter: "BuildRouter", wantObjectPath: "/Build/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "aerial defaults to legacy", agent: "aerial", wantBuild: "Build", wantRouter: "BuildRouter", wantObjectPath: "/Build/b1/Create", wantRouterPath: "/BuildRouter/RouteFailChunk"},
		{name: "suletta scoped", agent: "suletta", wantBuild: "Build-suletta", wantRouter: "BuildRouter-suletta", wantObjectPath: "/Build-suletta/b1/Create", wantRouterPath: "/BuildRouter-suletta/RouteFailChunk"},
		{name: "miorine scoped", agent: "miorine", wantBuild: "Build-miorine", wantRouter: "BuildRouter-miorine", wantObjectPath: "/Build-miorine/b1/Create", wantRouterPath: "/BuildRouter-miorine/RouteFailChunk"},
		{name: "mixed case scoped normalized", agent: "Suletta", wantBuild: "Build-suletta", wantRouter: "BuildRouter-suletta", wantObjectPath: "/Build-suletta/b1/Create", wantRouterPath: "/BuildRouter-suletta/RouteFailChunk"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildServiceName(tc.agent); got != tc.wantBuild {
				t.Fatalf("BuildServiceName(%q) = %q, want %q", tc.agent, got, tc.wantBuild)
			}
			if got := BuildRouterServiceName(tc.agent); got != tc.wantRouter {
				t.Fatalf("BuildRouterServiceName(%q) = %q, want %q", tc.agent, got, tc.wantRouter)
			}
			s := &VanadisServer{restateCfg: RestateConfig{AgentName: tc.agent}}
			if got := s.buildServiceName(); got != tc.wantBuild {
				t.Fatalf("buildServiceName() = %q, want %q", got, tc.wantBuild)
			}
			if got := s.buildRouterServiceName(); got != tc.wantRouter {
				t.Fatalf("buildRouterServiceName() = %q, want %q", got, tc.wantRouter)
			}
			if got := s.restateObjectPath("b1", "Create"); got != tc.wantObjectPath {
				t.Fatalf("restateObjectPath() = %q, want %q", got, tc.wantObjectPath)
			}
			if got := s.restateRouterPath("RouteFailChunk"); got != tc.wantRouterPath {
				t.Fatalf("restateRouterPath() = %q, want %q", got, tc.wantRouterPath)
			}
		})
	}
}
