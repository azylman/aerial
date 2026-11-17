package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRestateConfig(t *testing.T) {
	origFallback := vanadisFallbackConfigPath
	defer func() {
		vanadisFallbackConfigPath = origFallback
	}()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "restate.yaml")
	_ = os.WriteFile(yamlPath, []byte("enabled: true\nrestate_ingress_url: \"http://restate.aerial:8080/\"\n"), 0o600)

	badYamlPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badYamlPath, []byte(":::bad_yaml"), 0o600)

	tests := []struct {
		name              string
		envIngressURL     string
		envRestateEnabled string
		envVanadisEnabled string
		configPath        string
		fallbackPath      string
		wantEnabled       bool
		wantURL           string
	}{
		{
			name:        "defaults unconfigured",
			wantEnabled: false,
			wantURL:     "",
		},
		{
			name:          "env ingress url set",
			envIngressURL: "http://env-restate:8080/",
			wantEnabled:   true,
			wantURL:       "http://env-restate:8080",
		},
		{
			name:              "env restate enabled true",
			envRestateEnabled: "true",
			wantEnabled:       true,
			wantURL:           "",
		},
		{
			name:              "env vanadis restate enabled true",
			envVanadisEnabled: "true",
			wantEnabled:       true,
			wantURL:           "",
		},
		{
			name:              "env disabled overrides ingress url",
			envIngressURL:     "http://env-restate:8080",
			envRestateEnabled: "false",
			wantEnabled:       false,
			wantURL:           "http://env-restate:8080",
		},
		{
			name:        "config path yaml loaded",
			configPath:  yamlPath,
			wantEnabled: true,
			wantURL:     "http://restate.aerial:8080",
		},
		{
			name:         "fallback config path loaded",
			fallbackPath: yamlPath,
			wantEnabled:  true,
			wantURL:      "http://restate.aerial:8080",
		},
		{
			name:        "bad yaml fallback logs warning",
			configPath:  badYamlPath,
			wantEnabled: false,
			wantURL:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RESTATE_INGRESS_URL", tc.envIngressURL)
			t.Setenv("RESTATE_ENABLED", tc.envRestateEnabled)
			t.Setenv("VANADIS_RESTATE_ENABLED", tc.envVanadisEnabled)
			if tc.fallbackPath != "" {
				vanadisFallbackConfigPath = tc.fallbackPath
			} else {
				vanadisFallbackConfigPath = filepath.Join(tmpDir, "missing.yaml")
			}

			cfg := LoadRestateConfig(tc.configPath)
			if cfg.Enabled != tc.wantEnabled {
				t.Errorf("Enabled = %v, want %v", cfg.Enabled, tc.wantEnabled)
			}
			if cfg.RestateIngressURL != tc.wantURL {
				t.Errorf("RestateIngressURL = %q, want %q", cfg.RestateIngressURL, tc.wantURL)
			}
		})
	}
}

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
