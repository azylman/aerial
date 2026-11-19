package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMCPRouteWiring guards the Streamable HTTP readiness banner contract on
// both the /mcp and /sse aliases after the L2 internal/mcp extraction.
func TestMCPRouteWiring(t *testing.T) {
	t.Parallel()
	srv := NewVanadisServer(Config{Port: "0"}, newMemoryVanadisStore(), nil)
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantSubstr string
	}{
		{name: "ready banner on /mcp", path: "/mcp", wantStatus: http.StatusOK, wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n"},
		{name: "ready banner on /sse", path: "/sse", wantStatus: http.StatusOK, wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, err := ts.Client().Get(ts.URL + tc.path)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, string(body))
			}
			if !strings.Contains(string(body), tc.wantSubstr) {
				t.Fatalf("body %q does not contain %q", string(body), tc.wantSubstr)
			}
		})
	}
}

// TestRetryActionRouteWiring exercises the POST /builds/{id}/retry-action
// route through SetupMux so the root delegation wrapper stays covered.
func TestRetryActionRouteWiring(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{name: "invalid json rejected", body: `{`, wantStatus: http.StatusBadRequest, wantSubstr: "invalid retry payload"},
		{name: "unavailable ingress reported", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, wantStatus: http.StatusServiceUnavailable, wantSubstr: "durable Restate ingress is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewVanadisServer(Config{Port: "0"}, newMemoryVanadisStore(), nil)
			srv.restateCfg = RestateConfig{}
			ts := httptest.NewServer(srv.Routes())
			t.Cleanup(ts.Close)
			resp, err := ts.Client().Post(ts.URL+"/builds/b1/retry-action", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, string(body))
			}
			if !strings.Contains(string(body), tc.wantSubstr) {
				t.Fatalf("body %q does not contain %q", string(body), tc.wantSubstr)
			}
		})
	}
}

// TestMCPHandlerReceivers pins the observable wiring of the root MCP
// delegation helpers: which store instance is propagated, the RestateEnabled
// flag value, and that a nil server reports ingress unavailable instead of
// panicking.
func TestMCPHandlerReceivers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		nilServer    bool
		enabled      bool
		wantStoreNil bool
		wantEnabled  bool
	}{
		{name: "nil server wires unavailable handler", nilServer: true, wantStoreNil: true, wantEnabled: false},
		{name: "live server propagates store and enabled flag", enabled: true, wantEnabled: true},
		{name: "live server propagates disabled flag", wantEnabled: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *VanadisServer
			var mem *memoryVanadisStore
			if !tc.nilServer {
				mem = newMemoryVanadisStore()
				srv = NewVanadisServer(Config{Port: "0"}, mem, nil)
				srv.restateCfg = RestateConfig{Enabled: tc.enabled, RestateIngressURL: "http://restate.invalid"}
			}
			h := srv.mcpHandler()
			if h == nil || h.Restate == nil {
				t.Fatal("expected non-nil MCP handler and Restate client")
			}
			if (h.Store == nil) != tc.wantStoreNil {
				t.Fatalf("handler store nil = %v, want nil = %v", h.Store == nil, tc.wantStoreNil)
			}
			if !tc.wantStoreNil && h.Store != VanadisStore(mem) {
				t.Fatal("handler does not propagate the live server store")
			}
			if h.RestateEnabled != tc.wantEnabled {
				t.Fatalf("handler RestateEnabled = %v, want %v", h.RestateEnabled, tc.wantEnabled)
			}
			if srv.restateClient() == nil {
				t.Fatal("expected non-nil Restate client")
			}
			if tc.nilServer {
				reqBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}}}`
				r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				h.NewHTTPHandler()(w, r)
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "durable Restate ingress is required") {
					t.Fatalf("nil-server retry status=%d body=%s, want ingress-unavailable tool error", w.Code, w.Body.String())
				}
			}
		})
	}
}

// TestWriteStoreErrMapping pins the store/Restate error-to-HTTP-status mapping
// shared by the root handlers and the extracted MCP surface.
func TestWriteStoreErrMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		storeErr   error
		wantStatus int
	}{
		{name: "restate timeout maps to gateway timeout", storeErr: ErrRestateTimeout, wantStatus: http.StatusGatewayTimeout},
		{name: "build conflict maps to conflict", storeErr: ErrBuildConflict, wantStatus: http.StatusConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := newMemoryVanadisStore()
			st.getErr = tc.storeErr
			srv := NewVanadisServer(Config{Port: "0"}, st, nil)
			ts := httptest.NewServer(srv.Routes())
			t.Cleanup(ts.Close)
			resp, err := ts.Client().Get(ts.URL + "/builds/b1")
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}

// TestMCPHandlerLiveDependencies proves the /mcp and /sse tools resolve live
// server dependencies per request: the mux is built first, then the store is
// replaced via SetStore and the Restate ingress is pointed at a mock server.
// Both MCP calls below must observe the replacement dependencies, not the
// empty store and missing ingress present at Routes() time.
func TestMCPHandlerLiveDependencies(t *testing.T) {
	t.Parallel()
	ingress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"build-live","status":"running"}`))
	}))
	t.Cleanup(ingress.Close)

	srv := NewVanadisServer(Config{Port: "0"}, newMemoryVanadisStore(), nil)
	srv.restateCfg = RestateConfig{}
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	seeded := newMemoryVanadisStore()
	if err := seeded.CreateBuild(context.Background(), mkBuild("build-live", "1234567890", "Live Title", "Live Objective", "running"), []VanadisChunk{mkChunk("chunk-1", "build-live", 0, "Chunk 1", "Inst 1", "medium", "pending")}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	srv.SetStore(seeded)
	srv.restateCfg = RestateConfig{Enabled: true, RestateIngressURL: ingress.URL}

	tests := []struct {
		name       string
		tool       string
		args       string
		wantSubstr string
	}{
		{name: "get_build observes replaced store", tool: "get_build", args: `{"build_id":"build-live"}`, wantSubstr: `"title":"Live Title"`},
		{name: "update_build observes replaced ingress", tool: "update_build", args: `{"build_id":"build-live","title":"New Title"}`, wantSubstr: `"status":"updated"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"` + tc.tool + `","arguments":` + tc.args + `}}`
			resp, err := ts.Client().Post(ts.URL+"/mcp", "application/json", strings.NewReader(reqBody))
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), tc.wantSubstr) {
				t.Fatalf("status=%d body=%s, want substring %q", resp.StatusCode, string(body), tc.wantSubstr)
			}
			if strings.Contains(string(body), `"isError":true`) {
				t.Fatalf("unexpected tool error: %s", string(body))
			}
		})
	}
}

// TestSetupMuxNilSafeDelegation proves a nil-backed mux stays nil-safe: the
// per-request MCP closure serves the readiness banner and reports the missing
// store as a tool error instead of panicking, and the standalone metrics
// branch still serves.
func TestSetupMuxNilSafeDelegation(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(SetupMux(nil))
	t.Cleanup(ts.Close)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{name: "nil mux serves MCP banner without a store", method: http.MethodGet, path: "/mcp", wantStatus: http.StatusOK, wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n"},
		{name: "nil mux reports missing store as tool error", method: http.MethodPost, path: "/mcp", body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_build","arguments":{"build_id":"build-live"}}}`, wantStatus: http.StatusOK, wantSubstr: "vanadis store not configured"},
		{name: "nil mux serves standalone metrics", method: http.MethodGet, path: "/metrics", wantStatus: http.StatusOK, wantSubstr: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, bodyReader)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			if tc.method == http.MethodPost {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, string(body))
			}
			if tc.wantSubstr != "" && !strings.Contains(string(body), tc.wantSubstr) {
				t.Fatalf("body %q does not contain %q", string(body), tc.wantSubstr)
			}
		})
	}
}
