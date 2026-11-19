package main

import (
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

// TestMCPHandlerReceivers pins the nil/non-nil receiver branches of the root
// MCP delegation helpers introduced by the L2 extraction.
func TestMCPHandlerReceivers(t *testing.T) {
	t.Parallel()
	live := NewVanadisServer(Config{Port: "0"}, newMemoryVanadisStore(), nil)
	tests := []struct {
		name      string
		nilServer bool
	}{
		{name: "nil server builds handler", nilServer: true},
		{name: "live server builds handler", nilServer: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *VanadisServer
			if !tc.nilServer {
				srv = live
			}
			if h := srv.mcpHandler(); h == nil || h.NewHTTPHandler() == nil {
				t.Fatal("expected non-nil MCP handler")
			}
			if rc := srv.restateClient(); rc == nil {
				t.Fatal("expected non-nil Restate client")
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

// TestSetupMuxNilServer covers the standalone metrics branch used when no
// server instance backs the mux.
func TestSetupMuxNilServer(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(SetupMux(nil))
	t.Cleanup(ts.Close)
	resp, err := ts.Client().Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
