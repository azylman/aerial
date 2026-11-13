package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestSetupTracing mutates the global propagator, so it is not parallel.
func TestSetupTracing(t *testing.T) {
	tests := []struct {
		name      string
		content   string // "" means the config file is absent
		wantAgent string
		wantProxy bool // MCP trace proxy switched on
	}{
		{name: "missing config runs disabled", content: ""},
		{name: "malformed config runs disabled", content: "enabled: [true\nstack: marida\n"},
		{name: "enabled without endpoint runs disabled", content: "enabled: true\nstack: marida\n"},
		{name: "stack names the turn agent", content: "enabled: false\nstack: marida\n", wantAgent: "marida"},
		{name: "enabled config switches on the MCP trace proxy", content: "enabled: true\nendpoint: http://127.0.0.1:1/v1/traces\nstack: aerial\n", wantAgent: "aerial", wantProxy: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
			path := filepath.Join(t.TempDir(), "tracing.yaml")
			if tc.content != "" {
				if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			withTracing, shutdown := setupTracing(context.Background(), path)
			shutdown()
			var opts brainAppOptions
			withTracing(&opts)
			if opts.agentName != tc.wantAgent || opts.tracingEnabled != tc.wantProxy {
				t.Errorf("agent name = %q, proxy = %v; want %q, %v", opts.agentName, opts.tracingEnabled, tc.wantAgent, tc.wantProxy)
			}
			if fields := otel.GetTextMapPropagator().Fields(); !slices.Contains(fields, "traceparent") {
				t.Errorf("propagator fields = %v, want traceparent", fields)
			}
		})
	}
}

// storedTraceparent returns the traceparent persisted on the queued row (message or dispatch) with id.
func storedTraceparent(t *testing.T, store db.Store, id string) string {
	t.Helper()
	if m, err := store.GetMessage(context.Background(), id); err == nil && m != nil {
		return m.Traceparent
	}
	if d, err := store.GetDispatch(context.Background(), id); err == nil && d != nil {
		return d.Traceparent
	}
	t.Fatalf("no queued row with id %q", id)
	return ""
}

// TestIngressHandlers_StampTraceparent serves each queueing ingress through the production
// tracing middleware with tracing disabled (no config file), proving inbound trace context is
// still persisted on the row for the turn to resume.
func TestIngressHandlers_StampTraceparent(t *testing.T) {
	setupTestConfig(t, `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`)
	shutdown, err := tracing.Setup(context.Background(), "eri", tracing.Config{})
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tests := []struct {
		name        string
		handler     func(db.Store, *queue.WorkerPool) http.HandlerFunc
		path        string
		body        string
		traceparent string
		rowID       string
		want        string
	}{
		{
			name: "prompt", handler: handlePrompt, path: "/prompt", traceparent: inbound, rowID: "msg-tp-1", want: inbound,
			body: `{"prompt":"check","channel_id":"1542423172400291873","message_id":"msg-tp-1"}`,
		},
		{
			name: "prompt without trace context", handler: handlePrompt, path: "/prompt", rowID: "msg-tp-2", want: "",
			body: `{"prompt":"check","channel_id":"1542423172400291873","message_id":"msg-tp-2"}`,
		},
		{
			name: "dispatch", handler: handleDispatch, path: "/dispatch", traceparent: inbound, rowID: "disp-tp-1", want: inbound,
			body: `{"id":"disp-tp-1","source":"heartbeat","session_id":"heartbeat-tp","prompt":"poll"}`,
		},
		{
			name: "discord message", handler: handleInternalDiscordMessage, path: "/internal/discord/message", traceparent: inbound,
			rowID: "123456789012345679", want: inbound,
			body: `{"bot_user_id":"bot-1","message":{"id":"123456789012345679","channel_id":"ch-1","author":{"id":"u-1","username":"alex"},"content":"hey aerial"}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := dbtest.NewFakeStore()
			pool := newTestWorkerPool(store)
			mux := http.NewServeMux()
			mux.HandleFunc(tc.path, tc.handler(store, pool))

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			if tc.traceparent != "" {
				req.Header.Set("Traceparent", tc.traceparent)
			}
			rec := httptest.NewRecorder()
			tracing.Middleware("eri", metricsMiddleware(mux)).ServeHTTP(rec, req)
			if rec.Code >= http.StatusBadRequest {
				t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
			}
			if got := storedTraceparent(t, store, tc.rowID); got != tc.want {
				t.Errorf("stored traceparent = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBrainHandler_SkipsServerSpansForMCPProxy mutates the global tracer provider, so it is
// not parallel.
func TestBrainHandler_SkipsServerSpansForMCPProxy(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantSpans int
	}{
		{name: "ordinary route gets a server span", path: "/health", wantSpans: 1},
		{name: "MCP proxy route is left to the proxy", path: "/internal/mcp-proxy/discord/docket", wantSpans: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prev := otel.GetTracerProvider()
			defer otel.SetTracerProvider(prev)
			rec := tracetest.NewSpanRecorder()
			otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))

			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			w := httptest.NewRecorder()
			brainHandler(mux).ServeHTTP(w, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if w.Code != http.StatusNoContent {
				t.Errorf("status = %d, want 204", w.Code)
			}
			if got := len(rec.Ended()); got != tc.wantSpans {
				t.Errorf("server spans = %d, want %d", got, tc.wantSpans)
			}
		})
	}
}

func TestRegisterMCPTraceProxy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		enabled    bool
		wantPort   string
		wantStatus int
	}{
		{name: "tracing enabled mounts the proxy", enabled: true, wantPort: "8080", wantStatus: http.StatusAccepted},
		{name: "tracing disabled mounts nothing", wantStatus: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			routes, port := mcpTraceRoutes(tc.enabled, "8080")
			if port != tc.wantPort || (routes != nil) != tc.enabled {
				t.Fatalf("mcpTraceRoutes = %v, %q; want routes %v, port %q", routes, port, tc.enabled, tc.wantPort)
			}
			mux := http.NewServeMux()
			registerMCPTraceProxy(mux, routes, runner.NewActiveTurnRegistry())
			req := httptest.NewRequest(http.MethodPost, "/internal/mcp-proxy/announce", strings.NewReader(`{"daemon_id":"p1/discord-1","server":"docket","tool":"create"}`))
			req.RemoteAddr = "127.0.0.1:4000"
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Errorf("announce status = %d, want %d", w.Code, tc.wantStatus)
			}
		})
	}
}
