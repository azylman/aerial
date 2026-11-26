package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis/internal/metrics"
	"github.com/azylman/aerial/services/vanadis/internal/storetest"
)

func scrapeRootMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want %d", rec.Code, http.StatusOK)
	}
	return rec.Body.String()
}

func TestVanadisConsumerStatsQuerier(t *testing.T) {
	t.Parallel()
	var nilSrv *VanadisServer
	if nilSrv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil for nil server")
	}
	nilSrv.collectQueueMetrics(context.Background())

	srv := NewVanadisServer(Config{}, RestateConfig{}, nil, nil)
	if srv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil when no stats or bus configured")
	}
	mockQ := &pubsubtest.ConsumerStats{}
	srv.queueStats = mockQ
	if srv.consumerStatsQuerier() != mockQ {
		t.Errorf("expected queueStats override")
	}
}

type fakeQueueStatsBus struct {
	stats    pubsubtest.ConsumerStats
	closeErr error
}

func (f *fakeQueueStatsBus) ConsumerStats(ctx context.Context, stream, consumer string) (int64, int64, error) {
	return f.stats.ConsumerStats(ctx, stream, consumer)
}

func (f *fakeQueueStatsBus) Close() error { return f.closeErr }

func TestInitQueueStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		natsURL   string
		bus       *fakeQueueStatsBus
		busErr    error
		wantStats bool
	}{
		{name: "no nats url leaves stats unset", natsURL: ""},
		{name: "connect error leaves stats unset", natsURL: "nats://nats.test:4222", busErr: errors.New("connect refused")},
		{name: "connected bus backs queue stats", natsURL: "nats://nats.test:4222", bus: &fakeQueueStatsBus{}, wantStats: true},
		{name: "close error is logged not fatal", natsURL: "nats://nats.test:4222", bus: &fakeQueueStatsBus{closeErr: errors.New("already closed")}, wantStats: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewVanadisServer(Config{}, RestateConfig{}, nil, nil)
			newBus := func(context.Context, string) (queueStatsBus, error) {
				if tc.busErr != nil {
					return nil, tc.busErr
				}
				return tc.bus, nil
			}
			closeFn := initQueueStats(context.Background(), srv, tc.natsURL, newBus)
			if got := srv.consumerStatsQuerier() != nil; got != tc.wantStats {
				t.Errorf("queue stats configured = %v, want %v", got, tc.wantStats)
			}
			closeFn()
		})
	}
}

func TestInitPostgresAndRunServer(t *testing.T) {
	ctx := context.Background()
	srv := NewVanadisServer(Config{}, RestateConfig{}, nil, nil)
	if _, err := initPostgres(ctx, srv, "postgres://user:pass@invalid%xx/db"); err == nil {
		t.Fatal("expected parse error")
	}
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db")
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig failed: %v", err)
	}
	cleanup, err := initPostgres(ctx, srv, cfg.ConnString())
	if err != nil || cleanup == nil {
		t.Fatalf("expected success from initPostgres, got err=%v", err)
	}
	cleanup(ctx)
	if runServer(ctx, Config{Port: "-1", PostgresURL: cfg.ConnString()}, RestateConfig{}, nil) == nil || runServer(ctx, Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, RestateConfig{}, nil) == nil {
		t.Fatal("expected error on invalid runServer configs")
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- runServer(runCtx, Config{Port: "0", PostgresURL: cfg.ConnString()}, RestateConfig{}, func(addr string) { ready <- addr })
	}()
	select {
	case addr := <-ready:
		if addr == "" {
			t.Fatal("expected non-empty listen addr")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server ready")
	}
	runCancel()
	if err := <-done; err != nil {
		t.Fatalf("unexpected runServer exit error: %v", err)
	}
	// RunApp resolves Restate config from CONFIG_PATH plus env overrides; pin
	// both to hermetic values so the test never reads real config or pod env.
	restateYAML := filepath.Join(t.TempDir(), "vanadis.yaml")
	if err := os.WriteFile(restateYAML, []byte("enabled: false\n"), 0o600); err != nil {
		t.Fatalf("write restate config: %v", err)
	}
	t.Setenv("CONFIG_PATH", restateYAML)
	for _, key := range []string{"RESTATE_INGRESS_URL", "VANADIS_RESTATE_ENABLED", "RESTATE_ENABLED", "AGENT_NAME"} {
		t.Setenv(key, "")
	}
	appCtx, appCancel := context.WithCancel(context.Background())
	appCancel()
	if err := RunApp(appCtx, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected RunApp error: %v", err)
	}
}

func TestCreateBuildTerminalResponses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"invalid", 400, `{"message":"invalid dependency"}`, "invalid dependency"},
		{"missing", 404, `{"message":"build not found"}`, "build not found"},
		{"conflict", 409, `{"message":"chunk ID already exists"}`, "chunk ID already exists"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			s := NewVanadisServer(Config{}, RestateConfig{}, newMemoryVanadisStore(), upstream.Client())
			s.restateCfg.RestateIngressURL = upstream.URL
			rec := httptest.NewRecorder()
			s.handleCreateBuild(rec, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(`{"id":"example","thread_id":"123456789012345678","objective":"test","chunks":[{"repo":"aerial","title":"test","instructions":"test","depends_on":[]}]}`)))
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestRoutes_ServerSpanPropagatesToRestate(t *testing.T) {
	// Not parallel: swaps the global tracer provider and propagator.
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tests := []struct {
		name            string
		traceparent     string
		wantTraceID     string
		wantStatus      int
		wantRestateCall bool
	}{
		{"inbound trace continues into restate", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "4bf92f3577b34da6a3ce929d0e0e4736", http.StatusAccepted, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotTP string
			restate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotTP = r.Header.Get("Traceparent")
				_, _ = w.Write([]byte(`{"id":"b1","thread_id":"123456789012345678"}`))
			}))
			defer restate.Close()
			srv := &VanadisServer{
				store:      newMemoryVanadisStore(),
				restateCfg: RestateConfig{RestateIngressURL: restate.URL},
				httpClient: tracing.Client(restate.Client()),
			}
			body := `{"thread_id":"123456789012345678","objective":"o","chunks":[{"id":"c1","repo":"aerial","title":"t","instructions":"i","target_files":[],"target_symbols":[],"acceptance_criteria":[],"depends_on":[]}]}`
			req := httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(body))
			req.Header.Set("Traceparent", tc.traceparent)
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			parts := strings.Split(gotTP, "-")
			if len(parts) != 4 || parts[1] != tc.wantTraceID {
				t.Errorf("restate traceparent = %q, want trace id %s", gotTP, tc.wantTraceID)
			}
		})
	}
}

// TestMCPHandlerReceivers pins the observable wiring of the root MCP
// delegation helpers: which store instance is propagated, the RestateEnabled
// flag value, and that a nil server reports a missing store (it cannot
// resolve the build's runner) instead of panicking.
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
				srv = NewVanadisServer(Config{Port: "0"}, RestateConfig{}, mem, nil)
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
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "vanadis store not configured") {
					t.Fatalf("nil-server retry status=%d body=%s, want store-unavailable tool error", w.Code, w.Body.String())
				}
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

	srv := NewVanadisServer(Config{Port: "0"}, RestateConfig{}, newMemoryVanadisStore(), nil)
	srv.restateCfg = RestateConfig{}
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	seeded := newMemoryVanadisStore()
	if err := seeded.CreateBuild(context.Background(), storetest.Build("build-live", "1234567890", "Live Title", "Live Objective", "running"), []VanadisChunk{storetest.Chunk("chunk-1", "build-live", 0, "Chunk 1", "Inst 1", "medium", "pending")}); err != nil {
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
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}
			if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), tc.wantSubstr) {
				t.Fatalf("status=%d body=%s, want substring %q", resp.StatusCode, string(body), tc.wantSubstr)
			}
			if strings.Contains(string(body), `"isError":true`) {
				t.Fatalf("unexpected tool error: %s", string(body))
			}
		})
	}
}

// TestCollectBuildMetrics pins that collectBuildMetrics leaves the gauges
// untouched when it has nothing to read. Not parallel: shares global gauges.
func TestCollectBuildMetrics(t *testing.T) {
	tests := []struct {
		name string
		srv  *VanadisServer
	}{
		{name: "nil server", srv: nil},
		{name: "nil store", srv: &VanadisServer{}},
		{name: "store list error", srv: NewVanadisServer(Config{}, RestateConfig{}, &storetest.Store{ListErr: errors.New("db down")}, nil)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics.BuildsActive.WithLabelValues("running").Set(7)
			tc.srv.collectBuildMetrics(context.Background())
			if want, body := `vanadis_builds_active{queue_state="running"} 7`, scrapeRootMetrics(t); !strings.Contains(body, want) {
				t.Errorf("metrics exposition missing %q\nbody:\n%s", want, body)
			}
		})
	}
}

func TestRestateObjectPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, runner, buildID, handler, want string
	}{
		{name: "named runner targets the canonical Build service", runner: "agent-alpha", buildID: "b1", handler: "Create", want: "/Build/b1/Create"},
		{name: "default runner targets the canonical Build service", runner: "", buildID: "b2", handler: "Cancel", want: "/Build/b2/Cancel"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewVanadisServer(Config{}, RestateConfig{AgentName: "agent-beta", RestateIngressURL: "http://restate.test"}, nil, nil)
			if got := srv.restateObjectPath(tc.runner, tc.buildID, tc.handler); got != tc.want {
				t.Errorf("restateObjectPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

// routesStore seeds every build the TestRoutes rows address. Chunk IDs are
// unique across builds so GetChunk resolves deterministically; the Restate
// mock fails Build paths containing "fail" and conflicts on "b-done".
func routesStore() *memoryVanadisStore {
	s := newMemoryVanadisStore()
	seed := func(id, buildAgent, status string, chunks ...VanadisChunk) {
		b := storetest.Build(id, "1234567890", "Build "+id, "obj", status)
		b.Agent = buildAgent
		s.Builds[id], s.Chunks[id] = b, chunks
	}
	seed("b1", "aerial", "running", storetest.Chunk("c1", "b1", 0, "Chunk 1", "Inst 1", "", "running"), storetest.Chunk("c2", "b1", 1, "Chunk 2", "Inst 2", "", "pending"))
	seed("b-empty", "", "running", storetest.Chunk("c-empty", "b-empty", 0, "Chunk", "Inst", "", "running"))
	seed("b-fail", "aerial", "running", storetest.Chunk("c-fail", "b-fail", 0, "Chunk", "Inst", "", "running"))
	seed("b-done", "aerial", "completed")
	seed("b-runner-alpha", "agent-alpha", "running",
		storetest.Chunk("c-alpha-1", "b-runner-alpha", 0, "Chunk", "Inst", "", "running"), storetest.Chunk("c-alpha-2", "b-runner-alpha", 1, "Chunk", "Inst", "", "pending"))
	return s
}

// routesFailingStore is routesStore with injected GetBuild/GetChunk and ListBuilds errors.
func routesFailingStore(getErr, listErr error) *memoryVanadisStore {
	s := routesStore()
	s.GetErr, s.ListErr = getErr, listErr
	return s
}

// routesListStore holds one running, one completed and one queued build, newest first.
func routesListStore() *memoryVanadisStore {
	s := newMemoryVanadisStore()
	now := time.Now().UTC()
	for i, b := range []struct{ id, buildStatus, chunkStatus string }{{"b-run", "running", "running"}, {"b-comp", "completed", "completed"}, {"b-q", "running", "pending"}} {
		build := storetest.Build(b.id, "111", "", "", b.buildStatus)
		build.CreatedAt = now.Add(-time.Duration(i+1) * time.Minute)
		s.Builds[b.id], s.Chunks[b.id] = build, []VanadisChunk{storetest.Chunk("c-"+b.id, b.id, 0, "C", "", "", b.chunkStatus)}
	}
	return s
}

// routesMetricsStore holds a running build (one running, one pending chunk),
// a queued build (one pending chunk) and a completed build.
func routesMetricsStore() *memoryVanadisStore {
	s := newMemoryVanadisStore()
	s.Builds["b-e2e-run"] = storetest.Build("b-e2e-run", "111", "Running", "obj", "running")
	s.Chunks["b-e2e-run"] = []VanadisChunk{storetest.Chunk("c-e2e-1", "b-e2e-run", 0, "C1", "I1", "", "running"), storetest.Chunk("c-e2e-2", "b-e2e-run", 1, "C2", "I2", "", "pending")}
	s.Builds["b-e2e-queue"] = storetest.Build("b-e2e-queue", "222", "Queued", "obj", "running")
	s.Chunks["b-e2e-queue"] = []VanadisChunk{storetest.Chunk("c-e2e-3", "b-e2e-queue", 0, "C3", "I3", "", "pending")}
	s.Builds["b-e2e-done"] = storetest.Build("b-e2e-done", "333", "Done", "obj", "completed")
	s.Chunks["b-e2e-done"] = []VanadisChunk{storetest.Chunk("c-e2e-4", "b-e2e-done", 0, "C4", "I4", "", "merged")}
	return s
}

const (
	routesToken      = "Bearer valid-token"
	routesCreate     = `{"thread_id":"1234567890","objective":"obj","chunks":[{"mode":"action","title":"c","instructions":"i","depends_on":[]}]}`
	routesCreateFull = `{"thread_id":"stateful_medium:discord-1555999","title":"Feature X","objective":"Implement feature X","chunks":[` +
		`{"repo":"aerial","title":"Part 1","instructions":"Do part 1","effort":"low","depends_on":[]},` +
		`{"repo":"aerial","id":"custom-chunk-2","title":"Part 2","instructions":"Do part 2","target_files":["a.go"],"effort":"high","depends_on":["0"]}]}`
	routesMCPBanner = "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n"
	routesStep      = `{"chunk_id":"c1","active_step":"step 1"}`
	routesAppend    = `{"chunks":[{"mode":"action","title":"c2","instructions":"i2"}]}`
)

// TestRoutes fixtures: stores and auth fakes are read-only once built.
var (
	routesSeeded, routesListed, routesMetrics = routesStore(), routesListStore(), routesMetricsStore()
	routesGetFails                            = routesFailingStore(errors.New("simulated get err"), nil)
	routesListFails                           = routesFailingStore(nil, errors.New("simulated list failure"))
	routesAllow, routesDeny, routesUnauth     = authFake(true, true), authFake(true, false), authFake(false, false)
)

// routeCase is one TestRoutes row.
type routeCase struct {
	name, method, path, body string
	errBody                  bool
	store                    VanadisStore
	nilServer                bool
	auth                     *k8sauthtest.RoundTripper
	token                    string
	wantStatus               int
	wantSubstr               string
}

var routeCases = []routeCase{
	// Health, metrics, MCP and coverage flush (auth-exempt or wiring).
	{name: "health exempt from auth", method: http.MethodGet, path: "/health", store: routesSeeded, auth: routesAllow, wantStatus: http.StatusOK, wantSubstr: `"status":"ok"`},
	{name: "healthz exempt from auth", method: http.MethodGet, path: "/healthz", store: routesSeeded, auth: routesAllow, wantStatus: http.StatusOK, wantSubstr: `"status":"ok"`},
	{name: "metrics exempt from auth", method: http.MethodGet, path: "/metrics", store: routesSeeded, auth: routesAllow, wantStatus: http.StatusOK},
	{name: "metrics running builds", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_builds_active{queue_state="running"} 1`},
	{name: "metrics queued builds", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_builds_active{queue_state="queued"} 1`},
	{name: "metrics pending chunks", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_chunks_active{status="pending"} 2`},
	{name: "metrics running chunks", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_chunks_active{status="running"} 1`},
	{name: "metrics pr_opened chunks zero", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_chunks_active{status="pr_opened"} 0`},
	{name: "metrics queue depth without stats", method: http.MethodGet, path: "/metrics", store: routesMetrics, wantStatus: http.StatusOK, wantSubstr: `vanadis_queue_depth{state="pending"} 0`},
	{name: "nil server serves standalone metrics", method: http.MethodGet, path: "/metrics", nilServer: true, wantStatus: http.StatusOK},
	{name: "mcp ready banner", method: http.MethodGet, path: "/mcp", store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: routesMCPBanner},
	{name: "sse ready banner", method: http.MethodGet, path: "/sse", store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: routesMCPBanner},
	{name: "nil server mcp ready banner", method: http.MethodGet, path: "/mcp", nilServer: true, wantStatus: http.StatusOK, wantSubstr: routesMCPBanner},
	{
		name: "nil server mcp reports missing store", method: http.MethodPost, path: "/mcp", nilServer: true, wantStatus: http.StatusOK, wantSubstr: "vanadis store not configured",
		body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_build","arguments":{"build_id":"build-live"}}}`,
	},
	{name: "coverage flush post", method: http.MethodPost, path: "/debug/coverage/flush", store: routesSeeded, wantStatus: http.StatusOK},
	{name: "coverage flush get", method: http.MethodGet, path: "/debug/coverage/flush", store: routesSeeded, wantStatus: http.StatusOK},

	// Auth middleware wiring and list builds.
	{name: "list malformed token", method: http.MethodGet, path: "/builds", store: routesSeeded, auth: routesAllow, token: "Basic abc", wantStatus: http.StatusUnauthorized, wantSubstr: "invalid bearer token"},
	{name: "list unauthenticated token", method: http.MethodGet, path: "/builds", store: routesSeeded, auth: routesUnauth, token: routesToken, wantStatus: http.StatusUnauthorized, wantSubstr: "token not authenticated"},
	{name: "list forbidden", method: http.MethodGet, path: "/builds", store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden, wantSubstr: "access denied"},
	{name: "list allowed", method: http.MethodGet, path: "/builds", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK},
	{name: "list nil store", method: http.MethodGet, path: "/builds", wantStatus: http.StatusInternalServerError, wantSubstr: "vanadis store not configured"},
	{name: "list store error", method: http.MethodGet, path: "/builds", store: routesListFails, wantStatus: http.StatusInternalServerError, wantSubstr: "simulated list failure"},
	{name: "list head not allowed", method: http.MethodHead, path: "/builds", store: routesListed, wantStatus: http.StatusMethodNotAllowed},
	{
		name: "list counts queue states", method: http.MethodGet, path: "/builds", store: routesListed, wantStatus: http.StatusOK,
		wantSubstr: `"completed_count":1,"queued_count":1,"running_count":1,"status":"ok","total":3`,
	},
	{name: "runs alias lists builds", method: http.MethodGet, path: "/runs", store: routesListed, wantStatus: http.StatusOK, wantSubstr: `"total":3`},
	{name: "list queue_state filter", method: http.MethodGet, path: "/builds?queue_state=running", store: routesListed, wantStatus: http.StatusOK, wantSubstr: `"builds":[{"id":"b-run"`},
	{name: "list status filter", method: http.MethodGet, path: "/builds?status=completed", store: routesListed, wantStatus: http.StatusOK, wantSubstr: `"builds":[{"id":"b-comp"`},
	{name: "list limit", method: http.MethodGet, path: "/builds?limit=1", store: routesListed, wantStatus: http.StatusOK, wantSubstr: `"total":1`},

	// Create build.
	{name: "create nil store", method: http.MethodPost, path: "/builds", body: `{}`, wantStatus: http.StatusInternalServerError},
	{name: "create body read error", method: http.MethodPost, path: "/builds", errBody: true, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "failed to read request body"},
	{name: "create invalid json", method: http.MethodPost, path: "/builds", body: `{invalid`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "invalid json payload"},
	{name: "create bad thread id", method: http.MethodPost, path: "/builds", body: `{"thread_id":"not-numeric"}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "thread_id is required"},
	{name: "create missing objective", method: http.MethodPost, path: "/builds", body: `{"thread_id":"123","objective":""}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "objective is required"},
	{name: "create no chunks", method: http.MethodPost, path: "/builds", body: `{"thread_id":"123","objective":"obj","chunks":[]}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "at least one chunk"},
	{name: "create invalid chunk", method: http.MethodPost, path: "/builds", body: `{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"","instructions":"i"}]}`, store: routesSeeded, wantStatus: http.StatusBadRequest},
	{name: "create without auth accepts", method: http.MethodPost, path: "/builds", body: routesCreateFull, store: routesSeeded, wantStatus: http.StatusAccepted, wantSubstr: `"chunk_count":2`},
	{name: "create allowed", method: http.MethodPost, path: "/builds", body: routesCreate, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusAccepted, wantSubstr: `"status":"accepted"`},
	{name: "create forbidden", method: http.MethodPost, path: "/builds", body: routesCreate, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "create invalid agent slug", method: http.MethodPost, path: "/builds", body: `{"agent":"BAD_SLUG!",` + routesCreate[1:], store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusBadRequest, wantSubstr: "invalid target agent"},
	{name: "create for another agent", method: http.MethodPost, path: "/builds", body: `{"agent":"agent-alpha",` + routesCreate[1:], store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusAccepted},

	// Attach PR and chunk by ID.
	{
		name: "attach pins agent caller to its own agent", method: http.MethodPost, path: "/chunks/attach-pr", body: `{"repo":"aerial","pr_number":7,"agent":"agent-alpha"}`, store: routesSeeded, auth: routesAllow, token: routesToken,
		wantStatus: http.StatusOK, wantSubstr: `"thread_id":"thread-aerial"`,
	},
	{name: "chunk by id allowed", method: http.MethodGet, path: "/chunks/c1", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"id":"c1"`},
	{name: "chunk by id forbidden", method: http.MethodGet, path: "/chunks/c1", store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "chunk by id not found", method: http.MethodGet, path: "/chunks/c-missing", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusNotFound},
	{name: "chunk by id store error", method: http.MethodGet, path: "/chunks/c1", store: routesGetFails, wantStatus: http.StatusInternalServerError, wantSubstr: "simulated get err"},

	// Build root and path parsing.
	{name: "build nil store", method: http.MethodGet, path: "/builds/b1", wantStatus: http.StatusInternalServerError},
	{name: "build empty id", method: http.MethodGet, path: "/builds/", store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "build id is required"},
	{name: "build blank id", method: http.MethodPost, path: "/builds/%20/cancel", store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "build id is required"},
	{name: "get build allowed", method: http.MethodGet, path: "/builds/b1", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"id":"b1"`},
	{name: "get build forbidden", method: http.MethodGet, path: "/builds/b1", store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "get build not found", method: http.MethodGet, path: "/builds/unknown-id", store: routesSeeded, wantStatus: http.StatusNotFound},
	{name: "get build store error", method: http.MethodGet, path: "/builds/b1", store: routesGetFails, wantStatus: http.StatusInternalServerError, wantSubstr: "simulated get err"},
	{name: "get build summarizes entry", method: http.MethodGet, path: "/builds/b-run", store: routesListed, wantStatus: http.StatusOK, wantSubstr: `"queue_state":"running","total_chunks":1`},
	{name: "build root method not allowed", method: http.MethodPut, path: "/builds/b1", store: routesSeeded, wantStatus: http.StatusMethodNotAllowed},
	{name: "build unknown subpath", method: http.MethodGet, path: "/builds/b1/unknown", store: routesSeeded, wantStatus: http.StatusNotFound},
	{name: "build three parts not chunks", method: http.MethodGet, path: "/builds/b1/notchunks/c1", store: routesSeeded, wantStatus: http.StatusNotFound},
	{name: "build deep unknown subpath", method: http.MethodGet, path: "/builds/b1/chunks/c1/deep", store: routesSeeded, wantStatus: http.StatusNotFound},
	// Cancel build.
	{name: "cancel allowed", method: http.MethodPost, path: "/builds/b1/cancel", body: `{"reason":"cancel"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"status":"cancelled"`},
	{name: "cancel forbidden", method: http.MethodPost, path: "/builds/b1/cancel", body: `{"reason":"cancel"}`, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "cancel not found", method: http.MethodPost, path: "/builds/b-missing/cancel", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusNotFound},
	{name: "cancel empty agent fallback", method: http.MethodPost, path: "/builds/b-empty/cancel", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK},
	{name: "cancel conflict", method: http.MethodPost, path: "/builds/b-done/cancel", store: routesSeeded, wantStatus: http.StatusConflict},
	{name: "cancel routes to runner build", method: http.MethodPost, path: "/builds/b-runner-alpha/cancel", store: routesSeeded, wantStatus: http.StatusOK},
	{name: "cancel store error", method: http.MethodPost, path: "/builds/b1/cancel", store: routesGetFails, wantStatus: http.StatusInternalServerError},
	{name: "cancel method not allowed", method: http.MethodGet, path: "/builds/b1/cancel", store: routesSeeded, wantStatus: http.StatusMethodNotAllowed},

	// Active routesStep via /chunks/active-routesStep.
	{name: "chunks active step nil store", method: http.MethodPost, path: "/chunks/active-step", body: routesStep, wantStatus: http.StatusInternalServerError},
	{name: "chunks active step invalid json", method: http.MethodPost, path: "/chunks/active-step", body: `{bad json`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusBadRequest},
	{name: "chunks active step missing chunk_id", method: http.MethodPost, path: "/chunks/active-step", body: `{"active_step":"step 1"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusBadRequest},
	{name: "chunks active step chunk not found", method: http.MethodPost, path: "/chunks/active-step", body: `{"chunk_id":"c-missing"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusNotFound},
	{name: "chunks active step forbidden", method: http.MethodPost, path: "/chunks/active-step", body: routesStep, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "chunks active step success", method: http.MethodPost, path: "/chunks/active-step", body: routesStep, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"status":"ok"`},
	{name: "chunks active step empty agent fallback", method: http.MethodPost, path: "/chunks/active-step", body: `{"chunk_id":"c-empty"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK},
	{name: "chunks active step restate error", method: http.MethodPost, path: "/chunks/active-step", body: `{"chunk_id":"c-fail"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusInternalServerError},

	// Active routesStep via /builds/{id}/active-routesStep.
	{name: "builds active step method not allowed", method: http.MethodGet, path: "/builds/b1/active-step", store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusMethodNotAllowed},
	{name: "builds active step invalid json", method: http.MethodPost, path: "/builds/b1/active-step", body: `{bad json`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusBadRequest},
	{name: "builds active step missing chunk_id", method: http.MethodPost, path: "/builds/b1/active-step", body: `{"active_step":"step 1"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusBadRequest},
	{name: "builds active step build not found", method: http.MethodPost, path: "/builds/b-missing/active-step", body: routesStep, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusNotFound},
	{name: "builds active step forbidden", method: http.MethodPost, path: "/builds/b1/active-step", body: routesStep, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "builds active step success", method: http.MethodPost, path: "/builds/b1/active-step", body: routesStep, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"status":"ok"`},
	{name: "builds active step empty agent fallback", method: http.MethodPost, path: "/builds/b-empty/active-step", body: `{"chunk_id":"c-empty"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK},
	{name: "builds active step restate error", method: http.MethodPost, path: "/builds/b-fail/active-step", body: `{"chunk_id":"c-fail"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusInternalServerError},
	{name: "builds active step routes to runner build", method: http.MethodPost, path: "/builds/b-runner-alpha/active-step", body: `{"chunk_id":"c-alpha-1"}`, store: routesSeeded, wantStatus: http.StatusOK},

	// Steer chunk via /builds/{id}/chunks/{chunk_ref}/steer.
	{name: "builds chunk steer method not allowed", method: http.MethodGet, path: "/builds/b1/chunks/c1/steer", store: routesSeeded, wantStatus: http.StatusMethodNotAllowed},
	{name: "builds chunk steer running chunk", method: http.MethodPost, path: "/builds/b1/chunks/c1/steer", body: `{"prompt":"steer running"}`, store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: `"chunk_status":"running"`},
	{name: "builds chunk steer pending chunk", method: http.MethodPost, path: "/builds/b1/chunks/c2/steer", body: `{"prompt":"steer pending"}`, store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: `"chunk_status":"pending"`},
	{name: "builds chunk steer missing prompt", method: http.MethodPost, path: "/builds/b1/chunks/c1/steer", body: `{"prompt":""}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "prompt is required"},
	{name: "builds chunk steer invalid json", method: http.MethodPost, path: "/builds/b1/chunks/c1/steer", body: `{invalid`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "invalid json payload"},
	{name: "builds chunk steer build not found", method: http.MethodPost, path: "/builds/b-missing/chunks/c1/steer", body: `{"prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusNotFound},
	{name: "builds chunk steer conflict", method: http.MethodPost, path: "/builds/b-done/chunks/c1/steer", body: `{"prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusConflict},
	{name: "builds chunk steer forbidden", method: http.MethodPost, path: "/builds/b1/chunks/c1/steer", body: `{"prompt":"steer"}`, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "builds chunk steer allowed", method: http.MethodPost, path: "/builds/b1/chunks/c1/steer", body: `{"prompt":"steer"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"status":"steered"`},

	// Steer chunk via /chunks/steer.
	{name: "chunks steer nil store", method: http.MethodPost, path: "/chunks/steer", wantStatus: http.StatusInternalServerError},
	{name: "chunks steer running chunk", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","chunk_ref":"c1","prompt":"steer running"}`, store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: `"chunk_status":"running"`},
	{name: "chunks steer pending chunk", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","chunk_ref":"c2","prompt":"steer pending"}`, store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: `"chunk_status":"pending"`},
	{name: "chunks steer resolves parent from chunk_id", method: http.MethodPost, path: "/chunks/steer", body: `{"chunk_id":"c1","prompt":"steer resolve"}`, store: routesSeeded, wantStatus: http.StatusOK, wantSubstr: `"build_id":"b1"`},
	{name: "chunks steer chunk not found", method: http.MethodPost, path: "/chunks/steer", body: `{"chunk_id":"c-missing","prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusNotFound},
	{name: "chunks steer missing prompt", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","chunk_ref":"c1","prompt":""}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "prompt is required"},
	{name: "chunks steer missing chunk_ref", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "chunk ref is required"},
	{name: "chunks steer missing build_id", method: http.MethodPost, path: "/chunks/steer", body: `{"prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "build id is required"},
	{name: "chunks steer invalid json", method: http.MethodPost, path: "/chunks/steer", body: `{invalid`, store: routesSeeded, wantStatus: http.StatusBadRequest, wantSubstr: "invalid json payload"},
	{name: "chunks steer conflict", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b-done","chunk_ref":"c1","prompt":"steer"}`, store: routesSeeded, wantStatus: http.StatusConflict},
	{name: "chunks steer forbidden", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","chunk_ref":"c1","prompt":"steer"}`, store: routesSeeded, auth: routesDeny, token: routesToken, wantStatus: http.StatusForbidden},
	{name: "chunks steer allowed", method: http.MethodPost, path: "/chunks/steer", body: `{"build_id":"b1","chunk_ref":"c1","prompt":"steer"}`, store: routesSeeded, auth: routesAllow, token: routesToken, wantStatus: http.StatusOK, wantSubstr: `"status":"steered"`},
}

// TestRoutes drives every HTTP route through SetupMux against the shared
// Restate mock (newMockRestateServer). auth nil means auth disabled; token is
// sent verbatim as the Authorization header. Not parallel: /metrics rows read
// and write the global Prometheus gauges, and the coverage-flush writers are
// package globals.
func TestRoutes(t *testing.T) {
	restateSrv := newMockRestateServer()
	t.Cleanup(restateSrv.Close)
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() { httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters })
	httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = func(string) error { return nil }, func(string) error { return nil }

	for _, tc := range routeCases {
		t.Run(tc.name, func(t *testing.T) {
			var srv *VanadisServer
			if !tc.nilServer {
				var auth []*k8sauth.Authenticator
				if tc.auth != nil {
					auth = append(auth, k8sauthtest.Authenticator(t, tc.auth))
				}
				srv = NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: restateSrv.URL}, tc.store, restateSrv.Client(), auth...)
			}
			var body io.Reader = http.NoBody
			switch {
			case tc.errBody:
				body = errorReader{}
			case tc.body != "":
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			rec := httptest.NewRecorder()
			SetupMux(srv).ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServerIngressIdempotencyKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		method  string
		path    string
		body    string
		wantKey string
	}{
		{
			name:    "attach chunk pr sets deterministic key",
			method:  http.MethodPost,
			path:    "/chunks/attach-pr",
			body:    `{"chunk_id":"c1","repo":"aerial","pr_number":42}`,
			wantKey: "attach-pr:c1:aerial:42",
		},
		{
			name:    "active step does not set idempotency key for latest-write streaming",
			method:  http.MethodPost,
			path:    "/chunks/active-step",
			body:    `{"chunk_id":"c1","active_step":"running tests"}`,
			wantKey: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedKey string
			restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedKey = r.Header.Get("Idempotency-Key")
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "RouteAttachPR") {
					_ = json.NewEncoder(w).Encode(AttachChunkPRResponse{Status: "attached", BuildID: "b1", ChunkID: "c1", ThreadID: "123"})
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer restateSrv.Close()

			srv := NewVanadisServer(Config{}, RestateConfig{RestateIngressURL: restateSrv.URL}, routesStore(), restateSrv.Client())
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			srv.Routes().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if capturedKey != tc.wantKey {
				t.Fatalf("Idempotency-Key = %q, want %q", capturedKey, tc.wantKey)
			}
		})
	}
}
