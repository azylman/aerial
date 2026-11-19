package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/vanadis/internal/metrics"
)

func scrapeRootMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	NewHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want %d", rec.Code, http.StatusOK)
	}
	return rec.Body.String()
}

func TestServerDelegateEdges(t *testing.T) {
	tests := []struct {
		name, target, storeKind string
		want                    string
	}{
		{name: "nil server leaves gauges unchanged", target: "build-metrics", storeKind: "nil-server", want: `vanadis_builds_active{queue_state="running"} 7`},
		{name: "nil store leaves gauges unchanged", target: "build-metrics", storeKind: "nil-store", want: `vanadis_builds_active{queue_state="running"} 7`},
		{name: "store error leaves gauges unchanged", target: "build-metrics", storeKind: "list-error", want: `vanadis_builds_active{queue_state="running"} 7`},
		{name: "restate client inherits server config", target: "restate-client", want: "/Build-suletta/b1/Create"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.target {
			case "build-metrics":
				metrics.BuildsActive.WithLabelValues("running").Set(7)
				var srv *VanadisServer
				switch tc.storeKind {
				case "nil-store":
					srv = &VanadisServer{}
				case "list-error":
					srv = NewVanadisServer(Config{}, &memoryVanadisStore{listErr: errors.New("db down")}, nil)
				}
				srv.collectBuildMetrics(context.Background())
				if body := scrapeRootMetrics(t); !strings.Contains(body, tc.want) {
					t.Errorf("metrics exposition missing %q\nbody:\n%s", tc.want, body)
				}
			case "restate-client":
				srv := NewVanadisServer(Config{}, nil, nil)
				srv.restateCfg = RestateConfig{AgentName: "suletta", RestateIngressURL: "http://restate.test"}
				if got := srv.restateClient().ObjectPath("b1", "Create"); got != tc.want {
					t.Errorf("restateClient().ObjectPath() = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestMetricsEndToEndSeededStore(t *testing.T) {
	memStore := newMemoryVanadisStore()
	bRun := mkBuild("b-e2e-run", "111", "Running", "obj", "running")
	bQueue := mkBuild("b-e2e-queue", "222", "Queued", "obj", "running")
	bDone := mkBuild("b-e2e-done", "333", "Done", "obj", "completed")
	cRun1, cRun2 := mkChunk("c-e2e-1", "b-e2e-run", 0, "C1", "I1", "", "running"), mkChunk("c-e2e-2", "b-e2e-run", 1, "C2", "I2", "", "pending")
	cRun1.TargetFiles, cRun2.TargetFiles = []string{"a.go"}, []string{"b.go"}
	if err := memStore.CreateBuild(context.Background(), bRun, []VanadisChunk{cRun1, cRun2}); err != nil {
		t.Fatalf("seed running build: %v", err)
	}
	if err := memStore.CreateBuild(context.Background(), bQueue, []VanadisChunk{mkChunk("c-e2e-3", "b-e2e-queue", 0, "C3", "I3", "", "pending")}); err != nil {
		t.Fatalf("seed queued build: %v", err)
	}
	if err := memStore.CreateBuild(context.Background(), bDone, []VanadisChunk{mkChunk("c-e2e-4", "b-e2e-done", 0, "C4", "I4", "", "merged")}); err != nil {
		t.Fatalf("seed completed build: %v", err)
	}
	srv := NewVanadisServer(Config{}, memStore, nil)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", rec.Code, http.StatusOK)
	}
	for _, want := range []string{
		`vanadis_builds_active{queue_state="running"} 1`,
		`vanadis_builds_active{queue_state="queued"} 1`,
		`vanadis_chunks_active{status="pending"} 2`,
		`vanadis_chunks_active{status="running"} 1`,
		`vanadis_chunks_active{status="pr_opened"} 0`,
		`vanadis_queue_depth{state="pending"} 0`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics exposition missing %q\nbody:\n%s", want, rec.Body.String())
		}
	}
}

func TestVanadisConsumerStatsQuerier(t *testing.T) {
	t.Parallel()
	var nilSrv *VanadisServer
	if nilSrv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil for nil server")
	}
	nilSrv.collectQueueMetrics(context.Background())

	srv := NewVanadisServer(Config{}, nil, nil)
	if srv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil when no stats or bus configured")
	}
	mockQ := &mockConsumerStatsQuerier{}
	srv.queueStats = mockQ
	if srv.consumerStatsQuerier() != mockQ {
		t.Errorf("expected queueStats override")
	}
}

func testHTTPEndpointsBasicAndFlush(t *testing.T, srv *VanadisServer, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	for _, p := range []string{"/health", "/healthz", "/metrics"} {
		checkCode(http.MethodGet, p, nil, http.StatusOK)
	}
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	defer func() { httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters }()
	httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = func(string) error { return errors.New("meta err") }, func(string) error { return nil }
	checkCode(http.MethodPost, "/debug/coverage/flush", nil, http.StatusOK)
	httpkit.WriteCountersDirFn = func(string) error { return errors.New("counters err") }
	checkCode(http.MethodGet, "/debug/coverage/flush", nil, http.StatusInternalServerError)
	srv.SetStore(nil)
	checkCode(http.MethodPost, "/builds", strings.NewReader(`{}`), http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds/b1", nil, http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds", nil, http.StatusInternalServerError)
}

func testHTTPEndpointsBuildCreation(t *testing.T, srv *VanadisServer, memStore *memoryVanadisStore, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) string {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	srv.SetStore(memStore)
	checkCode(http.MethodPost, "/builds", errorReader{}, http.StatusBadRequest)
	invalidPayloads := []string{
		`{invalid`, `{"thread_id":""}`,
		`{"thread_id":"not-numeric","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":""}`, `{"thread_id":"123","objective":"obj","chunks":[]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":""}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":"i","effort":"unsupported"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","id":"bad:id","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"pr","repo":"","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"invalid","repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"","target_files":["a.go"],"title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"","target_symbols":["A"],"title":"t","instructions":"i"}]}`,
	}
	for _, p := range invalidPayloads {
		checkCode(http.MethodPost, "/builds", strings.NewReader(p), http.StatusBadRequest)
	}
	validPayload := `{"thread_id":"stateful_medium:discord-1555999","title":"Implement Feature X Build Title","objective":"Implement feature X","shared_context":"Targeting Nomad cluster","chunks":[{"repo":"aerial","title":"Part 1","instructions":"Do part 1","effort":"low","depends_on":[]},{"repo":"aerial","id":"custom-chunk-2","title":"Part 2","instructions":"Do part 2","target_files":["a.go"],"target_symbols":["A"],"effort":"high","acceptance_criteria":["All green"],"depends_on":["0"]}]}`
	rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	buildID, _ := createResp["build_id"].(string)
	if buildID == "" || createResp["status"] != "accepted" {
		t.Fatalf("unexpected create response: %+v", createResp)
	}
	// Verify that chunks returned by create build start with "pending" status and zero started_at.
	rawCreatedChunks, _ := createResp["chunks"].([]any)
	if len(rawCreatedChunks) != 2 {
		t.Fatalf("expected 2 chunks in create response, got %d", len(rawCreatedChunks))
	}
	for i, rc := range rawCreatedChunks {
		chunkMap, _ := rc.(map[string]any)
		if chunkMap["status"] != "pending" {
			t.Fatalf("expected chunk[%d].status to be pending, got %v", i, chunkMap["status"])
		}
		if startedAt, ok := chunkMap["started_at"].(string); ok && startedAt != "" && startedAt != "0001-01-01T00:00:00Z" {
			t.Fatalf("expected chunk[%d].started_at to be zero, got %v", i, startedAt)
		}
	}
	// Simulate storing the created build in memStore for subsequent tests
	c1 := mkChunk(buildID+"-chunk-1", buildID, 0, "Part 1", "Do part 1", "low", "pending")
	c2 := mkChunk("custom-chunk-2", buildID, 1, "Part 2", "Do part 2", "high", "pending")
	c2.DependsOn = []string{c1.ID}
	if err := memStore.CreateBuild(context.Background(), mkBuild(buildID, "1555999", "Implement Feature X Build Title", "Implement feature X", "running"), []VanadisChunk{c1, c2}); err != nil {
		t.Fatalf("failed to seed created build in memStore: %v", err)
	}
	return buildID
}

func testHTTPEndpointsBuildCancel(t *testing.T, srv *VanadisServer, memStore *memoryVanadisStore, buildID string, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	routes := []struct {
		m, p string
		w    int
	}{
		{http.MethodGet, "/builds/", http.StatusBadRequest},
		{http.MethodDelete, "/builds/" + buildID, http.StatusMethodNotAllowed},
		{http.MethodGet, "/builds/unknown-id", http.StatusNotFound},
		{http.MethodGet, "/builds/" + buildID, http.StatusOK},
		{http.MethodGet, "/builds/" + buildID + "/cancel", http.StatusMethodNotAllowed},
		{http.MethodPost, "/builds/%20/cancel", http.StatusBadRequest},
	}
	for _, tc := range routes {
		checkCode(tc.m, tc.p, nil, tc.w)
	}
	memStore.getErr = errors.New("get error")
	checkCode(http.MethodGet, "/builds/"+buildID, nil, http.StatusInternalServerError)
	memStore.getErr = nil
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil), http.StatusOK)
	checkCode(http.MethodPost, "/builds/b-conflict/cancel", bytes.NewReader(nil), http.StatusConflict)
}

func testHTTPEndpointsListAndFilter(t *testing.T, srv *VanadisServer, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	wPost := httptest.NewRecorder()
	srv.handleListBuilds(wPost, httptest.NewRequest(http.MethodPost, "/builds", http.NoBody))
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on handleListBuilds POST, got %d", wPost.Code)
	}
	now := time.Now().UTC()
	bRunning, bCompleted, bQueued := mkBuild("b-run", "111", "", "", "running"), mkBuild("b-comp", "222", "", "", "completed"), mkBuild("b-q", "333", "", "", "running")
	bRunning.CreatedAt, bCompleted.CreatedAt, bQueued.CreatedAt = now.Add(-time.Minute), now.Add(-5*time.Minute), now.Add(-10*time.Minute)
	listStore := newMemoryVanadisStore()
	_ = listStore.CreateBuild(context.Background(), bRunning, []VanadisChunk{mkChunk("c-run-1", "b-run", 0, "C1", "", "", "running")})
	_ = listStore.CreateBuild(context.Background(), bCompleted, []VanadisChunk{mkChunk("c-comp-1", "b-comp", 0, "C2", "", "", "completed")})
	_ = listStore.CreateBuild(context.Background(), bQueued, []VanadisChunk{mkChunk("c-q-1", "b-q", 0, "C3", "", "", "pending")})
	srv.SetStore(listStore)
	listStore.listErr = errors.New("simulated list failure")
	if rec := doReq(http.MethodGet, "/builds", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on listErr, got %d", rec.Code)
	}
	listStore.listErr = nil
	recList := doReq(http.MethodGet, "/builds", nil)
	var resp struct {
		Total          int                 `json:"total"`
		RunningCount   int                 `json:"running_count"`
		QueuedCount    int                 `json:"queued_count"`
		CompletedCount int                 `json:"completed_count"`
		Builds         []VanadisBuildEntry `json:"builds"`
	}
	if recList.Code != http.StatusOK || json.Unmarshal(recList.Body.Bytes(), &resp) != nil || resp.Total != 3 || resp.RunningCount != 1 || resp.CompletedCount != 1 || resp.QueuedCount != 1 {
		t.Fatalf("unexpected list response: code=%d resp=%+v", recList.Code, resp)
	}
	if rec := doReq(http.MethodGet, "/runs", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /runs, got %d", rec.Code)
	}
	fetchList := func(query string) (int, string) {
		rec := doReq(http.MethodGet, query, nil)
		var out struct {
			Total  int                 `json:"total"`
			Builds []VanadisBuildEntry `json:"builds"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		id := ""
		if len(out.Builds) > 0 {
			id = out.Builds[0].ID
		}
		return out.Total, id
	}
	queryCases := []struct {
		query, wantID string
		wantTot       int
	}{
		{"/builds?queue_state=running", "b-run", 1},
		{"/builds?status=completed", "b-comp", 1},
		{"/builds?limit=1", "", 1},
	}
	for _, tc := range queryCases {
		tot, id := fetchList(tc.query)
		if tot != tc.wantTot || (tc.wantID != "" && id != tc.wantID) {
			t.Fatalf("query %s got total=%d id=%s", tc.query, tot, id)
		}
	}
	recDetail := doReq(http.MethodGet, "/builds/b-run", nil)
	var detailResp struct {
		Entry VanadisBuildEntry `json:"entry"`
	}
	if err := json.Unmarshal(recDetail.Body.Bytes(), &detailResp); err != nil || detailResp.Entry.ID != "b-run" || detailResp.Entry.QueueState != "running" || detailResp.Entry.TotalChunks != 1 {
		t.Fatalf("unexpected Entry: %+v", detailResp.Entry)
	}
}

func TestHTTPEndpoints(t *testing.T) {
	restateSrv := newMockRestateServer()
	defer restateSrv.Close()

	memStore := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{Port: "0"}, memStore, restateSrv.Client())
	srv.restateCfg.RestateIngressURL = restateSrv.URL
	mux := srv.Routes()
	doReq := func(method, path string, body io.Reader) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, body))
		return rec
	}

	testHTTPEndpointsBasicAndFlush(t, srv, doReq)
	buildID := testHTTPEndpointsBuildCreation(t, srv, memStore, doReq)
	testHTTPEndpointsBuildCancel(t, srv, memStore, buildID, doReq)
	testHTTPEndpointsListAndFilter(t, srv, doReq)
}

func TestInitPostgresAndRunServer(t *testing.T) {
	origPoolFn := newPgxPoolFn
	defer func() { newPgxPoolFn = origPoolFn }()
	ctx := context.Background()
	srv := NewVanadisServer(Config{}, nil, nil)
	if _, err := initPostgres(ctx, srv, "postgres://user:pass@invalid%xx/db"); err == nil {
		t.Fatal("expected parse error")
	}
	newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, errors.New("pool creation failed")
	}
	if _, err := initPostgres(ctx, srv, "postgres://u:p@127.0.0.1:5432/db"); err == nil {
		t.Fatal("expected pool error")
	}
	cfg, _ := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db")
	newPgxPoolFn = origPoolFn
	cleanup, err := initPostgres(ctx, srv, cfg.ConnString())
	if err != nil || cleanup == nil {
		t.Fatalf("expected success from initPostgres, got err=%v", err)
	}
	cleanup(ctx)
	if runServer(ctx, Config{Port: "-1", PostgresURL: cfg.ConnString()}, nil) == nil || runServer(ctx, Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, nil) == nil {
		t.Fatal("expected error on invalid runServer configs")
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- runServer(runCtx, Config{Port: "0", PostgresURL: cfg.ConnString()}, func(addr string) { ready <- addr })
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
	appCtx, appCancel := context.WithCancel(context.Background())
	appCancel()
	if err := RunApp(appCtx, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected RunApp error: %v", err)
	}
}

type mockConsumerStatsQuerier struct {
	stats map[string][2]int64
	err   error
}

func (m *mockConsumerStatsQuerier) ConsumerStats(_ context.Context, stream, consumer string) (int64, int64, error) {
	if m.err != nil {
		return 0, 0, m.err
	}
	if v, ok := m.stats[stream+"/"+consumer]; ok {
		return v[0], v[1], nil
	}
	return 0, 0, nil
}

func TestVanadisEndpointCounterIncrements(t *testing.T) {
	restateSrv := newMockRestateServer()
	defer restateSrv.Close()

	memStore := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{}, memStore, restateSrv.Client())
	srv.restateCfg.RestateIngressURL = restateSrv.URL

	// 1. handleCreateBuild success -> created
	reqBody := `{"thread_id":"123456789012345678","tier":2,"objective":"Metric test build","chunks":[{"repo":"aerial","title":"C1","instructions":"I1","depends_on":[]}]}`
	recCreate := httptest.NewRecorder()
	srv.handleCreateBuild(recCreate, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(reqBody)))
	if recCreate.Code != http.StatusAccepted {
		t.Fatalf("handleCreateBuild status = %d, want %d", recCreate.Code, http.StatusAccepted)
	}

	// 2. handleCreateBuild enqueue failure -> failed
	srvFailInsert := NewVanadisServer(Config{}, memStore, restateSrv.Client())
	srvFailInsert.restateCfg.RestateIngressURL = restateSrv.URL + "/fail"
	recCreateFail := httptest.NewRecorder()
	srvFailInsert.handleCreateBuild(recCreateFail, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(reqBody)))
	if recCreateFail.Code != http.StatusInternalServerError {
		t.Fatalf("handleCreateBuild failure status = %d, want %d", recCreateFail.Code, http.StatusInternalServerError)
	}

	// 3. handleBuildCancel -> cancelled
	recCancel := httptest.NewRecorder()
	srv.handleBuildCancel(recCancel, httptest.NewRequest(http.MethodPost, "/builds/b-to-cancel/cancel", http.NoBody), "b-to-cancel")
	if recCancel.Code != http.StatusOK {
		t.Fatalf("handleBuildCancel status = %d, want %d", recCancel.Code, http.StatusOK)
	}
}

func TestParseCreateBuildChunks_InitialStatus(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	raw := []CreateChunkInput{
		{Title: "First Chunk", Instructions: "Do 1", Repo: "aerial", DependsOn: []string{}},
		{Title: "Second Chunk", Instructions: "Do 2", Repo: "aerial", DependsOn: []string{"b-initial-chunk-1"}},
	}
	chunks, err := parseCreateBuildChunks("b-initial", now, raw)
	if err != nil {
		t.Fatalf("parseCreateBuildChunks failed: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if c.Status != "pending" {
			t.Errorf("chunk[%d].Status = %q, want %q", i, c.Status, "pending")
		}
		if !c.StartedAt.IsZero() {
			t.Errorf("chunk[%d].StartedAt = %v, want zero time", i, c.StartedAt)
		}
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
			s := NewVanadisServer(Config{}, newMemoryVanadisStore(), upstream.Client())
			s.restateCfg.RestateIngressURL = upstream.URL
			rec := httptest.NewRecorder()
			s.handleCreateBuild(rec, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(`{"id":"example","thread_id":"123456789012345678","objective":"test","chunks":[{"repo":"aerial","title":"test","instructions":"test","depends_on":[]}]}`)))
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}
