package main

import (
	"bytes"
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/httpkit"
)

func assertConfigDefaults(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.Port != "4025" {
		t.Fatalf("unexpected port default: %s", cfg.Port)
	}
	if cfg.PostgresURL != "" {
		t.Fatalf("unexpected PostgresURL default: %s", cfg.PostgresURL)
	}
	if cfg.BrainURL != "" {
		t.Fatalf("unexpected BrainURL default: %s", cfg.BrainURL)
	}
	if cfg.EriURL != "" {
		t.Fatalf("unexpected EriURL default: %s", cfg.EriURL)
	}
	if cfg.NATSURL != "" {
		t.Fatalf("unexpected NATSURL default: %s", cfg.NATSURL)
	}
	if cfg.GitHubAPIURL != "https://api.github.com" {
		t.Fatalf("unexpected GitHubAPIURL default: %s", cfg.GitHubAPIURL)
	}
	if cfg.GitHubToken != "" {
		t.Fatalf("unexpected GitHubToken default: %s", cfg.GitHubToken)
	}
}

func assertExpectedConfig(t *testing.T, cfg Config, wantPort, wantPostgres, wantBrain, wantEri, wantGHURL, wantGHToken string) {
	t.Helper()
	if cfg.Port != wantPort || cfg.PostgresURL != wantPostgres {
		t.Fatalf("unexpected port or postgres_url: got port=%q pg=%q", cfg.Port, cfg.PostgresURL)
	}
	if cfg.BrainURL != wantBrain || cfg.EriURL != wantEri {
		t.Fatalf("unexpected endpoints: got brain=%q eri=%q", cfg.BrainURL, cfg.EriURL)
	}
	if cfg.GitHubAPIURL != wantGHURL || cfg.GitHubToken != wantGHToken {
		t.Fatalf("unexpected github config: got url=%q token=%q", cfg.GitHubAPIURL, cfg.GitHubToken)
	}
}

func assertFileConfig(t *testing.T, cfg Config) {
	t.Helper()
	assertExpectedConfig(t, cfg, "4099", "postgres://u:p@db:5432/vanadis", "http://eri.aerial:8088", "http://eri.aerial:8088", "https://api.github.internal", "yaml-pat")
	if cfg.DeployUIURL != "https://flux.lan.zylman.com" {
		t.Fatalf("unexpected DeployUIURL: %q", cfg.DeployUIURL)
	}
}

func assertEnvConfig(t *testing.T, cfg Config) {
	t.Helper()
	assertExpectedConfig(t, cfg, "4030", "postgres://env:5432/db", "http://env-eri:8088", "http://env-eri:8088", "https://env-api.github.com", "env-token")
}

func TestLoadConfigAndEnv(t *testing.T) {
	origFallback := vanadisFallbackConfigPath
	defer func() {
		vanadisFallbackConfigPath = origFallback
	}()
	for _, k := range []string{
		"PORT", "DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
		"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "ERI_URL", "NATS_URL", "CONFIG_PATH",
		"GITHUB_API_URL", "GITHUB_TOKEN", "GITHUB_PAT",
	} {
		t.Setenv(k, "")
	}
	tmpDir := t.TempDir()
	vanadisFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	tests := []struct {
		name, wantHost, password string
	}{
		{"UnconfiguredDefaults", "", ""},
		{"Decomposed_PostgresPassword", "@postgres:5432/", "custom_vanadis_pass"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.password != "" {
				t.Setenv("POSTGRES_PASSWORD", tc.password)
			}
			cfg := LoadConfig("")
			if tc.wantHost != "" && !strings.Contains(cfg.PostgresURL, tc.wantHost) {
				t.Fatalf("expected postgres URL to contain %s, got %s", tc.wantHost, cfg.PostgresURL)
			}
			if tc.password != "" && !strings.Contains(cfg.PostgresURL, ":"+tc.password+"@") {
				t.Fatalf("expected postgres URL to contain password %s, got %s", tc.password, cfg.PostgresURL)
			}
			if tc.name == "UnconfiguredDefaults" {
				assertConfigDefaults(t, cfg)
			}
		})
	}
	yamlPath := filepath.Join(tmpDir, "vanadis.yaml")
	_ = os.WriteFile(yamlPath, []byte("port: \"4099\"\npostgres_url: \"postgres://u:p@db:5432/vanadis\"\neri_url: \"http://eri.aerial:8088/\"\ngithub_api_url: \"https://api.github.internal/\"\ngithub_token: \"yaml-pat\"\ndeploy_ui_url: \"https://flux.lan.zylman.com/\"\n"), 0o600)
	vanadisFallbackConfigPath = yamlPath
	assertFileConfig(t, LoadConfig(filepath.Join(tmpDir, "missing.yaml")))

	badPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badPath, []byte(":::invalid_yaml"), 0o600)
	t.Setenv("CONFIG_PATH", badPath)
	t.Setenv("PORT", "4030")
	t.Setenv("DATABASE_URL", "postgres://env:5432/db")
	t.Setenv("ERI_URL", "http://env-eri:8088/")
	t.Setenv("GITHUB_API_URL", "https://env-api.github.com/")
	t.Setenv("GITHUB_TOKEN", "env-token")
	assertEnvConfig(t, LoadConfigFromEnv())

	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITHUB_PAT", "pat-token-fallback")
	if patCfg := LoadConfigFromEnv(); patCfg.GitHubToken != "pat-token-fallback" {
		t.Fatalf("expected GITHUB_PAT fallback, got: %s", patCfg.GitHubToken)
	}

	t.Setenv("NATS_URL", "nats://env-nats:4222")
	if natsCfg := LoadConfigFromEnv(); natsCfg.NATSURL != "nats://env-nats:4222" {
		t.Fatalf("expected NATS_URL override, got: %s", natsCfg.NATSURL)
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

func testPostgresVanadisStoreGetBuild(ctx context.Context, t *testing.T, store *PostgresVanadisStore, validChunks []VanadisChunk, explicitStart, explicitComplete time.Time) {
	t.Helper()
	if _, _, err := store.GetBuild(ctx, "   "); err == nil {
		t.Fatal("expected empty build id error")
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: pgx.ErrNoRows} }}).GetBuild(ctx, "b-404"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: errors.New("err")} }}).GetBuild(ctx, "b-err"); err == nil {
		t.Fatal("expected queryRow error")
	}
	popBuild := &VanadisBuild{ID: "b-1", ThreadID: "thread-1", Tier: 2, Title: "Build Title", Objective: "Objective", SharedContext: "Shared Context", Status: "running", StatusMessageID: "msg-1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	for _, r := range []*mockRows{nil, {items: validChunks, scanErr: errors.New("err")}, {items: validChunks, rowsErr: errors.New("err")}} {
		if _, _, err := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				if r == nil {
					return nil, errors.New("err")
				}
				return r, nil
			},
		}).GetBuild(ctx, "b-1"); err == nil {
			t.Fatal("expected GetBuild error")
		}
	}
	cGet0 := mkChunk("c-1", "b-1", 0, "T1", "I1", "", "")
	cGet0.StartedAt = explicitStart
	cGet0.CompletedAt = explicitComplete
	okGetStore := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{cGet0, mkChunk("c-2", "b-1", 1, "T2", "I2", "low", "")}}, nil
		},
	})
	gotB, gotC, err := okGetStore.GetBuild(ctx, "b-1")
	if err != nil || gotB.ID != "b-1" || gotB.Title != "Build Title" || len(gotC) != 2 || gotC[0].Effort != "medium" || gotC[1].Effort != "low" || !gotC[0].StartedAt.Equal(explicitStart) || !gotC[0].CompletedAt.Equal(explicitComplete) || !gotC[1].StartedAt.IsZero() || !gotC[1].CompletedAt.IsZero() {
		t.Fatalf("unexpected GetBuild result: b=%+v c=%+v err=%v", gotB, gotC, err)
	}
}

func testPostgresVanadisStoreListBuilds(ctx context.Context, t *testing.T) {
	t.Helper()
	failQueries := []struct {
		buildRows *mockRows
		buildErr  error
		chunkRows *mockRows
		chunkErr  error
	}{
		{nil, errors.New("err"), nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, scanErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, rowsErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, nil, errors.New("err")},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, scanErr: errors.New("err")}, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, rowsErr: errors.New("err")}, nil},
	}
	for _, tc := range failQueries {
		s := newMockStore(mockDB{queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				return tc.buildRows, tc.buildErr
			}
			return tc.chunkRows, tc.chunkErr
		}})
		if _, err := s.ListBuilds(ctx, 10); err == nil {
			t.Fatal("expected ListBuilds error")
		}
	}
	entries, err := newMockStore(mockDB{queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{}, nil }}).ListBuilds(ctx, 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected 0 entries on 0 builds, got %d, err=%v", len(entries), err)
	}
	now := time.Now().UTC()
	b1, b2 := mkBuild("b-1", "100", "", "", "running"), mkBuild("b-2", "101", "", "", "completed")
	b1.CreatedAt, b1.UpdatedAt, b2.CreatedAt, b2.UpdatedAt = now.Add(-time.Hour), now, now.Add(-2*time.Hour), now
	c1, c2, c3 := mkChunk("c-1", "b-1", 0, "chunk 1", "", "", "running"), mkChunk("c-2", "b-1", 1, "chunk 2", "", "", "pending"), mkChunk("c-3", "b-2", 0, "chunk 3", "", "", "merged")
	c1.CreatedAt, c1.UpdatedAt, c2.CreatedAt, c2.UpdatedAt, c3.CreatedAt, c3.UpdatedAt = now, now, now, now, now, now
	var capturedLimit any
	okStore := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				if len(args) > 0 {
					capturedLimit = args[0]
				}
				return &mockRows{builds: []VanadisBuild{b1, b2}}, nil
			}
			return &mockRows{items: []VanadisChunk{c1, c2, c3}}, nil
		},
	})
	gotEntries, err := okStore.ListBuilds(ctx, 25)
	if err != nil || capturedLimit != 25 || len(gotEntries) != 2 || gotEntries[0].ID != "b-1" || gotEntries[0].TotalChunks != 2 || gotEntries[0].QueueState != "running" || gotEntries[1].ID != "b-2" || gotEntries[1].QueueState != "completed" {
		t.Fatalf("unexpected ListBuilds result: entries=%+v limit=%v err=%v", gotEntries, capturedLimit, err)
	}
}

func TestPostgresVanadisStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *PostgresVanadisStore
	_, _, getErr := nilStore.GetBuild(ctx, "b1")
	_, listErr := nilStore.ListBuilds(ctx, 10)
	_, emptyDbListErr := (&PostgresVanadisStore{}).ListBuilds(ctx, 10)
	if getErr == nil || listErr == nil || emptyDbListErr == nil {
		t.Fatal("expected error on nil store operations")
	}
	cValid := mkChunk("c-1", "", 0, "T1", "I1", "medium", "")
	cValid.TargetFiles, cValid.TargetSymbols, cValid.AcceptanceCriteria, cValid.PRNumber = []string{"main.go"}, []string{"Foo"}, []string{"Must pass"}, 42
	validChunks := []VanadisChunk{cValid}
	store := newMockStore(mockDB{})

	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	explicitComplete := time.Date(2026, 10, 16, 18, 15, 0, 0, time.UTC)

	testPostgresVanadisStoreGetBuild(ctx, t, store, validChunks, explicitStart, explicitComplete)
	testPostgresVanadisStoreListBuilds(ctx, t)
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

func TestClassifyAndSummarizeBuildEntry(t *testing.T) {
	t.Parallel()
	classifyTests := []struct {
		name        string
		buildStatus string
		chunks      []VanadisChunk
		expected    string
	}{
		{"completed status", "completed", nil, "completed"},
		{"merged status", "merged", nil, "completed"},
		{"failed status", "failed", nil, "completed"},
		{"cancelled status", "cancelled", nil, "completed"},
		{"canceled status uppercase", "CANCELED", nil, "completed"},
		{"queued status", "queued", nil, "queued"},
		{"pending status", "pending", nil, "queued"},
		{"running with empty chunks", "running", []VanadisChunk{}, "running"},
		{"running with all pending chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "pending")}, "queued"},
		{"running with mixed chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "running")}, "running"},
		{"running with completed chunk", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "completed"), mkChunk("c2", "", 1, "", "", "", "pending")}, "running"},
	}
	for _, tc := range classifyTests {
		t.Run("classify_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyBuildQueueState(tc.buildStatus, tc.chunks); got != tc.expected {
				t.Errorf("ClassifyBuildQueueState(%q, chunks) = %q, want %q", tc.buildStatus, got, tc.expected)
			}
		})
	}
	b := mkBuild("b-sum", "100", "Title", "Obj", "running")
	b.CreatedAt = time.Unix(1760000000, 0)
	c1 := mkChunk("c-sum-1", "b-sum", 0, "Chunk 1", "inst", "low", "running")
	c1.StartedAt = time.Unix(1760000100, 0)
	c2 := mkChunk("c-sum-2", "b-sum", 1, "Chunk 2", "inst", "high", "pending")
	c2.StartedAt = time.Unix(1760000200, 0)
	c2.CompletedAt = time.Unix(1760000300, 0)
	entry := SummarizeBuildEntry(b, []VanadisChunk{c1, c2})
	if entry.ID != "b-sum" || entry.TotalChunks != 2 || entry.QueueState != "running" || len(entry.Chunks) != 2 || !entry.Chunks[0].StartedAt.Equal(c1.StartedAt) || !entry.Chunks[1].CompletedAt.Equal(c2.CompletedAt) || entry.Chunks[0].Effort != "low" || entry.Chunks[1].Effort != "high" {
		t.Fatalf("unexpected SummarizeBuildEntry result: %+v", entry)
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

type vanadisMetricsCase struct {
	name           string
	server         *VanadisServer
	method         string
	url            string
	wantStatus     int
	wantSubstrings []string
}

func makeVanadisMetricsCases(srvWithQueue, srvMem, srvPGErr *VanadisServer) []vanadisMetricsCase {
	var nilServer *VanadisServer
	nilStoreServer := &VanadisServer{}

	return []vanadisMetricsCase{
		{
			name:       "valid postgres server with active builds and jetstream queue metrics",
			server:     srvWithQueue,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_active",
				`vanadis_builds_active{queue_state="running"} 1`,
				`vanadis_builds_active{queue_state="queued"} 1`,
				"vanadis_chunks_active",
				`vanadis_chunks_active{status="pending"} 3`,
				`vanadis_chunks_active{status="running"} 1`,
				`vanadis_chunks_active{status="pr_opened"} 1`,
				"vanadis_queue_depth",
				`vanadis_queue_depth{state="pending"} 2`,
				`vanadis_queue_depth{state="in_flight"} 1`,
				"vanadis_builds_total",
				`vanadis_builds_total{status="created"}`,
				`vanadis_builds_total{status="completed"}`,
				`vanadis_builds_total{status="failed"}`,
				`vanadis_builds_total{status="cancelled"}`,
				"vanadis_chunks_total",
				`vanadis_chunks_total{status="dispatched"}`,
				`vanadis_chunks_total{status="merged"}`,
				`vanadis_chunks_total{status="completed"}`,
				`vanadis_chunks_total{status="failed"}`,
				`vanadis_chunks_total{status="cancelled"}`,
			},
		},
		{
			name:       "memory store server populates build metrics",
			server:     srvMem,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_active",
				"vanadis_chunks_active",
				"vanadis_queue_depth",
			},
		},
		{
			name:       "nil server serves metrics ok",
			server:     nilServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
		{
			name:       "server with nil store serves metrics ok",
			server:     nilStoreServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
		{
			name:       "server with db error in queries still serves metrics ok",
			server:     srvPGErr,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
	}
}

func runVanadisMetricsCase(t *testing.T, tc vanadisMetricsCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(tc.method, tc.url, http.NoBody)
	tc.server.handleMetrics(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("handleMetrics() status = %d, want %d", rec.Code, tc.wantStatus)
	}
	body := rec.Body.String()
	for _, sub := range tc.wantSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected body to contain %q, body:\n%s", sub, body)
		}
	}
}

func TestVanadisMetrics(t *testing.T) {
	RecordBuildTotal("created")
	RecordBuildTotal("completed")
	RecordBuildTotal("failed")
	RecordBuildTotal("cancelled")

	registerVanadisCollector(BuildsActive)

	b1 := mkBuild("b-run-1", "101", "Running Build", "obj", "running")
	b2 := mkBuild("b-queue-1", "102", "Queued Build", "obj", "running")
	b3 := mkBuild("b-comp-1", "103", "Completed Build", "obj", "completed")

	c1 := mkChunk("c-1", "b-run-1", 0, "Chunk 1", "inst", "medium", "running")
	c2 := mkChunk("c-2", "b-run-1", 1, "Chunk 2", "inst", "medium", "pending")
	c3 := mkChunk("c-3", "b-run-1", 2, "Chunk 3", "inst", "medium", "pr_opened")
	c4 := mkChunk("c-4", "b-queue-1", 0, "Chunk 4", "inst", "medium", "pending")
	c5 := mkChunk("c-5", "b-queue-1", 1, "Chunk 5", "inst", "medium", "pending")
	c6 := mkChunk("c-6", "b-comp-1", 0, "Chunk 6", "inst", "medium", "merged")

	pgStoreWithQueue := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{b1, b2, b3}}, nil
			}
			return &mockRows{items: []VanadisChunk{c1, c2, c3, c4, c5, c6}}, nil
		},
	})
	srvWithQueue := NewVanadisServer(Config{}, pgStoreWithQueue, nil)
	srvWithQueue.queueStats = &mockConsumerStatsQuerier{
		stats: map[string][2]int64{
			"AERIAL_JOBS/vanadis-chunk": {2, 1},
		},
	}

	memStore := newMemoryVanadisStore()
	_ = memStore.CreateBuild(context.Background(), b1, []VanadisChunk{c1, c2, c3})
	srvMem := NewVanadisServer(Config{}, memStore, nil)

	pgStoreErr := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			return nil, errors.New("db outage")
		},
	})
	srvPGErr := NewVanadisServer(Config{}, pgStoreErr, nil)
	srvPGErr.queueStats = &mockConsumerStatsQuerier{err: errors.New("simulated stats error")}

	tests := makeVanadisMetricsCases(srvWithQueue, srvMem, srvPGErr)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runVanadisMetricsCase(t, tc)
		})
	}
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
