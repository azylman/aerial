package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

func TestBuildMetadataDelegates(t *testing.T) {
	t.Parallel()

	logBuildMetadataWarning(context.Background(), "test warning", tRepo, 42, errors.New("sample error"))
}

func TestBuildMetadataRouterServerDelegations(t *testing.T) {
	t.Parallel()

	run := tRun(1, ghStatusCompleted, conclusionSuccess)
	api := metadataAPI{
		runs: runs(run),
		logs: tLegacyLogs(tHead),
	}

	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()

	cfg := legacyFixtureConfig(Config{GitHubAPIURL: server.URL})
	srv := newTestRouter(cfg, server.Client(), nil, nil)

	ctx := context.Background()

	var getOut struct {
		ID int64 `json:"id"`
	}
	if err := srv.GetGitHubJSON(ctx, "/repos/example/aerial/actions/runs/77", 1<<20, &getOut); err != nil {
		t.Fatalf("GetGitHubJSON error: %v", err)
	}

	archiveBytes, err := srv.FetchRunLogsArchive(ctx, tRepo, 77, 1)
	if err != nil || len(archiveBytes) == 0 {
		t.Fatalf("FetchRunLogsArchive got (%v, %v)", len(archiveBytes), err)
	}

	// 1. Missing run / 404
	errAPI := metadataAPI{
		logsStatus: http.StatusNotFound,
	}
	errServer := httptest.NewServer(http.HandlerFunc(errAPI.serve))
	defer errServer.Close()
	errSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: errServer.URL}), errServer.Client(), nil, nil)
	if _, err := errSrv.FetchRunLogsArchive(ctx, tRepo, 77, 1); err == nil {
		t.Fatalf("expected error on missing logs")
	}

	// 2. Network error
	badSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: "http://127.0.0.1:0"}), &http.Client{Transport: &zeroCallTransport{}}, nil, nil)
	if _, err := badSrv.FetchRunLogsArchive(ctx, tRepo, 77, 1); err == nil {
		t.Fatalf("expected error on bad client")
	}

	// 3. Oversize logs
	bigAPI := metadataAPI{
		runs: runs(tRun(99, ghStatusCompleted, conclusionSuccess)),
		logs: bytes.Repeat([]byte("a"), 16<<20+2),
	}
	bigServer := httptest.NewServer(http.HandlerFunc(bigAPI.serve))
	defer bigServer.Close()
	bigSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: bigServer.URL}), bigServer.Client(), nil, nil)
	if _, err := bigSrv.FetchRunLogsArchive(ctx, tRepo, 99, 1); err == nil {
		t.Fatalf("expected error on oversize logs")
	}
}

func TestBuildMetadataRegistryAdapter(t *testing.T) {
	t.Parallel()

	// 1. Nil inner tests
	nilAdapter := buildMetadataRegistryAdapter{inner: nil}
	ctx := context.Background()

	lc, err := nilAdapter.LoadPRBuildLifecycle(ctx, "repo", 1)
	if err != nil || lc != (model.PRBuildLifecycle{}) {
		t.Fatalf("nilAdapter LoadPRBuildLifecycle got (%+v, %v)", lc, err)
	}

	recOk, err := nilAdapter.RecordPRBuildMetadata(ctx, "repo", 1, model.BuildMetadata{})
	if err != nil || recOk {
		t.Fatalf("nilAdapter RecordPRBuildMetadata got (%v, %v)", recOk, err)
	}

	loadMeta, found, err := nilAdapter.LoadPRBuildMetadata(ctx, "repo", 1)
	if err != nil || found || loadMeta.Kind != "" {
		t.Fatalf("nilAdapter LoadPRBuildMetadata got (%+v, %v, %v)", loadMeta, found, err)
	}

	// 2. Error inner tests
	errReg := &mockPRRegistry{
		buildLifecycleErr:      errors.New("db error"),
		recordBuildMetadataErr: errors.New("db error"),
	}
	errAdapter := buildMetadataRegistryAdapter{inner: errReg}

	if _, loadErr := errAdapter.LoadPRBuildLifecycle(ctx, "repo", 1); loadErr == nil {
		t.Fatalf("expected error from LoadPRBuildLifecycle")
	}

	// 3. Successful pass-through
	succReg := &mockPRRegistry{
		buildLifecycle:     PRBuildLifecycle{Status: "open", HeadSHA: "head1", MergeSHA: "merge1", Started: true},
		buildMetadata:      BuildMetadata{Kind: buildMetadataFinal},
		buildMetadataFound: true,
	}
	succAdapter := buildMetadataRegistryAdapter{inner: succReg}

	lc, err = succAdapter.LoadPRBuildLifecycle(ctx, "repo", 1)
	if err != nil || lc.Status != "open" || lc.HeadSHA != "head1" || lc.MergeSHA != "merge1" || !lc.Started {
		t.Fatalf("succAdapter LoadPRBuildLifecycle got (%+v, %v)", lc, err)
	}

	recOk, err = succAdapter.RecordPRBuildMetadata(ctx, "repo", 1, model.BuildMetadata{Kind: buildMetadataFinal})
	if err != nil || !recOk {
		t.Fatalf("succAdapter RecordPRBuildMetadata got (%v, %v)", recOk, err)
	}

	loadMeta, found, err = succAdapter.LoadPRBuildMetadata(ctx, "repo", 1)
	if err != nil || !found || loadMeta.Kind != buildMetadataFinal {
		t.Fatalf("succAdapter LoadPRBuildMetadata got (%+v, %v, %v)", loadMeta, found, err)
	}
}

func TestBuildPRRolloutStateStructuredCards(t *testing.T) {
	t.Parallel()
	m := tMeta(2, "engine", "worker", "web", "api", "cron")
	progress := m
	progress.Kind = buildMetadataProgress
	progress.Results = []BuildResult{
		tImageResult("engine", buildResultRunning, false, 2),
		tImageResult("worker", conclusionSuccess, true, 2),
		tImageResult("web", conclusionSuccess, false, 2),
		tImageResult("api", buildResultCancelled, false, 2),
	}
	tests := []struct {
		name         string
		meta         map[string]any
		wantExpected []string
		wantStates   map[string]string
	}{
		{"no snapshot", map[string]any{"jobs": []any{"git-sync"}}, []string{"git-sync"}, nil},
		{"manifest pending", map[string]any{"build_metadata": tMeta(1, "engine")}, []string{"engine"}, map[string]string{"engine": "image_pending"}},
		{"explicit empty", map[string]any{"build_metadata": tFinalOf(tMeta(1))}, nil, nil},
		{"progress states", map[string]any{"build_metadata": progress}, []string{"engine", "worker", "web", "api", "cron"}, map[string]string{"engine": "image_building", "worker": "image_built", "web": "image_unpublished", "api": "image_failed", "cron": "image_pending"}},
		{"deployment state wins", map[string]any{"jobs": []any{"engine"}, "started_jobs": []any{"engine"}, "job_states": map[string]any{"engine": "verifying_health"}, "build_metadata": tMeta(1, "engine")}, []string{"engine"}, map[string]string{"engine": "verifying_health"}},
		{"raw registry JSON revalidated", map[string]any{"build_metadata": map[string]any{"kind": "final"}}, nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildPRRolloutState(tc.meta)
			var expected []string
			var states map[string]string
			if got != nil {
				expected, states = got.ExpectedJobs, got.JobStates
			}
			if !slicesEqual(expected, tc.wantExpected) || (len(tc.wantStates) > 0 && !mapsEqual(states, tc.wantStates)) || (len(tc.wantStates) == 0 && len(states) > 0) {
				t.Fatalf("expected=%v states=%v, want %v %v", expected, states, tc.wantExpected, tc.wantStates)
			}
		})
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestConfigFromEnvDelegates(t *testing.T) {
	_ = LoadConfigFromEnv()
	_, _ = loadConfigFromEnvStrict()

	var nilReg *PostgresPRRegistry
	ctx := context.Background()
	reqs, err := nilReg.RetryPRReadiness(ctx, "", "", 0)
	if err != nil || len(reqs) != 0 {
		t.Fatalf("nilReg RetryPRReadiness got (%v, %v)", reqs, err)
	}

	p := NewPostgresPRRegistry(nil)
	reqs, err = p.RetryPRReadiness(ctx, "repo", "merge", 1)
	if err != nil || len(reqs) != 0 {
		t.Fatalf("p RetryPRReadiness got (%v, %v)", reqs, err)
	}

	// Success case with mock pool
	r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
		return mockSynapsePgxRow{scanFn: func(dest ...any) error {
			return scanValues(dest, []byte(`{"config":{"namespace":"flux-system","requested_at":"2026-01-01T00:00:00Z"}}`))
		}}
	}})
	reqs, err = r.RetryPRReadiness(ctx, "example/aerial", "head1", 42)
	if err != nil || len(reqs) != 1 || reqs["config"].Namespace != "flux-system" {
		t.Fatalf("RetryPRReadiness got (%v, %v)", reqs, err)
	}

	// Database error case
	errR := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
		return mockSynapsePgxRow{scanFn: func(dest ...any) error {
			return errors.New("query failure")
		}}
	}})
	if _, retryErr := errR.RetryPRReadiness(ctx, "example/aerial", "head1", 42); retryErr == nil {
		t.Fatalf("expected error from RetryPRReadiness")
	}

	// Not found (ErrNoRows)
	noRowsR := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
		return mockSynapsePgxRow{scanFn: func(dest ...any) error {
			return pgx.ErrNoRows
		}}
	}})
	reqs, err = noRowsR.RetryPRReadiness(ctx, "example/aerial", "head1", 42)
	if err != nil || len(reqs) != 0 {
		t.Fatalf("expected empty from RetryPRReadiness on no rows, got (%v, %v)", reqs, err)
	}
}
