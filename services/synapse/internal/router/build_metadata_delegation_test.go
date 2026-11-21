package router

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
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
	srv := newTestRouter(cfg, server.Client(), nil)

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
	errSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: errServer.URL}), errServer.Client(), nil)
	if _, err := errSrv.FetchRunLogsArchive(ctx, tRepo, 77, 1); err == nil {
		t.Fatalf("expected error on missing logs")
	}

	// 2. Network error
	badSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: "http://127.0.0.1:0"}), &http.Client{Transport: &zeroCallTransport{}}, nil)
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
	bigSrv := newTestRouter(legacyFixtureConfig(Config{GitHubAPIURL: bigServer.URL}), bigServer.Client(), nil)
	if _, err := bigSrv.FetchRunLogsArchive(ctx, tRepo, 99, 1); err == nil {
		t.Fatalf("expected error on oversize logs")
	}
}
