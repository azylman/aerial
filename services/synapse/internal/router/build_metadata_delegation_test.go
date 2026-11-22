package router

import (
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

	api := metadataAPI{runs: runs(tRun(1, ghStatusCompleted, conclusionSuccess))}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	defer server.Close()
	srv := newTestRouter(Config{GitHubAPIURL: server.URL}, server.Client(), nil)

	var getOut struct {
		ID int64 `json:"id"`
	}
	if err := srv.GetGitHubJSON(context.Background(), "/repos/example/aerial/actions/runs/77", 1<<20, &getOut); err != nil || getOut.ID != 77 {
		t.Fatalf("GetGitHubJSON got (%d, %v)", getOut.ID, err)
	}
}
