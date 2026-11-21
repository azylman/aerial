package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// githubBodyServer answers every GitHub API request with code and body and
// counts the requests.
func githubBodyServer(t *testing.T, code int, body string, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestResolveAutoMergeDisabledPR(t *testing.T) {
	t.Parallel()
	known := true
	tests := []struct {
		name       string
		res        ResolvedGitHubEvent
		pr         GitHubPullRequest
		ghCode     int
		ghBody     string
		wantOK     bool
		wantErr    bool
		wantState  string
		wantBranch string
		wantCalls  int64
	}{
		{name: "known mergeability passes through without a fetch", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7}, pr: GitHubPullRequest{Number: 7, Mergeable: &known, MergeableState: "clean"}, wantOK: true, wantState: "clean"},
		{name: "pr-less event passes through without a fetch", res: ResolvedGitHubEvent{Repo: "azylman/aerial"}, pr: GitHubPullRequest{}, wantOK: true},
		{
			name: "unknown mergeability is fetched and filled from the event", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1"}, pr: GitHubPullRequest{Number: 7},
			ghCode: http.StatusOK, ghBody: `{"number":7,"state":"open","mergeable_state":"dirty","head":{"sha":"h1"}}`, wantOK: true, wantState: "dirty", wantBranch: "feat", wantCalls: 1,
		},
		{name: "merged pr is skipped", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7}, ghCode: http.StatusOK, ghBody: `{"number":7,"merged":true,"mergeable_state":"clean"}`, wantCalls: 1},
		{name: "closed pr is skipped", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7}, ghCode: http.StatusOK, ghBody: `{"number":7,"state":"closed","mergeable_state":"clean"}`, wantCalls: 1},
		{name: "moved head is skipped", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "h1"}, ghCode: http.StatusOK, ghBody: `{"number":7,"state":"open","mergeable_state":"dirty","head":{"sha":"h2"}}`, wantCalls: 1},
		{name: "fetch failure surfaces", res: ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7}, ghCode: http.StatusInternalServerError, ghBody: `{}`, wantErr: true, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			gh := githubBodyServer(t, tc.ghCode, tc.ghBody, &calls)
			srv := newTestRouter(Config{GitHubAPIURL: gh.URL}, gh.Client(), nil)
			res := tc.res
			got, ok, err := srv.resolveAutoMergeDisabledPR(context.Background(), &res, tc.pr)
			if (err != nil) != tc.wantErr || ok != tc.wantOK {
				t.Fatalf("resolveAutoMergeDisabledPR() = ok %v err %v, want ok %v wantErr %v", ok, err, tc.wantOK, tc.wantErr)
			}
			if ok && (got.MergeableState != tc.wantState || got.Head.Ref != tc.wantBranch) {
				t.Errorf("pr = state %q branch %q, want %q %q", got.MergeableState, got.Head.Ref, tc.wantState, tc.wantBranch)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Errorf("github calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestFetchLatestRoboRevComment(t *testing.T) {
	t.Parallel()
	const head = "abcdef1234567890"
	tests := []struct {
		name     string
		prNumber int
		ghCode   int
		ghBody   string
		want     string
	}{
		{name: "latest matching roborev comment wins", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev: old ` + "`abcdef1" + `"},{"body":"unrelated"},{"body":"## roborev: newest ` + "`abcdef1" + `"}]`, want: "## roborev: newest `abcdef1"},
		{name: "override comment counts", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev-override for ` + head + `"}]`, want: "## roborev-override for " + head},
		{name: "comment for another head is ignored", prNumber: 7, ghCode: http.StatusOK, ghBody: `[{"body":"## roborev: ` + "`9999999" + `"}]`},
		{name: "fetch failure yields nothing", prNumber: 7, ghCode: http.StatusInternalServerError, ghBody: `{}`},
		{name: "pr-less lookup yields nothing", prNumber: 0, ghCode: http.StatusOK, ghBody: `[]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			gh := githubBodyServer(t, tc.ghCode, tc.ghBody, &calls)
			srv := newTestRouter(Config{GitHubAPIURL: gh.URL}, gh.Client(), nil)
			if got := srv.fetchLatestRoboRevComment(context.Background(), "azylman/aerial", tc.prNumber, head); strings.TrimSpace(got) != tc.want {
				t.Errorf("fetchLatestRoboRevComment() = %q, want %q", got, tc.want)
			}
		})
	}
}
