package prstate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// apiResponse is one canned GitHub response.
type apiResponse struct {
	status    int
	body      string
	remaining string
}

// apiRequest is one request the fake GitHub API saw.
type apiRequest struct {
	method string
	path   string
	auth   string
}

// fakeGitHubAPI serves canned responses per "METHOD path?query", in order;
// the last response repeats. Unknown routes return 404.
type fakeGitHubAPI struct {
	mu        sync.Mutex
	responses map[string][]apiResponse
	requests  []apiRequest
}

func (f *fakeGitHubAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	route := r.Method + " " + r.URL.RequestURI()
	f.requests = append(f.requests, apiRequest{method: r.Method, path: r.URL.RequestURI(), auth: r.Header.Get("Authorization")})
	queue := f.responses[route]
	if len(queue) == 0 {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
		return
	}
	resp := queue[0]
	if len(queue) > 1 {
		f.responses[route] = queue[1:]
	}
	if resp.remaining != "" {
		w.Header().Set("X-Ratelimit-Remaining", resp.remaining)
	}
	w.WriteHeader(resp.status)
	_, _ = w.Write([]byte(resp.body))
}

func (f *fakeGitHubAPI) Requests() []apiRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]apiRequest(nil), f.requests...)
}

// newFakeGitHub starts a fake API with routes and returns a client on it.
func newFakeGitHub(t *testing.T, routes map[string][]apiResponse) (*GitHubClient, *fakeGitHubAPI) {
	t.Helper()
	api := &fakeGitHubAPI{responses: routes}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return NewGitHubClient(srv.Client(), srv.URL+"/", "tok"), api
}

const (
	updateBranchRoute = "PUT /repos/azylman/aerial/pulls/7/update-branch"
	openPRsRoute      = "GET /repos/azylman/aerial/pulls?state=open&base=main&sort=created&direction=asc&per_page=100"
	pr7Route          = "GET /repos/azylman/aerial/pulls/7"
	checksRoute       = "GET /repos/azylman/aerial/commits/h1/check-runs?per_page=100"
	rerunRoute        = "POST /repos/azylman/aerial/actions/runs/55/rerun-failed-jobs"
)

func TestNewGitHubClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		apiURL    string
		token     string
		wantURL   string
		wantToken string
	}{
		{name: "empty url targets api.github.com", apiURL: " ", token: " t1 ", wantURL: "https://api.github.com", wantToken: "t1"},
		{name: "trailing slash trimmed", apiURL: "http://gh.local/", wantURL: "http://gh.local"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewGitHubClient(nil, tc.apiURL, tc.token)
			require.Equal(t, tc.wantURL, c.apiURL)
			require.Equal(t, tc.wantToken, c.token)
			require.NotNil(t, c.http)
		})
	}
}

func TestGitHubErrorClassification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		err           *GitHubError
		wantRetryable bool
		wantNotFound  bool
		wantMessage   string
	}{
		{name: "nil error", err: nil},
		{name: "transport failure retries", err: &GitHubError{Message: "dial"}, wantRetryable: true, wantMessage: "dial"},
		{name: "rate limit retries", err: &GitHubError{Status: 403, RateLimited: true, Message: "rl"}, wantRetryable: true, wantMessage: "rl"},
		{name: "server error retries", err: &GitHubError{Status: 502, Message: "bad gateway"}, wantRetryable: true, wantMessage: "bad gateway"},
		{name: "not found is terminal", err: &GitHubError{Status: 404, Message: "nf"}, wantNotFound: true, wantMessage: "nf"},
		{name: "validation is terminal", err: &GitHubError{Status: 422, Message: "invalid"}, wantMessage: "invalid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantRetryable, tc.err.Retryable())
			require.Equal(t, tc.wantNotFound, tc.err.NotFound())
			if tc.err != nil {
				require.Equal(t, tc.wantMessage, tc.err.Error())
			}
		})
	}
}

func TestClassifyUpdateBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      int
		body        string
		want        UpdateBranchOutcome
		wantErrCode int
	}{
		{name: "accepted updates", status: 202, want: UpdateBranchUpdated},
		{name: "ok updates", status: 200, want: UpdateBranchUpdated},
		{name: "409 conflicts", status: 409, want: UpdateBranchConflict},
		{name: "422 merge conflict", status: 422, body: `{"message":"merge Conflict"}`, want: UpdateBranchConflict},
		{name: "422 no new commits is up to date", status: 422, body: `{"message":"There are no new commits on the base branch."}`, want: UpdateBranchUpToDate},
		{name: "head ref missing", status: 422, body: `{"message":"Head ref does not exist"}`, want: UpdateBranchHeadMissing},
		{name: "other 422 is a terminal error", status: 422, body: `{"message":"expected_head_sha mismatch"}`, wantErrCode: 422},
		{name: "server error", status: 500, body: "boom", wantErrCode: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, gerr := classifyUpdateBranch(tc.status, tc.body)
			require.Equal(t, tc.want, got)
			if tc.wantErrCode == 0 {
				require.Nil(t, gerr)
				return
			}
			require.Equal(t, tc.wantErrCode, gerr.Status)
		})
	}
}

func TestGitHubClientUpdateBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		repo            string
		prNumber        int
		routes          map[string][]apiResponse
		want            UpdateBranchOutcome
		wantErrStatus   int
		wantRateLimited bool
		wantRequests    int
	}{
		{name: "accepted", repo: "aerial", prNumber: 7, routes: map[string][]apiResponse{updateBranchRoute: {{status: 202}}}, want: UpdateBranchUpdated, wantRequests: 1},
		{name: "conflict", repo: "aerial", prNumber: 7, routes: map[string][]apiResponse{updateBranchRoute: {{status: 409}}}, want: UpdateBranchConflict, wantRequests: 1},
		{name: "rate limited", repo: "aerial", prNumber: 7, routes: map[string][]apiResponse{updateBranchRoute: {{status: 403, remaining: "0"}}}, wantErrStatus: 403, wantRateLimited: true, wantRequests: 1},
		{name: "too many requests", repo: "aerial", prNumber: 7, routes: map[string][]apiResponse{updateBranchRoute: {{status: 429}}}, wantErrStatus: 429, wantRateLimited: true, wantRequests: 1},
		{name: "invalid input never calls github", repo: "", prNumber: 7, wantErrStatus: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, api := newFakeGitHub(t, tc.routes)
			got, gerr := c.UpdateBranch(context.Background(), tc.repo, tc.prNumber)
			require.Equal(t, tc.want, got)
			require.Len(t, api.Requests(), tc.wantRequests)
			if tc.wantErrStatus == 0 {
				require.Nil(t, gerr)
				require.Equal(t, "token tok", api.Requests()[0].auth)
				return
			}
			require.Equal(t, tc.wantErrStatus, gerr.Status)
			require.Equal(t, tc.wantRateLimited, gerr.RateLimited)
		})
	}
}

func TestGitHubClientListOpenPRs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		repo          string
		routes        map[string][]apiResponse
		want          []GitHubPR
		wantErrStatus int
	}{
		{
			name: "lists open prs", repo: "azylman/aerial",
			routes: map[string][]apiResponse{openPRsRoute: {{status: 200, body: `[{"number":7,"draft":true,"user":{"login":"dependabot[bot]"},"head":{"ref":"b7","sha":"h7"}}]`}}},
			want:   []GitHubPR{{Number: 7, Draft: true, User: GitHubUser{Login: "dependabot[bot]"}, Head: GitHubPRRef{Ref: "b7", SHA: "h7"}}},
		},
		{name: "server error", repo: "aerial", routes: map[string][]apiResponse{openPRsRoute: {{status: 503}}}, wantErrStatus: 503},
		{name: "malformed body", repo: "aerial", routes: map[string][]apiResponse{openPRsRoute: {{status: 200, body: `{`}}}, wantErrStatus: 200},
		{name: "invalid repo", repo: " ", wantErrStatus: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := newFakeGitHub(t, tc.routes)
			got, gerr := c.ListOpenPRs(context.Background(), tc.repo)
			require.Equal(t, tc.want, got)
			if tc.wantErrStatus == 0 {
				require.Nil(t, gerr)
				return
			}
			require.Equal(t, tc.wantErrStatus, gerr.Status)
		})
	}
}

func TestGitHubClientFetchPRMergeability(t *testing.T) {
	t.Parallel()
	conflicted := false
	tests := []struct {
		name          string
		prNumber      int
		routes        map[string][]apiResponse
		want          GitHubPR
		wantErrStatus int
		wantRequests  int
	}{
		{
			name: "known mergeability fetches once", prNumber: 7,
			routes:       map[string][]apiResponse{pr7Route: {{status: 200, body: `{"number":7,"mergeable":false,"mergeable_state":"dirty"}`}}},
			want:         GitHubPR{Number: 7, Mergeable: &conflicted, MergeableState: "dirty"},
			wantRequests: 1,
		},
		{
			name: "unknown mergeability refetches until computed", prNumber: 7,
			routes: map[string][]apiResponse{pr7Route: {
				{status: 200, body: `{"number":7,"mergeable_state":"unknown"}`},
				{status: 200, body: `{"number":7}`},
				{status: 200, body: `{"number":7,"mergeable_state":"behind"}`},
			}},
			want:         GitHubPR{Number: 7, MergeableState: "behind"},
			wantRequests: 3,
		},
		{
			name: "never computed stops after the refetch budget", prNumber: 7,
			routes:       map[string][]apiResponse{pr7Route: {{status: 200, body: `{"number":7}`}}},
			want:         GitHubPR{Number: 7},
			wantRequests: 4,
		},
		{
			name: "refetch failure surfaces", prNumber: 7,
			routes:        map[string][]apiResponse{pr7Route: {{status: 200, body: `{"number":7}`}, {status: 500}}},
			wantErrStatus: 500, wantRequests: 2,
		},
		{name: "not found", prNumber: 7, wantErrStatus: 404, wantRequests: 1},
		{name: "invalid pr number", prNumber: 0, wantErrStatus: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, api := newFakeGitHub(t, tc.routes)
			got, gerr := c.FetchPRMergeability(context.Background(), "aerial", tc.prNumber)
			require.Equal(t, tc.want, got)
			require.Len(t, api.Requests(), tc.wantRequests)
			if tc.wantErrStatus == 0 {
				require.Nil(t, gerr)
				return
			}
			require.Equal(t, tc.wantErrStatus, gerr.Status)
		})
	}
}

func TestGitHubClientFetchPRMergeabilityCancelled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wantMessage string
	}{
		{name: "cancelled context stops refetching", wantMessage: "context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewGitHubClient(&http.Client{Transport: cancelAfterFirst{}}, "http://gh.local", "")
			ctx, cancel := context.WithCancel(context.Background())
			_, gerr := c.FetchPRMergeability(withCancel(ctx, cancel), "aerial", 7)
			require.Equal(t, tc.wantMessage, gerr.Message)
		})
	}
}

// cancelCtxKey carries the cancel func the transport fires on first use.
type cancelCtxKey struct{}

func withCancel(ctx context.Context, cancel context.CancelFunc) context.Context {
	return context.WithValue(ctx, cancelCtxKey{}, cancel)
}

// cancelAfterFirst answers an unknown-mergeability PR and cancels the
// request context, so the refetch loop sees a cancelled context.
type cancelAfterFirst struct{}

func (cancelAfterFirst) RoundTrip(r *http.Request) (*http.Response, error) {
	if cancel, ok := r.Context().Value(cancelCtxKey{}).(context.CancelFunc); ok {
		cancel()
	}
	rec := httptest.NewRecorder()
	rec.WriteHeader(http.StatusOK)
	_, _ = rec.WriteString(`{"number":7}`)
	return rec.Result(), nil
}

func TestGitHubClientHeadChecksBlocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		sha    string
		routes map[string][]apiResponse
		want   bool
	}{
		{name: "all passed", sha: "h1", routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"lint","status":"completed","conclusion":"success"}]}`}}}},
		{name: "pending run blocks", sha: "h1", routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"lint","status":"in_progress"}]}`}}}, want: true},
		{name: "failed run blocks", sha: "h1", routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"test","status":"completed","conclusion":"failure"}]}`}}}, want: true},
		{name: "failed gate aggregator alone does not block", sha: "h1", routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"CI Gate","status":"completed","conclusion":"failure"}]}`}}}},
		{
			name: "newest rerun of a check wins", sha: "h1",
			routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"test","status":"completed","conclusion":"failure"},{"id":2,"name":"test","status":"completed","conclusion":"success"},{"id":0,"name":"Test","status":"completed","conclusion":"failure"}]}`}}},
		},
		{
			name: "build metadata, dependabot, and nameless runs are ignored", sha: "h1",
			routes: map[string][]apiResponse{checksRoute: {{status: 200, body: `{"check_runs":[{"id":1,"name":"build-metadata","status":"queued"},{"id":2,"name":"build-metadata-result","status":"queued"},{"id":3,"name":"x","status":"queued","app":{"slug":"dependabot"}},{"id":4,"name":" ","status":"queued"}]}`}}},
		},
		{name: "missing commit does not block", sha: "h1"},
		{name: "read failure blocks", sha: "h1", routes: map[string][]apiResponse{checksRoute: {{status: 500}}}, want: true},
		{name: "zero sha never blocks", sha: "0000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := newFakeGitHub(t, tc.routes)
			require.Equal(t, tc.want, c.HeadChecksBlocked(context.Background(), "aerial", tc.sha))
		})
	}
}

func TestGitHubClientRerunFailedJobs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		runID         int64
		routes        map[string][]apiResponse
		wantErrStatus int
		wantNil       bool
	}{
		{name: "created", runID: 55, routes: map[string][]apiResponse{rerunRoute: {{status: 201}}}, wantNil: true},
		{name: "forbidden is rate limited", runID: 55, routes: map[string][]apiResponse{rerunRoute: {{status: 403}}}, wantErrStatus: 403},
		{name: "not found", runID: 55, wantErrStatus: 404},
		{name: "invalid run id", runID: 0, wantErrStatus: 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := newFakeGitHub(t, tc.routes)
			gerr := c.RerunFailedJobs(context.Background(), "aerial", tc.runID)
			if tc.wantNil {
				require.Nil(t, gerr)
				return
			}
			require.Equal(t, tc.wantErrStatus, gerr.Status)
		})
	}
}

func TestGitHubClientTransportFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		apiURL string
	}{
		{name: "unreachable host is retryable", apiURL: "http://127.0.0.1:1"},
		{name: "malformed url is retryable", apiURL: "http://bad host"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewGitHubClient(nil, tc.apiURL, "")
			_, gerr := c.FetchPR(context.Background(), "aerial", 7)
			require.Equal(t, 0, gerr.Status)
			require.True(t, gerr.Retryable())
		})
	}
}

func TestClassifyCheckState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{name: "success passes", status: "completed", conclusion: "success", want: CheckStatePassed},
		{name: "skipped passes", status: "Completed", conclusion: "skipped", want: CheckStatePassed},
		{name: "timed out fails", status: "completed", conclusion: "timed_out", want: CheckStateFailed},
		{name: "unknown conclusion is empty", status: "completed", conclusion: "stale"},
		{name: "queued is pending", status: "queued", want: CheckStatePending},
		{name: "any other status is pending", status: "weird", want: CheckStatePending},
		{name: "no status is empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, ClassifyCheckState(tc.status, tc.conclusion))
		})
	}
}

func TestIsMergeConflict(t *testing.T) {
	t.Parallel()
	no, yes := false, true
	tests := []struct {
		name       string
		pr         GitHubPR
		wantReason string
		want       bool
	}{
		{name: "dirty conflicts", pr: GitHubPR{MergeableState: "dirty"}, wantReason: "merge_conflict", want: true},
		{name: "unmergeable conflicts", pr: GitHubPR{Mergeable: &no}, wantReason: "merge_conflict", want: true},
		{name: "behind base is actionable", pr: GitHubPR{MergeableState: "behind"}, wantReason: "branch_behind_base", want: true},
		{name: "blocked is not a conflict", pr: GitHubPR{MergeableState: "blocked"}, wantReason: "checks_or_reviews_blocked"},
		{name: "clean is no block", pr: GitHubPR{Mergeable: &yes, MergeableState: "clean"}},
		{name: "unstable is no block", pr: GitHubPR{MergeableState: "unstable"}},
		{name: "unknown states pass through", pr: GitHubPR{MergeableState: "draft"}, wantReason: "draft"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantReason, AutoMergeBlockReason(tc.pr.MergeableState, tc.pr.Mergeable))
			require.Equal(t, tc.want, IsMergeConflict(tc.pr))
		})
	}
}

func TestIsMergeabilityUnknown(t *testing.T) {
	t.Parallel()
	known := true
	tests := []struct {
		name string
		pr   GitHubPR
		want bool
	}{
		{name: "nothing computed", pr: GitHubPR{}, want: true},
		{name: "explicit unknown", pr: GitHubPR{MergeableState: "UNKNOWN"}, want: true},
		{name: "mergeable known", pr: GitHubPR{Mergeable: &known}},
		{name: "state known", pr: GitHubPR{MergeableState: "clean"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, IsMergeabilityUnknown(tc.pr))
		})
	}
}
