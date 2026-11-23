package cilog

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/githubapitest"
)

const (
	ciLogsPath     = "GET /repos/acme/aerial/actions/jobs/42/logs"
	ciCommentsPath = "GET /repos/acme/aerial/issues/10/comments?per_page=100"
	ciJobURL       = "https://github.com/acme/aerial/actions/runs/1/job/42"
	ciRoboRevBody  = "## roborev: findings for `abcdef1`\nVerified finding"
	ciRunURL       = "https://github.com/acme/aerial/actions/runs/7"
	ciRunJobsPath  = "GET /repos/acme/aerial/actions/runs/7/jobs?filter=latest&per_page=100"
	ciJob1LogsPath = "GET /repos/acme/aerial/actions/jobs/1/logs"
	ciJob2LogsPath = "GET /repos/acme/aerial/actions/jobs/2/logs"
	ciJob3LogsPath = "GET /repos/acme/aerial/actions/jobs/3/logs"
	ciJob4LogsPath = "GET /repos/acme/aerial/actions/jobs/4/logs"
	ciWorkflowFail = `Workflow "CI" concluded with failure`
	// ciRunJobs lists two failed jobs, a success, and a failed gate aggregator.
	ciRunJobs = `{"jobs":[{"id":1,"name":"lint","conclusion":"failure"},{"id":5,"name":"build","conclusion":"success"},` +
		`{"id":6,"name":"CI Gate","conclusion":"failure"},{"id":2,"name":"unit","conclusion":"timed_out"}]}`
)

// ciLogEnv declares the client and fake GitHub API one row runs against.
// nilClient and noHTTPClient exercise the guard clauses; serverDown closes the
// fake before the call (transport error); apiURL overrides the fake's URL.
type ciLogEnv struct {
	owner        string
	token        string
	routes       map[string][]githubapitest.Response
	apiURL       string
	nilClient    bool
	noHTTPClient bool
	serverDown   bool
}

// newCILogClient builds the row's client and fake API; owner and token default
// to "acme" and "gh-token" ("-" means no token).
func newCILogClient(t *testing.T, env ciLogEnv) (*Client, *githubapitest.Server) {
	t.Helper()
	srv := githubapitest.New(t, env.routes)
	if env.nilClient {
		return nil, srv
	}
	owner, token := env.owner, env.token
	if owner == "" {
		owner = "acme"
	}
	switch token {
	case "":
		token = "gh-token"
	case "-":
		token = ""
	}
	c := NewClient(token, owner, srv.Client())
	c.GitHubAPIURL = srv.URL()
	if env.apiURL != "" {
		c.GitHubAPIURL = env.apiURL
	}
	if env.noHTTPClient {
		c.HTTPClient = nil
	}
	if env.serverDown {
		srv.Close()
	}
	return c, srv
}

// assertCILogRequests checks the paths the fake saw and that every request
// carried the GitHub Accept header and the row's bearer token (none for "-").
func assertCILogRequests(t *testing.T, srv *githubapitest.Server, env ciLogEnv, wantPaths []string) {
	t.Helper()
	wantAuth := "Bearer gh-token"
	switch env.token {
	case "":
	case "-":
		wantAuth = ""
	default:
		wantAuth = "Bearer " + env.token
	}
	reqs := srv.Requests()
	paths := make([]string, 0, len(reqs))
	for _, req := range reqs {
		paths = append(paths, req.Method+" "+req.Path)
		assert.Equal(t, "application/vnd.github+json", req.Accept, req.Path)
		assert.Equal(t, wantAuth, req.Auth, req.Path)
	}
	assert.Equal(t, append([]string{}, wantPaths...), paths)
	assert.NoError(t, srv.Err())
}

func assertCILogErr(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		assert.NoError(t, err)
		return
	}
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), wantErr)
	}
}

func ciResp(status int, body string) []githubapitest.Response {
	return []githubapitest.Response{{Status: status, Body: body}}
}

func TestExtractCIFailureDetails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "strips timestamps and plumbing and stops at cleanup",
			in: `2026-11-15T23:00:00.000Z ##[group]Run tests
2026-11-15T23:00:01.000Z === RUN   TestSomething
2026-11-15T23:00:02.000Z --- FAIL: TestSomething (0.01s)
2026-11-15T23:00:02.000Z     main_test.go:42: something failed
2026-11-15T23:00:03.000Z FAIL
2026-11-15T23:00:04.000Z ##[endgroup]
2026-11-15T23:00:05.000Z Post job cleanup.
2026-11-15T23:00:06.000Z panic: after cleanup
`,
			want: "=== RUN   TestSomething\n--- FAIL: TestSomething (0.01s)\nmain_test.go:42: something failed\nFAIL",
		},
		{name: "empty log", in: "", want: ""},
		{name: "only plumbing", in: "##[group]x\r\n[command]/usr/bin/git fetch\r##[endgroup]\n", want: ""},
		{name: "ansi escapes stripped", in: "\x1b[31m--- FAIL: TestRed\x1b[0m\n", want: "--- FAIL: TestRed"},
		{
			name: "high-signal lines before the 25-line tail survive and repeats dedupe",
			in:   "panic: early\n" + strings.Repeat("noise\n", 30) + "--- FAIL: TestLate\n",
			want: "panic: early\nnoise\n--- FAIL: TestLate",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractCIFailureDetails(tc.in))
		})
	}
}

func TestIsHighSignalLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "fail test line", line: "--- FAIL: TestFoo (0.01s)", want: true},
		{name: "fail tab", line: "FAIL\texample.com/acme/aerial", want: true},
		{name: "panic line", line: "panic: runtime error", want: true},
		{name: "error line", line: "Error: something failed", want: true},
		{name: "gha error", line: "##[error] Process failed", want: true},
		{name: "alert emoji", line: "🚨 test failure", want: true},
		{name: "go coordinate", line: "pkg/test.go:42:15: syntax error", want: true},
		{name: "regular log", line: "just some regular output line", want: false},
		{name: "empty line", line: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isHighSignalLine(tc.line)
			if got != tc.want {
				t.Errorf("isHighSignalLine(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestNewClientDefaults(t *testing.T) {
	t.Parallel()
	c := NewClient("tok", "acme", nil)
	assert.Equal(t, "tok", c.GitHubToken)
	assert.Equal(t, "https://api.github.com", c.GitHubAPIURL)
	if c.HTTPClient == nil {
		t.Error("expected non-nil HTTPClient")
	}
}

func TestExtractActionsJobIDCases(t *testing.T) {
	t.Parallel()
	assert.Equal(t, int64(456789), ExtractActionsJobID("https://github.com/o/r/actions/runs/123/job/456789"))
	assert.Equal(t, int64(0), ExtractActionsJobID("https://github.com/o/r/actions"))
	assert.Equal(t, int64(0), ExtractActionsJobID("https://github.com/o/r/actions/runs/1/job/9999999999999999999999"))
}

func TestExtractActionsRunIDCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
		want int64
	}{
		{name: "run url", url: "https://github.com/o/r/actions/runs/123", want: 123},
		{name: "job url yields run", url: "https://github.com/o/r/actions/runs/123/job/456", want: 123},
		{name: "attempt url", url: "https://github.com/o/r/actions/runs/88/attempts/2", want: 88},
		{name: "no run", url: "https://github.com/o/r/actions"},
		{name: "overflow", url: "https://github.com/o/r/actions/runs/9999999999999999999999"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ExtractActionsRunID(tc.url))
		})
	}
}

func TestNeedsCIJobLogFetch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		details string
		want    bool
	}{
		{name: "empty", details: "  ", want: true},
		{name: "failure", details: "FAILURE", want: true},
		{name: "timed out", details: "timed_out", want: true},
		{name: "workflow failure", details: ciWorkflowFail, want: true},
		{name: "other workflow timed out", details: ` Workflow "Release Build" concluded with timed_out `, want: true},
		{name: "check failure header only", details: `CI check "test" failed on azylman/aerial (commit abcdef1).`, want: true},
		{name: "check failure header with check url", details: "CI check \"test\" failed on azylman/aerial (commit abcdef1).\nCheck URL: https://github.com/o/r/actions/runs/1/job/2", want: true},
		{name: "check failure header with extra details", details: "CI check \"test\" failed on azylman/aerial (commit abcdef1).\nCheck URL: https://github.com/o/r/actions/runs/1/job/2\nextra line", want: false},
		{name: "workflow prefix with extra text", details: ciWorkflowFail + ": see logs", want: false},
		{name: "specific details", details: "--- FAIL: TestX", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, NeedsCIJobLogFetch(tc.details))
		})
	}
}

func TestFetchRunFailureLogsChecked(t *testing.T) {
	t.Parallel()
	log1 := ciResp(http.StatusOK, "--- FAIL: TestLint (0.01s)\n")
	log2 := ciResp(http.StatusOK, "--- FAIL: TestUnit (0.01s)\n")
	tests := []struct {
		name      string
		env       ciLogEnv
		repo      string
		runID     int64
		want      string
		wantErr   string
		wantPaths []string
	}{
		{name: "nil client", env: ciLogEnv{nilClient: true}, repo: "aerial", runID: 7},
		{name: "nil http client", env: ciLogEnv{noHTTPClient: true}, repo: "aerial", runID: 7},
		{name: "empty repo", repo: " / ", runID: 7},
		{name: "zero run id", repo: "aerial"},
		{name: "malformed api url", env: ciLogEnv{apiURL: "://bad-url"}, repo: "aerial", runID: 7, wantErr: "missing protocol scheme"},
		{name: "transport error", env: ciLogEnv{serverDown: true}, repo: "aerial", runID: 7, wantErr: "connection refused"},
		{
			name: "retryable listing", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusInternalServerError, "")}},
			repo: "aerial", runID: 7, wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciRunJobsPath},
		},
		{name: "listing not found is empty", repo: "aerial", runID: 7, wantPaths: []string{ciRunJobsPath}},
		{
			name: "invalid listing json is empty", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusOK, `{not-json`)}},
			repo: "aerial", runID: 7, wantPaths: []string{ciRunJobsPath},
		},
		{
			name: "failed jobs joined in order", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusOK, ciRunJobs), ciJob1LogsPath: log1, ciJob2LogsPath: log2}},
			repo: "aerial", runID: 7, want: "### Job: lint\n--- FAIL: TestLint (0.01s)\n\n### Job: unit\n--- FAIL: TestUnit (0.01s)",
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath, ciJob2LogsPath},
		},
		{
			name: "at most three failed jobs and empty logs dropped",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":0,"name":"ghost","conclusion":"failure"},{"id":1,"name":"a","conclusion":"failure"},{"id":2,"name":"b","conclusion":"failure"},`+
					`{"id":3,"name":"c","conclusion":"failure"},{"id":4,"name":"d","conclusion":"failure"}]}`),
				ciJob1LogsPath: log1, ciJob2LogsPath: ciResp(http.StatusOK, ""), ciJob3LogsPath: log2, ciJob4LogsPath: log2,
			}},
			repo: "aerial", runID: 7, want: "### Job: a\n--- FAIL: TestLint (0.01s)\n\n### Job: c\n--- FAIL: TestUnit (0.01s)",
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath, ciJob2LogsPath, ciJob3LogsPath},
		},
		{
			name: "job log retryable error", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusOK, ciRunJobs), ciJob1LogsPath: ciResp(http.StatusBadGateway, "")}},
			repo: "aerial", runID: 7, wantErr: "github api error (502): Bad Gateway", wantPaths: []string{ciRunJobsPath, ciJob1LogsPath},
		},
		{
			name: "qualified repo retains owner",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				"GET /repos/alpha/aerial/actions/runs/7/jobs?filter=latest&per_page=100": ciResp(http.StatusOK, `{"jobs":[{"id":1,"name":"lint","conclusion":"failure"}]}`),
				"GET /repos/alpha/aerial/actions/jobs/1/logs":                            log1,
			}},
			repo: "alpha/aerial", runID: 7, want: "### Job: lint\n--- FAIL: TestLint (0.01s)",
			wantPaths: []string{"GET /repos/alpha/aerial/actions/runs/7/jobs?filter=latest&per_page=100", "GET /repos/alpha/aerial/actions/jobs/1/logs"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.fetchRunFailureLogsChecked(context.Background(), tc.repo, tc.runID)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			if !tc.env.serverDown {
				assertCILogRequests(t, srv, tc.env, tc.wantPaths)
			}
		})
	}
}

func TestFetchCheckJobFailureLogsChecked(t *testing.T) {
	t.Parallel()
	failLog := ciResp(http.StatusOK, "--- FAIL: TestFail (0.01s)\n")
	tests := []struct {
		name      string
		env       ciLogEnv
		repo      string
		jobID     int64
		want      string
		wantErr   string
		wantPaths []string
	}{
		{name: "nil client", env: ciLogEnv{nilClient: true}, repo: "aerial", jobID: 42},
		{name: "nil http client", env: ciLogEnv{noHTTPClient: true}, repo: "aerial", jobID: 42},
		{name: "empty repo", repo: " / ", jobID: 42},
		{name: "zero job id", repo: "aerial"},
		{name: "malformed api url", env: ciLogEnv{apiURL: "://bad-url"}, repo: "aerial", jobID: 42, wantErr: "missing protocol scheme"},
		{name: "transport error", env: ciLogEnv{serverDown: true}, repo: "aerial", jobID: 42, wantErr: "connection refused"},
		{
			name: "retryable status", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusInternalServerError, "")}},
			repo: "aerial", jobID: 42, wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciLogsPath},
		},
		{name: "not found is empty", repo: "aerial", jobID: 42, wantPaths: []string{ciLogsPath}},
		{name: "empty log", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusOK, "")}}, repo: "aerial", jobID: 42, wantPaths: []string{ciLogsPath}},
		{
			name: "logs over 256KiB keep only the tail",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusOK,
				"panic: dropped by truncation\n"+strings.Repeat("log line\n", 30000)+"--- FAIL: TestHuge (0.01s)\n")}},
			repo: "aerial", jobID: 42, want: "log line\n--- FAIL: TestHuge (0.01s)", wantPaths: []string{ciLogsPath},
		},
		{
			name: "bare repo gets configured owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/acme/aerial/actions/jobs/123/logs": failLog}},
			repo: "aerial", jobID: 123, want: "--- FAIL: TestFail (0.01s)", wantPaths: []string{"GET /repos/acme/aerial/actions/jobs/123/logs"},
		},
		{
			name: "bare repo gets other configured owner", env: ciLogEnv{owner: "beta", routes: map[string][]githubapitest.Response{"GET /repos/beta/aerial/actions/jobs/123/logs": failLog}},
			repo: " /aerial/ ", jobID: 123, want: "--- FAIL: TestFail (0.01s)", wantPaths: []string{"GET /repos/beta/aerial/actions/jobs/123/logs"},
		},
		{
			name: "qualified repo retains owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/alpha/aerial/actions/jobs/123/logs": failLog}},
			repo: "alpha/aerial", jobID: 123, want: "--- FAIL: TestFail (0.01s)", wantPaths: []string{"GET /repos/alpha/aerial/actions/jobs/123/logs"},
		},
		{
			name: "external repo retains owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/external/repo/actions/jobs/123/logs": failLog}},
			repo: "external/repo", jobID: 123, want: "--- FAIL: TestFail (0.01s)", wantPaths: []string{"GET /repos/external/repo/actions/jobs/123/logs"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.fetchCheckJobFailureLogsChecked(context.Background(), tc.repo, tc.jobID)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			if !tc.env.serverDown {
				assertCILogRequests(t, srv, tc.env, tc.wantPaths)
			}
		})
	}
}

func TestFetchLatestRoboRevCommentChecked(t *testing.T) {
	t.Parallel()
	matching := ciResp(http.StatusOK, `[{"id":1,"body":"## roborev: findings for `+"`sha1234`"+`\nFinding 1"}]`)
	tests := []struct {
		name      string
		env       ciLogEnv
		repo      string
		pr        int
		sha       string
		want      string
		wantErr   string
		wantPaths []string
	}{
		{name: "nil client", env: ciLogEnv{nilClient: true}, repo: "aerial", pr: 10, sha: "abcdef1"},
		{name: "nil http client", env: ciLogEnv{noHTTPClient: true}, repo: "aerial", pr: 10, sha: "abcdef1"},
		{name: "empty repo", repo: "", pr: 10, sha: "abcdef1"},
		{name: "zero pr", repo: "aerial", sha: "abcdef1"},
		{name: "malformed api url", env: ciLogEnv{apiURL: "://bad-url"}, repo: "aerial", pr: 10, sha: "abcdef1", wantErr: "missing protocol scheme"},
		{name: "transport error", env: ciLogEnv{serverDown: true}, repo: "aerial", pr: 10, sha: "abcdef1", wantErr: "connection refused"},
		{
			name: "retryable status", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusInternalServerError, "")}},
			repo: "aerial", pr: 10, sha: "abcdef1", wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciCommentsPath},
		},
		{name: "not found is empty", repo: "aerial", pr: 10, sha: "abcdef1", wantPaths: []string{ciCommentsPath}},
		{
			name: "invalid json is empty", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK, `{not-json`)}},
			repo: "aerial", pr: 10, sha: "abcdef1", wantPaths: []string{ciCommentsPath},
		},
		{
			name: "no roborev comment", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK, `[{"id":3,"body":"no roborev here"}]`)}},
			repo: "aerial", pr: 10, sha: "abcdef1", wantPaths: []string{ciCommentsPath},
		},
		{
			name: "roborev comment for another sha",
			env:  ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK, `[{"id":1,"body":"## roborev: findings for `+"`other12345`"+`\nFinding"}]`)}},
			repo: "aerial", pr: 10, sha: "abcdef1", wantPaths: []string{ciCommentsPath},
		},
		{
			name: "newest comment matching the head sha wins",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK,
				`[{"id":1,"body":"## roborev: findings for `+"`abcdef1`"+`\nStale finding"},{"id":2,"body":"## roborev: findings for `+"`abcdef1`"+`\nVerified finding"},{"id":3,"body":"## roborev: findings for `+"`other12`"+`\nOther"}]`)}},
			repo: "aerial", pr: 10, sha: "abcdef123456", want: ciRoboRevBody, wantPaths: []string{ciCommentsPath},
		},
		{
			name: "bare repo gets configured owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/acme/aerial/issues/42/comments?per_page=100": matching}},
			repo: "aerial", pr: 42, sha: "sha1234", want: "## roborev: findings for `sha1234`\nFinding 1", wantPaths: []string{"GET /repos/acme/aerial/issues/42/comments?per_page=100"},
		},
		{
			name: "qualified repo retains owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/alpha/aerial/issues/42/comments?per_page=100": matching}},
			repo: "alpha/aerial", pr: 42, sha: "sha1234", want: "## roborev: findings for `sha1234`\nFinding 1", wantPaths: []string{"GET /repos/alpha/aerial/issues/42/comments?per_page=100"},
		},
		{
			name: "external repo retains owner", env: ciLogEnv{routes: map[string][]githubapitest.Response{"GET /repos/external/repo/issues/42/comments?per_page=100": matching}},
			repo: "external/repo", pr: 42, sha: "sha1234", want: "## roborev: findings for `sha1234`\nFinding 1", wantPaths: []string{"GET /repos/external/repo/issues/42/comments?per_page=100"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.fetchLatestRoboRevCommentChecked(context.Background(), tc.repo, tc.pr, tc.sha)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			if !tc.env.serverDown {
				assertCILogRequests(t, srv, tc.env, tc.wantPaths)
			}
		})
	}
}

func TestHasRoboRevOverrideChecked(t *testing.T) {
	t.Parallel()
	comments := func(body string) ciLogEnv {
		return ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK, body)}}
	}
	tests := []struct {
		name      string
		env       ciLogEnv
		sha       string
		want      bool
		wantErr   string
		wantPaths []string
	}{
		{name: "override on head", env: comments(`[{"id":1,"body":"## roborev-override (` + "`abcdef1`" + `)\n\n**Commit:** ` + "`abcdef1234567`" + `"}]`), sha: "ABCDEF1234567", want: true, wantPaths: []string{ciCommentsPath}},
		{name: "override on older head", env: comments(`[{"id":1,"body":"## roborev-override (` + "`1234567`" + `)"}]`), sha: "abcdef1234567", wantPaths: []string{ciCommentsPath}},
		{name: "review comment is not an override", env: comments(`[{"id":1,"body":"` + "## roborev: findings for `abcdef1`" + `"}]`), sha: "abcdef1234567", wantPaths: []string{ciCommentsPath}},
		{name: "empty head never looks up comments", sha: " "},
		{name: "missing comments read as none", sha: "abcdef1", wantPaths: []string{ciCommentsPath}},
		{
			name: "retryable status", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusBadGateway, "")}},
			sha: "abcdef1", wantErr: "github api error (502): Bad Gateway", wantPaths: []string{ciCommentsPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.HasRoboRevOverrideChecked(context.Background(), "aerial", 10, tc.sha)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			assertCILogRequests(t, srv, tc.env, tc.wantPaths)
		})
	}
}

func TestHasActiveOrRecoveredChecksChecked(t *testing.T) {
	t.Parallel()
	const checkRunsPath = "GET /repos/acme/aerial/commits/abc123/check-runs?filter=latest&per_page=100"
	runs := func(body string) ciLogEnv {
		return ciLogEnv{routes: map[string][]githubapitest.Response{checkRunsPath: ciResp(http.StatusOK, body)}}
	}
	tests := []struct {
		name      string
		env       ciLogEnv
		repo      string
		sha       string
		want      bool
		wantErr   string
		wantPaths []string
	}{
		{name: "nil client", env: ciLogEnv{nilClient: true}, repo: "aerial", sha: "abc123"},
		{name: "nil http client", env: ciLogEnv{noHTTPClient: true}, repo: "aerial", sha: "abc123"},
		{name: "empty repo", repo: "", sha: "abc123"},
		{name: "empty sha", repo: "aerial", sha: "   "},
		{name: "malformed api url", env: ciLogEnv{apiURL: "://bad-url"}, repo: "aerial", sha: "abc123", wantErr: "missing protocol scheme"},
		{name: "transport error", env: ciLogEnv{serverDown: true}, repo: "aerial", sha: "abc123", wantErr: "connection refused"},
		{
			name:      "retryable 500 error",
			env:       ciLogEnv{routes: map[string][]githubapitest.Response{checkRunsPath: ciResp(http.StatusInternalServerError, "")}},
			repo:      "aerial",
			sha:       "abc123",
			wantErr:   "github api error (500): Internal Server Error",
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "non-200 status is false",
			env:       ciLogEnv{routes: map[string][]githubapitest.Response{checkRunsPath: ciResp(http.StatusNotFound, `{"message":"Not Found"}`)}},
			repo:      "aerial",
			sha:       "abc123",
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "malformed json is false",
			env:       ciLogEnv{routes: map[string][]githubapitest.Response{checkRunsPath: ciResp(http.StatusOK, `not-json`)}},
			repo:      "aerial",
			sha:       "abc123",
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "empty check runs is false",
			env:       runs(`{"check_runs":[]}`),
			repo:      "aerial",
			sha:       "abc123",
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "gate-only check runs is false",
			env:       runs(`{"check_runs":[{"name":"CI Gate","status":"completed","conclusion":"success"}]}`),
			repo:      "aerial",
			sha:       "abc123",
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "active queued check run is true",
			env:       runs(`{"check_runs":[{"name":"unit-tests","status":"queued","conclusion":""}]}`),
			repo:      "aerial",
			sha:       "abc123",
			want:      true,
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "active in_progress check run is true",
			env:       runs(`{"check_runs":[{"name":"unit-tests","status":"in_progress","conclusion":""}]}`),
			repo:      "aerial",
			sha:       "abc123",
			want:      true,
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "all completed and passing is true",
			env:       runs(`{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"success"},{"name":"lint","status":"completed","conclusion":"neutral"}]}`),
			repo:      "aerial",
			sha:       "abc123",
			want:      true,
			wantPaths: []string{checkRunsPath},
		},
		{
			name:      "completed with a failing check run is false",
			env:       runs(`{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"failure"},{"name":"lint","status":"completed","conclusion":"success"}]}`),
			repo:      "aerial",
			sha:       "abc123",
			wantPaths: []string{checkRunsPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.HasActiveOrRecoveredChecksChecked(context.Background(), tc.repo, tc.sha)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			if !tc.env.serverDown {
				assertCILogRequests(t, srv, tc.env, tc.wantPaths)
			}
		})
	}
}

func TestIsWorkflowRunFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		checkURL string
		details  string
		want     bool
	}{
		{name: "workflow conclusion regex matches", checkURL: "", details: ciWorkflowFail, want: true},
		{name: "run url without job id", checkURL: "https://github.com/acme/aerial/actions/runs/123", details: "failure", want: true},
		{name: "job url with job id is not workflow failure", checkURL: ciJobURL, details: "failure", want: false},
		{name: "actions base url without run id is not workflow failure", checkURL: "https://github.com/acme/aerial/actions", details: "failure", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsWorkflowRunFailure(tc.checkURL, tc.details))
		})
	}
}

type resolveCIFailureCase struct {
	name      string
	env       ciLogEnv
	pr        int
	check     string
	url       string
	details   string
	want      CIFailureResolution
	wantErr   string
	wantPaths []string
}

func resolveCIFailureBasicCases() []resolveCIFailureCase {
	const (
		ciRun1JobsPath = "GET /repos/acme/aerial/actions/runs/1/jobs?filter=latest&per_page=100"
	)
	failLog := ciResp(http.StatusOK, "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL\n")
	return []resolveCIFailureCase{
		{
			name: "nil client returns details and sets failed checks when not workflow failure",
			env:  ciLogEnv{nilClient: true}, pr: 10, check: "unit", details: "details",
			want: CIFailureResolution{CheckName: "unit", Details: "details", FailedChecks: []string{"unit"}},
		},
		{
			name:    "url without run id uses fallback and sets FailedChecks",
			url:     "https://github.com/acme/aerial/actions",
			check:   "unit",
			details: "already specific error details",
			want:    CIFailureResolution{CheckName: "unit", Details: "already specific error details", FailedChecks: []string{"unit"}},
		},
		{
			name:    "workflow failure fallback leaves FailedChecks empty",
			url:     "https://github.com/acme/aerial/actions",
			check:   "CI",
			details: ciWorkflowFail,
			want:    CIFailureResolution{CheckName: "CI", Details: ciWorkflowFail},
		},
		{
			name: "pending sibling job holds remediation",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRun1JobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":42,"name":"lint","status":"completed","conclusion":"failure"},{"id":43,"name":"test","status":"in_progress"}]}`),
			}},
			check:     "lint",
			url:       ciJobURL,
			details:   "failure",
			want:      CIFailureResolution{Pending: true, CheckName: "lint", Details: "failure"},
			wantPaths: []string{ciRun1JobsPath},
		},
		{
			name: "single failed job matching triggerJobID with specific details uses details directly",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRun1JobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":42,"name":"lint","status":"completed","conclusion":"failure"},{"id":43,"name":"test","status":"completed","conclusion":"success"}]}`),
			}},
			check:     "lint",
			url:       ciJobURL,
			details:   "specific lint failure details",
			want:      CIFailureResolution{Pending: false, CheckName: "lint", Details: "specific lint failure details", FailedChecks: []string{"lint"}},
			wantPaths: []string{ciRun1JobsPath},
		},
		{
			name: "single failed job matching triggerJobID fetches job log",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRun1JobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":42,"name":"lint","status":"completed","conclusion":"failure"},{"id":43,"name":"test","status":"completed","conclusion":"success"}]}`),
				ciLogsPath:     failLog,
			}},
			check:     "lint",
			url:       ciJobURL,
			details:   "failure",
			want:      CIFailureResolution{Pending: false, CheckName: "lint", Details: "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL", FailedChecks: []string{"lint"}},
			wantPaths: []string{ciRun1JobsPath, ciLogsPath},
		},
	}
}

func resolveCIFailureRunCases() []resolveCIFailureCase {
	failLog := ciResp(http.StatusOK, "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL\n")
	return []resolveCIFailureCase{
		{
			name: "multiple failed jobs coalesces check names and formats sections",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":1,"name":"lint","status":"completed","conclusion":"failure"},{"id":2,"name":"unit","status":"completed","conclusion":"failure"}]}`),
				ciJob1LogsPath: ciResp(http.StatusOK, "--- FAIL: TestLint (0.01s)\n"),
				ciJob2LogsPath: failLog,
			}},
			check:   "lint",
			url:     "https://github.com/acme/aerial/actions/runs/7/job/1",
			details: "failure",
			want: CIFailureResolution{
				Pending:      false,
				CheckName:    "lint, unit",
				Details:      "### Job: lint\n--- FAIL: TestLint (0.01s)\n\n### Job: unit\n=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL",
				FailedChecks: []string{"lint", "unit"},
			},
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath, ciJob2LogsPath},
		},
		{
			name: "run with roborev comment and deterministic failure",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":1,"name":"lint","status":"completed","conclusion":"failure"},{"id":2,"name":"roborev","status":"completed","conclusion":"failure"}]}`),
				ciJob1LogsPath: ciResp(http.StatusOK, "--- FAIL: TestLint (0.01s)\n"),
				ciCommentsPath: ciResp(http.StatusOK, `[{"id":2,"body":"`+strings.ReplaceAll(ciRoboRevBody, "\n", `\n`)+`"}]`),
			}},
			pr:      10,
			check:   "CI",
			url:     ciRunURL,
			details: ciWorkflowFail,
			want: CIFailureResolution{
				Pending:      false,
				CheckName:    "lint, roborev",
				Details:      "### Job: lint\n--- FAIL: TestLint (0.01s)\n\n### Job: roborev\n" + ciRoboRevBody,
				FailedChecks: []string{"lint", "roborev"},
			},
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath, ciCommentsPath},
		},
		{
			name: "run with no failed jobs returns res leaving FailedChecks empty",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":5,"name":"build","status":"completed","conclusion":"success"}]}`),
			}},
			check:     "CI",
			url:       ciRunURL,
			details:   ciWorkflowFail,
			want:      CIFailureResolution{Pending: false, CheckName: "CI", Details: ciWorkflowFail},
			wantPaths: []string{ciRunJobsPath},
		},
		{
			name: "run with roborev without comment falls back to job logs",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":2,"name":"roborev","status":"completed","conclusion":"failure"}]}`),
				ciCommentsPath: ciResp(http.StatusOK, `[]`),
				ciJob2LogsPath: ciResp(http.StatusOK, "roborev job log failure"),
			}},
			pr:      10,
			check:   "CI",
			url:     ciRunURL,
			details: ciWorkflowFail,
			want: CIFailureResolution{
				Pending:      false,
				CheckName:    "roborev",
				Details:      "### Job: roborev\nroborev job log failure",
				FailedChecks: []string{"roborev"},
			},
			wantPaths: []string{ciRunJobsPath, ciCommentsPath, ciJob2LogsPath},
		},
		{
			name: "run with roborev without pr number fetches job logs directly",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":2,"name":"roborev","status":"completed","conclusion":"failure"}]}`),
				ciJob2LogsPath: ciResp(http.StatusOK, "roborev job log failure"),
			}},
			pr:      0,
			check:   "CI",
			url:     ciRunURL,
			details: ciWorkflowFail,
			want: CIFailureResolution{
				Pending:      false,
				CheckName:    "roborev",
				Details:      "### Job: roborev\nroborev job log failure",
				FailedChecks: []string{"roborev"},
			},
			wantPaths: []string{ciRunJobsPath, ciJob2LogsPath},
		},
		{
			name: "run with roborev comments api error propagates",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":2,"name":"roborev","status":"completed","conclusion":"failure"}]}`),
				ciCommentsPath: ciResp(http.StatusInternalServerError, ""),
			}},
			pr:        10,
			check:     "CI",
			url:       ciRunURL,
			details:   ciWorkflowFail,
			want:      CIFailureResolution{CheckName: "roborev", Details: ciWorkflowFail, FailedChecks: []string{"roborev"}},
			wantErr:   "github api error (500): Internal Server Error",
			wantPaths: []string{ciRunJobsPath, ciCommentsPath},
		},
		{
			name: "run with failed job log error propagates",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath:  ciResp(http.StatusOK, `{"jobs":[{"id":1,"name":"lint","status":"completed","conclusion":"failure"}]}`),
				ciJob1LogsPath: ciResp(http.StatusInternalServerError, ""),
			}},
			pr:        10,
			check:     "CI",
			url:       ciRunURL,
			details:   ciWorkflowFail,
			want:      CIFailureResolution{CheckName: "lint", Details: ciWorkflowFail, FailedChecks: []string{"lint"}},
			wantErr:   "github api error (500): Internal Server Error",
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath},
		},
		{
			name: "listing error propagates",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath: ciResp(http.StatusInternalServerError, ""),
			}},
			check:     "CI",
			url:       ciRunURL,
			details:   ciWorkflowFail,
			want:      CIFailureResolution{CheckName: "CI", Details: ciWorkflowFail},
			wantErr:   "github api error (500): Internal Server Error",
			wantPaths: []string{ciRunJobsPath},
		},
	}
}

func resolveCIFailureTestCases() []resolveCIFailureCase {
	return append(resolveCIFailureBasicCases(), resolveCIFailureRunCases()...)
}

func TestResolveCIFailureChecked(t *testing.T) {
	t.Parallel()
	for _, tc := range resolveCIFailureTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.ResolveCIFailureChecked(context.Background(), "aerial", tc.pr, tc.check, "abcdef123456", tc.url, tc.details)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			assertCILogRequests(t, srv, tc.env, tc.wantPaths)
		})
	}
}

func TestResolveCIFailureDetailsChecked(t *testing.T) {
	t.Parallel()
	roboRev := ciResp(http.StatusOK, `[{"id":1,"body":"some regular comment"},{"id":2,"body":"`+strings.ReplaceAll(ciRoboRevBody, "\n", `\n`)+`"}]`)
	failLog := ciResp(http.StatusOK, "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL\n")
	tests := []struct {
		name      string
		env       ciLogEnv
		pr        int
		check     string
		url       string
		details   string
		want      string
		wantErr   string
		wantPaths []string
	}{
		{name: "nil client returns details", env: ciLogEnv{nilClient: true}, pr: 10, check: "roborev", details: "details", want: "details"},
		{
			name: "roborev comment replaces details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: roboRev}},
			pr: 10, check: "roborev", url: ciJobURL, details: "failure", want: ciRoboRevBody, wantPaths: []string{ciCommentsPath},
		},
		{
			name: "roborev error keeps details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusInternalServerError, "")}},
			pr: 10, check: "roborev", url: ciJobURL, details: "failure", want: "failure",
			wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciCommentsPath},
		},
		{
			name: "roborev without comment falls back to job logs",
			env:  ciLogEnv{routes: map[string][]githubapitest.Response{ciCommentsPath: ciResp(http.StatusOK, `[]`), ciLogsPath: failLog}},
			pr:   10, check: "roborev", url: ciJobURL, details: "failure",
			want: "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL", wantPaths: []string{ciCommentsPath, ciLogsPath},
		},
		{
			name: "job logs replace failure details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: failLog}},
			pr: 10, check: "unit-tests", url: ciJobURL, details: "failure", want: "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL", wantPaths: []string{ciLogsPath},
		},
		{
			name: "job logs without a pr number",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusOK,
				strings.Repeat("2026-11-16T00:00:00Z line of log\n", 1000)+"2026-11-16T00:00:00Z FAIL: TestX\nPost job cleanup.\nmore logs\n")}},
			check: "roborev", url: ciJobURL, details: "failure", want: "line of log\nFAIL: TestX", wantPaths: []string{ciLogsPath},
		},
		{
			name: "job logs fetched without a token", env: ciLogEnv{token: "-", routes: map[string][]githubapitest.Response{ciLogsPath: failLog}},
			check: "unit", url: ciJobURL, details: "failure", want: "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL", wantPaths: []string{ciLogsPath},
		},
		{
			name: "job log error keeps details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusInternalServerError, "")}},
			check: "unit", url: ciJobURL, details: "failure", want: "failure",
			wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciLogsPath},
		},
		{
			name: "empty job log keeps details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: ciResp(http.StatusOK, "")}},
			check: "unit", url: ciJobURL, details: "timed_out", want: "timed_out", wantPaths: []string{ciLogsPath},
		},
		{name: "check url without job id keeps details", pr: 10, check: "unit-tests", url: "https://github.com/acme/aerial/actions", details: "timed_out", want: "timed_out"},
		{name: "specific details skip the log fetch", pr: 10, check: "unit-tests", url: ciJobURL, details: "already specific error details", want: "already specific error details"},
		{
			name: "workflow failure on run url fetches failed job logs",
			env: ciLogEnv{routes: map[string][]githubapitest.Response{
				ciRunJobsPath: ciResp(http.StatusOK, ciRunJobs), ciJob1LogsPath: ciResp(http.StatusOK, "--- FAIL: TestLint (0.01s)\n"), ciJob2LogsPath: failLog,
			}},
			pr: 10, check: "CI", url: ciRunURL, details: ciWorkflowFail,
			want:      "### Job: lint\n--- FAIL: TestLint (0.01s)\n\n### Job: unit\n=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL",
			wantPaths: []string{ciRunJobsPath, ciJob1LogsPath, ciJob2LogsPath},
		},
		{
			name: "run without failed jobs keeps details",
			env:  ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusOK, `{"jobs":[{"id":5,"name":"build","conclusion":"success"}]}`)}},
			pr:   10, check: "CI", url: ciRunURL, details: ciWorkflowFail, want: ciWorkflowFail, wantPaths: []string{ciRunJobsPath},
		},
		{
			name: "run jobs listing error keeps details", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciRunJobsPath: ciResp(http.StatusInternalServerError, "")}},
			pr: 10, check: "CI", url: ciRunURL, details: ciWorkflowFail, want: ciWorkflowFail,
			wantErr: "github api error (500): Internal Server Error", wantPaths: []string{ciRunJobsPath},
		},
		{
			name: "job url preferred over run url", env: ciLogEnv{routes: map[string][]githubapitest.Response{ciLogsPath: failLog}},
			pr: 10, check: "CI", url: ciJobURL, details: ciWorkflowFail, want: "=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL", wantPaths: []string{ciLogsPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, srv := newCILogClient(t, tc.env)
			got, err := c.ResolveCIFailureDetailsChecked(context.Background(), "aerial", tc.pr, tc.check, "abcdef123456", tc.url, tc.details)
			assertCILogErr(t, err, tc.wantErr)
			assert.Equal(t, tc.want, got)
			assertCILogRequests(t, srv, tc.env, tc.wantPaths)
		})
	}
}
