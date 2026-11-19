package cilog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCIFailureLogExtraction(t *testing.T) {
	t.Parallel()
	rawLog := `2026-11-15T23:00:00.000Z ##[group]Run tests
2026-11-15T23:00:01.000Z === RUN   TestSomething
2026-11-15T23:00:02.000Z --- FAIL: TestSomething (0.01s)
2026-11-15T23:00:02.000Z     main_test.go:42: something failed
2026-11-15T23:00:03.000Z FAIL
2026-11-15T23:00:04.000Z ##[endgroup]
2026-11-15T23:00:05.000Z Post job cleanup.
`
	extracted := ExtractCIFailureDetails(rawLog)
	if !strings.Contains(extracted, "FAIL: TestSomething") {
		t.Errorf("ExtractCIFailureDetails missing failure: %s", extracted)
	}
	if strings.Contains(extracted, "##[group]") || strings.Contains(extracted, "Post job cleanup.") {
		t.Errorf("ExtractCIFailureDetails contains plumbing lines: %s", extracted)
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
		{name: "fail tab", line: "FAIL\tgithub.com/azylman/aerial", want: true},
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

func TestResolveCIFailureDetails(t *testing.T) {
	t.Parallel()
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/issues/10/comments"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":1,"body":"some regular comment"},{"id":2,"body":"## roborev:\n` + "`abc1234`" + `\nFound potential issue on line 10"}]`))
		case strings.Contains(r.URL.Path, "/actions/jobs/555/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ghServer.Close)

	client := NewClient("gh-token", ghServer.Client())
	client.GitHubAPIURL = ghServer.URL

	tests := []struct {
		name      string
		repo      string
		pr        int
		check     string
		sha       string
		url       string
		details   string
		wantMatch string
	}{
		{
			name:      "roborev comment matched",
			repo:      "aerial",
			pr:        10,
			check:     "roborev",
			sha:       "abc123456789",
			details:   "failure",
			wantMatch: "roborev:",
		},
		{
			name:      "job logs extracted",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			url:       "https://github.com/azylman/aerial/actions/runs/123/job/555",
			details:   "failure",
			wantMatch: "FAIL: TestA",
		},
		{
			name:      "fallback details when check url has no job id",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			url:       "https://github.com/azylman/aerial/actions",
			details:   "timed_out",
			wantMatch: "timed_out",
		},
		{
			name:      "custom details preserved",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			details:   "already specific error details",
			wantMatch: "already specific error details",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := client.ResolveCIFailureDetails(context.Background(), tc.repo, tc.pr, tc.check, tc.sha, tc.url, tc.details)
			if !strings.Contains(got, tc.wantMatch) {
				t.Errorf("ResolveCIFailureDetails = %q, want containing %q", got, tc.wantMatch)
			}
		})
	}
}

func TestResolveCIFailureDetailsRoboRevAndLogs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Server returning RoboRev comment with non-matching SHA and matching SHA
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/comments"):
			w.WriteHeader(http.StatusOK)
			comments := []gitHubIssueComment{
				{ID: 1, Body: "## roborev: findings for `other12`\nSome finding"},
				{ID: 2, Body: "## roborev: findings for `abcdef1`\nVerified finding"},
			}
			data, _ := json.Marshal(comments)
			_, _ = w.Write(data)
		case strings.Contains(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)
			largeLog := strings.Repeat("2026-11-16T00:00:00Z line of log\n", 1000) +
				"2026-11-16T00:00:00Z FAIL: TestX\nPost job cleanup.\nmore logs\n"
			_, _ = w.Write([]byte(largeLog))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient("token", srv.Client())
	client.GitHubAPIURL = srv.URL

	// 1. RoboRev match
	details := client.ResolveCIFailureDetails(ctx, "aerial", 101, "roborev", "abcdef123456", "", "failure")
	if !strings.Contains(details, "Verified finding") {
		t.Errorf("expected Verified finding, got %q", details)
	}

	// 2. Fetch logs with actions URL
	actionsURL := "https://github.com/azylman/aerial/actions/runs/123/job/456789"
	logDetails := client.ResolveCIFailureDetails(ctx, "aerial", 0, "test", "abcdef1", actionsURL, "failure")
	if !strings.Contains(logDetails, "FAIL: TestX") {
		t.Errorf("expected FAIL: TestX in logs, got %q", logDetails)
	}
}

func TestFetchCheckJobFailureLogsAndRoboRevCases(t *testing.T) {
	t.Parallel()
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/actions/jobs/999/logs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/jobs/888/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(""))
		case strings.Contains(r.URL.Path, "/issues/99/comments"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/issues/88/comments"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":3,"body":"no roborev here"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ghServer.Close()

	client := NewClient("gh-token", ghServer.Client())
	client.GitHubAPIURL = ghServer.URL

	ctx := context.Background()

	// fetchCheckJobFailureLogsChecked nil or invalid
	if res, _ := (*Client)(nil).fetchCheckJobFailureLogsChecked(ctx, "aerial", 123); res != "" {
		t.Errorf("expected empty string, got %q", res)
	}
	if res, _ := client.fetchCheckJobFailureLogsChecked(ctx, "", 123); res != "" {
		t.Errorf("expected empty string for empty repo, got %q", res)
	}
	if res, _ := client.fetchCheckJobFailureLogsChecked(ctx, "aerial", 0); res != "" {
		t.Errorf("expected empty string for zero jobID, got %q", res)
	}
	if res, _ := client.fetchCheckJobFailureLogsChecked(ctx, "aerial", 999); res != "" {
		t.Errorf("expected empty string for 500 status, got %q", res)
	}
	if res, _ := client.fetchCheckJobFailureLogsChecked(ctx, "aerial", 888); res != "" {
		t.Errorf("expected empty for logs without high signal, got %q", res)
	}

	// fetchLatestRoboRevCommentChecked nil or invalid
	if res, _ := (*Client)(nil).fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "sha"); res != "" {
		t.Errorf("expected empty string, got %q", res)
	}
	if res, _ := client.fetchLatestRoboRevCommentChecked(ctx, "", 10, "sha"); res != "" {
		t.Errorf("expected empty string for empty repo, got %q", res)
	}
	if res, _ := client.fetchLatestRoboRevCommentChecked(ctx, "aerial", 0, "sha"); res != "" {
		t.Errorf("expected empty string for zero pr, got %q", res)
	}
	if res, _ := client.fetchLatestRoboRevCommentChecked(ctx, "aerial", 99, "sha"); res != "" {
		t.Errorf("expected empty string for 500 status, got %q", res)
	}
	if res, _ := client.fetchLatestRoboRevCommentChecked(ctx, "aerial", 88, "sha"); res != "" {
		t.Errorf("expected empty string when no roborev comment found, got %q", res)
	}

	// Huge logs > 256KB
	ghServerHuge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		prefix := strings.Repeat("log line\n", 30000)
		_, _ = w.Write([]byte(prefix + "\n--- FAIL: TestHuge (0.01s)\n"))
	}))
	defer ghServerHuge.Close()
	hugeClient := NewClient("token", ghServerHuge.Client())
	hugeClient.GitHubAPIURL = ghServerHuge.URL
	if res, _ := hugeClient.fetchCheckJobFailureLogsChecked(ctx, "aerial", 111); !strings.Contains(res, "FAIL: TestHuge") {
		t.Errorf("expected FAIL: TestHuge, got %q", res)
	}

	// RoboRev non-matching SHA
	ghServerRoboMismatch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		comments := []gitHubIssueComment{
			{ID: 1, Body: "## roborev: findings for `other12345`\nFinding"},
		}
		data, _ := json.Marshal(comments)
		_, _ = w.Write(data)
	}))
	defer ghServerRoboMismatch.Close()
	mismatchClient := NewClient("token", ghServerRoboMismatch.Client())
	mismatchClient.GitHubAPIURL = ghServerRoboMismatch.URL
	if res, _ := mismatchClient.fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "abcdef1"); res != "" {
		t.Errorf("expected empty string for non-matching SHA, got %q", res)
	}
}

func TestResolveGitHubRepoPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		repo     string
		wantPath string
	}{
		{name: "empty string", repo: "", wantPath: ""},
		{name: "whitespace only", repo: "   ", wantPath: ""},
		{name: "bare repo with slashes prepends default owner", repo: "/aerial/", wantPath: "azylman/aerial"},
		{name: "qualified repo with leading slash retains owner", repo: "/azylman/mirrormere", wantPath: "azylman/mirrormere"},
		{name: "external owner retains owner", repo: "other-org/other-repo", wantPath: "other-org/other-repo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveGitHubRepoPath(tc.repo)
			assert.Equal(t, tc.wantPath, got)
		})
	}
}

func TestFetchCheckJobFailureLogsRepoPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		repo        string
		wantSubpath string
	}{
		{name: "bare repo prepends azylman", repo: "aerial", wantSubpath: "/repos/azylman/aerial/actions/jobs/123/logs"},
		{name: "qualified repo retains owner", repo: "azylman/aerial", wantSubpath: "/repos/azylman/aerial/actions/jobs/123/logs"},
		{name: "external repo retains owner", repo: "external/repo", wantSubpath: "/repos/external/repo/actions/jobs/123/logs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedPath = r.URL.Path
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("--- FAIL: TestFail (0.01s)\n"))
			}))
			defer server.Close()

			client := NewClient("token", server.Client())
			client.GitHubAPIURL = server.URL
			res, _ := client.fetchCheckJobFailureLogsChecked(context.Background(), tc.repo, 123)
			assert.NotEmpty(t, res)
			assert.Equal(t, tc.wantSubpath, receivedPath)
		})
	}
}

func TestNewClientDefaults(t *testing.T) {
	t.Parallel()
	c := NewClient("tok", nil)
	assert.Equal(t, "tok", c.GitHubToken)
	assert.Equal(t, "https://api.github.com", c.GitHubAPIURL)
	if c.HTTPClient == nil {
		t.Error("expected non-nil HTTPClient")
	}
}

func TestIsRetryableGitHubResponseCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		resp *http.Response
		want bool
	}{
		{name: "nil response", resp: nil, want: false},
		{name: "server error", resp: &http.Response{StatusCode: http.StatusInternalServerError}, want: true},
		{name: "too many requests", resp: &http.Response{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "forbidden with retry after", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Retry-After": []string{"1"}}}, want: true},
		{name: "forbidden rate limited", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"X-Ratelimit-Remaining": []string{"0"}}}, want: true},
		{name: "forbidden plain", resp: &http.Response{StatusCode: http.StatusForbidden}, want: false},
		{name: "ok", resp: &http.Response{StatusCode: http.StatusOK}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsRetryableGitHubResponse(tc.resp))
		})
	}
}

func TestResolveCIFailureDetailsCheckedErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if got, err := (*Client)(nil).ResolveCIFailureDetailsChecked(ctx, "aerial", 1, "roborev", "sha", "", "details"); got != "details" || err != nil {
		t.Errorf("nil client = (%q, %v), want (details, nil)", got, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/comments"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("--- FAIL: TestErr (0.01s)\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// RoboRev path surfaces transport-level GitHub errors.
	errClient := NewClient("token", srv.Client())
	errClient.GitHubAPIURL = srv.URL
	if _, err := errClient.ResolveCIFailureDetailsChecked(ctx, "aerial", 10, "roborev-check", "abc123", "", "failure"); err == nil {
		t.Error("expected error for 500 comments response, got nil")
	}
	// Non-checked wrapper falls back to details on error.
	if got := errClient.ResolveCIFailureDetails(ctx, "aerial", 10, "roborev-check", "abc123", "", "fallback-details"); got != "fallback-details" {
		t.Errorf("expected fallback details, got %q", got)
	}
	// Log fetch success through the checked path.
	if got, err := errClient.ResolveCIFailureDetailsChecked(ctx, "aerial", 0, "unit", "abc", "https://github.com/o/r/actions/runs/1/job/42", "failure"); err != nil || !strings.Contains(got, "FAIL: TestErr") {
		t.Errorf("expected FAIL log, got (%q, %v)", got, err)
	}
}

func TestCILogClientEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/comments"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("--- FAIL: TestErr (0.01s)\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// Unreachable API URL surfaces a transport error.
	badClient := NewClient("token", srv.Client())
	badClient.GitHubAPIURL = "http://[::1]:namedport"
	if _, err := badClient.fetchCheckJobFailureLogsChecked(ctx, "aerial", 42); err == nil {
		t.Error("expected transport error, got nil")
	}
	if _, err := badClient.fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "sha"); err == nil {
		t.Error("expected transport error, got nil")
	}

	// Malformed API URL surfaces a request-build error.
	malformedClient := NewClient("token", srv.Client())
	malformedClient.GitHubAPIURL = "://bad-url"
	if _, err := malformedClient.fetchCheckJobFailureLogsChecked(ctx, "aerial", 42); err == nil {
		t.Error("expected request build error, got nil")
	}

	// Invalid comment JSON is treated as no comment.
	invalidJSONSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{not-json`))
	}))
	defer invalidJSONSrv.Close()
	invalidClient := NewClient("token", invalidJSONSrv.Client())
	invalidClient.GitHubAPIURL = invalidJSONSrv.URL
	if got, _ := invalidClient.fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "sha"); got != "" {
		t.Errorf("expected empty for invalid JSON, got %q", got)
	}

	// Non-retryable statuses resolve to empty without error.
	missingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer missingSrv.Close()
	missingClient := NewClient("token", missingSrv.Client())
	missingClient.GitHubAPIURL = missingSrv.URL
	if got, _ := missingClient.fetchCheckJobFailureLogsChecked(ctx, "aerial", 42); got != "" {
		t.Errorf("expected empty for 404 logs, got %q", got)
	}
	if got, _ := missingClient.fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "sha"); got != "" {
		t.Errorf("expected empty for 404 comments, got %q", got)
	}
	// Retryable log fetch errors propagate through the checked path.
	failingLogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failingLogSrv.Close()
	failingClient := NewClient("token", failingLogSrv.Client())
	failingClient.GitHubAPIURL = failingLogSrv.URL
	if _, err := failingClient.ResolveCIFailureDetailsChecked(ctx, "aerial", 0, "unit", "abc", "https://github.com/o/r/actions/runs/1/job/42", "failure"); err == nil {
		t.Error("expected error for 500 logs response, got nil")
	}

	// Empty token sends requests without an authorization header.
	noTokenClient := NewClient("", srv.Client())
	noTokenClient.GitHubAPIURL = srv.URL
	if got, err := noTokenClient.ResolveCIFailureDetailsChecked(ctx, "aerial", 0, "unit", "abc", "https://github.com/o/r/actions/runs/1/job/42", "failure"); err != nil || !strings.Contains(got, "FAIL: TestErr") {
		t.Errorf("expected FAIL log without token, got (%q, %v)", got, err)
	}

	// Nil HTTP client resolves to empty without error.
	bareClient := &Client{GitHubToken: "token", GitHubAPIURL: srv.URL}
	if got, _ := bareClient.fetchCheckJobFailureLogsChecked(ctx, "aerial", 42); got != "" {
		t.Errorf("expected empty for nil HTTPClient logs, got %q", got)
	}
	if got, _ := bareClient.fetchLatestRoboRevCommentChecked(ctx, "aerial", 10, "sha"); got != "" {
		t.Errorf("expected empty for nil HTTPClient comments, got %q", got)
	}
}

func TestExtractActionsJobIDCases(t *testing.T) {
	t.Parallel()
	assert.Equal(t, int64(456789), ExtractActionsJobID("https://github.com/o/r/actions/runs/123/job/456789"))
	assert.Equal(t, int64(0), ExtractActionsJobID("https://github.com/o/r/actions"))
	assert.Equal(t, int64(0), ExtractActionsJobID("https://github.com/o/r/actions/runs/1/job/9999999999999999999999"))
}

func TestFetchLatestRoboRevCommentRepoPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		repo        string
		wantSubpath string
	}{
		{name: "bare repo prepends azylman", repo: "aerial", wantSubpath: "/repos/azylman/aerial/issues/42/comments"},
		{name: "qualified repo retains owner", repo: "azylman/aerial", wantSubpath: "/repos/azylman/aerial/issues/42/comments"},
		{name: "external repo retains owner", repo: "external/repo", wantSubpath: "/repos/external/repo/issues/42/comments"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedPath = r.URL.Path
				w.WriteHeader(http.StatusOK)
				comments := []gitHubIssueComment{{ID: 1, Body: "## roborev: findings for `sha1234`\nFinding 1"}}
				_ = json.NewEncoder(w).Encode(comments)
			}))
			defer server.Close()

			client := NewClient("token", server.Client())
			client.GitHubAPIURL = server.URL
			res, _ := client.fetchLatestRoboRevCommentChecked(context.Background(), tc.repo, 42, "sha1234")
			assert.NotEmpty(t, res)
			assert.Equal(t, tc.wantSubpath, receivedPath)
		})
	}
}
