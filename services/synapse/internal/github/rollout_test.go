package github

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/githubapitest"
)

func TestExtractLogErrorLines(t *testing.T) {
	t.Parallel()
	raw := `
info: starting build
warning: unused variable
FAIL: TestSomething
error: syntax error at line 42
fatal: cannot continue
exit status 1
done
`
	lines := ExtractLogErrorLines(raw, 3)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %v", len(lines), lines)
	}

	// Default maxLines when <= 0
	linesDef := ExtractLogErrorLines(raw, 0)
	if len(linesDef) == 0 {
		t.Errorf("expected default maxLines > 0")
	}

	// When no patterns match, returns trailing non-blank lines
	cleanRaw := `
line 1
line 2
line 3
`
	nonErrLines := ExtractLogErrorLines(cleanRaw, 2)
	if len(nonErrLines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(nonErrLines))
	}
}

func TestSanitizeDetailsForPrompt(t *testing.T) {
	t.Parallel()
	colored := "\x1b[31mError:\x1b[0m failed test"
	sanitized := SanitizeDetailsForPrompt(colored, 50)
	if strings.Contains(sanitized, "\x1b") {
		t.Errorf("expected ANSI escape codes stripped: %q", sanitized)
	}
	if !strings.HasPrefix(sanitized, "Error: failed test") {
		t.Errorf("expected clean error message, got %q", sanitized)
	}

	sanitizedDefault := SanitizeDetailsForPrompt(colored, 0)
	if strings.Contains(sanitizedDefault, "\x1b") {
		t.Errorf("expected default sanitization to succeed")
	}
}

func TestFormatCheckFailurePrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		details  string
		checkURL string
		want     string
	}{
		{"url and details", "details text", " https://check.url ", "CI check \"test-unit\" failed on acme/widgets#42 (commit headsha).\nCheck URL: https://check.url\nDetails:\n```\ndetails text\n```\n"},
		{"no url or details", "", "", "CI check \"test-unit\" failed on acme/widgets#42 (commit headsha).\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatCheckFailurePrompt(" acme/widgets ", 42, "headsha", "test-unit", tc.details, tc.checkURL); got != tc.want {
				t.Errorf("FormatCheckFailurePrompt() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatRoboRevPrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{"findings", "review comments", "RoboRev review failed on acme/widgets#42 (commit headsha).\nReview Findings:\n```\nreview comments\n```\n"},
		{"no findings", "  ", "RoboRev review failed on acme/widgets#42 (commit headsha).\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatRoboRevPrompt("acme/widgets", 42, "headsha", tc.comment); got != tc.want {
				t.Errorf("FormatRoboRevPrompt() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatCollaboratorPRTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		repo    string
		prTitle string
		branch  string
		headSHA string
		want    string
	}{
		{name: "full pr target formatted with markdown link and metadata", repo: "acme/widgets", prTitle: "feat: add feature", branch: "refs/heads/feat/something", headSHA: "abcdef1234567890", want: `[acme/widgets#42](https://github.com/acme/widgets/pull/42) ("feat: add feature", branch: feat/something, commit: abcdef1)`},
		{name: "missing repo falls back to plain pr format", want: "PR #42"},
		{name: "repo without extra metadata formats plain link", repo: "acme/widgets", want: "[acme/widgets#42](https://github.com/acme/widgets/pull/42)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatCollaboratorPRTarget(tc.repo, 42, tc.prTitle, tc.branch, tc.headSHA); got != tc.want {
				t.Errorf("FormatCollaboratorPRTarget() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildCollaboratorAlertMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mention   string
		prTitle   string
		branch    string
		headSHA   string
		checkName string
		checkURL  string
		want      string
	}{
		{name: "named check failure with url", mention: "<@123>", prTitle: "fix: something", branch: "fix/something", headSHA: "1234567890abcdef", checkName: "Lint", checkURL: "https://example.com/runs/1", want: "<@123> CI check \"Lint\" failed for [acme/widgets#42](https://github.com/acme/widgets/pull/42) (\"fix: something\", branch: fix/something, commit: 1234567).\nCheck URL: https://example.com/runs/1"},
		{name: "generic check failure without check name or url", mention: "<@123>", want: "<@123> CI check failed for [acme/widgets#42](https://github.com/acme/widgets/pull/42)."},
		{name: "no mention omits the leading mention", checkName: "lint", want: "CI check \"lint\" failed for [acme/widgets#42](https://github.com/acme/widgets/pull/42)."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildCollaboratorAlertMessage(tc.mention, "acme/widgets", 42, tc.prTitle, tc.branch, tc.headSHA, tc.checkName, tc.checkURL); got != tc.want {
				t.Errorf("BuildCollaboratorAlertMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildCollaboratorConflictMessage(t *testing.T) {
	t.Parallel()
	const tail = " — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push."
	tests := []struct {
		name    string
		mention string
		repo    string
		prTitle string
		branch  string
		headSHA string
		want    string
	}{
		{name: "conflict message formatted with mention and target", mention: "<@123>", repo: "acme/widgets", prTitle: "feat: cool thing", branch: "feat/cool", headSHA: "abcdef123456", want: "<@123> Merge conflict with `main` detected on [acme/widgets#42](https://github.com/acme/widgets/pull/42) (\"feat: cool thing\", branch: feat/cool, commit: abcdef1)" + tail},
		{name: "no mention or repo", want: "Merge conflict with `main` detected on PR #42" + tail},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := BuildCollaboratorConflictMessage(tc.mention, tc.repo, 42, tc.prTitle, tc.branch, tc.headSHA); got != tc.want {
				t.Errorf("BuildCollaboratorConflictMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestVerifySignature(t *testing.T) {
	t.Parallel()
	secret := "mysecret"
	body := []byte(`{"action":"opened"}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	validHeader := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	tests := []struct {
		name      string
		secret    string
		header    string
		body      []byte
		wantValid bool
	}{
		{name: "valid signature", secret: secret, header: validHeader, body: body, wantValid: true},
		{name: "empty secret", secret: "", header: validHeader, body: body, wantValid: false},
		{name: "empty header", secret: secret, header: "", body: body, wantValid: false},
		{name: "wrong prefix", secret: secret, header: "sha1=1234", body: body, wantValid: false},
		{name: "invalid hex", secret: secret, header: "sha256=invalidhex!", body: body, wantValid: false},
		{name: "mismatched digest", secret: secret, header: "sha256=0000000000000000000000000000000000000000000000000000000000000000", body: body, wantValid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := VerifySignature(tc.secret, tc.header, tc.body)
			if got != tc.wantValid {
				t.Errorf("VerifySignature() = %v, want %v", got, tc.wantValid)
			}
		})
	}
}

func TestClientGetJSON(t *testing.T) {
	t.Parallel()
	srv := githubapitest.New(t, map[string][]githubapitest.Response{"GET /ok": {{Body: `{"id":123,"body":"hello"}`}}})
	tests := []struct {
		name       string
		client     *Client
		path       string
		wantID     int64
		wantStatus int
		wantErrSub string
	}{
		{name: "bound URL and http client decode", client: NewClient(srv.Client(), srv.URL(), ""), path: "/ok", wantID: 123},
		{name: "nil http client falls back to default client", client: NewClient(nil, srv.URL(), "tok"), path: "/ok", wantID: 123},
		{name: "non-200 surfaces githubapi status error", client: NewClient(srv.Client(), srv.URL(), ""), path: "/missing", wantStatus: http.StatusNotFound},
		{name: "invalid API URL fails request creation", client: NewClient(srv.Client(), "http://bad host", ""), path: "/ok", wantErrSub: "create github api request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var comment GitHubIssueComment
			err := tc.client.GetJSON(context.Background(), tc.path, 1<<20, &comment)
			switch {
			case tc.wantStatus != 0:
				statusErr, ok := errors.AsType[*githubapi.StatusError](err)
				if !ok || statusErr.StatusCode != tc.wantStatus {
					t.Fatalf("err = %v, want StatusError %d", err, tc.wantStatus)
				}
			case tc.wantErrSub != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErrSub)
				}
			default:
				if err != nil || comment.ID != tc.wantID {
					t.Fatalf("GetJSON = (%v, %+v), want id %d", err, comment, tc.wantID)
				}
			}
		})
	}
}

func TestClientNewRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		client   *Client
		wantURL  string
		wantAuth string
	}{
		{name: "nil client uses public API", client: nil, wantURL: "https://api.github.com/path"},
		{name: "configured client binds URL and token", client: NewClient(nil, "http://gh.local/", "tok"), wantURL: "http://gh.local/path", wantAuth: "Bearer tok"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := tc.client.NewRequest(context.Background(), http.MethodGet, "/path", nil)
			if err != nil {
				t.Fatalf("NewRequest error = %v", err)
			}
			if req.URL.String() != tc.wantURL || req.Header.Get("Authorization") != tc.wantAuth {
				t.Errorf("NewRequest = (%q, %q), want (%q, %q)", req.URL.String(), req.Header.Get("Authorization"), tc.wantURL, tc.wantAuth)
			}
		})
	}
}

// conflictRoutes is the fake GitHub API the FetchSinglePR tests read: 42 has no
// mergeability yet, 43 is unknown, 44 is clean, 45 fails and 46 is unknown and
// then fails.
var conflictRoutes = map[string][]githubapitest.Response{
	"GET /repos/acme/widgets/pulls/42": {{Body: `{"number":42,"state":"open","title":"Test PR","head":{"ref":"feat/branch","sha":"head42"}}`}},
	"GET /repos/acme/widgets/pulls/43": {{Body: `{"number":43,"state":"open","mergeable":null,"mergeable_state":"unknown"}`}},
	"GET /repos/acme/widgets/pulls/44": {{Body: `{"number":44,"state":"open","mergeable":true,"mergeable_state":"clean"}`}},
	"GET /repos/acme/widgets/pulls/45": {{Status: http.StatusInternalServerError, Body: `{}`}},
	"GET /repos/acme/widgets/pulls/46": {{Body: `{"number":46,"mergeable_state":"unknown"}`}, {Status: http.StatusInternalServerError, Body: `{}`}},
}

func TestFetchSinglePR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		noClient bool
		repo     string
		pr       int
		want     int
		wantErr  string
	}{
		{name: "empty repo", repo: " ", pr: 42, wantErr: "invalid arguments for fetch single pr"},
		{name: "zero pr", repo: "acme/widgets", wantErr: "invalid arguments for fetch single pr"},
		{name: "nil client", noClient: true, repo: "acme/widgets", pr: 42, wantErr: "invalid arguments for fetch single pr"},
		{name: "fetches the pr", repo: "acme/widgets", pr: 42, want: 42},
		{name: "api failure", repo: "acme/widgets", pr: 45, wantErr: "github api returned status 500"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := githubapitest.New(t, conflictRoutes)
			client := NewClient(api.Client(), api.URL(), "token")
			if tc.noClient {
				client = nil
			}
			got, err := FetchSinglePR(context.Background(), client, tc.repo, tc.pr)
			if got.Number != tc.want || (tc.wantErr == "") != (err == nil) || (err != nil && err.Error() != tc.wantErr) {
				t.Errorf("FetchSinglePR() = (#%d, %v), want (#%d, %q)", got.Number, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestFetchSinglePRWithMergeabilityRetry(t *testing.T) {
	t.Parallel()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name         string
		ctx          context.Context
		pr           int
		retries      int
		want         int
		wantErr      string
		wantRequests int
	}{
		{name: "missing mergeability refetches every retry", ctx: context.Background(), pr: 42, retries: 2, want: 42, wantRequests: 3},
		{name: "unknown state retries at least once", ctx: context.Background(), pr: 43, want: 43, wantRequests: 2},
		{name: "clean pr returns without refetching", ctx: context.Background(), pr: 44, retries: 2, want: 44, wantRequests: 1},
		{name: "first fetch failure", ctx: context.Background(), pr: 45, retries: 2, wantErr: "github api returned status 500", wantRequests: 1},
		{name: "refetch failure keeps the first pr", ctx: context.Background(), pr: 46, retries: 2, want: 46, wantErr: "github api returned status 500", wantRequests: 2},
		{name: "cancelled context", ctx: cancelled, pr: 43, retries: 2, wantErr: "context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := githubapitest.New(t, conflictRoutes)
			got, err := FetchSinglePRWithMergeabilityRetry(tc.ctx, NewClient(api.Client(), api.URL(), "token"), "acme/widgets", tc.pr, tc.retries)
			if got.Number != tc.want || (tc.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("FetchSinglePRWithMergeabilityRetry() = (#%d, %v), want (#%d, %q)", got.Number, err, tc.want, tc.wantErr)
			}
			if n := len(api.Requests()); n != tc.wantRequests {
				t.Errorf("requests = %d, want %d", n, tc.wantRequests)
			}
		})
	}
}

func TestResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()
	const openPulls = "GET /repos/acme/widgets/pulls?state=open&per_page=100"
	tests := []struct {
		name       string
		noClient   bool
		apiURL     string
		repo       string
		sha        string
		pulls      githubapitest.Response
		open       string
		wantPR     int
		wantBranch string
		wantErr    string
	}{
		{name: "empty repo", sha: "sha123", wantErr: "invalid repo or sha"},
		{name: "empty sha", repo: "acme/widgets", wantErr: "invalid repo or sha"},
		{name: "empty repo and sha", wantErr: "invalid repo or sha"},
		{name: "zero sha", repo: "acme/widgets", sha: "0000000000000000000000000000000000000000", wantErr: "invalid repo or sha"},
		{name: "short zero sha", repo: "acme/widgets", sha: "0000", wantErr: "invalid repo or sha"},
		{name: "nil client", noClient: true, repo: "acme/widgets", sha: "sha123", wantErr: "github client required"},
		{name: "invalid api url", apiURL: "::bad-url::", repo: "acme/widgets", sha: "sha123", wantErr: "create github api request"},
		{name: "403 rate limit", repo: "acme/widgets", sha: "sha403", pulls: githubapitest.Response{Status: http.StatusForbidden, Body: `{"message":"API rate limit exceeded"}`}, wantErr: "rate limit"},
		{name: "404 not found", repo: "acme/widgets", sha: "sha404", pulls: githubapitest.Response{Status: http.StatusNotFound, Body: `{"message":"Not Found"}`}, wantErr: "404"},
		{name: "malformed json", repo: "acme/widgets", sha: "shamalformed", pulls: githubapitest.Response{Body: `{not-an-array`}, wantErr: "invalid character"},
		{name: "empty pr list", repo: "acme/widgets", sha: "shaempty", pulls: githubapitest.Response{Body: `[]`}, wantErr: "no pull requests associated with commit shaempty"},
		{name: "closed pr wins over open pr", repo: "acme/widgets", sha: "shaclosed", pulls: githubapitest.Response{Body: `[{"number":10,"state":"open","head":{"ref":"feat/open"}},{"number":20,"state":"closed","head":{"ref":"feat/closed"}}]`}, wantPR: 20, wantBranch: "feat/closed"},
		{name: "closed pr is kept over a later open pr", repo: "acme/widgets", sha: "shaold", pulls: githubapitest.Response{Body: `[{"number":42,"state":"closed","head":{"ref":"feat/branch"}},{"number":50,"state":"open","head":{"ref":"feat/old"}}]`}, wantPR: 42, wantBranch: "feat/branch"},
		{name: "higher number wins when states match", repo: "acme/widgets", sha: "shahigher", pulls: githubapitest.Response{Body: `[{"number":10,"state":"closed","head":{"ref":"feat/ten"}},{"number":30,"state":"closed","head":{"ref":"feat/thirty"}},{"number":20,"state":"closed","head":{"ref":"feat/twenty"}}]`}, wantPR: 30, wantBranch: "feat/thirty"},
		{name: "fork pr fallback scans open pulls", repo: "acme/widgets", sha: "forksha123", pulls: githubapitest.Response{Body: `[]`}, open: `[{"number":456,"state":"open","head":{"ref":"fork-feature","sha":" forksha123 "}},{"number":455,"state":"open","head":{"ref":"other","sha":"diffsha"}}]`, wantPR: 456, wantBranch: "fork-feature"},
		{name: "open pull fallback without a head match", repo: "acme/widgets", sha: "notfoundsha", pulls: githubapitest.Response{Body: `[]`}, open: `[{"number":123,"state":"open","head":{"ref":"foo","sha":"other"}}]`, wantErr: "no pull requests associated"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			routes := map[string][]githubapitest.Response{"GET /repos/acme/widgets/commits/" + tc.sha + "/pulls": {tc.pulls}}
			if tc.open != "" {
				routes[openPulls] = []githubapitest.Response{{Body: tc.open}}
			}
			api := githubapitest.New(t, routes)
			client := NewClient(api.Client(), cmp.Or(tc.apiURL, api.URL()), "token")
			if tc.noClient {
				client = nil
			}
			prNum, branch, err := ResolvePRFromCommitSHA(context.Background(), client, tc.repo, tc.sha)
			if (tc.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("ResolvePRFromCommitSHA() err = %v, want %q", err, tc.wantErr)
			}
			if prNum != tc.wantPR || branch != tc.wantBranch {
				t.Errorf("ResolvePRFromCommitSHA() = (%d, %q), want (%d, %q)", prNum, branch, tc.wantPR, tc.wantBranch)
			}
		})
	}
}

func TestResolveCommitPR(t *testing.T) {
	t.Parallel()
	api := githubapitest.New(t, map[string][]githubapitest.Response{
		"GET /repos/acme/widgets/commits/merge2327/pulls": {{Body: `[{"number":2327,"state":"closed","head":{"ref":"feat/purge","sha":" head2327 "},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"merge2327"}]`}},
		"GET /repos/acme/widgets/commits/head50/pulls":    {{Body: `[{"number":50,"state":"open","head":{"ref":"feat/open","sha":"head50"},"merged_at":null,"merge_commit_sha":"testmerge50"}]`}},
	})
	client := NewClient(api.Client(), api.URL(), "token")
	tests := []struct {
		name    string
		sha     string
		want    CommitPR
		wantErr bool
	}{
		{name: "merged pr carries head and merge commit", sha: "merge2327", want: CommitPR{Number: 2327, Branch: "feat/purge", HeadSHA: "head2327", Merged: true, MergeCommitSHA: "merge2327"}},
		{name: "open pr is unmerged despite a test merge commit", sha: "head50", want: CommitPR{Number: 50, Branch: "feat/open", HeadSHA: "head50", MergeCommitSHA: "testmerge50"}},
		{name: "lookup error", sha: "missing", wantErr: true},
		{name: "zero sha is invalid", sha: "0000000000000000000000000000000000000000", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveCommitPR(context.Background(), client, "acme/widgets", tc.sha)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveCommitPR() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ResolveCommitPR() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCommitPRMergedOn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pr   CommitPR
		sha  string
		want bool
	}{
		{name: "merged on the sha", pr: CommitPR{Number: 7, Merged: true, MergeCommitSHA: "abc"}, sha: "abc", want: true},
		{name: "case and space are ignored", pr: CommitPR{Number: 7, Merged: true, MergeCommitSHA: "ABC "}, sha: " abc", want: true},
		{name: "unmerged test merge commit", pr: CommitPR{Number: 7, MergeCommitSHA: "abc"}, sha: "abc"},
		{name: "merged on another commit", pr: CommitPR{Number: 7, Merged: true, MergeCommitSHA: "def"}, sha: "abc"},
		{name: "no merge commit", pr: CommitPR{Number: 7, Merged: true}, sha: ""},
		{name: "no pr", pr: CommitPR{Merged: true, MergeCommitSHA: "abc"}, sha: "abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.pr.MergedOn(tc.sha); got != tc.want {
				t.Errorf("MergedOn(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}
}
