package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExtractActionsJobID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		url    string
		wantID int64
	}{
		{name: "empty url", url: "", wantID: 0},
		{name: "non actions url", url: "https://github.com/azylman/aerial/pull/1", wantID: 0},
		{name: "invalid run format", url: "https://github.com/azylman/aerial/actions/runs/abc/job/123", wantID: 0},
		{name: "valid actions url", url: "https://github.com/azylman/aerial/actions/runs/12345/job/67890", wantID: 67890},
		{name: "valid actions url with query", url: "https://github.com/azylman/aerial/actions/runs/12345/job/67890?check_suite_focus=true", wantID: 67890},
		{name: "zero job id", url: "https://github.com/azylman/aerial/actions/runs/12345/job/0", wantID: 0},
		{name: "negative job id", url: "https://github.com/azylman/aerial/actions/runs/12345/job/-1", wantID: 0},
		{name: "relative path", url: "/actions/runs/999/job/888", wantID: 888},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractActionsJobID(tc.url)
			if got != tc.wantID {
				t.Fatalf("extractActionsJobID(%q) = %d, want %d", tc.url, got, tc.wantID)
			}
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
		{name: "empty string", details: "", want: true},
		{name: "whitespace only", details: "   \n\t  ", want: true},
		{name: "short summary Failed", details: "Failed", want: true},
		{name: "short summary Review Failed", details: "Review Failed", want: true},
		{name: "short summary Workflow failure", details: "Workflow \"CI\" concluded with failure", want: true},
		{name: "multi line details", details: "Step failed:\nexit code 1", want: false},
		{name: "long single line >= 80 chars", details: strings.Repeat("A", 80), want: false},
		{name: "single line 79 chars", details: strings.Repeat("A", 79), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := needsCIJobLogFetch(tc.details)
			if got != tc.want {
				t.Fatalf("needsCIJobLogFetch(%q) = %v, want %v", tc.details, got, tc.want)
			}
		})
	}
}

func TestExtractCIFailureDetails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rawLog  string
		wantSub []string
		notSub  []string
	}{
		{
			name:    "empty log",
			rawLog:  "",
			wantSub: nil,
			notSub:  []string{"Error"},
		},
		{
			name:    "strips timestamps and ansi escapes",
			rawLog:  "2026-10-22T21:40:12.1234567Z \x1b[31;1mError: something failed\x1b[0m\r\n",
			wantSub: []string{"Error: something failed"},
			notSub:  []string{"2026-10-22", "\x1b"},
		},
		{
			name:    "skips plumbing and stops at post job cleanup",
			rawLog:  "##[group]Setup\n[command]/usr/bin/git checkout\n##[endgroup]\nError: actual error\nPost job cleanup.\n##[group]teardown\nsecret cleanup\n",
			wantSub: []string{"Error: actual error"},
			notSub:  []string{"##[group]", "checkout", "secret cleanup", "Post job cleanup."},
		},
		{
			name: "preserves high-signal lines outside 25-line window and deduplicates",
			rawLog: func() string {
				var sb strings.Builder
				sb.WriteString("--- FAIL: TestAlpha (0.01s)\n")
				sb.WriteString("pkg/router/main.go:42: undefined variable\n")
				sb.WriteString("panic: nil pointer dereference\n")
				sb.WriteString("FAIL\tpkg/router\t0.05s\n")
				sb.WriteString("🚨 alert trigger\n")
				sb.WriteString("##[error]Process failed\n")
				for i := 1; i <= 30; i++ {
					sb.WriteString(fmt.Sprintf("regular build log step %d\n", i))
				}
				sb.WriteString("regular build log step 30\n")
				return sb.String()
			}(),
			wantSub: []string{
				"--- FAIL: TestAlpha (0.01s)",
				"pkg/router/main.go:42: undefined variable",
				"panic: nil pointer dereference",
				"FAIL\tpkg/router\t0.05s",
				"🚨 alert trigger",
				"##[error]Process failed",
				"regular build log step 30",
			},
			notSub: []string{
				"regular build log step 1\n",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExtractCIFailureDetails(tc.rawLog)
			for _, sub := range tc.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("ExtractCIFailureDetails() missing expected %q in:\n%s", sub, got)
				}
			}
			for _, sub := range tc.notSub {
				if strings.Contains(got, sub) {
					t.Errorf("ExtractCIFailureDetails() unexpectedly contains %q in:\n%s", sub, got)
				}
			}
		})
	}
}

func TestFetchCheckJobFailureLogs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/jobs/404/logs") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.Contains(r.URL.Path, "/jobs/500/logs") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(r.URL.Path, "/jobs/100/logs") {
			auth := r.Header.Get("Authorization")
			if auth != "token test-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte("2026-10-22T21:40:12Z Error: build step 1 failed\n"))
			return
		}
		if strings.Contains(r.URL.Path, "/jobs/200/logs") {
			prefix := strings.Repeat("A\n", 150000)
			suffix := "Error: tail failure captured\n"
			_, _ = w.Write([]byte(prefix + suffix))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(mockServer.Close)

	router := newTestRouter(Config{
		GitHubAPIURL: mockServer.URL,
		GitHubToken:  "test-token",
	}, mockServer.Client(), &mockPRRegistry{}, &mockOutboundDispatcher{})

	tests := []struct {
		name    string
		srv     *RouterServer
		repo    string
		jobID   int64
		wantSub string
		wantNil bool
	}{
		{name: "nil server guard", srv: nil, repo: "azylman/aerial", jobID: 100, wantNil: true},
		{name: "nil http client guard", srv: &RouterServer{}, repo: "azylman/aerial", jobID: 100, wantNil: true},
		{name: "empty repo guard", srv: router, repo: "", jobID: 100, wantNil: true},
		{name: "invalid job id <= 0", srv: router, repo: "azylman/aerial", jobID: 0, wantNil: true},
		{name: "negative job id", srv: router, repo: "azylman/aerial", jobID: -5, wantNil: true},
		{name: "http 404", srv: router, repo: "azylman/aerial", jobID: 404, wantNil: true},
		{name: "http 500", srv: router, repo: "azylman/aerial", jobID: 500, wantNil: true},
		{name: "successful fetch", srv: router, repo: "azylman/aerial", jobID: 100, wantSub: "Error: build step 1 failed"},
		{name: "large log >256KB tail slice", srv: router, repo: "azylman/aerial", jobID: 200, wantSub: "Error: tail failure captured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.srv.fetchCheckJobFailureLogs(ctx, tc.repo, tc.jobID)
			if tc.wantNil && got != "" {
				t.Fatalf("fetchCheckJobFailureLogs() = %q, want empty", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("fetchCheckJobFailureLogs() = %q, want substring %q", got, tc.wantSub)
			}
		})
	}
}

func TestFetchLatestRoboRevComment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/issues/10/comments") {
			_, _ = w.Write([]byte(`[
				{"id": 1, "body": "regular comment"},
				{"id": 2, "body": "## roborev: Review Complete (` + "`sha1111`" + `)\nold report"},
				{"id": 3, "body": "## roborev: Combined Review (` + "`sha2222`" + `)\nfound issue"},
				{"id": 4, "body": "## roborev: Combined Review (` + "`fullsha3333333333`" + `)\nfull sha match"},
				{"id": 5, "body": "## roborev: Review Passed (` + "`pass444`" + `)\nNo issues found."}
			]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(mockServer.Close)

	router := newTestRouter(Config{GitHubAPIURL: mockServer.URL}, mockServer.Client(), &mockPRRegistry{}, &mockOutboundDispatcher{})
	router.nonRoboRevFailedSHAs.Store(prCheckKey("azylman/aerial", 10), "blockedsha555")

	tests := []struct {
		name           string
		srv            *RouterServer
		repo           string
		prNumber       int
		headSHA        string
		wantSub        string
		wantCanRecover bool
	}{
		{name: "nil server guard", srv: nil, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222", wantSub: "", wantCanRecover: false},
		{name: "empty repo guard", srv: router, repo: "", prNumber: 10, headSHA: "sha2222", wantSub: "", wantCanRecover: false},
		{name: "invalid prNumber guard", srv: router, repo: "azylman/aerial", prNumber: 0, headSHA: "sha2222", wantSub: "", wantCanRecover: false},
		{name: "github api 404", srv: router, repo: "azylman/aerial", prNumber: 99, headSHA: "sha2222", wantSub: "", wantCanRecover: false},
		{name: "matching short sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222extra", wantSub: "found issue", wantCanRecover: true},
		{name: "matching full sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "fullsha3333333333", wantSub: "full sha match", wantCanRecover: true},
		{name: "review passed comment does not trigger recovery", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "pass444", wantSub: "Review Passed", wantCanRecover: false},
		{name: "blocked by non-roborev failure on same sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "blockedsha555", wantSub: "", wantCanRecover: false},
		{name: "no matching sha", srv: router, repo: "azylman/aerial", prNumber: 10, headSHA: "nomatchsha", wantSub: "", wantCanRecover: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.srv.fetchLatestRoboRevComment(ctx, tc.repo, tc.prNumber, tc.headSHA)
			if tc.wantSub == "" && got != "" {
				t.Fatalf("fetchLatestRoboRevComment got %q, want empty", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("fetchLatestRoboRevComment got %q, want substring %q", got, tc.wantSub)
			}
			if gotRecover := tc.srv.canRecoverRoboRevSuccess(ctx, tc.repo, tc.prNumber, tc.headSHA); gotRecover != tc.wantCanRecover {
				t.Fatalf("canRecoverRoboRevSuccess got %v, want %v", gotRecover, tc.wantCanRecover)
			}
		})
	}
}

func TestCompleteCIFailureTransition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/issues/10/comments") {
			_, _ = w.Write([]byte(`[
				{"id": 1, "body": "## roborev: Combined Review (` + "`sha1111`" + `)\nroborev defect detected"}
			]`))
			return
		}
		if strings.Contains(r.URL.Path, "/jobs/555/logs") {
			_, _ = w.Write([]byte("2026-10-22T21:40:12Z Error: fetched job failure log\n"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(mockServer.Close)

	tests := []struct {
		name           string
		res            *ResolvedGitHubEvent
		details        string
		checkURL       string
		ciFailCount    int
		regBranch      string
		expectPrompt   bool
		wantSubPrompt  []string
		wantExtraEmpty bool
	}{
		{
			name:         "nil event guard",
			res:          nil,
			details:      "error",
			checkURL:     "http://example.com",
			expectPrompt: false,
		},
		{
			name: "roborev check comment extraction",
			res: &ResolvedGitHubEvent{
				Repo:      "azylman/aerial",
				PRNumber:  10,
				HeadSHA:   "sha1111",
				CheckName: "RoboRev Code Review",
				TargetID:  "1555405874565091380",
			},
			details:       "Review Failed",
			checkURL:      "http://example.com",
			ciFailCount:   1,
			expectPrompt:  true,
			wantSubPrompt: []string{"roborev defect detected", "/share/aerial/scripts/aerial-pr.sh init aerial --branch <branch>", "/share/aerial/scripts/aerial-pr.sh submit <scratch_dir>"},
		},
		{
			name: "job logs fetched on short details and branch resolved via registry",
			res: &ResolvedGitHubEvent{
				Repo:      "azylman/aerial",
				PRNumber:  42,
				HeadSHA:   "sha2222",
				CheckName: "CI / test",
				TargetID:  "1555405874565091380",
				Branch:    "",
			},
			details:       "Failed",
			checkURL:      "https://github.com/azylman/aerial/actions/runs/123/job/555",
			ciFailCount:   1,
			regBranch:     "refs/heads/feature-tail-logs",
			expectPrompt:  true,
			wantSubPrompt: []string{"fetched job failure log", "Branch: feature-tail-logs", "Check URL: https://github.com/azylman/aerial/actions/runs/123/job/555", "/share/aerial/scripts/aerial-pr.sh init aerial --branch feature-tail-logs", "/share/aerial/scripts/aerial-pr.sh submit <scratch_dir>"},
		},
		{
			name: "remediation cap reached pauses fix loop",
			res: &ResolvedGitHubEvent{
				Repo:      "azylman/aerial",
				PRNumber:  42,
				HeadSHA:   "sha2222",
				CheckName: "CI / test",
				TargetID:  "1555405874565091380",
			},
			details:      "Persistent failure",
			checkURL:     "http://example.com",
			ciFailCount:  maxCIRemediationAttempts + 1,
			expectPrompt: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dispatcher := &mockOutboundDispatcher{}
			reg := &mockPRRegistry{
				ciFailCount:   tc.ciFailCount,
				resolveBranch: tc.regBranch,
			}
			router := newTestRouter(Config{
				GitHubAPIURL: mockServer.URL,
			}, mockServer.Client(), reg, dispatcher)

			err := router.completeCIFailureTransition(ctx, tc.res, tc.details, tc.checkURL)
			if err != nil {
				t.Fatalf("completeCIFailureTransition() error: %v", err)
			}

			if tc.expectPrompt {
				if len(dispatcher.dispatchCalls) == 0 {
					t.Fatalf("expected dispatchCall, got none")
				}
				prompt := dispatcher.dispatchCalls[0].Prompt
				for _, sub := range tc.wantSubPrompt {
					if !strings.Contains(prompt, sub) {
						t.Errorf("prompt missing %q:\n%s", sub, prompt)
					}
				}
			} else {
				if len(dispatcher.dispatchCalls) > 0 {
					t.Fatalf("unexpected dispatchCall: %v", dispatcher.dispatchCalls)
				}
			}
		})
	}
}
