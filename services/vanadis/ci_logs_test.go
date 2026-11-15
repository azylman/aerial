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

func TestIsRoboRevCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		check string
		want  bool
	}{
		{name: "exact roborev", check: "roborev", want: true},
		{name: "uppercase ROBOREV", check: "ROBOREV", want: true},
		{name: "mixed case RoboRev", check: "RoboRev", want: true},
		{name: "RoboRev Code Review", check: "RoboRev Code Review", want: true},
		{name: "trimmed spaces", check: "  roborev  ", want: true},
		{name: "different check", check: "unit tests", want: false},
		{name: "empty string", check: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isRoboRevCheck(tc.check)
			if got != tc.want {
				t.Fatalf("isRoboRevCheck(%q) = %v, want %v", tc.check, got, tc.want)
			}
		})
	}
}

func TestIsGateAggregatorCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		check string
		want  bool
	}{
		{name: "ci gate exact", check: "ci gate", want: true},
		{name: "CI Gate uppercase", check: "CI Gate", want: true},
		{name: "custom gate suffix", check: "quality gate", want: true},
		{name: "arbitrary check", check: "lint", want: false},
		{name: "empty string", check: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isGateAggregatorCheck(tc.check)
			if got != tc.want {
				t.Fatalf("isGateAggregatorCheck(%q) = %v, want %v", tc.check, got, tc.want)
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
					fmt.Fprintf(&sb, "regular build log step %d\n", i)
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

	server := NewVanadisServer(Config{
		GitHubAPIURL: mockServer.URL,
		GitHubToken:  "test-token",
	}, nil, mockServer.Client())

	tests := []struct {
		name    string
		srv     *VanadisServer
		repo    string
		jobID   int64
		wantSub string
		wantNil bool
	}{
		{name: "nil server guard", srv: nil, repo: "azylman/aerial", jobID: 100, wantNil: true},
		{name: "nil http client guard", srv: &VanadisServer{}, repo: "azylman/aerial", jobID: 100, wantNil: true},
		{name: "empty repo guard", srv: server, repo: "", jobID: 100, wantNil: true},
		{name: "invalid job id <= 0", srv: server, repo: "azylman/aerial", jobID: 0, wantNil: true},
		{name: "negative job id", srv: server, repo: "azylman/aerial", jobID: -5, wantNil: true},
		{name: "http 404", srv: server, repo: "azylman/aerial", jobID: 404, wantNil: true},
		{name: "successful fetch", srv: server, repo: "azylman/aerial", jobID: 100, wantSub: "Error: build step 1 failed"},
		{name: "large log >256KB tail slice", srv: server, repo: "azylman/aerial", jobID: 200, wantSub: "Error: tail failure captured"},
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

	server := NewVanadisServer(Config{GitHubAPIURL: mockServer.URL}, nil, mockServer.Client())

	tests := []struct {
		name     string
		srv      *VanadisServer
		repo     string
		prNumber int
		headSHA  string
		wantSub  string
	}{
		{name: "nil server guard", srv: nil, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222", wantSub: ""},
		{name: "empty repo guard", srv: server, repo: "", prNumber: 10, headSHA: "sha2222", wantSub: ""},
		{name: "invalid prNumber guard", srv: server, repo: "azylman/aerial", prNumber: 0, headSHA: "sha2222", wantSub: ""},
		{name: "github api 404", srv: server, repo: "azylman/aerial", prNumber: 99, headSHA: "sha2222", wantSub: ""},
		{name: "matching short sha", srv: server, repo: "azylman/aerial", prNumber: 10, headSHA: "sha2222extra", wantSub: "found issue"},
		{name: "matching full sha", srv: server, repo: "azylman/aerial", prNumber: 10, headSHA: "fullsha3333333333", wantSub: "full sha match"},
		{name: "review passed comment matches", srv: server, repo: "azylman/aerial", prNumber: 10, headSHA: "pass444", wantSub: "No issues found."},
		{name: "unmatched sha returns empty", srv: server, repo: "azylman/aerial", prNumber: 10, headSHA: "unknownsha", wantSub: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.srv.fetchLatestRoboRevComment(ctx, tc.repo, tc.prNumber, tc.headSHA)
			if tc.wantSub == "" && got != "" {
				t.Fatalf("fetchLatestRoboRevComment() = %q, want empty", got)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("fetchLatestRoboRevComment() = %q, want substring %q", got, tc.wantSub)
			}
		})
	}
}

func TestResolveCIFailureDetails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/issues/10/comments") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id": 1, "body": "## roborev: (` + "`sha1`" + `)\nroborev issue"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/jobs/555/logs") {
			_, _ = w.Write([]byte("Error: job log error\n"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(mockServer.Close)

	server := NewVanadisServer(Config{GitHubAPIURL: mockServer.URL}, nil, mockServer.Client())

	tests := []struct {
		name      string
		repo      string
		prNum     int
		checkName string
		headSHA   string
		checkURL  string
		details   string
		wantSub   string
	}{
		{
			name:      "roborev check fetches comment",
			repo:      "azylman/aerial",
			prNum:     10,
			checkName: "roborev",
			headSHA:   "sha1",
			details:   "Failed",
			wantSub:   "roborev issue",
		},
		{
			name:      "actions check with short details fetches job log",
			repo:      "azylman/aerial",
			prNum:     10,
			checkName: "unit-tests",
			checkURL:  "https://github.com/azylman/aerial/actions/runs/1/job/555",
			details:   "Failed",
			wantSub:   "Error: job log error",
		},
		{
			name:      "long details keeps existing details without fetch",
			repo:      "azylman/aerial",
			prNum:     10,
			checkName: "unit-tests",
			details:   "Detailed failure:\nstep failed with exit code 1",
			wantSub:   "Detailed failure:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := server.resolveCIFailureDetails(ctx, tc.repo, tc.prNum, tc.checkName, tc.headSHA, tc.checkURL, tc.details)
			if !strings.Contains(got, tc.wantSub) {
				t.Fatalf("resolveCIFailureDetails() = %q, want substring %q", got, tc.wantSub)
			}
		})
	}
}
