package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsRoboRevCheck(t *testing.T) {
	t.Parallel()
	for _, check := range []string{"roborev", "ROBOREV", "RoboRev Code Review", "  roborev  "} {
		if !IsRoboRevCheck(check) {
			t.Errorf("IsRoboRevCheck(%q) = false, want true", check)
		}
	}
	for _, check := range []string{"unit-tests", "", "  "} {
		if IsRoboRevCheck(check) {
			t.Errorf("IsRoboRevCheck(%q) = true, want false", check)
		}
	}
}

func TestIsGateAggregatorCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		checkName string
		want      bool
	}{
		{name: "ci gate exact", checkName: "ci gate", want: true},
		{name: "ci gate uppercase", checkName: "CI Gate", want: true},
		{name: "pr gate suffix", checkName: "pr gate", want: true},
		{name: "build check", checkName: "build-image", want: false},
		{name: "gate prefix only", checkName: "gatekeeper", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsGateAggregatorCheck(tc.checkName)
			if got != tc.want {
				t.Errorf("IsGateAggregatorCheck(%q) = %v, want %v", tc.checkName, got, tc.want)
			}
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
		{name: "completed success", status: "completed", conclusion: "success", want: StatusPassed},
		{name: "completed neutral", status: "completed", conclusion: "neutral", want: StatusPassed},
		{name: "completed skipped", status: "completed", conclusion: "skipped", want: StatusPassed},
		{name: "completed failure", status: "completed", conclusion: "failure", want: StatusFailed},
		{name: "completed timed_out", status: "completed", conclusion: "timed_out", want: StatusFailed},
		{name: "completed cancelled", status: "completed", conclusion: "cancelled", want: StatusFailed},
		{name: "completed action_required", status: "completed", conclusion: "action_required", want: StatusFailed},
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued status", status: "queued", conclusion: "", want: StatusPending},
		{name: "in_progress status", status: "in_progress", conclusion: "", want: StatusPending},
		{name: "waiting status", status: "waiting", conclusion: "", want: StatusPending},
		{name: "requested status", status: "requested", conclusion: "", want: StatusPending},
		{name: "pending status", status: "pending", conclusion: "", want: StatusPending},
		{name: "other non-empty status", status: "running", conclusion: "", want: StatusPending},
		{name: "empty status", status: "", conclusion: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyCheckState(tc.status, tc.conclusion)
			if got != tc.want {
				t.Errorf("ClassifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}

func TestExtractAutoMergeBlockReason(t *testing.T) {
	t.Parallel()
	bFalse := false
	bTrue := true
	tests := []struct {
		name      string
		state     string
		mergeable *bool
		want      string
	}{
		{name: "dirty state", state: "dirty", mergeable: nil, want: "merge_conflict"},
		{name: "mergeable false", state: "clean", mergeable: &bFalse, want: "merge_conflict"},
		{name: "blocked state", state: "blocked", mergeable: &bTrue, want: "checks_or_reviews_blocked"},
		{name: "behind state", state: "behind", mergeable: &bTrue, want: "branch_behind_base"},
		{name: "clean state returns empty", state: "clean", mergeable: &bTrue, want: ""},
		{name: "unknown other state", state: "draft", mergeable: &bTrue, want: "draft"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExtractAutoMergeBlockReason(tc.state, tc.mergeable)
			if got != tc.want {
				t.Errorf("ExtractAutoMergeBlockReason(%q, %v) = %q, want %q", tc.state, tc.mergeable, got, tc.want)
			}
		})
	}
}

func TestIsActionableAutoMergeBlock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		reason string
		want   bool
	}{
		{name: "merge_conflict", reason: "merge_conflict", want: true},
		{name: "dirty", reason: "dirty", want: true},
		{name: "contains conflict", reason: "has merge conflicts with main", want: true},
		{name: "branch_behind_base", reason: "branch_behind_base", want: true},
		{name: "checks_or_reviews_blocked", reason: "checks_or_reviews_blocked", want: false},
		{name: "empty", reason: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsActionableAutoMergeBlock(tc.reason)
			if got != tc.want {
				t.Errorf("IsActionableAutoMergeBlock(%q) = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

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

func TestFormatPromptsAndCollaboratorMessages(t *testing.T) {
	t.Parallel()
	checkPrompt := FormatCheckFailurePrompt("azylman/aerial", 42, "headsha", "test-unit", "details text", "https://check.url")
	if !strings.Contains(checkPrompt, "test-unit") || !strings.Contains(checkPrompt, "https://check.url") {
		t.Errorf("unexpected check failure prompt: %s", checkPrompt)
	}
	emptyCheckPrompt := FormatCheckFailurePrompt("azylman/aerial", 42, "headsha", "test-unit", "", "")
	if strings.Contains(emptyCheckPrompt, "Details:") {
		t.Errorf("expected no details block for empty details")
	}

	rrPrompt := FormatRoboRevPrompt("azylman/aerial", 42, "headsha", "review comments")
	if !strings.Contains(rrPrompt, "RoboRev review failed") || !strings.Contains(rrPrompt, "review comments") {
		t.Errorf("unexpected roborev prompt: %s", rrPrompt)
	}
	emptyRRPrompt := FormatRoboRevPrompt("azylman/aerial", 42, "headsha", "")
	if strings.Contains(emptyRRPrompt, "Review Findings:") {
		t.Errorf("expected no findings block for empty comments")
	}

	alertMsg := BuildCollaboratorAlertMessage("@collaborator", "azylman/aerial", 42, "PR Title", "feat/branch", "headsha123", "lint", "https://url")
	if !strings.Contains(alertMsg, "@collaborator") || !strings.Contains(alertMsg, "PR Title") || !strings.Contains(alertMsg, "lint") {
		t.Errorf("unexpected alert message: %s", alertMsg)
	}

	// Empty mention and empty checkURL and empty checkName
	alertNoMention := BuildCollaboratorAlertMessage("", "azylman/aerial", 42, "", "", "", "", "")
	if strings.Contains(alertNoMention, "@") || strings.Contains(alertNoMention, "Check URL:") {
		t.Errorf("unexpected alert without mention: %s", alertNoMention)
	}

	conflictMsg := BuildCollaboratorConflictMessage("@collaborator", "azylman/aerial", 42, "PR Title", "feat/branch", "headsha123")
	if !strings.Contains(conflictMsg, "@collaborator") || !strings.Contains(conflictMsg, "Merge conflict with `main`") {
		t.Errorf("unexpected conflict message: %s", conflictMsg)
	}

	conflictNoMention := BuildCollaboratorConflictMessage("", "", 42, "", "", "")
	if !strings.Contains(conflictNoMention, "PR #42") {
		t.Errorf("unexpected conflict message without mention: %s", conflictNoMention)
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

func TestClient_RequestsAndSuccess(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":123,"body":"hello"}`))
	}))
	defer srv.Close()

	client := NewClient(srv.Client(), srv.URL, "token123")
	var comment GitHubIssueComment
	err := client.GetJSON(context.Background(), "/ok", 1<<20, &comment)
	if err != nil || comment.ID != 123 {
		t.Fatalf("expected successful GetJSON, got (%v, %+v)", err, comment)
	}

	// maxBytes <= 0 defaults to 10MB
	errDef := client.GetJSON(context.Background(), "/ok", 0, &comment)
	if errDef != nil {
		t.Errorf("expected maxBytes <= 0 to default safely: %v", errDef)
	}

	// Nil client handling
	var nilClient *Client
	nilReq, errNilReq := nilClient.NewRequest(context.Background(), http.MethodGet, "/path", nil)
	if errNilReq != nil || nilReq.URL.Host != "api.github.com" {
		t.Errorf("expected nilClient.NewRequest to fall back to default api.github.com: (%v, %v)", nilReq, errNilReq)
	}

	// Client with nil HTTPClient falls back to http.DefaultClient
	fallbackClient := NewClient(nil, srv.URL, "token")
	errFallback := fallbackClient.GetJSON(context.Background(), "/ok", 1<<20, &comment)
	if errFallback != nil {
		t.Errorf("expected fallback to default http client: %v", errFallback)
	}
}

func TestClient_ErrorsAndEdgeCases(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/malformed":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{not valid json`))
		case "/404":
			w.WriteHeader(http.StatusNotFound)
		case "/403":
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.Client(), srv.URL, "token123")
	var comment GitHubIssueComment

	// Network error on closed server
	closedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closedSrv.URL
	badClient := NewClient(closedSrv.Client(), closedURL, "tok")
	closedSrv.Close()
	if errNet := badClient.GetJSON(context.Background(), "/anything", 1<<20, &comment); errNet == nil {
		t.Errorf("expected network error on closed server")
	}

	errMal := client.GetJSON(context.Background(), "/malformed", 1<<20, &comment)
	if errMal == nil || !strings.Contains(errMal.Error(), "unmarshal") {
		t.Errorf("expected unmarshal error for malformed json, got %v", errMal)
	}

	err404 := client.GetJSON(context.Background(), "/404", 1<<20, &comment)
	var notFound *StatusError
	if !errors.As(err404, &notFound) || notFound.StatusCode != http.StatusNotFound {
		t.Errorf("expected StatusError 404, got %v", err404)
	}

	err403 := client.GetJSON(context.Background(), "/403", 1<<20, &comment)
	var statusErr *StatusError
	if !errors.As(err403, &statusErr) || statusErr.StatusCode != http.StatusForbidden {
		t.Errorf("expected StatusError 403, got %v", err403)
	}

	err500 := client.GetJSON(context.Background(), "/500", 1<<20, &comment)
	if err500 == nil || !strings.Contains(err500.Error(), "status 500") {
		t.Errorf("expected status 500 error, got %v", err500)
	}

	// NewRequest with invalid method
	_, errBadMethod := NewRequest(context.Background(), "", "", "INVALID METHOD \x7f", "/path", nil)
	if errBadMethod == nil {
		t.Errorf("expected error for invalid HTTP method")
	}
}

func TestConflicts_FetchSinglePR(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/azylman/aerial/pulls/42":
			_, _ = w.Write([]byte(`{"number":42,"state":"open","title":"Test PR","head":{"ref":"feat/branch","sha":"head42"}}`))
		case "/repos/azylman/aerial/pulls/43":
			// PR with unknown mergeable state
			_, _ = w.Write([]byte(`{"number":43,"state":"open","mergeable":null,"mergeable_state":"unknown"}`))
		case "/repos/azylman/aerial/pulls/44":
			// PR with mergeable=true
			_, _ = w.Write([]byte(`{"number":44,"state":"open","mergeable":true,"mergeable_state":"clean"}`))
		case "/repos/azylman/aerial/pulls/45":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.Client(), srv.URL, "token")

	// FetchSinglePR guards
	if _, err := FetchSinglePR(context.Background(), client, "", 42); err == nil {
		t.Errorf("expected error for empty repo in FetchSinglePR")
	}
	if _, err := FetchSinglePR(context.Background(), client, "azylman/aerial", 0); err == nil {
		t.Errorf("expected error for zero pr in FetchSinglePR")
	}
	if _, err := FetchSinglePR(context.Background(), nil, "azylman/aerial", 42); err == nil {
		t.Errorf("expected error for nil client in FetchSinglePR")
	}

	// FetchSinglePR
	pr, err := FetchSinglePR(context.Background(), client, "azylman/aerial", 42)
	if err != nil || pr.Number != 42 {
		t.Fatalf("FetchSinglePR failed: %v, %+v", err, pr)
	}

	// FetchSinglePRWithMergeabilityRetry
	prRetry, errRetry := FetchSinglePRWithMergeabilityRetry(context.Background(), client, "azylman/aerial", 42, 2)
	if errRetry != nil || prRetry.Number != 42 {
		t.Fatalf("FetchSinglePRWithMergeabilityRetry failed: %v", errRetry)
	}
	// Fetch unknown mergeable state retries and completes
	prUnknown, errUnknown := FetchSinglePRWithMergeabilityRetry(context.Background(), client, "azylman/aerial", 43, 0)
	if errUnknown != nil || prUnknown.Number != 43 {
		t.Fatalf("FetchSinglePRWithMergeabilityRetry for unknown failed: %v", errUnknown)
	}
	// Fetch clean mergeable PR immediately succeeds without retrying
	prClean, errClean := FetchSinglePRWithMergeabilityRetry(context.Background(), client, "azylman/aerial", 44, 2)
	if errClean != nil || prClean.Number != 44 {
		t.Fatalf("FetchSinglePRWithMergeabilityRetry for clean PR failed: %v", errClean)
	}
	// Fetch failing PR returns fetch error
	if _, errFail := FetchSinglePRWithMergeabilityRetry(context.Background(), client, "azylman/aerial", 45, 2); errFail == nil {
		t.Errorf("expected error on failing PR in FetchSinglePRWithMergeabilityRetry")
	}
	// Context cancelled returns error
	ctxC, cancelC := context.WithCancel(context.Background())
	cancelC()
	if _, errC := FetchSinglePRWithMergeabilityRetry(ctxC, client, "azylman/aerial", 43, 2); errC == nil {
		t.Errorf("expected context cancellation error")
	}
}

func TestConflicts_ResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/azylman/aerial/pulls":
			_, _ = w.Write([]byte(`[
				{"number":41,"state":"closed","title":"Closed PR","head":{"ref":"feat/branch","sha":"head-fallback"}},
				{"number":42,"state":"open","title":"Test PR","head":{"ref":"feat/branch","sha":"head42"}}
			]`))
		case "/repos/azylman/aerial/commits/head42/pulls":
			_, _ = w.Write([]byte(`[
				{"number":40,"state":"open","head":{"ref":"feat/old","sha":"head42"}},
				{"number":42,"state":"closed","head":{"ref":"feat/branch","sha":"head42"}}
			]`))
		case "/repos/azylman/aerial/commits/head-fallback/pulls", "/repos/azylman/aerial/commits/head-empty/pulls":
			_, _ = w.Write([]byte(`[]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.Client(), srv.URL, "token")

	// ResolvePRFromCommitSHA guards
	if _, _, err := ResolvePRFromCommitSHA(context.Background(), client, "", "head42"); err == nil {
		t.Errorf("expected error for empty repo in ResolvePRFromCommitSHA")
	}
	if _, _, err := ResolvePRFromCommitSHA(context.Background(), client, "azylman/aerial", ""); err == nil {
		t.Errorf("expected error for empty sha in ResolvePRFromCommitSHA")
	}
	if _, _, err := ResolvePRFromCommitSHA(context.Background(), nil, "azylman/aerial", "head42"); err == nil {
		t.Errorf("expected error for nil client")
	}

	// ResolvePRFromCommitSHA: selects closed PR 42 over open PR 40
	prNum, branch, errResolve := ResolvePRFromCommitSHA(context.Background(), client, "azylman/aerial", "head42")
	if errResolve != nil || prNum != 42 || branch != "feat/branch" {
		t.Errorf("ResolvePRFromCommitSHA failed: (%d, %q, %v)", prNum, branch, errResolve)
	}

	// Fallback to open PRs list when pulls endpoint is empty (finds PR 41)
	prNumFb, branchFb, errFb := ResolvePRFromCommitSHA(context.Background(), client, "azylman/aerial", "head-fallback")
	if errFb != nil || prNumFb != 41 || branchFb != "feat/branch" {
		t.Errorf("expected open PRs fallback to find PR 41: (%d, %q, %v)", prNumFb, branchFb, errFb)
	}

	// When neither endpoint finds any PR
	if _, _, errEmpty := ResolvePRFromCommitSHA(context.Background(), client, "azylman/aerial", "head-empty"); errEmpty == nil {
		t.Errorf("expected error when no PRs associated with commit")
	}
}

func TestResolveCommitPR(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/azylman/aerial/commits/merge2327/pulls":
			_, _ = w.Write([]byte(`[{"number":2327,"state":"closed","head":{"ref":"feat/purge","sha":" head2327 "},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"merge2327"}]`))
		case "/repos/azylman/aerial/commits/head50/pulls":
			_, _ = w.Write([]byte(`[{"number":50,"state":"open","head":{"ref":"feat/open","sha":"head50"},"merged_at":null,"merge_commit_sha":"testmerge50"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	client := NewClient(srv.Client(), srv.URL, "token")
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
			got, err := ResolveCommitPR(context.Background(), client, "azylman/aerial", tc.sha)
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
