package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type resolvePRFromCommitSHATestCase struct {
	name          string
	repo          string
	sha           string
	apiURL        string
	statusCode    int
	responseBody  string
	openPRsBody   string
	wantErr       bool
	wantErrSubstr string
	wantPRNum     int
	wantBranch    string
}

func newResolvePRMockServer(tc resolvePRFromCommitSHATestCase) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/pulls") && strings.Contains(r.URL.RawQuery, "state=open") {
			if tc.openPRsBody != "" {
				_, _ = w.Write([]byte(tc.openPRsBody))
				return
			}
			_, _ = w.Write([]byte(`[]`))
			return
		}
		code := tc.statusCode
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(tc.responseBody))
	}))
}

func runResolvePRFromCommitSHACase(ctx context.Context, t *testing.T, tc resolvePRFromCommitSHATestCase) {
	t.Helper()
	t.Parallel()
	ts := newResolvePRMockServer(tc)
	defer ts.Close()

	apiURL := ts.URL
	if tc.apiURL != "" {
		apiURL = tc.apiURL
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubAPIURL: apiURL, GitHubToken: "secret"}), ts.Client())
	prNum, branch, err := srv.resolvePRFromCommitSHA(ctx, tc.repo, tc.sha)
	if (err != nil) != tc.wantErr {
		t.Fatalf("resolvePRFromCommitSHA() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
		t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
	}
	if prNum != tc.wantPRNum {
		t.Errorf("got prNum = %d, want %d", prNum, tc.wantPRNum)
	}
	if branch != tc.wantBranch {
		t.Errorf("got branch = %q, want %q", branch, tc.wantBranch)
	}
}

func TestResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range resolvePRFromCommitSHATestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runResolvePRFromCommitSHACase(ctx, t, tc)
		})
	}
}

func resolvePRFromCommitSHATestCases() []resolvePRFromCommitSHATestCase {
	return []resolvePRFromCommitSHATestCase{
		{
			name:          "empty repo",
			repo:          "",
			sha:           "sha123",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:    "invalid api url",
			repo:    "azylman/aerial",
			sha:     "sha123",
			apiURL:  "::bad-url::",
			wantErr: true,
		},
		{
			name:          "empty sha",
			repo:          "azylman/aerial",
			sha:           "",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "zero SHA",
			repo:          "azylman/aerial",
			sha:           "0000000000000000000000000000000000000000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "403 rate limit",
			repo:          "azylman/aerial",
			sha:           "sha403",
			statusCode:    http.StatusForbidden,
			responseBody:  `{"message":"API rate limit exceeded"}`,
			wantErr:       true,
			wantErrSubstr: "rate limit",
		},
		{
			name:          "404 not found",
			repo:          "azylman/aerial",
			sha:           "sha404",
			statusCode:    http.StatusNotFound,
			responseBody:  `{"message":"Not Found"}`,
			wantErr:       true,
			wantErrSubstr: "404",
		},
		{
			name:         "malformed JSON",
			repo:         "azylman/aerial",
			sha:          "shamalformed",
			statusCode:   http.StatusOK,
			responseBody: `{not-an-array`,
			wantErr:      true,
		},
		{
			name:          "empty PR list",
			repo:          "azylman/aerial",
			sha:           "shaempty",
			statusCode:    http.StatusOK,
			responseBody:  `[]`,
			wantErr:       true,
			wantErrSubstr: "no pull requests associated",
		},
		{
			name:         "closed-PR over open-PR priority",
			repo:         "azylman/aerial",
			sha:          "shaclosed",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"open","head":{"ref":"feat/open"}},{"number":20,"state":"closed","head":{"ref":"feat/closed"}}]`,
			wantPRNum:    20,
			wantBranch:   "feat/closed",
		},
		{
			name:         "higher-number selection priority when states match",
			repo:         "azylman/aerial",
			sha:          "shahigher",
			statusCode:   http.StatusOK,
			responseBody: `[{"number":10,"state":"closed","head":{"ref":"feat/ten"}},{"number":30,"state":"closed","head":{"ref":"feat/thirty"}},{"number":20,"state":"closed","head":{"ref":"feat/twenty"}}]`,
			wantPRNum:    30,
			wantBranch:   "feat/thirty",
		},
		{
			name:         "fork PR fallback scans open pulls when commits pulls returns empty",
			repo:         "azylman/aerial",
			sha:          "forksha123",
			statusCode:   http.StatusOK,
			responseBody: `[]`,
			openPRsBody:  `[{"number":456,"state":"open","head":{"ref":"fork-feature","sha":"forksha123"}},{"number":455,"state":"open","head":{"ref":"other","sha":"diffsha"}}]`,
			wantPRNum:    456,
			wantBranch:   "fork-feature",
		},
		{
			name:          "invalid commit sha characters return error",
			repo:          "azylman/aerial",
			sha:           "0000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "empty repo and sha returns invalid error",
			repo:          "",
			sha:           "",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "open PR fallback fails if both commits/pulls and pulls return empty",
			repo:          "azylman/aerial",
			sha:           "notfoundsha",
			statusCode:    http.StatusOK,
			responseBody:  `[]`,
			openPRsBody:   `[{"number":123,"state":"open","head":{"ref":"foo","sha":"other"}}]`,
			wantErr:       true,
			wantErrSubstr: "no pull requests associated",
		},
	}
}
