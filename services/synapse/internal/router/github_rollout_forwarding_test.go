package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type ownedMapRegistry struct {
	mockPRRegistry

	ownedPRs  map[int]bool
	ownedSHAs map[string]int
}

func (r *ownedMapRegistry) ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (string, string, string, string, error) {
	if err := r.checkErr(r.resolveErr); err != nil {
		return "", "", "", "", err
	}
	if r.ownedPRs != nil && r.ownedPRs[prNumber] {
		return "100000000000000001", "feat/test", "sha123", "", nil
	}
	return "", "", "", "", nil
}

func (r *ownedMapRegistry) ResolvePRBySHA(ctx context.Context, repo string, sha string) (int, string, string, error) {
	if err := r.checkErr(r.resolveErr); err != nil {
		return 0, "", "", err
	}
	if r.ownedSHAs != nil {
		if prNum, ok := r.ownedSHAs[sha]; ok {
			return prNum, "feat/test", "100000000000000001", nil
		}
	}
	return 0, "", "", nil
}

// trailerScopingCase is one handlePullRequestRegistryAction row.
type trailerScopingCase struct {
	name             string
	botID            string
	bots             []string
	defaultOwner     bool
	prBody           string
	action           string
	merged           bool
	wantRegistered   bool
	wantEnsureAgents []string
	wantSyncHeads    []string
	wantClosed       bool
	wantEvents       []wantPREvent
}

func trailerScopingCases() []trailerScopingCase {
	return []trailerScopingCase{
		{
			name:             "matching bot_id registered",
			botID:            "aerial",
			defaultOwner:     false,
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			wantRegistered:   true,
			wantEnsureAgents: []string{"aerial"},
		},
		{
			name:             "case-insensitive matching bot_id registered",
			botID:            "aerial",
			defaultOwner:     false,
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"AERIAL\"} -->",
			wantRegistered:   true,
			wantEnsureAgents: []string{"AERIAL"},
		},
		{
			name:           "peer bot_id skipped",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"coven\"} -->",
			wantRegistered: false,
		},
		{
			name:           "peer bot_id synchronize skips sync and lifecycle events",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"coven\"} -->",
			action:         "synchronize",
			wantRegistered: false,
		},
		{
			name:           "peer bot_id closed skips closed transition",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"coven\"} -->",
			action:         "closed",
			merged:         false,
			wantRegistered: false,
			wantClosed:     false,
		},
		{
			name:             "untagged registered when default owner",
			botID:            "aerial",
			defaultOwner:     true,
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\"} -->",
			wantRegistered:   true,
			wantEnsureAgents: []string{""},
		},
		{
			name:           "untagged skipped when not default owner",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\"} -->",
			wantRegistered: false,
		},
		{
			name:           "closed unmerged pr skips registration and marks pr closed",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			action:         "closed",
			merged:         false,
			wantRegistered: false,
			wantClosed:     true,
			wantEvents:     []wantPREvent{{Event: "closed_unmerged", PRNumber: 101, HeadSHA: "sha101", Status: "closed_unmerged"}},
		},
		{
			name:           "untagged synchronize skips registration and publishes synchronize",
			botID:          "aerial",
			defaultOwner:   false,
			action:         "synchronize",
			wantRegistered: false,
			wantSyncHeads:  []string{"sha101"},
			wantEvents:     []wantPREvent{{Event: "synchronize", PRNumber: 101, HeadSHA: "sha101", Status: "open", Title: "Test PR"}},
		},
		{
			name:             "sibling miorine synchronize processed with configured bots",
			botID:            "aerial",
			bots:             []string{"aerial", "miorine", "suletta"},
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"miorine\"} -->",
			action:           "synchronize",
			wantRegistered:   true,
			wantEnsureAgents: []string{"miorine"},
			wantSyncHeads:    []string{"sha101"},
			wantEvents:       []wantPREvent{{Event: "synchronize", PRNumber: 101, HeadSHA: "sha101", Status: "open", Title: "Test PR"}},
		},
		{
			name:       "sibling suletta closed unmerged processed with configured bots",
			botID:      "aerial",
			bots:       []string{"aerial", "miorine", "suletta"},
			prBody:     "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"suletta\"} -->",
			action:     "closed",
			wantClosed: true,
			wantEvents: []wantPREvent{{Event: "closed_unmerged", PRNumber: 101, HeadSHA: "sha101", Status: "closed_unmerged"}},
		},
		{
			name:             "sibling suletta merged processed with configured bots",
			botID:            "aerial",
			bots:             []string{"aerial", "miorine", "suletta"},
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"suletta\"} -->",
			action:           "closed",
			merged:           true,
			wantRegistered:   true,
			wantEnsureAgents: []string{"suletta"},
			wantEvents:       []wantPREvent{{Event: "merged", PRNumber: 101, HeadSHA: "sha101", Status: "merged"}},
		},
		{
			name:             "default bot aerial synchronize processed with configured bots",
			botID:            "aerial",
			bots:             []string{"aerial", "miorine", "suletta"},
			prBody:           "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			action:           "synchronize",
			wantRegistered:   true,
			wantEnsureAgents: []string{"aerial"},
			wantSyncHeads:    []string{"sha101"},
			wantEvents:       []wantPREvent{{Event: "synchronize", PRNumber: 101, HeadSHA: "sha101", Status: "open", Title: "Test PR"}},
		},
		{
			name:   "bot outside configured bots synchronize dropped",
			botID:  "aerial",
			bots:   []string{"aerial", "miorine", "suletta"},
			prBody: "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"gundam\"} -->",
			action: "synchronize",
		},
	}
}

func TestHandlePullRequestRegistryAction_TrailerScoping(t *testing.T) {
	t.Parallel()

	tests := trailerScopingCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{}
			cfg := Config{
				BotID:        tc.botID,
				DefaultOwner: tc.defaultOwner,
			}
			if len(tc.bots) > 0 {
				cfg.Bots = map[string]BotConfig{}
				for _, b := range tc.bots {
					cfg.Bots[b] = BotConfig{}
				}
			}
			bus := &mockEventBus{}
			srv := newTestRouter(cfg, nil, reg, nil, bus)
			action := tc.action
			if action == "" {
				action = "opened"
			}
			res := &ResolvedGitHubEvent{
				Repo:     "azylman/aerial",
				PRNumber: 101,
				Branch:   "feat/something",
				HeadSHA:  "sha101",
				Action:   action,
			}
			pr := GitHubPullRequest{
				Number: 101,
				Title:  "Test PR",
				Body:   tc.prBody,
				Merged: tc.merged,
			}

			err := srv.handlePullRequestRegistryAction(context.Background(), res, pr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRegistryActionCalls(t, reg, tc.wantRegistered, tc.wantClosed, tc.wantEnsureAgents, tc.wantSyncHeads)
			assertPREvents(t, bus, tc.wantEvents)
		})
	}
}

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

// prCallHeads lists the head SHA of each recorded registry call.
func prCallHeads(calls []prCall) []string {
	heads := make([]string, 0, len(calls))
	for _, c := range calls {
		heads = append(heads, c.headSHA)
	}
	return heads
}

// assertRegistryActionCalls checks the registry writes one
// handlePullRequestRegistryAction call made.
func assertRegistryActionCalls(t *testing.T, reg *mockPRRegistry, wantRegistered, wantClosed bool, wantEnsureAgents, wantSyncHeads []string) {
	t.Helper()
	if got := len(reg.ensureCalls) > 0; got != wantRegistered {
		t.Errorf("EnsurePRRegistered called = %v (%d calls), want %v", got, len(reg.ensureCalls), wantRegistered)
	}
	if got := len(reg.closedUnmergedCalls) > 0; got != wantClosed {
		t.Errorf("UpdatePRClosedUnmerged called = %v (%d calls), want %v", got, len(reg.closedUnmergedCalls), wantClosed)
	}
	if !reflect.DeepEqual(reg.ensureAgents, wantEnsureAgents) {
		t.Errorf("EnsurePRRegistered agents = %v, want %v", reg.ensureAgents, wantEnsureAgents)
	}
	if got := prCallHeads(reg.syncCalls); len(got)+len(wantSyncHeads) > 0 && !reflect.DeepEqual(got, wantSyncHeads) {
		t.Errorf("UpdatePRSync heads = %v, want %v", got, wantSyncHeads)
	}
}
