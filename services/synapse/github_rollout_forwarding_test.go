package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCommitSHAPRCacheHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nilServer  bool
		repo       string
		sha        string
		prNumber   int
		branch     string
		corruptKey string
		corruptVal any
		wantLookup bool
		wantPR     int
		wantBranch string
	}{
		{
			name:       "nil receiver returns false on lookup and no-ops on record",
			nilServer:  true,
			repo:       "azylman/aerial",
			sha:        "abcdef123456",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "empty repo is no-op",
			repo:       "",
			sha:        "abcdef123456",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "empty sha is no-op",
			repo:       "azylman/aerial",
			sha:        "",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "zero sha is no-op",
			repo:       "azylman/aerial",
			sha:        "0000000000000000000000000000000000000000",
			prNumber:   42,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "invalid prNumber <= 0 is no-op",
			repo:       "azylman/aerial",
			sha:        "abcdef123456",
			prNumber:   0,
			branch:     "feat/test",
			wantLookup: false,
		},
		{
			name:       "valid record and lookup with trimming and case normalization",
			repo:       " Azylman/Aerial ",
			sha:        " ABCDEF123456 ",
			prNumber:   100,
			branch:     " feat/normalized ",
			wantLookup: true,
			wantPR:     100,
			wantBranch: "feat/normalized",
		},
		{
			name:       "corrupt cache entry type returns false",
			repo:       "azylman/aerial",
			sha:        "corrupttype",
			corruptKey: "azylman/aerial@corrupttype",
			corruptVal: "not-a-GitHubCheckRunPR",
			wantLookup: false,
		},
		{
			name:       "corrupt cache entry zero PR number returns false",
			repo:       "azylman/aerial",
			sha:        "corruptpr",
			corruptKey: "azylman/aerial@corruptpr",
			corruptVal: GitHubCheckRunPR{Number: 0},
			wantLookup: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{}
			}
			if tc.corruptKey != "" && srv != nil {
				srv.GHState().StoreRawCommitSHAPRForTest(tc.corruptKey, tc.corruptVal)
			} else {
				srv.recordCommitSHAPR(tc.repo, tc.sha, tc.prNumber, tc.branch)
			}
			prNum, branch, ok := srv.lookupCommitSHAPR(tc.repo, tc.sha)
			if ok != tc.wantLookup {
				t.Fatalf("lookupCommitSHAPR() ok = %v, want %v", ok, tc.wantLookup)
			}
			if prNum != tc.wantPR {
				t.Errorf("lookupCommitSHAPR() prNum = %d, want %d", prNum, tc.wantPR)
			}
			if branch != tc.wantBranch {
				t.Errorf("lookupCommitSHAPR() branch = %q, want %q", branch, tc.wantBranch)
			}
		})
	}
}

func TestUpdatedForCurrentMainHelpers(t *testing.T) {
	t.Parallel()

	var nilSrv *RouterServer
	nilSrv.markUpdatedForCurrentMain("azylman/aerial", 123)
	if nilSrv.wasUpdatedForCurrentMain("azylman/aerial", 123) {
		t.Errorf("expected false for nil server")
	}
	nilSrv.clearUpdatedPRsForRepo("azylman/aerial")

	srv := &RouterServer{}
	srv.markUpdatedForCurrentMain("azylman/aerial", 0)
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 0) {
		t.Errorf("expected false for pr <= 0")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.markUpdatedForCurrentMain("azylman/aerial", 43)
	srv.markUpdatedForCurrentMain("azylman/other", 99)

	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true for PR 42")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 43) {
		t.Errorf("expected true for PR 43")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/other", 99) {
		t.Errorf("expected true for PR 99")
	}

	srv.clearUpdatedPRsForRepo("azylman/aerial")

	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false for cleared PR 42")
	}
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 43) {
		t.Errorf("expected false for cleared PR 43")
	}
	if !srv.wasUpdatedForCurrentMain("azylman/other", 99) {
		t.Errorf("expected true for remaining PR 99")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false after new main commit mainsha1")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true on duplicate redelivery of same main commit mainsha1")
	}

	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha2")
	if srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected false after advancing to mainsha2")
	}

	srv.markUpdatedForCurrentMain("azylman/aerial", 42)
	srv.clearUpdatedPRsForMainCommit("azylman/aerial", "mainsha1")
	if !srv.wasUpdatedForCurrentMain("azylman/aerial", 42) {
		t.Errorf("expected true on stale redelivery of earlier main commit mainsha1 after mainsha2")
	}
}

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

func TestHandlePullRequestRegistryAction_TrailerScoping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		botID          string
		defaultOwner   bool
		prBody         string
		action         string
		merged         bool
		wantRegistered bool
		wantClosed     bool
		wantEvents     []wantPREvent
	}{
		{
			name:           "matching bot_id registered",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"aerial\"} -->",
			wantRegistered: true,
		},
		{
			name:           "case-insensitive matching bot_id registered",
			botID:          "aerial",
			defaultOwner:   false,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"AERIAL\"} -->",
			wantRegistered: true,
		},
		{
			name:           "peer bot_id skipped",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\",\"bot_id\":\"coven\"} -->",
			wantRegistered: false,
		},
		{
			name:           "untagged registered when default owner",
			botID:          "aerial",
			defaultOwner:   true,
			prBody:         "Some PR\n\n<!-- aerial-pr:{\"target_id\":\"123456789012345678\"} -->",
			wantRegistered: true,
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
			wantEvents:     []wantPREvent{{Event: "synchronize", PRNumber: 101, HeadSHA: "sha101", Status: "open", Title: "Test PR"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{}
			cfg := Config{
				BotID:        tc.botID,
				DefaultOwner: tc.defaultOwner,
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
			if tc.wantRegistered && len(reg.ensureCalls) == 0 {
				t.Fatalf("expected PR to be registered, but EnsurePRRegistered was not called")
			}
			if !tc.wantRegistered && len(reg.ensureCalls) > 0 {
				t.Fatalf("expected PR NOT to be registered, but EnsurePRRegistered was called %d times", len(reg.ensureCalls))
			}
			if tc.wantClosed && len(reg.closedUnmergedCalls) == 0 {
				t.Fatalf("expected PR to be marked closed unmerged, but UpdatePRClosedUnmerged was not called")
			}
			if !tc.wantClosed && len(reg.closedUnmergedCalls) > 0 {
				t.Fatalf("expected PR NOT to be marked closed unmerged, but UpdatePRClosedUnmerged was called %d times", len(reg.closedUnmergedCalls))
			}
			assertPREvents(t, bus, tc.wantEvents)
		})
	}
}

func TestHandleGitHubWebhook_TeeAndForwarding(t *testing.T) {
	t.Parallel()

	tests := teeAndForwardingTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var botACalls, botBCalls atomic.Int32
			var receivedEventA, receivedDeliveryA string
			var mu sync.Mutex

			serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				botACalls.Add(1)
				mu.Lock()
				receivedEventA = r.Header.Get("X-Github-Event")
				receivedDeliveryA = r.Header.Get("X-Github-Delivery")
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer serverA.Close()

			serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				botBCalls.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer serverB.Close()

			reg := &ownedMapRegistry{
				ownedPRs:  tc.ownedPRs,
				ownedSHAs: tc.ownedSHAs,
			}
			cfg := Config{
				BotID:               "aerial",
				GitHubWebhookSecret: testGitHubWebhookSecret,
				WebhookForwardURLs: map[string]string{
					"bot_a":  serverA.URL,
					"bot_b":  serverB.URL,
					"aerial": "http://127.0.0.1:9999", // self, must never be called
				},
			}
			bus := &mockEventBus{}
			srv := newTestRouter(cfg, serverA.Client(), reg, nil, bus)

			headers := map[string]string{
				"X-Github-Event":      tc.event,
				"X-Github-Delivery":   "del-test-1234",
				"X-Hub-Signature-256": signTestGitHubWebhook(testGitHubWebhookSecret, tc.payload),
				"Content-Type":        "application/json",
			}
			rec := doHTTPRequest(srv.Routes(), http.MethodPost, "/api/webhooks/github", strings.NewReader(tc.payload), headers)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
			}

			if int(botACalls.Load()) != tc.wantBotACalls {
				t.Errorf("bot_a calls = %d, want %d", botACalls.Load(), tc.wantBotACalls)
			}
			if int(botBCalls.Load()) != tc.wantBotBCalls {
				t.Errorf("bot_b calls = %d, want %d", botBCalls.Load(), tc.wantBotBCalls)
			}

			if tc.wantBotACalls > 0 {
				mu.Lock()
				defer mu.Unlock()
				if receivedEventA != tc.event {
					t.Errorf("received X-Github-Event = %q, want %q", receivedEventA, tc.event)
				}
				if receivedDeliveryA != "del-test-1234" {
					t.Errorf("received X-Github-Delivery = %q, want del-test-1234", receivedDeliveryA)
				}
			}
		})
	}
}

type teeAndForwardingTestCase struct {
	name          string
	event         string
	payload       string
	ownedPRs      map[int]bool
	ownedSHAs     map[string]int
	wantBotACalls int
	wantBotBCalls int
}

func teeAndForwardingTestCases() []teeAndForwardingTestCase {
	return []teeAndForwardingTestCase{
		{
			name:          "pull_request tagged with bot_a forwards strictly to bot_a",
			event:         "pull_request",
			payload:       `{"number": 101, "pull_request": {"body": "PR<!-- aerial-pr:{\"bot_id\":\"bot_a\"} -->"}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request tagged with self does not forward",
			event:         "pull_request",
			payload:       `{"number": 102, "pull_request": {"body": "PR<!-- aerial-pr:{\"bot_id\":\"aerial\"} -->"}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request untagged and locally owned does not forward",
			event:         "pull_request",
			payload:       `{"number": 103, "pull_request": {"body": "Plain PR"}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{103: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "pull_request untagged and not locally owned forwards to all peers",
			event:         "pull_request",
			payload:       `{"number": 104, "pull_request": {"body": "Plain PR"}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{},
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "push to main forwards to all peers",
			event:         "push",
			payload:       `{"ref": "refs/heads/main", "after": "sha_main", "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "push to feature branch locally owned does not forward",
			event:         "push",
			payload:       `{"ref": "refs/heads/feat/mine", "after": "sha_owned", "repository": {"full_name": "azylman/aerial"}}`,
			ownedSHAs:     map[string]int{"sha_owned": 105},
			ownedPRs:      map[int]bool{105: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "push to feature branch unowned forwards to all peers",
			event:         "push",
			payload:       `{"ref": "refs/heads/feat/other", "after": "sha_unowned", "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "check_run locally owned does not forward",
			event:         "check_run",
			payload:       `{"check_run": {"head_sha": "sha_cr_owned", "pull_requests": [{"number": 106}]}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{106: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "check_run unowned forwards to all peers",
			event:         "check_run",
			payload:       `{"check_run": {"head_sha": "sha_cr_unowned", "pull_requests": [{"number": 107}]}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
		{
			name:          "workflow_run locally owned does not forward",
			event:         "workflow_run",
			payload:       `{"workflow_run": {"head_sha": "sha_wf_owned", "pull_requests": [{"number": 108}]}, "repository": {"full_name": "azylman/aerial"}}`,
			ownedPRs:      map[int]bool{108: true},
			wantBotACalls: 0,
			wantBotBCalls: 0,
		},
		{
			name:          "workflow_run unowned forwards to all peers",
			event:         "workflow_run",
			payload:       `{"workflow_run": {"head_sha": "sha_wf_unowned", "pull_requests": [{"number": 109}]}, "repository": {"full_name": "azylman/aerial"}}`,
			wantBotACalls: 1,
			wantBotBCalls: 1,
		},
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
	preCacheRepo  string
	preCacheSHA   string
	preCachePR    int
	preCacheBr    string
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
	if tc.preCacheRepo != "" && tc.preCacheSHA != "" {
		srv.recordCommitSHAPR(tc.preCacheRepo, tc.preCacheSHA, tc.preCachePR, tc.preCacheBr)
	}
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
			name:         "in-memory commitSHAPRs cache hit avoids hitting GitHub API",
			repo:         "azylman/aerial",
			sha:          "cachedsha999",
			apiURL:       "::bad-url::",
			preCacheRepo: "azylman/aerial",
			preCacheSHA:  "cachedsha999",
			preCachePR:   999,
			preCacheBr:   "feat/cached-branch",
			wantPRNum:    999,
			wantBranch:   "feat/cached-branch",
		},
		{
			name:          "invalid commit sha characters return error",
			repo:          "azylman/aerial",
			sha:           "0000",
			wantErr:       true,
			wantErrSubstr: "invalid repo or sha",
		},
		{
			name:          "lookupCommitSHAPR empty or zero sha returns false",
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
