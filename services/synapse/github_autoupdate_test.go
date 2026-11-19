package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

func makeTestOpenPR(number int, branch, sha, author string) GitHubPullRequest {
	return GitHubPullRequest{Number: number, Head: GitHubPRRef{Ref: branch, SHA: sha}, User: struct {
		Login string `json:"login"`
	}{Login: author}}
}

type checkOpenPRConflictsTestCase struct {
	name                  string
	autoUpdateAuthors     []string
	listPRsStatus         int
	useNilRegistry        bool
	useBus                bool
	busErr                error
	conflictErr           error
	openPRs               []GitHubPullRequest
	prResponses           []string
	prStatusCodes         []int
	closedErr             error
	wantClosedPRs         []int
	wantErr               bool
	wantErrSubstr         string
	wantConflict          bool
	wantConflictPR        int
	wantConflictPublished int
	wantPrompts           int
	wantDispatches        int
	wantPromptSubs        []string
	wantDMs               int
	wantUpdatedPRs        []string
	checkRunsBySHA        map[string]string
	presetFailedSHAs      map[int]string
	presetUpdatedPRs      []int
	wantCachedFailedSHAs  map[int]string
	wantUncachedFailedPRs []int
	prDetails             map[int]GitHubPullRequest
	prDetailStatus        int
}

func handleConflictCheckPRDetail(w http.ResponseWriter, r *http.Request, tc checkOpenPRConflictsTestCase) bool {
	if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/pulls/") {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	if tc.prDetailStatus != 0 {
		w.WriteHeader(tc.prDetailStatus)
		_, _ = w.Write([]byte(`{"message":"error"}`))
		return true
	}
	parts := strings.Split(r.URL.Path, "/pulls/")
	var prNum int
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &prNum)
	}
	pr, ok := tc.prDetails[prNum]
	if !ok {
		pr = GitHubPullRequest{Number: prNum, State: ghActionClosed, Merged: false}
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(pr)
	return true
}

func newConflictCheckMockServer(tc checkOpenPRConflictsTestCase, putPaths *[]string, mu *sync.Mutex) *httptest.Server {
	var reqCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pulls") {
			w.Header().Set("Content-Type", "application/json")
			if tc.listPRsStatus != 0 {
				w.WriteHeader(tc.listPRsStatus)
				_, _ = w.Write([]byte(`{"message":"server error"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(tc.openPRs)
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/check-runs") {
			w.Header().Set("Content-Type", "application/json")
			trimmed := strings.TrimSuffix(r.URL.Path, "/check-runs")
			sha := trimmed[strings.LastIndex(trimmed, "/")+1:]
			body := `{"check_runs":[]}`
			if tc.checkRunsBySHA != nil {
				if resp, ok := tc.checkRunsBySHA[sha]; ok {
					body = resp
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
			return
		}
		if handleConflictCheckPRDetail(w, r, tc) {
			return
		}
		if r.Method == http.MethodPut {
			mu.Lock()
			*putPaths = append(*putPaths, r.URL.Path)
			mu.Unlock()
		}
		idx := int(reqCount.Add(1)) - 1
		code := http.StatusOK
		if idx < len(tc.prStatusCodes) && tc.prStatusCodes[idx] != 0 {
			code = tc.prStatusCodes[idx]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		body := `{"number":0}`
		if idx < len(tc.prResponses) {
			body = tc.prResponses[idx]
		} else if len(tc.prResponses) > 0 {
			body = tc.prResponses[len(tc.prResponses)-1]
		}
		_, _ = w.Write([]byte(body))
	}))
	return server
}

func assertConflictCheckRegistryCalls(t *testing.T, tc checkOpenPRConflictsTestCase, mockReg PRRegistryUpdater) {
	t.Helper()
	mReg, ok := mockReg.(*mockPRRegistry)
	if !ok || mReg == nil {
		return
	}
	if tc.wantConflict {
		if len(mReg.conflictCalls) != 1 || mReg.conflictCalls[0].prNumber != tc.wantConflictPR {
			t.Errorf("conflictCalls = %+v, want PR %d", mReg.conflictCalls, tc.wantConflictPR)
		}
	} else if len(mReg.conflictCalls) != 0 {
		t.Errorf("unexpected conflictCalls: %+v", mReg.conflictCalls)
	}
	if len(mReg.closedUnmergedCalls) != len(tc.wantClosedPRs) {
		t.Errorf("closedUnmergedCalls count = %d, want %d", len(mReg.closedUnmergedCalls), len(tc.wantClosedPRs))
		return
	}
	for i, wantPR := range tc.wantClosedPRs {
		if mReg.closedUnmergedCalls[i].prNumber != wantPR {
			t.Errorf("closedUnmergedCalls[%d] = %d, want %d", i, mReg.closedUnmergedCalls[i].prNumber, wantPR)
		}
	}
}

func assertConflictCheckDispatcherCalls(t *testing.T, tc checkOpenPRConflictsTestCase, mockDisp *mockOutboundDispatcher) {
	t.Helper()
	if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
		t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
	}
	if len(mockDisp.PromptCalls()) != tc.wantPrompts {
		t.Errorf("got %d prompts, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
	}
	if len(mockDisp.DispatchCalls()) != tc.wantDispatches {
		t.Errorf("got %d dispatches, want %d", len(mockDisp.DispatchCalls()), tc.wantDispatches)
	}
	for _, sub := range tc.wantPromptSubs {
		if tc.wantDispatches > 0 {
			if len(mockDisp.DispatchCalls()) == 0 || !strings.Contains(mockDisp.DispatchCalls()[0].Prompt, sub) {
				t.Errorf("expected dispatch prompt containing %q", sub)
			}
		} else if len(mockDisp.PromptCalls()) == 0 || !strings.Contains(mockDisp.PromptCalls()[0].Prompt, sub) {
			t.Errorf("expected prompt containing %q", sub)
		}
	}
}

func assertConflictCheckCacheAndPuts(t *testing.T, tc checkOpenPRConflictsTestCase, srv *RouterServer, putPaths []string) {
	t.Helper()
	for prNum, wantSHA := range tc.wantCachedFailedSHAs {
		v, ok := srv.failedCheckSHAs.Load(prCheckKey("azylman/aerial", prNum))
		if !ok || fmt.Sprint(v) != wantSHA {
			t.Errorf("cached failedCheckSHAs[%d] = %v (ok=%v), want %q", prNum, v, ok, wantSHA)
		}
	}
	for _, prNum := range tc.wantUncachedFailedPRs {
		if v, ok := srv.failedCheckSHAs.Load(prCheckKey("azylman/aerial", prNum)); ok {
			t.Errorf("cached failedCheckSHAs[%d] = %v, want uncached", prNum, v)
		}
	}
	if len(putPaths) != len(tc.wantUpdatedPRs) {
		t.Fatalf("got updated PR paths %v, want %v", putPaths, tc.wantUpdatedPRs)
	}
	for i := range tc.wantUpdatedPRs {
		if putPaths[i] != tc.wantUpdatedPRs[i] {
			t.Errorf("updated PR path[%d] = %q, want %q", i, putPaths[i], tc.wantUpdatedPRs[i])
		}
	}
}

func runCheckOpenPRConflictsCase(ctx context.Context, t *testing.T, tc checkOpenPRConflictsTestCase) {
	t.Helper()
	t.Parallel()
	var mu sync.Mutex
	var putPaths []string
	apiServer := newConflictCheckMockServer(tc, &putPaths, &mu)
	defer apiServer.Close()

	var mockReg PRRegistryUpdater
	if !tc.useNilRegistry {
		mockReg = &mockPRRegistry{
			conflictErr:      tc.conflictErr,
			conflictTargetID: "100000000000000001",
			conflictUpdated:  true,
			closedErr:        tc.closedErr,
		}
	}
	mockDisp := &mockOutboundDispatcher{}
	var inserters []pubsub.Bus
	var mockBus *mockEventBus
	if tc.useBus {
		mockBus = &mockEventBus{err: tc.busErr}
		inserters = append(inserters, mockBus)
	}
	srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL, GitHubToken: "test-token", AutoUpdatePRAuthors: tc.autoUpdateAuthors, DefaultOwner: true}, nil, mockReg, mockDisp, inserters...)
	for prNum, sha := range tc.presetFailedSHAs {
		srv.recordFailedCISHA("azylman/aerial", prNum, sha)
	}
	for _, prNum := range tc.presetUpdatedPRs {
		srv.markUpdatedForCurrentMain("azylman/aerial", prNum)
	}

	err := srv.checkOpenPRConflicts(ctx, "azylman/aerial")
	if (err != nil) != tc.wantErr {
		t.Fatalf("checkOpenPRConflicts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
		t.Errorf("err = %v, want substr %q", err, tc.wantErrSubstr)
	}

	mu.Lock()
	gotUpdated := append([]string(nil), putPaths...)
	mu.Unlock()

	assertConflictCheckCacheAndPuts(t, tc, srv, gotUpdated)
	assertConflictCheckRegistryCalls(t, tc, mockReg)
	if mockBus != nil && tc.wantConflictPublished > 0 {
		conflictCount := 0
		for _, evt := range mockBus.publishedPREvents {
			if evt.Event == "conflict" {
				conflictCount++
			}
		}
		if conflictCount != tc.wantConflictPublished {
			t.Errorf("published conflict events = %d, want %d", conflictCount, tc.wantConflictPublished)
		}
	}
	assertConflictCheckDispatcherCalls(t, tc, mockDisp)
}

func TestCheckOpenPRConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range checkOpenPRConflictsTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runCheckOpenPRConflictsCase(ctx, t, tc)
		})
	}
}

func checkOpenPRConflictsTestCases() []checkOpenPRConflictsTestCase {
	p1 := checkOpenPRConflictsCasesPart1()
	p2 := checkOpenPRConflictsCasesPart2()
	p3 := checkOpenPRConflictsCasesPart3()
	p4 := checkOpenPRConflictsCasesPart4()
	all := make([]checkOpenPRConflictsTestCase, 0, len(p1)+len(p2)+len(p3)+len(p4))
	all = append(all, p1...)
	all = append(all, p2...)
	all = append(all, p3...)
	all = append(all, p4...)
	return all
}

func checkOpenPRConflictsCasesPart1() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:              "empty autoUpdateAuthors is clean no-op",
			autoUpdateAuthors: nil,
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
			},
			wantErr: false,
		},
		{
			name:              "422 no new commits on base branch marks PR updated and stops FIFO loop",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/behind-2", "head551", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"There are no new commits on the base branch."}`,
			},
			prStatusCodes:  []int{http.StatusUnprocessableEntity},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "nil registry still updates whitelisted PRs",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/clean", "head550", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "nil registry on conflict does not crash",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "nil registry on stale head ref does not crash",
			useNilRegistry:    true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "skips non-whitelisted PR authors and updates whitelisted PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(540, "feat/untrusted", "head540", "untrusted-user"),
				makeTestOpenPR(541, "feat/trusted", "head541", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/541/update-branch"},
		},
		{
			name:              "list open PRs error returns error",
			autoUpdateAuthors: []string{"trusted-author"},
			listPRsStatus:     http.StatusInternalServerError,
			wantErr:           true,
			wantErrSubstr:     "list open prs",
		},
		{
			name:                  "422 merge conflict transitions to conflict and publishes lifecycle event",
			useBus:                true,
			autoUpdateAuthors:     []string{"trusted-author"},
			openPRs:               []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:         []int{http.StatusUnprocessableEntity},
			prResponses:           []string{`{"message":"merge conflict between base and head"}`},
			wantConflict:          true,
			wantConflictPR:        543,
			wantConflictPublished: 1,
			wantDMs:               0,
			wantDispatches:        0,
			wantUpdatedPRs:        []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
	}
}

func checkOpenPRConflictsCasesPart2() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:                  "409 conflict with bus client publishes conflict event without enqueueing merge conflict job",
			useBus:                true,
			autoUpdateAuthors:     []string{"trusted-author"},
			openPRs:               []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:         []int{http.StatusConflict},
			prResponses:           []string{`{"message":"Merge conflict"}`},
			wantConflict:          true,
			wantConflictPR:        543,
			wantConflictPublished: 1,
			wantDMs:               0,
			wantDispatches:        0,
			wantUpdatedPRs:        []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "bus insert error on conflict logs warning without failing check",
			useBus:            true,
			busErr:            errors.New("bus insert failed"),
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusConflict},
			prResponses:       []string{`{"message":"Merge conflict"}`},
			wantConflict:      true,
			wantConflictPR:    543,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "open PRs update only oldest clean PR in FIFO order",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/behind-2", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "conflicted oldest PR transitions to conflict and advances to update next clean PR",
			useBus:            true,
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(545, "feat/conflicted-1", "head545", "trusted-author"),
				makeTestOpenPR(546, "feat/clean-2", "head546", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"merge conflict between base and head"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes:         []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantConflict:          true,
			wantConflictPR:        545,
			wantConflictPublished: 1,
			wantDMs:               0,
			wantDispatches:        0,
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/545/update-branch",
				"/repos/azylman/aerial/pulls/546/update-branch",
			},
		},
		{
			name:              "non-conflict 422 logs warning and skips without marking conflict",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(552, "feat/behind-err", "head552", "trusted-author")},
			prResponses:       []string{`{"message":"Validation Failed"}`},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/552/update-branch"},
		},
		{
			name:              "403 rate-limit error returns error",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(546, "feat/rate-limit", "head546", "trusted-author")},
			prStatusCodes:     []int{http.StatusForbidden},
			prResponses:       []string{`{"message":"API rate limit exceeded"}`},
			wantErr:           true,
			wantErrSubstr:     "rate limit",
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/546/update-branch"},
		},
		{
			name:              "non-403 error skips PR and returns nil",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(547, "feat/not-found", "head547", "trusted-author")},
			prStatusCodes:     []int{http.StatusNotFound},
			prResponses:       []string{`{"message":"Not Found"}`},
			wantErr:           false,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/547/update-branch"},
		},
		{
			name:              "conflict transition error logged cleanly without returning error",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(549, "feat/trans-err", "head549", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			conflictErr:       errors.New("conflict transition err"),
			wantErr:           false,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/549/update-branch"},
		},
	}
}

func checkOpenPRConflictsCasesPart3() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:              "422 head ref does not exist closes stale PR and advances to next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantClosedPRs: []int{512},
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "422 head ref does not exist logs close error cleanly without failing check",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"head ref does not exist"}`},
			closedErr:         errors.New("close db err"),
			wantClosedPRs:     nil,
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/512/update-branch"},
		},
		{
			name:              "skips draft PRs and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				{Number: 550, Head: GitHubPRRef{Ref: "feat/draft", SHA: "head550"}, Draft: true, User: struct {
					Login string `json:"login"`
				}{Login: "trusted-author"}},
				makeTestOpenPR(551, "feat/ready", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/551/update-branch"},
		},
		{
			name:              "skips PR with in-memory failedCheckSHAs and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			presetFailedSHAs:  map[int]string{550: "head550"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failing", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/551/update-branch"},
		},
		{
			name:              "skips PR with failed check-run in API, caches failure, and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"Unit Tests","status":"completed","conclusion":"failure"}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failing-check", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:          []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:        []int{http.StatusAccepted},
			wantUpdatedPRs:       []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantCachedFailedSHAs: map[int]string{550: "head550"},
		},
		{
			name:              "breaks without updating when leading clean PR already updated for current main",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/waiting", "head551", "trusted-author"),
			},
			wantUpdatedPRs: nil,
		},
		{
			name:              "skips PR with pending check-run without caching failure and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"roborev","status":"in_progress","conclusion":""}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/pending-check", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:           []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:         []int{http.StatusAccepted},
			wantUpdatedPRs:        []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "breaks without updating when leading clean PR already updated for current main has pending checks",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"roborev","status":"in_progress","conclusion":""}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/waiting", "head551", "trusted-author"),
			},
			wantUpdatedPRs:        nil,
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "advances to update next clean PR when leading PR updated for current main has failed checks",
			autoUpdateAuthors: []string{"trusted-author"},
			presetUpdatedPRs:  []int{550},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"failure"}]}`,
			},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/failed-in-flight", "head550", "trusted-author"),
				makeTestOpenPR(551, "feat/clean", "head551", "trusted-author"),
			},
			prResponses:          []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:        []int{http.StatusAccepted},
			wantUpdatedPRs:       []string{"/repos/azylman/aerial/pulls/551/update-branch"},
			wantCachedFailedSHAs: map[int]string{550: "head550"},
		},
	}
}

func checkOpenPRConflictsCasesPart4() []checkOpenPRConflictsTestCase {
	return []checkOpenPRConflictsTestCase{
		{
			name:              "ignores queued dependabot app check-run and updates PR",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":".github/dependabot.yml","status":"queued","conclusion":"","app":{"slug":"dependabot"}}]}`,
			},
			openPRs:        []GitHubPullRequest{makeTestOpenPR(550, "feat/dependabot-check", "head550", "trusted-author")},
			prResponses:    []string{`{"message":"Updating pull request branch."}`},
			prStatusCodes:  []int{http.StatusAccepted},
			wantUpdatedPRs: []string{"/repos/azylman/aerial/pulls/550/update-branch"},
		},
		{
			name:              "skips PR with same-named queued check from github-actions app",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":".github/dependabot.yml","status":"queued","conclusion":"","app":{"slug":"github-actions"}}]}`,
			},
			openPRs:               []GitHubPullRequest{makeTestOpenPR(550, "feat/actions-check", "head550", "trusted-author")},
			wantUpdatedPRs:        nil,
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "skips PR with queued Dependabot job from github-actions alongside dependabot app check",
			autoUpdateAuthors: []string{"trusted-author"},
			checkRunsBySHA: map[string]string{
				"head550": `{"check_runs":[{"name":".github/dependabot.yml","status":"queued","conclusion":"","app":{"slug":"dependabot"}},{"name":"Dependabot","status":"queued","conclusion":"","app":{"slug":"github-actions"}}]}`,
			},
			openPRs:               []GitHubPullRequest{makeTestOpenPR(550, "feat/dependabot-job", "head550", "trusted-author")},
			wantUpdatedPRs:        nil,
			wantUncachedFailedPRs: []int{550},
		},
		{
			name:              "422 head ref does not exist ignores merged PR without closing and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			prDetails: map[int]GitHubPullRequest{
				512: {Number: 512, State: ghActionClosed, Merged: true},
			},
			wantClosedPRs: nil,
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "422 head ref does not exist with 500 pr detail error does not close PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes:  []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			prDetailStatus: http.StatusInternalServerError,
			wantClosedPRs:  nil,
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
		{
			name:              "422 head ref does not exist closes confirmed unmerged closed PR and updates next clean PR",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(512, "feat/stale-branch", "head512", "trusted-author"),
				makeTestOpenPR(550, "feat/clean-branch", "head550", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"head ref does not exist"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes: []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			prDetails: map[int]GitHubPullRequest{
				512: {Number: 512, State: ghActionClosed, Merged: false},
			},
			wantClosedPRs: []int{512},
			wantUpdatedPRs: []string{
				"/repos/azylman/aerial/pulls/512/update-branch",
				"/repos/azylman/aerial/pulls/550/update-branch",
			},
		},
	}
}
