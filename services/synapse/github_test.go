package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

type ghWebhookTestCase struct {
	name, path, wantMetricSubstr string
	body                         io.Reader
	headers                      map[string]string
	river                        *mockRiverInserter
	wantStatus, wantInserted     int
}

func assertGitHubWebhookResponse(t *testing.T, tc ghWebhookTestCase, rec *httptest.ResponseRecorder, routes http.Handler) {
	t.Helper()
	if rec.Code != tc.wantStatus {
		t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
	}
	if tc.wantInserted > 0 && tc.river != nil {
		if len(tc.river.insertedJobs) != tc.wantInserted {
			t.Fatalf("expected %d inserted jobs, got %d", tc.wantInserted, len(tc.river.insertedJobs))
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != tc.headers["X-GitHub-Event"] || resp["delivery"] != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected response body: %+v", resp)
		}
		if tc.river.insertedJobs[0].Event != tc.headers["X-GitHub-Event"] || tc.river.insertedJobs[0].Delivery != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected inserted jobs: %+v", tc.river.insertedJobs)
		}
	}
	if tc.wantMetricSubstr != "" {
		recM := doHTTPRequest(routes, http.MethodGet, "/metrics", nil, nil)
		if !strings.Contains(recM.Body.String(), tc.wantMetricSubstr) {
			t.Errorf("expected /metrics to include %q, got:\n%s", tc.wantMetricSubstr, recM.Body.String())
		}
	}
}

func runGitHubWebhookCase(t *testing.T, tc ghWebhookTestCase) {
	t.Helper()
	t.Parallel()
	var srv *RouterServer
	if tc.river != nil {
		srv = NewRouterServer(fixedConfig(Config{}), nil, tc.river)
	} else {
		srv = NewRouterServer(fixedConfig(Config{}), nil)
	}
	routes := srv.Routes()
	rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
	assertGitHubWebhookResponse(t, tc, rec, routes)
}

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	tests := []ghWebhookTestCase{
		{name: "missing X-GitHub-Event header", path: "/api/webhooks/github", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/github", body: errReader{}, headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusBadRequest},
		{name: "river client nil", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusInternalServerError},
		{name: "river client insert error", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping"}, river: &mockRiverInserter{err: errors.New("simulated database failure")}, wantStatus: http.StatusInternalServerError},
		{name: "api path success", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"}, river: &mockRiverInserter{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "legacy path success", path: "/webhooks/github", body: strings.NewReader(`{"zen":"Keep it simple"}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234"}, river: &mockRiverInserter{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "duplicate skipped", path: "/api/webhooks/github", body: strings.NewReader(`{"zen":"Responsive is better than fast.","hook_id":9999,"repository":{"full_name":"azylman/aerial"}}`), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "dup-gh-delivery-1"}, river: &mockRiverInserter{skipDuplicate: true}, wantStatus: http.StatusOK, wantMetricSubstr: `synapse_unique_skipped_total{kind="github_webhook"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGitHubWebhookCase(t, tc)
		})
	}

	opts := (GitHubWebhookArgs{Delivery: "test-delivery"}).InsertOpts()
	if !opts.UniqueOpts.ByArgs || opts.UniqueOpts.ByPeriod != 15*time.Minute {
		t.Errorf("unexpected GitHubWebhookArgs InsertOpts: %+v", opts)
	}
}

type ghPriorEvent struct {
	event    string
	delivery string
	payload  string
}

type ghEventCase struct {
	name                 string
	event                string
	delivery             string
	payload              string
	priorEvent           *ghPriorEvent
	reg                  *mockPRRegistry
	disp                 *mockOutboundDispatcher
	river                *mockRiverInserter
	spyNoGitHubHit       bool
	systemChannelID      string
	autoUpdateAuthors    []string
	wantErr              bool
	wantRepo             string
	wantAction           string
	wantPR               int
	wantBranch           string
	wantHeadSHA          string
	wantMergeSHA         string
	wantTargetID         string
	wantSource           string
	wantConclusion       string
	wantSender           string
	wantMergedCalls      int
	wantClosedCalls      int
	wantSyncCalls        int
	wantCIFailedCalls    int
	wantCIFailedSHAs     int
	wantBackfillCalls    int
	wantConflictCalls    int
	wantAdvanceCalls     int
	wantFailVanadis      int
	wantEnsureCalls      int
	wantGitPushes        int
	wantPrompts          int
	wantDispatches       int
	wantDMs              int
	wantPromptSubstr     string
	wantDMSubstr         string
	wantRecordCheckCalls int
	wantRecordCheckState string
	wantUpdatedBranchHit string
	wantReconciles       []fluxKustomizationRef
}

func TestProcessGitHubEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var ghPaths sync.Map
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ghPaths.Store(r.URL.Path, true)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/commits/commit-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 777, "state": "open", "head": {"ref": "feat/fallback", "sha": "commit-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/wf-fallback/pulls"):
			_, _ = w.Write([]byte(`[{"number": 888, "state": "open", "head": {"ref": "feat/wf-branch", "sha": "wf-fallback"}}]`))
		case strings.Contains(r.URL.Path, "/commits/squash123/pulls"):
			_, _ = w.Write([]byte(`[{"number": 999, "state": "closed", "head": {"ref": "feat/squash", "sha": "squash123"}}]`))
		case strings.Contains(r.URL.Path, "/commits/pushsha999/pulls"):
			_, _ = w.Write([]byte(`[{"number": 522, "state": "closed"}]`))
		case strings.Contains(r.URL.Path, "/pulls/543/update-branch"):
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"message": "merge conflict between base and head"}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/repos/azylman/aerial/pulls" || strings.HasSuffix(r.URL.Path, "/repos/azylman/aerial/pulls")):
			_, _ = w.Write([]byte(`[{"number": 543, "head": {"ref": "feat/conflict-check", "sha": "head543"}, "user": {"login": "trusted-author"}}]`))
		case r.Method == http.MethodGet && (r.URL.Path == "/repos/azylman/mirrormere/pulls" || strings.HasSuffix(r.URL.Path, "/repos/azylman/mirrormere/pulls")):
			_, _ = w.Write([]byte(`[{"number": 891, "head": {"ref": "feat/fail-pr", "sha": "failhead891"}, "user": {"login": "trusted-author"}}, {"number": 894, "head": {"ref": "feat/clean-pr", "sha": "cleanhead894"}, "user": {"login": "trusted-author"}}]`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/repos/azylman/mirrormere/pulls/894/update-branch"):
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"message": "Updating pull request branch."}`))
		case strings.Contains(r.URL.Path, "/check-runs"):
			_, _ = w.Write([]byte(`{"check_runs":[]}`))
		case strings.Contains(r.URL.Path, "/issues/525/comments"):
			_, _ = w.Write([]byte(`[{"id": 0, "body": "Non-roborev comment"}, {"id": 1, "body": "## roborev: Review Complete (` + "`old0000`" + `)\nOld bug"}, {"id": 2, "body": "## roborev: Combined Review (` + "`robo525`" + `)\nFound SQL injection in query"}]`))
		case strings.Contains(r.URL.Path, "/actions/runs/800/jobs"):
			_, _ = w.Write([]byte(`{"total_count":1,"jobs":[{"id":1,"name":"Verify Codebase","status":"completed","conclusion":"success"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(mockServer.Close)

	cases := processGitHubEventTestCases()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runProcessGitHubEventCase(ctx, t, tc, mockServer, &ghPaths)
		})
	}
}

func assertEventRegistryCalls(t *testing.T, tc ghEventCase) {
	t.Helper()
	if tc.reg == nil {
		return
	}
	assertEventRegistryTransitionCalls(t, tc)
	assertEventRegistryLifecycleCalls(t, tc)
}

func assertEventRegistryTransitionCalls(t *testing.T, tc ghEventCase) {
	t.Helper()
	if len(tc.reg.mergedCalls) != tc.wantMergedCalls {
		t.Errorf("mergedCalls = %d, want %d", len(tc.reg.mergedCalls), tc.wantMergedCalls)
	}
	if tc.wantMergeSHA != "" && len(tc.reg.mergedCalls) > 0 && tc.reg.mergedCalls[0].mergeSHA != tc.wantMergeSHA {
		t.Errorf("mergedCalls mergeSHA = %q, want %q", tc.reg.mergedCalls[0].mergeSHA, tc.wantMergeSHA)
	}
	if len(tc.reg.closedUnmergedCalls) != tc.wantClosedCalls {
		t.Errorf("closedUnmergedCalls = %d, want %d", len(tc.reg.closedUnmergedCalls), tc.wantClosedCalls)
	}
	if len(tc.reg.syncCalls) != tc.wantSyncCalls {
		t.Errorf("syncCalls = %d, want %d", len(tc.reg.syncCalls), tc.wantSyncCalls)
	}
	if len(tc.reg.ciFailedCalls) != tc.wantCIFailedCalls {
		t.Errorf("ciFailedCalls = %d, want %d", len(tc.reg.ciFailedCalls), tc.wantCIFailedCalls)
	}
	if len(tc.reg.ciFailedBySHACalls) != tc.wantCIFailedSHAs {
		t.Errorf("ciFailedBySHACalls = %d, want %d", len(tc.reg.ciFailedBySHACalls), tc.wantCIFailedSHAs)
	}
	if tc.wantHeadSHA != "" && len(tc.reg.ciFailedBySHACalls) > 0 && tc.reg.ciFailedBySHACalls[0].headSHA != tc.wantHeadSHA {
		t.Errorf("ciFailedBySHACalls headSHA = %q, want %q", tc.reg.ciFailedBySHACalls[0].headSHA, tc.wantHeadSHA)
	}
}

func assertEventRegistryLifecycleCalls(t *testing.T, tc ghEventCase) {
	t.Helper()
	if len(tc.reg.backfillCalls) != tc.wantBackfillCalls {
		t.Errorf("backfillCalls = %d, want %d", len(tc.reg.backfillCalls), tc.wantBackfillCalls)
	}
	if tc.wantMergeSHA != "" && len(tc.reg.backfillCalls) > 0 && tc.reg.backfillCalls[0].mergeSHA != tc.wantMergeSHA {
		t.Errorf("backfillCalls mergeSHA = %q, want %q", tc.reg.backfillCalls[0].mergeSHA, tc.wantMergeSHA)
	}
	if len(tc.reg.conflictCalls) != tc.wantConflictCalls {
		t.Errorf("conflictCalls = %d, want %d", len(tc.reg.conflictCalls), tc.wantConflictCalls)
	}
	if len(tc.reg.advanceVanadisCalls) != tc.wantAdvanceCalls {
		t.Errorf("advanceVanadisCalls = %d, want %d", len(tc.reg.advanceVanadisCalls), tc.wantAdvanceCalls)
	}
	if len(tc.reg.failVanadisCalls) != tc.wantFailVanadis {
		t.Errorf("failVanadisCalls = %d, want %d", len(tc.reg.failVanadisCalls), tc.wantFailVanadis)
	}
	if len(tc.reg.ensureCalls) != tc.wantEnsureCalls {
		t.Errorf("ensureCalls = %d, want %d", len(tc.reg.ensureCalls), tc.wantEnsureCalls)
	}
	if len(tc.reg.recordCheckCalls) != tc.wantRecordCheckCalls {
		t.Errorf("recordCheckCalls = %d, want %d", len(tc.reg.recordCheckCalls), tc.wantRecordCheckCalls)
	}
	if tc.wantRecordCheckState != "" && len(tc.reg.recordCheckCalls) > 0 && tc.reg.recordCheckCalls[0].checkState != tc.wantRecordCheckState {
		t.Errorf("recordCheckCalls checkState = %q, want %q", tc.reg.recordCheckCalls[0].checkState, tc.wantRecordCheckState)
	}
}

func assertEventDispatcherCalls(t *testing.T, tc ghEventCase) {
	t.Helper()
	if tc.disp == nil {
		return
	}
	assertEventDispatcherPushesAndReconciles(t, tc)
	assertEventDispatcherPromptsAndDMs(t, tc)
}

func assertEventDispatcherPushesAndReconciles(t *testing.T, tc ghEventCase) {
	t.Helper()
	gitPushes := tc.disp.GitPushCalls()
	if len(gitPushes) != tc.wantGitPushes {
		t.Errorf("gitPushes = %d, want %d", len(gitPushes), tc.wantGitPushes)
	}
	if tc.wantGitPushes > 0 && len(gitPushes) > 0 {
		if tc.wantTargetID != "" && gitPushes[0].TargetID != tc.wantTargetID {
			t.Errorf("git push TargetID = %q, want %q", gitPushes[0].TargetID, tc.wantTargetID)
		}
		if tc.wantPR != 0 && gitPushes[0].PRNumber != tc.wantPR {
			t.Errorf("git push PRNumber = %d, want %d", gitPushes[0].PRNumber, tc.wantPR)
		}
	}
	if got := tc.disp.ReconcileCalls(); !slices.Equal(got, tc.wantReconciles) {
		t.Errorf("reconcile calls = %v, want %v", got, tc.wantReconciles)
	}
}

func assertEventDispatcherPromptsAndDMs(t *testing.T, tc ghEventCase) {
	t.Helper()
	dispatches := tc.disp.DispatchCalls()
	if len(dispatches) != tc.wantDispatches {
		t.Errorf("dispatches = %d, want %d", len(dispatches), tc.wantDispatches)
	}
	if tc.wantPromptSubstr != "" && len(dispatches) > 0 && !strings.Contains(dispatches[len(dispatches)-1].Prompt, tc.wantPromptSubstr) {
		t.Errorf("expected dispatch to contain %q, got %+v", tc.wantPromptSubstr, dispatches)
	}
	prompts := tc.disp.PromptCalls()
	if len(prompts) != tc.wantPrompts {
		t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
	}
	if tc.wantPromptSubstr != "" && len(prompts) > 0 && !strings.Contains(prompts[len(prompts)-1].Prompt, tc.wantPromptSubstr) {
		t.Errorf("expected prompt to contain %q, got %+v", tc.wantPromptSubstr, prompts)
	}
	dms := tc.disp.DirectMessageCalls()
	if len(dms) != tc.wantDMs {
		t.Errorf("dms = %d, want %d", len(dms), tc.wantDMs)
	}
	if tc.wantDMSubstr != "" && (len(dms) == 0 || !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSubstr)) {
		t.Errorf("expected DM to contain %q, got %+v", tc.wantDMSubstr, dms)
	}
}

func assertEventResult(t *testing.T, tc ghEventCase, res *ResolvedGitHubEvent, ghPaths *sync.Map) {
	t.Helper()
	if tc.spyNoGitHubHit {
		if _, hit := ghPaths.Load("/repos/azylman/aerial/commits/localsha123/pulls"); hit {
			t.Fatalf("expected 0 GitHub API hits for localsha123, but endpoint was called")
		}
	}
	if tc.wantUpdatedBranchHit != "" {
		if _, hit := ghPaths.Load(tc.wantUpdatedBranchHit); !hit {
			t.Errorf("expected branch update hit for %q, but was not called", tc.wantUpdatedBranchHit)
		}
	}
	assertEventBasicFields(t, tc, res)
	assertEventRegistryCalls(t, tc)
	assertEventDispatcherCalls(t, tc)
}

func assertEventBasicFields(t *testing.T, tc ghEventCase, res *ResolvedGitHubEvent) {
	t.Helper()
	checkFields := []struct {
		name, got, want string
	}{
		{"Repo", res.Repo, tc.wantRepo},
		{"Action", res.Action, tc.wantAction},
		{"Branch", res.Branch, tc.wantBranch},
		{"HeadSHA", res.HeadSHA, tc.wantHeadSHA},
		{"MergeSHA", res.MergeSHA, tc.wantMergeSHA},
		{"TargetID", res.TargetID, tc.wantTargetID},
		{"ResolutionSource", res.ResolutionSource, tc.wantSource},
		{"Conclusion", res.Conclusion, tc.wantConclusion},
		{"Sender", res.Sender, tc.wantSender},
	}
	for _, f := range checkFields {
		if f.want != "" && f.got != f.want {
			t.Errorf("%s = %q, want %q", f.name, f.got, f.want)
		}
	}
	if tc.wantPR != 0 && res.PRNumber != tc.wantPR {
		t.Errorf("PRNumber = %d, want %d", res.PRNumber, tc.wantPR)
	}
}

func runProcessGitHubEventCase(ctx context.Context, t *testing.T, tc ghEventCase, mockServer *httptest.Server, ghPaths *sync.Map) {
	t.Helper()
	t.Parallel()
	var inserters []RiverInserter
	if tc.river != nil {
		inserters = append(inserters, tc.river)
	}
	var reg PRRegistryUpdater
	if tc.reg != nil {
		reg = tc.reg
	}
	var disp OutboundDispatcher
	if tc.disp != nil {
		disp = tc.disp
	}
	srv := newTestRouter(Config{GitHubAPIURL: mockServer.URL, GitHubToken: "test-token", SystemChannelID: tc.systemChannelID, AutoUpdatePRAuthors: tc.autoUpdateAuthors, DefaultOwner: true}, mockServer.Client(), reg, disp, inserters...)
	srv.onProcessed = func(string, string) {}

	if tc.priorEvent != nil {
		if _, err := srv.ProcessGitHubEvent(ctx, tc.priorEvent.event, tc.priorEvent.delivery, []byte(tc.priorEvent.payload)); err != nil {
			t.Fatalf("priorEvent failed: %v", err)
		}
	}

	res, err := srv.ProcessGitHubEvent(ctx, tc.event, tc.delivery, []byte(tc.payload))
	if (err != nil) != tc.wantErr {
		t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	assertEventResult(t, tc, res, ghPaths)
}

func processGitHubEventTestCases() []ghEventCase {
	p1 := processGitHubEventCasesPart1()
	p2 := processGitHubEventCasesPart2()
	p3 := processGitHubEventCasesPart3()
	p4 := processGitHubEventCasesPart4()
	p5 := processGitHubEventCasesPart5()
	p6 := processGitHubEventCasesPart6()
	p7 := processGitHubEventCasesPart7()
	p8 := processGitHubEventCasesPart8()
	total := len(p1) + len(p2) + len(p3) + len(p4) + len(p5) + len(p6) + len(p7) + len(p8)
	all := make([]ghEventCase, 0, total)
	all = append(all, p1...)
	all = append(all, p2...)
	all = append(all, p3...)
	all = append(all, p4...)
	all = append(all, p5...)
	all = append(all, p6...)
	all = append(all, p7...)
	all = append(all, p8...)
	return all
}

func processGitHubEventCasesPart1() []ghEventCase {
	return []ghEventCase{
		{
			name: "ping valid", event: "ping", delivery: "del-ping-1",
			payload:  `{"zen":"Responsive is better than fast.","hook_id":98765,"repository":{"full_name":"azylman/aerial"}}`,
			wantRepo: "azylman/aerial",
		},
		{
			name: "ping malformed", event: "ping", delivery: "del-ping-2",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "pull_request closed and merged", event: "pull_request", delivery: "del-pr-1",
			payload:    `{"action":"closed","number":512,"pull_request":{"number":512,"title":"feat: test pr","head":{"ref":"feat/test","sha":"headsha123"},"base":{"ref":"main","sha":"basesha456"},"merged":true,"merge_commit_sha":"mergesha789"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			wantAction: "closed", wantPR: 512, wantBranch: "feat/test", wantHeadSHA: "headsha123", wantMergeSHA: "mergesha789", wantSource: "pull_request_payload", wantSender: "arcane103",
		},
		{
			name: "pull_request malformed", event: "pull_request", delivery: "del-pr-2",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "check_run with pull_requests array", event: "check_run", delivery: "del-cr-1",
			payload: `{"action":"completed","check_run":{"id":111,"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  515, wantBranch: "feat/awesome", wantSource: "check_run_payload", wantConclusion: "success",
		},
		{
			name: "check_run fallback via commit SHA lookup", event: "check_run", delivery: "del-cr-2",
			payload: `{"action":"completed","check_run":{"id":222,"name":"Integration Tests","head_sha":"commit-fallback","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  777, wantBranch: "feat/fallback", wantSource: "commit_sha_lookup", wantConclusion: "failure",
		},
		{
			name: "check_run malformed", event: "check_run", delivery: "del-cr-3",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "check_run empty head sha and no prs", event: "check_run", delivery: "del-cr-empty-sha",
			payload: `{"action":"completed","check_run":{"id":991,"name":"Lint","head_sha":"","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
		},
		{
			name: "check_run with no prs and registry resolve error", event: "check_run", delivery: "del-cr-reg-err",
			payload: `{"action":"completed","check_run":{"id":992,"name":"Build","head_sha":"unknown-sha","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolveErr: errors.New("reg err")},
		},
		{
			name: "check_run ci failure transition error", event: "check_run", delivery: "del-cr-fail-err",
			payload: `{"action":"completed","check_run":{"id":993,"name":"Test","head_sha":"headsha111","status":"completed","conclusion":"failure","pull_requests":[{"number":515,"head":{"ref":"feat/fail","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedErr: errors.New("ci err")},
			wantErr: true,
		},
		{
			name: "check_run ci failure by sha transition error", event: "check_run", delivery: "del-cr-fail-sha-err",
			payload: `{"action":"completed","check_run":{"id":994,"name":"Test","head_sha":"headsha994","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedBySHAErr: errors.New("sha err")},
			wantErr: true,
		},
		{
			name: "check_run completed success records passed check status and syncs widget", event: "check_run", delivery: "del-cr-ci-passed",
			payload: `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{recordCheckUpdated: true, resolveTargetID: "1555405874565091380", resolveBranch: "feat/awesome"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "passed",
		},
		{
			name: "check_run in_progress records pending check status", event: "check_run", delivery: "del-cr-ci-pending",
			payload: `{"action":"in_progress","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"in_progress","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{recordCheckUpdated: false},
			wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "pending",
		},
	}
}

func processGitHubEventCasesPart2() []ghEventCase {
	return []ghEventCase{
		{
			name: "check_run failure does not record check status", event: "check_run", delivery: "del-cr-ci-failed-norecord",
			payload: `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"headsha111","status":"completed","conclusion":"failure","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: false},
			wantPR:  515, wantCIFailedCalls: 1, wantRecordCheckCalls: 0,
		},
		{
			name: "check_run Container Builds Gate aggregator does not record check status", event: "check_run", delivery: "del-cr-gate-norecord",
			payload: `{"action":"completed","check_run":{"name":"Container Builds Gate","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{},
			wantPR:  515, wantRecordCheckCalls: 0,
		},
		{
			name: "check_run record check status error logs warning cleanly", event: "check_run", delivery: "del-cr-record-err",
			payload: `{"action":"completed","check_run":{"name":"Lint","head_sha":"headsha111","status":"completed","conclusion":"success","pull_requests":[{"number":515,"head":{"ref":"feat/awesome","sha":"headsha111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{recordCheckErr: errors.New("db record failure")},
			wantPR:  515, wantRecordCheckCalls: 1, wantRecordCheckState: "passed",
		},
		{
			name: "workflow_run with PR array", event: "workflow_run", delivery: "del-wf-1",
			payload: `{"action":"completed","workflow_run":{"id":333,"name":"Continuous Delivery","head_sha":"headsha333","head_branch":"feat/cd","status":"completed","conclusion":"success","pull_requests":[{"number":516,"head":{"ref":"feat/cd","sha":"headsha333"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  516, wantBranch: "feat/cd", wantSource: "workflow_run_payload",
		},
		{
			name: "workflow_run fallback to commit lookup", event: "workflow_run", delivery: "del-wf-2",
			payload: `{"action":"completed","workflow_run":{"id":444,"name":"Continuous Delivery","head_sha":"wf-fallback","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  888, wantBranch: "feat/wf-branch", wantSource: "commit_sha_lookup",
		},
		{
			name: "workflow_run malformed", event: "workflow_run", delivery: "del-wf-3",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "push to main with commit resolution", event: "push", delivery: "del-push-1",
			payload: `{"ref":"refs/heads/main","before":"prev111","after":"squash123","deleted":false,"head_commit":{"id":"squash123","message":"feat: merged feature (#999)"},"repository":{"full_name":"azylman/aerial"}}`,
			wantPR:  999, wantMergeSHA: "squash123", wantSource: "commit_sha_lookup",
		},
		{
			name: "push branch deletion", event: "push", delivery: "del-push-2",
			payload:    `{"ref":"refs/heads/feat/temp","before":"temp111","after":"0000000000000000000000000000000000000000","deleted":true,"repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "branch_deleted",
		},
		{
			name: "push tag non-branch ref", event: "push", delivery: "del-push-3",
			payload:    `{"ref":"refs/tags/v1.0.0","after":"tagsha111","repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "non_branch_ref",
		},
		{
			name: "push feature branch", event: "push", delivery: "del-push-feat",
			payload:    `{"ref":"refs/heads/feat/new-feature","before":"featprev","after":"featsha222","head_commit":{"id":"featsha222","message":"feat: new feature"},"repository":{"full_name":"azylman/aerial"}}`,
			wantSource: "non_main_branch",
		},
		{
			name: "push git push dispatcher error", event: "push", delivery: "del-push-disp-err",
			payload: `{"ref":"refs/heads/main","before":"prev111","after":"pushsha999","head_commit":{"id":"pushsha999","message":"feat: push"},"repository":{"full_name":"azylman/aerial"}}`,
			disp:    &mockOutboundDispatcher{gitPushErr: errors.New("hangar down")},
			wantErr: true,
		},
		{
			name: "push malformed", event: "push", delivery: "del-push-4",
			payload: `{invalid`, wantErr: true,
		},
		{
			name: "unhandled event", event: "star", delivery: "del-star-1",
			payload: `{"action":"created"}`, wantSource: "unhandled_event",
		},
		{
			name: "pull_request closed and merged transitions registry", event: "pull_request", delivery: "del-pr-merged",
			payload: `{"action":"closed","number":515,"pull_request":{"number":515,"head":{"ref":"feat/decouple","sha":"head111"},"merged":true,"merge_commit_sha":"merge111"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, mergedTargetID: "target-merged-1"},
			wantPR:  515, wantMergeSHA: "merge111", wantTargetID: "target-merged-1", wantMergedCalls: 1, wantAdvanceCalls: 1,
		},
	}
}

func processGitHubEventCasesPart3() []ghEventCase {
	return []ghEventCase{
		{
			name: "pull_request closed unmerged transitions registry", event: "pull_request", delivery: "del-pr-closed",
			payload: `{"action":"closed","number":516,"pull_request":{"number":516,"head":{"ref":"feat/discard","sha":"head222"},"merged":false},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-1"},
			wantPR:  516, wantTargetID: "target-closed-1", wantClosedCalls: 1, wantFailVanadis: 1,
		},
		{
			name: "pull_request closed unmerged failVanadisChunk error propagates", event: "pull_request", delivery: "del-pr-closed-fail-vanadis-err",
			payload: `{"action":"closed","number":516,"pull_request":{"number":516,"head":{"ref":"feat/discard","sha":"head222"},"merged":false},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-1", failVanadisErr: errors.New("fail err")},
			wantErr: true,
		},
		{
			name: "pull_request synchronize transitions registry", event: "pull_request", delivery: "del-pr-sync",
			payload: `{"action":"synchronize","number":517,"pull_request":{"number":517,"head":{"ref":"feat/update","sha":"newhead333"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{syncTargetID: "target-sync-1", resolveTargetID: "target-sync-1", resolveBranch: "feat/update"},
			disp:    &mockOutboundDispatcher{widgetErr: errors.New("widget sync err")},
			wantPR:  517, wantHeadSHA: "newhead333", wantTargetID: "target-sync-1", wantSyncCalls: 1,
		},
		{
			name: "check_run failure with PR in payload transitions registry", event: "check_run", delivery: "del-cr-fail",
			payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 1,
		},
		{
			name: "check_run failure for CI Gate is ignored", event: "check_run", delivery: "del-cr-ci-gate-ignore",
			payload: `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 0,
		},
		{
			name: "check_run failure for CI Gate after cancelled upstream check transitions registry", event: "check_run", delivery: "del-cr-ci-gate-after-cancel",
			priorEvent: &ghPriorEvent{
				event:    "check_run",
				delivery: "del-cr-upstream-cancelled",
				payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"cancelhead777","status":"completed","conclusion":"cancelled","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"cancelhead777"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			},
			payload:           `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cancelhead777","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/mirrormere/actions/runs/777","output":{"title":"Gate Failed","summary":"Upstream job cancelled"},"pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"cancelhead777"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantPR:            518,
			wantHeadSHA:       "cancelhead777",
			wantCIFailedCalls: 1,
			wantDispatches:    1,
			wantPromptSubstr:  "Upstream job cancelled",
		},
		{
			name: "check_run success for CI Gate recovers after prior check failure on same SHA", event: "check_run", delivery: "del-cr-ci-gate-recover-fail",
			priorEvent: &ghPriorEvent{
				event:    "check_run",
				delivery: "del-cr-unit-fail-before-rerun",
				payload:  `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"rerunhead888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunhead888"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			},
			payload:           `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"rerunhead888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunhead888"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:               &mockPRRegistry{syncTargetID: "1555405874565091380", ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			wantPR:            530,
			wantHeadSHA:       "rerunhead888",
			wantTargetID:      "1555405874565091380",
			wantSyncCalls:     1,
			wantConclusion:    "success",
			wantCIFailedCalls: 1,
		},
		{
			name: "check_run success for CI Gate recovers after prior cancelled check on same SHA", event: "check_run", delivery: "del-cr-ci-gate-recover-cancel",
			priorEvent: &ghPriorEvent{
				event:    "check_run",
				delivery: "del-cr-cancelled-before-rerun",
				payload:  `{"action":"completed","check_run":{"name":"roborev","head_sha":"cancelhead999","status":"completed","conclusion":"cancelled","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cancelhead999"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			},
			payload:        `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cancelhead999","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cancelhead999"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{syncTargetID: "1555405874565091380"},
			wantPR:         530,
			wantHeadSHA:    "cancelhead999",
			wantTargetID:   "1555405874565091380",
			wantSyncCalls:  1,
			wantConclusion: "success",
		},
		{
			name: "check_run success for CI Gate without prior failure ignores UpdatePRSync", event: "check_run", delivery: "del-cr-ci-gate-clean-success",
			payload:        `{"action":"completed","check_run":{"name":"CI Gate","head_sha":"cleanhead111","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"cleanhead111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{syncTargetID: "1555405874565091380"},
			wantPR:         530,
			wantHeadSHA:    "cleanhead111",
			wantSyncCalls:  0,
			wantConclusion: "success",
		},
		{
			name: "check_run success on rerun for non-gate check recovers after prior failure on same SHA", event: "check_run", delivery: "del-cr-nongate-recover-fail",
			priorEvent: &ghPriorEvent{
				event:    "check_run",
				delivery: "del-cr-perf-fail-before-rerun",
				payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"rerunperf888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunperf888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			},
			payload:              `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"rerunperf888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"rerunperf888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380", ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			wantPR:               530,
			wantHeadSHA:          "rerunperf888",
			wantTargetID:         "1555405874565091380",
			wantSyncCalls:        1,
			wantConclusion:       "success",
			wantCIFailedCalls:    1,
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
	}
}

func processGitHubEventCasesPart4() []ghEventCase {
	return []ghEventCase{
		{
			name: "check_run success for unrelated non-gate check after another check failed on same SHA does not recover", event: "check_run", delivery: "del-cr-unrelated-nongate-no-recover",
			priorEvent: &ghPriorEvent{
				event:    "check_run",
				delivery: "del-cr-perf-fail-parallel",
				payload:  `{"action":"completed","check_run":{"name":"Display performance budget (React)","head_sha":"parallelhead888","status":"completed","conclusion":"failure","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"parallelhead888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			},
			payload:              `{"action":"completed","check_run":{"name":"Unit Tests","head_sha":"parallelhead888","status":"completed","conclusion":"success","pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"parallelhead888"}}]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380", ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			wantPR:               530,
			wantHeadSHA:          "parallelhead888",
			wantSyncCalls:        0,
			wantConclusion:       "success",
			wantCIFailedCalls:    1,
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
		{
			name: "check_run failure for Container Builds Gate is ignored", event: "check_run", delivery: "del-cr-gate-ignore",
			payload: `{"action":"completed","check_run":{"name":"Container Builds Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 0,
		},
		{
			name: "check_run failure for Sidecar Builds Gate is ignored", event: "check_run", delivery: "del-cr-sidecar-gate-ignore",
			payload: `{"action":"completed","check_run":{"name":"Sidecar Builds Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial-sidecars"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 0,
		},
		{
			name: "check_run failure for Configuration Gate is ignored", event: "check_run", delivery: "del-cr-config-gate-ignore",
			payload: `{"action":"completed","check_run":{"name":"Configuration Gate","head_sha":"checkhead444","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"checkhead444"}}]},"repository":{"full_name":"azylman/aerial-config"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 0,
		},
		{
			name: "check_run failure for superseded commit SHA is ignored", event: "check_run", delivery: "del-cr-stale-sha",
			payload: `{"action":"completed","check_run":{"name":"Run Service Unit Tests","head_sha":"stalehead111","status":"completed","conclusion":"failure","pull_requests":[{"number":518,"head":{"ref":"feat/test","sha":"stalehead111"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolveHeadSHA: "newerhead999", ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  518, wantCIFailedCalls: 0,
		},
		{
			name: "check_run failure for roborev enriches prompt with latest comment", event: "check_run", delivery: "del-cr-roborev",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Failed","summary":"Found issues"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Found SQL injection in query",
		},
		{
			name: "check_run failure for roborev produces critical evaluation and override prompt", event: "check_run", delivery: "del-cr-roborev-override",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Failed","summary":"Found issues"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "override-roborev 525",
		},
		{
			name: "check_run success for roborev calls UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-success",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380"},
			wantPR:               525,
			wantHeadSHA:          "robo525abcdef",
			wantTargetID:         "1555405874565091380",
			wantSyncCalls:        1,
			wantConclusion:       "success",
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
		{
			name: "check_run success for roborev recovers after workflow_run failure", event: "check_run", delivery: "del-cr-roborev-recover",
			priorEvent: &ghPriorEvent{
				event:    "workflow_run",
				delivery: "del-wf-cd-fail",
				payload:  `{"action":"completed","workflow_run":{"id":525,"name":"Continuous Delivery","head_sha":"robo525abcdef","head_branch":"feat/test","status":"completed","conclusion":"failure","pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			},
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380", ciFailedTargetID: "1555405874565091380", ciFailedUpdated: false},
			wantPR:               525,
			wantHeadSHA:          "robo525abcdef",
			wantTargetID:         "1555405874565091380",
			wantSyncCalls:        1,
			wantConclusion:       "success",
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
			wantCIFailedCalls:    1,
		},
		{
			name: "check_run success for Check RoboRev Bootstrap State ignores UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-bootstrap-success",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Check RoboRev Bootstrap State","head_sha":"robo525abcdef","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/525","output":{"title":"Bootstrap OK","summary":"All clean"},"pull_requests":[{"number":525,"head":{"ref":"feat/test","sha":"robo525abcdef"}}]}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380"},
			wantPR:               525,
			wantHeadSHA:          "robo525abcdef",
			wantSyncCalls:        0,
			wantConclusion:       "success",
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
		{
			name: "check_run success for roborev without prior roborev failure ignores UpdatePRSync", event: "check_run", delivery: "del-cr-roborev-firstpass-success",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"roborev","head_sha":"headsha789","status":"completed","conclusion":"success","html_url":"https://github.com/azylman/aerial/actions/runs/530","output":{"title":"Review Succeeded","summary":"All clean"},"pull_requests":[{"number":530,"head":{"ref":"feat/test","sha":"headsha789"}}]}}`,
			reg:                  &mockPRRegistry{syncTargetID: "1555405874565091380"},
			wantPR:               530,
			wantHeadSHA:          "headsha789",
			wantSyncCalls:        0,
			wantConclusion:       "success",
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
		{
			name: "check_run failure for standard CI check produces standard fix prompt", event: "check_run", delivery: "del-cr-standard-ci-fail",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Failed"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Please investigate and fix the failure by inspecting the failing logs",
		},
	}
}

func processGitHubEventCasesPart5() []ghEventCase {
	return []ghEventCase{
		{
			name: "check_run failure with ciFailCount exceeding cap suppresses prompt and dispatches cap DM", event: "check_run", delivery: "del-cr-cap",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Failed"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true, ciFailCount: 4},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 1, wantDMSubstr: "remediation cap 3/3 reached", wantPrompts: 0, wantFailVanadis: 1,
		},
		{
			name: "check_run failure with ciFailCount error still dispatches prompt", event: "check_run", delivery: "del-cr-failcount-err",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Lint","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Failed"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true, ciFailCountErr: errors.New("db error")},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Lint",
		},
		{
			name: "check_run timed_out with PR in payload transitions registry", event: "check_run", delivery: "del-cr-timeout",
			payload: `{"action":"completed","check_run":{"name":"Tests","head_sha":"checkhead555","status":"completed","conclusion":"timed_out","pull_requests":[{"number":519,"head":{"ref":"feat/test","sha":"checkhead555"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  519, wantCIFailedCalls: 1,
		},
		{
			name: "check_run failure without PR falls back to headSHA transition", event: "check_run", delivery: "del-cr-fallback",
			payload:     `{"action":"completed","check_run":{"name":"Lint","head_sha":"orphansha666","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
			wantHeadSHA: "orphansha666", wantCIFailedSHAs: 1,
		},
		{
			name: "workflow_run failure with PR transitions registry", event: "workflow_run", delivery: "del-wf-fail",
			payload: `{"action":"completed","workflow_run":{"name":"CI","head_sha":"wfhead777","status":"completed","conclusion":"failure","pull_requests":[{"number":521,"head":{"ref":"feat/wf","sha":"wfhead777"}}]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{ciFailedTargetID: "target-111", ciFailedUpdated: true},
			wantPR:  521, wantCIFailedCalls: 1,
		},
		{
			name: "workflow_run timed_out without PR falls back to headSHA transition", event: "workflow_run", delivery: "del-wf-timeout",
			payload:     `{"action":"completed","workflow_run":{"name":"Build Docker","head_sha":"wfhead888","status":"completed","conclusion":"timed_out","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:         &mockPRRegistry{ciFailedBySHAPRNum: 520, ciFailedBySHATarget: "target-222", ciFailedBySHAUpdated: true},
			wantHeadSHA: "wfhead888", wantCIFailedSHAs: 1,
		},
		{
			name: "push to main with resolved PR backfills merge SHA", event: "push", delivery: "del-push-main",
			payload: `{"ref":"refs/heads/main","after":"pushsha999","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"pushsha999","message":"feat: merge pr"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, backfillTargetID: "target-backfill"},
			wantPR:  522, wantMergeSHA: "pushsha999", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to main falls back to squash commit message when SHA lookup misses", event: "push", delivery: "del-push-msg-fallback",
			payload: `{"ref":"refs/heads/main","after":"unindexedsha898","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha898","message":"refactor(runner): compact streaming daemon (chunk 3/5) (#898)\n\nDetailed body"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, backfillTargetID: "1559768078131269684"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  898, wantMergeSHA: "unindexedsha898", wantTargetID: "1559768078131269684", wantSource: "commit_message", wantBackfillCalls: 1, wantAdvanceCalls: 1, wantGitPushes: 1,
		},
		{
			name: "push to main falls back to merge commit message when SHA lookup misses", event: "push", delivery: "del-push-merge-msg-fallback",
			payload: `{"ref":"refs/heads/main","after":"unindexedsha899","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha899","message":"Merge pull request #899 from azylman/feat/test\n\nMerge body"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, backfillTargetID: "1559768078131269684"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  899, wantMergeSHA: "unindexedsha899", wantTargetID: "1559768078131269684", wantSource: "commit_message", wantBackfillCalls: 1, wantAdvanceCalls: 1, wantGitPushes: 1,
		},
		{
			name: "feature branch push does not backfill", event: "push", delivery: "del-push-feat-reg",
			payload:    `{"ref":"refs/heads/feat/branch-1","after":"featsha888","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"featsha888","message":"feat: wip"}}`,
			reg:        &mockPRRegistry{},
			wantSource: "non_main_branch",
		},
		{
			name: "registry error propagation", event: "pull_request", delivery: "del-err",
			payload: `{"action":"closed","number":515,"pull_request":{"number":515,"head":{"ref":"feat/decouple","sha":"head111"},"merged":true,"merge_commit_sha":"merge111"},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("simulated database failure")},
			wantErr: true,
		},
		{
			name: "check_run local PR resolution priority avoids GitHub API", event: "check_run", delivery: "del-cr-local",
			payload:              `{"action":"completed","check_run":{"name":"Integration Tests","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:                  &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit:       true,
			wantPR:               530,
			wantTargetID:         "thread-local-530",
			wantSource:           "local_pr_registry",
			wantRecordCheckCalls: 1,
			wantRecordCheckState: "passed",
		},
		{
			name: "workflow_run local PR resolution priority avoids GitHub API", event: "workflow_run", delivery: "del-wf-local",
			payload:        `{"action":"completed","workflow_run":{"name":"Build Pipeline","head_sha":"localsha123","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantSource: "local_pr_registry",
		},
		{
			name: "push local PR resolution priority avoids GitHub API", event: "push", delivery: "del-push-local",
			payload:        `{"ref":"refs/heads/main","after":"localsha123","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"localsha123","message":"feat: merged commit"}}`,
			reg:            &mockPRRegistry{mergeAdvanceRes: true, resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantMergeSHA: "localsha123", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
	}
}

func processGitHubEventCasesPart6() []ghEventCase {
	return []ghEventCase{
		{
			name: "push to main triggers DispatchGitPush", event: "push", delivery: "del-push-main-disp",
			payload: `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  542, wantTargetID: "1555405874565091380", wantGitPushes: 1, wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to feature branch does not trigger DispatchGitPush", event: "push", delivery: "del-push-feat-disp",
			payload:       `{"ref":"refs/heads/feat/test-branch","after":"featcommit456","repository":{"full_name":"azylman/aerial"}}`,
			reg:           &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "1555405874565091380"},
			disp:          &mockOutboundDispatcher{},
			wantGitPushes: 0,
		},
		{
			name: "check_run failure with valid snowflake triggers DispatchPrompt", event: "check_run", delivery: "del-cr-fail-prompt",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Build Failed","summary":"Compilation error in brain/main.go:42"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Service Unit Tests",
		},
		{
			name: "check_run failure duplicate does not trigger prompt", event: "check_run", delivery: "del-cr-dup-prompt",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"headsha789","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/123","output":{"title":"Build Failed","summary":"Compilation error in brain/main.go:42"},"pull_requests":[{"number":530,"head":{"ref":"feat/test"}}]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: false},
			disp:              &mockOutboundDispatcher{},
			wantCIFailedCalls: 1, wantPrompts: 0,
		},
		{
			name: "pull_request closed and merged suppresses direct message", event: "pull_request", delivery: "del-pr-merge-disp",
			payload: `{"action":"closed","number":542,"pull_request":{"number":542,"merged":true,"head":{"ref":"feat/batch1","sha":"head542"},"merge_commit_sha":"merge542"},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, mergedTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  542, wantAction: "closed", wantMergedCalls: 1, wantAdvanceCalls: 1, wantDMs: 0,
		},
		{
			name: "pull_request auto_merge_disabled dispatches conflict DM and prompt", event: "pull_request", delivery: "del-amd",
			payload:    `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"},"mergeable_state":"dirty"},"repository":{"full_name":"azylman/aerial"}}`,
			reg:        &mockPRRegistry{conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:       &mockOutboundDispatcher{},
			wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Merge conflict detected on PR #543",
		},
		{
			name: "pull_request auto_merge_disabled error propagating", event: "pull_request", delivery: "del-amd-err",
			payload: `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"},"mergeable_state":"dirty"},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{conflictErr: errors.New("simulated conflict transition error")},
			wantErr: true,
		},
		{
			name: "pull_request auto_merge_disabled clean state ignores conflict", event: "pull_request", delivery: "del-amd-clean",
			payload:    `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"},"mergeable_state":"clean"},"repository":{"full_name":"azylman/aerial"}}`,
			reg:        &mockPRRegistry{conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:       &mockOutboundDispatcher{},
			wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 0, wantDMs: 0, wantDispatches: 0,
		},
		{
			name: "push to main triggers conflict check on open PRs", event: "push", delivery: "del-push-conflict",
			payload:           `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
			reg:               &mockPRRegistry{conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:              &mockOutboundDispatcher{},
			autoUpdateAuthors: []string{"trusted-author"},
			wantConflictCalls: 1, wantGitPushes: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Merge conflict detected on PR #543",
		},
		{
			name: "merged non-final PR on azylman/mirrormere updates merged status and advances Vanadis chunk", event: "pull_request", delivery: "del-pr-mirrormere",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, mergedTargetID: "1562162630997581946"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  210, wantMergedCalls: 1, wantAdvanceCalls: 1, wantPrompts: 0,
		},
		{
			name: "merged Vanadis PR not followed by a pr chunk on azylman/mirrormere defers Vanadis advance until deploy", event: "pull_request", delivery: "del-pr-mirrormere-final",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "1562162630997581946"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  210, wantMergedCalls: 1, wantAdvanceCalls: 0, wantPrompts: 0,
		},
		{
			name: "merged PR on azylman/mirrormere merge gate lookup error propagates", event: "pull_request", delivery: "del-pr-mirrormere-gate-err",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "1562162630997581946", mergeAdvanceErr: errors.New("simulated vanadis lookup error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name: "workflow_run on main without built images advances Vanadis and dispatches git-merge prompt", event: "workflow_run", delivery: "del-wf-main-merge",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge210","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 210, resolveBranch: "feat/ui", resolveTargetID: "1562162630997581946"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  210, wantAdvanceCalls: 1, wantPrompts: 1, wantPromptSubstr: "Continuous Delivery deployment completed for job git-merge on azylman/mirrormere",
		},
		{
			name: "workflow_run on main advance Vanadis error propagates", event: "workflow_run", delivery: "del-wf-main-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge211","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 211, resolveBranch: "feat/ui2", resolveTargetID: "1562162630997581946", advanceVanadisErr: errors.New("simulated advance error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
	}
}

func processGitHubEventCasesPart7() []ghEventCase {
	return []ghEventCase{
		{
			name: "closed unmerged PR calls FailVanadisChunkByPR", event: "pull_request", delivery: "del-pr-discard",
			payload: `{"action":"closed","number":212,"pull_request":{"number":212,"head":{"ref":"feat/discard","sha":"head212"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-m"},
			wantPR:  212, wantClosedCalls: 1, wantFailVanadis: 1,
		},
		{
			name: "push to azylman/aerial-sidecars backfills merge_sha and advances non-final Vanadis chunk", event: "push", delivery: "del-push-sidecars",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars"},
			wantPR:  310, wantMergeSHA: "sidecarsha1", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to azylman/aerial-sidecars defers Vanadis advance when next chunk is not a pr", event: "push", delivery: "del-push-sidecars-final",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars"},
			wantPR:  310, wantMergeSHA: "sidecarsha1", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 0,
		},
		{
			name: "push to azylman/aerial-sidecars advance Vanadis error propagates", event: "push", delivery: "del-push-sidecars-adv-err",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{mergeAdvanceRes: true, resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars", advanceVanadisErr: errors.New("simulated advance error")},
			wantErr: true,
		},
		{
			name: "pull_request closed merged UpdatePRMerged error", event: "pull_request", delivery: "del-pr-merged-err",
			payload: `{"action":"closed","number":215,"pull_request":{"number":215,"head":{"ref":"feat/ui3","sha":"head215"},"merged":true,"merge_commit_sha":"merge215"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("merged db error")},
			wantErr: true,
		},
		{
			name: "pull_request closed unmerged UpdatePRClosedUnmerged error", event: "pull_request", delivery: "del-pr-closed-err",
			payload: `{"action":"closed","number":216,"pull_request":{"number":216,"head":{"ref":"feat/discard2","sha":"head216"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("closed db error")},
			wantErr: true,
		},
		{
			name: "pull_request synchronize UpdatePRSync error", event: "pull_request", delivery: "del-pr-sync-err",
			payload: `{"action":"synchronize","number":217,"pull_request":{"number":217,"head":{"ref":"feat/update2","sha":"head217"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{err: errors.New("sync db error")},
			wantErr: true,
		},
		{
			name: "pull_request opened with aerial-pr trailer self-heals registration", event: "pull_request", delivery: "del-pr-trailer-open",
			payload: `{"action":"opened","number":692,"pull_request":{"number":692,"title":"fix: chunk 2","body":"PR description\n\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\",\"vanadis_chunk_id\":\"build-1792510680-f4ce8eee-chunk-2\",\"jobs\":[\"kiosk-client\"]} -->","head":{"ref":"feat/chunk2","sha":"head692"}},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{resolveTargetID: "1562162630997581946", resolveBranch: "feat/chunk2"}, wantPR: 692, wantTargetID: "1562162630997581946", wantEnsureCalls: 1,
		},
		{
			name: "pull_request with aerial-pr trailer EnsurePRRegistered error propagates", event: "pull_request", delivery: "del-pr-trailer-err",
			payload: `{"action":"opened","number":693,"pull_request":{"number":693,"title":"fix: chunk 3","body":"PR description\n\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\",\"vanadis_chunk_id\":\"build-123-chunk-3\",\"jobs\":[]} -->","head":{"ref":"feat/chunk3","sha":"head693"}},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{ensureErr: errors.New("ensure db error")}, wantErr: true,
		},
		{
			name: "workflow_run on main with expected deploy jobs defers Vanadis and prompt until deploy_success", event: "workflow_run", delivery: "del-wf-mirrormere-expected",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge220","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 220, resolveBranch: "feat/ui-deploy", resolveTargetID: "1562162630997581946", hasExpectedRes: true},
			disp:    &mockOutboundDispatcher{},
			wantPR:  220, wantAdvanceCalls: 0, wantPrompts: 0,
		},
		{
			name: "workflow_run on main without expected deploy jobs for Vanadis PR advances and syncs deployed widget", event: "workflow_run", delivery: "del-wf-mirrormere-no-expected",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge221","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 221, resolveBranch: "feat/ui-no-deploy", resolveTargetID: "1562162630997581946", hasExpectedRes: false, suppressVanadisRes: true},
			disp:    &mockOutboundDispatcher{},
			wantPR:  221, wantAdvanceCalls: 1, wantPrompts: 0,
		},
		{
			name: "workflow_run on main for a Flux-sourced repo hands completion to the kustomization watcher", event: "workflow_run", delivery: "del-wf-aerial-config-flux",
			payload:        `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge222","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial-config"}}`,
			reg:            &mockPRRegistry{resolvePRNum: 222, resolveBranch: "feat/config-only", resolveTargetID: "1562162630997581946", hasExpectedRes: false, suppressVanadisRes: true},
			disp:           &mockOutboundDispatcher{repoKustomizations: map[string][]fluxKustomizationRef{"azylman/aerial-config": {{Namespace: "flux-system", Name: "aerial-config"}}}},
			wantPR:         222,
			wantReconciles: []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-config"}},
		},
		{
			name: "workflow_run on main kustomization discovery error propagates", event: "workflow_run", delivery: "del-wf-aerial-config-discover-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge223","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/aerial-config"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 223, resolveBranch: "feat/config-only", resolveTargetID: "1562162630997581946", hasExpectedRes: false},
			disp:    &mockOutboundDispatcher{repoDiscoverErr: errors.New("kube api down")},
			wantErr: true,
		},
		{
			name: "workflow_run on main completeNoDeployWorkflowRun HasExpectedDeployJobs error propagates", event: "workflow_run", delivery: "del-wf-mirrormere-has-expected-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge221","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 221, resolveBranch: "feat/ui-no-deploy", resolveTargetID: "1562162630997581946", hasExpectedErr: errors.New("simulated has expected deploy jobs error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
	}
}

func processGitHubEventCasesPart8() []ghEventCase {
	return []ghEventCase{
		{
			name: "workflow_run on main completeNoDeployWorkflowRun isVanadisPR error propagates", event: "workflow_run", delivery: "del-wf-mirrormere-is-vanadis-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge221","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 221, resolveBranch: "feat/ui-no-deploy", resolveTargetID: "1562162630997581946", hasExpectedRes: false, suppressVanadisErr: errors.New("simulated is vanadis pr error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
		{
			name:            "dependabot PR opened registers in pr_registry with system channel snowflake",
			event:           "pull_request",
			delivery:        "del-pr-dependabot-reg-1",
			payload:         `{"action":"opened","number":605,"pull_request":{"node_id":"node-ok","number":605,"title":"build(deps): bump bar","head":{"ref":"dependabot/go_modules/bar-1.0.2","sha":"depsha605"},"base":{"ref":"main","sha":"basesha"},"user":{"login":"dependabot[bot]"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"dependabot[bot]"}}`,
			systemChannelID: "1542423172400291873",
			reg:             &mockPRRegistry{},
			wantRepo:        "azylman/aerial",
			wantAction:      "opened",
			wantPR:          605,
			wantBranch:      "dependabot/go_modules/bar-1.0.2",
			wantHeadSHA:     "depsha605",
			wantSender:      "dependabot[bot]",
			wantSource:      "pull_request_payload",
			wantEnsureCalls: 1,
			wantTargetID:    "1542423172400291873",
		},
		{
			name:            "dependabot PR opened registry error logs warning without failing event",
			event:           "pull_request",
			delivery:        "del-pr-dependabot-reg-err",
			payload:         `{"action":"opened","number":606,"pull_request":{"node_id":"node-ok","number":606,"title":"build(deps): bump baz","head":{"ref":"dependabot/go_modules/baz-1.0.2","sha":"depsha606"},"base":{"ref":"main","sha":"basesha"},"user":{"login":"dependabot[bot]"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"dependabot[bot]"}}`,
			systemChannelID: "1542423172400291873",
			reg:             &mockPRRegistry{ensureErr: errors.New("db err")},
			wantErr:         false,
			wantRepo:        "azylman/aerial",
			wantAction:      "opened",
			wantPR:          606,
			wantBranch:      "dependabot/go_modules/baz-1.0.2",
			wantHeadSHA:     "depsha606",
			wantSender:      "dependabot[bot]",
			wantSource:      "pull_request_payload",
			wantEnsureCalls: 1,
		},
		{
			name:              "check_run failure on dependabot branch self-heals registration and transitions CI failed",
			event:             "check_run",
			delivery:          "del-cr-dependabot-fail",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"depfailsha","status":"completed","conclusion":"failure","html_url":"https://github.com/azylman/aerial/actions/runs/999","output":{"title":"Tests Failed","summary":"Error in bar.go"},"pull_requests":[{"number":607,"head":{"ref":"dependabot/pip/foo-1.0","sha":"depfailsha"}}]}}`,
			systemChannelID:   "1542423172400291873",
			reg:               &mockPRRegistry{ciFailedTargetID: "1542423172400291873", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantPR:            607,
			wantBranch:        "dependabot/pip/foo-1.0",
			wantHeadSHA:       "depfailsha",
			wantEnsureCalls:   1,
			wantCIFailedCalls: 1,
			wantDispatches:    1,
			wantTargetID:      "1542423172400291873",
		},
		{
			name:                 "check_run success on dependabot branch skips recording check status and widget sync",
			event:                "check_run",
			delivery:             "del-cr-dependabot-pass",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"deppasssha","status":"completed","conclusion":"success","pull_requests":[{"number":608,"head":{"ref":"dependabot/pip/bar-1.0","sha":"deppasssha"}}]}}`,
			reg:                  &mockPRRegistry{recordCheckUpdated: true},
			disp:                 &mockOutboundDispatcher{},
			wantPR:               608,
			wantBranch:           "dependabot/pip/bar-1.0",
			wantHeadSHA:          "deppasssha",
			wantRecordCheckCalls: 0,
		},
		{
			name:                 "check_run failure cascades open pr conflicts and updates next clean pr",
			event:                "check_run",
			delivery:             "del-cr-cascade-update",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/mirrormere"},"check_run":{"name":"Service Unit Tests","head_sha":"failhead891","status":"completed","conclusion":"failure","pull_requests":[{"number":891,"head":{"ref":"feat/fail-pr","sha":"failhead891"}}]}}`,
			autoUpdateAuthors:    []string{"trusted-author"},
			reg:                  &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:                 &mockOutboundDispatcher{},
			wantPR:               891,
			wantHeadSHA:          "failhead891",
			wantCIFailedCalls:    1,
			wantDispatches:       1,
			wantUpdatedBranchHit: "/repos/azylman/mirrormere/pulls/894/update-branch",
		},
		{
			name:                 "check_run gate aggregator success triggers open pr conflicts check and branch update",
			event:                "check_run",
			delivery:             "del-cr-gate-success-update",
			payload:              `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"CI Gate","head_sha":"head543","status":"completed","conclusion":"success","pull_requests":[{"number":543,"head":{"ref":"feat/conflict-check","sha":"head543"}}]}}`,
			autoUpdateAuthors:    []string{"trusted-author"},
			wantPR:               543,
			wantHeadSHA:          "head543",
			wantUpdatedBranchHit: "/repos/azylman/aerial/pulls/543/update-branch",
		},
		{
			name:              "check_run gate aggregator success handles conflict check error cleanly",
			event:             "check_run",
			delivery:          "del-cr-gate-success-err",
			payload:           `{"action":"completed","repository":{"full_name":"azylman/unknown-repo"},"check_run":{"name":"CI Gate","head_sha":"head999","status":"completed","conclusion":"success","pull_requests":[{"number":999,"head":{"ref":"feat/unknown","sha":"head999"}}]}}`,
			autoUpdateAuthors: []string{"trusted-author"},
			wantPR:            999,
			wantHeadSHA:       "head999",
			wantErr:           false,
		},
		{
			name:     "check_run on fork PR resolves PR and branch from cached pull_request synchronize event",
			event:    "check_run",
			delivery: "del-cr-fork-pr-cache",
			priorEvent: &ghPriorEvent{
				event:    "pull_request",
				delivery: "del-pr-fork-sync",
				payload:  `{"action":"synchronize","number":711,"pull_request":{"number":711,"head":{"ref":"fork/patch-1","sha":"forksha711"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"contributor"}}`,
			},
			payload:           `{"action":"completed","repository":{"full_name":"azylman/aerial"},"check_run":{"name":"Service Unit Tests","head_sha":"forksha711","status":"completed","conclusion":"failure","pull_requests":[]}}`,
			reg:               &mockPRRegistry{ciFailedTargetID: "1555405874565091380", ciFailedUpdated: true},
			disp:              &mockOutboundDispatcher{},
			wantPR:            711,
			wantBranch:        "fork/patch-1",
			wantHeadSHA:       "forksha711",
			wantSyncCalls:     1,
			wantCIFailedCalls: 1,
			wantDispatches:    1,
		},
	}
}
