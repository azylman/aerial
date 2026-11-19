package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/azylman/aerial/pkg/pubsub"
)

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

const testGitHubWebhookSecret = "test-secret"

func signTestGitHubWebhook(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(secret)))
	mac.Write([]byte(payload))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

type ghWebhookTestCase struct {
	name, path, wantMetricSubstr string
	body                         io.Reader
	headers                      map[string]string
	bus                          *mockEventBus
	hasBus                       bool
	webhookSecret                string
	noWebhookSecret              bool
	wantStatus, wantInserted     int
}

func assertGitHubWebhookResponse(t *testing.T, tc ghWebhookTestCase, rec *httptest.ResponseRecorder, routes http.Handler) {
	t.Helper()
	if rec.Code != tc.wantStatus {
		t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
	}
	if tc.wantInserted > 0 && tc.bus != nil {
		if len(tc.bus.insertedJobs) != tc.wantInserted {
			t.Fatalf("expected %d inserted jobs, got %d", tc.wantInserted, len(tc.bus.insertedJobs))
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if resp["status"] != "accepted" || resp["event"] != tc.headers["X-GitHub-Event"] || resp["delivery"] != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected response body: %+v", resp)
		}
		if tc.bus.insertedJobs[0].Event != tc.headers["X-GitHub-Event"] || tc.bus.insertedJobs[0].Delivery != tc.headers["X-GitHub-Delivery"] {
			t.Errorf("unexpected inserted jobs: %+v", tc.bus.insertedJobs)
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
	secret := tc.webhookSecret
	if secret == "" && !tc.noWebhookSecret {
		secret = testGitHubWebhookSecret
	}
	srv := NewRouterServer(fixedConfig(Config{GitHubWebhookSecret: secret}), nil)
	if tc.bus != nil {
		srv.SetEventBus(tc.bus)
	}
	busCh := make(chan string, 1)
	if tc.hasBus {
		memBus := pubsub.NewMemoryBus()
		t.Cleanup(func() { _ = memBus.Close() })
		srv.SetEventBus(memBus)
		srv.onProcessed = func(_, delivery string) {
			select {
			case busCh <- delivery:
			default:
			}
		}
		if err := srv.StartJobConsumers(context.Background()); err != nil {
			t.Fatalf("StartJobConsumers() error: %v", err)
		}
	}
	routes := srv.Routes()
	rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
	assertGitHubWebhookResponse(t, tc, rec, routes)
	if tc.hasBus && tc.wantStatus == http.StatusOK {
		select {
		case gotDel := <-busCh:
			if gotDel != tc.headers["X-GitHub-Delivery"] {
				t.Errorf("bus consumer delivery = %q, want %q", gotDel, tc.headers["X-GitHub-Delivery"])
			}
		case <-time.After(2 * time.Second):
			t.Errorf("timed out waiting for synapse-github bus consumer")
		}
	}
}

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	pingBody := `{"zen":"Keep it simple"}`
	validPingSig := signTestGitHubWebhook(testGitHubWebhookSecret, pingBody)

	tests := []ghWebhookTestCase{
		{name: "missing X-GitHub-Event header", path: "/api/webhooks/github", body: strings.NewReader(`{}`), wantStatus: http.StatusBadRequest},
		{name: "read error in body", path: "/api/webhooks/github", body: errReader{}, headers: map[string]string{"X-GitHub-Event": "ping"}, wantStatus: http.StatusBadRequest},
		{name: "bus nil", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, wantStatus: http.StatusInternalServerError},
		{name: "bus publish error", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{err: errors.New("simulated database failure")}, wantStatus: http.StatusInternalServerError},
		{name: "api path success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "bus only success", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-bus-1", "X-Hub-Signature-256": validPingSig}, hasBus: true, wantStatus: http.StatusOK},
		{name: "legacy path success", path: "/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-1234", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, wantStatus: http.StatusOK, wantInserted: 1},
		{name: "empty secret fails closed without signature", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "empty secret fails closed even with signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": validPingSig}, noWebhookSecret: true, wantStatus: http.StatusUnauthorized},
		{name: "secret configured missing signature header", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature prefix", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha1=1234"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured malformed signature hex", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=invalidhex!"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured mismatched hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-Hub-Signature-256": "sha256=0000000000000000000000000000000000000000000000000000000000000000"}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusUnauthorized},
		{name: "secret configured valid hmac digest", path: "/api/webhooks/github", body: strings.NewReader(pingBody), headers: map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "del-sig-1", "X-Hub-Signature-256": validPingSig}, bus: &mockEventBus{}, webhookSecret: testGitHubWebhookSecret, wantStatus: http.StatusOK, wantInserted: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGitHubWebhookCase(t, tc)
		})
	}
}

type ghPriorEvent struct {
	event    string
	delivery string
	payload  string
}

type ghEventCase struct {
	name                    string
	event                   string
	delivery                string
	payload                 string
	priorEvent              *ghPriorEvent
	priorEvents             []ghPriorEvent
	reg                     *mockPRRegistry
	disp                    *mockOutboundDispatcher
	bus                     *mockEventBus
	spyNoGitHubHit          bool
	systemChannelID         string
	autoUpdateAuthors       []string
	nonDefaultOwner         bool
	wantErr                 bool
	wantRepo                string
	wantAction              string
	wantPR                  int
	wantBranch              string
	wantHeadSHA             string
	wantMergeSHA            string
	wantTargetID            string
	wantSource              string
	wantConclusion          string
	wantSender              string
	wantMergedCalls         int
	wantClosedCalls         int
	wantSyncCalls           int
	wantCIFailedCalls       int
	wantCIFailedSHAs        int
	wantDeployFailedCalls   int
	wantDeployFailedEvents  int
	wantDeployEventMergeSHA string
	wantBackfillCalls       int
	wantConflictCalls       int
	wantAdvanceCalls        int
	wantEnsureCalls         int
	wantGitPushes           int
	wantPrompts             int
	wantDispatches          int
	wantDMs                 int
	wantPromptSubstr        string
	wantDMSubstr            string
	wantRecordCheckCalls    int
	wantRecordCheckState    string
	wantUpdatedBranchHit    string
	wantRerunHit            string
	wantReconciles          []fluxKustomizationRef
	wantCIFailedChecks      []string
	wantSyncEvents          int
}

func newProcessGitHubEventMockServer(ghPaths *sync.Map) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		case strings.Contains(r.URL.Path, "/actions/runs/801/rerun-failed-jobs"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case strings.Contains(r.URL.Path, "/actions/runs/802/rerun-failed-jobs"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"failed to rerun"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

var prAndPushTestCases = []ghEventCase{
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
		name: "push git push dispatcher error still runs open pr conflict check and branch update", event: "push", delivery: "del-push-disp-err-update",
		payload:              `{"ref":"refs/heads/main","before":"prev111","after":"pushsha894","head_commit":{"id":"pushsha894","message":"feat: push"},"repository":{"full_name":"azylman/mirrormere"}}`,
		autoUpdateAuthors:    []string{"trusted-author"},
		disp:                 &mockOutboundDispatcher{gitPushErr: errors.New("flux 429 rate limit")},
		wantErr:              true,
		wantUpdatedBranchHit: "/repos/azylman/mirrormere/pulls/894/update-branch",
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
		reg:     &mockPRRegistry{mergedTargetID: "target-merged-1"},
		wantPR:  515, wantMergeSHA: "merge111", wantTargetID: "target-merged-1", wantMergedCalls: 1, wantAdvanceCalls: 1,
	},
	{
		name: "pull_request closed unmerged transitions registry", event: "pull_request", delivery: "del-pr-closed",
		payload: `{"action":"closed","number":516,"pull_request":{"number":516,"head":{"ref":"feat/discard","sha":"head222"},"merged":false},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
		reg:     &mockPRRegistry{closedTargetID: "target-closed-1"},
		wantPR:  516, wantTargetID: "target-closed-1", wantClosedCalls: 1,
	},
	{
		name: "pull_request synchronize transitions registry", event: "pull_request", delivery: "del-pr-sync",
		payload: `{"action":"synchronize","number":517,"pull_request":{"number":517,"head":{"ref":"feat/update","sha":"newhead333"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"arcane103"}}`,
		reg:     &mockPRRegistry{syncTargetID: "target-sync-1", resolveTargetID: "target-sync-1", resolveBranch: "feat/update"},
		wantPR:  517, wantHeadSHA: "newhead333", wantTargetID: "target-sync-1", wantSyncCalls: 1,
	},
	{
		name: "push to main with resolved PR backfills merge SHA", event: "push", delivery: "del-push-main",
		payload: `{"ref":"refs/heads/main","after":"pushsha999","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"pushsha999","message":"feat: merge pr"}}`,
		reg:     &mockPRRegistry{backfillTargetID: "target-backfill"},
		wantPR:  522, wantMergeSHA: "pushsha999", wantBackfillCalls: 1, wantAdvanceCalls: 1,
	},
	{
		name: "push to main falls back to squash commit message when SHA lookup misses", event: "push", delivery: "del-push-msg-fallback",
		payload: `{"ref":"refs/heads/main","after":"unindexedsha898","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha898","message":"refactor(runner): compact streaming daemon (chunk 3/5) (#898)\n\nDetailed body"}}`,
		reg:     &mockPRRegistry{backfillTargetID: "100000000000000003"},
		disp:    &mockOutboundDispatcher{},
		wantPR:  898, wantMergeSHA: "unindexedsha898", wantTargetID: "100000000000000003", wantSource: "commit_message", wantBackfillCalls: 1, wantAdvanceCalls: 1, wantGitPushes: 1,
	},
	{
		name: "push to main falls back to merge commit message when SHA lookup misses", event: "push", delivery: "del-push-merge-msg-fallback",
		payload: `{"ref":"refs/heads/main","after":"unindexedsha899","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha899","message":"Merge pull request #899 from azylman/feat/test\n\nMerge body"}}`,
		reg:     &mockPRRegistry{backfillTargetID: "100000000000000003"},
		disp:    &mockOutboundDispatcher{},
		wantPR:  899, wantMergeSHA: "unindexedsha899", wantTargetID: "100000000000000003", wantSource: "commit_message", wantBackfillCalls: 1, wantAdvanceCalls: 1, wantGitPushes: 1,
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
		name: "push local PR resolution priority avoids GitHub API", event: "push", delivery: "del-push-local",
		payload:        `{"ref":"refs/heads/main","after":"localsha123","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"localsha123","message":"feat: merged commit"}}`,
		reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
		spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantMergeSHA: "localsha123", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
	},
	{
		name: "push to main triggers DispatchGitPush", event: "push", delivery: "del-push-main-disp",
		payload: `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "100000000000000001"},
		disp:    &mockOutboundDispatcher{},
		wantPR:  542, wantTargetID: "100000000000000001", wantGitPushes: 1, wantBackfillCalls: 1, wantAdvanceCalls: 1,
	},
	{
		name: "push to feature branch does not trigger DispatchGitPush", event: "push", delivery: "del-push-feat-disp",
		payload:       `{"ref":"refs/heads/feat/test-branch","after":"featcommit456","repository":{"full_name":"azylman/aerial"}}`,
		reg:           &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "100000000000000001"},
		disp:          &mockOutboundDispatcher{},
		wantGitPushes: 0,
	},
	{
		name: "pull_request closed and merged suppresses direct message", event: "pull_request", delivery: "del-pr-merge-disp",
		payload: `{"action":"closed","number":542,"pull_request":{"number":542,"merged":true,"head":{"ref":"feat/batch1","sha":"head542"},"merge_commit_sha":"merge542"},"repository":{"full_name":"azylman/aerial"}}`,
		reg:     &mockPRRegistry{mergedTargetID: "100000000000000001"},
		disp:    &mockOutboundDispatcher{},
		wantPR:  542, wantAction: "closed", wantMergedCalls: 1, wantAdvanceCalls: 1, wantDMs: 0,
	},
	{
		name: "pull_request auto_merge_disabled transitions conflict without prompt", event: "pull_request", delivery: "del-amd",
		payload:    `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"},"mergeable_state":"dirty"},"repository":{"full_name":"azylman/aerial"}}`,
		reg:        &mockPRRegistry{conflictTargetID: "100000000000000001", conflictUpdated: true},
		disp:       &mockOutboundDispatcher{},
		wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 1, wantDMs: 0, wantDispatches: 0,
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
		reg:        &mockPRRegistry{conflictTargetID: "100000000000000001", conflictUpdated: true},
		disp:       &mockOutboundDispatcher{},
		wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 0, wantDMs: 0, wantDispatches: 0,
	},
	{
		name: "push to main triggers conflict check on open PRs", event: "push", delivery: "del-push-conflict",
		payload:           `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
		reg:               &mockPRRegistry{conflictTargetID: "100000000000000001", conflictUpdated: true},
		disp:              &mockOutboundDispatcher{},
		autoUpdateAuthors: []string{"trusted-author"},
		wantConflictCalls: 1, wantGitPushes: 1, wantDMs: 0, wantDispatches: 0,
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
		payload: `{"action":"opened","number":692,"pull_request":{"number":692,"title":"fix: chunk 2","body":"PR description\n\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"chunk_id\":\"build-1792510680-f4ce8eee-chunk-2\",\"jobs\":[\"kiosk-client\"]} -->","head":{"ref":"feat/chunk2","sha":"head692"}},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
		reg:     &mockPRRegistry{resolveTargetID: "100000000000000002", resolveBranch: "feat/chunk2"}, wantPR: 692, wantTargetID: "100000000000000002", wantEnsureCalls: 1,
	},
	{
		name: "pull_request with aerial-pr trailer EnsurePRRegistered error propagates", event: "pull_request", delivery: "del-pr-trailer-err",
		payload: `{"action":"opened","number":693,"pull_request":{"number":693,"title":"fix: chunk 3","body":"PR description\n\n<!-- aerial-pr: {\"target_id\":\"100000000000000002\",\"chunk_id\":\"build-123-chunk-3\",\"jobs\":[]} -->","head":{"ref":"feat/chunk3","sha":"head693"}},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
		reg:     &mockPRRegistry{ensureErr: errors.New("ensure db error")}, wantErr: true,
	},
	{
		name: "dependabot PR opened registers in pr_registry with system channel snowflake", event: "pull_request", delivery: "del-pr-dependabot-reg-1",
		payload:         `{"action":"opened","number":605,"pull_request":{"node_id":"node-ok","number":605,"title":"build(deps): bump bar","head":{"ref":"dependabot/go_modules/bar-1.0.2","sha":"depsha605"},"base":{"ref":"main","sha":"basesha"},"user":{"login":"dependabot[bot]"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"dependabot[bot]"}}`,
		systemChannelID: "100000000000000004", reg: &mockPRRegistry{},
		wantRepo: "azylman/aerial", wantAction: "opened", wantPR: 605, wantBranch: "dependabot/go_modules/bar-1.0.2", wantHeadSHA: "depsha605", wantSender: "dependabot[bot]", wantSource: "pull_request_payload", wantEnsureCalls: 1, wantTargetID: "100000000000000004",
	},
	{
		name: "dependabot PR opened registry error logs warning without failing event", event: "pull_request", delivery: "del-pr-dependabot-reg-err",
		payload:         `{"action":"opened","number":606,"pull_request":{"node_id":"node-ok","number":606,"title":"build(deps): bump baz","head":{"ref":"dependabot/go_modules/baz-1.0.2","sha":"depsha606"},"base":{"ref":"main","sha":"basesha"},"user":{"login":"dependabot[bot]"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"dependabot[bot]"}}`,
		systemChannelID: "100000000000000004", reg: &mockPRRegistry{ensureErr: errors.New("db err")},
		wantRepo: "azylman/aerial", wantAction: "opened", wantPR: 606, wantBranch: "dependabot/go_modules/baz-1.0.2", wantHeadSHA: "depsha606", wantSender: "dependabot[bot]", wantSource: "pull_request_payload", wantEnsureCalls: 1,
	},
	{
		name: "dependabot PR opened on non-default owner skips registration and leaves targetID empty", event: "pull_request", delivery: "del-pr-dependabot-non-default-owner",
		payload:         `{"action":"opened","number":609,"pull_request":{"node_id":"node-ok","number":609,"title":"build(deps): bump qux","head":{"ref":"dependabot/go_modules/qux-1.0.0","sha":"depsha609"},"base":{"ref":"main","sha":"basesha"},"user":{"login":"dependabot[bot]"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"dependabot[bot]"}}`,
		systemChannelID: "100000000000000004", nonDefaultOwner: true, reg: &mockPRRegistry{},
		wantRepo: "azylman/aerial", wantAction: "opened", wantPR: 609, wantBranch: "dependabot/go_modules/qux-1.0.0", wantHeadSHA: "depsha609", wantSender: "dependabot[bot]", wantSource: "pull_request_payload",
	},
}

func TestProcessGitHubEvent_PullRequestAndPush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range prAndPushTestCases {
		t.Run(tc.name, func(t *testing.T) {
			runProcessGitHubEventCase(ctx, t, tc)
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
	if len(tc.reg.deployFailedCalls) != tc.wantDeployFailedCalls {
		t.Errorf("deployFailedCalls = %d, want %d", len(tc.reg.deployFailedCalls), tc.wantDeployFailedCalls)
	}
	if tc.wantPR != 0 && tc.wantDeployFailedCalls > 0 && len(tc.reg.deployFailedCalls) > 0 && tc.reg.deployFailedCalls[0].prNumber != tc.wantPR {
		t.Errorf("deployFailedCalls prNumber = %d, want %d", tc.reg.deployFailedCalls[0].prNumber, tc.wantPR)
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
	for _, hitPath := range []string{tc.wantUpdatedBranchHit, tc.wantRerunHit} {
		if hitPath != "" {
			if _, hit := ghPaths.Load(hitPath); !hit {
				t.Errorf("expected hit for %q, but was not called", hitPath)
			}
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

func runProcessGitHubEventCase(ctx context.Context, t *testing.T, tc ghEventCase) {
	t.Helper()
	t.Parallel()
	var ghPaths sync.Map
	mockServer := newProcessGitHubEventMockServer(&ghPaths)
	defer mockServer.Close()
	var inserters []pubsub.Bus
	activeBus := tc.bus
	if activeBus == nil && (len(tc.wantCIFailedChecks) > 0 || tc.wantDeployFailedEvents > 0 || tc.wantSyncEvents > 0) {
		activeBus = &mockEventBus{}
	}
	if activeBus != nil {
		inserters = append(inserters, activeBus)
	}
	var reg PRRegistryUpdater
	if tc.reg != nil {
		reg = tc.reg
	}
	var disp OutboundDispatcher
	if tc.disp != nil {
		disp = tc.disp
	}
	cfg := legacyFixtureConfig(Config{GitHubAPIURL: mockServer.URL, GitHubToken: "test-token", SystemChannelID: tc.systemChannelID, AutoUpdatePRAuthors: tc.autoUpdateAuthors, DefaultOwner: !tc.nonDefaultOwner})
	srv := newTestRouter(cfg, legacyFixtureClient(mockServer.Client()), reg, disp, inserters...)
	srv.onProcessed = func(string, string) {}
	if tc.priorEvent != nil {
		if _, err := srv.ProcessGitHubEvent(ctx, tc.priorEvent.event, tc.priorEvent.delivery, []byte(tc.priorEvent.payload)); err != nil {
			t.Fatalf("priorEvent failed: %v", err)
		}
	}
	for i, pe := range tc.priorEvents {
		if _, err := srv.ProcessGitHubEvent(ctx, pe.event, pe.delivery, []byte(pe.payload)); err != nil {
			t.Fatalf("priorEvents[%d] failed: %v", i, err)
		}
	}
	res, err := srv.ProcessGitHubEvent(ctx, tc.event, tc.delivery, []byte(tc.payload))
	if (err != nil) != tc.wantErr {
		t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
	}
	if tc.wantErr {
		if tc.wantUpdatedBranchHit != "" {
			if _, hit := ghPaths.Load(tc.wantUpdatedBranchHit); !hit {
				t.Errorf("expected hit for %q, but was not called", tc.wantUpdatedBranchHit)
			}
		}
		return
	}
	assertEventResult(t, tc, res, &ghPaths)
	assertEventBusCalls(t, tc, activeBus)
}

func assertDeployFailedEvents(t *testing.T, tc ghEventCase, bus *mockEventBus) {
	t.Helper()
	if bus == nil {
		t.Fatalf("wantDeployFailedEvents specified but no bus available")
	}
	deployEventsCount := 0
	for _, evt := range bus.publishedPREvents {
		if evt.Event == eventDeployFailed {
			deployEventsCount++
			if tc.wantPR != 0 && evt.PRNumber != tc.wantPR {
				t.Errorf("deploy_failed PRNumber = %d, want %d", evt.PRNumber, tc.wantPR)
			}
			if tc.wantDeployEventMergeSHA != "" && evt.MergeSHA != tc.wantDeployEventMergeSHA {
				t.Errorf("deploy_failed MergeSHA = %q, want %q", evt.MergeSHA, tc.wantDeployEventMergeSHA)
			}
			if tc.wantTargetID != "" && evt.TargetID != tc.wantTargetID {
				t.Errorf("deploy_failed TargetID = %q, want %q", evt.TargetID, tc.wantTargetID)
			}
		}
	}
	if deployEventsCount != tc.wantDeployFailedEvents {
		t.Errorf("deploy_failed events count = %d, want %d", deployEventsCount, tc.wantDeployFailedEvents)
	}
}

func assertSyncEvents(t *testing.T, tc ghEventCase, bus *mockEventBus) {
	t.Helper()
	if bus == nil {
		t.Fatalf("wantSyncEvents specified but no bus available")
	}
	syncCount := 0
	for _, evt := range bus.publishedPREvents {
		if evt.Event == "synchronize" {
			syncCount++
			if tc.wantPR != 0 && evt.PRNumber != tc.wantPR {
				t.Errorf("synchronize PRNumber = %d, want %d", evt.PRNumber, tc.wantPR)
			}
			if tc.wantHeadSHA != "" && evt.HeadSHA != tc.wantHeadSHA {
				t.Errorf("synchronize HeadSHA = %q, want %q", evt.HeadSHA, tc.wantHeadSHA)
			}
		}
	}
	if syncCount != tc.wantSyncEvents {
		t.Errorf("synchronize events count = %d, want %d", syncCount, tc.wantSyncEvents)
	}
}

func assertEventBusCalls(t *testing.T, tc ghEventCase, bus *mockEventBus) {
	t.Helper()
	if len(tc.wantCIFailedChecks) > 0 {
		if bus == nil {
			t.Fatalf("wantCIFailedChecks specified but no bus available")
		}
		var gotChecks []string
		for _, evt := range bus.publishedPREvents {
			if evt.Event == "ci_failed" {
				gotChecks = append(gotChecks, firstNonEmpty(evt.CheckName, evt.JobName))
			}
		}
		if !slices.Equal(gotChecks, tc.wantCIFailedChecks) {
			t.Errorf("ci_failed event checks = %v, want %v", gotChecks, tc.wantCIFailedChecks)
		}
	}
	if tc.wantDeployFailedEvents > 0 {
		assertDeployFailedEvents(t, tc, bus)
	}
	if tc.wantSyncEvents > 0 {
		assertSyncEvents(t, tc, bus)
	}
}
