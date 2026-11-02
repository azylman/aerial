package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- 3. GitHub Webhook HTTP Handler & Event Processing ---

func TestHandleGitHubWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, path, wantMetricSubstr string
		body                         io.Reader
		headers                      map[string]string
		river                        *mockRiverInserter
		wantStatus, wantInserted     int
	}{
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
			t.Parallel()
			var srv *RouterServer
			if tc.river != nil {
				srv = NewRouterServer(Config{}, nil, tc.river)
			} else {
				srv = NewRouterServer(Config{}, nil)
			}
			routes := srv.Routes()

			rec := doHTTPRequest(routes, http.MethodPost, tc.path, tc.body, tc.headers)
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

	cases := []ghEventCase{
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
			reg:     &mockPRRegistry{mergedTargetID: "target-merged-1"},
			wantPR:  515, wantMergeSHA: "merge111", wantTargetID: "target-merged-1", wantMergedCalls: 1, wantAdvanceCalls: 1,
		},
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
			reg:     &mockPRRegistry{backfillTargetID: "target-backfill"},
			wantPR:  522, wantMergeSHA: "pushsha999", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to main falls back to squash commit message when SHA lookup misses", event: "push", delivery: "del-push-msg-fallback",
			payload: `{"ref":"refs/heads/main","after":"unindexedsha898","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha898","message":"refactor(runner): compact streaming daemon (chunk 3/5) (#898)\n\nDetailed body"}}`,
			reg:     &mockPRRegistry{backfillTargetID: "1559768078131269684"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  898, wantMergeSHA: "unindexedsha898", wantTargetID: "1559768078131269684", wantSource: "commit_message", wantBackfillCalls: 1, wantAdvanceCalls: 1, wantGitPushes: 1,
		},
		{
			name: "push to main falls back to merge commit message when SHA lookup misses", event: "push", delivery: "del-push-merge-msg-fallback",
			payload: `{"ref":"refs/heads/main","after":"unindexedsha899","repository":{"full_name":"azylman/aerial"},"head_commit":{"id":"unindexedsha899","message":"Merge pull request #899 from azylman/feat/test\n\nMerge body"}}`,
			reg:     &mockPRRegistry{backfillTargetID: "1559768078131269684"},
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
			reg:            &mockPRRegistry{resolvePRNum: 530, resolveBranch: "feat/local-resolved", resolveTargetID: "thread-local-530"},
			spyNoGitHubHit: true, wantPR: 530, wantTargetID: "thread-local-530", wantMergeSHA: "localsha123", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to main triggers DispatchGitPush", event: "push", delivery: "del-push-main-disp",
			payload: `{"ref":"refs/heads/main","after":"maincommit123","repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 542, resolveBranch: "main", resolveTargetID: "1555405874565091380"},
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
			reg:     &mockPRRegistry{mergedTargetID: "1555405874565091380"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  542, wantAction: "closed", wantMergedCalls: 1, wantAdvanceCalls: 1, wantDMs: 0,
		},
		{
			name: "pull_request auto_merge_disabled dispatches conflict DM and prompt", event: "pull_request", delivery: "del-amd",
			payload:    `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"}},"repository":{"full_name":"azylman/aerial"}}`,
			reg:        &mockPRRegistry{conflictTargetID: "1555405874565091380", conflictUpdated: true},
			disp:       &mockOutboundDispatcher{},
			wantAction: "auto_merge_disabled", wantPR: 543, wantConflictCalls: 1, wantDMs: 0, wantDispatches: 1, wantPromptSubstr: "Merge conflict detected on PR #543",
		},
		{
			name: "pull_request auto_merge_disabled error propagating", event: "pull_request", delivery: "del-amd-err",
			payload: `{"action":"auto_merge_disabled","number":543,"pull_request":{"number":543,"head":{"ref":"feat/my-feature","sha":"head543"}},"repository":{"full_name":"azylman/aerial"}}`,
			reg:     &mockPRRegistry{conflictErr: errors.New("simulated conflict transition error")},
			wantErr: true,
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
			reg:     &mockPRRegistry{mergedTargetID: "1562162630997581946"},
			disp:    &mockOutboundDispatcher{},
			wantPR:  210, wantMergedCalls: 1, wantAdvanceCalls: 1, wantPrompts: 0,
		},
		{
			name: "merged final Vanadis PR on azylman/mirrormere defers Vanadis advance until terminal deploy", event: "pull_request", delivery: "del-pr-mirrormere-final",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "1562162630997581946", finalVanadisRes: true},
			disp:    &mockOutboundDispatcher{},
			wantPR:  210, wantMergedCalls: 1, wantAdvanceCalls: 0, wantPrompts: 0,
		},
		{
			name: "merged PR on azylman/mirrormere advance Vanadis error propagates", event: "pull_request", delivery: "del-pr-mirrormere-adv-err",
			payload: `{"action":"closed","number":210,"pull_request":{"number":210,"head":{"ref":"feat/ui","sha":"head210"},"merged":true,"merge_commit_sha":"merge210"},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{mergedTargetID: "1562162630997581946", advanceVanadisErr: errors.New("simulated advance error")},
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
		{
			name: "closed unmerged PR calls FailVanadisChunkByPR", event: "pull_request", delivery: "del-pr-discard",
			payload: `{"action":"closed","number":212,"pull_request":{"number":212,"head":{"ref":"feat/discard","sha":"head212"},"merged":false},"repository":{"full_name":"azylman/mirrormere"},"sender":{"login":"arcane103"}}`,
			reg:     &mockPRRegistry{closedTargetID: "target-closed-m"},
			wantPR:  212, wantClosedCalls: 1, wantFailVanadis: 1,
		},
		{
			name: "push to azylman/aerial-sidecars backfills merge_sha and advances non-final Vanadis chunk", event: "push", delivery: "del-push-sidecars",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars"},
			wantPR:  310, wantMergeSHA: "sidecarsha1", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 1,
		},
		{
			name: "push to azylman/aerial-sidecars defers Vanadis advance for final chunk", event: "push", delivery: "del-push-sidecars-final",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars", finalVanadisRes: true},
			wantPR:  310, wantMergeSHA: "sidecarsha1", wantSource: "local_pr_registry", wantBackfillCalls: 1, wantAdvanceCalls: 0,
		},
		{
			name: "push to azylman/aerial-sidecars advance Vanadis error propagates", event: "push", delivery: "del-push-sidecars-adv-err",
			payload: `{"ref":"refs/heads/main","after":"sidecarsha1","repository":{"full_name":"azylman/aerial-sidecars"},"head_commit":{"id":"sidecarsha1","message":"feat: merged sidecar (#310)"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 310, resolveBranch: "feat/sidecar", resolveTargetID: "target-sidecars", backfillTargetID: "target-sidecars", advanceVanadisErr: errors.New("simulated advance error")},
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
			name: "workflow_run on main completeNoDeployWorkflowRun HasExpectedDeployJobs error propagates", event: "workflow_run", delivery: "del-wf-mirrormere-has-expected-err",
			payload: `{"action":"completed","workflow_run":{"id":800,"name":"Continuous Delivery","head_sha":"merge221","head_branch":"main","status":"completed","conclusion":"success","pull_requests":[]},"repository":{"full_name":"azylman/mirrormere"}}`,
			reg:     &mockPRRegistry{resolvePRNum: 221, resolveBranch: "feat/ui-no-deploy", resolveTargetID: "1562162630997581946", hasExpectedErr: errors.New("simulated has expected deploy jobs error")},
			disp:    &mockOutboundDispatcher{},
			wantErr: true,
		},
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			srv := newTestRouter(Config{GitHubAPIURL: mockServer.URL, GitHubToken: "test-token", SystemChannelID: tc.systemChannelID, AutoUpdatePRAuthors: tc.autoUpdateAuthors}, mockServer.Client(), reg, disp, inserters...)
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
			if tc.wantRepo != "" && res.Repo != tc.wantRepo {
				t.Errorf("Repo = %q, want %q", res.Repo, tc.wantRepo)
			}
			if tc.wantAction != "" && res.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", res.Action, tc.wantAction)
			}
			if tc.wantPR != 0 && res.PRNumber != tc.wantPR {
				t.Errorf("PRNumber = %d, want %d", res.PRNumber, tc.wantPR)
			}
			if tc.wantBranch != "" && res.Branch != tc.wantBranch {
				t.Errorf("Branch = %q, want %q", res.Branch, tc.wantBranch)
			}
			if tc.wantHeadSHA != "" && res.HeadSHA != tc.wantHeadSHA {
				t.Errorf("HeadSHA = %q, want %q", res.HeadSHA, tc.wantHeadSHA)
			}
			if tc.wantMergeSHA != "" && res.MergeSHA != tc.wantMergeSHA {
				t.Errorf("MergeSHA = %q, want %q", res.MergeSHA, tc.wantMergeSHA)
			}
			if tc.wantTargetID != "" && res.TargetID != tc.wantTargetID {
				t.Errorf("TargetID = %q, want %q", res.TargetID, tc.wantTargetID)
			}
			if tc.wantSource != "" && res.ResolutionSource != tc.wantSource {
				t.Errorf("ResolutionSource = %q, want %q", res.ResolutionSource, tc.wantSource)
			}
			if tc.wantConclusion != "" && res.Conclusion != tc.wantConclusion {
				t.Errorf("Conclusion = %q, want %q", res.Conclusion, tc.wantConclusion)
			}
			if tc.wantSender != "" && res.Sender != tc.wantSender {
				t.Errorf("Sender = %q, want %q", res.Sender, tc.wantSender)
			}
			if tc.reg != nil {
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
				if tc.wantRecordCheckState != "" && len(tc.reg.recordCheckCalls) > 0 {
					if tc.reg.recordCheckCalls[0].checkState != tc.wantRecordCheckState {
						t.Errorf("recordCheckCalls checkState = %q, want %q", tc.reg.recordCheckCalls[0].checkState, tc.wantRecordCheckState)
					}
				}
			}
			if tc.disp != nil {
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
				dispatches := tc.disp.DispatchCalls()
				if len(dispatches) != tc.wantDispatches {
					t.Errorf("dispatches = %d, want %d", len(dispatches), tc.wantDispatches)
				}
				if tc.wantPromptSubstr != "" && tc.wantDispatches > 0 {
					if len(dispatches) == 0 || !strings.Contains(dispatches[len(dispatches)-1].Prompt, tc.wantPromptSubstr) {
						t.Errorf("expected dispatch to contain %q, got %+v", tc.wantPromptSubstr, dispatches)
					}
				}
				prompts := tc.disp.PromptCalls()
				if len(prompts) != tc.wantPrompts {
					t.Errorf("prompts = %d, want %d", len(prompts), tc.wantPrompts)
				}
				if tc.wantPromptSubstr != "" && tc.wantPrompts > 0 {
					if len(prompts) == 0 || !strings.Contains(prompts[len(prompts)-1].Prompt, tc.wantPromptSubstr) {
						t.Errorf("expected prompt to contain %q, got %+v", tc.wantPromptSubstr, prompts)
					}
				}
				dms := tc.disp.DirectMessageCalls()
				if len(dms) != tc.wantDMs {
					t.Errorf("dms = %d, want %d", len(dms), tc.wantDMs)
				}
				if tc.wantDMSubstr != "" {
					if len(dms) == 0 || !strings.Contains(dms[len(dms)-1].Content, tc.wantDMSubstr) {
						t.Errorf("expected DM to contain %q, got %+v", tc.wantDMSubstr, dms)
					}
				}
			}
		})
	}
}

// --- 6. Workflow Run Image Resolution & Dispatch ---

func TestResolveWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockJobs := GitHubWorkflowJobsResponse{
		TotalCount: 7,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 1, Name: "Detect Changed Microservices", Status: "completed", Conclusion: "success"},
			{ID: 2, Name: "Run Service Unit Tests", Status: "completed", Conclusion: "success"},
			{ID: 3, Name: "Build & Push Images to GHCR (brain)", Status: "completed", Conclusion: "success"},
			{ID: 4, Name: "Build & Push Images to GHCR (synapse)", Status: "completed", Conclusion: "success"},
			{ID: 5, Name: "Build & Push Images to GHCR (custom-microservice)", Status: "completed", Conclusion: "success"},
			{ID: 6, Name: "Build & Push Images to GHCR (failed-service)", Status: "completed", Conclusion: "failure"},
			{ID: 7, Name: "Build & Push Images to GHCR (running-service)", Status: "in_progress", Conclusion: ""},
		},
	}
	mockSidecarJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 10, Name: "Build & Push Sidecar Images to GHCR (banana)", Status: "completed", Conclusion: "success"},
			{ID: 11, Name: "Build & Push Sidecar Images to GHCR (voice-cuda)", Status: "completed", Conclusion: "success"},
		},
	}
	mockMirrormereJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 20, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success"},
			{ID: 21, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success"},
		},
	}
	mockSkippedStepJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{
				ID: 101, Name: "Build & Publish Container Image (mirrormere)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "skipped", Number: 2},
				},
			},
			{
				ID: 102, Name: "Build & Publish Container Image (mirrormere-cast-watcher)", Status: "completed", Conclusion: "success",
				Steps: []GitHubWorkflowJobStep{
					{Name: "Checkout Repository", Status: "completed", Conclusion: "success", Number: 1},
					{Name: "Build and Push Docker Image", Status: "completed", Conclusion: "success", Number: 2},
				},
			},
		},
	}
	mockSkippedPromotionJobs := GitHubWorkflowJobsResponse{
		TotalCount: 2,
		Jobs: []GitHubWorkflowJobItem{
			{ID: 201, Name: "Build & Push Images to GHCR (eri)", Status: "completed", Conclusion: "success"},
			{ID: 202, Name: "Promote Images to :latest", Status: "completed", Conclusion: "skipped"},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/actions/runs/12345/jobs"):
			_ = json.NewEncoder(w).Encode(mockJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/54321/jobs"):
			_ = json.NewEncoder(w).Encode(mockSidecarJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/67890/jobs"):
			_ = json.NewEncoder(w).Encode(mockMirrormereJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/77777/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedStepJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/88888/jobs"):
			_ = json.NewEncoder(w).Encode(mockSkippedPromotionJobs)
		case strings.Contains(r.URL.Path, "/actions/runs/403/jobs"):
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		case strings.Contains(r.URL.Path, "/actions/runs/500/jobs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/runs/999/jobs"):
			_, _ = w.Write([]byte("malformed json"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)

	srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())

	cases := []struct {
		name    string
		repo    string
		runID   int64
		want    []string
		wantErr bool
	}{
		{
			name:  "aerial monorepo images",
			repo:  "azylman/aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "unprefixed repo normalization",
			repo:  "aerial",
			runID: 12345,
			want: []string{
				"ghcr.io/azylman/brain:latest",
				"ghcr.io/azylman/synapse:latest",
				"ghcr.io/azylman/custom-microservice:latest",
			},
		},
		{
			name:  "aerial-sidecars images",
			repo:  "azylman/aerial-sidecars",
			runID: 54321,
			want: []string{
				"ghcr.io/azylman/banana:latest",
				"ghcr.io/azylman/voice-cuda:latest",
			},
		},
		{
			name:  "mirrormere images",
			repo:  "azylman/mirrormere",
			runID: 67890,
			want: []string{
				"ghcr.io/azylman/mirrormere:latest",
				"ghcr.io/azylman/mirrormere-cast-watcher:latest",
			},
		},
		{
			name:  "skipped build step filtering",
			repo:  "azylman/mirrormere",
			runID: 77777,
			want:  []string{"ghcr.io/azylman/mirrormere-cast-watcher:latest"},
		},
		{
			name:  "skipped image promotion returns nil",
			repo:  "azylman/aerial",
			runID: 88888,
			want:  nil,
		},
		{name: "empty repo", repo: "", runID: 12345, wantErr: true},
		{name: "zero runID", repo: "aerial", runID: 0, wantErr: true},
		{name: "403 rate limit", repo: "aerial", runID: 403, wantErr: true},
		{name: "500 server error", repo: "aerial", runID: 500, wantErr: true},
		{name: "malformed json", repo: "aerial", runID: 999, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			images, err := srv.resolveWorkflowRunImages(ctx, tc.repo, tc.runID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveWorkflowRunImages() err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if len(images) != len(tc.want) {
					t.Fatalf("expected %v, got %v", tc.want, images)
				}
				for i := range tc.want {
					if images[i] != tc.want[i] {
						t.Errorf("image[%d] = %q, want %q", i, images[i], tc.want[i])
					}
				}
			}
		})
	}
}

func TestDispatchWorkflowRunImages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name            string
		isGitPush       bool
		ref             string
		commit          string
		headBranch      string
		conclusion      string
		jobsStatus      int
		jobsBody        string
		noDisp          bool
		gitPushErr      error
		imageReadyErr   error
		deployingErr    error
		hasExpectedErr  error
		suppressVanadis bool
		widgetErr       error
		wantImageCalls  int
		wantDeployCalls int
		wantWidgetCalls int
		wantErr         bool
	}{
		{
			name:            "main success with matched jobs and transition deploying",
			headBranch:      "main",
			conclusion:      "success",
			jobsStatus:      http.StatusOK,
			jobsBody:        `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			wantImageCalls:  1,
			wantDeployCalls: 1,
			wantWidgetCalls: 1,
		},
		{
			name:            "main success with matched jobs widget sync warning still succeeds",
			headBranch:      "main",
			conclusion:      "success",
			jobsStatus:      http.StatusOK,
			jobsBody:        `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			widgetErr:       errors.New("transient widget error"),
			wantImageCalls:  1,
			wantDeployCalls: 1,
			wantWidgetCalls: 1,
		},
		{
			name:       "jobs API 500 error",
			headBranch: "main",
			conclusion: "success",
			jobsStatus: http.StatusInternalServerError,
			jobsBody:   `api error`,
			wantErr:    true,
		},
		{
			name:       "0 resolved images returns nil",
			headBranch: "main",
			conclusion: "success",
			jobsStatus: http.StatusOK,
			jobsBody:   `{"jobs":[]}`,
			wantErr:    false,
		},
		{
			name:            "0 resolved images with hasExpectedErr and Vanadis widget sync warning",
			headBranch:      "main",
			conclusion:      "success",
			jobsStatus:      http.StatusOK,
			jobsBody:        `{"jobs":[]}`,
			hasExpectedErr:  errors.New("expected err"),
			suppressVanadis: true,
			widgetErr:       errors.New("widget sync err"),
			wantErr:         true,
		},
		{
			name:           "resolved image with 0 matched Nomad jobs completes no-deploy workflow run",
			headBranch:     "main",
			conclusion:     "success",
			jobsStatus:     http.StatusOK,
			jobsBody:       `{"jobs":[{"name":"Build & Push Images to GHCR (unmanaged)","status":"completed","conclusion":"success"}]}`,
			wantImageCalls: 1,
			wantErr:        false,
		},
		{
			name:           "imageReadyErr dispatch failure returns error",
			headBranch:     "main",
			conclusion:     "success",
			jobsStatus:     http.StatusOK,
			jobsBody:       `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			imageReadyErr:  errors.New("hangar image failed"),
			wantImageCalls: 1,
			wantErr:        true,
		},
		{
			name:           "deployingErr registry error returns error",
			headBranch:     "main",
			conclusion:     "success",
			jobsStatus:     http.StatusOK,
			jobsBody:       `{"jobs":[{"name":"Build & Push Images to GHCR (brain)","status":"completed","conclusion":"success"}]}`,
			deployingErr:   errors.New("db fail"),
			wantImageCalls: 1,
			wantErr:        true,
		},
		{
			name:       "non-main branch is no-op",
			headBranch: "feat/my-feature",
			conclusion: "success",
		},
		{
			name:       "non-success conclusion is no-op",
			headBranch: "main",
			conclusion: "failure",
		},
		{
			name:      "git push nil dispatcher is no-op",
			isGitPush: true,
			noDisp:    true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
		{
			name:      "git push non-main ref is no-op",
			isGitPush: true,
			ref:       "refs/heads/feat",
			commit:    "sha123",
		},
		{
			name:      "git push zero sha is no-op",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "0000000000000000000000000000000000000000",
		},
		{
			name:       "git push dispatcher error returns error",
			isGitPush:  true,
			ref:        "refs/heads/main",
			commit:     "sha123",
			gitPushErr: errors.New("hangar down"),
			wantErr:    true,
		},
		{
			name:      "git push successful dispatch",
			isGitPush: true,
			ref:       "refs/heads/main",
			commit:    "sha123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status := tc.jobsStatus
				if status == 0 {
					status = http.StatusOK
				}
				w.WriteHeader(status)
				body := tc.jobsBody
				if body == "" {
					body = `{"jobs":[]}`
				}
				_, _ = w.Write([]byte(body))
			}))
			defer ts.Close()

			var disp *mockOutboundDispatcher
			if !tc.noDisp {
				disp = &mockOutboundDispatcher{
					gitPushErr:    tc.gitPushErr,
					imageReadyErr: tc.imageReadyErr,
					widgetErr:     tc.widgetErr,
					matchedJobs: map[string][]string{
						"ghcr.io/azylman/brain:latest": {"brain"},
					},
				}
			}
			reg := &mockPRRegistry{
				deployingErr:       tc.deployingErr,
				hasExpectedErr:     tc.hasExpectedErr,
				suppressVanadisRes: tc.suppressVanadis,
				resolveTargetID:    "1555405874565091380",
				resolveBranch:      "feat/test",
			}
			srv := NewRouterServer(Config{GitHubAPIURL: ts.URL}, ts.Client())
			if disp != nil {
				srv.SetDispatcher(disp)
			}
			srv.SetRegistry(reg)

			var err error
			if tc.isGitPush {
				err = srv.dispatchGitPush(ctx, "azylman/aerial", tc.ref, tc.commit, "1555405874565091380", 1)
			} else {
				err = srv.dispatchWorkflowRunImages(ctx, "azylman/aerial", 12345, tc.headBranch, tc.conclusion, "sha123", "1555405874565091380", 42)
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if disp != nil && len(disp.ImageReadyCalls()) != tc.wantImageCalls {
				t.Errorf("imageReadyCalls = %d, want %d", len(disp.ImageReadyCalls()), tc.wantImageCalls)
			}
			if len(reg.deployingCalls) != tc.wantDeployCalls {
				t.Errorf("deployingCalls = %d, want %d", len(reg.deployingCalls), tc.wantDeployCalls)
			}
			if disp != nil && tc.wantWidgetCalls > 0 && len(disp.WidgetCalls()) != tc.wantWidgetCalls {
				t.Errorf("widgetCalls = %d, want %d", len(disp.WidgetCalls()), tc.wantWidgetCalls)
			}
		})
	}
}

// --- 7. PR Conflict Checks & SHA/Thread Resolution ---

func makeTestOpenPR(number int, branch, sha, author string) GitHubPullRequest {
	return GitHubPullRequest{Number: number, Head: GitHubPRRef{Ref: branch, SHA: sha}, User: struct {
		Login string `json:"login"`
	}{Login: author}}
}

func TestCheckOpenPRConflicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name                 string
		autoUpdateAuthors    []string
		listPRsStatus        int
		useNilRegistry       bool
		useRiver             bool
		riverErr             error
		conflictErr          error
		openPRs              []GitHubPullRequest
		prResponses          []string
		prStatusCodes        []int
		closedErr            error
		wantClosedPRs        []int
		wantErr              bool
		wantErrSubstr        string
		wantConflict         bool
		wantConflictPR       int
		wantConflictQueued   int
		wantPrompts          int
		wantDispatches       int
		wantPromptSubs       []string
		wantDMs              int
		wantUpdatedPRs       []string
		checkRunsBySHA       map[string]string
		presetFailedSHAs     map[int]string
		presetUpdatedPRs     []int
		wantCachedFailedSHAs map[int]string
	}{
		{
			name:              "empty autoUpdateAuthors is clean no-op",
			autoUpdateAuthors: nil,
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(550, "feat/behind-1", "head550", "trusted-author"),
			},
			wantErr: false,
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
			name:              "422 merge conflict transitions and dispatches via background /dispatch without DM",
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs:           []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:     []int{http.StatusUnprocessableEntity},
			prResponses:       []string{`{"message":"merge conflict between base and head"}`},
			wantConflict:      true,
			wantConflictPR:    543,
			wantDMs:           0,
			wantDispatches:    1,
			wantPromptSubs:    []string{"Merge conflict detected on PR #543"},
			wantUpdatedPRs:    []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:               "409 conflict with river client enqueues MergeConflictArgs on merge_conflict queue",
			useRiver:           true,
			autoUpdateAuthors:  []string{"trusted-author"},
			openPRs:            []GitHubPullRequest{makeTestOpenPR(543, "feat/my-feature", "head543", "trusted-author")},
			prStatusCodes:      []int{http.StatusConflict},
			prResponses:        []string{`{"message":"Merge conflict"}`},
			wantConflict:       true,
			wantConflictPR:     543,
			wantConflictQueued: 1,
			wantDMs:            0,
			wantDispatches:     0,
			wantUpdatedPRs:     []string{"/repos/azylman/aerial/pulls/543/update-branch"},
		},
		{
			name:              "river insert error on conflict logs warning without failing check",
			useRiver:          true,
			riverErr:          errors.New("river insert failed"),
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
			autoUpdateAuthors: []string{"trusted-author"},
			openPRs: []GitHubPullRequest{
				makeTestOpenPR(545, "feat/conflicted-1", "head545", "trusted-author"),
				makeTestOpenPR(546, "feat/clean-2", "head546", "trusted-author"),
			},
			prResponses: []string{
				`{"message":"merge conflict between base and head"}`,
				`{"message":"Updating pull request branch."}`,
			},
			prStatusCodes:  []int{http.StatusUnprocessableEntity, http.StatusAccepted},
			wantConflict:   true,
			wantConflictPR: 545,
			wantDMs:        0,
			wantDispatches: 1,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reqCount atomic.Int32
			var mu sync.Mutex
			var putPaths []string
			apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				if r.Method == http.MethodPut {
					mu.Lock()
					putPaths = append(putPaths, r.URL.Path)
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
			defer apiServer.Close()

			var mockReg PRRegistryUpdater
			if !tc.useNilRegistry {
				mockReg = &mockPRRegistry{
					conflictErr:      tc.conflictErr,
					conflictTargetID: "1555405874565091380",
					conflictUpdated:  true,
					closedErr:        tc.closedErr,
				}
			}
			mockDisp := &mockOutboundDispatcher{}
			var inserters []RiverInserter
			var mockRiver *mockRiverInserter
			if tc.useRiver {
				mockRiver = &mockRiverInserter{err: tc.riverErr}
				inserters = append(inserters, mockRiver)
			}
			srv := newTestRouter(Config{GitHubAPIURL: apiServer.URL, GitHubToken: "test-token", AutoUpdatePRAuthors: tc.autoUpdateAuthors}, nil, mockReg, mockDisp, inserters...)
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
			for prNum, wantSHA := range tc.wantCachedFailedSHAs {
				v, ok := srv.failedCheckSHAs.Load(prCheckKey("azylman/aerial", prNum))
				if !ok || fmt.Sprint(v) != wantSHA {
					t.Errorf("cached failedCheckSHAs[%d] = %v (ok=%v), want %q", prNum, v, ok, wantSHA)
				}
			}
			mu.Lock()
			gotUpdated := append([]string(nil), putPaths...)
			mu.Unlock()
			if len(gotUpdated) != len(tc.wantUpdatedPRs) {
				t.Fatalf("got updated PR paths %v, want %v", gotUpdated, tc.wantUpdatedPRs)
			}
			for i := range tc.wantUpdatedPRs {
				if gotUpdated[i] != tc.wantUpdatedPRs[i] {
					t.Errorf("updated PR path[%d] = %q, want %q", i, gotUpdated[i], tc.wantUpdatedPRs[i])
				}
			}
			if mReg, ok := mockReg.(*mockPRRegistry); ok && mReg != nil {
				if tc.wantConflict {
					if len(mReg.conflictCalls) != 1 || mReg.conflictCalls[0].prNumber != tc.wantConflictPR {
						t.Errorf("conflictCalls = %+v, want PR %d", mReg.conflictCalls, tc.wantConflictPR)
					}
				} else if len(mReg.conflictCalls) != 0 {
					t.Errorf("unexpected conflictCalls: %+v", mReg.conflictCalls)
				}
				if len(mReg.closedUnmergedCalls) != len(tc.wantClosedPRs) {
					t.Errorf("closedUnmergedCalls count = %d, want %d", len(mReg.closedUnmergedCalls), len(tc.wantClosedPRs))
				} else {
					for i, wantPR := range tc.wantClosedPRs {
						if mReg.closedUnmergedCalls[i].prNumber != wantPR {
							t.Errorf("closedUnmergedCalls[%d] = %d, want %d", i, mReg.closedUnmergedCalls[i].prNumber, wantPR)
						}
					}
				}
			}
			if mockRiver != nil && len(mockRiver.insertedConflictJobs) != tc.wantConflictQueued {
				t.Errorf("insertedConflictJobs = %d, want %d", len(mockRiver.insertedConflictJobs), tc.wantConflictQueued)
			}
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
		})
	}
}

func TestResolvePRFromCommitSHA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		repo          string
		sha           string
		apiURL        string
		statusCode    int
		responseBody  string
		wantErr       bool
		wantErrSubstr string
		wantPRNum     int
		wantBranch    string
	}{
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				code := tc.statusCode
				if code == 0 {
					code = http.StatusOK
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(tc.responseBody))
			}))
			defer ts.Close()

			apiURL := ts.URL
			if tc.apiURL != "" {
				apiURL = tc.apiURL
			}
			srv := NewRouterServer(Config{GitHubAPIURL: apiURL, GitHubToken: "secret"}, ts.Client())
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
		})
	}
}

func TestParseAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantTarget, wantChunk string
		wantOK                            bool
	}{
		{name: "no trailer", body: "Standard PR body\n<!-- regular comment -->"},
		{name: "unclosed trailer", body: "Body\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\"}"},
		{name: "invalid json", body: "Body\n<!-- aerial-pr: {bad-json} -->"},
		{name: "invalid target and empty chunk", body: "Body\n<!-- aerial-pr: {\"target_id\":\"bad\",\"vanadis_chunk_id\":\"\"} -->"},
		{name: "valid target and chunk", body: "<!-- x -->\n<!-- aerial-pr: {\"target_id\":\"1562162630997581946\",\"vanadis_chunk_id\":\"chunk-2\"} -->", wantTarget: "1562162630997581946", wantChunk: "chunk-2", wantOK: true},
		{name: "discord prefixed target and pool prefix", body: "<!-- aerial-pr: {\"target_id\":\"stateful_medium:discord-1562162630997581946\",\"vanadis_chunk_id\":\"\"} -->", wantTarget: "1562162630997581946", wantChunk: "", wantOK: true},
		{name: "valid chunk only", body: "<!-- aerial-pr: {\"target_id\":\"\",\"vanadis_chunk_id\":\"chunk-only\"} -->", wantChunk: "chunk-only", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotTarget, gotChunk, gotOK := ParseAerialPRMetadata(tc.body)
			if gotOK != tc.wantOK || gotTarget != tc.wantTarget || gotChunk != tc.wantChunk {
				t.Fatalf("ParseAerialPRMetadata() = (%q, %q, %v), want (%q, %q, %v)", gotTarget, gotChunk, gotOK, tc.wantTarget, tc.wantChunk, tc.wantOK)
			}
		})
	}
}

func TestClassifyCheckState_Unit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{name: "completed success", status: "completed", conclusion: "success", want: "passed"},
		{name: "completed neutral", status: "completed", conclusion: "neutral", want: "passed"},
		{name: "completed skipped", status: "completed", conclusion: "skipped", want: "passed"},
		{name: "completed failure", status: "completed", conclusion: "failure", want: "failed"},
		{name: "completed timed_out", status: "completed", conclusion: "timed_out", want: "failed"},
		{name: "completed cancelled", status: "completed", conclusion: "cancelled", want: "failed"},
		{name: "completed action_required", status: "completed", conclusion: "action_required", want: "failed"},
		{name: "completed unknown conclusion", status: "completed", conclusion: "stale", want: ""},
		{name: "queued", status: "queued", want: "pending"},
		{name: "in_progress", status: "in_progress", want: "pending"},
		{name: "waiting", status: "waiting", want: "pending"},
		{name: "requested", status: "requested", want: "pending"},
		{name: "pending", status: "pending", want: "pending"},
		{name: "other non-completed", status: "custom_status", want: "pending"},
		{name: "empty status", status: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyCheckState(tc.status, tc.conclusion)
			if got != tc.want {
				t.Fatalf("classifyCheckState(%q, %q) = %q, want %q", tc.status, tc.conclusion, got, tc.want)
			}
		})
	}
}

func TestFormatCIChecksActiveStep_Unit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		prStatus     string
		ciChecksJSON string
		metadataJSON string
		wantLegacy   string
		want         string
	}{
		{name: "non-open status merged legacy empty", prStatus: "merged", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: "Building container images (1/1 completed)"},
		{name: "non-open status closed", prStatus: "closed", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: ""},
		{name: "non-open status ci_failed", prStatus: "ci_failed", ciChecksJSON: `{"lint":"passed"}`, wantLegacy: "", want: ""},
		{name: "empty ciChecksJSON", prStatus: "open", ciChecksJSON: "", wantLegacy: "", want: ""},
		{name: "empty JSON object", prStatus: "open", ciChecksJSON: "{}", wantLegacy: "", want: ""},
		{name: "invalid JSON", prStatus: "open", ciChecksJSON: "not-json", wantLegacy: "", want: ""},
		{name: "single check passed", prStatus: "open", ciChecksJSON: `{"test":"passed"}`, wantLegacy: "Waiting on CI (1/1 passed)", want: "Waiting on CI (1/1 passed)"},
		{name: "multiple checks all passed", prStatus: "open", ciChecksJSON: `{"lint":"passed","unit":"passed","integration":"passed"}`, wantLegacy: "Waiting on CI (3/3 passed)", want: "Waiting on CI (3/3 passed)"},
		{name: "mixed passed and pending", prStatus: "open", ciChecksJSON: `{"lint":"passed","build":"pending","unit":"passed"}`, wantLegacy: "Waiting on CI (2/3 passed)", want: "Waiting on CI (2/3 passed)"},
		{name: "all pending", prStatus: "open", ciChecksJSON: `{"lint":"pending","unit":"pending"}`, wantLegacy: "Waiting on CI (0/2 passed)", want: "Waiting on CI (0/2 passed)"},
		{name: "any check failed returns empty", prStatus: "open", ciChecksJSON: `{"lint":"passed","unit":"failed"}`, wantLegacy: "", want: ""},

		// Post-merge tests for formatPRWidgetActiveStep
		{
			name:         "merged with candidate jobs and started verifying_health",
			prStatus:     "merged",
			metadataJSON: `{"jobs":["eri","haro"],"started_jobs":["eri"],"deployed_jobs":[],"job_states":{"eri":"verifying_health"}}`,
			want:         "`eri`: 🔄 verifying health\n`haro`: ⏳ building image",
		},
		{
			name:         "synced with deployed job and starting job",
			prStatus:     "synced",
			metadataJSON: `{"jobs":["coven","eri"],"started_jobs":["eri"],"deployed_jobs":["coven"]}`,
			want:         "`coven`: ✅ healthy\n`eri`: 🔄 starting",
		},
		{
			name:         "deploying status with empty started_jobs defaults candidate jobs to starting",
			prStatus:     "deploying",
			metadataJSON: `{"jobs":["eri","haro"]}`,
			want:         "`eri`: 🔄 starting\n`haro`: 🔄 starting",
		},
		{
			name:         "merged with buildJobRegex service matches in ci_checks",
			prStatus:     "merged",
			ciChecksJSON: `{"Build & Push Images (mesh-proxy)":"passed","Build & Push Images (eri)":"in_progress"}`,
			want:         "`eri`: ⏳ building image\n`mesh-proxy`: 🔄 starting",
		},
		{
			name:         "merged with non-matching ci_checks falls back to container images count",
			prStatus:     "merged",
			ciChecksJSON: `{"Docker Build":"passed","Test Gate":"passed"}`,
			want:         "Building container images (2/2 completed)",
		},
		{
			name:         "merged with failed check returns empty",
			prStatus:     "merged",
			ciChecksJSON: `{"Docker Build":"failed","Test Gate":"passed"}`,
			metadataJSON: `{"jobs":["eri"]}`,
			want:         "",
		},
		{
			name:         "merged fallback to meta ci_checks when ciChecksJSON is empty",
			prStatus:     "merged",
			metadataJSON: `{"ci_checks":{"Docker Build":"passed"}}`,
			want:         "Building container images (1/1 completed)",
		},
		{
			name:     "unsupported status returns empty",
			prStatus: "deployed",
			want:     "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotLegacy := formatCIChecksActiveStep(tc.prStatus, tc.ciChecksJSON)
			if tc.metadataJSON == "" && tc.prStatus == "open" {
				if gotLegacy != tc.wantLegacy {
					t.Fatalf("formatCIChecksActiveStep(%q, %q) = %q, want %q", tc.prStatus, tc.ciChecksJSON, gotLegacy, tc.wantLegacy)
				}
			}
			got := formatPRWidgetActiveStep(tc.prStatus, tc.ciChecksJSON, tc.metadataJSON)
			if got != tc.want {
				t.Fatalf("formatPRWidgetActiveStep(%q, %q, %q) = %q, want %q", tc.prStatus, tc.ciChecksJSON, tc.metadataJSON, got, tc.want)
			}
		})
	}
}

func TestExtractStringMapFromMetadata_Unit(t *testing.T) {
	t.Parallel()

	if res := extractStringMapFromMetadata(nil, "key"); res != nil {
		t.Errorf("expected nil for nil meta, got %v", res)
	}
	if res := extractStringMapFromMetadata(map[string]any{}, "key"); res != nil {
		t.Errorf("expected nil for missing key, got %v", res)
	}

	paddedKey := " ERI "
	metaObj := map[string]any{
		"job_states": map[string]any{
			paddedKey: "verifying_health",
			"":        "invalid",
			"HARO":    "starting",
		},
	}
	gotObj := extractStringMapFromMetadata(metaObj, "job_states")
	if gotObj["eri"] != "verifying_health" || gotObj["haro"] != "starting" {
		t.Errorf("unexpected map from interface: %+v", gotObj)
	}

	metaStr := map[string]any{
		"job_states": map[string]string{
			" CADVISOR ": " healthy ",
		},
	}
	gotStr := extractStringMapFromMetadata(metaStr, "job_states")
	if gotStr["cadvisor"] != "healthy" {
		t.Errorf("unexpected map from string map: %+v", gotStr)
	}
}
