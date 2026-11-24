package prflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/githubapitest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func newSpyChunk(chunk *model.VanadisChunk, failCount int) []model.VanadisChunk {
	if chunk == nil {
		return nil
	}
	c := *chunk
	c.ID, c.BuildID, c.CIFailCount = "c1", "b1", failCount
	if c.Effort == "" {
		c.Effort = effortMedium
	}
	return []model.VanadisChunk{c}
}

type prCheckResetCase struct {
	name        string
	event       string
	ev          pubsub.PRLifecycleEvent
	execNil     bool
	storeNil    bool
	apiStatus   int
	apiBody     string
	chunk       *model.VanadisChunk
	checkErr    error
	getBuildErr error
	wantErr     string
	wantBanners []string
	wantResets  []string
	wantChecks  []string
}

func prCheckResetCases() []prCheckResetCase {
	headAPI := `{"state":"open","merged":false,"head":{"sha":"sha-2"}}`
	return []prCheckResetCase{
		{name: "opened resets check state", event: "opened", ev: pubsub.PRLifecycleEvent{Event: "opened"}, execNil: true, wantResets: []string{"c1::"}},
		{name: "opened with executor and chunk resets check state for head sha", event: "opened", ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2"}, chunk: &model.VanadisChunk{Repo: "owner/repo"}, apiStatus: http.StatusOK, apiBody: headAPI, wantResets: []string{"c1:sha-2:"}},
		{name: "opened with nil store and executor is a no-op", event: "opened", ev: pubsub.PRLifecycleEvent{Event: "opened"}, execNil: true, storeNil: true},
		{name: "synchronize validates head against GitHub and resets check state", event: "synchronize", ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2"}, apiStatus: http.StatusOK, apiBody: headAPI, wantResets: []string{"c1:sha-2:"}},
		{name: "synchronize superseded head rejected without reset", event: "synchronize", ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1"}, apiStatus: http.StatusOK, apiBody: headAPI, wantErr: lifecycleSkipped},
		{name: "check_updated records check state", event: "check_updated", ev: pubsub.PRLifecycleEvent{Event: "check_updated", HeadSHA: "sha-9", CheckName: "lint", CheckState: "passed"}, execNil: true, wantChecks: []string{"c1:sha-9:lint:passed"}},
		{name: "unknown event is ignored", event: "labeled", ev: pubsub.PRLifecycleEvent{Event: "labeled"}, execNil: true, wantChecks: []string{}},
		{name: "check_failed skipped write with nil executor", event: "check_failed", ev: pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"}, execNil: true, checkErr: ErrPRTransitionSkipped, getBuildErr: errLifecycleRead, wantErr: lifecycleSkipped},
		{name: "check_failed failed write with nil executor", event: "check_failed", ev: pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"}, execNil: true, checkErr: errLifecycleDB, getBuildErr: errLifecycleRead, wantErr: "db error"},
		{name: "ci_failed with nil executor records banner only", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), execNil: true, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed with nil store and executor is a no-op", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), execNil: true, storeNil: true},
		{name: "ci_failed gate aggregator check is ignored", event: "ci_failed", ev: ciFailEv("CI Gate", "sha-1"), wantChecks: []string{}},
		{name: "ci_failed check with suffix gate is ignored", event: "ci_failed", ev: ciFailEv("Build Gate", "sha-1"), wantChecks: []string{}},
	}
}

func TestFlow_HandlePREventType_CheckAndHeadReset(t *testing.T) {
	t.Parallel()
	for _, tc := range prCheckResetCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, err: tc.checkErr}
			spy.chunks = newSpyChunk(tc.chunk, 0)
			eri := &eritest.Client{SimulateActive: true}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, tc.apiStatus, tc.apiBody)
			}
			f := NewFlow(st, exec)
			err := f.HandlePREventType(context.Background(), "b1", "c1", tc.event, tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantBanners, spy.banners)
			assert.Equal(t, tc.wantResets, spy.resets)
			if tc.wantChecks != nil {
				assert.ElementsMatch(t, tc.wantChecks, spy.calls)
			}
			assert.Nil(t, eri.LastDispatch)
		})
	}
}

type prCIFailRemediationCase struct {
	name         string
	ev           pubsub.PRLifecycleEvent
	extraEvents  []pubsub.PRLifecycleEvent
	chunk        *model.VanadisChunk
	failCount    int
	apiStatus    int
	apiRoutes    map[string][]githubapitest.Response
	dispatchErr  error
	bannerErr    error
	checkErr     error
	getBuildErr  error
	failCountErr error
	want         prRemediationWant
}

type prRemediationWant struct {
	err            string
	checks         []string
	banners        []string
	widget         string
	dispatchID     string
	skipIfActive   *bool
	dispatchPrompt string
	remediatedSHA  string
	failCountCalls int
	dispatch       bool
}

func prCIFailRemediationBasicCases() []prCIFailRemediationCase {
	yes, no := true, false
	sameHead := []pubsub.PRLifecycleEvent{
		ciFailEv("check-2", "abcdef1234"), ciFailEv("check-3", "abcdef1234"),
		ciFailEv("check-4", "abcdef1234"), ciFailEv("check-5", "abcdef1234"), ciFailEv("check-6", "abcdef1234"),
	}
	sameHeadBanners := []string{
		`c1:ci_failed:CI check "check-1" failed`, `c1:ci_failed:CI check "check-2" failed`,
		`c1:ci_failed:CI check "check-3" failed`, `c1:ci_failed:CI check "check-4" failed`,
		`c1:ci_failed:CI check "check-5" failed`, `c1:ci_failed:CI check "check-6" failed`,
	}

	return []prCIFailRemediationCase{
		{
			name: "ci_failed within cap dispatches background remediation, increments count and syncs widget", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 1,
			want: prRemediationWant{checks: []string{"c1:abcdef1234:unit-tests:failed"}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-2", skipIfActive: &no, widget: `CI check "unit-tests" failed — remediation in progress`},
		},
		{
			name: "six failing checks on same head coalesce into one accepted remediation attempt", ev: ciFailEv("check-1", "abcdef1234"), extraEvents: sameHead,
			want: prRemediationWant{banners: sameHeadBanners, failCountCalls: 1, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-1", skipIfActive: &no},
		},
		{
			name: "ci_failed skipped_active dispatch returns nil without consuming remediation attempt budget", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{RemediatedSHA: "abcdef1234"}, dispatchErr: eriwire.ErrDispatchActive,
			want: prRemediationWant{banners: []string{lifecycleCIBanner}, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-1", skipIfActive: &yes},
		},
		{
			name: "ci_failed dispatch transport error propagates without consuming remediation attempt budget", ev: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", banners: []string{lifecycleCIBanner}, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-1", skipIfActive: &no},
		},
		{
			name: "ci_failed details API error propagates before dispatch", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, apiStatus: http.StatusInternalServerError,
			want: prRemediationWant{err: "500", banners: []string{`c1:ci_failed:CI check "roborev" failed`}},
		},
		{
			name: "ci_failed banner update error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), bannerErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", banners: []string{lifecycleCIBanner}},
		},
		{
			name: "ci_failed get build error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), getBuildErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", banners: []string{lifecycleCIBanner}},
		},
		{
			name: "ci_failed increment count error after accepted dispatch propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCountErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", banners: []string{lifecycleCIBanner}, failCountCalls: 1, dispatch: true, skipIfActive: &no},
		},
		{
			name: "ci_failed on new head_sha dispatches with SkipIfActive false and records RemediatedSHA", ev: ciFailEv("unit-tests", "new-head-sha"), chunk: &model.VanadisChunk{RemediatedSHA: "old-head-sha"},
			want: prRemediationWant{banners: []string{lifecycleCIBanner}, failCountCalls: 1, dispatch: true, dispatchID: "vanadis-c1-cifail-new-head-sha-1", skipIfActive: &no, remediatedSHA: "new-head-sha"},
		},
	}
}

func prCIFailRemediationCoalescedCases() []prCIFailRemediationCase {
	no := false
	coalescedJobs := map[string][]githubapitest.Response{
		"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
			{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"completed","conclusion":"failure"}]}`},
		},
		"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
		"GET /repos/example-owner/aerial/actions/jobs/11/logs": {{Status: http.StatusOK, Body: "test panic"}},
	}
	cdRoutes := map[string][]githubapitest.Response{
		"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
			{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"}]}`},
		},
		"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
	}

	return []prCIFailRemediationCase{
		{
			name: "sibling pending job holds remediation",
			ev:   pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "lint", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123/job/10", Details: "lint failed"},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"in_progress","conclusion":null}]}`},
				},
			},
			want: prRemediationWant{checks: []string{"c1:abcdef1234:lint:failed"}, banners: []string{`c1:ci_failed:CI check "lint" failed`}, widget: `CI check "lint" failed — waiting for remaining CI jobs to finish`},
		},
		{
			name:      "completed run with multiple failed jobs coalesces into single wake",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "lint", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123/job/10", Details: "lint failure"},
			apiRoutes: coalescedJobs, failCount: 1,
			want: prRemediationWant{checks: []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"}, banners: []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`}, failCountCalls: 1, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-2", skipIfActive: &no, dispatchPrompt: "lint, test", widget: `CI check "lint, test" failed — remediation in progress`},
		},
		{
			name: "workflow run failure does not record workflow name in ci checks",
			ev:   pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "Continuous Delivery", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123", Details: "workflow concluded with failure"},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":12,"name":"CI Gate","status":"completed","conclusion":"failure"}]}`},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
			},
			failCount: 1,
			want:      prRemediationWant{checks: []string{"c1:abcdef1234:lint:failed"}, banners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`, `c1:ci_failed:CI check "lint" failed`}, failCountCalls: 1, dispatch: true, dispatchID: "vanadis-c1-cifail-abcdef1234-2", skipIfActive: &no, dispatchPrompt: "lint", widget: `CI check "lint" failed — remediation in progress`},
		},
		{
			name:      "coalesced check record error propagates",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "Continuous Delivery", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123", Details: "workflow concluded with failure"},
			apiRoutes: cdRoutes, checkErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", banners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`}},
		},
		{
			name:      "coalesced banner update error propagates",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "Continuous Delivery", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123", Details: "workflow concluded with failure"},
			apiRoutes: cdRoutes, bannerErr: errLifecycleDB,
			want: prRemediationWant{err: "db error", checks: []string{}, banners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`}},
		},
	}
}

func prCIFailRemediationCases() []prCIFailRemediationCase {
	return append(prCIFailRemediationBasicCases(), prCIFailRemediationCoalescedCases()...)
}

func assertRemediationStore(t *testing.T, spy *checkStatusSpyStore, want prRemediationWant) {
	t.Helper()
	assert.Equal(t, want.banners, spy.banners)
	if want.checks != nil {
		assert.ElementsMatch(t, want.checks, spy.calls)
	}
	assert.Len(t, spy.failCountCalls, want.failCountCalls)
	if want.remediatedSHA != "" {
		assert.Equal(t, want.remediatedSHA, spy.chunk("c1").RemediatedSHA)
	}
}

func assertRemediationEri(t *testing.T, eri *eritest.Client, want prRemediationWant) {
	t.Helper()
	if want.widget != "" && assert.NotNil(t, eri.LastWidget) {
		assert.Equal(t, want.widget, eri.LastWidget.Banner)
	}
	if !want.dispatch && want.dispatchID == "" {
		assert.Nil(t, eri.LastDispatch)
		return
	}
	require.NotNil(t, eri.LastDispatch)
	assert.Equal(t, "vanadis-c1", eri.LastDispatch.SessionID)
	assert.Equal(t, "c1", eri.LastDispatch.VanadisChunkID)
	assert.Equal(t, "th-1", eri.LastDispatch.TargetID)
	assert.NotEmpty(t, eri.LastDispatch.ID)
	if want.dispatchID != "" {
		assert.Equal(t, want.dispatchID, eri.LastDispatch.ID)
	}
	if want.skipIfActive != nil {
		assert.Equal(t, *want.skipIfActive, eri.LastDispatch.SkipIfActive)
	}
	assert.Contains(t, eri.LastDispatch.Prompt, want.dispatchPrompt)
}

func TestFlow_HandlePREventType_CIFailedRemediation(t *testing.T) {
	t.Parallel()
	for _, tc := range prCIFailRemediationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:  tc.getBuildErr,
				bannerErr:    tc.bannerErr,
				err:          tc.checkErr,
				failCount:    tc.failCount,
				failCountErr: tc.failCountErr,
			}
			spy.chunks = newSpyChunk(tc.chunk, tc.failCount)
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true}
			exec := newLifecycleExecutor(t, eri, nil, tc.apiStatus, "", tc.apiRoutes)
			f := NewFlow(spy, exec)
			ctx := context.Background()

			err := f.HandlePREventType(ctx, "b1", "c1", "ci_failed", tc.ev)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(ctx, "b1", "c1", extra.Event, extra)
			}
			assertLifecycleErr(t, tc.want.err, err)
			assertRemediationStore(t, spy, tc.want)
			assertRemediationEri(t, eri, tc.want)
		})
	}
}

type prCIFailCapCase struct {
	name          string
	ev            pubsub.PRLifecycleEvent
	extraEvents   []pubsub.PRLifecycleEvent
	chunk         *model.VanadisChunk
	failCount     int
	skipFailApply bool
	failBanner    string
	agent         string
	apiStatus     int
	apiRoutes     map[string][]githubapitest.Response
	errs          prCapErrors
	want          prCapWant
}

type prCapErrors struct {
	failCount error
	getBuild  error
	banner    error
	check     error
	fail      error
	notify    error
	cancel    error
	widget    error
}

type prCapWant struct {
	err            string
	failed         []string
	banners        []string
	checks         []string
	failCountCalls int
	ciFailCount    int
	dispatch       bool
	remediatedSHA  string
	cancel         string
	threadID       string
	prompt         string
	widget         string
}

func prCIFailCapAttemptCases() []prCIFailCapCase {
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"
	trailing := []pubsub.PRLifecycleEvent{ciFailEv("unit-tests", "abcdef1234")}
	trailingThenNewHead := append(append([]pubsub.PRLifecycleEvent{}, trailing...), ciFailEv("unit-tests", "feeedd9876"))
	newHeadRetry := append(append([]pubsub.PRLifecycleEvent{}, trailingThenNewHead...), ciFailEv("unit-tests", "feeedd9876"))

	return []prCIFailCapCase{
		{
			name: "ci_failed increment count error at cap propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, errs: prCapErrors{failCount: errLifecycleDB},
			want: prCapWant{err: "db error", banners: []string{lifecycleCIBanner}, failCountCalls: 1},
		},
		{
			name: "ci_failed exceeding cap marks chunk failed and sends alert message", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5,
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed tripping cap increments fail count without overwriting RemediatedSHA", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 5, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"},
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, remediatedSHA: "attempt5-active-sha", cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed retry with failCount 6 skips increment and completes cap reached", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 6, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"},
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, remediatedSHA: "attempt5-active-sha", cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "trailing same-SHA failure at cap neither increments, fails nor cancels the active remediation", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailing, failCount: 4,
			want: prCapWant{banners: []string{lifecycleCIBanner, lifecycleCIBanner}, failCountCalls: 1, ciFailCount: 5, dispatch: true},
		},
		{
			name: "new head after trailing same-SHA failure trips cap and keeps RemediatedSHA", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailingThenNewHead, failCount: 4,
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, failCountCalls: 2, ciFailCount: 6, dispatch: true, remediatedSHA: "abcdef1234", cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "retry of the cap-tripping head does not increment again", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: newHeadRetry, failCount: 4,
			want: prCapWant{failed: []string{lifecycleCapFail, lifecycleCapFail}, banners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, failCountCalls: 2, ciFailCount: 6, dispatch: true, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed record check skipped on already failed chunk resumes cap reached notifications", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}, skipFailApply: true, failBanner: lifecycleCapText, errs: prCapErrors{check: ErrPRTransitionSkipped},
			want: prCapWant{failed: []string{lifecycleCapFail}, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed update banner skipped on already failed chunk resumes cap reached notifications", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}, skipFailApply: true, failBanner: lifecycleCapText, errs: prCapErrors{banner: ErrPRTransitionSkipped},
			want: prCapWant{banners: []string{lifecycleCIBanner}, failed: []string{lifecycleCapFail}, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed record check skipped with stale head does not re-notify", ev: ciFailEv("unit-tests", "stale-sha"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "current-sha"}, errs: prCapErrors{check: ErrPRTransitionSkipped},
			want: prCapWant{err: lifecycleSkipped},
		},
	}
}

func prCIFailCapNotificationCases() []prCIFailCapCase {
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"

	return []prCIFailCapCase{
		{
			name: "ci_failed record check skipped with lookup error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), errs: prCapErrors{check: ErrPRTransitionSkipped, getBuild: errLifecycleDB},
			want: prCapWant{err: "db error"},
		},
		{
			name: "ci_failed exceeding cap preserves chunk failure and propagates notification error", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, errs: prCapErrors{notify: errLifecycleDB},
			want: prCapWant{err: "db error", failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt},
		},
		{
			name: "ci_failed exceeding cap retry with matching banner completes notifications", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, failBanner: lifecycleCapText,
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed exceeding cap fail chunk error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, errs: prCapErrors{fail: errLifecycleDB},
			want: prCapWant{err: "db error", failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1},
		},
		{
			name: "ci_failed at cap with unapplied fail chunk skips cap without cancelling", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true,
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1},
		},
		{
			name: "ci_failed exceeding cap dispatches with build agent", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, agent: "reviewer-bot",
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed exceeding cap failure details error propagates", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, failCount: 5, apiStatus: http.StatusInternalServerError,
			want: prCapWant{err: "500", failed: []string{lifecycleCapFail}, banners: []string{`c1:ci_failed:CI check "roborev" failed`}, failCountCalls: 1},
		},
		{
			name: "ci_failed exceeding cap cancel warning logged and continues", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, errs: prCapErrors{cancel: errors.New("cancel down")},
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed exceeding cap widget warning logged and continues", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, errs: prCapErrors{widget: errors.New("widget down")},
			want: prCapWant{failed: []string{lifecycleCapFail}, banners: []string{lifecycleCIBanner}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
		{
			name: "ci_failed exceeding cap with run jobs marks chunk failed and alerts",
			ev:   pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "lint", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123/job/10", Details: "lint failure"},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"completed","conclusion":"failure"}]}`},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
				"GET /repos/example-owner/aerial/actions/jobs/11/logs": {{Status: http.StatusOK, Body: "test panic"}},
			},
			failCount: 5,
			want:      prCapWant{checks: []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"}, banners: []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`}, failed: []string{lifecycleCapFail}, failCountCalls: 1, cancel: "c1", threadID: "th-1", prompt: capPrompt, widget: capProgress},
		},
	}
}

func prCIFailCapCases() []prCIFailCapCase {
	return append(prCIFailCapAttemptCases(), prCIFailCapNotificationCases()...)
}

func assertCapStore(t *testing.T, spy *checkStatusSpyStore, want prCapWant) {
	t.Helper()
	assert.Equal(t, want.failed, spy.failed)
	assert.Equal(t, want.banners, spy.banners)
	if want.checks != nil {
		assert.ElementsMatch(t, want.checks, spy.calls)
	}
	assert.Len(t, spy.failCountCalls, want.failCountCalls)
	if want.ciFailCount != 0 {
		assert.Equal(t, want.ciFailCount, spy.chunk("c1").CIFailCount)
	}
	if want.remediatedSHA != "" {
		assert.Equal(t, want.remediatedSHA, spy.chunk("c1").RemediatedSHA)
	}
}

func assertCapEri(t *testing.T, eri *eritest.Client, agent string, want prCapWant) {
	t.Helper()
	if want.prompt != "" {
		assert.Contains(t, eri.LastPrompt, want.prompt)
	} else {
		assert.Empty(t, eri.LastPrompt)
	}
	assert.Equal(t, want.threadID, eri.LastThreadID)
	assert.Equal(t, want.cancel, eri.LastCancel)
	if agent != "" {
		assert.Equal(t, agent, eri.CancelAgent)
	}
	if want.widget != "" && assert.NotNil(t, eri.LastWidget) {
		assert.Equal(t, want.widget, eri.LastWidget.Banner)
	}
	if want.dispatch {
		require.NotNil(t, eri.LastDispatch)
		return
	}
	assert.Nil(t, eri.LastDispatch)
}

func TestFlow_HandlePREventType_CIFailedCap(t *testing.T) {
	t.Parallel()
	for _, tc := range prCIFailCapCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:   tc.errs.getBuild,
				bannerErr:     tc.errs.banner,
				failErr:       tc.errs.fail,
				failCount:     tc.failCount,
				failCountErr:  tc.errs.failCount,
				skipFailApply: tc.skipFailApply,
				failBanner:    tc.failBanner,
				err:           tc.errs.check,
				agent:         tc.agent,
			}
			spy.chunks = newSpyChunk(tc.chunk, tc.failCount)
			cancelErr := tc.errs.notify
			if tc.errs.cancel != nil {
				cancelErr = tc.errs.cancel
			}
			widgetErr := tc.errs.notify
			if tc.errs.widget != nil {
				widgetErr = tc.errs.widget
			}
			eri := &eritest.Client{
				SimulateActive: true,
				CancelErr:      cancelErr,
				MessageErr:     tc.errs.notify,
				WidgetErr:      widgetErr,
				PromptErr:      tc.errs.notify,
			}
			exec := newLifecycleExecutor(t, eri, tc.errs.notify, tc.apiStatus, "", tc.apiRoutes)
			f := NewFlow(spy, exec)
			ctx := context.Background()

			err := f.HandlePREventType(ctx, "b1", "c1", "ci_failed", tc.ev)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(ctx, "b1", "c1", extra.Event, extra)
			}
			assertLifecycleErr(t, tc.want.err, err)
			assertCapStore(t, spy, tc.want)
			assertCapEri(t, eri, tc.agent, tc.want)
		})
	}
}

type prTerminalEventCase struct {
	name        string
	event       string
	ev          pubsub.PRLifecycleEvent
	chunk       *model.VanadisChunk
	execNil     bool
	storeNil    bool
	getBuildErr error
	dispatchErr error
	bannerErr   error
	resetErr    error
	statusErr   error
	notifyErr   error
	want        prTerminalWant
}

type prTerminalWant struct {
	err            string
	banners        []string
	resets         []string
	failed         []string
	lifecycle      string
	cancel         string
	dispatch       bool
	dispatchID     string
	skipIfActive   *bool
	dispatchPrompt string
}

func prConflictAndMergedCases() []prTerminalEventCase {
	yes := true
	merged := pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}
	mergedHead := pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge-event"}
	mergedChunk := &model.VanadisChunk{Status: "merged", HeadSHA: "head"}

	return []prTerminalEventCase{
		{
			name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", ev: conflictEv("abcdef1234"),
			want: prTerminalWant{banners: []string{lifecycleConflict}, dispatch: true, skipIfActive: &yes},
		},
		{
			name: "conflict on failed chunk transitions to conflict and dispatches remediation", event: "conflict", ev: conflictEv("abcdef1234"), chunk: &model.VanadisChunk{Status: "failed"},
			want: prTerminalWant{banners: []string{lifecycleConflict}, dispatch: true, skipIfActive: &yes},
		},
		{name: "conflict with nil executor records banner only", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abc"}, execNil: true, want: prTerminalWant{banners: []string{lifecycleConflict}}},
		{name: "conflict with nil store and executor is a no-op", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict"}, execNil: true, storeNil: true},
		{name: "conflict on superseded head skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "superseded-sha"}, chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, want: prTerminalWant{err: lifecycleSkipped}},
		{name: "conflict get build error propagates", event: "conflict", ev: conflictEv("abcdef1234"), getBuildErr: errLifecycleDB, want: prTerminalWant{err: "db error"}},
		{
			name: "conflict skipped_active dispatch returns nil", event: "conflict", ev: conflictEv("abcdef1234"), dispatchErr: eriwire.ErrDispatchActive,
			want: prTerminalWant{banners: []string{lifecycleConflict}, dispatch: true, dispatchID: "vanadis-conflict-c1-abcdef1", skipIfActive: &yes},
		},
		{
			name: "conflict dispatch transport error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, dispatchErr: errLifecycleDB,
			want: prTerminalWant{err: "db error", banners: []string{lifecycleConflict}, dispatch: true, dispatchID: "vanadis-conflict-c1-abcdef1", skipIfActive: &yes},
		},
		{name: "conflict update banner error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, bannerErr: errLifecycleDB, want: prTerminalWant{err: "db error", banners: []string{lifecycleConflict}}},
		{name: "conflict on merged chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "merged"}, want: prTerminalWant{err: lifecycleSkipped}},
		{name: "conflict on deployed chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "deployed"}, want: prTerminalWant{err: lifecycleSkipped}},
		{name: "conflict on chunk with merge_sha skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, want: prTerminalWant{err: lifecycleSkipped}},
		{
			name: "merged cancels chunk dispatch and resets check state", event: "merged", ev: merged,
			want: prTerminalWant{lifecycle: "merged", resets: []string{"c1:head-sha:merge-sha"}, cancel: "c1"},
		},
		{
			name: "merged action chunk preserves chunk dispatch", event: "merged", ev: merged, chunk: &model.VanadisChunk{Mode: "action"},
			want: prTerminalWant{lifecycle: "merged", resets: []string{"c1:head-sha:merge-sha"}},
		},
		{
			name: "merged swallows cancel and publish errors", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}, notifyErr: errors.New("notify unavailable"),
			want: prTerminalWant{lifecycle: "merged", resets: []string{"c1:head:merge"}, cancel: "c1"},
		},
		{name: "merged without optional merge SHA skips reset", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head"}, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, want: prTerminalWant{lifecycle: "merged"}},
		{name: "merged rejected merge reset reloads lifecycle", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, want: prTerminalWant{err: lifecycleSkipped, lifecycle: "merged", resets: []string{"c1:head:merge-event"}}},
		{name: "merged failed merge reset retries", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: errLifecycleDB, want: prTerminalWant{err: "db error", lifecycle: "merged", resets: []string{"c1:head:merge-event"}}},
		{name: "merged lifecycle status error propagates", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, statusErr: errLifecycleDB, want: prTerminalWant{err: "db error", lifecycle: "merged"}},
		{name: "merged with nil store and executor is a no-op", event: "merged", ev: mergedHead, execNil: true, storeNil: true},
		{name: "closed_unmerged fails chunk and leaves the wake to reconcile", event: "closed_unmerged", ev: pubsub.PRLifecycleEvent{Event: "closed_unmerged"}, want: prTerminalWant{failed: []string{"c1:pr_closed_unmerged"}, cancel: "c1"}},
	}
}

func prCISkipAndDeployCases() []prTerminalEventCase {
	yes := true
	return []prTerminalEventCase{
		{name: "ci_failed on merged chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "merged"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on deployed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deployed"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on synced chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "synced"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on deploying chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploying"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on deploy_failed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploy_failed"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on completed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "completed"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on cancelled chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "cancelled"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on chunk with merge_sha skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{lifecycleCIBanner}}},
		{name: "ci_failed on superseded head skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "superseded-sha"), chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, want: prTerminalWant{err: lifecycleSkipped, banners: []string{`c1:ci_failed:CI check "unit-tests" failed`}}},
		{
			name: "deploy_failed sets status banner and dispatches silent wake", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"},
			want: prTerminalWant{banners: []string{"c1:deploy_failed:Deployment failed for eri: crashloop"}, dispatch: true, skipIfActive: &yes, dispatchPrompt: "Deployment failed for job eri on aerial (PR #101"},
		},
		{
			name: "sync_failed sets status banner and dispatches silent wake", event: "sync_failed", ev: pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"},
			want: prTerminalWant{banners: []string{"c1:deploy_failed:Git sync failed: git pull conflict"}, dispatch: true, skipIfActive: &yes, dispatchPrompt: "Git sync failed for PR #102 on aerial"},
		},
		{name: "deploy_failed banner update error propagates", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: errLifecycleDB, want: prTerminalWant{err: "db error", banners: []string{"c1:deploy_failed:Deployment failed for synapse"}}},
		{name: "deploy_failed on completed chunk skips wake prompt", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: ErrPRTransitionSkipped, want: prTerminalWant{err: lifecycleSkipped, banners: []string{"c1:deploy_failed:Deployment failed for synapse"}}},
	}
}

func prTerminalEventCases() []prTerminalEventCase {
	return append(prConflictAndMergedCases(), prCISkipAndDeployCases()...)
}

func assertTerminalEri(t *testing.T, eri *eritest.Client, want prTerminalWant) {
	t.Helper()
	assert.Equal(t, want.cancel, eri.LastCancel)
	if !want.dispatch {
		assert.Nil(t, eri.LastDispatch)
		return
	}
	require.NotNil(t, eri.LastDispatch)
	if want.dispatchID != "" {
		assert.Equal(t, want.dispatchID, eri.LastDispatch.ID)
	}
	if want.skipIfActive != nil {
		assert.Equal(t, *want.skipIfActive, eri.LastDispatch.SkipIfActive)
	}
	if want.dispatchPrompt != "" {
		assert.Contains(t, eri.LastDispatch.Prompt, want.dispatchPrompt)
	}
}

func TestFlow_HandlePREventType_TerminalAndDeployEvents(t *testing.T) {
	t.Parallel()
	for _, tc := range prTerminalEventCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr: tc.getBuildErr,
				bannerErr:   tc.bannerErr,
				resetErr:    tc.resetErr,
				statusErr:   tc.statusErr,
			}
			spy.chunks = newSpyChunk(tc.chunk, 0)
			eri := &eritest.Client{
				DispatchErr:    tc.dispatchErr,
				SimulateActive: true,
				CancelErr:      tc.notifyErr,
				MessageErr:     tc.notifyErr,
				PromptErr:      tc.notifyErr,
			}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, tc.notifyErr, 0, "")
			}
			f := NewFlow(st, exec)
			err := f.HandlePREventType(context.Background(), "b1", "c1", tc.event, tc.ev)
			assertLifecycleErr(t, tc.want.err, err)
			assert.Equal(t, tc.want.banners, spy.banners)
			assert.Equal(t, tc.want.resets, spy.resets)
			assert.Equal(t, tc.want.failed, spy.failed)
			assert.Equal(t, tc.want.lifecycle, spy.lifecycleStatus)
			assertTerminalEri(t, eri, tc.want)
		})
	}
}
