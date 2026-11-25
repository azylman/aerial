package prflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
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
	name           string
	ev             pubsub.PRLifecycleEvent
	extraEvents    []pubsub.PRLifecycleEvent
	chunk          *model.VanadisChunk
	failCount      int
	apiRoutes      map[string][]githubapitest.Response
	dispatchErr    error
	wantChecks     []string
	wantBanners    []string
	wantWidget     string
	wantDispatchID string
	wantSkipActive *bool
	wantSHA        string
}

func prCIFailRemediationCases() []prCIFailRemediationCase {
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
	coalescedJobs := map[string][]githubapitest.Response{
		"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
			{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"completed","conclusion":"failure"}]}`},
		},
		"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
		"GET /repos/example-owner/aerial/actions/jobs/11/logs": {{Status: http.StatusOK, Body: "test panic"}},
	}

	return []prCIFailRemediationCase{
		{
			name: "ci_failed within cap dispatches background remediation, increments count and syncs widget", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 1,
			wantChecks: []string{"c1:abcdef1234:unit-tests:failed"}, wantBanners: []string{lifecycleCIBanner}, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2", wantSkipActive: &no, wantWidget: `CI check "unit-tests" failed — remediation in progress`,
		},
		{
			name: "six failing checks on same head coalesce into one accepted remediation attempt", ev: ciFailEv("check-1", "abcdef1234"), extraEvents: sameHead,
			wantBanners: sameHeadBanners, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipActive: &no,
		},
		{
			name: "ci_failed skipped_active dispatch returns nil without consuming remediation attempt budget", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{RemediatedSHA: "abcdef1234"}, dispatchErr: eriwire.ErrDispatchActive,
			wantBanners: []string{lifecycleCIBanner}, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipActive: &yes,
		},
		{
			name: "ci_failed on new head_sha dispatches with SkipIfActive false and records RemediatedSHA", ev: ciFailEv("unit-tests", "new-head-sha"), chunk: &model.VanadisChunk{RemediatedSHA: "old-head-sha"},
			wantBanners: []string{lifecycleCIBanner}, wantDispatchID: "vanadis-c1-cifail-new-head-sha-1", wantSkipActive: &no, wantSHA: "new-head-sha",
		},
		{
			name: "sibling pending job holds remediation",
			ev:   pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "lint", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123/job/10", Details: "lint failed"},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"in_progress","conclusion":null}]}`},
				},
			},
			wantChecks: []string{"c1:abcdef1234:lint:failed"}, wantBanners: []string{`c1:ci_failed:CI check "lint" failed`}, wantWidget: `CI check "lint" failed — waiting for remaining CI jobs to finish`,
		},
		{
			name:      "completed run with multiple failed jobs coalesces into single wake",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "lint", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123/job/10", Details: "lint failure"},
			apiRoutes: coalescedJobs, failCount: 1,
			wantChecks: []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"}, wantBanners: []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`}, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2", wantSkipActive: &no, wantWidget: `CI check "lint, test" failed — remediation in progress`,
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
			failCount:  1,
			wantChecks: []string{"c1:abcdef1234:lint:failed"}, wantBanners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`, `c1:ci_failed:CI check "lint" failed`}, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2", wantSkipActive: &no, wantWidget: `CI check "lint" failed — remediation in progress`,
		},
	}
}

func TestFlow_HandlePREventType_CIFailedRemediation(t *testing.T) {
	t.Parallel()
	for _, tc := range prCIFailRemediationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{failCount: tc.failCount}
			spy.chunks = newSpyChunk(tc.chunk, tc.failCount)
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true}
			exec := newLifecycleExecutor(t, eri, nil, 0, "", tc.apiRoutes)
			f := NewFlow(spy, exec)
			ctx := context.Background()

			err := f.HandlePREventType(ctx, "b1", "c1", "ci_failed", tc.ev)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(ctx, "b1", "c1", extra.Event, extra)
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantBanners, spy.banners)
			if tc.wantChecks != nil {
				assert.ElementsMatch(t, tc.wantChecks, spy.calls)
			}
			wantFailCalls := 0
			if tc.wantDispatchID != "" && tc.dispatchErr == nil {
				wantFailCalls = 1
			}
			assert.Len(t, spy.failCountCalls, wantFailCalls)
			if tc.wantSHA != "" {
				assert.Equal(t, tc.wantSHA, spy.chunk("c1").RemediatedSHA)
			}
			if tc.wantWidget != "" && assert.NotNil(t, eri.LastWidget) {
				assert.Equal(t, tc.wantWidget, eri.LastWidget.Banner)
			}
			if tc.wantDispatchID != "" {
				require.NotNil(t, eri.LastDispatch)
				assert.Equal(t, "vanadis-c1", eri.LastDispatch.SessionID)
				assert.Equal(t, "c1", eri.LastDispatch.VanadisChunkID)
				assert.Equal(t, "th-1", eri.LastDispatch.TargetID)
				assert.Equal(t, tc.wantDispatchID, eri.LastDispatch.ID)
				if tc.wantSkipActive != nil {
					assert.Equal(t, *tc.wantSkipActive, eri.LastDispatch.SkipIfActive)
				}
				assert.NotEmpty(t, eri.LastDispatch.Prompt)
			} else {
				assert.Nil(t, eri.LastDispatch)
			}
		})
	}
}

type prCIFailRemediationErrorCase struct {
	name         string
	ev           pubsub.PRLifecycleEvent
	apiStatus    int
	apiRoutes    map[string][]githubapitest.Response
	dispatchErr  error
	bannerErr    error
	checkErr     error
	getBuildErr  error
	failCountErr error
	wantErr      string
	wantBanners  []string
}

func prCIFailRemediationErrorCases() []prCIFailRemediationErrorCase {
	cdRoutes := map[string][]githubapitest.Response{
		"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
			{Status: http.StatusOK, Body: `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"}]}`},
		},
		"GET /repos/example-owner/aerial/actions/jobs/10/logs": {{Status: http.StatusOK, Body: "lint syntax error"}},
	}

	return []prCIFailRemediationErrorCase{
		{
			name: "ci_failed dispatch transport error propagates without consuming remediation attempt budget", ev: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{lifecycleCIBanner},
		},
		{
			name: "ci_failed details API error propagates before dispatch", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, apiStatus: http.StatusInternalServerError,
			wantErr: "500", wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`},
		},
		{
			name: "ci_failed banner update error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), bannerErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{lifecycleCIBanner},
		},
		{
			name: "ci_failed get build error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), getBuildErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{lifecycleCIBanner},
		},
		{
			name: "ci_failed increment count error after accepted dispatch propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCountErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{lifecycleCIBanner},
		},
		{
			name:      "coalesced check record error propagates",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "Continuous Delivery", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123", Details: "workflow concluded with failure"},
			apiRoutes: cdRoutes, checkErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`},
		},
		{
			name:      "coalesced banner update error propagates",
			ev:        pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "Continuous Delivery", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42, CheckURL: "https://github.com/example-owner/aerial/actions/runs/123", Details: "workflow concluded with failure"},
			apiRoutes: cdRoutes, bannerErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`},
		},
	}
}

func TestFlow_HandlePREventType_CIFailedRemediationErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range prCIFailRemediationErrorCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:  tc.getBuildErr,
				bannerErr:    tc.bannerErr,
				err:          tc.checkErr,
				failCountErr: tc.failCountErr,
			}
			spy.chunks = newSpyChunk(nil, 0)
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true}
			exec := newLifecycleExecutor(t, eri, nil, tc.apiStatus, "", tc.apiRoutes)
			f := NewFlow(spy, exec)
			err := f.HandlePREventType(context.Background(), "b1", "c1", "ci_failed", tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantBanners, spy.banners)
			if tc.failCountErr != nil {
				assert.Len(t, spy.failCountCalls, 1)
				require.NotNil(t, eri.LastDispatch)
				assert.False(t, eri.LastDispatch.SkipIfActive)
			}
		})
	}
}

type prCIFailCapCase struct {
	name              string
	ev                pubsub.PRLifecycleEvent
	extraEvents       []pubsub.PRLifecycleEvent
	chunk             *model.VanadisChunk
	failCount         int
	agent             string
	apiRoutes         map[string][]githubapitest.Response
	wantFailed        []string
	wantBanners       []string
	wantChecks        []string
	wantFailCalls     int
	wantCIFailCount   int
	wantRemediatedSHA string
	wantDispatch      bool
}

func prCIFailCapReachedCases() []prCIFailCapCase {
	trailing := []pubsub.PRLifecycleEvent{ciFailEv("unit-tests", "abcdef1234")}
	trailingThenNewHead := append(append([]pubsub.PRLifecycleEvent{}, trailing...), ciFailEv("unit-tests", "feeedd9876"))
	newHeadRetry := append(append([]pubsub.PRLifecycleEvent{}, trailingThenNewHead...), ciFailEv("unit-tests", "feeedd9876"))

	return []prCIFailCapCase{
		{
			name: "ci_failed exceeding cap marks chunk failed and sends alert message", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5,
			wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCalls: 1,
		},
		{
			name: "ci_failed tripping cap increments fail count without overwriting RemediatedSHA", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 5, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"},
			wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCalls: 1, wantRemediatedSHA: "attempt5-active-sha",
		},
		{
			name: "ci_failed retry with failCount 6 skips increment and completes cap reached", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 6, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"},
			wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantRemediatedSHA: "attempt5-active-sha",
		},
		{
			name: "trailing same-SHA failure at cap neither increments, fails nor cancels the active remediation", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailing, failCount: 4,
			wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner}, wantFailCalls: 1, wantCIFailCount: 5, wantDispatch: true,
		},
		{
			name: "new head after trailing same-SHA failure trips cap and keeps RemediatedSHA", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailingThenNewHead, failCount: 4,
			wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCalls: 2, wantCIFailCount: 6, wantDispatch: true, wantRemediatedSHA: "abcdef1234",
		},
		{
			name: "retry of the cap-tripping head does not increment again", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: newHeadRetry, failCount: 4,
			wantFailed: []string{lifecycleCapFail, lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCalls: 2, wantCIFailCount: 6, wantDispatch: true,
		},
		{
			name: "ci_failed exceeding cap dispatches with build agent", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, agent: "reviewer-bot",
			wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCalls: 1,
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
			failCount:  5,
			wantChecks: []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"}, wantBanners: []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`}, wantFailed: []string{lifecycleCapFail}, wantFailCalls: 1,
		},
	}
}

func assertCapReachedStore(t *testing.T, spy *checkStatusSpyStore, tc prCIFailCapCase) {
	t.Helper()
	assert.Equal(t, tc.wantFailed, spy.failed)
	assert.Equal(t, tc.wantBanners, spy.banners)
	if tc.wantChecks != nil {
		assert.ElementsMatch(t, tc.wantChecks, spy.calls)
	}
	assert.Len(t, spy.failCountCalls, tc.wantFailCalls)
	if tc.wantCIFailCount != 0 {
		assert.Equal(t, tc.wantCIFailCount, spy.chunk("c1").CIFailCount)
	}
	if tc.wantRemediatedSHA != "" {
		assert.Equal(t, tc.wantRemediatedSHA, spy.chunk("c1").RemediatedSHA)
	}
}

func assertCapReachedEri(t *testing.T, eri *eritest.Client, tc prCIFailCapCase, capPrompt, capProgress string) {
	t.Helper()
	if len(tc.wantFailed) > 0 {
		assert.Contains(t, eri.LastPrompt, capPrompt)
		assert.Equal(t, "th-1", eri.LastThreadID)
		assert.Equal(t, "c1", eri.LastCancel)
		if tc.agent != "" {
			assert.Equal(t, tc.agent, eri.CancelAgent)
		}
		if assert.NotNil(t, eri.LastWidget) {
			assert.Equal(t, capProgress, eri.LastWidget.Banner)
		}
	} else {
		assert.Empty(t, eri.LastPrompt)
		assert.Empty(t, eri.LastCancel)
	}
	if tc.wantDispatch {
		require.NotNil(t, eri.LastDispatch)
	} else {
		assert.Nil(t, eri.LastDispatch)
	}
}

func TestFlow_HandlePREventType_CIFailedCapReached(t *testing.T) {
	t.Parallel()
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"
	for _, tc := range prCIFailCapReachedCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{failCount: tc.failCount, agent: tc.agent}
			spy.chunks = newSpyChunk(tc.chunk, tc.failCount)
			eri := &eritest.Client{SimulateActive: true}
			exec := newLifecycleExecutor(t, eri, nil, 0, "", tc.apiRoutes)
			f := NewFlow(spy, exec)
			ctx := context.Background()

			err := f.HandlePREventType(ctx, "b1", "c1", "ci_failed", tc.ev)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(ctx, "b1", "c1", extra.Event, extra)
			}
			require.NoError(t, err)
			assertCapReachedStore(t, spy, tc)
			assertCapReachedEri(t, eri, tc, capPrompt, capProgress)
		})
	}
}

type prCIFailCapErrorCase struct {
	name         string
	ev           pubsub.PRLifecycleEvent
	chunk        *model.VanadisChunk
	failCount    int
	failBanner   string
	apiStatus    int
	checkErr     error
	bannerErr    error
	getBuildErr  error
	failCountErr error
	failErr      error
	notifyErr    error
	cancelErr    error
	widgetErr    error
}

func prCIFailCapErrorCases() []prCIFailCapErrorCase {
	return []prCIFailCapErrorCase{
		{
			name: "ci_failed increment count error at cap propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failCountErr: errLifecycleDB,
		},
		{
			name: "ci_failed record check skipped on already failed chunk resumes cap reached notifications", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}, failBanner: lifecycleCapText, checkErr: ErrPRTransitionSkipped,
		},
		{
			name: "ci_failed update banner skipped on already failed chunk resumes cap reached notifications", ev: ciFailEv("unit-tests", "abcdef1234"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}, failBanner: lifecycleCapText, bannerErr: ErrPRTransitionSkipped,
		},
		{
			name: "ci_failed record check skipped with stale head does not re-notify", ev: ciFailEv("unit-tests", "stale-sha"),
			chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "current-sha"}, checkErr: ErrPRTransitionSkipped,
		},
		{
			name: "ci_failed record check skipped with lookup error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), checkErr: ErrPRTransitionSkipped, getBuildErr: errLifecycleDB,
		},
		{
			name: "ci_failed exceeding cap preserves chunk failure and propagates notification error", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, notifyErr: errLifecycleDB,
		},
		{
			name: "ci_failed exceeding cap retry with matching banner completes notifications", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failBanner: lifecycleCapText,
		},
		{
			name: "ci_failed exceeding cap fail chunk error propagates", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failErr: errLifecycleDB,
		},
		{
			name: "ci_failed at cap with unapplied fail chunk skips cap without cancelling", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failBanner: "unapplied",
		},
		{
			name: "ci_failed exceeding cap failure details error propagates", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, failCount: 5, apiStatus: http.StatusInternalServerError,
		},
		{
			name: "ci_failed exceeding cap cancel warning logged and continues", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, cancelErr: errors.New("cancel down"),
		},
		{
			name: "ci_failed exceeding cap widget warning logged and continues", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, widgetErr: errors.New("widget down"),
		},
	}
}

func resolveCapErrorExpectation(tc prCIFailCapErrorCase) string {
	switch {
	case tc.apiStatus != 0:
		return strconv.Itoa(tc.apiStatus)
	case tc.checkErr != nil && tc.chunk != nil && tc.chunk.HeadSHA != tc.ev.HeadSHA:
		return lifecycleSkipped
	case tc.failCountErr != nil || tc.failErr != nil || tc.notifyErr != nil || tc.getBuildErr != nil:
		return "db error"
	default:
		return ""
	}
}

func assertCapErrorStore(t *testing.T, spy *checkStatusSpyStore, tc prCIFailCapErrorCase) {
	t.Helper()
	var wantFailed []string
	if (tc.failCount == 5 && tc.failCountErr == nil) || (tc.chunk != nil && tc.chunk.Status == "failed" && tc.failBanner == lifecycleCapText) {
		wantFailed = []string{lifecycleCapFail}
	}
	assert.Equal(t, wantFailed, spy.failed)

	var wantBanners []string
	if tc.failCount == 5 {
		if tc.ev.CheckName == "roborev" {
			wantBanners = []string{`c1:ci_failed:CI check "roborev" failed`}
		} else {
			wantBanners = []string{lifecycleCIBanner}
		}
	} else if tc.bannerErr != nil {
		wantBanners = []string{lifecycleCIBanner}
	}
	assert.Equal(t, wantBanners, spy.banners)

	if tc.failCount == 5 {
		assert.Len(t, spy.failCountCalls, 1)
	} else {
		assert.Empty(t, spy.failCountCalls)
	}
}

func assertCapErrorEri(t *testing.T, eri *eritest.Client, tc prCIFailCapErrorCase, capPrompt, capProgress string) {
	t.Helper()
	if tc.failBanner == lifecycleCapText || (tc.failCount == 5 && tc.failCountErr == nil && tc.failErr == nil && tc.apiStatus == 0 && tc.failBanner != "unapplied") {
		assert.Equal(t, "c1", eri.LastCancel)
		assert.Equal(t, "th-1", eri.LastThreadID)
		assert.Contains(t, eri.LastPrompt, capPrompt)
		if tc.notifyErr == nil && assert.NotNil(t, eri.LastWidget) {
			assert.Equal(t, capProgress, eri.LastWidget.Banner)
		}
	}
}

func TestFlow_HandlePREventType_CIFailedCapErrorsAndSkips(t *testing.T) {
	t.Parallel()
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"
	for _, tc := range prCIFailCapErrorCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:   tc.getBuildErr,
				bannerErr:     tc.bannerErr,
				failErr:       tc.failErr,
				failCount:     tc.failCount,
				failCountErr:  tc.failCountErr,
				skipFailApply: tc.failBanner != "",
				failBanner:    tc.failBanner,
				err:           tc.checkErr,
			}
			if tc.failBanner == "unapplied" {
				spy.failBanner = ""
			}
			spy.chunks = newSpyChunk(tc.chunk, tc.failCount)
			eri := &eritest.Client{
				SimulateActive: true,
				CancelErr:      tc.cancelErr,
				MessageErr:     tc.notifyErr,
				WidgetErr:      tc.widgetErr,
				PromptErr:      tc.notifyErr,
			}
			exec := newLifecycleExecutor(t, eri, tc.notifyErr, tc.apiStatus, "")
			f := NewFlow(spy, exec)
			err := f.HandlePREventType(context.Background(), "b1", "c1", "ci_failed", tc.ev)

			assertLifecycleErr(t, resolveCapErrorExpectation(tc), err)
			assertCapErrorStore(t, spy, tc)
			assertCapErrorEri(t, eri, tc, capPrompt, capProgress)
		})
	}
}

type prWakeEventCase struct {
	name         string
	event        string
	ev           pubsub.PRLifecycleEvent
	chunk        *model.VanadisChunk
	execNil      bool
	storeNil     bool
	getBuildErr  error
	dispatchErr  error
	bannerErr    error
	wantErr      string
	wantBanners  []string
	wantDispatch bool
	wantPrompt   string
}

func prConflictAndDeployCases() []prWakeEventCase {
	return []prWakeEventCase{
		{
			name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", ev: conflictEv("abcdef1234"),
			wantBanners: []string{lifecycleConflict}, wantDispatch: true,
		},
		{
			name: "conflict on failed chunk transitions to conflict and dispatches remediation", event: "conflict", ev: conflictEv("abcdef1234"), chunk: &model.VanadisChunk{Status: "failed"},
			wantBanners: []string{lifecycleConflict}, wantDispatch: true,
		},
		{name: "conflict with nil executor records banner only", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abc"}, execNil: true, wantBanners: []string{lifecycleConflict}},
		{name: "conflict with nil store and executor is a no-op", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict"}, execNil: true, storeNil: true},
		{name: "conflict on superseded head skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "superseded-sha"}, chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, wantErr: lifecycleSkipped},
		{name: "conflict get build error propagates", event: "conflict", ev: conflictEv("abcdef1234"), getBuildErr: errLifecycleDB, wantErr: "db error"},
		{
			name: "conflict skipped_active dispatch returns nil", event: "conflict", ev: conflictEv("abcdef1234"), dispatchErr: eriwire.ErrDispatchActive,
			wantBanners: []string{lifecycleConflict}, wantDispatch: true,
		},
		{
			name: "conflict dispatch transport error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, dispatchErr: errLifecycleDB,
			wantErr: "db error", wantBanners: []string{lifecycleConflict}, wantDispatch: true,
		},
		{name: "conflict update banner error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, bannerErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleConflict}},
		{name: "conflict on merged chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "merged"}, wantErr: lifecycleSkipped},
		{name: "conflict on deployed chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "deployed"}, wantErr: lifecycleSkipped},
		{name: "conflict on chunk with merge_sha skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, wantErr: lifecycleSkipped},
		{
			name: "deploy_failed sets status banner and dispatches silent wake", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"},
			wantBanners: []string{"c1:deploy_failed:Deployment failed for eri: crashloop"}, wantDispatch: true, wantPrompt: "Deployment failed for job eri on aerial (PR #101",
		},
		{
			name: "sync_failed sets status banner and dispatches silent wake", event: "sync_failed", ev: pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"},
			wantBanners: []string{"c1:deploy_failed:Git sync failed: git pull conflict"}, wantDispatch: true, wantPrompt: "Git sync failed for PR #102 on aerial",
		},
		{name: "deploy_failed banner update error propagates", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"}},
		{name: "deploy_failed on completed chunk skips wake prompt", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: ErrPRTransitionSkipped, wantErr: lifecycleSkipped, wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"}},
	}
}

func TestFlow_HandlePREventType_ConflictAndDeployFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range prConflictAndDeployCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr: tc.getBuildErr,
				bannerErr:   tc.bannerErr,
			}
			spy.chunks = newSpyChunk(tc.chunk, 0)
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			f := NewFlow(st, exec)
			err := f.HandlePREventType(context.Background(), "b1", "c1", tc.event, tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantBanners, spy.banners)
			if !tc.wantDispatch {
				assert.Nil(t, eri.LastDispatch)
				return
			}
			require.NotNil(t, eri.LastDispatch)
			assert.True(t, eri.LastDispatch.SkipIfActive)
			if tc.event == "conflict" {
				assert.Equal(t, "vanadis-conflict-c1-abcdef1", eri.LastDispatch.ID)
			}
			if tc.wantPrompt != "" {
				assert.Contains(t, eri.LastDispatch.Prompt, tc.wantPrompt)
			}
		})
	}
}

type prTerminalStateCase struct {
	name          string
	event         string
	ev            pubsub.PRLifecycleEvent
	chunk         *model.VanadisChunk
	execNil       bool
	storeNil      bool
	resetErr      error
	statusErr     error
	notifyErr     error
	wantErr       string
	wantLifecycle string
	wantResets    []string
	wantBanners   []string
}

func prTerminalStateCases() []prTerminalStateCase {
	merged := pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}
	mergedHead := pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge-event"}
	mergedChunk := &model.VanadisChunk{Status: "merged", HeadSHA: "head"}

	return []prTerminalStateCase{
		{
			name: "merged cancels chunk dispatch and resets check state", event: "merged", ev: merged,
			wantLifecycle: "merged", wantResets: []string{"c1:head-sha:merge-sha"},
		},
		{
			name: "merged action chunk preserves chunk dispatch", event: "merged", ev: merged, chunk: &model.VanadisChunk{Mode: "action"},
			wantLifecycle: "merged", wantResets: []string{"c1:head-sha:merge-sha"},
		},
		{
			name: "merged swallows cancel and publish errors", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}, notifyErr: errors.New("notify unavailable"),
			wantLifecycle: "merged", wantResets: []string{"c1:head:merge"},
		},
		{name: "merged without optional merge SHA skips reset", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head"}, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, wantLifecycle: "merged"},
		{name: "merged rejected merge reset reloads lifecycle", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, wantErr: lifecycleSkipped, wantLifecycle: "merged", wantResets: []string{"c1:head:merge-event"}},
		{name: "merged failed merge reset retries", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: errLifecycleDB, wantErr: "db error", wantLifecycle: "merged", wantResets: []string{"c1:head:merge-event"}},
		{name: "merged lifecycle status error propagates", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, statusErr: errLifecycleDB, wantErr: "db error", wantLifecycle: "merged"},
		{name: "merged with nil store and executor is a no-op", event: "merged", ev: mergedHead, execNil: true, storeNil: true},
		{name: "closed_unmerged fails chunk and leaves the wake to reconcile", event: "closed_unmerged", ev: pubsub.PRLifecycleEvent{Event: "closed_unmerged"}},
		{name: "ci_failed on merged chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "merged"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on deployed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deployed"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on synced chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "synced"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on deploying chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploying"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on deploy_failed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploy_failed"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on completed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "completed"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on cancelled chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "cancelled"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on chunk with merge_sha skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, wantErr: lifecycleSkipped, wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed on superseded head skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "superseded-sha"), chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, wantErr: lifecycleSkipped, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}},
	}
}

func TestFlow_HandlePREventType_MergedClosedAndTerminalSkips(t *testing.T) {
	t.Parallel()
	for _, tc := range prTerminalStateCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				resetErr:  tc.resetErr,
				statusErr: tc.statusErr,
			}
			spy.chunks = newSpyChunk(tc.chunk, 0)
			eri := &eritest.Client{
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
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantBanners, spy.banners)
			assert.Equal(t, tc.wantResets, spy.resets)
			assert.Equal(t, tc.wantLifecycle, spy.lifecycleStatus)

			wantCancel := ""
			wantFailed := []string(nil)
			if tc.event == "closed_unmerged" {
				wantFailed = []string{"c1:pr_closed_unmerged"}
				wantCancel = "c1"
			} else if tc.event == "merged" && (tc.chunk == nil || tc.chunk.Mode != "action") && !tc.execNil {
				wantCancel = "c1"
			}
			assert.Equal(t, wantFailed, spy.failed)
			assert.Equal(t, wantCancel, eri.LastCancel)
			assert.Nil(t, eri.LastDispatch)
		})
	}
}
