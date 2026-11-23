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

type prEventCase struct {
	name        string
	event       string
	ev          pubsub.PRLifecycleEvent
	extraEvents []pubsub.PRLifecycleEvent

	execNil   bool
	storeNil  bool
	apiStatus int
	apiBody   string
	apiRoutes map[string][]githubapitest.Response
	chunk     *model.VanadisChunk

	failCount     int
	failCountErr  error
	getBuildErr   error
	bannerErr     error
	checkErr      error
	failErr       error
	resetErr      error
	statusErr     error
	skipFailApply bool
	failBanner    string
	dispatchErr   error
	notifyErr     error
	agent         string
	cancelErr     error
	widgetErr     error

	wantErr            string
	wantFailed         []string
	wantBanners        []string
	wantResets         []string
	wantChecks         []string // nil: not asserted
	wantLifecycle      string
	wantFailCountCalls int
	wantCIFailCount    int
	wantDispatch       bool
	wantDispatchID     string
	wantSkipIfActive   *bool
	wantDispatchPrompt string
	wantRemediatedSHA  string
	wantCancel         string
	wantMsgChan        string
	wantWidget         string
	wantThreadID       string
	wantPrompt         string
}

func newPREventSpy(tc prEventCase) *checkStatusSpyStore {
	spy := &checkStatusSpyStore{
		getBuildErr: tc.getBuildErr, bannerErr: tc.bannerErr, failErr: tc.failErr, failCount: tc.failCount, failCountErr: tc.failCountErr,
		skipFailApply: tc.skipFailApply, failBanner: tc.failBanner, err: tc.checkErr, resetErr: tc.resetErr, statusErr: tc.statusErr,
		agent: tc.agent,
	}
	if tc.chunk != nil {
		c := *tc.chunk
		c.ID, c.BuildID, c.CIFailCount = "c1", "b1", tc.failCount
		if c.Effort == "" {
			c.Effort = effortMedium
		}
		spy.chunks = []model.VanadisChunk{c}
	}
	return spy
}

func prEventBasicCases() []prEventCase {
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
	trailing := []pubsub.PRLifecycleEvent{ciFailEv("unit-tests", "abcdef1234")}
	trailingThenNewHead := append(append([]pubsub.PRLifecycleEvent{}, trailing...), ciFailEv("unit-tests", "feeedd9876"))
	newHeadRetry := append(append([]pubsub.PRLifecycleEvent{}, trailingThenNewHead...), ciFailEv("unit-tests", "feeedd9876"))
	capChunk := &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"
	headAPI := `{"state":"open","merged":false,"head":{"sha":"sha-2"}}`

	return []prEventCase{
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
		{name: "ci_failed within cap dispatches background remediation, increments count and syncs widget", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 1, wantChecks: []string{"c1:abcdef1234:unit-tests:failed"}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2", wantSkipIfActive: &no, wantWidget: `CI check "unit-tests" failed — remediation in progress`},
		{name: "six failing checks on same head coalesce into one accepted remediation attempt", event: "ci_failed", ev: ciFailEv("check-1", "abcdef1234"), extraEvents: sameHead, wantBanners: sameHeadBanners, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &no},
		{name: "ci_failed skipped_active dispatch returns nil without consuming remediation attempt budget", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{RemediatedSHA: "abcdef1234"}, dispatchErr: eriwire.ErrDispatchActive, wantBanners: []string{lifecycleCIBanner}, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &yes},
		{name: "ci_failed dispatch transport error propagates without consuming remediation attempt budget", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleCIBanner}, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &no},
		{name: "ci_failed details API error propagates before dispatch", event: "ci_failed", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, apiStatus: http.StatusInternalServerError, wantErr: "500", wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`}},
		{name: "ci_failed banner update error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), bannerErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed get build error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), getBuildErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleCIBanner}},
		{name: "ci_failed increment count error after accepted dispatch propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCountErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantDispatch: true, wantSkipIfActive: &no},
		{name: "ci_failed increment count error at cap propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failCountErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1},
		{name: "ci_failed exceeding cap marks chunk failed and sends alert message", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed tripping cap increments fail count without overwriting RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 5, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"}, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantRemediatedSHA: "attempt5-active-sha", wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed retry with failCount 6 skips increment and completes cap reached", event: "ci_failed", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 6, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"}, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantRemediatedSHA: "attempt5-active-sha", wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "trailing same-SHA failure at cap neither increments, fails nor cancels the active remediation", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailing, failCount: 4, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 1, wantCIFailCount: 5, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-5", wantSkipIfActive: &no},
		{name: "new head after trailing same-SHA failure trips cap and keeps RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailingThenNewHead, failCount: 4, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 2, wantCIFailCount: 6, wantDispatch: true, wantRemediatedSHA: "abcdef1234", wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "retry of the cap-tripping head does not increment again", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: newHeadRetry, failCount: 4, wantFailed: []string{lifecycleCapFail, lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 2, wantCIFailCount: 6, wantDispatch: true, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed record check skipped on already failed chunk resumes cap reached notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), checkErr: ErrPRTransitionSkipped, chunk: capChunk, skipFailApply: true, failBanner: lifecycleCapText, wantFailed: []string{lifecycleCapFail}, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed update banner skipped on already failed chunk resumes cap reached notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), bannerErr: ErrPRTransitionSkipped, chunk: capChunk, skipFailApply: true, failBanner: lifecycleCapText, wantBanners: []string{lifecycleCIBanner}, wantFailed: []string{lifecycleCapFail}, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed record check skipped with stale head does not re-notify", event: "ci_failed", ev: ciFailEv("unit-tests", "stale-sha"), checkErr: ErrPRTransitionSkipped, chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "current-sha"}, wantErr: lifecycleSkipped},
		{name: "ci_failed record check skipped with lookup error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), checkErr: ErrPRTransitionSkipped, getBuildErr: errLifecycleDB, wantErr: "db error"},
		{name: "ci_failed exceeding cap preserves chunk failure and propagates notification error", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, notifyErr: errLifecycleDB, wantErr: "db error", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt},
		{name: "ci_failed exceeding cap retry with matching banner completes notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, failBanner: lifecycleCapText, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed exceeding cap fail chunk error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failErr: errLifecycleDB, wantErr: "db error", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1},
		{name: "ci_failed at cap with unapplied fail chunk skips cap without cancelling", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1},
		{name: "ci_failed exceeding cap dispatches with build agent", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, agent: "reviewer-bot", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed exceeding cap failure details error propagates", event: "ci_failed", ev: pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"}, failCount: 5, apiStatus: http.StatusInternalServerError, wantErr: "500", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`}, wantFailCountCalls: 1},
		{name: "ci_failed exceeding cap cancel warning logged and continues", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, cancelErr: errors.New("cancel down"), wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed exceeding cap widget warning logged and continues", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, widgetErr: errors.New("widget down"), wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantThreadID: "th-1", wantPrompt: capPrompt, wantWidget: capProgress},
		{name: "ci_failed on new head_sha dispatches with SkipIfActive false and records RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "new-head-sha"), chunk: &model.VanadisChunk{RemediatedSHA: "old-head-sha"}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-new-head-sha-1", wantSkipIfActive: &no, wantRemediatedSHA: "new-head-sha"},
	}
}

func prEventTerminalCases() []prEventCase {
	yes := true
	merged := pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}
	mergedHead := pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge-event"}
	mergedChunk := &model.VanadisChunk{Status: "merged", HeadSHA: "head"}

	return []prEventCase{
		{name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", ev: conflictEv("abcdef1234"), wantBanners: []string{lifecycleConflict}, wantDispatch: true, wantSkipIfActive: &yes},
		{name: "conflict on failed chunk transitions to conflict and dispatches remediation", event: "conflict", ev: conflictEv("abcdef1234"), chunk: &model.VanadisChunk{Status: "failed"}, wantBanners: []string{lifecycleConflict}, wantDispatch: true, wantSkipIfActive: &yes},
		{name: "conflict with nil executor records banner only", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abc"}, execNil: true, wantBanners: []string{lifecycleConflict}},
		{name: "conflict with nil store and executor is a no-op", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict"}, execNil: true, storeNil: true},
		{name: "conflict on superseded head skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "superseded-sha"}, chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, wantErr: lifecycleSkipped},
		{name: "conflict get build error propagates", event: "conflict", ev: conflictEv("abcdef1234"), getBuildErr: errLifecycleDB, wantErr: "db error"},
		{name: "conflict skipped_active dispatch returns nil", event: "conflict", ev: conflictEv("abcdef1234"), dispatchErr: eriwire.ErrDispatchActive, wantBanners: []string{lifecycleConflict}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1", wantSkipIfActive: &yes},
		{name: "conflict dispatch transport error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, dispatchErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleConflict}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1", wantSkipIfActive: &yes},
		{name: "conflict update banner error propagates", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, bannerErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{lifecycleConflict}},
		{name: "conflict on merged chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "merged"}, wantErr: lifecycleSkipped},
		{name: "conflict on deployed chunk skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{Status: "deployed"}, wantErr: lifecycleSkipped},
		{name: "conflict on chunk with merge_sha skipped", event: "conflict", ev: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, wantErr: lifecycleSkipped},

		{name: "ci_failed on merged chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "merged"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on deployed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deployed"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on synced chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "synced"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on deploying chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploying"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on deploy_failed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "deploy_failed"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on completed chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "completed"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on cancelled chunk skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{Status: "cancelled"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on chunk with merge_sha skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), chunk: &model.VanadisChunk{MergeSHA: "merged-sha"}, wantBanners: []string{lifecycleCIBanner}, wantErr: lifecycleSkipped},
		{name: "ci_failed on superseded head skipped", event: "ci_failed", ev: ciFailEv("unit-tests", "superseded-sha"), chunk: &model.VanadisChunk{HeadSHA: "current-sha"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantErr: lifecycleSkipped},

		{name: "merged cancels chunk dispatch and resets check state", event: "merged", ev: merged, wantLifecycle: "merged", wantResets: []string{"c1:head-sha:merge-sha"}, wantCancel: "c1"},
		{name: "merged action chunk preserves chunk dispatch", event: "merged", ev: merged, chunk: &model.VanadisChunk{Mode: "action"}, wantLifecycle: "merged", wantResets: []string{"c1:head-sha:merge-sha"}},
		{name: "merged swallows cancel and publish errors", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}, notifyErr: errors.New("notify unavailable"), wantLifecycle: "merged", wantResets: []string{"c1:head:merge"}, wantCancel: "c1"},
		{name: "merged without optional merge SHA skips reset", event: "merged", ev: pubsub.PRLifecycleEvent{HeadSHA: "head"}, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, wantLifecycle: "merged"},
		{name: "merged rejected merge reset reloads lifecycle", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: ErrPRTransitionSkipped, wantErr: lifecycleSkipped, wantLifecycle: "merged", wantResets: []string{"c1:head:merge-event"}},
		{name: "merged failed merge reset retries", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, resetErr: errLifecycleDB, wantErr: "db error", wantLifecycle: "merged", wantResets: []string{"c1:head:merge-event"}},
		{name: "merged lifecycle status error propagates", event: "merged", ev: mergedHead, execNil: true, chunk: mergedChunk, statusErr: errLifecycleDB, wantErr: "db error", wantLifecycle: "merged"},
		{name: "merged with nil store and executor is a no-op", event: "merged", ev: mergedHead, execNil: true, storeNil: true},

		{name: "closed_unmerged fails chunk and leaves the wake to reconcile", event: "closed_unmerged", ev: pubsub.PRLifecycleEvent{Event: "closed_unmerged"}, wantFailed: []string{"c1:pr_closed_unmerged"}, wantCancel: "c1"},

		{name: "deploy_failed sets status banner and dispatches silent wake", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"}, wantBanners: []string{"c1:deploy_failed:Deployment failed for eri: crashloop"}, wantDispatch: true, wantSkipIfActive: &yes, wantDispatchPrompt: "Deployment failed for job eri on aerial (PR #101"},
		{name: "sync_failed sets status banner and dispatches silent wake", event: "sync_failed", ev: pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"}, wantBanners: []string{"c1:deploy_failed:Git sync failed: git pull conflict"}, wantDispatch: true, wantSkipIfActive: &yes, wantDispatchPrompt: "Git sync failed for PR #102 on aerial"},
		{name: "deploy_failed banner update error propagates", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: errLifecycleDB, wantErr: "db error", wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"}},
		{name: "deploy_failed on completed chunk skips wake prompt", event: "deploy_failed", ev: pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"}, bannerErr: ErrPRTransitionSkipped, wantErr: lifecycleSkipped, wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"}},
	}
}

func prEventCoalescedRunCases() []prEventCase {
	no := false
	return []prEventCase{
		{
			name:  "sibling pending job holds remediation",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "lint",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123/job/10",
				Details:   "lint failed",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"in_progress","conclusion":null}]}`,
					},
				},
			},
			wantChecks:  []string{"c1:abcdef1234:lint:failed"},
			wantBanners: []string{`c1:ci_failed:CI check "lint" failed`},
			wantWidget:  `CI check "lint" failed — waiting for remaining CI jobs to finish`,
		},
		{
			name:  "completed run with multiple failed jobs coalesces into single wake",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "lint",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123/job/10",
				Details:   "lint failure",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"completed","conclusion":"failure"}]}`,
					},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {
					{Status: http.StatusOK, Body: "lint syntax error"},
				},
				"GET /repos/example-owner/aerial/actions/jobs/11/logs": {
					{Status: http.StatusOK, Body: "test panic"},
				},
			},
			failCount:          1,
			wantChecks:         []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"},
			wantBanners:        []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`},
			wantFailCountCalls: 1,
			wantDispatch:       true,
			wantDispatchID:     "vanadis-c1-cifail-abcdef1234-2",
			wantSkipIfActive:   &no,
			wantDispatchPrompt: "lint, test",
			wantWidget:         `CI check "lint, test" failed — remediation in progress`,
		},
		{
			name:  "workflow run failure does not record workflow name in ci checks",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "Continuous Delivery",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123",
				Details:   "workflow concluded with failure",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":12,"name":"CI Gate","status":"completed","conclusion":"failure"}]}`,
					},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {
					{Status: http.StatusOK, Body: "lint syntax error"},
				},
			},
			failCount:          1,
			wantChecks:         []string{"c1:abcdef1234:lint:failed"},
			wantBanners:        []string{`c1:ci_failed:CI check "Continuous Delivery" failed`, `c1:ci_failed:CI check "lint" failed`},
			wantFailCountCalls: 1,
			wantDispatch:       true,
			wantDispatchID:     "vanadis-c1-cifail-abcdef1234-2",
			wantSkipIfActive:   &no,
			wantDispatchPrompt: "lint",
			wantWidget:         `CI check "lint" failed — remediation in progress`,
		},
	}
}

func prEventCoalescedCapAndErrCases() []prEventCase {
	capProgress := fmt.Sprintf("CI remediation cap (%d/%d) reached — inline thread remediation in progress", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	capPrompt := "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]"

	return []prEventCase{
		{
			name:  "ci_failed exceeding cap with run jobs marks chunk failed and alerts",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "lint",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123/job/10",
				Details:   "lint failure",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"},{"id":11,"name":"test","status":"completed","conclusion":"failure"}]}`,
					},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {
					{Status: http.StatusOK, Body: "lint syntax error"},
				},
				"GET /repos/example-owner/aerial/actions/jobs/11/logs": {
					{Status: http.StatusOK, Body: "test panic"},
				},
			},
			failCount:          5,
			wantChecks:         []string{"c1:abcdef1234:lint:failed", "c1:abcdef1234:test:failed"},
			wantBanners:        []string{`c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "lint, test" failed`},
			wantFailed:         []string{lifecycleCapFail},
			wantFailCountCalls: 1,
			wantCancel:         "c1",
			wantThreadID:       "th-1",
			wantPrompt:         capPrompt,
			wantWidget:         capProgress,
		},
		{
			name:  "coalesced check record error propagates",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "Continuous Delivery",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123",
				Details:   "workflow concluded with failure",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"}]}`,
					},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {
					{Status: http.StatusOK, Body: "lint syntax error"},
				},
			},
			checkErr:    errLifecycleDB,
			wantErr:     "db error",
			wantBanners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`},
		},
		{
			name:  "coalesced banner update error propagates",
			event: "ci_failed",
			ev: pubsub.PRLifecycleEvent{
				Event:     "ci_failed",
				CheckName: "Continuous Delivery",
				HeadSHA:   "abcdef1234",
				Repo:      "aerial",
				PRNumber:  42,
				CheckURL:  "https://github.com/example-owner/aerial/actions/runs/123",
				Details:   "workflow concluded with failure",
			},
			apiRoutes: map[string][]githubapitest.Response{
				"GET /repos/example-owner/aerial/actions/runs/123/jobs?filter=latest&per_page=100": {
					{
						Status: http.StatusOK,
						Body:   `{"jobs":[{"id":10,"name":"lint","status":"completed","conclusion":"failure"}]}`,
					},
				},
				"GET /repos/example-owner/aerial/actions/jobs/10/logs": {
					{Status: http.StatusOK, Body: "lint syntax error"},
				},
			},
			bannerErr:   errLifecycleDB,
			wantErr:     "db error",
			wantChecks:  []string{},
			wantBanners: []string{`c1:ci_failed:CI check "Continuous Delivery" failed`},
		},
	}
}

func prEventCoalescedCases() []prEventCase {
	return append(prEventCoalescedRunCases(), prEventCoalescedCapAndErrCases()...)
}

func prEventCases() []prEventCase {
	return append(append(prEventBasicCases(), prEventTerminalCases()...), prEventCoalescedCases()...)
}

func TestFlow_HandlePREventType(t *testing.T) {
	t.Parallel()
	for _, tc := range prEventCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := newPREventSpy(tc)
			cancelErr := tc.notifyErr
			if tc.cancelErr != nil {
				cancelErr = tc.cancelErr
			}
			widgetErr := tc.notifyErr
			if tc.widgetErr != nil {
				widgetErr = tc.widgetErr
			}
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true, CancelErr: cancelErr, MessageErr: tc.notifyErr, WidgetErr: widgetErr, PromptErr: tc.notifyErr}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, tc.notifyErr, tc.apiStatus, tc.apiBody, tc.apiRoutes)
			}
			f := NewFlow(st, exec)
			ctx := context.Background()

			err := f.HandlePREventType(ctx, "b1", "c1", tc.event, tc.ev)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(ctx, "b1", "c1", extra.Event, extra)
			}
			assertLifecycleErr(t, tc.wantErr, err)
			assertPREventStore(t, tc, spy)
			assertPREventEri(t, tc, eri)
		})
	}
}

func assertPREventStore(t *testing.T, tc prEventCase, spy *checkStatusSpyStore) {
	t.Helper()
	assert.Equal(t, tc.wantFailed, spy.failed)
	assert.Equal(t, tc.wantBanners, spy.banners)
	assert.Equal(t, tc.wantResets, spy.resets)
	if tc.wantChecks != nil {
		assert.ElementsMatch(t, tc.wantChecks, spy.calls)
	}
	assert.Equal(t, tc.wantLifecycle, spy.lifecycleStatus)
	assert.Len(t, spy.failCountCalls, tc.wantFailCountCalls)
	if tc.wantCIFailCount != 0 {
		assert.Equal(t, tc.wantCIFailCount, spy.chunk("c1").CIFailCount)
	}
	if tc.wantRemediatedSHA != "" {
		assert.Equal(t, tc.wantRemediatedSHA, spy.chunk("c1").RemediatedSHA)
	}
}

func assertPREventEri(t *testing.T, tc prEventCase, eri *eritest.Client) {
	t.Helper()
	if tc.wantPrompt != "" {
		assert.Contains(t, eri.LastPrompt, tc.wantPrompt)
	} else {
		assert.Empty(t, eri.LastPrompt)
	}
	assert.Equal(t, tc.wantThreadID, eri.LastThreadID)
	assert.Equal(t, tc.wantCancel, eri.LastCancel)
	if tc.agent != "" {
		assert.Equal(t, tc.agent, eri.CancelAgent)
	}
	assert.Equal(t, tc.wantMsgChan, eri.LastMsgChan)
	if tc.wantWidget != "" && assert.NotNil(t, eri.LastWidget) {
		assert.Equal(t, tc.wantWidget, eri.LastWidget.Banner)
	}
	if !tc.wantDispatch {
		assert.Nil(t, eri.LastDispatch)
		return
	}
	require.NotNil(t, eri.LastDispatch)
	assert.Equal(t, "vanadis-c1", eri.LastDispatch.SessionID)
	assert.Equal(t, "c1", eri.LastDispatch.VanadisChunkID)
	assert.Equal(t, "th-1", eri.LastDispatch.TargetID)
	assert.NotEmpty(t, eri.LastDispatch.ID)
	if tc.wantDispatchID != "" {
		assert.Equal(t, tc.wantDispatchID, eri.LastDispatch.ID)
	}
	if tc.wantSkipIfActive != nil {
		assert.Equal(t, *tc.wantSkipIfActive, eri.LastDispatch.SkipIfActive)
	}
	assert.Contains(t, eri.LastDispatch.Prompt, tc.wantDispatchPrompt)
}
