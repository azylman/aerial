package prflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type checkStatusSpyStore struct {
	store.VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []model.VanadisChunk
	nilBuild        bool
	lookup          *store.VanadisChunkPRLookup
	lookupErr       error
	calls           []string
	resets          []string
	failed          []string
	banners         []string
	attached        []string
	attachErr       error
	failCountCalls  []string
	failCount       int
	failCountErr    error
	err             error
	resetErr        error
	statusErr       error
	bannerErr       error
	failErr         error
	skipFailApply   bool
	failBanner      string
}

func (s *checkStatusSpyStore) chunk(chunkID string) *model.VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == chunkID {
			return &s.chunks[i]
		}
	}
	s.chunks = append(s.chunks, model.VanadisChunk{ID: chunkID, BuildID: "b1", Effort: effortMedium, CIFailCount: s.failCount})
	return &s.chunks[len(s.chunks)-1]
}

func (s *checkStatusSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	s.failCountCalls = append(s.failCountCalls, chunkID+":"+banner)
	if s.failCountErr != nil {
		return 0, s.failCountErr
	}
	s.failCount++
	c := s.chunk(chunkID)
	c.CIFailCount = s.failCount
	if len(headSHA) > 0 && strings.TrimSpace(headSHA[0]) != "" {
		c.RemediatedSHA = strings.TrimSpace(headSHA[0])
	}
	return s.failCount, nil
}

func (s *checkStatusSpyStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	if s.nilBuild {
		return nil, nil, nil
	}
	chunks := s.chunks
	if len(chunks) == 0 {
		chunks = []model.VanadisChunk{{ID: "c1", BuildID: buildID, Effort: effortMedium, CIFailCount: s.failCount}}
	}
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, chunks, nil
}

func (s *checkStatusSpyStore) RecordPRCheckStatus(_ context.Context, chunkID, headSHA, checkName, checkState string) error {
	s.calls = append(s.calls, chunkID+":"+headSHA+":"+checkName+":"+checkState)
	return s.err
}

func (s *checkStatusSpyStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, mergeSHA string) error {
	s.resets = append(s.resets, chunkID+":"+headSHA+":"+mergeSHA)
	if s.resetErr != nil {
		return s.resetErr
	}
	if sha := strings.TrimSpace(headSHA); sha != "" {
		s.chunk(chunkID).HeadSHA = sha
	}
	return nil
}

func (s *checkStatusSpyStore) UpdatePRLifecycleStatus(_ context.Context, _ string, status string) error {
	s.lifecycleStatus = status
	return s.statusErr
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	if s.failErr != nil {
		return nil, nil, false, s.failErr
	}
	c := s.chunk(chunkID)
	banner := c.Banner
	if s.failBanner != "" {
		banner = s.failBanner
	} else if !s.skipFailApply && banner == "" {
		banner = "Chunk 1 failed: " + reason
	}
	return &model.VanadisBuild{ID: "b1", Status: statusRunning, Objective: "Obj", ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed, Banner: banner}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	return true, nil
}

func (s *checkStatusSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if s.lookup != nil {
		return s.lookup, nil
	}
	return &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, nil
}

func (s *checkStatusSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attached = append(s.attached, req.ChunkID)
	return "b1", req.ChunkID, "th-1", nil
}

var (
	errLifecycleDB   = errors.New("db error")
	errLifecycleRead = errors.New("read unavailable")
)

const (
	lifecycleSkipped  = "pr lifecycle transition not applied"
	lifecycleCIBanner = `c1:ci_failed:CI check "unit-tests" failed`
	lifecycleCapFail  = "c1:ci_remediation_cap_reached"
	lifecycleCapText  = "Chunk 1 failed: ci_remediation_cap_reached"
	lifecycleConflict = "c1:conflict:Merge conflict detected"
)

func ciFailEv(checkName, headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: checkName, HeadSHA: headSHA, Repo: "aerial", PRNumber: 42, Details: "test failure"}
}

func conflictEv(headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: headSHA, Repo: "aerial", PRNumber: 42}
}

// newLifecycleExecutor builds the executor every Flow table uses: a recording
// eri fake whose cancel/message/widget calls and bus publishes all fail with
// notifyErr, and an optional scripted GitHub API.
func newLifecycleExecutor(t *testing.T, eri *eritest.Client, notifyErr error, apiStatus int, apiBody string) *executor.Executor {
	t.Helper()
	var bus pubsub.Bus
	if notifyErr != nil {
		bus = &pubsubtest.FaultBus{PublishErr: notifyErr}
	}
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil)
	if apiStatus != 0 {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(apiStatus)
			_, _ = w.Write([]byte(apiBody))
		}))
		t.Cleanup(api.Close)
		exec.GitHubAPIURL = api.URL
	}
	return exec
}

func assertLifecycleErr(t *testing.T, want string, err error) {
	t.Helper()
	if want == "" {
		assert.NoError(t, err)
		return
	}
	assert.ErrorContains(t, err, want)
}

type prEventCase struct {
	name        string
	event       string
	ev          pubsub.PRLifecycleEvent
	extraEvents []pubsub.PRLifecycleEvent

	execNil   bool
	storeNil  bool
	apiStatus int
	apiBody   string
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
}

func newPREventSpy(tc prEventCase) *checkStatusSpyStore {
	spy := &checkStatusSpyStore{
		getBuildErr: tc.getBuildErr, bannerErr: tc.bannerErr, failErr: tc.failErr, failCount: tc.failCount, failCountErr: tc.failCountErr,
		skipFailApply: tc.skipFailApply, failBanner: tc.failBanner, err: tc.checkErr, resetErr: tc.resetErr, statusErr: tc.statusErr,
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

func TestFlow_HandlePREventType(t *testing.T) {
	t.Parallel()
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
	merged := pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}
	mergedHead := pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge-event"}
	mergedChunk := &model.VanadisChunk{Status: "merged", HeadSHA: "head"}
	capChunk := &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "abcdef1234"}
	headAPI := `{"state":"open","merged":false,"head":{"sha":"sha-2"}}`

	tests := []prEventCase{
		{name: "opened resets check state", event: "opened", ev: pubsub.PRLifecycleEvent{Event: "opened"}, execNil: true, wantResets: []string{"c1::"}},
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
		{name: "ci_failed exceeding cap marks chunk failed and sends alert message", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed tripping cap increments fail count without overwriting RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 5, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"}, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantRemediatedSHA: "attempt5-active-sha", wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed retry with failCount 6 skips increment and completes cap reached", event: "ci_failed", ev: ciFailEv("unit-tests", "new-failing-sha"), failCount: 6, chunk: &model.VanadisChunk{RemediatedSHA: "attempt5-active-sha"}, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantRemediatedSHA: "attempt5-active-sha", wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "trailing same-SHA failure at cap neither increments, fails nor cancels the active remediation", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailing, failCount: 4, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 1, wantCIFailCount: 5, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-5", wantSkipIfActive: &no},
		{name: "new head after trailing same-SHA failure trips cap and keeps RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: trailingThenNewHead, failCount: 4, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 2, wantCIFailCount: 6, wantDispatch: true, wantRemediatedSHA: "abcdef1234", wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "retry of the cap-tripping head does not increment again", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), extraEvents: newHeadRetry, failCount: 4, wantFailed: []string{lifecycleCapFail, lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner, lifecycleCIBanner}, wantFailCountCalls: 2, wantCIFailCount: 6, wantDispatch: true, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed record check skipped on already failed chunk resumes cap reached notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), checkErr: ErrPRTransitionSkipped, chunk: capChunk, skipFailApply: true, failBanner: lifecycleCapText, wantFailed: []string{lifecycleCapFail}, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed update banner skipped on already failed chunk resumes cap reached notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), bannerErr: ErrPRTransitionSkipped, chunk: capChunk, skipFailApply: true, failBanner: lifecycleCapText, wantBanners: []string{lifecycleCIBanner}, wantFailed: []string{lifecycleCapFail}, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed record check skipped with stale head does not re-notify", event: "ci_failed", ev: ciFailEv("unit-tests", "stale-sha"), checkErr: ErrPRTransitionSkipped, chunk: &model.VanadisChunk{Status: "failed", Banner: lifecycleCapText, HeadSHA: "current-sha"}, wantErr: lifecycleSkipped},
		{name: "ci_failed record check skipped with lookup error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), checkErr: ErrPRTransitionSkipped, getBuildErr: errLifecycleDB, wantErr: "db error"},
		{name: "ci_failed exceeding cap preserves chunk failure and propagates notification error", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, notifyErr: errLifecycleDB, wantErr: "db error", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap retry with matching banner completes notifications", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, failBanner: lifecycleCapText, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap fail chunk error propagates", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failErr: errLifecycleDB, wantErr: "db error", wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1},
		{name: "ci_failed at cap with unapplied fail chunk skips cap without cancelling", event: "ci_failed", ev: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, wantFailed: []string{lifecycleCapFail}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1},
		{name: "ci_failed on new head_sha dispatches with SkipIfActive false and records RemediatedSHA", event: "ci_failed", ev: ciFailEv("unit-tests", "new-head-sha"), chunk: &model.VanadisChunk{RemediatedSHA: "old-head-sha"}, wantBanners: []string{lifecycleCIBanner}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-new-head-sha-1", wantSkipIfActive: &no, wantRemediatedSHA: "new-head-sha"},

		{name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", ev: conflictEv("abcdef1234"), wantBanners: []string{lifecycleConflict}, wantDispatch: true, wantSkipIfActive: &yes},
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := newPREventSpy(tc)
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, SimulateActive: true, CancelErr: tc.notifyErr, MessageErr: tc.notifyErr, WidgetErr: tc.notifyErr}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, tc.notifyErr, tc.apiStatus, tc.apiBody)
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
	assert.Empty(t, eri.LastPrompt)
	assert.Equal(t, tc.wantCancel, eri.LastCancel)
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

func TestFlow_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		event     string
		ev        pubsub.PRLifecycleEvent
		storeNil  bool
		err       error
		wantErr   string
		wantCalls []string
	}{
		{name: "empty check name is ignored", event: "check_updated", ev: pubsub.PRLifecycleEvent{CheckName: ""}},
		{name: "empty check state on check_updated is ignored", event: "check_updated", ev: pubsub.PRLifecycleEvent{CheckName: "test"}},
		{name: "check_updated records exact state", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "running"}, wantCalls: []string{"c1:sha-1:test:running"}},
		{name: "check_failed defaults empty state to failed", event: "check_failed", ev: pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}, wantCalls: []string{"c1:sha-1:test:failed"}},
		{name: "store error propagates", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "test", CheckState: "passed"}, err: errLifecycleDB, wantErr: "db error", wantCalls: []string{"c1:merge-sha-3:test:passed"}},
		{name: "nil store returns nil", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "head", CheckName: "lint", CheckState: "passed"}, storeNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{err: tc.err}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			assertLifecycleErr(t, tc.wantErr, NewFlow(st, nil).HandlePRCheckEvent(context.Background(), "c1", tc.event, tc.ev))
			assert.Equal(t, tc.wantCalls, spy.calls)
		})
	}
}

func TestFlow_LocateOrAttachPRChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		lookup    *store.VanadisChunkPRLookup
		lookupErr error
		wantChunk string
		wantErr   string
	}{
		{name: "matched lookup returns chunk", wantChunk: "c1"},
		{name: "own build chunk", lookup: &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, wantChunk: "c1"},
		{name: "blank owner keeps chunk", lookup: &store.VanadisChunkPRLookup{ChunkID: "c1", Matched: true}, wantChunk: "c1"},
		{name: "foreign build chunk is never acted on", lookup: &store.VanadisChunkPRLookup{ChunkID: "c9", BuildID: "b2", Matched: true}},
		{name: "unmatched lookup without executor resolves nothing", lookup: &store.VanadisChunkPRLookup{Matched: false}},
		{name: "lookup error propagates", lookupErr: errLifecycleRead, wantErr: "read unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := NewFlow(&checkStatusSpyStore{lookup: tc.lookup, lookupErr: tc.lookupErr}, nil)
			got, err := f.LocateOrAttachPRChunk(context.Background(), "b1", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12})
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantChunk, got)
		})
	}
}

func TestFlow_SyncAttachedPRWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		execNil     bool
		getBuildErr error
		widgetErr   error
		wantWidget  bool
	}{
		{name: "nil executor skips widget", execNil: true},
		{name: "build read error skips widget", getBuildErr: errLifecycleRead},
		{name: "attached build syncs widget", wantWidget: true},
		{name: "widget error is swallowed", widgetErr: errors.New("widget unavailable"), wantWidget: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{WidgetErr: tc.widgetErr}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			NewFlow(&checkStatusSpyStore{getBuildErr: tc.getBuildErr}, exec).SyncAttachedPRWidget(context.Background(), "b1")
			assert.Equal(t, tc.wantWidget, eri.LastWidget != nil)
		})
	}
}

func TestFlow_HandleClosedPREvent(t *testing.T) {
	t.Parallel()
	unmerged := pubsub.PRLifecycleEvent{Event: "closed_unmerged"}
	failed := []string{"c1:pr_closed_unmerged"}
	tests := []struct {
		name          string
		ev            pubsub.PRLifecycleEvent
		failErr       error
		getBuildErr   error
		skipFailApply bool
		failBanner    string
		cancelErr     error
		promptErr     error
		widgetErr     error
		busErr        error
		wantErr       string
		wantFailed    []string
		wantCancel    string
	}{
		{name: "closed unmerged fails chunk and leaves the wake to reconcile", ev: pubsub.PRLifecycleEvent{Event: "closed", Status: "closed"}, wantFailed: failed, wantCancel: "c1"},
		{name: "closed with status merged skips fail chunk", ev: pubsub.PRLifecycleEvent{Event: "closed", Status: "merged"}},
		{name: "already failed chunk skips notifications", ev: unmerged, skipFailApply: true, wantFailed: failed},
		{name: "retry with matching banner completes notifications", ev: unmerged, skipFailApply: true, failBanner: "Chunk 1 failed: pr_closed_unmerged", wantFailed: failed, wantCancel: "c1"},
		{name: "fail chunk error propagates", ev: unmerged, failErr: errLifecycleDB, wantErr: "db error", wantFailed: failed},
		{name: "cancel error swallowed", ev: unmerged, cancelErr: errors.New("cancel unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "read error propagates", ev: unmerged, getBuildErr: errLifecycleRead, wantErr: "read unavailable", wantFailed: failed, wantCancel: "c1"},
		{name: "widget error swallowed", ev: unmerged, widgetErr: errors.New("widget unavailable"), busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "wake left to reconcile", ev: unmerged, promptErr: errors.New("prompt unavailable"), busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "publish errors swallowed", ev: unmerged, busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, failErr: tc.failErr, skipFailApply: tc.skipFailApply, failBanner: tc.failBanner}
			eri := &eritest.Client{CancelErr: tc.cancelErr, PromptErr: tc.promptErr, WidgetErr: tc.widgetErr}
			var bus pubsub.Bus
			if tc.busErr != nil {
				bus = &pubsubtest.FaultBus{PublishErr: tc.busErr}
			}
			f := NewFlow(spy, executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil))
			err := f.HandleClosedPREvent(context.Background(), "c1", tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantCancel, eri.LastCancel)
			assert.Empty(t, eri.LastPrompt)
		})
	}
}

func TestFlow_HandleDeployFailureEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		details        string
		execNil        bool
		storeNil       bool
		getBuildErr    error
		nilBuild       bool
		dispatchErr    error
		wantErr        string
		wantNotFound   bool
		wantDispatch   bool
		wantDispatchID string
	}{
		{name: "success dispatches silent wake", wantDispatch: true},
		{name: "nil executor skips silent wake", execNil: true},
		{name: "nil store skips banner and silent wake", storeNil: true},
		{name: "read error propagates", getBuildErr: errLifecycleDB, wantErr: "db error"},
		{name: "missing build reports not found", nilBuild: true, wantErr: "not found", wantNotFound: true},
		{name: "silent wake error propagates", dispatchErr: errLifecycleDB, wantErr: "db error", wantDispatch: true},
		{name: "active dispatch treated as handled", dispatchErr: eriwire.ErrDispatchActive, wantDispatch: true},
		// Redeliveries of one occurrence reuse the ID; a distinct failure gets its own.
		{name: "first occurrence dispatch id is stable across redelivery", details: "crashloop-1", wantDispatch: true, wantDispatchID: "vanadis-deploy-fail-c1-6036eca51e3809fa"},
		{name: "distinct occurrence gets a distinct dispatch id", details: "crashloop-2", wantDispatch: true, wantDispatchID: "vanadis-deploy-fail-c1-86465ab6c9979e95"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, nilBuild: tc.nilBuild}
			eri := &eritest.Client{DispatchErr: tc.dispatchErr}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			err := NewFlow(st, exec).HandleDeployFailureEvent(context.Background(), "b1", "c1", pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse", Details: tc.details})
			assertLifecycleErr(t, tc.wantErr, err)
			if tc.wantNotFound {
				assert.ErrorIs(t, err, store.ErrBuildNotFound)
			}
			assert.Empty(t, eri.LastPrompt)
			if !tc.wantDispatch {
				assert.Nil(t, eri.LastDispatch)
				return
			}
			require.NotNil(t, eri.LastDispatch)
			assert.Contains(t, eri.LastDispatch.Prompt, "Deployment failed")
			if tc.wantDispatchID != "" {
				assert.Equal(t, tc.wantDispatchID, eri.LastDispatch.ID)
			}
		})
	}
}

func TestFlow_NoPRTurnAction(t *testing.T) {
	t.Parallel()
	pr := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "pr", Status: status, PRNumber: prNumber}
	}
	act := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "action", Status: status, PRNumber: prNumber, StallNudges: 1}
	}
	tests := []struct {
		name        string
		chunk       model.VanadisChunk
		buildStatus string
		dispatchID  string
		want        NoPRAction
	}{
		{"running pr chunk launch turn resumes", pr("running", 0), "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"coding pr chunk launch turn resumes", pr("coding", 0), "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"legacy empty mode launch turn resumes", model.VanadisChunk{ID: "c1", Status: "running"}, "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"resume turn without pr fails", pr("running", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionFail},
		{"attached pr ignored", pr("running", 12), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"pr opened status ignored", pr("pr_opened", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"failed chunk ignored", pr("failed", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionNone},
		{"cancelled chunk ignored", pr("cancelled", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"failed build resumes", pr("running", 0), "failed", "vanadis-c1-turn-0", NoPRActionResume},
		{"cancelled build ignored", pr("running", 0), "cancelled", "vanadis-c1-nopr-resume-1", NoPRActionNone},
		{"ci remediation dispatch ignored", pr("running", 0), "running", "vanadis-c1-cifail-abc123-1", NoPRActionNone},
		{"conflict remediation dispatch ignored", pr("running", 0), "running", "vanadis-conflict-c1-abc123", NoPRActionNone},
		{"other chunk resume id ignored", pr("running", 0), "running", "vanadis-c2-nopr-resume-1", NoPRActionNone},
		{"action launch turn resumes", act("running", 0), "running", "vanadis-c1-turn-1", NoPRActionResume},
		{"action resume below cap resumes", act("running", 0), "running", "vanadis-c1-turn-1-resume-11", NoPRActionResume},
		{"action last resume fails", act("running", 0), "running", "vanadis-c1-turn-1-resume-12", NoPRActionFailAction},
		{"action repair pr opened resumes", act("pr_opened", 7), "running", "vanadis-c1-turn-1", NoPRActionResume},
		{"action repair deploying resumes", act("deploying", 7), "running", "vanadis-c1-turn-1-resume-2", NoPRActionResume},
		{"action repair deploy failed at cap fails", act("deploy_failed", 7), "running", "vanadis-c1-turn-1-resume-12", NoPRActionFailAction},
		{"action pr status without pr ignored", act("pr_opened", 0), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"completed action ignored", act("completed", 0), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"failed action ignored", act("failed", 0), "running", "vanadis-c1-turn-1-resume-12", NoPRActionNone},
		{"completed action with pr ignored", act("completed", 7), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"action on failed build resumes", act("running", 0), "failed", "vanadis-c1-turn-1", NoPRActionResume},
		{"action stale generation ignored", act("running", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"action ci remediation ignored", act("running", 0), "running", "vanadis-c1-cifail-abc-1", NoPRActionNone},
		{"action no-pr resume id ignored", act("running", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, NoPRTurnAction(tc.chunk, tc.buildStatus, tc.dispatchID))
		})
	}
}

func TestFlow_FindChunkIndex(t *testing.T) {
	t.Parallel()
	present := []model.VanadisChunk{{ID: "c1"}, {ID: "c2"}}
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		id     string
		want   int
	}{
		{"first match", present, "c1", 0},
		{"later match", present, "c2", 1},
		{"missing chunk", present, "c9", -1},
		{"empty list", nil, "c1", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, FindChunkIndex(tc.chunks, tc.id))
		})
	}
}

func TestFlow_SelectUnattachedTargetChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	running := func(id string) model.VanadisChunk {
		return model.VanadisChunk{ID: id, BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now}
	}
	build := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running"}
	threadEv := pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12}
	tests := []struct {
		name   string
		build  *model.VanadisBuild
		chunks []model.VanadisChunk
		ev     pubsub.PRLifecycleEvent
		want   string
	}{
		{"nil build matches nothing", nil, []model.VanadisChunk{running("c1")}, threadEv, ""},
		{"branch-encoded chunk matches", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", TargetID: "th-1"}, "c1"},
		{"branch-encoded attached chunk rejected", build, []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, PRNumber: 12}}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", TargetID: "th-1"}, ""},
		{"branch-encoded unknown chunk rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c9-123456", TargetID: "th-1"}, ""},
		{"thread fallback single match", build, []model.VanadisChunk{running("c1")}, threadEv, "c1"},
		{"thread fallback ambiguous match rejected", build, []model.VanadisChunk{running("c1"), running("c2")}, threadEv, ""},
		{"thread mismatch rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{TargetID: "other", Repo: "owner/repo", PRNumber: 12}, ""},
		{"quiet event rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12, Quiet: true}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SelectUnattachedTargetChunk("example-owner", tc.build, tc.chunks, tc.ev)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.ID)
		})
	}
}

func TestFlow_ResolveOrAttachPRChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	newChunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, Effort: "medium"}
	otherChunk := model.VanadisChunk{ID: "c2", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, Effort: "medium"}
	branch := "feat/repo-pr-vanadis-c1-123456"
	trailer := `notes <!-- aerial-pr:{"target_id":"th-1","vanadis_chunk_id":"c1"} -->`
	validBody := fmt.Sprintf(`{"state":"open","merged":false,"body":%q,"user":{"login":"owner"},"author_association":"MEMBER","head":{"sha":"deadbee","ref":%q,"repo":{"fork":false,"full_name":"owner/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`,
		trailer, branch)
	attachedEv := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Branch: branch, HeadSHA: "deadbee", TargetID: "th-1"}

	tests := []struct {
		name         string
		execNil      bool
		apiStatus    int
		apiBody      string
		chunks       []model.VanadisChunk
		attachErr    error
		getBuildErr  error
		widgetErr    bool
		event        pubsub.PRLifecycleEvent
		wantChunk    string
		wantErr      bool
		wantAttached []string
	}{
		{name: "nil executor resolves nothing", execNil: true, chunks: []model.VanadisChunk{newChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}},
		{name: "valid attachment attaches chunk and syncs widget", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, event: attachedEv, wantChunk: "c1", wantAttached: []string{"c1"}},
		{name: "branch mismatch skips without attaching", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Branch: "other", HeadSHA: "deadbee", TargetID: "th-1"}},
		{name: "build not found resolves nothing", chunks: []model.VanadisChunk{newChunk}, getBuildErr: store.ErrBuildNotFound, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}},
		{name: "read error propagates", chunks: []model.VanadisChunk{newChunk}, getBuildErr: errors.New("read unavailable"), event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}, wantErr: true},
		{name: "attach conflict resolves nothing", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, attachErr: store.ErrBuildConflict, event: attachedEv},
		{name: "ambiguous thread match resolves nothing", chunks: []model.VanadisChunk{newChunk, otherChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, TargetID: "th-1"}},
		{name: "attachment validation transport error propagates", apiStatus: http.StatusInternalServerError, apiBody: "{}", chunks: []model.VanadisChunk{newChunk}, event: attachedEv, wantErr: true},
		{name: "attach transport error propagates", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, attachErr: errors.New("attach unavailable"), event: attachedEv, wantErr: true},
		{name: "widget error after attach is swallowed", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, widgetErr: true, event: attachedEv, wantChunk: "c1", wantAttached: []string{"c1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.apiStatus)
				_, _ = w.Write([]byte(tc.apiBody))
			}))
			defer api.Close()
			eri := &eritest.Client{}
			if tc.widgetErr {
				eri.WidgetErr = errors.New("widget unavailable")
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
			exec.GitHubAPIURL = api.URL
			spy := &checkStatusSpyStore{chunks: tc.chunks, attachErr: tc.attachErr, getBuildErr: tc.getBuildErr}
			ex := exec
			if tc.execNil {
				ex = nil
			}
			f := NewFlow(spy, ex)
			got, err := f.ResolveOrAttachPRChunk(context.Background(), "b1", tc.event)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantChunk, got)
			assert.Equal(t, tc.wantAttached, spy.attached)
		})
	}
}

func TestFlow_ResumeNoPRChunk(t *testing.T) {
	t.Parallel()
	bld := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Title: "Build", Objective: "Objective", Status: "running"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Status: "running", Repo: "aerial", Title: "Chunk 1", Instructions: "Do it"}}

	tests := []struct {
		name        string
		dispatchErr bool
		widgetErr   bool
		wantErr     bool
	}{
		{name: "resume dispatch succeeds", wantErr: false},
		{name: "dispatch error propagates", dispatchErr: true, wantErr: true},
		{name: "widget error swallowed", widgetErr: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{}
			eri := &eritest.Client{}
			if tc.dispatchErr {
				eri.DispatchErr = errors.New("dispatch unavailable")
			}
			if tc.widgetErr {
				eri.WidgetErr = errors.New("widget unavailable")
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
			f := NewFlow(spy, exec)
			err := f.ResumeNoPRChunk(context.Background(), bld, chunks, 0)
			assert.Equal(t, tc.wantErr, err != nil)
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, "vanadis-c1-nopr-resume-1", eri.LastDispatch.ID)
			assert.False(t, eri.LastDispatch.SkipIfActive)
		})
	}
}

func TestFlow_NextActionResume(t *testing.T) {
	t.Parallel()
	chunk := model.VanadisChunk{ID: "c1", Mode: "action", StallNudges: 2}
	tests := []struct {
		name       string
		dispatchID string
		wantK      int
		wantOK     bool
	}{
		{"launch turn yields first resume", "vanadis-c1-turn-2", 1, true},
		{"resume eleven yields last resume", "vanadis-c1-turn-2-resume-11", 12, true},
		{"last resume yields none", "vanadis-c1-turn-2-resume-12", 0, false},
		{"stale generation yields none", "vanadis-c1-turn-1", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, ok := NextActionResume(chunk, tc.dispatchID)
			assert.Equal(t, tc.wantK, k)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestFlow_ResumeActionChunk(t *testing.T) {
	t.Parallel()
	bld := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Title: "Build", Objective: "Objective", Status: "running"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "action", Status: "running", StallNudges: 1, Title: "Chunk 1", Instructions: "Verify it live"}}
	tests := []struct {
		name         string
		k            int
		dispatchErr  error
		widgetErr    error
		wantErr      bool
		wantID       string
		wantPrefix   string
		wantBanner   string
		wantNoWidget bool
	}{
		{name: "first resume dispatched", k: 1, wantID: "vanadis-c1-turn-1-resume-1", wantPrefix: "[VANADIS AUTOMATIC RESUME 1/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 1/12 dispatched"},
		{name: "last resume dispatched", k: 12, wantID: "vanadis-c1-turn-1-resume-12", wantPrefix: "[VANADIS AUTOMATIC RESUME 12/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 12/12 dispatched"},
		{name: "dispatch error propagates", k: 3, dispatchErr: errors.New("dispatch unavailable"), wantErr: true, wantID: "vanadis-c1-turn-1-resume-3", wantPrefix: "[VANADIS AUTOMATIC RESUME 3/12]\n", wantNoWidget: true},
		{name: "widget error swallowed", k: 2, widgetErr: errors.New("widget unavailable"), wantID: "vanadis-c1-turn-1-resume-2", wantPrefix: "[VANADIS AUTOMATIC RESUME 2/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 2/12 dispatched"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, WidgetErr: tc.widgetErr, WidgetID: "msg-1"}
			f := NewFlow(&checkStatusSpyStore{}, executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil))
			err := f.ResumeActionChunk(context.Background(), bld, chunks, 0, tc.k)
			assert.Equal(t, tc.wantErr, err != nil)
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, tc.wantID, eri.LastDispatch.ID)
			assert.Equal(t, "vanadis-c1", eri.LastDispatch.SessionID)
			assert.False(t, eri.LastDispatch.SkipIfActive)
			assert.True(t, strings.HasPrefix(eri.LastDispatch.Prompt, tc.wantPrefix), eri.LastDispatch.Prompt)
			assert.Contains(t, eri.LastDispatch.Prompt, "Verify it live")
			if tc.wantNoWidget {
				assert.Nil(t, eri.LastWidget)
				return
			}
			require.NotNil(t, eri.LastWidget)
			assert.Contains(t, eri.LastWidget.Banner, tc.wantBanner)
		})
	}
}

func TestFlow_NotifyChunkFailure_Guards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		nilFlow bool
		nilExec bool
	}{
		{name: "nil flow skips failure notification", nilFlow: true},
		{name: "nil executor skips failure notification", nilExec: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f *Flow
			if !tc.nilFlow {
				var exec *executor.Executor
				if !tc.nilExec {
					exec = executor.NewExecutor(t.TempDir(), &eritest.Client{}, nil, "token", "example-owner", nil)
				}
				f = NewFlow(&checkStatusSpyStore{}, exec)
			}
			err := f.NotifyChunkFailure(context.Background(), model.VanadisBuild{ID: "b1"}, "c1", "boom")
			assert.NoError(t, err)
		})
	}
}

func TestFlow_IsEligibleUnattachedChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	eligible := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now}
	build := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running"}
	ev := pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12}
	tests := []struct {
		name  string
		build *model.VanadisBuild
		chunk *model.VanadisChunk
		event pubsub.PRLifecycleEvent
		want  bool
	}{
		{name: "nil build rejected", chunk: &eligible, event: ev},
		{name: "nil chunk rejected", build: build, event: ev},
		{name: "non-pr mode rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StartedAt: now}, event: ev},
		{name: "attached chunk rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", StartedAt: now, PRNumber: 12}, event: ev},
		{name: "unstarted chunk rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running"}, event: ev},
		{name: "failed status rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed", StartedAt: now}, event: ev},
		{name: "thread mismatch rejected", build: build, chunk: &eligible, event: pubsub.PRLifecycleEvent{TargetID: "other", Repo: "owner/repo", PRNumber: 12}},
		{name: "eligible chunk accepted", build: build, chunk: &eligible, event: ev, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsEligibleUnattachedChunk("example-owner", tc.build, tc.chunk, tc.event))
		})
	}
}

func TestChunkFailureNotice(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{{ID: "c1", ChunkIndex: 0}, {ID: "c2", ChunkIndex: 1}}
	tests := []struct {
		name, chunkID, want string
	}{
		{"indexed chunk", "c2", "Chunk 2 failed: boom (other chunks continue)"},
		{"unknown chunk", "c9", "Chunk failed: boom (other chunks continue)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, chunkFailureNotice(chunks, tc.chunkID, "boom"))
		})
	}
}
