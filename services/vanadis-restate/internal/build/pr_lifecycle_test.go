package build

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type checkStatusSpyStore struct {
	store.VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []model.VanadisChunk
	calls           []string
	resets          []string
	failed          []string
	banners         []string
	failCountCalls  []string
	failCount       int
	failCountErr    error
	err             error
	resetErr        error
	statusErr       error
	bannerErr       error
	failErr         error
	skipFailApply   bool
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

func TestBuildObject_MergeRevisionAfterLifecycle(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("reset unavailable")
	tests := []struct {
		name, mergeSHA    string
		resetErr, wantErr error
		wantResets        []string
	}{
		{"missing optional merge SHA", "", errPRTransitionSkipped, nil, nil},
		{"rejected merge reset reloads lifecycle", "merge-event", errPRTransitionSkipped, errPRTransitionSkipped, []string{"c1:head:merge-event"}},
		{"failed merge reset retries", "merge-event", unavailable, unavailable, []string{"c1:head:merge-event"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &checkStatusSpyStore{resetErr: tc.resetErr, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			ctx := mockRunContext{Context: context.Background()}
			err := bo.handlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: tc.mergeSHA})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("merge error=%v want=%v", err, tc.wantErr)
			}
			assert.Equal(t, "merged", st.lifecycleStatus)
			assert.Equal(t, tc.wantResets, st.resets)
		})
	}
}

func TestBuildObject_RejectedCheckFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{{"skipped write", errPRTransitionSkipped}, {"failed write", errors.New("database unavailable")}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &checkStatusSpyStore{err: tc.err, getBuildErr: errors.New("read unavailable")}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			err := bo.handlePREventType(mockRunContext{Context: context.Background()}, "b1", "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"})
			if !errors.Is(err, tc.err) || st.lifecycleStatus != "" {
				t.Fatalf("error=%v lifecycle=%s", err, st.lifecycleStatus)
			}
		})
	}
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	if s.failErr != nil {
		return nil, nil, false, s.failErr
	}
	return &model.VanadisBuild{ID: "b1", Status: statusFailed, Objective: "Obj", ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return nil
}

func (*checkStatusSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	return &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, nil
}

func TestBuildObject_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	var currentHead atomic.Value
	currentHead.Store("sha-2")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"state":"open","merged":false,"head":{"sha":%q}}`, currentHead.Load())
	}))
	defer api.Close()
	exec := executor.NewExecutor("", nil, nil, "", api.Client())
	exec.GitHubAPIURL = api.URL
	bo := NewBuildObject("", nil).WithAuthority(spy, exec)

	// synchronize validates head against GitHub API and resets check state in store
	t2 := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2", OccurredAt: t2}))
	assert.Equal(t, []string{"c1:sha-2:"}, spy.resets)

	// synchronize for superseded sha-1 rejected by GitHub API head validation without calling store reset
	spy.resets = nil
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1"}), errPRTransitionSkipped)
	assert.Empty(t, spy.resets)

	var parsedChunk model.VanadisChunk
	store.ApplyRawChunkCIChecks(&parsedChunk, []byte(`{"_head_sha":"sha-1","_merge_sha":"m-1","_remediated_sha":"sha-1","_head_at":"1700000000000000000","_seen_heads":"sha-0,sha-1","lint":"passed"}`))
	assert.Equal(t, "sha-1", parsedChunk.HeadSHA)
	assert.Equal(t, "m-1", parsedChunk.MergeSHA)
	assert.Equal(t, "sha-1", parsedChunk.RemediatedSHA)
	assert.Equal(t, map[string]string{"lint": "passed"}, parsedChunk.CIChecks)

	// merged updates lifecycle status and resets check state for merge commit
	spy.calls = nil
	spy.resets = nil
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", MergeSHA: "merge-sha-3"}))
	assert.Equal(t, []string{"c1:sha-2:merge-sha-3"}, spy.resets)
}

type mockBusError struct {
	pubsub.Bus

	err error
}

func (m *mockBusError) Publish(context.Context, string, string, any) error {
	return m.err
}

func TestBuildObject_ExecutorErrorBranches(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-err", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-err", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	st, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	eriErr := &mockEriClient{
		cancelFn: func(context.Context, string) error { return errors.New("cancel err") },
		promptFn: func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
			return "", errors.New("widget err")
		},
		messageFn: func(context.Context, string, string) error { return errors.New("msg err") },
	}
	mockBusErr := &mockBusError{err: errors.New("pub err")}
	execErr := executor.NewExecutor(t.TempDir(), eriErr, mockBusErr, "token", nil)
	boErr := NewBuildObject("", nil).WithAuthority(st, execErr)

	// notifyBuildTerminal error branches
	assert.Error(t, boErr.notifyBuildTerminal(context.Background(), &b1, []model.VanadisChunk{c1}, "failed", "chunk failed", "TITLE", "MSG"))

	// notifyChunkFailure error branches
	ctx := makeObjectMockContext(t, "b-err")
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		assert.Error(t, boErr.notifyChunkFailure(rc, b1, "c1", "reason"))
		return true, nil
	})

	// persistBuildCreation error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		_, _ = boErr.persistBuildCreation(rc, b1, []model.VanadisChunk{c1})
		return true, nil
	})

	// handlePREventType error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.handlePREventType(rc, "b-err", "c1", "check_failed", pubsub.PRLifecycleEvent{Event: "check_failed", CheckName: "unit-tests", HeadSHA: "head1", Repo: "aerial"})
		boErr.handlePREventType(rc, "b-err", "c1", "conflict", pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "head1"})
		boErr.handlePREventType(rc, "b-err", "c1", "merged", pubsub.PRLifecycleEvent{Event: "merged"})
		return true, nil
	})

	storeErr, _ := dbScript{bare: nilDB}.open()
	boStoreErr := NewBuildObject("", nil).WithAuthority(storeErr, execErr)
	for _, act := range []string{"chunk_merged", "chunk_deploy_success", "action_completed", "chunk_failed", "cancelled"} {
		boStoreErr.applyMutation(context.Background(), "b-err", &BuildMutationRequest{Action: act, ChunkID: "c1", Reason: "test"})
	}
	_, _ = boStoreErr.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = boErr.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		assert.Error(t, boStoreErr.notifyChunkFailure(rc, b1, "c1", "reason"))
		return true, nil
	})
	_ = boStoreErr.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", RolloutState: &pubsub.PRRolloutState{}})
}

func newRemediationTestHarness(t *testing.T, spy *checkStatusSpyStore, dispatchErr, notifyErr error) (*BuildObject, *mockEriClient) {
	t.Helper()
	eri := &mockEriClient{}
	var active bool
	var activeDispatch *executor.BrainDispatchRequest
	eri.dispatchFn = func(_ context.Context, req executor.BrainDispatchRequest) error {
		if dispatchErr != nil {
			return dispatchErr
		}
		if req.SkipIfActive && active {
			eri.lastDispatch = activeDispatch
			return executor.ErrDispatchActive
		}
		active = true
		activeDispatch = &req
		return nil
	}
	var bus pubsub.Bus
	if notifyErr != nil {
		eri.cancelFn = func(context.Context, string) error { return notifyErr }
		eri.messageFn = func(context.Context, string, string) error { return notifyErr }
		eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) { return "", notifyErr }
		bus = &mockBusError{err: notifyErr}
	}
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(spy, exec)
	return bo, eri
}

func ciFailEv(checkName, headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: checkName, HeadSHA: headSHA, Repo: "aerial", PRNumber: 42, Details: "test failure"}
}

func TestBuildObject_OnPREvent_RemediationDeferralAndRetry(t *testing.T) {
	t.Parallel()
	transportErr := errors.New("eri unavailable")

	tests := []struct {
		name             string
		ev               pubsub.PRLifecycleEvent
		remediatedSHA    string
		firstDispatchErr error
		wantErr          bool
	}{
		{
			name:             "ci_failed skipped_active ignores active worker without failing or incrementing budget",
			ev:               ciFailEv("unit-tests", "abcdef1234"),
			remediatedSHA:    "abcdef1234",
			firstDispatchErr: executor.ErrDispatchActive,
			wantErr:          false,
		},
		{
			name:             "conflict skipped_active ignores active worker without failing",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: executor.ErrDispatchActive,
			wantErr:          false,
		},
		{
			name:             "ci_failed transport error fails OnPREvent without consuming budget",
			ev:               ciFailEv("unit-tests", "abcdef1234"),
			remediatedSHA:    "abcdef1234",
			firstDispatchErr: transportErr,
			wantErr:          true,
		},
		{
			name:             "conflict transport error fails OnPREvent",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: transportErr,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				chunks: []model.VanadisChunk{{
					ID:            "c1",
					BuildID:       "b1",
					Status:        statusRunning,
					PRNumber:      42,
					Effort:        effortMedium,
					RemediatedSHA: tc.remediatedSHA,
				}},
			}
			bo, eri := newRemediationTestHarness(t, spy, tc.firstDispatchErr, nil)

			ctx := makeObjectMockContext(t, "b1")
			err := bo.OnPREvent(ctx, tc.ev)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Empty(t, spy.failCountCalls)
			assert.Empty(t, spy.failed)
			assert.Empty(t, eri.lastCancel)
		})
	}
}

func TestResolveCIFailureDetailsChecked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		statusCode int
		headerKey  string
		headerVal  string
		isLog      bool
		isRobo     bool
		body       string
		netErr     bool
		nilExec    bool
		wantErr    bool
		wantMatch  string
	}{
		{name: "nil executor returns details", nilExec: true, wantMatch: "default-details"},
		{name: "roborev retryable 500 error", isRobo: true, statusCode: http.StatusInternalServerError, wantErr: true},
		{name: "roborev retryable 429 error", isRobo: true, statusCode: http.StatusTooManyRequests, wantErr: true},
		{name: "roborev retryable 403 error", isRobo: true, statusCode: http.StatusForbidden, headerKey: "X-Ratelimit-Remaining", headerVal: "0", wantErr: true},
		{name: "roborev non-rate-limit 403 falls back to details", isRobo: true, statusCode: http.StatusForbidden, wantMatch: "default-details"},
		{name: "roborev transport error", isRobo: true, netErr: true, wantErr: true},
		{name: "roborev 404 falls back to details", isRobo: true, statusCode: http.StatusNotFound, wantMatch: "default-details"},
		{name: "roborev success match", isRobo: true, statusCode: http.StatusOK, body: `[{"id":1,"body":"## roborev:\n` + "`abc1234`" + `\nCritical finding"}]`, wantMatch: "Critical finding"},
		{name: "job logs retryable 500 error", isLog: true, statusCode: http.StatusInternalServerError, wantErr: true},
		{name: "job logs retryable 429 error", isLog: true, statusCode: http.StatusTooManyRequests, wantErr: true},
		{name: "job logs retryable 403 error", isLog: true, statusCode: http.StatusForbidden, headerKey: "Retry-After", headerVal: "60", wantErr: true},
		{name: "job logs non-rate-limit 403 falls back to details", isLog: true, statusCode: http.StatusForbidden, wantMatch: "failure"},
		{name: "job logs transport error", isLog: true, netErr: true, wantErr: true},
		{name: "job logs 404 falls back to details", isLog: true, statusCode: http.StatusNotFound, wantMatch: "failure"},
		{name: "job logs success match", isLog: true, statusCode: http.StatusOK, body: "=== RUN TestFailure\n--- FAIL: TestFailure (0.02s)\nFAIL\n", wantMatch: "FAIL: TestFailure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilExec {
				got, err := (*executor.Executor)(nil).ResolveCIFailureDetailsChecked(context.Background(), "aerial", 10, "unit-tests", "abc", "", "default-details")
				assert.NoError(t, err)
				assert.Equal(t, "default-details", got)
				return
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.headerKey != "" {
					w.Header().Set(tc.headerKey, tc.headerVal)
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(server.Close)

			client := server.Client()
			apiURL := server.URL
			if tc.netErr {
				apiURL = "http://127.0.0.1:1"
			}

			exec := executor.NewExecutor("", nil, nil, "token", client)
			exec.GitHubAPIURL = apiURL

			checkName := "unit-tests"
			prNum := 0
			checkURL := ""
			details := "failure"
			if tc.isRobo {
				checkName = "roborev"
				prNum = 10
				details = "default-details"
			}
			if tc.isLog {
				checkURL = "https://github.com/azylman/aerial/actions/runs/1/job/999"
				details = "failure"
			}

			got, err := exec.ResolveCIFailureDetailsChecked(context.Background(), "aerial", prNum, checkName, "abc1234", checkURL, details)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, got, tc.wantMatch)
			}
		})
	}
}

func TestActionFailureAcceptedAndCompletionMutation(t *testing.T) {
	t.Parallel()

	// actionFailureAccepted branches
	cFailed := model.VanadisChunk{ID: "c1", Status: statusFailed, Mode: chunkModeAction, StallNudges: 1}
	assert.True(t, actionFailureAccepted(cFailed, statusRunning, "vanadis-c1-turn-1"))
	assert.True(t, actionFailureAccepted(cFailed, statusFailed, "vanadis-c1-turn-1"))
	assert.False(t, actionFailureAccepted(cFailed, statusRunning, "vanadis-c1-turn-0"))

	cRunning := model.VanadisChunk{ID: "c1", Status: statusRunning, Mode: chunkModeAction, StallNudges: 1}
	assert.True(t, actionFailureAccepted(cRunning, statusRunning, "vanadis-c1-turn-1"))

	// actionFailureAllowed PR-attached branches (CI remediation identities)
	cPR := model.VanadisChunk{ID: "c1", Status: statusPROpened, Mode: chunkModeAction, PRNumber: 12, StallNudges: 1}
	assert.True(t, actionFailureAccepted(cPR, statusRunning, ""))
	assert.True(t, actionFailureAccepted(cPR, statusRunning, "vanadis-c1-cifail-abc"))
	assert.True(t, actionFailureAccepted(cPR, statusRunning, "vanadis-conflict-c1"))
	assert.False(t, actionFailureAccepted(cPR, statusRunning, "stale-dispatch"))

	// isFailureCompletionMutation branches
	assert.False(t, isFailureCompletionMutation(nil, nil))
	assert.False(t, isFailureCompletionMutation(&BuildMutationRequest{Action: "other", ChunkID: "c1"}, []model.VanadisChunk{cRunning}))
	assert.True(t, isFailureCompletionMutation(&BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1"}, []model.VanadisChunk{cRunning}))
	assert.False(t, isFailureCompletionMutation(&BuildMutationRequest{Action: "chunk_failed", ChunkID: "other"}, []model.VanadisChunk{cRunning}))

	// dispatchCompletionMutation branches
	mFail := dispatchCompletionMutation(pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "boom", DispatchID: "d1"})
	assert.NotNil(t, mFail)
	assert.Equal(t, "chunk_failed", mFail.Action)
	assert.Equal(t, "boom", mFail.Reason)
	assert.Equal(t, "d1", mFail.DispatchID)

	mComp := dispatchCompletionMutation(pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed", DispatchID: "d1"})
	assert.NotNil(t, mComp)
	assert.Equal(t, mutationActionCompleted, mComp.Action)

	mUnknown := dispatchCompletionMutation(pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "unknown"})
	assert.Nil(t, mUnknown)
}

func TestBuildObject_TransitionErrorPropagation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		handler string
		wantErr string
	}{
		{"create transition error propagates", "Create", "transition failed"},
		{"append transition error propagates", "AppendChunks", "transition failed"},
		{"update chunk transition error propagates", "UpdateChunk", "transition failed"},
		{"remove chunk transition error propagates", "RemoveChunk", "transition failed"},
		{"attach pr transition error propagates", "AttachPR", "transition failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b1 := model.VanadisBuild{ID: "b-trans", Title: "Build", Objective: "Obj", Status: "running", ThreadID: "th-1"}
			c1 := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Title: "Chunk 1", Instructions: "Inst 1", Status: "pending", Mode: "pr", Repo: "aerial", TargetFiles: []string{"pkg/a.go"}}
			c2 := model.VanadisChunk{ID: "c2", BuildID: "b-trans", Title: "Chunk 2", Instructions: "Inst 2", Status: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}}
			st, _ := dbScript{
				buildRow: &mockRow{b: &b1},
				chunks:   []model.VanadisChunk{c1, c2},
				execTag:  "UPDATE 1",
				row:      valuesRow("running"),
				countRow: valuesRow(1),
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("pr")},
					{match: "SELECT build_id FROM vanadis_chunks", row: valuesRow("b-trans")},
					{match: "RETURNING b.id, c.id, b.thread_id", row: valuesRow("b-trans", "c1", "th-1")},
					{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b-trans", 0, "medium", true)},
					{match: "MAX(chunk_index)", row: valuesRow(1)},
				},
			}.open()

			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), &mockEriClient{}, pubsub.NewMemoryBus(), "token", nil))
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
				rt := newFakeObjectRuntime("b-trans")
				rt.runStepErr = errors.New("transition failed")
				return rt
			}

			ctx := makeObjectMockContext(t, "b-trans")
			var err error
			switch tc.handler {
			case "Create":
				_, err = bo.Create(ctx, CreateBuildRequest{ID: "b-trans", ThreadID: "th-1", Objective: "Obj", Chunks: []store.CreateChunkInput{{ID: "c1", Title: "C1", Repo: "aerial", Instructions: "inst", DependsOn: []string{}}}})
			case "AppendChunks":
				_, err = bo.AppendChunks(ctx, AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "C3", Repo: "aerial", Instructions: "inst", DependsOn: []string{"c2"}, TargetFiles: []string{"pkg/c.go"}}}})
			case "UpdateChunk":
				_, err = bo.UpdateChunk(ctx, store.UpdateChunkRequest{ChunkRef: "c1", Title: "New Title"})
			case "RemoveChunk":
				err = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
			case "AttachPR":
				_, err = bo.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
			}
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestBuildObject_AuthoritativeFailureContinuation(t *testing.T) {
	t.Parallel()

	// 1. Replay notifyBuildTerminal on failure continuation when !terminalNotified
	bFailed := model.VanadisBuild{ID: "b-failed", Status: statusFailed, ThreadID: "th-1", Objective: "Test failure"}
	cFailed := model.VanadisChunk{ID: "c1", BuildID: "b-failed", Mode: chunkModeAction, Status: statusFailed, StallNudges: 1}
	st, _ := dbScript{
		buildRow: &mockRow{b: &bFailed},
		chunks:   []model.VanadisChunk{cFailed},
		row:      failRow(statusFailed, statusFailed, 1),
	}.open()
	bus := &deploymentEventRecorder{}
	var queuedPrompts []string
	eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
		queuedPrompts = append(queuedPrompts, prompt)
		return nil
	}}
	bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", nil))

	out, err := bo.executeAuthoritativeTransition(context.Background(), "b-failed", &BuildMutationRequest{
		Action:     "chunk_failed",
		ChunkID:    "c1",
		DispatchID: "vanadis-c1-turn-1",
		Reason:     "execution error",
	}, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, out.BuildStatus)
	assert.True(t, out.State.TerminalNotified)
	assert.Len(t, queuedPrompts, 1)
	assert.Contains(t, queuedPrompts[0], "[VANADIS BUILD FAILED]")
	assert.Len(t, bus.events, 1)

	// Second run with terminalNotified=true does not duplicate notification
	queuedPrompts = nil
	bus.events = nil
	out2, err2 := bo.executeAuthoritativeTransition(context.Background(), "b-failed", &BuildMutationRequest{
		Action:     "chunk_failed",
		ChunkID:    "c1",
		DispatchID: "vanadis-c1-turn-1",
	}, nil, true)
	assert.NoError(t, err2)
	assert.Equal(t, statusFailed, out2.BuildStatus)
	assert.Empty(t, queuedPrompts)
	assert.Empty(t, bus.events)

	// 2. FailMutation retry logic: returns nil when !failed but chunk was already failed and accepted
	bldAlreadyFailed := model.VanadisBuild{ID: "b-fail", Status: statusFailed}
	chkAlreadyFailed := model.VanadisChunk{ID: "c1", BuildID: "b-fail", Mode: chunkModeAction, Status: statusFailed, StallNudges: 1}
	storeRetryOk, _ := dbScript{
		buildRow: &mockRow{b: &bldAlreadyFailed},
		chunks:   []model.VanadisChunk{chkAlreadyFailed},
		row:      failRow(statusFailed, statusFailed, 1),
	}.open()
	boFailRetry := NewBuildObject("", nil).WithAuthority(storeRetryOk, nil)
	err = boFailRetry.failMutation(context.Background(), "b-fail", &BuildMutationRequest{
		Action:     "chunk_failed",
		ChunkID:    "c1",
		DispatchID: "vanadis-c1-turn-1",
	})
	assert.NoError(t, err)

	// 3. FailChunk second journaled step failure propagates before ClearState
	eriFail := &mockEriClient{
		promptFn: func(context.Context, string, string) error { return errors.New("prompt unavailable") },
	}
	execFail := executor.NewExecutor(t.TempDir(), eriFail, pubsub.NewMemoryBus(), "token", nil)
	bRunning := model.VanadisBuild{ID: "b-running", Status: "running", ThreadID: "th-1"}
	cAction := model.VanadisChunk{ID: "c1", BuildID: "b-running", Status: "running", Mode: chunkModeAction}
	storeFailChunk, _ := dbScript{
		buildRow: &mockRow{b: &bRunning},
		chunks:   []model.VanadisChunk{cAction},
		row:      failRow("running", "running"),
	}.open()
	boFailChunk := NewBuildObject("", nil).WithAuthority(storeFailChunk, execFail)
	fakeRT := newFakeObjectRuntime("b-running")
	boFailChunk.runtimeFactory = func(restate.ObjectContext) objectRuntime {
		return fakeRT
	}
	ctxMock := makeObjectMockContext(t, "b-running")
	_, fErr := boFailChunk.FailChunk(ctxMock, model.FailChunkRequest{ChunkID: "c1", Reason: "hard error"})
	assert.Error(t, fErr)
	assert.False(t, fakeRT.cleared, "ClearState must not be called when notification step fails")
}

type deploymentStartStore struct {
	*checkStatusSpyStore

	writeErr error
}

func (s *deploymentStartStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	c := s.chunk(id)
	c.Status = status
	c.RolloutState = rollout
	return nil
}

type deadlineLifecycleStore struct{ *checkStatusSpyStore }

func (s *deadlineLifecycleStore) GetBuild(ctx context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	b, c, err := s.checkStatusSpyStore.GetBuild(ctx, id)
	return b, append([]model.VanadisChunk(nil), c...), err
}

func (s *deadlineLifecycleStore) UpdateChunkBannerAndStatus(_ context.Context, id, status, banner string) error {
	if s.bannerErr != nil {
		return s.bannerErr
	}
	c := s.chunk(id)
	c.Status = status
	c.Banner = banner
	return nil
}

func (s *deadlineLifecycleStore) UpdatePRLifecycleStatus(_ context.Context, id, status string) error {
	c := s.chunk(id)
	c.Status = status
	return nil
}

func (s *deadlineLifecycleStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	c := s.chunk(id)
	if status != "" {
		c.Status = status
	}
	c.RolloutState = rollout
	return nil
}

// deploymentGateFakeRuntime is an in-memory deploygate.Runtime for deployment
// gate tests that exercise BuildObject wrappers end to end.
type deploymentGateFakeRuntime struct {
	*fakeObjectRuntime

	now       time.Time
	attempts  map[string]deploygate.Attempt
	pending   map[string]deploygate.PendingStart
	scheduled []deploygate.DeploymentDeadlineRequest
}

func (r *deploymentGateFakeRuntime) GetAttempts() (map[string]deploygate.Attempt, error) {
	return r.attempts, nil
}

func (r *deploymentGateFakeRuntime) SetAttempts(a map[string]deploygate.Attempt) { r.attempts = a }

func (r *deploymentGateFakeRuntime) Now() (time.Time, error) { return r.now, nil }

func (r *deploymentGateFakeRuntime) GetPendingStarts() (map[string]deploygate.PendingStart, error) {
	return r.pending, nil
}

func (r *deploymentGateFakeRuntime) SetPendingStarts(p map[string]deploygate.PendingStart) {
	r.pending = p
}

func (r *deploymentGateFakeRuntime) ScheduleDeadline(req deploygate.DeploymentDeadlineRequest, d time.Duration) {
	r.scheduled = append(r.scheduled, req)
}

func (r *deploymentGateFakeRuntime) RunStep(name string, fn func(ctx context.Context) (deploygate.StepOutput, error)) (deploygate.StepOutput, error) {
	out, err := r.RunReconcileStep(name, func(ctx context.Context) (reconcileStepOutput, error) {
		o, runErr := fn(ctx)
		return reconcileStepOutput(o), runErr
	})
	return deploygate.StepOutput(out), err
}

func TestDeploymentStartEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		rollout       *pubsub.PRRolloutState
		writeErr      error
		wantLifecycle string
	}{
		{"without snapshot", nil, nil, statusDeploying},
		{"with snapshot", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, nil, ""},
		{"snapshot write unavailable", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, errors.New("write unavailable"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &deploymentStartStore{writeErr: tc.writeErr, checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, MergeSHA: "sha", Status: statusMerged}}}}
			_, err := (&BuildObject{store: st}).recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b", pubsub.PRLifecycleEvent{Event: prEventDeployStarted, MergeSHA: "sha", RolloutState: tc.rollout})
			require.Equal(t, tc.writeErr != nil, err != nil)
			require.Equal(t, tc.wantLifecycle, st.lifecycleStatus)
			if tc.writeErr == nil {
				require.Equal(t, tc.rollout, st.chunks[0].RolloutState)
			} else {
				require.Nil(t, st.chunks[0].RolloutState)
			}
		})
	}
}

func TestDeploymentStartPreservesFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rollout *pubsub.PRRolloutState
	}{
		{"duplicate without evidence", nil},
		{"additional job evidence", &pubsub.PRRolloutState{ExpectedJobs: []string{"web", "worker"}, StartedJobs: []string{"worker"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}}
			b := &BuildObject{store: st}
			_, err := b.recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b1", pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", RolloutState: tc.rollout})
			require.NoError(t, err)
			require.Equal(t, statusDeployFailed, st.chunks[0].Status)
			require.Equal(t, "expired", st.chunks[0].Banner)
			require.Equal(t, tc.rollout, st.chunks[0].RolloutState)
		})
	}
}

func TestDeploymentDeadlineRepeatedMerge(t *testing.T) {
	t.Parallel()
	tests := []struct{ name string }{{"start then repeated merge then expiry"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := &deploymentGateFakeRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			st := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusMerged}}}}
			b := &BuildObject{store: st}
			ev := pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now}
			rc := mockRunContext{Context: context.Background()}
			_, err := b.recordDeploymentPREvent(rc, "b", ev)
			require.NoError(t, err)
			require.NoError(t, b.armDeploymentStart(rt, "b", ev))
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c1"].Deadline)
			require.NoError(t, b.handlePREventType(rc, "b", "c1", "merged", pubsub.PRLifecycleEvent{MergeSHA: "sha"}))
			require.Equal(t, statusMerged, st.chunks[0].Status)
			eri := &mockEriClient{}
			b.executor = &executor.Executor{EriClient: eri}
			rt.now = now.Add(15 * time.Minute)
			require.NoError(t, b.expireDeploymentDeadline(rt, deploygate.DeploymentDeadlineRequest{ChunkID: "c1", Revision: "sha", Generation: 1}))
			require.Equal(t, statusDeployFailed, st.chunks[0].Status)
			require.NotNil(t, eri.lastDispatch)
			require.True(t, rt.attempts["c1"].Delivered)
		})
	}
}
