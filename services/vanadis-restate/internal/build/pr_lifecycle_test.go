package build

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// checkStatusSpyStore backs the deploy gate, deployment-start/deadline and
// remediation-deferral tests: it records lifecycle status, budget increments
// and chunk failures, and accepts every other PR write.
type checkStatusSpyStore struct {
	store.VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []model.VanadisChunk
	failed          []string
	failCountCalls  []string
	failCount       int
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

func (*checkStatusSpyStore) RecordPRCheckStatus(context.Context, string, string, string, string) error {
	return nil
}

func (s *checkStatusSpyStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, _ string) error {
	if sha := strings.TrimSpace(headSHA); sha != "" {
		s.chunk(chunkID).HeadSHA = sha
	}
	return nil
}

func (s *checkStatusSpyStore) UpdatePRLifecycleStatus(_ context.Context, _ string, status string) error {
	s.lifecycleStatus = status
	return nil
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	return &model.VanadisBuild{ID: "b1", Status: statusRunning, Objective: "Obj", ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed}, true, nil
}

func (*checkStatusSpyStore) UpdateChunkBannerAndStatus(context.Context, string, string, string) error {
	return nil
}

func (*checkStatusSpyStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	return true, nil
}

func (*checkStatusSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	return &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, nil
}

func TestBuildObject_ExecutorErrorBranches(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-err", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-err", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	st, _ := seededStore(t, []model.VanadisBuild{b1}, []model.VanadisChunk{c1})
	eriErr := &mockEriClient{
		cancelFn: func(context.Context, string) error { return errors.New("cancel err") },
		promptFn: func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
			return "", errors.New("widget err")
		},
		messageFn: func(context.Context, string, string) error { return errors.New("msg err") },
	}
	mockBusErr := &pubsubtest.FaultBus{PublishErr: errors.New("pub err")}
	execErr := executor.NewExecutor(t.TempDir(), eriErr, mockBusErr, "token", "example-owner", nil)
	boErr := NewBuildObject("", nil).WithAuthority(st, execErr)

	// notifyBuildTerminal error branches
	assert.Error(t, boErr.notifyBuildTerminal(context.Background(), &b1, []model.VanadisChunk{c1}, "failed", "chunk failed", "TITLE", "MSG"))

	// notifyChunkFailure swallows cancel and widget errors; reconcile owns the wake
	ctx := makeObjectMockContext(t, "b-err")
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		assert.NoError(t, boErr.notifyChunkFailure(rc, b1, "c1", "reason"))
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

	storeErr := &store.Store{}
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
	var activeDispatch *eriwire.BrainDispatchRequest
	eri.dispatchFn = func(_ context.Context, req eriwire.BrainDispatchRequest) error {
		if dispatchErr != nil {
			return dispatchErr
		}
		if req.SkipIfActive && active {
			eri.LastDispatch = activeDispatch
			return eriwire.ErrDispatchActive
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
		bus = &pubsubtest.FaultBus{PublishErr: notifyErr}
	}
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil)
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
			firstDispatchErr: eriwire.ErrDispatchActive,
			wantErr:          false,
		},
		{
			name:             "conflict skipped_active ignores active worker without failing",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: eriwire.ErrDispatchActive,
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
		{
			name:             "ci_failed github api retryable error defers OnPREvent without failing",
			ev:               ciFailEv("unit-tests", "abcdef1234"),
			remediatedSHA:    "abcdef1234",
			firstDispatchErr: errors.New("github api request failed: connection reset"),
			wantErr:          false,
		},
		{
			name:             "conflict github api 500 error defers OnPREvent without failing",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: &githubapi.StatusError{StatusCode: 500, Message: "github api returned status 500"},
			wantErr:          false,
		},
		{
			name:             "conflict github api non-retryable 403 error fails OnPREvent without deferral",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: &githubapi.StatusError{StatusCode: 403, Message: "github api forbidden (status 403)"},
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
			assert.Empty(t, eri.LastCancel)
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
			c2 := model.VanadisChunk{ID: "c2", BuildID: "b-trans", ChunkIndex: 1, Title: "Chunk 2", Instructions: "Inst 2", Status: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}}
			st, _ := seededStore(t, []model.VanadisBuild{b1}, []model.VanadisChunk{c1, c2})

			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), &mockEriClient{}, pubsub.NewMemoryBus(), "token", "example-owner", nil))
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime {
				rt := newFakeObjectRuntime("b-trans")
				rt.runStepErr = errors.New("transition failed")
				return rt
			}

			ctx := makeObjectMockContext(t, "b-trans")
			var err error
			switch tc.handler {
			case "Create":
				_, err = bo.Create(ctx, CreateBuildRequest{ID: "b-trans", ThreadID: "th-1", Title: "Build", Objective: "Obj", Tier: 2, Chunks: []store.CreateChunkInput{
					{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Inst 1", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
					{ID: "c2", Title: "Chunk 2", Repo: "aerial", Instructions: "Inst 2", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}},
				}})
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

	// 1. A legacy failed build (no FailureIsolation) wakes once per new chunk
	// failure and never relaunches.
	bFailed := model.VanadisBuild{ID: "b-failed", Status: statusFailed, ThreadID: "th-1", Objective: "Test failure"}
	cFailed := model.VanadisChunk{ID: "c1", BuildID: "b-failed", Mode: chunkModeAction, Status: statusFailed, StallNudges: 1}
	st, _ := seededStore(t, []model.VanadisBuild{bFailed}, []model.VanadisChunk{cFailed})
	bus := &deploymentEventRecorder{}
	var queuedPrompts []string
	eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
		queuedPrompts = append(queuedPrompts, prompt)
		return nil
	}}
	bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", eri, bus, "token", "example-owner", nil))

	out, err := bo.executeAuthoritativeTransition(context.Background(), "b-failed", &BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1", Reason: "execution error"}, nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, out.BuildStatus)
	assert.False(t, out.State.FailureIsolation)
	assert.Equal(t, map[string]bool{failureNotifyKey(cFailed): true}, out.State.FailureNotified)
	assert.Len(t, queuedPrompts, 1)
	assert.Contains(t, queuedPrompts[0], "[VANADIS CHUNK FAILED]")
	assert.Len(t, bus.events, 1)

	// Second run with the recorded ledger does not duplicate the wake
	queuedPrompts, bus.events = nil, nil
	out2, err2 := bo.executeAuthoritativeTransition(context.Background(), "b-failed", &BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1"}, out.State, nil, true)
	assert.NoError(t, err2)
	assert.Equal(t, statusFailed, out2.BuildStatus)
	assert.Empty(t, queuedPrompts)
	assert.Empty(t, bus.events)

	// 2. FailMutation retry logic: returns nil when !failed but chunk was already failed and accepted
	bldAlreadyFailed := model.VanadisBuild{ID: "b-fail", Status: statusFailed}
	chkAlreadyFailed := model.VanadisChunk{ID: "c1", BuildID: "b-fail", Mode: chunkModeAction, Status: statusFailed, StallNudges: 1}
	storeRetryOk, _ := seededStore(t, []model.VanadisBuild{bldAlreadyFailed}, []model.VanadisChunk{chkAlreadyFailed})
	boFailRetry := NewBuildObject("", nil).WithAuthority(storeRetryOk, nil)
	err = boFailRetry.failMutation(context.Background(), "b-fail", &BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", DispatchID: "vanadis-c1-turn-1"})
	assert.NoError(t, err)

	// 3. FailChunk's notify step no longer wakes; the persisted failure is
	// woken by reconcile, so a failing wake client surfaces as a retryable
	// error only after the failure is durable. The sibling keeps the build
	// running, and ClearState is never called on a running build.
	eriFail := &mockEriClient{promptFn: func(context.Context, string, string) error { return errors.New("prompt unavailable") }}
	execFail := executor.NewExecutor(t.TempDir(), eriFail, pubsub.NewMemoryBus(), "token", "example-owner", nil)
	bRunning := model.VanadisBuild{ID: "b-running", Status: "running", ThreadID: "th-1"}
	cAction := model.VanadisChunk{ID: "c1", BuildID: "b-running", Status: "running", Mode: chunkModeAction}
	cSibling := model.VanadisChunk{ID: "c2", BuildID: "b-running", ChunkIndex: 1, Status: "running", Mode: chunkModeAction}
	storeFailChunk, _ := seededStore(t, []model.VanadisBuild{bRunning}, []model.VanadisChunk{cAction, cSibling})
	fakeRT := newFakeObjectRuntime("b-running")
	boFailChunk := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return fakeRT }).WithAuthority(storeFailChunk, execFail)
	ctxMock := makeObjectMockContext(t, "b-running")
	_, fErr := boFailChunk.FailChunk(ctxMock, model.FailChunkRequest{ChunkID: "c1", Reason: "hard error"})
	assert.ErrorContains(t, fErr, "prompt unavailable")
	assert.False(t, restate.IsTerminalError(fErr), "a failed wake must stay retryable")
	assert.Equal(t, map[string]string{"c1": statusFailed, "c2": statusRunning}, chunkStatuses(t, storeFailChunk, "b-running"))
	stored, _ := loadBuild(t, storeFailChunk, "b-running")
	assert.Equal(t, statusRunning, stored.Status)
	assert.False(t, fakeRT.cleared, "ClearState must not be called on a running build")

	// 4. FailChunk success on failed build leaves fakeRT.cleared false (non-terminal failed state)
	eriSuccess := &mockEriClient{}
	execSuccess := executor.NewExecutor(t.TempDir(), eriSuccess, pubsub.NewMemoryBus(), "token", "example-owner", nil)
	cRunningPR := model.VanadisChunk{ID: "c1", BuildID: "b-failed", Mode: "pr", Repo: "aerial", Status: "running"}
	cRunningPR.PRNumber = 101
	storeFailChunkOk, _ := seededStore(t, []model.VanadisBuild{bFailed}, []model.VanadisChunk{cRunningPR})
	fakeRTOk := newFakeObjectRuntime("b-failed")
	boFailChunkOk := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return fakeRTOk }).WithAuthority(storeFailChunkOk, execSuccess)
	ctxMockOk := makeObjectMockContext(t, "b-failed")
	respOk, fErrOk := boFailChunkOk.FailChunk(ctxMockOk, model.FailChunkRequest{ChunkID: "c1", Reason: "hard error"})
	assert.NoError(t, fErrOk)
	assert.True(t, respOk.Failed)
	assert.Equal(t, statusFailed, respOk.BuildStatus)
	assert.False(t, fakeRTOk.cleared, "ClearState must not be called when FailChunk succeeds on failed build")
	assert.Equal(t, map[string]string{"c1": statusFailed}, chunkStatuses(t, storeFailChunkOk, "b-failed"))
}

// deadlineLifecycleStore applies lifecycle, banner and rollout writes to the
// spy's chunks; writeErr fails RecordPRRolloutState.
type deadlineLifecycleStore struct {
	*checkStatusSpyStore

	writeErr error
}

func (s *deadlineLifecycleStore) GetBuild(ctx context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	b, c, err := s.checkStatusSpyStore.GetBuild(ctx, id)
	return b, append([]model.VanadisChunk(nil), c...), err
}

func (s *deadlineLifecycleStore) UpdateChunkBannerAndStatus(_ context.Context, id, status, banner string) error {
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
	if s.writeErr != nil {
		return s.writeErr
	}
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

func TestRecordDeploymentPREvent(t *testing.T) {
	t.Parallel()
	web := &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}
	worker := &pubsub.PRRolloutState{ExpectedJobs: []string{"web", "worker"}, StartedJobs: []string{"worker"}}
	merged := model.VanadisChunk{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusMerged}
	expired := model.VanadisChunk{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}
	tests := []struct {
		name                   string
		chunk                  model.VanadisChunk
		rollout                *pubsub.PRRolloutState
		writeErr               error
		wantErr                string
		wantStatus, wantBanner string
		wantRollout            *pubsub.PRRolloutState
	}{
		{name: "start without snapshot", chunk: merged, wantStatus: statusDeploying},
		{name: "start with snapshot", chunk: merged, rollout: web, wantStatus: statusDeploying, wantRollout: web},
		{name: "snapshot write unavailable", chunk: merged, rollout: web, writeErr: errors.New("write unavailable"), wantErr: "write unavailable", wantStatus: statusMerged},
		{name: "duplicate start without evidence preserves failure", chunk: expired, wantStatus: statusDeployFailed, wantBanner: "expired"},
		{name: "additional job evidence preserves failure", chunk: expired, rollout: worker, wantStatus: statusDeployFailed, wantBanner: "expired", wantRollout: worker},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &deadlineLifecycleStore{writeErr: tc.writeErr, checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{tc.chunk}}}
			ev := pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", RolloutState: tc.rollout}
			_, err := (&BuildObject{store: st}).recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b1", ev)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantStatus, st.chunks[0].Status)
			assert.Equal(t, tc.wantBanner, st.chunks[0].Banner)
			assert.Equal(t, tc.wantRollout, st.chunks[0].RolloutState)
		})
	}
}

// TestDeploymentDeadlineRepeatedMerge pins the ordering of a deploy start,
// a redelivered merge and the deadline expiry through the BuildObject
// wrappers: the repeated merge must not disarm the deadline.
func TestDeploymentDeadlineRepeatedMerge(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rt := &deploymentGateFakeRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
	st := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusMerged}}}}
	b := &BuildObject{store: st}
	ev := pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now}
	rc := mockRunContext{Context: context.Background()}
	_, err := b.recordDeploymentPREvent(rc, "b1", ev)
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
	require.NotNil(t, eri.LastDispatch)
	require.True(t, rt.attempts["c1"].Delivered)
}

func TestBuildObject_PRRecoveryWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, event, parent string
		wantWidget          bool
	}{
		{"failed synchronize", "synchronize", "failed", true},
		{"failed opened", "opened", "failed", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := seededStore(t, []model.VanadisBuild{{ID: "b1", Status: tc.parent, ThreadID: "th-1"}},
				[]model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "owner/repo", Status: "pr_opened", PRNumber: 12, HeadSHA: "old"}, {ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Status: "failed"}})
			var observed *executor.StatusWidgetRequest
			eri := &mockEriClient{widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
				observed = &req
				return "widget", nil
			}}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"state":"open","merged":false,"head":{"sha":"new"}}`))
			}))
			t.Cleanup(api.Close)
			exec := executor.NewExecutor("", eri, nil, "", "example-owner", nil)
			exec.GitHubAPIURL = api.URL
			exec.HTTPClient = api.Client()
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			err := bo.OnPREvent(makeObjectMockContext(t, "b1"), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Event: tc.event, HeadSHA: "new"})
			if err != nil {
				t.Fatal(err)
			}
			if observed == nil || observed.Status != "failed" || len(observed.Chunks) != 2 || observed.Chunks[0].Status != "open" {
				t.Fatalf("widget=%+v", observed)
			}
		})
	}
}

// TestBuildObject_OnPREvent_UnattachedPRResolution covers only the OnPREvent
// wiring into prflow resolution; the resolution rules themselves live in
// prflow TestFlow_ResolveOrAttachPRChunk and executor TestValidateChunkPRAttachment.
func TestBuildObject_OnPREvent_UnattachedPRResolution(t *testing.T) {
	t.Parallel()
	const prBody = `{"state":"open","merged":false,"author_association":%q,"user":{"login":%q},` +
		`"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->",` +
		`"head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},` +
		`"base":{"repo":{"full_name":"external/aerial"}}}`
	ev := pubsub.PRLifecycleEvent{Event: "opened", Repo: "external/aerial", PRNumber: 101, Branch: "feat/aerial-pr-vanadis-chunk-1-abcdef"}
	chunks := []model.VanadisChunk{{ID: "chunk-1", BuildID: "b1", Mode: "pr", Status: statusRunning, Repo: "external/aerial", StartedAt: time.Unix(1700000000, 0).UTC()}}
	tests := []struct {
		name           string
		association    string
		author         string
		widgetErr      error
		attachErr      error
		ghStatus       int
		wantErr        string
		wantAttached   string
		wantWidgetSync bool
	}{
		{name: "verified PR attaches and syncs widget", association: "OWNER", author: "external", wantAttached: "chunk-1", wantWidgetSync: true},
		{name: "untrusted author association rejected", association: "NONE", author: "stranger"},
		{name: "widget sync failure still attaches", association: "OWNER", author: "external", widgetErr: errors.New("widget down"), wantAttached: "chunk-1"},
		{name: "github verification unavailable is not fatal", association: "OWNER", author: "external", ghStatus: http.StatusServiceUnavailable},
		{name: "attach error propagates", association: "OWNER", author: "external", attachErr: errors.New("attach unavailable"), wantErr: "attach unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &unattachedPREventSpyStore{chunks: slices.Clone(chunks), attachErr: tc.attachErr}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.ghStatus != 0 {
					w.WriteHeader(tc.ghStatus)
				}
				fmt.Fprintf(w, prBody, tc.association, tc.author)
			}))
			t.Cleanup(api.Close)
			eri := &mockEriClient{widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) { return "msg-1", tc.widgetErr }}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", api.Client())
			exec.GitHubAPIURL = api.URL
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)

			err := bo.OnPREvent(makeObjectMockContext(t, "b1"), ev)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantAttached, spy.attachedChunkID)
			assert.Empty(t, spy.failedChunkID)
			if tc.wantAttached != "" {
				assert.Equal(t, 101, spy.attachedPR)
				assert.Equal(t, "external/aerial", spy.attachedRepo)
			}
			if tc.wantWidgetSync {
				assert.Positive(t, spy.syncWidgetCalls)
			}
		})
	}
}

type recordingChunkBus struct {
	pubsub.Bus

	mu       sync.Mutex
	subjects []string
	events   []pubsub.ExternalEvent
}

func (b *recordingChunkBus) Publish(_ context.Context, subject, _ string, data any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subjects = append(b.subjects, subject)
	if ev, ok := data.(pubsub.ExternalEvent); ok {
		b.events = append(b.events, ev)
	}
	return nil
}

// TestBuildObject_OnDispatchOutcome_CIFixupTurnEnded covers the OnDispatchOutcome
// routing for verdictless CI fix-up turns; the pause rules themselves live in
// prflow TestFlow_HandleCIRemediationTurnEnded.
func TestBuildObject_OnDispatchOutcome_CIFixupTurnEnded(t *testing.T) {
	t.Parallel()
	const (
		head      = "abcdef1234567"
		openPR    = `{"state":"open","merged":false,"head":{"sha":"abcdef1234567"}}`
		movedPR   = `{"state":"open","merged":false,"head":{"sha":"fedcba7654321"}}`
		pauseMsg  = "🛑 [aerial#42] CI fix-up turn ended without pushing a fix, recording an override, or calling fail_chunk. Chunk paused for a decision."
		buildWake = "[VANADIS BUILD FAILED]\nBuild ID: b1\nObjective: Fix CI\n\nBuild encountered failures.\nChunk 1 failed [c1]"
	)
	ciFailed := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "ci_failed", PRNumber: 42, CIFailCount: 2, Repo: "aerial", Title: "Chunk 1"}
	tests := []struct {
		name, dispatchID, status, prBody string
		prStatus                         int
		wantTerminal                     bool
		wantFailReason, wantMsg          string
		wantPrompts                      []string
		wantGitHubCalls                  int32
	}{
		{"completed fix-up on unchanged head pauses and wakes", executor.CIRemediationDispatchID("c1", head, 2), "completed", openPR, 200, false, "ci_remediation_no_outcome", pauseMsg, []string{buildWake}, 1},
		{"fix-up that pushed is a no-op", executor.CIRemediationDispatchID("c1", head, 2), "completed", movedPR, 200, false, "", "", nil, 1},
		{"stale attempt is a no-op", executor.CIRemediationDispatchID("c1", head, 1), "completed", openPR, 200, false, "", "", nil, 0},
		{"non-cifail dispatch keeps no-pr path", "vanadis-c1-turn-0", "completed", openPR, 200, false, "", "", nil, 0},
		{"conflict dispatch keeps no-pr path", executor.ConflictRemediationDispatchID("c1", head), "completed", openPR, 200, false, "", "", nil, 0},
		{"github client error is terminal", executor.CIRemediationDispatchID("c1", head, 2), "completed", `{"message":"Forbidden"}`, 403, true, "", "", nil, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, "/repos/example-owner/aerial/pulls/42", r.URL.Path)
				w.WriteHeader(tc.prStatus)
				_, _ = w.Write([]byte(tc.prBody))
			}))
			t.Cleanup(api.Close)
			build := model.VanadisBuild{ID: "b1", Status: statusRunning, ThreadID: "thread", Objective: "Fix CI"}
			pgStore, _ := seededStore(t, []model.VanadisBuild{build}, []model.VanadisChunk{ciFailed})
			st := &noPRFailStore{VanadisStore: pgStore}
			var prompts []string
			eri := &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
				prompts = append(prompts, prompt)
				return nil
			}}
			exec := executor.NewExecutor("", eri, &deploymentEventRecorder{}, "token", "example-owner", api.Client())
			exec.GitHubAPIURL = api.URL
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return newFakeObjectRuntime("b1") }
			err := bo.OnDispatchOutcome(makeObjectMockContext(t, "b1"), pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", DispatchID: tc.dispatchID, Status: tc.status})
			assert.Equal(t, tc.wantTerminal, restate.IsTerminalError(err), err)
			if !tc.wantTerminal {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantFailReason, st.failReason)
			assert.Equal(t, tc.wantMsg, eri.LastMsgCont)
			assert.Equal(t, tc.wantPrompts, prompts)
			assert.Equal(t, tc.wantGitHubCalls, calls.Load())
		})
	}
}
