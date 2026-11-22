package build

import (
	"context"
	"errors"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestBuildObject_GuardRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		ownAgent      string
		key           string
		guardErr      error
		wantGuardStep bool
		wantErr       bool
	}{
		{name: "non-empty key executes journaled runner guard", key: "b1", wantGuardStep: true},
		{name: "empty key skips runner guard", key: "", wantGuardStep: false},
		{name: "runner guard error surfaces", key: "b1", guardErr: errors.New("guard fail"), wantGuardStep: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeObjectRuntime(tc.key)
			rt.runnerGuardErr = tc.guardErr
			bo := NewBuildObjectWithAgent("", tc.ownAgent, nil, func(restate.ObjectContext) objectRuntime { return rt })
			err := bo.guardRunner(nil, "test")
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantGuardStep, rt.runnerGuardCalled)
		})
	}
}

// createCaptureStore records the build Create persists.
type createCaptureStore struct {
	store.VanadisStore

	created model.VanadisBuild
	chunks  []model.VanadisChunk
}

func (s *createCaptureStore) CreateBuild(_ context.Context, b model.VanadisBuild, chunks []model.VanadisChunk) error {
	s.created = b
	s.chunks = chunks
	return nil
}

func (s *createCaptureStore) GetBuild(_ context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.created.ID == id {
		return &s.created, s.chunks, nil
	}
	return nil, nil, store.ErrBuildNotFound
}

func TestBuildObject_CreateRecordsOwnRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		ownAgent   string
		reqAgent   string
		wantAgent  string
		wantRunner string
	}{
		{name: "default object runs unowned build", wantAgent: "aerial", wantRunner: "aerial"},
		{name: "default object runs default-owned build", reqAgent: "aerial", wantAgent: "aerial", wantRunner: "aerial"},
		{name: "default object runs sibling-owned build", reqAgent: "agent-beta", wantAgent: "agent-beta", wantRunner: "aerial"},
		{name: "sibling object runs its own build", ownAgent: "Agent-Alpha", reqAgent: "agent-alpha", wantAgent: "agent-alpha", wantRunner: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &createCaptureStore{}
			rt := newFakeObjectRuntime("b1")
			rt.runStepErr = errors.New("stop after persist")
			bo := NewBuildObjectWithAgent("", tc.ownAgent, nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(st, nil)
			req := CreateBuildRequest{Agent: tc.reqAgent, ThreadID: "t1", Objective: "o", Chunks: []store.CreateChunkInput{{ID: "c1", Title: "c", Mode: "action", Instructions: "i", DependsOn: []string{}}}}
			_, _ = bo.Create(makeObjectMockContext(t, "b1"), req)
			assert.Equal(t, tc.wantAgent, st.created.Agent)
			assert.Equal(t, tc.wantRunner, st.created.RunnerAgent)
			require.NotNil(t, rt.state)
			assert.Equal(t, tc.wantRunner, rt.state.Build.RunnerAgent)
		})
	}
}

func TestBuildObject_CreateAcceptsAllOwners(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		ownAgent  string
		reqAgent  string
		wantAgent string
	}{
		{name: "sibling object accepts unowned build", ownAgent: "agent-alpha", wantAgent: "agent-alpha"},
		{name: "sibling object accepts default-owned build", ownAgent: "agent-alpha", reqAgent: "aerial", wantAgent: "aerial"},
		{name: "sibling object accepts other sibling build", ownAgent: "agent-alpha", reqAgent: "agent-beta", wantAgent: "agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &createCaptureStore{}
			rt := newFakeObjectRuntime("b1")
			bo := NewBuildObjectWithAgent("", tc.ownAgent, nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(st, nil)
			req := CreateBuildRequest{Agent: tc.reqAgent, ThreadID: "t1", Objective: "o", Chunks: []store.CreateChunkInput{{ID: "c1", Title: "c", Mode: "action", Instructions: "i", DependsOn: []string{}}}}
			created, err := bo.Create(makeObjectMockContext(t, "b1"), req)
			require.NoError(t, err)
			assert.Equal(t, "b1", created.ID)
			assert.Equal(t, tc.wantAgent, created.Agent)
			assert.Equal(t, tc.wantAgent, st.created.Agent)
			require.NotNil(t, rt.state)
			assert.Equal(t, tc.wantAgent, rt.state.Build.Agent)
		})
	}
}

func TestWithBuildOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		ctxAgent  string
		build     *model.VanadisBuild
		wantAgent string
	}{
		{name: "nil build keeps context agent", ctxAgent: "aerial", wantAgent: "aerial"},
		{name: "unowned build keeps context agent", ctxAgent: "aerial", build: &model.VanadisBuild{Agent: " "}, wantAgent: "aerial"},
		{name: "owner overrides runner agent", ctxAgent: "aerial", build: &model.VanadisBuild{Agent: "agent-alpha", RunnerAgent: "aerial"}, wantAgent: "agent-alpha"},
		{name: "owner set on bare context", build: &model.VanadisBuild{Agent: "agent-beta"}, wantAgent: "agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.ctxAgent != "" {
				ctx = ericlient.WithAgent(ctx, tc.ctxAgent)
			}
			assert.Equal(t, tc.wantAgent, ericlient.AgentFromContext(withBuildOwner(ctx, tc.build)))
		})
	}
}

// removalStore answers the chunk-removal step for a build owned by owner.
type removalStore struct {
	store.VanadisStore

	owner string
}

func (s *removalStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: buildID, Agent: s.owner, RunnerAgent: "aerial"}, nil, nil
}

func (*removalStore) RemoveChunk(_ context.Context, buildID, chunkRef string) (*model.VanadisChunk, []model.VanadisChunk, error) {
	return &model.VanadisChunk{ID: chunkRef, BuildID: buildID}, nil, nil
}

func TestBuildObject_ChunkRemovalCancelsOnOwnerSessions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		owner     string
		wantAgent string
	}{
		{name: "pinned sibling build cancels on owner", owner: "agent-alpha", wantAgent: "agent-alpha"},
		{name: "default build cancels on default agent", owner: "aerial", wantAgent: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &mockEriClient{}
			exec := executor.NewExecutor("", eri, pubsub.NewMemoryBus(), "test-token", "azylman", nil)
			rt := newFakeObjectRuntime("b1")
			rt.runStepErr = errors.New("stop after removal")
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(&removalStore{owner: tc.owner}, exec)
			_ = bo.executeChunkRemoval(makeObjectMockContext(t, "b1"), rt, "b1", "c1")
			assert.Equal(t, "c1", eri.lastCancel)
			assert.Equal(t, tc.wantAgent, eri.cancelAgent)
		})
	}
}

// TestBuildObject_CrossBuildReconcileSends drives a deploy_success mutation
// through the object transition and asserts every swept foreign build gets a
// one-way widget_sync Reconcile on its own runner's Build service.
func TestBuildObject_CrossBuildReconcileSends(t *testing.T) {
	t.Parallel()
	mergeLater := time.Date(2026, 5, 1, 14, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		swept    []store.SweptChunk
		wantSent []sentReconcile
	}{
		{name: "no sweep sends nothing"},
		{name: "default runner build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2", RunnerAgent: "aerial"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}}},
		{name: "sibling runner build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2", RunnerAgent: "agent-alpha"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}}},
		{name: "one send per build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2", RunnerAgent: "agent-alpha"}, {ChunkID: "f2", BuildID: "b2", RunnerAgent: "agent-alpha"}, {ChunkID: "g1", BuildID: "b3"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}, {Service: "Build", BuildID: "b3", Reason: "widget_sync"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &crossBuildSweepSpyStore{
				ownBuild:   &model.VanadisBuild{ID: "b1", Status: statusRunning},
				ownChunks:  []model.VanadisChunk{{ID: "deploying", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: statusDeploying, PRNumber: 7, CompletedAt: mergeLater}},
				crossSwept: tc.swept,
			}
			rt := newFakeObjectRuntime("b1")
			bo := NewBuildObject("", nil).WithAuthority(spy, nil)
			_, err := bo.executeObjectTransition(rt, labelMutateBuild, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: "deploying"})
			require.NoError(t, err)
			assert.Equal(t, tc.wantSent, rt.reconcilesSent)
			assert.Empty(t, spy.foreignReads)
		})
	}
}

func TestRestateObjectRuntime_RunnerOperations(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Once()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object("Build", "b2", "Reconcile").Return(mClient).Once()
	mClient.EXPECT().Send(ReconcileBuildRequest{Reason: ReconcileReasonWidgetSync}).Return(mInv).Once()
	rt := restateObjectRuntime{ctx: restate.WithMockContext(mc)}
	sendCrossBuildReconciles(rt, []store.BuildRoute{{BuildID: "b2", RunnerAgent: "agent-alpha"}})
	got, err := rt.RunRunnerGuard(func(context.Context) (runnerGuardResult, error) {
		return runnerGuardResult{Found: true, RunnerAgent: "agent-alpha"}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, runnerGuardResult{Found: true, RunnerAgent: "agent-alpha"}, got)
}

func TestRecordCrossBuildReconcile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		collector bool
		routes    []store.BuildRoute
		want      []store.BuildRoute
	}{
		{name: "no collector drops", routes: []store.BuildRoute{{BuildID: "b2"}}},
		{name: "blank build ignored", collector: true, routes: []store.BuildRoute{{BuildID: " "}}},
		{name: "runner normalized", collector: true, routes: []store.BuildRoute{{BuildID: "b2", RunnerAgent: " Agent-Alpha "}}, want: []store.BuildRoute{{BuildID: "b2", RunnerAgent: "agent-alpha"}}},
		{name: "deduplicated by build", collector: true, routes: []store.BuildRoute{{BuildID: "b2"}, {BuildID: "b3", RunnerAgent: "agent-alpha"}, {BuildID: "b2", RunnerAgent: "agent-beta"}}, want: []store.BuildRoute{{BuildID: "b2", RunnerAgent: "aerial"}, {BuildID: "b3", RunnerAgent: "agent-alpha"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			var collector *crossBuildReconciles
			if tc.collector {
				ctx, collector = withCrossBuildReconciles(ctx)
			}
			for _, route := range tc.routes {
				recordCrossBuildReconcile(ctx, route)
			}
			assert.Equal(t, tc.want, collector.list())
		})
	}
}

func TestBuildObjectRunnerIdentity(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "Build", NewBuildObject("", nil).ServiceName())
	assert.Equal(t, "aerial", (*BuildObject)(nil).runnerAgent())
}

// TestBuildObject_ReconcileWidgetSyncFailedBuild covers the explicit widget
// sync: a failed build refreshes its card only for the widget_sync reason.
func TestBuildObject_ReconcileWidgetSyncFailedBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		reason      string
		buildStatus string
		wantStatus  string
	}{
		{name: "widget sync refreshes failed build", reason: ReconcileReasonWidgetSync, buildStatus: statusFailed, wantStatus: statusFailed},
		{name: "plain reconcile leaves failed build", reason: "active dispatch recovery", buildStatus: statusFailed, wantStatus: statusFailed},
		{name: "widget sync on running build reconciles normally", reason: ReconcileReasonWidgetSync, buildStatus: statusRunning, wantStatus: statusRunning},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &crossBuildSweepSpyStore{
				ownBuild:  &model.VanadisBuild{ID: "b1", Status: tc.buildStatus},
				ownChunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 3}},
			}
			rt := newFakeObjectRuntime("b1")
			exec := executor.NewExecutor("", &mockEriClient{}, &recordingChunkBus{}, "token", "azylman", nil)
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(spy, exec)
			res, err := bo.Reconcile(makeObjectMockContext(t, "b1"), ReconcileBuildRequest{Reason: tc.reason})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, res.Status)
			assert.Empty(t, spy.foreignReads)
		})
	}
}

// TestRouterFacingHelpers pins the build-package helpers the BuildRouter uses
// to resolve a build route before it picks the runner.
func TestRouterFacingHelpers(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "chunk-1-router", ExtractChunkIDFromBranch("aerial", "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3"))
	assert.True(t, CanFallbackThreadRepoLookup(pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1, Event: "opened", Branch: "feat/aerial-pr-20261116-123456", TargetID: "thread-1"}))
}

// attachRunnerSpyStore records the attach request the Build object persists.
type attachRunnerSpyStore struct {
	store.VanadisStore

	attached store.AttachChunkPRRequest
}

func (s *attachRunnerSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	s.attached = req
	return "b1", "b1-chunk-1", "th-1", nil
}

// TestBuildObject_AttachPRStampsOwnRunner proves a standalone build created
// by AttachPR runs on the executing Build object, never on a runner named by
// the request.
func TestBuildObject_AttachPRStampsOwnRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		ownAgent   string
		reqRunner  string
		wantRunner string
	}{
		{name: "default object stamps default runner", wantRunner: "aerial"},
		{name: "sibling object stamps itself", ownAgent: "agent-alpha", wantRunner: "agent-alpha"},
		{name: "forged runner is overwritten", ownAgent: "agent-alpha", reqRunner: "agent-beta", wantRunner: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &attachRunnerSpyStore{}
			rt := newFakeObjectRuntime("b1")
			rt.runStepErr = errors.New("stop after attach")
			bo := NewBuildObjectWithAgent("", tc.ownAgent, nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(spy, nil)
			_, _ = bo.AttachPR(makeObjectMockContext(t, "b1"), store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: "agent-alpha", RunnerAgent: tc.reqRunner})
			assert.Equal(t, tc.wantRunner, spy.attached.RunnerAgent)
			assert.Equal(t, "agent-alpha", spy.attached.Agent)
		})
	}
}
