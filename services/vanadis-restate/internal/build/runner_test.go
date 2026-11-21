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
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// runnerLookupStore answers only the runner guard's build read. Any other
// store call hits the nil embedded interface and panics, which proves a
// refused handler had no side effects.
type runnerLookupStore struct {
	store.VanadisStore

	runner  string
	err     error
	reads   int
	missing bool
}

func (s *runnerLookupStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	s.reads++
	if s.err != nil {
		return nil, nil, s.err
	}
	if s.missing {
		return nil, nil, nil
	}
	return &model.VanadisBuild{ID: buildID, RunnerAgent: s.runner}, nil, nil
}

func TestRunsBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		own    string
		runner string
		want   bool
	}{
		{name: "empty and default compare equal", own: "", runner: "aerial", want: true},
		{name: "default and empty compare equal", own: "aerial", runner: "", want: true},
		{name: "same sibling", own: "agent-alpha", runner: "agent-alpha", want: true},
		{name: "sibling normalized", own: "Agent-Alpha", runner: " agent-alpha ", want: true},
		{name: "default refuses sibling", own: "", runner: "agent-alpha", want: false},
		{name: "sibling refuses default", own: "agent-alpha", runner: "aerial", want: false},
		{name: "siblings differ", own: "agent-alpha", runner: "agent-beta", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, RunsBuild(tc.own, tc.runner))
		})
	}
}

func TestBuildObject_GuardRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		ownAgent string
		key      string
		lookup   *runnerLookupStore
		wantCode restate.Code
		wantErr  bool
	}{
		{name: "default object runs default build", key: "b1", lookup: &runnerLookupStore{runner: "aerial"}},
		{name: "default object runs legacy empty runner", key: "b1", lookup: &runnerLookupStore{runner: ""}},
		{name: "sibling object runs its own build", ownAgent: "agent-alpha", key: "b1", lookup: &runnerLookupStore{runner: "agent-alpha"}},
		{name: "default object refuses sibling build", key: "b1", lookup: &runnerLookupStore{runner: "agent-alpha"}, wantErr: true, wantCode: 409},
		{name: "sibling object refuses default build", ownAgent: "agent-alpha", key: "b1", lookup: &runnerLookupStore{runner: "aerial"}, wantErr: true, wantCode: 409},
		{name: "sibling object refuses other sibling build", ownAgent: "agent-alpha", key: "b1", lookup: &runnerLookupStore{runner: "agent-beta"}, wantErr: true, wantCode: 409},
		{name: "unknown build passes through", ownAgent: "agent-alpha", key: "b1", lookup: &runnerLookupStore{err: store.ErrBuildNotFound}},
		{name: "absent build row passes through", ownAgent: "agent-alpha", key: "b1", lookup: &runnerLookupStore{missing: true}},
		{name: "storage error is retryable", key: "b1", lookup: &runnerLookupStore{err: errors.New("db down")}, wantErr: true},
		{name: "empty key skips lookup", key: "", lookup: &runnerLookupStore{runner: "agent-alpha"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeObjectRuntime(tc.key)
			bo := NewBuildObjectWithAgent("", tc.ownAgent, nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(tc.lookup, nil)
			err := bo.guardRunner(nil, "test")
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var terminal restate.TerminalError
			if tc.wantCode == 0 {
				assert.False(t, errors.As(err, &terminal))
				return
			}
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, tc.wantCode, terminal.Code())
			assert.Contains(t, err.Error(), "build is executed by another runner")
		})
	}
}

func requireForeignRunner(t *testing.T, err error) {
	t.Helper()
	var terminal restate.TerminalError
	require.ErrorAs(t, err, &terminal)
	assert.Equal(t, restate.Code(409), terminal.Code())
}

// TestBuildObject_HandlersRefuseForeignRunner wires the guard into every
// handler except Create: each refuses a build run by another agent with a
// terminal 409 before touching the store, runtime state or widgets.
func TestBuildObject_HandlersRefuseForeignRunner(t *testing.T) {
	t.Parallel()
	lookup := &runnerLookupStore{runner: "agent-alpha"}
	rt := newFakeObjectRuntime("b1")
	bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(lookup, nil)
	ctx := makeObjectMockContext(t, "b1")

	_, err := bo.UpdateBuild(ctx, store.UpdateBuildRequest{Title: "t"})
	requireForeignRunner(t, err)
	_, err = bo.Cancel(ctx, CancelBuildRequest{Reason: "r"})
	requireForeignRunner(t, err)
	_, err = bo.AppendChunks(ctx, AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "c"}}})
	requireForeignRunner(t, err)
	_, err = bo.UpdateChunk(ctx, store.UpdateChunkRequest{ChunkRef: "c1"})
	requireForeignRunner(t, err)
	requireForeignRunner(t, bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"}))
	_, err = bo.FailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "boom"})
	requireForeignRunner(t, err)
	_, err = bo.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 1})
	requireForeignRunner(t, err)
	requireForeignRunner(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1, Event: "deploy_success"}))
	requireForeignRunner(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}))
	_, err = bo.Reconcile(ctx, ReconcileBuildRequest{Reason: ReconcileReasonWidgetSync})
	requireForeignRunner(t, err)
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: "c1"})
	requireForeignRunner(t, err)
	_, err = bo.RetryActionChunk(ctx, store.RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "d1", Reason: "retry"})
	requireForeignRunner(t, err)
	_, err = bo.UpdateActiveStep(ctx, UpdateActiveStepRequest{ChunkID: "c1", ActiveStep: "step"})
	requireForeignRunner(t, err)
	requireForeignRunner(t, bo.DeploymentDeadline(ctx, deploygate.DeploymentDeadlineRequest{ChunkID: "c1"}))

	assert.Equal(t, 14, lookup.reads)
	assert.Empty(t, rt.stepsRun)
	assert.Nil(t, rt.state)
	assert.False(t, rt.cleared)
	assert.False(t, rt.reconcileScheduled)
	assert.Empty(t, rt.reconcilesSent)
}

// createCaptureStore records the build Create persists.
type createCaptureStore struct {
	store.VanadisStore

	created model.VanadisBuild
}

func (s *createCaptureStore) CreateBuild(_ context.Context, b model.VanadisBuild, _ []model.VanadisChunk) error {
	s.created = b
	return nil
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
		{name: "default object runs default build", wantAgent: "aerial", wantRunner: "aerial"},
		{name: "sibling object runs its own build", ownAgent: "Agent-Alpha", wantAgent: "Agent-Alpha", wantRunner: "agent-alpha"},
		{name: "request agent sets owner never runner", ownAgent: "", reqAgent: "agent-beta", wantAgent: "agent-beta", wantRunner: "aerial"},
		{name: "sibling runner keeps foreign owner", ownAgent: "agent-alpha", reqAgent: "agent-beta", wantAgent: "agent-beta", wantRunner: "agent-alpha"},
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
		{name: "sibling runner build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2", RunnerAgent: "agent-alpha"}}, wantSent: []sentReconcile{{Service: "Build-agent-alpha", BuildID: "b2", Reason: "widget_sync"}}},
		{name: "one send per build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2", RunnerAgent: "agent-alpha"}, {ChunkID: "f2", BuildID: "b2", RunnerAgent: "agent-alpha"}, {ChunkID: "g1", BuildID: "b3"}}, wantSent: []sentReconcile{{Service: "Build-agent-alpha", BuildID: "b2", Reason: "widget_sync"}, {Service: "Build", BuildID: "b3", Reason: "widget_sync"}}},
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
	mc.EXPECT().Object("Build-agent-alpha", "b2", "Reconcile").Return(mClient).Once()
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
			exec := executor.NewExecutor("", &mockEriClient{}, &recordingChunkBus{}, "token", nil)
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
	assert.NotEmpty(t, GenerateBuildID())
}
