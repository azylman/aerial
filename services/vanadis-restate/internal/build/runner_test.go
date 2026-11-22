package build

import (
	"context"
	"errors"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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

func TestBuildObject_CreateAcceptsAllOwners(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		ownAgent  string
		reqAgent  string
		wantAgent string
	}{
		{name: "sibling object accepts unowned build", ownAgent: "agent-alpha", wantAgent: "aerial"},
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
		{name: "owner overrides context agent", ctxAgent: "aerial", build: &model.VanadisBuild{Agent: "agent-alpha"}, wantAgent: "agent-alpha"},
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
	return &model.VanadisBuild{ID: buildID, Agent: s.owner}, nil, nil
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
			exec := executor.NewExecutor("", eri, pubsub.NewMemoryBus(), "test-token", "example-owner", nil)
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
		{name: "default runner build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}}},
		{name: "sibling runner build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}}},
		{name: "one send per build", swept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}, {ChunkID: "f2", BuildID: "b2"}, {ChunkID: "g1", BuildID: "b3"}}, wantSent: []sentReconcile{{Service: "Build", BuildID: "b2", Reason: "widget_sync"}, {Service: "Build", BuildID: "b3", Reason: "widget_sync"}}},
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
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object("Build", "b2", "Reconcile").Return(mClient).Once()
	mClient.EXPECT().Send(ReconcileBuildRequest{Reason: ReconcileReasonWidgetSync}).Return(mInv).Once()
	rt := restateObjectRuntime{ctx: restate.WithMockContext(mc)}
	sendCrossBuildReconciles(rt, []store.BuildRoute{{BuildID: "b2"}})
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
		{name: "deduplicated by build", collector: true, routes: []store.BuildRoute{{BuildID: "b2"}, {BuildID: "b3"}, {BuildID: "b2"}}, want: []store.BuildRoute{{BuildID: "b2"}, {BuildID: "b3"}}},
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
			exec := executor.NewExecutor("", &mockEriClient{}, &recordingChunkBus{}, "token", "example-owner", nil)
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

// TestBuildObject_AttachPRPreservesOwner proves a standalone build created
// by AttachPR preserves the agent owner from the request.
func TestBuildObject_AttachPRPreservesOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		reqAgent  string
		wantAgent string
	}{
		{name: "default agent preserved", reqAgent: "aerial", wantAgent: "aerial"},
		{name: "sibling agent preserved", reqAgent: "agent-alpha", wantAgent: "agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &attachRunnerSpyStore{}
			rt := newFakeObjectRuntime("b1")
			rt.runStepErr = errors.New("stop after attach")
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(spy, nil)
			_, _ = bo.AttachPR(makeObjectMockContext(t, "b1"), store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: tc.reqAgent})
			assert.Equal(t, tc.wantAgent, spy.attached.Agent)
		})
	}
}
