package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestRouterJournalLookupErrors(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	tests := []struct {
		name        string
		storageErr  error
		callbackErr error
	}{
		{"missing build", store.ErrBuildNotFound, nil},
		{"missing chunk", store.ErrChunkNotFound, nil},
		{"storage retry", transient, transient},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			router := &BuildRouterService{
				store:          &restatetest.MetadataMutationStore{Err: tc.storageErr},
				invokerFactory: func(restate.Context) BuildObjectInvoker { return inv },
			}
			prContext := mocks.NewMockContext(t)
			restatetest.ObserveJournalCallback(t, prContext, tc.callbackErr, 0)
			prErr := router.RoutePREvent(restate.WithMockContext(prContext), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1})
			dispatchContext := mocks.NewMockContext(t)
			restatetest.ObserveJournalCallback(t, dispatchContext, tc.callbackErr, 0)
			dispatchErr := router.RouteDispatchOutcome(restate.WithMockContext(dispatchContext), pubsub.DispatchOutcomeEvent{VanadisChunkID: "chunk"})
			attachContext := mocks.NewMockContext(t)
			restatetest.ObserveJournalCallback(t, attachContext, tc.callbackErr, 0)
			_, attachErr := router.RouteAttachPR(restate.WithMockContext(attachContext), store.AttachChunkPRRequest{ChunkID: "chunk", Repo: "owner/repo", PRNumber: 1})
			if tc.callbackErr == nil {
				assert.NoError(t, prErr)
				assert.NoError(t, dispatchErr)
				assert.NoError(t, attachErr)
			} else {
				assert.Error(t, prErr)
				assert.Error(t, dispatchErr)
				assert.Error(t, attachErr)
			}
		})
	}
}

// Exercise the database classification through the actual persistence callback,
// before the SDK adapter can turn retryable errors into terminal mock results.

func TestBuildRouterService_ReflectionAndInterface(t *testing.T) {
	t.Parallel()
	r := NewBuildRouterService(nil)
	assert.Equal(t, "BuildRouter", r.ServiceName())
	assert.Equal(t, "BuildRouter", (*BuildRouterService)(nil).ServiceName())

	require.NotPanics(t, func() {
		def := restate.Reflect(r)
		require.NotNil(t, def)
	})
}

func TestBuildRouterService_InvokerWiring(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		routerAgent string
		wantService string
	}{
		{name: "routes all handlers to canonical Build", routerAgent: "", wantService: "Build"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			mClient := mocks.NewMockClient(t)
			mInv := mocks.NewMockInvocation(t)
			for _, handler := range []string{"OnPREvent", "OnDispatchOutcome", "AttachPR", "FailChunk", "Reconcile"} {
				mc.EXPECT().Object(tc.wantService, "b1", handler).Return(mClient).Once()
			}
			mClient.EXPECT().Send(mock.Anything).Return(mInv).Times(3)
			mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Times(2)
			r := NewBuildRouterServiceWithAgent(nil, tc.routerAgent)
			inv, ok := r.getInvoker(restate.WithMockContext(mc)).(restateBuildObjectInvoker)
			require.True(t, ok)
			ctx := context.Background()
			require.NoError(t, inv.RoutePREvent(ctx, "b1", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1}))
			require.NoError(t, inv.RouteDispatchOutcome(ctx, "b1", pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))
			_, err := inv.RouteAttachPR(ctx, "b1", store.AttachChunkPRRequest{PRNumber: 1})
			require.NoError(t, err)
			_, err = inv.RouteFailChunk(ctx, "b1", model.FailChunkRequest{ChunkID: "c1"})
			require.NoError(t, err)
			require.NoError(t, inv.RouteReconcile(ctx, "b1", build.ReconcileBuildRequest{Reason: build.ReconcileReasonWidgetSync}))
		})
	}
}

type mockInvoker struct {
	lastPRBuildID        string
	prBuildIDs           []string
	lastDispatchBuildID  string
	lastAttachBuildID    string
	lastFailBuildID      string
	lastFailDispatchID   string
	lastReconcileBuildID string
	lastReconcileReason  string
}

func (m *mockInvoker) RoutePREvent(_ context.Context, buildID string, _ pubsub.PRLifecycleEvent) error {
	m.lastPRBuildID = buildID
	m.prBuildIDs = append(m.prBuildIDs, buildID)
	return nil
}

func (m *mockInvoker) RouteDispatchOutcome(_ context.Context, buildID string, _ pubsub.DispatchOutcomeEvent) error {
	m.lastDispatchBuildID = buildID
	return nil
}

func (m *mockInvoker) RouteAttachPR(_ context.Context, buildID string, _ store.AttachChunkPRRequest) (model.AttachChunkPRResponse, error) {
	m.lastAttachBuildID = buildID
	return model.AttachChunkPRResponse{BuildID: buildID, ChunkID: "c1", Status: "attached"}, nil
}

func (m *mockInvoker) RouteFailChunk(_ context.Context, buildID string, req model.FailChunkRequest) (model.FailChunkResponse, error) {
	m.lastFailBuildID, m.lastFailDispatchID = buildID, req.DispatchID
	return model.FailChunkResponse{BuildID: buildID, ChunkID: req.ChunkID, Failed: true}, nil
}

func (m *mockInvoker) RouteReconcile(_ context.Context, buildID string, req build.ReconcileBuildRequest) error {
	m.lastReconcileBuildID, m.lastReconcileReason = buildID, req.Reason
	return nil
}

func makeRouterMockContext(t *testing.T) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe() // slog handlers look up the span context.
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(restatetest.RunMockRunContext).Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()
	return restate.WithMockContext(mc)
}

// The route tables run serially: wantMetric asserts the exact
// vanadis_restate_reconcile_errors_total delta for the handler, which
// parallel tests hitting the same handler would race. A nil db row exercises
// the router's nil-store paths.

func TestBuildRouterService_RoutePREvent(t *testing.T) {
	branchChunk := "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3"
	branchTS := "feat/aerial-pr-20261116-123456"
	started := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	thread := &dbFixture{
		builds: []model.VanadisBuild{{ID: "b-thread", ThreadID: "thread-123"}},
		chunks: []model.VanadisChunk{{BuildID: "b-thread", ID: "c1", Status: "running", StartedAt: started}},
	}
	tests := []struct {
		name            string
		ev              pubsub.PRLifecycleEvent
		db              *dbFixture
		wantBuildID     string
		wantBuildIDs    []string
		wantBuildPrefix string
		wantErr         bool
		wantMetric      float64
	}{
		{name: "empty repo", ev: pubsub.PRLifecycleEvent{Repo: "", PRNumber: 101}, db: &dbFixture{}, wantErr: true},
		{name: "zero pr number", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 0}, db: &dbFixture{}, wantErr: true},
		{name: "nil store is a no-op", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}},
		{name: "typed nil store normalized to no-op", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}, db: &dbFixture{typedNil: true}},
		{
			name: "primary lookup matched", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			db: &dbFixture{
				builds: []model.VanadisBuild{{ID: "b-matched"}},
				chunks: []model.VanadisChunk{{BuildID: "b-matched", ID: "c1", PRNumber: 101}},
			},
			wantBuildID: "b-matched",
		},
		{
			name: "multi-build fanout in deterministic order",
			ev:   pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 110, Event: "synchronize"},
			db: &dbFixture{
				builds: []model.VanadisBuild{
					{ID: "b-beta", Status: "running"},
					{ID: "b-alpha", Status: "running"},
				},
				chunks: []model.VanadisChunk{
					{BuildID: "b-beta", ID: "c-beta", PRNumber: 110},
					{BuildID: "b-alpha", ID: "c-alpha", PRNumber: 110},
				},
			},
			wantBuildIDs: []string{"b-alpha", "b-beta"},
		},
		{
			name: "primary lookup error records metric", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			db: &dbFixture{closed: true}, wantErr: true, wantMetric: 1,
		},
		{
			name: "branch-based fallback resolution", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 102, Event: "opened", Branch: branchChunk},
			db: &dbFixture{
				builds: []model.VanadisBuild{{ID: "b-branch"}},
				chunks: []model.VanadisChunk{{BuildID: "b-branch", ID: "chunk-1-router"}},
			},
			wantBuildID: "b-branch",
		},
		{
			name: "thread and repo fallback resolution", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 103, Event: "opened", Branch: branchTS, TargetID: "thread-123"},
			db: thread, wantBuildID: "b-thread",
		},
		{
			name: "dependabot guard exclusion", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 104, Event: "opened", Branch: "refs/heads/dependabot/npm_and_yarn/foo", TargetID: "thread-123"},
			db: thread,
		},
		{
			name: "quiet guard exclusion", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 105, Event: "opened", Branch: "feat/something", TargetID: "thread-123", Quiet: true},
			db: thread,
		},
		{
			name: "unmatched no-op routing", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 106, Event: "opened", Branch: "feat/something"},
			db: &dbFixture{},
		},
		{
			name:            "standalone pr event opened creates and routes to new build",
			ev:              pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 109, Event: "opened", Branch: "feat/aerial-pr-t123456789012345678-012345"},
			db:              &dbFixture{},
			wantBuildPrefix: "build-",
		},
		{
			// An integer chunk id cannot compare against the branch chunk id,
			// while the PR lookup (no matching rows) still succeeds.
			name: "branch-based lookup error propagates", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 107, Event: "opened", Branch: branchChunk},
			db:      &dbFixture{sabotage: `ALTER TABLE vanadis_chunks ALTER COLUMN id TYPE integer USING 0`},
			wantErr: true, wantMetric: 1,
		},
		{
			name: "thread-based lookup error propagates", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 108, Event: "opened", Branch: branchTS, TargetID: "thread-123"},
			db: &dbFixture{sabotage: `ALTER TABLE vanadis_builds DROP COLUMN thread_id`}, wantErr: true, wantMetric: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(openFixture(t, tc.db), func(restate.Context) BuildObjectInvoker { return inv })
			before := restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RoutePREvent")
			err := router.RoutePREvent(makeRouterMockContext(t), tc.ev)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			switch {
			case tc.wantBuildPrefix != "":
				assert.True(t, strings.HasPrefix(inv.lastPRBuildID, tc.wantBuildPrefix))
			case len(tc.wantBuildIDs) > 0:
				assert.Equal(t, tc.wantBuildIDs, inv.prBuildIDs)
			default:
				assert.Equal(t, tc.wantBuildID, inv.lastPRBuildID)
			}
			assert.Equal(t, before+tc.wantMetric, restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RoutePREvent"))
		})
	}
}

func TestBuildRouterService_RouteDispatchOutcome(t *testing.T) {
	dispatch := &dbFixture{
		builds: []model.VanadisBuild{{ID: "b-dispatch"}},
		chunks: []model.VanadisChunk{{BuildID: "b-dispatch", ID: "c1"}},
	}
	tests := []struct {
		name        string
		chunkID     string
		db          *dbFixture
		wantBuildID string
		wantErr     bool
		wantMetric  float64
	}{
		{name: "empty chunk id is a no-op", chunkID: "", db: dispatch},
		{name: "matched chunk routes to default runner", chunkID: "c1", db: dispatch, wantBuildID: "b-dispatch"},
		{name: "chunk not found is a no-op", chunkID: "c-unknown", db: dispatch},
		{name: "no rows chunk not routed", chunkID: "c1", db: &dbFixture{}},
		{name: "nil store is a no-op", chunkID: "c1"},
		{name: "query error records metric", chunkID: "c1", db: &dbFixture{closed: true}, wantErr: true, wantMetric: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(openFixture(t, tc.db), func(restate.Context) BuildObjectInvoker { return inv })
			before := restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RouteDispatchOutcome")
			err := router.RouteDispatchOutcome(makeRouterMockContext(t), pubsub.DispatchOutcomeEvent{VanadisChunkID: tc.chunkID})
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantBuildID, inv.lastDispatchBuildID)
			assert.Equal(t, before+tc.wantMetric, restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RouteDispatchOutcome"))
		})
	}
}

func TestBuildRouterService_RouteAttachPR(t *testing.T) {
	chunkIn := func(buildID string) *dbFixture {
		return &dbFixture{
			builds: []model.VanadisBuild{{ID: buildID}},
			chunks: []model.VanadisChunk{{BuildID: buildID, ID: "c1"}},
		}
	}
	prIn := func(buildID string, pr int) *dbFixture {
		return &dbFixture{
			builds: []model.VanadisBuild{{ID: buildID}},
			chunks: []model.VanadisChunk{{BuildID: buildID, ID: "c1", PRNumber: pr}},
		}
	}
	unmatched := &dbFixture{}
	tests := []struct {
		name        string
		routerAgent string
		db          *dbFixture
		req         store.AttachChunkPRRequest
		wantBuildID string // empty: a freshly generated standalone build id
		wantErr     bool
	}{
		{name: "validation failure", db: chunkIn("b-existing"), req: store.AttachChunkPRRequest{}, wantErr: true},
		{name: "chunk id resolves existing build", db: chunkIn("b-existing"), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}, wantBuildID: "b-existing"},
		{name: "existing build ignores caller agent", db: chunkIn("b1"), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 7, Agent: "agent-alpha"}, wantBuildID: "b1"},
		{name: "repo lookup resolves existing pr", db: prIn("b-repo", 101), req: store.AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"}, wantBuildID: "b-repo"},
		{name: "existing pr on default runner", routerAgent: "agent-beta", db: prIn("b2", 7), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantBuildID: "b2"},
		{name: "chunk not found starts standalone build", db: chunkIn("b-existing"), req: store.AttachChunkPRRequest{PRNumber: 999, Repo: "aerial"}},
		{name: "nil store starts standalone build", req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}},
		{name: "new build without caller runs on default agent", db: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}},
		{name: "new build without caller ignores scoped router", routerAgent: "agent-beta", db: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}},
		{name: "new build runs on default runner", db: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: "agent-alpha"}},
		{name: "new build caller agent normalized", db: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: " Agent-Alpha "}},
		{name: "unknown chunk id falls through to standalone build", db: chunkIn("b-existing"), req: store.AttachChunkPRRequest{ChunkID: "c-missing", PRNumber: 7, Repo: "aerial"}},
		{name: "chunk lookup error propagates", db: &dbFixture{closed: true}, req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 7}, wantErr: true},
		{name: "pr lookup error propagates", db: &dbFixture{closed: true}, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterServiceWithAgent(openFixture(t, tc.db), tc.routerAgent, func(restate.Context) BuildObjectInvoker { return inv })
			res, err := router.RouteAttachPR(makeRouterMockContext(t), tc.req)
			if tc.wantErr {
				require.Error(t, err)
				assert.Empty(t, inv.lastAttachBuildID)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, res.BuildID)
			assert.Equal(t, res.BuildID, inv.lastAttachBuildID)
			if tc.wantBuildID != "" {
				assert.Equal(t, tc.wantBuildID, res.BuildID)
			}
		})
	}
}

func TestBuildRouterService_RouteFailChunk(t *testing.T) {
	found := &dbFixture{
		builds: []model.VanadisBuild{{ID: "b-fail"}},
		chunks: []model.VanadisChunk{{BuildID: "b-fail", ID: "c1"}},
	}
	tests := []struct {
		name           string
		db             *dbFixture
		req            model.FailChunkRequest
		wantBuildID    string
		wantDispatchID string
		wantErr        bool
		wantMetric     float64
	}{
		{name: "validation failure", db: found, req: model.FailChunkRequest{}, wantErr: true},
		{name: "chunk not found", db: found, req: model.FailChunkRequest{ChunkID: "c-missing", Reason: "boom"}, wantErr: true},
		{name: "nil store is chunk not found", req: model.FailChunkRequest{ChunkID: "c1", Reason: "error"}, wantErr: true},
		{name: "query error records metric", db: &dbFixture{closed: true}, req: model.FailChunkRequest{ChunkID: "c1", Reason: "error"}, wantErr: true, wantMetric: 1},
		{name: "legacy request routes to default runner", db: found, req: model.FailChunkRequest{ChunkID: "c1", Reason: "boom"}, wantBuildID: "b-fail"},
		{name: "current generation", db: found, req: model.FailChunkRequest{BuildID: "b-fail", ChunkID: "c1", Reason: "boom", DispatchID: "vanadis-c1-turn-1"}, wantBuildID: "b-fail", wantDispatchID: "vanadis-c1-turn-1"},
		{name: "wrong owner", db: found, req: model.FailChunkRequest{BuildID: "other", ChunkID: "c1", Reason: "boom", DispatchID: "vanadis-c1-turn-1"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(openFixture(t, tc.db), func(restate.Context) BuildObjectInvoker { return inv })
			before := restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RouteFailChunk")
			res, err := router.RouteFailChunk(makeRouterMockContext(t), tc.req)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantBuildID, res.BuildID)
			assert.Equal(t, !tc.wantErr, res.Failed)
			assert.Equal(t, tc.wantBuildID, inv.lastFailBuildID)
			assert.Equal(t, tc.wantDispatchID, inv.lastFailDispatchID)
			assert.Equal(t, before+tc.wantMetric, restatetest.MetricValue(t, "vanadis_restate_reconcile_errors_total", "RouteFailChunk"))
		})
	}
}

func TestBuildRouterService_RouteBuildReconcile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		buildID     string
		db          *dbFixture
		wantErr     bool
		wantBuildID string
		wantReason  string
	}{
		{name: "empty build id rejected", buildID: " ", db: &dbFixture{}, wantErr: true},
		{name: "default runner", buildID: "b1", db: &dbFixture{builds: []model.VanadisBuild{{ID: "b1"}}}, wantBuildID: "b1", wantReason: "widget_sync"},
		{name: "unknown build dropped", buildID: "b3", db: &dbFixture{builds: []model.VanadisBuild{{ID: "b1"}}}},
		{name: "storage error propagates", buildID: "b4", db: &dbFixture{closed: true}, wantErr: true},
		{name: "nil store drops reconcile", buildID: "b1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			router := NewBuildRouterService(openFixture(t, tc.db), func(restate.Context) BuildObjectInvoker { return inv })
			err := router.RouteBuildReconcile(makeRouterMockContext(t), tc.buildID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantBuildID, inv.lastReconcileBuildID)
			assert.Equal(t, tc.wantReason, inv.lastReconcileReason)
		})
	}
}
