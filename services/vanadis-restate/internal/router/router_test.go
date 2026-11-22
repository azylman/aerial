package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/build"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return nil }

func runMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(mockRunContext{Context: context.Background()})
	if err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

type metadataMutationStore struct {
	store.VanadisStore

	err error
}

func (s *metadataMutationStore) UpdateBuild(_ context.Context, id string, req store.UpdateBuildRequest) (*model.VanadisBuild, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &model.VanadisBuild{ID: id, Title: req.Title}, nil
}

func (s *metadataMutationStore) LookupChunkBuildID(context.Context, string) (store.BuildRoute, error) {
	return store.BuildRoute{}, s.err
}

func (s *metadataMutationStore) LookupPRChunk(context.Context, string, int) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.err
}

func observeJournalCallback(t *testing.T, mc *mocks.MockContext, want error, code restate.Code) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if code != 0 {
			var terminal restate.TerminalError
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
		} else {
			assert.False(t, restate.IsTerminalError(err))
		}
		switch {
		case want != nil && code != 0:
			assert.EqualError(t, err, want.Error())
		case want != nil:
			assert.ErrorIs(t, err, want)
		default:
			assert.NoError(t, err)
		}
		// Only adapt after observing the raw callback error above.
		return runMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

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
				store:          &metadataMutationStore{err: tc.storageErr},
				invokerFactory: func(restate.Context) BuildObjectInvoker { return inv },
			}
			prContext := mocks.NewMockContext(t)
			observeJournalCallback(t, prContext, tc.callbackErr, 0)
			prErr := router.RoutePREvent(restate.WithMockContext(prContext), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1})
			dispatchContext := mocks.NewMockContext(t)
			observeJournalCallback(t, dispatchContext, tc.callbackErr, 0)
			dispatchErr := router.RouteDispatchOutcome(restate.WithMockContext(dispatchContext), pubsub.DispatchOutcomeEvent{VanadisChunkID: "chunk"})
			attachContext := mocks.NewMockContext(t)
			observeJournalCallback(t, attachContext, tc.callbackErr, 0)
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
			_, _ = inv.RouteAttachPR(ctx, "b1", store.AttachChunkPRRequest{PRNumber: 1})
			_, _ = inv.RouteFailChunk(ctx, "b1", model.FailChunkRequest{ChunkID: "c1"})
			require.NoError(t, inv.RouteReconcile(ctx, "b1", build.ReconcileBuildRequest{Reason: build.ReconcileReasonWidgetSync}))
		})
	}
}

type (
	dbScript = sqltest.Script
	sqlRoute = sqltest.Route
)

func open(s dbScript) (*store.Store, *[]string) {
	db, candidates := s.DB()
	return store.New(db, "example-org"), candidates
}

func routerTestMetricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func routerErrorMetricValue(t *testing.T, metric, handler string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	routerTestMetricsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	prefix := fmt.Sprintf(`%s{handler=%q} `, metric, handler)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if valStr, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			require.NoError(t, err)
			return val
		}
	}
	return 0
}

func routerReconcileErrorsCount(t *testing.T, handler string) float64 {
	t.Helper()
	return routerErrorMetricValue(t, "vanadis_restate_reconcile_errors_total", handler)
}

type mockInvoker struct {
	lastPRBuildID        string
	lastDispatchBuildID  string
	lastAttachBuildID    string
	lastFailBuildID      string
	lastFailDispatchID   string
	lastReconcileBuildID string
	lastReconcileReason  string
}

func (m *mockInvoker) RoutePREvent(_ context.Context, buildID string, _ pubsub.PRLifecycleEvent) error {
	m.lastPRBuildID = buildID
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
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()
	return restate.WithMockContext(mc)
}

// routeStore opens a scripted store for a route table row; a nil
// store.VanadisStore row exercises the router's nil-store paths.
func routeStore(db dbScript) store.VanadisStore {
	st, _ := open(db)
	return st
}

// The route tables run serially: wantMetric asserts the exact
// vanadis_restate_reconcile_errors_total delta for the handler, which
// parallel tests hitting the same handler would race.

func TestBuildRouterService_RoutePREvent(t *testing.T) {
	branchChunk := "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3"
	branchTS := "feat/aerial-pr-20261116-123456"
	threadChunks := []model.VanadisChunk{{BuildID: "b-thread", ID: "c1"}}
	tests := []struct {
		name        string
		ev          pubsub.PRLifecycleEvent
		st          store.VanadisStore
		wantBuildID string
		wantErr     bool
		wantMetric  float64
	}{
		{name: "empty repo", ev: pubsub.PRLifecycleEvent{Repo: "", PRNumber: 101}, st: routeStore(dbScript{}), wantErr: true},
		{name: "zero pr number", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 0}, st: routeStore(dbScript{}), wantErr: true},
		{name: "nil store is a no-op", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}},
		{name: "typed nil store normalized to no-op", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}, st: (*store.Store)(nil)},
		{
			name: "primary lookup matched", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			st: routeStore(dbScript{Row: sqltest.ValuesRow("c1", "b-matched", 0, "medium", false)}), wantBuildID: "b-matched",
		},
		{
			name: "primary lookup error records metric", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			st: routeStore(dbScript{Row: sqltest.ErrRow(errors.New("db query error"))}), wantErr: true, wantMetric: 1,
		},
		{
			name: "branch-based fallback resolution", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 102, Event: "opened", Branch: branchChunk},
			st:          routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), Routes: []sqlRoute{{Match: "FROM vanadis_chunks c WHERE c.id = $1", Row: sqltest.ValuesRow("b-branch")}}}),
			wantBuildID: "b-branch",
		},
		{
			name: "thread and repo fallback resolution", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 103, Event: "opened", Branch: branchTS, TargetID: "thread-123"},
			st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), Chunks: threadChunks}), wantBuildID: "b-thread",
		},
		{
			name: "dependabot guard exclusion", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 104, Event: "opened", Branch: "refs/heads/dependabot/npm_and_yarn/foo", TargetID: "thread-123"},
			st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), Chunks: threadChunks}),
		},
		{
			name: "quiet guard exclusion", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 105, Event: "opened", Branch: "feat/something", TargetID: "thread-123", Quiet: true},
			st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), Chunks: threadChunks}),
		},
		{
			name: "unmatched no-op routing", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 106, Event: "opened", Branch: "feat/something"},
			st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}),
		},
		{
			name: "branch-based lookup error propagates", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 107, Event: "opened", Branch: branchChunk},
			st:      routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), Routes: []sqlRoute{{Match: "FROM vanadis_chunks c WHERE c.id = $1", Row: sqltest.ErrRow(errors.New("lookup err"))}}}),
			wantErr: true, wantMetric: 1,
		},
		{
			name: "thread-based lookup error propagates", ev: pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 108, Event: "opened", Branch: branchTS, TargetID: "thread-123"},
			st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows), QueryErr: errors.New("lookup err")}), wantErr: true, wantMetric: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(tc.st, func(restate.Context) BuildObjectInvoker { return inv })
			before := routerReconcileErrorsCount(t, "RoutePREvent")
			err := router.RoutePREvent(makeRouterMockContext(t), tc.ev)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantBuildID, inv.lastPRBuildID)
			assert.Equal(t, before+tc.wantMetric, routerReconcileErrorsCount(t, "RoutePREvent"))
		})
	}
}

func TestBuildRouterService_RouteDispatchOutcome(t *testing.T) {
	tests := []struct {
		name        string
		chunkID     string
		st          store.VanadisStore
		wantBuildID string
		wantErr     bool
		wantMetric  float64
	}{
		{name: "empty chunk id is a no-op", chunkID: "", st: routeStore(dbScript{Row: sqltest.ValuesRow("b-dispatch")})},
		{name: "matched chunk routes to default runner", chunkID: "c1", st: routeStore(dbScript{Row: sqltest.ValuesRow("b-dispatch")}), wantBuildID: "b-dispatch"},
		{name: "chunk not found is a no-op", chunkID: "c-unknown", st: routeStore(dbScript{Row: sqltest.ErrRow(store.ErrChunkNotFound)})},
		{name: "no rows chunk not routed", chunkID: "c1", st: routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)})},
		{name: "nil store is a no-op", chunkID: "c1"},
		{name: "query error records metric", chunkID: "c1", st: routeStore(dbScript{Row: sqltest.ErrRow(errors.New("db query error"))}), wantErr: true, wantMetric: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(tc.st, func(restate.Context) BuildObjectInvoker { return inv })
			before := routerReconcileErrorsCount(t, "RouteDispatchOutcome")
			err := router.RouteDispatchOutcome(makeRouterMockContext(t), pubsub.DispatchOutcomeEvent{VanadisChunkID: tc.chunkID})
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantBuildID, inv.lastDispatchBuildID)
			assert.Equal(t, before+tc.wantMetric, routerReconcileErrorsCount(t, "RouteDispatchOutcome"))
		})
	}
}

func TestBuildRouterService_RouteAttachPR(t *testing.T) {
	prRow := func(buildID string) store.VanadisStore {
		return routeStore(dbScript{Row: sqltest.ValuesRow("c1", buildID, 0, "medium", false)})
	}
	unmatched := routeStore(dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)})
	tests := []struct {
		name        string
		routerAgent string
		st          store.VanadisStore
		req         store.AttachChunkPRRequest
		wantBuildID string // empty: a freshly generated standalone build id
		wantErr     bool
	}{
		{name: "validation failure", st: routeStore(dbScript{Row: sqltest.ValuesRow("b-existing")}), req: store.AttachChunkPRRequest{}, wantErr: true},
		{name: "chunk id resolves existing build", st: routeStore(dbScript{Row: sqltest.ValuesRow("b-existing")}), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}, wantBuildID: "b-existing"},
		{name: "existing build ignores caller agent", st: routeStore(dbScript{Row: sqltest.ValuesRow("b1")}), req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 7, Agent: "agent-alpha"}, wantBuildID: "b1"},
		{name: "repo lookup resolves existing pr", st: prRow("b-repo"), req: store.AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"}, wantBuildID: "b-repo"},
		{name: "existing pr on default runner", routerAgent: "agent-beta", st: prRow("b2"), req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}, wantBuildID: "b2"},
		{name: "chunk not found starts standalone build", st: routeStore(dbScript{Row: sqltest.ErrRow(store.ErrChunkNotFound)}), req: store.AttachChunkPRRequest{PRNumber: 999, Repo: "aerial"}},
		{name: "nil store starts standalone build", req: store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}},
		{name: "new build without caller runs on default agent", st: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}},
		{name: "new build without caller ignores scoped router", routerAgent: "agent-beta", st: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7}},
		{name: "new build runs on default runner", st: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: "agent-alpha"}},
		{name: "new build caller agent normalized", st: unmatched, req: store.AttachChunkPRRequest{Repo: "aerial", PRNumber: 7, Agent: " Agent-Alpha "}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterServiceWithAgent(tc.st, tc.routerAgent, func(restate.Context) BuildObjectInvoker { return inv })
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
	found := routeStore(dbScript{Row: sqltest.ValuesRow("b-fail")})
	tests := []struct {
		name           string
		st             store.VanadisStore
		req            model.FailChunkRequest
		wantBuildID    string
		wantDispatchID string
		wantErr        bool
		wantMetric     float64
	}{
		{name: "validation failure", st: found, req: model.FailChunkRequest{}, wantErr: true},
		{name: "chunk not found", st: routeStore(dbScript{Row: sqltest.ErrRow(store.ErrChunkNotFound)}), req: model.FailChunkRequest{ChunkID: "c-missing", Reason: "boom"}, wantErr: true},
		{name: "nil store is chunk not found", req: model.FailChunkRequest{ChunkID: "c1", Reason: "error"}, wantErr: true},
		{name: "query error records metric", st: routeStore(dbScript{Row: sqltest.ErrRow(errors.New("db query error"))}), req: model.FailChunkRequest{ChunkID: "c1", Reason: "error"}, wantErr: true, wantMetric: 1},
		{name: "legacy request routes to default runner", st: found, req: model.FailChunkRequest{ChunkID: "c1", Reason: "boom"}, wantBuildID: "b-fail"},
		{name: "current generation", st: found, req: model.FailChunkRequest{BuildID: "b-fail", ChunkID: "c1", Reason: "boom", DispatchID: "vanadis-c1-turn-1"}, wantBuildID: "b-fail", wantDispatchID: "vanadis-c1-turn-1"},
		{name: "wrong owner", st: found, req: model.FailChunkRequest{BuildID: "other", ChunkID: "c1", Reason: "boom", DispatchID: "vanadis-c1-turn-1"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := &mockInvoker{}
			router := NewBuildRouterService(tc.st, func(restate.Context) BuildObjectInvoker { return inv })
			before := routerReconcileErrorsCount(t, "RouteFailChunk")
			res, err := router.RouteFailChunk(makeRouterMockContext(t), tc.req)
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, tc.wantBuildID, res.BuildID)
			assert.Equal(t, !tc.wantErr, res.Failed)
			assert.Equal(t, tc.wantBuildID, inv.lastFailBuildID)
			assert.Equal(t, tc.wantDispatchID, inv.lastFailDispatchID)
			assert.Equal(t, before+tc.wantMetric, routerReconcileErrorsCount(t, "RouteFailChunk"))
		})
	}
}

func TestBuildRouterService_RouteBuildReconcile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		buildID     string
		db          dbScript
		wantErr     bool
		wantBuildID string
		wantReason  string
	}{
		{name: "empty build id rejected", buildID: " ", wantErr: true},
		{name: "default runner", buildID: "b1", db: dbScript{BuildRow: &sqltest.Row{Build: &model.VanadisBuild{ID: "b1"}}}, wantBuildID: "b1", wantReason: "widget_sync"},
		{name: "unknown build dropped", buildID: "b3", db: dbScript{BuildRow: sqltest.ErrRow(pgx.ErrNoRows)}},
		{name: "storage error propagates", buildID: "b4", db: dbScript{BuildRow: sqltest.ErrRow(errors.New("db down"))}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			st, _ := open(tc.db)
			router := NewBuildRouterService(st, func(restate.Context) BuildObjectInvoker { return inv })
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
