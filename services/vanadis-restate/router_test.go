package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

type mockInvoker struct {
	prEventFn         func(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error
	dispatchOutcomeFn func(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error
	attachPRFn        func(ctx context.Context, buildID string, req AttachChunkPRRequest) (AttachChunkPRResponse, error)
	failChunkFn       func(ctx context.Context, buildID string, req FailChunkRequest) (FailChunkResponse, error)

	lastPRBuildID       string
	lastDispatchBuildID string
	lastAttachBuildID   string
	lastFailBuildID     string
}

func (m *mockInvoker) RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error {
	m.lastPRBuildID = buildID
	if m.prEventFn != nil {
		return m.prEventFn(ctx, buildID, ev)
	}
	return nil
}

func (m *mockInvoker) RouteDispatchOutcome(ctx context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) error {
	m.lastDispatchBuildID = buildID
	if m.dispatchOutcomeFn != nil {
		return m.dispatchOutcomeFn(ctx, buildID, ev)
	}
	return nil
}

func (m *mockInvoker) RouteAttachPR(ctx context.Context, buildID string, req AttachChunkPRRequest) (AttachChunkPRResponse, error) {
	m.lastAttachBuildID = buildID
	if m.attachPRFn != nil {
		return m.attachPRFn(ctx, buildID, req)
	}
	return AttachChunkPRResponse{BuildID: buildID, ChunkID: "c1", Status: "attached"}, nil
}

func (m *mockInvoker) RouteFailChunk(ctx context.Context, buildID string, req FailChunkRequest) (FailChunkResponse, error) {
	m.lastFailBuildID = buildID
	if m.failChunkFn != nil {
		return m.failChunkFn(ctx, buildID, req)
	}
	return FailChunkResponse{BuildID: buildID, ChunkID: req.ChunkID, Failed: true}, nil
}

type routerMockRunContext struct {
	context.Context
}

func (routerMockRunContext) Log() *slog.Logger         { return slog.Default() }
func (routerMockRunContext) Request() *restate.Request { return nil }

func runRouterMockRunContext(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
	res, err := fn(routerMockRunContext{Context: context.Background()})
	if err != nil {
		return restate.TerminalErrorf("%v", err)
	}
	if strPtr, ok := output.(*string); ok && res != nil {
		if s, sOk := res.(string); sOk {
			*strPtr = s
		}
	}
	return nil
}

func makeRouterMockContext(t *testing.T) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runRouterMockRunContext).Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()
	return restate.WithMockContext(mc)
}

func TestBuildRouterService_ReflectionAndInterface(t *testing.T) {
	t.Parallel()
	r := NewBuildRouterService(nil)
	assert.Equal(t, "BuildRouter", r.ServiceName())

	require.NotPanics(t, func() {
		def := restate.Reflect(r)
		require.NotNil(t, def)
	})
}

func TestBuildRouterService_RoutePREvent(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	store, _ := dbScript{row: valuesRow("c1", "b-matched", 0, "medium", false)}.open()
	router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Invalid input
	err := router.RoutePREvent(ctx, pubsub.PRLifecycleEvent{})
	assert.Error(t, err)

	// Matched PR
	err = router.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"})
	assert.NoError(t, err)
	assert.Equal(t, "b-matched", inv.lastPRBuildID)

	// Unmatched opened PR -> generates build ID
	storeUnmatched, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	routerUnmatched := NewBuildRouterService(storeUnmatched, func(restate.Context) buildObjectInvoker { return inv })
	err = routerUnmatched.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 202, Event: "opened"})
	assert.NoError(t, err)
	assert.True(t, inv.lastPRBuildID != "")

	// Unmatched closed PR -> returns nil without routing
	inv.lastPRBuildID = ""
	err = routerUnmatched.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 202, Event: "closed"})
	assert.NoError(t, err)
	assert.Empty(t, inv.lastPRBuildID)
}

func TestBuildRouterService_RouteDispatchOutcome(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	store, _ := dbScript{row: valuesRow("b-dispatch")}.open()
	router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Empty chunk id -> no-op
	err := router.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{})
	assert.NoError(t, err)

	// Matched chunk
	err = router.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "b-dispatch", inv.lastDispatchBuildID)

	// Unmatched chunk -> no-op
	storeNotFound, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	routerNotFound := NewBuildRouterService(storeNotFound, func(restate.Context) buildObjectInvoker { return inv })
	inv.lastDispatchBuildID = ""
	err = routerNotFound.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c-unknown"})
	assert.NoError(t, err)
	assert.Empty(t, inv.lastDispatchBuildID)
}

func TestBuildRouterService_RouteAttachPR(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	store, _ := dbScript{row: valuesRow("b-existing")}.open()
	router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Validation failure
	_, err := router.RouteAttachPR(ctx, AttachChunkPRRequest{})
	assert.Error(t, err)

	// Route attach with chunk id
	res, err := router.RouteAttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-existing", res.BuildID)

	// Route attach without chunk id, but with repo lookup returning build id
	storeRepo, _ := dbScript{row: valuesRow("c1", "b-repo", 0, "medium", false)}.open()
	routerRepo := NewBuildRouterService(storeRepo, func(restate.Context) buildObjectInvoker { return inv })
	resRepo, err := routerRepo.RouteAttachPR(ctx, AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-repo", resRepo.BuildID)

	// Route attach standalone new build id
	storeUnmatched, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	routerUnmatched := NewBuildRouterService(storeUnmatched, func(restate.Context) buildObjectInvoker { return inv })
	resNew, err := routerUnmatched.RouteAttachPR(ctx, AttachChunkPRRequest{PRNumber: 999, Repo: "aerial"})
	assert.NoError(t, err)
	assert.True(t, resNew.BuildID != "")
}

func TestBuildRouterService_RouteFailChunk(t *testing.T) {
	t.Parallel()
	inv := &mockInvoker{}
	store, _ := dbScript{row: valuesRow("b-fail")}.open()
	router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })

	ctx := makeRouterMockContext(t)

	// Validation failure
	_, err := router.RouteFailChunk(ctx, FailChunkRequest{})
	assert.Error(t, err)

	// Chunk not found
	storeNotFound, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	routerNotFound := NewBuildRouterService(storeNotFound, func(restate.Context) buildObjectInvoker { return inv })
	_, err = routerNotFound.RouteFailChunk(ctx, FailChunkRequest{ChunkID: "c-missing", Reason: "boom"})
	assert.Error(t, err)

	tests := []struct {
		name, buildID, dispatchID string
		wantErr                   bool
	}{
		{"legacy", "", "", false},
		{"current generation", "b-fail", "vanadis-c1-turn-1", false},
		{"wrong owner", "other", "vanadis-c1-turn-1", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })
			inv.failChunkFn = func(_ context.Context, buildID string, req FailChunkRequest) (FailChunkResponse, error) {
				assert.Equal(t, tc.dispatchID, req.DispatchID)
				return FailChunkResponse{BuildID: buildID, Failed: true}, nil
			}
			res, err := router.RouteFailChunk(makeRouterMockContext(t), FailChunkRequest{BuildID: tc.buildID, ChunkID: "c1", Reason: "boom", DispatchID: tc.dispatchID})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, "b-fail", res.BuildID)
				assert.True(t, res.Failed)
			}
		})
	}
}

func TestRestateBuildObjectInvoker(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)

	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Request(mock.Anything, mock.Anything).Return(nil).Maybe()

	inv := restateBuildObjectInvoker{ctx: restate.WithMockContext(mc)}

	ctx := context.Background()
	_ = inv.RoutePREvent(ctx, "b1", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 1})
	_ = inv.RouteDispatchOutcome(ctx, "b1", pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	_, _ = inv.RouteAttachPR(ctx, "b1", AttachChunkPRRequest{PRNumber: 1})
	_, _ = inv.RouteFailChunk(ctx, "b1", FailChunkRequest{ChunkID: "c1"})
}

func TestBuildRouterService_NilStoreAndErrorPaths(t *testing.T) {
	rNil := NewBuildRouterService(nil)
	ctx := makeRouterMockContext(t)

	// RoutePREvent nil store
	assert.NoError(t, rNil.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "opened"}))

	// RoutePREvent query error records metric inside journaled callback
	storeErr, _ := dbScript{row: errRow(errors.New("db query error"))}.open()
	rErr := NewBuildRouterService(storeErr)
	beforePR := reconcileErrorsCount(t, labelRoutePREvent)
	assert.Error(t, rErr.RoutePREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"}))
	assert.Equal(t, beforePR+1, reconcileErrorsCount(t, labelRoutePREvent))

	// RouteDispatchOutcome nil store
	assert.NoError(t, rNil.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))

	// RouteDispatchOutcome query error records metric inside journaled callback
	beforeDisp := reconcileErrorsCount(t, labelRouteDispatchOutcome)
	assert.Error(t, rErr.RouteDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"}))
	assert.Equal(t, beforeDisp+1, reconcileErrorsCount(t, labelRouteDispatchOutcome))

	// RouteAttachPR nil store generates build ID
	_, err := rNil.RouteAttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)

	// RouteAttachPR standalone matching existing PR
	storeAttach, _ := dbScript{row: valuesRow("c-new", "b-new", 0, "medium", false)}.open()
	inv := &mockInvoker{}
	rStandalone := NewBuildRouterService(storeAttach, func(restate.Context) buildObjectInvoker { return inv })
	resp, err := rStandalone.RouteAttachPR(ctx, AttachChunkPRRequest{PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "b-new", resp.BuildID)

	// RouteFailChunk nil store
	_, err = rNil.RouteFailChunk(ctx, FailChunkRequest{ChunkID: "c1", Reason: "error"})
	assert.Error(t, err)

	// RouteFailChunk query error records metric inside journaled callback
	beforeFail := reconcileErrorsCount(t, labelRouteFailChunk)
	_, err = rErr.RouteFailChunk(ctx, FailChunkRequest{ChunkID: "c1", Reason: "error"})
	assert.Error(t, err)
	assert.Equal(t, beforeFail+1, reconcileErrorsCount(t, labelRouteFailChunk))
}

func TestPostgresVanadisStore_RecordPRCheckStatus(t *testing.T) {
	t.Parallel()

	// Nil store
	var sNil *PostgresVanadisStore
	assert.Error(t, sNil.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, sNil.ResetPRCheckState(context.Background(), "c1", "b1", "sha-1", ""))

	// Empty arguments
	store, _ := dbScript{execTag: "UPDATE 1"}.open()
	assert.Error(t, store.RecordPRCheckStatus(context.Background(), "", "sha-1", "test", "passed"))
	assert.Error(t, store.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "", "passed"))
	assert.Error(t, store.ResetPRCheckState(context.Background(), "", "b1", "sha-1", ""))

	// Success
	assert.NoError(t, store.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.NoError(t, store.ResetPRCheckState(context.Background(), "c1", "b1", "sha-2", ""))
	assert.NoError(t, store.ResetPRCheckState(context.Background(), "c1", "b1", "sha-2", "merge-sha-1"))

	// Not found
	storeZero, _ := dbScript{execTag: "UPDATE 0"}.open()
	assert.ErrorIs(t, storeZero.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"), ErrChunkNotFound)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "b1", "sha-2", ""), ErrChunkNotFound)

	// DB Exec error
	storeErr, _ := dbScript{execErr: errors.New("db error")}.open()
	assert.Error(t, storeErr.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "b1", "sha-2", ""))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "b1", "sha-2", "merge-sha-1"))
}
