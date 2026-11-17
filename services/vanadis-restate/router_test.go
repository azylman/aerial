package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	tests := []struct {
		name        string
		ev          pubsub.PRLifecycleEvent
		db          dbScript
		wantBuildID string
		wantErr     bool
	}{
		{
			name:    "empty repo",
			ev:      pubsub.PRLifecycleEvent{Repo: "", PRNumber: 101},
			wantErr: true,
		},
		{
			name:    "zero pr number",
			ev:      pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 0},
			wantErr: true,
		},
		{
			name:        "primary lookup matched",
			ev:          pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "synchronize"},
			db:          dbScript{row: valuesRow("c1", "b-matched", 0, "medium", false)},
			wantBuildID: "b-matched",
		},
		{
			name: "branch-based fallback resolution",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 102,
				Event:    "opened",
				Branch:   "feat/aerial-pr-vanadis-chunk-1-router-a1b2c3",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				routes: []sqlRoute{{match: "SELECT build_id FROM vanadis_chunks WHERE id = $1", row: valuesRow("b-branch")}},
			},
			wantBuildID: "b-branch",
		},
		{
			name: "thread and repo fallback resolution",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 103,
				Event:    "opened",
				Branch:   "feat/aerial-pr-20261116-123456",
				TargetID: "thread-123",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "b-thread",
		},
		{
			name: "dependabot guard exclusion",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 104,
				Event:    "opened",
				Branch:   "refs/heads/dependabot/npm_and_yarn/foo",
				TargetID: "thread-123",
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "",
		},
		{
			name: "quiet guard exclusion",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 105,
				Event:    "opened",
				Branch:   "feat/something",
				TargetID: "thread-123",
				Quiet:    true,
			},
			db: dbScript{
				row:    errRow(pgx.ErrNoRows),
				chunks: []VanadisChunk{{BuildID: "b-thread", ID: "c1"}},
			},
			wantBuildID: "",
		},
		{
			name: "unmatched no-op routing",
			ev: pubsub.PRLifecycleEvent{
				Repo:     "aerial",
				PRNumber: 106,
				Event:    "opened",
				Branch:   "feat/something",
			},
			db:          dbScript{row: errRow(pgx.ErrNoRows)},
			wantBuildID: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inv := &mockInvoker{}
			store, _ := tc.db.open()
			router := NewBuildRouterService(store, func(restate.Context) buildObjectInvoker { return inv })
			ctx := makeRouterMockContext(t)
			err := router.RoutePREvent(ctx, tc.ev)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantBuildID, inv.lastPRBuildID)
			}
		})
	}
}

func TestExtractChunkIDFromBranch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		repo   string
		branch string
		want   string
	}{
		{name: "empty repo", repo: "", branch: "feat/aerial-pr-vanadis-c1-012345", want: ""},
		{name: "empty branch", repo: "aerial", branch: "", want: ""},
		{name: "standalone branch without vanadis", repo: "aerial", branch: "feat/aerial-pr-20261116-123456", want: ""},
		{name: "valid with refs/heads", repo: "azylman/aerial", branch: "refs/heads/feat/aerial-pr-vanadis-chunk-1-router-a1b2c3", want: "chunk-1-router"},
		{name: "valid bare branch", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-012345", want: "c1"},
		{name: "valid uppercase hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-ABCDEF", want: "c1"},
		{name: "repo containing delimiter and chunk containing delimiter", repo: "owner/demo-pr-vanadis-tools", branch: "feat/demo-pr-vanadis-tools-pr-vanadis-c1-pr-vanadis-2-a1b2c3", want: "c1-pr-vanadis-2"},
		{name: "invalid suffix too short", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345", want: ""},
		{name: "invalid suffix too long", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-1234567", want: ""},
		{name: "invalid suffix non-hex", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1-12345g", want: ""},
		{name: "no separator after vanadis", repo: "aerial", branch: "feat/aerial-pr-vanadis-c1", want: ""},
		{name: "empty chunk id", repo: "aerial", branch: "feat/aerial-pr-vanadis--123456", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, extractChunkIDFromBranch(tc.repo, tc.branch))
		})
	}
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
	assert.Error(t, sNil.ResetPRCheckState(context.Background(), "c1", "sha-1", ""))

	// Empty arguments
	store, _ := dbScript{execTag: "UPDATE 1"}.open()
	assert.Error(t, store.RecordPRCheckStatus(context.Background(), "", "sha-1", "test", "passed"))
	assert.Error(t, store.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "", "passed"))
	assert.Error(t, store.ResetPRCheckState(context.Background(), "", "sha-1", ""))

	// Success
	assert.NoError(t, store.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.NoError(t, store.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.NoError(t, store.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))

	// Skipped transition (0 rows affected)
	storeZero, _ := dbScript{execTag: "UPDATE 0", row: errRow(pgx.ErrNoRows)}.open()
	assert.ErrorIs(t, storeZero.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"), errPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", ""), errPRTransitionSkipped)
	assert.ErrorIs(t, storeZero.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"), errPRTransitionSkipped)

	// DB Exec error
	storeErr, _ := dbScript{execErr: errors.New("db error")}.open()
	assert.Error(t, storeErr.RecordPRCheckStatus(context.Background(), "c1", "sha-1", "test", "passed"))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", ""))
	assert.Error(t, storeErr.ResetPRCheckState(context.Background(), "c1", "sha-2", "merge-sha-1"))
}

func TestPostgresVanadisStore_LookupUnattachedChunkByThreadAndRepo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		threadID  string
		repo      string
		db        dbScript
		wantBuild string
		wantChunk string
		wantErrIs error
		wantErr   bool
	}{
		{name: "nil db", threadID: "123", repo: "aerial", db: dbScript{bare: nilDB}, wantErr: true},
		{name: "empty thread id", threadID: "", repo: "aerial", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "empty repo", threadID: "123", repo: "", db: dbScript{}, wantErrIs: ErrChunkNotFound},
		{name: "query error", threadID: "123", repo: "aerial", db: dbScript{queryErr: errors.New("query failed")}, wantErr: true},
		{name: "scan error", threadID: "123", repo: "aerial", db: dbScript{chunks: []VanadisChunk{{BuildID: "b1", ID: "c1"}}, scanErr: errors.New("scan failed")}, wantErr: true},
		{name: "rows error", threadID: "123", repo: "aerial", db: dbScript{chunks: []VanadisChunk{{BuildID: "b1", ID: "c1"}}, rowsErr: errors.New("rows failed")}, wantErr: true},
		{name: "0 rows not found", threadID: "123", repo: "aerial", db: dbScript{chunks: []VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "1 row unambiguous match", threadID: "123", repo: "aerial", db: dbScript{
			chunks: []VanadisChunk{{BuildID: "b1", ID: "c1"}},
		}, wantBuild: "b1", wantChunk: "c1"},
		{name: "2 rows ambiguous rejection", threadID: "123", repo: "aerial", db: dbScript{
			chunks: []VanadisChunk{{BuildID: "b1", ID: "c1"}, {BuildID: "b1", ID: "c2"}},
		}, wantErrIs: ErrChunkNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			bID, cID, err := store.LookupUnattachedChunkByThreadAndRepo(ctx, tc.threadID, tc.repo)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if bID != tc.wantBuild || cID != tc.wantChunk {
				t.Fatalf("LookupUnattachedChunkByThreadAndRepo = (%q, %q), want (%q, %q)", bID, cID, tc.wantBuild, tc.wantChunk)
			}
		})
	}
}

type unattachedPREventSpyStore struct {
	VanadisStore

	chunks          []VanadisChunk
	getBuildErr     error
	attachErr       error
	attachedChunkID string
	attachedPR      int
	attachedRepo    string
	prStatusUpdated string
	cStatus         string
	banner          string
	failedChunkID   string
	syncWidgetCalls int
}

func (*unattachedPREventSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*VanadisChunkPRLookup, error) {
	return &VanadisChunkPRLookup{Matched: false}, nil
}

func (s *unattachedPREventSpyStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	return &VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, s.chunks, nil
}

func (s *unattachedPREventSpyStore) AttachChunkPR(_ context.Context, req AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attachedChunkID = req.ChunkID
	s.attachedPR = req.PRNumber
	s.attachedRepo = req.Repo
	return "b1", req.ChunkID, "th-1", nil
}

func (s *unattachedPREventSpyStore) UpdatePRLifecycleStatus(_ context.Context, chunkID, status string) error {
	s.prStatusUpdated = chunkID + ":" + status
	return nil
}

func (*unattachedPREventSpyStore) RecordPRCheckStatus(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ResetPRCheckState(_ context.Context, _, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) RecordPRRolloutState(_ context.Context, _, _ string, _ *pubsub.PRRolloutState) error {
	return nil
}

func (s *unattachedPREventSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.cStatus = chunkID + ":" + status
	s.banner = banner
	return nil
}

func (s *unattachedPREventSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	s.syncWidgetCalls++
	return nil
}

func (*unattachedPREventSpyStore) UpdateBuildStatus(_ context.Context, _, _ string) error {
	return nil
}

func (*unattachedPREventSpyStore) ReserveChunkForLaunch(_ context.Context, _ string) error {
	return nil
}

func (s *unattachedPREventSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string) (int, error) {
	s.cStatus = chunkID + ":ci_failed"
	s.banner = banner
	return 1, nil
}

func (s *unattachedPREventSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*VanadisBuild, *VanadisChunk, bool, error) {
	s.failedChunkID = chunkID + ":" + reason
	return &VanadisBuild{ID: "b1", Status: statusFailed, ThreadID: "th-1"}, &VanadisChunk{ID: chunkID, Status: statusFailed}, true, nil
}

func TestBuildObject_OnPREvent_UnattachedPRResolution_Matches(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(1700000000, 0).UTC()
	validGitHubPR := `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"Summary\n\n<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`
	prEv := func(ev, branch, sha, target string, pr int) pubsub.PRLifecycleEvent {
		return pubsub.PRLifecycleEvent{Event: ev, Repo: "external/aerial", PRNumber: pr, Branch: branch, HeadSHA: sha, TargetID: target}
	}
	ch := func(id, status string, pr int, started time.Time) []VanadisChunk {
		return []VanadisChunk{{ID: id, BuildID: "b1", Mode: "pr", Status: status, Repo: "external/aerial", PRNumber: pr, StartedAt: started}}
	}

	tests := []struct {
		name           string
		event          pubsub.PRLifecycleEvent
		chunks         []VanadisChunk
		ghResponse     string
		withoutExec    bool
		wantAttached   string
		wantAttachedPR int
		wantWidgetSync bool
		wantFailed     string
	}{
		{"early opened attached via verified PR", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "chunk-1", 101, true, ""},
		{"fork contributor PR rejected on close", prEv("closed_unmerged", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"closed","merged":false,"author_association":"FIRST_TIME_CONTRIBUTOR","user":{"login":"attacker"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-bad","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":true,"full_name":"attacker/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"untrusted author association rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 103), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"NONE","user":{"login":"stranger"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"missing dispatch trailer rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 104), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"manual PR","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"unstarted chunk rejected before dispatch", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 105), ch("chunk-1", statusRunning, 0, time.Time{}), validGitHubPR, false, "", 0, false, ""},
		{"superseded pr rejected when chunk has different pr", prEv("closed_unmerged", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 200, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"ineligible chunk status rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusCompleted, 0, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"unmatched pr event without branch encoding", prEv("opened", "main", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"target chunk absent in chunks list", prEv("opened", "feat/aerial-pr-vanadis-chunk-99-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"collaborator author accepted with matching sha", prEv("check_updated", "feat/aerial-pr-vanadis-chunk-1-abcdef", "sha-1", "th-1", 101), ch("chunk-1", statusCoding, 0, startedAt), `{"state":"open","merged":false,"author_association":"COLLABORATOR","user":{"login":"aerial-bot"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, ""},
		{"nonencoded branch resolved via unambiguous thread and verified trailer", prEv("ci_failed", "feat/custom-branch", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, ""},
		{"absent event branch resolved via unambiguous thread and verified trailer", prEv("conflict", "", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "chunk-1", 101, true, ""},
		{"nonencoded branch rejected without trailer", prEv("opened", "feat/custom-branch", "sha-1", "th-1", 101), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"manual PR","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"nonencoded branch rejected when parallel chunks ambiguous", prEv("opened", "feat/custom-branch", "sha-1", "th-1", 101), []VanadisChunk{{ID: "chunk-1", BuildID: "b1", Mode: "pr", Status: statusRunning, Repo: "external/aerial", StartedAt: startedAt}, {ID: "chunk-2", BuildID: "b1", Mode: "pr", Status: statusCoding, Repo: "external/aerial", StartedAt: startedAt}}, `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/custom-branch","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"mismatched head repository rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"other/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"mismatched head branch ref rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-2-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"mismatched head sha rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "sha-other", "", 102), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"mismatched event target thread rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "th-wrong", 102), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, false, "", 0, false, ""},
		{"mismatched trailer chunk id rejected", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 102), ch("chunk-1", statusRunning, 0, startedAt), `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"external"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-other\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"external/aerial"}},"base":{"repo":{"full_name":"external/aerial"}}}`, false, "", 0, false, ""},
		{"nil executor refuses unverified attachment", prEv("opened", "feat/aerial-pr-vanadis-chunk-1-abcdef", "", "", 101), ch("chunk-1", statusRunning, 0, startedAt), validGitHubPR, true, "", 0, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &unattachedPREventSpyStore{chunks: tc.chunks}
			var exec *Executor
			if !tc.withoutExec {
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, tc.ghResponse)
				}))
				t.Cleanup(api.Close)
				eri := &mockEriClient{
					widgetFn: func(context.Context, StatusWidgetRequest) (string, error) {
						return "msg-1", nil
					},
				}
				exec = NewExecutor(t.TempDir(), eri, nil, "token", api.Client())
				exec.gitHubAPIURL = api.URL
			}
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			ctx := makeObjectMockContext(t, "b1")

			assert.NoError(t, bo.OnPREvent(ctx, tc.event))
			assert.Equal(t, tc.wantAttached, spy.attachedChunkID)
			assert.Equal(t, tc.wantFailed, spy.failedChunkID)
			if tc.wantAttached != "" {
				assert.Equal(t, tc.wantAttachedPR, spy.attachedPR)
				assert.Equal(t, tc.event.Repo, spy.attachedRepo)
			}
			if tc.wantWidgetSync {
				assert.Positive(t, spy.syncWidgetCalls)
			}
		})
	}
}

func TestBuildObject_OnPREvent_UnattachedPRResolution_Errors(t *testing.T) {
	t.Parallel()

	errBuild := errors.New("build db error")
	errAttach := errors.New("attach generic error")
	startedAt := time.Unix(1700000000, 0).UTC()
	validGitHubPR := `{"state":"open","merged":false,"author_association":"OWNER","user":{"login":"azylman"},"body":"<!-- aerial-pr: {\"target_id\":\"th-1\",\"vanadis_chunk_id\":\"chunk-1\"} -->","head":{"sha":"sha-1","ref":"feat/aerial-pr-vanadis-chunk-1-abcdef","repo":{"fork":false,"full_name":"azylman/aerial"}},"base":{"repo":{"full_name":"azylman/aerial"}}}`

	tests := []struct {
		name        string
		event       pubsub.PRLifecycleEvent
		getBuildErr error
		attachErr   error
		ghStatus    int
		wantErr     bool
	}{
		{
			name: "get build error propagates",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			getBuildErr: errBuild,
			ghStatus:    http.StatusOK,
			wantErr:     true,
		},
		{
			name: "get build not found returns nil",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			getBuildErr: ErrBuildNotFound,
			ghStatus:    http.StatusOK,
			wantErr:     false,
		},
		{
			name: "github verification failure propagates retryable error",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			ghStatus: http.StatusServiceUnavailable,
			wantErr:  true,
		},
		{
			name: "attach chunk conflict swallowed",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			attachErr: ErrBuildConflict,
			ghStatus:  http.StatusOK,
			wantErr:   false,
		},
		{
			name: "attach chunk generic error propagates",
			event: pubsub.PRLifecycleEvent{
				Event:    "opened",
				Repo:     "aerial",
				PRNumber: 101,
				Branch:   "feat/aerial-pr-vanadis-chunk-1-abcdef",
			},
			attachErr: errAttach,
			ghStatus:  http.StatusOK,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.ghStatus)
				fmt.Fprint(w, validGitHubPR)
			}))
			t.Cleanup(api.Close)
			exec := NewExecutor(t.TempDir(), nil, nil, "token", api.Client())
			exec.gitHubAPIURL = api.URL
			spy := &unattachedPREventSpyStore{
				chunks: []VanadisChunk{
					{ID: "chunk-1", BuildID: "b1", Mode: "pr", Status: statusRunning, Repo: "aerial", PRNumber: 0, StartedAt: startedAt},
				},
				getBuildErr: tc.getBuildErr,
				attachErr:   tc.attachErr,
			}
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			ctx := makeObjectMockContext(t, "b1")

			err := bo.OnPREvent(ctx, tc.event)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
