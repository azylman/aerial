package build

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"reflect"
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

type fakeObjectRuntime struct {
	key                string
	launched           map[string]bool
	state              *model.BuildObjectState
	getStateErr        error
	getLaunchedErr     error
	runStepErr         error
	stepsRun           []string
	cleared            bool
	reconcileScheduled bool
	reconcilesSent     []sentReconcile
	invocationID       string
	deferredSends      []deferredSend
	runnerGuardCalled  bool
	runnerGuardErr     error
}

// deferredSend records one delayed deferral Reconcile.
type deferredSend struct {
	Delay time.Duration
	Seq   int
}

// sentReconcile records one cross-build Reconcile send.
type sentReconcile struct {
	Service string
	BuildID string
	Reason  string
}

func newFakeObjectRuntime(key string) *fakeObjectRuntime {
	return &fakeObjectRuntime{
		key: key,
	}
}

func (f *fakeObjectRuntime) Key() string {
	return f.key
}

func (f *fakeObjectRuntime) GetLaunched() (map[string]bool, error) {
	if f.getLaunchedErr != nil {
		return nil, f.getLaunchedErr
	}
	if f.launched == nil {
		return nil, nil
	}
	cp := make(map[string]bool, len(f.launched))
	maps.Copy(cp, f.launched)
	return cp, nil
}

func (f *fakeObjectRuntime) SetLaunched(launched map[string]bool) {
	f.launched = make(map[string]bool, len(launched))
	maps.Copy(f.launched, launched)
}

func (f *fakeObjectRuntime) GetState() (*model.BuildObjectState, error) {
	if f.getStateErr != nil {
		return nil, f.getStateErr
	}
	if f.state == nil {
		return nil, nil
	}
	cp := *f.state
	if f.state.Launched != nil {
		cp.Launched = make(map[string]bool, len(f.state.Launched))
		maps.Copy(cp.Launched, f.state.Launched)
	}
	if f.state.Chunks != nil {
		cp.Chunks = make([]model.VanadisChunk, len(f.state.Chunks))
		copy(cp.Chunks, f.state.Chunks)
	}
	return &cp, nil
}

func (f *fakeObjectRuntime) SetState(state model.BuildObjectState) {
	cp := state
	if state.Launched != nil {
		cp.Launched = make(map[string]bool, len(state.Launched))
		maps.Copy(cp.Launched, state.Launched)
	}
	if state.Chunks != nil {
		cp.Chunks = make([]model.VanadisChunk, len(state.Chunks))
		copy(cp.Chunks, state.Chunks)
	}
	f.state = &cp
}

func (f *fakeObjectRuntime) ClearState() {
	f.cleared = true
	f.launched = make(map[string]bool)
	f.state = nil
}

func (f *fakeObjectRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	f.stepsRun = append(f.stepsRun, name)
	if f.runStepErr != nil {
		return reconcileStepOutput{}, f.runStepErr
	}
	return fn(context.Background())
}

func TestBuildObject_ReflectionAndInterface(t *testing.T) {
	t.Parallel()
	bo := NewBuildObject("http://vanadis:4025", nil)
	assert.Equal(t, "Build", bo.ServiceName())

	require.NotPanics(t, func() {
		def := restate.Reflect(bo)
		require.NotNil(t, def)
	})
}

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

func makeObjectMockContext(t *testing.T, key string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe() // slog handlers look up the span context.
	mc.EXPECT().Key().Return(key).Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-test"}).Maybe()
	mc.EXPECT().Get("state", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get("launched", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get(deploygate.PendingDeploymentKey, mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()
	mc.EXPECT().Set("state", mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().Set("launched", mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().ClearAll().Return().Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Send(mock.Anything, mock.Anything).Return(mInv).Maybe()
	return restate.WithMockContext(mc)
}

func TestToTerminalErr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantNil  bool
		wantCode restate.Code
	}{
		{name: "nil error", err: nil, wantNil: true},
		{name: "invalid build input", err: store.ErrInvalidBuildInput, wantCode: 400},
		{name: "build not found", err: store.ErrBuildNotFound, wantCode: 404},
		{name: "chunk not found", err: ErrChunkNotFound, wantCode: 404},
		{name: "build conflict", err: store.ErrBuildConflict, wantCode: 409},
		{name: "unknown error", err: errors.New("something exploded"), wantCode: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ToTerminalErr(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			var term restate.TerminalError
			if tc.wantCode > 0 {
				require.True(t, errors.As(got, &term))
				assert.Equal(t, tc.wantCode, term.Code())
			} else {
				assert.False(t, errors.As(got, &term))
			}
		})
	}
}

func TestBuildObject_AuthoritativeHandlers(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "test-token", "azylman", nil)
	b1 := model.VanadisBuild{ID: "b1", Title: "Build 1", Objective: "Obj 1", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b1", Title: "Chunk 1", Instructions: "Inst 1", Status: "pending", Mode: "pr", Repo: "aerial", TargetFiles: []string{"pkg/a.go"}}
	c2 := model.VanadisChunk{ID: "c2", BuildID: "b1", Title: "Chunk 2", Instructions: "Inst 2", Status: "pending", Mode: "pr", Repo: "aerial", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}}
	st, _ := dbScript{
		routes: []sqlRoute{
			{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
			{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("pr")},
			{match: "SELECT build_id FROM vanadis_chunks", row: valuesRow("b1")},
			{match: "RETURNING b.id, c.id, b.thread_id", row: valuesRow("b1", "c1", "th-1")},
			{match: "RETURNING ci_fail_count", row: valuesRow(1)},
			{match: "WHERE c.id = $1", row: failRow("running", "running")},
			{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b1", 0, "medium", true)},
			{match: "MAX(chunk_index)", row: valuesRow(1)},
		},
		row:      valuesRow("running"),
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1, c2},
		countRow: valuesRow(1),
		execTag:  "UPDATE 1",
	}.open()

	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	// 1. Create validation failure
	ctx := makeObjectMockContext(t, "b1")
	_, err := bo.Create(ctx, CreateBuildRequest{})
	assert.Error(t, err)

	// 2. Create success
	createReq := CreateBuildRequest{
		ID:        "b1",
		ThreadID:  "th-1",
		Objective: "Test build",
		Chunks: []store.CreateChunkInput{
			{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do 1", DependsOn: []string{}},
		},
	}
	bld, err := bo.Create(ctx, createReq)
	assert.NoError(t, err)
	assert.Equal(t, "b1", bld.ID)

	// 3. UpdateBuild
	upReq := store.UpdateBuildRequest{Title: "New Title", Objective: "New Obj"}
	bldUp, err := bo.UpdateBuild(ctx, upReq)
	assert.NoError(t, err)
	assert.Equal(t, "b1", bldUp.ID)

	// 4. UpdateChunk
	cUp, err := bo.UpdateChunk(ctx, store.UpdateChunkRequest{ChunkRef: "c1", Title: "Updated Title"})
	assert.NoError(t, err)
	assert.Equal(t, "c1", cUp.ID)

	// 5. AppendChunks
	appReq := AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "Chunk 3", Repo: "aerial", Instructions: "Do 3", DependsOn: []string{"c2"}, TargetFiles: []string{"pkg/c.go"}}}}
	appended, err := bo.AppendChunks(ctx, appReq)
	assert.NoError(t, err)
	assert.NotEmpty(t, appended)

	// 6. AttachPR
	attReq := store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}
	attResp, err := bo.AttachPR(ctx, attReq)
	assert.NoError(t, err)
	assert.Equal(t, "attached", attResp.Status)

	// 7. FailChunk
	failReq := model.FailChunkRequest{ChunkID: "c1", Reason: "unrecoverable error"}
	failResp, err := bo.FailChunk(ctx, failReq)
	assert.NoError(t, err)
	assert.Equal(t, "c1", failResp.ChunkID)

	// 8. RemoveChunk
	remErr := bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	assert.NoError(t, remErr)

	// 9. OnPREvent
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", CheckName: "lint", CheckURL: "https://github.com/...", Details: "failure", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "conflict"}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "merged"}))
	assert.NoError(t, bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "closed"}))

	// 10. OnDispatchOutcome
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}))
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "crashed"}))
	assert.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{}))

	boNil := NewBuildObject("", nil)
	assert.NoError(t, boNil.OnPREvent(ctx, pubsub.PRLifecycleEvent{}))
	assert.NoError(t, boNil.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{}))

	// 11. Cancel
	canBld, err := bo.Cancel(ctx, CancelBuildRequest{Reason: "user cancelled"})
	assert.NoError(t, err)
	assert.Equal(t, "b1", canBld.ID)
}

func TestBuildObject_HandlerErrors(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")
	st, _ := dbScript{row: errRow(ErrChunkNotFound)}.open()
	bo := NewBuildObject("", nil).WithAuthority(st, nil)

	// UpdateBuild empty
	_, err := bo.UpdateBuild(ctx, store.UpdateBuildRequest{})
	assert.Error(t, err)

	// UpdateChunk empty ref
	_, err = bo.UpdateChunk(ctx, store.UpdateChunkRequest{})
	assert.Error(t, err)

	// AppendChunks empty
	_, err = bo.AppendChunks(ctx, AppendChunksRequest{})
	assert.Error(t, err)

	// RemoveChunk empty ref
	err = bo.RemoveChunk(ctx, RemoveChunkRequest{})
	assert.Error(t, err)

	// FailChunk empty
	_, err = bo.FailChunk(ctx, model.FailChunkRequest{})
	assert.Error(t, err)

	// Create with empty inputs
	_, err = bo.Create(ctx, CreateBuildRequest{})
	assert.Error(t, err)
}

func TestRestateObjectRuntime_SetLaunchedNil(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Set("launched", map[string]bool{}, mock.Anything).Return().Once()
	mc.EXPECT().Key().Return("b1").Once()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object("Build", "b1", "Reconcile").Return(mClient).Once()
	mClient.EXPECT().Send(ReconcileBuildRequest{Reason: "active dispatch recovery"}, restate.WithDelay(time.Second)).Return(mInv).Once()
	ctx := restate.WithMockContext(mc)
	rt := restateObjectRuntime{ctx: ctx}
	rt.SetLaunched(nil)
	rt.ScheduleReconcile()
}

func TestBuildObject_CreateAndCancelErrors(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")

	// Create with empty key and empty ID
	ctxEmptyKey := makeObjectMockContext(t, "")
	bo := NewBuildObject("", nil)
	_, err := bo.Create(ctxEmptyKey, CreateBuildRequest{ID: ""})
	assert.Error(t, err)

	// Create with chunk with empty ID (generates auto ID)
	ctxAuto := makeObjectMockContext(t, "b-auto")
	storeCreate, _ := dbScript{routes: []sqlRoute{
		{match: "FROM vanadis_builds WHERE id = $1", row: &mockRow{b: &model.VanadisBuild{ID: "b-auto", Status: "running"}}},
	}}.open()
	boCreate := NewBuildObject("", nil).WithAuthority(storeCreate, nil)
	validNoChunkIDReq := CreateBuildRequest{
		ID:        "b-auto",
		ThreadID:  "th1",
		Objective: "Test Auto Chunk ID",
		Chunks: []store.CreateChunkInput{
			{Title: "C1", Instructions: "I", Repo: "aerial", DependsOn: []string{}},
		},
	}
	bAuto, err := boCreate.Create(ctxAuto, validNoChunkIDReq)
	assert.NoError(t, err)
	assert.Equal(t, "b-auto", bAuto.ID)

	// Create with empty objective
	_, err = bo.Create(ctx, CreateBuildRequest{ID: "b1", ThreadID: "th1", Objective: ""})
	assert.Error(t, err)

	// Create with cycle in DAG
	cycleReq := CreateBuildRequest{
		ID:        "b1",
		ThreadID:  "th1",
		Objective: "Test Cycle",
		Chunks: []store.CreateChunkInput{
			{ID: "c1", Title: "C1", Instructions: "I", Repo: "aerial", DependsOn: []string{"c2"}},
			{ID: "c2", Title: "C2", Instructions: "I", Repo: "aerial", DependsOn: []string{"c1"}},
		},
	}
	_, err = bo.Create(ctx, cycleReq)
	assert.Error(t, err)

	// Cancel terminal builds
	bComp := model.VanadisBuild{ID: "b1", Status: "completed"}
	storeCompleted, _ := dbScript{buildRow: &mockRow{b: &bComp}}.open()
	rtCompleted := newFakeObjectRuntime("b1")
	boCompleted := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rtCompleted }).WithAuthority(storeCompleted, nil)
	_, err = boCompleted.Cancel(ctx, CancelBuildRequest{})
	assert.Error(t, err)
	assert.False(t, rtCompleted.cleared)

	bFail := model.VanadisBuild{ID: "b1", Status: "failed"}
	storeFailed, _ := dbScript{buildRow: &mockRow{b: &bFail}, execTag: "UPDATE 1"}.open()
	rtFailed := newFakeObjectRuntime("b1")
	boFailed := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rtFailed }).WithAuthority(storeFailed, nil)
	canFailed, err := boFailed.Cancel(ctx, CancelBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusCancelled, canFailed.Status)
	assert.True(t, rtFailed.cleared)

	bCancelled := model.VanadisBuild{ID: "b1", Status: statusCancelled}
	storeCancelled, _ := dbScript{buildRow: &mockRow{b: &bCancelled}}.open()
	rtCancelled := newFakeObjectRuntime("b1")
	boCancelled := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rtCancelled }).WithAuthority(storeCancelled, nil)
	_, err = boCancelled.Cancel(ctx, CancelBuildRequest{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, store.ErrBuildConflict.Error())
	assert.False(t, rtCancelled.cleared)
}

func TestBuildObject_HandlersWithAuthorityAndMockContext(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-auth", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-auth", ChunkIndex: 0, Title: "C1", Status: "pending", Mode: "action"}
	st, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []model.VanadisChunk{c1},
		routes: []sqlRoute{
			{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("action")},
			{match: "UPDATE vanadis_chunks", row: valuesRow("b-auth", "c1", "th-1")},
			{match: "WHERE c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b-auth", 0, "medium", false)},
			{match: "SELECT id, build_id, chunk_index, mode, title, status", row: valuesRow("c1", "b-auth", 0, "action", "C1", "running", 0)},
			{match: "SELECT id, thread_id, tier", row: valuesRow("b-auth", "th-1", 1, "Build Title", "Build Obj", "running", "")},
			{match: "SELECT COUNT(*)", row: valuesRow(1)},
		},
		execTag: "UPDATE 1",
	}.open()
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "azylman", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	ctx := makeObjectMockContext(t, "b-auth")

	// AppendChunks
	appReq := AppendChunksRequest{Chunks: []store.CreateChunkInput{{Mode: "action", Title: "C2", Instructions: "Inst"}}}
	_, _ = bo.AppendChunks(ctx, appReq)

	// UpdateChunk
	updChunkReq := store.UpdateChunkRequest{ChunkRef: "c1", Title: "Updated Action"}
	_, _ = bo.UpdateChunk(ctx, updChunkReq)

	// UpdateBuild
	rt := newFakeObjectRuntime("b-auth")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	updBldReq := store.UpdateBuildRequest{Title: "Updated Build"}
	_, _ = bo.UpdateBuild(ctx, updBldReq)

	// OnPREvent with check_failed
	prEv := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", CheckName: "lint"}
	_ = bo.OnPREvent(ctx, prEv)

	// OnPREvent with conflict
	prEvConflict := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "conflict"}
	_ = bo.OnPREvent(ctx, prEvConflict)

	// OnPREvent with merged
	prEvMerged := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "merged"}
	_ = bo.OnPREvent(ctx, prEvMerged)

	// OnPREvent with closed (status merged -> no fail)
	prEvClosed := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "merged"}
	_ = bo.OnPREvent(ctx, prEvClosed)

	// OnDispatchOutcome completed
	outEv := pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}
	_ = bo.OnDispatchOutcome(ctx, outEv)
}

func (f *fakeObjectRuntime) ScheduleReconcile() { f.reconcileScheduled = true }

func (f *fakeObjectRuntime) ScheduleDeferredReconcile(delay time.Duration, seq int) {
	f.reconcileScheduled = true
	f.deferredSends = append(f.deferredSends, deferredSend{Delay: delay, Seq: seq})
}

func (f *fakeObjectRuntime) InvocationID() string { return f.invocationID }

func (f *fakeObjectRuntime) RunRunnerGuard(fn func(ctx context.Context) (runnerGuardResult, error)) (runnerGuardResult, error) {
	f.runnerGuardCalled = true
	if f.runnerGuardErr != nil {
		return runnerGuardResult{}, f.runnerGuardErr
	}
	return fn(context.Background())
}

func (f *fakeObjectRuntime) SendReconcile(service, buildID, reason string) {
	f.reconcilesSent = append(f.reconcilesSent, sentReconcile{Service: service, BuildID: buildID, Reason: reason})
}

func TestBuildObject_NilStoreBranches(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")
	var typedNilStore *store.Store
	bo := NewBuildObject("", nil).WithAuthority(typedNilStore, nil)

	// UpdateBuild nil store
	b, err := bo.UpdateBuild(ctx, store.UpdateBuildRequest{Title: "Title"})
	assert.NoError(t, err)
	assert.Equal(t, "Title", b.Title)

	// Cancel nil store
	b, err = bo.Cancel(ctx, CancelBuildRequest{Reason: "test"})
	assert.NoError(t, err)
	assert.Equal(t, "cancelled", b.Status)

	// AppendChunks nil store
	chunks, err := bo.AppendChunks(ctx, AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}})
	assert.NoError(t, err)
	assert.Nil(t, chunks)

	// UpdateChunk nil store
	chk, err := bo.UpdateChunk(ctx, store.UpdateChunkRequest{ChunkRef: "c1", Title: "T"})
	assert.NoError(t, err)
	assert.Empty(t, chk.ID)

	// RemoveChunk nil store
	err = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	assert.NoError(t, err)

	// FailChunk nil store
	fResp, err := bo.FailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "err"})
	assert.NoError(t, err)
	assert.True(t, fResp.Failed)

	// AttachPR nil store
	aResp, err := bo.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	assert.NoError(t, err)
	assert.Equal(t, "attached", aResp.Status)

	// OnPREvent nil store
	err = bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101})
	assert.NoError(t, err)

	// OnDispatchOutcome nil store
	err = bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1"})
	assert.NoError(t, err)

	// Reconcile and Mutate with nil store and empty URL
	_, err = bo.Reconcile(ctx, ReconcileBuildRequest{})
	assert.Error(t, err)
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "ping"})
	assert.Error(t, err)
}
