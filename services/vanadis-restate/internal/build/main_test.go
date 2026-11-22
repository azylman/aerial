package build

import (
	"context"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strconv"
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

func TestBuildObject_AuthoritativeHandlers(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "test-token", "example-owner", nil)
	st, _ := openStore(t)

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
			{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do 1", DependsOn: []string{}, TargetFiles: []string{"pkg/a.go"}},
			{ID: "c2", Title: "Chunk 2", Repo: "aerial", Instructions: "Do 2", DependsOn: []string{"c1"}, TargetFiles: []string{"pkg/b.go"}},
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

	// 4. UpdateChunk: Create launched c1, so edit the still-pending dependent.
	cUp, err := bo.UpdateChunk(ctx, store.UpdateChunkRequest{ChunkRef: "c2", Title: "Updated Title"})
	assert.NoError(t, err)
	assert.Equal(t, "c2", cUp.ID)

	// 5. AppendChunks
	appReq := AppendChunksRequest{Chunks: []store.CreateChunkInput{{Title: "Chunk 3", Repo: "aerial", Instructions: "Do 3", DependsOn: []string{"c2"}, TargetFiles: []string{"pkg/c.go"}}}}
	appended, err := bo.AppendChunks(ctx, appReq)
	require.NoError(t, err)
	require.Len(t, appended, 1)
	assert.Equal(t, "b1-chunk-3", appended[0].ID)
	assert.Equal(t, []string{"b1-chunk-3", "c1", "c2"}, slices.Sorted(maps.Keys(chunkStatuses(t, st, "b1"))))

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

	// 8. RemoveChunk: only pending chunks are removable, so drop the appended one.
	remErr := bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: appended[0].ID})
	assert.NoError(t, remErr)
	assert.Equal(t, map[string]string{"c1": "failed", "c2": "pending"}, chunkStatuses(t, st, "b1"))
	stored, chunks := loadBuild(t, st, "b1")
	assert.Equal(t, "New Title", stored.Title)
	assert.Equal(t, "New Obj", stored.Objective)
	assert.Equal(t, "Updated Title", chunks[1].Title)
	assert.Equal(t, 101, chunks[0].PRNumber)

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
	stored, _ = loadBuild(t, st, "b1")
	assert.Equal(t, statusCancelled, stored.Status)
}

func TestBuildObject_HandlerErrors(t *testing.T) {
	t.Parallel()
	ctx := makeObjectMockContext(t, "b1")
	bo := NewBuildObject("", nil).WithAuthority(closedStore(t), nil)

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
	storeCreate, _ := openStore(t)
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
	assert.Equal(t, map[string]string{"b-auto-chunk-1": "pending"}, chunkStatuses(t, storeCreate, "b-auto"))

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
}

func TestBuildObject_CancelTerminalBuilds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      string
		wantErr     string
		wantCleared bool
		wantStored  string
	}{
		{name: "completed", status: "completed", wantErr: "vanadis build state conflict", wantStored: "completed"},
		{name: "failed", status: "failed", wantCleared: true, wantStored: "cancelled"},
		{name: "cancelled", status: "cancelled", wantErr: store.ErrBuildConflict.Error(), wantStored: "cancelled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := seededStore(t, []model.VanadisBuild{{ID: "b1", ThreadID: "th-1", Status: tc.status}}, nil)
			rt := newFakeObjectRuntime("b1")
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return rt }).WithAuthority(st, nil)
			got, err := bo.Cancel(makeObjectMockContext(t, "b1"), CancelBuildRequest{})
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, statusCancelled, got.Status)
			}
			assert.Equal(t, tc.wantCleared, rt.cleared)
			stored, _ := loadBuild(t, st, "b1")
			assert.Equal(t, tc.wantStored, stored.Status)
		})
	}
}

func TestBuildObject_HandlersWithAuthorityAndMockContext(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-auth", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-auth", ChunkIndex: 0, Title: "C1", Status: "pending", Mode: "action", Repo: "aerial", PRNumber: 101}
	st, _ := seededStore(t, []model.VanadisBuild{b1}, []model.VanadisChunk{c1})
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	ctx := makeObjectMockContext(t, "b-auth")

	// AppendChunks: the transition also launches the ready c1.
	appReq := AppendChunksRequest{Chunks: []store.CreateChunkInput{{Mode: "action", Title: "C2", Instructions: "Inst", DependsOn: []string{"c1"}}}}
	_, err := bo.AppendChunks(ctx, appReq)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"c1": "running", "b-auth-chunk-2": "pending"}, chunkStatuses(t, st, "b-auth"))

	// UpdateChunk on the still-pending appended chunk.
	updChunkReq := store.UpdateChunkRequest{ChunkRef: "b-auth-chunk-2", Title: "Updated Action"}
	_, err = bo.UpdateChunk(ctx, updChunkReq)
	require.NoError(t, err)

	// UpdateBuild
	rt := newFakeObjectRuntime("b-auth")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	updBldReq := store.UpdateBuildRequest{Title: "Updated Build"}
	_, err = bo.UpdateBuild(ctx, updBldReq)
	require.NoError(t, err)

	// PR lifecycle on the action chunk's attached PR, then its dispatch outcome.
	events := []pubsub.PRLifecycleEvent{
		{Repo: "aerial", PRNumber: 101, Event: "check_failed", CheckName: "lint"},
		{Repo: "aerial", PRNumber: 101, Event: "conflict"},
		{Repo: "aerial", PRNumber: 101, Event: "merged"},
		{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "merged"},
	}
	for _, ev := range events {
		require.NoError(t, bo.OnPREvent(ctx, ev), ev.Event)
	}
	require.NoError(t, bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "completed"}))

	stored, chunks := loadBuild(t, st, "b-auth")
	assert.Equal(t, "running", stored.Status)
	assert.Equal(t, "Updated Build", stored.Title)
	require.Len(t, chunks, 2)
	assert.Equal(t, []string{"c1", "merged", "101", "Merge conflict detected"}, []string{chunks[0].ID, chunks[0].Status, strconv.Itoa(chunks[0].PRNumber), chunks[0].Banner})
	assert.Equal(t, []string{"b-auth-chunk-2", "pending", "Updated Action"}, []string{chunks[1].ID, chunks[1].Status, chunks[1].Title})
}

func (f *fakeObjectRuntime) ScheduleReconcile() { f.reconcileScheduled = true }

func (f *fakeObjectRuntime) ScheduleDeferredReconcile(delay time.Duration, seq int) {
	f.reconcileScheduled = true
	f.deferredSends = append(f.deferredSends, deferredSend{Delay: delay, Seq: seq})
}

func (f *fakeObjectRuntime) InvocationID() string { return f.invocationID }

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
