package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// observeJournalCallback inspects the callback error before the SDK mock adapter
// terminates execution. It does not simulate Restate scheduling or retries.
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

func TestRunJournaledCallback(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	terminal := restate.ToTerminalError(errors.New("already terminal"), restate.WithErrorCode(422))
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"wrapped invalid", fmt.Errorf("validation: %w", store.ErrInvalidBuildInput), 400},
		{"missing build", store.ErrBuildNotFound, 404},
		{"missing chunk", ErrChunkNotFound, 404},
		{"conflict", store.ErrBuildConflict, 409},
		{"already terminal", terminal, 422},
		{"transient", transient, 0},
		{"success", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := RunJournaled(restate.WithMockContext(mc), labelUpdateBuild, "update-build-postgres", func(restate.RunContext) (string, error) {
				return "payload", tc.err
			})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, "payload", got)
			}
		})
	}
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

//nolint:tparallel // Requests must run in order against the same object after a conflict.
func TestUpdateBuildConflictThenValid(t *testing.T) {
	t.Parallel()
	st := &metadataMutationStore{err: store.ErrBuildConflict}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)
	rt := newFakeObjectRuntime("build")
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"conflict", store.ErrBuildConflict, 409},
		{"valid", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st.err = tc.err
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("build")
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := bo.UpdateBuild(restate.WithMockContext(mc), store.UpdateBuildRequest{Title: "Updated"})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, model.VanadisBuild{ID: "build", Title: "Updated"}, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func (s *metadataMutationStore) LookupChunkBuildID(context.Context, string) (store.BuildRoute, error) {
	return store.BuildRoute{}, s.err
}

func (s *metadataMutationStore) LookupPRChunk(context.Context, string, int) (store.BuildRoute, string, error) {
	return store.BuildRoute{}, "", s.err
}

func TestCreateBuildJournalCallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		code       string
		constraint string
		terminal   restate.Code
	}{
		{"chunk ID", "23505", "vanadis_chunks_pkey", 409},
		{"chunk index", "23505", "uq_vanadis_chunks_build_index", 409},
		{"unknown unique", "23505", "unknown", 0},
		{"serialization", "40001", "vanadis_chunks_pkey", 0},
		{"deadlock", "40P01", "", 0},
		{"connection", "08006", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dbErr := fmt.Errorf("database: %w", &pgconn.PgError{Code: tc.code, ConstraintName: tc.constraint, Detail: "private database details"})
			st, _ := (dbScript{failExec: "INSERT INTO vanadis_chunks", execErr: dbErr}).open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			mc := mocks.NewMockContext(t)
			want := dbErr
			if tc.terminal != 0 {
				want = fmt.Errorf("%w: chunk %q in build %q already exists", store.ErrBuildConflict, "c1", "b1")
			}
			observeJournalCallback(t, mc, want, tc.terminal)
			_, _ = RunJournaled(restate.WithMockContext(mc), labelCreate, "create-build-postgres", func(rc restate.RunContext) (bool, error) {
				return bo.persistBuildCreation(rc, model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}})
			})
		})
	}
}

func TestCreateBuildCommitAcknowledgementReplay(t *testing.T) {
	t.Parallel()
	lostAck := errors.New("connection lost after commit")
	build := model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}}
	tests := []struct {
		name string
		db   dbScript
		want error
	}{
		{"acknowledgement lost", dbScript{commitErr: lostAck}, lostAck},
		{"committed request replay", dbScript{execTag: "INSERT 0 0", buildRow: &mockRow{b: &build}, chunks: chunks}, nil},
	}
	// The second database fixture represents the durable state after the first
	// commit succeeded at PostgreSQL but its response failed at the connection.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			accepted, err := bo.persistBuildCreation(mockRunContext{Context: context.Background()}, build, chunks)
			assert.ErrorIs(t, err, tc.want)
			assert.Equal(t, tc.want == nil, accepted)
		})
	}
}

// creationTransaction models atomic visibility at the database boundary:
// inserts remain private until Commit and disappear on Rollback.
type creationTransaction struct {
	mockTx

	pending []string
	durable []string
	failure error
}

func (tx *creationTransaction) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(query, "INSERT INTO vanadis_chunks") {
		return pgconn.CommandTag{}, tx.failure
	}
	id, ok := args[0].(string)
	if !ok {
		return pgconn.CommandTag{}, errors.New("expected build ID")
	}
	tx.pending = append(tx.pending, id)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *creationTransaction) Commit(context.Context) error {
	tx.durable = append(tx.durable, tx.pending...)
	tx.pending = nil
	return nil
}

func (tx *creationTransaction) Rollback(context.Context) error {
	tx.pending = nil
	return nil
}

func TestCreateBuildConflictAtomicity(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, constraint string }{
		{"chunk ID", "vanadis_chunks_pkey"},
		{"chunk index", "uq_vanadis_chunks_build_index"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &creationTransaction{failure: &pgconn.PgError{Code: "23505", ConstraintName: tc.constraint}}
			st := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return tx, nil }})
			err := st.CreateBuild(context.Background(), model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Work", Repo: "aerial", DependsOn: []string{}}})
			require.ErrorIs(t, err, store.ErrBuildConflict)
			assert.Empty(t, tx.durable)
			assert.Empty(t, tx.pending)
		})
	}
}

type createJournal struct {
	results  []any
	executed []bool
}

func setJournalOutput(output, res any) {
	if output == nil || res == nil {
		return
	}
	outVal := reflect.ValueOf(output)
	resVal := reflect.ValueOf(res)
	if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
		outVal.Elem().Set(resVal)
	}
}

type captureCreateStore struct {
	store.VanadisStore

	build  model.VanadisBuild
	chunks []model.VanadisChunk
	calls  int
}

func (s *captureCreateStore) CreateBuild(ctx context.Context, b model.VanadisBuild, c []model.VanadisChunk) error {
	s.build, s.chunks, s.calls = b, append([]model.VanadisChunk(nil), c...), s.calls+1
	return s.VanadisStore.CreateBuild(ctx, b, c)
}

func makeCreateJournalContext(t *testing.T, key string, journal *createJournal, cursor *int, stateOut **model.BuildObjectState) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Key().Return(key).Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-test"}).Maybe()
	mc.EXPECT().Get("state", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get("launched", mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get(deploygate.PendingDeploymentKey, mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Get(deploygate.DeploymentDeadlineKey, mock.Anything).Return(false, nil).Maybe()
	mc.EXPECT().Set("state", mock.Anything, mock.Anything).RunAndReturn(func(_ string, value any, _ ...restate.SetOption) {
		if *stateOut == nil {
			if st, ok := value.(*model.BuildObjectState); ok && st != nil {
				*stateOut = st
			}
		}
	}).Maybe()
	mc.EXPECT().Set("launched", mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().Set(deploygate.PendingDeploymentKey, mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().Set(deploygate.DeploymentDeadlineKey, mock.Anything, mock.Anything).Return().Maybe()
	mc.EXPECT().ClearAll().Return().Maybe()
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything).Return(mInv).Maybe()
	mClient.EXPECT().Send(mock.Anything, mock.Anything).Return(mInv).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		if *cursor < len(journal.results) {
			setJournalOutput(output, journal.results[*cursor])
			journal.executed = append(journal.executed, false)
			*cursor++
			return nil
		}
		res, err := fn(mockRunContext{Context: context.Background()})
		journal.results = append(journal.results, res)
		journal.executed = append(journal.executed, true)
		*cursor++
		if err != nil {
			return restate.TerminalErrorf("%v", err)
		}
		setJournalOutput(output, res)
		return nil
	}).Maybe()
	return restate.WithMockContext(mc)
}

func TestCreate_JournaledClockIdentityAcrossReplay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		buildID    string
		chunkCount int
	}{
		{name: "single chunk clock identity", buildID: "b-clock-1", chunkCount: 1},
		{name: "two chunk clock identity", buildID: "b-clock-2", chunkCount: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inputs := make([]store.CreateChunkInput, tc.chunkCount)
			for i := range inputs {
				deps := []string{}
				if i > 0 {
					deps = []string{fmt.Sprintf("c%d", i)}
				}
				inputs[i] = store.CreateChunkInput{ID: fmt.Sprintf("c%d", i+1), Title: fmt.Sprintf("Chunk %d", i+1), Repo: "aerial", Instructions: "Do work", DependsOn: deps, TargetFiles: []string{fmt.Sprintf("pkg/%d.go", i+1)}}
			}
			req := CreateBuildRequest{ID: tc.buildID, ThreadID: "th1", Objective: "Obj", Chunks: inputs}
			openCapture := func() *captureCreateStore {
				b := model.VanadisBuild{ID: tc.buildID, ThreadID: "th1", Objective: "Obj", Status: "running"}
				st, _ := dbScript{buildRow: &mockRow{b: &b}}.open()
				return &captureCreateStore{VanadisStore: st}
			}
			journal := &createJournal{}
			cap1 := openCapture()
			bo1 := NewBuildObject("", nil).WithAuthority(cap1, nil)
			var state1 *model.BuildObjectState
			cursor1 := 0
			got1, err := bo1.Create(makeCreateJournalContext(t, tc.buildID, journal, &cursor1, &state1), req)
			require.NoError(t, err)
			require.Equal(t, 1, cap1.calls)
			require.NotNil(t, state1)
			require.Len(t, cap1.chunks, tc.chunkCount)
			require.True(t, journal.executed[0])
			assert.Equal(t, got1.CreatedAt, got1.UpdatedAt)
			assert.Equal(t, got1.CreatedAt, cap1.build.CreatedAt)
			assert.Equal(t, got1.CreatedAt, state1.Build.CreatedAt)
			for i := range cap1.chunks {
				assert.Equal(t, got1.CreatedAt, cap1.chunks[i].CreatedAt)
				assert.Equal(t, got1.CreatedAt, cap1.chunks[i].UpdatedAt)
				assert.Equal(t, got1.CreatedAt, state1.Chunks[i].CreatedAt)
			}
			replayed := len(journal.executed)
			cap2 := openCapture()
			bo2 := NewBuildObject("", nil).WithAuthority(cap2, nil)
			var state2 *model.BuildObjectState
			cursor2 := 0
			got2, err := bo2.Create(makeCreateJournalContext(t, tc.buildID, journal, &cursor2, &state2), req)
			require.NoError(t, err)
			assert.Equal(t, 0, cap2.calls)
			assert.False(t, journal.executed[replayed])
			require.NotNil(t, state2)
			assert.Equal(t, got1.CreatedAt, got2.CreatedAt)
			assert.Equal(t, got1.UpdatedAt, got2.UpdatedAt)
			assert.Equal(t, got1.CreatedAt, state2.Build.CreatedAt)
		})
	}
}

func (*metadataMutationStore) GetBuild(_ context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: id}, nil, nil
}

// makeDeferralJournalContext replays one journaled reconcile step and records
// every Restate command the transition issues around it.
func makeDeferralJournalContext(t *testing.T, prior *model.BuildObjectState, journaled reconcileStepOutput, commands *[]string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Key().Return("b").Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-1"}).Maybe()
	mc.EXPECT().Get(mock.Anything, mock.Anything).RunAndReturn(func(key string, output any, _ ...restate.GetOption) (bool, restate.TerminalError) {
		*commands = append(*commands, "get "+key)
		if key == "state" && prior != nil {
			setJournalOutput(output, prior)
			return true, nil
		}
		return false, nil
	}).Maybe()
	mc.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(key string, _ any, _ ...restate.SetOption) {
		*commands = append(*commands, "set "+key)
	}).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		*commands = append(*commands, "run")
		setJournalOutput(output, journaled)
		return nil
	}).Maybe()
	mClient := mocks.NewMockClient(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(func(input any, opts ...restate.SendOption) restate.Invocation {
		payload, err := json.Marshal(input)
		require.NoError(t, err)
		*commands = append(*commands, fmt.Sprintf("send %s %v", payload, opts))
		return mocks.NewMockInvocation(t)
	}).Maybe()
	return restate.WithMockContext(mc)
}

// In-flight invocations journaled before the deferral lease replay the exact
// legacy commands (1s send with the unchanged payload); new outputs only add
// the DeferralSeq field and delay on the same send, never a new command.
func TestReconcile_DeferralJournalCommandSequence(t *testing.T) {
	t.Parallel()
	running := model.VanadisBuild{ID: "b", Status: "running"}
	active := "dispatch active; reconciliation deferred"
	tests := []struct {
		name      string
		prior     *model.BuildObjectState
		tick      int
		journaled reconcileStepOutput
		wantSend  string
	}{
		{
			name:      "legacy deferral output replays 1s recovery send",
			journaled: reconcileStepOutput{BuildStatus: "running", Error: active, LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: running}},
			wantSend:  `send {"reason":"active dispatch recovery"} [{1000000000}]`,
		},
		{
			name:      "lease output schedules chain tick",
			journaled: reconcileStepOutput{BuildStatus: "running", Error: active, LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: running, DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 1}}, DeferralReconcileSeq: 1}},
			wantSend:  `send {"reason":"active dispatch recovery","deferral_seq":1} [{1000000000}]`,
		},
		{
			name:      "lease tick backs off",
			prior:     &model.BuildObjectState{Build: running, DeferralReconcileSeq: 3},
			tick:      3,
			journaled: reconcileStepOutput{BuildStatus: "running", Error: active, LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: running, DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 4}}, DeferralReconcileSeq: 4}},
			wantSend:  `send {"reason":"active dispatch recovery","deferral_seq":4} [{8000000000}]`,
		},
		{
			name:      "lease output with pending chain sends nothing",
			prior:     &model.BuildObjectState{Build: running, DeferralReconcileSeq: 3},
			journaled: reconcileStepOutput{BuildStatus: "running", Error: active, LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: running, DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 2}}, DeferralReconcileSeq: 3}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var commands []string
			ctx := makeDeferralJournalContext(t, tc.prior, tc.journaled, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&leaseStore{}, nil)
			_, err := bo.executeObjectTransitionTick(restateObjectRuntime{ctx: ctx}, labelReconcileBuild, nil, tc.tick)
			require.NoError(t, err)
			want := []string{"get state", "get launched", "run", "set state", "set launched"}
			if tc.wantSend != "" {
				want = append(want, tc.wantSend)
			}
			assert.Equal(t, want, commands)
		})
	}
}

// runStepName reads the restate.WithName option of a Run call.
func runStepName(opts []restate.RunOption) string {
	for _, opt := range opts {
		m := reflect.ValueOf(opt).MethodByName("BeforeRun")
		arg := reflect.New(m.Type().In(0).Elem())
		m.Call([]reflect.Value{arg})
		if name := arg.Elem().FieldByName("Name").String(); name != "" {
			return name
		}
	}
	return ""
}

// makeOutcomeJournalContext replays journaled Run results by step name and
// records every Restate command OnDispatchOutcome issues.
func makeOutcomeJournalContext(t *testing.T, journal map[string]any, commands *[]string) restate.ObjectContext {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Key().Return("b1").Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-1"}).Maybe()
	mc.EXPECT().Get(mock.Anything, mock.Anything).RunAndReturn(func(key string, _ any, _ ...restate.GetOption) (bool, restate.TerminalError) {
		*commands = append(*commands, "get "+key)
		return false, nil
	}).Maybe()
	mc.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(key string, _ any, _ ...restate.SetOption) {
		*commands = append(*commands, "set "+key)
	}).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		name := runStepName(opts)
		*commands = append(*commands, "run "+name)
		if res, ok := journal[name]; ok {
			setJournalOutput(output, res)
		}
		return nil
	}).Maybe()
	mClient := mocks.NewMockClient(t)
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(func(input any, _ ...restate.SendOption) restate.Invocation {
		payload, err := json.Marshal(input)
		require.NoError(t, err)
		*commands = append(*commands, fmt.Sprintf("send %s", payload))
		return mocks.NewMockInvocation(t)
	}).Maybe()
	return restate.WithMockContext(mc)
}

// Invocations journaled before the verdict cutover replay their recorded step
// results onto the same command sequence; the cutover only adds the
// fail_action result on the existing handle-nopr-turn-outcome step.
func TestOnDispatchOutcome_VerdictCutoverJournalCommandSequence(t *testing.T) {
	t.Parallel()
	transition := reconcileStepOutput{BuildStatus: "running", LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: model.VanadisBuild{ID: "b1", Status: "running"}}}
	prefix := []string{"run runner-guard", "run handle-dispatch-outcome-postgres"}
	tests := []struct {
		name    string
		status  string
		applied bool
		noPR    noPRAction
		want    []string
	}{
		{name: "legacy accepted action completion reconciles", status: "completed", applied: true, want: []string{"get state", "get launched", "run reconcile-after-dispatch-outcome", "set state", "set launched"}},
		{name: "verdictless turn resumed", status: "completed", noPR: noPRActionResume, want: []string{"run handle-nopr-turn-outcome"}},
		{name: "verdictless turn ignored", status: "completed", noPR: noPRActionNone, want: []string{"run handle-nopr-turn-outcome"}},
		{name: "legacy no-pr failure reconciles", status: "completed", noPR: noPRActionFail, want: []string{"run handle-nopr-turn-outcome", "get state", "get launched", "run reconcile-after-nopr-fail", "set state", "set launched"}},
		{name: "action resume cap reconciles failure", status: "completed", noPR: noPRActionFailAction, want: []string{"run handle-nopr-turn-outcome", "get state", "get launched", "run reconcile-after-nopr-fail", "set state", "set launched"}},
		{name: "timeout turn resumed", status: "timeout", noPR: noPRActionResume, want: []string{"run handle-nopr-turn-outcome"}},
		{name: "unapplied failed outcome stops", status: "failed", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var commands []string
			journal := map[string]any{
				labelRunnerGuard:                   runnerGuardResult{},
				"handle-dispatch-outcome-postgres": tc.applied,
				"handle-nopr-turn-outcome":         tc.noPR,
				labelReconcileAfterDispatch:        transition,
				labelReconcileAfterNoPRFail:        transition,
			}
			ctx := makeOutcomeJournalContext(t, journal, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&metadataMutationStore{}, executor.NewExecutor("", &mockEriClient{}, nil, "token", nil))
			err := bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Status: tc.status})
			require.NoError(t, err)
			assert.Equal(t, append(append([]string{}, prefix...), tc.want...), commands)
		})
	}
}

// A repair PR's deploy_success changes only what the journaled
// handle-pr-event-postgres step writes: invocations already in flight replay
// the same command sequence whether the deploying chunk is a PR chunk that
// completed or an action chunk moved back to running.
func TestOnPREvent_RepairDeployJournalCommandSequence(t *testing.T) {
	t.Parallel()
	transition := reconcileStepOutput{BuildStatus: "running", LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: model.VanadisBuild{ID: "b1", Status: "running"}}}
	tests := []struct {
		name    string
		outcome prEventOutcome
		want    []string
	}{
		{name: "action repair deploy reconciles", outcome: prEventOutcome{Applied: true}, want: []string{"run runner-guard", "run handle-pr-event-postgres", "get state", "get launched", "run reconcile-after-pr-event", "set state", "set launched"}},
		{name: "action repair deploy with cross build sweep", outcome: prEventOutcome{Applied: true, ReconcileTargets: []store.BuildRoute{{BuildID: "b2", RunnerAgent: "aerial"}}}, want: []string{"run runner-guard", "run handle-pr-event-postgres", `send {"reason":"widget_sync"}`, "get state", "get launched", "run reconcile-after-pr-event", "set state", "set launched"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var commands []string
			journal := map[string]any{
				labelRunnerGuard:           runnerGuardResult{},
				"handle-pr-event-postgres": tc.outcome,
				labelReconcileAfterPREvent: transition,
			}
			ctx := makeOutcomeJournalContext(t, journal, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&metadataMutationStore{}, executor.NewExecutor("", &mockEriClient{}, nil, "token", nil))
			err := bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 9, Event: "deploy_success"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, commands)
		})
	}
}
