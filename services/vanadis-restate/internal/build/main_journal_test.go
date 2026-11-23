package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

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
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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
			restatetest.ObserveJournalCallback(t, mc, tc.err, tc.code)
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

//nolint:tparallel // Requests must run in order against the same object after a conflict.
func TestUpdateBuildConflictThenValid(t *testing.T) {
	t.Parallel()
	st := &restatetest.MetadataMutationStore{Err: store.ErrBuildConflict}
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
			st.Err = tc.err
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("build")
			restatetest.ObserveJournalCallback(t, mc, tc.err, tc.code)
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

// createConflictSetupSQL seeds build b0 owning chunk c1 titled "Chunk".
const createConflictSetupSQL = `INSERT INTO vanadis_builds (id, thread_id, objective) VALUES ('b0', 'th0', 'Other');
INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions) VALUES ('c1', 'b0', 0, 'aerial', 'Chunk', 'Other work');`

// createGhostChunkSQL makes every new build arrive with a chunk at index 0,
// so the first planned chunk collides on the per-build index.
const createGhostChunkSQL = `CREATE FUNCTION pgtest_ghost_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions) VALUES ('ghost-' || NEW.id, NEW.id, 0, 'aerial', 'Ghost', 'Ghost');
	RETURN NULL;
END $$;
CREATE TRIGGER pgtest_ghost_chunk AFTER INSERT ON vanadis_builds FOR EACH ROW EXECUTE FUNCTION pgtest_ghost_chunk();`

// raiseOnChunkInsertSQL makes chunk inserts fail with SQLSTATE code.
func raiseOnChunkInsertSQL(code string) string {
	return fmt.Sprintf(`CREATE FUNCTION pgtest_raise() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'injected failure' USING ERRCODE = '%s';
END $$;
CREATE TRIGGER pgtest_raise BEFORE INSERT ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION pgtest_raise();`, code)
}

// observeCreateCallback checks the raw create callback error: a terminal
// error carries code and exact message, a retryable one keeps its SQLSTATE
// (empty for driver errors) and message fragment.
func observeCreateCallback(t *testing.T, mc *mocks.MockContext, code restate.Code, sqlState, wantErr string) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(restatetest.MockRunContext{Context: context.Background()})
		require.Error(t, err)
		var terminal restate.TerminalError
		if code != 0 {
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
			assert.EqualError(t, err, wantErr)
		} else {
			assert.False(t, restate.IsTerminalError(err))
			assert.ErrorContains(t, err, wantErr)
			var pgErr *pgconn.PgError
			if sqlState != "" {
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, sqlState, pgErr.Code)
			} else {
				assert.False(t, errors.As(err, &pgErr))
			}
		}
		return restatetest.RunMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

func TestCreateBuildJournalCallback(t *testing.T) {
	t.Parallel()
	conflict := fmt.Sprintf("%s: chunk %q in build %q already exists", store.ErrBuildConflict, "c1", "b1")
	tests := []struct {
		name     string
		setup    string
		closed   bool
		terminal restate.Code
		sqlState string
		wantErr  string
	}{
		{name: "chunk ID", setup: createConflictSetupSQL, terminal: 409, wantErr: conflict},
		{name: "chunk index", setup: createGhostChunkSQL, terminal: 409, wantErr: conflict},
		{name: "unknown unique", setup: createConflictSetupSQL + ` UPDATE vanadis_chunks SET id = 'c0'; CREATE UNIQUE INDEX pgtest_unique_title ON vanadis_chunks (title);`, sqlState: "23505", wantErr: "pgtest_unique_title"},
		{name: "serialization", setup: raiseOnChunkInsertSQL("40001"), sqlState: "40001", wantErr: "injected failure"},
		{name: "deadlock", setup: raiseOnChunkInsertSQL("40P01"), sqlState: "40P01", wantErr: "injected failure"},
		{name: "connection", closed: true, wantErr: "begin tx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			mustExec(t, pool, tc.setup)
			if tc.closed {
				pool.Close()
			}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			mc := mocks.NewMockContext(t)
			observeCreateCallback(t, mc, tc.terminal, tc.sqlState, tc.wantErr)
			accepted, err := RunJournaled(restate.WithMockContext(mc), labelCreate, "create-build-postgres", func(rc restate.RunContext) (bool, error) {
				return bo.persistBuildCreation(rc, model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}})
			})
			require.Error(t, err)
			assert.False(t, accepted)
		})
	}
}

// A lost COMMIT acknowledgement cannot be forced on a real server. The two
// durable outcomes it hides are covered instead: a transaction that fails at
// COMMIT persists nothing and is not accepted, and a request replayed after
// its commit landed is accepted without duplicating the build.
func TestCreateBuildCommitAcknowledgementReplay(t *testing.T) {
	t.Parallel()
	build := model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}}
	tests := []struct {
		name       string
		sabotage   string
		calls      int
		wantErr    string
		wantChunks []string
	}{
		{name: "commit fails", sabotage: failFunctionSQL + ` CREATE CONSTRAINT TRIGGER pgtest_fail_commit AFTER INSERT ON vanadis_chunks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pgtest_fail();`, calls: 1, wantErr: "sabotaged INSERT on vanadis_chunks"},
		{name: "committed request replay", calls: 2, wantChunks: []string{"c1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			mustExec(t, pool, tc.sabotage)
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			var accepted bool
			var err error
			for range tc.calls {
				accepted, err = bo.persistBuildCreation(restatetest.MockRunContext{Context: context.Background()}, build, chunks)
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.False(t, accepted)
				_, _, getErr := st.GetBuild(context.Background(), "b1")
				assert.ErrorIs(t, getErr, store.ErrBuildNotFound)
				return
			}
			require.NoError(t, err)
			assert.True(t, accepted)
			_, stored := loadBuild(t, st, "b1")
			var ids []string
			for _, c := range stored {
				ids = append(ids, c.ID)
			}
			assert.Equal(t, tc.wantChunks, ids)
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
		res, err := fn(restatetest.MockRunContext{Context: context.Background()})
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
			st, pool := openStore(t)
			require.NotNil(t, pool)
			journal := &createJournal{}
			cap1 := &faultStore{VanadisStore: st}
			bo1 := NewBuildObject("", nil).WithAuthority(cap1, nil)
			var state1 *model.BuildObjectState
			cursor1 := 0
			got1, err := bo1.Create(makeCreateJournalContext(t, tc.buildID, journal, &cursor1, &state1), req)
			require.NoError(t, err)
			created := callArgs(cap1.snapshotCalls(), "CreateBuild")
			require.Len(t, created, 1)
			require.NotNil(t, state1)
			require.Len(t, created[0], 2+2*tc.chunkCount)
			require.True(t, journal.executed[0])
			at := got1.CreatedAt.Format(time.RFC3339Nano)
			assert.Equal(t, got1.CreatedAt, got1.UpdatedAt)
			assert.Equal(t, tc.buildID, created[0][0])
			assert.Equal(t, at, created[0][1])
			assert.Equal(t, got1.CreatedAt, state1.Build.CreatedAt)
			for i := range tc.chunkCount {
				assert.Equal(t, at, created[0][2+2*i])
				assert.Equal(t, at, created[0][3+2*i])
				assert.Equal(t, got1.CreatedAt, state1.Chunks[i].CreatedAt)
			}
			replayed := len(journal.executed)
			cap2 := &faultStore{VanadisStore: st}
			bo2 := NewBuildObject("", nil).WithAuthority(cap2, nil)
			var state2 *model.BuildObjectState
			cursor2 := 0
			got2, err := bo2.Create(makeCreateJournalContext(t, tc.buildID, journal, &cursor2, &state2), req)
			require.NoError(t, err)
			assert.Empty(t, cap2.snapshotCalls())
			assert.False(t, journal.executed[replayed])
			require.NotNil(t, state2)
			assert.Equal(t, got1.CreatedAt, got2.CreatedAt)
			assert.Equal(t, got1.UpdatedAt, got2.UpdatedAt)
			assert.Equal(t, got1.CreatedAt, state2.Build.CreatedAt)
		})
	}
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

func TestReconcile_JournalCommandSequence(t *testing.T) {
	t.Parallel()
	running := model.VanadisBuild{ID: "b", Status: "running"}
	tests := []struct {
		name      string
		prior     *model.BuildObjectState
		journaled reconcileStepOutput
		want      []string
	}{
		{
			name:      "running transition updates state and launched",
			journaled: reconcileStepOutput{BuildStatus: "running", LaunchedMap: map[string]bool{"c1": true}, State: &model.BuildObjectState{Build: running}},
			want:      []string{"get state", "get launched", "run", "set state", "set launched"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var commands []string
			ctx := makeDeferralJournalContext(t, tc.prior, tc.journaled, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&fakeStore{}, nil)
			_, err := bo.executeObjectTransition(restateObjectRuntime{ctx: ctx}, labelReconcileBuild, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, commands)
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
	prefix := []string{"run handle-dispatch-outcome-postgres"}
	tests := []struct {
		name       string
		status     string
		dispatchID string
		applied    bool
		noPR       noPRAction
		want       []string
	}{
		{name: "verdictless ci fix-up turn checks then reconciles", status: "completed", dispatchID: "vanadis-c1-cifail-abc123-1", want: []string{"run handle-cifail-turn-ended", "get state", "get launched", "run reconcile-after-dispatch-outcome", "set state", "set launched"}},
		{name: "timed-out ci fix-up turn checks then reconciles", status: "timeout", dispatchID: "vanadis-c1-cifail-abc123-1", want: []string{"run handle-cifail-turn-ended", "get state", "get launched", "run reconcile-after-dispatch-outcome", "set state", "set launched"}},
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
				"handle-dispatch-outcome-postgres": tc.applied,
				"handle-nopr-turn-outcome":         tc.noPR,
				"handle-cifail-turn-ended":         true,
				labelReconcileAfterDispatch:        transition,
				labelReconcileAfterNoPRFail:        transition,
			}
			ctx := makeOutcomeJournalContext(t, journal, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&restatetest.MetadataMutationStore{}, executor.NewExecutor("", &mockEriClient{}, nil, "token", "example-owner", nil))
			dispatchID := tc.dispatchID
			if dispatchID == "" {
				dispatchID = "vanadis-c1-turn-0"
			}
			err := bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", DispatchID: dispatchID, Status: tc.status})
			require.NoError(t, err)
			assert.Equal(t, append(append([]string{}, prefix...), tc.want...), commands)
		})
	}
}

// Pins the Restate command sequence OnPREvent issues for deploy_success, the
// only OnPREvent journal sequence test. Deploy handling of either chunk mode
// runs inside the handle-pr-event-postgres step, so adding, renaming or
// reordering a command on this path (which would break in-flight invocations
// with RT0016) fails here. Mode-specific behavior is covered by the
// TestCompletePRDeploymentMutation_* tests.
func TestOnPREvent_DeploySuccessJournalCommandSequence(t *testing.T) {
	t.Parallel()
	transition := reconcileStepOutput{BuildStatus: "running", LaunchedMap: map[string]bool{}, State: &model.BuildObjectState{Build: model.VanadisBuild{ID: "b1", Status: "running"}}}
	tests := []struct {
		name    string
		outcome prEventOutcome
		want    []string
	}{
		{name: "deploy success reconciles", outcome: prEventOutcome{Applied: true}, want: []string{"run handle-pr-event-postgres", "get state", "get launched", "run reconcile-after-pr-event", "set state", "set launched"}},
		{name: "deploy success with cross build sweep", outcome: prEventOutcome{Applied: true, ReconcileTargets: []store.SweptChunk{{ChunkID: "c1", BuildID: "b2"}}}, want: []string{"run handle-pr-event-postgres", `send {"action":"chunk_deploy_success","chunk_id":"c1"}`, "get state", "get launched", "run reconcile-after-pr-event", "set state", "set launched"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var commands []string
			journal := map[string]any{
				"handle-pr-event-postgres": tc.outcome,
				labelReconcileAfterPREvent: transition,
			}
			ctx := makeOutcomeJournalContext(t, journal, &commands)
			bo := NewBuildObject("", nil).WithAuthority(&restatetest.MetadataMutationStore{}, executor.NewExecutor("", &mockEriClient{}, nil, "token", "example-owner", nil))
			err := bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 9, Event: "deploy_success"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, commands)
		})
	}
}
