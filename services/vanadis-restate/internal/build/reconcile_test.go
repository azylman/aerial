package build

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestBuildObject_WithRestateMockContext_Completed(t *testing.T) {
	b := model.VanadisBuild{ID: "b-restate-mock", Title: "Build", Status: statusCompleted, ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-restate-mock", Status: statusCompleted, Repo: "aerial", Mode: "pr"}
	st, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1}, ExecTag: "UPDATE 1"})
	bo := NewBuildObject("", nil).WithAuthority(st, nil)

	ctx := makeObjectMockContext(t, "b-restate-mock")
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{})
	require.NoError(t, err)
	assert.Equal(t, statusCompleted, res.Status)
}

func TestBuildObject_WithRestateMockContext_Running(t *testing.T) {
	b := model.VanadisBuild{ID: "b-restate-mock", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-restate-mock", Status: "pending", Repo: "aerial", Mode: "pr"}
	st, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1}, ExecTag: "UPDATE 1"})
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	ctx := makeObjectMockContext(t, "b-restate-mock")
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{})
	require.NoError(t, err)
	assert.Equal(t, "running", res.Status)
	assert.Contains(t, res.LaunchedChunks, "c1")
}

func TestBuildObject_ValidationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilObject   bool
		nilStore    bool
		legacyURL   bool
		emptyKey    bool
		stateErr    bool
		launchedErr bool
		stepErr     bool
	}{
		{name: "nil object rejects reconcile and mutate", nilObject: true},
		{name: "missing store rejects reconcile and mutate", nilStore: true},
		{name: "legacy vanadis url without store rejects reconcile and mutate", nilStore: true, legacyURL: true},
		{name: "empty build id rejects reconcile and mutate", emptyKey: true},
		{name: "state read failure rejects reconcile and mutate", stateErr: true},
		{name: "launched read failure rejects reconcile and mutate", launchedErr: true},
		{name: "step execution failure rejects reconcile and mutate", stepErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilObject {
				var nilBO *BuildObject
				_, err := nilBO.Reconcile(nil, ReconcileBuildRequest{})
				assert.Error(t, err)
				_, err = nilBO.Mutate(nil, BuildMutationRequest{})
				assert.Error(t, err)
				return
			}
			url := ""
			if tc.legacyURL {
				url = "http://localhost:4025"
			}
			key := "b-validate"
			if tc.emptyKey {
				key = ""
			}
			rt := newFakeObjectRuntime(key)
			if tc.stateErr {
				rt.getStateErr = errors.New("cannot read state")
			}
			if tc.launchedErr {
				rt.getLaunchedErr = errors.New("cannot read launched")
			}
			if tc.stepErr {
				rt.runStepErr = errors.New("step execution error")
			}
			bo := NewBuildObject(url, nil)
			if !tc.nilStore {
				st, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &model.VanadisBuild{ID: "b-validate", Status: "running", ThreadID: "th-1"}}})
				bo = bo.WithAuthority(st, nil)
			}
			bo.runtimeFactory = func(_ restate.ObjectContext) objectRuntime { return rt }
			_, err := bo.Reconcile(nil, ReconcileBuildRequest{})
			assert.Error(t, err)
			if tc.emptyKey {
				var term restate.TerminalError
				require.ErrorAs(t, err, &term)
				assert.Equal(t, http.StatusBadRequest, int(term.Code()))
			}
			_, err = bo.Mutate(nil, BuildMutationRequest{})
			assert.Error(t, err)
			if tc.emptyKey {
				var term restate.TerminalError
				require.ErrorAs(t, err, &term)
				assert.Equal(t, http.StatusBadRequest, int(term.Code()))
			}
		})
	}
}

func TestBuildObject_Transitions_Table(t *testing.T) {
	t.Parallel()

	runningBuild := model.VanadisBuild{ID: "b-trans", Title: "Build", Status: "running", ThreadID: "th-1"}

	tests := []struct {
		name         string
		build        model.VanadisBuild
		chunks       []model.VanadisChunk
		buildErr     bool
		notFound     bool
		nilStore     bool
		mutation     *BuildMutationRequest
		stateLaunch  map[string]bool
		wantStatus   string
		wantChunks   []string
		wantLaunched map[string]bool
		wantErr      bool
		wantCleared  bool
		// Dispatch deferral lease.
		dispatchErr        error
		priorDeferrals     map[string]model.DispatchDeferral
		priorSeq           int
		tick               int
		wantSends          []deferredSend
		wantDeferralCounts map[string]int
		wantSeq            int
		wantReserved       []string
		wantFailed         []string
		failErr            error
	}{
		{
			name:  "successful_reconcile_running",
			build: runningBuild,
			chunks: []model.VanadisChunk{
				{ID: "c1", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr"},
			},
			stateLaunch:  map[string]bool{"c0": true},
			wantStatus:   "running",
			wantChunks:   []string{"c1"},
			wantReserved: []string{"c1"},
		},
		{name: "deferred_launch_stays_pending_and_starts_lease_chain", build: runningBuild, chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, wantStatus: "running", wantSends: []deferredSend{{Delay: time.Second, Seq: 1}}, wantDeferralCounts: map[string]int{"c1": 1}, wantSeq: 1},
		{name: "deferred_launch_while_chain_pending_schedules_nothing", build: runningBuild, chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, priorSeq: 3, wantStatus: "running", wantDeferralCounts: map[string]int{"c1": 1}, wantSeq: 3},
		{name: "chain_tick_schedules_next_tick", build: runningBuild, chunks: leaseChunks("running", 0), dispatchErr: eriwire.ErrDispatchActive, priorSeq: 3, tick: 3, wantStatus: "running", wantSends: []deferredSend{{Delay: time.Second, Seq: 4}}, wantDeferralCounts: map[string]int{"c1": 1}, wantSeq: 4},
		{name: "lease_expiry_fails_pending_action_chunk_resumably", build: runningBuild, chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, priorDeferrals: expiredLease("c1"), priorSeq: 40, tick: 40, wantStatus: statusFailed, wantReserved: []string{"c1"}, wantFailed: []string{"c1 vanadis-c1-turn-0 RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"}},
		{name: "lease_expiry_fails_running_retry_generation", build: runningBuild, chunks: leaseChunks("running", 2), dispatchErr: eriwire.ErrDispatchActive, priorDeferrals: expiredLease("dispatch:vanadis-c1-turn-2"), wantStatus: statusFailed, wantFailed: []string{"c1 vanadis-c1-turn-2 RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"}},
		{name: "lease_expiry_fail_write_error_retries_step", build: runningBuild, chunks: leaseChunks("running", 0), dispatchErr: eriwire.ErrDispatchActive, priorDeferrals: expiredLease("c1"), failErr: errors.New("database unavailable"), wantErr: true},
		{name: "accepted_launch_clears_lease", build: runningBuild, chunks: leaseChunks("pending", 0), priorDeferrals: expiredLease("c1"), priorSeq: 5, wantStatus: "running", wantChunks: []string{"c1"}, wantReserved: []string{"c1"}},
		{
			name:  "successful_mutate_completed_clears_state",
			build: runningBuild,
			chunks: []model.VanadisChunk{
				{ID: "c1", BuildID: "b-trans", Status: statusCompleted, Repo: "aerial", Mode: "pr"},
			},
			mutation: &BuildMutationRequest{
				Action:  "chunk_merged",
				ChunkID: "c1",
			},
			stateLaunch: map[string]bool{"c0": true},
			wantStatus:  statusCompleted,
			wantCleared: true,
		},
		{
			name:  "reconcile_failed_preserves_state_and_launched",
			build: model.VanadisBuild{ID: "b-trans", Title: "Build", Status: statusFailed, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{
				{ID: "c1", BuildID: "b-trans", Status: statusFailed, Repo: "aerial", Mode: "pr"},
			},
			stateLaunch:  map[string]bool{"c0": true},
			wantStatus:   statusFailed,
			wantLaunched: map[string]bool{"c0": true},
		},
		{
			name:  "terminal_cancelled_clears_state",
			build: model.VanadisBuild{ID: "b-trans", Title: "Build", Status: statusCancelled, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{
				{ID: "c1", BuildID: "b-trans", Status: statusCancelled, Repo: "aerial", Mode: "pr"},
			},
			wantStatus:  statusCancelled,
			wantCleared: true,
		},
		{
			name:        "build_not_found_reports_not_found",
			notFound:    true,
			wantStatus:  statusNotFound,
			wantCleared: true,
		},
		{
			name:     "store_read_failure_returns_error",
			buildErr: true,
			wantErr:  true,
		},
		{
			name:     "missing_store_fails_transition_step",
			nilStore: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := tc.build
			script := dbScript{Row: &sqltest.Row{Build: &b}, Chunks: tc.chunks, ExecTag: "UPDATE 1"}
			if tc.buildErr {
				script = dbScript{Row: sqltest.ErrRow(errors.New("database unavailable"))}
			}
			if tc.notFound {
				script = dbScript{Row: sqltest.ErrRow(store.ErrBuildNotFound)}
			}
			raw, _ := open(script)
			st := &leaseStore{VanadisStore: raw, failErr: tc.failErr}
			eri := &mockEriClient{dispatchFn: func(context.Context, eriwire.BrainDispatchRequest) error { return tc.dispatchErr }}
			bus := pubsub.NewMemoryBus()
			exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil)
			bo := NewBuildObject("", nil)
			if !tc.nilStore {
				bo = bo.WithAuthority(st, exec)
			}
			rt := newFakeObjectRuntime("b-trans")
			if tc.stateLaunch != nil || tc.priorDeferrals != nil || tc.priorSeq != 0 {
				rt.state = &model.BuildObjectState{Build: tc.build, Launched: tc.stateLaunch, DispatchDeferrals: tc.priorDeferrals, DeferralReconcileSeq: tc.priorSeq}
			}
			stepName := "reconcile"
			if tc.mutation != nil {
				stepName = "mutate"
			}
			res, err := bo.executeObjectTransitionTick(rt, stepName, tc.mutation, tc.tick)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, res.Status)
			assert.Equal(t, tc.wantChunks, res.LaunchedChunks)
			assert.Equal(t, tc.wantCleared, rt.cleared)
			if tc.wantLaunched != nil {
				assert.Equal(t, tc.wantLaunched, rt.launched)
			}
			assert.Equal(t, tc.wantSends, rt.deferredSends)
			assert.Equal(t, tc.wantReserved, st.reserved)
			assert.Equal(t, tc.wantFailed, st.failed)
			seq, counts := deferralSummary(rt.state)
			assert.Equal(t, tc.wantSeq, seq)
			assert.Equal(t, tc.wantDeferralCounts, counts)
		})
	}
}

// leaseChunks is one action chunk at the given status and retry generation.
func leaseChunks(status string, nudges int) []model.VanadisChunk {
	return []model.VanadisChunk{{ID: "c1", BuildID: "b-trans", Status: status, Repo: "aerial", Mode: "action", StallNudges: nudges}}
}

// expiredLease is a deferral that began well over ten minutes ago.
func expiredLease(key string) map[string]model.DispatchDeferral {
	return map[string]model.DispatchDeferral{key: {FirstDeferredAt: time.Date(2026, 11, 21, 3, 0, 0, 0, time.UTC), Count: 40}}
}

// deferralSummary reduces state to its chain seq and per-launch counts.
func deferralSummary(state *model.BuildObjectState) (int, map[string]int) {
	if state == nil || len(state.DispatchDeferrals) == 0 {
		return deferralSeqOf(state), nil
	}
	counts := make(map[string]int, len(state.DispatchDeferrals))
	for key, entry := range state.DispatchDeferrals {
		counts[key] = entry.Count
	}
	return state.DeferralReconcileSeq, counts
}

func deferralSeqOf(state *model.BuildObjectState) int {
	if state == nil {
		return 0
	}
	return state.DeferralReconcileSeq
}

// leaseStore records launch reservations and failures and reflects them in
// later reads, like the PostgreSQL authority does.
type leaseStore struct {
	store.VanadisStore

	reserved   []string
	failed     []string
	reserveErr error
	failErr    error
	skipFail   bool
}

func (s *leaseStore) ReserveChunkForLaunch(_ context.Context, chunkID string) error {
	s.reserved = append(s.reserved, chunkID)
	return s.reserveErr
}

func (s *leaseStore) FailChunk(_ context.Context, chunkID, reason, dispatchID string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	if s.failErr != nil || s.skipFail {
		return nil, nil, false, s.failErr
	}
	s.failed = append(s.failed, chunkID+" "+dispatchID+" "+reason)
	return nil, nil, true, nil
}

func (s *leaseStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	b, chunks, err := s.VanadisStore.GetBuild(ctx, buildID)
	if err != nil || b == nil {
		return b, chunks, err
	}
	for i := range chunks {
		for _, f := range s.failed {
			if strings.HasPrefix(f, chunks[i].ID+" ") {
				chunks[i].Status, b.Status = statusFailed, statusFailed
			}
		}
	}
	return b, chunks, nil
}

func TestBuildObject_Transitions_WidgetSyncTable(t *testing.T) {
	t.Parallel()

	runningBuild := model.VanadisBuild{ID: "b-trans", Title: "Build", Status: "running", ThreadID: "th-1", StatusMessageID: "msg-123"}
	rollout := &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}
	checks := map[string]string{"lint": "passed"}

	makePrior := func(status string, pr int, chk map[string]string) *model.BuildObjectState {
		return &model.BuildObjectState{
			Build: runningBuild,
			Chunks: []model.VanadisChunk{{
				ID:           "c1",
				BuildID:      "b-trans",
				Status:       status,
				Repo:         "aerial",
				Mode:         "pr",
				PRNumber:     pr,
				CIChecks:     chk,
				RolloutState: rollout,
			}},
			Launched: map[string]bool{"c1": true},
		}
	}

	tests := []struct {
		name       string
		chunks     []model.VanadisChunk
		priorState *model.BuildObjectState
		wantSync   bool
	}{
		{
			name:       "skips_widget_sync_when_state_unchanged",
			chunks:     []model.VanadisChunk{{ID: "c1", BuildID: "b-trans", Status: "running", Repo: "aerial", Mode: "pr"}},
			priorState: makePrior("running", 0, checks),
			wantSync:   false,
		},
		{
			name:       "syncs_widget_when_active_step_ci_checks_change",
			chunks:     []model.VanadisChunk{{ID: "c1", BuildID: "b-trans", Status: "pr_opened", Repo: "aerial", Mode: "pr", CIChecks: map[string]string{"lint": "pending"}}},
			priorState: makePrior("pr_opened", 0, checks),
			wantSync:   true,
		},
		{
			name:       "syncs_widget_when_chunk_status_changes",
			chunks:     []model.VanadisChunk{{ID: "c1", BuildID: "b-trans", Status: "running", Repo: "aerial", Mode: "pr"}},
			priorState: makePrior("pending", 0, checks),
			wantSync:   true,
		},
		{
			name:       "syncs_widget_when_chunk_pr_number_changes",
			chunks:     []model.VanadisChunk{{ID: "c1", BuildID: "b-trans", Status: "pr_opened", Repo: "aerial", Mode: "pr", PRNumber: 101}},
			priorState: makePrior("pr_opened", 0, checks),
			wantSync:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := runningBuild
			st, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: tc.chunks, ExecTag: "UPDATE 1"})
			var widgetCalls atomic.Int32
			eri := &mockEriClient{
				widgetFn: func(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
					widgetCalls.Add(1)
					return "msg-123", nil
				},
			}
			exec := executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			rt := newFakeObjectRuntime("b-trans")
			rt.state = tc.priorState
			res, err := bo.executeObjectTransition(rt, "reconcile", nil)
			require.NoError(t, err)
			assert.Equal(t, "running", res.Status)
			assert.Equal(t, tc.wantSync, widgetCalls.Load() > 0)
		})
	}
}

func TestBuildObject_AuthoritativeTransitions(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "test-token", "example-owner", nil)

	b := model.VanadisBuild{ID: "b-trans", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr"}
	c2 := model.VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "pending", Repo: "aerial", Mode: "pr", DependsOn: []string{"c1"}}

	st, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1, c2}, ExecTag: "UPDATE 1"})
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	ctx := makeObjectMockContext(t, "b-trans")

	// Reconcile launches c1 (no dependencies)
	res, err := bo.Reconcile(ctx, ReconcileBuildRequest{Reason: "tick"})
	assert.NoError(t, err)
	assert.Equal(t, "b-trans", res.BuildID)
	assert.Equal(t, "running", res.Status)
	assert.Contains(t, res.LaunchedChunks, "c1")

	// Mutate: chunk 1 merged -> launches c2
	c1Merged := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "merged", Repo: "aerial", Mode: "pr"}
	storeMerged, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1Merged, c2}, ExecTag: "UPDATE 1"})
	boMerged := NewBuildObject("", nil).WithAuthority(storeMerged, exec)

	resMut, err := boMerged.Mutate(ctx, BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "running", resMut.Status)
	assert.Contains(t, resMut.LaunchedChunks, "c2")

	// Merged + completed -> still running until all chunks reach completed (deploy_success)
	c2Done := model.VanadisChunk{ID: "c2", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeStillDeploying, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1Merged, c2Done}, ExecTag: "UPDATE 1"})
	boStillDeploying := NewBuildObject("", nil).WithAuthority(storeStillDeploying, exec)

	resDeploying, err := boStillDeploying.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusRunning, resDeploying.Status)

	// All completed -> status completed
	c1Done := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "completed", Repo: "aerial", Mode: "pr"}
	storeDone, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{c1Done, c2Done}, ExecTag: "UPDATE 1"})
	boDone := NewBuildObject("", nil).WithAuthority(storeDone, exec)

	resDone, err := boDone.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusCompleted, resDone.Status)
	assert.Equal(t, statusCompleted, eri.LastWidget.Status)

	// Chunk failed -> status failed
	cFailed := model.VanadisChunk{ID: "c1", BuildID: "b-trans", Status: "failed", Repo: "aerial", Mode: "pr"}
	storeFail, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Chunks: []model.VanadisChunk{cFailed}, ExecTag: "UPDATE 1"})
	boFail := NewBuildObject("", nil).WithAuthority(storeFail, exec)

	resFail, err := boFail.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusFailed, resFail.Status)
}

func TestBuildObject_MutationActionsAndTerminalGuards(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := executor.NewExecutor("", eri, bus, "token", "example-owner", nil)
	b := model.VanadisBuild{ID: "b-ext", Title: "Build", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-ext", Status: "running", Repo: "aerial", Mode: "pr"}

	st, _ := open(dbScript{Row: &sqltest.Row{Build: &b}, Routes: []sqlRoute{{Match: "SELECT c.id, c.build_id", Row: sqltest.FailRow("running", "running")}}, Chunks: []model.VanadisChunk{c1}, ExecTag: "UPDATE 1"})
	bo := NewBuildObject("", nil).WithAuthority(st, exec)
	ctx := makeObjectMockContext(t, "b-ext")

	// chunk_deploy_success
	res, err := bo.Mutate(ctx, BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1"})
	assert.NoError(t, err)
	assert.Equal(t, "b-ext", res.BuildID)

	// action_completed
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "action_completed", ChunkID: "c1"})
	assert.NoError(t, err)

	// chunk_failed
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", Reason: "boom"})
	assert.NoError(t, err)

	// cancelled
	_, err = bo.Mutate(ctx, BuildMutationRequest{Action: "cancelled", ChunkID: "c1", Reason: "stopped"})
	assert.NoError(t, err)

	// build not found
	storeNotFound, _ := open(dbScript{Row: sqltest.ErrRow(store.ErrBuildNotFound)})
	boNotFound := NewBuildObject("", nil).WithAuthority(storeNotFound, exec)
	res, err = boNotFound.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, statusNotFound, res.Status)

	// build already terminal
	bTerminal := model.VanadisBuild{ID: "b-ext", Status: "completed"}
	storeTerm, _ := open(dbScript{Row: &sqltest.Row{Build: &bTerminal}})
	boTerm := NewBuildObject("", nil).WithAuthority(storeTerm, exec)
	res, err = boTerm.Reconcile(ctx, ReconcileBuildRequest{})
	assert.NoError(t, err)
	assert.Equal(t, "completed", res.Status)

	// nil store
	boNil := NewBuildObject("", nil)
	_, err = boNil.executeAuthoritativeTransition(context.Background(), "b1", nil, nil, nil, false)
	assert.Error(t, err)
}

func TestBuildObject_StoreNotConfiguredTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mutation  *BuildMutationRequest
		nilObject bool
	}{
		{name: "reconcile without store reports terminal store error"},
		{name: "mutate without store reports terminal store error", mutation: &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}},
		{name: "nil object skips chunk failure notification", nilObject: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilObject {
				var nilBO *BuildObject
				assert.NoError(t, nilBO.notifyChunkFailure(mockRunContext{}, model.VanadisBuild{ID: "b1"}, "c1", "boom"))
				return
			}
			bo := NewBuildObject("http://localhost:4025", nil)
			rt := newFakeObjectRuntime("b-trans")
			_, err := bo.executeObjectTransition(rt, "reconcile", tc.mutation)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "store not configured")
		})
	}
}

func TestBuildObject_AuthoritativeTransitions_LaunchAndAllDone(t *testing.T) {
	t.Parallel()

	// 1. Pending chunk eligible for launch
	bLaunch := model.VanadisBuild{
		ID:        "b-launch",
		ThreadID:  "th-1",
		Title:     "Launch Build",
		Objective: "Test Chunk Launch",
		Status:    "running",
	}
	chunkPending := model.VanadisChunk{
		ID:         "c-pending",
		BuildID:    "b-launch",
		ChunkIndex: 0,
		Title:      "Pending Chunk",
		Repo:       "aerial",
		Status:     "pending",
		Mode:       "pr",
	}

	eriMock := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eriMock, nil, "token", "example-owner", nil)
	storeLaunch, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &bLaunch},
		Chunks:   []model.VanadisChunk{chunkPending},
		ExecTag:  "UPDATE 1",
	})

	bo := NewBuildObject("", nil).WithAuthority(storeLaunch, exec)
	out, err := bo.executeAuthoritativeTransition(context.Background(), "b-launch", nil, nil, map[string]bool{"prior-chk": true}, false)
	assert.NoError(t, err)
	assert.Equal(t, "running", out.BuildStatus)
	assert.Contains(t, out.LaunchedChunks, "c-pending")
	assert.True(t, out.LaunchedMap["c-pending"])

	// 1b. Running chunk with PRNumber == 0 and absent from priorLaunched is dispatched.
	// Already launched running chunk or chunk with PRNumber > 0 is not re-dispatched.
	chunkRunningUnlaunched := model.VanadisChunk{
		ID:         "c-running-unlaunched",
		BuildID:    "b-launch-running",
		ChunkIndex: 0,
		Title:      "Running Unlaunched Chunk",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningAlreadyLaunched := model.VanadisChunk{
		ID:         "c-running-already",
		BuildID:    "b-launch-running",
		ChunkIndex: 1,
		Title:      "Running Already Launched",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   0,
	}
	chunkRunningWithPR := model.VanadisChunk{
		ID:         "c-running-pr",
		BuildID:    "b-launch-running",
		ChunkIndex: 2,
		Title:      "Running With PR",
		Repo:       "aerial",
		Status:     "running",
		Mode:       "pr",
		PRNumber:   42,
	}
	bLaunchRunning := model.VanadisBuild{
		ID:        "b-launch-running",
		ThreadID:  "th-1",
		Title:     "Launch Running Build",
		Objective: "Test Running Unlaunched Recovery",
		Status:    "running",
	}
	storeRunning, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &bLaunchRunning},
		Chunks:   []model.VanadisChunk{chunkRunningUnlaunched, chunkRunningAlreadyLaunched, chunkRunningWithPR},
		ExecTag:  "UPDATE 1",
	})
	boRunning := NewBuildObject("", nil).WithAuthority(storeRunning, exec)
	outRunning, errRunning := boRunning.executeAuthoritativeTransition(
		context.Background(),
		"b-launch-running",
		nil,
		nil,
		map[string]bool{"c-running-already": true},
		false,
	)
	assert.NoError(t, errRunning)
	assert.Equal(t, "running", outRunning.BuildStatus)
	assert.Contains(t, outRunning.LaunchedChunks, "c-running-unlaunched")
	assert.NotContains(t, outRunning.LaunchedChunks, "c-running-already")
	assert.NotContains(t, outRunning.LaunchedChunks, "c-running-pr")
	assert.True(t, outRunning.LaunchedMap["c-running-unlaunched"])
	assert.True(t, outRunning.LaunchedMap["c-running-already"])

	// Store GetBuild error branch
	storeGetErr, _ := open(dbScript{Row: sqltest.ErrRow(errors.New("db fail"))})
	boGetErr := NewBuildObject("", nil).WithAuthority(storeGetErr, nil)
	_, errGetErr := boGetErr.executeAuthoritativeTransition(context.Background(), "b-fail", nil, nil, nil, false)
	assert.Error(t, errGetErr)

	// 2. All chunks completed -> build completes
	chunkDone := model.VanadisChunk{
		ID:         "c-done",
		BuildID:    "b-done",
		ChunkIndex: 0,
		Title:      "Done Chunk",
		Repo:       "aerial",
		Status:     "completed",
		Mode:       "pr",
	}
	bDone := model.VanadisBuild{ID: "b-done", Status: "running", ThreadID: "th-1"}
	storeCompleted, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &bDone},
		Chunks:   []model.VanadisChunk{chunkDone},
		ExecTag:  "UPDATE 1",
	})
	boDone := NewBuildObject("", nil).WithAuthority(storeCompleted, exec)
	outDone, err := boDone.executeAuthoritativeTransition(context.Background(), "b-done", nil, nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "completed", outDone.BuildStatus)

	// 3. One chunk failed -> build fails
	chunkFailed := model.VanadisChunk{
		ID:         "c-failed",
		BuildID:    "b-fail",
		ChunkIndex: 0,
		Title:      "Failed Chunk",
		Repo:       "aerial",
		Status:     "failed",
		Mode:       "pr",
	}
	bFail := model.VanadisBuild{ID: "b-fail", Status: "running", ThreadID: "th-1"}
	storeFailed, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &bFail},
		Chunks:   []model.VanadisChunk{chunkFailed},
		ExecTag:  "UPDATE 1",
	})
	boFailed := NewBuildObject("", nil).WithAuthority(storeFailed, exec)
	outFailed, err := boFailed.executeAuthoritativeTransition(context.Background(), "b-fail", nil, nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "failed", outFailed.BuildStatus)
}

func TestBuildObject_ReconcileTerminalStatuses(t *testing.T) {
	t.Parallel()

	// Terminal statuses should return without error and not transition
	for _, status := range []string{"completed", "failed", "cancelled"} {
		bTerm := model.VanadisBuild{ID: "b-term", Status: status, ThreadID: "th-1"}
		storeTerm, _ := open(dbScript{
			BuildRow: &sqltest.Row{Build: &bTerm},
			Chunks:   []model.VanadisChunk{{ID: "c1", Status: status}},
		})
		bo := NewBuildObject("", nil).WithAuthority(storeTerm, nil)
		out, err := bo.executeAuthoritativeTransition(context.Background(), "b-term", nil, nil, nil, false)
		assert.NoError(t, err)
		assert.Equal(t, status, out.BuildStatus)
	}

	// Build not found returns not_found
	storeNF, _ := open(dbScript{Row: sqltest.ErrRow(store.ErrBuildNotFound)})
	boNF := NewBuildObject("", nil).WithAuthority(storeNF, nil)
	outNF, err := boNF.executeAuthoritativeTransition(context.Background(), "b-nf", nil, nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, "not_found", outNF.BuildStatus)
}

func TestBuildObject_ObjectTransitionsDirect(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b-active", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-active", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "pr", Repo: "aerial"}
	st, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &b1},
		Chunks:   []model.VanadisChunk{c1},
		ExecTag:  "UPDATE 1",
	})
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)

	rt := newFakeObjectRuntime("b-active")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})

	// executeObjectTransition with nil mutation
	res, err := bo.executeObjectTransition(rt, "reconcile", nil)
	assert.NoError(t, err)
	assert.Equal(t, "running", res.Status)

	// executeObjectTransition with complete action mutation
	cAction := model.VanadisChunk{ID: "c-act", BuildID: "b-active", ChunkIndex: 0, Title: "CAct", Status: "running", Mode: "action"}
	storeAct, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &b1},
		Chunks:   []model.VanadisChunk{cAction},
		ExecTag:  "UPDATE 1",
	})
	boAct := NewBuildObject("", nil).WithAuthority(storeAct, exec)
	rtAct := newFakeObjectRuntime("b-active")
	rtAct.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{cAction}})
	mut := &BuildMutationRequest{Action: "complete_chunk", ChunkID: "c-act"}
	resAct, err := boAct.executeObjectTransition(rtAct, "mutate", mut)
	assert.NoError(t, err)
	assert.Equal(t, "running", resAct.Status)

	// executeObjectTransition with cancel mutation
	bCancel := model.VanadisBuild{ID: "b-cancel", Status: "cancelled", ThreadID: "th-1"}
	storeCancel, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &bCancel},
		Chunks:   []model.VanadisChunk{c1},
		ExecTag:  "UPDATE 1",
	})
	boCancel := NewBuildObject("", nil).WithAuthority(storeCancel, exec)
	rtCancel := newFakeObjectRuntime("b-cancel")
	rtCancel.SetState(model.BuildObjectState{Build: bCancel, Chunks: []model.VanadisChunk{c1}})
	mutCancel := &BuildMutationRequest{Action: "cancelled", Reason: "user requested"}
	resCancel, err := boCancel.executeObjectTransition(rtCancel, "mutate-cancel", mutCancel)
	assert.NoError(t, err)
	assert.Equal(t, "cancelled", resCancel.Status)
	assert.True(t, rtCancel.cleared)
}

func openAuthorityHandlerHarness(t *testing.T) (*BuildObject, *fakeObjectRuntime) {
	t.Helper()
	b1 := model.VanadisBuild{ID: "b-cov", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-cov", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	st, _ := open(dbScript{
		BuildRow: &sqltest.Row{Build: &b1},
		Chunks:   []model.VanadisChunk{c1},
		Routes: []sqlRoute{
			{Match: "SELECT mode FROM vanadis_chunks", Row: sqltest.ValuesRow("action")},
			{Match: "UPDATE vanadis_chunks", Row: sqltest.ValuesRow("b-cov", "c1", "th-1")},
			{Match: "WHERE c.repo = ANY($1) AND c.pr_number = $2", Row: sqltest.ValuesRow("c1", "b-cov", 0, "medium", false)},
			{Match: "SELECT id, build_id, chunk_index, mode, title, status", Row: sqltest.ValuesRow("c1", "b-cov", 0, "action", "C1", "running", 0)},
			{Match: "SELECT id, thread_id, tier", Row: sqltest.ValuesRow("b-cov", "th-1", 1, "Build Title", "Build Obj", "running", "")},
			{Match: "SELECT COUNT(*)", Row: sqltest.ValuesRow(0)},
			{Match: "SELECT c.id, c.build_id, c.chunk_index, c.mode", Row: sqltest.FailRow("running", "running")},
		},
		ExecTag: "UPDATE 1",
	})
	exec := executor.NewExecutor(t.TempDir(), &mockEriClient{}, &mockBusExecutor{}, "token", "example-owner", nil)
	bo := NewBuildObject("", nil).WithAuthority(st, exec)
	rt := newFakeObjectRuntime("b-cov")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})
	return bo, rt
}

func TestBuildObject_AuthorityHandlerOperations(t *testing.T) {
	t.Parallel()
	bo, _ := openAuthorityHandlerHarness(t)
	ctx := makeObjectMockContext(t, "b-cov")

	// Smoke-cover each authority handler once; routes are reusable stubs.
	_, _ = bo.FailChunk(ctx, model.FailChunkRequest{ChunkID: "c1", Reason: "chunk fail"})
	_ = bo.RemoveChunk(ctx, RemoveChunkRequest{ChunkRef: "c1"})
	_, _ = bo.AttachPR(ctx, store.AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"})
	_, _ = bo.Cancel(ctx, CancelBuildRequest{Reason: "user cancel"})
	_ = bo.OnDispatchOutcome(ctx, pubsub.DispatchOutcomeEvent{VanadisChunkID: "c1", Status: "failed", Error: "boom"})
	_ = bo.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "closed", Status: "closed"})

	mutations := []struct {
		name    string
		action  string
		chunkID string
		reason  string
	}{
		{name: "chunk merged", action: "chunk_merged", chunkID: "c1", reason: "test"},
		{name: "chunk deploy success", action: "chunk_deploy_success", chunkID: "c1", reason: "test"},
		{name: "action completed", action: "action_completed", chunkID: "c1", reason: "test"},
		{name: "chunk failed", action: "chunk_failed", chunkID: "c1", reason: "test"},
		{name: "cancelled", action: "cancelled", chunkID: "c1", reason: "test"},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			boMut, rtMut := openAuthorityHandlerHarness(t)
			_, _ = boMut.executeObjectTransition(rtMut, "mut-"+tc.action, &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, Reason: tc.reason})
		})
	}

	terminals := []struct {
		name        string
		buildID     string
		chunkStatus string
		wantStatus  string
	}{
		{name: "all completed completes build", buildID: "b-done", chunkStatus: "completed", wantStatus: statusCompleted},
		{name: "chunk failed fails build", buildID: "b-fail", chunkStatus: "failed", wantStatus: statusFailed},
	}
	for _, tc := range terminals {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			boTerm, _ := openAuthorityHandlerHarness(t)
			bTerm := model.VanadisBuild{ID: tc.buildID, Status: "running", ThreadID: "th-1"}
			termStatus, err := boTerm.evaluateBuildTerminalStatus(context.Background(), &bTerm, []model.VanadisChunk{{ID: "c1", Status: tc.chunkStatus}}, map[string]bool{}, false)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStatus, termStatus)
		})
	}
}

type skippedCompletionStore struct {
	store.VanadisStore

	getBuildCalls int
}

func (s *skippedCompletionStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	s.getBuildCalls++
	if s.getBuildCalls > 1 {
		return nil, nil, errors.New("get build fail")
	}
	return &model.VanadisBuild{ID: buildID, Status: "running", ThreadID: "th-1"},
		[]model.VanadisChunk{{ID: "c1", BuildID: buildID}}, nil
}

func (*skippedCompletionStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	return &store.VanadisAdvancementResult{}, nil
}

type replayFailStore struct {
	store.VanadisStore
}

func (*replayFailStore) GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: buildID, Status: statusCompleted, ThreadID: "th-1"},
		[]model.VanadisChunk{{ID: "c1", BuildID: buildID, Mode: "action"}}, nil
}

func (*replayFailStore) CompleteActionChunk(ctx context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	return &store.VanadisAdvancementResult{CompletionAccepted: true}, nil
}

type errDeploymentAttemptsRuntime struct {
	*fakeObjectRuntime
}

func (errDeploymentAttemptsRuntime) GetAttempts() (map[string]deploygate.Attempt, error) {
	return nil, errors.New("attempts read fail")
}

func (errDeploymentAttemptsRuntime) SetAttempts(map[string]deploygate.Attempt) {}

func (errDeploymentAttemptsRuntime) Now() (time.Time, error) {
	return time.Now().UTC(), nil
}

func (errDeploymentAttemptsRuntime) GetPendingStarts() (map[string]deploygate.PendingStart, error) {
	return map[string]deploygate.PendingStart{}, nil
}

func (errDeploymentAttemptsRuntime) SetPendingStarts(map[string]deploygate.PendingStart) {}

func (errDeploymentAttemptsRuntime) ScheduleDeadline(deploygate.DeploymentDeadlineRequest, time.Duration) {
}

func (e errDeploymentAttemptsRuntime) RunStep(name string, fn func(ctx context.Context) (deploygate.StepOutput, error)) (deploygate.StepOutput, error) {
	out, err := e.RunReconcileStep(name, func(ctx context.Context) (reconcileStepOutput, error) {
		o, runErr := fn(ctx)
		return reconcileStepOutput(o), runErr
	})
	return deploygate.StepOutput(out), err
}

func TestBuildObject_ReconcileErrorBranches(t *testing.T) {
	t.Parallel()

	replays := []struct {
		name         string
		buildStatus  string
		action       string
		chunkID      string
		chunkMode    string
		chunkPR      int
		reason       string
		failNotify   bool
		wantErr      bool
		wantNotified bool
	}{
		{name: "completed action completion notifies", buildStatus: statusCompleted, action: "action_completed", chunkID: "c1", chunkMode: "action", reason: "test", wantNotified: true},
		{name: "completed action completion notify failure", buildStatus: statusCompleted, action: "action_completed", chunkID: "c1", chunkMode: "action", reason: "test", failNotify: true, wantErr: true},
		{name: "failed chunk failure leaves the wake to reconcile", buildStatus: statusFailed, action: "chunk_failed", chunkID: "c1", chunkMode: "pr", reason: "boom"},
		{name: "failed chunk failure ignores wake failures", buildStatus: statusFailed, action: "chunk_failed", chunkID: "c1", chunkMode: "pr", reason: "boom", failNotify: true},
	}
	for _, tc := range replays {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bo, _ := openAuthorityHandlerHarness(t)
			eri := &mockEriClient{}
			if tc.failNotify {
				eri.promptFn = func(context.Context, string, string) error { return errors.New("prompt fail") }
				eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
					return "", errors.New("widget fail")
				}
			}
			bo.executor = executor.NewExecutor(t.TempDir(), eri, &mockBusExecutor{}, "token", "example-owner", nil)
			build := &model.VanadisBuild{ID: "b-replay", Status: tc.buildStatus, ThreadID: "th-1"}
			chunks := []model.VanadisChunk{{ID: tc.chunkID, BuildID: "b-replay", Mode: tc.chunkMode, PRNumber: tc.chunkPR}}
			mutation := &BuildMutationRequest{Action: tc.action, ChunkID: tc.chunkID, Reason: tc.reason}
			notified, err := bo.replayTerminalMutationEffects(context.Background(), build, chunks, mutation, false)
			if tc.wantErr {
				assert.Error(t, err)
				assert.False(t, notified)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNotified, notified)
		})
	}

	bMerge := model.VanadisBuild{ID: "b-merge", Status: "running", ThreadID: "th-1"}
	cMerge := model.VanadisChunk{ID: "c1", BuildID: "b-merge", Status: "open", Mode: "pr", Repo: "aerial"}
	stMerge, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &bMerge}, Chunks: []model.VanadisChunk{cMerge}, ExecTag: "UPDATE 1"})
	busErr := &mockBusError{err: errors.New("pub fail")}
	boMerge := NewBuildObject("", nil).WithAuthority(stMerge, executor.NewExecutor(t.TempDir(), &mockEriClient{}, busErr, "token", "example-owner", nil))
	assert.NoError(t, boMerge.applyMutation(context.Background(), "b-merge", &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}))

	bBegin := model.VanadisBuild{ID: "b-begin", Status: "running", ThreadID: "th-1"}
	cBegin := model.VanadisChunk{ID: "c1", BuildID: "b-begin", Status: "running", Mode: "action"}
	stBegin, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &bBegin}, Chunks: []model.VanadisChunk{cBegin}, BeginErr: errors.New("begin fail")})
	boBegin := NewBuildObject("", nil).WithAuthority(stBegin, nil)
	assert.Error(t, boBegin.applyMutation(context.Background(), "b-begin", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}))

	bTerm := model.VanadisBuild{ID: "b-term", Status: "completed", ThreadID: "th-1"}
	stTerm, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &bTerm}})
	boTerm := NewBuildObject("", nil).WithAuthority(stTerm, nil)
	outTerm, err := boTerm.executeAuthoritativeTransition(context.Background(), "b-term", nil, nil, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, statusCompleted, outTerm.BuildStatus)

	boSkip := NewBuildObject("", nil).WithAuthority(&skippedCompletionStore{}, nil)
	_, err = boSkip.executeAuthoritativeTransition(context.Background(), "b-skip", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}, nil, nil, false)
	assert.Error(t, err)

	eriReplayFail := &mockEriClient{}
	eriReplayFail.promptFn = func(context.Context, string, string) error { return errors.New("prompt fail") }
	eriReplayFail.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
		return "", errors.New("widget fail")
	}
	boReplayFail := NewBuildObject("", nil).WithAuthority(&replayFailStore{},
		executor.NewExecutor(t.TempDir(), eriReplayFail, &mockBusExecutor{}, "token", "example-owner", nil))
	rtReplayFail := newFakeObjectRuntime("b-replay-fail")
	_, err = boReplayFail.executeObjectTransition(rtReplayFail, labelMutateBuild,
		&BuildMutationRequest{Action: "action_completed", ChunkID: "c1"})
	assert.Error(t, err)

	bDeadline := model.VanadisBuild{ID: "b-deadline", Status: "running", ThreadID: "th-1"}
	cDeadline := model.VanadisChunk{ID: "c1", BuildID: "b-deadline", Status: statusDeploying, Mode: "pr", Repo: "aerial", PRNumber: 1, MergeSHA: "sha"}
	stDeadline, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &bDeadline}, Chunks: []model.VanadisChunk{cDeadline}, ExecTag: "UPDATE 1"})
	eriDeadlineWidgetFail := &mockEriClient{}
	eriDeadlineWidgetFail.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
		return "", errors.New("widget fail")
	}
	boDeadline := NewBuildObject("", nil).WithAuthority(stDeadline,
		executor.NewExecutor(t.TempDir(), eriDeadlineWidgetFail, &mockBusExecutor{}, "token", "example-owner", nil))
	rtDeadline := &errDeploymentAttemptsRuntime{fakeObjectRuntime: newFakeObjectRuntime("b-deadline")}
	rtDeadline.SetState(model.BuildObjectState{Build: bDeadline, Chunks: []model.VanadisChunk{cDeadline}})
	_, err = boDeadline.executeObjectTransition(rtDeadline, "reconcile", nil)
	assert.Error(t, err)
}

type replayNotifyRuntime struct {
	*fakeObjectRuntime

	primaryErr error
	notifyDone bool
}

func (r *replayNotifyRuntime) RunReconcileStep(name string, fn func(ctx context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	if name == "notify-transition-error" {
		r.stepsRun = append(r.stepsRun, name)
		if r.notifyDone {
			return reconcileStepOutput{}, nil
		}
		out, err := fn(context.Background())
		if err != nil {
			return out, err
		}
		r.notifyDone = true
		return out, nil
	}
	if r.primaryErr != nil {
		r.stepsRun = append(r.stepsRun, name)
		return reconcileStepOutput{}, r.primaryErr
	}
	return fn(context.Background())
}

func TestRunTransitionStep_NotifyOnceAcrossReplay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		failFirst      int
		wantCalls      int
		wantDeliveries int
	}{
		{name: "delivered once across retries", failFirst: 0, wantCalls: 1, wantDeliveries: 1},
		{name: "failed delivery retried then delivered once", failFirst: 1, wantCalls: 2, wantDeliveries: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := model.VanadisBuild{ID: "b1", ThreadID: "th-1", Title: "Build", Objective: "Obj", Status: "running"}
			st, _ := open(dbScript{BuildRow: &sqltest.Row{Build: &b}})
			widgetCalls, deliveries := 0, 0
			eri := &mockEriClient{}
			eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
				widgetCalls++
				if widgetCalls <= tc.failFirst {
					return "", errors.New("widget down")
				}
				deliveries++
				return "msg-1", nil
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			rt := &replayNotifyRuntime{fakeObjectRuntime: newFakeObjectRuntime("b1"), primaryErr: errors.New("boom")}
			prior := &model.BuildObjectState{Build: b}
			for range 2 {
				_, err := bo.runTransitionStep(rt, "reconcile", "b1", nil, prior, nil, transitionAttempt{})
				require.Error(t, err)
			}
			assert.Equal(t, tc.wantCalls, widgetCalls)
			assert.Equal(t, tc.wantDeliveries, deliveries)
			assert.Contains(t, rt.stepsRun, "notify-transition-error")
		})
	}
}

func TestWidgetStateEqual_BuildFields(t *testing.T) {
	t.Parallel()

	baseBuild := model.VanadisBuild{
		ID:              "b1",
		StatusMessageID: "msg-123",
		Status:          "running",
		Title:           "Build Title",
		Tier:            2,
		ThreadID:        "th-1",
	}
	baseChunk := model.VanadisChunk{
		ID:         "c1",
		BuildID:    "b1",
		ChunkIndex: 0,
		Mode:       "pr",
		Repo:       "aerial",
		Title:      "Chunk 1",
		Status:     "running",
		PRNumber:   10,
	}

	tests := []struct {
		name   string
		prior  *model.BuildObjectState
		build  model.VanadisBuild
		chunks []model.VanadisChunk
		want   bool
	}{
		{
			name:   "nil prior returns false",
			prior:  nil,
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "empty status message id returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", Status: "running", Title: "Build Title", Tier: 2, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "status message id mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", StatusMessageID: "other-msg", Status: "running", Title: "Build Title", Tier: 2, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "status mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", StatusMessageID: "msg-123", Status: "failed", Title: "Build Title", Tier: 2, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "title mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", StatusMessageID: "msg-123", Status: "running", Title: "Other Title", Tier: 2, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "tier mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", StatusMessageID: "msg-123", Status: "running", Title: "Build Title", Tier: 1, ThreadID: "th-1"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "thread id mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  model.VanadisBuild{ID: "b1", StatusMessageID: "msg-123", Status: "running", Title: "Build Title", Tier: 2, ThreadID: "th-2"},
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "identical build state returns true",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, widgetStateEqual(tc.prior, tc.build, tc.chunks))
		})
	}
}

func TestWidgetStateEqual_ChunkFields(t *testing.T) {
	t.Parallel()

	baseBuild := model.VanadisBuild{
		ID:              "b1",
		StatusMessageID: "msg-123",
		Status:          "running",
		Title:           "Build Title",
		Tier:            2,
		ThreadID:        "th-1",
	}
	baseChunk := model.VanadisChunk{
		ID:         "c1",
		BuildID:    "b1",
		ChunkIndex: 0,
		Mode:       "pr",
		Repo:       "aerial",
		Title:      "Chunk 1",
		Status:     "running",
		PRNumber:   10,
	}

	rolloutA := &pubsub.PRRolloutState{
		ExpectedJobs:          []string{"job-a"},
		StartedJobs:           []string{"job-a"},
		DeployedJobs:          []string{"job-a"},
		JobStates:             map[string]string{"job-a": "deployed"},
		KustomizationStatuses: map[string]string{"kust": "healthy"},
	}
	rolloutB := &pubsub.PRRolloutState{
		ExpectedJobs:          []string{"job-b"},
		StartedJobs:           []string{"job-b"},
		DeployedJobs:          []string{"job-b"},
		JobStates:             map[string]string{"job-b": "deployed"},
		KustomizationStatuses: map[string]string{"kust": "healthy"},
	}

	tests := []struct {
		name   string
		prior  *model.BuildObjectState
		build  model.VanadisBuild
		chunks []model.VanadisChunk
		want   bool
	}{
		{
			name:   "chunk count mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk, baseChunk},
			want:   false,
		},
		{
			name:   "chunk id mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c-old", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk index mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk mode mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "action", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk repo mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "other", Title: "Chunk 1", Status: "running", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk title mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Other", Status: "running", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk status mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "pending", PRNumber: 10}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk pr number mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 20}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "chunk active step mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "pr_opened", PRNumber: 10, CIChecks: map[string]string{"lint": "passed"}}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "pr_opened", PRNumber: 10, CIChecks: map[string]string{"lint": "pending"}}},
			want:   false,
		},
		{
			name:   "chunk rollout state mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10, RolloutState: rolloutA}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10, RolloutState: rolloutB}},
			want:   false,
		},
		{
			name:   "chunk rollout state nil mismatch returns false",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10, RolloutState: rolloutA}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   false,
		},
		{
			name:   "identical state with nil rollout returns true",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{baseChunk}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{baseChunk},
			want:   true,
		},
		{
			name:   "identical state with matching rollout returns true",
			prior:  &model.BuildObjectState{Build: baseBuild, Chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10, RolloutState: rolloutA}}},
			build:  baseBuild,
			chunks: []model.VanadisChunk{{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "Chunk 1", Status: "running", PRNumber: 10, RolloutState: rolloutA}},
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, widgetStateEqual(tc.prior, tc.build, tc.chunks))
		})
	}
}

func TestBuildObject_EvaluateBuildTerminalStatus_SkipWidgetSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		skipSync  bool
		wantCalls int
	}{
		{name: "skip sync true skips widget call", skipSync: true, wantCalls: 0},
		{name: "skip sync false executes widget call", skipSync: false, wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var widgetCalls atomic.Int32
			eri := &mockEriClient{
				widgetFn: func(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
					widgetCalls.Add(1)
					return "msg-123", nil
				},
			}
			exec := executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
			bo := NewBuildObject("", nil).WithAuthority(&mockStoreForLaunch{}, exec)
			bld := model.VanadisBuild{ID: "b-test", Status: "running", StatusMessageID: "msg-123", ThreadID: "th-1"}
			chunks := []model.VanadisChunk{{ID: "c1", Status: "running"}}

			status, err := bo.evaluateBuildTerminalStatus(context.Background(), &bld, chunks, map[string]bool{}, tc.skipSync)
			require.NoError(t, err)
			assert.Equal(t, "running", status)
			assert.Equal(t, tc.wantCalls, int(widgetCalls.Load()))
		})
	}
}
