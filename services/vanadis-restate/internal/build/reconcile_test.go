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
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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
				st, _ := seededStore(t, []model.VanadisBuild{{ID: "b-validate", Status: "running", ThreadID: "th-1"}}, nil)
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

// ck is one chunk of build b1; deps become DependsOn. Pr chunks target
// aerial; action chunks without a PR carry no repo, as the schema requires.
func ck(id, status, mode string, pr int, deps ...string) model.VanadisChunk {
	repo := "aerial"
	if mode == "action" && pr == 0 {
		repo = ""
	}
	return model.VanadisChunk{ID: id, BuildID: "b1", Status: status, Repo: repo, Mode: mode, PRNumber: pr, DependsOn: deps}
}

// bld is tier-2 build b1 at status with a posted status widget.
func bld(status string) model.VanadisBuild {
	return model.VanadisBuild{ID: "b1", Title: "Build", Tier: 2, Status: status, ThreadID: "th-1", StatusMessageID: "msg-123"}
}

// objState is the object state holding build and chunks.
func objState(build model.VanadisBuild, chunks ...model.VanadisChunk) *model.BuildObjectState {
	return &model.BuildObjectState{Build: build, Chunks: chunks}
}

// launchedState is a running-build object state that already launched ids.
func launchedState(chunks []model.VanadisChunk, ids ...string) *model.BuildObjectState {
	state := objState(bld(statusRunning), chunks...)
	state.Launched = make(map[string]bool, len(ids))
	for _, id := range ids {
		state.Launched[id] = true
	}
	return state
}

// leaseState is a running-build object state carrying dispatch deferrals.
func leaseState(deferrals map[string]model.DispatchDeferral, seq int) *model.BuildObjectState {
	return &model.BuildObjectState{Build: bld(statusRunning), DispatchDeferrals: deferrals, DeferralReconcileSeq: seq}
}

// ciChunk is a pr chunk c1 with one lint check and the given rollout.
func ciChunk(status, lint string, pr int, rollout *pubsub.PRRolloutState) model.VanadisChunk {
	c := ck("c1", status, "pr", pr)
	c.CIChecks, c.RolloutState = map[string]string{"lint": lint}, rollout
	return c
}

// widgetStatus is the build status of the last widget sync, "" when none ran.
func widgetStatus(c *eritest.Client) string {
	if c.LastWidget == nil {
		return ""
	}
	return c.LastWidget.Status
}

func cks(cs ...model.VanadisChunk) []model.VanadisChunk { return cs }

func TestExecuteObjectTransition(t *testing.T) {
	t.Parallel()

	const expired = "RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"
	rollout := &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}
	running, pending := ck("c1", "running", "pr", 0), ck("c1", "pending", "pr", 0)
	action := ck("c1", "running", "action", 0)
	syncedPrior := func(s string) *model.BuildObjectState {
		return launchedState(cks(ciChunk(s, "passed", 0, rollout)), "c1")
	}

	tests := []struct {
		name          string
		build         model.VanadisBuild
		chunks        []model.VanadisChunk
		missing       bool
		closed        bool
		nilStore      bool
		mutation      *BuildMutationRequest
		prior         *model.BuildObjectState
		tick          int
		dispatchErr   error
		failErr       error
		wantStatus    string
		wantChunks    []string
		wantErr       string
		wantCleared   bool
		wantLaunched  map[string]bool
		wantSends     []deferredSend
		wantDeferrals map[string]int
		wantSeq       int
		wantReserved  []string
		wantFailed    []string
		wantWidget    string
		wantStored    map[string]string
		wantBuildRow  string
	}{
		{name: "pending chunk launches", build: bld("running"), chunks: cks(pending), prior: launchedState(nil, "c0"), wantStatus: "running", wantChunks: []string{"c1"}, wantReserved: []string{"c1"}, wantWidget: "running"},
		{name: "deferred launch starts lease chain", build: bld("running"), chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, wantStatus: "running", wantSends: []deferredSend{{Delay: time.Second, Seq: 1}}, wantDeferrals: map[string]int{"c1": 1}, wantSeq: 1, wantWidget: "running"},
		{name: "deferred launch with pending chain schedules nothing", build: bld("running"), chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, prior: leaseState(nil, 3), wantStatus: "running", wantDeferrals: map[string]int{"c1": 1}, wantSeq: 3, wantWidget: "running"},
		{name: "chain tick schedules next tick", build: bld("running"), chunks: leaseChunks("running", 0), dispatchErr: eriwire.ErrDispatchActive, prior: leaseState(nil, 3), tick: 3, wantStatus: "running", wantSends: []deferredSend{{Delay: time.Second, Seq: 4}}, wantDeferrals: map[string]int{"c1": 1}, wantSeq: 4, wantWidget: "running"},
		{name: "lease expiry fails pending action chunk", build: bld("running"), chunks: leaseChunks("pending", 0), dispatchErr: eriwire.ErrDispatchActive, prior: leaseState(expiredLease("c1"), 40), tick: 40, wantStatus: statusFailed, wantReserved: []string{"c1"}, wantFailed: []string{"c1 vanadis-c1-turn-0 " + expired}},
		{name: "lease expiry fails running retry generation", build: bld("running"), chunks: leaseChunks("running", 2), dispatchErr: eriwire.ErrDispatchActive, prior: leaseState(expiredLease("dispatch:vanadis-c1-turn-2"), 0), wantStatus: statusFailed, wantFailed: []string{"c1 vanadis-c1-turn-2 " + expired}},
		{name: "lease expiry fail write error", build: bld("running"), chunks: leaseChunks("running", 0), dispatchErr: eriwire.ErrDispatchActive, prior: leaseState(expiredLease("c1"), 0), failErr: errors.New("database unavailable"), wantErr: "database unavailable"},
		{name: "accepted launch clears lease", build: bld("running"), chunks: leaseChunks("pending", 0), prior: leaseState(expiredLease("c1"), 5), wantStatus: "running", wantChunks: []string{"c1"}, wantReserved: []string{"c1"}, wantWidget: "running"},
		{name: "merged and completed clears state", build: bld("running"), chunks: cks(ck("c1", "completed", "pr", 0)), mutation: &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}, prior: launchedState(nil, "c0"), wantStatus: statusCompleted, wantCleared: true, wantWidget: statusCompleted},
		{name: "failed build keeps state and launched", build: bld(statusFailed), chunks: cks(ck("c1", statusFailed, "pr", 0)), prior: launchedState(nil, "c0"), wantStatus: statusFailed, wantLaunched: map[string]bool{"c0": true}},
		{name: "cancelled build clears state", build: bld(statusCancelled), chunks: cks(ck("c1", statusCancelled, "pr", 0)), wantStatus: statusCancelled, wantCleared: true},
		{name: "cancel mutation clears state", build: bld(statusCancelled), chunks: cks(running), mutation: &BuildMutationRequest{Action: "cancelled", Reason: "user requested"}, prior: objState(bld(statusCancelled), running), wantStatus: statusCancelled, wantCleared: true},
		{name: "completed build reports completed", build: bld(statusCompleted), chunks: cks(ck("c1", statusCompleted, "pr", 0)), wantStatus: statusCompleted, wantCleared: true},
		{name: "build not found", missing: true, wantStatus: statusNotFound, wantCleared: true},
		{name: "store read failure", closed: true, wantErr: "closed pool"},
		{name: "reconcile without store", nilStore: true, wantErr: "store not configured"},
		{name: "mutate without store", nilStore: true, mutation: &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}, wantErr: "store not configured"},
		{name: "unchanged widget state skips sync", build: bld("running"), chunks: cks(ciChunk("running", "passed", 0, rollout)), prior: syncedPrior("running"), wantStatus: "running", wantStored: map[string]string{"c1": "running"}},
		{name: "ci check change syncs widget", build: bld("running"), chunks: cks(ciChunk("pr_opened", "pending", 0, nil)), prior: syncedPrior("pr_opened"), wantStatus: "running", wantWidget: "running"},
		{name: "chunk status change syncs widget", build: bld("running"), chunks: cks(running), prior: syncedPrior("pending"), wantStatus: "running", wantWidget: "running"},
		{name: "pr number change syncs widget", build: bld("running"), chunks: cks(ck("c1", "pr_opened", "pr", 101)), prior: syncedPrior("pr_opened"), wantStatus: "running", wantWidget: "running"},
		{name: "running chunk in prior state", build: bld("running"), chunks: cks(running), prior: objState(bld("running"), running), wantStatus: "running", wantChunks: []string{"c1"}, wantReserved: []string{"c1"}, wantWidget: "running"},
		{name: "complete_chunk on action chunk", build: bld("running"), chunks: cks(action), mutation: &BuildMutationRequest{Action: "complete_chunk", ChunkID: "c1"}, prior: objState(bld("running"), action), wantStatus: "running", wantChunks: []string{"c1"}, wantReserved: []string{"c1"}, wantWidget: "running"},
		{name: "chunk_deploy_success", build: bld("running"), chunks: cks(running), mutation: &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "c1"}, wantStatus: statusCompleted, wantCleared: true, wantWidget: statusCompleted, wantStored: map[string]string{"c1": statusCompleted}, wantBuildRow: statusCompleted},
		{name: "action_completed", build: bld("running"), chunks: cks(running), mutation: &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}, wantStatus: "running"},
		{name: "chunk_failed", build: bld("running"), chunks: cks(running), mutation: &BuildMutationRequest{Action: "chunk_failed", ChunkID: "c1", Reason: "boom"}, wantStatus: statusFailed, wantFailed: []string{"c1  boom"}},
		{name: "cancelled mutation on running build", build: bld("running"), chunks: cks(running), mutation: &BuildMutationRequest{Action: "cancelled", ChunkID: "c1", Reason: "stopped"}, wantStatus: statusCancelled, wantCleared: true, wantStored: map[string]string{"c1": "running"}, wantBuildRow: statusCancelled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := reconcileStore(t, tc.build, tc.chunks, tc.missing, tc.closed)
			st := &leaseStore{VanadisStore: raw, failErr: tc.failErr}
			eri := &eritest.Client{DispatchErr: tc.dispatchErr}
			bo := NewBuildObject("", nil)
			if !tc.nilStore {
				bo = bo.WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil))
			}
			rt := newFakeObjectRuntime("b1")
			rt.state = tc.prior
			res, err := bo.executeObjectTransitionTick(rt, "reconcile", tc.mutation, tc.tick)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "b1", res.BuildID)
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
			assert.Equal(t, tc.wantDeferrals, counts)
			assert.Equal(t, tc.wantWidget, widgetStatus(eri))
			if tc.wantStored != nil {
				assert.Equal(t, tc.wantStored, chunkStatuses(t, raw, "b1"))
			}
			if tc.wantBuildRow != "" {
				stored, _ := loadBuild(t, raw, "b1")
				assert.Equal(t, tc.wantBuildRow, stored.Status)
			}
		})
	}
}

func TestExecuteAuthoritativeTransition(t *testing.T) {
	t.Parallel()

	merged := &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}
	c1, c2 := ck("c1", "pending", "pr", 0), ck("c2", "pending", "pr", 0, "c1")
	c1Merged, c1Done, c2Done := ck("c1", "merged", "pr", 0), ck("c1", "completed", "pr", 0), ck("c2", "completed", "pr", 0, "c1")

	tests := []struct {
		name         string
		build        model.VanadisBuild
		chunks       []model.VanadisChunk
		missing      bool
		closed       bool
		nilStore     bool
		mutation     *BuildMutationRequest
		prior        map[string]bool
		wantStatus   string
		wantChunks   []string
		wantLaunched map[string]bool
		wantErr      string
		wantWidget   string
		wantStored   map[string]string
	}{
		{name: "pending chunk launches", build: bld("running"), chunks: cks(c1), prior: map[string]bool{"prior": true}, wantStatus: "running", wantChunks: []string{"c1"}, wantLaunched: map[string]bool{"prior": true, "c1": true}, wantWidget: "running", wantStored: map[string]string{"c1": "running"}},
		{name: "dependent waits for its dependency", build: bld("running"), chunks: cks(c1, c2), wantStatus: "running", wantChunks: []string{"c1"}, wantLaunched: map[string]bool{"c1": true}, wantWidget: "running", wantStored: map[string]string{"c1": "running", "c2": "pending"}},
		{name: "merge launches the dependent", build: bld("running"), chunks: cks(c1Merged, c2), mutation: merged, wantStatus: "running", wantChunks: []string{"c2"}, wantLaunched: map[string]bool{"c2": true}, wantWidget: "running", wantStored: map[string]string{"c1": "merged", "c2": "running"}},
		{name: "merged plus completed stays running", build: bld("running"), chunks: cks(c1Merged, c2Done), wantStatus: "running", wantLaunched: map[string]bool{}, wantWidget: "running"},
		{name: "all completed completes build", build: bld("running"), chunks: cks(c1Done, c2Done), wantStatus: statusCompleted, wantLaunched: map[string]bool{}, wantWidget: statusCompleted},
		{name: "failed chunk fails build", build: bld("running"), chunks: cks(ck("c1", "failed", "pr", 0)), wantStatus: statusFailed, wantLaunched: map[string]bool{}, wantWidget: statusFailed},
		{name: "running chunk without pr or launch is dispatched", build: bld("running"), chunks: cks(ck("c-run", "running", "pr", 0), ck("c-already", "running", "pr", 0), ck("c-pr", "running", "pr", 42)), prior: map[string]bool{"c-already": true}, wantStatus: "running", wantChunks: []string{"c-run"}, wantLaunched: map[string]bool{"c-already": true, "c-run": true}, wantWidget: "running"},
		{name: "terminal completed build is left alone", build: bld(statusCompleted), chunks: cks(ck("c1", statusCompleted, "pr", 0)), wantStatus: statusCompleted, wantLaunched: map[string]bool{}},
		{name: "terminal failed build is left alone", build: bld(statusFailed), chunks: cks(ck("c1", statusFailed, "pr", 0)), wantStatus: statusFailed, wantLaunched: map[string]bool{}},
		{name: "terminal cancelled build is left alone", build: bld(statusCancelled), chunks: cks(ck("c1", statusCancelled, "pr", 0)), wantStatus: statusCancelled, wantLaunched: map[string]bool{}},
		{name: "build not found", missing: true, wantStatus: statusNotFound},
		{name: "store read failure", closed: true, wantErr: "closed pool"},
		{name: "store not configured", nilStore: true, wantErr: "store not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := reconcileStore(t, tc.build, tc.chunks, tc.missing, tc.closed)
			eri := &eritest.Client{}
			bo := NewBuildObject("", nil)
			if !tc.nilStore {
				bo = bo.WithAuthority(st, executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil))
			}
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", tc.mutation, nil, tc.prior, false)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, out.BuildStatus)
			assert.Equal(t, tc.wantChunks, out.LaunchedChunks)
			assert.Equal(t, tc.wantLaunched, out.LaunchedMap)
			assert.Equal(t, tc.wantWidget, widgetStatus(eri))
			if tc.wantStored != nil {
				assert.Equal(t, tc.wantStored, chunkStatuses(t, st, "b1"))
			}
		})
	}
}

// reconcileStore is a real store holding build and chunks, each chunk
// persisted at its slice position; missing leaves the database empty and
// closed returns a store whose pool is already closed.
func reconcileStore(t *testing.T, build model.VanadisBuild, chunks []model.VanadisChunk, missing, closed bool) *store.Store {
	t.Helper()
	if closed {
		return closedStore(t)
	}
	if missing {
		st, _ := openStore(t)
		return st
	}
	indexed := make([]model.VanadisChunk, len(chunks))
	for i, c := range chunks {
		c.ChunkIndex = i
		indexed[i] = c
	}
	st, _ := seededStore(t, []model.VanadisBuild{build}, indexed)
	return st
}

func TestNotifyChunkFailure_NilObjectIsNoop(t *testing.T) {
	t.Parallel()
	var bo *BuildObject
	assert.NoError(t, bo.notifyChunkFailure(mockRunContext{}, model.VanadisBuild{ID: "b1"}, "c1", "boom"))
}

// leaseChunks is one action chunk at the given status and retry generation.
func leaseChunks(status string, nudges int) []model.VanadisChunk {
	return []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: status, Mode: "action", StallNudges: nudges}}
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

func openAuthorityHandlerHarness(t *testing.T) (*BuildObject, *fakeObjectRuntime) {
	t.Helper()
	b1 := model.VanadisBuild{ID: "b-cov", Status: "running", ThreadID: "th-1"}
	c1 := model.VanadisChunk{ID: "c1", BuildID: "b-cov", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	st, _ := seededStore(t, []model.VanadisBuild{b1}, []model.VanadisChunk{c1})
	bo := authorityObject(t, st)
	rt := newFakeObjectRuntime("b-cov")
	rt.SetState(model.BuildObjectState{Build: b1, Chunks: []model.VanadisChunk{c1}})
	return bo, rt
}

// authorityObject is a BuildObject over st with a stub eri and bus.
func authorityObject(t *testing.T, st store.VanadisStore) *BuildObject {
	t.Helper()
	exec := executor.NewExecutor(t.TempDir(), &mockEriClient{}, &mockBusExecutor{}, "token", "example-owner", nil)
	return NewBuildObject("", nil).WithAuthority(st, exec)
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
			bTerm := model.VanadisBuild{ID: tc.buildID, Status: "running", ThreadID: "th-1"}
			cTerm := model.VanadisChunk{ID: "c1", BuildID: tc.buildID, Status: tc.chunkStatus}
			st, _ := seededStore(t, []model.VanadisBuild{bTerm}, []model.VanadisChunk{cTerm})
			termStatus, err := authorityObject(t, st).evaluateBuildTerminalStatus(context.Background(), &bTerm, []model.VanadisChunk{cTerm}, map[string]bool{}, false)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStatus, termStatus)
			stored, _ := loadBuild(t, st, tc.buildID)
			assert.Equal(t, tc.wantStatus, stored.Status)
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
	stMerge, _ := seededStore(t, []model.VanadisBuild{bMerge}, []model.VanadisChunk{cMerge})
	busErr := &pubsubtest.FaultBus{PublishErr: errors.New("pub fail")}
	boMerge := NewBuildObject("", nil).WithAuthority(stMerge, executor.NewExecutor(t.TempDir(), &mockEriClient{}, busErr, "token", "example-owner", nil))
	assert.NoError(t, boMerge.applyMutation(context.Background(), "b-merge", &BuildMutationRequest{Action: "chunk_merged", ChunkID: "c1"}))

	bBegin := model.VanadisBuild{ID: "b-begin", Status: "running", ThreadID: "th-1"}
	cBegin := model.VanadisChunk{ID: "c1", BuildID: "b-begin", Status: "running", Mode: "action"}
	stBegin, poolBegin := seededStore(t, []model.VanadisBuild{bBegin}, []model.VanadisChunk{cBegin})
	mustExec(t, poolBegin, failStatement("vanadis_chunks", "UPDATE"))
	boBegin := NewBuildObject("", nil).WithAuthority(stBegin, nil)
	assert.ErrorContains(t, boBegin.applyMutation(context.Background(), "b-begin", &BuildMutationRequest{Action: "action_completed", ChunkID: "c1"}), "sabotaged UPDATE on vanadis_chunks")
	assert.Equal(t, map[string]string{"c1": "running"}, chunkStatuses(t, stBegin, "b-begin"))

	bTerm := model.VanadisBuild{ID: "b-term", Status: "completed", ThreadID: "th-1"}
	stTerm, _ := seededStore(t, []model.VanadisBuild{bTerm}, nil)
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
	stDeadline, _ := seededStore(t, []model.VanadisBuild{bDeadline}, []model.VanadisChunk{cDeadline})
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
			st, _ := seededStore(t, []model.VanadisBuild{b}, nil)
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

// wb is build b1 with the widget-relevant build fields.
func wb(msgID, status, title string, tier int, thread string) model.VanadisBuild {
	return model.VanadisBuild{ID: "b1", StatusMessageID: msgID, Status: status, Title: title, Tier: tier, ThreadID: thread}
}

// wc is the widget base chunk c1 with index, repo and title overridden.
func wc(index int, repo, title string) model.VanadisChunk {
	c := ck("c1", "running", "pr", 10)
	c.ChunkIndex, c.Repo, c.Title = index, repo, title
	return c
}

// withRollout is c carrying rollout.
func withRollout(c model.VanadisChunk, rollout *pubsub.PRRolloutState) model.VanadisChunk {
	c.RolloutState = rollout
	return c
}

func TestWidgetStateEqual(t *testing.T) {
	t.Parallel()

	rollout := func(job string) *pubsub.PRRolloutState {
		return &pubsub.PRRolloutState{ExpectedJobs: []string{job}, StartedJobs: []string{job}, DeployedJobs: []string{job}, JobStates: map[string]string{job: "deployed"}, KustomizationStatuses: map[string]string{"kust": "healthy"}}
	}
	rolloutA := rollout("job-a")
	b, c := wb("msg-123", "running", "T", 2, "th-1"), wc(0, "aerial", "")
	base := objState(b, c)

	tests := []struct {
		name  string
		prior *model.BuildObjectState
		cur   *model.BuildObjectState
		want  bool
	}{
		{"nil prior", nil, base, false},
		{"empty status message id", base, objState(wb("", "running", "T", 2, "th-1"), c), false},
		{"status message id mismatch", base, objState(wb("other-msg", "running", "T", 2, "th-1"), c), false},
		{"status mismatch", base, objState(wb("msg-123", "failed", "T", 2, "th-1"), c), false},
		{"title mismatch", base, objState(wb("msg-123", "running", "Other", 2, "th-1"), c), false},
		{"tier mismatch", base, objState(wb("msg-123", "running", "T", 1, "th-1"), c), false},
		{"thread id mismatch", base, objState(wb("msg-123", "running", "T", 2, "th-2"), c), false},
		{"identical state", base, objState(b, c), true},
		{"chunk count mismatch", base, objState(b, c, c), false},
		{"chunk id mismatch", objState(b, ck("c-old", "running", "pr", 10)), base, false},
		{"chunk index mismatch", objState(b, wc(1, "aerial", "")), base, false},
		{"chunk mode mismatch", objState(b, ck("c1", "running", "action", 10)), base, false},
		{"chunk repo mismatch", objState(b, wc(0, "other", "")), base, false},
		{"chunk title mismatch", objState(b, wc(0, "aerial", "Other")), base, false},
		{"chunk status mismatch", objState(b, ck("c1", "pending", "pr", 10)), base, false},
		{"chunk pr number mismatch", objState(b, ck("c1", "running", "pr", 20)), base, false},
		{"chunk active step mismatch", objState(b, ciChunk("pr_opened", "passed", 10, nil)), objState(b, ciChunk("pr_opened", "pending", 10, nil)), false},
		{"chunk rollout mismatch", objState(b, withRollout(c, rolloutA)), objState(b, withRollout(c, rollout("job-b"))), false},
		{"chunk rollout dropped", objState(b, withRollout(c, rolloutA)), base, false},
		{"same rollout pointer", objState(b, withRollout(c, rolloutA)), objState(b, withRollout(c, rolloutA)), true},
		{"equal rollout values", objState(b, withRollout(c, rolloutA)), objState(b, withRollout(c, rollout("job-a"))), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, widgetStateEqual(tc.prior, tc.cur.Build, tc.cur.Chunks))
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
