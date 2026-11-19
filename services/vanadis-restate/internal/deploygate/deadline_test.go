package deploygate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Frozen wire names, keys, and timing. Values stay byte-identical with the
// previous internal/build copies.
func TestDeploygateFrozenWireValues(t *testing.T) {
	t.Parallel()
	require.Equal(t, "deployment_deadlines", DeploymentDeadlineKey)
	require.Equal(t, "pending_deployment_starts", PendingDeploymentKey)
	require.Equal(t, "deploy_started", DeployStartedEvent)
	require.Equal(t, 15*time.Minute, MaxDeployTime)
}

type fakeRuntime struct {
	key                string
	now                time.Time
	nowErr             error
	attempts           map[string]Attempt
	getErr             error
	pending            map[string]PendingStart
	pendingErr         error
	scheduled          []DeploymentDeadlineRequest
	delays             []time.Duration
	runStepErr         error
	stepsRun           []string
	reconcileScheduled bool
}

func newFakeRuntime(key string) *fakeRuntime {
	return &fakeRuntime{key: key}
}

func (r *fakeRuntime) Key() string { return r.key }

func (r *fakeRuntime) GetAttempts() (map[string]Attempt, error) { return r.attempts, r.getErr }

func (r *fakeRuntime) SetAttempts(a map[string]Attempt) { r.attempts = a }

func (r *fakeRuntime) Now() (time.Time, error) { return r.now, r.nowErr }

func (r *fakeRuntime) GetPendingStarts() (map[string]PendingStart, error) {
	return r.pending, r.pendingErr
}

func (r *fakeRuntime) SetPendingStarts(p map[string]PendingStart) { r.pending = p }

func (r *fakeRuntime) ScheduleDeadline(req DeploymentDeadlineRequest, d time.Duration) {
	r.scheduled = append(r.scheduled, req)
	if d < 0 {
		d = 0
	}
	r.delays = append(r.delays, d)
}

func (r *fakeRuntime) ScheduleReconcile() { r.reconcileScheduled = true }

func (r *fakeRuntime) RunStep(name string, fn func(ctx context.Context) (StepOutput, error)) (StepOutput, error) {
	r.stepsRun = append(r.stepsRun, name)
	if r.runStepErr != nil {
		return StepOutput{}, r.runStepErr
	}
	return fn(context.Background())
}

type fakeStore struct {
	buildStatus     string
	nilBuild        bool
	getBuildErr     error
	statusErr       error
	bannerErr       error
	rolloutErr      error
	chunks          []model.VanadisChunk
	lifecycleStatus string
}

func (s *fakeStore) chunk(id string) *model.VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == id {
			return &s.chunks[i]
		}
	}
	s.chunks = append(s.chunks, model.VanadisChunk{ID: id})
	return &s.chunks[len(s.chunks)-1]
}

func (s *fakeStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	if s.nilBuild {
		return nil, nil, nil
	}
	status := s.buildStatus
	if status == "" {
		status = statusRunning
	}
	return &model.VanadisBuild{ID: buildID, Status: status, ThreadID: "th-1"}, append([]model.VanadisChunk(nil), s.chunks...), nil
}

func (s *fakeStore) UpdatePRLifecycleStatus(_ context.Context, id, status string) error {
	s.lifecycleStatus = status
	if s.statusErr != nil {
		return s.statusErr
	}
	s.chunk(id).Status = status
	return nil
}

func (s *fakeStore) UpdateChunkBannerAndStatus(_ context.Context, id, status, banner string) error {
	if s.bannerErr != nil {
		return s.bannerErr
	}
	c := s.chunk(id)
	c.Status = status
	c.Banner = banner
	return nil
}

func (s *fakeStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	if s.rolloutErr != nil {
		return s.rolloutErr
	}
	c := s.chunk(id)
	if status != "" {
		c.Status = status
	}
	c.RolloutState = rollout
	return nil
}

type fakeEriClient struct {
	dispatchFn   func(context.Context, executor.BrainDispatchRequest) error
	lastDispatch *executor.BrainDispatchRequest
	dispatchIDs  []string
}

func (*fakeEriClient) Dispatch(context.Context, executor.BrainDispatchRequest) error {
	return nil
}

func (f *fakeEriClient) DispatchAccepted(ctx context.Context, req executor.BrainDispatchRequest) error {
	cp := req
	f.lastDispatch = &cp
	f.dispatchIDs = append(f.dispatchIDs, req.ID)
	if f.dispatchFn != nil {
		return f.dispatchFn(ctx, req)
	}
	return nil
}

func (*fakeEriClient) CancelDispatch(context.Context, string) error { return nil }

func (*fakeEriClient) Prompt(context.Context, string, string) error { return nil }

func (*fakeEriClient) Widget(context.Context, executor.StatusWidgetRequest) (string, error) {
	return "", nil
}

func (*fakeEriClient) Message(context.Context, string, string) error { return nil }

func TestDeploymentDeadlineAttempts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old := Attempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(15 * time.Minute)}
	tests := []struct {
		name           string
		prior          Attempt
		chunk          model.VanadisChunk
		retry          bool
		wantGeneration int
		wantDeadline   time.Time
	}{
		{"first", Attempt{}, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, now.Add(15 * time.Minute)},
		{"duplicate", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, old.Deadline},
		{"explicit same revision retry", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, true, 2, now.Add(15 * time.Minute)},
		{"new revision", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "new"}, false, 2, now.Add(15 * time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := nextAttempt(tc.prior, tc.chunk, now, tc.retry)
			require.Equal(t, tc.wantGeneration, got.Generation)
			require.Equal(t, tc.wantDeadline, got.Deadline)
		})
	}
}

func TestDeploymentDeadlineLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		status        string
		revision      string
		generation    int
		delivered     bool
		absent        bool
		early         bool
		fail          bool
		wantDelivered bool
		wantRetry     bool
		wantWake      bool
	}{
		{name: "expiry", status: statusDeploying, revision: "sha", generation: 1, wantDelivered: true, wantWake: true},
		{name: "completion races expiry", status: statusSynced, revision: "sha", generation: 1, wantDelivered: true},
		{name: "superseded revision", status: statusDeploying, revision: "new", generation: 1, wantDelivered: true},
		{name: "stale retry", status: statusDeploying, revision: "sha", generation: 2},
		{name: "already delivered", status: statusDeploying, revision: "sha", generation: 1, delivered: true, wantDelivered: true},
		{name: "terminal clear", status: statusDeploying, revision: "sha", generation: 1, absent: true},
		{name: "early callback", status: statusDeploying, revision: "sha", generation: 1, early: true, wantRetry: true},
		{name: "delivery retry", status: statusDeploying, revision: "sha", generation: 1, fail: true, wantRetry: true, wantWake: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &fakeRuntime{key: "b", now: now, attempts: map[string]Attempt{}}
			deadline := now
			if tc.early {
				deadline = now.Add(time.Minute)
			}
			if !tc.absent {
				rt.attempts["c"] = Attempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: tc.generation, Deadline: deadline, Delivered: tc.delivered}
			}
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: tc.revision, Status: tc.status, Repo: "owner/repo", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}}
			eri := &fakeEriClient{}
			failDelivery := tc.fail
			eri.dispatchFn = func(_ context.Context, _ executor.BrainDispatchRequest) error {
				if failDelivery {
					return errors.New("delivery unavailable")
				}
				return nil
			}
			g := NewGate(st, &executor.Executor{EriClient: eri})
			require.NoError(t, g.ExpireDeadline(rt, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
			require.Equal(t, tc.wantDelivered, rt.attempts["c"].Delivered)
			require.Equal(t, tc.wantRetry, len(rt.scheduled) > 0)
			require.Equal(t, tc.wantWake, eri.lastDispatch != nil)
			if eri.lastDispatch != nil {
				require.Contains(t, eri.lastDispatch.Prompt, "Readiness remains unconfirmed")
				require.Contains(t, eri.lastDispatch.Prompt, "web")
				require.Equal(t, "vanadis-deploy-deadline-c-1-sha-1", eri.lastDispatch.ID)
			}
			if tc.fail {
				require.Equal(t, statusDeployFailed, st.chunks[0].Status)
				failDelivery = false
				require.NoError(t, g.ExpireDeadline(rt, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
				require.True(t, rt.attempts["c"].Delivered)
				require.Equal(t, []string{"vanadis-deploy-deadline-c-1-sha-1", "vanadis-deploy-deadline-c-1-sha-1"}, eri.dispatchIDs)
			}
		})
	}
}

func TestDeploymentDeadlineAdoption(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		event        *pubsub.PRLifecycleEvent
		wantDeadline time.Time
	}{
		{"adoption", nil, now.Add(15 * time.Minute)},
		{"event start", &pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now.Add(-time.Minute)}, now.Add(14 * time.Minute)},
		{"future timestamp", &pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now.Add(time.Hour)}, now.Add(15 * time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.now = now
			state := &model.BuildObjectState{Chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying, StartedAt: now.Add(-time.Hour)}}}
			g := NewGate(nil, nil)
			require.NoError(t, g.SyncDeadlines(rt, state, tc.event, nil))
			require.Equal(t, tc.wantDeadline, rt.attempts["c"].Deadline)
			rt.now = now.Add(time.Hour)
			require.NoError(t, NewGate(nil, nil).SyncDeadlines(rt, state, nil, nil))
			require.Equal(t, tc.wantDeadline, rt.attempts["c"].Deadline)
			require.Len(t, rt.scheduled, 1)
		})
	}
}

func TestDeploymentDeadlineSynchronization(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		retry          *RetryRequest
		wantGeneration int
		wantError      bool
	}{
		{"duplicate and multiple jobs", nil, 1, false},
		{"same revision explicit retry", &RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}, 2, false},
		{"stale retry generation", &RetryRequest{ChunkID: "c", Revision: "sha", Generation: 0}, 1, true},
		{"stale retry revision", &RetryRequest{ChunkID: "c", Revision: "old", Generation: 1}, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.now = now
			state := &model.BuildObjectState{Chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying, Repo: "owner/repo", RolloutState: &pubsub.PRRolloutState{StartedJobs: []string{"one"}}}}}
			state.Chunks = append(state.Chunks, model.VanadisChunk{ID: "completed", PRNumber: 2, MergeSHA: "done", Status: statusSynced})
			g := NewGate(nil, nil)
			require.NoError(t, g.SyncDeadlines(rt, state, nil, nil))
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
			require.NotContains(t, rt.attempts, "completed")
			rt.now = now.Add(5 * time.Minute)
			state.Chunks[0].RolloutState.StartedJobs = append(state.Chunks[0].RolloutState.StartedJobs, "two")
			require.NoError(t, g.SyncDeadlines(rt, state, nil, nil))
			require.Len(t, rt.scheduled, 1)
			err := g.SyncDeadlines(rt, state, nil, tc.retry)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantGeneration, rt.attempts["c"].Generation)
			if tc.wantGeneration == 2 {
				require.Equal(t, now.Add(20*time.Minute), rt.attempts["c"].Deadline)
			} else {
				require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
			}
		})
	}
}

func TestDeploymentDeadlineRetryMutation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		revision       string
		generation     int
		wantGeneration int
		wantError      bool
		resetErr       error
		stateErr       error
	}{
		{"same revision retry", "sha", 1, 2, false, nil, nil},
		{"stale generation", "sha", 0, 1, true, nil, nil},
		{"stale revision", "old", 1, 1, true, nil, nil},
		{"durable reset unavailable", "sha", 1, 1, true, errors.New("write unavailable"), nil},
		{"attempt state unavailable", "sha", 1, 1, true, nil, errors.New("state unavailable")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.now = now
			rt.getErr = tc.stateErr
			rt.attempts = map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute), Delivered: true}}
			st := &fakeStore{bannerErr: tc.resetErr, chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}
			g := NewGate(st, nil)
			_, err := g.RetryDeployment(rt, RetryRequest{ChunkID: "c", Revision: tc.revision, Generation: tc.generation})
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantGeneration, rt.attempts["c"].Generation)
			require.Equal(t, tc.wantError, rt.attempts["c"].Delivered)
			if !tc.wantError {
				require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
				require.Equal(t, statusDeploying, st.chunks[0].Status)
				require.Empty(t, st.chunks[0].Banner)
				require.True(t, rt.reconcileScheduled)
			} else {
				require.Equal(t, statusDeployFailed, st.chunks[0].Status)
				require.Equal(t, "expired", st.chunks[0].Banner)
			}
		})
	}
}

func TestDeploymentStartValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, revision string
		rollout                *pubsub.PRRolloutState
		storeErr, statusErr    error
		buildStatus            string
		nilBuild               bool
		wantValid, wantError   bool
		wantStatus             string
	}{
		{name: "nil rollout start", status: statusMerged, revision: "sha", wantValid: true, wantStatus: statusDeploying},
		{name: "rollout start preserves snapshot", status: statusDeploying, revision: "sha", rollout: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, wantValid: true},
		{name: "stale revision", status: statusDeploying, revision: "old"},
		{name: "completed", status: statusSynced, revision: "sha"},
		{name: "failed chunk preserves failure", status: statusDeployFailed, revision: "sha", wantValid: true},
		{name: "nil build", nilBuild: true, revision: "sha"},
		{name: "terminal build", buildStatus: statusCompleted, revision: "sha"},
		{name: "read unavailable", storeErr: errors.New("read unavailable"), wantError: true},
		{name: "status unavailable", status: statusMerged, revision: "sha", statusErr: errors.New("write unavailable"), wantError: true, wantStatus: statusDeploying},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &fakeStore{getBuildErr: tc.storeErr, statusErr: tc.statusErr, buildStatus: tc.buildStatus, nilBuild: tc.nilBuild, chunks: []model.VanadisChunk{{ID: "c", Status: tc.status, MergeSHA: "sha"}}}
			g := NewGate(st, nil)
			valid, _, err := g.RecordDeploymentStart(context.Background(), "b", "c", pubsub.PRLifecycleEvent{MergeSHA: tc.revision, RolloutState: tc.rollout})
			require.Equal(t, tc.wantValid, valid)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantStatus, st.lifecycleStatus)
		})
	}
}

func TestDeploymentStartArming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		storeErr    error
		buildStatus string
		wantError   bool
		wantArmed   bool
	}{
		{"starts deadline", nil, "", false, true},
		{"snapshot read fails", errors.New("unavailable"), "", true, false},
		{"terminal snapshot skips arming", nil, statusCompleted, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := newFakeRuntime("b")
			rt.now = now
			st := &fakeStore{getBuildErr: tc.storeErr, buildStatus: tc.buildStatus, chunks: []model.VanadisChunk{{ID: "c", Status: statusDeploying, PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha"}}}
			err := NewGate(st, nil).ArmDeploymentStart(rt, "b", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now.Add(-time.Minute)})
			require.Equal(t, tc.wantError, err != nil)
			if tc.wantArmed {
				require.Equal(t, now.Add(14*time.Minute), rt.attempts["c"].Deadline)
			} else {
				require.Empty(t, rt.attempts)
			}
		})
	}
}

func TestDeploymentDeadlineRuntimeFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		getErr, nowErr, stepErr error
		nilRuntime              bool
	}{
		{"state read", errors.New("state read"), nil, nil, false},
		{"clock", nil, errors.New("clock"), nil, false},
		{"journal", nil, nil, errors.New("journal"), false},
		{"nil runtime", nil, nil, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt.getErr = tc.getErr
			rt.nowErr = tc.nowErr
			rt.runStepErr = tc.stepErr
			rt.attempts = map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1}}
			var run Runtime
			run = rt
			if tc.nilRuntime {
				run = nil
			}
			require.Error(t, NewGate(nil, nil).ExpireDeadline(run, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
			require.False(t, rt.attempts["c"].Delivered)
		})
	}
}

func TestDeploymentDeadlineSynchronizationFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		getErr, nowErr error
		nilState       bool
		nilRuntime     bool
	}{
		{"state read", errors.New("state read"), nil, false, false},
		{"clock", nil, errors.New("clock"), false, false},
		{"nil state", nil, nil, true, false},
		{"nil runtime", nil, nil, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.getErr = tc.getErr
			rt.nowErr = tc.nowErr
			state := &model.BuildObjectState{Chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
			if tc.nilState {
				state = nil
			}
			var run Runtime
			run = rt
			if tc.nilRuntime {
				run = nil
			}
			st := &fakeStore{}
			if state != nil {
				st.chunks = state.Chunks
			}
			if tc.nilState || tc.nilRuntime {
				require.NoError(t, NewGate(st, nil).SyncDeadlines(run, state, nil, nil))
				require.Empty(t, rt.scheduled)
				return
			}
			require.Error(t, NewGate(st, nil).SyncDeadlines(run, state, nil, nil))
			require.Error(t, NewGate(st, nil).ArmDeploymentStart(run, "b", pubsub.PRLifecycleEvent{}))
			require.Empty(t, rt.scheduled)
		})
	}
}

func TestDeploymentRetryLookupFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		store *fakeStore
	}{
		{"unsupported authority", nil},
		{"snapshot unavailable", &fakeStore{getBuildErr: errors.New("snapshot unavailable")}},
		{"chunk unavailable", &fakeStore{chunks: []model.VanadisChunk{{ID: "other", PRNumber: 1, Status: statusDeploying}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			var st Store
			if tc.store != nil {
				st = tc.store
			}
			_, err := NewGate(st, nil).RetryDeployment(rt, RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1})
			require.Error(t, err)
			require.Empty(t, rt.scheduled)
		})
	}
}

func TestDeploymentDeadlineDeliveryFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		store       *fakeStore
		exec        bool
		buildStatus string
		chunkID     string
		wantError   bool
	}{
		{"store unavailable", nil, true, "", "c", true},
		{"read failed", &fakeStore{getBuildErr: errors.New("read failed")}, true, "", "c", true},
		{"banner failed", &fakeStore{bannerErr: errors.New("banner failed")}, true, "", "c", true},
		{"executor unavailable", &fakeStore{}, false, "", "c", true},
		{"terminal build skips delivery", &fakeStore{}, true, statusCompleted, "c", false},
		{"unknown chunk skips delivery", &fakeStore{}, true, "", "other", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st Store
			if tc.store != nil {
				tc.store.buildStatus = tc.buildStatus
				tc.store.chunks = []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}
				st = tc.store
			}
			var exec *executor.Executor
			if tc.exec {
				exec = &executor.Executor{EriClient: &fakeEriClient{}}
			}
			out, err := NewGate(st, exec).DeliverDeadline(context.Background(), "b", Attempt{ChunkID: tc.chunkID, PRNumber: 1, Revision: "sha", Generation: 1})
			require.NoError(t, err)
			require.Equal(t, tc.wantError, out.Error != "")
		})
	}
}

func TestDeploymentRetryResumesAfterClockFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		clockErr error
	}{{"temporary clock failure", errors.New("clock unavailable")}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := newFakeRuntime("b")
			rt.now = now
			rt.nowErr = tc.clockErr
			rt.attempts = map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Delivered: true}}
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}
			g := NewGate(st, nil)
			req := RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}
			_, err := g.RetryDeployment(rt, req)
			require.Error(t, err)
			require.Equal(t, statusDeploying, st.chunks[0].Status)
			require.Empty(t, st.chunks[0].Banner)
			require.Equal(t, 1, rt.attempts["c"].Generation)
			rt.nowErr = nil
			_, err = g.RetryDeployment(rt, req)
			require.NoError(t, err)
			require.Equal(t, 2, rt.attempts["c"].Generation)
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
			require.True(t, rt.reconcileScheduled)
		})
	}
}

func TestDeploymentStartPersistenceTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		chunkStatus    string
		chunkSHA       string
		eventSHA       string
		nilRollout     bool
		wantValid      bool
		wantStatus     string
		wantLifecycle  string
		wantScheduled  bool
		wantDelivered  bool
		wantFinal      string
		wantDispatchID string
	}{
		{name: "merged start persists deploying then deadline delivers failure", chunkStatus: statusMerged, chunkSHA: "sha", eventSHA: "sha", nilRollout: true, wantValid: true, wantStatus: statusDeploying, wantLifecycle: statusDeploying, wantScheduled: true, wantDelivered: true, wantFinal: statusDeployFailed, wantDispatchID: "vanadis-deploy-deadline-c-1-sha-1"},
		{name: "deploying start with evidence preserves snapshot then delivers", chunkStatus: statusDeploying, chunkSHA: "sha", eventSHA: "sha", wantValid: true, wantStatus: statusDeploying, wantScheduled: true, wantDelivered: true, wantFinal: statusDeployFailed, wantDispatchID: "vanadis-deploy-deadline-c-1-sha-1"},
		{name: "failed chunk preserves failure observation without arming", chunkStatus: statusDeployFailed, chunkSHA: "sha", eventSHA: "sha", nilRollout: true, wantValid: true, wantFinal: statusDeployFailed},
		{name: "stale revision ignored but durable deadline still delivers", chunkStatus: statusDeploying, chunkSHA: "sha", eventSHA: "old", nilRollout: true, wantScheduled: true, wantDelivered: true, wantFinal: statusDeployFailed, wantDispatchID: "vanadis-deploy-deadline-c-1-sha-1"},
		{name: "non-deploying status ignored", chunkStatus: statusRunning, chunkSHA: "sha", eventSHA: "sha", nilRollout: true, wantFinal: statusRunning},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := newFakeRuntime("b")
			rt.now = now
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, Repo: "owner/repo", MergeSHA: tc.chunkSHA, Status: tc.chunkStatus, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}}
			eri := &fakeEriClient{}
			g := NewGate(st, &executor.Executor{EriClient: eri})
			var rollout *pubsub.PRRolloutState
			if !tc.nilRollout {
				rollout = &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}
			}
			ev := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: tc.eventSHA, OccurredAt: now.Add(-time.Minute), RolloutState: rollout}
			valid, status, err := g.RecordDeploymentStart(context.Background(), "b", "c", ev)
			require.NoError(t, err)
			require.Equal(t, tc.wantValid, valid)
			require.Equal(t, tc.wantStatus, status)
			require.Equal(t, tc.wantLifecycle, st.lifecycleStatus)
			require.NoError(t, g.ArmDeploymentStart(rt, "b", ev))
			require.Equal(t, tc.wantScheduled, len(rt.scheduled) > 0)
			rt.now = now.Add(time.Hour)
			require.NoError(t, g.ExpireDeadline(rt, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
			require.Equal(t, tc.wantDelivered, rt.attempts["c"].Delivered)
			require.Equal(t, tc.wantFinal, st.chunks[0].Status)
			if tc.wantDispatchID == "" {
				require.Nil(t, eri.lastDispatch)
				return
			}
			require.NotNil(t, eri.lastDispatch)
			require.Equal(t, tc.wantDispatchID, eri.lastDispatch.ID)
		})
	}
}
