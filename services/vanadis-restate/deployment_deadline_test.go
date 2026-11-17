package main

import (
	"context"
	"errors"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestDeploymentDeadlineAttempts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old := deploymentAttempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(15 * time.Minute)}
	tests := []struct {
		name           string
		prior          deploymentAttempt
		chunk          VanadisChunk
		retry          bool
		wantGeneration int
		wantDeadline   time.Time
	}{
		{"first", deploymentAttempt{}, VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, now.Add(15 * time.Minute)},
		{"duplicate", old, VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, old.Deadline},
		{"explicit same revision retry", old, VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, true, 2, now.Add(15 * time.Minute)},
		{"new revision", old, VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "new"}, false, 2, now.Add(15 * time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := nextDeploymentAttempt(tc.prior, tc.chunk, now, tc.retry)
			require.Equal(t, tc.wantGeneration, got.Generation)
			require.Equal(t, tc.wantDeadline, got.Deadline)
		})
	}
}

type deadlineTestRuntime struct {
	*fakeObjectRuntime

	getErr     error
	pendingErr error
	nowErr     error
	attempts   map[string]deploymentAttempt
	pending    map[string]pendingDeploymentStart
	now        time.Time
	scheduled  []DeploymentDeadlineRequest
	delays     []time.Duration
}

func (r *deadlineTestRuntime) GetPendingDeploymentStarts() (map[string]pendingDeploymentStart, error) {
	return r.pending, r.pendingErr
}

func (r *deadlineTestRuntime) SetPendingDeploymentStarts(p map[string]pendingDeploymentStart) {
	r.pending = p
}

func (r *deadlineTestRuntime) GetDeploymentAttempts() (map[string]deploymentAttempt, error) {
	return r.attempts, r.getErr
}

func (r *deadlineTestRuntime) SetDeploymentAttempts(a map[string]deploymentAttempt) { r.attempts = a }

func (r *deadlineTestRuntime) DeploymentNow() (time.Time, error) { return r.now, r.nowErr }

func (r *deadlineTestRuntime) ScheduleDeploymentDeadline(q DeploymentDeadlineRequest, d time.Duration) {
	r.scheduled = append(r.scheduled, q)
	if d < 0 {
		d = 0
	}
	r.delays = append(r.delays, max(d, 0))
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
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now, attempts: map[string]deploymentAttempt{}}
			deadline := now
			if tc.early {
				deadline = now.Add(time.Minute)
			}
			if !tc.absent {
				rt.attempts["c"] = deploymentAttempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: tc.generation, Deadline: deadline, Delivered: tc.delivered}
			}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: tc.revision, Status: tc.status, Repo: "owner/repo", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}}}
			eri := &mockEriClient{}
			var dispatchIDs []string
			failDelivery := tc.fail
			eri.dispatchFn = func(_ context.Context, req BrainDispatchRequest) error {
				dispatchIDs = append(dispatchIDs, req.ID)
				if failDelivery {
					return errors.New("delivery unavailable")
				}
				return nil
			}
			b := &BuildObject{store: store, executor: &Executor{eriClient: eri}}
			require.NoError(t, b.expireDeploymentDeadline(rt, DeploymentDeadlineRequest{"c", "sha", 1}))
			require.Equal(t, tc.wantDelivered, rt.attempts["c"].Delivered)
			require.Equal(t, tc.wantRetry, len(rt.scheduled) > 0)
			require.Equal(t, tc.wantWake, eri.lastDispatch != nil)
			if eri.lastDispatch != nil {
				require.Contains(t, eri.lastDispatch.Prompt, "Readiness remains unconfirmed")
				require.Contains(t, eri.lastDispatch.Prompt, "web")
				require.Equal(t, "vanadis-deploy-deadline-c-1-sha-1", eri.lastDispatch.ID)
			}
			if tc.fail {
				require.Equal(t, statusDeployFailed, store.chunks[0].Status)
				failDelivery = false
				require.NoError(t, b.expireDeploymentDeadline(rt, DeploymentDeadlineRequest{"c", "sha", 1}))
				require.True(t, rt.attempts["c"].Delivered)
				require.Equal(t, []string{"vanadis-deploy-deadline-c-1-sha-1", "vanadis-deploy-deadline-c-1-sha-1"}, dispatchIDs)
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
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			state := &BuildObjectState{Chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying, StartedAt: now.Add(-time.Hour)}}}
			b := &BuildObject{}
			require.NoError(t, b.syncDeploymentDeadlines(rt, state, tc.event, nil))
			require.Equal(t, tc.wantDeadline, rt.attempts["c"].Deadline)
			rt.now = now.Add(time.Hour)
			require.NoError(t, (&BuildObject{}).syncDeploymentDeadlines(rt, state, nil, nil))
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
		retry          *BuildMutationRequest
		wantGeneration int
		wantError      bool
	}{
		{"duplicate and multiple jobs", nil, 1, false},
		{"same revision explicit retry", &BuildMutationRequest{ChunkID: "c", Revision: "sha", Generation: 1}, 2, false},
		{"stale retry generation", &BuildMutationRequest{ChunkID: "c", Revision: "sha", Generation: 0}, 1, true},
		{"stale retry revision", &BuildMutationRequest{ChunkID: "c", Revision: "old", Generation: 1}, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			state := &BuildObjectState{Chunks: []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying, Repo: "owner/repo", RolloutState: &pubsub.PRRolloutState{StartedJobs: []string{"one"}}}}}
			state.Chunks = append(state.Chunks, VanadisChunk{ID: "completed", PRNumber: 2, MergeSHA: "done", Status: statusSynced})
			b := &BuildObject{}
			require.NoError(t, b.syncDeploymentDeadlines(rt, state, nil, nil))
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
			require.NotContains(t, rt.attempts, "completed")
			rt.now = now.Add(5 * time.Minute)
			state.Chunks[0].RolloutState.StartedJobs = append(state.Chunks[0].RolloutState.StartedJobs, "two")
			require.NoError(t, b.syncDeploymentDeadlines(rt, state, nil, nil))
			require.Len(t, rt.scheduled, 1)
			err := b.syncDeploymentDeadlines(rt, state, nil, tc.retry)
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
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now, getErr: tc.stateErr, attempts: map[string]deploymentAttempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(-time.Minute), Delivered: true}}}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{bannerErr: tc.resetErr, chunks: []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}}
			b := &BuildObject{store: store, runtimeFactory: func(restate.ObjectContext) objectRuntime { return rt }}
			_, err := b.Mutate(nil, BuildMutationRequest{Action: "retry_deployment", ChunkID: "c", Revision: tc.revision, Generation: tc.generation})
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantGeneration, rt.attempts["c"].Generation)
			require.Equal(t, tc.wantError, rt.attempts["c"].Delivered)
			if !tc.wantError {
				require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
				require.Equal(t, statusDeploying, store.chunks[0].Status)
				require.Empty(t, store.chunks[0].Banner)
				require.True(t, rt.reconcileScheduled)
			} else {
				require.Equal(t, statusDeployFailed, store.chunks[0].Status)
				require.Equal(t, "expired", store.chunks[0].Banner)
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
		wantValid, wantError   bool
		wantStatus             string
	}{
		{name: "nil rollout start", status: statusMerged, revision: "sha", wantValid: true, wantStatus: statusDeploying},
		{name: "rollout start preserves snapshot", status: statusDeploying, revision: "sha", rollout: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, wantValid: true},
		{name: "stale revision", status: statusDeploying, revision: "old"},
		{name: "completed", status: statusSynced, revision: "sha"},
		{name: "read unavailable", storeErr: errors.New("read unavailable"), wantError: true},
		{name: "status unavailable", status: statusMerged, revision: "sha", statusErr: errors.New("write unavailable"), wantError: true, wantStatus: statusDeploying},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &checkStatusSpyStore{getBuildErr: tc.storeErr, statusErr: tc.statusErr, chunks: []VanadisChunk{{ID: "c", Status: tc.status, MergeSHA: "sha"}}}
			b := &BuildObject{store: store}
			valid, _, err := b.recordDeploymentStart(mockRunContext{Context: context.Background()}, "b", "c", pubsub.PRLifecycleEvent{MergeSHA: tc.revision, RolloutState: tc.rollout})
			require.Equal(t, tc.wantValid, valid)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantStatus, store.lifecycleStatus)
		})
	}
}

func TestDeploymentStartArming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		storeErr  error
		wantError bool
	}{
		{"starts deadline", nil, false},
		{"snapshot read fails", errors.New("unavailable"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			store := &checkStatusSpyStore{getBuildErr: tc.storeErr, chunks: []VanadisChunk{{ID: "c", Status: statusDeploying, PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha"}}}
			err := (&BuildObject{store: store}).armDeploymentStart(rt, "b", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now.Add(-time.Minute)})
			require.Equal(t, tc.wantError, err != nil)
			if !tc.wantError {
				require.Equal(t, now.Add(14*time.Minute), rt.attempts["c"].Deadline)
			}
		})
	}
}

func TestDeploymentDeadlineRuntimeFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		getErr, nowErr, stepErr error
	}{
		{"state read", errors.New("state read"), nil, nil},
		{"clock", nil, errors.New("clock"), nil},
		{"journal", nil, nil, errors.New("journal")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), getErr: tc.getErr, nowErr: tc.nowErr, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), attempts: map[string]deploymentAttempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1}}}
			rt.runStepErr = tc.stepErr
			require.Error(t, (&BuildObject{}).expireDeploymentDeadline(rt, DeploymentDeadlineRequest{"c", "sha", 1}))
			require.False(t, rt.attempts["c"].Delivered)
		})
	}
}

func TestDeploymentDeadlineSynchronizationFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		getErr, nowErr error
	}{
		{"state read", errors.New("state read"), nil},
		{"clock", nil, errors.New("clock")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), getErr: tc.getErr, nowErr: tc.nowErr}
			state := &BuildObjectState{Chunks: []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}}
			b := &BuildObject{store: &checkStatusSpyStore{chunks: state.Chunks}}
			require.Error(t, b.syncDeploymentDeadlines(rt, state, nil, nil))
			require.Error(t, b.armDeploymentStart(rt, "b", pubsub.PRLifecycleEvent{}))
			require.Empty(t, rt.scheduled)
		})
	}
}

func TestDeploymentRetryLookupFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		store *checkStatusSpyStore
	}{
		{"unsupported authority", nil},
		{"snapshot unavailable", &checkStatusSpyStore{getBuildErr: errors.New("snapshot unavailable")}},
		{"chunk unavailable", &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "other", PRNumber: 1, Status: statusDeploying}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b")}
			b := &BuildObject{}
			if tc.store != nil {
				b.store = tc.store
			}
			_, err := b.retryDeployment(rt, BuildMutationRequest{Action: "retry_deployment", ChunkID: "c", Revision: "sha", Generation: 1})
			require.Error(t, err)
			require.Empty(t, rt.scheduled)
		})
	}
}

type deadlineLifecycleStore struct{ *checkStatusSpyStore }

func (s *deadlineLifecycleStore) GetBuild(ctx context.Context, id string) (*VanadisBuild, []VanadisChunk, error) {
	b, c, err := s.checkStatusSpyStore.GetBuild(ctx, id)
	return b, append([]VanadisChunk(nil), c...), err
}

func (s *deadlineLifecycleStore) UpdateChunkBannerAndStatus(_ context.Context, id, status, banner string) error {
	if s.bannerErr != nil {
		return s.bannerErr
	}
	c := s.chunk(id)
	c.Status = status
	c.Banner = banner
	return nil
}

func (s *deadlineLifecycleStore) UpdatePRLifecycleStatus(_ context.Context, id, status string) error {
	c := s.chunk(id)
	c.Status = status
	return nil
}

func (s *deadlineLifecycleStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	c := s.chunk(id)
	if status != "" {
		c.Status = status
	}
	c.RolloutState = rollout
	return nil
}

func TestDeploymentStartPreservesFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rollout *pubsub.PRRolloutState
	}{
		{"duplicate without evidence", nil},
		{"additional job evidence", &pubsub.PRRolloutState{ExpectedJobs: []string{"web", "worker"}, StartedJobs: []string{"worker"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}}
			b := &BuildObject{store: store}
			_, err := b.recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b1", pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", RolloutState: tc.rollout})
			require.NoError(t, err)
			require.Equal(t, statusDeployFailed, store.chunks[0].Status)
			require.Equal(t, "expired", store.chunks[0].Banner)
			require.Equal(t, tc.rollout, store.chunks[0].RolloutState)
		})
	}
}
