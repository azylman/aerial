package deploygate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestDeploymentStartBeforeMerge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, revision, head string
		wantArmed                    bool
		elapsed                      time.Duration
		wantDelay                    time.Duration
	}{
		{"matching merge", statusMerged, "merge", "head", true, 5 * time.Minute, 10 * time.Minute},
		{"already deploying after deadline", statusDeploying, "merge", "head", true, 20 * time.Minute, 0},
		{"merge after deadline", statusMerged, "merge", "head", true, 20 * time.Minute, 0},
		{"obsolete merge", statusMerged, "new-merge", "new-head", false, 5 * time.Minute, 0},
		{"completed before merge", statusSynced, "merge", "head", false, 5 * time.Minute, 0},
		{"cancelled before merge", statusCancelled, "merge", "head", false, 5 * time.Minute, 0},
		{"head superseded", statusMerged, "merge", "new-head", false, 5 * time.Minute, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := newFakeRuntime("b")
			rt.now = now
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, Status: "pr_opened", HeadSHA: "head"}}}
			ev := pubsub.PRLifecycleEvent{Event: DeployStartedEvent, Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", OccurredAt: now, RolloutState: &pubsub.PRRolloutState{StartedJobs: []string{"web"}}}
			g := NewGate(st, nil)
			require.NoError(t, g.ArmDeploymentStart(rt, "b", ev))
			require.Empty(t, rt.attempts)
			rt.now = now.Add(2 * time.Minute)
			ev.OccurredAt = rt.now
			ev.RolloutState = &pubsub.PRRolloutState{StartedJobs: []string{"web", "worker"}}
			require.NoError(t, g.ArmDeploymentStart(rt, "b", ev))
			rt.now = now.Add(tc.elapsed)
			st.chunks[0].Status = tc.status
			st.chunks[0].MergeSHA = tc.revision
			st.chunks[0].HeadSHA = tc.head
			state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: append([]model.VanadisChunk(nil), st.chunks...)}
			require.NoError(t, NewGate(st, nil).SyncDeadlines(rt, state, nil, nil))
			require.Equal(t, tc.wantArmed, len(rt.attempts) > 0)
			require.Empty(t, rt.pending)
			if tc.wantArmed {
				require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
				require.Equal(t, tc.wantDelay, rt.delays[len(rt.delays)-1])
				require.Equal(t, statusDeploying, st.chunks[0].Status)
				require.Equal(t, []string{"web", "worker"}, st.chunks[0].RolloutState.StartedJobs)
			}
		})
	}
}

func TestPendingDeploymentFailures(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("unavailable")
	tests := []struct {
		name                                                            string
		pendingErr, attemptErr, clockErr, readErr, writeErr, journalErr error
		ev                                                              *pubsub.PRLifecycleEvent
	}{
		{name: "pending read", pendingErr: unavailable},
		{name: "attempt read", attemptErr: unavailable},
		{name: "clock", clockErr: unavailable, ev: &pubsub.PRLifecycleEvent{Event: DeployStartedEvent}},
		{name: "authority read", readErr: unavailable},
		{name: "authority write", writeErr: unavailable},
		{name: "journal", journalErr: unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			p := PendingStart{ChunkID: "c", HeadSHA: "head", Deadline: now.Add(15 * time.Minute), Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}
			rt := newFakeRuntime("b")
			rt.now = now
			rt.pendingErr = tc.pendingErr
			rt.getErr = tc.attemptErr
			rt.nowErr = tc.clockErr
			rt.runStepErr = tc.journalErr
			rt.pending = map[string]PendingStart{"c:merge": p}
			chunks := []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged}}
			st := &fakeStore{getBuildErr: tc.readErr, rolloutErr: tc.writeErr, chunks: chunks}
			state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: append([]model.VanadisChunk(nil), chunks...)}
			require.Error(t, NewGate(st, nil).SyncDeadlines(rt, state, tc.ev, nil))
			require.Empty(t, rt.scheduled)
			require.Equal(t, statusMerged, st.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentPromotionRechecksAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, status, revision string }{
		{"completed race", statusSynced, "merge"},
		{"cancelled race", statusCancelled, "merge"},
		{"superseded race", statusMerged, "other"},
		{"missing chunk", statusMerged, "missing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunkID := "c"
			if tc.name == "missing chunk" {
				chunkID = "other-chunk"
			}
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: chunkID, Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: tc.revision, Status: tc.status}}}
			out, err := NewGate(st, nil).PromotePendingStart(context.Background(), "b", PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}})
			require.NoError(t, err)
			require.NotNil(t, out.State)
			require.Equal(t, tc.status, out.State.Chunks[0].Status)
			require.Equal(t, tc.status, st.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentPreservesAuthoritativeEvidence(t *testing.T) {
	t.Parallel()
	newer := &pubsub.PRRolloutState{DeployedJobs: []string{"web"}, KustomizationStatuses: map[string]string{"core": "ready"}}
	pending := &pubsub.PRRolloutState{StartedJobs: []string{"web"}}
	tests := []struct {
		name          string
		current, want *pubsub.PRRolloutState
	}{
		{"pending fallback", nil, pending},
		{"newer authority", newer, newer},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged, RolloutState: tc.current}}}
			out, err := NewGate(st, nil).PromotePendingStart(context.Background(), "b", PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge", RolloutState: pending}})
			require.NoError(t, err)
			require.NotNil(t, out.State)
			require.Equal(t, tc.want, st.chunks[0].RolloutState)
		})
	}
}

func TestPendingDeploymentKeepsExistingAttempt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status string
		delivered    bool
	}{
		{"active attempt", statusDeploying, false},
		{"delivered attempt", statusMerged, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			a := Attempt{ChunkID: "c", PRNumber: 1, Revision: "merge", Generation: 7, Deadline: now.Add(time.Minute), Delivered: tc.delivered}
			p := PendingStart{ChunkID: "c", HeadSHA: "head", Deadline: now.Add(15 * time.Minute), Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := newFakeRuntime("b")
			rt.now = now
			rt.attempts = map[string]Attempt{"c": a}
			rt.pending = map[string]PendingStart{"c:merge": p}
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: tc.status}}}
			state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: append([]model.VanadisChunk(nil), st.chunks...)}
			require.NoError(t, NewGate(st, nil).SyncDeadlines(rt, state, nil, nil))
			require.Equal(t, a, rt.attempts["c"])
			require.Empty(t, rt.pending)
			require.Empty(t, rt.scheduled)
			require.Equal(t, tc.status, st.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentDoesNotAdoptStaleSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, revision, stateID string
		wantArmed                       bool
	}{
		{"completed", statusSynced, "merge", "c", false},
		{"cancelled", statusCancelled, "merge", "c", false},
		{"superseded", statusMerged, "new", "c", false},
		{"missing chunk dropped", statusMerged, "merge", "other", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := newFakeRuntime("b")
			rt.pending = map[string]PendingStart{"c:merge": p}
			chunks := []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: tc.revision, Status: tc.status}}
			st := &fakeStore{chunks: chunks}
			state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: []model.VanadisChunk{{ID: tc.stateID, Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusDeploying}}}
			require.NoError(t, NewGate(st, nil).SyncDeadlines(rt, state, nil, nil))
			require.Empty(t, rt.pending)
			if tc.wantArmed {
				require.NotEmpty(t, rt.attempts)
				require.NotEmpty(t, rt.scheduled)
			} else {
				require.Empty(t, rt.attempts)
				require.Empty(t, rt.scheduled)
			}
			require.Equal(t, tc.status, st.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentIgnoresIrrelevantChunks(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rt := newFakeRuntime("b")
	rt.now = now
	state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: []model.VanadisChunk{{ID: "c", Status: "pending"}}}
	starts, err := NewGate(&fakeStore{}, nil).SyncPendingStarts(rt, state, nil)
	require.NoError(t, err)
	require.Empty(t, starts)
	require.Empty(t, rt.pending)
}

func TestPendingDeploymentCapture(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name, event, head, revision string
		occurred                    time.Time
		wantPending                 bool
		wantDeadline                time.Time
	}{
		{"missing merge identity", DeployStartedEvent, "head", "", now, false, time.Time{}},
		{"stale head", DeployStartedEvent, "other", "merge", now, false, time.Time{}},
		{"past occurred timestamp", DeployStartedEvent, "head", "merge", now.Add(-5 * time.Minute), true, now.Add(10 * time.Minute)},
		{"future timestamp", DeployStartedEvent, "head", "merge", now.Add(time.Hour), true, now.Add(15 * time.Minute)},
		{"missing timestamp", " DEPLOY_STARTED ", "head", "merge", time.Time{}, true, now.Add(15 * time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := newFakeRuntime("b")
			rt.now = now
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", Status: "pr_opened"}}}
			require.NoError(t, NewGate(st, nil).ArmDeploymentStart(rt, "b", pubsub.PRLifecycleEvent{Event: tc.event, Repo: "owner/repo", PRNumber: 1, HeadSHA: tc.head, MergeSHA: tc.revision, OccurredAt: tc.occurred}))
			require.Equal(t, tc.wantPending, len(rt.pending) > 0)
			require.Empty(t, rt.attempts)
			if tc.wantPending {
				require.Equal(t, tc.wantDeadline, rt.pending["c:merge"].Deadline)
			}
		})
	}
}

func TestPendingDeploymentRechecksTerminalBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status string
		nilBuild     bool
	}{
		{"completed build", statusCompleted, false},
		{"cancelled build", statusCancelled, false},
		{"failed build", statusFailed, false},
		{"missing build", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := newFakeRuntime("b")
			rt.pending = map[string]PendingStart{"c:merge": p}
			chunks := []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusDeploying}}
			st := &fakeStore{buildStatus: tc.status, nilBuild: tc.nilBuild, chunks: chunks}
			state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: append([]model.VanadisChunk(nil), chunks...)}
			require.NoError(t, NewGate(st, nil).SyncDeadlines(rt, state, nil, nil))
			require.Empty(t, rt.attempts)
			require.Empty(t, rt.scheduled)
			require.Empty(t, rt.pending)
			require.Equal(t, tc.status, state.Build.Status)
		})
	}
}
