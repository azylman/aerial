package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
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
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, Status: "pr_opened", HeadSHA: "head"}}}}
			ev := pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", OccurredAt: now, RolloutState: &pubsub.PRRolloutState{StartedJobs: []string{"web"}}}
			b := &BuildObject{store: store}
			require.NoError(t, b.armDeploymentStart(rt, "b", ev))
			require.Empty(t, rt.attempts)
			rt.now = now.Add(2 * time.Minute)
			ev.OccurredAt = rt.now
			ev.RolloutState = &pubsub.PRRolloutState{StartedJobs: []string{"web", "worker"}}
			require.NoError(t, b.armDeploymentStart(rt, "b", ev))
			rt.now = now.Add(tc.elapsed)
			store.chunks[0].Status = tc.status
			store.chunks[0].MergeSHA = tc.revision
			store.chunks[0].HeadSHA = tc.head
			state := &BuildObjectState{Build: VanadisBuild{Status: statusRunning}, Chunks: append([]VanadisChunk(nil), store.chunks...)}
			require.NoError(t, (&BuildObject{store: store}).syncDeploymentDeadlines(rt, state, nil, nil))
			require.Equal(t, tc.wantArmed, len(rt.attempts) > 0)
			require.Empty(t, rt.pending)
			if tc.wantArmed {
				require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
				require.Equal(t, tc.wantDelay, rt.delays[len(rt.delays)-1])
				require.Equal(t, tc.wantDelay, rt.delays[len(rt.delays)-1])
				require.Equal(t, statusDeploying, store.chunks[0].Status)
				require.Equal(t, []string{"web", "worker"}, store.chunks[0].RolloutState.StartedJobs)
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
		{name: "clock", clockErr: unavailable, ev: &pubsub.PRLifecycleEvent{Event: prEventDeployStarted}},
		{name: "authority read", readErr: unavailable},
		{name: "authority write", writeErr: unavailable},
		{name: "journal", journalErr: unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			p := pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Deadline: now.Add(15 * time.Minute), Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now, pendingErr: tc.pendingErr, getErr: tc.attemptErr, nowErr: tc.clockErr, pending: map[string]pendingDeploymentStart{"c:merge": p}}
			rt.runStepErr = tc.journalErr
			chunks := []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged}}
			store := &deploymentStartStore{writeErr: tc.writeErr, checkStatusSpyStore: &checkStatusSpyStore{getBuildErr: tc.readErr, chunks: chunks}}
			state := &BuildObjectState{Build: VanadisBuild{Status: statusRunning}, Chunks: append([]VanadisChunk(nil), chunks...)}
			require.Error(t, (&BuildObject{store: store}).syncDeploymentDeadlines(rt, state, tc.ev, nil))
			require.Empty(t, rt.scheduled)
			require.Equal(t, statusMerged, store.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentPromotionRechecksAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, status, revision string }{
		{"completed race", statusSynced, "merge"},
		{"cancelled race", statusCancelled, "merge"},
		{"superseded race", statusMerged, "other"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &deploymentStartStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: tc.revision, Status: tc.status}}}}
			out, err := (&BuildObject{store: store}).promotePendingDeploymentStart(context.Background(), "b", pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}})
			require.NoError(t, err)
			require.NotNil(t, out.State)
			require.Equal(t, tc.status, out.State.Chunks[0].Status)
			require.Equal(t, tc.status, store.chunks[0].Status)
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
			store := &deploymentStartStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged, RolloutState: tc.current}}}}
			out, err := (&BuildObject{store: store}).promotePendingDeploymentStart(context.Background(), "b", pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge", RolloutState: pending}})
			require.NoError(t, err)
			require.NotNil(t, out.State)
			require.Equal(t, tc.want, store.chunks[0].RolloutState)
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
			a := deploymentAttempt{ChunkID: "c", PRNumber: 1, Revision: "merge", Generation: 7, Deadline: now.Add(time.Minute), Delivered: tc.delivered}
			p := pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Deadline: now.Add(15 * time.Minute), Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now, attempts: map[string]deploymentAttempt{"c": a}, pending: map[string]pendingDeploymentStart{"c:merge": p}}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: tc.status}}}}
			state := &BuildObjectState{Build: VanadisBuild{Status: statusRunning}, Chunks: append([]VanadisChunk(nil), store.chunks...)}
			require.NoError(t, (&BuildObject{store: store}).syncDeploymentDeadlines(rt, state, nil, nil))
			require.Equal(t, a, rt.attempts["c"])
			require.Empty(t, rt.pending)
			require.Empty(t, rt.scheduled)
			require.Equal(t, tc.status, store.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentDoesNotAdoptStaleSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, status, revision string }{
		{"completed", statusSynced, "merge"},
		{"cancelled", statusCancelled, "merge"},
		{"superseded", statusMerged, "new"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), pending: map[string]pendingDeploymentStart{"c:merge": p}}
			chunks := []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: tc.revision, Status: tc.status}}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: chunks}}
			state := &BuildObjectState{Build: VanadisBuild{Status: statusRunning}, Chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusDeploying}}}
			require.NoError(t, (&BuildObject{store: store}).syncDeploymentDeadlines(rt, state, nil, nil))
			require.Empty(t, rt.attempts)
			require.Empty(t, rt.pending)
			require.Empty(t, rt.scheduled)
			require.Equal(t, tc.status, store.chunks[0].Status)
		})
	}
}

func TestPendingDeploymentCapture(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name, event, head, revision string
		occurred                    time.Time
		wantPending                 bool
	}{
		{"missing merge identity", prEventDeployStarted, "head", "", now, false},
		{"stale head", prEventDeployStarted, "other", "merge", now, false},
		{"future timestamp", prEventDeployStarted, "head", "merge", now.Add(time.Hour), true},
		{"missing timestamp", " DEPLOY_STARTED ", "head", "merge", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", Status: "pr_opened"}}}}
			require.NoError(t, (&BuildObject{store: store}).armDeploymentStart(rt, "b", pubsub.PRLifecycleEvent{Event: tc.event, Repo: "owner/repo", PRNumber: 1, HeadSHA: tc.head, MergeSHA: tc.revision, OccurredAt: tc.occurred}))
			require.Equal(t, tc.wantPending, len(rt.pending) > 0)
			require.Empty(t, rt.attempts)
			if tc.wantPending {
				require.Equal(t, now.Add(15*time.Minute), rt.pending["c:merge"].Deadline)
			}
		})
	}
}

type pendingAuthorityStore struct {
	VanadisStore

	build  VanadisBuild
	chunks []VanadisChunk
}

func (s *pendingAuthorityStore) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return &s.build, s.chunks, nil
}

func TestPendingDeploymentRechecksTerminalBuild(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, status string }{
		{"completed build", statusCompleted},
		{"cancelled build", statusCancelled},
		{"failed build", statusFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := pendingDeploymentStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), pending: map[string]pendingDeploymentStart{"c:merge": p}}
			chunks := []VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusDeploying}}
			store := &pendingAuthorityStore{build: VanadisBuild{Status: tc.status}, chunks: chunks}
			state := &BuildObjectState{Build: VanadisBuild{Status: statusRunning}, Chunks: append([]VanadisChunk(nil), chunks...)}
			require.NoError(t, (&BuildObject{store: store}).syncDeploymentDeadlines(rt, state, nil, nil))
			require.Empty(t, rt.attempts)
			require.Empty(t, rt.scheduled)
			require.Empty(t, rt.pending)
			require.Equal(t, tc.status, state.Build.Status)
		})
	}
}
