package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

type deploymentStartStore struct {
	*checkStatusSpyStore

	writeErr error
}

func (s *deploymentStartStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	c := s.chunk(id)
	c.Status = status
	c.RolloutState = rollout
	return nil
}

func TestDeploymentStartEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		rollout       *pubsub.PRRolloutState
		writeErr      error
		wantLifecycle string
	}{
		{"without snapshot", nil, nil, statusDeploying},
		{"with snapshot", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, nil, ""},
		{"snapshot write unavailable", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, errors.New("write unavailable"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &deploymentStartStore{writeErr: tc.writeErr, checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, MergeSHA: "sha", Status: statusMerged}}}}
			_, err := (&BuildObject{store: store}).recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b", pubsub.PRLifecycleEvent{Event: prEventDeployStarted, MergeSHA: "sha", RolloutState: tc.rollout})
			require.Equal(t, tc.writeErr != nil, err != nil)
			require.Equal(t, tc.wantLifecycle, store.lifecycleStatus)
			if tc.writeErr == nil {
				require.Equal(t, tc.rollout, store.chunks[0].RolloutState)
			} else {
				require.Nil(t, store.chunks[0].RolloutState)
			}
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
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now, nowErr: tc.clockErr, attempts: map[string]deploymentAttempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Delivered: true}}}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}}
			b := &BuildObject{store: store}
			req := BuildMutationRequest{Action: "retry_deployment", ChunkID: "c", Revision: "sha", Generation: 1}
			_, err := b.retryDeployment(rt, req)
			require.Error(t, err)
			require.Equal(t, statusDeploying, store.chunks[0].Status)
			require.Empty(t, store.chunks[0].Banner)
			require.Equal(t, 1, rt.attempts["c"].Generation)
			rt.nowErr = nil
			_, err = b.retryDeployment(rt, req)
			require.NoError(t, err)
			require.Equal(t, 2, rt.attempts["c"].Generation)
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c"].Deadline)
			require.True(t, rt.reconcileScheduled)
		})
	}
}

func TestDeploymentDeadlineRepeatedMerge(t *testing.T) {
	t.Parallel()
	tests := []struct{ name string }{{"start then repeated merge then expiry"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt := &deadlineTestRuntime{fakeObjectRuntime: newFakeObjectRuntime("b"), now: now}
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []model.VanadisChunk{{ID: "c1", PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha", Status: statusMerged}}}}
			b := &BuildObject{store: store}
			ev := pubsub.PRLifecycleEvent{Event: prEventDeployStarted, Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: now}
			rc := mockRunContext{Context: context.Background()}
			_, err := b.recordDeploymentPREvent(rc, "b", ev)
			require.NoError(t, err)
			require.NoError(t, b.armDeploymentStart(rt, "b", ev))
			require.Equal(t, now.Add(15*time.Minute), rt.attempts["c1"].Deadline)
			require.NoError(t, b.handlePREventType(rc, "b", "c1", "merged", pubsub.PRLifecycleEvent{MergeSHA: "sha"}))
			require.Equal(t, statusMerged, store.chunks[0].Status)
			eri := &mockEriClient{}
			b.executor = &Executor{eriClient: eri}
			rt.now = now.Add(15 * time.Minute)
			require.NoError(t, b.expireDeploymentDeadline(rt, DeploymentDeadlineRequest{"c1", "sha", 1}))
			require.Equal(t, statusDeployFailed, store.chunks[0].Status)
			require.NotNil(t, eri.lastDispatch)
			require.True(t, rt.attempts["c1"].Delivered)
		})
	}
}
