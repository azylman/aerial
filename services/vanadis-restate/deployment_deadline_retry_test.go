package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
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
		name     string
		rollout  *pubsub.PRRolloutState
		writeErr error
	}{
		{"without snapshot", nil, nil},
		{"with snapshot", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, nil},
		{"snapshot write unavailable", &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, errors.New("write unavailable")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &deploymentStartStore{writeErr: tc.writeErr, checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c1", PRNumber: 1, MergeSHA: "sha", Status: statusMerged}}}}
			_, err := (&BuildObject{store: store}).recordDeploymentPREvent(mockRunContext{Context: context.Background()}, "b", pubsub.PRLifecycleEvent{Event: prEventDeployStarted, MergeSHA: "sha", RolloutState: tc.rollout})
			require.Equal(t, tc.writeErr != nil, err != nil)
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
			store := &deadlineLifecycleStore{checkStatusSpyStore: &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}}}}
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
