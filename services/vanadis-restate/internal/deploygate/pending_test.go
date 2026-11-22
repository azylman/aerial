package deploygate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// PromotePendingStart is the deploy-start-before-merge recovery step: it must
// recheck authority (a completed, cancelled, superseded or missing chunk is
// never promoted) and keep newer authoritative rollout evidence over the
// pending start's snapshot.
func TestPromotePendingStart(t *testing.T) {
	t.Parallel()
	newer := &pubsub.PRRolloutState{DeployedJobs: []string{"web"}, KustomizationStatuses: map[string]string{"core": "ready"}}
	pendingRollout := &pubsub.PRRolloutState{StartedJobs: []string{"web"}}
	start := PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge", RolloutState: pendingRollout}}
	bare := PendingStart{ChunkID: "c", HeadSHA: "head", Event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "merge"}}
	merged := deadlineChunk(statusMerged, "merge")
	tests := []struct {
		name          string
		store         fakeStore
		p             PendingStart
		wantErr       string
		wantOutBuild  string
		wantOutStatus string
		wantStatus    string
		wantRollout   *pubsub.PRRolloutState
	}{
		{name: "completed race", store: fakeStore{chunks: []model.VanadisChunk{deadlineChunk(statusSynced, "merge")}}, p: bare, wantOutBuild: statusRunning, wantOutStatus: statusSynced, wantStatus: statusSynced},
		{name: "cancelled race", store: fakeStore{chunks: []model.VanadisChunk{deadlineChunk(statusCancelled, "merge")}}, p: bare, wantOutBuild: statusRunning, wantOutStatus: statusCancelled, wantStatus: statusCancelled},
		{name: "superseded race", store: fakeStore{chunks: []model.VanadisChunk{deadlineChunk(statusMerged, "other")}}, p: bare, wantOutBuild: statusRunning, wantOutStatus: statusMerged, wantStatus: statusMerged},
		{
			name: "missing chunk", store: fakeStore{chunks: []model.VanadisChunk{{ID: "other-chunk", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged}}},
			p: bare, wantOutBuild: statusRunning, wantOutStatus: statusMerged, wantStatus: statusMerged,
		},
		{name: "terminal build returns the snapshot", store: fakeStore{buildStatus: statusCompleted, chunks: []model.VanadisChunk{merged}}, p: start, wantOutBuild: statusCompleted, wantOutStatus: statusMerged, wantStatus: statusMerged},
		{name: "missing build returns an empty state", store: fakeStore{nilBuild: true, chunks: []model.VanadisChunk{merged}}, p: start, wantStatus: statusMerged},
		{
			name: "pending rollout fills missing evidence", store: fakeStore{chunks: []model.VanadisChunk{merged}}, p: start,
			wantOutBuild: statusRunning, wantOutStatus: statusDeploying, wantStatus: statusDeploying, wantRollout: pendingRollout,
		},
		{
			name: "newer authoritative evidence is kept", store: fakeStore{chunks: []model.VanadisChunk{{ID: "c", Repo: "owner/repo", PRNumber: 1, HeadSHA: "head", MergeSHA: "merge", Status: statusMerged, RolloutState: newer}}},
			p: start, wantOutBuild: statusRunning, wantOutStatus: statusDeploying, wantStatus: statusDeploying, wantRollout: newer,
		},
		{name: "no rollout evidence updates the status only", store: fakeStore{chunks: []model.VanadisChunk{merged}}, p: bare, wantOutBuild: statusRunning, wantOutStatus: statusDeploying, wantStatus: statusDeploying},
		{name: "authority read fails", store: fakeStore{getBuildErr: errors.New("read unavailable"), chunks: []model.VanadisChunk{merged}}, p: start, wantErr: "read unavailable", wantStatus: statusMerged},
		{name: "rollout write fails", store: fakeStore{rolloutErr: errors.New("write unavailable"), chunks: []model.VanadisChunk{merged}}, p: start, wantErr: "write unavailable", wantStatus: statusMerged},
		{name: "status write fails", store: fakeStore{statusErr: errors.New("write unavailable"), chunks: []model.VanadisChunk{merged}}, p: bare, wantErr: "write unavailable", wantStatus: statusMerged},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := tc.store
			out, err := NewGate(&st, nil).PromotePendingStart(context.Background(), "b", tc.p)
			requireDeadlineErr(t, err, tc.wantErr)
			var outBuild, outStatus string
			if out.State != nil {
				outBuild = out.State.Build.Status
				if len(out.State.Chunks) > 0 {
					outStatus = out.State.Chunks[0].Status
				}
			}
			require.Equal(t, tc.wantErr == "", out.State != nil)
			require.Equal(t, tc.wantOutBuild, outBuild)
			require.Equal(t, tc.wantOutStatus, outStatus)
			require.Equal(t, tc.wantStatus, st.chunks[0].Status)
			require.Equal(t, tc.wantRollout, st.chunks[0].RolloutState)
		})
	}
}

func TestPendingDeploymentIgnoresIrrelevantChunks(t *testing.T) {
	t.Parallel()
	rt := &fakeRuntime{key: "b", now: deadlineNow}
	state := &model.BuildObjectState{Build: model.VanadisBuild{Status: statusRunning}, Chunks: []model.VanadisChunk{{ID: "c", Status: "pending"}}}
	starts, err := NewGate(&fakeStore{}, nil).SyncPendingStarts(rt, state, nil)
	require.NoError(t, err)
	require.Empty(t, starts)
	require.Empty(t, rt.pending)
}
