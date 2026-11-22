package build

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Regression coverage for cross-build deploy sweeps: retry recovery for
// side-effect losses, mutation rollout overlay, processing swept chunks
// before surfacing a store error, and handing every other build to its own
// runner via a queued Reconcile instead of evaluating it locally.
func TestCompleteCrossBuildSweep_RecoveryAndOverlay(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	mergeLater := mergeBase.Add(2 * time.Hour)
	tests := []struct {
		name               string
		deployJobs         []string
		mutationJobs       []string
		crossSwept         []store.SweptChunk
		crossRecovered     []store.SweptChunk
		crossErr           bool
		foreignStatus      string
		foreignChunkStatus string
		wantDeployJobs     []string
		wantReconciles     []store.BuildRoute
		wantEvents         []string
		wantErr            bool
	}{
		{
			name: "recovered chunk re-queues its build reconcile on retry", deployJobs: []string{"eri"},
			crossRecovered: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
			foreignStatus:  statusRunning, foreignChunkStatus: statusCompleted,
			wantDeployJobs: []string{"eri"}, wantEvents: []string{"f1"},
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
		{
			name: "failed foreign build on sibling runner is reconciled there, not locally", deployJobs: []string{"eri"},
			crossSwept:    []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
			foreignStatus: statusFailed, foreignChunkStatus: statusMerged,
			wantDeployJobs: []string{"eri"}, wantEvents: []string{"f1"},
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
		{
			name: "swept and recovered chunks of one build queue one reconcile", deployJobs: []string{"eri"},
			crossSwept:     []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
			crossRecovered: []store.SweptChunk{{ChunkID: "f0", BuildID: "b2"}},
			foreignStatus:  statusRunning, foreignChunkStatus: statusMerged,
			wantDeployJobs: []string{"eri"}, wantEvents: []string{"f1", "f0"},
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
		{
			name:          "mutation rollout jobs overlay onto stale deploying snapshot",
			mutationJobs:  []string{"eri"},
			crossSwept:    []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
			foreignStatus: statusRunning, foreignChunkStatus: statusMerged,
			wantDeployJobs: []string{"eri"}, wantEvents: []string{"f1"},
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
		{
			name: "absent mutation jobs keep stored jobs", deployJobs: []string{"worker"},
			crossSwept:    []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}},
			foreignStatus: statusRunning, foreignChunkStatus: statusMerged,
			wantDeployJobs: []string{"worker"}, wantEvents: []string{"f1"},
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
		{
			name: "store error after partial sweep still processes swept chunks", deployJobs: []string{"eri"},
			crossSwept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}}, crossErr: true,
			foreignStatus: statusRunning, foreignChunkStatus: statusMerged,
			wantDeployJobs: []string{"eri"}, wantEvents: []string{"f1"}, wantErr: true,
			wantReconciles: []store.BuildRoute{{BuildID: "b2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stored, mutated *pubsub.PRRolloutState
			if tc.deployJobs != nil {
				stored = &pubsub.PRRolloutState{ExpectedJobs: tc.deployJobs}
			}
			if tc.mutationJobs != nil {
				mutated = &pubsub.PRRolloutState{ExpectedJobs: tc.mutationJobs}
			}
			spy := &crossBuildSweepSpyStore{
				ownBuild:       &model.VanadisBuild{ID: "b1", Status: statusRunning},
				ownChunks:      []model.VanadisChunk{{ID: "deploying", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: statusDeploying, PRNumber: 7, CompletedAt: mergeLater, RolloutState: stored}},
				foreignBuilds:  []model.VanadisBuild{{ID: "b2", Status: tc.foreignStatus}},
				foreignChunks:  []model.VanadisChunk{{ID: "f1", BuildID: "b2", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: tc.foreignChunkStatus, PRNumber: 5, CompletedAt: mergeBase, RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}}},
				crossSwept:     tc.crossSwept,
				crossRecovered: tc.crossRecovered,
				crossErr:       tc.crossErr,
			}
			bus := &recordingChunkBus{}
			exec := executor.NewExecutor("", &mockEriClient{}, bus, "token", "azylman", nil)
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			mutation := &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: "deploying", RolloutState: mutated}
			ctx, crossBuild := withCrossBuildReconciles(context.Background())
			err := bo.completePRDeploymentMutation(ctx, "b1", mutation)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantDeployJobs, spy.crossDeployJobs)
			assert.Equal(t, tc.wantReconciles, crossBuild.list())
			assert.Empty(t, spy.statusUpdates, "other builds must never be evaluated by this runner")
			assert.Empty(t, spy.foreignReads, "other builds must never be read for local evaluation")
			for _, id := range tc.wantEvents {
				assert.True(t, bus.sweptEvent(id), "expected vanadis_chunk completed event for %q, got %+v", id, bus.events)
			}
		})
	}
}

func crossBuildForeignChunks() []model.VanadisChunk {
	return []model.VanadisChunk{{ID: "f1", BuildID: "b2", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: statusMerged, PRNumber: 5, CompletedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}}}
}

func crossBuildRunningChunks() []model.VanadisChunk {
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	jobs := &pubsub.PRRolloutState{ExpectedJobs: []string{"eri"}}
	return []model.VanadisChunk{
		{ID: "f1", BuildID: "b2", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: statusDeploying, PRNumber: 5, CompletedAt: mergeBase, RolloutState: jobs},
		{ID: "f2", BuildID: "b2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: statusSynced, PRNumber: 6, CompletedAt: mergeBase, RolloutState: jobs},
		{ID: "open", BuildID: "b2", ChunkIndex: 2, Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 8},
	}
}
