package build

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type deploySweepSpyStore struct {
	store.VanadisStore

	build     *model.VanadisBuild
	chunks    []model.VanadisChunk
	completed []string
	repaired  []string
	sweepErr  string
	sweepSkip string
}

func (s *deploySweepSpyStore) MarkActionRepairDeployed(_ context.Context, chunkID string, _ *pubsub.PRRolloutState) error {
	if chunkID == s.sweepErr {
		return errors.New("repair mark unavailable")
	}
	if chunkID == s.sweepSkip {
		return store.ErrPRTransitionSkipped
	}
	for i := range s.chunks {
		if s.chunks[i].ID == chunkID && s.chunks[i].Mode == "action" && s.chunks[i].PRNumber > 0 {
			s.chunks[i].Status = statusRunning
			s.repaired = append(s.repaired, chunkID)
			return nil
		}
	}
	return store.ErrPRTransitionSkipped
}

func (s *deploySweepSpyStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return s.build, s.chunks, nil
}

func (*deploySweepSpyStore) SweepEarlierDeploymentsAcrossBuilds(context.Context, model.VanadisChunk) ([]store.SweptChunk, []store.SweptChunk, error) {
	return nil, nil, nil
}

func (s *deploySweepSpyStore) CompletePRDeployment(_ context.Context, chunkID string, _ *pubsub.PRRolloutState) error {
	if chunkID == s.sweepErr {
		return errors.New("sweep unavailable")
	}
	if chunkID == s.sweepSkip {
		return store.ErrPRTransitionSkipped
	}
	for i := range s.chunks {
		if s.chunks[i].ID != chunkID {
			continue
		}
		if s.chunks[i].Status == statusFailed || s.chunks[i].Status == statusCancelled {
			return store.ErrPRTransitionSkipped
		}
		s.chunks[i].Status = statusCompleted
		s.completed = append(s.completed, chunkID)
		return nil
	}
	return store.ErrPRTransitionSkipped
}

func TestCompletePRDeploymentMutation_SweepsEarlierChunks(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	mergeLater := mergeBase.Add(2 * time.Hour)
	newChunks := func() []model.VanadisChunk {
		return []model.VanadisChunk{
			{ID: "earlier", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: statusMerged, CompletedAt: mergeBase},
			{ID: "deploying", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: statusDeploying, CompletedAt: mergeLater},
		}
	}
	repairDeploying := func() []model.VanadisChunk {
		return []model.VanadisChunk{
			{ID: "earlier", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: statusMerged, CompletedAt: mergeBase},
			{ID: "deploying", BuildID: "b1", ChunkIndex: 1, Mode: "action", Repo: "aerial", Status: statusDeploying, PRNumber: 9, CompletedAt: mergeLater},
		}
	}
	repairSwept := func() []model.VanadisChunk {
		return []model.VanadisChunk{
			{ID: "earlier", BuildID: "b1", ChunkIndex: 0, Mode: "action", Repo: "aerial", Status: statusMerged, PRNumber: 9, CompletedAt: mergeBase},
			{ID: "deploying", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: statusDeploying, CompletedAt: mergeLater},
		}
	}
	tests := []struct {
		name           string
		buildStatus    string
		chunks         []model.VanadisChunk
		deployID       string
		sweepErr       string
		sweepSkip      string
		withExecutor   bool
		wantErr        bool
		wantSweepEvent bool
		wantCompleted  []string
		wantRepaired   []string
		wantNoEvent    string
		wantStatus     string
	}{
		{name: "deploying chunk and earlier merged chunk complete", buildStatus: statusRunning, chunks: newChunks(), deployID: "deploying", wantCompleted: []string{"deploying", "earlier"}},
		{name: "failed build still completes deploying and earlier merged chunks", buildStatus: statusFailed, chunks: newChunks(), deployID: "deploying", wantCompleted: []string{"deploying", "earlier"}},
		{name: "cancelled build completes nothing", buildStatus: statusCancelled, chunks: newChunks(), deployID: "deploying", wantCompleted: nil},
		{name: "swept write skip continues without error", buildStatus: statusRunning, chunks: newChunks(), deployID: "deploying", sweepSkip: "earlier", wantCompleted: []string{"deploying"}},
		{name: "swept write error aborts", buildStatus: statusRunning, chunks: newChunks(), deployID: "deploying", sweepErr: "earlier", wantErr: true, wantCompleted: []string{"deploying"}},
		{name: "deployed repair pr keeps action chunk running and still sweeps", buildStatus: statusRunning, chunks: repairDeploying(), deployID: "deploying", withExecutor: true, wantCompleted: []string{"earlier"}, wantRepaired: []string{"deploying"}, wantNoEvent: "deploying", wantStatus: statusRunning},
		{name: "skipped repair mark completes nothing", buildStatus: statusRunning, chunks: repairDeploying(), deployID: "deploying", sweepSkip: "deploying", wantStatus: statusDeploying},
		{name: "repair mark error aborts", buildStatus: statusRunning, chunks: repairDeploying(), deployID: "deploying", sweepErr: "deploying", wantErr: true, wantStatus: statusDeploying},
		{name: "swept action repair stays running without completed event", buildStatus: statusRunning, chunks: repairSwept(), deployID: "deploying", withExecutor: true, wantCompleted: []string{"deploying"}, wantRepaired: []string{"earlier"}, wantNoEvent: "earlier"},
		{name: "swept action repair skip continues", buildStatus: statusRunning, chunks: repairSwept(), deployID: "deploying", sweepSkip: "earlier", wantCompleted: []string{"deploying"}},
		{name: "swept action repair error aborts", buildStatus: statusRunning, chunks: repairSwept(), deployID: "deploying", sweepErr: "earlier", wantErr: true, wantCompleted: []string{"deploying"}},
		{name: "swept completion publishes completed event", buildStatus: statusRunning, chunks: newChunks(), deployID: "deploying", withExecutor: true, wantSweepEvent: true, wantCompleted: []string{"deploying", "earlier"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &deploySweepSpyStore{build: &model.VanadisBuild{ID: "b1", Status: tc.buildStatus}, chunks: tc.chunks, sweepErr: tc.sweepErr, sweepSkip: tc.sweepSkip}
			var exec *executor.Executor
			bus := &recordingChunkBus{}
			if tc.withExecutor {
				exec = executor.NewExecutor("", &mockEriClient{}, bus, "token", nil)
			}
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)
			err := bo.completePRDeploymentMutation(context.Background(), "b1", &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: tc.deployID})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantCompleted, spy.completed)
			assert.Equal(t, tc.wantRepaired, spy.repaired)
			if tc.wantNoEvent != "" {
				assert.False(t, bus.sweptEvent(tc.wantNoEvent), "action chunk %q must never publish completed on deploy, got %+v", tc.wantNoEvent, bus.events)
			}
			if tc.wantStatus != "" {
				deploying, _ := findDeployingChunk(spy.chunks, tc.deployID)
				assert.Equal(t, tc.wantStatus, deploying.Status)
			}
			if tc.wantSweepEvent {
				assert.True(t, bus.sweptEvent("earlier"), "expected vanadis_chunk completed event for swept chunk %q, got %+v", "earlier", bus.events)
			}
		})
	}
}

func TestIsActionCompletionMutation(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{
		{ID: "plain", Mode: "action"},
		{ID: "repaired", Mode: "action", PRNumber: 9},
		{ID: "pr", Mode: "pr", PRNumber: 7},
	}
	tests := []struct {
		name     string
		mutation *BuildMutationRequest
		want     bool
	}{
		{"nil mutation", nil, false},
		{"verdict on plain action", &BuildMutationRequest{Action: "action_completed", ChunkID: "plain"}, true},
		{"verdict on action after repair deployed", &BuildMutationRequest{Action: " Action_Completed ", ChunkID: "repaired"}, true},
		{"deploy success on plain action", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "plain"}, true},
		{"repair deploy never completes action", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "repaired"}, false},
		{"pr chunk deploy", &BuildMutationRequest{Action: "chunk_deploy_success", ChunkID: "pr"}, false},
		{"unknown chunk", &BuildMutationRequest{Action: "action_completed", ChunkID: "missing"}, false},
		{"other action", &BuildMutationRequest{Action: "chunk_failed", ChunkID: "plain"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isActionCompletionMutation(tc.mutation, chunks))
		})
	}
}
