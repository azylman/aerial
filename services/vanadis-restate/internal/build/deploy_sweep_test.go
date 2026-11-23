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

// sweepFaults maps the sweep table's fault columns onto fakeStore errs:
// errChunk fails and skipChunk skips both deployment writes for that chunk,
// and crossErr fails the cross-build sweep after it returns its chunks.
func sweepFaults(errChunk, skipChunk string, crossErr bool) map[string]error {
	errs := map[string]error{}
	if errChunk != "" {
		errs["CompletePRDeployment:"+errChunk] = errors.New("sweep unavailable")
		errs["MarkActionRepairDeployed:"+errChunk] = errors.New("repair mark unavailable")
	}
	if skipChunk != "" {
		errs["CompletePRDeployment:"+skipChunk] = store.ErrPRTransitionSkipped
		errs["MarkActionRepairDeployed:"+skipChunk] = store.ErrPRTransitionSkipped
	}
	if crossErr {
		errs["SweepEarlierDeploymentsAcrossBuilds"] = errors.New("cross build sweep unavailable")
	}
	return errs
}

// sweepEventBus is a bus whose completed vanadis_chunk events the sweep
// table reads back.
type sweepEventBus interface {
	pubsub.Bus
	completedTargets() []string
}

// completedTargets lists the chunk IDs of published vanadis_chunk completed
// events in publish order.
func (b *recordingChunkBus) completedTargets() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var ids []string
	for _, ev := range b.events {
		if ev.Source == "vanadis_chunk" && ev.EventType == "completed" {
			ids = append(ids, ev.Targets...)
		}
	}
	return ids
}

// TestCompletePRDeploymentMutation covers deploy_success for the chunk
// "deploying" of build b1: own-build completion or repair marking, the
// same-build sweep of earlier merged chunks, and the cross-build sweep that
// hands every other build to its own runner via a queued Reconcile.
func TestCompletePRDeploymentMutation(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	dep := func(mode, status string, jobs ...string) model.VanadisChunk {
		c := model.VanadisChunk{ID: "deploying", BuildID: "b1", ChunkIndex: 1, Mode: mode, Repo: "aerial", Status: status, PRNumber: 7, CompletedAt: mergeBase.Add(2 * time.Hour)}
		if len(jobs) > 0 {
			c.RolloutState = &pubsub.PRRolloutState{ExpectedJobs: jobs}
		}
		return c
	}
	noPR := dep("pr", statusFailed, "eri")
	noPR.PRNumber = 0
	earlier := func(mode string) model.VanadisChunk {
		return model.VanadisChunk{ID: "earlier", BuildID: "b1", Mode: mode, Repo: "aerial", Status: statusMerged, PRNumber: 9, CompletedAt: mergeBase}
	}
	f1 := []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}}
	b2 := []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}}
	eri := []string{"eri"}
	tests := []struct {
		name, buildStatus           string
		own                         []model.VanadisChunk
		mutationJobs                []string
		sweepErr, sweepSkip         string
		crossSwept                  []store.SweptChunk
		crossErr                    bool
		bus                         sweepEventBus
		wantErr                     string
		wantCrossCalled             bool
		wantCompleted, wantRepaired []string
		wantStatus                  string
		wantDeployJobs              []string
		wantReconciles              []store.SweptChunk
		wantEvents                  []string
	}{
		// Same-build sweep of earlier merged chunks.
		{name: "deploying and earlier merged chunk complete", buildStatus: statusRunning, own: cks(earlier("pr"), dep("pr", statusDeploying)), wantCrossCalled: true, wantCompleted: []string{"deploying", "earlier"}, wantStatus: statusCompleted},
		{name: "failed build still completes deploying and earlier", buildStatus: statusFailed, own: cks(earlier("pr"), dep("pr", statusDeploying)), wantCrossCalled: true, wantCompleted: []string{"deploying", "earlier"}, wantStatus: statusCompleted},
		{name: "cancelled build completes nothing", buildStatus: statusCancelled, own: cks(earlier("pr"), dep("pr", statusDeploying)), wantStatus: statusDeploying},
		{name: "swept write skip continues", buildStatus: statusRunning, own: cks(earlier("pr"), dep("pr", statusDeploying)), sweepSkip: "earlier", wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted},
		{name: "swept write error aborts", buildStatus: statusRunning, own: cks(earlier("pr"), dep("pr", statusDeploying)), sweepErr: "earlier", wantErr: "sweep unavailable", wantCompleted: []string{"deploying"}, wantStatus: statusCompleted},
		{name: "swept completion publishes completed event", buildStatus: statusRunning, own: cks(earlier("pr"), dep("pr", statusDeploying)), bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying", "earlier"}, wantStatus: statusCompleted, wantEvents: []string{"deploying", "earlier"}},
		// Deployed repair PR on an action chunk: it stays running, without a completed event.
		{name: "deployed repair keeps action running and still sweeps", buildStatus: statusRunning, own: cks(earlier("pr"), dep("action", statusDeploying)), bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"earlier"}, wantRepaired: []string{"deploying"}, wantStatus: statusRunning, wantEvents: []string{"earlier"}},
		{name: "skipped repair mark completes nothing", buildStatus: statusRunning, own: cks(earlier("pr"), dep("action", statusDeploying)), sweepSkip: "deploying", wantStatus: statusDeploying},
		{name: "repair mark error aborts", buildStatus: statusRunning, own: cks(earlier("pr"), dep("action", statusDeploying)), sweepErr: "deploying", wantErr: "repair mark unavailable", wantStatus: statusDeploying},
		{name: "swept action repair stays running without completed event", buildStatus: statusRunning, own: cks(earlier("action"), dep("pr", statusDeploying)), bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantRepaired: []string{"earlier"}, wantStatus: statusCompleted, wantEvents: []string{"deploying"}},
		{name: "swept action repair skip continues", buildStatus: statusRunning, own: cks(earlier("action"), dep("pr", statusDeploying)), sweepSkip: "earlier", wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted},
		{name: "swept action repair error aborts", buildStatus: statusRunning, own: cks(earlier("action"), dep("pr", statusDeploying)), sweepErr: "earlier", wantErr: "repair mark unavailable", wantCompleted: []string{"deploying"}, wantStatus: statusCompleted},
		// Eligibility of failed deploying chunks.
		{name: "failed pr chunk with pr number in running build completes", buildStatus: statusRunning, own: cks(dep("pr", statusFailed, "eri")), crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2, wantEvents: []string{"deploying"}},
		{name: "failed pr chunk with pr number in failed build completes", buildStatus: statusFailed, own: cks(dep("pr", statusFailed, "eri")), crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2, wantEvents: []string{"deploying"}},
		{name: "failed pr chunk without pr number returns early", buildStatus: statusRunning, own: cks(noPR), crossSwept: f1, wantStatus: statusFailed},
		{name: "failed action chunk with pr returns early", buildStatus: statusRunning, own: cks(dep("action", statusFailed, "eri")), crossSwept: f1, wantStatus: statusFailed},
		// Cross-build sweep: other builds are reconciled by their own runner, never here.
		{name: "deployed action repair still sweeps other builds", buildStatus: statusRunning, own: cks(dep("action", statusDeploying, "eri")), crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantRepaired: []string{"deploying"}, wantStatus: statusRunning, wantDeployJobs: eri, wantReconciles: b2},
		{name: "action repair cross sweep error aborts", buildStatus: statusRunning, own: cks(dep("action", statusDeploying, "eri")), crossErr: true, wantErr: "cross build sweep unavailable", wantCrossCalled: true, wantRepaired: []string{"deploying"}, wantStatus: statusRunning, wantDeployJobs: eri},
		{name: "foreign swept chunk queues its build reconcile", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: f1, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2},
		{name: "two swept chunks of one build queue both chunk mutations", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}, {ChunkID: "f2", BuildID: "b2"}}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}, {ChunkID: "f2", BuildID: "b2"}}},
		{name: "no foreign sweep queues nothing", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri},
		{name: "cross sweep error aborts", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossErr: true, wantErr: "cross build sweep unavailable", wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri},
		{name: "cancelled own build skips cross sweep", buildStatus: statusCancelled, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: f1, wantStatus: statusDeploying},
		{name: "swept foreign chunks do not publish local completed events", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2, wantEvents: []string{"deploying"}},
		{name: "duplicate swept chunks are deduplicated", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: []store.SweptChunk{{ChunkID: "f1", BuildID: "b2"}, {ChunkID: "f1", BuildID: "b2"}}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2},
		{name: "mutation rollout jobs overlay onto stale snapshot", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying)), mutationJobs: eri, crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2, wantEvents: []string{"deploying"}},
		{name: "absent mutation jobs keep stored jobs", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "worker")), crossSwept: f1, bus: &recordingChunkBus{}, wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: []string{"worker"}, wantReconciles: b2, wantEvents: []string{"deploying"}},
		{name: "store error after partial sweep still processes swept chunks", buildStatus: statusRunning, own: cks(dep("pr", statusDeploying, "eri")), crossSwept: f1, crossErr: true, bus: &recordingChunkBus{}, wantErr: "cross build sweep unavailable", wantCrossCalled: true, wantCompleted: []string{"deploying"}, wantStatus: statusCompleted, wantDeployJobs: eri, wantReconciles: b2, wantEvents: []string{"deploying"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &fakeStore{builds: []model.VanadisBuild{{ID: "b1", Status: tc.buildStatus}}, chunks: tc.own, swept: tc.crossSwept, errs: sweepFaults(tc.sweepErr, tc.sweepSkip, tc.crossErr)}
			var exec *executor.Executor
			if tc.bus != nil {
				exec = executor.NewExecutor("", &mockEriClient{}, tc.bus, "token", "example-owner", nil)
			}
			mutation := &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: "deploying"}
			if tc.mutationJobs != nil {
				mutation.RolloutState = &pubsub.PRRolloutState{ExpectedJobs: tc.mutationJobs}
			}
			ctx, crossBuild := withCrossBuildReconciles(context.Background())
			err := NewBuildObject("", nil).WithAuthority(spy, exec).completePRDeploymentMutation(ctx, "b1", mutation)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			calls := spy.snapshotCalls()
			sweeps := callArgs(spy.attemptedCalls(), "SweepEarlierDeploymentsAcrossBuilds")
			var deployJobs []string
			if len(sweeps) > 0 && len(sweeps[0]) > 1 {
				deployJobs = sweeps[0][1:]
			}
			assert.Equal(t, tc.wantCrossCalled, len(sweeps) > 0)
			assert.Equal(t, tc.wantCompleted, pickCalls(calls, "CompletePRDeployment", "", 0))
			assert.Equal(t, tc.wantRepaired, pickCalls(calls, "MarkActionRepairDeployed", "", 0))
			assert.Equal(t, tc.wantStatus, spy.chunkSnapshot("deploying").Status)
			assert.Equal(t, tc.wantDeployJobs, deployJobs)
			assert.Equal(t, tc.wantReconciles, crossBuild.list())
			assert.Empty(t, pickCalls(calls, "UpdateBuildStatus", "=", 0, 1), "other builds must never be evaluated by this runner")
			assert.Empty(t, spy.foreignReads("b1"), "other builds must never be read for local evaluation")
			if tc.bus != nil {
				assert.Equal(t, tc.wantEvents, tc.bus.completedTargets())
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
