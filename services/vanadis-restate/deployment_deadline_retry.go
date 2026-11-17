package main

import (
	"context"
	"errors"
	"log"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

func (b *BuildObject) retryDeployment(rt objectRuntime, req BuildMutationRequest) (ReconcileBuildResult, error) {
	if b.store == nil {
		return ReconcileBuildResult{}, restate.TerminalErrorf("deployment retry requires store")
	}
	out, err := rt.RunReconcileStep("deployment-retry-snapshot", func(ctx context.Context) (reconcileStepOutput, error) {
		build, chunks, err := b.store.GetBuild(ctx, rt.Key())
		if err != nil {
			return reconcileStepOutput{}, err
		}
		if build == nil || build.Status != statusRunning {
			return reconcileStepOutput{}, restate.TerminalErrorf("deployment build not running")
		}
		found := false
		for i := range chunks {
			if chunks[i].ID == req.ChunkID && (chunks[i].Status == statusDeploying || chunks[i].Status == statusDeployFailed) {
				chunks[i].Status = statusDeploying
				found = true
			}
		}
		if !found {
			return reconcileStepOutput{}, restate.TerminalErrorf("deployment chunk not retryable")
		}
		return reconcileStepOutput{State: &BuildObjectState{Build: *build, Chunks: chunks}}, nil
	})
	if err != nil {
		return ReconcileBuildResult{}, err
	}
	dr, ok := rt.(deploymentRuntime)
	if !ok {
		return ReconcileBuildResult{}, restate.TerminalErrorf("deployment runtime unavailable")
	}
	attempts, err := dr.GetDeploymentAttempts()
	if err != nil {
		return ReconcileBuildResult{}, err
	}
	if err = validateDeploymentRetry(out.State, attempts, &req); err != nil {
		return ReconcileBuildResult{}, err
	}
	if _, err = rt.RunReconcileStep("deployment-retry-reset", func(ctx context.Context) (reconcileStepOutput, error) {
		resetErr := b.store.UpdateChunkBannerAndStatus(ctx, req.ChunkID, statusDeploying, "")
		return reconcileStepOutput{}, resetErr
	}); err != nil {
		return ReconcileBuildResult{}, err
	}
	if err = b.syncDeploymentDeadlines(rt, out.State, nil, &req); err != nil {
		return ReconcileBuildResult{}, err
	}
	rt.ScheduleReconcile()
	return ReconcileBuildResult{BuildID: rt.Key(), Status: statusRunning}, nil
}

func (b *BuildObject) recordDeploymentPREvent(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (bool, error) {
	chunkID, cErr := b.locateOrAttachPRChunk(rc, buildID, ev)
	if cErr != nil || chunkID == "" {
		return false, cErr
	}
	rolloutStatus := ""
	event := strings.ToLower(strings.TrimSpace(ev.Event))
	if event == prEventDeployStarted {
		valid, status, e := b.recordDeploymentStart(rc, buildID, chunkID, ev)
		rolloutStatus = status
		if e != nil || !valid {
			return false, e
		}
	}

	eventErr := b.dispatchPREvent(rc, buildID, chunkID, event, ev)
	if eventErr != nil {
		if errors.Is(eventErr, errPRTransitionSkipped) {
			return false, nil
		}
		return false, eventErr
	}
	b.refreshFailedPRWidget(rc, buildID, event)
	if event != "deploy_success" && ev.RolloutState != nil {
		if rErr := b.store.RecordPRRolloutState(rc, chunkID, rolloutStatus, ev.RolloutState); rErr != nil {
			if event == prEventDeployStarted {
				return false, rErr
			}
			log.Printf("[vanadis-restate] record rollout state warning: %v", rErr)
		}
	}
	return true, nil
}
