package main

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

const pendingDeploymentKey = "pending_deployment_starts"

type pendingDeploymentStart struct {
	ChunkID  string                  `json:"chunk_id"`
	HeadSHA  string                  `json:"head_sha"`
	Deadline time.Time               `json:"deadline"`
	Event    pubsub.PRLifecycleEvent `json:"event"`
}

func (r restateObjectRuntime) GetPendingDeploymentStarts() (map[string]pendingDeploymentStart, error) {
	return restate.Get[map[string]pendingDeploymentStart](r.ctx, pendingDeploymentKey)
}

func (r restateObjectRuntime) SetPendingDeploymentStarts(p map[string]pendingDeploymentStart) {
	restate.Set(r.ctx, pendingDeploymentKey, p)
}

// A start can precede the merge webhook. Preserve its original budget separately
// until authority proves that exact PR/revision merged. ClearAll owns cleanup.
func (b *BuildObject) syncPendingDeploymentStarts(rt objectRuntime, state *BuildObjectState, ev *pubsub.PRLifecycleEvent) (map[string]time.Time, error) {
	dr, ok := rt.(deploymentRuntime)
	if !ok || b.store == nil || !pendingDeploymentSnapshot(state, ev) {
		return map[string]time.Time{}, nil
	}
	pending, err := dr.GetPendingDeploymentStarts()
	if err != nil {
		return nil, err
	}
	if pending == nil {
		pending = map[string]pendingDeploymentStart{}
	}
	if pendingDeploymentStartEvent(ev) {
		now, clockErr := dr.DeploymentNow()
		if clockErr != nil {
			return nil, clockErr
		}
		capturePendingDeploymentStart(pending, state.Chunks, *ev, now)
	}
	starts := map[string]time.Time{}
	if len(pending) == 0 {
		return starts, nil
	}
	attempts, err := dr.GetDeploymentAttempts()
	if err != nil {
		return nil, err
	}
	for _, key := range slices.Sorted(maps.Keys(pending)) {
		p := pending[key]
		c := pendingDeploymentChunk(state.Chunks, p.ChunkID)
		if c == nil {
			delete(pending, key)
			continue
		}
		if pendingDeploymentObsolete(*c, p) {
			delete(pending, key)
			continue
		}
		if c.MergeSHA != p.Event.MergeSHA || (c.Status != statusMerged && c.Status != statusDeploying) {
			continue
		}
		a := attempts[c.ID]
		if existingDeploymentAttempt(a, *c) {
			delete(pending, key)
			continue
		}
		out, promoteErr := rt.RunReconcileStep("pending-deployment-promote", func(ctx context.Context) (reconcileStepOutput, error) {
			return b.promotePendingDeploymentStart(ctx, rt.Key(), p)
		})
		if promoteErr != nil {
			return nil, promoteErr
		}
		if out.State != nil {
			state.Build = out.State.Build
			state.Chunks = out.State.Chunks
			if pendingDeploymentPromoted(state, p) {
				starts[p.ChunkID] = p.Deadline.Add(-maxDeployTime)
				rt.ScheduleReconcile()
			}
		}
		delete(pending, key)
	}
	dr.SetPendingDeploymentStarts(pending)
	return starts, nil
}

func pendingDeploymentRelevant(chunks []VanadisChunk, ev *pubsub.PRLifecycleEvent) bool {
	for _, c := range chunks {
		if c.PRNumber > 0 && (c.Status == statusMerged || c.Status == statusDeploying || pendingDeploymentTerminal(c.Status) || (pendingDeploymentStartEvent(ev) && ev.Repo == c.Repo && ev.PRNumber == c.PRNumber)) {
			return true
		}
	}
	return false
}

func capturePendingDeploymentStart(pending map[string]pendingDeploymentStart, chunks []VanadisChunk, ev pubsub.PRLifecycleEvent, now time.Time) {
	for _, c := range chunks {
		if c.Repo != ev.Repo || c.PRNumber != ev.PRNumber || c.MergeSHA != "" || strings.TrimSpace(ev.MergeSHA) == "" || pendingDeploymentTerminal(c.Status) {
			continue
		}
		if ev.HeadSHA != "" && c.HeadSHA != "" && ev.HeadSHA != c.HeadSHA {
			continue
		}
		key := c.ID + ":" + ev.MergeSHA
		p, exists := pending[key]
		if !exists {
			p = pendingDeploymentStart{ChunkID: c.ID, HeadSHA: c.HeadSHA, Deadline: pendingDeploymentStartAt(now, ev).Add(maxDeployTime), Event: ev}
		}
		if ev.RolloutState != nil {
			p.Event.RolloutState = ev.RolloutState
		}
		pending[key] = p
	}
}

func pendingDeploymentStartAt(now time.Time, ev pubsub.PRLifecycleEvent) time.Time {
	if !ev.OccurredAt.IsZero() && ev.OccurredAt.Before(now) {
		return ev.OccurredAt
	}
	return now
}

func pendingDeploymentTerminal(status string) bool {
	return status == statusCompleted || status == statusFailed || status == statusCancelled || status == statusSynced || status == "deployed" || status == statusDeployFailed
}

func pendingDeploymentObsolete(c VanadisChunk, p pendingDeploymentStart) bool {
	return c.PRNumber != p.Event.PRNumber || c.Repo != p.Event.Repo || pendingDeploymentTerminal(c.Status) || (c.MergeSHA != "" && c.MergeSHA != p.Event.MergeSHA) || (c.HeadSHA != "" && p.HeadSHA != "" && c.HeadSHA != p.HeadSHA)
}

func pendingDeploymentChunk(chunks []VanadisChunk, id string) *VanadisChunk {
	for i := range chunks {
		if chunks[i].ID == id {
			return &chunks[i]
		}
	}
	return nil
}

func (b *BuildObject) promotePendingDeploymentStart(ctx context.Context, id string, p pendingDeploymentStart) (reconcileStepOutput, error) {
	build, chunks, err := b.store.GetBuild(ctx, id)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	if build == nil {
		return reconcileStepOutput{State: &BuildObjectState{}}, nil
	}
	snapshot := reconcileStepOutput{State: &BuildObjectState{Build: *build, Chunks: chunks}}
	if build.Status != statusRunning {
		return snapshot, nil
	}
	c := pendingDeploymentChunk(chunks, p.ChunkID)
	if c == nil || pendingDeploymentObsolete(*c, p) || c.MergeSHA != p.Event.MergeSHA || (c.Status != statusMerged && c.Status != statusDeploying) {
		return snapshot, nil
	}
	rollout := c.RolloutState
	if rollout == nil {
		rollout = p.Event.RolloutState
	}
	if rollout != nil {
		err = b.store.RecordPRRolloutState(ctx, c.ID, statusDeploying, rollout)
	} else {
		err = b.store.UpdatePRLifecycleStatus(ctx, c.ID, statusDeploying)
	}
	if err != nil {
		return reconcileStepOutput{}, err
	}
	c.Status = statusDeploying
	c.RolloutState = rollout
	return reconcileStepOutput{State: &BuildObjectState{Build: *build, Chunks: chunks}}, nil
}

func pendingDeploymentSnapshot(state *BuildObjectState, ev *pubsub.PRLifecycleEvent) bool {
	return state != nil && state.Build.Status == statusRunning && pendingDeploymentRelevant(state.Chunks, ev)
}

func existingDeploymentAttempt(a deploymentAttempt, c VanadisChunk) bool {
	return a.Generation > 0 && a.PRNumber == c.PRNumber && a.Revision == deploymentRevision(c)
}

func pendingDeploymentStartEvent(ev *pubsub.PRLifecycleEvent) bool {
	return ev != nil && strings.EqualFold(strings.TrimSpace(ev.Event), prEventDeployStarted)
}

func pendingDeploymentPromoted(state *BuildObjectState, p pendingDeploymentStart) bool {
	c := pendingDeploymentChunk(state.Chunks, p.ChunkID)
	return state.Build.Status == statusRunning && c != nil && c.Status == statusDeploying && c.MergeSHA == p.Event.MergeSHA && !pendingDeploymentObsolete(*c, p)
}
