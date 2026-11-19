package deploygate

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// PendingDeploymentKey is the Restate state key for pre-merge start tracking.
const PendingDeploymentKey = "pending_deployment_starts"

// PendingStart preserves a deploy_started budget observed before the merge
// webhook proves that exact PR/revision merged.
type PendingStart struct {
	ChunkID  string                  `json:"chunk_id"`
	HeadSHA  string                  `json:"head_sha"`
	Deadline time.Time               `json:"deadline"`
	Event    pubsub.PRLifecycleEvent `json:"event"`
}

// SyncPendingStarts reconciles pre-merge deployment starts. A start can
// precede the merge webhook; its original budget is preserved separately until
// authority proves that exact PR/revision merged. ClearAll owns cleanup.
func (g *Gate) SyncPendingStarts(rt Runtime, state *model.BuildObjectState, ev *pubsub.PRLifecycleEvent) (map[string]time.Time, error) {
	if rt == nil || g.store == nil || !pendingSnapshot(state, ev) {
		return map[string]time.Time{}, nil
	}
	pending, err := rt.GetPendingStarts()
	if err != nil {
		return nil, err
	}
	if pending == nil {
		pending = map[string]PendingStart{}
	}
	if pendingStartEvent(ev) {
		now, clockErr := rt.Now()
		if clockErr != nil {
			return nil, clockErr
		}
		CapturePendingStart(pending, state.Chunks, *ev, now)
	}
	starts := map[string]time.Time{}
	if len(pending) == 0 {
		return starts, nil
	}
	attempts, err := rt.GetAttempts()
	if err != nil {
		return nil, err
	}
	for _, key := range slices.Sorted(maps.Keys(pending)) {
		p := pending[key]
		c := pendingChunk(state.Chunks, p.ChunkID)
		if c == nil {
			delete(pending, key)
			continue
		}
		if pendingObsolete(*c, p) {
			delete(pending, key)
			continue
		}
		if c.MergeSHA != p.Event.MergeSHA || (c.Status != statusMerged && c.Status != statusDeploying) {
			continue
		}
		a := attempts[c.ID]
		if existingAttempt(a, *c) {
			delete(pending, key)
			continue
		}
		out, promoteErr := rt.RunStep("pending-deployment-promote", func(ctx context.Context) (StepOutput, error) {
			return g.PromotePendingStart(ctx, rt.Key(), p)
		})
		if promoteErr != nil {
			return nil, promoteErr
		}
		if out.State != nil {
			state.Build = out.State.Build
			state.Chunks = out.State.Chunks
			if pendingPromoted(state, p) {
				starts[p.ChunkID] = p.Deadline.Add(-MaxDeployTime)
				rt.ScheduleReconcile()
			}
		}
		delete(pending, key)
	}
	rt.SetPendingStarts(pending)
	return starts, nil
}

func pendingRelevant(chunks []model.VanadisChunk, ev *pubsub.PRLifecycleEvent) bool {
	for _, c := range chunks {
		if c.PRNumber > 0 && (c.Status == statusMerged || c.Status == statusDeploying || pendingTerminal(c.Status) || (pendingStartEvent(ev) && ev.Repo == c.Repo && ev.PRNumber == c.PRNumber)) {
			return true
		}
	}
	return false
}

// CapturePendingStart records a pre-merge deploy_started observation.
func CapturePendingStart(pending map[string]PendingStart, chunks []model.VanadisChunk, ev pubsub.PRLifecycleEvent, now time.Time) {
	for _, c := range chunks {
		if c.Repo != ev.Repo || c.PRNumber != ev.PRNumber || c.MergeSHA != "" || strings.TrimSpace(ev.MergeSHA) == "" || pendingTerminal(c.Status) {
			continue
		}
		if ev.HeadSHA != "" && c.HeadSHA != "" && ev.HeadSHA != c.HeadSHA {
			continue
		}
		key := c.ID + ":" + ev.MergeSHA
		p, exists := pending[key]
		if !exists {
			p = PendingStart{ChunkID: c.ID, HeadSHA: c.HeadSHA, Deadline: pendingStartAt(now, ev).Add(MaxDeployTime), Event: ev}
		}
		if ev.RolloutState != nil {
			p.Event.RolloutState = ev.RolloutState
		}
		pending[key] = p
	}
}

func pendingStartAt(now time.Time, ev pubsub.PRLifecycleEvent) time.Time {
	if !ev.OccurredAt.IsZero() && ev.OccurredAt.Before(now) {
		return ev.OccurredAt
	}
	return now
}

func pendingTerminal(status string) bool {
	return status == statusCompleted || status == statusFailed || status == statusCancelled || status == statusSynced || status == statusDeployed || status == statusDeployFailed
}

func pendingObsolete(c model.VanadisChunk, p PendingStart) bool {
	return c.PRNumber != p.Event.PRNumber || c.Repo != p.Event.Repo || pendingTerminal(c.Status) || (c.MergeSHA != "" && c.MergeSHA != p.Event.MergeSHA) || (c.HeadSHA != "" && p.HeadSHA != "" && c.HeadSHA != p.HeadSHA)
}

func pendingChunk(chunks []model.VanadisChunk, id string) *model.VanadisChunk {
	for i := range chunks {
		if chunks[i].ID == id {
			return &chunks[i]
		}
	}
	return nil
}

// PromotePendingStart applies a pending pre-merge start once the merge proves
// authoritative.
func (g *Gate) PromotePendingStart(ctx context.Context, id string, p PendingStart) (StepOutput, error) {
	build, chunks, err := g.store.GetBuild(ctx, id)
	if err != nil {
		return StepOutput{}, err
	}
	if build == nil {
		return StepOutput{State: &model.BuildObjectState{}}, nil
	}
	snapshot := StepOutput{State: &model.BuildObjectState{Build: *build, Chunks: chunks}}
	if build.Status != statusRunning {
		return snapshot, nil
	}
	c := pendingChunk(chunks, p.ChunkID)
	if c == nil || pendingObsolete(*c, p) || c.MergeSHA != p.Event.MergeSHA || (c.Status != statusMerged && c.Status != statusDeploying) {
		return snapshot, nil
	}
	rollout := c.RolloutState
	if rollout == nil {
		rollout = p.Event.RolloutState
	}
	if rollout != nil {
		err = g.store.RecordPRRolloutState(ctx, c.ID, statusDeploying, rollout)
	} else {
		err = g.store.UpdatePRLifecycleStatus(ctx, c.ID, statusDeploying)
	}
	if err != nil {
		return StepOutput{}, err
	}
	c.Status = statusDeploying
	c.RolloutState = rollout
	return StepOutput{State: &model.BuildObjectState{Build: *build, Chunks: chunks}}, nil
}

func pendingSnapshot(state *model.BuildObjectState, ev *pubsub.PRLifecycleEvent) bool {
	return state != nil && state.Build.Status == statusRunning && pendingRelevant(state.Chunks, ev)
}

func existingAttempt(a Attempt, c model.VanadisChunk) bool {
	return a.Generation > 0 && a.PRNumber == c.PRNumber && a.Revision == revision(c)
}

func pendingStartEvent(ev *pubsub.PRLifecycleEvent) bool {
	return ev != nil && strings.EqualFold(strings.TrimSpace(ev.Event), DeployStartedEvent)
}

func pendingPromoted(state *model.BuildObjectState, p PendingStart) bool {
	c := pendingChunk(state.Chunks, p.ChunkID)
	return state.Build.Status == statusRunning && c != nil && c.Status == statusDeploying && c.MergeSHA == p.Event.MergeSHA && !pendingObsolete(*c, p)
}
