package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// The provisional p95 (~233s over 75 intervals) informs this engineering
// margin (~3.9x); it is not a certified deployment percentile.
const (
	maxDeployTime         = 15 * time.Minute
	deploymentDeadlineKey = "deployment_deadlines"
	prEventDeployStarted  = "deploy_started"
)

type deploymentAttempt struct {
	ChunkID    string    `json:"chunk_id"`
	PRNumber   int       `json:"pr_number"`
	Revision   string    `json:"revision"`
	Generation int       `json:"generation"`
	Deadline   time.Time `json:"deadline"`
	Delivered  bool      `json:"delivered"`
}

// DeploymentDeadlineRequest identifies an immutable deployment attempt.
type DeploymentDeadlineRequest struct {
	ChunkID    string `json:"chunk_id"`
	Revision   string `json:"revision"`
	Generation int    `json:"generation"`
}

type deploymentRuntime interface {
	objectRuntime
	GetDeploymentAttempts() (map[string]deploymentAttempt, error)
	SetDeploymentAttempts(attempts map[string]deploymentAttempt)
	DeploymentNow() (time.Time, error)
	GetPendingDeploymentStarts() (map[string]pendingDeploymentStart, error)
	SetPendingDeploymentStarts(pending map[string]pendingDeploymentStart)
	ScheduleDeploymentDeadline(req DeploymentDeadlineRequest, delay time.Duration)
}

func (r restateObjectRuntime) GetDeploymentAttempts() (map[string]deploymentAttempt, error) {
	return restate.Get[map[string]deploymentAttempt](r.ctx, deploymentDeadlineKey)
}

func (r restateObjectRuntime) SetDeploymentAttempts(a map[string]deploymentAttempt) {
	restate.Set(r.ctx, deploymentDeadlineKey, a)
}

func (r restateObjectRuntime) DeploymentNow() (time.Time, error) {
	return restate.Run(r.ctx, func(restate.RunContext) (time.Time, error) { return time.Now(), nil }, restate.WithName("deployment-clock"))
}

func (r restateObjectRuntime) ScheduleDeploymentDeadline(req DeploymentDeadlineRequest, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	_ = restate.ObjectSend(r.ctx, "Build", r.Key(), "DeploymentDeadline").Send(req, restate.WithDelay(delay))
}

func deploymentRevision(c VanadisChunk) string {
	return firstNonEmpty(strings.TrimSpace(c.MergeSHA), strings.TrimSpace(c.HeadSHA))
}

func nextDeploymentAttempt(old deploymentAttempt, c VanadisChunk, start time.Time, retry bool) deploymentAttempt {
	if old.Generation > 0 && old.PRNumber == c.PRNumber && old.Revision == deploymentRevision(c) && !retry {
		return old
	}
	return deploymentAttempt{ChunkID: c.ID, PRNumber: c.PRNumber, Revision: deploymentRevision(c), Generation: old.Generation + 1, Deadline: start.Add(maxDeployTime)}
}

// Reconciliation adopts pre-upgrade deployments once. Chunk.StartedAt is the
// coding start, not deployment start; absent a deployment event we use adoption
// time rather than pretend that field is a historical deployment timestamp.
func (b *BuildObject) syncDeploymentDeadlines(rt objectRuntime, state *BuildObjectState, ev *pubsub.PRLifecycleEvent, retry *BuildMutationRequest) error {
	dr, ok := rt.(deploymentRuntime)
	if !ok || state == nil {
		return nil
	}
	starts, err := b.syncPendingDeploymentStarts(rt, state, ev)
	if err != nil {
		return err
	}
	if !deploymentBuildActive(state.Build.Status) {
		return nil
	}
	eligible := false
	for _, c := range state.Chunks {
		if deploymentEligible(c) {
			eligible = true
		}
	}
	if !eligible && retry == nil {
		return nil
	}
	attempts, err := dr.GetDeploymentAttempts()
	if err != nil {
		return err
	}
	if attempts == nil {
		attempts = map[string]deploymentAttempt{}
	}
	if err = validateDeploymentRetry(state, attempts, retry); err != nil {
		return err
	}

	now, err := dr.DeploymentNow()
	if err != nil {
		return err
	}
	for _, c := range state.Chunks {
		if retry != nil && retry.ChunkID != c.ID {
			continue
		}
		if !deploymentEligible(c) {
			continue
		}
		old := attempts[c.ID]
		explicit := retry != nil && retry.ChunkID == c.ID
		start := deploymentStartAt(now, ev, c)
		if original, ok := starts[c.ID]; ok {
			start = original
		}
		a := nextDeploymentAttempt(old, c, start, explicit)
		if a == old {
			continue
		}
		attempts[c.ID] = a
		dr.SetDeploymentAttempts(attempts)
		dr.ScheduleDeploymentDeadline(DeploymentDeadlineRequest{c.ID, a.Revision, a.Generation}, a.Deadline.Sub(now))
	}
	return nil
}

// DeploymentDeadline returns promptly: failed delivery is retried by a delayed
// self-send, never by holding the exclusive object across a sleeping invocation.
func (b *BuildObject) DeploymentDeadline(ctx restate.ObjectContext, req DeploymentDeadlineRequest) error {
	return b.expireDeploymentDeadline(b.getRuntime(ctx), req)
}

func (b *BuildObject) expireDeploymentDeadline(rt objectRuntime, req DeploymentDeadlineRequest) error {
	dr, ok := rt.(deploymentRuntime)
	if !ok {
		return errors.New("deployment runtime unavailable")
	}
	attempts, err := dr.GetDeploymentAttempts()
	if err != nil {
		return err
	}
	a, ok := attempts[req.ChunkID]
	// In particular, never recreate state after terminal ClearAll.
	if !ok || a.Delivered || a.Revision != req.Revision || a.Generation != req.Generation {
		return nil
	}
	now, err := dr.DeploymentNow()
	if err != nil {
		return err
	}
	if now.Before(a.Deadline) {
		dr.ScheduleDeploymentDeadline(req, a.Deadline.Sub(now))
		return nil
	}
	out, err := rt.RunReconcileStep("deployment-deadline-delivery", func(ctx context.Context) (reconcileStepOutput, error) {
		return b.deliverDeploymentDeadline(ctx, rt.Key(), a)
	})
	if err != nil {
		return err
	}
	if out.Error != "" {
		dr.ScheduleDeploymentDeadline(req, time.Minute)
		return nil
	}
	rt.ScheduleReconcile()
	a.Delivered = true
	attempts[req.ChunkID] = a
	dr.SetDeploymentAttempts(attempts)
	return nil
}

func (b *BuildObject) recordDeploymentStart(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) (bool, string, error) {
	build, chunks, err := b.store.GetBuild(rc, buildID)
	if err != nil {
		return false, "", err
	}
	if build == nil || build.Status != statusRunning {
		return false, "", nil
	}
	for _, c := range chunks {
		if c.ID != chunkID || deploymentRevision(c) != firstNonEmpty(ev.MergeSHA, ev.HeadSHA) {
			continue
		}
		// Starts are progress observations, not explicit retries. Preserve failure
		// until retry_deployment resets authority; still accept updated evidence.
		if c.Status == statusDeployFailed {
			return true, "", nil
		}
		if c.Status != statusMerged && c.Status != statusDeploying {
			return false, "", nil
		}
		if ev.RolloutState == nil {
			if err = b.store.UpdatePRLifecycleStatus(rc, chunkID, statusDeploying); err != nil {
				return false, "", err
			}
		}
		return true, statusDeploying, nil
	}
	return false, "", nil
}

func (b *BuildObject) armDeploymentStart(rt objectRuntime, buildID string, ev pubsub.PRLifecycleEvent) error {
	snapshot, err := rt.RunReconcileStep("deployment-start-snapshot", func(ctx context.Context) (reconcileStepOutput, error) {
		build, chunks, err := b.store.GetBuild(ctx, buildID)
		if err != nil {
			return reconcileStepOutput{}, err
		}
		if build == nil || build.Status != statusRunning {
			return reconcileStepOutput{}, nil
		}
		return reconcileStepOutput{State: &BuildObjectState{Build: *build, Chunks: chunks}}, nil
	})
	if err != nil {
		return err
	}
	if err = b.syncDeploymentDeadlines(rt, snapshot.State, &ev, nil); err != nil {
		return err
	}

	return nil
}

func validateDeploymentRetry(state *BuildObjectState, attempts map[string]deploymentAttempt, retry *BuildMutationRequest) error {
	if retry != nil {
		valid := false
		for _, c := range state.Chunks {
			if c.ID == retry.ChunkID && deploymentEligible(c) && retry.Revision == deploymentRevision(c) && retry.Generation == attempts[c.ID].Generation {
				valid = true
			}
		}
		if !valid {
			return restate.TerminalErrorf("deployment retry attempt no longer current")
		}
	}
	return nil
}

func (b *BuildObject) deliverDeploymentDeadline(ctx context.Context, buildID string, a deploymentAttempt) (reconcileStepOutput, error) {
	result := reconcileStepOutput{}
	if b.store == nil {
		result.Error = "deployment store unavailable"
		return result, nil
	}
	build, chunks, e := b.store.GetBuild(ctx, buildID)
	if e != nil {
		return deploymentDeliveryFailure(e)
	}
	if build == nil || build.Status != statusRunning {
		return result, nil
	}
	for _, c := range chunks {
		if c.ID != a.ChunkID {
			continue
		}
		if c.PRNumber != a.PRNumber || deploymentRevision(c) != a.Revision || (c.Status != statusDeploying && c.Status != statusDeployFailed) {
			return result, nil
		}
		evidence, marshalErr := json.Marshal(c.RolloutState)
		if marshalErr != nil {
			return deploymentDeliveryFailure(marshalErr)
		}
		details := "Readiness remains unconfirmed after the 15-minute deployment deadline. Last persisted job/Kustomization evidence (not a fresh Kubernetes observation): " + string(evidence)
		ev := pubsub.PRLifecycleEvent{Event: "deploy_failed", Repo: c.Repo, PRNumber: c.PRNumber, MergeSHA: a.Revision, Details: details}
		if e = b.store.UpdateChunkBannerAndStatus(ctx, c.ID, statusDeployFailed, FormatDeployFailureBanner(ev)); e != nil {
			return deploymentDeliveryFailure(e)
		}
		if b.executor == nil || b.executor.eriClient == nil {
			result.Error = "deadline wake executor unavailable"
			return result, nil
		}
		// Strict durable acceptance reuses one identity across ambiguous HTTP errors.
		// Eri's dedup retention bounds that guarantee; delivery after retention may
		// duplicate a wake if an earlier acceptance response was lost.
		id := fmt.Sprintf("vanadis-deploy-deadline-%s-%d-%s-%d", c.ID, a.PRNumber, a.Revision, a.Generation)
		e = b.executor.eriClient.DispatchAccepted(ctx, BrainDispatchRequest{ID: id, Source: "discord", SessionID: "vanadis-deploy-deadline-" + c.ID, TargetID: build.ThreadID, Prompt: FormatDeployFailurePrompt(ev)})
		if e != nil {
			result.Error = e.Error()
		}
		return result, nil
	}
	return result, nil
}

func deploymentEligible(c VanadisChunk) bool {
	return c.Status == statusDeploying && c.PRNumber != 0 && deploymentRevision(c) != ""
}

func deploymentStartAt(now time.Time, ev *pubsub.PRLifecycleEvent, c VanadisChunk) time.Time {
	if ev != nil && ev.Repo == c.Repo && ev.PRNumber == c.PRNumber && firstNonEmpty(ev.MergeSHA, ev.HeadSHA) == deploymentRevision(c) && !ev.OccurredAt.IsZero() && ev.OccurredAt.Before(now) {
		return ev.OccurredAt
	}
	return now
}

// Delivery failures are journaled as values so retries release the object lock.
func deploymentDeliveryFailure(err error) (reconcileStepOutput, error) {
	return reconcileStepOutput{Error: err.Error()}, nil
}

func deploymentBuildActive(status string) bool { return status == "" || status == statusRunning }
