package build

import (
	"context"
	"errors"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/deploygate"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// This file holds the thin deployment-gate delegation: Restate state glue for
// the deploygate.Runtime surface and BuildObject wrappers that forward to
// deploygate.Gate. Reflected handler names, state keys, and journal step
// names stay byte-identical via the deploygate constants.

func (r restateObjectRuntime) GetAttempts() (map[string]deploygate.Attempt, error) {
	return restate.Get[map[string]deploygate.Attempt](r.ctx, deploygate.DeploymentDeadlineKey)
}

func (r restateObjectRuntime) SetAttempts(a map[string]deploygate.Attempt) {
	restate.Set(r.ctx, deploygate.DeploymentDeadlineKey, a)
}

func (r restateObjectRuntime) Now() (time.Time, error) {
	return restate.Run(r.ctx, func(restate.RunContext) (time.Time, error) { return time.Now(), nil }, restate.WithName("deployment-clock"))
}

func (r restateObjectRuntime) ScheduleDeadline(req deploygate.DeploymentDeadlineRequest, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	_ = restate.ObjectSend(r.ctx, r.serviceName(), r.Key(), "DeploymentDeadline").Send(req, restate.WithDelay(delay))
}

// RunStep bridges the deployment gate step journal to the build step runner.
func (r restateObjectRuntime) RunStep(name string, fn func(ctx context.Context) (deploygate.StepOutput, error)) (deploygate.StepOutput, error) {
	return r.RunReconcileStep(name, fn)
}

// gate binds the deployment gate to this object's durable authority.
func (b *BuildObject) gate() *deploygate.Gate {
	return deploygate.NewGate(b.store, b.executor)
}

func toDeploygateRetry(retry *BuildMutationRequest) *deploygate.RetryRequest {
	if retry == nil {
		return nil
	}
	return &deploygate.RetryRequest{ChunkID: retry.ChunkID, Revision: retry.Revision, Generation: retry.Generation}
}

func (b *BuildObject) syncDeploymentDeadlines(rt objectRuntime, state *model.BuildObjectState, ev *pubsub.PRLifecycleEvent, retry *BuildMutationRequest) error {
	dr, ok := rt.(deploygate.Runtime)
	if !ok || state == nil {
		return nil
	}
	return b.gate().SyncDeadlines(dr, state, ev, toDeploygateRetry(retry))
}

// DeploymentDeadline returns promptly: failed delivery is retried by a delayed
// self-send, never by holding the exclusive object across a sleeping invocation.
func (b *BuildObject) DeploymentDeadline(ctx restate.ObjectContext, req deploygate.DeploymentDeadlineRequest) error {
	return b.expireDeploymentDeadline(b.getRuntime(ctx), req)
}

func (b *BuildObject) expireDeploymentDeadline(rt objectRuntime, req deploygate.DeploymentDeadlineRequest) error {
	dr, ok := rt.(deploygate.Runtime)
	if !ok {
		return errors.New("deployment runtime unavailable")
	}
	return b.gate().ExpireDeadline(dr, req)
}

func (b *BuildObject) recordDeploymentStart(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) (bool, string, error) {
	return b.gate().RecordDeploymentStart(rc, buildID, chunkID, ev)
}

func (b *BuildObject) armDeploymentStart(rt objectRuntime, buildID string, ev pubsub.PRLifecycleEvent) error {
	dr, ok := rt.(deploygate.Runtime)
	if !ok {
		return errors.New("deployment runtime unavailable")
	}
	return b.gate().ArmDeploymentStart(dr, buildID, ev)
}

func (b *BuildObject) retryDeployment(rt objectRuntime, req BuildMutationRequest) (ReconcileBuildResult, error) {
	dr, ok := rt.(deploygate.Runtime)
	if !ok {
		return ReconcileBuildResult{}, restate.TerminalErrorf("deployment runtime unavailable")
	}
	outcome, err := b.gate().RetryDeployment(dr, deploygate.RetryRequest{ChunkID: req.ChunkID, Revision: req.Revision, Generation: req.Generation})
	if err != nil {
		return ReconcileBuildResult{}, err
	}
	return ReconcileBuildResult{BuildID: outcome.BuildID, Status: outcome.Status}, nil
}
