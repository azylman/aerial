package deploygate

import (
	"context"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// RetryOutcome is the result of a successful retry_deployment transition.
type RetryOutcome struct {
	BuildID string
	Status  string
}

// RetryDeployment resets a deploying or deploy_failed chunk and rearms its
// deadline under explicit retry fencing.
func (g *Gate) RetryDeployment(rt Runtime, req RetryRequest) (RetryOutcome, error) {
	if g.store == nil {
		return RetryOutcome{}, restate.TerminalErrorf("deployment retry requires store")
	}
	out, err := rt.RunStep("deployment-retry-snapshot", func(ctx context.Context) (StepOutput, error) {
		build, chunks, err := g.store.GetBuild(ctx, rt.Key())
		if err != nil {
			return StepOutput{}, err
		}
		if build == nil || build.Status != statusRunning {
			return StepOutput{}, restate.TerminalErrorf("deployment build not running")
		}
		found := false
		for i := range chunks {
			if chunks[i].ID == req.ChunkID && (chunks[i].Status == statusDeploying || chunks[i].Status == statusDeployFailed) {
				chunks[i].Status = statusDeploying
				found = true
			}
		}
		if !found {
			return StepOutput{}, restate.TerminalErrorf("deployment chunk not retryable")
		}
		return StepOutput{State: &model.BuildObjectState{Build: *build, Chunks: chunks}}, nil
	})
	if err != nil {
		return RetryOutcome{}, err
	}
	attempts, err := rt.GetAttempts()
	if err != nil {
		return RetryOutcome{}, err
	}
	if err = ValidateRetry(out.State, attempts, &req); err != nil {
		return RetryOutcome{}, err
	}
	if _, err = rt.RunStep("deployment-retry-reset", func(ctx context.Context) (StepOutput, error) {
		resetErr := g.store.UpdateChunkBannerAndStatus(ctx, req.ChunkID, statusDeploying, "")
		return StepOutput{}, resetErr
	}); err != nil {
		return RetryOutcome{}, err
	}
	if err = g.SyncDeadlines(rt, out.State, nil, &req); err != nil {
		return RetryOutcome{}, err
	}
	rt.ScheduleReconcile()
	return RetryOutcome{BuildID: rt.Key(), Status: statusRunning}, nil
}
