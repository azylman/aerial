// Package deploygate owns the 15-minute PR deployment deadline state machine:
// pending deployment start tracking and retry_deployment generation fencing.
// It has no Restate or BuildObject dependency; the Restate state glue stays in
// internal/build, which delegates to Gate. Wire values (state keys, journal
// step names, JSON tags, event names) stay byte-identical with the previous
// internal/build copies.
package deploygate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Frozen deployment timing and Restate state keys. Values stay byte-identical
// with the previous internal/build copies.
const (
	MaxDeployTime         = 15 * time.Minute
	DeploymentDeadlineKey = "deployment_deadlines"
	// DeployStartedEvent is the PR lifecycle event that arms deployment tracking.
	DeployStartedEvent = "deploy_started"
)

// Status literals mirrored from internal/build/requests.go. They stay
// package-private so the wire values live in exactly one place per package;
// behavior must match internal/build/requests.go.
const (
	statusRunning      = "running"
	statusMerged       = "merged"
	statusSynced       = "synced"
	statusDeploying    = "deploying"
	statusDeployFailed = "deploy_failed"
	statusCompleted    = "completed"
	statusFailed       = "failed"
	statusCancelled    = "cancelled"
	statusDeployed     = "deployed"
)

// Attempt is one immutable deployment attempt for a chunk revision.
type Attempt struct {
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

// RetryRequest identifies the deployment retry under validation.
type RetryRequest struct {
	ChunkID    string
	Revision   string
	Generation int
}

// StepOutput mirrors the build reconcile step journal payload. Field order,
// types, and JSON tags must stay identical so both packages convert directly.
type StepOutput struct {
	RetryResult    *store.RetryActionChunkResult `json:"retry_result,omitempty"`
	BuildStatus    string                        `json:"build_status"`
	LaunchedChunks []string                      `json:"launched_chunks,omitempty"`
	LaunchedMap    map[string]bool               `json:"launched_map,omitempty"`
	Error          string                        `json:"error,omitempty"`
	State          *model.BuildObjectState       `json:"state,omitempty"`
}

// Runtime is the Restate deployment state surface Gate operates on.
type Runtime interface {
	Key() string
	GetAttempts() (map[string]Attempt, error)
	SetAttempts(attempts map[string]Attempt)
	Now() (time.Time, error)
	GetPendingStarts() (map[string]PendingStart, error)
	SetPendingStarts(pending map[string]PendingStart)
	ScheduleDeadline(req DeploymentDeadlineRequest, delay time.Duration)
	ScheduleReconcile()
	RunStep(name string, fn func(ctx context.Context) (StepOutput, error)) (StepOutput, error)
}

// Store is the durable authority subset Gate operates on.
type Store interface {
	GetBuild(ctx context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error)
	UpdatePRLifecycleStatus(ctx context.Context, chunkID, status string) error
	UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error
	RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error
}

// Gate executes deployment deadline transitions against Store and Executor.
type Gate struct {
	store Store
	exec  *executor.Executor
}

// NewGate builds a Gate bound to the given store and side-effect executor.
func NewGate(st Store, exec *executor.Executor) *Gate {
	return &Gate{store: st, exec: exec}
}

func revision(c model.VanadisChunk) string {
	return store.FirstNonEmpty(strings.TrimSpace(c.MergeSHA), strings.TrimSpace(c.HeadSHA))
}

func nextAttempt(old Attempt, c model.VanadisChunk, start time.Time, retry bool) Attempt {
	if old.Generation > 0 && old.PRNumber == c.PRNumber && old.Revision == revision(c) && !retry {
		return old
	}
	return Attempt{ChunkID: c.ID, PRNumber: c.PRNumber, Revision: revision(c), Generation: old.Generation + 1, Deadline: start.Add(MaxDeployTime)}
}

func eligible(c model.VanadisChunk) bool {
	return c.Status == statusDeploying && c.PRNumber != 0 && revision(c) != ""
}

func buildActive(status string) bool { return status == "" || status == statusRunning }

func startAt(now time.Time, ev *pubsub.PRLifecycleEvent, c model.VanadisChunk) time.Time {
	if ev != nil && ev.Repo == c.Repo && ev.PRNumber == c.PRNumber && store.FirstNonEmpty(ev.MergeSHA, ev.HeadSHA) == revision(c) && !ev.OccurredAt.IsZero() && ev.OccurredAt.Before(now) {
		return ev.OccurredAt
	}
	return now
}

// DeliveryFailure journals delivery failures as values so retries release the
// object lock.
func deliveryFailure(err error) (StepOutput, error) {
	return StepOutput{Error: err.Error()}, nil
}

// ValidateRetry rejects stale retry_deployment fencing attempts.
func ValidateRetry(state *model.BuildObjectState, attempts map[string]Attempt, retry *RetryRequest) error {
	if retry != nil {
		valid := false
		for _, c := range state.Chunks {
			if c.ID == retry.ChunkID && eligible(c) && retry.Revision == revision(c) && retry.Generation == attempts[c.ID].Generation {
				valid = true
			}
		}
		if !valid {
			return restate.ToTerminalError(errors.New("deployment retry attempt no longer current"), restate.WithErrorCode(http.StatusConflict))
		}
	}
	return nil
}

// SyncDeadlines adopts pre-upgrade deployments once and arms per-revision
// deadlines. Chunk.StartedAt is the coding start, not deployment start; absent
// a deployment event the adoption time is used rather than pretending that
// field is a historical deployment timestamp.
func (g *Gate) SyncDeadlines(rt Runtime, state *model.BuildObjectState, ev *pubsub.PRLifecycleEvent, retry *RetryRequest) error {
	if rt == nil || state == nil {
		return nil
	}
	starts, err := g.SyncPendingStarts(rt, state, ev)
	if err != nil {
		return err
	}
	if !buildActive(state.Build.Status) {
		return nil
	}
	hasEligible := false
	for _, c := range state.Chunks {
		if eligible(c) {
			hasEligible = true
		}
	}
	if !hasEligible && retry == nil {
		return nil
	}
	attempts, err := rt.GetAttempts()
	if err != nil {
		return err
	}
	if attempts == nil {
		attempts = map[string]Attempt{}
	}
	if err = ValidateRetry(state, attempts, retry); err != nil {
		return err
	}

	now, err := rt.Now()
	if err != nil {
		return err
	}
	for _, c := range state.Chunks {
		if retry != nil && retry.ChunkID != c.ID {
			continue
		}
		if !eligible(c) {
			continue
		}
		old := attempts[c.ID]
		explicit := retry != nil && retry.ChunkID == c.ID
		start := startAt(now, ev, c)
		if original, ok := starts[c.ID]; ok {
			start = original
		}
		a := nextAttempt(old, c, start, explicit)
		if a == old {
			continue
		}
		attempts[c.ID] = a
		rt.SetAttempts(attempts)
		rt.ScheduleDeadline(DeploymentDeadlineRequest{c.ID, a.Revision, a.Generation}, a.Deadline.Sub(now))
	}
	return nil
}

// ExpireDeadline settles one deployment deadline timer. It returns promptly:
// failed delivery is retried by a delayed self-send, never by holding the
// exclusive object across a sleeping invocation.
func (g *Gate) ExpireDeadline(rt Runtime, req DeploymentDeadlineRequest) error {
	if rt == nil {
		return errors.New("deployment runtime unavailable")
	}
	attempts, err := rt.GetAttempts()
	if err != nil {
		return err
	}
	a, ok := attempts[req.ChunkID]
	// In particular, never recreate state after terminal ClearAll.
	if !ok || a.Delivered || a.Revision != req.Revision || a.Generation != req.Generation {
		return nil
	}
	now, err := rt.Now()
	if err != nil {
		return err
	}
	if now.Before(a.Deadline) {
		rt.ScheduleDeadline(req, a.Deadline.Sub(now))
		return nil
	}
	out, err := rt.RunStep("deployment-deadline-delivery", func(ctx context.Context) (StepOutput, error) {
		return g.DeliverDeadline(ctx, rt.Key(), a)
	})
	if err != nil {
		return err
	}
	if out.Error != "" {
		rt.ScheduleDeadline(req, time.Minute)
		return nil
	}
	rt.ScheduleReconcile()
	a.Delivered = true
	attempts[req.ChunkID] = a
	rt.SetAttempts(attempts)
	return nil
}

// RecordDeploymentStart observes a deploy_started event against durable state.
// Starts are progress observations, not explicit retries: failure is preserved
// until retry_deployment resets authority, while updated evidence is accepted.
func (g *Gate) RecordDeploymentStart(ctx context.Context, buildID, chunkID string, ev pubsub.PRLifecycleEvent) (bool, string, error) {
	build, chunks, err := g.store.GetBuild(ctx, buildID)
	if err != nil {
		return false, "", err
	}
	if build == nil || build.Status != statusRunning {
		return false, "", nil
	}
	for _, c := range chunks {
		if c.ID != chunkID || revision(c) != store.FirstNonEmpty(ev.MergeSHA, ev.HeadSHA) {
			continue
		}
		if c.Status == statusDeployFailed {
			return true, "", nil
		}
		if c.Status != statusMerged && c.Status != statusDeploying {
			return false, "", nil
		}
		if ev.RolloutState == nil {
			if err = g.store.UpdatePRLifecycleStatus(ctx, chunkID, statusDeploying); err != nil {
				return false, "", err
			}
		}
		return true, statusDeploying, nil
	}
	return false, "", nil
}

// ArmDeploymentStart snapshots durable state and arms deadlines for a
// deploy_started event.
func (g *Gate) ArmDeploymentStart(rt Runtime, buildID string, ev pubsub.PRLifecycleEvent) error {
	snapshot, err := rt.RunStep("deployment-start-snapshot", func(ctx context.Context) (StepOutput, error) {
		build, chunks, err := g.store.GetBuild(ctx, buildID)
		if err != nil {
			return StepOutput{}, err
		}
		if build == nil || build.Status != statusRunning {
			return StepOutput{}, nil
		}
		return StepOutput{State: &model.BuildObjectState{Build: *build, Chunks: chunks}}, nil
	})
	if err != nil {
		return err
	}
	if err = g.SyncDeadlines(rt, snapshot.State, &ev, nil); err != nil {
		return err
	}

	return nil
}

// DeliverDeadline marks a chunk deploy_failed after its deadline lapses and
// wakes the build thread. A repeated merge notification can restore merged
// after deployment began; the matching durable attempt still proves the
// original deployment budget.
func (g *Gate) DeliverDeadline(ctx context.Context, buildID string, a Attempt) (StepOutput, error) {
	result := StepOutput{}
	if g.store == nil {
		result.Error = "deployment store unavailable"
		return result, nil
	}
	build, chunks, e := g.store.GetBuild(ctx, buildID)
	if e != nil {
		return deliveryFailure(e)
	}
	if build == nil || build.Status != statusRunning {
		return result, nil
	}
	for _, c := range chunks {
		if c.ID != a.ChunkID {
			continue
		}
		if c.PRNumber != a.PRNumber || revision(c) != a.Revision || (c.Status != statusDeploying && c.Status != statusDeployFailed && c.Status != statusMerged) {
			return result, nil
		}
		evidence, marshalErr := json.Marshal(c.RolloutState)
		if marshalErr != nil {
			return deliveryFailure(marshalErr)
		}
		details := "Readiness remains unconfirmed after the 15-minute deployment deadline. Last persisted job/Kustomization evidence (not a fresh Kubernetes observation): " + string(evidence)
		ev := pubsub.PRLifecycleEvent{Event: "deploy_failed", Repo: c.Repo, PRNumber: c.PRNumber, MergeSHA: a.Revision, Details: details}
		if e = g.store.UpdateChunkBannerAndStatus(ctx, c.ID, statusDeployFailed, executor.FormatDeployFailureBanner(ev)); e != nil {
			return deliveryFailure(e)
		}
		if g.exec == nil || g.exec.EriClient == nil {
			result.Error = "deadline wake executor unavailable"
			return result, nil
		}
		// Strict durable acceptance reuses one identity across ambiguous HTTP errors.
		// Eri's dedup retention bounds that guarantee; delivery after retention may
		// duplicate a wake if an earlier acceptance response was lost.
		id := fmt.Sprintf("vanadis-deploy-deadline-%s-%d-%s-%d", c.ID, a.PRNumber, a.Revision, a.Generation)
		e = g.exec.EriClient.DispatchAccepted(ctx, executor.BrainDispatchRequest{ID: id, Source: "discord", SessionID: "vanadis-deploy-deadline-" + c.ID, TargetID: build.ThreadID, Prompt: executor.FormatDeployFailurePrompt(ev)})
		if e != nil {
			result.Error = e.Error()
		}
		return result, nil
	}
	return result, nil
}
