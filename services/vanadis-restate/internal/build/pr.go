package build

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prflow"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	prEventOpened        = "opened"
	prEventDeployStarted = "deploy_started"
)

// flow binds PR lifecycle orchestration to this object's durable authority.
func (b *BuildObject) flow() *prflow.Flow {
	return prflow.NewFlow(b.store, b.executor)
}

func CanFallbackThreadRepoLookup(ev pubsub.PRLifecycleEvent) bool {
	return prflow.CanFallbackThreadRepoLookup(ev)
}

// ExtractChunkIDFromBranch delegates to the canonical internal/model helper.
func ExtractChunkIDFromBranch(repo, branch string) string {
	return model.ExtractChunkIDFromBranch(repo, branch)
}

func actionDispatchID(chunk model.VanadisChunk) string {
	return model.ActionDispatchID(chunk)
}

// actionLaunchKey preserves legacy acknowledgement keys outside action retries.
func actionLaunchKey(chunk model.VanadisChunk) string {
	if chunk.Mode == chunkModeAction && chunk.StallNudges > 0 {
		// A colon is forbidden in chunk IDs, keeping generation keys disjoint.
		return "dispatch:" + actionDispatchID(chunk)
	}
	return chunk.ID
}

// errPRTransitionSkipped aliases the store package sentinel so errors.Is identity
// holds wherever the lifecycle transition executes.
var errPRTransitionSkipped = store.ErrPRTransitionSkipped

func dispatchCompletionMutation(ev pubsub.DispatchOutcomeEvent) *BuildMutationRequest {
	switch strings.ToLower(strings.TrimSpace(ev.Status)) {
	case "completed", dispatchStatusSuccess:
		return &BuildMutationRequest{Action: mutationActionCompleted, ChunkID: strings.TrimSpace(ev.VanadisChunkID), DispatchID: ev.DispatchID}
	case statusFailed, statusTimeout, statusCrashed:
		return &BuildMutationRequest{
			Action:     mutationChunkFailed,
			ChunkID:    strings.TrimSpace(ev.VanadisChunkID),
			DispatchID: ev.DispatchID,
			Reason:     strutil.FirstNonEmpty(ev.Error, "dispatch outcome failed"),
		}
	default:
		return nil
	}
}

func isActionCompletionMutation(mutation *BuildMutationRequest, chunks []model.VanadisChunk) bool {
	if mutation == nil {
		return false
	}
	action := strings.ToLower(strings.TrimSpace(mutation.Action))
	if action != mutationActionCompleted && action != mutationChunkDeploySuccess {
		return false
	}
	for _, chunk := range chunks {
		// A verdict completes an action chunk even after a repair PR deployed;
		// a repair PR's deploy_success never completes one.
		if chunk.ID == mutation.ChunkID && chunk.Mode == "action" && (chunk.PRNumber == 0 || action == mutationActionCompleted) {
			return true
		}
	}
	return false
}

func terminalNotificationRecorded(state *model.BuildObjectState) bool {
	return state != nil && state.TerminalNotified
}

func normalizeCompletionMutation(mutation *BuildMutationRequest) *BuildMutationRequest {
	if mutation == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(mutation.Action)) {
	case mutationActionCompleted, mutationChunkDeploySuccess, mutationChunkFailed:
		normalized := *mutation
		normalized.ChunkID = strings.TrimSpace(normalized.ChunkID)
		return &normalized
	default:
		return mutation
	}
}

func actionFailureAccepted(c model.VanadisChunk, buildStatus, dispatchID string) bool {
	if c.Status == statusFailed && (buildStatus == statusRunning || buildStatus == statusFailed) {
		c.Status = statusRunning
		buildStatus = statusRunning
	}
	return store.ActionFailureAllowed(c, buildStatus, dispatchID)
}

const dispatchStatusSuccess = "success"

const mutationActionCompleted = "action_completed"

const mutationChunkDeploySuccess = "chunk_deploy_success"

var errActionCompletionSkipped = errors.New("action completion not applied")

// findDeployingChunk returns the deploying chunk snapshot from a build's
// chunk list.
func findDeployingChunk(chunks []model.VanadisChunk, chunkID string) (model.VanadisChunk, bool) {
	for _, chunk := range chunks {
		if chunk.ID == chunkID {
			return chunk, true
		}
	}
	return model.VanadisChunk{}, false
}

func prDeploymentEligible(build *model.VanadisBuild, deploying model.VanadisChunk) bool {
	if build == nil || build.Status == statusCancelled || (build.Status != statusRunning && build.Status != statusFailed) {
		return false
	}
	if deploying.Status == statusCancelled {
		return false
	}
	if deploying.Status == statusFailed && (deploying.Mode != "pr" || deploying.PRNumber <= 0) {
		return false
	}
	return true
}

func (b *BuildObject) notifyChunkCompleted(ctx context.Context, build *model.VanadisBuild, chunkID string) {
	if b.executor == nil {
		return
	}
	if cErr := b.executor.CancelChunkDispatch(withBuildOwner(ctx, build), chunkID); cErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
	}
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{chunkID}, ""); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
	}
}

func (b *BuildObject) completePRDeploymentMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	deploying, found := findDeployingChunk(chunks, mutation.ChunkID)
	if !found {
		return ErrChunkNotFound
	}
	alreadyCompleted := deploying.Status == statusCompleted
	if deploying.Mode == "action" && deploying.PRNumber == 0 {
		return b.completeActionDeploymentMutation(ctx, mutation)
	}
	if !prDeploymentEligible(build, deploying) {
		return nil
	}
	if deploying.Mode == chunkModeAction {
		return b.markActionRepairDeployed(ctx, deploying, chunks, mutation)
	}
	if err := b.store.CompletePRDeployment(ctx, mutation.ChunkID, mutation.RolloutState); err != nil {
		if errors.Is(err, errPRTransitionSkipped) {
			return nil
		}
		return err
	}
	if mutation.RolloutState != nil {
		deploying.RolloutState = mutation.RolloutState
	}
	if !alreadyCompleted {
		b.notifyChunkCompleted(ctx, build, mutation.ChunkID)
	}
	if err := b.completeSweptDeployments(ctx, chunks, mutation.ChunkID); err != nil {
		return err
	}
	return b.completeCrossBuildSweptDeployments(ctx, deploying)
}

// markActionRepairDeployed handles deploy_success for a repair PR attached to
// an action chunk. The repair is a step inside the chunk: the chunk moves back
// to running and completes only on its complete_chunk verdict, so no
// completed event is published and its dispatch is never cancelled. The
// landed commit still supersedes earlier merged chunks, so both sweeps run.
func (b *BuildObject) markActionRepairDeployed(ctx context.Context, deploying model.VanadisChunk, chunks []model.VanadisChunk, mutation *BuildMutationRequest) error {
	if err := b.store.MarkActionRepairDeployed(ctx, mutation.ChunkID, mutation.RolloutState); err != nil {
		if errors.Is(err, errPRTransitionSkipped) {
			return nil
		}
		return err
	}
	if mutation.RolloutState != nil {
		deploying.RolloutState = mutation.RolloutState
	}
	if err := b.completeSweptDeployments(ctx, chunks, mutation.ChunkID); err != nil {
		return err
	}
	return b.completeCrossBuildSweptDeployments(ctx, deploying)
}

// completeCrossBuildSweptDeployments completes eligible earlier chunks in
// other builds superseded by the deploying chunk's landed commit, including
// already-completed chunks re-derived for retry recovery. Skewed or
// concurrent transitions surface as skipped writes inside the store sweep;
// other errors return after processing partial progress so the journaled
// step retries the remainder. It publishes a completed event per swept
// chunk, then queues one Reconcile per affected build for that build's own
// runner: this object never evaluates another build's terminal status or
// widget with its own lock, token or eri session.
func (b *BuildObject) completeCrossBuildSweptDeployments(ctx context.Context, deploying model.VanadisChunk) error {
	swept, recovered, err := b.store.SweepEarlierDeploymentsAcrossBuilds(ctx, deploying)
	for _, sw := range append(swept, recovered...) {
		if b.executor != nil {
			if pubErr := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{sw.ChunkID}, ""); pubErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pubErr)
			}
		}
		recordCrossBuildReconcile(ctx, store.BuildRoute{BuildID: sw.BuildID, RunnerAgent: sw.RunnerAgent})
	}
	return err
}

// completeSweptDeployments completes earlier PR chunks superseded by the
// deploying chunk's landed commit, publishing a completed event per chunk.
// Superseded action chunks only have their repair marked deployed.
// Skipped transitions (a concurrent completion, failure, or cancellation)
// are harmless; other errors abort so the journaled step retries the sweep.
func (b *BuildObject) completeSweptDeployments(ctx context.Context, chunks []model.VanadisChunk, chunkID string) error {
	for _, sweptID := range store.EarlierDeploymentSweepIDs(b.executor.GitHubOwner(), chunks, chunkID) {
		if idx := findChunkIndex(chunks, sweptID); idx >= 0 && chunks[idx].Mode == chunkModeAction {
			// A swept action chunk's repair deployed; it stays running.
			if err := b.store.MarkActionRepairDeployed(ctx, sweptID, nil); err != nil && !errors.Is(err, errPRTransitionSkipped) {
				return err
			}
			continue
		}
		if err := b.store.CompletePRDeployment(ctx, sweptID, nil); err != nil {
			if errors.Is(err, errPRTransitionSkipped) {
				continue
			}
			return err
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{sweptID}, ""); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
			}
		}
	}
	return nil
}

func (b *BuildObject) validateActionBuild(ctx context.Context, buildID, chunkID string) error {
	_, err := b.buildChunk(ctx, buildID, chunkID)
	return err
}

// buildChunk returns the chunk snapshot when it belongs to the build.
func (b *BuildObject) buildChunk(ctx context.Context, buildID, chunkID string) (model.VanadisChunk, error) {
	_, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return model.VanadisChunk{}, err
	}
	for _, c := range chunks {
		if c.ID == chunkID && c.BuildID == buildID {
			return c, nil
		}
	}
	return model.VanadisChunk{}, ErrChunkNotFound
}

func (b *BuildObject) completeActionDeploymentMutation(ctx context.Context, mutation *BuildMutationRequest) error {
	res, err := b.store.CompleteActionChunk(ctx, mutation.ChunkID, mutation.DispatchID)
	if err != nil {
		return err
	}
	if res == nil || !res.CompletionAccepted {
		return errActionCompletionSkipped
	}
	if b.executor != nil && res.Advanced {
		if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{mutation.ChunkID}, ""); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
		}
	}
	return nil
}

func (b *BuildObject) failMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	chunkID := strings.TrimSpace(mutation.ChunkID)
	if err := b.validateActionBuild(ctx, buildID, chunkID); err != nil {
		return err
	}
	bld, chk, failed, err := b.store.FailChunk(ctx, chunkID, mutation.Reason, mutation.DispatchID)
	if err != nil {
		return err
	}
	if !failed {
		if bld != nil && chk != nil && chk.Status == statusFailed && (bld.Status == statusRunning || bld.Status == statusFailed) && (chk.Mode != chunkModeAction || actionFailureAccepted(*chk, bld.Status, mutation.DispatchID)) {
			return nil
		}
		return errActionCompletionSkipped
	}
	return nil
}

func (b *BuildObject) handlePREventType(ctx context.Context, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	return b.flow().HandlePREventType(ctx, buildID, chunkID, event, ev)
}

func (b *BuildObject) locateOrAttachPRChunk(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	return b.flow().LocateOrAttachPRChunk(ctx, buildID, ev)
}

// dispatchPREvent applies one PR event and returns the other builds a
// deploy_success sweep changed, for reconciliation by their own runners.
func (b *BuildObject) dispatchPREvent(ctx context.Context, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) ([]store.BuildRoute, error) {
	if event == "deploy_success" {
		cctx, crossBuild := withCrossBuildReconciles(ctx)
		err := b.applyMutation(cctx, buildID, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: chunkID, RolloutState: ev.RolloutState})
		return crossBuild.list(), err
	}
	return nil, b.handlePREventType(ctx, buildID, chunkID, event, ev)
}

func retryableGitHubDelay(err error, exec *executor.Executor, now time.Time) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	var se *githubapi.StatusError
	isStatusErr := errors.As(err, &se)
	delay, isRL := githubapi.RateLimitDelay(err)
	var execRL time.Duration
	if exec != nil {
		execRL = exec.RateLimitDelay(now)
	}

	switch {
	case isRL || execRL > 0:
	case isStatusErr && se.StatusCode >= 500:
	case !isStatusErr && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "github api request failed")):
	default:
		return 0, false
	}

	effectiveDelay := delay
	if effectiveDelay <= 0 {
		effectiveDelay = execRL
	}
	if effectiveDelay <= 0 {
		effectiveDelay = 15 * time.Second
	}
	return effectiveDelay, true
}

func (b *BuildObject) recordDeploymentPREvent(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (prEventOutcome, error) {
	eventCtx := context.Context(rc)
	if b.store != nil {
		if bld, _, gErr := b.store.GetBuild(rc, buildID); gErr == nil && bld != nil && strings.TrimSpace(bld.Agent) != "" {
			eventCtx = ericlient.WithAgent(rc, bld.Agent)
		}
	}
	chunkID, cErr := b.locateOrAttachPRChunk(eventCtx, buildID, ev)
	if cErr != nil {
		if delay, retryable := retryableGitHubDelay(cErr, b.executor, time.Now()); retryable {
			slog.WarnContext(rc, "[vanadis-restate] retryable github error locating chunk; deferring event", "build_id", buildID, "error", cErr, "retry_after", delay)
			return prEventOutcome{RetryAfter: delay}, nil
		}
		return prEventOutcome{}, cErr
	}
	if chunkID == "" {
		return prEventOutcome{}, nil
	}
	rolloutStatus := ""
	event := strings.ToLower(strings.TrimSpace(ev.Event))
	if event == prEventDeployStarted {
		valid, status, e := b.recordDeploymentStart(rc, buildID, chunkID, ev)
		rolloutStatus = status
		if e != nil || !valid {
			return prEventOutcome{}, e
		}
	}

	reconcileTargets, eventErr := b.dispatchPREvent(eventCtx, buildID, chunkID, event, ev)
	if eventErr != nil {
		if errors.Is(eventErr, errPRTransitionSkipped) {
			return prEventOutcome{}, nil
		}
		if delay, retryable := retryableGitHubDelay(eventErr, b.executor, time.Now()); retryable {
			slog.WarnContext(rc, "[vanadis-restate] retryable github error dispatching event; deferring event", "build_id", buildID, "error", eventErr, "retry_after", delay)
			return prEventOutcome{RetryAfter: delay}, nil
		}
		return prEventOutcome{}, eventErr
	}
	b.refreshFailedPRWidget(rc, buildID)
	if event != "deploy_success" && ev.RolloutState != nil {
		if rErr := b.store.RecordPRRolloutState(rc, chunkID, rolloutStatus, ev.RolloutState); rErr != nil {
			if event == prEventDeployStarted {
				return prEventOutcome{}, rErr
			}
			slog.WarnContext(rc, "[vanadis-restate] record rollout state warning", "error", rErr)
		}
		b.refreshFailedPRWidget(rc, buildID)
	}
	return prEventOutcome{Applied: true, ReconcileTargets: reconcileTargets}, nil
}

// prEventOutcome is the journaled result of applying one PR lifecycle event.
type prEventOutcome struct {
	Applied          bool               `json:"applied"`
	ReconcileTargets []store.BuildRoute `json:"reconcile_targets,omitempty"`
	RetryAfter       time.Duration      `json:"retry_after,omitempty"`
}

// OnPREvent receives PR lifecycle events for this build.
func (b *BuildObject) OnPREvent(ctx restate.ObjectContext, ev pubsub.PRLifecycleEvent) error {
	if b.store == nil {
		return nil
	}
	if err := b.guardRunner(ctx, labelOnPREvent); err != nil {
		return err
	}
	buildID := restate.Key(ctx)
	outcome, err := RunJournaled(ctx, labelOnPREvent, "handle-pr-event-postgres", func(rc restate.RunContext) (prEventOutcome, error) {
		return b.recordDeploymentPREvent(rc, buildID, ev)
	})
	if err != nil {
		return err
	}
	if outcome.RetryAfter > 0 {
		restate.ObjectSend(ctx, b.ServiceName(), buildID, labelOnPREvent).Send(ev, restate.WithDelay(outcome.RetryAfter))
		return nil
	}
	rt := b.getRuntime(ctx)
	sendCrossBuildReconciles(rt, outcome.ReconcileTargets)
	if strings.EqualFold(strings.TrimSpace(ev.Event), prEventDeployStarted) {
		if err := b.armDeploymentStart(rt, buildID, ev); err != nil {
			return err
		}
	}

	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterPREvent, nil); tErr != nil {
		return ToTerminalErr(tErr)
	}
	return nil
}

// OnDispatchOutcome receives task execution outcomes.
func (b *BuildObject) OnDispatchOutcome(ctx restate.ObjectContext, ev pubsub.DispatchOutcomeEvent) error {
	if b.store == nil {
		return nil
	}
	if err := b.guardRunner(ctx, labelOnDispatchOutcome); err != nil {
		return err
	}
	applied, err := RunJournaled(ctx, labelOnDispatchOutcome, "handle-dispatch-outcome-postgres", func(rc restate.RunContext) (bool, error) {
		return b.applyDispatchOutcome(rc, restate.Key(ctx), ev)
	})
	if err != nil {
		return err
	}
	if !applied {
		if b.executor != nil && isVerdictlessDispatchStatus(ev.Status) {
			return b.handleNoPRTurnOutcome(ctx, ev)
		}
		return nil
	}
	rt := b.getRuntime(ctx)
	mutation := dispatchCompletionMutation(ev)
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterDispatch, mutation); tErr != nil {
		return ToTerminalErr(tErr)
	}
	return nil
}

// applyDispatchOutcome applies a dispatch outcome inside the journaled
// handle-dispatch-outcome-postgres step and reports whether it changed the
// chunk. Completed and timeout outcomes never complete an action chunk: only
// the CompleteChunk verdict does.
func (b *BuildObject) applyDispatchOutcome(rc context.Context, buildID string, ev pubsub.DispatchOutcomeEvent) (bool, error) {
	chunkID := strings.TrimSpace(ev.VanadisChunkID)
	if chunkID == "" {
		return false, nil
	}
	status := strings.ToLower(strings.TrimSpace(ev.Status))
	switch status {
	case "completed", dispatchStatusSuccess:
		chunk, err := b.buildChunk(rc, buildID, chunkID)
		if err != nil {
			return false, err
		}
		// Action chunks complete only through the CompleteChunk verdict.
		if chunk.Mode == chunkModeAction {
			return false, nil
		}
		res, err := b.store.CompleteActionChunk(rc, chunkID, ev.DispatchID)
		if err != nil {
			return false, err
		}
		return res != nil && res.CompletionAccepted, nil
	case statusTimeout:
		chunk, err := b.buildChunk(rc, buildID, chunkID)
		if err != nil {
			return false, err
		}
		// A timed-out action turn is a verdictless turn: resume, never fail.
		if chunk.Mode == chunkModeAction {
			return false, nil
		}
		_, _, failed, err := b.store.FailChunk(rc, chunkID, strutil.FirstNonEmpty(ev.Error, "dispatch outcome failed"), ev.DispatchID)
		return failed, err
	case statusFailed, statusCrashed:
		reason := strutil.FirstNonEmpty(ev.Error, "dispatch outcome failed")
		if err := b.validateActionBuild(rc, buildID, chunkID); err != nil {
			return false, err
		}
		_, _, failed, err := b.store.FailChunk(rc, chunkID, reason, ev.DispatchID)
		return failed, err
	}
	return true, nil
}

// noPRAction aliases the prflow recovery decision so existing callers keep
// compiling against the thin build surface.
type noPRAction = prflow.NoPRAction

const (
	noPRActionNone       = prflow.NoPRActionNone
	noPRActionResume     = prflow.NoPRActionResume
	noPRActionFail       = prflow.NoPRActionFail
	noPRActionFailAction = prflow.NoPRActionFailAction
)

const noPRFailureReason = prflow.NoPRFailureReason

func isCompletedDispatchStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusCompleted, dispatchStatusSuccess:
		return true
	}
	return false
}

// isVerdictlessDispatchStatus reports outcomes that end a turn without a
// verdict: completed turns and turns eri stopped at its time limit.
func isVerdictlessDispatchStatus(status string) bool {
	return isCompletedDispatchStatus(status) || strings.EqualFold(strings.TrimSpace(status), statusTimeout)
}

// noPRTurnAction decides how to react when a PR chunk's turn completes. The
// submit script attaches PRs synchronously, so a completed launch or resume
// turn on a still-running chunk without a PR means the turn ended without one.
func noPRTurnAction(chunk model.VanadisChunk, buildStatus, dispatchID string) prflow.NoPRAction {
	return prflow.NoPRTurnAction(chunk, buildStatus, dispatchID)
}

func findChunkIndex(chunks []model.VanadisChunk, chunkID string) int {
	return prflow.FindChunkIndex(chunks, chunkID)
}

func (b *BuildObject) resumeNoPRChunk(ctx context.Context, bld *model.VanadisBuild, chunks []model.VanadisChunk, idx int) error {
	return b.flow().ResumeNoPRChunk(ctx, bld, chunks, idx)
}

// resumeVerdictlessChunk dispatches the next automatic resume: the counted
// action resume for action chunks, the single no-PR resume for PR chunks.
func (b *BuildObject) resumeVerdictlessChunk(ctx context.Context, bld *model.VanadisBuild, chunks []model.VanadisChunk, idx int, dispatchID string) error {
	if chunks[idx].Mode != chunkModeAction {
		return b.resumeNoPRChunk(ctx, bld, chunks, idx)
	}
	// noPRTurnAction only resumes current-generation IDs below the cap.
	k, _ := prflow.NextActionResume(chunks[idx], dispatchID)
	return b.flow().ResumeActionChunk(ctx, bld, chunks, idx, k)
}

// handleNoPRTurnOutcome resumes a PR chunk whose launch turn ended without a PR
// once, then fails it loudly when the resume turn also ends without one.
func (b *BuildObject) handleNoPRTurnOutcome(ctx restate.ObjectContext, ev pubsub.DispatchOutcomeEvent) error {
	buildID := restate.Key(ctx)
	chunkID := strings.TrimSpace(ev.VanadisChunkID)
	action, err := RunJournaled(ctx, labelOnDispatchOutcome, "handle-nopr-turn-outcome", func(rc restate.RunContext) (noPRAction, error) {
		bld, chunks, err := b.store.GetBuild(rc, buildID)
		if err != nil {
			return noPRActionNone, err
		}
		idx := findChunkIndex(chunks, chunkID)
		if bld == nil || idx < 0 {
			return noPRActionNone, nil
		}
		chunk := chunks[idx]
		// Only action turns treat a time-limit stop as verdictless; PR chunks keep
		// their failed-outcome handling.
		if chunk.Mode != chunkModeAction && !isCompletedDispatchStatus(ev.Status) {
			return noPRActionNone, nil
		}
		action := noPRTurnAction(chunk, bld.Status, ev.DispatchID)
		if action == noPRActionResume {
			if err := b.resumeVerdictlessChunk(rc, bld, chunks, idx, ev.DispatchID); err != nil {
				return noPRActionNone, err
			}
		}
		return action, nil
	})
	if err != nil {
		return err
	}
	reason := noPRFailureReason
	switch action {
	case noPRActionFail:
	case noPRActionFailAction:
		reason = prflow.ActionResumeFailureReason
	default:
		return nil
	}
	mutation := &BuildMutationRequest{Action: mutationChunkFailed, ChunkID: chunkID, DispatchID: ev.DispatchID, Reason: reason}
	if _, tErr := b.executeObjectTransition(b.getRuntime(ctx), labelReconcileAfterNoPRFail, mutation); tErr != nil {
		return ToTerminalErr(tErr)
	}
	return nil
}

// Failed parents return from reconciliation before its normal widget sync.
func (b *BuildObject) refreshFailedPRWidget(ctx context.Context, buildID string) {
	if b.executor == nil {
		return
	}
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] recovery widget read warning", "error", err)
		return
	}
	if build == nil || build.Status != statusFailed {
		return
	}
	if _, err = b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build failed"); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] recovery widget sync warning", "error", err)
	}
}
