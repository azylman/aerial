package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/dag"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (ReconcileBuildResult, error) {
	if b == nil || b.store == nil {
		err := restate.TerminalErrorf("store not configured")
		RecordReconcileError(labelReconcileBuild, err)
		return ReconcileBuildResult{}, err
	}
	if err := b.guardRunner(ctx, labelReconcileBuild); err != nil {
		return ReconcileBuildResult{}, err
	}
	rt := b.getRuntime(ctx)
	res, err := b.executeObjectTransitionTick(rt, labelReconcileBuild, nil, req.DeferralSeq)
	if err != nil || strings.TrimSpace(req.Reason) != ReconcileReasonWidgetSync || res.Status != statusFailed || b.executor == nil {
		return res, err
	}
	// Failed builds return from reconciliation before its widget sync, so an
	// explicit widget sync refreshes the failed card here.
	buildID := strings.TrimSpace(rt.Key())
	if _, wErr := RunJournaled(ctx, labelReconcileBuild, "widget-sync-failed-build", func(rc restate.RunContext) (bool, error) {
		b.refreshFailedPRWidget(rc, buildID)
		return true, nil
	}); wErr != nil {
		return res, wErr
	}
	return res, nil
}

// Mutate applies a mutation to the build and reconciles state.
func (b *BuildObject) Mutate(ctx restate.ObjectContext, req BuildMutationRequest) (ReconcileBuildResult, error) {
	if b == nil || b.store == nil {
		err := restate.TerminalErrorf("store not configured")
		RecordReconcileError(labelMutateBuild, err)
		return ReconcileBuildResult{}, err
	}
	if err := b.guardRunner(ctx, labelMutateBuild); err != nil {
		return ReconcileBuildResult{}, err
	}
	rt := b.getRuntime(ctx)
	if req.Action == "retry_deployment" {
		return b.retryDeployment(rt, req)
	}
	return b.executeObjectTransition(rt, labelMutateBuild, &req)
}

func mergePriorLaunched(priorState *model.BuildObjectState, priorLaunched map[string]bool) map[string]bool {
	if priorState == nil || len(priorState.Launched) == 0 {
		return priorLaunched
	}
	if priorLaunched == nil {
		priorLaunched = make(map[string]bool, len(priorState.Launched))
	}
	for k, v := range priorState.Launched {
		if v {
			priorLaunched[k] = true
		}
	}
	return priorLaunched
}

func (b *BuildObject) runTransitionStep(rt objectRuntime, stepName, buildID string, mutation *BuildMutationRequest, priorState *model.BuildObjectState, priorLaunched map[string]bool, attempt transitionAttempt) (reconcileStepOutput, error) {
	if b == nil || b.store == nil {
		err := restate.TerminalErrorf("store not configured")
		RecordReconcileError(stepName, err)
		return reconcileStepOutput{}, err
	}
	notifiedInStep := false
	stepOut, err := rt.RunReconcileStep(stepName, func(stepCtx context.Context) (reconcileStepOutput, error) {
		ctx, crossBuild := withCrossBuildReconciles(withTransitionAttempt(stepCtx, attempt))
		out, stepErr := b.executeAuthoritativeTransition(ctx, buildID, mutation, priorState, priorLaunched, terminalNotificationRecorded(priorState))
		if stepErr == nil {
			out.ReconcileTargets = crossBuild.list()
		}
		if stepErr != nil {
			notifiedInStep = true
			RecordReconcileError(stepName, stepErr)
			if nerr := b.notifyTransitionError(ctx, buildID, stepName, priorState, stepErr); nerr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] transition notify warning", "error", nerr)
			}
		}
		return out, stepErr
	})
	if err != nil && !notifiedInStep {
		RecordReconcileError(stepName, err)
		notifyErr := err
		if _, notifyStepErr := rt.RunReconcileStep("notify-transition-error", func(nctx context.Context) (reconcileStepOutput, error) {
			return reconcileStepOutput{}, b.notifyTransitionError(nctx, buildID, stepName, priorState, notifyErr)
		}); notifyStepErr != nil {
			slog.WarnContext(context.Background(), "[vanadis-restate] journaled notify warning", "error", notifyStepErr)
		}
	}
	return stepOut, err
}

func (b *BuildObject) executeObjectTransition(rt objectRuntime, stepName string, mutation *BuildMutationRequest) (ReconcileBuildResult, error) {
	return b.executeObjectTransitionTick(rt, stepName, mutation, 0)
}

// executeObjectTransitionTick runs one transition; deferralSeq identifies the
// delayed deferral Reconcile delivering it, zero for every other transition.
func (b *BuildObject) executeObjectTransitionTick(rt objectRuntime, stepName string, mutation *BuildMutationRequest, deferralSeq int) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		err := restate.ToTerminalError(fmt.Errorf("%w: build_id cannot be empty", store.ErrInvalidBuildInput), restate.WithErrorCode(http.StatusBadRequest))
		RecordReconcileError(stepName, err)
		return ReconcileBuildResult{}, err
	}

	priorState, err := rt.GetState()
	if err != nil {
		RecordReconcileError(stepName, err)
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched, err := rt.GetLaunched()
	if err != nil {
		RecordReconcileError(stepName, err)
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}

	priorLaunched = mergePriorLaunched(priorState, priorLaunched)
	// The invocation ID is read outside the journaled step: replays of this
	// invocation reuse it, while every new invocation gets a fresh launch key.
	attempt := transitionAttempt{InvocationID: rt.InvocationID(), DeferralSeq: deferralSeq}
	stepOut, err := b.runTransitionStep(rt, stepName, buildID, mutation, priorState, priorLaunched, attempt)
	if err != nil {
		return ReconcileBuildResult{BuildID: buildID, Status: statusFailed, Error: err.Error()}, err
	}
	sendCrossBuildReconciles(rt, stepOut.ReconcileTargets)

	if stepOut.BuildStatus == statusCompleted || stepOut.BuildStatus == statusCancelled || stepOut.BuildStatus == statusNotFound {
		rt.ClearState()
		// Keep only the delivery marker across terminal invocations. The journaled
		// step result restores it on replay after a crash before SetState.
		if terminalNotificationRecorded(priorState) || terminalNotificationRecorded(stepOut.State) {
			rt.SetState(model.BuildObjectState{TerminalNotified: true})
		}
	} else {
		if stepOut.State != nil {
			rt.SetState(*stepOut.State)
		}
		rt.SetLaunched(stepOut.LaunchedMap)
		if err := b.syncDeploymentDeadlines(rt, stepOut.State, nil, nil); err != nil {
			return ReconcileBuildResult{}, err
		}
	}

	if delay, seq, ok := scheduledDeferralReconcile(stepOut, priorDeferralSeq(priorState)); ok {
		rt.ScheduleDeferredReconcile(delay, seq)
	} else if b.shouldScheduleReconcile(stepOut) {
		rt.ScheduleReconcile()
	}
	return ReconcileBuildResult{
		BuildID:        buildID,
		Status:         stepOut.BuildStatus,
		LaunchedChunks: stepOut.LaunchedChunks,
		Error:          stepOut.Error,
	}, nil
}

func (b *BuildObject) applyMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	if mutation == nil {
		return nil
	}
	mutation = normalizeCompletionMutation(mutation)
	action := strings.ToLower(strings.TrimSpace(mutation.Action))
	switch action {
	case "chunk_merged":
		if err := b.store.UpdatePRLifecycleStatus(ctx, mutation.ChunkID, "merged"); err != nil {
			if errors.Is(err, errPRTransitionSkipped) {
				return nil
			}
			return err
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "merged", []string{mutation.ChunkID}, ""); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
			}
		}
	case mutationChunkDeploySuccess:
		return b.completePRDeploymentMutation(ctx, buildID, mutation)
	case mutationActionCompleted:
		if err := b.validateActionBuild(ctx, buildID, mutation.ChunkID); err != nil {
			return err
		}
		res, err := b.store.CompleteActionChunk(ctx, mutation.ChunkID, mutation.DispatchID)
		if err != nil {
			return err
		}
		if res == nil || !res.CompletionAccepted {
			return errActionCompletionSkipped
		}
		return nil
	case mutationChunkFailed:
		return b.failMutation(ctx, buildID, mutation)
	case "cancelled":
		if err := b.store.UpdateBuildStatus(ctx, buildID, statusCancelled); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] update build status warning", "error", err)
		}
		if b.executor != nil {
			if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", "cancelled", []string{buildID}, mutation.Reason); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
			}
		}
	}
	return nil
}

func countActiveChunks(chunks []model.VanadisChunk) int {
	activeCount := 0
	for _, c := range chunks {
		s := strings.ToLower(strings.TrimSpace(c.Status))
		if s == statusRunning || s == "coding" || s == "open" || s == "pr_opened" || s == "deploying" {
			activeCount++
		}
	}
	return activeCount
}

func isLaunchCandidate(chunk model.VanadisChunk, chunks []model.VanadisChunk, launchedMap map[string]bool) (candidate bool, resuming bool) {
	s := strings.ToLower(strings.TrimSpace(chunk.Status))
	res := s == statusRunning && chunk.Mode == chunkModeAction && chunk.StallNudges > 0
	unlaunched := s == statusRunning && !launchedMap[actionLaunchKey(chunk)] && chunk.PRNumber == 0
	if s != statusPending && s != "queued" && !res && !unlaunched {
		return false, false
	}
	if dag.CheckChunkDependenciesReady(chunk, chunks) != nil {
		return false, false
	}
	return true, res || unlaunched
}

// launchEligibleChunks dispatches ready chunks. It returns the launched chunk
// IDs and the launch keys eri deferred with skipped_active; a deferred chunk
// keeps its pre-launch status.
func (b *BuildObject) launchEligibleChunks(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk, launchedMap map[string]bool) ([]string, []string, error) {
	var launchedChunks, deferredKeys []string
	var launchErrors []error
	activeCount := countActiveChunks(chunks)

	for idx, chunk := range chunks {
		candidate, resuming := isLaunchCandidate(chunk, chunks, launchedMap)
		if !candidate {
			continue
		}
		key := actionLaunchKey(chunk)
		if activeCount > 4 || (!resuming && activeCount >= 4) || launchedMap[key] || b.executor == nil {
			continue
		}
		if !resuming {
			activeCount++
		}
		if err := b.executor.ExecuteLaunchChunk(ctx, b.store, build, chunks, idx, ""); err != nil {
			if errors.Is(err, executor.ErrDispatchActive) {
				deferredKeys = append(deferredKeys, key)
			}
			launchErrors = append(launchErrors, err)
			continue
		}
		chunks[idx].Status = statusRunning
		if chunks[idx].StartedAt.IsZero() {
			chunks[idx].StartedAt = time.Now().UTC()
		}
		launchedMap[key] = true
		launchedChunks = append(launchedChunks, chunk.ID)
	}
	return launchedChunks, deferredKeys, classifyLaunchErrors(launchErrors)
}

// Only active-session deferrals may complete the journaled step successfully.
// Any real failure still triggers durable invocation retry, including mixed failures.
func classifyLaunchErrors(errs []error) error {
	var failures []error
	for _, err := range errs {
		if !errors.Is(err, executor.ErrDispatchActive) {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if len(errs) > 0 {
		return executor.ErrDispatchActive
	}
	return nil
}

func (b *BuildObject) shouldScheduleReconcile(out reconcileStepOutput) bool {
	return b.store != nil && out.Error == executor.ErrDispatchActive.Error() && out.BuildStatus == statusRunning && !hasDispatchDeferrals(out)
}

func hasDispatchDeferrals(out reconcileStepOutput) bool {
	return out.State != nil && len(out.State.DispatchDeferrals) > 0
}

// scheduledDeferralReconcile derives the delayed deferral Reconcile from the
// journaled step output alone. A step that advanced DeferralReconcileSeq owns
// the build's single chain; an unchanged seq means another tick is pending.
// Outputs journaled before the lease carry no deferrals and keep the legacy
// 1s ScheduleReconcile, so in-flight invocations replay the same commands.
func scheduledDeferralReconcile(out reconcileStepOutput, priorSeq int) (time.Duration, int, bool) {
	if out.BuildStatus != statusRunning || !hasDispatchDeferrals(out) || out.State.DeferralReconcileSeq == priorSeq {
		return 0, 0, false
	}
	return deferralDelay(out.State.DispatchDeferrals), out.State.DeferralReconcileSeq, true
}

func (b *BuildObject) notifyBuildTerminal(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, status, reason, promptHeader, promptMsg string) error {
	if b.executor == nil {
		return nil
	}
	if build != nil && strings.TrimSpace(build.Agent) != "" {
		ctx = ericlient.WithAgent(ctx, build.Agent)
	}
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_build", status, []string{build.ID}, reason); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
	}
	var wakeErr error
	if !build.Quiet {
		prompt := fmt.Sprintf("[%s]\nBuild ID: %s\nObjective: %s\n\n%s", promptHeader, build.ID, build.Objective, promptMsg)
		if err := b.executor.DispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] wake prompt warning", "error", err)
			wakeErr = err
		}
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build "+status); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
	}
	return wakeErr
}

func rolloutStateJobsEqual(a, b *pubsub.PRRolloutState) bool {
	if a == b {
		return true
	}
	var aExp, aStart, aDep []string
	var aStates, aKust map[string]string
	if a != nil {
		aExp, aStart, aDep = a.ExpectedJobs, a.StartedJobs, a.DeployedJobs
		aStates, aKust = a.JobStates, a.KustomizationStatuses
	}
	var bExp, bStart, bDep []string
	var bStates, bKust map[string]string
	if b != nil {
		bExp, bStart, bDep = b.ExpectedJobs, b.StartedJobs, b.DeployedJobs
		bStates, bKust = b.JobStates, b.KustomizationStatuses
	}
	return slices.Equal(aExp, bExp) &&
		slices.Equal(aStart, bStart) &&
		slices.Equal(aDep, bDep) &&
		maps.Equal(aStates, bStates) &&
		maps.Equal(aKust, bKust)
}

func widgetStateEqual(prior *model.BuildObjectState, build model.VanadisBuild, chunks []model.VanadisChunk) bool {
	if prior == nil {
		return false
	}
	if strings.TrimSpace(build.StatusMessageID) == "" {
		return false
	}
	if prior.Build.StatusMessageID != build.StatusMessageID {
		return false
	}
	if prior.Build.Status != build.Status {
		return false
	}
	if prior.Build.Title != build.Title {
		return false
	}
	if prior.Build.Tier != build.Tier {
		return false
	}
	if prior.Build.ThreadID != build.ThreadID {
		return false
	}
	if len(prior.Chunks) != len(chunks) {
		return false
	}
	for i := range chunks {
		pc := prior.Chunks[i]
		c := chunks[i]
		if pc.ID != c.ID ||
			pc.ChunkIndex != c.ChunkIndex ||
			pc.Mode != c.Mode ||
			pc.Repo != c.Repo ||
			pc.Title != c.Title ||
			pc.Status != c.Status ||
			pc.PRNumber != c.PRNumber ||
			executor.FormatChunkActiveStep(pc) != executor.FormatChunkActiveStep(c) ||
			!rolloutStateJobsEqual(pc.RolloutState, c.RolloutState) {
			return false
		}
	}
	return true
}

func (b *BuildObject) evaluateBuildTerminalStatus(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, skipWidgetSync ...bool) (string, error) {
	allDone := true
	hasFailed := false
	for _, c := range chunks {
		s := strings.ToLower(strings.TrimSpace(c.Status))
		if s == statusFailed {
			hasFailed = true
		}
		if s != statusCompleted {
			allDone = false
		}
	}

	switch {
	case hasFailed:
		build.Status = statusFailed
		if err := b.notifyBuildTerminal(ctx, build, chunks, statusFailed, "chunk failed", "VANADIS BUILD FAILED", "Build encountered failures."); err != nil {
			return "", err
		}
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusFailed); err != nil {
			slog.ErrorContext(ctx, "[vanadis-restate] update build status failed", "error", err)
			return "", err
		}
		return statusFailed, nil
	case allDone && len(chunks) > 0:
		build.Status = statusCompleted
		if err := b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully."); err != nil {
			return "", err
		}
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusCompleted); err != nil {
			slog.ErrorContext(ctx, "[vanadis-restate] update build status completed", "error", err)
			return "", err
		}
		return statusCompleted, nil
	default:
		skip := len(skipWidgetSync) > 0 && skipWidgetSync[0]
		if !skip && b.executor != nil {
			if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, ""); err != nil {
				slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
			}
		}
		return statusRunning, nil
	}
}

func cloneLaunchedMap(priorLaunched map[string]bool) map[string]bool {
	launchedMap := make(map[string]bool, len(priorLaunched))
	for k, v := range priorLaunched {
		if v {
			launchedMap[k] = true
		}
	}
	return launchedMap
}

func (b *BuildObject) replayTerminalMutationEffects(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, mutation *BuildMutationRequest, terminalNotified bool) (bool, error) {
	if terminalNotified {
		return true, nil
	}
	if build.Status == statusCompleted && isActionCompletionMutation(mutation, chunks) {
		if nErr := b.notifyBuildTerminal(ctx, build, chunks, statusCompleted, "", "VANADIS BUILD COMPLETED", "All chunks completed successfully."); nErr != nil {
			return false, nErr
		}
		return true, nil
	}
	if build.Status == statusFailed && isFailureCompletionMutation(mutation, chunks) {
		if nErr := b.notifyBuildTerminal(ctx, build, chunks, statusFailed, store.FirstNonEmpty(mutation.Reason, "chunk failed"), "VANADIS BUILD FAILED", "Build encountered failures."); nErr != nil {
			return false, nErr
		}
		return true, nil
	}
	return false, nil
}

func (b *BuildObject) executeAuthoritativeTransition(ctx context.Context, buildID string, mutation *BuildMutationRequest, priorState *model.BuildObjectState, priorLaunched map[string]bool, terminalNotified bool) (reconcileStepOutput, error) {
	if b.store == nil {
		return reconcileStepOutput{}, errors.New("store not configured")
	}

	mutation = normalizeCompletionMutation(mutation)

	if err := b.applyMutation(ctx, buildID, mutation); err != nil {
		if errors.Is(err, errActionCompletionSkipped) {
			return b.skippedCompletionOutput(ctx, buildID, priorState, priorLaunched)
		}
		return reconcileStepOutput{}, err
	}

	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, store.ErrBuildNotFound) {
			return reconcileStepOutput{BuildStatus: statusNotFound}, nil
		}
		return reconcileStepOutput{}, err
	}

	var nErr error
	terminalNotified, nErr = b.replayTerminalMutationEffects(ctx, build, chunks, mutation, terminalNotified)
	if nErr != nil {
		return reconcileStepOutput{}, nErr
	}
	if build.Status == statusCompleted || build.Status == statusFailed || build.Status == statusCancelled {
		launchedMap := cloneLaunchedMap(priorLaunched)
		return reconcileStepOutput{
			BuildStatus: build.Status,
			LaunchedMap: launchedMap,
			State: &model.BuildObjectState{
				Build:            *build,
				Chunks:           chunks,
				Launched:         launchedMap,
				TerminalNotified: terminalNotified,
			},
		}, nil
	}

	launchedMap := cloneLaunchedMap(priorLaunched)
	launchedChunks, deferredKeys, err := b.launchEligibleChunks(ctx, *build, chunks, launchedMap)
	if err != nil && !errors.Is(err, executor.ErrDispatchActive) {
		return reconcileStepOutput{}, err
	}
	deferrals, expired := advanceDispatchDeferrals(priorDispatchDeferrals(priorState), deferredKeys, time.Now().UTC())
	if build, chunks, err = b.expireNeverAcceptedLaunches(ctx, buildID, build, chunks, expired); err != nil {
		return reconcileStepOutput{}, err
	}
	recoveryReason := ""
	if len(deferrals) > 0 {
		recoveryReason = executor.ErrDispatchActive.Error()
	}
	skipSync := len(launchedChunks) == 0 && widgetStateEqual(priorState, *build, chunks)
	finalStatus, err := b.evaluateBuildTerminalStatus(ctx, build, chunks, skipSync)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	build.Status = finalStatus
	if finalStatus != statusRunning {
		deferrals = nil
	}
	seq := planDeferralSeq(priorDeferralSeq(priorState), transitionAttemptFrom(ctx).DeferralSeq, deferrals)

	return reconcileStepOutput{
		BuildStatus:    finalStatus,
		Error:          recoveryReason,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State: &model.BuildObjectState{
			Build:                *build,
			Chunks:               chunks,
			Launched:             launchedMap,
			TerminalNotified:     finalStatus == statusCompleted || finalStatus == statusFailed,
			DispatchDeferrals:    deferrals,
			DeferralReconcileSeq: seq,
		},
	}, nil
}

const (
	// dispatchDeferralLease bounds continuous skipped_active deferral of one launch.
	dispatchDeferralLease = 10 * time.Minute
	deferralBaseDelay     = time.Second
	deferralMaxDelay      = time.Minute
	// ReasonDispatchNeverAccepted fails a launch eri kept deferring; the failed
	// chunk wakes the thread and retry_action_chunk resumes it.
	ReasonDispatchNeverAccepted = "RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"
)

// transitionAttempt identifies the invocation running a transition step.
type transitionAttempt struct {
	InvocationID string
	DeferralSeq  int
}

type transitionAttemptKey struct{}

// withTransitionAttempt scopes chunk launch idempotency to this invocation.
func withTransitionAttempt(ctx context.Context, attempt transitionAttempt) context.Context {
	return context.WithValue(ericlient.WithDispatchAttempt(ctx, attempt.InvocationID), transitionAttemptKey{}, attempt)
}

func transitionAttemptFrom(ctx context.Context) transitionAttempt {
	if attempt, ok := ctx.Value(transitionAttemptKey{}).(transitionAttempt); ok {
		return attempt
	}
	return transitionAttempt{}
}

func priorDispatchDeferrals(prior *model.BuildObjectState) map[string]model.DispatchDeferral {
	if prior == nil {
		return nil
	}
	return prior.DispatchDeferrals
}

func priorDeferralSeq(prior *model.BuildObjectState) int {
	if prior == nil {
		return 0
	}
	return prior.DeferralReconcileSeq
}

// advanceDispatchDeferrals keeps only launches deferred again in this
// transition (acceptance or no attempt ends continuity) and returns the keys
// whose deferral has lasted the full lease.
func advanceDispatchDeferrals(prior map[string]model.DispatchDeferral, deferredKeys []string, now time.Time) (map[string]model.DispatchDeferral, []string) {
	var next map[string]model.DispatchDeferral
	var expired []string
	for _, key := range deferredKeys {
		entry, ok := prior[key]
		if !ok || entry.FirstDeferredAt.IsZero() {
			entry = model.DispatchDeferral{FirstDeferredAt: now}
		}
		entry.Count++
		if now.Sub(entry.FirstDeferredAt) >= dispatchDeferralLease {
			expired = append(expired, key)
			continue
		}
		if next == nil {
			next = make(map[string]model.DispatchDeferral, len(deferredKeys))
		}
		next[key] = entry
	}
	return next, expired
}

// deferralDelay doubles from 1s per deferral of the freshest entry, capped at 60s.
func deferralDelay(deferrals map[string]model.DispatchDeferral) time.Duration {
	minCount := 0
	for _, entry := range deferrals {
		if minCount == 0 || entry.Count < minCount {
			minCount = entry.Count
		}
	}
	delay := deferralBaseDelay
	for i := 1; i < minCount && delay < deferralMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, deferralMaxDelay)
}

// planDeferralSeq keeps one delayed Reconcile chain per build. Only the
// pending chain's own tick, or a transition while none is pending, advances
// the seq (scheduling the next tick); otherwise the pending seq is kept.
// Without deferrals no tick is pending.
func planDeferralSeq(priorSeq, tickSeq int, deferrals map[string]model.DispatchDeferral) int {
	if len(deferrals) == 0 {
		return 0
	}
	if priorSeq != 0 && tickSeq != priorSeq {
		return priorSeq
	}
	return priorSeq + 1
}

// skippedCompletionOutput keeps state, including any pending deferral chain,
// when a stale completion mutation was skipped without launching anything.
func (b *BuildObject) skippedCompletionOutput(ctx context.Context, buildID string, priorState *model.BuildObjectState, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	skipped := &model.BuildObjectState{Build: *build, Chunks: chunks, Launched: priorLaunched}
	if priorState != nil {
		skipped.DispatchDeferrals, skipped.DeferralReconcileSeq = priorState.DispatchDeferrals, priorState.DeferralReconcileSeq
	}
	return reconcileStepOutput{BuildStatus: build.Status, State: skipped}, nil
}

// expireNeverAcceptedLaunches fails expired launches and re-reads the build.
func (b *BuildObject) expireNeverAcceptedLaunches(ctx context.Context, buildID string, build *model.VanadisBuild, chunks []model.VanadisChunk, expired []string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if len(expired) == 0 {
		return build, chunks, nil
	}
	if err := b.failNeverAcceptedLaunches(ctx, buildID, chunks, expired); err != nil {
		return nil, nil, err
	}
	return b.store.GetBuild(ctx, buildID)
}

// failNeverAcceptedLaunches fails chunks whose launch eri deferred for the
// whole lease. A pending action chunk is reserved first so its failure is
// accepted as the current generation and retry_action_chunk can resume it.
func (b *BuildObject) failNeverAcceptedLaunches(ctx context.Context, buildID string, chunks []model.VanadisChunk, expired []string) error {
	for _, key := range expired {
		for _, chunk := range chunks {
			if actionLaunchKey(chunk) != key {
				continue
			}
			if s := strings.ToLower(strings.TrimSpace(chunk.Status)); s == statusPending || s == "queued" {
				if err := b.store.ReserveChunkForLaunch(ctx, chunk.ID); err != nil && !errors.Is(err, store.ErrChunkNotFound) {
					return err
				}
			}
			err := b.failMutation(ctx, buildID, &BuildMutationRequest{Action: mutationChunkFailed, ChunkID: chunk.ID, DispatchID: model.ActionDispatchID(chunk), Reason: ReasonDispatchNeverAccepted})
			if err != nil && !errors.Is(err, errActionCompletionSkipped) {
				return err
			}
		}
	}
	return nil
}
