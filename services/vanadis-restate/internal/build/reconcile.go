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

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
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
	rt := b.getRuntime(ctx)
	res, err := b.executeObjectTransition(rt, labelReconcileBuild, nil)
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

func (b *BuildObject) runTransitionStep(rt objectRuntime, stepName, buildID string, mutation *BuildMutationRequest, priorState *model.BuildObjectState, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	if b == nil || b.store == nil {
		err := restate.TerminalErrorf("store not configured")
		RecordReconcileError(stepName, err)
		return reconcileStepOutput{}, err
	}
	notifiedInStep := false
	stepOut, err := rt.RunReconcileStep(stepName, func(stepCtx context.Context) (reconcileStepOutput, error) {
		ctx, crossBuild := withCrossBuildReconciles(stepCtx)
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
	stepOut, err := b.runTransitionStep(rt, stepName, buildID, mutation, priorState, priorLaunched)
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
	if s != statusPending && s != statusQueued && !res && !unlaunched {
		return false, false
	}
	if buildplan.CheckChunkDependenciesReady(chunk, chunks) != nil {
		return false, false
	}
	return true, res || unlaunched
}

// launchEligibleChunks dispatches ready chunks. It returns the launched chunk IDs.
// maxActiveChunks caps how many chunks of one build run at once.
const maxActiveChunks = 6

func (b *BuildObject) launchEligibleChunks(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk, launchedMap map[string]bool) ([]string, error) {
	var launchedChunks []string
	var launchErrors []error
	activeCount := countActiveChunks(chunks)

	for idx, chunk := range chunks {
		candidate, resuming := isLaunchCandidate(chunk, chunks, launchedMap)
		if !candidate {
			continue
		}
		key := actionLaunchKey(chunk)
		if activeCount > maxActiveChunks || (!resuming && activeCount >= maxActiveChunks) || launchedMap[key] || b.executor == nil {
			continue
		}
		if !resuming {
			activeCount++
		}
		if err := b.executor.ExecuteLaunchChunk(ctx, b.store, build, chunks, idx, ""); err != nil {
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
	return launchedChunks, errors.Join(launchErrors...)
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
	if !build.Quiet && promptHeader != "" {
		prompt := fmt.Sprintf("[%s]\nBuild ID: %s\nObjective: %s\n\n%s", promptHeader, build.ID, build.Objective, promptMsg)
		if err := b.executor.DispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] wake prompt warning", "error", err)
			wakeErr = err
		}
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build "+status); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
	}
	b.executor.ClearActiveSteps(build.ID)
	b.activeStepSyncs.Delete(build.ID)
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

const statusQueued = "queued"

// buildQuiesced reports whether nothing in the build can still move without
// an explicit retry: no chunk is in flight (anything outside completed, failed,
// cancelled, pending and queued, so deploy_failed counts) and no pending or
// queued chunk has its dependencies ready.
func buildQuiesced(chunks []model.VanadisChunk) bool {
	for _, c := range chunks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case statusCompleted, statusFailed, statusCancelled:
		case statusPending, statusQueued:
			if buildplan.CheckChunkDependenciesReady(c, chunks) == nil {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// failureNotifyKey identifies one failure of a chunk: a retried chunk that
// fails again gets a new completion time and so a new key.
func failureNotifyKey(c model.VanadisChunk) string {
	return c.ID + "@" + c.CompletedAt.UTC().Format(time.RFC3339Nano)
}

// seedFailureNotified returns the failure-wake ledger for this transition.
// Saved state from before the ledger existed counts every chunk it already
// recorded as failed as notified, so in-flight builds get no burst of wakes.
func seedFailureNotified(prior *model.BuildObjectState) map[string]bool {
	notified := map[string]bool{}
	if prior == nil {
		return notified
	}
	if prior.FailureNotified != nil {
		maps.Copy(notified, prior.FailureNotified)
		return notified
	}
	for _, c := range prior.Chunks {
		if strings.ToLower(strings.TrimSpace(c.Status)) == statusFailed {
			notified[failureNotifyKey(c)] = true
		}
	}
	return notified
}

// pendingFailureWakes returns failed chunks whose failure has not woken the
// thread yet and prunes ledger keys of chunks no longer failed.
func pendingFailureWakes(chunks []model.VanadisChunk, notified map[string]bool) []model.VanadisChunk {
	current := make(map[string]bool, len(notified))
	var pending []model.VanadisChunk
	for _, c := range chunks {
		if strings.ToLower(strings.TrimSpace(c.Status)) != statusFailed {
			continue
		}
		key := failureNotifyKey(c)
		current[key] = true
		if !notified[key] {
			pending = append(pending, c)
		}
	}
	maps.DeleteFunc(notified, func(k string, _ bool) bool { return !current[k] })
	return pending
}

func chunkFailureLine(c model.VanadisChunk) string {
	if banner := strings.TrimSpace(c.Banner); banner != "" {
		return banner + " [" + c.ID + "]"
	}
	return fmt.Sprintf("Chunk %d failed [%s]", c.ChunkIndex+1, c.ID)
}

func isCapReachedChunkFailure(c model.VanadisChunk) bool {
	return strings.HasSuffix(strings.TrimSpace(c.Banner), ": ci_remediation_cap_reached")
}

// notifyChunkFailures wakes the build's thread once per newly failed chunk and
// publishes its vanadis_chunk failed event, recording each in notified. A
// failed wake returns its error so the journaled step retries: a retry may
// duplicate a wake but never loses one.
func (b *BuildObject) notifyChunkFailures(ctx context.Context, build *model.VanadisBuild, failures []model.VanadisChunk, notified map[string]bool) error {
	if b.executor != nil && strings.TrimSpace(build.Agent) != "" {
		ctx = ericlient.WithAgent(ctx, build.Agent)
	}
	for _, c := range failures {
		if b.executor != nil {
			if !build.Quiet && !isCapReachedChunkFailure(c) {
				prompt := fmt.Sprintf("[VANADIS CHUNK FAILED]\nBuild ID: %s\nObjective: %s\n\n%s\nOther chunks continue; only this chunk's dependents wait. Repair in place with retry_action_chunk.", build.ID, build.Objective, chunkFailureLine(c))
				if err := b.executor.DispatchWakePrompt(ctx, build.ThreadID, prompt); err != nil {
					slog.WarnContext(ctx, "[vanadis-restate] chunk failure wake warning", "chunk_id", c.ID, "error", err)
					return err
				}
			}
			b.publishChunkFailed(ctx, c)
		}
		notified[failureNotifyKey(c)] = true
	}
	return nil
}

func (b *BuildObject) publishChunkFailed(ctx context.Context, c model.VanadisChunk) {
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", statusFailed, []string{c.ID}, strutil.FirstNonEmpty(strings.TrimSpace(c.Banner), "chunk failed")); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
	}
}

// evaluateBuildTerminalStatus decides the build status after a transition. A
// failed chunk fails the build only once the build has quiesced; until then
// independent chunks keep running and each failure wakes the thread once.
func (b *BuildObject) evaluateBuildTerminalStatus(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, notified map[string]bool, skipWidgetSync bool) (string, error) {
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
	failures := pendingFailureWakes(chunks, notified)

	switch {
	case hasFailed && buildQuiesced(chunks):
		return b.settleFailedBuild(ctx, build, chunks, failures, notified)
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
		return b.settleRunningBuild(ctx, build, chunks, failures, notified, skipWidgetSync)
	}
}

func (b *BuildObject) notifyInitialBuildFailure(ctx context.Context, build *model.VanadisBuild, chunks, failures []model.VanadisChunk, notified map[string]bool) error {
	var wakeFailures []model.VanadisChunk
	for _, c := range failures {
		if !isCapReachedChunkFailure(c) {
			wakeFailures = append(wakeFailures, c)
		}
	}
	allFailedAreCap := true
	for _, c := range chunks {
		if strings.ToLower(strings.TrimSpace(c.Status)) == statusFailed && !isCapReachedChunkFailure(c) {
			allFailedAreCap = false
			break
		}
	}
	promptHeader := "VANADIS BUILD FAILED"
	promptMsg := ""
	if len(wakeFailures) == 0 && allFailedAreCap {
		promptHeader = ""
	} else {
		lines := make([]string, 1, 1+len(wakeFailures))
		lines[0] = "Build encountered failures."
		for _, c := range wakeFailures {
			lines = append(lines, chunkFailureLine(c))
		}
		promptMsg = strings.Join(lines, "\n")
	}
	if err := b.notifyBuildTerminal(ctx, build, chunks, statusFailed, "chunk failed", promptHeader, promptMsg); err != nil {
		return err
	}
	for _, c := range failures {
		if b.executor != nil {
			b.publishChunkFailed(ctx, c)
		}
		notified[failureNotifyKey(c)] = true
	}
	return nil
}

// settleFailedBuild fails a quiesced build. Only the running->failed edge
// sends the combined build-failed wake, before the status write, so a step
// retry may duplicate a wake but never loses one.
func (b *BuildObject) settleFailedBuild(ctx context.Context, build *model.VanadisBuild, chunks, failures []model.VanadisChunk, notified map[string]bool) (string, error) {
	wasFailed := strings.ToLower(strings.TrimSpace(build.Status)) == statusFailed
	build.Status = statusFailed
	if wasFailed {
		if err := b.notifyChunkFailures(ctx, build, failures, notified); err != nil {
			return "", err
		}
	} else {
		if err := b.notifyInitialBuildFailure(ctx, build, chunks, failures, notified); err != nil {
			return "", err
		}
	}
	if err := b.store.UpdateBuildStatus(ctx, build.ID, statusFailed); err != nil {
		slog.ErrorContext(ctx, "[vanadis-restate] update build status failed", "error", err)
		return "", err
	}
	return statusFailed, nil
}

// settleRunningBuild keeps (or puts) a build running while chunks are in
// flight or launchable, waking the thread once per new chunk failure.
func (b *BuildObject) settleRunningBuild(ctx context.Context, build *model.VanadisBuild, chunks, failures []model.VanadisChunk, notified map[string]bool, skipWidgetSync bool) (string, error) {
	if err := b.notifyChunkFailures(ctx, build, failures, notified); err != nil {
		return "", err
	}
	if strings.ToLower(strings.TrimSpace(build.Status)) == statusFailed {
		build.Status = statusRunning
		if err := b.store.UpdateBuildStatus(ctx, build.ID, statusRunning); err != nil {
			slog.ErrorContext(ctx, "[vanadis-restate] update build status running", "error", err)
			return "", err
		}
		skipWidgetSync = false
	}
	if !skipWidgetSync && b.executor != nil {
		if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, ""); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
		}
	}
	return statusRunning, nil
}

// transitionHalted reports whether a transition must not launch anything.
// Completed and cancelled builds never launch. A failed build launches only
// when it failed under failure isolation; legacy failed builds resume only
// through an explicit retry, so no superseded work relaunches.
func transitionHalted(status string, isolated bool) bool {
	return status == statusCompleted || status == statusCancelled || (status == statusFailed && !isolated)
}

// haltedTransitionOutput keeps a halted build's state; a legacy failed build
// still wakes the thread once per new chunk failure.
func (b *BuildObject) haltedTransitionOutput(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, priorLaunched map[string]bool, terminalNotified bool, notified map[string]bool, isolated bool) (reconcileStepOutput, error) {
	if build.Status == statusFailed {
		if err := b.notifyChunkFailures(ctx, build, pendingFailureWakes(chunks, notified), notified); err != nil {
			return reconcileStepOutput{}, err
		}
	}
	launchedMap := cloneLaunchedMap(priorLaunched)
	st := compactStateForStorage(model.BuildObjectState{
		Build:            *build,
		Chunks:           chunks,
		Launched:         launchedMap,
		TerminalNotified: terminalNotified,
		FailureNotified:  notified,
		FailureIsolation: isolated,
	})
	return reconcileStepOutput{
		BuildStatus: build.Status,
		LaunchedMap: launchedMap,
		State:       &st,
	}, nil
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
	notified := seedFailureNotified(priorState)
	isolated := priorState != nil && priorState.FailureIsolation
	if transitionHalted(build.Status, isolated) {
		return b.haltedTransitionOutput(ctx, build, chunks, priorLaunched, terminalNotified, notified, isolated)
	}

	launchedMap := cloneLaunchedMap(priorLaunched)
	launchedChunks, err := b.launchEligibleChunks(ctx, *build, chunks, launchedMap)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	skipSync := len(launchedChunks) == 0 && widgetStateEqual(priorState, *build, chunks)
	finalStatus, err := b.evaluateBuildTerminalStatus(ctx, build, chunks, notified, skipSync)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	build.Status = finalStatus

	st := compactStateForStorage(model.BuildObjectState{
		Build:            *build,
		Chunks:           chunks,
		Launched:         launchedMap,
		TerminalNotified: finalStatus == statusCompleted || finalStatus == statusFailed,
		FailureNotified:  notified,
		FailureIsolation: true,
	})
	return reconcileStepOutput{
		BuildStatus:    finalStatus,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State:          &st,
	}, nil
}

func compactStateForStorage(state model.BuildObjectState) model.BuildObjectState {
	state.Build.SharedContext = ""
	if state.Chunks != nil {
		chunks := make([]model.VanadisChunk, len(state.Chunks))
		for i, c := range state.Chunks {
			c.Instructions = ""
			c.TargetFiles = nil
			c.TargetSymbols = nil
			c.AcceptanceCriteria = nil
			chunks[i] = c
		}
		state.Chunks = chunks
	}
	return state
}

// skippedCompletionOutput keeps state when a stale completion mutation was skipped without launching anything.
func (b *BuildObject) skippedCompletionOutput(ctx context.Context, buildID string, priorState *model.BuildObjectState, priorLaunched map[string]bool) (reconcileStepOutput, error) {
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	skipped := model.BuildObjectState{Build: *build, Chunks: chunks, Launched: priorLaunched}
	if priorState != nil {
		skipped.FailureNotified, skipped.FailureIsolation = priorState.FailureNotified, priorState.FailureIsolation
	}
	st := compactStateForStorage(skipped)
	return reconcileStepOutput{BuildStatus: build.Status, State: &st}, nil
}
