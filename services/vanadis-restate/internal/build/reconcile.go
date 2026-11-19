package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/dag"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Reconcile reconciles the build state on Restate.
func (b *BuildObject) Reconcile(ctx restate.ObjectContext, req ReconcileBuildRequest) (ReconcileBuildResult, error) {
	_ = req
	if b == nil || b.store == nil {
		err := restate.TerminalErrorf("store not configured")
		RecordReconcileError(labelReconcileBuild, err)
		return ReconcileBuildResult{}, err
	}
	rt := b.getRuntime(ctx)
	return b.executeObjectTransition(rt, labelReconcileBuild, nil)
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
	stepOut, err := rt.RunReconcileStep(stepName, func(ctx context.Context) (reconcileStepOutput, error) {
		out, stepErr := b.executeAuthoritativeTransition(ctx, buildID, mutation, priorLaunched, terminalNotificationRecorded(priorState))
		if stepErr != nil {
			notifiedInStep = true
			RecordReconcileError(stepName, stepErr)
			b.notifyTransitionError(ctx, buildID, stepName, priorState, stepErr)
		}
		return out, stepErr
	})
	if err != nil && !notifiedInStep {
		RecordReconcileError(stepName, err)
		b.notifyTransitionError(context.Background(), buildID, stepName, priorState, err)
	}
	return stepOut, err
}

func (b *BuildObject) executeObjectTransition(rt objectRuntime, stepName string, mutation *BuildMutationRequest) (ReconcileBuildResult, error) {
	buildID := strings.TrimSpace(rt.Key())
	if buildID == "" {
		err := restate.TerminalErrorf("build_id cannot be empty")
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

	if stepOut.BuildStatus == statusCompleted || stepOut.BuildStatus == statusFailed || stepOut.BuildStatus == statusCancelled || stepOut.BuildStatus == statusNotFound {
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

	if b.shouldScheduleReconcile(stepOut) {
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
		if activeCount > 4 || (!resuming && activeCount >= 4) || launchedMap[key] || b.executor == nil {
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
	return launchedChunks, classifyLaunchErrors(launchErrors)
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
	return b.store != nil && out.Error == executor.ErrDispatchActive.Error() && out.BuildStatus == statusRunning
}

func (b *BuildObject) notifyBuildTerminal(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk, status, reason, promptHeader, promptMsg string) error {
	if b.executor == nil {
		return nil
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
	var widgetErr error
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build "+status); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", err)
		widgetErr = err
	}
	return errors.Join(wakeErr, widgetErr)
}

func (b *BuildObject) evaluateBuildTerminalStatus(ctx context.Context, build *model.VanadisBuild, chunks []model.VanadisChunk) (string, error) {
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
		if b.executor != nil {
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

func (b *BuildObject) executeAuthoritativeTransition(ctx context.Context, buildID string, mutation *BuildMutationRequest, priorLaunched map[string]bool, terminalNotified bool) (reconcileStepOutput, error) {
	if b.store == nil {
		return reconcileStepOutput{}, errors.New("store not configured")
	}

	mutation = normalizeCompletionMutation(mutation)

	if err := b.applyMutation(ctx, buildID, mutation); err != nil {
		if errors.Is(err, errActionCompletionSkipped) {
			build, chunks, readErr := b.store.GetBuild(ctx, buildID)
			if readErr != nil {
				return reconcileStepOutput{}, readErr
			}
			return reconcileStepOutput{BuildStatus: build.Status, State: &model.BuildObjectState{Build: *build, Chunks: chunks, Launched: priorLaunched}}, nil
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
		return reconcileStepOutput{
			BuildStatus: build.Status,
			State:       &model.BuildObjectState{Build: *build, Chunks: chunks, TerminalNotified: terminalNotified},
		}, nil
	}

	launchedMap := cloneLaunchedMap(priorLaunched)
	launchedChunks, err := b.launchEligibleChunks(ctx, *build, chunks, launchedMap)
	deferred := errors.Is(err, executor.ErrDispatchActive)
	if err != nil && !deferred {
		return reconcileStepOutput{}, err
	}
	recoveryReason := ""
	if deferred {
		recoveryReason = executor.ErrDispatchActive.Error()
	}
	finalStatus, err := b.evaluateBuildTerminalStatus(ctx, build, chunks)
	if err != nil {
		return reconcileStepOutput{}, err
	}
	build.Status = finalStatus

	return reconcileStepOutput{
		BuildStatus:    finalStatus,
		Error:          recoveryReason,
		LaunchedChunks: launchedChunks,
		LaunchedMap:    launchedMap,
		State:          &model.BuildObjectState{Build: *build, Chunks: chunks, Launched: launchedMap, TerminalNotified: finalStatus == statusCompleted || finalStatus == statusFailed},
	}, nil
}
