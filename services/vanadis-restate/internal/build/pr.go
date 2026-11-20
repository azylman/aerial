package build

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
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

// GenerateBuildID delegates to the canonical internal/model helper.
func GenerateBuildID() string {
	return model.GenerateBuildID()
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
			Reason:     store.FirstNonEmpty(ev.Error, "dispatch outcome failed"),
		}
	default:
		return nil
	}
}

func isActionCompletionMutation(mutation *BuildMutationRequest, chunks []model.VanadisChunk) bool {
	if mutation == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(mutation.Action)) {
	case mutationActionCompleted, mutationChunkDeploySuccess:
		for _, chunk := range chunks {
			if chunk.ID == mutation.ChunkID && chunk.Mode == "action" && chunk.PRNumber == 0 {
				return true
			}
		}
	}
	return false
}

func isFailureCompletionMutation(mutation *BuildMutationRequest, chunks []model.VanadisChunk) bool {
	if mutation == nil {
		return false
	}
	if strings.ToLower(strings.TrimSpace(mutation.Action)) != mutationChunkFailed {
		return false
	}
	for _, chunk := range chunks {
		if chunk.ID == mutation.ChunkID {
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
	if deploying.Status == statusFailed || deploying.Status == statusCancelled || build.Status == statusCancelled {
		return nil
	}
	if err := b.store.CompletePRDeployment(ctx, mutation.ChunkID, mutation.RolloutState); err != nil {
		if errors.Is(err, errPRTransitionSkipped) {
			return nil
		}
		return err
	}
	if mutation.RolloutState != nil {
		// Mirror the evidence just stored: the deploying snapshot above
		// predates this write, and the cross-build sweep judges shared
		// deploy jobs from the deploying chunk.
		deploying.RolloutState = mutation.RolloutState
	}
	if b.executor != nil && !alreadyCompleted {
		if cErr := b.executor.CancelChunkDispatch(ctx, mutation.ChunkID); cErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", mutation.ChunkID, "error", cErr)
		}
		if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{mutation.ChunkID}, ""); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
		}
	}
	if err := b.completeSweptDeployments(ctx, chunks, mutation.ChunkID); err != nil {
		return err
	}
	if err := b.completeCrossBuildSweptDeployments(ctx, deploying); err != nil {
		return err
	}

	return nil
}

// completeCrossBuildSweptDeployments completes eligible earlier chunks in
// other builds superseded by the deploying chunk's landed commit, including
// already-completed chunks re-derived for retry recovery. Skewed or
// concurrent transitions surface as skipped writes inside the store sweep;
// other errors return after processing partial progress so the journaled
// step retries the remainder. Per affected non-terminal foreign build it
// publishes a completed event per swept chunk, re-syncs the build widget
// from the store, and applies the existing build-completion transition.
func (b *BuildObject) completeCrossBuildSweptDeployments(ctx context.Context, deploying model.VanadisChunk) error {
	swept, recovered, err := b.store.SweepEarlierDeploymentsAcrossBuilds(ctx, deploying)
	byBuild := make(map[string][]string)
	var buildOrder []string
	for _, sw := range append(swept, recovered...) {
		if _, ok := byBuild[sw.BuildID]; !ok {
			buildOrder = append(buildOrder, sw.BuildID)
		}
		byBuild[sw.BuildID] = append(byBuild[sw.BuildID], sw.ChunkID)
	}
	for _, buildID := range buildOrder {
		build, foreignChunks, getErr := b.store.GetBuild(ctx, buildID)
		if getErr != nil {
			return getErr
		}
		if build == nil || build.Status == statusCancelled || build.Status == statusCompleted {
			continue
		}
		if b.executor != nil {
			for _, sweptID := range byBuild[buildID] {
				if pubErr := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{sweptID}, ""); pubErr != nil {
					slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pubErr)
				}
			}
		}
		if build.Status == statusFailed {
			b.refreshFailedPRWidget(ctx, buildID)
			continue
		}
		if _, evalErr := b.evaluateBuildTerminalStatus(ctx, build, foreignChunks); evalErr != nil {
			return evalErr
		}
	}
	return err
}

// completeSweptDeployments completes earlier chunks superseded by the
// deploying chunk's landed commit, publishing a completed event per chunk.
// Skipped transitions (a concurrent completion, failure, or cancellation)
// are harmless; other errors abort so the journaled step retries the sweep.
func (b *BuildObject) completeSweptDeployments(ctx context.Context, chunks []model.VanadisChunk, chunkID string) error {
	for _, sweptID := range store.EarlierDeploymentSweepIDs(chunks, chunkID) {
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
	_, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		if c.ID == chunkID && c.BuildID == buildID {
			return nil
		}
	}
	return ErrChunkNotFound
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
		if bld != nil && chk != nil && chk.Status == statusFailed && bld.Status == statusFailed && (chk.Mode != chunkModeAction || actionFailureAccepted(*chk, bld.Status, mutation.DispatchID)) {
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

func (b *BuildObject) dispatchPREvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if event == "deploy_success" {
		return b.applyMutation(rc, buildID, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: chunkID, RolloutState: ev.RolloutState})
	}
	return b.handlePREventType(rc, buildID, chunkID, event, ev)
}

func (b *BuildObject) recordDeploymentPREvent(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (bool, error) {
	chunkID, cErr := b.locateOrAttachPRChunk(rc, buildID, ev)
	if cErr != nil || chunkID == "" {
		return false, cErr
	}
	rolloutStatus := ""
	event := strings.ToLower(strings.TrimSpace(ev.Event))
	if event == prEventDeployStarted {
		valid, status, e := b.recordDeploymentStart(rc, buildID, chunkID, ev)
		rolloutStatus = status
		if e != nil || !valid {
			return false, e
		}
	}

	eventErr := b.dispatchPREvent(rc, buildID, chunkID, event, ev)
	if eventErr != nil {
		if errors.Is(eventErr, errPRTransitionSkipped) {
			return false, nil
		}
		return false, eventErr
	}
	b.refreshFailedPRWidget(rc, buildID)
	if event != "deploy_success" && ev.RolloutState != nil {
		if rErr := b.store.RecordPRRolloutState(rc, chunkID, rolloutStatus, ev.RolloutState); rErr != nil {
			if event == prEventDeployStarted {
				return false, rErr
			}
			slog.WarnContext(rc, "[vanadis-restate] record rollout state warning", "error", rErr)
		}
		b.refreshFailedPRWidget(rc, buildID)
	}
	return true, nil
}

// OnPREvent receives PR lifecycle events for this build.
func (b *BuildObject) OnPREvent(ctx restate.ObjectContext, ev pubsub.PRLifecycleEvent) error {
	if b.store == nil {
		return nil
	}
	buildID := restate.Key(ctx)
	_, err := RunJournaled(ctx, labelOnPREvent, "handle-pr-event-postgres", func(rc restate.RunContext) (bool, error) {
		return b.recordDeploymentPREvent(rc, buildID, ev)
	})
	if err != nil {
		return err
	}
	rt := b.getRuntime(ctx)
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
	applied, err := RunJournaled(ctx, labelOnDispatchOutcome, "handle-dispatch-outcome-postgres", func(rc restate.RunContext) (bool, error) {
		chunkID := strings.TrimSpace(ev.VanadisChunkID)
		if chunkID == "" {
			return false, nil
		}
		status := strings.ToLower(strings.TrimSpace(ev.Status))
		switch status {
		case "completed", dispatchStatusSuccess:
			if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
				return false, err
			}
			res, err := b.store.CompleteActionChunk(rc, chunkID, ev.DispatchID)
			if err != nil {
				return false, err
			}
			return res != nil && res.CompletionAccepted, nil
		case statusFailed, statusTimeout, statusCrashed:
			reason := store.FirstNonEmpty(ev.Error, "dispatch outcome failed")
			if err := b.validateActionBuild(rc, restate.Key(ctx), chunkID); err != nil {
				return false, err
			}
			_, _, failed, err := b.store.FailChunk(rc, chunkID, reason, ev.DispatchID)
			return failed, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if !applied {
		if b.executor != nil && isCompletedDispatchStatus(ev.Status) {
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

// noPRAction aliases the prflow recovery decision so existing callers keep
// compiling against the thin build surface.
type noPRAction = prflow.NoPRAction

const (
	noPRActionNone   = prflow.NoPRActionNone
	noPRActionResume = prflow.NoPRActionResume
	noPRActionFail   = prflow.NoPRActionFail
)

const noPRFailureReason = prflow.NoPRFailureReason

func isCompletedDispatchStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusCompleted, dispatchStatusSuccess:
		return true
	}
	return false
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
		action := noPRTurnAction(chunks[idx], bld.Status, ev.DispatchID)
		if action == noPRActionResume {
			if err := b.resumeNoPRChunk(rc, bld, chunks, idx); err != nil {
				return noPRActionNone, err
			}
		}
		return action, nil
	})
	if err != nil {
		return err
	}
	if action != noPRActionFail {
		return nil
	}
	mutation := &BuildMutationRequest{Action: mutationChunkFailed, ChunkID: chunkID, DispatchID: ev.DispatchID, Reason: noPRFailureReason}
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
