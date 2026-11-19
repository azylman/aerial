package build

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	prEventOpened = "opened"
)

func CanFallbackThreadRepoLookup(ev pubsub.PRLifecycleEvent) bool {
	if strings.TrimSpace(ev.TargetID) == "" || ev.Quiet {
		return false
	}
	branch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	return !strings.HasPrefix(branch, "dependabot/")
}

func normalizeDiscordThreadID(s string) string {
	clean := strings.TrimSpace(s)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func ExtractChunkIDFromBranch(repo, branch string) string {
	repoName := strings.TrimSuffix(cleanRepoName(repo), ".git")
	if repoName == "" {
		return ""
	}
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	after, ok := strings.CutPrefix(branch, "feat/"+repoName+"-pr-vanadis-")
	if !ok {
		return ""
	}
	chunkID, suffix, hasSuffix := strings.CutLast(after, "-")
	if !hasSuffix || len(suffix) != 6 {
		return ""
	}
	for i := range suffix {
		if !isHexDigit(suffix[i]) {
			return ""
		}
	}
	if !store.IsValidVanadisChunkID(chunkID) {
		return ""
	}
	return chunkID
}

func GenerateBuildID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("build-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

func cleanRepoName(repo string) string {
	repo = strings.TrimSpace(repo)
	if idx := strings.LastIndex(repo, "/"); idx >= 0 {
		return repo[idx+1:]
	}
	return repo
}

func actionDispatchID(chunk model.VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

// actionCompletionAllowed mirrors the frozen internal/store copy byte-for-byte;
// router.go stays frozen until the facade-removal chunk unifies both copies.
func actionCompletionAllowed(chunk model.VanadisChunk, buildStatus, dispatchID string) bool {
	identityMatches := dispatchID == actionDispatchID(chunk) || (dispatchID == "" && chunk.StallNudges == 0)
	return chunk.Mode == chunkModeAction && chunk.Status == statusRunning && buildStatus == statusRunning && chunk.PRNumber == 0 && identityMatches
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
	return actionFailureAllowed(c, buildStatus, dispatchID)
}

func actionFailureAllowed(c model.VanadisChunk, buildStatus, dispatchID string) bool {
	if c.PRNumber == 0 {
		return actionCompletionAllowed(c, buildStatus, dispatchID)
	}
	// PR attachment retains action mode. PR closure and remediation retain their
	// identities, while old action-turn identities must still match the generation.
	prIdentity := dispatchID == "" || strings.HasPrefix(dispatchID, "vanadis-"+c.ID+"-cifail-") ||
		dispatchID == "vanadis-conflict-"+c.ID || strings.HasPrefix(dispatchID, "vanadis-conflict-"+c.ID+"-")
	return buildStatus == statusRunning && c.Status != statusMerged && c.Status != statusDeploying && c.Status != statusDeployFailed &&
		(prIdentity || dispatchID == actionDispatchID(c))
}

const dispatchStatusSuccess = "success"

const mutationActionCompleted = "action_completed"

const mutationChunkDeploySuccess = "chunk_deploy_success"

var errActionCompletionSkipped = errors.New("action completion not applied")

func (b *BuildObject) completePRDeploymentMutation(ctx context.Context, buildID string, mutation *BuildMutationRequest) error {
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	alreadyCompleted := false
	found := false
	for _, chunk := range chunks {
		if chunk.ID != mutation.ChunkID {
			continue
		}
		found = true
		alreadyCompleted = chunk.Status == statusCompleted
		if chunk.Mode == "action" && chunk.PRNumber == 0 {
			return b.completeActionDeploymentMutation(ctx, mutation)
		}
		if chunk.Status == statusFailed || chunk.Status == statusCancelled || build.Status == statusCancelled || (build.Status == statusFailed && chunk.Status != statusCompleted) {
			return nil
		}
	}
	if !found {
		return ErrChunkNotFound
	}
	if build.Status == statusCancelled {
		return nil
	}
	if err := b.store.CompletePRDeployment(ctx, mutation.ChunkID, mutation.RolloutState); err != nil {
		if errors.Is(err, errPRTransitionSkipped) {
			return nil
		}
		return err
	}
	if b.executor != nil && !alreadyCompleted {
		if cErr := b.executor.CancelChunkDispatch(ctx, mutation.ChunkID); cErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", mutation.ChunkID, "error", cErr)
		}
		if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", "completed", []string{mutation.ChunkID}, ""); err != nil {
			slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
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

type prHeadVerifiedContextKey = store.PrHeadVerifiedContextKey

func (b *BuildObject) recordPRHeadRevision(ctx context.Context, chunkID string, ev pubsub.PRLifecycleEvent) error {
	headSHA := strings.TrimSpace(ev.HeadSHA)
	if headSHA != "" && b.executor != nil {
		if err := b.executor.ValidatePRHead(ctx, ev.Repo, ev.PRNumber, headSHA); err != nil {
			return err
		}
		ctx = context.WithValue(ctx, prHeadVerifiedContextKey{}, true)
	}
	if b.store == nil {
		return nil
	}
	return b.store.ResetPRCheckState(ctx, chunkID, headSHA, "")
}

// recordPRMergeRevision follows an accepted merged lifecycle transition.
func (b *BuildObject) recordPRMergeRevision(ctx context.Context, chunkID, rawHeadSHA, rawMergeSHA string) error {
	mergeSHA := strings.TrimSpace(rawMergeSHA)
	if mergeSHA == "" {
		return nil
	}
	if b.store == nil {
		return nil
	}
	return b.store.ResetPRCheckState(ctx, chunkID, strings.TrimSpace(rawHeadSHA), mergeSHA)
}

func (b *BuildObject) handlePRCheckEvent(ctx context.Context, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	checkName := strings.TrimSpace(ev.CheckName)
	if checkName == "" {
		return nil
	}
	checkState := strings.TrimSpace(ev.CheckState)
	if event == "check_failed" || event == "ci_failed" {
		checkState = "failed"
	}
	if checkState == "" {
		return nil
	}
	if b.store == nil {
		return nil
	}
	return b.store.RecordPRCheckStatus(ctx, chunkID, strings.TrimSpace(ev.HeadSHA), checkName, checkState)
}

func (b *BuildObject) lookupBuildChunk(rc restate.RunContext, buildID, chunkID string) (*model.VanadisBuild, model.VanadisChunk, error) {
	bld, chunks, err := b.store.GetBuild(rc, buildID)
	if err != nil {
		return nil, model.VanadisChunk{}, err
	}
	chunk := model.VanadisChunk{ID: chunkID, BuildID: buildID, Effort: effortMedium}
	for _, c := range chunks {
		if c.ID == chunkID {
			chunk = c
			chunk.Effort = store.CapChunkEffort(c.Effort)
			break
		}
	}
	return bld, chunk, nil
}

func (b *BuildObject) dispatchChunkRemediationForChunk(rc restate.RunContext, bld *model.VanadisBuild, chunk model.VanadisChunk, dispatchID, prompt string, skipIfActive ...bool) error {
	skip := true
	if len(skipIfActive) > 0 {
		skip = skipIfActive[0]
	}
	return b.executor.DispatchChunkRemediation(rc, executor.BrainDispatchRequest{
		ID:             dispatchID,
		Source:         "vanadis-restate",
		SessionID:      "vanadis-" + strings.TrimSpace(chunk.ID),
		TargetID:       strings.TrimSpace(bld.ThreadID),
		VanadisChunkID: strings.TrimSpace(chunk.ID),
		Prompt:         prompt,
		Effort:         store.CapChunkEffort(chunk.Effort),
		SkipIfActive:   skip,
	})
}

func (b *BuildObject) handleCIRemediationCapReached(rc restate.RunContext, chunkID string, failCount int, ev pubsub.PRLifecycleEvent) error {
	bld, chk, failed, err := b.store.FailChunk(rc, chunkID, "ci_remediation_cap_reached", "")
	if err != nil {
		return err
	}
	if !failed || bld == nil || b.executor == nil {
		return nil
	}
	if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
	}
	if !bld.Quiet {
		targetID := strings.TrimSpace(bld.ThreadID)
		content := fmt.Sprintf("🛑 [%s#%d] CI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", ev.Repo, ev.PRNumber, ev.CheckName, failCount, executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
		if mErr := b.executor.DispatchDirectMessage(rc, targetID, content); mErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] direct alert message warning", "error", mErr)
		}
	}
	capBanner := fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	chunks := []model.VanadisChunk{}
	if chk != nil {
		chunks = append(chunks, *chk)
	}
	if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *bld, chunks, capBanner); sErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] sync build widget warning", "error", sErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "failed", []string{chunkID}, "ci_remediation_cap_reached"); pErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] publish event warning", "error", pErr)
	}
	if pErr := b.executor.PublishExternalEvent(rc, "vanadis_build", "failed", []string{bld.ID}, "ci_remediation_cap_reached"); pErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] publish event warning", "error", pErr)
	}
	return nil
}

func (b *BuildObject) handleConflictEvent(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if b.store == nil {
		return nil
	}
	bld, chunk, err := b.lookupBuildChunk(rc, buildID, chunkID)
	if err != nil {
		return err
	}
	if head := strings.TrimSpace(ev.HeadSHA); head != "" && chunk.HeadSHA != "" && !strings.EqualFold(chunk.HeadSHA, head) {
		return errPRTransitionSkipped
	}
	if err = b.store.UpdateChunkBannerAndStatus(rc, chunkID, statusConflict, "Merge conflict detected"); err != nil {
		return err
	}
	if b.executor == nil {
		return nil
	}
	dispatchID := executor.ConflictRemediationDispatchID(chunkID, ev.HeadSHA)
	err = b.dispatchChunkRemediationForChunk(rc, bld, chunk, dispatchID, executor.FormatConflictPrompt(ev))
	if errors.Is(err, executor.ErrDispatchActive) {
		return nil
	}
	return err
}

func (b *BuildObject) handleCIFailureEvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if executor.IsGateAggregatorCheck(ev.CheckName) {
		return nil
	}
	if err := b.handlePRCheckEvent(rc, chunkID, event, ev); err != nil {
		return err
	}
	banner := fmt.Sprintf("CI check %q failed", ev.CheckName)
	if b.store != nil {
		if err := b.store.UpdateChunkBannerAndStatus(rc, chunkID, statusCIFailed, banner); err != nil {
			return err
		}
	}
	if b.executor == nil {
		return nil
	}
	bld, chunk, err := b.lookupBuildChunk(rc, buildID, chunkID)
	if err != nil {
		return err
	}
	cleanHead := strings.TrimSpace(ev.HeadSHA)
	if chunk.CIFailCount >= executor.MaxCIRemediationAttempts {
		var failCount int
		if b.store != nil {
			failCount, err = b.store.IncrementChunkCIFailCount(rc, chunkID, banner, cleanHead)
			if err != nil {
				return err
			}
		}
		return b.handleCIRemediationCapReached(rc, chunkID, failCount, ev)
	}
	details, err := b.executor.ResolveCIFailureDetailsChecked(rc, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
	if err != nil {
		return err
	}
	dispatchID := executor.CIRemediationDispatchID(chunkID, ev.HeadSHA, chunk.CIFailCount+1)
	skipIfActive := cleanHead == "" || strings.EqualFold(strings.TrimSpace(chunk.RemediatedSHA), cleanHead)
	if err = b.dispatchChunkRemediationForChunk(rc, bld, chunk, dispatchID, executor.FormatCIFailurePrompt(ev, details), skipIfActive); err != nil {
		if errors.Is(err, executor.ErrDispatchActive) {
			return nil
		}
		return err
	}
	if b.store != nil {
		if _, err := b.store.IncrementChunkCIFailCount(rc, chunkID, banner, cleanHead); err != nil {
			return err
		}
	}
	return nil
}

func (b *BuildObject) handleClosedPREvent(rc restate.RunContext, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if ev.Status == "merged" || b.store == nil {
		return nil
	}
	bld, _, failed, fErr := b.store.FailChunk(rc, chunkID, "pr_closed_unmerged", "")
	if fErr != nil {
		return fErr
	}
	if failed && bld != nil && bld.Status == statusFailed && b.executor != nil {
		if err := b.notifyChunkFailure(rc, *bld, chunkID, "pr_closed_unmerged"); err != nil {
			return err
		}
	}
	return nil
}

func (b *BuildObject) handleDeployFailureEvent(rc restate.RunContext, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	banner := executor.FormatDeployFailureBanner(ev)
	if b.store != nil {
		if err := b.store.UpdateChunkBannerAndStatus(rc, chunkID, "deploy_failed", banner); err != nil {
			return err
		}
	}
	if b.executor != nil {
		if err := b.dispatchThreadWakePrompt(rc, buildID, executor.FormatDeployFailurePrompt(ev)); err != nil {
			return err
		}
	}
	return nil
}

func (b *BuildObject) handlePREventType(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	switch event {
	case prEventOpened, "synchronize":
		return b.recordPRHeadRevision(rc, chunkID, ev)
	case "check_updated":
		return b.handlePRCheckEvent(rc, chunkID, event, ev)
	case "check_failed", "ci_failed":
		return b.handleCIFailureEvent(rc, buildID, chunkID, event, ev)
	case "conflict":
		return b.handleConflictEvent(rc, buildID, chunkID, ev)
	case "merged":
		if b.store != nil {
			if err := b.store.UpdatePRLifecycleStatus(rc, chunkID, "merged"); err != nil {
				return err
			}
		}
		if err := b.recordPRMergeRevision(rc, chunkID, ev.HeadSHA, ev.MergeSHA); err != nil {
			return err
		}
		if b.executor != nil {
			if cErr := b.executor.CancelChunkDispatch(rc, chunkID); cErr != nil {
				slog.WarnContext(rc, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
			}
			if pErr := b.executor.PublishExternalEvent(rc, "vanadis_chunk", "merged", []string{chunkID}, ""); pErr != nil {
				slog.WarnContext(rc, "[vanadis-restate] publish event warning", "error", pErr)
			}
		}
	case "closed", "closed_unmerged":
		return b.handleClosedPREvent(rc, chunkID, ev)
	case "deploy_failed", "sync_failed":
		return b.handleDeployFailureEvent(rc, buildID, chunkID, ev)
	}
	return nil
}

func (b *BuildObject) locateOrAttachPRChunk(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	lookup, err := b.store.GetChunkByPR(rc, ev.Repo, ev.PRNumber)
	if err != nil {
		return "", err
	}
	if lookup != nil && lookup.Matched {
		return lookup.ChunkID, nil
	}
	return b.resolveOrAttachPRChunk(rc, buildID, ev)
}

func (b *BuildObject) dispatchPREvent(rc restate.RunContext, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if event == "deploy_success" {
		return b.applyMutation(rc, buildID, &BuildMutationRequest{Action: mutationChunkDeploySuccess, ChunkID: chunkID, RolloutState: ev.RolloutState})
	}
	return b.handlePREventType(rc, buildID, chunkID, event, ev)
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

func isEligibleUnattachedChunk(build *model.VanadisBuild, c *model.VanadisChunk, ev pubsub.PRLifecycleEvent) bool {
	if build == nil || c == nil {
		return false
	}
	if (c.Mode != "" && c.Mode != "pr") || c.PRNumber != 0 || c.StartedAt.IsZero() {
		return false
	}
	if c.Status != statusRunning && c.Status != statusCoding {
		return false
	}
	if buildThread := normalizeDiscordThreadID(build.ThreadID); buildThread != "" && strings.TrimSpace(ev.TargetID) != "" && normalizeDiscordThreadID(ev.TargetID) != buildThread {
		return false
	}
	return store.AttachmentMatches(*c, *build, store.AttachChunkPRRequest{Repo: strings.TrimSpace(ev.Repo), PRNumber: ev.PRNumber})
}

func (b *BuildObject) syncAttachedPRWidget(rc restate.RunContext, buildID string) {
	if b.executor == nil {
		return
	}
	refreshedBuild, refreshedChunks, rErr := b.store.GetBuild(rc, buildID)
	if rErr == nil && refreshedBuild != nil {
		if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *refreshedBuild, refreshedChunks, ""); sErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] sync build widget warning", "error", sErr)
		}
	}
}

func selectUnattachedTargetChunk(build *model.VanadisBuild, chunks []model.VanadisChunk, ev pubsub.PRLifecycleEvent) *model.VanadisChunk {
	if build == nil {
		return nil
	}
	if chunkID := ExtractChunkIDFromBranch(ev.Repo, ev.Branch); chunkID != "" {
		for i := range chunks {
			if chunks[i].ID == chunkID && chunks[i].BuildID == build.ID {
				if isEligibleUnattachedChunk(build, &chunks[i], ev) {
					return &chunks[i]
				}
				return nil
			}
		}
		return nil
	}
	evThread := normalizeDiscordThreadID(ev.TargetID)
	buildThread := normalizeDiscordThreadID(build.ThreadID)
	if !CanFallbackThreadRepoLookup(ev) || evThread == "" || buildThread == "" || evThread != buildThread {
		return nil
	}
	var matched *model.VanadisChunk
	for i := range chunks {
		if chunks[i].BuildID == build.ID && isEligibleUnattachedChunk(build, &chunks[i], ev) {
			if matched != nil {
				return nil
			}
			matched = &chunks[i]
		}
	}
	return matched
}

func (b *BuildObject) resolveOrAttachPRChunk(rc restate.RunContext, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	if b.executor == nil || (ExtractChunkIDFromBranch(ev.Repo, ev.Branch) == "" && !CanFallbackThreadRepoLookup(ev)) {
		return "", nil
	}

	build, chunks, gErr := b.store.GetBuild(rc, buildID)
	if gErr != nil {
		if errors.Is(gErr, store.ErrBuildNotFound) {
			return "", nil
		}
		return "", gErr
	}

	targetChunk := selectUnattachedTargetChunk(build, chunks, ev)
	if targetChunk == nil {
		return "", nil
	}
	if vErr := b.executor.ValidateChunkPRAttachment(rc, *build, *targetChunk, ev); vErr != nil {
		if errors.Is(vErr, errPRTransitionSkipped) {
			return "", nil
		}
		return "", vErr
	}

	_, attachedChunkID, _, aErr := b.store.AttachChunkPR(rc, store.AttachChunkPRRequest{
		ChunkID:  targetChunk.ID,
		PRNumber: ev.PRNumber,
		Repo:     strings.TrimSpace(ev.Repo),
		Branch:   ev.Branch,
		Title:    ev.Title,
		TargetID: ev.TargetID,
		Quiet:    ev.Quiet,
	})
	if aErr != nil {
		if errors.Is(aErr, store.ErrBuildConflict) || errors.Is(aErr, ErrChunkNotFound) {
			return "", nil
		}
		return "", aErr
	}

	b.syncAttachedPRWidget(rc, buildID)
	return attachedChunkID, nil
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

type noPRAction string

const (
	noPRActionNone   noPRAction = "none"
	noPRActionResume noPRAction = "resume"
	noPRActionFail   noPRAction = "fail"
)

const noPRFailureReason = "chunk turn ended without opening a PR (after one automatic resume)"

// noPRResumeDispatchID identifies the single automatic resume turn for a PR chunk.
func noPRResumeDispatchID(chunkID string) string {
	return fmt.Sprintf("vanadis-%s-nopr-resume-1", strings.TrimSpace(chunkID))
}

func isCompletedDispatchStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusCompleted, dispatchStatusSuccess:
		return true
	}
	return false
}

// noPRTurnAction decides how to react when a PR chunk's turn completes. The
// submit script attaches PRs synchronously, so a completed launch or resume turn
// on a still-running chunk without a PR means the turn ended without one.
func noPRTurnAction(chunk model.VanadisChunk, buildStatus, dispatchID string) noPRAction {
	if chunk.Mode == chunkModeAction || chunk.PRNumber != 0 || buildStatus != statusRunning {
		return noPRActionNone
	}
	if chunk.Status != statusRunning && chunk.Status != statusCoding {
		return noPRActionNone
	}
	switch dispatchID {
	case actionDispatchID(chunk):
		return noPRActionResume
	case noPRResumeDispatchID(chunk.ID):
		return noPRActionFail
	}
	return noPRActionNone
}

func findChunkIndex(chunks []model.VanadisChunk, chunkID string) int {
	for i := range chunks {
		if chunks[i].ID == chunkID {
			return i
		}
	}
	return -1
}

func (b *BuildObject) resumeNoPRChunk(rc restate.RunContext, bld *model.VanadisBuild, chunks []model.VanadisChunk, idx int) error {
	chunk := chunks[idx]
	chunkPrompt, err := b.executor.FormatChunkPrompt(*bld, chunks, idx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}
	chunk.Effort = store.CapChunkEffort(chunk.Effort)
	// eri dedupes by dispatch ID, so never skip: a dropped resume would stall the chunk.
	if err := b.dispatchChunkRemediationForChunk(rc, bld, chunk, noPRResumeDispatchID(chunk.ID), executor.FormatNoPRResumePrompt(chunk, chunkPrompt), false); err != nil {
		return err
	}
	banner := fmt.Sprintf("Chunk %d turn ended without a PR — automatic resume dispatched", chunk.ChunkIndex+1)
	if _, sErr := b.executor.SyncBuildWidget(rc, b.store, *bld, chunks, banner); sErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] sync build widget warning", "error", sErr)
	}
	return nil
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
func (b *BuildObject) refreshFailedPRWidget(ctx context.Context, buildID, event string) {
	if b.executor == nil || (event != prEventOpened && event != "synchronize") {
		return
	}
	build, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] recovery widget read warning", "error", err)
		return
	}
	if build.Status != statusFailed {
		return
	}
	if _, err = b.executor.SyncBuildWidget(ctx, b.store, *build, chunks, "Build failed"); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] recovery widget sync warning", "error", err)
	}
}
