package prflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Frozen lifecycle vocabulary mirrored from internal/build/requests.go so wire
// values (statuses, event names) stay byte-identical across the extraction.
const (
	prEventOpened  = "opened"
	statusRunning  = "running"
	statusCoding   = "coding"
	statusMerged   = "merged"
	statusFailed   = "failed"
	statusConflict = "conflict"
	statusCIFailed = "ci_failed"

	chunkModeAction = "action"
	effortMedium    = "medium"
)

// ErrPRTransitionSkipped aliases the store package sentinel so errors.Is
// identity holds wherever the lifecycle transition executes.
var ErrPRTransitionSkipped = store.ErrPRTransitionSkipped

// ActionDispatchID identifies the launch turn dispatch for a chunk.
func ActionDispatchID(chunk model.VanadisChunk) string {
	return fmt.Sprintf("vanadis-%s-turn-%d", chunk.ID, chunk.StallNudges)
}

// Flow executes PR lifecycle transitions against the durable store and the
// executor side-effect surface.
type Flow struct {
	Store store.VanadisStore
	Exec  *executor.Executor
}

// NewFlow binds a Flow to the given store and side-effect executor.
func NewFlow(st store.VanadisStore, exec *executor.Executor) *Flow {
	return &Flow{Store: st, Exec: exec}
}

// RecordPRHeadRevision validates a synchronize/opened head SHA against live
// GitHub state and records the accepted revision.
func (f *Flow) RecordPRHeadRevision(ctx context.Context, chunkID string, ev pubsub.PRLifecycleEvent) error {
	headSHA := strings.TrimSpace(ev.HeadSHA)
	if headSHA != "" && f.Exec != nil {
		if err := f.Exec.ValidatePRHead(ctx, ev.Repo, ev.PRNumber, headSHA); err != nil {
			return err
		}
		ctx = context.WithValue(ctx, store.PrHeadVerifiedContextKey{}, true)
	}
	if f.Store == nil {
		return nil
	}
	return f.Store.ResetPRCheckState(ctx, chunkID, headSHA, "")
}

// RecordPRMergeRevision follows an accepted merged lifecycle transition.
func (f *Flow) RecordPRMergeRevision(ctx context.Context, chunkID, rawHeadSHA, rawMergeSHA string) error {
	mergeSHA := strings.TrimSpace(rawMergeSHA)
	if mergeSHA == "" {
		return nil
	}
	if f.Store == nil {
		return nil
	}
	return f.Store.ResetPRCheckState(ctx, chunkID, strings.TrimSpace(rawHeadSHA), mergeSHA)
}

// HandlePRCheckEvent records a CI check state transition for a chunk.
func (f *Flow) HandlePRCheckEvent(ctx context.Context, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
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
	if f.Store == nil {
		return nil
	}
	return f.Store.RecordPRCheckStatus(ctx, chunkID, strings.TrimSpace(ev.HeadSHA), checkName, checkState)
}

// LookupBuildChunk loads the build and the named chunk with capped effort.
func (f *Flow) LookupBuildChunk(ctx context.Context, buildID, chunkID string) (*model.VanadisBuild, model.VanadisChunk, error) {
	bld, chunks, err := f.Store.GetBuild(ctx, buildID)
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

// DispatchChunkRemediationForChunk dispatches a remediation prompt for a chunk.
func (f *Flow) DispatchChunkRemediationForChunk(ctx context.Context, bld *model.VanadisBuild, chunk model.VanadisChunk, dispatchID, prompt string, skipIfActive ...bool) error {
	skip := true
	if len(skipIfActive) > 0 {
		skip = skipIfActive[0]
	}
	return f.Exec.DispatchChunkRemediation(ctx, executor.BrainDispatchRequest{
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

// notifyChunkFailure cancels active work and fans out failure notifications.
func (f *Flow) notifyChunkFailure(ctx context.Context, bld model.VanadisBuild, chunkID, reason string) error {
	if cErr := f.Exec.CancelChunkDispatch(ctx, chunkID); cErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
	}
	buildObj := bld
	var chunks []model.VanadisChunk
	if f.Store != nil {
		fetched, fetchedChunks, gErr := f.Store.GetBuild(ctx, bld.ID)
		if gErr != nil {
			return fmt.Errorf("get build %s for failure notification: %w", bld.ID, gErr)
		}
		if fetched != nil {
			buildObj = *fetched
			chunks = fetchedChunks
		}
	}
	var widgetErr error
	if _, sErr := f.Exec.SyncBuildWidget(ctx, f.Store, buildObj, chunks, "Build failed: "+reason); sErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync widget warning", "error", sErr)
		widgetErr = sErr
	}
	var wakeErr error
	if !buildObj.Quiet {
		prompt := fmt.Sprintf("[VANADIS BUILD FAILED]\nBuild ID: %s\nObjective: %s\n\nChunk failed: %s", buildObj.ID, buildObj.Objective, reason)
		if dErr := f.Exec.DispatchWakePrompt(ctx, buildObj.ThreadID, prompt); dErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] dispatch wake prompt warning", "error", dErr)
			wakeErr = dErr
		}
	}
	if pErr := f.Exec.PublishExternalEvent(ctx, "vanadis_chunk", "failed", []string{chunkID}, reason); pErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
	}
	if pErr := f.Exec.PublishExternalEvent(ctx, "vanadis_build", "failed", []string{buildObj.ID}, reason); pErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
	}
	return errors.Join(widgetErr, wakeErr)
}

// dispatchThreadWakePrompt wakes the build thread with a prompt.
func (f *Flow) dispatchThreadWakePrompt(ctx context.Context, buildID, prompt string) error {
	if f.Store == nil {
		return nil
	}
	bld, _, gErr := f.Store.GetBuild(ctx, buildID)
	if gErr != nil {
		return gErr
	}
	if bld == nil {
		return store.ErrBuildNotFound
	}
	if dErr := f.Exec.DispatchWakePrompt(ctx, bld.ThreadID, prompt); dErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] wake prompt warning", "error", dErr)
		return dErr
	}
	return nil
}

// HandleCIRemediationCapReached fails the chunk once CI remediation attempts
// are exhausted and fans out the human-review alert.
func (f *Flow) HandleCIRemediationCapReached(ctx context.Context, chunkID string, failCount int, ev pubsub.PRLifecycleEvent) error {
	bld, chk, failed, err := f.Store.FailChunk(ctx, chunkID, "ci_remediation_cap_reached", "")
	if err != nil {
		return err
	}
	if !failed || bld == nil || f.Exec == nil {
		return nil
	}
	if cErr := f.Exec.CancelChunkDispatch(ctx, chunkID); cErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
	}
	if !bld.Quiet {
		targetID := strings.TrimSpace(bld.ThreadID)
		content := fmt.Sprintf("🛑 [%s#%d] CI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", ev.Repo, ev.PRNumber, ev.CheckName, failCount, executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
		if mErr := f.Exec.DispatchDirectMessage(ctx, targetID, content); mErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] direct alert message warning", "error", mErr)
		}
	}
	capBanner := fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", executor.MaxCIRemediationAttempts, executor.MaxCIRemediationAttempts)
	chunks := []model.VanadisChunk{}
	if chk != nil {
		chunks = append(chunks, *chk)
	}
	if _, sErr := f.Exec.SyncBuildWidget(ctx, f.Store, *bld, chunks, capBanner); sErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync build widget warning", "error", sErr)
	}
	if pErr := f.Exec.PublishExternalEvent(ctx, "vanadis_chunk", "failed", []string{chunkID}, "ci_remediation_cap_reached"); pErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
	}
	if pErr := f.Exec.PublishExternalEvent(ctx, "vanadis_build", "failed", []string{bld.ID}, "ci_remediation_cap_reached"); pErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
	}
	return nil
}

// HandleConflictEvent marks the chunk conflicted and dispatches a
// conflict-remediation turn unless the head is superseded.
func (f *Flow) HandleConflictEvent(ctx context.Context, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if f.Store == nil {
		return nil
	}
	bld, chunk, err := f.LookupBuildChunk(ctx, buildID, chunkID)
	if err != nil {
		return err
	}
	if head := strings.TrimSpace(ev.HeadSHA); head != "" && chunk.HeadSHA != "" && !strings.EqualFold(chunk.HeadSHA, head) {
		return ErrPRTransitionSkipped
	}
	if err = f.Store.UpdateChunkBannerAndStatus(ctx, chunkID, statusConflict, "Merge conflict detected"); err != nil {
		return err
	}
	if f.Exec == nil {
		return nil
	}
	dispatchID := executor.ConflictRemediationDispatchID(chunkID, ev.HeadSHA)
	err = f.DispatchChunkRemediationForChunk(ctx, bld, chunk, dispatchID, executor.FormatConflictPrompt(ev))
	if errors.Is(err, executor.ErrDispatchActive) {
		return nil
	}
	return err
}

// HandleCIFailureEvent records a CI failure and orchestrates remediation,
// tripping the remediation cap only for failures beyond the attempt budget.
// A trailing failure on the same SHA that is already under remediation is
// ignored: cancelling there would kill the active attempt-5 turn and bump
// ci_fail_count to 6 on a duplicate workflow_run delivery.
func (f *Flow) HandleCIFailureEvent(ctx context.Context, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	if executor.IsGateAggregatorCheck(ev.CheckName) {
		return nil
	}
	if err := f.HandlePRCheckEvent(ctx, chunkID, event, ev); err != nil {
		return err
	}
	banner := fmt.Sprintf("CI check %q failed", ev.CheckName)
	if f.Store != nil {
		if err := f.Store.UpdateChunkBannerAndStatus(ctx, chunkID, statusCIFailed, banner); err != nil {
			return err
		}
	}
	if f.Exec == nil {
		return nil
	}
	bld, chunk, err := f.LookupBuildChunk(ctx, buildID, chunkID)
	if err != nil {
		return err
	}
	cleanHead := strings.TrimSpace(ev.HeadSHA)
	if chunk.CIFailCount >= executor.MaxCIRemediationAttempts {
		if cleanHead != "" && strings.EqualFold(strings.TrimSpace(chunk.RemediatedSHA), cleanHead) {
			return nil
		}
		var failCount int
		if f.Store != nil {
			failCount, err = f.Store.IncrementChunkCIFailCount(ctx, chunkID, banner, cleanHead)
			if err != nil {
				return err
			}
		}
		return f.HandleCIRemediationCapReached(ctx, chunkID, failCount, ev)
	}
	details, err := f.Exec.ResolveCIFailureDetailsChecked(ctx, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
	if err != nil {
		return err
	}
	dispatchID := executor.CIRemediationDispatchID(chunkID, ev.HeadSHA, chunk.CIFailCount+1)
	skipIfActive := cleanHead == "" || strings.EqualFold(strings.TrimSpace(chunk.RemediatedSHA), cleanHead)
	if err = f.DispatchChunkRemediationForChunk(ctx, bld, chunk, dispatchID, executor.FormatCIFailurePrompt(ev, details), skipIfActive); err != nil {
		if errors.Is(err, executor.ErrDispatchActive) {
			return nil
		}
		return err
	}
	if f.Store != nil {
		if _, err := f.Store.IncrementChunkCIFailCount(ctx, chunkID, banner, cleanHead); err != nil {
			return err
		}
	}
	return nil
}

// HandleClosedPREvent fails the chunk when its PR closes without merging.
func (f *Flow) HandleClosedPREvent(ctx context.Context, chunkID string, ev pubsub.PRLifecycleEvent) error {
	if ev.Status == "merged" || f.Store == nil {
		return nil
	}
	bld, _, failed, fErr := f.Store.FailChunk(ctx, chunkID, "pr_closed_unmerged", "")
	if fErr != nil {
		return fErr
	}
	if failed && bld != nil && bld.Status == statusFailed && f.Exec != nil {
		if err := f.notifyChunkFailure(ctx, *bld, chunkID, "pr_closed_unmerged"); err != nil {
			return err
		}
	}
	return nil
}

// HandleDeployFailureEvent marks the chunk deploy-failed and wakes the thread.
func (f *Flow) HandleDeployFailureEvent(ctx context.Context, buildID, chunkID string, ev pubsub.PRLifecycleEvent) error {
	banner := executor.FormatDeployFailureBanner(ev)
	if f.Store != nil {
		if err := f.Store.UpdateChunkBannerAndStatus(ctx, chunkID, "deploy_failed", banner); err != nil {
			return err
		}
	}
	if f.Exec != nil {
		if err := f.dispatchThreadWakePrompt(ctx, buildID, executor.FormatDeployFailurePrompt(ev)); err != nil {
			return err
		}
	}
	return nil
}

// HandlePREventType routes one PR lifecycle event to its transition.
func (f *Flow) HandlePREventType(ctx context.Context, buildID, chunkID, event string, ev pubsub.PRLifecycleEvent) error {
	switch event {
	case prEventOpened, "synchronize":
		return f.RecordPRHeadRevision(ctx, chunkID, ev)
	case "check_updated":
		return f.HandlePRCheckEvent(ctx, chunkID, event, ev)
	case "check_failed", "ci_failed":
		return f.HandleCIFailureEvent(ctx, buildID, chunkID, event, ev)
	case "conflict":
		return f.HandleConflictEvent(ctx, buildID, chunkID, ev)
	case "merged":
		if f.Store != nil {
			if err := f.Store.UpdatePRLifecycleStatus(ctx, chunkID, "merged"); err != nil {
				return err
			}
		}
		if err := f.RecordPRMergeRevision(ctx, chunkID, ev.HeadSHA, ev.MergeSHA); err != nil {
			return err
		}
		if f.Exec != nil {
			if cErr := f.Exec.CancelChunkDispatch(ctx, chunkID); cErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] cancel chunk dispatch failed", "chunk_id", chunkID, "error", cErr)
			}
			if pErr := f.Exec.PublishExternalEvent(ctx, "vanadis_chunk", "merged", []string{chunkID}, ""); pErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", pErr)
			}
		}
	case "closed", "closed_unmerged":
		return f.HandleClosedPREvent(ctx, chunkID, ev)
	case "deploy_failed", "sync_failed":
		return f.HandleDeployFailureEvent(ctx, buildID, chunkID, ev)
	}
	return nil
}

// LocateOrAttachPRChunk resolves the event to its chunk, attaching an
// unattached chunk when the branch or thread match is unambiguous.
func (f *Flow) LocateOrAttachPRChunk(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	lookup, err := f.Store.GetChunkByPR(ctx, ev.Repo, ev.PRNumber)
	if err != nil {
		return "", err
	}
	if lookup != nil && lookup.Matched {
		return lookup.ChunkID, nil
	}
	return f.ResolveOrAttachPRChunk(ctx, buildID, ev)
}

// IsEligibleUnattachedChunk reports whether a chunk may adopt an unattached PR.
func IsEligibleUnattachedChunk(build *model.VanadisBuild, c *model.VanadisChunk, ev pubsub.PRLifecycleEvent) bool {
	if build == nil || c == nil {
		return false
	}
	if (c.Mode != "" && c.Mode != "pr") || c.PRNumber != 0 || c.StartedAt.IsZero() {
		return false
	}
	if c.Status != statusRunning && c.Status != statusCoding {
		return false
	}
	if buildThread := NormalizeDiscordThreadID(build.ThreadID); buildThread != "" && strings.TrimSpace(ev.TargetID) != "" && NormalizeDiscordThreadID(ev.TargetID) != buildThread {
		return false
	}
	return store.AttachmentMatches(*c, *build, store.AttachChunkPRRequest{Repo: strings.TrimSpace(ev.Repo), PRNumber: ev.PRNumber})
}

// SyncAttachedPRWidget refreshes the build widget after a PR attachment.
func (f *Flow) SyncAttachedPRWidget(ctx context.Context, buildID string) {
	if f.Exec == nil {
		return
	}
	refreshedBuild, refreshedChunks, rErr := f.Store.GetBuild(ctx, buildID)
	if rErr == nil && refreshedBuild != nil {
		if _, sErr := f.Exec.SyncBuildWidget(ctx, f.Store, *refreshedBuild, refreshedChunks, ""); sErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] sync build widget warning", "error", sErr)
		}
	}
}

// SelectUnattachedTargetChunk picks the single unattached chunk an event may
// attach to: by branch-encoded chunk ID, else by unambiguous thread match.
func SelectUnattachedTargetChunk(build *model.VanadisBuild, chunks []model.VanadisChunk, ev pubsub.PRLifecycleEvent) *model.VanadisChunk {
	if build == nil {
		return nil
	}
	if chunkID := ExtractChunkIDFromBranch(ev.Repo, ev.Branch); chunkID != "" {
		for i := range chunks {
			if chunks[i].ID == chunkID && chunks[i].BuildID == build.ID {
				if IsEligibleUnattachedChunk(build, &chunks[i], ev) {
					return &chunks[i]
				}
				return nil
			}
		}
		return nil
	}
	evThread := NormalizeDiscordThreadID(ev.TargetID)
	buildThread := NormalizeDiscordThreadID(build.ThreadID)
	if !CanFallbackThreadRepoLookup(ev) || evThread == "" || buildThread == "" || evThread != buildThread {
		return nil
	}
	var matched *model.VanadisChunk
	for i := range chunks {
		if chunks[i].BuildID == build.ID && IsEligibleUnattachedChunk(build, &chunks[i], ev) {
			if matched != nil {
				return nil
			}
			matched = &chunks[i]
		}
	}
	return matched
}

// ResolveOrAttachPRChunk attaches an unattached chunk for the event PR.
func (f *Flow) ResolveOrAttachPRChunk(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) (string, error) {
	if f.Exec == nil || (ExtractChunkIDFromBranch(ev.Repo, ev.Branch) == "" && !CanFallbackThreadRepoLookup(ev)) {
		return "", nil
	}

	build, chunks, gErr := f.Store.GetBuild(ctx, buildID)
	if gErr != nil {
		if errors.Is(gErr, store.ErrBuildNotFound) {
			return "", nil
		}
		return "", gErr
	}

	targetChunk := SelectUnattachedTargetChunk(build, chunks, ev)
	if targetChunk == nil {
		return "", nil
	}
	if vErr := f.Exec.ValidateChunkPRAttachment(ctx, *build, *targetChunk, ev); vErr != nil {
		if errors.Is(vErr, ErrPRTransitionSkipped) {
			return "", nil
		}
		return "", vErr
	}

	_, attachedChunkID, _, aErr := f.Store.AttachChunkPR(ctx, store.AttachChunkPRRequest{
		ChunkID:  targetChunk.ID,
		PRNumber: ev.PRNumber,
		Repo:     strings.TrimSpace(ev.Repo),
		Branch:   ev.Branch,
		Title:    ev.Title,
		TargetID: ev.TargetID,
		Quiet:    ev.Quiet,
	})
	if aErr != nil {
		if errors.Is(aErr, store.ErrBuildConflict) || errors.Is(aErr, store.ErrChunkNotFound) {
			return "", nil
		}
		return "", aErr
	}

	f.SyncAttachedPRWidget(ctx, buildID)
	return attachedChunkID, nil
}

// NoPRAction is the recovery decision for a PR chunk turn that ended dirty.
type NoPRAction string

const (
	// NoPRActionNone takes no recovery action for the completed turn.
	NoPRActionNone NoPRAction = "none"
	// NoPRActionResume dispatches the single automatic resume turn.
	NoPRActionResume NoPRAction = "resume"
	// NoPRActionFail fails the chunk after the resume turn also ended dirty.
	NoPRActionFail NoPRAction = "fail"
)

// NoPRFailureReason is the failure reason recorded when a PR chunk turn ends
// without opening a PR even after the automatic resume.
const NoPRFailureReason = "chunk turn ended without opening a PR (after one automatic resume)"

// NoPRResumeDispatchID identifies the single automatic resume turn.
func NoPRResumeDispatchID(chunkID string) string {
	return fmt.Sprintf("vanadis-%s-nopr-resume-1", strings.TrimSpace(chunkID))
}

// NoPRTurnAction decides how to react when a PR chunk's turn completes. The
// submit script attaches PRs synchronously, so a completed launch or resume
// turn on a still-running chunk without a PR means the turn ended without one.
func NoPRTurnAction(chunk model.VanadisChunk, buildStatus, dispatchID string) NoPRAction {
	if chunk.Mode == chunkModeAction || chunk.PRNumber != 0 || buildStatus != statusRunning {
		return NoPRActionNone
	}
	if chunk.Status != statusRunning && chunk.Status != statusCoding {
		return NoPRActionNone
	}
	switch dispatchID {
	case ActionDispatchID(chunk):
		return NoPRActionResume
	case NoPRResumeDispatchID(chunk.ID):
		return NoPRActionFail
	}
	return NoPRActionNone
}

// FindChunkIndex returns the index of a chunk ID, or -1 when absent.
func FindChunkIndex(chunks []model.VanadisChunk, chunkID string) int {
	for i := range chunks {
		if chunks[i].ID == chunkID {
			return i
		}
	}
	return -1
}

// ResumeNoPRChunk dispatches the single automatic resume turn for a chunk
// whose launch turn ended without opening a PR.
func (f *Flow) ResumeNoPRChunk(ctx context.Context, bld *model.VanadisBuild, chunks []model.VanadisChunk, idx int) error {
	chunk := chunks[idx]
	chunkPrompt, err := f.Exec.FormatChunkPrompt(*bld, chunks, idx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}
	chunk.Effort = store.CapChunkEffort(chunk.Effort)
	// eri dedupes by dispatch ID, so never skip: a dropped resume would stall the chunk.
	if err := f.DispatchChunkRemediationForChunk(ctx, bld, chunk, NoPRResumeDispatchID(chunk.ID), executor.FormatNoPRResumePrompt(chunk, chunkPrompt), false); err != nil {
		return err
	}
	banner := fmt.Sprintf("Chunk %d turn ended without a PR — automatic resume dispatched", chunk.ChunkIndex+1)
	if _, sErr := f.Exec.SyncBuildWidget(ctx, f.Store, *bld, chunks, banner); sErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] sync build widget warning", "error", sErr)
	}
	return nil
}
