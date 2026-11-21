package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const (
	labelCompleteChunk               = "CompleteChunk"
	labelReconcileAfterCompleteChunk = "reconcile-after-complete-chunk"
	stepCompleteChunkVerdict         = "complete-chunk-verdict"
	stepCompleteChunkEffects         = "complete-chunk-effects"

	verdictStatusCompleted        = "completed"
	verdictStatusAlreadyCompleted = "already_completed"

	maxVerdictSummaryRunes = 4000
	maxVerdictBannerRunes  = 300
)

// CompleteChunk applies an action chunk's explicit completion verdict. The
// verdict completes only a running action chunk whose current dispatch
// identity matches; a repeat of an applied verdict reports already_completed.
func (b *BuildObject) CompleteChunk(ctx restate.ObjectContext, req CompleteChunkRequest) (CompleteChunkResponse, error) {
	if b == nil || b.store == nil {
		return CompleteChunkResponse{}, restate.TerminalErrorf("verdict authority not configured")
	}
	if err := b.guardRunner(ctx, labelCompleteChunk); err != nil {
		return CompleteChunkResponse{}, err
	}
	rt := b.getRuntime(ctx)
	buildID := strings.TrimSpace(rt.Key())
	req = normalizeCompleteChunkRequest(req)
	if err := validateCompleteChunkRequest(req, buildID); err != nil {
		RecordReconcileError(labelCompleteChunk, err)
		return CompleteChunkResponse{}, ToTerminalErr(err)
	}
	out, err := rt.RunReconcileStep(stepCompleteChunkVerdict, func(runCtx context.Context) (reconcileStepOutput, error) {
		status, err := b.applyChunkVerdict(runCtx, buildID, req)
		if err != nil {
			RecordReconcileError(labelCompleteChunk, err)
			return reconcileStepOutput{}, ToTerminalErr(err)
		}
		// The journaled verdict status rides in the shared step envelope.
		return reconcileStepOutput{BuildStatus: status}, nil
	})
	if err != nil {
		return CompleteChunkResponse{}, err
	}
	resp := CompleteChunkResponse{Status: out.BuildStatus, BuildID: buildID, ChunkID: req.ChunkID, Completed: true}
	switch out.BuildStatus {
	case verdictStatusAlreadyCompleted:
		return resp, nil
	case verdictStatusCompleted:
	default:
		return CompleteChunkResponse{}, errors.New("verdict journal missing outcome")
	}
	mutation := &BuildMutationRequest{Action: mutationActionCompleted, ChunkID: req.ChunkID, DispatchID: req.DispatchID}
	if _, tErr := b.executeObjectTransition(rt, labelReconcileAfterCompleteChunk, mutation); tErr != nil {
		return CompleteChunkResponse{}, ToTerminalErr(tErr)
	}
	if _, eErr := rt.RunReconcileStep(stepCompleteChunkEffects, func(runCtx context.Context) (reconcileStepOutput, error) {
		return reconcileStepOutput{}, b.publishChunkVerdict(runCtx, buildID, req)
	}); eErr != nil {
		return CompleteChunkResponse{}, eErr
	}
	return resp, nil
}

func normalizeCompleteChunkRequest(req CompleteChunkRequest) CompleteChunkRequest {
	req.BuildID = strings.TrimSpace(req.BuildID)
	req.ChunkID = strings.TrimSpace(req.ChunkID)
	req.DispatchID = strings.TrimSpace(req.DispatchID)
	req.Summary = strings.TrimSpace(req.Summary)
	return req
}

func validateCompleteChunkRequest(req CompleteChunkRequest, buildID string) error {
	switch {
	case buildID == "" || req.BuildID != buildID:
		return fmt.Errorf("%w: build_id must match the build object key", store.ErrInvalidBuildInput)
	case req.ChunkID == "" || req.DispatchID == "" || req.Summary == "":
		return fmt.Errorf("%w: chunk_id, dispatch_id and summary are required", store.ErrInvalidBuildInput)
	case utf8.RuneCountInString(req.Summary) > maxVerdictSummaryRunes:
		return fmt.Errorf("%w: summary exceeds %d characters", store.ErrInvalidBuildInput, maxVerdictSummaryRunes)
	}
	return nil
}

// applyChunkVerdict completes the chunk when it is a running action chunk of
// this build whose dispatch identity matches the verdict, and reports
// completed or already_completed.
func (b *BuildObject) applyChunkVerdict(ctx context.Context, buildID string, req CompleteChunkRequest) (string, error) {
	_, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return "", err
	}
	idx := findChunkIndex(chunks, req.ChunkID)
	if idx < 0 || chunks[idx].BuildID != buildID {
		return "", fmt.Errorf("%w: chunk %s does not belong to build %s", store.ErrInvalidBuildInput, req.ChunkID, buildID)
	}
	if chunks[idx].Mode != chunkModeAction {
		return "", fmt.Errorf("%w: chunk %s is not an action chunk", store.ErrInvalidBuildInput, req.ChunkID)
	}
	res, err := b.store.CompleteActionChunk(ctx, req.ChunkID, req.DispatchID)
	if err != nil {
		return "", err
	}
	if res == nil || !res.CompletionAccepted {
		return "", fmt.Errorf("%w: chunk %s is not running under dispatch %s", store.ErrBuildConflict, req.ChunkID, req.DispatchID)
	}
	if res.Advanced {
		return verdictStatusCompleted, nil
	}
	return verdictStatusAlreadyCompleted, nil
}

// publishChunkVerdict logs the summary, announces the completion and shows the
// summary on the widget while the build is still running.
func (b *BuildObject) publishChunkVerdict(ctx context.Context, buildID string, req CompleteChunkRequest) error {
	slog.InfoContext(ctx, "[vanadis-restate] action chunk verdict", "build_id", buildID, "chunk_id", req.ChunkID, "dispatch_id", req.DispatchID, "summary", req.Summary)
	if b.executor == nil {
		return nil
	}
	if err := b.executor.PublishExternalEvent(ctx, "vanadis_chunk", verdictStatusCompleted, []string{req.ChunkID}, ""); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] publish event warning", "error", err)
	}
	bld, chunks, err := b.store.GetBuild(ctx, buildID)
	if err != nil {
		return err
	}
	idx := findChunkIndex(chunks, req.ChunkID)
	if bld == nil || bld.Status != statusRunning || idx < 0 {
		return nil
	}
	if _, err := b.executor.SyncBuildWidget(ctx, b.store, *bld, chunks, verdictBanner(chunks[idx].ChunkIndex, req.Summary)); err != nil {
		slog.WarnContext(ctx, "[vanadis-restate] verdict widget sync warning", "error", err)
	}
	return nil
}

func verdictBanner(chunkIndex int, summary string) string {
	line, _, _ := strings.Cut(summary, "\n")
	line = strings.TrimSpace(line)
	if r := []rune(line); len(r) > maxVerdictBannerRunes {
		line = string(r[:maxVerdictBannerRunes-1]) + "…"
	}
	return fmt.Sprintf("✅ Chunk %d completed: %s", chunkIndex+1, line)
}
