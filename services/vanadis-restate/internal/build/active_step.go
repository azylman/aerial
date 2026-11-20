package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

// UpdateActiveStepRequest carries the target chunk and active step description.
type UpdateActiveStepRequest struct {
	ChunkID    string `json:"chunk_id"`
	ActiveStep string `json:"active_step,omitempty"`
	Step       string `json:"step,omitempty"`
}

// AckResponse represents an acknowledgement response from a Restate handler.
type AckResponse struct {
	Status string `json:"status"`
}

func (b *BuildObject) syncActiveStepWidget(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error {
	if msgID := deliverDirectWidgetIfConfigured(ctx, build, chunks); msgID != "" {
		if msgID != build.StatusMessageID && b.store != nil {
			if uErr := b.store.UpdateBuildStatusMessageID(ctx, build.ID, msgID); uErr != nil {
				slog.WarnContext(ctx, "[vanadis-restate] update status message id failed", "error", uErr)
			}
		}
		return nil
	}
	_, err := b.executor.SyncBuildWidget(ctx, b.store, build, chunks, "")
	return err
}

func deliverDirectWidgetIfConfigured(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) string {
	d, err := widget.NewDiscordWidgetDeliverer("")
	if err != nil || d == nil {
		return ""
	}
	msgID, dErr := d.DeliverStatusWidget(ctx, widget.BuildSyncWidgetRequest(build, chunks))
	if dErr != nil {
		return ""
	}
	return msgID
}

// UpdateActiveStep updates the running chunk's live active step description and refreshes the Discord widget.
func (b *BuildObject) UpdateActiveStep(ctx restate.ObjectSharedContext, req UpdateActiveStepRequest) (AckResponse, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID == "" {
		err := fmt.Errorf("%w: chunk_id cannot be empty", store.ErrInvalidBuildInput)
		RecordReconcileError("UpdateActiveStep", err)
		return AckResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	stepText := strings.TrimSpace(req.ActiveStep)
	if stepText == "" {
		stepText = strings.TrimSpace(req.Step)
	}

	if stepText == "" {
		widget.ClearChunkActiveStep(chunkID)
	} else {
		widget.SetChunkActiveStep(chunkID, stepText)
	}

	buildID := strings.TrimSpace(restate.Key(ctx))

	found, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		if b == nil || b.store == nil || b.executor == nil || buildID == "" {
			return true, nil
		}
		bld, chunks, gErr := b.store.GetBuild(rc, buildID)
		if gErr != nil {
			if errors.Is(gErr, store.ErrBuildNotFound) {
				return false, nil
			}
			return false, gErr
		}
		if bld == nil || bld.Quiet {
			return true, nil
		}
		if sErr := b.syncActiveStepWidget(rc, *bld, chunks); sErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] sync widget warning on active step update", "build_id", buildID, "chunk_id", chunkID, "error", sErr)
		}
		return true, nil
	}, restate.WithName("update-active-step"))
	if err != nil {
		widget.ClearChunkActiveStep(chunkID)
		RecordReconcileError("UpdateActiveStep", err)
		return AckResponse{}, ToTerminalErr(err)
	}

	if !found {
		widget.ClearChunkActiveStep(chunkID)
		return AckResponse{Status: "ok"}, nil
	}

	return AckResponse{Status: "ok"}, nil
}
