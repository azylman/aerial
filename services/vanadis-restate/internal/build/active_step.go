package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

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

type activeStepSyncState struct {
	mu       sync.Mutex
	inFlight bool
	dirty    bool
}

func (b *BuildObject) syncActiveStepWidget(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error {
	_, err := b.executor.SyncBuildWidget(ctx, b.store, build, chunks, "")
	return err
}

func (b *BuildObject) coalesceActiveStepSync(rc restate.RunContext, buildID, chunkID string) (bool, error) {
	val, _ := b.activeStepSyncs.LoadOrStore(buildID, &activeStepSyncState{})
	state, ok := val.(*activeStepSyncState)
	if !ok || state == nil {
		return false, nil
	}

	state.mu.Lock()
	if state.inFlight {
		state.dirty = true
		state.mu.Unlock()
		return true, nil
	}
	state.inFlight = true
	state.mu.Unlock()

	defer func() {
		state.mu.Lock()
		state.inFlight = false
		state.dirty = false
		state.mu.Unlock()
	}()

	for {
		bld, chunks, gErr := b.store.GetBuild(rc, buildID)
		if gErr != nil {
			if errors.Is(gErr, store.ErrBuildNotFound) {
				return false, nil
			}
			slog.WarnContext(rc, "[vanadis-restate] get build warning on active step update", "build_id", buildID, "chunk_id", chunkID, "error", gErr)
			return true, nil
		}
		if bld == nil || bld.Quiet {
			return true, nil
		}
		stepCtx, cancel := context.WithTimeout(rc, 3*time.Second)
		if sErr := b.syncActiveStepWidget(stepCtx, *bld, chunks); sErr != nil {
			slog.WarnContext(rc, "[vanadis-restate] sync widget warning on active step update", "build_id", buildID, "chunk_id", chunkID, "error", sErr)
		}
		cancel()

		state.mu.Lock()
		if !state.dirty {
			state.inFlight = false
			state.mu.Unlock()
			return true, nil
		}
		state.dirty = false
		state.mu.Unlock()
	}
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
		return b.coalesceActiveStepSync(rc, buildID, chunkID)
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
