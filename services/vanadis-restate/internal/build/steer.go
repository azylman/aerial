package build

import (
	"errors"
	"fmt"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// SteerChunk modifies a pending or running chunk's instructions and dispatches live steering if running.
func (b *BuildObject) SteerChunk(ctx restate.ObjectContext, req store.SteerChunkRequest) (store.SteerChunkResult, error) {
	buildID := strings.TrimSpace(restate.Key(ctx))
	if buildID == "" {
		buildID = strings.TrimSpace(req.BuildID)
	}
	chunkRef := strings.TrimSpace(req.ChunkRef)
	prompt := strings.TrimSpace(req.Prompt)

	if err := buildplan.ValidateSteerChunkInput(buildID, chunkRef, prompt); err != nil {
		RecordReconcileError(labelSteerChunk, err)
		return store.SteerChunkResult{}, restate.ToTerminalError(fmt.Errorf("%w: %w", store.ErrInvalidBuildInput, err), restate.WithErrorCode(400))
	}
	if b.store == nil {
		return store.SteerChunkResult{
			Accepted:     true,
			BuildID:      buildID,
			ChunkID:      chunkRef,
			ChunkStatus:  statusPending,
			Instructions: prompt,
		}, nil
	}

	res, err := RunJournaled(ctx, labelSteerChunk, "steer-chunk-postgres", func(rc restate.RunContext) (store.SteerChunkResult, error) {
		r, err := b.store.SteerChunk(rc, buildID, chunkRef, req)
		if err != nil {
			if errors.Is(err, store.ErrBuildConflict) {
				return store.SteerChunkResult{}, restate.ToTerminalError(err, restate.WithErrorCode(412))
			}
			return store.SteerChunkResult{}, err
		}
		if r == nil {
			return store.SteerChunkResult{}, nil
		}
		return *r, nil
	})
	if err != nil {
		if errors.Is(err, store.ErrBuildConflict) {
			return store.SteerChunkResult{}, restate.ToTerminalError(err, restate.WithErrorCode(412))
		}
		return store.SteerChunkResult{}, ToTerminalErr(err)
	}

	if res.ChunkStatus == statusRunning && b.executor != nil {
		dispatchID, err := b.dispatchRunningChunkSteer(ctx, buildID, res.ChunkID, req.Prompt)
		if err != nil {
			return store.SteerChunkResult{}, err
		}
		res.DispatchID = dispatchID
	}
	return res, nil
}

func (b *BuildObject) dispatchRunningChunkSteer(ctx restate.ObjectContext, buildID, chunkID, prompt string) (string, error) {
	seed := ""
	if reqObj := ctx.Request(); reqObj != nil {
		seed = reqObj.ID
	}
	if seed == "" {
		seed = fmt.Sprintf("%s:%s", buildID, chunkID)
	}
	dispatchID, err := RunJournaled(ctx, labelSteerChunk, "steer-chunk-dispatch", func(rc restate.RunContext) (string, error) {
		bld, chunks, err := b.store.GetBuild(rc, buildID)
		if err != nil {
			return "", err
		}
		target, found := findChunkByID(chunks, chunkID)
		if !found {
			return "", ErrChunkNotFound
		}
		var bldVal model.VanadisBuild
		if bld != nil {
			bldVal = *bld
		}
		return b.executor.DispatchChunkSteer(rc, bldVal, target, prompt, seed)
	})
	if err != nil {
		return "", ToTerminalErr(err)
	}
	return dispatchID, nil
}

func findChunkByID(chunks []model.VanadisChunk, chunkID string) (model.VanadisChunk, bool) {
	for _, c := range chunks {
		if c.ID == chunkID {
			return c, true
		}
	}
	return model.VanadisChunk{}, false
}
