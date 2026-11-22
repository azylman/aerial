package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// UpdateActiveStepRequest carries the target chunk and active step description.
type UpdateActiveStepRequest struct {
	ChunkID     string            `json:"chunk_id,omitempty"`
	ActiveStep  string            `json:"active_step,omitempty"`
	Step        string            `json:"step,omitempty"`
	ClearAll    bool              `json:"clear_all,omitempty"`
	ActiveSteps map[string]string `json:"active_steps,omitempty"`
}

// AckResponse represents an acknowledgement response from a Restate handler.
type AckResponse struct {
	Status string `json:"status"`
}

type activeStepSyncState struct {
	mu          sync.Mutex
	inFlight    bool
	dirty       bool
	activeSteps map[string]string
}

func (s *activeStepSyncState) recordUpdateLocked(req UpdateActiveStepRequest, steps map[string]string) {
	if req.ActiveSteps != nil || req.ClearAll {
		s.activeSteps = maps.Clone(steps)
		return
	}
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID == "" {
		return
	}
	if sText, exists := steps[chunkID]; exists && sText != "" {
		s.activeSteps[chunkID] = sText
	} else {
		s.activeSteps[chunkID] = ""
	}
}

func (s *activeStepSyncState) cleanupLocked(buildID string, activeStepSyncs *sync.Map) {
	s.inFlight = false
	s.dirty = false
	allEmpty := true
	for _, v := range s.activeSteps {
		if strings.TrimSpace(v) != "" {
			allEmpty = false
			break
		}
	}
	if allEmpty {
		activeStepSyncs.Delete(buildID)
	}
}

func applyActiveStepsToChunks(chunks []model.VanadisChunk, currentSteps map[string]string, isAuthoritative bool) {
	for i := range chunks {
		if isAuthoritative {
			chunks[i].ActiveStep = currentSteps[chunks[i].ID]
		} else if s, ok := currentSteps[chunks[i].ID]; ok {
			chunks[i].ActiveStep = s
		}
	}
}

func (b *BuildObject) syncActiveStepWidget(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk) error {
	buildCopy := build
	buildCopy.Quiet = true
	_, err := b.executor.SyncBuildWidget(ctx, b.store, buildCopy, chunks, "")
	return err
}

func normalizeActiveSteps(req UpdateActiveStepRequest) (map[string]string, error) {
	chunkID := strings.TrimSpace(req.ChunkID)
	if chunkID == "" && req.ActiveSteps == nil && !req.ClearAll {
		return nil, fmt.Errorf("%w: chunk_id cannot be empty without active_steps", store.ErrInvalidBuildInput)
	}

	steps := make(map[string]string, len(req.ActiveSteps)+1)
	for k, v := range req.ActiveSteps {
		kClean := strings.TrimSpace(k)
		vClean := strings.TrimSpace(v)
		if kClean != "" && vClean != "" {
			steps[kClean] = vClean
		}
	}
	if req.ActiveSteps == nil && !req.ClearAll && chunkID != "" {
		stepText := strings.TrimSpace(req.ActiveStep)
		if stepText == "" {
			stepText = strings.TrimSpace(req.Step)
		}
		if stepText != "" {
			steps[chunkID] = stepText
		}
	}
	return steps, nil
}

func (b *BuildObject) syncSingleActiveStepIteration(rc restate.RunContext, buildID string, req UpdateActiveStepRequest, state *activeStepSyncState) {
	bld, chunks, gErr := b.store.GetBuild(rc, buildID)
	if gErr != nil {
		if errors.Is(gErr, store.ErrBuildNotFound) {
			return
		}
		slog.WarnContext(rc, "[vanadis-restate] get build warning on active step update", "build_id", buildID, "chunk_id", req.ChunkID, "error", gErr)
		return
	}

	state.mu.Lock()
	currentSteps := maps.Clone(state.activeSteps)
	isAuthoritative := req.ActiveSteps != nil || req.ClearAll
	state.mu.Unlock()

	applyActiveStepsToChunks(chunks, currentSteps, isAuthoritative)

	if bld == nil || bld.Quiet {
		return
	}
	stepCtx, cancel := context.WithTimeout(rc, 3*time.Second)
	defer cancel()
	if sErr := b.syncActiveStepWidget(stepCtx, *bld, chunks); sErr != nil {
		slog.WarnContext(rc, "[vanadis-restate] sync widget warning on active step update", "build_id", buildID, "chunk_id", req.ChunkID, "error", sErr)
	}
}

func (b *BuildObject) coalesceActiveStepSync(rc restate.RunContext, buildID string, req UpdateActiveStepRequest, steps map[string]string) (bool, error) {
	val, _ := b.activeStepSyncs.LoadOrStore(buildID, &activeStepSyncState{
		activeSteps: make(map[string]string),
	})
	state, ok := val.(*activeStepSyncState)
	if !ok || state == nil {
		return false, nil
	}

	state.mu.Lock()
	state.recordUpdateLocked(req, steps)
	if state.inFlight {
		state.dirty = true
		state.mu.Unlock()
		return true, nil
	}
	state.inFlight = true
	state.mu.Unlock()

	defer func() {
		state.mu.Lock()
		state.cleanupLocked(buildID, &b.activeStepSyncs)
		state.mu.Unlock()
	}()

	for {
		b.syncSingleActiveStepIteration(rc, buildID, req, state)

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
	steps, err := normalizeActiveSteps(req)
	if err != nil {
		RecordReconcileError("UpdateActiveStep", err)
		return AckResponse{}, restate.ToTerminalError(err, restate.WithErrorCode(400))
	}

	buildID := strings.TrimSpace(restate.Key(ctx))

	found, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		if b == nil || b.store == nil || b.executor == nil || buildID == "" {
			return true, nil
		}
		return b.coalesceActiveStepSync(rc, buildID, req, steps)
	}, restate.WithName("update-active-step"))
	if err != nil {
		RecordReconcileError("UpdateActiveStep", err)
		return AckResponse{}, ToTerminalErr(err)
	}

	if !found {
		return AckResponse{Status: "ok"}, nil
	}

	return AckResponse{Status: "ok"}, nil
}
