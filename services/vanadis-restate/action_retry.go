package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	restate "github.com/restatedev/sdk-go"
)

// RetryActionChunk journals the accepted generation separately from launching.
// A crash before the reset acknowledgement replays the idempotent transaction;
// a crash after it replays only reconciliation, never another generation reset.
func (b *BuildObject) RetryActionChunk(ctx restate.ObjectContext, req RetryActionChunkRequest) (RetryActionChunkResult, error) {
	if b == nil || b.store == nil {
		return RetryActionChunkResult{}, restate.TerminalErrorf("retry authority not configured")
	}
	rt := b.getRuntime(ctx)
	if !retryActionRequestMatches(req, rt.Key()) {
		return RetryActionChunkResult{}, toTerminalErr(fmt.Errorf("%w: build key, chunk, expected dispatch and reason are required", ErrInvalidBuildInput))
	}
	reset, err := rt.RunReconcileStep("retry-action-reset", func(runCtx context.Context) (reconcileStepOutput, error) {
		result, err := b.store.RetryActionChunk(runCtx, req)
		if err != nil {
			return reconcileStepOutput{}, toTerminalErr(err)
		}
		if result == nil || !result.Accepted || result.BuildID != req.BuildID || result.ChunkID != req.ChunkID || result.DispatchID == "" {
			return reconcileStepOutput{}, errors.New("invalid retry reset acknowledgement")
		}
		return reconcileStepOutput{RetryResult: result}, nil
	})
	if err != nil {
		return RetryActionChunkResult{}, err
	}
	if reset.RetryResult == nil {
		return RetryActionChunkResult{}, errors.New("retry journal missing accepted generation")
	}
	// The reopened build may notify its next terminal outcome independently of
	// the failed generation. Preserve every sibling launch acknowledgement.
	state, err := rt.GetState()
	if err != nil {
		return RetryActionChunkResult{}, err
	}
	if state != nil && state.TerminalNotified {
		state.TerminalNotified = false
		rt.SetState(*state)
	}
	if _, err := b.executeObjectTransition(retryReconcileRuntime{rt}, "retry-action-reconcile", nil); err != nil {
		// Reset is committed; durable recovery owns the accepted generation.
		return RetryActionChunkResult{}, errors.New("retry reconciliation pending: " + err.Error())
	}
	return *reset.RetryResult, nil
}

// After reset, continuation errors must remain retriable before journaling.
type retryReconcileRuntime struct{ objectRuntime }

func (r retryReconcileRuntime) RunReconcileStep(name string, fn func(context.Context) (reconcileStepOutput, error)) (reconcileStepOutput, error) {
	return r.objectRuntime.RunReconcileStep(name, func(ctx context.Context) (reconcileStepOutput, error) {
		out, err := fn(ctx)
		if err != nil {
			return out, errors.New("retry continuation pending: " + err.Error())
		}
		return out, nil
	})
}

func retryActionRequestMatches(req RetryActionChunkRequest, key string) bool {
	return strings.TrimSpace(req.BuildID) != "" && req.BuildID == key && strings.TrimSpace(req.ChunkID) != "" && strings.TrimSpace(req.ExpectedDispatchID) != "" && strings.TrimSpace(req.Reason) != ""
}
