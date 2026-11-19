package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// RetryActionChunk journals the accepted generation separately from launching.
// A crash before the reset acknowledgement replays the idempotent transaction;
// a crash after it replays only reconciliation, never another generation reset.
func (b *BuildObject) RetryActionChunk(ctx restate.ObjectContext, req store.RetryActionChunkRequest) (store.RetryActionChunkResult, error) {
	if b == nil || b.store == nil {
		return store.RetryActionChunkResult{}, restate.TerminalErrorf("retry authority not configured")
	}
	rt := b.getRuntime(ctx)
	if !retryActionRequestMatches(req, rt.Key()) {
		return store.RetryActionChunkResult{}, toTerminalErr(fmt.Errorf("%w: build key, chunk, expected dispatch and reason are required", store.ErrInvalidBuildInput))
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
		return store.RetryActionChunkResult{}, err
	}
	if reset.RetryResult == nil {
		return store.RetryActionChunkResult{}, errors.New("retry journal missing accepted generation")
	}
	// The reopened build may notify its next terminal outcome independently of
	// the failed generation. Preserve every sibling launch acknowledgement.
	state, err := rt.GetState()
	if err != nil {
		return store.RetryActionChunkResult{}, err
	}
	if state != nil && state.TerminalNotified {
		state.TerminalNotified = false
		rt.SetState(*state)
	}
	if _, err := b.executeObjectTransition(retryReconcileRuntime{rt}, "retry-action-reconcile", nil); err != nil {
		// Reset is committed; durable recovery owns the accepted generation.
		return store.RetryActionChunkResult{}, errors.New("retry reconciliation pending: " + err.Error())
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

func retryActionRequestMatches(req store.RetryActionChunkRequest, key string) bool {
	return validRetryInstructionPair(req) && strings.TrimSpace(req.BuildID) != "" && req.BuildID == key && strings.TrimSpace(req.ChunkID) != "" && strings.TrimSpace(req.ExpectedDispatchID) != "" && strings.TrimSpace(req.Reason) != ""
}

// Shared retry helpers below (lockRetryBuildAndChunks, validRetryInstructionPair)
// mirror the internal/store copies byte-for-byte for the remaining root callers
// (retry request matching here, attachPRTransaction in pr_attachment.go); the
// store copies are authoritative until later chunks move those callers.
func validRetryInstructionPair(req store.RetryActionChunkRequest) bool {
	if req.ExpectedInstructions == nil && req.ReplacementInstructions == nil {
		return true
	}
	return req.ExpectedInstructions != nil && req.ReplacementInstructions != nil && strings.TrimSpace(*req.ExpectedInstructions) != "" && strings.TrimSpace(*req.ReplacementInstructions) != ""
}

// lockRetryBuildAndChunks follows the parent-first ordering used by existing mutations.
func lockRetryBuildAndChunks(ctx context.Context, tx pgx.Tx, buildID string) (model.VanadisBuild, []model.VanadisChunk, error) {
	b, err := store.ScanBuild(tx.QueryRow(ctx, `SELECT `+store.BuildColumns+` FROM vanadis_builds WHERE id = $1 FOR UPDATE;`, buildID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil, store.ErrBuildNotFound
	}
	if err != nil {
		return b, nil, err
	}
	rows, err := tx.Query(ctx, store.SelectChunksForUpdateSQL, buildID)
	if err != nil {
		return b, nil, err
	}
	defer rows.Close()
	chunks, err := store.ScanChunks(rows)
	return b, chunks, err
}
