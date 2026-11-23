package queue

import (
	"context"
	"log"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

const cancelledDispatchReason = "cancelled dispatch"

// DispatchCoordinator is the DispatchSession Virtual Object keyed by dispatch
// session key. It runs one dispatch per turn through TurnRunner RunDispatch.
type DispatchCoordinator struct {
	store     coordinatorStore
	agentName string
	core      coordinatorCore
}

// NewDispatchCoordinator constructs a DispatchCoordinator for agentName.
func NewDispatchCoordinator(store coordinatorStore, agentName string) *DispatchCoordinator {
	agentName = strings.TrimSpace(agentName)
	return &DispatchCoordinator{
		store:     store,
		agentName: agentName,
		core: coordinatorCore{
			selfService:   DispatchSessionServiceName(agentName),
			runnerService: TurnRunnerServiceName(agentName),
			records:       dispatchRecords{store: store},
		},
	}
}

// ServiceName returns the per-agent DispatchSession service name.
func (d *DispatchCoordinator) ServiceName() string {
	return DispatchSessionServiceName(d.agentName)
}

// dispatchRecords maps coordinator record operations onto the dispatches table.
type dispatchRecords struct {
	store coordinatorStore
}

func (dispatchRecords) runnerHandler() string { return HandlerRunDispatch }

func (dispatchRecords) runnerRequest(key, turnID string, ids []string) any {
	req := DispatchTurnRequest{SessionKey: key, TurnID: turnID}
	if len(ids) > 0 {
		req.DispatchID = ids[0]
	}
	return req
}

func (dispatchRecords) turnCap() int { return maxCoalescedDispatchItems }

func (r dispatchRecords) failIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return r.store.FailDispatchesIfPending(ctx, ids, reason)
}

func (r dispatchRecords) markSkipped(ctx context.Context, id string) error {
	return r.store.UpdateDispatchStatus(ctx, id, db.StatusCompleted, skippedActiveTurnReason)
}

// buildEnqueuedDispatch builds the PENDING dispatch row for req under sessionKey.
func buildEnqueuedDispatch(req eriwire.BrainDispatchRequest, dispatchID, sessionKey string, now time.Time) db.Dispatch {
	return db.Dispatch{
		ID:             dispatchID,
		Source:         strings.ToLower(strings.TrimSpace(req.Source)),
		SessionID:      sessionKey,
		TargetID:       session.NormalizeEnvTargetID(req.TargetID),
		VanadisChunkID: strings.TrimSpace(req.VanadisChunkID),
		Prompt:         req.Prompt,
		Summary:        db.CleanTaskSummary(req.Prompt),
		Effort:         strings.ToLower(strings.TrimSpace(req.Effort)),
		Model:          strings.TrimSpace(req.Model),
		Traceparent:    strings.TrimSpace(req.Traceparent),
		Status:         db.StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// Enqueue records a PENDING dispatch and appends it to this key. A caller-supplied ID that
// already exists answers already_exists but is still appended (Append dedups), so a retried
// enqueue is never stranded.
func (d *DispatchCoordinator) Enqueue(ctx restate.ObjectSharedContext, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	sessionKey := restate.Key(ctx)
	callerSuppliedID := strings.TrimSpace(req.ID) != ""
	dispID := strings.TrimSpace(req.ID)
	if sessionKey == "" {
		if dispID == "" {
			dispID = deriveDeterministicDispatchID(ctx, "")
		}
		keyReq := req
		keyReq.ID = dispID
		sessionKey = eriwire.ResolveDispatchSessionKey(keyReq)
	}
	if dispID == "" {
		dispID = deriveDeterministicDispatchID(ctx, sessionKey)
	}

	if req.SkipIfActive {
		st, err := loadCoordinatorState(ctx)
		if err != nil {
			return eriwire.DispatchEnqueueResponse{}, coordinatorError(err)
		}
		if st.busy() {
			return eriwire.DispatchEnqueueResponse{Status: eriwire.StatusSkippedActive, DispatchID: dispID, SessionID: sessionKey}, nil
		}
	}

	inserted, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		rec := buildEnqueuedDispatch(req, dispID, sessionKey, time.Now().UTC())
		ok, sErr := storeCall(rc, func(sc context.Context) (bool, error) {
			return d.store.InsertDispatch(sc, rec)
		})
		if sErr != nil {
			log.Printf("[DispatchSession] Error recording dispatch %s: %v", dispID, sErr)
		}
		return ok, sErr
	}, restate.WithName(StepRecordQueued))
	if err != nil {
		return eriwire.DispatchEnqueueResponse{}, coordinatorError(err)
	}

	existed := !inserted && callerSuppliedID
	// An existing row is appended without SkipIfActive: the skip path would overwrite a row
	// this request did not create.
	restate.ObjectSend(ctx, d.ServiceName(), sessionKey, HandlerAppend).Send(AppendRequest{IDs: []string{dispID}, SkipIfActive: req.SkipIfActive && !existed})
	if existed {
		return eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAlreadyExists, DispatchID: dispID, SessionID: sessionKey}, nil
	}
	return eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAccepted, DispatchID: dispID, SessionID: sessionKey}, nil
}

// Append adds IDs to the key's pending list (deduplicated) and starts a turn when idle.
func (d *DispatchCoordinator) Append(ctx restate.ObjectContext, req AppendRequest) error {
	return d.core.append(ctx, req)
}

// OnTurnDone ends the active turn reported by its TurnRunner (fenced on TurnID).
func (d *DispatchCoordinator) OnTurnDone(ctx restate.ObjectContext, req TurnDoneRequest) error {
	return d.core.onTurnDone(ctx, req, d.reschedule)
}

// reschedule requeues the dispatch at the front and starts it again; dispatch keys have no
// delayed Kick.
func (d *DispatchCoordinator) reschedule(ctx restate.ObjectContext, st coordinatorState, _ TurnDoneRequest) {
	st.pending = prependIDs(st.active.ItemIDs, st.pending)
	st.active = nil
	d.core.startNext(ctx, st)
}

// Cancel fails the key's still-PENDING dispatches from a state snapshot and asks ClearPending
// to drop them and cancel the active turn. It cancels everything on its key.
func (d *DispatchCoordinator) Cancel(ctx restate.ObjectSharedContext, req CancelDispatchRequest) (CancelDispatchResponse, error) {
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = cancelledDispatchReason
	}
	st, failed, err := d.core.cancel(ctx, reason)
	if err != nil {
		return CancelDispatchResponse{}, err
	}
	ids := slices.Clone(failed)
	if st.active != nil && !st.active.Waiting {
		ids = normalizeStepIDs(append(ids, st.active.ItemIDs...))
	}
	return CancelDispatchResponse{Status: "ok", CancelledCount: len(ids), CancelledIDs: ids}, nil
}

// ClearPending removes IDs from pending and, when asked, cancels the active turn it covers.
func (d *DispatchCoordinator) ClearPending(ctx restate.ObjectContext, req ClearPendingRequest) error {
	return d.core.clearPending(ctx, req)
}

// Status returns the coordinator state.
func (d *DispatchCoordinator) Status(ctx restate.ObjectSharedContext) (CoordinatorStatus, error) {
	return d.core.status(ctx)
}
