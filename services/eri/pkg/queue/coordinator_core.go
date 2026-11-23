package queue

import (
	"context"
	"log"
	"slices"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

// Coordinator Virtual Objects (ThreadSession / DispatchSession) own the
// per-key turn order: a FIFO "pending" list of record IDs and at most one "active" turn run by
// a TurnRunner invocation. Exclusive handlers hold the key lock for milliseconds: they never
// wait on a turn and never retry Postgres without bound.

const (
	cancelledByOperatorReason = "cancelled by operator"
	skippedActiveTurnReason   = "[SKIPPED: active turn]"
	maxCoalescedDispatchItems = 1
)

// coordinatorStore is the narrow store surface the coordinators and the dispatch cancel router use.
type coordinatorStore interface {
	InsertMessage(ctx context.Context, msg db.Message) error
	GetMessage(ctx context.Context, id string) (*db.Message, error)
	InsertDispatch(ctx context.Context, d db.Dispatch) (bool, error)
	FailMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	FailMessagesIfPending(ctx context.Context, ids []string, reason string) ([]string, error)
	FailDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	FailDispatchesIfPending(ctx context.Context, ids []string, reason string) ([]string, error)
	SkipMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	SkipDispatchesIfActive(ctx context.Context, ids []string, reason string) ([]string, error)
	UpdateMessageStatus(ctx context.Context, id, status, errorMsg string, telemetry ...db.TurnTelemetry) error
	UpdateDispatchStatus(ctx context.Context, id, status, errorMsg string, telemetry ...db.TurnTelemetry) error
}

// coordinatorRecords is what differs between the thread and dispatch coordinators: which
// TurnRunner handler runs a turn, how many items a turn takes, and which table holds the rows.
type coordinatorRecords interface {
	runnerHandler() string
	runnerRequest(key, turnID string, ids []string) any
	turnCap() int
	failIfPending(ctx context.Context, ids []string, reason string) ([]string, error)
	markSkipped(ctx context.Context, id string) error
}

// coordinatorState is the decoded state of one coordinator key.
type coordinatorState struct {
	pending []string
	active  *ActiveTurn
}

// coordinatorCore implements the handlers shared by both coordinators.
type coordinatorCore struct {
	selfService   string
	runnerService string
	records       coordinatorRecords
}

func loadCoordinatorState(ctx restate.ObjectSharedContext) (coordinatorState, error) {
	pending, err := restate.Get[[]string](ctx, StateKeyPending)
	if err != nil {
		return coordinatorState{}, err
	}
	active, err := restate.Get[*ActiveTurn](ctx, StateKeyActive)
	if err != nil {
		return coordinatorState{}, err
	}
	return coordinatorState{pending: pending, active: active}, nil
}

// save writes the state back; an idle key (nothing pending, nothing active) is cleared entirely.
func (st coordinatorState) save(ctx restate.ObjectContext) {
	if len(st.pending) == 0 && st.active == nil {
		restate.ClearAll(ctx)
		return
	}
	if len(st.pending) == 0 {
		restate.Clear(ctx, StateKeyPending)
	} else {
		restate.Set(ctx, StateKeyPending, st.pending)
	}
	if st.active == nil {
		restate.Clear(ctx, StateKeyActive)
	} else {
		restate.Set(ctx, StateKeyActive, st.active)
	}
}

// holds reports whether id is pending or carried by the active turn.
func (st coordinatorState) holds(id string) bool {
	return slices.Contains(st.pending, id) || (st.active != nil && slices.Contains(st.active.ItemIDs, id))
}

// busy reports whether a SkipIfActive request must be skipped.
func (st coordinatorState) busy() bool {
	return st.active != nil || len(st.pending) > 0
}

// cancelSet is every ID a cancel taken from this snapshot covers: pending plus the active turn's items.
func (st coordinatorState) cancelSet() []string {
	ids := normalizeStepIDs(st.pending)
	if st.active != nil {
		ids = normalizeStepIDs(append(ids, st.active.ItemIDs...))
	}
	return ids
}

// splitTurnBatch takes the first max IDs of pending as the next turn.
func splitTurnBatch(pending []string, maxItems int) (batch, rest []string) {
	if maxItems <= 0 || len(pending) <= maxItems {
		return pending, nil
	}
	return pending[:maxItems], pending[maxItems:]
}

// prependIDs puts front ahead of rest, dropping duplicates and keeping first occurrences.
func prependIDs(front, rest []string) []string {
	return normalizeStepIDs(append(slices.Clone(front), rest...))
}

// removeIDs returns ids without any member of drop.
func removeIDs(ids, drop []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !slices.Contains(drop, id) {
			out = append(out, id)
		}
	}
	return out
}

// clearPendingCancelsActive decides whether a ClearPending request cancels the active turn:
// it must name the turn, or every one of the turn's items must be in the request (a turn
// started from the cancelled backlog after the snapshot). A turn holding any item the request
// does not cover was started from work appended after the snapshot and is left running.
func clearPendingCancelsActive(active *ActiveTurn, req ClearPendingRequest) bool {
	if !req.CancelActive || active == nil {
		return false
	}
	if req.TurnID != "" && active.TurnID == req.TurnID {
		return true
	}
	if len(active.ItemIDs) == 0 {
		return false
	}
	for _, id := range active.ItemIDs {
		if !slices.Contains(req.IDs, id) {
			return false
		}
	}
	return true
}

// coordinatorError keeps Restate's cancellation code (409) out of coordinator responses:
// a terminal step failure carrying 409 surfaces as CodeQueueConflict.
func coordinatorError(err error) error {
	if err == nil {
		return nil
	}
	if restate.IsTerminalError(err) && restate.AsTerminalError(err).Code() == CodeRestateCancelled {
		return restate.ToTerminalError(err, restate.WithErrorCode(CodeQueueConflict))
	}
	return err
}

// startNext starts the next turn when the key is idle and saves the state.
func (c *coordinatorCore) startNext(ctx restate.ObjectContext, st coordinatorState) {
	if st.active != nil || len(st.pending) == 0 {
		st.save(ctx)
		return
	}
	key := restate.Key(ctx)
	batch, rest := splitTurnBatch(st.pending, c.records.turnCap())
	turnID := deriveTurnID(key, ctx.Request().ID)
	inv := restate.ServiceSend(ctx, c.runnerService, c.records.runnerHandler()).Send(c.records.runnerRequest(key, turnID, batch))
	st.active = &ActiveTurn{TurnID: turnID, ItemIDs: slices.Clone(batch), RunnerInvocationID: inv.GetInvocationId()}
	st.pending = slices.Clone(rest)
	st.save(ctx)
}

// append is the exclusive Append handler body.
func (c *coordinatorCore) append(ctx restate.ObjectContext, req AppendRequest) error {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return coordinatorError(err)
	}
	fresh := make([]string, 0, len(req.IDs))
	for _, id := range normalizeStepIDs(req.IDs) {
		if !st.holds(id) {
			fresh = append(fresh, id)
		}
	}
	if len(fresh) == 0 {
		if st.active == nil {
			c.startNext(ctx, st)
		}
		return nil
	}
	if req.SkipIfActive && st.busy() {
		if _, rErr := restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
			for _, id := range fresh {
				if _, sErr := storeCall(rc, func(sc context.Context) (struct{}, error) {
					return struct{}{}, c.records.markSkipped(sc, id)
				}); sErr != nil {
					return restate.Void{}, sErr
				}
			}
			return restate.Void{}, nil
		}, restate.WithName(StepRecordSkipped)); rErr != nil {
			return coordinatorError(rErr)
		}
		return nil
	}
	st.pending = append(st.pending, fresh...)
	c.startNext(ctx, st)
	return nil
}

// onTurnDone is the exclusive OnTurnDone handler body; reschedule is delegated to the caller.
func (c *coordinatorCore) onTurnDone(ctx restate.ObjectContext, req TurnDoneRequest, reschedule func(restate.ObjectContext, coordinatorState, TurnDoneRequest)) error {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return coordinatorError(err)
	}
	if st.active == nil || st.active.TurnID != req.TurnID {
		log.Printf("[%s] Ignoring OnTurnDone for stale turn %s on %s", c.selfService, req.TurnID, restate.Key(ctx))
		return nil
	}
	if req.Outcome == OutcomeReschedule {
		reschedule(ctx, st, req)
		return nil
	}
	st.active = nil
	c.startNext(ctx, st)
	return nil
}

// cancel is the shared CancelTurn / Cancel body: it fails the snapshot's still-PENDING rows and
// hands the runner cancel to ClearPending, which acts on the state at that moment.
func (c *coordinatorCore) cancel(ctx restate.ObjectSharedContext, reason string) (coordinatorState, []string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = cancelledByOperatorReason
	}
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return coordinatorState{}, nil, coordinatorError(err)
	}
	ids := st.cancelSet()
	if len(ids) == 0 {
		return st, nil, nil
	}
	failed, rErr := restate.Run(ctx, func(rc restate.RunContext) ([]string, error) {
		return storeCall(rc, func(sc context.Context) ([]string, error) {
			return c.records.failIfPending(sc, ids, reason)
		})
	}, restate.WithName(StepRecordCancelled))
	if rErr != nil {
		return coordinatorState{}, nil, coordinatorError(rErr)
	}
	clearReq := ClearPendingRequest{IDs: normalizeStepIDs(st.pending), CancelActive: true}
	if st.active != nil {
		clearReq.TurnID = st.active.TurnID
	}
	restate.ObjectSend(ctx, c.selfService, restate.Key(ctx), HandlerClearPending).Send(clearReq)
	return st, failed, nil
}

// clearPending is the exclusive ClearPending body. It does no DB work.
func (c *coordinatorCore) clearPending(ctx restate.ObjectContext, req ClearPendingRequest) error {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return coordinatorError(err)
	}
	st.pending = removeIDs(st.pending, req.IDs)
	if clearPendingCancelsActive(st.active, req) {
		if !st.active.Waiting && st.active.RunnerInvocationID != "" {
			restate.CancelInvocation(ctx, st.active.RunnerInvocationID)
		}
		st.active = nil
	}
	c.startNext(ctx, st)
	return nil
}

// status is the shared Status body.
func (*coordinatorCore) status(ctx restate.ObjectSharedContext) (CoordinatorStatus, error) {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return CoordinatorStatus{}, coordinatorError(err)
	}
	return CoordinatorStatus{Pending: st.pending, Active: st.active}, nil
}
