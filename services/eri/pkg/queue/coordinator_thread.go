package queue

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

// threadResolver resolves where a prompt lands: build-notice redirects and optional Discord
// thread creation. *WorkerPool implements it.
type threadResolver interface {
	RedirectBuildPrompt(channelID, prompt string) string
	ResolveTargetThread(channelID, title, prompt string, now time.Time) string
}

type ingressWakeEvaluator interface {
	EvaluateIngressWake(ctx context.Context, threadID string, msg db.Message) (shouldEnqueue bool, completedReason string)
}

// ThreadCoordinator is the ThreadSession Virtual Object keyed by thread ID. It
// owns the thread's turn order and its active turn; TurnRunner RunBurst executes each turn.
type ThreadCoordinator struct {
	store     coordinatorStore
	resolver  threadResolver
	agentName string
	core      coordinatorCore
}

// NewThreadCoordinator constructs a ThreadCoordinator for agentName.
func NewThreadCoordinator(store coordinatorStore, resolver threadResolver, agentName string) *ThreadCoordinator {
	agentName = strings.TrimSpace(agentName)
	return &ThreadCoordinator{
		store:     store,
		resolver:  resolver,
		agentName: agentName,
		core: coordinatorCore{
			selfService:   ThreadSessionServiceName(agentName),
			runnerService: TurnRunnerServiceName(agentName),
			records:       threadRecords{store: store},
		},
	}
}

// ServiceName returns the per-agent ThreadSession service name.
func (t *ThreadCoordinator) ServiceName() string {
	return ThreadSessionServiceName(t.agentName)
}

// threadRecords maps coordinator record operations onto the messages table.
type threadRecords struct {
	store coordinatorStore
}

func (threadRecords) runnerHandler() string { return HandlerRunBurst }

func (threadRecords) runnerRequest(key, turnID string, ids []string) any {
	return BurstTurnRequest{ThreadID: key, TurnID: turnID, MessageIDs: ids}
}

func (threadRecords) turnCap() int { return maxCoalescedBurstMessages }

func (r threadRecords) failIfPending(ctx context.Context, ids []string, reason string) ([]string, error) {
	return r.store.FailMessagesIfPending(ctx, ids, reason)
}

func (r threadRecords) markSkipped(ctx context.Context, id string) error {
	_, err := r.store.SkipMessagesIfActive(ctx, []string{id}, skippedActiveTurnReason)
	return err
}

// threadEnqueueTarget picks the thread a request addresses before any redirect.
func threadEnqueueTarget(key string, req ThreadEnqueueRequest) string {
	threadID := key
	if threadID == "" {
		threadID = strings.TrimSpace(req.ChannelID)
	}
	if req.Message != nil && strings.TrimSpace(req.Message.ThreadID) != "" {
		threadID = strings.TrimSpace(req.Message.ThreadID)
	}
	return strings.TrimSpace(threadID)
}

// buildEnqueuedMessage builds the PENDING row for a request landing on finalThreadID.
func buildEnqueuedMessage(req ThreadEnqueueRequest, finalThreadID, fallbackID string, now time.Time) db.Message {
	var msg db.Message
	if req.Message != nil {
		msg = *req.Message
	} else {
		msg = db.Message{
			ID:          strings.TrimSpace(req.MessageID),
			ThreadID:    finalThreadID,
			AuthorID:    httpClientAuthorID,
			AuthorName:  "HTTP Client",
			IsAdmin:     true,
			Content:     req.Prompt,
			Summary:     db.CleanTaskSummary(req.Prompt),
			Effort:      ResolvePromptEffort(strings.TrimSpace(req.Model), strings.TrimSpace(req.Effort)),
			ChannelID:   finalThreadID,
			Model:       strings.TrimSpace(req.Model),
			Traceparent: strings.TrimSpace(req.Traceparent),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}
	if strings.TrimSpace(msg.Traceparent) == "" {
		msg.Traceparent = strings.TrimSpace(req.Traceparent)
	}
	if strings.TrimSpace(msg.ID) == "" {
		msg.ID = fallbackID
	}
	if strings.TrimSpace(msg.ThreadID) == "" {
		msg.ThreadID = finalThreadID
	}
	msg.Status = db.StatusPending
	return msg
}

// Enqueue records a message or prompt as a PENDING row and appends it to the final thread's
// coordinator. Thread resolution (redirect plus optional thread creation) is journaled once,
// so a retry never creates a second Discord thread.
func (t *ThreadCoordinator) Enqueue(ctx restate.ObjectSharedContext, req ThreadEnqueueRequest) (ThreadEnqueueResponse, error) {
	key := restate.Key(ctx)
	target := threadEnqueueTarget(key, req)
	if target == "" {
		return ThreadEnqueueResponse{}, restate.ToTerminalError(errors.New("thread_id or channel_id is required"), restate.WithErrorCode(400))
	}

	if req.Message == nil {
		if existing, found, err := t.lookupExistingPrompt(ctx, req.MessageID); err != nil || found {
			return existing, coordinatorError(err)
		}
	}

	final := target
	if req.Message == nil && t.resolver != nil {
		resolved, err := restate.Run(ctx, func(rc restate.RunContext) (string, error) {
			channelID := t.resolver.RedirectBuildPrompt(target, req.Prompt)
			return t.resolver.ResolveTargetThread(channelID, strings.TrimSpace(req.Title), req.Prompt, time.Now().UTC()), nil
		}, restate.WithName(StepResolveTargetThread))
		if err != nil {
			return ThreadEnqueueResponse{}, coordinatorError(err)
		}
		if resolved = strings.TrimSpace(resolved); resolved != "" {
			final = resolved
		}
	}

	if req.SkipIfActive && final == key {
		st, err := loadCoordinatorState(ctx)
		if err != nil {
			return ThreadEnqueueResponse{}, coordinatorError(err)
		}
		if st.busy() {
			return ThreadEnqueueResponse{Status: eriwire.StatusSkippedActive, ThreadID: final}, nil
		}
	}

	fallbackID := deriveDeterministicMessageID(ctx, final)
	msgID, err := t.recordQueuedMessage(ctx, req, final, fallbackID)
	if err != nil {
		return ThreadEnqueueResponse{}, coordinatorError(err)
	}

	restate.ObjectSend(ctx, t.ServiceName(), final, HandlerAppend).Send(AppendRequest{IDs: []string{msgID}, SkipIfActive: req.SkipIfActive})
	return ThreadEnqueueResponse{Status: eriwire.StatusAccepted, ThreadID: final, MessageID: msgID}, nil
}

func (t *ThreadCoordinator) recordQueuedMessage(ctx restate.ObjectSharedContext, req ThreadEnqueueRequest, final, fallbackID string) (string, error) {
	return restate.Run(ctx, func(rc restate.RunContext) (string, error) {
		msg := buildEnqueuedMessage(req, final, fallbackID, time.Now().UTC())
		if _, sErr := storeCall(rc, func(sc context.Context) (struct{}, error) {
			return struct{}{}, t.store.InsertMessage(sc, msg)
		}); sErr != nil {
			log.Printf("[ThreadSession] Error recording message %s: %v", msg.ID, sErr)
			return "", sErr
		}
		return msg.ID, nil
	}, restate.WithName(StepRecordQueued))
}

// existingPrompt is the journaled result of looking up a prompt's message row.
type existingPrompt struct {
	Found    bool   `json:"found"`
	ThreadID string `json:"thread_id,omitempty"`
}

// lookupExistingPrompt reports (with an AlreadyExists response) whether a row for the caller's
// message ID is already recorded, so a request replayed after its idempotency window never
// creates a second Discord thread or re-runs a finished prompt.
func (t *ThreadCoordinator) lookupExistingPrompt(ctx restate.ObjectSharedContext, rawID string) (ThreadEnqueueResponse, bool, error) {
	msgID := strings.TrimSpace(rawID)
	if msgID == "" {
		return ThreadEnqueueResponse{}, false, nil
	}
	found, err := restate.Run(ctx, func(rc restate.RunContext) (existingPrompt, error) {
		return storeCall(rc, func(sc context.Context) (existingPrompt, error) {
			m, gErr := t.store.GetMessage(sc, msgID)
			if gErr != nil && !errors.Is(gErr, sql.ErrNoRows) {
				return existingPrompt{}, gErr
			}
			if m == nil {
				return existingPrompt{}, nil
			}
			return existingPrompt{Found: true, ThreadID: m.ThreadID}, nil
		})
	}, restate.WithName(StepLookupExisting))
	if err != nil || !found.Found {
		return ThreadEnqueueResponse{}, false, err
	}
	return ThreadEnqueueResponse{Status: eriwire.StatusAlreadyExists, ThreadID: found.ThreadID, MessageID: msgID}, true, nil
}

// Append adds IDs to the thread's pending list (deduplicated) and starts a turn when idle; with
// SkipIfActive on a busy thread the new rows are recorded COMPLETED as skipped instead.
func (t *ThreadCoordinator) Append(ctx restate.ObjectContext, req AppendRequest) error {
	ids := req.IDs
	eval, ok := t.resolver.(ingressWakeEvaluator)
	if ok && eval != nil {
		allowed, err := t.filterAllowedIDs(ctx, req, eval)
		if err != nil {
			return coordinatorError(err)
		}
		ids = allowed
	}
	return t.core.append(ctx, AppendRequest{IDs: ids, SkipIfActive: req.SkipIfActive})
}

func (t *ThreadCoordinator) filterAllowedIDs(ctx restate.ObjectContext, req AppendRequest, eval ingressWakeEvaluator) ([]string, error) {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return nil, err
	}
	if req.SkipIfActive && st.busy() {
		return req.IDs, nil
	}
	fresh := make([]string, 0, len(req.IDs))
	for _, id := range normalizeStepIDs(req.IDs) {
		if !st.holds(id) {
			fresh = append(fresh, id)
		}
	}
	if len(fresh) == 0 {
		return req.IDs, nil
	}
	return t.gatePendingMessages(ctx, fresh, eval)
}

func (t *ThreadCoordinator) gatePendingMessages(ctx restate.ObjectContext, fresh []string, eval ingressWakeEvaluator) ([]string, error) {
	threadKey := restate.Key(ctx)
	return restate.Run(ctx, func(rc restate.RunContext) ([]string, error) {
		allowed := make([]string, 0, len(fresh))
		for _, id := range fresh {
			var msg *db.Message
			if _, sErr := storeCall(rc, func(sc context.Context) (struct{}, error) {
				var gErr error
				msg, gErr = t.store.GetMessage(sc, id)
				return struct{}{}, gErr
			}); sErr != nil {
				return nil, sErr
			}
			if msg == nil {
				continue
			}
			shouldEnqueue, completedReason := eval.EvaluateIngressWake(rc, threadKey, *msg)
			if shouldEnqueue {
				allowed = append(allowed, id)
			} else {
				if _, sErr := storeCall(rc, func(sc context.Context) (struct{}, error) {
					return struct{}{}, t.store.UpdateMessageStatus(sc, id, db.StatusCompleted, completedReason)
				}); sErr != nil {
					return nil, sErr
				}
			}
		}
		return allowed, nil
	}, restate.WithName(StepGateIngressWake))
}

// OnTurnDone ends the active turn reported by its TurnRunner (fenced on TurnID).
func (t *ThreadCoordinator) OnTurnDone(ctx restate.ObjectContext, req TurnDoneRequest) error {
	return t.core.onTurnDone(ctx, req, t.reschedule)
}

// reschedule puts the turn's IDs back at the front, keeps the turn active as Waiting and
// self-sends a delayed Kick.
func (t *ThreadCoordinator) reschedule(ctx restate.ObjectContext, st coordinatorState, req TurnDoneRequest) {
	st.pending = prependIDs(st.active.ItemIDs, st.pending)
	waiting := *st.active
	waiting.Waiting = true
	st.active = &waiting
	st.save(ctx)
	restate.ObjectSend(ctx, t.ServiceName(), restate.Key(ctx), HandlerKick).Send(KickRequest{TurnID: req.TurnID}, restate.WithDelay(time.Duration(max(req.RescheduleDelayMs, 0))*time.Millisecond))
}

// Kick ends a Waiting (rescheduled) turn and starts the next one; any other Kick is a no-op.
func (t *ThreadCoordinator) Kick(ctx restate.ObjectContext, req KickRequest) error {
	st, err := loadCoordinatorState(ctx)
	if err != nil {
		return coordinatorError(err)
	}
	if st.active == nil || !st.active.Waiting || st.active.TurnID != req.TurnID {
		return nil
	}
	st.active = nil
	t.core.startNext(ctx, st)
	return nil
}

// CancelTurn fails the thread's still-PENDING rows from a state snapshot and asks ClearPending
// to drop them and cancel the active turn.
func (t *ThreadCoordinator) CancelTurn(ctx restate.ObjectSharedContext, req CancelTurnRequest) (CancelTurnResponse, error) {
	st, failed, err := t.core.cancel(ctx, req.Reason)
	if err != nil {
		return CancelTurnResponse{}, err
	}
	return CancelTurnResponse{
		Status:              "ok",
		CancelledCount:      len(failed),
		CancelledActiveTurn: st.active != nil && !st.active.Waiting,
	}, nil
}

// DrainPending claims all pending message IDs into the active turn if turnID matches.
func (t *ThreadCoordinator) DrainPending(ctx restate.ObjectContext, req DrainPendingRequest) (DrainPendingResponse, error) {
	return t.core.drainPending(ctx, req)
}

// ClearPending removes IDs from pending and, when asked, cancels the active turn it covers.
func (t *ThreadCoordinator) ClearPending(ctx restate.ObjectContext, req ClearPendingRequest) error {
	return t.core.clearPending(ctx, req)
}

// Status returns the coordinator state.
func (t *ThreadCoordinator) Status(ctx restate.ObjectSharedContext) (CoordinatorStatus, error) {
	return t.core.status(ctx)
}
