package queue

import (
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/agent"
)

// Cross-chunk contract for the Restate turn queue: coordinators (ThreadSession,
// DispatchSession) and the stateless TurnRunner share these types.

// TurnRunnerServiceName returns the per-agent TurnRunner service name.
func TurnRunnerServiceName(name string) string {
	return agent.ScopedServiceName("TurnRunner", name)
}

// Terminal error codes. 409 is reserved for Restate's own cancellation; queue
// conflicts, fencing and stale-state terminals use 412.
const (
	CodeRestateCancelled = 409
	CodeQueueConflict    = 412
)

// CrashLoopCap is the restart_count at which a row is failed as a crash loop.
const CrashLoopCap = 3

// State keys of the coordinator objects.
const (
	StateKeyPending  = "pending"
	StateKeyActive   = "active"
	StateKeyIngress  = "ingress"
	StateKeyGated    = "gated"
	StateKeyDraining = "draining"
)

// Handler names.
const (
	HandlerEnqueue      = "Enqueue"
	HandlerAppend       = "Append"
	HandlerOnTurnDone   = "OnTurnDone"
	HandlerKick         = "Kick"
	HandlerClearPending = "ClearPending"
	HandlerCancelTurn   = "CancelTurn"
	HandlerCancel       = "Cancel"
	HandlerStatus       = "Status"
	HandlerDrainPending = "DrainPending"
	HandlerRunBurst     = "RunBurst"
	HandlerRunDispatch  = "RunDispatch"
)

// Step names.
const (
	StepRecordQueued         = "record-queued"
	StepGateIngressWake      = "gate-ingress-wake"
	StepResolveTargetThread  = "resolve-target-thread"
	StepLookupExisting       = "lookup-existing"
	StepRecordSkipped        = "record-skipped"
	StepRecordCancelled      = "record-cancelled"
	StepRecordStarted        = "record-started"
	StepPrep                 = "prep"
	StepRecordOutcome        = "record-outcome"
	StepPostTurnHook         = "post-turn-hook"
	StepRefreshThreadSummary = "refresh-thread-summary"
	StepPublishDispatchOut   = "publish-dispatch-outcome"
	StepStopTurn             = "stop-turn"
	StepFailTurn             = "fail-turn"
)

// TurnOutcome is how a turn ended.
type TurnOutcome string

const (
	OutcomeCompleted  TurnOutcome = "completed"
	OutcomeFailed     TurnOutcome = "failed"
	OutcomeTimeout    TurnOutcome = "timeout"
	OutcomeCancelled  TurnOutcome = "cancelled"
	OutcomeReschedule TurnOutcome = "reschedule"
	OutcomeEmpty      TurnOutcome = "empty"
)

type BurstTurnRequest struct {
	ThreadID   string   `json:"thread_id"`
	TurnID     string   `json:"turn_id"`
	MessageIDs []string `json:"message_ids"`
	Gated      bool     `json:"gated,omitempty"`
}

type DispatchTurnRequest struct {
	SessionKey string `json:"session_key"`
	TurnID     string `json:"turn_id"`
	DispatchID string `json:"dispatch_id"`
}

type TurnDoneRequest struct {
	TurnID            string      `json:"turn_id"`
	Outcome           TurnOutcome `json:"outcome"`
	RescheduleDelayMs int64       `json:"reschedule_delay_ms"`
}

type TurnRunResult struct {
	TurnID  string      `json:"turn_id"`
	Outcome TurnOutcome `json:"outcome"`
}

// ActiveTurn is the coordinator's record of the turn currently running.
type ActiveTurn struct {
	TurnID             string   `json:"turn_id"`
	ItemIDs            []string `json:"item_ids"`
	RunnerInvocationID string   `json:"runner_invocation_id"`
	Waiting            bool     `json:"waiting"`
}

type AppendRequest struct {
	IDs          []string `json:"ids,omitempty"`
	ReserveID    string   `json:"reserve_id,omitempty"`
	DropID       string   `json:"drop_id,omitempty"`
	SkipIfActive bool     `json:"skip_if_active,omitempty"`
	Gated        bool     `json:"gated,omitempty"`
	FromIngress  bool     `json:"from_ingress,omitempty"`
}

type KickRequest struct {
	TurnID string `json:"turn_id"`
}

type DrainPendingRequest struct {
	TurnID string   `json:"turn_id"`
	AckIDs []string `json:"ack_ids,omitempty"`
}

type DrainPendingResponse struct {
	DrainedIDs []string `json:"drained_ids,omitempty"`
}

// DrainedPrompt pairs a drained row ID with its prompt text.
type DrainedPrompt struct {
	ID     string `json:"id"`
	Prompt string `json:"prompt"`
}

type ClearPendingRequest struct {
	IDs          []string `json:"ids"`
	CancelActive bool     `json:"cancel_active"`
	TurnID       string   `json:"turn_id"`
}

type CoordinatorStatus struct {
	Pending []string    `json:"pending"`
	Active  *ActiveTurn `json:"active"`
}

// deriveTurnID derives a deterministic turn ID from the coordinator key and
// the invocation that started it.
func deriveTurnID(key, invocationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("aerial:turn:"+key+":"+invocationID)).String()
}

// classifyStoreError tags deterministic Postgres errors terminal (code 500) and
// leaves everything else retryable.
func classifyStoreError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch strings.ToUpper(pgErr.Code[:2]) {
		case "22", "23", "42", "0A", "2B", "2D", "25":
			return restate.ToTerminalError(err, restate.WithErrorCode(500))
		}
	}
	return err
}
