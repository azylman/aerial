package queue

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

// TurnRunner is the stateless Restate service that runs one turn of a coordinator key as
// journaled steps and always reports back to its coordinator with OnTurnDone:
//
//	record-started -> prep -> run-1..run-n (restate.Sleep between retries) -> record-outcome
//	-> post-turn-hook / refresh-thread-summary (bursts) | publish-dispatch-outcome (dispatches)
//	-> ObjectSend OnTurnDone
//
// No step carries retry options: a retryable step error is retried under Restate's server
// default policy (so a turn waits out a Postgres outage), deterministic errors are terminal at
// once. A terminal error at any await ends the turn through stop-turn (code 409, a Restate cancel)
// or fail-turn (anything else); both are idempotent, because a crash re-runs them on replay.

// TurnHoldTimeout bounds TurnRunner, whose invocation stays open across a whole turn attempt.
// Service options are fixed at registration, so it derives from
// config.MaxTurnTimeoutMinutes, never the live turn_timeout_minutes: the largest allowed cap,
// plus the attempt context's +1 minute pad, plus a 14 minute margin.
const TurnHoldTimeout = time.Duration(config.MaxTurnTimeoutMinutes+1+14) * time.Minute

// Turn runner timeouts. No retry policy is declared: the server default applies, which pauses an
// invocation once exhausted and never kills it.
const (
	TurnRunnerInactivityTimeout = TurnHoldTimeout
	TurnRunnerAbortTimeout      = TurnHoldTimeout
)

// turnSteps is the narrow set of WorkerPool step functions the TurnRunner journals.
type turnSteps interface {
	RecordBurstStarted(ctx context.Context, threadID string, ids []string) (StartedTurn, error)
	PrepareBurstTurn(ctx context.Context, threadID, turnID string, ids []string) (TurnPrep, error)
	RunBurstAttempt(ctx context.Context, in AttemptInput) (AttemptResult, error)
	RecordBurstOutcome(ctx context.Context, ids []string, summary AttemptSummary) error
	RunBurstPostTurnHook(ctx context.Context, threadID string, ids []string, attempted bool, outcome TurnOutcome, hookMetadata map[string]string) error
	RefreshBurstThreadSummary(ctx context.Context, threadID string, ids []string, outcome TurnOutcome) error
	StopBurstTurn(ctx context.Context, turnID string, ids []string) error
	FailBurstTurn(ctx context.Context, turnID string, ids []string, reason string, hookMetadata ...map[string]string) error

	RecordDispatchStarted(ctx context.Context, sessionKey, dispatchID string) (StartedTurn, error)
	PrepareDispatchTurn(ctx context.Context, sessionKey, turnID, dispatchID string) (TurnPrep, error)
	RunDispatchAttempt(ctx context.Context, in AttemptInput) (AttemptResult, error)
	RecordDispatchOutcome(ctx context.Context, ids []string, summary AttemptSummary) error
	PublishDispatchOutcome(ctx context.Context, dispatchID string, outcome TurnOutcome) error
	StopDispatchTurn(ctx context.Context, turnID string, ids []string) error
	FailDispatchTurn(ctx context.Context, turnID string, ids []string, reason string) error

	maxAttempts() int
}

var _ turnSteps = (*WorkerPool)(nil)

// TurnRunnerService is the per-agent TurnRunner Restate service.
type TurnRunnerService struct {
	steps     turnSteps
	agentName string
}

// NewTurnRunnerService builds the TurnRunner of agentName over steps.
func NewTurnRunnerService(steps turnSteps, agentName string) *TurnRunnerService {
	return &TurnRunnerService{steps: steps, agentName: agentName}
}

// ServiceName is the per-agent Restate service name.
func (s *TurnRunnerService) ServiceName() string {
	return TurnRunnerServiceName(s.agentName)
}

// TurnRunnerOptions are the service-wide options the TurnRunner is bound with.
type TurnRunnerOptions struct {
	InactivityTimeout time.Duration
	AbortTimeout      time.Duration
}

// TurnRunnerReflectOptions returns the TurnRunner's inactivity and abort timeouts, and nothing
// else: no retry policy, so Restate's server default applies.
func TurnRunnerReflectOptions() TurnRunnerOptions {
	return TurnRunnerOptions{InactivityTimeout: TurnRunnerInactivityTimeout, AbortTimeout: TurnRunnerAbortTimeout}
}

// ReflectTurnRunner reflects svc into a Restate service definition with TurnRunnerReflectOptions.
func ReflectTurnRunner(svc *TurnRunnerService) restate.ServiceDefinition {
	o := TurnRunnerReflectOptions()
	return restate.Reflect(svc, restate.WithInactivityTimeout(o.InactivityTimeout), restate.WithAbortTimeout(o.AbortTimeout))
}

// RunBurst runs one burst turn of a ThreadSession key.
func (s *TurnRunnerService) RunBurst(ctx restate.Context, req BurstTurnRequest) (TurnRunResult, error) {
	subj := burstRunner{steps: s.steps, threadID: req.ThreadID, ids: normalizeStepIDs(req.MessageIDs)}
	return s.drive(ctx, subj, ThreadSessionServiceName(s.agentName), req.ThreadID, req.TurnID)
}

// RunDispatch runs one dispatch turn of a DispatchSession key.
func (s *TurnRunnerService) RunDispatch(ctx restate.Context, req DispatchTurnRequest) (TurnRunResult, error) {
	subj := dispatchRunner{steps: s.steps, sessionKey: req.SessionKey, dispatchID: req.DispatchID, ids: normalizeStepIDs([]string{req.DispatchID})}
	return s.drive(ctx, subj, DispatchSessionServiceName(s.agentName), req.SessionKey, req.TurnID)
}

// runnerSubject is what differs between a burst turn and a dispatch turn. Every method runs
// inside one restate.Run closure and receives only its RunContext.
type runnerSubject interface {
	inputIDs() []string
	recordStarted(ctx context.Context) (StartedTurn, error)
	prep(ctx context.Context, turnID string, ids []string) (TurnPrep, error)
	attemptInput(turnID string, ids []string, prep TurnPrep, ordinal int) AttemptInput
	attempt(ctx context.Context, in AttemptInput) (AttemptResult, error)
	recordOutcome(ctx context.Context, ids []string, summary AttemptSummary) error
	// afterSteps are the journaled steps run after record-outcome.
	afterSteps(t turnProgress) []namedStep
	stop(ctx context.Context, turnID string, ids []string) error
	fail(ctx context.Context, turnID string, ids []string, reason string, hookMetadata ...map[string]string) error
}

// namedStep is one journaled post-outcome step.
type namedStep struct {
	name string
	fn   func(ctx context.Context) error
}

// turnProgress is what the handler knows about the turn so far.
type turnProgress struct {
	ids          []string
	attempted    bool
	outcome      TurnOutcome
	delayMs      int64
	hookMetadata map[string]string
	last         AttemptResult
	hookRan      bool
}

type burstRunner struct {
	steps    turnSteps
	threadID string
	ids      []string
}

func (b burstRunner) inputIDs() []string { return b.ids }

func (b burstRunner) recordStarted(ctx context.Context) (StartedTurn, error) {
	return b.steps.RecordBurstStarted(ctx, b.threadID, b.ids)
}

func (b burstRunner) prep(ctx context.Context, turnID string, ids []string) (TurnPrep, error) {
	return b.steps.PrepareBurstTurn(ctx, b.threadID, turnID, ids)
}

func (b burstRunner) attemptInput(turnID string, ids []string, prep TurnPrep, ordinal int) AttemptInput {
	return AttemptInput{ThreadID: b.threadID, TurnID: turnID, IDs: ids, Prep: prep, Ordinal: ordinal, RunIndex: 1}
}

func (b burstRunner) attempt(ctx context.Context, in AttemptInput) (AttemptResult, error) {
	return b.steps.RunBurstAttempt(ctx, in)
}

func (b burstRunner) recordOutcome(ctx context.Context, ids []string, summary AttemptSummary) error {
	return b.steps.RecordBurstOutcome(ctx, ids, summary)
}

func (b burstRunner) afterSteps(t turnProgress) []namedStep {
	if !t.attempted {
		return nil
	}
	out := []namedStep{{name: StepPostTurnHook, fn: func(ctx context.Context) error {
		return b.steps.RunBurstPostTurnHook(ctx, b.threadID, t.ids, true, t.outcome, t.hookMetadata)
	}}}
	if t.outcome == OutcomeCompleted {
		out = append(out, namedStep{name: StepRefreshThreadSummary, fn: func(ctx context.Context) error {
			return b.steps.RefreshBurstThreadSummary(ctx, b.threadID, t.ids, t.outcome)
		}})
	}
	return out
}

func (b burstRunner) stop(ctx context.Context, turnID string, ids []string) error {
	return b.steps.StopBurstTurn(ctx, turnID, ids)
}

func (b burstRunner) fail(ctx context.Context, turnID string, ids []string, reason string, hookMetadata ...map[string]string) error {
	return b.steps.FailBurstTurn(ctx, turnID, ids, reason, hookMetadata...)
}

type dispatchRunner struct {
	steps      turnSteps
	sessionKey string
	dispatchID string
	ids        []string
}

func (d dispatchRunner) inputIDs() []string { return d.ids }

func (d dispatchRunner) recordStarted(ctx context.Context) (StartedTurn, error) {
	return d.steps.RecordDispatchStarted(ctx, d.sessionKey, d.dispatchID)
}

func (d dispatchRunner) prep(ctx context.Context, turnID string, _ []string) (TurnPrep, error) {
	return d.steps.PrepareDispatchTurn(ctx, d.sessionKey, turnID, d.dispatchID)
}

func (d dispatchRunner) attemptInput(turnID string, ids []string, prep TurnPrep, ordinal int) AttemptInput {
	return AttemptInput{SessionKey: d.sessionKey, TurnID: turnID, IDs: ids, Prep: prep, Ordinal: ordinal, RunIndex: 1}
}

func (d dispatchRunner) attempt(ctx context.Context, in AttemptInput) (AttemptResult, error) {
	return d.steps.RunDispatchAttempt(ctx, in)
}

func (d dispatchRunner) recordOutcome(ctx context.Context, ids []string, summary AttemptSummary) error {
	return d.steps.RecordDispatchOutcome(ctx, ids, summary)
}

func (d dispatchRunner) afterSteps(t turnProgress) []namedStep {
	if !t.attempted || dispatchOutcomeStatus(t.outcome) == "" {
		return nil
	}
	ids := t.ids
	if len(ids) == 0 {
		ids = []string{d.dispatchID}
	}
	return []namedStep{{name: StepPublishDispatchOut, fn: func(ctx context.Context) error {
		for _, id := range ids {
			if err := d.steps.PublishDispatchOutcome(ctx, id, t.outcome); err != nil {
				return err
			}
		}
		return nil
	}}}
}

func (d dispatchRunner) stop(ctx context.Context, turnID string, ids []string) error {
	return d.steps.StopDispatchTurn(ctx, turnID, ids)
}

func (d dispatchRunner) fail(ctx context.Context, turnID string, ids []string, reason string, _ ...map[string]string) error {
	return d.steps.FailDispatchTurn(ctx, turnID, ids, reason)
}

// drive runs the turn and reports its outcome to the coordinator on every path.
func (s *TurnRunnerService) drive(ctx restate.Context, subj runnerSubject, coordinator, key, turnID string) (TurnRunResult, error) {
	var t turnProgress
	if err := s.executeTurn(ctx, subj, turnID, &t); err != nil {
		return endOnTerminal(ctx, subj, coordinator, key, turnID, err, t)
	}
	handledIDs := normalizeStepIDs(append(slices.Clone(subj.inputIDs()), t.ids...))
	sendTurnDone(ctx, coordinator, key, TurnDoneRequest{TurnID: turnID, Outcome: t.outcome, RescheduleDelayMs: t.delayMs, HandledIDs: handledIDs})
	return TurnRunResult{TurnID: turnID, Outcome: t.outcome}, nil
}

// executeTurn runs record-started through the post-outcome steps. Any error it returns is
// terminal: retryable step errors never surface here, Restate retries them.
func (s *TurnRunnerService) executeTurn(ctx restate.Context, subj runnerSubject, turnID string, t *turnProgress) error {
	started, err := runStep(ctx, StepRecordStarted, func(rc context.Context) (StartedTurn, error) { return subj.recordStarted(rc) })
	if err != nil {
		return err
	}
	if len(started.IDs) == 0 {
		t.outcome = OutcomeEmpty
		return nil
	}
	t.ids = started.IDs

	prep, err := runStep(ctx, StepPrep, func(rc context.Context) (TurnPrep, error) { return subj.prep(rc, turnID, started.IDs) })
	if err != nil {
		return err
	}
	t.hookMetadata = prep.HookMetadata
	decided, err := safeCall(func() (bool, error) { return applyPrepDecision(prep, t) })
	if err != nil || decided {
		return err
	}

	if runErr := s.runAttempts(ctx, subj, turnID, started, prep, t); runErr != nil {
		return runErr
	}

	summary, err := safeCall(func() (AttemptSummary, error) { return outcomeSummary(*t), nil })
	if err != nil {
		return err
	}
	if _, outErr := runStep(ctx, StepRecordOutcome, func(rc context.Context) (struct{}, error) {
		return struct{}{}, subj.recordOutcome(rc, t.ids, summary)
	}); outErr != nil {
		return outErr
	}
	after, err := safeCall(func() ([]namedStep, error) { return subj.afterSteps(*t), nil })
	if err != nil {
		return err
	}
	for _, step := range after {
		if _, err := runStep(ctx, step.name, func(rc context.Context) (struct{}, error) { return struct{}{}, step.fn(rc) }); err != nil {
			return err
		}
		if step.name == StepPostTurnHook {
			t.hookRan = true
		}
	}
	return nil
}

// applyPrepDecision applies a prep decision other than run; it reports whether the turn is decided.
func applyPrepDecision(prep TurnPrep, t *turnProgress) (bool, error) {
	switch prep.Decision {
	case TurnDecisionRun:
		return false, nil
	case TurnDecisionReschedule:
		t.outcome, t.delayMs = OutcomeReschedule, prep.DelayMs
		return true, nil
	case TurnDecisionFinished:
		// prep recorded every row terminal (ignored, ambient, aborted by pre_turn).
		t.outcome = OutcomeCompleted
		return true, nil
	default:
		return true, restate.ToTerminalError(fmt.Errorf("queue: unknown prep decision %q", prep.Decision), restate.WithErrorCode(500))
	}
}

// attemptAction is what the handler does after one run step.
type attemptAction int

const (
	actionDone attemptAction = iota
	actionResume
	actionRetry
)

// nextAttempt maps one run step's result to the next action, the next run input and the turn
// outcome. It is pure.
func nextAttempt(in AttemptInput, res AttemptResult, maxAttempts int) (attemptAction, AttemptInput, TurnOutcome, error) {
	next := in
	next.RunIndex = in.RunIndex + 1
	next.AutoResumes = res.AutoResumes
	next.Usage = res.Telemetry.Usage
	next.Prep.SessionID = res.SessionID
	if len(res.DrainedIDs) > 0 {
		next.IDs = normalizeStepIDs(append(slices.Clone(in.IDs), res.DrainedIDs...))
	}
	for _, p := range res.DrainedPrompts {
		if p = strings.TrimSpace(p); p != "" {
			next.Prep.Prompt = strings.TrimSpace(next.Prep.Prompt) + "\n\n" + p
		}
	}
	switch res.Kind {
	case AttemptKindSuccess:
		return actionDone, in, OutcomeCompleted, nil
	case AttemptKindTimeout:
		return actionDone, in, OutcomeTimeout, nil
	case AttemptKindTerminal:
		return actionDone, in, OutcomeFailed, nil
	case AttemptKindRecorded:
		if res.Reason == "" {
			return actionDone, in, OutcomeEmpty, nil
		}
		return actionDone, in, TurnOutcome(res.Reason), nil
	case AttemptKindResume:
		next.Resume, next.LastError = true, ""
		return actionResume, next, "", nil
	case AttemptKindRetry:
		if in.Ordinal >= maxAttempts {
			// The attempt step marks the last ordinal exhausted itself; never run past MaxAttempts.
			return actionDone, in, OutcomeFailed, nil
		}
		next.Ordinal = in.Ordinal + 1
		next.Resume, next.LastError = false, res.Reason
		return actionRetry, next, "", nil
	default:
		return actionDone, in, "", restate.ToTerminalError(fmt.Errorf("queue: unexpected attempt kind %q", res.Kind), restate.WithErrorCode(500))
	}
}

// runAttempts runs run-1..run-n: one lease execution per step inside restate.RunAsync, with
// restate.Sleep(BackoffMs) before the next ordinal.
func (s *TurnRunnerService) runAttempts(ctx restate.Context, subj runnerSubject, turnID string, started StartedTurn, prep TurnPrep, t *turnProgress) error {
	maxAttempts := s.steps.maxAttempts()
	in, err := safeCall(func() (AttemptInput, error) {
		return subj.attemptInput(turnID, started.IDs, prep, started.BaseRetry+1), nil
	})
	if err != nil {
		return err
	}
	for {
		runIn := in
		fut := restate.RunAsync(ctx, func(rc restate.RunContext) (AttemptResult, error) {
			stepCtx := restateingress.ContextWithTrace(rc, ctx)
			return safeCall(func() (AttemptResult, error) { return subj.attempt(stepCtx, runIn) })
		}, restate.WithName(fmt.Sprintf("run-%d", runIn.RunIndex)))
		res, resErr := fut.Result()
		if resErr != nil {
			return resErr
		}
		t.attempted = true
		t.last = res
		if len(res.DrainedIDs) > 0 {
			t.ids = normalizeStepIDs(append(t.ids, res.DrainedIDs...))
		}
		type step struct {
			action  attemptAction
			next    AttemptInput
			outcome TurnOutcome
		}
		st, err := safeCall(func() (step, error) {
			a, n, o, e := nextAttempt(runIn, res, maxAttempts)
			return step{a, n, o}, e
		})
		if err != nil {
			return err
		}
		switch st.action {
		case actionDone:
			t.outcome = st.outcome
			return nil
		case actionRetry:
			if err := restate.Sleep(ctx, time.Duration(res.BackoffMs)*time.Millisecond); err != nil {
				return err
			}
		case actionResume:
		}
		in = st.next
	}
}

// outcomeSummary is the record-outcome input for the turn so far.
func outcomeSummary(t turnProgress) AttemptSummary {
	summary := AttemptSummary{
		Outcome:    t.outcome,
		Stderr:     t.last.Stderr,
		DurationMs: t.last.Telemetry.DurationMs,
		Usage:      t.last.Telemetry.Usage,
	}
	if t.outcome != OutcomeCompleted && t.last.Kind != AttemptKindRecorded {
		summary.Reason = t.last.Reason
	}
	return summary
}

// endOnTerminal ends a turn whose await returned a terminal error: a Restate cancel (409) takes
// stop-turn, anything else fail-turn. OnTurnDone is sent either way.
func endOnTerminal(ctx restate.Context, subj runnerSubject, coordinator, key, turnID string, cause error, prog ...turnProgress) (TurnRunResult, error) {
	var t turnProgress
	if len(prog) > 0 {
		t = prog[0]
	}
	ids := normalizeStepIDs(append(slices.Clone(subj.inputIDs()), t.ids...))
	outcome := OutcomeFailed
	var stepErr error
	if isRestateCancel(cause) {
		outcome = OutcomeCancelled
		log.Printf("[TurnRunner] Turn %s for %s/%s cancelled", turnID, coordinator, key)
		_, stepErr = runStep(ctx, StepStopTurn, func(rc context.Context) (struct{}, error) {
			return struct{}{}, subj.stop(rc, turnID, ids)
		})
	} else {
		reason := cause.Error()
		log.Printf("[TurnRunner] Turn %s for %s/%s failed: %s", turnID, coordinator, key, reason)
		var meta map[string]string
		if !t.hookRan {
			meta = t.hookMetadata
		}
		_, stepErr = runStep(ctx, StepFailTurn, func(rc context.Context) (struct{}, error) {
			return struct{}{}, subj.fail(rc, turnID, ids, reason, meta)
		})
	}
	sendTurnDone(ctx, coordinator, key, TurnDoneRequest{TurnID: turnID, Outcome: outcome, HandledIDs: ids})
	if stepErr != nil {
		log.Printf("[TurnRunner] Turn %s for %s/%s: %s step failed: %v", turnID, coordinator, key, outcome, stepErr)
		return TurnRunResult{TurnID: turnID, Outcome: outcome}, stepErr
	}
	return TurnRunResult{TurnID: turnID, Outcome: outcome}, nil
}

// isRestateCancel reports whether err is Restate's own cancellation (terminal code 409). Queue
// code never emits 409, so this is never a conflict of ours.
func isRestateCancel(err error) bool {
	if !restate.IsTerminalError(err) {
		return false
	}
	return restate.AsTerminalError(err).Code() == CodeRestateCancelled
}

func sendTurnDone(ctx restate.Context, coordinator, key string, req TurnDoneRequest) {
	restate.ObjectSend(ctx, coordinator, key, HandlerOnTurnDone).Send(req)
}

// runStep is one named journaled step without retry options. fn receives only the RunContext
// and runs through safeCall, so a panic is a terminal failure instead of an endless retry.
func runStep[T any](ctx restate.Context, name string, fn func(rc context.Context) (T, error)) (T, error) {
	out, err := restate.Run(ctx, func(rc restate.RunContext) (T, error) {
		stepCtx := restateingress.ContextWithTrace(rc, ctx)
		return safeCall(func() (T, error) { return fn(stepCtx) })
	}, restate.WithName(name))
	if err != nil {
		return out, err
	}
	return out, nil
}

// safeCall runs fn, turning a panic into a terminal error (code 500) so the turn takes fail-turn.
// It must never wrap Restate context calls: the SDK suspends an invocation by panicking.
func safeCall[T any](fn func() (T, error)) (out T, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero T
			out, err = zero, restate.ToTerminalError(fmt.Errorf("turn runner panic: %v", r), restate.WithErrorCode(500))
		}
	}()
	return fn()
}
