package queue

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// One TurnRunner run-<n> step executes exactly one lease attempt through RunBurstAttempt /
// RunDispatchAttempt, so no step outlives the Restate timeouts and a crash re-runs at most one
// attempt. The step returns how the attempt ended; waiting out a retry backoff is the caller's
// restate.Sleep, never an in-process wait. retry_count is written only by BeginAttempt.

// Attempt result kinds.
const (
	AttemptKindSuccess  = "success"  // delivered and recorded COMPLETED
	AttemptKindResume   = "resume"   // yield trap: run again with the resume prompt, same ordinal
	AttemptKindRetry    = "retry"    // transient: sleep BackoffMs, then run the next ordinal
	AttemptKindTimeout  = "timeout"  // per-attempt time limit: notice delivered, rows FAILED "timeout"
	AttemptKindAborted  = "aborted"  // cancelled, interrupted or refused: nothing written
	AttemptKindTerminal = "terminal" // final failure, already recorded (rows FAILED with Reason)
	AttemptKindRecorded = "recorded" // every row was already terminal; Reason is the recorded outcome
)

// Row error of a turn failed after too many crash re-entries.
const crashLoopReason = "crash loop"

// Row error of a turn that hit its per-attempt time limit.
const attemptTimeoutReason = "timeout"

// AttemptInput is one run-<n> step's input. ThreadID addresses bursts, SessionKey dispatches.
type AttemptInput struct {
	ThreadID    string   `json:"thread_id,omitempty"`
	SessionKey  string   `json:"session_key,omitempty"`
	TurnID      string   `json:"turn_id"`
	IDs         []string `json:"ids"`
	Prep        TurnPrep `json:"prep"`
	Ordinal     int      `json:"ordinal"`
	RunIndex    int      `json:"run_index"`
	AutoResumes int      `json:"auto_resumes"`
	// Resume runs the yield-trap resume prompt instead of the turn prompt.
	Resume bool `json:"resume,omitempty"`
	// LastError is the previous attempt's error; it shapes the continuation prompt.
	LastError string `json:"last_error,omitempty"`
	// Usage is the token usage of the turn's earlier runs, folded into the success telemetry.
	Usage runner.TokenUsage `json:"usage"`
}

// AttemptTelemetry is one attempt's duration and token usage.
type AttemptTelemetry struct {
	DurationMs int64             `json:"duration_ms,omitempty"`
	Usage      runner.TokenUsage `json:"usage"`
}

// AttemptResult is how one attempt ended.
type AttemptResult struct {
	Kind           string           `json:"kind"`
	BackoffMs      int64            `json:"backoff_ms,omitempty"`
	Reason         string           `json:"reason,omitempty"`
	Stderr         string           `json:"stderr,omitempty"`
	SessionID      string           `json:"session_id,omitempty"`
	AutoResumes    int              `json:"auto_resumes"`
	DrainedIDs     []string         `json:"drained_ids,omitempty"`
	DrainedPrompts []string         `json:"drained_prompts,omitempty"`
	Telemetry      AttemptTelemetry `json:"telemetry"`
}

// failureKind is the category of a failed lease attempt in the TurnRunner attempt step.
type failureKind int

const (
	failureNonTransient failureKind = iota
	failureQuotaPause
	failureCapacityBlip
	failureWatchdog
	failureSessionCorrupt
	failureTransient
)

// classifyFailure categorizes a failed attempt in precedence order: quota (pause or
// capacity blip), watchdog, session corruption, transient, non-transient.
func classifyFailure(errDetail, stderr string, transient, sessionCorruption bool) failureKind {
	switch {
	case runner.IsQuotaPause(errDetail, stderr):
		if isCapacityBlip(errDetail, stderr) {
			return failureCapacityBlip
		}
		return failureQuotaPause
	case isWatchdogTimeoutError(errDetail, stderr):
		return failureWatchdog
	case sessionCorruption:
		return failureSessionCorrupt
	case transient:
		return failureTransient
	default:
		return failureNonTransient
	}
}

// isCapacityBlip mirrors handleQuotaPause's split between a short capacity blip and a quota pause.
func isCapacityBlip(errDetail, stderr string) bool {
	resetDur, _ := runner.ExtractQuotaResetDuration(errDetail, stderr)
	return runner.IsCapacityBlip(errDetail, stderr) || resetDur <= 5*time.Second
}

// attemptClass is what the TurnRunner attempt step does with an attempt's outcome.
type attemptClass int

const (
	classSuccess attemptClass = iota
	classResume
	classRetry
	classCapacityRetry
	classSessionCorrupt
	classTimeout
	classAborted
	classQuotaPause
	classCapacityExhausted
	classTerminal
	classExhausted
)

// attemptClassInput is everything classifyAttempt looks at, as plain values.
type attemptClassInput struct {
	Failed            bool
	ErrDetail, Stderr string
	Transient         bool
	SessionCorruption bool
	// SuccessErr fails a run the agent finished (empty response, unlatched cold session).
	SuccessErr     string
	YieldTrap      bool
	AutoResumes    int
	MaxAutoResumes int
	Ordinal        int
	MaxAttempts    int
	// ParentCancelled: the turn's parent context ended (stream drop, abort, restart).
	ParentCancelled bool
	// Interrupted: TurnRegistry.Stop interrupted the execution.
	Interrupted bool
	// RunDeadline: the attempt's own run deadline fired.
	RunDeadline bool
}

// classifyAttempt decides an attempt's class. It is pure.
func classifyAttempt(in attemptClassInput) attemptClass {
	if in.Interrupted || in.ParentCancelled {
		return classAborted
	}
	lastAttempt := in.Ordinal >= in.MaxAttempts
	if !in.Failed {
		switch {
		case in.SuccessErr != "":
			return classTerminal
		case in.YieldTrap && in.AutoResumes < in.MaxAutoResumes:
			return classResume
		default:
			return classSuccess
		}
	}
	if in.RunDeadline {
		return classTimeout
	}
	switch classifyFailure(in.ErrDetail, in.Stderr, in.Transient, in.SessionCorruption) {
	case failureQuotaPause:
		return classQuotaPause
	case failureCapacityBlip:
		if lastAttempt {
			return classCapacityExhausted
		}
		return classCapacityRetry
	case failureWatchdog:
		return classTimeout
	case failureSessionCorrupt:
		if lastAttempt {
			return classExhausted
		}
		return classSessionCorrupt
	case failureTransient:
		if lastAttempt {
			return classExhausted
		}
		return classRetry
	default:
		return classTerminal
	}
}

// timeoutReason names which timeout stopped an attempt classified classTimeout: the attempt's own
// run deadline (turn cap) wins, otherwise the inactivity watchdog. It is pure.
func timeoutReason(in attemptClassInput) string {
	if in.RunDeadline {
		return metrics.TurnTimeoutReasonCap
	}
	return metrics.TurnTimeoutReasonWatchdog
}

// attemptInterruptedError is the terminal (412) error of an attempt interrupted by
// TurnRegistry.Stop or refused by its tombstone: the handler is already past the await, so a
// retryable error would abort the invocation attempt instead of just being recorded.
func attemptInterruptedError(turnID string) error {
	return restate.ToTerminalError(fmt.Errorf("queue: turn %s attempt interrupted", turnID), restate.WithErrorCode(CodeQueueConflict))
}

// attemptRequeueTime returns when a resumed row re-entered the queue (zero on a first attempt).
func attemptRequeueTime(restartCount int, updatedAt time.Time) time.Time {
	if restartCount > 0 {
		return updatedAt
	}
	return time.Time{}
}

// attemptSpanStatus maps an attempt result kind to the status recorded on the attempt root span.
func attemptSpanStatus(kind string, teStatus string) string {
	switch kind {
	case AttemptKindSuccess, AttemptKindResume:
		return outcomeSuccess
	case AttemptKindTimeout:
		return string(OutcomeTimeout)
	case AttemptKindAborted:
		return string(OutcomeCancelled)
	default:
		if teStatus != "" {
			return teStatus
		}
		return outcomeFailed
	}
}

// attemptSubject is what differs between a burst attempt and a dispatch attempt.
type attemptSubject interface {
	// load re-reads the rows onto te; it returns the IDs still PROCESSING, or the recorded
	// outcome when none are.
	load(ctx context.Context, s db.Store, te *turnExecution) ([]string, TurnOutcome, error)
	beginAttempt(ctx context.Context, s db.Store, id string, ordinal int) (db.AttemptStart, error)
	applyAttemptStart(te *turnExecution, id string, start db.AttemptStart)
	// recordFailure delivers the failure notice (bursts) and fails the still-active rows.
	recordFailure(ctx context.Context, s db.Store, te *turnExecution, ids []string, summary AttemptSummary) error
	// ensureCompleted writes COMPLETED for rows a successful delivery left active.
	ensureCompleted(ctx context.Context, s db.Store, te *turnExecution, ids []string) error
	startIndicators(te *turnExecution) func()
	traceItems(te *turnExecution) []queuedTurnItem
}

// RunBurstAttempt executes one lease attempt of a burst turn (one run-<n> step).
func (p *WorkerPool) RunBurstAttempt(ctx context.Context, in AttemptInput) (AttemptResult, error) {
	return p.runAttempt(ctx, in, burstSubject{pool: p, ids: in.IDs})
}

// RunDispatchAttempt executes one lease attempt of a dispatch turn (one run-<n> step).
func (p *WorkerPool) RunDispatchAttempt(ctx context.Context, in AttemptInput) (AttemptResult, error) {
	return p.runAttempt(ctx, in, dispatchSubject{pool: p, ids: in.IDs})
}

//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) runAttempt(ctx context.Context, in AttemptInput, subj attemptSubject) (res AttemptResult, err error) {
	s, err := p.stepStore()
	if err != nil {
		return AttemptResult{}, err
	}
	turnCtx, turnCancel := context.WithCancel(ctx)
	defer turnCancel()
	te := &turnExecution{
		pool:       p,
		threadID:   in.ThreadID,
		turnID:     in.TurnID,
		execStart:  time.Now().UTC(),
		wakeIdx:    -1,
		stopTyping: func() {},
		turnCtx:    turnCtx,
		turnCancel: turnCancel,
	}

	// (a) Begin first: it may wait out an orphaned closure of this turn that records the outcome.
	release, ok := p.turns.Begin(ctx, in.TurnID, te)
	if !ok {
		if p.turns.Tombstoned(in.TurnID) {
			return AttemptResult{Kind: AttemptKindAborted, Reason: CancelledByOperatorReason}, attemptInterruptedError(in.TurnID)
		}
		return AttemptResult{Kind: AttemptKindAborted, Reason: "context cancelled"}, ctx.Err()
	}
	defer release()
	defer func() {
		if r := recover(); r != nil {
			te.logCtxf(ctx, "[TurnRunner] Panic in attempt %d of turn %s: %v", in.Ordinal, in.TurnID, r)
			res, err = AttemptResult{}, restate.ToTerminalError(fmt.Errorf("panic: %v", r), restate.WithErrorCode(500))
		}
	}()

	// (b) Re-read the rows after Begin.
	ids, recorded, err := subj.load(ctx, s, te)
	if err != nil {
		return AttemptResult{}, err
	}
	if len(ids) == 0 {
		return AttemptResult{Kind: AttemptKindRecorded, Reason: string(recorded), AutoResumes: in.AutoResumes}, nil
	}

	// (c) Record the attempt start; a re-entry counts a crash.
	if res, done, err := beginAttemptRows(ctx, s, te, subj, in, ids); done || err != nil {
		return res, err
	}

	// (d) One lease execution.
	return p.executeAttempt(ctx, s, te, subj, in, ids)
}

// beginAttemptRows runs BeginAttempt per row and fails the turn as a crash loop at the cap.
//
// A yield-trap resume run repeats its ordinal, so its BeginAttempt always reports a re-entry and
// bumps restart_count once; the cap allows one such bump per auto-resume of the turn so far
// (AutoResumes), and a resume run that really crashed and is replayed bumps again and counts.
func beginAttemptRows(ctx context.Context, s db.Store, te *turnExecution, subj attemptSubject, in AttemptInput, ids []string) (AttemptResult, bool, error) {
	crashCap := CrashLoopCap + max(in.AutoResumes, 0)
	crashLoop := false
	for _, id := range ids {
		start, err := storeCall(ctx, func(c context.Context) (db.AttemptStart, error) { return subj.beginAttempt(c, s, id, in.Ordinal) })
		if errors.Is(err, db.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return AttemptResult{}, false, err
		}
		subj.applyAttemptStart(te, id, start)
		if start.Reentry && !in.Resume {
			te.logCtxf(ctx, "[TurnRunner] Turn %s re-entered attempt %d for %s (restart_count=%d)", in.TurnID, in.Ordinal, id, start.RestartCount)
		}
		if start.RestartCount >= crashCap {
			crashLoop = true
		}
	}
	if !crashLoop {
		return AttemptResult{}, false, nil
	}
	te.logCtxf(ctx, "[TurnRunner] Turn %s hit the crash loop cap (%d restarts); failing it", in.TurnID, CrashLoopCap)
	summary := AttemptSummary{Outcome: OutcomeFailed, Reason: crashLoopReason, NoticePrefix: "execution failed"}
	if err := subj.recordFailure(ctx, s, te, ids, summary); err != nil {
		return AttemptResult{}, true, err
	}
	return AttemptResult{Kind: AttemptKindTerminal, Reason: crashLoopReason, AutoResumes: in.AutoResumes}, true, nil
}

// attemptRun is the raw result of one lease execution.
type attemptRun struct {
	outcome     runner.TurnOutcome
	turnRes     *runner.TurnResult
	stderr      string
	execErr     error
	runDeadline bool
	leaseErr    error
	poolMissing bool
}

func (te *turnExecution) initTurnPrompt(basePrompt string) {
	te.turnPrompt = basePrompt
	te.steerMu.Lock()
	for _, dp := range te.drainedPrompts {
		if dp = strings.TrimSpace(dp); dp != "" && !strings.Contains(te.turnPrompt, dp) {
			te.turnPrompt += "\n\n" + dp
		}
	}
	te.steerMu.Unlock()
}

//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) executeAttempt(ctx context.Context, s db.Store, te *turnExecution, subj attemptSubject, in AttemptInput, ids []string) (res AttemptResult, err error) {
	maxAttempts := p.maxAttempts()
	cfg := te.resolveAttemptConfig(ctx, in.Ordinal)
	model := cfg.model
	te.turnModel = model
	te.initTurnPrompt(in.Prep.Prompt)
	te.injectedHookContext = in.Prep.InjectedContext
	te.turnAttempted = true
	te.hookMetadata = hookMetadataAny(in.Prep.HookMetadata)

	_, span := startTurnSpan(ctx, p.tracer(), turnSpanSpec{
		items:          subj.traceItems(te),
		conversationID: te.threadID,
		agentName:      p.cfg.AgentName,
		triggerType:    te.triggerType,
		pickup:         te.execStart,
	})
	te.turnSpan = span
	defer func() {
		if p.cfg.TraceContent {
			span.SetAttributes(runner.ContentAttributes(te.sentPrompt, te.turnResponseText)...)
		}
		finishTurnSpan(span, te.turnModel, te.turnTokenUsage, attemptSpanStatus(res.Kind, te.turnStatus))
	}()

	if te.checkGlobalQuotaLockout(cfg.apiKey, model) {
		return AttemptResult{Kind: AttemptKindTerminal, Reason: "global quota pause active", AutoResumes: in.AutoResumes}, nil
	}

	defer te.enqueueTranscriptPublish()
	stopIndicators := subj.startIndicators(te)
	defer stopIndicators()
	defer func() {
		te.clearActiveTurn()
		te.activeSessionMu.Lock()
		lease := te.lease
		te.lease = nil
		te.activeSessionMu.Unlock()
		if lease != nil {
			logInterruptWarning("release attempt lease", lease.Release())
		}
	}()

	run := te.runOneLease(cfg, in)
	te.turnDurationMs = time.Since(te.execStart).Milliseconds()

	var st attemptOutcomeState
	if run.leaseErr != nil {
		// A lease that could not be taken is a transient failure of this attempt; a missing pool is not.
		st = attemptOutcomeState{isFailure: true, isTransient: !run.poolMissing, errDetail: run.leaseErr.Error(), stderr: run.leaseErr.Error()}
	} else {
		st = te.unpackAttemptOutcome(run.outcome, run.turnRes, run.stderr, run.execErr)
	}
	usage := in.Usage
	if st.isFailure {
		addUsage(&usage, run.outcome.Usage)
		te.turnTokenUsage = usage
		if run.leaseErr == nil {
			st.errDetail, st.stderr = te.enrichFailureFromTranscript(run.outcome, st.stdout, st.stderr, st.isSessionCorruption)
		}
	}

	classIn := attemptClassInput{
		Failed: st.isFailure, ErrDetail: st.errDetail, Stderr: st.stderr, Transient: st.isTransient, SessionCorruption: st.isSessionCorruption,
		AutoResumes: in.AutoResumes, MaxAutoResumes: MaxYieldTrapAutoResumes, Ordinal: in.Ordinal, MaxAttempts: maxAttempts,
		ParentCancelled: ctx.Err() != nil, Interrupted: te.isInterrupted(), RunDeadline: run.runDeadline,
	}
	if !st.isFailure && st.resp != nil {
		classIn.SuccessErr, classIn.YieldTrap = te.inspectSuccess(st)
		if classIn.SuccessErr != "" {
			st.errDetail = classIn.SuccessErr
		}
	}
	class := classifyAttempt(classIn)
	runStatus := outcomeSuccess
	if st.isFailure || class == classAborted {
		runStatus = outcomeError
	}
	metrics.RecordRunnerExecution(runStatus, model, te.triggerType, time.Since(te.execStart))

	te.steerMu.Lock()
	if len(te.drainedIDs) > 0 {
		ids = normalizeStepIDs(append(ids, te.drainedIDs...))
	}
	drainedIDsClone := slices.Clone(te.drainedIDs)
	drainedPromptsClone := slices.Clone(te.drainedPrompts)
	te.steerMu.Unlock()

	base := AttemptResult{
		AutoResumes:    in.AutoResumes,
		DrainedIDs:     drainedIDsClone,
		DrainedPrompts: drainedPromptsClone,
		Telemetry:      AttemptTelemetry{DurationMs: te.turnDurationMs, Usage: usage},
	}
	switch class {
	case classAborted:
		return abortedAttempt(ctx, te, in, base)
	case classSuccess:
		return p.succeedAttempt(ctx, s, te, subj, in, ids, st, base, model, maxAttempts)
	case classResume:
		return te.resumeAttempt(st, base, model), nil
	default:
		f := attemptFailure{class: class, ordinal: in.Ordinal, maxAttempts: maxAttempts, errDetail: st.errDetail, stderr: st.stderr, model: model, cfg: cfg}
		if class == classTimeout {
			f.timeoutReason = timeoutReason(classIn)
		}
		return p.failAttempt(ctx, s, te, subj, f, ids, base)
	}
}

// runOneLease takes the lease and executes the agent once under the attempt's own run deadline.
func (te *turnExecution) runOneLease(cfg attemptConfig, in AttemptInput) attemptRun {
	te.steerMu.Lock()
	te.steeringClosed = false
	te.steerMu.Unlock()
	defer te.closeSteering()

	poolErr, leaseErr := te.acquireTurnLease()
	if poolErr != nil {
		return attemptRun{leaseErr: poolErr, poolMissing: true}
	}
	if leaseErr != nil {
		return attemptRun{leaseErr: fmt.Errorf("queue: failed to acquire session lease for thread %s: %w", te.threadID, leaseErr)}
	}
	te.registerActiveTurn()
	prompt := te.turnPrompt
	if in.Resume {
		prompt = YieldTrapResumePrompt
	}
	prompt = te.prepareAttemptPrompt(in.Ordinal, prompt, in.LastError, in.LastError)
	baseCtx := te.turnCtxValue()
	if te.turnSpan != nil {
		baseCtx = trace.ContextWithSpan(baseCtx, te.turnSpan)
	}
	runCtx, runCancel := context.WithTimeout(baseCtx, attemptRunTimeout(cfg.timeoutMinutes))
	defer runCancel()
	outcome, turnRes, stderr, execErr := te.executeLeaseAttemptCtx(runCtx, cfg.model, prompt)
	return attemptRun{
		outcome: outcome, turnRes: turnRes, stderr: stderr, execErr: execErr,
		runDeadline: errors.Is(execErr, context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.DeadlineExceeded),
	}
}

// inspectSuccess applies the success checks that precede delivery: the cold-start session
// latch and yield-trap detection. It returns the error failing the run, if any, and whether the
// run yielded prematurely.
func (te *turnExecution) inspectSuccess(st attemptOutcomeState) (string, bool) {
	extSess := st.resp.ConversationID
	if extSess == "" {
		extSess = runner.ExtractSessionID(st.stdout+"\n"+st.stderr, te.execStart)
	}
	if te.currentSessionID == "" && (extSess == "" || !runner.IsValidUUID(extSess)) {
		return "failed to latch active session UUID on cold start", false
	}
	te.resolveAndPersistSessionID(extSess, st.stdout, st.stderr, true)
	isYield, _ := runner.IsYieldTrap(0, st.stdout, st.stderr)
	return "", isYield
}

// abortedAttempt writes nothing: an interrupted or refused attempt returns a terminal 412, a
// parent-context cancel the raw context error so the step is retried on replay.
func abortedAttempt(ctx context.Context, te *turnExecution, in AttemptInput, base AttemptResult) (AttemptResult, error) {
	base.Kind = AttemptKindAborted
	if te.isInterrupted() {
		base.Reason = te.getInterruptReason()
		te.logCtxf(ctx, "[TurnRunner] Attempt %d of turn %s interrupted: %s", in.Ordinal, in.TurnID, base.Reason)
		return base, attemptInterruptedError(in.TurnID)
	}
	err := ctx.Err() // classAborted without an interrupt means the parent context ended
	base.Reason = err.Error()
	te.logCtxf(ctx, "[TurnRunner] Attempt %d of turn %s aborted: %v", in.Ordinal, in.TurnID, err)
	return base, err
}

// succeedAttempt delivers and records the reply (delivery then COMPLETED).
//
//nolint:contextcheck,nolintlint // the reused delivery helpers own their contexts; store calls take ctx.
func (p *WorkerPool) succeedAttempt(ctx context.Context, s db.Store, te *turnExecution, subj attemptSubject, in AttemptInput, ids []string, st attemptOutcomeState, base AttemptResult, model string, maxAttempts int) (AttemptResult, error) {
	if isYieldTrapCircuitBroken(st, in.AutoResumes) {
		metrics.RecordYieldTrap("circuit_breaker", "stderr_signature", model)
		metrics.RecordRunnerError("yield_trap_circuit_breaker", model)
	}
	ok, emptyErr := te.deliverSuccessAndFinalize(st.resp, in.Usage, model, in.Ordinal, maxAttempts)
	if !ok {
		return p.failAttempt(ctx, s, te, subj, attemptFailure{class: classTerminal, ordinal: in.Ordinal, maxAttempts: maxAttempts, errDetail: emptyErr, stderr: st.stderr, model: model, cfg: te.resolveAttemptConfig(ctx, 0)}, ids, base)
	}
	if err := subj.ensureCompleted(ctx, s, te, ids); err != nil {
		return AttemptResult{}, err
	}
	base.Kind = AttemptKindSuccess
	base.SessionID = te.currentSessionID
	base.Telemetry = AttemptTelemetry{DurationMs: te.turnDurationMs, Usage: te.turnTokenUsage}
	return base, nil
}

func isYieldTrapCircuitBroken(st attemptOutcomeState, autoResumes int) bool {
	isYield, _ := runner.IsYieldTrap(0, st.stdout, st.stderr)
	return isYield && autoResumes >= MaxYieldTrapAutoResumes
}

// resumeAttempt reports a yield trap under the auto-resume cap; the next run uses the resume prompt.
func (te *turnExecution) resumeAttempt(st attemptOutcomeState, base AttemptResult, model string) AttemptResult {
	addUsage(&base.Telemetry.Usage, st.resp.Usage)
	te.turnTokenUsage = base.Telemetry.Usage
	base.Kind = AttemptKindResume
	base.AutoResumes++
	base.SessionID = te.currentSessionID
	te.logf("[YieldTrap] Intercepted background command exit for thread %s in session %s (auto-resume %d/%d): auto-resuming turn.",
		te.threadID, te.currentSessionID, base.AutoResumes, MaxYieldTrapAutoResumes)
	metrics.RecordYieldTrap("resumed", "stderr_signature", model)
	metrics.RecordRunnerError("yield_trap_intercepted", model)
	return base
}

// attemptFailure carries a failed attempt's class and details to failAttempt.
type attemptFailure struct {
	class                attemptClass
	ordinal, maxAttempts int
	errDetail, stderr    string
	model                string
	cfg                  attemptConfig
	// timeoutReason is the metrics.TurnTimeoutReason* that stopped a classTimeout attempt.
	timeoutReason string
}

// failAttempt applies a failure class: retries return a backoff; timeouts and terminal failures
// record their rows before returning.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) failAttempt(ctx context.Context, s db.Store, te *turnExecution, subj attemptSubject, f attemptFailure, ids []string, base AttemptResult) (AttemptResult, error) {
	base.Reason, base.Stderr, base.SessionID = f.errDetail, f.stderr, te.currentSessionID
	te.logf("[TurnRunner] Turn for %s failed attempt %d/%d (class=%d): %s", te.threadID, f.ordinal, f.maxAttempts, f.class, f.errDetail)
	switch f.class {
	case classRetry:
		metrics.RecordRunnerError("transient", f.model)
		if te.lease != nil {
			te.lease.MarkDirty()
		}
		base.Kind, base.BackoffMs = AttemptKindRetry, retryBackoff(f.ordinal, p.cfg.BackoffBase).Milliseconds()
		return base, nil
	case classCapacityRetry:
		resetDur, _ := runner.ExtractQuotaResetDuration(f.errDetail, f.stderr)
		base.Kind, base.BackoffMs = AttemptKindRetry, capacityRetryDelay(f.ordinal, resetDur, p.cfg.RetryDelayOverride).Milliseconds()
		return base, nil
	case classSessionCorrupt:
		metrics.RecordRunnerError("session_corrupt", f.model)
		te.deliverFailureNotice(f.cfg.agyBin, f.cfg.apiKey, f.errDetail, f.stderr, "session reset due to context corruption", "session reset notice")
		te.rotateSessionID(te.threadID, "")
		base.Kind, base.BackoffMs, base.SessionID = AttemptKindRetry, retryBackoff(f.ordinal, p.cfg.BackoffBase).Milliseconds(), ""
		return base, nil
	case classQuotaPause, classCapacityExhausted:
		// Today's quota side effects: notice, global lockout (and haro retry for quota pauses), rows FAILED.
		effort, _ := te.turnEffortAndModel()
		te.handleQuotaPause(f.errDetail, f.stderr, f.model, effort, f.ordinal, f.maxAttempts)
		base.Kind = AttemptKindTerminal
		return base, nil
	case classTimeout, classTerminal, classExhausted, classSuccess, classResume, classAborted:
		// Final failures are recorded below; success, resume and aborted never reach failAttempt.
	}
	summary, kind := attemptFailureSummary(f)
	if f.class == classTimeout || f.class == classTerminal {
		te.rotateSessionID(te.threadID, "")
	}
	metrics.RecordTurnCompleted(postTurnStatus(summary.Outcome), te.triggerType, f.model, time.Since(te.execStart))
	if f.class == classTimeout {
		metrics.RecordTurnTimeout(f.timeoutReason)
	}
	summary.DurationMs, summary.Usage = base.Telemetry.DurationMs, base.Telemetry.Usage
	if err := subj.recordFailure(ctx, s, te, ids, summary); err != nil {
		return AttemptResult{}, err
	}
	base.Kind, base.Reason = kind, summary.failureReason()
	return base, nil
}

// attemptFailureSummary maps a final failure class to the rows' outcome, reason and notice.
func attemptFailureSummary(f attemptFailure) (AttemptSummary, string) {
	switch f.class {
	case classTimeout:
		return AttemptSummary{Outcome: OutcomeTimeout, Reason: attemptTimeoutReason, Stderr: f.stderr}, AttemptKindTimeout
	case classExhausted:
		return AttemptSummary{Outcome: OutcomeFailed, Reason: f.errDetail, Stderr: f.stderr, NoticePrefix: fmt.Sprintf("execution failed after exhausting %d attempts", f.maxAttempts)}, AttemptKindTerminal
	default:
		return AttemptSummary{Outcome: OutcomeFailed, Reason: f.errDetail, Stderr: f.stderr, NoticePrefix: "execution failed with non-transient error"}, AttemptKindTerminal
	}
}

func addUsage(dst *runner.TokenUsage, u runner.TokenUsage) {
	dst.InputTokens += u.InputTokens
	dst.OutputTokens += u.OutputTokens
	dst.ThinkingTokens += u.ThinkingTokens
	dst.CacheReadTokens += u.CacheReadTokens
	dst.TotalTokens += u.TotalTokens
	dst.CacheCreationTokens += u.CacheCreationTokens
}

// recordedOutcome is the outcome recorded by rows none of which is PROCESSING: completed when any
// row completed, cancelled or timeout when every failed row says so, reschedule when rows were
// requeued, empty when no row exists.
func recordedOutcome(statuses, failedErrs []string) TurnOutcome {
	switch {
	case len(statuses) == 0:
		return OutcomeEmpty
	case slices.Contains(statuses, db.StatusCompleted):
		return OutcomeCompleted
	case len(failedErrs) == 0:
		return OutcomeReschedule
	case !slices.ContainsFunc(failedErrs, func(e string) bool { return e != CancelledByOperatorReason && e != InterruptedByOperatorReason }):
		return OutcomeCancelled
	case !slices.ContainsFunc(failedErrs, func(e string) bool { return e != attemptTimeoutReason }):
		return OutcomeTimeout
	default:
		return OutcomeFailed
	}
}

// burstSubject runs attempts over a burst's message rows.
type burstSubject struct {
	pool *WorkerPool
	ids  []string
}

func (p *WorkerPool) currentTimezone() string {
	if p != nil && p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			return cur.Timezone
		}
	}
	return ""
}

const turnItemsSessionPrefix = "turn-items-"

func recordTurnOwnedIDs(ctx context.Context, s db.Store, turnID string, ids []string) error {
	turnID = strings.TrimSpace(turnID)
	norm := normalizeStepIDs(ids)
	if turnID == "" || len(norm) == 0 || s == nil {
		return nil
	}
	key := turnItemsSessionPrefix + turnID
	existingRaw, err := storeCall(ctx, func(c context.Context) (string, error) {
		sum, _, gErr := s.GetThreadSummary(c, key)
		return sum, gErr
	})
	if err != nil {
		return err
	}
	var merged []string
	if existingRaw != "" {
		merged = strings.Split(existingRaw, ",")
	}
	merged = normalizeStepIDs(append(merged, norm...))
	_, err = storeCall(ctx, func(c context.Context) (struct{}, error) {
		return struct{}{}, s.SaveThreadSummary(c, key, strings.Join(merged, ","), "")
	})
	return err
}

func loadTurnOwnedExtraIDs(ctx context.Context, s db.Store, turnID string, knownIDs []string) ([]string, error) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" || s == nil {
		return nil, nil
	}
	raw, err := storeCall(ctx, func(c context.Context) (string, error) {
		sum, _, gErr := s.GetThreadSummary(c, turnItemsSessionPrefix+turnID)
		return sum, gErr
	})
	if err != nil || raw == "" {
		return nil, err
	}
	var extra []string
	for _, id := range normalizeStepIDs(strings.Split(raw, ",")) {
		if !slices.Contains(knownIDs, id) {
			extra = append(extra, id)
		}
	}
	return extra, nil
}

func recoverBurstProcessingRows(ctx context.Context, s db.Store, turnID, threadID string, knownIDs []string) ([]db.Message, error) {
	extraIDs, err := loadTurnOwnedExtraIDs(ctx, s, turnID, knownIDs)
	if err != nil || len(extraIDs) == 0 {
		return nil, err
	}
	rows, err := messagesByIDOrdered(ctx, s, extraIDs)
	if err != nil {
		return nil, err
	}
	threadID = strings.TrimSpace(threadID)
	var out []db.Message
	for _, m := range rows {
		if m.Status == db.StatusProcessing && (threadID == "" || m.ThreadID == threadID) {
			out = append(out, m)
		}
	}
	return out, nil
}

func recoverDispatchProcessingIDs(ctx context.Context, s db.Store, turnID, sessionID string, knownIDs []string) ([]string, error) {
	extraIDs, err := loadTurnOwnedExtraIDs(ctx, s, turnID, knownIDs)
	if err != nil || len(extraIDs) == 0 {
		return nil, err
	}
	sessionID = strings.TrimSpace(sessionID)
	var out []string
	for _, id := range extraIDs {
		d, gErr := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, id) })
		if errors.Is(gErr, db.ErrRecordNotFound) || (gErr == nil && (d == nil || d.Status != db.StatusProcessing)) {
			continue
		}
		if gErr != nil {
			return nil, gErr
		}
		if sessionID == "" || d.SessionID == sessionID {
			out = append(out, id)
		}
	}
	return out, nil
}

func (b burstSubject) load(ctx context.Context, s db.Store, te *turnExecution) ([]string, TurnOutcome, error) {
	knownIDs := normalizeStepIDs(b.ids)
	rows, err := messagesByIDOrdered(ctx, s, knownIDs)
	if err != nil {
		return nil, "", err
	}
	var processing []db.Message
	var statuses, failedErrs []string
	for _, m := range rows {
		if m.Status == db.StatusProcessing {
			processing = append(processing, m)
			continue
		}
		statuses = append(statuses, m.Status)
		if m.Status == db.StatusFailed {
			failedErrs = append(failedErrs, m.ErrorMessage)
		}
	}
	if len(processing) == 0 {
		return nil, recordedOutcome(statuses, failedErrs), nil
	}
	threadID := processing[0].ThreadID
	if threadID == "" {
		threadID = te.threadID
	}
	extraRows, err := recoverBurstProcessingRows(ctx, s, te.turnID, threadID, knownIDs)
	if err != nil {
		return nil, "", err
	}
	if len(extraRows) > 0 {
		processing = append(processing, extraRows...)
		te.steerMu.Lock()
		for _, row := range extraRows {
			te.drainedIDs = normalizeStepIDs(append(te.drainedIDs, row.ID))
		}
		if coalesced := strings.TrimSpace(CoalesceBurstPrompt(extraRows, b.pool.currentTimezone())); coalesced != "" {
			te.drainedPrompts = append(te.drainedPrompts, coalesced)
		}
		te.steerMu.Unlock()
	}
	te.activeSessionMu.Lock()
	te.burst = processing
	te.triggerType = resolveBurstTriggerType(processing[0])
	te.activeSessionMu.Unlock()
	ids := make([]string, len(processing))
	for i, m := range processing {
		ids[i] = m.ID
	}
	return ids, "", nil
}

func (burstSubject) beginAttempt(ctx context.Context, s db.Store, id string, ordinal int) (db.AttemptStart, error) {
	return s.BeginMessageAttempt(ctx, id, ordinal)
}

func (burstSubject) applyAttemptStart(te *turnExecution, id string, start db.AttemptStart) {
	for i := range te.burst {
		if te.burst[i].ID == id {
			te.burst[i].RetryCount, te.burst[i].RestartCount = start.RetryCount, start.RestartCount
		}
	}
}

func (b burstSubject) recordFailure(ctx context.Context, s db.Store, te *turnExecution, ids []string, summary AttemptSummary) error {
	var turnID string
	if te != nil {
		turnID = te.turnID
	}
	_, err := b.pool.failActiveBurstRows(ctx, s, turnID, ids, summary)
	return err
}

func (burstSubject) ensureCompleted(ctx context.Context, s db.Store, te *turnExecution, ids []string) error {
	rows, err := messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return err
	}
	telemetry := te.turnTelemetry()
	for _, m := range rows {
		if !isActiveStatus(m.Status) {
			continue
		}
		if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
			return struct{}{}, s.UpdateMessageCompleted(c, m.ID, te.turnResponseText, te.currentSessionID, telemetry...)
		}); err != nil {
			return err
		}
	}
	te.steerMu.Lock()
	undelivered := removeIDs(slices.Clone(te.pendingDrainIDs), ids)
	te.pendingDrainIDs = nil
	te.steerMu.Unlock()
	if len(undelivered) > 0 {
		uRows, uErr := messagesByIDOrdered(ctx, s, undelivered)
		if uErr != nil {
			return uErr
		}
		for _, m := range uRows {
			if m.Status != db.StatusProcessing {
				continue
			}
			if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
				return struct{}{}, s.UpdateMessageStatus(c, m.ID, db.StatusPending, "")
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (burstSubject) startIndicators(te *turnExecution) func() {
	te.resolveChannelPolicy()
	te.startTurnIndicators()
	return te.stopIndicators
}

func (burstSubject) traceItems(te *turnExecution) []queuedTurnItem {
	items := make([]queuedTurnItem, 0, len(te.burst))
	for _, m := range te.burst {
		items = append(items, queuedTurnItem{
			traceparent:  m.Traceparent,
			createdAt:    m.CreatedAt,
			requeuedAt:   attemptRequeueTime(m.RestartCount, m.UpdatedAt),
			restartCount: m.RestartCount,
			retryCount:   m.RetryCount,
		})
	}
	return items
}

// dispatchSubject runs attempts over one dispatch row.
type dispatchSubject struct {
	pool *WorkerPool
	ids  []string
}

func loadExtraDispatches(ctx context.Context, s db.Store, te *turnExecution, knownIDs, extraIDs []string) ([]string, error) {
	if len(extraIDs) == 0 {
		return nil, nil
	}
	te.steerMu.Lock()
	defer te.steerMu.Unlock()
	var active []string
	for _, extraID := range extraIDs {
		extraRow, gErr := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, extraID) })
		if errors.Is(gErr, db.ErrRecordNotFound) || (gErr == nil && (extraRow == nil || extraRow.Status != db.StatusProcessing)) {
			continue
		}
		if gErr != nil {
			return nil, gErr
		}
		active = append(active, extraID)
		te.steeredDispatches = append(te.steeredDispatches, *extraRow)
		if !slices.Contains(knownIDs, extraID) {
			te.drainedIDs = normalizeStepIDs(append(te.drainedIDs, extraID))
			if pText := strings.TrimSpace(extraRow.Prompt); pText != "" {
				te.drainedPrompts = append(te.drainedPrompts, pText)
			}
		}
	}
	return active, nil
}

func (d dispatchSubject) load(ctx context.Context, s db.Store, te *turnExecution) ([]string, TurnOutcome, error) {
	knownIDs := normalizeStepIDs(d.ids)
	if len(knownIDs) == 0 {
		return nil, OutcomeEmpty, nil
	}
	row, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, knownIDs[0]) })
	if errors.Is(err, db.ErrRecordNotFound) || (err == nil && row == nil) {
		return nil, OutcomeEmpty, nil
	}
	if err != nil {
		return nil, "", err
	}
	if row.Status != db.StatusProcessing {
		var failedErrs []string
		if row.Status == db.StatusFailed {
			failedErrs = []string{row.ErrorMessage}
		}
		return nil, recordedOutcome([]string{row.Status}, failedErrs), nil
	}
	isEphem := strings.TrimSpace(row.SessionID) == ""
	leaseKey := row.SessionID
	if isEphem {
		leaseKey = "dispatch-" + row.ID
	}
	te.activeSessionMu.Lock()
	te.dispatch = row
	te.threadID = leaseKey
	te.isEphemeral = isEphem
	te.triggerType = "dispatch:" + row.Source
	te.activeSessionMu.Unlock()
	ids := slices.Clone(knownIDs)
	if !isEphem {
		extraIDs, rErr := recoverDispatchProcessingIDs(ctx, s, te.turnID, row.SessionID, knownIDs)
		if rErr != nil {
			return nil, "", rErr
		}
		ids = normalizeStepIDs(append(ids, extraIDs...))
	}
	extraActive, lErr := loadExtraDispatches(ctx, s, te, knownIDs, ids[1:])
	if lErr != nil {
		return nil, "", lErr
	}
	return append([]string{row.ID}, extraActive...), "", nil
}

func (dispatchSubject) beginAttempt(ctx context.Context, s db.Store, id string, ordinal int) (db.AttemptStart, error) {
	return s.BeginDispatchAttempt(ctx, id, ordinal)
}

func (dispatchSubject) applyAttemptStart(te *turnExecution, id string, start db.AttemptStart) {
	if te.dispatch != nil && te.dispatch.ID == id {
		te.dispatch.RetryCount, te.dispatch.RestartCount = start.RetryCount, start.RestartCount
	}
	for i := range te.steeredDispatches {
		if te.steeredDispatches[i].ID == id {
			te.steeredDispatches[i].RetryCount, te.steeredDispatches[i].RestartCount = start.RetryCount, start.RestartCount
		}
	}
}

func (d dispatchSubject) recordFailure(ctx context.Context, _ db.Store, _ *turnExecution, ids []string, summary AttemptSummary) error {
	return d.pool.RecordDispatchOutcome(ctx, ids, summary)
}

func (dispatchSubject) ensureCompleted(ctx context.Context, s db.Store, te *turnExecution, ids []string) error {
	for _, id := range ids {
		row, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, id) })
		if errors.Is(err, db.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if row == nil || !isActiveStatus(row.Status) {
			continue
		}
		telemetry := te.turnTelemetry()
		if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
			return struct{}{}, s.UpdateDispatchCompleted(c, id, te.turnResponseText, te.currentSessionID, telemetry...)
		}); err != nil {
			return err
		}
	}
	te.steerMu.Lock()
	undelivered := removeIDs(slices.Clone(te.pendingDrainIDs), ids)
	te.pendingDrainIDs = nil
	te.steerMu.Unlock()
	for _, id := range undelivered {
		row, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, id) })
		if errors.Is(err, db.ErrRecordNotFound) || (err == nil && (row == nil || row.Status != db.StatusProcessing)) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
			return struct{}{}, s.UpdateDispatchStatus(c, id, db.StatusPending, "")
		}); err != nil {
			return err
		}
	}
	return nil
}

func (d dispatchSubject) startIndicators(te *turnExecution) func() {
	te.activeSessionMu.Lock()
	row := te.dispatch
	te.activeSessionMu.Unlock()
	if row == nil {
		return func() {}
	}
	chunkID, buildID := extractVanadisDispatchInfo(row)
	if chunkID == "" {
		return func() {}
	}
	widgetCtx := context.WithoutCancel(te.turnCtxValue())
	te.statusUpdater = NewChunkStepStatusUpdater(chunkID, func(step string) error {
		return d.pool.FlushChunkActiveStepToRestate(widgetCtx, buildID, chunkID, step)
	})
	return func() {
		te.statusUpdater.Stop()
		logInterruptWarning("clear chunk active step", d.pool.FlushChunkActiveStepToRestate(widgetCtx, buildID, chunkID, ""))
	}
}

func (dispatchSubject) traceItems(te *turnExecution) []queuedTurnItem {
	if te.dispatch == nil {
		return nil
	}
	d := te.dispatch
	return []queuedTurnItem{{
		traceparent:  d.Traceparent,
		createdAt:    d.CreatedAt,
		requeuedAt:   attemptRequeueTime(d.RestartCount, d.UpdatedAt),
		restartCount: d.RestartCount,
		retryCount:   d.RetryCount,
	}}
}
