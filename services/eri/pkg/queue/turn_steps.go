package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// Step functions the TurnRunner journals one by one: record-started, prep, record-outcome,
// post-turn-hook / refresh-thread-summary and publish-dispatch-outcome. Each one is safe to
// re-run after a crash: it re-reads its rows and only acts on rows whose outcome is not
// recorded yet. They reuse the shared turnExecution helpers to write the rows.

// turnStepStoreTimeout bounds every store call made inside a step, so a hung connection
// surfaces as a retryable error instead of stalling the step.
const turnStepStoreTimeout = 5 * time.Second

// Prep decisions.
const (
	TurnDecisionRun        = "run"
	TurnDecisionFinished   = "finished"
	TurnDecisionReschedule = "reschedule"
)

// defaultPreTurnDelaySeconds is the pre_turn retry delay when the hook gives none.
const defaultPreTurnDelaySeconds = 5

// exhaustedPreTurnTag marks rows failed after too many pre_turn deferrals.
const exhaustedPreTurnTag = "[EXHAUSTED_PRE_TURN_RETRIES]"

// preTurnDeferredReason is the row error of a burst the pre_turn hook asked to retry later.
const preTurnDeferredReason = "pre_turn_deferred"

// CancelledByOperatorReason is the row error of a turn cancelled through Restate.
const CancelledByOperatorReason = "cancelled by operator"

// StartedTurn is the journaled result of record-started.
type StartedTurn struct {
	IDs       []string `json:"ids"`
	BaseRetry int      `json:"base_retry"`
}

// TurnPrep is the journaled result of prep.
type TurnPrep struct {
	Decision        string            `json:"decision"`
	Prompt          string            `json:"prompt,omitempty"`
	InjectedContext string            `json:"injected_context,omitempty"`
	SessionID       string            `json:"session_id,omitempty"`
	Effort          string            `json:"effort,omitempty"`
	Model           string            `json:"model,omitempty"`
	Reason          string            `json:"reason,omitempty"`
	HookMetadata    map[string]string `json:"hook_metadata,omitempty"`
	DelayMs         int64             `json:"delay_ms,omitempty"`
}

// AttemptSummary is how a turn's attempts ended, as record-outcome needs it.
type AttemptSummary struct {
	Outcome TurnOutcome `json:"outcome"`
	// Reason is the row error for non-success outcomes.
	Reason string `json:"reason,omitempty"`
	Stderr string `json:"stderr,omitempty"`
	// NoticePrefix overrides the failure notice description prefix.
	NoticePrefix string            `json:"notice_prefix,omitempty"`
	DurationMs   int64             `json:"duration_ms,omitempty"`
	Usage        runner.TokenUsage `json:"usage"`
}

func (s AttemptSummary) telemetry() []db.TurnTelemetry {
	if s.DurationMs == 0 && s.Usage == (runner.TokenUsage{}) {
		return nil
	}
	return []db.TurnTelemetry{{
		DurationMs:      s.DurationMs,
		InputTokens:     s.Usage.InputTokens,
		OutputTokens:    s.Usage.OutputTokens,
		ThinkingTokens:  s.Usage.ThinkingTokens,
		CacheReadTokens: s.Usage.CacheReadTokens,
		TotalTokens:     s.Usage.TotalTokens,
	}}
}

// failureReason is the row error recorded for a non-success outcome.
func (s AttemptSummary) failureReason() string {
	if r := strings.TrimSpace(s.Reason); r != "" {
		return r
	}
	switch s.Outcome {
	case OutcomeCancelled:
		return CancelledByOperatorReason
	case OutcomeTimeout:
		return "timeout"
	default:
		return outcomeFailed
	}
}

// noticePrefix is the failure notice description for an outcome; "" means no notice.
func (s AttemptSummary) noticePrefix() string {
	switch s.Outcome {
	case OutcomeFailed:
		if s.NoticePrefix != "" {
			return s.NoticePrefix
		}
		return "execution failed"
	case OutcomeTimeout:
		if s.NoticePrefix != "" {
			return s.NoticePrefix
		}
		return "execution timed out while processing the request"
	default:
		return ""
	}
}

// recordsFailure reports whether an outcome fails the turn's still-active rows.
func recordsFailure(o TurnOutcome) bool {
	return o == OutcomeFailed || o == OutcomeTimeout || o == OutcomeCancelled
}

// postTurnStatus maps a turn outcome to the post_turn hook / turn span status.
func postTurnStatus(o TurnOutcome) string {
	switch o {
	case OutcomeCompleted:
		return outcomeSuccess
	case OutcomeTimeout, OutcomeCancelled:
		return string(o)
	default:
		return outcomeFailed
	}
}

// dispatchOutcomeStatus maps a turn outcome to the published dispatch status; "" skips publishing.
func dispatchOutcomeStatus(o TurnOutcome) string {
	switch o {
	case OutcomeCompleted, OutcomeFailed, OutcomeTimeout:
		return string(o)
	default:
		return ""
	}
}

// stringifyHookMetadata flattens pre_turn metadata to strings so it journals as typed JSON.
func stringifyHookMetadata(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch val := v.(type) {
		case string:
			out[k] = val
		default:
			b, err := json.Marshal(val)
			if err != nil {
				out[k] = fmt.Sprint(val)
				continue
			}
			out[k] = string(b)
		}
	}
	return out
}

func hookMetadataAny(in map[string]string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func isActiveStatus(status string) bool {
	return status == db.StatusPending || status == db.StatusProcessing
}

func normalizeStepIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// storeCall runs one store call under the per-call deadline and tags deterministic errors terminal.
func storeCall[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := context.WithTimeout(ctx, turnStepStoreTimeout)
	defer cancel()
	v, err := fn(callCtx)
	return v, classifyStoreError(err)
}

var errStepNoStore = restate.ToTerminalError(errors.New("queue: turn step has no store"), restate.WithErrorCode(500))

func (p *WorkerPool) stepStore() (db.Store, error) {
	if p == nil || p.cfg.Store == nil {
		return nil, errStepNoStore
	}
	return p.cfg.Store, nil
}

func (p *WorkerPool) maxAttempts() int {
	if p != nil && p.cfg.MaxAttempts > 0 {
		return p.cfg.MaxAttempts
	}
	return 3
}

// messagesByIDOrdered loads ids in input order; missing rows are omitted.
func messagesByIDOrdered(ctx context.Context, s db.Store, ids []string) ([]db.Message, error) {
	rows, err := storeCall(ctx, func(c context.Context) ([]db.Message, error) { return s.GetMessagesByID(c, ids) })
	if err != nil {
		return nil, err
	}
	byID := make(map[string]db.Message, len(rows))
	for _, m := range rows {
		byID[m.ID] = m
	}
	out := make([]db.Message, 0, len(rows))
	for _, id := range ids {
		if m, ok := byID[id]; ok {
			out = append(out, m)
		}
	}
	return out, nil
}

// RecordBurstStarted claims the burst's rows (record-started). A PENDING row is claimed; a row
// already PROCESSING was claimed by an earlier try of this same step and is adopted; terminal or
// missing rows are skipped. restart_count is never touched here.
func (p *WorkerPool) RecordBurstStarted(ctx context.Context, threadID string, ids []string) (StartedTurn, error) {
	s, err := p.stepStore()
	if err != nil {
		return StartedTurn{}, err
	}
	ids = normalizeStepIDs(ids)
	for _, id := range ids {
		if _, claimErr := storeCall(ctx, func(c context.Context) (bool, error) { return s.ClaimPendingMessage(c, id) }); claimErr != nil {
			return StartedTurn{}, claimErr
		}
	}
	rows, err := messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return StartedTurn{}, err
	}
	var started StartedTurn
	for _, m := range rows {
		if m.Status != db.StatusProcessing {
			continue
		}
		if m.ThreadID != threadID {
			log.Printf("[TurnRunner] Message %s belongs to thread %s, started under %s", m.ID, m.ThreadID, threadID)
		}
		started.IDs = append(started.IDs, m.ID)
		started.BaseRetry = max(started.BaseRetry, m.RetryCount)
	}
	return started, nil
}

// RecordDispatchStarted claims the dispatch row (record-started) under the same claim rule.
func (p *WorkerPool) RecordDispatchStarted(ctx context.Context, sessionKey, dispatchID string) (StartedTurn, error) {
	s, err := p.stepStore()
	if err != nil {
		return StartedTurn{}, err
	}
	dispatchID = strings.TrimSpace(dispatchID)
	if dispatchID == "" {
		return StartedTurn{}, nil
	}
	if _, claimErr := storeCall(ctx, func(c context.Context) (bool, error) { return s.ClaimPendingDispatch(c, dispatchID) }); claimErr != nil {
		return StartedTurn{}, claimErr
	}
	d, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, dispatchID) })
	if errors.Is(err, db.ErrRecordNotFound) {
		return StartedTurn{}, nil
	}
	if err != nil {
		return StartedTurn{}, err
	}
	if d == nil || d.Status != db.StatusProcessing {
		return StartedTurn{}, nil
	}
	if d.SessionID != "" && d.SessionID != sessionKey {
		log.Printf("[TurnRunner] Dispatch %s belongs to session %s, started under %s", d.ID, d.SessionID, sessionKey)
	}
	return StartedTurn{IDs: []string{d.ID}, BaseRetry: d.RetryCount}, nil
}

// newStepTurnExecution builds the turnExecution through which a step reuses the shared turn helpers.
func (p *WorkerPool) newStepTurnExecution(ctx context.Context, threadID string, burst []db.Message) *turnExecution {
	if len(burst) > 0 && strings.TrimSpace(burst[0].Traceparent) != "" {
		ctx = tracing.ContextWithTraceparent(ctx, strings.TrimSpace(burst[0].Traceparent))
	}
	triggerType := triggerSourceDiscord
	if len(burst) > 0 {
		triggerType = resolveBurstTriggerType(burst[0])
	}
	return &turnExecution{
		pool:        p,
		burst:       burst,
		threadID:    threadID,
		triggerType: triggerType,
		execStart:   time.Now().UTC(),
		wakeIdx:     -1,
		stopTyping:  func() {},
		turnCtx:     ctx,
	}
}

// splitPrepRows sorts loaded rows into still-PROCESSING and requeued-PENDING ones.
func splitPrepRows(rows []db.Message) (processing []db.Message, pending int) {
	for _, m := range rows {
		switch m.Status {
		case db.StatusProcessing:
			processing = append(processing, m)
		case db.StatusPending:
			pending++
		}
	}
	return processing, pending
}

func (p *WorkerPool) preTurnDelayMs(delaySeconds int) int64 {
	if delaySeconds <= 0 {
		delaySeconds = defaultPreTurnDelaySeconds
	}
	if p != nil && p.cfg.RetryDelayOverride > 0 {
		return p.cfg.RetryDelayOverride.Milliseconds()
	}
	return (time.Duration(delaySeconds) * time.Second).Milliseconds()
}

// PrepareBurstTurn runs prep for a burst: channel policy, ambient classification, the pre_turn
// hook decision and the prompt, in that order. Terminal decisions write their rows through the
// shared turn helpers and return Decision finished; a pre_turn retry requeues the rows once and
// returns Decision reschedule. A replay only acts on rows still PROCESSING; rows this step already
// requeued are reported as reschedule again without another retry_count bump.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) PrepareBurstTurn(ctx context.Context, threadID, turnID string, ids []string) (TurnPrep, error) {
	s, err := p.stepStore()
	if err != nil {
		return TurnPrep{}, err
	}
	ids = normalizeStepIDs(ids)
	rows, err := messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return TurnPrep{}, err
	}
	burst, requeued := splitPrepRows(rows)
	if len(burst) == 0 {
		if requeued > 0 {
			return TurnPrep{Decision: TurnDecisionReschedule, Reason: preTurnDeferredReason, DelayMs: p.preTurnDelayMs(0)}, nil
		}
		return TurnPrep{Decision: TurnDecisionFinished, Reason: "no active rows"}, nil
	}

	te := p.newStepTurnExecution(ctx, threadID, burst)
	defer te.stopIndicators()

	if !te.resolveTurnPolicy() {
		return finishPrep(ctx, s, ids, "ignored")
	}
	if te.evaluateAmbientWake() {
		return finishPrep(ctx, s, ids, "ambient")
	}
	decision := te.evaluatePreTurnHook()
	switch decision.action {
	case preTurnAbort:
		te.abortBurst(decision.abortReason)
		return finishPrep(ctx, s, ids, decision.abortReason)
	case preTurnReschedule:
		return p.deferPrep(ctx, s, te, ids, decision.delaySeconds)
	case preTurnProceed:
	}
	te.applyPreTurnContext(decision)

	// The lease tells whether the session starts cold, which shapes the prompt; the run step
	// takes its own lease, so this one is released as soon as the prompt is built.
	if !te.acquirePoolAndLease() {
		return finishPrep(ctx, s, ids, "lease unavailable")
	}
	defer func() {
		if te.lease != nil {
			logInterruptWarning("release prep lease", te.lease.Release())
		}
	}()
	te.buildTurnPrompt(te.turnCtxValue())
	effort, model := te.turnEffortAndModel()
	log.Printf("[TurnRunner] Prepared turn %s for thread %s (%d message(s))", turnID, threadID, len(burst))
	return TurnPrep{
		Decision:        TurnDecisionRun,
		Prompt:          te.turnPrompt,
		InjectedContext: te.injectedHookContext,
		SessionID:       te.currentSessionID,
		Effort:          effort,
		Model:           model,
		HookMetadata:    stringifyHookMetadata(te.hookMetadata),
	}, nil
}

// finishPrep returns Decision finished once every row is terminal; otherwise the helper's write
// was lost (or left for recovery) and the step must retry.
func finishPrep(ctx context.Context, s db.Store, ids []string, reason string) (TurnPrep, error) {
	if err := unfinishedMessagesError(ctx, s, ids); err != nil {
		return TurnPrep{}, err
	}
	return TurnPrep{Decision: TurnDecisionFinished, Reason: reason}, nil
}

// deferPrep applies a pre_turn retry: rows out of attempts are failed exhausted;
// the rest go back to PENDING in one RequeueProcessingMessages call.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) deferPrep(ctx context.Context, s db.Store, te *turnExecution, ids []string, delaySeconds int) (TurnPrep, error) {
	te.stopIndicators()
	maxAttempts := p.maxAttempts()
	var requeue []string
	for _, m := range te.burst {
		if m.RetryCount+1 >= maxAttempts {
			te.logf("[TurnRunner] Message %s in thread %s exceeded max retry attempts (%d). Marking FAILED.", m.ID, te.threadID, maxAttempts)
			te.finalizeBurstMessages([]db.Message{m}, db.StatusFailed, exhaustedPreTurnTag, outcomeFailed, exhaustedPreTurnTag, "", true, false)
			continue
		}
		requeue = append(requeue, m.ID)
	}
	if len(requeue) == 0 {
		return finishPrep(ctx, s, ids, exhaustedPreTurnTag)
	}
	if _, err := storeCall(ctx, func(c context.Context) ([]string, error) {
		return s.RequeueProcessingMessages(c, requeue, preTurnDeferredReason)
	}); err != nil {
		return TurnPrep{}, err
	}
	return TurnPrep{Decision: TurnDecisionReschedule, Reason: preTurnDeferredReason, DelayMs: p.preTurnDelayMs(delaySeconds)}, nil
}

// PrepareDispatchTurn runs prep for a dispatch: it builds the dispatch prompt (dispatches run no hooks).
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) PrepareDispatchTurn(ctx context.Context, sessionKey, turnID, dispatchID string) (TurnPrep, error) {
	s, err := p.stepStore()
	if err != nil {
		return TurnPrep{}, err
	}
	d, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, dispatchID) })
	if errors.Is(err, db.ErrRecordNotFound) || (err == nil && (d == nil || d.Status != db.StatusProcessing)) {
		return TurnPrep{Decision: TurnDecisionFinished, Reason: "no active rows"}, nil
	}
	if err != nil {
		return TurnPrep{}, err
	}
	isEphem := strings.TrimSpace(d.SessionID) == ""
	leaseKey := d.SessionID
	if isEphem {
		leaseKey = "dispatch-" + d.ID
	}
	if strings.TrimSpace(d.Traceparent) != "" {
		ctx = tracing.ContextWithTraceparent(ctx, strings.TrimSpace(d.Traceparent))
	}
	te := p.newStepTurnExecution(ctx, leaseKey, nil)
	te.dispatch = d
	te.isEphemeral = isEphem
	te.triggerType = "dispatch:" + d.Source
	if !isEphem {
		// Only session dispatches use cold-start context, and only the lease knows if the session is cold.
		if !te.acquirePoolAndLease() {
			return finishDispatchPrep(ctx, s, d.ID, "lease unavailable")
		}
		defer func() {
			if te.lease != nil {
				logInterruptWarning("release prep lease", te.lease.Release())
			}
		}()
	}
	te.turnPrompt = te.buildDispatchPrompt(ctx)
	effort, model := te.turnEffortAndModel()
	log.Printf("[TurnRunner] Prepared turn %s for dispatch %s (session %s)", turnID, d.ID, sessionKey)
	return TurnPrep{Decision: TurnDecisionRun, Prompt: te.turnPrompt, SessionID: d.SessionID, Effort: effort, Model: model}, nil
}

func finishDispatchPrep(ctx context.Context, s db.Store, dispatchID, reason string) (TurnPrep, error) {
	if err := unfinishedDispatchError(ctx, s, dispatchID); err != nil {
		return TurnPrep{}, err
	}
	return TurnPrep{Decision: TurnDecisionFinished, Reason: reason}, nil
}

// unfinishedMessagesError returns a retryable error naming rows still PENDING or PROCESSING.
func unfinishedMessagesError(ctx context.Context, s db.Store, ids []string) error {
	rows, err := messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return err
	}
	var open []string
	for _, m := range rows {
		if isActiveStatus(m.Status) {
			open = append(open, m.ID)
		}
	}
	if len(open) > 0 {
		return fmt.Errorf("queue: message rows not terminal: %s", strings.Join(open, ","))
	}
	return nil
}

func unfinishedDispatchError(ctx context.Context, s db.Store, dispatchID string) error {
	d, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, dispatchID) })
	if errors.Is(err, db.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if d != nil && isActiveStatus(d.Status) {
		return fmt.Errorf("queue: dispatch row %s not terminal", d.ID)
	}
	return nil
}

// RecordBurstOutcome records a burst's non-success outcome (record-outcome): it delivers the
// failure notice through the shared turn helpers, fails the still-active rows, then checks every row is
// terminal (M2). A replay finding every row terminal sends no second notice.
func (p *WorkerPool) RecordBurstOutcome(ctx context.Context, ids []string, summary AttemptSummary) error {
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	ids = normalizeStepIDs(ids)
	if summary.Outcome == OutcomeReschedule || len(ids) == 0 {
		return nil
	}
	if recordsFailure(summary.Outcome) {
		if _, err := p.failActiveBurstRows(ctx, s, ids, summary); err != nil {
			return err
		}
	}
	return unfinishedMessagesError(ctx, s, ids)
}

//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) failActiveBurstRows(ctx context.Context, s db.Store, ids []string, summary AttemptSummary) (int, error) {
	rows, err := messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return 0, err
	}
	var active []db.Message
	for _, m := range rows {
		if isActiveStatus(m.Status) {
			active = append(active, m)
		}
	}
	if len(active) == 0 {
		return 0, nil
	}
	reason := summary.failureReason()
	te := p.newStepTurnExecution(ctx, active[0].ThreadID, active)
	if prefix := summary.noticePrefix(); prefix != "" {
		cfg := te.resolveAttemptConfig(ctx, 0)
		te.deliverFailureNotice(cfg.agyBin, cfg.apiKey, reason, summary.Stderr, prefix, "turn outcome notice")
	}
	activeIDs := make([]string, len(active))
	for i, m := range active {
		activeIDs[i] = m.ID
	}
	changed, err := storeCall(ctx, func(c context.Context) ([]string, error) { return s.FailMessagesIfActive(c, activeIDs, reason) })
	if err != nil {
		return 0, err
	}
	telemetry := summary.telemetry()
	for _, m := range active {
		if !slices.Contains(changed, m.ID) {
			continue
		}
		if telemetry != nil {
			if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
				return struct{}{}, s.UpdateMessageStatus(c, m.ID, db.StatusFailed, reason, telemetry...)
			}); err != nil {
				return 0, err
			}
		}
		if p.cfg.OnMessageCompleted != nil {
			p.cfg.OnMessageCompleted(m, db.StatusFailed)
		}
	}
	return len(changed), nil
}

// RecordDispatchOutcome records a dispatch's non-success outcome (dispatches deliver no notice),
// then checks the row is terminal (M2).
func (p *WorkerPool) RecordDispatchOutcome(ctx context.Context, ids []string, summary AttemptSummary) error {
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	ids = normalizeStepIDs(ids)
	if summary.Outcome == OutcomeReschedule || len(ids) == 0 {
		return nil
	}
	reason := summary.failureReason()
	telemetry := summary.telemetry()
	for _, id := range ids {
		if recordsFailure(summary.Outcome) {
			changed, err := storeCall(ctx, func(c context.Context) ([]string, error) { return s.FailDispatchesIfActive(c, []string{id}, reason) })
			if err != nil {
				return err
			}
			if telemetry != nil && slices.Contains(changed, id) {
				if _, err := storeCall(ctx, func(c context.Context) (struct{}, error) {
					return struct{}{}, s.UpdateDispatchStatus(c, id, db.StatusFailed, reason, telemetry...)
				}); err != nil {
					return err
				}
			}
		}
		if err := unfinishedDispatchError(ctx, s, id); err != nil {
			return err
		}
	}
	return nil
}

// RunBurstPostTurnHook runs the post_turn hook (post-turn-hook; only when the turn attempted the
// agent), rebuilt from the recorded rows. hookMetadata is the journaled TurnPrep.HookMetadata.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) RunBurstPostTurnHook(ctx context.Context, threadID string, ids []string, attempted bool, outcome TurnOutcome, hookMetadata map[string]string) error {
	te, ok, err := p.postTurnExecution(ctx, threadID, ids, attempted, outcome, hookMetadata)
	if err != nil || !ok {
		return err
	}
	te.runPostTurnHook()
	return nil
}

// RefreshBurstThreadSummary runs the turn-end thread summary refresh (refresh-thread-summary;
// only after a successful turn), rebuilt from the recorded rows.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) RefreshBurstThreadSummary(ctx context.Context, threadID string, ids []string, outcome TurnOutcome) error {
	te, ok, err := p.postTurnExecution(ctx, threadID, ids, true, outcome, nil)
	if err != nil || !ok {
		return err
	}
	te.refreshThreadSummaryAtTurnEnd(te.turnCtxValue())
	return nil
}

// postTurnExecution rebuilds the post-turn state of a burst from its recorded rows; ok is false when no row exists.
//
//nolint:contextcheck,nolintlint // the reused turn helpers own their contexts; store calls take ctx.
func (p *WorkerPool) postTurnExecution(ctx context.Context, threadID string, ids []string, attempted bool, outcome TurnOutcome, hookMetadata map[string]string) (*turnExecution, bool, error) {
	s, err := p.stepStore()
	if err != nil {
		return nil, false, err
	}
	rows, err := messagesByIDOrdered(ctx, s, normalizeStepIDs(ids))
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	te := p.newStepTurnExecution(ctx, threadID, rows)
	te.turnAttempted = attempted
	te.turnStatus = postTurnStatus(outcome)
	te.hookMetadata = hookMetadataAny(hookMetadata)
	te.applyRecordedTurnResult(rows)
	te.resolveChannelPolicy()
	if _, poolKey, poolErr := te.resolveExecutionPool(); poolErr == nil {
		te.isEphemeral = poolKey.Ephemeral
	}
	snap, _ := resolveChannelSnapshot(p.getDiscordSession(), threadID)
	te.summarizeThread = summarizesThread(snap, strings.EqualFold(te.policy.Mode, "channel"))
	return te, true, nil
}

// applyRecordedTurnResult fills the post_turn fields from the rows the turn recorded.
func (te *turnExecution) applyRecordedTurnResult(rows []db.Message) {
	for _, m := range rows {
		if te.turnResponseText == "" && m.Status == db.StatusCompleted {
			te.turnResponseText = m.ResponseText
		}
		if te.turnError == "" && m.Status == db.StatusFailed {
			te.turnError = m.ErrorMessage
		}
		if m.DurationMs > te.turnDurationMs {
			te.turnDurationMs = m.DurationMs
			te.turnTokenUsage = runner.TokenUsage{
				InputTokens:     m.InputTokens,
				OutputTokens:    m.OutputTokens,
				ThinkingTokens:  m.ThinkingTokens,
				CacheReadTokens: m.CacheReadTokens,
				TotalTokens:     m.TotalTokens,
			}
		}
	}
}

// PublishDispatchOutcome publishes the dispatch outcome event from the recorded row. The NATS
// message ID is dispatch:<id>:<status>, so a replay is deduplicated; cancelled turns publish nothing.
func (p *WorkerPool) PublishDispatchOutcome(ctx context.Context, dispatchID string, outcome TurnOutcome) error {
	status := dispatchOutcomeStatus(outcome)
	if status == "" {
		return nil
	}
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	d, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, dispatchID) })
	if errors.Is(err, db.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}
	if p.EventBus() == nil {
		if d.Source == "vanadis" {
			log.Printf("[TurnRunner] Warning: cannot publish dispatch outcome for dispatch %s (status %s): event bus is nil", d.ID, status)
		}
		return nil
	}
	completedAt := d.UpdatedAt.UTC()
	startedAt := completedAt.Add(-time.Duration(d.DurationMs) * time.Millisecond)
	errMsg := ""
	if status != string(OutcomeCompleted) {
		errMsg = d.ErrorMessage
	}
	pubCtx, cancel := context.WithTimeout(ctx, turnStepStoreTimeout)
	defer cancel()
	if strings.TrimSpace(d.Traceparent) != "" {
		pubCtx = tracing.ContextWithTraceparent(pubCtx, strings.TrimSpace(d.Traceparent))
	}
	return p.publishDispatchOutcome(pubCtx, *d, status, errMsg, startedAt, completedAt)
}

// StopBurstTurn is the stop-turn step of a cancelled burst: it stops the turn on this pod (killing
// its agent process) and records its still-active rows FAILED "cancelled by operator", without a
// failure notice. A replay finds the rows terminal and writes nothing.
func (p *WorkerPool) StopBurstTurn(ctx context.Context, turnID string, ids []string) error {
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	p.turns.Stop(turnID, CancelledByOperatorReason)
	_, err = p.failActiveBurstRows(ctx, s, normalizeStepIDs(ids), AttemptSummary{Outcome: OutcomeCancelled})
	return err
}

// StopDispatchTurn is the stop-turn step of a cancelled dispatch; cancelled dispatches publish nothing.
func (p *WorkerPool) StopDispatchTurn(ctx context.Context, turnID string, ids []string) error {
	if _, err := p.stepStore(); err != nil {
		return err
	}
	p.turns.Stop(turnID, CancelledByOperatorReason)
	return p.RecordDispatchOutcome(ctx, ids, AttemptSummary{Outcome: OutcomeCancelled})
}

func anyAttempted(rows []db.Message) bool {
	for _, m := range rows {
		if m.RetryCount > 0 {
			return true
		}
	}
	return false
}

// FailBurstTurn is the fail-turn step of a burst that hit a terminal error: it stops the turn on
// this pod, delivers the failure notice and records its still-active rows FAILED with reason. A
// replay finds the rows terminal and sends no second notice.
func (p *WorkerPool) FailBurstTurn(ctx context.Context, turnID string, ids []string, reason string, hookMetadata ...map[string]string) error {
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	p.turns.Stop(turnID, reason)
	ids = normalizeStepIDs(ids)
	failed, err := p.failActiveBurstRows(ctx, s, ids, AttemptSummary{Outcome: OutcomeFailed, Reason: reason})
	if err != nil {
		return err
	}
	if failed == 0 {
		return nil
	}
	var rows []db.Message
	rows, err = messagesByIDOrdered(ctx, s, ids)
	if err != nil {
		return err
	}
	var meta map[string]string
	for _, m := range hookMetadata {
		if m != nil {
			meta = m
			break
		}
	}
	if len(rows) > 0 && (anyAttempted(rows) || len(meta) > 0) {
		return p.RunBurstPostTurnHook(ctx, rows[0].ThreadID, ids, true, OutcomeFailed, meta)
	}
	return nil
}

// FailDispatchTurn is the fail-turn step of a dispatch that hit a terminal error: it stops the
// turn, records the row FAILED with reason and publishes the recorded outcome (failed, or
// completed when the reply was already recorded).
func (p *WorkerPool) FailDispatchTurn(ctx context.Context, turnID string, ids []string, reason string) error {
	s, err := p.stepStore()
	if err != nil {
		return err
	}
	p.turns.Stop(turnID, reason)
	ids = normalizeStepIDs(ids)
	if err := p.RecordDispatchOutcome(ctx, ids, AttemptSummary{Outcome: OutcomeFailed, Reason: reason}); err != nil {
		return err
	}
	for _, id := range ids {
		d, err := storeCall(ctx, func(c context.Context) (*db.Dispatch, error) { return s.GetDispatch(c, id) })
		if errors.Is(err, db.ErrRecordNotFound) || (err == nil && d == nil) {
			continue
		}
		if err != nil {
			return err
		}
		outcome := OutcomeFailed
		if d.Status == db.StatusCompleted {
			outcome = OutcomeCompleted
		}
		if err := p.PublishDispatchOutcome(ctx, id, outcome); err != nil {
			return err
		}
	}
	return nil
}
