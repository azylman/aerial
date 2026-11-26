package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/runner"
)

const (
	spanQueueWait = "queue.wait"
	spanPreTurn   = "pre_turn"
	spanPostTurn  = "post_turn"
	spanDeliver   = "deliver discord"
	spanTurnSetup = "turn.setup"
	spanLease     = "session.lease"
	spanPrompt    = "prompt.build"

	// spanTurnSummaryRefresh wraps the thread summary refresh run after a successful thread turn delivered.
	spanTurnSummaryRefresh = "turn.summary_refresh"

	// Child spans of prompt.build: every step that can block while the prompt is assembled.
	spanPromptChannelSnapshot  = "prompt.channel_snapshot"
	spanPromptHistoryFetch     = "prompt.history_fetch"
	spanPromptThreadSummary    = "prompt.thread_summary"
	spanPromptSummaryLoad      = "prompt.summary_cache_load"
	spanPromptSummarySave      = "prompt.summary_cache_save"
	spanPromptSummaryLLM       = "prompt.summary_llm"
	spanPromptPrevSessionID    = "prompt.previous_session_id"
	spanPromptPreviousSession  = "prompt.previous_session"
	spanPromptScopeInstr       = "prompt.scope_instructions"
	spanPromptAmbientContext   = "prompt.ambient_context"
	spanPromptRecentDispatches = "prompt.recent_dispatches"

	attrTurnStatus       = attribute.Key("aerial.turn.status")
	attrTriggerType      = attribute.Key("aerial.turn.trigger")
	attrBurstSize        = attribute.Key("aerial.turn.burst_size")
	attrRestartCount     = attribute.Key("aerial.turn.restart_count")
	attrRetryCount       = attribute.Key("aerial.turn.retry_count")
	attrResumed          = attribute.Key("aerial.turn.resumed")
	attrFirstEnqueuedMs  = attribute.Key("aerial.queue.first_enqueued_unix_ms")
	attrNewConversation  = attribute.Key("aerial.session.new_conversation")
	attrSessionEphemeral = attribute.Key("aerial.session.ephemeral")
	attrSessionOverflow  = attribute.Key("aerial.session.overflow")

	attrPromptColdStart          = attribute.Key("aerial.prompt.cold_start")
	attrPromptChars              = attribute.Key("aerial.prompt.chars")
	attrPromptSnapshotOK         = attribute.Key("aerial.prompt.snapshot_ok")
	attrPromptHistoryLimit       = attribute.Key("aerial.prompt.history_limit")
	attrPromptHistoryCount       = attribute.Key("aerial.prompt.history_count")
	attrPromptSummaryOutcome     = attribute.Key("aerial.prompt.summary_outcome")
	attrPromptSummaryHadPrior    = attribute.Key("aerial.prompt.summary_had_prior")
	attrPromptSummaryDeltaMsgs   = attribute.Key("aerial.prompt.summary_delta_msgs")
	attrPromptSummaryShared      = attribute.Key("aerial.prompt.summary_shared")
	attrPromptSummaryPromptChars = attribute.Key("aerial.prompt.summary_prompt_chars")
	attrPromptPrevSessionChars   = attribute.Key("aerial.prompt.previous_session_chars")
	attrPromptDispatchCount      = attribute.Key("aerial.prompt.dispatch_count")
)

// queuedTurnItem is the trace-relevant part of a queued row (message or dispatch).
type queuedTurnItem struct {
	traceparent  string
	createdAt    time.Time
	requeuedAt   time.Time // when a resumed row re-entered the queue; zero on a first attempt
	restartCount int
	retryCount   int
}

// remoteSpanContext parses a persisted W3C traceparent; the result is invalid when tp is empty or malformed.
func remoteSpanContext(tp string) trace.SpanContext {
	if tp == "" {
		return trace.SpanContext{}
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": tp})
	return trace.SpanContextFromContext(ctx)
}

// turnSpanSpec is everything startTurnSpan needs to open a turn's root span.
type turnSpanSpec struct {
	items          []queuedTurnItem
	conversationID string
	agentName      string
	triggerType    string
	pickup         time.Time
}

// startTurnSpan opens the turn's root invoke_agent span as a child of the first item's
// persisted traceparent, links every other item's trace, and records a synthetic
// queue.wait child spanning the first item's enqueue time to pickup. A resumed item's wait
// starts when it was requeued, not when it was first enqueued, so time spent running before
// a restart is not reported as queue wait.
func startTurnSpan(base context.Context, tracer trace.Tracer, spec turnSpanSpec) (context.Context, trace.Span) {
	parent := base
	var links []trace.Link
	for i, item := range spec.items {
		if sc := remoteSpanContext(item.traceparent); sc.IsValid() && i > 0 {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	if len(spec.items) > 0 {
		if sc := remoteSpanContext(spec.items[0].traceparent); sc.IsValid() {
			parent = trace.ContextWithRemoteSpanContext(base, sc)
		}
	}
	attrs := []attribute.KeyValue{
		runner.AttrGenAIOperationName.String(runner.OperationInvokeAgent),
		runner.AttrGenAIConversationID.String(spec.conversationID),
		attrTriggerType.String(spec.triggerType),
		attrBurstSize.Int(len(spec.items)),
	}
	if spec.agentName != "" {
		attrs = append(attrs, runner.AttrGenAIAgentName.String(spec.agentName))
	}
	start := spec.pickup
	var enqueued time.Time
	if len(spec.items) > 0 {
		first := spec.items[0]
		enqueued = first.createdAt
		if first.requeuedAt.After(enqueued) {
			enqueued = first.requeuedAt
		}
		attrs = append(attrs, attrRestartCount.Int(first.restartCount), attrRetryCount.Int(first.retryCount))
		if first.restartCount > 0 {
			attrs = append(attrs, attrResumed.Bool(true))
			if !first.createdAt.IsZero() {
				attrs = append(attrs, attrFirstEnqueuedMs.Int64(first.createdAt.UnixMilli()))
			}
		}
	}
	hasWait := !enqueued.IsZero() && enqueued.Before(spec.pickup)
	if hasWait {
		start = enqueued
	}
	ctx, span := tracer.Start(parent, runner.OperationInvokeAgent,
		trace.WithTimestamp(start), trace.WithLinks(links...), trace.WithAttributes(attrs...))
	if hasWait {
		_, wait := tracer.Start(ctx, spanQueueWait, trace.WithTimestamp(enqueued))
		wait.End(trace.WithTimestamp(spec.pickup))
	}
	return ctx, span
}

// turnSpanEndAttributes renders the outcome attributes recorded when a turn ends.
func turnSpanEndAttributes(model string, usage runner.AgyUsage, status string) []attribute.KeyValue {
	attrs := runner.UsageAttributes(usage)
	if model != "" {
		attrs = append(attrs, runner.AttrGenAIRequestModel.String(model))
	}
	if provider := runner.ProviderForModel(model); provider != "" {
		attrs = append(attrs, runner.AttrGenAIProviderName.String(provider))
	}
	if status != "" {
		attrs = append(attrs, attrTurnStatus.String(status))
	}
	return attrs
}

func (p *WorkerPool) tracer() trace.Tracer {
	if p == nil || p.cfg.TracerProvider == nil {
		return otel.GetTracerProvider().Tracer(runner.TracerScope)
	}
	return p.cfg.TracerProvider.Tracer(runner.TracerScope)
}

// finishTurnSpan records model, usage and outcome on a turn's root span, marks it failed for
// any status other than success, and ends it.
func finishTurnSpan(span trace.Span, model string, usage runner.AgyUsage, status string) {
	span.SetAttributes(turnSpanEndAttributes(model, usage, status)...)
	if status != "" && status != outcomeSuccess {
		span.SetStatus(codes.Error, status)
	}
	span.End()
}

// startChildSpan starts a child of the turn span on base, keeping base's cancellation.
func (te *turnExecution) startChildSpan(base context.Context, name string) (context.Context, trace.Span) {
	if te.turnSpan != nil {
		base = trace.ContextWithSpan(base, te.turnSpan)
	} else if !trace.SpanContextFromContext(base).IsValid() {
		if sc := trace.SpanContextFromContext(te.turnCtxValue()); sc.IsValid() {
			base = trace.ContextWithSpanContext(base, sc)
		}
	}
	return te.pool.tracer().Start(base, name) //nolint:spancheck // the caller owns and ends the span
}

// endSetupSpan ends turn.setup; it is idempotent so early exits and the first attempt can both call it.
func (te *turnExecution) endSetupSpan() {
	if te.setupSpan != nil {
		te.setupSpan.End()
		te.setupSpan = nil
	}
}

// startSetupChildSpan starts a child of turn.setup on base, or of the turn span once setup has ended.
func (te *turnExecution) startSetupChildSpan(base context.Context, name string) (context.Context, trace.Span) {
	if te.setupSpan == nil {
		return te.startChildSpan(base, name)
	}
	return te.pool.tracer().Start(trace.ContextWithSpan(base, te.setupSpan), name) //nolint:spancheck // the caller owns and ends the span
}

// promptStepCtx joins the cancellation and deadline of cancelCtx with the span carried by
// spanCtx, so a prompt.build step parents its span correctly without changing what cancels it.
func promptStepCtx(cancelCtx, spanCtx context.Context) context.Context {
	return trace.ContextWithSpan(cancelCtx, trace.SpanFromContext(spanCtx))
}

// startPromptStep opens a prompt.build step span as a child of the span carried by ctx.
func (te *turnExecution) startPromptStep(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return te.pool.tracer().Start(ctx, name, trace.WithAttributes(attrs...)) //nolint:spancheck // the caller owns and ends the span
}

// recordSpanError marks span failed with its error message; the collector masks secret values.
func recordSpanError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}
}

// overflowLease is implemented by leases that may run on an on-demand overflow worker.
type overflowLease interface{ IsOverflow() bool }

// leaseSpanAttributes describes the session a turn leased.
func leaseSpanAttributes(lease runner.SessionLease, ephemeral bool) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attrNewConversation.Bool(lease.TurnCount() <= 1),
		attrSessionEphemeral.Bool(ephemeral),
	}
	if o, ok := lease.(overflowLease); ok {
		attrs = append(attrs, attrSessionOverflow.Bool(o.IsOverflow()))
	}
	return attrs
}

// activeTurnRegistryOrNew gives a pool without an injected registry a private one.
func activeTurnRegistryOrNew(r *runner.ActiveTurnRegistry) *runner.ActiveTurnRegistry {
	if r == nil {
		return runner.NewActiveTurnRegistry()
	}
	return r
}

// ActiveTurns returns the registry mapping daemons to the span of the turn they are executing.
func (p *WorkerPool) ActiveTurns() *runner.ActiveTurnRegistry {
	if p == nil {
		return nil
	}
	return p.cfg.ActiveTurns
}

// registerActiveTurn registers the current lease's daemon under the turn span. It runs again
// when the turn swaps leases mid-flight (session corruption retry), since the replacement may be
// a different daemon or conversation.
func (te *turnExecution) registerActiveTurn() {
	te.clearActiveTurn()
	te.activeSessionMu.Lock()
	lease := te.lease
	te.activeSessionMu.Unlock()
	owner, ok := lease.(runner.ActiveTurnOwner)
	if !ok || te.turnSpan == nil {
		return
	}
	turn := owner.ActiveTurn()
	te.pool.ActiveTurns().Set(turn, te.turnSpan.SpanContext())
	te.activeSessionMu.Lock()
	te.activeTurnDaemon = turn.DaemonID
	te.activeSessionMu.Unlock()
}

// clearActiveTurn removes the turn's registration. It must run before the registered lease is
// released, or a turn that takes over the daemon could have its fresh entry cleared.
func (te *turnExecution) clearActiveTurn() {
	te.activeSessionMu.Lock()
	daemon := te.activeTurnDaemon
	te.activeTurnDaemon = ""
	te.activeSessionMu.Unlock()
	if daemon != "" {
		te.pool.ActiveTurns().Clear(daemon)
	}
}

// logf logs a turn-scoped line through slog with the turn span in context, so eri's JSON log
// line carries trace_id and span_id and Grafana's trace-to-logs link finds it. Callers holding a
// narrower step context use logCtxf instead.
func (te *turnExecution) logf(format string, args ...any) {
	ctx := te.turnCtxValue()
	if te != nil && te.turnSpan != nil {
		ctx = trace.ContextWithSpan(ctx, te.turnSpan)
	}
	te.logCtxf(ctx, format, args...)
}

// logCtxf logs a turn-scoped line with ctx, attributing it to the turn span when ctx carries no
// span of its own.
func (te *turnExecution) logCtxf(ctx context.Context, format string, args ...any) {
	if te != nil && te.turnSpan != nil && !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = trace.ContextWithSpan(ctx, te.turnSpan)
	}
	logger := slog.Default()
	if te != nil && te.pool != nil && te.pool.cfg.Logger != nil {
		logger = te.pool.cfg.Logger
	}
	logger.InfoContext(ctx, fmt.Sprintf(format, args...))
}
