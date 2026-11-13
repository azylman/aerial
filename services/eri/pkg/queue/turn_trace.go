package queue

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	spanQueueWait = "queue.wait"
	spanPreTurn   = "pre_turn"
	spanPostTurn  = "post_turn"
	spanDeliver   = "deliver discord"

	attrTurnStatus  = attribute.Key("aerial.turn.status")
	attrTriggerType = attribute.Key("aerial.turn.trigger")
	attrBurstSize   = attribute.Key("aerial.turn.burst_size")
)

// queuedTurnItem is the trace-relevant part of a queued row (message or dispatch).
type queuedTurnItem struct {
	traceparent string
	createdAt   time.Time
}

func burstTraceItems(burst []db.Message) []queuedTurnItem {
	items := make([]queuedTurnItem, 0, len(burst))
	for _, m := range burst {
		items = append(items, queuedTurnItem{traceparent: m.Traceparent, createdAt: m.CreatedAt})
	}
	return items
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
// queue.wait child spanning the first item's enqueue time to pickup.
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
		enqueued = spec.items[0].createdAt
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

// providerForModel maps a model ID to its gen_ai.provider.name.
func providerForModel(model string) string {
	m := strings.ToLower(model)
	switch {
	case strings.Contains(m, "claude"):
		return "anthropic"
	case strings.Contains(m, "gemini"):
		return "gcp.gemini"
	default:
		return ""
	}
}

// turnSpanEndAttributes renders the outcome attributes recorded when a turn ends.
func turnSpanEndAttributes(model string, usage runner.AgyUsage, status string) []attribute.KeyValue {
	attrs := runner.UsageAttributes(usage)
	if model != "" {
		attrs = append(attrs, runner.AttrGenAIRequestModel.String(model))
	}
	if provider := providerForModel(model); provider != "" {
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

// beginTurnTrace opens the turn's root span and returns the context carrying it.
func (p *WorkerPool) beginTurnTrace(items []queuedTurnItem, conversationID, triggerType string, pickup time.Time) (context.Context, trace.Span) {
	return startTurnSpan(p.ctx, p.tracer(), turnSpanSpec{
		items: items, conversationID: conversationID, agentName: p.cfg.AgentName, triggerType: triggerType, pickup: pickup,
	})
}

// turnSinkCloseGrace bounds how long a turn that ended without usage waits for the daemon to
// report its failure. A cancelled or interrupted turn returns before the killed daemon reports,
// and that report carries the tokens the turn already consumed.
const turnSinkCloseGrace = 2 * time.Second

// lateSinkUsage returns the usage the latest attempt's trace sink recorded once its turn closes,
// or zero if there is no sink or ctx ends first.
func (te *turnExecution) lateSinkUsage(ctx context.Context) runner.TokenUsage {
	if te.traceSink == nil {
		return runner.TokenUsage{}
	}
	usage, _ := te.traceSink.Usage(ctx)
	return usage
}

// endTurnSpan records model, usage and outcome on the root span and ends it.
func (te *turnExecution) endTurnSpan() {
	if te.turnSpan == nil {
		return
	}
	usage := te.turnTokenUsage
	if usage == (runner.TokenUsage{}) {
		ctx, cancel := context.WithTimeout(context.Background(), turnSinkCloseGrace)
		usage = te.lateSinkUsage(ctx)
		cancel()
	}
	te.turnSpan.SetAttributes(turnSpanEndAttributes(te.turnModel, usage, te.turnStatus)...)
	if te.turnStatus != "" && te.turnStatus != "success" {
		te.turnSpan.SetStatus(codes.Error, te.turnStatus)
	}
	te.turnSpan.End()
}

// startChildSpan starts a child of the turn span on base, keeping base's cancellation.
func (te *turnExecution) startChildSpan(base context.Context, name string) (context.Context, trace.Span) {
	if te.turnSpan != nil {
		base = trace.ContextWithSpan(base, te.turnSpan)
	}
	return te.pool.tracer().Start(base, name) //nolint:spancheck // the caller owns and ends the span
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

// markTurnActive registers the leased daemon's turn span in the pool's ActiveTurnRegistry so
// the loopback MCP proxy parents the daemon's MCP calls under this turn, and returns the func
// that clears it. Call it after the lease-release defer is registered so the turn is cleared
// before the lease (and the daemon) can be handed to the next turn.
func (te *turnExecution) markTurnActive() func() {
	te.registerActiveTurn()
	return te.clearActiveTurn
}

// registerActiveTurn registers the current lease's daemon under the turn span. It runs again
// when the turn swaps leases mid-flight (session corruption retry), since the replacement may be
// a different daemon or conversation.
func (te *turnExecution) registerActiveTurn() {
	te.clearActiveTurn()
	owner, ok := te.lease.(runner.ActiveTurnOwner)
	if !ok || te.turnSpan == nil {
		return
	}
	turn := owner.ActiveTurn()
	te.pool.ActiveTurns().Set(turn, te.turnSpan.SpanContext())
	te.activeTurnDaemon = turn.DaemonID
}

// clearActiveTurn removes the turn's registration. It must run before the registered lease is
// released, or a turn that takes over the daemon could have its fresh entry cleared.
func (te *turnExecution) clearActiveTurn() {
	if te.activeTurnDaemon != "" {
		te.pool.ActiveTurns().Clear(te.activeTurnDaemon)
		te.activeTurnDaemon = ""
	}
}

// logf logs a turn-scoped line through slog with the turn span in context, so eri's JSON log
// line carries trace_id and span_id and Grafana's trace-to-logs link finds it.
func (te *turnExecution) logf(format string, args ...any) {
	ctx := context.Background()
	if te != nil && te.turnSpan != nil {
		ctx = trace.ContextWithSpan(ctx, te.turnSpan)
	}
	logger := slog.Default()
	if te != nil && te.pool != nil && te.pool.cfg.Logger != nil {
		logger = te.pool.cfg.Logger
	}
	logger.InfoContext(ctx, fmt.Sprintf(format, args...))
}
