package queue

import (
	"context"
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

// endTurnSpan records model, usage and outcome on the root span and ends it.
func (te *turnExecution) endTurnSpan() {
	if te.turnSpan == nil {
		return
	}
	te.turnSpan.SetAttributes(turnSpanEndAttributes(te.turnModel, te.turnTokenUsage, te.turnStatus)...)
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
