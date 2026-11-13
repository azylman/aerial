package runner

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracerScope is the instrumentation scope for spans eri emits around agent turns.
const TracerScope = "github.com/azylman/aerial/services/eri"

// gen_ai semantic convention attribute keys used on turn, chat and tool spans.
const (
	AttrGenAIOperationName       = attribute.Key("gen_ai.operation.name")
	AttrGenAIAgentName           = attribute.Key("gen_ai.agent.name")
	AttrGenAIConversationID      = attribute.Key("gen_ai.conversation.id")
	AttrGenAIProviderName        = attribute.Key("gen_ai.provider.name")
	AttrGenAIRequestModel        = attribute.Key("gen_ai.request.model")
	AttrGenAIUsageInputTokens    = attribute.Key("gen_ai.usage.input_tokens")
	AttrGenAIUsageOutputTokens   = attribute.Key("gen_ai.usage.output_tokens")
	AttrGenAIUsageCacheRead      = attribute.Key("gen_ai.usage.cache_read.input_tokens")
	AttrGenAIUsageCacheCreation  = attribute.Key("gen_ai.usage.cache_creation.input_tokens")
	AttrGenAIToolName            = attribute.Key("gen_ai.tool.name")
	AttrGenAIToolCallID          = attribute.Key("gen_ai.tool.call.id")
	AttrMCPServer                = attribute.Key("aerial.mcp.server")
	AttrToolStatus               = attribute.Key("aerial.tool.status")
	OperationInvokeAgent         = "invoke_agent"
	OperationChat                = "chat"
	OperationExecuteTool         = "execute_tool"
	toolStatusAbortedOnTurnClose = "aborted"
)

// UsageAttributes renders token usage as gen_ai usage attributes.
func UsageAttributes(u AgyUsage) []attribute.KeyValue {
	return []attribute.KeyValue{
		AttrGenAIUsageInputTokens.Int(u.InputTokens),
		AttrGenAIUsageOutputTokens.Int(u.OutputTokens),
		AttrGenAIUsageCacheRead.Int(u.CacheReadTokens),
		AttrGenAIUsageCacheCreation.Int(u.CacheCreationTokens),
	}
}

// openToolSpan is an in-flight execute_tool span. Subagent calls also own a nested
// invoke_agent span; calls whose ParentID names this call start under it.
type openToolSpan struct {
	id    string
	name  string
	span  trace.Span
	agent trace.Span
}

// TracingTurnSink decorates a TurnSink with OpenTelemetry spans: one chat span per turn
// (carrying usage) and one execute_tool span per tool call, nested under a child
// invoke_agent span when the call runs inside a subagent. Every callback is forwarded to
// the inner sink unchanged. Span attributes carry only tool names, call IDs, MCP server
// names and redacted statuses, never tool arguments or response text.
type TracingTurnSink struct {
	inner  TurnSink
	tracer trace.Tracer
	turn   trace.Span // parent of the chat and top-level tool spans

	mu     sync.Mutex
	chat   trace.Span
	open   []*openToolSpan // start order, for FIFO correlation of ID-less calls
	closed bool
	usage  AgyUsage      // usage recorded on the chat span; set when the turn closes
	done   chan struct{} // closed once the turn has closed
}

var (
	_ TurnSink      = (*TracingTurnSink)(nil)
	_ StepAwareSink = (*TracingTurnSink)(nil)
)

// NewTracingTurnSink wraps inner so spans are children of the span carried by turnCtx and
// are created with that span's tracer provider (a noop when tracing is disabled).
func NewTracingTurnSink(turnCtx context.Context, inner TurnSink) *TracingTurnSink {
	if turnCtx == nil {
		turnCtx = context.Background()
	}
	turn := trace.SpanFromContext(turnCtx)
	return &TracingTurnSink{inner: inner, tracer: turn.TracerProvider().Tracer(TracerScope), turn: turn, done: make(chan struct{})}
}

// Usage waits until the turn has closed (on its result, its error, or daemon death) or ctx is
// done, and returns the token usage recorded on the chat span. ok is false if the turn had not
// closed in time.
func (s *TracingTurnSink) Usage(ctx context.Context) (usage AgyUsage, ok bool) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.usage, true
	case <-ctx.Done():
		return AgyUsage{}, false
	}
}

// startUnder starts a span named name as a child of parent.
func (s *TracingTurnSink) startUnder(parent trace.Span, name string, attrs ...attribute.KeyValue) trace.Span {
	_, span := s.tracer.Start(trace.ContextWithSpan(context.Background(), parent), name, trace.WithAttributes(attrs...)) //nolint:spancheck // ended by finishToolSpan/closeTurn
	return span                                                                                                          //nolint:spancheck // ended by finishToolSpan/closeTurn
}

func (s *TracingTurnSink) OnTurnStarted() {
	s.mu.Lock()
	if s.chat == nil && !s.closed {
		s.chat = s.startUnder(s.turn, OperationChat, AttrGenAIOperationName.String(OperationChat))
	}
	s.mu.Unlock()
	if s.inner != nil {
		s.inner.OnTurnStarted()
	}
}

func (s *TracingTurnSink) OnThinking() {
	if s.inner != nil {
		s.inner.OnThinking()
	}
}

func (s *TracingTurnSink) OnStepStarted(stepIndex int) {
	if sa, ok := s.inner.(StepAwareSink); ok {
		sa.OnStepStarted(stepIndex)
	}
}

func (s *TracingTurnSink) OnToolCall(call ToolCall) {
	s.startToolSpan(call)
	if s.inner != nil {
		s.inner.OnToolCall(call)
	}
}

func (s *TracingTurnSink) OnToolCompleted(call ToolCall, mcpServer string, duration time.Duration, status string) {
	s.endToolSpan(call, mcpServer, status)
	if s.inner != nil {
		s.inner.OnToolCompleted(call, mcpServer, duration, status)
	}
}

func (s *TracingTurnSink) OnSkillActivated(skillName, source string) {
	if s.inner != nil {
		s.inner.OnSkillActivated(skillName, source)
	}
}

func (s *TracingTurnSink) OnTextDelta(delta string) {
	if s.inner != nil {
		s.inner.OnTextDelta(delta)
	}
}

func (s *TracingTurnSink) OnResult(res *TurnResult) {
	var usage *AgyUsage
	if res != nil {
		usage = &res.Usage
	}
	s.closeTurn(codes.Unset, "", usage)
	if s.inner != nil {
		s.inner.OnResult(res)
	}
}

func (s *TracingTurnSink) OnError(err error) {
	msg := ""
	var usage *AgyUsage
	if err != nil {
		msg = tracing.Redact(err.Error())
	}
	if u, ok := UsageFromError(err); ok {
		usage = &u
	}
	s.closeTurn(codes.Error, msg, usage)
	if s.inner != nil {
		s.inner.OnError(err)
	}
}

func (s *TracingTurnSink) startToolSpan(call ToolCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	parent := s.turn
	if call.ParentID != "" {
		if p := s.findByIDLocked(call.ParentID); p != nil && p.agent != nil {
			parent = p.agent
		}
	}
	attrs := []attribute.KeyValue{AttrGenAIOperationName.String(OperationExecuteTool), AttrGenAIToolName.String(call.Name)}
	if call.ID != "" {
		attrs = append(attrs, AttrGenAIToolCallID.String(call.ID))
	}
	ts := &openToolSpan{id: call.ID, name: call.Name, span: s.startUnder(parent, OperationExecuteTool+" "+call.Name, attrs...)}
	if call.Subagent {
		ts.agent = s.startUnder(ts.span, OperationInvokeAgent, AttrGenAIOperationName.String(OperationInvokeAgent))
	}
	s.open = append(s.open, ts)
}

func (s *TracingTurnSink) endToolSpan(call ToolCall, mcpServer, status string) {
	s.mu.Lock()
	ts := s.takeLocked(call)
	s.mu.Unlock()
	if ts == nil {
		return
	}
	if call.Name != "" && call.Name != ts.name {
		// Completions carry the canonical (MCP-resolved) tool name; prefer it on the span.
		ts.span.SetName(OperationExecuteTool + " " + call.Name)
		ts.span.SetAttributes(AttrGenAIToolName.String(call.Name))
	}
	if mcpServer != "" {
		ts.span.SetAttributes(AttrMCPServer.String(mcpServer))
	}
	finishToolSpan(ts, status)
}

func finishToolSpan(ts *openToolSpan, status string) {
	if status != "" {
		ts.span.SetAttributes(AttrToolStatus.String(tracing.Redact(status)))
	}
	if status != "" && status != "ok" {
		ts.span.SetStatus(codes.Error, tracing.Redact(status))
	}
	if ts.agent != nil {
		if status != "" && status != "ok" {
			ts.agent.SetStatus(codes.Error, tracing.Redact(status))
		}
		ts.agent.End()
	}
	ts.span.End()
}

func (s *TracingTurnSink) findByIDLocked(id string) *openToolSpan {
	for _, ts := range s.open {
		if ts.id == id {
			return ts
		}
	}
	return nil
}

// takeLocked removes and returns the open span matching call: by call ID when the engine
// supplies one, otherwise the oldest ID-less span of the same name, falling back to the
// oldest ID-less span when the completion reports a different (canonical) name.
func (s *TracingTurnSink) takeLocked(call ToolCall) *openToolSpan {
	match := s.indexFIFOLocked(call.Name)
	if call.ID != "" {
		match = slices.IndexFunc(s.open, func(ts *openToolSpan) bool { return ts.id == call.ID })
	}
	if match < 0 {
		return nil
	}
	ts := s.open[match]
	s.open = slices.Delete(s.open, match, match+1)
	return ts
}

// indexFIFOLocked returns the oldest ID-less open span named name, else the oldest ID-less one.
func (s *TracingTurnSink) indexFIFOLocked(name string) int {
	fallback := -1
	for i, ts := range s.open {
		switch {
		case ts.id != "":
		case ts.name == name:
			return i
		case fallback < 0:
			fallback = i
		}
	}
	return fallback
}

// closeTurn ends every still-open tool span (innermost first) and the chat span. It runs
// once: on the turn result, the turn error, or daemon death (which surfaces as OnError). A nil
// usage leaves the chat span without usage attributes.
func (s *TracingTurnSink) closeTurn(code codes.Code, msg string, usage *AgyUsage) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if usage != nil {
		s.usage = *usage
	}
	open, chat := s.open, s.chat
	s.open, s.chat = nil, nil
	s.mu.Unlock()
	if s.done != nil {
		defer close(s.done)
	}

	for _, ts := range slices.Backward(open) {
		finishToolSpan(ts, toolStatusAbortedOnTurnClose)
	}
	if chat == nil {
		chat = s.startUnder(s.turn, OperationChat, AttrGenAIOperationName.String(OperationChat))
	}
	if usage != nil {
		chat.SetAttributes(UsageAttributes(*usage)...)
	}
	if code == codes.Error {
		chat.SetStatus(codes.Error, msg)
	}
	chat.End()
}
