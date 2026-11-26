package build

import (
	"context"
	"log/slog"

	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/restateingress"
)

const spanPrefix = "vanadis-restate."

// requestTraceparent extracts the traceparent from a Restate run or handler context.
func requestTraceparent(rc restate.RunContext) string {
	if rc == nil {
		return ""
	}
	return restateingress.TraceparentFromRequest(rc.Request())
}

// tracedRunContext overrides the context.Context of a restate.RunContext with one carrying the
// active span, so HTTP calls made from a journaled step inherit the trace. Request still comes
// from the original run context; Log wraps the original run logger so its records carry the
// step span's trace_id and span_id.
type tracedRunContext struct {
	context.Context //nolint:containedctx // restate.RunContext embeds context.Context; the override carries the step span.

	base restate.RunContext
}

func (t tracedRunContext) Request() *restate.Request { return t.base.Request() }

// Log returns the run context's logger bound to the step span, so plain Info/Warn/Error calls
// pass the span context down to the default tracing.SlogHandler instead of context.Background.
func (t tracedRunContext) Log() *slog.Logger {
	return slog.New(spanContextHandler{Handler: t.base.Log().Handler(), spanCtx: t.Context})
}

// spanContextHandler substitutes spanCtx for any record context that carries no valid span,
// letting the wrapped handler chain (ending in tracing.SlogHandler) attach trace IDs.
type spanContextHandler struct {
	slog.Handler

	spanCtx context.Context //nolint:containedctx // the logger is scoped to one journaled step.
}

func (h spanContextHandler) Handle(ctx context.Context, r slog.Record) error { //nolint:contextcheck // deliberately swaps in the step span context for records logged without one.
	if !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = h.spanCtx
	}
	return h.Handler.Handle(ctx, r)
}

func (h spanContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return spanContextHandler{Handler: h.Handler.WithAttrs(attrs), spanCtx: h.spanCtx}
}

func (h spanContextHandler) WithGroup(name string) slog.Handler {
	return spanContextHandler{Handler: h.Handler.WithGroup(name), spanCtx: h.spanCtx}
}

// runInSpan runs fn inside a span named name, parented on the invocation's traceparent, and
// hands fn a RunContext carrying that span. The span is closed before returning, and the
// return values pass through untouched, so what restate.Run journals does not change.
func runInSpan[T any](rc restate.RunContext, name string, fn func(restate.RunContext) (T, error)) (T, error) {
	tp := requestTraceparent(rc)
	return restateingress.ExecuteInSpan(rc, name, tp, func(ctx context.Context) (T, error) {
		return fn(tracedRunContext{Context: ctx, base: rc})
	})
}
