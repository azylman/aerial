package restateingress

import (
	"context"
	"strings"

	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

const traceparentHeader = "traceparent"

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func findInAttempt(attempt map[string][]string) string {
	if vals, ok := attempt[traceparentHeader]; ok {
		if v := firstNonEmpty(vals); v != "" {
			return v
		}
	}
	for k, vals := range attempt {
		if strings.EqualFold(k, traceparentHeader) {
			if v := firstNonEmpty(vals); v != "" {
				return v
			}
		}
	}
	return ""
}

func findInHeaders(req *restate.Request) string {
	if req.Headers == nil {
		return ""
	}
	if v := strings.TrimSpace(req.Headers.Get(traceparentHeader)); v != "" {
		return v
	}
	for k, v := range req.Headers.Iter() {
		if strings.EqualFold(k, traceparentHeader) {
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

// TraceparentFromRequest nil-safely inspects req.AttemptHeaders and req.Headers
// (exact key "traceparent" first, then case-insensitive strings.EqualFold fallback),
// returning the trimmed non-empty value or "".
func TraceparentFromRequest(req *restate.Request) string {
	if req == nil {
		return ""
	}
	if req.AttemptHeaders != nil {
		if tp := findInAttempt(req.AttemptHeaders); tp != "" {
			return tp
		}
	}
	if req.Headers != nil {
		if tp := findInHeaders(req); tp != "" {
			return tp
		}
	}
	return ""
}

// RequestTraceparent nil-safely returns TraceparentFromRequest(rc.Request()).
func RequestTraceparent(rc restate.Context) string {
	if rc == nil {
		return ""
	}
	return TraceparentFromRequest(rc.Request())
}

// ContextWithTrace finds the first non-empty trimmed traceparent from RequestTraceparent(rc)
// followed by fallbackTraceparents, and if non-empty calls tracing.ContextWithTraceparent(ctx, tp).
func ContextWithTrace(ctx context.Context, rc restate.Context, fallbackTraceparents ...string) context.Context {
	tp := RequestTraceparent(rc)
	if tp == "" {
		for _, f := range fallbackTraceparents {
			if trimmed := strings.TrimSpace(f); trimmed != "" {
				tp = trimmed
				break
			}
		}
	}
	if tp != "" {
		return tracing.ContextWithTraceparent(ctx, tp)
	}
	return ctx
}

// RunInSpan runs fn inside a step span parented on the invocation's traceparent,
// journaled via restate.Run.
func RunInSpan[T any](rc restate.Context, spanName string, fn func(context.Context, restate.RunContext) (T, error), opts ...restate.RunOption) (T, error) {
	ctx := ContextWithTrace(context.Background(), rc)
	out, err := restate.Run(rc, func(runCtx restate.RunContext) (T, error) {
		stepCtx, span := tracing.Tracer().Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal))
		defer span.End()
		res, stepErr := fn(stepCtx, runCtx)
		if stepErr != nil {
			span.RecordError(stepErr)
			span.SetStatus(codes.Error, "step failed")
		}
		return res, stepErr
	}, opts...)
	if err != nil {
		return out, err
	}
	return out, nil
}
