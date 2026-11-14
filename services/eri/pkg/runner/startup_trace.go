package runner

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Span names and attributes for the turn-startup work eri does before a daemon's chat span
// begins: waiting for the per-target session lock and forking/handshaking a daemon.
const (
	spanSessionLockWait = "session.lock_wait"
	spanDaemonSpawn     = "daemon.spawn"
	eventDaemonInit     = "daemon.init"

	attrAgentEngine    = attribute.Key("aerial.agent.engine")
	attrDaemonAttempts = attribute.Key("aerial.daemon.attempts")
)

// startChildOf starts a span under ctx's active span with that span's tracer provider. It
// records nothing when ctx carries no span (background prewarms and rotations), so startup
// spans only ever attach to an existing turn trace and never open orphan roots.
func startChildOf(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return trace.SpanFromContext(ctx).TracerProvider().Tracer(TracerScope).Start(ctx, name, opts...) //nolint:spancheck // the caller owns and ends the span
}

// engineName is the aerial.agent.engine value for a daemon config.
func (c DaemonConfig) engineName() string {
	if c.IsClaude() {
		return "claude"
	}
	return "agy"
}
