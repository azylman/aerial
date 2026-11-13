package runner

import (
	"sync"

	"go.opentelemetry.io/otel/trace"
)

// ActiveTurnRegistry maps a daemon (identified by the pool lease key that owns it) to the
// span context of the turn it is currently executing, so out-of-band traffic from that
// daemon (e.g. MCP requests) can be parented under the turn. Safe for concurrent use.
// The read side lands with its first consumer, the loopback MCP proxy.
type ActiveTurnRegistry struct {
	mu    sync.RWMutex
	turns map[string]trace.SpanContext
}

// NewActiveTurnRegistry returns an empty registry.
func NewActiveTurnRegistry() *ActiveTurnRegistry {
	return &ActiveTurnRegistry{turns: make(map[string]trace.SpanContext)}
}

// Set records sc as the active turn for daemonID. Invalid span contexts are ignored.
func (r *ActiveTurnRegistry) Set(daemonID string, sc trace.SpanContext) {
	if r == nil || daemonID == "" || !sc.IsValid() {
		return
	}
	r.mu.Lock()
	r.turns[daemonID] = sc
	r.mu.Unlock()
}

// Clear forgets the active turn for daemonID.
func (r *ActiveTurnRegistry) Clear(daemonID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.turns, daemonID)
	r.mu.Unlock()
}
