package runner

import (
	"sync"

	"go.opentelemetry.io/otel/trace"
)

// ActiveTurn identifies the daemon process executing a turn.
type ActiveTurn struct {
	// DaemonID is the daemon's ERI_DAEMON_ID; Claude Code echoes it in the X-Eri-Daemon MCP header.
	DaemonID string
	// Target is the runtime home (discord, voice, ephemeral) whose MCP config the daemon reads.
	Target string
	// ConversationID is the agy conversation / Claude session the daemon is serving, when known.
	ConversationID string
}

// ActiveTurnOwner is implemented by session leases backed by a single daemon process. The
// worker registers the owner's ActiveTurn while the lease executes so the loopback MCP proxy
// can parent MCP calls under the turn span.
type ActiveTurnOwner interface {
	ActiveTurn() ActiveTurn
}

type activeTurnEntry struct {
	turn ActiveTurn
	sc   trace.SpanContext
}

// ActiveTurnRegistry maps daemons to the span context of the turn they are executing. It is
// safe for concurrent use.
type ActiveTurnRegistry struct {
	mu      sync.RWMutex
	entries map[string]activeTurnEntry
}

// NewActiveTurnRegistry returns an empty registry.
func NewActiveTurnRegistry() *ActiveTurnRegistry {
	return &ActiveTurnRegistry{entries: make(map[string]activeTurnEntry)}
}

// Set records that turn's daemon is executing the turn whose root span is sc.
func (r *ActiveTurnRegistry) Set(turn ActiveTurn, sc trace.SpanContext) {
	if r == nil || turn.DaemonID == "" || !sc.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[turn.DaemonID] = activeTurnEntry{turn: turn, sc: sc}
}

// Clear removes daemonID's active turn.
func (r *ActiveTurnRegistry) Clear(daemonID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, daemonID)
}

// Get returns the span context of daemonID's active turn.
func (r *ActiveTurnRegistry) Get(daemonID string) (trace.SpanContext, bool) {
	if r == nil || daemonID == "" {
		return trace.SpanContext{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[daemonID]
	return e.sc, ok
}

// Target returns the runtime target of daemonID's active turn.
func (r *ActiveTurnRegistry) Target(daemonID string) (string, bool) {
	if r == nil || daemonID == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[daemonID]
	return e.turn.Target, ok
}

// DaemonByConversation returns the daemon (and its target) whose active turn serves conversationID.
func (r *ActiveTurnRegistry) DaemonByConversation(conversationID string) (daemonID, target string, ok bool) {
	if r == nil || conversationID == "" {
		return "", "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, e := range r.entries {
		if e.turn.ConversationID == conversationID {
			return id, e.turn.Target, true
		}
	}
	return "", "", false
}

// SoleTurnInTarget returns the span context of target's active turn when exactly one daemon
// in target is executing one; with zero or several the owner is ambiguous and ok is false.
func (r *ActiveTurnRegistry) SoleTurnInTarget(target string) (trace.SpanContext, bool) {
	if r == nil || target == "" {
		return trace.SpanContext{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var found trace.SpanContext
	n := 0
	for _, e := range r.entries {
		if e.turn.Target == target {
			found = e.sc
			n++
		}
	}
	if n != 1 {
		return trace.SpanContext{}, false
	}
	return found, true
}
