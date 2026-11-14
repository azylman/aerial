package runner

import (
	"context"
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
	turn  ActiveTurn
	sc    trace.SpanContext
	tools ToolSpanSource // the executing attempt's tool spans; nil until attached
}

// ActiveTurnRegistry maps daemons to the span context of the turn they are executing and to
// the tool spans of the attempt in flight. It is safe for concurrent use.
type ActiveTurnRegistry struct {
	mu      sync.RWMutex
	entries map[string]activeTurnEntry
}

// NewActiveTurnRegistry returns an empty registry.
func NewActiveTurnRegistry() *ActiveTurnRegistry {
	return &ActiveTurnRegistry{entries: make(map[string]activeTurnEntry)}
}

// Set records that turn's daemon is executing the turn whose root span is sc. It replaces any
// previous entry for the daemon, dropping its tool span source.
func (r *ActiveTurnRegistry) Set(turn ActiveTurn, sc trace.SpanContext) {
	if r == nil || turn.DaemonID == "" || !sc.IsValid() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[turn.DaemonID] = activeTurnEntry{turn: turn, sc: sc}
}

// SetToolSpans attaches the tool spans of daemonID's executing attempt, replacing the previous
// attempt's. It is a no-op unless Set registered daemonID.
func (r *ActiveTurnRegistry) SetToolSpans(daemonID string, src ToolSpanSource) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[daemonID]; ok {
		e.tools = src
		r.entries[daemonID] = e
	}
}

// ToolSpan claims the execute_tool span behind an MCP request from daemonID (see
// ToolSpanSource). With no source attached it returns false without waiting.
func (r *ActiveTurnRegistry) ToolSpan(ctx context.Context, daemonID, server, tool, callID string) (trace.SpanContext, bool) {
	if r == nil {
		return trace.SpanContext{}, false
	}
	r.mu.RLock()
	src := r.entries[daemonID].tools
	r.mu.RUnlock()
	if src == nil {
		return trace.SpanContext{}, false
	}
	return src.ClaimToolSpan(ctx, server, tool, callID)
}

// Clear removes daemonID's active turn and its tool span source.
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

// SoleDaemonInTarget returns the daemon executing target's active turn when exactly one daemon
// in target is executing one; with zero or several the owner is ambiguous and ok is false.
func (r *ActiveTurnRegistry) SoleDaemonInTarget(target string) (daemonID string, ok bool) {
	if r == nil || target == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for id, e := range r.entries {
		if e.turn.Target == target {
			daemonID = id
			n++
		}
	}
	if n != 1 {
		return "", false
	}
	return daemonID, true
}
