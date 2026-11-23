// Package eriwire holds the wire types shared by eri and its dispatch callers.
package eriwire

import (
	"errors"
	"strings"
)

// Dispatch enqueue status values.
const (
	StatusAccepted      = "accepted"
	StatusSkippedActive = "skipped_active"
	StatusAlreadyExists = "already_exists"
)

// ErrDispatchActive reports that a dispatch was skipped because a session is already active.
var ErrDispatchActive = errors.New("dispatch active; reconciliation deferred")

// BrainDispatchRequest represents a headless dispatch payload.
type BrainDispatchRequest struct {
	ID             string `json:"id,omitempty"`
	Agent          string `json:"agent,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	VanadisBuildID string `json:"vanadis_build_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	Model          string `json:"model,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
	Traceparent    string `json:"traceparent,omitempty"`
}

// DispatchEnqueueResponse defines the enqueue result for a dispatch.
type DispatchEnqueueResponse struct {
	Status     string `json:"status"` // StatusAccepted, StatusSkippedActive or StatusAlreadyExists
	DispatchID string `json:"dispatch_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

// ResolveDispatchSessionKey determines the canonical Virtual Object key for a dispatch.
func ResolveDispatchSessionKey(req BrainDispatchRequest) string {
	if s := strings.TrimSpace(req.SessionID); s != "" {
		return s
	}
	if c := strings.TrimSpace(req.VanadisChunkID); c != "" {
		return "vanadis-" + c
	}
	return "dispatch-" + strings.TrimSpace(req.ID)
}
