package main

import vanadismcp "github.com/azylman/aerial/services/vanadis/internal/mcp"

// Transitional aliases onto the canonical L2 action-retry surface in
// internal/mcp. This file declares zero-statement type and variable aliases
// only; VanadisServer delegation lives alongside SetupMux in main.go.
type (
	// RetryActionChunkRequest carries the identity and optional correction for a durable action-chunk retry.
	RetryActionChunkRequest = vanadismcp.RetryActionChunkRequest
	// RetryActionChunkResult reports the accepted durable retry dispatch.
	RetryActionChunkResult = vanadismcp.RetryActionChunkResult
)

// ErrRetryIngressUnavailable reports that durable Restate ingress is required for action retry.
var ErrRetryIngressUnavailable = vanadismcp.ErrRetryIngressUnavailable
