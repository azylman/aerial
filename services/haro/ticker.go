package main

// Root compatibility shims for the Layer-3 ticker package.
//
// The scheduling loop (Ticker, NewTicker, EvaluateDueSchedules, Start,
// HandleDispatchOutcome and helpers) now lives in internal/ticker. This
// file keeps zero-logic aliases so main.go compiles untouched until the
// final purge (haro-07b). The dispatch client aliases below keep the
// remaining root callers compiling; the Ticker aliases delegate to
// internal/ticker. The traceFire wrapper moved to internal/server with
// the server in haro-07a.

import (
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/ticker"
)

// PromptDispatchRequest and the other dispatch client types live in
// internal/dispatch. The aliases below keep Server and main.go compiling
// untouched until the purge.
type (
	PromptDispatchRequest  = dispatch.PromptDispatchRequest
	PromptDispatchResponse = dispatch.PromptDispatchResponse
	DispatchRequest        = dispatch.DispatchRequest
	DispatchResponse       = dispatch.DispatchResponse
	PromptDispatcher       = dispatch.PromptDispatcher
	HTTPPromptDispatcher   = dispatch.HTTPPromptDispatcher
)

// Ticker periodically evaluates due schedules and dispatches them.
type Ticker = ticker.Ticker
