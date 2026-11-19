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
	"context"
	"database/sql"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
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

func NewHTTPPromptDispatcher(brainURL string) *HTTPPromptDispatcher {
	return dispatch.NewHTTPPromptDispatcher(brainURL)
}

// Ticker periodically evaluates due schedules and dispatches them.
type Ticker = ticker.Ticker

// NewTicker returns a Ticker that evaluates schedules on interval.
func NewTicker(db *sql.DB, dispatcher PromptDispatcher, interval time.Duration) *Ticker {
	return ticker.NewTicker(db, dispatcher, interval)
}

// HandleDispatchOutcome updates schedule_runs and monotonically advances heartbeat last_completed_at.
func HandleDispatchOutcome(ctx context.Context, database *sql.DB, msg pubsub.Message) error {
	return ticker.HandleDispatchOutcome(ctx, database, msg)
}
