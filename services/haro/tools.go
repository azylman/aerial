package main

// Root compatibility shims for the Layer-3 tools package.
//
// Every tool-handler symbol formerly defined here now lives in
// internal/tools (Layer 3, imports model, config, store). This file keeps
// zero-logic aliases only so all not-yet-moved callers (server.go, ticker.go,
// main.go) compile unchanged until the final purge (haro-07b). Shims contain
// no logic and carry no coverage-bearing statements. ToolHandler is a pure
// alias (not a delegating wrapper) so its exported DB/Cfg/Timezone fields
// are the single source of truth.

import (
	"context"
	"database/sql"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/haro/internal/tools"
)

// ToolHandler executes haro MCP tool calls against the schedule store.
type ToolHandler = tools.ToolHandler

// Tool argument and output types.
type (
	ScheduleRecurringArgs    = tools.ScheduleRecurringArgs
	ScheduleRecurringOutput  = tools.ScheduleRecurringOutput
	ScheduleOnceArgs         = tools.ScheduleOnceArgs
	ScheduleOnceOutput       = tools.ScheduleOnceOutput
	ListSchedulesArgs        = tools.ListSchedulesArgs
	ListSchedulesOutput      = tools.ListSchedulesOutput
	GetScheduleArgs          = tools.GetScheduleArgs
	GetScheduleOutput        = tools.GetScheduleOutput
	CancelScheduleArgs       = tools.CancelScheduleArgs
	CancelScheduleOutput     = tools.CancelScheduleOutput
	UpdateCronScheduleArgs   = tools.UpdateCronScheduleArgs
	UpdateCronScheduleOutput = tools.UpdateCronScheduleOutput
	ScheduleHeartbeatArgs    = tools.ScheduleHeartbeatArgs
	UpdateHeartbeatArgs      = tools.UpdateHeartbeatArgs
	WatchEventArgs           = tools.WatchEventArgs
	ListEventWatchesArgs     = tools.ListEventWatchesArgs
	CancelEventWatchArgs     = tools.CancelEventWatchArgs
)

// NewToolHandler builds a ToolHandler over database.
func NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler {
	return tools.NewToolHandler(cfg, database)
}

// HandleScheduleHeartbeat handles the schedule_heartbeat MCP tool call.
func HandleScheduleHeartbeat(ctx context.Context, db *sql.DB, defaultTZ string, args map[string]any) (*mcp.CallToolResult, error) {
	return tools.HandleScheduleHeartbeat(ctx, db, defaultTZ, args)
}

// HandleUpdateHeartbeat handles the update_heartbeat MCP tool call.
func HandleUpdateHeartbeat(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	return tools.HandleUpdateHeartbeat(ctx, db, args)
}

// HandleWatchEvent registers a deterministic one-shot event watch trigger.
func HandleWatchEvent(ctx context.Context, db *sql.DB, args map[string]any, extraSources ...string) (*mcp.CallToolResult, error) {
	return tools.HandleWatchEvent(ctx, db, args, extraSources...)
}

// HandleListEventWatches returns active deterministic event watches.
func HandleListEventWatches(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	return tools.HandleListEventWatches(ctx, db, args)
}

// HandleCancelEventWatch cancels a deterministic event watch trigger.
func HandleCancelEventWatch(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	return tools.HandleCancelEventWatch(ctx, db, args)
}
