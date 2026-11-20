package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

// Heartbeat and event-watch MCP tool handlers. ScheduleHeartbeatArgs and
// friends expose exported ToMap so the not-yet-moved server.go registerTools
// (a different package) can convert typed args to the untyped maps these
// handlers accept. Effort validation here keeps the historical
// model.NormalizeEffort contract (empty defaults to low, invalid errors);
// the recurring/one-shot paths in tools.go use store.NormalizeScheduleEffort
// (medium default) instead - never substitute one for the other.

// ScheduleHeartbeatArgs defines arguments for the schedule_heartbeat MCP tool.
type ScheduleHeartbeatArgs struct {
	ChannelID      string `json:"channel_id,omitempty"      jsonschema:"Discord Channel ID for heartbeat alerts."`
	TargetID       string `json:"target_id,omitempty"       jsonschema:"Alias for channel_id."`
	TitlePrefix    string `json:"title_prefix,omitempty"    jsonschema:"Title prefix for spawned heartbeat alert threads (default: 'Heartbeat Alert')."`
	CronExpression string `json:"cron_expression,omitempty" jsonschema:"Standard 5-field cron expression or macro evaluated periodically by Haro."`
	CronExpr       string `json:"cron_expr,omitempty"       jsonschema:"Alias for cron_expression."`
	Prompt         string `json:"prompt"                    jsonschema:"Silent evaluation instructions or edge-triggered condition check."`
	Timezone       string `json:"timezone,omitempty"        jsonschema:"Timezone for cron evaluation (e.g. 'America/Los_Angeles' or 'UTC'). Defaults to configured server timezone."`
	Effort         string `json:"effort,omitempty"          jsonschema:"Effort tier: 'low', 'medium', or 'high'. Defaults to 'medium'."`
	Agent          string `json:"agent,omitempty"           jsonschema:"Target agent namespace (defaults to caller agent)."`
}

// ToMap converts typed heartbeat args to the untyped map the handler accepts.
func (a ScheduleHeartbeatArgs) ToMap() map[string]any {
	m := make(map[string]any)
	if a.ChannelID != "" {
		m["channel_id"] = a.ChannelID
	}
	if a.TargetID != "" {
		m["target_id"] = a.TargetID
	}
	if a.TitlePrefix != "" {
		m["title_prefix"] = a.TitlePrefix
	}
	if a.CronExpression != "" {
		m["cron_expression"] = a.CronExpression
	}
	if a.CronExpr != "" {
		m["cron_expr"] = a.CronExpr
	}
	if a.Prompt != "" {
		m["prompt"] = a.Prompt
	}
	if a.Timezone != "" {
		m["timezone"] = a.Timezone
	}
	if a.Effort != "" {
		m[model.FieldEffort] = a.Effort
	}
	if a.Agent != "" {
		m["agent"] = a.Agent
	}
	return m
}

// UpdateHeartbeatArgs defines arguments for the update_heartbeat MCP tool.
type UpdateHeartbeatArgs struct {
	ScheduleID     string `json:"schedule_id,omitempty"     jsonschema:"The ID of the heartbeat schedule to update."`
	ID             string `json:"id,omitempty"              jsonschema:"Alias for schedule_id."`
	ChannelID      string `json:"channel_id,omitempty"      jsonschema:"New target Discord channel ID."`
	TargetID       string `json:"target_id,omitempty"       jsonschema:"Alias for channel_id."`
	CronExpression string `json:"cron_expression,omitempty" jsonschema:"New cron expression."`
	CronExpr       string `json:"cron_expr,omitempty"       jsonschema:"Alias for cron_expression."`
	Prompt         string `json:"prompt,omitempty"          jsonschema:"New prompt instructions."`
	TitlePrefix    string `json:"title_prefix,omitempty"    jsonschema:"New title prefix."`
	Timezone       string `json:"timezone,omitempty"        jsonschema:"New timezone for evaluation."`
	Effort         string `json:"effort,omitempty"          jsonschema:"New effort tier: 'low', 'medium', or 'high'."`
	Enabled        *bool  `json:"enabled,omitempty"         jsonschema:"Whether the heartbeat schedule is enabled."`
}

// ToMap converts typed heartbeat-update args to the untyped map the handler accepts.
func (a UpdateHeartbeatArgs) ToMap() map[string]any {
	m := make(map[string]any)
	if a.ScheduleID != "" {
		m["schedule_id"] = a.ScheduleID
	}
	if a.ID != "" {
		m["id"] = a.ID
	}
	if a.ChannelID != "" {
		m["channel_id"] = a.ChannelID
	}
	if a.TargetID != "" {
		m["target_id"] = a.TargetID
	}
	if a.CronExpression != "" {
		m["cron_expression"] = a.CronExpression
	}
	if a.CronExpr != "" {
		m["cron_expr"] = a.CronExpr
	}
	if a.Prompt != "" {
		m["prompt"] = a.Prompt
	}
	if a.TitlePrefix != "" {
		m["title_prefix"] = a.TitlePrefix
	}
	if a.Timezone != "" {
		m["timezone"] = a.Timezone
	}
	if a.Effort != "" {
		m[model.FieldEffort] = a.Effort
	}
	if a.Enabled != nil {
		m["enabled"] = *a.Enabled
	}
	return m
}

// WatchEventArgs defines arguments for the watch_event MCP tool.
type WatchEventArgs struct {
	ChannelID  string `json:"channel_id,omitempty" jsonschema:"Target Discord channel ID or thread ID."`
	TargetID   string `json:"target_id,omitempty"  jsonschema:"Alias for channel_id."`
	ThreadID   string `json:"thread_id,omitempty"  jsonschema:"Alias for channel_id."`
	Source     string `json:"source"               jsonschema:"Event source (e.g. 'flux', 'vanadis_build', 'vanadis_chunk', 'camera')."`
	Target     string `json:"target"               jsonschema:"Identifier of the target entity being watched."`
	EventType  string `json:"event_type"           jsonschema:"Type of event to watch (e.g. 'sync_success', 'completed', 'merged')."`
	Prompt     string `json:"prompt"               jsonschema:"Instructions to execute when the event triggers."`
	Effort     string `json:"effort,omitempty"     jsonschema:"Effort tier: 'low', 'medium', or 'high'. Defaults to 'medium'."`
	Persistent *bool  `json:"persistent,omitempty" jsonschema:"Whether the event watch trigger remains active after firing (default false)."`
	Agent      string `json:"agent,omitempty"      jsonschema:"Target agent namespace (defaults to caller agent)."`
}

// ToMap converts typed watch args to the untyped map the handler accepts.
func (a WatchEventArgs) ToMap() map[string]any {
	m := make(map[string]any)
	if a.ChannelID != "" {
		m["channel_id"] = a.ChannelID
	}
	if a.TargetID != "" {
		m["target_id"] = a.TargetID
	}
	if a.ThreadID != "" {
		m["thread_id"] = a.ThreadID
	}
	if a.Source != "" {
		m["source"] = a.Source
	}
	if a.Target != "" {
		m["target"] = a.Target
	}
	if a.EventType != "" {
		m["event_type"] = a.EventType
	}
	if a.Prompt != "" {
		m["prompt"] = a.Prompt
	}
	if a.Effort != "" {
		m[model.FieldEffort] = a.Effort
	}
	if a.Persistent != nil {
		m["persistent"] = *a.Persistent
	}
	if a.Agent != "" {
		m["agent"] = a.Agent
	}
	return m
}

// ListEventWatchesArgs defines arguments for the list_event_watches MCP tool.
type ListEventWatchesArgs struct {
	ChannelID string `json:"channel_id,omitempty" jsonschema:"Optional Discord channel ID filter."`
	TargetID  string `json:"target_id,omitempty"  jsonschema:"Alias for channel_id filter."`
	Agent     string `json:"agent,omitempty"      jsonschema:"Optional agent filter."`
}

// ToMap converts typed list-watches args to the untyped map the handler accepts.
func (a ListEventWatchesArgs) ToMap() map[string]any {
	m := make(map[string]any)
	if a.ChannelID != "" {
		m["channel_id"] = a.ChannelID
	}
	if a.TargetID != "" {
		m["target_id"] = a.TargetID
	}
	if a.Agent != "" {
		m["agent"] = a.Agent
	}
	return m
}

// CancelEventWatchArgs defines arguments for the cancel_event_watch MCP tool.
type CancelEventWatchArgs struct {
	WatchID    string `json:"watch_id,omitempty"    jsonschema:"The ID of the event watch trigger to cancel."`
	ID         string `json:"id,omitempty"          jsonschema:"Alias for watch_id."`
	ScheduleID string `json:"schedule_id,omitempty" jsonschema:"Alias for watch_id."`
}

// ToMap converts typed cancel-watch args to the untyped map the handler accepts.
func (a CancelEventWatchArgs) ToMap() map[string]any {
	m := make(map[string]any)
	if a.WatchID != "" {
		m["watch_id"] = a.WatchID
	}
	if a.ID != "" {
		m["id"] = a.ID
	}
	if a.ScheduleID != "" {
		m["schedule_id"] = a.ScheduleID
	}
	return m
}

// HandleScheduleHeartbeat handles the schedule_heartbeat MCP tool call.
func HandleScheduleHeartbeat(ctx context.Context, db *sql.DB, defaultTZ string, args map[string]any) (*mcp.CallToolResult, error) {
	channelID := getMapString(args, "channel_id", "target_id")
	if !model.IsNumericSnowflake(channelID) {
		return nil, fmt.Errorf("'channel_id' is required and must be a valid numeric snowflake")
	}

	cronExpr := getMapString(args, "cron_expr", "cron_expression")
	if cronExpr == "" {
		return nil, fmt.Errorf("'cron_expr' is required")
	}

	prompt := getMapString(args, "prompt")
	if prompt == "" {
		return nil, fmt.Errorf("'prompt' is required")
	}

	titlePrefix := getMapString(args, "title_prefix")
	if titlePrefix == "" {
		titlePrefix = "Heartbeat Alert"
	}

	tz := getMapString(args, "timezone")
	tz = resolveTimezone(tz, defaultTZ)

	agent := getMapString(args, "agent")

	effortTier := getMapString(args, "effort")
	effort, err := model.NormalizeEffort(effortTier)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	nextRun, err := CalculateNextCronRun(cronExpr, tz, now)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	hb := model.HeartbeatSchedule{
		ID:          id,
		TargetID:    channelID,
		TitlePrefix: titlePrefix,
		CronExpr:    cronExpr,
		Prompt:      prompt,
		Timezone:    tz,
		Effort:      effort,
		NextRunAt:   nextRun,
		Enabled:     true,
		CreatedAt:   now,
		Agent:       model.NormalizeAgent(agent),
	}

	if err = store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		return nil, fmt.Errorf("failed to persist heartbeat schedule: %w", err)
	}

	out := map[string]any{
		model.FieldStatus:  model.StatusSuccess,
		"schedule_id":      id,
		"cron_expression":  cronExpr,
		"next_run_at":      nextRun.Format(time.RFC3339),
		"channel_id":       channelID,
		model.FieldEffort:  effort,
		model.FieldMessage: "Heartbeat schedule created successfully.",
	}
	jsonBytes, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal heartbeat output: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, nil
}

func applyHeartbeatTimingUpdates(args map[string]any, sched *model.HeartbeatSchedule) (bool, error) {
	recompute := false
	if cronExpr := getMapString(args, "cron_expr", "cron_expression"); cronExpr != "" {
		if _, err := CalculateNextCronRun(cronExpr, sched.Timezone, time.Now().UTC()); err != nil {
			return false, err
		}
		sched.CronExpr = cronExpr
		recompute = true
	}
	if tz := getMapString(args, "timezone"); tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return false, fmt.Errorf("invalid timezone %q: %w", tz, err)
		}
		sched.Timezone = tz
		recompute = true
	}
	if enabledVal, ok := getMapBool(args, "enabled"); ok && enabledVal != nil {
		if sched.Enabled != *enabledVal {
			sched.Enabled = *enabledVal
			recompute = true
		}
	}
	return recompute, nil
}

func applyHeartbeatContentUpdates(args map[string]any, sched *model.HeartbeatSchedule) error {
	if channelID := getMapString(args, "channel_id", "target_id"); channelID != "" {
		if !model.IsNumericSnowflake(channelID) {
			return fmt.Errorf("'channel_id' must be a valid numeric snowflake")
		}
		sched.TargetID = channelID
	}
	if prompt := getMapString(args, "prompt"); prompt != "" {
		sched.Prompt = prompt
	}
	if titlePrefix, ok := args["title_prefix"]; ok && titlePrefix != nil {
		if s, ok := titlePrefix.(string); ok {
			sched.TitlePrefix = strings.TrimSpace(s)
		}
	}
	if effortTier := getMapString(args, "effort"); effortTier != "" {
		effort, err := model.NormalizeEffort(effortTier)
		if err != nil {
			return err
		}
		sched.Effort = effort
	}
	return nil
}

// HandleUpdateHeartbeat handles the update_heartbeat MCP tool call.
func HandleUpdateHeartbeat(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	id := getMapString(args, "id", "schedule_id")
	if id == "" {
		return nil, fmt.Errorf("'id' is required")
	}

	sched, err := store.GetHeartbeatSchedule(ctx, db, id)
	if err != nil {
		return nil, err
	}

	if err = applyHeartbeatContentUpdates(args, sched); err != nil {
		return nil, err
	}

	recomputeNextRun, err := applyHeartbeatTimingUpdates(args, sched)
	if err != nil {
		return nil, err
	}

	if recomputeNextRun && sched.Enabled {
		nextRun, calculateErr := CalculateNextCronRun(sched.CronExpr, sched.Timezone, time.Now().UTC())
		if calculateErr != nil {
			return nil, calculateErr
		}
		sched.NextRunAt = nextRun
	}

	if err = store.UpdateHeartbeatSchedule(ctx, db, *sched); err != nil {
		return nil, fmt.Errorf("failed to update heartbeat schedule: %w", err)
	}

	out := map[string]any{
		model.FieldStatus:  model.StatusSuccess,
		"schedule_id":      id,
		model.FieldMessage: "Heartbeat schedule updated successfully.",
		"next_run_at":      sched.NextRunAt.Format(time.RFC3339),
		model.FieldEffort:  sched.Effort,
		"enabled":          sched.Enabled,
	}
	jsonBytes, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal heartbeat output: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, nil
}

// HandleWatchEvent registers a deterministic one-shot event watch trigger.
func HandleWatchEvent(ctx context.Context, db *sql.DB, args map[string]any, extraSources ...string) (*mcp.CallToolResult, error) {
	channelID := getMapString(args, "channel_id", "target_id", "thread_id")
	if !model.IsNumericSnowflake(channelID) {
		return nil, fmt.Errorf("'channel_id' is required and must be a valid numeric snowflake")
	}

	prompt := getMapString(args, "prompt")
	if prompt == "" {
		return nil, fmt.Errorf("'prompt' is required")
	}

	effortStr := getMapString(args, "effort")
	if effortStr == "" {
		effortStr = model.EffortMedium
	}
	effort, err := model.NormalizeEffort(effortStr)
	if err != nil {
		return nil, err
	}

	source := getMapString(args, "source")
	target := getMapString(args, "target")
	eventType := getMapString(args, "event_type")

	persistentPtr, _ := getMapBool(args, "persistent")
	persistent := false
	if persistentPtr != nil {
		persistent = *persistentPtr
	}

	normSource, normTarget, normEventType, err := model.ValidateEventWatchSpec(source, target, eventType, extraSources...)
	if err != nil {
		return nil, err
	}

	if err = store.ValidateLiveEventTarget(ctx, db, normSource, normTarget); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	agent := getMapString(args, "agent")
	trigger := model.EventTrigger{
		ID:         id,
		ChannelID:  channelID,
		Source:     normSource,
		Target:     normTarget,
		EventType:  normEventType,
		Prompt:     prompt,
		Effort:     effort,
		CreatedAt:  time.Now().UTC(),
		Persistent: persistent,
		Agent:      model.NormalizeAgent(agent),
	}

	if err = store.InsertEventTrigger(ctx, db, trigger); err != nil {
		return nil, fmt.Errorf("failed to persist event trigger: %w", err)
	}

	// TOCTOU guard: re-verify target state after insertion.
	if normSource == model.SourceVanadisBuild || normSource == model.SourceVanadisChunk {
		if liveErr := store.ValidateLiveEventTarget(ctx, db, normSource, normTarget); liveErr != nil {
			if _, delErr := store.DeleteEventTrigger(ctx, db, id); delErr != nil {
				log.Printf("[Haro] failed to rollback event trigger %s: %v", id, delErr)
			}
			return nil, fmt.Errorf("target entered terminal state during registration: %w", liveErr)
		}
	}

	out := map[string]any{
		model.FieldStatus:  model.StatusSuccess,
		"watch_id":         id,
		"channel_id":       channelID,
		"source":           normSource,
		"target":           normTarget,
		"event_type":       normEventType,
		model.FieldEffort:  effort,
		"persistent":       trigger.Persistent,
		model.FieldMessage: "Event watch registered successfully.",
	}
	jsonBytes, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal watch event output: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, nil
}

// HandleListEventWatches returns active deterministic event watches.
func HandleListEventWatches(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	channelID := getMapString(args, "channel_id", "target_id")
	agent := getMapString(args, "agent")
	triggers, err := store.ListEventTriggers(ctx, db, channelID, agent)
	if err != nil {
		return nil, fmt.Errorf("failed to list event watches: %w", err)
	}

	out := map[string]any{
		"event_watches": triggers,
		"count":         len(triggers),
	}
	jsonBytes, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal list event watches output: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, nil
}

// HandleCancelEventWatch cancels a deterministic event watch trigger.
func HandleCancelEventWatch(ctx context.Context, db *sql.DB, args map[string]any) (*mcp.CallToolResult, error) {
	id := getMapString(args, "watch_id", "id", "schedule_id")
	if id == "" {
		return nil, fmt.Errorf("'watch_id' is required")
	}

	deleted, err := store.DeleteEventTrigger(ctx, db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to cancel event watch: %w", err)
	}
	if !deleted {
		return nil, fmt.Errorf("event watch %q not found", id)
	}

	out := map[string]any{
		model.FieldStatus:  model.StatusSuccess,
		"watch_id":         id,
		model.FieldMessage: "Event watch cancelled successfully.",
	}
	jsonBytes, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cancel event watch output: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, nil
}
