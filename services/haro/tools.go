package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/robfig/cron/v3"
)

var (
	cronParser           = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	reRelative           = regexp.MustCompile(`^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)$`)
	explicitZonedLayouts = []string{time.RFC3339, time.RFC3339Nano}
	localLayouts         = []string{"2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04", "2006-01-02 15:04", "2006-01-02"}
)

func resolveTimezone(tz, fallback string) string {
	if s := strings.TrimSpace(tz); s != "" {
		return s
	}
	if s := strings.TrimSpace(fallback); s != "" {
		return s
	}
	return DefaultTimezone
}

// ParseRunAtWithTimezone parses relative durations (e.g. "30m", "2h", "1d", "45s", "2 hours", "1 day", "30 mins") or timestamps in specified timezone.
func ParseRunAtWithTimezone(input string, timezone string, now time.Time) (time.Time, error) {
	raw := strings.TrimSpace(input)
	if raw == "" {
		return time.Time{}, fmt.Errorf("run_at cannot be empty")
	}
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		return time.Time{}, fmt.Errorf("timezone cannot be empty")
	}
	loc, err := time.LoadLocation(tzTrimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone %q: %w", tzTrimmed, err)
	}

	lower := strings.ToLower(raw)
	if matches := reRelative.FindStringSubmatch(lower); len(matches) == 3 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid duration value: %s", matches[1])
		}
		var d time.Duration
		switch unit := matches[2]; {
		case strings.HasPrefix(unit, "s"):
			d = time.Duration(val) * time.Second
		case strings.HasPrefix(unit, "m"):
			d = time.Duration(val) * time.Minute
		case strings.HasPrefix(unit, "h"):
			d = time.Duration(val) * time.Hour
		case strings.HasPrefix(unit, "d"):
			d = time.Duration(val) * 24 * time.Hour
		case strings.HasPrefix(unit, "w"):
			d = time.Duration(val) * 7 * 24 * time.Hour
		}
		return now.Add(d).UTC(), nil
	}

	if d, err := time.ParseDuration(lower); err == nil {
		return now.Add(d).UTC(), nil
	}
	for _, layout := range explicitZonedLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	for _, layout := range localLayouts {
		if t, err := time.ParseInLocation(layout, raw, loc); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized run_at format %q: expected ISO timestamp (e.g. 2026-08-28T21:00:00Z) or relative duration (e.g. 30m, 2h, 1d)", raw)
}

func CalculateNextCronRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		return time.Time{}, fmt.Errorf("timezone cannot be empty")
	}
	loc, err := time.LoadLocation(tzTrimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone %q: %w", tzTrimmed, err)
	}
	return sched.Next(from.In(loc)).UTC(), nil
}

type ToolHandler struct {
	cfg      *Config
	db       *sql.DB
	timezone string
}

func NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler {
	if cfg == nil {
		panic("tool handler: config cannot be nil")
	}
	return &ToolHandler{cfg: cfg, db: database, timezone: cfg.Timezone}
}

type ScheduleRecurringArgs struct {
	ChannelID      string `json:"channel_id" jsonschema:"Discord Channel ID where fresh threads will be spawned."`
	CronExpression string `json:"cron_expression" jsonschema:"Standard 5-field cron expression (e.g. '0 20 * * 5') or macro (@daily, @weekly, @monthly)."`
	Prompt         string `json:"prompt" jsonschema:"Instructions to execute on every occurrence."`
	TitlePrefix    string `json:"title_prefix,omitempty" jsonschema:"Title prefix for spawned threads (e.g. 'Weekly Meal Plan')."`
	Timezone       string `json:"timezone,omitempty" jsonschema:"Timezone for evaluation (e.g. 'America/Los_Angeles', 'America/New_York', or 'UTC'). Defaults to configured server timezone."`
	Effort         string `json:"effort,omitempty" jsonschema:"Effort tier for the routine: 'high' (primary high-effort model), 'medium' (medium-effort model), or 'low' (lightweight low-effort model). Defaults to 'medium'."`
}

type ScheduleRecurringOutput struct {
	Status         string `json:"status"`
	ScheduleID     string `json:"schedule_id"`
	CronExpression string `json:"cron_expression"`
	NextRunAt      string `json:"next_run_at"`
	ChannelID      string `json:"channel_id"`
	Effort         string `json:"effort"`
	Message        string `json:"message"`
}

func handleScheduleRecurring(ctx context.Context, database *sql.DB, defaultTZ string, args ScheduleRecurringArgs) (ScheduleRecurringOutput, error) {
	args.ChannelID, args.CronExpression, args.Prompt, args.TitlePrefix = strings.TrimSpace(args.ChannelID), strings.TrimSpace(args.CronExpression), strings.TrimSpace(args.Prompt), strings.TrimSpace(args.TitlePrefix)
	args.Timezone = resolveTimezone(args.Timezone, defaultTZ)
	args.Effort = normalizeScheduleEffort(args.Effort)

	if args.ChannelID == "" {
		return ScheduleRecurringOutput{}, fmt.Errorf("'channel_id' is required")
	}
	if args.CronExpression == "" {
		return ScheduleRecurringOutput{}, fmt.Errorf("'cron_expression' is required")
	}
	if args.Prompt == "" {
		return ScheduleRecurringOutput{}, fmt.Errorf("'prompt' is required")
	}

	now := time.Now().UTC()
	nextRun, err := CalculateNextCronRun(args.CronExpression, args.Timezone, now)
	if err != nil {
		return ScheduleRecurringOutput{}, err
	}

	id := uuid.New().String()
	sched := CronSchedule{
		ID: id, TargetID: args.ChannelID, TitlePrefix: args.TitlePrefix, CronExpr: args.CronExpression,
		Prompt: args.Prompt, Timezone: args.Timezone, NextRunAt: nextRun, Enabled: true, CreatedAt: now, Effort: args.Effort,
	}
	if err := InsertCronSchedule(ctx, database, sched); err != nil {
		return ScheduleRecurringOutput{}, fmt.Errorf("failed to persist recurring schedule: %w", err)
	}

	return ScheduleRecurringOutput{
		Status: "success", ScheduleID: id, CronExpression: args.CronExpression,
		NextRunAt: nextRun.Format(time.RFC3339), ChannelID: args.ChannelID, Effort: args.Effort,
		Message: "Recurring schedule created successfully.",
	}, nil
}

func (h *ToolHandler) ScheduleRecurring(ctx context.Context, args ScheduleRecurringArgs) (ScheduleRecurringOutput, error) {
	var tz string
	var db *sql.DB
	if h != nil {
		tz, db = h.timezone, h.db
	}
	return handleScheduleRecurring(ctx, db, tz, args)
}

type ScheduleOnceArgs struct {
	TargetID string `json:"target_id,omitempty" jsonschema:"Target Discord thread ID or channel ID where reminder will be delivered."`
	ThreadID string `json:"thread_id,omitempty" jsonschema:"Target Discord thread ID or channel ID where reminder will be delivered."`
	RunAt    string `json:"run_at" jsonschema:"ISO 8601 timestamp (e.g. '2026-08-28T21:00:00Z') or relative duration (e.g. '30m', '2h', '1d')."`
	Prompt   string `json:"prompt" jsonschema:"Content/instructions of the reminder."`
	Timezone string `json:"timezone,omitempty" jsonschema:"Timezone for absolute timestamp evaluation (e.g. 'America/Los_Angeles', 'America/New_York', or 'UTC'). Defaults to configured server timezone."`
	Effort   string `json:"effort,omitempty" jsonschema:"Effort tier: 'low' (lightweight low-effort model), 'medium' (medium-effort model), or 'high' (primary high-effort model). Defaults to 'medium'."`
}

type ScheduleOnceOutput struct {
	Status     string `json:"status"`
	ScheduleID string `json:"schedule_id"`
	RunAt      string `json:"run_at"`
	Effort     string `json:"effort"`
	Message    string `json:"message"`
}

func handleScheduleOnce(ctx context.Context, database *sql.DB, args ScheduleOnceArgs) (ScheduleOnceOutput, error) {
	targetID := strings.TrimSpace(args.TargetID)
	if targetID == "" {
		targetID = strings.TrimSpace(args.ThreadID)
	}
	args.RunAt, args.Prompt = strings.TrimSpace(args.RunAt), strings.TrimSpace(args.Prompt)
	args.Timezone = resolveTimezone(args.Timezone, "")

	if targetID == "" {
		return ScheduleOnceOutput{}, fmt.Errorf("'target_id' is required")
	}
	if args.RunAt == "" {
		return ScheduleOnceOutput{}, fmt.Errorf("'run_at' is required")
	}
	if args.Prompt == "" {
		return ScheduleOnceOutput{}, fmt.Errorf("'prompt' is required")
	}

	now := time.Now().UTC()
	targetTime, err := ParseRunAtWithTimezone(args.RunAt, args.Timezone, now)
	if err != nil {
		return ScheduleOnceOutput{}, err
	}

	effort := normalizeScheduleEffort(args.Effort)
	id := uuid.New().String()
	sched := OneShotSchedule{ID: id, ThreadID: targetID, Prompt: args.Prompt, RunAt: targetTime, CreatedAt: now, Effort: effort}
	if err := InsertOneShotSchedule(ctx, database, sched); err != nil {
		return ScheduleOnceOutput{}, fmt.Errorf("failed to persist one-shot schedule: %w", err)
	}

	return ScheduleOnceOutput{
		Status: "success", ScheduleID: id, RunAt: targetTime.Format(time.RFC3339),
		Effort: effort, Message: "One-shot reminder scheduled successfully.",
	}, nil
}

func (h *ToolHandler) ScheduleOnce(ctx context.Context, args ScheduleOnceArgs) (ScheduleOnceOutput, error) {
	var db *sql.DB
	if h != nil {
		args.Timezone, db = resolveTimezone(args.Timezone, h.timezone), h.db
	}
	return handleScheduleOnce(ctx, db, args)
}

type ListSchedulesArgs struct {
	TargetID string `json:"target_id,omitempty" jsonschema:"Optional Discord Channel or Thread ID to filter schedules."`
}

type ListSchedulesOutput struct {
	Recurring  []CronSchedule      `json:"recurring"`
	OneShot    []OneShotSchedule   `json:"one_shot"`
	Heartbeats []HeartbeatSchedule `json:"heartbeats"`
	Summary    string              `json:"summary,omitempty"`
}

func FormatSchedulesSummary(out ListSchedulesOutput) string {
	var lines []string
	for _, r := range out.Recurring {
		lines = append(lines, fmt.Sprintf("[RECURRING] ID: %s | Cron: %s | Channel: %s | Effort: %s | Next: %s | Prompt: %s", r.ID, r.CronExpr, r.TargetID, r.Effort, r.NextRunAt.Format(time.RFC3339), r.Prompt))
	}
	for _, o := range out.OneShot {
		lines = append(lines, fmt.Sprintf("[ONE_SHOT] ID: %s | Target: %s | Effort: %s | RunAt: %s | Prompt: %s", o.ID, o.ThreadID, o.Effort, o.RunAt.Format(time.RFC3339), o.Prompt))
	}
	for _, h := range out.Heartbeats {
		lines = append(lines, fmt.Sprintf("[HEARTBEAT] ID: %s | Cron: %s | Channel: %s | Effort: %s | Next: %s | Prompt: %s", h.ID, h.CronExpr, h.TargetID, h.Effort, h.NextRunAt.Format(time.RFC3339), h.Prompt))
	}
	if len(lines) == 0 {
		return "No active schedules found."
	}
	return strings.Join(lines, "\n")
}

func (h *ToolHandler) ListSchedules(ctx context.Context, args ListSchedulesArgs) (ListSchedulesOutput, error) {
	targetID := strings.TrimSpace(args.TargetID)
	crons, err := ListCronSchedules(ctx, h.db, targetID)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query recurring schedules: %w", err)
	}
	oneShots, err := ListOneShotSchedules(ctx, h.db, targetID)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query one-shot schedules: %w", err)
	}
	heartbeats, err := ListHeartbeatSchedules(ctx, h.db, targetID)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query heartbeat schedules: %w", err)
	}
	if crons == nil {
		crons = []CronSchedule{}
	}
	if oneShots == nil {
		oneShots = []OneShotSchedule{}
	}
	if heartbeats == nil {
		heartbeats = []HeartbeatSchedule{}
	}
	out := ListSchedulesOutput{Recurring: crons, OneShot: oneShots, Heartbeats: heartbeats}
	out.Summary = FormatSchedulesSummary(out)
	return out, nil
}

type CancelScheduleArgs struct {
	ScheduleID string `json:"schedule_id" jsonschema:"The ID of the schedule to cancel."`
}

type CancelScheduleOutput struct {
	Status     string `json:"status"`
	ScheduleID string `json:"schedule_id"`
	Message    string `json:"message"`
}

func (h *ToolHandler) CancelSchedule(ctx context.Context, args CancelScheduleArgs) (CancelScheduleOutput, error) {
	id := strings.TrimSpace(args.ScheduleID)
	if id == "" {
		return CancelScheduleOutput{}, fmt.Errorf("'schedule_id' is required")
	}
	deleted, err := DeleteSchedule(ctx, h.db, id)
	if err != nil {
		return CancelScheduleOutput{}, fmt.Errorf("failed to delete schedule %s: %w", id, err)
	}
	if !deleted {
		return CancelScheduleOutput{}, fmt.Errorf("schedule with ID %q not found", id)
	}
	return CancelScheduleOutput{Status: "success", ScheduleID: id, Message: "Schedule cancelled successfully."}, nil
}

type UpdateCronScheduleArgs struct {
	ScheduleID     string  `json:"schedule_id" jsonschema:"The ID of the recurring cron schedule to update."`
	Effort         *string `json:"effort,omitempty" jsonschema:"Effort tier for the routine: 'high' (primary high-effort model), 'medium' (medium-effort model), or 'low' (lightweight low-effort model)."`
	CronExpression *string `json:"cron_expression,omitempty" jsonschema:"New 5-field cron expression or macro."`
	Prompt         *string `json:"prompt,omitempty" jsonschema:"New prompt instructions to execute on every occurrence."`
	TitlePrefix    *string `json:"title_prefix,omitempty" jsonschema:"New title prefix for spawned threads."`
	Timezone       *string `json:"timezone,omitempty" jsonschema:"New timezone for evaluation."`
}

type UpdateCronScheduleOutput struct {
	Status     string `json:"status"`
	ScheduleID string `json:"schedule_id"`
	Message    string `json:"message"`
	Effort     string `json:"effort,omitempty"`
	NextRunAt  string `json:"next_run_at,omitempty"`
}

func (h *ToolHandler) UpdateCronSchedule(ctx context.Context, args UpdateCronScheduleArgs) (UpdateCronScheduleOutput, error) {
	id := strings.TrimSpace(args.ScheduleID)
	if id == "" {
		return UpdateCronScheduleOutput{}, fmt.Errorf("'schedule_id' is required")
	}

	var nextRun *time.Time
	if args.CronExpression != nil && strings.TrimSpace(*args.CronExpression) != "" {
		var tzArg, hTZ string
		if args.Timezone != nil {
			tzArg = *args.Timezone
		}
		if h != nil {
			hTZ = h.timezone
		}
		tz := resolveTimezone(tzArg, hTZ)
		computed, err := CalculateNextCronRun(*args.CronExpression, tz, time.Now().UTC())
		if err != nil {
			return UpdateCronScheduleOutput{}, err
		}
		nextRun = &computed
	}

	if err := UpdateCronSchedule(ctx, h.db, id, args.Effort, args.CronExpression, args.Prompt, args.TitlePrefix, args.Timezone, nextRun); err != nil {
		return UpdateCronScheduleOutput{}, fmt.Errorf("failed to update recurring schedule: %w", err)
	}

	resp := UpdateCronScheduleOutput{Status: "success", ScheduleID: id, Message: "Recurring schedule updated successfully."}
	if args.Effort != nil {
		resp.Effort = normalizeScheduleEffort(*args.Effort)
	}
	if nextRun != nil {
		resp.NextRunAt = nextRun.Format(time.RFC3339)
	}
	return resp, nil
}

func getMapString(args map[string]any, keys ...string) string {
	if args == nil {
		return ""
	}
	for _, k := range keys {
		if v, ok := args[k]; ok && v != nil {
			if s, ok := v.(string); ok {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func getMapBool(args map[string]any, keys ...string) (*bool, bool) {
	if args == nil {
		return nil, false
	}
	for _, k := range keys {
		if v, ok := args[k]; ok && v != nil {
			switch b := v.(type) {
			case bool:
				return &b, true
			case string:
				lower := strings.ToLower(strings.TrimSpace(b))
				if lower == "true" || lower == "1" {
					val := true
					return &val, true
				}
				if lower == "false" || lower == "0" {
					val := false
					return &val, true
				}
			}
		}
	}
	return nil, false
}

// HandleScheduleHeartbeat handles the schedule_heartbeat MCP tool call.
func HandleScheduleHeartbeat(ctx context.Context, db *sql.DB, defaultTZ string, args map[string]any) (*mcp.CallToolResult, error) {
	channelID := getMapString(args, "channel_id", "target_id")
	if !IsNumericSnowflake(channelID) {
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

	effortTier := getMapString(args, "effort")
	effort, err := NormalizeEffort(effortTier)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	nextRun, err := CalculateNextCronRun(cronExpr, tz, now)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	hb := HeartbeatSchedule{
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
	}

	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		return nil, fmt.Errorf("failed to persist heartbeat schedule: %w", err)
	}

	out := map[string]any{
		"status":          "success",
		"schedule_id":     id,
		"cron_expression": cronExpr,
		"next_run_at":     nextRun.Format(time.RFC3339),
		"channel_id":      channelID,
		"effort":          effort,
		"message":         "Heartbeat schedule created successfully.",
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

func applyHeartbeatTimingUpdates(args map[string]any, sched *HeartbeatSchedule) (bool, error) {
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

func applyHeartbeatContentUpdates(args map[string]any, sched *HeartbeatSchedule) error {
	if channelID := getMapString(args, "channel_id", "target_id"); channelID != "" {
		if !IsNumericSnowflake(channelID) {
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
		effort, err := NormalizeEffort(effortTier)
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

	sched, err := GetHeartbeatSchedule(ctx, db, id)
	if err != nil {
		return nil, err
	}

	if err := applyHeartbeatContentUpdates(args, sched); err != nil {
		return nil, err
	}

	recomputeNextRun, err := applyHeartbeatTimingUpdates(args, sched)
	if err != nil {
		return nil, err
	}

	if recomputeNextRun && sched.Enabled {
		nextRun, err := CalculateNextCronRun(sched.CronExpr, sched.Timezone, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		sched.NextRunAt = nextRun
	}

	if err := UpdateHeartbeatSchedule(ctx, db, *sched); err != nil {
		return nil, fmt.Errorf("failed to update heartbeat schedule: %w", err)
	}

	out := map[string]any{
		"status":      "success",
		"schedule_id": id,
		"message":     "Heartbeat schedule updated successfully.",
		"next_run_at": sched.NextRunAt.Format(time.RFC3339),
		"effort":      sched.Effort,
		"enabled":     sched.Enabled,
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
