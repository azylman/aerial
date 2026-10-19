package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var (
	cronParser           = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	reRelative           = regexp.MustCompile(`^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)$`)
	explicitZonedLayouts = []string{time.RFC3339, time.RFC3339Nano}
	localLayouts         = []string{"2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04", "2006-01-02 15:04", "2006-01-02"}
)

func resolveTimezone(tz, fallback string) string {
	if s := strings.TrimSpace(tz); s != "" { return s }
	if s := strings.TrimSpace(fallback); s != "" { return s }
	return DefaultTimezone
}

// ParseRunAtWithTimezone parses relative durations (e.g. "30m", "2h", "1d", "45s", "2 hours", "1 day", "30 mins") or timestamps in specified timezone.
func ParseRunAtWithTimezone(input string, timezone string, now time.Time) (time.Time, error) {
	raw := strings.TrimSpace(input)
	if raw == "" { return time.Time{}, fmt.Errorf("run_at cannot be empty") }
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" { return time.Time{}, fmt.Errorf("timezone cannot be empty") }
	loc, err := time.LoadLocation(tzTrimmed)
	if err != nil { return time.Time{}, fmt.Errorf("invalid timezone %q: %w", tzTrimmed, err) }

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
	if err != nil { return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err) }
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" { return time.Time{}, fmt.Errorf("timezone cannot be empty") }
	loc, err := time.LoadLocation(tzTrimmed)
	if err != nil { return time.Time{}, fmt.Errorf("invalid timezone %q: %w", tzTrimmed, err) }
	return sched.Next(from.In(loc)).UTC(), nil
}

type ToolHandler struct {
	cfg      *Config
	db       *sql.DB
	timezone string
}

func NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler {
	if cfg == nil { panic("tool handler: config cannot be nil") }
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

	if args.ChannelID == "" { return ScheduleRecurringOutput{}, fmt.Errorf("'channel_id' is required") }
	if args.CronExpression == "" { return ScheduleRecurringOutput{}, fmt.Errorf("'cron_expression' is required") }
	if args.Prompt == "" { return ScheduleRecurringOutput{}, fmt.Errorf("'prompt' is required") }

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
	if h != nil { tz, db = h.timezone, h.db }
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
	if targetID == "" { targetID = strings.TrimSpace(args.ThreadID) }
	args.RunAt, args.Prompt = strings.TrimSpace(args.RunAt), strings.TrimSpace(args.Prompt)
	args.Timezone = resolveTimezone(args.Timezone, "")

	if targetID == "" { return ScheduleOnceOutput{}, fmt.Errorf("'target_id' is required") }
	if args.RunAt == "" { return ScheduleOnceOutput{}, fmt.Errorf("'run_at' is required") }
	if args.Prompt == "" { return ScheduleOnceOutput{}, fmt.Errorf("'prompt' is required") }

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
	if h != nil { args.Timezone, db = resolveTimezone(args.Timezone, h.timezone), h.db }
	return handleScheduleOnce(ctx, db, args)
}

type ListSchedulesArgs struct {
	TargetID string `json:"target_id,omitempty" jsonschema:"Optional Discord Channel or Thread ID to filter schedules."`
}

type ListSchedulesOutput struct {
	Recurring []CronSchedule    `json:"recurring"`
	OneShot   []OneShotSchedule `json:"one_shot"`
}

func (h *ToolHandler) ListSchedules(ctx context.Context, args ListSchedulesArgs) (ListSchedulesOutput, error) {
	targetID := strings.TrimSpace(args.TargetID)
	crons, err := ListCronSchedules(ctx, h.db, targetID)
	if err != nil { return ListSchedulesOutput{}, fmt.Errorf("failed to query recurring schedules: %w", err) }
	oneShots, err := ListOneShotSchedules(ctx, h.db, targetID)
	if err != nil { return ListSchedulesOutput{}, fmt.Errorf("failed to query one-shot schedules: %w", err) }
	if crons == nil { crons = []CronSchedule{} }
	if oneShots == nil { oneShots = []OneShotSchedule{} }
	return ListSchedulesOutput{Recurring: crons, OneShot: oneShots}, nil
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
	if id == "" { return CancelScheduleOutput{}, fmt.Errorf("'schedule_id' is required") }
	deleted, err := DeleteSchedule(ctx, h.db, id)
	if err != nil { return CancelScheduleOutput{}, fmt.Errorf("failed to delete schedule %s: %w", id, err) }
	if !deleted { return CancelScheduleOutput{}, fmt.Errorf("schedule with ID %q not found", id) }
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
	if id == "" { return UpdateCronScheduleOutput{}, fmt.Errorf("'schedule_id' is required") }

	var nextRun *time.Time
	if args.CronExpression != nil && strings.TrimSpace(*args.CronExpression) != "" {
		var tzArg, hTZ string
		if args.Timezone != nil { tzArg = *args.Timezone }
		if h != nil { hTZ = h.timezone }
		tz := resolveTimezone(tzArg, hTZ)
		computed, err := CalculateNextCronRun(*args.CronExpression, tz, time.Now().UTC())
		if err != nil { return UpdateCronScheduleOutput{}, err }
		nextRun = &computed
	}

	if err := UpdateCronSchedule(ctx, h.db, id, args.Effort, args.CronExpression, args.Prompt, args.TitlePrefix, args.Timezone, nextRun); err != nil {
		return UpdateCronScheduleOutput{}, fmt.Errorf("failed to update recurring schedule: %w", err)
	}

	resp := UpdateCronScheduleOutput{Status: "success", ScheduleID: id, Message: "Recurring schedule updated successfully."}
	if args.Effort != nil { resp.Effort = normalizeScheduleEffort(*args.Effort) }
	if nextRun != nil { resp.NextRunAt = nextRun.Format(time.RFC3339) }
	return resp, nil
}
