package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// GetDefaultTimezone returns the configured default timezone for the server.
// Reads DEFAULT_TIMEZONE -> TZ -> fallback "America/Los_Angeles".
func GetDefaultTimezone() string {
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		return tz
	}
	if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		return tz
	}
	return "America/Los_Angeles"
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ParseRunAtWithTimezone parses relative durations (e.g. "30m", "2h", "1d", "45s", "2 hours", "1 day", "30 mins") or timestamps in specified timezone.
func ParseRunAtWithTimezone(input string, timezone string, now time.Time) (time.Time, error) {
	raw := strings.TrimSpace(input)
	if raw == "" {
		return time.Time{}, fmt.Errorf("run_at cannot be empty")
	}

	lower := strings.ToLower(raw)

	// 1. Check relative human durations with regex
	reRelative := regexp.MustCompile(`^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)$`)
	if matches := reRelative.FindStringSubmatch(lower); len(matches) == 3 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid duration value: %s", matches[1])
		}
		unit := matches[2]
		var duration time.Duration
		switch {
		case strings.HasPrefix(unit, "s"):
			duration = time.Duration(val) * time.Second
		case strings.HasPrefix(unit, "m"):
			duration = time.Duration(val) * time.Minute
		case strings.HasPrefix(unit, "h"):
			duration = time.Duration(val) * time.Hour
		case strings.HasPrefix(unit, "d"):
			duration = time.Duration(val) * 24 * time.Hour
		case strings.HasPrefix(unit, "w"):
			duration = time.Duration(val) * 7 * 24 * time.Hour
		}
		return now.Add(duration).UTC(), nil
	}

	// 2. Try standard Go time.ParseDuration (e.g. "1h30m", "45s")
	if d, err := time.ParseDuration(lower); err == nil {
		return now.Add(d).UTC(), nil
	}

	// 3. Try standard absolute date/time layouts with explicit timezone / UTC
	explicitZonedLayouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, layout := range explicitZonedLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}

	// 4. Try standard absolute layouts in specified timezone
	loc := time.UTC
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed != "" {
		if l, err := time.LoadLocation(tzTrimmed); err == nil {
			loc = l
		}
	}

	localLayouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, layout := range localLayouts {
		if t, err := time.ParseInLocation(layout, raw, loc); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unrecognized run_at format %q: expected ISO timestamp (e.g. 2026-08-28T21:00:00Z) or relative duration (e.g. 30m, 2h, 1d)", raw)
}

// ParseRunAt parses relative durations or ISO timestamps using default timezone.
func ParseRunAt(input string, now time.Time) (time.Time, error) {
	return ParseRunAtWithTimezone(input, GetDefaultTimezone(), now)
}

func CalculateNextCronRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	loc := time.UTC
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		tzTrimmed = GetDefaultTimezone()
	}
	if l, err := time.LoadLocation(tzTrimmed); err == nil {
		loc = l
	}

	fromInLoc := from.In(loc)
	next := sched.Next(fromInLoc)
	return next.UTC(), nil
}

type ToolHandler struct {
	db *sql.DB
}

func NewToolHandler(database *sql.DB) *ToolHandler {
	return &ToolHandler{db: database}
}

type ScheduleRecurringArgs struct {
	ChannelID      string `json:"channel_id"`
	CronExpression string `json:"cron_expression"`
	Prompt         string `json:"prompt"`
	TitlePrefix    string `json:"title_prefix,omitempty"`
	Timezone       string `json:"timezone,omitempty"`
}

func (h *ToolHandler) HandleScheduleRecurring(rawArgs json.RawMessage) (interface{}, error) {
	var args ScheduleRecurringArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}

	args.ChannelID = strings.TrimSpace(args.ChannelID)
	args.CronExpression = strings.TrimSpace(args.CronExpression)
	args.Prompt = strings.TrimSpace(args.Prompt)
	args.TitlePrefix = strings.TrimSpace(args.TitlePrefix)
	args.Timezone = strings.TrimSpace(args.Timezone)
	if args.Timezone == "" {
		args.Timezone = GetDefaultTimezone()
	}

	if args.ChannelID == "" {
		return nil, fmt.Errorf("'channel_id' is required")
	}
	if args.CronExpression == "" {
		return nil, fmt.Errorf("'cron_expression' is required")
	}
	if args.Prompt == "" {
		return nil, fmt.Errorf("'prompt' is required")
	}

	now := time.Now().UTC()
	nextRun, err := CalculateNextCronRun(args.CronExpression, args.Timezone, now)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	sched := CronSchedule{
		ID:          id,
		TargetID:    args.ChannelID,
		TitlePrefix: args.TitlePrefix,
		CronExpr:    args.CronExpression,
		Prompt:      args.Prompt,
		Timezone:    args.Timezone,
		NextRunAt:   nextRun,
		Enabled:     true,
		CreatedAt:   now,
	}

	if err := InsertCronSchedule(h.db, sched); err != nil {
		return nil, fmt.Errorf("failed to persist recurring schedule: %w", err)
	}

	return map[string]interface{}{
		"status":          "success",
		"schedule_id":     id,
		"cron_expression": args.CronExpression,
		"next_run_at":     nextRun.Format(time.RFC3339),
		"channel_id":      args.ChannelID,
		"message":         "Recurring schedule created successfully.",
	}, nil
}

type ScheduleOnceArgs struct {
	TargetID string `json:"target_id"`
	RunAt    string `json:"run_at"`
	Prompt   string `json:"prompt"`
	Timezone string `json:"timezone,omitempty"`
}

func (h *ToolHandler) HandleScheduleOnce(rawArgs json.RawMessage) (interface{}, error) {
	var args ScheduleOnceArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}

	args.TargetID = strings.TrimSpace(args.TargetID)
	args.RunAt = strings.TrimSpace(args.RunAt)
	args.Prompt = strings.TrimSpace(args.Prompt)
	args.Timezone = strings.TrimSpace(args.Timezone)
	if args.Timezone == "" {
		args.Timezone = GetDefaultTimezone()
	}

	if args.TargetID == "" {
		return nil, fmt.Errorf("'target_id' is required")
	}
	if args.RunAt == "" {
		return nil, fmt.Errorf("'run_at' is required")
	}
	if args.Prompt == "" {
		return nil, fmt.Errorf("'prompt' is required")
	}

	now := time.Now().UTC()
	targetTime, err := ParseRunAtWithTimezone(args.RunAt, args.Timezone, now)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	sched := OneShotSchedule{
		ID:        id,
		ThreadID:  args.TargetID,
		Prompt:    args.Prompt,
		RunAt:     targetTime,
		CreatedAt: now,
	}

	if err := InsertOneShotSchedule(h.db, sched); err != nil {
		return nil, fmt.Errorf("failed to persist one-shot schedule: %w", err)
	}

	return map[string]interface{}{
		"status":      "success",
		"schedule_id": id,
		"run_at":      targetTime.Format(time.RFC3339),
		"message":     "One-shot reminder scheduled successfully.",
	}, nil
}

type ListSchedulesArgs struct {
	TargetID string `json:"target_id,omitempty"`
}

func (h *ToolHandler) HandleListSchedules(rawArgs json.RawMessage) (interface{}, error) {
	var args ListSchedulesArgs
	if len(rawArgs) > 0 {
		_ = json.Unmarshal(rawArgs, &args)
	}
	args.TargetID = strings.TrimSpace(args.TargetID)

	crons, err := ListCronSchedules(h.db, args.TargetID)
	if err != nil {
		return nil, fmt.Errorf("failed to query recurring schedules: %w", err)
	}
	if crons == nil {
		crons = []CronSchedule{}
	}

	oneShots, err := ListOneShotSchedules(h.db, args.TargetID)
	if err != nil {
		return nil, fmt.Errorf("failed to query one-shot schedules: %w", err)
	}
	if oneShots == nil {
		oneShots = []OneShotSchedule{}
	}

	return map[string]interface{}{
		"recurring": crons,
		"one_shot":  oneShots,
	}, nil
}

type CancelScheduleArgs struct {
	ScheduleID string `json:"schedule_id"`
}

func (h *ToolHandler) HandleCancelSchedule(rawArgs json.RawMessage) (interface{}, error) {
	var args CancelScheduleArgs
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	args.ScheduleID = strings.TrimSpace(args.ScheduleID)
	if args.ScheduleID == "" {
		return nil, fmt.Errorf("'schedule_id' is required")
	}

	deleted, err := DeleteSchedule(h.db, args.ScheduleID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete schedule %s: %w", args.ScheduleID, err)
	}

	if !deleted {
		return nil, fmt.Errorf("schedule with ID %q not found", args.ScheduleID)
	}

	return map[string]interface{}{
		"status":      "success",
		"schedule_id": args.ScheduleID,
		"message":     "Schedule cancelled successfully.",
	}, nil
}
