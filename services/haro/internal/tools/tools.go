// Package tools is the Layer-3 MCP tool-handler package for haro: typed
// schedule arguments/outputs, schedule CRUD methods, run-at parsing, and
// heartbeat/event-watch handlers. It imports Layer-0 packages internal/model
// (row types, constants, pure helpers) and internal/config (Config,
// DefaultTimezone) plus Layer-2 internal/store (persistence) only. Root
// tools.go keeps zero-logic aliases until the haro-07b purge.
package tools

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	// tzdata embeds the IANA timezone database so ParseRunAtWithTimezone
	// resolves schedule timezones in distroless containers without zoneinfo.
	_ "time/tzdata"

	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

var (
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
	return config.DefaultTimezone
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

// ToolHandler executes haro MCP tool calls against the schedule store.
// Fields are exported so the compiler-enforced layering (not yet moved
// server.go) can construct and read handlers across packages; root
// package main aliases this struct until the final purge.
type ToolHandler struct {
	Cfg        *config.Config
	DB         *sql.DB
	Timezone   string
	httpClient *http.Client
}

// NewToolHandler builds a ToolHandler over database, propagating the
// configured Vanadis URL to the store for live event-target validation.
func NewToolHandler(cfg *config.Config, database *sql.DB) *ToolHandler {
	if cfg == nil {
		panic("tool handler: config cannot be nil")
	}
	if cfg.VanadisURL != "" {
		store.SetVanadisURL(cfg.VanadisURL)
	}
	return &ToolHandler{Cfg: cfg, DB: database, Timezone: cfg.Timezone, httpClient: http.DefaultClient}
}

func (h *ToolHandler) RestateIngressURL() string {
	if h != nil && h.Cfg != nil {
		return strings.TrimRight(strings.TrimSpace(h.Cfg.RestateIngressURL), "/")
	}
	return ""
}

func (h *ToolHandler) armSchedule(ctx context.Context, scheduleType, id string) {
	ingressURL := h.RestateIngressURL()
	id = strings.TrimSpace(id)
	if ingressURL == "" || id == "" {
		return
	}
	idempotencyKey := fmt.Sprintf("haro:arm:%s:%s:%d", scheduleType, id, time.Now().UnixNano())
	endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Arm/send", ingressURL, url.PathEscape(id))
	payload := map[string]any{model.FieldScheduleType: scheduleType}
	client := http.DefaultClient
	if h != nil && h.httpClient != nil {
		client = h.httpClient
	}
	if _, postErr := restateingress.Post(ctx, client, endpoint, idempotencyKey, payload, 0); postErr != nil {
		log.Printf("[Haro Tools] warning: restate arm schedule %s failed: %v", id, postErr)
	}
}

func (h *ToolHandler) cancelSchedule(ctx context.Context, id string) {
	ingressURL := h.RestateIngressURL()
	id = strings.TrimSpace(id)
	if ingressURL == "" || id == "" {
		return
	}
	idempotencyKey := fmt.Sprintf("haro:cancel:%s:%d", id, time.Now().UnixNano())
	endpoint := fmt.Sprintf("%s/HaroSchedule/%s/Cancel/send", ingressURL, url.PathEscape(id))
	client := http.DefaultClient
	if h != nil && h.httpClient != nil {
		client = h.httpClient
	}
	if _, postErr := restateingress.Post(ctx, client, endpoint, idempotencyKey, struct{}{}, 0); postErr != nil {
		log.Printf("[Haro Tools] warning: restate cancel schedule %s failed: %v", id, postErr)
	}
}

type ScheduleRecurringArgs struct {
	ChannelID      string `json:"channel_id"             jsonschema:"Discord Channel ID where fresh threads will be spawned."`
	CronExpression string `json:"cron_expression"        jsonschema:"Standard 5-field cron expression (e.g. '0 20 * * 5') or macro (@daily, @weekly, @monthly)."`
	Prompt         string `json:"prompt"                 jsonschema:"Instructions to execute on every occurrence."`
	TitlePrefix    string `json:"title_prefix,omitempty" jsonschema:"Title prefix for spawned threads (e.g. 'Weekly Meal Plan')."`
	Timezone       string `json:"timezone,omitempty"     jsonschema:"Timezone for evaluation (e.g. 'America/Los_Angeles', 'America/New_York', or 'UTC'). Defaults to configured server timezone."`
	Effort         string `json:"effort,omitempty"       jsonschema:"Effort tier for the routine: 'high' (primary high-effort model), 'medium' (medium-effort model), or 'low' (lightweight low-effort model). Defaults to 'medium'."`
	Agent          string `json:"agent,omitempty"        jsonschema:"Target agent namespace (defaults to caller agent)."`
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
	args.Effort = store.NormalizeScheduleEffort(args.Effort)

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
	nextRun, err := model.CalculateNextCronRun(args.CronExpression, args.Timezone, now)
	if err != nil {
		return ScheduleRecurringOutput{}, err
	}

	id := uuid.New().String()
	sched := model.CronSchedule{
		ID: id, TargetID: args.ChannelID, TitlePrefix: args.TitlePrefix, CronExpr: args.CronExpression,
		Prompt: args.Prompt, Timezone: args.Timezone, NextRunAt: nextRun, Enabled: true, CreatedAt: now, Effort: args.Effort,
		Agent: model.NormalizeAgent(args.Agent),
	}
	if err := store.InsertCronSchedule(ctx, database, sched); err != nil {
		return ScheduleRecurringOutput{}, fmt.Errorf("failed to persist recurring schedule: %w", err)
	}

	return ScheduleRecurringOutput{
		Status: model.StatusSuccess, ScheduleID: id, CronExpression: args.CronExpression,
		NextRunAt: nextRun.Format(time.RFC3339), ChannelID: args.ChannelID, Effort: args.Effort,
		Message: "Recurring schedule created successfully.",
	}, nil
}

// ScheduleRecurring registers a persistent recurring cron schedule.
func (h *ToolHandler) ScheduleRecurring(ctx context.Context, args ScheduleRecurringArgs) (ScheduleRecurringOutput, error) {
	var tz string
	var db *sql.DB
	if h != nil {
		tz, db = h.Timezone, h.DB
	}
	out, err := handleScheduleRecurring(ctx, db, tz, args)
	if err == nil && h != nil {
		h.armSchedule(ctx, model.ScheduleTypeCron, out.ScheduleID)
	}
	return out, err
}

type ScheduleOnceArgs struct {
	TargetID string `json:"target_id,omitempty" jsonschema:"Target Discord thread ID or channel ID where reminder will be delivered."`
	ThreadID string `json:"thread_id,omitempty" jsonschema:"Target Discord thread ID or channel ID where reminder will be delivered."`
	RunAt    string `json:"run_at"              jsonschema:"ISO 8601 timestamp (e.g. '2026-08-28T21:00:00Z') or relative duration (e.g. '30m', '2h', '1d')."`
	Prompt   string `json:"prompt"              jsonschema:"Content/instructions of the reminder."`
	Timezone string `json:"timezone,omitempty"  jsonschema:"Timezone for absolute timestamp evaluation (e.g. 'America/Los_Angeles', 'America/New_York', or 'UTC'). Defaults to configured server timezone."`
	Effort   string `json:"effort,omitempty"    jsonschema:"Effort tier: 'low' (lightweight low-effort model), 'medium' (medium-effort model), or 'high' (primary high-effort model). Defaults to 'medium'."`
	Agent    string `json:"agent,omitempty"     jsonschema:"Target agent namespace (defaults to caller agent)."`
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

	effort := store.NormalizeScheduleEffort(args.Effort)
	id := uuid.New().String()
	sched := model.OneShotSchedule{
		ID:        id,
		ThreadID:  targetID,
		Prompt:    args.Prompt,
		RunAt:     targetTime,
		CreatedAt: now,
		Effort:    effort,
		Agent:     model.NormalizeAgent(args.Agent),
	}
	if err := store.InsertOneShotSchedule(ctx, database, sched); err != nil {
		return ScheduleOnceOutput{}, fmt.Errorf("failed to persist one-shot schedule: %w", err)
	}

	return ScheduleOnceOutput{
		Status: model.StatusSuccess, ScheduleID: id, RunAt: targetTime.Format(time.RFC3339),
		Effort: effort, Message: "One-shot reminder scheduled successfully.",
	}, nil
}

// ScheduleOnce registers a persistent one-shot reminder.
func (h *ToolHandler) ScheduleOnce(ctx context.Context, args ScheduleOnceArgs) (ScheduleOnceOutput, error) {
	var db *sql.DB
	if h != nil {
		args.Timezone, db = resolveTimezone(args.Timezone, h.Timezone), h.DB
	}
	out, err := handleScheduleOnce(ctx, db, args)
	if err == nil && h != nil {
		h.armSchedule(ctx, model.ScheduleTypeOneShot, out.ScheduleID)
	}
	return out, err
}

type ListSchedulesArgs struct {
	TargetID string `json:"target_id,omitempty" jsonschema:"Optional Discord Channel or Thread ID to filter schedules."`
	Agent    string `json:"agent,omitempty"     jsonschema:"Optional agent filter."`
}

type ListSchedulesOutput struct {
	Recurring  []model.CronSchedule      `json:"recurring"`
	OneShot    []model.OneShotSchedule   `json:"one_shot"`
	Heartbeats []model.HeartbeatSchedule `json:"heartbeats"`
	Summary    string                    `json:"summary,omitempty"`
}

// FormatSchedulesSummary renders a human-readable one-line-per-schedule digest.
func FormatSchedulesSummary(out ListSchedulesOutput) string {
	lines := make([]string, 0, len(out.Recurring)+len(out.OneShot)+len(out.Heartbeats))
	for _, r := range out.Recurring {
		lines = append(lines, fmt.Sprintf("[RECURRING] ID: %s | Title: %s | Cron: %s | Channel: %s | Effort: %s | Next: %s", r.ID, r.TitlePrefix, r.CronExpr, r.TargetID, r.Effort, r.NextRunAt.Format(time.RFC3339)))
	}
	for _, o := range out.OneShot {
		lines = append(lines, fmt.Sprintf("[ONE_SHOT] ID: %s | Target: %s | Effort: %s | RunAt: %s", o.ID, o.ThreadID, o.Effort, o.RunAt.Format(time.RFC3339)))
	}
	for _, h := range out.Heartbeats {
		lines = append(lines, fmt.Sprintf("[HEARTBEAT] ID: %s | Title: %s | Cron: %s | Channel: %s | Effort: %s | Next: %s", h.ID, h.TitlePrefix, h.CronExpr, h.TargetID, h.Effort, h.NextRunAt.Format(time.RFC3339)))
	}
	if len(lines) == 0 {
		return "No active schedules found."
	}
	return strings.Join(lines, "\n")
}

// ListSchedules lists recurring, one-shot, and heartbeat schedules with prompts redacted.
func (h *ToolHandler) ListSchedules(ctx context.Context, args ListSchedulesArgs) (ListSchedulesOutput, error) {
	targetID := strings.TrimSpace(args.TargetID)
	crons, err := store.ListCronSchedules(ctx, h.DB, targetID, args.Agent)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query recurring schedules: %w", err)
	}
	oneShots, err := store.ListOneShotSchedules(ctx, h.DB, targetID, args.Agent)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query one-shot schedules: %w", err)
	}
	heartbeats, err := store.ListHeartbeatSchedules(ctx, h.DB, targetID, args.Agent)
	if err != nil {
		return ListSchedulesOutput{}, fmt.Errorf("failed to query heartbeat schedules: %w", err)
	}
	if crons == nil {
		crons = []model.CronSchedule{}
	}
	if oneShots == nil {
		oneShots = []model.OneShotSchedule{}
	}
	if heartbeats == nil {
		heartbeats = []model.HeartbeatSchedule{}
	}
	for i := range crons {
		crons[i].Prompt = ""
	}
	for i := range oneShots {
		oneShots[i].Prompt = ""
	}
	for i := range heartbeats {
		heartbeats[i].Prompt = ""
	}
	out := ListSchedulesOutput{Recurring: crons, OneShot: oneShots, Heartbeats: heartbeats}
	out.Summary = FormatSchedulesSummary(out)
	return out, nil
}

type GetScheduleArgs struct {
	ScheduleID string `json:"schedule_id" jsonschema:"The ID of the schedule to retrieve."`
}

type GetScheduleOutput struct {
	Type      string                   `json:"type"`
	Recurring *model.CronSchedule      `json:"recurring,omitempty"`
	OneShot   *model.OneShotSchedule   `json:"one_shot,omitempty"`
	Heartbeat *model.HeartbeatSchedule `json:"heartbeat,omitempty"`
}

// GetSchedule fetches full details (including prompt) for one schedule by ID.
func (h *ToolHandler) GetSchedule(ctx context.Context, args GetScheduleArgs) (GetScheduleOutput, error) {
	id := strings.TrimSpace(args.ScheduleID)
	if id == "" {
		return GetScheduleOutput{}, fmt.Errorf("'schedule_id' is required")
	}
	if c, err := store.GetCronSchedule(ctx, h.DB, id); err == nil {
		return GetScheduleOutput{Type: "recurring", Recurring: c}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return GetScheduleOutput{}, fmt.Errorf("failed to query recurring schedule %s: %w", id, err)
	}
	if o, err := store.GetOneShotSchedule(ctx, h.DB, id); err == nil {
		return GetScheduleOutput{Type: model.ScheduleTypeOneShot, OneShot: o}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return GetScheduleOutput{}, fmt.Errorf("failed to query one-shot schedule %s: %w", id, err)
	}
	if hb, err := store.GetHeartbeatSchedule(ctx, h.DB, id); err == nil {
		return GetScheduleOutput{Type: model.ScheduleTypeHeartbeat, Heartbeat: hb}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return GetScheduleOutput{}, fmt.Errorf("failed to query heartbeat schedule %s: %w", id, err)
	}
	return GetScheduleOutput{}, fmt.Errorf("schedule with ID %q not found", id)
}

type CancelScheduleArgs struct {
	ScheduleID string `json:"schedule_id" jsonschema:"The ID of the schedule to cancel."`
}

type CancelScheduleOutput struct {
	Status     string `json:"status"`
	ScheduleID string `json:"schedule_id"`
	Message    string `json:"message"`
}

// CancelSchedule deletes a schedule by ID across every schedule table.
func (h *ToolHandler) CancelSchedule(ctx context.Context, args CancelScheduleArgs) (CancelScheduleOutput, error) {
	id := strings.TrimSpace(args.ScheduleID)
	if id == "" {
		return CancelScheduleOutput{}, fmt.Errorf("'schedule_id' is required")
	}
	deleted, err := store.DeleteSchedule(ctx, h.DB, id)
	if err != nil {
		return CancelScheduleOutput{}, fmt.Errorf("failed to delete schedule %s: %w", id, err)
	}
	if !deleted {
		return CancelScheduleOutput{}, fmt.Errorf("schedule with ID %q not found", id)
	}
	if h != nil {
		h.cancelSchedule(ctx, id)
	}
	return CancelScheduleOutput{Status: model.StatusSuccess, ScheduleID: id, Message: "Schedule cancelled successfully."}, nil
}

type UpdateCronScheduleArgs struct {
	ScheduleID     string  `json:"schedule_id"               jsonschema:"The ID of the recurring cron schedule to update."`
	Effort         *string `json:"effort,omitempty"          jsonschema:"Effort tier for the routine: 'high' (primary high-effort model), 'medium' (medium-effort model), or 'low' (lightweight low-effort model)."`
	CronExpression *string `json:"cron_expression,omitempty" jsonschema:"New 5-field cron expression or macro."`
	Prompt         *string `json:"prompt,omitempty"          jsonschema:"New prompt instructions to execute on every occurrence."`
	TitlePrefix    *string `json:"title_prefix,omitempty"    jsonschema:"New title prefix for spawned threads."`
	Timezone       *string `json:"timezone,omitempty"        jsonschema:"New timezone for evaluation."`
}

type UpdateCronScheduleOutput struct {
	Status     string `json:"status"`
	ScheduleID string `json:"schedule_id"`
	Message    string `json:"message"`
	Effort     string `json:"effort,omitempty"`
	NextRunAt  string `json:"next_run_at,omitempty"`
}

// UpdateCronSchedule patches selected recurring cron schedule columns.
func (h *ToolHandler) UpdateCronSchedule(ctx context.Context, args UpdateCronScheduleArgs) (UpdateCronScheduleOutput, error) {
	id := strings.TrimSpace(args.ScheduleID)
	if id == "" {
		return UpdateCronScheduleOutput{}, fmt.Errorf("'schedule_id' is required")
	}

	var nextRun *time.Time
	if newExpr, newTZ := trimmedOrEmpty(args.CronExpression), trimmedOrEmpty(args.Timezone); newExpr != "" || newTZ != "" {
		computed, err := h.recomputeCronNextRun(ctx, id, newExpr, newTZ)
		if err != nil {
			return UpdateCronScheduleOutput{}, err
		}
		nextRun = &computed
	}

	if err := store.UpdateCronSchedule(ctx, h.DB, id, args.Effort, args.CronExpression, args.Prompt, args.TitlePrefix, args.Timezone, nextRun); err != nil {
		return UpdateCronScheduleOutput{}, fmt.Errorf("failed to update recurring schedule: %w", err)
	}

	if h != nil {
		sched, getErr := store.GetCronSchedule(ctx, h.DB, id)
		if getErr == nil && !sched.Enabled {
			h.cancelSchedule(ctx, id)
		} else {
			h.armSchedule(ctx, model.ScheduleTypeCron, id)
		}
	}

	resp := UpdateCronScheduleOutput{Status: model.StatusSuccess, ScheduleID: id, Message: "Recurring schedule updated successfully."}
	if args.Effort != nil {
		resp.Effort = store.NormalizeScheduleEffort(*args.Effort)
	}
	if nextRun != nil {
		resp.NextRunAt = nextRun.Format(time.RFC3339)
	}
	return resp, nil
}

// recomputeCronNextRun returns the next run for an update that changes the cron
// expression or timezone, evaluated against the stored row so the result uses the
// timezone the schedule will actually run in. Blank newExpr/newTZ keep stored values.
func (h *ToolHandler) recomputeCronNextRun(ctx context.Context, id, newExpr, newTZ string) (time.Time, error) {
	sched, err := store.GetCronSchedule(ctx, h.DB, id)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to update recurring schedule: %w", err)
	}
	expr := sched.CronExpr
	if newExpr != "" {
		expr = newExpr
	}
	tz := resolveTimezone(newTZ, resolveTimezone(sched.Timezone, h.Timezone))
	return model.CalculateNextCronRun(expr, tz, time.Now().UTC())
}

func trimmedOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(*s)
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
