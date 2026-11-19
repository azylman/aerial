package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/services/haro/internal/store"
)

var sqliteTestInitHook func(string) (*sql.DB, error)

// Domain row types (CronSchedule, OneShotSchedule, HeartbeatSchedule,
// EventTrigger, ScheduleRun, ScheduleRunJSON, ScheduleSummaryMetrics) now
// live in internal/model; pure.go keeps zero-logic aliases for this package.

// OpenDB opens the scheduler database from cfg. SQLite DSNs keep routing
// through the test hook until the event-trigger half moves to internal/store;
// all other DSNs delegate to internal/store.
func OpenDB(ctx context.Context, cfg *Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	return openDB(ctx, cfg.DatabaseURL)
}

// NewDB opens the scheduler database from cfg.
func NewDB(ctx context.Context, cfg *Config) (*sql.DB, error) {
	return OpenDB(ctx, cfg)
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	trimmed := strings.TrimSpace(dsn)
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")
	if sqliteTestInitHook != nil && (trimmed == ":memory:" || strings.Contains(trimmed, ".db") || strings.Contains(trimmed, "sqlite") || strings.HasPrefix(trimmed, "file:")) {
		return sqliteTestInitHook(trimmed)
	}
	return store.NewDB(ctx, &Config{DatabaseURL: dsn})
}

func normalizeScheduleEffort(effort string) string {
	return store.NormalizeScheduleEffort(effort)
}

// InsertCronSchedule persists a new cron schedule row.
func InsertCronSchedule(ctx context.Context, database *sql.DB, c CronSchedule) error {
	return store.InsertCronSchedule(ctx, database, c)
}

// InsertOneShotSchedule persists a new one-shot schedule row.
func InsertOneShotSchedule(ctx context.Context, database *sql.DB, s OneShotSchedule) error {
	return store.InsertOneShotSchedule(ctx, database, s)
}

// InsertHeartbeatSchedule persists a new heartbeat schedule row.
func InsertHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	return store.InsertHeartbeatSchedule(ctx, database, h)
}

// ListCronSchedules lists enabled cron schedules, optionally filtered by target.
func ListCronSchedules(ctx context.Context, database *sql.DB, targetID string) ([]CronSchedule, error) {
	return store.ListCronSchedules(ctx, database, targetID)
}

// ListOneShotSchedules lists one-shot schedules, optionally filtered by thread.
func ListOneShotSchedules(ctx context.Context, database *sql.DB, targetID string) ([]OneShotSchedule, error) {
	return store.ListOneShotSchedules(ctx, database, targetID)
}

// ListHeartbeatSchedules lists enabled heartbeat schedules, optionally filtered by target.
func ListHeartbeatSchedules(ctx context.Context, database *sql.DB, targetID string) ([]HeartbeatSchedule, error) {
	return store.ListHeartbeatSchedules(ctx, database, targetID)
}

// GetCronSchedule fetches a cron schedule by id.
func GetCronSchedule(ctx context.Context, database *sql.DB, id string) (*CronSchedule, error) {
	return store.GetCronSchedule(ctx, database, id)
}

// GetOneShotSchedule fetches a one-shot schedule by id.
func GetOneShotSchedule(ctx context.Context, database *sql.DB, id string) (*OneShotSchedule, error) {
	return store.GetOneShotSchedule(ctx, database, id)
}

// GetHeartbeatSchedule fetches a heartbeat schedule by id.
func GetHeartbeatSchedule(ctx context.Context, database *sql.DB, id string) (*HeartbeatSchedule, error) {
	return store.GetHeartbeatSchedule(ctx, database, id)
}

// UpdateHeartbeatSchedule replaces a heartbeat schedule row.
func UpdateHeartbeatSchedule(ctx context.Context, database *sql.DB, h HeartbeatSchedule) error {
	return store.UpdateHeartbeatSchedule(ctx, database, h)
}

// UpdateCronSchedule patches selected cron schedule columns.
func UpdateCronSchedule(ctx context.Context, database *sql.DB, id string, effort, cronExpr, prompt, titlePrefix, timezone *string, nextRunAt *time.Time) error {
	return store.UpdateCronSchedule(ctx, database, id, effort, cronExpr, prompt, titlePrefix, timezone, nextRunAt)
}

// DeleteSchedule deletes a schedule id from every schedule table.
func DeleteSchedule(ctx context.Context, database *sql.DB, scheduleID string) (bool, error) {
	return store.DeleteSchedule(ctx, database, scheduleID)
}

// GetScheduleSummaryMetrics computes summary metrics across all schedules and runs in the last 24 hours.
func GetScheduleSummaryMetrics(ctx context.Context, database *sql.DB) (ScheduleSummaryMetrics, error) {
	return store.GetScheduleSummaryMetrics(ctx, database)
}

// GetScheduleRunsPaginated fetches a paginated, filtered slice of schedule runs and the total count.
func GetScheduleRunsPaginated(ctx context.Context, database *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRunJSON, int, error) {
	return store.GetScheduleRunsPaginated(ctx, database, limit, offset, scheduleID, status)
}

// SetVanadisURL sets the base URL for Vanadis HTTP API calls.
func SetVanadisURL(u string) {
	store.SetVanadisURL(u)
}

// ValidateLiveEventTarget checks that the referenced target is known and not in a terminal state.
func ValidateLiveEventTarget(ctx context.Context, database *sql.DB, source, target string) error {
	return store.ValidateLiveEventTarget(ctx, database, source, target)
}

// InsertEventTrigger persists a new event trigger in the database.
func InsertEventTrigger(ctx context.Context, database *sql.DB, t EventTrigger) error {
	return store.InsertEventTrigger(ctx, database, t)
}

// ListEventTriggers retrieves event triggers, optionally filtered by channel_id.
func ListEventTriggers(ctx context.Context, database *sql.DB, channelID string) ([]EventTrigger, error) {
	return store.ListEventTriggers(ctx, database, channelID)
}

// ClaimEventTriggers atomically deletes and returns matching event triggers from event_triggers.
func ClaimEventTriggers(ctx context.Context, database *sql.DB, source string, targets []string, eventType string) ([]EventTrigger, error) {
	return store.ClaimEventTriggers(ctx, database, source, targets, eventType)
}

// DeleteEventTrigger removes an event trigger by id.
func DeleteEventTrigger(ctx context.Context, database *sql.DB, id string) (bool, error) {
	return store.DeleteEventTrigger(ctx, database, id)
}
