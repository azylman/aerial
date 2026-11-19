package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

// InsertOneShotSchedule persists a new one-shot schedule row.
func InsertOneShotSchedule(ctx context.Context, database *sql.DB, s OneShotSchedule) error {
	return store.InsertOneShotSchedule(ctx, database, s)
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

// GetScheduleSummaryMetrics computes summary metrics across all schedules and runs in the last 24 hours.
func GetScheduleSummaryMetrics(ctx context.Context, database *sql.DB) (ScheduleSummaryMetrics, error) {
	return store.GetScheduleSummaryMetrics(ctx, database)
}

// GetScheduleRunsPaginated fetches a paginated, filtered slice of schedule runs and the total count.
func GetScheduleRunsPaginated(ctx context.Context, database *sql.DB, limit, offset int, scheduleID, status string) ([]ScheduleRunJSON, int, error) {
	return store.GetScheduleRunsPaginated(ctx, database, limit, offset, scheduleID, status)
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
