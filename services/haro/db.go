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
