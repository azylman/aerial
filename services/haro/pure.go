package main

// Root compatibility shims for the Layer-0 domain package.
//
// Every type, constant, and pure helper formerly defined here now lives in
// internal/model (Layer 0). This file keeps zero-logic aliases only so all
// not-yet-moved callers compile unchanged until the final purge (haro-07b).
// Shims contain no logic and carry no coverage-bearing statements.

import (
	"github.com/azylman/aerial/services/haro/internal/model"
)

// Domain row and DTO type aliases.
type (
	CronSchedule              = model.CronSchedule
	OneShotSchedule           = model.OneShotSchedule
	HeartbeatSchedule         = model.HeartbeatSchedule
	EventTrigger              = model.EventTrigger
	ScheduleRun               = model.ScheduleRun
	ScheduleRunJSON           = model.ScheduleRunJSON
	ScheduleSummaryMetrics    = model.ScheduleSummaryMetrics
	CronScheduleWithDesc      = model.CronScheduleWithDesc
	HeartbeatScheduleWithDesc = model.HeartbeatScheduleWithDesc
	OneShotScheduleJSON       = model.OneShotScheduleJSON
	SchedulesResponse         = model.SchedulesResponse
	ScheduleRunsResponse      = model.ScheduleRunsResponse
)

// RedactDSN returns a log-safe form of a database DSN.
func RedactDSN(dsn string) string {
	return model.RedactDSN(dsn)
}
