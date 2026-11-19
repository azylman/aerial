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

// Schedule type aliases.
const (
	scheduleTypeCron       = model.ScheduleTypeCron
	scheduleTypeOneShot    = model.ScheduleTypeOneShot
	scheduleTypeHeartbeat  = model.ScheduleTypeHeartbeat
	scheduleTypeEventWatch = model.ScheduleTypeEventWatch
)

// Run, tool-response and build event status aliases.
const (
	statusCompleted     = model.StatusCompleted
	statusFailed        = model.StatusFailed
	statusStale         = model.StatusStale
	statusSkippedActive = model.StatusSkippedActive
	statusMerged        = model.StatusMerged
	statusSuccess       = model.StatusSuccess
)

// Reasoning effort level aliases.
const (
	effortLow    = model.EffortLow
	effortMedium = model.EffortMedium
	effortHigh   = model.EffortHigh
)

// Event-watch source aliases.
const (
	sourceVanadisBuild = model.SourceVanadisBuild
	sourceVanadisChunk = model.SourceVanadisChunk
)

// Shared field key aliases.
const (
	fieldStatus  = model.FieldStatus
	fieldEffort  = model.FieldEffort
	fieldMessage = model.FieldMessage
	fieldError   = model.FieldError
)

// NormalizeEffort validates effort tiers, defaulting empty to "low".
func NormalizeEffort(effort string) (string, error) {
	return model.NormalizeEffort(effort)
}

// IsNumericSnowflake reports whether id is a non-empty string consisting only of ASCII digits [0-9].
func IsNumericSnowflake(id string) bool {
	return model.IsNumericSnowflake(id)
}

// FormatEventTriggerPrompt combines an optional context header with an event trigger prompt.
func FormatEventTriggerPrompt(contextHeader, prompt string) string {
	return model.FormatEventTriggerPrompt(contextHeader, prompt)
}

// ValidateEventWatchSpec validates and normalizes event trigger parameters.
func ValidateEventWatchSpec(source, target, eventType string, extraSources ...string) (normSource, normTarget, normEventType string, err error) {
	return model.ValidateEventWatchSpec(source, target, eventType, extraSources...)
}

// SanitizeScheduleText redacts GitHub PATs, Bearer tokens, and secret key assignments.
func SanitizeScheduleText(s string) string {
	return model.SanitizeScheduleText(s)
}

// FormatCronSchedulesWithDesc converts raw CronSchedule records into serialized CronScheduleWithDesc items.
func FormatCronSchedulesWithDesc(raw []CronSchedule) []CronScheduleWithDesc {
	return model.FormatCronSchedulesWithDesc(raw)
}

// FormatHeartbeatSchedulesWithDesc converts raw HeartbeatSchedule records into serialized HeartbeatScheduleWithDesc items.
func FormatHeartbeatSchedulesWithDesc(raw []HeartbeatSchedule) []HeartbeatScheduleWithDesc {
	return model.FormatHeartbeatSchedulesWithDesc(raw)
}

// FormatOneShotSchedulesJSON converts raw OneShotSchedule records into serialized OneShotScheduleJSON items.
func FormatOneShotSchedulesJSON(raw []OneShotSchedule) []OneShotScheduleJSON {
	return model.FormatOneShotSchedulesJSON(raw)
}

// RedactDSN returns a log-safe form of a database DSN.
func RedactDSN(dsn string) string {
	return model.RedactDSN(dsn)
}
