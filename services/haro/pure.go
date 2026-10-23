package main

import (
	"time"
)

// IsScheduleStale reports whether scheduledAt is more overdue than maxOverdue relative to now.
func IsScheduleStale(scheduledAt, now time.Time, maxOverdue time.Duration) bool {
	return maxOverdue > 0 && now.Sub(scheduledAt) > maxOverdue
}

// ShouldPruneRuns reports whether runs should be pruned given the last prune time, interval, and now.
func ShouldPruneRuns(now, lastPrune time.Time, interval time.Duration) bool {
	return interval > 0 && (lastPrune.IsZero() || now.Sub(lastPrune) >= interval)
}

// CalculateNextRun calculates the next UTC execution time for a cron expression in the specified timezone.
func CalculateNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	return CalculateNextCronRun(cronExpr, timezone, from)
}
