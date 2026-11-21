package router

import (
	"time"
)

// rolloutClock returns 2026-11-08 at hh:mm:ss UTC, the day of the rollout fixtures.
func rolloutClock(hh, mm, ss int) time.Time {
	return time.Date(2026, time.November, 8, hh, mm, ss, 0, time.UTC)
}
