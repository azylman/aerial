package model

import (
	"fmt"
	"strings"
	"time"
	// tzdata embeds the IANA timezone database so time.LoadLocation resolves
	// cron schedule timezones in distroless containers without system zoneinfo.
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// CalculateNextCronRun calculates the next UTC execution time for a cron expression in the specified timezone.
func CalculateNextCronRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}
	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		return time.Time{}, fmt.Errorf("timezone cannot be empty")
	}
	loc, err := time.LoadLocation(tzTrimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone %q: %w", tzTrimmed, err)
	}
	return sched.Next(from.In(loc)).UTC(), nil
}

// CalculateNextRun calculates the next UTC execution time for a cron expression in the specified timezone.
func CalculateNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	return CalculateNextCronRun(cronExpr, timezone, from)
}
