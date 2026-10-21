package scheduler

import (
	"fmt"
	"log"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/robfig/cron/v3"
)

// FormatThreadTitle formats the thread title for a recurring cron trigger, clamped to at most 100 runes.
func FormatThreadTitle(titlePrefix string, t time.Time) string {
	dateStr := t.Format("Jan 02, 2006")
	trimmed := strings.TrimSpace(titlePrefix)
	var title string
	if trimmed == "" {
		title = fmt.Sprintf("Scheduled Routine – %s", dateStr)
	} else {
		title = fmt.Sprintf("%s – %s", trimmed, dateStr)
	}
	runes := []rune(title)
	if len(runes) > 100 {
		runes = append(runes[:97], []rune("...")...)
	}
	return string(runes)
}

// CalculateNextRun parses a standard 5-field cron or descriptor and computes the next run time in UTC.
func CalculateNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	if from.IsZero() {
		return time.Time{}, fmt.Errorf("from timestamp cannot be zero")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		return time.Time{}, fmt.Errorf("timezone cannot be empty")
	}

	loc := time.UTC
	if l, err := time.LoadLocation(tzTrimmed); err == nil {
		loc = l
	} else {
		log.Printf("[Scheduler] Warning: unknown timezone %q, falling back to UTC", tzTrimmed)
	}

	fromInLoc := from.In(loc)
	next := sched.Next(fromInLoc)
	return next.UTC(), nil
}

// IsScheduleDue returns true if the current time has reached or passed nextRunAt.
func IsScheduleDue(nextRunAt, now time.Time) bool {
	if nextRunAt.IsZero() || now.IsZero() {
		return false
	}
	return !now.Before(nextRunAt)
}

// EvaluateCronStaleness returns true if the scheduled trigger is overdue by more than 24 hours.
func EvaluateCronStaleness(nextRunAt, now time.Time) bool {
	if nextRunAt.IsZero() || now.IsZero() {
		return false
	}
	return now.Sub(nextRunAt) > 24*time.Hour
}

// ResolveCronTimezone resolves the schedule timezone, falling back to defaultTimezone or UTC if blank.
func ResolveCronTimezone(scheduleTz, defaultTz string) string {
	if tz := strings.TrimSpace(scheduleTz); tz != "" {
		return tz
	}
	if tz := strings.TrimSpace(defaultTz); tz != "" {
		return tz
	}
	return "UTC"
}

// ShouldCreateThread determines whether a new public Discord thread should be created.
func ShouldCreateThread(isAlreadyThread bool, policyMode string, hasThreadCreator bool) bool {
	return !isAlreadyThread && strings.ToLower(strings.TrimSpace(policyMode)) != "channel" && hasThreadCreator
}

// BuildCronMessageSummary builds a cleaned task summary optionally prefixed with [titlePrefix].
func BuildCronMessageSummary(titlePrefix, prompt string) string {
	clean := db.CleanTaskSummary(prompt)
	trimmedPrefix := strings.TrimSpace(titlePrefix)
	if trimmedPrefix != "" {
		return fmt.Sprintf("[%s] %s", trimmedPrefix, clean)
	}
	return clean
}

// BuildOneShotMessageSummary builds a reminder summary prefixed with [Reminder].
func BuildOneShotMessageSummary(prompt string) string {
	return fmt.Sprintf("[Reminder] %s", db.CleanTaskSummary(prompt))
}

// DueCronAction encapsulates the pure evaluated decisions for a due cron schedule.
type DueCronAction struct {
	IsStale            bool
	ShouldFire         bool
	NextRunAt          time.Time
	NextRunError       error
	Title              string
	ShouldCreateThread bool
	Summary            string
	Effort             string
}

// PlanDueCronTurn evaluates the full lifecycle of a due cron event.
// When cron expression parsing fails, it falls back to advancing by 24 hours and preserves NextRunError.
func PlanDueCronTurn(
	cron db.CronSchedule,
	now time.Time,
	isAlreadyThread bool,
	policyMode string,
	hasThreadCreator bool,
	defaultTimezone string,
) DueCronAction {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tz := ResolveCronTimezone(cron.Timezone, defaultTimezone)
	nextRun, err := CalculateNextRun(cron.CronExpr, tz, now)
	if err != nil {
		nextRun = now.Add(24 * time.Hour)
	}

	isStale := EvaluateCronStaleness(cron.NextRunAt, now)
	title := FormatThreadTitle(cron.TitlePrefix, now)
	shouldCreate := ShouldCreateThread(isAlreadyThread, policyMode, hasThreadCreator)
	summary := BuildCronMessageSummary(cron.TitlePrefix, cron.Prompt)

	return DueCronAction{
		IsStale:            isStale,
		ShouldFire:         !isStale,
		NextRunAt:          nextRun,
		NextRunError:       err,
		Title:              title,
		ShouldCreateThread: shouldCreate,
		Summary:            summary,
		Effort:             cron.Effort,
	}
}

func newScheduleRun(id, schedID, schedType, msgID, targetID, threadID, title, prompt, effort string, now time.Time) db.ScheduleRun {
	return db.ScheduleRun{
		ID:           id,
		ScheduleID:   schedID,
		ScheduleType: schedType,
		MessageID:    msgID,
		TargetID:     targetID,
		ThreadID:     threadID,
		Title:        title,
		Prompt:       prompt,
		Status:       "enqueued",
		StartedAt:    now,
		Effort:       effort,
	}
}

func newScheduledMessage(msgID, runID, threadID, prompt, summary, effort string, now time.Time) db.Message {
	return db.Message{
		ID:            msgID,
		ThreadID:      threadID,
		GuildID:       "scheduled",
		AuthorID:      "scheduler",
		AuthorName:    "Scheduler",
		Content:       prompt,
		Summary:       summary,
		Status:        db.StatusPending,
		ScheduleRunID: runID,
		CreatedAt:     now,
		UpdatedAt:     now,
		Effort:        effort,
	}
}

// BuildCronScheduleRun constructs a db.ScheduleRun for a cron turn.
func BuildCronScheduleRun(
	runID, msgID, targetThreadID string,
	cron db.CronSchedule,
	title string,
	now time.Time,
) db.ScheduleRun {
	return newScheduleRun(runID, cron.ID, "cron", msgID, cron.TargetID, targetThreadID, title, cron.Prompt, cron.Effort, now)
}

// BuildCronMessage constructs a db.Message for an enqueued cron turn.
func BuildCronMessage(
	msgID, runID, targetThreadID string,
	cron db.CronSchedule,
	summary string,
	now time.Time,
) db.Message {
	return newScheduledMessage(msgID, runID, targetThreadID, cron.Prompt, summary, cron.Effort, now)
}

// BuildOneShotScheduleRun constructs a db.ScheduleRun for a one-shot reminder.
func BuildOneShotScheduleRun(
	runID, msgID string,
	oneShot db.OneShotSchedule,
	now time.Time,
) db.ScheduleRun {
	return newScheduleRun(runID, oneShot.ID, "one_shot", msgID, oneShot.ThreadID, oneShot.ThreadID, "One-shot Reminder", oneShot.Prompt, oneShot.Effort, now)
}

// BuildOneShotMessage constructs a db.Message for an enqueued one-shot reminder.
func BuildOneShotMessage(
	msgID, runID string,
	oneShot db.OneShotSchedule,
	summary string,
	now time.Time,
) db.Message {
	return newScheduledMessage(msgID, runID, oneShot.ThreadID, oneShot.Prompt, summary, oneShot.Effort, now)
}

// ShouldRunPruneRetention returns true when tickCount represents a daily interval (every 2880 ticks at 30s).
func ShouldRunPruneRetention(tickCount int) bool {
	return tickCount > 0 && tickCount%2880 == 0
}

// ShouldRunTranscriptSync returns true when tickCount represents a 5-minute interval (every 10 ticks at 30s).
func ShouldRunTranscriptSync(tickCount int) bool {
	return tickCount > 0 && tickCount%10 == 0
}
