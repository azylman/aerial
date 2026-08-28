package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// ThreadCreator defines an interface for creating Discord threads.
type ThreadCreator interface {
	CreatePublicThread(channelID, name string) (threadID string, err error)
}

// DiscordThreadCreator implements ThreadCreator using discordgo.Session.
type DiscordThreadCreator struct {
	session *discordgo.Session
}

func NewDiscordThreadCreator(s *discordgo.Session) *DiscordThreadCreator {
	return &DiscordThreadCreator{session: s}
}

func (d *DiscordThreadCreator) CreatePublicThread(channelID, name string) (string, error) {
	if d == nil || d.session == nil {
		return channelID, nil
	}
	th, err := d.session.ThreadStart(channelID, name, discordgo.ChannelTypeGuildPublicThread, 1440)
	if err != nil {
		return "", err
	}
	return th.ID, nil
}

// MessageEnqueuer abstracts enqueueing messages to the worker pool.
type MessageEnqueuer interface {
	Enqueue(msg db.Message)
}

// FormatThreadTitle formats the thread title for a recurring cron trigger, clamped to at most 100 runes.
func FormatThreadTitle(titlePrefix string, t time.Time) string {
	dateStr := t.Format("Jan 02, 2006")
	trimmed := strings.TrimSpace(titlePrefix)
	var title string
	if trimmed == "" {
		title = fmt.Sprintf("Scheduled Routine - %s", dateStr)
	} else {
		title = fmt.Sprintf("%s - %s", trimmed, dateStr)
	}
	runes := []rune(title)
	if len(runes) > 100 {
		runes = append(runes[:97], []rune("...")...)
	}
	return string(runes)
}

// GetDefaultTimezone returns the default timezone configured for the scheduler.
// Reads DEFAULT_TIMEZONE -> TZ -> fallback "America/Los_Angeles".
func GetDefaultTimezone() string {
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		return tz
	}
	if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		return tz
	}
	return "America/Los_Angeles"
}

// CalculateNextRun parses a standard 5-field cron or descriptor and computes the next run time in UTC.
func CalculateNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	tzTrimmed := strings.TrimSpace(timezone)
	if tzTrimmed == "" {
		tzTrimmed = GetDefaultTimezone()
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

// ProcessDueSchedules evaluates and processes due cron and one-shot schedules.
func ProcessDueSchedules(ctx context.Context, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator) error {
	if database == nil {
		return nil
	}

	now := time.Now().UTC()

	// 1. Process Due Cron Schedules
	dueCrons, err := db.GetDueCronSchedules(database)
	if err != nil {
		return fmt.Errorf("error querying due cron schedules: %w", err)
	}

	for _, c := range dueCrons {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 24h staleness guard: if cron trigger is overdue by >24h, advance next_run_at without firing.
		if now.Sub(c.NextRunAt) > 24*time.Hour {
			log.Printf("[Scheduler] Warning: Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
				c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
			nextRun, err := CalculateNextRun(c.CronExpr, c.Timezone, now)
			if err != nil {
				log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, err)
				nextRun = now.Add(24 * time.Hour)
			}
			if err := db.UpdateCronNextRun(database, c.ID, nextRun); err != nil {
				log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
			}
			continue
		}

		// Calculate and update next run time
		nextRun, err := CalculateNextRun(c.CronExpr, c.Timezone, now)
		if err != nil {
			log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, err)
			nextRun = now.Add(24 * time.Hour)
		}
		if err := db.UpdateCronNextRun(database, c.ID, nextRun); err != nil {
			log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
		}

		// Create fresh public Discord thread
		title := FormatThreadTitle(c.TitlePrefix, now)
		targetThreadID := c.TargetID
		if threadCreator != nil {
			thID, err := threadCreator.CreatePublicThread(c.TargetID, title)
			if err != nil {
				log.Printf("[Scheduler] Failed to create Discord thread %q in channel %s: %v. Fallback to channel ID.", title, c.TargetID, err)
			} else if thID != "" {
				targetThreadID = thID
				log.Printf("[Scheduler] Created Discord thread %q (ID: %s) for recurring cron %s", title, targetThreadID, c.ID)
			}
		}

		// Create and persist PENDING message
		msgID := uuid.New().String()
		msg := db.Message{
			ID:         msgID,
			ThreadID:   targetThreadID,
			GuildID:    "scheduled",
			AuthorID:   "scheduler",
			AuthorName: "Scheduler",
			Content:    c.Prompt,
			Status:     db.StatusPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		}

		if err := db.InsertMessage(database, msg); err != nil {
			log.Printf("[Scheduler] Error inserting recurring message %s for cron %s: %v", msgID, c.ID, err)
		}

		if enqueuer != nil {
			enqueuer.Enqueue(msg)
		}
		log.Printf("[Scheduler] Enqueued recurring turn for cron %s (message ID: %s, target thread: %s, next_run: %s)",
			c.ID, msgID, targetThreadID, nextRun.Format(time.RFC3339))
	}

	// 2. Process Due One-Shot Schedules (Atomic)
	dueOneShots, err := db.GetDueOneShotSchedules(database)
	if err != nil {
		return fmt.Errorf("error querying due one-shot schedules: %w", err)
	}

	for _, s := range dueOneShots {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgID := uuid.New().String()
		msg := db.Message{
			ID:         msgID,
			ThreadID:   s.ThreadID,
			GuildID:    "scheduled",
			AuthorID:   "scheduler",
			AuthorName: "Scheduler",
			Content:    s.Prompt,
			Status:     db.StatusPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		}

		if err := db.InsertMessageAndConsumeOneShot(database, s.ID, msg); err != nil {
			log.Printf("[Scheduler] Error atomically processing one-shot schedule %s (message %s): %v", s.ID, msgID, err)
			continue
		}

		if enqueuer != nil {
			enqueuer.Enqueue(msg)
		}

		log.Printf("[Scheduler] Enqueued one-shot reminder for schedule %s (message ID: %s, thread: %s)",
			s.ID, msgID, s.ThreadID)
	}

	return nil
}

// Start launches the background scheduler monitor daemon with a 30-second ticker.
// It returns a stop function that cleanly cancels the monitor and waits for Run to exit.
func Start(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, dg *discordgo.Session) (stop func()) {
	threadCreator := NewDiscordThreadCreator(dg)
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(subCtx, database, pool, threadCreator, 30*time.Second)
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

// Run executes the monitoring loop with ticker interval and context cancellation.
func Run(ctx context.Context, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator, interval time.Duration) {
	log.Printf("[Scheduler] Background scheduler monitor started (interval=%v)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial evaluation on start
	if err := ProcessDueSchedules(ctx, database, enqueuer, threadCreator); err != nil {
		log.Printf("[Scheduler] Error in initial schedule check: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("[Scheduler] Background scheduler monitor stopped cleanly")
			return
		case <-ticker.C:
			if err := ProcessDueSchedules(ctx, database, enqueuer, threadCreator); err != nil {
				log.Printf("[Scheduler] Error in schedule tick evaluation: %v", err)
			}
		}
	}
}
