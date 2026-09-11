package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
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

type Scheduler struct {
	cfg           *config.Config
	db            *sql.DB
	store         db.Store
	enqueuer      MessageEnqueuer
	threadCreator ThreadCreator
	runnerFn      runner.RunnerFunc
	sessionRoots  []string
}

// Option configures Scheduler options.
type Option func(*Scheduler)

// WithRunnerFunc sets the runner.RunnerFunc used by Scheduler for LLM fact extraction.
func WithRunnerFunc(fn runner.RunnerFunc) Option {
	return func(s *Scheduler) {
		s.runnerFn = fn
	}
}

// WithSessionRoots sets the transcript search roots used by memory fact extraction.
func WithSessionRoots(roots ...string) Option {
	return func(s *Scheduler) {
		var clean []string
		for _, r := range roots {
			if tr := strings.TrimSpace(r); tr != "" {
				clean = append(clean, tr)
			}
		}
		s.sessionRoots = clean
	}
}

// New constructs a Scheduler supporting both legacy *sql.DB and db.Store interface parameters.
func New(cfg *config.Config, dbOrStore any, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) *Scheduler {
	var database *sql.DB
	var store db.Store
	switch v := dbOrStore.(type) {
	case db.Store:
		store = v
	case *sql.DB:
		database = v
		if v != nil {
			store = db.NewSQLStore(v)
		}
	}
	s := &Scheduler{
		cfg:           cfg,
		db:            database,
		store:         store,
		enqueuer:      enqueuer,
		threadCreator: threadCreator,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *Scheduler) getStore() db.Store {
	if s == nil {
		return nil
	}
	if s.store != nil {
		return s.store
	}
	if s.db != nil {
		return db.NewSQLStore(s.db)
	}
	return nil
}
func NewWithStore(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) *Scheduler {
	return New(cfg, store, enqueuer, threadCreator, opts...)
}

// NewScheduler is a compatibility wrapper for New.
func NewScheduler(cfg *config.Config, dbOrStore any, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) *Scheduler {
	return New(cfg, dbOrStore, enqueuer, threadCreator, opts...)
}

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

// GetDefaultTimezone returns the default timezone configured for the scheduler.
func GetDefaultTimezone() string {
	return config.GetTimezone()
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
func (s *Scheduler) ProcessDueSchedules(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	return processDueSchedulesStore(ctx, s.cfg, s.store, s.enqueuer, s.threadCreator)
}

// ProcessDueSchedules evaluates and processes due cron and one-shot schedules (compatibility wrapper).
func ProcessDueSchedules(ctx context.Context, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator) error {
	return New(nil, database, enqueuer, threadCreator).ProcessDueSchedules(ctx)
}

func processDueSchedulesStore(ctx context.Context, cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator) error {
	if store == nil {
		return nil
	}

	now := time.Now().UTC()

	// 1. Process Due Cron Schedules
	dueCrons, err := store.GetDueCronSchedules(ctx)
	if err != nil {
		return fmt.Errorf("error querying due cron schedules: %w", err)
	}

	for _, c := range dueCrons {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		defaultTz := ""
		if cfg != nil {
			if cur := cfg.Current(); cur != nil {
				defaultTz = cur.Timezone
			}
		} else {
			defaultTz = GetDefaultTimezone()
		}
		tz := c.Timezone
		if strings.TrimSpace(tz) == "" {
			tz = defaultTz
		}

		// 24h staleness guard: if cron trigger is overdue by >24h, advance next_run_at without firing.
		if now.Sub(c.NextRunAt) > 24*time.Hour {
			log.Printf("[Scheduler] Warning: Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
				c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
			nextRun, err := CalculateNextRun(c.CronExpr, tz, now)
			if err != nil {
				log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, err)
				nextRun = now.Add(24 * time.Hour)
			}
			if err := store.UpdateCronNextRun(ctx, c.ID, nextRun); err != nil {
				log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
			}
			continue
		}

		// Calculate and update next run time
		nextRun, err := CalculateNextRun(c.CronExpr, tz, now)
		if err != nil {
			log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, err)
			nextRun = now.Add(24 * time.Hour)
		}
		if err := store.UpdateCronNextRun(ctx, c.ID, nextRun); err != nil {
			log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
		}

		// Resolve channel policy and metadata for the target channel
		title := FormatThreadTitle(c.TitlePrefix, now)
		targetThreadID := c.TargetID

		var channelName string
		var isAlreadyThread bool
		if snap, ok := queue.GetCachedChannel(c.TargetID); ok {
			channelName = snap.Name
			if snap.IsThread {
				isAlreadyThread = true
			}
		}

		var policy config.ChannelPolicy
		if cfg != nil {
			if cur := cfg.Current(); cur != nil {
				policy = cur.ResolveChannelPolicy(c.TargetID, channelName)
			}
		} else {
			policy = config.GetRuntimeConfig().ResolveChannelPolicy(c.TargetID, channelName)
		}

		// Create fresh public Discord thread only if:
		// 1. Target is not already a thread, AND
		// 2. Channel policy mode is NOT "channel" (i.e. it is "threads" or default), AND
		// 3. Thread creator is provided.
		if !isAlreadyThread && policy.Mode != "channel" && threadCreator != nil {
			thID, err := threadCreator.CreatePublicThread(c.TargetID, title)
			if err != nil {
				log.Printf("[Scheduler] Failed to create Discord thread %q in channel %s: %v. Fallback to channel ID.", title, c.TargetID, err)
			} else if thID != "" {
				targetThreadID = thID
				log.Printf("[Scheduler] Created Discord thread %q (ID: %s) for recurring cron %s", title, targetThreadID, c.ID)
			}
		}

		// Create run record and message
		runID := uuid.New().String()
		msgID := uuid.New().String()

		run := db.ScheduleRun{
			ID:           runID,
			ScheduleID:   c.ID,
			ScheduleType: "cron",
			MessageID:    msgID,
			TargetID:     c.TargetID,
			ThreadID:     targetThreadID,
			Title:        title,
			Prompt:       c.Prompt,
			Status:       "enqueued",
			StartedAt:    now,
			Effort:       c.Effort,
		}
		if err := store.CreateScheduleRun(ctx, run); err != nil {
			log.Printf("[Scheduler] Error creating schedule run %s for cron %s: %v", runID, c.ID, err)
		}

		// Create and persist PENDING message
		cronSummary := db.CleanTaskSummary(c.Prompt)
		if c.TitlePrefix != "" {
			cronSummary = fmt.Sprintf("[%s] %s", c.TitlePrefix, cronSummary)
		}
		msg := db.Message{
			ID:            msgID,
			ThreadID:      targetThreadID,
			GuildID:       "scheduled",
			AuthorID:      "scheduler",
			AuthorName:    "Scheduler",
			Content:       c.Prompt,
			Summary:       cronSummary,
			Status:        db.StatusPending,
			ScheduleRunID: runID,
			CreatedAt:     now,
			UpdatedAt:     now,
			Effort:        c.Effort,
		}

		if err := store.InsertMessage(ctx, msg); err != nil {
			log.Printf("[Scheduler] Error inserting recurring message %s for cron %s: %v", msgID, c.ID, err)
		}

		if enqueuer != nil {
			enqueuer.Enqueue(msg)
		}
		metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()
		log.Printf("[Scheduler] Enqueued recurring turn for cron %s (message ID: %s, target thread: %s, next_run: %s)",
			c.ID, msgID, targetThreadID, nextRun.Format(time.RFC3339))
	}

	// 2. Process Due One-Shot Schedules (Atomic)
	dueOneShots, err := store.GetDueOneShotSchedules(ctx)
	if err != nil {
		return fmt.Errorf("error querying due one-shot schedules: %w", err)
	}

	for _, s := range dueOneShots {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runID := uuid.New().String()
		msgID := uuid.New().String()
		oneShotSummary := fmt.Sprintf("[Reminder] %s", db.CleanTaskSummary(s.Prompt))
		msg := db.Message{
			ID:            msgID,
			ThreadID:      s.ThreadID,
			GuildID:       "scheduled",
			AuthorID:      "scheduler",
			AuthorName:    "Scheduler",
			Content:       s.Prompt,
			Summary:       oneShotSummary,
			Status:        db.StatusPending,
			ScheduleRunID: runID,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		if err := store.InsertMessageAndConsumeOneShot(ctx, s.ID, msg); err != nil {
			log.Printf("[Scheduler] Error atomically processing one-shot schedule %s (message %s): %v", s.ID, msgID, err)
			continue
		}

		run := db.ScheduleRun{
			ID:           runID,
			ScheduleID:   s.ID,
			ScheduleType: "one_shot",
			MessageID:    msgID,
			TargetID:     s.ThreadID,
			ThreadID:     s.ThreadID,
			Title:        "One-shot Reminder",
			Prompt:       s.Prompt,
			Status:       "enqueued",
			StartedAt:    now,
		}
		if err := store.CreateScheduleRun(ctx, run); err != nil {
			log.Printf("[Scheduler] Error creating schedule run %s for one-shot %s: %v", runID, s.ID, err)
		}

		if enqueuer != nil {
			enqueuer.Enqueue(msg)
		}
		metrics.SchedulerExecutionsTotal.WithLabelValues("one_shot", "enqueued").Inc()

		log.Printf("[Scheduler] Enqueued one-shot reminder for schedule %s (message ID: %s, thread: %s)",
			s.ID, msgID, s.ThreadID)
	}

	return nil
}

// Start launches the background scheduler monitor daemon with a 30-second ticker.
func (s *Scheduler) Start(ctx context.Context) (stop func()) {
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(subCtx, 30*time.Second)
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

// Start launches the background scheduler monitor daemon with a 30-second ticker (compatibility wrapper).
func Start(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, dg *discordgo.Session) (stop func()) {
	threadCreator := NewDiscordThreadCreator(dg)
	return New(nil, database, pool, threadCreator).Start(ctx)
}

// ExtractFactsLLM extracts facts using the primary LLM via the configured runner function.
func (s *Scheduler) ExtractFactsLLM(ctx context.Context, prompt string) (string, error) {
	if s == nil || s.runnerFn == nil {
		return "", fmt.Errorf("scheduler: RunnerFunc not configured for fact extraction")
	}
	apiKey := ""
	model := ""
	agyBin := ""
	if s.cfg != nil {
		if cur := s.cfg.Current(); cur != nil {
			apiKey = cur.APIKey
			model = cur.LowEffortModel
			if model == "" {
				model = cur.Model
			}
			agyBin = cur.AgyBin
		}
	}
	if model == "" {
		rc := config.GetRuntimeConfig()
		model = rc.LowEffortModel
		if model == "" {
			model = rc.Model
		}
	}
	if strings.TrimSpace(model) == "" {
		return "", fmt.Errorf("scheduler fact extraction error: model is not configured")
	}
	if agyBin == "" {
		agyBin = "agy"
	}
	stdout, _, exitCode, err := s.runnerFn(ctx, agyBin, prompt, "", apiKey, model, 5)
	if exitCode != 0 || err != nil {
		return "", fmt.Errorf("agy fact extraction exitCode=%d err=%v", exitCode, err)
	}
	resp, parseErr := runner.ParseAgyOutput(stdout)
	if parseErr != nil {
		return "", fmt.Errorf("failed to parse agy json output in scheduler: %w (raw: %q)", parseErr, stdout)
	}
	return resp.Response, nil
}

// ExtractFactsLLM extracts facts using the primary LLM (compatibility wrapper).
// Returns an error if no scheduler with a configured RunnerFunc is available.
func ExtractFactsLLM(ctx context.Context, prompt string) (string, error) {
	return New(nil, nil, nil, nil).ExtractFactsLLM(ctx, prompt)
}

// RunPruneRetention executes schedule run retention cleanup.
func RunPruneRetention(dbOrStore any) {
	var store db.Store
	switch v := dbOrStore.(type) {
	case db.Store:
		store = v
	case *sql.DB:
		if v != nil {
			store = db.NewSQLStore(v)
		}
	}
	if store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if pruned, err := store.PruneScheduleRuns(ctx, 1000, 30*24*time.Hour); err != nil {
		log.Printf("[Scheduler] Retention pruning error: %v", err)
	} else if pruned > 0 {
		log.Printf("[Scheduler] Retention pruning removed %d old schedule runs", pruned)
	}
}

// RunFactExtraction triggers embedding backfill and active conversation fact extraction.
func RunFactExtraction(ctx context.Context, dbOrStore any, client *memory.Client, llmFunc memory.LLMClientFunc) {
	if dbOrStore == nil || client == nil || llmFunc == nil {
		return
	}
	if backfilled, err := memory.BackfillMissingEmbeddings(ctx, dbOrStore, client); err != nil {
		log.Printf("[Scheduler] Embedding backfill error: %v", err)
	} else if backfilled > 0 {
		log.Printf("[Scheduler] Embedding backfill completed for %d facts", backfilled)
	}
	if err := memory.ExtractActiveConversationFacts(ctx, dbOrStore, client, llmFunc, 12); err != nil {
		log.Printf("[Scheduler] Fact extraction error: %v", err)
	}
}

// Run executes the monitoring loop with ticker interval and context cancellation.
func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	log.Printf("[Scheduler] Background scheduler monitor started (interval=%v)", interval)
	ticker := time.NewTicker(interval)
	var ollamaClient *memory.Client
	if s.cfg != nil {
		ollamaClient = memory.New(s.cfg, s.sessionRoots...)
	} else {
		ollamaClient = memory.NewClient("", s.sessionRoots...)
	}
	llmFunc := s.ExtractFactsLLM

	// Initial evaluation on start
	if err := s.ProcessDueSchedules(ctx); err != nil {
		log.Printf("[Scheduler] Error in initial schedule check: %v", err)
	}
	go RunPruneRetention(s.getStore())
	go RunFactExtraction(ctx, s.getStore(), ollamaClient, llmFunc)

	var tickCount int
	for {
		select {
		case <-ctx.Done():
			log.Println("[Scheduler] Background scheduler monitor stopped cleanly")
			return
		case <-ticker.C:
			tickCount++
			if err := s.ProcessDueSchedules(ctx); err != nil {
				log.Printf("[Scheduler] Error in schedule tick evaluation: %v", err)
			}

			// Run fact extraction hourly (every 120 ticks at 30s interval = 1 hour)
			if tickCount%120 == 0 {
				go RunFactExtraction(ctx, s.getStore(), ollamaClient, llmFunc)
			}

			// Run retention pruning daily (every 2880 ticks at 30s interval = 24 hours)
			if tickCount%2880 == 0 {
				go RunPruneRetention(s.getStore())
			}
		}
	}
}

// Run executes the monitoring loop with ticker interval and context cancellation (compatibility wrapper).
func Run(ctx context.Context, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator, interval time.Duration) {
	New(nil, database, enqueuer, threadCreator).Run(ctx, interval)
}
