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
	wg            sync.WaitGroup
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
func New(cfg *config.Config, dbOrStore any, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error) {
	if cfg == nil || cfg.Current() == nil {
		return nil, fmt.Errorf("scheduler: config cannot be nil")
	}
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
	return s, nil
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
func NewWithStore(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error) {
	return New(cfg, store, enqueuer, threadCreator, opts...)
}

// NewScheduler is a compatibility wrapper for New.
func NewScheduler(cfg *config.Config, dbOrStore any, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error) {
	return New(cfg, dbOrStore, enqueuer, threadCreator, opts...)
}

// ProcessDueSchedules evaluates and processes due cron and one-shot schedules.
func (s *Scheduler) ProcessDueSchedules(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	if s.cfg == nil || s.cfg.Current() == nil {
		return fmt.Errorf("scheduler: config cannot be nil")
	}
	return processDueSchedulesStore(ctx, s.cfg, s.store, s.enqueuer, s.threadCreator)
}

// ProcessDueSchedules evaluates and processes due cron and one-shot schedules (compatibility wrapper).
func ProcessDueSchedules(ctx context.Context, cfg *config.Config, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator) error {
	s, err := New(cfg, database, enqueuer, threadCreator)
	if err != nil {
		return err
	}
	return s.ProcessDueSchedules(ctx)
}

func processDueSchedulesStore(ctx context.Context, cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator) error {
	if store == nil {
		return nil
	}
	if cfg == nil || cfg.Current() == nil {
		return fmt.Errorf("scheduler: config cannot be nil")
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

		tz := strings.TrimSpace(c.Timezone)
		if tz == "" {
			tz = cfg.Current().Timezone
		}

		var channelName string
		var isAlreadyThread bool
		if snap, ok := queue.GetCachedChannel(c.TargetID); ok {
			channelName = snap.Name
			if snap.IsThread {
				isAlreadyThread = true
			}
		}

		policy := cfg.Current().ResolveChannelPolicy(c.TargetID, channelName)
		action := PlanDueCronTurn(c, now, isAlreadyThread, policy.Mode, threadCreator != nil, tz)

		if action.NextRunError != nil {
			log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, action.NextRunError)
		}

		if action.IsStale {
			log.Printf("[Scheduler] Warning: Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
				c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
			if err := store.UpdateCronNextRun(ctx, c.ID, action.NextRunAt); err != nil {
				log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
			}
			continue
		}

		if err := store.UpdateCronNextRun(ctx, c.ID, action.NextRunAt); err != nil {
			log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
		}

		targetThreadID := c.TargetID
		if action.ShouldCreateThread {
			thID, err := threadCreator.CreatePublicThread(c.TargetID, action.Title)
			if err != nil {
				log.Printf("[Scheduler] Failed to create Discord thread %q in channel %s: %v. Fallback to channel ID.", action.Title, c.TargetID, err)
			} else if thID != "" {
				targetThreadID = thID
				log.Printf("[Scheduler] Created Discord thread %q (ID: %s) for recurring cron %s", action.Title, targetThreadID, c.ID)
			}
		}

		runID := uuid.New().String()
		msgID := uuid.New().String()

		run := BuildCronScheduleRun(runID, msgID, targetThreadID, c, action.Title, now)
		if err := store.CreateScheduleRun(ctx, run); err != nil {
			log.Printf("[Scheduler] Error creating schedule run %s for cron %s: %v", runID, c.ID, err)
		}

		msg := BuildCronMessage(msgID, runID, targetThreadID, c, action.Summary, now)
		if err := store.InsertMessage(ctx, msg); err != nil {
			log.Printf("[Scheduler] Error inserting recurring message %s for cron %s: %v", msgID, c.ID, err)
		}

		if enqueuer != nil {
			enqueuer.Enqueue(msg)
		}
		metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()
		log.Printf("[Scheduler] Enqueued recurring turn for cron %s (message ID: %s, target thread: %s, next_run: %s)",
			c.ID, msgID, targetThreadID, action.NextRunAt.Format(time.RFC3339))
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
		oneShotSummary := BuildOneShotMessageSummary(s.Prompt)
		msg := BuildOneShotMessage(msgID, runID, s, oneShotSummary, now)

		if err := store.InsertMessageAndConsumeOneShot(ctx, s.ID, msg); err != nil {
			log.Printf("[Scheduler] Error atomically processing one-shot schedule %s (message %s): %v", s.ID, msgID, err)
			continue
		}

		run := BuildOneShotScheduleRun(runID, msgID, s, now)
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
func Start(ctx context.Context, cfg *config.Config, database *sql.DB, pool *queue.WorkerPool, dg *discordgo.Session) (stop func()) {
	threadCreator := NewDiscordThreadCreator(dg)
	s, err := New(cfg, database, pool, threadCreator)
	if err != nil {
		log.Printf("[Scheduler] Error: %v", err)
		return func() {}
	}
	return s.Start(ctx)
}

// ExtractFactsLLM extracts facts using the primary LLM via the configured runner function.
func (s *Scheduler) ExtractFactsLLM(ctx context.Context, prompt string) (string, error) {
	if s == nil || s.runnerFn == nil {
		return "", fmt.Errorf("scheduler: RunnerFunc not configured for fact extraction")
	}
	if s.cfg == nil || s.cfg.Current() == nil {
		return "", fmt.Errorf("scheduler: config cannot be nil")
	}
	cur := s.cfg.Current()
	apiKey := cur.APIKey
	model := cur.LowEffortModel
	if model == "" {
		model = cur.Model
	}
	agyBin := cur.AgyBin
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
	return "", fmt.Errorf("scheduler: config cannot be nil")
}

// RunPruneRetention executes schedule run retention cleanup.
func RunPruneRetention(dbOrStore any) {
	RunPruneRetentionWithContext(context.Background(), dbOrStore)
}

// RunPruneRetentionWithContext executes schedule run retention cleanup with the provided context.
func RunPruneRetentionWithContext(ctx context.Context, dbOrStore any) {
	if dbOrStore == nil {
		return
	}
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
	pruneCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if pruned, err := store.PruneScheduleRuns(pruneCtx, 1000, 30*24*time.Hour); err != nil {
		if ctx.Err() == nil {
			log.Printf("[Scheduler] Retention pruning error: %v", err)
		}
	} else if pruned > 0 {
		log.Printf("[Scheduler] Retention pruning removed %d old schedule runs", pruned)
	}
}

// RunFactExtraction triggers embedding backfill and active conversation fact extraction.
func RunFactExtraction(ctx context.Context, dbOrStore any, client *memory.Client, llmFunc memory.LLMClientFunc) {
	if dbOrStore == nil || client == nil || llmFunc == nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	if backfilled, err := memory.BackfillMissingEmbeddings(ctx, dbOrStore, client); err != nil {
		if ctx.Err() == nil {
			log.Printf("[Scheduler] Embedding backfill error: %v", err)
		}
	} else if backfilled > 0 {
		log.Printf("[Scheduler] Embedding backfill completed for %d facts", backfilled)
	}
	if ctx.Err() != nil {
		return
	}
	if err := memory.ExtractActiveConversationFacts(ctx, dbOrStore, client, llmFunc, 12); err != nil {
		if ctx.Err() == nil {
			log.Printf("[Scheduler] Fact extraction error: %v", err)
		}
	}
}

func (s *Scheduler) runPruneRetention(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		RunPruneRetentionWithContext(ctx, s.getStore())
	}()
}

func (s *Scheduler) runFactExtraction(ctx context.Context, ollamaClient *memory.Client, llmFunc memory.LLMClientFunc) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		RunFactExtraction(ctx, s.getStore(), ollamaClient, llmFunc)
	}()
}

// handleTick executes periodic evaluations for a single ticker event.
func (s *Scheduler) handleTick(ctx context.Context, tickCount int, ollamaClient *memory.Client, llmFunc memory.LLMClientFunc) {
	if s == nil {
		return
	}
	if err := s.ProcessDueSchedules(ctx); err != nil {
		log.Printf("[Scheduler] Error in schedule tick evaluation: %v", err)
	}

	// Run fact extraction hourly (every 120 ticks at 30s interval = 1 hour)
	if ShouldRunFactExtraction(tickCount) {
		s.runFactExtraction(ctx, ollamaClient, llmFunc)
	}

	// Run retention pruning daily (every 2880 ticks at 30s interval = 24 hours)
	if ShouldRunPruneRetention(tickCount) {
		s.runPruneRetention(ctx)
	}
}

// Run executes the monitoring loop with ticker interval and context cancellation.
func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	if s == nil || s.cfg == nil || s.cfg.Current() == nil {
		log.Printf("[Scheduler] Error: cannot run scheduler with nil config")
		return
	}
	log.Printf("[Scheduler] Background scheduler monitor started (interval=%v)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ollamaClient := memory.New(s.cfg, s.sessionRoots...)
	llmFunc := s.ExtractFactsLLM

	// Initial evaluation on start
	if err := s.ProcessDueSchedules(ctx); err != nil {
		log.Printf("[Scheduler] Error in initial schedule check: %v", err)
	}
	s.runPruneRetention(ctx)
	s.runFactExtraction(ctx, ollamaClient, llmFunc)

	var tickCount int
	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			log.Println("[Scheduler] Background scheduler monitor stopped cleanly")
			return
		case <-ticker.C:
			tickCount++
			s.handleTick(ctx, tickCount, ollamaClient, llmFunc)
		}
	}
}

// Run executes the monitoring loop with ticker interval and context cancellation (compatibility wrapper).
func Run(ctx context.Context, cfg *config.Config, database *sql.DB, enqueuer MessageEnqueuer, threadCreator ThreadCreator, interval time.Duration) {
	s, err := New(cfg, database, enqueuer, threadCreator)
	if err != nil {
		log.Printf("[Scheduler] Error: %v", err)
		return
	}
	s.Run(ctx, interval)
}
