package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
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
	cfg                *config.Config
	store              db.Store
	enqueuer           MessageEnqueuer
	threadCreator      ThreadCreator
	runnerFn           runner.RunnerFunc
	llmFn              runner.LLMFunc
	sessionRoots       []string
	brainDir           string
	transcriptEmbedder transcript.EmbedderFunc
	transcriptLLMFunc  transcript.LLMClientFunc
	wg                 sync.WaitGroup
}

// Option configures Scheduler options.
type Option func(*Scheduler)

// WithRunnerFunc sets the runner.RunnerFunc used by Scheduler for LLM fact extraction.
func WithRunnerFunc(fn runner.RunnerFunc) Option {
	return func(s *Scheduler) {
		s.runnerFn = fn
	}
}

// WithLLMFunc sets the runner.LLMFunc used by Scheduler for LLM fact extraction.
func WithLLMFunc(fn runner.LLMFunc) Option {
	return func(s *Scheduler) {
		s.llmFn = fn
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

// WithBrainDir sets the brain directory used for transcript synchronization.
func WithBrainDir(dir string) Option {
	return func(s *Scheduler) {
		s.brainDir = strings.TrimSpace(dir)
	}
}

// WithTranscriptEmbedder sets the embedder function used for transcript synchronization.
func WithTranscriptEmbedder(fn transcript.EmbedderFunc) Option {
	return func(s *Scheduler) {
		s.transcriptEmbedder = fn
	}
}

// WithTranscriptLLMFunc sets the LLM summarization function used for transcript synchronization.
func WithTranscriptLLMFunc(fn transcript.LLMClientFunc) Option {
	return func(s *Scheduler) {
		s.transcriptLLMFunc = fn
	}
}

// New constructs a Scheduler with a db.Store persistence contract.
func New(cfg *config.Config, store db.Store, enqueuer MessageEnqueuer, threadCreator ThreadCreator, opts ...Option) (*Scheduler, error) {
	if cfg == nil || cfg.Current() == nil {
		return nil, fmt.Errorf("scheduler: config cannot be nil")
	}
	s := &Scheduler{
		cfg:           cfg,
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

// ProcessDueSchedules evaluates and processes due cron and one-shot schedules.
func (s *Scheduler) ProcessDueSchedules(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	if s.cfg == nil || s.cfg.Current() == nil {
		return fmt.Errorf("scheduler: config cannot be nil")
	}

	now := time.Now().UTC()

	// 1. Process Due Cron Schedules
	dueCrons, err := s.store.GetDueCronSchedules(ctx)
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
			tz = s.cfg.Current().Timezone
		}

		var channelName string
		var isAlreadyThread bool
		if snap, ok := queue.GetCachedChannel(c.TargetID); ok {
			channelName = snap.Name
			if snap.IsThread {
				isAlreadyThread = true
			}
		}

		policy := s.cfg.ResolveChannelPolicy(c.TargetID, channelName)
		action := PlanDueCronTurn(c, now, isAlreadyThread, policy.Mode, s.threadCreator != nil, tz)

		if action.NextRunError != nil {
			log.Printf("[Scheduler] Failed to calculate next run for cron %s (%q): %v. Fallback 24h.", c.ID, c.CronExpr, action.NextRunError)
		}

		if action.IsStale {
			log.Printf("[Scheduler] Warning: Cron %s (%q) is stale (>24h overdue: due %s, now %s). Advancing next_run_at without firing.",
				c.ID, c.CronExpr, c.NextRunAt.Format(time.RFC3339), now.Format(time.RFC3339))
			if err := s.store.UpdateCronNextRun(ctx, c.ID, action.NextRunAt); err != nil {
				log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
			}
			continue
		}

		if err := s.store.UpdateCronNextRun(ctx, c.ID, action.NextRunAt); err != nil {
			log.Printf("[Scheduler] Failed to update next_run_at for cron %s: %v", c.ID, err)
		}

		targetThreadID := c.TargetID
		if action.ShouldCreateThread {
			thID, err := s.threadCreator.CreatePublicThread(c.TargetID, action.Title)
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
		if err := s.store.CreateScheduleRun(ctx, run); err != nil {
			log.Printf("[Scheduler] Error creating schedule run %s for cron %s: %v", runID, c.ID, err)
		}

		msg := BuildCronMessage(msgID, runID, targetThreadID, c, action.Summary, now)
		if err := s.store.InsertMessage(ctx, msg); err != nil {
			log.Printf("[Scheduler] Error inserting recurring message %s for cron %s: %v", msgID, c.ID, err)
		}

		if s.enqueuer != nil {
			s.enqueuer.Enqueue(msg)
		}
		metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()
		log.Printf("[Scheduler] Enqueued recurring turn for cron %s (message ID: %s, target thread: %s, next_run: %s)",
			c.ID, msgID, targetThreadID, action.NextRunAt.Format(time.RFC3339))
	}

	// 2. Process Due One-Shot Schedules (Atomic)
	dueOneShots, err := s.store.GetDueOneShotSchedules(ctx)
	if err != nil {
		return fmt.Errorf("error querying due one-shot schedules: %w", err)
	}

	for _, osSched := range dueOneShots {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runID := uuid.New().String()
		msgID := uuid.New().String()
		oneShotSummary := BuildOneShotMessageSummary(osSched.Prompt)
		msg := BuildOneShotMessage(msgID, runID, osSched, oneShotSummary, now)

		if err := s.store.InsertMessageAndConsumeOneShot(ctx, osSched.ID, msg); err != nil {
			log.Printf("[Scheduler] Error atomically processing one-shot schedule %s (message %s): %v", osSched.ID, msgID, err)
			continue
		}

		run := BuildOneShotScheduleRun(runID, msgID, osSched, now)
		if err := s.store.CreateScheduleRun(ctx, run); err != nil {
			log.Printf("[Scheduler] Error creating schedule run %s for one-shot %s: %v", runID, osSched.ID, err)
		}

		if s.enqueuer != nil {
			s.enqueuer.Enqueue(msg)
		}
		metrics.SchedulerExecutionsTotal.WithLabelValues("one_shot", "enqueued").Inc()

		log.Printf("[Scheduler] Enqueued one-shot reminder for schedule %s (message ID: %s, thread: %s)",
			osSched.ID, msgID, osSched.ThreadID)
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

// RunPruneRetentionWithContext executes schedule run retention cleanup with the provided context.
func RunPruneRetentionWithContext(ctx context.Context, store db.Store) {
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

var transcriptSyncMutex sync.Mutex

// RunTranscriptSync executes the background transcript synchronization routine.
func RunTranscriptSync(ctx context.Context, store db.Store, brainDir string, embedder transcript.EmbedderFunc, llmFunc transcript.LLMClientFunc) (transcript.SyncStats, error) {
	if store == nil {
		return transcript.SyncStats{}, nil
	}
	if !transcriptSyncMutex.TryLock() {
		log.Printf("[Scheduler] Transcript sync already in progress, skipping overlapping sweep.")
		return transcript.SyncStats{}, nil
	}
	defer transcriptSyncMutex.Unlock()

	syncCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	opts := transcript.DefaultSyncOptions()
	targetBrainDir := strings.TrimSpace(brainDir)
	if targetBrainDir != "" {
		opts.BrainDir = targetBrainDir
	}

	stats, err := transcript.SyncTranscripts(syncCtx, store, targetBrainDir, embedder, llmFunc, opts)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[Scheduler] Transcript sync error: %v", err)
		}
		return stats, err
	}
	if stats.Synced > 0 || stats.StepsInserted > 0 {
		log.Printf("[Scheduler] Transcript sync complete: %d sessions synced, %d steps inserted, %d summaries generated (%v)",
			stats.Synced, stats.StepsInserted, stats.SummariesGenerated, stats.Duration)
	}
	return stats, nil
}

func (s *Scheduler) spawnBg(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *Scheduler) runPruneRetention(ctx context.Context) {
	s.spawnBg(func() {
		RunPruneRetentionWithContext(ctx, s.store)
	})
}

func (s *Scheduler) runTranscriptSync(ctx context.Context) {
	s.spawnBg(func() {
		brainDir := s.brainDir
		if brainDir == "" && len(s.sessionRoots) > 0 {
			brainDir = s.sessionRoots[0]
		}
		if brainDir == "" {
			return
		}

		embedder := s.transcriptEmbedder
		if embedder == nil && s.cfg != nil {
			embedClient := transcript.NewEmbedder(s.cfg, s.sessionRoots...)
			embedder = func(eCtx context.Context, text string) ([]float32, error) {
				return embedClient.GenerateEmbedding(eCtx, text, false, 1)
			}
		}

		llmFunc := s.transcriptLLMFunc
		if llmFunc == nil && s.llmFn != nil && s.cfg != nil && s.cfg.Current() != nil {
			cur := s.cfg.Current()
			model := cur.LowEffortModel
			if model == "" {
				model = cur.Model
			}
			llmFunc = func(lCtx context.Context, prompt string) (string, error) {
				return s.llmFn(lCtx, model, prompt)
			}
		}
		if s.runnerFn != nil && llmFunc == nil {
			_ = s.runnerFn
		}

		for {
			stats, err := RunTranscriptSync(ctx, s.store, brainDir, embedder, llmFunc)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[Scheduler] Background transcript sync error: %v", err)
				}
				break
			}
			if stats.Synced < transcript.DefaultBatchLimit || ctx.Err() != nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	})
}

// handleTick executes periodic evaluations for a single ticker event.
func (s *Scheduler) handleTick(ctx context.Context, tickCount int) {
	if s == nil {
		return
	}
	if err := s.ProcessDueSchedules(ctx); err != nil {
		log.Printf("[Scheduler] Error in schedule tick evaluation: %v", err)
	}

	// Run transcript sync every 5 minutes (every 10 ticks at 30s interval = 5 minutes)
	if ShouldRunTranscriptSync(tickCount) {
		s.runTranscriptSync(ctx)
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

	// Initial evaluation on start
	if err := s.ProcessDueSchedules(ctx); err != nil {
		log.Printf("[Scheduler] Error in initial schedule check: %v", err)
	}
	s.runPruneRetention(ctx)
	s.runTranscriptSync(ctx)

	var tickCount int
	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			log.Println("[Scheduler] Background scheduler monitor stopped cleanly")
			return
		case <-ticker.C:
			tickCount++
			s.handleTick(ctx, tickCount)
		}
	}
}
