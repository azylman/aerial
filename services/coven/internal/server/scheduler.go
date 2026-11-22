package server

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// DefaultWorkerInitialDelay is the default initial delay before the worker ticks.
const DefaultWorkerInitialDelay = 30 * time.Second

// DefaultWorkerTickInterval is the default tick interval.
const DefaultWorkerTickInterval = 30 * time.Second

// WorkerConfig defines the parameters for the background memory maintenance worker.
type WorkerConfig struct {
	Store           store.Store
	Ollama          store.OllamaClient
	LLMClient       LLMClientFunc
	Publisher       SkillPublisher
	CustomSkillsDir string
	InitialDelay    time.Duration
	TickInterval    time.Duration
	Role            string
	Stack           string
	ExtractTimeout  time.Duration
}

// Worker coordinates periodic embedding backfills, fact extraction, and importance decay/pruning.
type Worker struct {
	store           store.Store
	ollama          store.OllamaClient
	llmClient       LLMClientFunc
	publisher       SkillPublisher
	customSkillsDir string
	initialDelay    time.Duration
	tickInterval    time.Duration
	tickCount       int64
	role            string
	stack           string
	extractTimeout  time.Duration
}

// NewWorker initializes a new background Worker instance.
func NewWorker(cfg WorkerConfig) *Worker {
	initDelay := cfg.InitialDelay
	tickInt := cfg.TickInterval
	if tickInt <= 0 {
		tickInt = DefaultWorkerTickInterval
	}
	customDir := cfg.CustomSkillsDir
	if customDir == "" {
		customDir = config.DefaultCustomSkillsDir
	}
	extractTimeout := cfg.ExtractTimeout
	if extractTimeout <= 0 {
		extractTimeout = config.DefaultExtractTimeout
	}
	return &Worker{
		store:           cfg.Store,
		ollama:          cfg.Ollama,
		llmClient:       cfg.LLMClient,
		publisher:       cfg.Publisher,
		customSkillsDir: customDir,
		initialDelay:    initDelay,
		tickInterval:    tickInt,
		tickCount:       1,
		role:            strings.TrimSpace(cfg.Role),
		stack:           config.NormalizeStack(cfg.Stack),
		extractTimeout:  extractTimeout,
	}
}

// Stack returns the configured stack namespace, defaulting to config.DefaultStack ("aerial").
func (w *Worker) Stack() string {
	if w != nil && w.stack != "" {
		return w.stack
	}
	return config.DefaultStack
}

// Role returns the configured role for this worker.
func (w *Worker) Role() string {
	if w != nil {
		return w.role
	}
	return ""
}

// ExtractThread extracts facts for a single thread within the configured extraction timeout.
func (w *Worker) ExtractThread(ctx context.Context, threadID string, agent ...string) error {
	if w == nil {
		return errors.New("coven worker is nil")
	}
	timeout := w.extractTimeout
	if timeout <= 0 {
		timeout = config.DefaultExtractTimeout
	}
	extractCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return ExtractThreadFacts(extractCtx, w.store, w.ollama, w.llmClient, threadID, agent...)
}

// Start begins the maintenance worker, executing the periodic maintenance loop
// (dreaming/pruning), running until ctx is canceled.
func (w *Worker) Start(ctx context.Context) error {
	if w != nil && strings.EqualFold(w.role, "extractor") {
		<-ctx.Done()
		return ctx.Err()
	}

	if w.initialDelay > 0 {
		timer := time.NewTimer(w.initialDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()

	for {
		w.Tick(ctx, w.tickCount)
		w.tickCount++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Tick performs scheduled maintenance operations for the given tick count.
// Invariant: Turn event ingestion is event-driven via JetStream durable consumers
// (coven-transcripts-indexed -> coven-extract) and pubsub auto-reconnect (nats.MaxReconnects(-1));
// legacy 120-tick DB fallback polling is retired.
func (w *Worker) Tick(ctx context.Context, tick int64) {
	// Daily: fact consolidation dreaming (tick % 2880 == 60)
	if tick%2880 == 60 {
		w.runDaily(ctx)
	}
}

func (w *Worker) shouldRunDailyDreaming(ctx context.Context) bool {
	_, lastRunAt, err := w.store.GetConversationFactWatermark(ctx, DreamingWatermarkThreadID)
	if err != nil || lastRunAt == nil || lastRunAt.IsZero() {
		return true
	}
	return time.Since(*lastRunAt) >= DefaultDreamingMinInterval
}

func (w *Worker) runDaily(ctx context.Context) {
	if w.store == nil {
		return
	}
	if !w.shouldRunDailyDreaming(ctx) {
		log.Printf("[Memory Worker] Skipping daily dreaming: already completed within the last %s", DefaultDreamingMinInterval)
		return
	}
	if w.ollama != nil && w.llmClient != nil {
		spanCtx, span := tracing.Tracer().Start(ctx, "coven.consolidate", trace.WithNewRoot())
		consolidated, err := ConsolidateFactClusters(spanCtx, w.store, w.ollama, w.llmClient)
		endSchedulerSpan(span, err)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Memory Worker] Fact consolidation dreaming failed: %v", err)
		} else {
			log.Printf("[Memory Worker] Fact consolidation dreaming completed: %d clusters consolidated", consolidated)
		}
	}
	if w.publisher != nil && w.llmClient != nil {
		spanCtx, span := tracing.Tracer().Start(ctx, "coven.consolidate_skills", trace.WithNewRoot())
		prNum, err := ConsolidateProceduralSkills(spanCtx, w.store, w.ollama, w.llmClient, w.publisher, w.customSkillsDir)
		endSchedulerSpan(span, err)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Memory Worker] Procedural skill consolidation failed: %v", err)
		} else if prNum > 0 {
			log.Printf("[Memory Worker] Procedural skill consolidation opened PR #%d", prNum)
		}
	}
}

// endSchedulerSpan closes a scheduler root span, marking it Error when the run failed.
func endSchedulerSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "scheduled run failed")
	}
	span.End()
}
