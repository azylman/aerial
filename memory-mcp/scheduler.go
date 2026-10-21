package main

import (
	"context"
	"errors"
	"log"
	"time"
)

// DefaultWorkerInitialDelay is the default initial delay before the worker ticks.
const DefaultWorkerInitialDelay = 30 * time.Second

// DefaultWorkerTickInterval is the default tick interval.
const DefaultWorkerTickInterval = 30 * time.Second

// WorkerConfig defines the parameters for the background memory maintenance worker.
type WorkerConfig struct {
	Store        Store
	Ollama       OllamaClient
	LLMClient    LLMClientFunc
	InitialDelay time.Duration
	TickInterval time.Duration
}

// Worker coordinates periodic embedding backfills, fact extraction, and importance decay/pruning.
type Worker struct {
	store        Store
	ollama       OllamaClient
	llmClient    LLMClientFunc
	initialDelay time.Duration
	tickInterval time.Duration
	tickCount    int64
}

// NewWorker initializes a new background Worker instance.
func NewWorker(cfg WorkerConfig) *Worker {
	initDelay := cfg.InitialDelay
	tickInt := cfg.TickInterval
	if tickInt <= 0 {
		tickInt = DefaultWorkerTickInterval
	}
	return &Worker{
		store:        cfg.Store,
		ollama:       cfg.Ollama,
		llmClient:    cfg.LLMClient,
		initialDelay: initDelay,
		tickInterval: tickInt,
	}
}

// Start begins the ticker loop, running until ctx is canceled.
func (w *Worker) Start(ctx context.Context) error {
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
func (w *Worker) Tick(ctx context.Context, tick int64) {
	// Hourly: fact extraction + backfill (tick % 120 == 0)
	if tick%120 == 0 {
		w.runHourly(ctx)
	}
	// Daily: decay and pruning (tick % 2880 == 60)
	if tick%2880 == 60 {
		w.runDaily(ctx)
	}
}

func (w *Worker) runHourly(ctx context.Context) {
	if w.store == nil || w.ollama == nil {
		return
	}
	// 1. Backfill missing embeddings
	if _, err := BackfillMissingEmbeddings(ctx, w.store, w.ollama); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[Memory Worker] Backfill missing embeddings failed: %v", err)
	}

	// 2. Fact extraction
	if w.llmClient != nil {
		if err := ExtractActiveConversationFacts(ctx, w.store, w.ollama, w.llmClient, 24); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Memory Worker] Fact extraction failed: %v", err)
		}
	}
}

func (w *Worker) runDaily(ctx context.Context) {
	if w.store == nil {
		return
	}
	decayed, pruned, err := w.store.DecayAndPruneFacts(ctx, 0.05, 0.15, 14)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[Memory Worker] Decay and prune failed: %v", err)
		return
	}
	log.Printf("[Memory Worker] Decay and prune completed: decayed=%d, pruned=%d", decayed, pruned)

	if w.ollama != nil && w.llmClient != nil {
		consolidated, err := ConsolidateFactClusters(ctx, w.store, w.ollama, w.llmClient)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Memory Worker] Fact consolidation dreaming failed: %v", err)
		} else {
			log.Printf("[Memory Worker] Fact consolidation dreaming completed: %d clusters consolidated", consolidated)
		}
	}
}
