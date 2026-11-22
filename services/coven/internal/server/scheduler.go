package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// DefaultWorkerInitialDelay is the default initial delay before the worker ticks.
const DefaultWorkerInitialDelay = 30 * time.Second

// DefaultWorkerTickInterval is the default tick interval.
const DefaultWorkerTickInterval = 30 * time.Second

// DefaultDreamingRetryInterval is the shorter retry delay scheduled when daily dreaming fails.
const DefaultDreamingRetryInterval = 15 * time.Minute

// WorkerConfig defines the parameters for the background memory maintenance worker.
type WorkerConfig struct {
	Store             store.Store
	Ollama            store.OllamaClient
	LLMClient         LLMClientFunc
	Publisher         SkillPublisher
	CustomSkillsDir   string
	InitialDelay      time.Duration
	TickInterval      time.Duration
	Role              string
	Stack             string
	ExtractTimeout    time.Duration
	RestateIngressURL string
	HTTPClient        *http.Client
	ArmRetryInterval  time.Duration
}

// Worker coordinates periodic embedding backfills, fact extraction, and importance decay/pruning.
type Worker struct {
	store             store.Store
	ollama            store.OllamaClient
	llmClient         LLMClientFunc
	publisher         SkillPublisher
	customSkillsDir   string
	initialDelay      time.Duration
	tickInterval      time.Duration
	tickCount         int64
	role              string
	stack             string
	extractTimeout    time.Duration
	restateIngressURL string
	httpClient        *http.Client
	armRetryInterval  time.Duration
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
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Worker{
		store:             cfg.Store,
		ollama:            cfg.Ollama,
		llmClient:         cfg.LLMClient,
		publisher:         cfg.Publisher,
		customSkillsDir:   customDir,
		initialDelay:      initDelay,
		tickInterval:      tickInt,
		tickCount:         1,
		role:              strings.TrimSpace(cfg.Role),
		stack:             config.NormalizeStack(cfg.Stack),
		extractTimeout:    extractTimeout,
		restateIngressURL: strings.TrimRight(strings.TrimSpace(cfg.RestateIngressURL), "/"),
		httpClient:        httpClient,
		armRetryInterval:  cfg.ArmRetryInterval,
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

// RestateIngressURL returns the configured Restate Ingress URL.
func (w *Worker) RestateIngressURL() string {
	if w != nil {
		return w.restateIngressURL
	}
	return ""
}

// HTTPClient returns the configured HTTP client or http.DefaultClient.
func (w *Worker) HTTPClient() *http.Client {
	if w != nil && w.httpClient != nil {
		return w.httpClient
	}
	return http.DefaultClient
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

func (w *Worker) armDreamer(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/CovenDreamer/%s/Arm", w.restateIngressURL, url.PathEscape(w.Stack()))
	idempotencyKey := "coven:dreamer:arm:" + w.Stack()
	backoff := w.armRetryInterval
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := 30 * time.Second
	if w.armRetryInterval > 0 && 4*w.armRetryInterval < maxBackoff {
		maxBackoff = 4 * w.armRetryInterval
	}
	for {
		if _, err := restateingress.Post(ctx, w.httpClient, endpoint, idempotencyKey, struct{}{}, 0); err != nil {
			log.Printf("[coven-dreamer] warning arming dreamer: %v; will retry in %s", err, backoff)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}
		return nil
	}
}

// Start begins the maintenance worker, arming the CovenDreamer Virtual Object when Restate
// is configured, or running an in-process ticker loop as fallback when Restate is disabled.
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

	if w != nil && w.restateIngressURL != "" {
		if err := w.armDreamer(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}

	if w != nil {
		log.Printf("[coven-dreamer] warning: RestateIngressURL is empty; falling back to in-process ticker loop for dreaming")
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

	<-ctx.Done()
	return ctx.Err()
}

// Tick performs scheduled maintenance operations for the given tick count.
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
	if span == nil {
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, "scheduled run failed")
	}
	span.End()
}

// ExtractJobRequest defines the request payload for extracting facts for a thread.
type ExtractJobRequest struct {
	ThreadID string `json:"thread_id"`
	Agent    string `json:"agent,omitempty"`
}

// CovenThread is a Restate Virtual Object executing fact extraction serially per thread.
type CovenThread struct {
	worker *Worker
}

// NewCovenThread creates a CovenThread Virtual Object handler.
func NewCovenThread(worker *Worker) *CovenThread {
	return &CovenThread{worker: worker}
}

// Extract extracts thread facts inside a durable Restate step.
func (c *CovenThread) Extract(ctx restate.ObjectContext, req ExtractJobRequest) error {
	key := restate.Key(ctx)
	agent, threadID := config.ParseScopedThreadKey(key)
	if req.ThreadID != "" {
		threadID = req.ThreadID
	}
	if req.Agent != "" {
		agent = req.Agent
	}
	if threadID == "" {
		return nil
	}

	_, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
		if c.worker == nil {
			return struct{}{}, restate.ToTerminalError(errors.New("coven worker is nil"))
		}
		if err := c.worker.ExtractThread(rc, threadID, agent); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}, restate.WithName("extract-thread-facts"))
	if err != nil {
		return err
	}
	return nil
}

// CovenDreamer is a Restate Virtual Object executing daily fact & skill consolidation.
type CovenDreamer struct {
	worker *Worker
}

// NewCovenDreamer creates a CovenDreamer Virtual Object handler.
func NewCovenDreamer(worker *Worker) *CovenDreamer {
	return &CovenDreamer{worker: worker}
}

// Arm computes delay until the next daily dream and schedules a delayed Dream invocation.
func (d *CovenDreamer) Arm(ctx restate.ObjectContext, _ struct{}) error {
	stack := restate.Key(ctx)
	if stack == "" {
		stack = config.DefaultStack
	}

	armed, err := restate.Get[bool](ctx, "armed")
	if err != nil {
		return err
	}
	if armed {
		return nil
	}

	delay, err := restate.Run(ctx, func(rc restate.RunContext) (time.Duration, error) {
		if d.worker == nil || d.worker.store == nil {
			return DefaultDreamingMinInterval, nil
		}
		_, lastRunAt, err := d.worker.store.GetConversationFactWatermark(rc, DreamingWatermarkThreadID)
		if err != nil {
			return 0, err
		}
		if lastRunAt == nil || lastRunAt.IsZero() {
			return 0, nil
		}
		elapsed := time.Since(*lastRunAt)
		if elapsed >= DefaultDreamingMinInterval {
			return 0, nil
		}
		return DefaultDreamingMinInterval - elapsed, nil
	}, restate.WithName("compute-delay"))
	if err != nil {
		return err
	}

	restate.Set(ctx, "armed", true)
	if delay > 0 {
		restate.ObjectSend(ctx, "CovenDreamer", stack, "Dream").Send(struct{}{}, restate.WithDelay(delay))
	} else {
		restate.ObjectSend(ctx, "CovenDreamer", stack, "Dream").Send(struct{}{})
	}
	return nil
}

// Dream executes fact and skill consolidation and self-arms the next cycle.
func (d *CovenDreamer) Dream(ctx restate.ObjectContext, _ struct{}) error {
	if d == nil {
		return errors.New("coven dreamer is nil")
	}

	stack := restate.Key(ctx)
	if stack == "" {
		stack = config.DefaultStack
	}

	restate.Set(ctx, "armed", true)

	var runErr error
	_, err := restate.Run(ctx, func(rc restate.RunContext) (int, error) {
		if d.worker == nil || d.worker.store == nil || d.worker.ollama == nil || d.worker.llmClient == nil {
			return 0, nil
		}
		spanCtx, span := tracing.Tracer().Start(rc, "coven.consolidate", trace.WithNewRoot())
		defer span.End()
		consolidated, err := ConsolidateFactClusters(spanCtx, d.worker.store, d.worker.ollama, d.worker.llmClient)
		if err != nil {
			span.SetStatus(codes.Error, "scheduled run failed")
			if !errors.Is(err, context.Canceled) {
				log.Printf("[Memory Worker] Fact consolidation dreaming failed: %v", err)
			}
			return 0, err
		}
		log.Printf("[Memory Worker] Fact consolidation dreaming completed: %d clusters consolidated", consolidated)
		return consolidated, nil
	}, restate.WithName("consolidate-facts"), restate.WithMaxRetryAttempts(2))
	if err != nil {
		runErr = err
	}

	_, err = restate.Run(ctx, func(rc restate.RunContext) (int, error) {
		if d.worker == nil || d.worker.store == nil || d.worker.publisher == nil || d.worker.llmClient == nil {
			return 0, nil
		}
		spanCtx, span := tracing.Tracer().Start(rc, "coven.consolidate_skills", trace.WithNewRoot())
		defer span.End()
		prNum, err := ConsolidateProceduralSkills(spanCtx, d.worker.store, d.worker.ollama, d.worker.llmClient, d.worker.publisher, d.worker.customSkillsDir)
		if err != nil {
			span.SetStatus(codes.Error, "scheduled run failed")
			if !errors.Is(err, context.Canceled) {
				log.Printf("[Memory Worker] Procedural skill consolidation failed: %v", err)
			}
			return 0, err
		}
		if prNum > 0 {
			log.Printf("[Memory Worker] Procedural skill consolidation opened PR #%d", prNum)
		}
		return prNum, nil
	}, restate.WithName("consolidate-skills"), restate.WithMaxRetryAttempts(2))
	if err != nil && runErr == nil {
		runErr = err
	}

	nextDelay := DefaultDreamingMinInterval
	if runErr != nil {
		nextDelay = DefaultDreamingRetryInterval
		log.Printf("[coven-dreamer] consolidation error occurred; scheduling retry in %s: %v", nextDelay, runErr)
	}
	restate.ObjectSend(ctx, "CovenDreamer", stack, "Dream").Send(struct{}{}, restate.WithDelay(nextDelay))
	return nil
}
