package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/attribute"
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
	Store               store.Store
	Ollama              store.OllamaClient
	LLMClient           LLMClientFunc
	Publisher           SkillPublisher
	CustomSkillsDir     string
	InitialDelay        time.Duration
	TickInterval        time.Duration
	Role                string
	Stack               string
	ExtractTimeout      time.Duration
	RestateIngressURL   string
	RestateAdminURL     string
	RestateAdvertiseURL string
	HTTPClient          *http.Client
	ArmRetryInterval    time.Duration
}

// Worker coordinates periodic embedding backfills, fact extraction, and importance decay/pruning.
type Worker struct {
	store               store.Store
	ollama              store.OllamaClient
	llmClient           LLMClientFunc
	publisher           SkillPublisher
	customSkillsDir     string
	initialDelay        time.Duration
	tickInterval        time.Duration
	tickCount           int64
	role                string
	stack               string
	extractTimeout      time.Duration
	restateIngressURL   string
	restateAdminURL     string
	restateAdvertiseURL string
	httpClient          *http.Client
	armRetryInterval    time.Duration
	fallbackMu          sync.Mutex
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
		store:               cfg.Store,
		ollama:              cfg.Ollama,
		llmClient:           cfg.LLMClient,
		publisher:           cfg.Publisher,
		customSkillsDir:     customDir,
		initialDelay:        initDelay,
		tickInterval:        tickInt,
		tickCount:           1,
		role:                strings.TrimSpace(cfg.Role),
		stack:               config.NormalizeStack(cfg.Stack),
		extractTimeout:      extractTimeout,
		restateIngressURL:   strings.TrimRight(strings.TrimSpace(cfg.RestateIngressURL), "/"),
		restateAdminURL:     strings.TrimRight(strings.TrimSpace(cfg.RestateAdminURL), "/"),
		restateAdvertiseURL: strings.TrimSpace(cfg.RestateAdvertiseURL),
		httpClient:          httpClient,
		armRetryInterval:    cfg.ArmRetryInterval,
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
	if w.restateIngressURL == "" {
		w.fallbackMu.Lock()
		defer w.fallbackMu.Unlock()
	}
	timeout := w.extractTimeout
	if timeout <= 0 {
		timeout = config.DefaultExtractTimeout
	}
	extractCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return ExtractThreadFacts(extractCtx, w.store, w.ollama, w.llmClient, threadID, agent...)
}

func registerRestateDeployment(ctx context.Context, client *http.Client, adminURL, advertiseURL string) error {
	admin := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	advertise := strings.TrimSpace(advertiseURL)
	if admin == "" || advertise == "" {
		return nil
	}
	endpoint := admin + "/deployments"
	payload := map[string]any{
		"uri":   advertise,
		"force": true,
	}
	_, err := restateingress.Post(ctx, client, endpoint, "", payload, 1024)
	if err == nil {
		return nil
	}
	if httpErr, ok := errors.AsType[*restateingress.HTTPError](err); ok {
		detail := httpErr.Message
		if detail == "" {
			detail = httpErr.Body
		}
		if detail != "" {
			detail = ": " + detail
		}
		return fmt.Errorf("restate admin /deployments returned %d%s", httpErr.Status, detail)
	}
	return fmt.Errorf("register coven restate deployment: %w", err)
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
		if err := registerRestateDeployment(ctx, w.httpClient, w.restateAdminURL, w.restateAdvertiseURL); err != nil {
			log.Printf("[coven-dreamer] warning registering restate deployment: %v; will retry in %s", err, backoff)
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
	if !w.fallbackMu.TryLock() {
		log.Printf("[Memory Worker] Skipping daily dreaming: consolidation already in progress")
		return
	}
	defer w.fallbackMu.Unlock()
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
// Restate's per-key lock is the only serialization extraction needs; each batch runs as
// journaled steps (load, LLM, one step per fact write, watermark) so a retry resumes at the
// failed step instead of re-running the batch.
type CovenThread struct {
	worker *Worker
}

// NewCovenThread creates a CovenThread Virtual Object handler.
func NewCovenThread(worker *Worker) *CovenThread {
	return &CovenThread{worker: worker}
}

const (
	// extractAttemptStateKey holds the rejected-answer attempt ordinal for the current batch.
	extractAttemptStateKey = "extract_attempt"
	// maxExtractAttempts is the house agent-retry cap for one batch's LLM answer.
	maxExtractAttempts = 3
)

// extractAttemptState counts rejected LLM answers for the batch that starts after Watermark.
type extractAttemptState struct {
	Watermark int64 `json:"watermark"`
	Attempt   int   `json:"attempt"`
}

// classifyExtractError marks deterministic failures terminal (runbook 3.4): Postgres SQLSTATE
// classes 22/23/42/0A/2B/2D/25 -> 400 and a missing fact -> 404. Connection, timeout,
// serialization and deadlock errors stay retryable.
func classifyExtractError(err error) error {
	if err == nil || restate.IsTerminalError(err) {
		return err
	}
	if errors.Is(err, store.ErrFactNotFound) {
		return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusNotFound))
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && len(pgErr.Code) >= 2 {
		switch strings.ToUpper(pgErr.Code[:2]) {
		case "22", "23", "42", "0A", "2B", "2D", "25":
			return restate.ToTerminalError(err, restate.WithErrorCode(http.StatusBadRequest))
		}
	}
	return err
}

// Extract extracts the thread's unextracted turns batch by batch.
func (c *CovenThread) Extract(ctx restate.ObjectContext, req ExtractJobRequest) error {
	key := restate.Key(ctx)
	agent, threadID := config.ParseScopedThreadKey(key)
	if req.ThreadID != "" {
		threadID = req.ThreadID
	}
	if req.Agent != "" {
		agent = req.Agent
	}
	threadID = strings.TrimSpace(threadID)
	agent = strings.TrimSpace(agent)
	if threadID == "" {
		return nil
	}

	batchSize := config.DefaultExtractMaxSteps
	for {
		batch, err := restate.Run(ctx, func(rc restate.RunContext) (extractBatch, error) {
			if c.worker == nil || c.worker.store == nil {
				return extractBatch{}, restate.ToTerminalError(errors.New("coven worker is nil"), restate.WithErrorCode(http.StatusBadRequest))
			}
			b, err := loadExtractBatch(rc, c.worker.store, threadID, agent, batchSize)
			return b, classifyExtractError(err)
		}, restate.WithName("load-extract-batch"))
		if err != nil {
			return err
		}
		switch {
		case batch.Halve > 0:
			batchSize = batch.Halve
			continue
		case batch.Skipped:
			batchSize = config.DefaultExtractMaxSteps
			continue
		case !batch.Ready:
			restate.Clear(ctx, extractAttemptStateKey)
			return nil
		}
		next, procErr := c.processBatch(ctx, threadID, agent, batch)
		if procErr != nil {
			return procErr
		}
		batchSize = next
	}
}

// processBatch runs one loaded batch through the LLM and applies the answer, returning the
// next batch size. A rejected answer is retried under a fresh LLM key (CallerKey + ":a<N>")
// up to maxExtractAttempts; then the batch is recorded as failed and the watermark moves past it.
func (c *CovenThread) processBatch(ctx restate.ObjectContext, threadID, agent string, batch extractBatch) (int, error) {
	state, err := restate.Get[extractAttemptState](ctx, extractAttemptStateKey)
	if err != nil {
		return 0, err
	}
	attempt := 0
	if state.Watermark == batch.Watermark {
		attempt = state.Attempt
	}

	llmKey := extractLLMKey(agent, threadID, batch.Watermark, attempt)
	ans, err := restate.Run(ctx, func(rc restate.RunContext) (extractAnswer, error) {
		return c.worker.callExtractStep(rc, llmKey, agent, batch)
	}, restate.WithName("extract-llm"))
	if err != nil {
		return 0, err
	}
	if ans.TimedOut {
		return max(1, batch.Turns/2), nil
	}

	if ans.Rejected != "" {
		attempt++
		if attempt < maxExtractAttempts {
			restate.Set(ctx, extractAttemptStateKey, extractAttemptState{Watermark: batch.Watermark, Attempt: attempt})
			return config.DefaultExtractMaxSteps, nil
		}
		if _, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
			return struct{}{}, classifyExtractError(skipRejectedBatch(rc, c.worker.store, threadID, agent, batch, ans.Rejected))
		}, restate.WithName("skip-rejected-batch")); err != nil {
			return 0, err
		}
		restate.Clear(ctx, extractAttemptStateKey)
		return config.DefaultExtractMaxSteps, nil
	}

	if err := c.applyBatch(ctx, threadID, agent, batch.MaxRowID, ans.Payload); err != nil {
		return 0, err
	}
	restate.Clear(ctx, extractAttemptStateKey)
	return config.DefaultExtractMaxSteps, nil
}

// callExtractStep is the body of the extract-llm step: one keyed LLM call bounded by the
// extraction timeout. A timeout on a multi-turn batch asks the handler to halve the batch.
func (w *Worker) callExtractStep(ctx context.Context, llmKey, agent string, batch extractBatch) (extractAnswer, error) {
	if w.llmClient == nil {
		return extractAnswer{}, restate.ToTerminalError(errors.New("coven LLM client is nil"), restate.WithErrorCode(http.StatusBadRequest))
	}
	spanCtx, span := startExtractSpan(ctx, agent)
	defer span.End()
	span.SetAttributes(attribute.Int("aerial.coven.batch_size", batch.Turns))
	callCtx, cancel := context.WithTimeout(spanCtx, w.extractTimeout)
	defer cancel()

	ans, err := callExtractLLM(withLLMKey(callCtx, llmKey), w.llmClient, batch.Prompt)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && batch.Turns > 1 {
		slog.WarnContext(ctx, "[Memory] extraction timed out, halving batch size", "llm_key", llmKey, "new_batch_size", max(1, batch.Turns/2))
		return extractAnswer{TimedOut: true}, nil
	}
	if err != nil {
		span.SetStatus(codes.Error, "extraction LLM call failed")
		return extractAnswer{}, err
	}
	if ans.Rejected != "" {
		slog.WarnContext(ctx, "[Memory] extraction LLM answer rejected", "llm_key", llmKey, "reason", ans.Rejected)
	}
	return ans, nil
}

// skipRejectedBatch advances the watermark past a batch whose every LLM answer was rejected,
// so the thread keeps extracting later turns, and records the skip in logs and metrics.
func skipRejectedBatch(ctx context.Context, s store.Store, threadID, agent string, batch extractBatch, reason string) error {
	if err := s.UpdateConversationFactWatermark(ctx, threadID, batch.MaxRowID, agent); err != nil {
		return fmt.Errorf("failed to skip rejected extraction batch: %w", err)
	}
	slog.ErrorContext(ctx, "[Memory] extraction batch failed: LLM answer rejected on every attempt; skipping batch",
		"agent", agent, "thread_id", threadID, "from_row_id", batch.Watermark, "to_row_id", batch.MaxRowID,
		"attempts", maxExtractAttempts, "reason", reason)
	ExtractBatchesRejectedTotal.Inc()
	RecordOperation("extract", "error", 0)
	return nil
}

// applyBatch writes one validated answer, one side effect per step: each reinforcement, each
// candidate's plan (embedding + tier-3 LLM) then its write, each procedural candidate, and
// finally the watermark.
func (c *CovenThread) applyBatch(ctx restate.ObjectContext, threadID, agent string, maxRowID int64, payload ExtractedFactsPayload) error {
	s := c.worker.store
	for _, id := range uniquePositiveIDs(payload.ReinforcedIDs) {
		if _, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
			return struct{}{}, classifyExtractError(reinforceExtractedFactIDs(rc, s, []int64{id}))
		}, restate.WithName("reinforce-fact")); err != nil {
			return err
		}
	}
	for _, item := range firstFactCandidates(payload.Facts) {
		plan, err := restate.Run(ctx, func(rc restate.RunContext) (reconcilePlan, error) {
			p, err := planCandidateReconcile(rc, s, c.worker.ollama, c.worker.llmClient, item, agent)
			return p, classifyExtractError(err)
		}, restate.WithName("plan-fact"))
		if err != nil {
			return err
		}
		if !plan.Admit {
			continue
		}
		if _, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
			return struct{}{}, classifyExtractError(writeCandidateReconcile(rc, s, item, plan, agent))
		}, restate.WithName("write-fact")); err != nil {
			return err
		}
	}
	procedural := payload.ProceduralCandidates
	if len(procedural) > 3 {
		procedural = procedural[:3]
	}
	for _, item := range procedural {
		if _, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
			err := stageProceduralCandidates(rc, s, c.worker.ollama, threadID, []ExtractedProceduralItem{item}, agent)
			return struct{}{}, classifyExtractError(err)
		}, restate.WithName("stage-procedural")); err != nil {
			return err
		}
	}
	_, err := restate.Run(ctx, func(rc restate.RunContext) (struct{}, error) {
		if err := s.UpdateConversationFactWatermark(rc, threadID, maxRowID, agent); err != nil {
			return struct{}{}, classifyExtractError(fmt.Errorf("failed to update conversation fact watermark: %w", err))
		}
		RecordOperation("extract", statusSuccess, 0)
		return struct{}{}, nil
	}, restate.WithName("advance-watermark"))
	return err
}

// uniquePositiveIDs returns ids without duplicates or non-positive values, in input order.
func uniquePositiveIDs(ids []int64) []int64 {
	seen := make(map[int64]bool, len(ids))
	var out []int64
	for _, id := range ids {
		if id > 0 && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
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

func (d *CovenDreamer) runConsolidateFacts(rc restate.RunContext) (int, error) {
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
}

func (d *CovenDreamer) runConsolidateSkills(rc restate.RunContext) (int, error) {
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
}

// Dream executes daily fact and skill consolidation and self-arms the next cycle.
// Both fact cluster consolidation and procedural skill consolidation are idempotent:
// fact cluster merges are deterministic, and procedural skill candidates update
// candidate status rows in PostgreSQL on success.
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
	if _, err := restate.Run(ctx, d.runConsolidateFacts, restate.WithName("consolidate-facts"), restate.WithMaxRetryAttempts(2)); err != nil {
		runErr = err
	}

	if _, err := restate.Run(ctx, d.runConsolidateSkills, restate.WithName("consolidate-skills"), restate.WithMaxRetryAttempts(2)); err != nil && runErr == nil {
		runErr = err
	}

	nextDelay := DefaultDreamingMinInterval
	if runErr != nil {
		failures, _ := restate.Get[int](ctx, "failures")
		failures++
		restate.Set(ctx, "failures", failures)
		RecordOperation("consolidate", "error", 0)
		shift := min(failures-1, 4)
		nextDelay = min(DefaultDreamingRetryInterval*(1<<shift), DefaultDreamingMinInterval)
		log.Printf("[coven-dreamer] ERROR: consolidation failed (consecutive failures=%d): %v; scheduling retry in %s", failures, runErr, nextDelay)
	} else {
		restate.Clear(ctx, "failures")
		RecordOperation("consolidate", statusSuccess, 0)
	}
	restate.ObjectSend(ctx, "CovenDreamer", stack, "Dream").Send(struct{}{}, restate.WithDelay(nextDelay))
	return nil
}
