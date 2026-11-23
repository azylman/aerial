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

// dreamDBTimeout bounds each Postgres call inside a CovenDreamer restate.Run step.
const dreamDBTimeout = 5 * time.Second

// WorkerConfig defines the parameters for the background memory maintenance worker.
type WorkerConfig struct {
	Store               store.Store
	Ollama              store.OllamaClient
	LLMClient           LLMClientFunc
	Publisher           SkillPublisher
	CustomSkillsDir     string
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
	role                string
	stack               string
	extractTimeout      time.Duration
	restateIngressURL   string
	restateAdminURL     string
	restateAdvertiseURL string
	httpClient          *http.Client
	armRetryInterval    time.Duration
}

// NewWorker initializes a new background Worker instance.
func NewWorker(cfg WorkerConfig) *Worker {
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
		if err := restateingress.RegisterDeployment(ctx, w.httpClient, w.restateAdminURL, w.restateAdvertiseURL); err != nil {
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

// Start arms the CovenDreamer Virtual Object for this stack and then waits for ctx to end.
// Extractor-role workers never dream. RunApp refuses to start without a Restate ingress URL,
// so there is no in-process fallback.
func (w *Worker) Start(ctx context.Context) error {
	if w == nil || strings.EqualFold(w.role, "extractor") {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := w.armDreamer(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
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

// CovenDreamer is a Restate Virtual Object executing daily fact & skill consolidation, one
// object per stack. Exactly one Dream chain runs per stack: every scheduled Dream carries the
// due token stored in dreamDueStateKey, and a Dream whose token no longer matches (superseded
// by a later Arm or a duplicate send) returns without running or rescheduling.
type CovenDreamer struct {
	worker *Worker
}

// NewCovenDreamer creates a CovenDreamer Virtual Object handler.
func NewCovenDreamer(worker *Worker) *CovenDreamer {
	return &CovenDreamer{worker: worker}
}

const (
	// dreamDueStateKey holds the due token (unix millis) of the one live scheduled Dream.
	dreamDueStateKey = "next_dream_due"
	// legacyArmedStateKey and legacyFailuresStateKey were written by the pre-token dreamer.
	legacyArmedStateKey    = "armed"
	legacyFailuresStateKey = "failures"
)

// DreamRequest is the Dream payload. Due is the token the scheduling handler stored; a legacy
// empty payload decodes to Due 0, which matches an object that never stored a token.
type DreamRequest struct {
	Due int64 `json:"due,omitempty"`
}

// dreamSchedule is the next Dream's delay and due token, computed inside one journaled step.
type dreamSchedule struct {
	Delay time.Duration `json:"delay"`
	Due   int64         `json:"due"`
}

func newDreamSchedule(now time.Time, delay time.Duration) dreamSchedule {
	return dreamSchedule{Delay: delay, Due: now.Add(delay).UnixMilli()}
}

// dreamDelay returns how long to wait before dreaming, given the last completed dream.
func dreamDelay(lastRunAt *time.Time, now time.Time) time.Duration {
	if lastRunAt == nil || lastRunAt.IsZero() {
		return 0
	}
	elapsed := now.Sub(*lastRunAt)
	if elapsed >= DefaultDreamingMinInterval {
		return 0
	}
	return DefaultDreamingMinInterval - elapsed
}

// scheduleDream records sched's due token as the live one and sends the matching Dream.
func scheduleDream(ctx restate.ObjectContext, stack string, sched dreamSchedule) {
	restate.Set(ctx, dreamDueStateKey, sched.Due)
	send := restate.ObjectSend(ctx, "CovenDreamer", stack, "Dream")
	if sched.Delay > 0 {
		send.Send(DreamRequest{Due: sched.Due}, restate.WithDelay(sched.Delay))
		return
	}
	send.Send(DreamRequest{Due: sched.Due})
}

// Arm (re)schedules the stack's next Dream from the last dream's watermark. The worker calls
// it on every start under the idempotency key coven:dreamer:arm:<stack>; any Dream an earlier
// Arm scheduled is superseded by the new due token, so re-arming never forks a second chain.
func (d *CovenDreamer) Arm(ctx restate.ObjectContext, _ struct{}) error {
	stack := restate.Key(ctx)
	if stack == "" {
		stack = config.DefaultStack
	}
	restate.Clear(ctx, legacyArmedStateKey)
	restate.Clear(ctx, legacyFailuresStateKey)

	sched, err := restate.Run(ctx, func(rc restate.RunContext) (dreamSchedule, error) {
		now := time.Now()
		if d.worker == nil || d.worker.store == nil {
			return newDreamSchedule(now, DefaultDreamingMinInterval), nil
		}
		dbCtx, cancel := context.WithTimeout(rc, dreamDBTimeout)
		defer cancel()
		_, lastRunAt, err := d.worker.store.GetConversationFactWatermark(dbCtx, DreamingWatermarkThreadID)
		if err != nil {
			return dreamSchedule{}, classifyExtractError(err)
		}
		return newDreamSchedule(now, dreamDelay(lastRunAt, now)), nil
	}, restate.WithName("compute-delay"))
	if err != nil {
		return err
	}
	scheduleDream(ctx, stack, sched)
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
		log.Printf("[Memory Worker] Fact consolidation dreaming failed: %v", err)
		return 0, classifyExtractError(err)
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
		log.Printf("[Memory Worker] Procedural skill consolidation failed: %v", err)
		return 0, classifyExtractError(err)
	}
	if prNum > 0 {
		log.Printf("[Memory Worker] Procedural skill consolidation opened PR #%d", prNum)
	}
	return prNum, nil
}

// Dream runs daily fact and skill consolidation, then schedules the next cycle. Retryable step
// failures are retried by Restate under the server default policy; a step only returns here
// with a terminal (deterministic) error, which is recorded and the next cycle keeps the normal
// interval. A Dream whose due token is stale returns without doing anything.
func (d *CovenDreamer) Dream(ctx restate.ObjectContext, req DreamRequest) error {
	if d == nil {
		return restate.ToTerminalError(errors.New("coven dreamer is nil"), restate.WithErrorCode(http.StatusBadRequest))
	}
	stack := restate.Key(ctx)
	if stack == "" {
		stack = config.DefaultStack
	}

	due, err := restate.Get[int64](ctx, dreamDueStateKey)
	if err != nil {
		return err
	}
	if due != req.Due {
		log.Printf("[coven-dreamer] skipping superseded dream for stack %s (due %d, live %d)", stack, req.Due, due)
		return nil
	}

	_, factsErr := restate.Run(ctx, d.runConsolidateFacts, restate.WithName("consolidate-facts"))
	_, skillsErr := restate.Run(ctx, d.runConsolidateSkills, restate.WithName("consolidate-skills"))
	runErr := errors.Join(factsErr, skillsErr)
	if runErr != nil {
		RecordOperation("consolidate", "error", 0)
		log.Printf("[coven-dreamer] ERROR: consolidation failed terminally for stack %s: %v", stack, runErr)
	} else {
		RecordOperation("consolidate", statusSuccess, 0)
	}

	sched, err := restate.Run(ctx, func(restate.RunContext) (dreamSchedule, error) {
		return newDreamSchedule(time.Now(), DefaultDreamingMinInterval), nil
	}, restate.WithName("next-dream"))
	if err != nil {
		return err
	}
	scheduleDream(ctx, stack, sched)
	return nil
}
