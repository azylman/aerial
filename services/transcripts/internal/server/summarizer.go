package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/services/transcripts/internal/config"
	"github.com/azylman/aerial/services/transcripts/internal/store"
)

// maxSummaryPromptChars bounds one step's contribution to the summary prompt.
const maxSummaryPromptChars = 2000

// ErrSummaryLockBusy reports a per-thread summary lock held by another worker.
// RunOnce skips the session without error so a busy thread never blocks the sweep.
var ErrSummaryLockBusy = errors.New("transcripts summary lock busy")

// ErrSummaryAttemptsExhausted reports a session whose LLM answers were rejected
// store.MaxSummaryAttempts times; it is skipped until new steps are indexed.
var ErrSummaryAttemptsExhausted = errors.New("transcripts summary attempts exhausted")

// Summarizer refreshes per-session summaries from indexed steps for one stack.
// It never marks a session settled and never advances the summary watermark on
// any failure; failures leave the row untouched for a later retry.
type Summarizer struct {
	store    *store.Store
	llm      LLMClient
	embedder Embedder
	cfg      config.SummarizerConfig
	now      func() time.Time
	acquire  func(ctx context.Context, threadID string) (func(), error)
}

// NewSummarizer binds a per-stack idle summarizer with constructor injection.
// A nil llm or embedder surfaces on RunOnce; the advisory release stays
// leak-safe through Store.AcquireThreadSummaryLock.
func NewSummarizer(st *store.Store, llm LLMClient, embedder Embedder, cfg config.SummarizerConfig) *Summarizer {
	cfg = cfg.WithDefaults()
	s := &Summarizer{store: st, llm: llm, embedder: embedder, cfg: cfg, now: time.Now}
	s.acquire = func(ctx context.Context, threadID string) (func(), error) {
		return st.AcquireThreadSummaryLock(ctx, threadID)
	}
	return s
}

// buildSummaryPrompt renders the LLM prompt from one session's stored steps.
// The prompt carries the session id plus the step indexes, types, tool names,
// and clipped contents the summarizer actually read.
func buildSummaryPrompt(sessionID, priorSummary string, steps []store.StepResult) string {
	var b strings.Builder
	b.WriteString("Summarize the following indexed transcript steps for session ")
	b.WriteString(strings.TrimSpace(sessionID))
	b.WriteString(" into a concise durable summary of what happened and what was decided. ")
	b.WriteString("Output plain text only, no markdown formatting.\n")
	if prior := strings.TrimSpace(priorSummary); prior != "" {
		if len([]rune(prior)) > maxSummaryPromptChars {
			prior = string([]rune(prior)[:maxSummaryPromptChars]) + "...[clipped]"
		}
		b.WriteString("Prior summary: ")
		b.WriteString(prior)
		b.WriteString("\n")
	}
	for _, st := range steps {
		label := strings.TrimSpace(st.StepType)
		if tool := strings.TrimSpace(st.ToolName); tool != "" {
			if label != "" {
				label += "/"
			}
			label += tool
		}
		if label == "" {
			label = "step"
		}
		content := strings.TrimSpace(st.Content)
		if len([]rune(content)) > maxSummaryPromptChars {
			content = string([]rune(content)[:maxSummaryPromptChars]) + "...[clipped]"
		}
		fmt.Fprintf(&b, "Step %d (%s): %s\n", st.StepIndex, label, content)
	}
	return b.String()
}

// RunOnce summarizes up to BatchLimit due sessions for the store's stack.
// Disabled configs and sessions already at their watermark summarize nothing.
// A busy per-thread lock skips that session; any LLM, embedding, or write
// failure leaves the row (including is_settled) untouched and is reported.
func (s *Summarizer) RunOnce(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, fmt.Errorf("transcripts summarizer store is nil")
	}
	if s.llm == nil {
		return 0, fmt.Errorf("transcripts summarizer llm client is nil")
	}
	if s.embedder == nil {
		return 0, fmt.Errorf("transcripts summarizer embedder is nil")
	}
	if !s.cfg.Enabled {
		return 0, nil
	}
	candidates, err := s.selectDueSessions(ctx)
	if err != nil {
		return 0, err
	}
	summarized := 0
	var failures []error
	for _, cand := range candidates {
		done, err := s.summarizeSession(ctx, cand)
		if err != nil {
			if errors.Is(err, ErrSummaryLockBusy) {
				continue
			}
			failures = append(failures, fmt.Errorf("summarize session %s: %w", cand.SessionID, err))
			continue
		}
		summarized += done
	}
	if len(failures) > 0 {
		return summarized, errors.Join(failures...)
	}
	return summarized, nil
}

// selectDueSessions lists sessions whose indexed steps outran the summary
// watermark, oldest first, bounded by the configured batch limit.
func (s *Summarizer) selectDueSessions(ctx context.Context) ([]store.SummaryCandidate, error) {
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	return s.store.ListDueSessions(ctx, s.cfg.IdleThreshold, s.cfg.MinStepDelta, s.cfg.BatchLimit, nowFn())
}

// summarizeSession refreshes one session's summary from steps after its
// watermark. It returns 1 when the summary advanced and 0 when skipped.
func (s *Summarizer) summarizeSession(ctx context.Context, cand store.SummaryCandidate) (int, error) {
	lockKey := cand.ThreadID
	if lockKey == "" {
		lockKey = cand.SessionID
	}
	release, err := s.acquire(ctx, lockKey)
	if err != nil {
		return 0, err
	}
	defer release()

	fromStep := cand.Watermark + 1
	endStep := cand.LastIndexed
	if s.cfg.MaxSteps > 0 && endStep-fromStep+1 > s.cfg.MaxSteps {
		fromStep = endStep - s.cfg.MaxSteps + 1
	}
	steps, err := s.store.GetSteps(ctx, cand.Agent, cand.SessionID, fromStep, endStep)
	if err != nil {
		return 0, fmt.Errorf("load summary steps: %w", err)
	}
	if len(steps) == 0 {
		return 0, nil
	}
	llmCtx := withLLMKey(ctx, summaryCallerKey(cand.Agent, cand.SessionID, fromStep, cand.Attempts+1))
	summary, err := s.llm.Complete(llmCtx, buildSummaryPrompt(cand.SessionID, cand.Summary, steps))
	if err != nil {
		return 0, fmt.Errorf("summarize session with llm: %w", err)
	}
	if strings.TrimSpace(summary) == "" {
		return 0, s.rejectSummary(ctx, cand, "empty summary")
	}
	if _, err := s.embedder.GenerateEmbedding(ctx, summary); err != nil {
		return 0, fmt.Errorf("embed session summary: %w", err)
	}
	if err := s.storeSummary(ctx, cand, summary); err != nil {
		return 0, err
	}
	return 1, nil
}

// summaryCallerKey names one summary LLM call. The first attempt keeps the bare
// key; a retry after a rejected answer appends ":a<N>" so it never replays the
// bad answer eri cached under the previous key.
func summaryCallerKey(agent, sessionID string, fromStep, attempt int) string {
	key := fmt.Sprintf("transcripts:summary:%s:%s:%d", agent, sessionID, fromStep)
	if attempt > 1 {
		key += fmt.Sprintf(":a%d", attempt)
	}
	return key
}

// rejectSummary records a rejected LLM answer against the session's durable
// attempt count. At store.MaxSummaryAttempts the session fails terminally
// (skipped until new steps arrive) with a logged reason and a metric.
func (s *Summarizer) rejectSummary(ctx context.Context, cand store.SummaryCandidate, reason string) error {
	attempts, err := s.store.RecordSummaryRejection(ctx, cand.Agent, cand.SessionID, cand.LastIndexed)
	if err != nil {
		return fmt.Errorf("summarize session with llm: %s: %w", reason, err)
	}
	if attempts >= store.MaxSummaryAttempts {
		summaryRejectionsTotal.WithLabelValues(rejectionExhausted).Inc()
		log.Printf("[Transcripts] summary for %s/%s failed terminally after %d rejected answers: %s", cand.Agent, cand.SessionID, attempts, reason)
		return fmt.Errorf("summarize session with llm: %s: %w", reason, ErrSummaryAttemptsExhausted)
	}
	summaryRejectionsTotal.WithLabelValues(rejectionRetry).Inc()
	return fmt.Errorf("summarize session with llm: %s (attempt %d of %d)", reason, attempts, store.MaxSummaryAttempts)
}

// storeSummary persists the refreshed summary and advances only the summary
// watermark. It never touches is_settled or last_indexed_step: a failed LLM
// or embedding call returns before this runs, so failures cannot settle.
func (s *Summarizer) storeSummary(ctx context.Context, cand store.SummaryCandidate, summary string) error {
	return s.store.SaveSummary(ctx, cand.Agent, cand.SessionID, summary, cand.LastIndexed)
}
