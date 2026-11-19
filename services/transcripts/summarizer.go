package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DefaultSummarizerBatchLimit caps sessions summarized per RunOnce sweep.
const DefaultSummarizerBatchLimit = 10

// DefaultSummarizerMaxSteps caps stored steps folded into one summary prompt.
const DefaultSummarizerMaxSteps = 50

// DefaultSummarizerIdleThreshold bounds how recently a session must have been updated to be considered active.
const DefaultSummarizerIdleThreshold = 10 * time.Minute

// DefaultSummarizerMinStepDelta is the minimum number of new steps required before re-summarizing an already-summarized session.
const DefaultSummarizerMinStepDelta = 20

// maxSummaryPromptChars bounds one step's contribution to the summary prompt.
const maxSummaryPromptChars = 2000

// ErrSummaryLockBusy reports a per-thread summary lock held by another worker.
// RunOnce skips the session without error so a busy thread never blocks the sweep.
var ErrSummaryLockBusy = errors.New("transcripts summary lock busy")

// SummarizerConfig tunes the dark-by-default per-stack idle summarizer.
// Enabled defaults to false; the indexer ships dark until the summaries
// cutover chunk flips it on.
type SummarizerConfig struct {
	Enabled       bool          `yaml:"enabled"`
	BatchLimit    int           `yaml:"batch_limit"`
	MaxSteps      int           `yaml:"max_steps"`
	IdleThreshold time.Duration `yaml:"idle_threshold"`
	MinStepDelta  int           `yaml:"min_step_delta"`
	EriLLMURL     string        `yaml:"eri_llm_url"`
	SensesURL     string        `yaml:"senses_url"`
	SensesModel   string        `yaml:"senses_model"`
}

// withDefaults fills non-positive sweep bounds; Enabled stays as configured.
func (c SummarizerConfig) withDefaults() SummarizerConfig {
	if c.BatchLimit <= 0 {
		c.BatchLimit = DefaultSummarizerBatchLimit
	}
	if c.MaxSteps <= 0 {
		c.MaxSteps = DefaultSummarizerMaxSteps
	}
	if c.IdleThreshold <= 0 {
		c.IdleThreshold = DefaultSummarizerIdleThreshold
	}
	if c.MinStepDelta <= 0 {
		c.MinStepDelta = DefaultSummarizerMinStepDelta
	}
	return c
}

// summaryCandidate is one session whose indexed steps outran its summary watermark.
type summaryCandidate struct {
	sessionID   string
	threadID    string
	summary     string
	lastIndexed int
	watermark   int
}

// Summarizer refreshes per-session summaries from indexed steps for one stack.
// It never marks a session settled and never advances the summary watermark on
// any failure; failures leave the row untouched for a later retry.
type Summarizer struct {
	store    *Store
	llm      LLMClient
	embedder Embedder
	cfg      SummarizerConfig
	now      func() time.Time
	acquire  func(ctx context.Context, threadID string) (func(), error)
}

// NewSummarizer binds a per-stack idle summarizer with constructor injection.
// A nil llm or embedder surfaces on RunOnce; the advisory release stays
// leak-safe through Store.AcquireThreadSummaryLock.
func NewSummarizer(store *Store, llm LLMClient, embedder Embedder, cfg SummarizerConfig) *Summarizer {
	cfg = cfg.withDefaults()
	s := &Summarizer{store: store, llm: llm, embedder: embedder, cfg: cfg, now: time.Now}
	s.acquire = func(ctx context.Context, threadID string) (func(), error) {
		return store.AcquireThreadSummaryLock(ctx, threadID)
	}
	return s
}

// buildSummaryPrompt renders the LLM prompt from one session's stored steps.
// The prompt carries the session id plus the step indexes, types, tool names,
// and clipped contents the summarizer actually read.
func buildSummaryPrompt(sessionID, priorSummary string, steps []StepResult) string {
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
			failures = append(failures, fmt.Errorf("summarize session %s: %w", cand.sessionID, err))
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
func (s *Summarizer) selectDueSessions(ctx context.Context) ([]summaryCandidate, error) {
	if err := s.store.checkWriter(); err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	cutoff := nowFn().UTC().Add(-s.cfg.IdleThreshold)
	scanLimit := s.cfg.BatchLimit * 4
	rows, err := s.store.db.QueryContext(queryCtx,
		`SELECT session_id, thread_id, summary, last_indexed_step, summary_step_watermark, last_mtime, is_settled FROM session_summaries
		 WHERE stack = $1 AND summary_step_watermark < last_indexed_step
		   AND (summary = '' OR (summary_step_watermark > 0 AND last_indexed_step - summary_step_watermark >= $2))
		 ORDER BY updated_at ASC LIMIT $3`, s.store.stack, s.cfg.MinStepDelta, scanLimit)
	if err != nil {
		return nil, fmt.Errorf("select summary candidates: %w", err)
	}
	defer rows.Close()
	var out []summaryCandidate
	for rows.Next() {
		var cand summaryCandidate
		var rawMtime any
		var isSettled sql.NullBool
		if err := rows.Scan(&cand.sessionID, &cand.threadID, &cand.summary, &cand.lastIndexed, &cand.watermark, &rawMtime, &isSettled); err != nil {
			return nil, fmt.Errorf("scan summary candidate: %w", err)
		}
		cand.sessionID = strings.TrimSpace(cand.sessionID)
		cand.threadID = strings.TrimSpace(cand.threadID)
		cand.summary = strings.TrimSpace(cand.summary)
		if cand.sessionID == "" {
			continue
		}
		var lastMtime time.Time
		if t, ok := parseDBTime(rawMtime); ok {
			lastMtime = t
		}
		settled := isSettled.Valid && isSettled.Bool
		if !settled && !lastMtime.IsZero() && lastMtime.After(cutoff) {
			continue
		}
		out = append(out, cand)
		if len(out) >= s.cfg.BatchLimit {
			break
		}
	}
	return out, rows.Err()
}

// summarizeSession refreshes one session's summary from steps after its
// watermark. It returns 1 when the summary advanced and 0 when skipped.
func (s *Summarizer) summarizeSession(ctx context.Context, cand summaryCandidate) (int, error) {
	lockKey := cand.threadID
	if lockKey == "" {
		lockKey = cand.sessionID
	}
	release, err := s.acquire(ctx, lockKey)
	if err != nil {
		return 0, err
	}
	defer release()

	fromStep := cand.watermark + 1
	endStep := cand.lastIndexed
	if s.cfg.MaxSteps > 0 && endStep-fromStep+1 > s.cfg.MaxSteps {
		fromStep = endStep - s.cfg.MaxSteps + 1
	}
	steps, err := s.store.GetSteps(ctx, s.store.stack, cand.sessionID, fromStep, endStep)
	if err != nil {
		return 0, fmt.Errorf("load summary steps: %w", err)
	}
	if len(steps) == 0 {
		return 0, nil
	}
	summary, err := s.llm.Complete(ctx, buildSummaryPrompt(cand.sessionID, cand.summary, steps))
	if err != nil {
		return 0, fmt.Errorf("summarize session with llm: %w", err)
	}
	if strings.TrimSpace(summary) == "" {
		return 0, fmt.Errorf("summarize session with llm: empty summary")
	}
	if _, err := s.embedder.GenerateEmbedding(ctx, summary); err != nil {
		return 0, fmt.Errorf("embed session summary: %w", err)
	}
	if err := s.storeSummary(ctx, cand, summary); err != nil {
		return 0, err
	}
	return 1, nil
}

// storeSummary persists the refreshed summary and advances only the summary
// watermark. It never touches is_settled or last_indexed_step: a failed LLM
// or embedding call returns before this runs, so failures cannot settle.
func (s *Summarizer) storeSummary(ctx context.Context, cand summaryCandidate, summary string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := s.store.db.ExecContext(queryCtx,
		`UPDATE session_summaries SET summary = $1, summary_step_watermark = $2, updated_at = CURRENT_TIMESTAMP
		 WHERE stack = $3 AND session_id = $4`,
		summary, cand.lastIndexed, s.store.stack, cand.sessionID); err != nil {
		return fmt.Errorf("store session summary: %w", err)
	}
	return nil
}
