// Package transcriptwire defines the NATS wire format for per-turn transcript
// step batches published on aerial.transcripts.steps.> and indexed from the
// AERIAL_TRANSCRIPTS stream. It is transport only: it never touches storage,
// clocks beyond the envelope timestamp, or any service caller.
package transcriptwire

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	// MaxStepsPerBatch caps how many transcript steps travel in one batch.
	MaxStepsPerBatch = 100
	// MaxBatchBytes caps the encoded JSON size of a batch's steps so every
	// batch stays well under the NATS max payload.
	MaxBatchBytes = 512 * 1024
	// clipMarker marks content truncated to fit the batch byte budget.
	clipMarker = "...[clipped]"
)

// TranscriptToolCall mirrors one tool_use block carried inside a step.
type TranscriptToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
	ID   string          `json:"id,omitempty"`
}

// TranscriptStep mirrors one transcript file line bound for the index.
type TranscriptStep struct {
	StepIndex int                  `json:"step_index"`
	Source    string               `json:"source,omitempty"`
	Type      string               `json:"type,omitempty"`
	Status    string               `json:"status,omitempty"`
	CreatedAt string               `json:"created_at,omitempty"`
	Content   string               `json:"content,omitempty"`
	ToolName  string               `json:"tool_name,omitempty"`
	ToolCalls []TranscriptToolCall `json:"tool_calls,omitempty"`
	Thinking  string               `json:"thinking,omitempty"`
}

// TranscriptStepBatch is one ordered slice of a session's transcript steps.
// Batches for a session publish strictly in order and share the session's
// FileMtime epoch so the indexer can tell a rewrite from a duplicate.
type TranscriptStepBatch struct {
	Stack       string           `json:"stack"`
	SessionID   string           `json:"session_id"`
	ThreadID    string           `json:"thread_id,omitempty"`
	Engine      string           `json:"engine,omitempty"`
	Source      string           `json:"source,omitempty"`
	FileMtime   time.Time        `json:"file_mtime"`
	BaseStep    int              `json:"base_step"`
	FirstStep   int              `json:"first_step"`
	LastStep    int              `json:"last_step"`
	Final       bool             `json:"final"`
	Reset       bool             `json:"reset"`
	Step0Prompt string           `json:"step0_prompt,omitempty"`
	Steps       []TranscriptStep `json:"steps"`
}

// MsgID returns the deduplication ID for a batch. It folds the file mtime's
// unix nanos in so a rewrite of the same steps publishes under a new ID while
// an identical republish deduplicates inside the stream's Duplicates window.
func MsgID(b TranscriptStepBatch) string {
	return fmt.Sprintf("transcript-batch:%s:%s:%d:%d:%d",
		strings.TrimSpace(b.Stack), strings.TrimSpace(b.SessionID),
		b.FirstStep, b.LastStep, b.FileMtime.UnixNano())
}

// SplitStepBatches slices steps into ordered batches sharing base's envelope.
// It never errors: a step that alone exceeds the byte budget is clipped to fit.
// Empty input yields exactly one empty Final batch. Reset and Step0Prompt ride
// on the first batch only; Final is set on the last batch only.
func SplitStepBatches(base TranscriptStepBatch, steps []TranscriptStep) []TranscriptStepBatch {
	if len(steps) == 0 {
		empty := base
		empty.Steps = nil
		empty.FirstStep = base.BaseStep
		empty.LastStep = base.BaseStep
		empty.Final = true
		return []TranscriptStepBatch{empty}
	}

	work := append([]TranscriptStep(nil), steps...)
	steps = work
	var out []TranscriptStepBatch
	start := 0
	for start < len(steps) {
		end := start
		size := 0
		for end < len(steps) && end-start < MaxStepsPerBatch {
			s := steps[end]
			if stepBytes(s) > MaxBatchBytes {
				s = clipOversizeStep(s, MaxBatchBytes)
				steps[end] = s
			}
			if size+stepBytes(s) > MaxBatchBytes && end > start {
				break
			}
			size += stepBytes(s)
			end++
		}
		batch := base
		batch.Steps = append([]TranscriptStep(nil), steps[start:end]...)
		batch.BaseStep = steps[0].StepIndex
		batch.FirstStep = steps[start].StepIndex
		batch.LastStep = steps[end-1].StepIndex
		if start != 0 {
			batch.Reset = false
			batch.Step0Prompt = ""
		}
		batch.Final = end == len(steps)
		out = append(out, batch)
		start = end
	}
	return out
}

// stepBytes returns the encoded JSON size of one step.
func stepBytes(s TranscriptStep) int {
	raw, err := json.Marshal(s)
	if err != nil {
		return len(s.Content) + len(s.Thinking) + 256
	}
	return len(raw)
}

// clipOversizeStep shrinks a step until it fits budget, dropping thinking first,
// then tool calls, then truncating content. Truncation over rune boundaries
// always converges, so this always returns a fittable step.
func clipOversizeStep(s TranscriptStep, budget int) TranscriptStep {
	if stepBytes(s) <= budget {
		return s
	}
	s.Thinking = ""
	if stepBytes(s) <= budget {
		return s
	}
	s.ToolCalls = nil
	if stepBytes(s) <= budget {
		return s
	}
	s.Content = truncateToFit(s, budget)
	return s
}

// truncateToFit keeps the longest content rune prefix (plus clip marker) that
// fits. Rune prefixes marshal monotonically, so the binary search converges on
// a fitting prefix whenever the empty prefix fits.
func truncateToFit(s TranscriptStep, budget int) string {
	runes := []rune(s.Content)
	probe := s
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		probe.Content = string(runes[:mid]) + clipMarker
		if stepBytes(probe) <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return string(runes[:lo]) + clipMarker
}
