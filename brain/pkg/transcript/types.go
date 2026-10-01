package transcript

import (
	"context"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

// EmbedderFunc generates a vector embedding for the provided text.
type EmbedderFunc func(ctx context.Context, text string) ([]float32, error)

// LLMClientFunc generates an LLM completion for the provided prompt.
type LLMClientFunc func(ctx context.Context, prompt string) (string, error)

// SyncOptions configures the transcript sync sweep.
type SyncOptions struct {
	BatchLimit    int           `json:"batch_limit"`
	IdleThreshold time.Duration `json:"idle_threshold"`
	BrainDir      string        `json:"brain_dir"`
	HeadLimit     int           `json:"head_limit"`
	TailLimit     int           `json:"tail_limit"`
}

// SyncStats records metrics from a transcript synchronization execution.
type SyncStats struct {
	Scanned            int           `json:"scanned"`
	Synced             int           `json:"synced"`
	StepsInserted      int           `json:"steps_inserted"`
	SummariesGenerated int           `json:"summaries_generated"`
	SkippedUnchanged   int           `json:"skipped_unchanged"`
	Errors             int           `json:"errors"`
	Duration           time.Duration `json:"duration"`
}

// SearchResult aggregates hits from macro session summaries and micro transcript steps.
type SearchResult struct {
	Query    string              `json:"query"`
	Mode     string              `json:"mode"`
	Sessions []db.SessionSummary `json:"sessions,omitempty"`
	Steps    []db.TranscriptStep `json:"steps,omitempty"`
	Total    int                 `json:"total"`
}

// DefaultSyncOptions returns safe defaults for transcript syncing.
func DefaultSyncOptions() SyncOptions {
	return SyncOptions{
		BatchLimit:    100,
		IdleThreshold: 10 * time.Minute,
		HeadLimit:     15000,
		TailLimit:     35000,
	}
}
