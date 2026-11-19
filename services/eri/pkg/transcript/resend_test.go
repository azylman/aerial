package transcript

import (
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

func resendBatch(session string, first, last int) transcriptwire.TranscriptStepBatch {
	return transcriptwire.TranscriptStepBatch{
		Stack:     "aerial",
		SessionID: session,
		FirstStep: first,
		LastStep:  last,
		Final:     true,
		FileMtime: time.Unix(1000, 0).UTC(),
		Steps:     []transcriptwire.TranscriptStep{{StepIndex: first}},
	}
}

func TestResendOnStartup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		watermark  map[string]int
		batches    []transcriptwire.TranscriptStepBatch
		wantSent   int
		wantQueued int
	}{
		{name: "empty input sends nothing", watermark: nil, batches: nil, wantSent: 0, wantQueued: 0},
		{name: "unknown session resends all", watermark: map[string]int{}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 2)}, wantSent: 1, wantQueued: 1},
		{name: "step zero batch with no watermark resends", watermark: map[string]int{}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 0)}, wantSent: 1, wantQueued: 1},
		{name: "step zero batch covered by zero watermark skips", watermark: map[string]int{"s1": 0}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 0)}, wantSent: 0, wantQueued: 0},
		{name: "fully indexed batch skipped", watermark: map[string]int{"s1": 2}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 2)}, wantSent: 0, wantQueued: 0},
		{name: "partial watermark resends tail", watermark: map[string]int{"s1": 0}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 0), resendBatch("s1", 1, 2)}, wantSent: 1, wantQueued: 1},
		{name: "sessions independent", watermark: map[string]int{"s1": 5}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("s1", 0, 5), resendBatch("s2", 0, 1)}, wantSent: 1, wantQueued: 1},
		{name: "blank session dropped", watermark: map[string]int{}, batches: []transcriptwire.TranscriptStepBatch{resendBatch("  ", 0, 1)}, wantSent: 0, wantQueued: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewPublisher("aerial", nil)
			if got := ResendOnStartup(p, tc.watermark, tc.batches); got != tc.wantSent {
				t.Errorf("ResendOnStartup = %d, want %d", got, tc.wantSent)
			}
			queued := 0
			for _, s := range p.PendingSessions() {
				queued += p.PendingCount(s)
			}
			if queued != tc.wantQueued {
				t.Errorf("queued = %d, want %d", queued, tc.wantQueued)
			}
		})
	}
}
