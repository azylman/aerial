package transcript

import (
	"strings"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

// ResendOnStartup re-enqueues batches not yet covered by the caller's
// watermarks (session ID to last indexed step). A session missing from the
// map resends fully: caller must omit sessions with unknown watermarks rather
// than storing zero, since zero is also a valid indexed step. Batches whose
// LastStep is at or below their session's present watermark are skipped as
// already indexed; duplicates that slip through are safe because batches
// deduplicate on MsgID and the indexer guards rewrites by FileMtime epoch.
// It returns the number of batches enqueued.
func ResendOnStartup(pub *Publisher, watermarks map[string]int, batches []transcriptwire.TranscriptStepBatch) int {
	if pub == nil {
		return 0
	}
	resetSessions := make(map[string]bool)
	for _, b := range batches {
		if b.Reset {
			if s := strings.TrimSpace(b.SessionID); s != "" {
				resetSessions[s] = true
			}
		}
	}
	var fresh []transcriptwire.TranscriptStepBatch
	for _, b := range batches {
		session := strings.TrimSpace(b.SessionID)
		if session == "" {
			continue
		}
		if !resetSessions[session] {
			if wm, ok := watermarks[session]; ok && b.LastStep <= wm {
				continue
			}
		}
		fresh = append(fresh, b)
	}
	pub.Enqueue(fresh...)
	return len(fresh)
}
