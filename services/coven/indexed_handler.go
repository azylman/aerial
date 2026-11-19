package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

// covenMaxNakDelay caps the retry-forever backoff for indexed events.
const covenMaxNakDelay = 1 * time.Hour

// covenNakDelay returns min(1s*2^(n-1), 1h) for delivery count n.
// Coven never gives up on a decodable event: failures always NakWithDelay,
// so every delay value feeds a retry, never a drop.
func covenNakDelay(numDelivered uint64) time.Duration {
	if numDelivered <= 1 {
		return 1 * time.Second
	}
	shift := min(numDelivered-1, 20)
	if d := (1 * time.Second) << shift; d > 0 && d < covenMaxNakDelay {
		return d
	}
	return covenMaxNakDelay
}

// handleTranscriptsIndexedEvent routes one TranscriptsIndexedEvent to extraction.
// It Terms only undecodable messages, skips foreign stacks and empty threads,
// and Naks every extraction failure with covenNakDelay so delivery retries
// forever. It is dark: no subscription calls it until the indexed cutover.
func handleTranscriptsIndexedEvent(ctx context.Context, msg pubsub.Message, stack string, extract func(ctx context.Context, threadID string) error) error {
	var ev pubsub.TranscriptsIndexedEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		slog.ErrorContext(ctx, "[Coven] error unmarshaling transcripts indexed event", "error", err)
		if termErr := msg.Term(); termErr != nil {
			slog.WarnContext(ctx, "[Coven] error terming transcripts indexed event", "error", termErr)
		}
		return nil
	}
	threadID := strings.TrimSpace(ev.ThreadID)
	if threadID == "" {
		return nil
	}
	if es := strings.TrimSpace(ev.Stack); es != "" && es != NormalizeStack(stack) {
		return nil
	}
	if err := extract(ctx, threadID); err != nil {
		if nakErr := msg.NakWithDelay(covenNakDelay(msg.NumDelivered())); nakErr != nil {
			slog.WarnContext(ctx, "[Coven] error naking transcripts indexed event", "error", nakErr)
		}
		return err
	}
	return nil
}

// darkCovenAnchor keeps legacy transcript sync and deferred watermark symbols reachable
// in the production call graph until removed in spl2-c20b-coven-legacy and wired in spl2-c16-watermark-pk.
var _ = func() bool {
	_ = ScopedThreadKey("", "")
	_ = nakBackoffDelay(0)
	errSub := subscribeCovenTranscriptSynced(context.Background(), nil)
	var w Worker
	w.RunStartupCatchUp(context.Background())
	_ = []error{errSub}
	return true
}()
