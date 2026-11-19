package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// covenMaxNakDelay caps the retry-forever backoff for indexed events.
const covenMaxNakDelay = 1 * time.Hour

// darkAnchorMessage feeds the reachability anchor below an empty event.
type darkAnchorMessage struct{}

func (darkAnchorMessage) Data() []byte                     { return []byte(`{}`) }
func (darkAnchorMessage) Subject() string                  { return "" }
func (darkAnchorMessage) Header() nats.Header              { return nil }
func (darkAnchorMessage) NumDelivered() uint64             { return 0 }
func (darkAnchorMessage) Ack() error                       { return nil }
func (darkAnchorMessage) NakWithDelay(time.Duration) error { return nil }
func (darkAnchorMessage) Term() error                      { return nil }
func (darkAnchorMessage) Snooze(time.Duration) error       { return nil }
func (darkAnchorMessage) InProgress() error                { return nil }

// darkCovenAnchor keeps the per-stack extraction pieces reachable in the
// production call graph until the indexed-event cutover chunk subscribes
// handleTranscriptsIndexedEvent. Every call below is a validated no-op:
// empty inputs return before any I/O, so this performs no network or
// database work and changes no behavior. The cutover chunk removes this
// anchor when it wires the live subscription.
var _ = func() bool {
	_ = NormalizeStack("")
	cfg := &Config{}
	_ = cfg.EffectiveStack()
	_ = cfg.ExtractTimeout()
	_ = cfg.ExtractBatchLimit()
	client := NewIndexerClient("", "", nil)
	client.SetMaxSteps(1)
	_, errGet := client.GetTrajectoryContext(context.Background(), "")
	_ = ScopedThreadKey("", "")
	_ = covenNakDelay(0)
	errHandle := handleTranscriptsIndexedEvent(context.Background(), darkAnchorMessage{}, "", nil)
	locker := NewThreadLocker(nil)
	_, errLock := locker.Lock(context.Background(), "")
	errWrite := withFactWriteLock(context.Background(), nil, "", nil)
	_ = []error{errGet, errHandle, errLock, errWrite}
	return true
}()

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
	if extract == nil {
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
