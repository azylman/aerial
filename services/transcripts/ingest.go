package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
)

// Ingester consumes transcript step batches from the AERIAL_TRANSCRIPTS stream
// and indexes them through Store.ApplyBatch, then publishes one
// TranscriptsIndexedEvent per ingested batch. Publish failures return an error
// so the batch naks and redelivers; stale gap batches ack silently.
type Ingester struct {
	store   *Store
	publish func(ctx context.Context, subject, msgID string, data any) error
}

// NewIngester wires an Ingester with constructor injection. A nil publish
// function disables event emission (read-path and dark-run use).
func NewIngester(store *Store, publish func(ctx context.Context, subject, msgID string, data any) error) *Ingester {
	return &Ingester{store: store, publish: publish}
}

// NewBusIngester wires an Ingester that publishes through a JetStream bus.
func NewBusIngester(store *Store, bus pubsub.Bus) *Ingester {
	return NewIngester(store, func(ctx context.Context, subject, msgID string, data any) error {
		if bus == nil {
			return fmt.Errorf("transcripts ingester bus is nil")
		}
		return bus.Publish(ctx, subject, msgID, data)
	})
}

// HandleBatchMessage indexes one delivered step batch. Undecodable payloads
// Term so they never redeliver. Stale batches ack without publishing. Every
// other outcome publishes a TranscriptsIndexedEvent; when the publish fails the
// error propagates so the batch naks and retries instead of dropping the event.
func (g *Ingester) HandleBatchMessage(ctx context.Context, msg pubsub.Message) error {
	if g == nil || g.store == nil {
		return fmt.Errorf("transcripts ingester store is nil")
	}
	if msg == nil {
		return fmt.Errorf("transcripts batch message is nil")
	}
	var batch transcriptwire.TranscriptStepBatch
	if err := json.Unmarshal(msg.Data(), &batch); err != nil {
		if termErr := msg.Term(); termErr != nil {
			return fmt.Errorf("term undecodable transcript batch: %w", errors.Join(termErr, err))
		}
		return nil
	}
	if strings.TrimSpace(batch.SessionID) == "" {
		if termErr := msg.Term(); termErr != nil {
			return fmt.Errorf("term empty-session transcript batch: %w", termErr)
		}
		return nil
	}
	outcome, err := g.store.ApplyBatch(ctx, batch)
	if err != nil {
		return err
	}
	if outcome.Stale {
		return nil
	}
	if g.publish == nil {
		return nil
	}
	event := pubsub.TranscriptsIndexedEvent{
		Stack:        g.store.stack,
		SessionID:    strings.TrimSpace(batch.SessionID),
		ThreadID:     strings.TrimSpace(batch.ThreadID),
		FileMtime:    batch.FileMtime,
		FirstStep:    batch.FirstStep,
		LastStep:     batch.LastStep,
		StepsIndexed: outcome.StepsIndexed,
		Final:        batch.Final,
		IndexedAt:    time.Now().UTC(),
	}
	if err := g.publish(ctx, pubsub.SubjectTranscriptsIndexed, transcriptwire.MsgID(batch), event); err != nil {
		return fmt.Errorf("publish transcripts indexed event: %w", err)
	}
	return nil
}
