package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
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

func termBatchWithError(msg pubsub.Message, termAction string, err error) error {
	if termErr := msg.Term(); termErr != nil {
		return fmt.Errorf("%s: %w", termAction, termErr)
	}
	return err
}

// resolveBatchSubjectAgent validates the NATS subject and payload agent slug.
// Per-agent publish authorization on 5-token subjects (aerial.transcripts.steps.<agent>.<session_id>)
// is enforced at the NATS broker layer via per-user publish ACLs (k8s/core-services.yaml).
// When a 5-token subject is present, any non-empty payload agent/stack must match the
// authenticated subject agent token. Legacy 4-token subjects (aerial.transcripts.steps.<session_id>),
// which lack a per-agent subject token, are restricted strictly to the store's default stack.
func (g *Ingester) resolveBatchSubjectAgent(msg pubsub.Message, batch *transcriptwire.TranscriptStepBatch) error {
	subjectAgent, _, ok := pubsub.ParseTranscriptsStepsSubject(msg.Subject())
	if ok && subjectAgent != "" {
		if !k8sauth.IsValidAgentSlug(subjectAgent) {
			return termBatchWithError(msg, fmt.Sprintf("term invalid agent %q transcript batch", subjectAgent), fmt.Errorf("invalid agent %q in subject %q", subjectAgent, msg.Subject()))
		}
		if payloadAgent := batch.EffectiveAgent(); payloadAgent != "" && payloadAgent != subjectAgent {
			return termBatchWithError(msg, fmt.Sprintf("term mismatched payload agent %q for subject %q", payloadAgent, msg.Subject()), fmt.Errorf("payload agent %q does not match subject agent %q", payloadAgent, subjectAgent))
		}
		batch.Agent = subjectAgent
		batch.Stack = subjectAgent
		return nil
	}
	payloadAgent := batch.EffectiveAgent()
	if payloadAgent == "" {
		return nil
	}
	if !k8sauth.IsValidAgentSlug(payloadAgent) {
		return termBatchWithError(msg, fmt.Sprintf("term invalid agent %q transcript batch", payloadAgent), fmt.Errorf("invalid agent %q in batch payload", payloadAgent))
	}
	if g.store.stack != "" && payloadAgent != g.store.stack {
		return termBatchWithError(msg, fmt.Sprintf("term cross-agent legacy subject batch for %q", payloadAgent), fmt.Errorf("legacy 4-token subject cannot write agent %q on store stack %q", payloadAgent, g.store.stack))
	}
	return nil
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
	if err := g.resolveBatchSubjectAgent(msg, &batch); err != nil {
		return err
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
	agent := batch.EffectiveAgent()
	if agent == "" {
		agent = g.store.stack
	}
	event := pubsub.TranscriptsIndexedEvent{
		Agent:        agent,
		Stack:        agent,
		SessionID:    strings.TrimSpace(batch.SessionID),
		ThreadID:     strings.TrimSpace(batch.ThreadID),
		FileMtime:    batch.FileMtime,
		FirstStep:    batch.FirstStep,
		LastStep:     batch.LastStep,
		StepsIndexed: outcome.StepsIndexed,
		Final:        batch.Final,
		IndexedAt:    time.Now().UTC(),
	}
	indexedSubject := pubsub.SubjectAerialTurnPrefix + "transcripts_indexed"
	indexedMsgID := "transcript-indexed:" + transcriptwire.MsgID(batch)
	if err := g.publish(ctx, indexedSubject, indexedMsgID, event); err != nil {
		return fmt.Errorf("publish transcripts indexed event: %w", err)
	}
	return nil
}
