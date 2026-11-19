// Package transcript dark publisher: per-turn transcript step batches are
// coalesced per session and drained strictly in order. This file has no
// callers yet; turn-end publishing wires it up in a later chunk.
package transcript

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

// BatchPublishFunc publishes one transcript step batch to the
// AERIAL_TRANSCRIPTS stream. Implementations must be safe for sequential use;
// the Publisher never calls it concurrently.
type BatchPublishFunc func(ctx context.Context, batch transcriptwire.TranscriptStepBatch) error

// Publisher coalesces transcript step batches keyed by session and drains
// them strictly in order. A session's batches are never dropped by Enqueue:
// once accepted they stay queued until Drain publishes them. On failure Drain
// abandons only the chain it snapshotted before the failed call and continues
// with the next session; batches a concurrent Enqueue appended after the
// snapshot were never attempted and stay queued, so a fresh Drain re-seeds
// from the indexer watermark instead of replaying stale steps.
type Publisher struct {
	mu      sync.Mutex
	stack   string
	publish BatchPublishFunc
	queues  map[string][]transcriptwire.TranscriptStepBatch
	order   []string
}

// NewPublisher returns a Publisher labeling batches with stack. A nil publish
// function disables draining: Drain reports an error and retains every queue.
func NewPublisher(stack string, publish BatchPublishFunc) *Publisher {
	return &Publisher{
		stack:   strings.TrimSpace(stack),
		publish: publish,
		queues:  make(map[string][]transcriptwire.TranscriptStepBatch),
	}
}

// Enqueue appends batches to their sessions' queues, preserving arrival order
// within each session. Batches with a blank session ID are dropped; every
// other batch is kept (never dropped) until Drain publishes it. A publish
// failure abandons only the chain Drain snapshotted before the failed call:
// batches Enqueue appends after that snapshot stay queued for the next Drain.
func (p *Publisher) Enqueue(batches ...transcriptwire.TranscriptStepBatch) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range batches {
		session := strings.TrimSpace(b.SessionID)
		if session == "" {
			continue
		}
		if _, ok := p.queues[session]; !ok {
			p.order = append(p.order, session)
		}
		p.queues[session] = append(p.queues[session], b)
	}
}

// EnqueueSteps splits steps with the shared wire splitter and enqueues the
// resulting batches. It returns the number of batches enqueued.
func (p *Publisher) EnqueueSteps(base transcriptwire.TranscriptStepBatch, steps []transcriptwire.TranscriptStep) int {
	batches := transcriptwire.SplitStepBatches(base, steps)
	p.Enqueue(batches...)
	return len(batches)
}

// removeSessionLocked drops a session's queue and its order entry together,
// keeping the order/queue membership invariant so a concurrent Enqueue can
// never append a duplicate order entry. The caller must hold p.mu.
func (p *Publisher) removeSessionLocked(session string) {
	delete(p.queues, session)
	kept := p.order[:0]
	for _, s := range p.order {
		if s != session {
			kept = append(kept, s)
		}
	}
	for i := len(kept); i < len(p.order); i++ {
		p.order[i] = ""
	}
	p.order = kept
}

// PendingSessions returns session IDs with queued batches in first-seen order.
func (p *Publisher) PendingSessions() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

// PendingCount returns the queued batch count for one session.
func (p *Publisher) PendingCount(sessionID string) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queues[strings.TrimSpace(sessionID)])
}

// Drain publishes every queued batch strictly in order within each session.
// On the first publish failure for a session it abandons only the chain it
// snapshotted before the failed call (the failed head plus its already-queued
// successors) and continues with the next session, returning the first error
// encountered. Batches appended after the snapshot were never attempted and
// stay queued for the next Drain. Successful sessions are fully dequeued.

// dropSnapshotLocked abandons the snapshotted chain while keeping batches a
// concurrent Enqueue appended after the snapshot. The caller must hold p.mu;
// snapshotLen is the queue length Drain observed before the failed publish.
// Since Enqueue only appends, dropping the first snapshotLen entries removes
// exactly the failed chain and preserves everything appended afterwards.
func (p *Publisher) dropSnapshotLocked(session string, snapshotLen int) {
	queue := p.queues[session]
	if len(queue) > snapshotLen {
		p.queues[session] = append([]transcriptwire.TranscriptStepBatch(nil), queue[snapshotLen:]...)
		return
	}
	p.removeSessionLocked(session)
}

func (p *Publisher) Drain(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.publish == nil && len(p.order) > 0 {
		p.mu.Unlock()
		return fmt.Errorf("transcript publisher: no publish function configured")
	}
	publish := p.publish
	sessions := append([]string(nil), p.order...)
	p.mu.Unlock()

	var firstErr error
	for _, session := range sessions {
		for {
			p.mu.Lock()
			queue := p.queues[session]
			if len(queue) == 0 {
				p.removeSessionLocked(session)
				p.mu.Unlock()
				break
			}
			next := queue[0]
			snapshotLen := len(queue)
			p.mu.Unlock()

			if err := publish(ctx, next); err != nil {
				p.mu.Lock()
				p.dropSnapshotLocked(session, snapshotLen)
				p.mu.Unlock()
				if firstErr == nil {
					firstErr = fmt.Errorf("transcript publisher: publish session %s: %w", session, err)
				}
				break
			}

			p.mu.Lock()
			if len(p.queues[session]) > 0 {
				p.queues[session] = p.queues[session][1:]
			}
			if len(p.queues[session]) == 0 {
				p.removeSessionLocked(session)
			}
			p.mu.Unlock()
		}
	}

	return firstErr
}
