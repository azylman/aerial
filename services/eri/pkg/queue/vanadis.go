package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

func (p *WorkerPool) publishDispatchOutcome(ctx context.Context, d db.Dispatch, status, errMsg string, startedAt, completedAt time.Time) error {
	bus := p.EventBus()
	if bus == nil {
		return errors.New("event bus is nil")
	}

	normStatus := strings.ToLower(strings.TrimSpace(status))
	if normStatus == outcomeSuccess {
		normStatus = "completed"
	}
	if normStatus == "" {
		return errors.New("empty dispatch outcome status")
	}

	durMs := max(0, completedAt.Sub(startedAt).Milliseconds())

	chunkID := strings.TrimSpace(d.VanadisChunkID)
	if chunkID == "" {
		extractedChunk, _, _ := extractVanadisDispatchInfo(&d)
		chunkID = extractedChunk
	}

	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:     d.ID,
		Source:         d.Source,
		SessionID:      d.SessionID,
		TargetID:       d.TargetID,
		VanadisChunkID: chunkID,
		Status:         normStatus,
		Error:          errMsg,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		DurationMs:     durMs,
	}

	subject := pubsub.SubjectAerialDispatchPrefix + normStatus
	msgID := fmt.Sprintf("dispatch:%s:%s", d.ID, normStatus)

	return bus.Publish(ctx, subject, msgID, ev)
}

func (p *WorkerPool) setupDispatchTurn(d db.Dispatch, te *turnExecution) func() {
	chunkID, buildID, chunkIdx := extractVanadisDispatchInfo(&d)
	if chunkID != "" {
		// Widget edits ride the turn's trace but must not be aborted by an interrupt.
		widgetCtx := context.WithoutCancel(te.turnCtxValue())
		p.StartChunkStepTurn(widgetCtx, chunkID, buildID, chunkIdx, d.ID)
		p.ensureChunkWidgetHydrated(widgetCtx, d.Prompt, chunkID, buildID)
		te.statusUpdater = NewChunkStepStatusUpdater(chunkID, func(step string) error {
			return p.FlushChunkActiveStep(widgetCtx, chunkID, d.ID, step)
		})
	}
	return func() {
		if te.statusUpdater != nil {
			te.statusUpdater.Stop()
			te.statusUpdater.DeleteStatusMessage()
		}
		if chunkID != "" {
			widgetCtx := context.WithoutCancel(te.turnCtxValue())
			p.FinishChunkStepTurn(widgetCtx, chunkID, d.ID)
		}
		if te.isInterrupted() || strings.TrimSpace(te.turnStatus) == "" {
			return
		}
		if p.EventBus() == nil {
			if d.Source == "vanadis" && chunkID != "" {
				log.Printf("[WorkerPool] Warning: cannot publish dispatch outcome for vanadis chunk %s (dispatch %s, status %s): event bus is nil", chunkID, d.ID, te.turnStatus)
			}
			return
		}
		p.publishDispatchOutcomeWithRetries(d, te)
	}
}

func (p *WorkerPool) publishDispatchOutcomeWithRetries(d db.Dispatch, te *turnExecution) {
	delay := 500 * time.Millisecond
	if p.cfg.RetryDelayOverride > 0 {
		delay = p.cfg.RetryDelayOverride
	}
	var pubErr error
retryLoop:
	for attempt := 1; attempt <= 3; attempt++ {
		pubCtx, cancel := context.WithTimeout(context.WithoutCancel(te.turnCtxValue()), 5*time.Second)
		pubErr = p.publishDispatchOutcome(pubCtx, d, te.turnStatus, te.turnError, te.execStart, time.Now().UTC())
		cancel()
		if pubErr == nil {
			return
		}
		if attempt < 3 {
			var doneCh <-chan struct{}
			if p.ctx != nil {
				doneCh = p.ctx.Done()
			}
			select {
			case <-time.After(delay):
			case <-doneCh:
				break retryLoop
			}
		}
	}
	if pubErr != nil {
		log.Printf("[WorkerPool] Warning publishing dispatch outcome for %s: %v", d.ID, pubErr)
	}
}
