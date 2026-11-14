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
	if normStatus == "success" {
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
		if (p.ctx != nil && p.ctx.Err() != nil) || te.isInterrupted() || strings.TrimSpace(te.turnStatus) == "" {
			return
		}
		if p.EventBus() == nil {
			if d.Source == "vanadis" && chunkID != "" {
				log.Printf("[WorkerPool] Warning: cannot publish dispatch outcome for vanadis chunk %s (dispatch %s, status %s): event bus is nil", chunkID, d.ID, te.turnStatus)
			}
			return
		}
		pubCtx, cancel := context.WithTimeout(context.WithoutCancel(te.turnCtxValue()), 5*time.Second)
		if err := p.publishDispatchOutcome(pubCtx, d, te.turnStatus, te.turnError, te.execStart, time.Now().UTC()); err != nil {
			log.Printf("[WorkerPool] Warning publishing dispatch outcome for %s: %v", d.ID, err)
		}
		cancel()
	}
}
