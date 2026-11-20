package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

var restateActiveStepHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
func BuildServiceName(agent string) string {
	norm := NormalizeAgentName(agent)
	if norm == "" || norm == defaultStackName {
		return "Build"
	}
	return "Build-" + norm
}

func (p *WorkerPool) resolveRestateIngressURL() string {
	if p != nil && strings.TrimSpace(p.cfg.RestateIngressURL) != "" {
		return strings.TrimRight(strings.TrimSpace(p.cfg.RestateIngressURL), "/")
	}
	if envURL := strings.TrimSpace(os.Getenv("RESTATE_INGRESS_URL")); envURL != "" {
		return strings.TrimRight(envURL, "/")
	}
	return "http://restate.aerial:8080"
}

type updateActiveStepPayload struct {
	ChunkID    string `json:"chunk_id"`
	ActiveStep string `json:"active_step"`
}

// FlushChunkActiveStepToRestate forwards the active step for a running chunk to vanadis-restate BuildObject.UpdateActiveStep.
func (p *WorkerPool) FlushChunkActiveStepToRestate(ctx context.Context, buildID, chunkID, step string) error {
	trimmedChunkID := strings.TrimSpace(chunkID)
	if trimmedChunkID == "" {
		return nil
	}

	trimmedBuildID := strings.TrimSpace(buildID)
	if trimmedBuildID == "" {
		err := errors.New("missing build_id for chunk active step update")
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}

	agentName := ""
	if p != nil {
		agentName = p.cfg.AgentName
	}
	serviceName := BuildServiceName(agentName)
	ingressURL := p.resolveRestateIngressURL()
	targetURL := fmt.Sprintf("%s/%s/%s/UpdateActiveStep/send", ingressURL, serviceName, url.PathEscape(trimmedBuildID))

	payload := updateActiveStepPayload{
		ChunkID:    trimmedChunkID,
		ActiveStep: strings.TrimSpace(step),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := restateActiveStepHTTPClient.Do(req)
	if err != nil {
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			respBody = []byte(fmt.Sprintf("<error reading response: %v>", readErr))
		}
		respErr := fmt.Errorf("restate returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, respErr)
		return respErr
	}

	return nil
}

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
		extractedChunk, _ := extractVanadisDispatchInfo(&d)
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
	chunkID, buildID := extractVanadisDispatchInfo(&d)
	if chunkID != "" {
		widgetCtx := context.WithoutCancel(te.turnCtxValue())
		te.statusUpdater = NewChunkStepStatusUpdater(chunkID, func(step string) error {
			return p.FlushChunkActiveStepToRestate(widgetCtx, buildID, chunkID, step)
		})
	}
	return func() {
		if te.statusUpdater != nil {
			te.statusUpdater.Stop()
		}
		if chunkID != "" {
			widgetCtx := context.WithoutCancel(te.turnCtxValue())
			if err := p.FlushChunkActiveStepToRestate(widgetCtx, buildID, chunkID, ""); err != nil {
				log.Printf("[WorkerPool] Warning clearing active step on turn cleanup for chunk %s (build %s): %v", chunkID, buildID, err)
			}
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
