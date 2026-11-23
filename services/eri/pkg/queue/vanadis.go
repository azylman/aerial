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
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

var restateActiveStepHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// vanadisHTTPClient returns the injected client or a ServiceAccount-authenticated
// client reading PoolConfig.SATokenPath (wired by main).
func (p *WorkerPool) vanadisHTTPClient() *http.Client {
	if p != nil && p.cfg.VanadisHTTPClient != nil {
		return p.cfg.VanadisHTTPClient
	}
	tokenPath := ""
	if p != nil {
		tokenPath = p.cfg.SATokenPath
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: k8sauth.NewServiceAccountTransport(nil, tokenPath),
	}
}

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// With vanadis-restate consolidated into a single agent-aware instance,
// this unconditionally returns the canonical "Build" Virtual Object name.
func BuildServiceName(_ string) string {
	return "Build"
}

// resolveRestateIngressURL returns PoolConfig.RestateIngressURL without a trailing slash.
func (p *WorkerPool) resolveRestateIngressURL() string {
	if p == nil {
		return ""
	}
	return strings.TrimRight(strings.TrimSpace(p.cfg.RestateIngressURL), "/")
}

// resolveVanadisAPIURL returns PoolConfig.VanadisURL, else the live vanadis_url
// from the app config snapshot, else "" (unconfigured).
func (p *WorkerPool) resolveVanadisAPIURL() string {
	if p == nil {
		return ""
	}
	if u := strings.TrimSpace(p.cfg.VanadisURL); u != "" {
		return strings.TrimRight(u, "/")
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			return strings.TrimRight(strings.TrimSpace(cur.VanadisURL), "/")
		}
	}
	return ""
}

type updateActiveStepPayload struct {
	ChunkID    string `json:"chunk_id"`
	ActiveStep string `json:"active_step"`
}

func (p *WorkerPool) flushActiveStepToRestateIngress(ctx context.Context, buildID string, body []byte) error {
	agentName := ""
	if p != nil {
		agentName = p.cfg.AgentName
	}
	serviceName := BuildServiceName(agentName)
	ingressURL := p.resolveRestateIngressURL()
	targetURL := fmt.Sprintf("%s/%s/%s/UpdateActiveStep", ingressURL, serviceName, url.PathEscape(buildID))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := restateActiveStepHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			respBody = []byte(fmt.Sprintf("<error reading response: %v>", readErr))
		}
		return fmt.Errorf("restate returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (p *WorkerPool) flushActiveStepToVanadisAPI(ctx context.Context, vanadisURL, buildID string, body []byte) error {
	var targetURL string
	if trimmed := strings.TrimSpace(buildID); trimmed != "" {
		targetURL = fmt.Sprintf("%s/builds/%s/active-step", vanadisURL, url.PathEscape(trimmed))
	} else {
		targetURL = vanadisURL + "/chunks/active-step"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.vanadisHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			respBody = []byte(fmt.Sprintf("<error reading response: %v>", readErr))
		}
		return fmt.Errorf("vanadis returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// FlushChunkActiveStepToRestate forwards the active step for a running chunk.
// When RestateIngressURL is explicitly configured on PoolConfig and VanadisURL is empty,
// it falls back to direct Restate ingress (<ingressURL>/<BuildServiceName>/<buildID>/UpdateActiveStep)
// for backward compatibility and hermetic unit tests. Otherwise, it routes through the vanadis
// HTTP API (<vanadisURL>/builds/<buildID>/active-step or <vanadisURL>/chunks/active-step) using a ServiceAccount Bearer token.
// With neither URL configured the update is skipped (logged once); it never dials a default host.
func (p *WorkerPool) FlushChunkActiveStepToRestate(ctx context.Context, buildID, chunkID, step string) error {
	trimmedChunkID := strings.TrimSpace(chunkID)
	if trimmedChunkID == "" {
		return nil
	}

	trimmedBuildID := strings.TrimSpace(buildID)
	isDirectRestate := p != nil && strings.TrimSpace(p.cfg.RestateIngressURL) != "" && strings.TrimSpace(p.cfg.VanadisURL) == ""
	if isDirectRestate && trimmedBuildID == "" {
		err := errors.New("missing build_id for chunk active step update")
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}

	payload := updateActiveStepPayload{
		ChunkID:    trimmedChunkID,
		ActiveStep: strings.TrimSpace(step),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, err)
		return err
	}

	if isDirectRestate {
		if flushErr := p.flushActiveStepToRestateIngress(ctx, trimmedBuildID, body); flushErr != nil {
			log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, flushErr)
			return flushErr
		}
		return nil
	}

	vanadisURL := p.resolveVanadisAPIURL()
	if vanadisURL == "" {
		if p != nil && p.vanadisURLWarned.CompareAndSwap(false, true) {
			log.Printf("[WorkerPool] Warning: vanadis_url is not configured; skipping chunk active-step updates")
		}
		return nil
	}
	if flushErr := p.flushActiveStepToVanadisAPI(ctx, vanadisURL, trimmedBuildID, body); flushErr != nil {
		log.Printf("[WorkerPool] Warning flushing active step for chunk %s (build %s): %v", chunkID, buildID, flushErr)
		return flushErr
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
