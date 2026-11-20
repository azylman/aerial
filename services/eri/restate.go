package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// RemoteQuotaProvider fetches quota telemetry from eri-restate over HTTP.
type RemoteQuotaProvider struct {
	eriRestateURL string
	httpClient    *http.Client
}

// NewRemoteQuotaProvider creates a RemoteQuotaProvider querying eriRestateURL.
func NewRemoteQuotaProvider(eriRestateURL string, httpClient ...*http.Client) *RemoteQuotaProvider {
	eriRestateURL = strings.TrimRight(strings.TrimSpace(eriRestateURL), "/")
	if eriRestateURL == "" {
		eriRestateURL = "http://eri-restate-health:4028"
	}
	client := &http.Client{Timeout: 5 * time.Second}
	if len(httpClient) > 0 && httpClient[0] != nil {
		client = httpClient[0]
	}
	return &RemoteQuotaProvider{
		eriRestateURL: eriRestateURL,
		httpClient:    client,
	}
}

// LatestQuotaGroups calls GET /internal/quota on eri-restate, returning []metrics.QuotaGroup or empty on error.
func (p *RemoteQuotaProvider) LatestQuotaGroups() []metrics.QuotaGroup {
	if p == nil || p.httpClient == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.eriRestateURL+"/internal/quota", http.NoBody)
	if err != nil {
		return nil
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var groups []metrics.QuotaGroup
	if err := json.NewDecoder(resp.Body).Decode(&groups); err != nil {
		return nil
	}
	return groups
}

// ProviderFunc returns a function conforming to interactions.QuotaProvider.
func (p *RemoteQuotaProvider) ProviderFunc() func() []metrics.QuotaGroup {
	return p.LatestQuotaGroups
}

// NewRemoteEphemeralLLMFunc creates a runner.LLMFunc that delegates ephemeral LLM generation to eri-restate over HTTP.
func NewRemoteEphemeralLLMFunc(eriRestateURL string, httpClient ...*http.Client) runner.LLMFunc {
	eriRestateURL = strings.TrimRight(strings.TrimSpace(eriRestateURL), "/")
	if eriRestateURL == "" {
		eriRestateURL = "http://eri-restate-health:4028"
	}
	client := &http.Client{Timeout: 60 * time.Second}
	if len(httpClient) > 0 && httpClient[0] != nil {
		client = httpClient[0]
	}
	return func(ctx context.Context, model, prompt string) (string, error) {
		reqBody, err := json.Marshal(map[string]string{
			"prompt": prompt,
			"model":  model,
		})
		if err != nil {
			return "", fmt.Errorf("marshal internal llm request: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, eriRestateURL+"/internal/llm", bytes.NewReader(reqBody))
		if err != nil {
			return "", fmt.Errorf("create internal llm request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("post internal llm request: %w", err)
		}
		defer resp.Body.Close()

		const maxInternalLLMResponseBytes = 1 << 20
		respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxInternalLLMResponseBytes+1))
		if err != nil {
			return "", fmt.Errorf("read internal llm response: %w", err)
		}
		if len(respBytes) > maxInternalLLMResponseBytes {
			return "", errors.New("internal llm response body exceeds 1MiB limit")
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("internal llm returned status %d: %s", resp.StatusCode, string(respBytes))
		}
		var out struct {
			Response string `json:"response"`
		}
		if err := json.Unmarshal(respBytes, &out); err != nil {
			return "", fmt.Errorf("unmarshal internal llm response: %w", err)
		}
		return out.Response, nil
	}
}

// RestateIngressClient delivers HTTP invocations to Eri actors via Restate Ingress.
type RestateIngressClient struct {
	ingressURL string
	agentName  string
	httpClient *http.Client
}

// NewRestateIngressClient constructs a RestateIngressClient.
func NewRestateIngressClient(ingressURL, agentName string, httpClient ...*http.Client) *RestateIngressClient {
	ingressURL = strings.TrimRight(strings.TrimSpace(ingressURL), "/")
	if ingressURL == "" {
		ingressURL = "http://restate.aerial:8080"
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if len(httpClient) > 0 && httpClient[0] != nil {
		client = httpClient[0]
	}
	return &RestateIngressClient{
		ingressURL: ingressURL,
		agentName:  strings.TrimSpace(agentName),
		httpClient: client,
	}
}

func (c *RestateIngressClient) postJSON(ctx context.Context, service, key, method string, payload any, out any) error {
	if c == nil || c.httpClient == nil {
		return errors.New("restate ingress client is nil")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	var reqURL string
	if key == "" {
		reqURL = fmt.Sprintf("%s/%s/%s", c.ingressURL, url.PathEscape(service), url.PathEscape(method))
	} else {
		reqURL = fmt.Sprintf("%s/%s/%s/%s", c.ingressURL, url.PathEscape(service), url.PathEscape(key), url.PathEscape(method))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create restate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post restate request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("restate returned status %d: %s", resp.StatusCode, string(respBody))
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// EnqueueThread enqueues a message into ThreadSession via Restate Ingress.
func (c *RestateIngressClient) EnqueueThread(ctx context.Context, threadID string, req queue.ThreadEnqueueRequest) (queue.ThreadEnqueueResponse, error) {
	var resp queue.ThreadEnqueueResponse
	svcName := queue.ThreadSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, threadID, "Enqueue", req, &resp)
	return resp, err
}

// CancelThreadTurn cancels pending messages and active turns for a thread.
func (c *RestateIngressClient) CancelThreadTurn(ctx context.Context, threadID string, req queue.CancelTurnRequest) (queue.CancelTurnResponse, error) {
	var resp queue.CancelTurnResponse
	svcName := queue.ThreadSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, threadID, "CancelTurn", req, &resp)
	return resp, err
}

// RotateThreadSession rotates session context for a thread.
func (c *RestateIngressClient) RotateThreadSession(ctx context.Context, threadID string, req queue.RotateSessionRequest) (queue.RotateSessionResponse, error) {
	var resp queue.RotateSessionResponse
	svcName := queue.ThreadSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, threadID, "RotateSession", req, &resp)
	return resp, err
}

// EnqueueDispatch enqueues a dispatch request into DispatchSession via Restate Ingress.
func (c *RestateIngressClient) EnqueueDispatch(ctx context.Context, sessionKey string, req queue.BrainDispatchRequest) (queue.DispatchEnqueueResponse, error) {
	var resp queue.DispatchEnqueueResponse
	svcName := queue.DispatchSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, sessionKey, "Enqueue", req, &resp)
	return resp, err
}

const (
	statusSkippedActive = "skipped_active"
	statusAccepted      = "accepted"
)

// DispatchAccepted enqueues a dispatch and requires acceptance, translating skipped_active into queue.ErrDispatchActive.
func (c *RestateIngressClient) DispatchAccepted(ctx context.Context, req queue.BrainDispatchRequest) error {
	if strings.TrimSpace(req.ID) == "" && strings.TrimSpace(req.SessionID) == "" && strings.TrimSpace(req.VanadisChunkID) == "" {
		return errors.New("dispatch id, session id, or chunk id is required")
	}
	sessionKey := queue.ResolveDispatchSessionKey(req)
	resp, err := c.EnqueueDispatch(ctx, sessionKey, req)
	if err != nil {
		return err
	}
	if resp.Status == statusSkippedActive {
		return queue.ErrDispatchActive
	}
	if req.ID != "" && resp.DispatchID != req.ID {
		return fmt.Errorf("dispatch ID mismatch: expected %s, got %s", req.ID, resp.DispatchID)
	}
	if resp.Status != statusAccepted && resp.Status != "already_exists" {
		return fmt.Errorf("dispatch rejected with status: %s", resp.Status)
	}
	return nil
}

// CancelDispatch cancels dispatches matching sessionKey/chunkID via Restate Ingress.
func (c *RestateIngressClient) CancelDispatch(ctx context.Context, sessionKey string, req queue.CancelDispatchRequest) (queue.CancelDispatchResponse, error) {
	var resp queue.CancelDispatchResponse
	svcName := queue.DispatchSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, sessionKey, "Cancel", req, &resp)
	return resp, err
}

// AskVoice executes a synchronous voice turn via Restate Ingress.
func (c *RestateIngressClient) AskVoice(ctx context.Context, deviceKey string, req queue.VoiceSessionRequest) (queue.VoiceSessionResult, error) {
	var resp queue.VoiceSessionResult
	svcName := queue.VoiceSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, deviceKey, "Ask", req, &resp)
	return resp, err
}

// CancelThreadImmediate delivers cancellation to the unkeyed SessionManager service to preempt running turns without key queuing.
func (c *RestateIngressClient) CancelThreadImmediate(ctx context.Context, threadID string, req queue.CancelThreadControlRequest) (queue.CancelTurnResponse, error) {
	var resp queue.CancelTurnResponse
	if req.ThreadID == "" {
		req.ThreadID = threadID
	}
	svcName := queue.SessionManagerServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, "", "CancelThread", req, &resp)
	return resp, err
}

// CancelDispatchImmediate delivers cancellation to the unkeyed SessionManager service to preempt running dispatches without key queuing.
func (c *RestateIngressClient) CancelDispatchImmediate(ctx context.Context, req queue.CancelDispatchControlRequest) (queue.CancelDispatchResponse, error) {
	var resp queue.CancelDispatchResponse
	svcName := queue.SessionManagerServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, "", "CancelDispatch", req, &resp)
	return resp, err
}

// darkLaunchRestateAnchor preserves dark-launched Restate actors and ingress client from deadcode analysis
// within the services/eri module while their runtime server registration lives in services/eri-restate.
var _ = []any{
	restate.Reflect(queue.NewThreadSessionObject(nil, "")),
	restate.Reflect(queue.NewDispatchSessionObject(nil, "")),
	restate.Reflect(queue.NewVoiceSessionObject(nil, "")),
	restate.Reflect(queue.NewSessionManagerService(nil, "")),
	NewRemoteQuotaProvider,
	(*RemoteQuotaProvider).LatestQuotaGroups,
	(*RemoteQuotaProvider).ProviderFunc,
	NewRemoteEphemeralLLMFunc,
	NewRestateIngressClient,
	(*RestateIngressClient).postJSON,
	(*RestateIngressClient).EnqueueThread,
	(*RestateIngressClient).CancelThreadTurn,
	(*RestateIngressClient).CancelThreadImmediate,
	(*RestateIngressClient).RotateThreadSession,
	(*RestateIngressClient).EnqueueDispatch,
	(*RestateIngressClient).DispatchAccepted,
	(*RestateIngressClient).CancelDispatch,
	(*RestateIngressClient).CancelDispatchImmediate,
	(*RestateIngressClient).AskVoice,
}
