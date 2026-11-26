package restateclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/llmcall"
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

// LatestQuotaSnapshot calls GET /internal/quota on eri-restate, returning a zero snapshot on error.
func (p *RemoteQuotaProvider) LatestQuotaSnapshot() metrics.QuotaSnapshot {
	if p == nil || p.httpClient == nil {
		return metrics.QuotaSnapshot{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.eriRestateURL+"/internal/quota", http.NoBody)
	if err != nil {
		return metrics.QuotaSnapshot{}
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return metrics.QuotaSnapshot{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return metrics.QuotaSnapshot{}
	}
	var snap metrics.QuotaSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return metrics.QuotaSnapshot{}
	}
	return snap
}

// ProviderFunc returns a function conforming to interactions.QuotaProvider.
func (p *RemoteQuotaProvider) ProviderFunc() func() metrics.QuotaSnapshot {
	return p.LatestQuotaSnapshot
}

// llmCallTimeout bounds one CompleteLLM wait. The Restate invocation itself
// keeps running past it; a retry with the same Idempotency-Key attaches.
const llmCallTimeout = 10 * time.Minute

// maxLLMResponseBytes caps the CompleteLLM response body.
const maxLLMResponseBytes = 1 << 20

// RestateIngressClient delivers HTTP invocations to Eri actors via Restate Ingress.
type RestateIngressClient struct {
	ingressURL string
	agentName  string
	httpClient *http.Client
	// llmClient serves CompleteLLM, whose calls outlast httpClient's timeout.
	llmClient *http.Client
}

var activeRestateIngressClient atomic.Pointer[RestateIngressClient]

// ActiveRestateIngressClient returns the global active RestateIngressClient instance, or nil.
func ActiveRestateIngressClient() *RestateIngressClient {
	return activeRestateIngressClient.Load()
}

// SwapActiveRestateIngressClient atomically swaps the active RestateIngressClient with c and returns the previous client.
func SwapActiveRestateIngressClient(c *RestateIngressClient) *RestateIngressClient {
	return activeRestateIngressClient.Swap(c)
}

// StoreActiveRestateIngressClient atomically stores c as the active RestateIngressClient.
func StoreActiveRestateIngressClient(c *RestateIngressClient) {
	activeRestateIngressClient.Store(c)
}

// ResolveRestateClient returns the first non-nil client in clients, or ActiveRestateIngressClient().
func ResolveRestateClient(clients ...*RestateIngressClient) *RestateIngressClient {
	if len(clients) > 0 && clients[0] != nil {
		return clients[0]
	}
	return ActiveRestateIngressClient()
}

// NewRestateIngressClient constructs a RestateIngressClient.
func NewRestateIngressClient(ingressURL, agentName string, httpClient ...*http.Client) *RestateIngressClient {
	ingressURL = strings.TrimRight(strings.TrimSpace(ingressURL), "/")
	if ingressURL == "" {
		ingressURL = "http://restate.aerial:8080"
	}
	baseClient := &http.Client{Timeout: 30 * time.Second}
	if len(httpClient) > 0 && httpClient[0] != nil {
		baseClient = httpClient[0]
	}
	client := tracing.Client(baseClient)
	llmClient := tracing.Client(&http.Client{Transport: baseClient.Transport, Timeout: llmCallTimeout})
	return &RestateIngressClient{
		ingressURL: ingressURL,
		agentName:  strings.TrimSpace(agentName),
		httpClient: client,
		llmClient:  llmClient,
	}
}

// CompleteLLM runs req as one invocation of the Restate LLM service and returns
// the model's response. A non-blank llmcall.IdempotencyKey(req) is sent as the
// Idempotency-Key, so a retry of the same logical call attaches to the running
// or finished invocation. Cancelling ctx only abandons the wait. A non-2xx
// Restate status is returned wrapping *restateingress.HTTPError.
func (c *RestateIngressClient) CompleteLLM(ctx context.Context, req llmcall.Request) (string, error) {
	if c == nil || c.llmClient == nil {
		return "", errors.New("restate ingress client is nil")
	}
	endpoint := fmt.Sprintf("%s/%s/Complete", c.ingressURL, url.PathEscape(llmcall.ServiceName(c.agentName)))
	body, err := restateingress.Post(ctx, c.llmClient, endpoint, llmcall.IdempotencyKey(req), req, maxLLMResponseBytes+1)
	if err != nil {
		return "", fmt.Errorf("restate llm call: %w", err)
	}
	if len(body) > maxLLMResponseBytes {
		return "", errors.New("restate llm response body exceeds 1MiB limit")
	}
	var out llmcall.Response
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("unmarshal restate llm response: %w", err)
	}
	return out.Response, nil
}

// NewRestateLLMFunc returns a runner.LLMFunc that runs each call through the
// Restate LLM service, keyed by the caller key the call site put on ctx with
// runner.WithLLMCallerKey (unkeyed when none was set). The caller ctx bounds
// only the HTTP wait.
func NewRestateLLMFunc(client *RestateIngressClient) runner.LLMFunc {
	return func(ctx context.Context, model, prompt string) (string, error) {
		return client.CompleteLLM(ctx, llmcall.Request{Prompt: prompt, Model: model, CallerKey: runner.LLMCallerKey(ctx)})
	}
}

func (c *RestateIngressClient) postJSON(ctx context.Context, service, key, method string, payload any, out any, idempotencyKey ...string) error {
	if c == nil || c.httpClient == nil {
		return errors.New("restate ingress client is nil")
	}
	reqURL := eriwire.RestateMethodEndpoint(c.ingressURL, service, key, method)
	idemKey := ""
	if len(idempotencyKey) > 0 {
		idemKey = idempotencyKey[0]
	}
	respBody, err := restateingress.Post(ctx, c.httpClient, reqURL, idemKey, payload, 0)
	if err != nil {
		if httpErr, ok := errors.AsType[*restateingress.HTTPError](err); ok {
			return fmt.Errorf("restate returned status %d: %s", httpErr.Status, httpErr.Body)
		}
		return fmt.Errorf("post restate request: %w", err)
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// EnqueueThread enqueues a message into ThreadSession via Restate Ingress.
func (c *RestateIngressClient) EnqueueThread(ctx context.Context, threadID string, req queue.ThreadEnqueueRequest, idempotencyKey ...string) (queue.ThreadEnqueueResponse, error) {
	var resp queue.ThreadEnqueueResponse
	svcName := eriwire.ThreadSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, threadID, "Enqueue", req, &resp, idempotencyKey...)
	return resp, err
}

// CancelThreadTurn cancels pending messages and active turns for a thread.
func (c *RestateIngressClient) CancelThreadTurn(ctx context.Context, threadID string, req queue.CancelTurnRequest) (queue.CancelTurnResponse, error) {
	var resp queue.CancelTurnResponse
	svcName := eriwire.ThreadSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, threadID, "CancelTurn", req, &resp)
	return resp, err
}

// EnqueueDispatch enqueues a dispatch request into DispatchSession via Restate Ingress.
func (c *RestateIngressClient) EnqueueDispatch(ctx context.Context, sessionKey string, req eriwire.BrainDispatchRequest, idempotencyKey ...string) (eriwire.DispatchEnqueueResponse, error) {
	var resp eriwire.DispatchEnqueueResponse
	svcName := eriwire.DispatchSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, sessionKey, "Enqueue", req, &resp, idempotencyKey...)
	return resp, err
}

// AskVoice executes a synchronous voice turn via Restate Ingress.
func (c *RestateIngressClient) AskVoice(ctx context.Context, deviceKey string, req queue.VoiceSessionRequest) (queue.VoiceSessionResult, error) {
	var resp queue.VoiceSessionResult
	svcName := eriwire.VoiceSessionServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, deviceKey, "Ask", req, &resp)
	return resp, err
}

// CancelDispatchImmediate delivers cancellation to the unkeyed SessionManager service to preempt running dispatches without key queuing.
func (c *RestateIngressClient) CancelDispatchImmediate(ctx context.Context, req queue.CancelDispatchControlRequest) (queue.CancelDispatchResponse, error) {
	var resp queue.CancelDispatchResponse
	svcName := eriwire.SessionManagerServiceName(c.agentName)
	err := c.postJSON(ctx, svcName, "", "CancelDispatch", req, &resp)
	return resp, err
}
