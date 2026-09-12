package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

// WakeRequest contains payload data sent to the on_wake ingress webhook.
type WakeRequest struct {
	ChannelID string     `json:"channel_id"`
	ThreadID  string     `json:"thread_id"`
	Message   db.Message `json:"message"`
	Timestamp time.Time  `json:"timestamp"`
}

// WakeResponse contains routing instructions from the on_wake ingress webhook.
type WakeResponse struct {
	Override string `json:"override"` // "wake", "drop", "classify"
	Reason   string `json:"reason,omitempty"`
}

// PreTurnRequest contains execution gate details sent to the pre_turn webhook.
type PreTurnRequest struct {
	ChannelID   string        `json:"channel_id"`
	ThreadID    string        `json:"thread_id"`
	BurstCount  int           `json:"burst_count"`
	Messages    []*db.Message `json:"messages"`
	Prompt      string        `json:"prompt"`
	RetryCount  int           `json:"retry_count"`
	Timestamp   time.Time     `json:"timestamp"`
}

// PreTurnResponse contains execution permission and context from the pre_turn webhook.
type PreTurnResponse struct {
	Allow             bool           `json:"allow"`
	Action            string         `json:"action,omitempty"` // "proceed", "drop", "retry"
	RetryAfterSeconds int            `json:"retry_after_seconds,omitempty"`
	InjectedContext   string         `json:"injected_context,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	Reason            string         `json:"reason,omitempty"`
}

// PostTurnRequest contains turn completion telemetry sent to the post_turn webhook.
type PostTurnRequest struct {
	ChannelID    string            `json:"channel_id"`
	ThreadID     string            `json:"thread_id"`
	Status       string            `json:"status"` // "success", "failed", "timeout"
	ResponseText string            `json:"response_text,omitempty"`
	Error        string            `json:"error,omitempty"`
	DurationMs   int64             `json:"duration_ms"`
	TokenUsage   runner.TokenUsage `json:"token_usage"`
	Metadata     map[string]any    `json:"metadata,omitempty"`
	Timestamp    time.Time         `json:"timestamp"`
}

// PostTurnResponse contains post_turn acknowledgement and optional state metadata.
type PostTurnResponse struct {
	Acknowledged bool           `json:"acknowledged"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// WebhookDispatcher defines the interface for channel lifecycle webhook interactions.
type WebhookDispatcher interface {
	CallWakeHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error)
	CallPreTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error)
	CallPostTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error)
}

// DefaultWebhookDispatcher implements WebhookDispatcher using pooled net/http clients.
type DefaultWebhookDispatcher struct {
	client *http.Client
}

var _ WebhookDispatcher = (*DefaultWebhookDispatcher)(nil)

// NewDefaultWebhookDispatcher creates a WebhookDispatcher configured with connection pooling.
func NewDefaultWebhookDispatcher() *DefaultWebhookDispatcher {
	return NewWebhookDispatcherWithClient(nil)
}

// NewWebhookDispatcherWithClient creates a WebhookDispatcher with a caller-provided http.Client.
func NewWebhookDispatcherWithClient(client *http.Client) *DefaultWebhookDispatcher {
	if client == nil {
		client = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
	return &DefaultWebhookDispatcher{client: client}
}

// CallWakeHook dispatches the on_wake webhook event.
func (d *DefaultWebhookDispatcher) CallWakeHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *WakeRequest) (*WakeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("wake request is nil")
	}
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now().UTC()
	}
	var resp WakeResponse
	if err := d.dispatch(ctx, "on_wake", endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CallPreTurnHook dispatches the pre_turn webhook event.
func (d *DefaultWebhookDispatcher) CallPreTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PreTurnRequest) (*PreTurnResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("pre_turn request is nil")
	}
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now().UTC()
	}
	var resp PreTurnResponse
	if err := d.dispatch(ctx, "pre_turn", endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CallPostTurnHook dispatches the post_turn webhook event.
func (d *DefaultWebhookDispatcher) CallPostTurnHook(ctx context.Context, endpoint *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("post_turn request is nil")
	}
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now().UTC()
	}
	var resp PostTurnResponse
	if err := d.dispatch(ctx, "post_turn", endpoint, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *DefaultWebhookDispatcher) dispatch(ctx context.Context, hook string, endpoint *config.WebhookEndpoint, payload any, respDest any) (err error) {
	if d == nil {
		return fmt.Errorf("webhook dispatcher is nil")
	}
	if endpoint == nil {
		return fmt.Errorf("webhook endpoint is nil")
	}
	url := strings.TrimSpace(endpoint.URL)
	if url == "" {
		return fmt.Errorf("webhook endpoint url is empty")
	}
	if payload == nil {
		return fmt.Errorf("webhook request payload is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	timeout := endpoint.GetTimeout()
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	defer func() {
		status := "success"
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				status = "timeout"
			} else if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				status = "canceled"
			} else {
				status = "error"
			}
		}
		metrics.RecordWebhookDispatch(hook, status, time.Since(start))
	}()

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "aerial-brain/1.0")
	httpReq.Header.Set("X-Aerial-Event", hook)

	client := d.client
	if client == nil {
		client = http.DefaultClient
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("webhook dispatch timed out after %v: %w", timeout, timeoutCtx.Err())
		}
		return fmt.Errorf("webhook http request failed: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 2048))
		return fmt.Errorf("webhook endpoint returned status %d: %s", httpResp.StatusCode, string(respBody))
	}

	respBytes, err := io.ReadAll(io.LimitReader(httpResp.Body, 10*1024*1024))
	if err != nil {
		return fmt.Errorf("failed to read webhook response body: %w", err)
	}

	if respDest != nil {
		if err := json.Unmarshal(respBytes, respDest); err != nil {
			return fmt.Errorf("failed to unmarshal webhook response json: %w", err)
		}
	}

	return nil
}
