package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

type PromptDispatchRequest struct {
	Prompt       string `json:"prompt"`
	ChannelID    string `json:"channel_id,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
	Effort       string `json:"effort,omitempty"`
	Silent       bool   `json:"silent,omitempty"`
	Ephemeral    *bool  `json:"ephemeral,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

type PromptDispatchResponse struct {
	Status    string `json:"status"`
	ChannelID string `json:"channel_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
}

type DispatchRequest struct {
	ID           string `json:"id,omitempty"`
	Source       string `json:"source"`
	SessionID    string `json:"session_id,omitempty"`
	TargetID     string `json:"target_id,omitempty"`
	Prompt       string `json:"prompt"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

type DispatchResponse struct {
	Status     string `json:"status"`
	DispatchID string `json:"dispatch_id,omitempty"`
	MessageID  string `json:"message_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

type PromptDispatcher interface {
	DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error)
	Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error)
}

type HTTPPromptDispatcher struct {
	brainURL   string
	httpClient *http.Client
	busMu      sync.RWMutex
	bus        pubsub.Bus
}

func (d *HTTPPromptDispatcher) SetEventBus(bus pubsub.Bus) {
	if d != nil {
		d.busMu.Lock()
		d.bus = bus
		d.busMu.Unlock()
	}
}

func (d *HTTPPromptDispatcher) EventBus() pubsub.Bus {
	return d.getBus()
}

func (d *HTTPPromptDispatcher) getBus() pubsub.Bus {
	if d == nil {
		return nil
	}
	d.busMu.RLock()
	defer d.busMu.RUnlock()
	return d.bus
}

func NewHTTPPromptDispatcher(brainURL string) *HTTPPromptDispatcher {
	trimmed := strings.TrimRight(strings.TrimSpace(brainURL), "/")
	if trimmed == "" {
		panic("ticker: brainURL cannot be empty")
	}
	return &HTTPPromptDispatcher{
		brainURL: trimmed,
		// Traced so each eri turn joins the trace of the schedule or trigger that fired it.
		httpClient: tracing.Client(&http.Client{
			Timeout: 15 * time.Second,
		}),
	}
}

// TraceFire runs one schedule, heartbeat or event-trigger dispatch under its own span, so the
// eri turn it starts joins a trace that begins where the work began. Inside an inbound request
// (event triggers) the span continues the caller's trace. Only the schedule type and id are
// recorded; dispatch errors set a generic status since their text can echo eri responses.
func TraceFire[R any](ctx context.Context, scheduleType, scheduleID string, dispatch func(context.Context) (R, error)) (R, error) {
	ctx, span := tracing.Tracer().Start(ctx, "haro.fire "+scheduleType,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("haro.schedule.type", scheduleType),
			attribute.String("haro.schedule.id", scheduleID),
		),
	)
	defer span.End()
	resp, err := dispatch(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "dispatch failed")
	}
	return resp, err
}

func postBrainJSON[Req any, Resp any](ctx context.Context, d *HTTPPromptDispatcher, path, opLabel string, req Req) (Resp, error) {
	var zero Resp
	if d == nil {
		return zero, fmt.Errorf("dispatcher is nil")
	}
	brainURL := strings.TrimRight(strings.TrimSpace(d.brainURL), "/")
	if brainURL == "" {
		return zero, fmt.Errorf("brain URL is not configured")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("failed to marshal %s request: %w", opLabel, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL+path, bytes.NewReader(body))
	if err != nil {
		return zero, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := d.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return zero, fmt.Errorf("%s failed: %w", opLabel, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp struct {
			Error string `json:"error"`
		}
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && strings.TrimSpace(errResp.Error) != "" {
			return zero, fmt.Errorf("brain %s error (%d): %s", path, resp.StatusCode, errResp.Error)
		}
		return zero, fmt.Errorf("brain %s returned HTTP %d: %s", path, resp.StatusCode, string(respBody))
	}

	var dispatchResp Resp
	if err := json.Unmarshal(respBody, &dispatchResp); err != nil {
		return zero, fmt.Errorf("failed to parse %s response: %w", opLabel, err)
	}

	return dispatchResp, nil
}

func (d *HTTPPromptDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	return postBrainJSON[PromptDispatchRequest, PromptDispatchResponse](ctx, d, "/prompt", "prompt dispatch", req)
}

func (d *HTTPPromptDispatcher) Dispatch(ctx context.Context, req DispatchRequest) (DispatchResponse, error) {
	if d == nil {
		return DispatchResponse{}, errors.New("dispatcher is nil")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return DispatchResponse{}, errors.New("dispatch prompt cannot be empty")
	}
	if strings.TrimSpace(req.Source) == "" {
		return DispatchResponse{}, errors.New("dispatch source cannot be empty")
	}
	bus := d.getBus()
	if bus == nil {
		return DispatchResponse{}, errors.New("event bus is not configured")
	}
	dispatchID := strings.TrimSpace(req.ID)
	if dispatchID == "" {
		dispatchID = uuid.New().String()
	}
	args := pubsub.EriDispatchJobArgs{
		ID:           dispatchID,
		Source:       req.Source,
		SessionID:    req.SessionID,
		TargetID:     req.TargetID,
		Prompt:       req.Prompt,
		Effort:       req.Effort,
		SkipIfActive: req.SkipIfActive,
	}
	msgID := "eri:dispatch:" + dispatchID
	if err := bus.Publish(ctx, pubsub.SubjectEriDispatchJob, msgID, args); err != nil {
		return DispatchResponse{}, err
	}
	return DispatchResponse{
		Status:     "accepted",
		DispatchID: dispatchID,
		MessageID:  dispatchID,
		SessionID:  req.SessionID,
	}, nil
}
