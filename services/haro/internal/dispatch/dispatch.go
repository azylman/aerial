package dispatch

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
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

const (
	defaultAgentName = "aerial"
	statusAccepted   = "accepted"
)

// NormalizeAgentName trims and lowercases an agent name.
func NormalizeAgentName(agent string) string {
	return strings.ToLower(strings.TrimSpace(agent))
}

// ThreadSessionServiceName returns the Restate service name for ThreadSession Virtual Objects.
func ThreadSessionServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == defaultAgentName {
		return "ThreadSession"
	}
	return "ThreadSession-" + name
}

// DispatchSessionServiceName returns the Restate service name for DispatchSession Virtual Objects.
func DispatchSessionServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == defaultAgentName {
		return "DispatchSession"
	}
	return "DispatchSession-" + name
}

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
	restateURL string
	agentName  string
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

func NewHTTPPromptDispatcher(brainURL string, restateOpts ...string) *HTTPPromptDispatcher {
	trimmed := strings.TrimRight(strings.TrimSpace(brainURL), "/")
	if trimmed == "" {
		panic("ticker: brainURL cannot be empty")
	}
	var restateURL, agentName string
	if len(restateOpts) > 0 {
		restateURL = strings.TrimRight(strings.TrimSpace(restateOpts[0]), "/")
	}
	if len(restateOpts) > 1 {
		agentName = strings.TrimSpace(restateOpts[1])
	}
	if agentName == "" {
		agentName = defaultAgentName
	}
	return &HTTPPromptDispatcher{
		brainURL:   trimmed,
		restateURL: restateURL,
		agentName:  agentName,
		// Traced so each eri turn joins the trace of the schedule or trigger that fired it.
		httpClient: tracing.Client(&http.Client{
			Timeout: 15 * time.Second,
		}),
	}
}

func (d *HTTPPromptDispatcher) SetRestateConfig(restateURL, agentName string) {
	if d != nil {
		d.restateURL = strings.TrimRight(strings.TrimSpace(restateURL), "/")
		agent := strings.TrimSpace(agentName)
		if agent == "" {
			agent = defaultAgentName
		}
		d.agentName = agent
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

func parseRestateHTTPError(opLabel string, statusCode int, respBody []byte) error {
	var errResp struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil {
		if msg := strings.TrimSpace(errResp.Message); msg != "" {
			return fmt.Errorf("restate %s error (%d): %s", opLabel, statusCode, msg)
		}
		if errStr := strings.TrimSpace(errResp.Error); errStr != "" {
			return fmt.Errorf("restate %s error (%d): %s", opLabel, statusCode, errStr)
		}
	}
	return fmt.Errorf("restate %s returned HTTP %d: %s", opLabel, statusCode, string(respBody))
}

func (d *HTTPPromptDispatcher) postRestateJSON(ctx context.Context, endpoint, idempotencyKey, opLabel string, req any) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("dispatcher is nil")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal %s request: %w", opLabel, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if key := strings.TrimSpace(idempotencyKey); key != "" {
		httpReq.Header.Set("Idempotency-Key", key)
	}

	client := d.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w", opLabel, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseRestateHTTPError(opLabel, resp.StatusCode, respBody)
	}

	return respBody, nil
}

type restateEnvelopeFields struct {
	status    string
	channelID string
	messageID string
	sessionID string
}

func parseRestateEnvelope(respBytes []byte) (restateEnvelopeFields, error) {
	if len(respBytes) == 0 {
		return restateEnvelopeFields{}, nil
	}
	var raw struct {
		Status     string `json:"status"`
		ChannelID  string `json:"channel_id"`
		DispatchID string `json:"dispatch_id"`
		MessageID  string `json:"message_id"`
		SessionID  string `json:"session_id"`
	}
	if err := json.Unmarshal(respBytes, &raw); err != nil {
		return restateEnvelopeFields{}, err
	}
	msgID := raw.MessageID
	if msgID == "" {
		msgID = raw.DispatchID
	}
	return restateEnvelopeFields{
		status:    raw.Status,
		channelID: raw.ChannelID,
		messageID: msgID,
		sessionID: raw.SessionID,
	}, nil
}

func (d *HTTPPromptDispatcher) dispatchPromptRestate(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	channelID := strings.TrimSpace(req.ChannelID)
	if channelID == "" {
		return PromptDispatchResponse{}, errors.New("channel_id cannot be empty for restate prompt dispatch")
	}

	msgID := strings.TrimSpace(req.MessageID)
	if msgID == "" {
		msgID = uuid.New().String()
	}

	svc := ThreadSessionServiceName(d.agentName)
	endpoint := d.restateURL + "/" + svc + "/" + url.PathEscape(channelID) + "/Enqueue"
	idempotencyKey := "prompt:" + channelID + ":" + msgID

	respBytes, err := d.postRestateJSON(ctx, endpoint, idempotencyKey, "prompt dispatch", req)
	if err != nil {
		return PromptDispatchResponse{}, err
	}

	fields, err := parseRestateEnvelope(respBytes)
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to parse prompt dispatch restate response: %w", err)
	}

	out := PromptDispatchResponse{
		Status:    statusAccepted,
		ChannelID: channelID,
		MessageID: msgID,
		SessionID: req.SessionID,
	}
	if fields.status != "" {
		out.Status = fields.status
	}
	if fields.channelID != "" {
		out.ChannelID = fields.channelID
	}
	if fields.messageID != "" {
		out.MessageID = fields.messageID
	}
	if fields.sessionID != "" {
		out.SessionID = fields.sessionID
	}
	return out, nil
}

func (d *HTTPPromptDispatcher) dispatchRestate(ctx context.Context, req DispatchRequest) (DispatchResponse, error) {
	dispatchID := strings.TrimSpace(req.ID)
	if dispatchID == "" {
		dispatchID = uuid.New().String()
	}

	sessionKey := strings.TrimSpace(req.SessionID)
	if sessionKey == "" {
		sessionKey = "dispatch-" + dispatchID
	}

	endpoint := fmt.Sprintf("%s/%s/%s/Enqueue", d.restateURL, DispatchSessionServiceName(d.agentName), url.PathEscape(sessionKey))

	raw := map[string]any{
		"id":             dispatchID,
		"source":         req.Source,
		"session_id":     req.SessionID,
		"target_id":      req.TargetID,
		"prompt":         req.Prompt,
		"effort":         req.Effort,
		"skip_if_active": req.SkipIfActive,
	}

	respBytes, err := d.postRestateJSON(ctx, endpoint, dispatchID, "dispatch", raw)
	if err != nil {
		return DispatchResponse{}, err
	}

	fields, err := parseRestateEnvelope(respBytes)
	if err != nil {
		return DispatchResponse{}, fmt.Errorf("failed to parse dispatch restate response: %w", err)
	}

	res := DispatchResponse{
		Status:     statusAccepted,
		DispatchID: dispatchID,
		MessageID:  dispatchID,
		SessionID:  req.SessionID,
	}
	if fields.status != "" {
		res.Status = fields.status
	}
	if fields.messageID != "" {
		res.DispatchID = fields.messageID
		res.MessageID = fields.messageID
	}
	if fields.sessionID != "" {
		res.SessionID = fields.sessionID
	}
	return res, nil
}

func (d *HTTPPromptDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	if d != nil && strings.TrimSpace(d.restateURL) != "" {
		return d.dispatchPromptRestate(ctx, req)
	}
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
	if strings.TrimSpace(d.restateURL) != "" {
		return d.dispatchRestate(ctx, req)
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
