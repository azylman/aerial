package dispatch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/tracing"
)

// ThreadSessionServiceName returns the Restate service name for ThreadSession Virtual Objects.
func ThreadSessionServiceName(name string) string {
	return eriwire.ThreadSessionServiceName(name)
}

// DispatchSessionServiceName returns the Restate service name for DispatchSession Virtual Objects.
func DispatchSessionServiceName(name string) string {
	return eriwire.DispatchSessionServiceName(name)
}

type PromptDispatchRequest struct {
	Agent        string `json:"agent,omitempty"`
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

type PromptDispatcher interface {
	DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error)
	Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error)
}

type HTTPPromptDispatcher struct {
	brainURL   string
	restateURL string
	agentName  string
	httpClient *http.Client
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
		agentName = agent.Default
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

// invalidRequest marks a dispatch request that can never succeed as a terminal
// 400 so the Restate step carrying it stops retrying.
func invalidRequest(msg string) error {
	return restate.ToTerminalError(errors.New(msg), restate.WithErrorCode(http.StatusBadRequest))
}

// classifyHTTPStatus makes a rejected dispatch terminal when retrying cannot
// change the answer: any 4xx except 408 (timeout) and 429 (throttled). 409 is
// Restate's cancellation code and maps to 412 so callers never emit 409 themselves.
// 5xx and transport failures stay retryable.
func classifyHTTPStatus(status int, err error) error {
	if status < 400 || status >= 500 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests {
		return err
	}
	code := status
	if status == http.StatusConflict {
		code = http.StatusPreconditionFailed
	}
	return restate.ToTerminalError(err, restate.WithErrorCode(restate.Code(code)))
}

// restateRequestError keeps haro's historical error text for failed Restate ingress posts
// and marks permanent 4xx rejections terminal (see classifyHTTPStatus).
func restateRequestError(opLabel string, err error) error {
	if httpErr, ok := errors.AsType[*restateingress.HTTPError](err); ok {
		if httpErr.Message != "" {
			return classifyHTTPStatus(httpErr.Status, fmt.Errorf("restate %s error (%d): %s", opLabel, httpErr.Status, httpErr.Message))
		}
		return classifyHTTPStatus(httpErr.Status, fmt.Errorf("restate %s returned HTTP %d: %s", opLabel, httpErr.Status, httpErr.Body))
	}
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		if urlErr.Op == "parse" {
			return fmt.Errorf("failed to create http request: %w", err)
		}
		return fmt.Errorf("%s failed: %w", opLabel, err)
	}
	return fmt.Errorf("failed to read response body: %w", err)
}

func (d *HTTPPromptDispatcher) dispatchPromptRestate(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	channelID := strings.TrimSpace(req.ChannelID)
	if channelID == "" {
		return PromptDispatchResponse{}, invalidRequest("channel_id cannot be empty for restate prompt dispatch")
	}

	msgID := strings.TrimSpace(req.MessageID)
	if msgID == "" {
		return PromptDispatchResponse{}, invalidRequest("message_id cannot be empty for restate prompt dispatch")
	}

	agentName := req.Agent
	if strings.TrimSpace(agentName) == "" {
		agentName = d.agentName
	}
	if strings.TrimSpace(agentName) == "" {
		agentName = agent.Default
	}
	svc := ThreadSessionServiceName(agentName)
	endpoint := eriwire.RestateMethodEndpoint(d.restateURL, svc, channelID, "Enqueue")
	idempotencyKey := "prompt:" + channelID + ":" + msgID

	payload := req
	payload.ChannelID = channelID
	payload.MessageID = msgID

	respBytes, err := restateingress.Post(ctx, d.httpClient, endpoint, idempotencyKey, payload, 0)
	if err != nil {
		return PromptDispatchResponse{}, restateRequestError("prompt dispatch", err)
	}

	env, err := eriwire.ParseRestateEnqueueEnvelope(respBytes)
	if err != nil {
		return PromptDispatchResponse{}, fmt.Errorf("failed to parse prompt dispatch restate response: %w", err)
	}

	out := PromptDispatchResponse{
		Status:    eriwire.StatusAccepted,
		ChannelID: channelID,
		MessageID: msgID,
		SessionID: req.SessionID,
	}
	if env.Status != "" {
		out.Status = env.Status
	}
	if env.ChannelID != "" {
		out.ChannelID = env.ChannelID
	}
	if id := env.EffectiveID(); id != "" {
		out.MessageID = id
	}
	if env.SessionID != "" {
		out.SessionID = env.SessionID
	}
	return out, nil
}

func (d *HTTPPromptDispatcher) dispatchRestate(ctx context.Context, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	dispatchID := strings.TrimSpace(req.ID)
	if dispatchID == "" {
		return eriwire.DispatchEnqueueResponse{}, invalidRequest("id cannot be empty for restate dispatch")
	}

	keyReq := req
	keyReq.ID = dispatchID
	sessionKey := eriwire.ResolveDispatchSessionKey(keyReq)

	agentName := req.Agent
	if strings.TrimSpace(agentName) == "" {
		agentName = d.agentName
	}
	if strings.TrimSpace(agentName) == "" {
		agentName = agent.Default
	}
	svc := DispatchSessionServiceName(agentName)
	endpoint := eriwire.RestateMethodEndpoint(d.restateURL, svc, sessionKey, "Enqueue")

	payload := req
	payload.ID = dispatchID
	payload.Agent = agentName

	respBytes, err := restateingress.Post(ctx, d.httpClient, endpoint, dispatchID, payload, 0)
	if err != nil {
		return eriwire.DispatchEnqueueResponse{}, restateRequestError("dispatch", err)
	}

	env, err := eriwire.ParseRestateEnqueueEnvelope(respBytes)
	if err != nil {
		return eriwire.DispatchEnqueueResponse{}, fmt.Errorf("failed to parse dispatch restate response: %w", err)
	}

	res := eriwire.DispatchEnqueueResponse{
		Status:     eriwire.StatusAccepted,
		DispatchID: dispatchID,
		SessionID:  req.SessionID,
	}
	if env.Status != "" {
		res.Status = env.Status
	}
	if id := env.EffectiveID(); id != "" {
		res.DispatchID = id
	}
	if env.SessionID != "" {
		res.SessionID = env.SessionID
	}
	return res, nil
}

func (d *HTTPPromptDispatcher) DispatchPrompt(ctx context.Context, req PromptDispatchRequest) (PromptDispatchResponse, error) {
	if d == nil {
		return PromptDispatchResponse{}, errors.New("dispatcher is nil")
	}
	return d.dispatchPromptRestate(ctx, req)
}

func (d *HTTPPromptDispatcher) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	if d == nil {
		return eriwire.DispatchEnqueueResponse{}, errors.New("dispatcher is nil")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return eriwire.DispatchEnqueueResponse{}, invalidRequest("dispatch prompt cannot be empty")
	}
	if strings.TrimSpace(req.Source) == "" {
		return eriwire.DispatchEnqueueResponse{}, invalidRequest("dispatch source cannot be empty")
	}
	return d.dispatchRestate(ctx, req)
}
