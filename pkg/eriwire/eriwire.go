// Package eriwire holds the wire types shared by eri and its dispatch callers.
package eriwire

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

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/tracing"
)

// Effort level constants accepted across eri prompts and dispatch calls.
const (
	EffortLow    = "low"
	EffortMedium = "medium"
	EffortHigh   = "high"
)

// ParseEffort validates raw effort against low/medium/high (case-insensitive and trimmed),
// returning the matched constant or fallback if unrecognized.
func ParseEffort(raw, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case EffortLow:
		return EffortLow
	case EffortMedium:
		return EffortMedium
	case EffortHigh:
		return EffortHigh
	default:
		return fallback
	}
}

type llmKeyCtx struct{}

// WithLLMKey attaches an Idempotency-Key identity to ctx for downstream LLM calls.
func WithLLMKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, llmKeyCtx{}, key)
}

// LLMKeyFrom extracts an Idempotency-Key identity from ctx if present.
func LLMKeyFrom(ctx context.Context) string {
	key, ok := ctx.Value(llmKeyCtx{}).(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(key)
}

// InternalLLMRequest encapsulates a prompt request to eri's /internal/llm endpoint.
type InternalLLMRequest struct {
	Prompt string `json:"prompt"`
	Effort string `json:"effort,omitempty"`
}

// InternalLLMResponse represents the completion reply from eri's /internal/llm endpoint.
type InternalLLMResponse struct {
	Response string `json:"response"`
}

// InternalLLMClient posts prompt completions to eri's /internal/llm proxy endpoint.
type InternalLLMClient struct {
	url        string
	httpClient *http.Client
	effort     string
}

// NewInternalLLMClient builds an InternalLLMClient targeting an internal LLM proxy URL.
// When effort is provided and non-empty, it is parsed and forwarded in the request body;
// when omitted or empty, effort is omitted from the request payload so eri selects its default.
func NewInternalLLMClient(endpointURL string, httpClient *http.Client, effort ...string) *InternalLLMClient {
	var eff string
	if len(effort) > 0 && strings.TrimSpace(effort[0]) != "" {
		eff = ParseEffort(effort[0], "")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 120 * time.Second}
	}
	return &InternalLLMClient{
		url:        strings.TrimSpace(endpointURL),
		httpClient: tracing.Client(httpClient),
		effort:     eff,
	}
}

// Complete sends prompt to the LLM endpoint and returns the completion response.
func (c *InternalLLMClient) Complete(ctx context.Context, prompt string) (string, error) {
	if c == nil || c.httpClient == nil {
		return "", errors.New("internal llm client is not initialized")
	}
	trimmed := strings.TrimSpace(prompt)
	if trimmed == "" {
		return "", errors.New("llm prompt cannot be empty")
	}
	if c.url == "" {
		return "", errors.New("eri llm url is not configured")
	}
	reqBody, err := json.Marshal(InternalLLMRequest{
		Prompt: trimmed,
		Effort: c.effort,
	})
	if err != nil {
		return "", fmt.Errorf("marshal internal llm request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("create internal llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key := LLMKeyFrom(ctx); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("internal llm request failed: %w", err)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read internal llm response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("internal llm returned status %d: %s", resp.StatusCode, string(respBytes))
	}
	var parsed InternalLLMResponse
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return "", fmt.Errorf("decode internal llm response: %w", err)
	}
	return parsed.Response, nil
}

// HTTPInternalLLMClient returns a closure compatible with func(context.Context, string) (string, error).
func HTTPInternalLLMClient(endpointURL string, httpClient *http.Client, effort ...string) func(context.Context, string) (string, error) {
	client := NewInternalLLMClient(endpointURL, httpClient, effort...)
	return func(ctx context.Context, prompt string) (string, error) {
		return client.Complete(ctx, prompt)
	}
}

// Dispatch enqueue status values.
const (
	StatusAccepted      = "accepted"
	StatusSkippedActive = "skipped_active"
	StatusAlreadyExists = "already_exists"
)

// ErrDispatchActive reports that a dispatch was skipped because a session is already active.
var ErrDispatchActive = errors.New("dispatch active; reconciliation deferred")

// BrainDispatchRequest represents a headless dispatch payload.
type BrainDispatchRequest struct {
	ID             string `json:"id,omitempty"`
	Agent          string `json:"agent,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	VanadisBuildID string `json:"vanadis_build_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	Model          string `json:"model,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
	Traceparent    string `json:"traceparent,omitempty"`
}

// DispatchEnqueueResponse defines the enqueue result for a dispatch.
type DispatchEnqueueResponse struct {
	Status     string `json:"status"` // StatusAccepted, StatusSkippedActive or StatusAlreadyExists
	DispatchID string `json:"dispatch_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

// ResolveDispatchSessionKey determines the canonical Virtual Object key for a dispatch.
func ResolveDispatchSessionKey(req BrainDispatchRequest) string {
	if s := strings.TrimSpace(req.SessionID); s != "" {
		return s
	}
	if c := strings.TrimSpace(req.VanadisChunkID); c != "" {
		return "vanadis-" + c
	}
	return "dispatch-" + strings.TrimSpace(req.ID)
}

// ThreadSessionServiceName returns the Restate service name for ThreadSession Virtual Objects.
func ThreadSessionServiceName(name string) string {
	return agent.ScopedServiceName("ThreadSession", name)
}

// DispatchSessionServiceName returns the Restate service name for DispatchSession Virtual Objects.
func DispatchSessionServiceName(name string) string {
	return agent.ScopedServiceName("DispatchSession", name)
}

// VoiceSessionServiceName returns the Restate service name for VoiceSession Virtual Objects.
func VoiceSessionServiceName(name string) string {
	return agent.ScopedServiceName("VoiceSession", name)
}

// SessionManagerServiceName returns the Restate service name for SessionManager.
func SessionManagerServiceName(name string) string {
	return agent.ScopedServiceName("SessionManager", name)
}

// RestateMethodEndpoint constructs a Restate ingress endpoint URL for a service method.
func RestateMethodEndpoint(restateURL, service, key, method string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(restateURL), "/")
	if key == "" {
		return fmt.Sprintf("%s/%s/%s", trimmed, url.PathEscape(service), url.PathEscape(method))
	}
	return fmt.Sprintf("%s/%s/%s/%s", trimmed, url.PathEscape(service), url.PathEscape(key), url.PathEscape(method))
}

// RestateEnqueueEnvelope holds the common fields returned by ThreadSession and DispatchSession Enqueue handlers.
type RestateEnqueueEnvelope struct {
	Status     string `json:"status"`
	ChannelID  string `json:"channel_id,omitempty"`
	ThreadID   string `json:"thread_id,omitempty"`
	DispatchID string `json:"dispatch_id,omitempty"`
	MessageID  string `json:"message_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

// EffectiveID returns DispatchID when non-empty, else MessageID.
func (e RestateEnqueueEnvelope) EffectiveID() string {
	if e.DispatchID != "" {
		return e.DispatchID
	}
	return e.MessageID
}

// ParseRestateEnqueueEnvelope decodes a Restate Enqueue response body; an empty body returns a zero envelope.
func ParseRestateEnqueueEnvelope(respBytes []byte) (RestateEnqueueEnvelope, error) {
	if len(respBytes) == 0 {
		return RestateEnqueueEnvelope{}, nil
	}
	var env RestateEnqueueEnvelope
	if err := json.Unmarshal(respBytes, &env); err != nil {
		return RestateEnqueueEnvelope{}, err
	}
	return env, nil
}
