// Package ericlient hosts the outbound Eri HTTP client and payloads for vanadis-restate.
package ericlient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
)

const statusOK = "ok"

type agentContextKey struct{}

// WithAgent returns a copy of parent context carrying the given agent identifier.
func WithAgent(ctx context.Context, agent string) context.Context {
	clean := strings.TrimSpace(agent)
	if clean == "" {
		return ctx
	}
	return context.WithValue(ctx, agentContextKey{}, clean)
}

// AgentFromContext extracts the agent identifier from the context, or returns "".
func AgentFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(agentContextKey{}).(string); ok {
		return strings.TrimSpace(val)
	}
	return ""
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	ID            string    `json:"id,omitempty"`
	Index         int       `json:"index"`
	Mode          string    `json:"mode,omitempty"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	ActiveStep    string    `json:"active_step,omitempty"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	Agent     string              `json:"agent,omitempty"`
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	SkipSink  bool                `json:"skip_sink,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
	StartedAt time.Time           `json:"started_at,omitempty"`
}

// SyncChunkPRWidgetRequest carries optional streaming state for a single PR chunk.
type SyncChunkPRWidgetRequest struct {
	PRNumber   int    `json:"pr_number"`
	PRStatus   string `json:"pr_status"`
	PRBanner   string `json:"pr_banner"`
	ActiveStep string `json:"active_step,omitempty"`
	PRFound    bool   `json:"pr_found"`
}

// EriClient describes the outbound communication interface to Eri.
type EriClient interface {
	Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error
	DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error
	CancelDispatch(ctx context.Context, chunkID string) error
	Prompt(ctx context.Context, threadID, prompt string) error
	Widget(ctx context.Context, req StatusWidgetRequest) (string, error)
	// DeleteWidget removes a status widget message posted by agent. A message
	// that is already gone is a success.
	DeleteWidget(ctx context.Context, agent, channelID, messageID string) error
	Message(ctx context.Context, channelID, content string) error
}

// HTTPEriClient implements EriClient against Eri's HTTP endpoints.
type HTTPEriClient struct {
	eriURL     string
	httpClient *http.Client
}

// NewHTTPEriClient builds an Eri client pointing at eriURL.
func NewHTTPEriClient(eriURL string, httpClient *http.Client) *HTTPEriClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPEriClient{
		eriURL:     strings.TrimRight(strings.TrimSpace(eriURL), "/"),
		httpClient: httpClient,
	}
}

func (c *HTTPEriClient) postEriJSON(ctx context.Context, path string, payload any, maxBytes int64) ([]byte, error) {
	if c == nil || c.httpClient == nil {
		return nil, errors.New("eri http client is nil")
	}
	if c.eriURL == "" {
		return nil, errors.New("eri url not configured")
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	url := c.eriURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create eri request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post eri request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return nil, fmt.Errorf("eri returned status %d (failed to read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("eri returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBytes))
}

func (c *HTTPEriClient) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	body, err := c.postEriJSON(ctx, "/dispatch", req, 64<<10)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode eri dispatch response: %w", err)
	}
	if result.Status == eriwire.StatusSkippedActive {
		return eriwire.ErrDispatchActive
	}
	if result.Status != "" && result.Status != statusOK && result.Status != eriwire.StatusAccepted && result.Status != eriwire.StatusAlreadyExists {
		return fmt.Errorf("eri rejected dispatch %q (status %q)", req.ID, result.Status)
	}
	return nil
}

// DispatchAccepted requires durable acceptance of the exact requested generation.
// skipped_active is a retryable rejection, even though Eri returns HTTP 200.
func (c *HTTPEriClient) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	body, err := c.postEriJSON(ctx, "/dispatch", req, 64<<10)
	if err != nil {
		return err
	}
	var result struct {
		Status     string `json:"status"`
		DispatchID string `json:"dispatch_id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode eri dispatch acceptance: %w", err)
	}
	if result.Status == eriwire.StatusSkippedActive {
		return eriwire.ErrDispatchActive
	}
	if req.ID == "" || result.DispatchID != req.ID || (result.Status != eriwire.StatusAccepted && result.Status != eriwire.StatusAlreadyExists) {
		return fmt.Errorf("eri did not accept dispatch %q (status %q, identity %q)", req.ID, result.Status, result.DispatchID)
	}
	return nil
}

func (c *HTTPEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil
	}
	payload := map[string]string{"chunk_id": chunkID}
	_, err := c.postEriJSON(ctx, "/dispatch/cancel", payload, 4<<10)
	return err
}

func deterministicPromptMessageID(threadID, prompt string) string {
	sum := sha256.Sum256([]byte(threadID + "\x00" + prompt))
	return "vanadis-prompt-" + hex.EncodeToString(sum[:16])
}

func (c *HTTPEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	cleanThreadID, cleanPrompt := strings.TrimSpace(threadID), strings.TrimSpace(prompt)
	if cleanThreadID == "" || cleanPrompt == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": cleanThreadID,
		"prompt":     cleanPrompt,
		"message_id": deterministicPromptMessageID(cleanThreadID, cleanPrompt),
	}
	_, err := c.postEriJSON(ctx, "/prompt", payload, 4<<10)
	return err
}

func (c *HTTPEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	respBody, err := c.postEriJSON(ctx, "/discord/widget", req, 4<<10)
	if err != nil {
		return "", err
	}
	var res struct {
		MessageID string `json:"message_id"`
	}
	if uErr := json.Unmarshal(respBody, &res); uErr != nil {
		return "", fmt.Errorf("unmarshal eri widget response: %w", uErr)
	}
	return res.MessageID, nil
}

// DeleteWidget is a no-op: eri exposes no widget delete route, so a status
// widget posted through the HTTP fallback stays in place.
func (*HTTPEriClient) DeleteWidget(context.Context, string, string, string) error {
	return nil
}

func (c *HTTPEriClient) Message(ctx context.Context, channelID, content string) error {
	cleanChannelID, cleanContent := strings.TrimSpace(channelID), strings.TrimSpace(content)
	if cleanChannelID == "" || cleanContent == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": cleanChannelID,
		"content":    cleanContent,
	}
	_, err := c.postEriJSON(ctx, "/discord/message", payload, 4<<10)
	return err
}
