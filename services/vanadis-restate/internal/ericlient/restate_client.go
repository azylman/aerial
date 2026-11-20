package ericlient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultAgentName = "aerial"

func isSafeAgentChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
}

// NormalizeAgentName trims and lowercases an agent name, ensuring it contains only safe identifier characters.
func NormalizeAgentName(agent string) string {
	clean := strings.ToLower(strings.TrimSpace(agent))
	if clean == "" {
		return ""
	}
	for _, c := range clean {
		if !isSafeAgentChar(c) {
			return defaultAgentName
		}
	}
	return clean
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

// VoiceSessionServiceName returns the Restate service name for VoiceSession Virtual Objects.
func VoiceSessionServiceName(agent string) string {
	name := NormalizeAgentName(agent)
	if name == "" || name == defaultAgentName {
		return "VoiceSession"
	}
	return "VoiceSession-" + name
}

// ReconcileRestateServiceTimeouts PATCHes the Restate admin API for ThreadSession, DispatchSession,
// and VoiceSession so long turns never hit Restate's 1m default inactivity timeout.
// It attempts to patch all services even if one fails, aggregating any errors.
func ReconcileRestateServiceTimeouts(ctx context.Context, adminURL, agent string, httpClient *http.Client) error {
	trimmedAdmin := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if trimmedAdmin == "" {
		return errors.New("admin url cannot be empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	services := []string{
		ThreadSessionServiceName(agent),
		DispatchSessionServiceName(agent),
		VoiceSessionServiceName(agent),
	}

	patchBody := []byte(`{"inactivity_timeout":"1h","abort_timeout":"1h"}`)
	var errs []error
	for _, svc := range services {
		reqURL := fmt.Sprintf("%s/services/%s", trimmedAdmin, url.PathEscape(svc))
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, reqURL, bytes.NewReader(patchBody))
		if err != nil {
			errs = append(errs, fmt.Errorf("create timeout patch request for %s: %w", svc, err))
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			errs = append(errs, fmt.Errorf("patch timeout for service %s: %w", svc, err))
			continue
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		if readErr != nil {
			errs = append(errs, fmt.Errorf("read timeout patch response for %s: %w", svc, readErr))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			errs = append(errs, fmt.Errorf("patch timeout for %s returned HTTP %d: %s", svc, resp.StatusCode, string(respBody)))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// StatusWidgetDeliverer abstracts status widget delivery to decouple from concrete discord sender.
type StatusWidgetDeliverer interface {
	DeliverStatusWidget(ctx context.Context, req StatusWidgetRequest) (string, error)
}

// DirectMessageDeliverer abstracts direct message delivery to decouple from concrete discord sender.
type DirectMessageDeliverer interface {
	DeliverDirectMessage(ctx context.Context, channelID, content string) error
}

// RestateEriClient implements EriClient against Restate Virtual Objects, falling back to HTTP Eri when unconfigured.
type RestateEriClient struct {
	restateURL   string
	eriURL       string
	agentName    string
	httpClient   *http.Client
	httpFallback *HTTPEriClient

	widgetMu        sync.RWMutex
	hasDiscordAuth  bool
	widgetDeliverer StatusWidgetDeliverer
}

// NewRestateEriClient constructs a RestateEriClient.
func NewRestateEriClient(restateURL, eriURL, agentName string, httpClient *http.Client, widgetDeliverer ...StatusWidgetDeliverer) *RestateEriClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	trimmedRestate := strings.TrimRight(strings.TrimSpace(restateURL), "/")
	trimmedEri := strings.TrimRight(strings.TrimSpace(eriURL), "/")
	agent := NormalizeAgentName(agentName)
	if agent == "" {
		agent = defaultAgentName
	}

	var fallback *HTTPEriClient
	if trimmedEri != "" {
		fallback = NewHTTPEriClient(trimmedEri, httpClient)
	}

	var wd StatusWidgetDeliverer
	if len(widgetDeliverer) > 0 && widgetDeliverer[0] != nil {
		wd = widgetDeliverer[0]
	}

	hasDiscord := strings.TrimSpace(os.Getenv("DISCORD_TOKEN")) != ""

	return &RestateEriClient{
		restateURL:      trimmedRestate,
		eriURL:          trimmedEri,
		agentName:       agent,
		hasDiscordAuth:  hasDiscord,
		httpClient:      httpClient,
		httpFallback:    fallback,
		widgetDeliverer: wd,
	}
}

// SetDiscordAuth explicitly overrides whether direct Discord REST authentication is available.
func (c *RestateEriClient) SetDiscordAuth(available bool) {
	if c != nil {
		c.widgetMu.Lock()
		c.hasDiscordAuth = available
		c.widgetMu.Unlock()
	}
}

// SetWidgetDeliverer dynamically sets the widget deliverer.
func (c *RestateEriClient) SetWidgetDeliverer(wd StatusWidgetDeliverer) {
	if c != nil {
		c.widgetMu.Lock()
		c.widgetDeliverer = wd
		c.widgetMu.Unlock()
	}
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

func (c *RestateEriClient) postRestateJSON(ctx context.Context, endpoint, idempotencyKey, opLabel string, req any) ([]byte, error) {
	if c == nil {
		return nil, errors.New("restate eri client is nil")
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

	client := c.httpClient
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

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseRestateHTTPError(opLabel, resp.StatusCode, respBody)
	}

	return respBody, nil
}

type restateDispatchResult struct {
	Status     string `json:"status"`
	DispatchID string `json:"dispatch_id"`
	MessageID  string `json:"message_id"`
	SessionID  string `json:"session_id"`
}

func parseDispatchResponse(respBytes []byte) (restateDispatchResult, error) {
	var result restateDispatchResult
	if len(respBytes) == 0 {
		return result, nil
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return result, fmt.Errorf("decode restate dispatch response: %w", err)
	}
	return result, nil
}

func resolveSessionKey(req BrainDispatchRequest) string {
	sessionKey := strings.TrimSpace(req.SessionID)
	if sessionKey == "" {
		chunkID := strings.TrimSpace(req.VanadisChunkID)
		if chunkID != "" {
			sessionKey = "vanadis-" + chunkID
		} else {
			dispatchID := strings.TrimSpace(req.ID)
			sessionKey = "dispatch-" + dispatchID
		}
	}
	return sessionKey
}

func (c *RestateEriClient) executeRestateDispatch(ctx context.Context, req BrainDispatchRequest) (restateDispatchResult, error) {
	if c == nil {
		return restateDispatchResult{}, errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return restateDispatchResult{}, errors.New("restate url not configured")
	}
	dispatchID := strings.TrimSpace(req.ID)
	sessionKey := resolveSessionKey(req)
	svc := DispatchSessionServiceName(c.agentName)
	endpoint := fmt.Sprintf("%s/%s/%s/Enqueue", c.restateURL, svc, url.PathEscape(sessionKey))

	respBytes, err := c.postRestateJSON(ctx, endpoint, dispatchID, "dispatch", req)
	if err != nil {
		return restateDispatchResult{}, err
	}
	return parseDispatchResponse(respBytes)
}

func (c *RestateEriClient) Dispatch(ctx context.Context, req BrainDispatchRequest) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		if c.httpFallback != nil {
			return c.httpFallback.Dispatch(ctx, req)
		}
		return errors.New("neither restate url nor eri url configured")
	}

	res, err := c.executeRestateDispatch(ctx, req)
	if err != nil {
		return err
	}
	if res.Status == statusSkippedActive {
		return ErrDispatchActive
	}
	switch res.Status {
	case "", statusOK, statusAccepted, statusAlreadyExists:
		return nil
	default:
		return fmt.Errorf("restate rejected dispatch %q (status %q)", req.ID, res.Status)
	}
}

func (c *RestateEriClient) DispatchAccepted(ctx context.Context, req BrainDispatchRequest) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		if c.httpFallback != nil {
			return c.httpFallback.DispatchAccepted(ctx, req)
		}
		return errors.New("neither restate url nor eri url configured")
	}

	res, err := c.executeRestateDispatch(ctx, req)
	if err != nil {
		return err
	}
	if res.Status == statusSkippedActive {
		return ErrDispatchActive
	}
	effectiveID := res.DispatchID
	if effectiveID == "" {
		effectiveID = res.MessageID
	}
	if req.ID != "" && effectiveID == req.ID {
		if res.Status == statusAccepted || res.Status == statusAlreadyExists || res.Status == statusOK {
			return nil
		}
	}
	return fmt.Errorf("restate did not accept dispatch %q (status %q, identity %q)", req.ID, res.Status, effectiveID)
}

func (c *RestateEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	targetChunk := strings.TrimSpace(chunkID)
	if targetChunk == "" {
		return nil
	}
	if c.restateURL != "" {
		key := "vanadis-" + targetChunk
		actionURL := fmt.Sprintf("%s/%s/%s/Cancel", c.restateURL, DispatchSessionServiceName(c.agentName), url.PathEscape(key))
		_, err := c.postRestateJSON(ctx, actionURL, "", "cancel dispatch", map[string]string{"chunk_id": targetChunk})
		return err
	}
	if c.httpFallback != nil {
		return c.httpFallback.CancelDispatch(ctx, targetChunk)
	}
	return errors.New("neither restate url nor eri url configured")
}

func (c *RestateEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	cleanThreadID, cleanPrompt := strings.TrimSpace(threadID), strings.TrimSpace(prompt)
	if cleanThreadID == "" || cleanPrompt == "" {
		return nil
	}
	if c.restateURL == "" {
		if c.httpFallback != nil {
			return c.httpFallback.Prompt(ctx, cleanThreadID, cleanPrompt)
		}
		return errors.New("neither restate url nor eri url configured")
	}

	msgID := deterministicPromptMessageID(cleanThreadID, cleanPrompt)
	endpoint := fmt.Sprintf("%s/%s/%s/Enqueue", c.restateURL, ThreadSessionServiceName(c.agentName), url.PathEscape(cleanThreadID))
	reqBody := struct {
		ChannelID string `json:"channel_id"`
		Prompt    string `json:"prompt"`
		MessageID string `json:"message_id"`
	}{
		ChannelID: cleanThreadID,
		Prompt:    cleanPrompt,
		MessageID: msgID,
	}
	_, err := c.postRestateJSON(ctx, endpoint, "prompt:"+cleanThreadID+":"+msgID, "prompt", reqBody)
	return err
}

func (c *RestateEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	if c == nil {
		return "", errors.New("restate eri client is nil")
	}
	// Direct widget delivery takes priority when widgetDeliverer is configured, or falls back to HTTPEriClient
	// when Discord authentication or RESTATE_INGRESS_URL is unconfigured.
	c.widgetMu.RLock()
	deliverer := c.widgetDeliverer
	hasDiscordToken := c.hasDiscordAuth
	c.widgetMu.RUnlock()

	hasRestateURL := c.restateURL != ""

	if deliverer != nil && hasDiscordToken && hasRestateURL {
		return deliverer.DeliverStatusWidget(ctx, req)
	}

	if c.httpFallback != nil {
		return c.httpFallback.Widget(ctx, req)
	}
	return "", errors.New("neither widget deliverer nor eri url configured for widget delivery")
}

func (c *RestateEriClient) Message(ctx context.Context, channelID, content string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	cleanChannelID, cleanContent := strings.TrimSpace(channelID), strings.TrimSpace(content)
	if cleanChannelID == "" || cleanContent == "" {
		return nil
	}

	c.widgetMu.RLock()
	deliverer := c.widgetDeliverer
	hasDiscordAuth := c.hasDiscordAuth
	c.widgetMu.RUnlock()

	if c.restateURL != "" && hasDiscordAuth {
		if md, ok := deliverer.(DirectMessageDeliverer); ok && md != nil {
			return md.DeliverDirectMessage(ctx, cleanChannelID, cleanContent)
		}
	}

	if c.httpFallback != nil {
		return c.httpFallback.Message(ctx, cleanChannelID, cleanContent)
	}
	return errors.New("neither direct message deliverer nor http fallback configured for message delivery")
}
