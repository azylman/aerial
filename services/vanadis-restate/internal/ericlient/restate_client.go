package ericlient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/agent"
	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingress"
)

// ThreadSessionServiceName returns the Restate service name for ThreadSession Virtual Objects.
func ThreadSessionServiceName(name string) string {
	return agent.ScopedServiceName("ThreadSession", name)
}

// DispatchSessionServiceName returns the Restate service name for DispatchSession Virtual Objects.
func DispatchSessionServiceName(name string) string {
	return agent.ScopedServiceName("DispatchSession", name)
}

// StatusWidgetDeliverer abstracts status widget delivery to decouple from concrete discord sender.
type StatusWidgetDeliverer interface {
	DeliverStatusWidget(ctx context.Context, req StatusWidgetRequest) (string, error)
	// DeleteStatusWidget removes messageID with agentName's own sender; a
	// missing message is a success.
	DeleteStatusWidget(ctx context.Context, agentName, channelID, messageID string) error
}

// DirectMessageDeliverer abstracts direct message delivery to decouple from concrete discord sender.
type DirectMessageDeliverer interface {
	DeliverDirectMessage(ctx context.Context, channelID, content string) error
}

// RestateEriClient implements EriClient against Restate Virtual Objects.
type RestateEriClient struct {
	restateURL string
	agentName  string
	httpClient *http.Client

	widgetMu        sync.RWMutex
	hasDiscordAuth  bool
	widgetDeliverer StatusWidgetDeliverer
}

// NewRestateEriClient constructs a RestateEriClient.
func NewRestateEriClient(restateURL, _ string, agentName string, httpClient *http.Client, widgetDeliverer ...StatusWidgetDeliverer) *RestateEriClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	trimmedRestate := strings.TrimRight(strings.TrimSpace(restateURL), "/")
	agentName = agent.Normalize(agentName)
	if agentName == "" {
		agentName = agent.Default
	}

	var wd StatusWidgetDeliverer
	if len(widgetDeliverer) > 0 && widgetDeliverer[0] != nil {
		wd = widgetDeliverer[0]
	}

	hasDiscord := strings.TrimSpace(os.Getenv("DISCORD_TOKEN")) != ""

	return &RestateEriClient{
		restateURL:      trimmedRestate,
		agentName:       agentName,
		hasDiscordAuth:  hasDiscord,
		httpClient:      httpClient,
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

// postRestateJSON posts req to a Restate ingress endpoint, keeping
// ericlient's "restate <op> ..." error text.
func (c *RestateEriClient) postRestateJSON(ctx context.Context, endpoint, idempotencyKey, opLabel string, req any) ([]byte, error) {
	if c == nil {
		return nil, errors.New("restate eri client is nil")
	}
	respBody, err := restateingress.Post(ctx, c.httpClient, endpoint, idempotencyKey, req, 64<<10)
	var httpErr *restateingress.HTTPError
	switch {
	case errors.As(err, &httpErr):
		if httpErr.Message != "" {
			return nil, fmt.Errorf("restate %s error (%d): %s", opLabel, httpErr.Status, httpErr.Message)
		}
		return nil, fmt.Errorf("restate %s returned HTTP %d: %s", opLabel, httpErr.Status, httpErr.Body)
	case err != nil:
		return nil, fmt.Errorf("%s failed: %w", opLabel, err)
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

// effectiveAgent resolves the first non-empty explicit value, else AgentFromContext(ctx), else c.agentName, else defaultAgentName.
func (c *RestateEriClient) effectiveAgent(ctx context.Context, explicit ...string) string {
	for _, exp := range explicit {
		clean := strings.TrimSpace(exp)
		if clean != "" {
			return agent.Normalize(clean)
		}
	}
	if ctx != nil {
		if fromCtx := AgentFromContext(ctx); fromCtx != "" {
			return agent.Normalize(fromCtx)
		}
	}
	if c != nil && c.agentName != "" {
		return agent.Normalize(c.agentName)
	}
	return agent.Default
}

func (c *RestateEriClient) executeRestateDispatch(ctx context.Context, req eriwire.BrainDispatchRequest) (restateDispatchResult, error) {
	if c == nil {
		return restateDispatchResult{}, errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return restateDispatchResult{}, errors.New("restate url not configured")
	}
	dispatchAgent := c.effectiveAgent(ctx, req.Agent)
	req.Agent = dispatchAgent
	dispatchID := strings.TrimSpace(req.ID)
	sessionKey := eriwire.ResolveDispatchSessionKey(req)
	svc := DispatchSessionServiceName(dispatchAgent)
	endpoint := fmt.Sprintf("%s/%s/%s/Enqueue", c.restateURL, svc, url.PathEscape(sessionKey))

	respBytes, err := c.postRestateJSON(ctx, endpoint, dispatchID, "dispatch", req)
	if err != nil {
		return restateDispatchResult{}, err
	}
	return parseDispatchResponse(respBytes)
}

func (c *RestateEriClient) Dispatch(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return errors.New("restate url not configured")
	}

	res, err := c.executeRestateDispatch(ctx, req)
	if err != nil {
		return err
	}
	if res.Status == eriwire.StatusSkippedActive {
		return eriwire.ErrDispatchActive
	}
	switch res.Status {
	case "", statusOK, eriwire.StatusAccepted, eriwire.StatusAlreadyExists:
		return nil
	default:
		return fmt.Errorf("restate rejected dispatch %q (status %q)", req.ID, res.Status)
	}
}

func (c *RestateEriClient) DispatchAccepted(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return errors.New("restate url not configured")
	}

	res, err := c.executeRestateDispatch(ctx, req)
	if err != nil {
		return err
	}
	if res.Status == eriwire.StatusSkippedActive {
		return eriwire.ErrDispatchActive
	}
	effectiveID := res.DispatchID
	if effectiveID == "" {
		effectiveID = res.MessageID
	}
	if req.ID != "" && effectiveID == req.ID {
		if res.Status == eriwire.StatusAccepted || res.Status == eriwire.StatusAlreadyExists || res.Status == statusOK {
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
	if c.restateURL == "" {
		return errors.New("restate url not configured")
	}
	key := "vanadis-" + targetChunk
	actionURL := fmt.Sprintf("%s/%s/%s/Cancel", c.restateURL, DispatchSessionServiceName(c.effectiveAgent(ctx)), url.PathEscape(key))
	_, err := c.postRestateJSON(ctx, actionURL, "", "cancel dispatch", map[string]string{"chunk_id": targetChunk})
	return err
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
		return errors.New("restate url not configured")
	}

	msgID := deterministicPromptMessageID(cleanThreadID, cleanPrompt)
	return c.enqueueThreadPrompt(ctx, c.effectiveAgent(ctx), cleanThreadID, cleanPrompt, msgID, "prompt:"+cleanThreadID+":"+msgID)
}

// enqueueThreadPrompt enqueues one prompt turn on agent's ThreadSession
// Virtual Object keyed by the thread.
func (c *RestateEriClient) enqueueThreadPrompt(ctx context.Context, agentName, threadID, prompt, msgID, idempotencyKey string) error {
	endpoint := fmt.Sprintf("%s/%s/%s/Enqueue", c.restateURL, ThreadSessionServiceName(agentName), url.PathEscape(threadID))
	reqBody := struct {
		ChannelID string `json:"channel_id"`
		Prompt    string `json:"prompt"`
		MessageID string `json:"message_id"`
	}{
		ChannelID: threadID,
		Prompt:    prompt,
		MessageID: msgID,
	}
	_, err := c.postRestateJSON(ctx, endpoint, idempotencyKey, "prompt", reqBody)
	return err
}

// errRestateNotConfigured rejects the agent-scoped Restate-only paths when
// no ingress URL is configured; they never fall back to eri HTTP.
var errRestateNotConfigured = errors.New("restate url not configured")

// PromptAgent enqueues a prompt turn on agent's ThreadSession through
// Restate ingress, keyed by the caller's deterministic idempotency key (the
// message ID derives from it too, so a resend is one turn). It never falls
// back to eri HTTP.
func (c *RestateEriClient) PromptAgent(ctx context.Context, agentName, threadID, prompt, idempotencyKey string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	cleanThreadID, cleanPrompt, cleanKey := strings.TrimSpace(threadID), strings.TrimSpace(prompt), strings.TrimSpace(idempotencyKey)
	if cleanThreadID == "" || cleanPrompt == "" {
		return nil
	}
	if c.restateURL == "" {
		return errRestateNotConfigured
	}
	if cleanKey == "" {
		return errors.New("prompt idempotency key is required")
	}
	return c.enqueueThreadPrompt(ctx, c.resolveAgent(agentName), cleanThreadID, cleanPrompt, deterministicPromptMessageID(cleanThreadID, cleanKey), cleanKey)
}

// DispatchRestate enqueues a background dispatch on req.Agent's
// DispatchSession through Restate ingress with Dispatch's status mapping.
// It never falls back to eri HTTP.
func (c *RestateEriClient) DispatchRestate(ctx context.Context, req eriwire.BrainDispatchRequest) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return errRestateNotConfigured
	}
	return c.Dispatch(ctx, req)
}

// MessageAgent delivers a plain direct message through agent's Discord
// sender. It requires direct Discord auth and an agent-routing deliverer and
// never falls back to eri HTTP.
func (c *RestateEriClient) MessageAgent(ctx context.Context, agentName, channelID, content string) error {
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
	mda, ok := deliverer.(agentMessageDeliverer)
	if !hasDiscordAuth || !ok || mda == nil {
		return errors.New("no agent direct message deliverer configured")
	}
	return mda.DeliverDirectMessageWithAgent(ctx, c.resolveAgent(agentName), cleanChannelID, cleanContent)
}

// agentMessageDeliverer is the agent-routing direct message surface.
type agentMessageDeliverer interface {
	DeliverDirectMessageWithAgent(ctx context.Context, agentName, channelID, content string) error
}

// resolveAgent normalizes agent, falling back to the client's own agent.
func (c *RestateEriClient) resolveAgent(name string) string {
	if clean := agent.Normalize(name); clean != "" {
		return clean
	}
	return c.agentName
}

func (c *RestateEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	if c == nil {
		return "", errors.New("restate eri client is nil")
	}
	if c.restateURL == "" {
		return "", errors.New("restate url not configured")
	}
	if strings.TrimSpace(req.Agent) == "" {
		req.Agent = c.effectiveAgent(ctx)
	}
	c.widgetMu.RLock()
	deliverer := c.widgetDeliverer
	hasDiscordToken := c.hasDiscordAuth
	c.widgetMu.RUnlock()

	if deliverer == nil || !hasDiscordToken {
		return "", errors.New("direct widget delivery is unconfigured")
	}

	return deliverer.DeliverStatusWidget(ctx, req)
}

// DeleteWidget removes a status widget through the direct deliverer under the
// same conditions Widget delivers through it. The eri HTTP fallback has no
// delete route, so without direct delivery it is a no-op.
func (c *RestateEriClient) DeleteWidget(ctx context.Context, agentName, channelID, messageID string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	cleanChannelID, cleanMessageID := strings.TrimSpace(channelID), strings.TrimSpace(messageID)
	if cleanChannelID == "" || cleanMessageID == "" {
		return nil
	}
	c.widgetMu.RLock()
	deliverer := c.widgetDeliverer
	hasDiscordToken := c.hasDiscordAuth
	c.widgetMu.RUnlock()
	if deliverer != nil && hasDiscordToken && c.restateURL != "" {
		return deliverer.DeleteStatusWidget(ctx, c.effectiveAgent(ctx, agentName), cleanChannelID, cleanMessageID)
	}
	return nil
}

func (c *RestateEriClient) Message(ctx context.Context, channelID, content string) error {
	if c == nil {
		return errors.New("restate eri client is nil")
	}
	cleanChannelID, cleanContent := strings.TrimSpace(channelID), strings.TrimSpace(content)
	if cleanChannelID == "" || cleanContent == "" {
		return nil
	}
	if c.restateURL == "" {
		return errors.New("restate url not configured")
	}

	c.widgetMu.RLock()
	deliverer := c.widgetDeliverer
	hasDiscordAuth := c.hasDiscordAuth
	c.widgetMu.RUnlock()

	if !hasDiscordAuth || deliverer == nil {
		return errors.New("direct message delivery is unconfigured")
	}

	if mda, ok := deliverer.(agentMessageDeliverer); ok && mda != nil {
		return mda.DeliverDirectMessageWithAgent(ctx, c.effectiveAgent(ctx), cleanChannelID, cleanContent)
	}
	if md, ok := deliverer.(DirectMessageDeliverer); ok && md != nil {
		return md.DeliverDirectMessage(ctx, cleanChannelID, cleanContent)
	}

	return errors.New("direct message delivery is unconfigured")
}
