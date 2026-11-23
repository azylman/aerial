package ericlient

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

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Client handles outbound HTTP dispatch to the Eri agent runtime and Discord ingress APIs.
type Client struct {
	brainURL                 string
	httpClient               *http.Client
	discordMsgClient         *http.Client
	discordInteractionClient *http.Client
	retryDelay               time.Duration
	maxAttempts              int
}

// NewClient constructs a Client configured for Eri dispatch.
func NewClient(brainURL string, client *http.Client) *Client {
	discordMsgClient := client
	discordInteractionClient := client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
		discordMsgClient = &http.Client{Timeout: 20 * time.Second}
		discordInteractionClient = &http.Client{Timeout: 2500 * time.Millisecond}
	}
	return &Client{
		brainURL:                 strings.TrimRight(brainURL, "/"),
		httpClient:               tracing.Client(client),
		discordMsgClient:         tracing.Client(discordMsgClient),
		discordInteractionClient: tracing.Client(discordInteractionClient),
		retryDelay:               1500 * time.Millisecond,
		maxAttempts:              6,
	}
}

// SetRetryConfig overrides retry delay and attempt bounds for testing or low-latency environments.
func (c *Client) SetRetryConfig(delay time.Duration, maxAttempts int) {
	c.retryDelay = delay
	c.maxAttempts = maxAttempts
}

// ValidateBrainChannel checks whether the channel identifier is valid for the target endpoint.
func ValidateBrainChannel(channelID, endpointPath string) error {
	if channelID != "" || endpointPath != "/dispatch" {
		if !model.IsValidDiscordChannel(channelID) {
			return fmt.Errorf("invalid discord snowflake channel id: %q", channelID)
		}
	}
	return nil
}

// AdvanceBackoffDelay calculates the next backoff delay when exponential backoff is active.
func AdvanceBackoffDelay(delay time.Duration, expBackoff bool) time.Duration {
	if expBackoff {
		delay = min(time.Duration(float64(delay)*1.5), 5*time.Second)
	}
	return delay
}

// PostBrainWithRetry executes an HTTP POST to an Eri endpoint with retry backoff.
func (c *Client) PostBrainWithRetry(ctx context.Context, endpointPath, reqLabel, statusLabel, channelID string, maxAttempts int, defaultDelay time.Duration, expBackoff bool, payload any) error {
	if strings.TrimSpace(c.brainURL) == "" {
		return errors.New("brain URL is not configured")
	}
	if err := ValidateBrainChannel(channelID, endpointPath); err != nil {
		return err
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	delay := c.retryDelay
	if delay <= 0 {
		delay = defaultDelay
	}
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	endpoint := c.brainURL + endpointPath
	var lastErr error

	for attempt := range maxAttempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 {
			metrics.RecordJobRetry(metrics.QueueDefault, "brain_http_dispatch", "brain_http_retry")
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create %s request: %w", reqLabel, err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("%s request failed (attempt %d/%d): %w", reqLabel, attempt+1, maxAttempts, err)
		} else {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
			}
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode < 500 {
				return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
			}
			lastErr = fmt.Errorf("%s returned status %d (attempt %d/%d): %s", statusLabel, resp.StatusCode, attempt+1, maxAttempts, string(respBody))
		}
		if attempt+1 < maxAttempts {
			select {
			case <-time.After(delay):
				delay = AdvanceBackoffDelay(delay, expBackoff)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

// DispatchDirectMessage sends a direct proactive message through Eri.
func (c *Client) DispatchDirectMessage(ctx context.Context, req model.DirectMessageRequest) error {
	return c.PostBrainWithRetry(ctx, "/discord/message", "direct message", "brain internal message", req.ChannelID, 3, 500*time.Millisecond, false, req)
}

// PostDiscordInternalJSONToURL executes an HTTP POST to an arbitrary Eri base URL.
func (c *Client) PostDiscordInternalJSONToURL(ctx context.Context, eriBaseURL string, client *http.Client, timeout time.Duration, path, reqLabel, statusLabel string, payload any) error {
	baseURL := strings.TrimRight(strings.TrimSpace(eriBaseURL), "/")
	if baseURL == "" {
		baseURL = c.brainURL
	}
	if baseURL == "" {
		return errors.New("brain URL is not configured")
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	endpoint := baseURL + path
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create %s request: %w", reqLabel, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", reqLabel, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read %s response: %w", reqLabel, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
	}
	return nil
}

// DispatchDiscordMessageToURL sends an inbound Discord message payload to a specific Eri base URL.
func (c *Client) DispatchDiscordMessageToURL(ctx context.Context, eriURL string, req model.DiscordMessageIngressRequest) error {
	return c.PostDiscordInternalJSONToURL(ctx, eriURL, c.discordMsgClient, 20*time.Second, "/internal/discord/message", "discord message ingress", "discord ingress", req)
}

// DispatchDiscordInteractionToURL sends an inbound Discord interaction event payload to a specific Eri base URL.
func (c *Client) DispatchDiscordInteractionToURL(ctx context.Context, eriURL string, interaction *discordgo.Interaction) error {
	return c.PostDiscordInternalJSONToURL(ctx, eriURL, c.discordInteractionClient, 2500*time.Millisecond, "/internal/discord/interaction", "discord interaction", "discord interaction", interaction)
}
