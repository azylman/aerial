package transcript

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// watermarkResponse decodes the indexer watermark endpoint payload.
type watermarkResponse struct {
	LastIndexedStep int `json:"last_indexed_step"`
}

type watermarksResponse struct {
	Watermarks []struct {
		SessionID       string `json:"session_id"`
		LastIndexedStep int    `json:"last_indexed_step"`
	} `json:"watermarks"`
	Sessions map[string]int `json:"sessions,omitempty"`
}

// IndexerClient reads per-session index watermarks from the transcripts
// indexer over HTTP. It has no callers yet; startup resend wires it up in a
// later chunk. An empty base URL disables the client: Watermark returns -1
// with no error.
type IndexerClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewIndexerClient returns an IndexerClient for baseURL. A nil httpClient gets
// a default client with a short timeout; constructor injection keeps tests
// hermetic.
func NewIndexerClient(baseURL string, httpClient *http.Client) *IndexerClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &IndexerClient{baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"), httpClient: httpClient}
}

// Watermark returns the indexer's last indexed step for one session.
func (c *IndexerClient) Watermark(ctx context.Context, stack, sessionID string) (int, error) {
	if c == nil || c.baseURL == "" {
		return -1, nil
	}
	endpoint := c.baseURL + "/watermark?stack=" + url.QueryEscape(strings.TrimSpace(stack)) +
		"&session_id=" + url.QueryEscape(strings.TrimSpace(sessionID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return -1, fmt.Errorf("build indexer watermark request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return -1, fmt.Errorf("fetch indexer watermark: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return -1, fmt.Errorf("indexer watermark status %d", resp.StatusCode)
	}
	var out watermarkResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return -1, fmt.Errorf("decode indexer watermark: %w", err)
	}
	return out.LastIndexedStep, nil
}

// Watermarks returns a sessionID -> LastIndexedStep map for stack, calling
// GET /watermarks?stack=S and omitting unindexed sessions.
func (c *IndexerClient) Watermarks(ctx context.Context, stack string) (map[string]int, error) {
	if c == nil || c.baseURL == "" {
		return map[string]int{}, nil
	}
	endpoint := c.baseURL + "/watermarks?stack=" + url.QueryEscape(strings.TrimSpace(stack))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build indexer watermarks request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch indexer watermarks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("indexer watermarks status %d", resp.StatusCode)
	}
	var out watermarksResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode indexer watermarks: %w", err)
	}
	result := make(map[string]int)
	for sid, step := range out.Sessions {
		sid = strings.TrimSpace(sid)
		if sid != "" && step >= 0 {
			result[sid] = step
		}
	}
	for _, wm := range out.Watermarks {
		sid := strings.TrimSpace(wm.SessionID)
		if sid != "" && wm.LastIndexedStep >= 0 {
			result[sid] = wm.LastIndexedStep
		}
	}
	return result, nil
}
