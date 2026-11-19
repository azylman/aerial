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

// IndexerClient reads per-session index watermarks from the transcripts
// indexer over HTTP. It has no callers yet; startup resend wires it up in a
// later chunk. An empty base URL disables the client: Watermark returns zero
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
		return 0, nil
	}
	endpoint := c.baseURL + "/watermark?stack=" + url.QueryEscape(strings.TrimSpace(stack)) +
		"&session_id=" + url.QueryEscape(strings.TrimSpace(sessionID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("build indexer watermark request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch indexer watermark: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("indexer watermark status %d", resp.StatusCode)
	}
	var out watermarkResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode indexer watermark: %w", err)
	}
	return out.LastIndexedStep, nil
}
