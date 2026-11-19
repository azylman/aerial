package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
)

// TrajectoryContext carries the retrieved-memory facts and tool calls for one thread.
type TrajectoryContext struct {
	RetrievedFacts []string `json:"retrieved_facts"`
	ToolCalls      []string `json:"tool_calls"`
}

// IndexerClient reads per-thread trajectory context from the transcripts
// indexer service. It is dark: no live caller uses it until the indexed-event
// cutover chunk wires it in. Reads are stack-filtered per Roles N5.
type IndexerClient struct {
	baseURL    string
	stack      string
	maxSteps   int
	httpClient *http.Client
}

// NewIndexerClient builds an IndexerClient for stack against baseURL.
func NewIndexerClient(baseURL, stack string, httpClient *http.Client) *IndexerClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &IndexerClient{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		stack:      NormalizeStack(stack),
		maxSteps:   DefaultExtractMaxSteps,
		httpClient: tracing.Client(httpClient),
	}
}

// SetMaxSteps overrides how many trajectory steps one read requests.
func (c *IndexerClient) SetMaxSteps(n int) {
	if c == nil || n <= 0 {
		return
	}
	c.maxSteps = n
}

// ScopedThreadKey namespaces a thread under its stack for watermark reads.
func ScopedThreadKey(stack, threadID string) string {
	return NormalizeStack(stack) + "/" + strings.TrimSpace(threadID)
}

// GetTrajectoryContext fetches retrieved facts and tool calls for threadID,
// filtered to the client's stack. Empty thread IDs return zero without a request.
func (c *IndexerClient) GetTrajectoryContext(ctx context.Context, threadID string) (TrajectoryContext, error) {
	var zero TrajectoryContext
	if c == nil || strings.TrimSpace(c.baseURL) == "" {
		return zero, fmt.Errorf("indexer client: transcripts URL is not configured")
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return zero, nil
	}
	endpoint := fmt.Sprintf("%s/transcripts/threads/%s/trajectory?%s",
		c.baseURL, url.PathEscape(threadID), c.query().Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return zero, fmt.Errorf("indexer client: failed to build trajectory request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return zero, fmt.Errorf("indexer client: trajectory request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("indexer client: failed to read trajectory response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return zero, fmt.Errorf("indexer client: trajectory request returned status %d", resp.StatusCode)
	}
	var got TrajectoryContext
	if err := json.Unmarshal(body, &got); err != nil {
		return zero, fmt.Errorf("indexer client: failed to decode trajectory response: %w", err)
	}
	return got, nil
}

func (c *IndexerClient) query() url.Values {
	q := url.Values{}
	q.Set("stack", c.stack)
	if c.maxSteps > 0 {
		q.Set("limit", strconv.Itoa(c.maxSteps))
	}
	return q
}
