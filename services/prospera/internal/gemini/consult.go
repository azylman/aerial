package gemini

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/mcpkit"
	"github.com/azylman/aerial/pkg/tracing"
)

// ContextConsultClient queries external context (calendar, memory) mid-call.
type ContextConsultClient interface {
	ConsultCalendar(ctx context.Context, query, startISO, endISO string) (string, error)
	ConsultMemory(ctx context.Context, query string) (string, error)
}

// ConsultOption configures HTTPContextConsultClient.
type ConsultOption func(*HTTPContextConsultClient)

// HTTPContextConsultClient implements ContextConsultClient over HTTP.
type HTTPContextConsultClient struct {
	googleMCPURL string
	covenURL     string
	httpClient   *http.Client
	mcp          *mcpkit.Client
}

// NewHTTPContextConsultClient initializes an HTTPContextConsultClient.
func NewHTTPContextConsultClient(googleMCPURL, covenURL string, opts ...ConsultOption) *HTTPContextConsultClient {
	c := &HTTPContextConsultClient{
		googleMCPURL: strings.TrimRight(googleMCPURL, "/"),
		covenURL:     strings.TrimRight(covenURL, "/"),
		httpClient:   tracing.Client(&http.Client{Timeout: 10 * time.Second}),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.mcp = mcpkit.NewClient(c.googleMCPURL, "prospera", c.httpClient)
	return c
}

// ConsultCalendar calls google-mcp over Streamable HTTP MCP to query calendar events or availability.
func (c *HTTPContextConsultClient) ConsultCalendar(ctx context.Context, _ string, startISO, endISO string) (string, error) {
	if c.googleMCPURL == "" {
		return "Calendar consultation unavailable (no Google MCP configured)", nil
	}

	now := time.Now().UTC()
	tMin := strings.TrimSpace(startISO)
	if tMin == "" {
		tMin = now.Format(time.RFC3339)
	}
	tMax := strings.TrimSpace(endISO)
	if tMax == "" {
		tMax = now.AddDate(0, 0, 30).Format(time.RFC3339)
	}
	text, err := c.mcp.CallTool(ctx, "get_events", map[string]any{"time_min": tMin, "time_max": tMax, "max_results": 25})
	if err != nil {
		return "", err
	}
	if text == "" {
		return "No matching calendar events found.", nil
	}
	return text, nil
}

// Close ends the cached google-mcp session, if any.
func (c *HTTPContextConsultClient) Close() error {
	if c == nil || c.mcp == nil {
		return nil
	}
	return c.mcp.Close()
}

// ConsultMemory queries Coven context/memory for background facts.
func (c *HTTPContextConsultClient) ConsultMemory(ctx context.Context, query string) (string, error) {
	if c.covenURL == "" {
		return "Memory consultation unavailable (no Coven configured)", nil
	}

	endpoint := fmt.Sprintf("%s/context?q=%s", c.covenURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("coven consult request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("coven consult returned status %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return "", fmt.Errorf("read coven response: %w", err)
	}

	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "No relevant memory context found.", nil
	}
	return text, nil
}
