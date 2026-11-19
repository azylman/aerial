package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

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
	return c
}

// ConsultCalendar calls google-mcp over Streamable HTTP MCP to query calendar events or availability.
func (c *HTTPContextConsultClient) ConsultCalendar(ctx context.Context, _ string, startISO, endISO string) (string, error) {
	if c.googleMCPURL == "" {
		return "Calendar consultation unavailable (no Google MCP configured)", nil
	}

	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"prospera","version":"1.0.0"}}}`
	_, sessionID, err := c.doMCPRequest(ctx, "", []byte(initBody))
	if err != nil {
		return "", err
	}
	if sessionID != "" {
		if _, _, err = c.doMCPRequest(ctx, sessionID, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); err != nil {
			return "", err
		}
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
	callBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_events","arguments":{"time_min":%q,"time_max":%q,"max_results":25}}}`, tMin, tMax)
	rawBody, _, err := c.doMCPRequest(ctx, sessionID, []byte(callBody))
	if err != nil {
		return "", err
	}

	var rpcResp struct {
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(extractMCPJSON(rawBody), &rpcResp); err != nil {
		return "", fmt.Errorf("decode calendar rpc response: %w", err)
	}

	if rpcResp.Error != nil {
		return "", fmt.Errorf("calendar mcp error: %s", rpcResp.Error.Message)
	}

	var sb strings.Builder
	for _, item := range rpcResp.Result.Content {
		if item.Text != "" {
			if sb.Len() > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(item.Text)
		}
	}
	if sb.Len() == 0 {
		return "No matching calendar events found.", nil
	}
	return sb.String(), nil
}

func (c *HTTPContextConsultClient) doMCPRequest(ctx context.Context, sessionID string, bodyBytes []byte) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.googleMCPURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("calendar consult request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("calendar consult returned status %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 128<<10))
	if err != nil {
		return nil, "", fmt.Errorf("read calendar response: %w", err)
	}
	return raw, resp.Header.Get("Mcp-Session-Id"), nil
}

func extractMCPJSON(raw []byte) []byte {
	for line := range strings.SplitSeq(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(trimmed, "data:"); ok {
			return []byte(strings.TrimSpace(after))
		}
	}
	return bytes.TrimSpace(raw)
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
