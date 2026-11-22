package mcpkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Client calls tools on a Streamable-HTTP MCP server through one go-sdk
// client session, created lazily and reused across calls.
type Client struct {
	endpoint   string
	name       string
	httpClient *http.Client
	mu         sync.Mutex
	cached     *mcp.ClientSession
}

// NewClient returns a Client for endpoint that identifies itself as clientName.
// A nil httpClient means a client with a 90s timeout.
func NewClient(endpoint, clientName string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 90 * time.Second}
	}
	return &Client{endpoint: endpoint, name: clientName, httpClient: httpClient}
}

func (c *Client) session(ctx context.Context) (*mcp.ClientSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached != nil {
		return c.cached, nil
	}
	client := mcp.NewClient(&mcp.Implementation{Name: c.name, Version: "1.0"}, nil)
	transport := &mcp.StreamableClientTransport{Endpoint: c.endpoint, HTTPClient: c.httpClient, MaxRetries: -1, DisableStandaloneSSE: true}
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("session init failed: %w", err)
	}
	c.cached = session
	return session, nil
}

// dropSession forgets a session whose connection broke so the next call
// reconnects, and returns the session's close error.
func (c *Client) dropSession(session *mcp.ClientSession) error {
	c.mu.Lock()
	if c.cached == session {
		c.cached = nil
	}
	c.mu.Unlock()
	return session.Close()
}

// Close ends the cached session, if any.
func (c *Client) Close() error {
	c.mu.Lock()
	session := c.cached
	c.cached = nil
	c.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close()
}

// CallTool runs a tool and returns its concatenated text content. A session
// the server no longer knows is recreated once; an isError result is an error
// carrying the tool's text.
func (c *Client) CallTool(ctx context.Context, tool string, args map[string]any) (string, error) {
	text, err := c.callTool(ctx, tool, args)
	if errors.Is(err, mcp.ErrSessionMissing) {
		text, err = c.callTool(ctx, tool, args)
	}
	return text, err
}

func (c *Client) callTool(ctx context.Context, tool string, args map[string]any) (string, error) {
	session, err := c.session(ctx)
	if err != nil {
		return "", err
	}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		callErr := fmt.Errorf("failed to call tool %s: %w", tool, err)
		if _, isWire := errors.AsType[*jsonrpc.Error](err); isWire {
			return "", callErr
		}
		return "", errors.Join(callErr, c.dropSession(session))
	}
	var text strings.Builder
	for _, content := range res.Content {
		if tc, ok := content.(*mcp.TextContent); ok {
			text.WriteString(tc.Text)
		}
	}
	if res.IsError {
		return "", fmt.Errorf("tool returned error: %s", text.String())
	}
	return text.String(), nil
}
