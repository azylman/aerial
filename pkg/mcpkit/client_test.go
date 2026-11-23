package mcpkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type partsArgs struct {
	Parts []string `json:"parts"`
	Fail  bool     `json:"fail,omitempty"`
}

// testMCPServer is an in-process go-sdk streamable server with one "echo"
// tool. It counts initialize requests so tests can see session reuse.
type testMCPServer struct {
	*httptest.Server

	srv   *mcp.Server
	inits atomic.Int32
}

func newTestMCPServer(t *testing.T) *testMCPServer {
	t.Helper()
	ts := &testMCPServer{srv: mcp.NewServer(&mcp.Implementation{Name: "test-mcp", Version: "1.0"}, nil)}
	mcp.AddTool(ts.srv, &mcp.Tool{Name: "echo", InputSchema: InputSchema[partsArgs]()}, func(_ context.Context, _ *mcp.CallToolRequest, args partsArgs) (*mcp.CallToolResult, any, error) {
		content := make([]mcp.Content, 0, len(args.Parts))
		for _, p := range args.Parts {
			content = append(content, &mcp.TextContent{Text: p})
		}
		return &mcp.CallToolResult{IsError: args.Fail, Content: content}, nil, nil
	})
	ts.srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "initialize" {
				ts.inits.Add(1)
			}
			return next(ctx, method, req)
		}
	})
	ts.Server = httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return ts.srv }, nil))
	t.Cleanup(ts.Close)
	return ts
}

// forgetSessions closes every server-side session, so the next request with
// an old session ID gets 404 session not found.
func (s *testMCPServer) forgetSessions() {
	for ss := range s.srv.Sessions() {
		_ = ss.Close()
	}
}

func closedEndpoint() string {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	return dead.URL
}

func primeClientSession(t *testing.T, c *Client, tool string) {
	t.Helper()
	_, err := c.CallTool(context.Background(), tool, map[string]any{"parts": []string{"first"}})
	if tool == "missing" && err == nil {
		t.Fatalf("expected missing prime tool to fail")
	}
	if tool != "missing" && err != nil {
		t.Fatalf("unexpected prime tool error: %v", err)
	}
}

func TestClientCallTool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		closed       bool
		primeTool    string
		forget       bool
		tool         string
		args         map[string]any
		wantText     string
		wantErr      string
		wantInits    int32
		wantSessions int
	}{
		{name: "text result", tool: "echo", args: map[string]any{"parts": []string{"ok"}}, wantText: "ok", wantInits: 1, wantSessions: 1},
		{name: "multi-part text joins with no separator", tool: "echo", args: map[string]any{"parts": []string{"Line 1", "Line 2"}}, wantText: "Line 1Line 2", wantInits: 1, wantSessions: 1},
		{name: "isError result becomes an error carrying its text", tool: "echo", args: map[string]any{"parts": []string{"quota exceeded"}, "fail": true}, wantErr: "tool returned error: quota exceeded", wantInits: 1, wantSessions: 1},
		{name: "unknown tool jsonrpc error keeps the session", tool: "missing", wantErr: "failed to call tool missing", wantInits: 1, wantSessions: 1},
		{name: "closed endpoint fails session init", closed: true, tool: "echo", wantErr: "session init failed", wantInits: 0, wantSessions: 0},
		{name: "session reused across calls", primeTool: "echo", tool: "echo", args: map[string]any{"parts": []string{"again"}}, wantText: "again", wantInits: 1, wantSessions: 1},
		{name: "jsonrpc error session reused by next call", primeTool: "missing", tool: "echo", args: map[string]any{"parts": []string{"still here"}}, wantText: "still here", wantInits: 1, wantSessions: 1},
		{name: "forgotten session retried exactly once", primeTool: "echo", forget: true, tool: "echo", args: map[string]any{"parts": []string{"back"}}, wantText: "back", wantInits: 2, wantSessions: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTestMCPServer(t)
			endpoint := srv.URL
			if tc.closed {
				endpoint = closedEndpoint()
			}
			c := NewClient(endpoint, "test-client", nil)
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			if tc.primeTool != "" {
				primeClientSession(t, c, tc.primeTool)
			}
			if tc.forget {
				srv.forgetSessions()
			}
			got, err := c.CallTool(ctx, tc.tool, tc.args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("CallTool() err = %v, want containing %q", err, tc.wantErr)
				}
			} else if err != nil || got != tc.wantText {
				t.Fatalf("CallTool() = (%q, %v), want %q", got, err, tc.wantText)
			}
			if n := srv.inits.Load(); n != tc.wantInits {
				t.Errorf("initialize count = %d, want %d", n, tc.wantInits)
			}
			sessions := 0
			for range srv.srv.Sessions() {
				sessions++
			}
			if sessions != tc.wantSessions {
				t.Errorf("live server sessions = %d, want %d", sessions, tc.wantSessions)
			}
		})
	}
}

func TestClientClose(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		callFirst          bool
		wantInitsAfterNext int32
	}{
		{name: "without a session is a no-op", callFirst: false, wantInitsAfterNext: 1},
		{name: "with a session ends it so the next call reconnects", callFirst: true, wantInitsAfterNext: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTestMCPServer(t)
			c := NewClient(srv.URL, "test-client", srv.Client())
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			if tc.callFirst {
				if _, err := c.CallTool(ctx, "echo", map[string]any{"parts": []string{"x"}}); err != nil {
					t.Fatalf("CallTool() err = %v", err)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatalf("Close() err = %v", err)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("second Close() err = %v", err)
			}
			if got, err := c.CallTool(ctx, "echo", map[string]any{"parts": []string{"y"}}); err != nil || got != "y" {
				t.Fatalf("CallTool() after Close = (%q, %v), want y", got, err)
			}
			if n := srv.inits.Load(); n != tc.wantInitsAfterNext {
				t.Errorf("initialize count = %d, want %d", n, tc.wantInitsAfterNext)
			}
		})
	}
}
