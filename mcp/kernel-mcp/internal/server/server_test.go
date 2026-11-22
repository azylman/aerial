package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/mcp/kernel-mcp/internal/config"
	"github.com/azylman/aerial/pkg/httpkittest"
)

// newRoutesMux builds the kernel-mcp mux around a client that never dials CDP.
func newRoutesMux() http.Handler {
	cfg := &config.Config{}
	return NewHTTPMux(NewServer(cfg, NewCDPClient(cfg)))
}

// postKernelMCP posts one JSON-RPC request to mux and returns the response body.
func postKernelMCP(t *testing.T, mux http.Handler, method string, params any) []byte {
	t.Helper()
	reqBytes, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal %s request: %v", method, err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(reqBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w.Body.Bytes()
}

func TestServer_HTTPRoutesTable(t *testing.T) {
	httpkittest.StubCoverageWriters(t, nil, nil)
	mux := newRoutesMux()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantSubstr string
	}{
		{"health GET returns 200", http.MethodGet, "/health", http.StatusOK, `"status":"ok"`},
		{"health HEAD returns 200", http.MethodHead, "/health", http.StatusOK, ""},
		{"health POST returns 405", http.MethodPost, "/health", http.StatusMethodNotAllowed, "Method not allowed"},
		{"coverage flush returns 200", http.MethodGet, "/debug/coverage/flush", http.StatusOK, ""},
		{"mcp GET returns ready banner", http.MethodGet, "/mcp", http.StatusOK, "kernel-mcp ready"},
		{"mcp empty body returns 400", http.MethodPost, "/mcp", http.StatusBadRequest, "Empty request body"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, http.NoBody))
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("body = %q, want substring %q", w.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServerInstructions(t *testing.T) {
	_, cfg := startFakeCDPServer(t, fakeCDPOptions{})
	client := NewCDPClient(cfg)
	defer client.Close()
	ts := httptest.NewServer(NewHTTPMux(NewServer(cfg, client)))
	t.Cleanup(ts.Close)

	tests := []struct {
		name           string
		path           string
		wantServerName string
		wantTool       string
	}{
		{name: "initialize advertises server identity and browser workflow instructions", path: "/mcp", wantServerName: "kernel-mcp", wantTool: "amazon_add_to_cart"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mcpClient := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := mcpClient.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL + tc.path,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer func() { _ = session.Close() }()

			if got := session.InitializeResult().ServerInfo.Name; got != tc.wantServerName {
				t.Errorf("server name = %q, want %q", got, tc.wantServerName)
			}
			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing %q: %q", tc.wantTool, got)
			}
		})
	}
}

func TestServer_ToolsList(t *testing.T) {
	var resp struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	body := postKernelMCP(t, newRoutesMux(), "tools/list", map[string]any{})
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode tools/list %q: %v", body, err)
	}
	got := make([]string, 0, len(resp.Result.Tools))
	for _, tool := range resp.Result.Tools {
		got = append(got, tool.Name)
	}
	slices.Sort(got)
	want := []string{
		"amazon_add_to_cart", "amazon_get_cart", "amazon_search",
		"browser_click", "browser_evaluate", "browser_get_content", "browser_navigate", "browser_screenshot", "browser_type",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
}

// callKernelTool calls tool on a fresh server backed by a fake CDP endpoint and
// returns its content rendered as text (images as "[<mime>]") and its isError flag.
func callKernelTool(t *testing.T, fake fakeCDPOptions, tool, args string) (string, bool) {
	t.Helper()
	_, cfg := startFakeCDPServer(t, fake)
	client := NewCDPClient(cfg)
	t.Cleanup(func() { _ = client.Close() })
	mux := NewHTTPMux(NewServer(cfg, client))
	body := postKernelMCP(t, mux, "tools/call", map[string]any{"name": tool, "arguments": json.RawMessage(args)})

	var resp struct {
		Result struct {
			Content []struct {
				Type     string `json:"type"`
				Text     string `json:"text"`
				MIMEType string `json:"mimeType"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode tools/call %q: %v", body, err)
	}
	if resp.Error != nil {
		return "rpc error: " + resp.Error.Message, true
	}
	parts := make([]string, 0, len(resp.Result.Content))
	for _, c := range resp.Result.Content {
		if c.Type == "image" {
			parts = append(parts, "["+c.MIMEType+"]")
			continue
		}
		parts = append(parts, c.Text)
	}
	return strings.Join(parts, "\n"), resp.Result.IsError
}

func TestServer_ToolsExecutionTable(t *testing.T) {
	t.Parallel()
	none := fakeCDPOptions{}
	evalFails := fakeCDPFailOn("Runtime.evaluate")
	navFails := fakeCDPFailOn("Page.navigate")
	clickFails := fakeCDPFailOn("Input.dispatchMouseEvent")
	const evalErr = "cdp error -32000: Runtime.evaluate failed"
	const navErr = "Page.navigate: cdp error -32000: Page.navigate failed"
	const clickErr = "mouseMoved: cdp error -32000: Input.dispatchMouseEvent failed"

	tests := []struct {
		name      string
		tool      string
		args      string
		fake      fakeCDPOptions
		wantText  string
		wantError bool
	}{
		{"navigate blank url", "browser_navigate", `{"url":"  "}`, none, "Argument 'url' is required and must be a valid URL string", true},
		{"navigate failure", "browser_navigate", `{"url":"https://example.com"}`, navFails, "Navigation failed: " + navErr, true},
		{"navigate success", "browser_navigate", `{"url":"https://example.com"}`, none, "Navigated to https://example.com", false},
		{"kernel_ alias", "kernel_browser_navigate", `{"url":"https://example.com"}`, none, "Navigated to https://example.com", false},
		{"evaluate blank expression", "browser_evaluate", `{"expression":" "}`, none, "Argument 'expression' is required", true},
		{"evaluate success", "browser_evaluate", `{"expression":"window.location.href"}`, none, `"eval-success"`, false},
		{"evaluate no await", "browser_evaluate", `{"expression":"x","awaitPromise":false,"returnByValue":false}`, none, `"eval-success"`, false},
		{"evaluate exception", "browser_evaluate", `{"expression":"throw new Error()"}`, none, "evaluation error: Uncaught Error: boom", true},
		{"evaluate blocks fetch", "browser_evaluate", `{"expression":"fetch('/s?k=gpu')"}`, none, "blocked unsafe browser_evaluate pattern", true},
		{"get_content redacts cards", "browser_get_content", `{"selector":"body","maxLength":99}`, none, "card [REDACTED_CARD] here", false},
		{"get_content legacy max_chars alias", "browser_get_content", `{"max_chars":5}`, none, `"text":"Produ\n...[truncated]"`, false},
		{"get_content eval error", "browser_get_content", `{}`, evalFails, "Content extraction failed: " + evalErr, true},
		{"click blank selector", "browser_click", `{"selector":""}`, none, "Argument 'selector' is required", true},
		{"click element not found", "browser_click", `{"selector":"#missing"}`, none, "Element not found for selector: #missing", true},
		{"click eval failure", "browser_click", `{"selector":".btn"}`, evalFails, "Click script evaluation failed: " + evalErr, true},
		{"click success", "browser_click", `{"selector":".btn"}`, none, `{"clicked":true,"tag":"button","text":"Add to Cart","trusted":true}`, false},
		{"click trusted dispatch failure", "browser_click", `{"selector":".btn"}`, clickFails, "Dispatch trusted click failed: " + clickErr, true},
		{"type blank selector", "browser_type", `{"selector":"","text":"hello"}`, none, "Argument 'selector' is required", true},
		{"type not found", "browser_type", `{"selector":"#missing","text":"hello"}`, none, "Element not found for selector: #missing", true},
		{"type eval failure", "browser_type", `{"selector":"input","text":"hello"}`, evalFails, "Focus evaluation failed: " + evalErr, true},
		{
			"type insertText failure", "browser_type", `{"selector":"input","text":"hello"}`, fakeCDPFailOn("Input.insertText"),
			"Input.insertText failed: cdp error -32000: Input.insertText failed", true,
		},
		{"type success", "browser_type", `{"selector":"input","text":"chair","clear":true}`, none, `{"trusted":true,"typed":true}`, false},
		{"type click best effort", "browser_type", `{"selector":"input","text":"hello"}`, clickFails, `{"trusted":true,"typed":true}`, false},
		{
			"type input events are best effort", "browser_type", `{"selector":"input","text":"hello"}`,
			fakeCDPOptions{evals: []fakeCDPEval{{match: "new Event(", err: "boom"}}},
			`{"trusted":true,"typed":true}`, false,
		},
		{
			"screenshot failure", "browser_screenshot", `{}`, fakeCDPFailOn("Page.captureScreenshot"),
			"Screenshot capture failed: cdp error -32000: Page.captureScreenshot failed", true,
		},
		{"screenshot default png image and caption", "browser_screenshot", `{}`, none, "[image/png]\nScreenshot captured (png)", false},
		{"screenshot jpeg mime type", "browser_screenshot", `{"format":" JPEG "}`, none, "[image/jpeg]\nScreenshot captured (jpeg)", false},
		{
			"screenshot undecodable data falls back to text", "browser_screenshot", `{}`,
			fakeCDPOptions{screenshotData: "not-base64!"},
			`{"data":"not-base64!","format":"png"}`, false,
		},
		{"search blank query", "amazon_search", `{"query":"   "}`, none, "Argument 'query' is required", true},
		{"search navigate failure", "amazon_search", `{"query":"echo dot"}`, navFails, "Amazon search navigation failed: " + navErr, true},
		{"search eval failure", "amazon_search", `{"query":"echo dot"}`, evalFails, "Amazon search extraction failed: " + evalErr, true},
		{"search sanitizes card PII", "amazon_search", `{"query":"echo dot","maxResults":3}`, none, "Echo Dot with card [REDACTED_CARD]", false},
		{"get_cart sanitizes card PII", "amazon_get_cart", `{}`, none, "Echo Dot [REDACTED_CARD]", false},
		{"get_cart navigation failure", "amazon_get_cart", `{}`, navFails, "Amazon cart navigation failed: " + navErr, true},
		{"get_cart extraction failure", "amazon_get_cart", `{}`, evalFails, "Amazon cart extraction failed: " + evalErr, true},
		{"add_to_cart blank asin", "amazon_add_to_cart", `{"asin":" "}`, none, "Argument 'asin' is required", true},
		{"add_to_cart bad asin", "amazon_add_to_cart", `{"asin":"bad-asin"}`, none, "Invalid ASIN format: bad-asin", true},
		{"add_to_cart navigate failure", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`, navFails, "Product navigation failed: " + navErr, true},
		{"add_to_cart locate failure", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`, evalFails, "Locate add-to-cart failed: " + evalErr, true},
		{
			"add_to_cart button not found", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`,
			fakeCDPOptions{evals: []fakeCDPEval{{match: "#add-to-cart-button", value: map[string]any{}}}},
			"Add to cart button not found for ASIN: B08N5WRWNW", true,
		},
		{"add_to_cart click failure", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`, clickFails, "Click add to cart failed: " + clickErr, true},
		{
			"add_to_cart success", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`, none,
			`{"added":true,"cartCount":"1","productTitle":"Echo Dot Gen 4","trusted":true}`, false,
		},
		{
			"add_to_cart cart count is best effort", "amazon_add_to_cart", `{"asin":"B08N5WRWNW"}`,
			fakeCDPOptions{evals: []fakeCDPEval{{match: "#nav-cart-count", err: "boom"}}},
			`{"added":true,"cartCount":"","productTitle":"Echo Dot Gen 4","trusted":true}`, false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, gotError := callKernelTool(t, tc.fake, tc.tool, tc.args)
			if !strings.Contains(got, tc.wantText) {
				t.Fatalf("text = %q, want substring %q", got, tc.wantText)
			}
			if gotError != tc.wantError {
				t.Fatalf("isError = %v, want %v (text %q)", gotError, tc.wantError, got)
			}
		})
	}
}

func TestRunServer_Execution(t *testing.T) {
	tests := []struct {
		name     string
		port     string
		canceled bool
		wantErr  string
	}{
		{"clean shutdown on context cancel", "0", true, ""},
		{"invalid port returns listen error", "-1", false, "invalid port"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			checkCDPErr(t, "RunServer", RunServer(ctx, &config.Config{Port: tc.port}), tc.wantErr)
		})
	}
}
