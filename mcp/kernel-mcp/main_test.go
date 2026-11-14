package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mainFakeCDPState struct {
	wsMu          sync.Mutex
	activeWS      *websocket.Conn
	lastNavURL    string
	cartNavCount  int
	cartEvalCount int
	clickCount    int
}

func writeMainCDPError(ws *websocket.Conn, mu *sync.Mutex, id int64, msg string) {
	mu.Lock()
	defer mu.Unlock()
	_ = ws.WriteJSON(map[string]any{
		"id": id,
		"error": map[string]any{
			"code":    -32000,
			"message": msg,
		},
	})
}

func writeMainCDPResult(ws *websocket.Conn, mu *sync.Mutex, id int64, result any) {
	mu.Lock()
	defer mu.Unlock()
	_ = ws.WriteJSON(map[string]any{
		"id":     id,
		"result": result,
	})
}

func handleMainFakeCDPList(w http.ResponseWriter, r *http.Request) {
	targets := []TargetInfo{
		{
			ID:                   "main-page-1",
			Type:                 "page",
			URL:                  "https://www.amazon.com",
			WebSocketDebuggerURL: "ws://" + r.Host + "/devtools/page/main-page-1",
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(targets)
}

func handleMainFakeCDPNavigate(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, params json.RawMessage) {
	var p struct {
		URL string `json:"url"`
	}
	_ = json.Unmarshal(params, &p)
	state.wsMu.Lock()
	state.lastNavURL = p.URL
	if strings.Contains(p.URL, "/cart/view.html") {
		state.cartNavCount++
	}
	failNav := strings.Contains(p.URL, "fail-navigate") || strings.Contains(p.URL, "failnav") || (strings.Contains(p.URL, "/cart/view.html") && state.cartNavCount == 2)
	state.wsMu.Unlock()

	if failNav {
		writeMainCDPError(ws, &state.wsMu, reqID, "Cannot navigate")
		return
	}
	writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{})
}

func evaluateAmazonCart(ws *websocket.Conn, state *mainFakeCDPState, reqID int64) (any, bool) {
	state.wsMu.Lock()
	state.cartEvalCount++
	failEval := state.cartEvalCount == 2
	state.wsMu.Unlock()
	if failEval {
		writeMainCDPError(ws, &state.wsMu, reqID, "Cart extraction failed")
		return nil, false
	}
	return map[string]any{
		"subtotal": "$49.99",
		"count":    "1 item",
		"items": []map[string]any{
			{
				"asin":     "B08N5WRWNW",
				"title":    "Echo Dot 4111-2222-3333-4444",
				"price":    "$49.99",
				"quantity": "1",
			},
		},
	}, true
}

func evaluateAmazonAddToCart(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, currentNav string) (any, bool) {
	if strings.Contains(currentNav, "failevl123") {
		writeMainCDPError(ws, &state.wsMu, reqID, "Locate add-to-cart failed")
		return nil, false
	}
	if strings.Contains(currentNav, "B08N5WRWNX") {
		return map[string]any{
			"found":        false,
			"hasRect":      false,
			"x":            0,
			"y":            0,
			"productTitle": "",
		}, true
	}
	return map[string]any{
		"found":        true,
		"hasRect":      true,
		"x":            200,
		"y":            150,
		"productTitle": "Echo Dot Gen 4",
	}, true
}

func evaluateAmazonSearch(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, currentNav string) (any, bool) {
	if strings.Contains(currentNav, "fail-search-eval") {
		writeMainCDPError(ws, &state.wsMu, reqID, "Search eval failed")
		return nil, false
	}
	return []map[string]any{
		{
			"asin":    "B08N5WRWNW",
			"title":   "Echo Dot with card 4111-2222-3333-4444",
			"price":   "$49.99",
			"rating":  "4.7",
			"reviews": "1200",
			"prime":   true,
			"url":     "https://www.amazon.com/dp/B08N5WRWNW",
		},
	}, true
}

func handleMainFakeCDPEvaluate(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, params json.RawMessage) {
	var p struct {
		Expression string `json:"expression"`
	}
	_ = json.Unmarshal(params, &p)
	expr := p.Expression

	state.wsMu.Lock()
	currentNav := state.lastNavURL
	state.wsMu.Unlock()

	switch {
	case strings.Contains(expr, "fail-eval"):
		writeMainCDPError(ws, &state.wsMu, reqID, "Eval failed")
		return
	case strings.Contains(expr, "throw new Error"):
		state.wsMu.Lock()
		_ = ws.WriteJSON(map[string]any{
			"id": reqID,
			"result": map[string]any{
				"result": map[string]any{"type": "undefined"},
				"exceptionDetails": map[string]any{
					"text": "Uncaught Error: boom",
				},
			},
		})
		state.wsMu.Unlock()
		return
	case strings.Contains(expr, "#sc-subtotal-amount-activecart"):
		if evalVal, ok := evaluateAmazonCart(ws, state, reqID); ok {
			writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{"result": map[string]any{"type": "object", "value": evalVal}})
		}
		return
	case strings.Contains(expr, "#add-to-cart-button"):
		if evalVal, ok := evaluateAmazonAddToCart(ws, state, reqID, currentNav); ok {
			writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{"result": map[string]any{"type": "object", "value": evalVal}})
		}
		return
	case strings.Contains(expr, "s-search-result"):
		if evalVal, ok := evaluateAmazonSearch(ws, state, reqID, currentNav); ok {
			writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{"result": map[string]any{"type": "object", "value": evalVal}})
		}
		return
	}

	evalVal := evaluateRemainingExpressions(expr)
	writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{
		"result": map[string]any{
			"type":  "object",
			"value": evalVal,
		},
	})
}

func evaluateRemainingExpressions(expr string) any {
	switch {
	case strings.Contains(expr, "StealthInitScript") || strings.Contains(expr, "navigator.webdriver"):
		return true
	case strings.Contains(expr, "cloneNode(true)"):
		return map[string]any{
			"title": "Amazon Test",
			"url":   "https://www.amazon.com",
			"text":  "Product details with card 4111-2222-3333-4444 here",
		}
	case strings.Contains(expr, "#nav-cart-count"):
		return "1"
	case strings.Contains(expr, `"#not-found"`):
		return map[string]any{"found": false, "tag": "", "text": "", "hasRect": false, "x": 0, "y": 0}
	case strings.Contains(expr, `"#type-not-found"`):
		return map[string]any{"found": false, "hasRect": false, "x": 0, "y": 0}
	case strings.Contains(expr, "getBoundingClientRect"):
		return map[string]any{"found": true, "tag": "button", "text": "Add to Cart", "hasRect": true, "x": 120, "y": 340}
	default:
		return "eval-ok"
	}
}

func handleMainFakeCDPScreenshot(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, params json.RawMessage) {
	var p struct {
		Format string `json:"format"`
	}
	_ = json.Unmarshal(params, &p)
	if p.Format == "fail" {
		writeMainCDPError(ws, &state.wsMu, reqID, "Screenshot failed")
		return
	}
	data := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
	if p.Format == "rawtext" {
		data = "not-base64!"
	}
	writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{"data": data})
}

func handleMainFakeCDPMouse(ws *websocket.Conn, state *mainFakeCDPState, reqID int64) {
	state.wsMu.Lock()
	state.clickCount++
	failClick := state.clickCount == 4
	state.wsMu.Unlock()
	if failClick {
		writeMainCDPError(ws, &state.wsMu, reqID, "Click dispatch failed")
		return
	}
	writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{})
}

func handleMainFakeCDPInsertText(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, params json.RawMessage) {
	var ip struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(params, &ip)
	if ip.Text == "fail-insert" {
		writeMainCDPError(ws, &state.wsMu, reqID, "Insert text failed")
		return
	}
	writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{})
}

func handleMainFakeCDPMethod(ws *websocket.Conn, state *mainFakeCDPState, reqID int64, method string, params json.RawMessage) {
	switch method {
	case "Target.attachToTarget":
		writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{"sessionId": "sess-main-1"})
	case "Page.enable", "Emulation.setUserAgentOverride", "Page.addScriptToEvaluateOnNewDocument":
		writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{})
	case "Page.navigate":
		handleMainFakeCDPNavigate(ws, state, reqID, params)
	case "Runtime.evaluate":
		handleMainFakeCDPEvaluate(ws, state, reqID, params)
	case "Page.captureScreenshot":
		handleMainFakeCDPScreenshot(ws, state, reqID, params)
	case "Input.dispatchMouseEvent":
		handleMainFakeCDPMouse(ws, state, reqID)
	case "Input.insertText":
		handleMainFakeCDPInsertText(ws, state, reqID, params)
	default:
		writeMainCDPResult(ws, &state.wsMu, reqID, map[string]any{})
	}
}

func handleMainFakeCDPWSLoop(ws *websocket.Conn, state *mainFakeCDPState) {
	defer ws.Close()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}

		var req struct {
			ID     int64           `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &req); err != nil {
			continue
		}

		handleMainFakeCDPMethod(ws, state, req.ID, req.Method, req.Params)
	}
}

func handleMainFakeCDPDevTools(w http.ResponseWriter, r *http.Request, state *mainFakeCDPState) {
	ws, err := testUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	state.wsMu.Lock()
	state.activeWS = ws
	state.wsMu.Unlock()

	go handleMainFakeCDPWSLoop(ws, state)
}

func startMainFakeCDPServer(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	state := &mainFakeCDPState{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/list" {
			handleMainFakeCDPList(w, r)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/devtools/page/") {
			handleMainFakeCDPDevTools(w, r, state)
			return
		}

		http.NotFound(w, r)
	}))

	u, _ := url.Parse(server.URL)
	cfg := &Config{
		Port:        "4018",
		CDPHost:     u.Hostname(),
		CDPPort:     u.Port(),
		SettleDelay: 1 * time.Millisecond,
	}

	t.Cleanup(func() {
		state.wsMu.Lock()
		if state.activeWS != nil {
			_ = state.activeWS.Close()
		}
		state.wsMu.Unlock()
		server.Close()
	})

	return server, cfg
}

func TestServer_HTTPRoutesTable(t *testing.T) {
	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	httpkit.WriteCountersDirFn = func(dir string) error { return nil }
	httpkit.WriteMetaDirFn = func(dir string) error { return nil }
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	_, cfg := startMainFakeCDPServer(t)
	client := NewCDPClient(cfg)
	defer client.Close()
	srv := NewServer(cfg, client)
	mux := NewHTTPMux(srv)

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "health GET returns 200",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "health HEAD returns 200",
			method:     http.MethodHead,
			path:       "/health",
			wantStatus: http.StatusOK,
		},
		{
			name:       "health POST returns 405",
			method:     http.MethodPost,
			path:       "/health",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "coverage flush returns 200",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
		{
			name:       "mcp GET returns ready banner",
			method:     http.MethodGet,
			path:       "/mcp",
			wantStatus: http.StatusOK,
			wantSubstr: "kernel-mcp ready",
		},
		{
			name:       "mcp empty body returns 400",
			method:     http.MethodPost,
			path:       "/mcp",
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Empty request body",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("%s: status = %d, want %d", tc.name, w.Code, tc.wantStatus)
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("%s: body = %q, want substring %q", tc.name, w.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServerInstructions(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
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

func TestServer_ToolsListSchemaTable(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
	client := NewCDPClient(cfg)
	defer client.Close()
	mux := NewHTTPMux(NewServer(cfg, client))

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				InputSchema struct {
					Properties map[string]struct {
						Type string   `json:"type"`
						Enum []string `json:"enum"`
					} `json:"properties"`
					Required []string `json:"required"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode tools/list %q: %v", w.Body.String(), err)
	}
	type schemaShape struct {
		props    map[string]string
		required []string
	}
	got := make(map[string]schemaShape)
	for _, tool := range resp.Result.Tools {
		shape := schemaShape{props: make(map[string]string), required: tool.InputSchema.Required}
		for name, prop := range tool.InputSchema.Properties {
			shape.props[name] = prop.Type
			if len(prop.Enum) > 0 {
				shape.props[name] += " enum"
			}
		}
		got[tool.Name] = shape
	}

	tests := []struct {
		tool         string
		wantProps    map[string]string
		wantRequired []string
	}{
		{tool: "browser_navigate", wantProps: map[string]string{"url": "string"}, wantRequired: []string{"url"}},
		{tool: "browser_evaluate", wantProps: map[string]string{"expression": "string", "awaitPromise": "boolean", "returnByValue": "boolean"}, wantRequired: []string{"expression"}},
		{tool: "browser_get_content", wantProps: map[string]string{"selector": "string", "maxLength": "integer", "max_chars": "integer"}},
		{tool: "browser_click", wantProps: map[string]string{"selector": "string"}, wantRequired: []string{"selector"}},
		{tool: "browser_type", wantProps: map[string]string{"selector": "string", "text": "string", "clear": "boolean"}, wantRequired: []string{"selector", "text"}},
		{tool: "browser_screenshot", wantProps: map[string]string{"format": "string"}},
		{tool: "amazon_search", wantProps: map[string]string{"query": "string", "maxResults": "integer"}, wantRequired: []string{"query"}},
		{tool: "amazon_get_cart", wantProps: map[string]string{}},
		{tool: "amazon_add_to_cart", wantProps: map[string]string{"asin": "string"}, wantRequired: []string{"asin"}},
	}
	if len(got) != len(tests) {
		t.Fatalf("tools/list returned %d tools, want %d", len(got), len(tests))
	}
	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			shape, ok := got[tc.tool]
			if !ok {
				t.Fatalf("tool %q missing from tools/list", tc.tool)
			}
			if !maps.Equal(shape.props, tc.wantProps) {
				t.Errorf("properties = %v, want %v", shape.props, tc.wantProps)
			}
			gotRequired := slices.Sorted(slices.Values(shape.required))
			if !slices.Equal(gotRequired, slices.Sorted(slices.Values(tc.wantRequired))) {
				t.Errorf("required = %v, want %v", shape.required, tc.wantRequired)
			}
		})
	}
}

type serverToolsExecutionCase struct {
	name       string
	toolName   string
	args       string
	wantSubstr string
	wantError  bool
}

func browserBasicToolsExecutionCases() []serverToolsExecutionCase {
	return []serverToolsExecutionCase{
		// 1. browser_navigate
		{
			name:       "browser_navigate blank url",
			toolName:   "browser_navigate",
			args:       `{"url":"  "}`,
			wantSubstr: "Argument 'url' is required",
			wantError:  true,
		},
		{
			name:       "browser_navigate navigation failure",
			toolName:   "browser_navigate",
			args:       `{"url":"https://fail-navigate.com"}`,
			wantSubstr: "Navigation failed",
			wantError:  true,
		},
		{
			name:       "browser_navigate success",
			toolName:   "browser_navigate",
			args:       `{"url":"https://www.amazon.com"}`,
			wantSubstr: "Navigated to https://www.amazon.com",
			wantError:  false,
		},
		// 2. browser_evaluate
		{
			name:       "browser_evaluate blank expression",
			toolName:   "browser_evaluate",
			args:       `{"expression":" "}`,
			wantSubstr: "Argument 'expression' is required",
			wantError:  true,
		},
		{
			name:       "browser_evaluate success",
			toolName:   "browser_evaluate",
			args:       `{"expression":"window.location.href"}`,
			wantSubstr: "eval-ok",
			wantError:  false,
		},
		{
			name:       "browser_evaluate with explicit false options",
			toolName:   "browser_evaluate",
			args:       `{"expression":"window.location.href","awaitPromise":false,"returnByValue":false}`,
			wantSubstr: "eval-ok",
			wantError:  false,
		},
		{
			name:       "browser_evaluate exception details",
			toolName:   "browser_evaluate",
			args:       `{"expression":"throw new Error('boom')"}`,
			wantSubstr: "Uncaught Error: boom",
			wantError:  true,
		},
		{
			name:       "browser_evaluate blocks unsafe patterns",
			toolName:   "browser_evaluate",
			args:       `{"expression":"fetch('https://www.amazon.com/s?k=gpu')"}`,
			wantSubstr: "blocked unsafe browser_evaluate pattern",
			wantError:  true,
		},
		// 3. browser_get_content
		{
			name:       "browser_get_content success and sanitizes card PII",
			toolName:   "browser_get_content",
			args:       `{"selector":"body","maxLength":15000}`,
			wantSubstr: "[REDACTED_CARD]",
			wantError:  false,
		},
		{
			name:       "browser_get_content accepts legacy max_chars alias",
			toolName:   "browser_get_content",
			args:       `{"max_chars":5}`,
			wantSubstr: `...[truncated]`,
			wantError:  false,
		},
		{
			name:       "browser_get_content eval error",
			toolName:   "browser_get_content",
			args:       `{"selector":"fail-eval"}`,
			wantSubstr: "Content extraction failed",
			wantError:  true,
		},
	}
}

func browserInteractionToolsExecutionCases() []serverToolsExecutionCase {
	return []serverToolsExecutionCase{
		// 4. browser_click
		{
			name:       "browser_click blank selector",
			toolName:   "browser_click",
			args:       `{"selector":""}`,
			wantSubstr: "Argument 'selector' is required",
			wantError:  true,
		},
		{
			name:       "browser_click element not found",
			toolName:   "browser_click",
			args:       `{"selector":"#not-found"}`,
			wantSubstr: "Element not found",
			wantError:  true,
		},
		{
			name:       "browser_click eval failure",
			toolName:   "browser_click",
			args:       `{"selector":"fail-eval"}`,
			wantSubstr: "Click script evaluation failed",
			wantError:  true,
		},
		{
			name:       "browser_click success with trusted click",
			toolName:   "browser_click",
			args:       `{"selector":".btn"}`,
			wantSubstr: "clicked",
			wantError:  false,
		},
		{
			name:       "browser_click dispatch trusted click failure",
			toolName:   "browser_click",
			args:       `{"selector":".btn"}`,
			wantSubstr: "Dispatch trusted click failed",
			wantError:  true,
		},
		// 5. browser_type
		{
			name:       "browser_type blank selector",
			toolName:   "browser_type",
			args:       `{"selector":"","text":"hello"}`,
			wantSubstr: "Argument 'selector' is required",
			wantError:  true,
		},
		{
			name:       "browser_type element not found",
			toolName:   "browser_type",
			args:       `{"selector":"#type-not-found","text":"hello"}`,
			wantSubstr: "Element not found",
			wantError:  true,
		},
		{
			name:       "browser_type eval failure",
			toolName:   "browser_type",
			args:       `{"selector":"fail-eval","text":"hello"}`,
			wantSubstr: "Focus evaluation failed",
			wantError:  true,
		},
		{
			name:       "browser_type insertText failure",
			toolName:   "browser_type",
			args:       `{"selector":"input#query","text":"fail-insert"}`,
			wantSubstr: "Input.insertText failed",
			wantError:  true,
		},
		{
			name:       "browser_type success",
			toolName:   "browser_type",
			args:       `{"selector":"input#query","text":"ergonomic chair","clear":true}`,
			wantSubstr: "typed",
			wantError:  false,
		},
		// 6. browser_screenshot
		{
			name:       "browser_screenshot failure",
			toolName:   "browser_screenshot",
			args:       `{"format":"fail"}`,
			wantSubstr: "Screenshot capture failed",
			wantError:  true,
		},
		{
			name:       "browser_screenshot default format",
			toolName:   "browser_screenshot",
			args:       `{}`,
			wantSubstr: `"type":"image"`,
			wantError:  false,
		},
		{
			name:       "browser_screenshot returns image and text",
			toolName:   "browser_screenshot",
			args:       `{"format":"png"}`,
			wantSubstr: `Screenshot captured (png)`,
			wantError:  false,
		},
		{
			name:       "browser_screenshot lowercases format into mime type",
			toolName:   "browser_screenshot",
			args:       `{"format":" JPEG "}`,
			wantSubstr: `"mimeType":"image/jpeg"`,
			wantError:  false,
		},
		{
			name:       "browser_screenshot undecodable data falls back to text",
			toolName:   "browser_screenshot",
			args:       `{"format":"rawtext"}`,
			wantSubstr: `not-base64!`,
			wantError:  false,
		},
	}
}

func browserToolsExecutionCases() []serverToolsExecutionCase {
	basic := browserBasicToolsExecutionCases()
	interact := browserInteractionToolsExecutionCases()
	cases := make([]serverToolsExecutionCase, 0, len(basic)+len(interact))
	cases = append(cases, basic...)
	cases = append(cases, interact...)
	return cases
}

func amazonToolsExecutionCases() []serverToolsExecutionCase {
	return []serverToolsExecutionCase{
		// 7. amazon_search
		{
			name:       "amazon_search blank query",
			toolName:   "amazon_search",
			args:       `{"query":"   "}`,
			wantSubstr: "Argument 'query' is required",
			wantError:  true,
		},
		{
			name:       "amazon_search navigate failure",
			toolName:   "amazon_search",
			args:       `{"query":"fail-navigate"}`,
			wantSubstr: "Amazon search navigation failed",
			wantError:  true,
		},
		{
			name:       "amazon_search eval failure",
			toolName:   "amazon_search",
			args:       `{"query":"fail-search-eval"}`,
			wantSubstr: "Amazon search extraction failed",
			wantError:  true,
		},
		{
			name:       "amazon_search success and sanitizes card PII",
			toolName:   "amazon_search",
			args:       `{"query":"echo dot","maxResults":3}`,
			wantSubstr: "[REDACTED_CARD]",
			wantError:  false,
		},
		// 8. amazon_get_cart
		{
			name:       "amazon_get_cart success and sanitizes card PII",
			toolName:   "amazon_get_cart",
			args:       `{}`,
			wantSubstr: "[REDACTED_CARD]",
			wantError:  false,
		},
		{
			name:       "amazon_get_cart navigation failure",
			toolName:   "amazon_get_cart",
			args:       `{}`,
			wantSubstr: "Amazon cart navigation failed",
			wantError:  true,
		},
		{
			name:       "amazon_get_cart extraction failure",
			toolName:   "amazon_get_cart",
			args:       `{}`,
			wantSubstr: "Amazon cart extraction failed",
			wantError:  true,
		},
		// 9. amazon_add_to_cart
		{
			name:       "amazon_add_to_cart blank asin",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":" "}`,
			wantSubstr: "Argument 'asin' is required",
			wantError:  true,
		},
		{
			name:       "amazon_add_to_cart invalid asin",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":"invalid-asin-length"}`,
			wantSubstr: "Invalid ASIN format",
			wantError:  true,
		},
		{
			name:       "amazon_add_to_cart navigate failure",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":"failnav123"}`,
			wantSubstr: "Product navigation failed",
			wantError:  true,
		},
		{
			name:       "amazon_add_to_cart locate failure",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":"failevl123"}`,
			wantSubstr: "Locate add-to-cart failed",
			wantError:  true,
		},
		{
			name:       "amazon_add_to_cart button not found",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":"B08N5WRWNX"}`,
			wantSubstr: "Add to cart button not found",
			wantError:  true,
		},
		{
			name:       "amazon_add_to_cart success",
			toolName:   "amazon_add_to_cart",
			args:       `{"asin":"B08N5WRWNW"}`,
			wantSubstr: "added",
			wantError:  false,
		},
		// Prefix alias: kernel_ resolves to bare names via ServeMCPStream.
		{
			name:       "kernel_ prefix normalized to browser_navigate",
			toolName:   "kernel_browser_navigate",
			args:       `{"url":"https://www.amazon.com"}`,
			wantSubstr: "Navigated to https://www.amazon.com",
			wantError:  false,
		},
	}
}

func serverToolsExecutionTestCases() []serverToolsExecutionCase {
	b := browserToolsExecutionCases()
	a := amazonToolsExecutionCases()
	cases := make([]serverToolsExecutionCase, 0, len(b)+len(a))
	cases = append(cases, b...)
	cases = append(cases, a...)
	return cases
}

func runServerToolsExecutionCase(t *testing.T, mux http.Handler, tc serverToolsExecutionCase) {
	t.Helper()
	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      100,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      tc.toolName,
			"arguments": json.RawMessage(tc.args),
		},
	}
	reqBytes, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(reqBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, tc.wantSubstr) {
		t.Fatalf("%s: body = %q, want substring %q", tc.name, body, tc.wantSubstr)
	}
	if tc.wantError && !strings.Contains(body, `"isError":true`) && !strings.Contains(body, `"error"`) {
		t.Fatalf("%s: expected isError:true or error object in response: %s", tc.name, body)
	}
}

func TestServer_ToolsExecutionTable(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
	client := NewCDPClient(cfg)
	defer client.Close()
	srv := NewServer(cfg, client)
	mux := NewHTTPMux(srv)

	tests := serverToolsExecutionTestCases()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runServerToolsExecutionCase(t, mux, tc)
		})
	}
}

func TestRunServer_Execution(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
	cfg.Port = "0"

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately to test clean shutdown

	err := runServer(ctx, cfg)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) {
		t.Fatalf("runServer() error: %v", err)
	}

	// Test bad port error return
	badCfg := *cfg
	badCfg.Port = "-1"
	err = runServer(context.Background(), &badCfg)
	if err == nil {
		t.Fatalf("expected error for invalid port")
	}
}

func TestMain_Execution_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	badConfigPath := filepath.Join(tmpDir, "bad_config.yaml")
	_ = os.WriteFile(badConfigPath, []byte("port: [unclosed"), 0o600)

	tests := []struct {
		name       string
		configPath string
		runErr     error
		wantExit   bool
	}{
		{"success exit", "", nil, false},
		{"server closed error", "", http.ErrServerClosed, false},
		{"fatal error calls exitFn", "", errors.New("fatal server error"), true},
		{"config load error calls exitFn", badConfigPath, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRun := runServerFn
			oldExit := exitFn
			exitCalled := false

			if tc.configPath != "" {
				t.Setenv("CONFIG_PATH", tc.configPath)
			} else {
				t.Setenv("CONFIG_PATH", "")
			}

			runServerFn = func(ctx context.Context, cfg *Config) error {
				return tc.runErr
			}
			exitFn = func(format string, v ...any) {
				exitCalled = true
			}

			t.Cleanup(func() {
				runServerFn = oldRun
				exitFn = oldExit
			})

			main()

			if exitCalled != tc.wantExit {
				t.Fatalf("exitCalled = %v, want %v", exitCalled, tc.wantExit)
			}
		})
	}
}
