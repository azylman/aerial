package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/gorilla/websocket"
)

func startMainFakeCDPServer(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()

	var wsMu sync.Mutex
	var activeWS *websocket.Conn
	var lastNavURL string
	var cartNavCount int
	var cartEvalCount int
	var clickCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/list" {
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
			return
		}

		if strings.HasPrefix(r.URL.Path, "/devtools/page/") {
			ws, err := testUpgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			wsMu.Lock()
			activeWS = ws
			wsMu.Unlock()

			go func() {
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

					var result any = map[string]any{}

					switch req.Method {
					case "Target.attachToTarget":
						result = map[string]any{"sessionId": "sess-main-1"}
					case "Page.enable", "Emulation.setUserAgentOverride", "Page.addScriptToEvaluateOnNewDocument":
						result = map[string]any{}
					case "Page.navigate":
						var p struct {
							URL string `json:"url"`
						}
						_ = json.Unmarshal(req.Params, &p)
						wsMu.Lock()
						lastNavURL = p.URL
						wsMu.Unlock()

						wsMu.Lock()
						if strings.Contains(p.URL, "/cart/view.html") {
							cartNavCount++
						}
						failNav := strings.Contains(p.URL, "fail-navigate") || strings.Contains(p.URL, "failnav") || (strings.Contains(p.URL, "/cart/view.html") && cartNavCount == 2)
						wsMu.Unlock()

						if failNav {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "Cannot navigate",
								},
							})
							wsMu.Unlock()
							continue
						}
						result = map[string]any{}
					case "Runtime.evaluate":
						var p struct {
							Expression string `json:"expression"`
						}
						_ = json.Unmarshal(req.Params, &p)
						expr := p.Expression

						wsMu.Lock()
						currentNav := lastNavURL
						wsMu.Unlock()

						var evalVal any
						switch {
						case strings.Contains(expr, "StealthInitScript") || strings.Contains(expr, "navigator.webdriver"):
							evalVal = true
						case strings.Contains(expr, "fail-eval"):
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "Eval failed",
								},
							})
							wsMu.Unlock()
							continue
						case strings.Contains(expr, "cloneNode(true)"):
							evalVal = map[string]any{
								"title": "Amazon Test",
								"url":   "https://www.amazon.com",
								"text":  "Product details with card 4111-2222-3333-4444 here",
							}
						case strings.Contains(expr, "#sc-subtotal-amount-activecart"):
							wsMu.Lock()
							cartEvalCount++
							failEval := cartEvalCount == 2
							wsMu.Unlock()
							if failEval {
								wsMu.Lock()
								_ = ws.WriteJSON(map[string]any{
									"id": req.ID,
									"error": map[string]any{
										"code":    -32000,
										"message": "Cart extraction failed",
									},
								})
								wsMu.Unlock()
								continue
							}
							evalVal = map[string]any{
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
							}
						case strings.Contains(expr, "#add-to-cart-button"):
							if strings.Contains(currentNav, "failevl123") {
								wsMu.Lock()
								_ = ws.WriteJSON(map[string]any{
									"id": req.ID,
									"error": map[string]any{
										"code":    -32000,
										"message": "Locate add-to-cart failed",
									},
								})
								wsMu.Unlock()
								continue
							}
							if strings.Contains(currentNav, "B08N5WRWNX") {
								evalVal = map[string]any{
									"found":        false,
									"hasRect":      false,
									"x":            0,
									"y":            0,
									"productTitle": "",
								}
							} else {
								evalVal = map[string]any{
									"found":        true,
									"hasRect":      true,
									"x":            200,
									"y":            150,
									"productTitle": "Echo Dot Gen 4",
								}
							}
						case strings.Contains(expr, "#nav-cart-count"):
							evalVal = "1"
						case strings.Contains(expr, "s-search-result"):
							if strings.Contains(currentNav, "fail-search-eval") {
								wsMu.Lock()
								_ = ws.WriteJSON(map[string]any{
									"id": req.ID,
									"error": map[string]any{
										"code":    -32000,
										"message": "Search eval failed",
									},
								})
								wsMu.Unlock()
								continue
							}
							evalVal = []map[string]any{
								{
									"asin":    "B08N5WRWNW",
									"title":   "Echo Dot with card 4111-2222-3333-4444",
									"price":   "$49.99",
									"rating":  "4.7",
									"reviews": "1200",
									"prime":   true,
									"url":     "https://www.amazon.com/dp/B08N5WRWNW",
								},
							}
						case strings.Contains(expr, `"#not-found"`):
							evalVal = map[string]any{
								"found":   false,
								"tag":     "",
								"text":    "",
								"hasRect": false,
								"x":       0,
								"y":       0,
							}
						case strings.Contains(expr, `"#type-not-found"`):
							evalVal = map[string]any{
								"found":   false,
								"hasRect": false,
								"x":       0,
								"y":       0,
							}
						case strings.Contains(expr, "getBoundingClientRect"):
							evalVal = map[string]any{
								"found":   true,
								"tag":     "button",
								"text":    "Add to Cart",
								"hasRect": true,
								"x":       120,
								"y":       340,
							}
						case strings.Contains(expr, "throw new Error"):
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"result": map[string]any{
									"result": map[string]any{"type": "undefined"},
									"exceptionDetails": map[string]any{
										"text": "Uncaught Error: boom",
									},
								},
							})
							wsMu.Unlock()
							continue
						default:
							evalVal = "eval-ok"
						}

						result = map[string]any{
							"result": map[string]any{
								"type":  "object",
								"value": evalVal,
							},
						}
					case "Page.captureScreenshot":
						var p struct {
							Format string `json:"format"`
						}
						_ = json.Unmarshal(req.Params, &p)
						if p.Format == "fail" {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "Screenshot failed",
								},
							})
							wsMu.Unlock()
							continue
						}
						result = map[string]any{
							"data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
						}
					case "Input.dispatchMouseEvent":
						wsMu.Lock()
						clickCount++
						failClick := clickCount == 4
						wsMu.Unlock()
						if failClick {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "Click dispatch failed",
								},
							})
							wsMu.Unlock()
							continue
						}
						result = map[string]any{}
					case "Input.insertText":
						var ip struct {
							Text string `json:"text"`
						}
						_ = json.Unmarshal(req.Params, &ip)
						if ip.Text == "fail-insert" {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "Insert text failed",
								},
							})
							wsMu.Unlock()
							continue
						}
						result = map[string]any{}
					default:
						result = map[string]any{}
					}

					wsMu.Lock()
					_ = ws.WriteJSON(map[string]any{
						"id":     req.ID,
						"result": result,
					})
					wsMu.Unlock()
				}
			}()
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
		wsMu.Lock()
		if activeWS != nil {
			_ = activeWS.Close()
		}
		wsMu.Unlock()
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

func TestServer_MCPMethodsTable(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
	client := NewCDPClient(cfg)
	defer client.Close()
	srv := NewServer(cfg, client)
	mux := NewHTTPMux(srv)

	tests := []struct {
		name       string
		request    string
		wantSubstr string
		wantError  bool
	}{
		{
			name:       "invalid json returns parse error",
			request:    `{invalid`,
			wantSubstr: "Parse error",
			wantError:  true,
		},
		{
			name:       "unknown method returns method not found",
			request:    `{"jsonrpc":"2.0","id":1,"method":"foo/bar"}`,
			wantSubstr: "Method not found",
			wantError:  true,
		},
		{
			name:       "initialize returns capabilities and serverInfo",
			request:    `{"jsonrpc":"2.0","id":2,"method":"initialize"}`,
			wantSubstr: `"name":"kernel-mcp"`,
			wantError:  false,
		},
		{
			name:       "notifications/initialized returns 204",
			request:    `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			wantSubstr: "",
			wantError:  false,
		},
		{
			name:       "tools/list returns 9 tools",
			request:    `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
			wantSubstr: `"browser_navigate"`,
			wantError:  false,
		},
		{
			name:       "tools/call invalid params returns error",
			request:    `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":"invalid"}`,
			wantSubstr: "Invalid params",
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.request))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("%s: response = %q, want substring %q", tc.name, w.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_ToolsExecutionTable(t *testing.T) {
	_, cfg := startMainFakeCDPServer(t)
	client := NewCDPClient(cfg)
	defer client.Close()
	srv := NewServer(cfg, client)
	mux := NewHTTPMux(srv)

	tests := []struct {
		name       string
		toolName   string
		args       string
		wantSubstr string
		wantError  bool
	}{
		// 1. browser_navigate
		{
			name:       "browser_navigate missing url",
			toolName:   "browser_navigate",
			args:       `{}`,
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
			name:       "browser_evaluate missing expression",
			toolName:   "browser_evaluate",
			args:       `{}`,
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
			name:       "browser_get_content eval error",
			toolName:   "browser_get_content",
			args:       `{"selector":"fail-eval"}`,
			wantSubstr: "Content extraction failed",
			wantError:  true,
		},
		// 4. browser_click
		{
			name:       "browser_click missing selector",
			toolName:   "browser_click",
			args:       `{}`,
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
			name:       "browser_type missing selector",
			toolName:   "browser_type",
			args:       `{"text":"hello"}`,
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
			wantSubstr: `"type":"image"`,
			wantError:  false,
		},
		// 7. amazon_search
		{
			name:       "amazon_search missing query",
			toolName:   "amazon_search",
			args:       `{}`,
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
			name:       "amazon_add_to_cart missing asin",
			toolName:   "amazon_add_to_cart",
			args:       `{}`,
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
		// Prefix aliases: steel_ and kernel_ prefixes
		{
			name:       "steel_ prefix normalized to browser_navigate",
			toolName:   "steel_browser_navigate",
			args:       `{"url":"https://www.amazon.com"}`,
			wantSubstr: "Navigated to https://www.amazon.com",
			wantError:  false,
		},
		{
			name:       "kernel_ prefix normalized to browser_navigate",
			toolName:   "kernel_browser_navigate",
			args:       `{"url":"https://www.amazon.com"}`,
			wantSubstr: "Navigated to https://www.amazon.com",
			wantError:  false,
		},
		{
			name:       "unknown tool returns error",
			toolName:   "unknown_tool",
			args:       `{}`,
			wantSubstr: "Unknown tool: unknown_tool",
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
