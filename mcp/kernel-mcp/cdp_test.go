package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var testUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type fakeCDPOptions struct {
	failList         bool
	emptyTargets     bool
	includeChrome    bool
	evalException    string
	detachOnMethod   string
	failPageEnable   bool
	failMouseMoved   bool
	failMousePress   bool
	failMouseRelease bool
	returnCDPError   bool
}

func startFakeCDPServer(t *testing.T, opts fakeCDPOptions) (*httptest.Server, *Config) {
	t.Helper()

	var wsMu sync.Mutex
	var activeWS *websocket.Conn

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/list" {
			if opts.failList {
				http.Error(w, "server error", http.StatusInternalServerError)
				return
			}
			if opts.emptyTargets {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[]`))
				return
			}
			host := r.Host
			targets := []TargetInfo{}
			if opts.includeChrome {
				targets = append(targets, TargetInfo{
					ID:                   "chrome-target",
					Type:                 "page",
					URL:                  "chrome://newtab",
					WebSocketDebuggerURL: "ws://" + host + "/devtools/page/chrome-target",
				})
			}
			targets = append(targets, TargetInfo{
				ID:                   "page-target-1",
				Type:                 "page",
				URL:                  "https://www.example.com",
				WebSocketDebuggerURL: "ws://" + host + "/devtools/page/page-target-1",
			})
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

					if opts.detachOnMethod != "" && req.Method == opts.detachOnMethod {
						_ = ws.WriteJSON(map[string]any{
							"method": "Target.detachedFromTarget",
							"params": map[string]any{"sessionId": "test-session-1"},
						})
						return
					}

					if opts.returnCDPError && req.Method == "Input.insertText" {
						wsMu.Lock()
						_ = ws.WriteJSON(map[string]any{
							"id": req.ID,
							"error": map[string]any{
								"code":    -32603,
								"message": "Internal error executing command",
							},
						})
						wsMu.Unlock()
						continue
					}

					if opts.failPageEnable && req.Method == "Page.enable" {
						wsMu.Lock()
						_ = ws.WriteJSON(map[string]any{
							"id": req.ID,
							"error": map[string]any{
								"code":    -32000,
								"message": "Page.enable failed",
							},
						})
						wsMu.Unlock()
						continue
					}

					if req.Method == "Input.dispatchMouseEvent" {
						var mp struct {
							Type string `json:"type"`
						}
						_ = json.Unmarshal(req.Params, &mp)
						if opts.failMouseMoved && mp.Type == "mouseMoved" {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "mouseMoved error",
								},
							})
							wsMu.Unlock()
							continue
						}
						if opts.failMousePress && mp.Type == "mousePressed" {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "mousePressed error",
								},
							})
							wsMu.Unlock()
							continue
						}
						if opts.failMouseRelease && mp.Type == "mouseReleased" {
							wsMu.Lock()
							_ = ws.WriteJSON(map[string]any{
								"id": req.ID,
								"error": map[string]any{
									"code":    -32000,
									"message": "mouseReleased error",
								},
							})
							wsMu.Unlock()
							continue
						}
					}

					var result any = map[string]any{}

					switch req.Method {
					case "Target.attachToTarget":
						result = map[string]any{"sessionId": "test-session-1"}
					case "Page.enable", "Emulation.setUserAgentOverride":
						result = map[string]any{}
					case "Page.addScriptToEvaluateOnNewDocument":
						result = map[string]any{"identifier": "1"}
					case "Runtime.evaluate":
						if opts.evalException != "" {
							result = map[string]any{
								"result": map[string]any{"type": "undefined"},
								"exceptionDetails": map[string]any{
									"text": opts.evalException, "exception": map[string]any{"description": opts.evalException},
								},
							}
						} else {
							result = map[string]any{
								"result": map[string]any{
									"type":  "string",
									"value": "eval-success",
								},
							}
						}
					case "Input.dispatchMouseEvent", "Input.insertText", "Page.navigate":
						result = map[string]any{}
					case "Page.captureScreenshot":
						result = map[string]any{
							"data": "dGVzdC1zY3JlZW5zaG90",
						}
					default:
						result = map[string]any{}
					}

					resp := map[string]any{
						"id":     req.ID,
						"result": result,
					}
					wsMu.Lock()
					_ = ws.WriteJSON(resp)
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

func TestCDPClient_EnsureConnectionTable(t *testing.T) {
	tests := []struct {
		name      string
		opts      fakeCDPOptions
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "successful connection and stealth init",
			wantErr: false,
		},
		{
			name:    "target selection skips chrome:// url",
			opts:    fakeCDPOptions{includeChrome: true},
			wantErr: false,
		},
		{
			name:      "fail list query returns error",
			opts:      fakeCDPOptions{failList: true},
			wantErr:   true,
			errSubstr: "cdp target query returned status 500",
		},
		{
			name:      "empty targets returns error",
			opts:      fakeCDPOptions{emptyTargets: true},
			wantErr:   true,
			errSubstr: "no open page target found",
		},
		{
			name:      "stealth Page.enable failure",
			opts:      fakeCDPOptions{failPageEnable: true},
			wantErr:   true,
			errSubstr: "Page.enable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, cfg := startFakeCDPServer(t, tc.opts)

			client := NewCDPClient(cfg)
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := client.EnsureConnection(ctx)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("EnsureConnection() expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("EnsureConnection() error = %q, want substring %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("EnsureConnection() unexpected error: %v", err)
			}

			// Calling EnsureStealthSession explicitly
			if err := client.EnsureStealthSession(ctx); err != nil {
				t.Fatalf("EnsureStealthSession() error: %v", err)
			}

			// Connected but stealthReady false
			client.stateMu.Lock()
			client.stealthReady = false
			client.stateMu.Unlock()
			if err := client.EnsureConnection(ctx); err != nil {
				t.Fatalf("EnsureConnection() re-stealth error: %v", err)
			}
		})
	}
}

func TestCDPClient_CommandsTable(t *testing.T) {
	_, cfg := startFakeCDPServer(t, fakeCDPOptions{})
	client := NewCDPClient(cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.EnsureConnection(ctx); err != nil {
		t.Fatalf("EnsureConnection failed: %v", err)
	}

	tests := []struct {
		name      string
		action    string
		expectErr bool
	}{
		{name: "trusted click", action: "click"},
		{name: "insert text", action: "type"},
		{name: "navigate", action: "navigate"},
		{name: "screenshot png", action: "screenshot"},
		{name: "screenshot empty format defaults png", action: "screenshot-empty"},
		{name: "evaluate js", action: "eval"},
		{name: "wait settle zero delay", action: "wait-settle-zero"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			switch tc.action {
			case "click":
				err = client.DispatchTrustedClick(ctx, 100, 200)
			case "type":
				err = client.InsertText(ctx, "hello baddie")
			case "navigate":
				err = client.Navigate(ctx, "https://www.example.com")
			case "screenshot":
				var data string
				data, err = client.CaptureScreenshot(ctx, "png")
				if err == nil && data != "dGVzdC1zY3JlZW5zaG90" {
					t.Fatalf("CaptureScreenshot data = %q, want %q", data, "dGVzdC1zY3JlZW5zaG90")
				}
			case "screenshot-empty":
				var data string
				data, err = client.CaptureScreenshot(ctx, "")
				if err == nil && data != "dGVzdC1zY3JlZW5zaG90" {
					t.Fatalf("CaptureScreenshot empty data = %q, want %q", data, "dGVzdC1zY3JlZW5zaG90")
				}
			case "eval":
				var raw json.RawMessage
				raw, err = client.EvaluateJS(ctx, "window.location.href", true, true)
				if err == nil && string(raw) != `"eval-success"` {
					t.Fatalf("EvaluateJS result = %s, want %q", string(raw), `"eval-success"`)
				}
			case "wait-settle-zero":
				oldDelay := client.cfg.SettleDelay
				client.cfg.SettleDelay = 0
				err = client.WaitSettle(ctx)
				client.cfg.SettleDelay = oldDelay
			}

			if tc.expectErr && err == nil {
				t.Fatalf("%s expected error, got nil", tc.action)
			}
			if !tc.expectErr && err != nil {
				t.Fatalf("%s unexpected error: %v", tc.action, err)
			}
		})
	}
}

func TestCDPClient_ErrorAndDisconnectTable(t *testing.T) {
	tests := []struct {
		name      string
		opts      fakeCDPOptions
		action    string
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "eval exception details returns error",
			opts:      fakeCDPOptions{evalException: "SyntaxError: Unexpected token"},
			action:    "eval",
			wantErr:   true,
			errSubstr: "javascript evaluation error: SyntaxError: Unexpected token",
		},
		{
			name:      "detachment disconnects connection",
			opts:      fakeCDPOptions{detachOnMethod: "Input.insertText"},
			action:    "detach",
			wantErr:   true,
			errSubstr: "connection closed",
		},
		{
			name:      "cancelled context aborts wait settle",
			action:    "settle-cancel",
			wantErr:   true,
			errSubstr: "context canceled",
		},
		{
			name:      "send cdp with dead server returns error",
			action:    "send-cdp-fail",
			wantErr:   true,
			errSubstr: "cdp target query failed",
		},
		{
			name:      "trusted click mouseMoved error",
			opts:      fakeCDPOptions{failMouseMoved: true},
			action:    "click-fail-moved",
			wantErr:   true,
			errSubstr: "mouseMoved",
		},
		{
			name:      "trusted click mousePressed error",
			opts:      fakeCDPOptions{failMousePress: true},
			action:    "click-fail-press",
			wantErr:   true,
			errSubstr: "mousePressed",
		},
		{
			name:      "trusted click mouseReleased error",
			opts:      fakeCDPOptions{failMouseRelease: true},
			action:    "click-fail-release",
			wantErr:   true,
			errSubstr: "mouseReleased",
		},
		{
			name:      "send cdp command returns cdp error code",
			opts:      fakeCDPOptions{returnCDPError: true},
			action:    "cdp-error-resp",
			wantErr:   true,
			errSubstr: "cdp error -32603",
		},
		{
			name:      "send cdp with pre-canceled context",
			action:    "send-cdp-canceled-ctx",
			wantErr:   true,
			errSubstr: "context canceled",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, cfg := startFakeCDPServer(t, tc.opts)

			client := NewCDPClient(cfg)
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			switch tc.action {
			case "eval":
				_, err := client.EvaluateJS(ctx, "bad()", true, true)
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("EvaluateJS error = %v, want %q", err, tc.errSubstr)
				}
			case "detach":
				_ = client.EnsureConnection(ctx)
				err := client.InsertText(ctx, "trigger detach")
				if tc.wantErr && err == nil {
					t.Fatalf("InsertText expected error on detach, got nil")
				}
			case "settle-cancel":
				cancelCtx, cancelFn := context.WithCancel(context.Background())
				cancelFn()
				client.cfg.SettleDelay = 500 * time.Millisecond
				err := client.WaitSettle(cancelCtx)
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("WaitSettle error = %v, want substring %q", err, tc.errSubstr)
				}
			case "send-cdp-fail":
				server.Close()
				_, err := client.SendCDP(ctx, "Page.enable", nil)
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("SendCDP error = %v, want substring %q", err, tc.errSubstr)
				}
			case "click-fail-moved", "click-fail-press", "click-fail-release":
				_ = client.EnsureConnection(ctx)
				err := client.DispatchTrustedClick(ctx, 50, 50)
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("DispatchTrustedClick error = %v, want substring %q", err, tc.errSubstr)
				}
			case "cdp-error-resp":
				_ = client.EnsureConnection(ctx)
				err := client.InsertText(ctx, "should error")
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("InsertText error = %v, want substring %q", err, tc.errSubstr)
				}
			case "send-cdp-canceled-ctx":
				_ = client.EnsureConnection(ctx)
				cancelCtx, cancelFn := context.WithCancel(context.Background())
				cancelFn()
				_, err := client.sendCDPDirect(cancelCtx, client.activeConn, "Page.enable", nil, "")
				if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("sendCDPDirect error = %v, want substring %q", err, tc.errSubstr)
				}
			}
		})
	}
}
