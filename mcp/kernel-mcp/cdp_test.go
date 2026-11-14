package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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
	corruptList      bool
	emptyTargets     bool
	includeChrome    bool
	onlyChrome       bool
	evalException    string
	detachOnMethod   string
	failPageEnable   bool
	failUserAgent    bool
	failAddScript    bool
	failStealthEval  bool
	failMouseMoved   bool
	failMousePress   bool
	failMouseRelease bool
	returnCDPError   bool
	withholdMethod   string
}

type fakeCDPServer struct {
	*httptest.Server

	attachedMu     sync.Mutex
	attachedTarget string
}

func (s *fakeCDPServer) getAttachedTarget() string {
	s.attachedMu.Lock()
	defer s.attachedMu.Unlock()
	return s.attachedTarget
}

func handleFakeCDPList(w http.ResponseWriter, r *http.Request, opts fakeCDPOptions) {
	if opts.failList {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if opts.corruptList {
		_, _ = w.Write([]byte("not valid json"))
		return
	}
	if opts.emptyTargets {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
		return
	}
	host := r.Host
	targets := []TargetInfo{}
	if opts.includeChrome || opts.onlyChrome {
		targets = append(targets, TargetInfo{
			ID:                   "chrome-target",
			Type:                 "page",
			URL:                  "chrome://newtab",
			WebSocketDebuggerURL: "ws://" + host + "/devtools/page/chrome-target",
		})
	}
	if !opts.onlyChrome {
		targets = append(targets, TargetInfo{
			ID:                   "page-target-1",
			Type:                 "page",
			URL:                  "https://www.example.com",
			WebSocketDebuggerURL: "ws://" + host + "/devtools/page/page-target-1",
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(targets)
}

type fakeCDPReq struct {
	ID     int64           `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func checkFakeCDPMethodFails(ws *websocket.Conn, req fakeCDPReq, opts fakeCDPOptions, wsMu *sync.Mutex) bool {
	if opts.returnCDPError && req.Method == "Input.insertText" {
		writeFakeCDPError(ws, req.ID, -32603, "Internal error executing command", wsMu)
		return true
	}
	if opts.failPageEnable && req.Method == "Page.enable" {
		writeFakeCDPError(ws, req.ID, -32000, "Page.enable failed", wsMu)
		return true
	}
	if opts.failUserAgent && req.Method == "Emulation.setUserAgentOverride" {
		writeFakeCDPError(ws, req.ID, -32000, "setUserAgentOverride failed", wsMu)
		return true
	}
	if opts.failAddScript && req.Method == "Page.addScriptToEvaluateOnNewDocument" {
		writeFakeCDPError(ws, req.ID, -32000, "addScriptToEvaluateOnNewDocument failed", wsMu)
		return true
	}
	return checkFakeCDPMouseFails(ws, req, opts, wsMu)
}

func checkFakeCDPMouseFails(ws *websocket.Conn, req fakeCDPReq, opts fakeCDPOptions, wsMu *sync.Mutex) bool {
	if req.Method != "Input.dispatchMouseEvent" {
		return false
	}
	var mp struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(req.Params, &mp)
	mouseFails := []struct {
		fail  bool
		mType string
		msg   string
	}{
		{opts.failMouseMoved, "mouseMoved", "mouseMoved error"},
		{opts.failMousePress, "mousePressed", "mousePressed error"},
		{opts.failMouseRelease, "mouseReleased", "mouseReleased error"},
	}
	for _, mf := range mouseFails {
		if mf.fail && mp.Type == mf.mType {
			writeFakeCDPError(ws, req.ID, -32000, mf.msg, wsMu)
			return true
		}
	}
	return false
}

func writeFakeCDPError(ws *websocket.Conn, id int64, code int, msg string, wsMu *sync.Mutex) {
	wsMu.Lock()
	_ = ws.WriteJSON(map[string]any{
		"id": id,
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
	wsMu.Unlock()
}

func handleFakeCDPMethod(ws *websocket.Conn, req fakeCDPReq, opts fakeCDPOptions, fakeSrv *fakeCDPServer, wsMu *sync.Mutex) (any, bool) {
	switch req.Method {
	case "Target.attachToTarget":
		var p struct {
			TargetID string `json:"targetId"`
		}
		_ = json.Unmarshal(req.Params, &p)
		fakeSrv.attachedMu.Lock()
		fakeSrv.attachedTarget = p.TargetID
		fakeSrv.attachedMu.Unlock()
		return map[string]any{"sessionId": "test-session-1"}, false
	case "Page.enable", "Emulation.setUserAgentOverride":
		return map[string]any{}, false
	case "Page.addScriptToEvaluateOnNewDocument":
		return map[string]any{"identifier": "1"}, false
	case "Runtime.evaluate":
		return handleFakeCDPEval(ws, req, opts, wsMu)
	case "Input.dispatchMouseEvent", "Input.insertText", "Page.navigate":
		return map[string]any{}, false
	case "Page.captureScreenshot":
		return handleFakeCDPScreenshot(ws, req, wsMu)
	default:
		return map[string]any{}, false
	}
}

func handleFakeCDPEval(ws *websocket.Conn, req fakeCDPReq, opts fakeCDPOptions, wsMu *sync.Mutex) (any, bool) {
	if opts.failStealthEval {
		writeFakeCDPError(ws, req.ID, -32000, "stealth eval failed", wsMu)
		return nil, true
	}
	if opts.evalException != "" {
		return map[string]any{
			"result": map[string]any{"type": "undefined"},
			"exceptionDetails": map[string]any{
				"text": opts.evalException, "exception": map[string]any{"description": opts.evalException},
			},
		}, false
	}
	return map[string]any{
		"result": map[string]any{
			"type":  "string",
			"value": "eval-success",
		},
	}, false
}

func handleFakeCDPScreenshot(ws *websocket.Conn, req fakeCDPReq, wsMu *sync.Mutex) (any, bool) {
	var p struct {
		Format string `json:"format"`
	}
	_ = json.Unmarshal(req.Params, &p)
	if p.Format == "fail-unmarshal" {
		wsMu.Lock()
		_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"id":`+strconv.FormatInt(req.ID, 10)+`,"result":"not-a-json-object"}`))
		wsMu.Unlock()
		return nil, true
	}
	return map[string]any{"data": "dGVzdC1zY3JlZW5zaG90"}, false
}

func handleFakeCDPWSLoop(ws *websocket.Conn, opts fakeCDPOptions, fakeSrv *fakeCDPServer, wsMu *sync.Mutex) {
	defer ws.Close()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		var req fakeCDPReq
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
		if opts.withholdMethod != "" && req.Method == opts.withholdMethod {
			continue
		}
		if checkFakeCDPMethodFails(ws, req, opts, wsMu) {
			continue
		}
		result, handled := handleFakeCDPMethod(ws, req, opts, fakeSrv, wsMu)
		if handled {
			continue
		}
		wsMu.Lock()
		_ = ws.WriteJSON(map[string]any{"id": req.ID, "result": result})
		wsMu.Unlock()
	}
}

func startFakeCDPServer(t *testing.T, opts fakeCDPOptions) (*fakeCDPServer, *Config) {
	t.Helper()

	var wsMu sync.Mutex
	var activeWS *websocket.Conn
	fakeSrv := &fakeCDPServer{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/json/list" {
			handleFakeCDPList(w, r, opts)
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

			go handleFakeCDPWSLoop(ws, opts, fakeSrv, &wsMu)
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

	fakeSrv.Server = server

	t.Cleanup(func() {
		wsMu.Lock()
		if activeWS != nil {
			_ = activeWS.Close()
		}
		wsMu.Unlock()
		server.Close()
	})

	return fakeSrv, cfg
}

func TestCDPClient_EnsureConnectionTable(t *testing.T) {
	tests := []struct {
		name               string
		opts               fakeCDPOptions
		wantErr            bool
		errSubstr          string
		wantAttachedTarget string
	}{
		{
			name:               "successful connection and stealth init",
			wantAttachedTarget: "page-target-1",
			wantErr:            false,
		},
		{
			name:               "target selection skips chrome:// url",
			opts:               fakeCDPOptions{includeChrome: true},
			wantAttachedTarget: "page-target-1",
			wantErr:            false,
		},
		{
			name:               "target selection falls back to chrome:// when only chrome target exists",
			opts:               fakeCDPOptions{onlyChrome: true},
			wantAttachedTarget: "chrome-target",
			wantErr:            false,
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
			name:      "corrupt targets returns error",
			opts:      fakeCDPOptions{corruptList: true},
			wantErr:   true,
			errSubstr: "parse cdp target response",
		},
		{
			name:      "stealth Page.enable failure",
			opts:      fakeCDPOptions{failPageEnable: true},
			wantErr:   true,
			errSubstr: "Page.enable",
		},
		{
			name:      "stealth Emulation.setUserAgentOverride failure",
			opts:      fakeCDPOptions{failUserAgent: true},
			wantErr:   true,
			errSubstr: "Emulation.setUserAgentOverride",
		},
		{
			name:      "stealth Page.addScriptToEvaluateOnNewDocument failure",
			opts:      fakeCDPOptions{failAddScript: true},
			wantErr:   true,
			errSubstr: "Page.addScriptToEvaluateOnNewDocument",
		},
		{
			name:      "stealth Runtime.evaluate failure",
			opts:      fakeCDPOptions{failStealthEval: true},
			wantErr:   true,
			errSubstr: "Runtime.evaluate stealth script",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, cfg := startFakeCDPServer(t, tc.opts)

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

			if tc.wantAttachedTarget != "" {
				if got := server.getAttachedTarget(); got != tc.wantAttachedTarget {
					t.Fatalf("attached target = %q, want %q", got, tc.wantAttachedTarget)
				}
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

func runCDPCommand(ctx context.Context, client *CDPClient, action string) error {
	switch action {
	case "click":
		return client.DispatchTrustedClick(ctx, 100, 200)
	case "type":
		return client.InsertText(ctx, "hello baddie")
	case "navigate":
		return client.Navigate(ctx, "https://www.example.com")
	case "screenshot":
		return verifyScreenshot(ctx, client, "png")
	case "screenshot-empty":
		return verifyScreenshot(ctx, client, "")
	case "screenshot-bad-json":
		_, err := client.CaptureScreenshot(ctx, "fail-unmarshal")
		return err
	case "eval":
		return verifyEval(ctx, client)
	case "wait-settle-zero":
		oldDelay := client.cfg.SettleDelay
		client.cfg.SettleDelay = 0
		defer func() { client.cfg.SettleDelay = oldDelay }()
		return client.WaitSettle(ctx)
	default:
		return fmt.Errorf("unknown action: %s", action)
	}
}

func verifyScreenshot(ctx context.Context, client *CDPClient, format string) error {
	data, err := client.CaptureScreenshot(ctx, format)
	if err != nil {
		return err
	}
	if data != "dGVzdC1zY3JlZW5zaG90" {
		return fmt.Errorf("CaptureScreenshot data = %q, want %q", data, "dGVzdC1zY3JlZW5zaG90")
	}
	return nil
}

func verifyEval(ctx context.Context, client *CDPClient) error {
	raw, err := client.EvaluateJS(ctx, "window.location.href", true, true)
	if err != nil {
		return err
	}
	if string(raw) != `"eval-success"` {
		return fmt.Errorf("EvaluateJS result = %s, want %q", string(raw), `"eval-success"`)
	}
	return nil
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
		{name: "navigate same host paced", action: "navigate"},
		{name: "screenshot png", action: "screenshot"},
		{name: "screenshot empty format defaults png", action: "screenshot-empty"},
		{name: "screenshot unmarshal failure", action: "screenshot-bad-json", expectErr: true},
		{name: "evaluate js", action: "eval"},
		{name: "wait settle zero delay", action: "wait-settle-zero"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := runCDPCommand(ctx, client, tc.action)
			if tc.expectErr && err == nil {
				t.Fatalf("%s expected error, got nil", tc.action)
			}
			if !tc.expectErr && err != nil {
				t.Fatalf("%s unexpected error: %v", tc.action, err)
			}
		})
	}
}

func executeCDPErrorAction(ctx context.Context, client *CDPClient, action string, server *fakeCDPServer) error {
	switch action {
	case "eval":
		_, err := client.EvaluateJS(ctx, "bad()", true, true)
		return err
	case "detach":
		_ = client.EnsureConnection(ctx)
		return client.InsertText(ctx, "trigger detach")
	case "settle-cancel":
		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cancelFn()
		client.cfg.SettleDelay = 500 * time.Millisecond
		client.waitSettleBestEffort(cancelCtx)
		return client.WaitSettle(cancelCtx)
	case "send-cdp-fail":
		server.Close()
		_, err := client.SendCDP(ctx, "Page.enable", nil)
		return err
	case "click-fail-moved", "click-fail-press", "click-fail-release":
		_ = client.EnsureConnection(ctx)
		return client.DispatchTrustedClick(ctx, 50, 50)
	case "cdp-error-resp":
		_ = client.EnsureConnection(ctx)
		return client.InsertText(ctx, "should error")
	case "send-cdp-canceled-ctx":
		_ = client.EnsureConnection(ctx)
		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cancelFn()
		_, err := client.sendCDPDirect(cancelCtx, client.activeConn, "Test.withheld", nil, "")
		return err
	case "send-cdp-write-err":
		_ = client.EnsureConnection(ctx)
		client.stateMu.Lock()
		conn := client.activeConn
		client.stateMu.Unlock()
		if conn != nil {
			_ = conn.UnderlyingConn().Close()
		}
		_, err := client.sendCDPDirect(ctx, conn, "Page.enable", nil, "")
		return err
	case "send-cdp-late-cancel":
		_ = client.EnsureConnection(ctx)
		lateCtx, lateCancel := context.WithCancel(context.Background())
		defer lateCancel()
		time.AfterFunc(20*time.Millisecond, lateCancel)
		_, err := client.sendCDPDirect(lateCtx, client.activeConn, "Test.withheld", nil, "")
		return err
	default:
		return nil
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
			opts:      fakeCDPOptions{withholdMethod: "Test.withheld"},
			action:    "send-cdp-canceled-ctx",
			wantErr:   true,
			errSubstr: "context canceled",
		},
		{
			name:      "send cdp with closed connection write error",
			action:    "send-cdp-write-err",
			wantErr:   true,
			errSubstr: "write cdp payload",
		},
		{
			name:      "send cdp with late context cancellation",
			opts:      fakeCDPOptions{withholdMethod: "Test.withheld"},
			action:    "send-cdp-late-cancel",
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

			err := executeCDPErrorAction(ctx, client, tc.action, server)
			if tc.wantErr && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
				t.Fatalf("action %s error = %v, want substring %q", tc.action, err, tc.errSubstr)
			}
		})
	}
}
