package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
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
	emptyHost        bool
	failList         bool
	corruptList      bool
	emptyTargets     bool
	includeChrome    bool
	onlyChrome       bool
	evalException    string
	detachOnMethod   string
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
	if opts.emptyHost {
		cfg.CDPHost = ""
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
			name:      "unconfigured cdp host fails fast",
			opts:      fakeCDPOptions{emptyHost: true},
			wantErr:   true,
			errSubstr: "cdp host is not configured",
		},
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

			if err := client.EnsureConnection(ctx); err != nil {
				t.Fatalf("EnsureConnection() fast-path error: %v", err)
			}
		})
	}
}

// startCDPClient starts a fake CDP server with opts and returns it with a
// client and a bounded context, all torn down via t.Cleanup.
func startCDPClient(t *testing.T, opts fakeCDPOptions) (*fakeCDPServer, *CDPClient, context.Context) {
	t.Helper()
	server, cfg := startFakeCDPServer(t, opts)
	client := NewCDPClient(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
	})
	return server, client, ctx
}

// checkCDPErr fails the test unless err matches wantErr ("" means no error).
func checkCDPErr(t *testing.T, call string, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("%s unexpected error: %v", call, err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("%s error = %v, want substring %q", call, err, wantErr)
	}
}

func TestCDPClient_DispatchTrustedClick(t *testing.T) {
	tests := []struct {
		name    string
		opts    fakeCDPOptions
		wantErr string
	}{
		{"three step click succeeds", fakeCDPOptions{}, ""},
		{"mouseMoved error", fakeCDPOptions{failMouseMoved: true}, "mouseMoved: cdp error -32000"},
		{"mousePressed error", fakeCDPOptions{failMousePress: true}, "mousePressed: cdp error -32000"},
		{"mouseReleased error", fakeCDPOptions{failMouseRelease: true}, "mouseReleased: cdp error -32000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, tc.opts)
			checkCDPErr(t, "DispatchTrustedClick", client.DispatchTrustedClick(ctx, 100, 200), tc.wantErr)
		})
	}
}

func TestCDPClient_InsertText(t *testing.T) {
	tests := []struct {
		name    string
		opts    fakeCDPOptions
		wantErr string
	}{
		{"insert text succeeds", fakeCDPOptions{}, ""},
		{"target detach closes connection", fakeCDPOptions{detachOnMethod: "Input.insertText"}, "cdp connection closed"},
		{"cdp error response", fakeCDPOptions{returnCDPError: true}, "cdp error -32603: Internal error executing command"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, tc.opts)
			checkCDPErr(t, "InsertText", client.InsertText(ctx, "hello there"), tc.wantErr)
		})
	}
}

func TestCDPClient_Navigate(t *testing.T) {
	tests := []struct {
		name        string
		lastNavHost string
		url         string
	}{
		{"first navigation", "", "https://www.example.com"},
		{"navigate same host paced", "www.example.com", "https://www.example.com/next"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, fakeCDPOptions{})
			client.lastNavHost = tc.lastNavHost
			checkCDPErr(t, "Navigate", client.Navigate(ctx, tc.url), "")
			client.stateMu.Lock()
			got := client.lastNavHost
			client.stateMu.Unlock()
			if got != "www.example.com" {
				t.Fatalf("lastNavHost = %q, want %q", got, "www.example.com")
			}
		})
	}
}

func TestCDPClient_CaptureScreenshot(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		want    string
		wantErr string
	}{
		{"png", "png", "dGVzdC1zY3JlZW5zaG90", ""},
		{"empty format defaults png", "", "dGVzdC1zY3JlZW5zaG90", ""},
		{"unmarshal failure", "fail-unmarshal", "", "unmarshal screenshot result"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, fakeCDPOptions{})
			got, err := client.CaptureScreenshot(ctx, tc.format)
			checkCDPErr(t, "CaptureScreenshot", err, tc.wantErr)
			if got != tc.want {
				t.Fatalf("CaptureScreenshot(%q) = %q, want %q", tc.format, got, tc.want)
			}
		})
	}
}

func TestCDPClient_EvaluateJS(t *testing.T) {
	tests := []struct {
		name    string
		opts    fakeCDPOptions
		want    string
		wantErr string
	}{
		{"returns value", fakeCDPOptions{}, `"eval-success"`, ""},
		{"exception details", fakeCDPOptions{evalException: "SyntaxError: bad token"}, "", "javascript evaluation error: SyntaxError: bad token"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, tc.opts)
			got, err := client.EvaluateJS(ctx, "window.location.href", true, true)
			checkCDPErr(t, "EvaluateJS", err, tc.wantErr)
			if string(got) != tc.want {
				t.Fatalf("EvaluateJS = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCDPClient_WaitSettle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		delay    time.Duration
		canceled bool
		wantErr  string
	}{
		{"zero delay returns immediately", 0, false, ""},
		{"positive delay elapses", time.Millisecond, false, ""},
		{"canceled context aborts", 500 * time.Millisecond, true, "context canceled"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := NewCDPClient(&Config{SettleDelay: tc.delay})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			client.waitSettleBestEffort(ctx)
			checkCDPErr(t, "WaitSettle", client.WaitSettle(ctx), tc.wantErr)
		})
	}
}

func TestCDPClient_SendCDP(t *testing.T) {
	tests := []struct {
		name         string
		closedServer bool
		want         string
		wantErr      string
	}{
		{"returns result", false, "{}", ""},
		{"dead cdp host", true, "", "cdp target query failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, client, ctx := startCDPClient(t, fakeCDPOptions{})
			if tc.closedServer {
				server.Close()
			}
			got, err := client.SendCDP(ctx, "Page.enable", nil)
			checkCDPErr(t, "SendCDP", err, tc.wantErr)
			if string(got) != tc.want {
				t.Fatalf("SendCDP = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestCDPClient_SendCDPDirectCancel covers both cancellation points of a
// withheld command: before the request is registered and while awaiting the
// reply.
func TestCDPClient_SendCDPDirectCancel(t *testing.T) {
	tests := []struct {
		name      string
		preCancel bool
	}{
		{"pre-canceled context", true},
		{"cancel while awaiting reply", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, client, ctx := startCDPClient(t, fakeCDPOptions{withholdMethod: "Test.withheld"})
			checkCDPErr(t, "EnsureConnection", client.EnsureConnection(ctx), "")
			client.stateMu.Lock()
			conn := client.activeConn
			client.stateMu.Unlock()
			sendCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.preCancel {
				cancel()
			}
			errCh := make(chan error, 1)
			go func() {
				_, err := client.sendCDPDirect(sendCtx, conn, "Test.withheld", nil, "")
				errCh <- err
			}()
			if !tc.preCancel {
				waitCDPPending(client)
				cancel()
			}
			checkCDPErr(t, "sendCDPDirect", <-errCh, "context canceled")
		})
	}
}

// waitCDPPending yields until client has a registered in-flight request.
func waitCDPPending(client *CDPClient) {
	for {
		client.stateMu.Lock()
		n := len(client.pending)
		client.stateMu.Unlock()
		if n > 0 {
			return
		}
		runtime.Gosched()
	}
}

func TestCDPClient_SendCDPDirectWriteError(t *testing.T) {
	_, client, ctx := startCDPClient(t, fakeCDPOptions{})
	checkCDPErr(t, "EnsureConnection", client.EnsureConnection(ctx), "")
	client.stateMu.Lock()
	conn := client.activeConn
	client.stateMu.Unlock()
	_ = conn.UnderlyingConn().Close()
	_, err := client.sendCDPDirect(ctx, conn, "Page.enable", nil, "")
	checkCDPErr(t, "sendCDPDirect", err, "write cdp payload")
}
