package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
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

// fakeCDPFault is a CDP error reply.
type fakeCDPFault struct {
	code int
	msg  string
}

// fakeCDPEval scripts the Runtime.evaluate reply for expressions containing match
// ("" matches every expression): a CDP error, an exception or a value.
type fakeCDPEval struct {
	match     string
	err       string
	exception string
	value     any
}

// fakeCDPOptions configures one fake CDP endpoint. Every failure is keyed on the
// request (CDP method, mouse event type, evaluated expression), never on call order.
type fakeCDPOptions struct {
	emptyHost      bool
	failList       bool
	corruptList    bool
	emptyTargets   bool
	includeChrome  bool
	onlyChrome     bool
	detachOnMethod string
	withholdMethod string
	// faults maps "Method" or "Input.dispatchMouseEvent/<type>" to an error reply.
	faults map[string]fakeCDPFault
	// evals are checked before fakeCDPPageEvals.
	evals          []fakeCDPEval
	screenshotData string
}

// fakeCDPFailOn fails requests matching key with a -32000 "<key> failed" error.
func fakeCDPFailOn(key string) fakeCDPOptions {
	return fakeCDPOptions{faults: map[string]fakeCDPFault{key: {-32000, key + " failed"}}}
}

// fakeCDPPageEvals answer the page scripts the tools build, keyed on markers in
// each script (selectors "#missing" and "throw new Error" are input keys).
var fakeCDPPageEvals = []fakeCDPEval{
	{match: "throw new Error", exception: "Uncaught Error: boom"},
	{match: "#sc-subtotal-amount-activecart", value: map[string]any{
		"subtotal": "$49.99", "count": "1 item",
		"items": []map[string]any{{"asin": "B08N5WRWNW", "title": "Echo Dot 4111-2222-3333-4444", "price": "$49.99", "quantity": "1"}},
	}},
	{match: "#add-to-cart-button", value: map[string]any{"found": true, "hasRect": true, "x": 200, "y": 150, "productTitle": "Echo Dot Gen 4"}},
	{match: "s-search-result", value: []map[string]any{{"asin": "B08N5WRWNW", "title": "Echo Dot with card 4111-2222-3333-4444", "price": "$49.99"}}},
	{match: "cloneNode(true)", value: map[string]any{
		"title": "Store", "url": "https://www.example.com", "text": "Product details with card 4111-2222-3333-4444 here",
	}},
	{match: "#nav-cart-count", value: "1"},
	{match: `"#missing"`, value: map[string]any{"found": false}},
	{match: "getBoundingClientRect", value: map[string]any{
		"found": true, "tag": "button", "text": "Add to Cart", "hasRect": true, "x": 120, "y": 340,
	}},
}

type fakeCDPServer struct {
	*httptest.Server

	opts           fakeCDPOptions
	wsMu           sync.Mutex
	activeWS       *websocket.Conn
	attachedTarget string
}

func (s *fakeCDPServer) getAttachedTarget() string {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	return s.attachedTarget
}

func (s *fakeCDPServer) handleList(w http.ResponseWriter, r *http.Request) {
	switch {
	case s.opts.failList:
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	case s.opts.corruptList:
		_, _ = w.Write([]byte("not valid json"))
		return
	}
	targets := []TargetInfo{}
	if s.opts.includeChrome || s.opts.onlyChrome {
		targets = append(targets, TargetInfo{
			ID: "chrome-target", Type: "page", URL: "chrome://newtab", WebSocketDebuggerURL: "ws://" + r.Host + "/devtools/page/chrome-target",
		})
	}
	if !s.opts.onlyChrome && !s.opts.emptyTargets {
		targets = append(targets, TargetInfo{
			ID: "page-target-1", Type: "page", URL: "https://example.com", WebSocketDebuggerURL: "ws://" + r.Host + "/devtools/page/page-target-1",
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

// fakeCDPParams is the union of the request params the fake keys on.
type fakeCDPParams struct {
	TargetID   string `json:"targetId"`
	Type       string `json:"type"`
	Expression string `json:"expression"`
	Format     string `json:"format"`
}

func (s *fakeCDPServer) write(ws *websocket.Conn, msg any) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	_ = ws.WriteJSON(msg)
}

// reply returns the reply for req, or nil when the request gets no reply.
func (s *fakeCDPServer) reply(req fakeCDPReq) any {
	var p fakeCDPParams
	_ = json.Unmarshal(req.Params, &p)
	if req.Method == s.opts.withholdMethod {
		return nil
	}
	fault, ok := s.opts.faults[req.Method+"/"+p.Type]
	if !ok {
		fault, ok = s.opts.faults[req.Method]
	}
	if ok {
		return map[string]any{"id": req.ID, "error": map[string]any{"code": fault.code, "message": fault.msg}}
	}
	switch req.Method {
	case "Target.attachToTarget":
		s.wsMu.Lock()
		s.attachedTarget = p.TargetID
		s.wsMu.Unlock()
		return map[string]any{"id": req.ID, "result": map[string]any{"sessionId": "test-session-1"}}
	case "Runtime.evaluate":
		return s.evalReply(req.ID, p.Expression)
	case "Page.captureScreenshot":
		if p.Format == "fail-unmarshal" {
			return json.RawMessage(`{"id":` + strconv.FormatInt(req.ID, 10) + `,"result":"not-a-json-object"}`)
		}
		data := "dGVzdC1zY3JlZW5zaG90"
		if s.opts.screenshotData != "" {
			data = s.opts.screenshotData
		}
		return map[string]any{"id": req.ID, "result": map[string]any{"data": data}}
	}
	return map[string]any{"id": req.ID, "result": map[string]any{}}
}

func (s *fakeCDPServer) evalReply(id int64, expr string) any {
	for _, ev := range slices.Concat(s.opts.evals, fakeCDPPageEvals) {
		if !strings.Contains(expr, ev.match) {
			continue
		}
		switch {
		case ev.err != "":
			return map[string]any{"id": id, "error": map[string]any{"code": -32000, "message": ev.err}}
		case ev.exception != "":
			return map[string]any{"id": id, "result": map[string]any{
				"result":           map[string]any{"type": "undefined"},
				"exceptionDetails": map[string]any{"text": ev.exception, "exception": map[string]any{"description": ev.exception}},
			}}
		}
		return map[string]any{"id": id, "result": map[string]any{"result": map[string]any{"type": "object", "value": ev.value}}}
	}
	return map[string]any{"id": id, "result": map[string]any{"result": map[string]any{"type": "string", "value": "eval-success"}}}
}

func (s *fakeCDPServer) serveWS(ws *websocket.Conn) {
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
		if req.Method == s.opts.detachOnMethod {
			s.write(ws, map[string]any{"method": "Target.detachedFromTarget", "params": map[string]any{"sessionId": "test-session-1"}})
			return
		}
		if msg := s.reply(req); msg != nil {
			s.write(ws, msg)
		}
	}
}

func startFakeCDPServer(t *testing.T, opts fakeCDPOptions) (*fakeCDPServer, *Config) {
	t.Helper()
	fakeSrv := &fakeCDPServer{opts: opts}
	fakeSrv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/json/list":
			fakeSrv.handleList(w, r)
		case strings.HasPrefix(r.URL.Path, "/devtools/page/"):
			ws, err := testUpgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			fakeSrv.wsMu.Lock()
			fakeSrv.activeWS = ws
			fakeSrv.wsMu.Unlock()
			go fakeSrv.serveWS(ws)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(func() {
		fakeSrv.wsMu.Lock()
		if fakeSrv.activeWS != nil {
			_ = fakeSrv.activeWS.Close()
		}
		fakeSrv.wsMu.Unlock()
		fakeSrv.Close()
	})

	u, _ := url.Parse(fakeSrv.URL)
	cfg := &Config{Port: "4018", CDPHost: u.Hostname(), CDPPort: u.Port(), SettleDelay: time.Millisecond}
	if opts.emptyHost {
		cfg.CDPHost = ""
	}
	return fakeSrv, cfg
}

func TestCDPClient_EnsureConnectionTable(t *testing.T) {
	tests := []struct {
		name               string
		opts               fakeCDPOptions
		wantErr            string
		wantAttachedTarget string
	}{
		{"unconfigured cdp host fails fast", fakeCDPOptions{emptyHost: true}, "cdp host is not configured", ""},
		{"successful connection and stealth init", fakeCDPOptions{}, "", "page-target-1"},
		{"target selection skips chrome:// url", fakeCDPOptions{includeChrome: true}, "", "page-target-1"},
		{"target selection falls back to chrome:// when only chrome target exists", fakeCDPOptions{onlyChrome: true}, "", "chrome-target"},
		{"fail list query returns error", fakeCDPOptions{failList: true}, "cdp target query returned status 500", ""},
		{"empty targets returns error", fakeCDPOptions{emptyTargets: true}, "no open page target found", ""},
		{"corrupt targets returns error", fakeCDPOptions{corruptList: true}, "parse cdp target response", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, client, ctx := startCDPClient(t, tc.opts)
			checkCDPErr(t, "EnsureConnection", client.EnsureConnection(ctx), tc.wantErr)
			if tc.wantErr != "" {
				return
			}
			if got := server.getAttachedTarget(); got != tc.wantAttachedTarget {
				t.Fatalf("attached target = %q, want %q", got, tc.wantAttachedTarget)
			}
			checkCDPErr(t, "EnsureConnection fast path", client.EnsureConnection(ctx), "")
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
		{"mouseMoved error", fakeCDPFailOn("Input.dispatchMouseEvent/mouseMoved"), "mouseMoved: cdp error -32000"},
		{"mousePressed error", fakeCDPFailOn("Input.dispatchMouseEvent/mousePressed"), "mousePressed: cdp error -32000"},
		{"mouseReleased error", fakeCDPFailOn("Input.dispatchMouseEvent/mouseReleased"), "mouseReleased: cdp error -32000"},
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
		{
			"cdp error response",
			fakeCDPOptions{faults: map[string]fakeCDPFault{"Input.insertText": {-32603, "Internal error executing command"}}},
			"cdp error -32603: Internal error executing command",
		},
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
		{
			"exception details",
			fakeCDPOptions{evals: []fakeCDPEval{{exception: "SyntaxError: bad token"}}},
			"",
			"javascript evaluation error: SyntaxError: bad token",
		},
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
