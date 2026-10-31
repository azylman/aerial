package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockMCPConfig struct {
	initStatus                       int
	initBody                         string
	sessionID                        string
	nonCanonHeader                   bool
	requireSessionID                 bool
	toolsListStatus                  int
	toolsListBody                    string
	tools                            []Tool
	toolsCallStatus                  int
	toolsCallBody                    string
	callContentText                  string
	callIsError                      bool
	callResultRaw                    string
	useSSE                           bool
	failFirstToolsCallMissingSession bool
	failFirstToolsListMissingSession bool
	failFirstCallRPC32600            bool
	failReInit                       bool
	closeConnOnCallRetry             bool
	closeConnOnListRetry             bool
}

func newMockMCPServer(t *testing.T, cfg mockMCPConfig) *httptest.Server {
	t.Helper()
	var listCalls, callCalls atomic.Int32

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &req)

		method, _ := req["method"].(string)
		id := req["id"]
		if id == nil {
			id = 1
		}

		sess := cfg.sessionID
		if sess == "" {
			sess = "sess-mock-default"
		}

		switch method {
		case "initialize":
			if cfg.failReInit {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("re-init failed"))
				return
			}
			status := cfg.initStatus
			if status == 0 {
				status = http.StatusOK
			}
			if status != http.StatusOK {
				w.WriteHeader(status)
				if cfg.initBody != "" {
					_, _ = w.Write([]byte(cfg.initBody))
				}
				return
			}
			if cfg.nonCanonHeader {
				w.Header()["mcp-session-id"] = []string{sess}
				delete(w.Header(), "Mcp-Session-Id")
			} else {
				w.Header().Set("Mcp-Session-Id", sess)
			}
			w.Header().Set("Content-Type", "application/json")
			if cfg.initBody != "" {
				_, _ = w.Write([]byte(cfg.initBody))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": map[string]interface{}{"protocolVersion": "2024-11-05"},
			})

		case "notifications/initialized":
			if cfg.requireSessionID && r.Header.Get("Mcp-Session-Id") != sess {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)

		case "tools/list":
			calls := listCalls.Add(1)
			if calls == 1 && cfg.failFirstToolsListMissingSession {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			if calls > 1 && cfg.closeConnOnListRetry {
				srv.CloseClientConnections()
				w.WriteHeader(http.StatusOK)
				return
			}
			if cfg.requireSessionID && r.Header.Get("Mcp-Session-Id") != sess {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			status := cfg.toolsListStatus
			if status == 0 {
				status = http.StatusOK
			}
			if status != http.StatusOK {
				w.WriteHeader(status)
				if cfg.toolsListBody != "" {
					_, _ = w.Write([]byte(cfg.toolsListBody))
				}
				return
			}
			if cfg.toolsListBody != "" {
				if cfg.useSSE {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "event: message\ndata: %s\n\n", cfg.toolsListBody)
				} else {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(cfg.toolsListBody))
				}
				return
			}
			tools := cfg.tools
			if tools == nil {
				tools = []Tool{{Name: "default_tool", Description: "Default mock tool"}}
			}
			respPayload, _ := json.Marshal(map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": map[string]interface{}{"tools": tools},
			})
			if cfg.useSSE {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(respPayload))
			} else {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(respPayload)
			}

		case "tools/call":
			calls := callCalls.Add(1)
			if calls == 1 && cfg.failFirstToolsCallMissingSession {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			if calls == 1 && cfg.failFirstCallRPC32600 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"jsonrpc": "2.0", "id": id,
					"error": map[string]interface{}{
						"code":    -32600,
						"message": "Invalid request: missing session id",
					},
				})
				return
			}
			if calls > 1 && cfg.closeConnOnCallRetry {
				srv.CloseClientConnections()
				w.WriteHeader(http.StatusOK)
				return
			}
			if cfg.requireSessionID && r.Header.Get("Mcp-Session-Id") != sess {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error": "missing session id"}`))
				return
			}
			status := cfg.toolsCallStatus
			if status == 0 {
				status = http.StatusOK
			}
			if status != http.StatusOK {
				w.WriteHeader(status)
				if cfg.toolsCallBody != "" {
					_, _ = w.Write([]byte(cfg.toolsCallBody))
				}
				return
			}
			if cfg.toolsCallBody != "" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(cfg.toolsCallBody))
				return
			}
			var resultVal interface{}
			if cfg.callResultRaw != "" {
				resultVal = json.RawMessage(cfg.callResultRaw)
			} else {
				txt := cfg.callContentText
				if txt == "" {
					txt = "call success"
				}
				resultVal = map[string]interface{}{
					"content": []map[string]interface{}{{"type": "text", "text": txt}},
					"isError": cfg.callIsError,
				}
			}
			respPayload, _ := json.Marshal(map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": resultVal,
			})
			if cfg.useSSE {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\n", string(respPayload))
			} else {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(respPayload)
			}

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return srv
}

func mkDisp(srvName, url string, sessID ...string) *DefaultDispatcher {
	srv := ServerConfig{Name: srvName, ServerURL: url}
	d := NewDispatcher([]ServerConfig{srv}, nil)
	if len(sessID) > 0 && sessID[0] != "" {
		d.sessionIDs[srvName] = sessID[0]
	}
	return d
}

func mkDispWithRoute(srvName, url, tool string) *DefaultDispatcher {
	srv := ServerConfig{Name: srvName, ServerURL: url}
	d := NewDispatcher([]ServerConfig{srv}, nil)
	d.toolRoutes[tool] = srv
	return d
}

func mkDispWithSession(srvName, url, tool, sess string, headers ...map[string]string) *DefaultDispatcher {
	srv := ServerConfig{Name: srvName, ServerURL: url}
	if len(headers) > 0 {
		srv.Headers = headers[0]
	}
	d := NewDispatcher([]ServerConfig{srv}, nil)
	d.toolRoutes[tool] = srv
	d.sessionIDs[srvName] = sess
	return d
}

func TestResolveAndFilterServerConfigs(t *testing.T) {
	ts := newMockMCPServer(t, mockMCPConfig{
		tools: []Tool{{Name: "allow_1"}, {Name: "block_2"}, {Name: "allow_3"}},
	})
	defer ts.Close()

	tests := []struct {
		name        string
		allowed     []string
		wantToolLen int
		execTool    string
		wantErr     bool
		wantSubstr  string
	}{
		{"NoFilterPermitsAll", nil, 3, "block_2", false, "call success"},
		{"AllowedFilterExcludesBlocked", []string{"allow_1", "allow_3"}, 2, "block_2", true, "not in the allowed tools list"},
		{"AllowedFilterExecutesAllowed", []string{"allow_1", "allow_3"}, 2, "allow_1", false, "call success"},
		{"WhitespaceAllowedTrimmed", []string{" allow_1 ", ""}, 1, "allow_1", false, "call success"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDispatcher([]ServerConfig{{Name: "srv", ServerURL: ts.URL}}, nil, tc.allowed)
			tools, err := d.ListTools(context.Background())
			if err != nil {
				t.Fatalf("ListTools error: %v", err)
			}
			if len(tools) != tc.wantToolLen {
				t.Fatalf("tools length got %d, want %d", len(tools), tc.wantToolLen)
			}
			res, err := d.Execute(context.Background(), tc.execTool, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Execute() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("Execute() err = %v, wantSubstr = %q", err, tc.wantSubstr)
			}
			if !tc.wantErr && res != tc.wantSubstr {
				t.Fatalf("Execute() res = %q, want = %q", res, tc.wantSubstr)
			}
		})
	}
}

func TestParseJSONRPCBodyAndHelpers(t *testing.T) {
	tests := []struct {
		name        string
		testParse   bool
		body        string
		wantErr     bool
		testMissing bool
		status      int
		rpcResp     *jsonRPCResponse
		wantMissing bool
	}{
		{"EmptyBody", true, "", true, false, 0, nil, false},
		{"StandardJSONRPC", true, `{"jsonrpc":"2.0","id":1,"result":"ok"}`, false, false, 0, nil, false},
		{"SSEDataPrefix", true, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"ok\"}\n\n", false, false, 0, nil, false},
		{"SSEEventAndData", true, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"ok\"}\n\n", false, false, 0, nil, false},
		{"InvalidJSON", true, `{invalid-json`, true, false, 0, nil, false},
		{"MissingSessionStatus400", false, "bad req", false, true, http.StatusBadRequest, nil, true},
		{"MissingSessionCode32600", false, "", false, true, http.StatusOK, &jsonRPCResponse{Error: &jsonRPCError{Code: -32600, Message: "error"}}, true},
		{"MissingSessionMsgSubstr", false, "", false, true, http.StatusOK, &jsonRPCResponse{Error: &jsonRPCError{Code: -1, Message: "Missing Session ID error"}}, true},
		{"MissingSessionBodySubstr", false, "missing session id in raw payload", false, true, http.StatusOK, nil, true},
		{"NominalResponse", false, `{"jsonrpc":"2.0","result":"ok"}`, false, true, http.StatusOK, nil, false},
		{"DifferentErrorCode", false, "", false, true, http.StatusOK, &jsonRPCResponse{Error: &jsonRPCError{Code: -32000, Message: "other"}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.testParse {
				_, err := parseJSONRPCBody([]byte(tc.body))
				if (err != nil) != tc.wantErr {
					t.Fatalf("parseJSONRPCBody() err = %v, wantErr = %v", err, tc.wantErr)
				}
			}
			if tc.testMissing {
				isMissing := isMissingSessionIDErr(tc.status, []byte(tc.body), tc.rpcResp)
				if isMissing != tc.wantMissing {
					t.Fatalf("isMissingSessionIDErr() = %v, want %v", isMissing, tc.wantMissing)
				}
			}
		})
	}
	if got := extractMCPSessionID(http.Header{"mcp-session-id": {"sess-non-canonical"}}); got != "sess-non-canonical" {
		t.Fatalf("extractMCPSessionID() = %q, want sess-non-canonical", got)
	}
}

func TestEnsureSession(t *testing.T) {
	okTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-ok-123"})
	defer okTs.Close()
	nonCanonTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-nc-456", nonCanonHeader: true})
	defer nonCanonTs.Close()
	errTs := newMockMCPServer(t, mockMCPConfig{initStatus: http.StatusInternalServerError, initBody: "server crash"})
	defer errTs.Close()

	tests := []struct {
		name       string
		dispatcher *DefaultDispatcher
		serverName string
		wantSess   string
		wantErr    bool
		wantSubstr string
	}{
		{"NilDispatcher", nil, "srv", "", true, "mcp dispatcher is nil"},
		{"UnknownServer", mkDisp("srv1", okTs.URL), "unknown", "", true, "not found in configuration"},
		{"StandardHandshake", NewDispatcher([]ServerConfig{{Name: "srv1", ServerURL: okTs.URL, Headers: map[string]string{"X-H": "V"}}}, nil), "srv1", "sess-ok-123", false, ""},
		{"CachedSessionHit", mkDisp("srv1", okTs.URL, "sess-pre-cached"), "srv1", "sess-pre-cached", false, ""},
		{"NonCanonicalHeader", mkDisp("srvNC", nonCanonTs.URL), "srvNC", "sess-nc-456", false, ""},
		{"ServerError500", mkDisp("srvErr", errTs.URL), "srvErr", "", true, "returned status 500"},
		{"InvalidURL", mkDisp("srvInv", "://invalid\x7f"), "srvInv", "", true, "failed to create initialize request"},
		{"ClosedPort", NewDispatcher([]ServerConfig{{Name: "srvClosed", ServerURL: "http://127.0.0.1:59998"}}, &http.Client{Timeout: 50 * time.Millisecond}), "srvClosed", "", true, "failed to send initialize"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sess, err := tc.dispatcher.EnsureSession(context.Background(), tc.serverName)
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsureSession() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("EnsureSession() err = %v, wantSubstr = %q", err, tc.wantSubstr)
			}
			if !tc.wantErr && sess != tc.wantSess {
				t.Fatalf("EnsureSession() sess = %q, want = %q", sess, tc.wantSess)
			}
		})
	}

	d := mkDisp("srv", okTs.URL)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = d.getServerMutex("srv")
			_, _ = d.EnsureSession(context.Background(), "srv")
		}()
	}
	wg.Wait()
}

func TestListTools(t *testing.T) {
	okTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-lt-1", tools: []Tool{{Name: "t1"}, {Name: ""}, {Name: "t2"}}})
	defer okTs.Close()
	retryTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-lt-retry", failFirstToolsListMissingSession: true, tools: []Tool{{Name: "t_retried"}}})
	defer retryTs.Close()
	retryFailTs := newMockMCPServer(t, mockMCPConfig{failFirstToolsListMissingSession: true, failReInit: true})
	defer retryFailTs.Close()
	retryCloseTs := newMockMCPServer(t, mockMCPConfig{failFirstToolsListMissingSession: true, closeConnOnListRetry: true})
	defer retryCloseTs.Close()
	rpcErrTs := newMockMCPServer(t, mockMCPConfig{toolsListBody: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rpc failure"}}`})
	defer rpcErrTs.Close()
	badJSONTs := newMockMCPServer(t, mockMCPConfig{toolsListBody: `{invalid-json`})
	defer badJSONTs.Close()
	badStructTs := newMockMCPServer(t, mockMCPConfig{toolsListBody: `{"jsonrpc":"2.0","id":1,"result":"not-a-struct"}`})
	defer badStructTs.Close()
	sseTs := newMockMCPServer(t, mockMCPConfig{useSSE: true, tools: []Tool{{Name: "t_sse"}}})
	defer sseTs.Close()

	tests := []struct {
		name        string
		dispatcher  *DefaultDispatcher
		wantTools   int
		wantErr     bool
		wantToolNam string
	}{
		{"NilDispatcher", nil, 0, false, ""},
		{"EmptyServers", NewDispatcher(nil, nil), 0, false, ""},
		{"EmptyURLServerSkipped", mkDisp("empty", ""), 0, false, ""},
		{"SuccessfulDiscovery", mkDisp("srvOk", okTs.URL), 2, false, "t1"},
		{"RetryOn400MissingSession", mkDisp("srvRetry", retryTs.URL, "old-sess"), 1, false, "t_retried"},
		{"RetryFailsOnReInitContinues", mkDisp("srvFail", retryFailTs.URL, "old-sess"), 0, false, ""},
		{"RetryClosedConnContinues", mkDisp("srvClose", retryCloseTs.URL, "old-sess"), 0, false, ""},
		{"RPCErrorContinues", mkDisp("srvRPC", rpcErrTs.URL), 0, false, ""},
		{"BadJSONContinues", mkDisp("srvJSON", badJSONTs.URL), 0, false, ""},
		{"BadStructContinues", mkDisp("srvStruct", badStructTs.URL), 0, false, ""},
		{"SSEDiscovery", mkDisp("srvSSE", sseTs.URL), 1, false, "t_sse"},
		{"InvalidURLContinues", mkDisp("srvInv", "://invalid\x7f"), 0, false, ""},
		{"ClosedPortContinues", NewDispatcher([]ServerConfig{{Name: "srvClosed", ServerURL: "http://127.0.0.1:59997"}}, &http.Client{Timeout: 50 * time.Millisecond}), 0, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tools, err := tc.dispatcher.ListTools(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("ListTools() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(tools) != tc.wantTools {
				t.Fatalf("ListTools() count = %d, want = %d", len(tools), tc.wantTools)
			}
			if tc.wantToolNam != "" && (len(tools) == 0 || tools[0].Name != tc.wantToolNam) {
				t.Fatalf("ListTools() tool[0] = %+v, want name %q", tools, tc.wantToolNam)
			}
		})
	}
}

func TestExecute(t *testing.T) {
	okTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-exec-ok", tools: []Tool{{Name: "tool1"}, {Name: "multi_line"}}})
	defer okTs.Close()
	strictTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-strict-ok", requireSessionID: true, callContentText: "strict success"})
	defer strictTs.Close()
	multiTs := newMockMCPServer(t, mockMCPConfig{callResultRaw: `{"content":[{"type":"text","text":"line1"},{"type":"text","text":"line2"}]}`})
	defer multiTs.Close()
	toolErrTs := newMockMCPServer(t, mockMCPConfig{callContentText: "tool failure details", callIsError: true})
	defer toolErrTs.Close()
	plainStrTs := newMockMCPServer(t, mockMCPConfig{callResultRaw: `"plain string returned"`})
	defer plainStrTs.Close()
	rawJSONTs := newMockMCPServer(t, mockMCPConfig{callResultRaw: `{"custom_key":"custom_val"}`})
	defer rawJSONTs.Close()
	retryTs := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-retry-2", failFirstToolsCallMissingSession: true, callContentText: "retry succeeded"})
	defer retryTs.Close()
	rpc32600Ts := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-32600-ok", failFirstCallRPC32600: true, callContentText: "32600 recovered"})
	defer rpc32600Ts.Close()
	retryFailTs := newMockMCPServer(t, mockMCPConfig{failFirstToolsCallMissingSession: true, failReInit: true})
	defer retryFailTs.Close()
	retryCloseTs := newMockMCPServer(t, mockMCPConfig{failFirstToolsCallMissingSession: true, closeConnOnCallRetry: true})
	defer retryCloseTs.Close()
	status500Ts := newMockMCPServer(t, mockMCPConfig{toolsCallStatus: http.StatusInternalServerError, toolsCallBody: "crash 500"})
	defer status500Ts.Close()
	rpcErrTs := newMockMCPServer(t, mockMCPConfig{toolsCallBody: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`})
	defer rpcErrTs.Close()
	badJSONTs := newMockMCPServer(t, mockMCPConfig{toolsCallBody: `{bad-json`})
	defer badJSONTs.Close()
	initFailTs := newMockMCPServer(t, mockMCPConfig{initStatus: http.StatusInternalServerError})
	defer initFailTs.Close()
	sseTs := newMockMCPServer(t, mockMCPConfig{useSSE: true, callContentText: "sse text call"})
	defer sseTs.Close()

	var emailReceived atomic.Pointer[string]
	emailTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &req)
		method, _ := req["method"].(string)
		if method == "initialize" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"protocolVersion": "2024-11-05"}})
			return
		}
		params, _ := req["params"].(map[string]interface{})
		args, _ := params["arguments"].(map[string]interface{})
		if val, ok := args["user_google_email"].(string); ok {
			emailReceived.Store(&val)
		} else {
			emailReceived.Store(nil)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "ok"})
	}))
	defer emailTs.Close()

	tests := []struct {
		name       string
		dispatcher *DefaultDispatcher
		toolName   string
		args       map[string]interface{}
		wantRes    string
		wantErr    bool
		wantSubstr string
	}{
		{"NilDispatcher", nil, "t", nil, "", true, "mcp dispatcher is nil"},
		{"AllowedFilterRejection", NewDispatcher([]ServerConfig{{Name: "srv", ServerURL: okTs.URL}}, nil, []string{"other"}), "t", nil, "", true, "not in the allowed tools list"},
		{"ToolNotFoundAfterJITList", mkDisp("srv", okTs.URL), "missing_tool", nil, "", true, "not found on any configured MCP server"},
		{"ToolNotFoundJITListFails", mkDisp("srv", status500Ts.URL), "missing_tool", nil, "", true, "not found on any configured MCP server"},
		{"UpfrontEnsureSessionSuccess", mkDispWithRoute("strict", strictTs.URL, "strict_tool"), "strict_tool", nil, "strict success", false, ""},
		{"UpfrontEnsureSessionFailsProceeds", mkDispWithRoute("initFail", initFailTs.URL, "tool"), "tool", nil, "call success", false, ""},
		{"MultipleTextContentJoined", mkDispWithRoute("multi", multiTs.URL, "multi_tool"), "multi_tool", nil, "line1\nline2", false, ""},
		{"ToolContentIsError", mkDispWithRoute("err", toolErrTs.URL, "err_tool"), "err_tool", nil, "tool failure details", true, "tool \"err_tool\" returned error: tool failure details"},
		{"PlainStringResult", mkDispWithRoute("str", plainStrTs.URL, "str_tool"), "str_tool", nil, "plain string returned", false, ""},
		{"RawJSONResult", mkDispWithRoute("raw", rawJSONTs.URL, "raw_tool"), "raw_tool", nil, `{"custom_key":"custom_val"}`, false, ""},
		{"RetryOn400MissingSession", mkDispWithSession("retry", retryTs.URL, "retry_tool", "old-sess", map[string]string{"X-Retry": "true"}), "retry_tool", nil, "retry succeeded", false, ""},
		{"RetryOnCode32600", mkDispWithSession("rpc32600", rpc32600Ts.URL, "t32600", "old-sess"), "t32600", nil, "32600 recovered", false, ""},
		{"RetryFailsOnReInit", mkDispWithSession("rf", retryFailTs.URL, "rf_tool", "old-sess"), "rf_tool", nil, "", true, "failed to re-establish session"},
		{"RetryClosedConnFails", mkDispWithSession("rc", retryCloseTs.URL, "rc_tool", "old-sess"), "rc_tool", nil, "", true, "failed to call"},
		{"Status500Fails", mkDispWithRoute("s500", status500Ts.URL, "s500_tool"), "s500_tool", nil, "", true, "returned status 500"},
		{"RPCErrorFails", mkDispWithRoute("rpcErr", rpcErrTs.URL, "rpc_tool"), "rpc_tool", nil, "", true, "returned error: internal error"},
		{"BadJSONFails", mkDispWithRoute("badJSON", badJSONTs.URL, "bad_tool"), "bad_tool", nil, "", true, "empty or invalid JSON-RPC response"},
		{"InvalidURLFails", mkDispWithRoute("inv", "://invalid\x7f", "inv_tool"), "inv_tool", nil, "", true, "failed to create request"},
		{"ClosedPortFails", mkDispWithRoute("closed", "http://127.0.0.1:59996", "closed_tool"), "closed_tool", nil, "", true, "failed to call"},
		{"SSEStreamCall", mkDispWithRoute("sse", sseTs.URL, "sse_tool"), "sse_tool", nil, "sse text call", false, ""},
		{"EmailNormalizationPrimary", mkDispWithRoute("email", emailTs.URL, "email_tool"), "email_tool", map[string]interface{}{"user_google_email": "PRIMARY", "foo": "bar"}, "ok", false, ""},
		{"EmailNormalizationPreserve", mkDispWithRoute("email", emailTs.URL, "email_tool"), "email_tool", map[string]interface{}{"user_google_email": "alex@domain.com"}, "ok", false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.dispatcher.Execute(context.Background(), tc.toolName, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Execute() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("Execute() err = %v, wantSubstr = %q", err, tc.wantSubstr)
			}
			if !tc.wantErr && res != tc.wantRes {
				t.Fatalf("Execute() res = %q, want = %q", res, tc.wantRes)
			}
		})
	}
}

func TestPrime(t *testing.T) {
	ts1 := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-p1", tools: []Tool{{Name: "p1_tool"}}})
	defer ts1.Close()
	ts2 := newMockMCPServer(t, mockMCPConfig{sessionID: "sess-p2", tools: []Tool{{Name: "p2_tool"}}})
	defer ts2.Close()
	tsFail := newMockMCPServer(t, mockMCPConfig{initStatus: http.StatusInternalServerError, toolsListStatus: http.StatusInternalServerError})
	defer tsFail.Close()

	tests := []struct {
		name       string
		dispatcher *DefaultDispatcher
		wantErr    bool
	}{
		{"NilDispatcher", nil, false},
		{"EmptyServers", NewDispatcher(nil, nil), false},
		{"EmptyURLServerSkipped", mkDisp("empty", ""), false},
		{"NormalPrimeConcurrent", NewDispatcher([]ServerConfig{
			{Name: "s1", ServerURL: ts1.URL},
			{Name: "s2", ServerURL: ts2.URL},
			{Name: "sFail", ServerURL: tsFail.URL},
		}, nil), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.dispatcher.Prime(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("Prime() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.dispatcher != nil && len(tc.dispatcher.servers) > 2 {
				tc.dispatcher.mu.RLock()
				defer tc.dispatcher.mu.RUnlock()
				if tc.dispatcher.sessionIDs["s1"] != "sess-p1" || tc.dispatcher.sessionIDs["s2"] != "sess-p2" {
					t.Fatalf("sessions not warmed up properly: %+v", tc.dispatcher.sessionIDs)
				}
				if _, ok := tc.dispatcher.toolRoutes["p1_tool"]; !ok {
					t.Fatalf("p1_tool not in toolRoutes")
				}
			}
		})
	}
}

func TestConcurrentExecute(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &req)
		method, _ := req["method"].(string)
		id := req["id"]
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "sess-concur")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": map[string]interface{}{"protocolVersion": "2024-11-05"}})
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": map[string]interface{}{"tools": []Tool{{Name: "concur_tool"}}}})
		case "tools/call":
			params, _ := req["params"].(map[string]interface{})
			args, _ := params["arguments"].(map[string]interface{})
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": map[string]interface{}{"content": []map[string]interface{}{{"type": "text", "text": fmt.Sprintf("val-%v", args["idx"])}}},
			})
		}
	}))
	defer ts.Close()

	d := mkDisp("concur", ts.URL)
	if err := d.Prime(context.Background()); err != nil {
		t.Fatalf("Prime error: %v", err)
	}

	tests := []struct {
		name    string
		workers int
	}{
		{"25WorkersParallel", 25},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			errChan := make(chan error, tc.workers)
			for i := range tc.workers {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					res, err := d.Execute(context.Background(), "concur_tool", map[string]interface{}{"idx": idx})
					if err != nil {
						errChan <- err
						return
					}
					if res != fmt.Sprintf("val-%d", idx) {
						errChan <- fmt.Errorf("got %q, want val-%d", res, idx)
					}
				}(i)
			}
			wg.Wait()
			close(errChan)
			for err := range errChan {
				t.Fatal(err)
			}
		})
	}
}

func TestMockMCPLifecycle(t *testing.T) {
	var notifyReceived atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &req)
		method, _ := req["method"].(string)
		id := req["id"]
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "sess-lifecycle")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": map[string]interface{}{"protocolVersion": "2024-11-05"}})
		case "notifications/initialized":
			if r.Header.Get("Mcp-Session-Id") != "sess-lifecycle" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			notifyReceived.Store(true)
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": map[string]interface{}{"tools": []Tool{{Name: "lc_tool"}}}})
		case "tools/call":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": map[string]interface{}{"content": []map[string]interface{}{{"type": "text", "text": "lc ok"}}}})
		}
	}))
	defer ts.Close()

	tests := []struct {
		name       string
		serverURL  string
		wantResult string
	}{
		{"FullSequence", ts.URL, "lc ok"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := mkDisp("lc", tc.serverURL)
			ctx := context.Background()
			sess, err := d.EnsureSession(ctx, "lc")
			if err != nil || sess != "sess-lifecycle" {
				t.Fatalf("EnsureSession error: %v, sess: %q", err, sess)
			}
			if !notifyReceived.Load() {
				t.Fatalf("expected notifications/initialized")
			}
			tools, err := d.ListTools(ctx)
			if err != nil || len(tools) != 1 {
				t.Fatalf("ListTools error: %v, tools: %+v", err, tools)
			}
			res, err := d.Execute(ctx, "lc_tool", nil)
			if err != nil || res != tc.wantResult {
				t.Fatalf("Execute error: %v, res: %q", err, res)
			}
		})
	}
}
