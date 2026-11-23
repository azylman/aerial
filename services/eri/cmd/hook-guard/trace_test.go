package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
)

const claudeMCPCall = `{"session_id":"sess-9","hook_event_name":"PreToolUse","tool_name":"mcp__docket__create","tool_input":{"title":"secret plan"}}`

const agyMCPCall = `{"conversationId":"conv-1","toolCall":{"name":"call_mcp_tool","args":{"ServerName":"docket","ToolName":"create","Arguments":{"title":"secret plan"}}}}`

// announceServer records the announcement it receives and answers with status, or blocks
// until the client gives up when hang is set.
func announceServer(t *testing.T, status int, hang bool, got chan<- string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(r.Body); err != nil {
			return
		}
		got <- r.Method + " " + r.URL.Path + " " + buf.String()
		if hang {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type traceCase struct {
	name             string
	status           int    // announce response status; 0 means 204
	hang             bool   // server blocks until the client gives up
	closeServer      bool   // server is closed before the call (connection refused)
	endpointOverride string // replaces the server URL when set
	unsetEndpoint    bool   // ERI_HOOK_ENDPOINT is empty
	daemonID         string
	stdin            string
	stdinErr         bool // stdin read fails
	failWrite        bool // stdout write fails
	wantPost         string
	wantOut          string // "" means the agy allow decision
}

// traceEndpoint starts the announce server for a row and returns the endpoint value and client.
func traceEndpoint(t *testing.T, tc traceCase, got chan<- string) (string, *http.Client) {
	t.Helper()
	status := tc.status
	if status == 0 {
		status = http.StatusNoContent
	}
	srv := announceServer(t, status, tc.hang, got)
	endpoint := srv.URL + "/internal/mcp-proxy/announce"
	if tc.closeServer {
		srv.Close()
	}
	if tc.endpointOverride != "" {
		endpoint = tc.endpointOverride
	}
	if tc.unsetEndpoint {
		endpoint = ""
	}
	client := srv.Client()
	client.Timeout = traceAnnounceTimeout
	return endpoint, client
}

func TestRunTrace(t *testing.T) {
	t.Parallel()
	wantPost := `POST /internal/mcp-proxy/announce {"daemon_id":"p1/discord-1","conversation_id":"conv-1","server":"docket","tool":"create"}`
	tests := []traceCase{
		{name: "posts names only and allows", daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "non-2xx still allows", status: http.StatusInternalServerError, daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "timeout still allows", hang: true, daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "refused connection allows", closeServer: true, daemonID: "p1/discord-1", stdin: agyMCPCall},
		{name: "unset endpoint does nothing", unsetEndpoint: true, daemonID: "p1/discord-1", stdin: agyMCPCall},
		{name: "malformed stdin allows without posting", stdin: `{not json`},
		{name: "payload without server allows without posting", stdin: `{"toolCall":{"name":"call_mcp_tool","args":{}}}`},
		{
			name: "claude mcp call announces and allows in claude schema", daemonID: "p1/discord-1", stdin: claudeMCPCall,
			wantPost: `POST /internal/mcp-proxy/announce {"daemon_id":"p1/discord-1","conversation_id":"sess-9","server":"docket","tool":"create"}`,
			wantOut:  `{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}}` + "\n",
		},
		{name: "unreadable stdin allows without posting", stdinErr: true},
		{name: "invalid endpoint allows without posting", endpointOverride: "http://[::1", stdin: agyMCPCall},
		{name: "stdout failure exits non-zero", daemonID: "p1/discord-1", stdin: agyMCPCall, failWrite: true, wantPost: wantPost},
		{name: "empty daemon id still announces conversation", stdin: agyMCPCall, wantPost: `POST /internal/mcp-proxy/announce {"daemon_id":"","conversation_id":"conv-1","server":"docket","tool":"create"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := make(chan string, 1)
			endpoint, client := traceEndpoint(t, tc, got)
			env := map[string]string{"ERI_HOOK_ENDPOINT": endpoint, "ERI_DAEMON_ID": tc.daemonID}
			var out bytes.Buffer
			var stdout io.Writer = &out
			if tc.failWrite {
				stdout = &errWriter{}
			}
			var stdin io.Reader = strings.NewReader(tc.stdin)
			if tc.stdinErr {
				stdin = iotest.ErrReader(errors.New("stdin closed"))
			}
			code := runTrace(stdin, stdout, func(k string) string { return env[k] }, client)

			if wantCode := map[bool]int{false: 0, true: 1}[tc.failWrite]; code != wantCode {
				t.Errorf("exit code = %d, want %d", code, wantCode)
			}
			wantOut := tc.wantOut
			if wantOut == "" {
				wantOut = "{\"decision\":\"allow\"}\n"
			}
			if !tc.failWrite && out.String() != wantOut {
				t.Errorf("stdout = %q, want only the allow decision %q", out.String(), wantOut)
			}
			var posted string
			select {
			case posted = <-got:
			default:
			}
			if posted != tc.wantPost {
				t.Errorf("posted %q, want %q", posted, tc.wantPost)
			}
		})
	}
}
