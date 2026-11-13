package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/azylman/aerial/services/eri/pkg/guard"
)

const agyMCPCall = `{"conversationId":"conv-1","toolCall":{"name":"call_mcp_tool","args":{"ServerName":"docket","ToolName":"create","Arguments":{"title":"secret plan"}}}}`

// announceServer records the announcement it receives and answers with status, or blocks
// until the client gives up when hang is set.
func announceServer(t *testing.T, status int, hang bool, got chan<- string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
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

func TestRunTrace(t *testing.T) {
	t.Parallel()
	wantPost := `POST /internal/mcp-proxy/announce {"daemon_id":"p1/discord-1","conversation_id":"conv-1","server":"docket","tool":"create"}`
	tests := []struct {
		name      string
		endpoint  string // "ok", "fail", "hang", "refused", "badurl" or "" (unset)
		daemonID  string
		stdin     string
		stdinErr  bool // stdin read fails
		failWrite bool // stdout write fails
		wantPost  string
	}{
		{name: "posts names only and allows", endpoint: "ok", daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "non-2xx still allows", endpoint: "fail", daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "timeout still allows", endpoint: "hang", daemonID: "p1/discord-1", stdin: agyMCPCall, wantPost: wantPost},
		{name: "refused connection allows", endpoint: "refused", daemonID: "p1/discord-1", stdin: agyMCPCall},
		{name: "unset endpoint does nothing", endpoint: "", daemonID: "p1/discord-1", stdin: agyMCPCall},
		{name: "malformed stdin allows without posting", endpoint: "ok", stdin: `{not json`},
		{name: "payload without server allows without posting", endpoint: "ok", stdin: `{"toolCall":{"name":"call_mcp_tool","args":{}}}`},
		{name: "unreadable stdin allows without posting", endpoint: "ok", stdinErr: true},
		{name: "invalid endpoint allows without posting", endpoint: "badurl", stdin: agyMCPCall},
		{name: "stdout failure exits non-zero", endpoint: "ok", daemonID: "p1/discord-1", stdin: agyMCPCall, failWrite: true, wantPost: wantPost},
		{name: "empty daemon id still announces conversation", endpoint: "ok", stdin: agyMCPCall, wantPost: `POST /internal/mcp-proxy/announce {"daemon_id":"","conversation_id":"conv-1","server":"docket","tool":"create"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := make(chan string, 1)
			var srv *httptest.Server
			switch tc.endpoint {
			case "fail":
				srv = announceServer(t, http.StatusInternalServerError, false, got)
			case "hang":
				srv = announceServer(t, http.StatusOK, true, got)
			default:
				srv = announceServer(t, http.StatusNoContent, false, got)
			}
			endpoint := srv.URL + "/internal/mcp-proxy/announce"
			switch tc.endpoint {
			case "refused":
				srv.Close()
			case "badurl":
				endpoint = "http://[::1"
			case "":
				endpoint = ""
			}
			client := srv.Client()
			client.Timeout = traceAnnounceTimeout
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
			var decision guard.Decision
			if err := json.Unmarshal(out.Bytes(), &decision); !tc.failWrite && (err != nil || decision.Decision != guard.DecisionAllow || out.String() != "{\"decision\":\"allow\"}\n") {
				t.Errorf("stdout = %q, want only the allow decision", out.String())
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
