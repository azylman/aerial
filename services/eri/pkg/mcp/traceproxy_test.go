package mcp_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/guard"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// endedSpans delivers each span as it ends; the proxy ends its span after the response is
// written, so tests wait on the channel rather than racing the handler.
type endedSpans struct {
	sdktrace.SpanProcessor

	ch chan sdktrace.ReadOnlySpan
}

func (e endedSpans) OnEnd(s sdktrace.ReadOnlySpan) { e.ch <- s }

type upstreamMap map[string]string

func (m upstreamMap) Upstream(target, server string) (string, bool) {
	u, ok := m[target+"/"+server]
	return u, ok
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func turnSpan(n byte) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0xa, n},
		SpanID:     trace.SpanID{0xb, n},
		TraceFlags: trace.FlagsSampled,
	})
}

// seenRequest is what the upstream MCP server observed.
type seenRequest struct {
	traceparent, daemonHeader, auth, path, body string
}

func newUpstream(t *testing.T) (*httptest.Server, <-chan seenRequest) {
	t.Helper()
	seen := make(chan seenRequest, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		seen <- seenRequest{
			traceparent: r.Header.Get("Traceparent"), daemonHeader: r.Header.Get("X-Eri-Daemon"),
			auth: r.Header.Get("Authorization"), path: r.URL.RequestURI(), body: buf.String(),
		}
		w.Header().Set("Mcp-Session-Id", "sess-42")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, seen
}

type activeTurn struct {
	daemon, target, conversation string
	span                         byte
}

type proxyCall struct {
	daemonHeader string
	body         string
	wantParent   byte // 0 = no span and no traceparent
}

func toolsCall(tool string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":{"secret":"do-not-leak"}}}`, tool)
}

const toolsList = `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`

const pingReply = `{"jsonrpc":"2.0","id":7,"result":{}}`

func TestTraceProxy(t *testing.T) {
	t.Parallel()
	twoTurns := []activeTurn{{daemon: "p1/discord-1", target: "discord", conversation: "conv-1", span: 1}, {daemon: "p1/discord-2", target: "discord", conversation: "conv-2", span: 2}}
	tests := []struct {
		name          string
		turns         []activeTurn
		announcements []guard.TraceAnnouncement
		advance       time.Duration
		calls         []proxyCall
	}{
		{
			name:  "claude daemon header picks its own turn",
			turns: twoTurns,
			calls: []proxyCall{{daemonHeader: "p1/discord-2", body: toolsCall("create"), wantParent: 2}},
		},
		{
			name:  "claude header for unknown daemon forwards without traceparent",
			turns: twoTurns,
			calls: []proxyCall{{daemonHeader: "p9/nobody", body: toolsCall("create")}},
		},
		{
			name:  "agy concurrent turns are told apart by announcements",
			turns: twoTurns,
			announcements: []guard.TraceAnnouncement{
				{DaemonID: "p1/discord-2", Server: "docket", Tool: "create"},
				{DaemonID: "p1/discord-1", Server: "docket", Tool: "create"},
			},
			calls: []proxyCall{{body: toolsCall("create"), wantParent: 2}, {body: toolsCall("create"), wantParent: 1}},
		},
		{
			name:          "announcement without daemon id resolves by conversation",
			turns:         twoTurns,
			announcements: []guard.TraceAnnouncement{{ConversationID: "conv-2", Server: "docket", Tool: "create"}},
			calls:         []proxyCall{{body: toolsCall("create"), wantParent: 2}},
		},
		{
			name:  "agy concurrent turns without announcement get no span",
			turns: twoTurns,
			calls: []proxyCall{{body: toolsCall("create")}, {body: toolsList}},
		},
		{
			name:  "agy single active turn is parented without announcement",
			turns: twoTurns[:1],
			calls: []proxyCall{{body: toolsCall("create"), wantParent: 1}, {body: toolsList, wantParent: 1}},
		},
		{
			name:          "expired announcement is ignored",
			turns:         twoTurns,
			announcements: []guard.TraceAnnouncement{{DaemonID: "p1/discord-1", Server: "docket", Tool: "create"}},
			advance:       mcp.AnnouncementTTL,
			calls:         []proxyCall{{body: toolsCall("create")}},
		},
		{
			name:          "announcement is consumed once",
			turns:         twoTurns,
			announcements: []guard.TraceAnnouncement{{DaemonID: "p1/discord-1", Server: "docket", Tool: "create"}},
			calls:         []proxyCall{{body: toolsCall("create"), wantParent: 1}, {body: toolsCall("create")}},
		},
		{
			name:          "announcement for another tool does not match",
			turns:         twoTurns,
			announcements: []guard.TraceAnnouncement{{DaemonID: "p1/discord-1", Server: "docket", Tool: "search"}},
			calls:         []proxyCall{{body: toolsCall("create")}},
		},
		{
			name:  "turn in another target does not count",
			turns: []activeTurn{{daemon: "p2/ephemeral:worker-0", target: "ephemeral", span: 3}},
			calls: []proxyCall{{body: toolsCall("create")}},
		},
		{
			name:  "claude ping reply joins no turn",
			turns: twoTurns,
			calls: []proxyCall{{daemonHeader: "p1/discord-2", body: pingReply}, {daemonHeader: "p1/discord-2", body: toolsList, wantParent: 2}},
		},
		{
			name:  "agy ping reply joins no sole turn",
			turns: twoTurns[:1],
			calls: []proxyCall{{body: pingReply}, {body: toolsList, wantParent: 1}},
		},
		{
			name:  "empty body joins no turn",
			turns: twoTurns,
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: ""}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream, seen := newUpstream(t)
			registry := runner.NewActiveTurnRegistry()
			for _, at := range tc.turns {
				registry.Set(runner.ActiveTurn{DaemonID: at.daemon, Target: at.target, ConversationID: at.conversation}, turnSpan(at.span))
			}
			rec := endedSpans{SpanProcessor: tracetest.NewSpanRecorder(), ch: make(chan sdktrace.ReadOnlySpan, 16)}
			clock := &manualClock{now: time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)}
			proxy := mcp.NewTraceProxy(mcp.TraceProxyConfig{
				Upstreams:      upstreamMap{"discord/docket": upstream.URL + "/mcp?team=1"},
				Turns:          registry,
				Clock:          clock.Now,
				TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)),
			})
			mux := http.NewServeMux()
			proxy.Register(mux)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			for _, a := range tc.announcements {
				body, _ := json.Marshal(a)
				resp, err := srv.Client().Post(srv.URL+"/internal/mcp-proxy/announce", "application/json", bytes.NewReader(body))
				if err != nil {
					t.Fatalf("announce: %v", err)
				}
				_ = resp.Body.Close()
			}
			clock.Advance(tc.advance)

			for i, call := range tc.calls {
				assertTraceProxyCall(t, srv, rec, seen, i, call)
			}
		})
	}
}

func assertTraceProxyCall(t *testing.T, srv *httptest.Server, rec endedSpans, seen <-chan seenRequest, i int, call proxyCall) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/internal/mcp-proxy/discord/docket", strings.NewReader(call.body))
	req.Header.Set("Authorization", "Bearer upstream-token")
	if call.daemonHeader != "" {
		req.Header.Set("X-Eri-Daemon", call.daemonHeader)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("call %d: %v", i, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get("Mcp-Session-Id") != "sess-42" {
		t.Errorf("call %d: status %d session %q, want 202 sess-42", i, resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
	}
	got := <-seen
	if got.auth != "Bearer upstream-token" || got.daemonHeader != "" || got.path != "/mcp?team=1" || got.body != call.body {
		t.Errorf("call %d: upstream saw %+v", i, got)
	}
	if call.wantParent == 0 {
		if got.traceparent != "" || len(rec.ch) != 0 {
			t.Errorf("call %d: traceparent %q, %d spans; want none", i, got.traceparent, len(rec.ch))
		}
		return
	}
	assertTraceProxySpan(t, rec, got, i, call.wantParent)
}

func assertTraceProxySpan(t *testing.T, rec endedSpans, got seenRequest, i int, wantParent byte) {
	t.Helper()
	var span sdktrace.ReadOnlySpan
	select {
	case span = <-rec.ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("call %d: no span ended", i)
	}
	want := turnSpan(wantParent)
	if span.Parent().SpanID() != want.SpanID() || span.SpanContext().TraceID() != want.TraceID() {
		t.Errorf("call %d: parent %s/%s, want %s/%s", i, span.SpanContext().TraceID(), span.Parent().SpanID(), want.TraceID(), want.SpanID())
	}
	wantTP := fmt.Sprintf("00-%s-%s-01", want.TraceID(), span.SpanContext().SpanID())
	if got.traceparent != wantTP {
		t.Errorf("call %d: traceparent %q, want %q", i, got.traceparent, wantTP)
	}
	for _, kv := range span.Attributes() {
		if strings.Contains(kv.Value.String(), "do-not-leak") || strings.Contains(kv.Value.String(), "upstream-token") || strings.Contains(kv.Value.String(), "team=1") {
			t.Errorf("call %d: span attribute %s leaks content", i, kv.Key)
		}
	}
}

func TestTraceProxy_StreamsSSE(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: first\n\n"))
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte("data: second\n\n"))
	}))
	defer upstream.Close()
	defer close(release)

	mux := http.NewServeMux()
	mcp.NewTraceProxy(mcp.TraceProxyConfig{
		Upstreams: upstreamMap{"discord/docket": upstream.URL},
		Turns:     runner.NewActiveTurnRegistry(),
	}).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+"/internal/mcp-proxy/discord/docket", "application/json", strings.NewReader(toolsList))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	// The first event must arrive while the upstream is still holding the stream open.
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first SSE line = %q, %v", line, err)
	}
}

func TestTraceProxy_Routes(t *testing.T) {
	t.Parallel()
	boom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
	t.Cleanup(boom.Close)
	tests := []struct {
		name       string
		method     string
		remoteAddr string
		path       string
		body       string
		wantStatus int
	}{
		{name: "proxy rejects non-loopback", remoteAddr: "10.0.0.7:5555", path: "/internal/mcp-proxy/discord/docket", body: toolsList, wantStatus: http.StatusForbidden},
		{name: "announce rejects non-loopback", remoteAddr: "10.0.0.7:5555", path: "/internal/mcp-proxy/announce", body: `{"daemon_id":"d1","server":"docket","tool":"create"}`, wantStatus: http.StatusForbidden},
		{name: "unknown server is not found", remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/discord/nope", body: toolsList, wantStatus: http.StatusNotFound},
		{name: "malformed announcement is rejected", remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/announce", body: `{`, wantStatus: http.StatusBadRequest},
		{name: "unreachable upstream is a bad gateway", remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/discord/down", body: toolsCall("create"), wantStatus: http.StatusBadGateway},
		{name: "invalid upstream is a bad gateway", remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/discord/bad", body: toolsList, wantStatus: http.StatusBadGateway},
		{name: "upstream error status is preserved for a bodiless GET", method: http.MethodGet, remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/discord/boom", wantStatus: http.StatusInternalServerError},
		{name: "announcement no turn claims is dropped", remoteAddr: "127.0.0.1:5555", path: "/internal/mcp-proxy/announce", body: `{"daemon_id":"d1","server":"docket","tool":"create"}`, wantStatus: http.StatusAccepted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := runner.NewActiveTurnRegistry()
			registry.Set(runner.ActiveTurn{DaemonID: "p1/discord-9", Target: "discord"}, turnSpan(9))
			mux := http.NewServeMux()
			mcp.NewTraceProxy(mcp.TraceProxyConfig{
				Upstreams: upstreamMap{"discord/docket": "http://127.0.0.1:1", "discord/down": "http://127.0.0.1:1/mcp", "discord/bad": "http://[::1", "discord/boom": boom.URL},
				Turns:     registry,
			}).Register(mux)
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, tc.path, strings.NewReader(tc.body))
			req.RemoteAddr = tc.remoteAddr
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
		})
	}
}

func TestAnnouncementStore(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	type record struct{ daemon, tool string }
	tests := []struct {
		name       string
		capacity   int
		records    []record
		takeTool   string
		wantDaemon string
		wantOK     bool
	}{
		{name: "oldest match wins", capacity: 4, records: []record{{"d1", "create"}, {"d2", "create"}}, takeTool: "create", wantDaemon: "d1", wantOK: true},
		{name: "capacity drops the oldest", capacity: 2, records: []record{{"d1", "create"}, {"d2", "search"}, {"d3", "create"}}, takeTool: "create", wantDaemon: "d3", wantOK: true},
		{name: "no match", capacity: 2, records: []record{{"d1", "search"}}, takeTool: "create"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := mcp.NewAnnouncementStore(mcp.AnnouncementTTL, tc.capacity)
			for i, r := range tc.records {
				s.Record("discord", "docket", r.tool, r.daemon, base.Add(time.Duration(i)*time.Second))
			}
			daemon, ok := s.Take("discord", "docket", tc.takeTool, base.Add(5*time.Second))
			if daemon != tc.wantDaemon || ok != tc.wantOK {
				t.Errorf("Take = %q, %v; want %q, %v", daemon, ok, tc.wantDaemon, tc.wantOK)
			}
		})
	}
}
