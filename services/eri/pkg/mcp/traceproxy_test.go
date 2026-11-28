package mcp_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itchyny/gojq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/guard"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/runner"
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
	agentHeader, turnIDHeader                   string
}

func newUpstream(t *testing.T) (*httptest.Server, <-chan seenRequest) {
	t.Helper()
	seen := make(chan seenRequest, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		seen <- seenRequest{
			traceparent: r.Header.Get("Traceparent"), daemonHeader: r.Header.Get("X-Eri-Daemon"),
			auth: r.Header.Get("Authorization"), path: r.URL.RequestURI(), body: buf.String(),
			agentHeader: r.Header.Get("X-Aerial-Agent"), turnIDHeader: r.Header.Get("X-Aerial-Turn-Id"),
		}
		w.Header().Set("Mcp-Session-Id", "sess-42")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, seen
}

// toolSpans is a runner.ToolSpanSource holding open execute_tool spans keyed by
// "server/tool/callID"; a claim resolves to turnSpan(n) for its key.
type toolSpans map[string]byte

func (m toolSpans) ClaimToolSpan(_ context.Context, server, tool, callID string) (trace.SpanContext, bool) {
	n, ok := m[server+"/"+tool+"/"+callID]
	if !ok {
		return trace.SpanContext{}, false
	}
	return turnSpan(n), true
}

type activeTurn struct {
	daemon, target, conversation string
	agent, turnID                string
	span                         byte
	tools                        toolSpans // attached tool span source; nil = none
}

type proxyCall struct {
	daemonHeader string
	body         string
	traceparent  string // inbound traceparent header the MCP client sends; "" = none
	inAgent      string // inbound X-Aerial-Agent header (for anti-spoofing tests)
	inTurnID     string // inbound X-Aerial-Turn-Id header (for anti-spoofing tests)
	wantParent   byte   // 0 = no span, inbound traceparent forwarded unchanged
	wantCallID   string // gen_ai.tool.call.id on the client span; "" = absent
	wantAgent    string // expected outbound X-Aerial-Agent header upstream saw
	wantTurnID   string // expected outbound X-Aerial-Turn-Id header upstream saw
}

func toolsCall(tool string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":{"secret":"do-not-leak"}}}`, tool)
}

// claudeToolsCall is a tools/call carrying Claude Code's tool_use ID in params._meta.
func claudeToolsCall(tool, toolUseID string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":{"secret":"do-not-leak"},"_meta":{"claudecode/toolUseId":%q}}}`, tool, toolUseID)
}

const toolsList = `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`

const pingReply = `{"jsonrpc":"2.0","id":7,"result":{}}`

type traceProxyCase struct {
	name          string
	turns         []activeTurn
	announcements []guard.TraceAnnouncement
	advance       time.Duration
	calls         []proxyCall
}

func TestTraceProxy(t *testing.T) {
	t.Parallel()
	twoTurns := []activeTurn{{daemon: "p1/discord-1", target: "discord", conversation: "conv-1", span: 1}, {daemon: "p1/discord-2", target: "discord", conversation: "conv-2", span: 2}}
	tests := []traceProxyCase{
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
		{name: "agy ping reply joins no sole turn", turns: twoTurns[:1], calls: []proxyCall{{body: pingReply}, {body: toolsList, wantParent: 1}}},
		{name: "empty body joins no turn", turns: twoTurns, calls: []proxyCall{{daemonHeader: "p1/discord-1", body: ""}}},
		{
			name:  "claude tools/call nests under the execute_tool span of its tool_use id",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", span: 1, tools: toolSpans{"docket/create/toolu_7": 9}}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: claudeToolsCall("create", "toolu_7"), wantParent: 9, wantCallID: "toolu_7"}},
		},
		{
			name:  "agy sole turn tools/call nests under the execute_tool span by server and tool",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", span: 1, tools: toolSpans{"docket/create/": 9}}},
			calls: []proxyCall{{body: toolsCall("create"), wantParent: 9}},
		},
		{
			name: "agy announced tools/call nests under the announcing daemon's execute_tool span",
			turns: []activeTurn{
				{daemon: "p1/discord-1", target: "discord", span: 1, tools: toolSpans{"docket/create/": 8}},
				{daemon: "p1/discord-2", target: "discord", span: 2, tools: toolSpans{"docket/create/": 9}},
			},
			announcements: []guard.TraceAnnouncement{{DaemonID: "p1/discord-2", Server: "docket", Tool: "create"}},
			calls:         []proxyCall{{body: toolsCall("create"), wantParent: 9}},
		},
		{
			name:  "unmatched tools/call falls back to the invoke_agent span",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", span: 1, tools: toolSpans{"docket/search/toolu_7": 9}}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: claudeToolsCall("create", "toolu_8"), wantParent: 1, wantCallID: "toolu_8"}},
		},
		{
			name:  "unattributed call parents on the engine's inbound traceparent",
			turns: twoTurns,
			calls: []proxyCall{{body: toolsCall("create"), traceparent: "00-0a050000000000000000000000000000-0b05000000000000-01", wantParent: 5}},
		},
		{
			name:  "unattributed tools/list parents on the engine's inbound traceparent",
			calls: []proxyCall{{daemonHeader: "p9/nobody", body: toolsList, traceparent: "00-0a050000000000000000000000000000-0b05000000000000-01", wantParent: 5}},
		},
		{
			name:  "turn attribution outranks the inbound traceparent",
			turns: twoTurns[:1],
			calls: []proxyCall{{body: toolsCall("create"), traceparent: "00-0a050000000000000000000000000000-0b05000000000000-01", wantParent: 1}},
		},
		{
			name:  "invalid inbound traceparent forwards unchanged without a span",
			turns: twoTurns,
			calls: []proxyCall{{body: toolsCall("create"), traceparent: "00-00000000000000000000000000000000-0b05000000000000-01"}},
		},
		{
			name:  "methodless request ignores the inbound traceparent",
			calls: []proxyCall{{body: pingReply, traceparent: "00-0a050000000000000000000000000000-0b05000000000000-01"}},
		},
		{
			name:  "non tools/call methods never claim a tool span",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", span: 1, tools: toolSpans{"docket//": 9}}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: toolsList, wantParent: 1}},
		},
		{
			name:  "agent and turn ID headers propagated to upstream MCP server",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", agent: "eri-worker", turnID: "turn-xyz-123", span: 1}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: toolsCall("create"), wantParent: 1, wantAgent: "eri-worker", wantTurnID: "turn-xyz-123"}},
		},
		{
			name:  "inbound agent and turn ID headers stripped (anti-spoofing)",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", agent: "eri-worker", turnID: "turn-legit", span: 1}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: toolsCall("create"), inAgent: "spoofed-agent", inTurnID: "spoofed-turn", wantParent: 1, wantAgent: "eri-worker", wantTurnID: "turn-legit"}},
		},
		{
			name:  "unattributed request strips inbound agent and turn ID headers",
			calls: []proxyCall{{body: toolsCall("create"), inAgent: "spoofed-agent", inTurnID: "spoofed-turn", wantParent: 0, wantAgent: "", wantTurnID: ""}},
		},
		{
			name:  "active turn with no-op tracer still propagates agent and turn ID",
			turns: []activeTurn{{daemon: "p1/discord-1", target: "discord", agent: "eri-worker", turnID: "turn-noop", span: 0}},
			calls: []proxyCall{{daemonHeader: "p1/discord-1", body: toolsCall("create"), wantParent: 0, wantAgent: "eri-worker", wantTurnID: "turn-noop"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceProxyCase(t, tc)
		})
	}
}

func runTraceProxyCase(t *testing.T, tc traceProxyCase) {
	t.Helper()
	upstream, seen := newUpstream(t)
	registry := runner.NewActiveTurnRegistry()
	for _, at := range tc.turns {
		var sc trace.SpanContext
		if at.span != 0 {
			sc = turnSpan(at.span)
		}
		registry.Set(runner.ActiveTurn{
			DaemonID:       at.daemon,
			Target:         at.target,
			ConversationID: at.conversation,
			Agent:          at.agent,
			TurnID:         at.turnID,
		}, sc)
		if at.tools != nil {
			registry.SetToolSpans(at.daemon, at.tools)
		}
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
		body, err := json.Marshal(a)
		if err != nil {
			t.Fatalf("marshal announcement: %v", err)
		}
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
}

func assertTraceProxyCall(t *testing.T, srv *httptest.Server, rec endedSpans, seen <-chan seenRequest, i int, call proxyCall) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/internal/mcp-proxy/discord/docket", strings.NewReader(call.body))
	if err != nil {
		t.Fatalf("call %d: NewRequest: %v", i, err)
	}
	req.Header.Set("Authorization", "Bearer upstream-token")
	if call.daemonHeader != "" {
		req.Header.Set("X-Eri-Daemon", call.daemonHeader)
	}
	if call.traceparent != "" {
		req.Header.Set("Traceparent", call.traceparent)
	}
	if call.inAgent != "" {
		req.Header.Set("X-Aerial-Agent", call.inAgent)
	}
	if call.inTurnID != "" {
		req.Header.Set("X-Aerial-Turn-Id", call.inTurnID)
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
	if got.agentHeader != call.wantAgent || got.turnIDHeader != call.wantTurnID {
		t.Errorf("call %d: upstream saw agent=%q turnID=%q, want agent=%q turnID=%q", i, got.agentHeader, got.turnIDHeader, call.wantAgent, call.wantTurnID)
	}
	if call.wantParent == 0 {
		if got.traceparent != call.traceparent || len(rec.ch) != 0 {
			t.Errorf("call %d: traceparent %q, %d spans; want %q and no span", i, got.traceparent, len(rec.ch), call.traceparent)
		}
		return
	}
	assertTraceProxySpan(t, rec, got, i, call.wantParent, call.wantCallID)
}

func assertTraceProxySpan(t *testing.T, rec endedSpans, got seenRequest, i int, wantParent byte, wantCallID string) {
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
	callID := ""
	for _, kv := range span.Attributes() {
		if kv.Key == "gen_ai.tool.call.id" {
			callID = kv.Value.AsString()
		}
		if strings.Contains(kv.Value.String(), "do-not-leak") || strings.Contains(kv.Value.String(), "upstream-token") || strings.Contains(kv.Value.String(), "team=1") {
			t.Errorf("call %d: span attribute %s leaks content", i, kv.Key)
		}
	}
	if callID != wantCallID {
		t.Errorf("call %d: gen_ai.tool.call.id = %q, want %q", i, callID, wantCallID)
	}
}

// newSSEUpstream streams event1, waits until event1Read closes, then streams event2 and holds the
// stream open until streamDone closes (closing streamDone alone also releases it).
func newSSEUpstream(t *testing.T, event1, event2 string) (upstream *httptest.Server, event1Read, streamDone chan struct{}) {
	t.Helper()
	event1Read, streamDone = make(chan struct{}), make(chan struct{})
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(event1 + "\n\n"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush first SSE event: %v", err)
		}
		select {
		case <-event1Read:
		case <-streamDone:
			return
		}
		_, _ = w.Write([]byte(event2 + "\n\n"))
		_ = http.NewResponseController(w).Flush()
		<-streamDone
	}))
	t.Cleanup(upstream.Close)
	return upstream, event1Read, streamDone
}

// TestTraceProxy_StreamsSSE checks each SSE event is flushed to the client while the upstream
// still holds the stream open, with and without a tool-shaping policy on the server.
func TestTraceProxy_StreamsSSE(t *testing.T) {
	t.Parallel()
	const (
		event1 = `data: {"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"allowed_tool"},{"name":"blocked_tool"}]}}`
		event2 = `data: {"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"allowed_tool_2"},{"name":"blocked_tool"}]}}`
	)
	tests := []struct {
		name                 string
		disabled             map[string]bool // shaping policy; nil means no policy for the server
		wantLine1, wantLine2 string
	}{
		{name: "pass-through", wantLine1: event1 + "\n", wantLine2: event2 + "\n"},
		{
			name:      "shaped",
			disabled:  map[string]bool{"blocked_tool": true},
			wantLine1: `data: {"id":1,"jsonrpc":"2.0","result":{"tools":[{"name":"allowed_tool"}]}}` + "\n",
			wantLine2: `data: {"id":2,"jsonrpc":"2.0","result":{"tools":[{"name":"allowed_tool_2"}]}}` + "\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream, event1Read, streamDone := newSSEUpstream(t, event1, event2)

			routes := upstreamPolicyMap{upstreams: map[string]string{"discord/serverA": upstream.URL}}
			if tc.disabled != nil {
				routes.policies = map[string]env.ServerPolicy{"discord/serverA": {DisabledTools: tc.disabled}}
			}
			mux := http.NewServeMux()
			mcp.NewTraceProxy(mcp.TraceProxyConfig{Upstreams: routes, Turns: runner.NewActiveTurnRegistry()}).Register(mux)
			srv := httptest.NewServer(mux)
			defer srv.Close()
			defer close(streamDone) // runs first so a failed read never leaves the servers blocked

			resp, err := srv.Client().Post(srv.URL+"/internal/mcp-proxy/discord/serverA", "application/json", strings.NewReader(toolsList))
			if err != nil {
				t.Fatalf("post: %v", err)
			}
			defer resp.Body.Close()
			if ct := resp.Header.Get("Content-Type"); resp.StatusCode != http.StatusOK || !strings.HasPrefix(ct, "text/event-stream") {
				t.Fatalf("status = %d, Content-Type = %q, want 200 text/event-stream", resp.StatusCode, ct)
			}
			reader := bufio.NewReader(resp.Body)
			// The first event must arrive while the upstream is still holding the stream open.
			if line, err := reader.ReadString('\n'); err != nil || line != tc.wantLine1 {
				t.Fatalf("first SSE line = %q, %v; want %q", line, err, tc.wantLine1)
			}
			if line, err := reader.ReadString('\n'); err != nil || line != "\n" {
				t.Fatalf("SSE delimiter = %q, %v", line, err)
			}
			close(event1Read)
			if line, err := reader.ReadString('\n'); err != nil || line != tc.wantLine2 {
				t.Fatalf("second SSE line = %q, %v; want %q", line, err, tc.wantLine2)
			}
		})
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
		{name: "proxy rejects non-loopback", remoteAddr: "192.0.2.7:5555", path: "/internal/mcp-proxy/discord/docket", body: toolsList, wantStatus: http.StatusForbidden},
		{name: "announce rejects non-loopback", remoteAddr: "192.0.2.7:5555", path: "/internal/mcp-proxy/announce", body: `{"daemon_id":"d1","server":"docket","tool":"create"}`, wantStatus: http.StatusForbidden},
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

func TestTraceProxy_AnnounceOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		body        string
		wantStatus  int
		wantOutcome string
		wantVia     string
	}{
		{name: "known daemon_id is recorded via daemon", body: `{"daemon_id":"p1/voice-kiosk","server":"ha","tool":"get_state"}`, wantStatus: http.StatusNoContent, wantOutcome: "recorded", wantVia: "daemon"},
		{name: "unknown daemon_id resolved by conversation", body: `{"daemon_id":"stale","conversation_id":"conv-kiosk","server":"ha","tool":"get_state"}`, wantStatus: http.StatusNoContent, wantOutcome: "recorded", wantVia: "conversation"},
		{name: "empty daemon_id resolved by conversation", body: `{"conversation_id":"conv-kiosk","server":"ha","tool":"get_state"}`, wantStatus: http.StatusNoContent, wantOutcome: "recorded", wantVia: "conversation"},
		{name: "announcement no turn claims is dropped", body: `{"daemon_id":"stale","conversation_id":"conv-gone","server":"ha","tool":"get_state"}`, wantStatus: http.StatusAccepted, wantOutcome: "dropped", wantVia: "none"},
		{name: "announcement without a tool is malformed", body: `{"daemon_id":"p1/voice-kiosk","server":"ha"}`, wantStatus: http.StatusAccepted, wantOutcome: "malformed", wantVia: "none"},
		{name: "announcement without a server is malformed", body: `{"daemon_id":"p1/voice-kiosk","tool":"get_state"}`, wantStatus: http.StatusAccepted, wantOutcome: "malformed", wantVia: "none"},
		{name: "undecodable announcement is malformed", body: `{`, wantStatus: http.StatusBadRequest, wantOutcome: "malformed", wantVia: "none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := runner.NewActiveTurnRegistry()
			registry.Set(runner.ActiveTurn{DaemonID: "p1/voice-kiosk", Target: "voice", ConversationID: "conv-kiosk"}, turnSpan(9))
			outcomes := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_announcements_total"}, []string{"outcome", "via"})
			mux := http.NewServeMux()
			mcp.NewTraceProxy(mcp.TraceProxyConfig{Upstreams: upstreamMap{}, Turns: registry, AnnouncementOutcomes: outcomes}).Register(mux)
			req := httptest.NewRequest(http.MethodPost, "/internal/mcp-proxy/announce", strings.NewReader(tc.body))
			req.RemoteAddr = "127.0.0.1:5555"
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if got := testutil.CollectAndCount(outcomes); got != 1 {
				t.Errorf("outcome series = %d, want 1", got)
			}
			if got := testutil.ToFloat64(outcomes.WithLabelValues(tc.wantOutcome, tc.wantVia)); got != 1 {
				t.Errorf("announcements{outcome=%q,via=%q} = %v, want 1", tc.wantOutcome, tc.wantVia, got)
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

type upstreamPolicyMap struct {
	upstreams map[string]string
	policies  map[string]env.ServerPolicy
}

func (m upstreamPolicyMap) Upstream(target, server string) (string, bool) {
	u, ok := m.upstreams[target+"/"+server]
	return u, ok
}

func (m upstreamPolicyMap) Policy(target, server string) (env.ServerPolicy, bool) {
	p, ok := m.policies[target+"/"+server]
	return p, ok
}

func compileJQ(t *testing.T, expr string) *gojq.Code {
	t.Helper()
	query, err := gojq.Parse(expr)
	if err != nil {
		t.Fatalf("parse jq %q: %v", expr, err)
	}
	code, err := gojq.Compile(query, gojq.WithVariables([]string{"$args"}))
	if err != nil {
		t.Fatalf("compile jq %q: %v", expr, err)
	}
	return code
}

type blockedToolCase struct {
	name        string
	policy      env.ServerPolicy
	rawReq      string
	wantBlocked bool
	wantReqID   any
	wantTool    string
}

func blockedToolTestCases() []blockedToolCase {
	return []blockedToolCase{
		{name: "empty policy allows any tool", policy: env.ServerPolicy{}, rawReq: toolsCall("delete"), wantBlocked: false},
		{
			name:        "disabled tool blocked",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      toolsCall("delete"),
			wantBlocked: true,
			wantReqID:   float64(1),
			wantTool:    "delete",
		},
		{
			name:        "disabled tool check is case-insensitive",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      toolsCall("DELETE"),
			wantBlocked: true,
			wantReqID:   float64(1),
			wantTool:    "DELETE",
		},
		{
			name:        "disabled tool check trims whitespace",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{"jsonrpc":"2.0","id":"req-42","method":"tools/call","params":{"name":"  delete  "}}`,
			wantBlocked: true,
			wantReqID:   "req-42",
			wantTool:    "  delete  ",
		},
		{
			name:        "enabled tools whitelist allows permitted tool",
			policy:      env.ServerPolicy{EnabledTools: map[string]bool{"read": true}},
			rawReq:      toolsCall("read"),
			wantBlocked: false,
		},
		{
			name:        "enabled tools whitelist blocks unlisted tool",
			policy:      env.ServerPolicy{EnabledTools: map[string]bool{"read": true}},
			rawReq:      toolsCall("write"),
			wantBlocked: true,
			wantReqID:   float64(1),
			wantTool:    "write",
		},
		{
			name:        "disabled tool outranks whitelist",
			policy:      env.ServerPolicy{EnabledTools: map[string]bool{"read": true}, DisabledTools: map[string]bool{"read": true}},
			rawReq:      toolsCall("read"),
			wantBlocked: true,
			wantReqID:   float64(1),
			wantTool:    "read",
		},
		{
			name:        "non tools/call method allowed regardless of disabled tools",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"search": true}},
			rawReq:      toolsList,
			wantBlocked: false,
		},
		{
			name:        "batch request with a blocked tool fails closed",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `[` + toolsCall("read") + `,` + toolsCall("delete") + `]`,
			wantBlocked: true,
			wantReqID:   float64(1),
			wantTool:    "delete",
		},
		{
			name:        "batch request with permitted tools passes",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `[` + toolsCall("read") + `,` + toolsCall("search") + `]`,
			wantBlocked: false,
		},
		{
			name:        "malformed json passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{malformed`,
			wantBlocked: false,
		},
		{
			name:        "malformed params passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":""}`,
			wantBlocked: false,
		},
		{
			name:        "empty request body is not blocked",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      "",
			wantBlocked: false,
		},
		{
			name:        "notification passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			wantBlocked: false,
		},
		{
			name:        "batch malformed json passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `[{bad`,
			wantBlocked: false,
		},
		{
			name:        "batch non-object item passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `[123]`,
			wantBlocked: false,
		},
		{
			name:        "scalar request passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `"scalar"`,
			wantBlocked: false,
		},
		{
			name:        "tools/call without params passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{"jsonrpc":"2.0","id":1,"method":"tools/call"}`,
			wantBlocked: false,
		},
		{
			name:        "tools/call with whitespace-only tool name passes through to upstream",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"delete": true}},
			rawReq:      `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"   "}}`,
			wantBlocked: false,
		},
	}
}

func TestIsBlockedToolCall(t *testing.T) {
	t.Parallel()
	for _, tc := range blockedToolTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			blocked, reqID, tool := mcp.IsBlockedToolCall([]byte(tc.rawReq), tc.policy)
			if blocked != tc.wantBlocked {
				t.Fatalf("blocked = %v, want %v", blocked, tc.wantBlocked)
			}
			if tc.wantBlocked {
				if reqID != tc.wantReqID {
					t.Errorf("reqID = %v, want %v", reqID, tc.wantReqID)
				}
				if tool != tc.wantTool {
					t.Errorf("tool = %q, want %q", tool, tc.wantTool)
				}
			}
		})
	}
}

type traceProxyShapingCase struct {
	name            string
	path            string
	body            string
	wantStatusCode  int
	wantContentType string
	wantSub         string
	dontWantSub     string
	wantUpstream    bool
}

func handleShapingUpstream(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/list":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"allowed_tool"},{"name":"blocked_tool"}]}}`))
	case "/init":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{"protocolVersion":"2024-11-05"}}`))
	case "/call":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"item1\",\"junk\":true}]}"}]}}`))
	case "/call_fail":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"fail_item\"}]}"}]}}`))
	case "/call_gzip":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte(`{"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"gzip_item\",\"junk\":true}]}"}]}}`))
		_ = gz.Close()
		_, _ = w.Write(buf.Bytes())
	case "/sse":
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":5,\"result\":{\"tools\":[{\"name\":\"allowed_tool\"},{\"name\":\"blocked_tool\"}]}}\n\n"))
	case "/error":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":9,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"unshaped\",\"junk\":true}]}"}]}}`))
	default:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}
}

func resolveShapingTarget(baseURL, body, name string) string {
	switch {
	case strings.Contains(body, "initialize"):
		return baseURL + "/init"
	case strings.Contains(body, "fail_tool"):
		return baseURL + "/call_fail"
	case strings.Contains(body, "filter_tool"):
		return baseURL + "/call"
	case strings.Contains(body, "gzip_tool"):
		return baseURL + "/call_gzip"
	case strings.Contains(body, "error_tool"):
		return baseURL + "/error"
	case strings.Contains(name, "sse"):
		return baseURL + "/sse"
	default:
		return baseURL + "/list"
	}
}

func runTraceProxyShapingCase(t *testing.T, filterCode *gojq.Code, tc traceProxyShapingCase) {
	t.Helper()
	var upstreamCalled atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled.Add(1)
		handleShapingUpstream(w, r)
	}))
	defer upstream.Close()

	failFilter := compileJQ(t, "error(\"filter failed\")")
	proxyRoutes := upstreamPolicyMap{
		upstreams: map[string]string{
			"discord/serverA": resolveShapingTarget(upstream.URL, tc.body, tc.name),
		},
		policies: map[string]env.ServerPolicy{
			"discord/serverA": {
				Instructions:  "Server instructions",
				DisabledTools: map[string]bool{"blocked_tool": true},
				ResponseFilters: map[string]*gojq.Code{
					"error_tool":  filterCode,
					"filter_tool": filterCode,
					"fail_tool":   failFilter,
					"gzip_tool":   filterCode,
				},
			},
		},
	}
	mux := http.NewServeMux()
	mcp.NewTraceProxy(mcp.TraceProxyConfig{
		Upstreams: proxyRoutes,
		Turns:     runner.NewActiveTurnRegistry(),
	}).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+tc.path, "application/json", strings.NewReader(tc.body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != tc.wantStatusCode {
		t.Errorf("status = %d, want %d", resp.StatusCode, tc.wantStatusCode)
	}
	if tc.wantContentType != "" {
		if gotCT := resp.Header.Get("Content-Type"); !strings.Contains(gotCT, tc.wantContentType) {
			t.Errorf("Content-Type = %q, want containing %q", gotCT, tc.wantContentType)
		}
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, tc.wantSub) {
		t.Errorf("response body missing %q, got: %s", tc.wantSub, bodyStr)
	}
	if tc.dontWantSub != "" && strings.Contains(bodyStr, tc.dontWantSub) {
		t.Errorf("response body unexpectedly contained %q, got: %s", tc.dontWantSub, bodyStr)
	}
	wasCalled := upstreamCalled.Load() > 0
	if wasCalled != tc.wantUpstream {
		t.Errorf("upstream called = %v, want %v", wasCalled, tc.wantUpstream)
	}
}

func TestTraceProxy_ShapingAndPolicy(t *testing.T) {
	t.Parallel()
	filterCode := compileJQ(t, ".items[] | {name: .name, query: $args.q}")
	tests := []traceProxyShapingCase{
		{
			name:            "blocked tool call returns 200 -32601 error immediately without calling upstream",
			path:            "/internal/mcp-proxy/discord/serverA",
			body:            `{"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":"blocked_tool","arguments":{}}}`,
			wantStatusCode:  http.StatusOK,
			wantContentType: "application/json",
			wantSub:         `"code":-32601`,
			dontWantSub:     `item1`,
			wantUpstream:    false,
		},
		{
			name:           "tools/list response filtered",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `"name":"allowed_tool"`,
			dontWantSub:    `"name":"blocked_tool"`,
			wantUpstream:   true,
		},
		{
			name:           "initialize injects instructions",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":2,"method":"initialize"}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `"instructions":"Server instructions"`,
			wantUpstream:   true,
		},
		{
			name:           "tools/call applies jq response filter with $args",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"filter_tool","arguments":{"q":"hello"}}}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `\"name\":\"item1\",\"query\":\"hello\"`,
			dontWantSub:    `junk`,
			wantUpstream:   true,
		},
		{
			name:           "gzipped tools/call response is uncompressed and shaped",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"gzip_tool","arguments":{"q":"gzip_q"}}}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `\"name\":\"gzip_item\",\"query\":\"gzip_q\"`,
			dontWantSub:    `junk`,
			wantUpstream:   true,
		},
		{
			name:           "sse tools/list stream is shaped in place preserving sse framing",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":5,"method":"tools/list"}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `data: {"id":5,"jsonrpc":"2.0","result":{"tools":[{"name":"allowed_tool"}]}}`,
			dontWantSub:    `blocked_tool`,
			wantUpstream:   true,
		},
		{
			name:           "upstream 500 error skips shaping and forwards verbatim",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"error_tool"}}`,
			wantStatusCode: http.StatusInternalServerError,
			wantSub:        `junk`,
			wantUpstream:   true,
		},
		{
			name:           "tools/call filter failure fails open and forwards unshaped response",
			path:           "/internal/mcp-proxy/discord/serverA",
			body:           `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"fail_tool"}}`,
			wantStatusCode: http.StatusOK,
			wantSub:        `fail_item`,
			wantUpstream:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceProxyShapingCase(t, filterCode, tc)
		})
	}
}

type bearerAuthCase struct {
	name             string
	initToken        string
	updateToken      string
	advance          time.Duration
	clientAuth       string
	allowedAuthHosts []string
	wantAuth1        string
	wantAuth2        string
}

func TestTraceProxy_BearerAuth(t *testing.T) {
	t.Parallel()
	tests := []bearerAuthCase{
		{name: "injects bearer token from file when auth header empty", initToken: "test-token-alpha", wantAuth1: "Bearer test-token-alpha"},
		{
			name:       "preserves explicit upstream authorization header",
			initToken:  "test-token-alpha",
			clientAuth: "Bearer custom-agent-token",
			wantAuth1:  "Bearer custom-agent-token",
		},
		{name: "graceful fallback when token file does not exist", initToken: "", wantAuth1: ""},
		{
			name:        "caches token for 60 seconds and re-reads after expiry",
			initToken:   "token-initial",
			updateToken: "token-rotated",
			advance:     61 * time.Second,
			wantAuth1:   "Bearer token-initial",
			wantAuth2:   "Bearer token-rotated",
		},
		{
			name:        "serves cached token within 60 seconds",
			initToken:   "token-initial",
			updateToken: "token-rotated",
			advance:     30 * time.Second,
			wantAuth1:   "Bearer token-initial",
			wantAuth2:   "Bearer token-initial",
		},
		{
			name:             "does not inject bearer token to unallowed upstreams",
			initToken:        "test-token-alpha",
			allowedAuthHosts: []string{"other.cluster.local"},
			wantAuth1:        "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBearerAuthCase(t, tc)
		})
	}
}

func runBearerAuthCase(t *testing.T, tc bearerAuthCase) {
	t.Helper()
	upstream, seen := newUpstream(t)
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if tc.initToken != "" {
		if err := os.WriteFile(tokenPath, []byte(tc.initToken+"\n"), 0o600); err != nil {
			t.Fatalf("write token: %v", err)
		}
	}

	clock := &manualClock{now: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	proxy := mcp.NewTraceProxy(mcp.TraceProxyConfig{
		Upstreams:        upstreamMap{"discord/docket": upstream.URL + "/mcp"},
		Turns:            runner.NewActiveTurnRegistry(),
		Clock:            clock.Now,
		TokenPath:        tokenPath,
		AllowedAuthHosts: tc.allowedAuthHosts,
	})
	mux := http.NewServeMux()
	proxy.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got1 := doBearerProxyCall(t, srv.Client(), srv.URL, tc.clientAuth, seen)
	if got1.auth != tc.wantAuth1 {
		t.Errorf("call 1 auth = %q, want %q", got1.auth, tc.wantAuth1)
	}

	if tc.updateToken == "" {
		return
	}
	if err := os.WriteFile(tokenPath, []byte(tc.updateToken+"\n"), 0o600); err != nil {
		t.Fatalf("update token: %v", err)
	}
	clock.Advance(tc.advance)
	got2 := doBearerProxyCall(t, srv.Client(), srv.URL, tc.clientAuth, seen)
	if got2.auth != tc.wantAuth2 {
		t.Errorf("call 2 auth = %q, want %q", got2.auth, tc.wantAuth2)
	}
}

func doBearerProxyCall(t *testing.T, client *http.Client, srvURL, authHeader string, seen <-chan seenRequest) seenRequest {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srvURL+"/internal/mcp-proxy/discord/docket", strings.NewReader(toolsList))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	return <-seen
}
