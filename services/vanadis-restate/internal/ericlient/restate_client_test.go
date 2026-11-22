package ericlient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingresstest"
)

func TestServiceNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		agent      string
		wantThread string
		wantDisp   string
	}{
		{name: "empty defaults to aerial", agent: "", wantThread: "ThreadSession", wantDisp: "DispatchSession"},
		{name: "aerial explicit", agent: "aerial", wantThread: "ThreadSession", wantDisp: "DispatchSession"},
		{name: "agent-alpha lowercase", agent: "agent-alpha", wantThread: "ThreadSession-agent-alpha", wantDisp: "DispatchSession-agent-alpha"},
		{name: "agent-beta uppercase trimmed", agent: "  Agent-Beta  ", wantThread: "ThreadSession-agent-beta", wantDisp: "DispatchSession-agent-beta"},
		{name: "unsafe characters fallback", agent: "bad/agent@name!", wantThread: "ThreadSession", wantDisp: "DispatchSession"},
		{name: "safe hyphens and underscores", agent: "bot-1_test", wantThread: "ThreadSession-bot-1_test", wantDisp: "DispatchSession-bot-1_test"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ThreadSessionServiceName(tc.agent); got != tc.wantThread {
				t.Errorf("ThreadSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantThread)
			}
			if got := DispatchSessionServiceName(tc.agent); got != tc.wantDisp {
				t.Errorf("DispatchSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantDisp)
			}
		})
	}
}

// mockDeliverer is a plain status-widget and direct-message deliverer that
// records the last direct message.
type mockDeliverer struct {
	msgID       string
	err         error
	lastChannel string
	lastContent string
	// deleted records each DeleteStatusWidget as "<agent> <channel> <message>".
	deleted []string
}

func (m *mockDeliverer) DeleteStatusWidget(_ context.Context, agent, channelID, messageID string) error {
	m.deleted = append(m.deleted, agent+" "+channelID+" "+messageID)
	return m.err
}

func (m *mockDeliverer) DeliverStatusWidget(_ context.Context, _ StatusWidgetRequest) (string, error) {
	return m.msgID, m.err
}

func (m *mockDeliverer) DeliverDirectMessage(_ context.Context, channelID, content string) error {
	m.lastChannel = channelID
	m.lastContent = content
	return m.err
}

// delivered is the last direct message as "<channel> <content>", or "".
func (m *mockDeliverer) delivered() string {
	return strings.TrimSpace(m.lastChannel + " " + m.lastContent)
}

var errBoom = errors.New("boom")

// deliveryRecorder is implemented by the fake deliverers.
type deliveryRecorder interface{ delivered() string }

// restateClientFake starts a recording Restate ingress and a recording eri,
// both replying status and body on path (other paths reply 200 with no body).
func restateClientFake(t *testing.T, path string, status int, body string) (ingress, eri *restateingresstest.Server) {
	t.Helper()
	routes := map[string]restateingresstest.Response{path: {Status: status, Body: body}}
	ingress, eri = restateingresstest.New(t), restateingresstest.New(t)
	ingress.Routes, eri.Routes = routes, routes
	return ingress, eri
}

// restateClientURLs picks the URLs a row configures: restateURL overrides the
// fake ingress, noRestate and noEri leave that URL empty.
func restateClientURLs(ingress, eri *restateingresstest.Server, noRestate, noEri bool, restateURL string) (string, string) {
	if restateURL == "" && !noRestate {
		restateURL = ingress.URL()
	}
	eriURL := ""
	if !noEri {
		eriURL = eri.URL()
	}
	return restateURL, eriURL
}

// restateClientCalls lists both fakes' requests as "restate <path>
// key=<Idempotency-Key>" and "eri <path>", ingress first.
func restateClientCalls(ingress, eri *restateingresstest.Server) []string {
	ingressCalls, eriCalls := ingress.Calls(), eri.Calls()
	if len(ingressCalls)+len(eriCalls) == 0 {
		return nil
	}
	calls := make([]string, 0, len(ingressCalls)+len(eriCalls))
	for _, c := range ingressCalls {
		calls = append(calls, fmt.Sprintf("restate %s key=%s", c.Path, c.IdempotencyKey))
	}
	for _, c := range eriCalls {
		calls = append(calls, "eri "+c.Path)
	}
	return calls
}

func TestRestateEriClient_Dispatch(t *testing.T) {
	t.Parallel()
	closedURL := eriClosedURL(t)
	accepted := `{"status":"accepted","dispatch_id":"d1"}`
	d1 := eriwire.BrainDispatchRequest{ID: "d1", Source: "test"}
	tests := []struct {
		name       string
		noRestate  bool
		noEri      bool
		restateURL string
		req        eriwire.BrainDispatchRequest
		path       string
		status     int
		body       string
		wantErr    string
		wantCalls  []string
	}{
		{
			name: "restate accepted on the chunk session", req: eriwire.BrainDispatchRequest{ID: "d1", VanadisChunkID: "c1", Source: "test"}, path: "/DispatchSession/vanadis-c1/Enqueue", body: accepted,
			wantCalls: []string{"restate /DispatchSession/vanadis-c1/Enqueue key=d1"},
		},
		{
			name: "request agent picks the agent session", req: eriwire.BrainDispatchRequest{ID: "d1", Agent: "agent-alpha", VanadisChunkID: "c1", Source: "test"},
			path: "/DispatchSession-agent-alpha/vanadis-c1/Enqueue", body: accepted, wantCalls: []string{"restate /DispatchSession-agent-alpha/vanadis-c1/Enqueue key=d1"},
		},
		{
			name: "skipped active maps to dispatch active", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", body: `{"status":"skipped_active"}`,
			wantErr: eriwire.ErrDispatchActive.Error(), wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{
			name: "unknown status is rejected", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", body: `{"status":"rejected"}`,
			wantErr: `restate rejected dispatch "d1" (status "rejected")`, wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{
			name: "http 500 carries the message field", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", status: 500, body: `{"message":"actor busy"}`,
			wantErr: "restate dispatch error (500): actor busy", wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{
			name: "http 400 carries the error field", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", status: 400, body: `{"error":"invalid payload"}`,
			wantErr: "restate dispatch error (400): invalid payload", wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{
			name: "http 502 non json carries the raw body", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", status: 502, body: "raw text gateway error",
			wantErr: "restate dispatch returned HTTP 502: raw text gateway error", wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{
			name: "malformed json reply errors", req: d1, path: "/DispatchSession/dispatch-d1/Enqueue", body: "{invalid-json",
			wantErr: "decode restate dispatch response: invalid character 'i'", wantCalls: []string{"restate /DispatchSession/dispatch-d1/Enqueue key=d1"},
		},
		{name: "bad restate url fails building the request", restateURL: "http://127.0.0.1:0\nbad", req: d1, wantErr: "dispatch failed: "},
		{name: "unreachable restate fails", restateURL: closedURL, req: d1, wantErr: "dispatch failed: "},
		{name: "no restate url falls back to eri", noRestate: true, req: d1, path: "/dispatch", body: accepted, wantCalls: []string{"eri /dispatch"}},
		{name: "neither url configured", noRestate: true, noEri: true, req: d1, wantErr: "neither restate url nor eri url configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, tc.path, tc.status, tc.body)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, tc.noEri, tc.restateURL)
			err := NewRestateEriClient(restateURL, eriURL, "aerial", nil).Dispatch(context.Background(), tc.req)
			requireErrText(t, err, tc.wantErr)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_DispatchAccepted(t *testing.T) {
	t.Parallel()
	accepted := `{"status":"accepted","dispatch_id":"d1"}`
	d1 := eriwire.BrainDispatchRequest{ID: "d1", Source: "test"}
	const path = "/DispatchSession/dispatch-d1/Enqueue"
	tests := []struct {
		name      string
		noRestate bool
		req       eriwire.BrainDispatchRequest
		attempt   string
		path      string
		body      string
		wantErr   string
		wantCalls []string
	}{
		{name: "accepted with dispatch id", req: d1, path: path, body: accepted, wantCalls: []string{"restate " + path + " key=d1"}},
		{name: "launch attempt scopes idempotency key", req: d1, attempt: "inv-7", path: path, body: accepted, wantCalls: []string{"restate " + path + " key=d1:inv-7"}},
		{name: "blank launch attempt keeps dispatch key", req: d1, attempt: "  ", path: path, body: accepted, wantCalls: []string{"restate " + path + " key=d1"}},
		{
			name: "launch attempt without dispatch id sends no key", req: eriwire.BrainDispatchRequest{Source: "test"}, attempt: "inv-7", path: "/DispatchSession/dispatch-/Enqueue",
			body: `{"status":"accepted"}`, wantErr: `restate did not accept dispatch "" (status "accepted", identity "")`, wantCalls: []string{"restate /DispatchSession/dispatch-/Enqueue key="},
		},
		{name: "already exists with message id", req: d1, path: path, body: `{"status":"already_exists","message_id":"d1"}`, wantCalls: []string{"restate " + path + " key=d1"}},
		{
			name: "skipped active maps to dispatch active", req: d1, path: path, body: `{"status":"skipped_active"}`, wantErr: eriwire.ErrDispatchActive.Error(),
			wantCalls: []string{"restate " + path + " key=d1"},
		},
		{
			name: "skipped active re-send under new attempt uses fresh key", req: d1, attempt: "inv-8", path: path, body: `{"status":"skipped_active"}`,
			wantErr: eriwire.ErrDispatchActive.Error(), wantCalls: []string{"restate " + path + " key=d1:inv-8"},
		},
		{
			name: "mismatched identity errors", req: d1, path: path, body: `{"status":"accepted","dispatch_id":"other"}`,
			wantErr: `restate did not accept dispatch "d1" (status "accepted", identity "other")`, wantCalls: []string{"restate " + path + " key=d1"},
		},
		{name: "empty reply errors", req: d1, path: path, wantErr: `restate did not accept dispatch "d1" (status "", identity "")`, wantCalls: []string{"restate " + path + " key=d1"}},
		{name: "no restate url falls back to eri", noRestate: true, req: d1, path: "/dispatch", body: accepted, wantCalls: []string{"eri /dispatch"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, tc.path, 0, tc.body)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, false, "")
			err := NewRestateEriClient(restateURL, eriURL, "aerial", nil).DispatchAccepted(WithDispatchAttempt(context.Background(), tc.attempt), tc.req)
			requireErrText(t, err, tc.wantErr)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_CancelDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		noRestate bool
		noEri     bool
		chunkID   string
		path      string
		wantErr   string
		wantCalls []string
	}{
		{name: "restate cancels the chunk session", chunkID: "c1", path: "/DispatchSession/vanadis-c1/Cancel", wantCalls: []string{"restate /DispatchSession/vanadis-c1/Cancel key="}},
		{name: "blank chunk is a noop", chunkID: " "},
		{name: "no restate url falls back to eri", noRestate: true, chunkID: "c1", path: "/dispatch/cancel", wantCalls: []string{"eri /dispatch/cancel"}},
		{name: "neither url configured", noRestate: true, noEri: true, chunkID: "c1", wantErr: "neither restate url nor eri url configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, tc.path, 0, `{"status":"ok"}`)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, tc.noEri, "")
			requireErrText(t, NewRestateEriClient(restateURL, eriURL, "aerial", nil).CancelDispatch(context.Background(), tc.chunkID), tc.wantErr)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_Prompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		noRestate bool
		noEri     bool
		thread    string
		prompt    string
		path      string
		wantErr   string
		wantCalls []string
	}{
		{
			name: "restate enqueues on the thread session", thread: "th-1", prompt: "hello world", path: "/ThreadSession/th-1/Enqueue",
			wantCalls: []string{"restate /ThreadSession/th-1/Enqueue key=prompt:th-1:vanadis-prompt-83e7044d26ed4a0c7faf9d30d9d2089e"},
		},
		{name: "blank thread and prompt is a noop", thread: " ", prompt: " "},
		{name: "no restate url falls back to eri", noRestate: true, thread: "th-1", prompt: "hello", path: "/prompt", wantCalls: []string{"eri /prompt"}},
		{name: "neither url configured", noRestate: true, noEri: true, thread: "th-1", prompt: "hello", wantErr: "neither restate url nor eri url configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, tc.path, 0, `{"status":"accepted"}`)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, tc.noEri, "")
			requireErrText(t, NewRestateEriClient(restateURL, eriURL, "aerial", nil).Prompt(context.Background(), tc.thread, tc.prompt), tc.wantErr)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

// The widget and message tables set Discord auth with SetDiscordAuth, so the
// DISCORD_TOKEN the constructor reads from the process env never matters.

func TestRestateEriClient_Widget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		auth      bool
		noRestate bool
		noEri     bool
		deliverer *mockDeliverer
		wantID    string
		wantErr   string
		wantCalls []string
	}{
		{name: "direct deliverer with auth and restate", auth: true, deliverer: &mockDeliverer{msgID: "msg-direct-1"}, wantID: "msg-direct-1"},
		{name: "missing auth falls back to eri", deliverer: &mockDeliverer{msgID: "unused"}, wantID: "msg-eri", wantCalls: []string{"eri /discord/widget"}},
		{name: "missing restate url falls back to eri", auth: true, noRestate: true, deliverer: &mockDeliverer{msgID: "unused"}, wantID: "msg-eri", wantCalls: []string{"eri /discord/widget"}},
		{name: "unconfigured errors", noRestate: true, noEri: true, wantErr: "neither widget deliverer nor eri url configured for widget delivery"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/discord/widget", 0, `{"message_id":"msg-eri"}`)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, tc.noEri, "")
			var deliverer StatusWidgetDeliverer
			if tc.deliverer != nil {
				deliverer = tc.deliverer
			}
			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil, deliverer)
			client.SetDiscordAuth(tc.auth)
			got, err := client.Widget(context.Background(), StatusWidgetRequest{ID: "w1"})
			requireErrText(t, err, tc.wantErr)
			assert.Equal(t, tc.wantID, got)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_DeleteWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		auth        bool
		noRestate   bool
		deliverer   *mockDeliverer
		agent       string
		channel     string
		message     string
		ctxAgent    string
		wantErr     string
		wantDeleted []string
	}{
		{name: "direct deliverer deletes with the explicit agent", auth: true, deliverer: &mockDeliverer{}, agent: "Beta", channel: " ch-1 ", message: " m1 ", wantDeleted: []string{"beta ch-1 m1"}},
		{name: "empty agent resolves from context", auth: true, deliverer: &mockDeliverer{}, ctxAgent: "alpha", channel: "ch-1", message: "m1", wantDeleted: []string{"alpha ch-1 m1"}},
		{name: "deliverer error propagates", auth: true, deliverer: &mockDeliverer{err: errBoom}, channel: "ch-1", message: "m1", wantErr: "boom", wantDeleted: []string{"aerial ch-1 m1"}},
		{name: "missing auth is a fallback no-op", deliverer: &mockDeliverer{}, channel: "ch-1", message: "m1"},
		{name: "missing restate url is a fallback no-op", auth: true, noRestate: true, deliverer: &mockDeliverer{}, channel: "ch-1", message: "m1"},
		{name: "no deliverer is a fallback no-op", auth: true, channel: "ch-1", message: "m1"},
		{name: "blank message is a no-op", auth: true, deliverer: &mockDeliverer{}, channel: "ch-1", message: " "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/discord/widget", 0, `{}`)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, false, "")
			var deliverer StatusWidgetDeliverer
			if tc.deliverer != nil {
				deliverer = tc.deliverer
			}
			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil, deliverer)
			client.SetDiscordAuth(tc.auth)
			ctx := context.Background()
			if tc.ctxAgent != "" {
				ctx = WithAgent(ctx, tc.ctxAgent)
			}
			requireErrText(t, client.DeleteWidget(ctx, tc.agent, tc.channel, tc.message), tc.wantErr)
			var deleted []string
			if tc.deliverer != nil {
				deleted = tc.deliverer.deleted
			}
			assert.Equal(t, tc.wantDeleted, deleted)
			assert.Empty(t, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_Message(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		auth          bool
		noRestate     bool
		noEri         bool
		deliverer     StatusWidgetDeliverer
		channel       string
		wantErr       string
		wantDelivered string
		wantCalls     []string
	}{
		{name: "plain deliverer with auth and restate", auth: true, deliverer: &mockDeliverer{}, channel: "ch-1", wantDelivered: "ch-1 test content"},
		{name: "agent deliverer routes through the client agent", auth: true, deliverer: &agentDeliverer{}, channel: "ch-1", wantDelivered: "aerial ch-1 test content"},
		{name: "deliverer error propagates", auth: true, deliverer: &mockDeliverer{err: errBoom}, channel: "ch-1", wantErr: "boom", wantDelivered: "ch-1 test content"},
		{name: "missing auth falls back to eri", deliverer: &mockDeliverer{}, channel: "ch-1", wantCalls: []string{"eri /discord/message"}},
		{name: "missing restate url falls back to eri", auth: true, noRestate: true, deliverer: &mockDeliverer{}, channel: "ch-1", wantCalls: []string{"eri /discord/message"}},
		{name: "no deliverer falls back to eri", auth: true, channel: "ch-1", wantCalls: []string{"eri /discord/message"}},
		{name: "blank channel is a noop", auth: true, noRestate: true, noEri: true, channel: " "},
		{
			name: "unconfigured errors", noRestate: true, noEri: true, channel: "ch-1",
			wantErr: "neither direct message deliverer nor http fallback configured for message delivery",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/discord/message", 0, `{"status":"ok"}`)
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, tc.noEri, "")
			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil, tc.deliverer)
			client.SetDiscordAuth(tc.auth)
			requireErrText(t, client.Message(context.Background(), tc.channel, "test content"), tc.wantErr)
			delivered := ""
			if rec, ok := tc.deliverer.(deliveryRecorder); ok {
				delivered = rec.delivered()
			}
			assert.Equal(t, tc.wantDelivered, delivered)
			assert.Equal(t, tc.wantCalls, restateClientCalls(ingress, eri))
		})
	}
}

func TestRestateEriClient_NilReceiver(t *testing.T) {
	t.Parallel()

	var nilClient *RestateEriClient
	ctx := context.Background()

	if err := nilClient.Dispatch(ctx, eriwire.BrainDispatchRequest{}); err == nil {
		t.Error("expected error on nil client Dispatch")
	}
	if err := nilClient.DispatchAccepted(ctx, eriwire.BrainDispatchRequest{}); err == nil {
		t.Error("expected error on nil client DispatchAccepted")
	}
	if err := nilClient.CancelDispatch(ctx, "c1"); err == nil {
		t.Error("expected error on nil client CancelDispatch")
	}
	if err := nilClient.Prompt(ctx, "th1", "p"); err == nil {
		t.Error("expected error on nil client Prompt")
	}
	if _, err := nilClient.Widget(ctx, StatusWidgetRequest{}); err == nil {
		t.Error("expected error on nil client Widget")
	}
	if err := nilClient.Message(ctx, "ch1", "m"); err == nil {
		t.Error("expected error on nil client Message")
	}
	if err := nilClient.DeleteWidget(ctx, "a", "ch1", "m1"); err == nil {
		t.Error("expected error on nil client DeleteWidget")
	}
	if err := nilClient.PromptAgent(ctx, "a", "th1", "p", "k"); err == nil {
		t.Error("expected error on nil client PromptAgent")
	}
	if err := nilClient.DispatchRestate(ctx, eriwire.BrainDispatchRequest{}); err == nil {
		t.Error("expected error on nil client DispatchRestate")
	}
	if err := nilClient.MessageAgent(ctx, "a", "ch1", "m"); err == nil {
		t.Error("expected error on nil client MessageAgent")
	}
}

func TestAgentContextAndEffectiveResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clientAgent   string
		ctxAgent      string
		explicitAgent string
		wantAgent     string
	}{
		{
			name:      "all empty falls back to default agent",
			wantAgent: "aerial",
		},
		{
			name:        "client agent used when context and explicit empty",
			clientAgent: "agent-alpha",
			wantAgent:   "agent-alpha",
		},
		{
			name:        "context agent overrides client agent",
			clientAgent: "agent-alpha",
			ctxAgent:    "agent-beta",
			wantAgent:   "agent-beta",
		},
		{
			name:        "empty with-agent context preserves fallback",
			clientAgent: "agent-alpha",
			ctxAgent:    "   ",
			wantAgent:   "agent-alpha",
		},
		{
			name:          "explicit agent overrides context and client",
			clientAgent:   "agent-alpha",
			ctxAgent:      "agent-beta",
			explicitAgent: "agent-gamma",
			wantAgent:     "agent-gamma",
		},
		{
			name:          "whitespace trimmed from explicit agent",
			clientAgent:   "agent-alpha",
			explicitAgent: "  agent-beta  ",
			wantAgent:     "agent-beta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.ctxAgent != "" {
				ctx = WithAgent(ctx, tc.ctxAgent)
			}
			c := NewRestateEriClient("", "", tc.clientAgent, nil)
			var got string
			if tc.explicitAgent != "" {
				got = c.effectiveAgent(ctx, tc.explicitAgent)
			} else {
				got = c.effectiveAgent(ctx)
			}
			if got != tc.wantAgent {
				t.Errorf("effectiveAgent() = %q, want %q", got, tc.wantAgent)
			}
		})
	}
}
