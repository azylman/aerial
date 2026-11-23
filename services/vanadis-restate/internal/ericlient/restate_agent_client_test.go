package ericlient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingresstest"
)

// ingressCall is one request the fake Restate ingress received.
type ingressCall struct {
	path string
	key  string
	body map[string]any
}

// TestRestateEriClient_PromptAgent pins the agent-scoped Restate prompt:
// it enqueues on the agent's ThreadSession with the caller's idempotency
// key and a message ID derived from it, and never falls back to eri HTTP.
func TestRestateEriClient_PromptAgent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		noRestate   bool
		status      int
		agent       string
		thread      string
		key         string
		wantErr     bool
		wantCalls   []ingressCall
		wantEriHits int
	}{
		{
			name: "enqueues on the agent thread session", status: http.StatusOK, agent: "Agent-Alpha", thread: "T1", key: "prstate-abc",
			wantCalls: []ingressCall{{path: "/ThreadSession-agent-alpha/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
		{
			name: "empty agent uses the client agent", status: http.StatusOK, agent: "", thread: "T1", key: "prstate-abc",
			wantCalls: []ingressCall{{path: "/ThreadSession/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
		{name: "empty thread sends nothing", status: http.StatusOK, agent: "agent-alpha", thread: " ", key: "prstate-abc"},
		{name: "missing key fails", status: http.StatusOK, agent: "agent-alpha", thread: "T1", key: " ", wantErr: true},
		{name: "no restate url never falls back to eri", noRestate: true, status: http.StatusOK, agent: "agent-alpha", thread: "T1", key: "prstate-abc", wantErr: true},
		{
			name: "ingress failure surfaces", status: http.StatusInternalServerError, agent: "agent-alpha", thread: "T1", key: "prstate-abc", wantErr: true,
			wantCalls: []ingressCall{{path: "/ThreadSession-agent-alpha/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/ThreadSession-agent-alpha/T1/Enqueue", tc.status, "")
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, false, "")
			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil)
			err := client.PromptAgent(context.Background(), tc.agent, tc.thread, "wake", tc.key)
			if (err != nil) != tc.wantErr {
				t.Fatalf("PromptAgent err = %v, wantErr %v", err, tc.wantErr)
			}
			assertIngressCalls(t, tc.wantCalls, ingress.Calls())
			if got := len(eri.Calls()); got != tc.wantEriHits {
				t.Errorf("eri HTTP hits = %d, want %d", got, tc.wantEriHits)
			}
		})
	}
}

// TestRestateEriClient_DispatchRestate pins the Restate-only background
// dispatch: it enqueues on the request agent's DispatchSession and refuses
// to fall back to eri HTTP.
func TestRestateEriClient_DispatchRestate(t *testing.T) {
	t.Parallel()
	req := eriwire.BrainDispatchRequest{ID: "prstate-d1", Agent: "agent-alpha", Source: "prstate", TargetID: "T1", Prompt: "fix"}
	tests := []struct {
		name      string
		noRestate bool
		body      string
		wantErr   error
		wantPaths []string
	}{
		{name: "enqueues on the agent dispatch session", body: `{"status":"accepted","dispatch_id":"prstate-d1"}`, wantPaths: []string{"/DispatchSession-agent-alpha/dispatch-prstate-d1/Enqueue"}},
		{name: "busy session maps to eriwire.ErrDispatchActive", body: `{"status":"skipped_active"}`, wantErr: eriwire.ErrDispatchActive, wantPaths: []string{"/DispatchSession-agent-alpha/dispatch-prstate-d1/Enqueue"}},
		{name: "no restate url never falls back to eri", noRestate: true, wantErr: errRestateNotConfigured},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/DispatchSession-agent-alpha/dispatch-prstate-d1/Enqueue", http.StatusOK, tc.body)
			eri.Routes = map[string]restateingresstest.Response{"/dispatch": {Body: `{"status":"accepted","dispatch_id":"prstate-d1"}`}}
			restateURL, eriURL := restateClientURLs(ingress, eri, tc.noRestate, false, "")
			err := NewRestateEriClient(restateURL, eriURL, "aerial", nil).DispatchRestate(context.Background(), req)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("DispatchRestate err = %v, want %v", err, tc.wantErr)
			}
			var paths []string
			for _, c := range ingress.Calls() {
				paths = append(paths, c.Path)
				if c.IdempotencyKey != "prstate-d1" {
					t.Errorf("Idempotency-Key = %q, want prstate-d1", c.IdempotencyKey)
				}
			}
			if len(paths) != len(tc.wantPaths) || (len(paths) == 1 && paths[0] != tc.wantPaths[0]) {
				t.Errorf("ingress paths = %v, want %v", paths, tc.wantPaths)
			}
			if got := len(eri.Calls()); got != 0 {
				t.Errorf("eri HTTP hits = %d, want 0", got)
			}
		})
	}
}

// agentDeliverer records agent-routed direct messages.
type agentDeliverer struct {
	mockDeliverer

	mu        sync.Mutex
	lastAgent string
	sends     int
}

func (a *agentDeliverer) DeliverDirectMessageWithAgent(_ context.Context, agent, channelID, content string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastAgent, a.lastChannel, a.lastContent = agent, channelID, content
	a.sends++
	return a.err
}

// delivered is the last agent-routed message as "<agent> <channel> <content>".
func (a *agentDeliverer) delivered() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastAgent + " " + a.lastChannel + " " + a.lastContent
}

// TestRestateEriClient_MessageAgent pins the agent-routed direct message:
// it sends through the agent's Discord sender, requires direct Discord auth
// and an agent-routing deliverer, and never falls back to eri HTTP.
func TestRestateEriClient_MessageAgent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		auth        bool
		plain       bool
		deliverErr  error
		agent       string
		channel     string
		wantErr     bool
		wantSends   int
		wantAgent   string
		wantChannel string
	}{
		{name: "sends through the agent sender", auth: true, agent: "Agent-Beta", channel: " C1 ", wantSends: 1, wantAgent: "agent-beta", wantChannel: "C1"},
		{name: "empty agent uses the client agent", auth: true, agent: "", channel: "C1", wantSends: 1, wantAgent: "aerial", wantChannel: "C1"},
		{name: "empty channel sends nothing", auth: true, agent: "agent-beta", channel: " "},
		{name: "missing discord auth fails", auth: false, agent: "agent-beta", channel: "C1", wantErr: true},
		{name: "deliverer without agent routing fails", auth: true, plain: true, agent: "agent-beta", channel: "C1", wantErr: true},
		{name: "delivery failure surfaces", auth: true, deliverErr: errors.New("discord down"), agent: "agent-beta", channel: "C1", wantErr: true, wantSends: 1, wantAgent: "agent-beta", wantChannel: "C1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, eri := restateClientFake(t, "/discord/message", http.StatusOK, "")
			deliverer := &agentDeliverer{}
			deliverer.err = tc.deliverErr
			var wd StatusWidgetDeliverer = deliverer
			if tc.plain {
				wd = &mockDeliverer{}
			}
			client := NewRestateEriClient(ingress.URL(), eri.URL(), "aerial", nil, wd)
			client.SetDiscordAuth(tc.auth)
			err := client.MessageAgent(context.Background(), tc.agent, tc.channel, "hello")
			if (err != nil) != tc.wantErr {
				t.Fatalf("MessageAgent err = %v, wantErr %v", err, tc.wantErr)
			}
			if deliverer.sends != tc.wantSends || deliverer.lastAgent != tc.wantAgent || deliverer.lastChannel != tc.wantChannel {
				t.Errorf("deliverer = (%d, %q, %q), want (%d, %q, %q)", deliverer.sends, deliverer.lastAgent, deliverer.lastChannel, tc.wantSends, tc.wantAgent, tc.wantChannel)
			}
			if got := restateClientCalls(ingress, eri); len(got) != 0 {
				t.Errorf("ingress/eri HTTP calls = %v, want none", got)
			}
		})
	}
}

func assertIngressCalls(t *testing.T, want []ingressCall, got []restateingresstest.Call) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ingress calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Path != want[i].path || got[i].IdempotencyKey != want[i].key {
			t.Errorf("call %d = (%q, %q), want (%q, %q)", i, got[i].Path, got[i].IdempotencyKey, want[i].path, want[i].key)
		}
		var body map[string]any
		if err := json.Unmarshal(got[i].Body, &body); err != nil {
			t.Fatalf("call %d body %q: %v", i, got[i].Body, err)
		}
		for k, v := range want[i].body {
			if body[k] != v {
				t.Errorf("call %d body[%s] = %v, want %v", i, k, body[k], v)
			}
		}
	}
}
