package ericlient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// ingressCall is one request the fake Restate ingress received.
type ingressCall struct {
	path string
	key  string
	body map[string]any
}

// ingressRecorder is a declarative fake Restate ingress (and eri HTTP
// fallback) that records each request and replies with status and body.
type ingressRecorder struct {
	status int
	body   string

	mu    sync.Mutex
	calls []ingressCall
}

func (r *ingressRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(req.Body).Decode(&body)
	r.mu.Lock()
	r.calls = append(r.calls, ingressCall{path: req.URL.Path, key: req.Header.Get("Idempotency-Key"), body: body})
	r.mu.Unlock()
	w.WriteHeader(r.status)
	_, _ = w.Write([]byte(r.body))
}

func (r *ingressRecorder) Calls() []ingressCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ingressCall(nil), r.calls...)
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
			name: "enqueues on the agent thread session", status: http.StatusOK, agent: "Miorine", thread: "T1", key: "prstate-abc",
			wantCalls: []ingressCall{{path: "/ThreadSession-miorine/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
		{
			name: "empty agent uses the client agent", status: http.StatusOK, agent: "", thread: "T1", key: "prstate-abc",
			wantCalls: []ingressCall{{path: "/ThreadSession/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
		{name: "empty thread sends nothing", status: http.StatusOK, agent: "miorine", thread: " ", key: "prstate-abc"},
		{name: "missing key fails", status: http.StatusOK, agent: "miorine", thread: "T1", key: " ", wantErr: true},
		{name: "no restate url never falls back to eri", noRestate: true, status: http.StatusOK, agent: "miorine", thread: "T1", key: "prstate-abc", wantErr: true},
		{
			name: "ingress failure surfaces", status: http.StatusInternalServerError, agent: "miorine", thread: "T1", key: "prstate-abc", wantErr: true,
			wantCalls: []ingressCall{{path: "/ThreadSession-miorine/T1/Enqueue", key: "prstate-abc", body: map[string]any{"channel_id": "T1", "prompt": "wake", "message_id": "vanadis-prompt-6b2f417788dc68777681c92ce3796215"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ingressRecorder{status: tc.status}
			eri := &ingressRecorder{status: http.StatusOK}
			ingressSrv, eriSrv := httptest.NewServer(ingress), httptest.NewServer(eri)
			t.Cleanup(ingressSrv.Close)
			t.Cleanup(eriSrv.Close)
			restateURL := ingressSrv.URL
			if tc.noRestate {
				restateURL = ""
			}
			client := NewRestateEriClient(restateURL, eriSrv.URL, "aerial", nil)
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
	req := BrainDispatchRequest{ID: "prstate-d1", Agent: "miorine", Source: "prstate", TargetID: "T1", Prompt: "fix"}
	tests := []struct {
		name      string
		noRestate bool
		body      string
		wantErr   error
		wantPaths []string
	}{
		{name: "enqueues on the agent dispatch session", body: `{"status":"accepted","dispatch_id":"prstate-d1"}`, wantPaths: []string{"/DispatchSession-miorine/dispatch-prstate-d1/Enqueue"}},
		{name: "busy session maps to ErrDispatchActive", body: `{"status":"skipped_active"}`, wantErr: ErrDispatchActive, wantPaths: []string{"/DispatchSession-miorine/dispatch-prstate-d1/Enqueue"}},
		{name: "no restate url never falls back to eri", noRestate: true, wantErr: errRestateNotConfigured},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ingressRecorder{status: http.StatusOK, body: tc.body}
			eri := &ingressRecorder{status: http.StatusOK, body: `{"status":"accepted","dispatch_id":"prstate-d1"}`}
			ingressSrv, eriSrv := httptest.NewServer(ingress), httptest.NewServer(eri)
			t.Cleanup(ingressSrv.Close)
			t.Cleanup(eriSrv.Close)
			restateURL := ingressSrv.URL
			if tc.noRestate {
				restateURL = ""
			}
			err := NewRestateEriClient(restateURL, eriSrv.URL, "aerial", nil).DispatchRestate(context.Background(), req)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("DispatchRestate err = %v, want %v", err, tc.wantErr)
			}
			var paths []string
			for _, c := range ingress.Calls() {
				paths = append(paths, c.path)
				if c.key != "prstate-d1" {
					t.Errorf("Idempotency-Key = %q, want prstate-d1", c.key)
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
		{name: "sends through the agent sender", auth: true, agent: "Suletta", channel: " C1 ", wantSends: 1, wantAgent: "suletta", wantChannel: "C1"},
		{name: "empty agent uses the client agent", auth: true, agent: "", channel: "C1", wantSends: 1, wantAgent: "aerial", wantChannel: "C1"},
		{name: "empty channel sends nothing", auth: true, agent: "suletta", channel: " "},
		{name: "missing discord auth fails", auth: false, agent: "suletta", channel: "C1", wantErr: true},
		{name: "deliverer without agent routing fails", auth: true, plain: true, agent: "suletta", channel: "C1", wantErr: true},
		{name: "delivery failure surfaces", auth: true, deliverErr: errors.New("discord down"), agent: "suletta", channel: "C1", wantErr: true, wantSends: 1, wantAgent: "suletta", wantChannel: "C1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &ingressRecorder{status: http.StatusOK}
			eriSrv := httptest.NewServer(eri)
			t.Cleanup(eriSrv.Close)
			deliverer := &agentDeliverer{}
			deliverer.err = tc.deliverErr
			var wd StatusWidgetDeliverer = deliverer
			if tc.plain {
				wd = &mockDeliverer{}
			}
			client := NewRestateEriClient("http://restate.invalid", eriSrv.URL, "aerial", nil, wd)
			client.SetDiscordAuth(tc.auth)
			err := client.MessageAgent(context.Background(), tc.agent, tc.channel, "hello")
			if (err != nil) != tc.wantErr {
				t.Fatalf("MessageAgent err = %v, wantErr %v", err, tc.wantErr)
			}
			if deliverer.sends != tc.wantSends || deliverer.lastAgent != tc.wantAgent || deliverer.lastChannel != tc.wantChannel {
				t.Errorf("deliverer = (%d, %q, %q), want (%d, %q, %q)", deliverer.sends, deliverer.lastAgent, deliverer.lastChannel, tc.wantSends, tc.wantAgent, tc.wantChannel)
			}
			if got := len(eri.Calls()); got != 0 {
				t.Errorf("eri HTTP hits = %d, want 0", got)
			}
		})
	}
}

func assertIngressCalls(t *testing.T, want, got []ingressCall) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ingress calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].path != want[i].path || got[i].key != want[i].key {
			t.Errorf("call %d = (%q, %q), want (%q, %q)", i, got[i].path, got[i].key, want[i].path, want[i].key)
		}
		for k, v := range want[i].body {
			if got[i].body[k] != v {
				t.Errorf("call %d body[%s] = %v, want %v", i, k, got[i].body[k], v)
			}
		}
	}
}
