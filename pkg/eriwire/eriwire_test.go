package eriwire

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveDispatchSessionKey(t *testing.T) {
	t.Parallel()
	type row struct {
		name string
		req  BrainDispatchRequest
		want string
	}
	tests := []row{
		{"session wins", BrainDispatchRequest{SessionID: " sess-abc ", VanadisChunkID: "c-1", ID: "d-1"}, "sess-abc"},
		{"chunk", BrainDispatchRequest{VanadisChunkID: "chunk-xyz", ID: "d-2"}, "vanadis-chunk-xyz"},
		{"id", BrainDispatchRequest{ID: " d-3 "}, "dispatch-d-3"},
		{"empty", BrainDispatchRequest{}, "dispatch-"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ResolveDispatchSessionKey(tc.req))
		})
	}
}

func TestParseEffort(t *testing.T) {
	t.Parallel()
	type row struct {
		name     string
		raw      string
		fallback string
		want     string
	}
	tc := func(name, raw, fallback, want string) row {
		return row{name: name, raw: raw, fallback: fallback, want: want}
	}
	tests := []row{
		tc("low", "low", "high", EffortLow),
		tc("low with spaces and upper", "  LOW  ", "high", EffortLow),
		tc("medium", "medium", "low", EffortMedium),
		tc("high", "HIGH", "low", EffortHigh),
		tc("unknown fallback", "extreme", "low", "low"),
		tc("empty fallback", "", "medium", "medium"),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ParseEffort(tt.raw, tt.fallback))
		})
	}
}

func TestLLMKeyContext(t *testing.T) {
	t.Parallel()
	type row struct {
		name string
		key  string
		set  bool
		want string
	}
	tc := func(name, key string, set bool, want string) row {
		return row{name: name, key: key, set: set, want: want}
	}
	tests := []row{
		tc("clean key", "key-123", true, "key-123"),
		tc("untrimmed key", "  key-456  ", true, "key-456"),
		tc("unset key", "", false, ""),
		tc("empty set key", "   ", true, ""),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			if tt.set {
				ctx = WithLLMKey(ctx, tt.key)
			}
			assert.Equal(t, tt.want, LLMKeyFrom(ctx))
		})
	}
}

func TestInternalLLMClient_Complete(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if r.Header.Get("Idempotency-Key") == "test-key" {
				w.WriteHeader(http.StatusOK)
				io.WriteString(w, `{"response":"reply-with-key"}`)
				return
			}
			if eff, ok := raw["effort"].(string); ok {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, `{"response":"reply-effort-%s"}`, eff)
				return
			}
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{"response":"reply-ok"}`)
		case "/bad-json":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{not valid json`)
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, `internal server error`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	type row struct {
		name       string
		client     *InternalLLMClient
		prompt     string
		key        string
		wantReply  string
		wantErrSub string
	}
	tc := func(name string, client *InternalLLMClient, prompt, key, wantReply, wantErrSub string) row {
		return row{name: name, client: client, prompt: prompt, key: key, wantReply: wantReply, wantErrSub: wantErrSub}
	}
	tests := []row{
		tc("nil client", nil, "hello", "", "", "not initialized"),
		tc("empty prompt", NewInternalLLMClient(ts.URL+"/ok", ts.Client()), "   ", "", "", "prompt cannot be empty"),
		tc("empty url", NewInternalLLMClient("", ts.Client()), "hello", "", "", "url is not configured"),
		tc("nil http client uses default", NewInternalLLMClient(ts.URL+"/ok", nil), "hello", "", "reply-ok", ""),
		tc("invalid url request creation error", NewInternalLLMClient("http://\x7f", ts.Client()), "hello", "", "", "create internal llm request"),
		tc("unreachable server network error", NewInternalLLMClient("http://127.0.0.1:0", ts.Client()), "hello", "", "", "internal llm request failed"),
		tc("success response without effort", NewInternalLLMClient(ts.URL+"/ok", ts.Client()), "hello", "", "reply-ok", ""),
		tc("success response with effort", NewInternalLLMClient(ts.URL+"/ok", ts.Client(), EffortMedium), "hello", "", "reply-effort-medium", ""),
		tc("with idempotency key", NewInternalLLMClient(ts.URL+"/ok", ts.Client()), "hello", "test-key", "reply-with-key", ""),
		tc("server 500 error", NewInternalLLMClient(ts.URL+"/500", ts.Client()), "hello", "", "", "status 500"),
		tc("bad json response", NewInternalLLMClient(ts.URL+"/bad-json", ts.Client()), "hello", "", "", "decode internal llm response"),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			if tt.key != "" {
				ctx = WithLLMKey(ctx, tt.key)
			}
			got, err := tt.client.Complete(ctx, tt.prompt)
			if tt.wantErrSub != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSub)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantReply, got)
			}
		})
	}
}

func TestHTTPInternalLLMClient(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"response":"closure-ok"}`)
	}))
	t.Cleanup(ts.Close)

	clientFn := HTTPInternalLLMClient(ts.URL, ts.Client())
	got, err := clientFn(t.Context(), "summarize this")
	assert.NoError(t, err)
	assert.Equal(t, "closure-ok", got)
}

func TestServiceNames(t *testing.T) {
	t.Parallel()
	type row struct {
		name         string
		agent        string
		wantThread   string
		wantDispatch string
		wantVoice    string
		wantManager  string
	}
	tests := []row{
		{
			name:         "empty defaults to unsuffixed",
			agent:        "",
			wantThread:   "ThreadSession",
			wantDispatch: "DispatchSession",
			wantVoice:    "VoiceSession",
			wantManager:  "SessionManager",
		},
		{
			name:         "default agent aerial",
			agent:        "aerial",
			wantThread:   "ThreadSession",
			wantDispatch: "DispatchSession",
			wantVoice:    "VoiceSession",
			wantManager:  "SessionManager",
		},
		{
			name:         "custom agent name",
			agent:        "sidecar",
			wantThread:   "ThreadSession-sidecar",
			wantDispatch: "DispatchSession-sidecar",
			wantVoice:    "VoiceSession-sidecar",
			wantManager:  "SessionManager-sidecar",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantThread, ThreadSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantDispatch, DispatchSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantVoice, VoiceSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantManager, SessionManagerServiceName(tc.agent))
		})
	}
}

func TestRestateMethodEndpoint(t *testing.T) {
	t.Parallel()
	type row struct {
		name       string
		restateURL string
		service    string
		key        string
		method     string
		want       string
	}
	tests := []row{
		{
			name:       "unkeyed service method",
			restateURL: "http://restate.aerial:8080/",
			service:    "SessionManager",
			key:        "",
			method:     "CancelDispatch",
			want:       "http://restate.aerial:8080/SessionManager/CancelDispatch",
		},
		{
			name:       "keyed virtual object method",
			restateURL: "http://restate.aerial:8080",
			service:    "ThreadSession",
			key:        "chan-123",
			method:     "Enqueue",
			want:       "http://restate.aerial:8080/ThreadSession/chan-123/Enqueue",
		},
		{
			name:       "escaped characters in key and service",
			restateURL: "  http://restate.aerial:8080/// ",
			service:    "ThreadSession-custom/v1",
			key:        "special/key with spaces",
			method:     "Enqueue",
			want:       "http://restate.aerial:8080/ThreadSession-custom%2Fv1/special%2Fkey%20with%20spaces/Enqueue",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, RestateMethodEndpoint(tc.restateURL, tc.service, tc.key, tc.method))
		})
	}
}

func TestRestateEnqueueEnvelope_EffectiveID(t *testing.T) {
	t.Parallel()
	type row struct {
		name string
		env  RestateEnqueueEnvelope
		want string
	}
	tests := []row{
		{
			name: "dispatch id takes precedence",
			env:  RestateEnqueueEnvelope{DispatchID: "disp-1", MessageID: "msg-1"},
			want: "disp-1",
		},
		{
			name: "message id when dispatch id is empty",
			env:  RestateEnqueueEnvelope{MessageID: "msg-2"},
			want: "msg-2",
		},
		{
			name: "empty when both empty",
			env:  RestateEnqueueEnvelope{},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.env.EffectiveID())
		})
	}
}

func TestParseRestateEnqueueEnvelope(t *testing.T) {
	t.Parallel()
	type row struct {
		name       string
		body       []byte
		want       RestateEnqueueEnvelope
		wantErrSub string
	}
	tests := []row{
		{
			name: "nil body returns zero envelope",
			body: nil,
			want: RestateEnqueueEnvelope{},
		},
		{
			name: "empty body returns zero envelope",
			body: []byte(""),
			want: RestateEnqueueEnvelope{},
		},
		{
			name: "full json payload",
			body: []byte(`{"status":"accepted","channel_id":"c1","thread_id":"t1","dispatch_id":"d1","message_id":"m1","session_id":"s1"}`),
			want: RestateEnqueueEnvelope{
				Status:     "accepted",
				ChannelID:  "c1",
				ThreadID:   "t1",
				DispatchID: "d1",
				MessageID:  "m1",
				SessionID:  "s1",
			},
		},
		{
			name:       "invalid json returns error",
			body:       []byte(`{invalid`),
			wantErrSub: "invalid character",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRestateEnqueueEnvelope(tc.body)
			if tc.wantErrSub != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}
