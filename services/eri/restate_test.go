package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type remoteQuotaCase struct {
	name           string
	statusCode     int
	responseBody   string
	transportError bool
	wantGroups     int
	wantFirstName  string
}

func runRemoteQuotaCase(t *testing.T, tc remoteQuotaCase) {
	t.Helper()
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/internal/quota", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(tc.statusCode)
		_, _ = w.Write([]byte(tc.responseBody))
	}))
	defer ts.Close()

	client := ts.Client()
	if tc.transportError {
		client = &http.Client{Transport: &errRoundTripper{err: assert.AnError}}
	}

	p := NewRemoteQuotaProvider(ts.URL, client)
	require.NotNil(t, p)
	groups := p.LatestQuotaGroups()
	assert.Len(t, groups, tc.wantGroups)
	if tc.wantGroups > 0 {
		assert.Equal(t, tc.wantFirstName, groups[0].Name)
	}

	fn := p.ProviderFunc()
	fnGroups := fn()
	assert.Len(t, fnGroups, tc.wantGroups)
}

func TestRemoteQuotaProvider(t *testing.T) {
	t.Parallel()

	tests := []remoteQuotaCase{
		{
			name:          "successful quota retrieval",
			statusCode:    http.StatusOK,
			responseBody:  `[{"name":"Claude","buckets":[{"id":"claude-5h","window":"5h","remaining_fraction":0.8}]}]`,
			wantGroups:    1,
			wantFirstName: "Claude",
		},
		{
			name:         "server error status",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error":"internal"}`,
			wantGroups:   0,
		},
		{
			name:         "invalid json body",
			statusCode:   http.StatusOK,
			responseBody: `not-valid-json`,
			wantGroups:   0,
		},
		{
			name:           "transport error",
			statusCode:     http.StatusOK,
			transportError: true,
			wantGroups:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runRemoteQuotaCase(t, tc)
		})
	}
}

func TestRemoteQuotaProvider_NilSafetyAndDefaultURL(t *testing.T) {
	t.Parallel()

	var nilProvider *RemoteQuotaProvider
	assert.Nil(t, nilProvider.LatestQuotaGroups())
	assert.Nil(t, nilProvider.ProviderFunc()())

	defaultProvider := NewRemoteQuotaProvider("")
	assert.Equal(t, "http://eri-restate-health:4028", defaultProvider.eriRestateURL)
}

type remoteLLMCase struct {
	name           string
	statusCode     int
	responseBody   string
	transportError bool
	model          string
	prompt         string
	wantResponse   string
	wantErrorSub   string
}

func runRemoteLLMCase(t *testing.T, tc remoteLLMCase) {
	t.Helper()
	t.Parallel()

	var mu sync.Mutex
	var gotModel, gotPrompt string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/internal/llm", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		var body struct {
			Model  string `json:"model"`
			Prompt string `json:"prompt"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		gotModel = body.Model
		gotPrompt = body.Prompt
		mu.Unlock()

		w.WriteHeader(tc.statusCode)
		_, _ = w.Write([]byte(tc.responseBody))
	}))
	defer ts.Close()

	client := ts.Client()
	if tc.transportError {
		client = &http.Client{Transport: &errRoundTripper{err: assert.AnError}}
	}

	llmFn := NewRemoteEphemeralLLMFunc(ts.URL, client)
	resp, err := llmFn(context.Background(), tc.model, tc.prompt)

	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		return
	}

	require.NoError(t, err)
	assert.Equal(t, tc.wantResponse, resp)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, tc.model, gotModel)
	assert.Equal(t, tc.prompt, gotPrompt)
}

func TestRemoteEphemeralLLMFunc(t *testing.T) {
	t.Parallel()

	tests := []remoteLLMCase{
		{
			name:         "successful generation",
			statusCode:   http.StatusOK,
			responseBody: `{"response":"generated text"}`,
			model:        "gemini-flash",
			prompt:       "summarize this",
			wantResponse: "generated text",
		},
		{
			name:         "server returns error status",
			statusCode:   http.StatusServiceUnavailable,
			responseBody: `{"error":"unavailable"}`,
			model:        "gemini-flash",
			prompt:       "summarize",
			wantErrorSub: "internal llm returned status 503",
		},
		{
			name:         "invalid json body",
			statusCode:   http.StatusOK,
			responseBody: `not-valid-json`,
			model:        "gemini-flash",
			prompt:       "summarize",
			wantErrorSub: "unmarshal internal llm response",
		},
		{
			name:           "transport error",
			statusCode:     http.StatusOK,
			transportError: true,
			model:          "gemini-flash",
			prompt:         "summarize",
			wantErrorSub:   "post internal llm request",
		},
		{
			name:         "oversized response body rejected",
			statusCode:   http.StatusOK,
			responseBody: `{"response":"` + strings.Repeat("x", (1<<20)+16) + `"}`,
			model:        "gemini-flash",
			prompt:       "summarize",
			wantErrorSub: "internal llm response body exceeds 1MiB limit",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runRemoteLLMCase(t, tc)
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestRemoteEphemeralLLMFunc_DefaultURL(t *testing.T) {
	t.Parallel()

	var gotURL string
	var gotMethod string
	var gotBody string
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotURL = r.URL.String()
			gotMethod = r.Method
			bodyBytes, _ := io.ReadAll(r.Body)
			gotBody = string(bodyBytes)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"response":"default-url-ok"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	fn := NewRemoteEphemeralLLMFunc("", client)
	require.NotNil(t, fn)

	resp, err := fn(context.Background(), "test-model", "test-prompt")
	require.NoError(t, err)
	assert.Equal(t, "default-url-ok", resp)
	assert.Equal(t, "http://eri-restate-health:4028/internal/llm", gotURL)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Contains(t, gotBody, "test-prompt")
}

type errRoundTripper struct {
	err error
}

func (e *errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}

type threadSessionOpCase struct {
	name           string
	op             string
	threadID       string
	enqueueReq     queue.ThreadEnqueueRequest
	cancelReq      queue.CancelTurnRequest
	rotateReq      queue.RotateSessionRequest
	serverResp     string
	statusCode     int
	transportError bool
	wantStatus     string
	wantErrorSub   string
	wantPathSub    string
}

func runThreadSessionOperationCase(t *testing.T, tc threadSessionOpCase) {
	t.Helper()
	t.Parallel()

	var mu sync.Mutex
	lastPath := ""

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastPath = r.URL.EscapedPath()
		mu.Unlock()

		w.WriteHeader(tc.statusCode)
		_, _ = w.Write([]byte(tc.serverResp))
	}))
	defer ts.Close()

	httpClient := ts.Client()
	if tc.transportError {
		httpClient = &http.Client{
			Transport: &errRoundTripper{err: assert.AnError},
		}
	}
	client := NewRestateIngressClient(ts.URL, "aerial", httpClient)

	var err error
	switch tc.op {
	case "enqueue":
		resp, e := client.EnqueueThread(context.Background(), tc.threadID, tc.enqueueReq)
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	case "cancel":
		resp, e := client.CancelThreadTurn(context.Background(), tc.threadID, tc.cancelReq)
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	case "cancel_immediate":
		resp, e := client.CancelThreadImmediate(context.Background(), tc.threadID, queue.CancelThreadControlRequest{ThreadID: tc.threadID, Reason: tc.cancelReq.Reason})
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	case "rotate":
		resp, e := client.RotateThreadSession(context.Background(), tc.threadID, tc.rotateReq)
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, lastPath, tc.wantPathSub)

	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		return
	}
	require.NoError(t, err)
}

func TestRestateIngressClient_ThreadSessionOperations(t *testing.T) {
	t.Parallel()

	tests := []threadSessionOpCase{
		{
			name:        "enqueue thread message accepted",
			op:          "enqueue",
			threadID:    "th-101",
			enqueueReq:  queue.ThreadEnqueueRequest{Prompt: "hello"},
			serverResp:  `{"status":"accepted","message_id":"msg-101"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "accepted",
			wantPathSub: "/ThreadSession/th-101/Enqueue",
		},
		{
			name:        "enqueue thread with special characters escaped in url path",
			op:          "enqueue",
			threadID:    "th/special?key#1",
			enqueueReq:  queue.ThreadEnqueueRequest{Prompt: "hello"},
			serverResp:  `{"status":"accepted","message_id":"msg-special"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "accepted",
			wantPathSub: "/ThreadSession/th%2Fspecial%3Fkey%231/Enqueue",
		},
		{
			name:        "enqueue thread message skipped active",
			op:          "enqueue",
			threadID:    "th-102",
			enqueueReq:  queue.ThreadEnqueueRequest{Prompt: "skip", SkipIfActive: true},
			serverResp:  `{"status":"skipped_active"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "skipped_active",
			wantPathSub: "/ThreadSession/th-102/Enqueue",
		},
		{
			name:        "cancel thread turn returns ok",
			op:          "cancel",
			threadID:    "th-103",
			cancelReq:   queue.CancelTurnRequest{Reason: "cancelled"},
			serverResp:  `{"status":"ok","cancelled_count":1,"cancelled_active_turn":true}`,
			statusCode:  http.StatusOK,
			wantStatus:  "ok",
			wantPathSub: "/ThreadSession/th-103/CancelTurn",
		},
		{
			name:        "cancel thread immediate returns ok",
			op:          "cancel_immediate",
			threadID:    "th-103-imm",
			cancelReq:   queue.CancelTurnRequest{Reason: "cancelled"},
			serverResp:  `{"status":"ok","cancelled_count":1,"cancelled_active_turn":true}`,
			statusCode:  http.StatusOK,
			wantStatus:  "ok",
			wantPathSub: "/SessionManager/CancelThread",
		},
		{
			name:        "rotate session returns new session id",
			op:          "rotate",
			threadID:    "th-104",
			rotateReq:   queue.RotateSessionRequest{Topic: "new task"},
			serverResp:  `{"status":"ok","new_session_id":"sess-new-123"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "ok",
			wantPathSub: "/ThreadSession/th-104/RotateSession",
		},
		{
			name:         "server returns 500 error",
			op:           "enqueue",
			threadID:     "th-105",
			enqueueReq:   queue.ThreadEnqueueRequest{Prompt: "boom"},
			serverResp:   `{"error":"server error"}`,
			statusCode:   http.StatusInternalServerError,
			wantErrorSub: "restate returned status 500",
			wantPathSub:  "/ThreadSession/th-105/Enqueue",
		},
		{
			name:         "server returns invalid json response",
			op:           "enqueue",
			threadID:     "th-106",
			enqueueReq:   queue.ThreadEnqueueRequest{Prompt: "corrupt"},
			serverResp:   `not-valid-json`,
			statusCode:   http.StatusOK,
			wantErrorSub: "unmarshal response",
			wantPathSub:  "/ThreadSession/th-106/Enqueue",
		},
		{
			name:           "http transport error returns error",
			op:             "enqueue",
			threadID:       "th-107",
			enqueueReq:     queue.ThreadEnqueueRequest{Prompt: "fail"},
			transportError: true,
			wantErrorSub:   "post restate request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runThreadSessionOperationCase(t, tc)
		})
	}
}

type dispatchOpCase struct {
	name           string
	op             string
	sessionKey     string
	dispatchReq    queue.BrainDispatchRequest
	cancelReq      queue.CancelDispatchRequest
	serverResp     string
	statusCode     int
	transportError bool
	wantStatus     string
	wantActive     bool
	wantErrorSub   string
	wantPathSub    string
}

func runDispatchOperationCase(t *testing.T, tc dispatchOpCase) {
	t.Helper()
	t.Parallel()

	var mu sync.Mutex
	lastPath := ""

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastPath = r.URL.EscapedPath()
		mu.Unlock()

		w.WriteHeader(tc.statusCode)
		_, _ = w.Write([]byte(tc.serverResp))
	}))
	defer ts.Close()

	httpClient := ts.Client()
	if tc.transportError {
		httpClient = &http.Client{
			Transport: &errRoundTripper{err: assert.AnError},
		}
	}
	client := NewRestateIngressClient(ts.URL, "aerial", httpClient)

	var err error
	switch tc.op {
	case "enqueue":
		resp, e := client.EnqueueDispatch(context.Background(), tc.sessionKey, tc.dispatchReq)
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	case "dispatch_accepted":
		err = client.DispatchAccepted(context.Background(), tc.dispatchReq)
		if tc.wantActive {
			assert.ErrorIs(t, err, queue.ErrDispatchActive)
			return
		}
	case "cancel":
		resp, e := client.CancelDispatch(context.Background(), tc.sessionKey, tc.cancelReq)
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	case "cancel_immediate":
		resp, e := client.CancelDispatchImmediate(context.Background(), queue.CancelDispatchControlRequest{
			SessionKey: tc.sessionKey,
			Reason:     tc.cancelReq.Reason,
		})
		err = e
		if err == nil {
			assert.Equal(t, tc.wantStatus, resp.Status)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, lastPath, tc.wantPathSub)

	if tc.wantErrorSub != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrorSub)
		return
	}
	require.NoError(t, err)
}

func TestRestateIngressClient_DispatchOperations(t *testing.T) {
	t.Parallel()

	tests := []dispatchOpCase{
		{
			name:        "enqueue dispatch accepted",
			op:          "enqueue",
			sessionKey:  "disp-201",
			dispatchReq: queue.BrainDispatchRequest{ID: "d-201", Source: "vanadis"},
			serverResp:  `{"status":"accepted","dispatch_id":"d-201"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "accepted",
			wantPathSub: "/DispatchSession/disp-201/Enqueue",
		},
		{
			name:        "dispatch accepted helper matches accepted",
			op:          "dispatch_accepted",
			sessionKey:  "disp-202",
			dispatchReq: queue.BrainDispatchRequest{ID: "d-202", Source: "vanadis", SessionID: "disp-202"},
			serverResp:  `{"status":"accepted","dispatch_id":"d-202"}`,
			statusCode:  http.StatusOK,
			wantPathSub: "/DispatchSession/disp-202/Enqueue",
		},
		{
			name:        "dispatch accepted helper matches already_exists",
			op:          "dispatch_accepted",
			sessionKey:  "disp-203",
			dispatchReq: queue.BrainDispatchRequest{ID: "d-203", Source: "vanadis", SessionID: "disp-203"},
			serverResp:  `{"status":"already_exists","dispatch_id":"d-203"}`,
			statusCode:  http.StatusOK,
			wantPathSub: "/DispatchSession/disp-203/Enqueue",
		},
		{
			name:        "dispatch accepted translates skipped_active to ErrDispatchActive",
			op:          "dispatch_accepted",
			sessionKey:  "disp-204",
			dispatchReq: queue.BrainDispatchRequest{ID: "d-204", Source: "vanadis", SessionID: "disp-204", SkipIfActive: true},
			serverResp:  `{"status":"skipped_active"}`,
			statusCode:  http.StatusOK,
			wantActive:  true,
			wantPathSub: "/DispatchSession/disp-204/Enqueue",
		},
		{
			name:         "dispatch accepted ID mismatch returns error",
			op:           "dispatch_accepted",
			sessionKey:   "disp-205",
			dispatchReq:  queue.BrainDispatchRequest{ID: "d-205", Source: "vanadis", SessionID: "disp-205"},
			serverResp:   `{"status":"accepted","dispatch_id":"d-other"}`,
			statusCode:   http.StatusOK,
			wantErrorSub: "dispatch ID mismatch",
			wantPathSub:  "/DispatchSession/disp-205/Enqueue",
		},
		{
			name:         "dispatch accepted unknown status returns error",
			op:           "dispatch_accepted",
			sessionKey:   "disp-206",
			dispatchReq:  queue.BrainDispatchRequest{ID: "d-206", Source: "vanadis", SessionID: "disp-206"},
			serverResp:   `{"status":"rejected","dispatch_id":"d-206"}`,
			statusCode:   http.StatusOK,
			wantErrorSub: "dispatch rejected with status: rejected",
			wantPathSub:  "/DispatchSession/disp-206/Enqueue",
		},
		{
			name:        "cancel dispatch returns ok",
			op:          "cancel",
			sessionKey:  "vanadis-chunk-207",
			cancelReq:   queue.CancelDispatchRequest{ChunkID: "chunk-207"},
			serverResp:  `{"status":"ok","cancelled_count":1}`,
			statusCode:  http.StatusOK,
			wantStatus:  "ok",
			wantPathSub: "/DispatchSession/vanadis-chunk-207/Cancel",
		},
		{
			name:        "cancel dispatch immediate returns ok",
			op:          "cancel_immediate",
			sessionKey:  "vanadis-chunk-imm",
			cancelReq:   queue.CancelDispatchRequest{ChunkID: "chunk-imm"},
			serverResp:  `{"status":"ok","cancelled_count":1}`,
			statusCode:  http.StatusOK,
			wantStatus:  "ok",
			wantPathSub: "/SessionManager/CancelDispatch",
		},
		{
			name:         "dispatch accepted requires id or session or chunk",
			op:           "dispatch_accepted",
			dispatchReq:  queue.BrainDispatchRequest{Source: "vanadis", Prompt: "missing all identifiers"},
			wantErrorSub: "dispatch id, session id, or chunk id is required",
		},
		{
			name:           "dispatch accepted returns error on transport failure",
			op:             "dispatch_accepted",
			sessionKey:     "disp-fail",
			dispatchReq:    queue.BrainDispatchRequest{ID: "d-fail", Source: "vanadis", SessionID: "disp-fail"},
			transportError: true,
			wantErrorSub:   "post restate request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDispatchOperationCase(t, tc)
		})
	}
}

func TestRestateIngressClient_AskVoice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		agentName      string
		deviceKey      string
		req            queue.VoiceSessionRequest
		serverResp     string
		statusCode     int
		transportError bool
		wantReply      string
		wantErrorSub   string
		wantPathSub    string
	}{
		{
			name:        "successful voice ask on aerial stack",
			agentName:   "aerial",
			deviceKey:   "kiosk-1",
			req:         queue.VoiceSessionRequest{Prompt: "weather", DeviceKey: "kiosk-1"},
			serverResp:  `{"reply":"it is sunny","conversation_id":"conv-1","device_key":"kiosk-1"}`,
			statusCode:  http.StatusOK,
			wantReply:   "it is sunny",
			wantPathSub: "/VoiceSession/kiosk-1/Ask",
		},
		{
			name:        "successful voice ask on miorine stack",
			agentName:   "miorine",
			deviceKey:   "kiosk-2",
			req:         queue.VoiceSessionRequest{Prompt: "status", DeviceKey: "kiosk-2"},
			serverResp:  `{"reply":"all good","conversation_id":"conv-2","device_key":"kiosk-2"}`,
			statusCode:  http.StatusOK,
			wantReply:   "all good",
			wantPathSub: "/VoiceSession-miorine/kiosk-2/Ask",
		},
		{
			name:         "voice ask backend failure returns error",
			agentName:    "aerial",
			deviceKey:    "kiosk-3",
			req:          queue.VoiceSessionRequest{Prompt: "bad", DeviceKey: "kiosk-3"},
			serverResp:   `{"error":"failed to generate speech"}`,
			statusCode:   http.StatusInternalServerError,
			wantErrorSub: "restate returned status 500",
			wantPathSub:  "/VoiceSession/kiosk-3/Ask",
		},
		{
			name:           "voice ask transport failure returns error",
			agentName:      "aerial",
			deviceKey:      "kiosk-4",
			req:            queue.VoiceSessionRequest{Prompt: "fail", DeviceKey: "kiosk-4"},
			transportError: true,
			wantErrorSub:   "post restate request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			lastPath := ""

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				lastPath = r.URL.EscapedPath()
				mu.Unlock()

				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.serverResp))
			}))
			defer ts.Close()

			httpClient := ts.Client()
			if tc.transportError {
				httpClient = &http.Client{
					Transport: &errRoundTripper{err: assert.AnError},
				}
			}
			client := NewRestateIngressClient(ts.URL, tc.agentName, httpClient)
			res, err := client.AskVoice(context.Background(), tc.deviceKey, tc.req)

			mu.Lock()
			defer mu.Unlock()
			assert.Contains(t, lastPath, tc.wantPathSub)

			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantReply, res.Reply)
		})
	}
}
