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
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type remoteQuotaCase struct {
	name           string
	statusCode     int
	responseBody   string
	transportError bool
	wantGroups     int
	wantFirstName  string
	wantUpdatedAt  string
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
	snap := p.LatestQuotaSnapshot()
	assert.Len(t, snap.Groups, tc.wantGroups)
	if tc.wantGroups > 0 {
		assert.Equal(t, tc.wantFirstName, snap.Groups[0].Name)
	}
	if tc.wantUpdatedAt == "" {
		assert.True(t, snap.UpdatedAt.IsZero())
	} else {
		assert.Equal(t, tc.wantUpdatedAt, snap.UpdatedAt.UTC().Format(time.RFC3339))
	}

	fnSnap := p.ProviderFunc()()
	assert.Len(t, fnSnap.Groups, tc.wantGroups)
	assert.Equal(t, snap.UpdatedAt, fnSnap.UpdatedAt)
}

func TestRemoteQuotaProvider(t *testing.T) {
	t.Parallel()

	tests := []remoteQuotaCase{
		{
			name:          "successful quota retrieval",
			statusCode:    http.StatusOK,
			responseBody:  `{"groups":[{"name":"Claude","buckets":[{"id":"claude-5h","window":"5h","remaining_fraction":0.8}]}],"updated_at":"2026-11-21T10:30:00Z"}`,
			wantGroups:    1,
			wantFirstName: "Claude",
			wantUpdatedAt: "2026-11-21T10:30:00Z",
		},
		{
			name:          "zero updated_at",
			statusCode:    http.StatusOK,
			responseBody:  `{"groups":[{"name":"Claude","buckets":[]}],"updated_at":"0001-01-01T00:00:00Z"}`,
			wantGroups:    1,
			wantFirstName: "Claude",
		},
		{
			name:         "legacy bare array body",
			statusCode:   http.StatusOK,
			responseBody: `[{"name":"Claude","buckets":[]}]`,
			wantGroups:   0,
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
	assert.Equal(t, metrics.QuotaSnapshot{}, nilProvider.LatestQuotaSnapshot())
	assert.Equal(t, metrics.QuotaSnapshot{}, nilProvider.ProviderFunc()())

	defaultProvider := NewRemoteQuotaProvider("")
	assert.Equal(t, "http://eri-restate-health:4028", defaultProvider.eriRestateURL)

	malformedProvider := NewRemoteQuotaProvider("http://bad\x7fhost")
	assert.Equal(t, metrics.QuotaSnapshot{}, malformedProvider.LatestQuotaSnapshot())
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
	name             string
	op               string
	threadID         string
	enqueueReq       queue.ThreadEnqueueRequest
	cancelReq        queue.CancelTurnRequest
	rotateReq        queue.RotateSessionRequest
	serverResp       string
	statusCode       int
	transportError   bool
	nilClient        bool
	emptyIngressURL  bool
	customIngressURL string
	wantStatus       string
	wantErrorSub     string
	wantPathSub      string
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
	if tc.emptyIngressURL {
		httpClient = &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				lastPath = r.URL.String()
				mu.Unlock()
				return &http.Response{
					StatusCode: tc.statusCode,
					Body:       io.NopCloser(strings.NewReader(tc.serverResp)),
					Header:     make(http.Header),
				}, nil
			}),
		}
	}
	ingressURL := ts.URL
	if tc.emptyIngressURL {
		ingressURL = ""
	} else if tc.customIngressURL != "" {
		ingressURL = tc.customIngressURL
	}
	client := NewRestateIngressClient(ingressURL, "aerial", httpClient)
	if tc.nilClient {
		client.httpClient = nil
	}

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
		{
			name:            "empty ingress url defaults to restate.aerial:8080",
			op:              "enqueue",
			threadID:        "th-108",
			enqueueReq:      queue.ThreadEnqueueRequest{Prompt: "default url"},
			emptyIngressURL: true,
			serverResp:      `{"status":"accepted","message_id":"msg-108"}`,
			statusCode:      http.StatusOK,
			wantStatus:      "accepted",
			wantPathSub:     "http://restate.aerial:8080/ThreadSession/th-108/Enqueue",
		},
		{
			name:         "nil client returns error",
			op:           "enqueue",
			threadID:     "th-109",
			enqueueReq:   queue.ThreadEnqueueRequest{Prompt: "nil client"},
			nilClient:    true,
			wantErrorSub: "restate ingress client is nil",
		},
		{
			name:             "invalid ingress url returns request creation error",
			op:               "enqueue",
			threadID:         "th-110",
			enqueueReq:       queue.ThreadEnqueueRequest{Prompt: "bad url"},
			customIngressURL: "http://\x7f",
			wantErrorSub:     "post restate request",
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
	dispatchReq    eriwire.BrainDispatchRequest
	cancelReq      queue.CancelDispatchRequest
	serverResp     string
	statusCode     int
	transportError bool
	wantStatus     string
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
			dispatchReq: eriwire.BrainDispatchRequest{ID: "d-201", Source: "vanadis"},
			serverResp:  `{"status":"accepted","dispatch_id":"d-201"}`,
			statusCode:  http.StatusOK,
			wantStatus:  "accepted",
			wantPathSub: "/DispatchSession/disp-201/Enqueue",
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
			name:           "enqueue dispatch returns error on transport failure",
			op:             "enqueue",
			sessionKey:     "disp-fail",
			dispatchReq:    eriwire.BrainDispatchRequest{ID: "d-fail", Source: "vanadis", SessionID: "disp-fail"},
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
			name:        "successful voice ask on agent-alpha stack",
			agentName:   "agent-alpha",
			deviceKey:   "kiosk-2",
			req:         queue.VoiceSessionRequest{Prompt: "status", DeviceKey: "kiosk-2"},
			serverResp:  `{"reply":"all good","conversation_id":"conv-2","device_key":"kiosk-2"}`,
			statusCode:  http.StatusOK,
			wantReply:   "all good",
			wantPathSub: "/VoiceSession-agent-alpha/kiosk-2/Ask",
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

func runWithMockRestate(statusCode int, serverResp string, fn func()) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(serverResp))
	}))
	defer ts.Close()

	client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := activeRestateIngressClient.Swap(client)
	defer activeRestateIngressClient.Store(orig)

	fn()
}

func TestHandlePrompt_RestateCutover(t *testing.T) {
	tests := []struct {
		name       string
		serverResp string
		statusCode int
		reqBody    string
		wantCode   int
		wantSubstr string
	}{
		{
			name:       "accepted via restate",
			serverResp: `{"status":"accepted","thread_id":"123456789012345678","message_id":"msg-1"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"prompt":"hello world","channel_id":"123456789012345678","message_id":"msg-1"}`,
			wantCode:   http.StatusAccepted,
			wantSubstr: `"status":"accepted"`,
		},
		{
			name:       "skipped active via restate",
			serverResp: `{"status":"skipped_active","thread_id":"123456789012345678"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"prompt":"hello again","channel_id":"123456789012345678","skip_if_active":true}`,
			wantCode:   http.StatusOK,
			wantSubstr: `"status":"skipped_active"`,
		},
		{
			name:       "restate error",
			serverResp: `{"error":"restate failure"}`,
			statusCode: http.StatusInternalServerError,
			reqBody:    `{"prompt":"hello error","channel_id":"123456789012345678"}`,
			wantCode:   http.StatusInternalServerError,
			wantSubstr: "Failed to enqueue prompt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWithMockRestate(tc.statusCode, tc.serverResp, func() {
				handler := handlePrompt(nil, nil)
				req := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(tc.reqBody))
				rec := httptest.NewRecorder()
				handler(rec, req)

				assert.Equal(t, tc.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			})
		})
	}
}

func TestHandleDispatch_RestateCutover(t *testing.T) {
	tests := []struct {
		name       string
		serverResp string
		statusCode int
		reqBody    string
		wantCode   int
		wantSubstr string
	}{
		{
			name:       "accepted via restate",
			serverResp: `{"status":"accepted","dispatch_id":"disp-1","session_id":"vanadis-1"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"source":"vanadis","prompt":"run chunk","vanadis_chunk_id":"1","id":"disp-1"}`,
			wantCode:   http.StatusAccepted,
			wantSubstr: `"status":"accepted"`,
		},
		{
			name:       "skipped active via restate",
			serverResp: `{"status":"skipped_active","dispatch_id":"disp-2","session_id":"vanadis-2"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"source":"vanadis","prompt":"run chunk 2","vanadis_chunk_id":"2","id":"disp-2","skip_if_active":true}`,
			wantCode:   http.StatusOK,
			wantSubstr: `"status":"skipped_active"`,
		},
		{
			name:       "already exists via restate",
			serverResp: `{"status":"already_exists","dispatch_id":"disp-3","session_id":"vanadis-3"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"source":"vanadis","prompt":"run chunk 3","vanadis_chunk_id":"3","id":"disp-3"}`,
			wantCode:   http.StatusOK,
			wantSubstr: `"status":"already_exists"`,
		},
		{
			name:       "restate error",
			serverResp: `{"error":"internal error"}`,
			statusCode: http.StatusInternalServerError,
			reqBody:    `{"source":"vanadis","prompt":"run chunk 4","vanadis_chunk_id":"4","id":"disp-4"}`,
			wantCode:   http.StatusInternalServerError,
			wantSubstr: "Failed to enqueue dispatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWithMockRestate(tc.statusCode, tc.serverResp, func() {
				handler := handleDispatch(nil, nil)
				req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(tc.reqBody))
				rec := httptest.NewRecorder()
				handler(rec, req)

				assert.Equal(t, tc.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			})
		})
	}
}

func TestHandleCancelDispatch_RestateCutover(t *testing.T) {
	tests := []struct {
		name       string
		serverResp string
		statusCode int
		reqBody    string
		wantCode   int
		wantSubstr string
	}{
		{
			name:       "cancel success via restate",
			serverResp: `{"status":"ok","cancelled_count":1,"cancelled_ids":["disp-1"]}`,
			statusCode: http.StatusOK,
			reqBody:    `{"dispatch_id":"disp-1","reason":"test cancel"}`,
			wantCode:   http.StatusOK,
			wantSubstr: `"cancelled_count":1`,
		},
		{
			name:       "cancel by build_id via restate",
			serverResp: `{"status":"ok","cancelled_count":2,"cancelled_ids":["disp-1","disp-2"]}`,
			statusCode: http.StatusOK,
			reqBody:    `{"build_id":"build-99","reason":"test build cancel"}`,
			wantCode:   http.StatusOK,
			wantSubstr: `"cancelled_count":2`,
		},
		{
			name:       "cancel error via restate",
			serverResp: `{"error":"restate failure"}`,
			statusCode: http.StatusInternalServerError,
			reqBody:    `{"dispatch_id":"disp-2"}`,
			wantCode:   http.StatusInternalServerError,
			wantSubstr: "Failed to cancel dispatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWithMockRestate(tc.statusCode, tc.serverResp, func() {
				handler := handleCancelDispatch(nil)
				req := httptest.NewRequest(http.MethodPost, "/dispatch/cancel", strings.NewReader(tc.reqBody))
				rec := httptest.NewRecorder()
				handler(rec, req)

				assert.Equal(t, tc.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			})
		})
	}
}

func TestHandleInternalDiscordInteraction_RestateCutover(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		statusCode  int
		serverResp  string
		wantCode    int
		wantPathSub string
		wantSubstr  string
	}{
		{
			name:        "interrupt application command",
			body:        `{"id":"int-1","type":2,"channel_id":"123456789012345678","user":{"id":"admin-1","username":"alice"},"data":{"name":"interrupt"}}`,
			statusCode:  http.StatusOK,
			serverResp:  `{"status":"ok"}`,
			wantCode:    http.StatusOK,
			wantPathSub: "/ThreadSession/123456789012345678/CancelTurn",
			wantSubstr:  `"status":"ok"`,
		},
		{
			name:        "rotate application command",
			body:        `{"id":"int-2","type":2,"channel_id":"123456789012345678","user":{"id":"admin-1","username":"alice"},"data":{"name":"rotate"}}`,
			statusCode:  http.StatusOK,
			serverResp:  `{"status":"ok"}`,
			wantCode:    http.StatusOK,
			wantPathSub: "/ThreadSession/123456789012345678/RotateSession",
			wantSubstr:  `"status":"ok"`,
		},
		{
			name:        "stop_turn message component",
			body:        `{"id":"int-3","type":3,"channel_id":"123456789012345678","user":{"id":"admin-1","username":"alice"},"data":{"custom_id":"stop_turn:msg-1"}}`,
			statusCode:  http.StatusOK,
			serverResp:  `{"status":"ok"}`,
			wantCode:    http.StatusOK,
			wantPathSub: "/ThreadSession/123456789012345678/CancelTurn",
			wantSubstr:  `"status":"ok"`,
		},
		{
			name:        "interrupt failure propagates 500",
			body:        `{"id":"int-4","type":2,"channel_id":"123456789012345678","user":{"id":"admin-1","username":"alice"},"data":{"name":"interrupt"}}`,
			statusCode:  http.StatusInternalServerError,
			serverResp:  `{"error":"restate down"}`,
			wantCode:    http.StatusInternalServerError,
			wantPathSub: "/ThreadSession/123456789012345678/CancelTurn",
			wantSubstr:  "Restate control interaction failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

			client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			orig := activeRestateIngressClient.Swap(client)
			defer activeRestateIngressClient.Store(orig)

			handler := handleInternalDiscordInteraction(nil, nil)
			req := httptest.NewRequest(http.MethodPost, "/internal/discord/interaction", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			mu.Lock()
			defer mu.Unlock()
			assert.Contains(t, lastPath, tc.wantPathSub)
		})
	}
}

func TestHandleVoiceAsk_RestateCutover(t *testing.T) {
	tests := []struct {
		name       string
		serverResp string
		statusCode int
		reqBody    string
		wantCode   int
		wantReply  string
	}{
		{
			name:       "voice ask json success",
			serverResp: `{"reply":"Restate voice reply","conversation_id":"conv-voice-1","device_key":"kiosk-1"}`,
			statusCode: http.StatusOK,
			reqBody:    `{"prompt":"hello voice","node_id":"kiosk-1"}`,
			wantCode:   http.StatusOK,
			wantReply:  "Restate voice reply",
		},
		{
			name:       "voice ask json error",
			serverResp: `{"error":"speech model timeout"}`,
			statusCode: http.StatusInternalServerError,
			reqBody:    `{"prompt":"bad voice","node_id":"kiosk-1"}`,
			wantCode:   http.StatusInternalServerError,
			wantReply:  "speech model timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWithMockRestate(tc.statusCode, tc.serverResp, func() {
				handler := handleVoiceAsk(nil)
				req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(tc.reqBody))
				rec := httptest.NewRecorder()
				handler(rec, req)

				assert.Equal(t, tc.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tc.wantReply)
			})
		})
	}
}

func TestHandleVoiceAskSSE_RestateCutover(t *testing.T) {
	bus := newSyncVoiceBus()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req queue.VoiceSessionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		subj := "voice.stream." + req.TurnID
		_ = bus.Publish(r.Context(), subj, "1", queue.VoiceStreamEvent{Event: "status", Status: "thinking"})
		_ = bus.Publish(r.Context(), subj, "2", queue.VoiceStreamEvent{Event: "sentence", Text: "Hello from SSE."})
		_ = bus.Publish(r.Context(), subj, "3", queue.VoiceStreamEvent{Event: "reply", Reply: "Hello from SSE.", ConversationID: "conv-123"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"reply":"Hello from SSE.","conversation_id":"conv-123","device_key":"kiosk-sse"}`))
	}))
	defer ts.Close()

	client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := activeRestateIngressClient.Swap(client)
	defer activeRestateIngressClient.Store(orig)

	handler := handleVoiceAsk(queue.New(newTestConfig(), queue.WorkerPoolConfig{EventBus: bus}))
	req := httptest.NewRequest(http.MethodPost, "/voice/ask?stream=true", strings.NewReader(`{"prompt":"hello","node_id":"kiosk-sse"}`))
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()
	handler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "event: status")
	assert.Contains(t, body, "event: sentence")
	assert.Contains(t, body, "event: reply")
	assert.Contains(t, body, "event: done")
}

func TestSetupBrainMuxWithEmbedder_RestateLLM(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/internal/llm", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"response":"remote llm result"}`))
	}))
	defer ts.Close()

	client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := activeRestateIngressClient.Swap(client)
	defer activeRestateIngressClient.Store(orig)

	t.Setenv("ERI_RESTATE_URL", ts.URL)

	mux := SetupBrainMuxWithEmbedder(nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/internal/llm", strings.NewReader(`{"prompt":"summarize"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "remote llm result")
}

func TestRestateIngressClient_CancelDispatchImmediate(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/SessionManager/CancelDispatch", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","cancelled_count":1}`))
	}))
	defer ts.Close()

	c := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	resp, err := c.CancelDispatchImmediate(context.Background(), queue.CancelDispatchControlRequest{BuildID: "b-1"})
	assert.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)
}

func TestHandleVoiceAskSSE_RestateNoBusWithPool(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		serverResp string
		wantEvent  string
		wantSubstr string
	}{
		{
			name:       "restate no-bus ask voice success",
			statusCode: http.StatusOK,
			serverResp: `{"reply":"restate non-streaming reply","conversation_id":"conv-nobus-1","device_key":"kiosk-nobus"}`,
			wantEvent:  "event: reply",
			wantSubstr: "restate non-streaming reply",
		},
		{
			name:       "restate no-bus ask voice error",
			statusCode: http.StatusInternalServerError,
			serverResp: `{"error":"restate voice failure"}`,
			wantEvent:  "event: error",
			wantSubstr: "restate voice failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.serverResp))
			}))
			defer ts.Close()

			client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			origClient := activeRestateIngressClient.Swap(client)
			defer activeRestateIngressClient.Store(origClient)

			cfg := newTestConfig()
			mockPool := queue.New(cfg, queue.WorkerPoolConfig{})
			handler := handleVoiceAsk(mockPool)

			req := httptest.NewRequest(http.MethodPost, "/voice/ask?stream=true", strings.NewReader(`{"prompt":"hello","node_id":"kiosk-nobus"}`))
			req.Header.Set("Accept", "text/event-stream")
			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			assert.Contains(t, body, tc.wantEvent)
			assert.Contains(t, body, tc.wantSubstr)
			assert.Contains(t, body, "event: done")
		})
	}
}

func TestHandleInteractionViaRestate_NilAndUnhandledInteractions(t *testing.T) {
	tests := []struct {
		name        string
		nilClient   bool
		serverError bool
		interaction *discordgo.Interaction
	}{
		{name: "nil client", nilClient: true, interaction: &discordgo.Interaction{ChannelID: "123"}},
		{name: "nil interaction", nilClient: false, interaction: nil},
		{name: "empty channel id", nilClient: false, interaction: &discordgo.Interaction{ChannelID: " "}},
		{name: "unhandled command name", nilClient: false, interaction: &discordgo.Interaction{ChannelID: "123", Type: discordgo.InteractionApplicationCommand, Data: discordgo.ApplicationCommandInteractionData{Name: "unknown"}}},
		{name: "unhandled component custom id", nilClient: false, interaction: &discordgo.Interaction{ChannelID: "123", Type: discordgo.InteractionMessageComponent, Data: discordgo.MessageComponentInteractionData{CustomID: "ignore_me"}}},
		{name: "interrupt command with server error", nilClient: false, serverError: true, interaction: &discordgo.Interaction{ChannelID: "123", Type: discordgo.InteractionApplicationCommand, Data: discordgo.ApplicationCommandInteractionData{Name: "interrupt"}}},
		{name: "rotate command with server error", nilClient: false, serverError: true, interaction: &discordgo.Interaction{ChannelID: "123", Type: discordgo.InteractionApplicationCommand, Data: discordgo.ApplicationCommandInteractionData{Name: "rotate"}}},
		{name: "stop_turn component with server error", nilClient: false, serverError: true, interaction: &discordgo.Interaction{ChannelID: "123", Type: discordgo.InteractionMessageComponent, Data: discordgo.MessageComponentInteractionData{CustomID: "stop_turn:msg-1"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var client *RestateIngressClient
			if !tc.nilClient {
				if tc.serverError {
					ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`{"error":"server error"}`))
					}))
					defer ts.Close()
					client = NewRestateIngressClient(ts.URL, "aerial", ts.Client())
				} else {
					client = NewRestateIngressClient("http://localhost:8080", "aerial")
				}
			}
			handled, err := handleInteractionViaRestate(context.Background(), client, tc.interaction)
			if tc.serverError {
				assert.True(t, handled)
				assert.Error(t, err)
			} else {
				assert.False(t, handled)
				assert.NoError(t, err)
			}
		})
	}
}

func TestHandleVoiceAskSSE_RestateFailures(t *testing.T) {
	tests := []struct {
		name       string
		serverResp string
		statusCode int
		publishErr bool
		wantSubstr string
	}{
		{
			name:       "voice ask restate error emits sse error",
			serverResp: `{"error":"speech model crash"}`,
			statusCode: http.StatusInternalServerError,
			wantSubstr: "event: error",
		},
		{
			name:       "voice ask bus error event emits sse error",
			serverResp: `{"reply":"ok"}`,
			statusCode: http.StatusOK,
			publishErr: true,
			wantSubstr: "event: error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bus := newSyncVoiceBus()

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.publishErr {
					var req queue.VoiceSessionRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					_ = bus.Publish(r.Context(), "voice.stream."+req.TurnID, "err-1", queue.VoiceStreamEvent{
						Event: "error",
						Error: "bus stream failed",
					})
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.serverResp))
			}))
			defer ts.Close()

			client := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			orig := activeRestateIngressClient.Swap(client)
			defer activeRestateIngressClient.Store(orig)

			handler := handleVoiceAsk(queue.New(newTestConfig(), queue.WorkerPoolConfig{EventBus: bus}))
			req := httptest.NewRequest(http.MethodPost, "/voice/ask?stream=true", strings.NewReader(`{"prompt":"hello","node_id":"kiosk-err"}`))
			req.Header.Set("Accept", "text/event-stream")
			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			assert.Contains(t, rec.Body.String(), "event: done")
		})
	}
}

func TestHandlePromptViaRestate_IdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		skipIfActive       bool
		channelID          string
		msgID              string
		wantIdempotencyKey string
	}{
		{
			name:               "idempotency key populated when skip-if-active is false",
			skipIfActive:       false,
			channelID:          "ch-100",
			msgID:              "msg-200",
			wantIdempotencyKey: "prompt:ch-100:msg-200",
		},
		{
			name:               "idempotency key populated when skip-if-active is true",
			skipIfActive:       true,
			channelID:          "ch-100",
			msgID:              "msg-200",
			wantIdempotencyKey: "prompt:ch-100:msg-200",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedKey string
			var headerPresent bool
			httpClient := &http.Client{
				Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					capturedKey = r.Header.Get("Idempotency-Key")
					_, headerPresent = r.Header["Idempotency-Key"]
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`{"status":"accepted"}`)),
						Header:     make(http.Header),
					}, nil
				}),
			}

			client := NewRestateIngressClient("http://restate.test", "aerial", httpClient)
			status, err := handlePromptViaRestate(context.Background(), client, PromptRequest{
				Prompt:       "hello world",
				SkipIfActive: tc.skipIfActive,
			}, tc.channelID, tc.msgID)
			require.NoError(t, err)
			assert.Equal(t, "accepted", status)
			assert.True(t, headerPresent)
			assert.Equal(t, tc.wantIdempotencyKey, capturedKey)
		})
	}
}

func TestHandleDispatchViaRestate_IdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		skipIfActive       bool
		sessionKey         string
		dispatchID         string
		wantIdempotencyKey string
	}{
		{
			name:               "idempotency key populated with dispatch id when skip-if-active is false",
			skipIfActive:       false,
			sessionKey:         "sess-100",
			dispatchID:         "disp-200",
			wantIdempotencyKey: "disp-200",
		},
		{
			name:               "idempotency key populated with dispatch id when skip-if-active is true",
			skipIfActive:       true,
			sessionKey:         "sess-100",
			dispatchID:         "disp-200",
			wantIdempotencyKey: "disp-200",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var receivedKey string
			var hasHeader bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedKey = r.Header.Get("Idempotency-Key")
				_, hasHeader = r.Header["Idempotency-Key"]
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			}))
			defer ts.Close()

			ingress := NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			res, err := handleDispatchViaRestate(context.Background(), ingress, DispatchRequest{
				Prompt:       "run task",
				SkipIfActive: tc.skipIfActive,
			}, tc.sessionKey, tc.dispatchID)
			require.NoError(t, err)
			assert.Equal(t, "accepted", res)
			assert.True(t, hasHeader)
			assert.Equal(t, tc.wantIdempotencyKey, receivedKey)
		})
	}
}
