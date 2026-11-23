package restateclient

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/eri/pkg/llmcall"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
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

type newRestateLLMCase struct {
	name         string
	agent        string
	callerKey    string
	wantIdemKey  string
	cancelCaller bool
	nilClient    bool
	stubStatus   int
	stubBody     string
	wantResponse string
	wantErrSub   string
	wantPath     string
}

type llmIngressCapture struct {
	path    string
	idemKey string
	hasKey  bool
	req     llmcall.Request
}

func runNewRestateLLMCase(t *testing.T, tc newRestateLLMCase) {
	t.Helper()
	t.Parallel()
	release := make(chan struct{})
	seen := make(chan llmIngressCapture, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := llmIngressCapture{path: r.URL.Path, idemKey: r.Header.Get("Idempotency-Key")}
		_, c.hasKey = r.Header["Idempotency-Key"]
		if err := json.NewDecoder(r.Body).Decode(&c.req); err != nil {
			return
		}
		seen <- c
		if tc.cancelCaller {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		w.WriteHeader(tc.stubStatus)
		_, _ = w.Write([]byte(tc.stubBody))
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callCtx := ctx
	if tc.callerKey != "" {
		callCtx = runner.WithLLMCallerKey(ctx, tc.callerKey)
	}
	client := NewRestateIngressClient(ts.URL, tc.agent, ts.Client())
	if tc.nilClient {
		client = nil
	}
	done := make(chan struct{})
	var resp string
	var err error
	go func() {
		defer close(done)
		resp, err = NewRestateLLMFunc(client)(callCtx, "flash", "summarize")
	}()
	var got llmIngressCapture
	if tc.cancelCaller {
		got = <-seen
		cancel()
	}
	<-done
	if !tc.cancelCaller {
		// The stub records the request before it writes the response, so
		// a finished call has already published its capture (if any).
		select {
		case got = <-seen:
		default:
		}
	}

	if tc.wantErrSub != "" {
		require.ErrorContains(t, err, tc.wantErrSub)
	} else {
		require.NoError(t, err)
	}
	assert.Equal(t, tc.wantResponse, resp)
	assert.Equal(t, tc.wantIdemKey != "", got.hasKey, "Idempotency-Key presence")
	assert.Equal(t, tc.wantIdemKey, got.idemKey)
	assert.Equal(t, tc.wantPath, got.path)
	if tc.wantPath != "" {
		assert.Equal(t, llmcall.Request{Prompt: "summarize", Model: "flash", CallerKey: tc.callerKey}, got.req)
	}
}

// summarizeFlashHash is the hand-computed sha256 over the length-prefixed
// fields of {Prompt:"summarize", Model:"flash"} with no effort or attachments.
const summarizeFlashHash = "4e7fc0701974d8e7bd6db7cef77ab27b740e67b1ae590039580cad25dbd2ec07"

func TestNewRestateLLMFunc(t *testing.T) {
	t.Parallel()

	tests := []newRestateLLMCase{
		{name: "unkeyed call returns response", agent: "aerial", stubStatus: http.StatusOK, stubBody: `{"response":"title"}`, wantResponse: "title", wantPath: "/LLM/Complete"},
		{name: "classify caller key sent as idempotency key", agent: "aerial", callerKey: "classify:42:99", wantIdemKey: "llm:classify:42:99:" + summarizeFlashHash, stubStatus: http.StatusOK, stubBody: `{"response":"{}"}`, wantResponse: "{}", wantPath: "/LLM/Complete"},
		{name: "title caller key sent as idempotency key", agent: "aerial", callerKey: "title:99", wantIdemKey: "llm:title:99:" + summarizeFlashHash, stubStatus: http.StatusOK, stubBody: `{"response":"Cute Title"}`, wantResponse: "Cute Title", wantPath: "/LLM/Complete"},
		{name: "sibling agent service", agent: "miorine", stubStatus: http.StatusOK, stubBody: `{"response":"ok"}`, wantResponse: "ok", wantPath: "/LLM-miorine/Complete"},
		{name: "restate failure", agent: "aerial", stubStatus: http.StatusInternalServerError, stubBody: `{"message":"boom"}`, wantErrSub: "boom", wantPath: "/LLM/Complete"},
		{name: "cancelled caller returns promptly", agent: "aerial", cancelCaller: true, wantErrSub: "context canceled", wantPath: "/LLM/Complete"},
		{name: "oversized response rejected", agent: "aerial", stubStatus: http.StatusOK, stubBody: `{"response":"` + strings.Repeat("a", 1<<20) + `"}`, wantErrSub: "exceeds 1MiB limit", wantPath: "/LLM/Complete"},
		{name: "malformed response rejected", agent: "aerial", stubStatus: http.StatusOK, stubBody: `{"response":`, wantErrSub: "unmarshal restate llm response", wantPath: "/LLM/Complete"},
		{name: "nil client sends nothing", nilClient: true, wantErrSub: "restate ingress client is nil"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runNewRestateLLMCase(t, tc)
		})
	}
}

func TestActiveRestateIngressClient_GlobalLifecycleAndResolve(t *testing.T) {
	c1 := NewRestateIngressClient("http://restate-1.test", "aerial")
	c2 := NewRestateIngressClient("http://restate-2.test", "aerial")

	orig := SwapActiveRestateIngressClient(c1)
	defer StoreActiveRestateIngressClient(orig)

	assert.Equal(t, c1, ActiveRestateIngressClient())
	assert.Equal(t, c2, ResolveRestateClient(c2))
	assert.Equal(t, c1, ResolveRestateClient())
	assert.Equal(t, c1, ResolveRestateClient(nil))

	StoreActiveRestateIngressClient(c2)
	assert.Equal(t, c2, ActiveRestateIngressClient())
}
