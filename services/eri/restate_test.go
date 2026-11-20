package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func TestNewEriRestateServer(t *testing.T) {
	t.Parallel()

	srv := NewEriRestateServer(nil, "aerial")
	require.NotNil(t, srv)
	h, err := srv.Handler()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
	req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v2+json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ThreadSession")
	assert.Contains(t, rec.Body.String(), "DispatchSession")
	assert.Contains(t, rec.Body.String(), "VoiceSession")
	assert.Contains(t, rec.Body.String(), "SessionManager")
}

func TestReconcileRestateServiceTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		agent          string
		adminURL       string
		statusCode     int
		returnBody     string
		transportError bool
		wantErrorSub   string
		wantCallCount  int
	}{
		{
			name:          "successful timeout reconciliation for all 3 services",
			agent:         "aerial",
			statusCode:    http.StatusOK,
			returnBody:    `{"status":"ok"}`,
			wantCallCount: 3,
		},
		{
			name:          "successful timeout reconciliation with namespaced agent",
			agent:         "miorine",
			statusCode:    http.StatusOK,
			returnBody:    `{"status":"ok"}`,
			wantCallCount: 3,
		},
		{
			name:          "admin returns 500 error",
			agent:         "aerial",
			statusCode:    http.StatusInternalServerError,
			returnBody:    `{"error":"internal"}`,
			wantErrorSub:  "failed with status 500",
			wantCallCount: 1,
		},
		{
			name:          "admin returns 404 error",
			agent:         "aerial",
			statusCode:    http.StatusNotFound,
			returnBody:    `{"error":"not found"}`,
			wantErrorSub:  "failed with status 404",
			wantCallCount: 1,
		},
		{
			name:           "http transport error",
			agent:          "aerial",
			transportError: true,
			wantErrorSub:   "patch service",
			wantCallCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			recordedPaths := make([]string, 0)
			recordedBodies := make([]string, 0)
			recordedMethods := make([]string, 0)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				recordedPaths = append(recordedPaths, r.URL.Path)
				recordedMethods = append(recordedMethods, r.Method)
				bodyBytes, _ := io.ReadAll(r.Body)
				recordedBodies = append(recordedBodies, string(bodyBytes))
				mu.Unlock()

				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.returnBody))
			}))
			defer ts.Close()

			targetURL := ts.URL
			if tc.adminURL != "" {
				targetURL = tc.adminURL
			}
			client := ts.Client()
			if tc.transportError {
				client = &http.Client{
					Transport: &errRoundTripper{err: assert.AnError},
				}
			}

			err := ReconcileRestateServiceTimeouts(context.Background(), targetURL, tc.agent, client)
			if tc.wantErrorSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrorSub)
				return
			}

			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tc.wantCallCount, len(recordedPaths))
			for i := range recordedPaths {
				assert.Equal(t, http.MethodPatch, recordedMethods[i])
				var p serviceTimeoutPayload
				uErr := json.Unmarshal([]byte(recordedBodies[i]), &p)
				require.NoError(t, uErr)
				assert.Equal(t, "1h", p.InactivityTimeout)
				assert.Equal(t, "1h", p.AbortTimeout)
			}
		})
	}
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
