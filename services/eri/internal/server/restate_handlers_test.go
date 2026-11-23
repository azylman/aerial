package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/internal/restateclient"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

func runWithMockRestate(statusCode int, serverResp string, fn func()) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(serverResp))
	}))
	defer ts.Close()

	client := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := restateclient.SwapActiveRestateIngressClient(client)
	defer restateclient.StoreActiveRestateIngressClient(orig)

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
				handler := handlePrompt(nil)
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
				handler := handleDispatch(nil)
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
				handler := handleCancelDispatch()
				req := httptest.NewRequest(http.MethodPost, "/dispatch/cancel", strings.NewReader(tc.reqBody))
				rec := httptest.NewRecorder()
				handler(rec, req)

				assert.Equal(t, tc.wantCode, rec.Code)
				assert.Contains(t, rec.Body.String(), tc.wantSubstr)
			})
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return
		}
		subj := "voice.stream." + req.TurnID
		if err := bus.Publish(r.Context(), subj, "1", queue.VoiceStreamEvent{Event: "status", Status: "thinking"}); err != nil {
			return
		}
		if err := bus.Publish(r.Context(), subj, "2", queue.VoiceStreamEvent{Event: "sentence", Text: "Hello from SSE."}); err != nil {
			return
		}
		if err := bus.Publish(r.Context(), subj, "3", queue.VoiceStreamEvent{Event: "reply", Reply: "Hello from SSE.", ConversationID: "conv-123"}); err != nil {
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"reply":"Hello from SSE.","conversation_id":"conv-123","device_key":"kiosk-sse"}`))
	}))
	defer ts.Close()

	client := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := restateclient.SwapActiveRestateIngressClient(client)
	defer restateclient.StoreActiveRestateIngressClient(orig)

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

func TestSetupBrainMux_RestateLLM(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/LLM/Complete", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"response":"remote llm result"}`))
	}))
	defer ts.Close()

	client := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
	orig := restateclient.SwapActiveRestateIngressClient(client)
	defer restateclient.StoreActiveRestateIngressClient(orig)

	mux := SetupBrainMux(nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/internal/llm", strings.NewReader(`{"prompt":"summarize"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "remote llm result")
}

func TestHandleVoiceAskSSE_RestateNoBusWithPool(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		serverResp    string
		cancelRequest bool
		failWrites    bool
		wantSSE       []string
		wantSubstr    string
	}{
		{
			name:       "restate no-bus ask voice success",
			statusCode: http.StatusOK,
			serverResp: `{"reply":"restate non-streaming reply","conversation_id":"conv-nobus-1","device_key":"kiosk-nobus"}`,
			wantSSE:    []string{"reply", "done"},
			wantSubstr: "restate non-streaming reply",
		},
		{
			name:       "restate no-bus ask voice error",
			statusCode: http.StatusInternalServerError,
			serverResp: `{"error":"restate voice failure"}`,
			wantSSE:    []string{"error", "done"},
			wantSubstr: "restate voice failure",
		},
		{
			name:          "client gone before ask emits nothing",
			statusCode:    http.StatusOK,
			serverResp:    `{"reply":"unseen","conversation_id":"conv-gone","device_key":"kiosk-nobus"}`,
			cancelRequest: true,
		},
		{
			name:       "failed chunk writes emit nothing",
			statusCode: http.StatusOK,
			serverResp: `{"reply":"unwritable","conversation_id":"conv-unwritable","device_key":"kiosk-nobus"}`,
			failWrites: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.serverResp))
			}))
			defer ts.Close()

			client := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			origClient := restateclient.SwapActiveRestateIngressClient(client)
			defer restateclient.StoreActiveRestateIngressClient(origClient)

			cfg := newTestConfig()
			mockPool := queue.New(cfg, queue.WorkerPoolConfig{})
			handler := handleVoiceAsk(mockPool)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelRequest {
				cancel()
			}
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/voice/ask?stream=true", strings.NewReader(`{"prompt":"hello","node_id":"kiosk-nobus"}`))
			req.Header.Set("Accept", "text/event-stream")
			rec := httptest.NewRecorder()
			var w http.ResponseWriter = rec
			if tc.failWrites {
				w = errWriteRecorder{rec}
			}
			handler(w, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			var gotSSE []string
			for line := range strings.SplitSeq(body, "\n") {
				if ev, ok := strings.CutPrefix(line, "event: "); ok {
					gotSSE = append(gotSSE, ev)
				}
			}
			assert.Equal(t, tc.wantSSE, gotSSE)
			assert.Contains(t, body, tc.wantSubstr)
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
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						return
					}
					if err := bus.Publish(r.Context(), "voice.stream."+req.TurnID, "err-1", queue.VoiceStreamEvent{
						Event: "error",
						Error: "bus stream failed",
					}); err != nil {
						return
					}
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.serverResp))
			}))
			defer ts.Close()

			client := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			orig := restateclient.SwapActiveRestateIngressClient(client)
			defer restateclient.StoreActiveRestateIngressClient(orig)

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
		traceparent        string
		skipIfActive       bool
		channelID          string
		recordID           string
		keyID              string
		wantRecordID       string
		title              string
		serverResp         string
		wantIdempotencyKey string
		wantTraceparent    string
		wantTitle          string
		wantResp           queue.ThreadEnqueueResponse
	}{
		{
			name:               "key names caller channel and message, not the thread Enqueue creates",
			channelID:          "ch-parent",
			recordID:           "msg-300",
			keyID:              "msg-300",
			wantRecordID:       "msg-300",
			title:              " Build notes ",
			serverResp:         `{"status":"accepted","thread_id":"th-created","message_id":"msg-300"}`,
			wantIdempotencyKey: "prompt:ch-parent:msg-300",
			wantTitle:          "Build notes",
			wantResp:           queue.ThreadEnqueueResponse{Status: "accepted", ThreadID: "th-created", MessageID: "msg-300"},
		},
		{
			name:               "derived key leaves message id for Enqueue to mint",
			channelID:          "ch-parent",
			keyID:              "derived-1",
			wantIdempotencyKey: "prompt:ch-parent:derived-1",
			wantResp:           queue.ThreadEnqueueResponse{Status: "accepted"},
		},
		{
			name:               "idempotency key populated when skip-if-active is false",
			skipIfActive:       false,
			channelID:          "ch-100",
			recordID:           "msg-200",
			keyID:              "msg-200",
			wantRecordID:       "msg-200",
			wantIdempotencyKey: "prompt:ch-100:msg-200",
			wantTraceparent:    "",
			wantResp:           queue.ThreadEnqueueResponse{Status: "accepted"},
		},
		{
			name:               "idempotency key populated when skip-if-active is true",
			skipIfActive:       true,
			channelID:          "ch-100",
			recordID:           "msg-200",
			keyID:              "msg-200",
			wantRecordID:       "msg-200",
			wantIdempotencyKey: "prompt:ch-100:msg-200",
			wantTraceparent:    "",
			wantResp:           queue.ThreadEnqueueResponse{Status: "accepted"},
		},
		{
			name:               "trace context propagated when active span in ctx",
			traceparent:        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			skipIfActive:       false,
			channelID:          "ch-100",
			recordID:           "msg-200",
			keyID:              "msg-200",
			wantRecordID:       "msg-200",
			wantIdempotencyKey: "prompt:ch-100:msg-200",
			wantTraceparent:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			wantResp:           queue.ThreadEnqueueResponse{Status: "accepted"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedKey, capturedPath string
			var headerPresent bool
			var capturedBody []byte
			serverResp := tc.serverResp
			if serverResp == "" {
				serverResp = `{"status":"accepted"}`
			}
			httpClient := &http.Client{
				Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
					capturedKey = r.Header.Get("Idempotency-Key")
					_, headerPresent = r.Header["Idempotency-Key"]
					capturedPath = r.URL.Path
					var err error
					capturedBody, err = io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(serverResp)),
						Header:     make(http.Header),
					}, nil
				}),
			}

			client := restateclient.NewRestateIngressClient("http://restate.test", "aerial", httpClient)
			ctx := context.Background()
			if tc.traceparent != "" {
				ctx = tracing.ContextWithTraceparent(ctx, tc.traceparent)
			}
			resp, err := handlePromptViaRestate(ctx, client, PromptRequest{
				Prompt:       "hello world",
				Title:        tc.title,
				SkipIfActive: tc.skipIfActive,
			}, tc.channelID, tc.recordID, tc.keyID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantResp, resp)
			assert.True(t, headerPresent)
			assert.Equal(t, tc.wantIdempotencyKey, capturedKey)
			assert.Equal(t, "/ThreadSession/"+tc.channelID+"/Enqueue", capturedPath)

			var enqueuedReq queue.ThreadEnqueueRequest
			require.NoError(t, json.Unmarshal(capturedBody, &enqueuedReq))
			assert.Equal(t, tc.wantTraceparent, enqueuedReq.Traceparent)
			assert.Equal(t, tc.wantTitle, enqueuedReq.Title)
			assert.Equal(t, tc.channelID, enqueuedReq.ChannelID)
			assert.Equal(t, tc.wantRecordID, enqueuedReq.MessageID)
		})
	}
}

func TestHandleDispatchViaRestate_IdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		traceparent        string
		skipIfActive       bool
		sessionKey         string
		recordID           string
		keyID              string
		wantRecordID       string
		wantIdempotencyKey string
		wantTraceparent    string
	}{
		{
			name:               "derived key leaves dispatch id for Enqueue to mint",
			sessionKey:         "sess-100",
			keyID:              "derived-2",
			wantIdempotencyKey: "derived-2",
		},
		{
			name:               "idempotency key populated with dispatch id when skip-if-active is false",
			skipIfActive:       false,
			sessionKey:         "sess-100",
			recordID:           "disp-200",
			keyID:              "disp-200",
			wantRecordID:       "disp-200",
			wantIdempotencyKey: "disp-200",
			wantTraceparent:    "",
		},
		{
			name:               "idempotency key populated with dispatch id when skip-if-active is true",
			skipIfActive:       true,
			sessionKey:         "sess-100",
			recordID:           "disp-200",
			keyID:              "disp-200",
			wantRecordID:       "disp-200",
			wantIdempotencyKey: "disp-200",
			wantTraceparent:    "",
		},
		{
			name:               "trace context propagated when active span in ctx",
			traceparent:        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			skipIfActive:       false,
			sessionKey:         "sess-100",
			recordID:           "disp-200",
			keyID:              "disp-200",
			wantRecordID:       "disp-200",
			wantIdempotencyKey: "disp-200",
			wantTraceparent:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var receivedKey string
			var hasHeader bool
			var capturedBody []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedKey = r.Header.Get("Idempotency-Key")
				_, hasHeader = r.Header["Idempotency-Key"]
				var err error
				capturedBody, err = io.ReadAll(r.Body)
				if err != nil {
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			}))
			defer ts.Close()

			ingress := restateclient.NewRestateIngressClient(ts.URL, "aerial", ts.Client())
			ctx := context.Background()
			if tc.traceparent != "" {
				ctx = tracing.ContextWithTraceparent(ctx, tc.traceparent)
			}
			res, err := handleDispatchViaRestate(ctx, ingress, DispatchRequest{
				Prompt:       "run task",
				SkipIfActive: tc.skipIfActive,
			}, tc.sessionKey, tc.recordID, tc.keyID)
			require.NoError(t, err)
			assert.Equal(t, "accepted", res.Status)
			assert.True(t, hasHeader)
			assert.Equal(t, tc.wantIdempotencyKey, receivedKey)

			var brainReq eriwire.BrainDispatchRequest
			require.NoError(t, json.Unmarshal(capturedBody, &brainReq))
			assert.Equal(t, tc.wantTraceparent, brainReq.Traceparent)
			assert.Equal(t, tc.wantRecordID, brainReq.ID)
		})
	}
}

func TestPromptRequestID(t *testing.T) {
	t.Parallel()
	base := PromptRequest{Prompt: "deploy done", AuthorID: "u1", Timestamp: "2026-11-22T10:00:00Z"}
	tests := []struct {
		name      string
		channelA  string
		reqA      PromptRequest
		channelB  string
		reqB      PromptRequest
		wantEqual bool
	}{
		{name: "retry of same request keeps id", channelA: "ch-1", reqA: base, channelB: "ch-1", reqB: base, wantEqual: true},
		{name: "model and effort do not change id", channelA: "ch-1", reqA: base, channelB: "ch-1", reqB: PromptRequest{Prompt: "deploy done", AuthorID: "u1", Timestamp: "2026-11-22T10:00:00Z", Model: "m", Effort: "high"}, wantEqual: true},
		{name: "different channel", channelA: "ch-1", reqA: base, channelB: "ch-2", reqB: base},
		{name: "different author", channelA: "ch-1", reqA: base, channelB: "ch-1", reqB: PromptRequest{Prompt: "deploy done", AuthorID: "u2", Timestamp: "2026-11-22T10:00:00Z"}},
		{name: "different content", channelA: "ch-1", reqA: base, channelB: "ch-1", reqB: PromptRequest{Prompt: "deploy failed", AuthorID: "u1", Timestamp: "2026-11-22T10:00:00Z"}},
		{name: "different timestamp", channelA: "ch-1", reqA: base, channelB: "ch-1", reqB: PromptRequest{Prompt: "deploy done", AuthorID: "u1", Timestamp: "2026-11-22T11:00:00Z"}},
		{name: "fields do not bleed across separators", channelA: "ch-1", reqA: PromptRequest{Prompt: "ab"}, channelB: "ch-1", reqB: PromptRequest{Title: "a", Prompt: "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, b := promptRequestID(tc.channelA, tc.reqA), promptRequestID(tc.channelB, tc.reqB)
			assert.Len(t, a, 36)
			assert.Equal(t, tc.wantEqual, a == b)
		})
	}
}
