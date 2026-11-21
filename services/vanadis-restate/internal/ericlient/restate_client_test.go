package ericlient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServiceNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		agent          string
		wantNorm       string
		wantSessionMgr string
		wantThread     string
		wantDisp       string
		wantVoice      string
	}{
		{
			name:           "empty defaults to aerial",
			agent:          "",
			wantNorm:       "",
			wantSessionMgr: "SessionManager",
			wantThread:     "ThreadSession",
			wantDisp:       "DispatchSession",
			wantVoice:      "VoiceSession",
		},
		{
			name:           "aerial explicit",
			agent:          "aerial",
			wantNorm:       "aerial",
			wantSessionMgr: "SessionManager",
			wantThread:     "ThreadSession",
			wantDisp:       "DispatchSession",
			wantVoice:      "VoiceSession",
		},
		{
			name:           "agent-alpha lowercase",
			agent:          "agent-alpha",
			wantNorm:       "agent-alpha",
			wantSessionMgr: "SessionManager-agent-alpha",
			wantThread:     "ThreadSession-agent-alpha",
			wantDisp:       "DispatchSession-agent-alpha",
			wantVoice:      "VoiceSession-agent-alpha",
		},
		{
			name:           "agent-beta uppercase trimmed",
			agent:          "  agent-beta  ",
			wantNorm:       "agent-beta",
			wantSessionMgr: "SessionManager-agent-beta",
			wantThread:     "ThreadSession-agent-beta",
			wantDisp:       "DispatchSession-agent-beta",
			wantVoice:      "VoiceSession-agent-beta",
		},
		{
			name:           "unsafe characters fallback",
			agent:          "bad/agent@name!",
			wantNorm:       "aerial",
			wantSessionMgr: "SessionManager",
			wantThread:     "ThreadSession",
			wantDisp:       "DispatchSession",
			wantVoice:      "VoiceSession",
		},
		{
			name:           "safe hyphens and underscores",
			agent:          "bot-1_test",
			wantNorm:       "bot-1_test",
			wantSessionMgr: "SessionManager-bot-1_test",
			wantThread:     "ThreadSession-bot-1_test",
			wantDisp:       "DispatchSession-bot-1_test",
			wantVoice:      "VoiceSession-bot-1_test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeAgentName(tc.agent); got != tc.wantNorm {
				t.Errorf("NormalizeAgentName(%q) = %q, want %q", tc.agent, got, tc.wantNorm)
			}
			if got := SessionManagerServiceName(tc.agent); got != tc.wantSessionMgr {
				t.Errorf("SessionManagerServiceName(%q) = %q, want %q", tc.agent, got, tc.wantSessionMgr)
			}
			if got := ThreadSessionServiceName(tc.agent); got != tc.wantThread {
				t.Errorf("ThreadSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantThread)
			}
			if got := DispatchSessionServiceName(tc.agent); got != tc.wantDisp {
				t.Errorf("DispatchSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantDisp)
			}
			if got := VoiceSessionServiceName(tc.agent); got != tc.wantVoice {
				t.Errorf("VoiceSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantVoice)
			}
		})
	}
}

type reconcileTestCase struct {
	name       string
	adminURL   string
	status     int
	body       string
	wantErr    bool
	errContain string
}

func runReconcileTest(t *testing.T, tc reconcileTestCase) {
	t.Helper()
	adminURL := tc.adminURL
	if tc.adminURL == "placeholder" {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPatch {
				t.Errorf("expected PATCH, got %s", r.Method)
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected application/json, got %s", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		defer srv.Close()
		adminURL = srv.URL
	}
	if tc.adminURL == "partial-placeholder" {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "VoiceSession") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`fail`))
		}))
		defer srv.Close()
		adminURL = srv.URL
	}

	err := ReconcileRestateServiceTimeouts(context.Background(), adminURL, "aerial", nil)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.errContain != "" && !strings.Contains(err.Error(), tc.errContain) {
			t.Fatalf("error %q does not contain %q", err.Error(), tc.errContain)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReconcileRestateServiceTimeouts(t *testing.T) {
	t.Parallel()

	tests := []reconcileTestCase{
		{
			name:     "success patch all services",
			adminURL: "placeholder",
			status:   http.StatusOK,
			body:     `{"status":"ok"}`,
		},
		{
			name:       "empty admin url",
			adminURL:   "",
			wantErr:    true,
			errContain: "admin url cannot be empty",
		},
		{
			name:       "server returns 500 error",
			adminURL:   "placeholder",
			status:     http.StatusInternalServerError,
			body:       `internal error`,
			wantErr:    true,
			errContain: "returned HTTP 500",
		},
		{
			name:       "server partial 500 aggregates errors",
			adminURL:   "partial-placeholder",
			status:     http.StatusInternalServerError,
			body:       `internal error`,
			wantErr:    true,
			errContain: "ThreadSession",
		},
		{
			name:       "invalid admin url",
			adminURL:   "http://127.0.0.1:0\nbad",
			wantErr:    true,
			errContain: "create timeout patch request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runReconcileTest(t, tc)
		})
	}
}

type mockDeliverer struct {
	msgID       string
	err         error
	lastChannel string
	lastContent string
}

func (m *mockDeliverer) DeliverStatusWidget(_ context.Context, _ StatusWidgetRequest) (string, error) {
	return m.msgID, m.err
}

func (m *mockDeliverer) DeliverDirectMessage(_ context.Context, channelID, content string) error {
	m.lastChannel = channelID
	m.lastContent = content
	return m.err
}

type dispatchTestCase struct {
	name       string
	restateSrv bool
	status     int
	body       string
	req        BrainDispatchRequest
	fallback   bool
	wantErr    bool
	errContain string
	errIs      error
}

func runDispatchTest(t *testing.T, tc dispatchTestCase) {
	t.Helper()
	var restateURL, eriURL string
	if tc.restateSrv {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Idempotency-Key") != tc.req.ID {
				t.Errorf("Idempotency-Key = %q, want %q", r.Header.Get("Idempotency-Key"), tc.req.ID)
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		defer srv.Close()
		restateURL = srv.URL
	}
	if tc.fallback {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/dispatch" {
				t.Errorf("path = %q, want /dispatch", r.URL.Path)
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		defer srv.Close()
		eriURL = srv.URL
	}

	client := NewRestateEriClient(restateURL, eriURL, "aerial", nil)
	err := client.Dispatch(context.Background(), tc.req)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.errIs != nil && !errors.Is(err, tc.errIs) {
			t.Fatalf("error %v is not %v", err, tc.errIs)
		}
		if tc.errContain != "" && !strings.Contains(err.Error(), tc.errContain) {
			t.Fatalf("error %q does not contain %q", err.Error(), tc.errContain)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestateEriClient_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []dispatchTestCase{
		{
			name:       "restate success accepted",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"accepted","dispatch_id":"d1"}`,
			req:        BrainDispatchRequest{ID: "d1", VanadisChunkID: "c1", Source: "test"},
		},
		{
			name:       "restate skipped_active maps to ErrDispatchActive",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"skipped_active"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errIs:      ErrDispatchActive,
		},
		{
			name:       "restate rejected unknown status",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"rejected"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errContain: "restate rejected dispatch",
		},
		{
			name:       "restate server http 500 with message",
			restateSrv: true,
			status:     http.StatusInternalServerError,
			body:       `{"message":"actor busy"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errContain: "actor busy",
		},
		{
			name:       "restate server http 400 with error field",
			restateSrv: true,
			status:     http.StatusBadRequest,
			body:       `{"error":"invalid payload"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errContain: "invalid payload",
		},
		{
			name:     "fallback to http eri when restate url empty",
			fallback: true,
			status:   http.StatusOK,
			body:     `{"status":"accepted","dispatch_id":"d1"}`,
			req:      BrainDispatchRequest{ID: "d1", Source: "test"},
		},
		{
			name:       "neither url configured",
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errContain: "neither restate url nor eri url configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchTest(t, tc)
		})
	}
}

func TestRestateEriClient_DispatchExplicitAgentPath(t *testing.T) {
	t.Parallel()
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"accepted","dispatch_id":"d1"}`))
	}))
	defer srv.Close()

	client := NewRestateEriClient(srv.URL, "", "aerial", nil)
	err := client.Dispatch(context.Background(), BrainDispatchRequest{ID: "d1", Agent: "agent-alpha", VanadisChunkID: "c1", Source: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSub := "/" + DispatchSessionServiceName("agent-alpha") + "/"
	if !strings.Contains(capturedPath, wantSub) {
		t.Errorf("captured path %q does not contain %q", capturedPath, wantSub)
	}
}

func TestRestateEriClient_DispatchAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		restateSrv bool
		status     int
		body       string
		req        BrainDispatchRequest
		fallback   bool
		wantErr    bool
		errContain string
		errIs      error
	}{
		{
			name:       "restate success accepted with dispatch_id",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"accepted","dispatch_id":"d1"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
		},
		{
			name:       "restate success already_exists with message_id",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"already_exists","message_id":"d1"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
		},
		{
			name:       "restate skipped_active maps to ErrDispatchActive",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"skipped_active"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errIs:      ErrDispatchActive,
		},
		{
			name:       "restate mismatched identity",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"accepted","dispatch_id":"other"}`,
			req:        BrainDispatchRequest{ID: "d1", Source: "test"},
			wantErr:    true,
			errContain: "restate did not accept dispatch",
		},
		{
			name:     "fallback to http eri",
			fallback: true,
			status:   http.StatusOK,
			body:     `{"status":"accepted","dispatch_id":"d1"}`,
			req:      BrainDispatchRequest{ID: "d1", Source: "test"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var restateURL, eriURL string
			if tc.restateSrv {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				restateURL = srv.URL
			}
			if tc.fallback {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				eriURL = srv.URL
			}

			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil)
			err := client.DispatchAccepted(context.Background(), tc.req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.errIs != nil && !errors.Is(err, tc.errIs) {
					t.Fatalf("error %v is not %v", err, tc.errIs)
				}
				if tc.errContain != "" && !strings.Contains(err.Error(), tc.errContain) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContain)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRestateEriClient_CancelDispatchAndPrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		chunkID    string
		threadID   string
		prompt     string
		restateSrv bool
		status     int
		body       string
		fallback   bool
		wantErr    bool
		errContain string
	}{
		{
			name:       "cancel dispatch restate success",
			op:         "cancel",
			chunkID:    "c1",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"ok"}`,
		},
		{
			name:       "cancel dispatch empty chunk id no-op",
			op:         "cancel",
			chunkID:    "",
			restateSrv: false,
		},
		{
			name:     "cancel dispatch fallback to eri",
			op:       "cancel",
			chunkID:  "c1",
			fallback: true,
			status:   http.StatusOK,
			body:     `{"status":"ok"}`,
		},
		{
			name:       "prompt restate success",
			op:         "prompt",
			threadID:   "th-1",
			prompt:     "hello world",
			restateSrv: true,
			status:     http.StatusOK,
			body:       `{"status":"accepted"}`,
		},
		{
			name:       "prompt empty no-op",
			op:         "prompt",
			threadID:   "",
			prompt:     "",
			restateSrv: false,
		},
		{
			name:     "prompt fallback to eri",
			op:       "prompt",
			threadID: "th-1",
			prompt:   "hello",
			fallback: true,
			status:   http.StatusOK,
			body:     `{"status":"ok"}`,
		},
		{
			name:       "prompt neither configured",
			op:         "prompt",
			threadID:   "th-1",
			prompt:     "hello",
			wantErr:    true,
			errContain: "neither restate url nor eri url configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var restateURL, eriURL string
			if tc.restateSrv {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				restateURL = srv.URL
			}
			if tc.fallback {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				eriURL = srv.URL
			}

			client := NewRestateEriClient(restateURL, eriURL, "aerial", nil)
			var err error
			if tc.op == "cancel" {
				err = client.CancelDispatch(context.Background(), tc.chunkID)
			} else {
				err = client.Prompt(context.Background(), tc.threadID, tc.prompt)
			}

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.errContain != "" && !strings.Contains(err.Error(), tc.errContain) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContain)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

type widgetTestCase struct {
	name        string
	envToken    string
	restateURL  string
	hasDeliver  bool
	deliverMsg  string
	deliverErr  error
	eriStatus   int
	eriBody     string
	wantMsgID   string
	wantErr     bool
	errContain  string
	testMessage bool
	wantEriHit  bool
}

func assertExpectedError(t *testing.T, err error, wantErr bool, errContain string) bool {
	t.Helper()
	if !wantErr {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return false
	}
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if errContain != "" && !strings.Contains(err.Error(), errContain) {
		t.Fatalf("error %q does not contain %q", err.Error(), errContain)
	}
	return true
}

func verifyWidgetMessageCall(t *testing.T, client *RestateEriClient, mockDel *mockDeliverer, eriHits *int, tc widgetTestCase) {
	t.Helper()
	err := client.Message(context.Background(), "ch-1", "test content")
	if assertExpectedError(t, err, tc.wantErr, tc.errContain) {
		return
	}
	if tc.wantEriHit {
		if eriHits == nil || *eriHits != 1 {
			t.Errorf("eriHits = %v, want 1", eriHits)
		}
		if mockDel != nil && (mockDel.lastChannel != "" || mockDel.lastContent != "") {
			t.Errorf("mockDeliverer was unexpectedly called: channel=%q, content=%q", mockDel.lastChannel, mockDel.lastContent)
		}
		return
	}
	if tc.hasDeliver && tc.envToken != "" && tc.restateURL != "" && mockDel != nil {
		if mockDel.lastChannel != "ch-1" || mockDel.lastContent != "test content" {
			t.Errorf("mockDeliverer got channel=%q content=%q, want ch-1 / test content", mockDel.lastChannel, mockDel.lastContent)
		}
		if eriHits != nil && *eriHits != 0 {
			t.Errorf("eriHits = %d, want 0 when direct deliverer active", *eriHits)
		}
	}
}

func runWidgetTest(t *testing.T, tc widgetTestCase) {
	t.Helper()
	t.Setenv("DISCORD_TOKEN", tc.envToken)
	var eriHits int
	var eriURL string
	if tc.eriBody != "" {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			eriHits++
			w.WriteHeader(tc.eriStatus)
			_, _ = w.Write([]byte(tc.eriBody))
		}))
		defer srv.Close()
		eriURL = srv.URL
	}

	var deliverer StatusWidgetDeliverer
	var mockDel *mockDeliverer
	if tc.hasDeliver {
		mockDel = &mockDeliverer{msgID: tc.deliverMsg, err: tc.deliverErr}
		deliverer = mockDel
	}

	client := NewRestateEriClient(tc.restateURL, eriURL, "aerial", nil, deliverer)
	if tc.testMessage {
		verifyWidgetMessageCall(t, client, mockDel, &eriHits, tc)
		return
	}

	msgID, err := client.Widget(context.Background(), StatusWidgetRequest{ID: "w1"})
	if assertExpectedError(t, err, tc.wantErr, tc.errContain) {
		return
	}
	if msgID != tc.wantMsgID {
		t.Errorf("msgID = %q, want %q", msgID, tc.wantMsgID)
	}
}

func TestRestateEriClient_WidgetAndMessage(t *testing.T) {
	tests := []widgetTestCase{
		{
			name:       "widget direct deliverer active when token and restate set",
			envToken:   "dummy-token",
			restateURL: "http://restate.test",
			hasDeliver: true,
			deliverMsg: "msg-direct-1",
			wantMsgID:  "msg-direct-1",
		},
		{
			name:       "widget falls back to eri when token missing",
			envToken:   "",
			restateURL: "http://restate.test",
			hasDeliver: true,
			eriStatus:  http.StatusOK,
			eriBody:    `{"message_id":"msg-eri-2"}`,
			wantMsgID:  "msg-eri-2",
		},
		{
			name:       "widget falls back to eri when restate url empty",
			envToken:   "dummy-token",
			restateURL: "",
			hasDeliver: true,
			eriStatus:  http.StatusOK,
			eriBody:    `{"message_id":"msg-eri-3"}`,
			wantMsgID:  "msg-eri-3",
		},
		{
			name:       "widget unconfigured returns error",
			envToken:   "",
			restateURL: "",
			wantErr:    true,
			errContain: "neither widget deliverer nor eri url configured",
		},
		{
			name:        "message direct deliverer active when token and restate set",
			envToken:    "dummy-token",
			restateURL:  "http://restate.test",
			hasDeliver:  true,
			testMessage: true,
		},
		{
			name:        "message direct deliverer error propagates",
			envToken:    "dummy-token",
			restateURL:  "http://restate.test",
			hasDeliver:  true,
			deliverErr:  errors.New("deliverer direct boom"),
			testMessage: true,
			wantErr:     true,
			errContain:  "deliverer direct boom",
		},
		{
			name:        "message falls back to eri when token missing",
			envToken:    "",
			restateURL:  "http://restate.test",
			hasDeliver:  true,
			eriStatus:   http.StatusOK,
			eriBody:     `{"status":"ok"}`,
			testMessage: true,
			wantEriHit:  true,
		},
		{
			name:        "message falls back to eri when restate url empty",
			envToken:    "dummy-token",
			restateURL:  "",
			hasDeliver:  true,
			eriStatus:   http.StatusOK,
			eriBody:     `{"status":"ok"}`,
			testMessage: true,
			wantEriHit:  true,
		},
		{
			name:        "message unconfigured returns error",
			envToken:    "",
			restateURL:  "",
			testMessage: true,
			wantErr:     true,
			errContain:  "neither direct message deliverer nor http fallback configured for message delivery",
		},
		{
			name:        "message delegates to http fallback",
			eriStatus:   http.StatusOK,
			eriBody:     `{"status":"ok"}`,
			testMessage: true,
			wantEriHit:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runWidgetTest(t, tc)
		})
	}
}

func TestRestateEriClient_NilReceiver(t *testing.T) {
	t.Parallel()

	var nilClient *RestateEriClient
	ctx := context.Background()

	if err := nilClient.Dispatch(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error on nil client Dispatch")
	}
	if err := nilClient.DispatchAccepted(ctx, BrainDispatchRequest{}); err == nil {
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
	if err := nilClient.PromptAgent(ctx, "a", "th1", "p", "k"); err == nil {
		t.Error("expected error on nil client PromptAgent")
	}
	if err := nilClient.DispatchRestate(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error on nil client DispatchRestate")
	}
	if err := nilClient.MessageAgent(ctx, "a", "ch1", "m"); err == nil {
		t.Error("expected error on nil client MessageAgent")
	}
}

func TestRestateEriClient_EdgeBehaviors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client := NewRestateEriClient("", "", "", nil)
	client.SetWidgetDeliverer(&mockDeliverer{})
	client.SetDiscordAuth(true)
	if err := client.Message(ctx, "", ""); err != nil {
		t.Errorf("empty message should be no-op: %v", err)
	}

	// Coverage for CancelDispatch without restate or eri url
	if err := client.CancelDispatch(ctx, "c1"); err == nil {
		t.Error("expected error when neither url configured in CancelDispatch")
	}

	// Coverage for DispatchAccepted with empty body
	emptyBodySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer emptyBodySrv.Close()

	emptyClient := NewRestateEriClient(emptyBodySrv.URL, "", "aerial", nil)
	if err := emptyClient.DispatchAccepted(ctx, BrainDispatchRequest{ID: "d1"}); err == nil {
		t.Error("expected error when DispatchAccepted receives empty body")
	}

	// Coverage for malformed JSON in parseDispatchResponse
	badJSONSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer badJSONSrv.Close()

	badJSONClient := NewRestateEriClient(badJSONSrv.URL, "", "aerial", nil)
	if err := badJSONClient.Dispatch(ctx, BrainDispatchRequest{ID: "d1"}); err == nil {
		t.Error("expected error on malformed JSON response")
	}

	// Coverage for HTTP request creation failure (invalid URL)
	badURLClient := NewRestateEriClient("http://127.0.0.1:0\nbad", "", "aerial", nil)
	if err := badURLClient.Dispatch(ctx, BrainDispatchRequest{ID: "d1"}); err == nil {
		t.Error("expected error on bad restate URL")
	}

	// Coverage for client.Do failure (unreachable server)
	closedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closedSrv.URL
	closedSrv.Close()
	unreachableClient := NewRestateEriClient(closedURL, "", "aerial", nil)
	if err := unreachableClient.Dispatch(ctx, BrainDispatchRequest{ID: "d1"}); err == nil {
		t.Error("expected error on unreachable restate server")
	}

	// Coverage for Message error when fallback is nil
	noFallbackClient := NewRestateEriClient("", "", "", nil)
	if err := noFallbackClient.Message(ctx, "ch1", "content"); err == nil {
		t.Error("expected error when Message called without fallback")
	}

	// Coverage for parseRestateHTTPError fallback string
	nonJSONErrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("raw text gateway error"))
	}))
	defer nonJSONErrSrv.Close()
	gatewayErrClient := NewRestateEriClient(nonJSONErrSrv.URL, "", "aerial", nil)
	if err := gatewayErrClient.Dispatch(ctx, BrainDispatchRequest{ID: "d1"}); err == nil || !strings.Contains(err.Error(), "raw text gateway error") {
		t.Errorf("expected raw text error, got %v", err)
	}

	// Coverage for resolveSessionKey fallback branches
	key1 := resolveSessionKey(BrainDispatchRequest{SessionID: "sess-1"})
	if key1 != "sess-1" {
		t.Errorf("key1 = %q, want sess-1", key1)
	}
	key2 := resolveSessionKey(BrainDispatchRequest{VanadisChunkID: "chunk-2"})
	if key2 != "vanadis-chunk-2" {
		t.Errorf("key2 = %q, want vanadis-chunk-2", key2)
	}
	key3 := resolveSessionKey(BrainDispatchRequest{ID: "disp-3"})
	if key3 != "dispatch-disp-3" {
		t.Errorf("key3 = %q, want dispatch-disp-3", key3)
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
			wantAgent: defaultAgentName,
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
