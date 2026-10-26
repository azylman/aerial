package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ActiveCount returns the number of currently active media stream sessions in tests.
func (m *CallSessionManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

type mockConsultClient struct {
	mu           sync.Mutex
	calendarResp string
	calendarErr  error
	memoryResp   string
	memoryErr    error
}

func (m *mockConsultClient) ConsultCalendar(ctx context.Context, query, startISO, endISO string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calendarResp, m.calendarErr
}

func (m *mockConsultClient) ConsultMemory(ctx context.Context, query string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memoryResp, m.memoryErr
}

type mockTwilioClient struct {
	mu             sync.Mutex
	hungUpCallSIDs []string
	createdCalls   []CreateCallRequest
	smsList        []string
}

func (m *mockTwilioClient) CreateCall(ctx context.Context, req CreateCallRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createdCalls = append(m.createdCalls, req)
	return "CA_mock_123", nil
}

func (m *mockTwilioClient) HangupCall(ctx context.Context, providerCallSID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hungUpCallSIDs = append(m.hungUpCallSIDs, providerCallSID)
	return nil
}

func (m *mockTwilioClient) IsCallActive(ctx context.Context, providerCallSID string) (bool, error) {
	return true, nil
}

func (m *mockTwilioClient) SendSMS(ctx context.Context, to, from, body string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.smsList = append(m.smsList, to+":"+body)
	return "SM_mock_123", nil
}

func (m *mockTwilioClient) EnsurePhoneNumber(ctx context.Context, areaCode, webhookBaseURL string) (string, error) {
	return "+15105550100", nil
}

func (m *mockTwilioClient) DownloadRecording(ctx context.Context, recordingURL string) ([]byte, string, error) {
	return []byte("test"), "audio/wav", nil
}

func TestMediaStreamBridge_Lifecycle(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	var geminiReceivedMessages []string
	var geminiMu sync.Mutex
	recordGeminiMsg := func(s string) {
		geminiMu.Lock()
		defer geminiMu.Unlock()
		geminiReceivedMessages = append(geminiReceivedMessages, s)
	}

	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, setupBytes, err := conn.ReadMessage()
		if err != nil {
			return
		}
		recordGeminiMsg(string(setupBytes))

		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})

		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				return
			}
			msgStr := string(msgBytes)
			recordGeminiMsg(msgStr)

			if strings.Contains(msgStr, "Call connected") {
				pcm24 := make([]byte, 480)
				_ = conn.WriteJSON(map[string]any{
					"serverContent": map[string]any{
						"modelTurn": map[string]any{
							"parts": []map[string]any{
								{
									"mimeType": "audio/pcm;rate=24000",
									"data":     base64.StdEncoding.EncodeToString(pcm24),
								},
								{
									"text": "Hello, this is Prospera speaking.",
								},
							},
						},
						"turnComplete": true,
					},
				})
			} else if strings.Contains(msgStr, "test_dtmf_trigger") {
				_ = conn.WriteJSON(map[string]any{
					"toolCall": map[string]any{
						"functionCalls": []map[string]any{
							{
								"id":   "call_dtmf_1",
								"name": "send_dtmf",
								"args": map[string]any{"digits": "12w3"},
							},
						},
					},
				})
			} else if strings.Contains(msgStr, "test_calendar_trigger") {
				_ = conn.WriteJSON(map[string]any{
					"toolCall": map[string]any{
						"functionCalls": []map[string]any{
							{
								"id":   "call_cal_1",
								"name": "consult_calendar",
								"args": map[string]any{"query": "dentist", "start_iso": "2026-10-26T00:00:00Z"},
							},
						},
					},
				})
			} else if strings.Contains(msgStr, "test_memory_trigger") {
				_ = conn.WriteJSON(map[string]any{
					"toolCall": map[string]any{
						"functionCalls": []map[string]any{
							{
								"id":   "call_mem_1",
								"name": "consult_memory",
								"args": map[string]any{"query": "address"},
							},
						},
					},
				})
			} else if strings.Contains(msgStr, "test_end_trigger") {
				_ = conn.WriteJSON(map[string]any{
					"toolCall": map[string]any{
						"functionCalls": []map[string]any{
							{
								"id":   "call_end_1",
								"name": "end_call",
								"args": map[string]any{"reason": "task_completed"},
							},
						},
					},
				})
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")

	cfg := &Config{
		GeminiLiveModel:     "gemini-3.8-live",
		GeminiFallbackModel: "gemini-3.1-flash-live-preview",
	}

	twilioClient := &mockTwilioClient{}
	consultClient := &mockConsultClient{
		calendarResp: "Dentist appointment at 2pm",
		memoryResp:   "Home address: 123 Example St",
	}
	tokenMgr := NewStreamTokenManager(time.Now)

	sessionMgr := NewCallSessionManager(cfg, nil, twilioClient, consultClient, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, err := tokenMgr.Issue("call-test-bridge", 30*time.Second)
	if err != nil {
		t.Fatalf("Issue() err = %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token

	dialer := websocket.DefaultDialer
	twilioWS, resp, err := dialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio media stream failed: %v", err)
	}
	defer twilioWS.Close()

	startMsg := map[string]any{
		"event":     "start",
		"streamSid": "MZ123",
		"start": map[string]any{
			"streamSid": "MZ123",
			"callSid":   "CA_twilio_123",
		},
	}
	if err := twilioWS.WriteJSON(startMsg); err != nil {
		t.Fatalf("write start event: %v", err)
	}

	sess, err := sessionMgr.WaitForSession(context.Background(), "call-test-bridge")
	if err != nil || sess == nil {
		t.Fatalf("WaitForSession failed: %v", err)
	}

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session start event")
	}

	speechFrame := base64.StdEncoding.EncodeToString(make([]byte, 160))
	loudUlaw := make([]byte, 160)
	for i := range loudUlaw {
		loudUlaw[i] = 0x00
	}
	loudPayload := base64.StdEncoding.EncodeToString(loudUlaw)

	for i := 0; i < 15; i++ {
		_ = twilioWS.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": loudPayload,
			},
		})
	}
	for i := 0; i < 31; i++ {
		_ = twilioWS.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": speechFrame,
			},
		})
	}

	if err := sessionMgr.SteerCall("call-test-bridge", "Focus on price confirmation", "guidance"); err != nil {
		t.Fatalf("SteerCall(guidance) error: %v", err)
	}
	if err := sessionMgr.SteerCall("call-test-bridge", "Understood, thank you", "say"); err != nil {
		t.Fatalf("SteerCall(say) error: %v", err)
	}

	if err := sessionMgr.SteerCall("call-test-bridge", "test_dtmf_trigger", "guidance"); err != nil {
		t.Fatalf("trigger dtmf err: %v", err)
	}
	if err := sessionMgr.SteerCall("call-test-bridge", "test_calendar_trigger", "guidance"); err != nil {
		t.Fatalf("trigger calendar err: %v", err)
	}
	if err := sessionMgr.SteerCall("call-test-bridge", "test_memory_trigger", "guidance"); err != nil {
		t.Fatalf("trigger memory err: %v", err)
	}

	if err := sessionMgr.HandleAMDWebhook("CA_twilio_123", "human"); err != nil {
		t.Fatalf("HandleAMDWebhook() error: %v", err)
	}

	if err := sessionMgr.SteerCall("call-test-bridge", "test_end_trigger", "guidance"); err != nil {
		t.Fatalf("trigger end_call err: %v", err)
	}

	var receivedMedia bool
	var receivedHangupMark bool

	for !receivedHangupMark {
		_ = twilioWS.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, data, err := twilioWS.ReadMessage()
		if err != nil {
			t.Fatalf("reading from Twilio WS failed before hangup mark: %v", err)
		}
		var evt map[string]any
		if err := json.Unmarshal(data, &evt); err == nil {
			if evt["event"] == "media" {
				receivedMedia = true
			}
			if evt["event"] == "mark" {
				if markObj, ok := evt["mark"].(map[string]any); ok && markObj["name"] == "hangup" {
					receivedHangupMark = true
					_ = twilioWS.WriteJSON(map[string]any{
						"event":     "mark",
						"streamSid": "MZ123",
						"mark":      map[string]any{"name": "hangup"},
					})
					break
				}
			}
		}
	}

	if !receivedMedia {
		t.Error("expected to receive paced media frames on Twilio WebSocket")
	}
	if !receivedHangupMark {
		t.Error("expected to receive mark event name=hangup on Twilio WebSocket")
	}

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session to complete")
	}
}

func TestMediaStream_TransparentReconnect(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	var connCount int
	var countMu sync.Mutex
	reconnected := make(chan struct{})
	var reconnectedOnce sync.Once

	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		countMu.Lock()
		connCount++
		c := connCount
		countMu.Unlock()

		_, _, _ = conn.ReadMessage()

		if c == 1 {
			_ = conn.Close()
			return
		}

		reconnectedOnce.Do(func() {
			close(reconnected)
		})

		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")

	cfg := &Config{
		GeminiLiveModel:     "gemini-3.8-live",
		GeminiFallbackModel: "gemini-3.1-flash-live-preview",
	}

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(cfg, nil, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, _ := tokenMgr.Issue("call-reconnect", 30*time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_recon",
		"start":     map[string]any{"streamSid": "MZ_recon", "callSid": "CA_recon"},
	})

	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Gemini reconnection")
	}

	countMu.Lock()
	finalCount := connCount
	countMu.Unlock()

	if finalCount < 2 {
		t.Fatalf("expected Gemini to reconnect (connCount >= 2), got %d", finalCount)
	}
}

func TestMediaStream_CornerCases(t *testing.T) {
	t.Parallel()

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, nil, nil, nil, tokenMgr,
		WithPacerInterval(5*time.Millisecond),
	)

	defaultMgr := NewCallSessionManager(&Config{}, nil, nil, nil, nil)
	if defaultMgr.ActiveCount() != 0 {
		t.Error("expected 0 active sessions")
	}

	store := setUpTestStore(t)
	mgrWithStore := NewCallSessionManager(&Config{}, store, nil, nil, tokenMgr)
	sessActive := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-steer-store"})
	mgrWithStore.registerSession("call-steer-store", sessActive)
	_ = mgrWithStore.SteerCall("call-steer-store", "be polite", "guidance")

	if err := sessionMgr.SteerCall("missing-call", "msg", "say"); err == nil {
		t.Error("SteerCall on missing session should return error")
	}
	if err := sessionMgr.HandleAMDWebhook("missing-sid", "human"); err == nil {
		t.Error("HandleAMDWebhook on missing SID should return error")
	}
	if err := sessionMgr.EndCall("missing-call"); err == nil {
		t.Error("EndCall on missing call should return error")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/twilio/media-stream?token=invalid", nil)
	sessionMgr.HandleMediaWebSocket(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("HandleMediaWebSocket with bad token status = %d, want 401", rec.Code)
	}

	sess := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "call-accessors",
	})
	sess.SetProviderCallSID("CA_test_sid")
	if sess.ProviderCallSID() != "CA_test_sid" {
		t.Errorf("ProviderCallSID = %q, want CA_test_sid", sess.ProviderCallSID())
	}
	sess.mu.Lock()
	sess.streamSID = "MZ_test_sid"
	sess.mu.Unlock()
	if sess.StreamSID() != "MZ_test_sid" {
		t.Errorf("StreamSID = %q, want MZ_test_sid", sess.StreamSID())
	}

	sessionMgr.registerSession("call-accessors", sess)
	if found, ok := sessionMgr.GetSessionByProviderSID("CA_test_sid"); !ok || found != sess {
		t.Errorf("GetSessionByProviderSID failed to find session")
	}
	if _, ok := sessionMgr.GetSessionByProviderSID("non-existent"); ok {
		t.Errorf("GetSessionByProviderSID found non-existent session")
	}
	if err := sessionMgr.EndCall("call-accessors"); err != nil {
		t.Errorf("EndCall failed: %v", err)
	}
	sessionMgr.deregisterSession("call-accessors")
}

func TestMediaStream_BargeInAndVoicemail(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})

		_ = conn.WriteJSON(map[string]any{
			"serverContent": map[string]any{
				"interrupted": true,
				"modelTurn": map[string]any{
					"parts": []map[string]any{
						{"text": "Spoken text during turn"},
						{
							"mimeType": "audio/pcm;rate=24000",
							"data":     base64.StdEncoding.EncodeToString(make([]byte, 480)),
						},
					},
				},
			},
		})

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
	tokenMgr := NewStreamTokenManager(time.Now)

	consultClient := &mockConsultClient{
		calendarErr: errors.New("calendar failed"),
		memoryErr:   errors.New("memory failed"),
	}

	bargeStore := setUpTestStore(t)
	sessionMgr := NewCallSessionManager(&Config{}, bargeStore, nil, consultClient, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, _ := tokenMgr.Issue("call-bargein", 30*time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_barge",
		"start":     map[string]any{"streamSid": "MZ_barge", "callSid": "CA_barge"},
	})

	sess, err := sessionMgr.WaitForSession(context.Background(), "call-bargein")
	if err != nil || sess == nil {
		t.Fatal("session not found")
	}

	select {
	case <-sess.Started():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	sess.handleAMDAction(AMDActionReleaseGreeting)
	sess.handleAMDAction(AMDActionSwitchToVoicemail)
	sess.handleAMDAction(AMDActionSpeakVoicemailAndHangup)
	sess.HandleAMD("human")

	_ = sess.Steer("guidance instruction", "guidance")
	_ = sess.Steer("say instruction", "say")

	sess.handleInboundTwilioMedia("")
	sess.handleInboundTwilioMedia("invalid-base64!!!")

	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_dtmf_good",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "123"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_good",
		Name: "consult_calendar",
		Args: map[string]any{"query": "schedule"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_good",
		Name: "consult_memory",
		Args: map[string]any{"query": "preferences"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_dtmf_bad",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "INVALID_DIGIT"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_bad",
		Name: "consult_calendar",
		Args: map[string]any{"query": "bad"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_bad",
		Name: "consult_memory",
		Args: map[string]any{"query": "bad"},
	})

	var sawClear bool
	for i := 0; i < 20; i++ {
		_ = twilioWS.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, data, err := twilioWS.ReadMessage()
		if err != nil {
			break
		}
		var evt map[string]any
		if err := json.Unmarshal(data, &evt); err == nil && evt["event"] == "clear" {
			sawClear = true
			break
		}
	}
	if !sawClear {
		t.Error("expected to see clear event on Twilio WS after barge-in/voicemail")
	}
}

func TestStreamTokenManager_NilTimeSource(t *testing.T) {
	t.Parallel()

	mgrNil := NewStreamTokenManager(nil)
	tok, err := mgrNil.Issue("c1", 10*time.Second)
	if err != nil {
		t.Fatalf("Issue err: %v", err)
	}
	if id, ok := mgrNil.Consume(tok); !ok || id != "c1" {
		t.Fatalf("Consume err: %q, %v", id, ok)
	}
}

func TestMediaStreamSession_EdgeCases(t *testing.T) {
	t.Parallel()

	sessNoDialer := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-nodialer",
		Config: &Config{GeminiAPIKey: "key"},
	})
	_, _ = sessNoDialer.dialGemini(context.Background(), "model")

	sessFailDial := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-faildial",
		GeminiDialer: func(ctx context.Context, model string) (*websocket.Conn, error) {
			return nil, errors.New("dial failed")
		},
	})
	sessFailDial.Run(context.Background())

	sessNilConsult := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-nil-consult",
	})
	sessNilConsult.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_nil",
		Name: "consult_calendar",
		Args: map[string]any{"query": "q"},
	})
	sessNilConsult.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_nil",
		Name: "consult_memory",
		Args: map[string]any{"query": "q"},
	})
	sessNilConsult.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_end_nil",
		Name: "end_call",
		Args: map[string]any{"reason": "done"},
	})

	sessFailReconnect := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-fail-recon",
		GeminiDialer: func(ctx context.Context, model string) (*websocket.Conn, error) {
			return nil, errors.New("fail reconnect")
		},
	})
	if sessFailReconnect.reconnectGemini(context.Background()) {
		t.Error("reconnectGemini should return false when dial fails")
	}

	sessHelpers := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "c-helpers"})
	sessHelpers.sendTwilioMedia([]byte{0x01})
	sessHelpers.sendTwilioClear()
	sessHelpers.sendTwilioMark("mark")
	if err := sessHelpers.sendGeminiJSON(map[string]any{}); err == nil {
		t.Error("expected error when sending JSON with nil conn")
	}
	sessHelpers.handleInboundTwilioMedia(base64.StdEncoding.EncodeToString(make([]byte, 160)))
	sessHelpers.handleInboundTwilioMedia(base64.StdEncoding.EncodeToString([]byte{}))
}

func TestCallSessionManager_HandleAMDWebhookByProviderSID(t *testing.T) {
	t.Parallel()

	store := setUpTestStore(t)
	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, store, nil, nil, tokenMgr)
	callLookup := "call-lookup-test"
	provLookup := "CA_lookup_prov_123"
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
		CallID:     callLookup,
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}, 5)
	_ = store.BindProviderCallSID(context.Background(), callLookup, provLookup)
	sessLookup := NewMediaStreamSession(MediaStreamSessionConfig{CallID: callLookup})
	sessionMgr.registerSession(callLookup, sessLookup)
	if err := sessionMgr.HandleAMDWebhook(provLookup, "human"); err != nil {
		t.Errorf("HandleAMDWebhook via store lookup failed: %v", err)
	}
	sessionMgr.deregisterSession(callLookup)
}

func TestMediaStream_StopEvent(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")

	store := setUpTestStore(t)
	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, store, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, _ := tokenMgr.Issue("call-stop-event", 30*time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_stop",
		"start":     map[string]any{"streamSid": "MZ_stop", "callSid": "CA_stop"},
	})

	sess, err := sessionMgr.WaitForSession(context.Background(), "call-stop-event")
	if err != nil || sess == nil {
		t.Fatalf("WaitForSession failed: %v", err)
	}

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	_ = twilioWS.WriteJSON(map[string]any{
		"event": "stop",
		"stop":  map[string]any{"callSid": "CA_stop"},
	})

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session stop")
	}
}

func TestWaitForSession_Branches(t *testing.T) {
	t.Parallel()

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, nil, nil, nil, tokenMgr,
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			return nil, nil
		}),
	)

	// 1. Context canceled before session is registered
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sessionMgr.WaitForSession(cancelCtx, "call-wait-cancel"); err == nil {
		t.Error("expected context canceled error")
	}

	// 2. Wait for session that gets registered asynchronously
	sess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-wait-async"})
	go func() {
		sessionMgr.registerSession("call-wait-async", sess)
	}()

	found, err := sessionMgr.WaitForSession(context.Background(), "call-wait-async")
	if err != nil || found != sess {
		t.Fatalf("WaitForSession async failed: %v, %v", found, err)
	}
}

func TestHandleMediaWebSocket_WithStoreCall(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	store := setUpTestStore(t)
	callID := "call-store-configured"
	_ = store.CreateOutboundCall(context.Background(), VoiceCall{
		CallID:     callID,
		Direction:  "outbound",
		State:      "initiated",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		Brief: CallBrief{
			Task:             "Confirm doctor visit",
			VoicemailMessage: "Please call the account owner back",
		},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}, 5)

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, store, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(geminiServer.URL, "http"), nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, _ := tokenMgr.Issue(callID, 30*time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_store_cfg",
		"start":     map[string]any{"streamSid": "MZ_store_cfg", "callSid": "CA_store_cfg"},
	})

	sess, err := sessionMgr.WaitForSession(context.Background(), callID)
	if err != nil || sess == nil {
		t.Fatalf("WaitForSession failed: %v", err)
	}

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	_ = twilioWS.WriteJSON(map[string]any{
		"event": "stop",
		"stop":  map[string]any{"callSid": "CA_store_cfg"},
	})

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for done")
	}
}

func TestHandleMediaWebSocket_InboundMode(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}})
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	store := setUpTestStore(t)
	callID := "call-inbound-mode-test"
	_ = store.CreateInboundCall(context.Background(), VoiceCall{
		CallID:     callID,
		Direction:  "inbound",
		State:      "in-progress",
		FromNumber: "+15105550111",
		ToNumber:   "+15105550100",
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	})

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(&Config{}, store, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, model string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, "ws"+strings.TrimPrefix(geminiServer.URL, "http"), nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
	)

	token, _ := tokenMgr.Issue(callID, 30*time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionMgr.HandleMediaWebSocket(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?token=" + token
	twilioWS, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial twilio ws failed: %v", err)
	}
	defer twilioWS.Close()

	_ = twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_inbound_test",
		"start":     map[string]any{"streamSid": "MZ_inbound_test", "callSid": "CA_inbound_test"},
	})

	sess, err := sessionMgr.WaitForSession(context.Background(), callID)
	if err != nil || sess == nil {
		t.Fatalf("WaitForSession failed: %v", err)
	}

	if !strings.Contains(sess.systemPrompt, "CALLBACK RECEPTIONIST MODE:") {
		t.Fatalf("inbound session system prompt missing CALLBACK RECEPTIONIST MODE:\n%s", sess.systemPrompt)
	}

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	_ = twilioWS.WriteJSON(map[string]any{
		"event": "stop",
		"stop":  map[string]any{"callSid": "CA_inbound_test"},
	})

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for done")
	}
}

func TestMediaStream_DirectHandlers(t *testing.T) {
	t.Parallel()

	store := setUpTestStore(t)
	consultClient := &mockConsultClient{
		calendarResp: "Confirmed 3pm meeting",
		memoryResp:   "Primary contact: Jane Doe",
	}

	sess := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:        "call-direct-test",
		Store:         store,
		ConsultClient: consultClient,
	})

	sess.handleServerContent(context.Background(), &GeminiServerContent{
		Interrupted: true,
		ModelTurn: &GeminiModelTurn{
			Parts: []GeminiPart{
				{
					Text: "Assistant turn response",
				},
				{
					MimeType: "audio/pcm;rate=24000",
					Data:     base64.StdEncoding.EncodeToString(make([]byte, 480)),
				},
			},
		},
	})

	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_dtmf_good",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "123#"},
	})
	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_dtmf_bad",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "BAD"},
	})

	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_good",
		Name: "consult_calendar",
		Args: map[string]any{"query": "schedule"},
	})

	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_good",
		Name: "consult_memory",
		Args: map[string]any{"query": "preferences"},
	})

	sess.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_end_good",
		Name: "end_call",
		Args: map[string]any{"reason": "finished"},
	})

	consultClientErr := &mockConsultClient{
		calendarErr: errors.New("cal error"),
		memoryErr:   errors.New("mem error"),
	}
	sessErr := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:        "call-err-test",
		ConsultClient: consultClientErr,
	})
	sessErr.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_err",
		Name: "consult_calendar",
		Args: map[string]any{"query": "error"},
	})
	sessErr.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_err",
		Name: "consult_memory",
		Args: map[string]any{"query": "error"},
	})

	sessNil := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "call-nil-client",
	})
	sessNil.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_cal_nil",
		Name: "consult_calendar",
		Args: map[string]any{"query": "nil"},
	})
	sessNil.executeToolCall(context.Background(), GeminiFunctionCall{
		ID:   "call_mem_nil",
		Name: "consult_memory",
		Args: map[string]any{"query": "nil"},
	})

	sess.InitiateHangup("test_hangup")
	sess.enqueueOutboundUlaw(make([]byte, 1005*160))
}

func WithPacerInterval(d time.Duration) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.pacerInterval = d
	}
}

func WithGeminiDialer(dialer func(ctx context.Context, model string) (*websocket.Conn, error)) SessionManagerOption {
	return func(m *CallSessionManager) {
		m.geminiDialer = dialer
	}
}

func (m *CallSessionManager) WaitForSession(ctx context.Context, callID string) (*MediaStreamSession, error) {
	m.mu.Lock()
	if sess, ok := m.sessions[callID]; ok {
		m.mu.Unlock()
		return sess, nil
	}
	ch, ok := m.sessionWaiters[callID]
	if !ok {
		ch = make(chan *MediaStreamSession, 1)
		m.sessionWaiters[callID] = ch
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case sess := <-ch:
		return sess, nil
	}
}

func (s *MediaStreamSession) Started() <-chan struct{} {
	return s.started
}

func (s *MediaStreamSession) Done() <-chan struct{} {
	return s.doneCh
}

func (s *MediaStreamSession) SetProviderCallSID(sid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providerCallSID = sid
}

func (s *MediaStreamSession) StreamSID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamSID
}

func TestMediaStreamSession_DisconnectedAMDActions(t *testing.T) {
	t.Parallel()
	sessNoGemini := &MediaStreamSession{
		openingGreeting: "Hello from opening",
	}
	sessNoGemini.handleAMDAction(AMDActionSwitchToVoicemail)
	sessNoGemini.handleAMDAction(AMDActionSpeakVoicemailAndHangup)
	sessNoGemini.handleAMDAction(AMDActionReleaseGreeting)

	sessWithDialFail := &MediaStreamSession{
		geminiDialer: func(ctx context.Context, model string) (*websocket.Conn, error) {
			return nil, errors.New("dial fail")
		},
	}
	if ok := sessWithDialFail.reconnectGemini(context.Background()); ok {
		t.Fatal("expected reconnect failure")
	}
}
