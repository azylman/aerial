package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	hangupErr      error
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
	return m.hangupErr
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

	var geminiReceivedMessages []string
	var geminiMu sync.Mutex
	recordGeminiMsg := func(s string) {
		geminiMu.Lock()
		defer geminiMu.Unlock()
		geminiReceivedMessages = append(geminiReceivedMessages, s)
	}

	geminiWSURL, _ := setupLifecycleGeminiServer(t, recordGeminiMsg)

	cfg := &Config{
		GeminiLiveModel:     "gemini-3.8-live",
		GeminiFallbackModel: "gemini-3.1-flash-live-preview",
		GeminiVoiceName:     "Kore",
	}

	twilioClient := &mockTwilioClient{}
	consultClient := &mockConsultClient{
		calendarResp: "Dentist appointment at 2pm",
		memoryResp:   "Home address: 123 Example St",
	}
	tokenMgr := NewStreamTokenManager(time.Now)

	sessionMgr := NewCallSessionManager(staticConfig{cfg: cfg}, nil, twilioClient, consultClient, tokenMgr,
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

	injectTwilioAudioFrames(twilioWS)

	exerciseBridgeSteerAndAMD(t, sessionMgr, "call-test-bridge", "CA_twilio_123")

	receivedMedia := waitForHangupMark(t, twilioWS)
	if !receivedMedia {
		t.Error("expected to receive paced media frames on Twilio WebSocket")
	}

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session to complete")
	}

	geminiMu.Lock()
	var sawRealtimeAudio bool
	for _, msg := range geminiReceivedMessages {
		if strings.Contains(msg, `"audio":{"mimeType":"audio/pcm;rate=16000"`) {
			sawRealtimeAudio = true
			break
		}
	}
	geminiMu.Unlock()
	if !sawRealtimeAudio {
		t.Error("expected Gemini Live to receive realtimeInput.audio chunk after normalized AMD cadence release")
	}
}

func setupLifecycleGeminiServer(t *testing.T, recordGeminiMsg func(string)) (string, *httptest.Server) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

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

			respondLifecycleGeminiEvent(conn, msgStr)
		}
	}))
	t.Cleanup(geminiServer.Close)
	return "ws" + strings.TrimPrefix(geminiServer.URL, "http"), geminiServer
}

func respondLifecycleGeminiEvent(conn *websocket.Conn, msgStr string) {
	switch {
	case strings.Contains(msgStr, "Call connected"):
		pcm24 := make([]byte, 4800)
		_ = conn.WriteJSON(map[string]any{
			"serverContent": map[string]any{
				"modelTurn": map[string]any{
					"parts": []map[string]any{
						{
							"inlineData": map[string]any{
								"mimeType": "audio/pcm;rate=24000",
								"data":     base64.StdEncoding.EncodeToString(pcm24),
							},
						},
						{
							"text":    "Internal thinking reasoning",
							"thought": true,
						},
					},
				},
				"outputTranscription": map[string]any{
					"text": "Hello, this is Prospera speaking.",
				},
				"turnComplete": true,
			},
		})
	case strings.Contains(msgStr, "test_dtmf_trigger"):
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
	case strings.Contains(msgStr, "test_calendar_trigger"):
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
	case strings.Contains(msgStr, "test_memory_trigger"):
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
	case strings.Contains(msgStr, "test_end_trigger"):
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

func injectTwilioAudioFrames(ws *websocket.Conn) {
	quietUlaw := make([]byte, 160)
	for i := range quietUlaw {
		quietUlaw[i] = 0xFE
	}
	quietPayload := base64.StdEncoding.EncodeToString(quietUlaw)
	loudUlaw := make([]byte, 160)
	for i := range loudUlaw {
		loudUlaw[i] = 0x00
	}
	loudPayload := base64.StdEncoding.EncodeToString(loudUlaw)

	for range 15 {
		_ = ws.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": loudPayload,
			},
		})
	}
	for range 31 {
		_ = ws.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": quietPayload,
			},
		})
	}
}

func waitForHangupMark(t *testing.T, ws *websocket.Conn) bool {
	t.Helper()
	var receivedMedia bool
	for {
		_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, data, err := ws.ReadMessage()
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
					_ = ws.WriteJSON(map[string]any{
						"event":     "mark",
						"streamSid": "MZ123",
						"mark":      map[string]any{"name": "hangup"},
					})
					return receivedMedia
				}
			}
		}
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
		GeminiVoiceName:     "Kore",
	}

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(staticConfig{cfg: cfg}, nil, nil, nil, tokenMgr,
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

func TestCallSessionManager_SessionRegistryAndAccessors(t *testing.T) {
	t.Parallel()

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, nil, nil, nil, tokenMgr,
		WithPacerInterval(5*time.Millisecond),
	)

	defaultMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, nil, nil, nil, nil)
	if defaultMgr.ActiveCount() != 0 {
		t.Error("expected 0 active sessions")
	}

	store := setUpTestStore(t)
	mgrWithStore := NewCallSessionManager(staticConfig{cfg: &Config{}}, store, nil, nil, tokenMgr)
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

	validTok, _ := tokenMgr.Issue("call-non-ws", 10*time.Second)
	recNonWS := httptest.NewRecorder()
	reqNonWS := httptest.NewRequest(http.MethodGet, "/twilio/media-stream?token="+validTok, nil)
	sessionMgr.HandleMediaWebSocket(recNonWS, reqNonWS)

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
	sessionMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, bargeStore, nil, consultClient, tokenMgr,
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

	executeBargeInToolCalls(context.Background(), sess)

	if !waitForClearEvent(twilioWS) {
		t.Error("expected to see clear event on Twilio WS after barge-in/voicemail")
	}
}

func executeBargeInToolCalls(ctx context.Context, sess *MediaStreamSession) {
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_dtmf_good",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "123"},
	})
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_cal_good",
		Name: "consult_calendar",
		Args: map[string]any{"query": "schedule"},
	})
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_mem_good",
		Name: "consult_memory",
		Args: map[string]any{"query": "preferences"},
	})
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_dtmf_bad",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "INVALID_DIGIT"},
	})
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_cal_bad",
		Name: "consult_calendar",
		Args: map[string]any{"query": "bad"},
	})
	sess.executeToolCall(ctx, GeminiFunctionCall{
		ID:   "call_mem_bad",
		Name: "consult_memory",
		Args: map[string]any{"query": "bad"},
	})
}

func waitForClearEvent(ws *websocket.Conn) bool {
	for range 20 {
		_ = ws.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, data, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var evt map[string]any
		if err := json.Unmarshal(data, &evt); err == nil && evt["event"] == "clear" {
			return true
		}
	}
	return false
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

func TestMediaStreamSession_NilConnAndDialFallbacks(t *testing.T) {
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
	sessionMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, store, nil, nil, tokenMgr)
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

type mediaWebSocketModeTestCase struct {
	name          string
	callID        string
	streamSID     string
	providerSID   string
	direction     string
	callMode      string
	task          string
	voicemailMsg  string
	wantPromptSub string
	wantGreetSub  string
}

func TestHandleMediaWebSocket_LifecycleModes(t *testing.T) {
	t.Parallel()

	tests := []mediaWebSocketModeTestCase{
		{
			name:        "stop event terminates session",
			callID:      "call-stop-event",
			streamSID:   "MZ_stop",
			providerSID: "CA_stop",
		},
		{
			name:         "with store call configured",
			callID:       "call-store-configured",
			streamSID:    "MZ_store_cfg",
			providerSID:  "CA_store_cfg",
			direction:    "outbound",
			task:         "Confirm doctor visit",
			voicemailMsg: "Please call the account owner back",
		},
		{
			name:          "inbound mode sets receptionist prompt and greeting",
			callID:        "call-inbound-mode-test",
			streamSID:     "MZ_inbound_test",
			providerSID:   "CA_inbound_test",
			direction:     "inbound",
			wantPromptSub: "CALLBACK RECEPTIONIST MODE:",
			wantGreetSub:  "Answer the incoming call with a warm, brief greeting",
		},
		{
			name:          "notify mode sets notify prompt",
			callID:        "call-notify-mode-test",
			streamSID:     "MZ_notify_test",
			providerSID:   "CA_notify_test",
			direction:     "outbound",
			callMode:      "notify",
			task:          "Alert: package delivered",
			wantPromptSub: "NOTIFY MODE:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runMediaWebSocketModeCase(t, tc)
		})
	}
}

func runMediaWebSocketModeCase(t *testing.T, tc mediaWebSocketModeTestCase) {
	t.Helper()
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
	setupMediaWebSocketStoreCall(context.Background(), store, tc)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, store, nil, nil, tokenMgr,
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

	token, _ := tokenMgr.Issue(tc.callID, 30*time.Second)

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
		"streamSid": tc.streamSID,
		"start":     map[string]any{"streamSid": tc.streamSID, "callSid": tc.providerSID},
	})

	sess, err := sessionMgr.WaitForSession(context.Background(), tc.callID)
	if err != nil || sess == nil {
		t.Fatalf("WaitForSession failed: %v", err)
	}

	if tc.wantPromptSub != "" && !strings.Contains(sess.systemPrompt, tc.wantPromptSub) {
		t.Fatalf("system prompt missing %q:\n%s", tc.wantPromptSub, sess.systemPrompt)
	}
	if tc.wantGreetSub != "" && !strings.Contains(sess.openingGreeting, tc.wantGreetSub) {
		t.Fatalf("openingGreeting missing %q: %s", tc.wantGreetSub, sess.openingGreeting)
	}

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	_ = twilioWS.WriteJSON(map[string]any{
		"event": "stop",
		"stop":  map[string]any{"callSid": tc.providerSID},
	})

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for done")
	}
}

func exerciseBridgeSteerAndAMD(t *testing.T, sessionMgr *CallSessionManager, callID, providerSID string) {
	t.Helper()
	if err := sessionMgr.SteerCall(callID, "Focus on price confirmation", "guidance"); err != nil {
		t.Fatalf("SteerCall(guidance) error: %v", err)
	}
	if err := sessionMgr.SteerCall(callID, "Understood, thank you", "say"); err != nil {
		t.Fatalf("SteerCall(say) error: %v", err)
	}
	if err := sessionMgr.SteerCall(callID, "test_dtmf_trigger", "guidance"); err != nil {
		t.Fatalf("trigger dtmf err: %v", err)
	}
	if err := sessionMgr.SteerCall(callID, "test_calendar_trigger", "guidance"); err != nil {
		t.Fatalf("trigger calendar err: %v", err)
	}
	if err := sessionMgr.SteerCall(callID, "test_memory_trigger", "guidance"); err != nil {
		t.Fatalf("trigger memory err: %v", err)
	}
	if err := sessionMgr.HandleAMDWebhook(providerSID, "human"); err != nil {
		t.Fatalf("HandleAMDWebhook() error: %v", err)
	}
	if err := sessionMgr.SteerCall(callID, "test_end_trigger", "guidance"); err != nil {
		t.Fatalf("trigger end_call err: %v", err)
	}
}

func setupMediaWebSocketStoreCall(ctx context.Context, store *PGStore, tc mediaWebSocketModeTestCase) {
	switch tc.direction {
	case "outbound":
		_ = store.CreateOutboundCall(ctx, VoiceCall{
			CallID:     tc.callID,
			Direction:  "outbound",
			State:      "initiated",
			FromNumber: "+15105550100",
			ToNumber:   "+15105550199",
			Brief: CallBrief{
				Mode:             tc.callMode,
				Task:             tc.task,
				VoicemailMessage: tc.voicemailMsg,
			},
			StartedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}, 5)
	case "inbound":
		_ = store.CreateInboundCall(ctx, VoiceCall{
			CallID:     tc.callID,
			Direction:  "inbound",
			State:      "in-progress",
			FromNumber: "+15105550111",
			ToNumber:   "+15105550100",
			StartedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		})
	}
}

func TestCallSessionManager_WaitForSession(t *testing.T) {
	t.Parallel()

	tokenMgr := NewStreamTokenManager(time.Now)
	sessionMgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, nil, nil, nil, tokenMgr,
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
		InputTranscription: &GeminiTranscription{Text: "Caller hello "},
	})
	sess.handleServerContent(context.Background(), &GeminiServerContent{
		InputTranscription:  &GeminiTranscription{Text: "world"},
		OutputTranscription: &GeminiTranscription{Text: "Hi from "},
	})
	sess.handleServerContent(context.Background(), &GeminiServerContent{
		OutputTranscription: &GeminiTranscription{Text: "Prospera"},
		GenerationComplete:  true,
	})
	sess.handleServerContent(context.Background(), &GeminiServerContent{
		Interrupted: true,
		ModelTurn: &GeminiModelTurn{
			Parts: []GeminiPart{
				{
					Text: "Assistant turn response",
				},
				{
					Text:    "Hidden internal thought",
					Thought: true,
				},
				{
					InlineData: &GeminiInlineData{
						MimeType: "audio/pcm;rate=24000",
						Data:     base64.StdEncoding.EncodeToString(make([]byte, 480)),
					},
				},
				{
					MimeType: "audio/pcm;rate=24000",
					Data:     base64.StdEncoding.EncodeToString(make([]byte, 480)),
				},
			},
		},
	})

	sess.handleServerContent(context.Background(), &GeminiServerContent{
		OutputTranscription: &GeminiTranscription{Text: "Closing words before tool call"},
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

func TestMediaStreamSession_RecordingWAVAndManager(t *testing.T) {
	t.Parallel()

	mgr := NewCallSessionManager(staticConfig{cfg: &Config{}}, nil, nil, nil, nil)
	if wav := mgr.GetRecordingWAV("missing"); wav != nil {
		t.Fatalf("expected nil wav for missing call, got %d bytes", len(wav))
	}

	sess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-rec-1"})
	sess.mu.Lock()
	sess.streamSID = "MZ_rec_1"
	sess.mu.Unlock()

	sess.recordInboundPCM([]int16{1000, 2000})
	sess.sendTwilioMedia(PCM16ToMuLaw([]int16{500, 500, 500, 500}))
	sess.recordInboundPCM([]int16{300, 400})
	sess.sendTwilioClear()

	mgr.registerSession("call-rec-1", sess)
	if activeWAV := mgr.GetRecordingWAV("call-rec-1"); len(activeWAV) <= 44 {
		t.Fatalf("expected active session WAV > 44 bytes, got %d", len(activeWAV))
	}

	for i := range 33 {
		id := "call-cap-" + strconv.Itoa(i)
		s := NewMediaStreamSession(MediaStreamSessionConfig{CallID: id})
		s.recordInboundPCM([]int16{100})
		mgr.registerSession(id, s)
		mgr.deregisterSession(id)
	}

	mgr.deregisterSession("call-rec-1")
	deregWAV := mgr.GetRecordingWAV("call-rec-1")
	if len(deregWAV) <= 44 {
		t.Fatalf("expected deregistered session WAV > 44 bytes, got %d", len(deregWAV))
	}
	if secondRead := mgr.GetRecordingWAV("call-rec-1"); secondRead != nil {
		t.Fatalf("expected consumed recording to return nil on second read, got %d bytes", len(secondRead))
	}

	capSess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-max-cap"})
	capSess.mu.Lock()
	capSess.inCursor = maxRecordingSamples - 1
	capSess.outCursor = maxRecordingSamples - 1
	capSess.recordedPCM = make([]int16, maxRecordingSamples-1)
	capSess.mu.Unlock()
	capSess.recordInboundPCM([]int16{10, 20, 30})
	capSess.recordOutboundUlaw(PCM16ToMuLaw([]int16{40, 50}))
	if len(capSess.recordedPCM) != maxRecordingSamples {
		t.Fatalf("len(recordedPCM) = %d, want %d", len(capSess.recordedPCM), maxRecordingSamples)
	}

	overlapSess := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:       "call-overlap-missing",
		Store:        setUpTestStore(t),
		TwilioClient: &mockTwilioClient{hangupErr: errors.New("hangup failed")},
	})
	overlapSess.SetProviderCallSID("CA_err_1")
	overlapSess.mu.Lock()
	overlapSess.inCursor = 0
	overlapSess.outCursor = 0
	overlapSess.recordedPCM = []int16{100, 200}
	overlapSess.mu.Unlock()
	overlapSess.recordOutboundUlaw(PCM16ToMuLaw([]int16{50}))
	overlapSess.handleTwilioHangupMark(context.Background())
	overlapSess.amd.OnAMDWebhook("human")
	overlapSess.handleInboundTwilioMedia(base64.StdEncoding.EncodeToString(make([]byte, 160)))
}
