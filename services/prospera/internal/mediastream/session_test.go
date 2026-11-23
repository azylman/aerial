package mediastream

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

	"github.com/azylman/aerial/services/prospera/internal/audio"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/testutil"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

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

// newBridgeTestSession adapts a manager session request to the concrete bridge implementation.
func newBridgeTestSession(cfg SessionConfig) Session {
	return newBridgeMediaSession(cfg)
}

// newBridgeMediaSession builds the concrete bridge for a manager session request.
func newBridgeMediaSession(cfg SessionConfig) *MediaStreamSession {
	return NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:           cfg.CallID,
		TwilioConn:       cfg.Conn,
		Config:           cfg.Config,
		Store:            cfg.Store,
		TwilioClient:     cfg.TwilioClient,
		ConsultClient:    cfg.ConsultClient,
		OpeningGreeting:  cfg.OpeningGreeting,
		VoicemailMessage: cfg.VoicemailMessage,
		SystemPrompt:     cfg.SystemPrompt,
		Language:         cfg.Language,
		PacerInterval:    cfg.PacerInterval,
		GeminiDialer:     cfg.GeminiDialer,
	})
}

// signalingBridgeSession announces its bridge on ran before delegating Run.
// The manager calls Run only after RegisterSession, so a receive on ran
// means the session is registered and live.
type signalingBridgeSession struct {
	*MediaStreamSession

	ran chan<- *MediaStreamSession
}

func (s signalingBridgeSession) Run(ctx context.Context) {
	s.ran <- s.MediaStreamSession
	s.MediaStreamSession.Run(ctx)
}

// newSignalingBridgeFactory returns a bridge factory plus the channel its
// sessions report on when Run starts.
func newSignalingBridgeFactory() (SessionFactory, <-chan *MediaStreamSession) {
	ran := make(chan *MediaStreamSession, 1)
	factory := func(cfg SessionConfig) Session {
		return signalingBridgeSession{MediaStreamSession: newBridgeMediaSession(cfg), ran: ran}
	}
	return factory, ran
}

// requireBridgeSession waits for the bridge session built by a signaling factory to start running.
func requireBridgeSession(t *testing.T, ran <-chan *MediaStreamSession) *MediaStreamSession {
	t.Helper()
	select {
	case sess := <-ran:
		return sess
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the bridge session to run")
		return nil
	}
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

	geminiWSURL := setupLifecycleGeminiServer(t, recordGeminiMsg)

	cfg := &config.Config{
		GeminiLiveModel:     "gemini-3.8-live",
		GeminiFallbackModel: "gemini-3.1-flash-live-preview",
		GeminiVoiceName:     "Kore",
	}

	twilioClient := &fakes.FakeTwilio{}
	consultClient := &mockConsultClient{
		calendarResp: "Dentist appointment at 2pm",
		memoryResp:   "Home address: 123 Example St",
	}
	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)

	bridgeFactory, bridgeRan := newSignalingBridgeFactory()
	sessionMgr := NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, nil, twilioClient, consultClient, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
		WithSessionFactory(bridgeFactory),
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
	if err = twilioWS.WriteJSON(startMsg); err != nil {
		t.Fatalf("write start event: %v", err)
	}

	sess := requireBridgeSession(t, bridgeRan)

	select {
	case <-sess.Started():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for session start event")
	}

	if got := sess.ProviderCallSID(); got != "CA_twilio_123" {
		t.Fatalf("ProviderCallSID() = %q, want CA_twilio_123 from start event", got)
	}

	injectTwilioAudioFrames(t, twilioWS)

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

func setupLifecycleGeminiServer(t *testing.T, recordGeminiMsg func(string)) string {
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

		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			return
		}

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
	return "ws" + strings.TrimPrefix(geminiServer.URL, "http")
}

func respondLifecycleGeminiEvent(conn *websocket.Conn, msgStr string) {
	switch {
	case strings.Contains(msgStr, "Call connected"):
		pcm24 := make([]byte, 4800)
		if err := conn.WriteJSON(map[string]any{
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
		}); err != nil {
			return
		}
	case strings.Contains(msgStr, "test_dtmf_trigger"):
		if err := conn.WriteJSON(map[string]any{
			"toolCall": map[string]any{
				"functionCalls": []map[string]any{
					{
						"id":   "call_dtmf_1",
						"name": "send_dtmf",
						"args": map[string]any{"digits": "12w3"},
					},
				},
			},
		}); err != nil {
			return
		}
	case strings.Contains(msgStr, "test_calendar_trigger"):
		if err := conn.WriteJSON(map[string]any{
			"toolCall": map[string]any{
				"functionCalls": []map[string]any{
					{
						"id":   "call_cal_1",
						"name": "consult_calendar",
						"args": map[string]any{"query": "dentist", "start_iso": "2026-10-26T00:00:00Z"},
					},
				},
			},
		}); err != nil {
			return
		}
	case strings.Contains(msgStr, "test_memory_trigger"):
		if err := conn.WriteJSON(map[string]any{
			"toolCall": map[string]any{
				"functionCalls": []map[string]any{
					{
						"id":   "call_mem_1",
						"name": "consult_memory",
						"args": map[string]any{"query": "address"},
					},
				},
			},
		}); err != nil {
			return
		}
	case strings.Contains(msgStr, "test_end_trigger"):
		if err := conn.WriteJSON(map[string]any{
			"toolCall": map[string]any{
				"functionCalls": []map[string]any{
					{
						"id":   "call_end_1",
						"name": "end_call",
						"args": map[string]any{"reason": "task_completed"},
					},
				},
			},
		}); err != nil {
			return
		}
	}
}

func injectTwilioAudioFrames(t *testing.T, ws *websocket.Conn) {
	t.Helper()
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
		if err := ws.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": loudPayload,
			},
		}); err != nil {
			t.Fatalf("WriteJSON media frame failed: %v", err)
		}
	}
	for range 31 {
		if err := ws.WriteJSON(map[string]any{
			"event":     "media",
			"streamSid": "MZ123",
			"media": map[string]any{
				"payload": quietPayload,
			},
		}); err != nil {
			t.Fatalf("WriteJSON media frame failed: %v", err)
		}
	}
}

func waitForHangupMark(t *testing.T, ws *websocket.Conn) bool {
	t.Helper()
	var receivedMedia bool
	for {
		if err := ws.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline failed: %v", err)
		}
		_, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("reading from Twilio WS failed before hangup mark: %v", err)
		}
		var evt map[string]any
		if err := json.Unmarshal(data, &evt); err != nil {
			continue
		}
		if evt["event"] == "media" {
			receivedMedia = true
			if evt["streamSid"] != "MZ123" {
				t.Errorf("outbound media frame streamSid = %v, want MZ123", evt["streamSid"])
			}
		}
		if markObj, ok := evt["mark"].(map[string]any); ok && evt["event"] == "mark" && markObj["name"] == "hangup" {
			if err := ws.WriteJSON(map[string]any{
				"event":     "mark",
				"streamSid": "MZ123",
				"mark":      map[string]any{"name": "hangup"},
			}); err != nil {
				t.Fatalf("WriteJSON hangup mark failed: %v", err)
			}
			return receivedMedia
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

		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}

		if c == 1 {
			_ = conn.Close()
			return
		}

		reconnectedOnce.Do(func() {
			close(reconnected)
		})

		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			return
		}
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")

	cfg := &config.Config{
		GeminiLiveModel:     "gemini-3.8-live",
		GeminiFallbackModel: "gemini-3.1-flash-live-preview",
		GeminiVoiceName:     "Kore",
	}

	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	sessionMgr := NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, nil, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
		WithSessionFactory(newBridgeTestSession),
	)

	token, err := tokenMgr.Issue("call-reconnect", 30*time.Second)
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}

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

	if err := twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_recon",
		"start":     map[string]any{"streamSid": "MZ_recon", "callSid": "CA_recon"},
	}); err != nil {
		t.Fatalf("WriteJSON start failed: %v", err)
	}

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

func TestMediaStream_BargeInAndVoicemail(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			return
		}

		if err := conn.WriteJSON(map[string]any{
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
		}); err != nil {
			return
		}

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(geminiServer.Close)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)

	consultClient := &mockConsultClient{
		calendarErr: errors.New("calendar failed"),
		memoryErr:   errors.New("memory failed"),
	}

	bargeStore := testutil.SetUpTestStore(t)
	bridgeFactory, bridgeRan := newSignalingBridgeFactory()
	sessionMgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, bargeStore, nil, consultClient, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
		WithSessionFactory(bridgeFactory),
	)

	token, err := tokenMgr.Issue("call-bargein", 30*time.Second)
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}

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

	if err := twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": "MZ_barge",
		"start":     map[string]any{"streamSid": "MZ_barge", "callSid": "CA_barge"},
	}); err != nil {
		t.Fatalf("WriteJSON start failed: %v", err)
	}

	sess := requireBridgeSession(t, bridgeRan)

	select {
	case <-sess.Started():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for start")
	}

	sess.handleAMDAction(audio.AMDActionReleaseGreeting)
	sess.handleAMDAction(audio.AMDActionSwitchToVoicemail)
	sess.handleAMDAction(audio.AMDActionSpeakVoicemailAndHangup)
	sess.HandleAMD("human")

	if err := sess.Steer("guidance instruction", "guidance"); err != nil {
		t.Fatalf("Steer failed: %v", err)
	}
	if err := sess.Steer("say instruction", "say"); err != nil {
		t.Fatalf("Steer failed: %v", err)
	}

	sess.handleInboundTwilioMedia("")
	sess.handleInboundTwilioMedia("invalid-base64!!!")

	executeBargeInToolCalls(context.Background(), sess)

	if !waitForClearEvent(twilioWS) {
		t.Error("expected to see clear event on Twilio WS after barge-in/voicemail")
	}
}

func executeBargeInToolCalls(ctx context.Context, sess *MediaStreamSession) {
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_dtmf_good",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "123"},
	})
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_cal_good",
		Name: "consult_calendar",
		Args: map[string]any{"query": "schedule"},
	})
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_mem_good",
		Name: "consult_memory",
		Args: map[string]any{"query": "preferences"},
	})
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_dtmf_bad",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "INVALID_DIGIT"},
	})
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_cal_bad",
		Name: "consult_calendar",
		Args: map[string]any{"query": "bad"},
	})
	sess.executeToolCall(ctx, gemini.GeminiFunctionCall{
		ID:   "call_mem_bad",
		Name: "consult_memory",
		Args: map[string]any{"query": "bad"},
	})
}

func waitForClearEvent(ws *websocket.Conn) bool {
	for range 20 {
		if err := ws.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return false
		}
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

func newMockGeminiEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			return
		}
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func runMediaWebSocketModeCase(t *testing.T, tc mediaWebSocketModeTestCase) {
	t.Helper()
	geminiServer := newMockGeminiEchoServer(t)

	st := testutil.SetUpTestStore(t)
	setupMediaWebSocketStoreCall(t, st, tc)

	geminiWSURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
	tokenMgr := twilio.NewStreamTokenManager(time.Now, nil)
	bridgeFactory, bridgeRan := newSignalingBridgeFactory()
	sessionMgr := NewCallSessionManager(fakes.StaticConfig{Cfg: &config.Config{}}, st, nil, nil, tokenMgr,
		WithPacerInterval(1*time.Millisecond),
		WithGeminiDialer(func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			dialer := websocket.DefaultDialer
			conn, resp, err := dialer.DialContext(ctx, geminiWSURL, nil)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			return conn, err
		}),
		WithSessionFactory(bridgeFactory),
	)

	token, err := tokenMgr.Issue(tc.callID, 30*time.Second)
	if err != nil {
		t.Fatalf("Issue failed: %v", err)
	}

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

	if err := twilioWS.WriteJSON(map[string]any{
		"event":     "start",
		"streamSid": tc.streamSID,
		"start":     map[string]any{"streamSid": tc.streamSID, "callSid": tc.providerSID},
	}); err != nil {
		t.Fatalf("WriteJSON start failed: %v", err)
	}

	sess := requireBridgeSession(t, bridgeRan)

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

	if err := twilioWS.WriteJSON(map[string]any{
		"event": "stop",
		"stop":  map[string]any{"callSid": tc.providerSID},
	}); err != nil {
		t.Fatalf("WriteJSON stop failed: %v", err)
	}

	select {
	case <-sess.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for done")
	}
}

func exerciseBridgeSteerAndAMD(t *testing.T, sessionMgr *CallSessionManager, callID, providerSID string) {
	t.Helper()
	if err := sessionMgr.SteerCall(t.Context(), callID, "Focus on price confirmation", "guidance"); err != nil {
		t.Fatalf("SteerCall(guidance) error: %v", err)
	}
	if err := sessionMgr.SteerCall(t.Context(), callID, "Understood, thank you", "say"); err != nil {
		t.Fatalf("SteerCall(say) error: %v", err)
	}
	if err := sessionMgr.SteerCall(t.Context(), callID, "test_dtmf_trigger", "guidance"); err != nil {
		t.Fatalf("trigger dtmf err: %v", err)
	}
	if err := sessionMgr.SteerCall(t.Context(), callID, "test_calendar_trigger", "guidance"); err != nil {
		t.Fatalf("trigger calendar err: %v", err)
	}
	if err := sessionMgr.SteerCall(t.Context(), callID, "test_memory_trigger", "guidance"); err != nil {
		t.Fatalf("trigger memory err: %v", err)
	}
	if err := sessionMgr.HandleAMDWebhook(t.Context(), providerSID, "human"); err != nil {
		t.Fatalf("HandleAMDWebhook() error: %v", err)
	}
	if err := sessionMgr.SteerCall(t.Context(), callID, "test_end_trigger", "guidance"); err != nil {
		t.Fatalf("trigger end_call err: %v", err)
	}
}

func setupMediaWebSocketStoreCall(t *testing.T, st *store.PGStore, tc mediaWebSocketModeTestCase) {
	t.Helper()
	ctx := t.Context()
	switch tc.direction {
	case "outbound":
		if err := st.CreateOutboundCall(ctx, model.VoiceCall{
			CallID:     tc.callID,
			Direction:  "outbound",
			State:      "initiated",
			FromNumber: "+15105550100",
			ToNumber:   "+15105550199",
			Brief: model.CallBrief{
				Mode:             tc.callMode,
				Task:             tc.task,
				VoicemailMessage: tc.voicemailMsg,
			},
			StartedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}, 5); err != nil {
			t.Fatalf("CreateOutboundCall failed: %v", err)
		}
	case "inbound":
		if err := st.CreateInboundCall(ctx, model.VoiceCall{
			CallID:     tc.callID,
			Direction:  "inbound",
			State:      "in-progress",
			FromNumber: "+15105550111",
			ToNumber:   "+15105550100",
			StartedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}); err != nil {
			t.Fatalf("CreateInboundCall failed: %v", err)
		}
	}
}
