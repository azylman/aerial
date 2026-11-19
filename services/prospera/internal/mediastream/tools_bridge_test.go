package mediastream

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/azylman/aerial/services/prospera/internal/audio"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/testutil"
)

func TestMediaStream_DirectHandlers(t *testing.T) {
	t.Parallel()

	st := testutil.SetUpTestStore(t)
	consultClient := &mockConsultClient{
		calendarResp: "Confirmed 3pm meeting",
		memoryResp:   "Primary contact: Jane Doe",
	}

	sess := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:        "call-direct-test",
		Store:         st,
		ConsultClient: consultClient,
	})

	sess.handleServerContent(context.Background(), &gemini.GeminiServerContent{
		InputTranscription: &gemini.GeminiTranscription{Text: "Caller hello "},
	})
	sess.handleServerContent(context.Background(), &gemini.GeminiServerContent{
		InputTranscription:  &gemini.GeminiTranscription{Text: "world"},
		OutputTranscription: &gemini.GeminiTranscription{Text: "Hi from "},
	})
	sess.handleServerContent(context.Background(), &gemini.GeminiServerContent{
		OutputTranscription: &gemini.GeminiTranscription{Text: "Prospera"},
		GenerationComplete:  true,
	})
	sess.handleServerContent(context.Background(), &gemini.GeminiServerContent{
		Interrupted: true,
		ModelTurn: &gemini.GeminiModelTurn{
			Parts: []gemini.GeminiPart{
				{
					Text: "Assistant turn response",
				},
				{
					Text:    "Hidden internal thought",
					Thought: true,
				},
				{
					InlineData: &gemini.GeminiInlineData{
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

	sess.handleServerContent(context.Background(), &gemini.GeminiServerContent{
		OutputTranscription: &gemini.GeminiTranscription{Text: "Closing words before tool call"},
	})
	sess.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_dtmf_good",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "123#"},
	})
	sess.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_dtmf_bad",
		Name: "send_dtmf",
		Args: map[string]any{"digits": "BAD"},
	})

	sess.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_cal_good",
		Name: "consult_calendar",
		Args: map[string]any{"query": "schedule"},
	})

	sess.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_mem_good",
		Name: "consult_memory",
		Args: map[string]any{"query": "preferences"},
	})

	sess.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
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
	sessErr.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_cal_err",
		Name: "consult_calendar",
		Args: map[string]any{"query": "error"},
	})
	sessErr.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_mem_err",
		Name: "consult_memory",
		Args: map[string]any{"query": "error"},
	})

	sessNil := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "call-nil-client",
	})
	sessNil.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_cal_nil",
		Name: "consult_calendar",
		Args: map[string]any{"query": "nil"},
	})
	sessNil.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_mem_nil",
		Name: "consult_memory",
		Args: map[string]any{"query": "nil"},
	})

	sess.InitiateHangup("test_hangup")
	sess.enqueueOutboundUlaw(make([]byte, 1005*160))
}

func TestMediaStreamSession_NilConnAndDialFallbacks(t *testing.T) {
	t.Parallel()

	sessNoDialer := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-nodialer",
		Config: nil,
	})
	sessNoDialerCfg := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-nodialer-key",
		Config: &config.Config{GeminiAPIKey: "key"},
	})

	// A canceled context fails the dial before any network use, keeping the
	// no-dialer fallback hermetic while covering both Config branches.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, sess := range []*MediaStreamSession{sessNoDialer, sessNoDialerCfg} {
		if _, err := sess.dialGemini(canceledCtx, "model"); !errors.Is(err, context.Canceled) {
			t.Fatalf("dialGemini() err = %v, want context canceled", err)
		}
	}

	sessFailDial := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-faildial",
		GeminiDialer: func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			return nil, errors.New("dial failed")
		},
	})
	sessFailDial.Run(context.Background())
	select {
	case <-sessFailDial.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for failed-dial session to complete")
	}

	sessNilConsult := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-nil-consult",
	})
	sessNilConsult.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_cal_nil",
		Name: "consult_calendar",
		Args: map[string]any{"query": "q"},
	})
	sessNilConsult.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_mem_nil",
		Name: "consult_memory",
		Args: map[string]any{"query": "q"},
	})
	sessNilConsult.executeToolCall(context.Background(), gemini.GeminiFunctionCall{
		ID:   "call_end_nil",
		Name: "end_call",
		Args: map[string]any{"reason": "done"},
	})

	sessFailReconnect := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID: "c-fail-recon",
		GeminiDialer: func(ctx context.Context, modelName string) (*websocket.Conn, error) {
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

func TestMediaStreamSession_DisconnectedAMDActions(t *testing.T) {
	t.Parallel()
	sessNoGemini := &MediaStreamSession{
		openingGreeting: "Hello from opening",
	}
	sessNoGemini.handleAMDAction(audio.AMDActionSwitchToVoicemail)
	sessNoGemini.handleAMDAction(audio.AMDActionSpeakVoicemailAndHangup)
	sessNoGemini.handleAMDAction(audio.AMDActionReleaseGreeting)
	if !sessNoGemini.greetingReleased {
		t.Error("expected greeting released after AMDActionReleaseGreeting")
	}

	sessWithDialFail := &MediaStreamSession{
		geminiDialer: func(ctx context.Context, modelName string) (*websocket.Conn, error) {
			return nil, errors.New("dial fail")
		},
	}
	if ok := sessWithDialFail.reconnectGemini(context.Background()); ok {
		t.Fatal("expected reconnect failure")
	}
}

func TestMediaStreamSession_TestConnSends(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	received := make(chan string, 4)
	geminiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- string(data)
	}))
	t.Cleanup(geminiServer.Close)

	wsURL := "ws" + strings.TrimPrefix(geminiServer.URL, "http")
	clientConn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial loopback gemini failed: %v", err)
	}
	t.Cleanup(func() { _ = clientConn.Close() })

	sessLive := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-testconn-live"})
	sessLive.SetGeminiConnForTest(clientConn)
	if err := sessLive.Steer("say hello now", "say"); err != nil {
		t.Fatalf("Steer() err = %v, want nil", err)
	}

	if got := readGeminiPayload(t, received); !strings.Contains(got, "say immediately") || !strings.Contains(got, "say hello now") {
		t.Fatalf("gemini payload = %s, want say-immediately instruction", got)
	}
}

func readGeminiPayload(t *testing.T, received <-chan string) string {
	t.Helper()
	select {
	case msg := <-received:
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for gemini payload")
		return ""
	}
}

func TestMediaStreamSession_HandleTwilioHangupMark(t *testing.T) {
	t.Parallel()

	st := testutil.SetUpTestStore(t)
	_ = st.CreateOutboundCall(context.Background(), model.VoiceCall{
		CallID:     "call-hangup-stored",
		Direction:  "outbound",
		State:      "in-progress",
		FromNumber: "+15105550100",
		ToNumber:   "+15105550199",
		Brief:      model.CallBrief{Task: "Confirm doctor visit"},
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}, 5)

	failingClient := &mockTwilioClient{hangupErr: errors.New("hangup boom")}
	plainClient := &mockTwilioClient{}

	tests := []struct {
		name      string
		callID    string
		sid       string
		client    *mockTwilioClient
		withStore bool
	}{
		{name: "hangup error surfaces caller sid", callID: "call-hangup-err", sid: "CA_hang_err", client: failingClient},
		{name: "stored call transitions to completed", callID: "call-hangup-stored", sid: "CA_hang_ok", client: plainClient, withStore: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: tc.callID})
			sess.twilioClient = tc.client
			if tc.withStore {
				sess.store = st
			}
			sess.SetProviderCallSID(tc.sid)
			sess.handleTwilioHangupMark(context.Background())
			if len(tc.client.hungUpCallSIDs) != 1 || tc.client.hungUpCallSIDs[0] != tc.sid {
				t.Fatalf("hungUpCallSIDs = %q, want [%q]", tc.client.hungUpCallSIDs, tc.sid)
			}
			if tc.withStore {
				got, err := st.GetCall(context.Background(), tc.callID)
				if err != nil {
					t.Fatalf("GetCall() err = %v, want nil", err)
				}
				if got == nil || got.State != "completed" {
					t.Fatalf("stored state = %v, want completed", got)
				}
			}
		})
	}
}

func TestMediaStreamSession_ClearTruncation(t *testing.T) {
	t.Parallel()

	sess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-clear-trunc"})
	sess.recordedPCM = make([]int16, 320)
	sess.inCursor = 160
	sess.outCursor = 200
	sess.sendTwilioClear()
	if sess.outCursor != 160 {
		t.Fatalf("outCursor = %d, want 160 after clear truncation", sess.outCursor)
	}
	if len(sess.recordedPCM) != 160 {
		t.Fatalf("len(recordedPCM) = %d, want 160 after clear truncation", len(sess.recordedPCM))
	}
}

func TestMediaStreamSession_RecordingEdges(t *testing.T) {
	t.Parallel()

	sess := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-rec-edge"})
	sess.recordedPCM = make([]int16, 4)
	sess.inCursor = 1
	sess.recordInboundPCM([]int16{7, 8})
	if sess.recordedPCM[1] != 7 {
		t.Fatalf("recordedPCM[1] = %d, want 7", sess.recordedPCM[1])
	}
	if sess.recordedPCM[2] != 8 {
		t.Fatalf("recordedPCM[2] = %d, want 8", sess.recordedPCM[2])
	}
	if len(sess.recordedPCM) != 4 {
		t.Fatalf("len(recordedPCM) = %d, want 4", len(sess.recordedPCM))
	}
	sess.inCursor = 4
	sess.recordInboundPCM([]int16{5})
	if len(sess.recordedPCM) != 5 {
		t.Fatalf("len(recordedPCM) = %d, want 5 after append", len(sess.recordedPCM))
	}

	sessFull := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-rec-full"})
	sessFull.inCursor = maxRecordingSamples
	sessFull.recordInboundPCM([]int16{9})
	if sessFull.inCursor != maxRecordingSamples {
		t.Fatalf("inCursor = %d, want cap %d", sessFull.inCursor, maxRecordingSamples)
	}

	sessOut := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-rec-out"})
	sessOut.recordedPCM = make([]int16, 4)
	sessOut.recordOutboundUlaw([]byte{0xFE})
	if len(sessOut.recordedPCM) != 4 {
		t.Fatalf("len(recordedPCM) = %d, want 4", len(sessOut.recordedPCM))
	}

	sessOutFull := NewMediaStreamSession(MediaStreamSessionConfig{CallID: "call-rec-out-full"})
	sessOutFull.inCursor = maxRecordingSamples
	sessOutFull.outCursor = maxRecordingSamples
	sessOutFull.recordOutboundUlaw([]byte{0xFE})
	if sessOutFull.outCursor != maxRecordingSamples {
		t.Fatalf("outCursor = %d, want cap %d", sessOutFull.outCursor, maxRecordingSamples)
	}
}
