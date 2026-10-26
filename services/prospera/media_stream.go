package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SessionManagerOption configures CallSessionManager.
type SessionManagerOption func(*CallSessionManager)

// CallSessionManager coordinates active Twilio Media Stream <-> Gemini Live sessions.
type CallSessionManager struct {
	mu             sync.RWMutex
	cfg            *Config
	store          Store
	twilioClient   TwilioClient
	consultClient  ContextConsultClient
	tokenMgr       *StreamTokenManager
	pacerInterval  time.Duration
	geminiDialer   func(ctx context.Context, model string) (*websocket.Conn, error)
	sessions       map[string]*MediaStreamSession
	sessionWaiters map[string]chan *MediaStreamSession
	upgrader       websocket.Upgrader
}

// NewCallSessionManager initializes a CallSessionManager.
func NewCallSessionManager(
	cfg *Config,
	store Store,
	twilioClient TwilioClient,
	consultClient ContextConsultClient,
	tokenMgr *StreamTokenManager,
	opts ...SessionManagerOption,
) *CallSessionManager {
	if tokenMgr == nil {
		tokenMgr = NewStreamTokenManager(time.Now)
	}

	m := &CallSessionManager{
		cfg:            cfg,
		store:          store,
		twilioClient:   twilioClient,
		consultClient:  consultClient,
		tokenMgr:       tokenMgr,
		pacerInterval:  20 * time.Millisecond,
		sessions:       make(map[string]*MediaStreamSession),
		sessionWaiters: make(map[string]chan *MediaStreamSession),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

// GetSession retrieves an active session by callID.
func (m *CallSessionManager) GetSession(callID string) (*MediaStreamSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.sessions[callID]
	return sess, ok
}

// GetSessionByProviderSID searches for an active session with a matching providerCallSID.
func (m *CallSessionManager) GetSessionByProviderSID(providerCallSID string) (*MediaStreamSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions {
		if sess.ProviderCallSID() == providerCallSID {
			return sess, true
		}
	}
	return nil, false
}

func (m *CallSessionManager) registerSession(callID string, sess *MediaStreamSession) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[callID] = sess
	if ch, ok := m.sessionWaiters[callID]; ok {
		select {
		case ch <- sess:
		default:
		}
		delete(m.sessionWaiters, callID)
	}
}

func (m *CallSessionManager) deregisterSession(callID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, callID)
}

// SteerCall injects an instruction turn into an active call session.
func (m *CallSessionManager) SteerCall(callID, message, mode string) error {
	sess, ok := m.GetSession(callID)
	if !ok {
		return fmt.Errorf("session: call %q is not currently active", callID)
	}

	if m.store != nil {
		if err := m.store.AppendOwnerInstruction(context.Background(), callID, OwnerInstruction{
			Instruction: message,
			Mode:        mode,
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
			log.Printf("prospera: append owner instruction: %v", err)
		}
	}

	return sess.Steer(message, mode)
}

// HandleAMDWebhook handles an inbound AMD webhook by routing to the matching session.
func (m *CallSessionManager) HandleAMDWebhook(providerCallSID, answeredBy string) error {
	sess, ok := m.GetSessionByProviderSID(providerCallSID)
	if !ok {
		// Attempt to resolve callID from store
		if m.store != nil {
			call, err := m.store.GetCallByProviderSID(context.Background(), providerCallSID)
			if err == nil && call != nil {
				sess, ok = m.GetSession(call.CallID)
			}
		}
	}
	if !ok || sess == nil {
		return fmt.Errorf("session: no active session found for provider call sid %q", providerCallSID)
	}
	sess.HandleAMD(answeredBy)
	return nil
}

// EndCall terminates an active call session.
func (m *CallSessionManager) EndCall(callID string) error {
	sess, ok := m.GetSession(callID)
	if !ok {
		return fmt.Errorf("session: call %q not found", callID)
	}
	sess.InitiateHangup("operator_requested")
	return nil
}

// HandleMediaWebSocket upgrades and handles the Twilio Media Stream connection.
func (m *CallSessionManager) HandleMediaWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	callID, ok := m.tokenMgr.Consume(token)
	if !ok || callID == "" {
		http.Error(w, "unauthorized stream token", http.StatusUnauthorized)
		return
	}

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("prospera: websocket upgrade failed: %v", err)
		return
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			log.Printf("prospera: connection close err: %v", closeErr)
		}
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var call *VoiceCall
	if m.store != nil {
		var getErr error
		call, getErr = m.store.GetCall(ctx, callID)
		if getErr != nil {
			log.Printf("prospera: get call %s: %v", callID, getErr)
		}
	}

	var openingGreeting, voicemailMsg, systemPrompt string
	if call != nil {
		openingGreeting = call.Brief.Task
		if openingGreeting == "" {
			if call.Direction == "inbound" {
				openingGreeting = "Hello, thank you for calling. How can I help you today?"
			} else {
				openingGreeting = "Hello, I am calling to help with an inquiry."
			}
		}
		voicemailMsg = ResolveVoicemailMessage(call.Brief, openingGreeting)
		mode := ModeConversation
		if call.Brief.Mode == string(ModeNotify) {
			mode = ModeNotify
		} else if call.Direction == "inbound" || call.CallbackOfCallID != "" {
			mode = ModeCallback
		}
		systemPrompt = BuildSystemPrompt(call.Brief, mode, nil)
	} else {
		openingGreeting = "Hello, I am calling to help with an inquiry."
		voicemailMsg = openingGreeting
		systemPrompt = "You are Aerial, an assistant making or handling a phone call. Speak like a warm, natural human on a live phone call—use a relaxed conversational cadence, natural contractions, and everyday phrasing rather than stiff or scripted readback. Focus directly on the task without introducing yourself upfront. If asked who is calling, say your name is Aerial and phrase your role naturally for the situation (for example, '<Name>'s assistant' if their name is known); only disclose that you are an AI assistant if explicitly asked."
	}

	sess := NewMediaStreamSession(MediaStreamSessionConfig{
		CallID:           callID,
		TwilioConn:       conn,
		Config:           m.cfg,
		Store:            m.store,
		TwilioClient:     m.twilioClient,
		ConsultClient:    m.consultClient,
		OpeningGreeting:  openingGreeting,
		VoicemailMessage: voicemailMsg,
		SystemPrompt:     systemPrompt,
		PacerInterval:    m.pacerInterval,
		GeminiDialer:     m.geminiDialer,
	})

	m.registerSession(callID, sess)
	defer m.deregisterSession(callID)

	sess.Run(ctx)
}

// MediaStreamSessionConfig defines the construction parameters for MediaStreamSession.
type MediaStreamSessionConfig struct {
	CallID           string
	TwilioConn       *websocket.Conn
	Config           *Config
	Store            Store
	TwilioClient     TwilioClient
	ConsultClient    ContextConsultClient
	OpeningGreeting  string
	VoicemailMessage string
	SystemPrompt     string
	PacerInterval    time.Duration
	GeminiDialer     func(ctx context.Context, model string) (*websocket.Conn, error)
}

// MediaStreamSession coordinates the live audio bridge between Twilio and Gemini Live.
type MediaStreamSession struct {
	mu                  sync.Mutex
	writeMu             sync.Mutex
	geminiWriteMu       sync.Mutex
	callID              string
	providerCallSID     string
	streamSID           string
	twilioConn          *websocket.Conn
	geminiConn          *websocket.Conn
	cfg                 *Config
	store               Store
	twilioClient        TwilioClient
	consultClient       ContextConsultClient
	openingGreeting     string
	voicemailMessage    string
	systemPrompt        string
	pacerInterval       time.Duration
	geminiDialer        func(ctx context.Context, model string) (*websocket.Conn, error)
	amd                 *AMDGreetingController
	outboundFrames      chan []byte
	started             chan struct{}
	startedOnce         sync.Once
	doneCh              chan struct{}
	doneOnce            sync.Once
	greetingReleased    bool
	hangupPending       bool
	hangupReason        string
	hangupMarkSent      bool
	closed              bool
	cancel              context.CancelFunc
	inputTranscriptBuf  strings.Builder
	outputTranscriptBuf strings.Builder
}

// NewMediaStreamSession creates a new MediaStreamSession.
func NewMediaStreamSession(cfg MediaStreamSessionConfig) *MediaStreamSession {
	if cfg.PacerInterval <= 0 {
		cfg.PacerInterval = 20 * time.Millisecond
	}

	return &MediaStreamSession{
		callID:           cfg.CallID,
		twilioConn:       cfg.TwilioConn,
		cfg:              cfg.Config,
		store:            cfg.Store,
		twilioClient:     cfg.TwilioClient,
		consultClient:    cfg.ConsultClient,
		openingGreeting:  cfg.OpeningGreeting,
		voicemailMessage: cfg.VoicemailMessage,
		systemPrompt:     cfg.SystemPrompt,
		pacerInterval:    cfg.PacerInterval,
		geminiDialer:     cfg.GeminiDialer,
		amd:              NewAMDGreetingController(0.015),
		outboundFrames:   make(chan []byte, 1000),
		started:          make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

// ProviderCallSID returns the remote provider call SID.
func (s *MediaStreamSession) ProviderCallSID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.providerCallSID
}

// Run executes the session until context cancellation or session completion.
func (s *MediaStreamSession) Run(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	defer func() {
		s.flushPendingTranscripts(context.WithoutCancel(parentCtx))
		cancel()
		s.doneOnce.Do(func() {
			close(s.doneCh)
		})
	}()

	// 1. Connect to Gemini Live
	model := "gemini-3.8-live"
	if s.cfg != nil && s.cfg.GeminiLiveModel != "" {
		model = s.cfg.GeminiLiveModel
	}

	gemConn, err := s.dialGemini(ctx, model)
	if err != nil {
		log.Printf("prospera: gemini live dial error for %s: %v", s.callID, err)
		return
	}
	s.mu.Lock()
	s.geminiConn = gemConn
	s.mu.Unlock()

	// Ensure connections are unblocked when context is cancelled
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if s.geminiConn != nil {
			if closeErr := s.geminiConn.Close(); closeErr != nil {
				log.Printf("prospera: gemini conn close err: %v", closeErr)
			}
		}
		if s.twilioConn != nil {
			if closeErr := s.twilioConn.Close(); closeErr != nil {
				log.Printf("prospera: twilio conn close err: %v", closeErr)
			}
		}
		s.closed = true
		s.mu.Unlock()
	}()

	// Send initial setup handshake
	setupMsg := BuildGeminiSetupMessage(model, s.systemPrompt, s.voiceName())
	if err := s.sendGeminiJSON(setupMsg); err != nil {
		log.Printf("prospera: gemini live setup error: %v", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		s.readTwilioLoop(ctx)
		cancel()
	}()

	go func() {
		defer wg.Done()
		s.readGeminiLoop(ctx)
		cancel()
	}()

	go func() {
		defer wg.Done()
		s.pacerLoop(ctx)
	}()

	wg.Wait()
}

func (s *MediaStreamSession) dialGemini(ctx context.Context, model string) (*websocket.Conn, error) {
	if s.geminiDialer != nil {
		return s.geminiDialer(ctx, model)
	}
	apiKey := ""
	if s.cfg != nil {
		apiKey = s.cfg.GeminiAPIKey
	}
	return DialGeminiLive(ctx, "", apiKey)
}

type twilioEventMessage struct {
	Event          string `json:"event"`
	SequenceNumber string `json:"sequenceNumber,omitempty"`
	StreamSid      string `json:"streamSid,omitempty"`
	Start          *struct {
		StreamSid        string            `json:"streamSid"`
		CallSid          string            `json:"callSid"`
		AccountSid       string            `json:"accountSid"`
		CustomParameters map[string]string `json:"customParameters"`
	} `json:"start,omitempty"`
	Media *struct {
		Track     string `json:"track"`
		Chunk     string `json:"chunk"`
		Timestamp string `json:"timestamp"`
		Payload   string `json:"payload"`
	} `json:"media,omitempty"`
	Mark *struct {
		Name string `json:"name"`
	} `json:"mark,omitempty"`
	Stop *struct {
		CallSid string `json:"callSid"`
	} `json:"stop,omitempty"`
}

func (s *MediaStreamSession) readTwilioLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, data, err := s.twilioConn.ReadMessage()
		if err != nil {
			return
		}

		var msg twilioEventMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if s.dispatchTwilioEvent(ctx, msg) {
			return
		}
	}
}

func (s *MediaStreamSession) dispatchTwilioEvent(ctx context.Context, msg twilioEventMessage) bool {
	switch msg.Event {
	case "start":
		if msg.Start != nil {
			s.handleTwilioStart(ctx, msg.Start.StreamSid, msg.Start.CallSid)
		}
	case "media":
		if msg.Media != nil && msg.Media.Payload != "" {
			s.handleInboundTwilioMedia(msg.Media.Payload)
		}
	case "mark":
		if msg.Mark != nil && msg.Mark.Name == "hangup" {
			s.handleTwilioHangupMark(ctx)
			return true
		}
	case "stop":
		s.flushPendingTranscripts(context.WithoutCancel(ctx))
		if s.store != nil {
			if err := s.store.UpdateCallState(ctx, s.callID, "completed", "completed_by_remote", ""); err != nil {
				log.Printf("prospera: update call state err: %v", err)
			}
		}
		return true
	}
	return false
}

func (s *MediaStreamSession) handleTwilioStart(ctx context.Context, streamSID, callSID string) {
	s.mu.Lock()
	s.streamSID = streamSID
	s.providerCallSID = callSID
	s.mu.Unlock()

	s.startedOnce.Do(func() {
		close(s.started)
	})

	if s.store != nil {
		if err := s.store.BindProviderCallSID(ctx, s.callID, callSID); err != nil {
			log.Printf("prospera: bind provider sid err: %v", err)
		}
		if err := s.store.UpdateCallState(ctx, s.callID, "in-progress", "", ""); err != nil {
			log.Printf("prospera: update call state err: %v", err)
		}
	}
}

func (s *MediaStreamSession) handleTwilioHangupMark(ctx context.Context) {
	s.flushPendingTranscripts(context.WithoutCancel(ctx))
	s.mu.Lock()
	sid := s.providerCallSID
	s.mu.Unlock()

	if s.twilioClient != nil && sid != "" {
		if err := s.twilioClient.HangupCall(ctx, sid); err != nil {
			log.Printf("prospera: twilio hangup err: %v", err)
		}
	}
	if s.store != nil {
		if err := s.store.UpdateCallState(ctx, s.callID, "completed", "completed", ""); err != nil {
			log.Printf("prospera: update call state err: %v", err)
		}
	}
}

func (s *MediaStreamSession) handleInboundTwilioMedia(base64Payload string) {
	ulawBytes, err := base64.StdEncoding.DecodeString(base64Payload)
	if err != nil || len(ulawBytes) == 0 {
		return
	}

	pcm8k := MuLawToPCM16(ulawBytes)
	rms := ComputeRMS(pcm8k) / 32768.0

	action := s.amd.OnInboundFrame(rms, 20)
	s.handleAMDAction(action)

	if !s.amd.IsReleased() || s.amd.State() >= AMDStateMachineDetected {
		return
	}

	pcm16k := Resample8kTo16k(pcm8k)
	pcm16LE := PCM16ToLEBytes(pcm16k)
	audioChunkMsg := BuildGeminiAudioChunk(pcm16LE)
	if sendErr := s.sendGeminiJSON(audioChunkMsg); sendErr != nil {
		log.Printf("prospera: send audio chunk err: %v", sendErr)
	}
}

func (s *MediaStreamSession) handleAMDAction(action AMDAction) {
	switch action {
	case AMDActionReleaseGreeting:
		s.mu.Lock()
		alreadyReleased := s.greetingReleased
		s.greetingReleased = true
		s.mu.Unlock()

		if !alreadyReleased {
			turnMsg := BuildGeminiClientContent(fmt.Sprintf("Call connected. Open the conversation naturally and warmly in your own words (do not read the task verbatim) for: %s", s.openingGreeting), "user")
			if err := s.sendGeminiJSON(turnMsg); err != nil {
				log.Printf("prospera: send turn msg err: %v", err)
			}
		}
	case AMDActionSwitchToVoicemail:
		s.drainOutboundQueue()
		s.sendTwilioClear()
		turnMsg := BuildGeminiClientContent("[Voicemail/machine answering detected. Wait for beep.]", "user")
		if err := s.sendGeminiJSON(turnMsg); err != nil {
			log.Printf("prospera: send turn msg err: %v", err)
		}
	case AMDActionSpeakVoicemailAndHangup:
		s.drainOutboundQueue()
		s.sendTwilioClear()
		vm := s.voicemailMessage
		if vm == "" {
			vm = s.openingGreeting
		}
		turnMsg := BuildGeminiClientContent(fmt.Sprintf("Voicemail beep heard. Speak this exact voicemail message and then call end_call: %s", vm), "user")
		if err := s.sendGeminiJSON(turnMsg); err != nil {
			log.Printf("prospera: send turn msg err: %v", err)
		}
	}
}

// HandleAMD processes an AMD webhook asynchronously.
func (s *MediaStreamSession) HandleAMD(answeredBy string) {
	action := s.amd.OnAMDWebhook(answeredBy)
	s.handleAMDAction(action)
}

func (s *MediaStreamSession) readGeminiLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		s.mu.Lock()
		conn := s.geminiConn
		s.mu.Unlock()
		if conn == nil {
			return
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			s.mu.Lock()
			isClosed := s.closed || s.hangupPending
			s.mu.Unlock()
			if isClosed {
				return
			}
			if s.reconnectGemini(ctx) {
				continue
			}
			return
		}

		var srvMsg GeminiServerMessage
		if err := json.Unmarshal(data, &srvMsg); err != nil {
			continue
		}

		if srvMsg.ServerContent != nil {
			s.handleServerContent(ctx, srvMsg.ServerContent)
		}

		if srvMsg.ToolCall != nil {
			s.flushPendingTranscripts(ctx)
			for _, fc := range srvMsg.ToolCall.FunctionCalls {
				s.executeToolCall(ctx, fc)
			}
		}
	}
}

func (s *MediaStreamSession) handleServerContent(ctx context.Context, sc *GeminiServerContent) {
	if sc.Interrupted {
		s.flushTranscriptBuf(ctx, "assistant", &s.outputTranscriptBuf)
		s.drainOutboundQueue()
		s.sendTwilioClear()
	}
	if sc.InputTranscription != nil && sc.InputTranscription.Text != "" {
		s.flushTranscriptBuf(ctx, "assistant", &s.outputTranscriptBuf)
		s.mu.Lock()
		s.inputTranscriptBuf.WriteString(sc.InputTranscription.Text)
		s.mu.Unlock()
	}
	if sc.OutputTranscription != nil && sc.OutputTranscription.Text != "" {
		s.flushTranscriptBuf(ctx, "user", &s.inputTranscriptBuf)
		s.mu.Lock()
		s.outputTranscriptBuf.WriteString(sc.OutputTranscription.Text)
		s.mu.Unlock()
	}
	if sc.ModelTurn != nil {
		s.flushTranscriptBuf(ctx, "user", &s.inputTranscriptBuf)
		for _, part := range sc.ModelTurn.Parts {
			s.handleModelTurnPart(ctx, part)
		}
	}
	if sc.GenerationComplete || sc.TurnComplete {
		s.flushPendingTranscripts(ctx)
	}
}

func (s *MediaStreamSession) flushPendingTranscripts(ctx context.Context) {
	s.flushTranscriptBuf(ctx, "user", &s.inputTranscriptBuf)
	s.flushTranscriptBuf(ctx, "assistant", &s.outputTranscriptBuf)
}

func (s *MediaStreamSession) flushTranscriptBuf(ctx context.Context, role string, buf *strings.Builder) {
	s.mu.Lock()
	text := strings.TrimSpace(buf.String())
	buf.Reset()
	s.mu.Unlock()
	if text == "" || s.store == nil {
		return
	}
	if err := s.store.AppendTranscriptTurn(ctx, s.callID, TranscriptTurn{
		Role:      role,
		Text:      text,
		Timestamp: time.Now().UTC(),
	}); err != nil {
		log.Printf("prospera: append transcript turn err: %v", err)
	}
}

func (s *MediaStreamSession) handleModelTurnPart(ctx context.Context, part GeminiPart) {
	mimeType := part.MimeType
	data := part.Data
	if part.InlineData != nil && part.InlineData.Data != "" {
		mimeType = part.InlineData.MimeType
		data = part.InlineData.Data
	}
	if strings.HasPrefix(mimeType, "audio/pcm") && data != "" {
		raw24, err := base64.StdEncoding.DecodeString(data)
		if err == nil {
			pcm24k := LEBytesToPCM16(raw24)
			pcm8k := Resample24kTo8k(pcm24k)
			ulaw := PCM16ToMuLaw(pcm8k)
			s.enqueueOutboundUlaw(ulaw)
		}
	}
	if part.Text != "" && !part.Thought && s.store != nil {
		if err := s.store.AppendTranscriptTurn(ctx, s.callID, TranscriptTurn{
			Role:      "assistant",
			Text:      part.Text,
			Timestamp: time.Now().UTC(),
		}); err != nil {
			log.Printf("prospera: append transcript turn err: %v", err)
		}
	}
}

func (s *MediaStreamSession) executeToolCall(ctx context.Context, fc GeminiFunctionCall) {
	s.flushPendingTranscripts(ctx)
	switch fc.Name {
	case "send_dtmf":
		s.toolSendDTMF(fc)
	case "consult_calendar":
		s.toolConsultCalendar(ctx, fc)
	case "consult_memory":
		s.toolConsultMemory(ctx, fc)
	case "end_call":
		s.toolEndCall(fc)
	}
}

func (s *MediaStreamSession) toolSendDTMF(fc GeminiFunctionCall) {
	var digits string
	if rawDigits, ok := fc.Args["digits"].(string); ok {
		digits = rawDigits
	}
	dtmfBytes, err := SynthesizeDTMFMuLaw(digits)
	if err != nil {
		resp := BuildGeminiToolResponse(fc.ID, map[string]any{"error": err.Error()})
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
		return
	}
	s.enqueueOutboundUlaw(dtmfBytes)
	resp := BuildGeminiToolResponse(fc.ID, map[string]any{"status": "sent", "digits": digits})
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
}

func (s *MediaStreamSession) toolConsultCalendar(ctx context.Context, fc GeminiFunctionCall) {
	var q, start, end string
	if v, ok := fc.Args["query"].(string); ok {
		q = v
	}
	if v, ok := fc.Args["start_iso"].(string); ok {
		start = v
	}
	if v, ok := fc.Args["end_iso"].(string); ok {
		end = v
	}
	if s.consultClient != nil {
		res, err := s.consultClient.ConsultCalendar(ctx, q, start, end)
		var resp GeminiToolResponseMessage
		if err != nil {
			resp = BuildGeminiToolResponse(fc.ID, map[string]any{"error": err.Error()})
		} else {
			resp = BuildGeminiToolResponse(fc.ID, map[string]any{"result": res})
		}
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
	} else {
		resp := BuildGeminiToolResponse(fc.ID, map[string]any{"result": "Calendar unavailable"})
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
	}
}

func (s *MediaStreamSession) toolConsultMemory(ctx context.Context, fc GeminiFunctionCall) {
	var q string
	if v, ok := fc.Args["query"].(string); ok {
		q = v
	}
	if s.consultClient != nil {
		res, err := s.consultClient.ConsultMemory(ctx, q)
		var resp GeminiToolResponseMessage
		if err != nil {
			resp = BuildGeminiToolResponse(fc.ID, map[string]any{"error": err.Error()})
		} else {
			resp = BuildGeminiToolResponse(fc.ID, map[string]any{"result": res})
		}
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
	} else {
		resp := BuildGeminiToolResponse(fc.ID, map[string]any{"result": "Memory unavailable"})
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
	}
}

func (s *MediaStreamSession) toolEndCall(fc GeminiFunctionCall) {
	var reason string
	if v, ok := fc.Args["reason"].(string); ok {
		reason = v
	}
	resp := BuildGeminiToolResponse(fc.ID, map[string]any{"status": "ending", "reason": reason})
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
	s.InitiateHangup(reason)
}

func (s *MediaStreamSession) reconnectGemini(ctx context.Context) bool {
	fallbackModel := "gemini-3.1-flash-live-preview"
	if s.cfg != nil && s.cfg.GeminiFallbackModel != "" {
		fallbackModel = s.cfg.GeminiFallbackModel
	}

	newConn, err := s.dialGemini(ctx, fallbackModel)
	if err != nil {
		return false
	}

	s.mu.Lock()
	if s.geminiConn != nil {
		if closeErr := s.geminiConn.Close(); closeErr != nil {
			log.Printf("prospera: close prev gemini conn err: %v", closeErr)
		}
	}
	s.geminiConn = newConn
	s.mu.Unlock()

	setupMsg := BuildGeminiSetupMessage(fallbackModel, s.systemPrompt, s.voiceName())
	if err := s.sendGeminiJSON(setupMsg); err != nil {
		return false
	}

	// Replay brief conversation prompt
	reconnectTurn := BuildGeminiClientContent("Connection restored. Continue conversation seamlessly.", "user")
	if err := s.sendGeminiJSON(reconnectTurn); err != nil {
		return false
	}
	return true
}

func (s *MediaStreamSession) voiceName() string {
	if s.cfg != nil && strings.TrimSpace(s.cfg.GeminiVoiceName) != "" {
		return strings.TrimSpace(s.cfg.GeminiVoiceName)
	}
	return "Aoede"
}

func (s *MediaStreamSession) enqueueOutboundUlaw(ulawBytes []byte) {
	const frameSize = 160 // 20ms at 8kHz
	for i := 0; i < len(ulawBytes); i += frameSize {
		end := i + frameSize
		if end > len(ulawBytes) {
			end = len(ulawBytes)
		}
		chunk := make([]byte, end-i)
		copy(chunk, ulawBytes[i:end])

		select {
		case s.outboundFrames <- chunk:
		default:
			// Buffer full; discard oldest to avoid unbounded lag
			select {
			case <-s.outboundFrames:
			default:
			}
			s.outboundFrames <- chunk
		}
	}
}

func (s *MediaStreamSession) drainOutboundQueue() {
	for {
		select {
		case <-s.outboundFrames:
		default:
			return
		}
	}
}

func (s *MediaStreamSession) popTalkspurtPayload(firstFrame []byte, inTalkspurt bool) []byte {
	if inTalkspurt {
		return firstFrame
	}
	const prebufferFrames = 3 // 60ms initial cushion at start of each talkspurt
	combined := make([]byte, 0, len(firstFrame)*prebufferFrames)
	combined = append(combined, firstFrame...)
	for i := 1; i < prebufferFrames; i++ {
		select {
		case extra, ok := <-s.outboundFrames:
			if ok && len(extra) > 0 {
				combined = append(combined, extra...)
			}
		default:
			return combined
		}
	}
	return combined
}

func (s *MediaStreamSession) pacerLoop(ctx context.Context) {
	ticker := time.NewTicker(s.pacerInterval)
	defer ticker.Stop()

	inTalkspurt := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case frame, ok := <-s.outboundFrames:
				if ok && len(frame) > 0 {
					s.sendTwilioMedia(s.popTalkspurtPayload(frame, inTalkspurt))
					inTalkspurt = true
				}
			default:
				inTalkspurt = false
				s.mu.Lock()
				pending := s.hangupPending && !s.hangupMarkSent
				s.mu.Unlock()
				if pending {
					s.mu.Lock()
					s.hangupMarkSent = true
					s.mu.Unlock()
					s.sendTwilioMark("hangup")
				}
			}
		}
	}
}

func (s *MediaStreamSession) sendTwilioMedia(frame []byte) {
	s.mu.Lock()
	streamSID := s.streamSID
	s.mu.Unlock()
	if streamSID == "" {
		return
	}

	payload := base64.StdEncoding.EncodeToString(frame)
	msg := map[string]any{
		"event":     "media",
		"streamSid": streamSID,
		"media": map[string]string{
			"payload": payload,
		},
	}
	s.sendTwilioJSON(msg)
}

func (s *MediaStreamSession) sendTwilioClear() {
	s.mu.Lock()
	streamSID := s.streamSID
	s.mu.Unlock()
	if streamSID == "" {
		return
	}

	msg := map[string]any{
		"event":     "clear",
		"streamSid": streamSID,
	}
	s.sendTwilioJSON(msg)
}

func (s *MediaStreamSession) sendTwilioMark(name string) {
	s.mu.Lock()
	streamSID := s.streamSID
	s.mu.Unlock()
	if streamSID == "" {
		return
	}

	msg := map[string]any{
		"event":     "mark",
		"streamSid": streamSID,
		"mark": map[string]string{
			"name": name,
		},
	}
	s.sendTwilioJSON(msg)
}

func (s *MediaStreamSession) sendTwilioJSON(v any) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.twilioConn != nil {
		if err := s.twilioConn.WriteJSON(v); err != nil {
			log.Printf("prospera: write twilio json err: %v", err)
		}
	}
}

func (s *MediaStreamSession) sendGeminiJSON(v any) error {
	s.geminiWriteMu.Lock()
	defer s.geminiWriteMu.Unlock()
	s.mu.Lock()
	conn := s.geminiConn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("gemini connection is nil")
	}
	return conn.WriteJSON(v)
}

// Steer delivers mid-call guidance or direct say instructions into Gemini Live.
func (s *MediaStreamSession) Steer(message, mode string) error {
	var instruction string
	if mode == "say" {
		instruction = fmt.Sprintf("[Owner instruction (say immediately): Please say: %q]", message)
	} else {
		instruction = fmt.Sprintf("[Owner instruction (guidance): %s]", message)
	}

	turnMsg := BuildGeminiClientContent(instruction, "user")
	return s.sendGeminiJSON(turnMsg)
}

// InitiateHangup schedules graceful hangup after buffered frames finish playing.
func (s *MediaStreamSession) InitiateHangup(reason string) {
	s.mu.Lock()
	s.hangupPending = true
	s.hangupReason = reason
	s.mu.Unlock()
}
