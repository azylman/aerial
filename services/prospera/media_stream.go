package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/azylman/aerial/services/prospera/internal/mediastream"
)

const (
	roleUser           = "user"
	twilioEventMedia   = "media"
	twilioEventMark    = "mark"
	twilioKeyEvent     = "event"
	twilioKeyStreamSid = "streamSid"
)

// Manager registry types and constructors live in internal/mediastream.
// The aliases below keep existing callers compiling until the session
// bridge chunk moves the remaining session implementation.
type (
	SessionManagerOption = mediastream.SessionManagerOption
	CallSessionManager   = mediastream.CallSessionManager
)

var (
	NewCallSessionManager = mediastream.NewCallSessionManager
	WithPacerInterval     = mediastream.WithPacerInterval
	WithGeminiDialer      = mediastream.WithGeminiDialer
	WithSessionFactory    = mediastream.WithSessionFactory

	// ActiveCount exposes the registry size query as a package-level
	// function value to keep the transition API reachable for deadcode
	// until post-cutover callers (e.g. Drain) land.
	ActiveCount = (*CallSessionManager).ActiveCount
	// WaitForSession exposes the registry wait primitive as a
	// package-level function value to keep the transition API reachable
	// for deadcode until post-cutover callers land.
	WaitForSession = (*CallSessionManager).WaitForSession
)

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
	Language         string
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
	language            string
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
	userTurns           int
	inputTranscriptBuf  strings.Builder
	outputTranscriptBuf strings.Builder
	recordedPCM         []int16
	inCursor            int
	outCursor           int
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
		language:         cfg.Language,
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
		slog.ErrorContext(ctx, "prospera: gemini live dial error", "call_id", s.callID, "error", err)
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
				slog.WarnContext(ctx, "prospera: gemini conn close err", "error", closeErr)
			}
		}
		if s.twilioConn != nil {
			if closeErr := s.twilioConn.Close(); closeErr != nil {
				slog.WarnContext(ctx, "prospera: twilio conn close err", "error", closeErr)
			}
		}
		s.closed = true
		s.mu.Unlock()
	}()

	// Send initial setup handshake
	setupMsg := BuildGeminiSetupMessage(model, s.systemPrompt, s.voiceName(), s.language)
	if err := s.sendGeminiJSON(setupMsg); err != nil {
		slog.ErrorContext(ctx, "prospera: gemini live setup error", "error", err)
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
	case twilioEventMedia:
		if msg.Media != nil && msg.Media.Payload != "" {
			s.handleInboundTwilioMedia(msg.Media.Payload)
		}
	case twilioEventMark:
		if msg.Mark != nil && msg.Mark.Name == "hangup" {
			s.handleTwilioHangupMark(ctx)
			return true
		}
	case "stop":
		s.flushPendingTranscripts(context.WithoutCancel(ctx))
		if s.store != nil {
			if err := s.store.UpdateCallState(ctx, s.callID, "completed", "completed_by_remote", ""); err != nil {
				slog.ErrorContext(ctx, "prospera: update call state err", "error", err)
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
			slog.ErrorContext(ctx, "prospera: bind provider sid err", "error", err)
		}
		if err := s.store.UpdateCallState(ctx, s.callID, "in-progress", "", ""); err != nil {
			slog.ErrorContext(ctx, "prospera: update call state err", "error", err)
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
			slog.ErrorContext(ctx, "prospera: twilio hangup err", "error", err)
		}
	}
	if s.store != nil {
		if err := s.store.UpdateCallState(ctx, s.callID, "completed", "completed", ""); err != nil {
			slog.ErrorContext(ctx, "prospera: update call state err", "error", err)
		}
	}
}

func (s *MediaStreamSession) handleInboundTwilioMedia(base64Payload string) {
	ulawBytes, err := base64.StdEncoding.DecodeString(base64Payload)
	if err != nil || len(ulawBytes) == 0 {
		return
	}

	pcm8k := MuLawToPCM16(ulawBytes)
	s.recordInboundPCM(pcm8k)
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

const maxRecordingSamples = 8000 * 300

func (s *MediaStreamSession) recordInboundPCM(pcm8k []int16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range pcm8k {
		if s.inCursor >= maxRecordingSamples {
			break
		}
		if s.inCursor < len(s.recordedPCM) {
			s.recordedPCM[s.inCursor] = clampInt16(int32(s.recordedPCM[s.inCursor]) + int32(sample))
		} else {
			s.recordedPCM = append(s.recordedPCM, sample)
		}
		s.inCursor++
	}
}

func (s *MediaStreamSession) recordOutboundUlaw(ulaw []byte) {
	pcm8k := MuLawToPCM16(ulaw)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outCursor < s.inCursor {
		s.outCursor = s.inCursor
	}
	for _, sample := range pcm8k {
		if s.outCursor >= maxRecordingSamples {
			break
		}
		if s.outCursor < len(s.recordedPCM) {
			s.recordedPCM[s.outCursor] = clampInt16(int32(s.recordedPCM[s.outCursor]) + int32(sample))
		} else {
			s.recordedPCM = append(s.recordedPCM, sample)
		}
		s.outCursor++
	}
}

// RecordingWAV encodes the session's recorded 8 kHz mono audio buffer into a WAV byte slice.
func (s *MediaStreamSession) RecordingWAV() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return EncodePCM16WAV(s.recordedPCM, 8000)
}

func (s *MediaStreamSession) handleAMDAction(action AMDAction) {
	switch action {
	case AMDActionReleaseGreeting:
		s.mu.Lock()
		alreadyReleased := s.greetingReleased
		s.greetingReleased = true
		s.mu.Unlock()

		if !alreadyReleased {
			turnMsg := BuildGeminiClientContent("Call connected. Open the conversation naturally and warmly in your own words (do not read the task verbatim) for: "+s.openingGreeting, "user")
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
		turnMsg := BuildGeminiClientContent("Voicemail beep heard. Speak this exact voicemail message and then call end_call: "+vm, "user")
		if err := s.sendGeminiJSON(turnMsg); err != nil {
			log.Printf("prospera: send turn msg err: %v", err)
		}
	case AMDActionNone:
		// No action needed for AMDActionNone
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
	if role == roleUser && text != "" {
		s.userTurns++
		if s.userTurns >= 2 {
			s.amd.MarkConversationActive()
		}
	}
	s.mu.Unlock()
	if text == "" || s.store == nil {
		return
	}
	if err := s.store.AppendTranscriptTurn(ctx, s.callID, TranscriptTurn{
		Role:      role,
		Text:      text,
		Timestamp: time.Now().UTC(),
	}); err != nil {
		slog.ErrorContext(ctx, "prospera: append transcript turn err", "error", err)
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
			slog.ErrorContext(ctx, "prospera: append transcript turn err", "error", err)
		}
	}
}

func (s *MediaStreamSession) executeToolCall(ctx context.Context, fc GeminiFunctionCall) {
	s.amd.MarkConversationActive()
	s.flushPendingTranscripts(ctx)
	switch fc.Name {
	case "send_dtmf":
		s.toolSendDTMF(fc)
	case "consult_calendar":
		s.toolConsultCalendar(ctx, fc)
	case "consult_memory":
		s.toolConsultMemory(ctx, fc)
	case endCallToolName:
		s.toolEndCall(fc)
	}
}

func (s *MediaStreamSession) toolSendDTMF(fc GeminiFunctionCall) {
	var digits string
	if rawDigits, ok := fc.Args[argDigits].(string); ok {
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
	resp := BuildGeminiToolResponse(fc.ID, map[string]any{fieldStatus: "sent", argDigits: digits})
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
}

func (s *MediaStreamSession) sendGeminiToolResult(callID string, res string, err error, fallback string) {
	var resp GeminiToolResponseMessage
	switch {
	case err != nil:
		resp = BuildGeminiToolResponse(callID, map[string]any{"error": err.Error()})
	case res != "":
		resp = BuildGeminiToolResponse(callID, map[string]any{"result": res})
	default:
		resp = BuildGeminiToolResponse(callID, map[string]any{"result": fallback})
	}
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
}

func (s *MediaStreamSession) toolConsultCalendar(ctx context.Context, fc GeminiFunctionCall) {
	var q, start, end string
	if v, ok := fc.Args[argQuery].(string); ok {
		q = v
	}
	if v, ok := fc.Args["start_iso"].(string); ok {
		start = v
	}
	if v, ok := fc.Args["end_iso"].(string); ok {
		end = v
	}
	if s.consultClient == nil {
		s.sendGeminiToolResult(fc.ID, "", nil, "Calendar unavailable")
		return
	}
	res, err := s.consultClient.ConsultCalendar(ctx, q, start, end)
	s.sendGeminiToolResult(fc.ID, res, err, "Calendar unavailable")
}

func (s *MediaStreamSession) toolConsultMemory(ctx context.Context, fc GeminiFunctionCall) {
	var q string
	if v, ok := fc.Args[argQuery].(string); ok {
		q = v
	}
	if s.consultClient == nil {
		s.sendGeminiToolResult(fc.ID, "", nil, "Memory unavailable")
		return
	}
	res, err := s.consultClient.ConsultMemory(ctx, q)
	s.sendGeminiToolResult(fc.ID, res, err, "Memory unavailable")
}

func (s *MediaStreamSession) toolEndCall(fc GeminiFunctionCall) {
	var reason string
	if v, ok := fc.Args["reason"].(string); ok {
		reason = v
	}
	resp := BuildGeminiToolResponse(fc.ID, map[string]any{fieldStatus: "ending", "reason": reason})
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
			slog.WarnContext(ctx, "prospera: close prev gemini conn err", "error", closeErr)
		}
	}
	s.geminiConn = newConn
	s.mu.Unlock()

	setupMsg := BuildGeminiSetupMessage(fallbackModel, s.systemPrompt, s.voiceName(), s.language)
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
	return "Kore"
}

func (s *MediaStreamSession) enqueueOutboundUlaw(ulawBytes []byte) {
	const frameSize = 160 // 20ms at 8kHz
	for i := 0; i < len(ulawBytes); i += frameSize {
		end := min(i+frameSize, len(ulawBytes))
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
	s.recordOutboundUlaw(frame)

	payload := base64.StdEncoding.EncodeToString(frame)
	msg := map[string]any{
		twilioKeyEvent:     twilioEventMedia,
		twilioKeyStreamSid: streamSID,
		twilioEventMedia: map[string]string{
			"payload": payload,
		},
	}
	s.sendTwilioJSON(msg)
}

func (s *MediaStreamSession) sendTwilioClear() {
	s.mu.Lock()
	streamSID := s.streamSID
	if s.outCursor > s.inCursor && s.inCursor <= len(s.recordedPCM) {
		s.recordedPCM = s.recordedPCM[:s.inCursor]
		s.outCursor = s.inCursor
	}
	s.mu.Unlock()
	if streamSID == "" {
		return
	}

	msg := map[string]any{
		twilioKeyEvent:     "clear",
		twilioKeyStreamSid: streamSID,
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
		twilioKeyEvent:     twilioEventMark,
		twilioKeyStreamSid: streamSID,
		twilioEventMark: map[string]string{
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
