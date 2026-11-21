package mediastream

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

	"github.com/azylman/aerial/services/prospera/internal/audio"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

const (
	roleUser           = "user"
	twilioEventMedia   = "media"
	twilioEventMark    = "mark"
	twilioKeyEvent     = "event"
	twilioKeyStreamSid = "streamSid"
)

// MediaStreamSessionConfig defines the construction parameters for MediaStreamSession.
type MediaStreamSessionConfig struct {
	CallID           string
	TwilioConn       *websocket.Conn
	Config           *config.Config
	Store            store.Store
	TwilioClient     twilio.TwilioClient
	ConsultClient    gemini.ContextConsultClient
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
	cfg                 *config.Config
	store               store.Store
	twilioClient        twilio.TwilioClient
	consultClient       gemini.ContextConsultClient
	openingGreeting     string
	voicemailMessage    string
	systemPrompt        string
	language            string
	pacerInterval       time.Duration
	geminiDialer        func(ctx context.Context, model string) (*websocket.Conn, error)
	amd                 *audio.AMDGreetingController
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
		amd:              audio.NewAMDGreetingController(0.015),
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

// SetProviderCallSID overrides the remote provider call SID (used by tests).
func (s *MediaStreamSession) SetProviderCallSID(sid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providerCallSID = sid
}

// Started reports when the Twilio start event has been processed.
func (s *MediaStreamSession) Started() <-chan struct{} {
	return s.started
}

// Done reports when the session run loop has completed.
func (s *MediaStreamSession) Done() <-chan struct{} {
	return s.doneCh
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
	modelName := "gemini-3.8-live"
	if s.cfg != nil && s.cfg.GeminiLiveModel != "" {
		modelName = s.cfg.GeminiLiveModel
	}

	gemConn, err := s.dialGemini(ctx, modelName)
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
	setupMsg := gemini.BuildGeminiSetupMessage(modelName, s.systemPrompt, s.voiceName(), s.language)
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

func (s *MediaStreamSession) dialGemini(ctx context.Context, modelName string) (*websocket.Conn, error) {
	if s.geminiDialer != nil {
		return s.geminiDialer(ctx, modelName)
	}
	apiKey := ""
	if s.cfg != nil {
		apiKey = s.cfg.GeminiAPIKey
	}
	return gemini.DialGeminiLive(ctx, "", apiKey)
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

	pcm8k := audio.MuLawToPCM16(ulawBytes)
	s.recordInboundPCM(pcm8k)
	rms := audio.ComputeRMS(pcm8k) / 32768.0

	action := s.amd.OnInboundFrame(rms, 20)
	s.handleAMDAction(action)

	if !s.amd.IsReleased() || s.amd.State() >= audio.AMDStateMachineDetected {
		return
	}

	pcm16k := audio.Resample8kTo16k(pcm8k)
	pcm16LE := audio.PCM16ToLEBytes(pcm16k)
	audioChunkMsg := gemini.BuildGeminiAudioChunk(pcm16LE)
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
			s.recordedPCM[s.inCursor] = audio.ClampInt16(int32(s.recordedPCM[s.inCursor]) + int32(sample))
		} else {
			s.recordedPCM = append(s.recordedPCM, sample)
		}
		s.inCursor++
	}
}

func (s *MediaStreamSession) recordOutboundUlaw(ulaw []byte) {
	pcm8k := audio.MuLawToPCM16(ulaw)
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
			s.recordedPCM[s.outCursor] = audio.ClampInt16(int32(s.recordedPCM[s.outCursor]) + int32(sample))
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
	return audio.EncodePCM16WAV(s.recordedPCM, 8000)
}

func (s *MediaStreamSession) handleAMDAction(action audio.AMDAction) {
	switch action {
	case audio.AMDActionReleaseGreeting:
		s.mu.Lock()
		alreadyReleased := s.greetingReleased
		s.greetingReleased = true
		s.mu.Unlock()

		if !alreadyReleased {
			turnMsg := gemini.BuildGeminiClientContent("Call connected. Open the conversation naturally and warmly in your own words (do not read the task verbatim) for: "+s.openingGreeting, "user")
			if err := s.sendGeminiJSON(turnMsg); err != nil {
				log.Printf("prospera: send turn msg err: %v", err)
			}
		}
	case audio.AMDActionSwitchToVoicemail:
		s.drainOutboundQueue()
		s.sendTwilioClear()
		turnMsg := gemini.BuildGeminiClientContent("[Voicemail/machine answering detected. Wait for beep.]", "user")
		if err := s.sendGeminiJSON(turnMsg); err != nil {
			log.Printf("prospera: send turn msg err: %v", err)
		}
	case audio.AMDActionSpeakVoicemailAndHangup:
		s.drainOutboundQueue()
		s.sendTwilioClear()
		vm := s.voicemailMessage
		if vm == "" {
			vm = s.openingGreeting
		}
		turnMsg := gemini.BuildGeminiClientContent("Voicemail beep heard. Speak this exact voicemail message and then call end_call: "+vm, "user")
		if err := s.sendGeminiJSON(turnMsg); err != nil {
			log.Printf("prospera: send turn msg err: %v", err)
		}
	case audio.AMDActionNone:
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

		var srvMsg gemini.GeminiServerMessage
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

func (s *MediaStreamSession) handleServerContent(ctx context.Context, sc *gemini.GeminiServerContent) {
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
	if err := s.store.AppendTranscriptTurn(ctx, s.callID, model.TranscriptTurn{
		Role:      role,
		Text:      text,
		Timestamp: time.Now().UTC(),
	}); err != nil {
		slog.ErrorContext(ctx, "prospera: append transcript turn err", "error", err)
	}
}

func (s *MediaStreamSession) handleModelTurnPart(ctx context.Context, part gemini.GeminiPart) {
	mimeType := part.MimeType
	data := part.Data
	if part.InlineData != nil && part.InlineData.Data != "" {
		mimeType = part.InlineData.MimeType
		data = part.InlineData.Data
	}
	if strings.HasPrefix(mimeType, "audio/pcm") && data != "" {
		raw24, err := base64.StdEncoding.DecodeString(data)
		if err == nil {
			pcm24k := audio.LEBytesToPCM16(raw24)
			pcm8k := audio.Resample24kTo8k(pcm24k)
			ulaw := audio.PCM16ToMuLaw(pcm8k)
			s.enqueueOutboundUlaw(ulaw)
		}
	}
	if part.Text != "" && !part.Thought && s.store != nil {
		if err := s.store.AppendTranscriptTurn(ctx, s.callID, model.TranscriptTurn{
			Role:      "assistant",
			Text:      part.Text,
			Timestamp: time.Now().UTC(),
		}); err != nil {
			slog.ErrorContext(ctx, "prospera: append transcript turn err", "error", err)
		}
	}
}

// Steer delivers mid-call guidance or direct say instructions into Gemini Live.
func (s *MediaStreamSession) Steer(message, mode string) error {
	var instruction string
	if mode == "say" {
		instruction = fmt.Sprintf("[Owner instruction (say immediately): Please say: %q]", message)
	} else {
		instruction = fmt.Sprintf("[Owner instruction (guidance): %s]", message)
	}

	turnMsg := gemini.BuildGeminiClientContent(instruction, "user")
	return s.sendGeminiJSON(turnMsg)
}

// InitiateHangup schedules graceful hangup after buffered frames finish playing.
func (s *MediaStreamSession) InitiateHangup(reason string) {
	s.mu.Lock()
	s.hangupPending = true
	s.hangupReason = reason
	s.mu.Unlock()
}

// compile-time check that MediaStreamSession satisfies the manager Session contract.
var _ Session = (*MediaStreamSession)(nil)
