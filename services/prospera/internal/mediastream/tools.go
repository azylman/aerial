package mediastream

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/azylman/aerial/services/prospera/internal/audio"
	"github.com/azylman/aerial/services/prospera/internal/gemini"
)

// toolFieldStatus is the tool-response payload key for the outcome status.
const toolFieldStatus = "status"

func (s *MediaStreamSession) executeToolCall(ctx context.Context, fc gemini.GeminiFunctionCall) {
	s.amd.MarkConversationActive()
	s.flushPendingTranscripts(ctx)
	switch fc.Name {
	case "send_dtmf":
		s.toolSendDTMF(fc)
	case "consult_calendar":
		s.toolConsultCalendar(ctx, fc)
	case "consult_memory":
		s.toolConsultMemory(ctx, fc)
	case gemini.EndCallToolName:
		s.toolEndCall(fc)
	}
}

func (s *MediaStreamSession) toolSendDTMF(fc gemini.GeminiFunctionCall) {
	var digits string
	if rawDigits, ok := fc.Args[gemini.ArgDigits].(string); ok {
		digits = rawDigits
	}
	dtmfBytes, err := audio.SynthesizeDTMFMuLaw(digits)
	if err != nil {
		resp := gemini.BuildGeminiToolResponse(fc.ID, map[string]any{"error": err.Error()})
		if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
			log.Printf("prospera: send tool response err: %v", sendErr)
		}
		return
	}
	s.enqueueOutboundUlaw(dtmfBytes)
	resp := gemini.BuildGeminiToolResponse(fc.ID, map[string]any{toolFieldStatus: "sent", gemini.ArgDigits: digits})
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
}

func (s *MediaStreamSession) sendGeminiToolResult(callID string, res string, err error, fallback string) {
	var resp gemini.GeminiToolResponseMessage
	switch {
	case err != nil:
		resp = gemini.BuildGeminiToolResponse(callID, map[string]any{"error": err.Error()})
	case res != "":
		resp = gemini.BuildGeminiToolResponse(callID, map[string]any{"result": res})
	default:
		resp = gemini.BuildGeminiToolResponse(callID, map[string]any{"result": fallback})
	}
	if sendErr := s.sendGeminiJSON(resp); sendErr != nil {
		log.Printf("prospera: send tool response err: %v", sendErr)
	}
}

func (s *MediaStreamSession) toolConsultCalendar(ctx context.Context, fc gemini.GeminiFunctionCall) {
	var q, start, end string
	if v, ok := fc.Args[gemini.ArgQuery].(string); ok {
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

func (s *MediaStreamSession) toolConsultMemory(ctx context.Context, fc gemini.GeminiFunctionCall) {
	var q string
	if v, ok := fc.Args[gemini.ArgQuery].(string); ok {
		q = v
	}
	if s.consultClient == nil {
		s.sendGeminiToolResult(fc.ID, "", nil, "Memory unavailable")
		return
	}
	res, err := s.consultClient.ConsultMemory(ctx, q)
	s.sendGeminiToolResult(fc.ID, res, err, "Memory unavailable")
}

func (s *MediaStreamSession) toolEndCall(fc gemini.GeminiFunctionCall) {
	var reason string
	if v, ok := fc.Args["reason"].(string); ok {
		reason = v
	}
	resp := gemini.BuildGeminiToolResponse(fc.ID, map[string]any{toolFieldStatus: "ending", "reason": reason})
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

	setupMsg := gemini.BuildGeminiSetupMessage(fallbackModel, s.systemPrompt, s.voiceName(), s.language)
	if err := s.sendGeminiJSON(setupMsg); err != nil {
		return false
	}

	// Replay brief conversation prompt
	reconnectTurn := gemini.BuildGeminiClientContent("Connection restored. Continue conversation seamlessly.", "user")
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
