package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type VoiceAskRequest struct {
	Prompt         string `json:"prompt"`
	SessionID      string `json:"session_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	Effort         string `json:"effort,omitempty"`
	NodeID         string `json:"node_id,omitempty"`
}

type VoiceAskResponse struct {
	Reply          string `json:"reply"`
	ConversationID string `json:"conversation_id,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	NodeID         string `json:"node_id,omitempty"`
}

func resolveVoiceAskSessionID(req VoiceAskRequest) (string, string) {
	sessionID := strings.TrimSpace(req.NodeID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(req.SessionID)
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(req.ConversationID)
	}
	if sessionID == "" {
		return "", "Invalid payload: 'session_id' (or 'node_id' / 'conversation_id') is required to identify the device"
	}
	if len(sessionID) > 128 || strings.ContainsAny(sessionID, "\r\n\x00") {
		return "", "Invalid payload: device identifier exceeds 128 characters or contains invalid characters"
	}
	return sessionID, ""
}

func persistVoiceConversationID(ctx context.Context, pool *queue.WorkerPool, sessionID, convID string) {
	if pool == nil || pool.Store() == nil || strings.TrimSpace(sessionID) == "" || strings.TrimSpace(convID) == "" {
		return
	}
	threadID := "voice-" + strings.TrimSpace(sessionID)
	newSessionID := strings.TrimSpace(convID)
	if err := pool.Store().RotateSessionID(context.WithoutCancel(ctx), threadID, newSessionID); err != nil {
		log.Printf("[VoiceAsk] Failed to persist voice conversation id for %s: %v", threadID, err)
	}
}

func handleVoiceAskSSE(w http.ResponseWriter, r *http.Request, pool *queue.WorkerPool, req VoiceAskRequest, sessionID string, reqStart time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{jsonKeyError: "Streaming not supported by client connection"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	var writeMu sync.Mutex
	var ttfrOnce sync.Once
	recordTTFR := func(status string) {
		ttfrOnce.Do(func() {
			metrics.RecordVoiceTTFR("sse", status, time.Since(reqStart))
		})
	}

	emitSSE := func(event string, data any) {
		if r.Context().Err() != nil {
			return
		}
		payload, mErr := json.Marshal(data)
		if mErr != nil {
			log.Printf("[VoiceAsk] Failed to marshal SSE payload: %v", mErr)
			return
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		if _, wErr := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, string(payload)); wErr != nil {
			log.Printf("[VoiceAsk] Failed to write SSE chunk: %v", wErr)
			return
		}
		flusher.Flush()

		switch event {
		case "reply", "delta", "sentence":
			recordTTFR("success")
		case sseEventError:
			recordTTFR(outcomeError)
		}
	}

	onStatus := func(status string) {
		emitSSE(sseEventStatus, map[string]string{jsonKeyStatus: status})
	}

	onSentence := func(sentence string) {
		emitSSE("sentence", map[string]string{"text": sentence})
	}

	if pool == nil {
		emitSSE(sseEventError, map[string]string{jsonKeyError: "worker pool is uninitialized"})
		emitSSE("done", map[string]any{})
		return
	}

	reply, convID, turnErr := pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, onStatus, onSentence)
	if turnErr != nil {
		if r.Context().Err() == nil {
			sanitizedErr := sanitizer.SanitizeString(turnErr.Error())
			emitSSE(sseEventError, map[string]string{jsonKeyError: sanitizedErr})
			emitSSE("done", map[string]any{})
		}
		return
	}

	emitSSE("reply", map[string]string{"reply": reply, "conversation_id": convID})
	emitSSE("done", map[string]any{"conversation_id": convID})
	persistVoiceConversationID(r.Context(), pool, sessionID, convID)
}

func handleVoiceAsk(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqStart := time.Now()
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{jsonKeyError: "Method not allowed"})
			return
		}

		var req VoiceAskRequest
		if !readPostJSON(w, r, &req) {
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{
				jsonKeyError: "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		sessionID, errMsg := resolveVoiceAskSessionID(req)
		if errMsg != "" {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{
				jsonKeyError: errMsg,
			})
			return
		}

		isSSE := strings.Contains(r.Header.Get("Accept"), "text/event-stream") || r.URL.Query().Get("stream") == "true"
		if isSSE {
			handleVoiceAskSSE(w, r, pool, req, sessionID, reqStart)
			return
		}

		if pool == nil {
			metrics.RecordVoiceTTFR("json", outcomeError, time.Since(reqStart))
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{jsonKeyError: "worker pool is uninitialized"})
			return
		}

		reply, convID, turnErr := pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, nil)
		if turnErr != nil {
			metrics.RecordVoiceTTFR("json", outcomeError, time.Since(reqStart))
			sanitizedErr := sanitizer.SanitizeString(turnErr.Error())
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{jsonKeyError: sanitizedErr})
			return
		}

		metrics.RecordVoiceTTFR("json", "success", time.Since(reqStart))
		httpkit.WriteJSON(w, http.StatusOK, VoiceAskResponse{
			Reply:          reply,
			ConversationID: convID,
			SessionID:      convID,
			NodeID:         req.NodeID,
		})
		persistVoiceConversationID(r.Context(), pool, sessionID, convID)
	}
}
