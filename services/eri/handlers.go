package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/discord"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

type PromptRequest struct {
	Prompt       string `json:"prompt"`
	ChannelID    string `json:"channel_id"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
	Model        string `json:"model,omitempty"`
	Effort       string `json:"effort,omitempty"`
	Silent       bool   `json:"silent,omitempty"`
	Ephemeral    *bool  `json:"ephemeral,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{"error": msg})
}

func writeStoreErr(w http.ResponseWriter, entity string, err error) {
	log.Printf("[HTTP] Error fetching %s: %v", entity, err)
	writeErr(w, http.StatusInternalServerError, "Failed to fetch "+entity)
}

func validateSnowflakeID(w http.ResponseWriter, id, field string) bool {
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("Invalid payload: '%s' field is required and cannot be empty", field))
		return false
	}
	if !queue.IsNumericSnowflake(id) {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("Invalid payload: '%s' must be a valid numeric Discord snowflake", field))
		return false
	}
	return true
}

func resolvePromptEffort(model, effort string) string {
	if effort != "" {
		return effort
	}
	modelLower := strings.ToLower(model)
	if strings.Contains(modelLower, "medium") {
		return "medium"
	}
	if strings.Contains(modelLower, "flash") {
		return "low"
	}
	return ""
}

func checkPromptActiveTurn(ctx context.Context, store db.Store, skipIfActive bool, sessionID, channelID string) (bool, string, error) {
	if !skipIfActive || store == nil {
		return false, "", nil
	}
	checkID := sessionID
	if checkID == "" {
		checkID = channelID
	}
	active, err := store.HasActiveThreadTurn(ctx, checkID)
	return active, checkID, err
}

func resolvePromptChannelID(channelID string, pool *queue.WorkerPool) string {
	channelID = strings.TrimSpace(channelID)
	if channelID != "" && !queue.IsNumericSnowflake(channelID) && pool != nil {
		if resolved, err := pool.ResolveChannelID(channelID); err == nil {
			return resolved
		}
	}
	return channelID
}

func validatePromptSessionID(w http.ResponseWriter, sessionID string) bool {
	if sessionID != "" && !queue.IsValidThreadKey(sessionID) {
		writeErr(w, http.StatusBadRequest, "Invalid payload: 'session_id' must be a valid alphanumeric/hyphen/underscore key")
		return false
	}
	return true
}

func resolvePromptSessionID(pool *queue.WorkerPool, channelID, sessionID, title, prompt string, silent bool) (string, string) {
	if sessionID != "" {
		return channelID, sessionID
	}
	if !silent && pool != nil {
		channelID = pool.ResolveTargetThread(channelID, strings.TrimSpace(title), prompt, time.Now().UTC())
	}
	return channelID, channelID
}

func handlePrompt(store db.Store, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req PromptRequest
		if !readPostJSON(w, r, &req) {
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty")
			return
		}

		channelID := resolvePromptChannelID(req.ChannelID, pool)
		if !validateSnowflakeID(w, channelID, "channel_id") {
			return
		}

		model := strings.TrimSpace(req.Model)
		effort := resolvePromptEffort(model, strings.TrimSpace(req.Effort))
		if _, err := queue.ResolvePoolKey(req.Ephemeral, effort); err != nil {
			writeErr(w, http.StatusBadRequest, "Invalid payload: "+err.Error())
			return
		}

		sessionID := strings.TrimSpace(req.SessionID)
		if !validatePromptSessionID(w, sessionID) {
			return
		}

		active, checkID, err := checkPromptActiveTurn(r.Context(), store, req.SkipIfActive, sessionID, channelID)
		if err != nil {
			log.Printf("[HTTP] Failed to check active turn for %s: %v", checkID, err)
			writeErr(w, http.StatusInternalServerError, "Failed to check active turn")
			return
		}
		if active {
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{
				"status":     "skipped_active",
				"channel_id": channelID,
				"session_id": checkID,
			})
			return
		}

		channelID, sessionID = resolvePromptSessionID(pool, channelID, sessionID, req.Title, req.Prompt, req.Silent)

		msgID := strings.TrimSpace(req.MessageID)
		if msgID == "" {
			msgID = uuid.New().String()
		}

		msg := db.Message{
			ID:         msgID,
			ThreadID:   sessionID,
			GuildID:    "",
			AuthorID:   "http-client",
			AuthorName: "HTTP Client",
			IsAdmin:    true,
			Content:    req.Prompt,
			Summary:    db.CleanTaskSummary(req.Prompt),
			Status:     db.StatusPending,
			Effort:     effort,
			ChannelID:  channelID,
			Ephemeral:  req.Ephemeral,
			Model:      model,
			Silent:     req.Silent,
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}

		if store == nil {
			writeErr(w, http.StatusInternalServerError, "Database store not configured")
			return
		}

		insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := store.InsertMessage(insertCtx, msg); err != nil {
			insertCancel()
			log.Printf("Failed to insert HTTP prompt message %s to DB: %v", msgID, err)
			writeErr(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist prompt message: %v", err))
			return
		}
		insertCancel()

		if pool != nil {
			pool.Enqueue(msg)
		}

		httpkit.WriteJSON(w, http.StatusAccepted, map[string]string{
			"status":     "accepted",
			"channel_id": channelID,
			"session_id": sessionID,
			"message_id": msgID,
			"message":    "Prompt execution enqueued in background",
		})
	}
}

type CancelPromptRequest struct {
	MessageID string `json:"message_id,omitempty"`
	BuildID   string `json:"build_id,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func matchesCancelPrompt(m db.Message, msgID, buildID string) bool {
	if m.Status != db.StatusPending {
		return false
	}
	return (msgID != "" && m.ID == msgID) || (buildID != "" && strings.Contains(m.Content, "Build ID: "+buildID+"\n"))
}

func resolveCancelReason(reason, buildID, msgID string) string {
	if reason = strings.TrimSpace(reason); reason != "" {
		return reason
	}
	if buildID != "" {
		return "cancelled vanadis build " + buildID
	}
	return "cancelled prompt " + msgID
}

func drainMatchingPendingPrompts(ctx context.Context, store db.Store, msgs []db.Message, msgID, buildID, reason string) []string {
	cancelledIDs := make([]string, 0)
	for _, m := range msgs {
		if !matchesCancelPrompt(m, msgID, buildID) {
			continue
		}
		if claimed, err := store.ClaimPendingMessage(ctx, m.ID); err != nil || !claimed {
			continue
		}
		if err := store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, reason); err != nil {
			log.Printf("[HTTP] Warning: failed to mark claimed prompt %s as cancelled: %v", m.ID, err)
		}
		cancelledIDs = append(cancelledIDs, m.ID)
	}
	return cancelledIDs
}

func handleCancelPrompt(store db.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if store == nil {
			writeErr(w, http.StatusInternalServerError, "Database store not configured")
			return
		}
		var req CancelPromptRequest
		if !readPostJSON(w, r, &req) {
			return
		}
		msgID, buildID := strings.TrimSpace(req.MessageID), strings.TrimSpace(req.BuildID)
		if msgID == "" && buildID == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'message_id' or 'build_id' is required")
			return
		}
		msgs, err := store.GetPendingOrProcessingMessages(r.Context(), 0)
		if err != nil {
			writeStoreErr(w, "pending messages", err)
			return
		}
		cancelledIDs := drainMatchingPendingPrompts(r.Context(), store, msgs, msgID, buildID, resolveCancelReason(req.Reason, buildID, msgID))
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok", "cancelled_count": len(cancelledIDs), "cancelled_ids": cancelledIDs})
	}
}

type DirectAttachmentPayload struct {
	Filename      string `json:"filename"`
	ContentType   string `json:"content_type,omitempty"`
	ContentBase64 string `json:"content_base64"`
}

type DirectMessageRequest struct {
	ChannelID   string                    `json:"channel_id"`
	ThreadID    string                    `json:"thread_id,omitempty"`
	Content     string                    `json:"content"`
	Text        string                    `json:"text,omitempty"`
	Attachments []DirectAttachmentPayload `json:"attachments,omitempty"`
}

func readPostJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	return readPostJSONWithLimit(w, r, dst, 1<<20)
}

func readPostJSONWithLimit(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(r.Body)
	if closeErr := r.Body.Close(); closeErr != nil {
		log.Printf("[HTTP] Warning closing request body: %v", closeErr)
	}
	if err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
		return false
	}
	return true
}

func writeDiscordDeliveryError(w http.ResponseWriter, err error, failPrefix string) {
	if strings.Contains(err.Error(), "discord session is not connected") || strings.Contains(err.Error(), "discord session is nil") {
		writeErr(w, http.StatusServiceUnavailable, "Discord session not connected")
		return
	}
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		if restErr.Response != nil && restErr.Response.StatusCode >= 400 && restErr.Response.StatusCode < 500 {
			writeErr(w, restErr.Response.StatusCode, fmt.Sprintf("Discord client error: %v", err))
			return
		}
	}
	if strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "cannot send") {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("Discord client error: %v", err))
		return
	}
	writeErr(w, http.StatusInternalServerError, fmt.Sprintf("%s: %v", failPrefix, err))
}

func requirePostWithPool(w http.ResponseWriter, r *http.Request, pool *queue.WorkerPool) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
		return false
	}
	if pool == nil {
		writeErr(w, http.StatusInternalServerError, "Worker pool not available")
		return false
	}
	return true
}

func handleDirectMessage(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePostWithPool(w, r, pool) {
			return
		}

		var req DirectMessageRequest
		if !readPostJSONWithLimit(w, r, &req, 12<<20) {
			return
		}

		targetID := strings.TrimSpace(req.ThreadID)
		if targetID == "" {
			targetID = strings.TrimSpace(req.ChannelID)
		}
		if !queue.IsNumericSnowflake(targetID) && targetID != "" {
			if resolved, err := pool.ResolveChannelID(targetID); err == nil {
				targetID = resolved
			}
		}
		if !validateSnowflakeID(w, targetID, "channel_id") {
			return
		}

		if len(req.Attachments) > delivery.MaxAttachmentsPerMessage {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("Too many attachments: maximum is %d", delivery.MaxAttachmentsPerMessage))
			return
		}

		var attachments []*delivery.Attachment
		for _, att := range req.Attachments {
			filename := strings.TrimSpace(att.Filename)
			if filename == "" {
				writeErr(w, http.StatusBadRequest, "Invalid attachment: 'filename' is required")
				return
			}
			data, err := base64.StdEncoding.DecodeString(att.ContentBase64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, fmt.Sprintf("Invalid attachment base64: %v", err))
				return
			}
			if len(data) == 0 {
				writeErr(w, http.StatusBadRequest, "Invalid attachment: payload is empty")
				return
			}
			if int64(len(data)) > delivery.MaxAttachmentSizeBytes {
				writeErr(w, http.StatusBadRequest, fmt.Sprintf("Attachment %s exceeds maximum size of %d bytes", filename, delivery.MaxAttachmentSizeBytes))
				return
			}
			attachments = append(attachments, &delivery.Attachment{
				Filename:    filename,
				ContentType: strings.TrimSpace(att.ContentType),
				Data:        data,
			})
		}

		content := strings.TrimSpace(req.Content)
		if content == "" {
			content = strings.TrimSpace(req.Text)
		}
		if content == "" && len(attachments) == 0 {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'content' field is required and cannot be empty")
			return
		}

		if err := pool.DeliverDirectWithAttachments(targetID, content, attachments); err != nil {
			writeDiscordDeliveryError(w, err, "Failed to deliver message")
			return
		}

		httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{"status": "sent", "channel_id": targetID})
	}
}

func handleStatusWidget(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePostWithPool(w, r, pool) {
			return
		}

		var req discord.StatusWidgetRequest
		if !readPostJSON(w, r, &req) {
			return
		}

		targetID := strings.TrimSpace(req.ChannelID)
		if !validateSnowflakeID(w, targetID, "channel_id") {
			return
		}

		if len(req.Chunks) == 0 {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'chunks' field is required and cannot be empty")
			return
		}

		msgID, err := pool.DeliverStatusWidget(req)
		if err != nil {
			writeDiscordDeliveryError(w, err, "Failed to deliver widget")
			return
		}

		httpkit.WriteJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "message_id": msgID})
	}
}

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

func handleVoiceAskSSE(w http.ResponseWriter, r *http.Request, pool *queue.WorkerPool, req VoiceAskRequest, sessionID string, reqStart time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "Streaming not supported by client connection"})
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

		if event == "reply" || event == "delta" || event == "sentence" {
			recordTTFR("success")
		} else if event == "error" {
			recordTTFR("error")
		}
	}

	onStatus := func(status string) {
		emitSSE("status", map[string]string{"status": status})
	}

	onSentence := func(sentence string) {
		emitSSE("sentence", map[string]string{"text": sentence})
	}

	if pool == nil {
		emitSSE("error", map[string]string{"error": "worker pool is uninitialized"})
		emitSSE("done", map[string]any{})
		return
	}

	reply, convID, turnErr := pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, onStatus, onSentence)
	if turnErr != nil {
		if r.Context().Err() == nil {
			sanitizedErr := sanitizer.SanitizeString(turnErr.Error())
			emitSSE("error", map[string]string{"error": sanitizedErr})
			emitSSE("done", map[string]any{})
		}
		return
	}

	emitSSE("reply", map[string]string{"reply": reply, "conversation_id": convID})
	emitSSE("done", map[string]any{"conversation_id": convID})
}

func handleVoiceAsk(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqStart := time.Now()
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			httpkit.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		var req VoiceAskRequest
		if !readPostJSON(w, r, &req) {
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		sessionID, errMsg := resolveVoiceAskSessionID(req)
		if errMsg != "" {
			httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{
				"error": errMsg,
			})
			return
		}

		isSSE := strings.Contains(r.Header.Get("Accept"), "text/event-stream") || r.URL.Query().Get("stream") == "true"
		if isSSE {
			handleVoiceAskSSE(w, r, pool, req, sessionID, reqStart)
			return
		}

		// Standard JSON response mode
		if pool == nil {
			metrics.RecordVoiceTTFR("json", "error", time.Since(reqStart))
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "worker pool is uninitialized"})
			return
		}

		reply, convID, turnErr := pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, nil)
		if turnErr != nil {
			metrics.RecordVoiceTTFR("json", "error", time.Since(reqStart))
			sanitizedErr := sanitizer.SanitizeString(turnErr.Error())
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": sanitizedErr})
			return
		}

		metrics.RecordVoiceTTFR("json", "success", time.Since(reqStart))
		httpkit.WriteJSON(w, http.StatusOK, VoiceAskResponse{
			Reply:          reply,
			ConversationID: convID,
			SessionID:      convID,
			NodeID:         req.NodeID,
		})
	}
}

func requireGetWithStore(w http.ResponseWriter, r *http.Request, storeNil bool) bool {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
		return false
	}
	if storeNil {
		writeErr(w, http.StatusInternalServerError, "Database not available")
		return false
	}
	return true
}

func handleTranscriptSearch(store db.TranscriptStore, embedder transcript.EmbedderFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireGetWithStore(w, r, store == nil) {
			return
		}

		q := r.URL.Query().Get("q")
		if q == "" {
			q = r.URL.Query().Get("query")
		}
		mode := r.URL.Query().Get("mode")
		tool := r.URL.Query().Get("tool")
		session := r.URL.Query().Get("session")
		limitStr := r.URL.Query().Get("limit")
		limit := 10
		if limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}

		var since time.Time
		if sinceStr := strings.TrimSpace(r.URL.Query().Get("since")); sinceStr != "" {
			if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
				since = t
			}
		}

		res, err := transcript.ExecuteTranscriptSearch(r.Context(), store, embedder, transcript.SearchOptions{
			Query:         q,
			Mode:          mode,
			SessionFilter: session,
			SessionID:     session,
			ToolFilter:    tool,
			Limit:         limit,
			Since:         since,
		})
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}

		httpkit.WriteJSON(w, http.StatusOK, res)
	}
}

func handleTranscriptStats(store db.TranscriptStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireGetWithStore(w, r, store == nil) {
			return
		}

		syncStates, err := store.GetSessionSyncStates(r.Context())
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}

		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"total_sessions": len(syncStates),
			"status":         "ok",
		})
	}
}

func handleTranscriptSteps(store db.TranscriptStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireGetWithStore(w, r, store == nil) {
			return
		}

		session := strings.TrimSpace(r.URL.Query().Get("session"))
		if session == "" {
			writeErr(w, http.StatusBadRequest, "session parameter is required")
			return
		}

		stepStr := strings.TrimSpace(r.URL.Query().Get("step"))
		if stepStr == "" {
			writeErr(w, http.StatusBadRequest, "step parameter is required")
			return
		}
		step, err := strconv.Atoi(stepStr)
		if err != nil || step < 0 {
			writeErr(w, http.StatusBadRequest, "step parameter must be a non-negative integer")
			return
		}

		contextWindow := 2
		contextStr := strings.TrimSpace(r.URL.Query().Get("context"))
		if contextStr != "" {
			c, err := strconv.Atoi(contextStr)
			if err != nil {
				writeErr(w, http.StatusBadRequest, "context parameter must be an integer")
				return
			}
			contextWindow = c
		}

		res, err := transcript.GetTranscriptStepWindow(r.Context(), store, session, step, contextWindow)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}

		httpkit.WriteJSON(w, http.StatusOK, res)
	}
}

func setNoCacheJSONHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
}

func handleInternalLLM(llmFn func(context.Context, string, string) (string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if llmFn == nil {
			writeErr(w, http.StatusServiceUnavailable, "LLM runner not available")
			return
		}
		var req struct {
			Prompt string `json:"prompt"`
		}
		if !readPostJSON(w, r, &req) {
			return
		}
		prompt := strings.TrimSpace(req.Prompt)
		if prompt == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty")
			return
		}
		resp, err := llmFn(r.Context(), "", prompt)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"response": resp})
	}
}

func newHTTPMemoryRetriever(baseURL string) runner.MemoryRetriever {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = strings.TrimRight(strings.TrimSpace(os.Getenv("MEMORY_MCP_URL")), "/")
	}
	if baseURL == "" {
		baseURL = "http://coven.aerial"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	return func(ctx context.Context, query string) (string, error) {
		q := strings.TrimSpace(query)
		if q == "" {
			return "", nil
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/context?q="+url.QueryEscape(q)+"&limit=5", nil)
		if err != nil {
			return "", err
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("coven returned status %d", resp.StatusCode)
		}
		var out struct {
			Context string `json:"context"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", err
		}
		return out.Context, nil
	}
}

type TasksResponse struct {
	Status string          `json:"status"`
	Total  int             `json:"total"`
	Tasks  []db.ActiveTask `json:"tasks"`
}

type tasksCache struct {
	mu        sync.Mutex
	tasks     []db.ActiveTask
	expiresAt time.Time
}

func parseTasksFilter(q url.Values) (db.TasksFilter, bool) {
	var filter db.TasksFilter
	hasFilter := false

	if statusParam := strings.TrimSpace(q.Get("status")); statusParam != "" {
		for _, p := range strings.Split(statusParam, ",") {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				filter.Statuses = append(filter.Statuses, trimmed)
			}
		}
		if len(filter.Statuses) > 0 {
			hasFilter = true
		}
	}
	if threadID := strings.TrimSpace(q.Get("thread_id")); threadID != "" {
		filter.ThreadID = threadID
		hasFilter = true
	}
	if adminParam := strings.TrimSpace(q.Get("is_admin")); adminParam != "" {
		if val, err := strconv.ParseBool(adminParam); err == nil {
			filter.IsAdmin = &val
			hasFilter = true
		}
	}
	if sinceParam := strings.TrimSpace(q.Get("since")); sinceParam != "" {
		if t, err := time.Parse(time.RFC3339, sinceParam); err == nil {
			filter.Since = t
			hasFilter = true
		}
	}
	if afterParam := strings.TrimSpace(q.Get("after_row_id")); afterParam != "" {
		if id, err := strconv.ParseInt(afterParam, 10, 64); err == nil {
			filter.AfterRowID = id
			hasFilter = true
		}
	}
	if beforeParam := strings.TrimSpace(q.Get("before_row_id")); beforeParam != "" {
		if id, err := strconv.ParseInt(beforeParam, 10, 64); err == nil {
			filter.BeforeRowID = id
			hasFilter = true
		}
	}
	if limitParam := strings.TrimSpace(q.Get("limit")); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 {
			filter.Limit = l
			hasFilter = true
		}
	}
	return filter, hasFilter
}

func sanitizeActiveTasks(rawTasks []db.ActiveTask, poolOpt ...*queue.WorkerPool) []db.ActiveTask {
	var pool *queue.WorkerPool
	if len(poolOpt) > 0 {
		pool = poolOpt[0]
	}
	tasks := make([]db.ActiveTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		if pool != nil && task.Status == db.StatusProcessing {
			if liveSess, ok := pool.ActiveTurnSessionID(task.ID); ok {
				task.SessionID = liveSess
			}
		}
		task.Prompt = sanitizer.SanitizeString(task.Prompt)
		task.Summary = sanitizer.SanitizeString(task.Summary)
		task.ResponseText = sanitizer.SanitizeString(task.ResponseText)
		task.ThreadSummary = sanitizer.SanitizeString(task.ThreadSummary)
		task.AuthorName = sanitizer.SanitizeString(sanitizer.SanitizeAuthor(task.AuthorName))
		tasks = append(tasks, task)
	}
	return tasks
}

func handleTasks(store db.Store, poolOpt ...*queue.WorkerPool) http.HandlerFunc {
	cache := &tasksCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if !requireGetWithStore(w, r, store == nil) {
			return
		}

		filter, hasFilter := parseTasksFilter(r.URL.Query())
		now := time.Now().UTC()

		if !hasFilter {
			cache.mu.Lock()
			if now.Before(cache.expiresAt) && cache.tasks != nil {
				cached := cache.tasks
				cache.mu.Unlock()
				setNoCacheJSONHeaders(w)
				httpkit.WriteJSON(w, http.StatusOK, TasksResponse{
					Status: "ok",
					Total:  len(cached),
					Tasks:  cached,
				})
				return
			}
			cache.mu.Unlock()
		}

		var rawTasks []db.ActiveTask
		var err error
		if hasFilter {
			rawTasks, err = store.GetActiveTasks(r.Context(), filter)
		} else {
			rawTasks, err = store.GetActiveTasks(r.Context())
		}
		if err != nil {
			writeStoreErr(w, "active tasks", err)
			return
		}

		tasks := sanitizeActiveTasks(rawTasks, poolOpt...)

		if !hasFilter {
			cache.mu.Lock()
			cache.tasks = tasks
			cache.expiresAt = now.Add(1 * time.Second)
			cache.mu.Unlock()
		}

		setNoCacheJSONHeaders(w)
		httpkit.WriteJSON(w, http.StatusOK, TasksResponse{
			Status: "ok",
			Total:  len(tasks),
			Tasks:  tasks,
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func normalizeRoute(path string) string {
	switch {
	case path == "/prompt", path == "/prompt/cancel", path == "/health", path == "/metrics", path == "/internal/llm":
		return path
	case path == "/voice/ask" || path == "/api/voice/ask":
		return "/voice/ask"
	case strings.HasPrefix(path, "/transcripts/") || strings.HasPrefix(path, "/api/transcripts"):
		return "/transcripts"
	case path == "/tasks" || strings.HasPrefix(path, "/tasks/"):
		return "/tasks"
	default:
		return "unmatched"
	}
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.HTTPInFlightRequests.Inc()
		defer metrics.HTTPInFlightRequests.Dec()

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)

		endpoint := normalizeRoute(r.URL.Path)
		method := r.Method
		if method == "" {
			method = "GET"
		}
		statusStr := strconv.Itoa(rec.statusCode)

		metrics.RecordHTTPRequest(endpoint, method, statusStr, time.Since(start))
	})
}

func SetupBrainMuxWithEmbedder(store db.Store, pool *queue.WorkerPool, embedder transcript.EmbedderFunc, searchPaths ...string) *http.ServeMux {
	_ = searchPaths
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/prompt", handlePrompt(store, pool))
	mux.HandleFunc("/prompt/cancel", handleCancelPrompt(store))
	mux.HandleFunc("/discord/message", handleDirectMessage(pool))
	mux.HandleFunc("/discord/widget", handleStatusWidget(pool))
	mux.HandleFunc("/voice/ask", handleVoiceAsk(pool))
	mux.HandleFunc("/api/voice/ask", handleVoiceAsk(pool))
	mux.HandleFunc("/transcripts/search", handleTranscriptSearch(store, embedder))
	mux.HandleFunc("/api/transcripts/search", handleTranscriptSearch(store, embedder))
	mux.HandleFunc("/transcripts/stats", handleTranscriptStats(store))
	mux.HandleFunc("/api/transcripts/stats", handleTranscriptStats(store))
	mux.HandleFunc("/transcripts/steps", handleTranscriptSteps(store))
	mux.HandleFunc("/api/transcripts/steps", handleTranscriptSteps(store))
	mux.HandleFunc("/tasks", handleTasks(store, pool))
	var llmFn func(context.Context, string, string) (string, error)
	if pool != nil {
		llmFn = pool.LLMFunc()
	}
	mux.HandleFunc("/internal/llm", handleInternalLLM(llmFn))
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("eri"))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			log.Printf("[HTTP] Failed to write health response: %v", err)
		}
	})
	return mux
}
