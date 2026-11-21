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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/githubproxy"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

type PromptRequest struct {
	Prompt       string `json:"prompt"`
	ChannelID    string `json:"channel_id"`
	Title        string `json:"title,omitempty"`
	MessageID    string `json:"message_id,omitempty"`
	Model        string `json:"model,omitempty"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	httpkit.WriteJSON(w, status, map[string]string{jsonKeyError: msg})
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
	if strings.Contains(strings.ToLower(model), "medium") {
		return config.EffortMedium
	}
	if strings.Contains(strings.ToLower(model), "flash") {
		return config.EffortLow
	}
	return ""
}

func checkPromptActiveTurn(ctx context.Context, store db.Store, skipIfActive bool, channelID string) (bool, error) {
	if !skipIfActive || store == nil {
		return false, nil
	}
	return store.HasActiveThreadTurn(ctx, channelID)
}

func resolvePromptChannelID(channelID string, pool *queue.WorkerPool) string {
	if channelID = strings.TrimSpace(channelID); channelID != "" && !queue.IsNumericSnowflake(channelID) && pool != nil {
		if resolved, err := pool.ResolveChannelID(channelID); err == nil {
			return resolved
		}
	}
	return channelID
}

func handleDuplicatePrompt(ctx context.Context, w http.ResponseWriter, store db.Store, rawID, channelID string) bool {
	msgID := strings.TrimSpace(rawID)
	if msgID == "" || store == nil {
		return false
	}
	exists, err := store.MessageExists(ctx, msgID)
	if err != nil {
		log.Printf("[HTTP] Failed to check existing prompt message %s: %v", msgID, err)
		writeErr(w, http.StatusInternalServerError, "Failed to check existing prompt message")
		return true
	}
	if exists {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			jsonKeyStatus:    statusAlreadyExists,
			jsonKeyChannelID: channelID,
			jsonKeySessionID: channelID,
			jsonKeyMessageID: msgID,
		})
		return true
	}
	return false
}

func handleRestatePrompt(ctx context.Context, w http.ResponseWriter, client *RestateIngressClient, req PromptRequest, channelID string) {
	msgID := strings.TrimSpace(req.MessageID)
	if msgID == "" {
		msgID = uuid.New().String()
	}
	status, err := handlePromptViaRestate(ctx, client, req, channelID, msgID)
	if err != nil {
		log.Printf("[HTTP] Failed to enqueue prompt via restate: %v", err)
		writeErr(w, http.StatusInternalServerError, "Failed to enqueue prompt: "+err.Error())
		return
	}
	httpCode := http.StatusAccepted
	if status == statusSkippedActive || status == statusAlreadyExists {
		httpCode = http.StatusOK
	}
	httpkit.WriteJSON(w, httpCode, map[string]string{
		jsonKeyStatus:    status,
		jsonKeyChannelID: channelID,
		jsonKeySessionID: channelID,
		jsonKeyMessageID: msgID,
		"message":        "Prompt execution enqueued in background",
	})
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
		if !validateSnowflakeID(w, channelID, jsonKeyChannelID) {
			return
		}

		if pool != nil {
			channelID = pool.RedirectBuildPrompt(channelID, req.Prompt)
		}

		if err := validatePromptEffort(req.Effort); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}

		model := strings.TrimSpace(req.Model)
		effort := resolvePromptEffort(model, strings.TrimSpace(req.Effort))
		req.Model = model
		req.Effort = effort

		if pool != nil {
			channelID = pool.ResolveTargetThread(channelID, strings.TrimSpace(req.Title), req.Prompt, time.Now().UTC())
		}

		active, err := checkPromptActiveTurn(r.Context(), store, req.SkipIfActive, channelID)
		if err != nil {
			log.Printf("[HTTP] Failed to check active turn for %s: %v", channelID, err)
			writeErr(w, http.StatusInternalServerError, "Failed to check active turn")
			return
		}
		if active {
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{
				jsonKeyStatus:    "skipped_active",
				jsonKeyChannelID: channelID,
				jsonKeySessionID: channelID,
			})
			return
		}

		if handleDuplicatePrompt(r.Context(), w, store, req.MessageID, channelID) {
			return
		}

		if client := resolveRestateClient(); client != nil {
			handleRestatePrompt(r.Context(), w, client, req, channelID)
			return
		}

		msgID := strings.TrimSpace(req.MessageID)
		if msgID == "" {
			msgID = uuid.New().String()
		}

		msg := db.Message{
			ID:          msgID,
			ThreadID:    channelID,
			GuildID:     "",
			AuthorID:    "http-client",
			AuthorName:  "HTTP Client",
			IsAdmin:     true,
			Content:     req.Prompt,
			Summary:     db.CleanTaskSummary(req.Prompt),
			Status:      db.StatusPending,
			Effort:      effort,
			ChannelID:   channelID,
			Model:       model,
			Traceparent: tracing.Traceparent(r.Context()),
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}

		if store == nil {
			writeErr(w, http.StatusInternalServerError, "Database store not configured")
			return
		}

		insertCtx, insertCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second) // persists even if the caller disconnects
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
			jsonKeyStatus:    "accepted",
			jsonKeyChannelID: channelID,
			jsonKeySessionID: channelID,
			jsonKeyMessageID: msgID,
			"message":        "Prompt execution enqueued in background",
		})
	}
}

func resolveCancelReason(reason, buildID, dispatchID string) string {
	if reason = strings.TrimSpace(reason); reason != "" {
		return reason
	}
	if buildID != "" {
		return "cancelled vanadis build " + buildID
	}
	return "cancelled dispatch " + dispatchID
}

type DispatchRequest struct {
	ID             string `json:"id,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	Model          string `json:"model,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
}

func checkDuplicateDispatch(ctx context.Context, store db.Store, dispatchID string) (bool, error) {
	existing, err := store.GetDispatch(ctx, dispatchID)
	if err != nil {
		return false, err
	}
	return existing != nil, nil
}

func handleSkipActiveDispatch(ctx context.Context, w http.ResponseWriter, store db.Store, req *DispatchRequest, sessionID, dispatchID string) bool {
	if !req.SkipIfActive || store == nil {
		return false
	}
	active, err := store.HasActiveDispatchTurn(ctx, sessionID, dispatchID)
	if err != nil {
		log.Printf("[HTTP] Failed to check active turn for %s: %v", sessionID, err)
		writeErr(w, http.StatusInternalServerError, "Failed to check active turn")
		return true
	}
	if active {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			jsonKeyStatus:     "skipped_active",
			jsonKeyDispatchID: dispatchID,
			jsonKeySessionID:  sessionID,
		})
		return true
	}
	return false
}

func handleDuplicateDispatch(ctx context.Context, w http.ResponseWriter, store db.Store, rawID, dispatchID, sessionID string) bool {
	if strings.TrimSpace(rawID) == "" || store == nil {
		return false
	}
	exists, err := checkDuplicateDispatch(ctx, store, dispatchID)
	if err != nil {
		log.Printf("[HTTP] Failed to check existing dispatch %s: %v", dispatchID, err)
		writeErr(w, http.StatusInternalServerError, "Failed to check existing dispatch")
		return true
	}
	if exists {
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			jsonKeyStatus: statusAlreadyExists, jsonKeyDispatchID: dispatchID,
			jsonKeyMessageID: dispatchID, jsonKeySessionID: sessionID,
		})
		return true
	}
	return false
}

func buildDispatchRecord(req DispatchRequest, dispatchID, sessionID, traceparent string) db.Dispatch {
	return db.Dispatch{
		ID:             dispatchID,
		Source:         strings.ToLower(strings.TrimSpace(req.Source)),
		SessionID:      sessionID,
		TargetID:       req.TargetID,
		VanadisChunkID: req.VanadisChunkID,
		Prompt:         req.Prompt,
		Summary:        db.CleanTaskSummary(req.Prompt),
		Effort:         strings.ToLower(strings.TrimSpace(req.Effort)),
		Model:          strings.TrimSpace(req.Model),
		Status:         db.StatusPending,
		Traceparent:    traceparent,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
}

func handleRestateDispatch(ctx context.Context, w http.ResponseWriter, client *RestateIngressClient, req DispatchRequest, sessionID, dispatchID string) {
	sessionKey := sessionID
	if sessionKey == "" {
		sessionKey = queue.ResolveDispatchSessionKey(queue.BrainDispatchRequest{
			SessionID:      req.SessionID,
			VanadisChunkID: req.VanadisChunkID,
			ID:             dispatchID,
		})
	}
	status, err := handleDispatchViaRestate(ctx, client, req, sessionKey, dispatchID)
	if err != nil {
		log.Printf("[HTTP] Failed to enqueue dispatch via restate: %v", err)
		writeErr(w, http.StatusInternalServerError, "Failed to enqueue dispatch: "+err.Error())
		return
	}
	httpCode := http.StatusAccepted
	if status == statusSkippedActive || status == statusAlreadyExists {
		httpCode = http.StatusOK
	}
	httpkit.WriteJSON(w, httpCode, map[string]string{
		jsonKeyStatus:     status,
		jsonKeyDispatchID: dispatchID,
		jsonKeyMessageID:  dispatchID,
		jsonKeySessionID:  sessionKey,
	})
}

func handleDispatch(store db.Store, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req DispatchRequest
		if !readPostJSON(w, r, &req) {
			return
		}

		if err := validateDispatchRequest(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}

		sessionID := strings.TrimSpace(req.SessionID)
		dispatchID := strings.TrimSpace(req.ID)
		if dispatchID == "" {
			dispatchID = uuid.New().String()
		}

		if handleDuplicateDispatch(r.Context(), w, store, req.ID, dispatchID, sessionID) {
			return
		}

		if handleSkipActiveDispatch(r.Context(), w, store, &req, sessionID, dispatchID) {
			return
		}

		if client := resolveRestateClient(); client != nil {
			handleRestateDispatch(r.Context(), w, client, req, sessionID, dispatchID)
			return
		}

		if store == nil {
			writeErr(w, http.StatusInternalServerError, "Database store not configured")
			return
		}

		d := buildDispatchRecord(req, dispatchID, sessionID, tracing.Traceparent(r.Context()))

		insertCtx, insertCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second) // persists even if the caller disconnects
		inserted, err := store.InsertDispatch(insertCtx, d)
		if err != nil {
			insertCancel()
			log.Printf("Failed to insert dispatch %s to DB: %v", dispatchID, err)
			writeErr(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist dispatch: %v", err))
			return
		}
		insertCancel()

		if !inserted {
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{
				jsonKeyStatus:     statusAlreadyExists,
				jsonKeyDispatchID: dispatchID,
				jsonKeyMessageID:  dispatchID,
				jsonKeySessionID:  sessionID,
			})
			return
		}

		if pool != nil {
			pool.EnqueueDispatch(d)
		}

		httpkit.WriteJSON(w, http.StatusAccepted, map[string]string{
			jsonKeyStatus:     "accepted",
			jsonKeyDispatchID: dispatchID,
			jsonKeyMessageID:  dispatchID,
			jsonKeySessionID:  sessionID,
		})
	}
}

type CancelDispatchRequest struct {
	DispatchID string `json:"dispatch_id,omitempty"`
	MessageID  string `json:"message_id,omitempty"`
	BuildID    string `json:"build_id,omitempty"`
	ChunkID    string `json:"chunk_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

func handleCancelDispatch(store db.Store, pools ...*queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var req CancelDispatchRequest
		if !readPostJSON(w, r, &req) {
			return
		}
		dispatchID := strings.TrimSpace(req.DispatchID)
		if dispatchID == "" {
			dispatchID = strings.TrimSpace(req.MessageID)
		}
		buildID := strings.TrimSpace(req.BuildID)
		// Vanadis routes chunk cancellation via ChunkID; mapped to buildID to preserve the db.Store signature.
		if buildID == "" {
			buildID = strings.TrimSpace(req.ChunkID)
		}
		if dispatchID == "" && buildID == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'dispatch_id' (or 'message_id') or 'build_id' is required")
			return
		}
		reason := resolveCancelReason(req.Reason, buildID, dispatchID)
		if client := resolveRestateClient(); client != nil {
			count, ids, err := handleCancelDispatchViaRestate(r.Context(), client, dispatchID, buildID, reason)
			if err != nil {
				log.Printf("[HTTP] Failed to cancel dispatch via restate: %v", err)
				writeErr(w, http.StatusInternalServerError, "Failed to cancel dispatch: "+err.Error())
				return
			}
			httpkit.WriteJSON(w, http.StatusOK, map[string]any{jsonKeyStatus: "ok", "cancelled_count": count, "cancelled_ids": ids})
			return
		}
		if store == nil {
			writeErr(w, http.StatusInternalServerError, "Database store not configured")
			return
		}
		cancelledIDs, err := store.CancelPendingDispatches(r.Context(), dispatchID, buildID, reason)
		if err != nil {
			writeStoreErr(w, "cancelling dispatches", err)
			return
		}
		if len(pools) > 0 && pools[0] != nil {
			for _, pid := range pools[0].CancelDispatches(dispatchID, buildID, reason) {
				if !slices.Contains(cancelledIDs, pid) {
					cancelledIDs = append(cancelledIDs, pid)
				}
			}
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{jsonKeyStatus: "ok", "cancelled_count": len(cancelledIDs), "cancelled_ids": cancelledIDs})
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
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{jsonKeyError: "Failed to read request body"})
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{jsonKeyError: "Invalid JSON payload"})
		return false
	}
	return true
}

func writeDiscordDeliveryError(w http.ResponseWriter, err error, failPrefix string) {
	if strings.Contains(err.Error(), "discord session is not connected") || strings.Contains(err.Error(), "discord session is nil") {
		writeErr(w, http.StatusServiceUnavailable, "Discord session not connected")
		return
	}
	if restErr, ok := errors.AsType[*discordgo.RESTError](err); ok {
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

func parseDirectMessageAttachment(att DirectAttachmentPayload) (*delivery.Attachment, error) {
	filename := strings.TrimSpace(att.Filename)
	if filename == "" {
		return nil, errors.New("Invalid attachment: 'filename' is required")
	}
	data, err := base64.StdEncoding.DecodeString(att.ContentBase64)
	if err != nil {
		return nil, fmt.Errorf("Invalid attachment base64: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("Invalid attachment: payload is empty")
	}
	if int64(len(data)) > delivery.MaxAttachmentSizeBytes {
		return nil, fmt.Errorf("Attachment %s exceeds maximum size of %d bytes", filename, delivery.MaxAttachmentSizeBytes)
	}
	return &delivery.Attachment{
		Filename:    filename,
		ContentType: strings.TrimSpace(att.ContentType),
		Data:        data,
	}, nil
}

func parseDirectMessageAttachments(raw []DirectAttachmentPayload) ([]*delivery.Attachment, error) {
	if len(raw) > delivery.MaxAttachmentsPerMessage {
		return nil, fmt.Errorf("Too many attachments: maximum is %d", delivery.MaxAttachmentsPerMessage)
	}
	attachments := make([]*delivery.Attachment, 0, len(raw))
	for _, item := range raw {
		att, err := parseDirectMessageAttachment(item)
		if err != nil {
			return nil, err
		}
		attachments = append(attachments, att)
	}
	return attachments, nil
}

func resolveDirectMessageTarget(pool *queue.WorkerPool, req DirectMessageRequest) string {
	targetID := strings.TrimSpace(req.ThreadID)
	if targetID == "" {
		targetID = strings.TrimSpace(req.ChannelID)
	}
	if !queue.IsNumericSnowflake(targetID) && targetID != "" {
		if resolved, err := pool.ResolveChannelID(targetID); err == nil {
			targetID = resolved
		}
	}
	return targetID
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

		targetID := resolveDirectMessageTarget(pool, req)
		if !validateSnowflakeID(w, targetID, jsonKeyChannelID) {
			return
		}

		attachments, err := parseDirectMessageAttachments(req.Attachments)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
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

		httpkit.WriteJSON(w, http.StatusOK, map[string]any{jsonKeyStatus: "sent", jsonKeyChannelID: targetID})
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

var transcriptsProxyHTTPClient = &http.Client{
	Timeout:   15 * time.Second,
	Transport: k8sauth.NewServiceAccountTransport(http.DefaultTransport, ""),
}

func resolveTranscriptsIndexerURL(cfg *config.Config, pool *queue.WorkerPool) string {
	if pool != nil && strings.TrimSpace(pool.TranscriptsIndexerURL) != "" {
		return strings.TrimRight(strings.TrimSpace(pool.TranscriptsIndexerURL), "/")
	}
	if cfg != nil {
		if cur := cfg.Current(); cur != nil && strings.TrimSpace(cur.Transcripts.IndexerURL) != "" {
			return strings.TrimRight(strings.TrimSpace(cur.Transcripts.IndexerURL), "/")
		}
	}
	return ""
}

func proxyTranscriptRequest(w http.ResponseWriter, r *http.Request, indexerURL, upstreamPath string) {
	targetURL := strings.TrimRight(indexerURL, "/") + upstreamPath
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "Failed to create proxy request: "+err.Error())
		return
	}

	for k, vv := range r.Header {
		if strings.EqualFold(k, "Connection") ||
			strings.EqualFold(k, "Transfer-Encoding") ||
			strings.EqualFold(k, "Authorization") ||
			strings.EqualFold(k, "Proxy-Authorization") ||
			strings.EqualFold(k, "Cookie") ||
			strings.EqualFold(k, "Set-Cookie") ||
			strings.EqualFold(k, "X-Api-Key") {
			continue
		}
		for _, v := range vv {
			proxyReq.Header.Add(k, v)
		}
	}

	resp, err := transcriptsProxyHTTPClient.Do(proxyReq)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "Upstream proxy error: "+err.Error())
		return
	}
	defer resp.Body.Close()

	githubproxy.StreamResponse(w, resp)
}

func handleTranscriptsSearch(indexerURL string, store db.TranscriptStore, embedder transcript.EmbedderFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if strings.TrimSpace(indexerURL) != "" {
			proxyTranscriptRequest(w, r, indexerURL, "/transcripts/search")
			return
		}
		handleTranscriptSearch(store, embedder)(w, r)
	}
}

func handleTranscriptsStats(indexerURL string, store db.TranscriptStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if strings.TrimSpace(indexerURL) != "" {
			proxyTranscriptRequest(w, r, indexerURL, "/transcripts/stats")
			return
		}
		handleTranscriptStats(store)(w, r)
	}
}

func handleTranscriptsSteps(indexerURL string, store db.TranscriptStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if strings.TrimSpace(indexerURL) != "" {
			proxyTranscriptRequest(w, r, indexerURL, "/transcripts/steps")
			return
		}
		handleTranscriptSteps(store)(w, r)
	}
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
			Stack:         strings.TrimSpace(r.URL.Query().Get("stack")),
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

		stacks, err := store.GetTranscriptStats(r.Context(), strings.TrimSpace(r.URL.Query().Get("stack")))
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if stacks == nil {
			stacks = []db.TranscriptStackStats{}
		}
		var totalSessions, totalSteps int
		for _, st := range stacks {
			totalSessions += st.Sessions
			totalSteps += st.Steps
		}

		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"total_sessions": totalSessions,
			"total_steps":    totalSteps,
			"stacks":         stacks,
			jsonKeyStatus:    "ok",
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
			c, atoiErr := strconv.Atoi(contextStr)
			if atoiErr != nil {
				writeErr(w, http.StatusBadRequest, "context parameter must be an integer")
				return
			}
			contextWindow = c
		}

		res, err := transcript.GetTranscriptStepWindow(r.Context(), store, r.URL.Query().Get("stack"), session, step, contextWindow)
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

type InternalLLMAttachment struct {
	URL      string `json:"url"`
	Filename string `json:"filename"`
}

func validateInternalLLMAttachments(atts []InternalLLMAttachment) error {
	if len(atts) > delivery.MaxInternalLLMAttachments {
		return fmt.Errorf("attachments count %d exceeds maximum of %d", len(atts), delivery.MaxInternalLLMAttachments)
	}
	seen := make(map[string]bool, len(atts))
	for _, a := range atts {
		rawURL := strings.TrimSpace(a.URL)
		filename := strings.TrimSpace(a.Filename)
		if rawURL == "" || filename == "" {
			return errors.New("attachment URL and filename cannot be empty")
		}
		lower := strings.ToLower(filename)
		if seen[lower] {
			return fmt.Errorf("duplicate attachment filename %q", filename)
		}
		seen[lower] = true
	}
	return nil
}

type stagedAttachmentFile struct {
	path     string
	mimeType string
	size     int
}

func cleanTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("[Internal LLM] Warning: failed to clean up temp dir %s: %v", dir, err)
	}
}

func stageInternalLLMAttachments(ctx context.Context, atts []InternalLLMAttachment) (string, string, int, error) {
	if len(atts) == 0 {
		return "", "", http.StatusOK, nil
	}
	if err := validateInternalLLMAttachments(atts); err != nil {
		return "", "", http.StatusBadRequest, err
	}
	tmpDir, err := os.MkdirTemp("", "eri-llm-*")
	if err != nil {
		return "", "", http.StatusInternalServerError, fmt.Errorf("failed to create temporary staging directory: %w", err)
	}
	if err := os.Chmod(tmpDir, 0o755); err != nil {
		cleanTempDir(tmpDir)
		return "", "", http.StatusInternalServerError, fmt.Errorf("failed to set permissions on temporary staging directory: %w", err)
	}

	stagedFiles := make([]stagedAttachmentFile, 0, len(atts))
	for _, a := range atts {
		att, err := delivery.ResolveAndFetchInternalAttachment(ctx, a.URL, a.Filename)
		if err != nil {
			cleanTempDir(tmpDir)
			if errors.Is(err, delivery.ErrInvalidInternalAttachment) {
				return "", "", http.StatusBadRequest, err
			}
			return "", "", http.StatusBadGateway, err
		}
		targetPath := filepath.Join(tmpDir, att.Filename)
		if err := os.WriteFile(targetPath, att.Data, 0o644); err != nil {
			cleanTempDir(tmpDir)
			return "", "", http.StatusInternalServerError, fmt.Errorf("failed to stage attachment %q: %w", att.Filename, err)
		}
		if err := os.Chmod(targetPath, 0o644); err != nil {
			cleanTempDir(tmpDir)
			return "", "", http.StatusInternalServerError, fmt.Errorf("failed to set permissions on staged attachment %q: %w", att.Filename, err)
		}
		stagedFiles = append(stagedFiles, stagedAttachmentFile{
			path:     targetPath,
			mimeType: att.ContentType,
			size:     len(att.Data),
		})
	}

	var sb strings.Builder
	sb.WriteString("\n\n<ATTACHMENTS>\n")
	fmt.Fprintf(&sb, "Staged directory: %s\n", tmpDir)
	sb.WriteString("Files are pre-downloaded and staged for direct inspection. Use read_file or view_file to examine them; do not execute curl, wget, or rm commands.\n")
	for _, f := range stagedFiles {
		fmt.Fprintf(&sb, "- %s (%s, %d bytes)\n", f.path, f.mimeType, f.size)
	}
	sb.WriteString("</ATTACHMENTS>")

	return tmpDir, sb.String(), http.StatusOK, nil
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
			Prompt      string                  `json:"prompt"`
			Attachments []InternalLLMAttachment `json:"attachments"`
			Effort      string                  `json:"effort,omitempty"`
		}
		if !readPostJSON(w, r, &req) {
			return
		}
		prompt := strings.TrimSpace(req.Prompt)
		if prompt == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty")
			return
		}
		// Effort is parsed and validated now but stays dark: the extraction
		// cutover wires it into the runner in a later chunk.
		if effort := strings.ToLower(strings.TrimSpace(req.Effort)); effort != "" && effort != config.EffortLow && effort != config.EffortMedium && effort != config.EffortHigh {
			writeErr(w, http.StatusBadRequest, "Invalid payload: 'effort' must be 'low', 'medium', or 'high'")
			return
		}

		tmpDir, attBlock, statusCode, err := stageInternalLLMAttachments(r.Context(), req.Attachments)
		if err != nil {
			writeErr(w, statusCode, err.Error())
			return
		}
		if tmpDir != "" {
			defer cleanTempDir(tmpDir)
			prompt += attBlock
		}

		resp, err := llmFn(r.Context(), "", prompt)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"response": resp})
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
		for p := range strings.SplitSeq(statusParam, ",") {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				filter.Statuses = append(filter.Statuses, trimmed)
			}
		}
		if len(filter.Statuses) > 0 {
			hasFilter = true
		}
	}
	if threadID := strings.TrimSpace(q.Get("thread_id")); threadID != "" {
		filter.ThreadID, hasFilter = threadID, true
	}
	if adminParam := strings.TrimSpace(q.Get("is_admin")); adminParam != "" {
		if val, err := strconv.ParseBool(adminParam); err == nil {
			filter.IsAdmin, hasFilter = &val, true
		}
	}
	if sinceParam := strings.TrimSpace(q.Get("since")); sinceParam != "" {
		if t, err := time.Parse(time.RFC3339, sinceParam); err == nil {
			filter.Since, hasFilter = t, true
		}
	}
	if afterParam := strings.TrimSpace(q.Get("after_row_id")); afterParam != "" {
		if id, err := strconv.ParseInt(afterParam, 10, 64); err == nil {
			filter.AfterRowID, hasFilter = id, true
		}
	}
	if beforeParam := strings.TrimSpace(q.Get("before_row_id")); beforeParam != "" {
		if id, err := strconv.ParseInt(beforeParam, 10, 64); err == nil {
			filter.BeforeRowID, hasFilter = id, true
		}
	}
	if limitParam := strings.TrimSpace(q.Get("limit")); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 {
			filter.Limit, hasFilter = l, true
		}
	}
	if orderParam := strings.ToLower(strings.TrimSpace(q.Get("order"))); orderParam != "" {
		if orderParam == db.TasksOrderAsc || orderParam == db.TasksOrderDesc {
			filter.Order, hasFilter = orderParam, true
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
				httpkit.WriteJSON(w, http.StatusOK, TasksResponse{Status: "ok", Total: len(cached), Tasks: cached})
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
		httpkit.WriteJSON(w, http.StatusOK, TasksResponse{Status: "ok", Total: len(tasks), Tasks: tasks})
	}
}

func handleInternalDiscordMessage(store db.Store, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var req DiscordMessageIngressRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "Invalid payload: "+err.Error())
			return
		}
		if req.Message == nil || req.Message.ID == "" || req.Message.Author == nil {
			writeErr(w, http.StatusBadRequest, "Invalid payload: message, id, and author are required")
			return
		}

		var s *discordgo.Session
		if pool != nil {
			s = pool.DiscordSession()
		}
		status, err := processFunnelMessageIngress(r.Context(), s, store, pool, req.Message, req.BotUserID, req.RoleNames)
		if err != nil {
			log.Printf("[Internal Ingress] Error processing message %s: %v", req.Message.ID, err)
			writeErr(w, http.StatusInternalServerError, "Failed to process message: "+err.Error())
			return
		}
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{
			jsonKeyStatus:    status,
			jsonKeyMessageID: req.Message.ID,
		})
	}
}

func handleInternalDiscordInteraction(store db.Store, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "Failed to read request body")
			return
		}
		var interaction discordgo.Interaction
		if err := json.Unmarshal(body, &interaction); err != nil || interaction.ID == "" {
			var env struct {
				Interaction *discordgo.Interaction `json:"interaction"`
			}
			if err := json.Unmarshal(body, &env); err == nil && env.Interaction != nil {
				interaction = *env.Interaction
			}
		}
		if interaction.ID == "" {
			writeErr(w, http.StatusBadRequest, "Invalid payload: interaction id is required")
			return
		}

		var dgSession *discordgo.Session
		if pool != nil {
			dgSession = pool.DiscordSession()
		}
		if dgSession == nil {
			var sessErr error
			dgSession, sessErr = discordgo.New("Bot dummy")
			if sessErr != nil {
				log.Printf("[Internal Ingress] Warning creating fallback discordgo session: %v", sessErr)
			}
		}

		ic := &discordgo.InteractionCreate{Interaction: &interaction}
		if client := resolveRestateClient(); client != nil {
			handled, restateErr := handleInteractionViaRestate(r.Context(), client, &interaction)
			if restateErr != nil {
				writeErr(w, http.StatusInternalServerError, "Restate control interaction failed: "+restateErr.Error())
				return
			}
			if handled {
				httpkit.WriteJSON(w, http.StatusOK, map[string]string{jsonKeyStatus: "ok"})
				return
			}
		}

		router := activeInteractionRouter.Load()
		if router == nil {
			router = initFunnelInteractionRouter(store, pool)
		}
		router.Handle(r.Context(), dgSession, ic)
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{jsonKeyStatus: "ok"})
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
	case path == "/prompt", path == "/dispatch", path == "/dispatch/cancel", path == "/metrics", path == "/internal/llm",
		path == "/internal/discord/message", path == "/internal/discord/interaction", path == "/internal/guard/scan":
		return path
	case path == "/health" || path == "/healthz":
		return "/health"
	case path == "/voice/ask" || path == "/api/voice/ask":
		return "/voice/ask"
	case strings.HasPrefix(path, "/transcripts/") || strings.HasPrefix(path, "/api/transcripts"):
		return "/transcripts"
	case path == "/tasks" || strings.HasPrefix(path, "/tasks/"):
		return "/tasks"
	case path == "/internal/github-api" || strings.HasPrefix(path, "/internal/github-api/"):
		return "/internal/github-api"
	case path == "/internal/github-git" || strings.HasPrefix(path, "/internal/github-git/"):
		return "/internal/github-git"
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

func SetupBrainMuxWithEmbedder(store db.Store, txStore db.TranscriptStore, pool *queue.WorkerPool, embedder transcript.EmbedderFunc, searchPaths ...string) *http.ServeMux {
	_ = searchPaths
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/prompt", handlePrompt(store, pool))
	mux.HandleFunc("/dispatch", handleDispatch(store, pool))
	mux.HandleFunc("/dispatch/cancel", handleCancelDispatch(store, pool))
	mux.HandleFunc("/discord/message", handleDirectMessage(pool))
	mux.HandleFunc("/internal/discord/message", handleInternalDiscordMessage(store, pool))
	mux.HandleFunc("/internal/discord/interaction", handleInternalDiscordInteraction(store, pool))
	mux.HandleFunc("/internal/guard/scan", handleInternalGuardScan())

	var cfg *config.Config
	if pool != nil && pool.AppConfig() != nil {
		cfg = pool.AppConfig()
	} else {
		cfg = config.ActiveConfig()
	}
	// eri-restate pods still reach this proxy cross-pod (AERIAL_INTERNAL_ERI_URL)
	// until the cutover, so plain eri mounts the shared handlers without the
	// loopback guard that githubproxy.Register applies inside eri-restate.
	mux.HandleFunc(githubproxy.APIPrefix, githubproxy.APIHandler(cfg))
	mux.HandleFunc(githubproxy.GitPrefix, githubproxy.GitHandler(cfg))

	mux.HandleFunc("/voice/ask", handleVoiceAsk(pool))
	mux.HandleFunc("/api/voice/ask", handleVoiceAsk(pool))
	indexerURL := resolveTranscriptsIndexerURL(cfg, pool)
	searchHandler := handleTranscriptsSearch(indexerURL, txStore, embedder)
	statsHandler := handleTranscriptsStats(indexerURL, txStore)
	stepsHandler := handleTranscriptsSteps(indexerURL, txStore)

	mux.HandleFunc("/transcripts/search", searchHandler)
	mux.HandleFunc("/api/transcripts/search", searchHandler)
	mux.HandleFunc("/transcripts/stats", statsHandler)
	mux.HandleFunc("/api/transcripts/stats", statsHandler)
	mux.HandleFunc("/transcripts/steps", stepsHandler)
	mux.HandleFunc("/api/transcripts/steps", stepsHandler)
	mux.HandleFunc("/tasks", handleTasks(store, pool))
	var llmFn func(context.Context, string, string) (string, error)
	if pool != nil {
		llmFn = pool.LLMFunc()
	}
	if llmFn == nil && activeRestateIngressClient.Load() != nil {
		llmFn = NewRemoteEphemeralLLMFunc(os.Getenv("ERI_RESTATE_URL"))
	}
	mux.HandleFunc("/internal/llm", handleInternalLLM(llmFn))
	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("eri"))
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			log.Printf("[HTTP] Failed to write health response: %v", err)
		}
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/healthz", healthHandler)
	return mux
}
