package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/coverage"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/ambient"
	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/mcp"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/scheduler"
	"github.com/azylman/aerial/brain/pkg/env"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/azylman/aerial/brain/pkg/transcript"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

type PromptRequest struct {
	Prompt    string `json:"prompt"`
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[HTTP] Failed to encode JSON response: %v", err)
	}
}

func handlePrompt(store db.Store, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if closeErr := r.Body.Close(); closeErr != nil {
			log.Printf("[HTTP] Warning closing request body: %v", closeErr)
		}
		if err != nil {
			http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
			return
		}

		var req PromptRequest
		if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		channelID := strings.TrimSpace(req.ChannelID)
		if channelID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'channel_id' field is required and cannot be empty",
			})
			return
		}
		if !queue.IsNumericSnowflake(channelID) {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake",
			})
			return
		}

		msgID := strings.TrimSpace(req.MessageID)
		if msgID == "" {
			msgID = uuid.New().String()
		}

		msg := db.Message{
			ID:         msgID,
			ThreadID:   channelID,
			GuildID:    "",
			AuthorID:   "http-client",
			AuthorName: "HTTP Client",
			Content:    req.Prompt,
			Summary:    db.CleanTaskSummary(req.Prompt),
			Status:     db.StatusPending,
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}

		if store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "Database store not configured",
			})
			return
		}

		insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := store.InsertMessage(insertCtx, msg); err != nil {
			insertCancel()
			log.Printf("Failed to insert HTTP prompt message %s to DB: %v", msgID, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": fmt.Sprintf("Failed to persist prompt message: %v", err),
			})
			return
		}
		insertCancel()

		if pool != nil {
			pool.Enqueue(msg)
		}

		writeJSON(w, http.StatusAccepted, map[string]string{
			"status":     "accepted",
			"channel_id": channelID,
			"message_id": msgID,
			"message":    "Prompt execution enqueued in background",
		})
	}
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	ThreadID  string `json:"thread_id,omitempty"`
	Content   string `json:"content"`
	Text      string `json:"text,omitempty"`
}

func handleDirectMessage(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if pool == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Worker pool not available"})
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if closeErr := r.Body.Close(); closeErr != nil {
			log.Printf("[HTTP] Warning closing direct message request body: %v", closeErr)
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
			return
		}

		var req DirectMessageRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
			return
		}

		targetID := strings.TrimSpace(req.ChannelID)
		if targetID == "" {
			targetID = strings.TrimSpace(req.ThreadID)
		}
		if targetID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid payload: 'channel_id' field is required and cannot be empty"})
			return
		}
		if !queue.IsNumericSnowflake(targetID) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"})
			return
		}

		content := strings.TrimSpace(req.Content)
		if content == "" {
			content = strings.TrimSpace(req.Text)
		}
		if content == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid payload: 'content' field is required and cannot be empty"})
			return
		}

		if err := pool.DeliverDirect(targetID, content); err != nil {
			if strings.Contains(err.Error(), "discord session is not connected") || strings.Contains(err.Error(), "discord session is nil") {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Discord session not connected"})
				return
			}
			var restErr *discordgo.RESTError
			if errors.As(err, &restErr) {
				if restErr.Response != nil && restErr.Response.StatusCode >= 400 && restErr.Response.StatusCode < 500 {
					writeJSON(w, restErr.Response.StatusCode, map[string]string{"error": fmt.Sprintf("Discord client error: %v", err)})
					return
				}
			}
			if strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "cannot send") {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("Discord client error: %v", err)})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to deliver message: %v", err)})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "sent", "channel_id": targetID})
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

func handleVoiceAsk(pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqStart := time.Now()
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB ceiling
		body, err := io.ReadAll(r.Body)
		if closeErr := r.Body.Close(); closeErr != nil {
			log.Printf("[HTTP] Warning closing voice request body: %v", closeErr)
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
			return
		}

		var req VoiceAskRequest
		if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		sessionID := strings.TrimSpace(req.NodeID)
		if sessionID == "" {
			sessionID = strings.TrimSpace(req.SessionID)
		}
		if sessionID == "" {
			sessionID = strings.TrimSpace(req.ConversationID)
		}
		if sessionID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: 'session_id' (or 'node_id' / 'conversation_id') is required to identify the device",
			})
			return
		}
		if len(sessionID) > 128 || strings.ContainsAny(sessionID, "\r\n\x00") {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "Invalid payload: device identifier exceeds 128 characters or contains invalid characters",
			})
			return
		}

		isSSE := strings.Contains(r.Header.Get("Accept"), "text/event-stream") || r.URL.Query().Get("stream") == "true"

		if isSSE {
			flusher, ok := w.(http.Flusher)
			if !ok {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Streaming not supported by client connection"})
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
			return
		}

		// Standard JSON response mode
		if pool == nil {
			metrics.RecordVoiceTTFR("json", "error", time.Since(reqStart))
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "worker pool is uninitialized"})
			return
		}

		reply, convID, turnErr := pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, nil)
		if turnErr != nil {
			metrics.RecordVoiceTTFR("json", "error", time.Since(reqStart))
			sanitizedErr := sanitizer.SanitizeString(turnErr.Error())
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": sanitizedErr})
			return
		}

		metrics.RecordVoiceTTFR("json", "success", time.Since(reqStart))
		writeJSON(w, http.StatusOK, VoiceAskResponse{
			Reply:          reply,
			ConversationID: convID,
			SessionID:      convID,
			NodeID:         req.NodeID,
		})
	}
}

// DefaultTranscriptRoots returns the search roots for conversation transcripts
// based on the provided configuration. In production, this includes the persistent
// data brain directory as well as CLI and Antigravity brain directories under GeminiHomeDir.
func DefaultTranscriptRoots(cfg *config.Config) []string {
	if cfg == nil {
		return []string{"/data/brain"}
	}
	var roots []string
	dataDir := strings.TrimSpace(cfg.DataDir())
	if dataDir == "" {
		dataDir = "/data"
	}
	roots = append(roots, filepath.Join(dataDir, "brain"))
	if homeDir := strings.TrimSpace(cfg.GeminiHomeDir()); homeDir != "" {
		roots = append(roots,
			filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
			filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		)
	}
	return roots
}

func handleTranscripts(store db.Store, searchPaths ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type TranscriptEntry struct {
			Path       string `json:"path"`
			ModTime    string `json:"mod_time"`
			TotalSteps int    `json:"total_steps"`
			LastStatus string `json:"last_status"`
			LastError  string `json:"last_error,omitempty"`
			ExternalID string `json:"external_id,omitempty"`
			RawJSONL   string `json:"raw_jsonl,omitempty"`
		}

		results := make([]TranscriptEntry, 0)
		seen := make(map[string]bool)
		includeRaw := r.URL.Query().Get("include_raw") == "true"

		for _, root := range searchPaths {
			root = strings.TrimSpace(root)
			if root == "" {
				continue
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}

				internalID := entry.Name()
				if seen[internalID] {
					continue
				}
				tPath := filepath.Join(root, internalID, ".system_generated", "logs", "transcript_full.jsonl")
				tStat, err := os.Stat(tPath)
				if err != nil {
					tPath = filepath.Join(root, internalID, ".system_generated", "logs", "transcript.jsonl")
					tStat, err = os.Stat(tPath)
					if err != nil {
						tStat = nil
					}
				}

				data, err := os.ReadFile(tPath)
				if err != nil {
					continue
				}

				modTime := ""
				if tStat != nil {
					modTime = tStat.ModTime().Format(time.RFC3339)
				}
				lines := strings.Split(string(data), "\n")
				totalSteps := 0
				lastStatus := "UNKNOWN"
				lastError := ""

				for _, line := range lines {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					totalSteps++

					var step struct {
						Status string `json:"status"`
						Error  any    `json:"error"`
					}
					if err := json.Unmarshal([]byte(line), &step); err == nil {
						if step.Status != "" {
							lastStatus = step.Status
						}
						if step.Error != nil {
							switch v := step.Error.(type) {
							case string:
								if trimmed := strings.TrimSpace(v); trimmed != "" {
									lastError = trimmed
								}
							case map[string]interface{}:
								if b, err := json.Marshal(v); err == nil {
									lastError = string(b)
								}
							default:
								if s := strings.TrimSpace(fmt.Sprintf("%v", v)); s != "" && s != "<nil>" {
									lastError = s
								}
							}
						}
					}
				}

				var extID string
				if store != nil {
					var err error
					extID, err = store.GetExternalConversationID(r.Context(), internalID)
					if err != nil {
						log.Printf("Warning: GetExternalConversationID error for %s: %v", internalID, err)
					}
				}

				item := TranscriptEntry{
					Path:       tPath,
					ModTime:    modTime,
					TotalSteps: totalSteps,
					LastStatus: lastStatus,
					LastError:  lastError,
					ExternalID: extID,
				}
				if includeRaw {
					item.RawJSONL = string(data)
				}
				seen[internalID] = true
				results = append(results, item)
			}
		}

		writeJSON(w, http.StatusOK, results)
	}
}

func handleTranscriptSearch(store db.TranscriptStore, embedder transcript.EmbedderFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Database not available"})
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

		res, err := transcript.SearchTranscripts(r.Context(), store, embedder, q, mode, session, tool, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, res)
	}
}

func handleTranscriptStats(store db.TranscriptStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Database not available"})
			return
		}

		syncStates, err := store.GetSessionSyncStates(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"total_sessions": len(syncStates),
			"status":         "ok",
		})
	}
}

func handleFacts(store db.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			http.Error(w, `{"error":"Database not available"}`, http.StatusInternalServerError)
			return
		}

		q := r.URL.Query()
		limit := 0
		if lStr := q.Get("limit"); lStr != "" {
			if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
				limit = l
			}
		}

		offset := 0
		if oStr := q.Get("offset"); oStr != "" {
			if o, err := strconv.Atoi(oStr); err == nil && o >= 0 {
				offset = o
			}
		}

		category := strings.TrimSpace(q.Get("category"))
		search := strings.TrimSpace(q.Get("q"))
		if runes := []rune(search); len(runes) > 64 {
			search = string(runes[:64])
		}

		filter := db.FactsFilter{
			Category: category,
			Query:    search,
			Limit:    limit,
			Offset:   offset,
		}

		result, err := store.GetFactsPaginated(r.Context(), filter)
		if err != nil {
			log.Printf("[HTTP] Error fetching facts: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch facts"})
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, http.StatusOK, result)
	}
}

// SanitizeString scrubs sensitive tokens, PATs, passwords, and credentials from text.
func SanitizeString(input string) string {
	return sanitizer.SanitizeString(input)
}

// Telemetry & Schedules Data Structures

type CronScheduleWithDesc struct {
	ID              string    `json:"id"`
	TargetID        string    `json:"target_id,omitempty"`
	ChannelID       string    `json:"channel_id"`
	TitlePrefix     string    `json:"title_prefix"`
	CronExpr        string    `json:"cron_expr"`
	CronDescription string    `json:"cron_description"`
	Prompt          string    `json:"prompt"`
	Timezone        string    `json:"timezone"`
	NextRunAt       time.Time `json:"next_run_at"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
}

type OneShotScheduleJSON struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

type SchedulesResponse struct {
	Status     string                    `json:"status"`
	SystemTime time.Time                 `json:"system_time"`
	Summary    db.ScheduleSummaryMetrics `json:"summary"`
	Crons      []CronScheduleWithDesc    `json:"crons"`
	OneShots   []OneShotScheduleJSON     `json:"one_shots"`
}

type ScheduleRunsResponse struct {
	Status string           `json:"status"`
	Total  int              `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
	Runs   []db.ScheduleRun `json:"runs"`
}

type schedulesCache struct {
	mu        sync.Mutex
	expiresAt time.Time
	summary   db.ScheduleSummaryMetrics
	crons     []CronScheduleWithDesc
	oneShots  []OneShotScheduleJSON
}

func handleSchedules(store db.Store) http.HandlerFunc {
	cache := &schedulesCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			http.Error(w, `{"error":"Database not available"}`, http.StatusInternalServerError)
			return
		}

		now := time.Now().UTC()

		cache.mu.Lock()
		defer cache.mu.Unlock()

		if now.Before(cache.expiresAt) && cache.crons != nil {
			resp := SchedulesResponse{
				Status:     "ok",
				SystemTime: now,
				Summary:    cache.summary,
				Crons:      cache.crons,
				OneShots:   cache.oneShots,
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			writeJSON(w, http.StatusOK, resp)
			return
		}

		summary, err := store.GetScheduleSummaryMetrics(r.Context())
		if err != nil {
			log.Printf("[HTTP] Error fetching schedule summary: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch schedule summary"})
			return
		}

		rawCrons, err := store.GetAllCronSchedules(r.Context(), "")
		if err != nil {
			log.Printf("[HTTP] Error fetching cron schedules: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch cron schedules"})
			return
		}

		rawOneShots, err := store.GetAllOneShotSchedules(r.Context(), "")
		if err != nil {
			log.Printf("[HTTP] Error fetching one-shot schedules: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch one-shot schedules"})
			return
		}

		crons := make([]CronScheduleWithDesc, 0, len(rawCrons))
		for _, c := range rawCrons {
			crons = append(crons, CronScheduleWithDesc{
				ID:              c.ID,
				TargetID:        c.TargetID,
				ChannelID:       c.TargetID,
				TitlePrefix:     SanitizeString(c.TitlePrefix),
				CronExpr:        c.CronExpr,
				CronDescription: FormatCronDescription(c.CronExpr),
				Prompt:          SanitizeString(c.Prompt),
				Timezone:        c.Timezone,
				NextRunAt:       c.NextRunAt,
				Enabled:         c.Enabled,
				CreatedAt:       c.CreatedAt,
			})
		}

		oneShots := make([]OneShotScheduleJSON, 0, len(rawOneShots))
		for _, s := range rawOneShots {
			oneShots = append(oneShots, OneShotScheduleJSON{
				ID:        s.ID,
				ThreadID:  s.ThreadID,
				Prompt:    SanitizeString(s.Prompt),
				RunAt:     s.RunAt,
				CreatedAt: s.CreatedAt,
			})
		}

		cache.summary = summary
		cache.crons = crons
		cache.oneShots = oneShots
		cache.expiresAt = now.Add(5 * time.Second)

		resp := SchedulesResponse{
			Status:     "ok",
			SystemTime: now,
			Summary:    summary,
			Crons:      crons,
			OneShots:   oneShots,
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, http.StatusOK, resp)
	}
}

func handleScheduleRuns(store db.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			http.Error(w, `{"error":"Database not available"}`, http.StatusInternalServerError)
			return
		}

		q := r.URL.Query()
		limit := 50
		if lStr := q.Get("limit"); lStr != "" {
			if l, err := strconv.Atoi(lStr); err == nil && l > 0 && l <= 100 {
				limit = l
			}
		}

		offset := 0
		if oStr := q.Get("offset"); oStr != "" {
			if o, err := strconv.Atoi(oStr); err == nil && o >= 0 {
				offset = o
			}
		}

		scheduleID := strings.TrimSpace(q.Get("schedule_id"))
		status := strings.TrimSpace(q.Get("status"))

		rawRuns, total, err := store.GetScheduleRunsPaginated(r.Context(), limit, offset, scheduleID, status)
		if err != nil {
			log.Printf("[HTTP] Error fetching schedule runs: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch schedule runs"})
			return
		}

		runs := make([]db.ScheduleRun, 0, len(rawRuns))
		for _, run := range rawRuns {
			run.Prompt = SanitizeString(run.Prompt)
			run.Error = SanitizeString(run.Error)
			run.Title = SanitizeString(run.Title)
			runs = append(runs, run)
		}

		resp := ScheduleRunsResponse{
			Status: "ok",
			Total:  total,
			Limit:  limit,
			Offset: offset,
			Runs:   runs,
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, http.StatusOK, resp)
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

func handleTasks(store db.Store) http.HandlerFunc {
	cache := &tasksCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			http.Error(w, `{"error":"Database not available"}`, http.StatusInternalServerError)
			return
		}

		now := time.Now().UTC()

		cache.mu.Lock()
		defer cache.mu.Unlock()

		if now.Before(cache.expiresAt) && cache.tasks != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			writeJSON(w, http.StatusOK, TasksResponse{
				Status: "ok",
				Total:  len(cache.tasks),
				Tasks:  cache.tasks,
			})
			return
		}

		rawTasks, err := store.GetActiveTasks(r.Context())
		if err != nil {
			log.Printf("[HTTP] Error fetching active tasks: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch active tasks"})
			return
		}

		tasks := make([]db.ActiveTask, 0, len(rawTasks))
		for _, task := range rawTasks {
			sanitizedPrompt := SanitizeString(task.Prompt)
			runes := []rune(sanitizedPrompt)
			if len(runes) > 500 {
				sanitizedPrompt = string(runes[:500]) + "..."
			}
			task.Prompt = sanitizedPrompt
			task.Summary = SanitizeString(task.Summary)
			task.AuthorName = SanitizeString(sanitizer.SanitizeAuthor(task.AuthorName))
			tasks = append(tasks, task)
		}

		cache.tasks = tasks
		cache.expiresAt = now.Add(1 * time.Second)

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, http.StatusOK, TasksResponse{
			Status: "ok",
			Total:  len(tasks),
			Tasks:  tasks,
		})
	}
}

type PRRegisterRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
	Title    string `json:"title,omitempty"`
	Metadata string `json:"metadata,omitempty"`
}

func handlePRRegister(store db.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
			return
		}

		if store == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Database not available"})
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
			return
		}

		var req PRRegisterRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
			return
		}

		req.Repo = strings.TrimSpace(req.Repo)
		req.Branch = strings.TrimSpace(req.Branch)
		req.HeadSHA = strings.TrimSpace(req.HeadSHA)
		req.TargetID = strings.TrimSpace(req.TargetID)
		req.Title = strings.TrimSpace(req.Title)

		if req.Repo == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "repo cannot be empty"})
			return
		}
		if req.PRNumber <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pr_number must be greater than 0"})
			return
		}
		if req.Branch == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "branch cannot be empty"})
			return
		}
		if req.HeadSHA == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "head_sha cannot be empty"})
			return
		}
		if req.TargetID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id cannot be empty"})
			return
		}

		if len(req.TargetID) < 17 || len(req.TargetID) > 20 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must be a valid Discord snowflake (17-20 digits)"})
			return
		}
		for _, c := range req.TargetID {
			if c < '0' || c > '9' {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "target_id must contain only numeric digits"})
				return
			}
		}

		rec := db.PRRecord{
			Repo:     req.Repo,
			PRNumber: req.PRNumber,
			Branch:   req.Branch,
			HeadSHA:  req.HeadSHA,
			TargetID: req.TargetID,
			Title:    req.Title,
			Status:   "open",
			Metadata: req.Metadata,
		}

		if err := store.UpsertPR(r.Context(), rec); err != nil {
			log.Printf("[HTTP] Failed to register PR #%d (%s): %v", req.PRNumber, req.Repo, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to register PR"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":    "registered",
			"repo":      req.Repo,
			"pr_number": req.PRNumber,
			"target_id": req.TargetID,
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
	case path == "/prompt":
		return "/prompt"
	case path == "/voice/ask" || path == "/api/voice/ask":
		return "/voice/ask"
	case path == "/transcripts" || strings.HasPrefix(path, "/transcripts/"):
		return "/transcripts"
	case path == "/tasks" || strings.HasPrefix(path, "/tasks/"):
		return "/tasks"
	case path == "/facts" || strings.HasPrefix(path, "/facts/"):
		return "/facts"
	case path == "/schedules":
		return "/schedules"
	case path == "/schedules/runs":
		return "/schedules/runs"
	case path == "/health":
		return "/health"
	case path == "/metrics":
		return "/metrics"
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

func SetupBrainMux(store db.Store, pool *queue.WorkerPool, searchPaths ...string) *http.ServeMux {
	return SetupBrainMuxWithEmbedder(store, pool, nil, searchPaths...)
}

var writeCountersDirFn = coverage.WriteCountersDir

func SetupBrainMuxWithEmbedder(store db.Store, pool *queue.WorkerPool, embedder transcript.EmbedderFunc, searchPaths ...string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/prompt", handlePrompt(store, pool))
	mux.HandleFunc("/discord/message", handleDirectMessage(pool))
	mux.HandleFunc("/voice/ask", handleVoiceAsk(pool))
	mux.HandleFunc("/api/voice/ask", handleVoiceAsk(pool))
	mux.HandleFunc("/transcripts", handleTranscripts(store, searchPaths...))
	mux.HandleFunc("/transcripts/search", handleTranscriptSearch(store, embedder))
	mux.HandleFunc("/api/transcripts/search", handleTranscriptSearch(store, embedder))
	mux.HandleFunc("/transcripts/stats", handleTranscriptStats(store))
	mux.HandleFunc("/api/transcripts/stats", handleTranscriptStats(store))
	mux.HandleFunc("/tasks", handleTasks(store))
	mux.HandleFunc("/facts", handleFacts(store))
	mux.HandleFunc("/schedules", handleSchedules(store))
	mux.HandleFunc("/schedules/runs", handleScheduleRuns(store))
	mux.HandleFunc("/internal/pr/register", handlePRRegister(store))
	mux.HandleFunc("/debug/coverage/flush", func(w http.ResponseWriter, r *http.Request) {
		if dir := os.Getenv("GOCOVERDIR"); dir != "" {
			if err := writeCountersDirFn(dir); err != nil {
				log.Printf("[brain] coverage flush error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok\n")); err != nil {
			log.Printf("[brain] failed to write coverage flush response: %v", err)
		}
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			log.Printf("[HTTP] Failed to write health response: %v", err)
		}
	})
	return mux
}

func InitializeBrainEnvironment(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	provisioner := env.NewFromConfig(cfg)
	if err := provisioner.Sync(ctx, cfg); err != nil {
		log.Printf("Warning: env sync error: %v", err)
		return err
	}
	return nil
}

type ReloadOption func(*reloadConfigOptions)

type reloadConfigOptions struct {
	dgSession           *discordgo.Session
	reloadSupplier      func(active *config.Config) error
	skipEnvironmentSync bool
	provisioner         *env.Provisioner
	workerPool          *queue.WorkerPool
	onReload            func(source string)
}

func WithDiscordSession(s *discordgo.Session) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.dgSession = s
	}
}

func WithReloadSupplier(fn func(active *config.Config) error) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.reloadSupplier = fn
	}
}

func WithSkipEnvironmentSync() ReloadOption {
	return func(o *reloadConfigOptions) {
		o.skipEnvironmentSync = true
	}
}

func WithProvisioner(p *env.Provisioner) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.provisioner = p
	}
}

func WithWorkerPool(pool *queue.WorkerPool) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.workerPool = pool
	}
}

func WithOnReloadCallback(fn func(source string)) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.onReload = fn
	}
}

func CreateReloadConfigFunc(cfg *config.Config, opts ...ReloadOption) func(source string) {
	options := &reloadConfigOptions{
		reloadSupplier: config.Reload,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}
	if options.provisioner == nil && cfg != nil {
		options.provisioner = env.NewFromConfig(cfg)
	}

	var reloadMu sync.Mutex
	return func(source string) {
		reloadMu.Lock()
		defer reloadMu.Unlock()
		log.Printf("[%s] Changes detected. Reloading configuration, system rules, and skills...", source)
		if cfg != nil {
			if err := options.reloadSupplier(cfg); err != nil {
				metrics.ConfigReloadsTotal.WithLabelValues(source, "failure").Inc()
				log.Printf("[%s] Warning: Failed to reload config: %v", source, err)
				if options.dgSession != nil {
					alertMsg := fmt.Sprintf("Failed to reload config:\n```\n%v\n```\nAerial has retained the Last Known Good Configuration (LKGC).", err)
					if alertErr := delivery.SendSystemAlert(options.dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", alertMsg); alertErr != nil {
						log.Printf("[%s] Warning: Failed to send system alert for config reload failure: %v", source, alertErr)
					}
				}
				if options.onReload != nil {
					options.onReload(source)
				}
				return
			}
			metrics.ConfigReloadsTotal.WithLabelValues(source, "success").Inc()
			if cur := cfg.Current(); cur != nil {
				changed := false
				if cur.GeminiHomeDir == "" {
					cur.GeminiHomeDir = DefaultGeminiHomeDir()
					changed = true
				}
				if cur.DataDir == "" {
					cur.DataDir = DefaultDataDir()
					changed = true
				}
				if changed {
					cfg.Update(cur)
				}
			}
			sanitizer.RegisterConfigTokens(cfg)

			if !options.skipEnvironmentSync && options.provisioner != nil {
				if err := options.provisioner.Sync(context.Background(), cfg); err != nil {
					log.Printf("[%s] Warning: env sync error: %v", source, err)
				}
			}

			if options.workerPool != nil {
				options.workerPool.MarkDirty()
			}
			if options.onReload != nil {
				options.onReload(source)
			}
		}
	}
}

// BrainAppOption configures optional runtime dependencies for RunBrainApp.
type BrainAppOption func(*brainAppOptions)

type brainAppOptions struct {
	store          db.Store
	processSpawner runner.DaemonSpawner
	hupChan        <-chan os.Signal
	onReload       func(source string)
}

// WithStore allows injecting a custom Store implementation (e.g. db.FakeStore for tests).
func WithStore(store db.Store) BrainAppOption {
	return func(o *brainAppOptions) {
		o.store = store
	}
}

// WithProcessSpawner allows injecting a custom DaemonSpawner (e.g. runner.NewMockDaemonSpawner for tests).
func WithProcessSpawner(spawner runner.DaemonSpawner) BrainAppOption {
	return func(o *brainAppOptions) {
		o.processSpawner = spawner
	}
}

// WithHupChannel allows injecting a signal channel for testing SIGHUP reload behavior without POSIX signal broadcasts.
func WithHupChannel(hupChan <-chan os.Signal) BrainAppOption {
	return func(o *brainAppOptions) {
		o.hupChan = hupChan
	}
}

// WithOnReload allows injecting a callback invoked when configuration reload completes (useful for event-driven tests).
func WithOnReload(fn func(source string)) BrainAppOption {
	return func(o *brainAppOptions) {
		o.onReload = fn
	}
}

// extractVoiceMCPServers extracts voice-relevant MCP servers (scheduler, common, voice) from config.
func extractVoiceMCPServers(cur *config.ConfigData) []runner.MCPServerConfig {
	serverMap := make(map[string]runner.MCPServerConfig)
	serverMap["scheduler"] = runner.MCPServerConfig{
		Name:      "scheduler",
		ServerURL: "http://scheduler-mcp:8080/mcp",
	}
	if cur != nil {
		type srvJSON struct {
			ServerURL string            `json:"serverUrl"`
			Headers   map[string]string `json:"headers,omitempty"`
		}
		parseMap := func(m map[string]json.RawMessage) {
			for name, raw := range m {
				var sj srvJSON
				if err := json.Unmarshal(raw, &sj); err == nil && strings.TrimSpace(sj.ServerURL) != "" {
					serverMap[name] = runner.MCPServerConfig{
						Name:      name,
						ServerURL: strings.TrimSpace(sj.ServerURL),
						Headers:   sj.Headers,
					}
				}
			}
		}
		parseMap(cur.McpServers.Common)
		parseMap(cur.McpServers.Voice)
	}
	result := make([]runner.MCPServerConfig, 0, len(serverMap))
	for _, s := range serverMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// extractAllMCPServers extracts MCP servers across common, discord, and voice targets from config.
func extractAllMCPServers(cur *config.ConfigData) []runner.MCPServerConfig {
	serverMap := make(map[string]runner.MCPServerConfig)
	serverMap["scheduler"] = runner.MCPServerConfig{
		Name:      "scheduler",
		ServerURL: "http://scheduler-mcp:8080/mcp",
	}
	if cur != nil {
		type srvJSON struct {
			ServerURL string            `json:"serverUrl"`
			Headers   map[string]string `json:"headers,omitempty"`
		}
		parseMap := func(m map[string]json.RawMessage) {
			for name, raw := range m {
				var sj srvJSON
				if err := json.Unmarshal(raw, &sj); err == nil && strings.TrimSpace(sj.ServerURL) != "" {
					serverMap[name] = runner.MCPServerConfig{
						Name:      name,
						ServerURL: strings.TrimSpace(sj.ServerURL),
						Headers:   sj.Headers,
					}
				}
			}
		}
		parseMap(cur.McpServers.Common)
		parseMap(cur.McpServers.Discord)
		parseMap(cur.McpServers.Voice)
	}
	result := make([]runner.MCPServerConfig, 0, len(serverMap))
	for _, s := range serverMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// buildPoolEnv returns a process environment slice with unified compiler and module
// cache directories under runtimeBase/cache when not already explicitly set.
func buildPoolEnv(baseEnv []string, runtimeBase string) []string {
	if runtimeBase == "" {
		return baseEnv
	}
	cacheBase := filepath.Join(runtimeBase, "cache")
	for _, dir := range []string{
		filepath.Join(cacheBase, "go-build"),
		filepath.Join(cacheBase, "go", "pkg", "mod"),
		filepath.Join(cacheBase, "golangci-lint"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("[WARN] Failed to create cache directory %q: %v", dir, err)
		}
	}

	envMap := make(map[string]bool)
	for _, e := range baseEnv {
		if k, _, ok := strings.Cut(e, "="); ok {
			envMap[k] = true
		}
	}

	var extra []string
	if !envMap["GOCACHE"] {
		extra = append(extra, "GOCACHE="+filepath.Join(cacheBase, "go-build"))
	}
	if !envMap["GOPATH"] {
		extra = append(extra, "GOPATH="+filepath.Join(cacheBase, "go"))
	}
	if !envMap["GOMODCACHE"] {
		extra = append(extra, "GOMODCACHE="+filepath.Join(cacheBase, "go", "pkg", "mod"))
	}
	if !envMap["GOLANGCI_LINT_CACHE"] {
		extra = append(extra, "GOLANGCI_LINT_CACHE="+filepath.Join(cacheBase, "golangci-lint"))
	}

	if len(extra) == 0 {
		return baseEnv
	}
	result := make([]string, len(baseEnv)+len(extra))
	copy(result, baseEnv)
	copy(result[len(baseEnv):], extra)
	return result
}

// createVoiceProcessPool constructs a runner.AgentPool based on cfg.VoiceEngine()
// wrapped in runner.DynamicVoicePool with a factory closure evaluating cfg.Current() dynamically.
func createVoiceProcessPool(cfg *config.Config, voiceHome string, lowEffortModel string, spawner runner.DaemonSpawner, memRetriever ...runner.MemoryRetriever) runner.AgentPool {
	var memoryRetriever runner.MemoryRetriever
	if len(memRetriever) > 0 {
		memoryRetriever = memRetriever[0]
	}
	buildPool := func() (runner.AgentPool, string, error) {
		var cur *config.ConfigData
		var voiceEngine string
		var prewarmedTargets []string
		if cfg != nil {
			cur = cfg.Current()
			voiceEngine = cfg.VoiceEngine()
			prewarmedTargets = cfg.VoicePrewarmedTargets()
		} else {
			cur = config.DefaultConfigData()
			voiceEngine = "agy"
			prewarmedTargets = []string{}
		}

		var voicePool runner.AgentPool
		var effectiveModel string
		var effectiveAPIKey string
		var mcpServers []runner.MCPServerConfig

		var ambRetriever runner.AmbientContextRetriever
		if ambCfg := cur.VoiceAmbientContext(); ambCfg != nil {
			voiceServers := extractVoiceMCPServers(cur)
			disp := mcp.NewDispatcher(voiceServers, nil)
			p, pErr := ambient.NewProvider(*ambCfg, disp)
			if pErr != nil {
				log.Printf("[INIT] Warning: failed to initialize voice ambient provider: %v", pErr)
			} else {
				go func() {
					primeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := p.Prime(primeCtx); err != nil {
						log.Printf("[INIT] Ambient context prime warning: %v", err)
					}
				}()
				ambRetriever = p.Retrieve
				log.Printf("[INIT] Voice ambient context provider initialized (tools=%d, cache_ttl=%s)", len(ambCfg.Tools), ambCfg.CacheTTL)
			}
		}

		if voiceEngine == "gemini_api" {
			apiKey := cur.HarnessAPIKey
			if apiKey == "" {
				apiKey = cur.APIKey
			}
			if apiKey == "" {
				log.Printf("[WARN] voice.engine is configured as 'gemini_api' but neither harness_api_key nor api_key is set; safely falling back to 'agy'")
				voiceEngine = "agy"
			} else {
				geminiModel := lowEffortModel
				if strings.TrimSpace(cur.Voice.Model) != "" {
					geminiModel = strings.TrimSpace(cur.Voice.Model)
				}
				effectiveModel = geminiModel
				effectiveAPIKey = apiKey
				mcpServers = extractVoiceMCPServers(cur)
				voicePool = runner.NewGeminiAPIPool(runner.GeminiAPIPoolConfig{
					APIKey:                  apiKey,
					Model:                   geminiModel,
					PrewarmedTargets:        prewarmedTargets,
					SystemPrompt:            cur.SystemPrompt,
					MCPServers:              mcpServers,
					AllowedTools:            cur.VoiceAllowedTools(),
					DataDir:                 cur.DataDir,
					MemoryRetriever:         memoryRetriever,
					AmbientContextRetriever: ambRetriever,
				})
				log.Printf("[INIT] Voice engine initialized with 'gemini_api' (model=%s, prewarmed=%v, mcp_servers=%d, allowed_tools=%d)", geminiModel, prewarmedTargets, len(mcpServers), len(cur.VoiceAllowedTools()))
			}
		}
		if voiceEngine != "gemini_api" {
			effectiveModel = lowEffortModel
			runtimeBase := cur.DataDir
			if runtimeBase == "" && cfg != nil {
				runtimeBase = cfg.DataDir()
			}
			if runtimeBase == "" && cfg != nil {
				runtimeBase = cfg.GeminiHomeDir()
			}
			voiceSessionMgr := session.New(voiceHome, runtimeBase)
			voicePool = runner.NewUnifiedProcessPool(runner.PoolConfig{
				GeminiHomeDir:           voiceHome,
				Model:                   lowEffortModel,
				AgyBin:                  cur.AgyBin,
				Cwd:                     cur.DataDir,
				Env:                     buildPoolEnv(os.Environ(), runtimeBase),
				PrewarmedTargets:        prewarmedTargets,
				MemoryRetriever:         memoryRetriever,
				AmbientContextRetriever: ambRetriever,
				SessionManager:          voiceSessionMgr,
			}, spawner)
		}

		// Compute deterministic configuration fingerprint
		var b strings.Builder
		b.WriteString("engine:")
		b.WriteString(voiceEngine)
		b.WriteString("\nmodel:")
		b.WriteString(effectiveModel)
		b.WriteString("\n")
		if effectiveAPIKey != "" {
			keyHash := sha256.Sum256([]byte(effectiveAPIKey))
			b.WriteString("apiKeyHash:")
			b.WriteString(hex.EncodeToString(keyHash[:]))
			b.WriteString("\n")
		}
		if ambCfg := cur.VoiceAmbientContext(); ambCfg != nil {
			b.WriteString("ambient_context_tools:")
			b.WriteString(fmt.Sprintf("%d", len(ambCfg.Tools)))
			b.WriteString("\nambient_context_ttl:")
			b.WriteString(ambCfg.CacheTTL)
			b.WriteString("\nambient_context_template:")
			b.WriteString(ambCfg.Template)
			b.WriteString("\nambient_context_template_path:")
			b.WriteString(ambCfg.TemplatePath)
			b.WriteString("\n")
		}
		for _, target := range prewarmedTargets {
			b.WriteString("prewarmed:")
			b.WriteString(target)
			b.WriteString("\n")
		}
		for _, s := range mcpServers {
			b.WriteString("mcp:")
			b.WriteString(s.Name)
			b.WriteString("=")
			b.WriteString(s.ServerURL)
			b.WriteString("\n")
		}
		b.WriteString("systemPrompt:")
		b.WriteString(cur.SystemPrompt)
		b.WriteString("\n")

		h := sha256.Sum256([]byte(b.String()))
		fingerprint := hex.EncodeToString(h[:])

		return voicePool, fingerprint, nil
	}

	return runner.NewDynamicVoicePool(buildPool)
}

// createVoicePool is an alias for createVoiceProcessPool.
func createVoicePool(cfg *config.Config, voiceHome string, lowEffortModel string, spawner runner.DaemonSpawner) runner.AgentPool {
	return createVoiceProcessPool(cfg, voiceHome, lowEffortModel, spawner)
}

// PrewarmProcessPoolsSequentially pre-warms process pool targets in series in the background,
// ensuring only one daemon is in the CPU-intensive startup phase at a time across all pools.
func PrewarmProcessPoolsSequentially(ctx context.Context, pools ...runner.AgentPool) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		prewarmCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		log.Printf("[INIT] Pre-warming process pools sequentially...")
		for _, p := range pools {
			if p == nil {
				continue
			}
			if err := p.Initialize(prewarmCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[WARN] Failed pre-warming pool: %v", err)
			}
			select {
			case <-prewarmCtx.Done():
				return
			default:
			}
		}
		log.Printf("[INIT] Process pool pre-warming completed.")
	}()
}

var onServerReady func(addr string)

func RunBrainApp(ctx context.Context, cfg *config.Config, opts ...BrainAppOption) error {
	if cfg == nil {
		return fmt.Errorf("brain: config cannot be nil")
	}
	if ctx.Err() != nil {
		return nil
	}

	var appOpts brainAppOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&appOpts)
		}
	}

	cur := cfg.Current()
	if appOpts.store == nil && cur.DatabaseURL == "" {
		return fmt.Errorf("database URL or path is required")
	}
	if cur.GeminiHomeDir == "" {
		return fmt.Errorf("gemini home directory is required")
	}
	if cur.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if cur.Port == "" {
		return fmt.Errorf("port is required")
	}

	provisioner := env.NewFromConfig(cfg)
	if err := InitializeBrainEnvironment(ctx, cfg); err != nil {
		log.Printf("Warning initializing brain environment: %v", err)
	}

	var store db.Store
	if appOpts.store != nil {
		store = appOpts.store
	} else {
		database, err := db.New(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
		defer func() {
			if err := database.Close(); err != nil {
				log.Printf("Error closing database: %v", err)
			}
		}()
		store = db.NewSQLStore(database)
	}

	sanitizer.RegisterConfigTokens(cfg)

	sessionMgr := session.New(cfg.GeminiHomeDir(), cfg.DataDir())

	lowEffortModel := cur.LowEffortModel
	if lowEffortModel == "" {
		lowEffortModel = "gemini-3.8-flash-low"
	}

	runtimeBase := cur.DataDir
	if runtimeBase == "" {
		runtimeBase = cfg.DataDir()
	}
	if runtimeBase == "" {
		runtimeBase = cfg.GeminiHomeDir()
	}

	discordHome := filepath.Join(runtimeBase, "runtimes", "discord")
	voiceHome := filepath.Join(runtimeBase, "runtimes", "voice")
	ephemeralHome := filepath.Join(runtimeBase, "runtimes", "ephemeral")

	poolEnv := buildPoolEnv(os.Environ(), runtimeBase)

	memClient := memory.New(cfg, sessionMgr.Roots()...)
	var memoryRetriever runner.MemoryRetriever
	if store != nil && memClient != nil {
		memoryRetriever = func(ctx context.Context, query string) (string, error) {
			queryText := memory.ExtractQueryText(query)
			if queryText == "" {
				queryText = query
			}
			return memory.RetrieveFormattedContext(ctx, store, memClient, queryText, 5)
		}
	}

	discordPrimaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:     discordHome,
		Model:             cur.Model,
		AgyBin:            cur.AgyBin,
		Cwd:               cur.DataDir,
		Env:               poolEnv,
		MemoryRetriever:   memoryRetriever,
		TranscriptRescuer: sessionMgr.ExtractResponseSince,
		SessionManager:    sessionMgr,
	}, appOpts.processSpawner)
	defer func() {
		if err := discordPrimaryPool.Close(); err != nil {
			log.Printf("[WARN] Failed to close discord primary process pool: %v", err)
		}
	}()

	ephemeralUnderlying := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:     ephemeralHome,
		Model:             lowEffortModel,
		AgyBin:            cur.AgyBin,
		Cwd:               cur.DataDir,
		Env:               poolEnv,
		PrewarmedTargets:  []string{"ephemeral:worker-0", "ephemeral:worker-1"},
		TranscriptRescuer: sessionMgr.ExtractResponseSince,
		SessionManager:    sessionMgr,
	}, appOpts.processSpawner)

	ephemeralPool := runner.NewInterchangeablePool(ephemeralUnderlying, runner.InterchangeablePoolConfig{
		WorkerCount: 2,
		KeyPrefix:   "ephemeral:worker",
	})
	defer func() {
		if err := ephemeralPool.Close(); err != nil {
			log.Printf("[WARN] Failed to close ephemeral process pool: %v", err)
		}
	}()

	discordLowEffortUnderlying := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:     discordHome,
		Model:             lowEffortModel,
		AgyBin:            cur.AgyBin,
		Cwd:               cur.DataDir,
		Env:               poolEnv,
		MemoryRetriever:   memoryRetriever,
		TranscriptRescuer: sessionMgr.ExtractResponseSince,
		PrewarmedTargets:  []string{"discord:worker-0", "discord:worker-1"},
		SessionManager:    sessionMgr,
	}, appOpts.processSpawner)

	discordLowEffortPool := runner.NewInterchangeablePool(discordLowEffortUnderlying, runner.InterchangeablePoolConfig{
		WorkerCount: 2,
		KeyPrefix:   "discord:worker",
	})
	defer func() {
		if err := discordLowEffortPool.Close(); err != nil {
			log.Printf("[WARN] Failed to close discord low effort process pool: %v", err)
		}
	}()

	var voicePool runner.AgentPool = createVoiceProcessPool(cfg, voiceHome, lowEffortModel, appOpts.processSpawner, memoryRetriever)
	defer func() {
		if err := voicePool.Close(); err != nil {
			log.Printf("[WARN] Failed to close voice process pool: %v", err)
		}
	}()

	PrewarmProcessPoolsSequentially(ctx, discordPrimaryPool, ephemeralPool, discordLowEffortPool, voicePool)

	cls := classifier.New(cfg, nil, classifier.WithProcessPool(ephemeralPool))

	var ambientProviders sync.Map
	ambientResolver := func(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
		if ambCfg == nil {
			return "", nil
		}
		toolsJSON, err := json.Marshal(ambCfg.Tools)
		if err != nil {
			log.Printf("[WorkerPool] Warning: failed to marshal ambient tools for cache key: %v", err)
		}
		key := fmt.Sprintf("%s:%s:%s:%s", ambCfg.TemplatePath, ambCfg.Template, string(toolsJSON), ambCfg.CacheTTL)
		val, ok := ambientProviders.Load(key)
		var prov *ambient.Provider
		if ok {
			if p, ok := val.(*ambient.Provider); ok {
				prov = p
			}
		}
		if prov == nil {
			var curData *config.ConfigData
			if cfg != nil {
				curData = cfg.Current()
			}
			servers := extractAllMCPServers(curData)
			disp := mcp.NewDispatcher(servers, nil)
			var pErr error
			prov, pErr = ambient.NewProvider(*ambCfg, disp)
			if pErr != nil {
				return "", fmt.Errorf("failed to create ambient provider: %w", pErr)
			}
			go func() {
				primeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := prov.Prime(primeCtx); err != nil {
					log.Printf("[WorkerPool] Ambient context prime warning: %v", err)
				}
			}()
			ambientProviders.Store(key, prov)
		}
		return prov.Retrieve(ctx)
	}

	pool := queue.New(cfg, queue.WorkerPoolConfig{
		Store:                store,
		Classifier:           cls,
		SessionManager:       sessionMgr,
		ProcessPool:          discordPrimaryPool,
		LowEffortProcessPool: discordLowEffortPool,
		VoiceProcessPool:     voicePool,
		AmbientResolver:      ambientResolver,
	})
	pool.Start()

	SetFunnelConfig(cfg)
	dgSession := connectDiscordFunnel(ctx, store, pool, cur.DiscordToken)
	if pool != nil && dgSession != nil {
		pool.SetDiscordSession(dgSession)
	}

	// Resume interrupted turns after Discord gateway session is registered
	// so poison pill and recovery notifications can reliably deliver to Discord.
	queue.RecoverInterrupted(store, pool)
	defer func() {
		log.Printf("Draining worker pool (10s timeout)...")
		pool.StopWithTimeout(10 * time.Second)
		if dgSession != nil {
			log.Printf("Closing Discord gateway session...")
			if err := dgSession.Close(); err != nil {
				log.Printf("[WARN] Failed to close Discord gateway session: %v", err)
			}
		}
	}()

	var reloadOpts []ReloadOption
	reloadOpts = append(reloadOpts, WithDiscordSession(dgSession), WithProvisioner(provisioner), WithWorkerPool(pool))
	if appOpts.onReload != nil {
		reloadOpts = append(reloadOpts, WithOnReloadCallback(appOpts.onReload))
	}
	reloadConfig := CreateReloadConfigFunc(cfg, reloadOpts...)

	// Listen for SIGHUP to trigger atomic zero-downtime reloads and worker pool bouncing
	var hupChan <-chan os.Signal
	var stopHup func()
	if appOpts.hupChan != nil {
		hupChan = appOpts.hupChan
	} else {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		hupChan = ch
		stopHup = func() {
			signal.Stop(ch)
		}
	}
	if stopHup != nil {
		defer stopHup()
	}

	hupCtx, hupCancel := context.WithCancel(ctx)
	defer hupCancel()
	go func() {
		for {
			select {
			case <-hupCtx.Done():
				return
			case sig, ok := <-hupChan:
				if !ok {
					return
				}
				if sig == syscall.SIGHUP {
					reloadConfig("SIGHUP")
				}
			}
		}
	}()

	// Start background scheduler monitor for due cron and one-shot routines
	sched, err := scheduler.New(cfg, store, pool, scheduler.NewDiscordThreadCreator(dgSession), scheduler.WithLLMFunc(ephemeralPool.EphemeralLLMFunc("ephemeral:summarizer")), scheduler.WithSessionRoots(sessionMgr.Roots()...))
	if err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}
	stopScheduler := sched.Start(ctx)
	defer stopScheduler()
	var embedder transcript.EmbedderFunc
	if cfg != nil {
		memClient := memory.New(cfg, sessionMgr.Roots()...)
		embedder = func(eCtx context.Context, text string) ([]float32, error) {
			return memClient.GenerateEmbedding(eCtx, text, false, 1)
		}
	}
	mux := SetupBrainMuxWithEmbedder(store, pool, embedder, sessionMgr.Roots()...)

	port := cur.Port
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("[WARN] Error closing listener: %v", err)
		}
	}()

	srv := &http.Server{
		Handler: metricsMiddleware(mux),
	}

	if onServerReady != nil {
		onServerReady(ln.Addr().String())
	}

	errChan := make(chan error, 1)
	go func() {
		log.Printf("Aerial Brain listening on %s (model=%s, timeout=%dm)", ln.Addr().String(), cur.Model, queue.DefaultTimeoutMinutes)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down Aerial Brain gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		log.Println("Aerial Brain shutdown complete")
		return nil
	case err := <-errChan:
		return err
	}
}

// DefaultGeminiHomeDir returns the production default base directory for .gemini files.
func DefaultGeminiHomeDir() string {
	return defaultGeminiHomeDir(os.Getenv("HOME"), testing.Testing())
}

func defaultGeminiHomeDir(home string, isTesting bool) string {
	return defaultGeminiHomeDirWithLookup(home, isTesting, os.UserHomeDir)
}

func defaultGeminiHomeDirWithLookup(home string, isTesting bool, userHomeDirFn func() (string, error)) string {
	if strings.TrimSpace(home) != "" {
		return strings.TrimSpace(home)
	}
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-gemini")
	}
	if userHomeDirFn != nil {
		if h, err := userHomeDirFn(); err == nil && strings.TrimSpace(h) != "" {
			return strings.TrimSpace(h)
		}
	}
	return "/root"
}

// DefaultDataDir returns the production default base directory for persistent data.
func DefaultDataDir() string {
	return defaultDataDir(testing.Testing())
}

func defaultDataDir(isTesting bool) string {
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-data")
	}
	return "/data"
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Fatal: failed to load configuration: %v", err)
	}

	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
	}
	if cur.Port == "" {
		cur.Port = "8080"
	}
	cfg.Update(cur)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-stopChan
		cancel()
	}()

	if err := RunBrainApp(ctx, cfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server failed: %v", err)
	}
}
