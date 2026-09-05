package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/scheduler"
	"github.com/azylman/aerial/brain/pkg/skills"
	"github.com/azylman/aerial/brain/pkg/watcher"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type PromptRequest struct {
	Prompt         string `json:"prompt"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id,omitempty"`
}

func handlePrompt(database *sql.DB, pool *queue.WorkerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()

		var req PromptRequest
		if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		msgID := strings.TrimSpace(req.MessageID)
		if msgID == "" {
			msgID = uuid.New().String()
		}

		threadID := strings.TrimSpace(req.ConversationID)
		if threadID == "" {
			threadID = uuid.New().String()
		}

		msg := db.Message{
			ID:         msgID,
			ThreadID:   threadID,
			GuildID:    "",
			AuthorID:   "http-client",
			AuthorName: "HTTP Client",
			Content:    req.Prompt,
			Summary:    db.CleanTaskSummary(req.Prompt),
			Status:     db.StatusPending,
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}

		if err := db.InsertMessage(database, msg); err != nil {
			log.Printf("Failed to insert HTTP prompt message %s to DB: %v", msgID, err)
		}

		if pool != nil {
			pool.Enqueue(msg)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":          "accepted",
			"conversation_id": threadID,
			"message_id":      msgID,
			"message":         "Prompt execution enqueued in background",
		})
	}
}

func handleTranscripts(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			homeDir = "/root"
		}

		type TranscriptEntry struct {
			Path       string `json:"path"`
			ModTime    string `json:"mod_time"`
			TotalSteps int    `json:"total_steps"`
			LastStatus string `json:"last_status"`
			LastError  string `json:"last_error,omitempty"`
			ExternalID string `json:"external_id,omitempty"`
			RawJSONL   string `json:"raw_jsonl,omitempty"`
		}

		var results []TranscriptEntry
		roots := []string{
			"/data/brain",
			filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
			filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		}

		includeRaw := r.URL.Query().Get("include_raw") == "true"

		for _, root := range roots {
			entries, err := os.ReadDir(root)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}

				internalID := entry.Name()
				tPath := filepath.Join(root, internalID, ".system_generated", "logs", "transcript_full.jsonl")
				if _, err := os.Stat(tPath); err != nil {
					tPath = filepath.Join(root, internalID, ".system_generated", "logs", "transcript.jsonl")
				}

				data, err := os.ReadFile(tPath)
				if err != nil {
					continue
				}

				info, err := entry.Info()
				if err != nil {
					continue
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
						Status string          `json:"status"`
						Error  json.RawMessage `json:"error"`
					}
					if err := json.Unmarshal([]byte(line), &step); err == nil {
						if step.Status != "" {
							lastStatus = step.Status
						}
						if len(step.Error) > 0 {
							errStr := string(step.Error)
							if errStr != "null" && errStr != "" {
								lastError = errStr
							}
						}
					}
				}

				extID, err := db.GetExternalConversationID(database, internalID)
				if err != nil {
					log.Printf("Warning: GetExternalConversationID error for %s: %v", internalID, err)
				}

				item := TranscriptEntry{
					Path:       tPath,
					ModTime:    info.ModTime().Format(time.RFC3339),
					TotalSteps: totalSteps,
					LastStatus: lastStatus,
					LastError:  lastError,
					ExternalID: extID,
				}
				if includeRaw {
					item.RawJSONL = string(data)
				}
				results = append(results, item)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(results)
	}
}

func handleFacts(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
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

		result, err := db.GetFactsPaginated(database, filter)
		if err != nil {
			log.Printf("[HTTP] Error fetching facts: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch facts"})
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(result)
	}
}

// Token & Secret Redaction

var tokenPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)basic\s+[a-zA-Z0-9+/=]+`),
	regexp.MustCompile(`(?i)x-access-token:[^@\s]+`),
	regexp.MustCompile(`(?i)github_pat_[a-zA-Z0-9_]+`),
	regexp.MustCompile(`(?i)ghp_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)gho_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)ghu_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)AIza[0-9A-Za-z-_]{35}`),
	regexp.MustCompile(`(?i)(GEMINI_API_KEY|DISCORD_BOT_TOKEN|DISCORD_TOKEN|GITHUB_PAT|HA_TOKEN|PASSWORD|SECRET|TOKEN|KEY)\s*[:=]\s*([^\s,;]+)`),
}

// SanitizeString scrubs sensitive tokens, PATs, passwords, and credentials from text.
func SanitizeString(input string) string {
	if input == "" {
		return ""
	}
	out := input

	// 1. Redact known sensitive environment variable values if set
	envKeys := []string{
		"GEMINI_API_KEY",
		"ANTIGRAVITY_API_KEY",
		"DISCORD_BOT_TOKEN",
		"DISCORD_TOKEN",
		"GITHUB_PAT",
		"GITHUB_PERSONAL_ACCESS_TOKEN",
		"HA_TOKEN",
	}
	for _, k := range envKeys {
		val := os.Getenv(k)
		if len(val) >= 4 {
			out = strings.ReplaceAll(out, val, "[REDACTED]")
		}
	}

	// 2. Redact any other env vars with sensitive key names
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			kUpper := strings.ToUpper(parts[0])
			val := parts[1]
			if len(val) >= 6 && (strings.Contains(kUpper, "TOKEN") || strings.Contains(kUpper, "KEY") || strings.Contains(kUpper, "SECRET") || strings.Contains(kUpper, "PASSWORD") || strings.Contains(kUpper, "PAT") || strings.Contains(kUpper, "AUTH")) {
				out = strings.ReplaceAll(out, val, "[REDACTED]")
			}
		}
	}

	// 3. Apply regex pattern sanitization
	for _, re := range tokenPatterns {
		out = re.ReplaceAllString(out, "[REDACTED]")
	}

	return out
}

func ordinal(n int) string {
	if n >= 11 && n <= 13 {
		return fmt.Sprintf("%dth", n)
	}
	switch n % 10 {
	case 1:
		return fmt.Sprintf("%dst", n)
	case 2:
		return fmt.Sprintf("%dnd", n)
	case 3:
		return fmt.Sprintf("%drd", n)
	default:
		return fmt.Sprintf("%dth", n)
	}
}

var cronMonthNames = map[int]string{
	1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
	7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

var cronDayNames = map[int]string{
	0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday",
}

var cronDayShortNames = map[int]string{
	0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun",
}

// FormatCronDescription converts a standard 5-field cron expression or descriptor into human-readable English.
func FormatCronDescription(cronExpr string) string {
	expr := strings.TrimSpace(cronExpr)
	if expr == "" {
		return ""
	}

	switch strings.ToLower(expr) {
	case "@yearly", "@annually":
		return "Every year on Jan 1st at 00:00"
	case "@monthly":
		return "1st of every month at 00:00"
	case "@weekly":
		return "Every week on Sunday at 00:00"
	case "@daily", "@midnight":
		return "Every day at 00:00"
	case "@hourly":
		return "Every hour"
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return expr
	}

	minStr, hourStr, domStr, monStr, dowStr := fields[0], fields[1], fields[2], fields[3], fields[4]

	// Case: Every minute (* * * * *)
	if minStr == "*" && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return "Every minute"
	}

	// Case: Every X minutes (*/N * * * *)
	if strings.HasPrefix(minStr, "*/") && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		interval := strings.TrimPrefix(minStr, "*/")
		return fmt.Sprintf("Every %s minutes", interval)
	}

	// Case: Every X hours (0 */N * * *)
	if minStr == "0" && strings.HasPrefix(hourStr, "*/") && domStr == "*" && monStr == "*" && dowStr == "*" {
		interval := strings.TrimPrefix(hourStr, "*/")
		return fmt.Sprintf("Every %s hours", interval)
	}

	// Try to parse hour and minute as integers
	m, minErr := strconv.Atoi(minStr)
	h, hourErr := strconv.Atoi(hourStr)

	if minErr == nil && hourErr == nil {
		timeStr := fmt.Sprintf("%02d:%02d", h, m)

		// 1. Every day at HH:MM (0 9 * * *)
		if domStr == "*" && monStr == "*" && dowStr == "*" {
			return fmt.Sprintf("Every day at %s", timeStr)
		}

		// 2. Specific day of week (0 9 * * 1-5, 0 9 * * 0, etc.)
		if domStr == "*" && monStr == "*" && dowStr != "*" {
			dowUpper := strings.ToUpper(dowStr)
			if dowUpper == "1-5" || dowUpper == "MON-FRI" {
				return fmt.Sprintf("Weekdays (Mon–Fri) at %s", timeStr)
			}
			if dowUpper == "0,6" || dowUpper == "6,0" || dowUpper == "SAT,SUN" || dowUpper == "SUN,SAT" {
				return fmt.Sprintf("Weekends (Sat–Sun) at %s", timeStr)
			}

			// Single number day of week
			if dowNum, err := strconv.Atoi(dowStr); err == nil && dowNum >= 0 && dowNum <= 7 {
				return fmt.Sprintf("Every %s at %s", cronDayNames[dowNum], timeStr)
			}

			// Comma-separated list of days (e.g. 1,3,5 or Mon,Wed,Fri)
			parts := strings.Split(dowStr, ",")
			var names []string
			for _, p := range parts {
				pTrim := strings.TrimSpace(p)
				if dNum, err := strconv.Atoi(pTrim); err == nil && dNum >= 0 && dNum <= 7 {
					names = append(names, cronDayShortNames[dNum])
				} else {
					names = append(names, pTrim)
				}
			}
			if len(names) > 0 {
				return fmt.Sprintf("%s at %s", strings.Join(names, ", "), timeStr)
			}
		}

		// 3. Specific day of month (0 12 1 * *)
		if domStr != "*" && monStr == "*" && dowStr == "*" {
			if domNum, err := strconv.Atoi(domStr); err == nil && domNum >= 1 && domNum <= 31 {
				return fmt.Sprintf("%s of every month at %s", ordinal(domNum), timeStr)
			}
		}

		// 4. Specific month and day (0 0 1 1 *)
		if domStr != "*" && monStr != "*" && dowStr == "*" {
			domNum, errDom := strconv.Atoi(domStr)
			monNum, errMon := strconv.Atoi(monStr)
			if errDom == nil && errMon == nil && monNum >= 1 && monNum <= 12 {
				return fmt.Sprintf("Every year on %s %s at %s", cronMonthNames[monNum], ordinal(domNum), timeStr)
			}
		}

		return fmt.Sprintf("At %s (cron: %s)", timeStr, expr)
	}

	return expr
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
	mu        sync.RWMutex
	expiresAt time.Time
	summary   db.ScheduleSummaryMetrics
	crons     []CronScheduleWithDesc
	oneShots  []OneShotScheduleJSON
}

func handleSchedules(database *sql.DB) http.HandlerFunc {
	cache := &schedulesCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		now := time.Now().UTC()

		cache.mu.RLock()
		if now.Before(cache.expiresAt) && cache.crons != nil {
			resp := SchedulesResponse{
				Status:     "ok",
				SystemTime: now,
				Summary:    cache.summary,
				Crons:      cache.crons,
				OneShots:   cache.oneShots,
			}
			cache.mu.RUnlock()

			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		cache.mu.RUnlock()

		cache.mu.Lock()
		defer cache.mu.Unlock()

		// Double-check under write lock
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
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
			return
		}

		summary, err := db.GetScheduleSummaryMetrics(database)
		if err != nil {
			log.Printf("[HTTP] Error fetching schedule summary: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch schedule summary"})
			return
		}

		rawCrons, err := db.GetAllCronSchedules(database, "")
		if err != nil {
			log.Printf("[HTTP] Error fetching cron schedules: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch cron schedules"})
			return
		}

		rawOneShots, err := db.GetAllOneShotSchedules(database, "")
		if err != nil {
			log.Printf("[HTTP] Error fetching one-shot schedules: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch one-shot schedules"})
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
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func handleScheduleRuns(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
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

		rawRuns, total, err := db.GetScheduleRunsPaginated(database, limit, offset, scheduleID, status)
		if err != nil {
			log.Printf("[HTTP] Error fetching schedule runs: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch schedule runs"})
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
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

type TasksResponse struct {
	Status string          `json:"status"`
	Total  int             `json:"total"`
	Tasks  []db.ActiveTask `json:"tasks"`
}

type tasksCache struct {
	mu        sync.RWMutex
	tasks     []db.ActiveTask
	expiresAt time.Time
}

func handleTasks(database *sql.DB) http.HandlerFunc {
	cache := &tasksCache{}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		now := time.Now().UTC()

		cache.mu.RLock()
		if now.Before(cache.expiresAt) && cache.tasks != nil {
			cached := cache.tasks
			cache.mu.RUnlock()
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(TasksResponse{
				Status: "ok",
				Total:  len(cached),
				Tasks:  cached,
			})
			return
		}
		cache.mu.RUnlock()

		cache.mu.Lock()
		defer cache.mu.Unlock()

		// Double-check under write lock
		if now.Before(cache.expiresAt) && cache.tasks != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(TasksResponse{
				Status: "ok",
				Total:  len(cache.tasks),
				Tasks:  cache.tasks,
			})
			return
		}

		rawTasks, err := db.GetActiveTasks(database)
		if err != nil {
			log.Printf("[HTTP] Error fetching active tasks: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch active tasks"})
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
			task.AuthorName = SanitizeString(task.AuthorName)
			tasks = append(tasks, task)
		}

		cache.tasks = tasks
		cache.expiresAt = now.Add(1 * time.Second)

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(TasksResponse{
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
	case path == "/prompt":
		return "/prompt"
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
	case path == "/internal/reload":
		return "/internal/reload"
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

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("Warning: initial LoadConfig error: %v", err)
	}

	portStr := config.GetEnv("PORT", "8080")
	agyBin := config.GetEnv("AGY_BIN", "agy")
	apiKey := config.GetEnv("GEMINI_API_KEY", config.GetEnv("ANTIGRAVITY_API_KEY", ""))
	systemPrompt := config.GetEnv("SYSTEM_PROMPT", "")

	if apiKey != "" {
		if err := config.EnsureAgySettings(apiKey, cfg.Model); err != nil {
			log.Printf("Warning: EnsureAgySettings error: %v", err)
		}
	}
	if err := config.EnsureSystemRules(systemPrompt); err != nil {
		log.Printf("Warning: EnsureSystemRules error: %v", err)
	}
	mcpConfig := config.LoadMCPConfig()
	if len(mcpConfig) > 0 {
		if err := config.EnsureMcpConfig(mcpConfig); err != nil {
			log.Printf("Warning: EnsureMcpConfig error: %v", err)
		}
	}
	if err := skills.EnsureSkills(); err != nil {
		log.Printf("Warning: EnsureSkills error: %v", err)
	}

	if _, err := os.Stat("/data"); err == nil {
		if err := os.MkdirAll("/data/brain", 0755); err != nil {
			log.Printf("Warning: MkdirAll /data/brain error: %v", err)
		}
		homeDir, err := os.UserHomeDir()
		if err != nil || homeDir == "" {
			homeDir = "/root"
		}
		cliBrainDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
		if err := os.MkdirAll(filepath.Dir(cliBrainDir), 0755); err != nil {
			log.Printf("Warning: MkdirAll cliBrainDir parent error: %v", err)
		}
		if _, err := os.Lstat(cliBrainDir); err != nil {
			if err := os.Symlink("/data/brain", cliBrainDir); err != nil {
				log.Printf("Warning: Symlink /data/brain error: %v", err)
			}
		}
	}

	dbPath := db.GetDBPath()
	database, err := db.InitDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	clsModel := config.GetEnv("AMBIENT_CLASSIFIER_MODEL", "Gemini 3.8 Flash (Low)")
	cls := classifier.NewClassifier(
		classifier.WithModel(clsModel),
		classifier.WithLLMFunc(classifier.NewAgyLLMFunc(agyBin, apiKey, runner.RunAgy)),
	)

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB:             database,
		AgyBin:         agyBin,
		APIKey:         apiKey,
		Model:          cfg.Model,
		SystemPrompt:   systemPrompt,
		TimeoutMinutes: cfg.TimeoutMinutes,
		Classifier:     cls,
	})
	pool.Start()
	defer pool.Stop()

	// Connect Discord Gateway session before startup crash recovery
	dgSession := connectDiscordFunnel(database, pool)
	if dgSession != nil {
		defer func() {
			_ = dgSession.Close()
		}()
	}

	reloadConfig := func(source string) {
		log.Printf("[%s] Changes detected. Reloading configuration, system rules, and skills...", source)
		latestCfg, parseErr := config.LoadConfig()
		if parseErr != nil {
			metrics.ConfigReloadsTotal.WithLabelValues(source, "failure").Inc()
			log.Printf("[%s] Warning: Failed to parse config.yaml: %v", source, parseErr)
			if dgSession != nil {
				alertMsg := fmt.Sprintf("Failed to parse config.yaml:\n```\n%v\n```\nAerial has retained the Last Known Good Configuration (LKGC).", parseErr)
				if alertErr := delivery.SendSystemAlert(dgSession, config.GetSystemChannel(), "Invalid Configuration File", alertMsg); alertErr != nil {
					log.Printf("[%s] Warning: failed to send Discord alert: %v", source, alertErr)
				}
			}
		} else {
			metrics.ConfigReloadsTotal.WithLabelValues(source, "success").Inc()
		}

		latestModel := latestCfg.Model
		latestTimeout := latestCfg.TimeoutMinutes
		latestMcpConfig := config.LoadMCPConfig()

		if apiKey != "" {
			if err := config.EnsureAgySettings(apiKey, latestModel); err != nil {
				log.Printf("[%s] Warning: EnsureAgySettings error: %v", source, err)
			}
		}
		if len(latestMcpConfig) > 0 {
			if err := config.EnsureMcpConfig(latestMcpConfig); err != nil {
				log.Printf("[%s] Warning: EnsureMcpConfig error: %v", source, err)
			}
		}
		pool.UpdateRuntimeConfig(latestModel, latestTimeout)
		if err := config.EnsureSystemRules(systemPrompt); err != nil {
			log.Printf("[%s] Warning: EnsureSystemRules error: %v", source, err)
		}
		if err := skills.EnsureSkills(); err != nil {
			log.Printf("[%s] Warning: EnsureSkills error: %v", source, err)
		}
	}

	// Start background file watcher for atomic hot-reloading of prompts and skills
	fileWatcher, err := watcher.NewWatcher(
		watcher.WithCallback(func() {
			reloadConfig("Hot-Reload")
		}),
	)
	if err != nil {
		log.Printf("Warning: failed to create file watcher: %v", err)
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil || homeDir == "" {
			homeDir = "/root"
		}

		watchDirs := []string{
			"/share/aerial-config",
			"/share/aerial",
			"/app/.agents/skills",
			filepath.Join(homeDir, ".gemini", "skills"),
			filepath.Join(homeDir, ".gemini", "config", "skills"),
		}

		for _, dir := range watchDirs {
			if _, err := os.Stat(dir); err == nil {
				if addErr := fileWatcher.AddRecursive(dir); addErr != nil {
					log.Printf("Warning: failed to watch %s: %v", dir, addErr)
				}
			}
		}

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		defer watcherCancel()
		go fileWatcher.Start(watcherCtx)
		defer func() { _ = fileWatcher.Close() }()
	}

	// Resume interrupted turns after Discord is attached
	queue.RecoverInterrupted(database, pool)

	// Start background scheduler monitor for due cron and one-shot routines
	stopScheduler := scheduler.Start(context.Background(), database, pool, dgSession)
	defer stopScheduler()

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/prompt", handlePrompt(database, pool))
	mux.HandleFunc("/transcripts", handleTranscripts(database))
	mux.HandleFunc("/tasks", handleTasks(database))
	mux.HandleFunc("/facts", handleFacts(database))
	mux.HandleFunc("/schedules", handleSchedules(database))
	mux.HandleFunc("/schedules/runs", handleScheduleRuns(database))
	mux.HandleFunc("/internal/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		reloadConfig("Internal Sidecar Trigger")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"reloaded"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:    ":" + portStr,
		Handler: metricsMiddleware(mux),
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Aerial Brain listening on port %s (model=%s, timeout=%dm)", portStr, cfg.Model, cfg.TimeoutMinutes)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	<-stopChan
	log.Println("Shutting down Aerial Brain gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	if stopScheduler != nil {
		stopScheduler()
	}
	pool.Stop()
	if dgSession != nil {
		_ = dgSession.Close()
	}
	if err := database.Close(); err != nil {
		log.Printf("Error closing database: %v", err)
	}

	log.Println("Aerial Brain shutdown complete")
}
