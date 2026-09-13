package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/memory"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/scheduler"
	"github.com/azylman/aerial/brain/pkg/env"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/azylman/aerial/brain/pkg/watcher"
	"github.com/bwmarrin/discordgo"
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

func handleTranscripts(database *sql.DB, searchPaths ...string) http.HandlerFunc {
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
					tStat, _ = os.Stat(tPath)
				}

				data, err := os.ReadFile(tPath)
				if err != nil {
					continue
				}

				modTime := ""
				if tStat != nil {
					modTime = tStat.ModTime().Format(time.RFC3339)
				} else if info, err := entry.Info(); err == nil {
					modTime = info.ModTime().Format(time.RFC3339)
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

// SanitizeString scrubs sensitive tokens, PATs, passwords, and credentials from text.
func SanitizeString(input string) string {
	return sanitizer.SanitizeString(input)
}

func ordinal(n int) string {
	return Ordinal(n)
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
	mu        sync.Mutex
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

		cache.mu.Lock()
		defer cache.mu.Unlock()

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

func SetupBrainMux(database *sql.DB, pool *queue.WorkerPool, reloadFn func(string), searchPaths ...string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/prompt", handlePrompt(database, pool))
	mux.HandleFunc("/transcripts", handleTranscripts(database, searchPaths...))
	mux.HandleFunc("/tasks", handleTasks(database))
	mux.HandleFunc("/facts", handleFacts(database))
	mux.HandleFunc("/schedules", handleSchedules(database))
	mux.HandleFunc("/schedules/runs", handleScheduleRuns(database))
	mux.HandleFunc("/internal/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if reloadFn != nil {
			reloadFn("Internal Sidecar Trigger")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"reloaded"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
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
	}

	dataDir := cfg.DataDir()
	if _, err := os.Stat(dataDir); err == nil {
		brainDir := filepath.Join(dataDir, "brain")
		if err := os.MkdirAll(brainDir, 0755); err != nil {
			log.Printf("Warning: MkdirAll %s error: %v", brainDir, err)
		}
		homeDir := cfg.GeminiHomeDir()
		cliBrainDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
		if err := os.MkdirAll(filepath.Dir(cliBrainDir), 0755); err != nil {
			log.Printf("Warning: MkdirAll cliBrainDir parent error: %v", err)
		}
		if _, err := os.Lstat(cliBrainDir); err != nil {
			if err := os.Symlink(brainDir, cliBrainDir); err != nil {
				log.Printf("Warning: Symlink %s error: %v", brainDir, err)
			}
		}
	}
	return nil
}

type ReloadOption func(*reloadConfigOptions)

type reloadConfigOptions struct {
	dgSession           *discordgo.Session
	reloadSupplier      func(active *config.Config) error
	skipEnvironmentSync bool
	provisioner         *env.Provisioner
	utilityDaemon       *runner.UtilityDaemon
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

func WithUtilityDaemon(d *runner.UtilityDaemon) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.utilityDaemon = d
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
					_ = delivery.SendSystemAlert(options.dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", alertMsg)
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

			if options.utilityDaemon != nil {
				options.utilityDaemon.TriggerRestart(source)
			}
		}
	}
}

var onServerReady func(addr string)

func RunBrainApp(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("brain: config cannot be nil")
	}
	if ctx.Err() != nil {
		return nil
	}

	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
	}
	cfg.Update(cur)

	homeDir := cfg.GeminiHomeDir()
	provisioner := env.NewFromConfig(cfg)
	_ = InitializeBrainEnvironment(ctx, cfg)

	if cur.DatabaseURL == "" {
		return fmt.Errorf("database URL or path is required")
	}

	database, err := db.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	sanitizer.RegisterConfigTokens(cfg)

	sessionMgr := session.New(cfg.GeminiHomeDir(), cfg.DataDir())
	utilityDaemon := runner.NewUtilityDaemon(cfg, runner.WithSessionRoots(sessionMgr.Roots()...))
	defer utilityDaemon.Close()

	utilityRunner := utilityDaemon.RunnerFunc()
	cls := classifier.New(cfg, utilityRunner)

	pool := queue.New(cfg, queue.WorkerPoolConfig{
		DB:                    database,
		Classifier:            cls,
		RunnerFunc:            runner.RunAgy,
		RunnerWithOptionsFunc: runner.RunAgyWithOptions,
		MemoryRetrieverFunc:   memory.RetrieveRelevantFacts,
		SessionManager:        sessionMgr,
	})
	pool.Start()

	SetFunnelConfig(cfg)
	dgSession := connectDiscordFunnel(ctx, database, pool, cur.DiscordToken)
	if pool != nil && dgSession != nil {
		pool.SetDiscordSession(dgSession)
	}

	// Resume interrupted turns after Discord gateway session is registered
	// so poison pill and recovery notifications can reliably deliver to Discord.
	queue.RecoverInterrupted(database, pool)
	defer func() {
		log.Printf("Draining worker pool (10s timeout)...")
		pool.StopWithTimeout(10 * time.Second)
		if dgSession != nil {
			log.Printf("Closing Discord gateway session...")
			_ = dgSession.Close()
		}
	}()

	reloadConfig := CreateReloadConfigFunc(cfg, WithDiscordSession(dgSession), WithProvisioner(provisioner), WithUtilityDaemon(utilityDaemon))

	// Start background file watcher for atomic hot-reloading of prompts and skills
	fileWatcher, err := watcher.NewWatcher(
		watcher.WithCallback(func() {
			reloadConfig("Hot-Reload")
		}),
	)
	if err != nil {
		log.Printf("Warning: failed to create file watcher: %v", err)
	} else {
		watchDirs := []string{
			"/share/aerial-config",
			"/share/aerial",
			"/app/.agents/skills",
			filepath.Join(homeDir, ".gemini", "skills"),
			filepath.Join(homeDir, ".gemini", "config", "skills"),
		}

		watcherCtx, watcherCancel := context.WithCancel(ctx)
		defer watcherCancel()
		var watcherWg sync.WaitGroup
		watcherWg.Add(1)
		go func() {
			defer watcherWg.Done()
			for _, dir := range watchDirs {
				if _, err := os.Stat(dir); err == nil {
					if addErr := fileWatcher.AddRecursive(dir); addErr != nil {
						log.Printf("Warning: failed to watch %s: %v", dir, addErr)
					}
				}
			}
			fileWatcher.Start(watcherCtx)
		}()
		defer func() {
			watcherCancel()
			_ = fileWatcher.Close()
			watcherWg.Wait()
		}()
	}

	// Start background scheduler monitor for due cron and one-shot routines
	sched, err := scheduler.New(cfg, database, pool, scheduler.NewDiscordThreadCreator(dgSession), scheduler.WithRunnerFunc(utilityRunner), scheduler.WithSessionRoots(sessionMgr.Roots()...))
	if err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}
	stopScheduler := sched.Start(ctx)
	defer stopScheduler()

	mux := SetupBrainMux(database, pool, reloadConfig, sessionMgr.Roots()...)

	port := cur.Port
	if port == "" {
		port = "8080"
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	defer ln.Close()

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
	if h := os.Getenv("HOME"); strings.TrimSpace(h) != "" {
		return strings.TrimSpace(h)
	}
	if h, err := os.UserHomeDir(); err == nil && strings.TrimSpace(h) != "" {
		return strings.TrimSpace(h)
	}
	return "/root"
}

// DefaultDataDir returns the production default base directory for persistent data.
func DefaultDataDir() string {
	return "/data"
}

func main() {
	cfg, err := config.New()
	if err != nil {
		log.Printf("Warning: initial LoadConfig error: %v", err)
	}

	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
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

	if err := RunBrainApp(ctx, cfg); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}
