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
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/delivery"
	"github.com/azylman/aerial/brain/pkg/gitsync"
	"github.com/azylman/aerial/brain/pkg/queue"
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

func main() {
	configRepoUrl := config.GetEnv("AERIAL_CONFIG_REPO_URL", "https://github.com/azylman/aerial-config.git")
	pat := config.GetEnv("GITHUB_PAT", "")

	bootCtx, bootCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := gitsync.EnsureRepo(bootCtx, "/share/aerial-config", configRepoUrl, pat); err != nil {
		log.Printf("[Startup Bootstrapping] Warning: Failed to ensure /share/aerial-config: %v. Proceeding with local configuration.", err)
	}
	bootCancel()

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

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB:             database,
		AgyBin:         agyBin,
		APIKey:         apiKey,
		Model:          cfg.Model,
		SystemPrompt:   systemPrompt,
		TimeoutMinutes: cfg.TimeoutMinutes,
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
			log.Printf("[%s] Warning: Failed to parse config.yaml: %v", source, parseErr)
			if dgSession != nil {
				alertMsg := fmt.Sprintf("Failed to parse config.yaml:\n```\n%v\n```\nAerial has retained the Last Known Good Configuration (LKGC).", parseErr)
				if alertErr := delivery.SendSystemAlert(dgSession, config.GetSystemChannel(), "Invalid Configuration File", alertMsg); alertErr != nil {
					log.Printf("[%s] Warning: failed to send Discord alert: %v", source, alertErr)
				}
			}
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

	// Start background git synchronization worker for config & project repos
	gitSyncRepos := cfg.GitSync.Repositories
	if len(gitSyncRepos) == 0 {
		gitSyncRepos = []string{
			"/share/aerial-config",
			"/share/aerial",
		}
	}
	syncInterval := 60 * time.Second
	if d, err := time.ParseDuration(cfg.GitSync.Interval); err == nil && d > 0 {
		syncInterval = d
	}

	stopGitSync := gitsync.StartPeriodicSync(
		context.Background(),
		syncInterval,
		gitSyncRepos,
		func(repo string) {
			reloadConfig(fmt.Sprintf("GitSync: %s", filepath.Base(repo)))
		},
	)
	defer stopGitSync()

	mux := http.NewServeMux()
	mux.HandleFunc("/prompt", handlePrompt(database, pool))
	mux.HandleFunc("/transcripts", handleTranscripts(database))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:    ":" + portStr,
		Handler: mux,
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

	if stopGitSync != nil {
		stopGitSync()
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
