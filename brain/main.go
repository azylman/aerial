package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type PromptRequest struct {
	Prompt         string `json:"prompt"`
	ConversationID string `json:"conversation_id"`
}

type Options struct {
	Port           int             `json:"port"`
	AgyBin         string          `json:"agy_bin"`
	ApiKey         string          `json:"api_key"`
	Model          string          `json:"model"`
	SystemPrompt   string          `json:"system_prompt"`
	TimeoutMinutes int             `json:"timeout_minutes"`
	McpConfig      json.RawMessage `json:"mcp_config"`
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getDBPath() string {
	if _, err := os.Stat("/data"); err == nil {
		return "/data/aerial.db"
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./aerial.db"
	}
	return filepath.Join(homeDir, ".gemini", "aerial.db")
}

func initDB(dbPath string) (*sql.DB, error) {
	_ = os.MkdirAll(filepath.Dir(dbPath), 0755)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	schema := `
	CREATE TABLE IF NOT EXISTS conversations (
		external_id TEXT PRIMARY KEY,
		internal_id TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_conversations_internal_id ON conversations(internal_id);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	log.Printf("SQLite conversation database initialized at %s", dbPath)
	return db, nil
}

func getInternalConversationID(db *sql.DB, externalID string) (string, error) {
	if db == nil || externalID == "" {
		return "", nil
	}
	var internalID string
	err := db.QueryRow("SELECT internal_id FROM conversations WHERE external_id = ?", externalID).Scan(&internalID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return internalID, err
}

func getExternalConversationID(db *sql.DB, internalID string) (string, error) {
	if db == nil || internalID == "" {
		return "", nil
	}
	var externalID string
	err := db.QueryRow("SELECT external_id FROM conversations WHERE internal_id = ?", internalID).Scan(&externalID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return externalID, err
}

func saveConversationMapping(db *sql.DB, externalID, internalID string) error {
	if db == nil || externalID == "" || internalID == "" {
		return nil
	}
	now := time.Now().UTC()
	query := `
	INSERT INTO conversations (external_id, internal_id, created_at, updated_at)
	VALUES (?, ?, ?, ?)
	ON CONFLICT(external_id) DO UPDATE SET
		internal_id = excluded.internal_id,
		updated_at = excluded.updated_at
	`
	_, err := db.Exec(query, externalID, internalID, now, now)
	if err != nil {
		log.Printf("Failed to save conversation mapping (%s -> %s): %v", externalID, internalID, err)
	} else {
		log.Printf("Saved conversation mapping: %s -> %s", externalID, internalID)
	}
	return err
}

func findLatestSessionDir(after time.Time) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	roots := []string{
		"/data/brain",
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
	}
	var newestID string
	var newestTime time.Time
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if info.ModTime().After(after) && info.ModTime().After(newestTime) {
				newestTime = info.ModTime()
				newestID = entry.Name()
			}
		}
	}
	return newestID
}

func ensureAgySettings(apiKey, model string) {
	if apiKey == "" {
		return
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	configDir := filepath.Join(homeDir, ".gemini", "antigravity-cli")
	_ = os.MkdirAll(configDir, 0755)

	settingsPath := filepath.Join(configDir, "settings.json")
	settings := map[string]interface{}{
		"modelProvider": "gemini",
		"model":         model,
	}
	if data, err := os.ReadFile(settingsPath); err == nil {
		_ = json.Unmarshal(data, &settings)
		settings["modelProvider"] = "gemini"
		if model != "" {
			settings["model"] = model
		}
	}
	if out, err := json.MarshalIndent(settings, "", "  "); err == nil {
		_ = os.WriteFile(settingsPath, out, 0644)
	}
}

func ensureSystemRules(customPrompt string) {
	if strings.TrimSpace(customPrompt) == "" {
		return
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	rulesDir := filepath.Join(homeDir, ".gemini", "rules")
	_ = os.MkdirAll(rulesDir, 0755)

	overrideContent := "# User Custom Instructions\n" + strings.TrimSpace(customPrompt) + "\n"
	ruleFile := filepath.Join(rulesDir, "user_override.md")
	_ = os.WriteFile(ruleFile, []byte(overrideContent), 0644)
	log.Printf("Configured custom user rules in %s", ruleFile)
}

func loadMCPConfig() json.RawMessage {
	// 1. Check mounted / external config files
	configPaths := []string{
		"/config/mcp.config.json",
		"/config/mcp.json",
		"/data/mcp.config.json",
		"./mcp.config.json",
	}

	var rawBytes []byte
	for _, p := range configPaths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			log.Printf("Loaded MCP configuration from %s", p)
			rawBytes = data
			break
		}
	}

	// 2. Check MCP_CONFIG environment variable if no file found
	if len(rawBytes) == 0 {
		if envVal := os.Getenv("MCP_CONFIG"); envVal != "" {
			rawBytes = []byte(envVal)
		}
	}

	// 3. Fallback to /data/options.json for Home Assistant add-on compatibility
	if len(rawBytes) == 0 {
		if data, err := os.ReadFile("/data/options.json"); err == nil {
			var opts Options
			if err := json.Unmarshal(data, &opts); err == nil && len(opts.McpConfig) > 0 {
				var strVal string
				if err := json.Unmarshal(opts.McpConfig, &strVal); err == nil && strVal != "" {
					rawBytes = []byte(strVal)
				} else {
					rawBytes = opts.McpConfig
				}
			}
		}
	}

	// 4. If still empty, construct default built-in configuration
	if len(rawBytes) == 0 {
		defaultConfig := map[string]interface{}{
			"mcpServers": map[string]interface{}{
				"discord": map[string]interface{}{
					"serverUrl": "http://discord-mcp:4001/mcp",
				},
				"docker": map[string]interface{}{
					"serverUrl": "http://docker-mcp:4002/sse",
				},
			},
		}
		mcpServers := defaultConfig["mcpServers"].(map[string]interface{})
		if haToken := os.Getenv("HA_TOKEN"); haToken != "" {
			mcpServers["ha-mcp"] = map[string]interface{}{
				"serverUrl": haToken,
			}
		}
		b, _ := json.Marshal(defaultConfig)
		rawBytes = b
	}

	// 5. Expand all environment variables (${VAR_NAME}) in the JSON
	expanded := os.ExpandEnv(string(rawBytes))
	return json.RawMessage(expanded)
}

func ensureMcpConfig(rawConfig json.RawMessage) {
	if len(rawConfig) == 0 {
		return
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == "null" {
		return
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	configDir := filepath.Join(homeDir, ".gemini", "config")
	_ = os.MkdirAll(configDir, 0755)
	targetPath := filepath.Join(configDir, "mcp_config.json")

	var configContent []byte
	var strVal string
	if err := json.Unmarshal(rawConfig, &strVal); err == nil && strVal != "" {
		configContent = []byte(strVal)
	} else {
		configContent = rawConfig
	}

	var js map[string]interface{}
	var serverList []string
	if err := json.Unmarshal(configContent, &js); err == nil {
		if servers, ok := js["mcpServers"].(map[string]interface{}); ok {
			for name := range servers {
				serverList = append(serverList, name)
			}
		}
		if formatted, err := json.MarshalIndent(js, "", "  "); err == nil {
			configContent = formatted
		}
	}

	if err := os.WriteFile(targetPath, configContent, 0644); err != nil {
		log.Printf("Failed to write %s: %v", targetPath, err)
	} else {
		log.Printf("Configured %d MCP server(s) in %s: %v", len(serverList), targetPath, serverList)
	}
}

func dumpSessionDiagnosticLogs(convID string) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	searchDirs := []string{
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain", convID, ".system_generated", "logs"),
		filepath.Join("/data", "brain", convID, ".system_generated", "logs"),
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "logs"),
		filepath.Join(homeDir, ".gemini", "antigravity", "logs"),
		filepath.Join("/data", "brain", convID),
	}

	var sb strings.Builder
	for _, dir := range searchDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			fPath := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(fPath)
			if err != nil || len(data) == 0 {
				continue
			}
			sb.WriteString(fmt.Sprintf("\n--- FILE: %s (%d bytes) ---\n", fPath, len(data)))
			lines := strings.Split(string(data), "\n")
			start := 0
			if len(lines) > 40 {
				start = len(lines) - 40
				sb.WriteString("[...showing last 40 lines...]\n")
			}
			for i := start; i < len(lines); i++ {
				if strings.TrimSpace(lines[i]) != "" {
					sb.WriteString(lines[i] + "\n")
				}
			}
		}
	}
	return sb.String()
}

func extractResponseAndError(convID string) (string, string) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	var targetDirs []string
	if convID != "" {
		targetDirs = append(targetDirs,
			filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", convID),
			filepath.Join(homeDir, ".gemini", "antigravity", "brain", convID),
			filepath.Join("/data", "brain", convID),
		)
	}

	brainRoots := []string{
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		filepath.Join("/data", "brain"),
	}

	for _, root := range brainRoots {
		if entries, err := os.ReadDir(root); err == nil {
			var latestDir string
			var latestTime time.Time
			for _, entry := range entries {
				if entry.IsDir() {
					if info, err := entry.Info(); err == nil && info.ModTime().After(latestTime) {
						latestTime = info.ModTime()
						latestDir = filepath.Join(root, entry.Name())
					}
				}
			}
			if latestDir != "" {
				targetDirs = append(targetDirs, latestDir)
			}
		}
	}

	var lastResponse string
	var lastError string

	for _, dir := range targetDirs {
		for _, name := range []string{"transcript_full.jsonl", "transcript.jsonl"} {
			tPath := filepath.Join(dir, ".system_generated", "logs", name)
			data, err := os.ReadFile(tPath)
			if err != nil {
				continue
			}

			lines := strings.Split(string(data), "\n")
			for i := len(lines) - 1; i >= 0; i-- {
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}
				var step struct {
					Type      string          `json:"type"`
					Status    string          `json:"status"`
					Error     json.RawMessage `json:"error"`
					Content   string          `json:"content"`
					Thinking  string          `json:"thinking"`
					ToolCalls json.RawMessage `json:"tool_calls"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if (step.Status == "ERROR" || len(step.Error) > 0) && lastError == "" {
						if errStr := strings.TrimSpace(string(step.Error)); errStr != "" && errStr != "null" {
							lastError = errStr
						}
					}
					if step.Type == "PLANNER_RESPONSE" && lastResponse == "" {
						if strings.TrimSpace(step.Content) != "" {
							lastResponse = step.Content
						} else if len(step.ToolCalls) > 2 && string(step.ToolCalls) != "[]" && string(step.ToolCalls) != "null" {
							lastResponse = fmt.Sprintf("[Tool Call Requested]: %s", string(step.ToolCalls))
						}
					}
				}
			}
			if lastResponse != "" || lastError != "" {
				return lastResponse, lastError
			}
		}
	}
	return lastResponse, lastError
}

func loadConfig() (string, string, string, string, string, int, json.RawMessage) {
	port := getEnv("PORT", "8080")
	agyBin := getEnv("AGY_BIN", "agy")
	apiKey := getEnv("GEMINI_API_KEY", getEnv("ANTIGRAVITY_API_KEY", ""))
	model := getEnv("AGY_MODEL", "Gemini 3.6 Flash (Low)")
	systemPrompt := getEnv("SYSTEM_PROMPT", "")
	timeoutMinutes := 15
	if tm := os.Getenv("TIMEOUT_MINUTES"); tm != "" {
		if val, err := strconv.Atoi(tm); err == nil && val > 0 {
			timeoutMinutes = val
		}
	}
	var mcpConfig json.RawMessage

	// Read Home Assistant add-on options if available
	if data, err := os.ReadFile("/data/options.json"); err == nil {
		var opts Options
		if err := json.Unmarshal(data, &opts); err == nil {
			if opts.Port != 0 {
				port = fmt.Sprintf("%d", opts.Port)
			}
			if strings.TrimSpace(opts.AgyBin) != "" {
				agyBin = opts.AgyBin
			}
			if strings.TrimSpace(opts.ApiKey) != "" {
				apiKey = opts.ApiKey
			}
			if strings.TrimSpace(opts.Model) != "" {
				model = opts.Model
			}
			if strings.TrimSpace(opts.SystemPrompt) != "" {
				systemPrompt = opts.SystemPrompt
			}
			if opts.TimeoutMinutes > 0 {
				timeoutMinutes = opts.TimeoutMinutes
			}
		}
	}

	mcpConfig = loadMCPConfig()

	if apiKey != "" {
		ensureAgySettings(apiKey, model)
	}
	ensureSystemRules(systemPrompt)
	if len(mcpConfig) > 0 {
		ensureMcpConfig(mcpConfig)
	}

	// Symlink brain directory to /data/brain if /data exists
	if _, err := os.Stat("/data"); err == nil {
		_ = os.MkdirAll("/data/brain", 0755)
		homeDir, _ := os.UserHomeDir()
		if homeDir == "" {
			homeDir = "/root"
		}
		cliBrainDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
		_ = os.MkdirAll(filepath.Dir(cliBrainDir), 0755)
		if _, err := os.Lstat(cliBrainDir); err != nil {
			_ = os.Symlink("/data/brain", cliBrainDir)
		}
	}

	return port, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig
}

func handlePrompt(db *sql.DB, agyBin, apiKey, model, systemPrompt string, timeoutMinutes int, mcpConfig json.RawMessage) http.HandlerFunc {
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
		defer r.Body.Close()

		var req PromptRequest
		if err := json.Unmarshal(body, &req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid payload: 'prompt' field is required and cannot be empty",
			})
			return
		}

		externalConvID := strings.TrimSpace(req.ConversationID)
		if externalConvID == "" {
			externalConvID = uuid.New().String()
		}

		internalConvID, _ := getInternalConversationID(db, externalConvID)

		// Spawn headless Antigravity CLI in a background goroutine with bounded execution timeout
		go func(prompt, extID, intID string) {
			startTime := time.Now().Add(-2 * time.Second)
			log.Printf("Starting background execution for prompt: %q (external_conversation: %s, mapped_internal: %q, timeout: %d minutes)",
				prompt, extID, intID, timeoutMinutes)

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
			defer cancel()

			args := []string{"--dangerously-skip-permissions"}
			if model != "" {
				args = append(args, "--model", model)
			}
			if timeoutMinutes > 0 {
				args = append(args, "--print-timeout", fmt.Sprintf("%dm", timeoutMinutes))
			}
			if intID != "" {
				args = append(args, "--conversation", intID)
			}
			args = append(args, "-p", prompt)

			cmd := exec.CommandContext(ctx, agyBin, args...)
			cmd.Dir = "/app"
			cmd.Stdin = strings.NewReader("")
			if apiKey != "" {
				cmd.Env = append(os.Environ(),
					"GEMINI_API_KEY="+apiKey,
					"ANTIGRAVITY_API_KEY="+apiKey,
					"GOOGLE_GENAI_API_KEY="+apiKey,
					"AGY_LOG_LEVEL=debug",
					"ANTIGRAVITY_LOG_LEVEL=debug",
				)
			}

			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()
			exitCode := 0
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok {
					exitCode = exitErr.ExitCode()
				} else {
					exitCode = -1
					log.Printf("Process error: %v", err)
				}
			}

			stderrStr := stderr.String()
			activeInternalID := intID
			if activeInternalID == "" {
				re := regexp.MustCompile(`Starting conversation update stream for ([a-f0-9\-]+)`)
				if match := re.FindStringSubmatch(stderrStr); len(match) > 1 {
					activeInternalID = match[1]
				} else {
					activeInternalID = findLatestSessionDir(startTime)
				}
				if activeInternalID != "" {
					_ = saveConversationMapping(db, extID, activeInternalID)
				}
			}

			lookupID := activeInternalID
			if lookupID == "" {
				lookupID = extID
			}
			outText, errDetail := extractResponseAndError(lookupID)
			if outText == "" {
				outText = strings.TrimSpace(stdout.String())
			}

			log.Printf("Execution finished | external_conv=%s internal_conv=%s exit_code=%d", extID, activeInternalID, exitCode)
			if exitCode != 0 || strings.Contains(outText, "error") || strings.Contains(outText, "terminated") || errDetail != "" {
				diagLogs := dumpSessionDiagnosticLogs(lookupID)
				log.Printf("=== AGENT ERROR DIAGNOSTIC REPORT ===\nCommand: %s %v\nExit Code: %d\nStdout: %s\nStderr: %s\nParsed Error: %s\nTranscript & System Logs:\n%s\n=====================================",
					agyBin, args, exitCode, stdout.String(), stderrStr, errDetail, diagLogs)
			} else {
				log.Printf("--- STDOUT / RESPONSE ---\n%s\n--- STDERR ---\n%s", outText, stderrStr)
			}
		}(req.Prompt, externalConvID, internalConvID)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":          "accepted",
			"conversation_id": externalConvID,
			"message":         "Prompt execution started in background",
		})
	}
}

func handleTranscripts(db *sql.DB) http.HandlerFunc {
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
			filepath.Join("/data", "brain"),
			filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
			filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		}

		seen := make(map[string]bool)
		for _, root := range roots {
			entries, err := os.ReadDir(root)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				convDir := entry.Name()
				if seen[convDir] {
					continue
				}
				seen[convDir] = true

				tPath := filepath.Join(root, convDir, ".system_generated", "logs", "transcript_full.jsonl")
				data, err := os.ReadFile(tPath)
				if err != nil {
					tPath = filepath.Join(root, convDir, ".system_generated", "logs", "transcript.jsonl")
					data, err = os.ReadFile(tPath)
					if err != nil {
						continue
					}
				}

				info, _ := os.Stat(tPath)
				modTime := ""
				if info != nil {
					modTime = info.ModTime().Format(time.RFC3339)
				}

				extID, _ := getExternalConversationID(db, convDir)

				te := TranscriptEntry{
					Path:       tPath,
					ModTime:    modTime,
					ExternalID: extID,
					RawJSONL:   string(data),
				}

				lines := strings.Split(string(data), "\n")
				for _, line := range lines {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					var m struct {
						Status string          `json:"status"`
						Error  json.RawMessage `json:"error"`
					}
					if err := json.Unmarshal([]byte(line), &m); err == nil {
						te.TotalSteps++
						if m.Status != "" {
							te.LastStatus = m.Status
						}
						if len(m.Error) > 0 && string(m.Error) != "null" {
							te.LastError = string(m.Error)
						}
					}
				}
				results = append(results, te)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(results)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func handleIndexUI(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		return
	}
	html := `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Aerial Brain - Transcript Viewer</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
  <script>tailwind.config = { darkMode: 'class' }</script>
</head>
<body class="bg-slate-950 text-slate-100 font-sans antialiased min-h-screen flex flex-col">
  <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur px-6 py-4 flex items-center justify-between sticky top-0 z-50">
    <div class="flex items-center space-x-3">
      <div class="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center font-bold text-white shadow-lg shadow-indigo-500/30">A</div>
      <div>
        <h1 class="text-lg font-bold tracking-tight text-white flex items-center gap-2">
          Aerial Brain
          <span class="text-xs px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">Live Dashboard</span>
        </h1>
      </div>
    </div>
    <div class="flex items-center space-x-3">
      <button id="refreshBtn" onclick="fetchTranscripts()" class="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-medium border border-slate-700 transition">
        Refresh
      </button>
      <a href="http://192.168.1.14:8089" target="_blank" class="px-3 py-1.5 rounded-lg bg-indigo-600/20 hover:bg-indigo-600/30 text-indigo-400 text-sm font-medium border border-indigo-500/30 transition flex items-center gap-1.5">
        Agentsview ?
      </a>
    </div>
  </header>
  <div class="flex-1 flex overflow-hidden">
    <aside class="w-80 border-r border-slate-800 bg-slate-900/40 flex flex-col">
      <div class="p-3 border-b border-slate-800">
        <input id="searchInput" type="text" placeholder="Filter sessions..." oninput="renderSidebar()" class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-indigo-500">
      </div>
      <div id="sessionList" class="flex-1 overflow-y-auto p-2 space-y-1"></div>
    </aside>
    <main class="flex-1 flex flex-col bg-slate-950 overflow-hidden">
      <div id="chatHeader" class="border-b border-slate-800 px-6 py-3 bg-slate-900/20 flex items-center justify-between text-sm text-slate-400">
        <span>Select a conversation from the sidebar</span>
      </div>
      <div id="chatContent" class="flex-1 overflow-y-auto p-6 space-y-6 max-w-4xl w-full mx-auto"></div>
    </main>
  </div>
  <script>
    let sessions = [];
    let selectedIdx = 0;
    async function fetchTranscripts() {
      try {
        const res = await fetch('/api/transcripts');
        sessions = await res.json();
        sessions.reverse();
        renderSidebar();
        if (sessions.length > 0) selectSession(selectedIdx < sessions.length ? selectedIdx : 0);
      } catch (err) { console.error(err); }
    }
    function renderSidebar() {
      const q = (document.getElementById('searchInput').value || '').toLowerCase();
      const el = document.getElementById('sessionList');
      el.innerHTML = '';
      sessions.forEach((s, idx) => {
        const ext = s.external_id || 'Direct API';
        if (!ext.toLowerCase().includes(q) && !s.path.toLowerCase().includes(q)) return;
        const isSel = idx === selectedIdx;
        const item = document.createElement('div');
        item.className = 'p-3 rounded-lg cursor-pointer transition flex flex-col gap-1 border ' + (isSel ? 'bg-indigo-600/15 border-indigo-500/50 text-white' : 'hover:bg-slate-900 border-transparent text-slate-400');
        item.onclick = () => selectSession(idx);
        item.innerHTML = '<div class="flex items-center justify-between"><span class="font-semibold text-sm truncate ' + (isSel ? 'text-indigo-400' : 'text-slate-200') + '">' + escapeHtml(ext) + '</span><span class="text-xs text-slate-500">' + new Date(s.mod_time).toLocaleTimeString([], {hour:"2-digit",minute:"2-digit"}) + '</span></div><div class="flex items-center justify-between text-xs text-slate-500"><span>' + s.total_steps + ' steps</span><span class="px-1.5 py-0.5 rounded text-[10px] ' + (s.last_status === 'DONE' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400') + '">' + s.last_status + '</span></div>';
        el.appendChild(item);
      });
    }
    function selectSession(idx) {
      selectedIdx = idx;
      renderSidebar();
      const s = sessions[idx];
      if (!s) return;
      document.getElementById('chatHeader').innerHTML = '<div class="flex items-center gap-3"><span class="font-semibold text-slate-200">Session ID:</span><code class="text-xs bg-slate-900 px-2 py-1 rounded text-slate-300 border border-slate-800">' + escapeHtml(s.external_id || s.path) + '</code></div><div class="text-xs text-slate-500">' + new Date(s.mod_time).toLocaleString() + '</div>';
      const c = document.getElementById('chatContent');
      c.innerHTML = '';
      (s.raw_jsonl || '').trim().split('\n').forEach(line => {
        if (!line.trim()) return;
        try {
          const step = JSON.parse(line);
          const el = renderStep(step);
          if (el) c.appendChild(el);
        } catch(e){}
      });
      document.querySelectorAll('pre code').forEach(el => hljs.highlightElement(el));
    }
    function renderStep(step) {
      const d = document.createElement('div');
      if (step.type === 'USER_INPUT') {
        d.className = 'flex justify-end';
        let raw = (step.content || '').replace(/<\/?USER_REQUEST>/g, '').replace(/<ADDITIONAL_METADATA>[\s\S]*?<\/ADDITIONAL_METADATA>/g, '').trim();
        d.innerHTML = '<div class="max-w-2xl bg-indigo-600/20 border border-indigo-500/30 rounded-2xl rounded-tr-sm px-5 py-3.5 text-slate-100 shadow-sm"><div class="text-xs font-semibold text-indigo-400 mb-1">User Request</div><div class="prose prose-invert prose-sm leading-relaxed">' + marked.parse(raw) + '</div></div>';
        return d;
      }
      if (step.type === 'PLANNER_RESPONSE' || step.type === 'MCP_TOOL') {
        d.className = 'space-y-3';
        let h = '';
        if (step.thinking) h += '<details class="bg-amber-950/20 border border-amber-500/30 rounded-xl p-3 text-xs text-amber-300"><summary class="font-semibold cursor-pointer select-none">Thinking / Chain of Thought</summary><div class="mt-2 text-slate-300 whitespace-pre-wrap leading-relaxed">' + escapeHtml(step.thinking) + '</div></details>';
        if (step.tool_calls) {
          step.tool_calls.forEach(tc => {
            const name = tc.args?.ToolName || tc.name || 'tool';
            h += '<div class="bg-slate-900 border border-slate-800 rounded-xl p-3 text-xs"><div class="flex items-center justify-between text-indigo-400 font-mono font-semibold mb-2"><span>?? Tool Call: ' + escapeHtml(name) + '</span><span class="text-slate-500 text-[11px]">' + (tc.args?.ServerName || 'mcp') + '</span></div><pre class="bg-slate-950 p-2.5 rounded border border-slate-800 overflow-x-auto text-slate-300 font-mono">' + escapeHtml(JSON.stringify(tc.args?.Arguments || tc.args || {}, null, 2)) + '</pre></div>';
          });
        }
        if (step.content && step.type === 'MCP_TOOL') h += '<div class="bg-slate-900/60 border border-emerald-500/20 rounded-xl p-3 text-xs text-emerald-300"><div class="font-semibold text-emerald-400 mb-1">? Tool Result</div><pre class="whitespace-pre-wrap text-slate-300 font-mono bg-slate-950 p-2 rounded">' + escapeHtml(step.content) + '</pre></div>';
        if (step.content && step.type === 'PLANNER_RESPONSE') h += '<div class="bg-slate-900 border border-slate-800 rounded-2xl rounded-tl-sm p-5 text-slate-100 shadow-sm"><div class="text-xs font-semibold text-slate-400 mb-2">?? Aerial Response</div><div class="prose prose-invert prose-sm max-w-none leading-relaxed">' + marked.parse(step.content) + '</div></div>';
        if (step.error) h += '<div class="bg-red-950/30 border border-red-500/30 rounded-xl p-4 text-xs text-red-300"><div class="font-bold text-red-400 mb-1">? Error Detail</div><pre class="whitespace-pre-wrap font-mono">' + escapeHtml(typeof step.error === 'string' ? step.error : JSON.stringify(step.error, null, 2)) + '</pre></div>';
        if (!h) return null;
        d.innerHTML = h;
        return d;
      }
      return null;
    }
    function escapeHtml(s) { if (!s) return ''; return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
    fetchTranscripts();
    setInterval(fetchTranscripts, 8000);
  </script>
</body>
</html>`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(html))
}

func main() {
	port, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig := loadConfig()

	dbPath := getDBPath()
	db, err := initDB(dbPath)
	if err != nil {
		log.Printf("Warning: failed to initialize SQLite DB at %s: %v", dbPath, err)
	} else {
		defer db.Close()
	}

	mux := http.NewServeMux()
	promptHandler := handlePrompt(db, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig)
	transcriptHandler := handleTranscripts(db)

	mux.HandleFunc("GET /", handleIndexUI)
	mux.HandleFunc("GET /ui", handleIndexUI)
	mux.HandleFunc("POST /prompt", promptHandler)
	mux.HandleFunc("POST /api/prompt", promptHandler)
	mux.HandleFunc("GET /transcripts", transcriptHandler)
	mux.HandleFunc("GET /api/transcripts", transcriptHandler)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /api/health", handleHealth)

	addr := ":" + port
	authStatus := "no API key configured"
	if apiKey != "" {
		authStatus = "Gemini API key configured"
	}
	mcpStatus := "no MCP servers configured"
	if len(mcpConfig) > 0 {
		mcpStatus = "custom MCP config loaded"
	}
	log.Printf("Aerial Brain server listening on %s (agy binary: %s, model: %s, timeout: %dm, auth: %s, mcp: %s, db: %s)",
		addr, agyBin, model, timeoutMinutes, authStatus, mcpStatus, dbPath)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

