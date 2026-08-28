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
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/azylman/aerial/brain/pkg/skills"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type PromptRequest struct {
	Prompt         string `json:"prompt"`
	ConversationID string `json:"conversation_id"`
}

func executePrompt(database *sql.DB, req PromptRequest, agyBin, apiKey, model, systemPrompt string, timeoutMinutes int, mcpConfig json.RawMessage, onComplete func()) string {
	externalConvID := strings.TrimSpace(req.ConversationID)
	if externalConvID == "" {
		externalConvID = uuid.New().String()
	}

	internalConvID, _ := db.GetInternalConversationID(database, externalConvID)

	go func(prompt, extID, intID string) {
		if onComplete != nil {
			defer onComplete()
		}

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
		if _, err := os.Stat("/share/aerial"); err == nil {
			cmd.Dir = "/share/aerial"
		} else {
			cmd.Dir = "/app"
		}
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
				activeInternalID = session.FindLatestSessionDir(startTime)
			}
		}

		lookupID := activeInternalID
		if lookupID == "" {
			lookupID = extID
		}
		outText, errDetail := session.ExtractResponseAndError(lookupID)
		if outText == "" {
			outText = strings.TrimSpace(stdout.String())
		}

		isFailure := exitCode != 0 || strings.Contains(strings.ToLower(outText), "agent execution terminated") || errDetail != "" || strings.Contains(strings.ToLower(stderrStr), "error encountered")

		log.Printf("Execution finished | external_conv=%s internal_conv=%s exit_code=%d failure=%t", extID, activeInternalID, exitCode, isFailure)
		if isFailure {
			if extID != "" {
				_, _ = database.Exec("DELETE FROM conversations WHERE external_id = ?", extID)
				log.Printf("Evicted broken conversation mapping for external_conv: %s (internal_conv: %s) to prevent repeat failure loops", extID, activeInternalID)
			}
			diagLogs := session.DumpSessionDiagnosticLogs(lookupID)
			log.Printf("=== AGENT ERROR DIAGNOSTIC REPORT ===\nCommand: %s %v\nExit Code: %d\nStdout: %s\nStderr: %s\nParsed Error: %s\nTranscript & System Logs:\n%s\n=====================================",
				agyBin, args, exitCode, stdout.String(), stderrStr, errDetail, diagLogs)
		} else {
			if activeInternalID != "" && extID != "" {
				_ = db.SaveConversationMapping(database, extID, activeInternalID)
			}
			log.Printf("--- STDOUT / RESPONSE ---\n%s\n--- STDERR ---\n%s", outText, stderrStr)
		}
	}(req.Prompt, externalConvID, internalConvID)

	return externalConvID
}

func handlePrompt(database *sql.DB, agyBin, apiKey, model, systemPrompt string, timeoutMinutes int, mcpConfig json.RawMessage) http.HandlerFunc {
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

		externalConvID := executePrompt(database, req, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig, nil)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":          "accepted",
			"conversation_id": externalConvID,
			"message":         "Prompt execution started in background",
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
	portStr, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig := config.LoadConfig()

	if apiKey != "" {
		if err := config.EnsureAgySettings(apiKey, model); err != nil {
			log.Printf("Warning: EnsureAgySettings error: %v", err)
		}
	}
	if err := config.EnsureSystemRules(systemPrompt); err != nil {
		log.Printf("Warning: EnsureSystemRules error: %v", err)
	}
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
		log.Fatalf("Failed to initialize conversation database: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	go startDiscordFunnel(database, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig)

	http.HandleFunc("/prompt", handlePrompt(database, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig))
	http.HandleFunc("/transcripts", handleTranscripts(database))
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	log.Printf("Aerial Brain listening on port %s (model=%s, timeout=%dm)", portStr, model, timeoutMinutes)
	if err := http.ListenAndServe(":"+portStr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
