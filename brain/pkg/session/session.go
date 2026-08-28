package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func FindLatestSessionDir(after time.Time) string {
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

func DumpSessionDiagnosticLogs(convID string) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	searchDirs := []string{
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain", convID, ".system_generated", "logs"),
		filepath.Join("/data", "brain", convID, ".system_generated", "logs"),
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "log"),
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "crashes"),
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

		type fileInfo struct {
			path    string
			modTime time.Time
			size    int64
		}
		var files []fileInfo
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err == nil {
				files = append(files, fileInfo{
					path:    filepath.Join(dir, entry.Name()),
					modTime: info.ModTime(),
					size:    info.Size(),
				})
			}
		}

		for i := 0; i < len(files) && i < 2; i++ {
			f := files[len(files)-1-i]
			data, err := os.ReadFile(f.path)
			if err != nil || len(data) == 0 {
				continue
			}
			sb.WriteString(fmt.Sprintf("\n--- FILE: %s (%d bytes, mod: %s) ---\n", f.path, f.size, f.modTime.Format(time.RFC3339)))
			lines := strings.Split(string(data), "\n")
			start := 0
			if len(lines) > 50 {
				start = len(lines) - 50
				sb.WriteString("[...showing last 50 lines...]\n")
			}
			for j := start; j < len(lines); j++ {
				line := lines[j]
				if strings.TrimSpace(line) != "" {
					sb.WriteString(line + "\n")
				}
			}
		}
	}
	return sb.String()
}

func ExtractResponseAndError(convID string) (string, string) {
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
	} else {
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
