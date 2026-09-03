package session

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
			if !entry.IsDir() || strings.HasPrefix(entry.Name(), "ambient-eval-") {
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

func getTargetDirs(convID string) []string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	if convID != "" {
		return []string{
			filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", convID),
			filepath.Join(homeDir, ".gemini", "antigravity", "brain", convID),
			filepath.Join("/data", "brain", convID),
		}
	}

	brainRoots := []string{
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		filepath.Join("/data", "brain"),
	}

	var targetDirs []string
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
	return targetDirs
}

func ExtractResponseAndError(convID string) (string, string) {
	targetDirs := getTargetDirs(convID)

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
			lastUserInputIdx := -1
			for i, rawLine := range lines {
				line := strings.TrimSpace(rawLine)
				if line == "" {
					continue
				}
				var step struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if step.Type == "USER_INPUT" {
						lastUserInputIdx = i
					}
				}
			}

			// Only search for responses and errors strictly AFTER the last USER_INPUT!
			startIdx := 0
			if lastUserInputIdx >= 0 {
				startIdx = lastUserInputIdx + 1
			}

			for i := len(lines) - 1; i >= startIdx; i-- {
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

func HasSuccessfulToolCall(convID string) bool {
	targetDirs := getTargetDirs(convID)

	for _, dir := range targetDirs {
		for _, name := range []string{"transcript_full.jsonl", "transcript.jsonl"} {
			tPath := filepath.Join(dir, ".system_generated", "logs", name)
			data, err := os.ReadFile(tPath)
			if err != nil {
				continue
			}

			lines := strings.Split(string(data), "\n")
			lastUserInputIdx := -1
			for i, rawLine := range lines {
				line := strings.TrimSpace(rawLine)
				if line == "" {
					continue
				}
				var step struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if step.Type == "USER_INPUT" {
						lastUserInputIdx = i
					}
				}
			}

			startIdx := 0
			if lastUserInputIdx >= 0 {
				startIdx = lastUserInputIdx + 1
			}

			for i := startIdx; i < len(lines); i++ {
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}
				var step struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if step.Status == "DONE" && (step.Type == "MCP_TOOL" || step.Type == "RUN_COMMAND" || step.Type == "CODE_ACTION" || step.Type == "WRITE_TO_FILE") {
						return true
					}
				}
			}
		}
	}
	return false
}

// resolveBaseDir locates the appropriate brain base directory for sessions.
func resolveBaseDir(sessionID string) string {
	if fi, err := os.Stat("/data/brain"); err == nil && fi.IsDir() {
		return "/data/brain"
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}

	// Check if session directory already exists under any known target dir
	for _, targetDir := range getTargetDirs(sessionID) {
		if fi, err := os.Stat(targetDir); err == nil && fi.IsDir() {
			return filepath.Dir(targetDir)
		}
	}

	// Check existing parent root dirs from getTargetDirs
	brainRoots := []string{
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
		"/data/brain",
	}
	for _, root := range brainRoots {
		if fi, err := os.Stat(root); err == nil && fi.IsDir() {
			return root
		}
	}

	// Fallback to primary root under homeDir
	return filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
}

// EnsureSessionDir bootstraps the session directory and transcript files if they do not exist.
func EnsureSessionDir(sessionID string) (string, error) {
	if strings.TrimSpace(sessionID) == "" {
		return "", fmt.Errorf("sessionID cannot be empty")
	}

	baseDir := resolveBaseDir(sessionID)
	sessionDir := filepath.Join(baseDir, sessionID)
	logsDir := filepath.Join(sessionDir, ".system_generated", "logs")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create session logs directory: %w", err)
	}

	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		filePath := filepath.Join(logsDir, name)
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to ensure %s: %w", name, err)
		}
		_ = f.Close()
	}

	return sessionDir, nil
}

// TranscriptStep represents a canonical Antigravity transcript step.
type TranscriptStep struct {
	StepIndex int    `json:"step_index"`
	Source    string `json:"source"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	Content   string `json:"content"`
}

var appendTurnMu sync.Mutex

// AppendAmbientTurn quietly appends an unaddressed ambient chat message to the session's transcripts.
func AppendAmbientTurn(sessionID, channelName, authorName, text string, timestamp time.Time) error {
	if strings.TrimSpace(sessionID) == "" {
		return nil
	}

	appendTurnMu.Lock()
	defer appendTurnMu.Unlock()

	var logsDir string
	for _, dir := range getTargetDirs(sessionID) {
		c1 := filepath.Join(dir, ".system_generated", "logs")
		if _, err := os.Stat(filepath.Join(c1, "transcript.jsonl")); err == nil {
			logsDir = c1
			break
		}
		c2 := filepath.Join(dir, sessionID, ".system_generated", "logs")
		if _, err := os.Stat(filepath.Join(c2, "transcript.jsonl")); err == nil {
			logsDir = c2
			break
		}
	}

	if logsDir == "" {
		return nil
	}

	transcriptPath := filepath.Join(logsDir, "transcript.jsonl")
	lastIndex, err := getLastStepIndex(transcriptPath)
	if err != nil {
		return fmt.Errorf("failed to read last step index: %w", err)
	}

	nextIndex := 0
	if lastIndex >= 0 {
		nextIndex = lastIndex + 1
	}

	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	} else {
		timestamp = timestamp.UTC()
	}
	timeStr := timestamp.Format(time.RFC3339)
	cleanChannel := strings.TrimLeft(strings.TrimSpace(channelName), "#")
	cleanAuthor := strings.TrimPrefix(strings.TrimSpace(authorName), "@")
	content := fmt.Sprintf("[Chat #%s] @%s (%s): %s", cleanChannel, cleanAuthor, timeStr, text)

	step := TranscriptStep{
		StepIndex: nextIndex,
		Source:    "USER_EXPLICIT",
		Type:      "USER_INPUT",
		Status:    "DONE",
		CreatedAt: timeStr,
		Content:   content,
	}

	lineBytes, err := json.Marshal(step)
	if err != nil {
		return fmt.Errorf("failed to marshal transcript step: %w", err)
	}

	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		filePath := filepath.Join(logsDir, name)
		if err := appendTranscriptStep(filePath, lineBytes); err != nil {
			return fmt.Errorf("failed to append to %s: %w", name, err)
		}
	}

	return nil
}

func appendTranscriptStep(filePath string, lineBytes []byte) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	var writeBuf []byte
	if fi.Size() > 0 {
		lastByte := make([]byte, 1)
		if _, err := f.ReadAt(lastByte, fi.Size()-1); err == nil {
			if lastByte[0] != '\n' {
				writeBuf = append(writeBuf, '\n')
			}
		}
	}

	writeBuf = append(writeBuf, lineBytes...)
	writeBuf = append(writeBuf, '\n')

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	if _, err := f.Write(writeBuf); err != nil {
		return err
	}

	return nil
}

func getLastStepIndex(filePath string) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return -1, err
	}
	if fi.Size() == 0 {
		return -1, nil
	}

	fileSize := fi.Size()
	chunkSize := int64(65536)
	seekSizes := []int64{4096}
	for offset := chunkSize; ; offset += chunkSize {
		seekSizes = append(seekSizes, offset)
		if offset >= fileSize {
			break
		}
	}

	for _, seekSize := range seekSizes {
		if seekSize > fileSize {
			seekSize = fileSize
		}
		if _, err := f.Seek(-seekSize, io.SeekEnd); err != nil {
			return -1, err
		}

		buf := make([]byte, seekSize)
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return -1, err
		}

		lines := strings.Split(strings.TrimRight(string(buf[:n]), "\r\n"), "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if line == "" {
				continue
			}
			var hdr struct {
				StepIndex *int `json:"step_index"`
			}
			if err := json.Unmarshal([]byte(line), &hdr); err == nil && hdr.StepIndex != nil {
				return *hdr.StepIndex, nil
			}
		}

		if seekSize >= fileSize {
			break
		}
	}

	return -1, nil
}


