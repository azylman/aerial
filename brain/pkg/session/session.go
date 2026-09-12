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



// DefaultMaxSessionTurns defines the engine-wide maximum turn limit before an agy session is rotated.
const DefaultMaxSessionTurns = 10

// SourceAmbient represents ambient chat messages appended to session transcripts.
const SourceAmbient = "AMBIENT"

// Manager manages session directories, transcripts, and activity tracking
// with explicitly injected homeDir and dataDir paths.
type Manager struct {
	homeDir  string
	dataDir  string
	roots    []string
	appendMu sync.Mutex
}

// New creates a new session Manager with explicitly injected homeDir and dataDir.
// No ambient environment fallbacks (os.UserHomeDir, os.Getenv("HOME"), /root, /data) are used.
func New(homeDir, dataDir string) *Manager {
	cleanHome := strings.TrimSpace(homeDir)
	cleanData := strings.TrimSpace(dataDir)

	var roots []string
	if cleanData != "" {
		roots = append(roots, filepath.Join(cleanData, "brain"))
	}
	if cleanHome != "" {
		roots = append(roots,
			filepath.Join(cleanHome, ".gemini", "antigravity-cli", "brain"),
			filepath.Join(cleanHome, ".gemini", "antigravity", "brain"),
		)
	}

	return &Manager{
		homeDir: cleanHome,
		dataDir: cleanData,
		roots:   roots,
	}
}

// Roots returns a defensive copy of configured session roots.
func (m *Manager) Roots() []string {
	if m == nil || len(m.roots) == 0 {
		return nil
	}
	return append([]string(nil), m.roots...)
}

// HomeDir returns the explicitly configured home directory for this Manager.
func (m *Manager) HomeDir() string {
	if m == nil {
		return ""
	}
	return m.homeDir
}

// DataDir returns the explicitly configured data directory for this Manager.
func (m *Manager) DataDir() string {
	if m == nil {
		return ""
	}
	return m.dataDir
}

// LastActivityFromRoots returns the latest modification timestamp across all log files
// (transcripts and background task logs) for a session ID within the provided search roots.
// Zero ambient environment defaults are used.
func LastActivityFromRoots(sessionID string, roots []string) (time.Time, error) {
	trimmed := strings.TrimSpace(sessionID)
	// Security: Prevent path traversal
	if trimmed == "" || strings.ContainsAny(trimmed, `/\:`) || strings.Contains(trimmed, "..") {
		return time.Time{}, nil
	}

	if len(roots) == 0 {
		return time.Time{}, nil
	}

	var latestTime time.Time
	updateLatest := func(t time.Time) {
		if t.After(latestTime) {
			latestTime = t
		}
	}

	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		expandedRoot := root
		if strings.Contains(expandedRoot, "%s") {
			expandedRoot = fmt.Sprintf(expandedRoot, trimmed)
		} else if strings.Contains(expandedRoot, "{session}") {
			expandedRoot = strings.ReplaceAll(expandedRoot, "{session}", trimmed)
		}

		sessDir := filepath.Join(expandedRoot, trimmed)
		if fi, err := os.Stat(sessDir); err != nil || !fi.IsDir() {
			// Also check if expandedRoot is directly the session directory or points to .system_generated/logs
			if fiRoot, errRoot := os.Stat(expandedRoot); errRoot == nil && fiRoot.IsDir() {
				cleanExpanded := filepath.Clean(expandedRoot)
				if filepath.Base(cleanExpanded) == trimmed {
					sessDir = cleanExpanded
				} else if strings.HasSuffix(cleanExpanded, filepath.Clean(filepath.Join(trimmed, ".system_generated", "logs"))) ||
					strings.HasSuffix(cleanExpanded, filepath.Clean(filepath.Join(".system_generated", "logs"))) {
					sessDir = filepath.Dir(filepath.Dir(cleanExpanded))
				} else {
					continue
				}
			} else {
				continue
			}
		}

		// 1. Transcripts in .system_generated/logs/
		logsDir := filepath.Join(sessDir, ".system_generated", "logs")
		for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
			if fi, err := os.Stat(filepath.Join(logsDir, name)); err == nil && !fi.IsDir() {
				updateLatest(fi.ModTime())
			}
		}

		// 2. Background task logs in .system_generated/tasks/*.log
		tasksDir := filepath.Join(sessDir, ".system_generated", "tasks")
		entries, err := os.ReadDir(tasksDir)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					continue
				}
				updateLatest(info.ModTime())
			}
		}
	}

	return latestTime, nil
}

// GetSessionLastActivity returns the latest modification timestamp across all log files
// for a session ID. If customRoots are provided, they override the manager's configured roots.
func (m *Manager) GetSessionLastActivity(sessionID string, customRoots ...string) (time.Time, error) {
	if len(customRoots) > 0 {
		return LastActivityFromRoots(sessionID, customRoots)
	}
	if m == nil {
		return time.Time{}, nil
	}
	return LastActivityFromRoots(sessionID, m.roots)
}

// FindLatestSessionDir scans the manager's configured roots for the newest session directory modified after the given time.
func (m *Manager) FindLatestSessionDir(after time.Time) string {
	if m == nil {
		return ""
	}
	var newestID string
	var newestTime time.Time
	for _, root := range m.roots {
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

// DumpSessionDiagnosticLogs returns formatted diagnostic output from session log files.
func (m *Manager) DumpSessionDiagnosticLogs(convID string) string {
	if m == nil {
		return ""
	}
	var searchDirs []string
	if m.homeDir != "" {
		searchDirs = append(searchDirs,
			filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs"),
			filepath.Join(m.homeDir, ".gemini", "antigravity", "brain", convID, ".system_generated", "logs"),
		)
	}
	if m.dataDir != "" {
		searchDirs = append(searchDirs,
			filepath.Join(m.dataDir, "brain", convID, ".system_generated", "logs"),
		)
	}
	if m.homeDir != "" {
		searchDirs = append(searchDirs,
			filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "log"),
			filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "crashes"),
			filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "logs"),
			filepath.Join(m.homeDir, ".gemini", "antigravity", "logs"),
		)
	}
	if m.dataDir != "" {
		searchDirs = append(searchDirs,
			filepath.Join(m.dataDir, "brain", convID),
		)
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

func (m *Manager) getTargetDirs(convID string) []string {
	if m == nil || len(m.roots) == 0 {
		return nil
	}

	if convID != "" {
		var res []string
		for _, root := range m.roots {
			res = append(res, filepath.Join(root, convID))
		}
		return res
	}

	var targetDirs []string
	for _, root := range m.roots {
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

// ExtractResponseAndError parses transcript files to extract the last response and error for a conversation.
func (m *Manager) ExtractResponseAndError(convID string) (string, string) {
	if m == nil {
		return "", ""
	}
	targetDirs := m.getTargetDirs(convID)

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
					Source  string `json:"source"`
					Type    string `json:"type"`
					Content string `json:"content"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					isAmbient := step.Source == SourceAmbient || (step.Type == "USER_INPUT" && strings.HasPrefix(step.Content, "[Chat #"))
					if step.Type == "USER_INPUT" && !isAmbient {
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

// HasSuccessfulToolCall returns whether the conversation contains any successful tool calls.
func (m *Manager) HasSuccessfulToolCall(convID string) bool {
	if m == nil {
		return false
	}
	targetDirs := m.getTargetDirs(convID)

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
					Source  string `json:"source"`
					Type    string `json:"type"`
					Content string `json:"content"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					isAmbient := step.Source == SourceAmbient || (step.Type == "USER_INPUT" && strings.HasPrefix(step.Content, "[Chat #"))
					if step.Type == "USER_INPUT" && !isAmbient {
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
func (m *Manager) resolveBaseDir(sessionID string) string {
	if m == nil || len(m.roots) == 0 {
		return ""
	}

	// Check if session directory already exists under any known target dir
	for _, targetDir := range m.getTargetDirs(sessionID) {
		if fi, err := os.Stat(targetDir); err == nil && fi.IsDir() {
			return filepath.Dir(targetDir)
		}
	}

	// Check existing parent root dirs from m.roots
	for _, root := range m.roots {
		if fi, err := os.Stat(root); err == nil && fi.IsDir() {
			return root
		}
	}

	// Fallback to first configured root
	return m.roots[0]
}

// EnsureSessionDir bootstraps the session directory and transcript files if they do not exist.
func (m *Manager) EnsureSessionDir(sessionID string) (string, error) {
	if m == nil {
		return "", fmt.Errorf("session manager is nil")
	}
	if strings.TrimSpace(sessionID) == "" {
		return "", fmt.Errorf("sessionID cannot be empty")
	}

	baseDir := m.resolveBaseDir(sessionID)
	if baseDir == "" {
		return "", fmt.Errorf("no session roots configured")
	}
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

// AppendAmbientTurn quietly appends an unaddressed ambient chat message to the session's transcripts.
func (m *Manager) AppendAmbientTurn(sessionID, channelName, authorName, text string, timestamp time.Time) error {
	if m == nil || strings.TrimSpace(sessionID) == "" {
		return nil
	}

	m.appendMu.Lock()
	defer m.appendMu.Unlock()

	var logsDir string
	for _, dir := range m.getTargetDirs(sessionID) {
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
		Source:    SourceAmbient,
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

// SessionExistsOnDisk checks if the session has a valid agy conversation
// protobuf or non-empty transcript on disk.
func (m *Manager) SessionExistsOnDisk(sessionID string) bool {
	if m == nil {
		return false
	}
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" || strings.ContainsAny(trimmed, `/\:`) || strings.Contains(trimmed, "..") {
		return false
	}

	// 1. Check agy conversation protobufs under homeDir (if configured)
	if m.homeDir != "" {
		cliConvPb := filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "conversations", trimmed+".pb")
		if fi, err := os.Stat(cliConvPb); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
		agyConvPb := filepath.Join(m.homeDir, ".gemini", "antigravity", "conversations", trimmed+".pb")
		if fi, err := os.Stat(agyConvPb); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
	}

	// 2. Check transcript.jsonl non-empty status
	for _, dir := range m.getTargetDirs(trimmed) {
		tPath := filepath.Join(dir, ".system_generated", "logs", "transcript.jsonl")
		if fi, err := os.Stat(tPath); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
	}
	return false
}

// CleanupEphemeralSession purges throwaway session directories from disk.
// If searchRoots are provided, it deletes convID subdirectories within those roots.
func CleanupEphemeralSession(convID string, searchRoots ...string) {
	cleanConvID := strings.TrimSpace(convID)
	if cleanConvID == "" || strings.ContainsAny(cleanConvID, `/\:`) || strings.Contains(cleanConvID, "..") {
		return
	}
	for _, root := range searchRoots {
		cleanRoot := strings.TrimSpace(root)
		if cleanRoot == "" {
			continue
		}
		dirs := []string{
			filepath.Join(cleanRoot, cleanConvID),
			filepath.Join(cleanRoot, ".gemini", "antigravity-cli", "brain", cleanConvID),
			filepath.Join(cleanRoot, ".gemini", "antigravity", "brain", cleanConvID),
			filepath.Join(cleanRoot, "brain", cleanConvID),
		}
		for _, d := range dirs {
			_ = os.RemoveAll(d)
		}
	}
}


