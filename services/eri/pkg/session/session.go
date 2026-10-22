package session

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DefaultMinSessionTurns defines the engine-wide minimum turns before eligible for routine session rotation.
const DefaultMinSessionTurns = 3

// DefaultEmergencyRotationMultiplier defines the factor multiplied with DefaultMaxSessionTurns for emergency rotation.
const DefaultEmergencyRotationMultiplier = 3

// DefaultMaxSessionTurns defines the engine-wide maximum user turns (8 turns) before session rotation.
const DefaultMaxSessionTurns = 8

// DefaultMaxSessionSteps defines the engine-wide maximum internal steps (180 steps) before session rotation.
const DefaultMaxSessionSteps = 180

// DefaultMaxTranscriptBytes defines the engine-wide maximum transcript size (1 MB) before rotation.
const DefaultMaxTranscriptBytes = 1024 * 1024

// DefaultMaxSessionTranscriptBytes defines the engine-wide maximum transcript file size in bytes (2 MB) before session rotation.
const DefaultMaxSessionTranscriptBytes = 2 * 1024 * 1024

// DefaultMaxSessionDBBytes defines the engine-wide maximum conversation SQLite/protobuf size in bytes (4 MB) before session rotation.
const DefaultMaxSessionDBBytes = 4 * 1024 * 1024

// DefaultMaxQuotaPauseDBBytes defines the reactive conversation size threshold in bytes (3 MB) to trigger session rotation upon quota exhaustion.
const DefaultMaxQuotaPauseDBBytes = 3 * 1024 * 1024

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

// Roots returns a copy of the configured search roots for this Manager.
func (m *Manager) Roots() []string {
	if m == nil {
		return nil
	}
	return append([]string(nil), m.roots...)
}

func cleanSessionID(sessionID string) (string, bool) {
	cleanID := strings.TrimSpace(sessionID)
	if cleanID == "" || strings.ContainsAny(cleanID, `/\:`) || strings.Contains(cleanID, "..") {
		return "", false
	}
	return cleanID, true
}

func (m *Manager) validSessionID(sessionID string) (string, bool) {
	if m == nil {
		return "", false
	}
	return cleanSessionID(sessionID)
}

func isAmbientInput(source, content string) bool {
	return source == SourceAmbient || strings.HasPrefix(strings.TrimSpace(content), "[Chat ")
}

func findLastUserInputIdx(lines []string) int {
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
			if step.Type == "USER_INPUT" && !isAmbientInput(step.Source, step.Content) {
				lastUserInputIdx = i
			}
		}
	}
	return lastUserInputIdx
}

func isStepBefore(createdAt string, since time.Time) bool {
	if createdAt == "" || since.IsZero() {
		return false
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		return t.Before(since)
	}
	return false
}

// LastActivityFromRoots returns the latest modification timestamp across all log files
// (transcripts and background task logs) for a session ID within the provided search roots.
// Zero ambient environment defaults are used.
func LastActivityFromRoots(sessionID string, roots []string) (time.Time, error) {
	cleanID, ok := cleanSessionID(sessionID)
	if !ok || len(roots) == 0 {
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
			expandedRoot = fmt.Sprintf(expandedRoot, cleanID)
		} else if strings.Contains(expandedRoot, "{session}") {
			expandedRoot = strings.ReplaceAll(expandedRoot, "{session}", cleanID)
		}

		sessDir := filepath.Join(expandedRoot, cleanID)
		if fi, err := os.Stat(sessDir); err != nil || !fi.IsDir() {
			// Also check if expandedRoot is directly the session directory or points to .system_generated/logs
			if fiRoot, errRoot := os.Stat(expandedRoot); errRoot == nil && fiRoot.IsDir() {
				cleanExpanded := filepath.Clean(expandedRoot)
				if filepath.Base(cleanExpanded) == cleanID {
					sessDir = cleanExpanded
				} else if strings.HasSuffix(cleanExpanded, filepath.Clean(filepath.Join(cleanID, ".system_generated", "logs"))) ||
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

func (m *Manager) getTargetDirs(convID string) []string {
	if m == nil || len(m.roots) == 0 {
		return nil
	}

	if convID != "" {
		var res []string
		seen := make(map[string]bool)
		for _, root := range m.roots {
			p := filepath.Join(root, convID)
			if !seen[p] {
				res = append(res, p)
				seen[p] = true
			}
		}
		if m.dataDir != "" {
			for _, runtime := range []string{"discord", "voice", "ephemeral"} {
				p := filepath.Join(m.dataDir, "runtimes", runtime, ".gemini", "antigravity-cli", "brain", convID)
				if !seen[p] {
					res = append(res, p)
					seen[p] = true
				}
				p2 := filepath.Join(m.dataDir, "runtimes", runtime, ".gemini", "antigravity", "brain", convID)
				if !seen[p2] {
					res = append(res, p2)
					seen[p2] = true
				}
			}
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

// ExtractLastTurnError parses transcript files to extract any error step specifically occurring
// during the active turn (strictly after 'since' or after the latest non-ambient USER_INPUT).
func (m *Manager) ExtractLastTurnError(ctx context.Context, convID string, since time.Time) (string, error) {
	cleanID, ok := m.validSessionID(convID)
	if !ok {
		return "", nil
	}
	if ctx != nil && ctx.Err() != nil {
		return "", ctx.Err()
	}

	targetDirs := m.getTargetDirs(cleanID)

	for _, dir := range targetDirs {
		for _, name := range []string{"transcript_full.jsonl", "transcript.jsonl"} {
			if ctx != nil && ctx.Err() != nil {
				return "", ctx.Err()
			}
			tPath := filepath.Join(dir, ".system_generated", "logs", name)
			f, err := os.Open(tPath)
			if err != nil {
				continue
			}

			fi, err := f.Stat()
			if err != nil || fi.Size() == 0 {
				if closeErr := f.Close(); closeErr != nil {
					log.Printf("[Session] Warning: failed to close file %s: %v", tPath, closeErr)
				}
				continue
			}

			fileSize := fi.Size()
			readSize := fileSize
			var offset int64 = 0
			const maxChunk = int64(1024 * 1024)
			if fileSize > maxChunk {
				readSize = maxChunk
				offset = fileSize - maxChunk
			}

			buf := make([]byte, readSize)
			_, err = f.ReadAt(buf, offset)
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("[Session] Warning: failed to close file %s: %v", tPath, closeErr)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				continue
			}

			lines := strings.Split(string(buf), "\n")
			if offset > 0 && len(lines) > 1 {
				lines = lines[1:]
			}

			lastUserInputIdx := findLastUserInputIdx(lines)
			if lastUserInputIdx == -1 && offset > 0 {
				fullData, readErr := os.ReadFile(tPath)
				if readErr == nil {
					lines = strings.Split(string(fullData), "\n")
					lastUserInputIdx = findLastUserInputIdx(lines)
				}
			}

			startIdx := 0
			if lastUserInputIdx >= 0 {
				startIdx = lastUserInputIdx + 1
			}

			for i := len(lines) - 1; i >= startIdx; i-- {
				if ctx != nil && ctx.Err() != nil {
					return "", ctx.Err()
				}
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}
				var step struct {
					Type      string          `json:"type"`
					Status    string          `json:"status"`
					Error     json.RawMessage `json:"error"`
					Content   string          `json:"content"`
					CreatedAt string          `json:"created_at"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if isStepBefore(step.CreatedAt, since.Add(-10*time.Second)) {
						break
					}

					if step.Type == "GENERIC" || step.Type == "USER_INPUT" {
						continue
					}

					if step.Status == "ERROR" || len(step.Error) > 0 || step.Type == "ERROR_MESSAGE" {
						if errStr := strings.TrimSpace(string(step.Error)); errStr != "" && errStr != "null" {
							var unquoted string
							if json.Unmarshal(step.Error, &unquoted) == nil && unquoted != "" {
								return unquoted, nil
							}
							return strings.Trim(errStr, "\""), nil
						}
						if trimmedContent := strings.TrimSpace(step.Content); trimmedContent != "" {
							return trimmedContent, nil
						}
					}
				}
			}
		}
	}

	return "", nil
}

// ExtractResponseSince parses transcript files for convID and returns the latest substantive
// PLANNER_RESPONSE turn strictly at or after 'since', or empty string if not found.
// It searches transcript files (transcript_full.jsonl, transcript.jsonl) from newest to oldest,
// applies a strict monotonic cutoff (step.CreatedAt >= since), ignores tool-call-only turns
// and empty steps, and returns the trimmed substantive response.
func (m *Manager) ExtractResponseSince(convID string, since time.Time) string {
	return m.ExtractResponseSinceContext(context.Background(), convID, since)
}

// ExtractResponseSinceContext parses transcript files for convID with context cancellation support.
func (m *Manager) ExtractResponseSinceContext(ctx context.Context, convID string, since time.Time) string {
	cleanID, ok := m.validSessionID(convID)
	if !ok || (ctx != nil && ctx.Err() != nil) {
		return ""
	}

	targetDirs := m.getTargetDirs(cleanID)

	for _, dir := range targetDirs {
		for _, name := range []string{"transcript_full.jsonl", "transcript.jsonl"} {
			if ctx != nil && ctx.Err() != nil {
				return ""
			}
			tPath := filepath.Join(dir, ".system_generated", "logs", name)
			data, err := os.ReadFile(tPath)
			if err != nil || len(data) == 0 {
				continue
			}

			lines := strings.Split(string(data), "\n")
			for i := len(lines) - 1; i >= 0; i-- {
				if ctx != nil && ctx.Err() != nil {
					return ""
				}
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}

				var step struct {
					Type      string            `json:"type"`
					Status    string            `json:"status"`
					Content   string            `json:"content"`
					CreatedAt string            `json:"created_at"`
					ToolCalls []json.RawMessage `json:"tool_calls"`
				}
				if err := json.Unmarshal([]byte(line), &step); err != nil {
					continue
				}

				if isStepBefore(step.CreatedAt, since) {
					break
				}

				if step.Type != "PLANNER_RESPONSE" {
					continue
				}

				trimmedContent := strings.TrimSpace(step.Content)
				if trimmedContent == "" || strings.HasPrefix(trimmedContent, "[Tool Call Requested]:") {
					continue
				}

				return trimmedContent
			}
		}
	}

	return ""
}

func getLastStepIndex(filePath string) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Session] Warning closing transcript file %s on getLastStepIndex: %v", filePath, closeErr)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return -1, err
	}
	if fi.IsDir() {
		return -1, fmt.Errorf("path %s is a directory, not a file", filePath)
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

// conversationCandidateDirs returns all potential conversation directories across configured roots.
func (m *Manager) conversationCandidateDirs() []string {
	if m == nil {
		return nil
	}
	var dirs []string
	if m.dataDir != "" {
		dirs = append(dirs, filepath.Join(m.dataDir, "conversations"))
		for _, runtime := range []string{"discord", "voice", "ephemeral"} {
			dirs = append(dirs,
				filepath.Join(m.dataDir, "runtimes", runtime, ".gemini", "antigravity-cli", "conversations"),
				filepath.Join(m.dataDir, "runtimes", runtime, ".gemini", "antigravity", "conversations"),
			)
		}
	}
	if m.homeDir != "" {
		dirs = append(dirs,
			filepath.Join(m.homeDir, ".gemini", "antigravity-cli", "conversations"),
			filepath.Join(m.homeDir, ".gemini", "antigravity", "conversations"),
		)
	}
	return dirs
}

// SessionExistsOnDisk checks if the session has a valid agy conversation
// SQLite database (.db), protobuf (.pb), or non-empty transcript on disk.
func (m *Manager) SessionExistsOnDisk(sessionID string) bool {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return false
	}

	// 1. Check agy conversation SQLite databases (.db) and protobufs (.pb) under candidate directories
	for _, dir := range m.conversationCandidateDirs() {
		for _, ext := range []string{".db", ".pb"} {
			convPath := filepath.Join(dir, cleanID+ext)
			if fi, err := os.Stat(convPath); err == nil && !fi.IsDir() && fi.Size() > 0 {
				return true
			} else if err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("[Session] Warning statting conversation candidate %s: %v", convPath, err)
			}
		}
	}

	// 2. Check transcript.jsonl non-empty status
	for _, dir := range m.getTargetDirs(cleanID) {
		tPath := filepath.Join(dir, ".system_generated", "logs", "transcript.jsonl")
		if fi, err := os.Stat(tPath); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[Session] Warning statting transcript candidate %s: %v", tPath, err)
		}
	}
	return false
}

// GetTranscriptSize returns the maximum file size across transcript.jsonl and transcript_full.jsonl
// for the given session ID across configured session roots.
// Returns 0 if manager is nil, sessionID is empty/invalid, or files do not exist.
func (m *Manager) GetTranscriptSize(sessionID string) int64 {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return 0
	}

	var maxSize int64
	for _, sessDir := range m.getTargetDirs(cleanID) {
		logsDir := filepath.Join(sessDir, ".system_generated", "logs")
		for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
			filePath := filepath.Join(logsDir, name)
			if fi, err := os.Stat(filePath); err == nil && !fi.IsDir() {
				if fi.Size() > maxSize {
					maxSize = fi.Size()
				}
			}
		}
	}
	return maxSize
}

// GetSessionDBSize returns the maximum file size across candidate conversation SQLite databases
// (.db) and protobufs (.pb) for the given session ID across configured session roots.
// Ephemeral SQLite WAL files (.db-wal) are intentionally excluded so transient uncheckpointed
// write buffers do not trigger premature session rotation.
// Returns 0 if manager is nil, sessionID is empty/invalid, or files do not exist.
func (m *Manager) GetSessionDBSize(sessionID string) int64 {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return 0
	}

	var maxSize int64
	for _, dir := range m.conversationCandidateDirs() {
		for _, ext := range []string{".db", ".pb"} {
			p := filepath.Join(dir, cleanID+ext)
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
				if fi.Size() > maxSize {
					maxSize = fi.Size()
				}
			} else if err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("[Session] Warning statting conversation %s: %v", p, err)
			}
		}
	}

	return maxSize
}

// CountTranscriptSteps returns the total internal steps for the session by inspecting
// the monotonic step_index at the tail of the transcript file. Returns 0 if empty, invalid, or not found.
func (m *Manager) CountTranscriptSteps(sessionID string) int {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return 0
	}

	m.appendMu.Lock()
	defer m.appendMu.Unlock()

	var maxSteps int
	for _, sessDir := range m.getTargetDirs(cleanID) {
		logsDir := filepath.Join(sessDir, ".system_generated", "logs")
		for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
			filePath := filepath.Join(logsDir, name)
			lastIdx, err := getLastStepIndex(filePath)
			if err == nil && lastIdx >= 0 {
				steps := lastIdx + 1
				if steps > maxSteps {
					maxSteps = steps
				}
			}
		}
	}
	return maxSteps
}

// CountTranscriptTurns returns the total number of non-ambient user turns for the session
// by counting non-ambient USER_INPUT entries in transcript.jsonl. Returns 0 if empty, invalid, or not found.
func (m *Manager) CountTranscriptTurns(sessionID string) int {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return 0
	}

	var maxTurns int
	for _, sessDir := range m.getTargetDirs(cleanID) {
		logsDir := filepath.Join(sessDir, ".system_generated", "logs")
		for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
			filePath := filepath.Join(logsDir, name)
			f, err := os.Open(filePath)
			if err != nil {
				continue
			}
			scanner := bufio.NewScanner(f)
			buf := make([]byte, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			var turns int
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" || !strings.Contains(line, "USER_INPUT") {
					continue
				}
				var step struct {
					Source  string `json:"source"`
					Type    string `json:"type"`
					Content string `json:"content"`
				}
				if err := json.Unmarshal([]byte(line), &step); err == nil {
					if step.Type == "USER_INPUT" && !isAmbientInput(step.Source, step.Content) {
						turns++
					}
				}
			}
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("[Session] Warning: failed to close file %s: %v", filePath, closeErr)
			}
			if turns > maxTurns {
				maxTurns = turns
			}
		}
	}
	return maxTurns
}
