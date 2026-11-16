package session

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type codexTranscriptEntry struct {
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type codexPayloadRecord struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Phase   string          `json:"phase"`
	Content json.RawMessage `json:"content"`
	Text    string          `json:"text"`
	Message string          `json:"message"`
	Reason  string          `json:"reason"`
	Error   json.RawMessage `json:"error"`
	IsError bool            `json:"is_error"`
	Status  string          `json:"status"`
}

func extractCodexText(content json.RawMessage, textFallback string) string {
	if textFallback != "" {
		return textFallback
	}
	if len(content) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(content, &text); err == nil {
		return text
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &blocks); err == nil {
		var parts []string
		for _, b := range blocks {
			switch b.Type {
			case "text", "input_text", "output_text":
				if strings.TrimSpace(b.Text) != "" {
					parts = append(parts, b.Text)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func scanCodexBaseForSession(base, cleanID string, seen map[string]bool) []string {
	if cleanID == "" || strings.ContainsAny(cleanID, "*?[]") {
		return nil
	}
	resolvedBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		resolvedBase = base
	}
	fi, err := os.Stat(resolvedBase)
	if err != nil || !fi.IsDir() {
		return nil
	}
	var paths []string
	addIfMatch := func(p string) {
		cleanP := filepath.Clean(p)
		if !seen[cleanP] {
			if s, err := os.Stat(cleanP); err == nil && !s.IsDir() {
				seen[cleanP] = true
				paths = append(paths, cleanP)
			}
		}
	}

	for _, name := range []string{cleanID + ".jsonl", "rollout-" + cleanID + ".jsonl"} {
		addIfMatch(filepath.Join(resolvedBase, name))
	}
	if matches, err := filepath.Glob(filepath.Join(resolvedBase, "rollout-*-"+cleanID+".jsonl")); err == nil && len(matches) > 0 {
		for _, m := range matches {
			addIfMatch(m)
		}
	}

	datePatterns := []string{
		filepath.Join(resolvedBase, "*", "*", "*", cleanID+".jsonl"),
		filepath.Join(resolvedBase, "*", "*", "*", "rollout-"+cleanID+".jsonl"),
		filepath.Join(resolvedBase, "*", "*", "*", "rollout-*-"+cleanID+".jsonl"),
	}
	for _, pattern := range datePatterns {
		if matches, err := filepath.Glob(pattern); err == nil && len(matches) > 0 {
			for _, m := range matches {
				addIfMatch(m)
			}
		}
	}
	return paths
}

// codexTranscriptPaths scans candidate directories under:
// - <dataDir>/codex/sessions
// - <dataDir>/runtimes/*/.codex/sessions
// - <homeDir>/.codex/sessions
// locating rollout-*-<cleanID>.jsonl and <cleanID>.jsonl (walking YYYY/MM/DD subdirectories).
func (m *Manager) codexTranscriptPaths(sessionID string) []string {
	if m == nil {
		return nil
	}
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return nil
	}
	var paths []string
	seen := make(map[string]bool)
	for _, base := range m.codexSessionBaseDirs() {
		paths = append(paths, scanCodexBaseForSession(base, cleanID, seen)...)
	}
	return paths
}

// CodexSessionExistsOnDisk returns true when any candidate Codex rollout or .jsonl file
// for sessionID exists on disk and has a non-zero size.
func (m *Manager) CodexSessionExistsOnDisk(sessionID string) bool {
	if m == nil {
		return false
	}
	paths := m.codexTranscriptPaths(sessionID)
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
	}
	return false
}

func isCodexSessionDir(root string) bool {
	base := filepath.Base(root)
	return base == "sessions" || base == ".codex" || strings.Contains(root, "codex")
}

func checkCodexSubdirsActivity(root, cleanID string, current time.Time) time.Time {
	if !isCodexSessionDir(root) {
		return current
	}
	seen := make(map[string]bool)
	for _, p := range scanCodexBaseForSession(root, cleanID, seen) {
		current = checkFileMtime(p, current)
	}
	return current
}

func isCodexStepLine(line string) bool {
	var entry codexTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.Type != entryTypeResponseItem {
		return false
	}
	var payload codexPayloadRecord
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return false
	}
	if payload.Type == entryTypeMessage {
		return payload.Role == roleAssistant
	}
	return true
}

func countStepsInCodexTranscript(filePath string) int {
	return countMatchingLinesInFile(filePath, "countSteps", isCodexStepLine)
}

func isNonAmbientCodexUserLine(line string) bool {
	var entry codexTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.Type != entryTypeResponseItem {
		return false
	}
	var payload codexPayloadRecord
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return false
	}
	if payload.Type != entryTypeMessage || payload.Role != roleUser {
		return false
	}
	text := extractCodexText(payload.Content, payload.Text)
	return !isAmbientInput("", text)
}

func countTurnsInCodexTranscript(filePath string) int {
	return countMatchingLinesInFile(filePath, "countTurns", isNonAmbientCodexUserLine)
}

func findLastCodexUserInputIdx(lines []string) int {
	lastUserInputIdx := -1
	for i, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line != "" && isNonAmbientCodexUserLine(line) {
			lastUserInputIdx = i
		}
	}
	return lastUserInputIdx
}

func extractErrorBytes(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var errStr string
	if json.Unmarshal(raw, &errStr) == nil && errStr != "" {
		return errStr
	}
	var errObj struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &errObj) == nil && errObj.Message != "" {
		return errObj.Message
	}
	return string(raw)
}

func extractCodexEventMsgError(payload *codexPayloadRecord) string {
	switch payload.Type {
	case entryTypeError:
		if payload.Message != "" {
			return payload.Message
		}
		if str := extractErrorBytes(payload.Error); str != "" {
			return str
		}
		if payload.Text != "" {
			return payload.Text
		}
		return "error event"
	case "turn_aborted":
		if payload.Reason != "" {
			return payload.Reason
		}
		if payload.Message != "" {
			return payload.Message
		}
		return "turn aborted"
	default:
		return ""
	}
}

func isCodexResponseError(payload *codexPayloadRecord) bool {
	return payload.IsError ||
		strings.EqualFold(payload.Status, "error") ||
		strings.EqualFold(payload.Status, "failed") ||
		payload.Type == entryTypeError ||
		len(payload.Error) > 0
}

func extractCodexResponseItemError(payload *codexPayloadRecord) string {
	if !isCodexResponseError(payload) {
		return ""
	}
	if payload.Message != "" {
		return payload.Message
	}
	if str := extractErrorBytes(payload.Error); str != "" {
		return str
	}
	return extractCodexText(payload.Content, payload.Text)
}

func extractCodexErrorFromPayload(entryType string, payload *codexPayloadRecord) string {
	switch entryType {
	case entryTypeEventMsg:
		return extractCodexEventMsgError(payload)
	case entryTypeResponseItem:
		return extractCodexResponseItemError(payload)
	default:
		return ""
	}
}

func extractCodexErrorFromLine(line string, cutoff time.Time) (errMsg string, beforeCutoff bool) {
	var entry codexTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return "", false
	}
	ts := entry.Timestamp
	if ts == "" {
		ts = entry.CreatedAt
	}
	if isStepBefore(ts, cutoff) {
		return "", true
	}

	var payload codexPayloadRecord
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return "", false
	}

	return extractCodexErrorFromPayload(entry.Type, &payload), false
}

func scanCodexLinesForTurnError(ctx context.Context, lines []string, since time.Time) (string, error) {
	return scanTranscriptLinesForTurnError(ctx, lines, since, findLastCodexUserInputIdx, extractCodexErrorFromLine)
}

func isCodexToolPayloadType(payloadType string) bool {
	switch payloadType {
	case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "local_shell_call", "web_search_call", "mcp_call":
		return true
	default:
		return false
	}
}

func parseCodexResponseEntry(raw string, since time.Time) (codexPayloadRecord, bool, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.Contains(raw, `"`+entryTypeResponseItem+`"`) {
		return codexPayloadRecord{}, false, false
	}
	var entry codexTranscriptEntry
	if json.Unmarshal([]byte(raw), &entry) != nil || entry.Type != entryTypeResponseItem {
		return codexPayloadRecord{}, false, false
	}
	ts := entry.Timestamp
	if ts == "" {
		ts = entry.CreatedAt
	}
	if isStepBefore(ts, since) {
		return codexPayloadRecord{}, false, true
	}
	var payload codexPayloadRecord
	if json.Unmarshal(entry.Payload, &payload) != nil {
		return codexPayloadRecord{}, false, false
	}
	return payload, true, false
}

func scanCodexLinesForResponse(ctx context.Context, lines []string, since time.Time) string {
	if ctx != nil && ctx.Err() != nil {
		return ""
	}
	for _, raw := range slices.Backward(lines) {
		payload, ok, expired := parseCodexResponseEntry(raw, since)
		if expired {
			return ""
		}
		if !ok {
			continue
		}
		if payload.Type == entryTypeMessage && payload.Role == roleUser {
			if !isAmbientInput("", extractCodexText(payload.Content, payload.Text)) {
				return ""
			}
		}
		if isCodexToolPayloadType(payload.Type) {
			return ""
		}
		if payload.Type != entryTypeMessage || payload.Role != roleAssistant {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(payload.Phase), "commentary") {
			continue
		}
		text := extractCodexText(payload.Content, payload.Text)
		trimmed := strings.TrimSpace(text)
		if trimmed != "" && !strings.HasPrefix(trimmed, "[Tool Call Requested]:") {
			return trimmed
		}
	}
	return ""
}

func extractResponseFromCodexTranscriptFile(ctx context.Context, tPath string, since time.Time) string {
	data, err := os.ReadFile(tPath)
	if err != nil || len(data) == 0 {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	return scanCodexLinesForResponse(ctx, lines, since)
}
