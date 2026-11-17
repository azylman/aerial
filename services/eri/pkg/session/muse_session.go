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

type museTranscriptEntry struct {
	PayloadType string      `json:"payload_type"`
	RecordedAt  int64       `json:"recorded_at"`
	Payload     musePayload `json:"payload"`
}

type musePayload struct {
	Kind  string    `json:"kind"`
	RunID string    `json:"run_id"`
	Event museEvent `json:"event"`
}

type museEvent struct {
	Kind      string           `json:"kind"`
	Prompt    string           `json:"prompt,omitempty"`
	Text      string           `json:"text,omitempty"`
	Terminal  string           `json:"terminal,omitempty"`
	Reason    string           `json:"reason,omitempty"`
	ToolCalls []museToolCall   `json:"tool_calls,omitempty"`
	Results   []museToolResult `json:"results,omitempty"`
}

type museToolCall struct {
	ID     string          `json:"id,omitempty"`
	CallID string          `json:"call_id,omitempty"`
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args,omitempty"`
}

type museToolResult struct {
	ToolCallID string `json:"tool_call_id,omitempty"`
	CallID     string `json:"call_id,omitempty"`
	Text       string `json:"text,omitempty"`
	Output     string `json:"output,omitempty"`
	IsError    bool   `json:"is_error,omitempty"`
}

func isMuseStepBeforeEntry(entry museTranscriptEntry, since time.Time) bool {
	return !since.IsZero() && entry.RecordedAt > 0 && time.UnixMicro(entry.RecordedAt).UTC().Before(since)
}

func scanMuseBaseForSession(base, cleanID string, seen map[string]bool) []string {
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

	addIfMatch(filepath.Join(resolvedBase, cleanID+".jsonl"))
	addIfMatch(filepath.Join(resolvedBase, cleanID, "session.jsonl"))

	patterns := []string{
		filepath.Join(resolvedBase, "*", "*", "*", cleanID, "session.jsonl"),
		filepath.Join(resolvedBase, "*", "*", "*", cleanID+".jsonl"),
	}
	for _, pattern := range patterns {
		if matches, err := filepath.Glob(pattern); err == nil && len(matches) > 0 {
			for _, m := range matches {
				addIfMatch(m)
			}
		}
	}
	return paths
}

func (m *Manager) museTranscriptPaths(sessionID string) []string {
	if m == nil {
		return nil
	}
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return nil
	}
	var paths []string
	seen := make(map[string]bool)
	for _, base := range m.museSessionBaseDirs() {
		paths = append(paths, scanMuseBaseForSession(base, cleanID, seen)...)
	}
	return paths
}

func (m *Manager) MuseSessionExistsOnDisk(sessionID string) bool {
	if m == nil {
		return false
	}
	paths := m.museTranscriptPaths(sessionID)
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
	}
	return false
}

func isMuseSessionDir(root string) bool {
	base := filepath.Base(root)
	return base == "sessions" || strings.Contains(root, "muse")
}

func checkMuseSubdirsActivity(root, cleanID string, current time.Time) time.Time {
	if !isMuseSessionDir(root) {
		return current
	}
	seen := make(map[string]bool)
	for _, p := range scanMuseBaseForSession(root, cleanID, seen) {
		current = checkFileMtime(p, current)
	}
	return current
}

const (
	musePayloadTypeSession = "runtime.session"
	musePayloadKindRun     = "run"
)

func isMuseStepLine(line string) bool {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.PayloadType != musePayloadTypeSession {
		return false
	}
	if entry.Payload.Kind != musePayloadKindRun {
		return false
	}
	switch entry.Payload.Event.Kind {
	case "assistant_message_committed", "assistant_tool_calls_committed", "tool_result_batch_committed":
		return true
	default:
		return false
	}
}

func countStepsInMuseTranscript(filePath string) int {
	return countMatchingLinesInFile(filePath, "countSteps", isMuseStepLine)
}

func isNonAmbientMuseUserLine(line string) bool {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.PayloadType != musePayloadTypeSession {
		return false
	}
	if entry.Payload.Kind != musePayloadKindRun {
		return false
	}
	if entry.Payload.Event.Kind == "started" {
		prompt := strings.TrimSpace(entry.Payload.Event.Prompt)
		if prompt != "" && !isAmbientInput("", prompt) {
			return true
		}
	}
	return false
}

func countTurnsInMuseTranscript(filePath string) int {
	return countMatchingLinesInFile(filePath, "countTurns", isNonAmbientMuseUserLine)
}

func findLastMuseUserInputIdx(lines []string) int {
	lastUserInputIdx := -1
	for i, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line != "" && isNonAmbientMuseUserLine(line) {
			lastUserInputIdx = i
		}
	}
	return lastUserInputIdx
}

func extractMuseErrorFromLine(line string, cutoff time.Time) (string, bool) {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return "", false
	}
	if isMuseStepBeforeEntry(entry, cutoff) {
		return "", true
	}
	if entry.PayloadType != musePayloadTypeSession || entry.Payload.Kind != musePayloadKindRun {
		return "", false
	}
	if entry.Payload.Event.Kind == "terminal" && entry.Payload.Event.Terminal == "failed" {
		reason := entry.Payload.Event.Reason
		if reason == "" {
			reason = "terminal failed"
		}
		return reason, false
	}
	return "", false
}

func scanMuseLinesForTurnError(ctx context.Context, lines []string, since time.Time) (string, error) {
	return scanTranscriptLinesForTurnError(ctx, lines, since, findLastMuseUserInputIdx, extractMuseErrorFromLine)
}

func parseMuseResponseLine(raw string, since time.Time) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil || entry.PayloadType != musePayloadTypeSession {
		return "", false
	}
	if entry.Payload.Kind != musePayloadKindRun {
		return "", false
	}
	if isMuseStepBeforeEntry(entry, since) {
		return "", true
	}
	switch entry.Payload.Event.Kind {
	case "started":
		prompt := strings.TrimSpace(entry.Payload.Event.Prompt)
		if prompt != "" && !isAmbientInput("", prompt) {
			return "", true
		}
	case "assistant_tool_calls_committed", "tool_result_batch_committed":
		return "", true
	case "assistant_message_committed":
		if text := strings.TrimSpace(entry.Payload.Event.Text); text != "" {
			return text, true
		}
	}
	return "", false
}

func scanMuseLinesForResponse(ctx context.Context, lines []string, since time.Time) string {
	if ctx != nil && ctx.Err() != nil {
		return ""
	}
	for _, raw := range slices.Backward(lines) {
		resp, stop := parseMuseResponseLine(raw, since)
		if resp != "" {
			return resp
		}
		if stop {
			return ""
		}
	}
	return ""
}

func extractResponseFromMuseTranscriptFile(ctx context.Context, tPath string, since time.Time) string {
	data, err := os.ReadFile(tPath)
	if err != nil || len(data) == 0 {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	return scanMuseLinesForResponse(ctx, lines, since)
}
