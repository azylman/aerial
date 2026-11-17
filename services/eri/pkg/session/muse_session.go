package session

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type museTranscriptEntry struct {
	PayloadType string          `json:"payload_type"`
	RecordedAt  any             `json:"recorded_at"`
	Timestamp   string          `json:"timestamp,omitempty"`
	CreatedAt   string          `json:"created_at,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

type musePayload struct {
	museEvent

	Event   *museEvent `json:"event,omitempty"`
	Record  *museEvent `json:"record,omitempty"`
	Type    string     `json:"type,omitempty"`
	Role    string     `json:"role,omitempty"`
	Message string     `json:"message,omitempty"`
}

func (p *musePayload) event() *museEvent {
	if p.Event != nil {
		return p.Event
	}
	if p.Record != nil {
		return p.Record
	}
	if p.Kind != "" || p.Prompt != "" || p.Terminal != "" || p.Reason != "" || extractErrorBytes(p.Error) != "" {
		return &p.museEvent
	}
	return nil
}

type museEvent struct {
	Kind      string          `json:"kind,omitempty"`
	Prompt    string          `json:"prompt,omitempty"`
	Text      string          `json:"text,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	Terminal  string          `json:"terminal,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
	Status    string          `json:"status,omitempty"`
	Type      string          `json:"type,omitempty"`
	Role      string          `json:"role,omitempty"`
	Tool      string          `json:"tool,omitempty"`
	Name      string          `json:"name,omitempty"`
	ID        string          `json:"id,omitempty"`
	CallID    string          `json:"call_id,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}

func extractMuseText(content json.RawMessage, textFallback string) string {
	return extractCodexText(content, textFallback)
}

func parseMuseTimestamp(raw any) time.Time {
	if raw == nil {
		return time.Time{}
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			return time.Time{}
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UTC()
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parseMuseNumericTimestamp(n)
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return parseMuseFloatTimestamp(f)
		}
	case float64:
		return parseMuseFloatTimestamp(v)
	}
	return time.Time{}
}

func parseMuseFloatTimestamp(f float64) time.Time {
	if f <= 0 {
		return time.Time{}
	}
	switch {
	case f > 1e17:
		return time.Unix(0, int64(f)).UTC()
	case f > 1e12:
		sec := int64(f / 1e6)
		nsec := int64(math.Round((f/1e6 - float64(sec)) * 1e9))
		if nsec >= 1e9 {
			sec++
			nsec -= 1e9
		}
		return time.Unix(sec, nsec).UTC()
	default:
		sec := int64(f)
		nsec := int64(math.Round((f - float64(sec)) * 1e9))
		if nsec >= 1e9 {
			sec++
			nsec -= 1e9
		}
		return time.Unix(sec, nsec).UTC()
	}
}

func parseMuseNumericTimestamp(n int64) time.Time {
	if n <= 0 {
		return time.Time{}
	}
	if n > 1e17 {
		return time.Unix(0, n).UTC()
	}
	if n > 1e12 {
		return time.UnixMicro(n).UTC()
	}
	return time.Unix(n, 0).UTC()
}

func extractMuseTimestamp(entry museTranscriptEntry) time.Time {
	if entry.RecordedAt != nil {
		if t := parseMuseTimestamp(entry.RecordedAt); !t.IsZero() {
			return t
		}
	}
	if entry.Timestamp != "" {
		if t := parseMuseTimestamp(entry.Timestamp); !t.IsZero() {
			return t
		}
	}
	if entry.CreatedAt != "" {
		if t := parseMuseTimestamp(entry.CreatedAt); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func isMuseStepBeforeEntry(entry museTranscriptEntry, since time.Time) bool {
	if since.IsZero() {
		return false
	}
	t := extractMuseTimestamp(entry)
	if t.IsZero() {
		return false
	}
	return t.Before(since)
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
	museKindToolCompleted  = "tool_completed"
)

func isMuseStepLine(line string) bool {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.PayloadType != musePayloadTypeSession {
		return false
	}
	var payload musePayload
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return false
	}
	ev := payload.event()
	if ev != nil {
		switch ev.Kind {
		case "assistant_message_committed", "tool_call", museKindToolCompleted, "model_completed":
			return true
		}
	}
	if payload.Type == entryTypeMessage && payload.Role == roleAssistant {
		return true
	}
	return false
}

func countStepsInMuseTranscript(filePath string) int {
	return countMatchingLinesInFile(filePath, "countSteps", isMuseStepLine)
}

func isNonAmbientMuseUserLine(line string) bool {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.PayloadType != musePayloadTypeSession {
		return false
	}
	var payload musePayload
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return false
	}
	ev := payload.event()
	if ev != nil && ev.Kind == "started" && strings.TrimSpace(ev.Prompt) != "" {
		return !isAmbientInput("", ev.Prompt)
	}
	if payload.Type == entryTypeMessage && payload.Role == roleUser {
		text := extractMuseText(payload.Content, payload.Text)
		if strings.TrimSpace(text) != "" {
			return !isAmbientInput("", text)
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

func extractMuseTerminalError(ev *museEvent) string {
	errStr := extractErrorBytes(ev.Error)
	if ev.Terminal != "failed" && ev.Reason == "" && errStr == "" {
		return ""
	}
	if ev.Reason != "" {
		return ev.Reason
	}
	if errStr != "" {
		return errStr
	}
	if ev.Text != "" {
		return ev.Text
	}
	if ev.Terminal == "failed" {
		return "terminal failed"
	}
	return ""
}

func extractMuseToolCompletedError(ev *museEvent) string {
	if !ev.IsError && !strings.EqualFold(ev.Status, "error") && !strings.EqualFold(ev.Status, "failed") {
		return ""
	}
	if ev.Reason != "" {
		return ev.Reason
	}
	if str := extractErrorBytes(ev.Error); str != "" {
		return str
	}
	outputStr := extractCodexText(ev.Output, ev.Text)
	if outputStr == "" && len(ev.Result) > 0 {
		outputStr = extractCodexText(ev.Result, "")
	}
	if outputStr == "" && len(ev.Content) > 0 {
		outputStr = extractCodexText(ev.Content, "")
	}
	if outputStr != "" {
		return outputStr
	}
	return "tool failed"
}

func extractMuseEventError(ev *museEvent) string {
	if ev == nil {
		return ""
	}
	if ev.Kind == "terminal" {
		return extractMuseTerminalError(ev)
	}
	if ev.Kind == museKindToolCompleted {
		return extractMuseToolCompletedError(ev)
	}
	if ev.Reason != "" {
		return ev.Reason
	}
	return extractErrorBytes(ev.Error)
}

func extractMuseErrorFromLine(line string, cutoff time.Time) (errMsg string, beforeCutoff bool) {
	var entry museTranscriptEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return "", false
	}
	if isMuseStepBeforeEntry(entry, cutoff) {
		return "", true
	}
	if entry.PayloadType != musePayloadTypeSession {
		return "", false
	}
	var payload musePayload
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		return "", false
	}
	if errStr := extractMuseEventError(payload.event()); errStr != "" {
		return errStr, false
	}
	if payload.Reason != "" {
		return payload.Reason, false
	}
	if str := extractErrorBytes(payload.Error); str != "" {
		return str, false
	}
	return "", false
}

func scanMuseLinesForTurnError(ctx context.Context, lines []string, since time.Time) (string, error) {
	return scanTranscriptLinesForTurnError(ctx, lines, since, findLastMuseUserInputIdx, extractMuseErrorFromLine)
}

func checkMuseNonAmbientUser(ev *museEvent, payload musePayload) bool {
	if ev != nil && ev.Kind == "started" && strings.TrimSpace(ev.Prompt) != "" {
		return !isAmbientInput("", ev.Prompt)
	}
	if payload.Type == entryTypeMessage && payload.Role == roleUser {
		text := extractMuseText(payload.Content, payload.Text)
		return !isAmbientInput("", text)
	}
	return false
}

func extractMuseCommittedAssistantText(ev *museEvent, payload musePayload) string {
	if ev != nil && (ev.Kind == "assistant_message_committed" || ev.Kind == "model_completed") {
		text := strings.TrimSpace(extractMuseText(ev.Content, ev.Text))
		if text != "" && !strings.HasPrefix(text, "[Tool Call Requested]:") {
			return text
		}
	}
	if payload.Type == entryTypeMessage && payload.Role == roleAssistant {
		text := strings.TrimSpace(extractMuseText(payload.Content, payload.Text))
		if text != "" && !strings.HasPrefix(text, "[Tool Call Requested]:") {
			return text
		}
	}
	return ""
}

func parseMuseResponseLine(raw string, since time.Time) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	var entry museTranscriptEntry
	if json.Unmarshal([]byte(raw), &entry) != nil || entry.PayloadType != musePayloadTypeSession {
		return "", false
	}
	if isMuseStepBeforeEntry(entry, since) {
		return "", true
	}
	var payload musePayload
	if json.Unmarshal(entry.Payload, &payload) != nil {
		return "", false
	}
	ev := payload.event()
	if checkMuseNonAmbientUser(ev, payload) {
		return "", true
	}
	if ev != nil && (ev.Kind == "tool_call" || ev.Kind == "tool_completed") {
		return "", true
	}
	if text := extractMuseCommittedAssistantText(ev, payload); text != "" {
		return text, true
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
