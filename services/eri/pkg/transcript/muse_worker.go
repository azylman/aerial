package transcript

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// discoverMuseSessions scans Muse session base directories for session files.
func discoverMuseSessions(museBases []string, discovered map[string]discoveredSession) {
	seenMuseBase := make(map[string]bool)
	for _, base := range museBases {
		if seenMuseBase[base] {
			continue
		}
		seenMuseBase[base] = true
		fi, err := os.Stat(base)
		if err != nil || !fi.IsDir() {
			continue
		}
		if err := filepath.WalkDir(base, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d == nil || d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".jsonl") {
				return nil
			}
			var sessionID string
			if d.Name() == "session.jsonl" {
				sessionID = filepath.Base(filepath.Dir(path))
			} else {
				sessionID = strings.TrimSuffix(d.Name(), ".jsonl")
			}
			if sessionID == "" || strings.HasPrefix(sessionID, ".") {
				return nil
			}
			if _, exists := discovered[sessionID]; !exists {
				discovered[sessionID] = discoveredSession{path: path, engine: EngineMuse}
			}
			return nil
		}); err != nil {
			log.Printf("[Transcript] WalkDir %s: %v", base, err)
		}
	}
}

func findMuseTranscriptFile(brainDir, sessionID string, extraDirs ...string) string {
	var bases []string
	for _, d := range extraDirs {
		if trimmed := strings.TrimSpace(d); trimmed != "" {
			bases = append(bases, filepath.Clean(trimmed))
		}
	}
	bases = append(bases,
		filepath.Clean(filepath.Join(brainDir, "..", "muse", "sessions")),
		filepath.Clean(filepath.Join(brainDir, "..", "..", "..", "muse", "sessions")),
		filepath.Clean(filepath.Join(brainDir, "..", ".local", "share", "muse", "sessions")),
		filepath.Clean(filepath.Join(brainDir, "..", "..", "..", ".local", "share", "muse", "sessions")),
	)
	runtimesBase := filepath.Clean(filepath.Join(brainDir, "..", "runtimes"))
	if entries, err := os.ReadDir(runtimesBase); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				bases = append(bases, filepath.Join(runtimesBase, e.Name(), ".local", "share", "muse", "sessions"))
			}
		}
	}

	for _, base := range bases {
		if fi, err := os.Stat(base); err != nil || !fi.IsDir() {
			continue
		}
		for _, cand := range []string{
			filepath.Join(base, sessionID+".jsonl"),
			filepath.Join(base, sessionID, "session.jsonl"),
		} {
			if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
				return cand
			}
		}
		for _, pat := range []string{
			filepath.Join(base, "*", "*", "*", sessionID, "session.jsonl"),
			filepath.Join(base, "*", "*", "*", sessionID+".jsonl"),
		} {
			if matches, err := filepath.Glob(pat); err == nil && len(matches) > 0 {
				return matches[0]
			}
		}
	}
	return ""
}

func isMuseTranscriptPath(cleanPath, slashPath string, extraDirs []string) bool {
	for _, d := range extraDirs {
		if trimmed := strings.TrimSpace(d); trimmed != "" {
			cleanDir := filepath.Clean(trimmed)
			if cleanPath == cleanDir || strings.HasPrefix(cleanPath, cleanDir+string(filepath.Separator)) {
				return true
			}
		}
	}
	return strings.Contains(slashPath, "muse/sessions") ||
		strings.Contains(slashPath, ".local/share/muse/sessions") ||
		strings.HasSuffix(slashPath, "/session.jsonl")
}

type museLineEnvelope struct {
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
	if p.Kind != "" || p.Prompt != "" || p.Terminal != "" || p.Reason != "" || extractMuseErrorBytes(p.Error) != "" {
		return &p.museEvent
	}
	return nil
}

type museEvent struct {
	Kind      string          `json:"kind,omitempty"`
	Prompt    string          `json:"prompt,omitempty"`
	Text      string          `json:"text,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	Name      string          `json:"name,omitempty"`
	Tool      string          `json:"tool,omitempty"`
	ID        string          `json:"id,omitempty"`
	CallID    string          `json:"call_id,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Args      json.RawMessage `json:"args,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Status    string          `json:"status,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Terminal  string          `json:"terminal,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
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
	if f > 1e17 {
		return time.Unix(0, int64(f)).UTC()
	}
	divisor := 1.0
	if f > 1e12 {
		divisor = 1e6
	}
	scaled := f / divisor
	sec := int64(scaled)
	nsec := int64(math.Round((scaled - float64(sec)) * 1e9))
	if nsec >= 1e9 {
		sec++
		nsec -= 1e9
	}
	return time.Unix(sec, nsec).UTC()
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

func extractMuseTimestamp(env museLineEnvelope) time.Time {
	if env.RecordedAt != nil {
		if t := parseMuseTimestamp(env.RecordedAt); !t.IsZero() {
			return t
		}
	}
	if env.Timestamp != "" {
		if t := parseMuseTimestamp(env.Timestamp); !t.IsZero() {
			return t
		}
	}
	if env.CreatedAt != "" {
		if t := parseMuseTimestamp(env.CreatedAt); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func extractMuseText(content json.RawMessage, textFallback string) string {
	return extractCodexText(content, textFallback)
}

func extractMuseErrorBytes(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
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

func (st *transcriptScanState) handleMuseStartedEvent(ev *museEvent, ts string) {
	prompt := strings.TrimSpace(ev.Prompt)
	if prompt == "" || strings.HasPrefix(prompt, "[Chat ") {
		return
	}
	st.recordStep(rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    stepSourceUserExplicit,
		Type:      stepTypeUserInput,
		Status:    stepStatusDone,
		CreatedAt: ts,
		Content:   prompt,
	})
}

func (st *transcriptScanState) handleMuseAssistantCommitted(ev *museEvent, ts string) {
	text := extractMuseText(ev.Content, ev.Text)
	if text == "" {
		return
	}
	st.recordStep(rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    sourceModel,
		Type:      stepTypePlannerResponse,
		Status:    stepStatusDone,
		CreatedAt: ts,
		Content:   text,
	})
}

func (st *transcriptScanState) handleMuseToolCall(ev *museEvent, ts string) {
	toolName := ev.Name
	if toolName == "" {
		toolName = ev.Tool
	}
	callID := ev.CallID
	if callID == "" {
		callID = ev.ID
	}
	args := ev.Arguments
	if len(args) == 0 {
		args = ev.Args
	}
	if len(args) == 0 {
		args = ev.Input
	}
	var strArgs string
	if json.Unmarshal(args, &strArgs) == nil {
		args = json.RawMessage(strArgs)
	}
	st.recordStep(rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    sourceModel,
		Type:      stepTypePlannerResponse,
		Status:    stepStatusDone,
		CreatedAt: ts,
		ToolName:  toolName,
		ToolCalls: []rawToolCall{{Name: toolName, Args: args, ID: callID}},
	})
	if callID != "" {
		st.registerClaudeToolUse(claudeContentBlock{ID: callID, Name: toolName})
	}
}

func (st *transcriptScanState) handleMuseToolCompleted(ev *museEvent, ts string) {
	callID := ev.CallID
	if callID == "" {
		callID = ev.ID
	}
	status := stepStatusDone
	if ev.IsError || strings.EqualFold(ev.Status, "error") || strings.EqualFold(ev.Status, "failed") {
		status = stepStatusError
	}
	toolName := ev.Name
	if toolName == "" {
		toolName = ev.Tool
	}
	if toolName == "" && callID != "" && st.claudeToolNames != nil {
		toolName = st.claudeToolNames[callID]
	}
	outputStr := extractOutputText(ev.Output, ev.Text)
	if outputStr == "" && len(ev.Result) > 0 {
		outputStr = extractOutputText(ev.Result, "")
	}
	if outputStr == "" && len(ev.Content) > 0 {
		outputStr = extractOutputText(ev.Content, "")
	}
	if status == stepStatusError && outputStr == "" {
		outputStr = ev.Reason
		if outputStr == "" {
			outputStr = extractMuseErrorBytes(ev.Error)
		}
		if outputStr == "" {
			outputStr = "tool failed"
		}
	}
	st.recordStep(rawTranscriptStep{
		StepIndex:  st.maxStepIndex + 1,
		Source:     stepTypeTool,
		Type:       stepTypeTool,
		Status:     status,
		CreatedAt:  ts,
		Content:    outputStr,
		ToolName:   toolName,
		ToolCallID: callID,
	})
}

func (st *transcriptScanState) handleMuseTerminal(ev *museEvent, ts string) {
	errStr := extractMuseErrorBytes(ev.Error)
	if ev.Terminal != "failed" && ev.Reason == "" && errStr == "" {
		return
	}
	errMsg := ev.Reason
	if errMsg == "" {
		errMsg = errStr
	}
	if errMsg == "" {
		errMsg = ev.Text
	}
	if errMsg == "" {
		errMsg = "terminal failed"
	}
	st.recordStep(rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    sourceModel,
		Type:      stepTypePlannerResponse,
		Status:    stepStatusError,
		CreatedAt: ts,
		Content:   errMsg,
	})
}

func (st *transcriptScanState) handleMuseEvent(ev *museEvent, ts string) bool {
	switch ev.Kind {
	case "started":
		st.handleMuseStartedEvent(ev, ts)
	case "assistant_message_committed", "model_completed":
		st.handleMuseAssistantCommitted(ev, ts)
	case "tool_call":
		st.handleMuseToolCall(ev, ts)
	case "tool_completed":
		st.handleMuseToolCompleted(ev, ts)
	case "terminal":
		st.handleMuseTerminal(ev, ts)
	}
	return true
}

func (st *transcriptScanState) handleMuseMessage(payload musePayload, ts string) bool {
	text := extractMuseText(payload.Content, payload.Text)
	switch payload.Role {
	case roleUser:
		if text != "" && !strings.HasPrefix(strings.TrimSpace(text), "[Chat ") {
			st.recordStep(rawTranscriptStep{
				StepIndex: st.maxStepIndex + 1,
				Source:    stepSourceUserExplicit,
				Type:      stepTypeUserInput,
				Status:    stepStatusDone,
				CreatedAt: ts,
				Content:   text,
			})
		}
	case roleAssistant:
		if text != "" {
			st.recordStep(rawTranscriptStep{
				StepIndex: st.maxStepIndex + 1,
				Source:    sourceModel,
				Type:      stepTypePlannerResponse,
				Status:    stepStatusDone,
				CreatedAt: ts,
				Content:   text,
			})
		}
	}
	return true
}

func (st *transcriptScanState) parseMuseEntry(env museLineEnvelope) bool {
	if env.PayloadType != "runtime.session" && !strings.HasPrefix(env.PayloadType, "runtime.") {
		return false
	}
	if len(env.Payload) == 0 {
		return false
	}
	var payload musePayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return false
	}

	t := extractMuseTimestamp(env)
	ts := ""
	if !t.IsZero() {
		ts = t.UTC().Format(time.RFC3339Nano)
	}

	if ev := payload.event(); ev != nil {
		return st.handleMuseEvent(ev, ts)
	}
	if payload.Type == "message" {
		return st.handleMuseMessage(payload, ts)
	}
	return true
}
