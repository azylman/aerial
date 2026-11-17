package transcript

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
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
	text := strings.TrimSpace(ev.Text)
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

func (st *transcriptScanState) handleMuseToolCallsCommitted(ev *museEvent, ts string) {
	if len(ev.ToolCalls) == 0 {
		return
	}
	var toolCalls []rawToolCall
	var primaryToolName string
	for _, tc := range ev.ToolCalls {
		callID := tc.CallID
		if callID == "" {
			callID = tc.ID
		}
		toolName := tc.Name
		if primaryToolName == "" {
			primaryToolName = toolName
		}
		toolCalls = append(toolCalls, rawToolCall{
			Name: toolName,
			Args: tc.Args,
			ID:   callID,
		})
		if callID != "" {
			st.registerClaudeToolUse(claudeContentBlock{ID: callID, Name: toolName})
		}
	}
	st.recordStep(rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    sourceModel,
		Type:      stepTypePlannerResponse,
		Status:    stepStatusDone,
		CreatedAt: ts,
		ToolName:  primaryToolName,
		ToolCalls: toolCalls,
	})
}

func (st *transcriptScanState) handleMuseToolResultBatch(ev *museEvent, ts string) {
	for _, res := range ev.Results {
		callID := res.CallID
		if callID == "" {
			callID = res.ToolCallID
		}
		status := stepStatusDone
		if res.IsError {
			status = stepStatusError
		}
		var toolName string
		if callID != "" && st.claudeToolNames != nil {
			toolName = st.claudeToolNames[callID]
		}
		outputStr := res.Output
		if outputStr == "" {
			outputStr = res.Text
		}
		if status == stepStatusError && outputStr == "" {
			outputStr = "tool failed"
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
}

func (st *transcriptScanState) handleMuseTerminal(ev *museEvent, ts string) {
	errMsg := ev.Reason
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

func (st *transcriptScanState) parseMuseEntry(env museLineEnvelope) bool {
	if env.PayloadType != "runtime.session" {
		return false
	}
	if env.Payload.Kind != "run" {
		return true
	}
	var ts string
	if env.RecordedAt > 0 {
		ts = time.UnixMicro(env.RecordedAt).UTC().Format(time.RFC3339Nano)
	}
	ev := env.Payload.Event
	switch ev.Kind {
	case "started":
		st.handleMuseStartedEvent(&ev, ts)
	case "assistant_message_committed":
		st.handleMuseAssistantCommitted(&ev, ts)
	case "assistant_tool_calls_committed":
		st.handleMuseToolCallsCommitted(&ev, ts)
	case "tool_result_batch_committed":
		st.handleMuseToolResultBatch(&ev, ts)
	case "terminal":
		if ev.Terminal == "failed" {
			st.handleMuseTerminal(&ev, ts)
		}
	}
	return true
}
