package transcript

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Step vocabulary for synthesized transcript steps.
const (
	stepTypeUserInput       = "USER_INPUT"
	stepTypeTool            = "TOOL"
	stepTypePlannerResponse = "PLANNER_RESPONSE"
	stepSourceUserExplicit  = "USER_EXPLICIT"
	stepStatusDone          = "DONE"
	stepStatusError         = "ERROR"
	sourceModel             = "MODEL"
	roleUser                = "user"
	roleAssistant           = "assistant"
)

type rawTranscriptStep struct {
	StepIndex int           `json:"step_index"`
	Source    string        `json:"source"`
	Type      string        `json:"type"`
	Status    string        `json:"status"`
	CreatedAt string        `json:"created_at"`
	Content   string        `json:"content"`
	ToolName  string        `json:"tool_name"`
	ToolCalls []rawToolCall `json:"tool_calls"`
	Thinking  string        `json:"thinking"`
	// ToolCallID pairs a Claude tool_result step with its originating tool_use block.
	// It is never decoded from agy lines so agy pending-name association is unchanged.
	ToolCallID string `json:"-"`
}

type rawToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
	// ID is the Claude tool_use block id; never decoded from agy lines.
	ID string `json:"-"`
}

// discoverSessions maps every session ID found under the brain dir (agy), Claude project
// dirs (Claude Code), Codex sessions dirs (Codex), and Muse sessions dirs (Muse) to its transcript path and engine.
// Brain-dir entries win on ID collisions.
func discoverSessions(targetBrainDir string, entries []os.DirEntry, claudeProjectsDir string, dirs ...string) map[string]discoveredSession {
	var codexSessionsDir, museSessionsDir string
	if len(dirs) > 0 {
		codexSessionsDir = dirs[0]
	}
	if len(dirs) > 1 {
		museSessionsDir = dirs[1]
	}
	discovered := make(map[string]discoveredSession)
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		sessionID := entry.Name()
		if transcriptPath := findTranscriptFile(targetBrainDir, sessionID, museSessionsDir); transcriptPath != "" {
			discovered[sessionID] = discoveredSession{path: transcriptPath, engine: engineForPath(targetBrainDir, transcriptPath, museSessionsDir)}
		}
	}

	// Also discover Claude sessions from Claude project directories
	var claudeBases []string
	if claudeProjectsDir != "" {
		claudeBases = append(claudeBases, claudeProjectsDir)
	}
	claudeBases = append(claudeBases, filepath.Clean(filepath.Join(targetBrainDir, "..", "claude", "projects")), filepath.Clean(filepath.Join(targetBrainDir, "..", "..", "..", "claude", "projects")), filepath.Clean(filepath.Join(targetBrainDir, "..", ".claude", "projects")))
	discoverClaudeProjectSessions(claudeBases, discovered)

	// Also discover Codex sessions from Codex sessions directories
	var codexBases []string
	if codexSessionsDir != "" {
		codexBases = append(codexBases, codexSessionsDir)
	}
	codexBases = append(codexBases, filepath.Clean(filepath.Join(targetBrainDir, "..", "codex", "sessions")), filepath.Clean(filepath.Join(targetBrainDir, "..", "..", "..", "codex", "sessions")), filepath.Clean(filepath.Join(targetBrainDir, "..", ".codex", "sessions")))
	discoverCodexSessions(codexBases, discovered)

	// Also discover Muse sessions from Muse sessions directories
	discoverMuseSessions(museSessionBases(targetBrainDir, museSessionsDir), discovered)

	return discovered
}

// discoverCodexSessions scans Codex session base directories for session files.
func discoverCodexSessions(codexBases []string, discovered map[string]discoveredSession) {
	seenCodexBase := make(map[string]bool)
	for _, base := range codexBases {
		if seenCodexBase[base] {
			continue
		}
		seenCodexBase[base] = true
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
			name := strings.TrimSuffix(d.Name(), ".jsonl")
			if name == "" || strings.HasPrefix(name, ".") {
				return nil
			}
			sessionID := extractCodexSessionID(path, name)
			if sessionID == "" || strings.HasPrefix(sessionID, ".") {
				return nil
			}
			if _, exists := discovered[sessionID]; !exists {
				discovered[sessionID] = discoveredSession{path: path, engine: EngineCodex}
			}
			return nil
		}); err != nil {
			log.Printf("[Transcript] WalkDir %s: %v", base, err)
		}
	}
}

func scanCodexMetaSessionID(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Transcript] Error closing file %s: %v", path, closeErr)
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var meta struct {
			Type    string `json:"type"`
			Payload struct {
				ID        string `json:"id"`
				SessionID string `json:"session_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal([]byte(line), &meta); err == nil && meta.Type == "session_meta" {
			if meta.Payload.ID != "" {
				return meta.Payload.ID
			}
			if meta.Payload.SessionID != "" {
				return meta.Payload.SessionID
			}
		}
		break
	}
	return ""
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range 36 {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !isHexDigit(c) {
			return false
		}
	}
	return true
}

func extractCodexSessionID(path, name string) string {
	if id := scanCodexMetaSessionID(path); id != "" {
		return id
	}
	if rest, ok := strings.CutPrefix(name, "rollout-"); ok {
		if testRest, ok := strings.CutPrefix(rest, "test-"); ok {
			return testRest
		}
		if len(rest) >= 36 {
			cand := rest[len(rest)-36:]
			if isUUID(cand) && (len(rest) == 36 || rest[len(rest)-37] == '-') {
				return cand
			}
		}
		return rest
	}
	return name
}

// discoveredSession locates a session transcript on disk and the engine that wrote it.
type discoveredSession struct {
	path   string
	engine string
}

// engineForPath attributes transcripts to muse, codex, agy, or claude.
func engineForPath(brainDir, transcriptPath string, extraDirs ...string) string {
	cleanPath := filepath.Clean(transcriptPath)
	slashPath := filepath.ToSlash(cleanPath)
	baseName := filepath.Base(cleanPath)
	if isMuseTranscriptPath(cleanPath, slashPath, extraDirs) {
		return EngineMuse
	}
	if strings.Contains(slashPath, "codex/sessions") ||
		strings.Contains(slashPath, ".codex/sessions") ||
		strings.HasPrefix(baseName, "rollout-") {
		return EngineCodex
	}
	if strings.HasPrefix(cleanPath, filepath.Clean(brainDir)+string(filepath.Separator)) {
		return EngineAgy
	}
	return EngineClaude
}

func scanClaudeProjDir(projDir string, discovered map[string]discoveredSession) {
	files, err := os.ReadDir(projDir)
	if err != nil {
		return
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".jsonl") {
			continue
		}
		sessionID := strings.TrimSuffix(f.Name(), ".jsonl")
		if sessionID == "" || strings.HasPrefix(sessionID, ".") {
			continue
		}
		if _, exists := discovered[sessionID]; !exists {
			discovered[sessionID] = discoveredSession{path: filepath.Join(projDir, f.Name()), engine: EngineClaude}
		}
	}
}

func discoverClaudeProjectSessions(claudeBases []string, discovered map[string]discoveredSession) {
	seenClaudeBase := make(map[string]bool)
	for _, base := range claudeBases {
		if seenClaudeBase[base] {
			continue
		}
		seenClaudeBase[base] = true
		projEntries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, proj := range projEntries {
			if !proj.IsDir() {
				continue
			}
			projDir := filepath.Join(base, proj.Name())
			scanClaudeProjDir(projDir, discovered)
		}
	}
}

func findClaudeTranscriptFile(brainDir, sessionID string) string {
	for _, claudeBase := range []string{filepath.Clean(filepath.Join(brainDir, "..", "claude", "projects")), filepath.Clean(filepath.Join(brainDir, "..", "..", "..", "claude", "projects")), filepath.Clean(filepath.Join(brainDir, "..", ".claude", "projects"))} {
		if entries, err := os.ReadDir(claudeBase); err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					cand := filepath.Join(claudeBase, entry.Name(), sessionID+".jsonl")
					if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
						return cand
					}
				}
			}
		}
	}
	return ""
}

func findCodexTranscriptFile(brainDir, sessionID string) string {
	for _, codexBase := range []string{filepath.Clean(filepath.Join(brainDir, "..", "codex", "sessions")), filepath.Clean(filepath.Join(brainDir, "..", "..", "..", "codex", "sessions")), filepath.Clean(filepath.Join(brainDir, "..", ".codex", "sessions"))} {
		if fi, err := os.Stat(codexBase); err != nil || !fi.IsDir() {
			continue
		}
		for _, cand := range []string{
			filepath.Join(codexBase, sessionID+".jsonl"),
			filepath.Join(codexBase, "rollout-"+sessionID+".jsonl"),
		} {
			if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
				return cand
			}
		}
		if matches, err := filepath.Glob(filepath.Join(codexBase, "rollout-*-"+sessionID+".jsonl")); err == nil && len(matches) > 0 {
			return matches[0]
		}
		for _, pat := range []string{
			filepath.Join(codexBase, "*", "*", "*", sessionID+".jsonl"),
			filepath.Join(codexBase, "*", "*", "*", "rollout-"+sessionID+".jsonl"),
			filepath.Join(codexBase, "*", "*", "*", "rollout-*-"+sessionID+".jsonl"),
		} {
			if matches, err := filepath.Glob(pat); err == nil && len(matches) > 0 {
				return matches[0]
			}
		}
	}
	return ""
}

func findTranscriptFile(brainDir, sessionID string, extraDirs ...string) string {
	candidates := []string{
		filepath.Join(brainDir, sessionID, ".system_generated", "logs", "transcript.jsonl"),
		filepath.Join(brainDir, sessionID, "system_generated", "logs", "transcript.jsonl"),
		filepath.Join(brainDir, sessionID, "transcript.jsonl"),
		filepath.Join(brainDir, sessionID+".jsonl"),
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c
		}
	}
	if cand := findClaudeTranscriptFile(brainDir, sessionID); cand != "" {
		return cand
	}
	if cand := findCodexTranscriptFile(brainDir, sessionID); cand != "" {
		return cand
	}
	return findMuseTranscriptFile(brainDir, sessionID, extraDirs...)
}

func FindTranscriptFile(brainDir, sessionID string, extraDirs ...string) string {
	return findTranscriptFile(brainDir, sessionID, extraDirs...)
}

type transcriptScanState struct {
	parsedSteps      []rawTranscriptStep
	firstParsedStep  int
	maxStepIndex     int
	step0Prompt      string
	firstStepTime    time.Time
	dialogueLines    []string
	pendingToolNames []string
	// claudeToolNames maps Claude tool_use ids to tool names so tool_result blocks resolve by id.
	claudeToolNames map[string]string
}

func parseTranscriptTimestamp(rawTime string, fallback time.Time) time.Time {
	if rawTime == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339Nano, rawTime); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, rawTime); err == nil {
		return t.UTC()
	}
	return fallback
}

// isIDPairedStep reports whether a step carries Claude tool ids, in which case tool names are
// resolved by id at parse time and the positional pending-name queue must not be touched.
func isIDPairedStep(raw rawTranscriptStep) bool {
	return raw.ToolCallID != "" || (len(raw.ToolCalls) > 0 && raw.ToolCalls[0].ID != "")
}

func resolveStepToolAssociation(raw rawTranscriptStep, pendingToolNames []string) (effectiveToolName string, nextPending []string) {
	effectiveToolName = raw.ToolName
	if effectiveToolName == "" && len(raw.ToolCalls) > 0 {
		effectiveToolName = raw.ToolCalls[0].Name
	}
	if isIDPairedStep(raw) {
		return effectiveToolName, pendingToolNames
	}
	if raw.Type == "GENERIC" || raw.Type == stepTypeTool || raw.Type == "RUN_COMMAND" {
		if len(pendingToolNames) > 0 {
			if effectiveToolName == "" {
				effectiveToolName = pendingToolNames[0]
			}
			nextPending = pendingToolNames[1:]
		}
		return effectiveToolName, nextPending
	}
	if raw.Type == stepTypePlannerResponse {
		if len(raw.ToolCalls) > 0 {
			nextPending = make([]string, 0, len(raw.ToolCalls))
			for _, tc := range raw.ToolCalls {
				nextPending = append(nextPending, tc.Name)
			}
			return effectiveToolName, nextPending
		}
		return effectiveToolName, pendingToolNames
	}
	return effectiveToolName, nil
}

func isCodexSystemPreamble(content string) bool {
	trimmed := strings.TrimSpace(content)
	return strings.HasPrefix(trimmed, "# AGENTS.md instructions") || strings.HasPrefix(trimmed, "<environment_context>")
}

func (st *transcriptScanState) recordStep(raw rawTranscriptStep) {
	if st.firstParsedStep == -1 {
		st.firstParsedStep = raw.StepIndex
	}
	if raw.StepIndex > st.maxStepIndex {
		st.maxStepIndex = raw.StepIndex
	}

	if st.firstStepTime.IsZero() && raw.CreatedAt != "" {
		st.firstStepTime = parseTranscriptTimestamp(raw.CreatedAt, time.Time{})
	}

	// Extract Step 0 prompt shortcut
	trimmedContent := strings.TrimSpace(raw.Content)
	if raw.Type == stepTypeUserInput || raw.Source == stepSourceUserExplicit {
		trimmedContent = CleanPromptEnvelope(trimmedContent)
	}
	if !isCodexSystemPreamble(trimmedContent) && st.step0Prompt == "" {
		if raw.Type == stepTypeUserInput || (raw.StepIndex == 0 && raw.Source == stepSourceUserExplicit) {
			st.step0Prompt = trimmedContent
		}
	}

	// Collect dialogue lines for settled session summary
	if (raw.Type == stepTypeUserInput || raw.Type == stepTypePlannerResponse) && trimmedContent != "" && len(st.dialogueLines) < 20 {
		st.dialogueLines = append(st.dialogueLines, fmt.Sprintf("%s: %s", raw.Type, trimmedContent))
	}

	// Tool name association
	raw.ToolName, st.pendingToolNames = resolveStepToolAssociation(raw, st.pendingToolNames)
	st.parsedSteps = append(st.parsedSteps, raw)
}

type claudeEntry struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Message   *struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
	Error json.RawMessage `json:"error"`
}

type claudeContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
	ToolUseID string          `json:"tool_use_id"`
}

func extractClaudeBlockText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var texts []string
		for _, b := range blocks {
			if b.Text != "" {
				texts = append(texts, b.Text)
			}
		}
		return strings.Join(texts, "\n")
	}
	return string(raw)
}

func (st *transcriptScanState) parseClaudeUserEntry(claude claudeEntry) bool {
	var promptStr string
	if err := json.Unmarshal(claude.Message.Content, &promptStr); err == nil {
		raw := rawTranscriptStep{
			StepIndex: st.maxStepIndex + 1,
			Source:    stepSourceUserExplicit,
			Type:      stepTypeUserInput,
			Status:    stepStatusDone,
			CreatedAt: claude.Timestamp,
			Content:   promptStr,
		}
		st.recordStep(raw)
		return true
	}

	var blocks []claudeContentBlock
	if err := json.Unmarshal(claude.Message.Content, &blocks); err == nil && len(blocks) > 0 {
		return st.parseClaudeToolResultBlocks(blocks, claude.Timestamp)
	}
	return false
}

func (st *transcriptScanState) parseClaudeToolResultBlocks(blocks []claudeContentBlock, timestamp string) bool {
	var isToolResult bool
	for _, b := range blocks {
		if b.Type != "tool_result" {
			continue
		}
		isToolResult = true
		status := stepStatusDone
		if b.IsError {
			status = stepStatusError
		}
		st.recordStep(rawTranscriptStep{
			StepIndex:  st.maxStepIndex + 1,
			Source:     stepTypeTool,
			Type:       stepTypeTool,
			Status:     status,
			CreatedAt:  timestamp,
			Content:    extractClaudeBlockText(b.Content),
			ToolName:   st.claudeToolNames[b.ToolUseID],
			ToolCallID: b.ToolUseID,
		})
	}
	return isToolResult
}

// registerClaudeToolUse records a tool_use block's id so later tool_result blocks resolve its name.
func (st *transcriptScanState) registerClaudeToolUse(b claudeContentBlock) {
	if b.ID == "" {
		return
	}
	if st.claudeToolNames == nil {
		st.claudeToolNames = make(map[string]string)
	}
	st.claudeToolNames[b.ID] = b.Name
}

func (st *transcriptScanState) parseClaudeAssistantEntry(claude claudeEntry) bool {
	var blocks []claudeContentBlock
	if err := json.Unmarshal(claude.Message.Content, &blocks); err == nil {
		var textParts []string
		var thinkingParts []string
		var toolCalls []rawToolCall

		for _, b := range blocks {
			switch b.Type {
			case "text":
				if b.Text != "" {
					textParts = append(textParts, b.Text)
				}
			case "thinking":
				if b.Thinking != "" {
					thinkingParts = append(thinkingParts, b.Thinking)
				}
			case "tool_use":
				toolCalls = append(toolCalls, rawToolCall{
					Name: b.Name,
					Args: b.Input,
					ID:   b.ID,
				})
				st.registerClaudeToolUse(b)
			}
		}

		raw := rawTranscriptStep{
			StepIndex: st.maxStepIndex + 1,
			Source:    sourceModel,
			Type:      stepTypePlannerResponse,
			Status:    stepStatusDone,
			CreatedAt: claude.Timestamp,
			Content:   strings.Join(textParts, "\n"),
			Thinking:  strings.Join(thinkingParts, "\n"),
			ToolCalls: toolCalls,
		}
		st.recordStep(raw)
		return true
	}

	var text string
	if err := json.Unmarshal(claude.Message.Content, &text); err == nil {
		raw := rawTranscriptStep{
			StepIndex: st.maxStepIndex + 1,
			Source:    sourceModel,
			Type:      stepTypePlannerResponse,
			Status:    stepStatusDone,
			CreatedAt: claude.Timestamp,
			Content:   text,
		}
		st.recordStep(raw)
		return true
	}
	return false
}

func (st *transcriptScanState) parseClaudeEntry(claude claudeEntry) bool {
	switch claude.Type {
	case roleUser:
		return st.parseClaudeUserEntry(claude)
	case roleAssistant:
		return st.parseClaudeAssistantEntry(claude)
	default:
		return false
	}
}

type codexLineEnvelope struct {
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type codexPayload struct {
	Type      string          `json:"type"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Text      string          `json:"text"`
	Summary   string          `json:"summary"`
	Name      string          `json:"name"`
	CallID    string          `json:"call_id"`
	ID        string          `json:"id"`
	Arguments json.RawMessage `json:"arguments"`
	Input     json.RawMessage `json:"input"`
	Command   string          `json:"command"`
	Cmd       string          `json:"cmd"`
	Query     string          `json:"query"`
	Output    json.RawMessage `json:"output"`
	Status    string          `json:"status"`
	IsError   bool            `json:"is_error"`
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
	return string(content)
}

func extractOutputText(raw json.RawMessage, textFallback string) string {
	if textFallback != "" {
		return textFallback
	}
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func (st *transcriptScanState) parseCodexEntry(env codexLineEnvelope) bool {
	switch env.Type {
	case "session_meta", "turn_context", "event_msg":
		return true
	case "response_item":
	default:
		return false
	}

	if len(env.Payload) == 0 {
		return false
	}
	var payload codexPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return false
	}

	ts := env.Timestamp
	if ts == "" {
		ts = env.CreatedAt
	}

	switch payload.Type {
	case "message":
		text := extractCodexText(payload.Content, payload.Text)
		switch payload.Role {
		case roleUser:
			raw := rawTranscriptStep{
				StepIndex: st.maxStepIndex + 1,
				Source:    stepSourceUserExplicit,
				Type:      stepTypeUserInput,
				Status:    stepStatusDone,
				CreatedAt: ts,
				Content:   text,
			}
			st.recordStep(raw)
			return true
		case roleAssistant:
			raw := rawTranscriptStep{
				StepIndex: st.maxStepIndex + 1,
				Source:    sourceModel,
				Type:      stepTypePlannerResponse,
				Status:    stepStatusDone,
				CreatedAt: ts,
				Content:   text,
			}
			st.recordStep(raw)
			return true
		default:
			return true
		}

	case "reasoning":
		text := extractCodexText(payload.Content, payload.Text)
		if text == "" && payload.Summary != "" {
			text = payload.Summary
		}
		raw := rawTranscriptStep{
			StepIndex: st.maxStepIndex + 1,
			Source:    sourceModel,
			Type:      stepTypePlannerResponse,
			Status:    stepStatusDone,
			CreatedAt: ts,
			Thinking:  text,
		}
		st.recordStep(raw)
		return true

	case "function_call", "custom_tool_call", "local_shell_call", "web_search_call":
		return parseCodexToolCall(payload, ts, st)

	case "function_call_output", "custom_tool_call_output":
		return parseCodexToolOutput(payload, ts, st)

	default:
		return false
	}
}

func parseCodexToolCall(payload codexPayload, ts string, st *transcriptScanState) bool {
	var toolName, callID string
	var args json.RawMessage

	switch payload.Type {
	case "function_call", "custom_tool_call":
		toolName = payload.Name
		callID = payload.CallID
		if callID == "" {
			callID = payload.ID
		}
		args = payload.Arguments
		if len(args) == 0 {
			args = payload.Input
		}
	case "local_shell_call":
		toolName = "local_shell"
		callID = payload.CallID
		if callID == "" {
			callID = payload.ID
		}
		cmd := payload.Command
		if cmd == "" {
			cmd = payload.Cmd
		}
		switch {
		case len(payload.Arguments) > 0:
			args = payload.Arguments
		case len(payload.Input) > 0:
			args = payload.Input
		case cmd != "":
			args = json.RawMessage(fmt.Sprintf(`{"command":%q}`, cmd))
		}
	case "web_search_call":
		toolName = "web_search"
		callID = payload.CallID
		if callID == "" {
			callID = payload.ID
		}
		switch {
		case len(payload.Arguments) > 0:
			args = payload.Arguments
		case len(payload.Input) > 0:
			args = payload.Input
		case payload.Query != "":
			args = json.RawMessage(fmt.Sprintf(`{"query":%q}`, payload.Query))
		}
	default:
		return false
	}

	var strArgs string
	if json.Unmarshal(args, &strArgs) == nil {
		args = json.RawMessage(strArgs)
	}
	raw := rawTranscriptStep{
		StepIndex: st.maxStepIndex + 1,
		Source:    sourceModel,
		Type:      stepTypePlannerResponse,
		Status:    stepStatusDone,
		CreatedAt: ts,
		ToolName:  toolName,
		ToolCalls: []rawToolCall{{Name: toolName, Args: args, ID: callID}},
	}
	if callID != "" {
		st.registerClaudeToolUse(claudeContentBlock{ID: callID, Name: toolName})
	}
	st.recordStep(raw)
	return true
}

func parseCodexToolOutput(payload codexPayload, ts string, st *transcriptScanState) bool {
	callID := payload.CallID
	if callID == "" {
		callID = payload.ID
	}
	status := stepStatusDone
	if payload.IsError || strings.EqualFold(payload.Status, "error") || strings.EqualFold(payload.Status, "failed") {
		status = "ERROR"
	}
	outputStr := extractOutputText(payload.Output, payload.Text)
	toolName := ""
	if callID != "" && st.claudeToolNames != nil {
		toolName = st.claudeToolNames[callID]
	}
	raw := rawTranscriptStep{
		StepIndex:  st.maxStepIndex + 1,
		Source:     stepTypeTool,
		Type:       stepTypeTool,
		Status:     status,
		CreatedAt:  ts,
		Content:    outputStr,
		ToolName:   toolName,
		ToolCallID: callID,
	}
	st.recordStep(raw)
	return true
}

func (st *transcriptScanState) parseLine(line string, isLast bool, stats *SyncStats) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}

	var claude claudeEntry
	if err := json.Unmarshal([]byte(trimmed), &claude); err == nil && claude.Message != nil && (claude.Type == "user" || claude.Type == "assistant") {
		if st.parseClaudeEntry(claude) {
			return
		}
	}

	var codex codexLineEnvelope
	if err := json.Unmarshal([]byte(trimmed), &codex); err == nil && codex.Type != "" {
		if st.parseCodexEntry(codex) {
			return
		}
	}

	var muse museLineEnvelope
	if err := json.Unmarshal([]byte(trimmed), &muse); err == nil && st.parseMuseEntry(muse) {
		return
	}

	var raw rawTranscriptStep
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		if isLast {
			// Gracefully ignore partial line error at EOF / last line
			return
		}
		if stats != nil {
			stats.Errors++
		}
		return
	}
	st.recordStep(raw)
}

func scanSessionTranscriptFile(transcriptPath string, stats *SyncStats) (transcriptScanState, error) {
	st := transcriptScanState{
		firstParsedStep: -1,
		maxStepIndex:    -1,
	}

	f, err := os.Open(transcriptPath)
	if err != nil {
		return st, fmt.Errorf("open transcript: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Transcript] Error closing file %s: %v", transcriptPath, closeErr)
		}
	}()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	var (
		hasPrev  bool
		prevLine string
	)

	for scanner.Scan() {
		if hasPrev {
			st.parseLine(prevLine, false, stats)
		}
		prevLine = scanner.Text()
		hasPrev = true
	}
	if hasPrev {
		st.parseLine(prevLine, true, stats)
	}

	return st, nil
}
