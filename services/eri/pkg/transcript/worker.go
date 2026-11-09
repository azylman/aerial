package transcript

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
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

// normalizeSyncOptions validates and populates default sync options and targets.
func normalizeSyncOptions(brainDir string, opts SyncOptions) (string, SyncOptions, error) {
	targetBrainDir := strings.TrimSpace(brainDir)
	if targetBrainDir == "" {
		targetBrainDir = strings.TrimSpace(opts.BrainDir)
	}
	if targetBrainDir == "" {
		return "", opts, fmt.Errorf("brain directory cannot be empty")
	}

	if opts.BatchLimit <= 0 {
		opts.BatchLimit = 100
	}
	if opts.IdleThreshold <= 0 {
		opts.IdleThreshold = 10 * time.Minute
	}
	if opts.HeadLimit <= 0 && opts.TailLimit <= 0 {
		opts.HeadLimit = DefaultHeadLimit
		opts.TailLimit = DefaultTailLimit
	}
	return targetBrainDir, opts, nil
}

// SyncTranscripts scans the brain directory and Claude project directories for session transcripts,
// reconciles modification times against the database, ingests new steps, and updates debounced macro summaries.
func SyncTranscripts(
	ctx context.Context,
	store db.TranscriptStore,
	brainDir string,
	embedder EmbedderFunc,
	llmFunc LLMClientFunc,
	opts SyncOptions,
) (SyncStats, error) {
	start := time.Now()
	var stats SyncStats

	if store == nil {
		return stats, fmt.Errorf("transcript store cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	targetBrainDir, opts, err := normalizeSyncOptions(brainDir, opts)
	if err != nil {
		return stats, err
	}

	entries, err := os.ReadDir(targetBrainDir)
	if err != nil {
		return stats, fmt.Errorf("read brain dir %s: %w", targetBrainDir, err)
	}

	syncStates, err := store.GetSessionSyncStates(ctx)
	if err != nil {
		return stats, fmt.Errorf("load session sync states: %w", err)
	}

	discovered := discoverSessions(targetBrainDir, entries, opts.ClaudeProjectsDir)

	// Deterministic ordering of sessions
	sessionIDs := make([]string, 0, len(discovered))
	for id := range discovered {
		sessionIDs = append(sessionIDs, id)
	}
	sort.Strings(sessionIDs)

	for _, sessionID := range sessionIDs {
		select {
		case <-ctx.Done():
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		found := discovered[sessionID]
		transcriptPath := found.path
		fi, err := os.Stat(transcriptPath)
		if err != nil {
			continue
		}
		mtime := fi.ModTime().UTC()
		stats.Scanned++

		state, exists := syncStates[sessionID]
		if exists && !mtime.Truncate(time.Millisecond).After(state.LastMtime.Truncate(time.Millisecond)) {
			stats.SkippedUnchanged++
			if state.Engine == "" {
				if err := store.SetSessionEngine(ctx, sessionID, found.engine); err != nil {
					log.Printf("[Transcript] SetSessionEngine error for session %s: %v", sessionID, err)
				}
			}
			continue
		}

		if stats.Synced >= opts.BatchLimit {
			break
		}

		if err := syncSingleSession(ctx, store, sessionID, found.engine, transcriptPath, mtime, state, exists, embedder, llmFunc, opts, &stats); err != nil {
			stats.Errors++
			log.Printf("[Transcript] Error syncing session %s: %v", sessionID, err)
		} else {
			stats.Synced++
		}
	}

	backfillToolKinds(ctx, store, opts.MCPServers)
	backfillSessionEngines(ctx, store)
	stats.Duration = time.Since(start)
	return stats, nil
}

// discoverSessions maps every session ID found under the brain dir (agy) and the Claude project
// dirs (Claude Code) to its transcript path and engine. Brain-dir entries win on ID collisions.
func discoverSessions(targetBrainDir string, entries []os.DirEntry, claudeProjectsDir string) map[string]discoveredSession {
	discovered := make(map[string]discoveredSession)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		sessionID := entry.Name()
		if strings.HasPrefix(sessionID, ".") {
			continue
		}

		if transcriptPath := findTranscriptFile(targetBrainDir, sessionID); transcriptPath != "" {
			discovered[sessionID] = discoveredSession{path: transcriptPath, engine: engineForPath(targetBrainDir, transcriptPath)}
		}
	}

	// Also discover Claude sessions from Claude project directories
	var claudeBases []string
	if claudeProjectsDir != "" {
		claudeBases = append(claudeBases, claudeProjectsDir)
	}
	claudeBases = append(claudeBases,
		filepath.Clean(filepath.Join(targetBrainDir, "..", "claude", "projects")),
		filepath.Clean(filepath.Join(targetBrainDir, "..", "..", "..", "claude", "projects")),
		filepath.Clean(filepath.Join(targetBrainDir, "..", ".claude", "projects")),
	)
	discoverClaudeProjectSessions(claudeBases, discovered)

	return discovered
}

// discoveredSession locates a session transcript on disk and the engine that wrote it.
type discoveredSession struct {
	path   string
	engine string
}

// engineForPath attributes transcripts inside the brain dir to agy and everything else
// (Claude project dirs) to Claude Code.
func engineForPath(brainDir, transcriptPath string) string {
	if strings.HasPrefix(filepath.Clean(transcriptPath), filepath.Clean(brainDir)+string(filepath.Separator)) {
		return EngineAgy
	}
	return EngineClaude
}

// backfillToolKinds normalizes tool_kind for local-stack steps ingested before normalization existed.
func backfillToolKinds(ctx context.Context, store db.TranscriptStore, mcpServers []string) {
	updated, err := store.BackfillToolKinds(ctx, func(toolName string) string {
		return NormalizeToolKind(toolName, mcpServers)
	})
	if err != nil {
		log.Printf("[Transcript] BackfillToolKinds error: %v", err)
		return
	}
	if updated > 0 {
		log.Printf("[Transcript] Backfilled tool_kind on %d steps", updated)
	}
}

// backfillSessionEngines labels local-stack sessions that never got an engine (their raw log was
// gone before engines were recorded) by inferring it from their stored tool names.
func backfillSessionEngines(ctx context.Context, store db.TranscriptStore) {
	updated, err := store.BackfillSessionEngines(ctx, InferEngine)
	if err != nil {
		log.Printf("[Transcript] BackfillSessionEngines error: %v", err)
		return
	}
	if updated > 0 {
		log.Printf("[Transcript] Backfilled engine on %d sessions", updated)
	}
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

func findTranscriptFile(brainDir, sessionID string) string {
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
	for _, claudeBase := range []string{
		filepath.Clean(filepath.Join(brainDir, "..", "claude", "projects")),
		filepath.Clean(filepath.Join(brainDir, "..", "..", "..", "claude", "projects")),
		filepath.Clean(filepath.Join(brainDir, "..", ".claude", "projects")),
	} {
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
	if raw.Type == "GENERIC" || raw.Type == "TOOL" || raw.Type == "RUN_COMMAND" {
		if len(pendingToolNames) > 0 {
			if effectiveToolName == "" {
				effectiveToolName = pendingToolNames[0]
			}
			nextPending = pendingToolNames[1:]
		}
		return effectiveToolName, nextPending
	}
	if raw.Type == "PLANNER_RESPONSE" {
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
	if raw.Type == "USER_INPUT" || raw.Source == "USER_EXPLICIT" {
		trimmedContent = CleanPromptEnvelope(trimmedContent)
	}
	if raw.StepIndex == 0 && (raw.Type == "USER_INPUT" || raw.Source == "USER_EXPLICIT") && st.step0Prompt == "" {
		st.step0Prompt = trimmedContent
	} else if raw.Type == "USER_INPUT" && st.step0Prompt == "" {
		st.step0Prompt = trimmedContent
	}

	// Collect dialogue lines for settled session summary
	if (raw.Type == "USER_INPUT" || raw.Type == "PLANNER_RESPONSE") && trimmedContent != "" && len(st.dialogueLines) < 20 {
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
			Source:    "USER_EXPLICIT",
			Type:      "USER_INPUT",
			Status:    "DONE",
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
		status := "DONE"
		if b.IsError {
			status = "ERROR"
		}
		st.recordStep(rawTranscriptStep{
			StepIndex:  st.maxStepIndex + 1,
			Source:     "TOOL",
			Type:       "TOOL",
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
			Source:    "MODEL",
			Type:      "PLANNER_RESPONSE",
			Status:    "DONE",
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
			Source:    "MODEL",
			Type:      "PLANNER_RESPONSE",
			Status:    "DONE",
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
	case "user":
		return st.parseClaudeUserEntry(claude)
	case "assistant":
		return st.parseClaudeAssistantEntry(claude)
	default:
		return false
	}
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

func computeSessionWatermark(exists bool, firstParsedStep int, state db.SessionSyncState) int {
	if !exists {
		return -1
	}
	if firstParsedStep >= 0 && state.LastIndexedStep >= 0 && firstParsedStep < state.LastIndexedStep {
		return -1
	}
	return state.LastIndexedStep
}

func buildTranscriptStepsToInsert(
	sessionID string,
	parsedSteps []rawTranscriptStep,
	watermark int,
	mtime time.Time,
	opts SyncOptions,
) []db.TranscriptStep {
	var stepsToInsert []db.TranscriptStep
	for _, raw := range parsedSteps {
		if raw.StepIndex <= watermark {
			continue
		}

		content := raw.Content
		if content == "" && len(raw.ToolCalls) > 0 {
			content = string(raw.ToolCalls[0].Args)
		}

		if !IsSubstantiveStep(raw.Type, raw.ToolName, content) {
			continue
		}

		clipped := ClipContent(content, opts.HeadLimit, opts.TailLimit)
		stepTime := parseTranscriptTimestamp(raw.CreatedAt, mtime)

		stepsToInsert = append(stepsToInsert, db.TranscriptStep{
			SessionID:  sessionID,
			StepIndex:  raw.StepIndex,
			StepType:   raw.Type,
			ToolName:   raw.ToolName,
			ToolKind:   NormalizeToolKind(raw.ToolName, opts.MCPServers),
			ToolCallID: raw.ToolCallID,
			Content:    clipped,
			CreatedAt:  stepTime,
		})
	}
	return stepsToInsert
}

func generateSettledSummaryText(
	ctx context.Context,
	sessionID string,
	isSettled bool,
	dialogueLines []string,
	step0Prompt string,
	llmFunc LLMClientFunc,
) string {
	summaryText := ""
	if isSettled && llmFunc != nil && len(dialogueLines) > 0 {
		prompt := "Summarize the following agent conversation transcript in 2-3 concise sentences. Focus on technical entities, tools executed, and outcomes.\n\nTranscript:\n" + strings.Join(dialogueLines, "\n")
		if res, err := llmFunc(ctx, prompt); err == nil && strings.TrimSpace(res) != "" {
			summaryText = strings.TrimSpace(res)
		}
	}

	if summaryText == "" {
		summaryText = CleanSessionSummary(step0Prompt, DefaultSummaryMaxRunes)
	}
	if summaryText == "" {
		summaryText = "Session " + sessionID
	}

	return summaryText
}

func buildSettledSummaryRecord(
	ctx context.Context,
	sessionID string,
	mtime time.Time,
	isSettled bool,
	st transcriptScanState,
	watermark int,
	embedder EmbedderFunc,
	llmFunc LLMClientFunc,
) db.SessionSummary {
	summaryText := generateSettledSummaryText(ctx, sessionID, isSettled, st.dialogueLines, st.step0Prompt, llmFunc)

	var emb []float32
	if embedder != nil {
		if e, err := embedder(ctx, summaryText); err == nil && len(e) == db.ExpectedEmbeddingDim {
			emb = e
		}
	}

	firstStepTime := st.firstStepTime
	if firstStepTime.IsZero() {
		firstStepTime = mtime
	}

	lastIndexedStepToSave := st.maxStepIndex
	if watermark == -1 && st.maxStepIndex == -1 {
		lastIndexedStepToSave = -2
	}

	return db.SessionSummary{
		SessionID:            sessionID,
		Summary:              summaryText,
		Embedding:            emb,
		LastIndexedStep:      lastIndexedStepToSave,
		LastMtime:            mtime,
		SummaryStepWatermark: st.maxStepIndex,
		IsSettled:            isSettled,
		CreatedAt:            firstStepTime,
		UpdatedAt:            time.Now().UTC(),
	}
}

func buildSessionSummaryRecord(
	ctx context.Context,
	sessionID string,
	mtime time.Time,
	state db.SessionSyncState,
	exists bool,
	st transcriptScanState,
	watermark int,
	embedder EmbedderFunc,
	llmFunc LLMClientFunc,
	opts SyncOptions,
) (db.SessionSummary, bool, bool) {
	isSettled := time.Since(mtime) > opts.IdleThreshold
	shouldSummarize := !exists || (isSettled && (!state.IsSettled || (st.maxStepIndex >= 0 && st.maxStepIndex-state.LastIndexedStep > 10)))

	if shouldSummarize {
		summaryRecord := buildSettledSummaryRecord(ctx, sessionID, mtime, isSettled, st, watermark, embedder, llmFunc)
		return summaryRecord, true, true
	}

	if exists {
		summaryRecord := db.SessionSummary{
			SessionID:       sessionID,
			LastIndexedStep: st.maxStepIndex,
			LastMtime:       mtime,
			IsSettled:       false,
			UpdatedAt:       time.Now().UTC(),
		}
		return summaryRecord, true, false
	}

	return db.SessionSummary{}, false, false
}

func insertTranscriptSteps(
	ctx context.Context,
	store db.TranscriptStore,
	sessionID string,
	stepsToInsert []db.TranscriptStep,
	hasSummaryRecord bool,
	summaryRecord db.SessionSummary,
	watermark int,
	stats *SyncStats,
) {
	if len(stepsToInsert) == 0 {
		return
	}
	if err := store.BatchInsertTranscriptSteps(ctx, stepsToInsert); err != nil {
		stats.Errors++
		log.Printf("[Transcript] BatchInsertTranscriptSteps error for session %s: %v", sessionID, err)
		if hasSummaryRecord {
			summaryRecord.LastIndexedStep = watermark
			if revertErr := store.UpsertSessionSummary(ctx, summaryRecord); revertErr != nil {
				log.Printf("[Transcript] Failed to revert session summary watermark for session %s: %v", sessionID, revertErr)
			}
		}
		return
	}
	stats.StepsInserted += len(stepsToInsert)
}

func syncSingleSession(
	ctx context.Context,
	store db.TranscriptStore,
	sessionID, engine, transcriptPath string,
	mtime time.Time,
	state db.SessionSyncState,
	exists bool,
	embedder EmbedderFunc,
	llmFunc LLMClientFunc,
	opts SyncOptions,
	stats *SyncStats,
) error {
	st, err := scanSessionTranscriptFile(transcriptPath, stats)
	if err != nil {
		return err
	}

	watermark := computeSessionWatermark(exists, st.firstParsedStep, state)
	stepsToInsert := buildTranscriptStepsToInsert(sessionID, st.parsedSteps, watermark, mtime, opts)
	summaryRecord, hasSummaryRecord, shouldSummarize := buildSessionSummaryRecord(
		ctx, sessionID, mtime, state, exists, st, watermark, embedder, llmFunc, opts,
	)
	summaryRecord.Engine = engine

	// 1. Ensure parent session summary exists before inserting child transcript steps to satisfy foreign key.
	if hasSummaryRecord {
		if err := store.UpsertSessionSummary(ctx, summaryRecord); err != nil {
			log.Printf("[Transcript] UpsertSessionSummary error for session %s: %v", sessionID, err)
			return err
		}
		if shouldSummarize {
			stats.SummariesGenerated++
		}
	}

	// 2. Batch insert child transcript steps now that parent session summary is guaranteed to exist.
	insertTranscriptSteps(ctx, store, sessionID, stepsToInsert, hasSummaryRecord, summaryRecord, watermark, stats)

	return nil
}
