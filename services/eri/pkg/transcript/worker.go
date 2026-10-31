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
}

type rawToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
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

// SyncTranscripts scans the brain directory for session transcripts, reconciles modification
// times against the database, ingests new steps, and updates debounced macro summaries.
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

	// Deterministic ordering of sessions
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		if !entry.IsDir() {
			continue
		}
		sessionID := entry.Name()
		if strings.HasPrefix(sessionID, ".") {
			continue
		}

		transcriptPath := findTranscriptFile(targetBrainDir, sessionID)
		if transcriptPath == "" {
			continue
		}

		fi, err := os.Stat(transcriptPath)
		if err != nil {
			continue
		}
		mtime := fi.ModTime().UTC()
		stats.Scanned++

		state, exists := syncStates[sessionID]
		if exists && !mtime.Truncate(time.Millisecond).After(state.LastMtime.Truncate(time.Millisecond)) {
			stats.SkippedUnchanged++
			continue
		}

		if stats.Synced >= opts.BatchLimit {
			break
		}

		if err := syncSingleSession(ctx, store, sessionID, transcriptPath, mtime, state, exists, embedder, llmFunc, opts, &stats); err != nil {
			stats.Errors++
			log.Printf("[Transcript] Error syncing session %s: %v", sessionID, err)
		} else {
			stats.Synced++
		}
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

func findTranscriptFile(brainDir, sessionID string) string {
	candidates := []string{
		filepath.Join(brainDir, sessionID, ".system_generated", "logs", "transcript.jsonl"),
		filepath.Join(brainDir, sessionID, "system_generated", "logs", "transcript.jsonl"),
		filepath.Join(brainDir, sessionID, "transcript.jsonl"),
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c
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

func resolveStepToolAssociation(raw rawTranscriptStep, pendingToolNames []string) (effectiveToolName string, nextPending []string) {
	effectiveToolName = raw.ToolName
	if effectiveToolName == "" && len(raw.ToolCalls) > 0 {
		effectiveToolName = raw.ToolCalls[0].Name
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

func (st *transcriptScanState) parseLine(line string, isLast bool, stats *SyncStats) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
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
			SessionID: sessionID,
			StepIndex: raw.StepIndex,
			StepType:  raw.Type,
			ToolName:  raw.ToolName,
			Content:   clipped,
			CreatedAt: stepTime,
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
		prompt := fmt.Sprintf("Summarize the following agent conversation transcript in 2-3 concise sentences. Focus on technical entities, tools executed, and outcomes.\n\nTranscript:\n%s", strings.Join(dialogueLines, "\n"))
		if res, err := llmFunc(ctx, prompt); err == nil && strings.TrimSpace(res) != "" {
			summaryText = strings.TrimSpace(res)
		}
	}

	if summaryText == "" {
		summaryText = CleanSessionSummary(step0Prompt, DefaultSummaryMaxRunes)
	}
	if summaryText == "" {
		summaryText = fmt.Sprintf("Session %s", sessionID)
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
	sessionID, transcriptPath string,
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
