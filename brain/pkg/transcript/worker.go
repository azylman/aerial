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

	"github.com/azylman/aerial/brain/pkg/db"
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

	targetBrainDir := strings.TrimSpace(brainDir)
	if targetBrainDir == "" {
		targetBrainDir = strings.TrimSpace(opts.BrainDir)
	}
	if targetBrainDir == "" {
		return stats, fmt.Errorf("brain directory cannot be empty")
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
	f, err := os.Open(transcriptPath)
	if err != nil {
		return fmt.Errorf("open transcript: %w", err)
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
		parsedSteps     []rawTranscriptStep
		firstParsedStep = -1
		maxStepIndex    = -1
		step0Prompt     = ""
		firstStepTime   time.Time
		dialogueLines   []string
		pendingToolName = ""
		hasPrev         = false
		prevLine        string
	)

	processStep := func(raw rawTranscriptStep) {
		if firstParsedStep == -1 {
			firstParsedStep = raw.StepIndex
		}
		if raw.StepIndex > maxStepIndex {
			maxStepIndex = raw.StepIndex
		}

		if firstStepTime.IsZero() && raw.CreatedAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, raw.CreatedAt); err == nil {
				firstStepTime = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
				firstStepTime = t.UTC()
			}
		}

		// Extract Step 0 prompt shortcut
		trimmedContent := strings.TrimSpace(raw.Content)
		if raw.StepIndex == 0 && (raw.Type == "USER_INPUT" || raw.Source == "USER_EXPLICIT") && step0Prompt == "" {
			step0Prompt = trimmedContent
		} else if raw.Type == "USER_INPUT" && step0Prompt == "" {
			step0Prompt = trimmedContent
		}

		// Collect dialogue lines for settled session summary
		if (raw.Type == "USER_INPUT" || raw.Type == "PLANNER_RESPONSE") && trimmedContent != "" && len(dialogueLines) < 20 {
			dialogueLines = append(dialogueLines, fmt.Sprintf("%s: %s", raw.Type, trimmedContent))
		}

		// Tool name association
		effectiveToolName := raw.ToolName
		if effectiveToolName == "" && len(raw.ToolCalls) > 0 {
			effectiveToolName = raw.ToolCalls[0].Name
		}
		if effectiveToolName == "" && (raw.Type == "GENERIC" || raw.Type == "TOOL" || raw.Type == "RUN_COMMAND") {
			effectiveToolName = pendingToolName
		}

		if raw.Type == "PLANNER_RESPONSE" && len(raw.ToolCalls) > 0 {
			pendingToolName = raw.ToolCalls[0].Name
		} else if raw.Type != "PLANNER_RESPONSE" {
			pendingToolName = ""
		}

		raw.ToolName = effectiveToolName
		parsedSteps = append(parsedSteps, raw)
	}

	parseLine := func(line string, isLast bool) {
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
			stats.Errors++
			return
		}
		processStep(raw)
	}

	for scanner.Scan() {
		if hasPrev {
			parseLine(prevLine, false)
		}
		prevLine = scanner.Text()
		hasPrev = true
	}
	if hasPrev {
		parseLine(prevLine, true)
	}

	// Truncation detection: if first parsed step < state.LastIndexedStep, reset watermark
	watermark := -1
	if exists {
		if firstParsedStep >= 0 && state.LastIndexedStep >= 0 && firstParsedStep < state.LastIndexedStep {
			watermark = -1
		} else {
			watermark = state.LastIndexedStep
		}
	}

	// Filter and clip steps for insertion
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

		stepTime := mtime
		if raw.CreatedAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, raw.CreatedAt); err == nil {
				stepTime = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
				stepTime = t.UTC()
			}
		}

		stepsToInsert = append(stepsToInsert, db.TranscriptStep{
			SessionID: sessionID,
			StepIndex: raw.StepIndex,
			StepType:  raw.Type,
			ToolName:  raw.ToolName,
			Content:   clipped,
			CreatedAt: stepTime,
		})
	}

	if len(stepsToInsert) > 0 {
		if err := store.BatchInsertTranscriptSteps(ctx, stepsToInsert); err != nil {
			stats.Errors++
			log.Printf("[Transcript] BatchInsertTranscriptSteps error for session %s: %v", sessionID, err)
		} else {
			stats.StepsInserted += len(stepsToInsert)
		}
	}

	// Debounced Macro Summary Generation
	isSettled := time.Since(mtime) > opts.IdleThreshold
	shouldSummarize := false
	if !exists {
		shouldSummarize = true
	} else if isSettled && (!state.IsSettled || (maxStepIndex >= 0 && maxStepIndex-state.LastIndexedStep > 10)) {
		shouldSummarize = true
	}

	if shouldSummarize {
		summaryText := ""
		if isSettled && llmFunc != nil && len(dialogueLines) > 0 {
			prompt := fmt.Sprintf("Summarize the following agent conversation transcript in 2-3 concise sentences. Focus on technical entities, tools executed, and outcomes.\n\nTranscript:\n%s", strings.Join(dialogueLines, "\n"))
			if res, err := llmFunc(ctx, prompt); err == nil && strings.TrimSpace(res) != "" {
				summaryText = strings.TrimSpace(res)
			}
		}

		if summaryText == "" {
			summaryText = step0Prompt
		}
		if summaryText == "" {
			summaryText = fmt.Sprintf("Session %s", sessionID)
		}

		var emb []float32
		if embedder != nil {
			if e, err := embedder(ctx, summaryText); err == nil && len(e) == db.ExpectedEmbeddingDim {
				emb = e
			}
		}

		if firstStepTime.IsZero() {
			firstStepTime = mtime
		}

		lastIndexedStepToSave := maxStepIndex
		if watermark == -1 && maxStepIndex == -1 {
			lastIndexedStepToSave = -2
		}

		summaryRecord := db.SessionSummary{
			SessionID:            sessionID,
			Summary:              summaryText,
			Embedding:            emb,
			LastIndexedStep:      lastIndexedStepToSave,
			LastMtime:            mtime,
			SummaryStepWatermark: maxStepIndex,
			IsSettled:            isSettled,
			CreatedAt:            firstStepTime,
			UpdatedAt:            time.Now().UTC(),
		}
		if err := store.UpsertSessionSummary(ctx, summaryRecord); err != nil {
			stats.Errors++
			log.Printf("[Transcript] UpsertSessionSummary error for session %s: %v", sessionID, err)
		} else {
			stats.SummariesGenerated++
		}
	} else if exists {
		summaryRecord := db.SessionSummary{
			SessionID:       sessionID,
			LastIndexedStep: maxStepIndex,
			LastMtime:       mtime,
			IsSettled:       false,
			UpdatedAt:       time.Now().UTC(),
		}
		if err := store.UpsertSessionSummary(ctx, summaryRecord); err != nil {
			stats.Errors++
			log.Printf("[Transcript] UpsertSessionSummary active progress error for session %s: %v", sessionID, err)
		}
	}

	return nil
}
