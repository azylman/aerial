package session

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultTerminalStepQuiescence is the minimum time since a terminal PLANNER_RESPONSE step's
// created_at timestamp and task log modifications before considering the turn quiescent and eligible for rescue.
const (
	DefaultTerminalStepQuiescence = 15 * time.Second
	backgroundTaskRunningMarker   = "Tool is running as a background task with task id:"
	backgroundTasksStoppedMarker  = "All your subagents and background tasks have been stopped due to server restart"
)

func hasRecentTaskLogActivity(sessDir string, minQuiescence time.Duration) bool {
	tasksDir := filepath.Join(sessDir, ".system_generated", "tasks")
	entries, err := os.ReadDir(tasksDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if time.Since(info.ModTime()) < minQuiescence {
			return true
		}
	}
	return false
}

func findTerminalStepIndices(lines []string) (int, int, bool) {
	lastUserInputIdx := findLastUserInputIdx(lines)
	startIdx := 0
	if lastUserInputIdx >= 0 {
		startIdx = lastUserInputIdx + 1
	}

	for i := len(lines) - 1; i >= startIdx; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return startIdx, i, true
		}
	}
	return startIdx, -1, false
}

type terminalStepLine struct {
	Type      string `json:"type"`
	Status    string `json:"status"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
	ToolCalls []struct {
		Name string `json:"name"`
	} `json:"tool_calls"`
}

func countPendingAsyncAndFinalStep(ctx context.Context, lines []string, startIdx, lastNonEmptyIdx int) (int, transcriptStepRecord, bool) {
	var finalStep transcriptStepRecord
	pendingAsyncCalls := 0

	for i := startIdx; i <= lastNonEmptyIdx; i++ {
		if ctx != nil && ctx.Err() != nil {
			return 0, finalStep, false
		}
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		var step terminalStepLine
		if err := json.Unmarshal([]byte(line), &step); err != nil {
			continue
		}

		for _, tc := range step.ToolCalls {
			switch tc.Name {
			case "invoke_subagent", "run_workflow", "schedule":
				pendingAsyncCalls++
			}
		}

		if step.Type != stepTypePlannerResponse && step.Type != stepTypeSystemMessage && step.Type != stepTypeUserInput && strings.Contains(step.Content, backgroundTaskRunningMarker) {
			pendingAsyncCalls++
		}

		if step.Type == stepTypeSystemMessage && strings.Contains(step.Content, backgroundTasksStoppedMarker) {
			pendingAsyncCalls = 0
		} else if (step.Type == stepTypeSystemMessage || step.Type == stepTypeUserInput) && pendingAsyncCalls > 0 {
			pendingAsyncCalls--
		}

		if i == lastNonEmptyIdx {
			if err := json.Unmarshal([]byte(line), &finalStep); err != nil {
				return 0, finalStep, false
			}
		}
	}

	return pendingAsyncCalls, finalStep, true
}

func validateTerminalResponse(finalStep transcriptStepRecord, since time.Time, minQuiescence time.Duration) string {
	if finalStep.Type != stepTypePlannerResponse || finalStep.Status != "DONE" || len(finalStep.ToolCalls) != 0 || finalStep.CreatedAt == "" {
		return ""
	}

	stepTime, err := time.Parse(time.RFC3339Nano, finalStep.CreatedAt)
	if err != nil || (!since.IsZero() && stepTime.Before(since)) || time.Since(stepTime) < minQuiescence {
		return ""
	}

	trimmedContent := strings.TrimSpace(finalStep.Content)
	if trimmedContent == "" || strings.HasPrefix(trimmedContent, "[Tool Call Requested]:") || strings.Contains(trimmedContent, AgyBackgroundAbortNotice) {
		return ""
	}

	return trimmedContent
}

func scanLinesForTerminalResponse(ctx context.Context, lines []string, since time.Time, minQuiescence time.Duration) string {
	if ctx != nil && ctx.Err() != nil {
		return ""
	}
	startIdx, lastNonEmptyIdx, ok := findTerminalStepIndices(lines)
	if !ok {
		return ""
	}
	pending, finalStep, ok := countPendingAsyncAndFinalStep(ctx, lines, startIdx, lastNonEmptyIdx)
	if !ok || pending != 0 {
		return ""
	}
	return validateTerminalResponse(finalStep, since, minQuiescence)
}

func extractTerminalResponseFromTranscriptFile(ctx context.Context, tPath string, since time.Time, minQuiescence time.Duration) string {
	data, err := os.ReadFile(tPath)
	if err != nil || len(data) == 0 {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	return scanLinesForTerminalResponse(ctx, lines, since, minQuiescence)
}

// ExtractTerminalTurnResponse parses transcript files for convID and returns the terminal substantive
// PLANNER_RESPONSE turn strictly after 'since' if quiescent and no async background calls are pending.
func (m *Manager) ExtractTerminalTurnResponse(ctx context.Context, convID string, since time.Time, minQuiescence time.Duration) string {
	cleanID, ok := m.validSessionID(convID)
	if !ok || (ctx != nil && ctx.Err() != nil) {
		return ""
	}

	targetDirs := m.getTargetDirs(cleanID)
	for _, dir := range targetDirs {
		if hasRecentTaskLogActivity(dir, minQuiescence) {
			continue
		}
		for _, name := range []string{transcriptFullFileName, transcriptFileName} {
			tPath := filepath.Join(dir, ".system_generated", "logs", name)
			if resp := extractTerminalResponseFromTranscriptFile(ctx, tPath, since, minQuiescence); resp != "" {
				return resp
			}
		}
	}

	return ""
}

// ExtractDefaultTerminalTurnResponse invokes ExtractTerminalTurnResponse using DefaultTerminalStepQuiescence.
func (m *Manager) ExtractDefaultTerminalTurnResponse(ctx context.Context, convID string, since time.Time) string {
	return m.ExtractTerminalTurnResponse(ctx, convID, since, DefaultTerminalStepQuiescence)
}
