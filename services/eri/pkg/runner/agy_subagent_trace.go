package runner

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// AsyncSubagentSink is an optional interface that TurnSinks can implement
// to backfill child spans when an asynchronous subagent conversation finishes.
type AsyncSubagentSink interface {
	CompleteAsyncSubagent(summary SubagentTraceSummary)
}

type pendingAgySubagent struct {
	parentCallID   string
	conversationID string
	transcriptPath string
	stepIndex      int
}

func (d *StreamingDaemon) agyTranscriptPath(convID string) string {
	home := strings.TrimSpace(d.cfg.GeminiHomeDir)
	convID = strings.TrimSpace(convID)
	if home == "" || convID == "" || strings.Contains(convID, "..") || strings.ContainsAny(convID, `/\`) {
		return ""
	}
	candidates := []string{
		filepath.Join(home, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs", "transcript.jsonl"),
		filepath.Join(home, ".gemini", "antigravity", "brain", convID, ".system_generated", "logs", "transcript.jsonl"),
		filepath.Join(home, "brain", convID, ".system_generated", "logs", "transcript.jsonl"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return candidates[0]
}

func newTranscriptScanner(f *os.File) *bufio.Scanner {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	return sc
}

func parseCreatedSubagentBlock(content string) (convID, transcriptPath string) {
	const marker = "Created the following subagent:"
	_, rest, ok := strings.Cut(content, marker)
	if !ok {
		return "", ""
	}
	start := strings.IndexByte(rest, '{')
	end := strings.IndexByte(rest, '}')
	if start < 0 || end <= start {
		return "", ""
	}
	var payload struct {
		ConversationID string `json:"conversationId"`
		LogAbsoluteURI string `json:"logAbsoluteUri"`
	}
	if err := json.Unmarshal([]byte(rest[start:end+1]), &payload); err != nil {
		return "", ""
	}
	convID = strings.TrimSpace(payload.ConversationID)
	uri := strings.TrimSpace(payload.LogAbsoluteURI)
	uri = strings.TrimPrefix(uri, "file://")
	return convID, uri
}

type parentTranscriptStep struct {
	StepIndex int    `json:"step_index"`
	Type      string `json:"type"`
	Content   string `json:"content"`
}

func scanParentTranscriptSteps(parentTranscriptPath string, visit func(parentTranscriptStep) bool) {
	if parentTranscriptPath == "" {
		return
	}
	f, err := os.Open(parentTranscriptPath)
	if err != nil {
		return
	}
	defer closeStreamQuietly(f, "parent transcript")

	sc := newTranscriptScanner(f)
	for sc.Scan() {
		var line parentTranscriptStep
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			continue
		}
		if visit(line) {
			return
		}
	}
}

func extractAgySubagentFromParentTranscript(parentTranscriptPath string, stepIndex int) (convID, transcriptPath string) {
	scanParentTranscriptSteps(parentTranscriptPath, func(line parentTranscriptStep) bool {
		if !strings.Contains(line.Content, "Created the following subagent:") {
			return false
		}
		id, path := parseCreatedSubagentBlock(line.Content)
		if id == "" {
			return false
		}
		convID, transcriptPath = id, path
		return stepIndex > 0 && line.StepIndex == stepIndex
	})
	return convID, transcriptPath
}

func parentTranscriptHasSubagentSender(parentTranscriptPath, subagentConvID string, afterStepIdx int) bool {
	if subagentConvID == "" {
		return false
	}
	needle := "sender=" + subagentConvID
	var found bool
	scanParentTranscriptSteps(parentTranscriptPath, func(line parentTranscriptStep) bool {
		if line.StepIndex > afterStepIdx && line.Type == "SYSTEM_MESSAGE" && strings.Contains(line.Content, needle) {
			found = true
			return true
		}
		return false
	})
	return found
}

func parseStepOutputTimestamps(content string) (createdAt, completedAt time.Time) {
	const (
		createdPrefix   = "Created At:"
		completedPrefix = "Completed At:"
	)
	for _, rawLine := range strings.SplitN(content, "\n", 10) {
		line := strings.TrimSpace(rawLine)
		if rest, ok := strings.CutPrefix(line, createdPrefix); ok && createdAt.IsZero() {
			if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(rest)); err == nil {
				createdAt = t
			}
		} else if rest, ok := strings.CutPrefix(line, completedPrefix); ok && completedAt.IsZero() {
			if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(rest)); err == nil {
				completedAt = t
			}
		}
	}
	return createdAt, completedAt
}

type agyTranscriptLine struct {
	Type            string            `json:"type"`
	Status          string            `json:"status"`
	CreatedAt       string            `json:"created_at"`
	Content         string            `json:"content"`
	Model           string            `json:"model"`
	InputTokens     int               `json:"input_tokens"`
	OutputTokens    int               `json:"output_tokens"`
	CacheReadTokens int               `json:"cache_read_tokens"`
	ThinkingTokens  int               `json:"thinking_tokens"`
	Usage           map[string]any    `json:"usage"`
	ToolCalls       []agyToolCallItem `json:"tool_calls"`
}

type agyToolCallItem struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

type pendingSubagentStep struct {
	name      string
	mcpServer string
	startTime time.Time
}

func extractAgyLineUsage(line agyTranscriptLine) AgyUsage {
	if line.InputTokens > 0 || line.OutputTokens > 0 || line.CacheReadTokens > 0 || line.ThinkingTokens > 0 {
		return AgyUsage{
			InputTokens:     line.InputTokens,
			OutputTokens:    line.OutputTokens,
			CacheReadTokens: line.CacheReadTokens,
			ThinkingTokens:  line.ThinkingTokens,
			TotalTokens:     line.InputTokens + line.OutputTokens,
		}
	}
	if line.Usage != nil {
		return usageFromObject(line.Usage)
	}
	return AgyUsage{}
}

func addAgyUsage(dst *AgyUsage, delta AgyUsage) {
	dst.InputTokens += delta.InputTokens
	dst.OutputTokens += delta.OutputTokens
	dst.CacheReadTokens += delta.CacheReadTokens
	dst.CacheCreationTokens += delta.CacheCreationTokens
	dst.ThinkingTokens += delta.ThinkingTokens
	dst.TotalTokens += delta.TotalTokens
}

func recordSubagentTimeBounds(summary *SubagentTraceSummary, ts time.Time) {
	if ts.IsZero() {
		return
	}
	if summary.ChatStart.IsZero() || ts.Before(summary.ChatStart) {
		summary.ChatStart = ts
	}
	if summary.ChatEnd.IsZero() || ts.After(summary.ChatEnd) {
		summary.ChatEnd = ts
	}
}

func parseAgySubagentTranscript(transcriptPath, parentCallID, convID string) (SubagentTraceSummary, bool) {
	if transcriptPath == "" {
		return SubagentTraceSummary{}, false
	}
	f, err := os.Open(transcriptPath)
	if err != nil {
		return SubagentTraceSummary{}, false
	}
	defer closeStreamQuietly(f, "subagent transcript")

	summary := SubagentTraceSummary{
		ParentCallID:   parentCallID,
		ConversationID: convID,
		Status:         "ok",
	}
	var queue []pendingSubagentStep
	var sawAny bool

	sc := newTranscriptScanner(f)
	for sc.Scan() {
		var line agyTranscriptLine
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			continue
		}
		sawAny = true
		var stepTime time.Time
		if parsedTime, parseErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(line.CreatedAt)); parseErr == nil {
			stepTime = parsedTime
		}
		recordSubagentTimeBounds(&summary, stepTime)

		if line.Type == "PLANNER_RESPONSE" {
			if m := strings.TrimSpace(line.Model); m != "" {
				summary.Model = m
			}
			addAgyUsage(&summary.Usage, extractAgyLineUsage(line))
			if strings.EqualFold(line.Status, statusUpperError) {
				summary.Status = statusError
			}
			for _, tc := range line.ToolCalls {
				cTool, mcpSrv := ExtractMCPToolInfo(tc.Name, tc.Args)
				queue = append(queue, pendingSubagentStep{name: cTool, mcpServer: mcpSrv, startTime: stepTime})
			}
			continue
		}
		if len(queue) > 0 && line.Type != "USER_INPUT" && line.Type != "SYSTEM_MESSAGE" {
			pending := queue[0]
			queue = queue[1:]
			summary.Steps = append(summary.Steps, buildSubagentToolStep(pending, line, stepTime))
			recordSubagentTimeBounds(&summary, summary.Steps[len(summary.Steps)-1].EndTime)
		}
	}
	for _, rem := range queue {
		st := clampSpanTime(rem.startTime, summary.ChatStart)
		summary.Steps = append(summary.Steps, SubagentToolStep{
			Name:      rem.name,
			MCPServer: rem.mcpServer,
			Status:    toolStatusAbortedOnTurnClose,
			StartTime: st,
			EndTime:   clampSpanTime(summary.ChatEnd, st),
		})
	}
	return summary, sawAny
}

func buildSubagentToolStep(pending pendingSubagentStep, line agyTranscriptLine, stepTime time.Time) SubagentToolStep {
	createdAt, completedAt := parseStepOutputTimestamps(line.Content)
	start := pending.startTime
	if start.IsZero() {
		start = createdAt
	} else if !createdAt.IsZero() && createdAt.After(start) {
		start = createdAt
	}
	if start.IsZero() {
		start = stepTime
	}
	end := completedAt
	if end.IsZero() {
		end = stepTime
	}
	if end.Before(start) {
		end = start
	}
	stStatus := "ok"
	if strings.EqualFold(line.Status, statusUpperError) {
		stStatus = statusError
	}
	return SubagentToolStep{
		Name:      pending.name,
		MCPServer: pending.mcpServer,
		Status:    stStatus,
		StartTime: start,
		EndTime:   end,
	}
}

func (d *StreamingDaemon) resolveAgyCompletedToolCall(activeTurn *TurnContext, rec inFlightToolCall, toolEndStatus string) ToolCall {
	call := rec.completion()
	if !call.Subagent || toolEndStatus != "ok" || d.cfg.IsClaude() || d.cfg.IsCodex() || d.cfg.IsMuse() {
		return call
	}
	parentTranscript := d.agyTranscriptPath(d.SessionID())
	convID, subTranscript := extractAgySubagentFromParentTranscript(parentTranscript, rec.stepIndex)
	if convID == "" {
		return call
	}
	if subTranscript == "" {
		subTranscript = d.agyTranscriptPath(convID)
	}
	call.SubagentConversationID = convID
	d.inflightMu.Lock()
	activeTurn.pendingAgySubagents = append(activeTurn.pendingAgySubagents, pendingAgySubagent{
		parentCallID:   call.ID,
		conversationID: convID,
		transcriptPath: subTranscript,
		stepIndex:      rec.stepIndex,
	})
	d.inflightMu.Unlock()
	return call
}

func (d *StreamingDaemon) probePendingAgySubagentsOnStep(activeTurn *TurnContext, f stepUpdateFields) {
	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}
	asyncSink, ok := activeTurn.Sink.(AsyncSubagentSink)
	if !ok {
		return
	}
	d.inflightMu.Lock()
	if len(activeTurn.pendingAgySubagents) == 0 {
		d.inflightMu.Unlock()
		return
	}
	if f.hasStepIdx {
		if f.stepIdx <= activeTurn.lastAgyProbeStep {
			d.inflightMu.Unlock()
			return
		}
		activeTurn.lastAgyProbeStep = f.stepIdx
	}
	pending := append([]pendingAgySubagent(nil), activeTurn.pendingAgySubagents...)
	d.inflightMu.Unlock()

	parentTranscript := d.agyTranscriptPath(d.SessionID())
	var completedIDs []string
	var summaries []SubagentTraceSummary
	for _, sub := range pending {
		if f.hasStepIdx && f.stepIdx <= sub.stepIndex {
			continue
		}
		if !parentTranscriptHasSubagentSender(parentTranscript, sub.conversationID, sub.stepIndex) {
			continue
		}
		summary, parsed := parseAgySubagentTranscript(sub.transcriptPath, sub.parentCallID, sub.conversationID)
		if !parsed {
			summary = SubagentTraceSummary{ParentCallID: sub.parentCallID, ConversationID: sub.conversationID, Status: "ok"}
		}
		completedIDs = append(completedIDs, sub.conversationID)
		summaries = append(summaries, summary)
	}
	if len(completedIDs) == 0 {
		return
	}
	d.removeCompletedPendingAgySubagents(activeTurn, completedIDs)
	for _, s := range summaries {
		asyncSink.CompleteAsyncSubagent(s)
	}
}

func (d *StreamingDaemon) removeCompletedPendingAgySubagents(activeTurn *TurnContext, completedIDs []string) {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	kept := activeTurn.pendingAgySubagents[:0]
	for _, sub := range activeTurn.pendingAgySubagents {
		if !slices.Contains(completedIDs, sub.conversationID) {
			kept = append(kept, sub)
		}
	}
	activeTurn.pendingAgySubagents = kept
}

func (d *StreamingDaemon) completePendingAgySubagents(activeTurn *TurnContext) {
	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}
	asyncSink, ok := activeTurn.Sink.(AsyncSubagentSink)
	if !ok {
		return
	}
	d.inflightMu.Lock()
	pending := activeTurn.pendingAgySubagents
	activeTurn.pendingAgySubagents = nil
	d.inflightMu.Unlock()

	for _, sub := range pending {
		if summary, parsed := parseAgySubagentTranscript(sub.transcriptPath, sub.parentCallID, sub.conversationID); parsed {
			asyncSink.CompleteAsyncSubagent(summary)
		}
	}
}
