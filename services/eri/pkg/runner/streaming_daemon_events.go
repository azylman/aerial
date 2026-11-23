package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

type stepUpdateFields struct {
	su            map[string]any
	toolName      string
	cmdName       string
	toolParams    map[string]any
	stepIdx       int
	hasStepIdx    bool
	isToolStart   bool
	isToolEnd     bool
	isSubagent    bool
	toolEndStatus string
}

// toolCall builds the sink identity of an agy tool step. agy has no call IDs, so the step
// index stands in when present; otherwise sinks correlate FIFO per tool name.
func (f stepUpdateFields) toolCall(name string) ToolCall {
	call := ToolCall{Name: name, Subagent: f.isSubagent || isAgySubagentTool(f.toolName)}
	if f.hasStepIdx {
		call.ID = fmt.Sprintf("step-%d", f.stepIdx)
	}
	return call
}

func isAgySubagentTool(toolName string) bool {
	return strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_") == canonicalToolSubagent
}

// isClaudeSubagentTool reports whether a Claude tool_use spawns a subagent.
func isClaudeSubagentTool(toolName string) bool {
	return toolName == "Agent" || toolName == "Task"
}

func extractToolInfoParams(su map[string]any) map[string]any {
	if info, ok := su["tool_info"].(map[string]any); ok {
		if params, ok := info["parameters"].(map[string]any); ok {
			return params
		}
	}
	return nil
}

func extractFirstToolCallEntry(su map[string]any, currentToolName string, currentParams map[string]any) (string, map[string]any) {
	tcs, ok := su["tool_calls"].([]any)
	if !ok || len(tcs) == 0 {
		return currentToolName, currentParams
	}
	firstTc, ok := tcs[0].(map[string]any)
	if !ok {
		return currentToolName, currentParams
	}
	toolName := currentToolName
	if n := extractFirstString(firstTc, "name"); n != "" && toolName == "" {
		toolName = n
	}
	toolParams := currentParams
	if args, ok := firstTc["args"].(map[string]any); ok && toolParams == nil {
		toolParams = args
	}
	return toolName, toolParams
}

// ExtractToolDetail extracts the primary target or summary detail from a tool's parameters.
func ExtractToolDetail(toolName string, params map[string]any, fallbackCmd string) string {
	if len(params) == 0 {
		return fallbackCmd
	}
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_"))
	switch clean {
	case "edit_file", "write_file", "read_file", "view_file", "replace_file_content":
		if p := extractFirstString(params, "Path", "path", "filePath", "file_path", "target_file", "AbsolutePath", "Target", "target"); p != "" {
			return filepath.Base(p)
		}
	case "run_command", "execute_command", canonicalToolBash, "sh":
		if ts := extractFirstString(params, "toolSummary", "tool_summary", "summary"); ts != "" {
			return ts
		}
		if cl := extractFirstString(params, "CommandLine", "command", "cmd"); cl != "" {
			if cmd := ExtractCommandName(cl); cmd != "" {
				return cmd
			}
			return ""
		}
	case toolCallMCPTool:
		if tn := extractFirstString(params, "ToolName", "tool_name", "tool"); tn != "" {
			return tn
		}
	case "grep_search", "find_by_name", "search_web", "web_search":
		if q := extractFirstString(params, "Query", "query", "Pattern", "pattern"); q != "" {
			return q
		}
	case canonicalToolSubagent, "define_subagent":
		if r := extractFirstString(params, "Role", "role", "TypeName", "type_name", "Name", "name"); r != "" {
			return r
		}
	case "manage_task":
		if a := extractFirstString(params, "Action", "action"); a != "" {
			return a
		}
	}

	if ts := extractFirstString(params, "toolSummary", "tool_summary"); ts != "" {
		return ts
	}
	if p := extractFirstString(params, "Path", "path", "filePath", "file_path"); p != "" {
		return filepath.Base(p)
	}
	if cl := extractFirstString(params, "CommandLine", "command", "cmd"); cl != "" {
		if cmd := ExtractCommandName(cl); cmd != "" {
			return cmd
		}
		return ""
	}
	return fallbackCmd
}

func applyStepUpdateSubmap(f *stepUpdateFields, su map[string]any) {
	f.su = su
	state := strings.ToUpper(extractFirstString(su, "state", "status"))
	typ := strings.ToLower(extractFirstString(su, fieldType, "step_type"))
	f.stepIdx, f.hasStepIdx = extractStepIndex(su)
	if tn := extractFirstString(su, "tool_name"); tn != "" {
		f.toolName = tn
	}
	if params := extractToolInfoParams(su); params != nil {
		f.toolParams = params
	}
	f.toolName, f.toolParams = extractFirstToolCallEntry(su, f.toolName, f.toolParams)
	f.cmdName = ExtractToolDetail(f.toolName, f.toolParams, f.cmdName)
	switch state {
	case statusUpperDone:
		f.toolEndStatus = "ok"
	case statusUpperError:
		f.toolEndStatus = statusError
	}
	f.isSubagent = typ == "subagent"
	if typ == "tool_call" || typ == "tool" || typ == "subagent" {
		f.isToolStart = f.toolEndStatus == ""
		f.isToolEnd = f.toolEndStatus != ""
	}
}

func parseStepUpdateEvent(raw map[string]any) stepUpdateFields {
	var f stepUpdateFields
	if tc, ok := raw["tool_call"].(map[string]any); ok {
		if n, ok := tc["name"].(string); ok {
			f.toolName = n
		}
		if c, ok := tc["command"].(string); ok {
			f.cmdName = c
		}
		f.isToolStart = true
	}
	if m, ok := raw["step_update"].(map[string]any); ok && m != nil {
		applyStepUpdateSubmap(&f, m)
	}
	if !f.hasStepIdx {
		f.stepIdx, f.hasStepIdx = extractStepIndex(raw)
	}
	if f.cmdName == "" && len(f.toolParams) > 0 {
		f.cmdName = ExtractToolDetail(f.toolName, f.toolParams, "")
	}
	return f
}

func (d *StreamingDaemon) notifyStepBoundary(activeTurn *TurnContext, stepIdx int) {
	d.inflightMu.Lock()
	isNewStep := !activeTurn.hasSeenStep || stepIdx != activeTurn.lastStepIndex
	if isNewStep {
		activeTurn.hasSeenStep, activeTurn.lastStepIndex = true, stepIdx
	}
	d.inflightMu.Unlock()
	if isNewStep {
		if sa, ok := activeTurn.Sink.(StepAwareSink); ok {
			sa.OnStepStarted(stepIdx)
		}
	}
}

func isShellReadCommand(cmd string) bool {
	switch cmd {
	case "cat", "head", "tail", "sed", "less", "more", "bat":
		return true
	default:
		return false
	}
}

func extractActivatedSkill(toolName string, toolParams map[string]any, cmdName string) (string, bool) {
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_"))
	switch clean {
	case "edit_file", "write_file", "replace_file_content", "multi_replace_file_content", "delete_file", "grep_search", "find_by_name", toolCallMCPTool:
		return "", false
	case "run_command", "execute_command", canonicalToolBash, "sh":
		cl := extractFirstString(toolParams, "CommandLine", "command_line", "command", "cmd")
		if isShellReadCommand(ExtractCommandName(cl)) {
			return ExtractSkillFromTarget(cl)
		}
		return "", false
	}
	if p := extractFirstString(toolParams, "Path", "path", "filePath", "file_path", "target_file", "AbsolutePath", "Target", "target"); p != "" {
		if skill, ok := ExtractSkillFromTarget(p); ok {
			return skill, true
		}
	}
	if cmdName != "" {
		return ExtractSkillFromTarget(cmdName)
	}
	return "", false
}

// recordToolStart registers call (whose Name is the canonical tool name) as in flight.
func (d *StreamingDaemon) recordToolStart(stepIdx int, hasStepIdx bool, call ToolCall, mcpServer string) bool {
	var isNewToolStart bool
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if d.inFlightTools == nil {
		d.inFlightTools = make(map[int]inFlightToolCall)
	}
	if hasStepIdx {
		if _, exists := d.inFlightTools[stepIdx]; !exists {
			d.inFlightTools[stepIdx] = inFlightToolCall{stepIndex: stepIdx, call: call, mcpServer: mcpServer, startedAt: time.Now()}
			isNewToolStart = true
		}
	} else {
		alreadyInFlight := false
		for _, rec := range d.inFlightTools {
			if rec.call.Name == call.Name {
				alreadyInFlight = true
				break
			}
		}
		if !alreadyInFlight {
			d.inFlightTools[-1] = inFlightToolCall{stepIndex: -1, call: call, mcpServer: mcpServer, startedAt: time.Now()}
			isNewToolStart = true
		}
	}
	return isNewToolStart
}

func matchFallbackInFlightTool(tools map[int]inFlightToolCall, toolName string, params map[string]any) (int, inFlightToolCall, bool) {
	cTool, _ := ExtractMCPToolInfo(toolName, params)
	for idx, rec := range tools {
		if rec.call.Name == toolName || rec.call.Name == cTool || toolName == toolCallMCPTool || len(tools) == 1 {
			return idx, rec, true
		}
	}
	return 0, inFlightToolCall{}, false
}

func (d *StreamingDaemon) recordToolEnd(f stepUpdateFields) (inFlightToolCall, bool, bool) {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if d.inFlightTools == nil {
		return inFlightToolCall{}, false, f.isToolEnd
	}

	var completedRecord inFlightToolCall
	var hasCompletedRecord bool
	isToolEnd := f.isToolEnd

	if f.hasStepIdx {
		if rec, exists := d.inFlightTools[f.stepIdx]; exists {
			completedRecord, hasCompletedRecord, isToolEnd = rec, true, true
			delete(d.inFlightTools, f.stepIdx)
		}
	}
	if isToolEnd && !hasCompletedRecord && len(d.inFlightTools) > 0 {
		if idx, rec, ok := matchFallbackInFlightTool(d.inFlightTools, f.toolName, f.toolParams); ok {
			completedRecord, hasCompletedRecord = rec, true
			delete(d.inFlightTools, idx)
		}
	}
	return completedRecord, hasCompletedRecord, isToolEnd
}

func extractStepTextDelta(raw, su map[string]any) string {
	for _, m := range []map[string]any{raw, su} {
		if m == nil {
			continue
		}
		for _, k := range []string{"delta", "text_delta"} {
			if d, ok := m[k].(string); ok && d != "" {
				return d
			}
		}
	}
	return ""
}

func hasThinkingField(raw, su map[string]any) bool {
	if _, ok := raw["thinking"]; ok {
		return true
	}
	if su != nil {
		if _, ok := su["thinking"]; ok {
			return true
		}
	}
	return false
}

func (d *StreamingDaemon) handleStepUpdateEvent(activeTurn *TurnContext, raw map[string]any) {
	d.mu.Lock()
	d.stepCount++
	d.mu.Unlock()
	if activeTurn.Sink == nil {
		return
	}

	f := parseStepUpdateEvent(raw)
	if f.hasStepIdx {
		d.notifyStepBoundary(activeTurn, f.stepIdx)
	}

	var (
		canonicalTool      string
		activatedSkill     string
		hasSkill           bool
		completedRecord    inFlightToolCall
		hasCompletedRecord bool
		isToolEnd          = f.isToolEnd
		isNewToolStart     bool
		mcpCallServer      string
		mcpCallTool        string
	)

	if f.isToolStart {
		var mcpServer string
		canonicalTool, mcpServer = ExtractMCPToolInfo(f.toolName, f.toolParams)
		activatedSkill, hasSkill = extractActivatedSkill(f.toolName, f.toolParams, f.cmdName)
		isNewToolStart = d.recordToolStart(f.stepIdx, f.hasStepIdx, f.toolCall(canonicalTool), mcpServer)
		mcpCallServer, mcpCallTool = mcpCallTarget(canonicalTool, mcpServer)
	} else if f.isToolEnd || (f.hasStepIdx && f.toolEndStatus != "") {
		completedRecord, hasCompletedRecord, isToolEnd = d.recordToolEnd(f)
	}

	if isNewToolStart && (canonicalTool != "" || f.cmdName != "") {
		toolForSink := canonicalTool
		cmdForSink := f.cmdName
		if f.toolName == toolCallMCPTool {
			toolForSink = toolCallMCPTool
		} else if canonicalTool == cmdForSink {
			cmdForSink = ""
		}
		call := f.toolCall(toolForSink)
		call.Command = cmdForSink
		call.MCPServer, call.MCPTool = mcpCallServer, mcpCallTool
		activeTurn.Sink.OnToolCall(call)
	}
	if isNewToolStart && hasSkill {
		activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
	}
	if isToolEnd && hasCompletedRecord {
		activeTurn.Sink.OnToolCompleted(completedRecord.completion(), completedRecord.mcpServer, time.Since(completedRecord.startedAt), f.toolEndStatus)
	}
	if delta := extractStepTextDelta(raw, f.su); delta != "" {
		activeTurn.Sink.OnTextDelta(delta)
	}
	if hasThinkingField(raw, f.su) {
		activeTurn.Sink.OnThinking()
	}
}

func extractErrorsArray(errs []any) string {
	var errStrs []string
	for _, e := range errs {
		if es, ok := e.(string); ok && strings.TrimSpace(es) != "" {
			errStrs = append(errStrs, es)
			continue
		}
		if em, ok := e.(map[string]any); ok {
			if msg := extractFirstString(em, "message", fieldError, fieldText); msg != "" {
				errStrs = append(errStrs, msg)
			}
		}
	}
	return strings.Join(errStrs, "; ")
}

func isClaudeResultError(raw, resObj map[string]any) bool {
	if b, ok := raw["is_error"].(bool); ok && b {
		return true
	}
	if resObj != nil {
		if b, ok := resObj["is_error"].(bool); ok && b {
			return true
		}
	}
	sub := extractFirstString(raw, "subtype")
	if sub == "" && resObj != nil {
		sub = extractFirstString(resObj, "subtype")
	}
	return strings.HasPrefix(strings.ToLower(sub), subtypeErrorPrefix)
}

func extractResultErrors(raw, resObj map[string]any, isErr bool) string {
	if isErr {
		if s, ok := raw[fieldResult].(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	for _, m := range []map[string]any{resObj, raw} {
		if m == nil {
			continue
		}
		if errs, ok := m["errors"].([]any); ok && len(errs) > 0 {
			if joined := extractErrorsArray(errs); joined != "" {
				return joined
			}
		}
	}
	return ""
}

func extractResultStatusAndError(raw map[string]any) (status, errMsg string) {
	var resObj map[string]any
	if r, ok := raw[fieldResult].(map[string]any); ok {
		resObj = r
	}
	for _, m := range []map[string]any{resObj, raw} {
		if m == nil {
			continue
		}
		if status == "" {
			status = extractFirstString(m, "status")
		}
		if errMsg == "" {
			errMsg = extractFirstString(m, fieldError)
		}
	}

	isErr := isClaudeResultError(raw, resObj)
	if isErr && status == "" {
		status = statusUpperError
	}
	if errMsg == "" {
		errMsg = extractResultErrors(raw, resObj, isErr)
	}

	return status, errMsg
}

func (d *StreamingDaemon) tryRescueTurnOnResult(activeTurn *TurnContext, raw map[string]any, errMsg, currentModel string) bool {
	if d.cfg.TranscriptRescuer == nil || d.SessionID() == "" {
		return false
	}
	rescued := d.rescueTranscript(activeTurn)
	if rescued == "" || !IsSubstantiveResponse(rescued) || strings.HasPrefix(rescued, "[Tool Call Requested]:") {
		return false
	}
	log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after error/empty result: %s", d.SessionID(), rescued)
	recordStreamErrorMetric(errMsg, currentModel, false)
	res := &TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
	populateUsage(res, raw)
	d.fillStreamUsage(activeTurn, res)
	activeTurn.Sink.OnResult(res)
	return true
}

func (d *StreamingDaemon) handleResultError(activeTurn *TurnContext, respStr, status, errMsg, currentModel string) bool {
	if !strings.EqualFold(status, statusUpperError) {
		return false
	}
	if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") {
		recordStreamErrorMetric(errMsg, currentModel, true)
		if errMsg == "" {
			errMsg = "daemon execution failed"
		}
		d.failTurn(activeTurn, errors.New(errMsg))
		return true
	}
	if errMsg != "" {
		log.Printf("[StreamingDaemon] Notice: daemon reported error with substantive response: %s", errMsg)
		recordStreamErrorMetric(errMsg, currentModel, false)
	}
	return false
}

func (d *StreamingDaemon) deliverResultToSink(activeTurn *TurnContext, raw map[string]any) {
	currentModel := d.cfg.Model
	if activeTurn != nil && strings.TrimSpace(activeTurn.Model) != "" {
		currentModel = strings.TrimSpace(activeTurn.Model)
	}
	if currentModel == "" {
		currentModel = defaultModelLabel
	}

	respStr := extractResponseString(raw)
	status, errMsg := extractResultStatusAndError(raw)

	if err := d.syntheticOnlyReplyError(activeTurn); err != nil {
		recordStreamErrorMetric(err.Error(), currentModel, true)
		d.failTurn(activeTurn, err)
		return
	}

	if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") || strings.EqualFold(status, statusUpperError) {
		if d.tryRescueTurnOnResult(activeTurn, raw, errMsg, currentModel) {
			return
		}
		if d.handleResultError(activeTurn, respStr, status, errMsg, currentModel) {
			return
		}
	}

	res := &TurnResult{ConversationID: d.SessionID(), Response: respStr, Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
	populateUsage(res, raw)
	d.fillStreamUsage(activeTurn, res)
	activeTurn.Sink.OnResult(res)
}

func (d *StreamingDaemon) handleResultEvent(raw map[string]any) {
	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
		d.inflight = d.inflight[1:]
	}
	hasMoreInflight := len(d.inflight) > 0
	dangling := d.drainInFlightToolsLocked()
	d.inflightMu.Unlock()

	for _, rec := range dangling {
		if activeTurn != nil && activeTurn.Sink != nil {
			activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), "aborted")
		}
	}

	d.mu.Lock()
	d.turnCount++
	d.lastUsed = time.Now()
	if !hasMoreInflight && d.state != StateClosed {
		d.state = StateReady
	}
	var turnFinishedCb func(d *StreamingDaemon)
	if !hasMoreInflight {
		turnFinishedCb = d.onTurnFinished
	}
	d.mu.Unlock()

	if turnFinishedCb != nil {
		go turnFinishedCb(d)
	}
	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}

	d.deliverResultToSink(activeTurn, raw)
}

// recordDaemonInit marks Claude's init message on the in-flight turn's span: StartStreamingDaemon
// returns before Claude has connected its MCP servers, so this event closes that gap in the trace.
func (d *StreamingDaemon) recordDaemonInit() {
	d.inflightMu.Lock()
	var turn *TurnContext
	if len(d.inflight) > 0 {
		turn = d.inflight[0]
	}
	d.inflightMu.Unlock()
	if turn != nil && turn.Ctx != nil {
		trace.SpanFromContext(turn.Ctx).AddEvent(eventDaemonInit)
	}
}

func (d *StreamingDaemon) dispatchNDJSONLine(line string) {
	trimmed := strings.TrimSpace(line)
	switch {
	case trimmed == "":
		return
	case d.cfg.IsCodex():
		d.dispatchCodexJSONRPCLine(trimmed)
		return
	case d.cfg.IsMuse():
		d.dispatchMuseJSONRPCLine(trimmed)
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed NDJSON line: %v", err)
		return
	}

	event, ok := raw["event"].(string)
	if !ok {
		event, ok = raw[fieldType].(string)
	}
	if !ok {
		log.Printf("[StreamingDaemon] NDJSON line missing string event/type field: %v", raw)
		return
	}

	// Handle Claude "system" init event before inflight check, as it may latch the session ID
	if event == "system" {
		if extractFirstString(raw, "subtype") == eventInit {
			if sessID := extractFirstString(raw, "session_id"); sessID != "" && IsValidUUID(sessID) {
				d.mu.Lock()
				d.sessionID = sessID
				d.mu.Unlock()
			}
			d.recordDaemonInit()
		}
		return
	}

	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
	}
	d.inflightMu.Unlock()
	if activeTurn == nil {
		return
	}

	switch event {
	case "step_update":
		d.handleStepUpdateEvent(activeTurn, raw)
	case eventResult:
		d.handleResultEvent(raw)
	case "stream_event":
		d.handleClaudeStreamEvent(activeTurn, raw)
	case "assistant":
		d.handleClaudeAssistantEvent(activeTurn, raw)
	case eventUser:
		d.handleClaudeUserEvent(activeTurn, raw)
	}
}

func (d *StreamingDaemon) handleClaudeStreamEvent(activeTurn *TurnContext, raw map[string]any) {
	ev, ok := raw["event"].(map[string]any)
	if !ok || ev == nil {
		return
	}
	d.recordStreamEventUsage(activeTurn, extractFirstString(raw, "parent_tool_use_id"), ev)
	switch extractFirstString(ev, fieldType) {
	case "message_start":
		d.mu.Lock()
		d.stepCount++
		stepIdx := d.stepCount
		d.mu.Unlock()
		d.notifyStepBoundary(activeTurn, stepIdx)
	case "content_block_delta":
		if delta, ok := ev["delta"].(map[string]any); ok && delta != nil {
			switch extractFirstString(delta, fieldType) {
			case "text_delta":
				if text := extractFirstString(delta, fieldText); text != "" && activeTurn.Sink != nil {
					activeTurn.Sink.OnTextDelta(text)
				}
			case "thinking_delta":
				if activeTurn.Sink != nil {
					activeTurn.Sink.OnThinking()
				}
			}
		}
	}
}

func (d *StreamingDaemon) handleClaudeAssistantEvent(activeTurn *TurnContext, raw map[string]any) {
	msg, ok := raw["message"].(map[string]any)
	if !ok || msg == nil {
		return
	}
	d.recordStreamUsage(activeTurn, msg)
	contents, ok := msg[fieldContent].([]any)
	d.recordAssistantOrigin(activeTurn, raw, msg, contents)
	if !ok {
		return
	}
	// Messages emitted by a subagent carry the tool_use id of the Agent/Task call that spawned it.
	parentToolUseID := extractFirstString(raw, "parent_tool_use_id")

	for _, block := range contents {
		bMap, ok := block.(map[string]any)
		if !ok || bMap == nil {
			continue
		}
		if extractFirstString(bMap, fieldType) != "tool_use" {
			continue
		}

		toolID := extractFirstString(bMap, "id")
		toolName := extractFirstString(bMap, "name")
		var inputMap map[string]any
		if in, ok := bMap["input"].(map[string]any); ok {
			inputMap = in
		}

		d.mu.Lock()
		d.stepCount++
		stepIdx := d.stepCount
		d.mu.Unlock()

		d.inflightMu.Lock()
		if d.claudeToolIDs == nil {
			d.claudeToolIDs = make(map[string]int)
		}
		if toolID != "" {
			d.claudeToolIDs[toolID] = stepIdx
		}
		d.inflightMu.Unlock()

		d.notifyStepBoundary(activeTurn, stepIdx)

		canonicalTool, mcpServer := ExtractMCPToolInfo(toolName, inputMap)
		cmdDetail := ExtractToolDetail(toolName, inputMap, "")
		activatedSkill, hasSkill := extractActivatedSkill(toolName, inputMap, cmdDetail)

		call := ToolCall{ID: toolID, ParentID: parentToolUseID, Name: canonicalTool, Subagent: isClaudeSubagentTool(toolName)}
		isNewStart := d.recordToolStart(stepIdx, true, call, mcpServer)
		call.MCPServer, call.MCPTool = mcpCallTarget(canonicalTool, mcpServer)
		if isNewStart && activeTurn.Sink != nil {
			toolForSink := canonicalTool
			cmdForSink := cmdDetail
			if toolName == toolCallMCPTool {
				toolForSink = toolCallMCPTool
			} else if canonicalTool == cmdForSink {
				cmdForSink = ""
			}
			call.Name, call.Command = toolForSink, cmdForSink
			activeTurn.Sink.OnToolCall(call)
			if hasSkill {
				activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
			}
		}
	}
}

// recordAssistantOrigin notes whether an assistant message came from a model call or was written by
// the Claude CLI itself (message.model == ClaudeSyntheticModel).
func (d *StreamingDaemon) recordAssistantOrigin(activeTurn *TurnContext, raw, msg map[string]any, contents []any) {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if extractFirstString(msg, "model") != ClaudeSyntheticModel {
		activeTurn.sawModelReply = true
		return
	}
	var parts []string
	for _, block := range contents {
		if bMap, ok := block.(map[string]any); ok && extractFirstString(bMap, fieldType) == blockTypeText {
			parts = append(parts, extractFirstString(bMap, fieldText))
		}
	}
	activeTurn.syntheticReply = &claudeSyntheticReply{text: strings.TrimSpace(strings.Join(parts, "\n")), errType: extractFirstString(raw, fieldError)}
}

// syntheticOnlyReplyError returns the turn's failure when its only assistant output was written by
// the Claude CLI without a model call, so it is never counted (or rescued) as a reply.
func (d *StreamingDaemon) syntheticOnlyReplyError(activeTurn *TurnContext) error {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	reply := activeTurn.syntheticReply
	if reply == nil || activeTurn.sawModelReply {
		return nil
	}
	errType := reply.errType
	if errType == "" {
		errType = "none"
	}
	return fmt.Errorf("claude synthetic reply (error=%s): %s", errType, reply.text)
}

func (d *StreamingDaemon) handleClaudeUserEvent(activeTurn *TurnContext, raw map[string]any) {
	msg, ok := raw["message"].(map[string]any)
	if !ok || msg == nil {
		return
	}
	contents, ok := msg[fieldContent].([]any)
	if !ok {
		return
	}

	for _, block := range contents {
		bMap, ok := block.(map[string]any)
		if !ok || bMap == nil {
			continue
		}
		if extractFirstString(bMap, fieldType) != "tool_result" {
			continue
		}

		toolUseID := extractFirstString(bMap, "tool_use_id")
		status := "ok"
		if isErr, ok := bMap["is_error"].(bool); ok && isErr {
			status = statusError
		}

		d.inflightMu.Lock()
		stepIdx, hasStep := d.claudeToolIDs[toolUseID]
		if hasStep {
			delete(d.claudeToolIDs, toolUseID)
		}
		d.inflightMu.Unlock()

		fields := stepUpdateFields{
			isToolEnd:     true,
			toolEndStatus: status,
			hasStepIdx:    hasStep,
			stepIdx:       stepIdx,
		}
		rec, hasRec, isEnd := d.recordToolEnd(fields)
		if isEnd && hasRec && activeTurn.Sink != nil {
			activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), status)
		}
	}
}

func extractFirstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func extractStepIndex(m map[string]any) (int, bool) {
	if idx, ok := m["step_index"].(float64); ok {
		return int(idx), true
	}
	if idx, ok := m["step_index"].(int); ok {
		return idx, true
	}
	return 0, false
}

func extractResponseString(raw map[string]any) string {
	// If it's an error result, the "result" string is the error, not the substantive response.
	isErr := false
	if b, ok := raw["is_error"].(bool); ok && b {
		isErr = true
	}
	if sub := extractFirstString(raw, "subtype"); strings.HasPrefix(strings.ToLower(sub), subtypeErrorPrefix) {
		isErr = true
	}

	keys := []string{fieldResponse, fieldResult, fieldContent, fieldText}
	if resObj, ok := raw[fieldResult].(map[string]any); ok {
		if b, ok := resObj["is_error"].(bool); ok && b {
			isErr = true
		}
		if sub := extractFirstString(resObj, "subtype"); strings.HasPrefix(strings.ToLower(sub), subtypeErrorPrefix) {
			isErr = true
		}
		for _, k := range keys {
			if resp, ok := resObj[k].(string); ok && strings.TrimSpace(resp) != "" {
				return resp
			}
		}
	}
	for _, k := range []string{fieldResponse, fieldResult, fieldContent, fieldText} {
		if k == fieldResult && isErr {
			continue
		}
		if resp, ok := raw[k].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
	}
	return ""
}

func recordStreamErrorMetric(errMsg, model string, recordProcessErr bool) {
	if errMsg == "" && !recordProcessErr {
		return
	}
	switch {
	case IsCapacityBlip(errMsg, ""):
		metrics.RecordRunnerError("capacity_throttle", model)
	case IsQuotaPause(errMsg, ""):
		metrics.RecordRunnerError("quota_paused", model)
	case recordProcessErr:
		metrics.RecordRunnerError("process_error", model)
	}
}
