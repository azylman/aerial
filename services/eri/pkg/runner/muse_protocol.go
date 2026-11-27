package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const museApprovalAllowAll = "allowAll"

func isUUIDv7(s string) bool {
	u, err := uuid.Parse(strings.TrimSpace(s))
	return err == nil && u.Version() == 7
}

func uuidV7() string {
	u, err := uuid.NewV7()
	if err != nil {
		return uuid.New().String()
	}
	return u.String()
}

func extractMuseSessionID(resultRaw json.RawMessage, fallback string) string {
	if len(resultRaw) == 0 {
		return fallback
	}
	var res map[string]any
	if err := json.Unmarshal(resultRaw, &res); err != nil {
		return fallback
	}
	if sessionObj, ok := res["session"].(map[string]any); ok && sessionObj != nil {
		if id := extractFirstString(sessionObj, "sessionId", "session_id", "id"); id != "" {
			return id
		}
	}
	if id := extractFirstString(res, "sessionId", "session_id", "id"); id != "" {
		return id
	}
	return fallback
}

// MuseResumeError reports a session/resume RPC rejection that is not a definite
// not-found, so the caller retries instead of starting a fresh session and the
// failure propagates retryably through AcquireLease.
type MuseResumeError struct {
	Code    int
	Message string
}

func (e *MuseResumeError) Error() string {
	if e == nil {
		return "muse session/resume rejected"
	}
	return fmt.Sprintf("muse session/resume rejected (code %d: %s)", e.Code, e.Message)
}

// isMuseSessionNotFound reports whether a resume rejection definitely means the
// session is gone (including an abandoned incomplete start that never produced
// a session), in which case the caller may start a fresh session.
func isMuseSessionNotFound(msg string) bool {
	lowered := strings.ToLower(strings.TrimSpace(msg))
	for _, sub := range []string{"not found", "unknown session", "no such session", "session expired", "session does not exist", "abandon"} {
		if strings.Contains(lowered, sub) {
			return true
		}
	}
	return false
}

func (d *StreamingDaemon) tryResumeMuseSession(ctx context.Context, reader *bufio.Reader, deadline time.Time, commandID, trimmedSess, baseModel string) (string, bool, error) {
	if !d.cfg.ResumeSession || trimmedSess == "" {
		return "", false, nil
	}
	resumeParams := map[string]any{
		fieldCommandID: commandID,
		fieldSessionID: trimmedSess,
		"excludeItems": true,
	}
	if baseModel != "" {
		resumeParams["modelId"] = baseModel
	}
	resumeReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      2,
		Method:  "session/resume",
		Params:  resumeParams,
	}
	if err := d.sendJSONRPCRequest(resumeReq, "session/resume"); err != nil {
		log.Printf("eri: [muse] session/resume request failed: %v", err)
		return "", false, nil
	}
	resp, err := d.awaitCodexRPCResponse(ctx, reader, deadline, 2)
	if err != nil {
		log.Printf("eri: [muse] session/resume await failed: %v", err)
		return "", false, nil
	}
	if resp.Error != nil {
		if isMuseSessionNotFound(resp.Error.Message) {
			log.Printf("eri: [muse] session/resume rejected for %s: %s", trimmedSess, resp.Error.Message)
			return "", false, nil
		}
		return "", false, &MuseResumeError{Code: resp.Error.Code, Message: resp.Error.Message}
	}
	sessID := extractMuseSessionID(resp.Result, trimmedSess)
	approvalParams := map[string]any{
		fieldCommandID: uuidV7(),
		fieldSessionID: sessID,
		"mode":         museApprovalAllowAll,
	}
	approvalReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      3,
		Method:  "session/setApprovalMode",
		Params:  approvalParams,
	}
	if _, setErr := d.sendAndAwaitJSONRPC(ctx, reader, deadline, approvalReq, "muse", "session/setApprovalMode"); setErr != nil {
		return "", false, setErr
	}
	return sessID, true, nil
}

func (d *StreamingDaemon) waitForMuseHandshake(ctx context.Context, reader *bufio.Reader, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	initReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      1,
		Method:  rpcMethodInitialize,
		Params: map[string]any{
			rpcClientInfoKey: map[string]any{
				rpcNameKey:    "eri",
				rpcVersionKey: rpcClientVersion,
			},
		},
	}
	if _, err := d.sendAndAwaitJSONRPC(ctx, reader, deadline, initReq, "muse", "initialize"); err != nil {
		return err
	}

	initNotif := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		Method:  "initialized",
		Params:  map[string]any{},
	}
	if err := d.sendJSONRPCRequest(initNotif, "initialized notification"); err != nil {
		return err
	}

	commandID := uuidV7()
	trimmedSess := strings.TrimSpace(d.cfg.SessionID)
	baseModel, _ := parseModelAndEffort(d.cfg.Model, d.cfg.Effort, true)

	sessID, resumeSucceeded, resumeErr := d.tryResumeMuseSession(ctx, reader, deadline, commandID, trimmedSess, baseModel)
	if resumeErr != nil {
		return resumeErr
	}

	if !resumeSucceeded {
		startParams := map[string]any{
			fieldCommandID:  commandID,
			"approvalMode":  museApprovalAllowAll,
			"workspaceRoot": d.cfg.Cwd,
		}
		if baseModel != "" {
			startParams["modelId"] = baseModel
		}
		fallbackSess := ""
		if !d.cfg.ResumeSession && trimmedSess != "" && isUUIDv7(trimmedSess) {
			startParams[fieldSessionID] = trimmedSess
			fallbackSess = trimmedSess
		}
		startReq := codexRPCRequest{
			JSONRPC: codexRPCVersion,
			ID:      4,
			Method:  "session/start",
			Params:  startParams,
		}
		resp, err := d.sendAndAwaitJSONRPC(ctx, reader, deadline, startReq, "muse", "session/start")
		if err != nil {
			return err
		}
		sessID = extractMuseSessionID(resp.Result, fallbackSess)
	}

	if sessID == "" {
		sessID = trimmedSess
	}

	d.mu.Lock()
	d.sessionID = sessID
	d.state = StateReady
	d.mu.Unlock()
	return nil
}

func marshalMuseTurnStart(sessionID, payloadPrompt, effort string) ([]byte, error) {
	params := map[string]any{
		fieldCommandID: uuidV7(),
		fieldSessionID: sessionID,
		"input": []map[string]string{
			{
				fieldType: fieldText,
				fieldText: payloadPrompt,
			},
		},
	}
	if trimmedEffort := strings.TrimSpace(effort); trimmedEffort != "" {
		params["reasoningEffort"] = trimmedEffort
	}
	req := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      10,
		Method:  methodTurnStart,
		Params:  params,
	}
	return json.Marshal(req)
}

func isMuseReminderChild(params map[string]any) bool {
	if params == nil {
		return false
	}
	if extractFirstString(params, "kind") == "reminderChild" {
		return true
	}
	if item, ok := params["item"].(map[string]any); ok && item != nil {
		return extractFirstString(item, "kind") == "reminderChild"
	}
	return false
}

func extractMuseChildSessionID(params map[string]any, rootSessionID string) string {
	if params == nil {
		return ""
	}
	root := strings.TrimSpace(rootSessionID)
	if root == "" {
		return ""
	}
	if sessID := extractFirstString(params, "sessionId", "session_id"); sessID != "" && sessID != root {
		return sessID
	}
	if item, ok := params["item"].(map[string]any); ok && item != nil {
		if itemSessID := extractFirstString(item, "sessionId", "session_id"); itemSessID != "" && itemSessID != root {
			return itemSessID
		}
	}
	return ""
}

func isMuseSubagentTool(toolName string) bool {
	switch strings.ToLower(strings.TrimSpace(toolName)) {
	case itemTypeSubagent, canonicalToolSubagent, "agent", "task":
		return true
	default:
		return false
	}
}

func isMuseToolItemType(itemType string) bool {
	switch itemType {
	case itemTypeToolCall, codexToolMCP, codexToolCommandExecution, itemTypeSubagent:
		return true
	default:
		return false
	}
}

func extractMuseItemType(params map[string]any, defaultType string) string {
	if params == nil {
		return defaultType
	}
	if t := extractFirstString(params, fieldType, "kind"); t != "" {
		return t
	}
	if item, ok := params["item"].(map[string]any); ok && item != nil {
		if t := extractFirstString(item, fieldType, "kind"); t != "" {
			return t
		}
	}
	if delta, ok := params["delta"].(map[string]any); ok && delta != nil {
		if t := extractFirstString(delta, fieldType, "kind"); t != "" {
			return t
		}
	}
	return defaultType
}

func extractMuseDeltaText(params map[string]any) string {
	if params == nil {
		return ""
	}
	switch d := params["delta"].(type) {
	case string:
		return d
	case map[string]any:
		return extractFirstString(d, fieldText, fieldContent, "summary")
	}
	return extractFirstString(params, fieldText, fieldContent)
}

func extractMuseUsage(params map[string]any) (inputTokens, outputTokens, cachedTokens int) {
	if params == nil {
		return 0, 0, 0
	}
	uMap := params
	if u, ok := params["usage"].(map[string]any); ok && u != nil {
		uMap = u
	} else if tu, ok := params["tokenUsage"].(map[string]any); ok && tu != nil {
		uMap = tu
		if last, ok := tu["last"].(map[string]any); ok && last != nil {
			uMap = last
		}
	}
	getInt := func(keys ...string) int {
		for _, k := range keys {
			if v, ok := uMap[k].(float64); ok {
				return int(v)
			}
		}
		return 0
	}
	inputTokens = getInt("inputTokens", "input_tokens")
	outputTokens = getInt("outputTokens", "output_tokens")
	cachedTokens = getInt("cachedTokens", "cached_tokens", "cachedInputTokens", "cached_input_tokens")
	return inputTokens, outputTokens, cachedTokens
}

func isMuseTerminalFailure(params map[string]any, tStatus string) bool {
	if tStatus == statusFailed || tStatus == statusError || tStatus == statusCancelled {
		return true
	}
	if termBool, ok := params["terminal"].(bool); ok && termBool {
		return tStatus != "" && tStatus != "completed" && tStatus != "success" && tStatus != "done"
	}
	if termStr, ok := params["terminal"].(string); ok {
		lowerTerm := strings.ToLower(strings.TrimSpace(termStr))
		return lowerTerm == statusFailed || lowerTerm == statusError || lowerTerm == statusCancelled
	}
	return false
}

func extractMuseErrorMessage(params map[string]any) string {
	switch e := params[fieldError].(type) {
	case map[string]any:
		return extractFirstString(e, "message")
	case string:
		return e
	}
	if turnObj, ok := params["turn"].(map[string]any); ok && turnObj != nil {
		switch e := turnObj[fieldError].(type) {
		case map[string]any:
			return extractFirstString(e, "message")
		case string:
			return e
		}
	}
	return ""
}

func parseMuseTurnOutcome(params map[string]any) (string, string) {
	if params == nil {
		return statusUpperDone, ""
	}
	tStatus := strings.ToLower(strings.TrimSpace(extractFirstString(params, fieldStatus)))
	if tStatus == "" {
		if turnObj, ok := params["turn"].(map[string]any); ok && turnObj != nil {
			tStatus = strings.ToLower(strings.TrimSpace(extractFirstString(turnObj, fieldStatus)))
		}
	}

	isFailed := isMuseTerminalFailure(params, tStatus)
	errMsg := extractMuseErrorMessage(params)

	if isFailed || errMsg != "" {
		if errMsg == "" {
			if tStatus != "" {
				errMsg = "turn " + tStatus
			} else {
				errMsg = "turn failed"
			}
		}
		return statusUpperError, errMsg
	}
	return statusUpperDone, ""
}

func (d *StreamingDaemon) handleMuseTurnCompleted(activeTurn *TurnContext, params map[string]any) {
	status, errMsg := parseMuseTurnOutcome(params)

	d.inflightMu.Lock()
	responseText := activeTurn.codexResponseText
	d.inflightMu.Unlock()

	rawResult := map[string]any{
		fieldEvent:    eventResult,
		fieldStatus:   status,
		fieldResponse: responseText,
	}
	if errMsg != "" {
		rawResult[fieldError] = errMsg
	}
	d.handleResultEvent(rawResult)
}

func extractToolParamsMap(item map[string]any) map[string]any {
	var paramsMap map[string]any
	switch args := item["arguments"].(type) {
	case map[string]any:
		paramsMap = args
	case string:
		if err := json.Unmarshal([]byte(args), &paramsMap); err != nil {
			paramsMap = map[string]any{fieldRaw: args}
		}
	}
	if paramsMap == nil {
		if args, ok := item["parameters"].(map[string]any); ok {
			paramsMap = args
		}
	}
	return paramsMap
}

func (d *StreamingDaemon) sendMuseViewSubscribe(activeTurn *TurnContext, childSessID string) {
	childSessID = strings.TrimSpace(childSessID)
	if childSessID == "" || activeTurn == nil {
		return
	}
	d.inflightMu.Lock()
	if activeTurn.museSubscribedChildren == nil {
		activeTurn.museSubscribedChildren = make(map[string]bool)
	}
	if activeTurn.museSubscribedChildren[childSessID] {
		d.inflightMu.Unlock()
		return
	}
	activeTurn.museSubscribedChildren[childSessID] = true
	d.inflightMu.Unlock()

	req := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      100,
		Method:  "view/subscribe",
		Params: map[string]any{
			fieldCommandID: uuidV7(),
			fieldSessionID: childSessID,
		},
	}
	data, err := json.Marshal(req)
	if err != nil {
		return
	}
	data = append(data, '\n')

	d.stdinMu.Lock()
	defer d.stdinMu.Unlock()
	if d.stdin == nil || d.closed.Load() {
		return
	}
	if _, writeErr := d.stdin.Write(data); writeErr != nil {
		log.Printf("eri: [muse] view/subscribe write failed for %s: %v", childSessID, writeErr)
	}
}

func bindMuseChildSessionLocked(activeTurn *TurnContext, itemID, childSessID string) {
	if itemID == "" || childSessID == "" {
		return
	}
	if activeTurn.museChildParentID == nil {
		activeTurn.museChildParentID = make(map[string]string)
	}
	if activeTurn.museItemChildSession == nil {
		activeTurn.museItemChildSession = make(map[string]string)
	}
	activeTurn.museChildParentID[childSessID] = itemID
	if activeTurn.museItemChildSession[itemID] == "" {
		activeTurn.museItemChildSession[itemID] = childSessID
	}
}

type parsedMuseToolItem struct {
	canonicalTool string
	detail        string
	server        string
	tool          string
	paramsMap     map[string]any
	isSubagent    bool
}

func parseMuseToolItem(item map[string]any, itemType string) parsedMuseToolItem {
	var p parsedMuseToolItem
	switch itemType {
	case codexToolMCP:
		p.server = extractFirstString(item, "server", "serverName", "server_name")
		p.tool = extractFirstString(item, "tool", "toolName", "tool_name")
		p.canonicalTool = p.tool
		p.paramsMap = extractToolParamsMap(item)
		p.detail = ExtractToolDetail(p.tool, p.paramsMap, "")
	case codexToolCommandExecution:
		p.canonicalTool = canonicalToolBash
		cmd := extractFirstString(item, "command", "cmd", "commandLine", "command_line")
		p.paramsMap = map[string]any{"command": cmd}
		p.detail = ExtractToolDetail(p.canonicalTool, p.paramsMap, cmd)
	case itemTypeSubagent:
		p.tool = extractFirstString(item, "tool", "toolName", "tool_name")
		if p.tool == "" {
			p.tool = itemTypeSubagent
		}
		p.canonicalTool = p.tool
		p.paramsMap = extractToolParamsMap(item)
		p.detail = extractFirstString(item, "name", "agent", "role", "description")
		if p.detail == "" {
			p.detail = ExtractToolDetail(canonicalToolSubagent, p.paramsMap, "")
		}
		p.isSubagent = true
	case itemTypeToolCall:
		p.tool = extractFirstString(item, "tool", "toolName", "tool_name", "name")
		p.server = extractFirstString(item, "server", "serverName", "server_name")
		p.canonicalTool = p.tool
		p.paramsMap = extractToolParamsMap(item)
		p.detail = ExtractToolDetail(p.tool, p.paramsMap, "")
		p.isSubagent = isMuseSubagentTool(p.tool)
	}
	if p.server == "" && p.tool != "" && !p.isSubagent {
		resolvedTool, resolvedServer := ExtractMCPToolInfo(p.tool, p.paramsMap)
		if resolvedServer != mcpServerNative && resolvedServer != mcpServerUnknown {
			p.canonicalTool = resolvedTool
			p.server = resolvedServer
		}
	}
	return p
}

func (d *StreamingDaemon) handleMuseToolCall(activeTurn *TurnContext, item map[string]any, itemType, itemID, parentID string, stepIdx int) {
	p := parseMuseToolItem(item, itemType)

	childSessID := extractFirstString(item, "childSessionId", "child_session_id")
	d.inflightMu.Lock()
	if parentID == "" {
		activeTurn.codexResponseText = ""
	}
	if d.claudeToolIDs == nil {
		d.claudeToolIDs = make(map[string]int)
	}
	if itemID != "" {
		d.claudeToolIDs[itemID] = stepIdx
		if p.isSubagent {
			activeTurn.museSubagentStack = append(activeTurn.museSubagentStack, itemID)
			bindMuseChildSessionLocked(activeTurn, itemID, childSessID)
		}
	}
	d.inflightMu.Unlock()

	if p.isSubagent && childSessID != "" {
		d.sendMuseViewSubscribe(activeTurn, childSessID)
	}

	call := ToolCall{
		ID:        itemID,
		ParentID:  parentID,
		Name:      p.canonicalTool,
		Command:   p.detail,
		Subagent:  p.isSubagent,
		MCPServer: p.server,
		MCPTool:   p.tool,
	}
	isNewStart := d.recordToolStart(stepIdx, true, call, p.server)
	call.MCPServer, call.MCPTool = mcpCallTarget(p.canonicalTool, p.server)
	activatedSkill, hasSkill := extractActivatedSkill(p.canonicalTool, p.paramsMap, p.detail)

	if isNewStart && activeTurn.Sink != nil {
		callForSink := call
		if p.canonicalTool == p.detail {
			callForSink.Command = ""
		}
		activeTurn.Sink.OnToolCall(callForSink)
		if hasSkill {
			activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
		}
	}
}

func (d *StreamingDaemon) handleMuseItemStarted(activeTurn *TurnContext, params map[string]any, parentID string) {
	if params == nil {
		return
	}
	var item map[string]any
	if it, ok := params["item"].(map[string]any); ok && it != nil {
		item = it
	} else {
		item = params
	}

	itemType := extractFirstString(item, fieldType, "kind")
	itemID := extractFirstString(item, "id", "itemId", "item_id")
	if parentID != "" && !isMuseToolItemType(itemType) {
		return
	}

	d.mu.Lock()
	d.stepCount++
	stepIdx := d.stepCount
	d.mu.Unlock()

	if parentID == "" {
		d.notifyStepBoundary(activeTurn, stepIdx)
	}

	d.inflightMu.Lock()
	if activeTurn.itemTypes == nil {
		activeTurn.itemTypes = make(map[string]string)
	}
	if itemID != "" && itemType != "" {
		activeTurn.itemTypes[itemID] = itemType
	}
	d.inflightMu.Unlock()

	switch itemType {
	case itemTypeAgentMessage:
		d.inflightMu.Lock()
		activeTurn.codexResponseText = ""
		d.inflightMu.Unlock()
	case itemTypeReasoning, itemTypeThinking:
		d.inflightMu.Lock()
		if activeTurn.seenReasoningItemIDs == nil {
			activeTurn.seenReasoningItemIDs = make(map[string]bool)
		}
		isNew := !activeTurn.seenReasoningItemIDs[itemID]
		if itemID != "" {
			activeTurn.seenReasoningItemIDs[itemID] = true
		}
		d.inflightMu.Unlock()
		if isNew && activeTurn.Sink != nil {
			activeTurn.Sink.OnThinking()
		}
	case itemTypeToolCall, codexToolMCP, codexToolCommandExecution, itemTypeSubagent:
		d.handleMuseToolCall(activeTurn, item, itemType, itemID, parentID, stepIdx)
	}
}

func (d *StreamingDaemon) handleMuseItemUpdated(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	var item map[string]any
	if it, ok := params["item"].(map[string]any); ok && it != nil {
		item = it
	} else {
		item = params
	}
	itemID := extractFirstString(item, "id", "itemId", "item_id")
	childSessID := extractFirstString(item, "childSessionId", "child_session_id")
	if childSessID == "" {
		return
	}
	d.inflightMu.Lock()
	targetID := itemID
	if targetID == "" && len(activeTurn.museSubagentStack) > 0 {
		targetID = activeTurn.museSubagentStack[len(activeTurn.museSubagentStack)-1]
	}
	bindMuseChildSessionLocked(activeTurn, targetID, childSessID)
	d.inflightMu.Unlock()

	d.sendMuseViewSubscribe(activeTurn, childSessID)
}

func (d *StreamingDaemon) handleMuseItemDelta(activeTurn *TurnContext, params map[string]any, method string) {
	if params == nil || activeTurn.Sink == nil {
		return
	}
	itemID := extractFirstString(params, "itemId", "item_id", "id")
	itemType := extractMuseItemType(params, "")
	if itemType == "" {
		switch method {
		case methodReasoningTextDelta, methodReasoningSummary:
			itemType = itemTypeReasoning
		case methodAgentMessageDelta:
			itemType = itemTypeAgentMessage
		default:
			if itemID != "" {
				d.inflightMu.Lock()
				itemType = activeTurn.itemTypes[itemID]
				d.inflightMu.Unlock()
			}
		}
	}

	if itemType == itemTypeReasoning || itemType == itemTypeThinking {
		d.inflightMu.Lock()
		if activeTurn.seenReasoningItemIDs == nil {
			activeTurn.seenReasoningItemIDs = make(map[string]bool)
		}
		isNew := !activeTurn.seenReasoningItemIDs[itemID]
		if itemID != "" {
			activeTurn.seenReasoningItemIDs[itemID] = true
		}
		d.inflightMu.Unlock()
		if isNew {
			activeTurn.Sink.OnThinking()
		}
		return
	}

	if itemType != "" && itemType != itemTypeAgentMessage {
		return
	}

	deltaStr := extractMuseDeltaText(params)
	if deltaStr != "" {
		d.inflightMu.Lock()
		activeTurn.codexResponseText += deltaStr
		d.inflightMu.Unlock()
		activeTurn.Sink.OnTextDelta(deltaStr)
	}
}

func removeMuseSubagentStackItem(stack []string, itemID string) []string {
	for i, id := range slices.Backward(stack) {
		if id == itemID {
			return append(stack[:i], stack[i+1:]...)
		}
	}
	return stack
}

func (d *StreamingDaemon) finishMuseCompletedItemStateLocked(activeTurn *TurnContext, itemID, childSessID string) (stepIdx int, hasStep, seenChildUsage bool) {
	stepIdx, hasStep = d.claudeToolIDs[itemID]
	if hasStep {
		delete(d.claudeToolIDs, itemID)
	}
	if itemID != "" {
		activeTurn.museSubagentStack = removeMuseSubagentStackItem(activeTurn.museSubagentStack, itemID)
		if childSessID == "" && activeTurn.museItemChildSession != nil {
			childSessID = activeTurn.museItemChildSession[itemID]
		}
		for cID, pID := range activeTurn.museChildParentID {
			if pID == itemID {
				seenChildUsage = seenChildUsage || (activeTurn.museSeenChildUsage != nil && activeTurn.museSeenChildUsage[cID])
				delete(activeTurn.museChildParentID, cID)
			}
		}
		delete(activeTurn.museItemChildSession, itemID)
	}
	if childSessID != "" {
		seenChildUsage = seenChildUsage || (activeTurn.museSeenChildUsage != nil && activeTurn.museSeenChildUsage[childSessID])
		delete(activeTurn.museChildParentID, childSessID)
	}
	return stepIdx, hasStep, seenChildUsage
}

func extractMuseCompletedToolStatusAndName(item map[string]any, itemType string) (status, canonicalTool string) {
	status = "ok"
	if itemStatus := extractFirstString(item, fieldStatus); itemStatus == statusFailed || itemStatus == statusError || itemStatus == statusCancelled {
		status = statusError
	}
	if isError, ok := item["is_error"].(bool); ok && isError {
		status = statusError
	}
	canonicalTool = extractFirstString(item, "tool", "toolName", "name")
	switch {
	case canonicalTool == "" && itemType == codexToolCommandExecution:
		canonicalTool = canonicalToolBash
	case canonicalTool == "" && itemType == itemTypeSubagent:
		canonicalTool = itemTypeSubagent
	}
	return status, canonicalTool
}

func (d *StreamingDaemon) handleMuseItemCompleted(activeTurn *TurnContext, params map[string]any, parentID string) {
	if params == nil {
		return
	}
	item := params
	if it, ok := params["item"].(map[string]any); ok && it != nil {
		item = it
	}

	itemType := extractFirstString(item, fieldType, "kind")
	itemID := extractFirstString(item, "id", "itemId", "item_id")

	if itemType == itemTypeAgentMessage {
		if parentID == "" {
			if text := extractCodexAgentMessageText(item); text != "" {
				d.inflightMu.Lock()
				activeTurn.codexResponseText = text
				d.inflightMu.Unlock()
				clearCodexSinkPurgedFlag(activeTurn.Sink)
			}
		}
		return
	}

	childSessID := extractFirstString(item, "childSessionId", "child_session_id")
	d.inflightMu.Lock()
	stepIdx, hasStep, seenChildUsage := d.finishMuseCompletedItemStateLocked(activeTurn, itemID, childSessID)
	d.inflightMu.Unlock()

	if parentID != "" && !hasStep {
		return
	}

	if !hasStep && !isMuseToolItemType(itemType) {
		return
	}

	if !seenChildUsage {
		if inTok, outTok, cachedTok := extractMuseUsage(item); inTok > 0 || outTok > 0 || cachedTok > 0 {
			d.handleMuseTokenUsage(activeTurn, item)
		}
	}

	status, canonicalTool := extractMuseCompletedToolStatusAndName(item, itemType)
	fields := stepUpdateFields{
		isToolEnd:     true,
		toolEndStatus: status,
		hasStepIdx:    hasStep,
		stepIdx:       stepIdx,
		toolName:      canonicalTool,
	}
	rec, hasRec, isEnd := d.recordToolEnd(fields)
	if isEnd && hasRec && activeTurn.Sink != nil {
		activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), status)
	}
}

func (d *StreamingDaemon) handleMuseTokenUsage(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	inputTokens, outputTokens, cachedTokens := extractMuseUsage(params)
	uncachedInput := max(0, inputTokens-cachedTokens)
	u := AgyUsage{
		InputTokens:     uncachedInput,
		CacheReadTokens: cachedTokens,
		OutputTokens:    outputTokens,
		TotalTokens:     inputTokens + outputTokens,
	}
	d.inflightMu.Lock()
	if activeTurn.streamUsage == nil {
		activeTurn.streamUsage = make(map[string]AgyUsage)
	}
	prev := activeTurn.streamUsage["muse-turn"]
	activeTurn.streamUsage["muse-turn"] = AgyUsage{
		InputTokens:     prev.InputTokens + u.InputTokens,
		CacheReadTokens: prev.CacheReadTokens + u.CacheReadTokens,
		OutputTokens:    prev.OutputTokens + u.OutputTokens,
		TotalTokens:     prev.TotalTokens + u.TotalTokens,
	}
	d.inflightMu.Unlock()
}

func extractRPCErrorMessage(raw map[string]any) string {
	errObj, ok := raw[fieldError].(map[string]any)
	if !ok || errObj == nil {
		return ""
	}
	var hasID bool
	if idNum, ok := raw["id"].(float64); ok && int(idNum) != 0 {
		hasID = true
	}
	if !hasID {
		return ""
	}
	msg := extractFirstString(errObj, "message")
	if msg == "" {
		return "muse rpc error"
	}
	return msg
}

func (d *StreamingDaemon) resolveMuseChildParentID(activeTurn *TurnContext, childSessID string, markUsage bool) string {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	var parentID string
	if activeTurn.museChildParentID != nil {
		parentID = activeTurn.museChildParentID[childSessID]
	}
	if parentID == "" && len(activeTurn.museSubagentStack) == 1 {
		parentID = activeTurn.museSubagentStack[0]
		if childSessID != "" {
			if activeTurn.museChildParentID == nil {
				activeTurn.museChildParentID = make(map[string]string)
			}
			activeTurn.museChildParentID[childSessID] = parentID
		}
	}
	if markUsage && childSessID != "" {
		if activeTurn.museSeenChildUsage == nil {
			activeTurn.museSeenChildUsage = make(map[string]bool)
		}
		activeTurn.museSeenChildUsage[childSessID] = true
	}
	return parentID
}

func (d *StreamingDaemon) dispatchMuseJSONRPCLine(trimmed string) {
	if trimmed == "" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed Muse JSON-RPC line: %v", err)
		return
	}

	if errMsg := extractRPCErrorMessage(raw); errMsg != "" {
		d.handleResultEvent(map[string]any{
			fieldEvent:  eventResult,
			fieldStatus: statusUpperError,
			fieldError:  errMsg,
		})
		return
	}

	method := extractFirstString(raw, "method")

	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
	}
	d.inflightMu.Unlock()

	if activeTurn == nil {
		return
	}

	var params map[string]any
	if p, ok := raw["params"].(map[string]any); ok {
		params = p
	}

	if isMuseReminderChild(params) {
		return
	}

	var parentID string
	if childSessID := extractMuseChildSessionID(params, d.SessionID()); childSessID != "" {
		parentID = d.resolveMuseChildParentID(activeTurn, childSessID, method == methodSessionTokenUsage)
		if parentID == "" {
			if method == methodSessionTokenUsage {
				d.handleMuseTokenUsage(activeTurn, params)
			}
			return
		}
		switch method {
		case methodItemStarted:
			d.handleMuseItemStarted(activeTurn, params, parentID)
		case methodItemCompleted:
			d.handleMuseItemCompleted(activeTurn, params, parentID)
		case methodSessionTokenUsage:
			d.handleMuseTokenUsage(activeTurn, params)
		}
		return
	}

	switch method {
	case methodSessionTokenUsage:
		d.handleMuseTokenUsage(activeTurn, params)
	case methodItemStarted:
		d.handleMuseItemStarted(activeTurn, params, "")
	case methodItemUpdated:
		d.handleMuseItemUpdated(activeTurn, params)
	case "item/delta", methodAgentMessageDelta, methodReasoningTextDelta, methodReasoningSummary:
		d.handleMuseItemDelta(activeTurn, params, method)
	case methodItemCompleted:
		d.handleMuseItemCompleted(activeTurn, params, "")
	case methodTurnCompleted:
		d.handleMuseTurnCompleted(activeTurn, params)
	}
}
