package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
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
		return "", false, nil
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
		if trimmedSess != "" && isUUIDv7(trimmedSess) {
			startParams[fieldSessionID] = trimmedSess
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
		sessID = extractMuseSessionID(resp.Result, trimmedSess)
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

func isChildMuseSession(params map[string]any, rootSessionID string) bool {
	if params == nil {
		return false
	}
	root := strings.TrimSpace(rootSessionID)
	sessID := extractFirstString(params, "sessionId", "session_id")
	if sessID != "" && root != "" && sessID != root {
		return true
	}
	if item, ok := params["item"].(map[string]any); ok && item != nil {
		if itemSessID := extractFirstString(item, "sessionId", "session_id"); itemSessID != "" && root != "" && itemSessID != root {
			return true
		}
		if kind := extractFirstString(item, "kind"); kind == "reminderChild" {
			return true
		}
	}
	if kind := extractFirstString(params, "kind"); kind == "reminderChild" {
		return true
	}
	return false
}

func isMuseToolItemType(itemType string) bool {
	switch itemType {
	case itemTypeToolCall, codexToolMCP, codexToolCommandExecution:
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

func (d *StreamingDaemon) handleMuseToolCall(activeTurn *TurnContext, item map[string]any, itemType, itemID string, stepIdx int) {
	var (
		canonicalTool string
		detail        string
		server        string
		tool          string
		paramsMap     map[string]any
	)

	switch itemType {
	case codexToolMCP:
		server = extractFirstString(item, "server", "serverName", "server_name")
		tool = extractFirstString(item, "tool", "toolName", "tool_name")
		canonicalTool = tool
		paramsMap = extractToolParamsMap(item)
		detail = ExtractToolDetail(tool, paramsMap, "")
	case codexToolCommandExecution:
		canonicalTool = canonicalToolBash
		cmd := extractFirstString(item, "command", "cmd", "commandLine", "command_line")
		paramsMap = map[string]any{"command": cmd}
		detail = ExtractToolDetail(canonicalTool, paramsMap, cmd)
	case itemTypeToolCall:
		tool = extractFirstString(item, "tool", "toolName", "tool_name", "name")
		server = extractFirstString(item, "server", "serverName", "server_name")
		canonicalTool = tool
		paramsMap = extractToolParamsMap(item)
		detail = ExtractToolDetail(tool, paramsMap, "")
	}

	if server == "" && tool != "" {
		resolvedTool, resolvedServer := ExtractMCPToolInfo(tool, paramsMap)
		if resolvedServer != mcpServerNative && resolvedServer != mcpServerUnknown {
			canonicalTool = resolvedTool
			server = resolvedServer
		}
	}

	d.inflightMu.Lock()
	activeTurn.codexResponseText = ""
	if d.claudeToolIDs == nil {
		d.claudeToolIDs = make(map[string]int)
	}
	if itemID != "" {
		d.claudeToolIDs[itemID] = stepIdx
	}
	d.inflightMu.Unlock()

	call := ToolCall{
		ID:        itemID,
		Name:      canonicalTool,
		Command:   detail,
		MCPServer: server,
		MCPTool:   tool,
	}
	isNewStart := d.recordToolStart(stepIdx, true, call, server)
	call.MCPServer, call.MCPTool = mcpCallTarget(canonicalTool, server)
	activatedSkill, hasSkill := extractActivatedSkill(canonicalTool, paramsMap, detail)

	if isNewStart && activeTurn.Sink != nil {
		callForSink := call
		if canonicalTool == detail {
			callForSink.Command = ""
		}
		activeTurn.Sink.OnToolCall(callForSink)
		if hasSkill {
			activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
		}
	}
}

func (d *StreamingDaemon) handleMuseItemStarted(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	var item map[string]any
	if it, ok := params["item"].(map[string]any); ok && it != nil {
		item = it
	} else {
		item = params
	}

	d.mu.Lock()
	d.stepCount++
	stepIdx := d.stepCount
	d.mu.Unlock()

	d.notifyStepBoundary(activeTurn, stepIdx)

	itemType := extractFirstString(item, fieldType, "kind")
	itemID := extractFirstString(item, "id", "itemId", "item_id")

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
	case itemTypeToolCall, codexToolMCP, codexToolCommandExecution:
		d.handleMuseToolCall(activeTurn, item, itemType, itemID, stepIdx)
	}
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

func (d *StreamingDaemon) handleMuseItemCompleted(activeTurn *TurnContext, params map[string]any) {
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

	if itemType == itemTypeAgentMessage {
		text := extractCodexAgentMessageText(item)
		if text != "" {
			d.inflightMu.Lock()
			activeTurn.codexResponseText = text
			d.inflightMu.Unlock()
		}
		return
	}

	d.inflightMu.Lock()
	stepIdx, hasStep := d.claudeToolIDs[itemID]
	if hasStep {
		delete(d.claudeToolIDs, itemID)
	}
	d.inflightMu.Unlock()

	if !hasStep && !isMuseToolItemType(itemType) {
		return
	}

	status := "ok"
	if itemStatus := extractFirstString(item, fieldStatus); itemStatus == statusFailed || itemStatus == statusError || itemStatus == statusCancelled {
		status = statusError
	}
	if isError, ok := item["is_error"].(bool); ok && isError {
		status = statusError
	}

	canonicalTool := extractFirstString(item, "tool", "toolName", "name")
	if canonicalTool == "" && itemType == codexToolCommandExecution {
		canonicalTool = canonicalToolBash
	}
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

	if isChildMuseSession(params, d.SessionID()) {
		return
	}

	switch method {
	case "session/tokenUsage":
		d.handleMuseTokenUsage(activeTurn, params)
	case methodItemStarted:
		d.handleMuseItemStarted(activeTurn, params)
	case "item/delta", methodAgentMessageDelta, methodReasoningTextDelta, methodReasoningSummary:
		d.handleMuseItemDelta(activeTurn, params, method)
	case methodItemCompleted:
		d.handleMuseItemCompleted(activeTurn, params)
	case methodTurnCompleted:
		d.handleMuseTurnCompleted(activeTurn, params)
	}
}
