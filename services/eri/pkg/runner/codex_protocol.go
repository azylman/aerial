package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"
)

func (d *StreamingDaemon) closeOnStartupError(reason string) {
	if err := d.Close(); err != nil {
		log.Printf("[StreamingDaemon] Warning: failed to close daemon during startup failure (%s): %v", reason, err)
	}
}

func (d *StreamingDaemon) appendStderrTail(errMsg string) string {
	if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
		return fmt.Sprintf("%s: %s", errMsg, tailText)
	}
	return errMsg
}

func (d *StreamingDaemon) marshalTurnPrompt(turnCtx *TurnContext, prompt string) ([]byte, error) {
	if d.cfg.IsCodex() {
		effectiveModel := d.cfg.Model
		if turnCtx != nil && strings.TrimSpace(turnCtx.Model) != "" {
			effectiveModel = strings.TrimSpace(turnCtx.Model)
		}
		baseModel, effort := parseModelAndEffort(effectiveModel, d.cfg.Effort, true)
		return marshalCodexTurnStart(d.SessionID(), prompt, baseModel, effort)
	}
	return marshalPromptPayload(d.cfg.IsClaude(), prompt)
}

type codexRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type codexRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *codexRPCError  `json:"error,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type codexRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func extractThreadID(resultRaw json.RawMessage) string {
	if len(resultRaw) == 0 {
		return ""
	}
	var res map[string]any
	if err := json.Unmarshal(resultRaw, &res); err != nil {
		return ""
	}
	if thread, ok := res["thread"].(map[string]any); ok {
		if id, ok := thread["id"].(string); ok && id != "" {
			return id
		}
	}
	if id, ok := res["threadId"].(string); ok && id != "" {
		return id
	}
	if id, ok := res["id"].(string); ok && id != "" {
		return id
	}
	return ""
}

func (*StreamingDaemon) readHandshakeLine(ctx context.Context, reader *bufio.Reader, deadline time.Time) (string, error) {
	rem := time.Until(deadline)
	if rem <= 0 {
		return "", errors.New("timeout")
	}
	timer := time.NewTimer(rem)
	defer timer.Stop()

	type readResult struct {
		line string
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		line, err := reader.ReadString('\n')
		ch <- readResult{line: line, err: err}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return "", errors.New("timeout")
	case res := <-ch:
		return res.line, res.err
	}
}

func (d *StreamingDaemon) sendCodexRPCRequest(req codexRPCRequest, stage string) error {
	data, err := json.Marshal(req)
	if err != nil {
		d.closeOnStartupError("marshal " + stage)
		return fmt.Errorf("failed marshaling %s request: %w", stage, err)
	}
	data = append(data, '\n')
	if _, err := d.stdin.Write(data); err != nil {
		d.closeOnStartupError("write " + stage)
		return fmt.Errorf("failed writing %s request: %w", stage, err)
	}
	return nil
}

func (d *StreamingDaemon) awaitCodexRPCResponse(ctx context.Context, reader *bufio.Reader, deadline time.Time, expectedID int) (*codexRPCResponse, error) {
	for {
		line, readErr := d.readHandshakeLine(ctx, reader, deadline)
		if readErr != nil {
			return nil, d.handleHandshakeReadErr(ctx, readErr)
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		var resp codexRPCResponse
		if err := json.Unmarshal([]byte(trimmed), &resp); err != nil {
			continue
		}
		if resp.ID == expectedID {
			return &resp, nil
		}
	}
}

func (d *StreamingDaemon) sendAndAwaitCodexRPC(ctx context.Context, reader *bufio.Reader, deadline time.Time, req codexRPCRequest, stage string) (*codexRPCResponse, error) {
	if err := d.sendCodexRPCRequest(req, stage); err != nil {
		return nil, err
	}
	resp, err := d.awaitCodexRPCResponse(ctx, reader, deadline, req.ID)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		d.closeOnStartupError(stage + " error")
		return nil, fmt.Errorf("codex %s error: %s", stage, resp.Error.Message)
	}
	return resp, nil
}

func (d *StreamingDaemon) executeCodexInit(ctx context.Context, reader *bufio.Reader, deadline time.Time) error {
	initReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      1,
		Method:  "initialize",
		Params: map[string]any{
			"clientInfo": map[string]any{
				"name":    "eri",
				"version": "1.0.0",
			},
			"capabilities": map[string]any{
				"experimentalApi": true,
			},
		},
	}
	if _, err := d.sendAndAwaitCodexRPC(ctx, reader, deadline, initReq, "initialize"); err != nil {
		return err
	}
	initNotif := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		Method:  "initialized",
	}
	return d.sendCodexRPCRequest(initNotif, "initialized notification")
}

func (d *StreamingDaemon) tryResumeCodexThread(ctx context.Context, reader *bufio.Reader, deadline time.Time) (string, error) {
	if !d.cfg.ResumeSession || strings.TrimSpace(d.cfg.SessionID) == "" {
		return "", nil
	}
	resumeReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      2,
		Method:  "thread/resume",
		Params: map[string]any{
			"threadId":       strings.TrimSpace(d.cfg.SessionID),
			"cwd":            d.cfg.Cwd,
			codexApprovalKey: codexApprovalNever,
			"sandbox":        "danger-full-access",
		},
	}
	if err := d.sendCodexRPCRequest(resumeReq, "thread/resume"); err != nil {
		return "", err
	}
	resp, err := d.awaitCodexRPCResponse(ctx, reader, deadline, 2)
	if err != nil {
		return "", err
	}
	if resp.Error != nil {
		return "", nil
	}
	threadID := extractThreadID(resp.Result)
	if threadID == "" {
		threadID = strings.TrimSpace(d.cfg.SessionID)
	}
	return threadID, nil
}

func (d *StreamingDaemon) startCodexThread(ctx context.Context, reader *bufio.Reader, deadline time.Time) (string, error) {
	startReq := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      3,
		Method:  "thread/start",
		Params: map[string]any{
			"cwd":            d.cfg.Cwd,
			codexApprovalKey: codexApprovalNever,
			"sandbox":        "danger-full-access",
		},
	}
	resp, err := d.sendAndAwaitCodexRPC(ctx, reader, deadline, startReq, "thread/start")
	if err != nil {
		return "", err
	}
	return extractThreadID(resp.Result), nil
}

func (d *StreamingDaemon) waitForCodexHandshake(ctx context.Context, reader *bufio.Reader, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	if err := d.executeCodexInit(ctx, reader, deadline); err != nil {
		return err
	}
	threadID, err := d.tryResumeCodexThread(ctx, reader, deadline)
	if err != nil {
		return err
	}
	if threadID == "" {
		threadID, err = d.startCodexThread(ctx, reader, deadline)
		if err != nil {
			return err
		}
	}
	if threadID == "" {
		threadID = strings.TrimSpace(d.cfg.SessionID)
	}

	d.mu.Lock()
	d.sessionID = threadID
	d.state = StateReady
	d.mu.Unlock()
	return nil
}

func (d *StreamingDaemon) handleHandshakeReadErr(ctx context.Context, readErr error) error {
	if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) || ctx.Err() != nil {
		d.closeOnStartupError("cancelled")
		return fmt.Errorf("daemon startup cancelled: %w", ctx.Err())
	}
	if readErr.Error() == "timeout" {
		d.closeOnStartupError("timeout")
		errMsg := "daemon startup timed out waiting for init event"
		if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, tailText)
		}
		return errors.New(errMsg)
	}
	d.closeOnStartupError("read error")
	errMsg := fmt.Sprintf("daemon stdout closed before init event: %v", readErr)
	if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
		errMsg = fmt.Sprintf("%s: %s", errMsg, tailText)
	}
	return errors.New(errMsg)
}

func marshalCodexTurnStart(threadID, payloadPrompt, model, effort string) ([]byte, error) {
	params := map[string]any{
		"threadId":       threadID,
		"input":          []map[string]string{{"type": "text", "text": payloadPrompt}},
		codexApprovalKey: codexApprovalNever,
	}
	if strings.TrimSpace(model) != "" {
		params["model"] = strings.TrimSpace(model)
	}
	if strings.TrimSpace(effort) != "" {
		params["effort"] = strings.TrimSpace(effort)
	}
	req := codexRPCRequest{
		JSONRPC: codexRPCVersion,
		ID:      4,
		Method:  "turn/start",
		Params:  params,
	}
	return json.Marshal(req)
}

func isChildCodexThread(params map[string]any, rootThreadID string) bool {
	root := strings.TrimSpace(rootThreadID)
	if root == "" || params == nil {
		return false
	}
	notifThreadID := strings.TrimSpace(extractFirstString(params, "threadId", "thread_id"))
	if notifThreadID == "" {
		if turn, ok := params["turn"].(map[string]any); ok && turn != nil {
			notifThreadID = strings.TrimSpace(extractFirstString(turn, "threadId", "thread_id"))
		}
	}
	if notifThreadID == "" {
		if item, ok := params["item"].(map[string]any); ok && item != nil {
			notifThreadID = strings.TrimSpace(extractFirstString(item, "threadId", "thread_id"))
		}
	}
	return notifThreadID != "" && notifThreadID != root
}

func (d *StreamingDaemon) handleCodexRPCError(raw map[string]any) {
	errObj, ok := raw[fieldError].(map[string]any)
	if !ok || errObj == nil {
		return
	}
	errMsg := extractFirstString(errObj, "message")
	if errMsg == "" {
		errMsg = "codex rpc error"
	}
	d.handleResultEvent(map[string]any{
		"event":  eventResult,
		"status": statusUpperError,
		"error":  errMsg,
	})
}

func (d *StreamingDaemon) dispatchCodexJSONRPCLine(trimmed string) {
	if trimmed == "" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed Codex JSON-RPC line: %v", err)
		return
	}

	method, ok := raw["method"].(string)
	if !ok {
		d.handleCodexRPCError(raw)
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

	var params map[string]any
	if p, ok := raw["params"].(map[string]any); ok {
		params = p
	}

	if method != "thread/tokenUsage/updated" && isChildCodexThread(params, d.SessionID()) {
		return
	}

	switch method {
	case "thread/tokenUsage/updated":
		d.handleCodexTokenUsageUpdated(activeTurn, params)
	case "item/started":
		d.handleCodexItemStarted(activeTurn, params)
	case "item/agentMessage/delta":
		d.handleCodexAgentMessageDelta(activeTurn, params)
	case "item/reasoning/summaryTextDelta", "item/reasoning/textDelta":
		d.handleCodexReasoningDelta(activeTurn, params)
	case "item/completed":
		d.handleCodexItemCompleted(activeTurn, params)
	case "turn/completed":
		d.handleCodexTurnCompleted(activeTurn, params)
	}
}

const (
	codexCommentaryPhase      = "commentary"
	codexCommentaryActiveKey  = "__codex_commentary_active__"
	codexCommentaryItemPrefix = "__codex_commentary_item__:"
)

func isCodexCommentaryPhase(item map[string]any) bool {
	return item != nil && strings.EqualFold(strings.TrimSpace(extractFirstString(item, "phase")), codexCommentaryPhase)
}

func purgeCodexSinkBuffer(sink TurnSink) {
	switch s := sink.(type) {
	case *BufferingTurnSink:
		s.mu.Lock()
		s.builder.Reset()
		s.emittedAny = false
		s.purgedIntermediate = false
		s.mu.Unlock()
	case *TracingTurnSink:
		purgeCodexSinkBuffer(s.inner)
	case *leaseTurnSinkWrapper:
		purgeCodexSinkBuffer(s.inner)
		if s.buf != nil {
			purgeCodexSinkBuffer(s.buf)
		}
	}
}

func clearCodexSinkPurgedFlag(sink TurnSink) {
	switch s := sink.(type) {
	case *BufferingTurnSink:
		s.mu.Lock()
		s.purgedIntermediate = false
		s.mu.Unlock()
	case *TracingTurnSink:
		clearCodexSinkPurgedFlag(s.inner)
	case *leaseTurnSinkWrapper:
		clearCodexSinkPurgedFlag(s.inner)
		if s.buf != nil {
			clearCodexSinkPurgedFlag(s.buf)
		}
	}
}

func (d *StreamingDaemon) handleCodexAgentMessageStarted(activeTurn *TurnContext, item map[string]any, itemID string) {
	isCommentary := isCodexCommentaryPhase(item)
	d.inflightMu.Lock()
	activeTurn.codexResponseText = ""
	if d.claudeToolIDs == nil {
		d.claudeToolIDs = make(map[string]int)
	}
	if isCommentary {
		d.claudeToolIDs[codexCommentaryActiveKey] = 1
		if itemID != "" {
			d.claudeToolIDs[codexCommentaryItemPrefix+itemID] = 1
		}
	} else {
		delete(d.claudeToolIDs, codexCommentaryActiveKey)
	}
	d.inflightMu.Unlock()
	if isCommentary {
		purgeCodexSinkBuffer(activeTurn.Sink)
	}
}

func (d *StreamingDaemon) handleCodexAgentMessageCompleted(activeTurn *TurnContext, item map[string]any, itemID string) {
	isCommentary := isCodexCommentaryPhase(item)
	text := extractCodexAgentMessageText(item)
	d.inflightMu.Lock()
	delete(d.claudeToolIDs, codexCommentaryActiveKey)
	delete(d.claudeToolIDs, codexCommentaryItemPrefix+itemID)
	if isCommentary {
		activeTurn.codexResponseText = ""
	} else if text != "" {
		activeTurn.codexResponseText = text
	}
	d.inflightMu.Unlock()
	if isCommentary {
		purgeCodexSinkBuffer(activeTurn.Sink)
		return
	}
	clearCodexSinkPurgedFlag(activeTurn.Sink)
}

func (d *StreamingDaemon) handleCodexItemStarted(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	item, ok := params["item"].(map[string]any)
	if !ok || item == nil {
		return
	}

	d.mu.Lock()
	d.stepCount++
	stepIdx := d.stepCount
	d.mu.Unlock()

	d.notifyStepBoundary(activeTurn, stepIdx)

	itemType := extractFirstString(item, "type")
	itemID := extractFirstString(item, "id")

	switch itemType {
	case "agentMessage":
		d.handleCodexAgentMessageStarted(activeTurn, item, itemID)
	case codexToolMCP, codexToolCommandExecution, codexToolFileChange, codexToolWebSearch, codexToolCollabAgent:
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
			switch args := item["arguments"].(type) {
			case map[string]any:
				paramsMap = args
			case string:
				if err := json.Unmarshal([]byte(args), &paramsMap); err != nil {
					paramsMap = map[string]any{"raw": args}
				}
			}
			detail = ExtractToolDetail(tool, paramsMap, "")
		case codexToolCommandExecution:
			canonicalTool = canonicalToolBash
			cmd := extractFirstString(item, "command", "cmd", "commandLine", "command_line")
			paramsMap = map[string]any{"command": cmd}
			detail = ExtractToolDetail(canonicalTool, paramsMap, cmd)
		case codexToolFileChange:
			canonicalTool = "edit"
			path := extractFirstString(item, "path", "filePath", "file_path", "target_file")
			paramsMap = map[string]any{"path": path}
			detail = filepath.Base(path)
		case codexToolWebSearch:
			canonicalTool = "search_web"
			query := extractFirstString(item, "query")
			paramsMap = map[string]any{"query": query}
			detail = query
		case codexToolCollabAgent:
			canonicalTool = canonicalToolSubagent
			agent := extractFirstString(item, "agent", "name", "recipient", "role")
			paramsMap = map[string]any{"role": agent}
			detail = agent
		}

		d.inflightMu.Lock()
		activeTurn.codexResponseText = ""
		if d.claudeToolIDs == nil {
			d.claudeToolIDs = make(map[string]int)
		}
		delete(d.claudeToolIDs, codexCommentaryActiveKey)
		if itemID != "" {
			d.claudeToolIDs[itemID] = stepIdx
		}
		d.inflightMu.Unlock()

		call := ToolCall{
			ID:        itemID,
			Name:      canonicalTool,
			Command:   detail,
			Subagent:  itemType == codexToolCollabAgent,
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
}

func (d *StreamingDaemon) handleCodexAgentMessageDelta(activeTurn *TurnContext, params map[string]any) {
	if params == nil || activeTurn.Sink == nil {
		return
	}
	itemID := extractFirstString(params, "itemId", "item_id", "id")
	d.inflightMu.Lock()
	inCommentary := d.claudeToolIDs != nil && (d.claudeToolIDs[codexCommentaryActiveKey] != 0 || (itemID != "" && d.claudeToolIDs[codexCommentaryItemPrefix+itemID] != 0))
	d.inflightMu.Unlock()
	if inCommentary {
		return
	}
	var deltaStr string
	switch delta := params["delta"].(type) {
	case string:
		deltaStr = delta
	case map[string]any:
		deltaStr = extractFirstString(delta, "text", "content")
	}
	if deltaStr != "" {
		activeTurn.Sink.OnTextDelta(deltaStr)
	}
}

func (d *StreamingDaemon) handleCodexReasoningDelta(activeTurn *TurnContext, params map[string]any) {
	if params == nil || activeTurn.Sink == nil {
		return
	}
	itemID := extractFirstString(params, "itemId", "item_id", "id")
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
}

func extractCodexAgentMessageText(item map[string]any) string {
	if t, ok := item["text"].(string); ok && t != "" {
		return t
	}
	contents, ok := item["content"].([]any)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, block := range contents {
		switch s := block.(type) {
		case string:
			b.WriteString(s)
		case map[string]any:
			if ts := extractFirstString(s, "text", "content"); ts != "" {
				b.WriteString(ts)
			}
		}
	}
	return b.String()
}

func isCodexToolItemType(itemType string) bool {
	switch itemType {
	case codexToolMCP, codexToolCommandExecution, codexToolFileChange, codexToolWebSearch, codexToolCollabAgent:
		return true
	default:
		return false
	}
}

func (d *StreamingDaemon) handleCodexItemCompleted(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	item, ok := params["item"].(map[string]any)
	if !ok || item == nil {
		return
	}
	itemType := extractFirstString(item, "type")
	itemID := extractFirstString(item, "id")

	if itemType == "agentMessage" {
		d.handleCodexAgentMessageCompleted(activeTurn, item, itemID)
		return
	}

	d.inflightMu.Lock()
	stepIdx, hasStep := d.claudeToolIDs[itemID]
	if hasStep {
		delete(d.claudeToolIDs, itemID)
	}
	d.inflightMu.Unlock()

	if !hasStep && !isCodexToolItemType(itemType) {
		return
	}

	status := "ok"
	if itemStatus := extractFirstString(item, "status"); itemStatus == "failed" || itemStatus == "error" {
		status = statusError
	}
	if isError, ok := item["is_error"].(bool); ok && isError {
		status = statusError
	}

	canonicalTool := extractFirstString(item, "tool", "toolName", "name")
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

func (d *StreamingDaemon) handleCodexTokenUsageUpdated(activeTurn *TurnContext, params map[string]any) {
	if params == nil {
		return
	}
	var lastObj map[string]any
	if tu, ok := params["tokenUsage"].(map[string]any); ok {
		if l, ok := tu["last"].(map[string]any); ok {
			lastObj = l
		}
	}
	if lastObj == nil {
		return
	}
	getInt := func(k string) int {
		if v, ok := lastObj[k].(float64); ok {
			return int(v)
		}
		if v, ok := lastObj[k].(int); ok {
			return v
		}
		return 0
	}
	inputTokens := getInt("inputTokens")
	cachedInputTokens := getInt("cachedInputTokens")
	outputTokens := getInt("outputTokens")
	reasoningOutputTokens := getInt("reasoningOutputTokens")
	totalTokens := getInt("totalTokens")

	uncachedInput := max(0, inputTokens-cachedInputTokens)
	u := AgyUsage{
		InputTokens:     uncachedInput,
		CacheReadTokens: cachedInputTokens,
		OutputTokens:    outputTokens,
		ThinkingTokens:  reasoningOutputTokens,
		TotalTokens:     totalTokens,
	}
	if u.TotalTokens == 0 && (u.InputTokens > 0 || u.OutputTokens > 0 || u.CacheReadTokens > 0) {
		u.TotalTokens = u.InputTokens + u.OutputTokens + u.CacheReadTokens
	}

	d.inflightMu.Lock()
	if activeTurn.streamUsage == nil {
		activeTurn.streamUsage = make(map[string]AgyUsage)
	}
	prev := activeTurn.streamUsage["codex-turn"]
	activeTurn.streamUsage["codex-turn"] = AgyUsage{
		InputTokens:     prev.InputTokens + u.InputTokens,
		CacheReadTokens: prev.CacheReadTokens + u.CacheReadTokens,
		OutputTokens:    prev.OutputTokens + u.OutputTokens,
		ThinkingTokens:  prev.ThinkingTokens + u.ThinkingTokens,
		TotalTokens:     prev.TotalTokens + u.TotalTokens,
	}
	d.inflightMu.Unlock()
}

func parseCodexTurnOutcome(turnObj map[string]any) (string, string) {
	if turnObj == nil {
		return statusUpperDone, ""
	}
	tStatus := extractFirstString(turnObj, "status")
	var errMsg string
	switch e := turnObj[fieldError].(type) {
	case map[string]any:
		errMsg = extractFirstString(e, "message")
	case string:
		errMsg = e
	}
	status := statusUpperDone
	if strings.EqualFold(tStatus, "failed") || strings.EqualFold(tStatus, "error") || strings.EqualFold(tStatus, "aborted") || errMsg != "" {
		status = statusUpperError
	}
	if status == statusUpperError && errMsg == "" {
		if tStatus != "" {
			errMsg = "turn " + tStatus
		} else {
			errMsg = "turn failed"
		}
	}
	return status, errMsg
}

func (d *StreamingDaemon) handleCodexTurnCompleted(activeTurn *TurnContext, params map[string]any) {
	var turnObj map[string]any
	if params != nil {
		if t, ok := params["turn"].(map[string]any); ok {
			turnObj = t
		}
	}
	status, errMsg := parseCodexTurnOutcome(turnObj)

	d.inflightMu.Lock()
	responseText := activeTurn.codexResponseText
	d.inflightMu.Unlock()

	rawResult := map[string]any{
		"event":       eventResult,
		"status":      status,
		fieldResponse: responseText,
	}
	if errMsg != "" {
		rawResult[fieldError] = errMsg
	}
	d.handleResultEvent(rawResult)
}
