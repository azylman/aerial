package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

// TurnSink receives streaming notifications and results during an active turn.
type TurnSink interface {
	OnTurnStarted()
	OnThinking()
	OnToolCall(toolName, commandName string)
	OnToolCompleted(toolName, mcpServer string, duration time.Duration, status string)
	OnSkillActivated(skillName, source string)
	OnTextDelta(delta string)
	OnResult(res *TurnResult)
	OnError(err error)
}

// StepAwareSink is an optional interface that TurnSinks can implement
// to receive step boundary notifications for intermediate delta purging.
type StepAwareSink interface {
	OnStepStarted(stepIndex int)
}

// TurnContext encapsulates the metadata, prompt, structured context, and callback sink for a single turn.
type TurnContext struct {
	TurnID              string
	SessionID           string
	Model               string
	Prompt              string
	MemoryQuery         string
	ScopeInstructions   string
	ChannelInstructions string // Deprecated: Use ScopeInstructions
	CoordinationContext string
	ThreadSummary       string
	Sink                TurnSink
	CreatedAt           time.Time
	Ctx                 context.Context
	lastStepIndex       int
	hasSeenStep         bool
}

type inFlightToolCall struct {
	stepIndex int
	toolName  string
	mcpServer string
	startedAt time.Time
}

// StreamingDaemon manages a long-lived streaming agy subprocess, its lifecycle state,
// and the stdout NDJSON event streaming loop.
type StreamingDaemon struct {
	cfg            DaemonConfig
	spawner        DaemonSpawner
	handle         ProcessHandle
	stdin          io.WriteCloser
	stdout         io.ReadCloser
	stderrCloser   io.Closer
	stderr         *ActivityWriter
	mu             sync.RWMutex
	stdinMu        sync.Mutex
	state          DaemonState
	sessionID      string
	dirty          bool
	lastUsed       time.Time
	turnCount      int
	stepCount      int
	onTurnFinished func(d *StreamingDaemon)
	inflightMu     sync.Mutex
	inflight       []*TurnContext
	inFlightTools  map[int]inFlightToolCall
	readerWg       sync.WaitGroup
	closeOnce      sync.Once
	closed         atomic.Bool
}

func closeStreamQuietly(c io.Closer, name string) {
	if c != nil {
		if err := c.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, os.ErrClosed) {
			log.Printf("[StreamingDaemon] Warning: failed to close %s: %v", name, err)
		}
	}
}

func (d *StreamingDaemon) closeOnStartupError(reason string) {
	if err := d.Close(); err != nil {
		log.Printf("[StreamingDaemon] Warning: failed to close daemon during startup failure (%s): %v", reason, err)
	}
}

// DefaultHandshakeTimeout is the default maximum duration to wait for the daemon init event.
const DefaultHandshakeTimeout = 30 * time.Second

// StartStreamingDaemon launches a streaming daemon, waits for the initial handshake NDJSON event,
// latches the session ID, and spawns the background stdout reader loop.
func StartStreamingDaemon(ctx context.Context, cfg DaemonConfig, spawner DaemonSpawner) (*StreamingDaemon, error) {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	stdin, stdout, stderr, handle, err := spawner.Spawn(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed spawning daemon: %w", err)
	}

	actWriter := NewActivityWriter(cfg.SessionID)
	if stderr != nil {
		go func() {
			defer closeStreamQuietly(stderr, "stderr")
			if _, copyErr := io.Copy(actWriter, stderr); copyErr != nil && !errors.Is(copyErr, io.EOF) && !errors.Is(copyErr, io.ErrClosedPipe) && !errors.Is(copyErr, os.ErrClosed) {
				log.Printf("[StreamingDaemon] Warning: error copying stderr: %v", copyErr)
			}
		}()
	}

	d := &StreamingDaemon{
		cfg: cfg, spawner: spawner, handle: handle, stdin: stdin,
		stdout: stdout, stderrCloser: stderr, stderr: actWriter,
		state: StateStarting, lastUsed: time.Now(),
		turnCount: cfg.InitialTurnCount, inFlightTools: make(map[int]inFlightToolCall),
	}

	timeout := DefaultHandshakeTimeout
	if cfg.Timeout > 0 {
		timeout = cfg.Timeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	reader := bufio.NewReader(stdout)
	lineChan, errChan := make(chan string, 1), make(chan error, 1)
	go func() {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			errChan <- readErr
		} else {
			lineChan <- line
		}
	}()

	select {
	case <-ctx.Done():
		d.closeOnStartupError("cancelled")
		return nil, fmt.Errorf("daemon startup cancelled: %w", ctx.Err())
	case <-timer.C:
		d.closeOnStartupError("timeout")
		return nil, errors.New("daemon startup timed out waiting for init event")
	case readErr := <-errChan:
		d.closeOnStartupError("read error")
		return nil, fmt.Errorf("daemon stdout closed before init event: %w", readErr)
	case line := <-lineChan:
		initSessID, ok := ParseInitEvent(line)
		if !ok || !IsValidUUID(initSessID) {
			d.closeOnStartupError("malformed init")
			return nil, fmt.Errorf("failed parsing init event (%q)", line)
		}
		d.mu.Lock()
		d.sessionID, d.state = initSessID, StateReady
		d.mu.Unlock()
		d.stderr.SetSessionID(initSessID)
	}

	d.readerWg.Add(1)
	go d.readStdoutLoop(reader)
	return d, nil
}

func (d *StreamingDaemon) State() DaemonState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

func (d *StreamingDaemon) SessionID() string {
	if d == nil {
		return ""
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sessionID
}

func (d *StreamingDaemon) SetTranscriptRescuer(r func(string, time.Time) string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.TranscriptRescuer = r
}

func (d *StreamingDaemon) TurnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.turnCount
}

func (d *StreamingDaemon) LastUsed() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastUsed
}

func (d *StreamingDaemon) InflightCount() int {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	return len(d.inflight)
}

func (d *StreamingDaemon) IsDirty() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dirty
}

func (d *StreamingDaemon) SetSessionID(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sessionID = id
	if d.stderr != nil {
		d.stderr.SetSessionID(id)
	}
}

func (d *StreamingDaemon) MarkDirty() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dirty = true
}

func (d *StreamingDaemon) SetOnTurnFinished(fn func(d *StreamingDaemon)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTurnFinished = fn
}

func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error {
	d.stdinMu.Lock()
	defer d.stdinMu.Unlock()
	d.mu.RLock()
	if d.state == StateClosed || d.closed.Load() {
		d.mu.RUnlock()
		return errors.New("cannot send to closed streaming daemon")
	}
	d.mu.RUnlock()

	ctx := context.Background()
	if turnCtx != nil && turnCtx.Ctx != nil {
		ctx = turnCtx.Ctx
	}
	memQuery := prompt
	if turnCtx != nil && strings.TrimSpace(turnCtx.MemoryQuery) != "" {
		memQuery = strings.TrimSpace(turnCtx.MemoryQuery)
	}
	payloadPrompt := injectRetrievedContext(ctx, prompt, memQuery, d.cfg.AmbientContextRetriever, 500*time.Millisecond, d.cfg.MemoryRetriever, 2*time.Second, "[StreamingDaemon]")
	encoded, err := json.Marshal(streamInputPayload{Event: "user", Message: streamInputMessage{Content: payloadPrompt}})
	if err != nil {
		return fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	d.inflightMu.Lock()
	d.inflight = append(d.inflight, turnCtx)
	d.inflightMu.Unlock()

	d.mu.Lock()
	d.state, d.lastUsed = StateExecuting, time.Now()
	d.mu.Unlock()

	if turnCtx != nil && turnCtx.Sink != nil {
		turnCtx.Sink.OnTurnStarted()
	}

	if _, err := d.stdin.Write(encoded); err != nil {
		d.inflightMu.Lock()
		if len(d.inflight) > 0 && d.inflight[len(d.inflight)-1] == turnCtx {
			d.inflight = d.inflight[:len(d.inflight)-1]
		}
		d.inflightMu.Unlock()
		d.mu.Lock()
		d.dirty, d.state = true, StateClosed
		d.mu.Unlock()
		return fmt.Errorf("failed writing prompt to daemon stdin: %w", err)
	}
	return nil
}

func (d *StreamingDaemon) Close() error {
	var closeErr error
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		d.mu.Lock()
		d.state = StateClosed
		d.mu.Unlock()

		closeStreamQuietly(d.stdin, "stdin")
		closeStreamQuietly(d.stdout, "stdout")
		closeStreamQuietly(d.stderrCloser, "stderr")
		if d.handle != nil {
			if err := d.handle.Kill(); err != nil {
				log.Printf("[StreamingDaemon] Warning: failed to kill process handle: %v", err)
				closeErr = err
			}
			if waitErr := d.handle.Wait(); waitErr != nil {
				log.Printf("[StreamingDaemon] Process wait finished with: %v", waitErr)
			}
		}
		d.readerWg.Wait()

		d.inflightMu.Lock()
		remaining := d.inflight
		d.inflight = nil
		d.inflightMu.Unlock()

		for _, turn := range remaining {
			if turn != nil && turn.Sink != nil {
				turn.Sink.OnError(errors.New("daemon closed while turn was in-flight"))
			}
		}
	})
	return closeErr
}

func (d *StreamingDaemon) drainInFlightToolsLocked() []inFlightToolCall {
	if len(d.inFlightTools) == 0 {
		return nil
	}
	dangling := make([]inFlightToolCall, 0, len(d.inFlightTools))
	for _, rec := range d.inFlightTools {
		dangling = append(dangling, rec)
	}
	d.inFlightTools = make(map[int]inFlightToolCall)
	return dangling
}

func (d *StreamingDaemon) readStdoutLoop(r *bufio.Reader) {
	defer d.readerWg.Done()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if !d.closed.Load() {
				d.mu.Lock()
				d.state = StateClosed
				d.mu.Unlock()
				d.inflightMu.Lock()
				remaining, dangling := d.inflight, d.drainInFlightToolsLocked()
				d.inflight = nil
				d.inflightMu.Unlock()

				for _, rec := range dangling {
					if len(remaining) > 0 && remaining[0] != nil && remaining[0].Sink != nil {
						remaining[0].Sink.OnToolCompleted(rec.toolName, rec.mcpServer, time.Since(rec.startedAt), "aborted")
					}
				}
				for _, turn := range remaining {
					if turn != nil && turn.Sink != nil {
						if d.cfg.TranscriptRescuer != nil && d.SessionID() != "" {
							if rescued := d.cfg.TranscriptRescuer(d.SessionID(), turn.CreatedAt); IsSubstantiveResponse(rescued) && !strings.HasPrefix(rescued, "[Tool Call Requested]:") {
								log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after unexpected EOF", d.SessionID())
								turn.Sink.OnResult(&TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Duration: time.Since(turn.CreatedAt)})
								continue
							}
						}
						turn.Sink.OnError(fmt.Errorf("daemon stdout unexpected EOF: %w", err))
					}
				}
			}
			return
		}
		d.dispatchNDJSONLine(line)
	}
}

func (d *StreamingDaemon) dispatchNDJSONLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed NDJSON line: %v", err)
		return
	}
	event, ok := raw["event"].(string)
	if !ok {
		log.Printf("[StreamingDaemon] NDJSON line missing string event field: %v", raw["event"])
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
		d.mu.Lock()
		d.stepCount++
		d.mu.Unlock()
		if activeTurn.Sink == nil {
			return
		}

		var (
			toolName, canonicalTool, mcpServer, cmdName, activatedSkill string
			stepIdx                                                     int
			hasStepIdx, isToolStart, isToolEnd, hasSkill                bool
			toolEndStatus                                               string
			completedRecord                                             inFlightToolCall
			hasCompletedRecord                                          bool
			toolParams                                                  map[string]any
		)

		if tc, ok := raw["tool_call"].(map[string]any); ok {
			if n, ok := tc["name"].(string); ok {
				toolName = n
			}
			if c, ok := tc["command"].(string); ok {
				cmdName = c
			}
			isToolStart = true
		}
		var su map[string]any
		if m, ok := raw["step_update"].(map[string]any); ok && m != nil {
			su = m
			var state, typ string
			if s, ok := su["state"].(string); ok {
				state = strings.ToUpper(s)
			} else if s, ok := su["status"].(string); ok {
				state = strings.ToUpper(s)
			}
			if t, ok := su["type"].(string); ok {
				typ = strings.ToLower(t)
			} else if t, ok := su["step_type"].(string); ok {
				typ = strings.ToLower(t)
			}
			if idx, ok := su["step_index"].(float64); ok {
				stepIdx, hasStepIdx = int(idx), true
			} else if idx, ok := su["step_index"].(int); ok {
				stepIdx, hasStepIdx = idx, true
			}
			if tn, ok := su["tool_name"].(string); ok && tn != "" {
				toolName = tn
			}
			if info, ok := su["tool_info"].(map[string]any); ok {
				if params, ok := info["parameters"].(map[string]any); ok {
					toolParams = params
					if cl, ok := params["CommandLine"].(string); ok && cl != "" {
						cmdName = cl
					}
				}
			}
			if tcs, ok := su["tool_calls"].([]any); ok && len(tcs) > 0 {
				if firstTc, ok := tcs[0].(map[string]any); ok {
					if n, ok := firstTc["name"].(string); ok && toolName == "" {
						toolName = n
					}
					if args, ok := firstTc["args"].(map[string]any); ok && toolParams == nil {
						toolParams = args
						if cl, ok := args["CommandLine"].(string); ok && cl != "" {
							cmdName = cl
						}
					}
				}
			}
			if typ == "tool_call" || typ == "tool" {
				if state != "DONE" && state != "ERROR" {
					isToolStart = true
				} else {
					isToolStart, isToolEnd = false, true
					if state == "DONE" {
						toolEndStatus = "ok"
					} else {
						toolEndStatus = "error"
					}
				}
			}
		}

		if !hasStepIdx {
			if idx, ok := raw["step_index"].(float64); ok {
				stepIdx, hasStepIdx = int(idx), true
			} else if idx, ok := raw["step_index"].(int); ok {
				stepIdx, hasStepIdx = idx, true
			}
		}

		if hasStepIdx {
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

		var isNewToolStart bool
		if isToolStart {
			canonicalTool, mcpServer = ExtractMCPToolInfo(toolName, toolParams)
			if toolParams != nil {
				if path, ok := toolParams["AbsolutePath"].(string); ok && path != "" {
					if skill, ok := ExtractSkillFromTarget(path); ok {
						activatedSkill, hasSkill = skill, true
					}
				}
			}
			if !hasSkill && cmdName != "" {
				if skill, ok := ExtractSkillFromTarget(cmdName); ok {
					activatedSkill, hasSkill = skill, true
				}
			}

			d.inflightMu.Lock()
			if d.inFlightTools == nil {
				d.inFlightTools = make(map[int]inFlightToolCall)
			}
			if hasStepIdx {
				if _, exists := d.inFlightTools[stepIdx]; !exists {
					d.inFlightTools[stepIdx] = inFlightToolCall{stepIndex: stepIdx, toolName: canonicalTool, mcpServer: mcpServer, startedAt: time.Now()}
					isNewToolStart = true
				}
			} else {
				alreadyInFlight := false
				for _, rec := range d.inFlightTools {
					if rec.toolName == canonicalTool {
						alreadyInFlight = true
						break
					}
				}
				if !alreadyInFlight {
					d.inFlightTools[-1] = inFlightToolCall{stepIndex: -1, toolName: canonicalTool, mcpServer: mcpServer, startedAt: time.Now()}
					isNewToolStart = true
				}
			}
			d.inflightMu.Unlock()
		} else if isToolEnd {
			d.inflightMu.Lock()
			if d.inFlightTools != nil {
				if hasStepIdx {
					if rec, exists := d.inFlightTools[stepIdx]; exists {
						completedRecord, hasCompletedRecord = rec, true
						delete(d.inFlightTools, stepIdx)
					}
				}
				if !hasCompletedRecord && len(d.inFlightTools) > 0 {
					cTool, _ := ExtractMCPToolInfo(toolName, toolParams)
					for idx, rec := range d.inFlightTools {
						if rec.toolName == toolName || rec.toolName == cTool || toolName == "call_mcp_tool" || len(d.inFlightTools) == 1 {
							completedRecord, hasCompletedRecord = rec, true
							delete(d.inFlightTools, idx)
							break
						}
					}
				}
			}
			d.inflightMu.Unlock()
		}

		if isNewToolStart && (canonicalTool != "" || cmdName != "") {
			activeTurn.Sink.OnToolCall(canonicalTool, cmdName)
		}
		if hasSkill {
			activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
		}
		if isToolEnd && hasCompletedRecord {
			activeTurn.Sink.OnToolCompleted(completedRecord.toolName, completedRecord.mcpServer, time.Since(completedRecord.startedAt), toolEndStatus)
		}
		var delta string
		for _, m := range []map[string]any{raw, su} {
			if m == nil || delta != "" {
				continue
			}
			for _, k := range []string{"delta", "text_delta"} {
				if d, ok := m[k].(string); ok && d != "" {
					delta = d
					break
				}
			}
		}
		if delta != "" {
			activeTurn.Sink.OnTextDelta(delta)
		}
		if _, ok := raw["thinking"]; ok {
			activeTurn.Sink.OnThinking()
		} else if su != nil {
			if _, ok := su["thinking"]; ok {
				activeTurn.Sink.OnThinking()
			}
		}

	case "result":
		d.inflightMu.Lock()
		if len(d.inflight) > 0 {
			activeTurn = d.inflight[0]
			d.inflight = d.inflight[1:]
		}
		hasMoreInflight := len(d.inflight) > 0
		dangling := d.drainInFlightToolsLocked()
		d.inflightMu.Unlock()

		for _, rec := range dangling {
			activeTurn.Sink.OnToolCompleted(rec.toolName, rec.mcpServer, time.Since(rec.startedAt), "aborted")
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

		currentModel := d.cfg.Model
		if activeTurn != nil && strings.TrimSpace(activeTurn.Model) != "" {
			currentModel = strings.TrimSpace(activeTurn.Model)
		}
		if currentModel == "" {
			currentModel = "default"
		}

		respStr := extractResponseString(raw)
		var status, errMsg string
		var resObj map[string]any
		if r, ok := raw["result"].(map[string]any); ok {
			resObj = r
		}
		for _, m := range []map[string]any{resObj, raw} {
			if m == nil {
				continue
			}
			if status == "" {
				if s, ok := m["status"].(string); ok {
					status = s
				}
			}
			if errMsg == "" {
				if em, ok := m["error"].(string); ok {
					errMsg = em
				}
			}
		}

		if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") || strings.EqualFold(status, "ERROR") {
			if d.cfg.TranscriptRescuer != nil && d.SessionID() != "" {
				rescued := d.cfg.TranscriptRescuer(d.SessionID(), activeTurn.CreatedAt)
				if rescued != "" && IsSubstantiveResponse(rescued) && !strings.HasPrefix(rescued, "[Tool Call Requested]:") {
					log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after error/empty result: %s", d.SessionID(), rescued)
					recordStreamErrorMetric(errMsg, currentModel, false)
					res := &TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
					populateUsage(res, raw)
					activeTurn.Sink.OnResult(res)
					return
				}
			}
			if strings.EqualFold(status, "ERROR") {
				if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") {
					recordStreamErrorMetric(errMsg, currentModel, true)
					if errMsg == "" {
						errMsg = "daemon execution failed"
					}
					activeTurn.Sink.OnError(errors.New(errMsg))
					return
				}
				if errMsg != "" {
					log.Printf("[StreamingDaemon] Notice: daemon reported error with substantive response: %s", errMsg)
					recordStreamErrorMetric(errMsg, currentModel, false)
				}
			}
		}

		res := &TurnResult{ConversationID: d.SessionID(), Response: respStr, Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
		populateUsage(res, raw)
		activeTurn.Sink.OnResult(res)
	}
}

func extractResponseString(raw map[string]any) string {
	keys := []string{"response", "result", "content", "text"}
	if resObj, ok := raw["result"].(map[string]any); ok {
		for _, k := range keys {
			if resp, ok := resObj[k].(string); ok && strings.TrimSpace(resp) != "" {
				return resp
			}
		}
	}
	for _, k := range []string{"response", "content", "text"} {
		if resp, ok := raw[k].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
	}
	return ""
}

func populateUsage(res *TurnResult, raw map[string]any) {
	if res == nil || raw == nil {
		return
	}
	var usageObj map[string]any
	if u, ok := raw["usage"].(map[string]any); ok {
		usageObj = u
	}
	if resObj, ok := raw["result"].(map[string]any); ok {
		if u, ok := resObj["usage"].(map[string]any); ok {
			usageObj = u
		}
	}
	if usageObj == nil {
		return
	}
	getInt := func(k string, dst *int) {
		if v, ok := usageObj[k].(float64); ok {
			*dst = int(v)
		}
	}
	getInt("input_tokens", &res.Usage.InputTokens)
	getInt("output_tokens", &res.Usage.OutputTokens)
	getInt("thinking_tokens", &res.Usage.ThinkingTokens)
	getInt("cache_read_tokens", &res.Usage.CacheReadTokens)
	getInt("total_tokens", &res.Usage.TotalTokens)
}

func recordStreamErrorMetric(errMsg, model string, recordProcessErr bool) {
	if errMsg == "" && !recordProcessErr {
		return
	}
	if IsCapacityBlip(errMsg, "") {
		metrics.RecordRunnerError("capacity_throttle", model)
	} else if IsQuotaPause(errMsg, "") {
		metrics.RecordRunnerError("quota_paused", model)
	} else if recordProcessErr {
		metrics.RecordRunnerError("process_error", model)
	}
}
