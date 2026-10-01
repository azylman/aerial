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

	"github.com/azylman/aerial/brain/pkg/metrics"
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

// TurnContext encapsulates the metadata, prompt, and callback sink for a single turn.
type TurnContext struct {
	TurnID    string
	SessionID string
	Model     string
	Prompt    string
	Sink      TurnSink
	CreatedAt time.Time
	Ctx       context.Context
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
	cfg          DaemonConfig
	spawner      DaemonSpawner
	handle       ProcessHandle
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderrCloser io.Closer
	stderr       *ActivityWriter
	taskTracker  *TaskTracker

	mu        sync.RWMutex
	stdinMu   sync.Mutex
	state     DaemonState
	sessionID string
	dirty     bool
	lastUsed  time.Time
	turnCount      int
	stepCount      int
	onTurnFinished func(d *StreamingDaemon)

	inflightMu    sync.Mutex
	inflight      []*TurnContext
	inFlightTools map[int]inFlightToolCall

	readerWg  sync.WaitGroup
	closeOnce sync.Once
	closed    atomic.Bool
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
		cfg:          cfg,
		spawner:      spawner,
		handle:       handle,
		stdin:        stdin,
		stdout:       stdout,
		stderrCloser: stderr,
		stderr:       actWriter,
		taskTracker:   NewTaskTracker(),
		state:         StateStarting,
		lastUsed:      time.Now(),
		inFlightTools: make(map[int]inFlightToolCall),
	}

	// Determine handshake timeout from cfg.Timeout if provided, defaulting to DefaultHandshakeTimeout (30s)
	handshakeTimeout := DefaultHandshakeTimeout
	if cfg.Timeout > 0 {
		handshakeTimeout = cfg.Timeout
	}
	timer := time.NewTimer(handshakeTimeout)
	defer timer.Stop()

	reader := bufio.NewReader(stdout)
	lineChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			errChan <- readErr
			return
		}
		lineChan <- line
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
		d.sessionID = initSessID
		d.state = StateReady
		d.mu.Unlock()
		d.stderr.SetSessionID(initSessID)
	}

	d.readerWg.Add(1)
	go d.readStdoutLoop(reader)

	return d, nil
}

// State returns the current lifecycle state of the daemon.
func (d *StreamingDaemon) State() DaemonState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

// SessionID returns the latched session UUID.
func (d *StreamingDaemon) SessionID() string {
	if d == nil {
		return ""
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sessionID
}

// SetTranscriptRescuer dynamically updates the transcript rescuer callback on the daemon.
func (d *StreamingDaemon) SetTranscriptRescuer(rescuer func(convID string, since time.Time) string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.TranscriptRescuer = rescuer
}

// TurnCount returns the total number of completed turns executed by this daemon.
func (d *StreamingDaemon) TurnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.turnCount
}

// StepCount returns the total number of step_update events received across all turns.
func (d *StreamingDaemon) StepCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stepCount
}

// LastUsed returns the timestamp of daemon initialization or the most recent turn completion.
func (d *StreamingDaemon) LastUsed() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastUsed
}

// TaskTracker returns the background task tracker instance for this daemon.
func (d *StreamingDaemon) TaskTracker() *TaskTracker {
	return d.taskTracker
}

// InflightCount returns the number of active in-flight turns currently queued or executing.
func (d *StreamingDaemon) InflightCount() int {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	return len(d.inflight)
}

// IsDirty reports whether the daemon encountered an unrecoverable write error or closed state.
func (d *StreamingDaemon) IsDirty() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dirty
}

// SetSessionID updates the session ID latched to this daemon.
func (d *StreamingDaemon) SetSessionID(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sessionID = id
	if d.stderr != nil {
		d.stderr.SetSessionID(id)
	}
}

// MarkDirty marks the daemon dirty so it will be rotated or recreated on next use.
func (d *StreamingDaemon) MarkDirty() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dirty = true
}

// SetOnTurnFinished registers a callback invoked when an in-flight turn finishes and no more in-flight turns remain.
func (d *StreamingDaemon) SetOnTurnFinished(fn func(d *StreamingDaemon)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTurnFinished = fn
}


// Send serializes prompt input writing to the daemon stdin under stdinMu mutex protection,
// tracks the TurnContext in the in-flight FIFO queue, and transitions daemon state to StateExecuting.
func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error {
	d.stdinMu.Lock()
	defer d.stdinMu.Unlock()

	d.mu.RLock()
	if d.state == StateClosed || d.closed.Load() {
		d.mu.RUnlock()
		return errors.New("cannot send to closed streaming daemon")
	}
	d.mu.RUnlock()

	payloadPrompt := prompt
	if d.cfg.AmbientContextRetriever != nil && !strings.Contains(prompt, "<ambient_context>") {
		ctx := context.Background()
		if turnCtx != nil && turnCtx.Ctx != nil {
			ctx = turnCtx.Ctx
		}
		ambCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		ambBlock, aErr := d.cfg.AmbientContextRetriever(ambCtx)
		cancel()
		if aErr != nil {
			log.Printf("[StreamingDaemon] Warning: AmbientContextRetriever failed: %v", aErr)
		} else if strings.TrimSpace(ambBlock) != "" {
			payloadPrompt = strings.TrimSpace(ambBlock) + "\n\n" + payloadPrompt
		}
	}
	if d.cfg.MemoryRetriever != nil && !strings.Contains(prompt, "<retrieved_memory>") {
		ctx := context.Background()
		if turnCtx != nil && turnCtx.Ctx != nil {
			ctx = turnCtx.Ctx
		}
		memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		memBlock, mErr := d.cfg.MemoryRetriever(memCtx, prompt)
		cancel()
		if mErr != nil {
			log.Printf("[StreamingDaemon] Warning: MemoryRetriever failed: %v", mErr)
		} else if strings.TrimSpace(memBlock) != "" {
			payloadPrompt = strings.TrimSpace(memBlock) + "\n\n" + payloadPrompt
		}
	}

	wireMsg := streamInputPayload{
		Event: "user",
		Message: streamInputMessage{
			Content: payloadPrompt,
		},
	}
	encoded, err := json.Marshal(wireMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	d.inflightMu.Lock()
	d.inflight = append(d.inflight, turnCtx)
	d.inflightMu.Unlock()

	d.mu.Lock()
	d.state = StateExecuting
	d.lastUsed = time.Now()
	d.mu.Unlock()

	if turnCtx != nil && turnCtx.Sink != nil {
		turnCtx.Sink.OnTurnStarted()
	}

	if _, err := d.stdin.Write(encoded); err != nil {
		d.mu.Lock()
		d.dirty = true
		d.state = StateClosed
		d.mu.Unlock()
		return fmt.Errorf("failed writing prompt to daemon stdin: %w", err)
	}

	return nil
}

// Close gracefully terminates the daemon process, closes streams, and notifies any in-flight turns.
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

		// Drain any remaining inflight turns with error
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
				remaining := d.inflight
				d.inflight = nil
				var dangling []inFlightToolCall
				for _, rec := range d.inFlightTools {
					dangling = append(dangling, rec)
				}
				d.inFlightTools = make(map[int]inFlightToolCall)
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
								turn.Sink.OnResult(&TurnResult{
									ConversationID: d.SessionID(),
									Response:       strings.TrimSpace(rescued),
									Duration:       time.Since(turn.CreatedAt),
								})
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

		if activeTurn.Sink != nil {
			var (
				toolName           string
				cmdName            string
				stepIdx            int
				hasStepIdx         bool
				isToolStart        bool
				isToolEnd          bool
				toolEndStatus      string
				completedRecord    inFlightToolCall
				hasCompletedRecord bool
				activatedSkill     string
				hasSkill           bool
				toolParams         map[string]any
			)

			if toolCall, ok := raw["tool_call"].(map[string]any); ok {
				if n, ok := toolCall["name"].(string); ok {
					toolName = n
				}
				if c, ok := toolCall["command"].(string); ok {
					cmdName = c
				}
			}
			if su, ok := raw["step_update"].(map[string]any); ok {
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
					stepIdx = int(idx)
					hasStepIdx = true
				} else if idx, ok := su["step_index"].(int); ok {
					stepIdx = idx
					hasStepIdx = true
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
						isToolEnd = true
						if state == "DONE" {
							toolEndStatus = "ok"
						} else {
							toolEndStatus = "error"
						}
					}
				}
			}

			if isToolStart {
				canonicalTool, mcpServer := ExtractMCPToolInfo(toolName, toolParams)
				if toolParams != nil {
					if path, ok := toolParams["AbsolutePath"].(string); ok && path != "" {
						if skill, ok := ExtractSkillFromTarget(path); ok {
							activatedSkill = skill
							hasSkill = true
						}
					}
				}
				if !hasSkill && cmdName != "" {
					if skill, ok := ExtractSkillFromTarget(cmdName); ok {
						activatedSkill = skill
						hasSkill = true
					}
				}

				d.inflightMu.Lock()
				if d.inFlightTools == nil {
					d.inFlightTools = make(map[int]inFlightToolCall)
				}
				if hasStepIdx {
					if _, exists := d.inFlightTools[stepIdx]; !exists {
						d.inFlightTools[stepIdx] = inFlightToolCall{
							stepIndex: stepIdx,
							toolName:  canonicalTool,
							mcpServer: mcpServer,
							startedAt: time.Now(),
						}
					}
				}
				d.inflightMu.Unlock()
			} else if isToolEnd && hasStepIdx {
				d.inflightMu.Lock()
				if d.inFlightTools != nil {
					if rec, exists := d.inFlightTools[stepIdx]; exists {
						completedRecord = rec
						hasCompletedRecord = true
						delete(d.inFlightTools, stepIdx)
					}
				}
				d.inflightMu.Unlock()
			}

			if toolName != "" || cmdName != "" {
				activeTurn.Sink.OnToolCall(toolName, cmdName)
			}
			if hasSkill {
				activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
			}
			if hasCompletedRecord {
				activeTurn.Sink.OnToolCompleted(completedRecord.toolName, completedRecord.mcpServer, time.Since(completedRecord.startedAt), toolEndStatus)
			}
			var delta string
			if d, ok := raw["delta"].(string); ok && d != "" {
				delta = d
			} else if d, ok := raw["text_delta"].(string); ok && d != "" {
				delta = d
			} else if su, ok := raw["step_update"].(map[string]any); ok {
				if d, ok := su["text_delta"].(string); ok && d != "" {
					delta = d
				} else if d, ok := su["delta"].(string); ok && d != "" {
					delta = d
				}
			}
			if delta != "" {
				activeTurn.Sink.OnTextDelta(delta)
			}
			if _, ok := raw["thinking"]; ok {
				activeTurn.Sink.OnThinking()
			} else if su, ok := raw["step_update"].(map[string]any); ok {
				if _, ok := su["thinking"]; ok {
					activeTurn.Sink.OnThinking()
				}
			}
		}

	case "result":
		d.inflightMu.Lock()
		if len(d.inflight) > 0 {
			activeTurn = d.inflight[0]
			d.inflight = d.inflight[1:]
		}
		hasMoreInflight := len(d.inflight) > 0

		var dangling []inFlightToolCall
		for _, rec := range d.inFlightTools {
			dangling = append(dangling, rec)
		}
		d.inFlightTools = make(map[int]inFlightToolCall)
		d.inflightMu.Unlock()

		for _, rec := range dangling {
			activeTurn.Sink.OnToolCompleted(rec.toolName, rec.mcpServer, time.Since(rec.startedAt), "aborted")
		}

		d.mu.Lock()
		d.turnCount++
		d.lastUsed = time.Now()
		if !hasMoreInflight && d.state != StateClosed {
			if d.taskTracker != nil && d.taskTracker.ActiveCount() > 0 {
				d.state = StateYieldWaiting
			} else {
				d.state = StateReady
			}
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
		if resObj, ok := raw["result"].(map[string]any); ok {
			if s, ok := resObj["status"].(string); ok {
				status = s
			}
			if em, ok := resObj["error"].(string); ok {
				errMsg = em
			}
		}
		if status == "" {
			if s, ok := raw["status"].(string); ok {
				status = s
			}
		}
		if errMsg == "" {
			if em, ok := raw["error"].(string); ok {
				errMsg = em
			}
		}

		if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") || strings.EqualFold(status, "ERROR") {
			if d.cfg.TranscriptRescuer != nil && d.SessionID() != "" {
				rescued := d.cfg.TranscriptRescuer(d.SessionID(), activeTurn.CreatedAt)
				if rescued != "" && IsSubstantiveResponse(rescued) && !strings.HasPrefix(rescued, "[Tool Call Requested]:") {
					log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after error/empty result: %s", d.SessionID(), rescued)
					if errMsg != "" {
						if IsCapacityBlip(errMsg, "") {
							metrics.RecordRunnerError("capacity_throttle", currentModel)
						} else if IsQuotaPause(errMsg, "") {
							metrics.RecordRunnerError("quota_paused", currentModel)
						}
					}
					res := &TurnResult{
						ConversationID: d.SessionID(),
						Response:       strings.TrimSpace(rescued),
						Stderr:         errMsg,
						Duration:       time.Since(activeTurn.CreatedAt),
					}
					populateUsage(res, raw)
					activeTurn.Sink.OnResult(res)
					return
				}
			}
			if strings.EqualFold(status, "ERROR") {
				if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") {
					if IsCapacityBlip(errMsg, "") {
						metrics.RecordRunnerError("capacity_throttle", currentModel)
					} else if IsQuotaPause(errMsg, "") {
						metrics.RecordRunnerError("quota_paused", currentModel)
					} else {
						metrics.RecordRunnerError("process_error", currentModel)
					}
					if errMsg == "" {
						errMsg = "daemon execution failed"
					}
					activeTurn.Sink.OnError(errors.New(errMsg))
					return
				}
				if errMsg != "" {
					log.Printf("[StreamingDaemon] Notice: daemon reported error with substantive response: %s", errMsg)
					if IsCapacityBlip(errMsg, "") {
						metrics.RecordRunnerError("capacity_throttle", currentModel)
					} else if IsQuotaPause(errMsg, "") {
						metrics.RecordRunnerError("quota_paused", currentModel)
					}
				}
			}
		}

		res := &TurnResult{
			ConversationID: d.SessionID(),
			Response:       respStr,
			Stderr:         errMsg,
			Duration:       time.Since(activeTurn.CreatedAt),
		}
		populateUsage(res, raw)
		activeTurn.Sink.OnResult(res)
	}
}

func extractResponseString(raw map[string]any) string {
	if resObj, ok := raw["result"].(map[string]any); ok {
		if resp, ok := resObj["response"].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
		if resp, ok := resObj["result"].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
		if resp, ok := resObj["content"].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
		if resp, ok := resObj["text"].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
	}
	if resp, ok := raw["response"].(string); ok && strings.TrimSpace(resp) != "" {
		return resp
	}
	if resp, ok := raw["content"].(string); ok && strings.TrimSpace(resp) != "" {
		return resp
	}
	if resp, ok := raw["text"].(string); ok && strings.TrimSpace(resp) != "" {
		return resp
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
	if usageObj != nil {
		if it, ok := usageObj["input_tokens"].(float64); ok {
			res.Usage.InputTokens = int(it)
		}
		if ot, ok := usageObj["output_tokens"].(float64); ok {
			res.Usage.OutputTokens = int(ot)
		}
		if tt, ok := usageObj["thinking_tokens"].(float64); ok {
			res.Usage.ThinkingTokens = int(tt)
		}
		if crt, ok := usageObj["cache_read_tokens"].(float64); ok {
			res.Usage.CacheReadTokens = int(crt)
		}
		if tot, ok := usageObj["total_tokens"].(float64); ok {
			res.Usage.TotalTokens = int(tot)
		}
	}
}

