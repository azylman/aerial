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
)

// TurnSink receives streaming notifications and results during an active turn.
type TurnSink interface {
	OnTurnStarted()
	OnThinking()
	OnToolCall(toolName, commandName string)
	OnTextDelta(delta string)
	OnResult(res *TurnResult)
	OnError(err error)
}

// TurnContext encapsulates the metadata, prompt, and callback sink for a single turn.
type TurnContext struct {
	TurnID    string
	Prompt    string
	Sink      TurnSink
	CreatedAt time.Time
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

	inflightMu sync.Mutex
	inflight   []*TurnContext

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
		taskTracker:  NewTaskTracker(),
		state:        StateStarting,
		lastUsed:     time.Now(),
	}

	// Determine handshake timeout from cfg.Timeout if provided, defaulting to 5s
	handshakeTimeout := 5 * time.Second
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sessionID
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

	wireMsg := streamInputPayload{
		Event: "user",
		Message: streamInputMessage{
			Content: prompt,
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
				d.inflightMu.Unlock()

				for _, turn := range remaining {
					if turn != nil && turn.Sink != nil {
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
			var toolName, cmdName string
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
					state = s
				}
				if t, ok := su["type"].(string); ok {
					typ = t
				}
				if (typ == "tool_call" || typ == "tool") && state != "DONE" && state != "ERROR" {
					if tn, ok := su["tool_name"].(string); ok && tn != "" {
						toolName = tn
					}
					if info, ok := su["tool_info"].(map[string]any); ok {
						if params, ok := info["parameters"].(map[string]any); ok {
							if cl, ok := params["CommandLine"].(string); ok && cl != "" {
								cmdName = cl
							}
						}
					}
				}
			}
			if toolName != "" || cmdName != "" {
				activeTurn.Sink.OnToolCall(toolName, cmdName)
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
		d.inflightMu.Unlock()

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

		if activeTurn.Sink != nil {
			respStr := extractResponseString(raw)
			if resObj, ok := raw["result"].(map[string]any); ok {
				if status, ok := resObj["status"].(string); ok && status == "ERROR" {
					if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") {
						var errMsg string
						if em, ok := resObj["error"].(string); ok {
							errMsg = em
						}
						if errMsg == "" {
							errMsg = "daemon execution failed"
						}
						activeTurn.Sink.OnError(errors.New(errMsg))
						return
					}
					if em, ok := resObj["error"].(string); ok && em != "" {
						log.Printf("[StreamingDaemon] Notice: daemon reported error with substantive response: %s", em)
					}
				}
			}
			res := &TurnResult{
				ConversationID: d.SessionID(),
				Response:       respStr,
				Duration:       time.Since(activeTurn.CreatedAt),
			}
			if resObj, ok := raw["result"].(map[string]any); ok {
				if u, ok := resObj["usage"].(map[string]any); ok {
					if it, ok := u["input_tokens"].(float64); ok {
						res.Usage.InputTokens = int(it)
					}
					if ot, ok := u["output_tokens"].(float64); ok {
						res.Usage.OutputTokens = int(ot)
					}
					if tt, ok := u["thinking_tokens"].(float64); ok {
						res.Usage.ThinkingTokens = int(tt)
					}
					if crt, ok := u["cache_read_tokens"].(float64); ok {
						res.Usage.CacheReadTokens = int(crt)
					}
					if tot, ok := u["total_tokens"].(float64); ok {
						res.Usage.TotalTokens = int(tot)
					}
				}
			}
			activeTurn.Sink.OnResult(res)
		}
	}
}

func extractResponseString(raw map[string]any) string {
	if resObj, ok := raw["result"].(map[string]any); ok {
		if resp, ok := resObj["response"].(string); ok {
			return resp
		}
	}
	if resp, ok := raw["response"].(string); ok {
		return resp
	}
	return ""
}
