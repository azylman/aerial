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
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/session"
)

var (
	closeGracePeriod = 3 * time.Second
)

type DaemonState string

const (
	StateStarting     DaemonState = "STARTING"
	StateReady        DaemonState = "READY"
	StateExecuting    DaemonState = "EXECUTING"
	StateYieldWaiting DaemonState = "YIELD_WAITING"
	StateClosed       DaemonState = "CLOSED"
)

type DaemonConfig struct {
	SessionID     string
	ThreadID      string
	Model         string
	AgyBin        string
	Cwd           string
	Env           []string
	GeminiHomeDir string
	Timeout       time.Duration
}

type TurnResult struct {
	ConversationID string
	Response       string
	Usage          AgyUsage
	ActiveTasks    []TaskMetadata
	IsYieldTrap    bool
	ExitCode       int
	Duration       time.Duration
}

type Daemon struct {
	cfg         DaemonConfig
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      *bufio.Reader
	stderrBuf   *ActivityWriter
	taskTracker *TaskTracker
	state       DaemonState
	mu          sync.Mutex
	dirty       bool
	lastUsed    time.Time
	sessionID   string
}

func StartDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error) {
	if cfg.AgyBin == "" {
		cfg.AgyBin = "agy"
	}

	args := []string{
		"--dangerously-skip-permissions",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
	}
	if cfg.SessionID != "" {
		args = append(args, "--conversation", cfg.SessionID)
	}
	if cfg.Model != "" {
		args = append(args, "--model", cfg.Model)
	}

	cmd := exec.Command(cfg.AgyBin, args...)
	if runtime.GOOS == "windows" && strings.HasSuffix(cfg.AgyBin, ".sh") {
		shBin := "sh"
		if p, err := exec.LookPath("sh"); err == nil {
			shBin = p
		} else if _, err := os.Stat(`C:\Users\alexz\AppData\Local\Programs\MinGit\usr\bin\sh.exe`); err == nil {
			shBin = `C:\Users\alexz\AppData\Local\Programs\MinGit\usr\bin\sh.exe`
		}
		cmd = exec.Command(shBin, append([]string{cfg.AgyBin}, args...)...)
	}
	if cfg.Cwd != "" {
		cmd.Dir = cfg.Cwd
	}
	cmd.Env = append(cmd.Environ(), cfg.Env...)
	if cfg.GeminiHomeDir != "" {
		cmd.Env = append(cmd.Env, "GEMINI_CLI_HOME="+cfg.GeminiHomeDir)
	}
	if target := strings.TrimSpace(cfg.ThreadID); target != "" {
		cmd.Env = append(cmd.Env,
			"AERIAL_TARGET_ID="+target,
			"DISCORD_THREAD_ID="+target,
		)
	}
	configureSysProcAttr(cmd)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		if closeErr := stdinPipe.Close(); closeErr != nil {
			log.Printf("[Daemon] Warning: failed to close stdin pipe on stdout pipe error: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to open stdout pipe: %w", err)
	}

	stderrWriter := NewActivityWriter(cfg.SessionID)
	cmd.Stderr = stderrWriter

	if err := cmd.Start(); err != nil {
		if closeErr := stdinPipe.Close(); closeErr != nil {
			log.Printf("[Daemon] Warning: failed to close stdin pipe on start error: %v", closeErr)
		}
		if closeErr := stdoutPipe.Close(); closeErr != nil {
			log.Printf("[Daemon] Warning: failed to close stdout pipe on start error: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to start agy daemon: %w", err)
	}

	d := &Daemon{
		cfg:         cfg,
		cmd:         cmd,
		stdin:       stdinPipe,
		stdout:      bufio.NewReader(stdoutPipe),
		stderrBuf:   stderrWriter,
		taskTracker: NewTaskTracker(),
		state:       StateStarting,
		lastUsed:    time.Now(),
		sessionID:   cfg.SessionID,
	}

	d.mu.Lock()
	d.state = StateReady
	d.mu.Unlock()

	return d, nil
}

type streamInputPayload struct {
	Event   string             `json:"event"`
	Message streamInputMessage `json:"message"`
}

type streamInputMessage struct {
	Content string `json:"content"`
}

func (d *Daemon) ExecuteTurn(ctx context.Context, prompt string) (*TurnResult, error) {
	return d.ExecuteTurnWithHandler(ctx, prompt, nil)
}

func (d *Daemon) ExecuteTurnWithHandler(ctx context.Context, prompt string, handler StepUpdateHandler) (*TurnResult, error) {
	d.mu.Lock()
	if d.state == StateClosed {
		d.mu.Unlock()
		return nil, errors.New("cannot execute turn on closed daemon")
	}
	d.state = StateExecuting
	d.lastUsed = time.Now()
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		if d.state != StateClosed {
			if ctx.Err() != nil || d.dirty {
				d.state = StateClosed
				d.dirty = true
			} else if d.taskTracker.ActiveCount() > 0 {
				d.state = StateYieldWaiting
			} else {
				d.state = StateReady
			}
		}
		d.lastUsed = time.Now()
		d.mu.Unlock()
	}()

	start := time.Now()

	wireMsg := streamInputPayload{
		Event: "user",
		Message: streamInputMessage{
			Content: prompt,
		},
	}
	encoded, err := json.Marshal(wireMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	if _, err := d.stdin.Write(encoded); err != nil {
		d.mu.Lock()
		d.state = StateClosed
		d.dirty = true
		d.mu.Unlock()
		return nil, fmt.Errorf("failed to write prompt to daemon stdin: %w", err)
	}

	doneChan := make(chan struct{})
	defer close(doneChan)

	go func() {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			d.state = StateClosed
			d.dirty = true
			if d.cmd != nil && d.cmd.Process != nil {
				terminateProcessGroup(d.cmd)
			}
			d.mu.Unlock()
		case <-doneChan:
		}
	}()

	var turnResponse strings.Builder
	var turnUsage AgyUsage
	var lastToolName string
	var lastToolCmd string

	for {
		line, readErr := d.stdout.ReadString('\n')
		if readErr != nil {
			d.mu.Lock()
			d.state = StateClosed
			d.dirty = true
			d.mu.Unlock()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(readErr, io.EOF) {
				return nil, fmt.Errorf("daemon stdout closed unexpectedly (EOF): %s", d.stderrBuf.String())
			}
			return nil, fmt.Errorf("error reading daemon stream: %w", readErr)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}

		event, ok := raw["event"].(string)
		if !ok {
			continue
		}
		switch event {
		case "init":
			var probe sessionProbe
			if err := json.Unmarshal([]byte(line), &probe); err == nil {
				if id := probe.extractUUID(); id != "" {
					d.mu.Lock()
					if d.sessionID == "" {
						d.sessionID = id
						d.cfg.SessionID = id
					}
					d.mu.Unlock()
					if d.stderrBuf != nil {
						d.stderrBuf.SetSessionID(id)
					}
				}
			}
			if id, ok := ParseInitEvent(line); ok && IsValidUUID(id) {
				d.mu.Lock()
				if d.sessionID == "" {
					d.sessionID = id
					d.cfg.SessionID = id
				}
				d.mu.Unlock()
				if d.stderrBuf != nil {
					d.stderrBuf.SetSessionID(id)
				}
			}

		case "step_update":
			if handler != nil {
				var rawStep struct {
					StepUpdate json.RawMessage `json:"step_update,omitempty"`
				}
				var stepEv StepUpdateEvent
				if err := json.Unmarshal([]byte(line), &rawStep); err == nil {
					var uErr error
					if len(rawStep.StepUpdate) > 0 {
						uErr = json.Unmarshal(rawStep.StepUpdate, &stepEv)
					} else {
						uErr = json.Unmarshal([]byte(line), &stepEv)
					}
					if uErr == nil && (stepEv.ResolvedType() != "" || stepEv.ResolvedToolName() != "") {
						handler(&stepEv)
					}
				}
			}

			stepUpdate, ok := raw["step_update"].(map[string]interface{})
			if ok && stepUpdate != nil {
				if toolName, ok := stepUpdate["tool_name"].(string); ok && toolName != "" {
					lastToolName = toolName
				}
				if toolInfo, ok := stepUpdate["tool_info"].(map[string]interface{}); ok {
					if params, ok := toolInfo["parameters"].(map[string]interface{}); ok {
						if cmd, ok := params["CommandLine"].(string); ok {
							lastToolCmd = cmd
						}
					}
				}

				if toolOut, ok := stepUpdate["tool_output"].(string); ok {
					if taskID := session.ParseBackgroundTaskStarted(toolOut); taskID != "" {
						d.taskTracker.Add(TaskMetadata{
							TaskID:      taskID,
							ToolName:    lastToolName,
							CommandLine: lastToolCmd,
							StartedAt:   time.Now(),
						})
						if d.cfg.ThreadID != "" {
							metrics.ActiveTasksGauge.WithLabelValues(d.cfg.ThreadID).Set(float64(d.taskTracker.ActiveCount()))
						}
					}
					if lastToolName == "invoke_subagent" {
						if subagentID := extractSubagentID(toolOut); subagentID != "" {
							d.taskTracker.Add(TaskMetadata{
								TaskID:    subagentID,
								ToolName:  "invoke_subagent",
								StartedAt: time.Now(),
							})
							if d.cfg.ThreadID != "" {
								metrics.ActiveTasksGauge.WithLabelValues(d.cfg.ThreadID).Set(float64(d.taskTracker.ActiveCount()))
							}
						}
					}
					if sender := session.ParseTaskMessageSender(toolOut); sender != "" {
						for _, t := range d.taskTracker.ActiveTasks() {
							if t.TaskID == sender || strings.HasSuffix(t.TaskID, "/"+sender) || strings.HasSuffix(sender, "/"+t.TaskID) {
								d.taskTracker.Remove(t.TaskID)
							}
						}
						if d.cfg.ThreadID != "" {
							metrics.ActiveTasksGauge.WithLabelValues(d.cfg.ThreadID).Set(float64(d.taskTracker.ActiveCount()))
						}
					}
					if finishedID := session.ParseTaskFinishedContent(toolOut); finishedID != "" {
						for _, t := range d.taskTracker.ActiveTasks() {
							if t.TaskID == finishedID || strings.HasSuffix(t.TaskID, "/"+finishedID) || strings.HasSuffix(finishedID, "/"+t.TaskID) {
								d.taskTracker.Remove(t.TaskID)
							}
						}
						if d.cfg.ThreadID != "" {
							metrics.ActiveTasksGauge.WithLabelValues(d.cfg.ThreadID).Set(float64(d.taskTracker.ActiveCount()))
						}
					}
				}
			}

		case "result":
			var probe sessionProbe
			if err := json.Unmarshal([]byte(line), &probe); err != nil {
				return nil, fmt.Errorf("failed to parse daemon result probe: %w", err)
			}

			var res AgyResponse
			if len(probe.Result) > 0 {
				if err := json.Unmarshal(probe.Result, &res); err != nil {
					var rawStr string
					if strErr := json.Unmarshal(probe.Result, &rawStr); strErr == nil {
						res.Response = rawStr
					} else {
						return nil, fmt.Errorf("failed to parse daemon result object: %w", err)
					}
				}
			} else {
				if err := json.Unmarshal([]byte(line), &res); err != nil {
					return nil, fmt.Errorf("failed to parse daemon result line: %w", err)
				}
			}

			if res.Response != "" {
				turnResponse.WriteString(res.Response)
			}
			turnUsage = res.Usage
			if turnUsage.TotalTokens == 0 && (res.Usage.InputTokens > 0 || res.Usage.OutputTokens > 0) {
				turnUsage.TotalTokens = turnUsage.InputTokens + turnUsage.OutputTokens
			}
			if turnUsage.TotalTokens == 0 && turnUsage.InputTokens == 0 && turnUsage.OutputTokens == 0 {
				if u, ok := raw["usage"].(map[string]interface{}); ok {
					turnUsage = parseRawUsage(u)
				}
			}

			resConvID := res.ConversationID
			if !IsValidUUID(resConvID) {
				resConvID = probe.extractUUID()
			}
			if !IsValidUUID(resConvID) {
				if id, ok := raw["conversation_id"].(string); ok && IsValidUUID(id) {
					resConvID = id
				} else if id, ok := raw["session_id"].(string); ok && IsValidUUID(id) {
					resConvID = id
				}
			}
			if resConvID != "" {
				d.mu.Lock()
				if d.sessionID == "" {
					d.sessionID = resConvID
					d.cfg.SessionID = resConvID
				}
				d.mu.Unlock()
				if d.stderrBuf != nil {
					d.stderrBuf.SetSessionID(resConvID)
				}
			}
			activeTasks := d.taskTracker.ActiveTasks()
			isYield := len(activeTasks) > 0

			return &TurnResult{
				ConversationID: d.SessionID(),
				Response:       turnResponse.String(),
				Usage:          turnUsage,
				ActiveTasks:    activeTasks,
				IsYieldTrap:    isYield,
				ExitCode:       0,
				Duration:       time.Since(start),
			}, nil
		}
	}
}

func parseRawUsage(u map[string]interface{}) AgyUsage {
	var usage AgyUsage
	if v, ok := u["input_tokens"].(float64); ok {
		usage.InputTokens = int(v)
	}
	if v, ok := u["output_tokens"].(float64); ok {
		usage.OutputTokens = int(v)
	}
	if v, ok := u["thinking_tokens"].(float64); ok {
		usage.ThinkingTokens = int(v)
	}
	if v, ok := u["cache_read_tokens"].(float64); ok {
		usage.CacheReadTokens = int(v)
	}
	if v, ok := u["total_tokens"].(float64); ok {
		usage.TotalTokens = int(v)
	}
	if usage.TotalTokens == 0 && (usage.InputTokens > 0 || usage.OutputTokens > 0) {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	return usage
}

func (d *Daemon) Close() error {
	d.mu.Lock()
	if d.state == StateClosed {
		d.mu.Unlock()
		return nil
	}
	d.state = StateClosed
	d.mu.Unlock()

	if d.cfg.ThreadID != "" {
		metrics.ActiveTasksGauge.WithLabelValues(d.cfg.ThreadID).Set(0)
	}

	if d.stdin != nil {
		if closeErr := d.stdin.Close(); closeErr != nil {
			log.Printf("[Daemon] Warning: failed to close daemon stdin on close: %v", closeErr)
		}
	}

	if d.cmd != nil && d.cmd.Process != nil {
		terminateProcessGroup(d.cmd)

		done := make(chan error, 1)
		go func() {
			done <- d.cmd.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					if isProcessTerminatedBySignal(exitErr) {
						return nil
					}
				}
			}
			return err
		case <-time.After(closeGracePeriod):
			killProcessGroup(d.cmd)
			<-done
			return nil
		}
	}
	return nil
}

// RSSBytes returns the resident set size (RSS) in bytes of the daemon subprocess on Linux, or 0 if unavailable.
func (d *Daemon) RSSBytes() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd == nil || d.cmd.Process == nil || d.cmd.Process.Pid <= 0 {
		return 0
	}
	statmPath := fmt.Sprintf("/proc/%d/statm", d.cmd.Process.Pid)
	data, err := os.ReadFile(statmPath)
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

func (d *Daemon) State() DaemonState         { d.mu.Lock(); defer d.mu.Unlock(); return d.state }
func (d *Daemon) SessionID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessionID != "" {
		return d.sessionID
	}
	if d.stderrBuf != nil {
		if id := d.stderrBuf.SessionID(); id != "" {
			d.sessionID = id
			d.cfg.SessionID = id
			return id
		}
	}
	return d.cfg.SessionID
}

func (d *Daemon) SetSessionID(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sessionID = id
	d.cfg.SessionID = id
	if d.stderrBuf != nil {
		d.stderrBuf.SetSessionID(id)
	}
}
func (d *Daemon) TaskTracker() *TaskTracker { return d.taskTracker }
func (d *Daemon) SetDirty(dirty bool)       { d.mu.Lock(); defer d.mu.Unlock(); d.dirty = dirty }
func (d *Daemon) IsDirty() bool             { d.mu.Lock(); defer d.mu.Unlock(); return d.dirty }
func (d *Daemon) LastUsed() time.Time       { d.mu.Lock(); defer d.mu.Unlock(); return d.lastUsed }

// extractSubagentID extracts a subagent conversation ID from invoke_subagent tool output.
// It parses JSON objects and arrays via json.Unmarshal / json.Decoder, supporting both
// camelCase ("conversationId") and snake_case ("conversation_id") keys.
func extractSubagentID(toolOut string) string {
	trimmed := strings.TrimSpace(toolOut)
	if trimmed == "" {
		return ""
	}

	type subagentItem struct {
		ConversationID string `json:"conversationId"`
		AltConvID      string `json:"conversation_id"`
	}

	// 1. If it's a JSON array [...]
	if strings.HasPrefix(trimmed, "[") {
		var list []subagentItem
		if err := json.Unmarshal([]byte(trimmed), &list); err == nil && len(list) > 0 {
			for _, item := range list {
				id := item.ConversationID
				if id == "" {
					id = item.AltConvID
				}
				id = strings.Trim(strings.TrimSpace(id), `"'\,;`)
				if id != "" {
					return strings.Clone(id)
				}
			}
		}
	}

	// 2. Direct JSON object unmarshal or embedded JSON object scan
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '{' {
			var payload struct {
				ConversationID string         `json:"conversationId"`
				AltConvID      string         `json:"conversation_id"`
				Subagents      []subagentItem `json:"subagents"`
			}
			dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
			if err := dec.Decode(&payload); err == nil {
				id := payload.ConversationID
				if id == "" {
					id = payload.AltConvID
				}
				if id == "" {
					for _, item := range payload.Subagents {
						subID := item.ConversationID
						if subID == "" {
							subID = item.AltConvID
						}
						subID = strings.Trim(strings.TrimSpace(subID), `"'\,;.:`)
						if subID != "" {
							id = subID
							break
						}
					}
				}
				id = strings.Trim(strings.TrimSpace(id), `"'\,;.:`)
				if id != "" {
					return strings.Clone(id)
				}
			}
		}
	}

	return ""
}

