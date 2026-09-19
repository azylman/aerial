package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	reBackgroundTaskStarted = regexp.MustCompile(`(?i)Tool is running as a background task with task id:\s*([^\s\r\n]+)`)
	reSubagentStarted       = regexp.MustCompile(`"conversationId":\s*"([^"]+)"`)
	reTaskMessageSender     = regexp.MustCompile(`(?i)sender=([^\s\r\n]+)`)
	reTaskFinishedContent   = regexp.MustCompile(`(?i)Task id\s*["\']?([^"\'\s]+)["\']?\s*finished with result:`)

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
	Content     string
	Usage       AgyUsage
	ActiveTasks []TaskMetadata
	IsYieldTrap bool
	ExitCode    int
	Duration    time.Duration
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
}

func StartDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error) {
	if cfg.AgyBin == "" {
		cfg.AgyBin = "agy"
	}

	args := []string{
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
	if cfg.Cwd != "" {
		cmd.Dir = cfg.Cwd
	}
	cmd.Env = append(cmd.Environ(), cfg.Env...)
	if cfg.GeminiHomeDir != "" {
		cmd.Env = append(cmd.Env, "GEMINI_CLI_HOME="+cfg.GeminiHomeDir)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

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
			if d.taskTracker.ActiveCount() > 0 {
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
		return nil, fmt.Errorf("failed to write prompt to daemon stdin: %w", err)
	}

	doneChan := make(chan struct{})
	defer close(doneChan)

	go func() {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			if d.state != StateClosed && d.cmd != nil && d.cmd.Process != nil {
				if killErr := syscall.Kill(-d.cmd.Process.Pid, syscall.SIGTERM); killErr != nil {
					log.Printf("[Daemon] Warning: failed to send SIGTERM to process group on context cancellation: %v", killErr)
				}
			}
			d.mu.Unlock()
		case <-doneChan:
		}
	}()

	var turnContent strings.Builder
	var turnUsage AgyUsage
	var lastToolName string
	var lastToolCmd string

	for {
		line, readErr := d.stdout.ReadString('\n')
		if readErr != nil {
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
		case "step_update":
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
					if m := reBackgroundTaskStarted.FindStringSubmatch(toolOut); len(m) > 1 {
						taskID := strings.Trim(strings.TrimSpace(m[1]), `"'\,;`)
						d.taskTracker.Add(TaskMetadata{
							TaskID:      taskID,
							ToolName:    lastToolName,
							CommandLine: lastToolCmd,
							StartedAt:   time.Now(),
						})
					}
					if lastToolName == "invoke_subagent" {
						if m := reSubagentStarted.FindStringSubmatch(toolOut); len(m) > 1 {
							subagentID := strings.Trim(strings.TrimSpace(m[1]), `"'\,;`)
							d.taskTracker.Add(TaskMetadata{
								TaskID:    subagentID,
								ToolName:  "invoke_subagent",
								StartedAt: time.Now(),
							})
						}
					}
					if m := reTaskMessageSender.FindStringSubmatch(toolOut); len(m) > 1 {
						sender := strings.Trim(strings.TrimSpace(m[1]), `"'\,;`)
						for _, t := range d.taskTracker.ActiveTasks() {
							if t.TaskID == sender || strings.HasSuffix(t.TaskID, "/"+sender) || strings.HasSuffix(sender, "/"+t.TaskID) {
								d.taskTracker.Remove(t.TaskID)
							}
						}
					}
					if m := reTaskFinishedContent.FindStringSubmatch(toolOut); len(m) > 1 {
						finishedID := strings.Trim(strings.TrimSpace(m[1]), `"'\,;`)
						for _, t := range d.taskTracker.ActiveTasks() {
							if t.TaskID == finishedID || strings.HasSuffix(t.TaskID, "/"+finishedID) || strings.HasSuffix(finishedID, "/"+t.TaskID) {
								d.taskTracker.Remove(t.TaskID)
							}
						}
					}
				}
			}

		case "result":
			if c, ok := raw["content"].(string); ok {
				turnContent.WriteString(c)
			}
			if u, ok := raw["usage"].(map[string]interface{}); ok {
				turnUsage = parseRawUsage(u)
			}
			activeTasks := d.taskTracker.ActiveTasks()
			isYield := len(activeTasks) > 0

			return &TurnResult{
				Content:     turnContent.String(),
				Usage:       turnUsage,
				ActiveTasks: activeTasks,
				IsYieldTrap: isYield,
				ExitCode:    0,
				Duration:    time.Since(start),
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

	if d.stdin != nil {
		if closeErr := d.stdin.Close(); closeErr != nil {
			log.Printf("[Daemon] Warning: failed to close daemon stdin on close: %v", closeErr)
		}
	}

	if d.cmd != nil && d.cmd.Process != nil {
		pid := d.cmd.Process.Pid
		if killErr := syscall.Kill(-pid, syscall.SIGTERM); killErr != nil {
			log.Printf("[Daemon] Warning: failed to send SIGTERM to process group %d on close: %v", pid, killErr)
		}

		done := make(chan error, 1)
		go func() {
			done <- d.cmd.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
						if status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL) {
							return nil
						}
					}
				}
			}
			return err
		case <-time.After(closeGracePeriod):
			if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil {
				log.Printf("[Daemon] Warning: failed to send SIGKILL to process group %d on close: %v", pid, killErr)
			}
			<-done
			return nil
		}
	}
	return nil
}

func (d *Daemon) State() DaemonState         { d.mu.Lock(); defer d.mu.Unlock(); return d.state }
func (d *Daemon) SessionID() string         { return d.cfg.SessionID }
func (d *Daemon) TaskTracker() *TaskTracker { return d.taskTracker }
func (d *Daemon) SetDirty(dirty bool)       { d.mu.Lock(); defer d.mu.Unlock(); d.dirty = dirty }
func (d *Daemon) IsDirty() bool             { d.mu.Lock(); defer d.mu.Unlock(); return d.dirty }
func (d *Daemon) LastUsed() time.Time       { d.mu.Lock(); defer d.mu.Unlock(); return d.lastUsed }
