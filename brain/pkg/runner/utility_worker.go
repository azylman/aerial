package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

var (
	ErrWorkerDead        = errors.New("utility worker is dead")
	ErrHandshakeTimeout  = errors.New("utility worker handshake timeout")
	ErrInvalidHandshake  = errors.New("invalid utility worker handshake")
	ErrWorkerTurnTimeout = errors.New("utility worker turn timeout")
)

type WorkerOptions struct {
	AgyBin       string
	Model        string
	HomeDir      string
	APIKey       string
	SessionRoots []string
	Timeout      time.Duration
}

type WorkerInstance struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdout    *bufio.Reader
	stderrBuf bytes.Buffer
	convID    string
	turnsUsed atomic.Int32
	isDead    atomic.Bool
	closed    atomic.Bool
	mu        sync.Mutex
	roots     []string
	cancel    context.CancelFunc
}

type streamUserMessage struct {
	Event   string                `json:"event"`
	Message streamUserMessageBody `json:"message"`
}

type streamUserMessageBody struct {
	Content string `json:"content"`
}

// NewWorkerInstance spawns an agy process in stream-json mode and waits for the init event handshake.
func NewWorkerInstance(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
	agyBin := strings.TrimSpace(opts.AgyBin)
	if agyBin == "" {
		agyBin = "agy"
	}

	args := []string{
		"--dangerously-skip-permissions",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
	}
	if strings.TrimSpace(opts.Model) != "" {
		args = append(args, "--model", strings.TrimSpace(opts.Model))
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(workerCtx, agyBin, args...)
	if _, statErr := os.Stat("/share/aerial"); statErr == nil {
		cmd.Dir = "/share/aerial"
	} else if _, statErr := os.Stat("/app"); statErr == nil {
		cmd.Dir = "/app"
	} else {
		cmd.Dir = "."
	}

	cmdEnv := append(cmd.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"AGY_LOG_LEVEL=debug",
		"ANTIGRAVITY_LOG_LEVEL=debug",
	)
	if strings.TrimSpace(opts.HomeDir) != "" {
		cmdEnv = append(cmdEnv,
			"HOME="+strings.TrimSpace(opts.HomeDir),
			"USERPROFILE="+strings.TrimSpace(opts.HomeDir),
		)
	}
	if strings.TrimSpace(opts.APIKey) != "" {
		apiKey := strings.TrimSpace(opts.APIKey)
		cmdEnv = append(cmdEnv,
			"GEMINI_API_KEY="+apiKey,
			"ANTIGRAVITY_API_KEY="+apiKey,
			"GOOGLE_GENAI_API_KEY="+apiKey,
		)
	}
	cmd.Env = cmdEnv

	configureSysProcAttr(cmd)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		workerCancel()
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		workerCancel()
		_ = stdinPipe.Close()
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	w := &WorkerInstance{
		cmd:    cmd,
		stdin:  stdinPipe,
		stdout: bufio.NewReader(stdoutPipe),
		roots:  opts.SessionRoots,
		cancel: workerCancel,
	}
	cmd.Stderr = &w.stderrBuf

	if err := cmd.Start(); err != nil {
		workerCancel()
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return nil, fmt.Errorf("failed to start worker process: %w", err)
	}

	// Perform handshake with timeout
	handshakeTimeout := 30 * time.Second
	if opts.Timeout > 0 {
		handshakeTimeout = opts.Timeout
	}

	type initResult struct {
		convID string
		err    error
	}
	initCh := make(chan initResult, 1)

	go func() {
		for {
			line, readErr := w.stdout.ReadString('\n')
			if readErr != nil {
				initCh <- initResult{err: fmt.Errorf("stdout closed before init event: %w", readErr)}
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			var ev struct {
				Event          string `json:"event"`
				ConversationID string `json:"conversation_id"`
			}
			if err := json.Unmarshal([]byte(line), &ev); err == nil {
				if ev.Event == "init" && ev.ConversationID != "" {
					initCh <- initResult{convID: ev.ConversationID}
					return
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		w.Close()
		return nil, ctx.Err()
	case <-time.After(handshakeTimeout):
		w.Close()
		return nil, ErrHandshakeTimeout
	case res := <-initCh:
		if res.err != nil {
			w.Close()
			return nil, fmt.Errorf("%w: %v", ErrInvalidHandshake, res.err)
		}
		w.convID = res.convID
		return w, nil
	}
}

// ConversationID returns the session conversation UUID emitted by the worker on boot.
func (w *WorkerInstance) ConversationID() string {
	return w.convID
}

// TurnsUsed returns the total number of turns executed by this worker instance.
func (w *WorkerInstance) TurnsUsed() int {
	return int(w.turnsUsed.Load())
}

// RSSBytes returns the resident set size (RSS) in bytes of the worker subprocess on Linux, or 0 if unavailable.
func (w *WorkerInstance) RSSBytes() uint64 {
	if w.cmd == nil || w.cmd.Process == nil || w.cmd.Process.Pid <= 0 {
		return 0
	}
	statmPath := fmt.Sprintf("/proc/%d/statm", w.cmd.Process.Pid)
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

// IsDead reports whether the worker subprocess has terminated, crashed, or encountered a fatal pipe error.
func (w *WorkerInstance) IsDead() bool {
	return w.isDead.Load() || w.closed.Load()
}

// Execute writes a user turn over stdin and scans stdout for the final result event.
func (w *WorkerInstance) Execute(ctx context.Context, prompt string) (*AgyResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.IsDead() {
		return nil, ErrWorkerDead
	}

	req := streamUserMessage{
		Event: "user",
		Message: streamUserMessageBody{
			Content: prompt,
		},
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal prompt request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := w.stdin.Write(payload); err != nil {
		w.markDeadAndKill()
		return nil, fmt.Errorf("failed to write prompt to worker stdin: %w", err)
	}

	type turnResult struct {
		resp *AgyResponse
		err  error
	}
	resCh := make(chan turnResult, 1)

	go func() {
		var outputBuffer strings.Builder
		for {
			line, readErr := w.stdout.ReadString('\n')
			if readErr != nil {
				resCh <- turnResult{err: fmt.Errorf("worker stdout read failed: %w", readErr)}
				return
			}
			outputBuffer.WriteString(line)

			trimmed := strings.TrimSpace(line)
			var ev struct {
				Event string `json:"event"`
			}
			isResult := false
			if err := json.Unmarshal([]byte(trimmed), &ev); err == nil {
				isResult = (ev.Event == "result")
			} else {
				normalized := strings.ReplaceAll(trimmed, " ", "")
				isResult = strings.HasPrefix(normalized, `{"event":"result"`)
			}

			if isResult {
				resp, parseErr := ParseAgyOutput(outputBuffer.String())
				if parseErr != nil {
					resCh <- turnResult{err: fmt.Errorf("failed to parse worker result output: %w (raw: %q)", parseErr, trimmed)}
					return
				}
				resCh <- turnResult{resp: resp}
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		w.markDeadAndKill()
		return nil, ctx.Err()
	case res := <-resCh:
		if res.err != nil {
			w.markDeadAndKill()
			return nil, res.err
		}
		w.turnsUsed.Add(1)
		return res.resp, nil
	}
}

func (w *WorkerInstance) markDeadAndKill() {
	w.isDead.Store(true)
	if w.cancel != nil {
		w.cancel()
	}
	if w.stdin != nil {
		_ = w.stdin.Close()
	}
	if w.cmd != nil && w.cmd.Process != nil {
		// Kill process group
		killProcessGroup(w.cmd)
	}
}

// Close gracefully terminates the worker and purges its ephemeral disk artifacts.
func (w *WorkerInstance) Close() {
	if w.closed.Swap(true) {
		return
	}

	// Allow any in-flight Execute turn a brief grace period to complete cleanly
	drainCh := make(chan struct{})
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		close(drainCh)
	}()
	select {
	case <-drainCh:
	case <-time.After(3 * time.Second):
	}

	w.isDead.Store(true)
	if w.cancel != nil {
		w.cancel()
	}

	if w.stdin != nil {
		_ = w.stdin.Close()
	}

	if w.cmd != nil && w.cmd.Process != nil {
		done := make(chan struct{})
		go func() {
			_ = w.cmd.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			killProcessGroup(w.cmd)
		}
	}

	if w.convID != "" && len(w.roots) > 0 {
		session.CleanupEphemeralSession(w.convID, w.roots...)
	}
}
