package runner

import (
	"bufio"
	"bytes"
	"context"
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
	ExtraEnv     []string
}

type WorkerInstance struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       *bufio.Reader
	stdoutCloser io.Closer
	stderrBuf    bytes.Buffer
	convID       string
	turnsUsed    atomic.Int32
	isDead       atomic.Bool
	closed       atomic.Bool
	mu           sync.Mutex
	roots        []string
	cancel       context.CancelFunc
	rssFunc      func() uint64
}

type streamUserMessage struct {
	Event   string                `json:"event"`
	Message streamUserMessageBody `json:"message"`
}

type streamUserMessageBody struct {
	Content string `json:"content"`
}

// NegotiateHandshake scans line-by-line from r until it encounters the initial {"event":"init","conversation_id":"..."}.
// If timeout expires or ctx is cancelled, closer is closed to unblock any pending read in the background routine.
func NegotiateHandshake(ctx context.Context, r *bufio.Reader, closer io.Closer, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	type initResult struct {
		convID string
		err    error
	}
	initCh := make(chan initResult, 1)

	go func() {
		for {
			line, readErr := r.ReadString('\n')
			if readErr != nil {
				initCh <- initResult{err: fmt.Errorf("stdout closed before init event: %w", readErr)}
				return
			}
			if convID, ok := ParseInitEvent(line); ok {
				initCh <- initResult{convID: convID}
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		if closer != nil {
			_ = closer.Close()
		}
		return "", ctx.Err()
	case <-time.After(timeout):
		if closer != nil {
			_ = closer.Close()
		}
		return "", ErrHandshakeTimeout
	case res := <-initCh:
		if res.err != nil {
			if closer != nil {
				_ = closer.Close()
			}
			return "", fmt.Errorf("%w: %v", ErrInvalidHandshake, res.err)
		}
		return res.convID, nil
	}
}

// ReadWorkerTurn scans lines from r until a stream-json result event is encountered, accumulating stdout and returning the parsed response.
func ReadWorkerTurn(r *bufio.Reader) (*AgyResponse, error) {
	if r == nil {
		return nil, fmt.Errorf("worker reader is nil: %w", ErrWorkerDead)
	}

	var outputBuffer strings.Builder
	for {
		line, readErr := r.ReadString('\n')
		if readErr != nil {
			return nil, fmt.Errorf("worker stdout read failed: %w", readErr)
		}
		outputBuffer.WriteString(line)

		if IsResultEvent(line) {
			resp, parseErr := ParseAgyOutput(outputBuffer.String())
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse worker result output: %w (raw: %q)", parseErr, strings.TrimSpace(line))
			}
			return resp, nil
		}
	}
}

// NewWorkerInstanceFromStreams initializes a WorkerInstance on abstract I/O streams and performs initial handshake negotiation.
func NewWorkerInstanceFromStreams(ctx context.Context, stdin io.WriteCloser, stdout io.ReadCloser, cancel context.CancelFunc, roots []string, timeout time.Duration, rssFunc func() uint64) (*WorkerInstance, error) {
	bufReader := bufio.NewReader(stdout)
	convID, err := NegotiateHandshake(ctx, bufReader, stdout, timeout)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if stdin != nil {
			_ = stdin.Close()
		}
		if stdout != nil {
			_ = stdout.Close()
		}
		return nil, err
	}

	return &WorkerInstance{
		stdin:        stdin,
		stdout:       bufReader,
		stdoutCloser: stdout,
		convID:       convID,
		roots:        roots,
		cancel:       cancel,
		rssFunc:      rssFunc,
	}, nil
}

// NewWorkerInstance spawns an agy process in stream-json mode and waits for the init event handshake.
func NewWorkerInstance(ctx context.Context, opts WorkerOptions) (*WorkerInstance, error) {
	agyBin := strings.TrimSpace(opts.AgyBin)
	if agyBin == "" {
		agyBin = "agy"
	}

	args := BuildAgyArgs(AgyArgsInput{
		WorkerMode:   true,
		OutputFormat: "stream-json",
		Model:        opts.Model,
	})

	workerCtx, workerCancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(workerCtx, agyBin, args...)
	if _, statErr := os.Stat("/share/aerial"); statErr == nil {
		cmd.Dir = "/share/aerial"
	} else if _, statErr := os.Stat("/app"); statErr == nil {
		cmd.Dir = "/app"
	} else {
		cmd.Dir = "."
	}

	cmd.Env = BuildAgyEnv(AgyEnvInput{
		BaseEnv:  cmd.Environ(),
		HomeDir:  opts.HomeDir,
		APIKey:   opts.APIKey,
		ExtraEnv: opts.ExtraEnv,
	})

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

	if err := cmd.Start(); err != nil {
		workerCancel()
		_ = stdinPipe.Close()
		_ = stdoutPipe.Close()
		return nil, fmt.Errorf("failed to start worker process: %w", err)
	}

	w, err := NewWorkerInstanceFromStreams(ctx, stdinPipe, stdoutPipe, workerCancel, opts.SessionRoots, opts.Timeout, nil)
	if err != nil {
		killProcessGroup(cmd)
		return nil, err
	}

	w.cmd = cmd
	cmd.Stderr = &w.stderrBuf

	return w, nil
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
	if w.rssFunc != nil {
		return w.rssFunc()
	}
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

	if err := WriteWorkerTurn(w.stdin, prompt); err != nil {
		w.markDeadAndKill()
		return nil, err
	}

	type turnResult struct {
		resp *AgyResponse
		err  error
	}
	resCh := make(chan turnResult, 1)

	go func() {
		resp, err := ReadWorkerTurn(w.stdout)
		resCh <- turnResult{resp: resp, err: err}
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
	if w.stdoutCloser != nil {
		_ = w.stdoutCloser.Close()
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
	if w.stdoutCloser != nil {
		_ = w.stdoutCloser.Close()
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
