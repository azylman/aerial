package runner

import (
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
)

// ProcessHandle abstracts an operating system process.
type ProcessHandle interface {
	Pid() int
	Kill() error
	Wait() error
}

// DaemonSpawner abstracts spawning a daemon process.
type DaemonSpawner interface {
	Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func closeQuietly(c io.Closer) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("[DaemonSpawner] Warning: failed to close stream: %v", err)
		}
	}
}

// ConfigureProcessGroup sets up platform-specific process group options on cmd.
func ConfigureProcessGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	configureSysProcAttr(cmd)
}

var killProcessGroupFunc = killProcessGroup

// KillProcessGroup terminates the process group associated with cmd.
func KillProcessGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	killProcessGroupFunc(cmd)
	return nil
}

// OSProcessHandle wraps an *exec.Cmd process.
type OSProcessHandle struct {
	cmd *exec.Cmd
}

func (h *OSProcessHandle) Pid() int {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

func (h *OSProcessHandle) Kill() error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	return KillProcessGroup(h.cmd)
}

func (h *OSProcessHandle) Wait() error {
	if h == nil || h.cmd == nil {
		return nil
	}
	return h.cmd.Wait()
}

// DefaultDaemonSpawner launches an agy OS subprocess.
type DefaultDaemonSpawner struct{}

func (s *DefaultDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if cfg.AgyBin == "" {
		cfg.AgyBin = "agy"
	}
	args := BuildDaemonArgs(cfg)
	cmd := exec.CommandContext(ctx, cfg.AgyBin, args...)
	ConfigureProcessGroup(cmd)
	cmd.Dir = cfg.Cwd
	cmd.Env = cfg.Env

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		closeQuietly(stdin)
		return nil, nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		closeQuietly(stdin)
		closeQuietly(stdout)
		return nil, nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		closeQuietly(stdin)
		closeQuietly(stdout)
		closeQuietly(stderr)
		return nil, nil, nil, nil, fmt.Errorf("failed to start daemon process: %w", err)
	}

	return stdin, stdout, stderr, &OSProcessHandle{cmd: cmd}, nil
}

// MockProcessHandle simulates a running process in tests.
type MockProcessHandle struct {
	pid     int
	killErr error
	waitErr error
	mu      sync.Mutex
	killed  bool
	waited  bool
}

func (m *MockProcessHandle) Pid() int {
	return m.pid
}

func (m *MockProcessHandle) Kill() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.killed = true
	return m.killErr
}

func (m *MockProcessHandle) Wait() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waited = true
	return m.waitErr
}

// MockDaemonSpawner enables hermetic testing of streaming daemons.
type MockDaemonSpawner struct {
	SpawnFn func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func NewMockDaemonSpawner() *MockDaemonSpawner {
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()

			closeQuietly(inR)
			closeQuietly(outW)
			closeQuietly(errW)

			handle := &MockProcessHandle{pid: 12345}
			return inW, outR, errR, handle, nil
		},
	}
}

func (m *MockDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if m != nil && m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	return NewMockDaemonSpawner().Spawn(ctx, cfg)
}
