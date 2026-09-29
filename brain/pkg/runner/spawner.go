package runner

import (
	"context"
	"errors"
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
	if ctx == nil {
		ctx = context.Background()
	}
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

// SetKillErr configures the error returned by Kill().
func (m *MockProcessHandle) SetKillErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.killErr = err
}

// NewMockProcessHandle creates a MockProcessHandle with the specified PID.
func NewMockProcessHandle(pid int) *MockProcessHandle {
	return &MockProcessHandle{pid: pid}
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

			go func() {
				if _, err := io.Copy(io.Discard, inR); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
					log.Printf("[DaemonSpawner] Warning: failed to discard stdin: %v", err)
				}
			}()
			go func() {
				defer closeQuietly(errW)
				sessID := cfg.SessionID
				if sessID == "" || !IsValidUUID(sessID) {
					sessID = "550e8400-e29b-41d4-a716-446655440000"
				}
				if _, err := fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID); err != nil {
					log.Printf("[DaemonSpawner] Warning: failed to write init event: %v", err)
				}
			}()

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

// MockSpawner records spawned configs and simulates running daemons for testing.
type MockSpawner struct {
	mu           sync.Mutex
	lastSpawnCfg DaemonConfig
	spawnCount   int
	SpawnFn      func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

// NewMockSpawner creates a new MockSpawner with default pipe behavior.
func NewMockSpawner() *MockSpawner {
	m := &MockSpawner{}
	m.SpawnFn = func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		inR, inW := io.Pipe()
		outR, outW := io.Pipe()
		errR, errW := io.Pipe()

		go func() {
			if _, err := io.Copy(io.Discard, inR); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
				log.Printf("[MockSpawner] Warning: failed to discard stdin: %v", err)
			}
		}()
		go func() {
			defer closeQuietly(errW)
			defer closeQuietly(outW)
			sessID := cfg.SessionID
			if sessID == "" || !IsValidUUID(sessID) {
				sessID = "550e8400-e29b-41d4-a716-446655440000"
			}
			if _, err := fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID); err != nil {
				log.Printf("[MockSpawner] Warning: failed to write init event: %v", err)
			}
		}()

		handle := &MockProcessHandle{pid: 12345}
		return inW, outR, errR, handle, nil
	}
	return m
}

func (m *MockSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if m == nil {
		return NewMockSpawner().Spawn(ctx, cfg)
	}
	m.mu.Lock()
	m.lastSpawnCfg = cfg
	m.spawnCount++
	m.mu.Unlock()

	if m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	return NewMockSpawner().Spawn(ctx, cfg)
}

func (m *MockSpawner) LastSpawnCfg() DaemonConfig {
	if m == nil {
		return DaemonConfig{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSpawnCfg
}

