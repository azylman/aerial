package runner

import (
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
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

func closeQuietlyAll(closers ...io.Closer) {
	for _, c := range closers {
		closeQuietly(c)
	}
}

// ConfigureProcessGroup sets up platform-specific process group options on cmd.
func ConfigureProcessGroup(cmd *exec.Cmd) {
	if cmd != nil {
		configureSysProcAttr(cmd)
	}
}

var killProcessGroupFunc = killProcessGroup

// KillProcessGroup terminates the process group associated with cmd.
func KillProcessGroup(cmd *exec.Cmd) error {
	if cmd != nil && cmd.Process != nil {
		killProcessGroupFunc(cmd)
	}
	return nil
}

// OSProcessHandle wraps an *exec.Cmd process.
type OSProcessHandle struct {
	cmd *exec.Cmd
}

func (h *OSProcessHandle) Pid() int {
	if h != nil && h.cmd != nil && h.cmd.Process != nil {
		return h.cmd.Process.Pid
	}
	return 0
}

func (h *OSProcessHandle) Kill() error {
	if h != nil && h.cmd != nil && h.cmd.Process != nil {
		return KillProcessGroup(h.cmd)
	}
	return nil
}

func (h *OSProcessHandle) Wait() error {
	if h != nil && h.cmd != nil {
		return h.cmd.Wait()
	}
	return nil
}

// DefaultDaemonSpawner launches an agy OS subprocess.
type DefaultDaemonSpawner struct{}

func (s *DefaultDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	bin := cfg.AgyBin
	if bin == "" {
		bin = "agy"
	}
	cmd := exec.CommandContext(ctx, bin, BuildDaemonArgs(cfg)...)
	ConfigureProcessGroup(cmd)
	cmd.Dir, cmd.Env = cfg.Cwd, cfg.Env

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
		closeQuietlyAll(stdin, stdout)
		return nil, nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		closeQuietlyAll(stdin, stdout, stderr)
		return nil, nil, nil, nil, fmt.Errorf("failed to start daemon process: %w", err)
	}
	return stdin, stdout, stderr, &OSProcessHandle{cmd: cmd}, nil
}
