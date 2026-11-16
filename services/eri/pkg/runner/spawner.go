package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// ProcessHandle abstracts an operating system process.
type ProcessHandle interface {
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

var defaultSpawnRetryDelays = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	1500 * time.Millisecond,
	1600 * time.Millisecond,
}

// DefaultDaemonSpawner launches an agy OS subprocess.
type DefaultDaemonSpawner struct {
	RetryDelays []time.Duration
	spawnOnceFn func(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func spawnOnce(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	ConfigureProcessGroup(cmd)
	cmd.Dir = cfg.Cwd
	cmd.Env = cfg.Env
	if len(cmd.Env) == 0 {
		cmd.Env = BuildAgyEnv(AgyEnvInput{
			HomeDir:     cfg.GeminiHomeDir,
			TargetID:    cfg.ThreadID,
			TurnEnvFile: cfg.TurnEnvFile,
		})
	}

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

func isTransientBinarySpawnError(bin string, cwd string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ETXTBSY) {
		return true
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return hasActiveBinaryReplacement(bin, cwd)
	}
	return false
}

// usrLocalBinDir is checked first for bare binary names, ahead of $PATH.
const usrLocalBinDir = "/usr/local/bin"

func hasActiveBinaryReplacement(bin string, cwd string) bool {
	if bin == "" {
		return false
	}

	var candidates []string
	switch {
	case filepath.IsAbs(bin):
		candidates = []string{filepath.Clean(bin)}
	case strings.Contains(bin, "/"):
		if cwd != "" {
			candidates = []string{filepath.Join(cwd, bin)}
		} else {
			candidates = []string{filepath.Clean(bin)}
		}
	default:
		candidates = append(candidates, filepath.Join(usrLocalBinDir, bin))
		for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
			if dir != "" {
				candidates = append(candidates, filepath.Join(dir, bin))
			}
		}
	}

	for _, cand := range candidates {
		matches, err := filepath.Glob(cand + ".*.old")
		if err == nil && len(matches) > 0 {
			return true
		}
		info, err := os.Stat(cand)
		if err == nil && info.Mode().IsRegular() && (info.Mode().Perm()&0o111 == 0) {
			return true
		}
	}

	return false
}

func (s *DefaultDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	bin := cfg.AgyBin
	if cfg.IsClaude() {
		bin = cfg.ClaudeBin
		if bin == "" {
			bin = "claude"
		}
	} else if bin == "" {
		bin = "agy"
	}
	args := BuildDaemonArgs(cfg)

	delays := s.RetryDelays
	if len(delays) == 0 {
		delays = defaultSpawnRetryDelays
	}

	spawnFn := spawnOnce
	if s.spawnOnceFn != nil {
		spawnFn = s.spawnOnceFn
	}

	attempt := 0
	for {
		stdin, stdout, stderr, handle, err := spawnFn(ctx, bin, args, cfg)
		if err == nil {
			return stdin, stdout, stderr, handle, nil
		}

		if attempt >= len(delays) || !isTransientBinarySpawnError(bin, cfg.Cwd, err) {
			return nil, nil, nil, nil, err
		}

		delay := delays[attempt]
		attempt++

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, nil, nil, fmt.Errorf("failed spawning daemon: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
