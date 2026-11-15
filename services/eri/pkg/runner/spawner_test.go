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
	"sync"
	"syscall"
	"testing"
	"time"
)

// MockProcessHandle simulates a running process in tests.
type MockProcessHandle struct {
	pid     int
	killErr error
	waitErr error
	mu      sync.Mutex
	killed  bool
	waited  bool
}

func (m *MockProcessHandle) Pid() int { return m.pid }

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

func (m *MockProcessHandle) SetKillErr(err error) {
	m.mu.Lock()
	m.killErr = err
	m.mu.Unlock()
}

func NewMockProcessHandle(pid int) *MockProcessHandle { return &MockProcessHandle{pid: pid} }

// MockDaemonSpawner enables hermetic testing of streaming daemons.
type MockDaemonSpawner struct {
	SpawnFn func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func spawnDefaultMockPipes(cfg DaemonConfig, closeOutW bool, logPrefix string) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()

	discardIn := func() {
		if _, err := io.Copy(io.Discard, inR); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
			log.Printf("%s Warning: failed to discard stdin: %v", logPrefix, err)
		}
	}
	go func() {
		sessID := cfg.SessionID
		if sessID == "" || !IsValidUUID(sessID) {
			sessID = "550e8400-e29b-41d4-a716-446655440000"
		}
		if _, err := fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID); err != nil {
			log.Printf("%s Warning: failed to write init event: %v", logPrefix, err)
		}
		closeQuietly(errW)
		if closeOutW {
			var buf [4096]byte
			if _, err := inR.Read(buf[:]); err == nil {
				go discardIn()
			}
			closeQuietly(outW)
			return
		}
		discardIn()
	}()
	return inW, outR, errR, &MockProcessHandle{pid: 12345}, nil
}

func NewMockDaemonSpawner() *MockDaemonSpawner { return &MockDaemonSpawner{} }

func (m *MockDaemonSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if m != nil && m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	return spawnDefaultMockPipes(cfg, false, "[DaemonSpawner]")
}

// MockSpawner records spawned configs and simulates running daemons for testing.
type MockSpawner struct {
	mu           sync.Mutex
	lastSpawnCfg DaemonConfig
	spawnCount   int
	SpawnFn      func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
}

func NewMockSpawner() *MockSpawner { return &MockSpawner{} }

func (m *MockSpawner) Spawn(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	if m == nil {
		return spawnDefaultMockPipes(cfg, true, "[MockSpawner]")
	}
	m.mu.Lock()
	m.lastSpawnCfg, m.spawnCount = cfg, m.spawnCount+1
	fn := m.SpawnFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, cfg)
	}
	return spawnDefaultMockPipes(cfg, true, "[MockSpawner]")
}

func (m *MockSpawner) LastSpawnCfg() DaemonConfig {
	if m == nil {
		return DaemonConfig{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSpawnCfg
}

type errCloser struct{ err error }

func (e errCloser) Close() error { return e.err }

func TestMockSpawnersAndProcessHandle(t *testing.T) {
	expectedErr := errors.New("spawn failed")
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, expectedErr
		},
	}
	if _, _, _, _, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"}); !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	for _, s := range []*MockDaemonSpawner{NewMockDaemonSpawner(), {}} {
		stdin, stdout, stderr, handle, err := s.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
		if err != nil || handle.Kill() != nil || handle.Wait() != nil {
			t.Errorf("spawn checks failed: %v", err)
		}
		closeQuietlyAll(stdin, stdout, stderr)
	}

	killErr, waitErr := errors.New("mock kill error"), errors.New("mock wait error")
	phMethods := &MockProcessHandle{pid: 4242, killErr: killErr, waitErr: waitErr}
	if phMethods.Pid() != 4242 || !errors.Is(phMethods.Kill(), killErr) || !phMethods.killed || !errors.Is(phMethods.Wait(), waitErr) || !phMethods.waited {
		t.Errorf("phMethods checks failed")
	}

	var nilSpawner *MockSpawner
	if cfg := nilSpawner.LastSpawnCfg(); cfg.Model != "" {
		t.Errorf("expected empty config from nil spawner, got %+v", cfg)
	}
	for _, s := range []*MockSpawner{nilSpawner, NewMockSpawner()} {
		in, out, errR, handle, err := s.Spawn(context.Background(), DaemonConfig{Model: "test-model-1"})
		if err != nil {
			t.Fatalf("unexpected spawn error: %v", err)
		}
		closeQuietlyAll(in, out, errR)
		_ = handle.Kill()
	}
	sp := NewMockSpawner()
	in2, out2, errR2, handle2, _ := sp.Spawn(context.Background(), DaemonConfig{Model: "test-model-1"})
	closeQuietlyAll(in2, out2, errR2)
	_ = handle2.Kill()
	if sp.LastSpawnCfg().Model != "test-model-1" {
		t.Errorf("expected test-model-1, got %s", sp.LastSpawnCfg().Model)
	}

	customCalled := false
	spawnerCustom := &MockSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			customCalled = true
			return nil, nil, nil, nil, errors.New("custom error")
		},
	}
	if _, _, _, _, errCustom := spawnerCustom.Spawn(context.Background(), DaemonConfig{}); errCustom == nil || !customCalled {
		t.Fatalf("expected custom SpawnFn to be called and return error")
	}

	ph := NewMockProcessHandle(999)
	if ph.Pid() != 999 {
		t.Errorf("expected pid 999, got %d", ph.Pid())
	}
	ph.SetKillErr(errors.New("custom kill err"))
	if err := ph.Kill(); err == nil {
		t.Errorf("expected kill error")
	}
}

func TestOSProcessHandle_And_DefaultDaemonSpawner(t *testing.T) {
	closeQuietly(nil)
	closeQuietly(errCloser{err: errors.New("simulated close error")})

	for _, h := range []*OSProcessHandle{nil, {}} {
		if h.Kill() != nil || h.Wait() != nil {
			t.Errorf("handle check failed for %v", h)
		}
	}

	handleNotStarted := &OSProcessHandle{cmd: exec.Command("dummy")}
	if err := handleNotStarted.Wait(); err == nil {
		t.Errorf("expected wait error for unstarted cmd")
	}

	origKill := killProcessGroupFunc
	defer func() { killProcessGroupFunc = origKill }()

	var killedCmd *exec.Cmd
	killProcessGroupFunc = func(c *exec.Cmd) { killedCmd = c }

	cmdWithProc := &exec.Cmd{Process: &os.Process{Pid: 9876}}
	handleWithProc := &OSProcessHandle{cmd: cmdWithProc}
	if handleWithProc.Kill() != nil || killedCmd != cmdWithProc {
		t.Errorf("handleWithProc failed")
	}

	ConfigureProcessGroup(nil)
	ConfigureProcessGroup(&exec.Cmd{})

	for _, c := range []*exec.Cmd{nil, {}} {
		if err := KillProcessGroup(c); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	var killedCalled bool
	killProcessGroupFunc = func(c *exec.Cmd) { killedCalled = true }
	if err := KillProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 111}}); err != nil || !killedCalled {
		t.Errorf("KillProcessGroup with process failed")
	}

	stdin, stdout, stderr, dHandle, err := (&DefaultDaemonSpawner{}).Spawn(context.Background(), DaemonConfig{
		AgyBin: "/nonexistent/binary/that/cannot/be/started/agy",
	})
	if err == nil || stdin != nil || stdout != nil || stderr != nil || dHandle != nil {
		t.Errorf("expected all nil on spawn error")
	}
}

func TestDefaultDaemonSpawner_RetryAndTransientError(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Binary with sibling .old backup retries and exhausts retries.
	missingWithOldBin := filepath.Join(tempDir, "missing-with-old")
	if err := os.WriteFile(missingWithOldBin+".12345.old", []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("failed creating old backup: %v", err)
	}

	// 2. Binary with non-executable file during replacement.
	nonExecBin := filepath.Join(tempDir, "non-exec-bin")
	if err := os.WriteFile(nonExecBin, []byte("#!/bin/sh\nexit 0\n"), 0o644); err != nil {
		t.Fatalf("failed creating non-executable bin: %v", err)
	}

	// 3. Simulated transient failure recovering on second attempt using mock spawn seam.
	attempts := 0
	mockSpawnOnce := func(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
		attempts++
		if attempts == 1 {
			return nil, nil, nil, nil, syscall.ETXTBSY
		}
		inR, inW := io.Pipe()
		outR, outW := io.Pipe()
		errR, errW := io.Pipe()
		closeQuietlyAll(inR, outW, errW)
		return inW, outR, errR, NewMockProcessHandle(12345), nil
	}

	// 4. Context cancellation during retry timer wait.
	cancelWaitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()

	tests := []struct {
		name          string
		ctx           context.Context
		bin           string
		retryDelays   []time.Duration
		spawnOnceFn   func(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error)
		expectSuccess bool
		expectErrSub  string
	}{
		{
			name:          "missing_binary_with_old_sibling_retries_and_fails_after_exhaustion",
			ctx:           context.Background(),
			bin:           missingWithOldBin,
			retryDelays:   []time.Duration{time.Millisecond, time.Millisecond},
			expectSuccess: false,
		},
		{
			name:          "non_executable_binary_during_replacement_retries_and_fails",
			ctx:           context.Background(),
			bin:           nonExecBin,
			retryDelays:   []time.Duration{time.Millisecond, time.Millisecond},
			expectSuccess: false,
		},
		{
			name:          "recovering_binary_succeeds_on_retry",
			ctx:           context.Background(),
			bin:           "dummy-binary",
			retryDelays:   []time.Duration{time.Millisecond, 2 * time.Millisecond},
			spawnOnceFn:   mockSpawnOnce,
			expectSuccess: true,
		},
		{
			name:          "context_cancellation_during_wait_returns_context_error",
			ctx:           cancelWaitCtx,
			bin:           missingWithOldBin,
			retryDelays:   []time.Duration{500 * time.Millisecond},
			expectSuccess: false,
			expectErrSub:  "failed spawning daemon",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "context_cancellation_during_wait_returns_context_error" {
				go func() {
					timer := time.NewTimer(5 * time.Millisecond)
					<-timer.C
					cancelWait()
				}()
			}
			spawner := &DefaultDaemonSpawner{
				RetryDelays: tc.retryDelays,
				spawnOnceFn: tc.spawnOnceFn,
			}
			stdin, stdout, stderr, handle, err := spawner.Spawn(tc.ctx, DaemonConfig{
				AgyBin: tc.bin,
			})
			if !tc.expectSuccess {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.expectErrSub != "" && !strings.Contains(err.Error(), tc.expectErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.expectErrSub, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected success, got err: %v", err)
			}
			if stdin == nil || stdout == nil || stderr == nil || handle == nil {
				t.Fatalf("expected non-nil pipes and handle on success")
			}
			closeQuietlyAll(stdin, stdout, stderr)
			_ = handle.Kill()
			_ = handle.Wait()
		})
	}

	if attempts != 2 {
		t.Errorf("expected recovering case to take exactly 2 attempts, got %d", attempts)
	}
}

func TestTransientBinarySpawnHelperFunctions(t *testing.T) {
	tempDir := t.TempDir()

	// isTransientBinarySpawnError ETXTBSY check
	if !isTransientBinarySpawnError("somebin", "", syscall.ETXTBSY) {
		t.Errorf("expected true for ETXTBSY")
	}

	// Relative path with Cwd check
	cwdSubdir := filepath.Join(tempDir, "subproject")
	if err := os.MkdirAll(cwdSubdir, 0o755); err != nil {
		t.Fatalf("failed creating subdir: %v", err)
	}
	relBin := "./bin/custom-agy"
	absRelTarget := filepath.Join(cwdSubdir, relBin)
	if err := os.MkdirAll(filepath.Dir(absRelTarget), 0o755); err != nil {
		t.Fatalf("failed creating target dir: %v", err)
	}
	if hasActiveBinaryReplacement(relBin, cwdSubdir) {
		t.Errorf("expected false before file creation")
	}
	if err := os.WriteFile(absRelTarget+".123.old", []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("failed creating old backup in cwd: %v", err)
	}
	if !hasActiveBinaryReplacement(relBin, cwdSubdir) {
		t.Errorf("expected true for relative bin with .old in Cwd")
	}

	// PATH lookup test for non-slash binary
	binName := "test-custom-bin-unique"
	origPath := os.Getenv("PATH")
	t.Setenv("PATH", tempDir+":"+origPath)

	if hasActiveBinaryReplacement(binName, "") {
		t.Errorf("expected false when no file exists")
	}

	// Create non-executable in tempDir (which is in PATH)
	targetInPath := filepath.Join(tempDir, binName)
	if err := os.WriteFile(targetInPath, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatalf("failed creating target: %v", err)
	}

	if !hasActiveBinaryReplacement(binName, "") {
		t.Errorf("expected true for non-executable binary found in PATH")
	}

	// Make executable
	if err := os.Chmod(targetInPath, 0o755); err != nil {
		t.Fatalf("failed chmod: %v", err)
	}
	if hasActiveBinaryReplacement(binName, "") {
		t.Errorf("expected false for executable binary with no .old sibling")
	}

	// Add .old sibling
	oldPath := targetInPath + ".999.old"
	if err := os.WriteFile(oldPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("failed creating oldPath: %v", err)
	}
	if !hasActiveBinaryReplacement(binName, "") {
		t.Errorf("expected true for binary with .old sibling in PATH")
	}

	// Default delays fallback in Spawn
	spawnerDefault := &DefaultDaemonSpawner{}
	in, out, errR, ph, err := spawnerDefault.Spawn(nil, DaemonConfig{
		AgyBin: "/nonexistent/binary/that/never/exists",
	})
	if err == nil {
		closeQuietlyAll(in, out, errR)
		if ph != nil {
			_ = ph.Kill()
		}
		t.Errorf("expected spawn error with default spawner delays")
	}
}
