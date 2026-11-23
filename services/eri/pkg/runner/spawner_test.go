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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSpawnCfg
}

type errCloser struct{ err error }

func (e errCloser) Close() error { return e.err }

func assertDefaultDaemonSpawnerEngine(t *testing.T, engine, customBin, defaultBin string) {
	t.Helper()
	var spawnedBin string
	customSpawner := &DefaultDaemonSpawner{
		spawnOnceFn: func(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnedBin = bin
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			closeQuietlyAll(inR, outW, errW)
			return inW, outR, errR, NewMockProcessHandle(111), nil
		},
	}

	cfgCustom := DaemonConfig{Engine: engine}
	switch engine {
	case engineCodex:
		cfgCustom.CodexBin = customBin
	case engineMuse:
		cfgCustom.MuseBin = customBin
	}

	inC, outC, errC, hC, errCSpawn := customSpawner.Spawn(context.Background(), cfgCustom)
	if errCSpawn != nil || spawnedBin != customBin {
		t.Errorf("expected %s, got %s (err: %v)", customBin, spawnedBin, errCSpawn)
	}
	closeQuietlyAll(inC, outC, errC)
	if hC != nil {
		if err := hC.Kill(); err != nil {
			t.Errorf("hC.Kill: %v", err)
		}
	}

	inCDef, outCDef, errCDef, hCDef, errCDefSpawn := customSpawner.Spawn(context.Background(), DaemonConfig{
		Engine: engine,
	})
	if errCDefSpawn != nil || spawnedBin != defaultBin {
		t.Errorf("expected default %s, got %s (err: %v)", defaultBin, spawnedBin, errCDefSpawn)
	}
	closeQuietlyAll(inCDef, outCDef, errCDef)
	if hCDef != nil {
		if err := hCDef.Kill(); err != nil {
			t.Errorf("hCDef.Kill: %v", err)
		}
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

	assertDefaultDaemonSpawnerEngine(t, engineCodex, "/custom/bin/codex", "codex")
	assertDefaultDaemonSpawnerEngine(t, engineMuse, "/custom/bin/muse", "muse")
}

// retrySpawnStep scripts one spawnOnceFn attempt: delegate to the real spawnOnce, fail with err
// (optionally cancelling the startup context first), or (zero value) succeed with pipes.
type retrySpawnStep struct {
	real   bool
	err    error
	cancel bool
}

// retrySpawnScript replays steps in order (repeating the last one) and counts attempts.
type retrySpawnScript struct {
	steps    []retrySpawnStep
	cancel   context.CancelFunc
	attempts int
}

func (s *retrySpawnScript) spawn(ctx context.Context, bin string, args []string, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
	step := s.steps[min(s.attempts, len(s.steps)-1)]
	s.attempts++
	if step.cancel {
		s.cancel()
	}
	if step.real {
		return spawnOnce(ctx, bin, args, cfg)
	}
	if step.err != nil {
		return nil, nil, nil, nil, step.err
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	closeQuietlyAll(inR, outW, errW)
	return inW, outR, errR, NewMockProcessHandle(12345), nil
}

func TestDefaultDaemonSpawner_RetryAndTransientError(t *testing.T) {
	tempDir := t.TempDir()
	// A missing binary with a sibling .old backup and a non-executable binary both look like an
	// in-progress binary replacement, so their spawn errors are transient.
	if err := os.WriteFile(filepath.Join(tempDir, "missing-with-old.12345.old"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("failed creating old backup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "non-exec-bin"), []byte("#!/bin/sh\nexit 0\n"), 0o644); err != nil {
		t.Fatalf("failed creating non-executable bin: %v", err)
	}
	ms := time.Millisecond

	tests := []struct {
		name         string
		bin          string
		retryDelays  []time.Duration
		steps        []retrySpawnStep
		wantAttempts int
		wantOK       bool
		wantErrSub   string
		wantCanceled bool
	}{
		{name: "missing binary with old sibling exhausts retries", bin: "missing-with-old", retryDelays: []time.Duration{ms, ms}, steps: []retrySpawnStep{{real: true}}, wantAttempts: 3},
		{name: "non-executable binary during replacement exhausts retries", bin: "non-exec-bin", retryDelays: []time.Duration{ms, ms}, steps: []retrySpawnStep{{real: true}}, wantAttempts: 3},
		{name: "recovering binary succeeds on retry", bin: "dummy-binary", retryDelays: []time.Duration{ms, 2 * ms}, steps: []retrySpawnStep{{err: syscall.ETXTBSY}, {}}, wantAttempts: 2, wantOK: true},
		{
			name: "cancellation during retry wait returns context error", bin: "missing-with-old", retryDelays: []time.Duration{500 * ms},
			steps: []retrySpawnStep{{err: syscall.ETXTBSY, cancel: true}}, wantAttempts: 1, wantErrSub: "failed spawning daemon", wantCanceled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			script := &retrySpawnScript{steps: tc.steps, cancel: cancel}
			spawner := &DefaultDaemonSpawner{RetryDelays: tc.retryDelays, spawnOnceFn: script.spawn}
			stdin, stdout, stderr, handle, err := spawner.Spawn(ctx, DaemonConfig{AgyBin: filepath.Join(tempDir, tc.bin)})
			if script.attempts != tc.wantAttempts {
				t.Errorf("attempts = %d, want %d", script.attempts, tc.wantAttempts)
			}
			if !tc.wantOK {
				assertSpawnFailure(t, err, tc.wantErrSub, tc.wantCanceled)
				return
			}
			if err != nil || stdin == nil || stdout == nil || stderr == nil || handle == nil {
				t.Fatalf("expected success with valid pipes and handle, got err: %v", err)
			}
			closeQuietlyAll(stdin, stdout, stderr)
			if err := handle.Kill(); err != nil {
				t.Fatalf("handle.Kill: %v", err)
			}
			if err := handle.Wait(); err != nil {
				t.Fatalf("handle.Wait: %v", err)
			}
		})
	}
}

func assertSpawnFailure(t *testing.T, err error, expectSub string, assertCtx bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if expectSub != "" && !strings.Contains(err.Error(), expectSub) {
		t.Fatalf("expected error containing %q, got %v", expectSub, err)
	}
	if assertCtx && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected errors.Is(err, context.Canceled), got %v", err)
	}
}

func TestTransientBinarySpawnHelperFunctions(t *testing.T) {
	tempDir := t.TempDir()

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
	in, out, errR, ph, err := spawnerDefault.Spawn(t.Context(), DaemonConfig{
		AgyBin: "/nonexistent/binary/that/never/exists",
	})
	if err == nil {
		closeQuietlyAll(in, out, errR)
		if ph != nil {
			if err := ph.Kill(); err != nil {
				t.Errorf("ph.Kill: %v", err)
			}
		}
		t.Errorf("expected spawn error with default spawner delays")
	}
}
