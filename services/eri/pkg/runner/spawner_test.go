package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
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
	_, _, _, _, _ = sp.Spawn(context.Background(), DaemonConfig{Model: "test-model-1"})
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
