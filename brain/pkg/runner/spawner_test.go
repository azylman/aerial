package runner

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
)

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
		if err != nil || handle.Pid() <= 0 || handle.Kill() != nil || handle.Wait() != nil {
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
		if h.Pid() != 0 || h.Kill() != nil || h.Wait() != nil {
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
	if handleWithProc.Pid() != 9876 || handleWithProc.Kill() != nil || killedCmd != cmdWithProc {
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
