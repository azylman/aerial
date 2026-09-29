package runner

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
)

type errCloser struct {
	err error
}

func (e errCloser) Close() error {
	return e.err
}

func TestCloseQuietly(t *testing.T) {
	closeQuietly(nil)
	closeQuietly(errCloser{err: errors.New("simulated close error")})
}

func TestMockDaemonSpawner_CustomFunction(t *testing.T) {
	expectedErr := errors.New("spawn failed")
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, expectedErr
		},
	}

	_, _, _, _, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockDaemonSpawner_DefaultPipeBehavior(t *testing.T) {
	mock := NewMockDaemonSpawner()
	stdin, stdout, stderr, handle, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
	if err != nil {
		t.Fatalf("unexpected spawn error: %v", err)
	}
	defer stdin.Close()
	defer stdout.Close()
	defer stderr.Close()

	if handle.Pid() <= 0 {
		t.Errorf("expected positive pid, got %d", handle.Pid())
	}
	if err := handle.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if err := handle.Wait(); err != nil {
		t.Errorf("unexpected wait error: %v", err)
	}
}

func TestMockDaemonSpawner_NilSpawnFnFallback(t *testing.T) {
	mock := &MockDaemonSpawner{}
	stdin, stdout, stderr, handle, err := mock.Spawn(context.Background(), DaemonConfig{SessionID: "fallback-sess"})
	if err != nil {
		t.Fatalf("unexpected spawn error with nil SpawnFn: %v", err)
	}
	defer stdin.Close()
	defer stdout.Close()
	defer stderr.Close()

	if handle.Pid() <= 0 {
		t.Errorf("expected positive pid, got %d", handle.Pid())
	}
	if err := handle.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if err := handle.Wait(); err != nil {
		t.Errorf("unexpected wait error: %v", err)
	}
}

func TestMockProcessHandle_Methods(t *testing.T) {
	killErr := errors.New("mock kill error")
	waitErr := errors.New("mock wait error")
	handle := &MockProcessHandle{
		pid:     4242,
		killErr: killErr,
		waitErr: waitErr,
	}

	if handle.Pid() != 4242 {
		t.Errorf("expected pid 4242, got %d", handle.Pid())
	}
	if err := handle.Kill(); !errors.Is(err, killErr) {
		t.Errorf("expected killErr %v, got %v", killErr, err)
	}
	if !handle.killed {
		t.Errorf("expected handle to be marked killed")
	}
	if err := handle.Wait(); !errors.Is(err, waitErr) {
		t.Errorf("expected waitErr %v, got %v", waitErr, err)
	}
	if !handle.waited {
		t.Errorf("expected handle to be marked waited")
	}
}

func TestOSProcessHandle_NilAndMock(t *testing.T) {
	var nilHandle *OSProcessHandle
	if nilHandle.Pid() != 0 {
		t.Errorf("expected 0 pid for nil handle, got %d", nilHandle.Pid())
	}
	if err := nilHandle.Kill(); err != nil {
		t.Errorf("expected nil kill error for nil handle, got %v", err)
	}
	if err := nilHandle.Wait(); err != nil {
		t.Errorf("expected nil wait error for nil handle, got %v", err)
	}

	emptyHandle := &OSProcessHandle{}
	if emptyHandle.Pid() != 0 {
		t.Errorf("expected 0 pid for empty handle, got %d", emptyHandle.Pid())
	}
	if err := emptyHandle.Kill(); err != nil {
		t.Errorf("expected nil kill error for empty handle, got %v", err)
	}
	if err := emptyHandle.Wait(); err != nil {
		t.Errorf("expected nil wait error for empty handle, got %v", err)
	}

	cmdNotStarted := exec.Command("dummy")
	handleNotStarted := &OSProcessHandle{cmd: cmdNotStarted}
	if err := handleNotStarted.Wait(); err == nil {
		t.Errorf("expected wait error for unstarted cmd, got nil")
	}

	origKill := killProcessGroupFunc
	defer func() { killProcessGroupFunc = origKill }()

	var killedCmd *exec.Cmd
	killProcessGroupFunc = func(c *exec.Cmd) {
		killedCmd = c
	}

	cmdWithProc := &exec.Cmd{Process: &os.Process{Pid: 9876}}
	handleWithProc := &OSProcessHandle{cmd: cmdWithProc}
	if handleWithProc.Pid() != 9876 {
		t.Errorf("expected pid 9876, got %d", handleWithProc.Pid())
	}
	if err := handleWithProc.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if killedCmd != cmdWithProc {
		t.Errorf("expected killProcessGroupFunc to be called with cmdWithProc")
	}
}

func TestConfigureAndKillProcessGroup(t *testing.T) {
	ConfigureProcessGroup(nil)
	cmd := &exec.Cmd{}
	ConfigureProcessGroup(cmd)

	if err := KillProcessGroup(nil); err != nil {
		t.Errorf("unexpected error for nil cmd: %v", err)
	}
	if err := KillProcessGroup(&exec.Cmd{}); err != nil {
		t.Errorf("unexpected error for cmd with nil Process: %v", err)
	}

	origKill := killProcessGroupFunc
	defer func() { killProcessGroupFunc = origKill }()

	var killedCalled bool
	killProcessGroupFunc = func(c *exec.Cmd) {
		killedCalled = true
	}
	if err := KillProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 111}}); err != nil {
		t.Errorf("unexpected error for cmd with process: %v", err)
	}
	if !killedCalled {
		t.Errorf("expected killProcessGroupFunc to be called")
	}
}

func TestDefaultDaemonSpawner_SpawnError(t *testing.T) {
	spawner := &DefaultDaemonSpawner{}
	cfg := DaemonConfig{
		AgyBin: "/nonexistent/binary/that/cannot/be/started/agy",
	}
	stdin, stdout, stderr, handle, err := spawner.Spawn(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error spawning nonexistent binary, got nil")
	}
	if stdin != nil || stdout != nil || stderr != nil || handle != nil {
		t.Errorf("expected all returned values to be nil on spawn error")
	}
}

func TestMockSpawner_MethodsAndNilHandling(t *testing.T) {
	var nilSpawner *MockSpawner
	if cfg := nilSpawner.LastSpawnCfg(); cfg.Model != "" {
		t.Errorf("expected empty config from nil spawner, got %+v", cfg)
	}
	in, out, errR, handle, err := nilSpawner.Spawn(context.Background(), DaemonConfig{Model: "default-nil"})
	if err != nil {
		t.Fatalf("unexpected spawn error from nil spawner: %v", err)
	}
	_ = in.Close()
	_ = out.Close()
	_ = errR.Close()
	_ = handle.Kill()

	spawner := NewMockSpawner()
	in2, out2, errR2, handle2, err2 := spawner.Spawn(context.Background(), DaemonConfig{Model: "test-model-1"})
	if err2 != nil {
		t.Fatalf("unexpected spawn error: %v", err2)
	}
	_ = in2.Close()
	_ = out2.Close()
	_ = errR2.Close()
	_ = handle2.Kill()

	if spawner.LastSpawnCfg().Model != "test-model-1" {
		t.Errorf("expected test-model-1, got %s", spawner.LastSpawnCfg().Model)
	}

	// Custom SpawnFn on MockSpawner
	customCalled := false
	spawnerCustom := &MockSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			customCalled = true
			return nil, nil, nil, nil, errors.New("custom error")
		},
	}
	_, _, _, _, errCustom := spawnerCustom.Spawn(context.Background(), DaemonConfig{})
	if errCustom == nil || !customCalled {
		t.Fatalf("expected custom SpawnFn to be called and return error")
	}

	// NewMockProcessHandle and SetKillErr
	ph := NewMockProcessHandle(999)
	if ph.Pid() != 999 {
		t.Errorf("expected pid 999, got %d", ph.Pid())
	}
	ph.SetKillErr(errors.New("custom kill err"))
	if err := ph.Kill(); err == nil {
		t.Errorf("expected kill error")
	}
}

