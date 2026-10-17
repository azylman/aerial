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

func TestMockSpawnersAndProcessHandle(t *testing.T) {
	// MockDaemonSpawner CustomFunction
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

	// MockDaemonSpawner DefaultPipeBehavior
	mockDefault := NewMockDaemonSpawner()
	stdin, stdout, stderr, handle, err := mockDefault.Spawn(context.Background(), DaemonConfig{SessionID: "test-sess"})
	if err != nil {
		t.Fatalf("unexpected spawn error: %v", err)
	}
	_ = stdin.Close()
	_ = stdout.Close()
	_ = stderr.Close()
	if handle.Pid() <= 0 {
		t.Errorf("expected positive pid, got %d", handle.Pid())
	}
	if err := handle.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if err := handle.Wait(); err != nil {
		t.Errorf("unexpected wait error: %v", err)
	}

	// MockDaemonSpawner NilSpawnFnFallback
	mockFallback := &MockDaemonSpawner{}
	stdin2, stdout2, stderr2, handle2, err := mockFallback.Spawn(context.Background(), DaemonConfig{SessionID: "fallback-sess"})
	if err != nil {
		t.Fatalf("unexpected spawn error with nil SpawnFn: %v", err)
	}
	_ = stdin2.Close()
	_ = stdout2.Close()
	_ = stderr2.Close()
	if handle2.Pid() <= 0 {
		t.Errorf("expected positive pid, got %d", handle2.Pid())
	}
	if err := handle2.Kill(); err != nil {
		t.Errorf("unexpected kill error: %v", err)
	}
	if err := handle2.Wait(); err != nil {
		t.Errorf("unexpected wait error: %v", err)
	}

	// MockProcessHandle Methods
	killErr := errors.New("mock kill error")
	waitErr := errors.New("mock wait error")
	phMethods := &MockProcessHandle{
		pid:     4242,
		killErr: killErr,
		waitErr: waitErr,
	}
	if phMethods.Pid() != 4242 {
		t.Errorf("expected pid 4242, got %d", phMethods.Pid())
	}
	if err := phMethods.Kill(); !errors.Is(err, killErr) {
		t.Errorf("expected killErr %v, got %v", killErr, err)
	}
	if !phMethods.killed {
		t.Errorf("expected handle to be marked killed")
	}
	if err := phMethods.Wait(); !errors.Is(err, waitErr) {
		t.Errorf("expected waitErr %v, got %v", waitErr, err)
	}
	if !phMethods.waited {
		t.Errorf("expected handle to be marked waited")
	}

	// MockSpawner Methods and Nil Handling
	var nilSpawner *MockSpawner
	if cfg := nilSpawner.LastSpawnCfg(); cfg.Model != "" {
		t.Errorf("expected empty config from nil spawner, got %+v", cfg)
	}
	inN, outN, errRN, handleN, err := nilSpawner.Spawn(context.Background(), DaemonConfig{Model: "default-nil"})
	if err != nil {
		t.Fatalf("unexpected spawn error from nil spawner: %v", err)
	}
	_ = inN.Close()
	_ = outN.Close()
	_ = errRN.Close()
	_ = handleN.Kill()

	spawner := NewMockSpawner()
	inS, outS, errRS, handleS, errS := spawner.Spawn(context.Background(), DaemonConfig{Model: "test-model-1"})
	if errS != nil {
		t.Fatalf("unexpected spawn error: %v", errS)
	}
	_ = inS.Close()
	_ = outS.Close()
	_ = errRS.Close()
	_ = handleS.Kill()
	if spawner.LastSpawnCfg().Model != "test-model-1" {
		t.Errorf("expected test-model-1, got %s", spawner.LastSpawnCfg().Model)
	}

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
	// CloseQuietly
	closeQuietly(nil)
	closeQuietly(errCloser{err: errors.New("simulated close error")})

	// Nil and empty OSProcessHandle
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

	// ConfigureProcessGroup & KillProcessGroup
	ConfigureProcessGroup(nil)
	cmd := &exec.Cmd{}
	ConfigureProcessGroup(cmd)

	if err := KillProcessGroup(nil); err != nil {
		t.Errorf("unexpected error for nil cmd: %v", err)
	}
	if err := KillProcessGroup(&exec.Cmd{}); err != nil {
		t.Errorf("unexpected error for cmd with nil Process: %v", err)
	}

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

	// DefaultDaemonSpawner SpawnError
	spawner := &DefaultDaemonSpawner{}
	cfg := DaemonConfig{
		AgyBin: "/nonexistent/binary/that/cannot/be/started/agy",
	}
	stdin, stdout, stderr, dHandle, err := spawner.Spawn(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error spawning nonexistent binary, got nil")
	}
	if stdin != nil || stdout != nil || stderr != nil || dHandle != nil {
		t.Errorf("expected all returned values to be nil on spawn error")
	}
}
