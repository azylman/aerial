//go:build windows

package runner

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestConfigureSysProcAttr_Windows(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("cmd.exe")
	configureSysProcAttr(cmd)
	if cmd.WaitDelay != 3*time.Second {
		t.Errorf("expected WaitDelay=3s, got %v", cmd.WaitDelay)
	}

	killProcessGroup(nil)
	killProcessGroup(&exec.Cmd{})
	terminateProcessGroup(nil)

	// Test killProcessGroupWith and terminateProcessGroupWith with custom mock
	var customKillCalled bool
	mockCmd := &exec.Cmd{Process: &os.Process{Pid: 12345}}
	killProcessGroupWith(mockCmd, func(p *os.Process) error {
		customKillCalled = true
		return errors.New("simulated kill error")
	})
	if !customKillCalled {
		t.Errorf("expected custom killFunc to be called")
	}

	var customTermCalled bool
	terminateProcessGroupWith(mockCmd, func(p *os.Process) error {
		customTermCalled = true
		return os.ErrProcessDone
	})
	if !customTermCalled {
		t.Errorf("expected custom termFunc to be called")
	}

	if isProcessTerminatedBySignal(nil) {
		t.Errorf("expected false for nil ExitError")
	}
	if !isProcessTerminatedBySignal(&exec.ExitError{}) {
		t.Errorf("expected true for non-nil ExitError on Windows")
	}
}

