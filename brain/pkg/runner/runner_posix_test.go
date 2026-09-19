//go:build !windows

package runner

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestConfigureSysProcAttr_CancelNilProcess(t *testing.T) {
	cmd := exec.Command("true")
	configureSysProcAttr(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}
	if cmd.WaitDelay != 3*time.Second {
		t.Errorf("expected WaitDelay=3s, got %v", cmd.WaitDelay)
	}
	if cmd.Cancel == nil {
		t.Fatalf("expected non-nil cmd.Cancel")
	}

	// Calling Cancel before process starts (cmd.Process == nil) should return nil safely
	if err := cmd.Cancel(); err != nil {
		t.Errorf("expected nil error when canceling nil process, got %v", err)
	}
}

func TestRunnerPosix_KillHelpers(t *testing.T) {
	if isIgnorableKillError(nil) {
		t.Errorf("expected false for nil")
	}
	if !isIgnorableKillError(os.ErrProcessDone) {
		t.Errorf("expected true for os.ErrProcessDone")
	}
	if !isIgnorableKillError(syscall.ESRCH) {
		t.Errorf("expected true for syscall.ESRCH")
	}
	if isIgnorableKillError(errors.New("other error")) {
		t.Errorf("expected false for other error")
	}

	// Test killProcessGroup with nil and dead command
	killProcessGroup(nil)
	cmd := exec.Command("true")
	killProcessGroup(cmd)
	if err := cmd.Start(); err == nil {
		_ = cmd.Wait()
		killProcessGroup(cmd)
	}

	cmdAlive := exec.Command("sleep", "10")
	configureSysProcAttr(cmdAlive)
	if err := cmdAlive.Start(); err == nil {
		killProcessGroup(cmdAlive)
		_ = cmdAlive.Wait()
	}
}
