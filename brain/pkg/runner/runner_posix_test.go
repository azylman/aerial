//go:build !windows

package runner

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureSysProcAttr_Setpgid(t *testing.T) {
	cmd := exec.Command("true")
	configureSysProcAttr(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}
}

func TestRunnerPosix_SignalAndTerminateHelpers(t *testing.T) {
	terminateProcessGroup(nil)
	cmd := exec.Command("true")
	terminateProcessGroup(cmd)

	if isProcessTerminatedBySignal(nil) {
		t.Errorf("expected false for nil ExitError")
	}

	cmdAlive := exec.Command("sleep", "10")
	configureSysProcAttr(cmdAlive)
	if err := cmdAlive.Start(); err == nil {
		terminateProcessGroup(cmdAlive)
		err := cmdAlive.Wait()
		if exitErr, ok := err.(*exec.ExitError); ok {
			if !isProcessTerminatedBySignal(exitErr) {
				t.Errorf("expected true from isProcessTerminatedBySignal for SIGTERM exit")
			}
		}
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
		err := cmdAlive.Wait()
		if exitErr, ok := err.(*exec.ExitError); ok {
			if !isProcessTerminatedBySignal(exitErr) {
				t.Errorf("expected true from isProcessTerminatedBySignal for SIGKILL exit")
			}
		}
	}

	// Test non-signaled exit (e.g. exit code 1) returns false from isProcessTerminatedBySignal
	cmdExit := exec.Command("sh", "-c", "exit 1")
	if err := cmdExit.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if isProcessTerminatedBySignal(exitErr) {
				t.Errorf("expected false from isProcessTerminatedBySignal for normal exit 1")
			}
		}
	}

	// Edge case process group calls with negative/zero pid or nil process
	terminateProcessGroup(&exec.Cmd{Process: &os.Process{Pid: -1}})
	terminateProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 0}})
	terminateProcessGroup(&exec.Cmd{})
	killProcessGroup(&exec.Cmd{Process: &os.Process{Pid: -1}})
	killProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 0}})
	killProcessGroup(&exec.Cmd{})
}

