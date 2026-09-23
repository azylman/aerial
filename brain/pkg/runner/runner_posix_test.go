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

	// Test killProcessGroupWith and terminateProcessGroupWith with injected mocks
	var mockSigKillCalled, mockProcKillCalled bool
	mockCmd := &exec.Cmd{Process: &os.Process{Pid: 98765}}
	killProcessGroupWith(mockCmd, func(pid int, sig syscall.Signal) error {
		mockSigKillCalled = true
		if pid != -98765 || sig != syscall.SIGKILL {
			t.Errorf("unexpected signal args: pid=%d, sig=%v", pid, sig)
		}
		return errors.New("simulated sigkill error")
	}, func(p *os.Process) error {
		mockProcKillCalled = true
		return errors.New("simulated prockill error")
	})
	if !mockSigKillCalled || !mockProcKillCalled {
		t.Errorf("expected both signal and process kill mocks to be called")
	}

	var mockSigTermCalled bool
	terminateProcessGroupWith(mockCmd, func(pid int, sig syscall.Signal) error {
		mockSigTermCalled = true
		if pid != -98765 || sig != syscall.SIGTERM {
			t.Errorf("unexpected signal args: pid=%d, sig=%v", pid, sig)
		}
		return errors.New("simulated sigterm error")
	})
	if !mockSigTermCalled {
		t.Errorf("expected signal term mock to be called")
	}
}



