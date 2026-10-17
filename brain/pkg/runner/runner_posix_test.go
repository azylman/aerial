//go:build !windows

package runner

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestRunnerPosix_ProcessGroupAndSignalHelpers(t *testing.T) {
	// SysProcAttr Setpgid
	cmdPgid := exec.Command("true")
	configureSysProcAttr(cmdPgid)
	if cmdPgid.SysProcAttr == nil || !cmdPgid.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}

	// Nil / dead command terminateProcessGroup
	terminateProcessGroup(nil)
	cmdTrue := exec.Command("true")
	terminateProcessGroup(cmdTrue)

	// isProcessTerminatedBySignal nil check
	if isProcessTerminatedBySignal(nil) {
		t.Errorf("expected false for nil ExitError")
	}

	// Terminate process group with live process (sh -c "read _" with stdin pipe)
	cmdAliveTerm := exec.Command("sh", "-c", "read _")
	stdinTerm, err := cmdAliveTerm.StdinPipe()
	if err == nil {
		configureSysProcAttr(cmdAliveTerm)
		if err := cmdAliveTerm.Start(); err == nil {
			terminateProcessGroup(cmdAliveTerm)
			_ = stdinTerm.Close()
			err := cmdAliveTerm.Wait()
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if !isProcessTerminatedBySignal(exitErr) {
					t.Errorf("expected true from isProcessTerminatedBySignal for SIGTERM exit")
				}
			}
		}
	}

	// Ignorable kill errors
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

	// killProcessGroup with nil and dead command
	killProcessGroup(nil)
	cmdKillDead := exec.Command("true")
	killProcessGroup(cmdKillDead)
	if err := cmdKillDead.Start(); err == nil {
		_ = cmdKillDead.Wait()
		killProcessGroup(cmdKillDead)
	}

	// Kill process group with live process (sh -c "read _" with stdin pipe)
	cmdAliveKill := exec.Command("sh", "-c", "read _")
	stdinKill, err := cmdAliveKill.StdinPipe()
	if err == nil {
		configureSysProcAttr(cmdAliveKill)
		if err := cmdAliveKill.Start(); err == nil {
			killProcessGroup(cmdAliveKill)
			_ = stdinKill.Close()
			err := cmdAliveKill.Wait()
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if !isProcessTerminatedBySignal(exitErr) {
					t.Errorf("expected true from isProcessTerminatedBySignal for SIGKILL exit")
				}
			}
		}
	}

	// Non-signaled exit (exit code 1)
	exitCmd := exec.Command("sh", "-c", "exit 1")
	if err := exitCmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if isProcessTerminatedBySignal(exitErr) {
				t.Errorf("expected false from isProcessTerminatedBySignal for normal exit 1")
			}
		}
	}

	// Negative/zero PID or nil process edge cases
	terminateProcessGroup(&exec.Cmd{Process: &os.Process{Pid: -1}})
	terminateProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 0}})
	terminateProcessGroup(&exec.Cmd{})
	killProcessGroup(&exec.Cmd{Process: &os.Process{Pid: -1}})
	killProcessGroup(&exec.Cmd{Process: &os.Process{Pid: 0}})
	killProcessGroup(&exec.Cmd{})

	// Dependency-injected mocks for killProcessGroupWith and terminateProcessGroupWith
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
