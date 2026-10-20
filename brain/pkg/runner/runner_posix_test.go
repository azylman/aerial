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
	cmdPgid := &exec.Cmd{}
	configureSysProcAttr(cmdPgid)
	if cmdPgid.SysProcAttr == nil || !cmdPgid.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}

	if isProcessTerminatedBySignal(nil) || isProcessTerminatedBySignal(&exec.ExitError{}) || isProcessTerminatedBySignal(&exec.ExitError{ProcessState: &os.ProcessState{}}) {
		t.Errorf("expected false for nil or un-signaled ExitError")
	}
	if !isWaitStatusSignaled(syscall.WaitStatus(syscall.SIGTERM)) || !isWaitStatusSignaled(syscall.WaitStatus(syscall.SIGKILL)) || isWaitStatusSignaled(syscall.WaitStatus(0)) || isWaitStatusSignaled("invalid") {
		t.Errorf("isWaitStatusSignaled assertions failed")
	}

	if isIgnorableKillError(nil) || !isIgnorableKillError(os.ErrProcessDone) || !isIgnorableKillError(syscall.ESRCH) || isIgnorableKillError(errors.New("other error")) {
		t.Errorf("isIgnorableKillError assertions failed")
	}

	for _, c := range []*exec.Cmd{nil, {}, {Process: &os.Process{Pid: -1}}, {Process: &os.Process{Pid: 0}}} {
		terminateProcessGroup(c)
		killProcessGroup(c)
	}

	var mockKills, mockTerms int
	mockCmd := &exec.Cmd{Process: &os.Process{Pid: 98765}}
	killProcessGroupWith(mockCmd, func(pid int, sig syscall.Signal) error {
		if pid != -98765 || sig != syscall.SIGKILL {
			t.Errorf("unexpected sigkill args: %d, %v", pid, sig)
		}
		mockKills++
		return errors.New("sigkill")
	}, func(*os.Process) error {
		mockKills++
		return errors.New("prockill")
	})
	terminateProcessGroupWith(mockCmd, func(pid int, sig syscall.Signal) error {
		if pid != -98765 || sig != syscall.SIGTERM {
			t.Errorf("unexpected sigterm args: %d, %v", pid, sig)
		}
		mockTerms++
		return errors.New("sigterm")
	})
	if mockKills != 2 || mockTerms != 1 {
		t.Errorf("expected 2 mock kills and 1 mock term, got %d, %d", mockKills, mockTerms)
	}
}
