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

	if isIgnorableKillError(nil) || !isIgnorableKillError(os.ErrProcessDone) || !isIgnorableKillError(syscall.ESRCH) || isIgnorableKillError(errors.New("other error")) {
		t.Errorf("isIgnorableKillError assertions failed")
	}

	for _, c := range []*exec.Cmd{nil, {}, {Process: &os.Process{Pid: -1}}, {Process: &os.Process{Pid: 0}}} {
		killProcessGroup(c)
	}

	var mockKills int
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
	if mockKills != 2 {
		t.Errorf("expected 2 mock kills, got %d", mockKills)
	}
}
