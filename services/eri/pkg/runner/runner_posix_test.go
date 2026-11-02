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
	origGetEuid := getEuid
	defer func() { getEuid = origGetEuid }()

	getEuid = func() int { return 1000 }
	cmdPgid := &exec.Cmd{}
	configureSysProcAttr(cmdPgid)
	if cmdPgid.SysProcAttr == nil || !cmdPgid.SysProcAttr.Setpgid || cmdPgid.SysProcAttr.Credential != nil {
		t.Errorf("expected SysProcAttr.Setpgid=true and nil Credential for non-root")
	}

	getEuid = func() int { return 0 }
	cmdRoot := &exec.Cmd{}
	configureSysProcAttr(cmdRoot)
	if cmdRoot.SysProcAttr == nil || !cmdRoot.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}
	if cmdRoot.SysProcAttr.Credential == nil || cmdRoot.SysProcAttr.Credential.Uid != agyUnprivilegedUID || cmdRoot.SysProcAttr.Credential.Gid != agyUnprivilegedGID {
		t.Errorf("expected Credential UID/GID 1000 for root, got: %v", cmdRoot.SysProcAttr.Credential)
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
