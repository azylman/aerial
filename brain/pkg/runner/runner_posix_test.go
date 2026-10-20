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
	cmdPgid := exec.Command("true")
	configureSysProcAttr(cmdPgid)
	if cmdPgid.SysProcAttr == nil || !cmdPgid.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}

	terminateProcessGroup(nil)
	terminateProcessGroup(exec.Command("true"))

	if isProcessTerminatedBySignal(nil) {
		t.Errorf("expected false for nil ExitError")
	}

	testSignalKill := func(killSig bool) {
		cmd := exec.Command("sh", "-c", "read _")
		if stdin, err := cmd.StdinPipe(); err == nil {
			configureSysProcAttr(cmd)
			if err := cmd.Start(); err == nil {
				if killSig {
					killProcessGroup(cmd)
				} else {
					terminateProcessGroup(cmd)
				}
				_ = stdin.Close()
				var exitErr *exec.ExitError
				if errors.As(cmd.Wait(), &exitErr) && !isProcessTerminatedBySignal(exitErr) {
					t.Errorf("expected true from isProcessTerminatedBySignal")
				}
			}
		}
	}
	testSignalKill(false)
	testSignalKill(true)

	if isIgnorableKillError(nil) || !isIgnorableKillError(os.ErrProcessDone) || !isIgnorableKillError(syscall.ESRCH) || isIgnorableKillError(errors.New("other error")) {
		t.Errorf("isIgnorableKillError tests failed")
	}

	killProcessGroup(nil)
	cmdKillDead := exec.Command("true")
	killProcessGroup(cmdKillDead)
	if err := cmdKillDead.Start(); err == nil {
		_ = cmdKillDead.Wait()
		killProcessGroup(cmdKillDead)
	}

	exitCmd := exec.Command("sh", "-c", "exit 1")
	if err := exitCmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && isProcessTerminatedBySignal(exitErr) {
			t.Errorf("expected false from isProcessTerminatedBySignal for normal exit 1")
		}
	}

	for _, c := range []*exec.Cmd{{Process: &os.Process{Pid: -1}}, {Process: &os.Process{Pid: 0}}, {}} {
		terminateProcessGroup(c)
		killProcessGroup(c)
	}

	var mockKills, mockTerms int
	mockCmd := &exec.Cmd{Process: &os.Process{Pid: 98765}}
	killProcessGroupWith(mockCmd, func(int, syscall.Signal) error { mockKills++; return errors.New("sigkill") }, func(*os.Process) error { mockKills++; return errors.New("prockill") })
	terminateProcessGroupWith(mockCmd, func(int, syscall.Signal) error { mockTerms++; return errors.New("sigterm") })
	if mockKills != 2 || mockTerms != 1 {
		t.Errorf("expected 2 mock kills and 1 mock term, got %d, %d", mockKills, mockTerms)
	}
}
