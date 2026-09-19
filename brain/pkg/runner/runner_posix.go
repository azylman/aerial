//go:build !windows

package runner

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 3 * time.Second
}

func isIgnorableKillError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH)
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil && cmd.Process.Pid > 0 {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to kill process group %d: %v", cmd.Process.Pid, err)
		}
		if err := cmd.Process.Kill(); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to kill process %d: %v", cmd.Process.Pid, err)
		}
	}
}

func terminateProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil && cmd.Process.Pid > 0 {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to send SIGTERM to process group %d: %v", cmd.Process.Pid, err)
		}
	}
}

func isProcessTerminatedBySignal(exitErr *exec.ExitError) bool {
	if exitErr == nil {
		return false
	}
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		return status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL)
	}
	return false
}
