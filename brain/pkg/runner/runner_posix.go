//go:build !windows

package runner

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"syscall"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func isIgnorableKillError(err error) bool {
	return err != nil && (errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH))
}

// GroupSignalFunc sends a signal to a process group or process.
type GroupSignalFunc func(pid int, sig syscall.Signal) error

// ProcessKillFunc terminates a single process.
type ProcessKillFunc func(p *os.Process) error

func killProcessGroupWith(cmd *exec.Cmd, signalFunc GroupSignalFunc, killFunc ProcessKillFunc) {
	if cmd != nil && cmd.Process != nil && cmd.Process.Pid > 0 {
		if err := signalFunc(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to kill process group %d: %v", cmd.Process.Pid, err)
		}
		if err := killFunc(cmd.Process); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to kill process %d: %v", cmd.Process.Pid, err)
		}
	}
}

func killProcessGroup(cmd *exec.Cmd) { killProcessGroupWith(cmd, syscall.Kill, (*os.Process).Kill) }

func terminateProcessGroupWith(cmd *exec.Cmd, signalFunc GroupSignalFunc) {
	if cmd != nil && cmd.Process != nil && cmd.Process.Pid > 0 {
		if err := signalFunc(-cmd.Process.Pid, syscall.SIGTERM); err != nil && !isIgnorableKillError(err) {
			log.Printf("[WARN] Failed to send SIGTERM to process group %d: %v", cmd.Process.Pid, err)
		}
	}
}

func terminateProcessGroup(cmd *exec.Cmd) { terminateProcessGroupWith(cmd, syscall.Kill) }

func isWaitStatusSignaled(sys any) bool {
	status, ok := sys.(syscall.WaitStatus)
	return ok && status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL)
}

func isProcessTerminatedBySignal(exitErr *exec.ExitError) bool {
	return exitErr != nil && exitErr.ProcessState != nil && isWaitStatusSignaled(exitErr.Sys())
}
