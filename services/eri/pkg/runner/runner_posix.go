//go:build !windows

package runner

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"syscall"
)

var getEuid = os.Geteuid

const (
	agyUnprivilegedUID = 1000
	agyUnprivilegedGID = 1000
)

func configureSysProcAttr(cmd *exec.Cmd) {
	sysAttr := &syscall.SysProcAttr{Setpgid: true}
	if getEuid() == 0 {
		sysAttr.Credential = &syscall.Credential{Uid: agyUnprivilegedUID, Gid: agyUnprivilegedGID}
	}
	cmd.SysProcAttr = sysAttr
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
