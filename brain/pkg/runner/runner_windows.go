//go:build windows

package runner

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"time"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	cmd.WaitDelay = 3 * time.Second
}

func killProcessGroupWith(cmd *exec.Cmd, killFunc func(*os.Process) error) {
	if cmd != nil && cmd.Process != nil {
		if killFunc == nil {
			killFunc = func(p *os.Process) error { return p.Kill() }
		}
		if err := killFunc(cmd.Process); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("[WARN] Failed to kill process %d: %v", cmd.Process.Pid, err)
		}
	}
}

func killProcessGroup(cmd *exec.Cmd) {
	killProcessGroupWith(cmd, nil)
}

func terminateProcessGroupWith(cmd *exec.Cmd, killFunc func(*os.Process) error) {
	killProcessGroupWith(cmd, killFunc)
}

func terminateProcessGroup(cmd *exec.Cmd) {
	killProcessGroup(cmd)
}

func isProcessTerminatedBySignal(exitErr *exec.ExitError) bool {
	return exitErr != nil
}
