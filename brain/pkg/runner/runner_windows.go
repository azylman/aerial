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

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("[WARN] Failed to kill process %d: %v", cmd.Process.Pid, err)
		}
	}
}

