//go:build windows

package runner

import (
	"os/exec"
	"time"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	cmd.WaitDelay = 3 * time.Second
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

