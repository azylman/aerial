//go:build windows

package runner

import (
	"os/exec"
	"time"
)

func configureSysProcAttr(cmd *exec.Cmd) {
	cmd.WaitDelay = 2 * time.Second
}
