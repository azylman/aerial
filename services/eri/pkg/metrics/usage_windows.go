//go:build windows

package metrics

import (
	"os/exec"
	"time"
)

func configureUsageCmd(cmd *exec.Cmd) {
	cmd.WaitDelay = 3 * time.Second
}
