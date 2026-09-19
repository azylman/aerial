//go:build windows

package runner

import (
	"os/exec"
	"testing"
	"time"
)

func TestConfigureSysProcAttr_Windows(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("cmd.exe")
	configureSysProcAttr(cmd)
	if cmd.WaitDelay != 3*time.Second {
		t.Errorf("expected WaitDelay=3s, got %v", cmd.WaitDelay)
	}

	killProcessGroup(nil)
	killProcessGroup(&exec.Cmd{})
	terminateProcessGroup(nil)

	if isProcessTerminatedBySignal(nil) {
		t.Errorf("expected false for nil ExitError")
	}
	if !isProcessTerminatedBySignal(&exec.ExitError{}) {
		t.Errorf("expected true for non-nil ExitError on Windows")
	}
}

