//go:build windows

package runner

import (
	"os/exec"
	"testing"
	"time"
)

func TestConfigureSysProcAttr_Windows(t *testing.T) {
	cmd := exec.Command("cmd.exe")
	configureSysProcAttr(cmd)
	if cmd.WaitDelay != 3*time.Second {
		t.Errorf("expected WaitDelay=3s, got %v", cmd.WaitDelay)
	}
}
