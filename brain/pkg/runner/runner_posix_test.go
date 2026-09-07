//go:build !windows

package runner

import (
	"os/exec"
	"testing"
	"time"
)

func TestConfigureSysProcAttr_CancelNilProcess(t *testing.T) {
	cmd := exec.Command("true")
	configureSysProcAttr(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Errorf("expected SysProcAttr.Setpgid=true")
	}
	if cmd.WaitDelay != 3*time.Second {
		t.Errorf("expected WaitDelay=3s, got %v", cmd.WaitDelay)
	}
	if cmd.Cancel == nil {
		t.Fatalf("expected non-nil cmd.Cancel")
	}

	// Calling Cancel before process starts (cmd.Process == nil) should return nil safely
	if err := cmd.Cancel(); err != nil {
		t.Errorf("expected nil error when canceling nil process, got %v", err)
	}
}
