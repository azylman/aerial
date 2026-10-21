//go:build windows

package runner

import (
	"errors"
	"os"
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

	var customKillCalled bool
	mockCmd := &exec.Cmd{Process: &os.Process{Pid: 12345}}
	killProcessGroupWith(mockCmd, func(p *os.Process) error {
		customKillCalled = true
		return errors.New("simulated kill error")
	})
	if !customKillCalled {
		t.Errorf("expected custom kill func called")
	}
}
