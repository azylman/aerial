//go:build !windows

package metrics

import (
	"os"
	"os/exec"
	"syscall"
)

const (
	usageUnprivilegedUID = 1000
	usageUnprivilegedGID = 1000
)

func usageSysProcAttr(euid int) *syscall.SysProcAttr {
	sysAttr := &syscall.SysProcAttr{Setpgid: true}
	if euid == 0 {
		sysAttr.Credential = &syscall.Credential{Uid: usageUnprivilegedUID, Gid: usageUnprivilegedGID}
	}
	return sysAttr
}

func configureUsageCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = usageSysProcAttr(os.Geteuid())
}
