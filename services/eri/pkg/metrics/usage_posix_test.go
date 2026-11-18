//go:build !windows

package metrics

import (
	"os/exec"
	"testing"
)

func TestUsageSysProcAttr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		euid           int
		wantSetpgid    bool
		wantCredential bool
		wantUID        uint32
		wantGID        uint32
	}{
		{
			name:           "non-root euid keeps setpgid without credential",
			euid:           1000,
			wantSetpgid:    true,
			wantCredential: false,
		},
		{
			name:           "root euid sets setpgid and unprivileged credential",
			euid:           0,
			wantSetpgid:    true,
			wantCredential: true,
			wantUID:        usageUnprivilegedUID,
			wantGID:        usageUnprivilegedGID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attr := usageSysProcAttr(tc.euid)
			if attr == nil {
				t.Fatalf("expected non-nil SysProcAttr")
			}
			if attr.Setpgid != tc.wantSetpgid {
				t.Errorf("Setpgid = %v, want %v", attr.Setpgid, tc.wantSetpgid)
			}
			if tc.wantCredential {
				if attr.Credential == nil {
					t.Fatalf("expected non-nil Credential")
				}
				if attr.Credential.Uid != tc.wantUID || attr.Credential.Gid != tc.wantGID {
					t.Errorf("Credential = %+v, want UID %d GID %d", attr.Credential, tc.wantUID, tc.wantGID)
				}
			} else if attr.Credential != nil {
				t.Errorf("expected nil Credential, got %+v", attr.Credential)
			}
		})
	}
}

func TestConfigureUsageCmd(t *testing.T) {
	t.Parallel()
	cmd := &exec.Cmd{}
	configureUsageCmd(cmd)
	if cmd.SysProcAttr == nil {
		t.Fatalf("configureUsageCmd() resulted in nil SysProcAttr")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Errorf("configureUsageCmd() Setpgid = false, want true")
	}
}
