package guard

import "testing"

func TestCheckLiveState(t *testing.T) {
	t.Parallel()

	deny := Decision{Decision: DecisionDeny, Reason: LiveStateDenyReason}
	allow := Decision{Decision: DecisionAllow}
	bash := func(cmd, cwd string) ToolArgs { return ToolArgs{CommandLine: cmd, Cwd: cwd} }

	tests := []struct {
		name string
		tool string
		args ToolArgs
		want Decision
	}{
		{"rm live config", "run_command", bash("rm -f /data/ssh/config", "/data"), deny},
		{"sudo rm live config", "run_command", bash("sudo rm -f /data/ssh/config", "/data"), deny},
		{"env-prefixed rm live config", "Bash", bash("env FOO=1 command rm -f /data/ssh/config", "/tmp"), deny},
		{"relative rm resolved against cwd", "run_command", bash("rm ssh/config", "/data"), deny},
		{"fd-prefixed redirect into known_hosts", "run_command", bash("echo x 2>/data/ssh/known_hosts", "/tmp"), deny},
		{"rm after cd in chained command", "run_command", bash("cd /data/scratch/x && rm -f /data/ssh/config", "/data"), deny},
		{"install onto live config", "run_command", bash("install -m 0600 a /data/ssh/config", "/tmp"), deny},
		{"install into target directory", "run_command", bash("install -t /data/ssh a b", "/tmp"), deny},
		{"cp into live dir via target-directory", "run_command", bash("cp --target-directory=/data/ssh a", "/tmp"), deny},
		{"spaced redirect into known_hosts", "run_command", bash("echo x > /data/ssh/known_hosts", "/tmp"), deny},
		{"rm ancestor of live state", "run_command", bash("rm -rf /data", "/tmp"), deny},
		{"mv live dir away", "run_command", bash("mv /data/ssh /tmp/old", "/tmp"), deny},
		{"sed in-place on live config", "run_command", bash("sed -i s/a/b/ /data/ssh/config", "/tmp"), deny},
		{"ln over live config", "run_command", bash("ln -sf /tmp/c /data/ssh/config", "/tmp"), deny},
		{"truncate known_hosts", "run_command", bash("truncate -s 0 /data/ssh/known_hosts", "/tmp"), deny},
		{"bash -c rm live config", "Bash", bash("bash -c 'rm /data/ssh/config'", "/tmp"), deny},
		{"mutating binary in live cwd", "run_command", bash("touch x", "/data/ssh"), deny},
		{"write tool on live config", "Write", ToolArgs{FilePathSnake: "/data/ssh/config"}, deny},
		{"edit tool on live key", "edit_file", ToolArgs{Path: "/data/ssh/id_ed25519"}, deny},
		{"ssh to host", "run_command", bash("ssh calibarn whoami", "/data"), allow},
		{"fingerprint public key", "run_command", bash("ssh-keygen -lf /data/ssh/id_ed25519.pub", "/data"), allow},
		{"read live config", "run_command", bash("cat /data/ssh/config", "/data"), allow},
		{"rm unrelated ssh dir", "run_command", bash("rm -rf /tmp/t/ssh", "/tmp"), allow},
		{"cp live config to temp copy", "run_command", bash("cp /data/ssh/config /tmp/t/config", "/data"), allow},
		{"install live config to temp copy", "run_command", bash("install -m 0600 /data/ssh/config /tmp/t/c", "/data"), allow},
		{"sed print live config", "run_command", bash("sed -n 1p /data/ssh/config", "/data"), allow},
		{"mkdir scratch dir", "run_command", bash("mkdir -p /data/scratch/x", "/data"), allow},
		{"rm sibling of live state", "run_command", bash("rm -rf /data/scratch/x", "/data"), allow},
		{"stderr dup to stdout", "run_command", bash("ls /data/ssh 2>&1", "/data"), allow},
		{"write tool on temp copy", "Write", ToolArgs{FilePathSnake: "/tmp/t/config"}, allow},
		{"read tool on live config", "view_file", ToolArgs{AbsolutePath: "/data/ssh/config"}, allow},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CheckLiveState(tc.tool, tc.args)
			if got.Decision != tc.want.Decision || got.Reason != tc.want.Reason {
				t.Fatalf("CheckLiveState(%q, %+v) = %+v, want %+v", tc.tool, tc.args, *got, tc.want)
			}
		})
	}
}
