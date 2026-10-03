package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/azylman/aerial/brain/pkg/guard"
)

func TestHookGuardCLI(t *testing.T) {
	// Build a temporary test binary
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "hook-guard")

	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build hook-guard: %v, output: %s", err, string(out))
	}

	tests := []struct {
		name       string
		args       []string
		input      string
		wantDeny   bool
		wantReason string
	}{
		{
			name: "share mode denies write to share",
			args: []string{"share"},
			input: `{
				"toolCall": {
					"name": "write_to_file",
					"args": {"TargetFile": "/share/aerial/test.go"}
				}
			}`,
			wantDeny:   true,
			wantReason: "read-only",
		},
		{
			name: "schedule mode denies schedule tool",
			args: []string{"schedule"},
			input: `{
				"toolCall": {
					"name": "schedule",
					"args": {"DurationSeconds": 10, "Prompt": "hi"}
				}
			}`,
			wantDeny:   true,
			wantReason: "strictly prohibited",
		},
		{
			name: "all mode allows safe command",
			args: []string{"all"},
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "ls -la", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(binPath, tt.args...)
			cmd.Stdin = stringsReader(tt.input)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			if err := cmd.Run(); err != nil {
				t.Fatalf("cmd.Run() failed: %v, stderr: %s", err, stderr.String())
			}

			var d guard.Decision
			if err := json.Unmarshal(stdout.Bytes(), &d); err != nil {
				t.Fatalf("failed to unmarshal output %q: %v", stdout.String(), err)
			}

			if tt.wantDeny && d.Decision != guard.DecisionDeny {
				t.Errorf("expected deny, got allow: %v", d)
			}
			if !tt.wantDeny && d.Decision != guard.DecisionAllow {
				t.Errorf("expected allow, got deny: %v", d)
			}
		})
	}
}

func stringsReader(s string) *bytes.Reader {
	return bytes.NewReader([]byte(s))
}

func TestMainHelp(t *testing.T) {
	// Simple test verifying main compiles and package level variables work
	if len(os.Args) == 0 {
		t.Fatal("empty os.Args")
	}
}
