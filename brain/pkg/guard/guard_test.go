package guard

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func runGuard(t *testing.T, mode string, input string) Decision {
	t.Helper()
	var out bytes.Buffer
	err := Process(strings.NewReader(input), &out, mode)
	if err != nil {
		t.Fatalf("unexpected Process error: %v", err)
	}

	var d Decision
	if err := json.Unmarshal(out.Bytes(), &d); err != nil {
		t.Fatalf("failed to unmarshal output %q: %v", out.String(), err)
	}
	return d
}

func TestShareGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantDeny bool
	}{
		{
			name: "direct write to /share/aerial denied",
			input: `{
				"toolCall": {
					"name": "write_to_file",
					"args": {"TargetFile": "/share/aerial/main.go"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "replace content in /share/aerial-config denied",
			input: `{
				"toolCall": {
					"name": "default_api:replace_file_content",
					"args": {"TargetFile": "/share/aerial-config/config.yaml"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "relative traversal to /share/aerial denied",
			input: `{
				"toolCall": {
					"name": "write_to_file",
					"args": {"TargetFile": "/data/../share/aerial/rules/test.md"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "write to safe workspace allowed",
			input: `{
				"toolCall": {
					"name": "write_to_file",
					"args": {"TargetFile": "/data/scratch/test.go"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "mutating git checkout in share Cwd denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git checkout -b fix", "Cwd": "/share/aerial"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "mutating git commit in share Cwd denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git commit -m 'test'", "Cwd": "/share/aerial"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "mutating git branch -D in share Cwd denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git branch -D old-branch", "Cwd": "/share/aerial"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "read-only git status in share Cwd allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git status", "Cwd": "/share/aerial"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "read-only git log in share Cwd allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git log -n 5", "Cwd": "/share/aerial"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "git -C /share/aerial checkout denied from safe Cwd",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git -C /share/aerial checkout main", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "git -C /share/aerial diff allowed from safe Cwd",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "git -C /share/aerial diff", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "file redirection into share denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "echo 'hack' > /share/aerial/new.txt", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "mutating binary rm targeting share denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "rm -rf /share/aerial-config/rules/foo.md", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "safe run_command allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "go test ./...", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := runGuard(t, "share", tt.input)
			if tt.wantDeny && d.Decision != DecisionDeny {
				t.Errorf("expected deny, got allow: %v", d)
			}
			if !tt.wantDeny && d.Decision != DecisionAllow {
				t.Errorf("expected allow, got deny: %v (reason: %s)", d, d.Reason)
			}
			if tt.wantDeny && !strings.Contains(d.Reason, "read-only") {
				t.Errorf("expected mention of read-only in reason, got: %s", d.Reason)
			}
		})
	}
}

func TestScheduleGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantDeny bool
	}{
		{
			name: "cli schedule tool denied",
			input: `{
				"toolCall": {
					"name": "schedule",
					"args": {"DurationSeconds": 60, "Prompt": "Wake up"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "default_api:schedule denied",
			input: `{
				"toolCall": {
					"name": "default_api:schedule",
					"args": {"DurationSeconds": 300, "Prompt": "Check status"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "sleep 10 in run_command denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "sleep 10", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "chained sleep 15 denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "echo starting && sleep 15", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "/bin/sleep 30 denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "/bin/sleep 30", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "while sleep loop denied",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "while true; do sleep 5; done", "Cwd": "/data"}
				}
			}`,
			wantDeny: true,
		},
		{
			name: "short sleep 1s allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "sleep 1", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "cat sleep.txt allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "cat sleep_stats.txt", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
		{
			name: "grep sleep hooks.go allowed",
			input: `{
				"toolCall": {
					"name": "run_command",
					"args": {"CommandLine": "grep sleep hooks.go", "Cwd": "/data"}
				}
			}`,
			wantDeny: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := runGuard(t, "schedule", tt.input)
			if tt.wantDeny && d.Decision != DecisionDeny {
				t.Errorf("expected deny, got allow: %v", d)
			}
			if !tt.wantDeny && d.Decision != DecisionAllow {
				t.Errorf("expected allow, got deny: %v (reason: %s)", d, d.Reason)
			}
		})
	}
}

func TestGuardFailOpen(t *testing.T) {
	t.Parallel()

	// Empty input
	d1 := runGuard(t, "all", "")
	if d1.Decision != DecisionAllow {
		t.Errorf("expected allow on empty input, got: %v", d1)
	}

	// Malformed JSON
	d2 := runGuard(t, "all", "{not valid json}")
	if d2.Decision != DecisionAllow {
		t.Errorf("expected allow on malformed json, got: %v", d2)
	}

	// String args format
	stringArgsInput := `{
		"toolCall": {
			"name": "write_to_file",
			"args": "{\"TargetFile\": \"/share/aerial/foo.go\"}"
		}
	}`
	d3 := runGuard(t, "all", stringArgsInput)
	if d3.Decision != DecisionDeny {
		t.Errorf("expected deny on string-encoded args targeting /share, got: %v", d3)
	}
}
