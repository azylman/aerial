package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/azylman/aerial/brain/pkg/guard"
)

type errWriter struct{}

func (e *errWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}

func TestRunCLI(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		input      string
		wantDeny   bool
		wantOutput string
		wantCode   int
	}{
		{
			name:     "share subcommand denies write to share",
			args:     []string{"hook-guard", "share"},
			input:    `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/test.go"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "schedule subcommand denies schedule",
			args:     []string{"hook-guard", "schedule"},
			input:    `{"toolCall": {"name": "schedule", "args": {"DurationSeconds": 10}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "all subcommand allows benign run_command",
			args:     []string{"hook-guard", "all"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "Cwd": "/data"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{
			name:     "symlink share-guard base name",
			args:     []string{"/usr/local/bin/share-guard"},
			input:    `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial-config/rules.md"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "symlink schedule-guard base name",
			args:     []string{"/usr/local/bin/schedule-guard"},
			input:    `{"toolCall": {"name": "schedule"}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:       "help flag outputs usage",
			args:       []string{"hook-guard", "--help"},
			input:      "",
			wantOutput: "Usage: hook-guard",
			wantCode:   0,
		},
		{
			name:       "short help flag outputs usage",
			args:       []string{"hook-guard", "-h"},
			input:      "",
			wantOutput: "Usage: hook-guard",
			wantCode:   0,
		},
		{
			name:       "word help outputs usage",
			args:       []string{"hook-guard", "help"},
			input:      "",
			wantOutput: "Usage: hook-guard",
			wantCode:   0,
		},
		{
			name:     "empty args defaults to all",
			args:     []string{},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo hi"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{
			name:     "schedule-guard alias arg",
			args:     []string{"hook-guard", "schedule-guard"},
			input:    `{"toolCall": {"name": "schedule"}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "batch subcommand denies unscoped test",
			args:     []string{"hook-guard", "batch"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "go test ./..."}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "symlink batch-guard base name",
			args:     []string{"/usr/local/bin/batch-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "go test ./..."}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "batch-guard alias arg",
			args:     []string{"hook-guard", "batch-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git commit -m fix"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "all subcommand denies unbatched git mutation",
			args:     []string{"hook-guard", "all"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git add ."}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "share-guard alias arg",
			args:     []string{"hook-guard", "share-guard"},
			input:    `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/foo.go"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			stdinBuf := strings.NewReader(tt.input)

			code := RunCLI(tt.args, stdinBuf, &stdout)
			if code != tt.wantCode {
				t.Errorf("expected exit code %d, got: %d", tt.wantCode, code)
			}

			if tt.wantOutput != "" {
				if !strings.Contains(stdout.String(), tt.wantOutput) {
					t.Errorf("expected output to contain %q, got: %q", tt.wantOutput, stdout.String())
				}
				return
			}

			var d guard.Decision
			if err := json.Unmarshal(stdout.Bytes(), &d); err != nil {
				t.Fatalf("failed to unmarshal output %q: %v", stdout.String(), err)
			}

			if tt.wantDeny && d.Decision != guard.DecisionDeny {
				t.Errorf("expected deny, got: %v", d)
			}
			if !tt.wantDeny && d.Decision != guard.DecisionAllow {
				t.Errorf("expected allow, got: %v", d)
			}
		})
	}

	t.Run("failing writer covered", func(t *testing.T) {
		code := RunCLI([]string{"hook-guard"}, strings.NewReader(`{malformed`), &errWriter{})
		if code != 0 && code != 1 {
			t.Errorf("unexpected code %d", code)
		}
	})

	t.Run("help flag with errWriter", func(t *testing.T) {
		code := RunCLI([]string{"hook-guard", "--help"}, nil, &errWriter{})
		if code != 1 {
			t.Errorf("expected 1 on help write failure, got: %d", code)
		}
	})

	t.Run("HelpMessage direct", func(t *testing.T) {
		msg := HelpMessage()
		if !strings.Contains(msg, "Usage: hook-guard") {
			t.Errorf("unexpected HelpMessage: %s", msg)
		}
	})
}
