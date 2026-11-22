package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/guard"
)

type errWriter struct{}

func (*errWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}

type runCLICase struct {
	name       string
	args       []string
	input      string
	wantDeny   bool
	wantOutput string
	wantCode   int
	// brokenWriter routes stdout to a writer that always fails; only the exit code is asserted.
	brokenWriter bool
}

func assertRunCLICase(t *testing.T, tt runCLICase) {
	t.Helper()
	var stdout bytes.Buffer
	stdinBuf := strings.NewReader(tt.input)

	var w io.Writer = &stdout
	if tt.brokenWriter {
		w = &errWriter{}
	}
	code := RunCLI(tt.args, stdinBuf, w, hermeticEnv(t))
	if code != tt.wantCode {
		t.Errorf("expected exit code %d, got: %d", tt.wantCode, code)
	}
	if tt.brokenWriter {
		return
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
}

func runCLITestCases() []runCLICase {
	extra := runCLIExtraTestCases()
	cases := make([]runCLICase, 0, 25+len(extra))
	cases = append(cases, []runCLICase{
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
			name:       "trace subcommand allows",
			args:       []string{"hook-guard", "trace"},
			input:      agyMCPCall,
			wantOutput: `{"decision":"allow"}`,
			wantCode:   0,
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
		{name: "live-state subcommand checks only live state", args: []string{"hook-guard", "live-state"}, input: `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 5"}}}`, wantDeny: false, wantCode: 0},
		{name: "live-state-guard subcommand checks only live state", args: []string{"hook-guard", "live-state-guard"}, input: `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 5"}}}`, wantDeny: false, wantCode: 0},
		{name: "symlink live-state-guard base name checks only live state", args: []string{"/usr/local/bin/live-state-guard"}, input: `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 5"}}}`, wantDeny: false, wantCode: 0},
		{name: "subcommand followed by --help outputs usage", args: []string{"hook-guard", "commit", "--help"}, wantOutput: "Usage: hook-guard", wantCode: 0},
		{name: "subcommand followed by -h outputs usage", args: []string{"hook-guard", "share", "-h"}, wantOutput: "Usage: hook-guard", wantCode: 0},
		{name: "help flag followed by subcommand outputs usage", args: []string{"hook-guard", "--help", "commit"}, wantOutput: "Usage: hook-guard", wantCode: 0},
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
			name:     "search subcommand denies root search",
			args:     []string{"hook-guard", "search"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find / -maxdepth 1"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
	}...)
	return append(cases, extra...)
}

func runCLIExtraTestCases() []runCLICase {
	return []runCLICase{
		{
			name:     "symlink search-guard base name",
			args:     []string{"/usr/local/bin/search-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find . -name '*.go'"}}}`,
			wantDeny: true,
			wantCode: 0,
		},
		{
			name:     "search-guard alias arg",
			args:     []string{"hook-guard", "search-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find . -name '*.go'"}}}`,
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
		{
			name:     "commit subcommand allows benign command",
			args:     []string{"hook-guard", "commit"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo hello", "Cwd": "/data"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{
			name:     "symlink commit-guard base name",
			args:     []string{"/usr/local/bin/commit-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo hello", "Cwd": "/data"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{
			name:     "commit-guard alias arg",
			args:     []string{"hook-guard", "commit-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo hello", "Cwd": "/data"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{
			name:     "commit-safety-guard alias arg",
			args:     []string{"hook-guard", "commit-safety-guard"},
			input:    `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo hello", "Cwd": "/data"}}}`,
			wantDeny: false,
			wantCode: 0,
		},
		{name: "pre-invocation normal", args: []string{"hook-guard", "pre-invocation"}, input: `{"conversationId":"c1","invocationNum":1}`, wantOutput: "{}", wantCode: 0},
		{name: "invocation-guard symlink", args: []string{"/usr/local/bin/invocation-guard"}, input: `{"conversationId":"c1","invocationNum":25}`, wantOutput: "{}", wantCode: 0},
		{name: "post-batch subcommand", args: []string{"hook-guard", "post-batch"}, input: `{"conversationId":"c2","stepIdx":1,"error":"e","toolCall":{"name":"edit_file","args":{"Path":"/tmp/a.go"}}}`, wantOutput: "{}", wantCode: 0},
		{name: "malformed payload with broken writer", args: []string{"hook-guard"}, input: `{malformed`, wantCode: 1, brokenWriter: true},
		{name: "malformed pre-invocation with broken writer", args: []string{"hook-guard", "pre-invocation"}, input: `{malformed`, wantCode: 1, brokenWriter: true},
		{name: "help with broken writer", args: []string{"hook-guard", "--help"}, wantCode: 1, brokenWriter: true},
		{name: "post-batch-guard symlink", args: []string{"/usr/local/bin/post-batch-guard"}, input: `{"conversationId":"c2","stepIdx":1,"toolCall":{"name":"edit_file","args":{"Path":"/tmp/a.go"}}}`, wantOutput: "{}", wantCode: 0},
	}
}

// hermeticEnv stands in for os.Getenv so RunCLI never reads the real environment: batch-guard
// state lands in a per-test temp dir instead of the live /tmp/aerial-batch-guard, and
// DATABASE_URL and ERI_HOOK_ENDPOINT stay unset so no Postgres or eri endpoint is reached.
func hermeticEnv(t *testing.T) func(string) string {
	t.Helper()
	env := map[string]string{"AERIAL_BATCH_GUARD_DIR": t.TempDir()}
	return func(k string) string { return env[k] }
}

func TestRunCLI(t *testing.T) {
	t.Parallel()
	tests := runCLITestCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertRunCLICase(t, tt)
		})
	}
}
