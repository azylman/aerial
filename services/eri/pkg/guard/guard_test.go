package guard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func TestGuardProcess_EndToEnd(t *testing.T) {
	t.Setenv("AERIAL_BATCH_GUARD_DIR", t.TempDir())
	t.Setenv("DATABASE_URL", ":memory:")

	tests := []struct {
		name             string
		mode             string
		input            string
		wantDeny         bool
		wantWaitMs       int
		wantOverwriteCmd string
	}{
		{"direct write to share aerial", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/main.go"}}}`, true, 0, ""},
		{"edit_file in share aerial", "share", `{"toolCall": {"name": "edit_file", "args": {"path": "/share/aerial/main.go"}}}`, true, 0, ""},
		{"write_file in share aerial-config", "share", `{"toolCall": {"name": "write_file", "args": {"path": "/share/aerial-config/config.yaml"}}}`, true, 0, ""},
		{"replace content in share aerial-config", "share", `{"toolCall": {"name": "default_api:replace_file_content", "args": {"TargetFile": "/share/aerial-config/config.yaml"}}}`, true, 0, ""},
		{"relative traversal to share aerial", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/data/../share/aerial/file.txt"}}}`, true, 0, ""},
		{"write to safe workspace", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/data/scratch/workspace/file.txt"}}}`, false, 0, ""},
		{"mutating git checkout in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git checkout -b feature", "Cwd": "/share/aerial"}}}`, true, 0, ""},
		{"mutating git commit in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git commit -m 'test'", "Cwd": "/share/aerial"}}}`, true, 0, ""},
		{"mutating git branch -D in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git branch -D old", "Cwd": "/share/aerial-config"}}}`, true, 0, ""},
		{"read-only git status in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status", "Cwd": "/share/aerial"}}}`, false, 0, ""},
		{"read-only git log in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git log -n 5", "Cwd": "/share/aerial"}}}`, false, 0, ""},
		{"git -C share checkout denied", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git -C /share/aerial checkout main", "Cwd": "/data"}}}`, true, 0, ""},
		{"git -C attached share checkout denied", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git -C/share/aerial --no-pager checkout main", "Cwd": "/data"}}}`, true, 0, ""},
		{"git --git-dir share checkout denied", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git --git-dir=/share/aerial/.git checkout main", "Cwd": "/data"}}}`, true, 0, ""},
		{"git --work-tree share branch -d denied", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git --work-tree=/share/aerial branch -d old", "Cwd": "/data"}}}`, true, 0, ""},
		{"git -C share diff allowed", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git -C /share/aerial diff", "Cwd": "/data"}}}`, false, 0, ""},
		{"file redirection into share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 'bad' > /share/aerial/pwn.txt", "Cwd": "/data"}}}`, true, 0, ""},
		{"attached redirect into share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 'bad' >/share/aerial/pwn.txt", "Cwd": "/data"}}}`, true, 0, ""},
		{"mutating binary rm targeting share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "rm -rf /share/aerial-config", "Cwd": "/data"}}}`, true, 0, ""},
		{"mutating binary in protected cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "touch foo.txt", "Cwd": "/share/aerial"}}}`, true, 0, ""},
		{"bash -c mutating rm in share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "bash -c 'rm -rf /share/aerial/file.txt'", "Cwd": "/data"}}}`, true, 0, ""},
		{"bash -c safe echo in share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": " ; bash -c 'echo ok'", "Cwd": "/data"}}}`, false, 0, ""},
		{"safe run_command allowed", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 'hello'", "Cwd": "/data"}}}`, false, 0, ""},
		{"schedule tool direct invocation", "schedule", `{"toolCall": {"name": "schedule", "args": {"CommandLine": "echo 1"}}}`, true, 0, ""},
		{"default_api:schedule invocation", "schedule", `{"toolCall": {"name": "default_api:schedule", "args": {"CommandLine": "echo 1"}}}`, true, 0, ""},
		{"sleep 10 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 10"}}}`, true, 0, ""},
		{"sleep 3 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 3"}}}`, true, 0, ""},
		{"sleep 1 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 1"}}}`, true, 0, ""},
		{"/bin/sleep 5 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "/bin/sleep 5"}}}`, true, 0, ""},
		{"chained sleep", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "make build && sleep 20"}}}`, true, 0, ""},
		{"semicolon chained sleep", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 1; sleep 5; echo 2"}}}`, true, 0, ""},
		{"sleep in while loop", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "while true; do sleep 1; done"}}}`, true, 0, ""},
		{"sleep 0 allowed and overwrites WaitMsBeforeAsync", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 0"}}}`, false, 10000, ""},
		{"manage_task status denied", "schedule", `{"toolCall": {"name": "manage_task", "args": {"Action": "status", "TaskId": "t1"}}}`, true, 0, ""},
		{"manage_task quoted status denied", "schedule", `{"toolCall": {"name": "default_api:manage_task", "args": {"Action": "\"status\"", "TaskId": "t1"}}}`, true, 0, ""},
		{"manage_task list denied", "schedule", `{"toolCall": {"name": "manage_task", "args": {"Action": "list"}}}`, true, 0, ""},
		{"manage_task kill allowed", "schedule", `{"toolCall": {"name": "manage_task", "args": {"Action": "kill", "TaskId": "t1"}}}`, false, 0, ""},
		{"manage_subagents list denied", "schedule", `{"toolCall": {"name": "manage_subagents", "args": {"Action": "list"}}}`, true, 0, ""},
		{"manage_subagents kill allowed", "schedule", `{"toolCall": {"name": "manage_subagents", "args": {"Action": "kill"}}}`, false, 0, ""},
		{"view_file task log denied", "schedule", `{"toolCall": {"name": "view_file", "args": {"AbsolutePath": "/data/runtimes/discord/.gemini/antigravity-cli/brain/abc/.system_generated/tasks/task-1.log"}}}`, true, 0, ""},
		{"read_file task log denied", "schedule", `{"toolCall": {"name": "read_file", "args": {"Path": "/data/runtimes/discord/.gemini/antigravity-cli/brain/abc/.system_generated/tasks/task-1.log"}}}`, true, 0, ""},
		{"run_command cat task log denied", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "cat /data/brain/abc/.system_generated/tasks/task-1.log | tail -n 20"}}}`, true, 0, ""},
		{"run_command 5000ms overwritten to 10000ms", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": 5000}}}`, false, 10000, ""},
		{"run_command string 5000ms overwritten to 10000ms", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": "5000"}}}`, false, 10000, ""},
		{"run_command invalid waitMs overwritten to 10000ms", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": "bad"}}}`, false, 10000, ""},
		{"run_command 10000ms not overwritten", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"run_command daemon not overwritten", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": 500, "IsDaemon": true}}}`, false, 0, ""},
		{"python3 -c braced escaped quotes rewritten", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 -c 'print(f\"{x[\\\"k\\\"]}\"); print('\\''ok'\\'')'", "WaitMsBeforeAsync": 10000}}}`, false, 0, `python3 -c 'print(f"{x["k"]}"); print('\''ok'\'')'`},
		{"python3 -c nested f-string and rf prefix rewritten", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 -c 'print(rf\"{len({1: x[\\\"k\\\"]})} {f\\\"{y[\\\"z\\\"]}\\\"}\")'", "WaitMsBeforeAsync": 10000}}}`, false, 0, `python3 -c 'print(rf"{len({1: x["k"]})} {f"{y["z"]}"}")'`},
		{"python3 -c f-string escaped backslash and literal braces preserved", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 -c 'print(f\"\\\"lit\\\" {{x}} {d[\\\"a\\\\\\\\\\\"]}\")'", "WaitMsBeforeAsync": 10000}}}`, false, 0, `python3 -c 'print(f"\"lit\" {{x}} {d["a\\\\"]}")'`},
		{"python3 -c dict and json string escaped quotes untouched", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 -c 'import json; d = {\"k\": \"a\\\"b\"}; json.loads(\"{\\\"k\\\": 1}\"); print(f\"{d[\"a\\\"b\"]}\")'", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"python3 -c unclosed single quote untouched", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 -c 'print(f\"{x[\\\"k\\\"]}\")", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"non-python escaped quotes untouched", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 script.py -c \"{\\\"k\\\"}\"", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"command with substring sleep in identifier", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git log --grep='sleep fix'", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"python script with sleep in name", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 check_sleep_timer.py", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"non-schedule non-sleep run_command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la", "WaitMsBeforeAsync": 10000}}}`, false, 0, ""},
		{"all mode denies share violation", "all", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/evil.go"}}}`, true, 0, ""},
		{"all mode denies sleep violation", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 5"}}}`, true, 0, ""},
		{"all mode denies unconstrained search", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find / -name secret"}}}`, true, 0, ""},
		{"all mode allows safe command and overwrites WaitMsBeforeAsync", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status", "Cwd": "/data", "WaitMsBeforeAsync": 5000}}}`, false, 10000, ""},
		{"all mode preserves psql -A overwrite and WaitMsBeforeAsync", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "psql -c \"\\dt\"", "Cwd": "/data"}}}`, false, 10000, `psql -A -c "\dt"`},
		{"non-intercepted view_file", "all", `{"toolCall": {"name": "view_file", "args": {"AbsolutePath": "/share/aerial/README.md"}}}`, false, 0, ""},
		{"empty tool call", "all", `{"toolCall": {"name": "", "args": {}}}`, false, 0, ""},
		{"search mode denies unconstrained find", "search", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find / -name foo"}}}`, true, 0, ""},
		{"search mode allows scoped grep", "search", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "grep -rn -m 20 --exclude-dir=.git foo ."}}}`, false, 0, ""},
		{"commit mode allows safe commit command", "commit", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status"}}}`, false, 0, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := runGuard(t, tc.mode, tc.input)
			if tc.wantDeny && d.Decision != DecisionDeny {
				t.Fatalf("mode=%q input=%s: expected deny, got %v", tc.mode, tc.input, d)
			}
			if !tc.wantDeny && d.Decision != DecisionAllow {
				t.Fatalf("mode=%q input=%s: expected allow, got deny: %v", tc.mode, tc.input, d)
			}
			if tc.wantWaitMs > 0 {
				gotWait, _ := d.Overwrite["WaitMsBeforeAsync"].(float64)
				if int(gotWait) != tc.wantWaitMs {
					t.Fatalf("mode=%q input=%s: Overwrite[WaitMsBeforeAsync]=%v, want %d", tc.mode, tc.input, d.Overwrite["WaitMsBeforeAsync"], tc.wantWaitMs)
				}
			} else if d.Overwrite != nil && d.Overwrite["WaitMsBeforeAsync"] != nil {
				t.Fatalf("mode=%q input=%s: unexpected WaitMsBeforeAsync overwrite: %v", tc.mode, tc.input, d.Overwrite)
			}
			if tc.wantOverwriteCmd != "" {
				gotCmd, _ := d.Overwrite["CommandLine"].(string)
				if gotCmd != tc.wantOverwriteCmd {
					t.Fatalf("mode=%q input=%s: Overwrite[CommandLine]=%q, want %q", tc.mode, tc.input, gotCmd, tc.wantOverwriteCmd)
				}
			} else if d.Overwrite != nil && d.Overwrite["CommandLine"] != nil {
				t.Fatalf("mode=%q input=%s: unexpected CommandLine overwrite: %v", tc.mode, tc.input, d.Overwrite)
			}
		})
	}
}

func TestIsProtectedPath(t *testing.T) {
	t.Chdir(t.TempDir())

	tmp := t.TempDir()
	tempAerial := filepath.Join(tmp, "share", "aerial")
	if err := os.MkdirAll(tempAerial, 0o755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	existingFile := filepath.Join(tempAerial, "config.json")
	if err := os.WriteFile(existingFile, []byte("{}"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	unrelated := filepath.Join(tmp, "other", "file.txt")

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"empty path", "", false},
		{"whitespace path", "   ", false},
		{"nonexistent deep path", "/nonexistent/deep/nested/path/to/check/parent/loop", false},
		{"real share aerial", "/share/aerial", true},
		{"real share aerial child", "/share/aerial/main.go", true},
		{"real share aerial-config", "/share/aerial-config", true},
		{"real share aerial-config child", "/share/aerial-config/rules/foo.md", true},
		{"safe data path", "/data/scratch/workspace", false},
		{"relative safe path", "main.go", false},
		{"temp existing file", existingFile, false},
		{"unrelated file", unrelated, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsProtectedPath(tc.path)
			if got != tc.want {
				t.Fatalf("IsProtectedPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated read failure")
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic("simulated reader panic")
}

func TestGuardProcess_IOAndModes(t *testing.T) {
	t.Parallel()

	ioTests := []struct {
		name   string
		r      io.Reader
		mode   string
		expect DecisionType
	}{
		{"failing reader fail-open", errorReader{}, "all", DecisionAllow},
		{"panic reader fail-open", panicReader{}, "all", DecisionAllow},
		{"empty reader fail-open", strings.NewReader(""), "all", DecisionAllow},
		{"whitespace reader fail-open", strings.NewReader("   \n\t  "), "all", DecisionAllow},
		{"invalid json fail-open", strings.NewReader("invalid-json{"), "all", DecisionAllow},
	}
	for _, tc := range ioTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			err := Process(tc.r, &out, tc.mode)
			if err != nil {
				t.Fatalf("expected nil error on fail-open, got: %v", err)
			}
			var d Decision
			if err := json.Unmarshal(out.Bytes(), &d); err != nil {
				t.Fatalf("failed to unmarshal output %q: %v", out.String(), err)
			}
			if d.Decision != tc.expect {
				t.Fatalf("got decision=%q, want %q", d.Decision, tc.expect)
			}
		})
	}

	parseTests := []struct {
		name     string
		raw      string
		wantFile string
		wantCmd  string
	}{
		{"nil bytes", "", "", ""},
		{"empty json string", `""`, "", ""},
		{"plain string", `"plain non-json string"`, "", ""},
		{"json string encoded object", `"{\"CommandLine\": \"echo 1\", \"Cwd\": \"/data\"}"`, "", "echo 1"},
		{"direct map object", `{"path": "/tmp/test.go", "CommandLine": "cat test.go"}`, "/tmp/test.go", "cat test.go"},
	}
	for _, tc := range parseTests {
		t.Run("parse_"+tc.name, func(t *testing.T) {
			t.Parallel()
			var raw []byte
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			args := ParseArgs(raw)
			if args.FilePath() != tc.wantFile || (tc.wantCmd != "" && args.CommandLine != tc.wantCmd) {
				t.Fatalf("ParseArgs(%s) = %+v, want file=%q cmd=%q", tc.raw, args, tc.wantFile, tc.wantCmd)
			}
		})
	}
}
