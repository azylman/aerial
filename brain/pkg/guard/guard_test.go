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
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		input    string
		wantDeny bool
	}{
		{"direct write to share aerial", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/main.go"}}}`, true},
		{"replace content in share aerial-config", "share", `{"toolCall": {"name": "default_api:replace_file_content", "args": {"TargetFile": "/share/aerial-config/config.yaml"}}}`, true},
		{"relative traversal to share aerial", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/data/../share/aerial/file.txt"}}}`, true},
		{"write to safe workspace", "share", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/data/scratch/workspace/file.txt"}}}`, false},
		{"mutating git checkout in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git checkout -b feature", "Cwd": "/share/aerial"}}}`, true},
		{"mutating git commit in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git commit -m 'test'", "Cwd": "/share/aerial"}}}`, true},
		{"mutating git branch -D in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git branch -D old", "Cwd": "/share/aerial-config"}}}`, true},
		{"read-only git status in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status", "Cwd": "/share/aerial"}}}`, false},
		{"read-only git log in share Cwd", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git log -n 5", "Cwd": "/share/aerial"}}}`, false},
		{"git -C share checkout denied", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git -C /share/aerial checkout main", "Cwd": "/data"}}}`, true},
		{"git -C share diff allowed", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git -C /share/aerial diff", "Cwd": "/data"}}}`, false},
		{"file redirection into share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 'bad' > /share/aerial/pwn.txt", "Cwd": "/data"}}}`, true},
		{"mutating binary rm targeting share", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "rm -rf /share/aerial-config", "Cwd": "/data"}}}`, true},
		{"safe run_command allowed", "share", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 'hello'", "Cwd": "/data"}}}`, false},
		{"schedule tool direct invocation", "schedule", `{"toolCall": {"name": "schedule", "args": {"CommandLine": "echo 1"}}}`, true},
		{"default_api:schedule invocation", "schedule", `{"toolCall": {"name": "default_api:schedule", "args": {"CommandLine": "echo 1"}}}`, true},
		{"sleep 10 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 10"}}}`, true},
		{"/bin/sleep 5 command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "/bin/sleep 5"}}}`, true},
		{"chained sleep", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "make build && sleep 20"}}}`, true},
		{"semicolon chained sleep", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "echo 1; sleep 5; echo 2"}}}`, true},
		{"sleep in while loop", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "while true; do sleep 1; done"}}}`, true},
		{"sleep 0 allowed", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 0"}}}`, false},
		{"command with substring sleep in identifier", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git log --grep='sleep fix'"}}}`, false},
		{"python script with sleep in name", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "python3 check_sleep_timer.py"}}}`, false},
		{"non-schedule non-sleep run_command", "schedule", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "ls -la"}}}`, false},
		{"all mode denies share violation", "all", `{"toolCall": {"name": "write_to_file", "args": {"TargetFile": "/share/aerial/evil.go"}}}`, true},
		{"all mode denies sleep violation", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "sleep 5"}}}`, true},
		{"all mode denies unconstrained search", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find / -name secret"}}}`, true},
		{"all mode allows safe command", "all", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status", "Cwd": "/data"}}}`, false},
		{"non-intercepted view_file", "all", `{"toolCall": {"name": "view_file", "args": {"AbsolutePath": "/share/aerial/README.md"}}}`, false},
		{"empty tool call", "all", `{"toolCall": {"name": "", "args": {}}}`, false},
		{"search mode denies unconstrained find", "search", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "find / -name foo"}}}`, true},
		{"search mode allows scoped grep", "search", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "grep -rn --exclude-dir=.git foo ."}}}`, false},
		{"commit mode allows safe commit command", "commit", `{"toolCall": {"name": "run_command", "args": {"CommandLine": "git status"}}}`, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := runGuard(t, tc.mode, tc.input)
			if tc.wantDeny && d.Decision != DecisionDeny {
				t.Fatalf("mode=%q input=%s: expected deny, got %v", tc.mode, tc.input, d)
			}
			if !tc.wantDeny && d.Decision != DecisionAllow {
				t.Fatalf("mode=%q input=%s: expected allow, got deny: %v", tc.mode, tc.input, d)
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
