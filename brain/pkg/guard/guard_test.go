package guard

import (
	"bytes"
	"encoding/json"
	"errors"
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

	// All mode where share allows but schedule denies
	allSleep := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "sleep 10", "Cwd": "/data"}
		}
	}`
	d4 := runGuard(t, "all", allSleep)
	if d4.Decision != DecisionDeny {
		t.Errorf("expected deny on sleep in all mode, got: %v", d4)
	}

	// All mode benign command
	allSafe := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "echo 'safe'", "Cwd": "/data"}
		}
	}`
	d5 := runGuard(t, "all", allSafe)
	if d5.Decision != DecisionAllow {
		t.Errorf("expected allow on safe command in all mode, got: %v", d5)
	}

	// All mode where batch denies
	allBatch := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "git add ."}
		}
	}`
	d6 := runGuard(t, "all", allBatch)
	if d6.Decision != DecisionDeny {
		t.Errorf("expected deny on unbatched git add in all mode, got: %v", d6)
	}

	// Explicit batch mode
	batchDeny := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "go test ./..."}
		}
	}`
	d7 := runGuard(t, "batch", batchDeny)
	if d7.Decision != DecisionDeny {
		t.Errorf("expected deny on unscoped test in batch mode, got: %v", d7)
	}

	batchAllow := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "go test ./pkg/guard"}
		}
	}`
	d8 := runGuard(t, "batch", batchAllow)
	if d8.Decision != DecisionAllow {
		t.Errorf("expected allow on scoped test in batch mode, got: %v", d8)
	}

	// All mode where search denies
	allSearch := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "find . -name '*.go'"}
		}
	}`
	d9 := runGuard(t, "all", allSearch)
	if d9.Decision != DecisionDeny {
		t.Errorf("expected deny on find without maxdepth in all mode, got: %v", d9)
	}

	// Search mode explicit
	searchDeny := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "find / -maxdepth 1"}
		}
	}`
	d10 := runGuard(t, "search", searchDeny)
	if d10.Decision != DecisionDeny {
		t.Errorf("expected deny on root search in search mode, got: %v", d10)
	}

	searchAllow := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "find . -maxdepth 2 -name '*.go'"}
		}
	}`
	d11 := runGuard(t, "search", searchAllow)
	if d11.Decision != DecisionAllow {
		t.Errorf("expected allow on find with maxdepth in search mode, got: %v", d11)
	}

	// Commit mode explicit
	origGitRunner := gitCmdRunner
	defer func() { gitCmdRunner = origGitRunner }()
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		for _, a := range args {
			if a == "--cached" {
				return "+token = \"ghp_111111111111111111111111111111111111\"\n", nil
			}
		}
		return "", nil
	}

	commitDeny := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "git commit -m 'secret'", "Cwd": "/data"}
		}
	}`
	d12 := runGuard(t, "commit", commitDeny)
	if d12.Decision != DecisionDeny {
		t.Errorf("expected deny on commit with secret in commit mode, got: %v", d12)
	}

	// Commit allowed in commit mode
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		return "+const clean = true\n", nil
	}
	commitAllow := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "git commit -m 'clean'", "Cwd": "/data"}
		}
	}`
	d13 := runGuard(t, "commit", commitAllow)
	if d13.Decision != DecisionAllow {
		t.Errorf("expected allow on clean commit in commit mode, got: %v", d13)
	}

	// All mode where commit denies
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		for _, a := range args {
			if a == "--cached" {
				return "+token = \"ghp_111111111111111111111111111111111111\"\n", nil
			}
		}
		return "", nil
	}
	allCommitDeny := `{
		"toolCall": {
			"name": "run_command",
			"args": {"CommandLine": "git commit -m 'secret'", "Cwd": "/data"}
		}
	}`
	d14 := runGuard(t, "all", allCommitDeny)
	if d14.Decision != DecisionDeny {
		t.Errorf("expected deny on commit with secret in all mode, got: %v", d14)
	}
}

func TestGuardEdgeCases(t *testing.T) {
	t.Parallel()

	// 1. IsProtectedPath edge cases
	if IsProtectedPath("") {
		t.Errorf("expected empty path to not be protected")
	}
	if IsProtectedPath("   ") {
		t.Errorf("expected whitespace path to not be protected")
	}
	if IsProtectedPath("/nonexistent/deep/nested/path/to/check/parent/loop") {
		t.Errorf("expected random nonexistent path to not be protected")
	}

	// 2. ParseArgs edge cases
	argsNil := ParseArgs(nil)
	if argsNil.TargetFile != "" {
		t.Errorf("expected empty args on nil raw, got: %+v", argsNil)
	}
	argsEmpty := ParseArgs([]byte(""))
	if argsEmpty.TargetFile != "" {
		t.Errorf("expected empty args on empty raw, got: %+v", argsEmpty)
	}

	// Fallback map parsing with lowercase keys and non-string skipping
	mapJSON := []byte(`{
		"targetfile": "/share/aerial/bar.go",
		"commandline": "git status",
		"cwd": "/share/aerial",
		"ignored_int": 42
	}`)
	argsMap := ParseArgs(mapJSON)
	if argsMap.TargetFile != "/share/aerial/bar.go" || argsMap.CommandLine != "git status" || argsMap.Cwd != "/share/aerial" {
		t.Errorf("unexpected argsMap values: %+v", argsMap)
	}

	// 3. Git mutating command edge cases
	gitTests := []struct {
		cmd      string
		cwd      string
		wantDeny bool
	}{
		{"git --no-pager commit -m 'fix'", "/share/aerial", true},
		{"git --work-tree=/share/aerial checkout main", "/data", true},
		{"git --git-dir=/share/aerial commit", "/data", true},
		{"git -C/share/aerial commit", "/data", true},
		{"git branch --delete old-branch", "/share/aerial", true},
		{"rm local.txt", "/share/aerial", true}, // Mutating binary inside protected Cwd
		{"echo hi > /share/aerial/new.txt", "/data", true},
	}
	for _, gt := range gitTests {
		input := map[string]interface{}{
			"toolCall": map[string]interface{}{
				"name": "run_command",
				"args": map[string]string{
					"CommandLine": gt.cmd,
					"Cwd":         gt.cwd,
				},
			},
		}
		data, _ := json.Marshal(input)
		d := runGuard(t, "share", string(data))
		if gt.wantDeny && d.Decision != DecisionDeny {
			t.Errorf("cmd %q with cwd %q expected deny, got allow", gt.cmd, gt.cwd)
		}
	}

	// 4. Schedule guard edge cases
	if isSleepCommand("") {
		t.Errorf("expected empty cmd to not be sleep command")
	}
	dEmptyCmd := CheckSchedule("run_command", ToolArgs{CommandLine: ""})
	if dEmptyCmd.Decision != DecisionAllow {
		t.Errorf("expected allow on empty cmd in schedule guard")
	}

	// 5. Panic recovery in Process
	var out bytes.Buffer
	panicReader := &panickingReader{}
	if err := Process(panicReader, &out, "all"); err != nil {
		t.Errorf("expected Process to recover panic and return nil, got: %v", err)
	}
	var dPanic Decision
	if err := json.Unmarshal(out.Bytes(), &dPanic); err != nil || dPanic.Decision != DecisionAllow {
		t.Errorf("expected allow decision on recovered panic, got: %v", dPanic)
	}

	// 6. Failing reader in Process
	var outFail bytes.Buffer
	failReader := &failingReader{}
	if err := Process(failReader, &outFail, "all"); err != nil {
		t.Errorf("expected Process to handle reader error and return nil, got: %v", err)
	}

	// 7. ParseArgs with plain non-JSON string
	argsPlainStr := ParseArgs([]byte(`"plain non-json string"`))
	if argsPlainStr.TargetFile != "" {
		t.Errorf("expected empty args for plain non-json string, got: %+v", argsPlainStr)
	}
}

func TestIsProtectedPath_HermeticWithSymlinks(t *testing.T) {
	tempRoot := t.TempDir()
	tempAerial := filepath.Join(tempRoot, "share", "aerial")
	if err := os.MkdirAll(tempAerial, 0755); err != nil {
		t.Fatalf("failed to create tempAerial: %v", err)
	}
	existingFile := filepath.Join(tempAerial, "existing.go")
	if err := os.WriteFile(existingFile, []byte("package main"), 0644); err != nil {
		t.Fatalf("failed to write existingFile: %v", err)
	}

	symlinkDir := filepath.Join(tempRoot, "link-to-aerial")
	if err := os.Symlink(tempAerial, symlinkDir); err != nil {
		t.Fatalf("failed to create symlinkDir: %v", err)
	}

	origPrefixes := ProtectedPrefixes
	defer func() { ProtectedPrefixes = origPrefixes }()
	ProtectedPrefixes = []string{tempAerial}

	// 1. Exact match on prefix
	if !IsProtectedPath(tempAerial) {
		t.Errorf("expected tempAerial to be protected")
	}

	// 2. Existing file under prefix (direct EvalSymlinks success)
	if !IsProtectedPath(existingFile) {
		t.Errorf("expected existingFile to be protected")
	}

	// 3. Non-existent file under prefix (exercises parent EvalSymlinks loop)
	newFile := filepath.Join(tempAerial, "nested", "new_file.go")
	if !IsProtectedPath(newFile) {
		t.Errorf("expected newFile under tempAerial to be protected")
	}

	// 4. File via symlinked directory
	symlinkedFile := filepath.Join(symlinkDir, "new_file.go")
	if !IsProtectedPath(symlinkedFile) {
		t.Errorf("expected symlinkedFile to resolve to protected path")
	}

	// 5. Unrelated path
	unrelated := filepath.Join(tempRoot, "other", "file.go")
	if IsProtectedPath(unrelated) {
		t.Errorf("expected unrelated path to not be protected")
	}
}

type panickingReader struct{}

func (p *panickingReader) Read([]byte) (int, error) {
	panic("simulated panic in reader")
}

type failingReader struct{}

func (f *failingReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated read error")
}

