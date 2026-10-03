package guard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	testPAT     = "ghp" + "_" + "111111111111111111111111111111111111"
	testPrivKey = "-----BEGIN " + "RSA PRIVATE KEY-----"
	testDiscord = "https://discord." + "com/api/webhooks/1234567890/token-abc_XYZ"
	testSlack   = "https://" + "hooks.slack.com/" + "services/" + "T12345678" + "/" + "B12345678/" + "AbCdEfGhIjKlMnOp12345678"
	testAPIKey  = "sk" + "-" + "11111111112222222222333333333344"
	testJWT     = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

func TestScanDiff_Patterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		diff         string
		wantCategory string
		wantFile     string
		wantLine     int
		wantSample   string
	}{
		{
			name: "RFC1918 IP address detected",
			diff: `diff --git a/pkg/net/client.go b/pkg/net/client.go
--- a/pkg/net/client.go
+++ b/pkg/net/client.go
@@ -10,3 +10,4 @@
 package net
+var gateway = "192.168.1.150"
`,
			wantCategory: "RFC1918_192_168",
			wantFile:     "pkg/net/client.go",
			wantLine:     11,
			wantSample:   "192.168.1.150",
		},
		{
			name: "GitHub PAT detected",
			diff: "diff --git a/pkg/auth/token.go b/pkg/auth/token.go\n--- a/pkg/auth/token.go\n+++ b/pkg/auth/token.go\n@@ -1,3 +1,4 @@\n package auth\n+const token = \"" + testPAT + "\"\n",
			wantCategory: "GitHubPAT",
			wantFile:     "pkg/auth/token.go",
			wantLine:     2,
			wantSample:   testPAT,
		},
		{
			name: "Private Key header detected",
			diff: "diff --git a/pkg/crypto/key.pem b/pkg/crypto/key.pem\n--- a/pkg/crypto/key.pem\n+++ b/pkg/crypto/key.pem\n@@ -0,0 +1,3 @@\n+" + testPrivKey + "\nMIIEowIBAAKCAQEA0\n-----END RSA PRIVATE KEY-----\n",
			wantCategory: "PrivateKey",
			wantFile:     "pkg/crypto/key.pem",
			wantLine:     1,
			wantSample:   testPrivKey,
		},
		{
			name: "Discord Webhook detected",
			diff: "diff --git a/pkg/notify/discord.go b/pkg/notify/discord.go\n--- a/pkg/notify/discord.go\n+++ b/pkg/notify/discord.go\n@@ -5,2 +5,3 @@\n+webhookURL := \"" + testDiscord + "\"\n",
			wantCategory: "DiscordWebhook",
			wantFile:     "pkg/notify/discord.go",
			wantLine:     5,
			wantSample:   testDiscord,
		},
		{
			name: "Slack Webhook detected",
			diff: "diff --git a/pkg/notify/slack.go b/pkg/notify/slack.go\n--- a/pkg/notify/slack.go\n+++ b/pkg/notify/slack.go\n@@ -20,2 +20,3 @@\n+slackURL := \"" + testSlack + "\"\n",
			wantCategory: "SlackWebhook",
			wantFile:     "pkg/notify/slack.go",
			wantLine:     20,
			wantSample:   testSlack,
		},
		{
			name: "Generic API Key (sk-...) detected",
			diff: "diff --git a/pkg/ai/client.go b/pkg/ai/client.go\n--- a/pkg/ai/client.go\n+++ b/pkg/ai/client.go\n@@ -10,3 +10,4 @@\n+apiKey := \"" + testAPIKey + "\"\n",
			wantCategory: "GenericAPIKey",
			wantFile:     "pkg/ai/client.go",
			wantLine:     10,
			wantSample:   testAPIKey,
		},
		{
			name: "JWT Token detected",
			diff: "diff --git a/pkg/auth/jwt.go b/pkg/auth/jwt.go\n--- a/pkg/auth/jwt.go\n+++ b/pkg/auth/jwt.go\n@@ -1,2 +1,3 @@\n+rawJWT := \"" + testJWT + "\"\n",
			wantCategory: "JWTToken",
			wantFile:     "pkg/auth/jwt.go",
			wantLine:     1,
			wantSample:   testJWT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			violations := ScanDiff(tt.diff, false)
			if len(violations) == 0 {
				t.Fatalf("expected violation for %s, got none", tt.name)
			}
			found := false
			for _, v := range violations {
				if v.Category == tt.wantCategory {
					found = true
					if v.File != tt.wantFile {
						t.Errorf("expected file %q, got %q", tt.wantFile, v.File)
					}
					if v.LineNum != tt.wantLine {
						t.Errorf("expected line %d, got %d", tt.wantLine, v.LineNum)
					}
					if v.Sample != tt.wantSample {
						t.Errorf("expected sample %q, got %q", tt.wantSample, v.Sample)
					}
				}
			}
			if !found {
				t.Errorf("expected category %q in violations: %+v", tt.wantCategory, violations)
			}
		})
	}
}

func TestScanDiff_IgnoredLinesAndTestFiles(t *testing.T) {
	t.Parallel()

	// 1. Deleted lines must not trigger violations
	deletionDiff := "diff --git a/pkg/auth/token.go b/pkg/auth/token.go\n--- a/pkg/auth/token.go\n+++ b/pkg/auth/token.go\n@@ -1,4 +1,3 @@\n-const oldToken = \"" + testPAT + "\"\n+const oldToken = \"\"\n"
	if violations := ScanDiff(deletionDiff, false); len(violations) != 0 {
		t.Errorf("expected 0 violations for deleted token line, got: %+v", violations)
	}

	// 2. Diff headers (+++) must not trigger violations even if path contains token-like text
	headerDiff := "diff --git a/pkg/" + testPAT + ".go b/pkg/" + testPAT + ".go\n--- a/pkg/" + testPAT + ".go\n+++ b/pkg/" + testPAT + ".go\n@@ -1,2 +1,2 @@\n+// safe comment\n"
	if violations := ScanDiff(headerDiff, false); len(violations) != 0 {
		t.Errorf("expected 0 violations for diff headers, got: %+v", violations)
	}

	// 3. Binary diff markers must be skipped
	binaryDiff := `diff --git a/assets/image.png b/assets/image.png
Binary files a/assets/image.png and b/assets/image.png differ
`
	if violations := ScanDiff(binaryDiff, false); len(violations) != 0 {
		t.Errorf("expected 0 violations for binary diff, got: %+v", violations)
	}

	// 4. Test files and fixtures skipped when checkTestFiles is false
	testFilesDiffs := []struct {
		name string
		diff string
	}{
		{
			name: "Go test file skipped",
			diff: "diff --git a/pkg/guard/commit_test.go b/pkg/guard/commit_test.go\n--- a/pkg/guard/commit_test.go\n+++ b/pkg/guard/commit_test.go\n@@ -1,2 +1,3 @@\n+const dummy = \"" + testPAT + "\"\n",
		},
		{
			name: "Python test file skipped",
			diff: "diff --git a/tests/test_auth_test.py b/tests/test_auth_test.py\n--- a/tests/test_auth_test.py\n+++ b/tests/test_auth_test.py\n@@ -1,2 +1,3 @@\n+TOKEN = \"" + testPAT + "\"\n",
		},
		{
			name: "TypeScript test file skipped",
			diff: "diff --git a/src/client.test.ts b/src/client.test.ts\n--- a/src/client.test.ts\n+++ b/src/client.test.ts\n@@ -1,2 +1,3 @@\n+const token = \"" + testPAT + "\";\n",
		},
		{
			name: "JavaScript test file skipped",
			diff: "diff --git a/src/api.test.js b/src/api.test.js\n--- a/src/api.test.js\n+++ b/src/api.test.js\n@@ -1,2 +1,3 @@\n+const key = \"" + testAPIKey + "\";\n",
		},
		{
			name: "Fixtures path skipped",
			diff: `diff --git a/test/fixtures/payload.json b/test/fixtures/payload.json
--- a/test/fixtures/payload.json
+++ b/test/fixtures/payload.json
@@ -1,2 +1,3 @@
+{"ip": "192.168.1.1"}
`,
		},
		{
			name: "Testdata path skipped",
			diff: "diff --git a/pkg/testdata/tokens.txt b/pkg/testdata/tokens.txt\n--- a/pkg/testdata/tokens.txt\n+++ b/pkg/testdata/tokens.txt\n@@ -1,2 +1,3 @@\n+" + testPAT + "\n",
		},
	}

	for _, tc := range testFilesDiffs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// When checkTestFiles is false, should be 0 violations
			violations := ScanDiff(tc.diff, false)
			if len(violations) != 0 {
				t.Errorf("expected 0 violations when checkTestFiles=false for %s, got: %+v", tc.name, violations)
			}
			// When checkTestFiles is true, should detect violation
			violationsChecked := ScanDiff(tc.diff, true)
			if len(violationsChecked) == 0 {
				t.Errorf("expected violations when checkTestFiles=true for %s, got 0", tc.name)
			}
		})
	}
}

func TestIsCommitCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cmd        string
		wantCommit bool
		wantDir    string
		wantSubmit bool
	}{
		{
			name:       "simple git commit",
			cmd:        `git commit -m "feat: new feature"`,
			wantCommit: true,
			wantDir:    "",
			wantSubmit: false,
		},
		{
			name:       "git -C with directory",
			cmd:        `git -C /data/scratch/workspace commit -m "fix: bug"`,
			wantCommit: true,
			wantDir:    "/data/scratch/workspace",
			wantSubmit: false,
		},
		{
			name:       "git -C without space",
			cmd:        `git -C/data/scratch/workspace commit`,
			wantCommit: true,
			wantDir:    "/data/scratch/workspace",
			wantSubmit: false,
		},
		{
			name:       "chained git add and git commit",
			cmd:        `git add . && git commit -m "commit"`,
			wantCommit: true,
			wantDir:    "",
			wantSubmit: false,
		},
		{
			name:       "semicolon chained git commit",
			cmd:        `git add -A; git -C /tmp/repo commit -m "chained"`,
			wantCommit: true,
			wantDir:    "/tmp/repo",
			wantSubmit: false,
		},
		{
			name:       "scripts/aerial-pr.sh submit with scratch dir",
			cmd:        `scripts/aerial-pr.sh submit /data/scratch/aerial-code-scratch.123 "test PR"`,
			wantCommit: true,
			wantDir:    "/data/scratch/aerial-code-scratch.123",
			wantSubmit: true,
		},
		{
			name:       "./scripts/aerial-pr.sh submit",
			cmd:        `./scripts/aerial-pr.sh submit /tmp/dir`,
			wantCommit: true,
			wantDir:    "/tmp/dir",
			wantSubmit: true,
		},
		{
			name:       "bash invocation of aerial-pr.sh submit",
			cmd:        `bash /share/aerial/scripts/aerial-pr.sh submit /data/scratch/work "msg"`,
			wantCommit: true,
			wantDir:    "/data/scratch/work",
			wantSubmit: true,
		},
		{
			name:       "non-commit git status",
			cmd:        `git status`,
			wantCommit: false,
			wantDir:    "",
			wantSubmit: false,
		},
		{
			name:       "non-commit git diff",
			cmd:        `git diff HEAD`,
			wantCommit: false,
			wantDir:    "",
			wantSubmit: false,
		},
		{
			name:       "echo command containing git commit",
			cmd:        `echo "git commit"`,
			wantCommit: false,
			wantDir:    "",
			wantSubmit: false,
		},
		{
			name:       "aerial-pr.sh init is not submit",
			cmd:        `scripts/aerial-pr.sh init aerial`,
			wantCommit: false,
			wantDir:    "",
			wantSubmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotCommit, gotDir, gotSubmit := IsCommitCommand(tt.cmd)
			if gotCommit != tt.wantCommit {
				t.Errorf("IsCommitCommand(%q) gotCommit = %v, want %v", tt.cmd, gotCommit, tt.wantCommit)
			}
			if gotDir != tt.wantDir {
				t.Errorf("IsCommitCommand(%q) gotDir = %q, want %q", tt.cmd, gotDir, tt.wantDir)
			}
			if gotSubmit != tt.wantSubmit {
				t.Errorf("IsCommitCommand(%q) gotSubmit = %v, want %v", tt.cmd, gotSubmit, tt.wantSubmit)
			}
		})
	}
}

func TestIsAerialConfig(t *testing.T) {
	t.Parallel()

	// 1. Direct path matches
	if !IsAerialConfig("/data/scratch/aerial-config-scratch.abc", "git commit") {
		t.Errorf("expected true when targetDir contains aerial-config")
	}

	// 2. Command argument matches
	if !IsAerialConfig("/tmp/repo", "scripts/aerial-pr.sh submit /tmp/repo --repo aerial-config") {
		t.Errorf("expected true when command contains aerial-config")
	}

	// 3. .git/config file matches
	tmpRepo := t.TempDir()
	gitDir := filepath.Join(tmpRepo, ".git")
	if err := os.MkdirAll(gitDir, 0755); err != nil {
		t.Fatalf("failed to create .git: %v", err)
	}
	cfgContent := `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = git@github.com:azylman/aerial-config.git
	fetch = +refs/heads/*:refs/remotes/origin/*
`
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write git config: %v", err)
	}
	if !IsAerialConfig(tmpRepo, "git commit") {
		t.Errorf("expected true when .git/config contains aerial-config")
	}

	// 4. Git worktree .git file matches
	worktreeDir := t.TempDir()
	sharedGitDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedGitDir, "config"), []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write shared git config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(worktreeDir, ".git"), []byte("gitdir: "+sharedGitDir), 0644); err != nil {
		t.Fatalf("failed to write .git worktree pointer: %v", err)
	}
	if !IsAerialConfig(worktreeDir, "git commit") {
		t.Errorf("expected true when worktree .git points to aerial-config repo")
	}

	// 5. Unrelated repo returns false
	cleanRepo := t.TempDir()
	cleanGitDir := filepath.Join(cleanRepo, ".git")
	_ = os.MkdirAll(cleanGitDir, 0755)
	_ = os.WriteFile(filepath.Join(cleanGitDir, "config"), []byte("[remote \"origin\"]\nurl = https://github.com/azylman/aerial.git\n"), 0644)
	if IsAerialConfig(cleanRepo, "git commit") {
		t.Errorf("expected false for standard aerial repo")
	}
}

func TestCheckCommit(t *testing.T) {
	// 1. Non-run_command tool -> allow
	d1 := CheckCommit("write_to_file", ToolArgs{TargetFile: "/tmp/foo"})
	if d1.Decision != DecisionAllow {
		t.Errorf("expected allow for non-run_command tool, got: %v", d1)
	}

	// 2. Non-commit run_command -> allow
	d2 := CheckCommit("run_command", ToolArgs{CommandLine: "git status"})
	if d2.Decision != DecisionAllow {
		t.Errorf("expected allow for non-commit run_command, got: %v", d2)
	}

	// 3. aerial-config target -> allow even if violations would otherwise trigger
	d3 := CheckCommit("run_command", ToolArgs{
		CommandLine: "git commit -m 'update secret'",
		Cwd:         "/data/scratch/aerial-config-scratch.test",
	})
	if d3.Decision != DecisionAllow {
		t.Errorf("expected allow for aerial-config target, got: %v", d3)
	}

	// 4. Mock git runner with violation diff on git commit -> deny
	origGitRunner := gitCmdRunner
	defer func() { gitCmdRunner = origGitRunner }()

	gitCmdRunner = func(dir string, args ...string) (string, error) {
		for _, a := range args {
			if a == "--cached" {
				return "diff --git a/pkg/auth.go b/pkg/auth.go\n--- a/pkg/auth.go\n+++ b/pkg/auth.go\n@@ -1,1 +1,2 @@\n+apiKey := \"" + testAPIKey + "\"\n", nil
			}
		}
		return "", nil
	}

	d4 := CheckCommit("run_command", ToolArgs{
		CommandLine: "git commit -m 'add key'",
		Cwd:         "/data/scratch/aerial-code",
	})
	if d4.Decision != DecisionDeny {
		t.Errorf("expected deny for secret in staged commit, got: %v", d4)
	}
	if !strings.Contains(d4.Reason, "GenericAPIKey") {
		t.Errorf("expected mention of GenericAPIKey in reason, got: %s", d4.Reason)
	}

	// 5. Mock git runner with clean diff -> allow
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		return `diff --git a/pkg/auth.go b/pkg/auth.go
--- a/pkg/auth.go
+++ b/pkg/auth.go
@@ -1,1 +1,2 @@
+const timeout = 30
`, nil
	}

	d5 := CheckCommit("run_command", ToolArgs{
		CommandLine: "git commit -m 'clean commit'",
		Cwd:         "/data/scratch/aerial-code",
	})
	if d5.Decision != DecisionAllow {
		t.Errorf("expected allow for clean commit, got: %v", d5)
	}

	// 6. aerial-pr.sh submit with untracked file violation -> deny
	tmpWorkDir := t.TempDir()
	untrackedFile := filepath.Join(tmpWorkDir, "secret.env")
	if err := os.WriteFile(untrackedFile, []byte("SECRET_IP=192.168.1.99\n"), 0644); err != nil {
		t.Fatalf("failed to write untracked file: %v", err)
	}

	gitCmdRunner = func(dir string, args ...string) (string, error) {
		for _, a := range args {
			if a == "--others" {
				return "secret.env\n", nil
			}
		}
		return "", nil
	}

	d6 := CheckCommit("run_command", ToolArgs{
		CommandLine: "scripts/aerial-pr.sh submit " + tmpWorkDir + ` "test pr"`,
	})
	if d6.Decision != DecisionDeny {
		t.Errorf("expected deny for untracked file with violation in aerial-pr.sh submit, got: %v", d6)
	}
	if !strings.Contains(d6.Reason, "RFC1918_192_168") {
		t.Errorf("expected RFC1918_192_168 in reason, got: %s", d6.Reason)
	}
}

func TestCustomPatterns(t *testing.T) {
	// 1. Load from JSON file format
	tmpHome := t.TempDir()
	configDir := filepath.Join(tmpHome, ".gemini", "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	jsonPatterns := `[
		{"category": "InternalDomain", "pattern": "internal\\.corp\\.net"},
		{"category": "CustomSecret", "pattern": "super_secret_token_[0-9]+"}
	]`
	if err := os.WriteFile(filepath.Join(configDir, "commit-guard.json"), []byte(jsonPatterns), 0644); err != nil {
		t.Fatalf("failed to write commit-guard.json: %v", err)
	}

	rules := LoadCustomPatterns(tmpHome)
	if len(rules) != 2 {
		t.Fatalf("expected 2 custom rules from JSON, got %d", len(rules))
	}

	// Scan diff with these custom rules
	diff := `diff --git a/pkg/config.go b/pkg/config.go
+++ b/pkg/config.go
@@ -1,1 +1,2 @@
+host := "internal.corp.net"
`
	violations := ScanDiffWithPatterns(diff, false, rules)
	if len(violations) != 1 || violations[0].Category != "InternalDomain" {
		t.Errorf("expected InternalDomain violation, got: %+v", violations)
	}

	// 2. Load from environment variable
	t.Setenv("AERIAL_COMMIT_GUARD_PATTERNS", `{"EnvSecret": "my-env-secret-[a-z]+"}`)
	envRules := LoadCustomPatterns("")
	foundEnvRule := false
	for _, r := range envRules {
		if r.Category == "EnvSecret" {
			foundEnvRule = true
		}
	}
	if !foundEnvRule {
		t.Errorf("expected EnvSecret rule loaded from environment")
	}

	// Test comma-separated raw regexes in env var
	t.Setenv("AERIAL_COMMIT_GUARD_PATTERNS", `foo-secret-\d+,bar-token-[a-z]+`)
	rawRules := LoadCustomPatterns("")
	if len(rawRules) < 2 {
		t.Errorf("expected at least 2 raw rules from comma-separated env var, got: %d", len(rawRules))
	}
}

func TestIsAllCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cmd  string
		want bool
	}{
		{"git commit -a -m 'msg'", true},
		{"git commit -am 'msg'", true},
		{"git commit --all -m 'msg'", true},
		{"git commit -m 'msg'", false},
		{"git status", false},
	}

	for _, tt := range tests {
		if got := isAllCommit(tt.cmd); got != tt.want {
			t.Errorf("isAllCommit(%q) = %v, want %v", tt.cmd, got, tt.want)
		}
	}
}

func TestScanDiff_EmptyContextLines(t *testing.T) {
	t.Parallel()

	diff := "diff --git a/pkg/service.go b/pkg/service.go\n" +
		"--- a/pkg/service.go\n" +
		"+++ b/pkg/service.go\n" +
		"@@ -10,6 +10,7 @@\n" +
		" existing_line_1\n" +
		"\n" + // empty context line without leading space
		" existing_line_2\n" +
		"+ip := \"192.168.1.100\"\n"

	violations := ScanDiff(diff, true)
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(violations))
	}
	// Line 10: existing_line_1, Line 11: empty line, Line 12: existing_line_2, Line 13: added line
	if violations[0].LineNum != 13 {
		t.Errorf("expected lineNum 13, got %d", violations[0].LineNum)
	}
}

func TestCheckCommit_RelativeDirAndCleanHead(t *testing.T) {
	oldRunner := gitCmdRunner
	defer func() { gitCmdRunner = oldRunner }()

	var executedDirs []string
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		executedDirs = append(executedDirs, dir)
		if len(args) > 1 && args[0] == "diff" && args[1] == "HEAD" {
			// clean diff against HEAD (no error, empty string)
			return "", nil
		}
		return "", nil
	}

	args := ToolArgs{
		CommandLine: "./scripts/aerial-pr.sh submit relative_scratch 'feat: test'",
		Cwd:         "/workspace/root",
	}

	dec := CheckCommit("run_command", args)
	if dec.Decision != DecisionAllow {
		t.Fatalf("expected allow for clean submit, got %s", dec.Decision)
	}

	// Verify targetDir was joined with args.Cwd
	expectedDir := "/workspace/root/relative_scratch"
	found := false
	for _, d := range executedDirs {
		if d == expectedDir {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected executedDir to contain joined path %q, got: %v", expectedDir, executedDirs)
	}
}
