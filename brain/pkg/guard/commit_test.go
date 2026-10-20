package guard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testPAT     = "ghp_" + "111111111122222222223333333333444444"
	testPrivKey = "-----BEGIN RSA PRIVATE KEY-----"
	testDiscord = "https://discord.com/" + "api/webhooks/123456789/abcdefghijklmnopqrstuvwxyz"
	testSlack   = "https://hooks." + "slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
	testAPIKey  = "sk-" + "abcdefghijklmnopqrstuvwxyz123456"
	testJWT     = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

func testDiff(file string, line int, added string) string {
	return fmt.Sprintf("diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n@@ -1,1 +%d,1 @@\n+%s\n", file, file, file, file, line, added)
}

func TestScanDiff(t *testing.T) {
	t.Parallel()

	patternTests := []struct {
		name         string
		diff         string
		wantCategory string
		wantFile     string
		wantLine     int
		wantSample   string
	}{
		{"cluster domain", testDiff("pkg/net/client.go", 11, `var watcherURL = "http://mirrormere-cast-watcher.aerial/action"`), "InternalClusterDomain", "pkg/net/client.go", 11, "mirrormere-cast-watcher.aerial"},
		{"lan domain", testDiff("pkg/net/client.go", 11, `var edgeURL = "http://ollama.lan:11434"`), "InternalClusterDomain", "pkg/net/client.go", 11, "ollama.lan"},
		{"RFC1918 IP", testDiff("pkg/net/client.go", 11, `var gateway = "192.168.1.150"`), "RFC1918_192_168", "pkg/net/client.go", 11, "192.168.1.150"},
		{"GitHub PAT", testDiff("pkg/auth/token.go", 2, `const token = "`+testPAT+`"`), "GitHubPAT", "pkg/auth/token.go", 2, testPAT},
		{"Private Key", "diff --git a/pkg/crypto/key.pem b/pkg/crypto/key.pem\n--- a/pkg/crypto/key.pem\n+++ b/pkg/crypto/key.pem\n@@ -0,0 +1,3 @@\n+" + testPrivKey + "\nMIIEowIBAAKCAQEA0\n-----END RSA PRIVATE KEY-----\n", "PrivateKey", "pkg/crypto/key.pem", 1, testPrivKey},
		{"Discord Webhook", testDiff("pkg/notify/discord.go", 5, `webhookURL := "`+testDiscord+`"`), "DiscordWebhook", "pkg/notify/discord.go", 5, testDiscord},
		{"Slack Webhook", testDiff("pkg/notify/slack.go", 20, `slackURL := "`+testSlack+`"`), "SlackWebhook", "pkg/notify/slack.go", 20, testSlack},
		{"Generic API Key", testDiff("pkg/ai/client.go", 10, `apiKey := "`+testAPIKey+`"`), "GenericAPIKey", "pkg/ai/client.go", 10, testAPIKey},
		{"JWT Token", testDiff("pkg/auth/jwt.go", 1, `rawJWT := "`+testJWT+`"`), "JWTToken", "pkg/auth/jwt.go", 1, testJWT},
	}

	for _, tt := range patternTests {
		t.Run("pattern_"+tt.name, func(t *testing.T) {
			t.Parallel()
			violations := ScanDiff(tt.diff, false)
			if len(violations) == 0 {
				t.Fatalf("expected violation for %s, got none", tt.name)
			}
			found := false
			for _, v := range violations {
				if v.Category == tt.wantCategory {
					found = true
					if v.File != tt.wantFile || v.LineNum != tt.wantLine || v.Sample != tt.wantSample {
						t.Errorf("violation mismatch: got %+v", v)
					}
				}
			}
			if !found {
				t.Errorf("expected category %q in violations: %+v", tt.wantCategory, violations)
			}
		})
	}

	ignoredDiffs := []struct {
		name       string
		diff       string
		checkTests bool
		wantCount  int
	}{
		{"deleted lines", "diff --git a/pkg/auth/token.go b/pkg/auth/token.go\n--- a/pkg/auth/token.go\n+++ b/pkg/auth/token.go\n@@ -1,4 +1,3 @@\n-const oldToken = \"" + testPAT + "\"\n+const oldToken = \"\"\n", false, 0},
		{"diff header path", "diff --git a/pkg/" + testPAT + ".go b/pkg/" + testPAT + ".go\n--- a/pkg/" + testPAT + ".go\n+++ b/pkg/" + testPAT + ".go\n@@ -1,2 +1,2 @@\n+// safe comment\n", false, 0},
		{"binary diff", "diff --git a/assets/image.png b/assets/image.png\nBinary files a/assets/image.png and b/assets/image.png differ\n", false, 0},
		{"Go test file", testDiff("pkg/guard/commit_test.go", 1, `const dummy = "`+testPAT+`"`), false, 0},
		{"Python test file", testDiff("tests/test_auth_test.py", 1, `TOKEN = "`+testPAT+`"`), false, 0},
		{"TypeScript test file", testDiff("src/client.test.ts", 1, `const token = "`+testPAT+`";`), false, 0},
		{"JavaScript test file", testDiff("src/api.test.js", 1, `const key = "`+testAPIKey+`";`), false, 0},
		{"Fixtures path", testDiff("test/fixtures/payload.json", 1, `{"ip": "192.168.1.1"}`), false, 0},
		{"Testdata skipped", testDiff("pkg/testdata/tokens.txt", 1, testPAT), false, 0},
		{"Test checked when enabled", testDiff("pkg/testdata/tokens.txt", 1, testPAT), true, 1},
		{"empty context lines", "diff --git a/pkg/example.go b/pkg/example.go\n--- a/pkg/example.go\n+++ b/pkg/example.go\n@@ -1,5 +1,5 @@\n package example\n\n-var old = 1\n+var old = 2\n\n", false, 0},
	}

	for _, tc := range ignoredDiffs {
		t.Run("ignored_"+tc.name, func(t *testing.T) {
			t.Parallel()
			v := ScanDiff(tc.diff, tc.checkTests)
			if len(v) != tc.wantCount {
				t.Fatalf("ScanDiff(%s) got %d violations, want %d: %+v", tc.name, len(v), tc.wantCount, v)
			}
		})
	}

	hunkTests := []struct {
		name string
		hunk string
		want int
	}{
		{"valid hunk", "@@ -1,4 +10,5 @@", 10},
		{"missing plus", "@@ -1,5 @@", 1},
		{"invalid number", "@@ -1,5 +notanumber @@", 1},
		{"empty line", "", 1},
	}
	for _, tc := range hunkTests {
		t.Run("hunk_"+tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseHunkLineNum(tc.hunk)
			if got != tc.want {
				t.Fatalf("parseHunkLineNum(%q) = %d, want %d", tc.hunk, got, tc.want)
			}
		})
	}
}

func TestCustomPatterns(t *testing.T) {
	// Not t.Parallel() because it exercises t.Setenv
	payloadTests := []struct {
		name     string
		input    string
		wantLen  int
		wantCat0 string
	}{
		{"empty", "", 0, ""},
		{"whitespace", "   \n\t  ", 0, ""},
		{"format 1 default category", `[{"pattern": "abc\\d+"}]`, 1, "CustomRule"},
		{"format 1 explicit category", `[{"category": "Auth", "pattern": "key-\\w+"}]`, 1, "Auth"},
		{"format 2 wrapper", `{"patterns": [{"category": "MyCat", "pattern": "xyz"}, {"pattern": "def"}]}`, 2, "MyCat"},
		{"format 3 key-value map", `{"CatA": "patA", "CatB": "patB"}`, 2, ""},
		{"format 4 line delimited", "pattern_one_\\d+\n# comment\npattern_two_\\w+", 2, "CustomRule_1"},
		{"format 4 comma delimited", "patA, patB", 2, "CustomRule_1"},
		{"invalid regex skipped", `[{"pattern": "["}]`, 0, ""},
	}

	for _, tc := range payloadTests {
		t.Run("payload_"+tc.name, func(t *testing.T) {
			rules := parsePatternPayload([]byte(tc.input))
			if len(rules) != tc.wantLen {
				t.Fatalf("parsePatternPayload(%s) len = %d, want %d", tc.name, len(rules), tc.wantLen)
			}
			if tc.wantCat0 != "" && len(rules) > 0 && rules[0].Category != tc.wantCat0 {
				t.Fatalf("rules[0].Category = %q, want %q", rules[0].Category, tc.wantCat0)
			}
		})
	}

	// ScanDiffWithPatterns with custom rules
	customRules := parsePatternPayload([]byte(`[{"category": "APISecret", "pattern": "secret-[a-z0-9]+"}]`))
	diff := "diff --git a/file.go b/file.go\n--- a/file.go\n+++ b/file.go\n@@ -1,1 +1,2 @@\n+const token = \"secret-12345abc\"\n"
	violations := ScanDiffWithPatterns(diff, false, customRules)
	if len(violations) != 1 || violations[0].Category != "APISecret" {
		t.Fatalf("expected 1 custom violation APISecret, got: %+v", violations)
	}

	// FormatCommitDenyReason check
	reason := FormatCommitDenyReason(violations)
	if !strings.Contains(reason, "APISecret") || !strings.Contains(reason, "secret-12345abc") {
		t.Fatalf("FormatCommitDenyReason missing details: %s", reason)
	}

	// LoadCustomPatterns file loading checks
	tmpHome := t.TempDir()
	geminiConfig := filepath.Join(tmpHome, ".gemini", "config")
	if err := os.MkdirAll(geminiConfig, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(geminiConfig, "commit-guard.json"), []byte(`[{"pattern": "cg_pat_\\d+"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	rulesCG := LoadCustomPatterns(tmpHome)
	if len(rulesCG) != 1 {
		t.Fatalf("expected 1 rule from commit-guard.json, got %d", len(rulesCG))
	}

	// LoadCustomPatterns guard.json fallback
	tmpHome2 := t.TempDir()
	geminiConfig2 := filepath.Join(tmpHome2, ".gemini", "config")
	if err := os.MkdirAll(geminiConfig2, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(geminiConfig2, "guard.json"), []byte(`[{"pattern": "g_pat_\\d+"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	rulesG := LoadCustomPatterns(tmpHome2)
	if len(rulesG) != 1 {
		t.Fatalf("expected 1 rule from guard.json, got %d", len(rulesG))
	}

	// Empty homeDir fallback
	_ = LoadCustomPatterns("")

	// GetActivePatterns with environment variable
	t.Setenv("AERIAL_COMMIT_GUARD_PATTERNS", `env_pat_\d+`)
	activeWithEnv := GetActivePatterns()
	if len(activeWithEnv) <= len(BuiltinPatterns) {
		t.Fatalf("expected GetActivePatterns with env to be augmented, got %d", len(activeWithEnv))
	}
	os.Unsetenv("AERIAL_COMMIT_GUARD_PATTERNS")
}

func TestRepoAndCommitDetection(t *testing.T) {
	// Not t.Parallel() because it touches gitCmdRunner
	oldRunner := gitCmdRunner
	defer func() { gitCmdRunner = oldRunner }()

	commitTests := []struct {
		name       string
		cmd        string
		wantCommit bool
		wantDir    string
		wantSubmit bool
	}{
		{"simple git commit", `git commit -m "feat: new feature"`, true, "", false},
		{"git commit -am", `git commit -am "fix: test"`, true, "", false},
		{"./scripts/aerial-pr.sh submit", `./scripts/aerial-pr.sh submit /tmp/scratch "feat: test"`, true, "/tmp/scratch", true},
		{"scripts/aerial-pr.sh submit", `scripts/aerial-pr.sh submit /data/scratch/dir`, true, "/data/scratch/dir", true},
		{"bash aerial-pr.sh submit", `bash -c "./scripts/aerial-pr.sh submit /tmp/scratch 'commit'"`, true, "/tmp/scratch", true},
		{"git -C with dir", `git -C /tmp/repo commit -m "test"`, true, "/tmp/repo", false},
		{"git -C without space", `git -C/tmp/repo commit -m "test"`, true, "/tmp/repo", false},
		{"semicolon chained commit", `cd /tmp/repo; git commit -m "msg"`, true, "", false},
		{"chained git add and commit", `git add . && git commit -m "msg"`, true, "", false},
		{"echo commit allowed", `echo "git commit"`, false, "", false},
		{"git diff allowed", `git diff`, false, "", false},
		{"git status allowed", `git status`, false, "", false},
		{"aerial-pr.sh init allowed", `./scripts/aerial-pr.sh init azylman/aerial`, false, "", false},
	}

	for _, tc := range commitTests {
		t.Run("commit_"+tc.name, func(t *testing.T) {
			isCommit, targetDir, isSubmit := IsCommitCommand(tc.cmd)
			if isCommit != tc.wantCommit || targetDir != tc.wantDir || isSubmit != tc.wantSubmit {
				t.Fatalf("IsCommitCommand(%q) = (%v, %q, %v); want (%v, %q, %v)", tc.cmd, isCommit, targetDir, isSubmit, tc.wantCommit, tc.wantDir, tc.wantSubmit)
			}
		})
	}

	allCommitTests := []struct {
		name string
		cmd  string
		want bool
	}{
		{"git commit -a", "git commit -a -m 'msg'", true},
		{"git commit -am", "git commit -am 'msg'", true},
		{"git commit --all", "git commit --all -m 'msg'", true},
		{"bash -c git commit -a", " ; bash -c 'git commit -a -m msg'", true},
		{"bash -c git commit -m", "bash -c 'git commit -m msg'", false},
		{"git commit -m only", "git commit -m 'msg'", false},
		{"non-git command", "echo 'git commit -a'", false},
	}
	for _, tc := range allCommitTests {
		t.Run("allcommit_"+tc.name, func(t *testing.T) {
			got := isAllCommit(tc.cmd)
			if got != tc.want {
				t.Fatalf("isAllCommit(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	// IsAerialConfig checks
	tmpDir := t.TempDir()
	aerialConfigDir := filepath.Join(tmpDir, "worktrees", "wt-config")
	if err := os.MkdirAll(aerialConfigDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(aerialConfigDir, "config"), []byte("[remote \"origin\"]\nurl = https://github.com/azylman/aerial-config.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	worktreeRoot := filepath.Join(tmpDir, "wt-root")
	if err := os.MkdirAll(worktreeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktreeRoot, ".git"), []byte("gitdir: "+aerialConfigDir+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Commondir traversal worktree
	wtCommonRoot := filepath.Join(tmpDir, "wt-common-root")
	wtCommonGit := filepath.Join(tmpDir, "wt-common-git")
	if err := os.MkdirAll(wtCommonRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(wtCommonGit, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wtCommonRoot, ".git"), []byte("gitdir: "+wtCommonGit+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wtCommonGit, "commondir"), []byte(aerialConfigDir+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	configTests := []struct {
		name string
		dir  string
		cmd  string
		want bool
	}{
		{"dir contains aerial-config", "/data/scratch/aerial-config-scratch.abc", "git commit", true},
		{"cmd contains aerial-config", "/tmp/repo", "scripts/aerial-pr.sh submit /tmp/repo --repo aerial-config", true},
		{"worktree points to aerial-config", worktreeRoot, "git commit", true},
		{"worktree commondir points to aerial-config", wtCommonRoot, "git commit", true},
		{"clean repo", "/data/scratch/aerial-code-scratch.xyz", "git commit", false},
	}
	for _, tc := range configTests {
		t.Run("config_"+tc.name, func(t *testing.T) {
			got := IsAerialConfig(tc.dir, tc.cmd)
			if got != tc.want {
				t.Fatalf("IsAerialConfig(%q, %q) = %v, want %v", tc.dir, tc.cmd, got, tc.want)
			}
		})
	}

	// IsCoreAerial checks
	tmpAerial := filepath.Join(tmpDir, "aerial-monorepo")
	if err := os.MkdirAll(filepath.Join(tmpAerial, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpAerial, ".git", "config"), []byte("[remote \"origin\"]\nurl = git@github.com:azylman/aerial.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Core aerial without .git (fallback checks brain/go.mod + scripts/aerial-pr.sh)
	tmpFallback := filepath.Join(tmpDir, "aerial-fallback")
	if err := os.MkdirAll(filepath.Join(tmpFallback, "brain"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmpFallback, "scripts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpFallback, "brain", "go.mod"), []byte("module github.com/azylman/aerial/brain\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpFallback, "scripts", "aerial-pr.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	coreTests := []struct {
		name string
		dir  string
		cmd  string
		want bool
	}{
		{"core aerial repo", tmpAerial, "git commit", true},
		{"core aerial fallback structure", tmpFallback, "git commit", true},
		{"aerial-config repo", aerialConfigDir, "git commit", false},
		{"other repo", tmpDir, "git commit", false},
	}
	for _, tc := range coreTests {
		t.Run("core_"+tc.name, func(t *testing.T) {
			got := IsCoreAerial(tc.dir, tc.cmd)
			if got != tc.want {
				t.Fatalf("IsCoreAerial(%q, %q) = %v, want %v", tc.dir, tc.cmd, got, tc.want)
			}
		})
	}

	// Git origin runner check
	gitCmdRunner = func(dir string, args ...string) (string, error) {
		return "https://github.com/azylman/aerial.git", nil
	}
	if !hasRepoOriginMatch(t.TempDir(), func(s string) bool { return strings.Contains(s, "aerial") }) {
		t.Fatalf("expected hasRepoOriginMatch to succeed via gitCmdRunner")
	}
}

func TestCheckCommit(t *testing.T) {
	oldRunner := gitCmdRunner
	defer func() { gitCmdRunner = oldRunner }()

	tmpCore := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpCore, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpCore, ".git", "config"), []byte("[remote \"origin\"]\nurl = git@github.com:azylman/aerial.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tmpWorkspace := t.TempDir()
	untrackedFile := filepath.Join(tmpWorkspace, "secret.txt")
	if err := os.WriteFile(untrackedFile, []byte("key = "+testAPIKey), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		tool       string
		cmd        string
		cwd        string
		mockDiff   string
		emptyTree  bool
		untracked  bool
		useCoreDir bool
		wantDeny   bool
		wantReason string
	}{
		{name: "non-commit allowed", tool: "run_command", cmd: "git status"},
		{name: "aerial-config bypass", tool: "run_command", cmd: "git commit -m 'feat'", cwd: "/share/aerial-config"},
		{name: "clean diff allowed", tool: "run_command", cmd: "git commit -m 'clean'", cwd: "/workspace", mockDiff: "diff --git a/pkg/file.go b/pkg/file.go\n+var x = 1\n"},
		{name: "sensitive diff denied", tool: "run_command", cmd: "git commit -m 'leak'", cwd: "/workspace", mockDiff: "diff --git a/token.go b/token.go\n+const token = \"" + testPAT + "\"\n", wantDeny: true, wantReason: "GitHubPAT"},
		{name: "internal cluster in core", tool: "run_command", cmd: "git commit -m 'mesh'", useCoreDir: true, mockDiff: "diff --git a/main.go b/main.go\n+var s = \"http://db.aerial:5432\"\n"},
		{name: "unborn branch empty tree", tool: "run_command", cmd: "./scripts/aerial-pr.sh submit /tmp/scratch 'feat'", cwd: "/workspace", emptyTree: true},
		{name: "colon tool name and all commit", tool: "default_api:run_command", cmd: "git commit -a -m 'all'", cwd: "/workspace"},
		{name: "relative dir clean", tool: "run_command", cmd: "./scripts/aerial-pr.sh submit relative/scratch 'clean'", cwd: "/data/repo"},
		{name: "untracked secret denied", tool: "run_command", cmd: "./scripts/aerial-pr.sh submit " + tmpWorkspace + " 'add'", cwd: tmpWorkspace, untracked: true, wantDeny: true, wantReason: "GenericAPIKey"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cwd := tc.cwd
			if tc.useCoreDir {
				cwd = tmpCore
			}
			gitCmdRunner = func(dir string, args ...string) (string, error) {
				if tc.untracked && len(args) > 0 && args[0] == "ls-files" {
					return "secret.txt\n", nil
				}
				if tc.emptyTree && len(args) > 1 && args[0] == "diff" && args[1] == "HEAD" {
					return "", os.ErrNotExist
				}
				return tc.mockDiff, nil
			}

			dec := CheckCommit(tc.tool, ToolArgs{CommandLine: tc.cmd, Cwd: cwd})
			if tc.wantDeny && dec.Decision != DecisionDeny {
				t.Fatalf("expected deny, got %v", dec)
			}
			if !tc.wantDeny && dec.Decision != DecisionAllow {
				t.Fatalf("expected allow, got deny: %v", dec)
			}
			if tc.wantReason != "" && !strings.Contains(dec.Reason, tc.wantReason) {
				t.Fatalf("expected %q in reason, got %q", tc.wantReason, dec.Reason)
			}
		})
	}
}
