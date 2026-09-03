package gitsync

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test User",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test User",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed in %s: %v, output: %s", args, dir, err, string(out))
	}
	return string(out)
}

func setupGitRepos(t *testing.T) (originDir, repoADir, repoBDir string) {
	t.Helper()

	baseDir := t.TempDir()
	originDir = filepath.Join(baseDir, "origin.git")
	repoADir = filepath.Join(baseDir, "repoA")
	repoBDir = filepath.Join(baseDir, "repoB")

	// 1. Create bare origin
	if err := os.MkdirAll(originDir, 0755); err != nil {
		t.Fatalf("failed to create origin dir: %v", err)
	}
	runGitCmd(t, originDir, "init", "--bare")

	// 2. Clone to repoA
	runGitCmd(t, baseDir, "clone", originDir, "repoA")
	runGitCmd(t, repoADir, "config", "user.name", "Test User")
	runGitCmd(t, repoADir, "config", "user.email", "test@example.com")

	// 3. Initial commit in repoA and push
	testFile := filepath.Join(repoADir, "README.md")
	if err := os.WriteFile(testFile, []byte("# Initial commit\n"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	runGitCmd(t, repoADir, "add", "README.md")
	runGitCmd(t, repoADir, "commit", "-m", "Initial commit")
	runGitCmd(t, repoADir, "push", "origin", "HEAD")

	// 4. Clone to repoB
	runGitCmd(t, baseDir, "clone", originDir, "repoB")
	runGitCmd(t, repoBDir, "config", "user.name", "Test User")
	runGitCmd(t, repoBDir, "config", "user.email", "test@example.com")

	return originDir, repoADir, repoBDir
}

func TestSyncRepo_NonGit(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does_not_exist")
	hasChanges, err := SyncRepo(context.Background(), nonExistent)
	if err != nil {
		t.Errorf("Expected nil error for non-existent repo, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false for non-existent repo")
	}

	emptyDir := t.TempDir()
	hasChanges, err = SyncRepo(context.Background(), emptyDir)
	if err != nil {
		t.Errorf("Expected nil error for non-git dir, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false for non-git dir")
	}

	hasChanges, err = SyncRepo(context.Background(), "")
	if err != nil || hasChanges {
		t.Errorf("Expected false, nil for empty path")
	}
}

func TestSyncRepo_IndexLock(t *testing.T) {
	_, _, repoB := setupGitRepos(t)

	// Create index.lock in repoB
	lockPath := filepath.Join(repoB, ".git", "index.lock")
	if err := os.WriteFile(lockPath, []byte("locked"), 0644); err != nil {
		t.Fatalf("failed to write index.lock: %v", err)
	}

	hasChanges, err := SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Errorf("Expected nil error when index.lock exists, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false when index.lock exists")
	}

	// Remove index.lock and verify normal sync succeeds
	_ = os.Remove(lockPath)
	hasChanges, err = SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Errorf("Expected nil error after removing index.lock, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false since repoB is up to date")
	}
}

func TestSyncRepo_FastForward(t *testing.T) {
	_, repoA, repoB := setupGitRepos(t)

	// 1. Initial state: repoB is already at HEAD
	hasChanges, err := SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Fatalf("SyncRepo failed: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false initially")
	}

	// 2. Commit in repoA and push to origin
	f := filepath.Join(repoA, "new_file.txt")
	if err := os.WriteFile(f, []byte("new content"), 0644); err != nil {
		t.Fatalf("failed to write new_file.txt: %v", err)
	}
	runGitCmd(t, repoA, "add", "new_file.txt")
	runGitCmd(t, repoA, "commit", "-m", "add new_file.txt")
	runGitCmd(t, repoA, "push", "origin", "HEAD")

	// 3. SyncRepo on repoB should fast-forward and return hasChanges=true
	hasChanges, err = SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Fatalf("SyncRepo failed after remote push: %v", err)
	}
	if !hasChanges {
		t.Errorf("Expected hasChanges=true after remote push")
	}

	// Verify file pulled into repoB
	pulledFile := filepath.Join(repoB, "new_file.txt")
	if data, err := os.ReadFile(pulledFile); err != nil || string(data) != "new content" {
		t.Errorf("Expected pulled content 'new content', got data: %s, err: %v", string(data), err)
	}

	// 4. Subsequent sync without changes should return hasChanges=false
	hasChanges, err = SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Fatalf("Subsequent SyncRepo failed: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false for subsequent sync")
	}
}

func TestSyncRepo_NonFastForwardConflict(t *testing.T) {
	_, repoA, repoB := setupGitRepos(t)

	// Commit on repoA and push
	fA := filepath.Join(repoA, "README.md")
	if err := os.WriteFile(fA, []byte("# Changed on A\n"), 0644); err != nil {
		t.Fatalf("failed to write file on A: %v", err)
	}
	runGitCmd(t, repoA, "add", "README.md")
	runGitCmd(t, repoA, "commit", "-m", "changed on A")
	runGitCmd(t, repoA, "push", "origin", "HEAD")

	// Create a conflicting local commit on repoB without pulling
	fB := filepath.Join(repoB, "README.md")
	if err := os.WriteFile(fB, []byte("# Changed on B\n"), 0644); err != nil {
		t.Fatalf("failed to write file on B: %v", err)
	}
	runGitCmd(t, repoB, "add", "README.md")
	runGitCmd(t, repoB, "commit", "-m", "changed on B")

	// SyncRepo on repoB should fail --ff-only and return error gracefully without panic
	hasChanges, err := SyncRepo(context.Background(), repoB)
	if err == nil {
		t.Errorf("Expected error on non-fast-forward conflict, got nil")
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false on sync error")
	}
}

func TestStartPeriodicSync_LoopAndCancel(t *testing.T) {
	_, repoA, repoB := setupGitRepos(t)

	updateCh := make(chan string, 10)
	stop := StartPeriodicSync(
		context.Background(),
		50*time.Millisecond,
		[]string{repoB},
		func(repo string) {
			updateCh <- repo
		},
	)
	defer stop()

	// Commit on repoA and push
	f := filepath.Join(repoA, "periodic.txt")
	if err := os.WriteFile(f, []byte("periodic update"), 0644); err != nil {
		t.Fatalf("failed to write periodic.txt: %v", err)
	}
	runGitCmd(t, repoA, "add", "periodic.txt")
	runGitCmd(t, repoA, "commit", "-m", "periodic update commit")
	runGitCmd(t, repoA, "push", "origin", "HEAD")

	// Wait for callback notification
	select {
	case repo := <-updateCh:
		if repo != repoB {
			t.Errorf("Expected update for %s, got %s", repoB, repo)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for periodic sync update callback")
	}

	// Stop periodic sync
	stop()

	// Push another commit to repoA
	if err := os.WriteFile(f, []byte("another update"), 0644); err != nil {
		t.Fatalf("failed to write periodic.txt: %v", err)
	}
	runGitCmd(t, repoA, "add", "periodic.txt")
	runGitCmd(t, repoA, "commit", "-m", "second commit")
	runGitCmd(t, repoA, "push", "origin", "HEAD")

	// Ensure no more callbacks are received
	select {
	case repo := <-updateCh:
		t.Errorf("Received unexpected update after stop: %s", repo)
	case <-time.After(200 * time.Millisecond):
		// Clean stop verified
	}
}

func TestStartPeriodicSync_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := StartPeriodicSync(ctx, 10*time.Millisecond, []string{"/nonexistent"}, nil)

	cancel()
	time.Sleep(30 * time.Millisecond)
	stop()
}

func TestSyncRepo_Worktree_IndexLock(t *testing.T) {
	baseDir := t.TempDir()
	_, repoADir, _ := setupGitRepos(t)

	// Create a worktree of repoA
	wtDir := filepath.Join(baseDir, "repoA_wt")
	branch := strings.TrimSpace(runGitCmd(t, repoADir, "rev-parse", "--abbrev-ref", "HEAD"))
	runGitCmd(t, repoADir, "worktree", "add", "-b", "wt-branch", wtDir, "HEAD")
	runGitCmd(t, wtDir, "branch", "--set-upstream-to=origin/"+branch)

	// Verify that wtDir/.git is indeed a file containing gitdir:
	gitFile := filepath.Join(wtDir, ".git")
	data, err := os.ReadFile(gitFile)
	if err != nil {
		t.Fatalf("failed to read worktree .git file: %v", err)
	}
	content := string(data)
	if !strings.HasPrefix(content, "gitdir:") {
		t.Fatalf("expected worktree .git file to start with 'gitdir:', got: %s", content)
	}

	// Resolve the real gitdir
	realGitDir, err := resolveGitDir(wtDir)
	if err != nil {
		t.Fatalf("resolveGitDir failed: %v", err)
	}

	// Create index.lock in realGitDir
	lockPath := filepath.Join(realGitDir, "index.lock")
	if err := os.WriteFile(lockPath, []byte("locked"), 0644); err != nil {
		t.Fatalf("failed to write worktree index.lock: %v", err)
	}

	// SyncRepo on worktree should skip due to index.lock
	hasChanges, err := SyncRepo(context.Background(), wtDir)
	if err != nil {
		t.Errorf("Expected nil error when worktree index.lock exists, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false when worktree index.lock exists")
	}

	// Remove index.lock and verify normal sync
	_ = os.Remove(lockPath)
	hasChanges, err = SyncRepo(context.Background(), wtDir)
	if err != nil {
		t.Errorf("Expected nil error after removing worktree index.lock, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false as wt is up to date")
	}
}

func TestSyncRepo_SafeDirectory_Idempotent(t *testing.T) {
	_, repoA, _ := setupGitRepos(t)

	// Run SyncRepo multiple times
	for i := 0; i < 3; i++ {
		_, err := SyncRepo(context.Background(), repoA)
		if err != nil {
			t.Fatalf("SyncRepo iteration %d failed: %v", i, err)
		}
	}

	// Check git config --global --get-all safe.directory
	cmd := exec.Command("git", "config", "--global", "--get-all", "safe.directory")
	out, err := cmd.Output()
	if err == nil {
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		// Count occurrences of "*"
		starCount := 0
		for _, l := range lines {
			if strings.TrimSpace(l) == "*" {
				starCount++
			}
		}
		if starCount > 1 {
			t.Errorf("safe.directory '*' duplicated %d times in global config", starCount)
		}
	}
}

func TestSanitizeLog(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean log without tokens",
			input:    "git clone completed successfully in /share/aerial-config",
			expected: "git clone completed successfully in /share/aerial-config",
		},
		{
			name:     "classic github PAT (ghp_)",
			input:    "Error connecting with token ghp_1234567890abcdefABCDEF to repo",
			expected: "Error connecting with token [REDACTED_TOKEN] to repo",
		},
		{
			name:     "fine-grained PAT (github_pat_)",
			input:    "fatal: authentication failed for github_pat_11ABCD_0123456789_abcdef",
			expected: "fatal: authentication failed for [REDACTED_TOKEN]",
		},
		{
			name:     "x-access-token embedded in URL",
			input:    "fatal: unable to access 'https://x-access-token:ghp_secretToken123@github.com/org/repo.git/': 403",
			expected: "fatal: unable to access 'https://[REDACTED_TOKEN]@github.com/org/repo.git/': 403",
		},
		{
			name:     "basic authorization header",
			input:    "-c http.extraHeader=AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46Z2hwXzEyMzQ1",
			expected: "-c http.extraHeader=AUTHORIZATION: [REDACTED_TOKEN]",
		},
		{
			name:     "multiple tokens in string",
			input:    "ghp_tokenA failed, trying github_pat_tokenB with basic dGVzdDp0ZXN0",
			expected: "[REDACTED_TOKEN] failed, trying [REDACTED_TOKEN] with [REDACTED_TOKEN]",
		},
		{
			name:     "oauth tokens (gho_ and ghu_)",
			input:    "tokens gho_OAuthSecret123 and ghu_UserSecret456",
			expected: "tokens [REDACTED_TOKEN] and [REDACTED_TOKEN]",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := SanitizeLog(tc.input)
			if actual != tc.expected {
				t.Errorf("SanitizeLog(%q) =\n got:  %q\n want: %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestBuildAuthArgs(t *testing.T) {
	// Empty PAT
	if args := buildAuthArgs(""); len(args) != 0 {
		t.Errorf("expected empty slice for empty pat, got %v", args)
	}
	if args := buildAuthArgs("   "); len(args) != 0 {
		t.Errorf("expected empty slice for whitespace pat, got %v", args)
	}

	// Valid PAT
	pat := "ghp_test12345"
	args := buildAuthArgs(pat)
	if len(args) != 2 || args[0] != "-c" {
		t.Fatalf("unexpected auth args structure: %v", args)
	}
	expectedHeader := "http.extraHeader=AUTHORIZATION: basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+pat))
	if args[1] != expectedHeader {
		t.Errorf("expected header %q, got %q", expectedHeader, args[1])
	}
}

func TestEnsureRepo_EmptyInputs(t *testing.T) {
	if err := EnsureRepo(context.Background(), "", "https://github.com/example/repo.git", "pat"); err != nil {
		t.Errorf("expected nil error for empty repoPath, got %v", err)
	}
	if err := EnsureRepo(context.Background(), "/some/path", "", "pat"); err != nil {
		t.Errorf("expected nil error for empty repoUrl, got %v", err)
	}
}

func TestEnsureRepo_CloneNewPath(t *testing.T) {
	originDir, _, _ := setupGitRepos(t)

	targetDir := filepath.Join(t.TempDir(), "cloned_new")
	err := EnsureRepo(context.Background(), targetDir, originDir, "ghp_dummyToken")
	if err != nil {
		t.Fatalf("EnsureRepo failed for new path: %v", err)
	}

	// Verify README.md exists
	readmePath := filepath.Join(targetDir, "README.md")
	if data, err := os.ReadFile(readmePath); err != nil || !strings.Contains(string(data), "Initial commit") {
		t.Fatalf("README.md not properly cloned: data=%s, err=%v", string(data), err)
	}

	// Verify .git/config does not contain PAT
	gitConfigFile := filepath.Join(targetDir, ".git", "config")
	configData, err := os.ReadFile(gitConfigFile)
	if err != nil {
		t.Fatalf("failed to read .git/config: %v", err)
	}
	if strings.Contains(string(configData), "ghp_dummyToken") {
		t.Errorf("Plaintext token found in .git/config: %s", string(configData))
	}

	// Calling EnsureRepo again is a no-op / returns nil
	if err := EnsureRepo(context.Background(), targetDir, originDir, "ghp_dummyToken"); err != nil {
		t.Errorf("second EnsureRepo call failed: %v", err)
	}
}

func TestEnsureRepo_AdoptionNonEmptyDir(t *testing.T) {
	originDir, _, _ := setupGitRepos(t)

	adoptDir := filepath.Join(t.TempDir(), "adopt_existing")
	if err := os.MkdirAll(adoptDir, 0755); err != nil {
		t.Fatalf("failed to create adopt dir: %v", err)
	}

	// Create local pre-existing files
	localFile := filepath.Join(adoptDir, "AGENTS.md")
	if err := os.WriteFile(localFile, []byte("# Local Custom Agents\n"), 0644); err != nil {
		t.Fatalf("failed to create local file: %v", err)
	}
	customSkill := filepath.Join(adoptDir, "custom.txt")
	if err := os.WriteFile(customSkill, []byte("local skill"), 0644); err != nil {
		t.Fatalf("failed to create custom skill: %v", err)
	}

	err := EnsureRepo(context.Background(), adoptDir, originDir, "ghp_adoptToken")
	if err != nil {
		t.Fatalf("EnsureRepo adoption failed: %v", err)
	}

	// Check that local files are preserved intact
	if data, err := os.ReadFile(localFile); err != nil || string(data) != "# Local Custom Agents\n" {
		t.Errorf("Local AGENTS.md was corrupted or deleted: %s, %v", string(data), err)
	}
	if data, err := os.ReadFile(customSkill); err != nil || string(data) != "local skill" {
		t.Errorf("Local custom.txt was corrupted or deleted: %s, %v", string(data), err)
	}

	// Check that .git exists
	if _, err := os.Stat(filepath.Join(adoptDir, ".git")); err != nil {
		t.Fatalf("Adopted directory missing .git: %v", err)
	}

	// Verify .git/config does not contain PAT
	gitConfigFile := filepath.Join(adoptDir, ".git", "config")
	configData, err := os.ReadFile(gitConfigFile)
	if err != nil {
		t.Fatalf("failed to read .git/config: %v", err)
	}
	if strings.Contains(string(configData), "ghp_adoptToken") {
		t.Errorf("Plaintext token found in .git/config: %s", string(configData))
	}

	// Test that subsequent SyncRepo succeeds
	hasChanges, err := SyncRepo(context.Background(), adoptDir)
	if err != nil {
		t.Errorf("SyncRepo failed on adopted directory: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false on freshly adopted directory")
	}
}

func TestEnsureRepo_ZeroPlaintextTokenInvariant_WithEmbeddedURL(t *testing.T) {
	originDir, _, _ := setupGitRepos(t)

	// Even if someone passes a URL with embedded credentials:
	embeddedURL := "https://x-access-token:ghp_leakedSecret12345@github.com/azylman/aerial-config.git"
	cleaned := cleanURL(embeddedURL)
	if strings.Contains(cleaned, "ghp_leakedSecret12345") || strings.Contains(cleaned, "x-access-token") {
		t.Errorf("cleanURL failed to strip credentials: %s", cleaned)
	}
	if cleaned != "https://github.com/azylman/aerial-config.git" {
		t.Errorf("expected clean URL https://github.com/azylman/aerial-config.git, got %s", cleaned)
	}

	// Test EnsureRepo with originDir and PAT
	targetDir := filepath.Join(t.TempDir(), "zero_token_check")
	if err := EnsureRepo(context.Background(), targetDir, originDir, "ghp_secretTokenDiskCheck"); err != nil {
		t.Fatalf("EnsureRepo failed: %v", err)
	}

	configBytes, err := os.ReadFile(filepath.Join(targetDir, ".git", "config"))
	if err != nil {
		t.Fatalf("failed to read .git/config: %v", err)
	}
	configStr := string(configBytes)
	if strings.Contains(configStr, "ghp_secretTokenDiskCheck") {
		t.Fatalf("FATAL: Token leaked into .git/config: %s", configStr)
	}
}

func TestSyncRepo_AuthenticatedPull(t *testing.T) {
	_, repoA, repoB := setupGitRepos(t)

	// Set GITHUB_PAT env var
	t.Setenv("GITHUB_PAT", "ghp_mockSyncPat123")

	// Commit on repoA and push
	f := filepath.Join(repoA, "auth_sync.txt")
	if err := os.WriteFile(f, []byte("authenticated sync"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	runGitCmd(t, repoA, "add", "auth_sync.txt")
	runGitCmd(t, repoA, "commit", "-m", "authenticated commit")
	runGitCmd(t, repoA, "push", "origin", "HEAD")

	// SyncRepo on repoB should pull with auth args
	hasChanges, err := SyncRepo(context.Background(), repoB)
	if err != nil {
		t.Fatalf("SyncRepo with GITHUB_PAT failed: %v", err)
	}
	if !hasChanges {
		t.Errorf("Expected hasChanges=true after authenticated pull")
	}

	// Verify content
	pulledFile := filepath.Join(repoB, "auth_sync.txt")
	data, err := os.ReadFile(pulledFile)
	if err != nil || string(data) != "authenticated sync" {
		t.Errorf("Expected 'authenticated sync', got %q, err: %v", string(data), err)
	}
}

func TestEnsureGitHooks(t *testing.T) {
	_, repoA, _ := setupGitRepos(t)

	// 1. Repo without .githooks - should return nil without setting core.hooksPath
	if err := EnsureGitHooks(context.Background(), repoA); err != nil {
		t.Fatalf("EnsureGitHooks on repo without .githooks failed: %v", err)
	}

	// 2. Create .githooks dir and pre-push hook
	hooksDir := filepath.Join(repoA, ".githooks")
	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		t.Fatalf("Failed to create .githooks dir: %v", err)
	}
	hookScript := filepath.Join(hooksDir, "pre-push")
	if err := os.WriteFile(hookScript, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("Failed to write hook script: %v", err)
	}

	// 3. EnsureGitHooks should configure core.hooksPath
	if err := EnsureGitHooks(context.Background(), repoA); err != nil {
		t.Fatalf("EnsureGitHooks on repo with .githooks failed: %v", err)
	}

	// Verify git config value
	val := runGitCmd(t, repoA, "config", "--get", "core.hooksPath")
	if strings.TrimSpace(val) != ".githooks" {
		t.Errorf("Expected core.hooksPath to be '.githooks', got %q", val)
	}

	// 4. Empty path
	if err := EnsureGitHooks(context.Background(), ""); err != nil {
		t.Errorf("Expected nil error for empty repoPath, got %v", err)
	}
}



