package gitsync

import (
	"context"
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

