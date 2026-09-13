package gitsync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)

func TestSyncRepo_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		GitHubPAT: "ghp_mock_token_for_test",
	})
	ctx := context.Background()
	_, _ = SyncRepo(ctx, t.TempDir(), cfg)
}

func TestSyncRepo_NonGit(t *testing.T) {
	nonExistent := filepath.Join(t.TempDir(), "does_not_exist")
	hasChanges, err := SyncRepo(context.Background(), nonExistent, nil)
	if err != nil {
		t.Errorf("Expected nil error for non-existent repo, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false for non-existent repo")
	}

	emptyDir := t.TempDir()
	hasChanges, err = SyncRepo(context.Background(), emptyDir, nil)
	if err != nil {
		t.Errorf("Expected nil error for non-git dir, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false for non-git dir")
	}

	hasChanges, err = SyncRepo(context.Background(), "", nil)
	if err != nil || hasChanges {
		t.Errorf("Expected false, nil for empty path")
	}
}

func TestSyncRepo_IndexLock(t *testing.T) {
	dir := t.TempDir()
	gitDir := filepath.Join(dir, ".git")
	if err := os.MkdirAll(gitDir, 0755); err != nil {
		t.Fatalf("failed to create .git dir: %v", err)
	}

	lockPath := filepath.Join(gitDir, "index.lock")
	if err := os.WriteFile(lockPath, []byte("locked"), 0644); err != nil {
		t.Fatalf("failed to write index.lock: %v", err)
	}

	hasChanges, err := SyncRepo(context.Background(), dir, nil)
	if err != nil {
		t.Errorf("Expected nil error when index.lock exists, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false when index.lock exists")
	}

	_ = os.Remove(lockPath)
	SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte("sha1"), nil, nil
		}
		return nil, nil, nil
	})

	hasChanges, err = SyncRepo(context.Background(), dir, nil)
	if err != nil {
		t.Errorf("Expected nil error after removing index.lock, got: %v", err)
	}
	if hasChanges {
		t.Errorf("Expected hasChanges=false since repo is up to date")
	}
}

func TestSyncRepo_FastForward(t *testing.T) {
	t.Run("up to date repo with no changes", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		type gitCall struct {
			dir  string
			args []string
		}
		var calls []gitCall
		var mu sync.Mutex

		SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			calls = append(calls, gitCall{dir: d, args: args})
			mu.Unlock()

			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("sha_identical\n"), nil, nil
			}
			return nil, nil, nil
		})

		cfg := config.NewFromData(&config.ConfigData{GitHubPAT: "ghp_secret_token_123"})
		hasChanges, err := SyncRepo(context.Background(), dir, cfg)
		if err != nil {
			t.Fatalf("SyncRepo failed: %v", err)
		}
		if hasChanges {
			t.Errorf("expected hasChanges=false for identical commit SHAs")
		}

		mu.Lock()
		defer mu.Unlock()

		// Verify exact 4-step sequence
		if len(calls) != 4 {
			t.Fatalf("expected exactly 4 git calls, got %d: %+v", len(calls), calls)
		}

		// Step 1: safe.directory (global scope, dir == "")
		if calls[0].dir != "" {
			t.Errorf("step 1 dir expected empty, got %q", calls[0].dir)
		}
		expectedSafeArgs := BuildSafeDirectoryArgs()
		if strings.Join(calls[0].args, " ") != strings.Join(expectedSafeArgs, " ") {
			t.Errorf("step 1 args mismatch: got %v, want %v", calls[0].args, expectedSafeArgs)
		}

		// Step 2: rev-parse HEAD before (dir == dir)
		if calls[1].dir != dir {
			t.Errorf("step 2 dir expected %q, got %q", dir, calls[1].dir)
		}
		if strings.Join(calls[1].args, " ") != "rev-parse HEAD" {
			t.Errorf("step 2 args mismatch: got %v", calls[1].args)
		}

		// Step 3: pull with auth header (dir == dir)
		if calls[2].dir != dir {
			t.Errorf("step 3 dir expected %q, got %q", dir, calls[2].dir)
		}
		// Verify Zero Plaintext Token Invariant
		argsStr := strings.Join(calls[2].args, " ")
		if strings.Contains(argsStr, "ghp_secret_token_123") {
			t.Fatalf("CRITICAL SECURITY VIOLATION: raw PAT leaked in git arguments: %s", argsStr)
		}
		if !strings.Contains(argsStr, "http.extraHeader=AUTHORIZATION: basic ") {
			t.Errorf("step 3 expected basic auth header, got %s", argsStr)
		}
		if !strings.HasSuffix(argsStr, "pull --ff-only") {
			t.Errorf("step 3 expected suffix 'pull --ff-only', got %s", argsStr)
		}

		// Step 4: rev-parse HEAD after (dir == dir)
		if calls[3].dir != dir {
			t.Errorf("step 4 dir expected %q, got %q", dir, calls[3].dir)
		}
		if strings.Join(calls[3].args, " ") != "rev-parse HEAD" {
			t.Errorf("step 4 args mismatch: got %v", calls[3].args)
		}
	})

	t.Run("fast-forward remote commit change detected", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		revCount := 0
		SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				revCount++
				if revCount == 1 {
					return []byte("  commit_sha_before\r\n"), nil, nil
				}
				return []byte("  commit_sha_after\n"), nil, nil
			}
			return nil, nil, nil
		})

		hasChanges, err := SyncRepo(context.Background(), dir, nil)
		if err != nil {
			t.Fatalf("SyncRepo failed: %v", err)
		}
		if !hasChanges {
			t.Errorf("expected hasChanges=true when commit changes from before to after")
		}
	})

	t.Run("pull error sanitizes token in error output", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("sha1"), nil, nil
			}
			if len(args) > 0 && args[0] == "-c" {
				return nil, []byte("fatal: authentication failed for token ghp_leaked_token_xyz"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		cfg := config.NewFromData(&config.ConfigData{GitHubPAT: "ghp_leaked_token_xyz"})
		_, err := SyncRepo(context.Background(), dir, cfg)
		if err == nil {
			t.Fatalf("expected error on pull failure, got nil")
		}
		if strings.Contains(err.Error(), "ghp_leaked_token_xyz") {
			t.Fatalf("expected token to be redacted from error output, got: %v", err)
		}
		if !strings.Contains(err.Error(), "[REDACTED_TOKEN]") {
			t.Fatalf("expected [REDACTED_TOKEN] in error output, got: %v", err)
		}
	})
}

func TestStartPeriodicSync_LoopAndCancel(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

	var updateCount int
	var mu sync.Mutex
	onUpdate := func(repo string) {
		mu.Lock()
		defer mu.Unlock()
		updateCount++
	}

	currentSHA := "sha1"
	shouldUpdate := false
	SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(args) > 0 && args[0] == "pull" && shouldUpdate {
			currentSHA = "sha2"
		}
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte(currentSHA), nil, nil
		}
		return nil, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopSync := StartPeriodicSync(ctx, 10*time.Millisecond, []string{dir}, nil, onUpdate)
	defer stopSync()

	time.Sleep(25 * time.Millisecond)
	mu.Lock()
	initialCount := updateCount
	mu.Unlock()
	if initialCount != 0 {
		t.Errorf("Expected 0 updates before remote change, got %d", initialCount)
	}

	// Trigger remote change
	mu.Lock()
	shouldUpdate = true
	mu.Unlock()

	deadline := time.Now().Add(500 * time.Millisecond)
	updated := false
	for time.Now().Before(deadline) {
		mu.Lock()
		if updateCount > 0 {
			updated = true
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	if !updated {
		t.Errorf("Expected onUpdate callback to be called after change")
	}

	stopSync()
	mu.Lock()
	countAtStop := updateCount
	// Trigger another change after stopSync
	currentSHA = "sha3"
	mu.Unlock()

	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	finalCount := updateCount
	mu.Unlock()

	if finalCount != countAtStop {
		t.Errorf("Expected updates to stop after calling stopSync(), count went from %d to %d", countAtStop, finalCount)
	}
}

func TestStartPeriodicSync_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopSync := StartPeriodicSync(ctx, 100*time.Millisecond, []string{"/mock/repo"}, nil, nil)
	defer stopSync()

	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestStartPeriodicSync_ZeroInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopSync := StartPeriodicSync(ctx, 0, []string{"/mock/repo"}, nil, nil)
	defer stopSync()
	time.Sleep(20 * time.Millisecond)
}

func TestSyncRepo_SafeDirectory_Idempotent(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

	safeDirCallCount := 0
	var mu sync.Mutex

	SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
		mu.Lock()
		defer mu.Unlock()

		expected := BuildSafeDirectoryArgs()
		if len(args) == len(expected) && strings.Join(args, " ") == strings.Join(expected, " ") {
			if d != "" {
				t.Errorf("expected safe.directory to be executed with empty dir (global scope), got %q", d)
			}
			safeDirCallCount++
		}
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte("sha1"), nil, nil
		}
		return nil, nil, nil
	})

	for i := 0; i < 3; i++ {
		_, err := SyncRepo(context.Background(), dir, nil)
		if err != nil {
			t.Fatalf("SyncRepo iteration %d failed: %v", i, err)
		}
	}

	mu.Lock()
	count := safeDirCallCount
	mu.Unlock()

	if count != 3 {
		t.Errorf("Expected safe.directory to be called 3 times (once per iteration), got %d", count)
	}
}

func TestEnsureRepo_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	if err := EnsureRepo(ctx, "", "https://github.com/test/repo.git", ""); err != nil {
		t.Errorf("Expected nil error for empty repoPath")
	}
	if err := EnsureRepo(ctx, "/path", "", ""); err != nil {
		t.Errorf("Expected nil error for empty repoUrl")
	}
}

func TestEnsureRepo_EmptyArgs(t *testing.T) {
	ctx := context.Background()
	if err := EnsureRepo(ctx, "", "", ""); err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestEnsureRepo_AlreadyValid(t *testing.T) {
	dir := t.TempDir()
	gitDir := filepath.Join(dir, ".git")
	_ = os.MkdirAll(gitDir, 0755)

	err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/aerial.git", "")
	if err != nil {
		t.Fatalf("Expected nil error for already valid repo, got: %v", err)
	}
}

func TestEnsureRepo_MkdirAllError(t *testing.T) {
	SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, nil, nil
	})
	nonDirFile := filepath.Join(t.TempDir(), "blocker_file")
	_ = os.WriteFile(nonDirFile, []byte("file content"), 0644)
	targetDir := filepath.Join(nonDirFile, "target_repo")

	err := EnsureRepo(context.Background(), targetDir, "https://github.com/azylman/aerial.git", "")
	if err == nil {
		t.Errorf("Expected error when parent directory cannot be created, got nil")
	}
}

func TestSyncRepo_CorruptedGitDir(t *testing.T) {
	SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "rev-parse" {
			return nil, []byte("fatal: not a git repository"), errors.New("exit status 128")
		}
		return nil, nil, nil
	})
	dir := t.TempDir()
	gitDir := filepath.Join(dir, ".git")
	_ = os.MkdirAll(gitDir, 0755)

	_, err := SyncRepo(context.Background(), dir, nil)
	if err == nil {
		t.Errorf("Expected error for git directory with missing refs/HEAD, got nil")
	}
}

func TestEnsureGitHooks(t *testing.T) {
	var invokedArgs []string
	var invokedDir string
	var mu sync.Mutex

	SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		mu.Lock()
		defer mu.Unlock()
		invokedDir = dir
		invokedArgs = args
		return nil, nil, nil
	})

	// Case 1: empty repoPath -> early return nil, executor not called
	if err := EnsureGitHooks(context.Background(), ""); err != nil {
		t.Errorf("Expected nil error for empty repoPath")
	}
	mu.Lock()
	if len(invokedArgs) != 0 {
		t.Errorf("Expected no git commands for empty repoPath")
	}
	mu.Unlock()

	// Case 2: directory without .githooks -> executor not called
	dir := t.TempDir()
	if err := EnsureGitHooks(context.Background(), dir); err != nil {
		t.Errorf("Expected nil error when .githooks does not exist")
	}
	mu.Lock()
	if len(invokedArgs) != 0 {
		t.Errorf("Expected no git commands when .githooks does not exist")
	}
	mu.Unlock()

	// Case 3: directory with .githooks directory -> executor called with BuildHooksPathArgs
	hooksDir := filepath.Join(dir, ".githooks")
	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		t.Fatalf("failed to create .githooks dir: %v", err)
	}

	if err := EnsureGitHooks(context.Background(), dir); err != nil {
		t.Fatalf("EnsureGitHooks failed with .githooks present: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if invokedDir != dir {
		t.Errorf("Expected invokedDir to be %q, got %q", dir, invokedDir)
	}
	expectedArgs := BuildHooksPathArgs(".githooks")
	if len(invokedArgs) != len(expectedArgs) {
		t.Fatalf("Expected %v args, got %v", expectedArgs, invokedArgs)
	}
	for i := range expectedArgs {
		if invokedArgs[i] != expectedArgs[i] {
			t.Errorf("Expected arg %d to be %q, got %q", i, expectedArgs[i], invokedArgs[i])
		}
	}
}

// In-memory GitExecutor Mock Test Suites (0.00s each)

func TestEnsureRepo_MockScenarios(t *testing.T) {
	t.Run("clone error with combined stderr output", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "clone" {
				return nil, []byte("fatal: repository not found"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := filepath.Join(t.TempDir(), "nonexistent_sub")
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/nonexistent.git", "")
		if err == nil || !strings.Contains(err.Error(), "fatal: repository not found") {
			t.Fatalf("expected clone error with stderr output, got: %v", err)
		}
	})

	t.Run("clone error with empty output falls back to error string", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "clone" {
				return nil, nil, errors.New("network timeout")
			}
			return nil, nil, nil
		})

		dir := filepath.Join(t.TempDir(), "empty_sub")
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err == nil || !strings.Contains(err.Error(), "network timeout") {
			t.Fatalf("expected fallback to err.Error(), got: %v", err)
		}
	})

	t.Run("adoption init failure", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "init" {
				return nil, []byte("permission denied"), errors.New("exit status 1")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err == nil || !strings.Contains(err.Error(), "git init failed") {
			t.Fatalf("expected init failure error, got: %v", err)
		}
	})

	t.Run("adoption remote add failure and set-url success", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 3 && args[0] == "remote" && args[1] == "add" {
				return nil, []byte("fatal: remote origin already exists"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err != nil {
			t.Fatalf("expected successful remote set-url fallback, got error: %v", err)
		}
	})

	t.Run("adoption remote add failure and set-url failure", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 3 && args[0] == "remote" {
				return nil, []byte("remote command failed"), errors.New("exit status 1")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err == nil || !strings.Contains(err.Error(), "git remote add/set-url failed") {
			t.Fatalf("expected remote set-url failure error, got: %v", err)
		}
	})

	t.Run("adoption fetch main failure and fallback origin fetch success", func(t *testing.T) {
		fetchCount := 0
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "fetch" {
				fetchCount++
				if fetchCount == 1 {
					return nil, []byte("fatal: couldn't find remote ref main"), errors.New("exit status 128")
				}
				return []byte("fetch success"), nil, nil
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err != nil {
			t.Fatalf("expected successful fetch fallback, got: %v", err)
		}
		if fetchCount != 2 {
			t.Fatalf("expected 2 fetch attempts, got: %d", fetchCount)
		}
	})

	t.Run("adoption fetch main failure and fallback origin fetch failure", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "fetch" {
				return nil, []byte("fatal: unable to connect"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err == nil || !strings.Contains(err.Error(), "git fetch failed") {
			t.Fatalf("expected fetch failure error, got: %v", err)
		}
	})

	t.Run("adoption master branch verification and reset", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 3 && args[0] == "rev-parse" && args[1] == "--verify" {
				if args[2] == "origin/main" {
					return nil, nil, errors.New("main not found")
				}
				if args[2] == "origin/master" {
					return []byte("deadbeef123"), nil, nil
				}
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err != nil {
			t.Fatalf("expected adoption with master branch to succeed, got: %v", err)
		}
	})

	t.Run("adoption reset branch failure and FETCH_HEAD fallback failure", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "reset" {
				return nil, []byte("fatal: corrupt ref"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
		if err == nil || !strings.Contains(err.Error(), "git reset --soft failed") {
			t.Fatalf("expected reset soft failure, got: %v", err)
		}
	})

	t.Run("double check under lock branch", func(t *testing.T) {
		dir := t.TempDir()
		firstCheck := true
		SetGitExecutorForTest(t, func(ctx context.Context, d string, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		})

		_ = os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644)
		origResolve := resolveGitDir
		_ = origResolve

		// When EnsureRepo acquires lock, simulate that repo was adopted by another thread
		var lockAcquired bool
		go func() {
			SyncMutex.Lock()
			lockAcquired = true
			_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)
			time.Sleep(50 * time.Millisecond)
			SyncMutex.Unlock()
		}()

		time.Sleep(10 * time.Millisecond)
		if lockAcquired {
			err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/test.git", "")
			if err != nil {
				t.Fatalf("expected nil error on double-check under lock, got: %v", err)
			}
		}
		_ = firstCheck
	})
}

func TestSyncRepo_MockScenarios(t *testing.T) {
	t.Run("rev-parse before pull failure", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return nil, []byte("fatal: ambiguous argument 'HEAD'"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		hasChanges, err := SyncRepo(context.Background(), dir, nil)
		if err == nil || !strings.Contains(err.Error(), "fatal: ambiguous argument 'HEAD'") {
			t.Fatalf("expected rev-parse error, got: %v", err)
		}
		if hasChanges {
			t.Errorf("expected hasChanges=false on rev-parse error")
		}
	})

	t.Run("pull error with combined output", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("sha1"), nil, nil
			}
			if len(args) > 0 && args[0] == "pull" {
				return nil, []byte("fatal: Not possible to fast-forward, aborting."), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		hasChanges, err := SyncRepo(context.Background(), dir, nil)
		if err == nil || !strings.Contains(err.Error(), "git pull failed") {
			t.Fatalf("expected pull error, got: %v", err)
		}
		if hasChanges {
			t.Errorf("expected hasChanges=false on pull error")
		}
	})

	t.Run("rev-parse after pull failure", func(t *testing.T) {
		revCount := 0
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				revCount++
				if revCount == 1 {
					return []byte("sha1"), nil, nil
				}
				return nil, []byte("fatal: corrupt ref"), errors.New("exit status 128")
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		hasChanges, err := SyncRepo(context.Background(), dir, nil)
		if err == nil || !strings.Contains(err.Error(), "fatal: corrupt ref") {
			t.Fatalf("expected rev-parse after pull error, got: %v", err)
		}
		if hasChanges {
			t.Errorf("expected hasChanges=false on rev-parse after pull error")
		}
	})

	t.Run("pull detects changes when commit changes", func(t *testing.T) {
		revCount := 0
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				revCount++
				if revCount == 1 {
					return []byte("sha1_old"), nil, nil
				}
				return []byte("sha1_new"), nil, nil
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		cfg := config.NewFromData(&config.ConfigData{GitHubPAT: "ghp_mock"})
		hasChanges, err := SyncRepo(context.Background(), dir, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasChanges {
			t.Errorf("expected hasChanges=true when commit changed")
		}
	})

	t.Run("pull detects no changes when commit identical", func(t *testing.T) {
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("sha1_same"), nil, nil
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

		hasChanges, err := SyncRepo(context.Background(), dir, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasChanges {
			t.Errorf("expected hasChanges=false when commit identical")
		}
	})
}

func TestEnsureGitHooks_MockScenarios(t *testing.T) {
	t.Run("githooks is regular file not directory", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, ".githooks"), []byte("regular file"), 0644)

		called := false
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			called = true
			return nil, nil, nil
		})

		err := EnsureGitHooks(context.Background(), dir)
		if err != nil {
			t.Fatalf("expected nil error when .githooks is regular file, got: %v", err)
		}
		if called {
			t.Errorf("expected GitExecutor not to be called when .githooks is not a directory")
		}
	})

	t.Run("git config command failure", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, ".githooks"), 0755)

		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("error: could not lock config file"), errors.New("exit status 1")
		})

		err := EnsureGitHooks(context.Background(), dir)
		if err == nil || !strings.Contains(err.Error(), "failed to configure git core.hooksPath") {
			t.Fatalf("expected config failure error, got: %v", err)
		}
	})
}

func TestDefaultGitExecutor_Execution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()

	// Test valid command via defaultGitExecutor with directory
	stdout, stderr, err := defaultGitExecutor(ctx, dir, "version")
	if err == nil {
		if !strings.Contains(string(stdout), "git version") && !strings.Contains(string(stderr), "git version") {
			t.Logf("git version output: %s %s", string(stdout), string(stderr))
		}
	}

	// Test valid command with empty directory (exercises dir == "" branch in defaultGitExecutor)
	_, _, _ = defaultGitExecutor(ctx, "", "version")

	// Test invalid command via defaultGitExecutor
	_, _, err = defaultGitExecutor(ctx, dir, "invalid-git-subcommand-xyz-123")
	if err == nil {
		t.Errorf("expected error for invalid git subcommand")
	}
}

func TestResolveGitDir_WorktreeAndErrors(t *testing.T) {
	t.Run("worktree file with relative path", func(t *testing.T) {
		dir := t.TempDir()
		gitFile := filepath.Join(dir, ".git")
		worktreeTarget := filepath.Join(dir, "target_gitdir")
		_ = os.MkdirAll(worktreeTarget, 0755)

		relTarget := "target_gitdir"
		_ = os.WriteFile(gitFile, []byte("gitdir: "+relTarget+"\n"), 0644)

		resolved, err := resolveGitDir(dir)
		if err != nil {
			t.Fatalf("resolveGitDir failed: %v", err)
		}
		expected := filepath.Clean(worktreeTarget)
		if filepath.Clean(resolved) != expected {
			t.Fatalf("resolveGitDir returned %q, want %q", resolved, expected)
		}
	})

	t.Run("worktree file with invalid content falls back to gitPath", func(t *testing.T) {
		dir := t.TempDir()
		gitFile := filepath.Join(dir, ".git")
		_ = os.WriteFile(gitFile, []byte("invalid content not starting with gitdir"), 0644)

		resolved, err := resolveGitDir(dir)
		if err != nil {
			t.Fatalf("resolveGitDir failed: %v", err)
		}
		if filepath.Clean(resolved) != filepath.Clean(gitFile) {
			t.Fatalf("resolveGitDir returned %q, want %q", resolved, gitFile)
		}
	})

	t.Run("missing git dir returns stat error", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nonexistent")
		_, err := resolveGitDir(dir)
		if err == nil {
			t.Fatalf("expected error for nonexistent directory, got nil")
		}
	})
}

func TestEnsureRepo_AdditionalMockScenarios(t *testing.T) {
	t.Run("successful clone into empty directory", func(t *testing.T) {
		var cloneArgs []string
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			for _, arg := range args {
				if arg == "clone" {
					cloneArgs = args
					return []byte("Cloning into..."), nil, nil
				}
			}
			return nil, nil, nil
		})

		dir := filepath.Join(t.TempDir(), "empty_repo")
		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/aerial.git", "ghp_mock")
		if err != nil {
			t.Fatalf("expected nil error on successful clone, got: %v", err)
		}
		if len(cloneArgs) == 0 {
			t.Fatalf("expected git clone to be called")
		}
	})

	t.Run("soft reset fallback to FETCH_HEAD succeeds when origin/branch fails", func(t *testing.T) {
		resetCount := 0
		SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "reset" {
				resetCount++
				if resetCount == 1 {
					return nil, []byte("fatal: ambiguous ref origin/main"), errors.New("exit status 128")
				}
				return []byte("reset FETCH_HEAD success"), nil, nil
			}
			return nil, nil, nil
		})

		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, "local_file.txt"), []byte("data"), 0644)

		err := EnsureRepo(context.Background(), dir, "https://github.com/azylman/aerial.git", "")
		if err != nil {
			t.Fatalf("expected nil error on FETCH_HEAD fallback reset, got: %v", err)
		}
		if resetCount != 2 {
			t.Fatalf("expected 2 reset attempts, got: %d", resetCount)
		}
	})
}

func TestBackwardCompatibilityShims(t *testing.T) {
	authArgs := buildAuthArgs("test_token")
	expectedAuth := BuildAuthArgs("test_token")
	if strings.Join(authArgs, " ") != strings.Join(expectedAuth, " ") {
		t.Errorf("buildAuthArgs mismatch: got %v, want %v", authArgs, expectedAuth)
	}

	rawURL := "  https://github.com/test.git  "
	cleaned := cleanURL(rawURL)
	expectedURL := CleanURL(rawURL)
	if cleaned != expectedURL {
		t.Errorf("cleanURL mismatch: got %q, want %q", cleaned, expectedURL)
	}
}

func TestSyncRepo_EmptyErrorOutput(t *testing.T) {
	SetGitExecutorForTest(t, func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte("sha1"), nil, nil
		}
		if len(args) > 0 && args[0] == "pull" {
			return []byte(""), []byte(""), errors.New("aborted")
		}
		return nil, nil, nil
	})

	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, ".git"), 0755)

	hasChanges, err := SyncRepo(context.Background(), dir, nil)
	if err == nil {
		t.Fatalf("expected error on empty output pull failure, got nil")
	}
	if hasChanges {
		t.Errorf("expected hasChanges=false on error")
	}
}

