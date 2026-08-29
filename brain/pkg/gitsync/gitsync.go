package gitsync

import (
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SyncMutex is exported so runner or agent turns can coordinate with gitsync if needed.
var SyncMutex sync.Mutex

// resolveGitDir checks if repoPath contains a .git directory or a .git file (e.g., in a git worktree or submodule).
// If it is a .git file containing "gitdir: <path>", it resolves and returns the target git directory.
func resolveGitDir(repoPath string) (string, error) {
	gitPath := filepath.Join(repoPath, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return gitPath, nil
	}

	// If .git is a file (e.g. worktree or submodule), parse gitdir: <path>
	data, err := os.ReadFile(gitPath)
	if err != nil {
		return "", err
	}
	content := strings.TrimSpace(string(data))
	const prefix = "gitdir:"
	if strings.HasPrefix(content, prefix) {
		target := strings.TrimSpace(content[len(prefix):])
		if !filepath.IsAbs(target) {
			target = filepath.Join(repoPath, target)
		}
		return filepath.Clean(target), nil
	}

	return gitPath, nil
}

// SyncRepo checks if the specified repository has git tracking, skips if index.lock is active,
// locks SyncMutex, and performs a fast-forward git pull.
// Returns (hasChanges bool, err error).
func SyncRepo(ctx context.Context, repoPath string) (bool, error) {
	if repoPath == "" {
		return false, nil
	}

	// 1. Check if repoPath exists and resolve the .git directory (handles directories, worktrees, and submodules)
	gitDir, err := resolveGitDir(repoPath)
	if err != nil {
		return false, nil
	}

	// 2. Check for index.lock -> if exists, skip cycle to avoid collisions
	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[GitSync] %s has index.lock present, skipping sync cycle", repoPath)
		return false, nil
	}

	// 3. Acquire SyncMutex
	SyncMutex.Lock()
	defer SyncMutex.Unlock()

	// Unified bounded timeout for all git subprocesses in this sync operation
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 4. Configure safe.directory to prevent dubious ownership issues across host/container UIDs
	cmdSafe := exec.CommandContext(opCtx, "git", "-C", repoPath, "config", "--global", "safe.directory", "*")
	_ = cmdSafe.Run()

	// 5. Rev-parse HEAD before pull
	cmdBefore := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	outBefore, err := cmdBefore.Output()
	if err != nil {
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD before pull for %s: %v", repoPath, err)
		return false, err
	}
	headBefore := strings.TrimSpace(string(outBefore))

	// 6. Pull with --ff-only and GIT_TERMINAL_PROMPT=0
	cmdPull := exec.CommandContext(opCtx, "git", "-C", repoPath, "pull", "--ff-only")
	cmdPull.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	outPull, err := cmdPull.CombinedOutput()
	if err != nil {
		log.Printf("[GitSync] Warning: failed to pull %s: %v, output: %s", repoPath, err, strings.TrimSpace(string(outPull)))
		return false, err
	}

	// 7. Rev-parse HEAD after pull
	cmdAfter := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	outAfter, err := cmdAfter.Output()
	if err != nil {
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD after pull for %s: %v", repoPath, err)
		return false, err
	}
	headAfter := strings.TrimSpace(string(outAfter))

	// 8. Determine if changes occurred
	if headBefore != headAfter {
		log.Printf("[GitSync] Repository %s updated: %s -> %s", repoPath, headBefore, headAfter)
		return true, nil
	}

	return false, nil
}

// StartPeriodicSync starts a periodic sync loop in a background goroutine for the specified repositories.
// If interval <= 0, it defaults to 60 seconds.
// For each repository in repos, calls SyncRepo; if changes are detected, onUpdate(repo) is invoked.
// Returns a stop function that cancels the background worker cleanly.
func StartPeriodicSync(ctx context.Context, interval time.Duration, repos []string, onUpdate func(repo string)) func() {
	if interval <= 0 {
		interval = 60 * time.Second
	}

	syncCtx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-syncCtx.Done():
				return
			case <-ticker.C:
				for _, repo := range repos {
					hasChanges, err := SyncRepo(syncCtx, repo)
					if err == nil && hasChanges && onUpdate != nil {
						onUpdate(repo)
					}
				}
			}
		}
	}()

	return cancel
}
