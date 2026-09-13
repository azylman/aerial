package gitsync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
)

// SyncMutex is exported so runner or agent turns can coordinate with gitsync if needed.
var SyncMutex sync.Mutex

// SanitizeLog uses regex to replace any personal access tokens or auth headers with [REDACTED_TOKEN].
func SanitizeLog(input string) string {
	return sanitizer.SanitizeLog(input)
}

// buildAuthArgs forwards to BuildAuthArgs for backward compatibility.
func buildAuthArgs(pat string) []string {
	return BuildAuthArgs(pat)
}

// cleanURL forwards to CleanURL for backward compatibility.
func cleanURL(rawURL string) string {
	return CleanURL(rawURL)
}

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
	if target, ok := ParseGitDirContent(data, repoPath); ok {
		return target, nil
	}

	return gitPath, nil
}

var defaultGitExecutor GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
	gitBin := ResolveGitBin(os.Getenv)
	cmd := exec.CommandContext(ctx, gitBin, args...)
	if dir != "" {
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
			cmd.Dir = dir
		}
	}
	cmd.Env = BuildGitEnv(os.Environ())
	cmd.WaitDelay = 5 * time.Second

	var stdoutBuf, stderrBuf strings.Builder
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	stdout := []byte(stdoutBuf.String())
	stderr := []byte(stderrBuf.String())

	if err != nil {
		return stdout, stderr, errors.New(SanitizeLog(err.Error()))
	}
	return stdout, stderr, nil
}

var (
	gitExecutorMu sync.RWMutex
	gitExecutor   = defaultGitExecutor
)

func getGitExecutor() GitExecutor {
	gitExecutorMu.RLock()
	defer gitExecutorMu.RUnlock()
	return gitExecutor
}

// SetGitExecutorForTest swaps gitExecutor for testing and restores default on cleanup.
func SetGitExecutorForTest(t *testing.T, mock GitExecutor) {
	gitExecutorMu.Lock()
	orig := gitExecutor
	gitExecutor = mock
	gitExecutorMu.Unlock()
	t.Cleanup(func() {
		gitExecutorMu.Lock()
		gitExecutor = orig
		gitExecutorMu.Unlock()
	})
}

// EnsureRepo ensures repoPath is a valid git repository tracked against repoUrl.
// It uses a non-destructive directory adoption protocol if repoPath already has local files.
// Token authentication is injected ephemerally per git execution, preserving the Zero Plaintext Token Invariant on disk.
func EnsureRepo(ctx context.Context, repoPath, repoUrl, pat string) error {
	if repoUrl == "" || repoPath == "" {
		return nil
	}

	cleanRepoUrl := CleanURL(repoUrl)

	// If already a valid git repository, nothing to do
	if _, err := resolveGitDir(repoPath); err == nil {
		return nil
	}

	SyncMutex.Lock()
	defer SyncMutex.Unlock()

	// Double check after acquiring lock
	if _, err := resolveGitDir(repoPath); err == nil {
		return nil
	}

	execGit := getGitExecutor()

	// Configure safe.directory to prevent ownership conflicts
	_, _, _ = execGit(ctx, "", BuildSafeDirectoryArgs()...)

	entries, readErr := os.ReadDir(repoPath)
	isEmptyOrNotExist := readErr != nil || len(entries) == 0

	if isEmptyOrNotExist {
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
			return errors.New(SanitizeLog(err.Error()))
		}

		cloneArgs := BuildCloneArgs(cleanRepoUrl, repoPath, pat)
		outClone, errClone, err := execGit(ctx, "", cloneArgs...)
		if err != nil {
			combined := SanitizeLog(strings.TrimSpace(string(outClone) + " " + string(errClone)))
			if combined == "" {
				combined = SanitizeLog(err.Error())
			}
			return fmt.Errorf("git clone failed for %s: %s", repoPath, combined)
		}
		return nil
	}

	// Non-Destructive Directory Adoption Protocol:
	// 1. git init
	outInit, errInit, err := execGit(ctx, repoPath, "init")
	if err != nil {
		combined := SanitizeLog(strings.TrimSpace(string(outInit) + " " + string(errInit)))
		return fmt.Errorf("git init failed for %s: %s", repoPath, combined)
	}

	// 2. git config --global safe.directory "*"
	_, _, _ = execGit(ctx, "", BuildSafeDirectoryArgs()...)

	// 3. git remote add origin <cleanRepoUrl>
	outRemote, errRemote, err := execGit(ctx, repoPath, "remote", "add", "origin", cleanRepoUrl)
	if err != nil {
		outSet, errSetOut, errSet := execGit(ctx, repoPath, "remote", "set-url", "origin", cleanRepoUrl)
		if errSet != nil {
			combined := SanitizeLog(strings.TrimSpace(string(outSet) + " " + string(errSetOut) + " " + string(outRemote) + " " + string(errRemote) + " " + errSet.Error()))
			return fmt.Errorf("git remote add/set-url failed for %s: %s", repoPath, combined)
		}
	}

	// 4. git fetch origin main
	outFetch, errFetch, err := execGit(ctx, repoPath, BuildFetchArgs(pat, "origin", "main")...)
	if err != nil {
		outFB, errFB, errFBEx := execGit(ctx, repoPath, BuildFetchArgs(pat, "origin", "")...)
		if errFBEx != nil {
			combined := SanitizeLog(strings.TrimSpace(string(outFB) + " " + string(errFB) + " " + string(outFetch) + " " + string(errFetch)))
			return fmt.Errorf("git fetch failed for %s: %s", repoPath, combined)
		}
	}

	// Detect target remote branch (main vs master)
	targetBranch := "main"
	if _, _, err := execGit(ctx, repoPath, BuildRevParseVerifyArgs("origin/main")...); err != nil {
		if _, _, errMaster := execGit(ctx, repoPath, BuildRevParseVerifyArgs("origin/master")...); errMaster == nil {
			targetBranch = "master"
		}
	}

	// 5. git branch -M targetBranch & reset --soft
	_, _, _ = execGit(ctx, repoPath, BuildBranchRenameArgs(targetBranch)...)

	outReset, errReset, err := execGit(ctx, repoPath, BuildResetSoftArgs("origin/"+targetBranch)...)
	if err != nil {
		outFH, errFH, errFHEx := execGit(ctx, repoPath, BuildResetSoftArgs("FETCH_HEAD")...)
		if errFHEx != nil {
			combined := SanitizeLog(strings.TrimSpace(string(outFH) + " " + string(errFH) + " " + string(outReset) + " " + string(errReset)))
			return fmt.Errorf("git reset --soft failed for %s: %s", repoPath, combined)
		}
	}

	_, _, _ = execGit(ctx, repoPath, BuildBranchTrackArgs("origin", targetBranch)...)
	return nil
}

// SyncRepo checks if the specified repository has git tracking, skips if index.lock is active,
// locks SyncMutex, and performs a fast-forward git pull with optional token authentication.
// Returns (hasChanges bool, err error).
func SyncRepo(ctx context.Context, repoPath string, cfg *config.Config) (bool, error) {
	if repoPath == "" {
		return false, nil
	}

	// 1. Check if repoPath exists and resolve the .git directory
	gitDir, err := resolveGitDir(repoPath)
	if err != nil {
		return false, nil
	}

	// 2. Acquire SyncMutex
	SyncMutex.Lock()
	defer SyncMutex.Unlock()

	// 3. Check for index.lock inside mutex -> if exists, skip cycle
	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[GitSync] %s has index.lock present, skipping sync cycle", repoPath)
		return false, nil
	}

	// Unified bounded timeout for all git subprocesses in this sync operation
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	execGit := getGitExecutor()

	// 4. Configure safe.directory
	_, _, _ = execGit(opCtx, "", BuildSafeDirectoryArgs()...)

	// 5. Rev-parse HEAD before pull
	outBefore, errBefore, err := execGit(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		combined := SanitizeLog(strings.TrimSpace(string(outBefore) + " " + string(errBefore) + " " + err.Error()))
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, combined)
		return false, errors.New(combined)
	}
	headBefore := strings.TrimSpace(string(outBefore))

	// 6. Pull with --ff-only and auth args
	var pat string
	if cfg != nil {
		cur := cfg.Current()
		if cur != nil {
			pat = cur.GitHubPAT
		}
	}
	pullArgs := BuildPullArgs(pat)

	outPull, errPull, err := execGit(opCtx, repoPath, pullArgs...)
	if err != nil {
		sanitizedOut := SanitizeLog(strings.TrimSpace(string(outPull) + " " + string(errPull)))
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to pull %s: %s, output: %s", repoPath, sanitizedErr, sanitizedOut)
		return false, fmt.Errorf("git pull failed: %s", sanitizedOut)
	}

	// 7. Rev-parse HEAD after pull
	outAfter, errAfter, err := execGit(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		combined := SanitizeLog(strings.TrimSpace(string(outAfter) + " " + string(errAfter) + " " + err.Error()))
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD after pull for %s: %s", repoPath, combined)
		return false, errors.New(combined)
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
func StartPeriodicSync(ctx context.Context, interval time.Duration, repos []string, cfg *config.Config, onUpdate func(repo string)) func() {
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
					hasChanges, err := SyncRepo(syncCtx, repo, cfg)
					if err == nil && hasChanges && onUpdate != nil {
						onUpdate(repo)
					}
				}
			}
		}
	}()

	return cancel
}

// EnsureGitHooks checks if repoPath contains a .githooks directory, and if so, configures git core.hooksPath.
func EnsureGitHooks(ctx context.Context, repoPath string) error {
	if repoPath == "" {
		return nil
	}
	hooksDir := filepath.Join(repoPath, ".githooks")
	if fi, err := os.Stat(hooksDir); err == nil && fi.IsDir() {
		execGit := getGitExecutor()
		out, errOut, err := execGit(ctx, repoPath, BuildHooksPathArgs(".githooks")...)
		if err != nil {
			combined := SanitizeLog(strings.TrimSpace(string(out) + " " + string(errOut)))
			return fmt.Errorf("failed to configure git core.hooksPath for %s: %w (output: %s)", repoPath, err, combined)
		}
	}
	return nil
}
