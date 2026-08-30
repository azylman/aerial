package gitsync

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// SyncMutex is exported so runner or agent turns can coordinate with gitsync if needed.
var SyncMutex sync.Mutex

var sanitizePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)basic\s+[a-zA-Z0-9+/=]+`),
	regexp.MustCompile(`(?i)x-access-token:[^@\s]+`),
	regexp.MustCompile(`(?i)github_pat_[a-zA-Z0-9_]+`),
	regexp.MustCompile(`(?i)ghp_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)gho_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)ghu_[a-zA-Z0-9]+`),
}

// SanitizeLog uses regex to replace any personal access tokens or auth headers with [REDACTED_TOKEN].
func SanitizeLog(input string) string {
	out := input
	for _, re := range sanitizePatterns {
		out = re.ReplaceAllString(out, "[REDACTED_TOKEN]")
	}
	return out
}

// buildAuthArgs returns git command-line arguments to inject HTTP basic auth headers
// for GitHub personal access tokens without writing them to .git/config on disk.
func buildAuthArgs(pat string) []string {
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return []string{}
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
	return []string{"-c", fmt.Sprintf("http.extraHeader=AUTHORIZATION: basic %s", encoded)}
}

// cleanURL removes embedded userinfo/credentials from a URL to maintain zero plaintext token on disk.
func cleanURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") || strings.HasPrefix(rawURL, "ssh://") || strings.HasPrefix(rawURL, "git://") {
		if u, err := url.Parse(rawURL); err == nil {
			u.User = nil
			return u.String()
		}
	}
	return rawURL
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

// EnsureRepo ensures repoPath is a valid git repository tracked against repoUrl.
// It uses a non-destructive directory adoption protocol if repoPath already has local files.
// Token authentication is injected ephemerally per git execution, preserving the Zero Plaintext Token Invariant on disk.
func EnsureRepo(ctx context.Context, repoPath, repoUrl, pat string) error {
	if repoUrl == "" || repoPath == "" {
		return nil
	}

	cleanRepoUrl := cleanURL(repoUrl)

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

	// Configure safe.directory to prevent ownership conflicts
	cmdSafe := exec.CommandContext(ctx, "git", "config", "--global", "safe.directory", "*")
	_ = cmdSafe.Run()

	entries, readErr := os.ReadDir(repoPath)
	isEmptyOrNotExist := readErr != nil || len(entries) == 0

	authArgs := buildAuthArgs(pat)

	if isEmptyOrNotExist {
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
			return errors.New(SanitizeLog(err.Error()))
		}

		args := append([]string{}, authArgs...)
		args = append(args, "clone", cleanRepoUrl, repoPath)

		cmdClone := exec.CommandContext(ctx, "git", args...)
		cmdClone.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		outClone, err := cmdClone.CombinedOutput()
		if err != nil {
			sanitizedOut := SanitizeLog(strings.TrimSpace(string(outClone)))
			if sanitizedOut == "" {
				sanitizedOut = SanitizeLog(err.Error())
			}
			return fmt.Errorf("git clone failed for %s: %s", repoPath, sanitizedOut)
		}
		return nil
	}

	// Non-Destructive Directory Adoption Protocol:
	// 1. git -C <repoPath> init
	cmdInit := exec.CommandContext(ctx, "git", "-C", repoPath, "init")
	outInit, err := cmdInit.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git init failed for %s: %s", repoPath, SanitizeLog(strings.TrimSpace(string(outInit))))
	}

	// 2. git config --global safe.directory "*"
	cmdSafeAdopt := exec.CommandContext(ctx, "git", "config", "--global", "safe.directory", "*")
	_ = cmdSafeAdopt.Run()

	// 3. git -C <repoPath> remote add origin <cleanRepoUrl>
	cmdRemote := exec.CommandContext(ctx, "git", "-C", repoPath, "remote", "add", "origin", cleanRepoUrl)
	if outRemote, err := cmdRemote.CombinedOutput(); err != nil {
		cmdSet := exec.CommandContext(ctx, "git", "-C", repoPath, "remote", "set-url", "origin", cleanRepoUrl)
		if outSet, errSet := cmdSet.CombinedOutput(); errSet != nil {
			return fmt.Errorf("git remote add/set-url failed for %s: %s", repoPath, SanitizeLog(strings.TrimSpace(string(outSet)+" "+string(outRemote))))
		}
	}

	// 4. git -C <repoPath> <authArgs> fetch origin main
	fetchArgs := append([]string{"-C", repoPath}, authArgs...)
	fetchArgs = append(fetchArgs, "fetch", "origin", "main")
	cmdFetch := exec.CommandContext(ctx, "git", fetchArgs...)
	cmdFetch.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if outFetch, err := cmdFetch.CombinedOutput(); err != nil {
		// Fallback: try fetching origin without branch specification
		fetchArgsFallback := append([]string{"-C", repoPath}, authArgs...)
		fetchArgsFallback = append(fetchArgsFallback, "fetch", "origin")
		cmdFetchFB := exec.CommandContext(ctx, "git", fetchArgsFallback...)
		cmdFetchFB.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if outFB, errFB := cmdFetchFB.CombinedOutput(); errFB != nil {
			return fmt.Errorf("git fetch failed for %s: %s", repoPath, SanitizeLog(strings.TrimSpace(string(outFB)+" "+string(outFetch))))
		}
	}

	// Detect target remote branch (main vs master)
	targetBranch := "main"
	if err := exec.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "--verify", "origin/main").Run(); err != nil {
		if errMaster := exec.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "--verify", "origin/master").Run(); errMaster == nil {
			targetBranch = "master"
		}
	}

	// 5. git -C <repoPath> reset --soft origin/main (or origin/<targetBranch>)
	cmdBranch := exec.CommandContext(ctx, "git", "-C", repoPath, "branch", "-M", targetBranch)
	_ = cmdBranch.Run()

	cmdReset := exec.CommandContext(ctx, "git", "-C", repoPath, "reset", "--soft", "origin/"+targetBranch)
	if outReset, err := cmdReset.CombinedOutput(); err != nil {
		cmdResetFH := exec.CommandContext(ctx, "git", "-C", repoPath, "reset", "--soft", "FETCH_HEAD")
		if outFH, errFH := cmdResetFH.CombinedOutput(); errFH != nil {
			return fmt.Errorf("git reset --soft failed for %s: %s", repoPath, SanitizeLog(strings.TrimSpace(string(outFH)+" "+string(outReset))))
		}
	}

	cmdTrack := exec.CommandContext(ctx, "git", "-C", repoPath, "branch", "-u", "origin/"+targetBranch, targetBranch)
	_ = cmdTrack.Run()

	return nil
}

// SyncRepo checks if the specified repository has git tracking, skips if index.lock is active,
// locks SyncMutex, and performs a fast-forward git pull with optional token authentication.
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
	cmdSafe := exec.CommandContext(opCtx, "git", "config", "--global", "safe.directory", "*")
	_ = cmdSafe.Run()

	// 5. Rev-parse HEAD before pull
	cmdBefore := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	outBefore, err := cmdBefore.Output()
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, sanitizedErr)
		return false, errors.New(sanitizedErr)
	}
	headBefore := strings.TrimSpace(string(outBefore))

	// 6. Pull with --ff-only, auth args if GITHUB_PAT is set, and GIT_TERMINAL_PROMPT=0
	pat := os.Getenv("GITHUB_PAT")
	authArgs := buildAuthArgs(pat)
	pullArgs := append([]string{"-C", repoPath}, authArgs...)
	pullArgs = append(pullArgs, "pull", "--ff-only")

	cmdPull := exec.CommandContext(opCtx, "git", pullArgs...)
	cmdPull.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	outPull, err := cmdPull.CombinedOutput()
	if err != nil {
		sanitizedOut := SanitizeLog(strings.TrimSpace(string(outPull)))
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to pull %s: %s, output: %s", repoPath, sanitizedErr, sanitizedOut)
		return false, fmt.Errorf("git pull failed: %s", sanitizedOut)
	}

	// 7. Rev-parse HEAD after pull
	cmdAfter := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	outAfter, err := cmdAfter.Output()
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD after pull for %s: %s", repoPath, sanitizedErr)
		return false, errors.New(sanitizedErr)
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

// SyncComposeOverride synchronizes docker-compose.override.yml from configDir to projectDir via symlink (or copy fallback).
// If the source override file in configDir is removed, it automatically cleans up the stale symlink.
func SyncComposeOverride(configDir, projectDir string) error {
	if configDir == "" || projectDir == "" {
		return nil
	}

	target := filepath.Join(projectDir, "docker-compose.override.yml")

	sourceYml := filepath.Join(configDir, "docker-compose.override.yml")
	sourceYaml := filepath.Join(configDir, "docker-compose.override.yaml")

	var sourcePath string
	if _, err := os.Stat(sourceYml); err == nil {
		sourcePath = sourceYml
	} else if _, err := os.Stat(sourceYaml); err == nil {
		sourcePath = sourceYaml
	}

	if sourcePath != "" {
		// Ensure target directory exists
		if err := os.MkdirAll(projectDir, 0755); err != nil {
			return fmt.Errorf("failed to create project directory %s: %w", projectDir, err)
		}

		// Check if target already exists as a symlink or file
		if fi, err := os.Lstat(target); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				if link, readErr := os.Readlink(target); readErr == nil && (link == sourcePath || filepath.Clean(link) == filepath.Clean(sourcePath)) {
					// Already correctly symlinked to sourcePath
					return nil
				}
			}
			// Remove existing target (dead symlink, old symlink, or regular file)
			_ = os.Remove(target)
		}

		// Create symlink target -> source
		if err := os.Symlink(sourcePath, target); err != nil {
			// Fallback to copy if symlink fails across filesystems or due to OS permissions
			data, readErr := os.ReadFile(sourcePath)
			if readErr != nil {
				return fmt.Errorf("failed to read source compose override %s: %w", sourcePath, readErr)
			}
			if writeErr := os.WriteFile(target, data, 0644); writeErr != nil {
				return fmt.Errorf("failed to copy compose override to %s: %w", target, writeErr)
			}
		}

		log.Printf("[Compose-Override] Synchronized docker-compose.override.yml from %s to %s", configDir, projectDir)
		return nil
	}

	// Source does not exist: cleanup obsolete symlink if target is a symlink pointing to source
	if fi, err := os.Lstat(target); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			link, readErr := os.Readlink(target)
			if readErr == nil {
				cleanLink := filepath.Clean(link)
				if cleanLink == filepath.Clean(sourceYml) || cleanLink == filepath.Clean(sourceYaml) || strings.HasPrefix(cleanLink, filepath.Clean(configDir)) {
					if err := os.Remove(target); err != nil {
						return fmt.Errorf("failed to remove obsolete compose override: %w", err)
					}
					log.Printf("[Compose-Override] Cleaned up obsolete docker-compose.override.yml")
				}
			}
		}
	}

	return nil
}
