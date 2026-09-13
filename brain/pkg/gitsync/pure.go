package gitsync

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// GitExecutor defines an abstract execution signature for Git CLI commands.
// stdout and stderr are separated, and errors must be sanitized.
type GitExecutor func(ctx context.Context, dir string, args ...string) (stdout []byte, stderr []byte, err error)

// CleanURL removes embedded userinfo/credentials from a URL to maintain zero plaintext token on disk.
func CleanURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") || strings.HasPrefix(rawURL, "ssh://") || strings.HasPrefix(rawURL, "git://") {
		if u, err := url.Parse(rawURL); err == nil {
			u.User = nil
			return u.String()
		}
	}
	return rawURL
}

// BuildAuthArgs returns git command-line arguments to inject HTTP basic auth headers
// for GitHub personal access tokens without writing them to .git/config on disk.
func BuildAuthArgs(pat string) []string {
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return []string{}
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
	return []string{"-c", fmt.Sprintf("http.extraHeader=AUTHORIZATION: basic %s", encoded)}
}

// ParseGitDirContent inspects the raw bytes of a .git file (e.g. in a worktree or submodule)
// and extracts the target directory referenced by the gitdir: prefix.
// Handles CRLF, quotes, whitespace, and resolves relative paths against repoPath.
func ParseGitDirContent(data []byte, repoPath string) (string, bool) {
	content := strings.TrimSpace(string(data))
	const prefix = "gitdir:"
	if !strings.HasPrefix(content, prefix) {
		return "", false
	}
	target := strings.TrimSpace(content[len(prefix):])
	target = strings.Trim(target, "\"'")
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(repoPath, target)
	}
	return filepath.Clean(target), true
}

// BuildCloneArgs formats arguments for git clone, ensuring the URL is sanitized
// and ephemeral auth arguments are injected before the clone subcommand.
func BuildCloneArgs(repoUrl, repoPath, pat string) []string {
	cleanRepoUrl := CleanURL(repoUrl)
	args := append([]string{}, BuildAuthArgs(pat)...)
	args = append(args, "clone", cleanRepoUrl, repoPath)
	return args
}

// BuildFetchArgs formats arguments for git fetch, injecting ephemeral auth arguments
// and optionally targeting a specific remote branch.
func BuildFetchArgs(pat, remote, branch string) []string {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		remote = "origin"
	}
	args := append([]string{}, BuildAuthArgs(pat)...)
	args = append(args, "fetch", remote)
	branch = strings.TrimSpace(branch)
	if branch != "" {
		args = append(args, branch)
	}
	return args
}

// BuildPullArgs formats arguments for git pull --ff-only with ephemeral auth arguments.
func BuildPullArgs(pat string) []string {
	args := append([]string{}, BuildAuthArgs(pat)...)
	args = append(args, "pull", "--ff-only")
	return args
}

// BuildResetSoftArgs formats arguments for git reset --soft against a target ref (e.g. origin/main or FETCH_HEAD).
func BuildResetSoftArgs(ref string) []string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = "FETCH_HEAD"
	}
	return []string{"reset", "--soft", ref}
}

// BuildSafeDirectoryArgs formats arguments to configure global safe.directory wildcard.
func BuildSafeDirectoryArgs() []string {
	return []string{"config", "--global", "safe.directory", "*"}
}

// BuildHooksPathArgs formats arguments to configure core.hooksPath.
func BuildHooksPathArgs(hooksDir string) []string {
	hooksDir = strings.TrimSpace(hooksDir)
	if hooksDir == "" {
		hooksDir = ".githooks"
	}
	return []string{"config", "core.hooksPath", hooksDir}
}

// BuildBranchRenameArgs formats arguments for git branch -M <branch>.
func BuildBranchRenameArgs(branch string) []string {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		branch = "main"
	}
	return []string{"branch", "-M", branch}
}

// BuildBranchTrackArgs formats arguments for git branch -u <remote>/<branch> <branch>.
func BuildBranchTrackArgs(remote, branch string) []string {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		remote = "origin"
	}
	branch = strings.TrimSpace(branch)
	if branch == "" {
		branch = "main"
	}
	return []string{"branch", "-u", remote + "/" + branch, branch}
}

// BuildRevParseVerifyArgs formats arguments for git rev-parse --verify <ref>.
func BuildRevParseVerifyArgs(ref string) []string {
	ref = strings.TrimSpace(ref)
	return []string{"rev-parse", "--verify", ref}
}

// BuildGitEnv constructs process environment variables for Git subprocesses.
// Appends GIT_TERMINAL_PROMPT=0 if not already set, preserving base environment.
func BuildGitEnv(baseEnv ...[]string) []string {
	var env []string
	if len(baseEnv) > 0 && baseEnv[0] != nil {
		env = append([]string{}, baseEnv[0]...)
	}
	hasTerminalPrompt := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_TERMINAL_PROMPT=") {
			hasTerminalPrompt = true
			break
		}
	}
	if !hasTerminalPrompt {
		env = append(env, "GIT_TERMINAL_PROMPT=0")
	}
	return env
}

// ResolveGitBin locates the git executable across platforms with Windows MinGit support.
func ResolveGitBin(lookup func(string) string, fileExists ...func(string) bool) string {
	return resolveGitBinInternal(lookup, fileExists, exec.LookPath, runtime.GOOS)
}

func resolveGitBinInternal(
	lookup func(string) string,
	fileExists []func(string) bool,
	lookPath func(string) (string, error),
	goos string,
) string {
	exists := func(path string) bool {
		if len(fileExists) > 0 && fileExists[0] != nil {
			return fileExists[0](path)
		}
		fi, err := os.Stat(path)
		return err == nil && !fi.IsDir()
	}

	if lookPath != nil {
		if p, err := lookPath("git"); err == nil && p != "" {
			return p
		}
	}

	if lookup == nil {
		return "git"
	}

	if goos == "windows" || lookup("OS") == "Windows_NT" {
		sep := string(filepath.Separator)
		if goos == "windows" {
			sep = "\\"
		}
		localAppData := lookup("LOCALAPPDATA")
		if localAppData != "" {
			minGit := strings.Join([]string{localAppData, "Programs", "MinGit", "cmd", "git.exe"}, sep)
			if exists(minGit) {
				return filepath.Clean(minGit)
			}
		}
		progFiles := lookup("ProgramFiles")
		if progFiles != "" {
			pfg := strings.Join([]string{progFiles, "Git", "cmd", "git.exe"}, sep)
			if exists(pfg) {
				return filepath.Clean(pfg)
			}
		}
		progFilesX86 := lookup("ProgramFiles(x86)")
		if progFilesX86 != "" {
			pfg86 := strings.Join([]string{progFilesX86, "Git", "cmd", "git.exe"}, sep)
			if exists(pfg86) {
				return filepath.Clean(pfg86)
			}
		}
		userProfile := lookup("USERPROFILE")
		if userProfile != "" {
			upGit := strings.Join([]string{userProfile, "AppData", "Local", "Programs", "MinGit", "cmd", "git.exe"}, sep)
			if exists(upGit) {
				return filepath.Clean(upGit)
			}
		}
	}

	return "git"
}
