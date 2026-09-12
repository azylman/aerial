package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// ConflictContainer holds metadata for a container identified as a Docker Compose conflict.
type ConflictContainer struct {
	ID     string
	Names  string
	Status string
}

// FilterConflictContainers parses tab-delimited docker ps output (ID\tNames\tStatus)
// and returns non-running conflict containers left behind by aborted Docker Compose recreations.
func FilterConflictContainers(dockerPsOutput, selfHostname string) []ConflictContainer {
	var result []ConflictContainer
	scanner := bufio.NewScanner(strings.NewReader(dockerPsOutput))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 3 {
			continue
		}
		id := strings.TrimSpace(parts[0])
		names := strings.TrimSpace(parts[1])
		status := strings.TrimSpace(parts[2])

		if id == "" {
			continue
		}

		shortID := id
		if len(shortID) > 12 {
			shortID = shortID[:12]
		}
		if len(shortID) < 12 {
			continue
		}

		// Ensure shortID is valid hex
		isHex := true
		for i := 0; i < len(shortID); i++ {
			c := shortID[i]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				isHex = false
				break
			}
		}
		if !isHex {
			continue
		}

		// Self-preservation: do not touch self
		if selfHostname != "" && (strings.EqualFold(id, selfHostname) || strings.EqualFold(shortID, selfHostname)) {
			continue
		}

		// State gate: only remove containers in created, exited, or dead states.
		statusLower := strings.ToLower(status)
		isNonRunning := strings.HasPrefix(statusLower, "created") ||
			strings.HasPrefix(statusLower, "exited") ||
			strings.HasPrefix(statusLower, "dead")
		if !isNonRunning {
			continue
		}

		// Cryptographic prefix check:
		// Docker Compose renames old containers before recreating them to: <short_id>_<service_name>.
		nameList := strings.Split(names, ",")
		expectedPrefix := strings.ToLower(shortID) + "_"
		isConflict := false

		for _, name := range nameList {
			cleanName := strings.ToLower(strings.TrimLeft(strings.TrimSpace(name), "/"))
			if strings.HasPrefix(cleanName, expectedPrefix) {
				isConflict = true
				break
			}
		}

		if isConflict {
			result = append(result, ConflictContainer{
				ID:     id,
				Names:  names,
				Status: status,
			})
		}
	}

	return result
}

// ParseComposeServices parses newline-delimited compose service names, trims whitespace,
// deduplicates entries, and filters out the gitsync sidecar service (case-insensitive).
func ParseComposeServices(output string) []string {
	lines := strings.Split(output, "\n")
	seen := make(map[string]struct{}, len(lines))
	targets := make([]string, 0, len(lines))

	for _, line := range lines {
		svc := strings.TrimSpace(line)
		svc = strings.TrimRight(svc, "\r")
		if svc == "" || strings.EqualFold(svc, "gitsync") {
			continue
		}
		if _, exists := seen[svc]; !exists {
			seen[svc] = struct{}{}
			targets = append(targets, svc)
		}
	}
	return targets
}

var composeFileTargets = []string{
	"docker-compose.yml",
	"docker-compose.yaml",
	"docker-compose.override.yml",
	"docker-compose.override.yaml",
	"compose.yaml",
	"compose.yml",
	"compose.override.yaml",
	"compose.override.yml",
	".env",
	".env.example",
}

// IsComposeFile returns true if the given path or filename corresponds to a compose or environment configuration file.
func IsComposeFile(path string) bool {
	base := filepath.Base(strings.TrimSpace(path))
	for _, target := range composeFileTargets {
		if strings.EqualFold(base, target) {
			return true
		}
	}
	return false
}

// FilterComposeChanges filters a list of changed file paths and returns only those that affect compose configuration.
func FilterComposeChanges(changedFiles []string) []string {
	var result []string
	for _, f := range changedFiles {
		if IsComposeFile(f) {
			result = append(result, f)
		}
	}
	return result
}

// BuildDiscordAlertContent constructs markdown content for automated GitOps rollback alerts.
func BuildDiscordAlertContent(title, repoPath, faultyCommit, rolledBackTo, stage, errorMsg string) string {
	sanitizedErr := strings.TrimSpace(errorMsg)
	if len(sanitizedErr) > 1200 {
		sanitizedErr = sanitizedErr[:1200] + "\n[... truncated for length]"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🚨 **GitSync GitOps Rollback Alert: %s**\n\n", strings.TrimSpace(title)))
	if repoPath != "" {
		sb.WriteString(fmt.Sprintf("**Repository:** `%s`\n", filepath.Clean(repoPath)))
	}
	if stage != "" {
		sb.WriteString(fmt.Sprintf("**Stage:** `%s`\n", stage))
	}
	if faultyCommit != "" {
		sb.WriteString(fmt.Sprintf("**Faulty Commit:** `%s`\n", faultyCommit))
	}
	if rolledBackTo != "" {
		sb.WriteString(fmt.Sprintf("**Rolled Back To:** `%s`\n", rolledBackTo))
	}
	if sanitizedErr != "" {
		sb.WriteString(fmt.Sprintf("\n**Error:**\n```\n%s\n```\n", sanitizedErr))
	}
	sb.WriteString("*Faulty commit quarantined to prevent sync loops. Newer commits to origin/main will sync normally.*")

	return sb.String()
}

// CalculateSyncStatus evaluates repository statuses and determines the overall daemon status.
// Precedence: "error" > "quarantined" > "lagging" > "synced".
func CalculateSyncStatus(repos map[string]RepoStatus) string {
	hasError := false
	hasQuarantine := false
	hasLag := false

	for _, st := range repos {
		if st.SyncStatus == "error" || st.Error != "" {
			hasError = true
		}
		if st.SyncStatus == "quarantined" || st.Quarantined {
			hasQuarantine = true
		}
		if st.SyncStatus == "lagging" {
			hasLag = true
		}
	}

	if hasError {
		return "error"
	}
	if hasQuarantine {
		return "quarantined"
	}
	if hasLag {
		return "lagging"
	}
	return "synced"
}

// ScrubComposeEnv filters out container-internal path overrides (AERIAL_CONFIG_DIR, AERIAL_PROJECT_DIR)
// so docker compose does not inherit container filesystem paths for host volume mounts.
func ScrubComposeEnv(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, env := range environ {
		eq := strings.IndexByte(env, '=')
		if eq == -1 {
			continue
		}
		key := env[:eq]
		if strings.EqualFold(key, "AERIAL_CONFIG_DIR") || strings.EqualFold(key, "AERIAL_PROJECT_DIR") {
			continue
		}
		out = append(out, env)
	}
	return out
}

// BuildGitEnv constructs environment variables for git subprocess execution with authentication and prompt suppression.
func BuildGitEnv(pat string, environ []string) []string {
	env := make([]string, 0, len(environ)+6)
	env = append(env, environ...)
	env = append(env, "GIT_TERMINAL_PROMPT=0")

	pat = strings.TrimSpace(pat)
	if pat == "" {
		return env
	}

	encoded := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
	cleanEncoded := strings.ReplaceAll(strings.ReplaceAll(encoded, "\r", ""), "\n", "")

	return append(env,
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=http.extraHeader",
		"GIT_CONFIG_VALUE_0="+fmt.Sprintf("AUTHORIZATION: basic %s", cleanEncoded),
		"GIT_CONFIG_KEY_1=http.version",
		"GIT_CONFIG_VALUE_1=HTTP/1.1",
	)
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

	if goos == "windows" || lookup("OS") == "Windows_NT" {
		sep := string(filepath.Separator)
		if goos == "windows" {
			sep = "\\"
		}
		localAppData := lookup("LOCALAPPDATA")
		if localAppData != "" {
			minGit := strings.Join([]string{localAppData, "Programs", "MinGit", "cmd", "git.exe"}, sep)
			if exists(minGit) {
				return minGit
			}
		}

		progFiles := lookup("ProgramFiles")
		if progFiles != "" {
			gitExe := strings.Join([]string{progFiles, "Git", "cmd", "git.exe"}, sep)
			if exists(gitExe) {
				return gitExe
			}
		}

		userProfile := lookup("USERPROFILE")
		if userProfile != "" {
			minGitUser := strings.Join([]string{userProfile, "AppData", "Local", "Programs", "MinGit", "cmd", "git.exe"}, sep)
			if exists(minGitUser) {
				return minGitUser
			}
		}
	}

	return "git"
}
