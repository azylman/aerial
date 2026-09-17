package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
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
		if svc == "" || strings.EqualFold(svc, "gitsync") || strings.EqualFold(svc, "hangar") {
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
func IsComposeFile(p string) bool {
	cleanPath := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	base := path.Base(cleanPath)
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

var allowedExactKeys = map[string]struct{}{
	"path":            {},
	"home":            {},
	"user":            {},
	"tmpdir":          {},
	"temp":            {},
	"tmp":             {},
	"hostname":        {},
	"lang":            {},
	"lc_all":          {},
	"lc_ctype":        {},
	"xdg_runtime_dir": {},
	"ci":              {},
	"http_proxy":      {},
	"https_proxy":     {},
	"no_proxy":        {},
	"all_proxy":       {},
	"ssl_cert_file":   {},
	"ssl_cert_dir":    {},
	"systemroot":      {},
	"systemdrive":     {},
	"comspec":         {},
	"pathext":         {},
}

var allowedPrefixes = []string{
	"docker_",
	"compose_",
}

func isAllowedEnvKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	if _, ok := allowedExactKeys[lower]; ok {
		return true
	}
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

// ScrubComposeEnv filters environment variables for docker compose execution,
// strictly allowlisting engine, system, proxy, and Docker CLI essentials
// to prevent container process environment variables from shadowing host .env values.
func ScrubComposeEnv(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, env := range environ {
		eq := strings.IndexByte(env, '=')
		if eq == -1 {
			continue
		}
		key := env[:eq]
		if isAllowedEnvKey(key) {
			out = append(out, env)
		}
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

// GenerateDockerConfig formats or updates a Docker config.json file with authentication
// credentials for a target registry (defaulting to ghcr.io). Preserves any existing
// non-conflicting registries and top-level fields.
func GenerateDockerConfig(existingContent []byte, registry, username, pat string) ([]byte, error) {
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return existingContent, nil
	}

	registry = strings.TrimSpace(registry)
	if registry == "" {
		registry = "ghcr.io"
	}

	username = strings.TrimSpace(username)
	if username == "" {
		username = "x-access-token"
	}

	var cfg map[string]any
	if len(bytes.TrimSpace(existingContent)) > 0 {
		if err := json.Unmarshal(existingContent, &cfg); err != nil {
			// If existing file is corrupted or not valid JSON, re-initialize cleanly
			cfg = make(map[string]any)
		}
	} else {
		cfg = make(map[string]any)
	}

	auths, ok := cfg["auths"].(map[string]any)
	if !ok {
		auths = make(map[string]any)
		cfg["auths"] = auths
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + pat))
	cleanEncoded := strings.ReplaceAll(strings.ReplaceAll(encoded, "\r", ""), "\n", "")

	auths[registry] = map[string]any{
		"auth": cleanEncoded,
	}

	marshaled, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal docker config: %w", err)
	}
	return append(marshaled, '\n'), nil
}

// ResolveRegistryUser determines the username for registry authentication.
// Checks DOCKER_REGISTRY_USER, GITHUB_ACTOR, parses from repoURL, or defaults to "x-access-token".
func ResolveRegistryUser(repoURL string, lookup func(string) string) string {
	if lookup != nil {
		if u := strings.TrimSpace(lookup("DOCKER_REGISTRY_USER")); u != "" {
			return u
		}
		if a := strings.TrimSpace(lookup("GITHUB_ACTOR")); a != "" {
			return a
		}
	}

	repoURL = strings.TrimSpace(repoURL)
	if repoURL != "" {
		trimmed := strings.TrimSuffix(repoURL, ".git")
		var pathPart string
		if idx := strings.Index(trimmed, "://"); idx != -1 {
			pathPart = trimmed[idx+3:]
			if slashIdx := strings.Index(pathPart, "/"); slashIdx != -1 {
				pathPart = pathPart[slashIdx+1:]
			}
		} else if idx := strings.Index(trimmed, ":"); idx != -1 {
			pathPart = trimmed[idx+1:]
		}

		parts := strings.Split(strings.Trim(pathPart, "/"), "/")
		if len(parts) >= 2 && parts[0] != "" {
			return parts[0]
		}
	}

	return "x-access-token"
}

// ResolveDockerConfigPath returns the absolute path to the Docker config.json file
// based on DOCKER_CONFIG or HOME environment variables.
func ResolveDockerConfigPath(lookup func(string) string) string {
	if lookup != nil {
		if cfg := strings.TrimSpace(lookup("DOCKER_CONFIG")); cfg != "" {
			return filepath.Join(cfg, "config.json")
		}
		if home := strings.TrimSpace(lookup("HOME")); home != "" {
			return filepath.Join(home, ".docker", "config.json")
		}
	}
	return filepath.FromSlash("/root/.docker/config.json")
}

// ParseImageReference decomposes a container image reference into its registry,
// repository path, and tag. Normalizes Docker Hub references (docker.io -> registry-1.docker.io
// and library/ prefixing for official images).
func ParseImageReference(ref string) (registry, repository, tag string, err error) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return "", "", "", fmt.Errorf("empty image reference")
	}
	if strings.Contains(trimmed, " ") {
		return "", "", "", fmt.Errorf("image reference cannot contain spaces: %q", trimmed)
	}

	// Check for digest pin (@sha256:...)
	if atIdx := strings.Index(trimmed, "@"); atIdx != -1 {
		digestPart := trimmed[atIdx+1:]
		trimmed = trimmed[:atIdx]
		if !strings.HasPrefix(digestPart, "sha256:") {
			return "", "", "", fmt.Errorf("invalid digest pin in image reference: %q", ref)
		}
		tag = digestPart
	}

	// Extract tag if present
	var namePart string
	if tag == "" {
		lastColon := strings.LastIndex(trimmed, ":")
		lastSlash := strings.LastIndex(trimmed, "/")
		if lastColon != -1 && lastColon > lastSlash {
			tag = trimmed[lastColon+1:]
			namePart = trimmed[:lastColon]
			if tag == "" || strings.Contains(tag, "/") {
				return "", "", "", fmt.Errorf("invalid tag in image reference: %q", ref)
			}
		} else {
			tag = "latest"
			namePart = trimmed
		}
	} else {
		namePart = trimmed
	}

	if namePart == "" {
		return "", "", "", fmt.Errorf("missing repository name in image reference: %q", ref)
	}

	// Determine registry vs repository
	slashIdx := strings.Index(namePart, "/")
	if slashIdx != -1 {
		firstSegment := namePart[:slashIdx]
		if strings.Contains(firstSegment, ".") || strings.Contains(firstSegment, ":") || firstSegment == "localhost" {
			registry = firstSegment
			repository = namePart[slashIdx+1:]
		} else {
			registry = "registry-1.docker.io"
			repository = namePart
		}
	} else {
		registry = "registry-1.docker.io"
		repository = "library/" + namePart
	}

	// Normalize docker.io aliases
	if registry == "docker.io" || registry == "index.docker.io" {
		registry = "registry-1.docker.io"
	}
	if registry == "registry-1.docker.io" && !strings.Contains(repository, "/") {
		repository = "library/" + repository
	}

	if repository == "" {
		return "", "", "", fmt.Errorf("empty repository path in image reference: %q", ref)
	}

	return registry, repository, tag, nil
}

// ParseWwwAuthenticate parses a Registry v2 Www-Authenticate challenge header
// (e.g. Bearer realm="https://...",service="...",scope="...").
func ParseWwwAuthenticate(header string) (realm, service, scope string, err error) {
	trimmed := strings.TrimSpace(header)
	if trimmed == "" {
		return "", "", "", fmt.Errorf("empty Www-Authenticate header")
	}

	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "bearer ") {
		return "", "", "", fmt.Errorf("unsupported auth scheme in challenge header: %q", header)
	}

	paramsPart := strings.TrimSpace(trimmed[7:])
	params := make(map[string]string)

	for len(paramsPart) > 0 {
		eqIdx := strings.Index(paramsPart, "=")
		if eqIdx == -1 {
			break
		}
		key := strings.ToLower(strings.TrimSpace(paramsPart[:eqIdx]))
		rem := strings.TrimSpace(paramsPart[eqIdx+1:])

		var val string
		if strings.HasPrefix(rem, "\"") {
			endQuote := strings.Index(rem[1:], "\"")
			if endQuote == -1 {
				val = strings.Trim(rem, "\"")
				paramsPart = ""
			} else {
				val = rem[1 : endQuote+1]
				remAfter := rem[endQuote+2:]
				if commaIdx := strings.Index(remAfter, ","); commaIdx != -1 {
					paramsPart = strings.TrimSpace(remAfter[commaIdx+1:])
				} else {
					paramsPart = ""
				}
			}
		} else {
			if commaIdx := strings.Index(rem, ","); commaIdx != -1 {
				val = strings.TrimSpace(rem[:commaIdx])
				paramsPart = strings.TrimSpace(rem[commaIdx+1:])
			} else {
				val = strings.TrimSpace(rem)
				paramsPart = ""
			}
		}

		if key != "" {
			params[key] = val
		}
	}

	realm = params["realm"]
	if realm == "" {
		return "", "", "", fmt.Errorf("missing realm in Www-Authenticate header: %q", header)
	}
	service = params["service"]
	scope = params["scope"]

	return realm, service, scope, nil
}

// ContainsDigest checks whether the specified target digest matches any entry in the image's RepoDigests.
func ContainsDigest(repoDigests []string, targetDigest string) bool {
	target := strings.TrimSpace(targetDigest)
	if target == "" {
		return false
	}
	for _, entry := range repoDigests {
		e := strings.TrimSpace(entry)
		if e == target {
			return true
		}
		if strings.HasSuffix(e, "@"+target) {
			return true
		}
		parts := strings.Split(e, "@")
		if len(parts) == 2 && parts[1] == target {
			return true
		}
	}
	return false
}

// RollbackTagForService constructs the deterministic snapshot rollback tag for a service.
func RollbackTagForService(service string) string {
	trimmed := strings.TrimSpace(service)
	trimmed = strings.TrimPrefix(trimmed, "aerial-")
	return fmt.Sprintf("aerial-%s:rollback-target", trimmed)
}


