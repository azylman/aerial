package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
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
	"nomad",
}

// IsNomadConfigFile returns true if the given path corresponds to a Nomad daemon configuration file
// (e.g. nomad/nomad.hcl or any .hcl file located under a nomad directory).
func IsNomadConfigFile(p string) bool {
	cleanPath := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	base := path.Base(cleanPath)
	dir := path.Dir(cleanPath)
	if !strings.HasSuffix(strings.ToLower(base), ".hcl") {
		return false
	}
	return dir == "nomad" || strings.HasSuffix(dir, "/nomad") || strings.EqualFold(base, "nomad.hcl")
}

// FilterNomadConfigFiles filters a list of changed file paths and returns only those that affect Nomad daemon configuration.
func FilterNomadConfigFiles(changedFiles []string) []string {
	var result []string
	for _, f := range changedFiles {
		if IsNomadConfigFile(f) {
			result = append(result, f)
		}
	}
	return result
}

// IsComposeFile returns true if the given path or filename corresponds to a compose or environment configuration file.
func IsComposeFile(p string) bool {
	if IsNomadConfigFile(p) {
		return true
	}
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

// ParseGitHubSlug extracts and lowercases the "owner/repo" slug from a GitHub remote URL.
// It strips embedded credentials/tokens and returns "" if the URL is invalid or not strictly github.com.
func ParseGitHubSlug(rawRemoteURL string) string {
	raw := strings.TrimSpace(rawRemoteURL)
	if raw == "" {
		return ""
	}

	// Handle standard SCP-like SSH syntax: git@github.com:owner/repo(.git)
	if strings.HasPrefix(raw, "git@") {
		cleanSSH := strings.TrimPrefix(raw, "git@")
		colonIdx := strings.Index(cleanSSH, ":")
		if colonIdx != -1 {
			host := cleanSSH[:colonIdx]
			pathPart := cleanSSH[colonIdx+1:]
			if strings.EqualFold(host, "github.com") {
				return cleanSlugPath(pathPart)
			}
		}
		return ""
	}

	// URL parsing for HTTP(S) or SSH schemes (e.g. ssh://git@github.com/owner/repo or ssh://git@github.com:22/owner/repo)
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return ""
		}
		host := strings.ToLower(u.Hostname())
		if host != "github.com" {
			return ""
		}
		return cleanSlugPath(u.Path)
	}

	return ""
}

func cleanSlugPath(p string) string {
	p = strings.Trim(p, "/")
	p = strings.TrimSuffix(p, ".git")
	p = strings.Trim(p, "/")
	// Strip any query or fragment if present
	if qIdx := strings.IndexAny(p, "?#"); qIdx != -1 {
		p = p[:qIdx]
	}
	parts := strings.Split(p, "/")
	if len(parts) != 2 {
		return ""
	}
	owner := strings.TrimSpace(parts[0])
	repo := strings.TrimSpace(parts[1])
	if owner == "" || repo == "" {
		return ""
	}
	return strings.ToLower(fmt.Sprintf("%s/%s", owner, repo))
}

// IsNomadJobFile returns true if the given path or filename corresponds to a Nomad job specification file.
func IsNomadJobFile(p string) bool {
	cleanPath := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	base := path.Base(cleanPath)
	lower := strings.ToLower(base)
	return strings.HasSuffix(lower, ".nomad") || strings.HasSuffix(lower, ".nomad.hcl")
}

// FilterNomadChanges filters a list of changed file paths and returns only those that affect Nomad jobs.
func FilterNomadChanges(changedFiles []string) []string {
	var result []string
	for _, f := range changedFiles {
		if IsNomadJobFile(f) {
			result = append(result, f)
		}
	}
	return result
}

// ExtractJobName extracts the Nomad job name from job specification content (e.g. job "name" { ... }).
// If parsing fails, it falls back to the file basename stripped of .nomad extensions.
func ExtractJobName(content string, fallbackFileName string) string {
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "job ") {
			parts := strings.SplitN(line, "\"", 3)
			if len(parts) >= 2 {
				name := strings.TrimSpace(parts[1])
				if name != "" {
					return name
				}
			}
		}
	}
	base := path.Base(strings.ReplaceAll(strings.TrimSpace(fallbackFileName), "\\", "/"))
	base = strings.TrimSuffix(base, ".nomad.hcl")
	base = strings.TrimSuffix(base, ".nomad")
	return base
}

// ExtractNomadJobImages extracts all Docker container image references from a Nomad job specification.
func ExtractNomadJobImages(content string) []string {
	var images []string
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		if idx := strings.Index(line, "image"); idx != -1 {
			sub := strings.TrimSpace(line[idx+len("image"):])
			if strings.HasPrefix(sub, "=") {
				val := strings.TrimSpace(sub[1:])
				if qStart := strings.IndexAny(val, `"'`); qStart != -1 {
					quote := val[qStart]
					rest := val[qStart+1:]
					if qEnd := strings.IndexByte(rest, quote); qEnd != -1 {
						img := strings.TrimSpace(rest[:qEnd])
						if img != "" {
							if _, exists := seen[img]; !exists {
								seen[img] = struct{}{}
								images = append(images, img)
							}
						}
					}
				}
			}
		}
	}
	return images
}

// NomadFileChange represents a changed Nomad job specification with its git action (apply or delete).
type NomadFileChange struct {
	Path    string
	Action  string // "apply" or "delete"
	JobName string
}

// ParseNomadGitStatus parses git diff --name-status output and classifies changed Nomad jobs.
func ParseNomadGitStatus(gitStatusOutput string) []NomadFileChange {
	var changes []NomadFileChange
	scanner := bufio.NewScanner(strings.NewReader(gitStatusOutput))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		status := parts[0]
		filePath := parts[len(parts)-1]
		if !IsNomadJobFile(filePath) {
			continue
		}
		action := "apply"
		if strings.HasPrefix(status, "D") {
			action = "delete"
		}
		changes = append(changes, NomadFileChange{
			Path:    filePath,
			Action:  action,
			JobName: ExtractJobName("", filePath),
		})
	}
	return changes
}

// FindJobDefinitionInRepos searches candidate repositories for a Nomad job specification
// that defines the specified jobName, excluding the repo at skipRepoPath.
// Returns the file path where the job is defined and true if found, or ("", false) if not found.
func FindJobDefinitionInRepos(jobName string, skipRepoPath string, candidateRepos []string) (string, bool) {
	if jobName == "" {
		return "", false
	}
	cleanSkip := ""
	if skipRepoPath != "" {
		cleanSkip = filepath.Clean(skipRepoPath)
	}

	seenDirs := make(map[string]struct{})
	for _, repo := range candidateRepos {
		if repo == "" {
			continue
		}
		cleanRepo := filepath.Clean(repo)
		if cleanSkip != "" && cleanRepo == cleanSkip {
			continue
		}

		// Candidate directories in each repo where job specifications might reside
		candidateDirs := []string{
			filepath.Join(cleanRepo, "jobs"),
			filepath.Join(cleanRepo, "nomad", "jobs"),
		}

		for _, dir := range candidateDirs {
			cleanDir := filepath.Clean(dir)
			if _, seen := seenDirs[cleanDir]; seen {
				continue
			}
			seenDirs[cleanDir] = struct{}{}

			entries, err := os.ReadDir(cleanDir)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				if entry.IsDir() || !IsNomadJobFile(entry.Name()) {
					continue
				}
				fullPath := filepath.Join(cleanDir, entry.Name())
				content, err := os.ReadFile(fullPath)
				if err != nil {
					continue
				}
				if ExtractJobName(string(content), entry.Name()) == jobName {
					return fullPath, true
				}
			}
		}
	}
	return "", false
}

// GitPushEventRequest represents an incoming git push event.
type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

// GitPushEventResponse represents the acknowledgment for a git push event.
type GitPushEventResponse struct {
	Status             string   `json:"status"` // "accepted", "ignored", "error"
	Repo               string   `json:"repo,omitempty"`
	Ref                string   `json:"ref,omitempty"`
	Commit             string   `json:"commit,omitempty"`
	ContainersBuilding bool     `json:"containers_building,omitempty"`
	NomadChanged       bool     `json:"nomad_changed,omitempty"`
	AppliedJobs        []string `json:"applied_jobs,omitempty"`
	Message            string   `json:"message,omitempty"`
}

// ImageReadyEventRequest represents an incoming notification of a published container image.
type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

// ImageReadyEventResponse represents the acknowledgment for an image ready event.
type ImageReadyEventResponse struct {
	Status      string   `json:"status"` // "accepted", "not_found", "error"
	Image       string   `json:"image,omitempty"`
	MatchedJobs []string `json:"matched_jobs,omitempty"`
	Message     string   `json:"message,omitempty"`
}

// PendingImageRollout represents an enqueued container rollout pending debounced execution.
type PendingImageRollout struct {
	JobName string
	JobPath string
	Request ImageReadyEventRequest
}

// CoreBuildPaths contains path prefixes in azylman/aerial that trigger container builds in CI.
var CoreBuildPaths = []string{
	"brain/",
	"scheduler-mcp/",
	"discord-mcp/",
	"docker-mcp/",
	"github-mcp/",
	"nomad-mcp/",
	"infisical-mcp/",
	"dashboard/",
	"docs-service/",
	"proxy/",
	"sidecars/",
	"webhooks-router/",
	"Dockerfile",
	"go.mod",
	"go.sum",
	".dockerignore",
}

// HasContainerBuildChanges evaluates if any changed file in a repository touches paths that trigger container builds.
// For aerial-config, it always returns false. For aerial, it matches against CoreBuildPaths.
func HasContainerBuildChanges(repoPath string, changedFiles []string) bool {
	cleanRepo := strings.ToLower(filepath.Clean(repoPath))
	if strings.Contains(cleanRepo, "aerial-config") {
		return false
	}
	for _, file := range changedFiles {
		cleanFile := strings.TrimPrefix(filepath.Clean(file), "/")
		cleanFile = strings.ReplaceAll(cleanFile, "\\", "/")
		for _, buildPath := range CoreBuildPaths {
			if strings.HasSuffix(buildPath, "/") {
				if strings.HasPrefix(cleanFile, buildPath) {
					return true
				}
			} else {
				if cleanFile == buildPath {
					return true
				}
			}
		}
	}
	return false
}

// ImageMatches compares an incoming image reference with an image declared in a Nomad job specification.
func ImageMatches(query, target string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	t := strings.TrimSpace(strings.ToLower(target))
	if q == "" || t == "" {
		return false
	}
	if q == t {
		return true
	}
	cleanQ := stripImageTag(q)
	cleanT := stripImageTag(t)
	if cleanQ == cleanT {
		return true
	}
	baseQ := path.Base(cleanQ)
	baseT := path.Base(cleanT)
	return baseQ == baseT
}

func stripImageTag(img string) string {
	if idx := strings.Index(img, "@"); idx != -1 {
		img = img[:idx]
	}
	lastSlash := strings.LastIndex(img, "/")
	if lastColon := strings.LastIndex(img, ":"); lastColon > lastSlash {
		img = img[:lastColon]
	}
	return img
}

// IsSafeJobPath validates that targetPath resides strictly within baseDir and ends with .nomad or .nomad.hcl.
func IsSafeJobPath(baseDir, targetPath string) bool {
	if baseDir == "" || targetPath == "" {
		return false
	}
	cleanBase := filepath.Clean(baseDir)
	cleanTarget := filepath.Clean(targetPath)
	rel, err := filepath.Rel(cleanBase, cleanTarget)
	if err != nil || strings.HasPrefix(rel, "..") || rel == "." {
		return false
	}
	return IsNomadJobFile(cleanTarget)
}

// MatchedNomadJob holds metadata for a Nomad job matching an image reference.
type MatchedNomadJob struct {
	JobName string
	JobPath string
}

// FindNomadJobsByImage searches a directory for Nomad job specifications referencing the target image.
func FindNomadJobsByImage(jobsDir, imageRef string) []MatchedNomadJob {
	var matches []MatchedNomadJob
	if jobsDir == "" || imageRef == "" {
		return nil
	}
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() || !IsNomadJobFile(entry.Name()) {
			continue
		}
		fullPath := filepath.Join(jobsDir, entry.Name())
		if !IsSafeJobPath(jobsDir, fullPath) {
			continue
		}
		content, err := os.ReadFile(fullPath)
		if err != nil {
			continue
		}
		images := ExtractNomadJobImages(string(content))
		for _, img := range images {
			if ImageMatches(imageRef, img) {
				matches = append(matches, MatchedNomadJob{
					JobName: ExtractJobName(string(content), entry.Name()),
					JobPath: fullPath,
				})
				break
			}
		}
	}
	return matches
}

// NormalizeGitHubSlug normalizes an input repository name or URL to an "owner/repo" slug.
// Defaults to "azylman" as the owner if none is specified.
func NormalizeGitHubSlug(input string) string {
	s := strings.TrimSpace(input)
	if s == "" {
		return ""
	}
	s = strings.TrimPrefix(s, "https://github.com/")
	s = strings.TrimPrefix(s, "http://github.com/")
	s = strings.TrimPrefix(s, "git@github.com:")
	s = strings.TrimSuffix(s, ".git")
	s = strings.TrimPrefix(s, "/")

	// Handle local paths like /share/aerial
	if strings.HasPrefix(input, "/") {
		base := filepath.Base(filepath.Clean(input))
		if base != "" && base != "." && base != "/" {
			return "azylman/" + base
		}
	}

	parts := strings.Split(s, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	if len(parts) == 1 && parts[0] != "" {
		return "azylman/" + parts[0]
	}
	return s
}

// ResolveRepoPath matches an incoming repository query (slug, URL, or short name)
// against the daemon's configured repository paths on disk.
func ResolveRepoPath(query string, configuredRepos []string) string {
	q := strings.TrimSpace(strings.ToLower(query))
	if q == "" {
		return ""
	}
	q = strings.TrimSuffix(q, ".git")
	baseQ := path.Base(q)
	cleanQ := filepath.Clean(q)

	for _, repo := range configuredRepos {
		cleanRepo := filepath.Clean(repo)
		cleanLower := strings.ToLower(cleanRepo)
		baseRepo := strings.ToLower(filepath.Base(cleanRepo))

		if cleanLower == cleanQ || cleanLower == q {
			return repo
		}
		if baseRepo == q || baseRepo == baseQ {
			return repo
		}
		if strings.HasSuffix(cleanLower, "/"+baseQ) {
			return repo
		}
	}
	return ""
}

// ImageSourceRepo resolves which GitHub source repository is responsible for building a container image.
// Returns an "owner/repo" slug (e.g. "azylman/aerial", "azylman/mirrormere", "azylman/aerial-sidecars")
// or "" if the image is third-party/external.
func ImageSourceRepo(imageRef string) string {
	ref := strings.TrimSpace(strings.ToLower(imageRef))
	if ref == "" {
		return ""
	}
	if !strings.Contains(ref, "azylman/") && !strings.Contains(ref, "aerial") && !strings.Contains(ref, "mirrormere") && !strings.Contains(ref, "orin-voice") {
		return ""
	}
	// Check sidecars first
	if strings.Contains(ref, "aerial-sidecar-") || strings.Contains(ref, "orin-voice") {
		return "azylman/aerial-sidecars"
	}
	// Check mirrormere
	if strings.Contains(ref, "mirrormere") {
		return "azylman/mirrormere"
	}
	// Check core aerial images (webhooks-router, infisical-mcp, nomad-mcp, brain, hangar, etc.)
	if strings.Contains(ref, "aerial-") || strings.Contains(ref, "/aerial:") || strings.HasSuffix(ref, "/aerial") {
		return "azylman/aerial"
	}
	return ""
}

// HangarDeployEvent represents deployment and rollback lifecycle events.
type HangarDeployEvent struct {
	Event        string    `json:"event"`                   // "deploy_started", "deploy_success", "deploy_failed", "deploy_rollback"
	JobName      string    `json:"job_name"`                // Nomad job name (e.g. "webhooks-router", "brain")
	Repo         string    `json:"repo,omitempty"`          // repository name (e.g. "azylman/aerial", "azylman/aerial-config")
	CommitSHA    string    `json:"commit_sha,omitempty"`    // git commit SHA (head or merge commit)
	PRNumber     int       `json:"pr_number,omitempty"`     // PR number if known
	TargetID     string    `json:"target_id,omitempty"`     // Discord thread / channel snowflake
	Image        string    `json:"image,omitempty"`         // container image (e.g. ghcr.io/azylman/aerial-webhooks-router:latest)
	Digest       string    `json:"digest,omitempty"`        // image digest (sha256:...)
	Status       string    `json:"status"`                  // "started", "success", "failed", "rollback"
	DeploymentID string    `json:"deployment_id,omitempty"` // Nomad deployment UUID
	Details      string    `json:"details,omitempty"`       // status description or error message
	Timestamp    time.Time `json:"timestamp"`
}

// TaskGroupSummary represents allocation summary counts for a single task group.
type TaskGroupSummary struct {
	Queued   int `json:"Queued"`
	Complete int `json:"Complete"`
	Failed   int `json:"Failed"`
	Running  int `json:"Running"`
	Starting int `json:"Starting"`
	Lost     int `json:"Lost"`
	Unknown  int `json:"Unknown"`
}

// NomadJobSummary encapsulates the Nomad job allocation summary.
// Real Nomad API provides a map of task group names to TaskGroupSummary.
// For backwards compatibility with synthetic test payloads, Children is also supported.
type NomadJobSummary struct {
	TaskGroups map[string]TaskGroupSummary `json:"-"`
	Children   struct {
		Pending int `json:"Pending"`
		Running int `json:"Running"`
		Dead    int `json:"Dead"`
	} `json:"Children"`
}

// UnmarshalJSON handles both map-based task group summaries and legacy Children formats.
func (s *NomadJobSummary) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.TaskGroups = make(map[string]TaskGroupSummary)
	for k, v := range raw {
		if k == "Children" {
			if err := json.Unmarshal(v, &s.Children); err != nil {
				return err
			}
			continue
		}
		var tg TaskGroupSummary
		if err := json.Unmarshal(v, &tg); err == nil {
			s.TaskGroups[k] = tg
		}
	}
	return nil
}

// NomadJobStatusOutput represents parsed output from nomad job status -json.
type NomadJobStatusOutput struct {
	ID               string `json:"ID"`
	JobVersion       int    `json:"JobVersion"`
	SubmitTime       int64  `json:"SubmitTime"`
	LatestDeployment *struct {
		ID                string `json:"ID"`
		JobVersion        int    `json:"JobVersion"`
		Status            string `json:"Status"` // "running", "successful", "failed", "cancelled"
		StatusDescription string `json:"StatusDescription"`
		TaskGroups        map[string]struct {
			AutoRevert bool `json:"AutoRevert"`
		} `json:"TaskGroups"`
	} `json:"LatestDeployment"`
	Summary NomadJobSummary `json:"Summary"`
}

// TotalRunning returns the total count of running allocations across all task groups.
func (o *NomadJobStatusOutput) TotalRunning() int {
	if o == nil {
		return 0
	}
	total := 0
	for _, tg := range o.Summary.TaskGroups {
		total += tg.Running
	}
	if total == 0 && o.Summary.Children.Running > 0 {
		return o.Summary.Children.Running
	}
	return total
}

// ParseJobStatusOutput parses JSON output from nomad job status -json,
// handling both single object and array formats gracefully.
func ParseJobStatusOutput(raw []byte) (*NomadJobStatusOutput, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty job status output")
	}

	startIdx := bytes.IndexAny(trimmed, "{[")
	if startIdx == -1 {
		return nil, fmt.Errorf("no json object or array found in job status output: %q", string(trimmed))
	}
	trimmed = trimmed[startIdx:]

	if trimmed[0] == '[' {
		endIdx := bytes.LastIndexByte(trimmed, ']')
		if endIdx != -1 {
			trimmed = trimmed[:endIdx+1]
		}
		var list []NomadJobStatusOutput
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return nil, fmt.Errorf("failed to parse nomad job status array: %w", err)
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("empty nomad job status array")
		}
		return &list[0], nil
	}

	endIdx := bytes.LastIndexByte(trimmed, '}')
	if endIdx != -1 {
		trimmed = trimmed[:endIdx+1]
	}
	var single NomadJobStatusOutput
	if err := json.Unmarshal(trimmed, &single); err != nil {
		return nil, fmt.Errorf("failed to parse nomad job status object: %w", err)
	}
	return &single, nil
}
