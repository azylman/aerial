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
)

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

// CalculateSyncStatus evaluates repository statuses and determines the overall daemon status.
// Precedence: "error" > "lagging" > "synced".
func CalculateSyncStatus(repos map[string]RepoStatus) string {
	hasError := false
	hasLag := false

	for _, st := range repos {
		if st.SyncStatus == "error" || st.Error != "" {
			hasError = true
		}
		if st.SyncStatus == "lagging" {
			hasLag = true
		}
	}

	if hasError {
		return "error"
	}
	if hasLag {
		return "lagging"
	}
	return "synced"
}

var allowedExactKeys = map[string]struct{}{
	"path": {}, "home": {}, "user": {}, "tmpdir": {}, "temp": {}, "tmp": {},
	"hostname": {}, "lang": {}, "lc_all": {}, "lc_ctype": {}, "xdg_runtime_dir": {}, "ci": {},
	"http_proxy": {}, "https_proxy": {}, "no_proxy": {}, "all_proxy": {}, "ssl_cert_file": {}, "ssl_cert_dir": {},
	"systemroot": {}, "systemdrive": {}, "comspec": {}, "pathext": {},
}

var allowedPrefixes = []string{"docker_", "compose_"}

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
		before, _, ok := strings.Cut(env, "=")
		if !ok {
			continue
		}
		key := before
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
		"GIT_CONFIG_VALUE_0=AUTHORIZATION: basic "+cleanEncoded,
		"GIT_CONFIG_KEY_1=http.version",
		"GIT_CONFIG_VALUE_1=HTTP/1.1",
	)
}

// ResolveGitBin locates the git executable across platforms with Windows MinGit support.
func ResolveGitBin(lookup func(string) string, fileExists ...func(string) bool) string {
	return resolveGitBinInternal(lookup, fileExists, exec.LookPath, runtime.GOOS)
}

func resolveWindowsGitBin(lookup func(string) string, exists func(string) bool, goos string) (string, bool) {
	sep := string(filepath.Separator)
	if goos == "windows" {
		sep = "\\"
	}
	localAppData := lookup("LOCALAPPDATA")
	if localAppData != "" {
		minGit := strings.Join([]string{localAppData, "Programs", "MinGit", "cmd", "git.exe"}, sep)
		if exists(minGit) {
			return minGit, true
		}
	}

	progFiles := lookup("ProgramFiles")
	if progFiles != "" {
		gitExe := strings.Join([]string{progFiles, "Git", "cmd", "git.exe"}, sep)
		if exists(gitExe) {
			return gitExe, true
		}
	}

	userProfile := lookup("USERPROFILE")
	if userProfile != "" {
		minGitUser := strings.Join([]string{userProfile, "AppData", "Local", "Programs", "MinGit", "cmd", "git.exe"}, sep)
		if exists(minGitUser) {
			return minGitUser, true
		}
	}

	return "", false
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
		if minGit, ok := resolveWindowsGitBin(lookup, exists, goos); ok {
			return minGit
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

func parseRepoUser(repoURL string) string {
	trimmed := strings.TrimSuffix(repoURL, ".git")
	var pathPart string
	if _, after, ok := strings.Cut(trimmed, "://"); ok {
		pathPart = after
		if slashIdx := strings.Index(pathPart, "/"); slashIdx != -1 {
			pathPart = pathPart[slashIdx+1:]
		}
	} else if _, after, ok := strings.Cut(trimmed, ":"); ok {
		pathPart = after
	}

	parts := strings.Split(strings.Trim(pathPart, "/"), "/")
	if len(parts) >= 2 && parts[0] != "" {
		return parts[0]
	}
	return ""
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

	if repoURL = strings.TrimSpace(repoURL); repoURL != "" {
		if user := parseRepoUser(repoURL); user != "" {
			return user
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

func splitRegistryAndRepo(namePart string) (registry, repository string) {
	before, after, ok := strings.Cut(namePart, "/")
	if ok {
		firstSegment := before
		if strings.Contains(firstSegment, ".") || strings.Contains(firstSegment, ":") || firstSegment == "localhost" {
			registry = firstSegment
			repository = after
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
	return registry, repository
}

func parseImageTagAndName(trimmed, tag string) (string, string, error) {
	if tag != "" {
		return trimmed, tag, nil
	}
	lastColon := strings.LastIndex(trimmed, ":")
	lastSlash := strings.LastIndex(trimmed, "/")
	if lastColon != -1 && lastColon > lastSlash {
		extractedTag := trimmed[lastColon+1:]
		namePart := trimmed[:lastColon]
		if extractedTag == "" || strings.Contains(extractedTag, "/") {
			return "", "", fmt.Errorf("invalid tag in image reference: %q", trimmed)
		}
		if extractedTag == "${var.image_tag}" {
			extractedTag = "latest"
		}
		return namePart, extractedTag, nil
	}
	return trimmed, "latest", nil
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
	namePart, tag, err := parseImageTagAndName(trimmed, tag)
	if err != nil {
		return "", "", "", err
	}

	if namePart == "" {
		return "", "", "", fmt.Errorf("missing repository name in image reference: %q", ref)
	}

	registry, repository = splitRegistryAndRepo(namePart)

	if repository == "" {
		return "", "", "", fmt.Errorf("empty repository path in image reference: %q", ref)
	}

	return registry, repository, tag, nil
}

func parseQuotedAuthParam(rem string) (val, nextRem string) {
	endQuote := strings.Index(rem[1:], "\"")
	if endQuote == -1 {
		return strings.Trim(rem, "\""), ""
	}
	val = rem[1 : endQuote+1]
	remAfter := rem[endQuote+2:]
	if _, after, ok := strings.Cut(remAfter, ","); ok {
		return val, strings.TrimSpace(after)
	}
	return val, ""
}

func parseUnquotedAuthParam(rem string) (val, nextRem string) {
	if before, after, ok := strings.Cut(rem, ","); ok {
		return strings.TrimSpace(before), strings.TrimSpace(after)
	}
	return strings.TrimSpace(rem), ""
}

func parseAuthParamValue(rem string) (val, nextRem string) {
	if strings.HasPrefix(rem, "\"") {
		return parseQuotedAuthParam(rem)
	}
	return parseUnquotedAuthParam(rem)
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

		val, nextRem := parseAuthParamValue(rem)
		paramsPart = nextRem
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

// ParseGitHubSlug extracts and lowercases the "owner/repo" slug from a GitHub remote URL.
// It strips embedded credentials/tokens and returns "" if the URL is invalid or not strictly github.com.
func ParseGitHubSlug(rawRemoteURL string) string {
	raw := strings.TrimSpace(rawRemoteURL)
	if raw == "" {
		return ""
	}

	// Handle standard SCP-like SSH syntax: git@github.com:owner/repo(.git)
	if after, ok := strings.CutPrefix(raw, "git@"); ok {
		cleanSSH := after
		before, after, ok := strings.Cut(cleanSSH, ":")
		if ok {
			host := before
			pathPart := after
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

// IsNomadVarFile returns true if the given path or filename corresponds to a Nomad job variable file (*.vars.hcl).
func IsNomadVarFile(p string) bool {
	cleanPath := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	lower := strings.ToLower(path.Base(cleanPath))
	return strings.HasSuffix(lower, ".vars.hcl") && len(lower) > len(".vars.hcl")
}

// ExtractJobName extracts the Nomad job name from job specification content (e.g. job "name" { ... }).
// If parsing fails, it falls back to the file basename stripped of .nomad or .vars.hcl extensions.
func ExtractJobName(content string, fallbackFileName string) string {
	scanner := bufio.NewScanner(strings.NewReader(content))
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
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
	base = strings.TrimSuffix(base, ".vars.hcl")
	base = strings.TrimSuffix(base, ".nomad.hcl")
	base = strings.TrimSuffix(base, ".nomad")
	return base
}

func extractImageFromHCLAssignment(line string) string {
	_, after, ok := strings.Cut(line, "image")
	if !ok {
		return ""
	}
	sub := strings.TrimSpace(after)
	if !strings.HasPrefix(sub, "=") {
		return ""
	}
	val := strings.TrimSpace(sub[1:])
	qStart := strings.IndexAny(val, `"'`)
	if qStart == -1 {
		return ""
	}
	quote := val[qStart]
	rest := val[qStart+1:]
	before0, _, ok0 := strings.Cut(rest, string([]byte{quote}))
	if !ok0 {
		return ""
	}
	img := strings.TrimSpace(before0)
	return strings.ReplaceAll(img, "${var.image_tag}", "latest")
}

// ExtractNomadJobImages extracts all Docker container image references from a Nomad job specification.
// Normalizes Nomad ${var.image_tag} interpolations to "latest".
func ExtractNomadJobImages(content string) []string {
	var images []string
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(strings.NewReader(content))
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		img := extractImageFromHCLAssignment(line)
		if img != "" {
			if _, exists := seen[img]; !exists {
				seen[img] = struct{}{}
				images = append(images, img)
			}
		}
	}
	return images
}

// JobHasContainerBuildChanges returns true if any container image referenced
// in jobSpec has matching source file modifications in changedFiles according to repoPath's CI workflow rules.
func JobHasContainerBuildChanges(repoPath, jobSpec string, changedFiles []string) bool {
	affected := AffectedWorkflowImages(repoPath, changedFiles)
	if len(affected) == 0 {
		return false
	}
	for _, img := range ExtractNomadJobImages(jobSpec) {
		for _, aff := range affected {
			if ImageMatches(img, aff) {
				return true
			}
		}
	}
	return false
}

func cleanChangedFilePath(f string) string {
	return strings.TrimPrefix(path.Clean(strings.ReplaceAll(strings.TrimSpace(f), "\\", "/")), "/")
}

// HasNomadJobVariable returns true if the Nomad job specification content declares the specified input variable.
func HasNomadJobVariable(content, varName string) bool {
	varName = strings.TrimSpace(varName)
	if content == "" || varName == "" {
		return false
	}
	scanner := bufio.NewScanner(strings.NewReader(content))
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "variable" {
			token := strings.TrimSuffix(fields[1], "{")
			token = strings.Trim(token, "\"'`")
			if token == varName {
				return true
			}
		}
	}
	return false
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
		if strings.HasPrefix(status, "R") && len(parts) >= 3 {
			oldPath := parts[1]
			newPath := parts[2]
			if IsNomadJobFile(oldPath) {
				changes = append(changes, NomadFileChange{
					Path:    oldPath,
					Action:  "delete",
					JobName: ExtractJobName("", oldPath),
				})
			} else if IsNomadVarFile(oldPath) {
				changes = append(changes, NomadFileChange{
					Path:    oldPath,
					Action:  "apply",
					JobName: ExtractJobName("", oldPath),
				})
			}
			if IsNomadJobFile(newPath) || IsNomadVarFile(newPath) {
				changes = append(changes, NomadFileChange{
					Path:    newPath,
					Action:  "apply",
					JobName: ExtractJobName("", newPath),
				})
			}
			continue
		}
		filePath := parts[len(parts)-1]
		if IsNomadVarFile(filePath) {
			changes = append(changes, NomadFileChange{
				Path:    filePath,
				Action:  "apply",
				JobName: ExtractJobName("", filePath),
			})
			continue
		}
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

// walkNomadJobFiles iterates over the specified directories, deduplicating them and visiting
// each valid Nomad job file. If visit returns false, traversal terminates early.
func walkNomadJobFiles(dirs []string, visit func(dir, jobPath, jobName, content string) bool) {
	seenDirs := make(map[string]struct{})
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
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
			if !IsSafeJobPath(cleanDir, fullPath) {
				continue
			}
			content, err := os.ReadFile(fullPath)
			if err != nil {
				continue
			}
			jobName := ExtractJobName(string(content), entry.Name())
			if !visit(cleanDir, fullPath, jobName, string(content)) {
				return
			}
		}
	}
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

	var dirs []string
	for _, repo := range candidateRepos {
		if repo == "" {
			continue
		}
		cleanRepo := filepath.Clean(repo)
		if cleanSkip != "" && cleanRepo == cleanSkip {
			continue
		}
		dirs = append(dirs,
			filepath.Join(cleanRepo, "jobs"),
			filepath.Join(cleanRepo, "nomad", "jobs"),
		)
	}

	var foundPath string
	walkNomadJobFiles(dirs, func(dir, jobPath, name, content string) bool {
		if name == jobName {
			foundPath = jobPath
			return false
		}
		return true
	})
	return foundPath, foundPath != ""
}
