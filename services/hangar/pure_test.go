package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsNomadConfigFile_TableDriven(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"nomad/nomad.hcl", true},
		{"nomad/server.hcl", true},
		{"nomad/client.hcl", true},
		{"/share/aerial/nomad/nomad.hcl", true},
		{"nomad.hcl", true},
		{"jobs/laya.nomad", false},
		{"jobs/laya.nomad.hcl", false},
		{"nomad/README.md", false},
		{"main.go", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := IsNomadConfigFile(tt.path)
			if got != tt.expected {
				t.Errorf("IsNomadConfigFile(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestFilterNomadConfigFiles_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "mixed list",
			input:    []string{"brain/main.go", "nomad/nomad.hcl", "README.md", "nomad/client.hcl", "docker-compose.yml"},
			expected: []string{"nomad/nomad.hcl", "nomad/client.hcl"},
		},
		{
			name:     "no nomad configs",
			input:    []string{"docker-compose.yml", "jobs/server.nomad"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterNomadConfigFiles(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d matches, got %d: %v", len(tt.expected), len(got), got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestCalculateSyncStatus_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		repos    map[string]RepoStatus
		expected string
	}{
		{
			name:     "empty repos defaults to synced",
			repos:    map[string]RepoStatus{},
			expected: "synced",
		},
		{
			name: "all repos synced",
			repos: map[string]RepoStatus{
				"repo1": {SyncStatus: "synced"},
				"repo2": {SyncStatus: "synced"},
			},
			expected: "synced",
		},
		{
			name: "lagging takes precedence over synced",
			repos: map[string]RepoStatus{
				"repo1": {SyncStatus: "synced"},
				"repo2": {SyncStatus: "lagging"},
			},
			expected: "lagging",
		},
		{
			name: "error takes highest precedence",
			repos: map[string]RepoStatus{
				"repo1": {SyncStatus: "synced"},
				"repo2": {SyncStatus: "lagging"},
				"repo3": {SyncStatus: "error", Error: "disk read failed"},
			},
			expected: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateSyncStatus(tt.repos)
			if got != tt.expected {
				t.Errorf("CalculateSyncStatus() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestScrubComposeEnv_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name: "preserves allowlisted keys and prefixes, drops non-allowlisted and malformed entries",
			input: []string{
				"AERIAL_CONFIG_DIR=/dir/config",
				"aerial_project_dir=/dir/project",
				"MALFORMED_ENTRY",
				"PATH=/usr/bin:/bin",
				"path=/custom/path",
				"HOME=/root",
				"USER=aerial",
				"DOCKER_HOST=unix:///var/run/docker.sock",
				"docker_config=/etc/docker",
				"COMPOSE_PROJECT_NAME=aerial",
				"compose_file=docker-compose.yml",
				"XDG_RUNTIME_DIR=/run/user/1000",
				"CI=true",
				"HTTP_PROXY=http://proxy:8080",
				"https_proxy=https://proxy:8443",
				"NO_PROXY=localhost,127.0.0.1",
				"SSL_CERT_FILE=/etc/ssl/certs/ca.crt",
				"SYSTEMROOT=C:\\Windows",
				"GITHUB_PAT=ghp_secret",
				"DISCORD_BOT_TOKEN=token123",
				"POSTGRES_PASSWORD=pass123",
				"HA_TOKEN=token456",
				"PORT=8080",
				"SAFE_VAR=123",
				"OTHER_CONFIG=/dir/other",
			},
			expected: []string{
				"PATH=/usr/bin:/bin",
				"path=/custom/path",
				"HOME=/root",
				"USER=aerial",
				"DOCKER_HOST=unix:///var/run/docker.sock",
				"docker_config=/etc/docker",
				"COMPOSE_PROJECT_NAME=aerial",
				"compose_file=docker-compose.yml",
				"XDG_RUNTIME_DIR=/run/user/1000",
				"CI=true",
				"HTTP_PROXY=http://proxy:8080",
				"https_proxy=https://proxy:8443",
				"NO_PROXY=localhost,127.0.0.1",
				"SSL_CERT_FILE=/etc/ssl/certs/ca.crt",
				"SYSTEMROOT=C:\\Windows",
			},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ScrubComposeEnv(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d vars, got %d: %v", len(tt.expected), len(got), got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestBuildGitEnv_TableDriven(t *testing.T) {
	baseEnv := []string{"FOO=BAR"}

	// 1. Non-empty PAT
	authEnv := BuildGitEnv("test_pat_value", baseEnv)
	hasPrompt := false
	hasAuthHeader := false
	for _, e := range authEnv {
		if e == "GIT_TERMINAL_PROMPT=0" {
			hasPrompt = true
		}
		if strings.HasPrefix(e, "GIT_CONFIG_VALUE_0=AUTHORIZATION: basic ") {
			hasAuthHeader = true
		}
	}
	if !hasPrompt || !hasAuthHeader {
		t.Errorf("BuildGitEnv with PAT missing required headers: prompt=%v, auth=%v", hasPrompt, hasAuthHeader)
	}

	// 2. Empty PAT
	cleanEnv := BuildGitEnv("", baseEnv)
	hasAuthHeader = false
	for _, e := range cleanEnv {
		if strings.Contains(e, "GIT_CONFIG") {
			hasAuthHeader = true
		}
	}
	if hasAuthHeader {
		t.Errorf("BuildGitEnv with empty PAT should not have GIT_CONFIG auth variables")
	}
}

func TestResolveGitBin_TableDriven(t *testing.T) {
	mockFiles := map[string]bool{}
	mockFileExists := func(path string) bool {
		return mockFiles[path]
	}
	noLookPath := func(string) (string, error) {
		return "", errors.New("not on path")
	}

	// 1. LookPath hit takes priority
	resolvedHit := resolveGitBinInternal(func(string) string { return "" }, nil, func(string) (string, error) {
		return "/usr/bin/custom-git", nil
	}, "linux")
	if resolvedHit != "/usr/bin/custom-git" {
		t.Errorf("expected '/usr/bin/custom-git', got %q", resolvedHit)
	}

	// 2. MinGit via LOCALAPPDATA
	minGitPath := "C:\\mock\\localappdata\\Programs\\MinGit\\cmd\\git.exe"
	mockFiles[minGitPath] = true

	lookup1 := func(key string) string {
		switch key {
		case "OS":
			return "Windows_NT"
		case "LOCALAPPDATA":
			return "C:\\mock\\localappdata"
		default:
			return ""
		}
	}
	resolved := resolveGitBinInternal(lookup1, []func(string) bool{mockFileExists}, noLookPath, "windows")
	if resolved != minGitPath {
		t.Errorf("expected %q, got %q", minGitPath, resolved)
	}

	// 3. Git via ProgramFiles
	delete(mockFiles, minGitPath)
	progFilesGit := "C:\\mock\\ProgramFiles\\Git\\cmd\\git.exe"
	mockFiles[progFilesGit] = true

	lookup2 := func(key string) string {
		switch key {
		case "OS":
			return "Windows_NT"
		case "ProgramFiles":
			return "C:\\mock\\ProgramFiles"
		default:
			return ""
		}
	}
	resolved = resolveGitBinInternal(lookup2, []func(string) bool{mockFileExists}, noLookPath, "windows")
	if resolved != progFilesGit {
		t.Errorf("expected %q, got %q", progFilesGit, resolved)
	}

	// 4. MinGit via USERPROFILE
	delete(mockFiles, progFilesGit)
	userProfileMinGit := "C:\\mock\\user\\AppData\\Local\\Programs\\MinGit\\cmd\\git.exe"
	mockFiles[userProfileMinGit] = true

	lookup3 := func(key string) string {
		switch key {
		case "OS":
			return "Windows_NT"
		case "USERPROFILE":
			return "C:\\mock\\user"
		default:
			return ""
		}
	}
	resolved = resolveGitBinInternal(lookup3, []func(string) bool{mockFileExists}, noLookPath, "windows")
	if resolved != userProfileMinGit {
		t.Errorf("expected %q, got %q", userProfileMinGit, resolved)
	}

	// 5. Fallback to "git" when none found
	delete(mockFiles, userProfileMinGit)
	lookupEmpty := func(key string) string { return "" }
	resolved = resolveGitBinInternal(lookupEmpty, []func(string) bool{mockFileExists}, noLookPath, "windows")
	if resolved != "git" {
		t.Errorf("expected 'git', got %q", resolved)
	}

	// 6. Linux without git on path fallback
	resolvedLinux := resolveGitBinInternal(lookupEmpty, nil, noLookPath, "linux")
	if resolvedLinux != "git" {
		t.Errorf("expected 'git', got %q", resolvedLinux)
	}

	// 7. Test public ResolveGitBin
	_ = ResolveGitBin(lookupEmpty)
	_ = ResolveGitBin(lookup1, mockFileExists)
}

func TestGenerateDockerConfig_TableDriven(t *testing.T) {
	dummyPAT := "test-pat-dummy-12345"
	defaultAuth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + dummyPAT))
	customAuth := base64.StdEncoding.EncodeToString([]byte("custom-user:" + dummyPAT))

	tests := []struct {
		name         string
		existingJSON string
		registry     string
		username     string
		pat          string
		wantSub      string
	}{
		{"empty pat returns existing content", `{"credsStore":"desktop"}`, "ghcr.io", "azylman", "", `"credsStore":"desktop"`},
		{"whitespace pat returns existing content", `{"credsStore":"desktop"}`, "ghcr.io", "azylman", "  \t\n ", `"credsStore":"desktop"`},
		{"fresh generation with defaults", "", "", "", dummyPAT, defaultAuth},
		{"merges into existing config preserving docker.io", `{"credsStore":"pass","auths":{"docker.io":{"auth":"ZG9ja2VydXNlcjpkb2NrZXJwYXNz"}}}`, "ghcr.io", "custom-user", dummyPAT, customAuth},
		{"recovers from malformed existing JSON", `{invalid-json`, "ghcr.io", "azylman", dummyPAT, `"ghcr.io"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := GenerateDockerConfig([]byte(tt.existingJSON), tt.registry, tt.username, tt.pat)
			var parsed map[string]any
			if err != nil || json.Unmarshal(out, &parsed) != nil || !strings.Contains(string(out), tt.wantSub) {
				t.Fatalf("GenerateDockerConfig() err=%v, out=%s, wantSub=%q", err, string(out), tt.wantSub)
			}
		})
	}
}

func TestResolveRegistryUser_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		repoURL  string
		env      map[string]string
		nilEnv   bool
		expected string
	}{
		{"DOCKER_REGISTRY_USER priority", "https://github.com/azylman/aerial-config.git", map[string]string{"DOCKER_REGISTRY_USER": "explicit-user", "GITHUB_ACTOR": "actor-user"}, false, "explicit-user"},
		{"GITHUB_ACTOR priority", "https://github.com/azylman/aerial-config.git", map[string]string{"GITHUB_ACTOR": "actor-user"}, false, "actor-user"},
		{"https github URL with .git", "https://github.com/my-org/my-repo.git", nil, false, "my-org"},
		{"https github URL without .git", "https://github.com/custom-owner/custom-repo", nil, false, "custom-owner"},
		{"git@ ssh URL", "git@github.com:ssh-owner/ssh-repo.git", nil, false, "ssh-owner"},
		{"nil lookup unparseable URL", "invalid-url", nil, true, "x-access-token"},
		{"empty URL defaults to x-access-token", "", nil, false, "x-access-token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookup func(string) string
			if !tt.nilEnv {
				lookup = func(k string) string { return tt.env[k] }
			}
			if got := ResolveRegistryUser(tt.repoURL, lookup); got != tt.expected {
				t.Errorf("ResolveRegistryUser() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestResolveDockerConfigPath_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		nilEnv   bool
		expected string
	}{
		{"DOCKER_CONFIG takes precedence", map[string]string{"DOCKER_CONFIG": "/custom/docker/cfg", "HOME": "/home/user"}, false, "/custom/docker/cfg/config.json"},
		{"HOME used when DOCKER_CONFIG unset", map[string]string{"HOME": "/home/user"}, false, "/home/user/.docker/config.json"},
		{"nil lookup defaults to root", nil, true, "/root/.docker/config.json"},
		{"empty env defaults to root", nil, false, "/root/.docker/config.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookup func(string) string
			if !tt.nilEnv {
				lookup = func(k string) string { return tt.env[k] }
			}
			if got := filepath.ToSlash(ResolveDockerConfigPath(lookup)); got != tt.expected {
				t.Errorf("ResolveDockerConfigPath() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseImageReference_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantReg  string
		wantRepo string
		wantTag  string
		wantErr  bool
	}{
		{"official single segment", "nginx", "registry-1.docker.io", "library/nginx", "latest", false},
		{"official single segment with tag", "redis:alpine", "registry-1.docker.io", "library/redis", "alpine", false},
		{"namespaced docker hub", "pgvector/pgvector:pg16", "registry-1.docker.io", "pgvector/pgvector", "pg16", false},
		{"explicit docker.io domain", "docker.io/library/ubuntu:latest", "registry-1.docker.io", "library/ubuntu", "latest", false},
		{"explicit index.docker.io domain", "index.docker.io/prom/node-exporter:v1.8.2", "registry-1.docker.io", "prom/node-exporter", "v1.8.2", false},
		{"ghcr private monorepo reference", "ghcr.io/azylman/aerial-brain:latest", "ghcr.io", "azylman/aerial-brain", "latest", false},
		{"ghcr public reference without tag defaults to latest", "ghcr.io/gethomepage/homepage", "ghcr.io", "gethomepage/homepage", "latest", false},
		{"gcr third-party reference", "gcr.io/cadvisor/cadvisor:v0.49.1", "gcr.io", "cadvisor/cadvisor", "v0.49.1", false},
		{"localhost registry with port", "localhost:5000/my-app:1.0", "localhost:5000", "my-app", "1.0", false},
		{"custom domain with port", "registry.internal:8443/deep/path/svc:dev", "registry.internal:8443", "deep/path/svc", "dev", false},
		{"pinned sha256 digest reference", "ghcr.io/azylman/brain@sha256:1f9fd513925ef06272dea861c4c8cb7c10a9eb6b5a4e7ad0e4ca00b49fc27866", "ghcr.io", "azylman/brain", "sha256:1f9fd513925ef06272dea861c4c8cb7c10a9eb6b5a4e7ad0e4ca00b49fc27866", false},
		{"empty string errors", "", "", "", "", true},
		{"whitespace only errors", "   ", "", "", "", true},
		{"spaces in reference errors", "ghcr.io/ azylman/brain:latest", "", "", "", true},
		{"invalid digest prefix errors", "redis@md5:12345", "", "", "", true},
		{"trailing colon without tag errors", "redis:", "", "", "", true},
		{"missing repository name errors", ":latest", "", "", "", true},
		{"nomad image_tag variable interpolation normalized to latest", "ghcr.io/azylman/aerial-vanadis:${var.image_tag}", "ghcr.io", "azylman/aerial-vanadis", "latest", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg, repo, tag, err := ParseImageReference(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.input, err)
			}
			if reg != tt.wantReg {
				t.Errorf("registry = %q, want %q", reg, tt.wantReg)
			}
			if repo != tt.wantRepo {
				t.Errorf("repository = %q, want %q", repo, tt.wantRepo)
			}
			if tag != tt.wantTag {
				t.Errorf("tag = %q, want %q", tag, tt.wantTag)
			}
		})
	}
}

func TestParseWwwAuthenticate_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantRealm   string
		wantService string
		wantScope   string
		wantErr     bool
	}{
		{"docker hub standard challenge with quotes", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:pgvector/pgvector:pull"`, "https://auth.docker.io/token", "registry.docker.io", "repository:pgvector/pgvector:pull", false},
		{"ghcr standard challenge without scope", `Bearer realm="https://ghcr.io/token",service="ghcr.io"`, "https://ghcr.io/token", "ghcr.io", "", false},
		{"case-insensitive bearer prefix and whitespace", `bearer  realm="https://auth.example.com/token" , service="example.com" `, "https://auth.example.com/token", "example.com", "", false},
		{"unquoted values", `Bearer realm=https://auth.example.com/token,service=my.registry`, "https://auth.example.com/token", "my.registry", "", false},
		{"unquoted single realm", `Bearer realm=https://auth.example.com/token`, "https://auth.example.com/token", "", "", false},
		{"unclosed quote realm", `Bearer realm="https://auth.example.com/token`, "https://auth.example.com/token", "", "", false},
		{"trailing parameter without value", `Bearer realm="https://example.com",service="test",trailingnoparam`, "https://example.com", "test", "", false},
		{"empty header", "", "", "", "", true},
		{"non-bearer scheme", `Basic realm="WallyWorld"`, "", "", "", true},
		{"missing realm", `Bearer service="registry.docker.io"`, "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			realm, service, scope, err := ParseWwwAuthenticate(tt.header)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tt.header)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.header, err)
			}
			if realm != tt.wantRealm {
				t.Errorf("realm = %q, want %q", realm, tt.wantRealm)
			}
			if service != tt.wantService {
				t.Errorf("service = %q, want %q", service, tt.wantService)
			}
			if scope != tt.wantScope {
				t.Errorf("scope = %q, want %q", scope, tt.wantScope)
			}
		})
	}
}

func TestParseGitHubSlug_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard https with .git", "https://github.com/azylman/aerial.git", "azylman/aerial"},
		{"standard https without .git", "https://github.com/azylman/mirrormere", "azylman/mirrormere"},
		{"https with trailing slash", "https://github.com/azylman/mirrormere/", "azylman/mirrormere"},
		{"https with auth credentials / token", "https://x-access-token:ghp_secret12345@github.com/azylman/mirrormere.git", "azylman/mirrormere"},
		{"scp-like ssh syntax with .git", "git@github.com:azylman/aerial.git", "azylman/aerial"},
		{"scp-like ssh syntax without .git", "git@github.com:azylman/aerial", "azylman/aerial"},
		{"ssh scheme with user", "ssh://git@github.com/azylman/aerial.git", "azylman/aerial"},
		{"ssh scheme with port", "ssh://git@github.com:22/azylman/aerial.git", "azylman/aerial"},
		{"mixed casing normalized to lowercase", "https://github.com/Azylman/MirrorMere.git", "azylman/mirrormere"},
		{"with query params and fragment", "https://github.com/azylman/aerial.git?ref=main#readme", "azylman/aerial"},
		{"leading and trailing whitespace and newlines", "  \n\thttps://github.com/azylman/aerial.git \r\n ", "azylman/aerial"},
		{"gitlab rejected", "https://gitlab.com/azylman/aerial.git", ""},
		{"spoofed host rejected", "https://notgithub.com/azylman/aerial.git", ""},
		{"subdomain spoof rejected", "https://github.com.evil.com/azylman/aerial.git", ""},
		{"gitlab ssh rejected", "git@gitlab.com:azylman/aerial.git", ""},
		{"local path rejected", "/share/aerial", ""},
		{"empty input", "", ""},
		{"whitespace only", "   \n\t  ", ""},
		{"root github url rejected", "https://github.com/", ""},
		{"single segment path rejected", "https://github.com/azylman", ""},
		{"extra path segments rejected", "https://github.com/azylman/aerial/tree/main", ""},
		{"invalid url", "http://:invalid-url", ""},
		{"blank owner or repo", "https://github.com/ /aerial", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseGitHubSlug(tc.input)
			if got != tc.expected {
				t.Errorf("ParseGitHubSlug(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestIsNomadJobFile_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"root nomad file", "app.nomad", true},
		{"jobs dir nomad file", "jobs/infisical.nomad", true},
		{"nested jobs nomad file", "jobs/edge/kiosk.nomad", true},
		{"nomad hcl extension", "jobs/voice.nomad.hcl", true},
		{"uppercase nomad extension", "jobs/TEST.NOMAD", true},
		{"compose file false", "docker-compose.yml", false},
		{"yaml file false", "services/mirrormere/voice.yaml", false},
		{"go file false", "main.go", false},
		{"nomad in dir name but not file", "nomad/server.hcl", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNomadJobFile(tt.path); got != tt.expected {
				t.Errorf("IsNomadJobFile(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestExtractJobName_TableDriven(t *testing.T) {
	tests := []struct {
		name             string
		content          string
		fallbackFileName string
		expected         string
	}{
		{
			name: "standard job definition",
			content: `
job "infisical" {
  datacenters = ["dc1"]
  type        = "service"
}`,
			fallbackFileName: "jobs/infisical.nomad",
			expected:         "infisical",
		},
		{
			name: "job with comments above",
			content: `
# Comment about job
// Another comment
job "mirrormere-core" {
  type = "service"
}`,
			fallbackFileName: "jobs/mirrormere-core.nomad",
			expected:         "mirrormere-core",
		},
		{
			name:             "fallback when content has no job block",
			content:          `some random hcl without job declaration`,
			fallbackFileName: "jobs/kiosk-client.nomad.hcl",
			expected:         "kiosk-client",
		},
		{
			name:             "fallback strips vars hcl extension",
			content:          `extra_mounts = []`,
			fallbackFileName: "jobs/eri.vars.hcl",
			expected:         "eri",
		},
		{
			name:             "fallback strips nomad hcl extension",
			content:          `# only comments`,
			fallbackFileName: "jobs/svc.nomad.hcl",
			expected:         "svc",
		},
		{
			name:             "empty job name in job declaration falls back",
			content:          `job "" {}`,
			fallbackFileName: "jobs/fallback.nomad",
			expected:         "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractJobName(tt.content, tt.fallbackFileName); got != tt.expected {
				t.Errorf("ExtractJobName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestExtractNomadJobImages_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected []string
	}{
		{
			name: "multi-task job spec",
			content: `
job "infisical" {
  group "infisical" {
    task "redis" {
      config {
        image = "redis:7-alpine"
      }
    }
    task "server" {
      config {
        image = "infisical/infisical:latest"
      }
    }
  }
}`,
			expected: []string{"redis:7-alpine", "infisical/infisical:latest"},
		},
		{
			name: "single task with quotes and spaces",
			content: `
task "voice" {
  config {
    image = "ghcr.io/azylman/orin-voice:latest"
  }
}`,
			expected: []string{"ghcr.io/azylman/orin-voice:latest"},
		},
		{
			name: "parameterized image_tag normalized to latest",
			content: `
task "vanadis" {
  config {
    image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}"
  }
}`,
			expected: []string{"ghcr.io/azylman/aerial-vanadis:latest"},
		},
		{
			name: "no images",
			content: `
job "raw-exec-job" {
  task "script" {
    driver = "raw_exec"
  }
}
// image = "ignored/comment:latest"
# image = "ignored/comment2:latest"
task "bad1" {
  config {
    image = unquoted
  }
}
task "bad2" {
  config {
    image = "unclosed
  }
}`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractNomadJobImages(tt.content)
			if len(got) != len(tt.expected) {
				t.Fatalf("ExtractNomadJobImages() returned %d items, want %d", len(got), len(tt.expected))
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("ExtractNomadJobImages()[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func writeTestWorkflowFixture(t testing.TB, repoDir, filtersYAML, catalogJS string) {
	t.Helper()
	wfDir := filepath.Join(repoDir, ".github", "workflows")
	if err := os.MkdirAll(wfDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`name: CI
on: [push]
jobs:
  changes:
    runs-on: ubuntu-latest
    steps:
      - uses: dorny/paths-filter@v4
        id: filter
        with:
          filters: |
%s
      - name: Compute Dynamic Build Matrix
        id: set-matrix
        run: |
          const catalog = [
%s
          ];
`, indentLines(filtersYAML, 12), indentLines(catalogJS, 12))
	if err := os.WriteFile(filepath.Join(wfDir, "docker-publish.yml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func indentLines(s string, spaces int) string {
	pad := strings.Repeat(" ", spaces)
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, l := range lines {
		if strings.TrimSpace(l) != "" {
			lines[i] = pad + l
		}
	}
	return strings.Join(lines, "\n")
}

func testAssetBundles(composeDir, configDir, mirrormereDir string) []AssetBundleConfig {
	return []AssetBundleConfig{
		{
			Name:       "migrations",
			TriggerJob: "nomad/jobs/db-migrate.nomad",
			Sources: map[string]string{
				".": filepath.Join(composeDir, "services/eri/pkg/db/migrations"),
			},
		},
		{
			Name:     "dashboard",
			NomadVar: "nomad/jobs/dashboard",
			Sources: map[string]string{
				".": filepath.Join(composeDir, "services/dashboard/static"),
			},
		},
		{
			Name:     "docs",
			NomadVar: "nomad/jobs/docs",
			Sources: map[string]string{
				"app":     filepath.Join(composeDir, "services/docs-service/app"),
				"content": filepath.Join(configDir, "docs"),
			},
		},
		{
			Name:     "grafana",
			NomadVar: "nomad/jobs/grafana",
			Sources: map[string]string{
				"system": filepath.Join(composeDir, "nomad/grafana/dashboards"),
				"custom": filepath.Join(configDir, "services/grafana/dashboards"),
			},
		},
		{
			Name:     "homepage",
			NomadVar: "nomad/jobs/homepage",
			Excludes: []string{"core-homepage/test"},
			Sources: map[string]string{
				"core-homepage": filepath.Join(composeDir, "services/homepage"),
				"user-homepage": filepath.Join(configDir, "homepage"),
			},
		},
		{
			Name:     "mirrormere",
			NomadVar: "nomad/jobs/mirrormere-core",
			Sources: map[string]string{
				"web":     filepath.Join(mirrormereDir, "web"),
				"widgets": filepath.Join(mirrormereDir, "widgets"),
			},
		},
	}
}

func testServiceConfigs() []ServiceConfigMapping {
	return []ServiceConfigMapping{
		{RelPath: "config.yaml", NomadVar: "nomad/jobs/eri", VarKey: "CONFIG_YAML"},
		{RelPath: "services/homepage/homepage.yaml", NomadVar: "nomad/jobs/homepage", VarKey: "HOMEPAGE_YAML"},
		{RelPath: "services/haro/haro.yaml", NomadVar: "nomad/jobs/haro", VarKey: "CONFIG_YAML"},
		{RelPath: "services/coven/coven.yaml", NomadVar: "nomad/jobs/coven", VarKey: "CONFIG_YAML"},
		{RelPath: "services/synapse/synapse.yaml", NomadVar: "nomad/jobs/synapse", VarKey: "CONFIG_YAML"},
		{RelPath: "services/dashboard/dashboard.yaml", NomadVar: "nomad/jobs/dashboard", VarKey: "CONFIG_YAML"},
		{RelPath: "services/mirrormere/mirrormere.yaml", NomadVar: "nomad/jobs/mirrormere-core", VarKey: "CONFIG_YAML"},
	}
}

func seedRepoWorkflowFixture(t testing.TB, repoDir string) {
	t.Helper()
	base := strings.ToLower(filepath.Base(repoDir))
	switch {
	case strings.Contains(base, "sidecar"):
		writeTestWorkflowFixture(t, repoDir,
			"vision:\n  - 'vision/**'\nmigrator:\n  - 'scratch-service/migrations/**'\n  - 'atlas.hcl'",
			`{ service: "vision", image_name: "aerial-sidecar-vision" }, { service: "migrator", image_name: "aerial-sidecar-migrator" }`)
	case strings.Contains(base, "mirrormere"):
		writeTestWorkflowFixture(t, repoDir,
			"mirrormere:\n  - 'cmd/**'\n  - 'Dockerfile'\near:\n  - 'client/ear/**'\ncast-watcher:\n  - 'client/cast-watcher/**'\n  - 'go.mod'",
			`{ service: "mirrormere", image_name: "mirrormere" }, { service: "ear", image_name: "mirrormere-ear" }, { service: "cast-watcher", image_name: "mirrormere-cast-watcher" }`)
	default:
		writeTestWorkflowFixture(t, repoDir,
			"eri:\n  - 'services/eri/**'\n  - 'pkg/**'\ncoven:\n  - 'services/coven/**'\n  - 'pkg/**'\nsynapse:\n  - 'services/synapse/**'\n  - 'pkg/**'\nhangar:\n  - 'services/hangar/**'\n  - 'pkg/**'\nvanadis:\n  - 'services/vanadis/**'\n  - 'pkg/**'\n  - 'Dockerfile'\ndocs:\n  - 'services/docs-service/**'\ndiscord-mcp:\n  - 'mcp/discord-mcp/**'\n  - 'pkg/**'\ninfisical-mcp:\n  - 'mcp/infisical-mcp/**'",
			`{ service: "eri", image_name: "aerial-eri" }, { service: "coven", image_name: "aerial-coven" }, { service: "synapse", image_name: "aerial-synapse" }, { service: "hangar", image_name: "aerial-hangar" }, { service: "vanadis", image_name: "aerial-vanadis" }, { service: "docs", image_name: "aerial-docs" }, { service: "discord-mcp", image_name: "aerial-discord-mcp" }, { service: "infisical-mcp", image_name: "aerial-infisical-mcp" }`)
	}
}

func TestJobHasContainerBuildChanges_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	sidecarsDir := filepath.Join(tmpDir, "aerial-sidecars")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	seedRepoWorkflowFixture(t, aerialDir)
	seedRepoWorkflowFixture(t, sidecarsDir)
	seedRepoWorkflowFixture(t, mirrormereDir)

	specVanadis := `job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`
	specDocs := `job "docs" { task "docs" { config { image = "ghcr.io/azylman/aerial-docs:${var.image_tag}" } } }`
	specMCP := `job "discord-mcp" { task "mcp" { config { image = "ghcr.io/azylman/aerial-discord-mcp:${var.image_tag}" } } }`
	specExternal := `job "coverage-ingest" { task "flush" { config { image = "golang:1.27.1-alpine" } } }`

	tests := []struct {
		name     string
		repoPath string
		jobSpec  string
		files    []string
		want     bool
	}{
		{"matching services dir", aerialDir, specVanadis, []string{"services/vanadis/main.go"}, true},
		{"matching mcp dir", aerialDir, specMCP, []string{"mcp/discord-mcp/main.go"}, true},
		{"matching docs-service alias", aerialDir, specDocs, []string{"services/docs-service/Dockerfile"}, true},
		{"matching root Dockerfile", aerialDir, specVanadis, []string{"Dockerfile"}, true},
		{"matching pkg dir", aerialDir, specVanadis, []string{"pkg/httpkit/httpkit.go"}, true},
		{"pkg dir does not match non-pkg service", aerialDir, specDocs, []string{"pkg/httpkit/httpkit.go"}, false},
		{"unmatched other service dir", aerialDir, specVanadis, []string{"services/hangar/main.go", "nomad/jobs/vanadis.nomad"}, false},
		{"external image never matches", aerialDir, specExternal, []string{"services/vanadis/main.go", "Dockerfile"}, false},
		{"empty changed files", aerialDir, specVanadis, nil, false},
		{"sidecar vision image matches vision dir", sidecarsDir, `job "vision" { task "vision" { config { image = "ghcr.io/azylman/aerial-sidecar-vision:${var.image_tag}" } } }`, []string{"vision/app/detector.py"}, true},
		{"sidecar migrator matches atlas.hcl", sidecarsDir, `job "aerial-sidecars-db-migrate" { task "atlas" { config { image = "ghcr.io/azylman/aerial-sidecar-migrator:latest" } } }`, []string{"atlas.hcl"}, true},
		{"mirrormere core matches cmd", mirrormereDir, `job "mirrormere-core" { task "core" { config { image = "ghcr.io/azylman/mirrormere:${var.image_tag}" } } }`, []string{"cmd/mirrormere/main.go"}, true},
		{"mirrormere core ignores web static assets", mirrormereDir, `job "mirrormere-core" { task "core" { config { image = "ghcr.io/azylman/mirrormere:${var.image_tag}" } } }`, []string{"web/index.html", "widgets/clock/widget.js"}, false},
		{"mirrormere ear matches client ear dir", mirrormereDir, `job "kiosk-ear" { task "ear" { config { image = "ghcr.io/azylman/mirrormere-ear:${var.image_tag}" } } }`, []string{"client/ear/main.py"}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := JobHasContainerBuildChanges(tc.repoPath, tc.jobSpec, tc.files); got != tc.want {
				t.Errorf("JobHasContainerBuildChanges(%q) = %v, want %v", tc.repoPath, got, tc.want)
			}
		})
	}
}

func TestParseNomadGitStatus_TableDriven(t *testing.T) {
	statusOutput := `M	jobs/infisical.nomad
A	jobs/mirrormere-core.nomad
D	jobs/deprecated.nomad
M	docker-compose.yml
R100	jobs/old.nomad	jobs/new.nomad
`
	changes := ParseNomadGitStatus(statusOutput)
	if len(changes) != 5 {
		t.Fatalf("expected 5 nomad changes, got %d: %+v", len(changes), changes)
	}
	if changes[0].Path != "jobs/infisical.nomad" || changes[0].Action != "apply" || changes[0].JobName != "infisical" ||
		changes[2].Path != "jobs/deprecated.nomad" || changes[2].Action != "delete" || changes[2].JobName != "deprecated" ||
		changes[3].Action != "delete" || changes[3].JobName != "old" || changes[4].Action != "apply" || changes[4].JobName != "new" {
		t.Errorf("unexpected changes: %+v", changes)
	}
	if len(ParseNomadGitStatus("")) != 0 || len(ParseNomadGitStatus("M\tdocker-compose.yml\nA\tREADME.md\n")) != 0 {
		t.Errorf("expected 0 changes for empty or non-nomad status")
	}
	varStatus := ParseNomadGitStatus("M\tjobs/eri.vars.hcl\nD\tjobs/vanadis.vars.hcl\nR100\tjobs/old.vars.hcl\tjobs/new.vars.hcl\n")
	if len(varStatus) != 4 || varStatus[0].Action != "apply" || varStatus[0].JobName != "eri" || varStatus[1].Action != "apply" || varStatus[1].JobName != "vanadis" || varStatus[2].Action != "apply" || varStatus[2].JobName != "old" || varStatus[3].Action != "apply" || varStatus[3].JobName != "new" {
		t.Errorf("expected apply actions for .vars.hcl modifications, deletes, and renames, got %+v", varStatus)
	}
	if !IsNomadVarFile("jobs/eri.vars.hcl") || IsNomadVarFile(".vars.hcl") || IsNomadVarFile("jobs/eri.nomad") {
		t.Errorf("unexpected IsNomadVarFile classification")
	}
	if ResolveJobVarFile("", "/cfg") != "" || ResolveJobVarFile("eri", "") != "" || ResolveJobVarFile("eri", "/cfg", func(string) bool { return false }) != "" {
		t.Errorf("expected empty ResolveJobVarFile when inputs empty or not found")
	}
	if got := ResolveJobVarFile("eri", "/cfg", func(p string) bool { return strings.HasSuffix(filepath.ToSlash(p), "jobs/eri.vars.hcl") }); got == "" {
		t.Errorf("expected ResolveJobVarFile to resolve jobs/eri.vars.hcl")
	}
}

func TestHasContainerBuildChanges_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	configDir := filepath.Join(tmpDir, "aerial-config")
	sidecarsDir := filepath.Join(tmpDir, "aerial-sidecars")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")

	seedRepoWorkflowFixture(t, aerialDir)
	seedRepoWorkflowFixture(t, sidecarsDir)
	seedRepoWorkflowFixture(t, mirrormereDir)

	tests := []struct {
		name     string
		repoPath string
		files    []string
		expected bool
	}{
		{"aerial-config without workflows returns false", configDir, []string{"jobs/brain.nomad", "rules/common.md", "Dockerfile"}, false},
		{"aerial core services touched", aerialDir, []string{"services/eri/main.go"}, true},
		{"aerial core mcp touched", aerialDir, []string{"mcp/discord-mcp/main.go"}, true},
		{"aerial core Dockerfile touched", aerialDir, []string{"Dockerfile"}, true},
		{"aerial core pkg touched", aerialDir, []string{"pkg/httpkit/httpkit.go"}, true},
		{"aerial core non-build files", aerialDir, []string{"nomad/jobs/migrate.nomad", "README.md", "scripts/verify.sh", ".github/workflows/docker-publish.yml"}, false},
		{"aerial core empty files", aerialDir, nil, false},
		{"aerial-sidecars service dir touched", sidecarsDir, []string{"vision/app/detector.py"}, true},
		{"aerial-sidecars non-build files", sidecarsDir, []string{"README.md", "nomad/jobs/migrate.nomad"}, false},
		{"mirrormere cmd touched", mirrormereDir, []string{"cmd/mirrormere/main.go"}, true},
		{"mirrormere web non-container paths", mirrormereDir, []string{"web/index.html", "widgets/clock/widget.js"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasContainerBuildChanges(tt.repoPath, tt.files)
			if got != tt.expected {
				t.Errorf("HasContainerBuildChanges(%q, %v) = %v, want %v", tt.repoPath, tt.files, got, tt.expected)
			}
		})
	}
}

func TestImageMatches_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		target   string
		expected bool
	}{
		{"exact match with tag", "ghcr.io/azylman/aerial-brain:latest", "ghcr.io/azylman/aerial-brain:latest", true},
		{"short name matches full ref", "aerial-brain", "ghcr.io/azylman/aerial-brain:latest", true},
		{"full ref matches without tag", "ghcr.io/azylman/aerial-brain:latest", "ghcr.io/azylman/aerial-brain", true},
		{"case insensitive match", "GHCR.IO/AZYLMAN/AERIAL-BRAIN:LATEST", "ghcr.io/azylman/aerial-brain:latest", true},
		{"digest stripped match", "ghcr.io/azylman/aerial-brain@sha256:12345", "ghcr.io/azylman/aerial-brain:latest", true},
		{"different images false", "aerial-hangar", "ghcr.io/azylman/aerial-brain:latest", false},
		{"empty query false", "", "ghcr.io/azylman/aerial-brain:latest", false},
		{"empty target false", "aerial-brain", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ImageMatches(tt.query, tt.target)
			if got != tt.expected {
				t.Errorf("ImageMatches(%q, %q) = %v, want %v", tt.query, tt.target, got, tt.expected)
			}
		})
	}
}

func TestReplaceImageTag_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		imageRef string
		tag      string
		expected string
	}{
		{"replace latest tag", "ghcr.io/azylman/aerial-brain:latest", "sha123", "ghcr.io/azylman/aerial-brain:sha123"},
		{"replace sha256 digest", "ghcr.io/azylman/aerial-brain@sha256:12345", "v2", "ghcr.io/azylman/aerial-brain:v2"},
		{"untagged ref", "ghcr.io/azylman/aerial-brain", "v1.0.0", "ghcr.io/azylman/aerial-brain:v1.0.0"},
		{"registry port ref with tag", "localhost:5000/app:v1", "v2", "localhost:5000/app:v2"},
		{"registry port ref untagged", "localhost:5000/app", "v2", "localhost:5000/app:v2"},
		{"empty imageRef", "", "v1", ""},
		{"whitespace imageRef", "   ", "v1", ""},
		{"empty tag", "ghcr.io/azylman/aerial-brain:latest", "", "ghcr.io/azylman/aerial-brain:latest"},
		{"whitespace tag", "ghcr.io/azylman/aerial-brain:latest", "   ", "ghcr.io/azylman/aerial-brain:latest"},
		{"both empty", "", "", ""},
		{"trimmed inputs", "  ghcr.io/azylman/aerial-brain:latest  ", "  sha999  ", "ghcr.io/azylman/aerial-brain:sha999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReplaceImageTag(tt.imageRef, tt.tag)
			if got != tt.expected {
				t.Errorf("ReplaceImageTag(%q, %q) = %q, want %q", tt.imageRef, tt.tag, got, tt.expected)
			}
		})
	}
}

func TestIsSafeJobPath_TableDriven(t *testing.T) {
	baseDir := "/share/aerial-config/jobs"
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"valid nomad file in base", "/share/aerial-config/jobs/aerial-edge-proxy.nomad", true},
		{"valid nested nomad file", "/share/aerial-config/jobs/edge/webhooks.nomad.hcl", true},
		{"path traversal escape false", "/share/aerial-config/jobs/../secret.nomad", false},
		{"outside directory false", "/etc/passwd", false},
		{"base directory itself false", "/share/aerial-config/jobs", false},
		{"non-nomad file false", "/share/aerial-config/jobs/README.md", false},
		{"empty path false", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSafeJobPath(baseDir, tt.path)
			if got != tt.expected {
				t.Errorf("IsSafeJobPath(%q, %q) = %v, want %v", baseDir, tt.path, got, tt.expected)
			}
		})
	}
}

func TestNormalizeGitHubSlug_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"short name", "aerial", "azylman/aerial"},
		{"owner/repo slug", "azylman/aerial-config", "azylman/aerial-config"},
		{"https github url", "https://github.com/azylman/aerial.git", "azylman/aerial"},
		{"git ssh url", "git@github.com:azylman/mirrormere.git", "azylman/mirrormere"},
		{"local absolute path", "/share/aerial-sidecars", "azylman/aerial-sidecars"},
		{"http github url", "http://github.com/azylman/aerial", "azylman/aerial"},
		{"root slash", "/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeGitHubSlug(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeGitHubSlug(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestResolveRepoPath_TableDriven(t *testing.T) {
	configuredRepos := []string{
		"/share/aerial",
		"/share/aerial-config",
		"/share/mirrormere",
	}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"exact full path", "/share/aerial-config", "/share/aerial-config"},
		{"short name aerial", "aerial", "/share/aerial"},
		{"short name aerial-config", "aerial-config", "/share/aerial-config"},
		{"github slug azylman/mirrormere", "azylman/mirrormere", "/share/mirrormere"},
		{"github url", "https://github.com/azylman/aerial.git", "/share/aerial"},
		{"unknown repo", "unknown-repo", ""},
		{"empty query", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveRepoPath(tt.query, configuredRepos)
			if got != tt.expected {
				t.Errorf("ResolveRepoPath(%q) = %q, want %q", tt.query, got, tt.expected)
			}
		})
	}
}

func TestImageSourceRepo_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	sidecarsDir := filepath.Join(tmpDir, "aerial-sidecars")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")

	seedRepoWorkflowFixture(t, aerialDir)
	seedRepoWorkflowFixture(t, sidecarsDir)
	seedRepoWorkflowFixture(t, mirrormereDir)

	candidateRepos := []string{aerialDir, sidecarsDir, mirrormereDir}

	tests := []struct {
		name           string
		imageRef       string
		expected       string
		wantCatalogRef string
	}{
		{"core aerial synapse", "ghcr.io/azylman/aerial-synapse:latest", "azylman/aerial", "ghcr.io/azylman/aerial-synapse:latest"},
		{"core aerial infisical-mcp", "ghcr.io/azylman/aerial-infisical-mcp:latest", "azylman/aerial", "ghcr.io/azylman/aerial-infisical-mcp:latest"},
		{"core aerial eri", "ghcr.io/azylman/aerial-eri:latest", "azylman/aerial", "ghcr.io/azylman/aerial-eri:latest"},
		{"core service key eri translates to catalog image", "ghcr.io/azylman/eri:latest", "", "ghcr.io/azylman/aerial-eri:latest"},
		{"mirrormere core", "ghcr.io/azylman/mirrormere:latest", "azylman/mirrormere", "ghcr.io/azylman/mirrormere:latest"},
		{"mirrormere ear", "ghcr.io/azylman/mirrormere-ear:latest", "azylman/mirrormere", "ghcr.io/azylman/mirrormere-ear:latest"},
		{"sidecar migrator", "ghcr.io/azylman/aerial-sidecar-migrator:latest", "azylman/aerial-sidecars", "ghcr.io/azylman/aerial-sidecar-migrator:latest"},
		{"voice sidecar", "ghcr.io/azylman/aerial-sidecar-vision:latest", "azylman/aerial-sidecars", "ghcr.io/azylman/aerial-sidecar-vision:latest"},
		{"sidecar service key vision translates to catalog image", "ghcr.io/azylman/vision:latest", "", "ghcr.io/azylman/aerial-sidecar-vision:latest"},
		{"third party go2rtc", "alexxit/go2rtc:1.9.8", "", "alexxit/go2rtc:1.9.8"},
		{"third party redis", "redis:7-alpine", "", "redis:7-alpine"},
		{"empty string", "", "", ""},
		{"azylman other image", "ghcr.io/azylman/other-tool:latest", "", "ghcr.io/azylman/other-tool:latest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveImageSourceRepo(tt.imageRef, candidateRepos)
			if got != tt.expected {
				t.Errorf("ResolveImageSourceRepo(%q) = %q, want %q", tt.imageRef, got, tt.expected)
			}
			if gotRef := ResolveCatalogImageRef(tt.imageRef, candidateRepos); gotRef != tt.wantCatalogRef {
				t.Errorf("ResolveCatalogImageRef(%q) = %q, want %q", tt.imageRef, gotRef, tt.wantCatalogRef)
			}
		})
	}
}

func TestParseActiveNomadJobs_TableDriven(t *testing.T) {
	t.Parallel()
	statusSample := "ID Type Priority Status Submit Date\nsynapse service 50 running 2026-10-31\nwebhooks-router service 50 running 2026-10-30\ncoverage-ingest/periodic-123 batch 50 dead 2026-10-31\ndb-migrate batch 50 dead 2026-10-29\nshort line\n"
	tests := []struct {
		name, input string
		want        []string
	}{
		{"empty output", "", nil},
		{"no running jobs message", "No running jobs\n", nil},
		{"filters header, periodic children, dead jobs, and short lines", statusSample, []string{"synapse", "webhooks-router"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseActiveNomadJobs(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("ParseActiveNomadJobs() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("ParseActiveNomadJobs()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestFindJobDefinitionInRepos(t *testing.T) {
	tempDir := t.TempDir()
	repoA := filepath.Join(tempDir, "repo-a")
	repoB := filepath.Join(tempDir, "repo-b")
	repoEdge := filepath.Join(tempDir, "repo-edge")

	jobsDirA := filepath.Join(repoA, "jobs")
	nomadJobsDirB := filepath.Join(repoB, "nomad", "jobs")
	jobsDirEdge := filepath.Join(repoEdge, "jobs")

	for _, d := range []string{jobsDirA, nomadJobsDirB, jobsDirEdge, filepath.Join(jobsDirEdge, "subdir.nomad")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	jobSpecBrain := `job "brain" { type = "service" }`
	jobSpecPostgres := `job "postgres" { type = "service" }`
	jobSpecOther := `job "other" { type = "service" }`

	_ = os.WriteFile(filepath.Join(jobsDirA, "brain.nomad"), []byte(jobSpecBrain), 0o644)
	_ = os.WriteFile(filepath.Join(nomadJobsDirB, "brain.nomad"), []byte(jobSpecBrain), 0o644)
	_ = os.WriteFile(filepath.Join(nomadJobsDirB, "postgres.nomad"), []byte(jobSpecPostgres), 0o644)
	_ = os.WriteFile(filepath.Join(jobsDirEdge, "notes.txt"), []byte("not a nomad job"), 0o644)
	_ = os.WriteFile(filepath.Join(jobsDirEdge, "other.nomad"), []byte(jobSpecOther), 0o644)

	repos := []string{repoA, repoB}
	edgeRepos := []string{"", repoEdge}

	tests := []struct {
		name         string
		jobName      string
		skipRepoPath string
		candidate    []string
		wantPath     string
		wantFound    bool
	}{
		{"found in repoB when repoA skipped", "brain", repoA, repos, filepath.Join(nomadJobsDirB, "brain.nomad"), true},
		{"not found when repoB skipped", "postgres", repoB, repos, "", false},
		{"empty jobName", "", repoA, repos, "", false},
		{"nil candidateRepos", "brain", "", nil, "", false},
		{"matching other in edge repo with empty entry", "other", "", edgeRepos, filepath.Join(jobsDirEdge, "other.nomad"), true},
		{"target not in edge repo", "target", "", edgeRepos, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotFound := FindJobDefinitionInRepos(tt.jobName, tt.skipRepoPath, tt.candidate)
			if gotFound != tt.wantFound {
				t.Errorf("FindJobDefinitionInRepos() found = %v, want %v", gotFound, tt.wantFound)
			}
			if tt.wantPath != "" && gotPath != tt.wantPath {
				t.Errorf("FindJobDefinitionInRepos() path = %s, want %s", gotPath, tt.wantPath)
			}
		})
	}
}

func TestIsHangarJob_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"hangar", true},
		{"Hangar", true},
		{" HANGAR ", true},
		{"synapse", false},
		{"brain", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHangarJob(tt.name); got != tt.expected {
				t.Errorf("IsHangarJob(%q) = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestIsHangarImage_TableDriven(t *testing.T) {
	tests := []struct {
		image    string
		expected bool
	}{
		{"ghcr.io/azylman/aerial-hangar:latest", true},
		{"aerial-hangar", true},
		{"ghcr.io/azylman/aerial-synapse:latest", false},
		{"brain:latest", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			if got := IsHangarImage(tt.image); got != tt.expected {
				t.Errorf("IsHangarImage(%q) = %v, want %v", tt.image, got, tt.expected)
			}
		})
	}
}

func TestSortNomadChangesHangarLast_TableDriven(t *testing.T) {
	changes := []NomadFileChange{
		{JobName: "hangar", Path: "jobs/hangar.nomad"},
		{JobName: "brain", Path: "jobs/brain.nomad"},
		{JobName: "synapse", Path: "jobs/synapse.nomad"},
	}
	SortNomadChangesHangarLast(changes)
	if changes[0].JobName != "brain" || changes[1].JobName != "synapse" || changes[2].JobName != "hangar" {
		t.Fatalf("unexpected order after SortNomadChangesHangarLast: %+v", changes)
	}
}

func TestSortMatchedJobsHangarLast_TableDriven(t *testing.T) {
	matches := []MatchedNomadJob{
		{JobName: "hangar", JobPath: "jobs/hangar.nomad"},
		{JobName: "haro", JobPath: "jobs/haro.nomad"},
	}
	SortMatchedJobsHangarLast(matches)
	if matches[0].JobName != "haro" || matches[1].JobName != "hangar" {
		t.Fatalf("unexpected order after SortMatchedJobsHangarLast: %+v", matches)
	}
}

func TestHasNomadJobVariable_TableDriven(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		content string
		varName string
		want    bool
	}{
		{"standard double quotes with block", "variable \"image_tag\" {\n  type    = string\n  default = \"latest\"\n}", "image_tag", true},
		{"single quotes with block", "variable 'image_tag' {\n  type = string\n}", "image_tag", true},
		{"bare identifier", "variable image_tag {}", "image_tag", true},
		{"compact brace without space", "variable \"image_tag\"{", "image_tag", true},
		{"indented with trailing comment", "  variable \"image_tag\" { # input parameter", "image_tag", true},
		{"hash commented out variable", "# variable \"image_tag\" {", "image_tag", false},
		{"slash commented out variable", "// variable \"image_tag\" {", "image_tag", false},
		{"different variable name", "variable \"db_password\" {\n  type = string\n}", "image_tag", false},
		{"prefix match does not trigger false positive", "variable \"image_tag_prefix\" {}", "image_tag", false},
		{"empty content", "", "image_tag", false},
		{"empty varName", "variable \"image_tag\" {}", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := HasNomadJobVariable(tc.content, tc.varName)
			if got != tc.want {
				t.Fatalf("HasNomadJobVariable(%q, %q) = %v, want %v", tc.content, tc.varName, got, tc.want)
			}
		})
	}
}

func TestResolveExpectedDeployJobs_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	configDir := filepath.Join(tmpDir, "aerial-config")
	sidecarsDir := filepath.Join(tmpDir, "aerial-sidecars")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	seedRepoWorkflowFixture(t, aerialDir)
	seedRepoWorkflowFixture(t, sidecarsDir)
	seedRepoWorkflowFixture(t, mirrormereDir)
	if err := os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(configDir, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sidecarsDir, "nomad", "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}

	writeJob := func(path, content string) {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeJob(filepath.Join(aerialDir, "nomad", "jobs", "vanadis.nomad"), `job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`)
	writeJob(filepath.Join(aerialDir, "nomad", "jobs", "hangar.nomad"), `job "hangar" { task "hangar" { config { image = "ghcr.io/azylman/aerial-hangar:${var.image_tag}" } } }`)
	writeJob(filepath.Join(aerialDir, "nomad", "jobs", "docs.nomad"), `job "docs" { task "docs" { config { image = "ghcr.io/azylman/aerial-docs:${var.image_tag}" } } }`)
	writeJob(filepath.Join(configDir, "jobs", "eri.nomad"), `job "eri" { task "eri" { config { image = "ghcr.io/azylman/aerial-eri:${var.image_tag}" } } }`)
	writeJob(filepath.Join(configDir, "jobs", "vision.nomad"), `job "vision" { task "vision" { config { image = "ghcr.io/azylman/aerial-sidecar-vision:${var.image_tag}" } } }`)
	writeJob(filepath.Join(configDir, "jobs", "mirrormere-core.nomad"), `job "mirrormere-core" { task "core" { config { image = "ghcr.io/azylman/mirrormere:${var.image_tag}" } } }`)
	writeJob(filepath.Join(configDir, "jobs", "kiosk-ear.nomad"), `job "kiosk-ear" { task "ear" { config { image = "ghcr.io/azylman/mirrormere-ear:${var.image_tag}" } } }`)
	writeJob(filepath.Join(sidecarsDir, "nomad", "jobs", "migrate.nomad"), `job "aerial-sidecars-db-migrate" { task "atlas" { config { image = "ghcr.io/azylman/aerial-sidecar-migrator:latest" } } }`)

	serviceMappings := []ServiceConfigMapping{
		{RelPath: "config.yaml", NomadVar: "nomad/jobs/eri", VarKey: "CONFIG_YAML"},
		{RelPath: "services/homepage/homepage.yaml", NomadVar: "nomad/jobs/homepage", VarKey: "HOMEPAGE_YAML"},
		{RelPath: "services/haro/haro.yaml", NomadVar: "nomad/jobs/haro", VarKey: "CONFIG_YAML"},
		{RelPath: "services/voice/voice.yaml", NomadVar: "nomad/jobs/voice", VarKey: "CONFIG_YAML"},
	}

	cases := []struct {
		name           string
		repoPath       string
		changedFiles   []string
		nomadChanges   []NomadFileChange
		serviceConfigs []ServiceConfigMapping
		candidateRepos []string
		want           []string
	}{
		{
			name:     "empty changes returns nil",
			repoPath: aerialDir,
			want:     nil,
		},
		{
			name:     "nomad changes filters out deletes and blank names and sorts hangar last",
			repoPath: aerialDir,
			nomadChanges: []NomadFileChange{
				{Path: "nomad/jobs/hangar.nomad", Action: "apply", JobName: "hangar"},
				{Path: "nomad/jobs/old.nomad", Action: "delete", JobName: "old-job"},
				{Path: "nomad/jobs/blank.nomad", Action: "apply", JobName: "   "},
				{Path: "nomad/jobs/coven.nomad", Action: "apply", JobName: "coven"},
				{Path: "nomad/jobs/brain.nomad", Action: "apply", JobName: "brain"},
			},
			want: []string{"brain", "coven", "hangar"},
		},
		{
			name:         "migration changes in aerial core adds db-migrate and deduplicates",
			repoPath:     aerialDir,
			changedFiles: []string{"services/eri/pkg/db/migrations/20261023_init.sql"},
			nomadChanges: []NomadFileChange{
				{Path: "nomad/jobs/db-migrate.nomad", Action: "apply", JobName: "db-migrate"},
				{Path: "nomad/jobs/eri.nomad", Action: "apply", JobName: "eri"},
			},
			want: []string{"db-migrate", "eri"},
		},
		{
			name:           "aerial-config variable mappings and nomad changes combined",
			repoPath:       configDir,
			changedFiles:   []string{"config.yaml", "services/homepage/homepage.yaml", "services/voice/voice.yaml"},
			nomadChanges:   []NomadFileChange{{Path: "jobs/hangar.nomad", Action: "apply", JobName: "hangar"}},
			serviceConfigs: serviceMappings,
			want:           []string{"eri", "homepage", "voice", "hangar"},
		},
		{
			name:           "aerial-config homepage widget asset changes resolve homepage in expected jobs",
			repoPath:       configDir,
			changedFiles:   []string{"config.yaml", "homepage/widgets.yaml", "services/voice/voice.yaml"},
			nomadChanges:   []NomadFileChange{{Path: "jobs/hangar.nomad", Action: "apply", JobName: "hangar"}},
			serviceConfigs: serviceMappings,
			want:           []string{"eri", "homepage", "voice", "hangar"},
		},
		{
			name:           "asset bundle change without mapped service config does not expect deploy job",
			repoPath:       configDir,
			changedFiles:   []string{"docs/index.html", "services/grafana/dashboards/overview.json"},
			serviceConfigs: serviceMappings,
			want:           nil,
		},
		{
			name:           "container build changes across repoPath and candidateRepos with hangar sorted last",
			repoPath:       aerialDir,
			changedFiles:   []string{"services/hangar/main.go", "services/vanadis/main.go", "services/eri/main.go"},
			candidateRepos: []string{configDir},
			want:           []string{"eri", "vanadis", "hangar"},
		},
		{
			name:           "shared pkg changes deploy only pkg dependent services and exclude non pkg services",
			repoPath:       aerialDir,
			changedFiles:   []string{"pkg/httpkit/httpkit.go"},
			candidateRepos: []string{configDir},
			want:           []string{"eri", "vanadis", "hangar"},
		},
		{
			name:           "aerial-sidecars vision and migrator changes resolve jobs across sidecars and config repos",
			repoPath:       sidecarsDir,
			changedFiles:   []string{"vision/app/detector.py", "atlas.hcl"},
			candidateRepos: []string{aerialDir, configDir},
			want:           []string{"aerial-sidecars-db-migrate", "vision"},
		},
		{
			name:           "mirrormere root Dockerfile only matches mirrormere jobs and not aerial jobs",
			repoPath:       mirrormereDir,
			changedFiles:   []string{"Dockerfile", "client/ear/main.py"},
			candidateRepos: []string{aerialDir, configDir},
			want:           []string{"kiosk-ear", "mirrormere-core"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveExpectedDeployJobs(tc.repoPath, tc.changedFiles, tc.nomadChanges, tc.serviceConfigs, tc.candidateRepos, testAssetBundles(aerialDir, configDir, mirrormereDir))
			if len(got) != len(tc.want) {
				t.Fatalf("ResolveExpectedDeployJobs() = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("ResolveExpectedDeployJobs()[%d] = %q, want %q (full: %v)", i, got[i], tc.want[i], got)
				}
			}
		})
	}
}

func TestParseWorkflowBuildRules_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		yaml        string
		wantFilters map[string][]string
		wantCatalog map[string]string
	}{
		{
			name:        "empty input",
			yaml:        "",
			wantFilters: map[string][]string{},
			wantCatalog: map[string]string{},
		},
		{
			name: "valid dorny filter and js catalog",
			yaml: `
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v4
        id: filter
        with:
          filters: |
            eri:
              - 'services/eri/**'
              - 'pkg/**'
            docs:
              - 'services/docs-service/**'
      - name: Compute Dynamic Build Matrix
        run: |
          const catalog = [
            { service: "eri", image_name: "aerial-eri" },
            { service: "docs", image_name: "aerial-docs" }
          ];
`,
			wantFilters: map[string][]string{
				"eri":  {"services/eri/**", "pkg/**"},
				"docs": {"services/docs-service/**"},
			},
			wantCatalog: map[string]string{
				"eri":  "aerial-eri",
				"docs": "aerial-docs",
			},
		},
		{
			name: "single glob in filter and multiline object in catalog",
			yaml: `
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v3
        with:
          filters: |
            app:
              - 'src/**'
      - run: |
          const catalog = [
            {
              service: "app",
              context: "./src",
              image_name: "my-app"
            }
          ];
`,
			wantFilters: map[string][]string{
				"app": {"src/**"},
			},
			wantCatalog: map[string]string{
				"app": "my-app",
			},
		},
		{
			name: "ignored catalog entries and multiple filters",
			yaml: `
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v4
        with:
          filters: |
            app:
              - 'services/app/**'
      - run: |
          const catalog = [
            { service: "", image_name: "" },
            { dummy: true }
          ];
`,
			wantFilters: map[string][]string{
				"app": {"services/app/**"},
			},
			wantCatalog: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			filters, catalog := ParseWorkflowBuildRules([]byte(tt.yaml))
			if len(filters) != len(tt.wantFilters) {
				t.Fatalf("ParseWorkflowBuildRules() filters len = %d, want %d", len(filters), len(tt.wantFilters))
			}
			for k, wantGlobs := range tt.wantFilters {
				gotGlobs := filters[k]
				if len(gotGlobs) != len(wantGlobs) {
					t.Fatalf("filters[%q] len = %d, want %d", k, len(gotGlobs), len(wantGlobs))
				}
				for i := range wantGlobs {
					if gotGlobs[i] != wantGlobs[i] {
						t.Errorf("filters[%q][%d] = %q, want %q", k, i, gotGlobs[i], wantGlobs[i])
					}
				}
			}
			if len(catalog) != len(tt.wantCatalog) {
				t.Fatalf("ParseWorkflowBuildRules() catalog len = %d, want %d", len(catalog), len(tt.wantCatalog))
			}
			for k, wantImg := range tt.wantCatalog {
				if catalog[k] != wantImg {
					t.Errorf("catalog[%q] = %q, want %q", k, catalog[k], wantImg)
				}
			}
		})
	}
}

func TestResolveAssetBundleSources_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bundle      string
		composeDir  string
		configDir   string
		mirrormere  string
		wantSources int
		wantExcl    int
		wantErr     bool
	}{
		{"migrations bundle", "migrations", "/compose", "/config", "/mirrormere", 1, 0, false},
		{"migrations bundle with ext", "migrations.tar.gz", "/compose", "/config", "/mirrormere", 1, 0, false},
		{"dashboard bundle", "dashboard", "/compose", "/config", "/mirrormere", 1, 0, false},
		{"dashboard bundle with slashes", "/dashboard.tar.gz/", "/compose", "/config", "/mirrormere", 1, 0, false},
		{"docs bundle", "docs", "/compose", "/config", "/mirrormere", 2, 0, false},
		{"grafana bundle", "grafana", "/compose", "/config", "/mirrormere", 2, 0, false},
		{"homepage bundle", "homepage", "/compose", "/config", "/mirrormere", 2, 1, false},
		{"mirrormere bundle", "mirrormere", "/compose", "/config", "/mirrormere", 2, 0, false},
		{"unknown bundle", "invalid-bundle", "/compose", "/config", "/mirrormere", 0, 0, true},
		{"empty bundle", "", "/compose", "/config", "/mirrormere", 0, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sources, excl, err := ResolveAssetBundleSources(tc.bundle, testAssetBundles(tc.composeDir, tc.configDir, tc.mirrormere))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveAssetBundleSources(%q) err = %v, wantErr = %v", tc.bundle, err, tc.wantErr)
			}
			if len(sources) != tc.wantSources {
				t.Errorf("ResolveAssetBundleSources(%q) len(sources) = %d, want %d", tc.bundle, len(sources), tc.wantSources)
			}
			if len(excl) != tc.wantExcl {
				t.Errorf("ResolveAssetBundleSources(%q) len(excl) = %d, want %d", tc.bundle, len(excl), tc.wantExcl)
			}
		})
	}
}

func TestBuildAssetBundleTarGz_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	src1 := filepath.Join(tmpDir, "src1")
	src2 := filepath.Join(tmpDir, "src2")
	_ = os.MkdirAll(filepath.Join(src1, "sub"), 0o755)
	_ = os.MkdirAll(filepath.Join(src1, "test"), 0o755)
	_ = os.MkdirAll(src2, 0o755)

	_ = os.WriteFile(filepath.Join(src1, "hello.txt"), []byte("hello"), 0o644)
	_ = os.WriteFile(filepath.Join(src1, "run.sh"), []byte("#!/bin/sh\necho hi"), 0o755)
	_ = os.WriteFile(filepath.Join(src1, "sub", "data.json"), []byte("{}"), 0o644)
	_ = os.WriteFile(filepath.Join(src1, "test", "ignore.js"), []byte("test"), 0o644)
	_ = os.WriteFile(filepath.Join(src1, ".hidden"), []byte("skip"), 0o644)
	_ = os.WriteFile(filepath.Join(src1, "scratch.tmp"), []byte("skip"), 0o644)
	_ = os.MkdirAll(filepath.Join(src1, ".git"), 0o755)
	_ = os.WriteFile(filepath.Join(src1, ".git", "config"), []byte("skip"), 0o644)
	_ = os.MkdirAll(filepath.Join(src1, ".hidden_dir"), 0o755)
	_ = os.WriteFile(filepath.Join(src1, ".hidden_dir", "file"), []byte("skip"), 0o644)
	_ = os.MkdirAll(filepath.Join(src1, "build.tmp"), 0o755)
	_ = os.WriteFile(filepath.Join(src1, "build.tmp", "file"), []byte("skip"), 0o644)
	fileAsSrc := filepath.Join(tmpDir, "regular_file.txt")
	_ = os.WriteFile(fileAsSrc, []byte("regular file"), 0o644)
	_ = os.WriteFile(filepath.Join(src2, "world.txt"), []byte("world"), 0o644)

	tests := []struct {
		name          string
		sources       map[string]string
		excludeRel    []string
		wantFiles     []string
		wantExec      []string
		dontWantFiles []string
		wantErr       bool
	}{
		{
			name: "multi source with prefix and exclude",
			sources: map[string]string{
				"prefix1": src1,
				"prefix2": src2,
			},
			excludeRel: []string{"prefix1/test"},
			wantFiles: []string{
				"prefix1/",
				"prefix1/hello.txt",
				"prefix1/run.sh",
				"prefix1/sub/",
				"prefix1/sub/data.json",
				"prefix2/",
				"prefix2/world.txt",
			},
			wantExec: []string{"prefix1/run.sh"},
			dontWantFiles: []string{
				"prefix1/test/ignore.js",
				"prefix1/.hidden",
				"prefix1/scratch.tmp",
				"prefix1/.git",
				"prefix1/.git/config",
				"prefix1/.hidden_dir",
				"prefix1/build.tmp",
			},
			wantErr: false,
		},
		{
			name: "root prefix with dot",
			sources: map[string]string{
				".": src2,
			},
			excludeRel: nil,
			wantFiles: []string{
				"world.txt",
			},
			dontWantFiles: nil,
			wantErr:       false,
		},
		{
			name: "missing source directory emits prefix dir",
			sources: map[string]string{
				"absent": filepath.Join(tmpDir, "does-not-exist"),
			},
			excludeRel: nil,
			wantFiles: []string{
				"absent/",
			},
			dontWantFiles: nil,
			wantErr:       false,
		},
		{
			name: "file source path is skipped",
			sources: map[string]string{
				"file_src": fileAsSrc,
			},
			excludeRel: nil,
			wantFiles: []string{
				"file_src/",
			},
			dontWantFiles: nil,
			wantErr:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, hash, err := BuildAssetBundleTarGz(tc.sources, tc.excludeRel...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("BuildAssetBundleTarGz() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if hash == "" {
				t.Errorf("expected non-empty hash")
			}

			gr, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("gzip reader err = %v", err)
			}
			defer gr.Close()

			tr := tar.NewReader(gr)
			seen := make(map[string]tar.Header)
			for {
				hdr, trErr := tr.Next()
				if trErr == io.EOF {
					break
				}
				if trErr != nil {
					t.Fatalf("tar next err = %v", trErr)
				}
				seen[hdr.Name] = *hdr
			}

			for _, wf := range tc.wantFiles {
				if _, ok := seen[wf]; !ok {
					t.Errorf("missing expected archive file: %s (seen: %v)", wf, seen)
				}
			}

			for _, dwf := range tc.dontWantFiles {
				if _, ok := seen[dwf]; ok {
					t.Errorf("unexpected file present in archive: %s", dwf)
				}
			}

			for _, ef := range tc.wantExec {
				hdr, ok := seen[ef]
				if !ok {
					continue
				}
				if hdr.Mode != 0o755 {
					t.Errorf("expected file %s mode 0755, got %o", ef, hdr.Mode)
				}
			}
		})
	}
}

func TestAssetBundleMatchesRepoChanges_TableDriven(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	bundles := testAssetBundles("", "", "")
	byName := make(map[string]AssetBundleConfig, len(bundles))
	for _, b := range bundles {
		byName[b.Name] = b
	}

	tests := []struct {
		name     string
		bundle   AssetBundleConfig
		repoPath string
		files    []string
		want     bool
	}{
		{"dashboard static change", byName["dashboard"], "", []string{"services/dashboard/static/app.js"}, true},
		{"dashboard job change", byName["dashboard"], "", []string{"nomad/jobs/dashboard.nomad"}, true},
		{"dashboard unrelated go change", byName["dashboard"], "", []string{"services/dashboard/main.go"}, false},
		{"docs markdown change", byName["docs"], "", []string{"docs/index.md"}, true},
		{"grafana dashboard change", byName["grafana"], "", []string{"nomad/grafana/dashboards/sys.json"}, true},
		{"homepage core change", byName["homepage"], "", []string{"services/homepage/entrypoint.sh"}, true},
		{"homepage test directory excluded", byName["homepage"], "", []string{"services/homepage/test/prepare-config.test.js"}, false},
		{"mirrormere widgets change", byName["mirrormere"], "", []string{"widgets/weather.js"}, true},
		{"migrations sql change", byName["migrations"], "", []string{"services/eri/pkg/db/migrations/001_init.sql"}, true},
		{"migrations job change", byName["migrations"], "", []string{"nomad/jobs/db-migrate.nomad"}, true},
		{"repo-scoped source prefix match", AssetBundleConfig{Name: "rel", Sources: map[string]string{"app": "services/my-app"}}, tmpDir, []string{"services/my-app/index.html"}, true},
		{"repo-scoped excluded path", AssetBundleConfig{Name: "excl", Sources: map[string]string{"app": "services/my-app"}, Excludes: []string{"services/my-app/test"}}, tmpDir, []string{"services/my-app/test/spec.js"}, false},
		{"repo-scoped unrelated change", AssetBundleConfig{Name: "unrel", Sources: map[string]string{"app": "services/my-app"}}, tmpDir, []string{"services/other/main.go", ""}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AssetBundleMatchesRepoChanges(tc.bundle, tc.repoPath, tc.files); got != tc.want {
				t.Errorf("AssetBundleMatchesRepoChanges() = %v, want %v", got, tc.want)
			}
		})
	}
}
