package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
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

	tests := []struct {
		name         string
		existingJSON string
		registry     string
		username     string
		pat          string
		validate     func(t *testing.T, out []byte, err error)
	}{
		{
			name:         "empty pat returns existing content without error",
			existingJSON: `{"credsStore":"desktop"}`,
			registry:     "ghcr.io",
			username:     "azylman",
			pat:          "",
			validate: func(t *testing.T, out []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if string(out) != `{"credsStore":"desktop"}` {
					t.Errorf("expected unchanged content, got: %s", string(out))
				}
			},
		},
		{
			name:         "whitespace pat returns existing content without error",
			existingJSON: `{"credsStore":"desktop"}`,
			registry:     "ghcr.io",
			username:     "azylman",
			pat:          "   \t\n  ",
			validate: func(t *testing.T, out []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if string(out) != `{"credsStore":"desktop"}` {
					t.Errorf("expected unchanged content, got: %s", string(out))
				}
			},
		},
		{
			name:         "fresh generation with default registry and username",
			existingJSON: "",
			registry:     "",
			username:     "",
			pat:          dummyPAT,
			validate: func(t *testing.T, out []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				var parsed struct {
					Auths map[string]struct {
						Auth string `json:"auth"`
					} `json:"auths"`
				}
				if err := json.Unmarshal(out, &parsed); err != nil {
					t.Fatalf("failed to unmarshal generated json: %v", err)
				}
				authEntry, ok := parsed.Auths["ghcr.io"]
				if !ok {
					t.Fatalf("expected ghcr.io in auths map")
				}
				expectedAuth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + dummyPAT))
				if authEntry.Auth != expectedAuth {
					t.Errorf("auth mismatch: got %q, want %q", authEntry.Auth, expectedAuth)
				}
			},
		},
		{
			name:         "merges into existing config preserving other registries and top-level fields",
			existingJSON: `{"credsStore":"pass","auths":{"docker.io":{"auth":"ZG9ja2VydXNlcjpkb2NrZXJwYXNz"}}}`,
			registry:     "ghcr.io",
			username:     "custom-user",
			pat:          dummyPAT,
			validate: func(t *testing.T, out []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				var parsed map[string]any
				if err := json.Unmarshal(out, &parsed); err != nil {
					t.Fatalf("failed to unmarshal output: %v", err)
				}
				if parsed["credsStore"] != "pass" {
					t.Errorf("expected credsStore preserved, got %v", parsed["credsStore"])
				}
				auths, ok := parsed["auths"].(map[string]any)
				if !ok {
					t.Fatalf("expected auths map")
				}
				if _, ok := auths["docker.io"]; !ok {
					t.Errorf("expected docker.io preserved in auths")
				}
				ghcrEntry, ok := auths["ghcr.io"].(map[string]any)
				if !ok {
					t.Fatalf("expected ghcr.io entry")
				}
				expectedAuth := base64.StdEncoding.EncodeToString([]byte("custom-user:" + dummyPAT))
				if ghcrEntry["auth"] != expectedAuth {
					t.Errorf("ghcr auth mismatch: got %v, want %s", ghcrEntry["auth"], expectedAuth)
				}
			},
		},
		{
			name:         "recovers from malformed existing JSON cleanly",
			existingJSON: `{invalid-json-content`,
			registry:     "ghcr.io",
			username:     "azylman",
			pat:          dummyPAT,
			validate: func(t *testing.T, out []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error on malformed recovery: %v", err)
				}
				var parsed map[string]any
				if err := json.Unmarshal(out, &parsed); err != nil {
					t.Fatalf("failed to unmarshal output: %v", err)
				}
				auths, ok := parsed["auths"].(map[string]any)
				if !ok {
					t.Fatalf("expected auths map after recovery")
				}
				if _, ok := auths["ghcr.io"]; !ok {
					t.Errorf("expected ghcr.io entry present after recovery")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := GenerateDockerConfig([]byte(tt.existingJSON), tt.registry, tt.username, tt.pat)
			tt.validate(t, out, err)
		})
	}
}

func TestResolveRegistryUser_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		repoURL  string
		lookup   func(string) string
		expected string
	}{
		{
			name:    "DOCKER_REGISTRY_USER takes highest priority",
			repoURL: "https://github.com/azylman/aerial-config.git",
			lookup: func(k string) string {
				switch k {
				case "DOCKER_REGISTRY_USER":
					return "explicit-user"
				case "GITHUB_ACTOR":
					return "actor-user"
				default:
					return ""
				}
			},
			expected: "explicit-user",
		},
		{
			name:    "GITHUB_ACTOR takes priority over repo URL parsing",
			repoURL: "https://github.com/azylman/aerial-config.git",
			lookup: func(k string) string {
				if k == "GITHUB_ACTOR" {
					return "actor-user"
				}
				return ""
			},
			expected: "actor-user",
		},
		{
			name:     "parses owner from https github URL",
			repoURL:  "https://github.com/my-org/my-repo.git",
			lookup:   func(string) string { return "" },
			expected: "my-org",
		},
		{
			name:     "parses owner from https URL without .git suffix",
			repoURL:  "https://github.com/custom-owner/custom-repo",
			lookup:   func(string) string { return "" },
			expected: "custom-owner",
		},
		{
			name:     "parses owner from git@ ssh URL",
			repoURL:  "git@github.com:ssh-owner/ssh-repo.git",
			lookup:   func(string) string { return "" },
			expected: "ssh-owner",
		},
		{
			name:     "nil lookup with unparseable URL defaults to x-access-token",
			repoURL:  "invalid-url-without-slashes",
			lookup:   nil,
			expected: "x-access-token",
		},
		{
			name:     "empty lookup and empty URL defaults to x-access-token",
			repoURL:  "",
			lookup:   func(string) string { return "" },
			expected: "x-access-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveRegistryUser(tt.repoURL, tt.lookup)
			if got != tt.expected {
				t.Errorf("ResolveRegistryUser() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestResolveDockerConfigPath_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		lookup   func(string) string
		expected string
	}{
		{
			name: "DOCKER_CONFIG takes precedence",
			lookup: func(k string) string {
				switch k {
				case "DOCKER_CONFIG":
					return "/custom/docker/cfg"
				case "HOME":
					return "/home/user"
				default:
					return ""
				}
			},
			expected: "/custom/docker/cfg/config.json",
		},
		{
			name: "HOME is used when DOCKER_CONFIG is unset",
			lookup: func(k string) string {
				if k == "HOME" {
					return "/home/user"
				}
				return ""
			},
			expected: "/home/user/.docker/config.json",
		},
		{
			name:     "defaults to /root/.docker/config.json when lookup is nil",
			lookup:   nil,
			expected: "/root/.docker/config.json",
		},
		{
			name:     "defaults to /root/.docker/config.json when env is empty",
			lookup:   func(string) string { return "" },
			expected: "/root/.docker/config.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveDockerConfigPath(tt.lookup)
			if filepath.ToSlash(got) != tt.expected {
				t.Errorf("ResolveDockerConfigPath() = %q, want %q", filepath.ToSlash(got), tt.expected)
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

func TestJobHasContainerBuildChanges_TableDriven(t *testing.T) {
	specVanadis := `job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`
	specProspera := `job "prospera" { task "prospera" { config { image = "ghcr.io/azylman/aerial-prospera:${var.image_tag}" } } }`
	specDocs := `job "docs" { task "docs" { config { image = "ghcr.io/azylman/aerial-docs:${var.image_tag}" } } }`
	specMCP := `job "discord-mcp" { task "mcp" { config { image = "ghcr.io/azylman/aerial-discord-mcp:${var.image_tag}" } } }`
	specExternal := `job "coverage-ingest" { task "flush" { config { image = "golang:1.27.1-alpine" } } }`

	tests := []struct {
		name    string
		jobSpec string
		files   []string
		want    bool
	}{
		{"matching services dir", specVanadis, []string{"services/vanadis/main.go"}, true},
		{"matching prospera service dir", specProspera, []string{"services/prospera/main.go"}, true},
		{"matching legacy root dir", specVanadis, []string{"vanadis/Dockerfile"}, false},
		{"matching mcp dir", specMCP, []string{"mcp/discord-mcp/main.go"}, true},
		{"matching kernel-mcp dir", `job "kernel-mcp" { task "kernel-mcp" { config { image = "ghcr.io/azylman/aerial-kernel-mcp:${var.image_tag}" } } }`, []string{"mcp/kernel-mcp/main.go"}, true},
		{"matching docs-service alias", specDocs, []string{"services/docs-service/Dockerfile"}, true},
		{"matching root Dockerfile", specVanadis, []string{"Dockerfile"}, true},
		{"matching pkg dir", specVanadis, []string{"pkg/httpkit/httpkit.go"}, true},
		{"matching pkg dir on prospera", specProspera, []string{"pkg/httpkit/httpkit.go"}, true},
		{"matching pkg dir on mcp go service", specMCP, []string{"pkg/ollama/client.go"}, true},
		{"matching pkg dir on kernel-mcp", `job "kernel-mcp" { task "kernel-mcp" { config { image = "ghcr.io/azylman/aerial-kernel-mcp:${var.image_tag}" } } }`, []string{"pkg/httpkit/httpkit.go"}, true},
		{"pkg dir does not match non-pkg service", specDocs, []string{"pkg/httpkit/httpkit.go"}, false},
		{"unmatched other service dir", specVanadis, []string{"services/hangar/main.go", "nomad/jobs/vanadis.nomad"}, false},
		{"external image never matches", specExternal, []string{"services/vanadis/main.go", "Dockerfile"}, false},
		{"empty changed files", specVanadis, nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := JobHasContainerBuildChanges(tc.jobSpec, tc.files); got != tc.want {
				t.Errorf("JobHasContainerBuildChanges() = %v, want %v", got, tc.want)
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

	if changes[0].Path != "jobs/infisical.nomad" || changes[0].Action != "apply" || changes[0].JobName != "infisical" {
		t.Errorf("unexpected change[0]: %+v", changes[0])
	}
	if changes[1].Path != "jobs/mirrormere-core.nomad" || changes[1].Action != "apply" || changes[1].JobName != "mirrormere-core" {
		t.Errorf("unexpected change[1]: %+v", changes[1])
	}
	if changes[2].Path != "jobs/deprecated.nomad" || changes[2].Action != "delete" || changes[2].JobName != "deprecated" {
		t.Errorf("unexpected change[2]: %+v", changes[2])
	}
	if changes[3].Path != "jobs/old.nomad" || changes[3].Action != "delete" || changes[3].JobName != "old" {
		t.Errorf("unexpected change[3]: %+v", changes[3])
	}
	if changes[4].Path != "jobs/new.nomad" || changes[4].Action != "apply" || changes[4].JobName != "new" {
		t.Errorf("unexpected change[4]: %+v", changes[4])
	}

	// Additional ParseNomadGitStatus cases
	emptyStatus := ParseNomadGitStatus("")
	if len(emptyStatus) != 0 {
		t.Errorf("expected 0 changes for empty status")
	}

	renameStatus := ParseNomadGitStatus("R090\tjobs/old.nomad\tjobs/renamed.nomad\n")
	if len(renameStatus) != 2 ||
		renameStatus[0].Action != "delete" || renameStatus[0].JobName != "old" ||
		renameStatus[1].Action != "apply" || renameStatus[1].JobName != "renamed" {
		t.Errorf("expected 2 changes (delete old, apply renamed) for rename status, got %+v", renameStatus)
	}

	nonNomadStatus := ParseNomadGitStatus("M\tdocker-compose.yml\nA\tREADME.md\n")
	if len(nonNomadStatus) != 0 {
		t.Errorf("expected 0 changes for non-nomad files")
	}

	migrationStatus := ParseNomadGitStatus("A\tbrain/pkg/db/migrations/20261016182900_add_vanadis_chunk_started_at.sql\nM\tbrain/pkg/db/migrations/atlas.sum\nM\tbrain/pkg/db/schema.sql\n")
	if len(migrationStatus) != 1 || migrationStatus[0].JobName != "db-migrate" || migrationStatus[0].Path != "nomad/jobs/db-migrate.nomad" || migrationStatus[0].Action != "apply" {
		t.Errorf("expected db-migrate change when migration files modified, got %+v", migrationStatus)
	}
	if len(ParseNomadGitStatus("D\tbrain/pkg/db/migrations/old.sql\n")) != 0 {
		t.Errorf("expected 0 changes when migration file is deleted only")
	}
	if !IsMigrationFile("brain/pkg/db/migrations/20261016_init.sql") || !IsMigrationFile("/brain/pkg/db/migrations/atlas.sum") || IsMigrationFile("brain/pkg/db/schema.sql") || IsMigrationFile("") || IsMigrationFile("brain/pkg/db/migrations") {
		t.Errorf("unexpected IsMigrationFile classification for brain")
	}
	if !IsMigrationFile("services/eri/pkg/db/migrations/20261021_init.sql") || !IsMigrationFile("/services/eri/pkg/db/migrations/atlas.sum") || IsMigrationFile("services/eri/pkg/db/schema.sql") || IsMigrationFile("services/eri/pkg/db/migrations") {
		t.Errorf("unexpected IsMigrationFile classification for services/eri")
	}
	if !IsMigrationFile("eri/pkg/db/migrations/20261021_init.sql") || !IsMigrationFile("/eri/pkg/db/migrations/atlas.sum") || IsMigrationFile("eri/pkg/db/schema.sql") || IsMigrationFile("eri/pkg/db/migrations") {
		t.Errorf("unexpected IsMigrationFile classification for eri")
	}
	if !HasMigrationChanges([]string{"README.md", "brain/pkg/db/migrations/20261016_init.sql"}) || HasMigrationChanges([]string{"brain/pkg/db/schema.sql"}) {
		t.Errorf("unexpected HasMigrationChanges result for brain")
	}
	if !HasMigrationChanges([]string{"README.md", "services/eri/pkg/db/migrations/20261021_init.sql"}) || !HasMigrationChanges([]string{"eri/pkg/db/migrations/20261021_init.sql"}) {
		t.Errorf("unexpected HasMigrationChanges result for services/eri and eri")
	}
	if ren := ParseNomadGitStatus("R100\tbrain/pkg/db/old.sql\tbrain/pkg/db/migrations/20261016_new.sql\n"); len(ren) != 1 || ren[0].JobName != "db-migrate" {
		t.Errorf("expected db-migrate when file renamed into migrations dir, got %+v", ren)
	}
	ensured := EnsureDBMigrateNomadChange([]NomadFileChange{{Path: "nomad/jobs/vanadis.nomad", Action: "apply", JobName: "vanadis"}, {Path: "nomad/jobs/db-migrate.nomad", Action: "apply", JobName: "db-migrate"}})
	if len(ensured) != 2 || ensured[0].JobName != "db-migrate" || ensured[1].JobName != "vanadis" || len(EnsureDBMigrateNomadChange(ensured)) != 2 {
		t.Errorf("expected db-migrate moved to index 0 and idempotent, got %+v", ensured)
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
	tests := []struct {
		name     string
		repoPath string
		files    []string
		expected bool
	}{
		{"aerial-config always false", "/share/aerial-config", []string{"jobs/brain.nomad", "rules/common.md", "Dockerfile"}, false},
		{"aerial core legacy brain touched returns false", "/share/aerial", []string{"brain/main.go"}, false},
		{"aerial core services touched", "/share/aerial", []string{"services/coven/main.go"}, true},
		{"aerial core mcp touched", "/share/aerial", []string{"mcp/github-mcp/main.go"}, true},
		{"aerial core legacy coven touched returns false", "/share/aerial", []string{"coven/main.go"}, false},
		{"aerial core eri touched", "/share/aerial", []string{"services/eri/main.go"}, true},
		{"aerial core legacy webhooks-router touched returns false", "/share/aerial", []string{"webhooks-router/main.go"}, false},
		{"aerial core legacy vanadis touched returns false", "/share/aerial", []string{"vanadis/main.go"}, false},
		{"aerial core legacy roborev-runner touched returns false", "/share/aerial", []string{"roborev-runner/Dockerfile"}, false},
		{"aerial core Dockerfile touched", "/share/aerial", []string{"Dockerfile"}, true},
		{"aerial core go.mod touched", "/share/aerial", []string{"go.mod"}, true},
		{"aerial core pkg touched", "/share/aerial", []string{"pkg/httpkit/httpkit.go"}, true},
		{"aerial core scripts touched returns false", "/share/aerial", []string{"scripts/verify.sh"}, false},
		{"aerial core rules touched returns false", "/share/aerial", []string{"rules/discord/foo.md"}, false},
		{"aerial core .agents touched returns false", "/share/aerial", []string{".agents/skills/self-improvement/SKILL.md"}, false},
		{"aerial core docker publish workflow touched returns false", "/share/aerial", []string{".github/workflows/docker-publish.yml"}, false},
		{"aerial core non-build files (docs, nomad, readme)", "/share/aerial", []string{"nomad/jobs/migrate.nomad", "README.md", "docs/spec.md"}, false},
		{"aerial core empty files", "/share/aerial", nil, false},
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
	tests := []struct {
		name     string
		imageRef string
		expected string
	}{
		{"core aerial webhooks-router", "ghcr.io/azylman/aerial-webhooks-router:latest", "azylman/aerial"},
		{"core aerial infisical-mcp", "ghcr.io/azylman/aerial-infisical-mcp:latest", "azylman/aerial"},
		{"core aerial brain", "ghcr.io/azylman/aerial-brain:latest", "azylman/aerial"},
		{"mirrormere core", "ghcr.io/azylman/mirrormere:latest", "azylman/mirrormere"},
		{"mirrormere voice fingerprinter", "ghcr.io/azylman/mirrormere-voice-fingerprinter:latest", "azylman/mirrormere"},
		{"sidecar banana", "ghcr.io/azylman/aerial-sidecar-banana:latest", "azylman/aerial-sidecars"},
		{"voice sidecar", "ghcr.io/azylman/aerial-sidecar-voice-cuda:latest", "azylman/aerial-sidecars"},
		{"third party go2rtc", "alexxit/go2rtc:1.9.8", ""},
		{"third party redis", "redis:7-alpine", ""},
		{"empty string", "", ""},
		{"aerial suffix", "ghcr.io/someone/aerial", "azylman/aerial"},
		{"aerial with tag", "ghcr.io/someone/aerial:v1.0", "azylman/aerial"},
		{"azylman other image", "ghcr.io/azylman/other-tool:latest", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ImageSourceRepo(tt.imageRef)
			if got != tt.expected {
				t.Errorf("ImageSourceRepo(%q) = %q, want %q", tt.imageRef, got, tt.expected)
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
		{"webhooks-router", false},
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
		{"ghcr.io/azylman/aerial-webhooks-router:latest", false},
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
		{JobName: "webhooks-router", Path: "jobs/webhooks-router.nomad"},
	}
	SortNomadChangesHangarLast(changes)
	if changes[0].JobName != "brain" || changes[1].JobName != "webhooks-router" || changes[2].JobName != "hangar" {
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

func TestIsValidGitCommitSHA_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		sha  string
		want bool
	}{
		{name: "valid 7-char short SHA", sha: "a1b2c3d", want: true},
		{name: "valid 40-char full SHA", sha: "0123456789abcdef0123456789abcdef01234567", want: true},
		{name: "valid uppercase hex", sha: "A1B2C3D4E5F67890", want: true},
		{name: "valid mixed case hex", sha: "AbCdEf01234567", want: true},
		{name: "invalid non-hex character", sha: "a1b2c3g", want: false},
		{name: "invalid punctuation", sha: "a1b2c3-", want: false},
		{name: "too short 6-chars", sha: "123456", want: false},
		{name: "too short 1-char", sha: "a", want: false},
		{name: "too long 41-chars", sha: "0123456789abcdef0123456789abcdef012345678", want: false},
		{name: "empty string", sha: "", want: false},
		{name: "whitespace only", sha: "       ", want: false},
		{name: "spaces around valid SHA", sha: " a1b2c3d ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidGitCommitSHA(tt.sha)
			if got != tt.want {
				t.Errorf("IsValidGitCommitSHA(%q) = %v, want %v", tt.sha, got, tt.want)
			}
		})
	}
}

func TestExtractRevisionFromAnnotations_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{"org.opencontainers.image.revision", map[string]string{"org.opencontainers.image.revision": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}, "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"},
		{"aerial.commit_sha", map[string]string{"aerial.commit_sha": "0123456789abcdef0123456789abcdef01234567"}, "0123456789abcdef0123456789abcdef01234567"},
		{"org.label-schema.vcs-ref", map[string]string{"org.label-schema.vcs-ref": "abcdef0123456"}, "abcdef0123456"},
		{"precedence: OCI revision over aerial.commit_sha", map[string]string{"org.opencontainers.image.revision": "1111111111111111111111111111111111111111", "aerial.commit_sha": "2222222222222222222222222222222222222222"}, "1111111111111111111111111111111111111111"},
		{"precedence: aerial.commit_sha over label-schema", map[string]string{"aerial.commit_sha": "2222222222222222222222222222222222222222", "org.label-schema.vcs-ref": "3333333333333333333333333333333333333333"}, "2222222222222222222222222222222222222222"},
		{"fallback to next key when earlier key has invalid SHA", map[string]string{"org.opencontainers.image.revision": "invalid-sha", "aerial.commit_sha": "4444444444444444444444444444444444444444"}, "4444444444444444444444444444444444444444"},
		{"whitespace trimmed around valid SHA", map[string]string{"org.opencontainers.image.revision": "  5555555555555555555555555555555555555555  \n"}, "5555555555555555555555555555555555555555"},
		{"all keys invalid SHAs", map[string]string{"org.opencontainers.image.revision": "bad", "aerial.commit_sha": "also-bad"}, ""},
		{"empty annotations map", map[string]string{}, ""},
		{"nil annotations map", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractRevisionFromAnnotations(tt.annotations)
			if got != tt.want {
				t.Errorf("ExtractRevisionFromAnnotations() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractRevisionFromConfigJSON_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		configJSON []byte
		want       string
	}{
		{"standard config.Labels with OCI revision", []byte(`{"config":{"Labels":{"org.opencontainers.image.revision":"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}}}`), "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"},
		{"standard config.Labels with aerial.commit_sha", []byte(`{"config":{"Labels":{"aerial.commit_sha":"abcdef01234567"}}}`), "abcdef01234567"},
		{"uppercase hex in config.Labels", []byte(`{"config":{"Labels":{"org.opencontainers.image.revision":"A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2"}}}`), "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2"},
		{"root labels fallback", []byte(`{"labels":{"org.label-schema.vcs-ref":"9876543210fedcba"}}`), "9876543210fedcba"},
		{"missing labels in config", []byte(`{"config":{"Image":"sha256:12345"}}`), ""},
		{"empty json object", []byte(`{}`), ""},
		{"malformed json", []byte(`not json`), ""},
		{"empty byte slice", []byte{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractRevisionFromConfigJSON(tt.configJSON)
			if got != tt.want {
				t.Errorf("ExtractRevisionFromConfigJSON() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractPlatformManifestDigest_TableDriven(t *testing.T) {
	indexDoc := []byte(`{"schemaVersion":2,"manifests":[{"digest":"sha256:arm64digest111111111111111111111111111111111111111111111111111111","platform":{"architecture":"arm64","os":"linux"}},{"digest":"sha256:amd64digest222222222222222222222222222222222222222222222222222222","platform":{"architecture":"amd64","os":"linux"}},{"digest":"sha256:windowsdigest3333333333333333333333333333333333333333333333333333","platform":{"architecture":"amd64","os":"windows"}}]}`)

	tests := []struct {
		name       string
		indexJSON  []byte
		targetOS   string
		targetArch string
		want       string
	}{
		{"matching linux/amd64", indexDoc, "linux", "amd64", "sha256:amd64digest222222222222222222222222222222222222222222222222222222"},
		{"case-insensitive matching LINUX/AMD64", indexDoc, "LINUX", "AMD64", "sha256:amd64digest222222222222222222222222222222222222222222222222222222"},
		{"fallback to first entry when platform not found", indexDoc, "darwin", "arm64", "sha256:arm64digest111111111111111111111111111111111111111111111111111111"},
		{"fallback to first entry when OS/Arch empty", indexDoc, "", "", "sha256:arm64digest111111111111111111111111111111111111111111111111111111"},
		{"empty manifests list", []byte(`{"manifests": []}`), "linux", "amd64", ""},
		{"malformed JSON", []byte(`not json`), "linux", "amd64", ""},
		{"empty input", []byte{}, "linux", "amd64", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractPlatformManifestDigest(tt.indexJSON, tt.targetOS, tt.targetArch)
			if got != tt.want {
				t.Errorf("ExtractPlatformManifestDigest() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractConfigBlobDigest_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		manifestJSON []byte
		want         string
	}{
		{"standard OCI manifest with config digest", []byte(`{"schemaVersion":2,"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:configblobdigest111111111111111111111111111111111111111111111111","size":1234}}`), "sha256:configblobdigest111111111111111111111111111111111111111111111111"},
		{"missing config digest", []byte(`{"schemaVersion":2,"config":{}}`), ""},
		{"empty JSON", []byte(`{}`), ""},
		{"malformed JSON", []byte(`invalid json`), ""},
		{"empty byte slice", []byte{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractConfigBlobDigest(tt.manifestJSON)
			if got != tt.want {
				t.Errorf("ExtractConfigBlobDigest() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractManifestAnnotations_TableDriven(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json []byte
		want map[string]string
	}{
		{"empty input", []byte{}, nil},
		{"invalid json", []byte("not-json"), nil},
		{"valid json with annotations", []byte(`{"annotations":{"org.opencontainers.image.revision":"abcdef1234567890"}}`), map[string]string{"org.opencontainers.image.revision": "abcdef1234567890"}},
		{"valid json without annotations", []byte(`{"schemaVersion": 2}`), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExtractManifestAnnotations(tc.json)
			if tc.want == nil && got != nil {
				t.Fatalf("expected nil, got %v", got)
			}
			if tc.want != nil {
				if len(got) != len(tc.want) {
					t.Fatalf("expected len %d, got %d", len(tc.want), len(got))
				}
				for k, v := range tc.want {
					if got[k] != v {
						t.Fatalf("for key %s, expected %s, got %s", k, v, got[k])
					}
				}
			}
		})
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
	if err := os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(configDir, "jobs"), 0o755); err != nil {
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

	serviceMappings := []ServiceConfigMapping{
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
			changedFiles:   []string{"config.yaml", "homepage/widgets.yaml", "services/voice/voice.yaml"},
			nomadChanges:   []NomadFileChange{{Path: "jobs/hangar.nomad", Action: "apply", JobName: "hangar"}},
			serviceConfigs: serviceMappings,
			want:           []string{"eri", "homepage", "voice", "hangar"},
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveExpectedDeployJobs(tc.repoPath, tc.changedFiles, tc.nomadChanges, tc.serviceConfigs, tc.candidateRepos)
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

func TestIsStaticAssetOnlyFile_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"dashboard static file", "services/dashboard/static/index.html", true},
		{"dashboard static nested", "services/dashboard/static/assets/app.js", true},
		{"dashboard static leading slash", "/services/dashboard/static/app.css", true},
		{"docs service app file", "services/docs-service/app/index.html", true},
		{"docs service app nested", "services/docs-service/app/vendor/docsify.js", true},
		{"dashboard go file", "services/dashboard/main.go", false},
		{"docs service go file", "services/docs-service/main.go", false},
		{"unrelated service", "services/eri/pkg/db/db.go", false},
		{"root Dockerfile", "Dockerfile", false},
		{"empty path", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isStaticAssetOnlyFile(tc.path)
			if got != tc.expected {
				t.Errorf("isStaticAssetOnlyFile(%q) = %v, want %v", tc.path, got, tc.expected)
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
			sources, excl, err := ResolveAssetBundleSources(tc.bundle, tc.composeDir, tc.configDir, tc.mirrormere)
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

func TestAssetBundleChangedInList_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		bundle   string
		files    []string
		expected bool
	}{
		{"dashboard static change", "dashboard", []string{"services/dashboard/static/app.js"}, true},
		{"dashboard job change", "dashboard", []string{"nomad/jobs/dashboard.nomad"}, true},
		{"dashboard job change alt", "dashboard", []string{"jobs/dashboard.nomad"}, true},
		{"dashboard unrelated go change", "dashboard", []string{"services/dashboard/main.go"}, false},
		{"docs service app change", "docs", []string{"services/docs-service/app/index.html"}, true},
		{"docs markdown change", "docs", []string{"docs/index.md"}, true},
		{"docs job change", "docs", []string{"nomad/jobs/docs.nomad"}, true},
		{"docs unrelated go change", "docs", []string{"services/docs-service/main.go"}, false},
		{"grafana system dashboard change", "grafana", []string{"nomad/grafana/dashboards/sys.json"}, true},
		{"grafana custom dashboard change", "grafana", []string{"services/grafana/dashboards/user.json"}, true},
		{"grafana job change", "grafana", []string{"nomad/jobs/grafana.nomad"}, true},
		{"grafana unrelated change", "grafana", []string{"services/grafana/main.go"}, false},
		{"homepage core change", "homepage", []string{"services/homepage/entrypoint.sh"}, true},
		{"homepage user config change", "homepage", []string{"homepage/services.yaml"}, true},
		{"homepage service yaml change", "homepage", []string{"services/homepage/homepage.yaml"}, true},
		{"homepage job change", "homepage", []string{"nomad/jobs/homepage.nomad"}, true},
		{"homepage test directory excluded", "homepage", []string{"services/homepage/test/prepare-config.test.js"}, false},
		{"homepage unrelated change", "homepage", []string{"services/eri/main.go"}, false},
		{"mirrormere web change", "mirrormere", []string{"web/index.html"}, true},
		{"mirrormere widgets change", "mirrormere", []string{"widgets/weather.js"}, true},
		{"mirrormere job change", "mirrormere", []string{"jobs/mirrormere-core.nomad"}, true},
		{"mirrormere unrelated change", "mirrormere", []string{"server.go"}, false},
		{"migrations sql change", "migrations", []string{"services/eri/pkg/db/migrations/001_init.sql"}, true},
		{"migrations job change", "migrations", []string{"nomad/jobs/db-migrate.nomad"}, true},
		{"migrations unrelated change", "migrations", []string{"services/eri/main.go"}, false},
		{"unknown bundle", "unknown", []string{"services/dashboard/static/app.js"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := AssetBundleChangedInList(tc.bundle, tc.files)
			if got != tc.expected {
				t.Errorf("AssetBundleChangedInList(%q, %v) = %v, want %v", tc.bundle, tc.files, got, tc.expected)
			}
		})
	}
}

func TestJobHasContainerBuildChanges_StaticAssetExclusion(t *testing.T) {
	t.Parallel()
	spec := `job "dashboard" {
		task "dashboard" {
			driver = "docker"
			config {
				image = "ghcr.io/azylman/aerial-dashboard:latest"
			}
		}
	}`

	tests := []struct {
		name     string
		files    []string
		expected bool
	}{
		{"static file change only", []string{"services/dashboard/static/index.html"}, false},
		{"static nested file change only", []string{"services/dashboard/static/assets/bundle.js"}, false},
		{"code change in service", []string{"services/dashboard/main.go"}, true},
		{"dockerfile change", []string{"Dockerfile"}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := JobHasContainerBuildChanges(spec, tc.files)
			if got != tc.expected {
				t.Errorf("JobHasContainerBuildChanges() = %v, want %v for %v", got, tc.expected, tc.files)
			}
		})
	}
}
