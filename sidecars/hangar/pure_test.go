package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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

func TestBuildDiscordAlertContent_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		title        string
		repoPath     string
		faultyCommit string
		rolledBackTo string
		stage        string
		errorMsg     string
		checkSubstrs []string
	}{
		{
			name:         "full alert formatting",
			title:        "Reconcile Failed",
			repoPath:     "/share/aerial",
			faultyCommit: "abc1234",
			rolledBackTo: "def5678",
			stage:        "nomad apply",
			errorMsg:     "job failed to place",
			checkSubstrs: []string{
				"🚨 **GitSync GitOps Alert: Reconcile Failed**",
				"**Repository:**",
				"**Stage:** `nomad apply`",
				"**Commit:** `abc1234`",
				"**Rolled Back To:** `def5678`",
				"job failed to place",
			},
		},
		{
			name:         "error truncation over 1200 chars",
			title:        "Validation Error",
			repoPath:     "",
			faultyCommit: "",
			rolledBackTo: "",
			stage:        "validation",
			errorMsg:     strings.Repeat("X", 1300),
			checkSubstrs: []string{
				"🚨 **GitSync GitOps Alert: Validation Error**",
				"**Stage:** `validation`",
				strings.Repeat("X", 1200),
				"[... truncated for length]",
			},
		},
		{
			name:         "empty optional fields",
			title:        "Clean Alert",
			repoPath:     "",
			faultyCommit: "",
			rolledBackTo: "",
			stage:        "",
			errorMsg:     "",
			checkSubstrs: []string{
				"🚨 **GitSync GitOps Alert: Clean Alert**",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := BuildDiscordAlertContent(tt.title, tt.repoPath, tt.faultyCommit, tt.rolledBackTo, tt.stage, tt.errorMsg)
			for _, sub := range tt.checkSubstrs {
				if !strings.Contains(content, sub) {
					t.Errorf("expected alert to contain %q, but got:\n%s", sub, content)
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
		name       string
		input      string
		wantReg    string
		wantRepo   string
		wantTag    string
		wantErr    bool
	}{
		{
			name:     "official single segment",
			input:    "nginx",
			wantReg:  "registry-1.docker.io",
			wantRepo: "library/nginx",
			wantTag:  "latest",
		},
		{
			name:     "official single segment with tag",
			input:    "redis:alpine",
			wantReg:  "registry-1.docker.io",
			wantRepo: "library/redis",
			wantTag:  "alpine",
		},
		{
			name:     "namespaced docker hub",
			input:    "pgvector/pgvector:pg16",
			wantReg:  "registry-1.docker.io",
			wantRepo: "pgvector/pgvector",
			wantTag:  "pg16",
		},
		{
			name:     "explicit docker.io domain",
			input:    "docker.io/library/ubuntu:latest",
			wantReg:  "registry-1.docker.io",
			wantRepo: "library/ubuntu",
			wantTag:  "latest",
		},
		{
			name:     "explicit index.docker.io domain",
			input:    "index.docker.io/prom/node-exporter:v1.8.2",
			wantReg:  "registry-1.docker.io",
			wantRepo: "prom/node-exporter",
			wantTag:  "v1.8.2",
		},
		{
			name:     "ghcr private monorepo reference",
			input:    "ghcr.io/azylman/aerial-brain:latest",
			wantReg:  "ghcr.io",
			wantRepo: "azylman/aerial-brain",
			wantTag:  "latest",
		},
		{
			name:     "ghcr public reference without tag defaults to latest",
			input:    "ghcr.io/gethomepage/homepage",
			wantReg:  "ghcr.io",
			wantRepo: "gethomepage/homepage",
			wantTag:  "latest",
		},
		{
			name:     "gcr third-party reference",
			input:    "gcr.io/cadvisor/cadvisor:v0.49.1",
			wantReg:  "gcr.io",
			wantRepo: "cadvisor/cadvisor",
			wantTag:  "v0.49.1",
		},
		{
			name:     "localhost registry with port",
			input:    "localhost:5000/my-app:1.0",
			wantReg:  "localhost:5000",
			wantRepo: "my-app",
			wantTag:  "1.0",
		},
		{
			name:     "custom domain with port",
			input:    "registry.internal:8443/deep/path/svc:dev",
			wantReg:  "registry.internal:8443",
			wantRepo: "deep/path/svc",
			wantTag:  "dev",
		},
		{
			name:     "pinned sha256 digest reference",
			input:    "ghcr.io/azylman/brain@sha256:1f9fd513925ef06272dea861c4c8cb7c10a9eb6b5a4e7ad0e4ca00b49fc27866",
			wantReg:  "ghcr.io",
			wantRepo: "azylman/brain",
			wantTag:  "sha256:1f9fd513925ef06272dea861c4c8cb7c10a9eb6b5a4e7ad0e4ca00b49fc27866",
		},
		{
			name:    "empty string errors",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only errors",
			input:   "   ",
			wantErr: true,
		},
		{
			name:    "spaces in reference errors",
			input:   "ghcr.io/ azylman/brain:latest",
			wantErr: true,
		},
		{
			name:    "invalid digest prefix errors",
			input:   "redis@md5:12345",
			wantErr: true,
		},
		{
			name:    "trailing colon without tag errors",
			input:   "redis:",
			wantErr: true,
		},
		{
			name:    "missing repository name errors",
			input:   ":latest",
			wantErr: true,
		},
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
		{
			name:        "docker hub standard challenge with quotes",
			header:      `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:pgvector/pgvector:pull"`,
			wantRealm:   "https://auth.docker.io/token",
			wantService: "registry.docker.io",
			wantScope:   "repository:pgvector/pgvector:pull",
		},
		{
			name:        "ghcr standard challenge without scope",
			header:      `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
			wantRealm:   "https://ghcr.io/token",
			wantService: "ghcr.io",
			wantScope:   "",
		},
		{
			name:        "case-insensitive bearer prefix and whitespace",
			header:      `bearer  realm="https://auth.example.com/token" , service="example.com" `,
			wantRealm:   "https://auth.example.com/token",
			wantService: "example.com",
			wantScope:   "",
		},
		{
			name:        "unquoted values",
			header:      `Bearer realm=https://auth.example.com/token,service=my.registry`,
			wantRealm:   "https://auth.example.com/token",
			wantService: "my.registry",
			wantScope:   "",
		},
		{
			name:        "unquoted single realm",
			header:      `Bearer realm=https://auth.example.com/token`,
			wantRealm:   "https://auth.example.com/token",
			wantService: "",
			wantScope:   "",
		},
		{
			name:        "unclosed quote realm",
			header:      `Bearer realm="https://auth.example.com/token`,
			wantRealm:   "https://auth.example.com/token",
			wantService: "",
			wantScope:   "",
		},
		{
			name:    "empty header",
			header:  "",
			wantErr: true,
		},
		{
			name:    "non-bearer scheme",
			header:  `Basic realm="WallyWorld"`,
			wantErr: true,
		},
		{
			name:    "missing realm",
			header:  `Bearer service="registry.docker.io"`,
			wantErr: true,
		},
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

func TestContainsDigest_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		repoDigests []string
		target      string
		want        bool
	}{
		{
			name: "exact suffix match with repository",
			repoDigests: []string{
				"ghcr.io/azylman/aerial-brain@sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			},
			target: "sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			want:   true,
		},
		{
			name: "exact match without repo prefix",
			repoDigests: []string{
				"sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			},
			target: "sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			want:   true,
		},
		{
			name: "registry with port in repo digest",
			repoDigests: []string{
				"localhost:5000/aerial-brain@sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			},
			target: "sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			want:   true,
		},
		{
			name: "multiple digests with match in middle",
			repoDigests: []string{
				"docker.io/library/redis@sha256:1111111111111111111111111111111111111111111111111111111111111111",
				"redis@sha256:2222222222222222222222222222222222222222222222222222222222222222",
			},
			target: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			want:   true,
		},
		{
			name: "digest mismatch",
			repoDigests: []string{
				"ghcr.io/azylman/aerial-brain@sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			},
			target: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			want:   false,
		},
		{
			name:        "empty repo digests",
			repoDigests: []string{},
			target:      "sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			want:        false,
		},
		{
			name: "empty target digest",
			repoDigests: []string{
				"ghcr.io/azylman/aerial-brain@sha256:9302a45515e41263e8051f6be0a4b5295753259c3368134f59220afb23ae6aff",
			},
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ContainsDigest(tt.repoDigests, tt.target)
			if got != tt.want {
				t.Errorf("ContainsDigest() = %v, want %v", got, tt.want)
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
		{
			name:     "standard https with .git",
			input:    "https://github.com/azylman/aerial.git",
			expected: "azylman/aerial",
		},
		{
			name:     "standard https without .git",
			input:    "https://github.com/azylman/mirrormere",
			expected: "azylman/mirrormere",
		},
		{
			name:     "https with trailing slash",
			input:    "https://github.com/azylman/mirrormere/",
			expected: "azylman/mirrormere",
		},
		{
			name:     "https with auth credentials / token",
			input:    "https://x-access-token:ghp_secret12345@github.com/azylman/mirrormere.git",
			expected: "azylman/mirrormere",
		},
		{
			name:     "scp-like ssh syntax with .git",
			input:    "git@github.com:azylman/aerial.git",
			expected: "azylman/aerial",
		},
		{
			name:     "scp-like ssh syntax without .git",
			input:    "git@github.com:azylman/aerial",
			expected: "azylman/aerial",
		},
		{
			name:     "ssh scheme with user",
			input:    "ssh://git@github.com/azylman/aerial.git",
			expected: "azylman/aerial",
		},
		{
			name:     "ssh scheme with port",
			input:    "ssh://git@github.com:22/azylman/aerial.git",
			expected: "azylman/aerial",
		},
		{
			name:     "mixed casing normalized to lowercase",
			input:    "https://github.com/Azylman/MirrorMere.git",
			expected: "azylman/mirrormere",
		},
		{
			name:     "with query params and fragment",
			input:    "https://github.com/azylman/aerial.git?ref=main#readme",
			expected: "azylman/aerial",
		},
		{
			name:     "leading and trailing whitespace and newlines",
			input:    "  \n\thttps://github.com/azylman/aerial.git \r\n ",
			expected: "azylman/aerial",
		},
		{
			name:     "gitlab rejected",
			input:    "https://gitlab.com/azylman/aerial.git",
			expected: "",
		},
		{
			name:     "spoofed host rejected",
			input:    "https://notgithub.com/azylman/aerial.git",
			expected: "",
		},
		{
			name:     "subdomain spoof rejected",
			input:    "https://github.com.evil.com/azylman/aerial.git",
			expected: "",
		},
		{
			name:     "gitlab ssh rejected",
			input:    "git@gitlab.com:azylman/aerial.git",
			expected: "",
		},
		{
			name:     "local path rejected",
			input:    "/share/aerial",
			expected: "",
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "whitespace only",
			input:    "   \n\t  ",
			expected: "",
		},
		{
			name:     "root github url rejected",
			input:    "https://github.com/",
			expected: "",
		},
		{
			name:     "single segment path rejected",
			input:    "https://github.com/azylman",
			expected: "",
		},
		{
			name:     "extra path segments rejected",
			input:    "https://github.com/azylman/aerial/tree/main",
			expected: "",
		},
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

func TestFilterNomadChanges_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "mixed file list",
			input:    []string{"docker-compose.yml", "jobs/infisical.nomad", "README.md", "jobs/voice.nomad"},
			expected: []string{"jobs/infisical.nomad", "jobs/voice.nomad"},
		},
		{
			name:     "no nomad files",
			input:    []string{"docker-compose.yml", "services/voice.yaml"},
			expected: nil,
		},
		{
			name:     "empty list",
			input:    []string{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterNomadChanges(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("FilterNomadChanges() returned %d items, want %d", len(got), len(tt.expected))
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("FilterNomadChanges()[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
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

func TestParseNomadGitStatus_TableDriven(t *testing.T) {
	statusOutput := `M	jobs/infisical.nomad
A	jobs/mirrormere-core.nomad
D	jobs/deprecated.nomad
M	docker-compose.yml
R100	jobs/old.nomad	jobs/new.nomad
`
	changes := ParseNomadGitStatus(statusOutput)
	if len(changes) != 4 {
		t.Fatalf("expected 4 nomad changes, got %d: %+v", len(changes), changes)
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
	if changes[3].Path != "jobs/new.nomad" || changes[3].Action != "apply" || changes[3].JobName != "new" {
		t.Errorf("unexpected change[3]: %+v", changes[3])
	}

	// Additional ParseNomadGitStatus cases
	emptyStatus := ParseNomadGitStatus("")
	if len(emptyStatus) != 0 {
		t.Errorf("expected 0 changes for empty status")
	}

	renameStatus := ParseNomadGitStatus("R090\tjobs/old.nomad\tjobs/renamed.nomad\n")
	if len(renameStatus) != 1 || renameStatus[0].Action != "apply" || renameStatus[0].JobName != "renamed" {
		t.Errorf("expected 1 change (apply renamed) for rename status, got %+v", renameStatus)
	}

	nonNomadStatus := ParseNomadGitStatus("M\tdocker-compose.yml\nA\tREADME.md\n")
	if len(nonNomadStatus) != 0 {
		t.Errorf("expected 0 changes for non-nomad files")
	}
}

func TestPureSlugAndDigestHelpers(t *testing.T) {
	// ParseGitHubSlug test cases
	slugCases := []struct {
		input string
		want  string
	}{
		{"https://github.com/azylman/aerial.git?ref=main#hash", "azylman/aerial"},
		{"git@github.com:azylman/aerial.git", "azylman/aerial"},
		{"git@github.com:azylman/aerial", "azylman/aerial"},
		{"git@gitlab.com:azylman/aerial.git", ""},
		{"git@malformed", ""},
		{"https://gitlab.com/azylman/aerial.git", ""},
		{"https://github.com/invalid/path/extra/parts", ""},
		{"https://github.com/singlepart", ""},
		{"https://github.com/ /aerial", ""},
		{"https://github.com/azylman/ ", ""},
		{"http://github.com/azylman/aerial.git", "azylman/aerial"},
		{"ssh://git@github.com/azylman/aerial.git", "azylman/aerial"},
		{"", ""},
		{"   ", ""},
		{"http://:invalid-url", ""},
	}
	for _, tc := range slugCases {
		got := ParseGitHubSlug(tc.input)
		if got != tc.want {
			t.Errorf("ParseGitHubSlug(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}

	if cleanSlugPath("azylman/aerial?query#hash") != "azylman/aerial" {
		t.Errorf("expected clean slug with query/hash")
	}
	if cleanSlugPath("too/many/parts") != "" {
		t.Errorf("expected empty for too many parts")
	}
	if cleanSlugPath(" / ") != "" {
		t.Errorf("expected empty for blank parts")
	}


	// ContainsDigest test cases
	if !ContainsDigest([]string{"sha256:abc", "sha256:def"}, "sha256:abc") {
		t.Errorf("expected true for existing digest")
	}
	if ContainsDigest([]string{"sha256:abc"}, "sha256:xyz") {
		t.Errorf("expected false for nonexistent digest")
	}
	if ContainsDigest(nil, "sha256:abc") {
		t.Errorf("expected false for nil slice")
	}
}

func TestHasContainerBuildChanges_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		repoPath string
		files    []string
		expected bool
	}{
		{
			name:     "aerial-config always false",
			repoPath: "/share/aerial-config",
			files:    []string{"jobs/brain.nomad", "rules/common.md", "Dockerfile"},
			expected: false,
		},
		{
			name:     "aerial core brain touched",
			repoPath: "/share/aerial",
			files:    []string{"brain/main.go"},
			expected: true,
		},
		{
			name:     "aerial core webhooks-router touched",
			repoPath: "/share/aerial",
			files:    []string{"webhooks-router/main.go"},
			expected: true,
		},
		{
			name:     "aerial core Dockerfile touched",
			repoPath: "/share/aerial",
			files:    []string{"Dockerfile"},
			expected: true,
		},
		{
			name:     "aerial core go.mod touched",
			repoPath: "/share/aerial",
			files:    []string{"go.mod"},
			expected: true,
		},
		{
			name:     "aerial core non-build files (docs, nomad, rules)",
			repoPath: "/share/aerial",
			files:    []string{"nomad/jobs/migrate.nomad", "README.md", "rules/discord/foo.md", "docs/spec.md"},
			expected: false,
		},
		{
			name:     "aerial core empty files",
			repoPath: "/share/aerial",
			files:    nil,
			expected: false,
		},
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
		{"valid nomad file in base", "/share/aerial-config/jobs/webhooks-edge.nomad", true},
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

func TestFindNomadJobsByImage_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()

	job1 := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`
	job2 := `job "infisical-mcp" {
		task "mcp" {
			config {
				image = "ghcr.io/azylman/aerial-infisical-mcp:latest"
			}
		}
	}`
	job3 := `job "multi-task" {
		task "first" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`

	_ = os.WriteFile(filepath.Join(tmpDir, "webhooks-router.nomad"), []byte(job1), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "infisical-mcp.nomad"), []byte(job2), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "multi-task.nomad"), []byte(job3), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "README.md"), []byte("non-nomad"), 0644)

	matches := FindNomadJobsByImage(tmpDir, "aerial-webhooks-router")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches for aerial-webhooks-router, got %d", len(matches))
	}

	foundJobNames := make(map[string]bool)
	for _, m := range matches {
		foundJobNames[m.JobName] = true
	}
	if !foundJobNames["webhooks-router"] || !foundJobNames["multi-task"] {
		t.Errorf("expected webhooks-router and multi-task, got %v", foundJobNames)
	}

	noMatches := FindNomadJobsByImage(tmpDir, "nonexistent-image")
	if len(noMatches) != 0 {
		t.Errorf("expected 0 matches for nonexistent-image, got %d", len(noMatches))
	}

	emptyMatches := FindNomadJobsByImage("", "aerial-webhooks-router")
	if len(emptyMatches) != 0 {
		t.Errorf("expected 0 matches for empty dir, got %d", len(emptyMatches))
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
		{"orin voice sidecar", "ghcr.io/azylman/orin-voice:latest", "azylman/aerial-sidecars"},
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

func TestPureFunctions_EdgeCasesCoverage(t *testing.T) {
	// FindNomadJobsByImage with non-existent directory
	if res := FindNomadJobsByImage("/nonexistent/directory/path/12345", "test"); res != nil {
		t.Errorf("expected nil for nonexistent dir, got %v", res)
	}
	// FindNomadJobsByImage with empty image
	if res := FindNomadJobsByImage(t.TempDir(), ""); res != nil {
		t.Errorf("expected nil for empty imageRef, got %v", res)
	}

	// NormalizeGitHubSlug edge cases
	if s := NormalizeGitHubSlug("/share/aerial"); s != "azylman/aerial" {
		t.Errorf("NormalizeGitHubSlug(/share/aerial) = %q; want azylman/aerial", s)
	}
	if s := NormalizeGitHubSlug("/"); s != "" && s != "azylman/" {
		t.Logf("NormalizeGitHubSlug(/) = %q", s)
	}
	if s := NormalizeGitHubSlug("git@github.com:azylman/mirrormere.git"); s != "azylman/mirrormere" {
		t.Errorf("NormalizeGitHubSlug(git@github.com:azylman/mirrormere.git) = %q; want azylman/mirrormere", s)
	}
	if s := NormalizeGitHubSlug("http://github.com/azylman/aerial"); s != "azylman/aerial" {
		t.Errorf("NormalizeGitHubSlug(http://github.com/azylman/aerial) = %q; want azylman/aerial", s)
	}

	// ResolveRepoPath edge cases
	repos := []string{"/opt/aerial", "/opt/aerial-config"}
	if r := ResolveRepoPath("", repos); r != "" {
		t.Errorf("ResolveRepoPath empty query = %q; want empty", r)
	}
	if r := ResolveRepoPath("https://github.com/azylman/aerial.git", repos); r != "/opt/aerial" {
		t.Errorf("ResolveRepoPath github url = %q; want /opt/aerial", r)
	}
	if r := ResolveRepoPath("/opt/aerial", repos); r != "/opt/aerial" {
		t.Errorf("ResolveRepoPath exact path = %q; want /opt/aerial", r)
	}
}

func TestFindJobDefinitionInRepos(t *testing.T) {
	tempDir := t.TempDir()
	repoA := filepath.Join(tempDir, "repo-a")
	repoB := filepath.Join(tempDir, "repo-b")

	jobsDirA := filepath.Join(repoA, "jobs")
	nomadJobsDirB := filepath.Join(repoB, "nomad", "jobs")

	if err := os.MkdirAll(jobsDirA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nomadJobsDirB, 0755); err != nil {
		t.Fatal(err)
	}

	jobSpecBrain := `job "brain" {
  type = "service"
}`
	jobSpecPostgres := `job "postgres" {
  type = "service"
}`

	if err := os.WriteFile(filepath.Join(jobsDirA, "brain.nomad"), []byte(jobSpecBrain), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nomadJobsDirB, "brain.nomad"), []byte(jobSpecBrain), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nomadJobsDirB, "postgres.nomad"), []byte(jobSpecPostgres), 0644); err != nil {
		t.Fatal(err)
	}

	repos := []string{repoA, repoB}

	// Case 1: Job exists in repoB when repoA is skipped
	foundPath, found := FindJobDefinitionInRepos("brain", repoA, repos)
	if !found {
		t.Errorf("expected brain to be found in repoB when skipping repoA")
	}
	expectedB := filepath.Join(nomadJobsDirB, "brain.nomad")
	if foundPath != expectedB {
		t.Errorf("expected path %s, got %s", expectedB, foundPath)
	}

	// Case 2: Job does not exist when repoB is skipped (postgres only in repoB)
	_, found = FindJobDefinitionInRepos("postgres", repoB, repos)
	if found {
		t.Errorf("expected postgres NOT to be found when skipping repoB")
	}

	// Case 3: Empty job name or empty repos
	if _, found := FindJobDefinitionInRepos("", repoA, repos); found {
		t.Errorf("expected false for empty jobName")
	}
	if _, found := FindJobDefinitionInRepos("brain", "", nil); found {
		t.Errorf("expected false for empty candidateRepos")
	}
}

func TestFindJobDefinitionInRepos_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	repo := filepath.Join(tempDir, "repo")
	jobsDir := filepath.Join(repo, "jobs")
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// 1. Directory entry inside jobsDir (entry.IsDir() branch)
	subDir := filepath.Join(jobsDir, "subdir.nomad")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	// 2. Non-nomad file (!IsNomadJobFile branch)
	if err := os.WriteFile(filepath.Join(jobsDir, "notes.txt"), []byte("not a nomad job"), 0644); err != nil {
		t.Fatal(err)
	}

	// 3. Nomad file with different job name
	otherJobSpec := `job "other" { type = "service" }`
	if err := os.WriteFile(filepath.Join(jobsDir, "other.nomad"), []byte(otherJobSpec), 0644); err != nil {
		t.Fatal(err)
	}

	// Candidate repos including empty string and skipRepoPath
	repos := []string{"", repo}
	if path, found := FindJobDefinitionInRepos("target", "", repos); found {
		t.Errorf("expected target not found, got %s", path)
	}

	// Target matching "other"
	if path, found := FindJobDefinitionInRepos("other", "", repos); !found || !strings.Contains(path, "other.nomad") {
		t.Errorf("expected other to be found, got %s, %v", path, found)
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
		{JobName: "scheduler-mcp", JobPath: "jobs/scheduler-mcp.nomad"},
	}
	SortMatchedJobsHangarLast(matches)
	if matches[0].JobName != "scheduler-mcp" || matches[1].JobName != "hangar" {
		t.Fatalf("unexpected order after SortMatchedJobsHangarLast: %+v", matches)
	}
}
