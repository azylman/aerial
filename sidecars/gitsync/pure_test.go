package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestFilterConflictContainers_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		dockerPs     string
		selfHostname string
		wantIDs      []string
	}{
		{
			name: "valid conflict containers with different non-running states",
			dockerPs: strings.Join([]string{
				"c01111111111\t/c01111111111_brain\tExited (0)",
				"c02222222222\t/c02222222222_brain\tCreated",
				"c03333333333\tc03333333333_brain\tDead",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c01111111111", "c02222222222", "c03333333333"},
		},
		{
			name: "running or restarting containers are ignored",
			dockerPs: strings.Join([]string{
				"c01111111111\t/c01111111111_brain\tUp 2 hours",
				"c02222222222\t/c02222222222_brain\tRestarting (1) 5 seconds ago",
				"c03333333333\t/c03333333333_brain\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c03333333333"},
		},
		{
			name: "self hostname is preserved and ignored",
			dockerPs: strings.Join([]string{
				"selfhost1234\t/selfhost1234_brain\tExited (0)",
				"c02222222222\t/c02222222222_brain\tExited (0)",
			}, "\n"),
			selfHostname: "selfhost1234",
			wantIDs:      []string{"c02222222222"},
		},
		{
			name: "self hostname match on full ID when shortID is used",
			dockerPs: strings.Join([]string{
				"aabbccddeeff001122\t/aabbccddeeff_brain\tExited (0)",
				"c02222222222\t/c02222222222_brain\tExited (0)",
			}, "\n"),
			selfHostname: "aabbccddeeff001122",
			wantIDs:      []string{"c02222222222"},
		},
		{
			name: "dead gitsync conflict container is included",
			dockerPs: strings.Join([]string{
				"c01111111111\t/c01111111111_aerial-gitsync\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c01111111111"},
		},
		{
			name: "case-insensitive hex matching",
			dockerPs: strings.Join([]string{
				"C0A1B2C3D4E5\t/c0a1b2c3d4e5_brain\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"C0A1B2C3D4E5"},
		},
		{
			name: "non-hex shortID or length less than 12 ignored",
			dockerPs: strings.Join([]string{
				"short123\t/short123_brain\tExited (0)",
				"nonhexzzzzzz\t/nonhexzzzzzz_brain\tExited (0)",
				"c01111111111\t/c01111111111_brain\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c01111111111"},
		},
		{
			name: "multiple comma-separated names with leading slashes",
			dockerPs: strings.Join([]string{
				"c01111111111\t/alias1,/c01111111111_brain,alias2\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c01111111111"},
		},
		{
			name: "malformed lines and empty output",
			dockerPs: strings.Join([]string{
				"",
				"only_id",
				"id\tname",
				"   \t  \t  ",
				"c01111111111\t/c01111111111_brain\tExited (0)",
			}, "\n"),
			selfHostname: "otherhost",
			wantIDs:      []string{"c01111111111"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterConflictContainers(tt.dockerPs, tt.selfHostname)
			var gotIDs []string
			for _, c := range got {
				gotIDs = append(gotIDs, c.ID)
			}
			if len(gotIDs) != len(tt.wantIDs) {
				t.Fatalf("expected %d IDs, got %d: %v", len(tt.wantIDs), len(gotIDs), gotIDs)
			}
			for i := range gotIDs {
				if gotIDs[i] != tt.wantIDs[i] {
					t.Errorf("ID[%d] = %q, want %q", i, gotIDs[i], tt.wantIDs[i])
				}
			}
		})
	}
}

func TestParseComposeServices_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "standard services with gitsync excluded",
			input:    "brain\ngitsync\ndashboard\nprometheus\n",
			expected: []string{"brain", "dashboard", "prometheus"},
		},
		{
			name:     "case variants of gitsync",
			input:    "GITSYNC\nservice1\nGitSync\nservice2\ngitsync\n",
			expected: []string{"service1", "service2"},
		},
		{
			name:     "crlf and duplicates",
			input:    "brain\r\nserviceA\r\nbrain\r\nserviceB\r\n\r\n",
			expected: []string{"brain", "serviceA", "serviceB"},
		},
		{
			name:     "empty input",
			input:    "\n   \n\r\n",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseComposeServices(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d services, got %d: %v", len(tt.expected), len(got), got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestIsComposeFile_TableDriven(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"docker-compose.yml", true},
		{"docker-compose.yaml", true},
		{"docker-compose.override.yml", true},
		{"docker-compose.override.yaml", true},
		{"compose.yaml", true},
		{"compose.yml", true},
		{"compose.override.yaml", true},
		{"compose.override.yml", true},
		{".env", true},
		{".env.example", true},
		{"/root/deploy/docker-compose.yml", true},
		{"C:\\Users\\alexz\\.env", true},
		{"main.go", false},
		{"README.md", false},
		{"docker-compose.sh", false},
		{"compose.json", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := IsComposeFile(tt.path)
			if got != tt.expected {
				t.Errorf("IsComposeFile(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestFilterComposeChanges_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "mixed list",
			input:    []string{"brain/main.go", "docker-compose.yml", "README.md", "deploy/.env"},
			expected: []string{"docker-compose.yml", "deploy/.env"},
		},
		{
			name:     "no compose files",
			input:    []string{"pkg/config.go", "Makefile"},
			expected: nil,
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FilterComposeChanges(tt.input)
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
			stage:        "compose apply",
			errorMsg:     "container failed to start",
			checkSubstrs: []string{
				"🚨 **GitSync GitOps Rollback Alert: Reconcile Failed**",
				"**Repository:**",
				"**Stage:** `compose apply`",
				"**Faulty Commit:** `abc1234`",
				"**Rolled Back To:** `def5678`",
				"container failed to start",
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
				"🚨 **GitSync GitOps Rollback Alert: Validation Error**",
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
				"🚨 **GitSync GitOps Rollback Alert: Clean Alert**",
				"*Faulty commit quarantined to prevent sync loops. Newer commits to origin/main will sync normally.*",
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
			name: "quarantined takes precedence over lagging and synced",
			repos: map[string]RepoStatus{
				"repo1": {SyncStatus: "synced"},
				"repo2": {SyncStatus: "lagging"},
				"repo3": {SyncStatus: "quarantined", Quarantined: true},
			},
			expected: "quarantined",
		},
		{
			name: "error takes highest precedence",
			repos: map[string]RepoStatus{
				"repo1": {SyncStatus: "synced"},
				"repo2": {SyncStatus: "lagging"},
				"repo3": {SyncStatus: "quarantined", Quarantined: true},
				"repo4": {SyncStatus: "error", Error: "disk read failed"},
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
			if got != tt.expected {
				t.Errorf("ResolveDockerConfigPath() = %q, want %q", got, tt.expected)
			}
		})
	}
}

