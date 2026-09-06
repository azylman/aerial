package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSanitizeLog(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean string",
			input:    "repository synced cleanly",
			expected: "repository synced cleanly",
		},
		{
			name:     "github pat",
			input:    "failed with token github_pat_11AAAA_secret_token_123",
			expected: "failed with token [REDACTED_TOKEN]",
		},
		{
			name:     "classic ghp token",
			input:    "cloning with ghp_1234567890abcdef1234567890abcdef12",
			expected: "cloning with [REDACTED_TOKEN]",
		},
		{
			name:     "basic auth header",
			input:    "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46c2VjcmV0",
			expected: "AUTHORIZATION: [REDACTED_TOKEN]",
		},
		{
			name:     "postgres connection string",
			input:    "connecting to postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable",
			expected: "connecting to [REDACTED_TOKEN]?sslmode=disable",
		},
		{
			name:     "anthropic api key",
			input:    "using sk-ant-api03-1234567890abcdef-secret",
			expected: "using [REDACTED_TOKEN]",
		},
		{
			name:     "google api key",
			input:    "key AIzaSyD1234567890abcdefghijklmnopqrstuv",
			expected: "key [REDACTED_TOKEN]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeLog(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeLog() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestBuildGitEnv(t *testing.T) {
	env := buildGitEnv("my_test_pat")
	foundAuth := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_CONFIG_VALUE_0=") {
			foundAuth = true
			if !strings.Contains(e, "AUTHORIZATION: basic ") {
				t.Errorf("expected basic auth header, got %s", e)
			}
		}
	}
	if !foundAuth {
		t.Errorf("buildGitEnv() did not produce GIT_CONFIG_VALUE_0 auth header")
	}

	emptyEnv := buildGitEnv("")
	for _, e := range emptyEnv {
		if strings.HasPrefix(e, "GIT_CONFIG") {
			t.Errorf("unexpected GIT_CONFIG in empty PAT env: %s", e)
		}
	}
}

func TestHasComposeChanges(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize git repo
	cmdInit := exec.Command("git", "init", "-b", "main", tempDir)
	if out, err := cmdInit.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %s (%v)", out, err)
	}

	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	// Initial commit with non-compose file
	readmePath := filepath.Join(tempDir, "README.md")
	if err := os.WriteFile(readmePath, []byte("# Test"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "initial").Run()

	c1Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c1 := string(c1Out)

	daemon := &SyncDaemon{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Same head -> no changes
	changed, err := daemon.HasComposeChanges(ctx, tempDir, c1, c1)
	if err != nil || changed {
		t.Errorf("expected false, got changed=%v, err=%v", changed, err)
	}

	// 2. Commit modifying markdown -> no compose changes
	_ = os.WriteFile(readmePath, []byte("# Updated README"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "update readme").Run()
	c2Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c2 := string(c2Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c1, c2)
	if err != nil || changed {
		t.Errorf("expected false for markdown change, got changed=%v, err=%v", changed, err)
	}

	// 3. Commit modifying docker-compose.yml -> compose changes detected
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	if err := os.WriteFile(composePath, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "add compose").Run()
	c3Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c3 := string(c3Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c2, c3)
	if err != nil || !changed {
		t.Errorf("expected true for docker-compose.yml change, got changed=%v, err=%v", changed, err)
	}

	// 4. Commit modifying .env -> compose changes detected
	envPath := filepath.Join(tempDir, ".env")
	if err := os.WriteFile(envPath, []byte("FOO=BAR"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "add env").Run()
	c4Out, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	c4 := string(c4Out)

	changed, err = daemon.HasComposeChanges(ctx, tempDir, c3, c4)
	if err != nil || !changed {
		t.Errorf("expected true for .env change, got changed=%v, err=%v", changed, err)
	}

	// 5. Empty or identical head args -> false
	if ch, err := daemon.HasComposeChanges(ctx, tempDir, "", c4); err != nil || ch {
		t.Errorf("expected false for empty prevHead, got ch=%v, err=%v", ch, err)
	}
	if ch, err := daemon.HasComposeChanges(ctx, tempDir, c4, ""); err != nil || ch {
		t.Errorf("expected false for empty currHead, got ch=%v, err=%v", ch, err)
	}

	// 6. Broken ref fallback trigger -> true (fail-safe)
	nonGitDir := t.TempDir()
	if ch, err := daemon.HasComposeChanges(ctx, nonGitDir, "deadbeef1", "deadbeef2"); err != nil || !ch {
		t.Errorf("expected fail-safe true when git diff and fallback fail, got ch=%v, err=%v", ch, err)
	}
}

func TestGetStatus(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize git repo
	cmdInit := exec.Command("git", "init", "-b", "main", tempDir)
	if out, err := cmdInit.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %s (%v)", out, err)
	}

	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	readmePath := filepath.Join(tempDir, "README.md")
	if err := os.WriteFile(readmePath, []byte("# Initial"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "initial commit").Run()

	daemon := &SyncDaemon{
		repos: []string{tempDir},
	}

	ctx := context.Background()
	status := daemon.GetStatus(ctx)

	if status.Status != "synced" {
		t.Errorf("expected status 'synced', got %q", status.Status)
	}
	if status.MaxLagSeconds != 0 {
		t.Errorf("expected max_lag_seconds 0, got %d", status.MaxLagSeconds)
	}
	repoSt, ok := status.Repos[tempDir]
	if !ok {
		t.Fatalf("expected repo %s in status response", tempDir)
	}
	if repoSt.DiskCommit == "" {
		t.Errorf("expected non-empty disk commit")
	}
	if repoSt.SyncStatus != "synced" {
		t.Errorf("expected repo sync_status 'synced', got %q", repoSt.SyncStatus)
	}

	// Test HTTP Endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(daemon.GetStatus(r.Context()))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var jsonResp GitSyncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if jsonResp.Status != "synced" {
		t.Errorf("expected json status 'synced', got %q", jsonResp.Status)
	}
}

func TestGetComposeArgs_BaseOnly(t *testing.T) {
	composeDir := t.TempDir()
	configDir := t.TempDir()

	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	daemon := &SyncDaemon{
		composeDir: composeDir,
		configDir:  configDir,
	}

	args := daemon.getComposeArgs(composeDir, "config", "--quiet")

	expectedPrefix := []string{
		"--project-name", "aerial",
		"--project-directory", composeDir,
		"-f", baseFile,
		"config", "--quiet",
	}

	if len(args) != len(expectedPrefix) {
		t.Fatalf("expected %d args, got %d: %v", len(expectedPrefix), len(args), args)
	}
	for i := range expectedPrefix {
		if args[i] != expectedPrefix[i] {
			t.Errorf("arg[%d] = %q, want %q", i, args[i], expectedPrefix[i])
		}
	}
}

func TestGetComposeArgs_WithConfigOverride(t *testing.T) {
	composeDir := t.TempDir()
	configDir := t.TempDir()

	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	overrideFile := filepath.Join(configDir, "docker-compose.override.yml")
	if err := os.WriteFile(overrideFile, []byte("services: { brain: {} }"), 0644); err != nil {
		t.Fatal(err)
	}

	daemon := &SyncDaemon{
		composeDir: composeDir,
		configDir:  configDir,
	}

	args := daemon.getComposeArgs(composeDir, "up", "-d", "--no-recreate", "gitsync")

	expectedArgs := []string{
		"--project-name", "aerial",
		"--project-directory", composeDir,
		"-f", baseFile,
		"-f", filepath.Clean(overrideFile),
		"up", "-d", "--no-recreate", "gitsync",
	}

	if len(args) != len(expectedArgs) {
		t.Fatalf("expected %d args, got %d: %v", len(expectedArgs), len(args), args)
	}
	for i := range expectedArgs {
		if args[i] != expectedArgs[i] {
			t.Errorf("arg[%d] = %q, want %q", i, args[i], expectedArgs[i])
		}
	}
}

func TestGetComposeArgs_Deduplication(t *testing.T) {
	composeDir := t.TempDir()

	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	overrideFile := filepath.Join(composeDir, "docker-compose.override.yml")
	if err := os.WriteFile(overrideFile, []byte("services: { brain: {} }"), 0644); err != nil {
		t.Fatal(err)
	}

	// Set configDir to the same directory as composeDir
	daemon := &SyncDaemon{
		composeDir: composeDir,
		configDir:  composeDir,
	}

	args := daemon.getComposeArgs(composeDir, "config")

	fCount := 0
	for _, a := range args {
		if a == "-f" {
			fCount++
		}
	}

	if fCount != 2 {
		t.Fatalf("expected exactly 2 -f flags (base + deduplicated override), got %d: %v", fCount, args)
	}
}

func TestParseComposeServices(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "standard multi-service list",
			input:    "brain\ngitsync\ndashboard\nunpoller\n",
			expected: []string{"brain", "dashboard", "unpoller"},
		},
		{
			name:     "whitespace and carriage returns",
			input:    "  brain  \r\n\r\n  gitsync\r\n  dashboard \r\n",
			expected: []string{"brain", "dashboard"},
		},
		{
			name:     "case-insensitive gitsync filter",
			input:    "GitSync\nGITSYNC\ngitsync\nbrain\n",
			expected: []string{"brain"},
		},
		{
			name:     "only gitsync present",
			input:    "gitsync\n",
			expected: []string{},
		},
		{
			name:     "empty input",
			input:    "",
			expected: []string{},
		},
		{
			name:     "duplicate service entries preserved uniquely",
			input:    "brain\nunpoller\nbrain\nunpoller\n",
			expected: []string{"brain", "unpoller"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := parseComposeServices(tt.input)
			if len(actual) != len(tt.expected) {
				t.Fatalf("expected %d targets (%v), got %d (%v)", len(tt.expected), tt.expected, len(actual), actual)
			}
			for i := range tt.expected {
				if actual[i] != tt.expected[i] {
					t.Errorf("actual[%d] = %q, want %q", i, actual[i], tt.expected[i])
				}
			}
		})
	}
}

func TestGetRepoCommitAndStatus(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize git repo and make a commit
	cmdInit := exec.Command("git", "init", "-b", "main", tempDir)
	_ = cmdInit.Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	testFile := filepath.Join(tempDir, "README.md")
	_ = os.WriteFile(testFile, []byte("# Test Repo\n"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "Initial commit").Run()

	ctx := context.Background()
	sha, ts, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("getRepoCommit failed: %v", err)
	}
	if sha == "" || ts == nil {
		t.Fatalf("expected non-empty sha and timestamp, got sha=%q, ts=%v", sha, ts)
	}

	daemon := &SyncDaemon{
		interval:      time.Minute,
		repos:         []string{tempDir},
		lastSyncTimes: make(map[string]time.Time),
	}
	status := daemon.GetStatus(ctx)
	if status.Status == "" {
		t.Errorf("expected valid status string")
	}
	if len(status.Repos) != 1 {
		t.Errorf("expected 1 repo status, got %d", len(status.Repos))
	}
}

func TestResolveGitDir(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Directory does not exist
	_, err := resolveGitDir(filepath.Join(tempDir, "nonexistent"))
	if err == nil {
		t.Errorf("expected error for nonexistent directory, got nil")
	}

	// 2. Standard .git directory
	standardRepo := filepath.Join(tempDir, "standard")
	_ = os.MkdirAll(filepath.Join(standardRepo, ".git"), 0755)
	gitDir, err := resolveGitDir(standardRepo)
	if err != nil {
		t.Fatalf("resolveGitDir failed on standard repo: %v", err)
	}
	if gitDir != filepath.Join(standardRepo, ".git") {
		t.Errorf("expected %s, got %s", filepath.Join(standardRepo, ".git"), gitDir)
	}

	// 3. Worktree .git file with absolute target
	worktreeRepo := filepath.Join(tempDir, "worktree")
	_ = os.MkdirAll(worktreeRepo, 0755)
	targetGitDir := filepath.Join(tempDir, "target-git-dir")
	_ = os.MkdirAll(targetGitDir, 0755)
	_ = os.WriteFile(filepath.Join(worktreeRepo, ".git"), []byte("gitdir: "+targetGitDir+"\n"), 0644)
	gitDir, err = resolveGitDir(worktreeRepo)
	if err != nil {
		t.Fatalf("resolveGitDir failed on worktree repo: %v", err)
	}
	if gitDir != targetGitDir {
		t.Errorf("expected %s, got %s", targetGitDir, gitDir)
	}

	// 4. Worktree .git file with relative target
	relWorktreeRepo := filepath.Join(tempDir, "relworktree")
	_ = os.MkdirAll(relWorktreeRepo, 0755)
	_ = os.WriteFile(filepath.Join(relWorktreeRepo, ".git"), []byte("gitdir: ../target-git-dir\n"), 0644)
	gitDir, err = resolveGitDir(relWorktreeRepo)
	if err != nil {
		t.Fatalf("resolveGitDir failed on relative worktree repo: %v", err)
	}
	if gitDir != targetGitDir {
		t.Errorf("expected %s, got %s", targetGitDir, gitDir)
	}

	// 5. Worktree .git file without gitdir prefix
	plainFileRepo := filepath.Join(tempDir, "plainfile")
	_ = os.MkdirAll(plainFileRepo, 0755)
	_ = os.WriteFile(filepath.Join(plainFileRepo, ".git"), []byte("not_a_gitdir"), 0644)
	gitDir, err = resolveGitDir(plainFileRepo)
	if err != nil || gitDir != filepath.Join(plainFileRepo, ".git") {
		t.Errorf("expected %s, got %s (err=%v)", filepath.Join(plainFileRepo, ".git"), gitDir, err)
	}
}

func TestValidateCompose(t *testing.T) {
	tempDir := t.TempDir()
	daemon := &SyncDaemon{}
	ctx := context.Background()

	// Missing compose file returns error
	err := daemon.ValidateCompose(ctx, tempDir)
	if err == nil {
		t.Errorf("expected error when compose file missing, got nil")
	}

	// Create dummy compose file
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	if err := os.WriteFile(composePath, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	// Mock docker binary
	binDir := t.TempDir()
	mockDocker := filepath.Join(binDir, "docker")
	script := `#!/bin/sh
if [ "$MOCK_DOCKER_FAIL" = "1" ]; then
	echo "mock compose error with token ghp_1234567890abcdef1234567890abcdef12" >&2
	exit 1
fi
exit 0
`
	if err := os.WriteFile(mockDocker, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+":"+oldPath)

	// Successful validation
	if err := daemon.ValidateCompose(ctx, tempDir); err != nil {
		t.Errorf("expected nil error on valid compose, got %v", err)
	}

	// Failed validation
	t.Setenv("MOCK_DOCKER_FAIL", "1")
	err = daemon.ValidateCompose(ctx, tempDir)
	if err == nil {
		t.Errorf("expected error when mock docker fails, got nil")
	}
	if strings.Contains(err.Error(), "ghp_1234567890abcdef1234567890abcdef12") {
		t.Errorf("expected sanitized error, got raw secret: %v", err)
	}
}

func TestGetReconcileTargets(t *testing.T) {
	tempDir := t.TempDir()
	daemon := &SyncDaemon{
		composeDir: tempDir,
	}
	ctx := context.Background()

	binDir := t.TempDir()
	mockDocker := filepath.Join(binDir, "docker")
	script := `#!/bin/sh
if [ "$MOCK_SERVICES_FAIL" = "1" ]; then
	echo "discovery failure ghp_1234567890abcdef1234567890abcdef12" >&2
	exit 1
fi
printf "brain\ngitsync\nGITSYNC\ndashboard\n"
exit 0
`
	if err := os.WriteFile(mockDocker, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+":"+oldPath)

	// Success
	targets, err := daemon.GetReconcileTargets(ctx, tempDir)
	if err != nil {
		t.Fatalf("GetReconcileTargets failed: %v", err)
	}
	if len(targets) != 2 || targets[0] != "brain" || targets[1] != "dashboard" {
		t.Errorf("unexpected targets: %v", targets)
	}

	// Error
	t.Setenv("MOCK_SERVICES_FAIL", "1")
	_, err = daemon.GetReconcileTargets(ctx, tempDir)
	if err == nil {
		t.Errorf("expected error when services discovery fails, got nil")
	}
	if strings.Contains(err.Error(), "ghp_1234567890abcdef1234567890abcdef12") {
		t.Errorf("expected sanitized discovery error: %v", err)
	}
}

func TestReconcileCompose(t *testing.T) {
	tempDir := t.TempDir()
	ctx := context.Background()

	// 1. Missing compose file -> skip
	daemon := &SyncDaemon{
		composeDir: tempDir,
	}
	if err := daemon.ReconcileCompose(ctx); err != nil {
		t.Errorf("expected nil when compose missing, got %v", err)
	}

	// Create compose file
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	if err := os.WriteFile(composePath, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	mockDocker := filepath.Join(binDir, "docker")
	script := `#!/bin/sh
if [ "$1" = "compose" ]; then
	case "$*" in
		*"config --quiet"*)
			if [ "$MOCK_VAL_FAIL" = "1" ]; then
				echo "syntax err" >&2
				exit 1
			fi
			exit 0
			;;
		*"config --services"*)
			if [ "$MOCK_SERV_FAIL" = "1" ]; then
				echo "services discovery err" >&2
				exit 1
			fi
			if [ "$MOCK_ZERO_TARGETS" = "1" ]; then
				echo "gitsync"
			else
				echo "brain"
				echo "dashboard"
			fi
			exit 0
			;;
		*"up -d"*)
			if [ "$MOCK_UP_FAIL" = "1" ]; then
				echo "compose up failure with token ghp_1234567890abcdef" >&2
				exit 1
			fi
			echo "services started cleanly"
			exit 0
			;;
	esac
fi
exit 0
`
	if err := os.WriteFile(mockDocker, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// 2. Validation error
	t.Setenv("MOCK_VAL_FAIL", "1")
	if err := daemon.ReconcileCompose(ctx); err == nil {
		t.Errorf("expected validation error, got nil")
	}
	t.Setenv("MOCK_VAL_FAIL", "0")

	// 3. Service discovery error
	t.Setenv("MOCK_SERV_FAIL", "1")
	if err := daemon.ReconcileCompose(ctx); err == nil {
		t.Errorf("expected discovery error, got nil")
	}
	t.Setenv("MOCK_SERV_FAIL", "0")

	// 4. Zero targets
	t.Setenv("MOCK_ZERO_TARGETS", "1")
	if err := daemon.ReconcileCompose(ctx); err != nil {
		t.Errorf("expected nil on zero targets, got %v", err)
	}
	t.Setenv("MOCK_ZERO_TARGETS", "0")

	// 5. Compose up failure
	t.Setenv("MOCK_UP_FAIL", "1")
	err := daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Errorf("expected up failure, got nil")
	}
	if strings.Contains(err.Error(), "ghp_1234567890abcdef") {
		t.Errorf("expected sanitized compose up error: %v", err)
	}
	t.Setenv("MOCK_UP_FAIL", "0")

	// 6. Compose up success
	if err := daemon.ReconcileCompose(ctx); err != nil {
		t.Errorf("expected successful compose reconcile, got %v", err)
	}
	if daemon.lastReconcile.IsZero() {
		t.Errorf("expected lastReconcile timestamp updated")
	}

	// Default composeDir check
	daemonDefault := &SyncDaemon{
		composeDir: "",
	}
	_ = daemonDefault.ReconcileCompose(ctx)
}

func TestStartReconcilerLoop(t *testing.T) {
	daemon := &SyncDaemon{
		composeDir:  t.TempDir(),
		reconcileCh: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon.StartReconcilerLoop(ctx)
	// Signal reconcile channel
	daemon.reconcileCh <- struct{}{}
	time.Sleep(20 * time.Millisecond)

	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestStartPeriodicLoop(t *testing.T) {
	daemon := &SyncDaemon{
		interval: 10 * time.Millisecond,
		repos:    []string{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	daemon.StartPeriodicLoop(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestEnsureRepoAndSync(t *testing.T) {
	tempBase := t.TempDir()
	bareRemote := filepath.Join(tempBase, "remote.git")
	localClone := filepath.Join(tempBase, "local")

	// 1. Initialize bare remote repository
	cmdBare := exec.Command("git", "init", "--bare", "-b", "main", bareRemote)
	if out, err := cmdBare.CombinedOutput(); err != nil {
		t.Fatalf("failed to init bare remote: %s (%v)", out, err)
	}

	// 2. Initialize temporary seed repo and push to bare remote
	seedRepo := filepath.Join(tempBase, "seed")
	_ = exec.Command("git", "init", "-b", "main", seedRepo).Run()
	_ = exec.Command("git", "-C", seedRepo, "config", "user.name", "Seed").Run()
	_ = exec.Command("git", "-C", seedRepo, "config", "user.email", "seed@example.com").Run()
	_ = os.WriteFile(filepath.Join(seedRepo, "file.txt"), []byte("hello v1\n"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "initial").Run()
	_ = exec.Command("git", "-C", seedRepo, "remote", "add", "origin", bareRemote).Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "-u", "origin", "main").Run()

	// 3. EnsureRepo test on empty directory (clones from bareRemote)
	daemon := &SyncDaemon{
		repos:       []string{localClone},
		repoUrls:    map[string]string{localClone: bareRemote},
		reconcileCh: make(chan struct{}, 1),
	}
	ctx := context.Background()
	if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
		t.Fatalf("EnsureRepo failed: %v", err)
	}

	// Calling EnsureRepo again should be a no-op
	if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
		t.Fatalf("EnsureRepo second call failed: %v", err)
	}
	// Calling EnsureRepo with empty strings
	if err := daemon.EnsureRepo(ctx, "", ""); err != nil {
		t.Errorf("EnsureRepo with empty strings failed: %v", err)
	}

	// EnsureRepo on non-empty non-git directory (adoption)
	adoptDir := filepath.Join(tempBase, "adopt")
	_ = os.MkdirAll(adoptDir, 0755)
	_ = os.WriteFile(filepath.Join(adoptDir, "existing.txt"), []byte("pre-existing content"), 0644)
	if err := daemon.EnsureRepo(ctx, adoptDir, bareRemote); err != nil {
		t.Fatalf("EnsureRepo adoption failed: %v", err)
	}

	// 4. SyncRepo test when up to date
	res := daemon.SyncRepo(ctx, localClone)
	if res.Error != "" {
		t.Fatalf("SyncRepo failed: %s", res.Error)
	}
	if res.Changed {
		t.Errorf("expected changed=false when up to date")
	}

	// 5. Commit change to seedRepo and push to bareRemote
	_ = os.WriteFile(filepath.Join(seedRepo, "docker-compose.yml"), []byte("services: { app: {} }\n"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "update compose").Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "origin", "main").Run()

	// 6. SyncRepo test when changes exist
	res2 := daemon.SyncRepo(ctx, localClone)
	if res2.Error != "" {
		t.Fatalf("SyncRepo failed on update: %s", res2.Error)
	}
	if !res2.Changed {
		t.Errorf("expected changed=true after remote update")
	}
	if !res2.ComposeChanged {
		t.Errorf("expected compose_changed=true when compose file updated")
	}
	if res2.PreviousHead == res2.CurrentHead {
		t.Errorf("expected PreviousHead != CurrentHead, got %s == %s", res2.PreviousHead, res2.CurrentHead)
	}

	// 7. Test index.lock guard
	lockFile := filepath.Join(localClone, ".git", "index.lock")
	_ = os.WriteFile(lockFile, []byte(""), 0644)
	resLock := daemon.SyncRepo(ctx, localClone)
	if resLock.Error != "index.lock active" {
		t.Errorf("expected 'index.lock active', got %q", resLock.Error)
	}
	_ = os.Remove(lockFile)

	// 8. Test pull divergence recovery (simulate non-fast-forward conflict)
	_ = os.WriteFile(filepath.Join(localClone, "local_diverge.txt"), []byte("diverged"), 0644)
	_ = exec.Command("git", "-C", localClone, "add", "-A").Run()
	_ = exec.Command("git", "-C", localClone, "commit", "-m", "divergent local commit").Run()

	_ = os.WriteFile(filepath.Join(seedRepo, "remote_commit.txt"), []byte("remote update"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "remote commit").Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "origin", "main").Run()

	resRecover := daemon.SyncRepo(ctx, localClone)
	if resRecover.Error != "" {
		t.Fatalf("SyncRepo failed during reset recovery: %s", resRecover.Error)
	}

	// 9. Test TriggerSync
	results, err := daemon.TriggerSync()
	if err != nil {
		t.Fatalf("TriggerSync failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result from TriggerSync, got %d", len(results))
	}

	// 10. Test SyncRepo empty repo path & invalid repo
	emptyRes := daemon.SyncRepo(ctx, "")
	if emptyRes.Repo != "" {
		t.Errorf("expected empty repo result")
	}

	invalidRes := daemon.SyncRepo(ctx, filepath.Join(tempBase, "nonexistent"))
	if invalidRes.Error == "" {
		t.Errorf("expected error on nonexistent repo")
	}
}

func TestSyncRepoErrorBranches(t *testing.T) {
	tempBase := t.TempDir()
	ctx := context.Background()
	daemon := &SyncDaemon{}

	// 1. Repo with empty .git dir (no commits -> rev-parse HEAD fails before pull)
	emptyGitRepo := filepath.Join(tempBase, "emptygit")
	_ = os.MkdirAll(filepath.Join(emptyGitRepo, ".git"), 0755)
	resEmpty := daemon.SyncRepo(ctx, emptyGitRepo)
	if resEmpty.Error == "" {
		t.Errorf("expected error on empty git repo without commits")
	}

	// 2. Repo where pull fails and fetch also fails
	brokenOriginRepo := filepath.Join(tempBase, "brokenorigin")
	_ = exec.Command("git", "init", "-b", "main", brokenOriginRepo).Run()
	_ = exec.Command("git", "-C", brokenOriginRepo, "config", "user.name", "Tester").Run()
	_ = exec.Command("git", "-C", brokenOriginRepo, "config", "user.email", "tester@example.com").Run()
	_ = os.WriteFile(filepath.Join(brokenOriginRepo, "test.txt"), []byte("data"), 0644)
	_ = exec.Command("git", "-C", brokenOriginRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", brokenOriginRepo, "commit", "-m", "init").Run()
	_ = exec.Command("git", "-C", brokenOriginRepo, "remote", "add", "origin", "file:///nonexistent/path/git").Run()

	resBroken := daemon.SyncRepo(ctx, brokenOriginRepo)
	if !strings.Contains(resBroken.Error, "fetch failed") {
		t.Errorf("expected fetch failed error on broken remote, got: %s", resBroken.Error)
	}

	// 3. EnsureRepo failure branch in SyncRepo
	daemonEnsureFail := &SyncDaemon{
		repos:    []string{filepath.Join(tempBase, "fail_adopt")},
		repoUrls: map[string]string{filepath.Join(tempBase, "fail_adopt"): "http://invalid-non-routable-domain-1234567.com/repo.git"},
	}
	_ = os.MkdirAll(filepath.Join(tempBase, "fail_adopt"), 0755)
	_ = os.WriteFile(filepath.Join(tempBase, "fail_adopt", "file.txt"), []byte("content"), 0644)
	_ = daemonEnsureFail.SyncRepo(ctx, filepath.Join(tempBase, "fail_adopt"))
}

func TestStartReconcilerLoopExecution(t *testing.T) {
	oldDebounce := reconcilerDebounceDuration
	oldCooldown := reconcilerMinCooldown
	reconcilerDebounceDuration = 5 * time.Millisecond
	reconcilerMinCooldown = 5 * time.Millisecond
	defer func() {
		reconcilerDebounceDuration = oldDebounce
		reconcilerMinCooldown = oldCooldown
	}()

	tempDir := t.TempDir()
	daemon := &SyncDaemon{
		composeDir: tempDir,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon.StartReconcilerLoop(ctx)
	// Trigger channel
	daemon.reconcileCh <- struct{}{}
	time.Sleep(20 * time.Millisecond)

	// Trigger second time after lastReconcile updated
	daemon.lastReconcile = time.Now()
	daemon.reconcileCh <- struct{}{}
	time.Sleep(20 * time.Millisecond)
}

func TestGetRepoCommitAndStatusEdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	ctx := context.Background()

	// 1. Non-existent repo error
	_, _, err := getRepoCommit(ctx, filepath.Join(tempDir, "nonexistent"), "HEAD", "")
	if err == nil {
		t.Errorf("expected error from getRepoCommit on nonexistent repo")
	}

	// 2. Initialize repo with commit
	_ = exec.Command("git", "init", "-b", "main", tempDir).Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Tester").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "tester@example.com").Run()
	_ = os.WriteFile(filepath.Join(tempDir, "file.txt"), []byte("data"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "msg").Run()

	// Valid commit with PAT
	sha, ts, err := getRepoCommit(ctx, tempDir, "HEAD", "my_pat")
	if err != nil || sha == "" || ts == nil {
		t.Errorf("expected valid sha and ts with PAT, got sha=%s, err=%v", sha, err)
	}

	// 3. GetStatus with lagging and error repos
	pastTime := time.Now().Add(-1 * time.Hour)
	daemon := &SyncDaemon{
		repos: []string{
			tempDir,
			filepath.Join(tempDir, "nonexistent"),
		},
		lastSyncTimes: map[string]time.Time{
			tempDir: pastTime,
		},
	}

	status := daemon.GetStatus(ctx)
	if status.Status != "error" {
		t.Errorf("expected overall status 'error' when one repo fails, got %q", status.Status)
	}

	// 4. Clean status with synced repo
	cleanDaemon := &SyncDaemon{
		repos: []string{tempDir},
	}
	cleanStatus := cleanDaemon.GetStatus(ctx)
	if cleanStatus.Status != "synced" {
		t.Errorf("expected clean status 'synced', got %q", cleanStatus.Status)
	}
}

func TestNewConfigFromEnv_Custom(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("SYNC_REPOS", "/path/a, /path/b")
	t.Setenv("SYNC_INTERVAL", "30s")
	t.Setenv("GITHUB_PAT", "my_secret_pat")
	t.Setenv("AERIAL_CONFIG_REPO_URL", "https://github.com/custom/config.git")
	t.Setenv("AERIAL_PROJECT_DIR", "/custom/aerial")
	t.Setenv("AERIAL_CONFIG_DIR", "/custom/config")

	cfg := NewConfigFromEnv()
	if cfg.Port != "9090" {
		t.Errorf("expected port 9090, got %q", cfg.Port)
	}
	if len(cfg.Repos) != 2 || cfg.Repos[0] != "/path/a" || cfg.Repos[1] != "/path/b" {
		t.Errorf("unexpected repos: %v", cfg.Repos)
	}
	if cfg.Interval != 30*time.Second {
		t.Errorf("expected interval 30s, got %v", cfg.Interval)
	}
	if cfg.PAT != "my_secret_pat" {
		t.Errorf("expected pat 'my_secret_pat', got %q", cfg.PAT)
	}
	if cfg.ComposeDir != "/custom/aerial" {
		t.Errorf("expected composeDir '/custom/aerial', got %q", cfg.ComposeDir)
	}
	if cfg.ConfigDir != "/custom/config" {
		t.Errorf("expected configDir '/custom/config', got %q", cfg.ConfigDir)
	}
}

func TestNewConfigFromEnv_Defaults(t *testing.T) {
	// Clear relevant env vars
	t.Setenv("PORT", "")
	t.Setenv("SYNC_REPOS", "")
	t.Setenv("SYNC_INTERVAL", "")
	t.Setenv("GITHUB_PAT", "")
	t.Setenv("AERIAL_CONFIG_REPO_URL", "")
	t.Setenv("AERIAL_PROJECT_DIR", "")
	t.Setenv("AERIAL_CONFIG_DIR", "")

	cfg := NewConfigFromEnv()
	if cfg.Port != "8080" {
		t.Errorf("expected default port 8080, got %q", cfg.Port)
	}
	if cfg.Interval != 60*time.Second {
		t.Errorf("expected default interval 60s, got %v", cfg.Interval)
	}
	if cfg.ComposeDir != "/share/aerial" {
		t.Errorf("expected default composeDir /share/aerial, got %q", cfg.ComposeDir)
	}
	if cfg.ConfigDir != "/share/aerial-config" {
		t.Errorf("expected default configDir /share/aerial-config, got %q", cfg.ConfigDir)
	}
}

func TestDaemonLifecycleAndHTTP(t *testing.T) {
	tempDir := t.TempDir()
	cfg := DaemonConfig{
		Port:       "0",
		Repos:      []string{tempDir},
		Interval:   50 * time.Millisecond,
		ComposeDir: tempDir,
		ConfigDir:  tempDir,
	}

	daemon := NewDaemon(cfg)
	if daemon.interval != 50*time.Millisecond {
		t.Errorf("expected interval 50ms, got %v", daemon.interval)
	}

	defaultDaemon := NewDaemon(DaemonConfig{Interval: 0})
	if defaultDaemon.interval != 60*time.Second {
		t.Errorf("expected default interval 60s, got %v", defaultDaemon.interval)
	}

	mux := SetupMux(daemon)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. GET /health
	resp, err := http.Get(srv.URL + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /health failed: %v, status=%d", err, resp.StatusCode)
	}
	resp.Body.Close()

	// 2. GET /metrics
	respMetrics, err := http.Get(srv.URL + "/metrics")
	if err != nil || respMetrics.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics failed: %v, status=%d", err, respMetrics.StatusCode)
	}
	respMetrics.Body.Close()

	// 3. /status endpoint method validation
	respBadMethod, _ := http.Post(srv.URL+"/status", "application/json", nil)
	if respBadMethod.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /status, got %d", respBadMethod.StatusCode)
	}
	respBadMethod.Body.Close()

	// 4. /sync endpoint method validation
	respBadSync, _ := http.Get(srv.URL + "/sync")
	if respBadSync.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on GET /sync, got %d", respBadSync.StatusCode)
	}
	respBadSync.Body.Close()

	// 5. POST /sync success
	respSync, err := http.Post(srv.URL+"/sync", "application/json", nil)
	if err != nil || respSync.StatusCode != http.StatusOK {
		t.Fatalf("POST /sync failed: %v, status=%d", err, respSync.StatusCode)
	}
	respSync.Body.Close()

	// 5b. POST /sync error
	daemon.triggerFn = func() ([]RepoSyncResult, error) {
		return nil, errors.New("mock trigger sync failure")
	}
	respSyncErr, err := http.Post(srv.URL+"/sync", "application/json", nil)
	if err != nil || respSyncErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on POST /sync error, got %v (status=%d)", err, respSyncErr.StatusCode)
	}
	respSyncErr.Body.Close()
	daemon.triggerFn = nil

	// 6. Test RunDaemon graceful cancel
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunDaemon(ctx, DaemonConfig{
			Port:     "0",
			Interval: 100 * time.Millisecond,
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case runErr := <-done:
		if runErr != nil {
			t.Errorf("RunDaemon returned error: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunDaemon did not shut down in time")
	}

	// 7. Test RunDaemon server error (invalid port)
	errBadPort := RunDaemon(context.Background(), DaemonConfig{
		Port: "invalid-port-string",
	})
	if errBadPort == nil {
		t.Errorf("expected error on invalid port, got nil")
	}
}



