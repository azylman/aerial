package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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
		if strings.HasPrefix(e, "GIT_CONFIG_COUNT=") || strings.HasPrefix(e, "GIT_CONFIG_KEY_") || strings.HasPrefix(e, "GIT_CONFIG_VALUE_") {
			t.Errorf("unexpected auth GIT_CONFIG in empty PAT env: %s", e)
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

	// Successful validation with mock executor
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("valid"), nil, nil
	}
	if err := daemon.ValidateCompose(ctx, tempDir); err != nil {
		t.Errorf("expected nil error on valid compose, got %v", err)
	}

	// Failed validation with secret token in stderr
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("mock compose error with token ghp_1234567890abcdef1234567890abcdef12"), errors.New("exit status 1")
	}
	err = daemon.ValidateCompose(ctx, tempDir)
	if err == nil {
		t.Errorf("expected error when mock docker fails, got nil")
	} else if strings.Contains(err.Error(), "ghp_1234567890abcdef1234567890abcdef12") {
		t.Errorf("expected sanitized error, got raw secret: %v", err)
	}
}

func TestGetReconcileTargets(t *testing.T) {
	tempDir := t.TempDir()
	daemon := &SyncDaemon{
		composeDir: tempDir,
	}
	ctx := context.Background()

	// Success
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		output := "brain\ngitsync\nGITSYNC\ndashboard\n"
		return []byte(output), nil, nil
	}
	targets, err := daemon.GetReconcileTargets(ctx, tempDir)
	if err != nil {
		t.Fatalf("GetReconcileTargets failed: %v", err)
	}
	if len(targets) != 2 || targets[0] != "brain" || targets[1] != "dashboard" {
		t.Errorf("unexpected targets: %v", targets)
	}

	// Error with secret token in stderr
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("services discovery err ghp_1234567890abcdef1234567890abcdef12"), errors.New("exit status 1")
	}
	_, err = daemon.GetReconcileTargets(ctx, tempDir)
	if err == nil {
		t.Errorf("expected error when services discovery fails, got nil")
	} else if strings.Contains(err.Error(), "ghp_1234567890abcdef1234567890abcdef12") {
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

	// 2. Validation error
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, []byte("syntax err"), errors.New("exit status 1")
		}
		return nil, nil, nil
	}
	if err := daemon.ReconcileCompose(ctx); err == nil {
		t.Errorf("expected validation error, got nil")
	}

	// 3. Service discovery error
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return nil, []byte("services discovery err ghp_1234567890abcdef1234567890abcdef12"), errors.New("exit status 1")
		}
		return nil, nil, nil
	}
	if err := daemon.ReconcileCompose(ctx); err == nil {
		t.Errorf("expected discovery error, got nil")
	}

	// 4. Zero targets
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return []byte("gitsync\n"), nil, nil
		}
		return nil, nil, nil
	}
	if err := daemon.ReconcileCompose(ctx); err != nil {
		t.Errorf("expected nil on zero targets, got %v", err)
	}

	// 5. Compose up failure
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return []byte("brain\ndashboard\n"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			return nil, []byte("compose up failure with token ghp_1234567890abcdef"), errors.New("exit status 1")
		}
		return nil, nil, nil
	}
	err := daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Errorf("expected up failure, got nil")
	}
	if strings.Contains(err.Error(), "ghp_1234567890abcdef") {
		t.Errorf("expected sanitized compose up error: %v", err)
	}

	// 6. Compose up success
	daemon.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return []byte("brain\ndashboard\n"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			return []byte("services started cleanly\n"), nil, nil
		}
		return nil, nil, nil
	}
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

func TestNewConfigFromLookup_Custom(t *testing.T) {
	envMap := map[string]string{
		"PORT":                   "9090",
		"SYNC_REPOS":             "/path/a, /path/b",
		"SYNC_INTERVAL":          "30s",
		"GITHUB_PAT":             "my_secret_pat",
		"AERIAL_CONFIG_REPO_URL": "https://github.com/custom/config.git",
		"AERIAL_PROJECT_DIR":     "/custom/aerial",
		"AERIAL_CONFIG_DIR":      "/custom/config",
		"DISCORD_BOT_TOKEN":      "bot_token",
		"DISCORD_CHANNEL":        "custom-channel",
		"DISCORD_WEBHOOK_URL":    "https://discord.com/api/webhooks/123/abc",
		"BRAIN_INTERNAL_URL":     "http://brain:9999/reload",
		"POSTGRES_PASSWORD":      "pg_pass",
		"HA_TOKEN":               "ha_tok",
		"GEMINI_API_KEY":         "gem_key",
	}
	lookup := func(k string) string {
		return envMap[k]
	}

	cfg := NewConfigFromLookup(lookup)
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
	if cfg.DiscordToken != "bot_token" {
		t.Errorf("expected discordToken 'bot_token', got %q", cfg.DiscordToken)
	}
	if cfg.DiscordChannel != "custom-channel" {
		t.Errorf("expected discordChannel 'custom-channel', got %q", cfg.DiscordChannel)
	}
	if cfg.DiscordWebhookURL != "https://discord.com/api/webhooks/123/abc" {
		t.Errorf("expected discordWebhookURL 'https://discord.com/api/webhooks/123/abc', got %q", cfg.DiscordWebhookURL)
	}
	if cfg.BrainInternalURL != "http://brain:9999/reload" {
		t.Errorf("expected brainInternalURL 'http://brain:9999/reload', got %q", cfg.BrainInternalURL)
	}
	if len(cfg.ExtraSecrets) != 4 {
		t.Errorf("expected 4 extraSecrets, got %d (%v)", len(cfg.ExtraSecrets), cfg.ExtraSecrets)
	}
}

func TestNewConfigFromLookup_Defaults(t *testing.T) {
	// Empty lookup returns defaults
	cfg := NewConfigFromLookup(func(string) string { return "" })
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
	if cfg.BrainInternalURL != "http://brain:8080/internal/reload" {
		t.Errorf("expected default brainInternalURL 'http://brain:8080/internal/reload', got %q", cfg.BrainInternalURL)
	}
	if len(cfg.ExtraSecrets) != 0 {
		t.Errorf("expected 0 extraSecrets with empty lookup, got %v", cfg.ExtraSecrets)
	}

	// Nil lookup safely behaves the same as empty lookup
	cfgNil := NewConfigFromLookup(nil)
	if cfgNil.Port != "8080" {
		t.Errorf("expected default port 8080 with nil lookup, got %q", cfgNil.Port)
	}
}

func TestNewConfigFromEnv_Smoke(t *testing.T) {
	cfg := NewConfigFromEnv()
	if cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv, got %q", cfg.Port)
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

// TestComposeGraph_NoGitsyncDependents parses docker-compose.yml to assert that zero services
// declare a dependency on gitsync. This guarantees indegree(gitsync) == 0 so that external
// compose commands targeting any other service (or gitops reconcile commands) will never recreate gitsync.
func TestComposeGraph_NoGitsyncDependents(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.yml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", composePath, err)
	}

	lines := strings.Split(string(data), "\n")
	inDependsOn := false
	currentService := ""

	for lineNum, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(trimmed, ":") {
			currentService = strings.TrimSuffix(trimmed, ":")
			inDependsOn = false
		}
		if strings.HasPrefix(line, "    depends_on:") {
			inDependsOn = true
			continue
		}
		if inDependsOn {
			if len(line) > 0 && line[0] != ' ' {
				inDependsOn = false
				currentService = ""
			} else if strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "    depends_on:") && strings.HasSuffix(trimmed, ":") {
				inDependsOn = false
			} else if strings.Contains(line, "gitsync:") || trimmed == "- gitsync" {
				t.Fatalf("line %d: service %q declares dependency on gitsync (%s); gitsync must have in-degree 0 in Compose DAG", lineNum+1, currentService, trimmed)
			}
		}
	}
}

func TestAutomatedRollback_OnValidationFailure(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize git repo with 2 commits
	_ = exec.Command("git", "init", "-b", "main", tempDir).Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	composePath := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v1\n"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "Valid compose v1").Run()

	ctx := context.Background()
	head1, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit 1: %v", err)
	}

	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v2\n    bad: [invalid\n"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "Invalid compose v2").Run()

	head2, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit 2: %v", err)
	}

	valMockExecutor := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return nil, []byte("syntax err"), errors.New("exit status 1")
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			return []byte("restored\n"), nil, nil
		}
		return nil, nil, nil
	}

	daemon := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		Repos:           []string{tempDir},
		ComposeExecutor: valMockExecutor,
	})
	daemon.recordPendingChange(ComposeChangeEvent{
		RepoPath:     tempDir,
		PreviousHead: head1,
		CurrentHead:  head2,
		Timestamp:    time.Now(),
	})

	err = daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Fatalf("expected ReconcileCompose to fail on validation error, got nil")
	}

	// Verify working tree was rolled back to head1
	currentHead, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit after rollback: %v", err)
	}
	if currentHead != head1 {
		t.Errorf("expected rolled back HEAD to be %s, got %s", head1, currentHead)
	}

	// Verify head2 is quarantined
	rec, quarantined := daemon.isQuarantined(tempDir, head2)
	if !quarantined {
		t.Fatalf("expected commit %s to be quarantined", head2)
	}
	if rec.FailureStage != "pre-flight validation" {
		t.Errorf("expected failure stage 'pre-flight validation', got %s", rec.FailureStage)
	}
	if rec.PreviousHead != head1 {
		t.Errorf("expected quarantine record previousHead %s, got %s", head1, rec.PreviousHead)
	}

	// Verify status reports quarantined
	status := daemon.GetStatus(ctx)
	if status.Status != "quarantined" {
		t.Errorf("expected overall status 'quarantined', got %s", status.Status)
	}
	repoSt, ok := status.Repos[tempDir]
	if !ok || !repoSt.Quarantined {
		t.Errorf("expected repo %s to be reported as quarantined: %+v", tempDir, repoSt)
	}
	if repoSt.QuarantinedCommit != head2 {
		t.Errorf("expected quarantined commit %s, got %s", head2, repoSt.QuarantinedCommit)
	}
}

func TestAutomatedRollback_OnComposeUpFailure(t *testing.T) {
	tempDir := t.TempDir()

	_ = exec.Command("git", "init", "-b", "main", tempDir).Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", tempDir, "config", "user.email", "test@example.com").Run()

	composePath := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v1\n"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "Valid compose v1").Run()

	ctx := context.Background()
	head1, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit 1: %v", err)
	}

	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v2\n"), 0644)
	_ = exec.Command("git", "-C", tempDir, "add", "-A").Run()
	_ = exec.Command("git", "-C", tempDir, "commit", "-m", "Failing compose v2").Run()

	head2, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit 2: %v", err)
	}

	var upCallCount int
	upMockExecutor := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--quiet") {
			return []byte("valid\n"), nil, nil
		}
		if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			upCallCount++
			if upCallCount == 1 {
				return nil, []byte("compose up failure with token ghp_1234567890abcdef"), errors.New("exit status 1")
			}
			return []byte("restored\n"), nil, nil
		}
		return nil, nil, nil
	}

	daemon := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		Repos:           []string{tempDir},
		ComposeExecutor: upMockExecutor,
	})
	daemon.recordPendingChange(ComposeChangeEvent{
		RepoPath:     tempDir,
		PreviousHead: head1,
		CurrentHead:  head2,
		Timestamp:    time.Now(),
	})

	err = daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Fatalf("expected ReconcileCompose to fail on compose up, got nil")
	}

	// Verify working tree rolled back to head1
	currentHead, _, err := getRepoCommit(ctx, tempDir, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get commit after rollback: %v", err)
	}
	if currentHead != head1 {
		t.Errorf("expected rolled back HEAD to be %s, got %s", head1, currentHead)
	}

	// Verify head2 is quarantined with failure stage compose apply
	rec, quarantined := daemon.isQuarantined(tempDir, head2)
	if !quarantined {
		t.Fatalf("expected commit %s to be quarantined", head2)
	}
	if rec.FailureStage != "compose apply" {
		t.Errorf("expected failure stage 'compose apply', got %s", rec.FailureStage)
	}
}

func TestQuarantine_PreventsPullLoop(t *testing.T) {
	tempBase := t.TempDir()
	bareRemote := filepath.Join(tempBase, "remote.git")
	seedRepo := filepath.Join(tempBase, "seed")
	localClone := filepath.Join(tempBase, "local")

	// 1. Bare remote
	if out, err := exec.Command("git", "init", "--bare", "-b", "main", bareRemote).CombinedOutput(); err != nil {
		t.Fatalf("failed to init bare remote: %s (%v)", out, err)
	}

	// 2. Seed repo
	_ = exec.Command("git", "init", "-b", "main", seedRepo).Run()
	_ = exec.Command("git", "-C", seedRepo, "config", "user.name", "Seed").Run()
	_ = exec.Command("git", "-C", seedRepo, "config", "user.email", "seed@example.com").Run()
	_ = os.WriteFile(filepath.Join(seedRepo, "file.txt"), []byte("v1\n"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "commit 1").Run()
	_ = exec.Command("git", "-C", seedRepo, "remote", "add", "origin", bareRemote).Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "-u", "origin", "main").Run()

	// 3. Setup local clone
	daemon := NewDaemon(DaemonConfig{
		Repos:    []string{localClone},
		RepoURLs: map[string]string{localClone: bareRemote},
	})
	ctx := context.Background()
	if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
		t.Fatalf("EnsureRepo failed: %v", err)
	}

	head1, _, err := getRepoCommit(ctx, localClone, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get head1: %v", err)
	}

	// 4. Commit 2 to seed and push to bare remote
	_ = os.WriteFile(filepath.Join(seedRepo, "file.txt"), []byte("v2 bad commit\n"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "commit 2 bad").Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "origin", "main").Run()

	head2, _, err := getRepoCommit(ctx, seedRepo, "HEAD", "")
	if err != nil {
		t.Fatalf("failed to get head2: %v", err)
	}

	// 5. Quarantine commit 2 in daemon
	daemon.quarantineCommit(localClone, head2, head1, "pre-flight validation", "broken syntax")

	// 6. Attempt sync: should skip pull and return error mentioning quarantine
	res := daemon.SyncRepo(ctx, localClone)
	if !strings.Contains(res.Error, "quarantined") {
		t.Errorf("expected sync to be blocked by quarantine, got error: %q", res.Error)
	}

	// Verify local clone is STILL at head1
	currentHead, _, _ := getRepoCommit(ctx, localClone, "HEAD", "")
	if currentHead != head1 {
		t.Errorf("expected local clone to remain at %s, but advanced to %s", head1, currentHead)
	}

	// 7. Seed advances to commit 3 (clean fix)
	_ = os.WriteFile(filepath.Join(seedRepo, "file.txt"), []byte("v3 good fix\n"), 0644)
	_ = exec.Command("git", "-C", seedRepo, "add", "-A").Run()
	_ = exec.Command("git", "-C", seedRepo, "commit", "-m", "commit 3 good").Run()
	_ = exec.Command("git", "-C", seedRepo, "push", "origin", "main").Run()

	head3, _, _ := getRepoCommit(ctx, seedRepo, "HEAD", "")

	// 8. Attempt sync: should succeed, advance to head3, and clear quarantine for head2
	res2 := daemon.SyncRepo(ctx, localClone)
	if res2.Error != "" {
		t.Fatalf("expected successful sync after upstream fix, got: %s", res2.Error)
	}
	if !res2.Changed {
		t.Errorf("expected changed=true after syncing head3")
	}
	currentHead2, _, _ := getRepoCommit(ctx, localClone, "HEAD", "")
	if currentHead2 != head3 {
		t.Errorf("expected local clone to advance to %s, got %s", head3, currentHead2)
	}

	// Verify quarantine for head2 was cleared
	if _, isQ := daemon.isQuarantined(localClone, head2); isQ {
		t.Errorf("expected quarantine for head2 to be cleared after clean advance to head3")
	}
}

type urlRewritingTransport struct {
	targetBase string
}

func (t *urlRewritingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite https://discord.com to targetBase
	if strings.HasPrefix(req.URL.String(), "https://discord.com") {
		rewritten := strings.Replace(req.URL.String(), "https://discord.com", t.targetBase, 1)
		newReq, err := http.NewRequestWithContext(req.Context(), req.Method, rewritten, req.Body)
		if err != nil {
			return nil, err
		}
		newReq.Header = req.Header
		return http.DefaultTransport.RoundTrip(newReq)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func TestDiscordAlert_FormattingAndTruncation(t *testing.T) {
	var receivedPayloads []map[string]interface{}
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if strings.Contains(r.URL.Path, "webhook") {
			var payload map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			receivedPayloads = append(receivedPayloads, payload)
			w.WriteHeader(http.StatusOK)
			return
		}

		if strings.HasSuffix(r.URL.Path, "/guilds") {
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"id": "guild_123"},
			})
			return
		}

		if strings.HasSuffix(r.URL.Path, "/channels") && r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{
				{"id": "chan_456", "name": "aerial-dev", "type": 0},
			})
			return
		}

		if strings.Contains(r.URL.Path, "/channels/chan_456/messages") || strings.Contains(r.URL.Path, "/channels/snowflake_789/messages") {
			var payload map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			receivedPayloads = append(receivedPayloads, payload)
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	customClient := &http.Client{
		Transport: &urlRewritingTransport{targetBase: srv.URL},
		Timeout:   5 * time.Second,
	}

	daemon := NewDaemon(DaemonConfig{
		DiscordWebhookURL: srv.URL + "/webhook",
	})
	daemon.alertHTTPClient = customClient

	// 1. Send alert via webhook with large error (>1200 chars) and sensitive secrets
	longError := "sensitive token ghp_1234567890abcdef1234567890abcdef12 and secret\n" + strings.Repeat("error detail line with some context\n", 40)
	ctx := context.Background()

	daemon.SendDiscordAlert(ctx, "Test Rollback", "/share/aerial", "badsha123", "goodsha456", "compose apply", longError)

	mu.Lock()
	if len(receivedPayloads) != 1 {
		t.Fatalf("expected 1 webhook payload, got %d", len(receivedPayloads))
	}
	content := receivedPayloads[0]["content"].(string)
	mu.Unlock()

	if !strings.Contains(content, "🚨 **GitSync GitOps Rollback Alert: Test Rollback**") {
		t.Errorf("expected alert header in payload, got: %s", content)
	}
	if !strings.Contains(content, "[... truncated for length]") {
		t.Errorf("expected error truncation marker in payload")
	}
	if strings.Contains(content, "ghp_1234567890abcdef1234567890abcdef12") {
		t.Errorf("expected token to be scrubbed, but found in payload")
	}
	if !strings.Contains(content, "[REDACTED_TOKEN]") {
		t.Errorf("expected [REDACTED_TOKEN] in payload")
	}

	// 2. Deduplication: Send identical alert within 15 minutes
	daemon.SendDiscordAlert(ctx, "Test Rollback", "/share/aerial", "badsha123", "goodsha456", "compose apply", longError)
	mu.Lock()
	if len(receivedPayloads) != 1 {
		t.Errorf("expected alert to be deduplicated (count=1), got count=%d", len(receivedPayloads))
	}
	mu.Unlock()

	// 3. Test Bot token and channel name resolution
	daemonBot := NewDaemon(DaemonConfig{
		DiscordToken:   "my-bot-token",
		DiscordChannel: "aerial-dev",
	})
	daemonBot.alertHTTPClient = customClient

	daemonBot.SendDiscordAlert(ctx, "Bot API Alert", "/share/aerial", "badsha789", "goodsha000", "pre-flight validation", "syntax error")
	mu.Lock()
	if len(receivedPayloads) != 2 {
		t.Fatalf("expected 2 total payloads after bot alert, got %d", len(receivedPayloads))
	}
	botContent := receivedPayloads[1]["content"].(string)
	mu.Unlock()

	if !strings.Contains(botContent, "badsha789") {
		t.Errorf("expected faulty commit in bot alert, got: %s", botContent)
	}

	// 4. Test direct snowflake channel ID
	daemonSnowflake := NewDaemon(DaemonConfig{
		DiscordToken:   "my-bot-token",
		DiscordChannel: "123456789012345678",
	})
	daemonSnowflake.alertHTTPClient = customClient
	if !isSnowflake(daemonSnowflake.discordChannel) {
		t.Errorf("expected 123456789012345678 to be recognized as snowflake")
	}
}

func TestResolveAlertChannel_DynamicFromConfig(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Explicit daemon config field overrides everything, including config.yaml
	configContent := "system_channel: \"ops-alerts\"\ntimezone: \"America/Los_Angeles\"\n"
	_ = os.WriteFile(filepath.Join(tempDir, "config.yaml"), []byte(configContent), 0644)

	d1 := NewDaemon(DaemonConfig{
		DiscordChannel: "override-channel",
		ConfigDir:      tempDir,
	})
	if ch := d1.ResolveAlertChannel(); ch != "override-channel" {
		t.Errorf("expected 'override-channel', got %q", ch)
	}

	// 2. Dynamic read from config.yaml in ConfigDir when DiscordChannel is empty
	d2 := NewDaemon(DaemonConfig{
		ConfigDir: tempDir,
	})
	if ch := d2.ResolveAlertChannel(); ch != "ops-alerts" {
		t.Errorf("expected 'ops-alerts' from config.yaml, got %q", ch)
	}

	// 3. Invalid YAML falls back to "aerial-dev"
	tempDirBad := t.TempDir()
	_ = os.WriteFile(filepath.Join(tempDirBad, "config.yaml"), []byte("invalid: [yaml\n"), 0644)
	d3 := NewDaemon(DaemonConfig{
		ConfigDir: tempDirBad,
	})
	if ch := d3.ResolveAlertChannel(); ch != "aerial-dev" {
		t.Errorf("expected fallback 'aerial-dev' on invalid yaml, got %q", ch)
	}

	// 4. Missing config.yaml falls back to "aerial-dev"
	tempDirEmpty := t.TempDir()
	d4 := NewDaemon(DaemonConfig{
		ConfigDir: tempDirEmpty,
	})
	if ch := d4.ResolveAlertChannel(); ch != "aerial-dev" {
		t.Errorf("expected fallback 'aerial-dev' on missing config.yaml, got %q", ch)
	}
}

func TestSanitizeAll(t *testing.T) {
	d := &SyncDaemon{
		pat:               "ghp_myPersonalAccessToken12345",
		discordToken:      "my_discord_secret_token",
		discordWebhookURL: "https://discord.com/api/webhooks/999/super_secret_webhook",
		extraSecrets: []string{
			"pg_super_secret_password",
			"ha_long_lived_access_token_12345",
			"AIzaSyTestApiKeyForGemini1234567890",
			"abc", // length < 4, should NOT be added or sanitized as literal secret
		},
	}

	raw := "Connect to postgres://user:pg_super_secret_password@db:5432 with ghp_myPersonalAccessToken12345 and token my_discord_secret_token or webhook https://discord.com/api/webhooks/999/super_secret_webhook. Also ha_long_lived_access_token_12345 and key AIzaSyTestApiKeyForGemini1234567890 and normal word abc."

	sanitized := d.SanitizeAll(raw)

	if strings.Contains(sanitized, "pg_super_secret_password") {
		t.Errorf("expected postgres password to be redacted, got: %s", sanitized)
	}
	if strings.Contains(sanitized, "ghp_myPersonalAccessToken12345") {
		t.Errorf("expected PAT to be redacted, got: %s", sanitized)
	}
	if strings.Contains(sanitized, "my_discord_secret_token") {
		t.Errorf("expected discord token to be redacted, got: %s", sanitized)
	}
	if strings.Contains(sanitized, "super_secret_webhook") {
		t.Errorf("expected webhook URL to be redacted, got: %s", sanitized)
	}
	if strings.Contains(sanitized, "ha_long_lived_access_token_12345") {
		t.Errorf("expected HA token to be redacted, got: %s", sanitized)
	}
	if strings.Contains(sanitized, "AIzaSyTestApiKeyForGemini1234567890") {
		t.Errorf("expected Gemini API key to be redacted, got: %s", sanitized)
	}
	// "abc" must NOT be redacted because len < 4
	if !strings.Contains(sanitized, "abc") {
		t.Errorf("expected short string 'abc' to be preserved, got: %s", sanitized)
	}
}

func TestNotifyBrainReload(t *testing.T) {
	var received bool
	var receivedMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		receivedMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := &SyncDaemon{
		brainInternalURL: srv.URL,
	}

	d.notifyBrainReload()

	if !received {
		t.Errorf("expected reload notification to reach mock Brain server")
	}
	if receivedMethod != http.MethodPost {
		t.Errorf("expected POST method, got %s", receivedMethod)
	}

	// Empty brainInternalURL is safe no-op
	dEmpty := &SyncDaemon{
		brainInternalURL: "",
	}
	dEmpty.notifyBrainReload()
}

func TestRepoLock_ThreadSafety(t *testing.T) {
	daemon := NewDaemon(DaemonConfig{})

	pathA := filepath.Clean("/share/aerial")
	pathB := filepath.Clean("/share/aerial-config")

	lockA1 := daemon.getRepoLock(pathA)
	lockA2 := daemon.getRepoLock(pathA)
	lockB := daemon.getRepoLock(pathB)

	if lockA1 != lockA2 {
		t.Errorf("expected identical mutex pointer for same repo path, got %p vs %p", lockA1, lockA2)
	}
	if lockA1 == lockB {
		t.Errorf("expected distinct mutex pointers for different repo paths")
	}

	// Concurrent lock test
	var wg sync.WaitGroup
	iterations := 50
	counter := 0

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := daemon.getRepoLock(pathA)
			l.Lock()
			counter++
			l.Unlock()
		}()
	}
	wg.Wait()

	if counter != iterations {
		t.Errorf("expected counter %d, got %d", iterations, counter)
	}
}

func TestDefaultComposeExecutor(t *testing.T) {
	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create bin dir: %v", err)
	}

	fakeDocker := filepath.Join(binDir, "docker")
	scriptContent := "#!/bin/sh\ncase \"$*\" in\n  *\"version\"*) echo \"Docker Compose mock v2.0\"; exit 0;;\n  *\"sleep\"*) trap 'exit 0' TERM INT; while :; do sleep 0.05; done;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
	if err := os.WriteFile(fakeDocker, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("failed to write fake docker: %v", err)
	}

	origPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+origPath)

	ctx := context.Background()
	stdout, stderr, err := defaultComposeExecutor(ctx, tempDir, "version")
	if err != nil {
		t.Fatalf("defaultComposeExecutor failed: %v (stderr: %s)", err, stderr)
	}
	if !strings.Contains(string(stdout), "Docker Compose mock") {
		t.Errorf("unexpected stdout: %s", stdout)
	}

	// Test cancellation
	cancelCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, errCancel := defaultComposeExecutor(cancelCtx, tempDir, "sleep")
	if errCancel == nil {
		t.Errorf("expected error from canceled context, got nil")
	}
}

func TestGetComposeExecutor(t *testing.T) {
	var nilDaemon *SyncDaemon
	if nilDaemon.getComposeExecutor() == nil {
		t.Errorf("expected non-nil default executor for nil daemon")
	}

	d := &SyncDaemon{}
	if d.getComposeExecutor() == nil {
		t.Errorf("expected non-nil default executor when composeExecutor is nil")
	}

	customCalled := false
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		customCalled = true
		return []byte("custom"), nil, nil
	}
	execFn := d.getComposeExecutor()
	_, _, _ = execFn(context.Background(), "")
	if !customCalled {
		t.Errorf("expected custom compose executor to be invoked")
	}
}

func TestSendWebhook_Extended(t *testing.T) {
	d := NewDaemon(DaemonConfig{})

	// Success case
	srvSuccess := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srvSuccess.Close()

	d.sendWebhook(context.Background(), srvSuccess.Client(), srvSuccess.URL, "alert test success")

	// Failure case (HTTP 500)
	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srvFail.Close()

	d.sendWebhook(context.Background(), srvFail.Client(), srvFail.URL, "alert test 500")

	// Network error
	closedClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused mock")
			},
		},
	}
	d.sendWebhook(context.Background(), closedClient, "http://127.0.0.1:65530/webhook", "alert test error")
}

func TestResolveChannelID_Extended(t *testing.T) {
	d := NewDaemon(DaemonConfig{})

	// 1. Snowflake direct return
	chID, err := d.resolveChannelID(context.Background(), http.DefaultClient, "token", "123456789012345678")
	if err != nil || chID != "123456789012345678" {
		t.Errorf("expected snowflake return, got %s, %v", chID, err)
	}

	// 2. Cached return
	d.cachedChannelID = "999888777666"
	chID, err = d.resolveChannelID(context.Background(), http.DefaultClient, "token", "general")
	if err != nil || chID != "999888777666" {
		t.Errorf("expected cached return, got %s, %v", chID, err)
	}
	d.cachedChannelID = ""

	// 3. Discord API error (HTTP 500 from guilds)
	client500 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       ioNopCloser(strings.NewReader("server error")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, err = d.resolveChannelID(context.Background(), client500, "token", "general")
	if err == nil {
		t.Errorf("expected error on HTTP 500 guilds")
	}

	// 4. Malformed JSON from guilds
	clientBadJSON := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioNopCloser(strings.NewReader("not json")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, err = d.resolveChannelID(context.Background(), clientBadJSON, "token", "general")
	if err == nil {
		t.Errorf("expected error on bad JSON guilds")
	}

	// 5. Successful guild & channel resolution
	clientSuccess := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/guilds") && !strings.Contains(req.URL.Path, "/channels") {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       ioNopCloser(strings.NewReader(`[{"id":"guild-1"}]`)),
						Header:     make(http.Header),
					}, nil
				}
				if strings.Contains(req.URL.Path, "/channels") {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       ioNopCloser(strings.NewReader(`[{"id":"chan-42","name":"aerial-alerts","type":0}]`)),
						Header:     make(http.Header),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioNopCloser(strings.NewReader(`{}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	chID, err = d.resolveChannelID(context.Background(), clientSuccess, "token", "#aerial-alerts")
	if err != nil || chID != "chan-42" {
		t.Errorf("expected chan-42, got %s, err: %v", chID, err)
	}

	// 6. Channel not found in connected guilds
	d.cachedChannelID = ""
	_, err = d.resolveChannelID(context.Background(), clientSuccess, "token", "nonexistent-channel")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected not found error, got %v", err)
	}
}

func TestPostChannelMessage_Extended(t *testing.T) {
	d := NewDaemon(DaemonConfig{})

	// 1. Success 200
	client200 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioNopCloser(strings.NewReader(`{"id":"msg-1"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	d.postChannelMessage(context.Background(), client200, "token", "123", "hello")

	// 2. 404 Invalidate Cache
	d.cachedChannelID = "123"
	client404 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioNopCloser(strings.NewReader(`{"message":"Unknown Channel"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	d.postChannelMessage(context.Background(), client404, "token", "123", "hello")
	if d.cachedChannelID != "" {
		t.Errorf("expected cached channel to be invalidated on 404")
	}

	// 3. 500 error
	client500 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       ioNopCloser(strings.NewReader(`{"message":"Server error"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	d.postChannelMessage(context.Background(), client500, "token", "123", "hello")

	// 4. Network error
	clientErr := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network drop")
			},
		},
	}
	d.postChannelMessage(context.Background(), clientErr, "token", "123", "hello")
}

func TestExecuteRollback_Extended(t *testing.T) {
	tempDir := t.TempDir()
	mockExecutor := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("mock compose output"), nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		ComposeExecutor: mockExecutor,
	})

	// 1. Empty pending
	d.executeRollback(context.Background(), nil, "validation", errors.New("test err"))

	// 2. ch with empty previousHead or repoPath
	d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: "", PreviousHead: "abc"},
		{RepoPath: "/some/path", PreviousHead: ""},
	}, "validation", errors.New("test err"))

	// 3. Reset error branch and target discovery error
	badYamlDir := t.TempDir()
	d.composeDir = badYamlDir
	// Write invalid docker-compose.yml so GetReconcileTargets fails
	_ = os.WriteFile(filepath.Join(badYamlDir, "docker-compose.yml"), []byte("services: [invalid yaml"), 0644)

	d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: filepath.Join(badYamlDir, "nonexistent-repo"), PreviousHead: "deadbeef", CurrentHead: "cafebabe"},
	}, "compose_up", errors.New("mock up failure"))

	// 4. Valid targets but composeExecutor returns error on up -d
	validDir := t.TempDir()
	d.composeDir = validDir
	_ = os.WriteFile(filepath.Join(validDir, "docker-compose.yml"), []byte("version: '3.8'\nservices:\n  app:\n    image: alpine\n"), 0644)
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("compose up error"), errors.New("compose up failed")
	}
	d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: validDir, PreviousHead: "HEAD", CurrentHead: "HEAD"},
	}, "compose_up", errors.New("mock fail"))
}

func TestSyncRepo_ResetRecovery(t *testing.T) {
	tempDir := t.TempDir()
	originDir := filepath.Join(tempDir, "origin")
	localDir := filepath.Join(tempDir, "local")

	// Create origin repo with 2 commits
	_ = os.MkdirAll(originDir, 0755)
	runGit(t, originDir, "init", "-b", "main")
	runGit(t, originDir, "config", "user.name", "Test")
	runGit(t, originDir, "config", "user.email", "test@example.com")
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v1"), 0644)
	runGit(t, originDir, "add", "file.txt")
	runGit(t, originDir, "commit", "-m", "commit 1")

	// Clone to local
	runGit(t, tempDir, "clone", originDir, localDir)
	runGit(t, localDir, "config", "user.name", "Test")
	runGit(t, localDir, "config", "user.email", "test@example.com")

	// Add commit 2 to origin
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v2"), 0644)
	runGit(t, originDir, "commit", "-am", "commit 2")

	// Add conflicting commit to local so ff-only merge fails
	_ = os.WriteFile(filepath.Join(localDir, "file.txt"), []byte("v-local-diverged"), 0644)
	runGit(t, localDir, "commit", "-am", "local commit diverged")

	d := NewDaemon(DaemonConfig{})
	res := d.SyncRepo(context.Background(), localDir)
	if res.Error != "" {
		t.Fatalf("SyncRepo failed unexpectedly during reset recovery: %s", res.Error)
	}
	if !res.Changed {
		t.Errorf("expected Changed to be true after reset recovery")
	}
}

func TestGetStatus_Extended(t *testing.T) {
	tempDir := t.TempDir()
	runGit(t, tempDir, "init", "-b", "main")
	runGit(t, tempDir, "config", "user.name", "Test")
	runGit(t, tempDir, "config", "user.email", "test@example.com")
	_ = os.WriteFile(filepath.Join(tempDir, "README.md"), []byte("hello"), 0644)
	runGit(t, tempDir, "add", "README.md")
	runGit(t, tempDir, "commit", "-m", "init")

	d := NewDaemon(DaemonConfig{
		Repos: []string{tempDir},
	})

	// 1. Quarantined status check on repo
	d.quarantineCommit(tempDir, "deadbeef", "cafebabe", "validation", "bad syntax")
	st := d.GetStatus(context.Background())
	if st.Status != "quarantined" {
		t.Errorf("expected quarantined status, got %s", st.Status)
	}

	// 2. Quarantine match on disk commit SHA
	cmdOut, _ := exec.Command("git", "-C", tempDir, "rev-parse", "HEAD").Output()
	diskSha := strings.TrimSpace(string(cmdOut))
	d.clearQuarantineForRepo(tempDir, "")
	d.quarantineCommit(tempDir, diskSha, "prev123", "validation", "disk quarantined")
	st2 := d.GetStatus(context.Background())
	if st2.Status != "quarantined" {
		t.Errorf("expected quarantined on diskSha, got %s", st2.Status)
	}
}

func TestGetStatus_LaggingAndRemoteQuarantine(t *testing.T) {
	tempDir := t.TempDir()
	originDir := filepath.Join(tempDir, "origin")
	localDir := filepath.Join(tempDir, "local")

	_ = os.MkdirAll(originDir, 0755)
	runGit(t, originDir, "init", "-b", "main")
	runGit(t, originDir, "config", "user.name", "Test")
	runGit(t, originDir, "config", "user.email", "test@example.com")
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v1"), 0644)
	runGit(t, originDir, "add", "file.txt")
	runGit(t, originDir, "commit", "-m", "commit 1")

	runGit(t, tempDir, "clone", originDir, localDir)
	runGit(t, localDir, "config", "user.name", "Test")
	runGit(t, localDir, "config", "user.email", "test@example.com")

	// Make origin have commit 2 with a future timestamp
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v2"), 0644)
	cmdCommit2 := exec.Command("git", "-C", originDir, "commit", "-am", "commit 2")
	cmdCommit2.Env = append(os.Environ(), "GIT_COMMITTER_DATE=2035-01-01T12:00:00Z", "GIT_AUTHOR_DATE=2035-01-01T12:00:00Z")
	if out, err := cmdCommit2.CombinedOutput(); err != nil {
		t.Fatalf("commit 2 failed: %s (%v)", out, err)
	}

	// Fetch origin in local so origin/main is updated with commit 2 while HEAD remains at commit 1
	runGit(t, localDir, "fetch", "origin", "main")

	cmdOut, _ := exec.Command("git", "-C", originDir, "rev-parse", "HEAD").Output()
	remoteSha := strings.TrimSpace(string(cmdOut))

	d := NewDaemon(DaemonConfig{
		Repos: []string{localDir},
	})

	// Case 1: lagging status (remote has newer commit and future timestamp)
	stLagging := d.GetStatus(context.Background())
	if stLagging.Status != "lagging" {
		t.Errorf("expected lagging status, got %s", stLagging.Status)
	}

	// Case 2: remote commit quarantined
	d.quarantineCommit(localDir, remoteSha, "prev", "validation", "remote quarantined")
	stQuarRemote := d.GetStatus(context.Background())
	if stQuarRemote.Status != "quarantined" {
		t.Errorf("expected quarantined status for remote commit, got %s", stQuarRemote.Status)
	}
}

func TestSetupMux_Extended(t *testing.T) {
	d := NewDaemon(DaemonConfig{})
	handler := SetupMux(d)

	// 1. /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	recHealth := httptest.NewRecorder()
	handler.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Errorf("expected 200 from /health, got %d", recHealth.Code)
	}

	// 2. /status GET
	reqStatus := httptest.NewRequest(http.MethodGet, "/status", nil)
	recStatus := httptest.NewRecorder()
	handler.ServeHTTP(recStatus, reqStatus)
	if recStatus.Code != http.StatusOK {
		t.Errorf("expected 200 from /status, got %d", recStatus.Code)
	}

	// /status method not allowed
	reqStatusPost := httptest.NewRequest(http.MethodPost, "/status", nil)
	recStatusPost := httptest.NewRecorder()
	handler.ServeHTTP(recStatusPost, reqStatusPost)
	if recStatusPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from POST /status, got %d", recStatusPost.Code)
	}

	// 3. /sync method not allowed
	reqSyncGet := httptest.NewRequest(http.MethodGet, "/sync", nil)
	recSyncGet := httptest.NewRecorder()
	handler.ServeHTTP(recSyncGet, reqSyncGet)
	if recSyncGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from GET /sync, got %d", recSyncGet.Code)
	}

	// /sync POST success
	reqSyncPost := httptest.NewRequest(http.MethodPost, "/sync", nil)
	recSyncPost := httptest.NewRecorder()
	handler.ServeHTTP(recSyncPost, reqSyncPost)
	if recSyncPost.Code != http.StatusOK {
		t.Errorf("expected 200 from POST /sync, got %d", recSyncPost.Code)
	}

	// /sync POST error branch
	dErr := NewDaemon(DaemonConfig{})
	dErr.triggerFn = func() ([]RepoSyncResult, error) {
		return nil, errors.New("sync trigger failed mock")
	}
	handlerErr := SetupMux(dErr)
	recSyncErr := httptest.NewRecorder()
	handlerErr.ServeHTTP(recSyncErr, reqSyncPost)
	if recSyncErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 from failing /sync, got %d", recSyncErr.Code)
	}
}

func TestExecuteRollback_EmptyTargetsAndErrUp(t *testing.T) {
	tempDir := t.TempDir()
	// Compose file with no matching services
	_ = os.WriteFile(filepath.Join(tempDir, "docker-compose.yml"), []byte("version: '3.8'\nservices:\n  some-other-service:\n    image: alpine\n"), 0644)

	errUpCalled := false
	mockExecutor := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		errUpCalled = true
		return []byte("stdout err"), []byte("stderr err"), errors.New("up error")
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		ComposeExecutor: mockExecutor,
	})

	// Case 1: zero restoredTargets discovered (since targets filter for aerial services)
	d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: tempDir, PreviousHead: "HEAD"},
	}, "validation", errors.New("target discovery empty"))

	// Case 2: compose file with aerial service so targets > 0, but composeExecutor returns error
	aerialDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(aerialDir, "docker-compose.yml"), []byte("version: '3.8'\nservices:\n  brain:\n    image: alpine\n"), 0644)
	d.composeDir = aerialDir
	d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: aerialDir, PreviousHead: "HEAD"},
	}, "compose_up", errors.New("up error test"))

	if !errUpCalled {
		t.Errorf("expected mockExecutor to be called for up error test")
	}
}

func TestEnsureRepo_CloneError(t *testing.T) {
	tempDir := t.TempDir()
	d := NewDaemon(DaemonConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := d.EnsureRepo(ctx, filepath.Join(tempDir, "empty"), "file:///nonexistent-repo-url-xyz")
	if err == nil {
		t.Errorf("expected error from clone, got nil")
	}
}

func TestNotifyBrainReload_Error(t *testing.T) {
	d := &SyncDaemon{
		brainInternalURL: "http://127.0.0.1:65534/invalid",
	}
	d.notifyBrainReload()
}

func TestRunDaemon_ServerFatalError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := RunDaemon(ctx, DaemonConfig{
		Port: "-1",
	})
	if err == nil {
		t.Errorf("expected server fatal error on port -1, got nil")
	}
}

func TestRunDaemon_NormalLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	port := fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunDaemon(ctx, DaemonConfig{
			Port:       port,
			Interval:   time.Hour,
			ComposeDir: t.TempDir(),
			ConfigDir:  t.TempDir(),
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return []byte("ok"), nil, nil
			},
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("RunDaemon returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("RunDaemon timed out shutting down")
	}
}

func TestNilMapInitializers(t *testing.T) {
	d := &SyncDaemon{}
	d.SendDiscordAlert(context.Background(), "title", "", "", "", "stage", "msg")
	if d.lastAlertTimes == nil {
		t.Errorf("expected lastAlertTimes to be initialized")
	}

	d.quarantineCommit("/some/repo", "sha1", "prev", "val", "bad")
	if d.quarantinedCommits == nil {
		t.Errorf("expected quarantinedCommits to be initialized")
	}
}

func TestTriggerSync_AnyChanged(t *testing.T) {
	tempDir := t.TempDir()
	originDir := filepath.Join(tempDir, "origin")
	localDir := filepath.Join(tempDir, "local")

	_ = os.MkdirAll(originDir, 0755)
	runGit(t, originDir, "init", "-b", "main")
	runGit(t, originDir, "config", "user.name", "Test")
	runGit(t, originDir, "config", "user.email", "test@example.com")
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v1"), 0644)
	runGit(t, originDir, "add", "file.txt")
	runGit(t, originDir, "commit", "-m", "commit 1")

	runGit(t, tempDir, "clone", originDir, localDir)
	runGit(t, localDir, "config", "user.name", "Test")
	runGit(t, localDir, "config", "user.email", "test@example.com")

	// Add new commit to origin
	_ = os.WriteFile(filepath.Join(originDir, "file.txt"), []byte("v2"), 0644)
	runGit(t, originDir, "commit", "-am", "commit 2")

	d := NewDaemon(DaemonConfig{
		Repos: []string{localDir},
	})
	results, err := d.TriggerSync()
	if err != nil {
		t.Fatalf("TriggerSync failed: %v", err)
	}
	if len(results) != 1 || !results[0].Changed {
		t.Errorf("expected 1 result with Changed=true, got %+v", results)
	}
}

func TestEnsureRepo_NotDirectoryError(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "file-not-dir")
	_ = os.WriteFile(filePath, []byte("regular file"), 0644)
	d := NewDaemon(DaemonConfig{})
	err := d.EnsureRepo(context.Background(), filePath, "https://github.com/example/repo.git")
	if err == nil {
		t.Errorf("expected error when repoPath is a regular file")
	}
}

func TestSendDiscordAlert_ResolveFailure(t *testing.T) {
	client500 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       ioNopCloser(strings.NewReader("server error")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	d := NewDaemon(DaemonConfig{
		DiscordToken:   "mock_token",
		DiscordChannel: "unresolvable-chan",
	})
	d.alertHTTPClient = client500
	d.SendDiscordAlert(context.Background(), "title", "/repo", "sha", "prev", "stage", "err msg")
}

func TestSendWebhook_InvalidURL(t *testing.T) {
	d := NewDaemon(DaemonConfig{})
	d.sendWebhook(context.Background(), http.DefaultClient, "://invalid-url", "content")
}

type roundTripperFunc struct {
	fn func(req *http.Request) (*http.Response, error)
}

func (r *roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.fn(req)
}

func ioNopCloser(r io.Reader) io.ReadCloser {
	return io.NopCloser(r)
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed in %s: %s (%v)", args, dir, out, err)
	}
}

func TestCleanConflictContainers_AllBranches(t *testing.T) {
	ctx := context.Background()

	// 1. Error listing containers
	dErr := NewDaemon(DaemonConfig{})
	dErr.dockerExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, []byte("daemon connection refused"), errors.New("exit status 1")
	}
	if err := dErr.CleanConflictContainers(ctx); err == nil {
		t.Errorf("expected error when docker ps fails")
	}

	// 2. Empty output, malformed lines, self hostname, running containers, non-conflict, and conflict containers
	t.Setenv("HOSTNAME", "selfhost1234")
	mockOutput := strings.Join([]string{
		"",
		"only-id-no-tabs",
		"   \t  \t  ",
		"selfhost1234\t/aerial-brain\tExited (0)",
		"run123456789\t/run123456789_brain\tUp 2 hours",
		"gitsync12345\t/gitsync12345_aerial-gitsync\tExited (0)",
		"safe12345678\t/aerial-brain\tExited (0)",
		"c01111111111\t/c01111111111_brain\tExited (0)",
		"c02222222222\t/c02222222222_brain\tExited (1)",
		"c03333333333\t/c03333333333_brain\tDead",
	}, "\n")

	d := NewDaemon(DaemonConfig{})
	d.dockerExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "ps" {
			return []byte(mockOutput), nil, nil
		}
		if len(args) > 0 && args[0] == "rm" {
			id := args[len(args)-1]
			switch id {
			case "c01111111111":
				return []byte("c01111111111\n"), nil, nil
			case "c02222222222":
				return nil, []byte("Error: No such container: c02222222222"), errors.New("exit status 1")
			case "c03333333333":
				return nil, []byte("Error response from daemon: permission denied"), errors.New("exit status 1")
			}
		}
		return nil, nil, nil
	}

	if err := d.CleanConflictContainers(ctx); err != nil {
		t.Errorf("expected clean completion, got %v", err)
	}
}

func TestDefaultDockerExecutor_Coverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _ = defaultDockerExecutor(ctx, "version")
}

func TestScrubComposeEnv_Extended(t *testing.T) {
	input := []string{
		"INVALID_NO_EQUALS",
		"AERIAL_CONFIG_DIR=/dir/config",
		"AERIAL_PROJECT_DIR=/dir/project",
		"GOOD_VAR=123",
	}
	out := scrubComposeEnv(input)
	if len(out) != 1 || out[0] != "GOOD_VAR=123" {
		t.Errorf("unexpected scrubbed output: %v", out)
	}
}

func TestNotifyBrainReload_Success(t *testing.T) {
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	d := &SyncDaemon{
		brainInternalURL: ts.URL,
	}
	d.notifyBrainReload()
	if !called {
		t.Errorf("expected brain reload endpoint to be called")
	}

	// Empty URL no-op
	dEmpty := &SyncDaemon{}
	dEmpty.notifyBrainReload()
}



