package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

const mockDockerSrc = `package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	args := strings.Join(os.Args[1:], " ")
	if os.Getenv("MOCK_DOCKER_FAIL") == "1" {
		fmt.Fprintln(os.Stderr, "mock compose error with token ghp_1234567890abcdef1234567890abcdef12")
		os.Exit(1)
	}
	if strings.Contains(args, "config --quiet") {
		if os.Getenv("MOCK_VAL_FAIL") == "1" {
			fmt.Fprintln(os.Stderr, "syntax err")
			os.Exit(1)
		}
		os.Exit(0)
	}
	if strings.Contains(args, "config --services") {
		if os.Getenv("MOCK_SERV_FAIL") == "1" || os.Getenv("MOCK_SERVICES_FAIL") == "1" {
			fmt.Fprintln(os.Stderr, "services discovery err ghp_1234567890abcdef1234567890abcdef12")
			os.Exit(1)
		}
		if os.Getenv("MOCK_ZERO_TARGETS") == "1" {
			fmt.Println("gitsync")
		} else {
			fmt.Println("brain")
			fmt.Println("gitsync")
			fmt.Println("GITSYNC")
			fmt.Println("dashboard")
		}
		os.Exit(0)
	}
	if strings.Contains(args, "up -d") {
		if os.Getenv("MOCK_UP_FAIL") == "1" {
			fmt.Fprintln(os.Stderr, "compose up failure with token ghp_1234567890abcdef")
			os.Exit(1)
		}
		fmt.Println("services started cleanly")
		os.Exit(0)
	}
	os.Exit(0)
}
`

var (
	mockDockerBinOnce sync.Once
	mockDockerBinPath string
	mockDockerBinErr  error
)

func getMockDockerBin(t *testing.T) string {
	t.Helper()
	mockDockerBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mock-docker-*")
		if err != nil {
			mockDockerBinErr = err
			return
		}
		ext := ""
		if runtime.GOOS == "windows" {
			ext = ".exe"
		}
		target := filepath.Join(dir, "docker"+ext)
		srcFile := filepath.Join(dir, "main.go")
		if err := os.WriteFile(srcFile, []byte(mockDockerSrc), 0644); err != nil {
			mockDockerBinErr = err
			return
		}
		cmd := exec.Command("go", "build", "-o", target, srcFile)
		if out, err := cmd.CombinedOutput(); err != nil {
			mockDockerBinErr = fmt.Errorf("failed to build mock docker: %s (%w)", string(out), err)
			return
		}
		mockDockerBinPath = target
	})
	if mockDockerBinErr != nil {
		t.Fatalf("getMockDockerBin error: %v", mockDockerBinErr)
	}
	return mockDockerBinPath
}

func setupMockDocker(t *testing.T) {
	t.Helper()
	binPath := getMockDockerBin(t)
	binDir := filepath.Dir(binPath)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
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

	setupMockDocker(t)

	// Successful validation
	if err := daemon.ValidateCompose(ctx, tempDir); err != nil {
		t.Errorf("expected nil error on valid compose, got %v", err)
	}

	// Failed validation
	t.Setenv("MOCK_DOCKER_FAIL", "1")
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

	setupMockDocker(t)

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

	setupMockDocker(t)

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

	setupMockDocker(t)
	t.Setenv("MOCK_VAL_FAIL", "1")

	daemon := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		Repos:      []string{tempDir},
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

	setupMockDocker(t)
	t.Setenv("MOCK_UP_FAIL", "1")

	daemon := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		Repos:      []string{tempDir},
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

	// 1. Explicit daemon config field overrides everything
	d1 := NewDaemon(DaemonConfig{
		DiscordChannel: "override-channel",
		ConfigDir:      tempDir,
	})
	if ch := d1.ResolveAlertChannel(); ch != "override-channel" {
		t.Errorf("expected 'override-channel', got %q", ch)
	}

	// 2. DISCORD_CHANNEL env var
	t.Setenv("DISCORD_CHANNEL", "env-channel")
	d2 := NewDaemon(DaemonConfig{
		ConfigDir: tempDir,
	})
	if ch := d2.ResolveAlertChannel(); ch != "env-channel" {
		t.Errorf("expected 'env-channel', got %q", ch)
	}
	t.Setenv("DISCORD_CHANNEL", "")

	// 3. Dynamic read from config.yaml in ConfigDir
	configContent := "system_channel: \"ops-alerts\"\ntimezone: \"America/Los_Angeles\"\n"
	_ = os.WriteFile(filepath.Join(tempDir, "config.yaml"), []byte(configContent), 0644)
	d3 := NewDaemon(DaemonConfig{
		ConfigDir: tempDir,
	})
	if ch := d3.ResolveAlertChannel(); ch != "ops-alerts" {
		t.Errorf("expected 'ops-alerts' from config.yaml, got %q", ch)
	}

	// 4. Invalid YAML falls back to "aerial-dev"
	tempDirBad := t.TempDir()
	_ = os.WriteFile(filepath.Join(tempDirBad, "config.yaml"), []byte("invalid: [yaml\n"), 0644)
	d4 := NewDaemon(DaemonConfig{
		ConfigDir: tempDirBad,
	})
	if ch := d4.ResolveAlertChannel(); ch != "aerial-dev" {
		t.Errorf("expected fallback 'aerial-dev' on invalid yaml, got %q", ch)
	}

	// 5. Missing config.yaml falls back to "aerial-dev"
	tempDirEmpty := t.TempDir()
	d5 := NewDaemon(DaemonConfig{
		ConfigDir: tempDirEmpty,
	})
	if ch := d5.ResolveAlertChannel(); ch != "aerial-dev" {
		t.Errorf("expected fallback 'aerial-dev' on missing config.yaml, got %q", ch)
	}
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

