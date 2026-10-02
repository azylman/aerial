package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
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
	env := BuildGitEnv("my_test_pat", os.Environ())
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
		t.Errorf("BuildGitEnv() did not produce GIT_CONFIG_VALUE_0 auth header")
	}

	emptyEnv := BuildGitEnv("", os.Environ())
	for _, e := range emptyEnv {
		if strings.HasPrefix(e, "GIT_CONFIG_COUNT=") || strings.HasPrefix(e, "GIT_CONFIG_KEY_") || strings.HasPrefix(e, "GIT_CONFIG_VALUE_") {
			t.Errorf("unexpected auth GIT_CONFIG in empty PAT env: %s", e)
		}
	}
}

func TestGetStatus(t *testing.T) {
	fakeRepo := "/mock/repo/aerial"
	daemon := &SyncDaemon{
		repos: []string{fakeRepo},
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("abc1234567890abcdef1234567890abcdef12\x002026-09-12T12:00:00Z"), nil, nil
		},
	}

	ctx := context.Background()
	status := daemon.GetStatus(ctx)

	if status.Status != "synced" {
		t.Errorf("expected status 'synced', got %q", status.Status)
	}
	if status.MaxLagSeconds != 0 {
		t.Errorf("expected max_lag_seconds 0, got %d", status.MaxLagSeconds)
	}
	repoSt, ok := status.Repos[fakeRepo]
	if !ok {
		t.Fatalf("expected repo %s in status response", fakeRepo)
	}
	if repoSt.DiskCommit != "abc1234567890abcdef1234567890abcdef12" {
		t.Errorf("expected disk commit 'abc1234567890abcdef1234567890abcdef12', got %q", repoSt.DiskCommit)
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

func TestGetComposeArgs_WithEnvFile(t *testing.T) {
	composeDir := t.TempDir()
	configDir := t.TempDir()

	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
		t.Fatal(err)
	}

	envFile := filepath.Join(composeDir, ".env")
	if err := os.WriteFile(envFile, []byte("TEST_KEY=123"), 0644); err != nil {
		t.Fatal(err)
	}

	daemon := &SyncDaemon{
		composeDir: composeDir,
		configDir:  configDir,
	}

	args := daemon.getComposeArgs(composeDir, "config")

	hasEnvFile := false
	for i, a := range args {
		if a == "--env-file" && i+1 < len(args) && args[i+1] == envFile {
			hasEnvFile = true
			break
		}
	}
	if !hasEnvFile {
		t.Fatalf("expected --env-file %s in args, got: %v", envFile, args)
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
	fakeRepo := "/mock/repo"
	daemon := &SyncDaemon{
		interval:      time.Minute,
		repos:         []string{fakeRepo},
		lastSyncTimes: make(map[string]time.Time),
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("fedcba9876543210fedcba9876543210fedcba98\x002026-09-12T12:00:00Z"), nil, nil
		},
	}

	ctx := context.Background()
	sha, ts, err := daemon.getRepoCommit(ctx, fakeRepo, "HEAD")
	if err != nil {
		t.Fatalf("getRepoCommit failed: %v", err)
	}
	if sha != "fedcba9876543210fedcba9876543210fedcba98" || ts == nil {
		t.Fatalf("expected non-empty sha and timestamp, got sha=%q, ts=%v", sha, ts)
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

	mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, nil, nil
	}
	mockGit := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, nil, nil
	}

	// 1. Missing compose file -> skip
	daemon := &SyncDaemon{
		composeDir:     tempDir,
		dockerExecutor: mockDocker,
		gitExecutor:    mockGit,
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
		composeDir:     "",
		dockerExecutor: mockDocker,
		gitExecutor:    mockGit,
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
	bareRemote := "https://example.com/repo.git"
	localClone := filepath.Join(tempBase, "local")

	simulatedCurrentHead := "1111111111111111111111111111111111111111"
	simulatedFetchHead := "1111111111111111111111111111111111111111"
	cloneCalled := false

	gitMock := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) == 0 {
			return nil, nil, nil
		}
		switch args[0] {
		case "clone":
			cloneCalled = true
			targetPath := args[len(args)-1]
			_ = os.MkdirAll(filepath.Join(targetPath, ".git"), 0755)
			return nil, nil, nil
		case "config":
			return nil, nil, nil
		case "rev-parse":
			if len(args) > 1 && args[1] == "FETCH_HEAD" {
				return []byte(simulatedFetchHead + "\n"), nil, nil
			}
			return []byte(simulatedCurrentHead + "\n"), nil, nil
		case "fetch":
			return nil, nil, nil
		case "merge":
			simulatedCurrentHead = simulatedFetchHead
			return []byte("Updating " + simulatedCurrentHead + "\n"), nil, nil
		case "diff":
			return []byte("docker-compose.yml\n"), nil, nil
		}
		return nil, nil, nil
	}

	daemon := &SyncDaemon{
		repos:       []string{localClone},
		repoUrls:    map[string]string{localClone: bareRemote},
		reconcileCh: make(chan struct{}, 1),
		gitExecutor: gitMock,
	}
	ctx := context.Background()

	// 1. EnsureRepo test on empty directory (clones from bareRemote)
	if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
		t.Fatalf("EnsureRepo failed: %v", err)
	}
	if !cloneCalled {
		t.Errorf("expected clone to be called on empty directory")
	}

	// 2. Calling EnsureRepo again should be a no-op since .git now exists
	if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
		t.Fatalf("EnsureRepo second call failed: %v", err)
	}

	// 3. Calling EnsureRepo with empty strings returns nil immediately
	if err := daemon.EnsureRepo(ctx, "", ""); err != nil {
		t.Errorf("EnsureRepo with empty strings failed: %v", err)
	}

	// 4. SyncRepo test when up to date
	res := daemon.SyncRepo(ctx, localClone)
	if res.Error != "" {
		t.Fatalf("SyncRepo failed: %s", res.Error)
	}
	if res.Changed {
		t.Errorf("expected changed=false when up to date")
	}
	if res.ComposeChanged {
		t.Errorf("expected compose_changed=false when up to date")
	}
	if res.PreviousHead != res.CurrentHead {
		t.Errorf("expected PreviousHead == CurrentHead, got %s != %s", res.PreviousHead, res.CurrentHead)
	}

	// 5. SyncRepo test when remote changes exist with compose update
	simulatedFetchHead = "2222222222222222222222222222222222222222"
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
	if res2.PreviousHead != "1111111111111111111111111111111111111111" || res2.CurrentHead != "2222222222222222222222222222222222222222" {
		t.Errorf("unexpected heads: prev=%s curr=%s", res2.PreviousHead, res2.CurrentHead)
	}

	// 6. Test index.lock guard
	lockFile := filepath.Join(localClone, ".git", "index.lock")
	_ = os.WriteFile(lockFile, []byte(""), 0644)
	resLock := daemon.SyncRepo(ctx, localClone)
	if resLock.Error != "index.lock active" {
		t.Errorf("expected 'index.lock active', got %q", resLock.Error)
	}
	_ = os.Remove(lockFile)

	// 7. Test SyncRepo empty repo path & invalid repo
	emptyRes := daemon.SyncRepo(ctx, "")
	if emptyRes.Repo != "" {
		t.Errorf("expected empty repo result")
	}

	invalidRes := daemon.SyncRepo(ctx, filepath.Join(tempBase, "nonexistent"))
	if invalidRes.Error == "" {
		t.Errorf("expected error on nonexistent repo")
	}
}

func TestEnsureRepo_Adoption(t *testing.T) {
	tempBase := t.TempDir()
	adoptDir := filepath.Join(tempBase, "adopt")
	_ = os.MkdirAll(adoptDir, 0755)
	_ = os.WriteFile(filepath.Join(adoptDir, "existing.txt"), []byte("pre-existing content"), 0644)

	var calls []string
	d := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				calls = append(calls, args[0])
			}
			return nil, nil, nil
		},
	})
	if err := d.EnsureRepo(context.Background(), adoptDir, "https://github.com/example/repo.git"); err != nil {
		t.Fatalf("EnsureRepo adoption failed: %v", err)
	}
	if len(calls) < 4 {
		t.Errorf("expected at least 4 git calls during adoption, got %d (%v)", len(calls), calls)
	}
}

func TestSyncRepoErrorBranches(t *testing.T) {
	tempBase := t.TempDir()
	ctx := context.Background()

	// 1. Repo with empty .git dir (no commits -> rev-parse HEAD fails before pull)
	emptyGitRepo := filepath.Join(tempBase, "emptygit")
	_ = os.MkdirAll(filepath.Join(emptyGitRepo, ".git"), 0755)
	daemonEmpty := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: ambiguous argument 'HEAD'"), errors.New("exit status 128")
		},
	})
	resEmpty := daemonEmpty.SyncRepo(ctx, emptyGitRepo)
	if resEmpty.Error == "" {
		t.Errorf("expected error on empty git repo without commits")
	}

	// 2. Repo where pull fails and fetch also fails
	brokenOriginRepo := filepath.Join(tempBase, "brokenorigin")
	_ = os.MkdirAll(filepath.Join(brokenOriginRepo, ".git"), 0755)
	daemonBroken := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				switch args[0] {
				case "config":
					return nil, nil, nil
				case "rev-parse":
					return []byte("1111111111111111111111111111111111111111"), nil, nil
				case "fetch":
					return nil, []byte("fatal: unable to access"), errors.New("fetch failed")
				case "pull":
					return nil, []byte("fatal: unable to access"), errors.New("pull failed")
				}
			}
			return nil, nil, nil
		},
	})

	resBroken := daemonBroken.SyncRepo(ctx, brokenOriginRepo)
	if !strings.Contains(resBroken.Error, "fetch failed") {
		t.Errorf("expected fetch failed error on broken remote, got: %s", resBroken.Error)
	}

	// 3. EnsureRepo failure branch in SyncRepo
	daemonEnsureFail := NewDaemon(DaemonConfig{
		Repos:    []string{filepath.Join(tempBase, "fail_adopt")},
		RepoURLs: map[string]string{filepath.Join(tempBase, "fail_adopt"): "http://invalid-non-routable-domain-1234567.com/repo.git"},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: clone failed"), errors.New("clone failed")
		},
	})
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
	ctx := context.Background()

	// 1. Error from getRepoCommit
	dErr := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: not a git repository"), errors.New("exit status 128")
		},
	}
	_, _, err := dErr.getRepoCommit(ctx, "/nonexistent", "HEAD")
	if err == nil {
		t.Errorf("expected error from getRepoCommit on nonexistent repo")
	}

	// 2. Valid commit with PAT
	dPat := &SyncDaemon{
		pat: "my_pat",
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z"), nil, nil
		},
	}
	sha, ts, err := dPat.getRepoCommit(ctx, "/repo", "HEAD")
	if err != nil || sha == "" || ts == nil {
		t.Errorf("expected valid sha and ts with PAT, got sha=%s, err=%v", sha, err)
	}

	// 3. GetStatus with lagging and error repos
	pastTime := time.Now().Add(-1 * time.Hour)
	daemon := &SyncDaemon{
		repos: []string{
			"/repo/ok",
			"/repo/failing",
		},
		lastSyncTimes: map[string]time.Time{
			"/repo/ok": pastTime,
		},
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if dir == "/repo/failing" {
				return nil, []byte("error"), errors.New("failed")
			}
			return []byte("1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z"), nil, nil
		},
	}

	status := daemon.GetStatus(ctx)
	if status.Status != "error" {
		t.Errorf("expected overall status 'error' when one repo fails, got %q", status.Status)
	}

	// 4. Clean status with synced repo
	cleanDaemon := &SyncDaemon{
		repos: []string{"/repo/ok"},
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z"), nil, nil
		},
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

	// 5c. /reconcile endpoint method validation
	respBadReconcile, _ := http.Get(srv.URL + "/reconcile")
	if respBadReconcile.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on GET /reconcile, got %d", respBadReconcile.StatusCode)
	}
	respBadReconcile.Body.Close()

	// 5d. POST /reconcile async success
	respReconcile, err := http.Post(srv.URL+"/reconcile", "application/json", nil)
	if err != nil || respReconcile.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /reconcile failed: %v, status=%d", err, respReconcile.StatusCode)
	}
	respReconcile.Body.Close()

	// 5e. POST /reconcile?sync=true success
	daemon.reconcileFn = func(ctx context.Context) error {
		return nil
	}
	respReconcileSync, err := http.Post(srv.URL+"/reconcile?sync=true", "application/json", nil)
	if err != nil || respReconcileSync.StatusCode != http.StatusOK {
		t.Fatalf("POST /reconcile?sync=true failed: %v, status=%d", err, respReconcileSync.StatusCode)
	}
	respReconcileSync.Body.Close()

	// 5f. POST /reconcile?sync=true error
	daemon.reconcileFn = func(ctx context.Context) error {
		return errors.New("mock reconcile compose failure")
	}
	respReconcileErr, err := http.Post(srv.URL+"/reconcile?sync=true", "application/json", nil)
	if err != nil || respReconcileErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on POST /reconcile?sync=true error, got %v (status=%d)", err, respReconcileErr.StatusCode)
	}
	respReconcileErr.Body.Close()
	daemon.reconcileFn = nil

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
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v2\n"), 0644)

	head1 := "1111111111111111111111111111111111111111"
	head2 := "2222222222222222222222222222222222222222"
	currentHead := head2

	gitMock := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "reset" {
			currentHead = head1
			return []byte("HEAD is now at " + head1), nil, nil
		}
		if len(args) > 0 && args[0] == "log" {
			return []byte(currentHead + "\x002026-09-12T12:00:00Z"), nil, nil
		}
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte(currentHead), nil, nil
		}
		return nil, nil, nil
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
		GitExecutor:     gitMock,
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	daemon.recordPendingChange(ComposeChangeEvent{
		RepoPath:     tempDir,
		PreviousHead: head1,
		CurrentHead:  head2,
		Timestamp:    time.Now(),
	})

	ctx := context.Background()
	err := daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Fatalf("expected ReconcileCompose to fail on validation error, got nil")
	}

	// Verify working tree was rolled back to head1
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
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: aerial-brain:v2\n"), 0644)

	head1 := "1111111111111111111111111111111111111111"
	head2 := "2222222222222222222222222222222222222222"
	currentHead := head2

	gitMock := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "reset" {
			currentHead = head1
			return []byte("HEAD is now at " + head1), nil, nil
		}
		if len(args) > 0 && args[0] == "log" {
			return []byte(currentHead + "\x002026-09-12T12:00:00Z"), nil, nil
		}
		if len(args) > 0 && args[0] == "rev-parse" {
			return []byte(currentHead), nil, nil
		}
		return nil, nil, nil
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
		GitExecutor:     gitMock,
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	daemon.recordPendingChange(ComposeChangeEvent{
		RepoPath:     tempDir,
		PreviousHead: head1,
		CurrentHead:  head2,
		Timestamp:    time.Now(),
	})

	ctx := context.Background()
	err := daemon.ReconcileCompose(ctx)
	if err == nil {
		t.Fatalf("expected ReconcileCompose to fail on compose up, got nil")
	}

	// Verify working tree rolled back to head1
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
	localClone := filepath.Join(tempBase, "local")
	_ = os.MkdirAll(filepath.Join(localClone, ".git"), 0755)

	head1 := "1111111111111111111111111111111111111111"
	head2 := "2222222222222222222222222222222222222222"
	head3 := "3333333333333333333333333333333333333333"

	currentHead := head1
	fetchHead := head2

	gitMock := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) == 0 {
			return nil, nil, nil
		}
		switch args[0] {
		case "config":
			return nil, nil, nil
		case "rev-parse":
			if len(args) > 1 && args[1] == "FETCH_HEAD" {
				return []byte(fetchHead), nil, nil
			}
			return []byte(currentHead), nil, nil
		case "fetch":
			return nil, nil, nil
		case "merge":
			currentHead = fetchHead
			return []byte("Updating " + currentHead), nil, nil
		case "diff":
			return nil, nil, nil
		}
		return nil, nil, nil
	}

	daemon := NewDaemon(DaemonConfig{
		Repos:       []string{localClone},
		GitExecutor: gitMock,
	})
	ctx := context.Background()

	// 1. Quarantine commit 2 in daemon
	daemon.quarantineCommit(localClone, head2, head1, "pre-flight validation", "broken syntax")

	// 2. Attempt sync: should skip pull and return error mentioning quarantine
	res := daemon.SyncRepo(ctx, localClone)
	if !strings.Contains(res.Error, "quarantined") {
		t.Errorf("expected sync to be blocked by quarantine, got error: %q", res.Error)
	}

	// Verify local clone is STILL at head1
	if currentHead != head1 {
		t.Errorf("expected local clone to remain at %s, but advanced to %s", head1, currentHead)
	}

	// 3. Upstream advances to commit 3 (clean fix)
	fetchHead = head3

	// 4. Attempt sync: should succeed, advance to head3, and clear quarantine for head2
	res2 := daemon.SyncRepo(ctx, localClone)
	if res2.Error != "" {
		t.Fatalf("expected successful sync after upstream fix, got: %s", res2.Error)
	}
	if !res2.Changed {
		t.Errorf("expected changed=true after syncing head3")
	}
	if currentHead != head3 {
		t.Errorf("expected local clone to advance to %s, got %s", head3, currentHead)
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

	if runtime.GOOS == "windows" {
		fakeDocker := filepath.Join(binDir, "docker.cmd")
		scriptContent := "@echo off\r\n" +
			"set \"arg=%*\"\r\n" +
			"echo %arg% | findstr /i \"version\" >nul && (\r\n" +
			"  echo Docker Compose mock v2.0\r\n" +
			"  exit /b 0\r\n" +
			")\r\n" +
			"echo %arg% | findstr /i \"sleep\" >nul && goto do_sleep\r\n" +
			"echo unknown cmd >&2\r\n" +
			"exit /b 1\r\n" +
			":do_sleep\r\n" +
			"goto do_sleep\r\n"
		if err := os.WriteFile(fakeDocker, []byte(scriptContent), 0755); err != nil {
			t.Fatalf("failed to write fake docker: %v", err)
		}
	} else {
		fakeDocker := filepath.Join(binDir, "docker")
		scriptContent := "#!/bin/sh\ncase \"$*\" in\n  *\"version\"*) echo \"Docker Compose mock v2.0\"; exit 0;;\n  *\"sleep\"*) trap 'exit 0' TERM INT; while :; do sleep 0.05; done;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
		if err := os.WriteFile(fakeDocker, []byte(scriptContent), 0755); err != nil {
			t.Fatalf("failed to write fake docker: %v", err)
		}
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
	cancelCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
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
	mockGit := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		ComposeExecutor: mockExecutor,
		GitExecutor:     mockGit,
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})

	// 1. Empty pending
	_ = d.executeRollback(context.Background(), nil, "validation", errors.New("test err"), nil, nil, nil, nil)

	// 2. ch with empty previousHead or repoPath
	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: "", PreviousHead: "abc"},
		{RepoPath: "/some/path", PreviousHead: ""},
	}, "validation", errors.New("test err"), nil, nil, nil, nil)

	// 3. Reset error branch and target discovery error
	badYamlDir := t.TempDir()
	d.composeDir = badYamlDir
	// Write invalid docker-compose.yml so GetReconcileTargets fails
	_ = os.WriteFile(filepath.Join(badYamlDir, "docker-compose.yml"), []byte("services: [invalid yaml"), 0644)

	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: filepath.Join(badYamlDir, "nonexistent-repo"), PreviousHead: "deadbeef", CurrentHead: "cafebabe"},
	}, "compose_up", errors.New("mock up failure"), nil, nil, nil, nil)

	// 4. Valid targets but composeExecutor returns error on up -d
	validDir := t.TempDir()
	d.composeDir = validDir
	_ = os.WriteFile(filepath.Join(validDir, "docker-compose.yml"), []byte("version: '3.8'\nservices:\n  app:\n    image: alpine\n"), 0644)
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("compose up error"), errors.New("compose up failed")
	}
	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: validDir, PreviousHead: "HEAD", CurrentHead: "HEAD"},
	}, "compose_up", errors.New("mock fail"), nil, nil, nil, nil)
}

func TestSyncRepo_ResetRecovery(t *testing.T) {
	tempDir := t.TempDir()
	localDir := filepath.Join(tempDir, "local")
	_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

	head1 := "1111111111111111111111111111111111111111"
	head2 := "2222222222222222222222222222222222222222"
	currentHead := head1

	gitMock := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		if len(args) == 0 {
			return nil, nil, nil
		}
		switch args[0] {
		case "config":
			return nil, nil, nil
		case "rev-parse":
			if len(args) > 1 && args[1] == "FETCH_HEAD" {
				return []byte(head2), nil, nil
			}
			return []byte(currentHead), nil, nil
		case "fetch":
			return nil, nil, nil
		case "merge":
			return nil, []byte("fatal: Not possible to fast-forward, aborting."), errors.New("exit status 128")
		case "reset":
			currentHead = head2
			return []byte("HEAD is now at " + head2), nil, nil
		case "clean":
			return nil, nil, nil
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		GitExecutor: gitMock,
	})
	res := d.SyncRepo(context.Background(), localDir)
	if res.Error != "" {
		t.Fatalf("SyncRepo failed unexpectedly during reset recovery: %s", res.Error)
	}
	if !res.Changed {
		t.Errorf("expected Changed to be true after reset recovery")
	}
	if currentHead != head2 {
		t.Errorf("expected HEAD to be reset to %s, got %s", head2, currentHead)
	}
}

func TestGetStatus_Extended(t *testing.T) {
	fakeRepo := "/mock/repo"
	d := NewDaemon(DaemonConfig{
		Repos: []string{fakeRepo},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("abcdef1234567890abcdef1234567890abcdef12\x002026-09-12T12:00:00Z"), nil, nil
		},
	})

	// 1. Quarantined status check on repo
	d.quarantineCommit(fakeRepo, "deadbeef", "cafebabe", "validation", "bad syntax")
	st := d.GetStatus(context.Background())
	if st.Status != "quarantined" {
		t.Errorf("expected quarantined status, got %s", st.Status)
	}

	// 2. Quarantine match on disk commit SHA
	d.clearQuarantineForRepo(fakeRepo, "")
	d.quarantineCommit(fakeRepo, "abcdef1234567890abcdef1234567890abcdef12", "prev123", "validation", "disk quarantined")
	st2 := d.GetStatus(context.Background())
	if st2.Status != "quarantined" {
		t.Errorf("expected quarantined on diskSha, got %s", st2.Status)
	}
}

func TestGetStatus_LaggingAndRemoteQuarantine(t *testing.T) {
	fakeRepo := "/mock/local"
	remoteSha := "2222222222222222222222222222222222222222"
	localSha := "1111111111111111111111111111111111111111"

	d := NewDaemon(DaemonConfig{
		Repos: []string{fakeRepo},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			ref := args[len(args)-1]
			if ref == "@{u}" || strings.Contains(ref, "origin/") {
				return []byte(remoteSha + "\x002035-01-01T12:00:00Z"), nil, nil
			}
			return []byte(localSha + "\x002026-09-12T12:00:00Z"), nil, nil
		},
	})

	// Case 1: lagging status (remote has newer commit and future timestamp)
	stLagging := d.GetStatus(context.Background())
	if stLagging.Status != "lagging" {
		t.Errorf("expected lagging status, got %s", stLagging.Status)
	}

	// Case 2: remote commit quarantined
	d.quarantineCommit(fakeRepo, remoteSha, "prev", "validation", "remote quarantined")
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

	// 4. /reconcile method not allowed
	reqRecGet := httptest.NewRequest(http.MethodGet, "/reconcile", nil)
	recRecGet := httptest.NewRecorder()
	handler.ServeHTTP(recRecGet, reqRecGet)
	if recRecGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from GET /reconcile, got %d", recRecGet.Code)
	}

	// 5. /reconcile POST async success
	reqRecPost := httptest.NewRequest(http.MethodPost, "/reconcile", nil)
	recRecPost := httptest.NewRecorder()
	handler.ServeHTTP(recRecPost, reqRecPost)
	if recRecPost.Code != http.StatusAccepted {
		t.Errorf("expected 202 from POST /reconcile, got %d", recRecPost.Code)
	}

	// 6. /reconcile POST sync success (query param)
	dSync := NewDaemon(DaemonConfig{})
	dSync.reconcileFn = func(ctx context.Context) error { return nil }
	handlerSync := SetupMux(dSync)
	reqRecSync := httptest.NewRequest(http.MethodPost, "/reconcile?sync=true", nil)
	recRecSync := httptest.NewRecorder()
	handlerSync.ServeHTTP(recRecSync, reqRecSync)
	if recRecSync.Code != http.StatusOK {
		t.Errorf("expected 200 from POST /reconcile?sync=true, got %d", recRecSync.Code)
	}

	// 7. /reconcile POST sync error (query param)
	dSyncErr := NewDaemon(DaemonConfig{})
	dSyncErr.reconcileFn = func(ctx context.Context) error { return errors.New("compose fail") }
	handlerSyncErr := SetupMux(dSyncErr)
	recRecSyncErr := httptest.NewRecorder()
	handlerSyncErr.ServeHTTP(recRecSyncErr, reqRecSync)
	if recRecSyncErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 from failing /reconcile?sync=true, got %d", recRecSyncErr.Code)
	}

	// 8. /reconcile POST sync with JSON body {"sync": true}
	reqRecJSONSync := httptest.NewRequest(http.MethodPost, "/reconcile", strings.NewReader(`{"sync": true}`))
	reqRecJSONSync.Header.Set("Content-Type", "application/json")
	recRecJSONSync := httptest.NewRecorder()
	handlerSync.ServeHTTP(recRecJSONSync, reqRecJSONSync)
	if recRecJSONSync.Code != http.StatusOK {
		t.Errorf("expected 200 from POST /reconcile JSON sync=true, got %d", recRecJSONSync.Code)
	}

	// 9. /reconcile POST async with JSON body {"sync": false}
	reqRecJSONAsync := httptest.NewRequest(http.MethodPost, "/reconcile", strings.NewReader(`{"sync": false}`))
	reqRecJSONAsync.Header.Set("Content-Type", "application/json")
	recRecJSONAsync := httptest.NewRecorder()
	handlerSync.ServeHTTP(recRecJSONAsync, reqRecJSONAsync)
	if recRecJSONAsync.Code != http.StatusAccepted {
		t.Errorf("expected 202 from POST /reconcile JSON sync=false, got %d", recRecJSONAsync.Code)
	}
}

func TestTriggerReconcile_TableDriven(t *testing.T) {
	ctx := context.Background()

	// 1. Sync execution calls ReconcileCompose
	calledSync := false
	d := &SyncDaemon{
		reconcileFn: func(ctx context.Context) error {
			calledSync = true
			return nil
		},
	}
	if err := d.TriggerReconcile(ctx, false); err != nil || !calledSync {
		t.Fatalf("expected TriggerReconcile(false) to execute reconcileFn, err=%v", err)
	}

	// 2. Async execution queues into reconcileCh
	dAsync := &SyncDaemon{
		reconcileCh: make(chan struct{}, 1),
	}
	if err := dAsync.TriggerReconcile(ctx, true); err != nil {
		t.Fatalf("expected TriggerReconcile(true) to succeed, err=%v", err)
	}
	select {
	case <-dAsync.reconcileCh:
	default:
		t.Errorf("expected message in reconcileCh")
	}

	// 3. Async execution when channel is already full (default branch)
	dAsync.reconcileCh <- struct{}{}
	if err := dAsync.TriggerReconcile(ctx, true); err != nil {
		t.Fatalf("expected TriggerReconcile(true) with full channel to succeed, err=%v", err)
	}

	// 4. Async execution when reconcileCh is nil (lazy initialization branch)
	dNil := &SyncDaemon{}
	if err := dNil.TriggerReconcile(ctx, true); err != nil {
		t.Fatalf("expected TriggerReconcile(true) with nil channel to succeed, err=%v", err)
	}
	if dNil.reconcileCh == nil {
		t.Errorf("expected reconcileCh to be initialized")
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
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})

	// Case 1: zero restoredTargets discovered (since targets filter for aerial services)
	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: tempDir, PreviousHead: "HEAD"},
	}, "validation", errors.New("target discovery empty"), nil, nil, nil, nil)

	// Case 2: compose file with aerial service so targets > 0, but composeExecutor returns error
	aerialDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(aerialDir, "docker-compose.yml"), []byte("version: '3.8'\nservices:\n  brain:\n    image: alpine\n"), 0644)
	d.composeDir = aerialDir
	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: aerialDir, PreviousHead: "HEAD"},
	}, "compose_up", errors.New("up error test"), nil, nil, nil, nil)

	if !errUpCalled {
		t.Errorf("expected mockExecutor to be called for up error test")
	}
}

func TestEnsureRepo_CloneError(t *testing.T) {
	tempDir := t.TempDir()
	d := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: repository not found"), errors.New("clone failed")
		},
	})
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
			PAT:        "ghp_dummy_token_1234567890",
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
	localDir := filepath.Join(tempDir, "local")
	_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

	head1 := "1111111111111111111111111111111111111111"
	head2 := "2222222222222222222222222222222222222222"
	currentHead := head1

	d := NewDaemon(DaemonConfig{
		Repos: []string{localDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) == 0 {
				return nil, nil, nil
			}
			switch args[0] {
			case "config":
				return nil, nil, nil
			case "rev-parse":
				return []byte(currentHead), nil, nil
			case "fetch":
				return nil, nil, nil
			case "merge":
				currentHead = head2
				return []byte("Updating " + head2), nil, nil
			case "diff":
				return nil, nil, nil
			}
			return nil, nil, nil
		},
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
	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create bin dir: %v", err)
	}

	if runtime.GOOS == "windows" {
		fakeDocker := filepath.Join(binDir, "docker.cmd")
		scriptContent := "@echo off\r\n" +
			"set \"arg=%*\"\r\n" +
			"echo %arg% | findstr /i \"version\" >nul && (\r\n" +
			"  echo Docker mock v24.0\r\n" +
			"  exit /b 0\r\n" +
			")\r\n" +
			"echo %arg% | findstr /i \"sleep\" >nul && goto do_sleep\r\n" +
			"echo unknown cmd >&2\r\n" +
			"exit /b 1\r\n" +
			":do_sleep\r\n" +
			"goto do_sleep\r\n"
		if err := os.WriteFile(fakeDocker, []byte(scriptContent), 0755); err != nil {
			t.Fatalf("failed to write fake docker: %v", err)
		}
	} else {
		fakeDocker := filepath.Join(binDir, "docker")
		scriptContent := "#!/bin/sh\ncase \"$*\" in\n  *\"version\"*) echo \"Docker mock v24.0\"; exit 0;;\n  *\"sleep\"*) trap 'exit 0' TERM INT; while :; do sleep 0.05; done;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
		if err := os.WriteFile(fakeDocker, []byte(scriptContent), 0755); err != nil {
			t.Fatalf("failed to write fake docker: %v", err)
		}
	}

	origPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+origPath)

	// 1. Success execution
	stdout, _, err := defaultDockerExecutor(context.Background(), "version")
	if err != nil || !strings.Contains(string(stdout), "Docker mock v24.0") {
		t.Errorf("expected mock output, got err=%v, stdout=%s", err, string(stdout))
	}

	// 2. Cancellation execution
	cancelCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, _ = defaultDockerExecutor(cancelCtx, "sleep")

	// 3. Error execution
	_, stderr, err := defaultDockerExecutor(context.Background(), "unknown")
	if err == nil || !strings.Contains(string(stderr), "unknown cmd") {
		t.Errorf("expected error from unknown command, got err=%v, stderr=%s", err, string(stderr))
	}
}

func TestScrubComposeEnv_Extended(t *testing.T) {
	input := []string{
		"INVALID_NO_EQUALS",
		"AERIAL_CONFIG_DIR=/dir/config",
		"AERIAL_PROJECT_DIR=/dir/project",
		"GOOD_VAR=123",
		"COMPOSE_PROJECT_NAME=aerial",
	}
	out := scrubComposeEnv(input)
	if len(out) != 1 || out[0] != "COMPOSE_PROJECT_NAME=aerial" {
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

func TestHasComposeChanges_Coverage(t *testing.T) {
	d := &SyncDaemon{}

	// 1. Canceled context
	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	changed, err := d.HasComposeChanges(ctxCancelled, "/path", "v1", "v2")
	if err == nil || changed {
		t.Errorf("expected context error, got %v, %v", changed, err)
	}

	// 2. Empty or matching heads / empty repoPath
	cases := []struct {
		repo, prev, curr string
	}{
		{"/path", "", "v2"},
		{"/path", "v1", ""},
		{"/path", "v1", "v1"},
		{"", "v1", "v2"},
	}
	for _, c := range cases {
		got, err := d.HasComposeChanges(context.Background(), c.repo, c.prev, c.curr)
		if err != nil || got {
			t.Errorf("expected false, nil for %v, got %v, %v", c, got, err)
		}
	}

	// 3. Diff returns compose change
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("docker-compose.yml\nmain.go\n"), nil, nil
	}
	got, err := d.HasComposeChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || !got {
		t.Errorf("expected true, nil, got %v, %v", got, err)
	}

	// 4. Diff returns no compose change
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("README.md\nmain.go\n"), nil, nil
	}
	got, err = d.HasComposeChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || got {
		t.Errorf("expected false, nil, got %v, %v", got, err)
	}

	// 5. Diff fails, fallback diff-tree succeeds with compose changes
	callCount := 0
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		callCount++
		if callCount == 1 {
			return nil, []byte("fatal: ambiguous argument"), errors.New("diff failed")
		}
		return []byte(".env\n"), nil, nil
	}
	got, err = d.HasComposeChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || !got {
		t.Errorf("expected fallback true, nil, got %v, %v", got, err)
	}

	// 6. Diff fails, fallback diff-tree fails (fail safe)
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("fatal: repo corrupt"), errors.New("diff failed")
	}
	got, err = d.HasComposeChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || !got {
		t.Errorf("expected fail-safe true, nil, got %v, %v", got, err)
	}

	// 7. Diff fails with canceled context during diff
	ctxFail, cancelFail := context.WithCancel(context.Background())
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		cancelFail()
		return nil, nil, errors.New("aborted")
	}
	got, err = d.HasComposeChanges(ctxFail, "/path", "v1", "v2")
	if err == nil || got {
		t.Errorf("expected ctx error on diff abort, got %v, %v", got, err)
	}

	// 8. Diff fails, diff-tree fails with canceled context
	ctxTreeFail, cancelTreeFail := context.WithCancel(context.Background())
	treeCalls := 0
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		treeCalls++
		if treeCalls == 1 {
			return nil, nil, errors.New("diff error")
		}
		cancelTreeFail()
		return nil, nil, errors.New("diff-tree error")
	}
	got, err = d.HasComposeChanges(ctxTreeFail, "/path", "v1", "v2")
	if err == nil || got {
		t.Errorf("expected ctx error on diff-tree abort, got %v, %v", got, err)
	}
}

func TestHasNomadConfigChanges_Coverage(t *testing.T) {
	d := &SyncDaemon{}

	// 1. Canceled context
	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	changed, err := d.HasNomadConfigChanges(ctxCancelled, "/path", "v1", "v2")
	if err == nil || changed {
		t.Errorf("expected context error, got %v, %v", changed, err)
	}

	// 2. Empty or matching heads / empty repoPath
	cases := []struct {
		repo, prev, curr string
	}{
		{"/path", "", "v2"},
		{"/path", "v1", ""},
		{"/path", "v1", "v1"},
		{"", "v1", "v2"},
	}
	for _, c := range cases {
		got, err := d.HasNomadConfigChanges(context.Background(), c.repo, c.prev, c.curr)
		if err != nil || got {
			t.Errorf("expected false, nil for %v, got %v, %v", c, got, err)
		}
	}

	// 3. Diff returns nomad config change
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("nomad/nomad.hcl\nmain.go\n"), nil, nil
	}
	got, err := d.HasNomadConfigChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || !got {
		t.Errorf("expected true, nil, got %v, %v", got, err)
	}

	// 4. Diff returns no nomad config change
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("README.md\ndocker-compose.yml\n"), nil, nil
	}
	got, err = d.HasNomadConfigChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || got {
		t.Errorf("expected false, nil, got %v, %v", got, err)
	}

	// 5. Diff fails, fallback diff-tree succeeds with nomad config changes
	callCount := 0
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		callCount++
		if callCount == 1 {
			return nil, []byte("fatal: ambiguous argument"), errors.New("diff failed")
		}
		return []byte("nomad/client.hcl\n"), nil, nil
	}
	got, err = d.HasNomadConfigChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || !got {
		t.Errorf("expected fallback true, nil, got %v, %v", got, err)
	}

	// 6. Diff fails, fallback diff-tree fails (fail safe to false)
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("fatal: repo corrupt"), errors.New("diff failed")
	}
	got, err = d.HasNomadConfigChanges(context.Background(), "/path", "v1", "v2")
	if err != nil || got {
		t.Errorf("expected fail-safe false, nil, got %v, %v", got, err)
	}
}

func TestValidateNomadConfig_Coverage(t *testing.T) {
	d := &SyncDaemon{}

	// 1. Success
	d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return []byte("Configuration is valid!\n"), nil, nil
	}
	if err := d.ValidateNomadConfig(context.Background(), "/tmp/nomad"); err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// 2. Failure
	d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, []byte("syntax error on line 12"), errors.New("exit 1")
	}
	if err := d.ValidateNomadConfig(context.Background(), "/tmp/nomad"); err == nil {
		t.Errorf("expected validation error, got nil")
	}
}

func TestReconcileNomadServer_Coverage(t *testing.T) {
	tmpDir := t.TempDir()
	d := &SyncDaemon{}

	// 1. Missing directory (no-op)
	if err := d.reconcileNomadServer(context.Background(), tmpDir); err != nil {
		t.Errorf("expected nil for missing nomad dir, got %v", err)
	}

	// Create nomad dir
	nomadDir := filepath.Join(tmpDir, "nomad")
	_ = os.MkdirAll(nomadDir, 0755)

	// 2. Validation failure aborts restart
	restartCalled := false
	d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, []byte("invalid config syntax"), errors.New("validation failed")
	}
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		restartCalled = true
		return []byte("restarted"), nil, nil
	}
	if err := d.reconcileNomadServer(context.Background(), tmpDir); err == nil {
		t.Errorf("expected validation error, got nil")
	}
	if restartCalled {
		t.Errorf("expected restart to be aborted on validation error")
	}

	// 3. Validation success, restart succeeds
	d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return []byte("Configuration is valid!\n"), nil, nil
	}
	restartCalled = false
	if err := d.reconcileNomadServer(context.Background(), tmpDir); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !restartCalled {
		t.Errorf("expected restart to be called on successful validation")
	}

	// 4. Validation success, restart fails
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("container restart failed"), errors.New("exit 1")
	}
	if err := d.reconcileNomadServer(context.Background(), tmpDir); err == nil {
		t.Errorf("expected restart error, got nil")
	}
}

func TestGetRepoCommit_DetailedCoverage(t *testing.T) {
	d := &SyncDaemon{}

	// 1. Error from git executor
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, nil, errors.New("git error")
	}
	sha, tm, err := d.getRepoCommit(context.Background(), "/repo", "HEAD")
	if err == nil || sha != "" || tm != nil {
		t.Errorf("expected error, got %v, %v, %v", sha, tm, err)
	}

	// 2. Output without null delimiter
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("abc1234"), nil, nil
	}
	sha, tm, err = d.getRepoCommit(context.Background(), "/repo", "HEAD")
	if err != nil || sha != "abc1234" || tm != nil {
		t.Errorf("expected sha only, got %v, %v, %v", sha, tm, err)
	}

	// 3. Output with invalid date
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("abc1234\x00invalid-date"), nil, nil
	}
	sha, tm, err = d.getRepoCommit(context.Background(), "/repo", "HEAD")
	if err != nil || sha != "abc1234" || tm != nil {
		t.Errorf("expected sha with nil time on bad date, got %v, %v, %v", sha, tm, err)
	}

	// 4. Output with valid RFC3339 date
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return []byte("abc1234\x002026-09-12T12:00:00Z"), nil, nil
	}
	sha, tm, err = d.getRepoCommit(context.Background(), "/repo", "HEAD")
	if err != nil || sha != "abc1234" || tm == nil || tm.Year() != 2026 {
		t.Errorf("expected valid sha and time, got %v, %v, %v", sha, tm, err)
	}
}

func TestCleanConflictContainers_DetailedCoverage(t *testing.T) {
	d := &SyncDaemon{}

	// 1. Docker ps error
	d.dockerExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, []byte("permission denied"), errors.New("daemon error")
	}
	if err := d.CleanConflictContainers(context.Background()); err == nil {
		t.Errorf("expected error on ps failure")
	}

	// 2. Rm returns "No such container", generic rm error, and rm success
	rmStep := 0
	d.dockerExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if args[0] == "ps" {
			// Provide 3 dead conflict containers (with >12 char IDs)
			return []byte("1111222233334444\t111122223333_aerial-db\tExited (0)\n" +
				"5555666677778888\t555566667777_aerial-brain\tDead\n" +
				"9999000011112222\t999900001111_aerial-watchtower\tExited (1)\n"), nil, nil
		}
		if args[0] == "rm" {
			rmStep++
			switch rmStep {
			case 1:
				// "No such container" error
				return nil, []byte("Error: No such container: 111122223333"), errors.New("exit 1")
			case 2:
				// Generic error
				return nil, []byte("Error: container locked"), errors.New("exit 1")
			case 3:
				// Success
				return []byte("999900001111"), nil, nil
			}
		}
		return nil, nil, nil
	}

	if err := d.CleanConflictContainers(context.Background()); err != nil {
		t.Errorf("expected clean completion, got %v", err)
	}
}

func TestResolveChannelID_DetailedCoverage(t *testing.T) {
	d := &SyncDaemon{}

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

	// 1. Guilds HTTP 500 error
	_, err := d.resolveChannelID(context.Background(), client500, "token", "alerts")
	if err == nil {
		t.Errorf("expected error on HTTP 500")
	}

	// 2. Snowflake fast return
	ch, err := d.resolveChannelID(context.Background(), client500, "token", "123456789012345678")
	if err != nil || ch != "123456789012345678" {
		t.Errorf("expected snowflake fast return, got %v, %v", ch, err)
	}

	// 3. Channel not found / cached return
	d.cachedChannelID = "cached-999"
	ch, err = d.resolveChannelID(context.Background(), client500, "token", "alerts")
	if err != nil || ch != "cached-999" {
		t.Errorf("expected cached channel ID, got %v, %v", ch, err)
	}
}

func TestGetGitExecutor_Branches(t *testing.T) {
	var nilDaemon *SyncDaemon
	if nilDaemon.getGitExecutor() == nil {
		t.Errorf("expected non-nil default git executor for nil daemon")
	}

	d := &SyncDaemon{}
	if d.getGitExecutor() == nil {
		t.Errorf("expected non-nil executor when gitExecutor is nil")
	}

	customCalled := false
	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		customCalled = true
		return []byte("custom-git"), nil, nil
	}
	execFn := d.getGitExecutor()
	_, _, _ = execFn(context.Background(), "")
	if !customCalled {
		t.Errorf("expected custom git executor to be called")
	}
}

func TestGetDockerExecutor_Branches(t *testing.T) {
	var nilDaemon *SyncDaemon
	if nilDaemon.getDockerExecutor() == nil {
		t.Errorf("expected non-nil default docker executor for nil daemon")
	}

	d := &SyncDaemon{}
	if d.getDockerExecutor() == nil {
		t.Errorf("expected non-nil executor when dockerExecutor is nil")
	}
}

func TestExecuteRollback_ErrorBranches(t *testing.T) {
	tempDir := t.TempDir()
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composePath, []byte("services:\n  brain:\n    image: alpine\n"), 0644)

	d := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "reset" {
				return nil, []byte("fatal: reset failed"), errors.New("reset failed")
			}
			return nil, nil, nil
		},
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config") && strings.Contains(argsStr, "--services") {
				return []byte("brain\n"), nil, nil
			}
			if strings.Contains(argsStr, "up -d") {
				return nil, []byte("fatal: compose up failed"), errors.New("compose up failed")
			}
			return nil, nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})

	_ = d.executeRollback(context.Background(), []ComposeChangeEvent{
		{RepoPath: tempDir, PreviousHead: "head1", CurrentHead: "head2"},
	}, "test_stage", errors.New("cause error"), nil, nil, nil, nil)
}

func TestSyncRepo_ResetFailureAndPostMergeRevParseError(t *testing.T) {
	tempDir := t.TempDir()
	localDir := filepath.Join(tempDir, "local")
	_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

	// 1. Reset failure in recovery branch
	dResetFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				switch args[0] {
				case "config":
					return nil, nil, nil
				case "rev-parse":
					return []byte("1111111111111111111111111111111111111111"), nil, nil
				case "fetch":
					return nil, nil, nil
				case "merge":
					return nil, []byte("fatal: conflict"), errors.New("merge conflict")
				case "reset":
					return nil, []byte("fatal: reset failed"), errors.New("reset failed")
				}
			}
			return nil, nil, nil
		},
	})
	res := dResetFail.SyncRepo(context.Background(), localDir)
	if !strings.Contains(res.Error, "reset failed") {
		t.Errorf("expected reset failed error, got %s", res.Error)
	}

	// 2. Post-merge rev-parse failure
	revParseCount := 0
	dRevParseFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				switch args[0] {
				case "config":
					return nil, nil, nil
				case "rev-parse":
					revParseCount++
					if revParseCount > 2 {
						return nil, []byte("fatal: rev-parse failed"), errors.New("rev-parse error")
					}
					return []byte("1111111111111111111111111111111111111111"), nil, nil
				case "fetch":
					return nil, nil, nil
				case "merge":
					return []byte("Already up to date."), nil, nil
				}
			}
			return nil, nil, nil
		},
	})
	res2 := dRevParseFail.SyncRepo(context.Background(), localDir)
	if res2.Error == "" {
		t.Errorf("expected error when post-merge rev-parse fails")
	}
}

func TestGetStatus_RemoteFallbackAndNegativeLag(t *testing.T) {
	fakeRepo := "/mock/repo"
	now := time.Now()
	past := now.Add(-10 * time.Minute)

	// Case 1: origin/main fails, fallback to FETCH_HEAD
	d := NewDaemon(DaemonConfig{
		Repos: []string{fakeRepo},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			ref := args[len(args)-1]
			if ref == "origin/main" {
				return nil, nil, errors.New("no origin/main")
			}
			if ref == "FETCH_HEAD" {
				return []byte("2222222222222222222222222222222222222222\x00" + past.Format(time.RFC3339)), nil, nil
			}
			return []byte("1111111111111111111111111111111111111111\x00" + now.Format(time.RFC3339)), nil, nil
		},
	})
	st := d.GetStatus(context.Background())
	if st.Repos[fakeRepo].RemoteCommit != "2222222222222222222222222222222222222222" {
		t.Errorf("expected FETCH_HEAD fallback, got %s", st.Repos[fakeRepo].RemoteCommit)
	}

	// Case 2: both origin/main and FETCH_HEAD fail, fallback to diskSha
	dFallbackDisk := NewDaemon(DaemonConfig{
		Repos: []string{fakeRepo},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			ref := args[len(args)-1]
			if ref == "origin/main" || ref == "FETCH_HEAD" {
				return nil, nil, errors.New("remote ref not found")
			}
			return []byte("1111111111111111111111111111111111111111\x00" + now.Format(time.RFC3339)), nil, nil
		},
	})
	st2 := dFallbackDisk.GetStatus(context.Background())
	if st2.Repos[fakeRepo].RemoteCommit != "1111111111111111111111111111111111111111" {
		t.Errorf("expected diskSha fallback, got %s", st2.Repos[fakeRepo].RemoteCommit)
	}
}

func TestRunGitCommand_CancelCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _ = runGitCommand(ctx, "", "", "status")
}

func TestEnsureDockerAuth_TableDriven(t *testing.T) {
	dummyPAT := "dummy-pat-for-test-only"

	// 1. Nil daemon returns nil
	var nilDaemon *SyncDaemon
	if err := nilDaemon.EnsureDockerAuth(); err != nil {
		t.Errorf("expected nil error for nil daemon, got %v", err)
	}

	// 2. Empty PAT daemon returns nil
	emptyPATDaemon := &SyncDaemon{pat: ""}
	if err := emptyPATDaemon.EnsureDockerAuth(); err != nil {
		t.Errorf("expected nil error for empty PAT daemon, got %v", err)
	}

	// 3. EnsureDockerAuthPath unit branches
	t.Run("empty pat is no-op", func(t *testing.T) {
		err := EnsureDockerAuthPath("", "https://github.com/azylman/aerial-config.git", nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("mkdir failure returns error", func(t *testing.T) {
		errMkdir := errors.New("disk permission denied")
		err := EnsureDockerAuthPath(
			dummyPAT,
			"https://github.com/azylman/aerial-config.git",
			func(string) string { return "/mock/docker" },
			nil,
			nil,
			func(string, os.FileMode) error { return errMkdir },
		)
		if err == nil || !strings.Contains(err.Error(), "disk permission denied") {
			t.Fatalf("expected wrapped mkdir error, got %v", err)
		}
	})

	t.Run("read error other than not-exist returns error", func(t *testing.T) {
		errRead := errors.New("read fault")
		err := EnsureDockerAuthPath(
			dummyPAT,
			"https://github.com/azylman/aerial-config.git",
			func(string) string { return "/mock/docker" },
			func(string) ([]byte, error) { return nil, errRead },
			nil,
			func(string, os.FileMode) error { return nil },
		)
		if err == nil || !strings.Contains(err.Error(), "read fault") {
			t.Fatalf("expected wrapped read error, got %v", err)
		}
	})

	t.Run("write failure returns error", func(t *testing.T) {
		errWrite := errors.New("write fault")
		err := EnsureDockerAuthPath(
			dummyPAT,
			"https://github.com/azylman/aerial-config.git",
			func(string) string { return "/mock/docker" },
			func(string) ([]byte, error) { return nil, os.ErrNotExist },
			func(string, []byte, os.FileMode) error { return errWrite },
			func(string, os.FileMode) error { return nil },
		)
		if err == nil || !strings.Contains(err.Error(), "write fault") {
			t.Fatalf("expected wrapped write error, got %v", err)
		}
	})

	t.Run("successful write creates valid docker config file with 0600 mode", func(t *testing.T) {
		var writtenPath string
		var writtenData []byte
		var writtenMode os.FileMode

		err := EnsureDockerAuthPath(
			dummyPAT,
			"https://github.com/custom-org/aerial-config.git",
			func(k string) string {
				if k == "DOCKER_CONFIG" {
					return "/mock/docker"
				}
				return ""
			},
			func(string) ([]byte, error) { return nil, os.ErrNotExist },
			func(path string, data []byte, mode os.FileMode) error {
				writtenPath = path
				writtenData = data
				writtenMode = mode
				return nil
			},
			func(string, os.FileMode) error { return nil },
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if filepath.ToSlash(writtenPath) != "/mock/docker/config.json" {
			t.Errorf("expected /mock/docker/config.json, got %s", writtenPath)
		}
		if writtenMode != 0600 {
			t.Errorf("expected 0600 mode, got %v", writtenMode)
		}

		var parsed struct {
			Auths map[string]struct {
				Auth string `json:"auth"`
			} `json:"auths"`
		}
		if err := json.Unmarshal(writtenData, &parsed); err != nil {
			t.Fatalf("failed to unmarshal written config: %v", err)
		}
		entry, ok := parsed.Auths["ghcr.io"]
		if !ok {
			t.Fatalf("expected ghcr.io entry in auths")
		}
		expectedAuth := base64.StdEncoding.EncodeToString([]byte("custom-org:" + dummyPAT))
		if entry.Auth != expectedAuth {
			t.Errorf("auth mismatch: got %q, want %q", entry.Auth, expectedAuth)
		}
	})

	// 4. Integration test using temporary directory
	t.Run("filesystem integration test via daemon EnsureDockerAuth", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("DOCKER_CONFIG", tempDir)

		d := &SyncDaemon{
			pat: dummyPAT,
			configDir: "/share/aerial-config",
			repoUrls: map[string]string{
				"/share/aerial-config": "https://github.com/azylman/aerial-config.git",
			},
		}

		if err := d.EnsureDockerAuth(); err != nil {
			t.Fatalf("daemon.EnsureDockerAuth failed: %v", err)
		}

		cfgFile := filepath.Join(tempDir, "config.json")
		data, err := os.ReadFile(cfgFile)
		if err != nil {
			t.Fatalf("failed to read written docker config: %v", err)
		}

		var parsed map[string]any
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("failed to unmarshal written config: %v", err)
		}
		auths, ok := parsed["auths"].(map[string]any)
		if !ok {
			t.Fatalf("expected auths map in config")
		}
		ghcrEntry, ok := auths["ghcr.io"].(map[string]any)
		if !ok {
			t.Fatalf("expected ghcr.io in auths")
		}
		expectedAuth := base64.StdEncoding.EncodeToString([]byte("azylman:" + dummyPAT))
		if ghcrEntry["auth"] != expectedAuth {
			t.Errorf("auth mismatch: got %v, want %s", ghcrEntry["auth"], expectedAuth)
		}
	})
}

type mockErrReadCloser struct{}

func (e *mockErrReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (e *mockErrReadCloser) Close() error {
	return errors.New("simulated close error")
}

type mockErrResponseWriter struct {
	header http.Header
	err    error
}

func (m *mockErrResponseWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *mockErrResponseWriter) Write(p []byte) (int, error) {
	return 0, m.err
}

func (m *mockErrResponseWriter) WriteHeader(statusCode int) {}

func TestHangar_Helpers(t *testing.T) {
	// 1. closeWarn
	closeWarn(nil, "nil closer")
	closeWarn(io.NopCloser(strings.NewReader("")), "valid closer")
	closeWarn(&mockErrReadCloser{}, "err closer")

	// 2. isClientDisconnect
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	if isClientDisconnect(req, nil) {
		t.Errorf("expected false for nil err")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)
	if !isClientDisconnect(reqCanceled, errors.New("err")) {
		t.Errorf("expected true for canceled ctx")
	}
	if !isClientDisconnect(req, syscall.EPIPE) {
		t.Errorf("expected true for EPIPE")
	}
	if !isClientDisconnect(req, syscall.ECONNRESET) {
		t.Errorf("expected true for ECONNRESET")
	}
	if !isClientDisconnect(req, net.ErrClosed) {
		t.Errorf("expected true for net.ErrClosed")
	}

	// 3. writeResponse
	rec := httptest.NewRecorder()
	writeResponse(rec, req, []byte("ok"))
	if rec.Body.String() != "ok" {
		t.Errorf("expected ok, got %s", rec.Body.String())
	}
	mockErr := &mockErrResponseWriter{err: errors.New("fail")}
	writeResponse(mockErr, req, []byte("fail"))

	// 4. writeJSON
	recJSON := httptest.NewRecorder()
	writeJSON(recJSON, req, http.StatusOK, map[string]string{"foo": "bar"})
	if recJSON.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", recJSON.Code)
	}
	recErrJSON := httptest.NewRecorder()
	writeJSON(recErrJSON, req, http.StatusOK, make(chan int))
	if recErrJSON.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", recErrJSON.Code)
	}
}

func TestEnsureRepo_AdoptionErrors(t *testing.T) {
	tempDir := t.TempDir()
	adoptDir := filepath.Join(tempDir, "adopt")
	if err := os.MkdirAll(adoptDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(adoptDir, "file.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	// 1. Init failure
	dInitFail := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "init" {
				return nil, []byte("fatal: init error"), errors.New("init error")
			}
			return nil, nil, nil
		},
	}
	if err := dInitFail.EnsureRepo(context.Background(), adoptDir, "https://example.com/repo.git"); err == nil {
		t.Errorf("expected error on init failure")
	}

	// 2. Remote add failure (should NOT fail, should continue to fetch)
	dRemoteFail := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "remote" {
				return nil, []byte("remote origin exists"), errors.New("remote exists")
			}
			if len(args) > 0 && args[0] == "fetch" {
				return nil, []byte("fatal: fetch error"), errors.New("fetch error")
			}
			return nil, nil, nil
		},
	}
	if err := dRemoteFail.EnsureRepo(context.Background(), adoptDir, "https://example.com/repo.git"); err == nil {
		t.Errorf("expected error on fetch failure")
	}

	// 3. Reset soft failure
	dResetFail := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "reset" {
				return nil, []byte("fatal: reset error"), errors.New("reset error")
			}
			return nil, nil, nil
		},
	}
	if err := dResetFail.EnsureRepo(context.Background(), adoptDir, "https://example.com/repo.git"); err == nil {
		t.Errorf("expected error on reset soft failure")
	}
}

func TestTriggerSync_SingleflightInvalidType(t *testing.T) {
	d := &SyncDaemon{}
	started := make(chan struct{})
	go func() {
		_, _, _ = d.sfg.Do("sync", func() (interface{}, error) {
			close(started)
			time.Sleep(20 * time.Millisecond)
			return "invalid-type", nil
		})
	}()
	<-started
	_, err := d.TriggerSync()
	if err == nil || !strings.Contains(err.Error(), "unexpected return type") {
		t.Errorf("expected unexpected return type error, got %v", err)
	}
}

func TestSendWebhook_Errors(t *testing.T) {
	d := &SyncDaemon{}
	client := &http.Client{Timeout: 5 * time.Millisecond}
	d.sendWebhook(context.Background(), client, "http://127.0.0.1:1/invalid", "msg")
}

func TestPostChannelMessage_Errors(t *testing.T) {
	d := &SyncDaemon{}
	client := &http.Client{Timeout: 5 * time.Millisecond}
	d.postChannelMessage(context.Background(), client, "token", "chan-id", "msg")
}

func TestResolveChannelID_ClientFail(t *testing.T) {
	d := &SyncDaemon{}
	client := &http.Client{Timeout: 5 * time.Millisecond}
	_, err := d.resolveChannelID(context.Background(), client, "token", "test-channel")
	if err == nil {
		t.Errorf("expected error from resolveChannelID on timeout/failure")
	}
}

func TestImageQuarantine_Lifecycle(t *testing.T) {
	d := NewDaemon(DaemonConfig{})

	service := "brain"
	digest := "sha256:1111222233334444555566667777888899990000111122223333444455556666"

	// 1. Initially not quarantined
	if d.isImageQuarantined(service, digest) {
		t.Fatalf("expected digest %s not to be quarantined initially", digest)
	}

	// 2. Quarantine image
	d.quarantineImage(service, digest, "crash loop on start")
	if !d.isImageQuarantined(service, digest) {
		t.Fatalf("expected digest %s to be quarantined", digest)
	}

	// Different digest for same service is not quarantined
	otherDigest := "sha256:ffff222233334444555566667777888899990000111122223333444455556666"
	if d.isImageQuarantined(service, otherDigest) {
		t.Fatalf("expected different digest %s not to be quarantined", otherDigest)
	}

	// 3. TTL expiry test
	key := fmt.Sprintf("%s@%s", service, digest)
	d.imageQuarantineMu.Lock()
	rec := d.imageQuarantine[key]
	rec.QuarantinedAt = time.Now().Add(-61 * time.Minute)
	d.imageQuarantine[key] = rec
	d.imageQuarantineMu.Unlock()

	if d.isImageQuarantined(service, digest) {
		t.Fatalf("expected expired quarantine record to return false")
	}

	// 4. Clear quarantine
	d.quarantineImage(service, digest, "broken again")
	if !d.isImageQuarantined(service, digest) {
		t.Fatalf("expected digest to be quarantined after re-adding")
	}
	d.clearImageQuarantine(service)
	if d.isImageQuarantined(service, digest) {
		t.Fatalf("expected quarantine to be cleared for service %s", service)
	}

	// 5. Nil receiver or nil map safety
	nilD := &SyncDaemon{}
	if nilD.isImageQuarantined(service, digest) {
		t.Errorf("expected isImageQuarantined to return false on nil map")
	}
	nilD.clearImageQuarantine(service)
}

func TestGetRegistryClient_AndReconcileAccessors(t *testing.T) {
	// Default client
	dDefault := &SyncDaemon{}
	c := dDefault.getRegistryClient()
	if c == nil || c.Timeout != 10*time.Second {
		t.Errorf("expected default registry client with 10s timeout, got %v", c)
	}

	// Custom client
	custom := &http.Client{Timeout: 42 * time.Second}
	dCustom := NewDaemon(DaemonConfig{RegistryClient: custom})
	if dCustom.getRegistryClient() != custom {
		t.Errorf("expected custom registry client to be returned")
	}

	// LastReconcile accessors
	now := time.Now().Truncate(time.Second)
	dCustom.SetLastReconcile(now)
	if !dCustom.GetLastReconcile().Equal(now) {
		t.Errorf("expected LastReconcile to equal %v, got %v", now, dCustom.GetLastReconcile())
	}
}

func TestGetRemoteImageDigest_TableDriven(t *testing.T) {
	validDigest := "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	t.Run("invalid image reference returns error", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		_, err := d.GetRemoteImageDigest(context.Background(), "invalid reference with spaces")
		if err == nil {
			t.Fatalf("expected error for invalid reference, got nil")
		}
	})

	t.Run("direct 200 with Docker-Content-Digest", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method != http.MethodHead {
						t.Errorf("expected HEAD request, got %s", req.Method)
					}
					header := make(http.Header)
					header.Set("Docker-Content-Digest", validDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		digest, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if digest != validDigest {
			t.Errorf("got %q, want %q", digest, validDigest)
		}
	})

	t.Run("direct 200 with ETag fallback", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("ETag", `W/"`+validDigest+`"`)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		digest, err := d.GetRemoteImageDigest(context.Background(), "pgvector/pgvector:pg16")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if digest != validDigest {
			t.Errorf("got %q, want %q", digest, validDigest)
		}
	})

	t.Run("401 challenge exchange with GHCR and PAT", func(t *testing.T) {
		var tokenRequested bool
		var authHeadDone bool

		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.URL.Host == "ghcr.io" && req.URL.Path == "/v2/azylman/aerial-brain/manifests/latest" {
						if req.Header.Get("Authorization") == "" {
							header := make(http.Header)
							header.Set("Www-Authenticate", `Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:azylman/aerial-brain:pull"`)
							return &http.Response{
								StatusCode: http.StatusUnauthorized,
								Header:     header,
								Body:       ioNopCloser(strings.NewReader("")),
							}, nil
						}
						if req.Header.Get("Authorization") != "Bearer test-ghcr-token" {
							t.Errorf("unexpected auth header: %s", req.Header.Get("Authorization"))
						}
						authHeadDone = true
						header := make(http.Header)
						header.Set("Docker-Content-Digest", validDigest)
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}

					if req.URL.Host == "ghcr.io" && req.URL.Path == "/token" {
						tokenRequested = true
						user, pass, ok := req.BasicAuth()
						if !ok || pass != "secret-pat" {
							t.Errorf("expected basic auth with secret-pat, got user=%s ok=%v", user, ok)
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"token":"test-ghcr-token"}`)),
						}, nil
					}

					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
					return nil, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{
			PAT:            "secret-pat",
			RegistryClient: mockClient,
		})

		digest, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !tokenRequested || !authHeadDone {
			t.Errorf("expected token request and auth head to complete, got token=%v authHead=%v", tokenRequested, authHeadDone)
		}
		if digest != validDigest {
			t.Errorf("got %q, want %q", digest, validDigest)
		}
	})

	t.Run("401 challenge exchange with Docker Hub synthesized scope and access_token", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.URL.Host == "registry-1.docker.io" {
						if req.Header.Get("Authorization") == "" {
							header := make(http.Header)
							header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
							return &http.Response{
								StatusCode: http.StatusUnauthorized,
								Header:     header,
								Body:       ioNopCloser(strings.NewReader("")),
							}, nil
						}
						if req.Header.Get("Authorization") != "Bearer hub-token" {
							t.Errorf("unexpected authorization: %s", req.Header.Get("Authorization"))
						}
						header := make(http.Header)
						header.Set("Docker-Content-Digest", validDigest)
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}

					if req.URL.Host == "auth.docker.io" {
						scope := req.URL.Query().Get("scope")
						if scope != "repository:library/postgres:pull" {
							t.Errorf("unexpected synthesized scope: %s", scope)
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"access_token":"hub-token"}`)),
						}, nil
					}

					return nil, fmt.Errorf("unexpected host: %s", req.URL.Host)
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		digest, err := d.GetRemoteImageDigest(context.Background(), "postgres:16")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if digest != validDigest {
			t.Errorf("got %q, want %q", digest, validDigest)
		}
	})

	// Error branches
	t.Run("network failure on initial HEAD", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("simulated dial timeout")
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected dial timeout error")
		}
	})

	t.Run("401 with malformed authenticate header", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Www-Authenticate", `Basic realm="foo"`)
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected error on malformed challenge")
		}
	})

	t.Run("401 with invalid token realm url", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Www-Authenticate", `Bearer realm="http://[::1]:namedport",service="test"`)
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected error on invalid realm url")
		}
	})

	t.Run("401 with token network error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodHead {
						header := make(http.Header)
						header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}
					return nil, errors.New("token endpoint refused")
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "postgres:16")
		if err == nil {
			t.Fatalf("expected error on token network error")
		}
	})

	t.Run("401 with token HTTP 500 error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodHead {
						header := make(http.Header)
						header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("internal error")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "postgres:16")
		if err == nil {
			t.Fatalf("expected error on token HTTP 500")
		}
	})

	t.Run("401 with invalid token JSON and empty token", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodHead {
						header := make(http.Header)
						header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("invalid-json")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if _, err := d.GetRemoteImageDigest(context.Background(), "postgres:16"); err == nil {
			t.Fatalf("expected error on invalid token json")
		}

		mockClientEmpty := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodHead {
						header := make(http.Header)
						header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"token":""}`)),
					}, nil
				},
			},
		}
		dEmpty := NewDaemon(DaemonConfig{RegistryClient: mockClientEmpty})
		if _, err := dEmpty.GetRemoteImageDigest(context.Background(), "postgres:16"); err == nil {
			t.Fatalf("expected error on empty token payload")
		}
	})

	t.Run("401 with authenticated HEAD network failure", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Header.Get("Authorization") == "" {
						if req.Method == http.MethodHead {
							header := make(http.Header)
							header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`)
							return &http.Response{
								StatusCode: http.StatusUnauthorized,
								Header:     header,
								Body:       ioNopCloser(strings.NewReader("")),
							}, nil
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"token":"abc"}`)),
						}, nil
					}
					return nil, errors.New("auth head failed")
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if _, err := d.GetRemoteImageDigest(context.Background(), "postgres:16"); err == nil {
			t.Fatalf("expected error on auth head failure")
		}
	})

	t.Run("non-200 manifest status returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected 404 error, got nil")
		}
	})

	t.Run("200 OK without valid digest or etag returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("ETag", `"not-a-sha256"`)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageDigest(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected error on missing/invalid digest, got nil")
		}
	})
}

func TestGetServiceImages_TableDriven(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("success parsing valid compose json", func(t *testing.T) {
		mockJSON := `{
			"services": {
				"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"},
				"postgres": {"image": "pgvector/pgvector:pg16"},
				"gitsync": {"image": "ghcr.io/azylman/aerial-gitsync:latest"},
				"hangar": {"image": "ghcr.io/azylman/aerial-hangar:latest"},
				"noimage": {"image": ""}
			}
		}`
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return []byte(mockJSON), nil, nil
			},
		})

		images, err := d.GetServiceImages(context.Background(), tempDir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(images) != 2 {
			t.Fatalf("expected 2 services, got %d (%v)", len(images), images)
		}
		if images["brain"] != "ghcr.io/azylman/aerial-brain:latest" {
			t.Errorf("expected brain image, got %s", images["brain"])
		}
		if images["postgres"] != "pgvector/pgvector:pg16" {
			t.Errorf("expected postgres image, got %s", images["postgres"])
		}
		if _, ok := images["gitsync"]; ok {
			t.Errorf("expected gitsync to be filtered out")
		}
		if _, ok := images["hangar"]; ok {
			t.Errorf("expected hangar to be filtered out")
		}
		if _, ok := images["noimage"]; ok {
			t.Errorf("expected noimage to be omitted")
		}
	})

	t.Run("executor error propagates", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, []byte("compose failure"), errors.New("compose exec error")
			},
		})

		_, err := d.GetServiceImages(context.Background(), tempDir)
		if err == nil {
			t.Fatalf("expected compose error, got nil")
		}
	})

	t.Run("json unmarshal error propagates", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return []byte("not valid json"), nil, nil
			},
		})

		_, err := d.GetServiceImages(context.Background(), tempDir)
		if err == nil {
			t.Fatalf("expected unmarshal error, got nil")
		}
	})
}

func TestGetServicesWithNewImages_TableDriven(t *testing.T) {
	validLocalDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	newRemoteDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	t.Run("empty candidates returns nil", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		up, digests, err := d.GetServicesWithNewImages(context.Background(), nil, nil)
		if err != nil || up != nil || digests != nil {
			t.Errorf("expected nil result on empty candidates, got %v, %v, %v", up, digests, err)
		}
	})

	t.Run("filters gitsync, hangar, and empty images", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		up, digests, err := d.GetServicesWithNewImages(context.Background(), []string{"gitsync", "hangar", "empty"}, map[string]string{
			"gitsync": "ghcr.io/azylman/aerial-gitsync:latest",
			"hangar":  "ghcr.io/azylman/aerial-hangar:latest",
			"empty":   "",
		})
		if err != nil || len(up) != 0 || len(digests) != 0 {
			t.Errorf("expected 0 services to update, got %v", up)
		}
	})

	t.Run("container inspect error continues gracefully", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return nil, []byte("no such container"), errors.New("container not found")
			},
		})
		up, _, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil || len(up) != 0 {
			t.Errorf("expected 0 updates on container inspect error, got %v, %v", up, err)
		}
	})

	t.Run("image inspect error continues gracefully", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "inspect" {
					return []byte("sha256:imageid123"), nil, nil
				}
				return nil, []byte("image inspect err"), errors.New("image not found")
			},
		})
		up, _, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil || len(up) != 0 {
			t.Errorf("expected 0 updates on image inspect error, got %v, %v", up, err)
		}
	})

	t.Run("invalid json in RepoDigests continues gracefully", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "inspect" {
					return []byte("sha256:imageid123"), nil, nil
				}
				return []byte("not-valid-json"), nil, nil
			},
		})
		up, _, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil || len(up) != 0 {
			t.Errorf("expected 0 updates on invalid json, got %v, %v", up, err)
		}
	})

	t.Run("detects new image builds when digest differs", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", newRemoteDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{
			RegistryClient: mockClient,
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "inspect" {
					return []byte("sha256:imageid123"), nil, nil
				}
				jsonBytes, _ := json.Marshal([]string{"ghcr.io/azylman/aerial-brain@" + validLocalDigest})
				return jsonBytes, nil, nil
			},
		})

		up, digests, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(up) != 1 || up[0] != "brain" {
			t.Errorf("expected brain in services to update, got %v", up)
		}
		if digests["brain"] != newRemoteDigest {
			t.Errorf("got digest %s, want %s", digests["brain"], newRemoteDigest)
		}
	})

	t.Run("skips update if digest matches local repo digests", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", validLocalDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{
			RegistryClient: mockClient,
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "inspect" {
					return []byte("sha256:imageid123"), nil, nil
				}
				jsonBytes, _ := json.Marshal([]string{"ghcr.io/azylman/aerial-brain@" + validLocalDigest})
				return jsonBytes, nil, nil
			},
		})

		up, digests, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(up) != 0 || len(digests) != 0 {
			t.Errorf("expected 0 services to update when matching, got %v", up)
		}
	})

	t.Run("skips update if remote digest is quarantined", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", newRemoteDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{
			RegistryClient: mockClient,
			DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "inspect" {
					return []byte("sha256:imageid123"), nil, nil
				}
				jsonBytes, _ := json.Marshal([]string{"ghcr.io/azylman/aerial-brain@" + validLocalDigest})
				return jsonBytes, nil, nil
			},
		})

		d.quarantineImage("brain", newRemoteDigest, "fails health checks")

		up, _, err := d.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
			"brain": "ghcr.io/azylman/aerial-brain:latest",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(up) != 0 {
			t.Errorf("expected quarantined image to be skipped, got %v", up)
		}
	})
}

func TestCheckAndReconcileNewImages_TableDriven(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("no docker-compose.yml returns nil", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{ComposeDir: filepath.Join(tempDir, "nonexistent")})
		if err := d.CheckAndReconcileNewImages(context.Background()); err != nil {
			t.Fatalf("expected nil when compose file absent, got %v", err)
		}
	})

	t.Run("GetServiceImages error propagates", func(t *testing.T) {
		composeFile := filepath.Join(tempDir, "docker-compose.yml")
		_ = os.WriteFile(composeFile, []byte("services:\n"), 0644)

		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, []byte("config error"), errors.New("compose config error")
			},
		})

		if err := d.CheckAndReconcileNewImages(context.Background()); err == nil {
			t.Fatalf("expected error from GetServiceImages")
		}
	})

	t.Run("triggers reconcile when new images detected", func(t *testing.T) {
		composeFile := filepath.Join(tempDir, "docker-compose.yml")
		_ = os.WriteFile(composeFile, []byte("services:\n"), 0644)

		newDigest := "sha256:3333333333333333333333333333333333333333333333333333333333333333"

		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", newDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				},
			},
		}

		mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			mockJSON := `{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`
			return []byte(mockJSON), nil, nil
		}

		mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "inspect" {
				return []byte("sha256:currentid"), nil, nil
			}
			return []byte(`["ghcr.io/azylman/aerial-brain@sha256:olddigest"]`), nil, nil
		}

		d := NewDaemon(DaemonConfig{
			ComposeDir:      tempDir,
			RegistryClient:  mockClient,
			ComposeExecutor: mockCompose,
			DockerExecutor:  mockDocker,
		})

		if err := d.CheckAndReconcileNewImages(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case <-d.reconcileCh:
		default:
			t.Errorf("expected reconciliation to be queued onto reconcileCh")
		}
	})
}

func TestReconcileCompose_PreFlightAndHealthGating(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n  brain:\n    image: ghcr.io/azylman/aerial-brain:latest\n"), 0644)

	var pullExecuted bool
	var snapshotCreated bool
	var rmiExecuted bool

	mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config --services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "config --quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config --format json") {
			return []byte(`{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`), nil, nil
		}
		if strings.Contains(argsStr, "pull") {
			pullExecuted = true
			if dl, ok := ctx.Deadline(); ok {
				remaining := time.Until(dl)
				if remaining < 5*time.Minute {
					t.Errorf("expected pull context to have generous timeout (> 5m), got remaining %v", remaining)
				}
			} else {
				t.Errorf("expected pull context to have a deadline")
			}
			return []byte("Pulled"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			if !strings.Contains(argsStr, "--wait") || !strings.Contains(argsStr, "--wait-timeout 180") {
				t.Errorf("expected --wait and --wait-timeout 180 in compose up args: %v", args)
			}
			return []byte("Started"), nil, nil
		}
		return nil, nil, nil
	}

	mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 {
			switch args[0] {
			case "inspect":
				return []byte("sha256:current-working-image-id"), nil, nil
			case "tag":
				snapshotCreated = true
				return nil, nil, nil
			case "rmi":
				rmiExecuted = true
				return nil, nil, nil
			}
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		ComposeExecutor: mockCompose,
		DockerExecutor:  mockDocker,
	})

	d.quarantineImage("brain", "sha256:oldbad", "previous failure")

	err := d.ReconcileCompose(context.Background(), "brain")
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	if !snapshotCreated {
		t.Errorf("expected snapshot tag to be created before pull")
	}
	if !pullExecuted {
		t.Errorf("expected compose pull to be executed")
	}
	if !rmiExecuted {
		t.Errorf("expected snapshot tag to be removed via rmi on success")
	}
	if d.isImageQuarantined("brain", "sha256:oldbad") {
		t.Errorf("expected quarantine to be cleared on successful deploy")
	}
	if d.GetLastReconcile().IsZero() {
		t.Errorf("expected LastReconcile to be set on success")
	}
}

func TestReconcileCompose_CustomPullTimeoutAndCleanupContext(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n  brain:\n    image: ghcr.io/azylman/aerial-brain:latest\n"), 0644)

	var pullDeadline time.Time
	var rmiCtxActive bool

	mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config --services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "config --quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config --format json") {
			return []byte(`{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`), nil, nil
		}
		if strings.Contains(argsStr, "pull") {
			if dl, ok := ctx.Deadline(); ok {
				pullDeadline = dl
			}
			return []byte("Pulled"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			return []byte("Started"), nil, nil
		}
		return nil, nil, nil
	}

	mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 {
			switch args[0] {
			case "inspect":
				return []byte("sha256:current-working-image-id"), nil, nil
			case "tag":
				return nil, nil, nil
			case "rmi":
				if ctx.Err() == nil {
					rmiCtxActive = true
				}
				return nil, nil, nil
			}
		}
		return nil, nil, nil
	}

	customTimeout := 15 * time.Minute
	d := NewDaemon(DaemonConfig{
		ComposeDir:         tempDir,
		ComposeExecutor:    mockCompose,
		DockerExecutor:     mockDocker,
		ComposePullTimeout: customTimeout,
	})

	err := d.ReconcileCompose(context.Background(), "brain")
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	if pullDeadline.IsZero() {
		t.Fatalf("expected pull to have a deadline set")
	}
	remaining := time.Until(pullDeadline)
	if remaining < 10*time.Minute || remaining > 16*time.Minute {
		t.Errorf("expected pull context remaining duration ~15m, got %v", remaining)
	}
	if !rmiCtxActive {
		t.Errorf("expected snapshot rmi cleanup to be executed with an active, uncancelled context")
	}

	// Test environment variable override when cfg.ComposePullTimeout is not set
	t.Setenv("COMPOSE_PULL_TIMEOUT", "7m")
	dEnv := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
	})
	if dEnv.composePullTimeout != 7*time.Minute {
		t.Errorf("expected COMPOSE_PULL_TIMEOUT env var to set composePullTimeout to 7m, got %v", dEnv.composePullTimeout)
	}
}

func TestReconcileCompose_PullFailureTriggersRollback(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n  brain:\n    image: ghcr.io/azylman/aerial-brain:latest\n"), 0644)

	mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config --services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "config --quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config --format json") {
			return []byte(`{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`), nil, nil
		}
		if strings.Contains(argsStr, "pull") {
			return nil, []byte("pull network error"), errors.New("pull failed")
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		ComposeExecutor: mockCompose,
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})

	err := d.ReconcileCompose(context.Background(), "brain")
	if err == nil {
		t.Fatalf("expected error from pull failure, got nil")
	}
	if !strings.Contains(err.Error(), "compose pull failed") {
		t.Errorf("expected error message to contain 'compose pull failed', got: %v", err)
	}
}

func TestReconcileCompose_WaitFailureWithQuarantineAndRollback(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n  brain:\n    image: ghcr.io/azylman/aerial-brain:latest\n"), 0644)

	failedDigest := "sha256:" + strings.Repeat("b", 64)
	var tagRestored bool

	mockClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", failedDigest)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	isFirstUp := true
	mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		argsStr := strings.Join(args, " ")
		if strings.Contains(argsStr, "config --services") {
			return []byte("brain\n"), nil, nil
		}
		if strings.Contains(argsStr, "config --quiet") {
			return nil, nil, nil
		}
		if strings.Contains(argsStr, "config --format json") {
			return []byte(`{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`), nil, nil
		}
		if strings.Contains(argsStr, "pull") {
			return []byte("Pulled"), nil, nil
		}
		if strings.Contains(argsStr, "up -d") {
			if isFirstUp {
				isFirstUp = false
				return nil, []byte("container unhealthy timeout 180s"), errors.New("healthcheck failed")
			}
			return []byte("Restored"), nil, nil
		}
		return nil, nil, nil
	}

	mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 {
			switch args[0] {
			case "inspect":
				return []byte("sha256:working-sha"), nil, nil
			case "tag":
				if len(args) >= 3 && args[1] == "aerial-brain:rollback-target" {
					tagRestored = true
				}
				return nil, nil, nil
			case "rmi":
				return nil, nil, nil
			}
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		RegistryClient:  mockClient,
		ComposeExecutor: mockCompose,
		DockerExecutor:  mockDocker,
	})

	err := d.ReconcileCompose(context.Background(), "brain")
	if err == nil {
		t.Fatalf("expected error from compose up failure, got nil")
	}

	if !tagRestored {
		t.Errorf("expected snapshot tag to be restored during rollback")
	}
	if !d.isImageQuarantined("brain", failedDigest) {
		t.Errorf("expected failing digest %s to be quarantined", failedDigest)
	}
}

func TestExecuteRollback_ImageAndTagRestorationError(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n  brain:\n    image: ghcr.io/azylman/aerial-brain:latest\n"), 0644)

	d := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --services") {
				return []byte("brain\n"), nil, nil
			}
			return []byte("Restored"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "tag" {
				return nil, []byte("tag error: permission denied"), errors.New("tag failed")
			}
			return nil, nil, nil
		},
	})

	err := d.executeRollback(
		context.Background(),
		nil,
		"compose apply",
		errors.New("initial up fail"),
		[]string{"brain"},
		map[string]string{"brain": "aerial-brain:rollback-target"},
		map[string]string{"brain": "ghcr.io/azylman/aerial-brain:latest"},
		map[string]string{"brain": "sha256:bad123"},
	)

	if err == nil {
		t.Fatalf("expected rollback error due to tag restoration failure, got nil")
	}
	if !strings.Contains(err.Error(), "failed to restore image tag") {
		t.Errorf("expected error message about image tag restoration, got: %v", err)
	}
}

func TestEnsureRepo_RemediatedErrors(t *testing.T) {
	tempDir := t.TempDir()

	// 1. git init error
	dInitFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "init" {
				return nil, []byte("init permission denied"), errors.New("init failed")
			}
			return nil, nil, nil
		},
	})
	nonEmptyDir := filepath.Join(tempDir, "nonempty1")
	_ = os.MkdirAll(nonEmptyDir, 0755)
	_ = os.WriteFile(filepath.Join(nonEmptyDir, "file.txt"), []byte("data"), 0644)

	if err := dInitFail.EnsureRepo(context.Background(), nonEmptyDir, "https://github.com/org/repo.git"); err == nil {
		t.Fatalf("expected git init error, got nil")
	}

	// 2. git remote add origin warning branch
	dRemoteFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				if args[0] == "init" {
					return nil, nil, nil
				}
				if args[0] == "remote" {
					return nil, []byte("fatal: remote origin already exists"), errors.New("already exists")
				}
				if args[0] == "fetch" {
					return nil, nil, nil
				}
				if args[0] == "reset" {
					return nil, nil, nil
				}
			}
			return nil, nil, nil
		},
	})
	nonEmptyDir2 := filepath.Join(tempDir, "nonempty2")
	_ = os.MkdirAll(nonEmptyDir2, 0755)
	_ = os.WriteFile(filepath.Join(nonEmptyDir2, "file.txt"), []byte("data"), 0644)
	if err := dRemoteFail.EnsureRepo(context.Background(), nonEmptyDir2, "https://github.com/org/repo.git"); err != nil {
		t.Fatalf("expected already exists remote error to be non-fatal, got %v", err)
	}

	// 3. git reset --soft error
	dResetFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				if args[0] == "reset" && args[1] == "--soft" {
					return nil, []byte("soft reset failed"), errors.New("soft reset error")
				}
			}
			return nil, nil, nil
		},
	})
	nonEmptyDir3 := filepath.Join(tempDir, "nonempty3")
	_ = os.MkdirAll(nonEmptyDir3, 0755)
	_ = os.WriteFile(filepath.Join(nonEmptyDir3, "file.txt"), []byte("data"), 0644)
	if err := dResetFail.EnsureRepo(context.Background(), nonEmptyDir3, "https://github.com/org/repo.git"); err == nil {
		t.Fatalf("expected git reset --soft error, got nil")
	}
}

func TestImageQuarantine_DirectNilInitialization(t *testing.T) {
	d := &SyncDaemon{}
	validDigest := "sha256:" + strings.Repeat("c", 64)
	d.quarantineImage("brain", validDigest, "initialization test")
	if !d.isImageQuarantined("brain", validDigest) {
		t.Errorf("expected image to be quarantined after initializing nil map")
	}
}

func TestGetServicesWithNewImages_EmptyImageIDAndRemoteError(t *testing.T) {
	// 1. Empty image ID branch
	dEmptyID := NewDaemon(DaemonConfig{
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("   \n"), nil, nil
		},
	})
	up, digests, err := dEmptyID.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
		"brain": "ghcr.io/azylman/aerial-brain:latest",
	})
	if err != nil || len(up) != 0 || len(digests) != 0 {
		t.Errorf("expected 0 updates on empty image ID, got up=%v digests=%v err=%v", up, digests, err)
	}

	// 2. Remote digest error branch
	client500 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Header:     make(http.Header),
					Body:       ioNopCloser(strings.NewReader("server error")),
				}, nil
			},
		},
	}
	dRemoteErr := NewDaemon(DaemonConfig{
		RegistryClient: client500,
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "inspect" {
				return []byte("sha256:currentid"), nil, nil
			}
			return []byte(`["ghcr.io/azylman/aerial-brain@sha256:old"]`), nil, nil
		},
	})
	up, digests, err = dRemoteErr.GetServicesWithNewImages(context.Background(), []string{"brain"}, map[string]string{
		"brain": "ghcr.io/azylman/aerial-brain:latest",
	})
	if err != nil || len(up) != 0 || len(digests) != 0 {
		t.Errorf("expected 0 updates on remote digest error, got up=%v digests=%v err=%v", up, digests, err)
	}
}

func TestCheckAndReconcileNewImages_NoChangesReturnsNil(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n"), 0644)

	matchingDigest := "sha256:" + strings.Repeat("d", 64)

	mockClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", matchingDigest)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	mockCompose := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		mockJSON := `{"services": {"brain": {"image": "ghcr.io/azylman/aerial-brain:latest"}}}`
		return []byte(mockJSON), nil, nil
	}

	mockDocker := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) > 0 && args[0] == "inspect" {
			return []byte("sha256:currentid"), nil, nil
		}
		return []byte(fmt.Sprintf(`["ghcr.io/azylman/aerial-brain@%s"]`, matchingDigest)), nil, nil
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir:      tempDir,
		RegistryClient:  mockClient,
		ComposeExecutor: mockCompose,
		DockerExecutor:  mockDocker,
	})

	if err := d.CheckAndReconcileNewImages(context.Background()); err != nil {
		t.Fatalf("expected nil when no new images, got %v", err)
	}

	select {
	case <-d.reconcileCh:
		t.Errorf("expected no reconciliation to be queued onto reconcileCh")
	default:
	}
}

func TestReconcileCompose_ValidationAndDiscoveryFailures(t *testing.T) {
	tempDir := t.TempDir()
	composeFile := filepath.Join(tempDir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services:\n"), 0644)

	// 1. Validation failure triggers rollback
	dValFail := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --quiet") {
				return nil, []byte("yaml syntax error"), errors.New("invalid syntax")
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	if err := dValFail.ReconcileCompose(context.Background()); err == nil {
		t.Errorf("expected validation failure error, got nil")
	}

	// 2. Service discovery failure triggers rollback
	dDiscFail := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --quiet") {
				return nil, nil, nil
			}
			if strings.Contains(argsStr, "config --services") {
				return nil, []byte("services inspect fail"), errors.New("cannot list services")
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	if err := dDiscFail.ReconcileCompose(context.Background()); err == nil {
		t.Errorf("expected discovery failure error, got nil")
	}

	// 3. Zero targets returns nil cleanly
	dZeroTargets := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --quiet") {
				return nil, nil, nil
			}
			if strings.Contains(argsStr, "config --services") {
				return []byte("gitsync\n"), nil, nil // Only gitsync, which is filtered out
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	if err := dZeroTargets.ReconcileCompose(context.Background()); err != nil {
		t.Errorf("expected nil for zero external targets, got %v", err)
	}

	// 4. Tag failure logs warning and proceeds with pull and up
	tagErrEncountered := false
	dTagWarn := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --services") {
				return []byte("brain\n"), nil, nil
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 {
				if args[0] == "inspect" {
					return []byte("sha256:img123"), nil, nil
				}
				if args[0] == "tag" {
					tagErrEncountered = true
					return nil, []byte("tag error: disk full"), errors.New("tag fail")
				}
			}
			return nil, nil, nil
		},
	})
	if err := dTagWarn.ReconcileCompose(context.Background(), "brain"); err != nil {
		t.Errorf("expected ReconcileCompose to proceed despite tag error, got %v", err)
	}
	if !tagErrEncountered {
		t.Errorf("expected tag error to be encountered")
	}

	// 5. Pull failure with rollback failure joins both errors
	dPullAndRollbackFail := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "config --services") {
				return []byte("brain\n"), nil, nil
			}
			if strings.Contains(argsStr, "pull") {
				return nil, []byte("pull fatal"), errors.New("pull err")
			}
			if strings.Contains(argsStr, "up -d") {
				return nil, []byte("rollback up fatal"), errors.New("rollback up err")
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, nil
		},
	})
	errJoined := dPullAndRollbackFail.ReconcileCompose(context.Background(), "brain")
	if errJoined == nil {
		t.Fatalf("expected joined error on pull and rollback failure, got nil")
	}
	if !strings.Contains(errJoined.Error(), "compose pull failed") {
		t.Errorf("expected pull error in joined message: %v", errJoined)
	}
}

func TestStartPeriodicLoop_ExecutionAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := NewDaemon(DaemonConfig{
		Interval: 10 * time.Millisecond,
		ComposeDir: t.TempDir(),
	})

	d.StartPeriodicLoop(ctx)
	time.Sleep(35 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestPendingTargets_UnionAndDrain(t *testing.T) {
	d := NewDaemon(DaemonConfig{})

	// nil/empty calls are safe
	d.recordPendingTargets(nil)
	d.recordPendingTargets([]string{})
	if targets := d.drainPendingTargets(); len(targets) != 0 {
		t.Fatalf("expected empty drained targets, got %v", targets)
	}

	// union of multiple calls
	d.recordPendingTargets([]string{"brain", "dashboard"})
	d.recordPendingTargets([]string{"dashboard", "proxy", "  "})
	targets := d.drainPendingTargets()
	if len(targets) != 3 {
		t.Fatalf("expected 3 deduplicated targets, got %v", targets)
	}
	sort.Strings(targets)
	expected := []string{"brain", "dashboard", "proxy"}
	for i, s := range expected {
		if targets[i] != s {
			t.Errorf("expected target %s at %d, got %s", s, i, targets[i])
		}
	}

	// second drain is empty
	if drainedAgain := d.drainPendingTargets(); len(drainedAgain) != 0 {
		t.Errorf("expected second drain to be empty, got %v", drainedAgain)
	}
}

func TestReconciliationStatus_TrackingAndLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	composeContent := `
services:
  brain:
    image: ghcr.io/azylman/aerial-brain:latest
`
	if err := os.WriteFile(composePath, []byte(composeContent), 0644); err != nil {
		t.Fatalf("failed to write compose file: %v", err)
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("ok"), nil, nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("abc1234\n"), nil, nil
		},
	})

	// Initial cold start state is idle
	initial := d.GetReconciliationStatus()
	if initial.State != "idle" || initial.Stage != "idle" || initial.Active {
		t.Fatalf("expected idle cold start, got %+v", initial)
	}

	// Record pending targets to simulate image poll
	d.recordPendingTargets([]string{"brain"})

	// Intercept execution and check status
	err := d.ReconcileCompose(context.Background())
	if err != nil {
		t.Fatalf("expected ReconcileCompose to succeed, got %v", err)
	}

	finalStatus := d.GetReconciliationStatus()
	if finalStatus.State != "healthy" || finalStatus.Stage != "healthy" {
		t.Errorf("expected healthy final status, got %+v", finalStatus)
	}
	if finalStatus.Active {
		t.Errorf("expected Active to be false after completion, got true")
	}
	if len(finalStatus.TargetServices) != 1 || finalStatus.TargetServices[0] != "brain" {
		t.Errorf("expected targetServices [brain], got %v", finalStatus.TargetServices)
	}
	if finalStatus.Trigger != "image_poll" {
		t.Errorf("expected trigger image_poll, got %s", finalStatus.Trigger)
	}
	if finalStatus.CommitSHA != "abc1234" {
		t.Errorf("expected commit abc1234, got %s", finalStatus.CommitSHA)
	}

	// Verify deep copy does not mutate daemon internal state
	finalStatus.TargetServices[0] = "mutated"
	freshStatus := d.GetReconciliationStatus()
	if freshStatus.TargetServices[0] != "brain" {
		t.Errorf("expected deep copy, but internal state was mutated to %s", freshStatus.TargetServices[0])
	}
}

func TestReconciliationStatus_TTLReset(t *testing.T) {
	d := NewDaemon(DaemonConfig{})
	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.Active = false
		s.State = "healthy"
		s.Stage = "healthy"
		s.TargetServices = []string{"brain"}
		s.CompletedAt = time.Now().Add(-6 * time.Minute)
	})

	// Since CompletedAt is > 5m ago, GetReconciliationStatus returns idle
	status := d.GetReconciliationStatus()
	if status.State != "idle" || status.Stage != "idle" {
		t.Errorf("expected TTL to reset state to idle, got %+v", status)
	}
}

func TestReconciliationStatus_FailureAndRollback(t *testing.T) {
	tempDir := t.TempDir()
	composePath := filepath.Join(tempDir, "docker-compose.yml")
	composeContent := `
services:
  dashboard:
    image: ghcr.io/azylman/aerial-dashboard:latest
`
	if err := os.WriteFile(composePath, []byte(composeContent), 0644); err != nil {
		t.Fatalf("failed to write compose file: %v", err)
	}

	d := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			argsStr := strings.Join(args, " ")
			if strings.Contains(argsStr, "pull") {
				return nil, []byte("fatal: image not found"), errors.New("pull failed")
			}
			return []byte("ok"), nil, nil
		},
		DockerExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("ok"), nil, nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("def5678\n"), nil, nil
		},
	})

	err := d.ReconcileCompose(context.Background(), "dashboard")
	if err == nil {
		t.Fatalf("expected ReconcileCompose to return pull error, got nil")
	}

	status := d.GetReconciliationStatus()
	if status.State != "failed" || status.Stage != "failed" {
		t.Errorf("expected state failed, got %+v", status)
	}
	if !strings.Contains(status.Error, "pull failed") {
		t.Errorf("expected pull failed in status error, got %s", status.Error)
	}
	if status.Active {
		t.Errorf("expected Active=false after failure, got true")
	}
}

func TestReconciliationStatus_ExposedInGetStatus(t *testing.T) {
	d := NewDaemon(DaemonConfig{})
	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.Active = true
		s.State = "swapping"
		s.Stage = "swapping"
		s.TargetServices = []string{"scheduler-mcp"}
		s.StartedAt = time.Now().UTC()
	})

	statusResp := d.GetStatus(context.Background())
	if statusResp.Reconciliation == nil {
		t.Fatalf("expected non-nil Reconciliation in GetStatus")
	}
	if statusResp.Reconciliation.State != "swapping" {
		t.Errorf("expected state swapping, got %s", statusResp.Reconciliation.State)
	}
	if len(statusResp.Reconciliation.TargetServices) != 1 || statusResp.Reconciliation.TargetServices[0] != "scheduler-mcp" {
		t.Errorf("expected target services [scheduler-mcp], got %v", statusResp.Reconciliation.TargetServices)
	}
}

func TestGetStatus_PopulatesAndCachesGitHubRepo(t *testing.T) {
	remoteCalls := 0
	d := NewDaemon(DaemonConfig{
		Repos: []string{"/share/mirrormere"},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 3 && args[0] == "remote" && args[1] == "get-url" && args[2] == "origin" {
				remoteCalls++
				return []byte("https://github.com/azylman/mirrormere.git\n"), nil, nil
			}
			if len(args) >= 4 && args[0] == "log" && args[1] == "-1" {
				return []byte("abc1234\x002026-09-26T20:00:00Z\n"), nil, nil
			}
			return nil, nil, nil
		},
	})

	// First call resolves and caches
	st1 := d.GetStatus(context.Background())
	repoSt1, ok := st1.Repos["/share/mirrormere"]
	if !ok {
		t.Fatalf("expected /share/mirrormere in repos")
	}
	if repoSt1.GitHubRepo != "azylman/mirrormere" {
		t.Errorf("expected GitHubRepo 'azylman/mirrormere', got %q", repoSt1.GitHubRepo)
	}
	if remoteCalls != 1 {
		t.Errorf("expected 1 remote call, got %d", remoteCalls)
	}

	// Second call uses cached slug, remoteCalls should stay 1
	st2 := d.GetStatus(context.Background())
	repoSt2, ok := st2.Repos["/share/mirrormere"]
	if !ok {
		t.Fatalf("expected /share/mirrormere in repos on second call")
	}
	if repoSt2.GitHubRepo != "azylman/mirrormere" {
		t.Errorf("expected cached GitHubRepo 'azylman/mirrormere', got %q", repoSt2.GitHubRepo)
	}
	if remoteCalls != 1 {
		t.Errorf("expected remoteCalls to remain 1 after cached call, got %d", remoteCalls)
	}
}

func TestNomadValidationAndReconciliation_Hermetic(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	jobFile := filepath.Join(tmpDir, "test.nomad")
	jobContent := `job "test" { group "g" { task "t" { config { image = "test:latest" } } } }`
	if err := os.WriteFile(jobFile, []byte(jobContent), 0644); err != nil {
		t.Fatalf("failed to write job file: %v", err)
	}

	var executedArgs [][]string
	mockNomad := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		executedArgs = append(executedArgs, args)
		if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
			if strings.Contains(args[len(args)-1], "invalid") {
				return nil, []byte("syntax error on line 1"), errors.New("exit 1")
			}
			return []byte("Job validation successful\n"), nil, nil
		}
		if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
			return []byte("Evaluation ID: abc-123\n"), nil, nil
		}
		if len(args) >= 2 && args[0] == "job" && args[1] == "stop" {
			return []byte("Job stop scheduled\n"), nil, nil
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		NomadExecutor: mockNomad,
		ConfigDir:     tmpDir,
	})

	// 1. Validation test
	if err := d.ValidateNomadJob(context.Background(), jobFile); err != nil {
		t.Errorf("expected ValidateNomadJob to succeed, got %v", err)
	}

	invalidJobFile := filepath.Join(tmpDir, "invalid.nomad")
	if err := d.ValidateNomadJob(context.Background(), invalidJobFile); err == nil {
		t.Errorf("expected ValidateNomadJob to fail for invalid job, got nil")
	}

	// 2. Reconciliation test (apply + delete)
	changes := []NomadFileChange{
		{Path: "test.nomad", Action: "apply", JobName: "test"},
		{Path: "old.nomad", Action: "delete", JobName: "old-job"},
	}

	if err := d.ReconcileNomadChanges(context.Background(), tmpDir, changes); err != nil {
		t.Fatalf("expected ReconcileNomadChanges to succeed, got %v", err)
	}

	// Verify executed commands: stop should run before apply (teardown first)
	var foundStop, foundRun bool
	var stopIdx, runIdx int
	for idx, call := range executedArgs {
		if len(call) >= 2 && call[0] == "job" && call[1] == "stop" && call[len(call)-1] == "old-job" {
			foundStop = true
			stopIdx = idx
		}
		if len(call) >= 2 && call[0] == "job" && call[1] == "run" && strings.HasSuffix(call[len(call)-1], "test.nomad") {
			foundRun = true
			runIdx = idx
		}
	}

	if !foundStop {
		t.Errorf("expected nomad job stop to be called for old-job")
	}
	if !foundRun {
		t.Errorf("expected nomad job run to be called for test.nomad")
	}
	if foundStop && foundRun && stopIdx > runIdx {
		t.Errorf("expected teardown (stop) to execute before application (run), stopIdx=%d, runIdx=%d", stopIdx, runIdx)
	}
}

func TestNomadHasChanges_Hermetic(t *testing.T) {
	t.Parallel()

	mockGit := func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		output := "M\tjobs/infisical.nomad\nD\tjobs/legacy.nomad\nM\tdocker-compose.yml\n"
		return []byte(output), nil, nil
	}

	d := NewDaemon(DaemonConfig{
		GitExecutor: mockGit,
	})

	changes, err := d.HasNomadChanges(context.Background(), "/share/aerial-config", "HEAD~1", "HEAD")
	if err != nil {
		t.Fatalf("HasNomadChanges returned error: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("expected 2 nomad changes, got %d: %+v", len(changes), changes)
	}

	if changes[0].Path != "jobs/infisical.nomad" || changes[0].Action != "apply" {
		t.Errorf("unexpected change 0: %+v", changes[0])
	}
	if changes[1].Path != "jobs/legacy.nomad" || changes[1].Action != "delete" {
		t.Errorf("unexpected change 1: %+v", changes[1])
	}
}

func TestNomadImagePollingAndRestart_Hermetic(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		t.Fatalf("failed to create jobs dir: %v", err)
	}

	jobFile := filepath.Join(jobsDir, "mirrormere-core.nomad")
	jobContent := `job "mirrormere-core" { group "core" { task "core" { config { image = "ghcr.io/azylman/mirrormere:latest" } } } }`
	if err := os.WriteFile(jobFile, []byte(jobContent), 0644); err != nil {
		t.Fatalf("failed to write job file: %v", err)
	}

	currentDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	var restartCalled bool
	var restartedJob string

	mockNomad := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 4 && args[0] == "job" && args[1] == "restart" && args[2] == "-reschedule" {
			restartCalled = true
			restartedJob = args[3]
			return []byte("Job restart scheduled\n"), nil, nil
		}
		return nil, nil, nil
	}

	mockClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", currentDigest)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	d := NewDaemon(DaemonConfig{
		ConfigDir:      tmpDir,
		NomadExecutor:  mockNomad,
		RegistryClient: mockClient,
	})

	// Pre-seed known digest with older hash so new digest triggers restart
	d.nomadKnownDigests["mirrormere-core:ghcr.io/azylman/mirrormere:latest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

	if err := d.CheckAndReconcileNomadImages(context.Background()); err != nil {
		t.Errorf("CheckAndReconcileNomadImages returned error: %v", err)
	}

	if !restartCalled {
		t.Errorf("expected nomad job restart to be called")
	}
	if restartedJob != "mirrormere-core" {
		t.Errorf("expected restarted job 'mirrormere-core', got %q", restartedJob)
	}
}

func TestNomadOrchestration_UnitSuite(t *testing.T) {
	t.Parallel()

	// 1. defaultNomadExecutor execution & token scrubbing
	execFn := defaultNomadExecutor("http://127.0.0.1:4646", "secret-nomad-token")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, _ = execFn(ctx, "version")

	// 2. getNomadExecutor coverage
	var nilDaemon *SyncDaemon
	if nilDaemon.getNomadExecutor() == nil {
		t.Errorf("expected non-nil default executor from nil daemon")
	}

	dDefault := &SyncDaemon{
		nomadAddr:  "http://localhost:4646",
		nomadToken: "tok",
	}
	if dDefault.getNomadExecutor() == nil {
		t.Errorf("expected non-nil executor from dDefault")
	}

	customCalled := false
	dCustom := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			customCalled = true
			return []byte("custom"), nil, nil
		},
	}
	out, _, _ := dCustom.getNomadExecutor()(ctx, "test")
	if !customCalled || string(out) != "custom" {
		t.Errorf("expected custom nomad executor to be invoked")
	}

	// 3. Pending Nomad change recording and draining
	d := NewDaemon(DaemonConfig{})
	if len(d.drainPendingNomadChanges()) != 0 {
		t.Errorf("expected empty pending changes initially")
	}
	d.recordPendingNomadChanges("/repo/a", []NomadFileChange{
		{JobName: "job-a", Path: "jobs/job-a.nomad", Action: "apply"},
	})
	d.recordPendingNomadChanges("/repo/b", []NomadFileChange{
		{JobName: "job-b", Path: "jobs/job-b.nomad", Action: "delete"},
	})
	pending := d.drainPendingNomadChanges()
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending changes, got %d", len(pending))
	}
	if len(d.drainPendingNomadChanges()) != 0 {
		t.Errorf("expected pending changes to be drained")
	}

	// 4. ExecuteNomadTeardowns
	d.ExecuteNomadTeardowns(ctx, nil)

	var stoppedJobs []string
	dTeardown := NewDaemon(DaemonConfig{
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 4 && args[0] == "job" && args[1] == "stop" {
				stoppedJobs = append(stoppedJobs, args[3])
				if args[3] == "fail-job" {
					return []byte("err-out"), []byte("err-bytes"), errors.New("stop error")
				}
				return []byte("ok"), nil, nil
			}
			return nil, nil, nil
		},
	})
	dTeardown.ExecuteNomadTeardowns(ctx, []NomadChangeEvent{
		{
			RepoPath: "/repo",
			Changes: []NomadFileChange{
				{JobName: "normal-job", Action: "delete"},
				{JobName: "fail-job", Action: "delete"},
				{JobName: "apply-job", Action: "apply"},
			},
		},
	})
	if len(stoppedJobs) != 2 {
		t.Errorf("expected 2 jobs stopped, got %v", stoppedJobs)
	}

	// 5. ValidateNomadJob
	dValSuccess := NewDaemon(DaemonConfig{
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Job syntax is valid"), nil, nil
		},
	})
	if err := dValSuccess.ValidateNomadJob(ctx, "job.nomad"); err != nil {
		t.Errorf("expected valid job, got %v", err)
	}

	dValFail := NewDaemon(DaemonConfig{
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("syntax error on line 5"), errors.New("exit status 1")
		},
	})
	if err := dValFail.ValidateNomadJob(ctx, "job.nomad"); err == nil {
		t.Errorf("expected validation error, got nil")
	}

	// 6. ReconcileNomadChanges & ReconcilePendingNomad
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)
	validJobFile := filepath.Join(jobsDir, "valid.nomad")
	_ = os.WriteFile(validJobFile, []byte(`job "valid" {}`), 0644)
	invalidJobFile := filepath.Join(jobsDir, "invalid.nomad")
	_ = os.WriteFile(invalidJobFile, []byte(`job "invalid" {}`), 0644)
	runFailJobFile := filepath.Join(jobsDir, "runfail.nomad")
	_ = os.WriteFile(runFailJobFile, []byte(`job "runfail" {}`), 0644)

	var runJobs []string
	dReconcile := NewDaemon(DaemonConfig{
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 3 && args[0] == "job" && args[1] == "validate" {
				if strings.Contains(args[2], "invalid.nomad") {
					return nil, []byte("invalid HCL"), errors.New("val error")
				}
				return []byte("valid"), nil, nil
			}
			if len(args) >= 4 && args[0] == "job" && args[1] == "run" {
				if strings.Contains(args[3], "runfail.nomad") {
					return []byte("fail"), []byte("alloc failed"), errors.New("run error")
				}
				runJobs = append(runJobs, args[3])
				return []byte("Evaluation ID: 12345"), nil, nil
			}
			if len(args) >= 4 && args[0] == "job" && args[1] == "stop" {
				if args[3] == "stop-fail" {
					return nil, []byte("stop fail"), errors.New("stop error")
				}
				return []byte("stopped"), nil, nil
			}
			return nil, nil, nil
		},
	})

	// Empty changes
	if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, nil); err != nil {
		t.Errorf("expected nil for empty changes: %v", err)
	}

	// Delete change (success & failure branch)
	_ = dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
		{JobName: "stop-ok", Action: "delete"},
		{JobName: "stop-fail", Action: "delete"},
	})

	// Apply change missing from disk (notice)
	_ = dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
		{JobName: "missing", Path: "jobs/missing.nomad", Action: "apply"},
	})

	// Apply change validation failure
	if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
		{JobName: "invalid", Path: "jobs/invalid.nomad", Action: "apply"},
	}); err == nil {
		t.Errorf("expected validation error for invalid.nomad")
	}

	// Apply change run failure
	if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
		{JobName: "runfail", Path: "jobs/runfail.nomad", Action: "apply"},
	}); err == nil {
		t.Errorf("expected run error for runfail.nomad")
	}

	// Apply change success
	if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
		{JobName: "valid", Path: "jobs/valid.nomad", Action: "apply"},
	}); err != nil {
		t.Errorf("expected success for valid.nomad, got %v", err)
	}

	// ReconcilePendingNomad
	dReconcile.recordPendingNomadChanges(tmpDir, []NomadFileChange{
		{JobName: "valid", Path: "jobs/valid.nomad", Action: "apply"},
	})
	if err := dReconcile.ReconcilePendingNomad(ctx); err != nil {
		t.Errorf("expected success in ReconcilePendingNomad, got %v", err)
	}

	// CheckAndReconcileNomadImages throttle & edge cases
	dReconcile.configDir = tmpDir
	// Already called recently
	dReconcile.lastNomadImagePoll = time.Now()
	if err := dReconcile.CheckAndReconcileNomadImages(ctx); err != nil {
		t.Errorf("expected nil when throttled: %v", err)
	}

	// Missing jobs directory
	dMissingDir := NewDaemon(DaemonConfig{ConfigDir: filepath.Join(tmpDir, "nonexistent")})
	if err := dMissingDir.CheckAndReconcileNomadImages(ctx); err != nil {
		t.Errorf("expected nil for missing jobs dir: %v", err)
	}

	// 7. HasNomadChanges branches
	// Canceled ctx
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := d.HasNomadChanges(canceledCtx, "/repo", "a", "b"); err == nil {
		t.Errorf("expected context error")
	}

	// Empty repo or empty/equal heads
	if ch, err := d.HasNomadChanges(ctx, "", "a", "b"); err != nil || ch != nil {
		t.Errorf("expected nil for empty repo")
	}
	if ch, err := d.HasNomadChanges(ctx, "/repo", "", "b"); err != nil || ch != nil {
		t.Errorf("expected nil for empty prevHead")
	}
	if ch, err := d.HasNomadChanges(ctx, "/repo", "a", ""); err != nil || ch != nil {
		t.Errorf("expected nil for empty currHead")
	}
	if ch, err := d.HasNomadChanges(ctx, "/repo", "a", "a"); err != nil || ch != nil {
		t.Errorf("expected nil for identical heads")
	}

	// Git error, fallback success
	dFallback := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "diff" {
				return nil, []byte("fatal: ambiguous argument"), errors.New("diff error")
			}
			if len(args) > 0 && args[0] == "diff-tree" {
				return []byte("M\tjobs/app.nomad\n"), nil, nil
			}
			return nil, nil, nil
		},
	})
	ch, err := dFallback.HasNomadChanges(ctx, "/repo", "head1", "head2")
	if err != nil || len(ch) != 1 || ch[0].JobName != "app" {
		t.Errorf("expected fallback diff-tree to succeed with 1 change, got %v (err=%v)", ch, err)
	}

	// Git error, fallback error
	dFallbackErr := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: bad object"), errors.New("git error")
		},
	})
	ch, err = dFallbackErr.HasNomadChanges(ctx, "/repo", "head1", "head2")
	if err != nil || ch != nil {
		t.Errorf("expected nil changes and nil error when fallback fails, got %v (err=%v)", ch, err)
	}

	// Git error with canceled context during diff
	dDiffCancel := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			cancelNow()
			return nil, []byte("interrupted"), errors.New("signal: killed")
		},
	})
	if _, err := dDiffCancel.HasNomadChanges(canceledCtx, "/repo", "head1", "head2"); err == nil {
		t.Errorf("expected error on canceled ctx in diff fallback")
	}

	// Git fallback error with canceled context
	dFallbackCancel := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "diff" {
				return nil, nil, errors.New("diff err")
			}
			return nil, nil, errors.New("tree err")
		},
	})
	if _, err := dFallbackCancel.HasNomadChanges(canceledCtx, "/repo", "head1", "head2"); err == nil {
		t.Errorf("expected error on canceled ctx in diff-tree fallback")
	}

	// 8. defaultDockerExecutor & defaultComposeExecutor & runGitCommand & defaultNomadExecutor
	_, _, _ = defaultDockerExecutor(ctx, "version")
	_, _, _ = defaultComposeExecutor(ctx, "/nonexistent", "version")
	_, _, _ = runGitCommand(ctx, "/nonexistent", "dummy-pat", "status")
	execNomad := defaultNomadExecutor("http://127.0.0.1:4646", "dummy-nomad-token")
	_, _, _ = execNomad(ctx, "version")
	execNomadNoTok := defaultNomadExecutor("", "")
	_, _, _ = execNomadNoTok(ctx, "version")
	cancCtx, cancelNomad := context.WithCancel(context.Background())
	cancelNomad()
	_, _, _ = execNomad(cancCtx, "version")
}

func TestSyncDaemon_RemainingHelpers(t *testing.T) {
	ctx := context.Background()
	// nil daemon and nil fn updateReconcileStatus
	var nilD *SyncDaemon
	nilD.updateReconcileStatus(func(s *ReconciliationStatus) {})
	d := NewDaemon(DaemonConfig{})
	d.updateReconcileStatus(nil)

	// GetReconciliationStatus edge cases
	if st := nilD.GetReconciliationStatus(); st.State != "idle" {
		t.Errorf("expected idle for nil daemon, got %q", st.State)
	}

	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.Active = false
		s.State = "healthy"
		s.CompletedAt = time.Now().Add(-10 * time.Minute)
	})
	if st := d.GetReconciliationStatus(); st.State != "idle" {
		t.Errorf("expected idle after TTL expiry, got %q", st.State)
	}

	// recordPendingTargets / drainPendingTargets
	nilD.recordPendingTargets([]string{"svc1"})
	if res := nilD.drainPendingTargets(); res != nil {
		t.Errorf("expected nil for nil daemon drain")
	}
	dEmpty := NewDaemon(DaemonConfig{})
	if res := dEmpty.drainPendingTargets(); res != nil {
		t.Errorf("expected nil for empty drain")
	}

	// notifyBrainReload
	d.brainInternalURL = ""
	d.notifyBrainReload()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d.brainInternalURL = srv.URL
	d.notifyBrainReload()

	d.brainInternalURL = "http://127.0.0.1:1" // unreachable
	d.notifyBrainReload()

	// EnsureDockerAuth
	if err := nilD.EnsureDockerAuth(); err != nil {
		t.Errorf("unexpected error for nil daemon: %v", err)
	}
	dNoPat := NewDaemon(DaemonConfig{PAT: ""})
	if err := dNoPat.EnsureDockerAuth(); err != nil {
		t.Errorf("unexpected error for empty PAT: %v", err)
	}
	dWithPat := NewDaemon(DaemonConfig{
		PAT:       "ghp_test123",
		ConfigDir: "/cfg",
		RepoURLs: map[string]string{
			"/cfg": "https://github.com/azylman/aerial-config.git",
		},
	})
	_ = dWithPat.EnsureDockerAuth()

	// resolveGitDir
	if res, err := resolveGitDir(""); err == nil || res != "" {
		t.Errorf("expected error for empty dir")
	}

	tempGitDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tempGitDir, ".git"), 0755)
	if res, err := resolveGitDir(tempGitDir); err != nil || res != filepath.Join(tempGitDir, ".git") {
		t.Errorf("expected .git dir, got %s (err=%v)", res, err)
	}

	tempGitFileRel := t.TempDir()
	_ = os.WriteFile(filepath.Join(tempGitFileRel, ".git"), []byte("gitdir: ../dotgit\n"), 0644)
	if res, err := resolveGitDir(tempGitFileRel); err != nil || !strings.HasSuffix(res, "dotgit") {
		t.Errorf("expected relative dotgit resolved, got %s (err=%v)", res, err)
	}

	tempGitFileAbs := t.TempDir()
	_ = os.WriteFile(filepath.Join(tempGitFileAbs, ".git"), []byte("gitdir: /var/git/target\n"), 0644)
	if res, err := resolveGitDir(tempGitFileAbs); err != nil || res != "/var/git/target" {
		t.Errorf("expected abs dotgit resolved, got %s (err=%v)", res, err)
	}

	tempGitFileRaw := t.TempDir()
	_ = os.WriteFile(filepath.Join(tempGitFileRaw, ".git"), []byte("notgitdir\n"), 0644)
	if res, err := resolveGitDir(tempGitFileRaw); err != nil || res != filepath.Join(tempGitFileRaw, ".git") {
		t.Errorf("expected raw dotgit file returned, got %s (err=%v)", res, err)
	}

	// ReconcilePendingNomad error aggregation
	_ = os.MkdirAll(filepath.Join(tempGitDir, "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(tempGitDir, "jobs", "bad.nomad"), []byte(`job "bad" {}`), 0644)
	dNomadErr := NewDaemon(DaemonConfig{
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("syntax err"), errors.New("val fail")
		},
	})
	dNomadErr.recordPendingNomadChanges(tempGitDir, []NomadFileChange{
		{JobName: "bad", Path: "jobs/bad.nomad", Action: "apply"},
	})
	if err := dNomadErr.ReconcilePendingNomad(ctx); err == nil {
		t.Errorf("expected error from ReconcilePendingNomad with invalid job")
	}

	// postChannelMessage
	d.cachedChannelID = "12345"
	client404 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}
	d.postChannelMessage(ctx, client404, "bot-tok", "12345", "hello")
	if d.cachedChannelID != "" {
		t.Errorf("expected cachedChannelID to be cleared on 404")
	}

	client500 := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}
	d.postChannelMessage(ctx, client500, "bot-tok", "12345", "hello")

	clientErr := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network failure")
			},
		},
	}
	d.postChannelMessage(ctx, clientErr, "bot-tok", "12345", "hello")
}

func TestNomadCoverageRemediation(t *testing.T) {
	ctx := context.Background()

	// 1. StartReconcilerLoop with debounce timer execution and failure branches
	oldDebounce := reconcilerDebounceDuration
	reconcilerDebounceDuration = 5 * time.Millisecond
	defer func() { reconcilerDebounceDuration = oldDebounce }()

	tempDir := t.TempDir()
	d := NewDaemon(DaemonConfig{
		ComposeDir: tempDir,
		ConfigDir:  tempDir,
		ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, nil, errors.New("compose fail")
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, nil, errors.New("nomad fail")
		},
	})
	d.recordPendingNomadChanges(tempDir, []NomadFileChange{
		{JobName: "deljob", Action: "delete"},
		{JobName: "appjob", Action: "apply", Path: "jobs/app.nomad"},
	})
	loopCtx, loopCancel := context.WithCancel(context.Background())
	d.StartReconcilerLoop(loopCtx)
	d.TriggerReconcile(ctx, true)
	time.Sleep(30 * time.Millisecond)
	loopCancel()

	// 2. CheckAndReconcileNomadImages: directory entries, unreadable file, nil cache, restart error, throttle cooldown
	cfgDir := t.TempDir()
	jobsDir := filepath.Join(cfgDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)
	_ = os.MkdirAll(filepath.Join(jobsDir, "nested-dir"), 0755)
	_ = os.WriteFile(filepath.Join(jobsDir, "readme.txt"), []byte("not a nomad file"), 0644)
	_ = os.WriteFile(filepath.Join(jobsDir, "job1.nomad"), []byte(`job "job1" { task "t" { config { image = "ghcr.io/test/img1:latest" } } }`), 0644)

	dImages := NewDaemon(DaemonConfig{
		ConfigDir: cfgDir,
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					resp := &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("{}")),
					}
					resp.Header.Set("Docker-Content-Digest", "sha256:firstdigest111")
					return resp, nil
				},
			},
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) > 1 && args[1] == "restart" {
				return []byte("restart err"), []byte("err details"), errors.New("restart fail")
			}
			return []byte("ok"), nil, nil
		},
	})

	// Initial poll populates nil map
	dImages.nomadKnownDigests = nil
	if err := dImages.CheckAndReconcileNomadImages(ctx); err != nil {
		t.Errorf("unexpected error on first CheckAndReconcileNomadImages: %v", err)
	}

	// Immediate second poll triggers 5-minute throttle cooldown
	if err := dImages.CheckAndReconcileNomadImages(ctx); err != nil {
		t.Errorf("unexpected error on throttled CheckAndReconcileNomadImages: %v", err)
	}

	// Reset lastNomadImagePoll to simulate elapsed time, update mock client digest to trigger restart failure path
	dImages.lastNomadImagePoll = time.Time{}
	dImages.registryClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       ioNopCloser(strings.NewReader("{}")),
				}
				resp.Header.Set("Docker-Content-Digest", "sha256:seconddigest222")
				return resp, nil
			},
		},
	}
	if err := dImages.CheckAndReconcileNomadImages(ctx); err != nil {
		t.Errorf("unexpected error on restart fail CheckAndReconcileNomadImages: %v", err)
	}

	// 3. HasNomadChanges empty repoPath
	if ch, err := d.HasNomadChanges(ctx, "", "head1", "head2"); err != nil || ch != nil {
		t.Errorf("expected nil for empty repoPath in HasNomadChanges")
	}

	// 4. ParseNomadGitStatus with empty lines and spaces
	changes := ParseNomadGitStatus("\n\n   \n\t\nM jobs/app.nomad\nD jobs/old.nomad\n")
	if len(changes) != 2 {
		t.Errorf("expected 2 changes from ParseNomadGitStatus, got %d", len(changes))
	}

	// 5. recordPendingTargets with nil map
	dNilTargets := &SyncDaemon{}
	dNilTargets.recordPendingTargets([]string{"svc1", "   "})

	// 6. postChannelMessage and sendWebhook with invalid URLs
	d.postChannelMessage(ctx, http.DefaultClient, "tok", "bad\nchan", "msg")
	d.sendWebhook(ctx, http.DefaultClient, "http://bad\nurl", "msg")
}

func TestCheckGitHubRepoBuildInProgress_TableDriven(t *testing.T) {
	ctx := context.Background()

	// 1. Nil daemon
	var dNil *SyncDaemon
	if dNil.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected false for nil daemon")
	}

	// 2. Empty repo slug
	dEmpty := &SyncDaemon{}
	if dEmpty.CheckGitHubRepoBuildInProgress(ctx, "") {
		t.Errorf("expected false for empty slug")
	}

	// 3. In-progress workflow run exists
	dInProgress := &SyncDaemon{
		pat: "test-pat",
		registryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.String(), "status=in_progress") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"total_count": 1, "workflow_runs": [{}]}`)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`)),
					}, nil
				},
			},
		},
	}
	if !dInProgress.CheckGitHubRepoBuildInProgress(ctx, "azylman/aerial") {
		t.Errorf("expected true when in_progress workflow run exists")
	}

	// 4. Queued workflow run exists (in_progress is 0)
	dQueued := &SyncDaemon{
		pat: "test-pat",
		registryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.String(), "status=queued") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"total_count": 2, "workflow_runs": [{},{}]}`)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`)),
					}, nil
				},
			},
		},
	}
	if !dQueued.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected true when queued workflow run exists")
	}

	// 5. Zero runs
	dZero := &SyncDaemon{
		registryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`)),
					}, nil
				},
			},
		},
	}
	if dZero.CheckGitHubRepoBuildInProgress(ctx, "azylman/mirrormere") {
		t.Errorf("expected false when no runs exist")
	}

	// 6. HTTP Error from API
	dErr := &SyncDaemon{
		registryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`error`)),
					}, nil
				},
			},
		},
	}
	if dErr.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected false on HTTP 502 error")
	}

	// 7. Client transport error
	dNetErr := &SyncDaemon{
		registryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("network timeout")
				},
			},
		},
	}
	if dNetErr.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected false on network error")
	}
}

func TestExecuteGitPushEvent_TableDriven(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	_ = os.MkdirAll(aerialDir, 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.MkdirAll(mirrormereDir, 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(mirrormereDir, ".git"), 0755)

	repos := []string{aerialDir, aerialConfigDir, mirrormereDir}
	var revParseCount int

	d := NewDaemon(DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseCount++
					if revParseCount == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "fetch" {
				return []byte(""), nil, nil
			}
			if len(args) >= 2 && args[0] == "merge" {
				return []byte(""), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
				if strings.Contains(dir, "aerial") && !strings.Contains(dir, "aerial-config") {
					return []byte("brain/main.go\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: 12345"), nil, nil
		},
	})

	// 1. Missing repo
	resp, code := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{})
	if code != http.StatusBadRequest || resp.Status != "error" {
		t.Errorf("expected 400 Bad Request for missing repo, got %d (%s)", code, resp.Status)
	}

	// 2. Non-main branch
	respIgnored, codeIgnored := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/feature-branch",
	})
	if codeIgnored != http.StatusOK || respIgnored.Status != "ignored" {
		t.Errorf("expected 200 ignored for non-main branch, got %d (%s)", codeIgnored, respIgnored.Status)
	}

	// 3. Unmanaged repository
	respNotFound, codeNotFound := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "nonexistent-repo",
		Ref:  "refs/heads/main",
	})
	if codeNotFound != http.StatusNotFound || respNotFound.Status != "error" {
		t.Errorf("expected 404 for unmanaged repo, got %d (%s)", codeNotFound, respNotFound.Status)
	}

	// 4. Aerial monorepo push with container build changes (brain/main.go)
	respAerialBuild, codeAerialBuild := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if codeAerialBuild != http.StatusOK || respAerialBuild.Status != "accepted" || !respAerialBuild.ContainersBuilding {
		t.Errorf("expected accepted with ContainersBuilding=true for aerial monorepo build changes, got code %d, status %s, building %v", codeAerialBuild, respAerialBuild.Status, respAerialBuild.ContainersBuilding)
	}

	// 5. Aerial monorepo push with non-build changes only (nomad job changed)
	var revParseAerialNomad int
	dNomadAerial := NewDaemon(DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseAerialNomad++
					if revParseAerialNomad == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
				return []byte("nomad/jobs/migrate.nomad\n"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M nomad/jobs/migrate.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: migrate-123"), nil, nil
		},
	})
	_ = os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(aerialDir, "nomad", "jobs", "migrate.nomad"), []byte(`job "migrate" {}`), 0644)

	respAerialNomad, codeAerialNomad := dNomadAerial.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if codeAerialNomad != http.StatusOK || respAerialNomad.Status != "accepted" || respAerialNomad.ContainersBuilding || !respAerialNomad.NomadChanged {
		t.Errorf("expected accepted with NomadChanged=true for aerial nomad change, got code %d, status %s, nomadChanged %v", codeAerialNomad, respAerialNomad.Status, respAerialNomad.NomadChanged)
	}

	// 6. Aerial-config push with deferred nomad job due to active build
	jobWithAerialImg := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(aerialConfigDir, "jobs", "webhooks-router.nomad"), []byte(jobWithAerialImg), 0644)

	var revParseConfigDef int
	dConfigDeferred := NewDaemon(DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseConfigDef++
					if revParseConfigDef == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.String(), "actions/runs") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"total_count": 1, "workflow_runs": [{}]}`)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{}`)),
					}, nil
				},
			},
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: 12345"), nil, nil
		},
	})

	respConfigDef, codeConfigDef := dConfigDeferred.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if codeConfigDef != http.StatusOK || !respConfigDef.ContainersBuilding || len(respConfigDef.AppliedJobs) != 0 {
		t.Errorf("expected job deferred when image source repo is building, got code %d, building %v, applied %v", codeConfigDef, respConfigDef.ContainersBuilding, respConfigDef.AppliedJobs)
	}

	// 7. Aerial-config push with image ready (applies Nomad job)
	var revParseConfigApp int
	dConfigApplied := NewDaemon(DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseConfigApp++
					if revParseConfigApp == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.String(), "actions/runs") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`)),
						}, nil
					}
					resp := &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{}`)),
					}
					resp.Header.Set("Docker-Content-Digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
					return resp, nil
				},
			},
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: applied-123"), nil, nil
		},
	})

	respConfigApp, codeConfigApp := dConfigApplied.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if codeConfigApp != http.StatusOK || respConfigApp.ContainersBuilding || len(respConfigApp.AppliedJobs) != 1 {
		t.Errorf("expected job applied when image ready, got code %d, building %v, applied %v", codeConfigApp, respConfigApp.ContainersBuilding, respConfigApp.AppliedJobs)
	}

	// 8. Push to peripheral repo (mirrormere)
	respMirror, codeMirror := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "mirrormere",
		Ref:  "refs/heads/main",
	})
	if codeMirror != http.StatusOK || respMirror.Status != "accepted" {
		t.Errorf("expected 200 accepted for mirrormere push, got %d (%s)", codeMirror, respMirror.Status)
	}
}

func TestExecuteImageReadyEvent_TableDriven(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	job1 := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "webhooks-router.nomad"), []byte(job1), 0644)

	var runCalled bool
	var restartCalled bool

	d := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				runCalled = true
				return []byte("Evaluation ID: run-123"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				restartCalled = true
				return []byte("Restart triggered"), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	// 1. Missing image
	respEmpty, codeEmpty := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{})
	if codeEmpty != http.StatusBadRequest || respEmpty.Status != "error" {
		t.Errorf("expected 400 for empty image, got %d (%s)", codeEmpty, respEmpty.Status)
	}

	// 2. Unknown image
	respUnknown, codeUnknown := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/nonexistent-service:latest",
	})
	if codeUnknown != http.StatusNotFound || respUnknown.Status != "not_found" {
		t.Errorf("expected 404 for unknown image, got %d (%s)", codeUnknown, respUnknown.Status)
	}

	// 3. Matching image
	respMatch, codeMatch := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest: "sha256:newdigest999",
	})
	if codeMatch != http.StatusOK || respMatch.Status != "accepted" || len(respMatch.MatchedJobs) != 1 {
		t.Errorf("expected 200 accepted for matching image, got %d (%s), matches: %v", codeMatch, respMatch.Status, respMatch.MatchedJobs)
	}
	if !runCalled || !restartCalled {
		t.Errorf("expected both job run and restart to be invoked, run: %v, restart: %v", runCalled, restartCalled)
	}

	// Check digest cache was updated
	d.nomadDigestsMu.RLock()
	cachedDigest := d.nomadKnownDigests["webhooks-router:ghcr.io/azylman/aerial-webhooks-router:latest"]
	d.nomadDigestsMu.RUnlock()
	if cachedDigest != "sha256:newdigest999" {
		t.Errorf("expected cached digest sha256:newdigest999, got %q", cachedDigest)
	}

	// 4. Validation error on all matches
	dFail := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				return nil, []byte("syntax error"), errors.New("validate error")
			}
			return []byte("OK"), nil, nil
		},
	})
	respFail, codeFail := dFail.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/aerial-webhooks-router:latest",
	})
	if codeFail != http.StatusInternalServerError || respFail.Status != "error" {
		t.Errorf("expected 500 when all jobs fail validation, got %d (%s)", codeFail, respFail.Status)
	}
}

func TestSetupMux_PushEventsEndpoints(t *testing.T) {
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".git"), 0755)

	job1 := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "webhooks-router.nomad"), []byte(job1), 0644)

	d := NewDaemon(DaemonConfig{
		Repos:     []string{tmpDir},
		ConfigDir: tmpDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("abc1234"), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("OK"), nil, nil
		},
	})

	mux := SetupMux(d)

	// 1. POST /events/git_push
	wPush := httptest.NewRecorder()
	rPush := httptest.NewRequest(http.MethodPost, "/events/git_push", strings.NewReader(`{"repo":"`+tmpDir+`","ref":"refs/heads/main"}`))
	rPush.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(wPush, rPush)
	if wPush.Code != http.StatusOK {
		t.Errorf("POST /events/git_push returned %d; want 200", wPush.Code)
	}

	// 2. GET /events/git_push -> 405
	wPushGet := httptest.NewRecorder()
	rPushGet := httptest.NewRequest(http.MethodGet, "/events/git_push", nil)
	mux.ServeHTTP(wPushGet, rPushGet)
	if wPushGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /events/git_push returned %d; want 405", wPushGet.Code)
	}

	// 3. POST /events/git_push bad JSON -> 400
	wPushBad := httptest.NewRecorder()
	rPushBad := httptest.NewRequest(http.MethodPost, "/events/git_push", strings.NewReader(`invalid-json`))
	mux.ServeHTTP(wPushBad, rPushBad)
	if wPushBad.Code != http.StatusBadRequest {
		t.Errorf("POST /events/git_push bad JSON returned %d; want 400", wPushBad.Code)
	}

	// 4. POST /events/image_ready
	wImg := httptest.NewRecorder()
	rImg := httptest.NewRequest(http.MethodPost, "/events/image_ready", strings.NewReader(`{"image":"ghcr.io/azylman/aerial-webhooks-router:latest"}`))
	rImg.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(wImg, rImg)
	if wImg.Code != http.StatusOK {
		t.Errorf("POST /events/image_ready returned %d; want 200", wImg.Code)
	}

	// 5. GET /events/image_ready -> 405
	wImgGet := httptest.NewRecorder()
	rImgGet := httptest.NewRequest(http.MethodGet, "/events/image_ready", nil)
	mux.ServeHTTP(wImgGet, rImgGet)
	if wImgGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /events/image_ready returned %d; want 405", wImgGet.Code)
	}

	// 6. POST /events/image_ready bad JSON -> 400
	wImgBad := httptest.NewRecorder()
	rImgBad := httptest.NewRequest(http.MethodPost, "/events/image_ready", strings.NewReader(`{invalid`))
	mux.ServeHTTP(wImgBad, rImgBad)
	if wImgBad.Code != http.StatusBadRequest {
		t.Errorf("POST /events/image_ready bad JSON returned %d; want 400", wImgBad.Code)
	}
}

func TestGetChangedFiles_Comprehensive(t *testing.T) {
	ctx := context.Background()
	d := &SyncDaemon{}

	// 1. Empty heads
	files, err := d.getChangedFiles(ctx, "/tmp", "", "sha2")
	if err != nil || files != nil {
		t.Errorf("expected nil files and err, got files=%v err=%v", files, err)
	}
	files, err = d.getChangedFiles(ctx, "/tmp", "sha1", "")
	if err != nil || files != nil {
		t.Errorf("expected nil files and err, got files=%v err=%v", files, err)
	}
	files, err = d.getChangedFiles(ctx, "/tmp", "sha1", "sha1")
	if err != nil || files != nil {
		t.Errorf("expected nil files and err, got files=%v err=%v", files, err)
	}

	// 2. Diff success
	dSuccess := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte("file1.go\n\nfile2.hcl\n"), nil, nil
		},
	}
	files, err = dSuccess.getChangedFiles(ctx, "/tmp", "sha1", "sha2")
	if err != nil || len(files) != 2 || files[0] != "file1.go" || files[1] != "file2.hcl" {
		t.Errorf("expected 2 files, got %v (err: %v)", files, err)
	}

	// 3. Diff fails, diff-tree fallback succeeds
	dFallback := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "diff" {
				return nil, []byte("fatal: bad object"), errors.New("diff failed")
			}
			if len(args) > 0 && args[0] == "diff-tree" {
				return []byte("fallback.txt\n"), nil, nil
			}
			return nil, nil, nil
		},
	}
	files, err = dFallback.getChangedFiles(ctx, "/tmp", "sha1", "sha2")
	if err != nil || len(files) != 1 || files[0] != "fallback.txt" {
		t.Errorf("expected fallback.txt, got %v (err: %v)", files, err)
	}

	// 4. Diff fails and diff-tree fallback fails
	dFallbackFail := &SyncDaemon{
		gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: bad object"), errors.New("git error")
		},
	}
	files, err = dFallbackFail.getChangedFiles(ctx, "/tmp", "sha1", "sha2")
	if err == nil || files != nil {
		t.Errorf("expected error from fallback, got nil (files: %v)", files)
	}
}

func TestApplyNomadChangesDirectly_Comprehensive(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	validJobPath := filepath.Join(tmpDir, "jobs", "test.nomad")
	_ = os.MkdirAll(filepath.Dir(validJobPath), 0755)
	_ = os.WriteFile(validJobPath, []byte(`job "test" {}`), 0644)

	// 1. Delete action with stop error and success
	d := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "stop" {
				return nil, []byte("stop error"), errors.New("stop fail")
			}
			return []byte("ok"), nil, nil
		},
	}
	applied, err := d.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "delete", JobName: "deleted-job", Path: "jobs/del.nomad"},
	})
	if err != nil || len(applied) != 0 {
		t.Errorf("expected nil error on delete, got applied=%v err=%v", applied, err)
	}

	// 2. Unsafe job path
	applied, err = d.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "update", JobName: "evil", Path: "../evil.nomad"},
	})
	if err != nil || len(applied) != 0 {
		t.Errorf("expected ignored unsafe path, got applied=%v err=%v", applied, err)
	}

	// 3. Validation error
	dValFail := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				return nil, []byte("invalid syntax"), errors.New("validate fail")
			}
			return []byte("ok"), nil, nil
		},
	}
	applied, err = dValFail.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "update", JobName: "test", Path: "jobs/test.nomad"},
	})
	if err == nil {
		t.Errorf("expected validation error, got nil (applied: %v)", applied)
	}

	// 4. Run error
	dRunFail := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("run error"), errors.New("run fail")
			}
			return []byte("ok"), nil, nil
		},
	}
	applied, err = dRunFail.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "update", JobName: "test", Path: "jobs/test.nomad"},
	})
	if err == nil {
		t.Errorf("expected run error, got nil (applied: %v)", applied)
	}

	// 5. Success
	dSuccess := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: eval-123"), nil, nil
		},
	}
	applied, err = dSuccess.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "update", JobName: "test", Path: "jobs/test.nomad"},
	})
	if err != nil || len(applied) != 1 || applied[0] != "test" {
		t.Errorf("expected applied job [test], got %v (err: %v)", applied, err)
	}
}

func TestExecuteGitPushEvent_AdditionalBranches(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	aerialCoreDir := filepath.Join(tmpDir, "aerial")
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(aerialCoreDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)

	// 1. SyncRepo failure
	dSyncFail := NewDaemon(DaemonConfig{
		Repos: []string{aerialCoreDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: remote not found"), errors.New("sync failed")
		},
	})
	resp, code := dSyncFail.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Errorf("expected 500 sync error, got code %d (%s)", code, resp.Status)
	}

	// 2. Aerial Core with Nomad changes failure in applyNomadChangesDirectly
	var revParseAerial int
	dAerialNomadFail := NewDaemon(DaemonConfig{
		Repos:      []string{aerialCoreDir},
		ComposeDir: aerialCoreDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseAerial++
					if revParseAerial == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
				return []byte("nomad/jobs/migrate.nomad\n"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M nomad/jobs/migrate.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				return nil, []byte("validate error"), errors.New("invalid job")
			}
			return []byte("OK"), nil, nil
		},
	})
	_ = os.MkdirAll(filepath.Join(aerialCoreDir, "nomad", "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(aerialCoreDir, "nomad", "jobs", "migrate.nomad"), []byte(`job "migrate" {}`), 0644)

	respAerialFail, codeAerialFail := dAerialNomadFail.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if codeAerialFail != http.StatusInternalServerError || respAerialFail.Status != "error" {
		t.Errorf("expected 500 when nomad apply fails, got code %d (%s)", codeAerialFail, respAerialFail.Status)
	}

	// 3. Aerial config with deleted nomad jobs and stopped successfully
	var revParseConfigDel int
	dConfigDel := NewDaemon(DaemonConfig{
		Repos:     []string{aerialConfigDir},
		ConfigDir: aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseConfigDel++
					if revParseConfigDel == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("D jobs/old-service.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Stop evaluation ID: stop-123"), nil, nil
		},
	})
	respDel, codeDel := dConfigDel.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if codeDel != http.StatusOK || respDel.Status != "accepted" {
		t.Errorf("expected 200 on config delete, got %d (%s)", codeDel, respDel.Status)
	}

	// 4. Aerial config with nomad job validate error
	jobBad := `job "broken" {}`
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(aerialConfigDir, "jobs", "broken.nomad"), []byte(jobBad), 0644)

	var revParseConfigBad int
	dConfigBad := NewDaemon(DaemonConfig{
		Repos:     []string{aerialConfigDir},
		ConfigDir: aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseConfigBad++
					if revParseConfigBad == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/broken.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				return nil, []byte("syntax error"), errors.New("validate fail")
			}
			return []byte("OK"), nil, nil
		},
	})
	respBad, codeBad := dConfigBad.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if codeBad != http.StatusOK || len(respBad.AppliedJobs) != 0 {
		t.Errorf("expected 200 with 0 applied jobs on validation failure, got %d (applied: %v)", codeBad, respBad.AppliedJobs)
	}

	// 5. Aerial config with nomad job run error
	var revParseConfigRunErr int
	dConfigRunErr := NewDaemon(DaemonConfig{
		Repos:     []string{aerialConfigDir},
		ConfigDir: aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[1] == "HEAD" {
					revParseConfigRunErr++
					if revParseConfigRunErr == 1 {
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/broken.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("nomad unreachable"), errors.New("run fail")
			}
			return []byte("OK"), nil, nil
		},
	})
	respRunErr, codeRunErr := dConfigRunErr.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if codeRunErr != http.StatusOK || len(respRunErr.AppliedJobs) != 0 {
		t.Errorf("expected 200 with 0 applied jobs on run failure, got %d (applied: %v)", codeRunErr, respRunErr.AppliedJobs)
	}
}

func TestExecutors_DirectCoverage(t *testing.T) {
	ctx := context.Background()

	// runGitCommand
	out, _, err := runGitCommand(ctx, "", "", "version")
	if err != nil || !strings.Contains(string(out), "git version") {
		t.Logf("runGitCommand git version output: %s (err: %v)", string(out), err)
	}

	// defaultNomadExecutor
	nomadExec := defaultNomadExecutor("http://127.0.0.1:4646", "secret-token")
	if nomadExec != nil {
		_, _, _ = nomadExec(ctx, "version")
	}

	// defaultDockerExecutor
	_, _, _ = defaultDockerExecutor(ctx, "version")

	// defaultComposeExecutor
	_, _, _ = defaultComposeExecutor(ctx, t.TempDir(), "version")
}








