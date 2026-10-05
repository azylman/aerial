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
	triggered := make(chan struct{}, 1)
	daemon := &SyncDaemon{
		interval: 5 * time.Millisecond,
		repos:    []string{},
		triggerFn: func() ([]RepoSyncResult, error) {
			select {
			case triggered <- struct{}{}:
			default:
			}
			return nil, errors.New("simulated periodic sync notice")
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	daemon.StartPeriodicLoop(ctx)
	select {
	case <-triggered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for periodic loop execution")
	}
	cancel()
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
	if res.PreviousHead != res.CurrentHead {
		t.Errorf("expected PreviousHead == CurrentHead, got %s != %s", res.PreviousHead, res.CurrentHead)
	}

	// 5. SyncRepo test when remote changes exist
	simulatedFetchHead = "2222222222222222222222222222222222222222"
	res2 := daemon.SyncRepo(ctx, localClone)
	if res2.Error != "" {
		t.Fatalf("SyncRepo failed on update: %s", res2.Error)
	}
	if !res2.Changed {
		t.Errorf("expected changed=true after remote update")
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
	if cfg.Port != "8087" {
		t.Errorf("expected default port 8087, got %q", cfg.Port)
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
	if cfg.BrainInternalURL != "http://brain:8088/internal/reload" {
		t.Errorf("expected default brainInternalURL 'http://brain:8088/internal/reload', got %q", cfg.BrainInternalURL)
	}
	if len(cfg.ExtraSecrets) != 0 {
		t.Errorf("expected 0 extraSecrets with empty lookup, got %v", cfg.ExtraSecrets)
	}

	// Nil lookup safely behaves the same as empty lookup
	cfgNil := NewConfigFromLookup(nil)
	if cfgNil.Port != "8087" {
		t.Errorf("expected default port 8087 with nil lookup, got %q", cfgNil.Port)
	}
}

func TestNewConfigFromLookup_YAML(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "hangar.yaml")
	yamlData := `
port: "8087"
sync_interval: "45s"
sync_repos:
  - "/repo/a"
  - "/repo/b"
nomad_addr: "http://nomad.cluster:4646"
brain_internal_url: "http://brain.cluster:8088/internal/reload"
service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/orin-voice"
    var_key: "CONFIG_YAML"
`
	if err := os.WriteFile(cfgPath, []byte(yamlData), 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	// 1. Load from YAML via CONFIG_PATH
	cfg := NewConfigFromLookup(func(k string) string {
		if k == "CONFIG_PATH" {
			return cfgPath
		}
		return ""
	})

	if cfg.Port != "8087" {
		t.Errorf("expected port 8087, got %q", cfg.Port)
	}
	if cfg.Interval != 45*time.Second {
		t.Errorf("expected interval 45s, got %v", cfg.Interval)
	}
	if len(cfg.Repos) != 2 || cfg.Repos[0] != "/repo/a" || cfg.Repos[1] != "/repo/b" {
		t.Errorf("unexpected repos from yaml: %v", cfg.Repos)
	}
	if cfg.NomadAddr != "http://nomad.cluster:4646" {
		t.Errorf("expected nomadAddr 'http://nomad.cluster:4646', got %q", cfg.NomadAddr)
	}
	if cfg.BrainInternalURL != "http://brain.cluster:8088/internal/reload" {
		t.Errorf("expected brainInternalURL 'http://brain.cluster:8088/internal/reload', got %q", cfg.BrainInternalURL)
	}
	if len(cfg.ServiceConfigs) != 1 || cfg.ServiceConfigs[0].RelPath != "services/voice/voice.yaml" {
		t.Errorf("expected 1 serviceConfig from yaml, got %v", cfg.ServiceConfigs)
	}

	// 2. Env overrides take precedence over YAML
	cfgOver := NewConfigFromLookup(func(k string) string {
		switch k {
		case "CONFIG_PATH":
			return cfgPath
		case "PORT":
			return "9999"
		case "SYNC_INTERVAL":
			return "10s"
		case "SYNC_REPOS":
			return "/override/repo"
		case "NOMAD_ADDR":
			return "http://override.nomad:4646"
		case "BRAIN_INTERNAL_URL":
			return "http://override.brain:8088/reload"
		default:
			return ""
		}
	})

	if cfgOver.Port != "9999" {
		t.Errorf("expected port override 9999, got %q", cfgOver.Port)
	}
	if cfgOver.Interval != 10*time.Second {
		t.Errorf("expected interval override 10s, got %v", cfgOver.Interval)
	}
	if len(cfgOver.Repos) != 1 || cfgOver.Repos[0] != "/override/repo" {
		t.Errorf("expected repos override, got %v", cfgOver.Repos)
	}
	if cfgOver.NomadAddr != "http://override.nomad:4646" {
		t.Errorf("expected nomadAddr override, got %q", cfgOver.NomadAddr)
	}
	if cfgOver.BrainInternalURL != "http://override.brain:8088/reload" {
		t.Errorf("expected brainInternalURL override, got %q", cfgOver.BrainInternalURL)
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

	if !strings.Contains(content, "🚨 **GitSync GitOps Alert: Test Rollback**") {
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

func TestSyncBrainConfigToNomad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Missing config.yaml -> nil (safe no-op)
	emptyDir := t.TempDir()
	d := &SyncDaemon{nomadAddr: "http://127.0.0.1:4646"}
	if err := d.SyncBrainConfigToNomad(ctx, emptyDir); err != nil {
		t.Fatalf("expected nil for missing config.yaml, got %v", err)
	}

	// 2. Unreadable config (directory instead of file) -> error
	badDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(badDir, "config.yaml"), 0755); err != nil {
		t.Fatalf("failed creating subfolder: %v", err)
	}
	if err := d.SyncBrainConfigToNomad(ctx, badDir); err == nil {
		t.Fatalf("expected error reading directory as config.yaml, got nil")
	}

	// 3. Invalid YAML syntax -> error
	invDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(invDir, "config.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
	if err := d.SyncBrainConfigToNomad(ctx, invDir); err == nil || !strings.Contains(err.Error(), "invalid YAML syntax") {
		t.Fatalf("expected invalid YAML syntax error, got %v", err)
	}

	// 4. Semantic error: channels missing default -> error
	semDir := t.TempDir()
	semYAML := `
channels:
  other:
    mode: "threads"
`
	_ = os.WriteFile(filepath.Join(semDir, "config.yaml"), []byte(semYAML), 0644)
	if err := d.SyncBrainConfigToNomad(ctx, semDir); err == nil || !strings.Contains(err.Error(), "channels.default is required") {
		t.Fatalf("expected semantic validation error, got %v", err)
	}

	// 5. Valid config but Nomad executor / addr disabled -> nil
	validYAML := `
model: "gemini-2.5-flash"
channels:
  default:
    mode: "threads"
`
	valDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(valDir, "config.yaml"), []byte(validYAML), 0644)
	dNoNomad := &SyncDaemon{nomadAddr: ""}
	if err := dNoNomad.SyncBrainConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected nil for disabled nomadAddr, got %v", err)
	}

	// 6. Successful push to Nomad with hash caching
	var capturedArgs []string
	brainCalls := 0
	dSuccess := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			brainCalls++
			capturedArgs = args
			return []byte("var updated"), nil, nil
		},
	}
	if err := dSuccess.SyncBrainConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success pushing to nomad, got %v", err)
	}
	if len(capturedArgs) < 5 || capturedArgs[0] != "var" || capturedArgs[1] != "put" || capturedArgs[2] != "-force" || capturedArgs[3] != "nomad/jobs/brain" {
		t.Errorf("unexpected nomad CLI args: %v", capturedArgs)
	}
	if brainCalls != 1 {
		t.Fatalf("expected 1 call to nomadExecutor, got %d", brainCalls)
	}

	// 6b. Second call with unchanged config -> cache hit, skips Nomad CLI call
	if err := dSuccess.SyncBrainConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success on cached call, got %v", err)
	}
	if brainCalls != 1 {
		t.Errorf("expected brainCalls to remain 1, got %d", brainCalls)
	}

	// 6c. Modified config -> cache invalidated, pushes to Nomad
	_ = os.WriteFile(filepath.Join(valDir, "config.yaml"), []byte("channels:\n  default:\n    mode: classify\n"), 0644)
	if err := dSuccess.SyncBrainConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success after config update, got %v", err)
	}
	if brainCalls != 2 {
		t.Errorf("expected brainCalls to be 2 after modification, got %d", brainCalls)
	}

	// 7. Nomad CLI failure -> error
	dFail := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
		},
	}
	if err := dFail.SyncBrainConfigToNomad(ctx, valDir); err == nil || !strings.Contains(err.Error(), "failed updating Nomad variable") {
		t.Fatalf("expected Nomad update error, got %v", err)
	}
}

func TestSyncHomepageConfigToNomad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Missing homepage.yaml and missing homepage directory -> safe success / push empty hash
	emptyDir := t.TempDir()
	var emptyArgs []string
	dEmpty := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			emptyArgs = args
			return []byte("ok"), nil, nil
		},
	}
	if err := dEmpty.SyncHomepageConfigToNomad(ctx, emptyDir); err != nil {
		t.Fatalf("expected nil for empty configDir, got %v", err)
	}
	if len(emptyArgs) < 5 || emptyArgs[2] != "-force" || emptyArgs[3] != "nomad/jobs/homepage" {
		t.Errorf("expected nomad/jobs/homepage target, got %v", emptyArgs)
	}

	// 2. Unreadable config (directory instead of file) -> error
	badDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(badDir, "services", "homepage", "homepage.yaml"), 0755); err != nil {
		t.Fatalf("failed creating bad path: %v", err)
	}
	d := &SyncDaemon{nomadAddr: "http://127.0.0.1:4646"}
	if err := d.SyncHomepageConfigToNomad(ctx, badDir); err == nil {
		t.Fatalf("expected error reading directory as homepage.yaml, got nil")
	}

	// 3. Invalid YAML syntax in homepage.yaml -> error
	invDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(invDir, "services", "homepage"), 0755)
	_ = os.WriteFile(filepath.Join(invDir, "services", "homepage", "homepage.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
	if err := d.SyncHomepageConfigToNomad(ctx, invDir); err == nil || !strings.Contains(err.Error(), "invalid YAML syntax") {
		t.Fatalf("expected invalid YAML syntax error, got %v", err)
	}

	// 4. Valid homepage.yaml and user homepage files -> successful push with deterministic hash
	valDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(valDir, "services", "homepage"), 0755)
	_ = os.WriteFile(filepath.Join(valDir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\nallowed_hosts: \"*\"\n"), 0644)
	_ = os.MkdirAll(filepath.Join(valDir, "homepage"), 0755)
	_ = os.WriteFile(filepath.Join(valDir, "homepage", "services.yaml"), []byte("- My Group: []\n"), 0644)
	_ = os.WriteFile(filepath.Join(valDir, "homepage", "widgets.yaml"), []byte("- search: {}\n"), 0644)
	// Add .hidden and .tmp files that must be ignored
	_ = os.WriteFile(filepath.Join(valDir, "homepage", ".hidden"), []byte("ignore me"), 0644)
	_ = os.WriteFile(filepath.Join(valDir, "homepage", "temp.tmp"), []byte("ignore me too"), 0644)

	var capturedArgs []string
	homepageCalls := 0
	dSuccess := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			homepageCalls++
			capturedArgs = args
			return []byte("var updated"), nil, nil
		},
	}
	if err := dSuccess.SyncHomepageConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success pushing to nomad, got %v", err)
	}
	if homepageCalls != 1 {
		t.Fatalf("expected 1 homepage call, got %d", homepageCalls)
	}

	// 4b. Second call with unchanged config -> cache hit, skips Nomad CLI call
	if err := dSuccess.SyncHomepageConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success on cached call, got %v", err)
	}
	if homepageCalls != 1 {
		t.Errorf("expected homepageCalls to remain 1, got %d", homepageCalls)
	}

	// 4c. Modified homepage config -> cache invalidated, pushes to Nomad
	_ = os.WriteFile(filepath.Join(valDir, "services", "homepage", "homepage.yaml"), []byte("title: Updated Aerial Dashboard\n"), 0644)
	if err := dSuccess.SyncHomepageConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success after homepage update, got %v", err)
	}
	if homepageCalls != 2 {
		t.Errorf("expected homepageCalls to be 2 after modification, got %d", homepageCalls)
	}
	if len(capturedArgs) < 6 || capturedArgs[2] != "-force" || capturedArgs[3] != "nomad/jobs/homepage" {
		t.Fatalf("unexpected nomad CLI args: %v", capturedArgs)
	}
	foundHash := false
	foundYAML := false
	for _, arg := range capturedArgs {
		if strings.HasPrefix(arg, "CONFIG_HASH=") {
			foundHash = true
			hashVal := strings.TrimPrefix(arg, "CONFIG_HASH=")
			if len(hashVal) != 64 {
				t.Errorf("expected 64-char sha256 hex hash, got %s", hashVal)
			}
		}
		if strings.HasPrefix(arg, "HOMEPAGE_YAML=@") {
			foundYAML = true
		}
	}
	if !foundHash || !foundYAML {
		t.Errorf("missing CONFIG_HASH or HOMEPAGE_YAML in args: %v", capturedArgs)
	}

	// 5. Disabled Nomad executor -> safe no-op
	dNoNomad := &SyncDaemon{nomadAddr: ""}
	if err := dNoNomad.SyncHomepageConfigToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected nil for disabled nomadAddr, got %v", err)
	}

	// 6. Nomad CLI failure -> error
	dFail := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
		},
	}
	if err := dFail.SyncHomepageConfigToNomad(ctx, valDir); err == nil || !strings.Contains(err.Error(), "failed updating Nomad variable") {
		t.Fatalf("expected Nomad update error, got %v", err)
	}
}

func TestSyncServiceConfigsToNomad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Missing service configs -> skipped gracefully, returns nil
	emptyDir := t.TempDir()
	dEmpty := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			t.Fatalf("unexpected call to nomadExecutor on empty dir: %v", args)
			return nil, nil, nil
		},
	}
	if err := dEmpty.SyncServiceConfigsToNomad(ctx, emptyDir); err != nil {
		t.Fatalf("expected nil for empty configDir, got %v", err)
	}

	// 2. Unreadable file (directory instead of file) -> error
	badDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(badDir, "services", "mcp", "scheduler-mcp.yaml"), 0755); err != nil {
		t.Fatalf("failed creating bad path: %v", err)
	}
	dBad := &SyncDaemon{nomadAddr: "http://127.0.0.1:4646"}
	if err := dBad.SyncServiceConfigsToNomad(ctx, badDir); err == nil || !strings.Contains(err.Error(), "failed to read service config") {
		t.Fatalf("expected error reading directory as file, got %v", err)
	}

	// 3. Invalid YAML syntax in a service config -> error
	invDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(invDir, "services", "mcp"), 0755)
	_ = os.WriteFile(filepath.Join(invDir, "services", "mcp", "scheduler-mcp.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
	dInv := &SyncDaemon{nomadAddr: "http://127.0.0.1:4646"}
	if err := dInv.SyncServiceConfigsToNomad(ctx, invDir); err == nil || !strings.Contains(err.Error(), "invalid YAML syntax") {
		t.Fatalf("expected invalid YAML syntax error, got %v", err)
	}

	// 4. Valid service configs -> pushes to all Nomad variables
	valDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(valDir, "services", "mcp"), 0755)
	_ = os.MkdirAll(filepath.Join(valDir, "services", "webhooks-router"), 0755)
	_ = os.WriteFile(filepath.Join(valDir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: \"4005\"\n"), 0644)
	_ = os.WriteFile(filepath.Join(valDir, "services", "mcp", "docker-mcp.yaml"), []byte("port: \"4002\"\n"), 0644)
	_ = os.WriteFile(filepath.Join(valDir, "services", "webhooks-router", "webhooks-router.yaml"), []byte("port: \"4020\"\n"), 0644)

	var calls [][]string
	dSuccess := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			calls = append(calls, args)
			return []byte("var updated"), nil, nil
		},
	}
	if err := dSuccess.SyncServiceConfigsToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("expected 3 nomad var put calls, got %d: %v", len(calls), calls)
	}

	// 4b. Second call with unchanged configs -> cache hit, skips all 3
	if err := dSuccess.SyncServiceConfigsToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success on cached call, got %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("expected calls to remain 3, got %d", len(calls))
	}

	// 4c. Modify 1 service config -> only that 1 gets pushed
	_ = os.WriteFile(filepath.Join(valDir, "services", "mcp", "docker-mcp.yaml"), []byte("docker_host: unix:///var/run/docker.sock\n# modified\n"), 0644)
	if err := dSuccess.SyncServiceConfigsToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected success on modified service call, got %v", err)
	}
	if len(calls) != 4 {
		t.Fatalf("expected calls to increase to 4, got %d", len(calls))
	}

	// 5. Disabled Nomad executor -> safe no-op
	dNoNomad := &SyncDaemon{nomadAddr: ""}
	if err := dNoNomad.SyncServiceConfigsToNomad(ctx, valDir); err != nil {
		t.Fatalf("expected nil for disabled nomadAddr, got %v", err)
	}

	// 6. Nomad CLI failure -> error
	dFail := &SyncDaemon{
		nomadAddr: "http://127.0.0.1:4646",
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
		},
	}
	if err := dFail.SyncServiceConfigsToNomad(ctx, valDir); err == nil || !strings.Contains(err.Error(), "failed updating Nomad variable") {
		t.Fatalf("expected Nomad update error, got %v", err)
	}

	// 5. Verify core default service configs stay generic (no user/deployment specific jobs in core)
	for _, m := range DefaultManagedServiceConfigs {
		if strings.Contains(m.NomadVar, "orin-voice") || strings.Contains(m.NomadVar, "mirrormere") {
			t.Errorf("expected DefaultManagedServiceConfigs to remain generic, found deployment-specific mapping: %v", m)
		}
	}

	// 6. Verify GetServiceConfigMappings dynamically reads service_configs from hangar.yaml in configDir
	customDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(customDir, "services", "hangar"), 0755)
	hangarYAML := `
service_configs:
  - rel_path: "services/custom/custom.yaml"
    nomad_var: "nomad/jobs/custom-job"
    var_key: "CONFIG_YAML"
`
	_ = os.WriteFile(filepath.Join(customDir, "services", "hangar", "hangar.yaml"), []byte(hangarYAML), 0644)
	customMappings := dSuccess.GetServiceConfigMappings(customDir)
	if len(customMappings) != 1 || customMappings[0].RelPath != "services/custom/custom.yaml" || customMappings[0].NomadVar != "nomad/jobs/custom-job" {
		t.Fatalf("expected custom mappings from hangar.yaml, got %v", customMappings)
	}

	// 7. Verify GetServiceConfigMappings falls back to daemon-configured serviceConfigs
	dCustom := &SyncDaemon{
		serviceConfigs: []ServiceConfigMapping{
			{RelPath: "services/test/test.yaml", NomadVar: "nomad/jobs/test-job", VarKey: "CONFIG_YAML"},
		},
	}
	daemonMappings := dCustom.GetServiceConfigMappings(t.TempDir())
	if len(daemonMappings) != 1 || daemonMappings[0].RelPath != "services/test/test.yaml" {
		t.Fatalf("expected daemon-configured mappings, got %v", daemonMappings)
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

func TestGetStatus_Lagging(t *testing.T) {
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

	stLagging := d.GetStatus(context.Background())
	if stLagging.Status != "lagging" {
		t.Errorf("expected lagging status, got %s", stLagging.Status)
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

	// 1. Sync execution calls reconcileFn
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

func TestTriggerSync_UnconditionalNomadConfigSync(t *testing.T) {
	tempDir := t.TempDir()
	localDir := filepath.Join(tempDir, "local")
	_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

	configDir := filepath.Join(tempDir, "config")
	_ = os.MkdirAll(filepath.Join(configDir, "services", "mcp"), 0755)
	_ = os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte("channels:\n  default:\n    mode: all\n"), 0644)
	_ = os.WriteFile(filepath.Join(configDir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: 4001\n"), 0644)

	head := "1111111111111111111111111111111111111111"
	var nomadCalls [][]string
	var nomadMu sync.Mutex

	d := NewDaemon(DaemonConfig{
		Repos:     []string{localDir},
		ConfigDir: configDir,
		NomadAddr: "http://127.0.0.1:4646",
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			nomadMu.Lock()
			nomadCalls = append(nomadCalls, args)
			nomadMu.Unlock()
			return []byte("var updated"), nil, nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) == 0 {
				return nil, nil, nil
			}
			switch args[0] {
			case "config":
				return nil, nil, nil
			case "rev-parse":
				return []byte(head), nil, nil
			case "fetch":
				return nil, nil, nil
			case "merge":
				return []byte("Already up to date."), nil, nil
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
	if len(results) != 1 || results[0].Changed {
		t.Fatalf("expected 1 result with Changed=false, got %+v", results)
	}

	nomadMu.Lock()
	callsCount := len(nomadCalls)
	nomadMu.Unlock()

	if callsCount == 0 {
		t.Fatalf("expected nomad var put calls even when git repo has no changes, got 0")
	}

	results2, err := d.TriggerSync()
	if err != nil {
		t.Fatalf("second TriggerSync failed: %v", err)
	}
	if len(results2) != 1 || results2[0].Changed {
		t.Fatalf("expected 1 result with Changed=false, got %+v", results2)
	}

	nomadMu.Lock()
	secondCount := len(nomadCalls)
	nomadMu.Unlock()

	if secondCount != callsCount {
		t.Fatalf("expected call count to remain %d due to caching, got %d", callsCount, secondCount)
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

	customCalled := false
	d.dockerExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		customCalled = true
		return []byte("custom-docker"), nil, nil
	}
	execFn := d.getDockerExecutor()
	_, _, _ = execFn(context.Background(), "")
	if !customCalled {
		t.Errorf("expected custom docker executor to be called")
	}
}

func TestGetComposeExecutor_Branches(t *testing.T) {
	var nilDaemon *SyncDaemon
	if nilDaemon.getComposeExecutor() == nil {
		t.Errorf("expected non-nil default compose executor for nil daemon")
	}

	d := &SyncDaemon{}
	if d.getComposeExecutor() == nil {
		t.Errorf("expected non-nil executor when composeExecutor is nil")
	}

	customCalled := false
	d.composeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		customCalled = true
		return []byte("custom-compose"), nil, nil
	}
	execFn := d.getComposeExecutor()
	_, _, _ = execFn(context.Background(), "", "")
	if !customCalled {
		t.Errorf("expected custom compose executor to be called")
	}
}

func TestPendingNomadServerRestart(t *testing.T) {
	var nilDaemon *SyncDaemon
	nilDaemon.recordPendingNomadServerRestart()
	if nilDaemon.consumePendingNomadServerRestart() {
		t.Errorf("expected false for nil daemon")
	}

	d := &SyncDaemon{}
	if d.consumePendingNomadServerRestart() {
		t.Errorf("expected false initially")
	}

	d.recordPendingNomadServerRestart()
	if !d.consumePendingNomadServerRestart() {
		t.Errorf("expected true after recording restart")
	}
	if d.consumePendingNomadServerRestart() {
		t.Errorf("expected false on subsequent consume")
	}
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
					if revParseCount > 1 {
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

func TestStartPeriodicLoop_ExecutionAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	triggered := make(chan struct{}, 1)
	d := NewDaemon(DaemonConfig{
		Interval:   5 * time.Millisecond,
		ComposeDir: t.TempDir(),
	})
	d.triggerFn = func() ([]RepoSyncResult, error) {
		select {
		case triggered <- struct{}{}:
		default:
		}
		return nil, nil
	}

	d.StartPeriodicLoop(ctx)
	select {
	case <-triggered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for periodic loop execution")
	}
	cancel()
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
		if len(args) >= 4 && args[0] == "job" && args[1] == "restart" {
			restartCalled = true
			restartedJob = args[len(args)-1]
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

	evtCh := make(chan HangarDeployEvent, 10)
	d := NewDaemon(DaemonConfig{
		ConfigDir:      tmpDir,
		NomadExecutor:  mockNomad,
		RegistryClient: mockClient,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("poll_commit_sha_123\n"), nil, nil
			}
			return nil, nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
	})

	// Pre-seed known digest with older hash so new digest triggers restart
	d.nomadKnownDigests["mirrormere-core:ghcr.io/azylman/mirrormere:latest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

	if err := d.CheckAndReconcileNomadImages(context.Background()); err != nil {
		t.Errorf("CheckAndReconcileNomadImages returned error: %v", err)
	}

	if !restartCalled {
		t.Errorf("expected nomad job restart to be called")
	}
	select {
	case dispatchedEvt := <-evtCh:
		if dispatchedEvt.Event != "deploy_started" || dispatchedEvt.JobName != "mirrormere-core" || dispatchedEvt.CommitSHA != "poll_commit_sha_123" {
			t.Errorf("expected deploy_started with commit poll_commit_sha_123, got %+v", dispatchedEvt)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("timed out waiting for deploy_started event")
	}
	if restartedJob != "mirrormere-core" {
		t.Errorf("expected restarted job 'mirrormere-core', got %q", restartedJob)
	}
}

func TestNomadOrchestration_UnitSuite(t *testing.T) {
	t.Parallel()

	// 1. defaultNomadExecutor execution & token scrubbing
	execFn := defaultNomadExecutor("http://127.0.0.1:4646", "secret-nomad-token")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	evtChAerial := make(chan HangarDeployEvent, 10)
	dNomadAerial.deployDispatcher = func(ctx context.Context, evt HangarDeployEvent) error {
		evtChAerial <- evt
		return nil
	}
	respAerialNomad, codeAerialNomad := dNomadAerial.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial",
		Ref:      "refs/heads/main",
		PRNumber: 611,
		TargetID: "1555422677936644147",
	})
	if codeAerialNomad != http.StatusOK || respAerialNomad.Status != "accepted" || respAerialNomad.ContainersBuilding || !respAerialNomad.NomadChanged {
		t.Errorf("expected accepted with NomadChanged=true for aerial nomad change, got code %d, status %s, nomadChanged %v", codeAerialNomad, respAerialNomad.Status, respAerialNomad.NomadChanged)
	}
	var foundAerialDeployEvt bool
	for !foundAerialDeployEvt {
		select {
		case evt := <-evtChAerial:
			if evt.PRNumber != 611 || evt.TargetID != "1555422677936644147" {
				t.Errorf("expected PRNumber 611 and TargetID 1555422677936644147, got %+v", evt)
			}
			if evt.JobName == "migrate" {
				foundAerialDeployEvt = true
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for migrate deploy event from dNomadAerial")
		}
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

	evtChConfig := make(chan HangarDeployEvent, 10)
	dConfigApplied.deployDispatcher = func(ctx context.Context, evt HangarDeployEvent) error {
		evtChConfig <- evt
		return nil
	}
	respConfigApp, codeConfigApp := dConfigApplied.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial-config",
		Ref:      "refs/heads/main",
		PRNumber: 255,
		TargetID: "1555422677936644147",
	})
	if codeConfigApp != http.StatusOK || respConfigApp.ContainersBuilding || len(respConfigApp.AppliedJobs) != 1 {
		t.Errorf("expected job applied when image ready, got code %d, building %v, applied %v", codeConfigApp, respConfigApp.ContainersBuilding, respConfigApp.AppliedJobs)
	}
	var foundConfigDeployEvt bool
	for !foundConfigDeployEvt {
		select {
		case evt := <-evtChConfig:
			if evt.PRNumber != 255 || evt.TargetID != "1555422677936644147" {
				t.Errorf("expected config deploy event with PRNumber 255 and TargetID 1555422677936644147, got %+v", evt)
			}
			if evt.JobName == "webhooks-router" {
				foundConfigDeployEvt = true
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for webhooks-router deploy event from dConfigApplied")
		}
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

	job1 := `variable "image_tag" {
		type    = string
		default = "latest"
	}

	job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:${var.image_tag}"
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "webhooks-router.nomad"), []byte(job1), 0644)

	var runCalled bool
	var restartCalled bool

	d := NewDaemon(DaemonConfig{
		ConfigDir:  tmpDir,
		ComposeDir: tmpDir,
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
	if !runCalled {
		t.Errorf("expected job run to be invoked, run: %v", runCalled)
	}
	if restartCalled {
		t.Errorf("expected job restart NOT to be invoked (double-tap eliminated), restart: %v", restartCalled)
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
	if wImg.Code != http.StatusAccepted {
		t.Errorf("POST /events/image_ready returned %d; want 202 (Accepted)", wImg.Code)
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

	// 6. Metadata propagation (TargetID and PRNumber)
	evtCh := make(chan HangarDeployEvent, 1)
	dMeta := &SyncDaemon{
		nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: eval-123"), nil, nil
		},
		deployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
	}
	applied, err = dMeta.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
		{Action: "update", JobName: "test", Path: "jobs/test.nomad"},
	}, "azylman/aerial", "headsha123", "1555422677936644147", "123")
	if err != nil || len(applied) != 1 || applied[0] != "test" {
		t.Fatalf("expected applied job [test], got %v (err: %v)", applied, err)
	}

	var capturedEvt HangarDeployEvent
	select {
	case capturedEvt = <-evtCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for deploy event")
	}
	if capturedEvt.PRNumber != 123 || capturedEvt.TargetID != "1555422677936644147" || capturedEvt.Repo != "azylman/aerial" || capturedEvt.CommitSHA != "headsha123" {
		t.Errorf("expected metadata propagated, got %+v", capturedEvt)
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

func TestCheckAndReconcileNomadImages_Comprehensive(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobFile := filepath.Join(jobsDir, "svc.nomad")
	content := `
job "svc" {
  group "g" {
    task "t" {
      driver = "docker"
      config {
        image = "ghcr.io/azylman/aerial-brain:latest"
      }
    }
  }
}
`
	_ = os.WriteFile(jobFile, []byte(content), 0644)

	currentDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
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

	var restartedJob string
	mockNomad := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 4 && args[0] == "job" && args[1] == "restart" {
			restartedJob = args[len(args)-1]
			return []byte("Restarted"), nil, nil
		}
		return nil, nil, nil
	}

	evtCh := make(chan HangarDeployEvent, 10)
	d := NewDaemon(DaemonConfig{
		ConfigDir:      tmpDir,
		NomadExecutor:  mockNomad,
		RegistryClient: mockClient,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("poll_commit_sha_123\n"), nil, nil
			}
			return nil, nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
	})

	// 1. First poll seeds known digest
	err := d.CheckAndReconcileNomadImages(ctx)
	if err != nil {
		t.Fatalf("first poll unexpected error: %v", err)
	}
	if restartedJob != "" {
		t.Fatalf("expected no restart on initial discovery, got %s", restartedJob)
	}

	// Reset poll throttle
	d.nomadPollMu.Lock()
	d.lastNomadImagePoll = time.Time{}
	d.nomadPollMu.Unlock()

	// 2. Second poll sees same digest (no restart)
	err = d.CheckAndReconcileNomadImages(ctx)
	if err != nil {
		t.Fatalf("second poll unexpected error: %v", err)
	}
	if restartedJob != "" {
		t.Fatalf("expected no restart when digest matches, got %s", restartedJob)
	}

	// Reset poll throttle
	d.nomadPollMu.Lock()
	d.lastNomadImagePoll = time.Time{}
	d.nomadPollMu.Unlock()

	// 3. Third poll sees new digest -> triggers restart
	currentDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	err = d.CheckAndReconcileNomadImages(ctx)
	if err != nil {
		t.Fatalf("third poll unexpected error: %v", err)
	}
	if restartedJob != "svc" {
		t.Errorf("expected restart for svc, got %q", restartedJob)
	}
	select {
	case dispatchedEvt := <-evtCh:
		if dispatchedEvt.Event != "deploy_started" || dispatchedEvt.CommitSHA != "poll_commit_sha_123" || dispatchedEvt.Status != "started" {
			t.Errorf("expected deploy_started with commit poll_commit_sha_123, got %+v", dispatchedEvt)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("timed out waiting for deploy_started event")
	}

	// Reset poll throttle
	d.nomadPollMu.Lock()
	d.lastNomadImagePoll = time.Time{}
	d.nomadPollMu.Unlock()

	// 4. Restart fails gracefully
	currentDigest = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		return nil, []byte("nomad down"), errors.New("restart fail")
	}
	err = d.CheckAndReconcileNomadImages(ctx)
	if err != nil {
		t.Fatalf("poll with restart fail returned error: %v", err)
	}
}

func TestHasNomadConfigChanges_ContextError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	d := &SyncDaemon{}
	got, err := d.HasNomadConfigChanges(canceledCtx, "/some/path", "v1", "v2")
	if err == nil || got {
		t.Errorf("expected ctx error, got %v, %v", got, err)
	}

	d.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return nil, []byte("error"), errors.New("fail")
	}
	// Fallback path with canceled context
	got, err = d.HasNomadConfigChanges(canceledCtx, "/some/path", "v1", "v2")
	if err == nil || got {
		t.Errorf("expected ctx error in fallback, got %v, %v", got, err)
	}
}

func TestValidateNomadConfig_NilExecutor(t *testing.T) {
	d := &SyncDaemon{}
	err := d.ValidateNomadConfig(context.Background(), "/path")
	if err == nil {
		t.Errorf("expected error for nil nomad executor")
	}
}

func TestExecuteGitPushEvent_DispatchesDeployStarted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)

	jobFile := filepath.Join(aerialConfigDir, "jobs", "webhooks-router.nomad")
	_ = os.WriteFile(jobFile, []byte(`job "webhooks-router" {}`), 0644)

	evtCh := make(chan HangarDeployEvent, 10)
	var revParseCount int

	d := NewDaemon(DaemonConfig{
		Repos:                  []string{aerialConfigDir},
		ConfigDir:              aerialConfigDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
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
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return []byte("Evaluation ID: run-ok"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "status" {
				return []byte(`{
					"ID": "webhooks-router",
					"JobVersion": 1,
					"LatestDeployment": {
						"ID": "dep-git-push-1",
						"JobVersion": 1,
						"Status": "successful",
						"StatusDescription": "Deployment completed successfully"
					}
				}`), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:   "aerial-config",
		Ref:    "refs/heads/main",
		Commit: "commit_abc123",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
	}

	// 1. Wait for sync_success (git-sync) and deploy_started (webhooks-router)
	var receivedEvts []HangarDeployEvent
	for i := 0; i < 2; i++ {
		select {
		case evt := <-evtCh:
			receivedEvts = append(receivedEvts, evt)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i+1)
		}
	}

	var deployStartedEvt *HangarDeployEvent
	var syncSuccessEvt *HangarDeployEvent
	for i := range receivedEvts {
		if receivedEvts[i].Event == "deploy_started" {
			deployStartedEvt = &receivedEvts[i]
		} else if receivedEvts[i].Event == "sync_success" {
			syncSuccessEvt = &receivedEvts[i]
		}
	}
	if syncSuccessEvt == nil {
		t.Fatalf("missing sync_success event in received: %+v", receivedEvts)
	}
	if deployStartedEvt == nil {
		t.Fatalf("missing deploy_started event in received: %+v", receivedEvts)
	}
	if deployStartedEvt.JobName != "webhooks-router" {
		t.Errorf("expected job_name webhooks-router, got %q", deployStartedEvt.JobName)
	}
	if deployStartedEvt.Repo != "aerial-config" {
		t.Errorf("expected repo aerial-config, got %q", deployStartedEvt.Repo)
	}
	if deployStartedEvt.CommitSHA != "commit_abc123" {
		t.Errorf("expected commit_sha commit_abc123, got %q", deployStartedEvt.CommitSHA)
	}
	if deployStartedEvt.Status != "started" {
		t.Errorf("expected status started, got %q", deployStartedEvt.Status)
	}

	// 2. Ensure deployment observation is delegated to webhooks-router
	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected event from Hangar: %+v (deploy observation is delegated to webhooks-router)", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no further events dispatched
	}
}

func TestExecuteGitPushEvent_DispatchesSyncSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0755)

	evtCh := make(chan HangarDeployEvent, 10)

	// 1. Successful sync with TargetID dispatches sync_success
	dSuccess := NewDaemon(DaemonConfig{
		Repos:     []string{aerialDir},
		ConfigDir: aerialDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha_sync_ok"), nil, nil
			}
			return []byte(""), nil, nil
		},
	})

	resp, code := dSuccess.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial",
		Ref:      "refs/heads/main",
		Commit:   "commit_12345",
		TargetID: "1555405874565091380",
		PRNumber: 542,
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "sync_success" {
			t.Errorf("expected sync_success, got %q", evt.Event)
		}
		if evt.TargetID != "1555405874565091380" {
			t.Errorf("expected TargetID 1555405874565091380, got %q", evt.TargetID)
		}
		if evt.PRNumber != 542 {
			t.Errorf("expected PRNumber 542, got %d", evt.PRNumber)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync_success event")
	}

	// 2. Failed sync with TargetID dispatches sync_failed
	dFail := NewDaemon(DaemonConfig{
		Repos:     []string{aerialDir},
		ConfigDir: aerialDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && (args[0] == "fetch" || args[0] == "pull") {
				return nil, []byte("fatal: remote error"), fmt.Errorf("git fetch failed")
			}
			return []byte(""), nil, nil
		},
	})

	respFail, codeFail := dFail.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial",
		Ref:      "refs/heads/main",
		Commit:   "commit_fail",
		TargetID: "1555405874565091380",
		PRNumber: 542,
	})
	if codeFail != http.StatusInternalServerError || respFail.Status != "error" {
		t.Fatalf("expected 500 error, got %d (%s)", codeFail, respFail.Status)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "sync_failed" {
			t.Errorf("expected sync_failed, got %q", evt.Event)
		}
		if evt.TargetID != "1555405874565091380" {
			t.Errorf("expected TargetID 1555405874565091380, got %q", evt.TargetID)
		}
		if evt.Status != "failed" {
			t.Errorf("expected status failed, got %q", evt.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync_failed event")
	}

	// 3. Successful sync with empty CurrentHead falls back to req.Commit
	dFallback := NewDaemon(DaemonConfig{
		Repos:     []string{aerialDir},
		ConfigDir: aerialDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return []byte(""), nil, nil
		},
	})

	respFallback, codeFallback := dFallback.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial",
		Ref:      "refs/heads/main",
		Commit:   "commit_req_fallback",
		TargetID: "1555405874565091380",
		PRNumber: 542,
	})
	if codeFallback != http.StatusOK || respFallback.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", codeFallback, respFallback.Status)
	}

	select {
	case evt := <-evtCh:
		if evt.CommitSHA != "commit_req_fallback" {
			t.Errorf("expected CommitSHA commit_req_fallback, got %q", evt.CommitSHA)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fallback commit sync_success event")
	}

	// 4. Successful sync with empty TargetID still dispatches sync_success (for late resolution)
	respEmpty, codeEmpty := dSuccess.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:     "aerial",
		Ref:      "refs/heads/main",
		Commit:   "commit_empty_target",
		TargetID: "",
		PRNumber: 0,
	})
	if codeEmpty != http.StatusOK || respEmpty.Status != "accepted" {
		t.Fatalf("expected 200 accepted for empty TargetID, got %d (%s)", codeEmpty, respEmpty.Status)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "sync_success" {
			t.Errorf("expected sync_success, got %q", evt.Event)
		}
		if evt.TargetID != "" {
			t.Errorf("expected empty TargetID, got %q", evt.TargetID)
		}
		if evt.CommitSHA != "sha_sync_ok" {
			t.Errorf("expected CommitSHA sha_sync_ok, got %q", evt.CommitSHA)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for empty TargetID sync_success event")
	}
}

func TestExecuteGitPushEvent_DispatchesDeployFailed(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)

	jobFile := filepath.Join(aerialConfigDir, "jobs", "webhooks-router.nomad")
	_ = os.WriteFile(jobFile, []byte(`job "webhooks-router" {}`), 0644)

	evtCh := make(chan HangarDeployEvent, 10)
	var revParseCount int

	d := NewDaemon(DaemonConfig{
		Repos:                  []string{aerialConfigDir},
		ConfigDir:              aerialConfigDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
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
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("driver error: failed to place allocation"), errors.New("nomad run failed")
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo:   "aerial-config",
		Ref:    "refs/heads/main",
		Commit: "fail_commit_123",
	})
	if code != http.StatusOK {
		t.Fatalf("expected 200 accepted response, got %d", code)
	}
	if len(resp.AppliedJobs) != 0 {
		t.Errorf("expected 0 applied jobs on run failure, got %v", resp.AppliedJobs)
	}

	// Collect all three dispatched events: sync_success (git-sync), deploy_started (webhooks-router), deploy_failed (webhooks-router)
	var received []HangarDeployEvent
	for i := 0; i < 3; i++ {
		select {
		case evt := <-evtCh:
			received = append(received, evt)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i+1)
		}
	}

	hasSyncSuccess := false
	hasStarted := false
	hasFailed := false
	for _, evt := range received {
		if evt.Event == "sync_success" {
			hasSyncSuccess = true
		}
		if evt.Event == "deploy_started" {
			hasStarted = true
		}
		if evt.Event == "deploy_failed" {
			hasFailed = true
			if evt.Status != "failed" {
				t.Errorf("expected status failed, got %q", evt.Status)
			}
			if !strings.Contains(evt.Details, "driver error") {
				t.Errorf("expected details to contain error message, got %q", evt.Details)
			}
		}
	}
	if !hasSyncSuccess {
		t.Errorf("missing sync_success event in received: %+v", received)
	}
	if !hasStarted {
		t.Errorf("missing deploy_started event in received: %+v", received)
	}
	if !hasFailed {
		t.Errorf("missing deploy_failed event in received: %+v", received)
	}
}

func TestExecuteImageReadyEvent_DispatchesDeployStartedOnly(t *testing.T) {
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

	evtCh := make(chan HangarDeployEvent, 10)

	d := NewDaemon(DaemonConfig{
		ConfigDir:              tmpDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return []byte("Evaluation ID: run-123"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				return []byte("Restart triggered"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "status" {
				return []byte(`{
					"ID": "webhooks-router",
					"JobVersion": 2,
					"LatestDeployment": {
						"ID": "dep-rollback-uuid",
						"JobVersion": 2,
						"Status": "failed",
						"StatusDescription": "deployment failed: auto-reverting to version 1",
						"TaskGroups": {
							"router": {
								"AutoRevert": true
							}
						}
					}
				}`), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest: "sha256:digest_abc",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
	}

	// 1. Wait for deploy_started
	select {
	case evt := <-evtCh:
		if evt.Event != "deploy_started" {
			t.Errorf("expected deploy_started, got %q", evt.Event)
		}
		if evt.Image != "ghcr.io/azylman/aerial-webhooks-router:latest" {
			t.Errorf("expected image in event, got %q", evt.Image)
		}
		if evt.Digest != "sha256:digest_abc" {
			t.Errorf("expected digest sha256:digest_abc, got %q", evt.Digest)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deploy_started")
	}

	// 2. Ensure deployment observation is delegated to webhooks-router
	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected event from Hangar: %+v (deploy observation is delegated to webhooks-router)", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no further events dispatched
	}
}

func TestExecuteImageReadyEvent_PropagatesMetadataInDeployStarted(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	job1 := `job "brain" {
		task "brain" {
			config {
				image = "ghcr.io/azylman/aerial-brain:latest"
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "brain.nomad"), []byte(job1), 0644)

	evtCh := make(chan HangarDeployEvent, 10)

	d := NewDaemon(DaemonConfig{
		ConfigDir:              tmpDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return []byte("Evaluation ID: run-success-123"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				return []byte("Restart triggered"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "status" {
				return []byte(`{
					"ID": "brain",
					"JobVersion": 3,
					"LatestDeployment": {
						"ID": "dep-success-uuid",
						"JobVersion": 3,
						"Status": "successful",
						"StatusDescription": "deployment successful"
					}
				}`), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/aerial-brain:latest",
		Digest:    "sha256:digest_brain",
		Repo:      "azylman/aerial",
		CommitSHA: "headsha777",
		PRNumber:  535,
		TargetID:  "1555405874565091380",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
	}

	// 1. Wait for deploy_started
	select {
	case evt := <-evtCh:
		if evt.Event != "deploy_started" {
			t.Errorf("expected deploy_started, got %q", evt.Event)
		}
		if evt.JobName != "brain" {
			t.Errorf("expected job_name brain, got %q", evt.JobName)
		}
		if evt.Repo != "azylman/aerial" || evt.CommitSHA != "headsha777" || evt.PRNumber != 535 || evt.TargetID != "1555405874565091380" {
			t.Errorf("metadata missing in deploy_started: %+v", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deploy_started")
	}

	// 2. Ensure deployment observation is delegated to webhooks-router
	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected event from Hangar: %+v (deploy observation is delegated to webhooks-router)", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no further events dispatched
	}
}

func TestDispatchDeployEvent_DispatcherError(t *testing.T) {
	errCh := make(chan struct{}, 1)
	d := NewDaemon(DaemonConfig{
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			errCh <- struct{}{}
			return fmt.Errorf("simulated dispatch error")
		},
	})
	d.DispatchDeployEvent(HangarDeployEvent{
		Event:   "deploy_started",
		JobName: "err-job",
	})
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deploy dispatcher error invocation")
	}
}

func TestDefaultDeployDispatcher_TableDriven(t *testing.T) {
	var receivedEvt HangarDeployEvent
	var receivedPath string
	var returnStatus int = http.StatusOK

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&receivedEvt)
		w.WriteHeader(returnStatus)
		if returnStatus >= 400 {
			_, _ = w.Write([]byte("error processing webhook"))
		}
	}))
	defer server.Close()

	d := NewDaemon(DaemonConfig{
		WebhooksRouterURL: server.URL,
	})

	testEvt := HangarDeployEvent{
		Event:     "deploy_started",
		JobName:   "test-job",
		Status:    "started",
		Timestamp: time.Now().UTC(),
	}

	// 1. Success
	err := d.defaultDeployDispatcher(context.Background(), testEvt)
	if err != nil {
		t.Fatalf("unexpected error in defaultDeployDispatcher: %v", err)
	}
	if receivedPath != "/api/webhooks/hangar" {
		t.Errorf("expected path /api/webhooks/hangar, got %q", receivedPath)
	}
	if receivedEvt.JobName != "test-job" {
		t.Errorf("expected job_name test-job, got %q", receivedEvt.JobName)
	}

	// 2. HTTP error response
	returnStatus = http.StatusInternalServerError
	errFail := d.defaultDeployDispatcher(context.Background(), testEvt)
	if errFail == nil {
		t.Errorf("expected error when server returns 500")
	}

	// 3. Network error (connection refused / closed port)
	dClosed := NewDaemon(DaemonConfig{
		WebhooksRouterURL: "http://127.0.0.1:59999",
	})
	errNetwork := dClosed.defaultDeployDispatcher(context.Background(), testEvt)
	if errNetwork == nil {
		t.Errorf("expected network error when connecting to closed port")
	}
}

func TestReconcileNomadChanges_LifecycleEvents(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobFile := filepath.Join(tmpDir, "server.nomad")
	_ = os.WriteFile(jobFile, []byte(`job "server" {}`), 0644)

	evtCh := make(chan HangarDeployEvent, 10)

	d := NewDaemon(DaemonConfig{
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return []byte("Evaluation ID: run-rec"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "status" {
				return []byte(`{
					"ID": "server",
					"JobVersion": 1,
					"LatestDeployment": {
						"ID": "dep-rec-1",
						"JobVersion": 1,
						"Status": "successful",
						"StatusDescription": "reconciliation successful"
					}
				}`), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	changes := []NomadFileChange{
		{
			Path:    "server.nomad",
			Action:  "apply",
			JobName: "server",
		},
	}

	err := d.ReconcileNomadJobs(ctx, tmpDir, changes, "azylman/aerial", "commit616", "thread_12345", "616")
	if err != nil {
		t.Fatalf("ReconcileNomadJobs failed: %v", err)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "deploy_started" {
			t.Errorf("expected deploy_started, got %q", evt.Event)
		}
		if evt.Repo != "azylman/aerial" || evt.CommitSHA != "commit616" || evt.PRNumber != 616 || evt.TargetID != "thread_12345" {
			t.Errorf("expected PR provenance in deploy_started, got %+v", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deploy_started")
	}

	// 2. Ensure deployment observation is delegated to webhooks-router
	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected event from Hangar: %+v (deploy observation is delegated to webhooks-router)", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no further events dispatched
	}
}

func TestExecuteGitPushEvent_ComprehensiveEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Missing repo
	d := NewDaemon(DaemonConfig{})
	resp, code := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{})
	if code != http.StatusBadRequest || resp.Status != "error" {
		t.Errorf("expected 400 error for missing repo, got %d (%s)", code, resp.Status)
	}

	// 2. Non-main branch push ignored
	resp, code = d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/feature-123",
	})
	if code != http.StatusOK || resp.Status != "ignored" {
		t.Errorf("expected 200 ignored for non-main branch, got %d (%s)", code, resp.Status)
	}

	// 3. Unmanaged repo
	resp, code = d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "unknown-repo",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusNotFound || resp.Status != "error" {
		t.Errorf("expected 404 for unmanaged repo, got %d (%s)", code, resp.Status)
	}

	// 4. Git sync error
	tmpDir := t.TempDir()
	aerialCoreDir := filepath.Join(tmpDir, "aerial")
	_ = os.MkdirAll(filepath.Join(aerialCoreDir, ".git"), 0755)

	dSyncErr := NewDaemon(DaemonConfig{
		Repos: []string{aerialCoreDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			return nil, []byte("fatal: remote error"), errors.New("sync failed")
		},
	})
	resp, code = dSyncErr.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Errorf("expected 500 error for failed git sync, got %d (%s)", code, resp.Status)
	}

	// 5. Aerial Core: Container build changes detected
	var revParse5 int
	dCoreBuild := NewDaemon(DaemonConfig{
		Repos: []string{aerialCoreDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				revParse5++
				if revParse5 == 1 {
					return []byte("sha_before"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
				return []byte("brain/main.go\nDockerfile\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
	})
	resp, code = dCoreBuild.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusOK || !resp.ContainersBuilding {
		t.Errorf("expected 200 with ContainersBuilding=true, got %d (%+v)", code, resp)
	}

	// 6. Aerial Core: Nomad changes with apply error
	_ = os.MkdirAll(filepath.Join(aerialCoreDir, "nomad", "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(aerialCoreDir, "nomad", "jobs", "test.nomad"), []byte(`job "test" {}`), 0644)
	var revParse6 int
	dCoreNomadErr := NewDaemon(DaemonConfig{
		Repos: []string{aerialCoreDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				revParse6++
				if revParse6 == 1 {
					return []byte("sha_before"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
				return []byte("README.md\n"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("M nomad/jobs/test.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("run failure"), errors.New("nomad run failed")
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dCoreNomadErr.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Errorf("expected 500 error for nomad apply error, got %d (%+v)", code, resp)
	}

	// 7. Aerial Core: No changes
	dCoreNoChange := NewDaemon(DaemonConfig{
		Repos: []string{aerialCoreDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha1"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" {
				return []byte("docs/index.html\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
	})
	resp, code = dCoreNoChange.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Errorf("expected 200 accepted for core no changes, got %d (%+v)", code, resp)
	}

	// 8. Aerial Config: Delete job action, deferred job, unreadable file, invalid job
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.WriteFile(filepath.Join(aerialConfigDir, "jobs", "deferred.nomad"), []byte(`job "deferred" { task "t" { config { image = "ghcr.io/azylman/aerial-brain:latest" } } }`), 0644)
	_ = os.WriteFile(filepath.Join(aerialConfigDir, "jobs", "invalid.nomad"), []byte(`invalid syntax`), 0644)

	var revParse8 int
	dConfigComplex := NewDaemon(DaemonConfig{
		Repos:     []string{aerialConfigDir},
		ConfigDir: aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				if len(args) >= 2 && args[1] == "FETCH_HEAD" {
					return []byte("sha_after"), nil, nil
				}
				revParse8++
				if revParse8 == 1 {
					return []byte("sha_before"), nil, nil
				}
				return []byte("sha_after"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
				return []byte("D jobs/deleted.nomad\nM jobs/deferred.nomad\nM jobs/invalid.nomad\nM jobs/nonexistent.nomad\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				if strings.Contains(args[len(args)-1], "invalid.nomad") {
					return nil, []byte("syntax error"), errors.New("invalid job")
				}
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dConfigComplex.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusOK || !resp.NomadChanged {
		t.Errorf("expected 200 with NomadChanged=true for aerial-config, got %d (%+v)", code, resp)
	}

	// 9. Aerial Config: No changes
	dConfigNoChange := NewDaemon(DaemonConfig{
		Repos:     []string{aerialConfigDir},
		ConfigDir: aerialConfigDir,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha1"), nil, nil
			}
			if len(args) >= 2 && args[0] == "diff" {
				return []byte(""), nil, nil
			}
			return []byte(""), nil, nil
		},
	})
	resp, code = dConfigNoChange.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "aerial-config",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Errorf("expected 200 accepted for config no changes, got %d (%+v)", code, resp)
	}

	// 10. Generic third-party repo
	otherRepoDir := filepath.Join(tmpDir, "other-repo")
	_ = os.MkdirAll(filepath.Join(otherRepoDir, ".git"), 0755)
	dOther := NewDaemon(DaemonConfig{
		Repos: []string{otherRepoDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha1"), nil, nil
			}
			return []byte(""), nil, nil
		},
	})
	resp, code = dOther.ExecuteGitPushEvent(ctx, GitPushEventRequest{
		Repo: "other-repo",
		Ref:  "refs/heads/main",
	})
	if code != http.StatusOK || resp.Message != "repository synced" {
		t.Errorf("expected 200 'repository synced' for other repo, got %d (%+v)", code, resp)
	}
}

func TestExecuteImageReadyEvent_ComprehensiveEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Missing image
	d := NewDaemon(DaemonConfig{})
	resp, code := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{})
	if code != http.StatusBadRequest || resp.Status != "error" {
		t.Errorf("expected 400 for empty image, got %d (%s)", code, resp.Status)
	}

	// 2. No matching jobs
	resp, code = d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/nonexistent:latest",
	})
	if code != http.StatusNotFound || resp.Status != "not_found" {
		t.Errorf("expected 404 for nonexistent image, got %d (%s)", code, resp.Status)
	}

	// 3. Matching jobs with validation error and run error resulting in 500
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobValid := `job "job-valid" { task "t" { config { image = "ghcr.io/azylman/test-app:latest" } } }`
	jobInvalid := `job "job-invalid" { task "t" { config { image = "ghcr.io/azylman/test-app:latest" } } }`
	_ = os.WriteFile(filepath.Join(jobsDir, "valid.nomad"), []byte(jobValid), 0644)
	_ = os.WriteFile(filepath.Join(jobsDir, "invalid.nomad"), []byte(jobInvalid), 0644)

	dErr := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				if strings.Contains(args[len(args)-1], "invalid.nomad") {
					return nil, []byte("val error"), errors.New("val failed")
				}
				return []byte("OK"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("run error"), errors.New("run failed")
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dErr.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/test-app:latest",
	})
	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Errorf("expected 500 error when all matching jobs fail run, got %d (%+v)", code, resp)
	}

	// 4. Core composeDir matching job with restart warning and digest caching
	composeDir := filepath.Join(tmpDir, "core")
	coreJobsDir := filepath.Join(composeDir, "nomad", "jobs")
	_ = os.MkdirAll(coreJobsDir, 0755)
	jobCore := `job "core-job" { task "t" { config { image = "ghcr.io/azylman/core-service:latest" } } }`
	_ = os.WriteFile(filepath.Join(coreJobsDir, "core.nomad"), []byte(jobCore), 0644)

	dCore := NewDaemon(DaemonConfig{
		ConfigDir:  filepath.Join(tmpDir, "empty-config"),
		ComposeDir: composeDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				return nil, []byte("restart warning"), errors.New("restart warning err")
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dCore.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:  "ghcr.io/azylman/core-service:latest",
		Digest: "sha256:core123456",
	})
	if code != http.StatusOK || resp.Status != "accepted" || len(resp.MatchedJobs) != 1 {
		t.Errorf("expected 200 accepted for core job match, got %d (%+v)", code, resp)
	}
}

func TestEnsureDockerAuth_AllCases(t *testing.T) {
	// 1. Empty PAT -> no-op nil
	dEmpty := NewDaemon(DaemonConfig{PAT: ""})
	if err := dEmpty.EnsureDockerAuth(); err != nil {
		t.Errorf("expected nil for empty PAT, got %v", err)
	}

	// 2. Valid PAT -> writes config file successfully
	tmpDir := t.TempDir()
	lookup := func(k string) string {
		if k == "DOCKER_CONFIG" {
			return filepath.Join(tmpDir, ".docker")
		}
		return ""
	}
	if err := EnsureDockerAuthPath("ghp_mock_token_12345", "", lookup, nil, nil, nil); err != nil {
		t.Errorf("expected success writing docker config, got %v", err)
	}
	configPath := filepath.Join(tmpDir, ".docker", "config.json")
	content, err := os.ReadFile(configPath)
	if err != nil || !strings.Contains(string(content), "ghcr.io") {
		t.Errorf("expected docker config with ghcr.io, got %s, err: %v", string(content), err)
	}
}

func TestExecutorsAndDispatchers_EdgeCoverage(t *testing.T) {
	// 1. defaultNomadExecutor
	nomadExec := defaultNomadExecutor("http://127.0.0.1:4646", "secret-token")
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _ = nomadExec(cancelCtx, "version")

	// 2. runGitCommand
	ctx := context.Background()
	_, _, _ = runGitCommand(ctx, "", "", "version")

	cancelCtxGit, cancelGit := context.WithCancel(context.Background())
	cancelGit()
	_, _, _ = runGitCommand(cancelCtxGit, "", "", "status")

	// 3. DispatchDeployEvent & MonitorDeploymentAsync nil / panic safeguards
	var nilDaemon *SyncDaemon
	nilDaemon.DispatchDeployEvent(HangarDeployEvent{})

	dPanic := NewDaemon(DaemonConfig{
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			panic("simulated panic in dispatcher")
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			panic("simulated panic in monitor")
		},
	})
	dPanic.DispatchDeployEvent(HangarDeployEvent{JobName: "test-panic"})
	time.Sleep(50 * time.Millisecond)
}

func TestDaemonLoops_GracefulCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d := NewDaemon(DaemonConfig{
		Interval: 10 * time.Millisecond,
	})
	d.StartPeriodicLoop(ctx)
	d.StartReconcilerLoop(ctx)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestReconcileNomadChanges_SkipPurgeWhenDefinedInOtherRepo(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	repoA := filepath.Join(tempDir, "aerial-config")
	repoB := filepath.Join(tempDir, "aerial")

	jobsDirA := filepath.Join(repoA, "jobs")
	nomadJobsDirB := filepath.Join(repoB, "nomad", "jobs")

	if err := os.MkdirAll(jobsDirA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nomadJobsDirB, 0755); err != nil {
		t.Fatal(err)
	}

	jobSpecBrain := `job "brain" { type = "service" }`
	if err := os.WriteFile(filepath.Join(nomadJobsDirB, "brain.nomad"), []byte(jobSpecBrain), 0644); err != nil {
		t.Fatal(err)
	}

	var stoppedJobs []string
	var mu sync.Mutex

	d := NewDaemon(DaemonConfig{
		Repos:      []string{repoA, repoB},
		ConfigDir:  repoA,
		ComposeDir: repoB,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			defer mu.Unlock()
			if len(args) >= 3 && args[0] == "job" && args[1] == "stop" {
				stoppedJobs = append(stoppedJobs, args[len(args)-1])
			}
			return []byte("OK"), nil, nil
		},
	})

	// Case 1: Job brain deleted in repoA, but exists in repoB -> should NOT be stopped
	err := d.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
		{
			Path:    "jobs/brain.nomad",
			Action:  "delete",
			JobName: "brain",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	if len(stoppedJobs) > 0 {
		t.Errorf("expected 0 stopped jobs for brain, got %v", stoppedJobs)
	}
	stoppedJobs = nil
	mu.Unlock()

	// Case 2: Job truly_deleted deleted in repoA and not in repoB -> SHOULD be stopped
	err = d.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
		{
			Path:    "jobs/truly_deleted.nomad",
			Action:  "delete",
			JobName: "truly_deleted",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	if len(stoppedJobs) != 1 || stoppedJobs[0] != "truly_deleted" {
		t.Errorf("expected truly_deleted to be stopped, got %v", stoppedJobs)
	}
	stoppedJobs = nil
	mu.Unlock()

	// Case 3: ExecuteNomadTeardowns with both jobs
	pending := []NomadChangeEvent{
		{
			RepoPath: repoA,
			Changes: []NomadFileChange{
				{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
				{Path: "jobs/old_orphan.nomad", Action: "delete", JobName: "old_orphan"},
			},
		},
	}
	d.ExecuteNomadTeardowns(ctx, pending)

	mu.Lock()
	if len(stoppedJobs) != 1 || stoppedJobs[0] != "old_orphan" {
		t.Errorf("expected only old_orphan to be stopped in teardowns, got %v", stoppedJobs)
	}
	mu.Unlock()
}

func TestEnqueueImageReadyEvent_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobContent := `job "debounced-test" {
		group "debounced-test" {
			task "debounced-test" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/debounced-app:latest"
				}
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "debounced-test.nomad"), []byte(jobContent), 0644)

	d := NewDaemon(DaemonConfig{
		Repos:     []string{tmpDir},
		ComposeDir: tmpDir,
		ConfigDir:  tmpDir,
	})

	// 1. Missing image -> 400
	respEmpty, codeEmpty := d.EnqueueImageReadyEvent(ImageReadyEventRequest{})
	if codeEmpty != http.StatusBadRequest || respEmpty.Status != "error" {
		t.Errorf("expected 400 for empty image, got %d (%+v)", codeEmpty, respEmpty)
	}

	// 2. Unknown image -> 404
	respUnknown, codeUnknown := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image: "ghcr.io/azylman/nonexistent:latest",
	})
	if codeUnknown != http.StatusNotFound || respUnknown.Status != "not_found" {
		t.Errorf("expected 404 for unknown image, got %d (%+v)", codeUnknown, respUnknown)
	}

	// 3. Valid image -> 202 Accepted and queued
	respValid, codeValid := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/debounced-app:latest",
		Repo:      "azylman/aerial",
		CommitSHA: "sha123",
		PRNumber:  555,
		TargetID:  "1555405874565091380",
	})
	if codeValid != http.StatusAccepted || respValid.Status != "accepted" {
		t.Errorf("expected 202 Accepted, got %d (%+v)", codeValid, respValid)
	}
	if len(respValid.MatchedJobs) != 1 || respValid.MatchedJobs[0] != "debounced-test" {
		t.Errorf("expected matched job debounced-test, got %v", respValid.MatchedJobs)
	}

	d.pendingImageMu.Lock()
	rollout, ok := d.pendingImageRollouts["debounced-test"]
	d.pendingImageMu.Unlock()
	if !ok {
		t.Fatalf("expected pendingImageRollouts to contain debounced-test")
	}
	if rollout.JobName != "debounced-test" || rollout.Request.CommitSHA != "sha123" {
		t.Errorf("unexpected rollout stored: %+v", rollout)
	}
}

func TestStartImageRolloutLoop_DebouncedExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	job1 := `job "service-a" {
		group "service-a" {
			task "service-a" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/service-a:latest"
				}
			}
		}
	}`
	job2 := `job "service-b" {
		group "service-b" {
			task "service-b" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/service-b:latest"
				}
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "service-a.nomad"), []byte(job1), 0644)
	_ = os.WriteFile(filepath.Join(jobsDir, "service-b.nomad"), []byte(job2), 0644)

	var runCalls []string
	var runMu sync.Mutex
	evtCh := make(chan HangarDeployEvent, 10)

	mockRegistryClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	d := NewDaemon(DaemonConfig{
		Repos:          []string{tmpDir},
		ComposeDir:     tmpDir,
		ConfigDir:      tmpDir,
		RegistryClient: mockRegistryClient,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			runMu.Lock()
			runCalls = append(runCalls, strings.Join(args, " "))
			runMu.Unlock()
			return []byte("OK"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
	})

	d.SetImageRolloutDebounce(10 * time.Millisecond)
	d.StartImageRolloutLoop(ctx)

	// Enqueue both images in quick succession
	_, codeA := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/service-a:latest",
		CommitSHA: "commit-alpha",
	})
	if codeA != http.StatusAccepted {
		t.Fatalf("expected 202 for service-a, got %d", codeA)
	}

	_, codeB := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/service-b:latest",
		CommitSHA: "commit-alpha",
	})
	if codeB != http.StatusAccepted {
		t.Fatalf("expected 202 for service-b, got %d", codeB)
	}

	// Wait for debounce timer to fire and execute both rollouts
	time.Sleep(50 * time.Millisecond)

	runMu.Lock()
	executed := append([]string(nil), runCalls...)
	runMu.Unlock()

	hasJobRunA := false
	hasJobRunB := false
	runCount := 0
	for _, call := range executed {
		if strings.Contains(call, "service-a.nomad") {
			hasJobRunA = true
		}
		if strings.Contains(call, "service-b.nomad") {
			hasJobRunB = true
		}
		if strings.Contains(call, "job run -detach") {
			runCount++
		}
	}

	if !hasJobRunA || !hasJobRunB || runCount != 2 {
		t.Errorf("expected debounced execution of both service-a and service-b (runs: %d), got calls: %v", runCount, executed)
	}
}

func TestStartImageRolloutLoop_PanicRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	job1 := `job "service-panic" {
		group "service-panic" {
			task "service-panic" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/service-panic:latest"
				}
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "service-panic.nomad"), []byte(job1), 0644)

	var runCount int
	var runMu sync.Mutex

	mockRegistryClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	d := NewDaemon(DaemonConfig{
		Repos:          []string{tmpDir},
		ComposeDir:     tmpDir,
		ConfigDir:      tmpDir,
		RegistryClient: mockRegistryClient,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			runMu.Lock()
			runCount++
			count := runCount
			runMu.Unlock()
			if count == 1 {
				panic("simulated panic in rollout executor")
			}
			return []byte("OK"), nil, nil
		},
	})

	d.SetImageRolloutDebounce(10 * time.Millisecond)
	d.StartImageRolloutLoop(ctx)

	// First event will trigger panic
	_, code1 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/service-panic:latest",
		CommitSHA: "panic-sha",
	})
	if code1 != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", code1)
	}

	time.Sleep(30 * time.Millisecond)

	// Second event should still execute because loop survived the panic
	_, code2 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/service-panic:latest",
		CommitSHA: "recovered-sha",
	})
	if code2 != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", code2)
	}

	time.Sleep(30 * time.Millisecond)

	runMu.Lock()
	finalCount := runCount
	runMu.Unlock()

	if finalCount < 2 {
		t.Errorf("expected at least 2 executor attempts (surviving panic), got %d", finalCount)
	}
}

func TestExecutePendingImageRollouts_Branches(t *testing.T) {
	mockRegistryClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	d := NewDaemon(DaemonConfig{
		RegistryClient: mockRegistryClient,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: 12345"), nil, nil
		},
	})

	// 1. Empty rollouts (immediate return)
	d.executePendingImageRollouts(context.Background())

	// 2. Cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	d.pendingImageMu.Lock()
	d.pendingImageRollouts["img1"] = PendingImageRollout{
		Request: ImageReadyEventRequest{
			Image: "ghcr.io/azylman/aerial-brain:latest",
		},
	}
	d.pendingImageMu.Unlock()
	d.executePendingImageRollouts(cancelCtx)

	// 3. Normal execution
	d.pendingImageMu.Lock()
	d.pendingImageRollouts["img1"] = PendingImageRollout{
		Request: ImageReadyEventRequest{
			Image: "ghcr.io/azylman/aerial-brain:latest",
		},
	}
	d.pendingImageMu.Unlock()
	d.executePendingImageRollouts(context.Background())
}

func TestExecutePendingImageRollouts_HangarLast(t *testing.T) {
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	_ = os.WriteFile(filepath.Join(jobsDir, "hangar.nomad"), []byte(`job "hangar" { task "h" { config { image = "ghcr.io/azylman/aerial-hangar:latest" } } }`), 0644)
	_ = os.WriteFile(filepath.Join(jobsDir, "webhooks-router.nomad"), []byte(`job "webhooks-router" { task "r" { config { image = "ghcr.io/azylman/aerial-webhooks-router:latest" } } }`), 0644)

	var mu sync.Mutex
	var executedJobs []string

	mockRegistryClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", "sha256:1111111111111111111111111111111111111111111111111111111111111111")
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
		RegistryClient: mockRegistryClient,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				jobPath := args[len(args)-1]
				body, _ := os.ReadFile(jobPath)
				jobName := ExtractJobName(string(body), filepath.Base(jobPath))
				mu.Lock()
				executedJobs = append(executedJobs, jobName)
				mu.Unlock()
			}
			return []byte("Evaluation ID: 12345"), nil, nil
		},
	})

	d.pendingImageMu.Lock()
	d.pendingImageRollouts["hangar"] = PendingImageRollout{
		JobName: "hangar",
		Request: ImageReadyEventRequest{
			Image: "ghcr.io/azylman/aerial-hangar:latest",
		},
	}
	d.pendingImageRollouts["webhooks-router"] = PendingImageRollout{
		JobName: "webhooks-router",
		Request: ImageReadyEventRequest{
			Image: "ghcr.io/azylman/aerial-webhooks-router:latest",
		},
	}
	d.pendingImageMu.Unlock()

	d.executePendingImageRollouts(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(executedJobs) != 2 {
		t.Fatalf("expected 2 executed jobs, got %v", executedJobs)
	}
	if executedJobs[0] != "webhooks-router" || executedJobs[1] != "hangar" {
		t.Fatalf("expected webhooks-router first and hangar last, got %v", executedJobs)
	}
}

func TestHandleImageReadyEvent_CachesDigestInNomadKnownDigests(t *testing.T) {
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	job := `job "test-caching" {
		group "test-caching" {
			task "test-caching" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/test-caching:latest"
				}
			}
		}
	}`
	_ = os.WriteFile(filepath.Join(jobsDir, "test-caching.nomad"), []byte(job), 0644)

	expectedDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	evtCh := make(chan HangarDeployEvent, 10)

	mockRegistryClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Docker-Content-Digest", expectedDigest)
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
		RegistryClient: mockRegistryClient,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: 12345"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			if evt.Event == "deploy_started" {
				evtCh <- evt
			}
			return nil
		},
	})

	_, code := d.ExecuteImageReadyEvent(context.Background(), ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/test-caching:latest",
		Repo:      "azylman/aerial",
		CommitSHA: "testsha",
		PRNumber:  123,
	})

	if code != http.StatusOK {
		t.Fatalf("expected HTTP 200 from ExecuteImageReadyEvent, got %d", code)
	}

	d.nomadMu.Lock()
	cachedDigest := d.nomadKnownDigests["test-caching:ghcr.io/azylman/test-caching:latest"]
	d.nomadMu.Unlock()

	if cachedDigest != expectedDigest {
		t.Errorf("expected cached digest %q, got %q", expectedDigest, cachedDigest)
	}

	var dispatchedEvt HangarDeployEvent
	select {
	case dispatchedEvt = <-evtCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for deploy_started event dispatch")
	}

	if dispatchedEvt.Digest != expectedDigest {
		t.Errorf("expected dispatched event digest %q, got %q", expectedDigest, dispatchedEvt.Digest)
	}
}

func TestStartReconcilerLoop_NilChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &SyncDaemon{}
	d.StartReconcilerLoop(ctx)
	if d.reconcileCh == nil {
		t.Errorf("expected reconcileCh to be initialized")
	}
}


func TestRunGitCommand_WithDir(t *testing.T) {
	dir := t.TempDir()
	stdout, _, err := runGitCommand(context.Background(), dir, "", "init")
	if err != nil {
		t.Logf("runGitCommand init: %v, stdout: %s", err, string(stdout))
	}
}

func TestDefaultNomadExecutor_AddrAndToken(t *testing.T) {
	execNoAddrTok := defaultNomadExecutor("", "")
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _ = execNoAddrTok(cancelCtx, "version")

	execAddrTok := defaultNomadExecutor("http://127.0.0.1:4646", "secret-token")
	cancelCtx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	_, _, _ = execAddrTok(cancelCtx2, "version")
}

func TestGetDeployDispatcher_Branches(t *testing.T) {
	var nilDaemon *SyncDaemon
	fnNil := nilDaemon.getDeployDispatcher()
	if fnNil == nil {
		t.Fatalf("expected non-nil dispatcher function for nil daemon")
	}
	if err := fnNil(context.Background(), HangarDeployEvent{}); err == nil {
		t.Errorf("expected error from deploy dispatcher on nil daemon")
	}

	d := &SyncDaemon{}
	fn := d.getDeployDispatcher()
	if fn == nil {
		t.Fatalf("expected non-nil dispatcher function when deployDispatcher is nil")
	}
	if err := fn(context.Background(), HangarDeployEvent{}); err == nil {
		t.Errorf("expected error from deploy dispatcher when deployDispatcher is nil")
	}

	customCalled := false
	d.deployDispatcher = func(ctx context.Context, evt HangarDeployEvent) error {
		customCalled = true
		return nil
	}
	fnCustom := d.getDeployDispatcher()
	if err := fnCustom(context.Background(), HangarDeployEvent{}); err != nil {
		t.Errorf("unexpected error from custom deploy dispatcher: %v", err)
	}
	if !customCalled {
		t.Errorf("expected custom deploy dispatcher to be called")
	}
}

func TestGetRegistryClient_NilDaemon(t *testing.T) {
	var nilDaemon *SyncDaemon
	client := nilDaemon.getRegistryClient()
	if client == nil {
		t.Errorf("expected non-nil default client for nil daemon")
	}
}

func TestDefaultDeployDispatcher_InvalidURL(t *testing.T) {
	d := &SyncDaemon{
		webhooksRouterURL: "://invalid-router-url",
	}
	err := d.defaultDeployDispatcher(context.Background(), HangarDeployEvent{})
	if err == nil {
		t.Errorf("expected error for invalid router URL")
	}
}

func TestRunGitCommand_WithPat(t *testing.T) {
	stdout, _, err := runGitCommand(context.Background(), "", "mock-pat", "version")
	if err != nil {
		t.Logf("runGitCommand version: %v, stdout: %s", err, string(stdout))
	}
}

func TestNomadChanges_CancelledContext(t *testing.T) {
	cancelCtx, cancel := context.WithCancel(context.Background())
	dFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			cancel()
			return nil, nil, errors.New("git fail")
		},
	})
	hasCfg, errCfg := dFail.HasNomadConfigChanges(cancelCtx, "/repo", "c1", "c2")
	if !errors.Is(errCfg, context.Canceled) || hasCfg {
		t.Errorf("expected context.Canceled from HasNomadConfigChanges, got %v, %v", hasCfg, errCfg)
	}

	cancelCtx2, cancel2 := context.WithCancel(context.Background())
	dFail2 := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			cancel2()
			return nil, nil, errors.New("git fail")
		},
	})
	changes, errChanges := dFail2.HasNomadChanges(cancelCtx2, "/repo", "c1", "c2")
	if !errors.Is(errChanges, context.Canceled) || changes != nil {
		t.Errorf("expected context.Canceled from HasNomadChanges, got %v, %v", changes, errChanges)
	}
}

func TestNomadChanges_FallbackCancelledContext(t *testing.T) {
	cancelCtx, cancel := context.WithCancel(context.Background())
	callCount := 0
	dFail := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			callCount++
			if callCount == 2 {
				cancel()
			}
			return nil, nil, errors.New("git fail")
		},
	})
	hasCfg, errCfg := dFail.HasNomadConfigChanges(cancelCtx, "/repo", "c1", "c2")
	if !errors.Is(errCfg, context.Canceled) || hasCfg {
		t.Errorf("expected context.Canceled from HasNomadConfigChanges fallback, got %v, %v", hasCfg, errCfg)
	}

	cancelCtx2, cancel2 := context.WithCancel(context.Background())
	callCount2 := 0
	dFail2 := NewDaemon(DaemonConfig{
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			callCount2++
			if callCount2 == 2 {
				cancel2()
			}
			return nil, nil, errors.New("git fail")
		},
	})
	changes, errChanges := dFail2.HasNomadChanges(cancelCtx2, "/repo", "c1", "c2")
	if !errors.Is(errChanges, context.Canceled) || changes != nil {
		t.Errorf("expected context.Canceled from HasNomadChanges fallback, got %v, %v", changes, errChanges)
	}
}

func TestCoverageBoosters(t *testing.T) {
	t.Parallel()

	// 1. ResolveGitBinInternal default fileExists fallback using os.Stat
	tmpDir := t.TempDir()
	dummyGit := filepath.Join(tmpDir, "git.exe")
	if err := os.WriteFile(dummyGit, []byte("echo"), 0755); err != nil {
		t.Fatalf("failed creating dummy git: %v", err)
	}
	res := resolveGitBinInternal(
		func(k string) string {
			if k == "ProgramFiles" {
				return tmpDir
			}
			return ""
		},
		nil, // nil fileExists -> falls back to os.Stat
		func(string) (string, error) { return "", errors.New("not found") },
		"windows",
	)
	_ = res

	// 2. ParseImageReference edge cases
	reg, repo, tag, err := ParseImageReference("docker.io/myimage:v1")
	if err != nil || reg != "registry-1.docker.io" || repo != "library/myimage" || tag != "v1" {
		t.Errorf("unexpected ParseImageReference result: %s, %s, %s, %v", reg, repo, tag, err)
	}
	reg, repo, tag, err = ParseImageReference("index.docker.io/owner/image:latest")
	if err != nil || reg != "registry-1.docker.io" || repo != "owner/image" || tag != "latest" {
		t.Errorf("unexpected ParseImageReference result: %s, %s, %s, %v", reg, repo, tag, err)
	}
	_, _, _, err = ParseImageReference("myregistry.com/:v1")
	if err == nil {
		t.Errorf("expected error for empty repository, got nil")
	}

	// 3. ParseWwwAuthenticate with trailing param without '='
	_, svc, _, pErr := ParseWwwAuthenticate("Bearer realm=\"https://example.com\",service=\"test\",trailingnoparam")
	if pErr != nil || svc != "test" {
		t.Errorf("unexpected ParseWwwAuthenticate error: %v, svc=%s", pErr, svc)
	}

	// 4. ContainsDigest with 2-part @ split
	if !ContainsDigest([]string{"repo/img@sha256:1234567890abcdef"}, "sha256:1234567890abcdef") {
		t.Errorf("expected ContainsDigest to match")
	}

	// 5. ResolveRepoPath suffix match
	resolved := ResolveRepoPath("aerial", []string{"/mnt/data/supervisor/share/aerial"})
	if resolved != "/mnt/data/supervisor/share/aerial" {
		t.Errorf("expected /mnt/data/supervisor/share/aerial, got %s", resolved)
	}

	// 6. defaultDeployDispatcher error status code
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal dispatcher error"))
	}))
	defer srv.Close()

	dDisp := &SyncDaemon{webhooksRouterURL: srv.URL}
	if err := dDisp.defaultDeployDispatcher(context.Background(), HangarDeployEvent{JobName: "test"}); err == nil {
		t.Errorf("expected error from dispatcher, got nil")
	}

	// 7. resolveGitDir relative path and non-gitdir content
	gitDirFile := filepath.Join(tmpDir, "repo-rel")
	_ = os.MkdirAll(gitDirFile, 0755)
	_ = os.WriteFile(filepath.Join(gitDirFile, ".git"), []byte("gitdir: relative/git\n"), 0644)
	resGit, errGit := resolveGitDir(gitDirFile)
	if errGit != nil || !strings.HasSuffix(resGit, "relative/git") {
		t.Errorf("unexpected resolveGitDir with relative target: %s, %v", resGit, errGit)
	}

	gitDirNon := filepath.Join(tmpDir, "repo-non")
	_ = os.MkdirAll(gitDirNon, 0755)
	_ = os.WriteFile(filepath.Join(gitDirNon, ".git"), []byte("something else\n"), 0644)
	resGitNon, errGitNon := resolveGitDir(gitDirNon)
	if errGitNon != nil || resGitNon != filepath.Join(gitDirNon, ".git") {
		t.Errorf("unexpected resolveGitDir with non-gitdir content: %s, %v", resGitNon, errGitNon)
	}

	// 8. getGitExecutor default execution
	dGit := &SyncDaemon{pat: "dummy-pat"}
	execFn := dGit.getGitExecutor()
	if execFn != nil {
		_, _, _ = execFn(context.Background(), tmpDir, "version")
	}

	// 9. postChannelMessage and sendWebhook with mock error server
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srvErr.Close()

	dHooks := &SyncDaemon{}
	dHooks.sendWebhook(context.Background(), srvErr.Client(), srvErr.URL, "test content")
	dHooks.postChannelMessage(context.Background(), srvErr.Client(), "secret-token", "12345", "test message")
}


func TestGetRemoteImageRevision_TableDriven(t *testing.T) {
	expectedSHA := "0123456789abcdef0123456789abcdef01234567"

	t.Run("invalid image reference returns error", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		_, err := d.GetRemoteImageRevision(context.Background(), "invalid reference with spaces")
		if err == nil {
			t.Fatalf("expected error for invalid reference, got nil")
		}
	})

	t.Run("Case 1: OCI Index to Child Manifest to Config Blob with org.opencontainers.image.revision", func(t *testing.T) {
		childManifestDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		configBlobDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

		indexJSON := fmt.Sprintf(`{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.index.v1+json",
			"manifests": [
				{
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"digest": "%s",
					"size": 1024,
					"platform": { "architecture": "amd64", "os": "linux" }
				}
			]
		}`, childManifestDigest)

		manifestJSON := fmt.Sprintf(`{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"config": {
				"mediaType": "application/vnd.oci.image.config.v1+json",
				"digest": "%s",
				"size": 512
			},
			"layers": []
		}`, configBlobDigest)

		configJSON := fmt.Sprintf(`{
			"config": {
				"Labels": {
					"org.opencontainers.image.revision": "%s"
				}
			}
		}`, expectedSHA)

		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.Method != http.MethodGet {
						t.Errorf("expected GET request, got %s", req.Method)
					}
					urlStr := req.URL.String()
					switch {
					case strings.HasSuffix(urlStr, "/manifests/latest"):
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(indexJSON)),
						}, nil
					case strings.HasSuffix(urlStr, "/manifests/"+childManifestDigest):
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(manifestJSON)),
						}, nil
					case strings.HasSuffix(urlStr, "/blobs/"+configBlobDigest):
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(configJSON)),
						}, nil
					default:
						t.Errorf("unexpected request URL: %s", urlStr)
						return &http.Response{
							StatusCode: http.StatusNotFound,
							Body:       ioNopCloser(strings.NewReader("not found")),
						}, nil
					}
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		rev, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rev != expectedSHA {
			t.Errorf("got %q, want %q", rev, expectedSHA)
		}
	})

	t.Run("Case 2: Direct Manifest with aerial.commit_sha in annotations without fetching blob", func(t *testing.T) {
		manifestWithAnnotations := fmt.Sprintf(`{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"config": {
				"digest": "sha256:unusedblobdigest33333333333333333333333333333333333333333333333333"
			},
			"annotations": {
				"aerial.commit_sha": "%s"
			}
		}`, expectedSHA)

		blobFetched := false
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "/blobs/") {
						blobFetched = true
						t.Errorf("unexpected fetch to config blob when manifest has annotations")
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(manifestWithAnnotations)),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		rev, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-webhooks-router:latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rev != expectedSHA {
			t.Errorf("got %q, want %q", rev, expectedSHA)
		}
		if blobFetched {
			t.Errorf("config blob was fetched unexpectedly")
		}
	})

	t.Run("Case 3: 401 Www-Authenticate challenge + token exchange + config blob", func(t *testing.T) {
		configBlobDigest := "sha256:4444444444444444444444444444444444444444444444444444444444444444"
		manifestJSON := fmt.Sprintf(`{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"config": {
				"digest": "%s"
			}
		}`, configBlobDigest)

		configJSON := fmt.Sprintf(`{
			"config": {
				"Labels": {
					"org.label-schema.vcs-ref": "%s"
				}
			}
		}`, expectedSHA)

		var tokenRequested bool
		var authManifestDone bool
		var authBlobDone bool

		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					// 1. Initial unauthenticated request gets 401
					if req.Header.Get("Authorization") == "" && strings.Contains(req.URL.Path, "/manifests/") {
						header := make(http.Header)
						header.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}

					// 2. Token request
					if req.URL.Host == "auth.docker.io" {
						tokenRequested = true
						if req.URL.Query().Get("scope") != "repository:library/redis:pull" {
							t.Errorf("unexpected token scope: %s", req.URL.Query().Get("scope"))
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"token":"secret-bearer-token"}`)),
						}, nil
					}

					// 3. Authenticated manifest request
					if strings.Contains(req.URL.Path, "/manifests/") {
						if req.Header.Get("Authorization") != "Bearer secret-bearer-token" {
							t.Errorf("missing or invalid authorization header on manifest request: %s", req.Header.Get("Authorization"))
						}
						authManifestDone = true
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(manifestJSON)),
						}, nil
					}

					// 4. Authenticated blob request
					if strings.Contains(req.URL.Path, "/blobs/") {
						if req.Header.Get("Authorization") != "Bearer secret-bearer-token" {
							t.Errorf("missing or invalid authorization header on blob request: %s", req.Header.Get("Authorization"))
						}
						authBlobDone = true
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(configJSON)),
						}, nil
					}

					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Body:       ioNopCloser(strings.NewReader("unknown endpoint")),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		rev, err := d.GetRemoteImageRevision(context.Background(), "redis:latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rev != expectedSHA {
			t.Errorf("got %q, want %q", rev, expectedSHA)
		}
		if !tokenRequested || !authManifestDone || !authBlobDone {
			t.Errorf("expected complete auth flow, got token=%v manifest=%v blob=%v", tokenRequested, authManifestDone, authBlobDone)
		}
	})

	t.Run("Case 4: Missing labels returns error", func(t *testing.T) {
		configBlobDigest := "sha256:5555555555555555555555555555555555555555555555555555555555555555"
		manifestJSON := fmt.Sprintf(`{
			"schemaVersion": 2,
			"config": { "digest": "%s" }
		}`, configBlobDigest)

		configJSON := `{ "config": { "Labels": {} } }`

		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "/manifests/") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(manifestJSON)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(configJSON)),
					}, nil
				},
			},
		}

		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		_, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest")
		if err == nil {
			t.Fatalf("expected error on missing labels, got nil")
		}
	})

	t.Run("Case 5: 500 error / network error returns error", func(t *testing.T) {
		mockClient500 := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("server internal error")),
					}, nil
				},
			},
		}
		d500 := NewDaemon(DaemonConfig{RegistryClient: mockClient500})
		if _, err := d500.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest"); err == nil {
			t.Fatalf("expected error on HTTP 500, got nil")
		}

		mockClientNetErr := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("network dial timeout")
				},
			},
		}
		dNetErr := NewDaemon(DaemonConfig{RegistryClient: mockClientNetErr})
		if _, err := dNetErr.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest"); err == nil {
			t.Fatalf("expected error on network error, got nil")
		}
	})
}

func TestNomadImagePoll_UsesImageRevision(t *testing.T) {
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		t.Fatalf("failed to create jobs dir: %v", err)
	}

	jobFile := filepath.Join(jobsDir, "webhooks-router.nomad")
	jobContent := `job "webhooks-router" { group "core" { task "router" { config { image = "ghcr.io/azylman/aerial-webhooks-router:latest" } } } }`
	if err := os.WriteFile(jobFile, []byte(jobContent), 0644); err != nil {
		t.Fatalf("failed to write job file: %v", err)
	}

	newDigest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"
	expectedImageRevision := "9876543210abcdef9876543210abcdef98765432"

	manifestWithAnnotations := fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"digest": "sha256:dummyblobdigest11111111111111111111111111111111111111111111111111"
		},
		"annotations": {
			"org.opencontainers.image.revision": "%s"
		}
	}`, expectedImageRevision)

	var restartCalled bool
	var restartedJob string

	mockNomad := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 4 && args[0] == "job" && args[1] == "restart" {
			restartCalled = true
			restartedJob = args[len(args)-1]
			return []byte("Job restart scheduled\n"), nil, nil
		}
		return nil, nil, nil
	}

	mockClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodHead {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", newDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				}
				if req.Method == http.MethodGet {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(manifestWithAnnotations)),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       ioNopCloser(strings.NewReader("bad request")),
				}, nil
			},
		},
	}

	evtCh := make(chan HangarDeployEvent, 10)
	d := NewDaemon(DaemonConfig{
		ConfigDir:      tmpDir,
		NomadExecutor:  mockNomad,
		RegistryClient: mockClient,
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("git_head_fallback_should_not_be_used\n"), nil, nil
			}
			return nil, nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
	})

	// Pre-seed known digest with older hash so new digest triggers rollout
	d.nomadKnownDigests["webhooks-router:ghcr.io/azylman/aerial-webhooks-router:latest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

	if err := d.CheckAndReconcileNomadImages(context.Background()); err != nil {
		t.Fatalf("CheckAndReconcileNomadImages returned error: %v", err)
	}

	if !restartCalled {
		t.Fatalf("expected nomad job restart to be called")
	}
	if restartedJob != "webhooks-router" {
		t.Errorf("restartedJob = %q, want webhooks-router", restartedJob)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "deploy_started" {
			t.Errorf("event = %q, want deploy_started", evt.Event)
		}
		if evt.JobName != "webhooks-router" {
			t.Errorf("job_name = %q, want webhooks-router", evt.JobName)
		}
		if evt.CommitSHA != expectedImageRevision {
			t.Errorf("commit_sha = %q, want %q (should resolve from image label, not git HEAD)", evt.CommitSHA, expectedImageRevision)
		}
		if evt.Repo != "azylman/aerial" {
			t.Errorf("repo = %q, want azylman/aerial (resolved from ImageSourceRepo)", evt.Repo)
		}
		if evt.Digest != newDigest {
			t.Errorf("digest = %q, want %q", evt.Digest, newDigest)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for deploy_started event")
	}
}


func TestGetRemoteImageRevision_ExtraErrorBranches(t *testing.T) {
	t.Run("invalid image reference returns error", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		if _, err := d.GetRemoteImageRevision(context.Background(), "invalid ref with spaces"); err == nil {
			t.Fatalf("expected error on invalid image ref, got nil")
		}
	})

	t.Run("child manifest request fails with non-200", func(t *testing.T) {
		indexJSON := `{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.index.v1+json",
			"manifests": [
				{
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"digest": "sha256:childdigest11111111111111111111111111111111111111111111111111111111",
					"platform": { "architecture": "amd64", "os": "linux" }
				}
			]
		}`
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "latest") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(indexJSON)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("child manifest not found")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if _, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest"); err == nil {
			t.Fatalf("expected error on child manifest 404, got nil")
		}
	})

	t.Run("manifest missing config digest and annotations returns error", func(t *testing.T) {
		manifestJSON := `{ "schemaVersion": 2 }`
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(manifestJSON)),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if _, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest"); err == nil {
			t.Fatalf("expected error on missing config digest, got nil")
		}
	})

	t.Run("blob fetch non-200 returns error", func(t *testing.T) {
		manifestJSON := `{
			"schemaVersion": 2,
			"config": { "digest": "sha256:blobdigest22222222222222222222222222222222222222222222222222222222" }
		}`
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "/manifests/") {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(manifestJSON)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("gateway error on blob")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if _, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial-brain:latest"); err == nil {
			t.Fatalf("expected error on blob 502, got nil")
		}
	})
}

func TestFetchRegistryWithAuth_ErrorBranches(t *testing.T) {
	t.Run("existing bearer returns 401 and recovers via challenge", func(t *testing.T) {
		step := 0
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					step++
					if step == 1 {
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull"`}},
							Body:       ioNopCloser(strings.NewReader("stale token")),
						}, nil
					}
					if req.URL.Host == "auth.docker.io" {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       ioNopCloser(strings.NewReader(`{"access_token":"refreshed-token"}`)),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("ok")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		resp, tok, err := d.fetchRegistryWithAuth(context.Background(), "https://registry-1.docker.io/v2/library/redis/manifests/latest", "registry-1.docker.io", "library/redis", "", "stale-bearer")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
		if tok != "refreshed-token" {
			t.Errorf("got token %q, want refreshed-token", tok)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("malformed Www-Authenticate header returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     http.Header{"Www-Authenticate": []string{"Basic realm=foo"}},
						Body:       ioNopCloser(strings.NewReader("unauthorized")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if r, _, err := d.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/foo/manifests/latest", "ghcr.io", "foo", "", ""); err == nil {
			if r != nil && r.Body != nil {
				r.Body.Close()
			}
			t.Fatalf("expected error on malformed auth header, got nil")
		}
	})

	t.Run("token request non-200 returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "manifests") {
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://ghcr.io/token",service="ghcr.io"`}},
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusForbidden,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("forbidden")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if r, _, err := d.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/foo/manifests/latest", "ghcr.io", "foo", "", ""); err == nil {
			if r != nil && r.Body != nil {
				r.Body.Close()
			}
			t.Fatalf("expected error on token 403, got nil")
		}
	})

	t.Run("token request malformed JSON returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "manifests") {
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://ghcr.io/token",service="ghcr.io"`}},
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("{invalid-json")),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if r, _, err := d.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/foo/manifests/latest", "ghcr.io", "foo", "", ""); err == nil {
			if r != nil && r.Body != nil {
				r.Body.Close()
			}
			t.Fatalf("expected error on malformed token json, got nil")
		}
	})

	t.Run("token request empty token returns error", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "manifests") {
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://ghcr.io/token",service="ghcr.io"`}},
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
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
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient})
		if r, _, err := d.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/foo/manifests/latest", "ghcr.io", "foo", "", ""); err == nil {
			if r != nil && r.Body != nil {
				r.Body.Close()
			}
			t.Fatalf("expected error on empty token, got nil")
		}
	})

	t.Run("ghcr.io token request sets Basic Auth when PAT is present", func(t *testing.T) {
		var basicAuthHeader string
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if strings.Contains(req.URL.Path, "manifests") {
						if req.Header.Get("Authorization") == "Bearer dummy-ghcr-token" {
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("ok")),
							}, nil
						}
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://ghcr.io/token",service="ghcr.io"`}},
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}
					basicAuthHeader = req.Header.Get("Authorization")
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"token":"dummy-ghcr-token"}`)),
					}, nil
				},
			},
		}
		d := NewDaemon(DaemonConfig{RegistryClient: mockClient, PAT: "my-secret-pat"})
		resp, tok, err := d.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/azylman/aerial/manifests/latest", "ghcr.io", "azylman/aerial", "", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
		if tok != "dummy-ghcr-token" {
			t.Errorf("got token %q, want dummy-ghcr-token", tok)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if !strings.HasPrefix(basicAuthHeader, "Basic ") {
			t.Errorf("expected Basic auth header on ghcr.io token request, got %q", basicAuthHeader)
		}
	})
}

func TestIsHangarRequest_Direct(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "nomad", "jobs")
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		t.Fatalf("failed creating jobs dir: %v", err)
	}
	hangarJobHCL := `
job "hangar" {
  group "hangar" {
    task "hangar" {
      config {
        image = "ghcr.io/custom/worker:v1"
      }
    }
  }
}
`
	if err := os.WriteFile(filepath.Join(jobsDir, "hangar.nomad"), []byte(hangarJobHCL), 0644); err != nil {
		t.Fatalf("failed writing job file: %v", err)
	}

	d := &SyncDaemon{
		repos: []string{tmpDir},
	}

	// 1. Direct hangar image match
	if !d.isHangarRequest(ImageReadyEventRequest{Image: "ghcr.io/azylman/aerial/hangar:latest"}) {
		t.Fatal("expected isHangarRequest true for hangar image")
	}

	// 2. Matching nomad job that is a hangar job
	if !d.isHangarRequest(ImageReadyEventRequest{Image: "ghcr.io/custom/worker:v1"}) {
		t.Fatal("expected isHangarRequest true for job named hangar")
	}

	// 3. Unrelated image
	if d.isHangarRequest(ImageReadyEventRequest{Image: "ghcr.io/custom/other:v1"}) {
		t.Fatal("expected isHangarRequest false for unrelated image")
	}
}

func TestGetRemoteImageRevision_IndexWithAnnotations(t *testing.T) {
	t.Parallel()
	mockClient := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/vnd.oci.image.index.v1+json"}},
					Body: ioNopCloser(strings.NewReader(`{
						"schemaVersion": 2,
						"manifests": [
							{
								"digest": "sha256:child111111111111111111111111111111111111111111111111111111111111",
								"platform": {"os": "linux", "architecture": "amd64"}
							}
						],
						"annotations": {
							"org.opencontainers.image.revision": "1234567890abcdef1234567890abcdef12345678"
						}
					}`)),
				}, nil
			},
		},
	}

	d := &SyncDaemon{
		registryClient: mockClient,
	}

	rev, err := d.GetRemoteImageRevision(context.Background(), "ghcr.io/azylman/aerial:latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rev != "1234567890abcdef1234567890abcdef12345678" {
		t.Fatalf("expected revision, got %s", rev)
	}
}

func TestFetchRegistryWithAuth_RealmAndEmptyTokenErrors(t *testing.T) {
	t.Parallel()

	// 1. Invalid realm
	mockClientInvalidRealm := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnauthorized,
					Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="://invalid-realm-url"`}},
					Body:       ioNopCloser(strings.NewReader("unauthorized")),
				}, nil
			},
		},
	}

	d1 := &SyncDaemon{registryClient: mockClientInvalidRealm}
	resp1, _, err1 := d1.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/repo/manifests/latest", "ghcr.io", "repo", "", "")
	if resp1 != nil && resp1.Body != nil {
		_ = resp1.Body.Close()
	}
	if err1 == nil || !strings.Contains(err1.Error(), "failed parsing token realm") {
		t.Fatalf("expected failed parsing token realm error, got %v", err1)
	}

	// 2. Token endpoint returns empty token
	mockClientEmptyToken := &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "manifests") {
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     http.Header{"Www-Authenticate": []string{`Bearer realm="https://ghcr.io/token",service="ghcr.io"`}},
						Body:       ioNopCloser(strings.NewReader("unauthorized")),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       ioNopCloser(strings.NewReader(`{}`)),
				}, nil
			},
		},
	}

	d2 := &SyncDaemon{registryClient: mockClientEmptyToken}
	resp2, _, err2 := d2.fetchRegistryWithAuth(context.Background(), "https://ghcr.io/v2/repo/manifests/latest", "ghcr.io", "repo", "", "")
	if resp2 != nil && resp2.Body != nil {
		_ = resp2.Body.Close()
	}
	if err2 == nil || !strings.Contains(err2.Error(), "empty bearer token returned") {
		t.Fatalf("expected empty bearer token error, got %v", err2)
	}
}

func TestDefaultNomadExecutor_ExtraBranches(t *testing.T) {
	t.Parallel()
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	execFn := defaultNomadExecutor("http://127.0.0.1:4646", "dummy-secret-token")
	_, _, _ = execFn(cancelCtx, "status")
	_, _, _ = defaultDockerExecutor(cancelCtx, "version")
	_, _, _ = defaultComposeExecutor(cancelCtx, "", "version")
	_, _, _ = runGitCommand(cancelCtx, "", "", "version")
}

func TestSyncConfigsToNomad_WithSyncContextAndDeployStarted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	configDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte("model: \"test\"\nchannels:\n  default:\n    mode: threads\n"), 0644)
	if err := os.MkdirAll(filepath.Join(configDir, "services", "homepage"), 0755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(configDir, "services", "homepage", "homepage.yaml"), []byte("title: \"Test\"\n"), 0644)
	if err := os.MkdirAll(filepath.Join(configDir, "services", "mcp"), 0755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(configDir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: 8080\n"), 0644)

	var (
		mu           sync.Mutex
		capturedArgs [][]string
		events       []HangarDeployEvent
	)

	d := &SyncDaemon{
		nomadAddr:        "http://127.0.0.1:4646",
		lastPushedConfig: make(map[string]string),
		nomadExecutor: func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			capturedArgs = append(capturedArgs, args)
			mu.Unlock()
			return []byte("ok"), nil, nil
		},
		deployDispatcher: func(dispCtx context.Context, evt HangarDeployEvent) error {
			mu.Lock()
			events = append(events, evt)
			mu.Unlock()
			return nil
		},
	}

	sc := ConfigSyncContext{
		Repo:      "azylman/aerial-config",
		CommitSHA: "sha_cfg_123",
		PRNumber:  101,
		TargetID:  "1555405874565091380",
	}

	// 1. SyncBrainConfigToNomad with sc
	if err := d.SyncBrainConfigToNomad(ctx, configDir, sc); err != nil {
		t.Fatalf("SyncBrainConfigToNomad error: %v", err)
	}

	// 2. SyncHomepageConfigToNomad with sc
	if err := d.SyncHomepageConfigToNomad(ctx, configDir, sc); err != nil {
		t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
	}

	// 3. SyncServiceConfigsToNomad with sc
	if err := d.SyncServiceConfigsToNomad(ctx, configDir, sc); err != nil {
		t.Fatalf("SyncServiceConfigsToNomad error: %v", err)
	}

	// Give goroutines time to invoke deployDispatcher
	var capturedEvents []HangarDeployEvent
	for i := 0; i < 50; i++ {
		mu.Lock()
		count := len(events)
		capturedEvents = append([]HangarDeployEvent(nil), events...)
		mu.Unlock()
		if count >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(capturedArgs) < 3 {
		t.Fatalf("expected at least 3 calls to nomadExecutor, got %d", len(capturedArgs))
	}

	// Verify COMMIT_SHA was passed in all calls
	for i, args := range capturedArgs {
		hasCommit := false
		for _, arg := range args {
			if arg == "COMMIT_SHA=sha_cfg_123" {
				hasCommit = true
				break
			}
		}
		if !hasCommit {
			t.Errorf("call %d missing COMMIT_SHA arg: %v", i, args)
		}
	}

	// Verify events
	foundBrain := false
	foundHomepage := false
	foundScheduler := false
	for _, evt := range capturedEvents {
		if evt.Event != "deploy_started" || evt.Repo != sc.Repo || evt.CommitSHA != sc.CommitSHA || evt.PRNumber != sc.PRNumber || evt.TargetID != sc.TargetID {
			t.Errorf("unexpected event payload: %+v", evt)
		}
		switch evt.JobName {
		case "brain":
			foundBrain = true
		case "homepage":
			foundHomepage = true
		case "scheduler-mcp":
			foundScheduler = true
		}
	}

	if !foundBrain || !foundHomepage || !foundScheduler {
		t.Errorf("missing expected deploy_started events: brain=%v, homepage=%v, scheduler=%v (events: %+v)",
			foundBrain, foundHomepage, foundScheduler, capturedEvents)
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	mux := SetupMux(&SyncDaemon{})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. Without GOCOVERDIR -> 200 OK
	orig := os.Getenv("GOCOVERDIR")
	_ = os.Unsetenv("GOCOVERDIR")
	defer func() {
		if orig != "" {
			_ = os.Setenv("GOCOVERDIR", orig)
		} else {
			_ = os.Unsetenv("GOCOVERDIR")
		}
	}()

	resp, err := http.Get(ts.URL + "/debug/coverage/flush")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK without GOCOVERDIR, got: %d", resp.StatusCode)
	}

	// 2. With GOCOVERDIR + mock success -> 200 OK
	oldFn := writeCountersDirFn
	defer func() { writeCountersDirFn = oldFn }()

	called := false
	writeCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	_ = os.Setenv("GOCOVERDIR", t.TempDir())

	resp2, err2 := http.Get(ts.URL + "/debug/coverage/flush")
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK with GOCOVERDIR, got: %d", resp2.StatusCode)
	}
	if !called {
		t.Errorf("expected writeCountersDirFn to be called")
	}

	// 3. With GOCOVERDIR + mock error -> 500 Internal Server Error
	writeCountersDirFn = func(dir string) error {
		return errors.New("simulated flush error")
	}

	resp3, err3 := http.Get(ts.URL + "/debug/coverage/flush")
	if err3 != nil {
		t.Fatalf("unexpected error: %v", err3)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on error, got: %d", resp3.StatusCode)
	}
}



func TestExecuteImageReadyEvent_VariableTagInjection(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobContent := `variable "image_tag" {
  type    = string
  default = "latest"
}

job "router" {
  group "router" {
    task "router" {
      driver = "docker"
      config {
        image = "ghcr.io/azylman/aerial-webhooks-router:${var.image_tag}"
      }
    }
  }
}`
	routerPath := filepath.Join(jobsDir, "router.nomad")
	_ = os.WriteFile(routerPath, []byte(jobContent), 0644)

	var capturedArgs []string
	var restartCalled bool
	mockDispatcher := func(ctx context.Context, evt HangarDeployEvent) error {
		return nil
	}

	d := NewDaemon(DaemonConfig{
		ConfigDir:        tmpDir,
		ComposeDir:       tmpDir,
		DeployDispatcher: mockDispatcher,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				capturedArgs = append([]string(nil), args...)
				return []byte("Evaluation ID: run-args-123"), nil, nil
			}
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				restartCalled = true
				return []byte("Restart triggered"), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Repo:      "azylman/aerial",
		CommitSHA: "sha12345",
		PRNumber:  615,
		TargetID:  "discord_thread_999",
	})

	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
	}

	if restartCalled {
		t.Fatalf("expected nomad job restart NOT to be called; double-tap must be eliminated")
	}

	wantArgs := []string{"job", "run", "-detach", "-var=image_tag=sha12345", routerPath}
	if len(capturedArgs) != len(wantArgs) {
		t.Fatalf("capturedArgs = %v, want %v", capturedArgs, wantArgs)
	}
	for i := range wantArgs {
		if capturedArgs[i] != wantArgs[i] {
			t.Errorf("capturedArgs[%d] = %q, want %q", i, capturedArgs[i], wantArgs[i])
		}
	}

	// Sub-test 2: Fallback to digest when CommitSHA is empty
	capturedArgs = nil
	restartCalled = false
	resp, code = d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest:   "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Repo:     "azylman/aerial",
		PRNumber: 615,
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted for digest fallback, got %d (%s)", code, resp.Status)
	}
	if restartCalled {
		t.Fatalf("expected nomad job restart NOT to be called for parameterized job")
	}
	wantDigestArg := "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	foundDigest := false
	for _, a := range capturedArgs {
		if a == wantDigestArg {
			foundDigest = true
			break
		}
	}
	if !foundDigest {
		t.Fatalf("expected args to contain %q, got %v", wantDigestArg, capturedArgs)
	}

	// Sub-test 3: Unparameterized legacy job triggers job restart fallback
	legacyContent := `job "legacy" {
  task "legacy" {
    config {
      image = "ghcr.io/azylman/legacy-app:latest"
    }
  }
}`
	legacyPath := filepath.Join(jobsDir, "legacy.nomad")
	_ = os.WriteFile(legacyPath, []byte(legacyContent), 0644)

	capturedArgs = nil
	restartCalled = false
	resp, code = d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/legacy-app:latest",
		CommitSHA: "legacy123",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted for legacy app, got %d (%s)", code, resp.Status)
	}
	if !restartCalled {
		t.Fatalf("expected nomad job restart to be called for unparameterized legacy job")
	}
	for _, a := range capturedArgs {
		if strings.HasPrefix(a, "-var=image_tag=") {
			t.Fatalf("expected no -var=image_tag passed to unparameterized job, got %v", capturedArgs)
		}
	}

	// Sub-test 4: Empty CommitSHA and empty Digest falls back to latest
	capturedArgs = nil
	// Sub-test 4: Empty CommitSHA and empty Digest falls back to latest when remote digest cannot be resolved
	capturedArgs = nil
	restartCalled = false
	dNoDigest := NewDaemon(DaemonConfig{
		ConfigDir:        tmpDir,
		ComposeDir:       tmpDir,
		DeployDispatcher: mockDispatcher,
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader("not found")),
					}, nil
				},
			},
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				capturedArgs = append([]string(nil), args...)
				return []byte("Evaluation ID: run-latest"), nil, nil
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dNoDigest.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
		Repo:     "azylman/aerial",
		PRNumber: 615,
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted for empty sha/digest fallback, got %d (%s)", code, resp.Status)
	}
	wantLatestArg := "-var=image_tag=latest"
	foundLatest := false
	for _, a := range capturedArgs {
		if a == wantLatestArg {
			foundLatest = true
			break
		}
	}
	if !foundLatest {
		t.Fatalf("expected args to contain %q, got %v", wantLatestArg, capturedArgs)
	}

	// Sub-test 5: Digest without sha256: prefix gets normalized
	capturedArgs = nil
	resp, code = d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
		Digest:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Repo:     "azylman/aerial",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted for unadorned digest, got %d (%s)", code, resp.Status)
	}
	foundCleanDigest := false
	for _, a := range capturedArgs {
		if a == wantDigestArg {
			foundCleanDigest = true
			break
		}
	}
	if !foundCleanDigest {
		t.Fatalf("expected args to contain %q with prepended sha256, got %v", wantDigestArg, capturedArgs)
	}

	// Sub-test 6: Unparameterized legacy job with restart error logs warning and succeeds
	dRestartFail := NewDaemon(DaemonConfig{
		ConfigDir:        tmpDir,
		ComposeDir:       tmpDir,
		DeployDispatcher: mockDispatcher,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
				return nil, []byte("restart failed warning"), errors.New("exit status 1")
			}
			return []byte("OK"), nil, nil
		},
	})
	resp, code = dRestartFail.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/legacy-app:latest",
	})
	if code != http.StatusOK || resp.Status != "accepted" {
		t.Fatalf("expected 200 accepted even if restart fails warning, got %d (%s)", code, resp.Status)
	}
}

func TestExecuteImageReadyEvent_NonZeroExitCodeDoesNotSwallowError(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobContent := `job "failing-job" {
  task "t" {
    config {
      image = "ghcr.io/azylman/aerial-broken:latest"
    }
  }
}`
	_ = os.WriteFile(filepath.Join(jobsDir, "failing.nomad"), []byte(jobContent), 0644)

	evtCh := make(chan HangarDeployEvent, 10)

	d := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
				return nil, []byte("Nomad agent connection refused"), errors.New("exit status 1")
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := d.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image:     "ghcr.io/azylman/aerial-broken:latest",
		Digest:    "sha256:abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
		Repo:      "azylman/aerial",
		CommitSHA: "broken_sha",
		PRNumber:  999,
		TargetID:  "thread_broken",
	})

	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Fatalf("expected 500 error when nomad job run fails, got %d (%+v)", code, resp)
	}

	if len(resp.MatchedJobs) != 0 {
		t.Errorf("expected 0 applied matched jobs on failure, got %v", resp.MatchedJobs)
	}

	var sawFailed bool
	timer := time.After(2 * time.Second)
	for !sawFailed {
		select {
		case evt := <-evtCh:
			if evt.Event == "deploy_failed" {
				sawFailed = true
				if evt.Repo != "azylman/aerial" || evt.CommitSHA != "broken_sha" || evt.PRNumber != 999 || evt.TargetID != "thread_broken" {
					t.Errorf("metadata missing in deploy_failed event: %+v", evt)
				}
			}
		case <-timer:
			t.Fatalf("timed out waiting for deploy_failed event to be dispatched")
		}
	}
}

func TestReconcilePendingNomad_PipesPRMetadata(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "bot.nomad"), []byte(`job "bot" {}`), 0644)

	evtCh := make(chan HangarDeployEvent, 10)

	d := NewDaemon(DaemonConfig{
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			evtCh <- evt
			return nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: run-bot"), nil, nil
		},
	})

	d.recordPendingNomadChanges(tmpDir, []NomadFileChange{
		{
			Path:    "bot.nomad",
			Action:  "apply",
			JobName: "bot",
		},
	}, "azylman/aerial", "sha-pending-123", "thread-777", "616")

	if err := d.ReconcilePendingNomad(ctx); err != nil {
		t.Fatalf("ReconcilePendingNomad failed: %v", err)
	}

	select {
	case evt := <-evtCh:
		if evt.Event != "deploy_started" {
			t.Errorf("expected deploy_started, got %q", evt.Event)
		}
		if evt.Repo != "azylman/aerial" || evt.CommitSHA != "sha-pending-123" || evt.PRNumber != 616 || evt.TargetID != "thread-777" {
			t.Errorf("expected PR provenance in deploy_started, got %+v", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deploy_started from ReconcilePendingNomad")
	}
}

func TestExecuteImageReadyEvent_JobFileValidationAndReadErrors(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	jobPath := filepath.Join(jobsDir, "val-fail.nomad")
	_ = os.WriteFile(jobPath, []byte(`job "val-fail" {
  task "t" {
    config {
      image = "ghcr.io/azylman/val-fail:latest"
    }
  }
}`), 0644)

	// 1. ValidateNomadJob returns error
	dValFail := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
				return nil, []byte("syntax error in HCL"), errors.New("validate failed")
			}
			return []byte("OK"), nil, nil
		},
	})

	resp, code := dValFail.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/val-fail:latest",
	})
	if code != http.StatusInternalServerError || resp.Status != "error" {
		t.Fatalf("expected 500 error on validation failure, got %d (%+v)", code, resp)
	}
	if !strings.Contains(resp.Message, "validation failed") {
		t.Errorf("expected validation failed in message, got %q", resp.Message)
	}

	// 2. ReadFile returns error (file removed during lookup)
	jobPath2 := filepath.Join(jobsDir, "read-fail.nomad")
	_ = os.WriteFile(jobPath2, []byte(`job "read-fail" {
  task "t" {
    config {
      image = "ghcr.io/azylman/read-fail:latest"
    }
  }
}`), 0644)

	dReadFail := NewDaemon(DaemonConfig{
		ConfigDir: tmpDir,
		RegistryClient: &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					// Remove the job file before the loop reads it!
					_ = os.Remove(jobPath2)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(`{"token":"abc"}`)),
					}, nil
				},
			},
		},
	})

	resp2, code2 := dReadFail.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
		Image: "ghcr.io/azylman/read-fail:latest",
	})
	if code2 != http.StatusInternalServerError || resp2.Status != "error" {
		t.Fatalf("expected 500 error on read failure, got %d (%+v)", code2, resp2)
	}
	if !strings.Contains(resp2.Message, "read error") {
		t.Errorf("expected read error in message, got %q", resp2.Message)
	}
}

func TestExecuteGitPushEvent_ConfigSyncNotices(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(configDir, ".git"), 0755)

	d := NewDaemon(DaemonConfig{
		ConfigDir: configDir,
		Repos:     []string{configDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "log" {
				return []byte("sha123\x002026-10-05T00:00:00Z"), nil, nil
			}
			if len(args) > 0 && args[0] == "pull" {
				return []byte("Already up to date."), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "var" && args[1] == "put" {
				return nil, []byte("nomad var put permission denied"), errors.New("var put failed")
			}
			return []byte(""), nil, nil
		},
	})

	req := GitPushEventRequest{
		Repo:   "azylman/aerial-config",
		Commit: "sha123",
	}
	resp, code := d.ExecuteGitPushEvent(ctx, req)
	if code != http.StatusOK {
		t.Fatalf("expected 200 OK even with config sync notice, got %d (%+v)", code, resp)
	}
}


func TestSyncBrainConfigToNomad_DiffGated(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(configDir, 0755)

	configPath := filepath.Join(configDir, "config.yaml")
	_ = os.WriteFile(configPath, []byte("channels:\n  default:\n    mode: mention\n"), 0644)

	var mu sync.Mutex
	var nomadCalls [][]string
	var events []HangarDeployEvent

	d := NewDaemon(DaemonConfig{
		NomadAddr: "http://127.0.0.1:4646",
		ConfigDir: configDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			nomadCalls = append(nomadCalls, args)
			mu.Unlock()
			return []byte("ok"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			mu.Lock()
			events = append(events, evt)
			mu.Unlock()
			return nil
		},
	})

	scUntouched := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_untouched",
		DiffGated:    true,
		ChangedFiles: []string{"README.md"},
	}

	// 1. Untouched file in diff-gated push: should NOT put var, but should prime cache
	if err := d.SyncBrainConfigToNomad(ctx, configDir, scUntouched); err != nil {
		t.Fatalf("SyncBrainConfigToNomad unexpected error: %v", err)
	}

	mu.Lock()
	if len(nomadCalls) != 0 {
		t.Fatalf("expected 0 nomad calls for untouched diff, got %d", len(nomadCalls))
	}
	d.lastPushedConfigMu.Lock()
	cached := d.lastPushedConfig["nomad/jobs/brain:CONFIG_YAML"]
	d.lastPushedConfigMu.Unlock()
	if cached == "" {
		t.Fatalf("expected cache to be primed for brain config")
	}
	mu.Unlock()

	// 2. Touched in diff, but identical content (double-filter): should NOT put var
	scSameContent := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_same",
		DiffGated:    true,
		ChangedFiles: []string{"config.yaml"},
	}
	if err := d.SyncBrainConfigToNomad(ctx, configDir, scSameContent); err != nil {
		t.Fatalf("SyncBrainConfigToNomad error: %v", err)
	}
	mu.Lock()
	if len(nomadCalls) != 0 {
		t.Fatalf("expected 0 nomad calls for identical content, got %d", len(nomadCalls))
	}
	mu.Unlock()

	// 3. Touched in diff with modified content: SHOULD put var and emit deploy_started
	_ = os.WriteFile(configPath, []byte("channels:\n  default:\n    mode: always\n"), 0644)
	scModified := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_modified",
		DiffGated:    true,
		ChangedFiles: []string{"config.yaml"},
	}
	if err := d.SyncBrainConfigToNomad(ctx, configDir, scModified); err != nil {
		t.Fatalf("SyncBrainConfigToNomad error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(nomadCalls) != 1 {
		t.Fatalf("expected 1 nomad call for modified config, got %d", len(nomadCalls))
	}
	if len(events) != 1 || events[0].JobName != "brain" || events[0].Event != "deploy_started" {
		t.Fatalf("expected deploy_started event for brain, got %+v", events)
	}
	mu.Unlock()
}

func TestSyncHomepageConfigToNomad_DiffGated(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(configDir, "services", "homepage"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "homepage"), 0755)

	hpYaml := filepath.Join(configDir, "services", "homepage", "homepage.yaml")
	_ = os.WriteFile(hpYaml, []byte("title: Homepage\n"), 0644)
	subYaml := filepath.Join(configDir, "homepage", "services.yaml")
	_ = os.WriteFile(subYaml, []byte("services:\n  - name: test\n"), 0644)

	var mu sync.Mutex
	var nomadCalls [][]string
	var events []HangarDeployEvent

	d := NewDaemon(DaemonConfig{
		NomadAddr: "http://127.0.0.1:4646",
		ConfigDir: configDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			nomadCalls = append(nomadCalls, args)
			mu.Unlock()
			return []byte("ok"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			mu.Lock()
			events = append(events, evt)
			mu.Unlock()
			return nil
		},
	})

	scUntouched := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_untouched",
		DiffGated:    true,
		ChangedFiles: []string{"services/voice/voice.yaml"},
	}

	// 1. Untouched in diff: should NOT put var, but should prime cache
	if err := d.SyncHomepageConfigToNomad(ctx, configDir, scUntouched); err != nil {
		t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
	}

	mu.Lock()
	if len(nomadCalls) != 0 {
		t.Fatalf("expected 0 nomad calls for untouched diff, got %d", len(nomadCalls))
	}
	d.lastPushedConfigMu.Lock()
	cached := d.lastPushedConfig["nomad/jobs/homepage"]
	d.lastPushedConfigMu.Unlock()
	if cached == "" {
		t.Fatalf("expected cache to be primed for homepage config")
	}
	mu.Unlock()

	// 2. Subfile in homepage/ directory touched: SHOULD put var
	_ = os.WriteFile(subYaml, []byte("services:\n  - name: test2\n"), 0644)
	scSubTouched := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_sub",
		DiffGated:    true,
		ChangedFiles: []string{"homepage/services.yaml"},
	}
	if err := d.SyncHomepageConfigToNomad(ctx, configDir, scSubTouched); err != nil {
		t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(nomadCalls) != 1 {
		t.Fatalf("expected 1 nomad call for modified homepage subfile, got %d", len(nomadCalls))
	}
	if len(events) != 1 || events[0].JobName != "homepage" {
		t.Fatalf("expected deploy_started event for homepage, got %+v", events)
	}
	mu.Unlock()
}

func TestSyncServiceConfigsToNomad_DiffGated(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(configDir, "services", "voice"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "services", "mcp"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "services", "hangar"), 0755)

	voiceYaml := filepath.Join(configDir, "services", "voice", "voice.yaml")
	_ = os.WriteFile(voiceYaml, []byte("model: small.en\n"), 0644)
	schedYaml := filepath.Join(configDir, "services", "mcp", "scheduler-mcp.yaml")
	_ = os.WriteFile(schedYaml, []byte("enabled: true\n"), 0644)

	hangarYaml := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
	hangarContent := `service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/orin-voice"
    var_key: "CONFIG_YAML"
  - rel_path: "services/mcp/scheduler-mcp.yaml"
    nomad_var: "nomad/jobs/scheduler-mcp"
    var_key: "CONFIG_YAML"
`
	_ = os.WriteFile(hangarYaml, []byte(hangarContent), 0644)

	var mu sync.Mutex
	var nomadCalls [][]string
	var events []HangarDeployEvent

	d := NewDaemon(DaemonConfig{
		NomadAddr: "http://127.0.0.1:4646",
		ConfigDir: configDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			mu.Lock()
			nomadCalls = append(nomadCalls, args)
			mu.Unlock()
			return []byte("ok"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			mu.Lock()
			events = append(events, evt)
			mu.Unlock()
			return nil
		},
	})

	// Scenario mimicking PR #264: ONLY services/hangar/hangar.yaml was touched in the git push!
	scHangarOnly := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_pr264",
		DiffGated:    true,
		ChangedFiles: []string{"services/hangar/hangar.yaml"},
	}

	if err := d.SyncServiceConfigsToNomad(ctx, configDir, scHangarOnly); err != nil {
		t.Fatalf("SyncServiceConfigsToNomad unexpected error: %v", err)
	}

	mu.Lock()
	if len(nomadCalls) != 0 {
		t.Fatalf("expected ZERO nomad calls for PR #264 hangar-only diff, got %d", len(nomadCalls))
	}
	if len(events) != 0 {
		t.Fatalf("expected ZERO deploy_started events (no phantom deploys), got %d", len(events))
	}
	d.lastPushedConfigMu.Lock()
	cachedVoice := d.lastPushedConfig["nomad/jobs/orin-voice:CONFIG_YAML"]
	cachedSched := d.lastPushedConfig["nomad/jobs/scheduler-mcp:CONFIG_YAML"]
	d.lastPushedConfigMu.Unlock()
	if cachedVoice == "" || cachedSched == "" {
		t.Fatalf("expected cache to be primed for untouched service configs")
	}
	mu.Unlock()

	// Now modify services/voice/voice.yaml and push: ONLY orin-voice should be pushed!
	_ = os.WriteFile(voiceYaml, []byte("model: distil-small.en\n"), 0644)
	scVoiceOnly := ConfigSyncContext{
		Repo:         "azylman/aerial-config",
		CommitSHA:    "sha_voice_update",
		DiffGated:    true,
		ChangedFiles: []string{"services/voice/voice.yaml"},
	}

	if err := d.SyncServiceConfigsToNomad(ctx, configDir, scVoiceOnly); err != nil {
		t.Fatalf("SyncServiceConfigsToNomad error: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(nomadCalls) != 1 {
		t.Fatalf("expected exactly 1 nomad call (only orin-voice), got %d", len(nomadCalls))
	}
	if len(events) != 1 || events[0].JobName != "orin-voice" {
		t.Fatalf("expected deploy_started event exclusively for orin-voice, got %+v", events)
	}
	mu.Unlock()
}

func TestExecuteGitPushEvent_AerialConfig_DiffGating(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "aerial-config")
	_ = os.MkdirAll(filepath.Join(configDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "services", "voice"), 0755)
	_ = os.MkdirAll(filepath.Join(configDir, "services", "hangar"), 0755)

	voiceYaml := filepath.Join(configDir, "services", "voice", "voice.yaml")
	_ = os.WriteFile(voiceYaml, []byte("model: small.en\n"), 0644)
	hangarYaml := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
	hangarContent := `service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/orin-voice"
    var_key: "CONFIG_YAML"
`
	_ = os.WriteFile(hangarYaml, []byte(hangarContent), 0644)

	var mu sync.Mutex
	var varPutJobs []string

	d := NewDaemon(DaemonConfig{
		NomadAddr: "http://127.0.0.1:4646",
		ConfigDir: configDir,
		Repos:     []string{configDir},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) > 0 && args[0] == "log" {
				return []byte("sha_curr\x002026-10-05T00:00:00Z"), nil, nil
			}
			if len(args) > 0 && args[0] == "rev-parse" {
				return []byte("sha_curr\n"), nil, nil
			}
			if len(args) > 0 && args[0] == "pull" {
				return []byte("Updating sha_prev..sha_curr"), nil, nil
			}
			if len(args) > 0 && args[0] == "diff" && args[1] == "--name-only" {
				return []byte("services/hangar/hangar.yaml\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 4 && args[0] == "var" && args[1] == "put" {
				mu.Lock()
				varPutJobs = append(varPutJobs, args[3])
				mu.Unlock()
			}
			return []byte("ok"), nil, nil
		},
	})

	req := GitPushEventRequest{
		Repo:   "azylman/aerial-config",
		Commit: "sha_curr",
	}

	resp, code := d.ExecuteGitPushEvent(ctx, req)
	if code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (%+v)", code, resp)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(varPutJobs) != 0 {
		t.Fatalf("expected zero var put calls for untouched service configs, got: %v", varPutJobs)
	}
}
