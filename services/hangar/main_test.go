package main

import (
	"context"
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

	"github.com/azylman/aerial/pkg/httpkit"
)

func TestSanitizeAndEnv_TableDriven(t *testing.T) {
	t.Run("SanitizeLog", func(t *testing.T) {
		tests := []struct {
			name, input, expected string
		}{
			{"clean string", "repository synced cleanly", "repository synced cleanly"},
			{"github pat", "failed with token github_pat_11AAAA_secret_token_123", "failed with token [REDACTED_TOKEN]"},
			{"classic ghp token", "cloning with ghp_1234567890abcdef1234567890abcdef12", "cloning with [REDACTED_TOKEN]"},
			{"basic auth header", "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46c2VjcmV0", "AUTHORIZATION: [REDACTED_TOKEN]"},
			{"postgres connection string", "connecting to postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable", "connecting to postgres://aerial:[REDACTED_TOKEN]@postgres:5432/aerial?sslmode=disable"},
			{"anthropic api key", "using sk-ant-api03-1234567890abcdef-secret", "using [REDACTED_TOKEN]"},
			{"google api key", "key AIzaSyD1234567890abcdefghijklmnopqrstuv", "key [REDACTED_TOKEN]"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := SanitizeLog(tt.input); got != tt.expected {
					t.Errorf("SanitizeLog() = %q, want %q", got, tt.expected)
				}
			})
		}
	})
}

func TestGetComposeArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name                    string
		subArgs                 []string
		writeBaseCompose        bool
		writeConfigOverride     bool
		sameComposeAndConfigDir bool
		writeLocalOverride      bool
		writeEnvFile            bool
		wantFFlags              int
		checkExactArgs          bool
	}{
		{name: "base_only", subArgs: []string{"config", "--quiet"}, writeBaseCompose: true, wantFFlags: 1, checkExactArgs: true},
		{name: "with_config_override", subArgs: []string{"up", "-d", "--no-recreate", "gitsync"}, writeBaseCompose: true, writeConfigOverride: true, wantFFlags: 2, checkExactArgs: true},
		{name: "deduplication", subArgs: []string{"config"}, writeBaseCompose: true, writeLocalOverride: true, sameComposeAndConfigDir: true, wantFFlags: 2},
		{name: "with_env_file", subArgs: []string{"config"}, writeBaseCompose: true, writeEnvFile: true, wantFFlags: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			composeDir := t.TempDir()
			configDir := t.TempDir()
			if tc.sameComposeAndConfigDir {
				configDir = composeDir
			}
			baseFile := filepath.Join(composeDir, "docker-compose.yml")
			if tc.writeBaseCompose {
				if err := os.WriteFile(baseFile, []byte("services: {}"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			overrideFile := filepath.Join(configDir, "docker-compose.override.yml")
			if tc.writeConfigOverride {
				if err := os.WriteFile(overrideFile, []byte("services: { brain: {} }"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if tc.writeLocalOverride {
				localOverride := filepath.Join(composeDir, "docker-compose.override.yml")
				if err := os.WriteFile(localOverride, []byte("services: { brain: {} }"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			envFile := filepath.Join(composeDir, ".env")
			if tc.writeEnvFile {
				if err := os.WriteFile(envFile, []byte("TEST_KEY=123"), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			daemon := &SyncDaemon{composeDir: composeDir, configDir: configDir}
			args := daemon.getComposeArgs(composeDir, tc.subArgs...)

			fCount := 0
			hasEnvFile := false
			for i, a := range args {
				if a == "-f" {
					fCount++
				}
				if a == "--env-file" && i+1 < len(args) && args[i+1] == envFile {
					hasEnvFile = true
				}
			}
			if fCount != tc.wantFFlags {
				t.Fatalf("expected %d -f flags, got %d: %v", tc.wantFFlags, fCount, args)
			}
			if tc.writeEnvFile && !hasEnvFile {
				t.Fatalf("expected --env-file %s in args, got: %v", envFile, args)
			}
			if tc.checkExactArgs {
				want := []string{"--project-name", "aerial", "--project-directory", composeDir, "-f", baseFile}
				if tc.writeConfigOverride {
					want = append(want, "-f", filepath.Clean(overrideFile))
				}
				want = append(want, tc.subArgs...)
				if len(args) != len(want) {
					t.Fatalf("expected %d args, got %d: %v", len(want), len(args), args)
				}
				for i := range want {
					if args[i] != want[i] {
						t.Errorf("arg[%d] = %q, want %q", i, args[i], want[i])
					}
				}
			}
		})
	}
}

func TestGetRepoCommitAndStatus_TableDriven(t *testing.T) {
	t.Run("getRepoCommit", func(t *testing.T) {
		tests := []struct {
			name        string
			pat         string
			gitOut      string
			gitErr      error
			wantSha     string
			wantErr     bool
			wantNilTime bool
			wantYear    int
		}{
			{"git_error", "", "", errors.New("git error"), "", true, true, 0},
			{"output_without_null_delimiter", "", "abc1234", nil, "abc1234", false, true, 0},
			{"output_with_invalid_rfc3339_date", "", "abc1234\x00invalid-date", nil, "abc1234", false, true, 0},
			{"output_with_valid_rfc3339_date_and_pat", "my_pat", "1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z", nil, "1122334455667788990011223344556677889900", false, false, 2026},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{
					pat: tc.pat,
					gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						return []byte(tc.gitOut), nil, tc.gitErr
					},
				}
				sha, tm, err := d.getRepoCommit(context.Background(), "/repo", "HEAD")
				if (err != nil) != tc.wantErr {
					t.Fatalf("getRepoCommit() error = %v, wantErr = %v", err, tc.wantErr)
				}
				if sha != tc.wantSha {
					t.Errorf("getRepoCommit() sha = %q, want %q", sha, tc.wantSha)
				}
				if tc.wantNilTime && tm != nil {
					t.Errorf("expected nil time, got %v", tm)
				}
				if !tc.wantNilTime && (tm == nil || tm.Year() != tc.wantYear) {
					t.Errorf("expected valid time with year %d, got %v", tc.wantYear, tm)
				}
			})
		}
	})

	t.Run("GetStatus", func(t *testing.T) {
		now := time.Now()
		past := now.Add(-10 * time.Minute)
		sha1 := "1111111111111111111111111111111111111111"
		sha2 := "2222222222222222222222222222222222222222"
		shaSync := "abc1234567890abcdef1234567890abcdef12"

		tests := []struct {
			name               string
			repos              []string
			failRepo           string
			seedLastSyncRepo   string
			headOut            string
			originMainOut      string
			originMainErr      bool
			fetchHeadOut       string
			fetchHeadErr       bool
			wantStatus         string
			wantRepo           string
			wantDiskCommit     string
			wantRemoteCommit   string
			wantRepoSyncStatus string
		}{
			{
				name: "synced_and_http_endpoint", repos: []string{"/mock/repo/aerial"},
				headOut:    shaSync + "\x002026-09-12T12:00:00Z",
				wantStatus: "synced", wantRepo: "/mock/repo/aerial", wantDiskCommit: shaSync, wantRepoSyncStatus: "synced",
			},
			{
				name: "error_status_when_one_repo_fails_with_lastSyncTimes", repos: []string{"/repo/ok", "/repo/failing"},
				failRepo: "/repo/failing", seedLastSyncRepo: "/repo/ok",
				headOut: sha1 + "\x002026-09-12T12:00:00Z", wantStatus: "error",
			},
			{
				name: "lagging_status_when_remote_sha_differs_and_timestamp_older", repos: []string{"/mock/local"},
				headOut: sha1 + "\x002026-09-12T12:00:00Z", originMainOut: sha2 + "\x002035-01-01T12:00:00Z",
				wantStatus: "lagging",
			},
			{
				name: "origin_main_error_fallback_to_fetch_head", repos: []string{"/mock/repo"},
				headOut: sha1 + "\x00" + now.Format(time.RFC3339), originMainErr: true, fetchHeadOut: sha2 + "\x00" + past.Format(time.RFC3339),
				wantStatus: "synced", wantRepo: "/mock/repo", wantRemoteCommit: sha2,
			},
			{
				name: "origin_main_and_fetch_head_failing_fallback_to_disk_sha", repos: []string{"/mock/repo"},
				headOut: sha1 + "\x00" + now.Format(time.RFC3339), originMainErr: true, fetchHeadErr: true,
				wantStatus: "synced", wantRepo: "/mock/repo", wantRemoteCommit: sha1,
			},
			{
				name: "clean_status_synced", repos: []string{"/repo/ok"},
				headOut: sha1 + "\x002026-09-12T12:00:00Z", wantStatus: "synced", wantRepo: "/repo/ok",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				daemon := &SyncDaemon{
					repos: tc.repos,
					gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						if tc.failRepo != "" && dir == tc.failRepo {
							return nil, []byte("error"), errors.New("failed")
						}
						ref := args[len(args)-1]
						if ref == "origin/main" || ref == "@{u}" {
							if tc.originMainErr {
								return nil, nil, errors.New("no origin/main")
							}
							if tc.originMainOut != "" {
								return []byte(tc.originMainOut), nil, nil
							}
						}
						if ref == "FETCH_HEAD" {
							if tc.fetchHeadErr {
								return nil, nil, errors.New("remote ref not found")
							}
							if tc.fetchHeadOut != "" {
								return []byte(tc.fetchHeadOut), nil, nil
							}
						}
						return []byte(tc.headOut), nil, nil
					},
				}
				if tc.seedLastSyncRepo != "" {
					daemon.lastSyncTimes = map[string]time.Time{tc.seedLastSyncRepo: time.Now().Add(-1 * time.Hour)}
				}

				status := daemon.GetStatus(context.Background())
				if status.Status != tc.wantStatus {
					t.Errorf("expected status %q, got %q", tc.wantStatus, status.Status)
				}
				if tc.wantRepo != "" {
					repoSt, ok := status.Repos[tc.wantRepo]
					if !ok {
						t.Fatalf("expected repo %s in status response", tc.wantRepo)
					}
					if tc.wantDiskCommit != "" && repoSt.DiskCommit != tc.wantDiskCommit {
						t.Errorf("expected disk commit %q, got %q", tc.wantDiskCommit, repoSt.DiskCommit)
					}
					if tc.wantRemoteCommit != "" && repoSt.RemoteCommit != tc.wantRemoteCommit {
						t.Errorf("expected remote commit %q, got %q", tc.wantRemoteCommit, repoSt.RemoteCommit)
					}
					if tc.wantRepoSyncStatus != "" && repoSt.SyncStatus != tc.wantRepoSyncStatus {
						t.Errorf("expected repo sync_status %q, got %q", tc.wantRepoSyncStatus, repoSt.SyncStatus)
					}
				}
			})
		}
	})

	t.Run("resolveGitDir", func(t *testing.T) {
		tempDir := t.TempDir()
		standardRepo := filepath.Join(tempDir, "standard")
		_ = os.MkdirAll(filepath.Join(standardRepo, ".git"), 0o755)

		worktreeRepo := filepath.Join(tempDir, "worktree")
		_ = os.MkdirAll(worktreeRepo, 0o755)
		targetGitDir := filepath.Join(tempDir, "target-git-dir")
		_ = os.MkdirAll(targetGitDir, 0o755)
		_ = os.WriteFile(filepath.Join(worktreeRepo, ".git"), []byte("gitdir: "+targetGitDir+"\n"), 0o644)

		relWorktreeRepo := filepath.Join(tempDir, "relworktree")
		_ = os.MkdirAll(relWorktreeRepo, 0o755)
		_ = os.WriteFile(filepath.Join(relWorktreeRepo, ".git"), []byte("gitdir: ../target-git-dir\n"), 0o644)

		plainFileRepo := filepath.Join(tempDir, "plainfile")
		_ = os.MkdirAll(plainFileRepo, 0o755)
		_ = os.WriteFile(filepath.Join(plainFileRepo, ".git"), []byte("not_a_gitdir"), 0o644)

		tests := []struct {
			name, repoPath, wantGitDir string
			wantErr                    bool
		}{
			{"empty_path", "", "", true},
			{"nonexistent_directory", filepath.Join(tempDir, "nonexistent"), "", true},
			{"standard_git_directory", standardRepo, filepath.Join(standardRepo, ".git"), false},
			{"worktree_absolute_gitdir", worktreeRepo, targetGitDir, false},
			{"worktree_relative_gitdir", relWorktreeRepo, targetGitDir, false},
			{"worktree_without_gitdir_prefix", plainFileRepo, filepath.Join(plainFileRepo, ".git"), false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				gitDir, err := resolveGitDir(tc.repoPath)
				if (err != nil) != tc.wantErr {
					t.Fatalf("resolveGitDir() error = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && gitDir != tc.wantGitDir {
					t.Errorf("resolveGitDir() = %q, want %q", gitDir, tc.wantGitDir)
				}
			})
		}
	})
}

func TestReconcilerAndPeriodicLoops_TableDriven(t *testing.T) {
	t.Run("StartReconcilerLoop_nil_channel_init", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		d := &SyncDaemon{}
		d.StartReconcilerLoop(ctx)
		if d.reconcileCh == nil {
			t.Errorf("expected reconcileCh to be initialized")
		}
	})

	t.Run("StartReconcilerLoop_debounce_and_execution", func(t *testing.T) {
		oldDebounce, oldCooldown := reconcilerDebounceDuration, reconcilerMinCooldown
		reconcilerDebounceDuration, reconcilerMinCooldown = 5*time.Millisecond, 5*time.Millisecond
		defer func() {
			reconcilerDebounceDuration, reconcilerMinCooldown = oldDebounce, oldCooldown
		}()

		tempDir := t.TempDir()
		nomadExecuted := make(chan struct{}, 10)
		d := NewDaemon(DaemonConfig{
			ComposeDir: tempDir,
			ConfigDir:  tempDir,
			ComposeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, nil, errors.New("compose fail")
			},
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				select {
				case nomadExecuted <- struct{}{}:
				default:
				}
				return nil, nil, errors.New("nomad fail")
			},
		})
		d.recordPendingNomadChanges(tempDir, []NomadFileChange{
			{JobName: "deljob", Action: "delete"},
			{JobName: "appjob", Action: "apply", Path: "jobs/app.nomad"},
		})

		loopCtx, loopCancel := context.WithCancel(context.Background())
		d.StartReconcilerLoop(loopCtx)

		_ = d.TriggerReconcile(context.Background(), true)
		select {
		case <-nomadExecuted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for nomad executor signal on first trigger")
		}

		d.SetLastReconcile(time.Now())
		_ = d.TriggerReconcile(context.Background(), true)
		select {
		case <-nomadExecuted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for nomad executor signal on second trigger")
		}
		loopCancel()
	})

	t.Run("StartPeriodicLoop_execution_and_shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		triggered := make(chan struct{}, 1)
		d := NewDaemon(DaemonConfig{Interval: 5 * time.Millisecond, ComposeDir: t.TempDir()})
		d.triggerFn = func() ([]RepoSyncResult, error) {
			select {
			case triggered <- struct{}{}:
			default:
			}
			return nil, errors.New("simulated periodic sync notice")
		}

		d.StartPeriodicLoop(ctx)
		select {
		case <-triggered:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for periodic loop execution")
		}
		cancel()
	})

	t.Run("DaemonLoops_graceful_cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		d := NewDaemon(DaemonConfig{Interval: 10 * time.Millisecond})
		d.StartPeriodicLoop(ctx)
		d.StartReconcilerLoop(ctx)
		cancel()
	})
}

func TestEnsureRepoAndSync_TableDriven(t *testing.T) {
	t.Run("EnsureRepo", func(t *testing.T) {
		tests := []struct {
			name               string
			emptyArgs          bool
			seedRegularFile    bool
			seedNonEmptyDir    bool
			cloneCreatesGitDir bool
			callTwice          bool
			gitErrOnCmd        map[string]error
			wantCloneCalled    bool
			wantMinGitCalls    int
			wantErr            bool
		}{
			{name: "empty_path_or_url_noop", emptyArgs: true},
			{name: "regular_file_not_a_directory_error", seedRegularFile: true, wantErr: true},
			{name: "empty_directory_clone_success_and_second_call_noop", cloneCreatesGitDir: true, callTwice: true, wantCloneCalled: true},
			{name: "clone_error", gitErrOnCmd: map[string]error{"clone": errors.New("clone failed")}, wantErr: true},
			{name: "non_empty_directory_adoption_success", seedNonEmptyDir: true, wantMinGitCalls: 4},
			{name: "adoption_git_init_failure", seedNonEmptyDir: true, gitErrOnCmd: map[string]error{"init": errors.New("init error")}, wantErr: true},
			{name: "adoption_git_remote_add_nonfatal_warning_when_fetch_succeeds", seedNonEmptyDir: true, gitErrOnCmd: map[string]error{"remote": errors.New("already exists")}},
			{name: "adoption_git_remote_add_warning_and_fetch_fails", seedNonEmptyDir: true, gitErrOnCmd: map[string]error{"remote": errors.New("remote exists"), "fetch": errors.New("fetch error")}, wantErr: true},
			{name: "adoption_git_reset_soft_failure", seedNonEmptyDir: true, gitErrOnCmd: map[string]error{"reset": errors.New("reset error")}, wantErr: true},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.emptyArgs {
					d := &SyncDaemon{}
					if err := d.EnsureRepo(context.Background(), "", ""); err != nil {
						t.Errorf("EnsureRepo with empty strings failed: %v", err)
					}
					return
				}

				tempDir := t.TempDir()
				repoPath := filepath.Join(tempDir, "repo")
				if tc.seedRegularFile {
					_ = os.WriteFile(repoPath, []byte("regular file"), 0o644)
				}
				if tc.seedNonEmptyDir {
					_ = os.MkdirAll(repoPath, 0o755)
					_ = os.WriteFile(filepath.Join(repoPath, "file.txt"), []byte("data"), 0o644)
				}

				var calls []string
				cloneCalled := false
				d := NewDaemon(DaemonConfig{
					GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						if len(args) > 0 {
							cmd := args[0]
							calls = append(calls, cmd)
							if cmd == "clone" && tc.cloneCreatesGitDir {
								cloneCalled = true
								_ = os.MkdirAll(filepath.Join(args[len(args)-1], ".git"), 0o755)
							}
							if err, ok := tc.gitErrOnCmd[cmd]; ok && err != nil {
								return nil, []byte("fatal: " + err.Error()), err
							}
						}
						return nil, nil, nil
					},
				})

				err := d.EnsureRepo(context.Background(), repoPath, "https://github.com/example/repo.git")
				if (err != nil) != tc.wantErr {
					t.Fatalf("EnsureRepo() error = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantCloneCalled && !cloneCalled {
					t.Errorf("expected clone to be called")
				}
				if tc.wantMinGitCalls > 0 && len(calls) < tc.wantMinGitCalls {
					t.Errorf("expected at least %d git calls, got %d (%v)", tc.wantMinGitCalls, len(calls), calls)
				}
				if tc.callTwice {
					if err := d.EnsureRepo(context.Background(), repoPath, "https://github.com/example/repo.git"); err != nil {
						t.Fatalf("EnsureRepo second call failed: %v", err)
					}
				}
			})
		}
	})

	t.Run("SyncRepo", func(t *testing.T) {
		head1 := "1111111111111111111111111111111111111111"
		head2 := "2222222222222222222222222222222222222222"

		tests := []struct {
			name              string
			seedMode          string // "empty", "nonexistent", "git", "non_empty_no_git", "index_lock"
			withRepoURL       bool
			initHead          string
			fetchHead         string
			diffOut           string
			revParseErrOnCall int
			fetchErr          error
			mergeErr          error
			resetErr          error
			failAllGit        bool
			wantEmptyRepo     bool
			wantAnyErr        bool
			wantErrSubstr     string
			wantChanged       bool
			wantPrevHead      string
			wantCurrHead      string
		}{
			{name: "empty_repo_path", seedMode: "empty", wantEmptyRepo: true},
			{name: "nonexistent_repo_path", seedMode: "nonexistent", wantAnyErr: true},
			{name: "index_lock_active_guard", seedMode: "index_lock", wantErrSubstr: "index.lock active"},
			{name: "up_to_date", seedMode: "git", withRepoURL: true, initHead: head1, fetchHead: head1, diffOut: "docker-compose.yml\n", wantChanged: false, wantPrevHead: head1, wantCurrHead: head1},
			{name: "fast_forward_update_with_diff", seedMode: "git", withRepoURL: true, initHead: head1, fetchHead: head2, diffOut: "docker-compose.yml\n", wantChanged: true, wantPrevHead: head1, wantCurrHead: head2},
			{name: "empty_git_rev_parse_head_error", seedMode: "git", revParseErrOnCall: 1, wantAnyErr: true},
			{name: "fetch_error", seedMode: "git", initHead: head1, fetchErr: errors.New("fetch failed"), wantErrSubstr: "fetch failed"},
			{name: "ensure_repo_failure_warning_uninitialized", seedMode: "non_empty_no_git", withRepoURL: true, failAllGit: true, wantAnyErr: true},
			{name: "merge_conflict_reset_recovery", seedMode: "git", initHead: head1, fetchHead: head2, mergeErr: errors.New("exit status 128"), wantChanged: true, wantPrevHead: head1, wantCurrHead: head2},
			{name: "recovery_reset_hard_failure", seedMode: "git", initHead: head1, fetchHead: head1, mergeErr: errors.New("merge conflict"), resetErr: errors.New("reset failed"), wantErrSubstr: "reset failed"},
			{name: "post_merge_rev_parse_failure", seedMode: "git", initHead: head1, fetchHead: head1, revParseErrOnCall: 2, wantAnyErr: true},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.seedMode == "empty" {
					d := &SyncDaemon{}
					if res := d.SyncRepo(context.Background(), ""); res.Repo != "" {
						t.Errorf("expected empty repo result, got %+v", res)
					}
					return
				}

				tempDir := t.TempDir()
				repoPath := filepath.Join(tempDir, "local")
				if tc.seedMode == "nonexistent" {
					repoPath = filepath.Join(tempDir, "nonexistent")
				}
				if tc.seedMode == "git" || tc.seedMode == "index_lock" {
					_ = os.MkdirAll(filepath.Join(repoPath, ".git"), 0o755)
				}
				if tc.seedMode == "non_empty_no_git" {
					_ = os.MkdirAll(repoPath, 0o755)
					_ = os.WriteFile(filepath.Join(repoPath, "file.txt"), []byte("content"), 0o644)
				}
				if tc.seedMode == "index_lock" {
					_ = os.WriteFile(filepath.Join(repoPath, ".git", "index.lock"), []byte(""), 0o644)
				}

				currentHead := tc.initHead
				revCalls := 0
				var repoURLs map[string]string
				if tc.withRepoURL {
					repoURLs = map[string]string{repoPath: "https://example.com/repo.git"}
				}

				d := &SyncDaemon{
					repos:         []string{repoPath},
					repoUrls:      repoURLs,
					reconcileCh:   make(chan struct{}, 1),
					lastSyncTimes: make(map[string]time.Time),
					gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						if tc.failAllGit {
							return nil, []byte("fatal: clone failed"), errors.New("clone failed")
						}
						if len(args) == 0 {
							return nil, nil, nil
						}
						switch args[0] {
						case "rev-parse":
							if len(args) > 1 && args[1] == "FETCH_HEAD" {
								return []byte(tc.fetchHead + "\n"), nil, nil
							}
							revCalls++
							if tc.revParseErrOnCall > 0 && revCalls >= tc.revParseErrOnCall {
								return nil, []byte("fatal: rev-parse failed"), errors.New("rev-parse error")
							}
							return []byte(currentHead + "\n"), nil, nil
						case "fetch", "pull":
							if tc.fetchErr != nil {
								return nil, []byte("fatal: unable to access"), tc.fetchErr
							}
						case "merge":
							if tc.mergeErr != nil {
								return nil, []byte("fatal: conflict"), tc.mergeErr
							}
							currentHead = tc.fetchHead
							return []byte("Updating " + currentHead + "\n"), nil, nil
						case "reset":
							if tc.resetErr != nil {
								return nil, []byte("fatal: reset failed"), tc.resetErr
							}
							currentHead = tc.fetchHead
							return []byte("HEAD is now at " + currentHead), nil, nil
						case "diff":
							return []byte(tc.diffOut), nil, nil
						}
						return nil, nil, nil
					},
				}

				res := d.SyncRepo(context.Background(), repoPath)
				if tc.wantAnyErr && res.Error == "" {
					t.Fatalf("expected non-empty error, got empty")
				}
				if tc.wantErrSubstr != "" && !strings.Contains(res.Error, tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErrSubstr, res.Error)
				}
				if !tc.wantAnyErr && tc.wantErrSubstr == "" && res.Error != "" {
					t.Fatalf("unexpected error: %s", res.Error)
				}
				if res.Changed != tc.wantChanged {
					t.Errorf("Changed = %v, want %v", res.Changed, tc.wantChanged)
				}
				if tc.wantPrevHead != "" && res.PreviousHead != tc.wantPrevHead {
					t.Errorf("PreviousHead = %q, want %q", res.PreviousHead, tc.wantPrevHead)
				}
				if tc.wantCurrHead != "" && res.CurrentHead != tc.wantCurrHead {
					t.Errorf("CurrentHead = %q, want %q", res.CurrentHead, tc.wantCurrHead)
				}
			})
		}
	})

	t.Run("TriggerSync", func(t *testing.T) {
		tempDir := t.TempDir()
		localDir := filepath.Join(tempDir, "local")
		_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0o755)
		configDir := filepath.Join(tempDir, "config")
		_ = os.MkdirAll(filepath.Join(configDir, "services", "haro"), 0o755)
		_ = os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte("channels:\n  default:\n    mode: all\n"), 0o644)
		_ = os.WriteFile(filepath.Join(configDir, "services", "haro", "haro.yaml"), []byte("port: 4001\n"), 0o644)

		head1 := "1111111111111111111111111111111111111111"
		head2 := "2222222222222222222222222222222222222222"
		currentHead := head1
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
				if len(args) > 0 {
					switch args[0] {
					case "rev-parse":
						return []byte(currentHead), nil, nil
					case "merge":
						if currentHead == head1 {
							currentHead = head2
							return []byte("Updating " + head2), nil, nil
						}
						return []byte("Already up to date."), nil, nil
					}
				}
				return nil, nil, nil
			},
		})

		// 1. Initial sync with Changed: true and ConfigDir nomad var put calls
		results, err := d.TriggerSync()
		if err != nil {
			t.Fatalf("TriggerSync failed: %v", err)
		}
		if len(results) != 1 || !results[0].Changed {
			t.Errorf("expected 1 result with Changed=true, got %+v", results)
		}
		nomadMu.Lock()
		initialCalls := len(nomadCalls)
		nomadMu.Unlock()
		if initialCalls == 0 {
			t.Fatalf("expected nomad var put calls during initial sync, got 0")
		}

		// 2. Second sync with Changed: false verifying nomad var put hash cache hit
		results2, err := d.TriggerSync()
		if err != nil || len(results2) != 1 || results2[0].Changed {
			t.Fatalf("expected 1 unchanged result on second sync, err=%v, results=%+v", err, results2)
		}
		nomadMu.Lock()
		secondCalls := len(nomadCalls)
		nomadMu.Unlock()
		if secondCalls != initialCalls {
			t.Fatalf("expected call count to remain %d due to caching, got %d", initialCalls, secondCalls)
		}

		// 3. sfg.Do invalid return type error
		started := make(chan struct{})
		go func() {
			_, _, _ = d.sfg.Do("sync", func() (interface{}, error) {
				close(started)
				<-time.After(20 * time.Millisecond)
				return "invalid-type", nil
			})
		}()
		<-started
		_, err = d.TriggerSync()
		if err == nil || !strings.Contains(err.Error(), "unexpected return type") {
			t.Errorf("expected unexpected return type error, got %v", err)
		}
	})
}

func TestNewConfigFromLookup_TableDriven(t *testing.T) {
	if cfg := NewConfigFromEnv(); cfg.Port == "" {
		t.Errorf("expected non-empty port from NewConfigFromEnv()")
	}
	if cfg := NewConfigFromLookup(nil); cfg.Port != "8087" {
		t.Errorf("NewConfigFromLookup(nil) Port = %q, want 8087", cfg.Port)
	}

	baseYAML := `
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
	tests := []struct {
		name                     string
		yamlContent              string
		env                      map[string]string
		wantPort                 string
		wantRepos                []string
		wantInterval             time.Duration
		wantPAT                  string
		wantComposeDir           string
		wantConfigDir            string
		wantDiscordToken         string
		wantDiscordChannel       string
		wantDiscordWebhook       string
		wantNomadAddr            string
		wantBrainURL             string
		wantExtraSecretsLen      int
		checkExtraSecrets        bool
		wantServiceConfigRelPath string
	}{
		{
			name: "custom environment lookup with extra secrets",
			env: map[string]string{
				"PORT": "9090", "SYNC_REPOS": "/path/a, /path/b", "SYNC_INTERVAL": "30s",
				"GITHUB_PAT": "my_secret_pat", "AERIAL_CONFIG_REPO_URL": "https://github.com/custom/config.git",
				"AERIAL_PROJECT_DIR": "/custom/aerial", "AERIAL_CONFIG_DIR": "/custom/config",
				"DISCORD_BOT_TOKEN": "bot_token", "DISCORD_CHANNEL": "custom-channel",
				"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/123/abc",
				"BRAIN_INTERNAL_URL":  "http://brain:9999/reload",
				"POSTGRES_PASSWORD":   "pg_pass", "HA_TOKEN": "ha_tok", "GEMINI_API_KEY": "gem_key",
			},
			wantPort: "9090", wantRepos: []string{"/path/a", "/path/b"}, wantInterval: 30 * time.Second,
			wantPAT: "my_secret_pat", wantComposeDir: "/custom/aerial", wantConfigDir: "/custom/config",
			wantDiscordToken: "bot_token", wantDiscordChannel: "custom-channel",
			wantDiscordWebhook: "https://discord.com/api/webhooks/123/abc", wantBrainURL: "http://brain:9999/reload",
			wantExtraSecretsLen: 4, checkExtraSecrets: true,
		},
		{
			name: "empty lookup defaults", env: map[string]string{},
			wantPort: "8087", wantInterval: 60 * time.Second, wantComposeDir: "/share/aerial",
			wantConfigDir: "/share/aerial-config", wantBrainURL: "http://brain:8088/internal/reload",
			wantExtraSecretsLen: 0, checkExtraSecrets: true,
		},
		{
			name: "CONFIG_PATH YAML loading and service configs", yamlContent: baseYAML,
			wantPort: "8087", wantInterval: 45 * time.Second, wantRepos: []string{"/repo/a", "/repo/b"},
			wantNomadAddr: "http://nomad.cluster:4646", wantBrainURL: "http://brain.cluster:8088/internal/reload",
			wantServiceConfigRelPath: "services/voice/voice.yaml",
		},
		{
			name: "env overrides take precedence over CONFIG_PATH YAML", yamlContent: baseYAML,
			env: map[string]string{
				"PORT": "9999", "SYNC_INTERVAL": "10s", "SYNC_REPOS": "/override/repo",
				"NOMAD_ADDR": "http://override.nomad:4646", "BRAIN_INTERNAL_URL": "http://override.brain:8088/reload",
			},
			wantPort: "9999", wantInterval: 10 * time.Second, wantRepos: []string{"/override/repo"},
			wantNomadAddr: "http://override.nomad:4646", wantBrainURL: "http://override.brain:8088/reload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envMap := make(map[string]string, len(tt.env)+1)
			for k, v := range tt.env {
				envMap[k] = v
			}
			if tt.yamlContent != "" {
				cfgPath := filepath.Join(t.TempDir(), "hangar.yaml")
				if err := os.WriteFile(cfgPath, []byte(tt.yamlContent), 0o644); err != nil {
					t.Fatalf("failed to write test yaml: %v", err)
				}
				envMap["CONFIG_PATH"] = cfgPath
			}
			cfg := NewConfigFromLookup(func(k string) string { return envMap[k] })

			if tt.wantPort != "" && cfg.Port != tt.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tt.wantPort)
			}
			if len(tt.wantRepos) > 0 {
				if len(cfg.Repos) != len(tt.wantRepos) {
					t.Fatalf("Repos = %v, want %v", cfg.Repos, tt.wantRepos)
				}
				for i := range tt.wantRepos {
					if cfg.Repos[i] != tt.wantRepos[i] {
						t.Errorf("Repos[%d] = %q, want %q", i, cfg.Repos[i], tt.wantRepos[i])
					}
				}
			}
			if tt.wantInterval != 0 && cfg.Interval != tt.wantInterval {
				t.Errorf("Interval = %v, want %v", cfg.Interval, tt.wantInterval)
			}
			if tt.wantPAT != "" && cfg.PAT != tt.wantPAT {
				t.Errorf("PAT = %q, want %q", cfg.PAT, tt.wantPAT)
			}
			if tt.wantComposeDir != "" && cfg.ComposeDir != tt.wantComposeDir {
				t.Errorf("ComposeDir = %q, want %q", cfg.ComposeDir, tt.wantComposeDir)
			}
			if tt.wantConfigDir != "" && cfg.ConfigDir != tt.wantConfigDir {
				t.Errorf("ConfigDir = %q, want %q", cfg.ConfigDir, tt.wantConfigDir)
			}
			if tt.wantDiscordToken != "" && cfg.DiscordToken != tt.wantDiscordToken {
				t.Errorf("DiscordToken = %q, want %q", cfg.DiscordToken, tt.wantDiscordToken)
			}
			if tt.wantDiscordChannel != "" && cfg.DiscordChannel != tt.wantDiscordChannel {
				t.Errorf("DiscordChannel = %q, want %q", cfg.DiscordChannel, tt.wantDiscordChannel)
			}
			if tt.wantDiscordWebhook != "" && cfg.DiscordWebhookURL != tt.wantDiscordWebhook {
				t.Errorf("DiscordWebhookURL = %q, want %q", cfg.DiscordWebhookURL, tt.wantDiscordWebhook)
			}
			if tt.wantNomadAddr != "" && cfg.NomadAddr != tt.wantNomadAddr {
				t.Errorf("NomadAddr = %q, want %q", cfg.NomadAddr, tt.wantNomadAddr)
			}
			if tt.wantBrainURL != "" && cfg.BrainInternalURL != tt.wantBrainURL {
				t.Errorf("BrainInternalURL = %q, want %q", cfg.BrainInternalURL, tt.wantBrainURL)
			}
			if tt.checkExtraSecrets && len(cfg.ExtraSecrets) != tt.wantExtraSecretsLen {
				t.Errorf("len(ExtraSecrets) = %d, want %d (%v)", len(cfg.ExtraSecrets), tt.wantExtraSecretsLen, cfg.ExtraSecrets)
			}
			if tt.wantServiceConfigRelPath != "" {
				if len(cfg.ServiceConfigs) != 1 || cfg.ServiceConfigs[0].RelPath != tt.wantServiceConfigRelPath {
					t.Errorf("ServiceConfigs = %v, want RelPath %q", cfg.ServiceConfigs, tt.wantServiceConfigRelPath)
				}
			}
		})
	}
}

func TestSetupMuxAndDaemonLifecycle_TableDriven(t *testing.T) {
	t.Run("NewDaemon interval configuration", func(t *testing.T) {
		for _, tc := range []struct {
			name           string
			interval, want time.Duration
		}{
			{"custom interval", 50 * time.Millisecond, 50 * time.Millisecond},
			{"default fallback", 0, 60 * time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if d := NewDaemon(DaemonConfig{Interval: tc.interval}); d.interval != tc.want {
					t.Errorf("expected interval %v, got %v", tc.want, d.interval)
				}
			})
		}
	})

	t.Run("SetupMux endpoints and HTTP methods", func(t *testing.T) {
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0o755)
		_ = os.MkdirAll(filepath.Join(tmpDir, ".git"), 0o755)
		_ = os.WriteFile(filepath.Join(jobsDir, "test.nomad"), []byte(`job "test" { task "t" { config { image = "ghcr.io/azylman/aerial:latest" } } }`), 0o644)

		tests := []struct {
			name, method, path, body, contentType, reconcileMode string
			syncErr                                              bool
			wantCode                                             int
		}{
			{name: "GET /health 200", method: http.MethodGet, path: "/health", wantCode: http.StatusOK},
			{name: "GET /metrics 200", method: http.MethodGet, path: "/metrics", wantCode: http.StatusOK},
			{name: "GET /status 200", method: http.MethodGet, path: "/status", wantCode: http.StatusOK},
			{name: "POST /status 405", method: http.MethodPost, path: "/status", wantCode: http.StatusMethodNotAllowed},
			{name: "GET /sync 405", method: http.MethodGet, path: "/sync", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /sync 200", method: http.MethodPost, path: "/sync", wantCode: http.StatusOK},
			{name: "POST /sync 500", method: http.MethodPost, path: "/sync", syncErr: true, wantCode: http.StatusInternalServerError},
			{name: "GET /reconcile 405", method: http.MethodGet, path: "/reconcile", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /reconcile 202 async", method: http.MethodPost, path: "/reconcile", wantCode: http.StatusAccepted},
			{name: "POST /reconcile?sync=true 200", method: http.MethodPost, path: "/reconcile?sync=true", reconcileMode: "ok", wantCode: http.StatusOK},
			{name: "POST /reconcile?sync=true 500", method: http.MethodPost, path: "/reconcile?sync=true", reconcileMode: "err", wantCode: http.StatusInternalServerError},
			{name: "POST /reconcile JSON sync=true 200", method: http.MethodPost, path: "/reconcile", body: `{"sync":true}`, contentType: "application/json", reconcileMode: "ok", wantCode: http.StatusOK},
			{name: "POST /reconcile JSON sync=false 202", method: http.MethodPost, path: "/reconcile", body: `{"sync":false}`, contentType: "application/json", wantCode: http.StatusAccepted},
			{name: "POST /events/git_push 200", method: http.MethodPost, path: "/events/git_push", body: `{"repo":"` + tmpDir + `","ref":"refs/heads/main"}`, contentType: "application/json", wantCode: http.StatusOK},
			{name: "GET /events/git_push 405", method: http.MethodGet, path: "/events/git_push", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /events/git_push bad JSON 400", method: http.MethodPost, path: "/events/git_push", body: `bad`, wantCode: http.StatusBadRequest},
			{name: "POST /events/image_ready 202", method: http.MethodPost, path: "/events/image_ready", body: `{"image":"ghcr.io/azylman/aerial:latest"}`, contentType: "application/json", wantCode: http.StatusAccepted},
			{name: "GET /events/image_ready 405", method: http.MethodGet, path: "/events/image_ready", wantCode: http.StatusMethodNotAllowed},
			{name: "POST /events/image_ready bad JSON 400", method: http.MethodPost, path: "/events/image_ready", body: `{bad`, wantCode: http.StatusBadRequest},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
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
				if tc.syncErr {
					d.triggerFn = func() ([]RepoSyncResult, error) { return nil, errors.New("fail") }
				}
				switch tc.reconcileMode {
				case "ok":
					d.reconcileFn = func(ctx context.Context) error { return nil }
				case "err":
					d.reconcileFn = func(ctx context.Context) error { return errors.New("fail") }
				}
				var body io.Reader
				if tc.body != "" {
					body = strings.NewReader(tc.body)
				}
				req := httptest.NewRequest(tc.method, tc.path, body)
				if tc.contentType != "" {
					req.Header.Set("Content-Type", tc.contentType)
				}
				rec := httptest.NewRecorder()
				SetupMux(d).ServeHTTP(rec, req)
				if rec.Code != tc.wantCode {
					t.Errorf("%s %s returned %d; want %d", tc.method, tc.path, rec.Code, tc.wantCode)
				}
			})
		}
	})

	t.Run("RunDaemon lifecycle", func(t *testing.T) {
		for _, tc := range []struct {
			name, port, pat   string
			withRepo, wantErr bool
		}{
			{name: "invalid port string fatal error", port: "invalid-port-string", wantErr: true},
			{name: "invalid negative port fatal error", port: "-1", wantErr: true},
			{name: "normal lifecycle with PAT and deterministic sync signaling", port: "0", pat: "ghp_dummy_token_1234567890", withRepo: true, wantErr: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cfg := DaemonConfig{Port: tc.port, PAT: tc.pat}
				if tc.withRepo {
					t.Setenv("DOCKER_CONFIG", t.TempDir())
					repoDir := filepath.Join(t.TempDir(), "repo")
					_ = os.MkdirAll(filepath.Join(repoDir, ".git"), 0o755)
					started := make(chan struct{}, 1)
					cfg.Interval = time.Hour
					cfg.Repos = []string{repoDir}
					cfg.ComposeDir = t.TempDir()
					cfg.ConfigDir = t.TempDir()
					cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						select {
						case started <- struct{}{}:
						default:
						}
						return []byte("abc1234"), nil, nil
					}
					cfg.ComposeExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						return []byte("ok"), nil, nil
					}
					go func() {
						select {
						case <-started:
						case <-time.After(2 * time.Second):
						}
						cancel()
					}()
				}
				err := RunDaemon(ctx, cfg)
				if (err != nil) != tc.wantErr {
					t.Errorf("RunDaemon() error = %v, wantErr = %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("coverage_flush_endpoint", func(t *testing.T) {
		mux := SetupMux(&SyncDaemon{})
		orig := os.Getenv("GOCOVERDIR")
		_ = os.Unsetenv("GOCOVERDIR")
		defer func() {
			if orig != "" {
				_ = os.Setenv("GOCOVERDIR", orig)
			} else {
				_ = os.Unsetenv("GOCOVERDIR")
			}
		}()

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK without GOCOVERDIR, got: %d", rec.Code)
		}

		oldFn, oldMetaFn := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
		defer func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldFn, oldMetaFn }()

		called, metaCalled := false, false
		httpkit.WriteCountersDirFn = func(dir string) error {
			called = true
			return nil
		}
		httpkit.WriteMetaDirFn = func(dir string) error {
			metaCalled = true
			return nil
		}
		_ = os.Setenv("GOCOVERDIR", t.TempDir())

		rec2 := httptest.NewRecorder()
		mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2.Code != http.StatusOK || !called || !metaCalled {
			t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", rec2.Code, called, metaCalled)
		}

		httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
		rec2b := httptest.NewRecorder()
		mux.ServeHTTP(rec2b, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2b.Code != http.StatusOK {
			t.Errorf("expected 200 OK when meta flush errors, got %d", rec2b.Code)
		}

		httpkit.WriteCountersDirFn = func(dir string) error { return errors.New("simulated flush error") }
		rec3 := httptest.NewRecorder()
		mux.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec3.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on error, got: %d", rec3.Code)
		}
	})
}

func TestComposeGraph_NoGitsyncDependents(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.yml")
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", composePath, err)
	}
	inDependsOn, currentService := false, ""
	for lineNum, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(trimmed, ":") {
			currentService, inDependsOn = strings.TrimSuffix(trimmed, ":"), false
		}
		if strings.HasPrefix(line, "    depends_on:") {
			inDependsOn = true
			continue
		}
		if inDependsOn {
			if len(line) > 0 && line[0] != ' ' {
				inDependsOn, currentService = false, ""
			} else if strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "    depends_on:") && strings.HasSuffix(trimmed, ":") {
				inDependsOn = false
			} else if strings.Contains(line, "gitsync:") || trimmed == "- gitsync" {
				t.Fatalf("line %d: service %q declares dependency on gitsync (%s); gitsync must have in-degree 0 in Compose DAG", lineNum+1, currentService, trimmed)
			}
		}
	}
}

type nomadConfigSyncTestEnv struct {
	configDir string
	daemon    *SyncDaemon
	mu        sync.Mutex
	calls     [][]string
	eventsCh  chan HangarDeployEvent
}

func newNomadConfigSyncTestEnv(t *testing.T) *nomadConfigSyncTestEnv {
	t.Helper()
	env := &nomadConfigSyncTestEnv{
		configDir: t.TempDir(),
		eventsCh:  make(chan HangarDeployEvent, 16),
	}
	env.daemon = NewDaemon(DaemonConfig{
		NomadAddr: "http://127.0.0.1:4646",
		ConfigDir: env.configDir,
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			env.mu.Lock()
			env.calls = append(env.calls, args)
			env.mu.Unlock()
			return []byte("var updated"), nil, nil
		},
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			select {
			case env.eventsCh <- evt:
			default:
			}
			return nil
		},
	})
	return env
}

func (e *nomadConfigSyncTestEnv) nomadCalls() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	copied := make([][]string, len(e.calls))
	for i, c := range e.calls {
		copied[i] = append([]string(nil), c...)
	}
	return copied
}

func (e *nomadConfigSyncTestEnv) waitForEvents(t *testing.T, count int) []HangarDeployEvent {
	t.Helper()
	var events []HangarDeployEvent
	timeout := time.After(2 * time.Second)
	for len(events) < count {
		select {
		case evt := <-e.eventsCh:
			events = append(events, evt)
		case <-timeout:
			t.Fatalf("timed out waiting for %d deploy events (received %d so far: %+v)", count, len(events), events)
		}
	}
	return events
}

type nomadSyncValidationCase struct {
	name, target, dirAsFile, wantErrSub string
	files                               map[string]string
	disableNomad, nomadErr              bool
	checkEmptyHomepageHash              bool
}

func runNomadValidationCases(t *testing.T, cases []nomadSyncValidationCase) {
	t.Helper()
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNomadConfigSyncTestEnv(t)
			if tc.dirAsFile != "" {
				if err := os.MkdirAll(filepath.Join(env.configDir, tc.dirAsFile), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			for rel, content := range tc.files {
				full := filepath.Join(env.configDir, rel)
				_ = os.MkdirAll(filepath.Dir(full), 0o755)
				_ = os.WriteFile(full, []byte(content), 0o644)
			}
			if tc.disableNomad {
				env.daemon.nomadAddr = ""
			}
			if tc.nomadErr {
				env.daemon.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
				}
			}
			var err error
			switch tc.target {
			case "brain":
				err = env.daemon.SyncBrainConfigToNomad(ctx, env.configDir)
			case "homepage":
				err = env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir)
			case "service":
				err = env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir)
			default:
				t.Fatalf("unknown target: %s", tc.target)
			}

			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
			} else if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if tc.checkEmptyHomepageHash {
				calls := env.nomadCalls()
				if len(calls) != 1 || len(calls[0]) < 5 || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/homepage" {
					t.Fatalf("expected nomad/jobs/homepage target, got %v", calls)
				}
				joined := strings.Join(calls[0], " ")
				if !strings.Contains(joined, "CONFIG_HASH=") || strings.Contains(joined, "HOMEPAGE_YAML=@") {
					t.Errorf("expected CONFIG_HASH and NO HOMEPAGE_YAML on empty dir, got %v", calls[0])
				}
			}
		})
	}
}

func TestSyncConfigsToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validBrainYAML := "model: gemini-2.5-flash\nchannels:\n  default:\n    mode: threads\n"

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		runNomadValidationCases(t, []nomadSyncValidationCase{
			{name: "missing_config_yaml_safe_noop", target: "brain"},
			{name: "directory_as_config_yaml_error", target: "brain", dirAsFile: "config.yaml", wantErrSub: "failed to read brain config"},
			{name: "invalid_yaml_syntax_error", target: "brain", files: map[string]string{"config.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "semantic_validation_missing_channels_default", target: "brain", files: map[string]string{"config.yaml": "channels:\n  other:\n    mode: threads\n"}, wantErrSub: "channels.default is required"},
			{name: "disabled_nomad_addr_safe_noop_brain", target: "brain", files: map[string]string{"config.yaml": validBrainYAML}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error_brain", target: "brain", files: map[string]string{"config.yaml": validBrainYAML}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},

			{name: "empty_dir_branch_pushes_empty_hash_homepage", target: "homepage", checkEmptyHomepageHash: true},
			{name: "directory_as_homepage_yaml_error", target: "homepage", dirAsFile: "services/homepage/homepage.yaml", wantErrSub: "failed to read homepage config"},
			{name: "invalid_yaml_syntax_in_homepage_yaml", target: "homepage", files: map[string]string{"services/homepage/homepage.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "disabled_nomad_addr_safe_noop_homepage", target: "homepage", files: map[string]string{"services/homepage/homepage.yaml": "port: \"3001\"\n"}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error_homepage", target: "homepage", files: map[string]string{"services/homepage/homepage.yaml": "port: \"3001\"\n"}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},

			{name: "empty_dir_graceful_skip_service", target: "service"},
			{name: "directory_instead_of_file_error", target: "service", dirAsFile: "services/haro/haro.yaml", wantErrSub: "failed to read service config"},
			{name: "invalid_yaml_syntax_in_service_config", target: "service", files: map[string]string{"services/haro/haro.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "disabled_nomad_addr_safe_noop_service", target: "service", files: map[string]string{"services/haro/haro.yaml": "port: \"4005\"\n"}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error_service", target: "service", files: map[string]string{"services/haro/haro.yaml": "port: \"4005\"\n"}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},
		})
	})

	t.Run("caching_diff_gating_and_events", func(t *testing.T) {
		t.Parallel()
		type syncLifecycleCase struct {
			name              string
			setup             func(configDir string)
			modify            func(configDir string)
			syncFn            func(d *SyncDaemon, configDir string, syncCtx ...ConfigSyncContext) error
			cacheKey          string
			wantInitialCalls  int
			wantModifiedCalls int
			untouchedCtx      ConfigSyncContext
			sameContentCtx    ConfigSyncContext
			modifiedCtx       ConfigSyncContext
			wantJobName       string
		}

		cases := []syncLifecycleCase{
			{
				name: "brain",
				setup: func(dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(validBrainYAML), 0o644)
				},
				modify: func(dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("channels:\n  default:\n    mode: always\n"), 0o644)
				},
				syncFn: func(d *SyncDaemon, dir string, sCtx ...ConfigSyncContext) error {
					return d.SyncBrainConfigToNomad(ctx, dir, sCtx...)
				},
				cacheKey:          "nomad/jobs/eri:CONFIG_YAML",
				wantInitialCalls:  1,
				wantModifiedCalls: 1,
				untouchedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}},
				sameContentCtx:    ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}, ExpectedJobs: []string{"eri"}},
				modifiedCtx:       ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}, ExpectedJobs: []string{"eri"}},
				wantJobName:       "eri",
			},
			{
				name: "homepage",
				setup: func(dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "homepage"), 0o755)
					_ = os.WriteFile(filepath.Join(dir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\nallowed_hosts: \"*\"\n"), 0o644)
					_ = os.MkdirAll(filepath.Join(dir, "homepage", "subdir"), 0o755)
					for name, content := range map[string]string{
						"services.yaml": "- My Group: []\n",
						"widgets.yaml":  "- search: {}\n",
						".hidden":       "ignore me",
						"temp.tmp":      "ignore me too",
					} {
						_ = os.WriteFile(filepath.Join(dir, "homepage", name), []byte(content), 0o644)
					}
				},
				modify: func(dir string) {
					_ = os.WriteFile(filepath.Join(dir, "homepage", "widgets.yaml"), []byte("- search: { updated: true }\n"), 0o644)
				},
				syncFn: func(d *SyncDaemon, dir string, sCtx ...ConfigSyncContext) error {
					return d.SyncHomepageConfigToNomad(ctx, dir, sCtx...)
				},
				cacheKey:         "nomad/jobs/homepage",
				wantInitialCalls: 1,
				untouchedCtx:     ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}},
				sameContentCtx:   ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/homepage/homepage.yaml"}, ExpectedJobs: []string{"homepage"}},
				modifiedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"homepage/widgets.yaml"}, ExpectedJobs: []string{"homepage"}},
				wantJobName:      "homepage",
			},
			{
				name: "service",
				setup: func(dir string) {
					for rel, content := range map[string]string{
						"services/voice/voice.yaml":   "model: small.en\n",
						"services/haro/haro.yaml":     "enabled: true\n",
						"services/hangar/hangar.yaml": "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/orin-voice\"\n    var_key: \"CONFIG_YAML\"\n  - rel_path: \"services/haro/haro.yaml\"\n    nomad_var: \"nomad/jobs/haro\"\n    var_key: \"CONFIG_YAML\"\n",
					} {
						full := filepath.Join(dir, rel)
						_ = os.MkdirAll(filepath.Dir(full), 0o755)
						_ = os.WriteFile(full, []byte(content), 0o644)
					}
				},
				modify: func(dir string) {
					_ = os.WriteFile(filepath.Join(dir, "services", "voice", "voice.yaml"), []byte("model: distil-small.en\n"), 0o644)
				},
				syncFn: func(d *SyncDaemon, dir string, sCtx ...ConfigSyncContext) error {
					return d.SyncServiceConfigsToNomad(ctx, dir, sCtx...)
				},
				cacheKey:         "nomad/jobs/orin-voice:CONFIG_YAML",
				wantInitialCalls: 2,
				untouchedCtx:     ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/hangar/hangar.yaml"}},
				sameContentCtx:   ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}, ExpectedJobs: []string{"orin-voice"}},
				modifiedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}, ExpectedJobs: []string{"orin-voice"}},
				wantJobName:      "orin-voice",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newNomadConfigSyncTestEnv(t)
				tc.setup(env.configDir)

				// (a) initial push
				if err := tc.syncFn(env.daemon, env.configDir); err != nil {
					t.Fatalf("initial push failed: %v", err)
				}
				if len(env.nomadCalls()) != tc.wantInitialCalls {
					t.Fatalf("initial push calls = %d, want %d", len(env.nomadCalls()), tc.wantInitialCalls)
				}

				// (b) cached second push
				if err := tc.syncFn(env.daemon, env.configDir); err != nil || len(env.nomadCalls()) != tc.wantInitialCalls {
					t.Fatalf("cached push calls = %d, want %d", len(env.nomadCalls()), tc.wantInitialCalls)
				}

				// (c) diff-gated untouched file
				callsBeforeUntouched := len(env.nomadCalls())
				if err := tc.syncFn(env.daemon, env.configDir, tc.untouchedCtx); err != nil || len(env.nomadCalls()) != callsBeforeUntouched {
					t.Fatalf("untouched diff calls = %d, want %d", len(env.nomadCalls()), callsBeforeUntouched)
				}
				env.daemon.lastPushedConfigMu.Lock()
				cached := env.daemon.lastPushedConfig[tc.cacheKey]
				env.daemon.lastPushedConfigMu.Unlock()
				if cached == "" {
					t.Fatalf("expected cache to be primed for %s", tc.cacheKey)
				}

				// (d) diff-gated same-content file with CommitSHA after periodic sync (not suppressed by un-SHA'd cache, then cached on repeat)
				wantMod := tc.wantModifiedCalls
				if wantMod == 0 {
					wantMod = 1
				}
				callsBeforeSame := len(env.nomadCalls())
				if err := tc.syncFn(env.daemon, env.configDir, tc.sameContentCtx); err != nil || len(env.nomadCalls()) != callsBeforeSame+wantMod {
					t.Fatalf("same content with commitSHA calls = %d, want %d", len(env.nomadCalls()), callsBeforeSame+wantMod)
				}
				sameEvts := env.waitForEvents(t, wantMod)
				if len(sameEvts) == 0 || sameEvts[0].CommitSHA != tc.sameContentCtx.CommitSHA || len(sameEvts[0].ExpectedJobs) != 1 || sameEvts[0].ExpectedJobs[0] != tc.wantJobName {
					t.Fatalf("expected deploy_started with commitSHA %s and expected_jobs [%s], got %+v", tc.sameContentCtx.CommitSHA, tc.wantJobName, sameEvts)
				}
				callsAfterFirstSame := len(env.nomadCalls())
				if err := tc.syncFn(env.daemon, env.configDir, tc.sameContentCtx); err != nil || len(env.nomadCalls()) != callsAfterFirstSame {
					t.Fatalf("repeated same content with same commitSHA calls = %d, want %d", len(env.nomadCalls()), callsAfterFirstSame)
				}
				if err := tc.syncFn(env.daemon, env.configDir); err != nil || len(env.nomadCalls()) != callsAfterFirstSame {
					t.Fatalf("periodic sync after commitSHA push calls = %d, want %d", len(env.nomadCalls()), callsAfterFirstSame)
				}

				// (e) diff-gated modified file
				tc.modify(env.configDir)
				callsBeforeMod := len(env.nomadCalls())
				if err := tc.syncFn(env.daemon, env.configDir, tc.modifiedCtx); err != nil {
					t.Fatalf("modified sync error: %v", err)
				}
				calls := env.nomadCalls()
				if len(calls) != callsBeforeMod+wantMod {
					t.Fatalf("expected %d new call(s), got %d (total %d)", wantMod, len(calls)-callsBeforeMod, len(calls))
				}
				lastCall := strings.Join(calls[len(calls)-1], " ")
				if !strings.Contains(lastCall, "COMMIT_SHA="+tc.modifiedCtx.CommitSHA) {
					t.Fatalf("expected COMMIT_SHA=%s in last call: %s", tc.modifiedCtx.CommitSHA, lastCall)
				}
				evts := env.waitForEvents(t, wantMod)
				var foundJob bool
				for _, evt := range evts {
					if evt.Event == "deploy_started" && evt.JobName == tc.wantJobName && evt.Repo == tc.modifiedCtx.Repo && evt.CommitSHA == tc.modifiedCtx.CommitSHA && evt.PRNumber == tc.modifiedCtx.PRNumber && evt.TargetID == tc.modifiedCtx.TargetID && len(evt.ExpectedJobs) == 1 && evt.ExpectedJobs[0] == tc.wantJobName {
						foundJob = true
						break
					}
				}
				if !foundJob {
					t.Errorf("did not find expected deploy_started event for %s in: %+v", tc.wantJobName, evts)
				}
			})
		}
	})

	t.Run("service_config_mappings_and_non_yaml", func(t *testing.T) {
		t.Parallel()
		for _, m := range DefaultManagedServiceConfigs {
			if strings.Contains(m.NomadVar, "orin-voice") || strings.Contains(m.NomadVar, "mirrormere") {
				t.Errorf("expected DefaultManagedServiceConfigs to remain generic, found: %v", m)
			}
		}

		customDir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(customDir, "services", "hangar"), 0o755)
		_ = os.WriteFile(filepath.Join(customDir, "services", "hangar", "hangar.yaml"), []byte("service_configs:\n  - rel_path: \"services/custom/custom.yaml\"\n    nomad_var: \"nomad/jobs/custom-job\"\n    var_key: \"CONFIG_YAML\"\n"), 0o644)
		d := NewDaemon(DaemonConfig{ConfigDir: customDir})
		if customMappings := d.GetServiceConfigMappings(customDir); len(customMappings) != 1 || customMappings[0].RelPath != "services/custom/custom.yaml" || customMappings[0].NomadVar != "nomad/jobs/custom-job" {
			t.Fatalf("expected custom mappings from hangar.yaml, got %v", customMappings)
		}

		dCustom := &SyncDaemon{serviceConfigs: []ServiceConfigMapping{{RelPath: "services/test/test.yaml", NomadVar: "nomad/jobs/test-job", VarKey: "CONFIG_YAML"}}}
		if daemonMappings := dCustom.GetServiceConfigMappings(t.TempDir()); len(daemonMappings) != 1 || daemonMappings[0].RelPath != "services/test/test.yaml" {
			t.Fatalf("expected daemon-configured mappings, got %v", daemonMappings)
		}

		if defaultMappings := (&SyncDaemon{}).GetServiceConfigMappings(t.TempDir()); len(defaultMappings) != len(DefaultManagedServiceConfigs) {
			t.Fatalf("expected DefaultManagedServiceConfigs fallback, got %d mappings", len(defaultMappings))
		}

		env := newNomadConfigSyncTestEnv(t)
		env.daemon.serviceConfigs = []ServiceConfigMapping{{RelPath: "proxy/default.conf", NomadVar: "nomad/jobs/mesh-proxy", VarKey: "DEFAULT_CONF"}}
		_ = os.MkdirAll(filepath.Join(env.configDir, "proxy"), 0o755)
		_ = os.WriteFile(filepath.Join(env.configDir, "proxy", "default.conf"), []byte("server { listen 80; proxy_pass http://backend; }"), 0o644)
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 1 {
			t.Fatalf("expected non-YAML config to sync with 1 call, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
	})
}

func TestRepoLock_ThreadSafety(t *testing.T) {
	daemon := NewDaemon(DaemonConfig{})
	pathA, pathB := filepath.Clean("/share/aerial"), filepath.Clean("/share/aerial-config")
	lockA1, lockA2, lockB := daemon.getRepoLock(pathA), daemon.getRepoLock(pathA), daemon.getRepoLock(pathB)
	if lockA1 != lockA2 {
		t.Errorf("expected identical mutex pointer for same repo path, got %p vs %p", lockA1, lockA2)
	}
	if lockA1 == lockB {
		t.Errorf("expected distinct mutex pointers for different repo paths")
	}
	var wg sync.WaitGroup
	counter := 0
	for i := 0; i < 50; i++ {
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
	if counter != 50 {
		t.Errorf("expected counter 50, got %d", counter)
	}
}

func TestTriggerReconcile_TableDriven(t *testing.T) {
	ctx := context.Background()
	calledSync := false
	d := &SyncDaemon{reconcileFn: func(ctx context.Context) error {
		calledSync = true
		return nil
	}}
	if err := d.TriggerReconcile(ctx, false); err != nil || !calledSync {
		t.Fatalf("expected TriggerReconcile(false) to execute reconcileFn, err=%v", err)
	}

	dAsync := &SyncDaemon{reconcileCh: make(chan struct{}, 1)}
	if err := dAsync.TriggerReconcile(ctx, true); err != nil {
		t.Fatalf("expected TriggerReconcile(true) to succeed, err=%v", err)
	}
	select {
	case <-dAsync.reconcileCh:
	default:
		t.Errorf("expected message in reconcileCh")
	}

	dAsync.reconcileCh <- struct{}{}
	if err := dAsync.TriggerReconcile(ctx, true); err != nil {
		t.Fatalf("expected TriggerReconcile(true) with full channel to succeed, err=%v", err)
	}

	dNil := &SyncDaemon{}
	if err := dNil.TriggerReconcile(ctx, true); err != nil || dNil.reconcileCh == nil {
		t.Fatalf("expected TriggerReconcile(true) with nil channel to initialize reconcileCh, err=%v", err)
	}
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

func TestExecutorsAndDispatchers_TableDriven(t *testing.T) {
	t.Run("defaultComposeExecutor CLI mocking", func(t *testing.T) {
		tempDir := t.TempDir()
		binDir := filepath.Join(tempDir, "bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			t.Fatalf("failed to create bin dir: %v", err)
		}
		if runtime.GOOS == "windows" {
			script := "@echo off\r\nset \"arg=%*\"\r\necho %arg% | findstr /i \"waitloop\" >nul && goto do_wait\r\necho %arg% | findstr /i \"compose\" >nul && (echo Docker Compose mock v2.0 & exit /b 0)\r\necho %arg% | findstr /i \"version\" >nul && (echo Docker mock v24.0 & exit /b 0)\r\necho unknown cmd >&2\r\nexit /b 1\r\n:do_wait\r\ngoto do_wait\r\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker.cmd"), []byte(script), 0o755)
		} else {
			script := "#!/bin/sh\ncase \"$*\" in\n  *\"waitloop\"*) trap 'exit 0' TERM INT; while :; do :; done;;\n  *\"compose\"*) echo \"Docker Compose mock v2.0\"; exit 0;;\n  *\"version\"*) echo \"Docker mock v24.0\"; exit 0;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker"), []byte(script), 0o755)
		}
		t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

		for _, tc := range []struct {
			name, arg, wantOut string
			cancelCtx, wantErr bool
		}{
			{name: "compose executor success", arg: "version", wantOut: "Docker Compose mock"},
			{name: "compose executor cancel", arg: "waitloop", cancelCtx: true, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()
				}
				stdout, stderr, err := defaultComposeExecutor(ctx, tempDir, tc.arg)
				if (err != nil) != tc.wantErr {
					t.Errorf("error = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.wantOut != "" && !strings.Contains(string(stdout), tc.wantOut) && !strings.Contains(string(stderr), tc.wantOut) {
					t.Errorf("expected output to contain %q, stdout: %s, stderr: %s", tc.wantOut, stdout, stderr)
				}
			})
		}
	})

	t.Run("daemon executor resolution branches", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		if nilDaemon.getGitExecutor() == nil || (&SyncDaemon{}).getGitExecutor() == nil {
			t.Errorf("expected non-nil default git executor")
		}
		customGit := false
		dCustomGit := &SyncDaemon{gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			customGit = true
			return nil, nil, nil
		}}
		_, _, _ = dCustomGit.getGitExecutor()(context.Background(), "")
		if !customGit {
			t.Errorf("expected custom git executor to be called")
		}
		if defaultExec := (&SyncDaemon{pat: "pat"}).getGitExecutor(); defaultExec != nil {
			_, _, _ = defaultExec(context.Background(), t.TempDir(), "version")
		}

		if nilDaemon.getComposeExecutor() == nil || (&SyncDaemon{}).getComposeExecutor() == nil {
			t.Errorf("expected non-nil default compose executor")
		}
		customCompose := false
		dCustomCompose := &SyncDaemon{composeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			customCompose = true
			return nil, nil, nil
		}}
		_, _, _ = dCustomCompose.getComposeExecutor()(context.Background(), "", "")
		if !customCompose {
			t.Errorf("expected custom compose executor to be called")
		}

		if fnNil := nilDaemon.getDeployDispatcher(); fnNil == nil || fnNil(context.Background(), HangarDeployEvent{}) == nil {
			t.Errorf("expected error from deploy dispatcher on nil daemon")
		}
		if fn := (&SyncDaemon{}).getDeployDispatcher(); fn == nil || fn(context.Background(), HangarDeployEvent{}) == nil {
			t.Errorf("expected error from deploy dispatcher when deployDispatcher is nil")
		}
		customDisp := false
		dCustomDisp := &SyncDaemon{deployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			customDisp = true
			return nil
		}}
		if err := dCustomDisp.getDeployDispatcher()(context.Background(), HangarDeployEvent{}); err != nil || !customDisp {
			t.Errorf("expected custom deploy dispatcher to be called: %v", err)
		}
	})

	t.Run("runGitCommand variations", func(t *testing.T) {
		for _, tc := range []struct {
			name, pat, arg        string
			useTempDir, cancelCtx bool
		}{
			{name: "normal version", arg: "version"},
			{name: "with dir init", useTempDir: true, arg: "init"},
			{name: "with pat version", pat: "mock-pat", arg: "version"},
			{name: "canceled context", cancelCtx: true, arg: "status"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				if tc.cancelCtx {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				dir := ""
				if tc.useTempDir {
					dir = t.TempDir()
				}
				_, _, _ = runGitCommand(ctx, dir, tc.pat, tc.arg)
			})
		}
	})

	t.Run("defaultNomadExecutor variations", func(t *testing.T) {
		if nomadExec := defaultNomadExecutor("http://127.0.0.1:4646", "secret-token"); nomadExec != nil {
			_, _, _ = nomadExec(context.Background(), "version")
			cancelCtx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _, _ = nomadExec(cancelCtx, "version")
		}
	})

	t.Run("defaultDeployDispatcher HTTP handling", func(t *testing.T) {
		for _, tc := range []struct {
			name                string
			invalidURL, wantErr bool
			status              int
		}{
			{name: "invalid router URL", invalidURL: true, wantErr: true},
			{name: "HTTP 500 server error response", status: http.StatusInternalServerError, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				routerURL := "://invalid-router-url"
				if !tc.invalidURL {
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(tc.status)
					}))
					defer srv.Close()
					routerURL = srv.URL
				}
				d := &SyncDaemon{webhooksRouterURL: routerURL}
				err := d.defaultDeployDispatcher(context.Background(), HangarDeployEvent{JobName: "test"})
				if (err != nil) != tc.wantErr {
					t.Errorf("defaultDeployDispatcher() error = %v, wantErr = %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("DispatchDeployEvent safeguards and synchronization", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		nilDaemon.DispatchDeployEvent(HangarDeployEvent{})

		panicked := make(chan struct{}, 1)
		dPanic := NewDaemon(DaemonConfig{
			DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
				defer func() { panicked <- struct{}{} }()
				panic("simulated panic in dispatcher")
			},
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				panic("simulated panic in monitor")
			},
		})
		dPanic.DispatchDeployEvent(HangarDeployEvent{JobName: "test-panic"})

		select {
		case <-panicked:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deploy dispatcher panic recovery")
		}
	})
}

type mockErrReadCloser struct{}

func (e *mockErrReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (e *mockErrReadCloser) Close() error               { return errors.New("simulated close error") }

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
func (m *mockErrResponseWriter) Write(p []byte) (int, error) { return 0, m.err }
func (m *mockErrResponseWriter) WriteHeader(statusCode int)  {}

func TestHangarHelpers_TableDriven(t *testing.T) {
	t.Run("closeWarn", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			closer io.Closer
		}{
			{"nil closer", nil},
			{"valid NopCloser", io.NopCloser(strings.NewReader(""))},
			{"mock error closer", &mockErrReadCloser{}},
		} {
			t.Run(tc.name, func(t *testing.T) { httpkit.CloseWarn(tc.closer, tc.name) })
		}
	})

	t.Run("isClientDisconnect", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, tc := range []struct {
			name string
			req  *http.Request
			err  error
			want bool
		}{
			{name: "nil error", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: nil, want: false},
			{name: "canceled request context", req: httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(canceledCtx), err: errors.New("err"), want: true},
			{name: "syscall EPIPE", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: syscall.EPIPE, want: true},
			{name: "syscall ECONNRESET", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: syscall.ECONNRESET, want: true},
			{name: "net ErrClosed", req: httptest.NewRequest(http.MethodGet, "/health", nil), err: net.ErrClosed, want: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := httpkit.IsClientDisconnect(tc.req, tc.err); got != tc.want {
					t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("writeResponse", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		for _, tc := range []struct {
			name string
			w    http.ResponseWriter
			body []byte
			want string
		}{
			{name: "standard ResponseRecorder", w: httptest.NewRecorder(), body: []byte("ok"), want: "ok"},
			{name: "mock error response writer", w: &mockErrResponseWriter{err: errors.New("fail")}, body: []byte("fail")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				httpkit.WriteResponse(tc.w, req, "hangar", tc.body)
				if rec, ok := tc.w.(*httptest.ResponseRecorder); ok && tc.want != "" && rec.Body.String() != tc.want {
					t.Errorf("expected body %q, got %q", tc.want, rec.Body.String())
				}
			})
		}
	})

	t.Run("writeJSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		for _, tc := range []struct {
			name     string
			data     any
			wantCode int
		}{
			{name: "valid JSON map returns 200", data: map[string]string{"foo": "bar"}, wantCode: http.StatusOK},
			{name: "unserializable data returns 500", data: make(chan int), wantCode: http.StatusInternalServerError},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				writeJSON(rec, req, http.StatusOK, tc.data)
				if rec.Code != tc.wantCode {
					t.Errorf("expected status %d, got %d", tc.wantCode, rec.Code)
				}
			})
		}
	})

	t.Run("ReconciliationStatus", func(t *testing.T) {
		var nilD *SyncDaemon
		if st := nilD.GetReconciliationStatus(); st.State != "idle" || st.Stage != "idle" {
			t.Errorf("expected idle for nil daemon, got %+v", st)
		}
		d := NewDaemon(DaemonConfig{})
		if st := d.GetReconciliationStatus(); st.State != "idle" || st.Stage != "idle" {
			t.Errorf("expected idle for daemon, got %+v", st)
		}
		statusResp := d.GetStatus(context.Background())
		if statusResp.Reconciliation == nil || statusResp.Reconciliation.State != "idle" {
			t.Errorf("expected state idle in GetStatus, got %+v", statusResp.Reconciliation)
		}
	})

	t.Run("GetStatus_populates_and_caches_GitHubRepo", func(t *testing.T) {
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

		st1 := d.GetStatus(context.Background())
		repoSt1, ok := st1.Repos["/share/mirrormere"]
		if !ok || repoSt1.GitHubRepo != "azylman/mirrormere" {
			t.Errorf("expected GitHubRepo azylman/mirrormere, got %q", repoSt1.GitHubRepo)
		}
		if remoteCalls != 1 {
			t.Errorf("expected 1 remote call, got %d", remoteCalls)
		}

		st2 := d.GetStatus(context.Background())
		repoSt2, ok := st2.Repos["/share/mirrormere"]
		if !ok || repoSt2.GitHubRepo != "azylman/mirrormere" {
			t.Errorf("expected cached GitHubRepo azylman/mirrormere, got %q", repoSt2.GitHubRepo)
		}
		if remoteCalls != 1 {
			t.Errorf("expected remoteCalls to remain 1 after cached call, got %d", remoteCalls)
		}
	})

	t.Run("RegistryClient_and_LastReconcile_accessors", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		if c := nilDaemon.getRegistryClient(); c == nil {
			t.Errorf("expected default registry client for nil daemon")
		}

		dDefault := &SyncDaemon{}
		c := dDefault.getRegistryClient()
		if c == nil || c.Timeout != 10*time.Second {
			t.Errorf("expected default registry client with 10s timeout, got %v", c)
		}

		custom := &http.Client{Timeout: 42 * time.Second}
		dCustom := NewDaemon(DaemonConfig{RegistryClient: custom})
		if dCustom.getRegistryClient() != custom {
			t.Errorf("expected custom registry client to be returned")
		}

		now := time.Now().Truncate(time.Second)
		dCustom.SetLastReconcile(now)
		if !dCustom.GetLastReconcile().Equal(now) {
			t.Errorf("expected LastReconcile to equal %v, got %v", now, dCustom.GetLastReconcile())
		}
	})
}

type nomadImagePollCase struct {
	name             string
	emptyDir         bool
	isAerialRepo     bool
	jobFile          string
	jobContent       string
	seedNonNomad     bool
	clientMode       string // "dynamic_digest", "oci_revision"
	initialDigest    string
	secondDigest     string
	thirdDigest      string
	imageRevision    string
	knownDigestKey   string
	knownDigestVal   string
	wantRestartJob   string
	wantEventRepo    string
	wantEventCommit  string
	wantDigest       string
	testRestartError bool
}

type imagePollTestEnv struct {
	daemon        *SyncDaemon
	evtCh         chan HangarDeployEvent
	restartedJob  *string
	restartCalled *bool
	currentDigest *string
}

func newImagePollTestEnv(t *testing.T, tc nomadImagePollCase) *imagePollTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	baseDir := tmpDir
	if tc.isAerialRepo {
		baseDir = filepath.Join(tmpDir, "aerial")
	}
	jobsDir := filepath.Join(baseDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0o755)

	if tc.seedNonNomad {
		_ = os.MkdirAll(filepath.Join(jobsDir, "nested-dir"), 0o755)
		_ = os.WriteFile(filepath.Join(jobsDir, "readme.txt"), []byte("not a nomad file"), 0o644)
	}
	if tc.jobFile != "" {
		_ = os.WriteFile(filepath.Join(jobsDir, tc.jobFile), []byte(tc.jobContent), 0o644)
	}

	curDigest := tc.initialDigest
	restarted := ""
	restartCalled := false
	evtCh := make(chan HangarDeployEvent, 10)

	var registryClient *http.Client
	if tc.clientMode == "oci_revision" {
		manifestWithAnnotations := fmt.Sprintf(`{
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"config": {
				"digest": "sha256:dummyblobdigest11111111111111111111111111111111111111111111111111"
			},
			"annotations": {
				"org.opencontainers.image.revision": "%s"
			}
		}`, tc.imageRevision)
		registryClient = newMockOCIRegistryClient(mockOCIOptions{
			OnRequest: func(req *http.Request) (*http.Response, error, bool) {
				if req.Method == http.MethodHead {
					h := make(http.Header)
					h.Set("Docker-Content-Digest", curDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     h,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil, true
				}
				if req.Method == http.MethodGet {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(manifestWithAnnotations)),
					}, nil, true
				}
				return nil, nil, false
			},
		})
	} else {
		registryClient = &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.URL != nil && strings.Contains(req.URL.String(), "test/img1") {
						header := make(http.Header)
						header.Set("Docker-Content-Digest", curDigest)
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     header,
							Body:       ioNopCloser(strings.NewReader("")),
						}, nil
					}
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       ioNopCloser(strings.NewReader("not found")),
					}, nil
				},
			},
		}
	}

	mockNomad := func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 4 && args[0] == "job" && args[1] == "restart" {
			restartCalled = true
			restarted = args[len(args)-1]
			return []byte("Job restart scheduled\n"), nil, nil
		}
		return nil, nil, nil
	}

	d := NewDaemon(DaemonConfig{
		Repos:          []string{baseDir},
		ComposeDir:     baseDir,
		ConfigDir:      baseDir,
		NomadExecutor:  mockNomad,
		RegistryClient: registryClient,
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

	return &imagePollTestEnv{
		daemon:        d,
		evtCh:         evtCh,
		restartedJob:  &restarted,
		restartCalled: &restartCalled,
		currentDigest: &curDigest,
	}
}

func TestCheckAndReconcileNomadImages_TableDriven(t *testing.T) {
	ctx := context.Background()

	pollCases := []nomadImagePollCase{
		{
			name:     "missing_jobs_dir_noop",
			emptyDir: true,
		},
		{
			name:             "initial_discovery_throttle_and_unchanged_digest",
			jobFile:          "job1.nomad",
			jobContent:       `job "job1" { task "t" { config { image = "ghcr.io/test/img1:latest" } } }`,
			seedNonNomad:     true,
			clientMode:       "dynamic_digest",
			initialDigest:    "sha256:firstdigest111",
			secondDigest:     "sha256:firstdigest111",
			testRestartError: true,
		},
		{
			name:             "changed_digest_git_head_fallback_and_restart_error",
			isAerialRepo:     true,
			jobFile:          "svc.nomad",
			jobContent:       "job \"svc\" {\n  group \"g\" {\n    task \"t\" {\n      driver = \"docker\"\n      config {\n        image = \"ghcr.io/test/img1:latest\"\n      }\n    }\n  }\n}",
			clientMode:       "dynamic_digest",
			initialDigest:    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			secondDigest:     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			thirdDigest:      "sha256:3333333333333333333333333333333333333333333333333333333333333333",
			wantRestartJob:   "svc",
			wantEventRepo:    "azylman/aerial",
			wantEventCommit:  "poll_commit_sha_123",
			testRestartError: true,
		},
		{
			name:            "changed_digest_uses_oci_image_revision",
			jobFile:         "webhooks-router.nomad",
			jobContent:      `job "webhooks-router" { group "core" { task "router" { config { image = "ghcr.io/azylman/aerial-webhooks-router:latest" } } } }`,
			clientMode:      "oci_revision",
			initialDigest:   "sha256:9999999999999999999999999999999999999999999999999999999999999999",
			imageRevision:   "9876543210abcdef9876543210abcdef98765432",
			knownDigestKey:  "webhooks-router:ghcr.io/azylman/aerial-webhooks-router:latest",
			knownDigestVal:  "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			wantRestartJob:  "webhooks-router",
			wantEventRepo:   "azylman/aerial",
			wantEventCommit: "9876543210abcdef9876543210abcdef98765432",
			wantDigest:      "sha256:9999999999999999999999999999999999999999999999999999999999999999",
		},
	}

	for _, tc := range pollCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.emptyDir {
				emptyDir := t.TempDir()
				d := NewDaemon(DaemonConfig{ComposeDir: emptyDir, ConfigDir: emptyDir})
				if err := d.CheckAndReconcileNomadImages(ctx); err != nil {
					t.Errorf("expected nil error for missing jobs directory, got %v", err)
				}
				return
			}

			env := newImagePollTestEnv(t, tc)

			if tc.knownDigestKey != "" {
				env.daemon.nomadKnownDigests[tc.knownDigestKey] = tc.knownDigestVal
			} else {
				env.daemon.nomadKnownDigests = nil
			}

			// 1. Initial poll
			if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
				t.Fatalf("first poll unexpected error: %v", err)
			}

			if tc.clientMode == "oci_revision" {
				if !*env.restartCalled || *env.restartedJob != tc.wantRestartJob {
					t.Fatalf("expected restart for %s, got %s", tc.wantRestartJob, *env.restartedJob)
				}
				select {
				case evt := <-env.evtCh:
					if evt.Event != "deploy_started" || evt.JobName != tc.wantRestartJob || evt.CommitSHA != tc.wantEventCommit || evt.Repo != tc.wantEventRepo || evt.Digest != tc.wantDigest {
						t.Errorf("unexpected event: %+v", evt)
					}
				case <-time.After(2 * time.Second):
					t.Errorf("timed out waiting for deploy_started event")
				}
				return
			}

			if tc.knownDigestKey == "" && *env.restartedJob != "" {
				t.Fatalf("expected no restart on initial discovery, got %s", *env.restartedJob)
			}

			// 2. Throttle test (immediate second poll)
			if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
				t.Fatalf("throttled poll unexpected error: %v", err)
			}

			// 3. Reset throttle for 3rd poll
			env.daemon.nomadPollMu.Lock()
			env.daemon.lastNomadImagePoll = time.Time{}
			env.daemon.nomadPollMu.Unlock()

			if tc.name == "initial_discovery_throttle_and_unchanged_digest" {
				if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
					t.Fatalf("third poll unchanged digest unexpected error: %v", err)
				}
				if *env.restartedJob != "" {
					t.Fatalf("expected no restart when digest matches, got %s", *env.restartedJob)
				}
				if tc.testRestartError {
					env.daemon.nomadPollMu.Lock()
					env.daemon.lastNomadImagePoll = time.Time{}
					env.daemon.nomadPollMu.Unlock()
					*env.currentDigest = "sha256:seconddigest222"
					env.daemon.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
						if len(args) > 1 && args[1] == "restart" {
							return []byte("restart err"), []byte("err details"), errors.New("restart fail")
						}
						return []byte("ok"), nil, nil
					}
					if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
						t.Errorf("unexpected error on restart fail CheckAndReconcileNomadImages: %v", err)
					}
				}
				return
			}

			// Unchanged digest poll first
			if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
				t.Fatalf("unchanged digest poll unexpected error: %v", err)
			}
			if *env.restartedJob != "" {
				t.Fatalf("expected no restart when digest matches, got %s", *env.restartedJob)
			}

			// Reset throttle and change digest
			env.daemon.nomadPollMu.Lock()
			env.daemon.lastNomadImagePoll = time.Time{}
			env.daemon.nomadPollMu.Unlock()

			*env.currentDigest = tc.secondDigest
			if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
				t.Fatalf("changed digest poll unexpected error: %v", err)
			}
			if *env.restartedJob != tc.wantRestartJob {
				t.Errorf("expected restart for %s, got %s", tc.wantRestartJob, *env.restartedJob)
			}
			select {
			case evt := <-env.evtCh:
				if evt.Event != "deploy_started" || evt.CommitSHA != tc.wantEventCommit || evt.Repo != tc.wantEventRepo {
					t.Errorf("expected deploy_started event for %s, got %+v", tc.wantRestartJob, evt)
				}
			case <-time.After(2 * time.Second):
				t.Errorf("timed out waiting for deploy_started event")
			}

			if tc.testRestartError {
				env.daemon.nomadPollMu.Lock()
				env.daemon.lastNomadImagePoll = time.Time{}
				env.daemon.nomadPollMu.Unlock()
				*env.currentDigest = tc.thirdDigest
				env.daemon.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("nomad down"), errors.New("restart fail")
				}
				if err := env.daemon.CheckAndReconcileNomadImages(ctx); err != nil {
					t.Fatalf("poll with restart fail returned error: %v", err)
				}
			}
		})
	}
}

type pushEventTestEnv struct {
	tmpDir          string
	aerialDir       string
	aerialConfigDir string
	mirrormereDir   string
	repos           []string
	daemon          *SyncDaemon
	evtCh           chan HangarDeployEvent
	appliedPaths    []string
	varPutJobs      []string
	mu              sync.Mutex
}

func newPushEventTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *pushEventTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	_ = os.MkdirAll(aerialDir, 0o755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0o755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0o755)
	_ = os.MkdirAll(mirrormereDir, 0o755)
	_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0o755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0o755)
	_ = os.MkdirAll(filepath.Join(mirrormereDir, ".git"), 0o755)

	repos := []string{aerialDir, aerialConfigDir, mirrormereDir}
	evtCh := make(chan HangarDeployEvent, 50)

	cfg := DaemonConfig{
		Repos:      repos,
		ComposeDir: aerialDir,
		ConfigDir:  aerialConfigDir,
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			select {
			case evtCh <- evt:
			default:
			}
			return nil
		},
		GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
			if len(args) >= 1 && args[0] == "rev-parse" {
				return []byte("sha_after\n"), nil, nil
			}
			return []byte(""), nil, nil
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return []byte("Evaluation ID: default-123"), nil, nil
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	d := NewDaemon(cfg)
	return &pushEventTestEnv{
		tmpDir:          tmpDir,
		aerialDir:       aerialDir,
		aerialConfigDir: aerialConfigDir,
		mirrormereDir:   mirrormereDir,
		repos:           repos,
		daemon:          d,
		evtCh:           evtCh,
	}
}

func (e *pushEventTestEnv) collectEvents(count int, timeout time.Duration) []HangarDeployEvent {
	var evts []HangarDeployEvent
	deadline := time.After(timeout)
	for i := 0; i < count; i++ {
		select {
		case evt := <-e.evtCh:
			evts = append(evts, evt)
		case <-deadline:
			return evts
		}
	}
	return evts
}

type mockGitScript struct {
	custom     bool
	beforeSHA  string
	afterSHA   string
	nameOnly   string
	nameStatus string
	failCmd    string
}

func (e *pushEventTestEnv) setGitScript(s mockGitScript) {
	var revCalls int
	e.daemon.gitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		cmd := strings.Join(args, " ")
		if s.failCmd != "" {
			for _, part := range strings.Split(s.failCmd, ",") {
				if strings.Contains(cmd, strings.TrimSpace(part)) {
					return nil, []byte("git error"), errors.New("simulated " + s.failCmd + " failure")
				}
			}
		}
		switch {
		case len(args) >= 2 && args[0] == "rev-parse" && args[1] == "HEAD":
			revCalls++
			if revCalls == 1 || s.afterSHA == "" {
				if s.beforeSHA == "" {
					return []byte(""), nil, nil
				}
				return []byte(s.beforeSHA + "\n"), nil, nil
			}
			return []byte(s.afterSHA + "\n"), nil, nil
		case strings.Contains(cmd, "--name-only"):
			return []byte(s.nameOnly), nil, nil
		case strings.Contains(cmd, "--name-status"):
			return []byte(s.nameStatus), nil, nil
		default:
			return []byte("ok"), nil, nil
		}
	}
}

func (env *pushEventTestEnv) setupCase(tc gitPushCase) {
	if tc.nomadAddr != "" {
		env.daemon.nomadAddr = tc.nomadAddr
	}
	if tc.serviceFiles {
		_ = os.MkdirAll(filepath.Join(env.aerialConfigDir, "services", "voice"), 0o755)
		_ = os.MkdirAll(filepath.Join(env.aerialConfigDir, "services", "hangar"), 0o755)
		_ = os.WriteFile(filepath.Join(env.aerialConfigDir, "services", "voice", "voice.yaml"), []byte("model: small.en\n"), 0o644)
		hangarContent := "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/orin-voice\"\n    var_key: \"CONFIG_YAML\"\n"
		_ = os.WriteFile(filepath.Join(env.aerialConfigDir, "services", "hangar", "hangar.yaml"), []byte(hangarContent), 0o644)
	}
	targetDir := env.aerialDir
	if strings.Contains(tc.req.Repo, "aerial-config") {
		targetDir = env.aerialConfigDir
	}
	for p, content := range tc.jobFiles {
		fullP := filepath.Join(targetDir, p)
		_ = os.MkdirAll(filepath.Dir(fullP), 0o755)
		_ = os.WriteFile(fullP, []byte(content), 0o644)
	}
	env.daemon.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
			if tc.nomadFailCmd == "validate" {
				return nil, []byte("validate error"), errors.New("invalid job")
			}
			if tc.nomadFailCmd == "complex" && strings.Contains(args[len(args)-1], "invalid.nomad") {
				return nil, []byte("syntax error"), errors.New("invalid job")
			}
			return []byte("OK"), nil, nil
		}
		if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
			env.mu.Lock()
			env.appliedPaths = append(env.appliedPaths, args[len(args)-1])
			env.mu.Unlock()
			if tc.nomadFailCmd == "run" {
				return nil, []byte("driver error: failed to place allocation"), errors.New("nomad run failed")
			}
			if tc.nomadFailCmd == "complex" && strings.Contains(args[len(args)-1], "broken.nomad") {
				return nil, []byte("run error"), errors.New("run failed")
			}
			return []byte("Evaluation ID: default-123"), nil, nil
		}
		if len(args) >= 2 && args[0] == "var" && args[1] == "put" {
			if len(args) >= 4 {
				env.mu.Lock()
				env.varPutJobs = append(env.varPutJobs, args[3])
				env.mu.Unlock()
			}
			if tc.nomadFailCmd == "var" {
				return nil, []byte("nomad var put permission denied"), errors.New("var put failed")
			}
			return []byte("ok"), nil, nil
		}
		return []byte("OK"), nil, nil
	}
	switch tc.registryMode {
	case "not_found":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`))}, nil
			}
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))}, nil
		}}}
	case "runs_active":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 1, "workflow_runs": [{}]}`))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{}`))}, nil
		}}}
	case "digest_ready":
		env.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "actions/runs") {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`))}, nil
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{}`))}
			resp.Header.Set("Docker-Content-Digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			return resp, nil
		}}}
	}
	if tc.gitScript != (mockGitScript{}) {
		env.setGitScript(tc.gitScript)
	}
}

type gitPushCase struct {
	name                 string
	req                  GitPushEventRequest
	gitScript            mockGitScript
	jobFiles             map[string]string
	serviceFiles         bool
	nomadAddr            string
	nomadFailCmd         string // "validate", "run", "var", "complex"
	registryMode         string // "not_found", "runs_active", "digest_ready"
	wantCode             int
	wantStatus           string
	wantMessage          string
	wantBuilding         bool
	wantNomadChanged     bool
	wantApplied          []string
	wantExpectedJobs     []string
	wantEvents           []string
	wantCommit           string
	wantDeployJob        string
	checkDeferralJob     string
	checkZeroVarPutCalls bool
	checkPendingDeletes  []string
}

func TestExecuteGitPushEvent_TableDriven(t *testing.T) { // t.Run(
	ctx := context.Background()
	jobWithAerialImg := `job "webhooks-router" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-webhooks-router:latest"
			}
		}
	}`

	cases := []gitPushCase{
		{
			name:       "missing repo",
			req:        GitPushEventRequest{},
			wantCode:   http.StatusBadRequest,
			wantStatus: "error",
		},
		{
			name:       "non-main branch",
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/feature-branch"},
			wantCode:   http.StatusOK,
			wantStatus: "ignored",
		},
		{
			name:       "unmanaged repository",
			req:        GitPushEventRequest{Repo: "nonexistent-repo", Ref: "refs/heads/main"},
			wantCode:   http.StatusNotFound,
			wantStatus: "error",
		},
		{
			name:       "sync failure",
			gitScript:  mockGitScript{failCmd: "fetch,pull"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
		},
		{
			name:        "peripheral repo",
			req:         GitPushEventRequest{Repo: "mirrormere", Ref: "refs/heads/main"},
			wantCode:    http.StatusOK,
			wantStatus:  "accepted",
			wantMessage: "repository synced",
		},
		{
			name:       "successful sync with target id",
			gitScript:  mockGitScript{beforeSHA: "sha_sync_ok"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_12345", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
		},
		{
			name:       "failed sync with target id dispatches sync_failed",
			gitScript:  mockGitScript{failCmd: "fetch,pull"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_fail", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantEvents: []string{"sync_failed"},
		},
		{
			name:       "empty current head falls back to req commit",
			gitScript:  mockGitScript{custom: true},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_req_fallback", TargetID: "1555405874565091380", PRNumber: 542},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
			wantCommit: "commit_req_fallback",
		},
		{
			name:       "empty target id still dispatches sync_success",
			gitScript:  mockGitScript{beforeSHA: "sha_sync_ok"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", Commit: "commit_empty_target"},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantEvents: []string{"sync_success"},
			wantCommit: "sha_sync_ok",
		},
		{
			name:         "build changes",
			gitScript:    mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/eri/main.go\nDockerfile\n"},
			req:          GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:     http.StatusOK,
			wantBuilding: true,
		},
		{
			name:             "non-build nomad change applied directly",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "nomad/jobs/migrate.nomad\n", nameStatus: "M nomad/jobs/migrate.nomad\n"},
			jobFiles:         map[string]string{"nomad/jobs/migrate.nomad": `job "migrate" {}`},
			req:              GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main", PRNumber: 611, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantApplied:      []string{"migrate"},
			wantExpectedJobs: []string{"migrate"},
			wantDeployJob:    "migrate",
		},
		{
			name:             "nomad apply failure returns 500",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "nomad/jobs/migrate.nomad\n", nameStatus: "M nomad/jobs/migrate.nomad\n"},
			jobFiles:         map[string]string{"nomad/jobs/migrate.nomad": `job "migrate" {}`},
			nomadFailCmd:     "validate",
			req:              GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:         http.StatusInternalServerError,
			wantStatus:       "error",
			wantNomadChanged: true,
		},
		{
			name:       "no changes",
			gitScript:  mockGitScript{beforeSHA: "sha1", nameOnly: "docs/index.html\n"},
			req:        GitPushEventRequest{Repo: "aerial", Ref: "refs/heads/main"},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
		},
		{
			name:      "mixed build and nomad deferral",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/vanadis/Dockerfile\nnomad/jobs/vanadis.nomad\nnomad/jobs/coverage-ingest.nomad\n", nameStatus: "A\tnomad/jobs/vanadis.nomad\nM\tnomad/jobs/coverage-ingest.nomad\n"},
			jobFiles: map[string]string{
				"nomad/jobs/vanadis.nomad":         `job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`,
				"nomad/jobs/coverage-ingest.nomad": `job "coverage-ingest" { task "flush" { config { image = "golang:1.27.1-alpine" } } }`,
			},
			registryMode:     "not_found",
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after", PRNumber: 719, TargetID: "1560318118792790111"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"coverage-ingest"},
			wantExpectedJobs: []string{"coverage-ingest", "vanadis"},
			checkDeferralJob: "vanadis",
		},
		{
			name:      "mixed build and untouched repo nomad job applies untouched job immediately",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameOnly: "services/hangar/main.go\nnomad/jobs/hangar.nomad\nnomad/jobs/eri.nomad\n", nameStatus: "M\tnomad/jobs/hangar.nomad\nM\tnomad/jobs/eri.nomad\n"},
			jobFiles: map[string]string{
				"nomad/jobs/hangar.nomad": `job "hangar" { task "hangar" { config { image = "ghcr.io/azylman/aerial-hangar:${var.image_tag}" } } }`,
				"nomad/jobs/eri.nomad":    `job "eri" { task "eri" { config { image = "ghcr.io/azylman/aerial-eri:${var.image_tag}" } } }`,
			},
			registryMode:     "digest_ready",
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after", PRNumber: 948, TargetID: "1562543935417487501"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"eri"},
			wantExpectedJobs: []string{"eri", "hangar"},
		},
		{
			name: "job rename with containers building defers delete",
			gitScript: mockGitScript{
				beforeSHA:  "sha_rename_before",
				afterSHA:   "sha_rename_after",
				nameOnly:   "services/coven/Dockerfile\nnomad/jobs/memory-mcp.nomad\nnomad/jobs/coven.nomad\n",
				nameStatus: "D\tnomad/jobs/memory-mcp.nomad\nA\tnomad/jobs/coven.nomad\n",
			},
			jobFiles: map[string]string{
				"nomad/jobs/coven.nomad": `job "coven" { task "coven" { config { image = "ghcr.io/azylman/aerial-coven:${var.image_tag}" } } }`,
			},
			registryMode:        "not_found",
			req:                 GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_rename_after", PRNumber: 800, TargetID: "1560318118792790999"},
			wantCode:            http.StatusOK,
			wantBuilding:        true,
			wantNomadChanged:    true,
			wantApplied:         nil,
			wantExpectedJobs:    []string{"coven"},
			checkPendingDeletes: []string{"memory-mcp"},
		},
		{
			name:             "migration change runs db-migrate immediately",
			gitScript:        mockGitScript{beforeSHA: "sha_before_mig", afterSHA: "sha_after_mig", nameOnly: "services/eri/pkg/db/migrations/20261016182900_add_started_at.sql\nservices/eri/pkg/db/vanadis.go\n", nameStatus: "A\tservices/eri/pkg/db/migrations/20261016182900_add_started_at.sql\nM\tservices/eri/pkg/db/vanadis.go\n"},
			jobFiles:         map[string]string{"nomad/jobs/db-migrate.nomad": `job "db-migrate" { type = "batch" task "atlas" { config { image = "arigaio/atlas:1.3.3" } } }`},
			req:              GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after_mig", PRNumber: 751, TargetID: "1560698400780062884"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantApplied:      []string{"db-migrate"},
			wantExpectedJobs: []string{"db-migrate"},
		},
		{
			name:         "config sync notices nomad var put fails gracefully",
			gitScript:    mockGitScript{beforeSHA: "sha123"},
			nomadFailCmd: "var",
			req:          GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha123"},
			wantCode:     http.StatusOK,
		},
		{
			name:                 "diff gating untouched service configs skip nomad var put",
			gitScript:            mockGitScript{beforeSHA: "sha_prev", afterSHA: "sha_curr", nameOnly: "services/hangar/hangar.yaml\n"},
			nomadAddr:            "http://127.0.0.1:4646",
			serviceFiles:         true,
			req:                  GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha_curr"},
			wantCode:             http.StatusOK,
			checkZeroVarPutCalls: true,
		},
		{
			name:             "aerial-config variable mapping resolves expected_jobs and dispatches on sync_success and deploy_started",
			gitScript:        mockGitScript{beforeSHA: "sha_prev", afterSHA: "sha_curr", nameOnly: "services/voice/voice.yaml\n"},
			nomadAddr:        "http://127.0.0.1:4646",
			serviceFiles:     true,
			req:              GitPushEventRequest{Repo: "azylman/aerial-config", Commit: "sha_curr", PRNumber: 301, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantExpectedJobs: []string{"orin-voice"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deferred nomad job due to active build",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": jobWithAerialImg},
			registryMode:     "runs_active",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantExpectedJobs: []string{"webhooks-router"},
		},
		{
			name:             "image ready applies nomad job and dispatches deploy_started",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": jobWithAerialImg},
			registryMode:     "digest_ready",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "commit_abc123", PRNumber: 255, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantApplied:      []string{"webhooks-router"},
			wantExpectedJobs: []string{"webhooks-router"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deploy failed dispatches deploy_failed",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/webhooks-router.nomad\n"},
			jobFiles:         map[string]string{"jobs/webhooks-router.nomad": `job "webhooks-router" {}`},
			nomadFailCmd:     "run",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "fail_commit_123"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantEvents:       []string{"sync_success", "deploy_started", "deploy_failed"},
		},
		{
			name:      "complex aerial-config diff",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "D jobs/deleted.nomad\nM jobs/deferred.nomad\nM jobs/invalid.nomad\nM jobs/nonexistent.nomad\nM jobs/broken.nomad\n"},
			jobFiles: map[string]string{
				"jobs/invalid.nomad": `job "invalid" {}`,
				"jobs/broken.nomad":  `job "broken" {}`,
			},
			nomadFailCmd:     "complex",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
		},
		{
			name:       "config diff error returns 500",
			gitScript:  mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", failCmd: "diff"},
			req:        GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newPushEventTestEnv(t)
			env.setupCase(tc)
			resp, code := env.daemon.ExecuteGitPushEvent(ctx, tc.req)
			if code != tc.wantCode {
				t.Fatalf("ExecuteGitPushEvent() code = %d, want %d (resp: %+v)", code, tc.wantCode, resp)
			}
			if tc.wantStatus != "" && resp.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if tc.wantMessage != "" && resp.Message != tc.wantMessage {
				t.Errorf("message = %q, want %q", resp.Message, tc.wantMessage)
			}
			if resp.ContainersBuilding != tc.wantBuilding {
				t.Errorf("ContainersBuilding = %v, want %v", resp.ContainersBuilding, tc.wantBuilding)
			}
			if resp.NomadChanged != tc.wantNomadChanged {
				t.Errorf("NomadChanged = %v, want %v", resp.NomadChanged, tc.wantNomadChanged)
			}
			if tc.wantApplied != nil {
				if len(resp.AppliedJobs) != len(tc.wantApplied) {
					t.Errorf("AppliedJobs = %v, want %v", resp.AppliedJobs, tc.wantApplied)
				} else {
					for i, job := range tc.wantApplied {
						if resp.AppliedJobs[i] != job {
							t.Errorf("AppliedJobs[%d] = %q, want %q", i, resp.AppliedJobs[i], job)
						}
					}
				}
			}
			if tc.wantExpectedJobs != nil {
				if len(resp.ExpectedJobs) != len(tc.wantExpectedJobs) {
					t.Errorf("ExpectedJobs = %v, want %v", resp.ExpectedJobs, tc.wantExpectedJobs)
				} else {
					for i, job := range tc.wantExpectedJobs {
						if resp.ExpectedJobs[i] != job {
							t.Errorf("ExpectedJobs[%d] = %q, want %q", i, resp.ExpectedJobs[i], job)
						}
					}
				}
			}
			if tc.checkZeroVarPutCalls {
				env.mu.Lock()
				if len(env.varPutJobs) != 0 {
					t.Errorf("expected zero var put calls for untouched service configs, got: %v", env.varPutJobs)
				}
				env.mu.Unlock()
			}
			if tc.wantDeployJob != "" {
				evts := env.collectEvents(2, 2*time.Second)
				var found bool
				for _, evt := range evts {
					if evt.JobName == tc.wantDeployJob {
						found = true
						if tc.req.PRNumber != 0 && evt.PRNumber != tc.req.PRNumber {
							t.Errorf("PRNumber = %d, want %d", evt.PRNumber, tc.req.PRNumber)
						}
						if tc.req.TargetID != "" && evt.TargetID != tc.req.TargetID {
							t.Errorf("TargetID = %q, want %q", evt.TargetID, tc.req.TargetID)
						}
						if tc.wantExpectedJobs != nil && len(evt.ExpectedJobs) != len(tc.wantExpectedJobs) {
							t.Errorf("deploy event ExpectedJobs = %v, want %v", evt.ExpectedJobs, tc.wantExpectedJobs)
						}
					}
				}
				if !found {
					t.Errorf("missing %s deploy event in: %+v", tc.wantDeployJob, evts)
				}
			}
			if len(tc.wantEvents) > 0 {
				evts := env.collectEvents(len(tc.wantEvents), 2*time.Second)
				for _, wantEvt := range tc.wantEvents {
					var found bool
					for _, evt := range evts {
						if evt.Event == wantEvt {
							found = true
							if tc.wantCommit != "" && evt.CommitSHA != tc.wantCommit {
								t.Errorf("commitSHA = %q, want %q", evt.CommitSHA, tc.wantCommit)
							}
							if tc.req.PRNumber != 0 && evt.PRNumber != tc.req.PRNumber {
								t.Errorf("PRNumber = %d, want %d", evt.PRNumber, tc.req.PRNumber)
							}
							if tc.req.TargetID != "" && evt.TargetID != tc.req.TargetID {
								t.Errorf("TargetID = %q, want %q", evt.TargetID, tc.req.TargetID)
							}
							if tc.wantExpectedJobs != nil && (wantEvt == "sync_success" || wantEvt == "deploy_started") {
								if len(evt.ExpectedJobs) != len(tc.wantExpectedJobs) {
									t.Errorf("%s ExpectedJobs = %v, want %v", wantEvt, evt.ExpectedJobs, tc.wantExpectedJobs)
								}
							}
							if wantEvt == "deploy_failed" {
								if evt.Status != "failed" || !strings.Contains(evt.Details, "driver error") {
									t.Errorf("expected failed status and driver error details, got: %+v", evt)
								}
							}
						}
					}
					if !found {
						t.Errorf("missing expected event %q in: %+v", wantEvt, evts)
					}
				}
			}
			if tc.checkDeferralJob != "" {
				if pending := env.daemon.drainPendingNomadChanges(); len(pending) != 0 {
					t.Fatalf("expected zero pendingNomadChanges, got %+v", pending)
				}
				if err := env.daemon.ReconcileNomadChanges(ctx, env.aerialDir, []NomadFileChange{{Path: "nomad/jobs/" + tc.checkDeferralJob + ".nomad", Action: "apply", JobName: tc.checkDeferralJob}}); err != nil {
					t.Fatalf("unexpected error in ReconcileNomadChanges: %v", err)
				}
				env.mu.Lock()
				for _, p := range env.appliedPaths {
					if strings.Contains(p, tc.checkDeferralJob+".nomad") {
						t.Fatalf("expected %s.nomad to be deferred when image is missing in registry, got: %v", tc.checkDeferralJob, env.appliedPaths)
					}
				}
				env.mu.Unlock()
			}
			if len(tc.checkPendingDeletes) > 0 {
				pending := env.daemon.drainPendingNomadChanges()
				var gotDeletedJobs []string
				for _, evt := range pending {
					for _, ch := range evt.Changes {
						if ch.Action == "delete" {
							gotDeletedJobs = append(gotDeletedJobs, ch.JobName)
						}
					}
				}
				if len(gotDeletedJobs) != len(tc.checkPendingDeletes) {
					t.Fatalf("pending deleted jobs = %v, want %v", gotDeletedJobs, tc.checkPendingDeletes)
				}
				for i, j := range tc.checkPendingDeletes {
					if gotDeletedJobs[i] != j {
						t.Errorf("pending deleted job[%d] = %q, want %q", i, gotDeletedJobs[i], j)
					}
				}
			}
		})
	}
}

type imageReadyTestEnv struct {
	t           *testing.T
	ctx         context.Context
	tmpDir      string
	jobsDir     string
	coreJobsDir string
	daemon      *SyncDaemon
	evtCh       chan HangarDeployEvent
	runCalls    []string
	runMu       sync.Mutex
	restarts    []string
	restartMu   sync.Mutex
	stoppedJobs []string
	stopMu      sync.Mutex
	valErr      error
	runErr      error
	restartErr  error
	runSignal   chan string
}

func newImageReadyTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *imageReadyTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0o755)

	coreJobsDir := filepath.Join(tmpDir, "core", "nomad", "jobs")
	_ = os.MkdirAll(coreJobsDir, 0o755)

	env := &imageReadyTestEnv{
		t:           t,
		ctx:         context.Background(),
		tmpDir:      tmpDir,
		jobsDir:     jobsDir,
		coreJobsDir: coreJobsDir,
		evtCh:       make(chan HangarDeployEvent, 20),
	}

	cfg := DaemonConfig{
		ConfigDir:  tmpDir,
		ComposeDir: filepath.Join(tmpDir, "core"),
		DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
			select {
			case env.evtCh <- evt:
			default:
			}
			return nil
		},
		RegistryClient: &http.Client{
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
		},
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" {
				switch args[1] {
				case "validate":
					if env.valErr != nil {
						return nil, []byte("syntax error in HCL"), env.valErr
					}
					return []byte("OK"), nil, nil
				case "run":
					env.runMu.Lock()
					env.runCalls = append(env.runCalls, strings.Join(args, " "))
					env.runMu.Unlock()
					if env.runSignal != nil {
						select {
						case env.runSignal <- strings.Join(args, " "):
						default:
						}
					}
					if env.runErr != nil {
						return nil, []byte("Nomad agent connection refused"), env.runErr
					}
					return []byte("Evaluation ID: eval-success-123"), nil, nil
				case "restart":
					env.restartMu.Lock()
					if len(args) >= 3 {
						env.restarts = append(env.restarts, args[len(args)-1])
					}
					env.restartMu.Unlock()
					if env.restartErr != nil {
						return nil, []byte("restart failed warning"), env.restartErr
					}
					return []byte("Restart triggered"), nil, nil
				case "stop":
					env.stopMu.Lock()
					if len(args) >= 4 {
						env.stoppedJobs = append(env.stoppedJobs, args[len(args)-1])
					}
					env.stopMu.Unlock()
					return []byte("OK"), nil, nil
				case "status":
					return []byte(`{"ID":"status-ok","JobVersion":1,"LatestDeployment":{"ID":"dep-1","Status":"successful"}}`), nil, nil
				}
			}
			return []byte("OK"), nil, nil
		},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	env.daemon = NewDaemon(cfg)
	return env
}

func (e *imageReadyTestEnv) writeJob(relDir, filename, hcl string) string {
	e.t.Helper()
	var targetDir string
	if relDir == "core" {
		targetDir = e.coreJobsDir
	} else {
		targetDir = e.jobsDir
	}
	path := filepath.Join(targetDir, filename)
	if err := os.WriteFile(path, []byte(hcl), 0o644); err != nil {
		e.t.Fatalf("failed writing job file %s: %v", path, err)
	}
	return path
}

func (e *imageReadyTestEnv) getDigest(jobKey string) string {
	e.daemon.nomadDigestsMu.RLock()
	defer e.daemon.nomadDigestsMu.RUnlock()
	return e.daemon.nomadKnownDigests[jobKey]
}

func (e *imageReadyTestEnv) getRunCalls() []string {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return append([]string(nil), e.runCalls...)
}

func (e *imageReadyTestEnv) getRestartCalls() []string {
	e.restartMu.Lock()
	defer e.restartMu.Unlock()
	return append([]string(nil), e.restarts...)
}

func (e *imageReadyTestEnv) getStoppedJobs() []string {
	e.stopMu.Lock()
	defer e.stopMu.Unlock()
	return append([]string(nil), e.stoppedJobs...)
}

func TestExecuteImageReadyEvent_TableDriven(t *testing.T) { // t.Run(
	ctx := context.Background()

	routerHCL := `variable "image_tag" {
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
	legacyHCL := `job "legacy" {
		task "legacy" {
			config {
				image = "ghcr.io/azylman/legacy-app:latest"
			}
		}
	}`

	type imageReadyCase struct {
		name                 string
		jobDir               string
		jobFile              string
		jobHCL               string
		req                  ImageReadyEventRequest
		valErr               error
		runErr               error
		restartErr           error
		registryNotFound     bool
		removeJobOnRegistry  bool
		pendingDeletes       []NomadFileChange
		wantCode             int
		wantStatus           string
		wantRuns             int
		wantRests            int
		wantRunArg           string
		wantEvent            string
		wantCachedDigest     string
		wantStoppedJobs      []string
		wantRemainingPending int
	}

	cases := []imageReadyCase{
		{
			name:       "missing image",
			req:        ImageReadyEventRequest{},
			wantCode:   http.StatusBadRequest,
			wantStatus: "error",
		},
		{
			name:       "unknown image",
			req:        ImageReadyEventRequest{Image: "ghcr.io/azylman/nonexistent:latest"},
			wantCode:   http.StatusNotFound,
			wantStatus: "not_found",
		},
		{
			name:    "deploy started dispatch and metadata propagation",
			jobFile: "brain.nomad",
			jobHCL: `job "brain" {
				task "brain" {
					config {
						image = "ghcr.io/azylman/aerial-brain:latest"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-brain:latest",
				Digest:    "sha256:digest_brain",
				Repo:      "azylman/aerial",
				CommitSHA: "headsha777",
				PRNumber:  535,
				TargetID:  "1555405874565091380",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
			wantEvent:  "deploy_started",
		},
		{
			name:    "commit SHA variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:      "azylman/aerial",
				CommitSHA: "sha12345",
				PRNumber:  615,
				TargetID:  "discord_thread_999",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "job run -detach -var=image_tag=sha12345 ",
		},
		{
			name:    "digest fallback variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:   "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:    "unparameterized legacy job triggers restart",
			jobFile: "legacy.nomad",
			jobHCL:  legacyHCL,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/legacy-app:latest",
				CommitSHA: "legacy123",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
		},
		{
			name:             "empty sha and digest fallback to latest",
			jobFile:          "router.nomad",
			jobHCL:           routerHCL,
			registryNotFound: true,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest",
		},
		{
			name:    "digest prefix normalization",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:   "azylman/aerial",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  0,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:       "unparameterized restart warning succeeds",
			jobFile:    "legacy.nomad",
			jobHCL:     legacyHCL,
			restartErr: errors.New("exit status 1"),
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/legacy-app:latest",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRests:  1,
		},
		{
			name:    "validation failure returns 500",
			jobFile: "val-fail.nomad",
			jobHCL:  `job "val-fail" { task "t" { config { image = "ghcr.io/azylman/val-fail:latest" } } }`,
			valErr:  errors.New("validate failed"),
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/val-fail:latest",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   0,
			wantRests:  0,
		},
		{
			name:                "mid-flight file removal triggers read error",
			jobFile:             "read-fail.nomad",
			jobHCL:              `job "read-fail" { task "t" { config { image = "ghcr.io/azylman/read-fail:latest" } } }`,
			removeJobOnRegistry: true,
			req: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/read-fail:latest",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   0,
			wantRests:  0,
		},
		{
			name:    "nomad job run error dispatches deploy_failed",
			jobFile: "failing.nomad",
			jobHCL:  `job "failing-job" { task "t" { config { image = "ghcr.io/azylman/aerial-broken:latest" } } }`,
			runErr:  errors.New("exit status 1"),
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-broken:latest",
				Digest:    "sha256:abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
				Repo:      "azylman/aerial",
				CommitSHA: "broken_sha",
				PRNumber:  999,
				TargetID:  "thread_broken",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   1,
			wantRests:  0,
		},
		{
			name:       "core composeDir matching and digest caching",
			jobDir:     "core",
			jobFile:    "core.nomad",
			jobHCL:     `job "core-job" { task "t" { config { image = "ghcr.io/azylman/core-service:latest" } } }`,
			restartErr: errors.New("restart warning err"),
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/core-service:latest",
				Digest: "sha256:core123456",
			},
			wantCode:         http.StatusOK,
			wantStatus:       "accepted",
			wantRuns:         1,
			wantRests:        1,
			wantCachedDigest: "sha256:core123456",
		},
		{
			name:    "drains and stops pending deleted jobs",
			jobFile: "coven.nomad",
			jobHCL: `variable "image_tag" {
				type    = string
				default = "latest"
			}
			job "coven" {
				task "coven" {
					config {
						image = "ghcr.io/azylman/aerial-coven:${var.image_tag}"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-coven:latest",
				CommitSHA: "sha_coven_ready",
			},
			pendingDeletes: []NomadFileChange{
				{JobName: "memory-mcp", Action: "delete"},
			},
			wantCode:        http.StatusOK,
			wantStatus:      "accepted",
			wantRuns:        1,
			wantStoppedJobs: []string{"memory-mcp"},
		},
		{
			name:    "drains deletes while preserving non-delete changes",
			jobFile: "coven.nomad",
			jobHCL: `variable "image_tag" {
				type    = string
				default = "latest"
			}
			job "coven" {
				task "coven" {
					config {
						image = "ghcr.io/azylman/aerial-coven:${var.image_tag}"
					}
				}
			}`,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-coven:latest",
				CommitSHA: "sha_coven_ready",
			},
			pendingDeletes: []NomadFileChange{
				{JobName: "memory-mcp", Action: "delete"},
				{JobName: "pending-service", Action: "apply"},
			},
			wantCode:             http.StatusOK,
			wantStatus:           "accepted",
			wantRuns:             1,
			wantStoppedJobs:      []string{"memory-mcp"},
			wantRemainingPending: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.valErr = tc.valErr
			env.runErr = tc.runErr
			env.restartErr = tc.restartErr

			var jobPath string
			if tc.jobFile != "" {
				dir := tc.jobDir
				if dir == "" {
					dir = "jobs"
				}
				jobPath = env.writeJob(dir, tc.jobFile, tc.jobHCL)
			}
			if tc.registryNotFound {
				env.daemon.registryClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusNotFound,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("not found")),
							}, nil
						},
					},
				}
			}
			if tc.removeJobOnRegistry {
				env.daemon.registryClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							if jobPath != "" {
								_ = os.Remove(jobPath)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader(`{"token":"abc"}`)),
							}, nil
						},
					},
				}
			}

			if len(tc.pendingDeletes) > 0 {
				env.daemon.recordPendingNomadChanges(env.tmpDir, tc.pendingDeletes, tc.req.Repo, tc.req.CommitSHA, tc.req.TargetID, fmt.Sprintf("%d", tc.req.PRNumber))
			}

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, tc.req)
			if code != tc.wantCode || resp.Status != tc.wantStatus {
				t.Fatalf("expected code %d status %q, got code %d status %q (%+v)", tc.wantCode, tc.wantStatus, code, resp.Status, resp)
			}
			runs := env.getRunCalls()
			if len(runs) != tc.wantRuns {
				t.Errorf("expected %d runs, got %d (%v)", tc.wantRuns, len(runs), runs)
			}
			if tc.wantRunArg != "" {
				if len(runs) == 0 || !strings.Contains(runs[0], tc.wantRunArg) {
					t.Errorf("expected run call to contain %q, got %v", tc.wantRunArg, runs)
				}
			}
			restarts := env.getRestartCalls()
			if len(restarts) != tc.wantRests {
				t.Errorf("expected %d restarts, got %d (%v)", tc.wantRests, len(restarts), restarts)
			}
			if tc.wantStoppedJobs != nil {
				stopped := env.getStoppedJobs()
				if len(stopped) != len(tc.wantStoppedJobs) {
					t.Fatalf("expected %d stopped jobs, got %d (%v)", len(tc.wantStoppedJobs), len(stopped), stopped)
				}
				for i, j := range tc.wantStoppedJobs {
					if stopped[i] != j {
						t.Errorf("stopped job[%d] = %q, want %q", i, stopped[i], j)
					}
				}
				remainingPending := env.daemon.drainPendingNomadChanges()
				var gotRemainingCount int
				for _, evt := range remainingPending {
					gotRemainingCount += len(evt.Changes)
				}
				if gotRemainingCount != tc.wantRemainingPending {
					t.Fatalf("expected %d remaining pending changes after drain, got %d (%+v)", tc.wantRemainingPending, gotRemainingCount, remainingPending)
				}
			}
			if tc.wantCachedDigest != "" {
				cached := env.getDigest("core-job:" + tc.req.Image)
				if cached != tc.wantCachedDigest {
					t.Errorf("expected cached digest %q, got %q", tc.wantCachedDigest, cached)
				}
			}
			if tc.wantEvent != "" {
				select {
				case evt := <-env.evtCh:
					if evt.Event != tc.wantEvent {
						t.Errorf("expected %s, got %q", tc.wantEvent, evt.Event)
					}
					if evt.JobName != "brain" {
						t.Errorf("expected job_name brain, got %q", evt.JobName)
					}
					if evt.Repo != tc.req.Repo || evt.CommitSHA != tc.req.CommitSHA || evt.PRNumber != tc.req.PRNumber || evt.TargetID != tc.req.TargetID {
						t.Errorf("metadata missing in deploy_started: %+v", evt)
					}
					if evt.Digest != tc.req.Digest || evt.Image != tc.req.Image {
						t.Errorf("digest/image mismatch: %+v", evt)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for %s", tc.wantEvent)
				}
				select {
				case evt := <-env.evtCh:
					t.Fatalf("unexpected followup event: %+v", evt)
				default:
				}
			}
			if tc.runErr != nil {
				var sawFailed bool
				deadline := time.After(2 * time.Second)
				for !sawFailed {
					select {
					case evt := <-env.evtCh:
						if evt.Event == "deploy_failed" {
							sawFailed = true
							if evt.Repo != tc.req.Repo || evt.CommitSHA != tc.req.CommitSHA || evt.PRNumber != tc.req.PRNumber || evt.TargetID != tc.req.TargetID {
								t.Errorf("metadata missing in deploy_failed event: %+v", evt)
							}
						}
					case <-deadline:
						t.Fatal("timed out waiting for deploy_failed")
					}
				}
			}
		})
	}
}

func TestImageRolloutQueue_TableDriven(t *testing.T) {
	t.Run("enqueue image ready event validation and queueing", func(t *testing.T) {
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
		env := newImageReadyTestEnv(t)
		env.writeJob("jobs", "debounced-test.nomad", jobContent)
		d := env.daemon

		cases := []struct {
			name       string
			req        ImageReadyEventRequest
			wantCode   int
			wantStatus string
			wantQueue  bool
		}{
			{
				name:       "empty image 400",
				req:        ImageReadyEventRequest{},
				wantCode:   http.StatusBadRequest,
				wantStatus: "error",
			},
			{
				name:       "unknown image 404",
				req:        ImageReadyEventRequest{Image: "ghcr.io/azylman/nonexistent:latest"},
				wantCode:   http.StatusNotFound,
				wantStatus: "not_found",
			},
			{
				name: "valid image 202 accepted and queued",
				req: ImageReadyEventRequest{
					Image:     "ghcr.io/azylman/debounced-app:latest",
					Repo:      "azylman/aerial",
					CommitSHA: "sha123",
					PRNumber:  555,
					TargetID:  "1555405874565091380",
				},
				wantCode:   http.StatusAccepted,
				wantStatus: "accepted",
				wantQueue:  true,
			},
		}

		for _, tc := range cases {
			resp, code := d.EnqueueImageReadyEvent(tc.req)
			if code != tc.wantCode || resp.Status != tc.wantStatus {
				t.Errorf("%s: EnqueueImageReadyEvent() = (%d, %s), want (%d, %s)", tc.name, code, resp.Status, tc.wantCode, tc.wantStatus)
			}
			if tc.wantQueue {
				if len(resp.MatchedJobs) != 1 || resp.MatchedJobs[0] != "debounced-test" {
					t.Errorf("expected matched job debounced-test, got %v", resp.MatchedJobs)
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
		}
	})

	t.Run("debounced loop execution synchronizes on channel without sleep", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		env := newImageReadyTestEnv(t)
		env.runSignal = make(chan string, 10)
		env.writeJob("jobs", "service-a.nomad", `job "service-a" { task "a" { config { image = "ghcr.io/azylman/service-a:latest" } } }`)
		env.writeJob("jobs", "service-b.nomad", `job "service-b" { task "b" { config { image = "ghcr.io/azylman/service-b:latest" } } }`)

		d := env.daemon
		d.imageRolloutDebounce = 20 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

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

		var executed []string
		for i := 0; i < 2; i++ {
			select {
			case call := <-env.runSignal:
				executed = append(executed, call)
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for debounced run %d, got %v", i+1, executed)
			}
		}

		hasA := false
		hasB := false
		for _, call := range executed {
			if strings.Contains(call, "service-a.nomad") {
				hasA = true
			}
			if strings.Contains(call, "service-b.nomad") {
				hasB = true
			}
		}
		if !hasA || !hasB {
			t.Errorf("expected runs for both service-a and service-b, got %v", executed)
		}
	})

	t.Run("panic recovery in rollout loop", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var runCount int
		var runMu sync.Mutex
		panickedCh := make(chan struct{}, 1)
		recoveredCh := make(chan struct{}, 2)

		env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				runMu.Lock()
				runCount++
				count := runCount
				runMu.Unlock()
				if count == 1 {
					select {
					case panickedCh <- struct{}{}:
					default:
					}
					panic("simulated panic in rollout executor")
				}
				select {
				case recoveredCh <- struct{}{}:
				default:
				}
				return []byte("OK"), nil, nil
			}
		})

		env.writeJob("jobs", "service-panic.nomad", `job "service-panic" { task "p" { config { image = "ghcr.io/azylman/service-panic:latest" } } }`)

		d := env.daemon
		d.imageRolloutDebounce = 10 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

		_, code1 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "panic-sha",
		})
		if code1 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code1)
		}

		select {
		case <-panickedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for first rollout panic")
		}

		_, code2 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "recovered-sha",
		})
		if code2 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code2)
		}

		select {
		case <-recoveredCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for second rollout execution after panic recovery")
		}

		runMu.Lock()
		finalCount := runCount
		runMu.Unlock()
		if finalCount < 2 {
			t.Errorf("expected at least 2 executor attempts (surviving panic), got %d", finalCount)
		}
	})

	t.Run("execute pending rollouts branches and hangar-last ordering", func(t *testing.T) {
		var mu sync.Mutex
		var executedJobs []string

		env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					jobPath := args[len(args)-1]
					body, _ := os.ReadFile(jobPath)
					jobName := ExtractJobName(string(body), filepath.Base(jobPath))
					mu.Lock()
					executedJobs = append(executedJobs, jobName)
					mu.Unlock()
				}
				return []byte("Evaluation ID: 12345"), nil, nil
			}
		})

		env.writeJob("jobs", "hangar.nomad", `job "hangar" { task "h" { config { image = "ghcr.io/azylman/aerial-hangar:latest" } } }`)
		env.writeJob("jobs", "webhooks-router.nomad", `job "webhooks-router" { task "r" { config { image = "ghcr.io/azylman/aerial-webhooks-router:latest" } } }`)

		d := env.daemon

		d.executePendingImageRollouts(context.Background())

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

		d.pendingImageMu.Lock()
		d.pendingImageRollouts["hangar"] = PendingImageRollout{
			JobName: "hangar",
			Request: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/aerial-hangar:latest",
			},
		}
		d.pendingImageMu.Unlock()
		d.pendingImageMu.Lock()
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
	})
}

func mockGitDiffExec(primaryOut, fallbackOut string, primaryErr, fallbackErr error, cancelOnCall int, cancel context.CancelFunc) GitExecutor {
	var calls int
	return func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		calls++
		if cancel != nil && calls == cancelOnCall {
			cancel()
			return nil, []byte("interrupted"), errors.New("signal: killed")
		}
		if len(args) > 0 && args[0] == "diff" {
			if primaryErr != nil {
				return nil, []byte(primaryErr.Error()), primaryErr
			}
			return []byte(primaryOut), nil, nil
		}
		if fallbackErr != nil {
			return nil, []byte(fallbackErr.Error()), fallbackErr
		}
		return []byte(fallbackOut), nil, nil
	}
}

func TestNomadChangeDetection_TableDriven(t *testing.T) {
	t.Run("HasNomadConfigChanges_table", func(t *testing.T) {
		tests := []struct {
			name         string
			repo         string
			prev         string
			curr         string
			cancelEarly  bool
			cancelOnCall int
			primaryOut   string
			fallbackOut  string
			primaryErr   error
			fallbackErr  error
			wantChanged  bool
			wantErr      bool
			wantErrIs    error
		}{
			{name: "pre-canceled context returns error", repo: "/path", prev: "v1", curr: "v2", cancelEarly: true, wantErr: true},
			{name: "empty repo path", repo: "", prev: "v1", curr: "v2"},
			{name: "empty prev head", repo: "/path", prev: "", curr: "v2"},
			{name: "empty curr head", repo: "/path", prev: "v1", curr: ""},
			{name: "identical heads", repo: "/path", prev: "v1", curr: "v1"},
			{name: "diff detects nomad config change", repo: "/path", prev: "v1", curr: "v2", primaryOut: "nomad/nomad.hcl\nmain.go\n", wantChanged: true},
			{name: "diff detects no nomad config change", repo: "/path", prev: "v1", curr: "v2", primaryOut: "README.md\ndocker-compose.yml\n"},
			{name: "diff fails and fallback diff-tree succeeds with config changes", repo: "/path", prev: "v1", curr: "v2", primaryErr: errors.New("diff failed"), fallbackOut: "nomad/client.hcl\n", wantChanged: true},
			{name: "diff fails and fallback fails with normal error (fail-safe false)", repo: "/path", prev: "v1", curr: "v2", primaryErr: errors.New("git fail"), fallbackErr: errors.New("git fail")},
			{name: "context canceled during primary diff", repo: "/repo", prev: "c1", curr: "c2", cancelOnCall: 1, wantErr: true, wantErrIs: context.Canceled},
			{name: "context canceled during fallback diff-tree", repo: "/repo", prev: "c1", curr: "c2", primaryErr: errors.New("diff failed"), cancelOnCall: 2, wantErr: true, wantErrIs: context.Canceled},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancelEarly {
					cancel()
				}
				d := &SyncDaemon{
					gitExecutor: mockGitDiffExec(tc.primaryOut, tc.fallbackOut, tc.primaryErr, tc.fallbackErr, tc.cancelOnCall, cancel),
				}
				got, err := d.HasNomadConfigChanges(ctx, tc.repo, tc.prev, tc.curr)
				if (err != nil) != tc.wantErr || got != tc.wantChanged {
					t.Fatalf("HasNomadConfigChanges() = (%v, %v), want (%v, wantErr:%v)", got, err, tc.wantChanged, tc.wantErr)
				}
				if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("HasNomadConfigChanges() error = %v, want error %v", err, tc.wantErrIs)
				}
			})
		}
	})

	t.Run("HasNomadChanges_table", func(t *testing.T) {
		tests := []struct {
			name         string
			repo         string
			prev         string
			curr         string
			cancelEarly  bool
			cancelOnCall int
			primaryOut   string
			fallbackOut  string
			primaryErr   error
			fallbackErr  error
			wantCount    int
			wantErr      bool
			wantErrIs    error
		}{
			{name: "pre-canceled context returns error", repo: "/repo", prev: "a", curr: "b", cancelEarly: true, wantErr: true},
			{name: "empty repo path", repo: "", prev: "a", curr: "b"},
			{name: "empty prev head", repo: "/repo", prev: "", curr: "b"},
			{name: "empty curr head", repo: "/repo", prev: "a", curr: ""},
			{name: "identical heads", repo: "/repo", prev: "a", curr: "a"},
			{
				name: "git diff success with multiple changes",
				repo: "/share/aerial-config", prev: "HEAD~1", curr: "HEAD",
				primaryOut: "M\tjobs/infisical.nomad\nD\tjobs/legacy.nomad\nM\tdocker-compose.yml\n",
				wantCount:  2,
			},
			{
				name: "git diff fails, fallback diff-tree succeeds",
				repo: "/repo", prev: "head1", curr: "head2",
				primaryErr:  errors.New("diff error"),
				fallbackOut: "M\tjobs/app.nomad\n",
				wantCount:   1,
			},
			{
				name: "git diff fails, fallback diff-tree fails (fail-safe nil)",
				repo: "/repo", prev: "head1", curr: "head2",
				primaryErr:  errors.New("git error"),
				fallbackErr: errors.New("git error"),
			},
			{name: "context canceled during primary diff", repo: "/repo", prev: "c1", curr: "c2", cancelOnCall: 1, wantErr: true, wantErrIs: context.Canceled},
			{name: "context canceled during fallback diff-tree", repo: "/repo", prev: "c1", curr: "c2", primaryErr: errors.New("diff err"), cancelOnCall: 2, wantErr: true, wantErrIs: context.Canceled},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancelEarly {
					cancel()
				}
				d := &SyncDaemon{
					gitExecutor: mockGitDiffExec(tc.primaryOut, tc.fallbackOut, tc.primaryErr, tc.fallbackErr, tc.cancelOnCall, cancel),
				}
				changes, err := d.HasNomadChanges(ctx, tc.repo, tc.prev, tc.curr)
				if (err != nil) != tc.wantErr || len(changes) != tc.wantCount {
					t.Fatalf("HasNomadChanges() = (%v, %v), want (count:%d, wantErr:%v)", changes, err, tc.wantCount, tc.wantErr)
				}
				if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("HasNomadChanges() error = %v, want error %v", err, tc.wantErrIs)
				}
			})
		}
	})

	t.Run("getChangedFiles_branches", func(t *testing.T) {
		ctx := context.Background()
		d := &SyncDaemon{}

		for _, heads := range [][2]string{{"", "sha2"}, {"sha1", ""}, {"sha1", "sha1"}} {
			files, err := d.getChangedFiles(ctx, "/tmp", heads[0], heads[1])
			if err != nil || files != nil {
				t.Fatalf("expected nil for heads %v, got (%v, %v)", heads, files, err)
			}
		}

		dSuccess := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return []byte("file1.go\n\nfile2.hcl\n"), nil, nil
			},
		}
		if files, err := dSuccess.getChangedFiles(ctx, "/tmp", "sha1", "sha2"); err != nil || len(files) != 2 || files[0] != "file1.go" || files[1] != "file2.hcl" {
			t.Fatalf("unexpected files from diff: %v, %v", files, err)
		}

		dFallback := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "diff" {
					return nil, []byte("bad"), errors.New("fail")
				}
				return []byte("fallback.txt\n"), nil, nil
			},
		}
		if files, err := dFallback.getChangedFiles(ctx, "/tmp", "sha1", "sha2"); err != nil || len(files) != 1 || files[0] != "fallback.txt" {
			t.Fatalf("unexpected files from fallback: %v, %v", files, err)
		}

		dFallbackFail := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, []byte("bad"), errors.New("fail")
			},
		}
		if files, err := dFallbackFail.getChangedFiles(ctx, "/tmp", "sha1", "sha2"); err == nil || files != nil {
			t.Fatalf("expected error from fallback failure, got (%v, %v)", files, err)
		}
	})
}

func TestNomadValidationAndServerReconcile_TableDriven(t *testing.T) {
	t.Run("defaultNomadExecutor_and_accessors", func(t *testing.T) {
		ctx := context.Background()
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		for _, cfg := range [][2]string{{"", ""}, {"http://127.0.0.1:4646", "secret-token"}} {
			fn := defaultNomadExecutor(cfg[0], cfg[1])
			_, _, _ = fn(ctx, "version")
			_, _, _ = fn(cancelCtx, "version")
		}

		_, _, _ = defaultComposeExecutor(cancelCtx, "", "version")
		_, _, _ = runGitCommand(cancelCtx, "", "", "version")

		var nilDaemon *SyncDaemon
		if nilDaemon.getNomadExecutor() == nil {
			t.Fatal("expected non-nil executor from nil daemon")
		}
		dDefault := &SyncDaemon{nomadAddr: "http://localhost:4646", nomadToken: "tok"}
		if dDefault.getNomadExecutor() == nil {
			t.Fatal("expected non-nil executor from default daemon")
		}
		customCalled := false
		dCustom := &SyncDaemon{
			nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				customCalled = true
				return []byte("custom"), nil, nil
			},
		}
		if out, _, err := dCustom.getNomadExecutor()(ctx, "test"); err != nil || !customCalled || string(out) != "custom" {
			t.Fatalf("expected custom executor invocation, got out=%s customCalled=%v err=%v", string(out), customCalled, err)
		}
	})

	t.Run("ValidateNomadConfig_table", func(t *testing.T) {
		tmpDir := t.TempDir()
		jobFile := filepath.Join(tmpDir, "test.nomad")
		_ = os.WriteFile(jobFile, []byte(`job "test" {}`), 0o644)

		tests := []struct {
			name      string
			target    string
			path      string
			nomadExec NomadExecutor
			wantErr   bool
		}{
			{name: "config nil executor returns error", target: "config", path: "/tmp/nomad", nomadExec: nil, wantErr: true},
			{
				name:   "config validation success",
				target: "config",
				path:   "/tmp/nomad",
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("Configuration is valid!\n"), nil, nil
				},
			},
			{
				name:   "config validation failure",
				target: "config",
				path:   "/tmp/nomad",
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("syntax error"), errors.New("exit 1")
				},
				wantErr: true,
			},
			{
				name:   "job validation success",
				target: "job",
				path:   jobFile,
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("Job validation successful!\n"), nil, nil
				},
			},
			{
				name:   "job validation failure",
				target: "job",
				path:   filepath.Join(tmpDir, "invalid.nomad"),
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("syntax error"), errors.New("exit 1")
				},
				wantErr: true,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{nomadExecutor: tc.nomadExec}
				var err error
				if tc.target == "job" {
					err = d.ValidateNomadJob(context.Background(), tc.path)
				} else {
					err = d.ValidateNomadConfig(context.Background(), tc.path)
				}
				if (err != nil) != tc.wantErr {
					t.Fatalf("Validate (%s) error = %v, wantErr %v", tc.target, err, tc.wantErr)
				}
			})
		}
	})

	t.Run("reconcileNomadServer_table", func(t *testing.T) {
		tmpDir := t.TempDir()
		dNoop := &SyncDaemon{}
		if err := dNoop.reconcileNomadServer(context.Background(), tmpDir); err != nil {
			t.Fatalf("expected nil for missing nomad dir: %v", err)
		}

		nomadDir := filepath.Join(tmpDir, "nomad")
		if err := os.MkdirAll(nomadDir, 0o755); err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			name         string
			valErr       error
			composeErr   error
			wantErr      bool
			expectCalled bool
		}{
			{
				name:         "validation failure aborts compose restart",
				valErr:       errors.New("val fail"),
				wantErr:      true,
				expectCalled: false,
			},
			{
				name:         "validation success and restart success",
				wantErr:      false,
				expectCalled: true,
			},
			{
				name:         "validation success and restart failure",
				composeErr:   errors.New("compose fail"),
				wantErr:      true,
				expectCalled: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				called := false
				d := &SyncDaemon{
					nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
						if tc.valErr != nil {
							return nil, []byte("syntax error"), tc.valErr
						}
						return []byte("valid"), nil, nil
					},
					composeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						called = true
						if tc.composeErr != nil {
							return nil, []byte("compose down"), tc.composeErr
						}
						return []byte("restarted"), nil, nil
					},
				}
				err := d.reconcileNomadServer(context.Background(), tmpDir)
				if (err != nil) != tc.wantErr || called != tc.expectCalled {
					t.Fatalf("reconcileNomadServer() err=%v (wantErr:%v), restartCalled=%v (expectCalled:%v)", err, tc.wantErr, called, tc.expectCalled)
				}
			})
		}
	})

	t.Run("PendingNomadServerRestart_lifecycle", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		nilDaemon.recordPendingNomadServerRestart()
		if nilDaemon.consumePendingNomadServerRestart() {
			t.Fatal("expected false for nil daemon")
		}

		d := &SyncDaemon{}
		if d.consumePendingNomadServerRestart() {
			t.Fatal("expected false initially")
		}
		d.recordPendingNomadServerRestart()
		if !d.consumePendingNomadServerRestart() {
			t.Fatal("expected true after recording restart")
		}
		if d.consumePendingNomadServerRestart() {
			t.Fatal("expected false on second consume")
		}
	})

	t.Run("IsHangarRequest_table", func(t *testing.T) {
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "nomad", "jobs")
		if err := os.MkdirAll(jobsDir, 0o755); err != nil {
			t.Fatal(err)
		}
		hcl := `job "hangar" { group "g" { task "t" { config { image = "ghcr.io/custom/worker:v1" } } } }`
		if err := os.WriteFile(filepath.Join(jobsDir, "hangar.nomad"), []byte(hcl), 0o644); err != nil {
			t.Fatal(err)
		}

		d := &SyncDaemon{repos: []string{tmpDir}}
		tests := []struct {
			image string
			want  bool
		}{
			{"ghcr.io/azylman/aerial/hangar:latest", true},
			{"ghcr.io/custom/worker:v1", true},
			{"ghcr.io/custom/other:v1", false},
		}
		for _, tc := range tests {
			if got := d.isHangarRequest(ImageReadyEventRequest{Image: tc.image}); got != tc.want {
				t.Fatalf("isHangarRequest(%q) = %v, want %v", tc.image, got, tc.want)
			}
		}
	})
}

func newNomadReconcileDaemon(opts ...func(*DaemonConfig)) *SyncDaemon {
	cfg := DaemonConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewDaemon(cfg)
}

func TestNomadJobReconciliation_TableDriven(t *testing.T) {
	t.Run("ReconcileNomadChanges_teardown_order_and_cases", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0o755)
		_ = os.WriteFile(filepath.Join(jobsDir, "valid.nomad"), []byte(`job "valid" {}`), 0o644)
		_ = os.WriteFile(filepath.Join(jobsDir, "invalid.nomad"), []byte(`job "invalid" {}`), 0o644)
		_ = os.WriteFile(filepath.Join(jobsDir, "runfail.nomad"), []byte(`job "runfail" {}`), 0o644)

		var executedCalls [][]string
		var mu sync.Mutex
		dReconcile := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				mu.Lock()
				executedCalls = append(executedCalls, args)
				mu.Unlock()
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
					return []byte("Evaluation ID: 12345"), nil, nil
				}
				if len(args) >= 4 && args[0] == "job" && args[1] == "stop" {
					if args[3] == "stop-fail" {
						return nil, []byte("stop fail"), errors.New("stop error")
					}
					return []byte("stopped"), nil, nil
				}
				return nil, nil, nil
			}
		})

		evtCh := make(chan HangarDeployEvent, 1)
		dMeta := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return []byte("Evaluation ID: eval-123"), nil, nil
			}
			cfg.DeployDispatcher = func(ctx context.Context, evt HangarDeployEvent) error {
				evtCh <- evt
				return nil
			}
		})

		cases := []struct {
			name       string
			changes    []NomadFileChange
			repo       string
			commitSHA  string
			targetID   string
			prNumber   string
			wantErr    bool
			checkOrder bool
			checkMeta  bool
		}{
			{name: "empty changes"},
			{
				name: "delete change success and failure branches",
				changes: []NomadFileChange{
					{JobName: "stop-ok", Action: "delete"},
					{JobName: "stop-fail", Action: "delete"},
				},
			},
			{
				name: "apply change missing from disk",
				changes: []NomadFileChange{
					{JobName: "missing", Path: "jobs/missing.nomad", Action: "apply"},
				},
			},
			{
				name: "unsafe job path ignored",
				changes: []NomadFileChange{
					{JobName: "evil", Path: "../evil.nomad", Action: "update"},
				},
			},
			{
				name: "apply change validation failure",
				changes: []NomadFileChange{
					{JobName: "invalid", Path: "jobs/invalid.nomad", Action: "apply"},
				},
				wantErr: true,
			},
			{
				name: "apply change run failure",
				changes: []NomadFileChange{
					{JobName: "runfail", Path: "jobs/runfail.nomad", Action: "apply"},
				},
				wantErr: true,
			},
			{
				name: "combined apply and delete with teardown first",
				changes: []NomadFileChange{
					{Path: "jobs/valid.nomad", Action: "apply", JobName: "valid"},
					{Path: "jobs/old.nomad", Action: "delete", JobName: "old-job"},
				},
				checkOrder: true,
			},
			{
				name: "metadata propagation check",
				changes: []NomadFileChange{
					{Action: "update", JobName: "valid", Path: "jobs/valid.nomad"},
				},
				repo:      "azylman/aerial",
				commitSHA: "headsha123",
				targetID:  "1555422677936644147",
				prNumber:  "123",
				checkMeta: true,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.checkOrder {
					mu.Lock()
					executedCalls = nil
					mu.Unlock()
				}
				daemon := dReconcile
				if tc.checkMeta {
					daemon = dMeta
				}
				var err error
				if tc.repo != "" {
					err = daemon.ReconcileNomadChanges(ctx, tmpDir, tc.changes, tc.repo, tc.commitSHA, tc.targetID, tc.prNumber)
				} else {
					err = daemon.ReconcileNomadChanges(ctx, tmpDir, tc.changes)
				}
				if (err != nil) != tc.wantErr {
					t.Fatalf("ReconcileNomadChanges() error = %v, wantErr %v", err, tc.wantErr)
				}
				if tc.checkOrder {
					mu.Lock()
					var foundStop, foundRun bool
					var stopIdx, runIdx int
					for idx, call := range executedCalls {
						if len(call) >= 4 && call[0] == "job" && call[1] == "stop" && call[3] == "old-job" {
							foundStop = true
							stopIdx = idx
						}
						if len(call) >= 4 && call[0] == "job" && call[1] == "run" && strings.HasSuffix(call[3], "valid.nomad") {
							foundRun = true
							runIdx = idx
						}
					}
					mu.Unlock()
					if !foundStop || !foundRun || stopIdx > runIdx {
						t.Fatalf("expected teardown before run, stopIdx=%d, runIdx=%d, stop=%v, run=%v", stopIdx, runIdx, foundStop, foundRun)
					}
				}
				if tc.checkMeta {
					select {
					case evt := <-evtCh:
						if evt.PRNumber != 123 || evt.TargetID != tc.targetID || evt.Repo != tc.repo || evt.CommitSHA != tc.commitSHA {
							t.Fatalf("unexpected metadata: %+v", evt)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("timed out waiting for deploy event")
					}
				}
			})
		}
	})

	t.Run("ExecuteNomadTeardowns_and_cross_repo_purge_protection", func(t *testing.T) {
		ctx := context.Background()
		dBasic := newNomadReconcileDaemon()
		dBasic.ExecuteNomadTeardowns(ctx, nil)

		var stoppedJobs []string
		var mu sync.Mutex
		dTeardown := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				mu.Lock()
				defer mu.Unlock()
				if len(args) >= 4 && args[0] == "job" && args[1] == "stop" {
					stoppedJobs = append(stoppedJobs, args[3])
					if args[3] == "fail-job" {
						return []byte("err-out"), []byte("err-bytes"), errors.New("stop error")
					}
					return []byte("ok"), nil, nil
				}
				return nil, nil, nil
			}
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
		mu.Lock()
		if len(stoppedJobs) != 2 {
			t.Fatalf("expected 2 jobs stopped, got %v", stoppedJobs)
		}
		mu.Unlock()

		tempDir := t.TempDir()
		repoA := filepath.Join(tempDir, "aerial-config")
		repoB := filepath.Join(tempDir, "aerial")
		_ = os.MkdirAll(filepath.Join(repoA, "jobs"), 0o755)
		nomadJobsDirB := filepath.Join(repoB, "nomad", "jobs")
		_ = os.MkdirAll(nomadJobsDirB, 0o755)
		_ = os.WriteFile(filepath.Join(nomadJobsDirB, "brain.nomad"), []byte(`job "brain" {}`), 0o644)

		dCross := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.Repos = []string{repoA, repoB}
			cfg.ConfigDir = repoA
			cfg.ComposeDir = repoB
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				mu.Lock()
				defer mu.Unlock()
				if len(args) >= 3 && args[0] == "job" && args[1] == "stop" {
					stoppedJobs = append(stoppedJobs, args[len(args)-1])
				}
				return []byte("OK"), nil, nil
			}
		})

		type crossRepoCase struct {
			name        string
			fn          func() error
			wantStopped []string
		}

		cases := []crossRepoCase{
			{
				name: "defined in other repo skips stop",
				fn: func() error {
					return dCross.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
						{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
					})
				},
				wantStopped: nil,
			},
			{
				name: "truly deleted job stops",
				fn: func() error {
					return dCross.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
						{Path: "jobs/truly_deleted.nomad", Action: "delete", JobName: "truly_deleted"},
					})
				},
				wantStopped: []string{"truly_deleted"},
			},
			{
				name: "teardowns filters out cross repo defined jobs",
				fn: func() error {
					dCross.ExecuteNomadTeardowns(ctx, []NomadChangeEvent{
						{
							RepoPath: repoA,
							Changes: []NomadFileChange{
								{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
								{Path: "jobs/old_orphan.nomad", Action: "delete", JobName: "old_orphan"},
							},
						},
					})
					return nil
				},
				wantStopped: []string{"old_orphan"},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				mu.Lock()
				stoppedJobs = nil
				mu.Unlock()
				if err := tc.fn(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				mu.Lock()
				got := append([]string(nil), stoppedJobs...)
				mu.Unlock()
				if len(got) != len(tc.wantStopped) {
					t.Fatalf("stopped = %v, want %v", got, tc.wantStopped)
				}
				for i := range got {
					if got[i] != tc.wantStopped[i] {
						t.Errorf("stopped[%d] = %q, want %q", i, got[i], tc.wantStopped[i])
					}
				}
			})
		}
	})

	t.Run("ReconcilePendingNomad_table", func(t *testing.T) {
		ctx := context.Background()

		// 1. Empty queue
		dEmpty := newNomadReconcileDaemon()
		if err := dEmpty.ReconcilePendingNomad(ctx); err != nil {
			t.Fatalf("expected nil for empty queue, got %v", err)
		}

		// 2. Pending nomad change recording and draining
		if len(dEmpty.drainPendingNomadChanges()) != 0 {
			t.Fatal("expected empty pending changes initially")
		}
		dEmpty.recordPendingNomadChanges("/repo/a", []NomadFileChange{
			{JobName: "job-a", Path: "jobs/job-a.nomad", Action: "apply"},
		})
		dEmpty.recordPendingNomadChanges("/repo/b", []NomadFileChange{
			{JobName: "job-b", Path: "jobs/job-b.nomad", Action: "delete"},
		})
		delOnly := dEmpty.drainPendingNomadDeletes()
		if len(delOnly) != 1 || len(delOnly[0].Changes) != 1 || delOnly[0].Changes[0].JobName != "job-b" {
			t.Fatalf("unexpected drainPendingNomadDeletes result: %+v", delOnly)
		}
		if drained := dEmpty.drainPendingNomadChanges(); len(drained) != 1 || len(drained[0].Changes) != 1 || drained[0].Changes[0].JobName != "job-a" || len(dEmpty.drainPendingNomadChanges()) != 0 {
			t.Fatalf("unexpected drain result: len=%d", len(drained))
		}

		// 3. Valid pending job with PR metadata
		tmpDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(tmpDir, "bot.nomad"), []byte(`job "bot" {}`), 0o644)
		evtCh := make(chan HangarDeployEvent, 10)
		dMeta := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.DeployDispatcher = func(ctx context.Context, evt HangarDeployEvent) error {
				evtCh <- evt
				return nil
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return []byte("Evaluation ID: run-bot"), nil, nil
			}
		})
		dMeta.recordPendingNomadChanges(tmpDir, []NomadFileChange{
			{Path: "bot.nomad", Action: "apply", JobName: "bot"},
		}, "azylman/aerial", "sha-pending-123", "thread-777", "616")

		if err := dMeta.ReconcilePendingNomad(ctx); err != nil {
			t.Fatalf("ReconcilePendingNomad failed: %v", err)
		}
		select {
		case evt := <-evtCh:
			if evt.Event != "deploy_started" || evt.Repo != "azylman/aerial" || evt.CommitSHA != "sha-pending-123" || evt.PRNumber != 616 || evt.TargetID != "thread-777" {
				t.Fatalf("expected PR provenance in deploy_started, got %+v", evt)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deploy_started")
		}

		// 4. Pending job fails validation (error aggregation)
		invalidDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(invalidDir, "invalid.nomad"), []byte(`job "invalid" {}`), 0o644)
		dFailVal := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 3 && args[0] == "job" && args[1] == "validate" {
					return nil, []byte("syntax error"), errors.New("val fail")
				}
				return []byte("ok"), nil, nil
			}
		})
		dFailVal.recordPendingNomadChanges(invalidDir, []NomadFileChange{
			{Path: "invalid.nomad", Action: "apply", JobName: "invalid"},
		})
		if err := dFailVal.ReconcilePendingNomad(ctx); err == nil {
			t.Fatal("expected error aggregation when pending nomad job fails validation, got nil")
		}
	})
}
