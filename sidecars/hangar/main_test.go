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

func TestSanitizeAndEnv_TableDriven(t *testing.T) {
	t.Run("SanitizeLog", func(t *testing.T) {
		tests := []struct {
			name, input, expected string
		}{
			{"clean string", "repository synced cleanly", "repository synced cleanly"},
			{"github pat", "failed with token github_pat_11AAAA_secret_token_123", "failed with token [REDACTED_TOKEN]"},
			{"classic ghp token", "cloning with ghp_1234567890abcdef1234567890abcdef12", "cloning with [REDACTED_TOKEN]"},
			{"basic auth header", "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46c2VjcmV0", "AUTHORIZATION: [REDACTED_TOKEN]"},
			{"postgres connection string", "connecting to postgres://aerial:aerial_secure_pass@postgres:5432/aerial?sslmode=disable", "connecting to [REDACTED_TOKEN]?sslmode=disable"},
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

	t.Run("SanitizeAll", func(t *testing.T) {
		tests := []struct {
			name                   string
			daemon                 *SyncDaemon
			raw                    string
			shouldHide, shouldKeep []string
		}{
			{
				name: "redacts secrets len >= 4 and preserves short strings",
				daemon: NewDaemon(DaemonConfig{
					PAT:               "ghp_myPersonalAccessToken12345",
					DiscordToken:      "my_discord_secret_token",
					DiscordWebhookURL: "https://discord.com/api/webhooks/999/super_secret_webhook",
					ExtraSecrets:      []string{"pg_super_secret_password", "ha_long_lived_access_token_12345", "AIzaSyTestApiKeyForGemini1234567890", "abc"},
				}),
				raw:        "Connect to postgres://user:pg_super_secret_password@db:5432 with ghp_myPersonalAccessToken12345 and token my_discord_secret_token or webhook https://discord.com/api/webhooks/999/super_secret_webhook. Also ha_long_lived_access_token_12345 and key AIzaSyTestApiKeyForGemini1234567890 and normal word abc.",
				shouldHide: []string{"pg_super_secret_password", "ghp_myPersonalAccessToken12345", "my_discord_secret_token", "super_secret_webhook", "ha_long_lived_access_token_12345", "AIzaSyTestApiKeyForGemini1234567890"},
				shouldKeep: []string{"abc"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sanitized := tt.daemon.SanitizeAll(tt.raw)
				for _, secret := range tt.shouldHide {
					if strings.Contains(sanitized, secret) {
						t.Errorf("expected %q to be redacted, got: %s", secret, sanitized)
					}
				}
				for _, keep := range tt.shouldKeep {
					if !strings.Contains(sanitized, keep) {
						t.Errorf("expected %q to be preserved, got: %s", keep, sanitized)
					}
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
				if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			overrideFile := filepath.Join(configDir, "docker-compose.override.yml")
			if tc.writeConfigOverride {
				if err := os.WriteFile(overrideFile, []byte("services: { brain: {} }"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if tc.writeLocalOverride {
				localOverride := filepath.Join(composeDir, "docker-compose.override.yml")
				if err := os.WriteFile(localOverride, []byte("services: { brain: {} }"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			envFile := filepath.Join(composeDir, ".env")
			if tc.writeEnvFile {
				if err := os.WriteFile(envFile, []byte("TEST_KEY=123"), 0644); err != nil {
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
			testHTTPEndpoint   bool
			wantStatus         string
			wantRepo           string
			wantDiskCommit     string
			wantRemoteCommit   string
			wantRepoSyncStatus string
		}{
			{
				name: "synced_and_http_endpoint", repos: []string{"/mock/repo/aerial"},
				headOut: shaSync + "\x002026-09-12T12:00:00Z", testHTTPEndpoint: true,
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
				if tc.testHTTPEndpoint {
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
					var jsonResp HangarStatusResponse
					if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
						t.Fatalf("failed to decode JSON: %v", err)
					}
					if jsonResp.Status != tc.wantStatus {
						t.Errorf("expected json status %q, got %q", tc.wantStatus, jsonResp.Status)
					}
				}
			})
		}
	})

	t.Run("resolveGitDir", func(t *testing.T) {
		tempDir := t.TempDir()
		standardRepo := filepath.Join(tempDir, "standard")
		_ = os.MkdirAll(filepath.Join(standardRepo, ".git"), 0755)

		worktreeRepo := filepath.Join(tempDir, "worktree")
		_ = os.MkdirAll(worktreeRepo, 0755)
		targetGitDir := filepath.Join(tempDir, "target-git-dir")
		_ = os.MkdirAll(targetGitDir, 0755)
		_ = os.WriteFile(filepath.Join(worktreeRepo, ".git"), []byte("gitdir: "+targetGitDir+"\n"), 0644)

		relWorktreeRepo := filepath.Join(tempDir, "relworktree")
		_ = os.MkdirAll(relWorktreeRepo, 0755)
		_ = os.WriteFile(filepath.Join(relWorktreeRepo, ".git"), []byte("gitdir: ../target-git-dir\n"), 0644)

		plainFileRepo := filepath.Join(tempDir, "plainfile")
		_ = os.MkdirAll(plainFileRepo, 0755)
		_ = os.WriteFile(filepath.Join(plainFileRepo, ".git"), []byte("not_a_gitdir"), 0644)

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
					_ = os.WriteFile(repoPath, []byte("regular file"), 0644)
				}
				if tc.seedNonEmptyDir {
					_ = os.MkdirAll(repoPath, 0755)
					_ = os.WriteFile(filepath.Join(repoPath, "file.txt"), []byte("data"), 0644)
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
								_ = os.MkdirAll(filepath.Join(args[len(args)-1], ".git"), 0755)
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
			emptyPath         bool
			nonexistentPath   bool
			seedGitDir        bool
			seedNonEmptyNoGit bool
			seedIndexLock     bool
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
			{name: "empty_repo_path", emptyPath: true, wantEmptyRepo: true},
			{name: "nonexistent_repo_path", nonexistentPath: true, wantAnyErr: true},
			{name: "index_lock_active_guard", seedGitDir: true, seedIndexLock: true, wantErrSubstr: "index.lock active"},
			{name: "up_to_date", seedGitDir: true, withRepoURL: true, initHead: head1, fetchHead: head1, diffOut: "docker-compose.yml\n", wantChanged: false, wantPrevHead: head1, wantCurrHead: head1},
			{name: "fast_forward_update_with_diff", seedGitDir: true, withRepoURL: true, initHead: head1, fetchHead: head2, diffOut: "docker-compose.yml\n", wantChanged: true, wantPrevHead: head1, wantCurrHead: head2},
			{name: "empty_git_rev_parse_head_error", seedGitDir: true, revParseErrOnCall: 1, wantAnyErr: true},
			{name: "fetch_error", seedGitDir: true, initHead: head1, fetchErr: errors.New("fetch failed"), wantErrSubstr: "fetch failed"},
			{name: "ensure_repo_failure_warning_uninitialized", seedNonEmptyNoGit: true, withRepoURL: true, failAllGit: true, wantAnyErr: true},
			{name: "merge_conflict_reset_recovery", seedGitDir: true, initHead: head1, fetchHead: head2, mergeErr: errors.New("exit status 128"), wantChanged: true, wantPrevHead: head1, wantCurrHead: head2},
			{name: "recovery_reset_hard_failure", seedGitDir: true, initHead: head1, fetchHead: head1, mergeErr: errors.New("merge conflict"), resetErr: errors.New("reset failed"), wantErrSubstr: "reset failed"},
			{name: "post_merge_rev_parse_failure", seedGitDir: true, initHead: head1, fetchHead: head1, revParseErrOnCall: 2, wantAnyErr: true},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.emptyPath {
					d := &SyncDaemon{}
					if res := d.SyncRepo(context.Background(), ""); res.Repo != "" {
						t.Errorf("expected empty repo result, got %+v", res)
					}
					return
				}

				tempDir := t.TempDir()
				repoPath := filepath.Join(tempDir, "local")
				if tc.nonexistentPath {
					repoPath = filepath.Join(tempDir, "nonexistent")
				}
				if tc.seedGitDir {
					_ = os.MkdirAll(filepath.Join(repoPath, ".git"), 0755)
				}
				if tc.seedNonEmptyNoGit {
					_ = os.MkdirAll(repoPath, 0755)
					_ = os.WriteFile(filepath.Join(repoPath, "file.txt"), []byte("content"), 0644)
				}
				if tc.seedIndexLock {
					_ = os.WriteFile(filepath.Join(repoPath, ".git", "index.lock"), []byte(""), 0644)
				}

				currentHead := tc.initHead
				revParseCount := 0
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
							revParseCount++
							if tc.revParseErrOnCall > 0 && revParseCount >= tc.revParseErrOnCall {
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
		_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

		head1 := "1111111111111111111111111111111111111111"
		head2 := "2222222222222222222222222222222222222222"
		currentHead := head1

		d := NewDaemon(DaemonConfig{
			Repos: []string{localDir},
			GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 {
					switch args[0] {
					case "rev-parse":
						return []byte(currentHead), nil, nil
					case "merge":
						currentHead = head2
						return []byte("Updating " + head2), nil, nil
					}
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
	})

	t.Run("TriggerSync_singleflight_invalid_type", func(t *testing.T) {
		d := &SyncDaemon{}
		started := make(chan struct{})
		go func() {
			_, _, _ = d.sfg.Do("sync", func() (interface{}, error) {
				close(started)
				<-time.After(20 * time.Millisecond)
				return "invalid-type", nil
			})
		}()
		<-started
		_, err := d.TriggerSync()
		if err == nil || !strings.Contains(err.Error(), "unexpected return type") {
			t.Errorf("expected unexpected return type error, got %v", err)
		}
	})

	t.Run("TriggerSync_unconditional_nomad_config_sync", func(t *testing.T) {
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
				if len(args) > 0 && args[0] == "rev-parse" {
					return []byte(head), nil, nil
				}
				if len(args) > 0 && args[0] == "merge" {
					return []byte("Already up to date."), nil, nil
				}
				return nil, nil, nil
			},
		})

		results, err := d.TriggerSync()
		if err != nil || len(results) != 1 || results[0].Changed {
			t.Fatalf("expected 1 unchanged result, err=%v, results=%+v", err, results)
		}
		nomadMu.Lock()
		callsCount := len(nomadCalls)
		nomadMu.Unlock()
		if callsCount == 0 {
			t.Fatalf("expected nomad var put calls even when git repo has no changes, got 0")
		}

		results2, err := d.TriggerSync()
		if err != nil || len(results2) != 1 || results2[0].Changed {
			t.Fatalf("expected 1 unchanged result on second sync, err=%v, results=%+v", err, results2)
		}
		nomadMu.Lock()
		secondCount := len(nomadCalls)
		nomadMu.Unlock()
		if secondCount != callsCount {
			t.Fatalf("expected call count to remain %d due to caching, got %d", callsCount, secondCount)
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
				if err := os.WriteFile(cfgPath, []byte(tt.yamlContent), 0644); err != nil {
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
		_ = os.MkdirAll(jobsDir, 0755)
		_ = os.MkdirAll(filepath.Join(tmpDir, ".git"), 0755)
		_ = os.WriteFile(filepath.Join(jobsDir, "test.nomad"), []byte(`job "test" { task "t" { config { image = "ghcr.io/azylman/aerial:latest" } } }`), 0644)

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
					_ = os.MkdirAll(filepath.Join(repoDir, ".git"), 0755)
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

		oldFn, oldMetaFn := writeCountersDirFn, writeMetaDirFn
		defer func() { writeCountersDirFn, writeMetaDirFn = oldFn, oldMetaFn }()

		called, metaCalled := false, false
		writeCountersDirFn = func(dir string) error { called = true; return nil }
		writeMetaDirFn = func(dir string) error { metaCalled = true; return nil }
		_ = os.Setenv("GOCOVERDIR", t.TempDir())

		rec2 := httptest.NewRecorder()
		mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2.Code != http.StatusOK || !called || !metaCalled {
			t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", rec2.Code, called, metaCalled)
		}

		writeMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
		rec2b := httptest.NewRecorder()
		mux.ServeHTTP(rec2b, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
		if rec2b.Code != http.StatusOK {
			t.Errorf("expected 200 OK when meta flush errors, got %d", rec2b.Code)
		}

		writeCountersDirFn = func(dir string) error { return errors.New("simulated flush error") }
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
	name, dirAsFile, wantErrSub string
	files                       map[string]string
	disableNomad, nomadErr      bool
}

func runNomadValidationCases(t *testing.T, cases []nomadSyncValidationCase, syncFn func(d *SyncDaemon, dir string) error) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNomadConfigSyncTestEnv(t)
			if tc.dirAsFile != "" {
				if err := os.MkdirAll(filepath.Join(env.configDir, tc.dirAsFile), 0755); err != nil {
					t.Fatal(err)
				}
			}
			for rel, content := range tc.files {
				full := filepath.Join(env.configDir, rel)
				_ = os.MkdirAll(filepath.Dir(full), 0755)
				_ = os.WriteFile(full, []byte(content), 0644)
			}
			if tc.disableNomad {
				env.daemon.nomadAddr = ""
			}
			if tc.nomadErr {
				env.daemon.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
				}
			}
			err := syncFn(env.daemon, env.configDir)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
			} else if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
		})
	}
}

func TestSyncBrainConfigToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validBrainYAML := "model: gemini-2.5-flash\nchannels:\n  default:\n    mode: threads\n"

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		runNomadValidationCases(t, []nomadSyncValidationCase{
			{name: "missing_config_yaml_safe_noop"},
			{name: "directory_as_config_yaml_error", dirAsFile: "config.yaml", wantErrSub: "is a directory"},
			{name: "invalid_yaml_syntax_error", files: map[string]string{"config.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "semantic_validation_missing_channels_default", files: map[string]string{"config.yaml": "channels:\n  other:\n    mode: threads\n"}, wantErrSub: "channels.default is required"},
			{name: "disabled_nomad_addr_safe_noop", files: map[string]string{"config.yaml": validBrainYAML}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error", files: map[string]string{"config.yaml": validBrainYAML}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},
		}, func(d *SyncDaemon, dir string) error {
			return d.SyncBrainConfigToNomad(ctx, dir)
		})
	})

	t.Run("hash_caching_and_invalidation", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		cfgPath := filepath.Join(env.configDir, "config.yaml")
		_ = os.WriteFile(cfgPath, []byte(validBrainYAML), 0644)

		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on initial push: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || len(calls[0]) < 5 || calls[0][0] != "var" || calls[0][1] != "put" || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/brain" {
			t.Fatalf("unexpected initial nomad calls: %v", calls)
		}

		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 1 {
			t.Fatalf("expected cached call to remain at 1 call, err=%v, calls=%d", err, len(env.nomadCalls()))
		}

		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: classify\n"), 0644)
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 2 {
			t.Fatalf("expected modified config to push 2nd call, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
	})

	t.Run("diff_gating_and_deploy_event_lifecycle", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		cfgPath := filepath.Join(env.configDir, "config.yaml")
		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: mention\n"), 0644)

		scUntouched := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}}
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scUntouched); err != nil || len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for untouched diff, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cached := env.daemon.lastPushedConfig["nomad/jobs/brain:CONFIG_YAML"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cached == "" {
			t.Fatalf("expected cache to be primed for brain config")
		}

		scSameContent := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}}
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scSameContent); err != nil || len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for identical content, err=%v, calls=%d", err, len(env.nomadCalls()))
		}

		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: always\n"), 0644)
		scModified := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}}
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scModified); err != nil {
			t.Fatalf("SyncBrainConfigToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || ! strings.Contains(strings.Join(calls[0], " "), "COMMIT_SHA=sha_modified") {
			t.Fatalf("expected 1 nomad call with COMMIT_SHA=sha_modified, got %v", calls)
		}
		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "brain" || evts[0].Repo != scModified.Repo || evts[0].CommitSHA != scModified.CommitSHA || evts[0].PRNumber != scModified.PRNumber || evts[0].TargetID != scModified.TargetID {
			t.Errorf("unexpected deploy_started event payload: %+v", evts[0])
		}
	})
}

func TestSyncHomepageConfigToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("empty_dir_branch_pushes_empty_hash", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected nil for empty configDir, got %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || len(calls[0]) < 5 || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/homepage" {
			t.Fatalf("expected nomad/jobs/homepage target, got %v", calls)
		}
		joined := strings.Join(calls[0], " ")
		if !strings.Contains(joined, "CONFIG_HASH=") || strings.Contains(joined, "HOMEPAGE_YAML=@") {
			t.Errorf("expected CONFIG_HASH and NO HOMEPAGE_YAML on empty dir, got %v", calls[0])
		}
	})

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		runNomadValidationCases(t, []nomadSyncValidationCase{
			{name: "directory_as_homepage_yaml_error", dirAsFile: "services/homepage/homepage.yaml", wantErrSub: "is a directory"},
			{name: "invalid_yaml_syntax_in_homepage_yaml", files: map[string]string{"services/homepage/homepage.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "disabled_nomad_addr_safe_noop", files: map[string]string{"services/homepage/homepage.yaml": "port: \"3001\"\n"}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error", files: map[string]string{"services/homepage/homepage.yaml": "port: \"3001\"\n"}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},
		}, func(d *SyncDaemon, dir string) error {
			return d.SyncHomepageConfigToNomad(ctx, dir)
		})
	})

	t.Run("hash_caching_and_dir_filtering", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "homepage"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\nallowed_hosts: \"*\"\n"), 0644)
		_ = os.MkdirAll(filepath.Join(env.configDir, "homepage", "subdir"), 0755)
		for name, content := range map[string]string{"services.yaml": "- My Group: []\n", "widgets.yaml": "- search: {}\n", ".hidden": "ignore me", "temp.tmp": "ignore me too"} {
			_ = os.WriteFile(filepath.Join(env.configDir, "homepage", name), []byte(content), 0644)
		}

		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success pushing to nomad: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || len(calls[0]) < 6 || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/homepage" {
			t.Fatalf("unexpected nomad CLI args: %v", calls)
		}
		joined := strings.Join(calls[0], " ")
		if !strings.Contains(joined, "CONFIG_HASH=") || !strings.Contains(joined, "HOMEPAGE_YAML=@") {
			t.Errorf("missing CONFIG_HASH or HOMEPAGE_YAML in args: %v", calls[0])
		}

		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 1 {
			t.Fatalf("expected cached call to remain 1, err=%v, calls=%d", err, len(env.nomadCalls()))
		}

		_ = os.WriteFile(filepath.Join(env.configDir, "services", "homepage", "homepage.yaml"), []byte("title: Updated Aerial Dashboard\n"), 0644)
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 2 {
			t.Fatalf("expected calls to become 2 after update, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
	})

	t.Run("diff_gating_and_deploy_event_lifecycle", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "homepage"), 0755)
		_ = os.MkdirAll(filepath.Join(env.configDir, "homepage"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "homepage", "homepage.yaml"), []byte("title: Homepage\n"), 0644)
		subYaml := filepath.Join(env.configDir, "homepage", "services.yaml")
		_ = os.WriteFile(subYaml, []byte("services:\n  - name: test\n"), 0644)

		scUntouched := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}}
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir, scUntouched); err != nil || len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for untouched diff, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cached := env.daemon.lastPushedConfig["nomad/jobs/homepage"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cached == "" {
			t.Fatalf("expected cache to be primed for homepage config")
		}

		_ = os.WriteFile(subYaml, []byte("services:\n  - name: test2\n"), 0644)
		scSubTouched := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_sub", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"homepage/services.yaml"}}
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir, scSubTouched); err != nil {
			t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || !strings.Contains(strings.Join(calls[0], " "), "COMMIT_SHA=sha_sub") {
			t.Fatalf("expected 1 nomad call with COMMIT_SHA=sha_sub, got %v", calls)
		}
		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "homepage" || evts[0].Repo != scSubTouched.Repo || evts[0].CommitSHA != scSubTouched.CommitSHA || evts[0].PRNumber != scSubTouched.PRNumber || evts[0].TargetID != scSubTouched.TargetID {
			t.Errorf("unexpected deploy_started event payload: %+v", evts[0])
		}
	})
}

func TestSyncServiceConfigsToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		runNomadValidationCases(t, []nomadSyncValidationCase{
			{name: "empty_dir_graceful_skip"},
			{name: "directory_instead_of_file_error", dirAsFile: "services/mcp/scheduler-mcp.yaml", wantErrSub: "failed to read service config"},
			{name: "invalid_yaml_syntax_in_service_config", files: map[string]string{"services/mcp/scheduler-mcp.yaml": "invalid: yaml: [unclosed"}, wantErrSub: "invalid YAML syntax"},
			{name: "disabled_nomad_addr_safe_noop", files: map[string]string{"services/mcp/scheduler-mcp.yaml": "port: \"4005\"\n"}, disableNomad: true},
			{name: "nomad_cli_failure_returns_error", files: map[string]string{"services/mcp/scheduler-mcp.yaml": "port: \"4005\"\n"}, nomadErr: true, wantErrSub: "failed updating Nomad variable"},
		}, func(d *SyncDaemon, dir string) error {
			return d.SyncServiceConfigsToNomad(ctx, dir)
		})
	})

	t.Run("non_yaml_config_skips_yaml_validation", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		env.daemon.serviceConfigs = []ServiceConfigMapping{{RelPath: "proxy/default.conf", NomadVar: "nomad/jobs/mesh-proxy", VarKey: "DEFAULT_CONF"}}
		_ = os.MkdirAll(filepath.Join(env.configDir, "proxy"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "proxy", "default.conf"), []byte("server { listen 80; proxy_pass http://backend; }"), 0644)
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 1 {
			t.Fatalf("expected non-YAML config to sync with 1 call, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
	})

	t.Run("hash_caching_and_sequence_yaml_sync", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		for rel, content := range map[string]string{
			"services/mcp/scheduler-mcp.yaml":            "port: \"4005\"\n",
			"services/mcp/docker-mcp.yaml":               "port: \"4002\"\n",
			"services/webhooks-router/webhooks-router.yaml": "port: \"4020\"\n",
			"services/victoriametrics/scrape.yml":        "- job_name: \"anomaly-detector\"\n",
		} {
			full := filepath.Join(env.configDir, rel)
			_ = os.MkdirAll(filepath.Dir(full), 0755)
			_ = os.WriteFile(full, []byte(content), 0644)
		}

		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 4 {
			t.Fatalf("expected 4 nomad var put calls, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 4 {
			t.Fatalf("expected cached calls to remain 4, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "mcp", "docker-mcp.yaml"), []byte("docker_host: unix:///var/run/docker.sock\n# modified\n"), 0644)
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil || len(env.nomadCalls()) != 5 {
			t.Fatalf("expected calls to increase to 5, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
	})

	t.Run("service_config_mappings_precedence_and_invariants", func(t *testing.T) {
		t.Parallel()
		for _, m := range DefaultManagedServiceConfigs {
			if strings.Contains(m.NomadVar, "orin-voice") || strings.Contains(m.NomadVar, "mirrormere") {
				t.Errorf("expected DefaultManagedServiceConfigs to remain generic, found: %v", m)
			}
		}

		customDir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(customDir, "services", "hangar"), 0755)
		_ = os.WriteFile(filepath.Join(customDir, "services", "hangar", "hangar.yaml"), []byte("service_configs:\n  - rel_path: \"services/custom/custom.yaml\"\n    nomad_var: \"nomad/jobs/custom-job\"\n    var_key: \"CONFIG_YAML\"\n"), 0644)
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
	})

	t.Run("diff_gating_and_deploy_event_lifecycle", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		for rel, content := range map[string]string{
			"services/voice/voice.yaml":       "model: small.en\n",
			"services/mcp/scheduler-mcp.yaml": "enabled: true\n",
			"services/hangar/hangar.yaml":     "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/orin-voice\"\n    var_key: \"CONFIG_YAML\"\n  - rel_path: \"services/mcp/scheduler-mcp.yaml\"\n    nomad_var: \"nomad/jobs/scheduler-mcp\"\n    var_key: \"CONFIG_YAML\"\n",
		} {
			full := filepath.Join(env.configDir, rel)
			_ = os.MkdirAll(filepath.Dir(full), 0755)
			_ = os.WriteFile(full, []byte(content), 0644)
		}

		scHangarOnly := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_pr264", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/hangar/hangar.yaml"}}
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir, scHangarOnly); err != nil || len(env.nomadCalls()) != 0 {
			t.Fatalf("expected ZERO nomad calls for hangar-only diff, err=%v, calls=%d", err, len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cachedVoice, cachedSched := env.daemon.lastPushedConfig["nomad/jobs/orin-voice:CONFIG_YAML"], env.daemon.lastPushedConfig["nomad/jobs/scheduler-mcp:CONFIG_YAML"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cachedVoice == "" || cachedSched == "" {
			t.Fatalf("expected cache to be primed for untouched service configs")
		}

		_ = os.WriteFile(filepath.Join(env.configDir, "services", "voice", "voice.yaml"), []byte("model: distil-small.en\n"), 0644)
		scVoiceOnly := ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_voice_update", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}}
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir, scVoiceOnly); err != nil {
			t.Fatalf("SyncServiceConfigsToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 || !strings.Contains(strings.Join(calls[0], " "), "COMMIT_SHA=sha_voice_update") {
			t.Fatalf("expected 1 nomad call with COMMIT_SHA=sha_voice_update, got %v", calls)
		}
		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "orin-voice" || evts[0].Repo != scVoiceOnly.Repo || evts[0].CommitSHA != scVoiceOnly.CommitSHA || evts[0].PRNumber != scVoiceOnly.PRNumber || evts[0].TargetID != scVoiceOnly.TargetID {
			t.Errorf("unexpected deploy_started event payload: %+v", evts[0])
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
	d := &SyncDaemon{reconcileFn: func(ctx context.Context) error { calledSync = true; return nil }}
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
		if err := os.MkdirAll(binDir, 0755); err != nil {
			t.Fatalf("failed to create bin dir: %v", err)
		}
		if runtime.GOOS == "windows" {
			script := "@echo off\r\nset \"arg=%*\"\r\necho %arg% | findstr /i \"waitloop\" >nul && goto do_wait\r\necho %arg% | findstr /i \"compose\" >nul && (echo Docker Compose mock v2.0 & exit /b 0)\r\necho %arg% | findstr /i \"version\" >nul && (echo Docker mock v24.0 & exit /b 0)\r\necho unknown cmd >&2\r\nexit /b 1\r\n:do_wait\r\ngoto do_wait\r\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker.cmd"), []byte(script), 0755)
		} else {
			script := "#!/bin/sh\ncase \"$*\" in\n  *\"waitloop\"*) trap 'exit 0' TERM INT; while :; do :; done;;\n  *\"compose\"*) echo \"Docker Compose mock v2.0\"; exit 0;;\n  *\"version\"*) echo \"Docker mock v24.0\"; exit 0;;\n  *) echo \"unknown cmd\" >&2; exit 1;;\nesac\n"
			_ = os.WriteFile(filepath.Join(binDir, "docker"), []byte(script), 0755)
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
		dCustomDisp := &SyncDaemon{deployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error { customDisp = true; return nil }}
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
			name               string
			invalidURL, wantErr bool
			status             int
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

func TestEnsureDockerAuth_TableDriven(t *testing.T) {
	dummyPAT := "ghp_mock_token_12345"

	t.Run("nil_and_empty_daemon", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		for _, err := range []error{nilDaemon.EnsureDockerAuth(), (&SyncDaemon{pat: ""}).EnsureDockerAuth(), NewDaemon(DaemonConfig{PAT: ""}).EnsureDockerAuth()} {
			if err != nil {
				t.Errorf("expected nil error for empty/nil daemon, got %v", err)
			}
		}
	})

	t.Run("real_fs_callbacks", func(t *testing.T) {
		tmpDir := t.TempDir()
		lookup := func(k string) string {
			if k == "DOCKER_CONFIG" {
				return filepath.Join(tmpDir, ".docker")
			}
			return ""
		}
		if err := EnsureDockerAuthPath(dummyPAT, "", lookup, nil, nil, nil); err != nil {
			t.Fatalf("expected success writing docker config, got %v", err)
		}
		if content, err := os.ReadFile(filepath.Join(tmpDir, ".docker", "config.json")); err != nil || !strings.Contains(string(content), "ghcr.io") {
			t.Errorf("expected docker config with ghcr.io, got %s, err: %v", string(content), err)
		}
	})

	t.Run("daemon_EnsureDockerAuth_fs", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("DOCKER_CONFIG", tempDir)
		d := &SyncDaemon{pat: dummyPAT, configDir: "/share/aerial-config", repoUrls: map[string]string{"/share/aerial-config": "https://github.com/azylman/aerial-config.git"}}
		if err := d.EnsureDockerAuth(); err != nil {
			t.Fatalf("daemon.EnsureDockerAuth failed: %v", err)
		}
		if data, err := os.ReadFile(filepath.Join(tempDir, "config.json")); err != nil || !strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte("azylman:"+dummyPAT))) {
			t.Errorf("expected valid auth in written config, got %s, err: %v", string(data), err)
		}
	})

	t.Run("mock_callbacks_table", func(t *testing.T) {
		tests := []struct {
			name, pat, repoURL, wantErrSub, wantAuthUser string
			mkdirErr, readErr, writeErr                  error
		}{
			{name: "empty PAT in EnsureDockerAuthPath is no-op", pat: "", repoURL: "https://github.com/azylman/aerial-config.git"},
			{name: "mkdir failure returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", mkdirErr: errors.New("disk permission denied"), wantErrSub: "disk permission denied"},
			{name: "read error other than not-exist returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", readErr: errors.New("read fault"), wantErrSub: "read fault"},
			{name: "write failure returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", readErr: os.ErrNotExist, writeErr: errors.New("write fault"), wantErrSub: "write fault"},
			{name: "successful write creates valid docker config file with 0600 mode", pat: dummyPAT, repoURL: "https://github.com/custom-org/aerial-config.git", readErr: os.ErrNotExist, wantAuthUser: "custom-org"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var writtenPath string
				var writtenData []byte
				var writtenMode os.FileMode
				err := EnsureDockerAuthPath(
					tc.pat,
					tc.repoURL,
					func(k string) string {
						if k == "DOCKER_CONFIG" {
							return "/mock/docker"
						}
						return ""
					},
					func(string) ([]byte, error) { return nil, tc.readErr },
					func(path string, data []byte, mode os.FileMode) error {
						writtenPath, writtenData, writtenMode = path, data, mode
						return tc.writeErr
					},
					func(string, os.FileMode) error { return tc.mkdirErr },
				)
				if tc.wantErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
						t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.wantAuthUser != "" {
					if filepath.ToSlash(writtenPath) != "/mock/docker/config.json" || writtenMode != 0600 {
						t.Errorf("expected /mock/docker/config.json and 0600, got %s and %v", writtenPath, writtenMode)
					}
					var parsed struct {
						Auths map[string]struct {
							Auth string `json:"auth"`
						} `json:"auths"`
					}
					if err := json.Unmarshal(writtenData, &parsed); err != nil {
						t.Fatalf("failed to unmarshal written config: %v", err)
					}
					if entry, ok := parsed.Auths["ghcr.io"]; !ok || entry.Auth != base64.StdEncoding.EncodeToString([]byte(tc.wantAuthUser+":"+tc.pat)) {
						t.Errorf("unexpected ghcr auth entry: %+v", entry)
					}
				}
			})
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
			t.Run(tc.name, func(t *testing.T) { closeWarn(tc.closer, tc.name) })
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
				if got := isClientDisconnect(tc.req, tc.err); got != tc.want {
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
				writeResponse(tc.w, req, tc.body)
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

type mockOCIOptions struct {
	ProbeErr     error
	ProbeStatus  int
	ProbeHeaders map[string]string
	ProbeBody    string

	ChallengeAuthHeader string

	TokenErr       error
	TokenStatus    int
	TokenBody      string
	ExpectedPAT    string
	ExpectedScope  string
	OnTokenRequest func(req *http.Request)

	AuthErr          error
	AuthStatus       int
	AuthHeaders      map[string]string
	AuthBody         string
	ExpectedBearer   string
	ChildManifestMap map[string]string
	BlobMap          map[string]string

	OnRequest func(req *http.Request) (*http.Response, error, bool)
}

func newMockOCIRegistryClient(opts mockOCIOptions) *http.Client {
	return &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				if opts.OnRequest != nil {
					if resp, err, handled := opts.OnRequest(req); handled {
						return resp, err
					}
				}

				// Check token endpoint
				if strings.Contains(req.URL.Path, "/token") || strings.HasSuffix(req.URL.Path, "/token") || req.URL.Host == "auth.docker.io" {
					if opts.OnTokenRequest != nil {
						opts.OnTokenRequest(req)
					}
					if opts.TokenErr != nil {
						return nil, opts.TokenErr
					}
					if opts.ExpectedPAT != "" {
						_, pass, ok := req.BasicAuth()
						if !ok || pass != opts.ExpectedPAT {
							return &http.Response{
								StatusCode: http.StatusUnauthorized,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("bad basic auth")),
							}, nil
						}
					}
					if opts.ExpectedScope != "" {
						if req.URL.Query().Get("scope") != opts.ExpectedScope {
							return &http.Response{
								StatusCode: http.StatusBadRequest,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("unexpected scope")),
							}, nil
						}
					}
					status := opts.TokenStatus
					if status == 0 {
						status = http.StatusOK
					}
					body := opts.TokenBody
					if body == "" && status == http.StatusOK {
						body = `{"token":"mock-token"}`
					}
					return &http.Response{
						StatusCode: status,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(body)),
					}, nil
				}

				authHeader := req.Header.Get("Authorization")

				// Check child manifests
				if opts.ChildManifestMap != nil {
					for dgst, manifest := range opts.ChildManifestMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							if opts.AuthErr != nil {
								return nil, opts.AuthErr
							}
							status := opts.AuthStatus
							if status == 0 {
								status = http.StatusOK
							}
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: status,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(manifest)),
							}, nil
						}
					}
				}

				// Check config blobs
				if opts.BlobMap != nil {
					for dgst, blob := range opts.BlobMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							if opts.AuthErr != nil {
								return nil, opts.AuthErr
							}
							status := opts.AuthStatus
							if status == 0 {
								status = http.StatusOK
							}
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: status,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(blob)),
							}, nil
						}
					}
				}

				// Probe request without Authorization
				if authHeader == "" {
					if opts.ChallengeAuthHeader != "" {
						h := make(http.Header)
						h.Set("Www-Authenticate", opts.ChallengeAuthHeader)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     h,
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}
					if opts.ProbeErr != nil {
						return nil, opts.ProbeErr
					}
					status := opts.ProbeStatus
					if status == 0 {
						status = http.StatusOK
					}
					h := make(http.Header)
					for k, v := range opts.ProbeHeaders {
						h.Set(k, v)
					}
					return &http.Response{
						StatusCode: status,
						Header:     h,
						Body:       ioNopCloser(strings.NewReader(opts.ProbeBody)),
					}, nil
				}

				// Authenticated request
				if opts.ExpectedBearer != "" && authHeader != "Bearer "+opts.ExpectedBearer {
					h := make(http.Header)
					if opts.ChallengeAuthHeader != "" {
						h.Set("Www-Authenticate", opts.ChallengeAuthHeader)
					}
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     h,
						Body:       ioNopCloser(strings.NewReader("invalid bearer")),
					}, nil
				}

				if opts.AuthErr != nil {
					return nil, opts.AuthErr
				}

				// Check child manifests
				if opts.ChildManifestMap != nil {
					for dgst, manifest := range opts.ChildManifestMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(manifest)),
							}, nil
						}
					}
				}

				// Check config blobs
				if opts.BlobMap != nil {
					for dgst, blob := range opts.BlobMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(blob)),
							}, nil
						}
					}
				}

				status := opts.AuthStatus
				if status == 0 {
					status = http.StatusOK
				}
				h := make(http.Header)
				for k, v := range opts.AuthHeaders {
					h.Set(k, v)
				}
				return &http.Response{
					StatusCode: status,
					Header:     h,
					Body:       ioNopCloser(strings.NewReader(opts.AuthBody)),
				}, nil
			},
		},
	}
}

func TestGetRemoteImageDigest_TableDriven(t *testing.T) {
	t.Parallel()
	validDigest := "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	tests := []struct {
		name       string
		imageRef   string
		pat        string
		clientOpts mockOCIOptions
		wantDigest string
		wantErr    bool
	}{
		{
			name:     "invalid image reference returns error",
			imageRef: "invalid reference with spaces",
			wantErr:  true,
		},
		{
			name:     "direct 200 with Docker-Content-Digest",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "direct 200 with ETag fallback",
			imageRef: "pgvector/pgvector:pg16",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"ETag": `W/"` + validDigest + `"`},
			},
			wantDigest: validDigest,
		},
		{
			name:     "401 challenge exchange with GHCR and PAT",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			pat:      "secret-pat",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:azylman/aerial-brain:pull"`,
				ExpectedPAT:         "secret-pat",
				TokenBody:           `{"token":"test-ghcr-token"}`,
				ExpectedBearer:      "test-ghcr-token",
				AuthStatus:          http.StatusOK,
				AuthHeaders:         map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "401 challenge exchange with Docker Hub synthesized scope and access_token",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				ExpectedScope:       "repository:library/postgres:pull",
				TokenBody:           `{"access_token":"hub-token"}`,
				ExpectedBearer:      "hub-token",
				AuthStatus:          http.StatusOK,
				AuthHeaders:         map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "network failure on initial HEAD",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeErr: errors.New("simulated dial timeout"),
			},
			wantErr: true,
		},
		{
			name:     "401 with malformed authenticate header",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Basic realm="foo"`,
			},
			wantErr: true,
		},
		{
			name:     "401 with invalid token realm url",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="http://[::1]:namedport",service="test"`,
			},
			wantErr: true,
		},
		{
			name:     "401 with token network error",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenErr:            errors.New("token endpoint refused"),
			},
			wantErr: true,
		},
		{
			name:     "401 with token HTTP 500 error",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusInternalServerError,
				TokenBody:           "internal error",
			},
			wantErr: true,
		},
		{
			name:     "401 with invalid token JSON",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           "invalid-json",
			},
			wantErr: true,
		},
		{
			name:     "401 with empty token payload",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           `{"token":""}`,
			},
			wantErr: true,
		},
		{
			name:     "401 with authenticated HEAD network failure",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenBody:           `{"token":"abc"}`,
				AuthErr:             errors.New("auth head failed"),
			},
			wantErr: true,
		},
		{
			name:     "non-200 manifest status returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusNotFound,
			},
			wantErr: true,
		},
		{
			name:     "200 OK without valid digest or etag returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"ETag": `"not-a-sha256"`},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDaemon(DaemonConfig{
				PAT:            tc.pat,
				RegistryClient: newMockOCIRegistryClient(tc.clientOpts),
			})
			digest, err := d.GetRemoteImageDigest(context.Background(), tc.imageRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetRemoteImageDigest() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && digest != tc.wantDigest {
				t.Errorf("GetRemoteImageDigest() = %q, want %q", digest, tc.wantDigest)
			}
		})
	}
}

func TestCheckAndReconcileNomadImages_TableDriven(t *testing.T) {
	ctx := context.Background()

	t.Run("missing_jobs_dir_returns_nil", func(t *testing.T) {
		emptyDir := t.TempDir()
		d := NewDaemon(DaemonConfig{
			ComposeDir: emptyDir,
			ConfigDir:  emptyDir,
		})
		if err := d.CheckAndReconcileNomadImages(ctx); err != nil {
			t.Errorf("expected nil error for missing jobs directory, got %v", err)
		}
	})

	t.Run("directory_entries_unreadable_and_throttle_early_return", func(t *testing.T) {
		cfgDir := t.TempDir()
		jobsDir := filepath.Join(cfgDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0755)
		_ = os.MkdirAll(filepath.Join(jobsDir, "nested-dir"), 0755)
		_ = os.WriteFile(filepath.Join(jobsDir, "readme.txt"), []byte("not a nomad file"), 0644)
		_ = os.WriteFile(filepath.Join(jobsDir, "job1.nomad"), []byte(`job "job1" { task "t" { config { image = "ghcr.io/test/img1:latest" } } }`), 0644)

		dImages := NewDaemon(DaemonConfig{
			ComposeDir: cfgDir,
			ConfigDir:  cfgDir,
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
		dImages.nomadPollMu.Lock()
		dImages.lastNomadImagePoll = time.Time{}
		dImages.nomadPollMu.Unlock()

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
	})

	t.Run("image_polling_discovery_unchanged_changed_and_restart_handling", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Create a repo dir named "aerial" so filepath.Base(repo) resolves to "azylman/aerial"
		repoDir := filepath.Join(tmpDir, "aerial")
		jobsDir := filepath.Join(repoDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0755)

		jobFile := filepath.Join(jobsDir, "svc.nomad")
		// Non-azylman image ref triggers ImageSourceRepo == "" fallback to filepath.Base(repo) -> "azylman/aerial"
		content := `job "svc" {
  group "g" {
    task "t" {
      driver = "docker"
      config {
        image = "ghcr.io/test/img1:latest"
      }
    }
  }
}`
		_ = os.WriteFile(jobFile, []byte(content), 0644)

		currentDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
		mockClient := &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					if req.URL != nil && strings.Contains(req.URL.String(), "test/img1") {
						header := make(http.Header)
						header.Set("Docker-Content-Digest", currentDigest)
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
			Repos:          []string{repoDir},
			ComposeDir:     repoDir,
			ConfigDir:      repoDir,
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

		// 1. Initial discovery with nil nomadKnownDigests
		d.nomadKnownDigests = nil
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

		// 2. Unchanged digest poll (no restart)
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

		// 3. Changed digest poll with non-azylman image, git rev-parse HEAD fallback, and deploy_started event dispatch
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
			if dispatchedEvt.Event != "deploy_started" || dispatchedEvt.CommitSHA != "poll_commit_sha_123" || dispatchedEvt.Repo != "azylman/aerial" {
				t.Errorf("expected deploy_started with repo azylman/aerial and commit poll_commit_sha_123, got %+v", dispatchedEvt)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("timed out waiting for deploy_started event")
		}

		// Reset poll throttle
		d.nomadPollMu.Lock()
		d.lastNomadImagePoll = time.Time{}
		d.nomadPollMu.Unlock()

		// 4. Nomad job restart error handling
		currentDigest = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
		d.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			return nil, []byte("nomad down"), errors.New("restart fail")
		}
		err = d.CheckAndReconcileNomadImages(ctx)
		if err != nil {
			t.Fatalf("poll with restart fail returned error: %v", err)
		}
	})

	t.Run("nomad_image_poll_uses_image_revision", func(t *testing.T) {
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

		mockClient := newMockOCIRegistryClient(mockOCIOptions{
			OnRequest: func(req *http.Request) (*http.Response, error, bool) {
				if req.Method == http.MethodHead {
					header := make(http.Header)
					header.Set("Docker-Content-Digest", newDigest)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     header,
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
					t.Errorf("commit_sha = %q, want %q", evt.CommitSHA, expectedImageRevision)
				}
				if evt.Repo != "azylman/aerial" {
					t.Errorf("repo = %q, want azylman/aerial", evt.Repo)
				}
				if evt.Digest != newDigest {
					t.Errorf("digest = %q, want %q", evt.Digest, newDigest)
				}
			case <-time.After(2 * time.Second):
				t.Errorf("timed out waiting for deploy_started event")
		}
	})
}


func TestCheckGitHubRepoBuildInProgress_TableDriven(t *testing.T) {
	ctx := context.Background()

	var dNil *SyncDaemon
	if dNil.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected false for nil daemon")
	}

	tests := []struct {
		name      string
		repo      string
		pat       string
		respCode  int
		respBody  string
		clientErr error
		want      bool
	}{
		{name: "empty repo slug", repo: "", want: false},
		{
			name: "in-progress workflow run exists", repo: "azylman/aerial", pat: "test-pat",
			respCode: http.StatusOK, respBody: `{"total_count": 1, "workflow_runs": [{}]}`, want: true,
		},
		{
			name: "queued workflow run exists", repo: "aerial", pat: "test-pat",
			respCode: http.StatusOK, respBody: `{"total_count": 2, "workflow_runs": [{},{}]}`, want: true,
		},
		{
			name: "zero runs", repo: "azylman/mirrormere",
			respCode: http.StatusOK, respBody: `{"total_count": 0, "workflow_runs": []}`, want: false,
		},
		{
			name: "HTTP 502 error", repo: "aerial",
			respCode: http.StatusBadGateway, respBody: "error", want: false,
		},
		{
			name: "client transport error", repo: "aerial",
			clientErr: errors.New("network timeout"), want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &SyncDaemon{
				pat: tc.pat,
				registryClient: &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							if tc.clientErr != nil {
								return nil, tc.clientErr
							}
							return &http.Response{
								StatusCode: tc.respCode,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader(tc.respBody)),
							}, nil
						},
					},
				},
			}
			got := d.CheckGitHubRepoBuildInProgress(ctx, tc.repo)
			if got != tc.want {
				t.Errorf("CheckGitHubRepoBuildInProgress(%q) = %v, want %v", tc.repo, got, tc.want)
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
}

func newPushEventTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *pushEventTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	aerialDir := filepath.Join(tmpDir, "aerial")
	aerialConfigDir := filepath.Join(tmpDir, "aerial-config")
	mirrormereDir := filepath.Join(tmpDir, "mirrormere")
	_ = os.MkdirAll(aerialDir, 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, "nomad", "jobs"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, "jobs"), 0755)
	_ = os.MkdirAll(mirrormereDir, 0755)
	_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(aerialConfigDir, ".git"), 0755)
	_ = os.MkdirAll(filepath.Join(mirrormereDir, ".git"), 0755)

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

func TestExecuteGitPushEvent_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("validation and routing", func(t *testing.T) {
		t.Parallel()
		env := newPushEventTestEnv(t)

		// 1. Missing repo
		resp, code := env.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{})
		if code != http.StatusBadRequest || resp.Status != "error" {
			t.Errorf("expected 400 Bad Request for missing repo, got %d (%s)", code, resp.Status)
		}

		// 2. Non-main branch
		respIgnored, codeIgnored := env.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial",
			Ref:  "refs/heads/feature-branch",
		})
		if codeIgnored != http.StatusOK || respIgnored.Status != "ignored" {
			t.Errorf("expected 200 ignored for non-main branch, got %d (%s)", codeIgnored, respIgnored.Status)
		}

		// 3. Unmanaged repository
		respNotFound, codeNotFound := env.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "nonexistent-repo",
			Ref:  "refs/heads/main",
		})
		if codeNotFound != http.StatusNotFound || respNotFound.Status != "error" {
			t.Errorf("expected 404 for unmanaged repo, got %d (%s)", codeNotFound, respNotFound.Status)
		}

		// 4. SyncRepo failure
		envSyncFail := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return nil, []byte("fatal: remote not found"), errors.New("sync failed")
			}
		})
		respSyncFail, codeSyncFail := envSyncFail.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial",
			Ref:  "refs/heads/main",
		})
		if codeSyncFail != http.StatusInternalServerError || respSyncFail.Status != "error" {
			t.Errorf("expected 500 sync error, got code %d (%s)", codeSyncFail, respSyncFail.Status)
		}

		// 5. Push to peripheral repo (mirrormere)
		respMirror, codeMirror := env.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "mirrormere",
			Ref:  "refs/heads/main",
		})
		if codeMirror != http.StatusOK || respMirror.Status != "accepted" || respMirror.Message != "repository synced" {
			t.Errorf("expected 200 accepted for mirrormere push, got %d (%+v)", codeMirror, respMirror)
		}
	})

	t.Run("event dispatching and fallback commit", func(t *testing.T) {
		t.Parallel()

		// 1. Successful sync with TargetID dispatches sync_success
		envSuccess := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					return []byte("sha_sync_ok"), nil, nil
				}
				return []byte(""), nil, nil
			}
		})

		resp, code := envSuccess.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial",
			Ref:      "refs/heads/main",
			Commit:   "commit_12345",
			TargetID: "1555405874565091380",
			PRNumber: 542,
		})
		if code != http.StatusOK || resp.Status != "accepted" {
			t.Fatalf("expected 200 accepted, got %d (%s)", code, resp.Status)
		}
		evts := envSuccess.collectEvents(1, 2*time.Second)
		if len(evts) == 0 {
			t.Fatal("timed out waiting for sync_success event")
		}
		if evts[0].Event != "sync_success" || evts[0].TargetID != "1555405874565091380" || evts[0].PRNumber != 542 {
			t.Errorf("unexpected sync_success event payload: %+v", evts[0])
		}

		// 2. Failed sync with TargetID dispatches sync_failed
		envFail := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && (args[0] == "fetch" || args[0] == "pull") {
					return nil, []byte("fatal: remote error"), fmt.Errorf("git fetch failed")
				}
				return []byte(""), nil, nil
			}
		})
		respFail, codeFail := envFail.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial",
			Ref:      "refs/heads/main",
			Commit:   "commit_fail",
			TargetID: "1555405874565091380",
			PRNumber: 542,
		})
		if codeFail != http.StatusInternalServerError || respFail.Status != "error" {
			t.Fatalf("expected 500 error, got %d (%s)", codeFail, respFail.Status)
		}
		failEvts := envFail.collectEvents(1, 2*time.Second)
		if len(failEvts) == 0 || failEvts[0].Event != "sync_failed" || failEvts[0].Status != "failed" || failEvts[0].TargetID != "1555405874565091380" {
			t.Errorf("unexpected sync_failed event payload: %+v", failEvts)
		}

		// 3. Fallback: Empty CurrentHead falls back to req.Commit
		envFallback := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				return []byte(""), nil, nil
			}
		})
		respFallback, codeFallback := envFallback.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial",
			Ref:      "refs/heads/main",
			Commit:   "commit_req_fallback",
			TargetID: "1555405874565091380",
			PRNumber: 542,
		})
		if codeFallback != http.StatusOK || respFallback.Status != "accepted" {
			t.Fatalf("expected 200 accepted, got %d (%s)", codeFallback, respFallback.Status)
		}
		fallbackEvts := envFallback.collectEvents(1, 2*time.Second)
		if len(fallbackEvts) == 0 || fallbackEvts[0].CommitSHA != "commit_req_fallback" {
			t.Errorf("expected CommitSHA commit_req_fallback, got %+v", fallbackEvts)
		}

		// 4. Empty TargetID still dispatches sync_success
		respEmpty, codeEmpty := envSuccess.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial",
			Ref:      "refs/heads/main",
			Commit:   "commit_empty_target",
			TargetID: "",
			PRNumber: 0,
		})
		if codeEmpty != http.StatusOK || respEmpty.Status != "accepted" {
			t.Fatalf("expected 200 accepted for empty TargetID, got %d (%s)", codeEmpty, respEmpty.Status)
		}
		emptyEvts := envSuccess.collectEvents(1, 2*time.Second)
		if len(emptyEvts) == 0 || emptyEvts[0].Event != "sync_success" || emptyEvts[0].TargetID != "" || emptyEvts[0].CommitSHA != "sha_sync_ok" {
			t.Errorf("unexpected empty TargetID event: %+v", emptyEvts)
		}
	})

	t.Run("aerial core diff branches and deferral", func(t *testing.T) {
		t.Parallel()

		// 1. Build changes (brain/main.go)
		var revParseCount int
		envBuild := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseCount++
						if revParseCount == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
					return []byte("brain/main.go\nDockerfile\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
		})
		respBuild, codeBuild := envBuild.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial",
			Ref:  "refs/heads/main",
		})
		if codeBuild != http.StatusOK || !respBuild.ContainersBuilding {
			t.Errorf("expected accepted with ContainersBuilding=true, got code %d, resp %+v", codeBuild, respBuild)
		}

		// 2. Aerial Core non-build nomad change applied directly
		var revParseNomad int
		envNomad := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseNomad++
						if revParseNomad == 1 {
							return []byte("sha_before"), nil, nil
						}
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
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return []byte("Evaluation ID: migrate-123"), nil, nil
			}
		})
		_ = os.WriteFile(filepath.Join(envNomad.aerialDir, "nomad", "jobs", "migrate.nomad"), []byte(`job "migrate" {}`), 0644)
		respNomad, codeNomad := envNomad.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial",
			Ref:      "refs/heads/main",
			PRNumber: 611,
			TargetID: "1555422677936644147",
		})
		if codeNomad != http.StatusOK || respNomad.ContainersBuilding || !respNomad.NomadChanged || len(respNomad.AppliedJobs) != 1 {
			t.Errorf("expected accepted with NomadChanged=true and applied job, got code %d, resp %+v", codeNomad, respNomad)
		}
		nomadEvts := envNomad.collectEvents(2, 2*time.Second)
		var foundMigrate bool
		for _, evt := range nomadEvts {
			if evt.JobName == "migrate" {
				foundMigrate = true
				if evt.PRNumber != 611 || evt.TargetID != "1555422677936644147" {
					t.Errorf("expected PRNumber 611 and TargetID 1555422677936644147, got %+v", evt)
				}
			}
		}
		if !foundMigrate {
			t.Errorf("missing migrate deploy event in: %+v", nomadEvts)
		}

		// 3. Aerial Core Nomad apply failure returns 500
		var revParseFail int
		envNomadFail := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseFail++
						if revParseFail == 1 {
							return []byte("sha_before"), nil, nil
						}
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
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
					return nil, []byte("validate error"), errors.New("invalid job")
				}
				return []byte("OK"), nil, nil
			}
		})
		_ = os.WriteFile(filepath.Join(envNomadFail.aerialDir, "nomad", "jobs", "migrate.nomad"), []byte(`job "migrate" {}`), 0644)
		respFail, codeFail := envNomadFail.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial",
			Ref:  "refs/heads/main",
		})
		if codeFail != http.StatusInternalServerError || respFail.Status != "error" {
			t.Errorf("expected 500 when nomad apply fails, got code %d (%s)", codeFail, respFail.Status)
		}

		// 4. Aerial Core no changes
		envNoChange := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					return []byte("sha1"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" {
					return []byte("docs/index.html\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
		})
		respNoChange, codeNoChange := envNoChange.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial",
			Ref:  "refs/heads/main",
		})
		if codeNoChange != http.StatusOK || respNoChange.Status != "accepted" {
			t.Errorf("expected 200 accepted for core no changes, got %d (%+v)", codeNoChange, respNoChange)
		}

		// 5. Mixed aerial core build + nomad deferral (404 manifest deferral)
		var revParseMixed int
		var appliedPaths []string
		var mu sync.Mutex
		envMixed := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" && revParseMixed == 0 {
						revParseMixed++
						return []byte("sha_before"), nil, nil
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
					return []byte("vanadis/Dockerfile\nnomad/jobs/vanadis.nomad\nnomad/jobs/coverage-ingest.nomad\n"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("A\tnomad/jobs/vanadis.nomad\nM\tnomad/jobs/coverage-ingest.nomad\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.RegistryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.String(), "actions/runs") {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"total_count": 0, "workflow_runs": []}`))}, nil
				}
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))}, nil
			}}}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					mu.Lock()
					appliedPaths = append(appliedPaths, args[len(args)-1])
					mu.Unlock()
				}
				return []byte("Evaluation ID: 999"), nil, nil
			}
		})
		_ = os.WriteFile(filepath.Join(envMixed.aerialDir, "nomad", "jobs", "vanadis.nomad"), []byte(`job "vanadis" { task "vanadis" { config { image = "ghcr.io/azylman/aerial-vanadis:${var.image_tag}" } } }`), 0644)
		_ = os.WriteFile(filepath.Join(envMixed.aerialDir, "nomad", "jobs", "coverage-ingest.nomad"), []byte(`job "coverage-ingest" { task "flush" { config { image = "golang:1.27.1-alpine" } } }`), 0644)

		respMixed, codeMixed := envMixed.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "azylman/aerial",
			Ref:      "refs/heads/main",
			Commit:   "sha_after",
			PRNumber: 719,
			TargetID: "1560318118792790111",
		})
		if codeMixed != http.StatusOK || !respMixed.ContainersBuilding || !respMixed.NomadChanged || len(respMixed.AppliedJobs) != 1 || respMixed.AppliedJobs[0] != "coverage-ingest" {
			t.Fatalf("unexpected git push response (%d): %+v", codeMixed, respMixed)
		}
		if pending := envMixed.daemon.drainPendingNomadChanges(); len(pending) != 0 {
			t.Fatalf("expected zero pendingNomadChanges queued after ExecuteGitPushEvent, got %+v", pending)
		}
		if err := envMixed.daemon.ReconcileNomadChanges(ctx, envMixed.aerialDir, []NomadFileChange{{Path: "nomad/jobs/vanadis.nomad", Action: "apply", JobName: "vanadis"}}); err != nil {
			t.Fatalf("unexpected error in ReconcileNomadChanges: %v", err)
		}
		mu.Lock()
		for _, p := range appliedPaths {
			if strings.Contains(p, "vanadis.nomad") {
				t.Fatalf("expected vanadis.nomad to be deferred when image is missing in registry, got: %v", appliedPaths)
			}
		}
		mu.Unlock()

		// 6. Aerial Core migration change immediately runs db-migrate while deferring container build
		var revParseMig int
		var migAppliedPaths []string
		envMig := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "rev-parse" && args[1] == "HEAD" && revParseMig == 0 {
					revParseMig++
					return []byte("sha_before_mig"), nil, nil
				}
				if len(args) >= 1 && args[0] == "rev-parse" {
					return []byte("sha_after_mig"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-only" {
					return []byte("brain/pkg/db/migrations/20261016182900_add_started_at.sql\nbrain/pkg/db/vanadis.go\n"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("A\tbrain/pkg/db/migrations/20261016182900_add_started_at.sql\nM\tbrain/pkg/db/vanadis.go\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					migAppliedPaths = append(migAppliedPaths, args[len(args)-1])
				}
				return []byte("Evaluation ID: db-migrate-eval"), nil, nil
			}
		})
		_ = os.WriteFile(filepath.Join(envMig.aerialDir, "nomad", "jobs", "db-migrate.nomad"), []byte(`job "db-migrate" { type = "batch" task "atlas" { config { image = "arigaio/atlas:1.3.3" } } }`), 0644)
		respMig, codeMig := envMig.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "sha_after_mig", PRNumber: 751, TargetID: "1560698400780062884"})
		if codeMig != http.StatusOK || !respMig.ContainersBuilding || !respMig.NomadChanged || len(respMig.AppliedJobs) != 1 || respMig.AppliedJobs[0] != "db-migrate" {
			t.Fatalf("expected db-migrate applied immediately with ContainersBuilding=true, got (%d): %+v", codeMig, respMig)
		}
		if len(migAppliedPaths) != 1 || !strings.HasSuffix(filepath.ToSlash(migAppliedPaths[0]), "nomad/jobs/db-migrate.nomad") {
			t.Fatalf("expected nomad/jobs/db-migrate.nomad to be executed, got: %v", migAppliedPaths)
		}
	})

	t.Run("aerial-config diff gating, notices, and deploy events", func(t *testing.T) {
		t.Parallel()

		// 1. Config sync notices: nomad var put fails gracefully with 200 OK
		envNotices := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) > 0 && args[0] == "log" {
					return []byte("sha123\x002026-10-05T00:00:00Z"), nil, nil
				}
				if len(args) > 0 && args[0] == "pull" {
					return []byte("Already up to date."), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "var" && args[1] == "put" {
					return nil, []byte("nomad var put permission denied"), errors.New("var put failed")
				}
				return []byte(""), nil, nil
			}
		})
		respNotices, codeNotices := envNotices.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:   "azylman/aerial-config",
			Commit: "sha123",
		})
		if codeNotices != http.StatusOK {
			t.Fatalf("expected 200 OK even with config sync notice, got %d (%+v)", codeNotices, respNotices)
		}

		// 2. Diff gating: untouched service configs skip nomad var put
		var muGating sync.Mutex
		var varPutJobs []string
		envGating := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadAddr = "http://127.0.0.1:4646"
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
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
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 4 && args[0] == "var" && args[1] == "put" {
					muGating.Lock()
					varPutJobs = append(varPutJobs, args[3])
					muGating.Unlock()
				}
				return []byte("ok"), nil, nil
			}
		})
		_ = os.MkdirAll(filepath.Join(envGating.aerialConfigDir, "services", "voice"), 0755)
		_ = os.MkdirAll(filepath.Join(envGating.aerialConfigDir, "services", "hangar"), 0755)
		_ = os.WriteFile(filepath.Join(envGating.aerialConfigDir, "services", "voice", "voice.yaml"), []byte("model: small.en\n"), 0644)
		hangarContent := "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/orin-voice\"\n    var_key: \"CONFIG_YAML\"\n"
		_ = os.WriteFile(filepath.Join(envGating.aerialConfigDir, "services", "hangar", "hangar.yaml"), []byte(hangarContent), 0644)

		respGating, codeGating := envGating.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:   "azylman/aerial-config",
			Commit: "sha_curr",
		})
		if codeGating != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (%+v)", codeGating, respGating)
		}
		muGating.Lock()
		if len(varPutJobs) != 0 {
			t.Fatalf("expected zero var put calls for untouched service configs, got: %v", varPutJobs)
		}
		muGating.Unlock()

		// 3. Deferred nomad job due to active build vs Image Ready
		jobWithAerialImg := `job "webhooks-router" {
			task "router" {
				config {
					image = "ghcr.io/azylman/aerial-webhooks-router:latest"
				}
			}
		}`
		_ = os.WriteFile(filepath.Join(envNotices.aerialConfigDir, "jobs", "webhooks-router.nomad"), []byte(jobWithAerialImg), 0644)

		var revParseConfigDef int
		envDef := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseConfigDef++
						if revParseConfigDef == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.RegistryClient = &http.Client{
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
			}
		})
		_ = os.WriteFile(filepath.Join(envDef.aerialConfigDir, "jobs", "webhooks-router.nomad"), []byte(jobWithAerialImg), 0644)
		respDef, codeDef := envDef.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial-config",
			Ref:  "refs/heads/main",
		})
		if codeDef != http.StatusOK || !respDef.ContainersBuilding || len(respDef.AppliedJobs) != 0 {
			t.Errorf("expected job deferred when image source repo is building, got code %d, building %v, applied %v", codeDef, respDef.ContainersBuilding, respDef.AppliedJobs)
		}

		// 4. Image ready applies Nomad job and dispatches deploy_started
		var revParseConfigApp int
		envApp := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseConfigApp++
						if revParseConfigApp == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.RegistryClient = &http.Client{
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
			}
		})
		_ = os.WriteFile(filepath.Join(envApp.aerialConfigDir, "jobs", "webhooks-router.nomad"), []byte(jobWithAerialImg), 0644)
		respApp, codeApp := envApp.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:     "aerial-config",
			Ref:      "refs/heads/main",
			Commit:   "commit_abc123",
			PRNumber: 255,
			TargetID: "1555422677936644147",
		})
		if codeApp != http.StatusOK || respApp.ContainersBuilding || len(respApp.AppliedJobs) != 1 {
			t.Errorf("expected job applied when image ready, got code %d, building %v, applied %v", codeApp, respApp.ContainersBuilding, respApp.AppliedJobs)
		}
		appEvts := envApp.collectEvents(2, 2*time.Second)
		var hasSyncSuccess, hasDeployStarted bool
		for _, evt := range appEvts {
			if evt.Event == "sync_success" {
				hasSyncSuccess = true
			}
			if evt.Event == "deploy_started" && evt.JobName == "webhooks-router" {
				hasDeployStarted = true
				if evt.PRNumber != 255 || evt.TargetID != "1555422677936644147" || evt.CommitSHA != "commit_abc123" || evt.Status != "started" {
					t.Errorf("unexpected deploy_started payload: %+v", evt)
				}
			}
		}
		if !hasSyncSuccess || !hasDeployStarted {
			t.Errorf("expected sync_success and deploy_started, got: %+v", appEvts)
		}

		// 5. Deploy failed dispatches deploy_failed
		var revParseDeployFail int
		envDeployFail := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseDeployFail++
						if revParseDeployFail == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("M jobs/webhooks-router.nomad\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					return nil, []byte("driver error: failed to place allocation"), errors.New("nomad run failed")
				}
				return []byte("OK"), nil, nil
			}
		})
		_ = os.WriteFile(filepath.Join(envDeployFail.aerialConfigDir, "jobs", "webhooks-router.nomad"), []byte(`job "webhooks-router" {}`), 0644)
		respDepFail, codeDepFail := envDeployFail.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo:   "aerial-config",
			Ref:    "refs/heads/main",
			Commit: "fail_commit_123",
		})
		if codeDepFail != http.StatusOK || len(respDepFail.AppliedJobs) != 0 {
			t.Errorf("expected 200 with 0 applied jobs, got %d (%+v)", codeDepFail, respDepFail)
		}
		failEvtsAll := envDeployFail.collectEvents(3, 2*time.Second)
		var hasDepFail bool
		for _, evt := range failEvtsAll {
			if evt.Event == "deploy_failed" {
				hasDepFail = true
				if evt.Status != "failed" || !strings.Contains(evt.Details, "driver error") {
					t.Errorf("expected failed status and driver error details, got: %+v", evt)
				}
			}
		}
		if !hasDepFail {
			t.Errorf("missing deploy_failed event in: %+v", failEvtsAll)
		}

		// 6. Complex aerial-config diff: deleted, deferred, unreadable, invalid, run-failed
		_ = os.WriteFile(filepath.Join(envNotices.aerialConfigDir, "jobs", "deferred.nomad"), []byte(`job "deferred" { task "t" { config { image = "ghcr.io/azylman/aerial-brain:latest" } } }`), 0644)
		_ = os.WriteFile(filepath.Join(envNotices.aerialConfigDir, "jobs", "invalid.nomad"), []byte(`invalid syntax`), 0644)
		_ = os.WriteFile(filepath.Join(envNotices.aerialConfigDir, "jobs", "broken.nomad"), []byte(`job "broken" {}`), 0644)

		var revParseComplex int
		envComplex := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseComplex++
						if revParseComplex == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 2 && args[0] == "diff" && args[1] == "--name-status" {
					return []byte("D jobs/deleted.nomad\nM jobs/deferred.nomad\nM jobs/invalid.nomad\nM jobs/nonexistent.nomad\nM jobs/broken.nomad\n"), nil, nil
				}
				return []byte(""), nil, nil
			}
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
					if strings.Contains(args[len(args)-1], "invalid.nomad") {
						return nil, []byte("syntax error"), errors.New("invalid job")
					}
				}
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					if strings.Contains(args[len(args)-1], "broken.nomad") {
						return nil, []byte("run error"), errors.New("run failed")
					}
				}
				return []byte("OK"), nil, nil
			}
		})
		respComplex, codeComplex := envComplex.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial-config",
			Ref:  "refs/heads/main",
		})
		if codeComplex != http.StatusOK || !respComplex.NomadChanged {
			t.Errorf("expected 200 with NomadChanged=true for complex aerial-config, got %d (%+v)", codeComplex, respComplex)
		}

		// 7. Config diff error returns 500
		var revParseDiffErr int
		envDiffErr := newPushEventTestEnv(t, func(cfg *DaemonConfig) {
			cfg.GitExecutor = func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				if len(args) >= 1 && args[0] == "rev-parse" {
					if len(args) >= 2 && args[1] == "HEAD" {
						revParseDiffErr++
						if revParseDiffErr == 1 {
							return []byte("sha_before"), nil, nil
						}
					}
					return []byte("sha_after"), nil, nil
				}
				if len(args) >= 1 && (args[0] == "diff" || args[0] == "diff-tree") {
					return nil, []byte("diff fail"), errors.New("diff failed")
				}
				return []byte(""), nil, nil
			}
		})
		respDiffErr, codeDiffErr := envDiffErr.daemon.ExecuteGitPushEvent(ctx, GitPushEventRequest{
			Repo: "aerial-config",
			Ref:  "refs/heads/main",
		})
		if codeDiffErr != http.StatusInternalServerError || respDiffErr.Status != "error" {
			t.Errorf("expected 500 error on diff failure, got %d (%+v)", codeDiffErr, respDiffErr)
		}
	})
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
}

func newImageReadyTestEnv(t *testing.T, opts ...func(*DaemonConfig)) *imageReadyTestEnv {
	t.Helper()
	tmpDir := t.TempDir()
	jobsDir := filepath.Join(tmpDir, "jobs")
	_ = os.MkdirAll(jobsDir, 0755)

	coreJobsDir := filepath.Join(tmpDir, "core", "nomad", "jobs")
	_ = os.MkdirAll(coreJobsDir, 0755)

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
		NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
			if len(args) >= 2 && args[0] == "job" {
				switch args[1] {
				case "run":
					env.runMu.Lock()
					env.runCalls = append(env.runCalls, strings.Join(args, " "))
					env.runMu.Unlock()
					return []byte("Evaluation ID: eval-success-123"), nil, nil
				case "restart":
					env.restartMu.Lock()
					if len(args) >= 3 {
						env.restarts = append(env.restarts, args[len(args)-1])
					}
					env.restartMu.Unlock()
					return []byte("Restart triggered"), nil, nil
				case "status":
					return []byte(`{"ID":"status-ok","JobVersion":1,"LatestDeployment":{"ID":"dep-1","Status":"successful"}}`), nil, nil
				case "validate":
					return []byte("OK"), nil, nil
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
	if err := os.WriteFile(path, []byte(hcl), 0644); err != nil {
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

func TestExecuteImageReadyEvent_TableDriven(t *testing.T) {
	ctx := context.Background()

	t.Run("validation and routing edge cases", func(t *testing.T) {
		env := newImageReadyTestEnv(t)

		// 1. Missing image -> 400
		resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{})
		if code != http.StatusBadRequest || resp.Status != "error" {
			t.Errorf("expected 400 for empty image, got %d (%+v)", code, resp)
		}

		// 2. Unknown image -> 404
		resp, code = env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
			Image: "ghcr.io/azylman/nonexistent:latest",
		})
		if code != http.StatusNotFound || resp.Status != "not_found" {
			t.Errorf("expected 404 for unknown image, got %d (%+v)", code, resp)
		}
	})

	t.Run("deploy started dispatch and metadata propagation", func(t *testing.T) {
		env := newImageReadyTestEnv(t)
		env.writeJob("jobs", "brain.nomad", `job "brain" {
			task "brain" {
				config {
					image = "ghcr.io/azylman/aerial-brain:latest"
				}
			}
		}`)

		resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/aerial-brain:latest",
			Digest:    "sha256:digest_brain",
			Repo:      "azylman/aerial",
			CommitSHA: "headsha777",
			PRNumber:  535,
			TargetID:  "1555405874565091380",
		})
		if code != http.StatusOK || resp.Status != "accepted" || len(resp.MatchedJobs) != 1 {
			t.Fatalf("expected 200 accepted, got %d (%+v)", code, resp)
		}

		select {
		case evt := <-env.evtCh:
			if evt.Event != "deploy_started" {
				t.Errorf("expected deploy_started, got %q", evt.Event)
			}
			if evt.JobName != "brain" {
				t.Errorf("expected job_name brain, got %q", evt.JobName)
			}
			if evt.Repo != "azylman/aerial" || evt.CommitSHA != "headsha777" || evt.PRNumber != 535 || evt.TargetID != "1555405874565091380" {
				t.Errorf("metadata missing in deploy_started: %+v", evt)
			}
			if evt.Digest != "sha256:digest_brain" || evt.Image != "ghcr.io/azylman/aerial-brain:latest" {
				t.Errorf("digest/image mismatch: %+v", evt)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deploy_started")
		}

		// Ensure deployment observation is delegated to webhooks-router (no followup event)
		select {
		case evt := <-env.evtCh:
			t.Fatalf("unexpected followup event: %+v", evt)
		default:
		}
	})

	t.Run("variable tag injection and legacy restart fallbacks", func(t *testing.T) {
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

		// Case 1: CommitSHA provided -> injected into -var=image_tag
		t.Run("commit SHA variable injection", func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			routerPath := env.writeJob("jobs", "router.nomad", routerHCL)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:      "azylman/aerial",
				CommitSHA: "sha12345",
				PRNumber:  615,
				TargetID:  "discord_thread_999",
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted, got %d (%+v)", code, resp)
			}
			runs := env.getRunCalls()
			if len(runs) != 1 {
				t.Fatalf("expected 1 run call, got %v", runs)
			}
			want := "job run -detach -var=image_tag=sha12345 " + routerPath
			if runs[0] != want {
				t.Errorf("got run call %q, want %q", runs[0], want)
			}
			if len(env.getRestartCalls()) > 0 {
				t.Errorf("job restart should not be called for parameterized job")
			}
		})

		// Case 2: Digest fallback when CommitSHA is empty
		t.Run("digest fallback variable injection", func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.writeJob("jobs", "router.nomad", routerHCL)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest:   "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted for digest fallback, got %d (%+v)", code, resp)
			}
			runs := env.getRunCalls()
			wantArg := "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
			if len(runs) == 0 || !strings.Contains(runs[0], wantArg) {
				t.Errorf("expected args to contain %q, got %v", wantArg, runs)
			}
		})

		// Case 3: Unparameterized legacy job triggers restart fallback
		t.Run("unparameterized legacy job triggers restart", func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.writeJob("jobs", "legacy.nomad", `job "legacy" {
				task "legacy" {
					config {
						image = "ghcr.io/azylman/legacy-app:latest"
					}
				}
			}`)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/legacy-app:latest",
				CommitSHA: "legacy123",
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted for legacy app, got %d (%+v)", code, resp)
			}
			restarts := env.getRestartCalls()
			if len(restarts) != 1 || restarts[0] != "legacy" {
				t.Fatalf("expected restart for legacy job, got %v", restarts)
			}
			runs := env.getRunCalls()
			for _, r := range runs {
				if strings.Contains(r, "-var=image_tag=") {
					t.Fatalf("expected no -var=image_tag for unparameterized job, got %v", runs)
				}
			}
		})

		// Case 4: Empty CommitSHA and empty Digest falls back to latest when remote digest 404s
		t.Run("empty sha and digest fallback to latest", func(t *testing.T) {
			env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
				cfg.RegistryClient = &http.Client{
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
			})
			env.writeJob("jobs", "router.nomad", routerHCL)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-webhooks-router:latest",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted for empty sha/digest fallback, got %d (%+v)", code, resp)
			}
			runs := env.getRunCalls()
			wantArg := "-var=image_tag=latest"
			if len(runs) == 0 || !strings.Contains(runs[0], wantArg) {
				t.Fatalf("expected args to contain %q, got %v", wantArg, runs)
			}
		})

		// Case 5: Digest without sha256: prefix gets normalized
		t.Run("digest prefix normalization", func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.writeJob("jobs", "router.nomad", routerHCL)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/aerial-webhooks-router:latest",
				Digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:   "azylman/aerial",
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted for unadorned digest, got %d (%+v)", code, resp)
			}
			runs := env.getRunCalls()
			wantArg := "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
			if len(runs) == 0 || !strings.Contains(runs[0], wantArg) {
				t.Fatalf("expected args to contain %q with prepended sha256, got %v", wantArg, runs)
			}
		})

		// Case 6: Unparameterized legacy job with restart warning succeeds
		t.Run("unparameterized restart warning succeeds", func(t *testing.T) {
			env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
				cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
						return nil, []byte("restart failed warning"), errors.New("exit status 1")
					}
					return []byte("OK"), nil, nil
				}
			})
			env.writeJob("jobs", "legacy.nomad", `job "legacy" { task "t" { config { image = "ghcr.io/azylman/legacy-app:latest" } } }`)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image: "ghcr.io/azylman/legacy-app:latest",
			})
			if code != http.StatusOK || resp.Status != "accepted" {
				t.Fatalf("expected 200 accepted even if restart fails warning, got %d (%+v)", code, resp)
			}
		})
	})

	t.Run("validation errors, read errors and execution failure propagation", func(t *testing.T) {
		// 1. Validation failure on job
		t.Run("validation failure returns 500", func(t *testing.T) {
			env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
				cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
						return nil, []byte("syntax error in HCL"), errors.New("validate failed")
					}
					return []byte("OK"), nil, nil
				}
			})
			env.writeJob("jobs", "val-fail.nomad", `job "val-fail" { task "t" { config { image = "ghcr.io/azylman/val-fail:latest" } } }`)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image: "ghcr.io/azylman/val-fail:latest",
			})
			if code != http.StatusInternalServerError || resp.Status != "error" {
				t.Fatalf("expected 500 error on validation failure, got %d (%+v)", code, resp)
			}
			if !strings.Contains(resp.Message, "validation failed") {
				t.Errorf("expected validation failed in message, got %q", resp.Message)
			}
		})

		// 2. Mid-flight os.Remove inside RegistryClient triggers read error
		t.Run("mid-flight file removal triggers read error", func(t *testing.T) {
			var jobPath string
			env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
				cfg.RegistryClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							// Remove the job file before the loop reads it!
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
			})
			jobPath = env.writeJob("jobs", "read-fail.nomad", `job "read-fail" { task "t" { config { image = "ghcr.io/azylman/read-fail:latest" } } }`)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
				Image: "ghcr.io/azylman/read-fail:latest",
			})
			if code != http.StatusInternalServerError || resp.Status != "error" {
				t.Fatalf("expected 500 error on read failure, got %d (%+v)", code, resp)
			}
			if !strings.Contains(resp.Message, "read error") {
				t.Errorf("expected read error in message, got %q", resp.Message)
			}
		})

		// 3. Nomad job run non-zero exit code emits deploy_failed
		t.Run("nomad job run error dispatches deploy_failed", func(t *testing.T) {
			env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
				cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
						return nil, []byte("Nomad agent connection refused"), errors.New("exit status 1")
					}
					return []byte("OK"), nil, nil
				}
			})
			env.writeJob("jobs", "failing.nomad", `job "failing-job" { task "t" { config { image = "ghcr.io/azylman/aerial-broken:latest" } } }`)

			resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
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
			deadline := time.After(2 * time.Second)
			for !sawFailed {
				select {
				case evt := <-env.evtCh:
					if evt.Event == "deploy_failed" {
						sawFailed = true
						if evt.Repo != "azylman/aerial" || evt.CommitSHA != "broken_sha" || evt.PRNumber != 999 || evt.TargetID != "thread_broken" {
							t.Errorf("metadata missing in deploy_failed event: %+v", evt)
						}
					}
				case <-deadline:
					t.Fatalf("timed out waiting for deploy_failed event")
				}
			}
		})
	})

	t.Run("core composeDir matching and digest caching", func(t *testing.T) {
		env := newImageReadyTestEnv(t, func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 2 && args[0] == "job" && args[1] == "restart" {
					return nil, []byte("restart warning"), errors.New("restart warning err")
				}
				return []byte("OK"), nil, nil
			}
		})
		env.writeJob("core", "core.nomad", `job "core-job" { task "t" { config { image = "ghcr.io/azylman/core-service:latest" } } }`)

		resp, code := env.daemon.ExecuteImageReadyEvent(ctx, ImageReadyEventRequest{
			Image:  "ghcr.io/azylman/core-service:latest",
			Digest: "sha256:core123456",
		})
		if code != http.StatusOK || resp.Status != "accepted" || len(resp.MatchedJobs) != 1 {
			t.Errorf("expected 200 accepted for core job match, got %d (%+v)", code, resp)
		}
		cached := env.getDigest("core-job:ghcr.io/azylman/core-service:latest")
		if cached != "sha256:core123456" {
			t.Errorf("expected cached digest sha256:core123456, got %q", cached)
		}
	})
}

func TestImageRolloutQueue_TableDriven(t *testing.T) {
	_ = context.Background()

	t.Run("enqueue image ready event validation and queueing", func(t *testing.T) {
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
			Repos:      []string{tmpDir},
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
	})

	t.Run("debounced loop execution synchronizes on channel without sleep", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0755)

		_ = os.WriteFile(filepath.Join(jobsDir, "service-a.nomad"), []byte(`job "service-a" { task "a" { config { image = "ghcr.io/azylman/service-a:latest" } } }`), 0644)
		_ = os.WriteFile(filepath.Join(jobsDir, "service-b.nomad"), []byte(`job "service-b" { task "b" { config { image = "ghcr.io/azylman/service-b:latest" } } }`), 0644)

		runSignal := make(chan string, 10)
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
				if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
					call := strings.Join(args, " ")
					select {
					case runSignal <- call:
					default:
					}
				}
				return []byte("OK"), nil, nil
			},
		})

		d.imageRolloutDebounce = 20 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

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

		// Channel-based synchronization: wait for both runs with timeout
		var executed []string
		for i := 0; i < 2; i++ {
			select {
			case call := <-runSignal:
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

		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0755)

		_ = os.WriteFile(filepath.Join(jobsDir, "service-panic.nomad"), []byte(`job "service-panic" { task "p" { config { image = "ghcr.io/azylman/service-panic:latest" } } }`), 0644)

		var runCount int
		var runMu sync.Mutex
		recoveredCh := make(chan struct{}, 2)

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
				select {
				case recoveredCh <- struct{}{}:
				default:
				}
				return []byte("OK"), nil, nil
			},
		})

		d.imageRolloutDebounce = 10 * time.Millisecond
		d.StartImageRolloutLoop(loopCtx)

		// First event triggers panic in worker
		_, code1 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "panic-sha",
		})
		if code1 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code1)
		}

		// Wait briefly for debounce timer to fire first panic
		<-time.After(25 * time.Millisecond)

		// Second event tests that loop survived panic and executes successfully
		_, code2 := d.EnqueueImageReadyEvent(ImageReadyEventRequest{
			Image:     "ghcr.io/azylman/service-panic:latest",
			CommitSHA: "recovered-sha",
		})
		if code2 != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", code2)
		}

		select {
		case <-recoveredCh:
			// Recovered and executed run 2!
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

		// 3. Hangar-last ordering
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

	t.Run("handle image ready event caches digest and emits deploy_started", func(t *testing.T) {
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		_ = os.MkdirAll(jobsDir, 0755)

		_ = os.WriteFile(filepath.Join(jobsDir, "test-caching.nomad"), []byte(`job "test-caching" {
			task "test-caching" {
				driver = "docker"
				config {
					image = "ghcr.io/azylman/test-caching:latest"
				}
			}
		}`), 0644)

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

		select {
		case dispatchedEvt := <-evtCh:
			if dispatchedEvt.Digest != expectedDigest {
				t.Errorf("expected dispatched event digest %q, got %q", expectedDigest, dispatchedEvt.Digest)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for deploy_started event dispatch")
		}
	})
}

func TestGetRemoteImageRevision_TableDriven(t *testing.T) {
	t.Parallel()
	expectedSHA := "0123456789abcdef0123456789abcdef01234567"
	childDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
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
	}`, childDigest)

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

	var child401Sent bool

	tests := []struct {
		name       string
		imageRef   string
		clientOpts mockOCIOptions
		wantRev    string
		wantErr    bool
	}{
		{
			name:     "invalid image reference returns error",
			imageRef: "invalid reference with spaces",
			wantErr:  true,
		},
		{
			name:     "401 with existingBearer clears token and retries",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:azylman/aerial-brain:pull"`,
				AuthBody:            indexJSON,
				ChildManifestMap: map[string]string{
					childDigest: manifestJSON,
				},
				BlobMap: map[string]string{
					configBlobDigest: configJSON,
				},
				OnRequest: func(req *http.Request) (*http.Response, error, bool) {
					if strings.Contains(req.URL.Path, childDigest) && !child401Sent {
						child401Sent = true
						h := make(http.Header)
						h.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:azylman/aerial-brain:pull"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     h,
							Body:       ioNopCloser(strings.NewReader("stale bearer")),
						}, nil, true
					}
					return nil, nil, false
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 1: OCI Index to Child Manifest to Config Blob",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   indexJSON,
				ChildManifestMap: map[string]string{
					childDigest: manifestJSON,
				},
				BlobMap: map[string]string{
					configBlobDigest: configJSON,
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 2: Direct Manifest with aerial.commit_sha annotations without fetching blob",
			imageRef: "ghcr.io/azylman/aerial-webhooks-router:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"config": { "digest": "sha256:unused" },
					"annotations": { "aerial.commit_sha": "%s" }
				}`, expectedSHA),
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 3: 401 challenge exchange + token + config blob",
			imageRef: "redis:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull"`,
				ExpectedScope:       "repository:library/redis:pull",
				TokenBody:           `{"token":"secret-bearer-token"}`,
				ExpectedBearer:      "secret-bearer-token",
				AuthStatus:          http.StatusOK,
				AuthBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				BlobMap: map[string]string{
					configBlobDigest: fmt.Sprintf(`{"config":{"Labels":{"org.label-schema.vcs-ref":"%s"}}}`, expectedSHA),
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 4: Index with annotations directly resolves revision",
			imageRef: "ghcr.io/azylman/aerial:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"manifests": [
						{
							"digest": "%s",
							"platform": {"os": "linux", "architecture": "amd64"}
						}
					],
					"annotations": {
						"org.opencontainers.image.revision": "%s"
					}
				}`, childDigest, expectedSHA),
			},
			wantRev: expectedSHA,
		},
		{
			name:     "missing labels in config blob returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				BlobMap: map[string]string{
					configBlobDigest: `{"config":{"Labels":{}}}`,
				},
			},
			wantErr: true,
		},
		{
			name:     "child manifest request fails with non-200",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   indexJSON,
				AuthStatus:  http.StatusNotFound,
				AuthBody:    "child manifest not found",
			},
			wantErr: true,
		},
		{
			name:     "manifest missing config digest and annotations returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   `{ "schemaVersion": 2 }`,
			},
			wantErr: true,
		},
		{
			name:     "blob fetch non-200 returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				AuthStatus: http.StatusBadGateway,
				AuthBody:   "gateway error on blob",
			},
			wantErr: true,
		},
		{
			name:     "probe 500 error returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusInternalServerError,
				ProbeBody:   "server internal error",
			},
			wantErr: true,
		},
		{
			name:     "probe network error returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeErr: errors.New("network dial timeout"),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDaemon(DaemonConfig{RegistryClient: newMockOCIRegistryClient(tc.clientOpts)})
			rev, err := d.GetRemoteImageRevision(context.Background(), tc.imageRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetRemoteImageRevision() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && rev != tc.wantRev {
				t.Errorf("GetRemoteImageRevision() = %q, want %q", rev, tc.wantRev)
			}
		})
	}
}


func TestNomadChangeDetection_TableDriven(t *testing.T) {

	t.Run("HasNomadConfigChanges_table", func(t *testing.T) {
		tests := []struct {
			name        string
			repo        string
			prev        string
			curr        string
			cancelEarly bool
			gitExec     GitExecutor
			wantChanged bool
			wantErr     bool
		}{
			{name: "pre-canceled context returns error", repo: "/path", prev: "v1", curr: "v2", cancelEarly: true, wantErr: true},
			{name: "empty repo path", repo: "", prev: "v1", curr: "v2"},
			{name: "empty prev head", repo: "/path", prev: "", curr: "v2"},
			{name: "empty curr head", repo: "/path", prev: "v1", curr: ""},
			{name: "identical heads", repo: "/path", prev: "v1", curr: "v1"},
			{
				name: "diff detects nomad config change",
				repo: "/path", prev: "v1", curr: "v2",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("nomad/nomad.hcl\nmain.go\n"), nil, nil
				},
				wantChanged: true,
			},
			{
				name: "diff detects no nomad config change",
				repo: "/path", prev: "v1", curr: "v2",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("README.md\ndocker-compose.yml\n"), nil, nil
				},
			},
			{
				name: "diff fails and fallback diff-tree succeeds with config changes",
				repo: "/path", prev: "v1", curr: "v2",
				gitExec: func() GitExecutor {
					call := 0
					return func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						call++
						if call == 1 {
							return nil, []byte("fatal: ambiguous"), errors.New("diff failed")
						}
						return []byte("nomad/client.hcl\n"), nil, nil
					}
				}(),
				wantChanged: true,
			},
			{
				name: "diff fails and fallback fails with normal error (fail-safe false)",
				repo: "/path", prev: "v1", curr: "v2",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return nil, []byte("fatal: repo corrupt"), errors.New("git fail")
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancelEarly {
					cancel()
				}
				d := &SyncDaemon{gitExecutor: tc.gitExec}
				got, err := d.HasNomadConfigChanges(ctx, tc.repo, tc.prev, tc.curr)
				if (err != nil) != tc.wantErr || got != tc.wantChanged {
					t.Fatalf("HasNomadConfigChanges() = (%v, %v), want (%v, wantErr:%v)", got, err, tc.wantChanged, tc.wantErr)
				}
			})
		}
	})

	t.Run("HasNomadConfigChanges_context_cancellation_paths", func(t *testing.T) {
		// 1. Context canceled during primary git diff
		ctx1, cancel1 := context.WithCancel(context.Background())
		d1 := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				cancel1()
				return nil, []byte("interrupted"), errors.New("signal: killed")
			},
		}
		if got, err := d1.HasNomadConfigChanges(ctx1, "/repo", "c1", "c2"); !errors.Is(err, context.Canceled) || got {
			t.Fatalf("expected context.Canceled during primary diff, got (%v, %v)", got, err)
		}

		// 2. Context canceled during fallback diff-tree
		ctx2, cancel2 := context.WithCancel(context.Background())
		calls2 := 0
		d2 := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				calls2++
				if calls2 == 1 {
					return nil, []byte("primary failed"), errors.New("diff failed")
				}
				cancel2()
				return nil, []byte("fallback interrupted"), errors.New("signal: killed")
			},
		}
		if got, err := d2.HasNomadConfigChanges(ctx2, "/repo", "c1", "c2"); !errors.Is(err, context.Canceled) || got {
			t.Fatalf("expected context.Canceled during fallback diff-tree, got (%v, %v)", got, err)
		}
	})

	t.Run("HasNomadChanges_table", func(t *testing.T) {
		tests := []struct {
			name        string
			repo        string
			prev        string
			curr        string
			cancelEarly bool
			gitExec     GitExecutor
			wantCount   int
			wantErr     bool
		}{
			{name: "pre-canceled context returns error", repo: "/repo", prev: "a", curr: "b", cancelEarly: true, wantErr: true},
			{name: "empty repo path", repo: "", prev: "a", curr: "b"},
			{name: "empty prev head", repo: "/repo", prev: "", curr: "b"},
			{name: "empty curr head", repo: "/repo", prev: "a", curr: ""},
			{name: "identical heads", repo: "/repo", prev: "a", curr: "a"},
			{
				name: "git diff success with multiple changes",
				repo: "/share/aerial-config", prev: "HEAD~1", curr: "HEAD",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("M\tjobs/infisical.nomad\nD\tjobs/legacy.nomad\nM\tdocker-compose.yml\n"), nil, nil
				},
				wantCount: 2,
			},
			{
				name: "git diff fails, fallback diff-tree succeeds",
				repo: "/repo", prev: "head1", curr: "head2",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					if len(args) > 0 && args[0] == "diff" {
						return nil, []byte("fatal: ambiguous"), errors.New("diff error")
					}
					return []byte("M\tjobs/app.nomad\n"), nil, nil
				},
				wantCount: 1,
			},
			{
				name: "git diff fails, fallback diff-tree fails (fail-safe nil)",
				repo: "/repo", prev: "head1", curr: "head2",
				gitExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return nil, []byte("fatal: bad object"), errors.New("git error")
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancelEarly {
					cancel()
				}
				d := &SyncDaemon{gitExecutor: tc.gitExec}
				changes, err := d.HasNomadChanges(ctx, tc.repo, tc.prev, tc.curr)
				if (err != nil) != tc.wantErr || len(changes) != tc.wantCount {
					t.Fatalf("HasNomadChanges() = (%v, %v), want (count:%d, wantErr:%v)", changes, err, tc.wantCount, tc.wantErr)
				}
			})
		}
	})

	t.Run("HasNomadChanges_context_cancellation_paths", func(t *testing.T) {
		// 1. Context canceled during primary git diff
		ctx1, cancel1 := context.WithCancel(context.Background())
		d1 := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				cancel1()
				return nil, []byte("interrupted"), errors.New("signal: killed")
			},
		}
		if ch, err := d1.HasNomadChanges(ctx1, "/repo", "c1", "c2"); !errors.Is(err, context.Canceled) || ch != nil {
			t.Fatalf("expected context.Canceled during primary diff, got (%v, %v)", ch, err)
		}

		// 2. Context canceled during fallback diff-tree
		ctx2, cancel2 := context.WithCancel(context.Background())
		calls2 := 0
		d2 := &SyncDaemon{
			gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
				calls2++
				if calls2 == 1 {
					return nil, []byte("diff err"), errors.New("diff err")
				}
				cancel2()
				return nil, []byte("fallback interrupted"), errors.New("signal: killed")
			},
		}
		if ch, err := d2.HasNomadChanges(ctx2, "/repo", "c1", "c2"); !errors.Is(err, context.Canceled) || ch != nil {
			t.Fatalf("expected context.Canceled during fallback diff-tree, got (%v, %v)", ch, err)
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

	t.Run("ParseNomadGitStatus_whitespace_and_entries", func(t *testing.T) {
		changes := ParseNomadGitStatus("\n\n   \n\t\nM jobs/app.nomad\nD jobs/old.nomad\n")
		if len(changes) != 2 || changes[0].JobName != "app" || changes[0].Action != "apply" || changes[1].JobName != "old" || changes[1].Action != "delete" {
			t.Fatalf("unexpected ParseNomadGitStatus result: %+v", changes)
		}
	})
}

func TestNomadValidationAndServerReconcile_TableDriven(t *testing.T) {

	t.Run("defaultNomadExecutor_and_accessors", func(t *testing.T) {
		ctx := context.Background()
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// 1. defaultNomadExecutor: with and without addr/token (normal + canceled context)
		for _, cfg := range [][2]string{{"", ""}, {"http://127.0.0.1:4646", "secret-token"}} {
			fn := defaultNomadExecutor(cfg[0], cfg[1])
			_, _, _ = fn(ctx, "version")
			_, _, _ = fn(cancelCtx, "version")
		}

		// Additional pre-canceled coverage for default executors
		_, _, _ = defaultComposeExecutor(cancelCtx, "", "version")
		_, _, _ = runGitCommand(cancelCtx, "", "", "version")

		// 2. getNomadExecutor on nil, default, and custom daemon
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
		tests := []struct {
			name      string
			nomadExec NomadExecutor
			wantErr   bool
		}{
			{name: "nil executor returns error", nomadExec: nil, wantErr: true},
			{
				name: "validation success",
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("Configuration is valid!\n"), nil, nil
				},
			},
			{
				name: "validation failure",
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("syntax error"), errors.New("exit 1")
				},
				wantErr: true,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{nomadExecutor: tc.nomadExec}
				if err := d.ValidateNomadConfig(context.Background(), "/tmp/nomad"); (err != nil) != tc.wantErr {
					t.Fatalf("ValidateNomadConfig() error = %v, wantErr %v", err, tc.wantErr)
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
		if err := os.MkdirAll(nomadDir, 0755); err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			name         string
			valExec      NomadExecutor
			composeExec  ComposeExecutor
			wantErr      bool
			expectCalled bool
		}{
			{
				name: "validation failure aborts compose restart",
				valExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("syntax error"), errors.New("val fail")
				},
				composeExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("restarted"), nil, nil
				},
				wantErr:      true,
				expectCalled: false,
			},
			{
				name: "validation success and restart success",
				valExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("valid"), nil, nil
				},
				composeExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("restarted"), nil, nil
				},
				wantErr:      false,
				expectCalled: true,
			},
			{
				name: "validation success and restart failure",
				valExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("valid"), nil, nil
				},
				composeExec: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return nil, []byte("compose down"), errors.New("compose fail")
				},
				wantErr:      true,
				expectCalled: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				called := false
				d := &SyncDaemon{
					nomadExecutor: tc.valExec,
					composeExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
						called = true
						return tc.composeExec(ctx, dir, args...)
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
		if err := os.MkdirAll(jobsDir, 0755); err != nil {
			t.Fatal(err)
		}
		hcl := `job "hangar" { group "g" { task "t" { config { image = "ghcr.io/custom/worker:v1" } } } }`
		if err := os.WriteFile(filepath.Join(jobsDir, "hangar.nomad"), []byte(hcl), 0644); err != nil {
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

func TestNomadJobReconciliation_TableDriven(t *testing.T) {

	t.Run("ValidateNomadJob_hermetic", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		jobFile := filepath.Join(tmpDir, "test.nomad")
		if err := os.WriteFile(jobFile, []byte(`job "test" {}`), 0644); err != nil {
			t.Fatal(err)
		}

		dSuccess := NewDaemon(DaemonConfig{
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return []byte("Job validation successful\n"), nil, nil
			},
		})
		if err := dSuccess.ValidateNomadJob(ctx, jobFile); err != nil {
			t.Fatalf("expected ValidateNomadJob success: %v", err)
		}

		dFail := NewDaemon(DaemonConfig{
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return nil, []byte("syntax error"), errors.New("exit 1")
			},
		})
		if err := dFail.ValidateNomadJob(ctx, filepath.Join(tmpDir, "invalid.nomad")); err == nil {
			t.Fatal("expected ValidateNomadJob error, got nil")
		}
	})

	t.Run("applyNomadChangesDirectly_branches", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		validJobPath := filepath.Join(tmpDir, "jobs", "test.nomad")
		if err := os.MkdirAll(filepath.Dir(validJobPath), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(validJobPath, []byte(`job "test" {}`), 0644); err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			name        string
			changes     []NomadFileChange
			nomadExec   NomadExecutor
			wantApplied []string
			wantErr     bool
		}{
			{
				name:    "delete action with stop error and success",
				changes: []NomadFileChange{{Action: "delete", JobName: "del", Path: "jobs/del.nomad"}},
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return nil, []byte("stop error"), errors.New("stop fail")
				},
				wantApplied: nil,
			},
			{
				name:        "unsafe job path is ignored",
				changes:     []NomadFileChange{{Action: "update", JobName: "evil", Path: "../evil.nomad"}},
				wantApplied: nil,
			},
			{
				name:    "validation error returns error",
				changes: []NomadFileChange{{Action: "update", JobName: "test", Path: "jobs/test.nomad"}},
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					if len(args) >= 2 && args[0] == "job" && args[1] == "validate" {
						return nil, []byte("invalid"), errors.New("val fail")
					}
					return []byte("ok"), nil, nil
				},
				wantErr: true,
			},
			{
				name:    "run error returns error",
				changes: []NomadFileChange{{Action: "update", JobName: "test", Path: "jobs/test.nomad"}},
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					if len(args) >= 2 && args[0] == "job" && args[1] == "run" {
						return nil, []byte("run error"), errors.New("run fail")
					}
					return []byte("ok"), nil, nil
				},
				wantErr: true,
			},
			{
				name:    "success applies job",
				changes: []NomadFileChange{{Action: "update", JobName: "test", Path: "jobs/test.nomad"}},
				nomadExec: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					return []byte("Evaluation ID: eval-123"), nil, nil
				},
				wantApplied: []string{"test"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{nomadExecutor: tc.nomadExec}
				applied, err := d.applyNomadChangesDirectly(ctx, tmpDir, tc.changes)
				if (err != nil) != tc.wantErr || len(applied) != len(tc.wantApplied) {
					t.Fatalf("applyNomadChangesDirectly() = (%v, %v), want (%v, wantErr:%v)", applied, err, tc.wantApplied, tc.wantErr)
				}
			})
		}

		// Metadata propagation branch
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
		applied, err := dMeta.applyNomadChangesDirectly(ctx, tmpDir, []NomadFileChange{
			{Action: "update", JobName: "test", Path: "jobs/test.nomad"},
		}, "azylman/aerial", "headsha123", "1555422677936644147", "123")
		if err != nil || len(applied) != 1 || applied[0] != "test" {
			t.Fatalf("applyNomadChangesDirectly with metadata failed: %v, %v", applied, err)
		}
		select {
		case evt := <-evtCh:
			if evt.PRNumber != 123 || evt.TargetID != "1555422677936644147" || evt.Repo != "azylman/aerial" || evt.CommitSHA != "headsha123" {
				t.Fatalf("unexpected metadata: %+v", evt)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deploy event")
		}
	})

	t.Run("ReconcileNomadChanges_teardown_order_and_cases", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		jobsDir := filepath.Join(tmpDir, "jobs")
		if err := os.MkdirAll(jobsDir, 0755); err != nil {
			t.Fatal(err)
		}
		validJobFile := filepath.Join(jobsDir, "valid.nomad")
		_ = os.WriteFile(validJobFile, []byte(`job "valid" {}`), 0644)
		invalidJobFile := filepath.Join(jobsDir, "invalid.nomad")
		_ = os.WriteFile(invalidJobFile, []byte(`job "invalid" {}`), 0644)
		runFailJobFile := filepath.Join(jobsDir, "runfail.nomad")
		_ = os.WriteFile(runFailJobFile, []byte(`job "runfail" {}`), 0644)

		var executedCalls [][]string
		var mu sync.Mutex
		dReconcile := NewDaemon(DaemonConfig{
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
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
			},
		})

		// Empty changes
		if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, nil); err != nil {
			t.Fatalf("expected nil for empty changes: %v", err)
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
			t.Fatal("expected validation error for invalid.nomad")
		}

		// Apply change run failure
		if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, []NomadFileChange{
			{JobName: "runfail", Path: "jobs/runfail.nomad", Action: "apply"},
		}); err == nil {
			t.Fatal("expected run error for runfail.nomad")
		}

		// Combined apply and delete: verify stop runs before run (teardown first)
		mu.Lock()
		executedCalls = nil
		mu.Unlock()

		changes := []NomadFileChange{
			{Path: "jobs/valid.nomad", Action: "apply", JobName: "valid"},
			{Path: "jobs/old.nomad", Action: "delete", JobName: "old-job"},
		}
		if err := dReconcile.ReconcileNomadChanges(ctx, tmpDir, changes); err != nil {
			t.Fatalf("expected ReconcileNomadChanges success: %v", err)
		}

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
	})

	t.Run("ExecuteNomadTeardowns_and_cross_repo_purge_protection", func(t *testing.T) {
		ctx := context.Background()
		dBasic := NewDaemon(DaemonConfig{})
		dBasic.ExecuteNomadTeardowns(ctx, nil)

		var stoppedJobs []string
		var mu sync.Mutex
		dTeardown := NewDaemon(DaemonConfig{
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
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
		mu.Lock()
		if len(stoppedJobs) != 2 {
			t.Fatalf("expected 2 jobs stopped, got %v", stoppedJobs)
		}
		mu.Unlock()

		// Cross-repo purge protection (SkipPurgeWhenDefinedInOtherRepo)
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
		if err := os.WriteFile(filepath.Join(nomadJobsDirB, "brain.nomad"), []byte(`job "brain" {}`), 0644); err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		stoppedJobs = nil
		mu.Unlock()

		dCross := NewDaemon(DaemonConfig{
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

		// Job brain deleted in repoA, but exists in repoB -> should NOT be stopped
		if err := dCross.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
			{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mu.Lock()
		if len(stoppedJobs) > 0 {
			t.Fatalf("expected 0 stopped jobs for brain, got %v", stoppedJobs)
		}
		stoppedJobs = nil
		mu.Unlock()

		// Job truly_deleted deleted in repoA and not in repoB -> SHOULD be stopped
		if err := dCross.ReconcileNomadChanges(ctx, repoA, []NomadFileChange{
			{Path: "jobs/truly_deleted.nomad", Action: "delete", JobName: "truly_deleted"},
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mu.Lock()
		if len(stoppedJobs) != 1 || stoppedJobs[0] != "truly_deleted" {
			t.Fatalf("expected truly_deleted to be stopped, got %v", stoppedJobs)
		}
		stoppedJobs = nil
		mu.Unlock()

		// ExecuteNomadTeardowns with both jobs
		dCross.ExecuteNomadTeardowns(ctx, []NomadChangeEvent{
			{
				RepoPath: repoA,
				Changes: []NomadFileChange{
					{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
					{Path: "jobs/old_orphan.nomad", Action: "delete", JobName: "old_orphan"},
				},
			},
		})
		mu.Lock()
		if len(stoppedJobs) != 1 || stoppedJobs[0] != "old_orphan" {
			t.Fatalf("expected only old_orphan stopped in teardowns, got %v", stoppedJobs)
		}
		mu.Unlock()
	})

	t.Run("ReconcilePendingNomad_table", func(t *testing.T) {
		ctx := context.Background()

		// 1. Empty queue
		dEmpty := NewDaemon(DaemonConfig{})
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
		if drained := dEmpty.drainPendingNomadChanges(); len(drained) != 2 || len(dEmpty.drainPendingNomadChanges()) != 0 {
			t.Fatalf("unexpected drain result: len=%d", len(drained))
		}

		// 3. Valid pending job with PR metadata
		tmpDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(tmpDir, "bot.nomad"), []byte(`job "bot" {}`), 0644)
		evtCh := make(chan HangarDeployEvent, 10)
		dMeta := NewDaemon(DaemonConfig{
			DeployDispatcher: func(ctx context.Context, evt HangarDeployEvent) error {
				evtCh <- evt
				return nil
			},
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				return []byte("Evaluation ID: run-bot"), nil, nil
			},
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
		_ = os.WriteFile(filepath.Join(invalidDir, "invalid.nomad"), []byte(`job "invalid" {}`), 0644)
		dFailVal := NewDaemon(DaemonConfig{
			NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				if len(args) >= 3 && args[0] == "job" && args[1] == "validate" {
					return nil, []byte("syntax error"), errors.New("val fail")
				}
				return []byte("ok"), nil, nil
			},
		})
		dFailVal.recordPendingNomadChanges(invalidDir, []NomadFileChange{
			{Path: "invalid.nomad", Action: "apply", JobName: "invalid"},
		})
		if err := dFailVal.ReconcilePendingNomad(ctx); err == nil {
			t.Fatal("expected error aggregation when pending nomad job fails validation, got nil")
		}
	})
}
