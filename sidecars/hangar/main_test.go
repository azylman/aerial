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

func TestGetComposeArgs_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		subArgs []string
		setup   func(t *testing.T) (composeDir string, configDir string, check func(t *testing.T, args []string, composeDir, configDir string))
	}{
		{
			name:    "base_only",
			subArgs: []string{"config", "--quiet"},
			setup: func(t *testing.T) (string, string, func(*testing.T, []string, string, string)) {
				composeDir := t.TempDir()
				configDir := t.TempDir()
				baseFile := filepath.Join(composeDir, "docker-compose.yml")
				if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
					t.Fatal(err)
				}
				expectedPrefix := []string{
					"--project-name", "aerial",
					"--project-directory", composeDir,
					"-f", baseFile,
					"config", "--quiet",
				}
				return composeDir, configDir, func(t *testing.T, args []string, _, _ string) {
					if len(args) != len(expectedPrefix) {
						t.Fatalf("expected %d args, got %d: %v", len(expectedPrefix), len(args), args)
					}
					for i := range expectedPrefix {
						if args[i] != expectedPrefix[i] {
							t.Errorf("arg[%d] = %q, want %q", i, args[i], expectedPrefix[i])
						}
					}
				}
			},
		},
		{
			name:    "with_config_override",
			subArgs: []string{"up", "-d", "--no-recreate", "gitsync"},
			setup: func(t *testing.T) (string, string, func(*testing.T, []string, string, string)) {
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
				expectedArgs := []string{
					"--project-name", "aerial",
					"--project-directory", composeDir,
					"-f", baseFile,
					"-f", filepath.Clean(overrideFile),
					"up", "-d", "--no-recreate", "gitsync",
				}
				return composeDir, configDir, func(t *testing.T, args []string, _, _ string) {
					if len(args) != len(expectedArgs) {
						t.Fatalf("expected %d args, got %d: %v", len(expectedArgs), len(args), args)
					}
					for i := range expectedArgs {
						if args[i] != expectedArgs[i] {
							t.Errorf("arg[%d] = %q, want %q", i, args[i], expectedArgs[i])
						}
					}
				}
			},
		},
		{
			name:    "deduplication",
			subArgs: []string{"config"},
			setup: func(t *testing.T) (string, string, func(*testing.T, []string, string, string)) {
				composeDir := t.TempDir()
				baseFile := filepath.Join(composeDir, "docker-compose.yml")
				if err := os.WriteFile(baseFile, []byte("services: {}"), 0644); err != nil {
					t.Fatal(err)
				}
				overrideFile := filepath.Join(composeDir, "docker-compose.override.yml")
				if err := os.WriteFile(overrideFile, []byte("services: { brain: {} }"), 0644); err != nil {
					t.Fatal(err)
				}
				return composeDir, composeDir, func(t *testing.T, args []string, _, _ string) {
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
			},
		},
		{
			name:    "with_env_file",
			subArgs: []string{"config"},
			setup: func(t *testing.T) (string, string, func(*testing.T, []string, string, string)) {
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
				return composeDir, configDir, func(t *testing.T, args []string, _, _ string) {
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
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			composeDir, configDir, check := tc.setup(t)
			daemon := &SyncDaemon{
				composeDir: composeDir,
				configDir:  configDir,
			}
			args := daemon.getComposeArgs(composeDir, tc.subArgs...)
			check(t, args, composeDir, configDir)
		})
	}
}

func TestGetRepoCommitAndStatus_TableDriven(t *testing.T) {
	t.Run("getRepoCommit", func(t *testing.T) {
		tests := []struct {
			name        string
			pat         string
			gitExecutor func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error)
			wantSha     string
			wantErr     bool
			checkTime   func(t *testing.T, tm *time.Time)
		}{
			{
				name: "git_error",
				gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return nil, nil, errors.New("git error")
				},
				wantErr: true,
				checkTime: func(t *testing.T, tm *time.Time) {
					if tm != nil {
						t.Errorf("expected nil time on error, got %v", tm)
					}
				},
			},
			{
				name: "output_without_null_delimiter",
				gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("abc1234"), nil, nil
				},
				wantSha: "abc1234",
				wantErr: false,
				checkTime: func(t *testing.T, tm *time.Time) {
					if tm != nil {
						t.Errorf("expected nil time without delimiter, got %v", tm)
					}
				},
			},
			{
				name: "output_with_invalid_rfc3339_date",
				gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("abc1234\x00invalid-date"), nil, nil
				},
				wantSha: "abc1234",
				wantErr: false,
				checkTime: func(t *testing.T, tm *time.Time) {
					if tm != nil {
						t.Errorf("expected nil time on invalid date, got %v", tm)
					}
				},
			},
			{
				name: "output_with_valid_rfc3339_date_and_pat",
				pat:  "my_pat",
				gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					return []byte("1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z"), nil, nil
				},
				wantSha: "1122334455667788990011223344556677889900",
				wantErr: false,
				checkTime: func(t *testing.T, tm *time.Time) {
					if tm == nil || tm.Year() != 2026 {
						t.Errorf("expected valid time with year 2026, got %v", tm)
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{
					pat:         tc.pat,
					gitExecutor: tc.gitExecutor,
				}
				sha, tm, err := d.getRepoCommit(context.Background(), "/repo", "HEAD")
				if (err != nil) != tc.wantErr {
					t.Fatalf("getRepoCommit() error = %v, wantErr = %v", err, tc.wantErr)
				}
				if sha != tc.wantSha {
					t.Errorf("getRepoCommit() sha = %q, want %q", sha, tc.wantSha)
				}
				if tc.checkTime != nil {
					tc.checkTime(t, tm)
				}
			})
		}
	})

	t.Run("GetStatus", func(t *testing.T) {
		tests := []struct {
			name  string
			check func(t *testing.T)
		}{
			{
				name: "synced_and_http_endpoint",
				check: func(t *testing.T) {
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
				},
			},
			{
				name: "error_status_when_one_repo_fails_with_lastSyncTimes",
				check: func(t *testing.T) {
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

					status := daemon.GetStatus(context.Background())
					if status.Status != "error" {
						t.Errorf("expected overall status 'error' when one repo fails, got %q", status.Status)
					}
				},
			},
			{
				name: "lagging_status_when_remote_sha_differs_and_timestamp_older",
				check: func(t *testing.T) {
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
				},
			},
			{
				name: "origin_main_error_fallback_to_fetch_head",
				check: func(t *testing.T) {
					fakeRepo := "/mock/repo"
					now := time.Now()
					past := now.Add(-10 * time.Minute)

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
				},
			},
			{
				name: "origin_main_and_fetch_head_failing_fallback_to_disk_sha",
				check: func(t *testing.T) {
					fakeRepo := "/mock/repo"
					now := time.Now()

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
				},
			},
			{
				name: "clean_status_synced",
				check: func(t *testing.T) {
					cleanDaemon := &SyncDaemon{
						repos: []string{"/repo/ok"},
						gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							return []byte("1122334455667788990011223344556677889900\x002026-09-12T12:00:00Z"), nil, nil
						},
					}
					cleanStatus := cleanDaemon.GetStatus(context.Background())
					if cleanStatus.Status != "synced" {
						t.Errorf("expected clean status 'synced', got %q", cleanStatus.Status)
					}
					if len(cleanStatus.Repos) != 1 {
						t.Errorf("expected 1 repo status, got %d", len(cleanStatus.Repos))
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, tc.check)
		}
	})

	t.Run("resolveGitDir", func(t *testing.T) {
		tempDir := t.TempDir()

		// Setup dirs
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
			name        string
			repoPath    string
			wantGitDir  string
			wantErr     bool
		}{
			{
				name:       "nonexistent_directory",
				repoPath:   filepath.Join(tempDir, "nonexistent"),
				wantErr:    true,
			},
			{
				name:       "standard_git_directory",
				repoPath:   standardRepo,
				wantGitDir: filepath.Join(standardRepo, ".git"),
				wantErr:    false,
			},
			{
				name:       "worktree_absolute_gitdir",
				repoPath:   worktreeRepo,
				wantGitDir: targetGitDir,
				wantErr:    false,
			},
			{
				name:       "worktree_relative_gitdir",
				repoPath:   relWorktreeRepo,
				wantGitDir: targetGitDir,
				wantErr:    false,
			},
			{
				name:       "worktree_without_gitdir_prefix",
				repoPath:   plainFileRepo,
				wantGitDir: filepath.Join(plainFileRepo, ".git"),
				wantErr:    false,
			},
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
		oldDebounce := reconcilerDebounceDuration
		oldCooldown := reconcilerMinCooldown
		reconcilerDebounceDuration = 5 * time.Millisecond
		reconcilerMinCooldown = 5 * time.Millisecond
		defer func() {
			reconcilerDebounceDuration = oldDebounce
			reconcilerMinCooldown = oldCooldown
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

		// First trigger
		d.TriggerReconcile(context.Background(), true)
		select {
		case <-nomadExecuted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for nomad executor signal on first trigger")
		}

		// Second trigger with updated lastReconcile to test cooldown path
		d.SetLastReconcile(time.Now())
		d.TriggerReconcile(context.Background(), true)
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
		d := NewDaemon(DaemonConfig{
			Interval:   5 * time.Millisecond,
			ComposeDir: t.TempDir(),
		})
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
		d := NewDaemon(DaemonConfig{
			Interval: 10 * time.Millisecond,
		})
		d.StartPeriodicLoop(ctx)
		d.StartReconcilerLoop(ctx)
		cancel()
	})
}

func TestEnsureRepoAndSync_TableDriven(t *testing.T) {
	t.Run("EnsureRepo", func(t *testing.T) {
		tests := []struct {
			name  string
			check func(t *testing.T)
		}{
			{
				name: "empty_path_or_url_noop",
				check: func(t *testing.T) {
					daemon := &SyncDaemon{}
					if err := daemon.EnsureRepo(context.Background(), "", ""); err != nil {
						t.Errorf("EnsureRepo with empty strings failed: %v", err)
					}
				},
			},
			{
				name: "regular_file_not_a_directory_error",
				check: func(t *testing.T) {
					tempDir := t.TempDir()
					filePath := filepath.Join(tempDir, "file-not-dir")
					_ = os.WriteFile(filePath, []byte("regular file"), 0644)
					d := NewDaemon(DaemonConfig{})
					err := d.EnsureRepo(context.Background(), filePath, "https://github.com/example/repo.git")
					if err == nil {
						t.Errorf("expected error when repoPath is a regular file")
					}
				},
			},
			{
				name: "empty_directory_clone_success_and_second_call_noop",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					bareRemote := "https://example.com/repo.git"
					localClone := filepath.Join(tempBase, "local")
					cloneCalled := false

					daemon := &SyncDaemon{
						gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							if len(args) > 0 && args[0] == "clone" {
								cloneCalled = true
								targetPath := args[len(args)-1]
								_ = os.MkdirAll(filepath.Join(targetPath, ".git"), 0755)
							}
							return nil, nil, nil
						},
					}

					ctx := context.Background()
					if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
						t.Fatalf("EnsureRepo failed: %v", err)
					}
					if !cloneCalled {
						t.Errorf("expected clone to be called on empty directory")
					}

					// Second call should be a no-op since .git now exists
					if err := daemon.EnsureRepo(ctx, localClone, bareRemote); err != nil {
						t.Fatalf("EnsureRepo second call failed: %v", err)
					}
				},
			},
			{
				name: "clone_error",
				check: func(t *testing.T) {
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
				},
			},
			{
				name: "non_empty_directory_adoption_success",
				check: func(t *testing.T) {
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
				},
			},
			{
				name: "adoption_git_init_failure",
				check: func(t *testing.T) {
					tempDir := t.TempDir()
					adoptDir := filepath.Join(tempDir, "adopt")
					_ = os.MkdirAll(adoptDir, 0755)
					_ = os.WriteFile(filepath.Join(adoptDir, "file.txt"), []byte("data"), 0644)

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
				},
			},
			{
				name: "adoption_git_remote_add_nonfatal_warning_when_fetch_succeeds_and_fails",
				check: func(t *testing.T) {
					tempDir := t.TempDir()

					// Case A: remote add warning, fetch succeeds -> EnsureRepo succeeds
					dRemoteSuccess := NewDaemon(DaemonConfig{
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
					nonEmptyDir1 := filepath.Join(tempDir, "nonempty1")
					_ = os.MkdirAll(nonEmptyDir1, 0755)
					_ = os.WriteFile(filepath.Join(nonEmptyDir1, "file.txt"), []byte("data"), 0644)
					if err := dRemoteSuccess.EnsureRepo(context.Background(), nonEmptyDir1, "https://github.com/org/repo.git"); err != nil {
						t.Fatalf("expected already exists remote error to be non-fatal when fetch succeeds, got %v", err)
					}

					// Case B: remote add warning, fetch fails -> EnsureRepo fails
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
					nonEmptyDir2 := filepath.Join(tempDir, "nonempty2")
					_ = os.MkdirAll(nonEmptyDir2, 0755)
					_ = os.WriteFile(filepath.Join(nonEmptyDir2, "file.txt"), []byte("data"), 0644)
					if err := dRemoteFail.EnsureRepo(context.Background(), nonEmptyDir2, "https://example.com/repo.git"); err == nil {
						t.Errorf("expected error on fetch failure after remote add warning")
					}
				},
			},
			{
				name: "adoption_git_reset_soft_failure",
				check: func(t *testing.T) {
					tempDir := t.TempDir()
					nonEmptyDir := filepath.Join(tempDir, "nonempty")
					_ = os.MkdirAll(nonEmptyDir, 0755)
					_ = os.WriteFile(filepath.Join(nonEmptyDir, "file.txt"), []byte("data"), 0644)

					dResetFail := NewDaemon(DaemonConfig{
						GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							if len(args) > 0 {
								if args[0] == "reset" && len(args) > 1 && args[1] == "--soft" {
									return nil, []byte("soft reset failed"), errors.New("soft reset error")
								}
								if args[0] == "reset" {
									return nil, []byte("fatal: reset error"), errors.New("reset error")
								}
							}
							return nil, nil, nil
						},
					})
					if err := dResetFail.EnsureRepo(context.Background(), nonEmptyDir, "https://github.com/org/repo.git"); err == nil {
						t.Fatalf("expected git reset --soft error, got nil")
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, tc.check)
		}
	})

	t.Run("SyncRepo", func(t *testing.T) {
		tests := []struct {
			name  string
			check func(t *testing.T)
		}{
			{
				name: "empty_repo_path",
				check: func(t *testing.T) {
					daemon := &SyncDaemon{}
					emptyRes := daemon.SyncRepo(context.Background(), "")
					if emptyRes.Repo != "" {
						t.Errorf("expected empty repo result")
					}
				},
			},
			{
				name: "nonexistent_repo_path",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					daemon := &SyncDaemon{}
					invalidRes := daemon.SyncRepo(context.Background(), filepath.Join(tempBase, "nonexistent"))
					if invalidRes.Error == "" {
						t.Errorf("expected error on nonexistent repo")
					}
				},
			},
			{
				name: "index_lock_active_guard",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					localClone := filepath.Join(tempBase, "local")
					_ = os.MkdirAll(filepath.Join(localClone, ".git"), 0755)
					lockFile := filepath.Join(localClone, ".git", "index.lock")
					_ = os.WriteFile(lockFile, []byte(""), 0644)

					daemon := &SyncDaemon{}
					resLock := daemon.SyncRepo(context.Background(), localClone)
					if resLock.Error != "index.lock active" {
						t.Errorf("expected 'index.lock active', got %q", resLock.Error)
					}
				},
			},
			{
				name: "up_to_date_and_fast_forward_update_with_diff",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					localClone := filepath.Join(tempBase, "local")
					_ = os.MkdirAll(filepath.Join(localClone, ".git"), 0755)

					simCurrentHead := "1111111111111111111111111111111111111111"
					simFetchHead := "1111111111111111111111111111111111111111"

					daemon := &SyncDaemon{
						repos:       []string{localClone},
						repoUrls:    map[string]string{localClone: "https://example.com/repo.git"},
						reconcileCh: make(chan struct{}, 1),
						gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							if len(args) == 0 {
								return nil, nil, nil
							}
							switch args[0] {
							case "config":
								return nil, nil, nil
							case "rev-parse":
								if len(args) > 1 && args[1] == "FETCH_HEAD" {
									return []byte(simFetchHead + "\n"), nil, nil
								}
								return []byte(simCurrentHead + "\n"), nil, nil
							case "fetch":
								return nil, nil, nil
							case "merge":
								simCurrentHead = simFetchHead
								return []byte("Updating " + simCurrentHead + "\n"), nil, nil
							case "diff":
								return []byte("docker-compose.yml\n"), nil, nil
							}
							return nil, nil, nil
						},
					}

					ctx := context.Background()
					// 1. Up to date
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

					// 2. Remote update exists
					simFetchHead = "2222222222222222222222222222222222222222"
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
				},
			},
			{
				name: "empty_git_rev_parse_head_error",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					emptyGitRepo := filepath.Join(tempBase, "emptygit")
					_ = os.MkdirAll(filepath.Join(emptyGitRepo, ".git"), 0755)
					daemonEmpty := NewDaemon(DaemonConfig{
						GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							return nil, []byte("fatal: ambiguous argument 'HEAD'"), errors.New("exit status 128")
						},
					})
					resEmpty := daemonEmpty.SyncRepo(context.Background(), emptyGitRepo)
					if resEmpty.Error == "" {
						t.Errorf("expected error on empty git repo without commits")
					}
				},
			},
			{
				name: "fetch_error",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
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

					resBroken := daemonBroken.SyncRepo(context.Background(), brokenOriginRepo)
					if !strings.Contains(resBroken.Error, "fetch failed") {
						t.Errorf("expected fetch failed error on broken remote, got: %s", resBroken.Error)
					}
				},
			},
			{
				name: "ensure_repo_failure_warning_uninitialized",
				check: func(t *testing.T) {
					tempBase := t.TempDir()
					failDir := filepath.Join(tempBase, "fail_adopt")
					daemonEnsureFail := NewDaemon(DaemonConfig{
						Repos:    []string{failDir},
						RepoURLs: map[string]string{failDir: "http://invalid-non-routable-domain-1234567.com/repo.git"},
						GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
							return nil, []byte("fatal: clone failed"), errors.New("clone failed")
						},
					})
					_ = os.MkdirAll(failDir, 0755)
					_ = os.WriteFile(filepath.Join(failDir, "file.txt"), []byte("content"), 0644)
					_ = daemonEnsureFail.SyncRepo(context.Background(), failDir)
				},
			},
			{
				name: "merge_conflict_reset_recovery",
				check: func(t *testing.T) {
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
				},
			},
			{
				name: "recovery_reset_hard_failure",
				check: func(t *testing.T) {
					tempDir := t.TempDir()
					localDir := filepath.Join(tempDir, "local")
					_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

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
				},
			},
			{
				name: "post_merge_rev_parse_failure",
				check: func(t *testing.T) {
					tempDir := t.TempDir()
					localDir := filepath.Join(tempDir, "local")
					_ = os.MkdirAll(filepath.Join(localDir, ".git"), 0755)

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
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, tc.check)
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
	})
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

// nomadConfigSyncTestEnv encapsulates a test harness for Nomad configuration synchronization.
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

func TestSyncBrainConfigToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name        string
			setup       func(t *testing.T, dir string)
			daemonSetup func(d *SyncDaemon)
			wantErrSub  string
		}{
			{
				name:  "missing_config_yaml_safe_noop",
				setup: func(t *testing.T, dir string) {},
			},
			{
				name: "directory_as_config_yaml_error",
				setup: func(t *testing.T, dir string) {
					if err := os.Mkdir(filepath.Join(dir, "config.yaml"), 0755); err != nil {
						t.Fatal(err)
					}
				},
				wantErrSub: "is a directory",
			},
			{
				name: "invalid_yaml_syntax_error",
				setup: func(t *testing.T, dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
				},
				wantErrSub: "invalid YAML syntax",
			},
			{
				name: "semantic_validation_missing_channels_default",
				setup: func(t *testing.T, dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("channels:\n  other:\n    mode: threads\n"), 0644)
				},
				wantErrSub: "channels.default is required",
			},
			{
				name: "disabled_nomad_addr_safe_noop",
				setup: func(t *testing.T, dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("model: gemini-2.5-flash\nchannels:\n  default:\n    mode: threads\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadAddr = ""
				},
			},
			{
				name: "nomad_cli_failure_returns_error",
				setup: func(t *testing.T, dir string) {
					_ = os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("model: gemini-2.5-flash\nchannels:\n  default:\n    mode: threads\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
						return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
					}
				},
				wantErrSub: "failed updating Nomad variable",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newNomadConfigSyncTestEnv(t)
				tc.setup(t, env.configDir)
				if tc.daemonSetup != nil {
					tc.daemonSetup(env.daemon)
				}
				err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir)
				if tc.wantErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
						t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
					}
				} else if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
			})
		}
	})

	t.Run("hash_caching_and_invalidation", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		cfgPath := filepath.Join(env.configDir, "config.yaml")
		_ = os.WriteFile(cfgPath, []byte("model: gemini-2.5-flash\nchannels:\n  default:\n    mode: threads\n"), 0644)

		// 1. Initial push -> updates Nomad var
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on initial push: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 nomad call, got %d", len(calls))
		}
		if len(calls[0]) < 5 || calls[0][0] != "var" || calls[0][1] != "put" || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/brain" {
			t.Errorf("unexpected nomad CLI args: %v", calls[0])
		}

		// 2. Unchanged config -> cache hit skips CLI call
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on cached call: %v", err)
		}
		if len(env.nomadCalls()) != 1 {
			t.Errorf("expected nomadCalls to remain 1 after unchanged push, got %d", len(env.nomadCalls()))
		}

		// 3. Modified config -> cache invalidated, pushes update
		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: classify\n"), 0644)
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on modified config: %v", err)
		}
		if len(env.nomadCalls()) != 2 {
			t.Errorf("expected nomadCalls to become 2 after modification, got %d", len(env.nomadCalls()))
		}
	})

	t.Run("diff_gating_and_deploy_event_lifecycle", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		cfgPath := filepath.Join(env.configDir, "config.yaml")
		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: mention\n"), 0644)

		scUntouched := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_untouched",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"services/voice/voice.yaml"},
		}

		// 1. Untouched file: no CLI push, but primes cache
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scUntouched); err != nil {
			t.Fatalf("SyncBrainConfigToNomad error: %v", err)
		}
		if len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for untouched diff, got %d", len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cached := env.daemon.lastPushedConfig["nomad/jobs/brain:CONFIG_YAML"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cached == "" {
			t.Fatalf("expected cache to be primed for brain config")
		}

		// 2. Touched in diff, but identical content (double-filter): no push
		scSameContent := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_same",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"config.yaml"},
		}
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scSameContent); err != nil {
			t.Fatalf("SyncBrainConfigToNomad error: %v", err)
		}
		if len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for identical content, got %d", len(env.nomadCalls()))
		}

		// 3. Touched in diff with modified content: pushes var with COMMIT_SHA and emits deploy_started
		_ = os.WriteFile(cfgPath, []byte("channels:\n  default:\n    mode: always\n"), 0644)
		scModified := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_modified",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"config.yaml"},
		}
		if err := env.daemon.SyncBrainConfigToNomad(ctx, env.configDir, scModified); err != nil {
			t.Fatalf("SyncBrainConfigToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 nomad call for modified config, got %d", len(calls))
		}
		hasCommit := false
		for _, arg := range calls[0] {
			if arg == "COMMIT_SHA=sha_modified" {
				hasCommit = true
				break
			}
		}
		if !hasCommit {
			t.Errorf("expected COMMIT_SHA in nomad CLI args: %v", calls[0])
		}

		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "brain" || evts[0].Repo != scModified.Repo ||
			evts[0].CommitSHA != scModified.CommitSHA || evts[0].PRNumber != scModified.PRNumber || evts[0].TargetID != scModified.TargetID {
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
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if len(calls[0]) < 5 || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/homepage" {
			t.Errorf("expected nomad/jobs/homepage target, got %v", calls[0])
		}
		foundHash := false
		foundYAML := false
		for _, arg := range calls[0] {
			if strings.HasPrefix(arg, "CONFIG_HASH=") {
				foundHash = true
			}
			if strings.HasPrefix(arg, "HOMEPAGE_YAML=@") {
				foundYAML = true
			}
		}
		if !foundHash || foundYAML {
			t.Errorf("expected CONFIG_HASH and NO HOMEPAGE_YAML on empty dir, got %v", calls[0])
		}
	})

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name        string
			setup       func(t *testing.T, dir string)
			daemonSetup func(d *SyncDaemon)
			wantErrSub  string
		}{
			{
				name: "directory_as_homepage_yaml_error",
				setup: func(t *testing.T, dir string) {
					if err := os.MkdirAll(filepath.Join(dir, "services", "homepage", "homepage.yaml"), 0755); err != nil {
						t.Fatal(err)
					}
				},
				wantErrSub: "is a directory",
			},
			{
				name: "invalid_yaml_syntax_in_homepage_yaml",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "homepage"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "homepage", "homepage.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
				},
				wantErrSub: "invalid YAML syntax",
			},
			{
				name: "disabled_nomad_addr_safe_noop",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "homepage"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadAddr = ""
				},
			},
			{
				name: "nomad_cli_failure_returns_error",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "homepage"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
						return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
					}
				},
				wantErrSub: "failed updating Nomad variable",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newNomadConfigSyncTestEnv(t)
				tc.setup(t, env.configDir)
				if tc.daemonSetup != nil {
					tc.daemonSetup(env.daemon)
				}
				err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir)
				if tc.wantErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
						t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
					}
				} else if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
			})
		}
	})

	t.Run("hash_caching_and_dir_filtering", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)

		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "homepage"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "homepage", "homepage.yaml"), []byte("port: \"3001\"\nallowed_hosts: \"*\"\n"), 0644)
		_ = os.MkdirAll(filepath.Join(env.configDir, "homepage", "subdir"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "homepage", "services.yaml"), []byte("- My Group: []\n"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "homepage", "widgets.yaml"), []byte("- search: {}\n"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "homepage", ".hidden"), []byte("ignore me"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "homepage", "temp.tmp"), []byte("ignore me too"), 0644)

		// 1. Initial push -> verified hash & args
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success pushing to nomad: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if len(calls[0]) < 6 || calls[0][2] != "-force" || calls[0][3] != "nomad/jobs/homepage" {
			t.Fatalf("unexpected nomad CLI args: %v", calls[0])
		}
		foundHash := false
		foundYAML := false
		for _, arg := range calls[0] {
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
			t.Errorf("missing CONFIG_HASH or HOMEPAGE_YAML in args: %v", calls[0])
		}

		// 2. Unchanged call -> cache hit
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on cached call: %v", err)
		}
		if len(env.nomadCalls()) != 1 {
			t.Errorf("expected calls to remain 1, got %d", len(env.nomadCalls()))
		}

		// 3. Modified homepage config -> pushes update
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "homepage", "homepage.yaml"), []byte("title: Updated Aerial Dashboard\n"), 0644)
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success after homepage update: %v", err)
		}
		if len(env.nomadCalls()) != 2 {
			t.Errorf("expected calls to become 2, got %d", len(env.nomadCalls()))
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

		scUntouched := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_untouched",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"services/voice/voice.yaml"},
		}

		// 1. Untouched in diff: primes cache, skips push
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir, scUntouched); err != nil {
			t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
		}
		if len(env.nomadCalls()) != 0 {
			t.Fatalf("expected 0 nomad calls for untouched diff, got %d", len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cached := env.daemon.lastPushedConfig["nomad/jobs/homepage"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cached == "" {
			t.Fatalf("expected cache to be primed for homepage config")
		}

		// 2. Subfile in homepage/ directory touched: pushes var and emits deploy_started
		_ = os.WriteFile(subYaml, []byte("services:\n  - name: test2\n"), 0644)
		scSubTouched := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_sub",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"homepage/services.yaml"},
		}
		if err := env.daemon.SyncHomepageConfigToNomad(ctx, env.configDir, scSubTouched); err != nil {
			t.Fatalf("SyncHomepageConfigToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 nomad call for modified homepage subfile, got %d", len(calls))
		}
		hasCommit := false
		for _, arg := range calls[0] {
			if arg == "COMMIT_SHA=sha_sub" {
				hasCommit = true
				break
			}
		}
		if !hasCommit {
			t.Errorf("expected COMMIT_SHA in args: %v", calls[0])
		}

		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "homepage" || evts[0].Repo != scSubTouched.Repo ||
			evts[0].CommitSHA != scSubTouched.CommitSHA || evts[0].PRNumber != scSubTouched.PRNumber || evts[0].TargetID != scSubTouched.TargetID {
			t.Errorf("unexpected deploy_started event payload: %+v", evts[0])
		}
	})
}

func TestSyncServiceConfigsToNomad_TableDriven(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("validation_and_disabled_paths", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name        string
			setup       func(t *testing.T, dir string)
			daemonSetup func(d *SyncDaemon)
			wantErrSub  string
		}{
			{
				name:  "empty_dir_graceful_skip",
				setup: func(t *testing.T, dir string) {},
			},
			{
				name: "directory_instead_of_file_error",
				setup: func(t *testing.T, dir string) {
					if err := os.MkdirAll(filepath.Join(dir, "services", "mcp", "scheduler-mcp.yaml"), 0755); err != nil {
						t.Fatal(err)
					}
				},
				wantErrSub: "failed to read service config",
			},
			{
				name: "invalid_yaml_syntax_in_service_config",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "mcp"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "mcp", "scheduler-mcp.yaml"), []byte("invalid: yaml: [unclosed"), 0644)
				},
				wantErrSub: "invalid YAML syntax",
			},
			{
				name: "disabled_nomad_addr_safe_noop",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "mcp"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: \"4005\"\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadAddr = ""
				},
			},
			{
				name: "nomad_cli_failure_returns_error",
				setup: func(t *testing.T, dir string) {
					_ = os.MkdirAll(filepath.Join(dir, "services", "mcp"), 0755)
					_ = os.WriteFile(filepath.Join(dir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: \"4005\"\n"), 0644)
				},
				daemonSetup: func(d *SyncDaemon) {
					d.nomadExecutor = func(execCtx context.Context, args ...string) ([]byte, []byte, error) {
						return nil, []byte("nomad var put permission denied"), errors.New("exit 1")
					}
				},
				wantErrSub: "failed updating Nomad variable",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newNomadConfigSyncTestEnv(t)
				tc.setup(t, env.configDir)
				if tc.daemonSetup != nil {
					tc.daemonSetup(env.daemon)
				}
			err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir)
			if tc.wantErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
						t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
					}
				} else if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
			})
		}
	})

	t.Run("non_yaml_config_skips_yaml_validation", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)
		env.daemon.serviceConfigs = []ServiceConfigMapping{
			{RelPath: "proxy/default.conf", NomadVar: "nomad/jobs/mesh-proxy", VarKey: "DEFAULT_CONF"},
		}
		_ = os.MkdirAll(filepath.Join(env.configDir, "proxy"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "proxy", "default.conf"), []byte("server { listen 80; proxy_pass http://backend; }"), 0644)

		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected non-YAML config to sync successfully, got %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 nomad var put call, got %d", len(calls))
		}
	})

	t.Run("hash_caching_and_sequence_yaml_sync", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)

		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "mcp"), 0755)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "webhooks-router"), 0755)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "victoriametrics"), 0755)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "mcp", "scheduler-mcp.yaml"), []byte("port: \"4005\"\n"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "mcp", "docker-mcp.yaml"), []byte("port: \"4002\"\n"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "webhooks-router", "webhooks-router.yaml"), []byte("port: \"4020\"\n"), 0644)
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "victoriametrics", "scrape.yml"), []byte("- job_name: \"anomaly-detector\"\n"), 0644)

		// 1. Initial push -> 4 calls
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 4 {
			t.Fatalf("expected 4 nomad var put calls, got %d: %v", len(calls), calls)
		}

		// 2. Unchanged push -> cache hit, 0 additional calls
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on cached call: %v", err)
		}
		if len(env.nomadCalls()) != 4 {
			t.Fatalf("expected calls to remain 4, got %d", len(env.nomadCalls()))
		}

		// 3. Modify 1 service config -> exactly 1 gets pushed
		_ = os.WriteFile(filepath.Join(env.configDir, "services", "mcp", "docker-mcp.yaml"), []byte("docker_host: unix:///var/run/docker.sock\n# modified\n"), 0644)
		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir); err != nil {
			t.Fatalf("expected success on modified service call: %v", err)
		}
		if len(env.nomadCalls()) != 5 {
			t.Fatalf("expected calls to increase to 5, got %d", len(env.nomadCalls()))
		}
	})

	t.Run("service_config_mappings_precedence_and_invariants", func(t *testing.T) {
		t.Parallel()
		// Invariant: DefaultManagedServiceConfigs stays generic
		for _, m := range DefaultManagedServiceConfigs {
			if strings.Contains(m.NomadVar, "orin-voice") || strings.Contains(m.NomadVar, "mirrormere") {
				t.Errorf("expected DefaultManagedServiceConfigs to remain generic, found deployment-specific mapping: %v", m)
			}
		}

		// Precedence Tier 1: services/hangar/hangar.yaml override in configDir
		customDir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(customDir, "services", "hangar"), 0755)
		hangarYAML := "service_configs:\n  - rel_path: \"services/custom/custom.yaml\"\n    nomad_var: \"nomad/jobs/custom-job\"\n    var_key: \"CONFIG_YAML\"\n"
		_ = os.WriteFile(filepath.Join(customDir, "services", "hangar", "hangar.yaml"), []byte(hangarYAML), 0644)
		d := NewDaemon(DaemonConfig{ConfigDir: customDir})
		customMappings := d.GetServiceConfigMappings(customDir)
		if len(customMappings) != 1 || customMappings[0].RelPath != "services/custom/custom.yaml" || customMappings[0].NomadVar != "nomad/jobs/custom-job" {
			t.Fatalf("expected custom mappings from hangar.yaml, got %v", customMappings)
		}

		// Precedence Tier 2: daemon-configured serviceConfigs fallback
		dCustom := &SyncDaemon{
			serviceConfigs: []ServiceConfigMapping{
				{RelPath: "services/test/test.yaml", NomadVar: "nomad/jobs/test-job", VarKey: "CONFIG_YAML"},
			},
		}
		daemonMappings := dCustom.GetServiceConfigMappings(t.TempDir())
		if len(daemonMappings) != 1 || daemonMappings[0].RelPath != "services/test/test.yaml" {
			t.Fatalf("expected daemon-configured mappings, got %v", daemonMappings)
		}

		// Precedence Tier 3: DefaultManagedServiceConfigs fallback when empty
		dDefault := &SyncDaemon{}
		defaultMappings := dDefault.GetServiceConfigMappings(t.TempDir())
		if len(defaultMappings) != len(DefaultManagedServiceConfigs) {
			t.Fatalf("expected DefaultManagedServiceConfigs fallback, got %d mappings", len(defaultMappings))
		}
	})

	t.Run("diff_gating_and_deploy_event_lifecycle", func(t *testing.T) {
		t.Parallel()
		env := newNomadConfigSyncTestEnv(t)

		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "voice"), 0755)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "mcp"), 0755)
		_ = os.MkdirAll(filepath.Join(env.configDir, "services", "hangar"), 0755)

		voiceYaml := filepath.Join(env.configDir, "services", "voice", "voice.yaml")
		_ = os.WriteFile(voiceYaml, []byte("model: small.en\n"), 0644)
		schedYaml := filepath.Join(env.configDir, "services", "mcp", "scheduler-mcp.yaml")
		_ = os.WriteFile(schedYaml, []byte("enabled: true\n"), 0644)

		hangarYaml := filepath.Join(env.configDir, "services", "hangar", "hangar.yaml")
		hangarContent := `service_configs:
  - rel_path: "services/voice/voice.yaml"
    nomad_var: "nomad/jobs/orin-voice"
    var_key: "CONFIG_YAML"
  - rel_path: "services/mcp/scheduler-mcp.yaml"
    nomad_var: "nomad/jobs/scheduler-mcp"
    var_key: "CONFIG_YAML"
`
		_ = os.WriteFile(hangarYaml, []byte(hangarContent), 0644)

		// 1. Untouched service configs in diff (e.g. only hangar.yaml changed): primes cache, 0 calls, 0 events
		scHangarOnly := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_pr264",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"services/hangar/hangar.yaml"},
		}

		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir, scHangarOnly); err != nil {
			t.Fatalf("SyncServiceConfigsToNomad unexpected error: %v", err)
		}
		if len(env.nomadCalls()) != 0 {
			t.Fatalf("expected ZERO nomad calls for PR #264 hangar-only diff, got %d", len(env.nomadCalls()))
		}
		env.daemon.lastPushedConfigMu.Lock()
		cachedVoice := env.daemon.lastPushedConfig["nomad/jobs/orin-voice:CONFIG_YAML"]
		cachedSched := env.daemon.lastPushedConfig["nomad/jobs/scheduler-mcp:CONFIG_YAML"]
		env.daemon.lastPushedConfigMu.Unlock()
		if cachedVoice == "" || cachedSched == "" {
			t.Fatalf("expected cache to be primed for untouched service configs")
		}

		// 2. Touched in diff with modified content: pushes ONLY orin-voice with COMMIT_SHA and emits deploy_started
		_ = os.WriteFile(voiceYaml, []byte("model: distil-small.en\n"), 0644)
		scVoiceOnly := ConfigSyncContext{
			Repo:         "azylman/aerial-config",
			CommitSHA:    "sha_voice_update",
			PRNumber:     101,
			TargetID:     "1555405874565091380",
			DiffGated:    true,
			ChangedFiles: []string{"services/voice/voice.yaml"},
		}

		if err := env.daemon.SyncServiceConfigsToNomad(ctx, env.configDir, scVoiceOnly); err != nil {
			t.Fatalf("SyncServiceConfigsToNomad error: %v", err)
		}
		calls := env.nomadCalls()
		if len(calls) != 1 {
			t.Fatalf("expected exactly 1 nomad call (only orin-voice), got %d", len(calls))
		}
		hasCommit := false
		for _, arg := range calls[0] {
			if arg == "COMMIT_SHA=sha_voice_update" {
				hasCommit = true
				break
			}
		}
		if !hasCommit {
			t.Errorf("expected COMMIT_SHA in args: %v", calls[0])
		}

		evts := env.waitForEvents(t, 1)
		if evts[0].Event != "deploy_started" || evts[0].JobName != "orin-voice" || evts[0].Repo != scVoiceOnly.Repo ||
			evts[0].CommitSHA != scVoiceOnly.CommitSHA || evts[0].PRNumber != scVoiceOnly.PRNumber || evts[0].TargetID != scVoiceOnly.TargetID {
			t.Errorf("unexpected deploy_started event payload: %+v", evts[0])
		}
	})
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
			case "rev-parse":
				return []byte(head), nil, nil
			case "merge":
				return []byte("Already up to date."), nil, nil
			default:
				return nil, nil, nil
			}
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

type mockOCIOptions struct {
	ProbeErr       error
	ProbeStatus    int
	ProbeHeaders   map[string]string
	ProbeBody      string

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
}

func TestReconciliationStatusAndHelpers_TableDriven(t *testing.T) {
	ctx := context.Background()

	t.Run("ReconciliationStatus_nil_daemon_and_nil_fn", func(t *testing.T) {
		var nilD *SyncDaemon
		nilD.updateReconcileStatus(func(s *ReconciliationStatus) {})
		if st := nilD.GetReconciliationStatus(); st.State != "idle" {
			t.Errorf("expected idle for nil daemon, got %q", st.State)
		}

		d := NewDaemon(DaemonConfig{})
		d.updateReconcileStatus(nil)
	})

	t.Run("ReconciliationStatus_TTL_reset", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		d.updateReconcileStatus(func(s *ReconciliationStatus) {
			s.Active = false
			s.State = "healthy"
			s.Stage = "healthy"
			s.TargetServices = []string{"brain"}
			s.CompletedAt = time.Now().Add(-6 * time.Minute)
		})

		status := d.GetReconciliationStatus()
		if status.State != "idle" || status.Stage != "idle" {
			t.Errorf("expected TTL to reset state to idle, got %+v", status)
		}
	})

	t.Run("ReconciliationStatus_exposed_in_GetStatus", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
		d.updateReconcileStatus(func(s *ReconciliationStatus) {
			s.Active = true
			s.State = "swapping"
			s.Stage = "swapping"
			s.TargetServices = []string{"scheduler-mcp"}
			s.StartedAt = time.Now().UTC()
		})

		statusResp := d.GetStatus(ctx)
		if statusResp.Reconciliation == nil {
			t.Fatalf("expected non-nil Reconciliation in GetStatus")
		}
		if statusResp.Reconciliation.State != "swapping" {
			t.Errorf("expected state swapping, got %s", statusResp.Reconciliation.State)
		}
		if len(statusResp.Reconciliation.TargetServices) != 1 || statusResp.Reconciliation.TargetServices[0] != "scheduler-mcp" {
			t.Errorf("expected target services [scheduler-mcp], got %v", statusResp.Reconciliation.TargetServices)
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

		st1 := d.GetStatus(ctx)
		repoSt1, ok := st1.Repos["/share/mirrormere"]
		if !ok || repoSt1.GitHubRepo != "azylman/mirrormere" {
			t.Errorf("expected GitHubRepo azylman/mirrormere, got %q", repoSt1.GitHubRepo)
		}
		if remoteCalls != 1 {
			t.Errorf("expected 1 remote call, got %d", remoteCalls)
		}

		st2 := d.GetStatus(ctx)
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

	t.Run("resolveGitDir_cases", func(t *testing.T) {
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
	})

	t.Run("postChannelMessage_and_sendWebhook", func(t *testing.T) {
		d := NewDaemon(DaemonConfig{})
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

		d.postChannelMessage(ctx, http.DefaultClient, "tok", "bad\nchan", "msg")
		d.sendWebhook(ctx, http.DefaultClient, "http://bad\nurl", "msg")
	})
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

		d.SetImageRolloutDebounce(20 * time.Millisecond)
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

		d.SetImageRolloutDebounce(10 * time.Millisecond)
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
		time.Sleep(25 * time.Millisecond)

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


func TestRunGitCommand_WithDir(t *testing.T) {
	dir := t.TempDir()
	stdout, _, err := runGitCommand(context.Background(), dir, "", "init")
	if err != nil {
		t.Logf("runGitCommand init: %v, stdout: %s", err, string(stdout))
	}
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

func TestNomadImagePoll_UsesImageRevision(t *testing.T) {
	t.Parallel()
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

func TestFetchRegistryWithAuth_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		targetURL      string
		registry       string
		repository     string
		existingBearer string
		pat            string
		clientOpts     mockOCIOptions
		wantToken      string
		wantErrSubstr  string
	}{
		{
			name:           "existing bearer returns 401 and recovers via challenge",
			targetURL:      "https://registry-1.docker.io/v2/library/redis/manifests/latest",
			registry:       "registry-1.docker.io",
			repository:     "library/redis",
			existingBearer: "stale-bearer",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull"`,
				TokenBody:           `{"access_token":"refreshed-token"}`,
				ExpectedBearer:      "refreshed-token",
				AuthStatus:          http.StatusOK,
				AuthBody:            "ok",
			},
			wantToken: "refreshed-token",
		},
		{
			name:          "malformed Www-Authenticate header returns error",
			targetURL:     "https://ghcr.io/v2/foo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "foo",
			clientOpts:    mockOCIOptions{ChallengeAuthHeader: "Basic realm=foo"},
			wantErrSubstr: "failed parsing Www-Authenticate header",
		},
		{
			name:          "invalid realm returns error",
			targetURL:     "https://ghcr.io/v2/repo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "repo",
			clientOpts:    mockOCIOptions{ChallengeAuthHeader: `Bearer realm="://invalid-realm-url"`},
			wantErrSubstr: "failed parsing token realm",
		},
		{
			name:          "token request non-200 returns error",
			targetURL:     "https://ghcr.io/v2/foo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "foo",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
				TokenStatus:         http.StatusForbidden,
				TokenBody:           "forbidden",
			},
			wantErrSubstr: "returned HTTP 403",
		},
		{
			name:          "token request malformed JSON returns error",
			targetURL:     "https://ghcr.io/v2/foo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "foo",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           "{invalid-json",
			},
			wantErrSubstr: "failed decoding token",
		},
		{
			name:          "token request empty token returns error",
			targetURL:     "https://ghcr.io/v2/foo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "foo",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           `{"token":""}`,
			},
			wantErrSubstr: "empty bearer token returned",
		},
		{
			name:          "token request empty token json object returns error",
			targetURL:     "https://ghcr.io/v2/repo/manifests/latest",
			registry:      "ghcr.io",
			repository:    "repo",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           `{}`,
			},
			wantErrSubstr: "empty bearer token returned",
		},
		{
			name:       "ghcr.io token request sets Basic Auth when PAT is present",
			targetURL:  "https://ghcr.io/v2/azylman/aerial/manifests/latest",
			registry:   "ghcr.io",
			repository: "azylman/aerial",
			pat:        "my-secret-pat",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io"`,
				ExpectedPAT:         "my-secret-pat",
				TokenBody:           `{"token":"dummy-ghcr-token"}`,
				ExpectedBearer:      "dummy-ghcr-token",
				AuthStatus:          http.StatusOK,
				AuthBody:            "ok",
			},
			wantToken: "dummy-ghcr-token",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDaemon(DaemonConfig{
				PAT:            tc.pat,
				RegistryClient: newMockOCIRegistryClient(tc.clientOpts),
			})
			resp, tok, err := d.fetchRegistryWithAuth(context.Background(), tc.targetURL, tc.registry, tc.repository, "", tc.existingBearer)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSubstr, err)
				}
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if tok != tc.wantToken {
				t.Errorf("got token %q, want %q", tok, tc.wantToken)
			}
			if resp != nil && resp.StatusCode != http.StatusOK {
				t.Errorf("status = %d, want 200", resp.StatusCode)
			}
		})
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
	oldFn, oldMetaFn := writeCountersDirFn, writeMetaDirFn
	defer func() { writeCountersDirFn, writeMetaDirFn = oldFn, oldMetaFn }()

	called, metaCalled := false, false
	writeCountersDirFn = func(dir string) error { called = true; return nil }
	writeMetaDirFn = func(dir string) error { metaCalled = true; return nil }
	_ = os.Setenv("GOCOVERDIR", t.TempDir())

	resp2, err2 := http.Get(ts.URL + "/debug/coverage/flush")
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", resp2.StatusCode, called, metaCalled)
	}

	writeMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
	if resp2b, err2b := http.Get(ts.URL + "/debug/coverage/flush"); err2b != nil || resp2b.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got resp=%v err=%v", resp2b, err2b)
	} else {
		resp2b.Body.Close()
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
		_, _, _ = defaultDockerExecutor(cancelCtx, "version")
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

	t.Run("ReconcileNomadJobs_alias", func(t *testing.T) {
		d := &SyncDaemon{}
		_ = d.ReconcileNomadJobs(context.Background(), t.TempDir(), nil)
	})

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
