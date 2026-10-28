package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

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
							if (cmd == "clone" || cmd == "fetch") && !strings.Contains(strings.Join(args, " "), "--depth 50") {
								t.Errorf("expected --depth 50 in %s call, got %v", cmd, args)
							}
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

func syncNomadConfig(ctx context.Context, d *SyncDaemon, target, configDir string, syncCtx ...ConfigSyncContext) error {
	switch target {
	case "brain":
		return d.SyncBrainConfigToNomad(ctx, configDir, syncCtx...)
	case "homepage":
		return d.SyncHomepageConfigToNomad(ctx, configDir, syncCtx...)
	case "service":
		return d.SyncServiceConfigsToNomad(ctx, configDir, syncCtx...)
	default:
		return errors.New("unknown target: " + target)
	}
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
			err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir)

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
			target            string
			initialFiles      map[string]string
			mkdirs            []string
			modifiedFiles     map[string]string
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
				name:              "brain",
				target:            "brain",
				initialFiles:      map[string]string{"config.yaml": validBrainYAML},
				modifiedFiles:     map[string]string{"config.yaml": "channels:\n  default:\n    mode: always\n"},
				cacheKey:          "nomad/jobs/eri:CONFIG_YAML",
				wantInitialCalls:  1,
				wantModifiedCalls: 1,
				untouchedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}},
				sameContentCtx:    ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}, ExpectedJobs: []string{"eri"}},
				modifiedCtx:       ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"config.yaml"}, ExpectedJobs: []string{"eri"}},
				wantJobName:       "eri",
			},
			{
				name:   "homepage",
				target: "homepage",
				mkdirs: []string{"homepage/subdir"},
				initialFiles: map[string]string{
					"services/homepage/homepage.yaml": "port: \"3001\"\nallowed_hosts: \"*\"\n",
					"homepage/services.yaml":          "- My Group: []\n",
					"homepage/widgets.yaml":           "- search: {}\n",
					"homepage/.hidden":                "ignore me",
					"homepage/temp.tmp":               "ignore me too",
				},
				modifiedFiles: map[string]string{
					"homepage/widgets.yaml": "- search: { updated: true }\n",
				},
				cacheKey:         "nomad/jobs/homepage",
				wantInitialCalls: 1,
				untouchedCtx:     ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}},
				sameContentCtx:   ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/homepage/homepage.yaml"}, ExpectedJobs: []string{"homepage"}},
				modifiedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"homepage/widgets.yaml"}, ExpectedJobs: []string{"homepage"}},
				wantJobName:      "homepage",
			},
			{
				name:   "service",
				target: "service",
				initialFiles: map[string]string{
					"services/voice/voice.yaml":   "model: small.en\n",
					"services/haro/haro.yaml":     "enabled: true\n",
					"services/hangar/hangar.yaml": "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/voice\"\n    var_key: \"CONFIG_YAML\"\n  - rel_path: \"services/haro/haro.yaml\"\n    nomad_var: \"nomad/jobs/haro\"\n    var_key: \"CONFIG_YAML\"\n",
				},
				modifiedFiles: map[string]string{
					"services/voice/voice.yaml": "model: distil-small.en\n",
				},
				cacheKey:         "nomad/jobs/voice:CONFIG_YAML",
				wantInitialCalls: 2,
				untouchedCtx:     ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_untouched", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/hangar/hangar.yaml"}},
				sameContentCtx:   ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_same", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}, ExpectedJobs: []string{"voice"}},
				modifiedCtx:      ConfigSyncContext{Repo: "azylman/aerial-config", CommitSHA: "sha_modified", PRNumber: 101, TargetID: "1555405874565091380", DiffGated: true, ChangedFiles: []string{"services/voice/voice.yaml"}, ExpectedJobs: []string{"voice"}},
				wantJobName:      "voice",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				env := newNomadConfigSyncTestEnv(t)
				for _, dir := range tc.mkdirs {
					if err := os.MkdirAll(filepath.Join(env.configDir, dir), 0o755); err != nil {
						t.Fatal(err)
					}
				}
				for rel, content := range tc.initialFiles {
					full := filepath.Join(env.configDir, rel)
					if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
						t.Fatal(err)
					}
				}

				// (a) initial push
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir); err != nil {
					t.Fatalf("initial push failed: %v", err)
				}
				if len(env.nomadCalls()) != tc.wantInitialCalls {
					t.Fatalf("initial push calls = %d, want %d", len(env.nomadCalls()), tc.wantInitialCalls)
				}

				// (b) cached second push
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir); err != nil || len(env.nomadCalls()) != tc.wantInitialCalls {
					t.Fatalf("cached push calls = %d, want %d", len(env.nomadCalls()), tc.wantInitialCalls)
				}

				// (c) diff-gated untouched file
				callsBeforeUntouched := len(env.nomadCalls())
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir, tc.untouchedCtx); err != nil || len(env.nomadCalls()) != callsBeforeUntouched {
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
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir, tc.sameContentCtx); err != nil || len(env.nomadCalls()) != callsBeforeSame+wantMod {
					t.Fatalf("same content with commitSHA calls = %d, want %d", len(env.nomadCalls()), callsBeforeSame+wantMod)
				}
				sameEvts := env.waitForEvents(t, wantMod)
				if len(sameEvts) == 0 || sameEvts[0].CommitSHA != tc.sameContentCtx.CommitSHA || len(sameEvts[0].ExpectedJobs) != 1 || sameEvts[0].ExpectedJobs[0] != tc.wantJobName {
					t.Fatalf("expected deploy_started with commitSHA %s and expected_jobs [%s], got %+v", tc.sameContentCtx.CommitSHA, tc.wantJobName, sameEvts)
				}
				callsAfterFirstSame := len(env.nomadCalls())
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir, tc.sameContentCtx); err != nil || len(env.nomadCalls()) != callsAfterFirstSame {
					t.Fatalf("repeated same content with same commitSHA calls = %d, want %d", len(env.nomadCalls()), callsAfterFirstSame)
				}
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir); err != nil || len(env.nomadCalls()) != callsAfterFirstSame {
					t.Fatalf("periodic sync after commitSHA push calls = %d, want %d", len(env.nomadCalls()), callsAfterFirstSame)
				}

				// (e) diff-gated modified file
				for rel, content := range tc.modifiedFiles {
					full := filepath.Join(env.configDir, rel)
					if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				callsBeforeMod := len(env.nomadCalls())
				if err := syncNomadConfig(ctx, env.daemon, tc.target, env.configDir, tc.modifiedCtx); err != nil {
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
		var foundKernelMCP bool
		for _, m := range DefaultManagedServiceConfigs {
			if strings.Contains(m.NomadVar, "voice") || strings.Contains(m.NomadVar, "mirrormere") {
				t.Errorf("expected DefaultManagedServiceConfigs to remain generic, found: %v", m)
			}
			if m.NomadVar == "nomad/jobs/kernel-mcp" {
				foundKernelMCP = true
			}
		}
		if !foundKernelMCP {
			t.Errorf("expected DefaultManagedServiceConfigs to contain kernel-mcp mapping")
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

		multiEnv := newNomadConfigSyncTestEnv(t)
		multiEnv.daemon.serviceConfigs = []ServiceConfigMapping{
			{RelPath: "services/multi/config.yaml", NomadVar: "nomad/jobs/multi-job", VarKey: "CONFIG_YAML"},
			{RelPath: "services/multi/extra.yaml", NomadVar: "nomad/jobs/multi-job", VarKey: "EXTRA_YAML"},
		}
		_ = os.MkdirAll(filepath.Join(multiEnv.configDir, "services", "multi"), 0o755)
		_ = os.WriteFile(filepath.Join(multiEnv.configDir, "services", "multi", "config.yaml"), []byte("foo: bar\n"), 0o644)
		_ = os.WriteFile(filepath.Join(multiEnv.configDir, "services", "multi", "extra.yaml"), []byte("baz: qux\n"), 0o644)

		if err := multiEnv.daemon.SyncServiceConfigsToNomad(ctx, multiEnv.configDir); err != nil || len(multiEnv.nomadCalls()) != 1 {
			t.Fatalf("expected grouped multi-key config to sync with 1 call, err=%v, calls=%d", err, len(multiEnv.nomadCalls()))
		}
		initialCall := strings.Join(multiEnv.nomadCalls()[0], " ")
		if !strings.Contains(initialCall, "var put -force nomad/jobs/multi-job") ||
			!strings.Contains(initialCall, "CONFIG_YAML=@") ||
			!strings.Contains(initialCall, "EXTRA_YAML=@") {
			t.Fatalf("unexpected call args for grouped multi-job: %s", initialCall)
		}

		unrelatedCtx := ConfigSyncContext{DiffGated: true, ChangedFiles: []string{"services/other/other.yaml"}, CommitSHA: "sha1"}
		if err := multiEnv.daemon.SyncServiceConfigsToNomad(ctx, multiEnv.configDir, unrelatedCtx); err != nil || len(multiEnv.nomadCalls()) != 1 {
			t.Fatalf("expected diff-gating to suppress sync for unrelated change, calls=%d", len(multiEnv.nomadCalls()))
		}

		jobNomadCtx := ConfigSyncContext{DiffGated: true, ChangedFiles: []string{"jobs/multi-job.nomad"}, CommitSHA: "sha2"}
		if err := multiEnv.daemon.SyncServiceConfigsToNomad(ctx, multiEnv.configDir, jobNomadCtx); err != nil || len(multiEnv.nomadCalls()) != 2 {
			t.Fatalf("expected diff-gating to trigger on jobs/multi-job.nomad, calls=%d", len(multiEnv.nomadCalls()))
		}
	})
}

func TestSyncAssetBundlesToNomad_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		diffGated            bool
		changedFiles         []string
		invalidMirrormere    bool
		wantErr              bool
		wantDocsPushed       bool
		wantGrafanaPushed    bool
		wantDashboardPushed  bool
		wantHaroPushed       bool
		wantMirrormerePushed bool
	}{
		{
			name:                 "full sync without diff gating pushes all bundles and configs",
			diffGated:            false,
			changedFiles:         nil,
			wantDocsPushed:       true,
			wantGrafanaPushed:    true,
			wantDashboardPushed:  true,
			wantHaroPushed:       true,
			wantMirrormerePushed: true,
		},
		{
			name:                 "diff gated with docs changed only",
			diffGated:            true,
			changedFiles:         []string{"services/docs-service/app/index.html"},
			wantDocsPushed:       true,
			wantGrafanaPushed:    false,
			wantDashboardPushed:  false,
			wantHaroPushed:       false,
			wantMirrormerePushed: false,
		},
		{
			name:                 "diff gated with grafana dashboard changed only",
			diffGated:            true,
			changedFiles:         []string{"nomad/grafana/dashboards/sys.json"},
			wantDocsPushed:       false,
			wantGrafanaPushed:    true,
			wantDashboardPushed:  false,
			wantHaroPushed:       false,
			wantMirrormerePushed: false,
		},
		{
			name:                 "diff gated with dashboard static asset changed forwards diff gating",
			diffGated:            true,
			changedFiles:         []string{"services/dashboard/static/app.js"},
			wantDocsPushed:       false,
			wantGrafanaPushed:    false,
			wantDashboardPushed:  true,
			wantHaroPushed:       false,
			wantMirrormerePushed: false,
		},
		{
			name:                 "diff gated with mirrormere widget changed",
			diffGated:            true,
			changedFiles:         []string{"widgets/weather.js"},
			wantDocsPushed:       false,
			wantGrafanaPushed:    false,
			wantDashboardPushed:  false,
			wantHaroPushed:       false,
			wantMirrormerePushed: true,
		},
		{
			name:                 "diff gated with unrelated file changed",
			diffGated:            true,
			changedFiles:         []string{"services/eri/main.go"},
			wantDocsPushed:       false,
			wantGrafanaPushed:    false,
			wantDashboardPushed:  false,
			wantHaroPushed:       false,
			wantMirrormerePushed: false,
		},
		{
			name:                 "invalid mirrormere yaml returns error and skips mirrormere push",
			diffGated:            false,
			changedFiles:         nil,
			invalidMirrormere:    true,
			wantErr:              true,
			wantDocsPushed:       true,
			wantGrafanaPushed:    true,
			wantDashboardPushed:  true,
			wantHaroPushed:       true,
			wantMirrormerePushed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNomadConfigSyncTestEnv(t)
			ctx := context.Background()

			composeDir := filepath.Join(env.configDir, "aerial")
			mirrormereDir := filepath.Join(env.configDir, "mirrormere")
			_ = os.MkdirAll(filepath.Join(composeDir, "services", "dashboard", "static"), 0o755)
			_ = os.WriteFile(filepath.Join(composeDir, "services", "dashboard", "static", "index.html"), []byte("<h1>Dash</h1>"), 0o644)
			_ = os.MkdirAll(filepath.Join(composeDir, "services", "dashboard"), 0o755)
			_ = os.WriteFile(filepath.Join(composeDir, "services", "dashboard", "dashboard.yaml"), []byte("title: Dashboard\n"), 0o644)
			_ = os.MkdirAll(filepath.Join(composeDir, "services", "docs-service", "app"), 0o755)
			_ = os.WriteFile(filepath.Join(composeDir, "services", "docs-service", "app", "index.html"), []byte("<h1>Docs</h1>"), 0o644)
			_ = os.MkdirAll(filepath.Join(composeDir, "nomad", "grafana", "dashboards"), 0o755)
			_ = os.WriteFile(filepath.Join(composeDir, "nomad", "grafana", "dashboards", "sys.json"), []byte("{}"), 0o644)
			_ = os.MkdirAll(filepath.Join(mirrormereDir, "web"), 0o755)
			_ = os.WriteFile(filepath.Join(mirrormereDir, "web", "index.html"), []byte("<h1>Mirrormere</h1>"), 0o644)

			mirrormereContent := []byte("port: 8080\n")
			if tc.invalidMirrormere {
				mirrormereContent = []byte("invalid: [unclosed yaml\n")
			}
			_ = os.MkdirAll(filepath.Join(env.configDir, "services", "mirrormere"), 0o755)
			_ = os.WriteFile(filepath.Join(env.configDir, "services", "mirrormere", "mirrormere.yaml"), mirrormereContent, 0o644)

			_ = os.MkdirAll(filepath.Join(env.configDir, "services", "dashboard"), 0o755)
			_ = os.WriteFile(filepath.Join(env.configDir, "services", "dashboard", "dashboard.yaml"), []byte("title: Dashboard\n"), 0o644)

			_ = os.MkdirAll(filepath.Join(env.configDir, "services", "haro"), 0o755)
			_ = os.WriteFile(filepath.Join(env.configDir, "services", "haro", "haro.yaml"), []byte("log_level: info\n"), 0o644)

			env.daemon.composeDir = composeDir
			env.daemon.repos = []string{composeDir, env.configDir, mirrormereDir}
			env.daemon.serviceConfigs = []ServiceConfigMapping{
				{RelPath: "services/dashboard/dashboard.yaml", NomadVar: "nomad/jobs/dashboard", VarKey: "CONFIG_YAML"},
				{RelPath: "services/haro/haro.yaml", NomadVar: "nomad/jobs/haro", VarKey: "CONFIG_YAML"},
			}

			var syncCtx []ConfigSyncContext
			if tc.diffGated {
				syncCtx = append(syncCtx, ConfigSyncContext{
					DiffGated:    true,
					ChangedFiles: tc.changedFiles,
				})
			}

			err := env.daemon.SyncAssetBundlesToNomad(ctx, env.configDir, syncCtx...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("SyncAssetBundlesToNomad err = %v, wantErr = %v", err, tc.wantErr)
			}

			calls := env.nomadCalls()
			joinedCalls := make([]string, len(calls))
			for i, c := range calls {
				joinedCalls[i] = strings.Join(c, " ")
			}
			allCalls := strings.Join(joinedCalls, " | ")

			docsFound := strings.Contains(allCalls, "var put -force nomad/jobs/docs ASSET_HASH=")
			if docsFound != tc.wantDocsPushed {
				t.Errorf("docs push = %v, want %v (calls: %s)", docsFound, tc.wantDocsPushed, allCalls)
			}

			grafanaFound := strings.Contains(allCalls, "var put -force nomad/jobs/grafana ASSET_HASH=")
			if grafanaFound != tc.wantGrafanaPushed {
				t.Errorf("grafana push = %v, want %v (calls: %s)", grafanaFound, tc.wantGrafanaPushed, allCalls)
			}

			dashboardFound := strings.Contains(allCalls, "var put -force nomad/jobs/dashboard") && strings.Contains(allCalls, "ASSET_HASH=")
			if dashboardFound != tc.wantDashboardPushed {
				t.Errorf("dashboard push = %v, want %v (calls: %s)", dashboardFound, tc.wantDashboardPushed, allCalls)
			}

			haroFound := strings.Contains(allCalls, "var put -force nomad/jobs/haro")
			if haroFound != tc.wantHaroPushed {
				t.Errorf("haro push = %v, want %v (calls: %s)", haroFound, tc.wantHaroPushed, allCalls)
			}

			mirrormereFound := strings.Contains(allCalls, "var put -force nomad/jobs/mirrormere-core") && strings.Contains(allCalls, "ASSET_HASH=")
			if mirrormereFound != tc.wantMirrormerePushed {
				t.Errorf("mirrormere push = %v, want %v (calls: %s)", mirrormereFound, tc.wantMirrormerePushed, allCalls)
			}

			if tc.wantMirrormerePushed {
				var mirrormereCall string
				for _, c := range joinedCalls {
					if strings.Contains(c, "nomad/jobs/mirrormere-core") {
						mirrormereCall = c
						break
					}
				}
				cfgIdx := strings.Index(mirrormereCall, "CONFIG_YAML=@")
				assetIdx := strings.Index(mirrormereCall, "ASSET_HASH=")
				if cfgIdx == -1 || assetIdx == -1 || cfgIdx >= assetIdx {
					t.Errorf("expected CONFIG_YAML to precede ASSET_HASH in mirrormere call: %s", mirrormereCall)
				}
			}
		})
	}
}

func TestIsServiceConfigChangedInGit_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		nomadVar     string
		diffGated    bool
		checkPaths   []string
		changedFiles []string
		wantResult   bool
	}{
		{
			name:         "not diff gated returns true unconditionally",
			nomadVar:     "nomad/jobs/dashboard",
			diffGated:    false,
			checkPaths:   []string{"foo.yaml"},
			changedFiles: nil,
			wantResult:   true,
		},
		{
			name:         "diff gated matches direct check path",
			nomadVar:     "nomad/jobs/dashboard",
			diffGated:    true,
			checkPaths:   []string{"services/dashboard/dashboard.yaml"},
			changedFiles: []string{"services/dashboard/dashboard.yaml"},
			wantResult:   true,
		},
		{
			name:         "diff gated matches dashboard asset bundle change",
			nomadVar:     "nomad/jobs/dashboard",
			diffGated:    true,
			checkPaths:   []string{"services/dashboard/dashboard.yaml"},
			changedFiles: []string{"services/dashboard/static/index.html"},
			wantResult:   true,
		},
		{
			name:         "diff gated matches mirrormere asset bundle change",
			nomadVar:     "nomad/jobs/mirrormere-core",
			diffGated:    true,
			checkPaths:   []string{"services/mirrormere/mirrormere.yaml"},
			changedFiles: []string{"widgets/clock.js"},
			wantResult:   true,
		},
		{
			name:         "diff gated with unrelated file for dashboard returns false",
			nomadVar:     "nomad/jobs/dashboard",
			diffGated:    true,
			checkPaths:   []string{"services/dashboard/dashboard.yaml"},
			changedFiles: []string{"services/eri/main.go"},
			wantResult:   false,
		},
		{
			name:         "diff gated with unrelated file for mirrormere returns false",
			nomadVar:     "nomad/jobs/mirrormere-core",
			diffGated:    true,
			checkPaths:   []string{"services/mirrormere/mirrormere.yaml"},
			changedFiles: []string{"services/eri/main.go"},
			wantResult:   false,
		},
		{
			name:         "diff gated for unknown job returns false",
			nomadVar:     "nomad/jobs/other",
			diffGated:    true,
			checkPaths:   []string{"services/other/config.yaml"},
			changedFiles: []string{"services/eri/main.go"},
			wantResult:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isServiceConfigChangedInGit(tc.nomadVar, tc.diffGated, tc.checkPaths, tc.changedFiles)
			if got != tc.wantResult {
				t.Errorf("isServiceConfigChangedInGit(%s) = %v, want %v", tc.nomadVar, got, tc.wantResult)
			}
		})
	}
}

func TestAppendServiceAssetHash_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	composeDir := filepath.Join(tmpDir, "aerial")
	_ = os.MkdirAll(filepath.Join(composeDir, "services", "dashboard", "static"), 0o755)
	_ = os.WriteFile(filepath.Join(composeDir, "services", "dashboard", "static", "index.html"), []byte("<h1>Dash</h1>"), 0o644)

	d := &SyncDaemon{
		composeDir: composeDir,
		repos:      []string{composeDir},
	}

	tests := []struct {
		name        string
		nomadVar    string
		configDir   string
		initialKeys []string
		initialArgs []string
		wantKeyLen  int
		wantArgLen  int
	}{
		{
			name:        "dashboard job appends asset hash",
			nomadVar:    "nomad/jobs/dashboard",
			configDir:   tmpDir,
			initialKeys: []string{"CONFIG_YAML"},
			initialArgs: []string{"CONFIG_YAML=@path"},
			wantKeyLen:  2,
			wantArgLen:  2,
		},
		{
			name:        "other job does not append asset hash",
			nomadVar:    "nomad/jobs/other",
			configDir:   tmpDir,
			initialKeys: []string{"CONFIG_YAML"},
			initialArgs: []string{"CONFIG_YAML=@path"},
			wantKeyLen:  1,
			wantArgLen:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sb strings.Builder
			gotKeys, gotArgs := d.appendServiceAssetHash(tc.nomadVar, tc.configDir, tc.initialKeys, tc.initialArgs, &sb)
			if len(gotKeys) != tc.wantKeyLen {
				t.Errorf("gotKeys len = %d, want %d", len(gotKeys), tc.wantKeyLen)
			}
			if len(gotArgs) != tc.wantArgLen {
				t.Errorf("gotArgs len = %d, want %d", len(gotArgs), tc.wantArgLen)
			}
		})
	}
}

func TestEnqueuePostSyncReconciliation_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		diffOutput           string
		diffErr              error
		syncCtx              []ConfigSyncContext
		wantServerRestart    bool
		wantPendingNomadJob  string
		wantReconcileTrigger bool
	}{
		{
			name:                 "nomad daemon config change triggers restart and reconcile",
			diffOutput:           "nomad/server.hcl\n",
			diffErr:              nil,
			syncCtx:              nil,
			wantServerRestart:    true,
			wantPendingNomadJob:  "",
			wantReconcileTrigger: true,
		},
		{
			name:       "nomad job change with sync context records changes",
			diffOutput: "M\tnomad/jobs/dashboard.nomad\n",
			diffErr:    nil,
			syncCtx: []ConfigSyncContext{
				{
					Repo:      "azylman/aerial",
					CommitSHA: "abc1234",
					TargetID:  "thread-1",
					PRNumber:  42,
				},
			},
			wantServerRestart:    false,
			wantPendingNomadJob:  "dashboard",
			wantReconcileTrigger: true,
		},
		{
			name:                 "nomad job change with empty sync context uses fallback metadata",
			diffOutput:           "A\tnomad/jobs/bot.nomad\n",
			diffErr:              nil,
			syncCtx:              nil,
			wantServerRestart:    false,
			wantPendingNomadJob:  "bot",
			wantReconcileTrigger: true,
		},
		{
			name:       "skip pending nomad in sync context returns early",
			diffOutput: "M\tnomad/jobs/dashboard.nomad\n",
			diffErr:    nil,
			syncCtx: []ConfigSyncContext{
				{
					SkipPendingNomad: true,
				},
			},
			wantServerRestart:    false,
			wantPendingNomadJob:  "",
			wantReconcileTrigger: false,
		},
		{
			name:                 "git diff error logged safely without panic",
			diffOutput:           "",
			diffErr:              errors.New("git diff error"),
			syncCtx:              nil,
			wantServerRestart:    false,
			wantPendingNomadJob:  "",
			wantReconcileTrigger: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			reconcileCh := make(chan struct{}, 10)
			d := &SyncDaemon{
				gitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					if tc.diffErr != nil {
						return nil, []byte(tc.diffErr.Error()), tc.diffErr
					}
					return []byte(tc.diffOutput), nil, nil
				},
				reconcileCh: reconcileCh,
			}

			d.enqueuePostSyncReconciliation(context.Background(), tmpDir, "sha1", "sha2", tc.syncCtx)

			gotRestart := d.consumePendingNomadServerRestart()
			if gotRestart != tc.wantServerRestart {
				t.Errorf("consumePendingNomadServerRestart() = %v, want %v", gotRestart, tc.wantServerRestart)
			}

			d.pendingNomadMu.Lock()
			hasChanges := len(d.pendingNomadChanges) > 0
			var firstJob string
			if hasChanges && len(d.pendingNomadChanges[0].Changes) > 0 {
				firstJob = d.pendingNomadChanges[0].Changes[0].JobName
			}
			d.pendingNomadMu.Unlock()

			if tc.wantPendingNomadJob != "" {
				if firstJob != tc.wantPendingNomadJob {
					t.Errorf("firstJob = %q, want %q", firstJob, tc.wantPendingNomadJob)
				}
			} else if hasChanges {
				t.Errorf("unexpected pending nomad changes: %+v", d.pendingNomadChanges)
			}

			select {
			case <-reconcileCh:
				if !tc.wantReconcileTrigger {
					t.Errorf("unexpected reconcile trigger")
				}
			default:
				if tc.wantReconcileTrigger {
					t.Errorf("expected reconcile trigger, got none")
				}
			}
		})
	}
}
