package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	seedRepoWorkflowFixture(t, baseDir)
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
			isAerialRepo:    true,
			jobFile:         "synapse.nomad",
			jobContent:      `job "synapse" { group "core" { task "router" { config { image = "ghcr.io/azylman/aerial-synapse:latest" } } } }`,
			clientMode:      "oci_revision",
			initialDigest:   "sha256:9999999999999999999999999999999999999999999999999999999999999999",
			imageRevision:   "9876543210abcdef9876543210abcdef98765432",
			knownDigestKey:  "synapse:ghcr.io/azylman/aerial-synapse:latest",
			knownDigestVal:  "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			wantRestartJob:  "synapse",
			wantEventRepo:   "azylman/aerial",
			wantEventCommit: "9876543210abcdef9876543210abcdef98765432",
			wantDigest:      "sha256:9999999999999999999999999999999999999999999999999999999999999999",
		},
		{
			name:           "parameterized_image_tag_job_skipped",
			isAerialRepo:   true,
			jobFile:        "eri.nomad",
			jobContent:     `variable "image_tag" { type = string default = "latest" } job "eri" { group "core" { task "eri" { config { image = "ghcr.io/test/img1:${var.image_tag}" } } } }`,
			clientMode:     "dynamic_digest",
			initialDigest:  "sha256:9999999999999999999999999999999999999999999999999999999999999999",
			knownDigestKey: "eri:ghcr.io/test/img1:latest",
			knownDigestVal: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
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

			if tc.name == "parameterized_image_tag_job_skipped" {
				if *env.restartCalled || *env.restartedJob != "" {
					t.Fatalf("expected parameterized image_tag job to be skipped without restart, got %q", *env.restartedJob)
				}
				return
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
				var evts []HangarDeployEvent
				for i := 0; i < 2; i++ {
					select {
					case evt := <-env.evtCh:
						evts = append(evts, evt)
					case <-time.After(2 * time.Second):
						t.Fatalf("timed out waiting for deploy events (got %d)", len(evts))
					}
				}
				var hasStarted, hasFailed bool
				for _, evt := range evts {
					if evt.Event == "deploy_started" && evt.Digest == tc.thirdDigest {
						hasStarted = true
					}
					if evt.Event == "deploy_failed" && evt.Status == "failed" && evt.Digest == tc.thirdDigest {
						hasFailed = true
					}
				}
				if !hasStarted {
					t.Errorf("expected deploy_started event for %s with digest %s, got %+v", tc.wantRestartJob, tc.thirdDigest, evts)
				}
				if !hasFailed {
					t.Errorf("expected deploy_failed event for %s with digest %s, got %+v", tc.wantRestartJob, tc.thirdDigest, evts)
				}
				env.daemon.nomadDigestsMu.RLock()
				retainedDigest := env.daemon.nomadKnownDigests["svc:ghcr.io/test/img1:latest"]
				env.daemon.nomadDigestsMu.RUnlock()
				if retainedDigest != tc.secondDigest {
					t.Errorf("expected retained digest %s, got %s", tc.secondDigest, retainedDigest)
				}
			}
		})
	}
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
			{name: "all zeros prev head triggers diff-tree fallback", repo: "/path", prev: "0000000000000000000000000000000000000000", curr: "v2", fallbackOut: "nomad/client.hcl\n", wantChanged: true},
			{name: "all zeros prev head diff-tree error fails safe", repo: "/path", prev: "0000000000000000000000000000000000000000", curr: "v2", fallbackErr: errors.New("diff-tree failed"), wantChanged: false},
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
			{
				name: "all zeros prev head diff-tree succeeds",
				repo: "/repo", prev: "0000000000000000000000000000000000000000", curr: "head2",
				fallbackOut: "M\tjobs/app.nomad\n",
				wantCount:   1,
			},
			{
				name: "all zeros prev head diff-tree fails",
				repo: "/repo", prev: "0000000000000000000000000000000000000000", curr: "head2",
				fallbackErr: errors.New("git err"),
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
			name     string
			target   string
			path     string
			nilExec  bool
			execOut  string
			execErrB string
			execErr  error
			wantErr  bool
		}{
			{name: "config nil executor returns error", target: "config", path: "/tmp/nomad", nilExec: true, wantErr: true},
			{name: "config validation success", target: "config", path: "/tmp/nomad", execOut: "Configuration is valid!\n"},
			{name: "config validation failure", target: "config", path: "/tmp/nomad", execErrB: "syntax error", execErr: errors.New("exit 1"), wantErr: true},
			{name: "job validation success", target: "job", path: jobFile, execOut: "Job validation successful!\n"},
			{name: "job validation failure", target: "job", path: filepath.Join(tmpDir, "invalid.nomad"), execErrB: "syntax error", execErr: errors.New("exit 1"), wantErr: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var nomadExec NomadExecutor
				if !tc.nilExec {
					nomadExec = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
						return []byte(tc.execOut), []byte(tc.execErrB), tc.execErr
					}
				}
				d := &SyncDaemon{nomadExecutor: nomadExec}
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
			name    string
			valErr  error
			wantErr bool
		}{
			{
				name:    "validation failure returns error",
				valErr:  errors.New("val fail"),
				wantErr: true,
			},
			{
				name:    "validation success",
				wantErr: false,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				d := &SyncDaemon{
					nomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
						if tc.valErr != nil {
							return nil, []byte("syntax error"), tc.valErr
						}
						return []byte("valid"), nil, nil
					},
				}
				err := d.reconcileNomadServer(context.Background(), tmpDir)
				if (err != nil) != tc.wantErr {
					t.Fatalf("reconcileNomadServer() err=%v (wantErr:%v)", err, tc.wantErr)
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
		_ = os.WriteFile(filepath.Join(jobsDir, "typechange.nomad"), []byte(`job "typechange" {}`), 0o644)
		_ = os.WriteFile(filepath.Join(jobsDir, "typechange_stopfail.nomad"), []byte(`job "typechange_stopfail" {}`), 0o644)
		_ = os.WriteFile(filepath.Join(jobsDir, "typechange_retryfail.nomad"), []byte(`job "typechange_retryfail" {}`), 0o644)
		_ = os.WriteFile(filepath.Join(jobsDir, "db-migrate.nomad"), []byte(`job "db-migrate" { type = "batch" }`), 0o644)

		var executedCalls [][]string
		purgedJobs := make(map[string]bool)
		var mu sync.Mutex
		dReconcile := newNomadReconcileDaemon(func(cfg *DaemonConfig) {
			cfg.NomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
				mu.Lock()
				executedCalls = append(executedCalls, args)
				if len(args) >= 3 && args[0] == "job" && args[1] == "validate" {
					mu.Unlock()
					if strings.Contains(args[2], "invalid.nomad") {
						return nil, []byte("invalid HCL"), errors.New("val error")
					}
					return []byte("valid"), nil, nil
				}
				if len(args) >= 4 && args[0] == "job" && args[1] == "run" {
					target := args[len(args)-1]
					if (strings.Contains(target, "typechange") || strings.Contains(target, "db-migrate.nomad")) && !purgedJobs[filepath.Base(target)] {
						mu.Unlock()
						return nil, []byte(`Error submitting job: Unexpected response code: 500 (cannot update job from type "service" to "system")`), errors.New("exit status 1")
					}
					mu.Unlock()
					if strings.Contains(target, "runfail.nomad") || strings.Contains(target, "typechange_retryfail.nomad") {
						return []byte("fail"), []byte("alloc failed"), errors.New("run error")
					}
					return []byte("Evaluation ID: 12345"), nil, nil
				}
				if len(args) >= 4 && args[0] == "job" && args[1] == "stop" {
					jobID := args[3]
					if jobID == "stop-fail" || jobID == "typechange_stopfail" || (jobID == "db-migrate" && purgedJobs["db-migrate.nomad"]) {
						mu.Unlock()
						return nil, []byte("stop fail"), errors.New("stop error")
					}
					purgedJobs[jobID+".nomad"] = true
					mu.Unlock()
					return []byte("stopped"), nil, nil
				}
				mu.Unlock()
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
				name: "apply change scheduler type transition purges and retries",
				changes: []NomadFileChange{
					{JobName: "typechange", Path: "jobs/typechange.nomad", Action: "apply"},
				},
			},
			{
				name: "apply change scheduler type transition stop failure",
				changes: []NomadFileChange{
					{JobName: "typechange_stopfail", Path: "jobs/typechange_stopfail.nomad", Action: "apply"},
				},
				wantErr: true,
			},
			{
				name: "apply change scheduler type transition retry failure",
				changes: []NomadFileChange{
					{JobName: "typechange_retryfail", Path: "jobs/typechange_retryfail.nomad", Action: "apply"},
				},
				wantErr: true,
			},
			{
				name: "db-migrate purges dead batch job before running",
				changes: []NomadFileChange{
					{JobName: "db-migrate", Path: "jobs/db-migrate.nomad", Action: "apply"},
				},
			},
			{
				name: "db-migrate pre-run purge notice continues to run",
				changes: []NomadFileChange{
					{JobName: "db-migrate", Path: "jobs/db-migrate.nomad", Action: "apply"},
				},
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

		reconcileCases := []struct {
			name        string
			changes     []NomadFileChange
			wantStopped []string
		}{
			{
				name: "defined in other repo skips stop",
				changes: []NomadFileChange{
					{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
				},
				wantStopped: nil,
			},
			{
				name: "truly deleted job stops",
				changes: []NomadFileChange{
					{Path: "jobs/truly_deleted.nomad", Action: "delete", JobName: "truly_deleted"},
				},
				wantStopped: []string{"truly_deleted"},
			},
		}

		for _, tc := range reconcileCases {
			t.Run(tc.name, func(t *testing.T) {
				mu.Lock()
				stoppedJobs = nil
				mu.Unlock()
				if err := dCross.ReconcileNomadChanges(ctx, repoA, tc.changes); err != nil {
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

		teardownCases := []struct {
			name        string
			events      []NomadChangeEvent
			wantStopped []string
		}{
			{
				name: "teardowns filters out cross repo defined jobs",
				events: []NomadChangeEvent{
					{
						RepoPath: repoA,
						Changes: []NomadFileChange{
							{Path: "jobs/brain.nomad", Action: "delete", JobName: "brain"},
							{Path: "jobs/old_orphan.nomad", Action: "delete", JobName: "old_orphan"},
						},
					},
				},
				wantStopped: []string{"old_orphan"},
			},
		}

		for _, tc := range teardownCases {
			t.Run(tc.name, func(t *testing.T) {
				mu.Lock()
				stoppedJobs = nil
				mu.Unlock()
				dCross.ExecuteNomadTeardowns(ctx, tc.events)
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

func TestSleepWithContext_TableDriven(t *testing.T) {
	t.Parallel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name       string
		ctx        context.Context
		duration   time.Duration
		wantResult bool
	}{
		{
			name:       "zero duration with active context",
			ctx:        context.Background(),
			duration:   0,
			wantResult: true,
		},
		{
			name:       "negative duration with active context",
			ctx:        context.Background(),
			duration:   -time.Second,
			wantResult: true,
		},
		{
			name:       "zero duration with canceled context",
			ctx:        canceledCtx,
			duration:   0,
			wantResult: false,
		},
		{
			name:       "positive duration with canceled context returns false immediately",
			ctx:        canceledCtx,
			duration:   5 * time.Second,
			wantResult: false,
		},
		{
			name:       "short positive duration with active context elapses",
			ctx:        context.Background(),
			duration:   time.Millisecond,
			wantResult: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sleepWithContext(tc.ctx, tc.duration)
			if got != tc.wantResult {
				t.Errorf("sleepWithContext() = %v, want %v", got, tc.wantResult)
			}
		})
	}
}
