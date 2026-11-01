package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	seedRepoWorkflowFixture(t, aerialDir)
	seedRepoWorkflowFixture(t, mirrormereDir)

	repos := []string{aerialDir, aerialConfigDir, mirrormereDir}
	evtCh := make(chan HangarDeployEvent, 50)

	cfg := DaemonConfig{
		Repos:          repos,
		ComposeDir:     aerialDir,
		ConfigDir:      aerialConfigDir,
		ServiceConfigs: testServiceConfigs(),
		AssetBundles:   testAssetBundles(aerialDir, aerialConfigDir, mirrormereDir),
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
	for range count {
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
			for part := range strings.SplitSeq(s.failCmd, ",") {
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

func (e *pushEventTestEnv) setupCase(tc gitPushCase) {
	if tc.nomadAddr != "" {
		e.daemon.nomadAddr = tc.nomadAddr
	}
	if tc.serviceFiles {
		_ = os.MkdirAll(filepath.Join(e.aerialConfigDir, "services", "voice"), 0o755)
		_ = os.MkdirAll(filepath.Join(e.aerialConfigDir, "services", "hangar"), 0o755)
		_ = os.WriteFile(filepath.Join(e.aerialConfigDir, "services", "voice", "voice.yaml"), []byte("model: small.en\n"), 0o644)
		hangarContent := "service_configs:\n  - rel_path: \"services/voice/voice.yaml\"\n    nomad_var: \"nomad/jobs/voice\"\n    var_key: \"CONFIG_YAML\"\n"
		_ = os.WriteFile(filepath.Join(e.aerialConfigDir, "services", "hangar", "hangar.yaml"), []byte(hangarContent), 0o644)
	}
	targetDir := e.aerialDir
	if strings.Contains(tc.req.Repo, "aerial-config") {
		targetDir = e.aerialConfigDir
	}
	for p, content := range tc.jobFiles {
		fullP := filepath.Join(targetDir, p)
		_ = os.MkdirAll(filepath.Dir(fullP), 0o755)
		_ = os.WriteFile(fullP, []byte(content), 0o644)
	}
	e.daemon.nomadExecutor = func(ctx context.Context, args ...string) ([]byte, []byte, error) {
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
			e.mu.Lock()
			e.appliedPaths = append(e.appliedPaths, args[len(args)-1])
			e.mu.Unlock()
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
				e.mu.Lock()
				e.varPutJobs = append(e.varPutJobs, args[3])
				e.mu.Unlock()
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
		e.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`))}, nil
		}}}
	case "digest_ready":
		e.daemon.registryClient = &http.Client{Transport: &roundTripperFunc{fn: func(req *http.Request) (*http.Response, error) {
			resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser(strings.NewReader(`{}`))}
			resp.Header.Set("Docker-Content-Digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			return resp, nil
		}}}
	}
	if tc.gitScript != (mockGitScript{}) {
		e.setGitScript(tc.gitScript)
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
	jobWithAerialImg := `job "synapse" {
		task "router" {
			config {
				image = "ghcr.io/azylman/aerial-synapse:latest"
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
			wantExpectedJobs: []string{"voice"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deferred nomad job when image missing in registry",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/synapse.nomad\n"},
			jobFiles:         map[string]string{"jobs/synapse.nomad": jobWithAerialImg},
			registryMode:     "not_found",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main"},
			wantCode:         http.StatusOK,
			wantBuilding:     true,
			wantNomadChanged: true,
			wantExpectedJobs: []string{"synapse"},
		},
		{
			name:             "image ready applies nomad job and dispatches deploy_started",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/synapse.nomad\n"},
			jobFiles:         map[string]string{"jobs/synapse.nomad": jobWithAerialImg},
			registryMode:     "digest_ready",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "commit_abc123", PRNumber: 255, TargetID: "1555422677936644147"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantApplied:      []string{"synapse"},
			wantExpectedJobs: []string{"synapse"},
			wantEvents:       []string{"sync_success", "deploy_started"},
		},
		{
			name:             "deploy failed dispatches deploy_failed",
			gitScript:        mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "M jobs/synapse.nomad\n"},
			jobFiles:         map[string]string{"jobs/synapse.nomad": `job "synapse" {}`},
			nomadFailCmd:     "run",
			req:              GitPushEventRequest{Repo: "aerial-config", Ref: "refs/heads/main", Commit: "fail_commit_123"},
			wantCode:         http.StatusOK,
			wantNomadChanged: true,
			wantEvents:       []string{"sync_success", "deploy_started", "deploy_failed"},
		},
		{
			name:      "complex aerial-config diff",
			gitScript: mockGitScript{beforeSHA: "sha_before", afterSHA: "sha_after", nameStatus: "D jobs/deleted.nomad\nM jobs/deferred.nomad\nM jobs/invalid.nomad\nM jobs/nonexistent.nomad\nM jobs/broken.nomad\nM jobs/orphan.vars.hcl\nM jobs/valid.vars.hcl\n"},
			jobFiles: map[string]string{
				"jobs/invalid.nomad":  `job "invalid" {}`,
				"jobs/broken.nomad":   `job "broken" {}`,
				"jobs/valid.nomad":    `job "valid" {}`,
				"jobs/valid.vars.hcl": `extra_mounts = []`,
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
	stoppedJobs []string
	stopMu      sync.Mutex
	valErr      error
	runErr      error
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

func (e *imageReadyTestEnv) getRunCalls() []string {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return append([]string(nil), e.runCalls...)
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
					image = "ghcr.io/azylman/aerial-synapse:${var.image_tag}"
				}
			}
		}
	}`

	type imageReadyCase struct {
		name                 string
		jobDir               string
		jobFile              string
		jobHCL               string
		varFile              string
		req                  ImageReadyEventRequest
		valErr               error
		runErr               error
		registryNotFound     bool
		removeJobOnRegistry  bool
		pendingDeletes       []NomadFileChange
		wantCode             int
		wantStatus           string
		wantRuns             int
		wantRunArg           string
		wantEvent            string
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
			wantEvent:  "deploy_started",
		},
		{
			name:    "commit SHA variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				Digest:    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:      "azylman/aerial",
				CommitSHA: "sha12345",
				PRNumber:  615,
				TargetID:  "discord_thread_999",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRunArg: "job run -detach -var=image_tag=sha12345 ",
		},
		{
			name:             "unpushed commit SHA fails registry preflight",
			jobFile:          "router.nomad",
			jobHCL:           routerHCL,
			registryNotFound: true,
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "unpushed_sha_44d3d1f",
				Repo:      "azylman/aerial",
			},
			wantCode:   http.StatusInternalServerError,
			wantStatus: "error",
			wantRuns:   0,
		},
		{
			name:    "digest fallback variable injection",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-synapse:latest",
				Digest:   "@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:             "empty sha and digest fallback to latest",
			jobFile:          "router.nomad",
			jobHCL:           routerHCL,
			registryNotFound: true,
			req: ImageReadyEventRequest{
				Image:    "ghcr.io/azylman/aerial-synapse:latest",
				Repo:     "azylman/aerial",
				PRNumber: 615,
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRunArg: "-var=image_tag=latest",
		},
		{
			name:    "digest prefix normalization",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/aerial-synapse:latest",
				Digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				Repo:   "azylman/aerial",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRunArg: "-var=image_tag=latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
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
		},
		{
			name:    "core composeDir matching",
			jobDir:  "core",
			jobFile: "core.nomad",
			jobHCL:  `job "core-job" { task "t" { config { image = "ghcr.io/azylman/core-service:latest" } } }`,
			req: ImageReadyEventRequest{
				Image:  "ghcr.io/azylman/core-service:latest",
				Digest: "sha256:core123456",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
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
		{
			name:    "var file and commit SHA variable injection",
			jobDir:  "core",
			jobFile: "router.nomad",
			jobHCL:  routerHCL,
			varFile: "router.vars.hcl",
			req: ImageReadyEventRequest{
				Image:     "ghcr.io/azylman/aerial-synapse:latest",
				CommitSHA: "sha_varfile",
			},
			wantCode:   http.StatusOK,
			wantStatus: "accepted",
			wantRuns:   1,
			wantRunArg: "-var-file=",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newImageReadyTestEnv(t)
			env.valErr = tc.valErr
			env.runErr = tc.runErr

			var jobPath string
			if tc.jobFile != "" {
				dir := tc.jobDir
				if dir == "" {
					dir = "jobs"
				}
				jobPath = env.writeJob(dir, tc.jobFile, tc.jobHCL)
			}
			if tc.varFile != "" {
				env.writeJob("jobs", tc.varFile, "extra_mounts = []\n")
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
				env.daemon.recordPendingNomadChanges(env.tmpDir, tc.pendingDeletes, tc.req.Repo, tc.req.CommitSHA, tc.req.TargetID, strconv.Itoa(tc.req.PRNumber))
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
		loopCtx := t.Context()

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
		for i := range 2 {
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
		loopCtx := t.Context()

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
		env.writeJob("jobs", "synapse.nomad", `job "synapse" { task "r" { config { image = "ghcr.io/azylman/aerial-synapse:latest" } } }`)

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
		d.pendingImageRollouts["synapse"] = PendingImageRollout{
			JobName: "synapse",
			Request: ImageReadyEventRequest{
				Image: "ghcr.io/azylman/aerial-synapse:latest",
			},
		}
		d.pendingImageMu.Unlock()

		d.executePendingImageRollouts(context.Background())

		mu.Lock()
		defer mu.Unlock()
		if len(executedJobs) != 2 {
			t.Fatalf("expected 2 executed jobs, got %v", executedJobs)
		}
		if executedJobs[0] != "synapse" || executedJobs[1] != "hangar" {
			t.Fatalf("expected synapse first and hangar last, got %v", executedJobs)
		}
	})
}

func TestExecuteGitPushEvent_AssetBundles_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		repoName            string
		changedFiles        []string
		wantContainersBuild bool
		wantStatus          string
	}{
		{
			name:                "aerial core push with static asset change only",
			repoName:            "aerial",
			changedFiles:        []string{"services/dashboard/static/app.js"},
			wantContainersBuild: false,
			wantStatus:          "accepted",
		},
		{
			name:                "aerial core push with docs service app static change only",
			repoName:            "aerial",
			changedFiles:        []string{"services/docs-service/app/index.html"},
			wantContainersBuild: false,
			wantStatus:          "accepted",
		},
		{
			name:                "mirrormere push triggers asset bundle sync",
			repoName:            "mirrormere",
			changedFiles:        []string{"web/index.html"},
			wantContainersBuild: false,
			wantStatus:          "accepted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			aerialDir := filepath.Join(tmpDir, "aerial")
			configDir := filepath.Join(tmpDir, "aerial-config")
			mirrormereDir := filepath.Join(tmpDir, "mirrormere")

			_ = os.MkdirAll(filepath.Join(aerialDir, ".git"), 0o755)
			_ = os.MkdirAll(filepath.Join(configDir, ".git"), 0o755)
			_ = os.MkdirAll(filepath.Join(mirrormereDir, ".git"), 0o755)
			_ = os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte("channels:\n  default: {}\n"), 0o644)

			var pushedVars []string
			var mu sync.Mutex
			var revCalls int

			d := NewDaemon(DaemonConfig{
				Repos:      []string{aerialDir, configDir, mirrormereDir},
				ComposeDir: aerialDir,
				ConfigDir:  configDir,
				GitExecutor: func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
					if len(args) > 0 && args[0] == "diff" {
						return []byte(strings.Join(tc.changedFiles, "\n")), nil, nil
					}
					if len(args) > 1 && args[0] == "rev-parse" {
						mu.Lock()
						revCalls++
						c := revCalls
						mu.Unlock()
						if c == 1 {
							return []byte("sha_before\n"), nil, nil
						}
						return []byte("sha_after\n"), nil, nil
					}
					return []byte(""), nil, nil
				},
				NomadExecutor: func(ctx context.Context, args ...string) ([]byte, []byte, error) {
					mu.Lock()
					pushedVars = append(pushedVars, strings.Join(args, " "))
					mu.Unlock()
					return []byte("ok"), nil, nil
				},
				NomadAddr: "http://127.0.0.1:4646",
			})

			ctx := context.Background()
			resp, code := d.ExecuteGitPushEvent(ctx, GitPushEventRequest{
				Repo: tc.repoName,
				Ref:  "refs/heads/main",
			})

			if code != http.StatusOK {
				t.Fatalf("ExecuteGitPushEvent() code = %d, want 200 (msg: %s)", code, resp.Message)
			}
			if resp.Status != tc.wantStatus {
				t.Errorf("ExecuteGitPushEvent() status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if resp.ContainersBuilding != tc.wantContainersBuild {
				t.Errorf("ExecuteGitPushEvent() containersBuilding = %v, want %v", resp.ContainersBuilding, tc.wantContainersBuild)
			}
		})
	}
}

func TestIsAllZeros_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantResult bool
	}{
		{"empty string", "", false},
		{"single zero", "0", true},
		{"sha of all zeros", "0000000000000000000000000000000000000000", true},
		{"mixed digits with trailing one", "0000000000000000000000000000000000000001", false},
		{"letters", "abcdef", false},
		{"starts with zero but has letter", "000a000", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isAllZeros(tc.input)
			if got != tc.wantResult {
				t.Errorf("isAllZeros(%q) = %v, want %v", tc.input, got, tc.wantResult)
			}
		})
	}
}
