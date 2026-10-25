package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/hangar/pkg/metrics"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v3"
)

func writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("[Hangar:HTTP] Failed to marshal JSON response: %v", err)
		http.Error(w, `{"error":"Internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	httpkit.WriteResponse(w, r, "hangar", b)
}

// SanitizeLog scrubs sensitive tokens from error and log messages.
func SanitizeLog(input string) string {
	return sanitizer.SanitizeLog(input)
}

// RepoSyncResult holds telemetry for a single repository sync operation.
type RepoSyncResult struct {
	Repo         string `json:"repo"`
	PreviousHead string `json:"previous_head"`
	CurrentHead  string `json:"current_head"`
	Changed      bool   `json:"changed"`
	Error        string `json:"error,omitempty"`
}

// RepoStatus holds git commit and timestamp metadata for an individual repository.
type RepoStatus struct {
	Repo             string     `json:"repo"`
	GitHubRepo       string     `json:"github_repo,omitempty"`
	DiskCommit       string     `json:"disk_commit"`
	DiskCommitTime   *time.Time `json:"disk_commit_time,omitempty"`
	RemoteCommit     string     `json:"remote_commit"`
	RemoteCommitTime *time.Time `json:"remote_commit_time,omitempty"`
	TimeLagSeconds   int64      `json:"time_lag_seconds"`
	SyncStatus       string     `json:"sync_status"` // "synced", "lagging", "error"
	LastSyncTime     time.Time  `json:"last_sync_time"`
	Error            string     `json:"error,omitempty"`
}

// ReconciliationStatus captures in-memory state of active and recent reconciliations.
type ReconciliationStatus struct {
	Active         bool      `json:"active"`
	State          string    `json:"state"`                     // "idle", "healthy", "failed"
	Stage          string    `json:"stage"`                     // alias to State for backward compatibility
	Trigger        string    `json:"trigger,omitempty"`         // "image_poll", "git_sync", "manual_api"
	TargetServices []string  `json:"target_services,omitempty"` // services planned/targeted for reconciliation
	CommitSHA      string    `json:"commit_sha,omitempty"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// ConfigSyncContext holds context for configuration sync events.
type ConfigSyncContext struct {
	Repo             string
	CommitSHA        string
	PRNumber         int
	TargetID         string
	ChangedFiles     []string
	ExpectedJobs     []string
	DiffGated        bool
	SkipPendingNomad bool
}

// HangarStatusResponse is the aggregated telemetry payload returned by GET /status.
type HangarStatusResponse struct {
	Status         string                `json:"status"` // "synced", "lagging", "error"
	MaxLagSeconds  int64                 `json:"max_lag_seconds"`
	LastSyncTime   time.Time             `json:"last_sync_time"`
	Repos          map[string]RepoStatus `json:"repos"`
	Reconciliation *ReconciliationStatus `json:"reconciliation,omitempty"`
}

func runSubprocess(ctx context.Context, bin, dir string, env []string, waitDelay time.Duration, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if env != nil {
		cmd.Env = env
	}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				return cmd.Process.Kill()
			}
			return nil
		}
		return nil
	}
	cmd.WaitDelay = waitDelay

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// ComposeExecutor executes docker compose commands.
type ComposeExecutor func(ctx context.Context, dir string, args ...string) (stdout []byte, stderr []byte, err error)

func defaultComposeExecutor(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
	cmdArgs := append([]string{"compose"}, args...)
	return runSubprocess(ctx, "docker", dir, ScrubComposeEnv(os.Environ()), 10*time.Second, cmdArgs...)
}

// GitExecutor executes git CLI commands.
type GitExecutor func(ctx context.Context, dir string, args ...string) (stdout []byte, stderr []byte, err error)

// NomadExecutor executes nomad CLI commands.
type NomadExecutor func(ctx context.Context, args ...string) (stdout []byte, stderr []byte, err error)

func defaultNomadExecutor(nomadAddr, nomadToken string) NomadExecutor {
	return func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		env := ScrubComposeEnv(os.Environ())
		if nomadAddr != "" {
			env = append(env, "NOMAD_ADDR="+nomadAddr)
		}
		if nomadToken != "" {
			env = append(env, "NOMAD_TOKEN="+nomadToken)
		}

		stdout, stderr, err := runSubprocess(opCtx, "nomad", "", env, 5*time.Second, args...)
		sanitizedStderr := SanitizeLog(string(stderr))
		if nomadToken != "" {
			sanitizedStderr = strings.ReplaceAll(sanitizedStderr, nomadToken, "[REDACTED_NOMAD_TOKEN]")
		}
		return stdout, []byte(sanitizedStderr), err
	}
}

// NomadChangeEvent holds metadata for a git update affecting Nomad job specifications.
type NomadChangeEvent struct {
	RepoPath  string
	Changes   []NomadFileChange
	Repo      string
	CommitSHA string
	PRNumber  int
	TargetID  string
	Timestamp time.Time
}

func runGitCommand(ctx context.Context, dir, pat string, args ...string) ([]byte, []byte, error) {
	gitBin := ResolveGitBin(os.Getenv)
	env := BuildGitEnv(pat, os.Environ())
	return runSubprocess(ctx, gitBin, dir, env, 10*time.Second, args...)
}

// SyncDaemon coordinates periodic and on-demand repository synchronizations.
type SyncDaemon struct {
	sfg             singleflight.Group
	mu              sync.Mutex
	ticker          *time.Ticker
	interval        time.Duration
	repos           []string
	repoUrls        map[string]string
	pat             string
	composeDir      string
	configDir       string
	reconcileCh     chan struct{}
	lastReconcile   time.Time
	lastSyncTimes   map[string]time.Time
	repoGitHubSlugs map[string]string
	statusMu        sync.RWMutex
	triggerFn       func() ([]RepoSyncResult, error)
	reconcileFn     func(ctx context.Context) error

	brainInternalURL string
	extraSecrets     []string
	composeExecutor  ComposeExecutor
	gitExecutor      GitExecutor

	// Nomad Orchestration
	nomadAddr           string
	nomadToken          string
	nomadExecutor       NomadExecutor
	nomadMu             sync.Mutex
	pendingNomadMu      sync.Mutex
	pendingNomadChanges []NomadChangeEvent
	nomadKnownDigests   map[string]string
	nomadDigestsMu      sync.RWMutex
	lastNomadImagePoll  time.Time
	nomadPollMu         sync.Mutex

	// Debounced Image Rollouts
	pendingImageMu       sync.Mutex
	pendingImageRollouts map[string]PendingImageRollout
	imageRolloutCh       chan struct{}
	imageRolloutDebounce time.Duration

	// Concurrency & Nomad Server Reconcile
	repoLocksMu                 sync.Mutex
	repoLocks                   map[string]*sync.Mutex
	pendingNomadServerRestartMu sync.Mutex
	pendingNomadServerRestart   bool

	// HTTP & Registry
	registryClient *http.Client

	// Webhooks & Deploy Lifecycle
	webhooksRouterURL string
	deployDispatcher  func(ctx context.Context, evt HangarDeployEvent) error

	// Nomad Config Cache
	lastPushedConfigMu sync.Mutex
	lastPushedConfig   map[string]string

	serviceConfigs []ServiceConfigMapping
}

func (d *SyncDaemon) getComposeExecutor() ComposeExecutor {
	if d != nil && d.composeExecutor != nil {
		return d.composeExecutor
	}
	return defaultComposeExecutor
}

func (d *SyncDaemon) getNomadExecutor() NomadExecutor {
	if d != nil && d.nomadExecutor != nil {
		return d.nomadExecutor
	}
	nomadAddr := ""
	nomadToken := ""
	if d != nil {
		nomadAddr = d.nomadAddr
		nomadToken = d.nomadToken
	}
	return defaultNomadExecutor(nomadAddr, nomadToken)
}

func (d *SyncDaemon) getGitExecutor() GitExecutor {
	if d != nil && d.gitExecutor != nil {
		return d.gitExecutor
	}
	pat := ""
	if d != nil {
		pat = d.pat
	}
	return func(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
		return runGitCommand(ctx, dir, pat, args...)
	}
}

func (d *SyncDaemon) getRegistryClient() *http.Client {
	if d != nil && d.registryClient != nil {
		return d.registryClient
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (d *SyncDaemon) getDeployDispatcher() func(ctx context.Context, evt HangarDeployEvent) error {
	if d != nil && d.deployDispatcher != nil {
		return d.deployDispatcher
	}
	return func(ctx context.Context, evt HangarDeployEvent) error {
		return errors.New("deploy dispatcher is not configured (deployDispatcher is nil)")
	}
}

func (d *SyncDaemon) defaultDeployDispatcher(ctx context.Context, evt HangarDeployEvent) error {
	routerURL := "http://127.0.0.1:4020"
	if d != nil && strings.TrimSpace(d.webhooksRouterURL) != "" {
		routerURL = strings.TrimRight(strings.TrimSpace(d.webhooksRouterURL), "/")
	}
	targetURL := routerURL + "/api/webhooks/hangar"

	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal deploy event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create deploy event request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req) //nolint:bodyclose // closed by httpkit.CloseWarn
	if err != nil {
		return fmt.Errorf("deploy event post failed: %s (%w)", SanitizeLog(err.Error()), err)
	}
	defer httpkit.CloseWarn(resp.Body, "deploy event response body")

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return fmt.Errorf("deploy event endpoint returned status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("deploy event endpoint returned status %d: %s", resp.StatusCode, SanitizeLog(strings.TrimSpace(string(respBody))))
	}
	return nil
}

func (d *SyncDaemon) DispatchDeployEvent(evt HangarDeployEvent) {
	if d == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Hangar:Deploy] PANIC recovered in deploy dispatcher: %v", r)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := d.getDeployDispatcher()(bgCtx, evt); err != nil {
			log.Printf("[Hangar:Deploy] Warning: failed to dispatch %s for %s: %v", evt.Event, evt.JobName, err)
		}
	}()
}

// GetLastReconcile returns the timestamp of the last successful reconciliation under statusMu read lock.
func (d *SyncDaemon) GetLastReconcile() time.Time {
	d.statusMu.RLock()
	defer d.statusMu.RUnlock()
	return d.lastReconcile
}

// SetLastReconcile updates the timestamp of the last successful reconciliation under statusMu write lock.
func (d *SyncDaemon) SetLastReconcile(t time.Time) {
	d.statusMu.Lock()
	defer d.statusMu.Unlock()
	d.lastReconcile = t
}

// GetReconciliationStatus returns a safe snapshot of the current or recent reconciliation status.
func (d *SyncDaemon) GetReconciliationStatus() ReconciliationStatus {
	return ReconciliationStatus{State: "idle", Stage: "idle"}
}

// DaemonConfig holds configuration options for the GitSync daemon.
type DaemonConfig struct {
	Port              string
	Repos             []string
	RepoURLs          map[string]string
	Interval          time.Duration
	PAT               string
	ComposeDir        string
	ConfigDir         string
	DiscordToken      string
	DiscordChannel    string
	DiscordWebhookURL string
	BrainInternalURL  string
	ExtraSecrets      []string
	ComposeExecutor   ComposeExecutor
	GitExecutor       GitExecutor
	NomadExecutor     NomadExecutor
	NomadAddr         string
	NomadToken        string
	RegistryClient    *http.Client
	WebhooksRouterURL string
	DeployDispatcher  func(ctx context.Context, evt HangarDeployEvent) error
	ServiceConfigs    []ServiceConfigMapping
}

// NewDaemon initializes a new SyncDaemon from config.
// firstNonEmpty returns the first non-empty string in vals, or "" if all are empty.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func NewDaemon(cfg DaemonConfig) *SyncDaemon {
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	cfg.ComposeDir = firstNonEmpty(cfg.ComposeDir, "/share/aerial")
	cfg.ConfigDir = firstNonEmpty(cfg.ConfigDir, "/share/aerial-config")
	cfg.BrainInternalURL = firstNonEmpty(cfg.BrainInternalURL, "http://brain:8080/internal/reload")
	webhooksRouterURL := firstNonEmpty(cfg.WebhooksRouterURL, os.Getenv("WEBHOOKS_ROUTER_URL"), "http://127.0.0.1:4020")
	extraSecrets := append([]string(nil), cfg.ExtraSecrets...)
	if s := strings.TrimSpace(cfg.DiscordToken); s != "" {
		extraSecrets = append(extraSecrets, s)
	}
	if s := strings.TrimSpace(cfg.DiscordWebhookURL); s != "" {
		extraSecrets = append(extraSecrets, s)
	}

	return &SyncDaemon{
		repos:                cfg.Repos,
		repoUrls:             cfg.RepoURLs,
		interval:             cfg.Interval,
		pat:                  cfg.PAT,
		composeDir:           cfg.ComposeDir,
		configDir:            cfg.ConfigDir,
		brainInternalURL:     cfg.BrainInternalURL,
		extraSecrets:         extraSecrets,
		composeExecutor:      cfg.ComposeExecutor,
		gitExecutor:          cfg.GitExecutor,
		nomadExecutor:        cfg.NomadExecutor,
		nomadAddr:            cfg.NomadAddr,
		nomadToken:           cfg.NomadToken,
		nomadKnownDigests:    make(map[string]string),
		registryClient:       cfg.RegistryClient,
		webhooksRouterURL:    webhooksRouterURL,
		deployDispatcher:     cfg.DeployDispatcher,
		reconcileCh:          make(chan struct{}, 1),
		pendingImageRollouts: make(map[string]PendingImageRollout),
		imageRolloutCh:       make(chan struct{}, 1),
		imageRolloutDebounce: 3 * time.Second,
		repoLocks:            make(map[string]*sync.Mutex),
		repoGitHubSlugs:      make(map[string]string),
		lastPushedConfig:     make(map[string]string),
		serviceConfigs:       cfg.ServiceConfigs,
	}
}

func isAllZeros(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c != '0' {
			return false
		}
	}
	return true
}

func fileInChangedList(target string, changed []string) bool {
	targetClean := filepath.Clean(filepath.ToSlash(target))
	targetClean = strings.TrimPrefix(targetClean, "/")
	for _, f := range changed {
		clean := filepath.Clean(filepath.ToSlash(f))
		clean = strings.TrimPrefix(clean, "/")
		if clean == targetClean {
			return true
		}
	}
	return false
}

func homepageInChangedList(changed []string) bool {
	targetClean := "services/homepage/homepage.yaml"
	for _, f := range changed {
		clean := filepath.Clean(filepath.ToSlash(f))
		clean = strings.TrimPrefix(clean, "/")
		if clean == targetClean {
			return true
		}
		if clean == "homepage" || strings.HasPrefix(clean, "homepage/") {
			return true
		}
	}
	return false
}

func (d *SyncDaemon) getChangedFiles(ctx context.Context, repoPath, prevHead, currHead string) ([]string, error) {
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return nil, nil
	}
	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-only", prevHead, currHead, currHead)
	if err != nil {
		return nil, err
	}
	return splitLines(out), nil
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func (d *SyncDaemon) shouldDeferNomadJob(ctx context.Context, jobName, jobSpec string, checkRepoBuilds bool) bool {
	for _, img := range ExtractNomadJobImages(jobSpec) {
		srcRepo := ImageSourceRepo(img)
		if srcRepo != "" {
			if checkRepoBuilds && d.CheckGitHubRepoBuildInProgress(ctx, srcRepo) {
				log.Printf("[Hangar:GitPush] Job %s deferred: source repo %s has builds in progress", jobName, srcRepo)
				return true
			}
			dgst, dgstErr := d.GetRemoteImageDigest(ctx, img)
			if dgstErr != nil || dgst == "" {
				log.Printf("[Hangar:GitPush] Job %s deferred: image %s not ready in registry (%v)", jobName, img, dgstErr)
				return true
			}
		}
	}
	return false
}

func (d *SyncDaemon) applyNomadChangesDirectly(ctx context.Context, repoPath string, changes []NomadFileChange, checkRepoBuilds, failOnJobErr, containersBuilding bool, changedFiles, expectedJobs []string, meta ...string) (applied, deferred []string, err error) {
	repo, commit, targetID, prNumber := parseNomadMeta(meta)

	SortNomadChangesHangarLast(changes)

	type readyApply struct {
		change   NomadFileChange
		fullPath string
	}
	var readyApplies []readyApply
	var deleteChanges []NomadFileChange

	for _, ch := range changes {
		if ch.Action == "delete" {
			deleteChanges = append(deleteChanges, ch)
			continue
		}
		fullPath := filepath.Join(repoPath, ch.Path)
		if !IsSafeJobPath(repoPath, fullPath) {
			continue
		}
		content, readErr := os.ReadFile(fullPath)
		if readErr != nil {
			log.Printf("[Hangar:Nomad] Notice: job file %s not found on disk, skipping", fullPath)
			continue
		}
		jobSpec := string(content)
		if (containersBuilding && JobHasContainerBuildChanges(jobSpec, changedFiles)) || d.shouldDeferNomadJob(ctx, ch.JobName, jobSpec, checkRepoBuilds) {
			log.Printf("[Hangar:GitPush] Deferring Nomad job %s until container image is ready", ch.JobName)
			deferred = append(deferred, ch.JobName)
			continue
		}
		readyApplies = append(readyApplies, readyApply{change: ch, fullPath: fullPath})
	}

	// Phase 1: Teardowns (stop deleted jobs, or defer if core container build rollout is pending)
	if len(deleteChanges) > 0 {
		if !checkRepoBuilds && (containersBuilding || len(deferred) > 0) {
			d.recordPendingNomadChanges(repoPath, deleteChanges, repo, commit, targetID, strconv.Itoa(prNumber))
		} else {
			for _, ch := range deleteChanges {
				d.stopDeletedNomadJob(ctx, "[Hangar:Nomad]", repoPath, ch.JobName)
			}
		}
	}

	// Phase 2: Applications (validate, then run)
	for _, item := range readyApplies {
		ch, fullPath := item.change, item.fullPath
		if errVal := d.ValidateNomadJob(ctx, fullPath); errVal != nil {
			log.Printf("[Hangar:Nomad] ERROR: Validation failed for %s: %v", ch.Path, errVal)
			if failOnJobErr {
				return applied, deferred, errVal
			}
			continue
		}

		baseEvt := HangarDeployEvent{
			JobName:      ch.JobName,
			Repo:         repo,
			CommitSHA:    commit,
			PRNumber:     prNumber,
			TargetID:     targetID,
			ExpectedJobs: expectedJobs,
		}

		log.Printf("[Hangar:Nomad] Applying job %s (%s)...", ch.JobName, ch.Path)
		out, sanitizedCombined, errRun := d.runNomadJobWithEvents(ctx, baseEvt, "job", "run", "-detach", fullPath)
		if errRun != nil {
			log.Printf("[Hangar:Nomad] nomad job run failed for %s: %s (%v)", ch.JobName, sanitizedCombined, errRun)
			if failOnJobErr {
				return applied, deferred, fmt.Errorf("nomad job run failed for %s: %s (%w)", ch.Path, sanitizedCombined, errRun)
			}
			continue
		}
		log.Printf("[Hangar:Nomad] Successfully applied job %s: %s", ch.JobName, SanitizeLog(strings.TrimSpace(string(out))))
		applied = append(applied, ch.JobName)
	}
	return applied, deferred, nil
}

// ExecuteGitPushEvent processes an incoming git push webhook event.
func (d *SyncDaemon) ExecuteGitPushEvent(ctx context.Context, req GitPushEventRequest) (GitPushEventResponse, int) {
	if req.Repo == "" {
		return GitPushEventResponse{
			Status:  "error",
			Message: "missing required field: repo",
		}, http.StatusBadRequest
	}

	// Filter out non-main branch pushes
	ref := strings.TrimSpace(req.Ref)
	if ref != "" && ref != "refs/heads/main" && ref != "main" && !strings.HasSuffix(ref, "/main") {
		return GitPushEventResponse{
			Status:  "ignored",
			Repo:    req.Repo,
			Ref:     req.Ref,
			Commit:  req.Commit,
			Message: fmt.Sprintf("push to non-main ref %q ignored", req.Ref),
		}, http.StatusOK
	}

	repoPath := ResolveRepoPath(req.Repo, d.repos)
	if repoPath == "" {
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Message: fmt.Sprintf("repository %q is not managed by hangar", req.Repo),
		}, http.StatusNotFound
	}

	commitSHA := req.Commit
	sc := ConfigSyncContext{
		Repo:             req.Repo,
		CommitSHA:        commitSHA,
		PRNumber:         req.PRNumber,
		TargetID:         req.TargetID,
		SkipPendingNomad: true,
	}
	res := d.SyncRepo(ctx, repoPath, sc)
	if res.Error != "" {
		if res.CurrentHead != "" {
			commitSHA = res.CurrentHead
		}
		failEvt := HangarDeployEvent{
			Event:     "sync_failed",
			JobName:   "git-sync",
			Repo:      req.Repo,
			PRNumber:  req.PRNumber,
			TargetID:  req.TargetID,
			CommitSHA: commitSHA,
			Status:    "failed",
			Details:   res.Error,
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(failEvt)
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Commit:  res.CurrentHead,
			Message: fmt.Sprintf("git sync failed: %s", res.Error),
		}, http.StatusInternalServerError
	}

	resp := GitPushEventResponse{
		Status: "accepted",
		Repo:   req.Repo,
		Ref:    req.Ref,
		Commit: res.CurrentHead,
	}

	if res.CurrentHead != "" {
		commitSHA = res.CurrentHead
	}
	sc.CommitSHA = commitSHA

	cleanRepo := strings.ToLower(filepath.Clean(repoPath))
	isAerialConfig := strings.Contains(cleanRepo, "aerial-config")
	isAerialCore := strings.HasSuffix(cleanRepo, "/aerial") || cleanRepo == "aerial"

	changedFiles, errDiff := d.getChangedFiles(ctx, repoPath, res.PreviousHead, res.CurrentHead)
	if isAerialConfig && errDiff != nil {
		log.Printf("[Hangar:GitPush] Error: getChangedFiles failed for %s: %v", repoPath, errDiff)
		return GitPushEventResponse{
			Status:  "error",
			Repo:    req.Repo,
			Commit:  res.CurrentHead,
			Message: fmt.Sprintf("failed inspecting git diff: %v", errDiff),
		}, http.StatusInternalServerError
	}

	nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
	if isAerialConfig && errNomad != nil {
		log.Printf("[Hangar:GitPush] Warning: HasNomadChanges failed for %s: %v", repoPath, errNomad)
	}
	if isAerialCore && errDiff == nil && HasMigrationChanges(changedFiles) {
		nomadChanges = EnsureDBMigrateNomadChange(nomadChanges)
		errNomad = nil
	}

	expectedJobs := ResolveExpectedDeployJobs(repoPath, changedFiles, nomadChanges, d.GetServiceConfigMappings(repoPath), d.candidateJobRepos())
	resp.ExpectedJobs = expectedJobs
	sc.ExpectedJobs = expectedJobs

	syncEvt := HangarDeployEvent{
		Event:        "sync_success",
		JobName:      "git-sync",
		Repo:         req.Repo,
		PRNumber:     req.PRNumber,
		TargetID:     req.TargetID,
		CommitSHA:    commitSHA,
		Status:       "success",
		Details:      "git sync completed",
		ExpectedJobs: expectedJobs,
		Timestamp:    time.Now().UTC(),
	}
	d.DispatchDeployEvent(syncEvt)

	if isAerialCore {
		containersBuilding := errDiff == nil && HasContainerBuildChanges(repoPath, changedFiles)
		if containersBuilding {
			resp.ContainersBuilding = true
		}

		if errNomad == nil && len(nomadChanges) > 0 {
			resp.NomadChanged = true
			applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, false, true, containersBuilding, changedFiles, expectedJobs, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
			resp.AppliedJobs = applied
			if len(deferred) > 0 {
				resp.ContainersBuilding = true
			}
			if errRec != nil {
				resp.Status = "error"
				resp.Message = fmt.Sprintf("failed applying nomad changes: %v", errRec)
				return resp, http.StatusInternalServerError
			}
			if resp.ContainersBuilding {
				resp.Message = fmt.Sprintf("repository synced; applied %d nomad job(s), deferred container build rollout until image_ready", len(applied))
			} else {
				resp.Message = fmt.Sprintf("repository synced; applied %d nomad job(s)", len(applied))
			}
			return resp, http.StatusOK
		}

		if containersBuilding {
			resp.Message = "repository synced; container build changes detected, deferring rollout until image_ready"
			log.Printf("[Hangar:GitPush] Container build changes detected in %s (%d files). Deferring Nomad rollout.", repoPath, len(changedFiles))
			return resp, http.StatusOK
		}

		resp.Message = "repository synced; no container build or nomad changes"
		return resp, http.StatusOK
	}

	if isAerialConfig {
		sc.DiffGated = true
		sc.ChangedFiles = changedFiles

		if len(nomadChanges) > 0 {
			resp.NomadChanged = true
			applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, true, false, false, nil, expectedJobs, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
			if errRec != nil {
				log.Printf("[Hangar:GitPush] Notice: applyNomadChangesDirectly notice: %v", errRec)
			}
			resp.AppliedJobs = applied
			if len(deferred) > 0 {
				resp.ContainersBuilding = true
				resp.Message = fmt.Sprintf("applied %d job(s); deferred %d job(s) pending container builds", len(applied), len(deferred))
			} else {
				resp.Message = fmt.Sprintf("applied %d nomad job(s)", len(applied))
			}
		} else {
			resp.Message = "configuration synced; no nomad job changes"
		}

		sc.CommitSHA = commitSHA
		d.syncAllConfigsToNomad(ctx, "[Hangar:GitPush]", repoPath, sc)

		return resp, http.StatusOK
	}

	resp.Message = "repository synced"
	return resp, http.StatusOK
}

func (d *SyncDaemon) findAllNomadJobsByImage(imgRef string) []MatchedNomadJob {
	var dirs []string
	for _, repo := range d.candidateJobRepos() {
		dirs = append(dirs, filepath.Join(repo, "jobs"), filepath.Join(repo, "nomad", "jobs"))
	}
	var matches []MatchedNomadJob
	seenJobs := make(map[string]struct{})
	walkNomadJobFiles(dirs, func(dir, jobPath, jobName, content string) bool {
		for _, img := range ExtractNomadJobImages(content) {
			if ImageMatches(imgRef, img) {
				if _, ok := seenJobs[jobName]; !ok {
					seenJobs[jobName] = struct{}{}
					matches = append(matches, MatchedNomadJob{
						JobName: jobName,
						JobPath: jobPath,
					})
				}
				break
			}
		}
		return true
	})
	return matches
}

// ExecuteImageReadyEvent processes an incoming image ready event by deploying matching Nomad jobs.
func (d *SyncDaemon) ExecuteImageReadyEvent(ctx context.Context, req ImageReadyEventRequest) (ImageReadyEventResponse, int) {
	imgRef := strings.TrimSpace(req.Image)
	if imgRef == "" {
		return ImageReadyEventResponse{
			Status:  "error",
			Message: "missing required field: image",
		}, http.StatusBadRequest
	}

	matches := d.findAllNomadJobsByImage(imgRef)

	if len(matches) == 0 {
		log.Printf("[Hangar:ImageReady] No Nomad jobs found referencing image %s", imgRef)
		return ImageReadyEventResponse{
			Status:  "not_found",
			Image:   imgRef,
			Message: fmt.Sprintf("no nomad jobs found using image %s", imgRef),
		}, http.StatusNotFound
	}

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	SortMatchedJobsHangarLast(matches)

	pendingTeardowns := d.drainPendingNomadDeletes()
	if len(pendingTeardowns) > 0 {
		d.ExecuteNomadTeardowns(ctx, pendingTeardowns)
	}

	digest := req.Digest
	if digest == "" {
		if remoteDigest, err := d.GetRemoteImageDigest(ctx, imgRef); err == nil && remoteDigest != "" {
			digest = remoteDigest
		} else if err != nil {
			log.Printf("[Hangar:ImageReady] Warning: could not resolve remote digest for %s: %v", imgRef, err)
		}
	}

	var appliedJobs []string
	var runErrors []string
	for _, job := range matches {
		content, readErr := os.ReadFile(job.JobPath)
		if readErr != nil {
			log.Printf("[Hangar:ImageReady] Failed reading job file %s: %v", job.JobPath, readErr)
			runErrors = append(runErrors, fmt.Sprintf("%s: read error: %v", job.JobName, readErr))
			continue
		}

		jobSpec := string(content)

		if errVal := d.ValidateNomadJob(ctx, job.JobPath); errVal != nil {
			log.Printf("[Hangar:ImageReady] Validation failed for job %s (%s): %v", job.JobName, job.JobPath, errVal)
			runErrors = append(runErrors, fmt.Sprintf("%s validation failed: %v", job.JobName, errVal))
			continue
		}

		baseEvt := HangarDeployEvent{
			JobName:   job.JobName,
			Repo:      req.Repo,
			CommitSHA: req.CommitSHA,
			PRNumber:  req.PRNumber,
			TargetID:  req.TargetID,
			Image:     req.Image,
			Digest:    digest,
		}

		runArgs := []string{"job", "run", "-detach"}
		varInjected := false
		if HasNomadJobVariable(jobSpec, "image_tag") {
			tag := req.CommitSHA
			if tag == "" {
				if digest != "" {
					cleanDigest := strings.TrimPrefix(digest, "@")
					if !strings.HasPrefix(cleanDigest, "sha256:") {
						cleanDigest = "sha256:" + cleanDigest
					}
					tag = "latest@" + cleanDigest
				} else {
					tag = "latest"
				}
			}
			runArgs = append(runArgs, "-var=image_tag="+tag)
			varInjected = true
		}
		runArgs = append(runArgs, job.JobPath)

		log.Printf("[Hangar:ImageReady] Applying Nomad job %s from %s (args: %v)...", job.JobName, job.JobPath, runArgs)
		if _, sanitizedCombined, errRun := d.runNomadJobWithEvents(ctx, baseEvt, runArgs...); errRun != nil {
			log.Printf("[Hangar:ImageReady] Error: job run failed for %s: %s (%v)", job.JobName, sanitizedCombined, errRun)
			runErrors = append(runErrors, fmt.Sprintf("%s: %v", job.JobName, errRun))
			continue
		}

		if !varInjected {
			log.Printf("[Hangar:ImageReady] Rescheduling allocation restart for unparameterized job %s...", job.JobName)
			outRest, errRestBytes, errRest := d.getNomadExecutor()(ctx, "job", "restart", "-yes", "-reschedule", "-on-error=fail", job.JobName)
			if errRest != nil {
				log.Printf("[Hangar:ImageReady] Warning: job restart -reschedule failed for %s: %s (%v)", job.JobName, SanitizeLog(strings.TrimSpace(string(append(outRest, errRestBytes...)))), errRest)
			}
		}

		appliedJobs = append(appliedJobs, job.JobName)

		if digest != "" {
			key := job.JobName + ":" + imgRef
			d.nomadDigestsMu.Lock()
			if d.nomadKnownDigests == nil {
				d.nomadKnownDigests = make(map[string]string)
			}
			d.nomadKnownDigests[key] = digest
			d.nomadDigestsMu.Unlock()
		}
	}

	if len(runErrors) > 0 {
		errMsg := strings.Join(runErrors, "; ")
		return ImageReadyEventResponse{
			Status:      "error",
			Image:       imgRef,
			MatchedJobs: appliedJobs,
			Message:     errMsg,
		}, http.StatusInternalServerError
	}

	return ImageReadyEventResponse{
		Status:      "accepted",
		Image:       imgRef,
		MatchedJobs: appliedJobs,
		Message:     fmt.Sprintf("successfully applied %d matching nomad job(s)", len(appliedJobs)),
	}, http.StatusOK
}

// EnqueueImageReadyEvent validates an incoming image ready notification, records all matching
// Nomad jobs into the pending rollout batch, signals the debouncer worker, and returns HTTP 202 Accepted immediately.
func (d *SyncDaemon) EnqueueImageReadyEvent(req ImageReadyEventRequest) (ImageReadyEventResponse, int) {
	imgRef := strings.TrimSpace(req.Image)
	if imgRef == "" {
		return ImageReadyEventResponse{
			Status:  "error",
			Message: "missing required field: image",
		}, http.StatusBadRequest
	}

	matches := d.findAllNomadJobsByImage(imgRef)

	if len(matches) == 0 {
		log.Printf("[Hangar:ImageReady] No Nomad jobs found referencing image %s", imgRef)
		return ImageReadyEventResponse{
			Status:  "not_found",
			Image:   imgRef,
			Message: fmt.Sprintf("no nomad jobs found using image %s", imgRef),
		}, http.StatusNotFound
	}

	d.pendingImageMu.Lock()
	if d.pendingImageRollouts == nil {
		d.pendingImageRollouts = make(map[string]PendingImageRollout)
	}
	appliedJobs := make([]string, 0, len(matches))
	for _, m := range matches {
		d.pendingImageRollouts[m.JobName] = PendingImageRollout{
			JobName: m.JobName,
			JobPath: m.JobPath,
			Request: req,
		}
		appliedJobs = append(appliedJobs, m.JobName)
	}
	d.pendingImageMu.Unlock()

	// Non-blocking channel signal to debounce worker
	if d.imageRolloutCh != nil {
		select {
		case d.imageRolloutCh <- struct{}{}:
		default:
		}
	}

	log.Printf("[Hangar:ImageReady] Queued debounced rollout for image %s (matched %d job(s): %v)", imgRef, len(appliedJobs), appliedJobs)

	return ImageReadyEventResponse{
		Status:      "accepted",
		Image:       imgRef,
		MatchedJobs: appliedJobs,
		Message:     fmt.Sprintf("queued debounced rollout for %d job(s)", len(appliedJobs)),
	}, http.StatusAccepted
}

// StartImageRolloutLoop runs the background debounced worker goroutine for image rollouts.
func (d *SyncDaemon) StartImageRolloutLoop(ctx context.Context) {
	if d.imageRolloutCh == nil {
		d.imageRolloutCh = make(chan struct{}, 1)
	}

	debounceDuration := d.imageRolloutDebounce
	if debounceDuration <= 0 {
		debounceDuration = 3 * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Hangar:ImageRollout] PANIC recovered in StartImageRolloutLoop: %v", r)
			}
		}()

		var (
			debounceTimer *time.Timer
			timerCh       <-chan time.Time
		)

		for {
			select {
			case <-ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			case <-d.imageRolloutCh:
				if debounceTimer != nil {
					if !debounceTimer.Stop() {
						select {
						case <-debounceTimer.C:
						default:
						}
					}
				}
				debounceTimer = time.NewTimer(debounceDuration)
				timerCh = debounceTimer.C
			case <-timerCh:
				timerCh = nil
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[Hangar:ImageRollout] PANIC recovered in executePendingImageRollouts: %v", r)
						}
					}()
					d.executePendingImageRollouts(ctx)
				}()
			}
		}
	}()
}

func (d *SyncDaemon) isHangarRequest(req ImageReadyEventRequest) bool {
	if IsHangarImage(req.Image) {
		return true
	}
	for _, m := range d.findAllNomadJobsByImage(req.Image) {
		if IsHangarJob(m.JobName) {
			return true
		}
	}
	return false
}

func (d *SyncDaemon) executePendingImageRollouts(ctx context.Context) {
	d.pendingImageMu.Lock()
	if len(d.pendingImageRollouts) == 0 {
		d.pendingImageMu.Unlock()
		return
	}
	// Deduplicate rollout requests by image to execute unified job rollout per image
	uniqueRequests := make(map[string]ImageReadyEventRequest)
	for _, v := range d.pendingImageRollouts {
		uniqueRequests[v.Request.Image] = v.Request
	}
	d.pendingImageRollouts = make(map[string]PendingImageRollout)
	d.pendingImageMu.Unlock()

	log.Printf("[Hangar:ImageRollout] Debounce timer expired, executing rollout batch for %d unique image(s)...", len(uniqueRequests))

	var nonHangar, hangar []ImageReadyEventRequest
	for _, req := range uniqueRequests {
		if d.isHangarRequest(req) {
			hangar = append(hangar, req)
		} else {
			nonHangar = append(nonHangar, req)
		}
	}

	for _, req := range append(nonHangar, hangar...) {
		if ctx.Err() != nil {
			return
		}
		resp, code := d.ExecuteImageReadyEvent(ctx, req)
		log.Printf("[Hangar:ImageRollout] Rollout for %s completed with code %d: status=%s, message=%s", req.Image, code, resp.Status, resp.Message)
	}
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// SetupMux configures HTTP handlers for metrics, health, status, and sync.
func SetupMux(daemon *SyncDaemon) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/debug/coverage/flush", httpkit.HandleCoverageFlush("hangar", false))

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		status := daemon.GetStatus(ctx)
		writeJSON(w, r, http.StatusOK, status)
	})

	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}

		results, err := daemon.TriggerSync()
		if err != nil {
			metrics.RecordSyncRequest("webhook", "error")
			writeJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		metrics.RecordSyncRequest("webhook", "synced")
		writeJSON(w, r, http.StatusOK, map[string]interface{}{
			"status":  "synced",
			"results": results,
		})
	})

	mux.HandleFunc("/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}

		if r.Body != nil {
			defer httpkit.CloseWarn(r.Body, "reconcile request body")
		}

		isSync := r.URL.Query().Get("sync") == "true"
		if !isSync && r.Header.Get("Content-Type") == "application/json" && r.Body != nil {
			var bodyReq struct {
				Sync *bool `json:"sync"`
			}
			if err := json.NewDecoder(r.Body).Decode(&bodyReq); err == nil && bodyReq.Sync != nil {
				isSync = *bodyReq.Sync
			}
		}

		if isSync {
			if err := daemon.TriggerReconcile(r.Context(), false); err != nil {
				writeJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}

			writeJSON(w, r, http.StatusOK, map[string]interface{}{
				"status":  "reconciled",
				"applied": true,
			})
			return
		}

		if qErr := daemon.TriggerReconcile(r.Context(), true); qErr != nil {
			log.Printf("[Hangar:HTTP] Failed to queue reconcile: %v", qErr)
		}
		writeJSON(w, r, http.StatusAccepted, map[string]interface{}{
			"status":           "queued",
			"message":          "Nomad reconciliation scheduled",
			"debounce_seconds": int(reconcilerDebounceDuration.Seconds()),
		})
	})

	mux.HandleFunc("/events/git_push", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		if r.Body != nil {
			defer httpkit.CloseWarn(r.Body, "git_push request body")
		}

		var req GitPushEventRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, r, http.StatusBadRequest, GitPushEventResponse{
				Status:  "error",
				Message: fmt.Sprintf("invalid JSON payload: %v", err),
			})
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 60*time.Second)
		defer cancel()

		resp, code := daemon.ExecuteGitPushEvent(ctx, req)
		metrics.RecordSyncRequest("git_push", resp.Status)
		writeJSON(w, r, code, resp)
	})

	mux.HandleFunc("/events/image_ready", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		if r.Body != nil {
			defer httpkit.CloseWarn(r.Body, "image_ready request body")
		}

		var req ImageReadyEventRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, r, http.StatusBadRequest, ImageReadyEventResponse{
				Status:  "error",
				Message: fmt.Sprintf("invalid JSON payload: %v", err),
			})
			return
		}

		resp, code := daemon.EnqueueImageReadyEvent(req)
		metrics.RecordSyncRequest("image_ready", resp.Status)
		writeJSON(w, r, code, resp)
	})

	return mux
}

// RunDaemon starts the background sync daemon and HTTP server.
func RunDaemon(ctx context.Context, cfg DaemonConfig) error {
	daemon := NewDaemon(cfg)
	if daemon.deployDispatcher == nil {
		daemon.deployDispatcher = daemon.defaultDeployDispatcher
	}

	if err := daemon.EnsureDockerAuth(); err != nil {
		log.Printf("[Hangar] Warning: Failed to configure Docker registry authentication: %v", SanitizeLog(err.Error()))
	} else if strings.TrimSpace(cfg.PAT) != "" {
		log.Printf("[Hangar] Docker registry authentication configured for ghcr.io")
	}

	daemon.StartPeriodicLoop(ctx)
	daemon.StartReconcilerLoop(ctx)
	daemon.StartImageRolloutLoop(ctx)
	log.Printf("[Hangar] Sidecar GitOps daemon started on :%s (interval: %v, repos: %v, composeDir: %s)", cfg.Port, cfg.Interval, cfg.Repos, cfg.ComposeDir)

	go func() {
		if _, err := daemon.TriggerSync(); err != nil {
			log.Printf("[Hangar] Initial startup sync completed with notice: %v", err)
		}
	}()

	mux := SetupMux(daemon)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("[Hangar] Shutting down GitSync sidecar gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[Hangar] HTTP server shutdown error: %v", err)
		}
		log.Println("[Hangar] GitSync sidecar stopped cleanly")
		return nil
	case err := <-serverErr:
		return fmt.Errorf("HTTP server fatal error: %w", err)
	}
}

// HangarYAMLConfig defines the declarative file schema for services/hangar/hangar.yaml.
type HangarYAMLConfig struct {
	Port             string                 `yaml:"port"`
	SyncInterval     string                 `yaml:"sync_interval"`
	SyncRepos        []string               `yaml:"sync_repos"`
	NomadAddr        string                 `yaml:"nomad_addr"`
	BrainInternalURL string                 `yaml:"brain_internal_url"`
	ServiceConfigs   []ServiceConfigMapping `yaml:"service_configs"`
}

// NewConfigFromLookup extracts DaemonConfig using a provided lookup function.
// If lookup is nil, it safely defaults to returning empty strings without ambient env access.
func NewConfigFromLookup(lookup func(string) string) DaemonConfig {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}

	configPath := lookup("CONFIG_PATH")
	var fileCfg HangarYAMLConfig
	if configPath != "" {
		if data, err := os.ReadFile(configPath); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err != nil {
				log.Printf("[Hangar] Warning: failed to parse yaml config at %s: %v", configPath, err)
			}
		}
	}

	port := firstNonEmpty(lookup("PORT"), fileCfg.Port, "8087")

	var rawRepoList []string
	if rawRepos := lookup("SYNC_REPOS"); rawRepos != "" {
		rawRepoList = strings.Split(rawRepos, ",")
	} else if len(fileCfg.SyncRepos) > 0 {
		rawRepoList = fileCfg.SyncRepos
	}
	var repos []string
	for _, r := range rawRepoList {
		if r = strings.TrimSpace(r); r != "" {
			repos = append(repos, r)
		}
	}
	if len(repos) == 0 {
		repos = []string{"/share/aerial-config", "/share/aerial"}
	}

	intervalStr := firstNonEmpty(lookup("SYNC_INTERVAL"), fileCfg.SyncInterval)
	interval := 60 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d > 0 {
			interval = d
		}
	}

	pat := lookup("GITHUB_PAT")
	configRepoURL := firstNonEmpty(lookup("AERIAL_CONFIG_REPO_URL"), "https://github.com/azylman/aerial-config.git")
	composeDir := firstNonEmpty(lookup("AERIAL_INTERNAL_PROJECT_DIR"), lookup("AERIAL_PROJECT_DIR"), "/share/aerial")
	configDir := firstNonEmpty(lookup("AERIAL_INTERNAL_CONFIG_DIR"), lookup("AERIAL_CONFIG_DIR"), "/share/aerial-config")

	repoUrls := map[string]string{
		configDir:  configRepoURL,
		composeDir: "https://github.com/azylman/aerial.git",
	}

	discordToken := firstNonEmpty(lookup("DISCORD_BOT_TOKEN"), lookup("DISCORD_TOKEN"))
	discordChannel := lookup("DISCORD_CHANNEL")
	discordWebhookURL := lookup("DISCORD_WEBHOOK_URL")

	brainInternalURL := firstNonEmpty(lookup("BRAIN_INTERNAL_URL"), fileCfg.BrainInternalURL, "http://brain:8088/internal/reload")
	extraSecretCandidates := []string{
		lookup("POSTGRES_PASSWORD"),
		lookup("HA_TOKEN"),
		lookup("GEMINI_HARNESS_API_KEY"),
		lookup("AERIAL_GEMINI_API_KEY"),
		lookup("GEMINI_API_KEY"),
		lookup("GITHUB_PAT"),
		lookup("NOMAD_TOKEN"),
	}
	var extraSecrets []string
	seenSecrets := make(map[string]bool)
	for _, s := range extraSecretCandidates {
		s = strings.TrimSpace(s)
		if len(s) >= 4 && !seenSecrets[s] {
			seenSecrets[s] = true
			extraSecrets = append(extraSecrets, s)
		}
	}

	nomadAddr := firstNonEmpty(lookup("NOMAD_ADDR"), fileCfg.NomadAddr, "http://host.docker.internal:4646")
	nomadToken := lookup("NOMAD_TOKEN")

	webhooksRouterURL := firstNonEmpty(lookup("WEBHOOKS_ROUTER_URL"), "http://127.0.0.1:4020")
	var serviceConfigs []ServiceConfigMapping
	if len(fileCfg.ServiceConfigs) > 0 {
		serviceConfigs = fileCfg.ServiceConfigs
	}

	return DaemonConfig{
		Port:              port,
		Repos:             repos,
		RepoURLs:          repoUrls,
		Interval:          interval,
		PAT:               pat,
		ComposeDir:        composeDir,
		ConfigDir:         configDir,
		DiscordToken:      discordToken,
		DiscordChannel:    discordChannel,
		DiscordWebhookURL: discordWebhookURL,
		BrainInternalURL:  brainInternalURL,
		ExtraSecrets:      extraSecrets,
		NomadAddr:         nomadAddr,
		NomadToken:        nomadToken,
		WebhooksRouterURL: webhooksRouterURL,
		ServiceConfigs:    serviceConfigs,
	}
}

// NewConfigFromEnv extracts DaemonConfig from environment variables with sensible defaults.
func NewConfigFromEnv() DaemonConfig {
	return NewConfigFromLookup(func(k string) string {
		v := os.Getenv(k)
		if k == "CONFIG_PATH" && v == "" {
			defaultPath := "/share/aerial-config/services/hangar/hangar.yaml"
			if _, err := os.Stat(defaultPath); err == nil {
				return defaultPath
			}
		}
		return v
	})
}

func main() {
	cfg := NewConfigFromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunDaemon(ctx, cfg); err != nil {
		log.Fatalf("[Hangar] Fatal error: %v", err)
	}
}
