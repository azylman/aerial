package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/sidecars/gitsync/pkg/metrics"
	"golang.org/x/sync/singleflight"
)

var sanitizePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)basic\s+[a-zA-Z0-9+/=]+`),
	regexp.MustCompile(`(?i)x-access-token:[^@\s]+`),
	regexp.MustCompile(`(?i)github_pat_[a-zA-Z0-9_]+`),
	regexp.MustCompile(`(?i)ghp_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)gho_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)ghu_[a-zA-Z0-9]+`),
	regexp.MustCompile(`sk-ant-[a-zA-Z0-9_-]+`),
	regexp.MustCompile(`sk-[a-zA-Z0-9_-]{20,}`),
	regexp.MustCompile(`AIza[0-9A-Za-z-_]{35}`),
	regexp.MustCompile(`(?i)mfa\.[a-z0-9_-]{20,}|[a-z0-9_-]{24}\.[a-z0-9_-]{6}\.[a-z0-9_-]{27}`),
	regexp.MustCompile(`postgres://[^:]+:[^@]+@[^/]+/[^?]+`),
}

// SanitizeLog scrubs sensitive tokens from error and log messages.
func SanitizeLog(input string) string {
	out := input
	for _, re := range sanitizePatterns {
		out = re.ReplaceAllString(out, "[REDACTED_TOKEN]")
	}
	return out
}

// RepoSyncResult holds telemetry for a single repository sync operation.
type RepoSyncResult struct {
	Repo           string `json:"repo"`
	PreviousHead   string `json:"previous_head"`
	CurrentHead    string `json:"current_head"`
	Changed        bool   `json:"changed"`
	ComposeChanged bool   `json:"compose_changed,omitempty"`
	Error          string `json:"error,omitempty"`
}

// RepoStatus holds git commit and timestamp metadata for an individual repository.
type RepoStatus struct {
	Repo             string     `json:"repo"`
	DiskCommit       string     `json:"disk_commit"`
	DiskCommitTime   *time.Time `json:"disk_commit_time,omitempty"`
	RemoteCommit     string     `json:"remote_commit"`
	RemoteCommitTime *time.Time `json:"remote_commit_time,omitempty"`
	TimeLagSeconds   int64      `json:"time_lag_seconds"`
	SyncStatus       string     `json:"sync_status"` // "synced", "lagging", "error"
	LastSyncTime     time.Time  `json:"last_sync_time"`
	Error            string     `json:"error,omitempty"`
}

// GitSyncStatusResponse is the aggregated telemetry payload returned by GET /status.
type GitSyncStatusResponse struct {
	Status        string                `json:"status"` // "synced", "lagging", "error"
	MaxLagSeconds int64                 `json:"max_lag_seconds"`
	LastSyncTime  time.Time             `json:"last_sync_time"`
	Repos         map[string]RepoStatus `json:"repos"`
}

// SyncDaemon coordinates periodic and on-demand repository synchronizations.
type SyncDaemon struct {
	sfg           singleflight.Group
	mu            sync.Mutex
	ticker        *time.Ticker
	interval      time.Duration
	repos         []string
	repoUrls      map[string]string
	pat           string
	composeDir    string
	configDir     string
	reconcileCh   chan struct{}
	composeMu     sync.Mutex
	lastReconcile time.Time
	lastSyncTimes map[string]time.Time
	statusMu      sync.RWMutex
}

// resolveGitDir checks if repoPath contains a .git directory or a .git file (e.g., worktree/submodule).
func resolveGitDir(repoPath string) (string, error) {
	gitPath := filepath.Join(repoPath, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return gitPath, nil
	}

	data, err := os.ReadFile(gitPath)
	if err != nil {
		return "", err
	}
	content := strings.TrimSpace(string(data))
	const prefix = "gitdir:"
	if strings.HasPrefix(content, prefix) {
		target := strings.TrimSpace(content[len(prefix):])
		if !filepath.IsAbs(target) {
			target = filepath.Join(repoPath, target)
		}
		return filepath.Clean(target), nil
	}
	return gitPath, nil
}

// buildGitEnv builds the environment variables for git execution with secret hygiene.
func buildGitEnv(pat string) []string {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return env
	}

	encoded := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + pat))
	cleanEncoded := strings.ReplaceAll(strings.ReplaceAll(encoded, "\r", ""), "\n", "")

	return append(env,
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=http.extraHeader",
		"GIT_CONFIG_VALUE_0="+fmt.Sprintf("AUTHORIZATION: basic %s", cleanEncoded),
		"GIT_CONFIG_KEY_1=http.version",
		"GIT_CONFIG_VALUE_1=HTTP/1.1",
	)
}

// HasComposeChanges checks whether compose or environment configuration files changed between commits.
func (d *SyncDaemon) HasComposeChanges(ctx context.Context, repoPath, prevHead, currHead string) (bool, error) {
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return false, nil
	}

	composeTargets := []string{
		"docker-compose.yml",
		"docker-compose.override.yml",
		"docker-compose.override.yaml",
		"compose.yaml",
		"compose.override.yaml",
		".env",
		".env.example",
	}

	args := append([]string{"-C", repoPath, "diff", "--name-only", prevHead, currHead, "--"}, composeTargets...)
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = buildGitEnv(d.pat)
	out, err := cmd.Output()
	if err != nil {
		// Fallback for shallow clone or disconnected histories
		fallbackArgs := append([]string{"-C", repoPath, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD", "--"}, composeTargets...)
		cmdFallback := exec.CommandContext(ctx, "git", fallbackArgs...)
		cmdFallback.Env = buildGitEnv(d.pat)
		outFallback, errFallback := cmdFallback.Output()
		if errFallback != nil {
			log.Printf("[GitSync] Warning: Failed to inspect diff in %s (%v); failing safe to trigger reconcile", repoPath, errFallback)
			return true, nil
		}
		return len(strings.TrimSpace(string(outFallback))) > 0, nil
	}

	return len(strings.TrimSpace(string(out))) > 0, nil
}

// getComposeArgs constructs the compose CLI flags with base docker-compose.yml and any present overrides.
func (d *SyncDaemon) getComposeArgs(composeDir string, subCmd ...string) []string {
	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	args := []string{
		"--project-name", "aerial",
		"--project-directory", composeDir,
		"-f", baseFile,
	}

	configDir := d.configDir
	if configDir == "" {
		configDir = os.Getenv("AERIAL_CONFIG_DIR")
		if configDir == "" {
			configDir = "/share/aerial-config"
		}
	}

	overrideCandidates := []string{
		filepath.Join(composeDir, "docker-compose.override.yml"),
		filepath.Join(composeDir, "docker-compose.override.yaml"),
		filepath.Join(configDir, "docker-compose.override.yml"),
		filepath.Join(configDir, "docker-compose.override.yaml"),
	}

	seen := make(map[string]bool)
	for _, cand := range overrideCandidates {
		cleaned := filepath.Clean(cand)
		if seen[cleaned] {
			continue
		}
		seen[cleaned] = true
		if _, err := os.Stat(cleaned); err == nil {
			args = append(args, "-f", cleaned)
		}
	}

	return append(args, subCmd...)
}

// ValidateCompose executes docker compose config --quiet to verify valid syntax and schema before apply.
func (d *SyncDaemon) ValidateCompose(ctx context.Context, composeDir string) error {
	composeFile := filepath.Join(composeDir, "docker-compose.yml")
	if _, err := os.Stat(composeFile); err != nil {
		return fmt.Errorf("compose file not found: %w", err)
	}

	args := append([]string{"compose"}, d.getComposeArgs(composeDir, "config", "--quiet")...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = composeDir
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		sanitized := SanitizeLog(strings.TrimSpace(errBuf.String()))
		return fmt.Errorf("compose validation failed: %s (%w)", sanitized, err)
	}
	return nil
}

// ReconcileCompose executes docker compose up -d with timeout, metrics observation, and output sanitization.
func (d *SyncDaemon) ReconcileCompose(parentCtx context.Context) (err error) {
	d.composeMu.Lock()
	defer d.composeMu.Unlock()

	start := time.Now()
	defer func() {
		metrics.RecordReconciliation(metrics.SanitizeStatus(err), time.Since(start))
	}()

	composeDir := d.composeDir
	if composeDir == "" {
		composeDir = "/share/aerial"
	}

	if _, statErr := os.Stat(filepath.Join(composeDir, "docker-compose.yml")); statErr != nil {
		log.Printf("[GitSync:GitOps] Notice: No docker-compose.yml found in %s, skipping reconciliation", composeDir)
		return nil
	}

	// 1. Pre-flight validation gate
	valCtx, valCancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer valCancel()

	if valErr := d.ValidateCompose(valCtx, composeDir); valErr != nil {
		log.Printf("[GitSync:GitOps] ERROR: Pre-flight validation failed: %v. Aborting compose reconciliation.", valErr)
		return valErr
	}

	// 2. Bounded compose execution
	ctx, cancel := context.WithTimeout(parentCtx, 120*time.Second)
	defer cancel()

	log.Printf("[GitSync:GitOps] Reconciling Docker Compose state in %s...", composeDir)

	args := append([]string{"compose"}, d.getComposeArgs(composeDir, "up", "-d", "--no-recreate", "gitsync")...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = composeDir
	cmd.Cancel = func() error {
		log.Printf("[GitSync:GitOps] Compose apply timeout reached, sending SIGTERM...")
		if cmd.Process != nil {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

	out, cmdErr := cmd.CombinedOutput()
	sanitized := SanitizeLog(strings.TrimSpace(string(out)))

	if cmdErr != nil {
		log.Printf("[GitSync:GitOps] ERROR: docker compose up failed: %s (%v)", sanitized, cmdErr)
		return fmt.Errorf("compose up failed: %s (%w)", sanitized, cmdErr)
	}

	if sanitized != "" {
		log.Printf("[GitSync:GitOps] Reconcile output: %s", sanitized)
	}
	log.Printf("[GitSync:GitOps] Docker Compose reconciliation successfully applied.")
	d.lastReconcile = time.Now()
	return nil
}

// StartReconcilerLoop runs the background debounced worker goroutine.
func (d *SyncDaemon) StartReconcilerLoop(ctx context.Context) {
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}

	go func() {
		var debounceTimer *time.Timer
		const debounceDuration = 5 * time.Second
		const minCooldown = 15 * time.Second

		for {
			select {
			case <-ctx.Done():
				return
			case <-d.reconcileCh:
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(debounceDuration, func() {
					if elapsed := time.Since(d.lastReconcile); elapsed < minCooldown {
						time.Sleep(minCooldown - elapsed)
					}
					_ = d.ReconcileCompose(context.Background())
				})
			}
		}
	}()
}

// EnsureRepo clones or initializes the repository if .git does not exist.
func (d *SyncDaemon) EnsureRepo(ctx context.Context, repoPath, repoURL string) error {
	if repoPath == "" || repoURL == "" {
		return nil
	}

	if _, err := resolveGitDir(repoPath); err == nil {
		return nil
	}

	log.Printf("[GitSync] Bootstrapping repository at %s from %s...", repoPath, repoURL)
	_ = os.MkdirAll(repoPath, 0755)

	entries, err := os.ReadDir(repoPath)
	if err != nil {
		return err
	}

	gitEnv := buildGitEnv(d.pat)

	if len(entries) == 0 {
		cmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "-b", "main", repoURL, repoPath)
		cmd.Env = gitEnv
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git clone failed for %s: %s (%w)", repoPath, SanitizeLog(string(out)), err)
		}
		return nil
	}

	cmdInit := exec.CommandContext(ctx, "git", "-C", repoPath, "init", "-b", "main")
	_ = cmdInit.Run()

	cmdRemote := exec.CommandContext(ctx, "git", "-C", repoPath, "remote", "add", "origin", repoURL)
	_ = cmdRemote.Run()

	cmdFetch := exec.CommandContext(ctx, "git", "-C", repoPath, "fetch", "--depth", "1", "origin", "main")
	cmdFetch.Env = gitEnv
	if out, err := cmdFetch.CombinedOutput(); err != nil {
		return fmt.Errorf("git fetch failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(string(out)), err)
	}

	cmdReset := exec.CommandContext(ctx, "git", "-C", repoPath, "reset", "--soft", "FETCH_HEAD")
	cmdReset.Env = gitEnv
	_ = cmdReset.Run()

	return nil
}

// SyncRepo synchronizes a single repository via git pull --ff-only with fallback to reset --hard FETCH_HEAD.
func (d *SyncDaemon) SyncRepo(ctx context.Context, repoPath string) (res RepoSyncResult) {
	res = RepoSyncResult{Repo: repoPath}

	if repoPath == "" {
		return res
	}

	start := time.Now()
	defer func() {
		status := "success"
		if res.Error != "" {
			status = "error"
		}
		metrics.RecordPull(repoPath, status, res.Changed, time.Since(start))
		if status == "success" {
			now := time.Now()
			metrics.RecordLastSync(repoPath, now)
			d.statusMu.Lock()
			if d.lastSyncTimes == nil {
				d.lastSyncTimes = make(map[string]time.Time)
			}
			d.lastSyncTimes[repoPath] = now
			d.statusMu.Unlock()
		}
	}()

	if repoURL, ok := d.repoUrls[repoPath]; ok && repoURL != "" {
		if err := d.EnsureRepo(ctx, repoPath, repoURL); err != nil {
			log.Printf("[GitSync] Warning: EnsureRepo failed for %s: %v", repoPath, err)
		}
	}

	gitDir, err := resolveGitDir(repoPath)
	if err != nil {
		res.Error = fmt.Sprintf("git dir not found: %v", err)
		return res
	}

	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[GitSync] %s has index.lock present, skipping sync cycle", repoPath)
		res.Error = "index.lock active"
		return res
	}

	opCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()

	gitEnv := buildGitEnv(d.pat)

	cmdSafe := exec.CommandContext(opCtx, "git", "config", "--global", "safe.directory", "*")
	_ = cmdSafe.Run()

	cmdBefore := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	cmdBefore.Env = gitEnv
	outBefore, err := cmdBefore.Output()
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, sanitizedErr)
		res.Error = sanitizedErr
		return res
	}
	res.PreviousHead = strings.TrimSpace(string(outBefore))
	res.CurrentHead = res.PreviousHead

	cmdPull := exec.CommandContext(opCtx, "git", "-C", repoPath, "pull", "--ff-only")
	cmdPull.Env = gitEnv
	outPull, errPull := cmdPull.CombinedOutput()

	if errPull != nil {
		sanitizedOut := SanitizeLog(strings.TrimSpace(string(outPull)))
		sanitizedErr := SanitizeLog(errPull.Error())
		log.Printf("[GitSync] Notice: git pull --ff-only failed for %s (%s, %s). Attempting safe reset recovery...", repoPath, sanitizedErr, sanitizedOut)

		cmdFetch := exec.CommandContext(opCtx, "git", "-C", repoPath, "fetch", "origin", "main")
		cmdFetch.Env = gitEnv
		if outFetch, errFetch := cmdFetch.CombinedOutput(); errFetch == nil {
			cmdReset := exec.CommandContext(opCtx, "git", "-C", repoPath, "reset", "--hard", "FETCH_HEAD")
			cmdReset.Env = gitEnv
			if outReset, errReset := cmdReset.CombinedOutput(); errReset != nil {
				res.Error = fmt.Sprintf("reset failed: %s", SanitizeLog(string(outReset)))
				return res
			}

			cmdClean := exec.CommandContext(opCtx, "git", "-C", repoPath, "clean", "-fd")
			cmdClean.Env = gitEnv
			_ = cmdClean.Run()

			log.Printf("[GitSync] Successfully recovered %s via reset to FETCH_HEAD", repoPath)
		} else {
			res.Error = fmt.Sprintf("pull failed: %s; fetch failed: %s", sanitizedOut, SanitizeLog(strings.TrimSpace(string(outFetch))))
			return res
		}
	}

	cmdAfter := exec.CommandContext(opCtx, "git", "-C", repoPath, "rev-parse", "HEAD")
	cmdAfter.Env = gitEnv
	outAfter, err := cmdAfter.Output()
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		res.Error = sanitizedErr
		return res
	}
	res.CurrentHead = strings.TrimSpace(string(outAfter))

	if res.PreviousHead != res.CurrentHead {
		res.Changed = true
		log.Printf("[GitSync] Repository %s updated: %s -> %s", repoPath, res.PreviousHead, res.CurrentHead)

		if composeChanged, _ := d.HasComposeChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead); composeChanged {
			res.ComposeChanged = true
			log.Printf("[GitSync:GitOps] Infrastructure/compose changes detected in %s (%s -> %s). Triggering debounced reconciliation.", repoPath, res.PreviousHead, res.CurrentHead)
			if d.reconcileCh != nil {
				select {
				case d.reconcileCh <- struct{}{}:
				default:
				}
			}
		}
	}

	return res
}

// TriggerSync runs a synchronous singleflight sync across all managed repositories.
func (d *SyncDaemon) TriggerSync() ([]RepoSyncResult, error) {
	val, err, _ := d.sfg.Do("sync", func() (interface{}, error) {
		d.mu.Lock()
		if d.ticker != nil {
			d.ticker.Reset(d.interval)
		}
		d.mu.Unlock()

		syncCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		results := make([]RepoSyncResult, 0, len(d.repos))
		hasError := false
		anyChanged := false

		for _, repo := range d.repos {
			res := d.SyncRepo(syncCtx, repo)
			if res.Error != "" {
				hasError = true
			}
			if res.Changed {
				anyChanged = true
			}
			results = append(results, res)
		}

		status := "no_change"
		if hasError {
			status = "error"
		} else if anyChanged {
			status = "synced"
		}
		metrics.RecordSyncRequest("periodic", status)

		return results, nil
	})

	if err != nil {
		return nil, err
	}
	return val.([]RepoSyncResult), nil
}

// StartPeriodicLoop runs the background ticker loop.
func (d *SyncDaemon) StartPeriodicLoop(ctx context.Context) {
	d.mu.Lock()
	d.ticker = time.NewTicker(d.interval)
	ticker := d.ticker
	d.mu.Unlock()

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = d.TriggerSync()
			}
		}
	}()
}

// getRepoCommit extracts the commit SHA and author timestamp for a given ref in a repository.
func getRepoCommit(ctx context.Context, repoPath, ref, pat string) (string, *time.Time, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "log", "-1", "--format=%H%x00%aI", ref)
	if pat != "" {
		cmd.Env = buildGitEnv(pat)
	}
	out, err := cmd.Output()
	if err != nil {
		return "", nil, err
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "\x00")
	if len(parts) < 2 {
		return strings.TrimSpace(string(out)), nil, nil
	}
	sha := strings.TrimSpace(parts[0])
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[1]))
	if err != nil {
		return sha, nil, nil
	}
	return sha, &t, nil
}

// GetStatus computes real-time synchronization telemetry across all configured repositories.
func (d *SyncDaemon) GetStatus(ctx context.Context) GitSyncStatusResponse {
	resp := GitSyncStatusResponse{
		Status: "synced",
		Repos:  make(map[string]RepoStatus),
	}

	d.statusMu.RLock()
	syncTimes := make(map[string]time.Time, len(d.lastSyncTimes))
	for k, v := range d.lastSyncTimes {
		syncTimes[k] = v
	}
	d.statusMu.RUnlock()

	var maxLag int64
	var latestSync time.Time
	hasError := false
	hasLag := false

	for _, repo := range d.repos {
		st := RepoStatus{
			Repo:       repo,
			SyncStatus: "synced",
		}

		if t, ok := syncTimes[repo]; ok {
			st.LastSyncTime = t
			if t.After(latestSync) {
				latestSync = t
			}
		}

		// Check disk HEAD
		diskSha, diskTime, err := getRepoCommit(ctx, repo, "HEAD", d.pat)
		if err != nil {
			st.SyncStatus = "error"
			st.Error = fmt.Sprintf("failed to get disk HEAD: %v", SanitizeLog(err.Error()))
			hasError = true
			resp.Repos[repo] = st
			continue
		}
		st.DiskCommit = diskSha
		st.DiskCommitTime = diskTime

		// Check remote commit: try origin/main, fallback to FETCH_HEAD
		remoteSha, remoteTime, err := getRepoCommit(ctx, repo, "origin/main", d.pat)
		if err != nil || remoteSha == "" {
			remoteSha, remoteTime, err = getRepoCommit(ctx, repo, "FETCH_HEAD", d.pat)
		}
		if err != nil || remoteSha == "" {
			remoteSha = diskSha
			remoteTime = diskTime
		}
		st.RemoteCommit = remoteSha
		st.RemoteCommitTime = remoteTime

		if diskTime != nil && remoteTime != nil {
			lag := int64(remoteTime.Sub(*diskTime).Seconds())
			if lag < 0 {
				lag = 0
			}
			st.TimeLagSeconds = lag
			if lag > maxLag {
				maxLag = lag
			}
			if diskSha != remoteSha && lag > 0 {
				st.SyncStatus = "lagging"
				hasLag = true
			}
		}

		resp.Repos[repo] = st
	}

	resp.MaxLagSeconds = maxLag
	resp.LastSyncTime = latestSync
	if resp.LastSyncTime.IsZero() {
		resp.LastSyncTime = time.Now()
	}

	if hasError {
		resp.Status = "error"
	} else if hasLag {
		resp.Status = "lagging"
	} else {
		resp.Status = "synced"
	}

	return resp
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	rawRepos := os.Getenv("SYNC_REPOS")
	if rawRepos == "" {
		rawRepos = "/share/aerial-config,/share/aerial"
	}

	var repos []string
	for _, r := range strings.Split(rawRepos, ",") {
		r = strings.TrimSpace(r)
		if r != "" {
			repos = append(repos, r)
		}
	}

	intervalStr := os.Getenv("SYNC_INTERVAL")
	interval := 60 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d > 0 {
			interval = d
		}
	}

	pat := os.Getenv("GITHUB_PAT")
	configRepoURL := os.Getenv("AERIAL_CONFIG_REPO_URL")
	if configRepoURL == "" {
		configRepoURL = "https://github.com/azylman/aerial-config.git"
	}

	composeDir := os.Getenv("AERIAL_PROJECT_DIR")
	if composeDir == "" {
		composeDir = "/share/aerial"
	}

	configDir := os.Getenv("AERIAL_CONFIG_DIR")
	if configDir == "" {
		configDir = "/share/aerial-config"
	}

	repoUrls := map[string]string{
		configDir:  configRepoURL,
		composeDir: "https://github.com/azylman/aerial.git",
	}

	daemon := &SyncDaemon{
		repos:       repos,
		repoUrls:    repoUrls,
		interval:    interval,
		pat:         pat,
		composeDir:  composeDir,
		configDir:   configDir,
		reconcileCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon.StartPeriodicLoop(ctx)
	daemon.StartReconcilerLoop(ctx)
	log.Printf("[GitSync] Sidecar GitOps daemon started on :%s (interval: %v, repos: %v, composeDir: %s)", port, interval, repos, composeDir)

	mux := http.NewServeMux()

	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		status := daemon.GetStatus(ctx)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		results, err := daemon.TriggerSync()
		if err != nil {
			metrics.RecordSyncRequest("webhook", "error")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		metrics.RecordSyncRequest("webhook", "synced")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "synced",
			"results": results,
		})
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[GitSync] HTTP server fatal error: %v", err)
		}
	}()

	<-stopChan
	log.Println("[GitSync] Shutting down GitSync sidecar gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[GitSync] HTTP server shutdown error: %v", err)
	}
	log.Println("[GitSync] GitSync sidecar stopped cleanly")
}
