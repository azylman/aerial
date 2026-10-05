package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime/coverage"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/sidecars/hangar/pkg/metrics"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v3"
)

var sanitizePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:basic\s+[a-zA-Z0-9+/]{8,}={1,2}|basic\s+[a-zA-Z0-9+/]{10,}|basic\s+[a-zA-Z0-9+/=]+|bearer\s+[a-zA-Z0-9_\-\.]{12,})`),
	regexp.MustCompile(`(?i)x-access-token:[^@\s]+`),
	regexp.MustCompile(`(?i)https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/\d+/[a-zA-Z0-9_-]+`),
	regexp.MustCompile(`(?i)(?:gh[pousr]_[a-zA-Z0-9_]+|github_pat_[a-zA-Z0-9_]+)`),
	regexp.MustCompile(`\bsk-(?:proj-|ant-|svcacct-)?[a-zA-Z0-9_-]{16,}`),
	regexp.MustCompile(`(?i)(?:AIza[0-9A-Za-z-_]{35,}|(?:antigravity|gemini)_[a-zA-Z0-9_\-]{16,})`),
	regexp.MustCompile(`\b(?:mfa\.[a-zA-Z0-9_-]{20,}|[a-zA-Z0-9_-]{24,28}\.[a-zA-Z0-9_-]{6}\.[a-zA-Z0-9_-]{27,38})`),
	regexp.MustCompile(`\beyJ[a-zA-Z0-9_-]{10,}\.eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}`),
	regexp.MustCompile(`postgres://[^:]+:[^@]+@[^/]+/[^?]+`),
}

func closeWarn(closer io.Closer, name string) {
	if closer != nil {
		if err := closer.Close(); err != nil {
			log.Printf("[Hangar] Warning closing %s: %v", name, err)
		}
	}
}

func isClientDisconnect(r *http.Request, err error) bool {
	if err == nil {
		return false
	}
	if r != nil && r.Context().Err() != nil {
		return true
	}
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed)
}

func writeResponse(w http.ResponseWriter, r *http.Request, data []byte) {
	if _, err := w.Write(data); err != nil {
		if !isClientDisconnect(r, err) {
			log.Printf("[Hangar:HTTP] Failed to write response: %v", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("[Hangar:HTTP] Failed to marshal JSON response: %v", err)
		http.Error(w, `{"error":"Internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	writeResponse(w, r, b)
}

// SanitizeLog scrubs sensitive tokens from error and log messages.
func SanitizeLog(input string) string {
	out := input
	for _, re := range sanitizePatterns {
		out = re.ReplaceAllString(out, "[REDACTED_TOKEN]")
	}
	return out
}

// SanitizeAll scrubs pattern-matched tokens as well as known literal secret values.
func (d *SyncDaemon) SanitizeAll(input string) string {
	out := SanitizeLog(input)
	secrets := append([]string{
		d.pat,
		d.discordToken,
		d.discordWebhookURL,
	}, d.extraSecrets...)
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if len(s) >= 4 {
			out = strings.ReplaceAll(out, s, "[REDACTED_SECRET]")
		}
	}
	return out
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
	Repo      string
	CommitSHA string
	PRNumber  int
	TargetID  string
}

// HangarStatusResponse is the aggregated telemetry payload returned by GET /status.
type HangarStatusResponse struct {
	Status         string                `json:"status"` // "synced", "lagging", "error"
	MaxLagSeconds  int64                 `json:"max_lag_seconds"`
	LastSyncTime   time.Time             `json:"last_sync_time"`
	Repos          map[string]RepoStatus `json:"repos"`
	Reconciliation *ReconciliationStatus `json:"reconciliation,omitempty"`
}

// GitSyncStatusResponse is an alias for backward compatibility.
type GitSyncStatusResponse = HangarStatusResponse

// DockerExecutor executes docker CLI commands.
type DockerExecutor func(ctx context.Context, args ...string) (stdout []byte, stderr []byte, err error)

func defaultDockerExecutor(ctx context.Context, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				return cmd.Process.Kill()
			}
			return nil
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// scrubComposeEnv filters out container-internal path overrides (AERIAL_CONFIG_DIR, AERIAL_PROJECT_DIR)
// so docker compose does not inherit container filesystem paths for host volume mounts.
func scrubComposeEnv(environ []string) []string {
	return ScrubComposeEnv(environ)
}

// ComposeExecutor executes docker compose commands.
type ComposeExecutor func(ctx context.Context, dir string, args ...string) (stdout []byte, stderr []byte, err error)

func defaultComposeExecutor(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
	cmdArgs := append([]string{"compose"}, args...)
	cmd := exec.CommandContext(ctx, "docker", cmdArgs...)
	cmd.Dir = dir
	cmd.Env = scrubComposeEnv(os.Environ())
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				return cmd.Process.Kill()
			}
			return nil
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// GitExecutor executes git CLI commands.
type GitExecutor func(ctx context.Context, dir string, args ...string) (stdout []byte, stderr []byte, err error)

// NomadExecutor executes nomad CLI commands.
type NomadExecutor func(ctx context.Context, args ...string) (stdout []byte, stderr []byte, err error)

func defaultNomadExecutor(nomadAddr, nomadToken string) NomadExecutor {
	return func(ctx context.Context, args ...string) ([]byte, []byte, error) {
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		cmd := exec.CommandContext(opCtx, "nomad", args...)
		cmd.Env = scrubComposeEnv(os.Environ())
		if nomadAddr != "" {
			cmd.Env = append(cmd.Env, "NOMAD_ADDR="+nomadAddr)
		}
		if nomadToken != "" {
			cmd.Env = append(cmd.Env, "NOMAD_TOKEN="+nomadToken)
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
		cmd.WaitDelay = 5 * time.Second

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		sanitizedStderr := SanitizeLog(stderr.String())
		if nomadToken != "" {
			sanitizedStderr = strings.ReplaceAll(sanitizedStderr, nomadToken, "[REDACTED_NOMAD_TOKEN]")
		}
		return stdout.Bytes(), []byte(sanitizedStderr), err
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
	cmd := exec.CommandContext(ctx, gitBin, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = BuildGitEnv(pat, os.Environ())
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				return cmd.Process.Kill()
			}
			return nil
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
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
	lastReconcile time.Time
	lastSyncTimes   map[string]time.Time
	repoGitHubSlugs map[string]string
	statusMu        sync.RWMutex
	triggerFn     func() ([]RepoSyncResult, error)
	reconcileFn   func(ctx context.Context) error

	brainInternalURL string
	extraSecrets     []string
	composeExecutor  ComposeExecutor
	dockerExecutor   DockerExecutor
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
	reconcileStateMu            sync.RWMutex
	reconcileStatus             ReconciliationStatus

	// HTTP & Registry
	registryClient *http.Client

	// Discord Alerting
	discordToken      string
	discordChannel    string
	discordWebhookURL string
	discordChannelMu  sync.RWMutex
	cachedChannelID   string
	alertDedupeMu     sync.Mutex
	lastAlertTimes    map[string]time.Time
	alertHTTPClient   *http.Client

	// Webhooks & Deploy Lifecycle
	webhooksRouterURL      string
	deployDispatcher       func(ctx context.Context, evt HangarDeployEvent) error

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

func (d *SyncDaemon) getDockerExecutor() DockerExecutor {
	if d != nil && d.dockerExecutor != nil {
		return d.dockerExecutor
	}
	return defaultDockerExecutor
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
	return d.defaultDeployDispatcher
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
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("deploy event post failed: %s (%w)", SanitizeLog(err.Error()), err)
	}
	defer closeWarn(resp.Body, "deploy event response body")

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

func (d *SyncDaemon) updateReconcileStatus(fn func(*ReconciliationStatus)) {
	if d == nil || fn == nil {
		return
	}
	d.reconcileStateMu.Lock()
	defer d.reconcileStateMu.Unlock()
	fn(&d.reconcileStatus)
}

// GetReconciliationStatus returns a safe snapshot of the current or recent reconciliation status.
func (d *SyncDaemon) GetReconciliationStatus() ReconciliationStatus {
	if d == nil {
		return ReconciliationStatus{State: "idle", Stage: "idle"}
	}
	d.reconcileStateMu.RLock()
	defer d.reconcileStateMu.RUnlock()

	cp := d.reconcileStatus
	// Check completion TTL (5 minutes grace period before reverting to idle)
	if !cp.Active && (cp.State == "healthy" || cp.State == "failed") {
		if !cp.CompletedAt.IsZero() && time.Since(cp.CompletedAt) > 5*time.Minute {
			return ReconciliationStatus{State: "idle", Stage: "idle"}
		}
	}
	if len(d.reconcileStatus.TargetServices) > 0 {
		cp.TargetServices = make([]string, len(d.reconcileStatus.TargetServices))
		copy(cp.TargetServices, d.reconcileStatus.TargetServices)
	}
	if cp.State == "" {
		cp.State = "idle"
	}
	if cp.Stage == "" {
		cp.Stage = cp.State
	}
	return cp
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

// HasNomadConfigChanges checks whether Nomad daemon configuration files changed between commits.
func (d *SyncDaemon) HasNomadConfigChanges(ctx context.Context, repoPath, prevHead, currHead string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return false, nil
	}
	if repoPath == "" {
		return false, nil
	}

	args := []string{"diff", "--name-only", prevHead, currHead, "--", "nomad"}
	stdout, _, err := d.getGitExecutor()(ctx, repoPath, args...)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		fallbackArgs := []string{"diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD", "--", "nomad"}
		stdoutFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, fallbackArgs...)
		if errFallback != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			log.Printf("[Hangar] Warning: Failed to inspect nomad diff in %s (%v); failing safe to false", repoPath, errFallback)
			return false, nil
		}
		lines := strings.Split(strings.TrimSpace(string(stdoutFallback)), "\n")
		return len(FilterNomadConfigFiles(lines)) > 0, nil
	}

	lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
	return len(FilterNomadConfigFiles(lines)) > 0, nil
}

// HasNomadChanges checks whether any Nomad job specifications changed between commits.
func (d *SyncDaemon) HasNomadChanges(ctx context.Context, repoPath, prevHead, currHead string) ([]NomadFileChange, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return nil, nil
	}
	if repoPath == "" {
		return nil, nil
	}

	args := []string{"diff", "--name-status", prevHead, currHead, "--"}
	stdout, _, err := d.getGitExecutor()(ctx, repoPath, args...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		fallbackArgs := []string{"diff-tree", "--no-commit-id", "--name-status", "-r", "HEAD", "--"}
		stdoutFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, fallbackArgs...)
		if errFallback != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			log.Printf("[Hangar:Nomad] Warning: Failed to inspect diff in %s (%v)", repoPath, errFallback)
			return nil, nil
		}
		return ParseNomadGitStatus(string(stdoutFallback)), nil
	}

	return ParseNomadGitStatus(string(stdout)), nil
}

func (d *SyncDaemon) recordPendingNomadChanges(repoPath string, changes []NomadFileChange, meta ...string) {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	var repo, commit, targetID string
	var prNumber int
	if len(meta) >= 1 {
		repo = meta[0]
	}
	if len(meta) >= 2 {
		commit = meta[1]
	}
	if len(meta) >= 3 {
		targetID = meta[2]
	}
	if len(meta) >= 4 {
		if parsed, err := strconv.Atoi(meta[3]); err == nil {
			prNumber = parsed
		}
	}
	d.pendingNomadChanges = append(d.pendingNomadChanges, NomadChangeEvent{
		RepoPath:  repoPath,
		Changes:   changes,
		Repo:      repo,
		CommitSHA: commit,
		PRNumber:  prNumber,
		TargetID:  targetID,
		Timestamp: time.Now(),
	})
}

func (d *SyncDaemon) drainPendingNomadChanges() []NomadChangeEvent {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	out := d.pendingNomadChanges
	d.pendingNomadChanges = nil
	return out
}

// getComposeArgs constructs the compose CLI flags with base docker-compose.yml and any present overrides.
func (d *SyncDaemon) getComposeArgs(composeDir string, subCmd ...string) []string {
	baseFile := filepath.Join(composeDir, "docker-compose.yml")
	args := []string{
		"--project-name", "aerial",
		"--project-directory", composeDir,
		"-f", baseFile,
	}

	envFile := filepath.Join(composeDir, ".env")
	if fi, err := os.Stat(envFile); err == nil && !fi.IsDir() {
		args = append(args, "--env-file", envFile)
	}

	configDir := d.configDir

	var overrideCandidates []string
	overrideCandidates = append(overrideCandidates,
		filepath.Join(composeDir, "docker-compose.override.yml"),
		filepath.Join(composeDir, "docker-compose.override.yaml"),
	)
	if configDir != "" {
		overrideCandidates = append(overrideCandidates,
			filepath.Join(configDir, "docker-compose.override.yml"),
			filepath.Join(configDir, "docker-compose.override.yaml"),
		)
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


var sha256DigestRegex = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

const acceptManifestHeaders = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json"

// GetRemoteImageDigest performs an HTTP HEAD query against the target container registry
// to obtain the current manifest digest, authenticating via OAuth2 / Bearer challenge if required.
func (d *SyncDaemon) GetRemoteImageDigest(ctx context.Context, imageRef string) (digest string, err error) {
	start := time.Now()
	var registry string
	defer func() {
		metrics.RecordRegistryManifest(registry, metrics.SanitizeStatus(err), time.Since(start))
	}()

	var repository, tag string
	registry, repository, tag, err = ParseImageReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}

	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, tag)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create manifest request for %s: %w", imageRef, err)
	}
	req.Header.Set("Accept", acceptManifestHeaders)

	resp, err := d.getRegistryClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("manifest HEAD request failed for %s: %w", imageRef, err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		authHeader := resp.Header.Get("Www-Authenticate")
		closeWarn(resp.Body, "unauthorized manifest response body")

		realm, service, scope, pErr := ParseWwwAuthenticate(authHeader)
		if pErr != nil {
			return "", fmt.Errorf("failed parsing Www-Authenticate header for %s: %w", imageRef, pErr)
		}
		if scope == "" {
			scope = fmt.Sprintf("repository:%s:pull", repository)
		}

		tokURL, uErr := url.Parse(realm)
		if uErr != nil {
			return "", fmt.Errorf("failed parsing token realm %q: %w", realm, uErr)
		}
		q := tokURL.Query()
		if service != "" {
			q.Set("service", service)
		}
		if scope != "" {
			q.Set("scope", scope)
		}
		tokURL.RawQuery = q.Encode()

		tokReq, tErr := http.NewRequestWithContext(ctx, http.MethodGet, tokURL.String(), nil)
		if tErr != nil {
			return "", fmt.Errorf("failed creating token request for %s: %w", imageRef, tErr)
		}

		if registry == "ghcr.io" && d != nil && strings.TrimSpace(d.pat) != "" {
			username := ResolveRegistryUser("", os.Getenv)
			tokReq.SetBasicAuth(username, d.pat)
		}

		tokResp, dErr := d.getRegistryClient().Do(tokReq)
		if dErr != nil {
			return "", fmt.Errorf("token request failed for %s: %w", imageRef, dErr)
		}
		defer tokResp.Body.Close()

		if tokResp.StatusCode != http.StatusOK {
			closeWarn(tokResp.Body, "token error response body")
			return "", fmt.Errorf("token request for %s returned HTTP %d", imageRef, tokResp.StatusCode)
		}

		var tokPayload struct {
			Token       string `json:"token"`
			AccessToken string `json:"access_token"`
		}
		decErr := json.NewDecoder(tokResp.Body).Decode(&tokPayload)
		closeWarn(tokResp.Body, "token response body")
		if decErr != nil {
			return "", fmt.Errorf("failed decoding token for %s: %w", imageRef, decErr)
		}

		bearerToken := tokPayload.Token
		if bearerToken == "" {
			bearerToken = tokPayload.AccessToken
		}
		if bearerToken == "" {
			return "", fmt.Errorf("empty bearer token returned for %s", imageRef)
		}

		authReq, aErr := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
		if aErr != nil {
			return "", fmt.Errorf("failed creating authenticated manifest request for %s: %w", imageRef, aErr)
		}
		authReq.Header.Set("Accept", acceptManifestHeaders)
		authReq.Header.Set("Authorization", "Bearer "+bearerToken)

		resp, err = d.getRegistryClient().Do(authReq)
		if err != nil {
			return "", fmt.Errorf("authenticated manifest HEAD request failed for %s: %w", imageRef, err)
		}
	}

	defer closeWarn(resp.Body, "manifest response body")

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("manifest query for %s returned HTTP %d", imageRef, resp.StatusCode)
	}

	digest = strings.TrimSpace(resp.Header.Get("Docker-Content-Digest"))
	if digest == "" {
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		etag = strings.TrimPrefix(etag, "W/")
		etag = strings.Trim(etag, "\"")
		if sha256DigestRegex.MatchString(etag) {
			digest = etag
		}
	}

	if digest == "" || !sha256DigestRegex.MatchString(digest) {
		return "", fmt.Errorf("no valid digest returned for %s (Docker-Content-Digest: %q, ETag: %q)",
			imageRef, resp.Header.Get("Docker-Content-Digest"), resp.Header.Get("ETag"))
	}

	return digest, nil
}

// fetchRegistryWithAuth performs an HTTP request against a container registry, handling OAuth2/Bearer
// token challenges automatically and reusing existingBearer if still valid.
func (d *SyncDaemon) fetchRegistryWithAuth(ctx context.Context, targetURL, registry, repository, acceptHeader, existingBearer string) (*http.Response, string, error) {
	var resp *http.Response

	if existingBearer != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, "", fmt.Errorf("failed creating authenticated request for %s: %w", targetURL, err)
		}
		if acceptHeader != "" {
			req.Header.Set("Accept", acceptHeader)
		}
		req.Header.Set("Authorization", "Bearer "+existingBearer)

		resp, err = d.getRegistryClient().Do(req)
		if err != nil {
			return nil, "", fmt.Errorf("authenticated request failed for %s: %w", targetURL, err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			return resp, existingBearer, nil
		}
		closeWarn(resp.Body, "unauthorized response body with existing bearer")
	} else {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, "", fmt.Errorf("failed creating probe request for %s: %w", targetURL, err)
		}
		if acceptHeader != "" {
			req.Header.Set("Accept", acceptHeader)
		}

		resp, err = d.getRegistryClient().Do(req)
		if err != nil {
			return nil, "", fmt.Errorf("probe request failed for %s: %w", targetURL, err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			return resp, "", nil
		}
		closeWarn(resp.Body, "unauthorized probe response body")
	}

	authHeader := resp.Header.Get("Www-Authenticate")
	realm, service, scope, pErr := ParseWwwAuthenticate(authHeader)
	if pErr != nil {
		return nil, "", fmt.Errorf("failed parsing Www-Authenticate header for %s: %w", targetURL, pErr)
	}
	if scope == "" {
		scope = fmt.Sprintf("repository:%s:pull", repository)
	}

	tokURL, uErr := url.Parse(realm)
	if uErr != nil {
		return nil, "", fmt.Errorf("failed parsing token realm %q: %w", realm, uErr)
	}
	q := tokURL.Query()
	if service != "" {
		q.Set("service", service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	tokURL.RawQuery = q.Encode()

	tokReq, tErr := http.NewRequestWithContext(ctx, http.MethodGet, tokURL.String(), nil)
	if tErr != nil {
		return nil, "", fmt.Errorf("failed creating token request for %s: %w", targetURL, tErr)
	}

	if registry == "ghcr.io" && d != nil && strings.TrimSpace(d.pat) != "" {
		username := ResolveRegistryUser("", os.Getenv)
		tokReq.SetBasicAuth(username, d.pat)
	}

	tokResp, dErr := d.getRegistryClient().Do(tokReq)
	if dErr != nil {
		return nil, "", fmt.Errorf("token request failed for %s: %w", targetURL, dErr)
	}
	defer closeWarn(tokResp.Body, "token response body")

	if tokResp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("token request for %s returned HTTP %d", targetURL, tokResp.StatusCode)
	}

	var tokPayload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	decErr := json.NewDecoder(tokResp.Body).Decode(&tokPayload)
	if decErr != nil {
		return nil, "", fmt.Errorf("failed decoding token for %s: %w", targetURL, decErr)
	}

	token := tokPayload.Token
	if token == "" {
		token = tokPayload.AccessToken
	}
	if token == "" {
		return nil, "", fmt.Errorf("empty bearer token returned for %s", targetURL)
	}

	authReq, aErr := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if aErr != nil {
		return nil, "", fmt.Errorf("failed creating final authenticated request for %s: %w", targetURL, aErr)
	}
	if acceptHeader != "" {
		authReq.Header.Set("Accept", acceptHeader)
	}
	authReq.Header.Set("Authorization", "Bearer "+token)

	finalResp, fErr := d.getRegistryClient().Do(authReq)
	if fErr != nil {
		return nil, "", fmt.Errorf("authenticated request failed for %s: %w", targetURL, fErr)
	}

	return finalResp, token, nil
}

// GetRemoteImageRevision retrieves the git commit revision associated with an OCI/Docker container image
// by inspecting manifest annotations or configuration blob labels.
func (d *SyncDaemon) GetRemoteImageRevision(ctx context.Context, imageRef string) (string, error) {
	registry, repository, tagOrDigest, err := ParseImageReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}

	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, tagOrDigest)
	resp, bearerToken, err := d.fetchRegistryWithAuth(ctx, manifestURL, registry, repository, acceptManifestHeaders, "")
	if err != nil {
		return "", fmt.Errorf("failed to fetch manifest for %s: %w", imageRef, err)
	}
	defer closeWarn(resp.Body, "manifest response body")

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("manifest request for %s returned HTTP %d", imageRef, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("failed to read manifest body for %s: %w", imageRef, err)
	}

	// Check if this is an OCI Image Index or Docker Manifest List
	childDigest := ExtractPlatformManifestDigest(bodyBytes, "linux", "amd64")
	if childDigest != "" {
		if rev := ExtractRevisionFromAnnotations(ExtractManifestAnnotations(bodyBytes)); rev != "" {
			return rev, nil
		}

		childURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, childDigest)
		childResp, childToken, childErr := d.fetchRegistryWithAuth(ctx, childURL, registry, repository, acceptManifestHeaders, bearerToken)
		if childErr != nil {
			return "", fmt.Errorf("failed to fetch child manifest %s for %s: %w", childDigest, imageRef, childErr)
		}
		defer closeWarn(childResp.Body, "child manifest response body")

		if childResp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("child manifest request for %s (%s) returned HTTP %d", imageRef, childDigest, childResp.StatusCode)
		}

		childBytes, readErr := io.ReadAll(io.LimitReader(childResp.Body, 1<<20))
		if readErr != nil {
			return "", fmt.Errorf("failed to read child manifest body for %s: %w", imageRef, readErr)
		}

		bodyBytes = childBytes
		if childToken != "" {
			bearerToken = childToken
		}
	}

	// Single manifest processing:
	// 1. Check annotations on manifest
	if rev := ExtractRevisionFromAnnotations(ExtractManifestAnnotations(bodyBytes)); rev != "" {
		return rev, nil
	}

	// 2. Extract config blob digest and fetch config blob
	configDigest := ExtractConfigBlobDigest(bodyBytes)
	if configDigest == "" {
		return "", fmt.Errorf("no config digest or revision annotations found for %s", imageRef)
	}

	blobURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", registry, repository, configDigest)
	const acceptBlobHeaders = "application/vnd.oci.image.config.v1+json, application/vnd.docker.container.image.v1+json, application/octet-stream"
	blobResp, _, blobErr := d.fetchRegistryWithAuth(ctx, blobURL, registry, repository, acceptBlobHeaders, bearerToken)
	if blobErr != nil {
		return "", fmt.Errorf("failed to fetch config blob %s for %s: %w", configDigest, imageRef, blobErr)
	}
	defer closeWarn(blobResp.Body, "config blob response body")

	if blobResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("config blob request for %s (%s) returned HTTP %d", imageRef, configDigest, blobResp.StatusCode)
	}

	blobBytes, readErr := io.ReadAll(io.LimitReader(blobResp.Body, 1<<20))
	if readErr != nil {
		return "", fmt.Errorf("failed to read config blob body for %s: %w", imageRef, readErr)
	}

	rev := ExtractRevisionFromConfigJSON(blobBytes)
	if rev == "" {
		return "", fmt.Errorf("no valid revision label found in config blob for %s", imageRef)
	}

	return rev, nil
}

// CheckAndReconcileNewImages checks if any Nomad jobs have newer images available in their registry,
// and triggers an asynchronous, debounced reconciliation if new images are detected.
func (d *SyncDaemon) CheckAndReconcileNewImages(ctx context.Context) error {
	return d.CheckAndReconcileNomadImages(ctx)
}


var snowflakeRegex = regexp.MustCompile(`^\d{17,20}$`)

func isSnowflake(str string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(str))
}

func (d *SyncDaemon) getRepoLock(repoPath string) *sync.Mutex {
	cleaned := filepath.Clean(repoPath)
	d.repoLocksMu.Lock()
	defer d.repoLocksMu.Unlock()
	if d.repoLocks == nil {
		d.repoLocks = make(map[string]*sync.Mutex)
	}
	l, exists := d.repoLocks[cleaned]
	if !exists {
		l = &sync.Mutex{}
		d.repoLocks[cleaned] = l
	}
	return l
}

func (d *SyncDaemon) recordPendingNomadServerRestart() {
	if d == nil {
		return
	}
	d.pendingNomadServerRestartMu.Lock()
	defer d.pendingNomadServerRestartMu.Unlock()
	d.pendingNomadServerRestart = true
}

func (d *SyncDaemon) consumePendingNomadServerRestart() bool {
	if d == nil {
		return false
	}
	d.pendingNomadServerRestartMu.Lock()
	defer d.pendingNomadServerRestartMu.Unlock()
	restart := d.pendingNomadServerRestart
	d.pendingNomadServerRestart = false
	return restart
}


type rawSystemConfig struct {
	SystemChannel string `yaml:"system_channel"`
}

// ResolveAlertChannel dynamically resolves the Discord alert channel name or snowflake ID.
// Precedence:
// 1. Explicit DiscordChannel configured on daemon (if non-empty)
// 2. system_channel from config.yaml in configDir (/share/aerial-config/config.yaml)
// 3. Fallback to "aerial-dev"
func (d *SyncDaemon) ResolveAlertChannel() string {
	if ch := strings.TrimSpace(d.discordChannel); ch != "" {
		return ch
	}
	configDir := d.configDir
	if configDir != "" {
		configPath := filepath.Join(configDir, "config.yaml")
		if data, err := os.ReadFile(configPath); err == nil {
			var raw rawSystemConfig
			if err := yaml.Unmarshal(data, &raw); err == nil && strings.TrimSpace(raw.SystemChannel) != "" {
				return strings.TrimSpace(raw.SystemChannel)
			}
		}
	}
	return "aerial-dev"
}

// SendDiscordAlert dispatches an alert message to Discord with deduplication and bounded timeout.
func (d *SyncDaemon) SendDiscordAlert(ctx context.Context, title, repoPath, faultyCommit, rolledBackTo, stage, errorMsg string) {
	dedupeKey := fmt.Sprintf("%s:%s:%s", repoPath, faultyCommit, stage)
	d.alertDedupeMu.Lock()
	if d.lastAlertTimes == nil {
		d.lastAlertTimes = make(map[string]time.Time)
	}
	lastSent, exists := d.lastAlertTimes[dedupeKey]
	if exists && time.Since(lastSent) < 15*time.Minute {
		d.alertDedupeMu.Unlock()
		log.Printf("[Hangar:Discord] Suppressing duplicate alert for %s (sent %v ago)", dedupeKey, time.Since(lastSent).Truncate(time.Second))
		return
	}
	d.lastAlertTimes[dedupeKey] = time.Now()
	d.alertDedupeMu.Unlock()

	sanitizedErr := d.SanitizeAll(strings.TrimSpace(errorMsg))
	content := BuildDiscordAlertContent(title, repoPath, faultyCommit, rolledBackTo, stage, sanitizedErr)

	client := d.alertHTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	alertCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if webhookURL := strings.TrimSpace(d.discordWebhookURL); webhookURL != "" {
		d.sendWebhook(alertCtx, client, webhookURL, content)
		return
	}

	token := strings.TrimSpace(d.discordToken)
	if token == "" {
		log.Printf("[Hangar:Discord] Notice: No DISCORD_BOT_TOKEN or DISCORD_WEBHOOK_URL configured, skipping alert")
		return
	}

	targetChannel := d.ResolveAlertChannel()
	channelID, err := d.resolveChannelID(alertCtx, client, token, targetChannel)
	if err != nil {
		log.Printf("[Hangar:Discord] Warning: Failed to resolve channel %q: %s", targetChannel, d.SanitizeAll(err.Error()))
		return
	}

	d.postChannelMessage(alertCtx, client, token, channelID, content)
}

func (d *SyncDaemon) sendWebhook(ctx context.Context, client *http.Client, webhookURL, content string) {
	payloadBytes, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		log.Printf("[Hangar:Discord] Error marshaling webhook payload: %s", d.SanitizeAll(err.Error()))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Printf("[Hangar:Discord] Error creating webhook request: %s", d.SanitizeAll(err.Error()))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Hangar:Discord] Webhook dispatch failed: %s", d.SanitizeAll(err.Error()))
		return
	}
	defer closeWarn(resp.Body, "webhook response body")
	if resp.StatusCode >= 400 {
		log.Printf("[Hangar:Discord] Webhook returned HTTP %d", resp.StatusCode)
	} else {
		log.Printf("[Hangar:Discord] Successfully dispatched alert via webhook")
	}
}

func (d *SyncDaemon) resolveChannelID(ctx context.Context, client *http.Client, token, targetChannel string) (string, error) {
	targetChannel = strings.TrimSpace(targetChannel)
	if isSnowflake(targetChannel) {
		return targetChannel, nil
	}

	d.discordChannelMu.RLock()
	cached := d.cachedChannelID
	d.discordChannelMu.RUnlock()
	if cached != "" {
		return cached, nil
	}

	cleanName := strings.TrimPrefix(targetChannel, "#")
	authHeader := "Bot " + strings.TrimPrefix(token, "Bot ")

	reqGuilds, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://discord.com/api/v10/users/@me/guilds", nil)
	if err != nil {
		return "", fmt.Errorf("failed to create guilds request: %w", err)
	}
	reqGuilds.Header.Set("Authorization", authHeader)
	respGuilds, err := client.Do(reqGuilds)
	if err != nil {
		return "", fmt.Errorf("failed to fetch guilds: %w", err)
	}
	defer closeWarn(respGuilds.Body, "guilds response body")

	if respGuilds.StatusCode >= 400 {
		return "", fmt.Errorf("failed to fetch guilds: HTTP %d", respGuilds.StatusCode)
	}

	var guilds []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(respGuilds.Body).Decode(&guilds); err != nil {
		return "", fmt.Errorf("failed to decode guilds: %w", err)
	}

	for _, g := range guilds {
		url := fmt.Sprintf("https://discord.com/api/v10/guilds/%s/channels", g.ID)
		reqChans, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		reqChans.Header.Set("Authorization", authHeader)
		respChans, err := client.Do(reqChans)
		if err != nil {
			continue
		}
		var channels []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Type int    `json:"type"`
		}
		decErr := json.NewDecoder(respChans.Body).Decode(&channels)
		closeWarn(respChans.Body, "channels response body")
		respChans.Body.Close()
		if decErr != nil {
			continue
		}

		for _, ch := range channels {
			if strings.EqualFold(ch.Name, cleanName) {
				d.discordChannelMu.Lock()
				d.cachedChannelID = ch.ID
				d.discordChannelMu.Unlock()
				return ch.ID, nil
			}
		}
	}

	return "", fmt.Errorf("channel %q not found in connected guilds", targetChannel)
}

func (d *SyncDaemon) postChannelMessage(ctx context.Context, client *http.Client, token, channelID, content string) {
	url := fmt.Sprintf("https://discord.com/api/v10/channels/%s/messages", channelID)
	payloadBytes, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		log.Printf("[Hangar:Discord] Error marshaling channel message payload: %s", d.SanitizeAll(err.Error()))
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Printf("[Hangar:Discord] Error creating message request: %s", d.SanitizeAll(err.Error()))
		return
	}
	req.Header.Set("Authorization", "Bot "+strings.TrimPrefix(token, "Bot "))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Hangar:Discord] Channel message dispatch failed: %s", d.SanitizeAll(err.Error()))
		return
	}
	defer closeWarn(resp.Body, "channel message response body")

	if resp.StatusCode == http.StatusNotFound {
		d.discordChannelMu.Lock()
		d.cachedChannelID = ""
		d.discordChannelMu.Unlock()
		log.Printf("[Hangar:Discord] Channel %s returned 404, invalidated channel cache", channelID)
	} else if resp.StatusCode >= 400 {
		log.Printf("[Hangar:Discord] Failed to post message to channel %s: HTTP %d", channelID, resp.StatusCode)
	} else {
		log.Printf("[Hangar:Discord] Successfully dispatched alert to channel %s", channelID)
	}
}

// TriggerReconcile requests reconciliation of pending Nomad jobs. If async is true, it queues
// reconciliation onto the debounced worker channel. If async is false, it executes
// ReconcilePendingNomad synchronously.
func (d *SyncDaemon) TriggerReconcile(ctx context.Context, async bool) error {
	if !async {
		if d.reconcileFn != nil {
			return d.reconcileFn(ctx)
		}
		return d.ReconcilePendingNomad(ctx)
	}
	d.mu.Lock()
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}
	ch := d.reconcileCh
	d.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

var (
	reconcilerDebounceDuration = 5 * time.Second
	reconcilerMinCooldown      = 15 * time.Second
)

// ValidateNomadJob validates a Nomad job specification using nomad job validate.
func (d *SyncDaemon) ValidateNomadJob(ctx context.Context, jobPath string) error {
	valCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stdout, stderr, err := d.getNomadExecutor()(valCtx, "job", "validate", jobPath)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("nomad job validate failed for %s: %s (%w)", jobPath, SanitizeLog(strings.TrimSpace(combined)), err)
	}
	return nil
}

// ValidateNomadConfig validates a Nomad daemon configuration path using nomad config validate.
func (d *SyncDaemon) ValidateNomadConfig(ctx context.Context, configPath string) error {
	valCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	nomadExec := d.getNomadExecutor()
	if nomadExec == nil {
		return errors.New("nomad executor unavailable, failing safe")
	}
	stdout, stderr, err := nomadExec(valCtx, "config", "validate", configPath)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("nomad config validate failed for %s: %s (%w)", configPath, SanitizeLog(strings.TrimSpace(combined)), err)
	}
	return nil
}

// reconcileNomadServer safely restarts the nomad-server container after validating daemon configuration.
func (d *SyncDaemon) reconcileNomadServer(ctx context.Context, composeDir string) error {
	nomadDir := filepath.Join(composeDir, "nomad")
	if _, statErr := os.Stat(nomadDir); statErr != nil {
		log.Printf("[Hangar:GitOps] Notice: %s not found on disk, skipping nomad-server restart", nomadDir)
		return nil
	}

	if errVal := d.ValidateNomadConfig(ctx, nomadDir); errVal != nil {
		log.Printf("[Hangar:GitOps] ERROR: %v. Aborting nomad-server restart to prevent control plane outage.", errVal)
		return errVal
	}

	log.Printf("[Hangar:GitOps] Nomad daemon configuration changes detected and validated. Restarting nomad-server...")
	restartCtx, restartCancel := context.WithTimeout(ctx, 60*time.Second)
	defer restartCancel()

	restartArgs := d.getComposeArgs(composeDir, "restart", "nomad-server")
	out, stderr, err := d.getComposeExecutor()(restartCtx, composeDir, restartArgs...)
	if err != nil {
		combined := SanitizeLog(strings.TrimSpace(string(append(out, stderr...))))
		log.Printf("[Hangar:GitOps] ERROR: Failed to restart nomad-server: %s (%v)", combined, err)
		return fmt.Errorf("failed to restart nomad-server: %s (%w)", combined, err)
	}
	log.Printf("[Hangar:GitOps] Successfully restarted nomad-server for updated daemon configuration.")
	return nil
}

// candidateJobRepos returns all distinct repository paths known to the daemon.
func (d *SyncDaemon) candidateJobRepos() []string {
	var repos []string
	seen := make(map[string]struct{})
	add := func(p string) {
		if p == "" {
			return
		}
		clean := filepath.Clean(p)
		if _, ok := seen[clean]; !ok {
			seen[clean] = struct{}{}
			repos = append(repos, clean)
		}
	}
	for _, r := range d.repos {
		add(r)
	}
	add(d.composeDir)
	add(d.configDir)
	return repos
}

// IsJobDefinedInOtherRepo checks whether a Nomad job is defined in any tracked repo other than skipRepo.
func (d *SyncDaemon) IsJobDefinedInOtherRepo(jobName string, skipRepo string) (string, bool) {
	return FindJobDefinitionInRepos(jobName, skipRepo, d.candidateJobRepos())
}

// ReconcileNomadChanges validates and applies or stops Nomad job specifications.
func (d *SyncDaemon) ReconcileNomadChanges(ctx context.Context, repoPath string, changes []NomadFileChange, meta ...string) error {
	if len(changes) == 0 {
		return nil
	}

	var repo, commit, targetID string
	var prNumber int
	if len(meta) >= 1 {
		repo = meta[0]
	}
	if len(meta) >= 2 {
		commit = meta[1]
	}
	if len(meta) >= 3 {
		targetID = meta[2]
	}
	if len(meta) >= 4 {
		if parsed, err := strconv.Atoi(meta[3]); err == nil {
			prNumber = parsed
		}
	}
	if repo == "" && repoPath != "" {
		repo = NormalizeGitHubSlug(repoPath)
	}
	if commit == "" && repoPath != "" {
		if headOut, _, headErr := d.getGitExecutor()(ctx, repoPath, "rev-parse", "HEAD"); headErr == nil {
			commit = strings.TrimSpace(string(headOut))
		}
	}

	SortNomadChangesHangarLast(changes)

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	recCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// Phase 1: Teardowns (stop deleted jobs)
	for _, change := range changes {
		if change.Action == "delete" {
			if survivingPath, found := d.IsJobDefinedInOtherRepo(change.JobName, repoPath); found {
				log.Printf("[Hangar:Nomad] Notice: skipping teardown for deleted job %s (deleted from %s): job is still defined in %s", change.JobName, repoPath, survivingPath)
				continue
			}
			log.Printf("[Hangar:Nomad] Stopping deleted job %s...", change.JobName)
			out, errBytes, err := d.getNomadExecutor()(recCtx, "job", "stop", "-purge", change.JobName)
			if err != nil {
				log.Printf("[Hangar:Nomad] Warning: failed to stop deleted job %s: %s (%v)", change.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
			} else {
				log.Printf("[Hangar:Nomad] Successfully stopped deleted job %s", change.JobName)
			}
		}
	}

	// Phase 2: Applications (validate, then run)
	for _, change := range changes {
		if change.Action != "delete" {
			fullPath := filepath.Join(repoPath, change.Path)
			if _, statErr := os.Stat(fullPath); statErr != nil {
				log.Printf("[Hangar:Nomad] Notice: job file %s not found on disk, skipping", fullPath)
				continue
			}

			if valErr := d.ValidateNomadJob(recCtx, fullPath); valErr != nil {
				log.Printf("[Hangar:Nomad] ERROR: Validation failed for %s: %v", change.Path, valErr)
				return valErr
			}

			baseEvt := HangarDeployEvent{
				Event:     "deploy_started",
				JobName:   change.JobName,
				Repo:      repo,
				CommitSHA: commit,
				PRNumber:  prNumber,
				TargetID:  targetID,
				Status:    "started",
				Timestamp: time.Now().UTC(),
			}
			d.DispatchDeployEvent(baseEvt)

			log.Printf("[Hangar:Nomad] Applying job %s (%s)...", change.JobName, change.Path)
			out, errBytes, err := d.getNomadExecutor()(recCtx, "job", "run", "-detach", fullPath)
			if err != nil {
				combined := string(append(out, errBytes...))
				failEvt := baseEvt
				failEvt.Event = "deploy_failed"
				failEvt.Status = "failed"
				failEvt.Details = SanitizeLog(strings.TrimSpace(combined))
				failEvt.Timestamp = time.Now().UTC()
				d.DispatchDeployEvent(failEvt)
				return fmt.Errorf("nomad job run failed for %s: %s (%w)", change.Path, SanitizeLog(strings.TrimSpace(combined)), err)
			}
			log.Printf("[Hangar:Nomad] Successfully applied job %s: %s", change.JobName, SanitizeLog(strings.TrimSpace(string(out))))
		}
	}

	return nil
}

// ReconcileNomadJobs is an alias for ReconcileNomadChanges.
func (d *SyncDaemon) ReconcileNomadJobs(ctx context.Context, repoPath string, changes []NomadFileChange, meta ...string) error {
	return d.ReconcileNomadChanges(ctx, repoPath, changes, meta...)
}

func (d *SyncDaemon) ExecuteNomadTeardowns(ctx context.Context, pending []NomadChangeEvent) {
	if len(pending) == 0 {
		return
	}
	for _, evt := range pending {
		for _, change := range evt.Changes {
			if change.Action == "delete" {
				if survivingPath, found := d.IsJobDefinedInOtherRepo(change.JobName, evt.RepoPath); found {
					log.Printf("[Hangar:Nomad] Pre-flight teardown: skipping stop for deleted job %s (deleted from %s): job is still defined in %s", change.JobName, evt.RepoPath, survivingPath)
					continue
				}
				log.Printf("[Hangar:Nomad] Pre-flight teardown for deleted job %s...", change.JobName)
				out, errBytes, err := d.getNomadExecutor()(ctx, "job", "stop", "-purge", change.JobName)
				if err != nil {
					log.Printf("[Hangar:Nomad] Warning: pre-flight stop of %s: %s (%v)", change.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
				} else {
					log.Printf("[Hangar:Nomad] Successfully stopped %s", change.JobName)
				}
			}
		}
	}
}

// ReconcilePendingNomad applies all pending Nomad changes.
func (d *SyncDaemon) ReconcilePendingNomad(ctx context.Context) error {
	pending := d.drainPendingNomadChanges()
	if len(pending) == 0 {
		return nil
	}
	var errs []error
	for _, evt := range pending {
		if err := d.ReconcileNomadChanges(ctx, evt.RepoPath, evt.Changes, evt.Repo, evt.CommitSHA, evt.TargetID, strconv.Itoa(evt.PRNumber)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CheckAndReconcileNomadImages scans jobs/*.nomad in configDir for updated image digests and triggers rolling restarts.
func (d *SyncDaemon) CheckAndReconcileNomadImages(ctx context.Context) error {
	d.nomadPollMu.Lock()
	if !d.lastNomadImagePoll.IsZero() && time.Since(d.lastNomadImagePoll) < 5*time.Minute {
		d.nomadPollMu.Unlock()
		return nil
	}
	d.lastNomadImagePoll = time.Now()
	d.nomadPollMu.Unlock()

	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	seenFiles := make(map[string]struct{})
	for _, repo := range d.candidateJobRepos() {
		for _, sub := range []string{"jobs", filepath.Join("nomad", "jobs")} {
			jobsDir := filepath.Join(repo, sub)
			entries, err := os.ReadDir(jobsDir)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				if entry.IsDir() || !IsNomadJobFile(entry.Name()) {
					continue
				}
				jobPath := filepath.Join(jobsDir, entry.Name())
				if _, seen := seenFiles[jobPath]; seen {
					continue
				}
				seenFiles[jobPath] = struct{}{}

				content, readErr := os.ReadFile(jobPath)
				if readErr != nil {
					continue
				}
				jobName := ExtractJobName(string(content), entry.Name())
				images := ExtractNomadJobImages(string(content))

				for _, imgRef := range images {
					remoteDigest, digestErr := d.GetRemoteImageDigest(pollCtx, imgRef)
					if digestErr != nil || remoteDigest == "" {
						continue
					}

					key := jobName + ":" + imgRef
					d.nomadDigestsMu.RLock()
					lastDigest, seen := d.nomadKnownDigests[key]
					d.nomadDigestsMu.RUnlock()

					if !seen {
						d.nomadDigestsMu.Lock()
						if d.nomadKnownDigests == nil {
							d.nomadKnownDigests = make(map[string]string)
						}
						d.nomadKnownDigests[key] = remoteDigest
						d.nomadDigestsMu.Unlock()
						continue
					}

					if lastDigest != remoteDigest {
						log.Printf("[Hangar:NomadImagePoll] New image digest %s detected for Nomad job %s (image %s, previous %s). Rescheduling allocation.", remoteDigest, jobName, imgRef, lastDigest)
						d.nomadDigestsMu.Lock()
						d.nomadKnownDigests[key] = remoteDigest
						d.nomadDigestsMu.Unlock()

						// Resolve commit SHA: first try reading remote container image OCI revision labels,
						// falling back to local git repo HEAD if unavailable.
						var headSHA string
						if rev, revErr := d.GetRemoteImageRevision(pollCtx, imgRef); revErr == nil && rev != "" {
							headSHA = rev
							log.Printf("[Hangar:NomadImagePoll] Resolved commit SHA %s from image labels for %s", headSHA, imgRef)
						} else {
							if revErr != nil {
								log.Printf("[Hangar:NomadImagePoll] Notice: could not resolve revision from image %s: %v (falling back to git HEAD)", imgRef, revErr)
							}
							if headOut, _, headErr := d.getGitExecutor()(pollCtx, repo, "rev-parse", "HEAD"); headErr == nil {
								headSHA = strings.TrimSpace(string(headOut))
							}
						}

						repoName := ImageSourceRepo(imgRef)
						if repoName == "" {
							repoName = filepath.Base(repo)
							if repoName == "aerial" || repoName == "aerial-config" || repoName == "aerial-sidecars" {
								repoName = "azylman/" + repoName
							}
						}

						deployEvt := HangarDeployEvent{
							Event:     "deploy_started",
							JobName:   jobName,
							Repo:      repoName,
							CommitSHA: headSHA,
							Image:     imgRef,
							Digest:    remoteDigest,
							Status:    "started",
							Details:   fmt.Sprintf("nomad image poll restart for %s (digest %s)", imgRef, remoteDigest),
							Timestamp: time.Now().UTC(),
						}
						d.DispatchDeployEvent(deployEvt)

						out, errBytes, runErr := d.getNomadExecutor()(pollCtx, "job", "restart", "-yes", "-reschedule", "-on-error=fail", jobName)
						if runErr != nil {
							log.Printf("[Hangar:NomadImagePoll] Warning: nomad job restart failed for %s: %s (%v)", jobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), runErr)
						} else {
							log.Printf("[Hangar:NomadImagePoll] Successfully triggered reschedule restart for Nomad job %s", jobName)
						}
					}
				}
			}
		}
	}

	return nil
}

// StartReconcilerLoop runs the background debounced worker goroutine.
func (d *SyncDaemon) StartReconcilerLoop(ctx context.Context) {
	if d.reconcileCh == nil {
		d.reconcileCh = make(chan struct{}, 1)
	}

	go func() {
		var debounceTimer *time.Timer

		for {
			select {
			case <-ctx.Done():
				return
			case <-d.reconcileCh:
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(reconcilerDebounceDuration, func() {
					if elapsed := time.Since(d.GetLastReconcile()); elapsed < reconcilerMinCooldown {
						time.Sleep(reconcilerMinCooldown - elapsed)
					}

					// Phase 1: Teardowns for any deleted Nomad jobs (releases ports)
					d.pendingNomadMu.Lock()
					pendingNomad := append([]NomadChangeEvent(nil), d.pendingNomadChanges...)
					d.pendingNomadMu.Unlock()
					d.ExecuteNomadTeardowns(context.Background(), pendingNomad)

					// Phase 2: Check for Nomad server daemon config changes
					if d.consumePendingNomadServerRestart() {
						if errNomad := d.reconcileNomadServer(context.Background(), d.composeDir); errNomad != nil {
							log.Printf("[Hangar:GitOps] Warning: reconcileNomadServer encountered error: %v", errNomad)
						} else {
							// Wait briefly for Nomad API readiness after server container restart
							time.Sleep(3 * time.Second)
						}
					}

					// Phase 3: Nomad reconciliation (job applications)
					if nomadErr := d.ReconcilePendingNomad(context.Background()); nomadErr != nil {
						log.Printf("[Hangar:GitOps] Debounced Nomad reconcile failed: %v", nomadErr)
					}
					d.SetLastReconcile(time.Now())
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

	log.Printf("[Hangar] Bootstrapping repository at %s from %s...", repoPath, repoURL)
	if err := os.MkdirAll(repoPath, 0755); err != nil {
		return fmt.Errorf("failed to create repo directory %s: %w", repoPath, err)
	}

	entries, err := os.ReadDir(repoPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		out, errBytes, err := d.getGitExecutor()(ctx, "", "clone", "--depth", "1", "-b", "main", repoURL, repoPath)
		if err != nil {
			combined := string(append(out, errBytes...))
			return fmt.Errorf("git clone failed for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
		}
		return nil
	}

	if out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "init", "-b", "main"); err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git init failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}
	// Best-effort remote add: if origin already exists, do not fail
	if _, _, err := d.getGitExecutor()(ctx, repoPath, "remote", "add", "origin", repoURL); err != nil {
		log.Printf("[Hangar] Notice adding remote origin for %s (continuing): %v", repoPath, err)
	}
	out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "fetch", "--depth", "1", "origin", "main")
	if err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git fetch failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}

	if out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "reset", "--soft", "FETCH_HEAD"); err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git reset soft failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(combined), err)
	}
	return nil
}

// SyncRepo synchronizes a single repository via git pull --ff-only with fallback to reset --hard FETCH_HEAD.
func (d *SyncDaemon) SyncRepo(ctx context.Context, repoPath string, syncCtx ...ConfigSyncContext) (res RepoSyncResult) {
	res = RepoSyncResult{Repo: repoPath}

	if repoPath == "" {
		return res
	}

	repoLock := d.getRepoLock(repoPath)
	repoLock.Lock()
	defer repoLock.Unlock()

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
			log.Printf("[Hangar] Warning: EnsureRepo failed for %s: %v", repoPath, err)
		}
	}

	gitDir, err := resolveGitDir(repoPath)
	if err != nil {
		res.Error = fmt.Sprintf("git dir not found: %v", err)
		return res
	}

	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[Hangar] %s has index.lock present, skipping sync cycle", repoPath)
		res.Error = "index.lock active"
		return res
	}

	opCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()

	if _, stderr, err := d.getGitExecutor()(opCtx, "", "config", "--global", "safe.directory", "*"); err != nil {
		log.Printf("[Hangar] Warning: failed to configure safe.directory: %s (%v)", SanitizeLog(strings.TrimSpace(string(stderr))), err)
	}

	outBefore, _, err := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[Hangar] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, sanitizedErr)
		res.Error = sanitizedErr
		return res
	}
	res.PreviousHead = strings.TrimSpace(string(outBefore))
	res.CurrentHead = res.PreviousHead

	// 1. Fetch remote commit metadata without modifying working tree
	outFetch, errFetchBytes, errFetch := d.getGitExecutor()(opCtx, repoPath, "fetch", "origin", "main")
	if errFetch != nil {
		combinedFetch := string(append(outFetch, errFetchBytes...))
		sanitizedOut := SanitizeLog(strings.TrimSpace(combinedFetch))
		sanitizedErr := SanitizeLog(errFetch.Error())
		log.Printf("[Hangar] Notice: git fetch origin main failed for %s (%s, %s). Attempting git pull fallback...", repoPath, sanitizedErr, sanitizedOut)

		outPull, errPullBytes, errPull := d.getGitExecutor()(opCtx, repoPath, "pull", "--ff-only")
		if errPull != nil {
			combinedPull := string(append(outPull, errPullBytes...))
			res.Error = fmt.Sprintf("fetch failed: %s; pull failed: %s", sanitizedOut, SanitizeLog(strings.TrimSpace(combinedPull)))
			return res
		}
	} else {
		// 2. Fast-forward merge verified clean upstream commit
		outMerge, errMergeBytes, errMerge := d.getGitExecutor()(opCtx, repoPath, "merge", "--ff-only", "FETCH_HEAD")
		if errMerge != nil {
			combinedMerge := string(append(outMerge, errMergeBytes...))
			sanitizedOut := SanitizeLog(strings.TrimSpace(combinedMerge))
			sanitizedErr := SanitizeLog(errMerge.Error())
			log.Printf("[Hangar] Notice: git merge --ff-only failed for %s (%s, %s). Attempting safe reset recovery...", repoPath, sanitizedErr, sanitizedOut)

			outReset, errResetBytes, errReset := d.getGitExecutor()(opCtx, repoPath, "reset", "--hard", "FETCH_HEAD")
			if errReset != nil {
				combinedReset := string(append(outReset, errResetBytes...))
				res.Error = fmt.Sprintf("reset failed: %s", SanitizeLog(combinedReset))
				return res
			}

			if _, cleanStderr, cleanErr := d.getGitExecutor()(opCtx, repoPath, "clean", "-fd"); cleanErr != nil {
				log.Printf("[Hangar] Warning: git clean -fd failed for %s: %s (%v)", repoPath, SanitizeLog(strings.TrimSpace(string(cleanStderr))), cleanErr)
			}

			log.Printf("[Hangar] Successfully recovered %s via reset to FETCH_HEAD", repoPath)
		}
	}

	outAfter, _, err := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		res.Error = sanitizedErr
		return res
	}
	res.CurrentHead = strings.TrimSpace(string(outAfter))

	if res.PreviousHead != res.CurrentHead {
		res.Changed = true
		log.Printf("[Hangar] Repository %s updated: %s -> %s", repoPath, res.PreviousHead, res.CurrentHead)

		nomadCfgChanged, errNomadCfg := d.HasNomadConfigChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead)
		if errNomadCfg != nil {
			log.Printf("[Hangar] Warning: Failed to check nomad config changes in %s: %v", repoPath, errNomadCfg)
		}
		if nomadCfgChanged {
			log.Printf("[Hangar:GitOps] Nomad daemon configuration changes detected in %s (%s -> %s). Triggering debounced reconciliation.", repoPath, res.PreviousHead, res.CurrentHead)
			d.recordPendingNomadServerRestart()
			if d.reconcileCh != nil {
				select {
				case d.reconcileCh <- struct{}{}:
				default:
				}
			}
		}

		nomadChanges, errNomad := d.HasNomadChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead)
		if errNomad != nil {
			log.Printf("[Hangar] Warning: Failed to check Nomad changes in %s: %v", repoPath, errNomad)
		}
		if len(nomadChanges) > 0 {
			log.Printf("[Hangar:GitOps] Nomad job changes detected in %s (%d changes). Triggering debounced reconciliation.", repoPath, len(nomadChanges))
			var meta []string
			if len(syncCtx) > 0 {
				meta = []string{
					syncCtx[0].Repo,
					syncCtx[0].CommitSHA,
					syncCtx[0].TargetID,
					strconv.Itoa(syncCtx[0].PRNumber),
				}
			} else {
				meta = []string{
					NormalizeGitHubSlug(repoPath),
					res.CurrentHead,
					"",
					"0",
				}
			}
			d.recordPendingNomadChanges(repoPath, nomadChanges, meta...)
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
	if d.triggerFn != nil {
		return d.triggerFn()
	}

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

		if !hasError && d.configDir != "" {
			if err := d.SyncBrainConfigToNomad(syncCtx, d.configDir); err != nil {
				log.Printf("[Hangar:Periodic] Notice: SyncBrainConfigToNomad: %v", err)
			}
			if err := d.SyncHomepageConfigToNomad(syncCtx, d.configDir); err != nil {
				log.Printf("[Hangar:Periodic] Notice: SyncHomepageConfigToNomad: %v", err)
			}
			if err := d.SyncServiceConfigsToNomad(syncCtx, d.configDir); err != nil {
				log.Printf("[Hangar:Periodic] Notice: SyncServiceConfigsToNomad: %v", err)
			}
		}
		metrics.RecordSyncRequest("periodic", status)

		return results, nil
	})

	if err != nil {
		return nil, err
	}
	res, ok := val.([]RepoSyncResult)
	if !ok {
		return nil, fmt.Errorf("unexpected return type from singleflight: %T", val)
	}
	return res, nil
}

// SyncBrainConfigToNomad validates config.yaml and writes it to Nomad variable nomad/jobs/brain (CONFIG_YAML).
func (d *SyncDaemon) SyncBrainConfigToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	configPath := filepath.Join(configDir, "config.yaml")
	raw, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read brain config at %s: %w", configPath, err)
	}

	var parsed struct {
		Channels map[string]interface{} `yaml:"channels"`
	}
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("invalid YAML syntax in %s: %w", configPath, err)
	}
	if parsed.Channels != nil {
		hasDefault := false
		for k := range parsed.Channels {
			if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == "default" {
				hasDefault = true
				break
			}
		}
		if !hasDefault {
			return fmt.Errorf("semantic validation error in %s: channels.default is required", configPath)
		}
	}

	h := sha256.Sum256(raw)
	configHash := hex.EncodeToString(h[:])

	nomadExec := d.getNomadExecutor()
	if nomadExec == nil || d.nomadAddr == "" {
		return nil
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig != nil && d.lastPushedConfig["nomad/jobs/brain:CONFIG_YAML"] == configHash {
		d.lastPushedConfigMu.Unlock()
		return nil
	}
	d.lastPushedConfigMu.Unlock()

	valCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	args := []string{"var", "put", "-force", "nomad/jobs/brain", "CONFIG_YAML=@" + configPath}
	if len(syncCtx) > 0 && syncCtx[0].CommitSHA != "" {
		args = append(args, "COMMIT_SHA="+syncCtx[0].CommitSHA)
		baseEvt := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   "brain",
			Repo:      syncCtx[0].Repo,
			CommitSHA: syncCtx[0].CommitSHA,
			PRNumber:  syncCtx[0].PRNumber,
			TargetID:  syncCtx[0].TargetID,
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		go d.DispatchDeployEvent(baseEvt)
	}

	stdout, stderr, err := nomadExec(valCtx, args...)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("failed updating Nomad variable nomad/jobs/brain: %s (%w)", SanitizeLog(strings.TrimSpace(combined)), err)
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig == nil {
		d.lastPushedConfig = make(map[string]string)
	}
	d.lastPushedConfig["nomad/jobs/brain:CONFIG_YAML"] = configHash
	d.lastPushedConfigMu.Unlock()

	log.Printf("[Hangar:Nomad] Successfully pushed CONFIG_YAML to nomad/jobs/brain")
	return nil
}

// SyncHomepageConfigToNomad validates homepage.yaml, computes a deterministic hash of all homepage configs,
// and writes HOMEPAGE_YAML and CONFIG_HASH to Nomad variable nomad/jobs/homepage.
func (d *SyncDaemon) SyncHomepageConfigToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	homepageConfigPath := filepath.Join(configDir, "services", "homepage", "homepage.yaml")
	raw, err := os.ReadFile(homepageConfigPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read homepage config at %s: %w", homepageConfigPath, err)
	}

	if err == nil {
		var parsed map[string]interface{}
		if unmarshalErr := yaml.Unmarshal(raw, &parsed); unmarshalErr != nil {
			return fmt.Errorf("invalid YAML syntax in %s: %w", homepageConfigPath, unmarshalErr)
		}
	}

	h := sha256.New()
	if len(raw) > 0 {
		h.Write([]byte("homepage.yaml:"))
		h.Write(raw)
		h.Write([]byte("\n"))
	}

	userHomepageDir := filepath.Join(configDir, "homepage")
	if entries, readDirErr := os.ReadDir(userHomepageDir); readDirErr == nil {
		var files []string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
				continue
			}
			files = append(files, name)
		}
		sort.Strings(files)
		for _, f := range files {
			fPath := filepath.Join(userHomepageDir, f)
			if fContent, fErr := os.ReadFile(fPath); fErr == nil {
				h.Write([]byte(f + ":"))
				h.Write(fContent)
				h.Write([]byte("\n"))
			}
		}
	}

	configHash := hex.EncodeToString(h.Sum(nil))

	nomadExec := d.getNomadExecutor()
	if nomadExec == nil || d.nomadAddr == "" {
		return nil
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig != nil && d.lastPushedConfig["nomad/jobs/homepage"] == configHash {
		d.lastPushedConfigMu.Unlock()
		return nil
	}
	d.lastPushedConfigMu.Unlock()

	valCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	args := []string{"var", "put", "-force", "nomad/jobs/homepage", "CONFIG_HASH=" + configHash}
	if len(raw) > 0 {
		args = append(args, "HOMEPAGE_YAML=@"+homepageConfigPath)
	}
	if len(syncCtx) > 0 && syncCtx[0].CommitSHA != "" {
		args = append(args, "COMMIT_SHA="+syncCtx[0].CommitSHA)
		baseEvt := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   "homepage",
			Repo:      syncCtx[0].Repo,
			CommitSHA: syncCtx[0].CommitSHA,
			PRNumber:  syncCtx[0].PRNumber,
			TargetID:  syncCtx[0].TargetID,
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		go d.DispatchDeployEvent(baseEvt)
	}

	stdout, stderr, err := nomadExec(valCtx, args...)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("failed updating Nomad variable nomad/jobs/homepage: %s (%w)", SanitizeLog(strings.TrimSpace(combined)), err)
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig == nil {
		d.lastPushedConfig = make(map[string]string)
	}
	d.lastPushedConfig["nomad/jobs/homepage"] = configHash
	d.lastPushedConfigMu.Unlock()

	log.Printf("[Hangar:Nomad] Successfully pushed homepage config to nomad/jobs/homepage (hash=%s)", configHash[:12])
	return nil
}

// ServiceConfigMapping defines the declarative mapping between a repository configuration file
// and its destination Nomad variable.
type ServiceConfigMapping struct {
	RelPath  string `yaml:"rel_path"`
	NomadVar string `yaml:"nomad_var"`
	VarKey   string `yaml:"var_key"`
}

// DefaultManagedServiceConfigs specifies the default built-in MCP and auxiliary service configs
// synchronized into Nomad variables when not overridden by services/hangar/hangar.yaml.
var DefaultManagedServiceConfigs = []ServiceConfigMapping{
	{RelPath: "services/mcp/scheduler-mcp.yaml", NomadVar: "nomad/jobs/scheduler-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/mcp/docker-mcp.yaml", NomadVar: "nomad/jobs/docker-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/mcp/github-mcp.yaml", NomadVar: "nomad/jobs/github-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/mcp/nomad-mcp.yaml", NomadVar: "nomad/jobs/nomad-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/mcp/discord-mcp.yaml", NomadVar: "nomad/jobs/discord-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/mcp/infisical-mcp.yaml", NomadVar: "nomad/jobs/infisical-mcp", VarKey: "CONFIG_YAML"},
	{RelPath: "services/webhooks-router/webhooks-router.yaml", NomadVar: "nomad/jobs/webhooks-router", VarKey: "CONFIG_YAML"},
}

// ManagedServiceConfigs provides backward compatibility for references to the default service configs.
var ManagedServiceConfigs = DefaultManagedServiceConfigs

// GetServiceConfigMappings resolves the active service config mappings for a given configDir.
// If configDir contains services/hangar/hangar.yaml with non-empty service_configs, it dynamically parses and returns them.
// Otherwise, it falls back to daemon-configured serviceConfigs, or DefaultManagedServiceConfigs if none are configured.
func (d *SyncDaemon) GetServiceConfigMappings(configDir string) []ServiceConfigMapping {
	if configDir != "" {
		hangarCfgPath := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
		if raw, err := os.ReadFile(hangarCfgPath); err == nil {
			var hCfg HangarYAMLConfig
			if err := yaml.Unmarshal(raw, &hCfg); err == nil && len(hCfg.ServiceConfigs) > 0 {
				return hCfg.ServiceConfigs
			}
		}
	}
	if d != nil && len(d.serviceConfigs) > 0 {
		return d.serviceConfigs
	}
	return DefaultManagedServiceConfigs
}

// SyncServiceConfigsToNomad iterates over resolved service config mappings, validates YAML syntax,
// and pushes non-empty configurations to their respective Nomad variables using `nomad var put`.
func (d *SyncDaemon) SyncServiceConfigsToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	for _, mapping := range d.GetServiceConfigMappings(configDir) {
		filePath := filepath.Join(configDir, mapping.RelPath)
		raw, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read service config at %s: %w", filePath, err)
		}

		var parsed map[string]interface{}
		if err := yaml.Unmarshal(raw, &parsed); err != nil {
			return fmt.Errorf("invalid YAML syntax in %s: %w", filePath, err)
		}

		nomadExec := d.getNomadExecutor()
		if nomadExec == nil || d.nomadAddr == "" {
			continue
		}

		h := sha256.Sum256(raw)
		configHash := hex.EncodeToString(h[:])
		cacheKey := mapping.NomadVar + ":" + mapping.VarKey

		d.lastPushedConfigMu.Lock()
		if d.lastPushedConfig != nil && d.lastPushedConfig[cacheKey] == configHash {
			d.lastPushedConfigMu.Unlock()
			continue
		}
		d.lastPushedConfigMu.Unlock()

		valCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		args := []string{"var", "put", "-force", mapping.NomadVar, mapping.VarKey + "=@" + filePath}
		if len(syncCtx) > 0 && syncCtx[0].CommitSHA != "" {
			args = append(args, "COMMIT_SHA="+syncCtx[0].CommitSHA)
			baseEvt := HangarDeployEvent{
				Event:     "deploy_started",
				JobName:   strings.TrimPrefix(mapping.NomadVar, "nomad/jobs/"),
				Repo:      syncCtx[0].Repo,
				CommitSHA: syncCtx[0].CommitSHA,
				PRNumber:  syncCtx[0].PRNumber,
				TargetID:  syncCtx[0].TargetID,
				Status:    "started",
				Timestamp: time.Now().UTC(),
			}
			go d.DispatchDeployEvent(baseEvt)
		}
		stdout, stderr, err := nomadExec(valCtx, args...)
		cancel()
		if err != nil {
			combined := string(append(stdout, stderr...))
			return fmt.Errorf("failed updating Nomad variable %s: %s (%w)", mapping.NomadVar, SanitizeLog(strings.TrimSpace(combined)), err)
		}

		d.lastPushedConfigMu.Lock()
		if d.lastPushedConfig == nil {
			d.lastPushedConfig = make(map[string]string)
		}
		d.lastPushedConfig[cacheKey] = configHash
		d.lastPushedConfigMu.Unlock()

		log.Printf("[Hangar:Nomad] Successfully pushed %s to %s", mapping.VarKey, mapping.NomadVar)
	}

	return nil
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
				if _, syncErr := d.TriggerSync(); syncErr != nil {
					log.Printf("[Hangar] Periodic sync notice: %v", syncErr)
				}
				if imgErr := d.CheckAndReconcileNewImages(ctx); imgErr != nil {
					log.Printf("[Hangar] Periodic image check notice: %v", imgErr)
				}
			}
		}
	}()
}

// getRepoCommit extracts the commit SHA and author timestamp for a given ref in a repository.
func (d *SyncDaemon) getRepoCommit(ctx context.Context, repoPath, ref string) (string, *time.Time, error) {
	out, _, err := d.getGitExecutor()(ctx, repoPath, "log", "-1", "--format=%H%x00%aI", ref)
	if err != nil {
		return "", nil, err
	}
	parts := strings.Split(strings.TrimSpace(string(out)), "\x00")
	if len(parts) < 2 {
		return strings.TrimSpace(string(out)), nil, nil
	}
	sha := strings.TrimSpace(parts[0])
	if t, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(parts[1])); parseErr == nil {
		return sha, &t, nil
	}
	return sha, nil, nil
}

// GetStatus computes real-time synchronization telemetry across all configured repositories.
func (d *SyncDaemon) GetStatus(ctx context.Context) GitSyncStatusResponse {
	rec := d.GetReconciliationStatus()
	resp := GitSyncStatusResponse{
		Status:         "synced",
		Repos:          make(map[string]RepoStatus),
		Reconciliation: &rec,
	}

	d.statusMu.RLock()
	syncTimes := make(map[string]time.Time, len(d.lastSyncTimes))
	for k, v := range d.lastSyncTimes {
		syncTimes[k] = v
	}
	d.statusMu.RUnlock()

	var maxLag int64
	var latestSync time.Time

	for _, repo := range d.repos {
		st := RepoStatus{
			Repo:       repo,
			SyncStatus: "synced",
		}

		var ghSlug string
		d.statusMu.RLock()
		if d.repoGitHubSlugs != nil {
			ghSlug = d.repoGitHubSlugs[repo]
		}
		d.statusMu.RUnlock()
		if ghSlug == "" {
			out, _, err := d.getGitExecutor()(ctx, repo, "remote", "get-url", "origin")
			if err != nil || len(out) == 0 {
				out, _, err = d.getGitExecutor()(ctx, repo, "config", "--get", "remote.origin.url")
			}
			if err != nil {
				out = nil
			}
			rawURL := strings.TrimSpace(string(out))
			if rawURL == "" && d.repoUrls != nil {
				rawURL = d.repoUrls[repo]
			}
			if rawURL != "" {
				ghSlug = ParseGitHubSlug(rawURL)
				if ghSlug != "" {
					d.statusMu.Lock()
					if d.repoGitHubSlugs == nil {
						d.repoGitHubSlugs = make(map[string]string)
					}
					d.repoGitHubSlugs[repo] = ghSlug
					d.statusMu.Unlock()
				}
			}
		}
		st.GitHubRepo = ghSlug

		if t, ok := syncTimes[repo]; ok {
			st.LastSyncTime = t
			if t.After(latestSync) {
				latestSync = t
			}
		}

		// Check disk HEAD
		diskSha, diskTime, err := d.getRepoCommit(ctx, repo, "HEAD")
		if err != nil {
			st.SyncStatus = "error"
			st.Error = fmt.Sprintf("failed to get disk HEAD: %v", SanitizeLog(err.Error()))
			resp.Repos[repo] = st
			continue
		}
		st.DiskCommit = diskSha
		st.DiskCommitTime = diskTime

		// Check remote commit: try origin/main, fallback to FETCH_HEAD
		remoteSha, remoteTime, err := d.getRepoCommit(ctx, repo, "origin/main")
		if err != nil || remoteSha == "" {
			remoteSha, remoteTime, err = d.getRepoCommit(ctx, repo, "FETCH_HEAD")
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
			}
		}

		resp.Repos[repo] = st
	}

	resp.MaxLagSeconds = maxLag
	resp.LastSyncTime = latestSync
	if resp.LastSyncTime.IsZero() {
		resp.LastSyncTime = time.Now()
	}

	resp.Status = CalculateSyncStatus(resp.Repos)

	return resp
}

// DaemonConfig holds configuration options for the GitSync daemon.
type DaemonConfig struct {
	Port                   string
	Repos                  []string
	RepoURLs               map[string]string
	Interval               time.Duration
	PAT                    string
	ComposeDir             string
	ConfigDir              string
	DiscordToken           string
	DiscordChannel         string
	DiscordWebhookURL      string
	BrainInternalURL       string
	ExtraSecrets           []string
	ComposeExecutor        ComposeExecutor
	DockerExecutor         DockerExecutor
	GitExecutor            GitExecutor
	NomadExecutor          NomadExecutor
	NomadAddr              string
	NomadToken             string
	RegistryClient         *http.Client
	WebhooksRouterURL      string
	DeployDispatcher       func(ctx context.Context, evt HangarDeployEvent) error
	ServiceConfigs         []ServiceConfigMapping
}

// NewDaemon initializes a new SyncDaemon from config.
func NewDaemon(cfg DaemonConfig) *SyncDaemon {
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.ComposeDir == "" {
		cfg.ComposeDir = "/share/aerial"
	}
	if cfg.ConfigDir == "" {
		cfg.ConfigDir = "/share/aerial-config"
	}
	if cfg.BrainInternalURL == "" {
		cfg.BrainInternalURL = "http://brain:8080/internal/reload"
	}
	webhooksRouterURL := cfg.WebhooksRouterURL
	if webhooksRouterURL == "" {
		if envVal := os.Getenv("WEBHOOKS_ROUTER_URL"); envVal != "" {
			webhooksRouterURL = envVal
		} else {
			webhooksRouterURL = "http://127.0.0.1:4020"
		}
	}
	return &SyncDaemon{
		repos:                  cfg.Repos,
		repoUrls:               cfg.RepoURLs,
		interval:               cfg.Interval,
		pat:                    cfg.PAT,
		composeDir:             cfg.ComposeDir,
		configDir:              cfg.ConfigDir,
		discordToken:           cfg.DiscordToken,
		discordChannel:         cfg.DiscordChannel,
		discordWebhookURL:      cfg.DiscordWebhookURL,
		brainInternalURL:       cfg.BrainInternalURL,
		extraSecrets:           cfg.ExtraSecrets,
		composeExecutor:        cfg.ComposeExecutor,
		dockerExecutor:         cfg.DockerExecutor,
		gitExecutor:            cfg.GitExecutor,
		nomadExecutor:          cfg.NomadExecutor,
		nomadAddr:              cfg.NomadAddr,
		nomadToken:             cfg.NomadToken,
		nomadKnownDigests:      make(map[string]string),
		registryClient:         cfg.RegistryClient,
		webhooksRouterURL:      webhooksRouterURL,
		deployDispatcher:       cfg.DeployDispatcher,
		reconcileCh:            make(chan struct{}, 1),
		pendingImageRollouts:   make(map[string]PendingImageRollout),
		imageRolloutCh:         make(chan struct{}, 1),
		imageRolloutDebounce:   3 * time.Second,
		repoLocks:              make(map[string]*sync.Mutex),
		reconcileStatus: ReconciliationStatus{
			State: "idle",
			Stage: "idle",
		},
		lastAlertTimes:   make(map[string]time.Time),
		repoGitHubSlugs:  make(map[string]string),
		lastPushedConfig: make(map[string]string),
		serviceConfigs:   cfg.ServiceConfigs,
	}
}

// EnsureDockerAuth writes or updates the Docker config.json file with authentication credentials
// for ghcr.io using the daemon's configured PAT, enabling Docker Compose to pull private sidecar images.
func (d *SyncDaemon) EnsureDockerAuth() error {
	if d == nil || strings.TrimSpace(d.pat) == "" {
		return nil
	}

	repoURL := ""
	if d.repoUrls != nil {
		repoURL = d.repoUrls[d.configDir]
		if repoURL == "" {
			repoURL = d.repoUrls[d.composeDir]
		}
	}

	return EnsureDockerAuthPath(d.pat, repoURL, os.Getenv, os.ReadFile, os.WriteFile, os.MkdirAll)
}

// EnsureDockerAuthPath is the parameterized implementation of EnsureDockerAuth, enabling
// hermetic unit testing without filesystem or ambient environment side-effects.
func EnsureDockerAuthPath(
	pat string,
	repoURL string,
	lookup func(string) string,
	readFile func(string) ([]byte, error),
	writeFile func(string, []byte, os.FileMode) error,
	mkdirAll func(string, os.FileMode) error,
) error {
	pat = strings.TrimSpace(pat)
	if pat == "" {
		return nil
	}

	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	if readFile == nil {
		readFile = os.ReadFile
	}
	if writeFile == nil {
		writeFile = os.WriteFile
	}
	if mkdirAll == nil {
		mkdirAll = os.MkdirAll
	}

	configPath := ResolveDockerConfigPath(lookup)
	configDir := filepath.Dir(configPath)

	if err := mkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create docker config directory %s: %w", configDir, err)
	}

	var existingContent []byte
	if data, err := readFile(configPath); err == nil {
		existingContent = data
	} else if !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read existing docker config %s: %w", configPath, err)
	}

	username := ResolveRegistryUser(repoURL, lookup)
	newContent, err := GenerateDockerConfig(existingContent, "ghcr.io", username, pat)
	if err != nil {
		return fmt.Errorf("failed to generate docker config: %w", err)
	}

	if err := writeFile(configPath, newContent, 0600); err != nil {
		return fmt.Errorf("failed to write docker config to %s: %w", configPath, err)
	}

	return nil
}

// CheckGitHubRepoBuildInProgress checks if any GitHub Actions workflow runs are queued or in progress for a repo.
func (d *SyncDaemon) CheckGitHubRepoBuildInProgress(ctx context.Context, repoSlug string) bool {
	if d == nil {
		return false
	}
	slug := NormalizeGitHubSlug(repoSlug)
	if slug == "" {
		return false
	}

	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs?status=in_progress&per_page=5", slug)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		log.Printf("[Hangar:GitHub] Warning: failed to create actions request for %s: %v", slug, err)
		return false
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if strings.TrimSpace(d.pat) != "" {
		req.Header.Set("Authorization", "Bearer "+d.pat)
	}

	client := d.getRegistryClient()
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Hangar:GitHub] Warning: actions API request failed for %s: %v", slug, err)
		return false
	}
	defer closeWarn(resp.Body, "github actions response body")

	if resp.StatusCode == http.StatusOK {
		var data struct {
			TotalCount int `json:"total_count"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&data); err == nil && data.TotalCount > 0 {
			log.Printf("[Hangar:GitHub] %d active workflow run(s) in progress for %s", data.TotalCount, slug)
			return true
		}
	}

	// Also check queued runs
	apiQueuedURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs?status=queued&per_page=5", slug)
	reqQ, errQ := http.NewRequestWithContext(ctx, http.MethodGet, apiQueuedURL, nil)
	if errQ == nil {
		reqQ.Header.Set("Accept", "application/vnd.github+json")
		if strings.TrimSpace(d.pat) != "" {
			reqQ.Header.Set("Authorization", "Bearer "+d.pat)
		}
		if respQ, errQDo := client.Do(reqQ); errQDo == nil {
			defer closeWarn(respQ.Body, "github queued actions response body")
			if respQ.StatusCode == http.StatusOK {
				var dataQ struct {
					TotalCount int `json:"total_count"`
				}
				if err := json.NewDecoder(respQ.Body).Decode(&dataQ); err == nil && dataQ.TotalCount > 0 {
					log.Printf("[Hangar:GitHub] %d queued workflow run(s) for %s", dataQ.TotalCount, slug)
					return true
				}
			}
		}
	}

	return false
}

func (d *SyncDaemon) getChangedFiles(ctx context.Context, repoPath, prevHead, currHead string) ([]string, error) {
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return nil, nil
	}
	out, _, err := d.getGitExecutor()(ctx, repoPath, "diff", "--name-only", prevHead, currHead)
	if err != nil {
		outFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, "diff-tree", "--no-commit-id", "--name-only", "-r", currHead)
		if errFallback != nil {
			return nil, errFallback
		}
		return splitLines(string(outFallback)), nil
	}
	return splitLines(string(out)), nil
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

func (d *SyncDaemon) applyNomadChangesDirectly(ctx context.Context, repoPath string, changes []NomadFileChange, meta ...string) ([]string, error) {
	var repo, commit, targetID string
	var prNumber int
	if len(meta) >= 1 {
		repo = meta[0]
	}
	if len(meta) >= 2 {
		commit = meta[1]
	}
	if len(meta) >= 3 {
		targetID = meta[2]
	}
	if len(meta) >= 4 {
		if parsed, err := strconv.Atoi(meta[3]); err == nil {
			prNumber = parsed
		}
	}

	SortNomadChangesHangarLast(changes)

	var applied []string
	for _, ch := range changes {
		if ch.Action == "delete" {
			if survivingPath, found := d.IsJobDefinedInOtherRepo(ch.JobName, repoPath); found {
				log.Printf("[Hangar:Nomad] Notice: skipping teardown for deleted job %s (deleted from %s): job is still defined in %s", ch.JobName, repoPath, survivingPath)
				continue
			}
			out, errBytes, err := d.getNomadExecutor()(ctx, "job", "stop", "-purge", ch.JobName)
			if err != nil {
				log.Printf("[Hangar:Nomad] Warning: failed to stop deleted job %s: %s (%v)", ch.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
			}
			continue
		}
		fullPath := filepath.Join(repoPath, ch.Path)
		if !IsSafeJobPath(repoPath, fullPath) {
			continue
		}
		if errVal := d.ValidateNomadJob(ctx, fullPath); errVal != nil {
			return applied, errVal
		}

		baseEvt := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   ch.JobName,
			Repo:      repo,
			CommitSHA: commit,
			PRNumber:  prNumber,
			TargetID:  targetID,
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(baseEvt)

		out, errBytes, errRun := d.getNomadExecutor()(ctx, "job", "run", "-detach", fullPath)
		if errRun != nil {
			failEvt := baseEvt
			failEvt.Event = "deploy_failed"
			failEvt.Status = "failed"
			failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
			failEvt.Timestamp = time.Now().UTC()
			d.DispatchDeployEvent(failEvt)
			return applied, errRun
		}

		applied = append(applied, ch.JobName)
	}
	return applied, nil
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
		Repo:      req.Repo,
		CommitSHA: commitSHA,
		PRNumber:  req.PRNumber,
		TargetID:  req.TargetID,
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
	syncEvt := HangarDeployEvent{
		Event:     "sync_success",
		JobName:   "git-sync",
		Repo:      req.Repo,
		PRNumber:  req.PRNumber,
		TargetID:  req.TargetID,
		CommitSHA: commitSHA,
		Status:    "success",
		Details:   "git sync completed",
		Timestamp: time.Now().UTC(),
	}
	d.DispatchDeployEvent(syncEvt)

	cleanRepo := strings.ToLower(filepath.Clean(repoPath))
	isAerialConfig := strings.Contains(cleanRepo, "aerial-config")
	isAerialCore := strings.HasSuffix(cleanRepo, "/aerial") || cleanRepo == "aerial"

	if isAerialCore {
		changedFiles, errDiff := d.getChangedFiles(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errDiff == nil && HasContainerBuildChanges(repoPath, changedFiles) {
			resp.ContainersBuilding = true
			resp.Message = "repository synced; container build changes detected, deferring rollout until image_ready"
			log.Printf("[Hangar:GitPush] Container build changes detected in %s (%d files). Deferring Nomad rollout.", repoPath, len(changedFiles))
			return resp, http.StatusOK
		}

		nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errNomad == nil && len(nomadChanges) > 0 {
			resp.NomadChanged = true
			applied, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
			resp.AppliedJobs = applied
			if errRec != nil {
				resp.Status = "error"
				resp.Message = fmt.Sprintf("failed applying nomad changes: %v", errRec)
				return resp, http.StatusInternalServerError
			}
			resp.Message = fmt.Sprintf("repository synced; applied %d nomad job(s)", len(applied))
			return resp, http.StatusOK
		}

		resp.Message = "repository synced; no container build or nomad changes"
		return resp, http.StatusOK
	}

	if isAerialConfig {
		nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errNomad != nil {
			log.Printf("[Hangar:GitPush] Warning: HasNomadChanges failed for %s: %v", repoPath, errNomad)
		}

		if len(nomadChanges) > 0 {
			resp.NomadChanged = true
			SortNomadChangesHangarLast(nomadChanges)
			var appliedJobs []string
			var deferredJobs []string

			for _, ch := range nomadChanges {
				if ch.Action == "delete" {
					if survivingPath, found := d.IsJobDefinedInOtherRepo(ch.JobName, repoPath); found {
						log.Printf("[Hangar:GitPush] Notice: skipping stop for deleted job %s (deleted from %s): job is still defined in %s", ch.JobName, repoPath, survivingPath)
						continue
					}
					out, errBytes, err := d.getNomadExecutor()(ctx, "job", "stop", "-purge", ch.JobName)
					if err != nil {
						log.Printf("[Hangar:GitPush] Warning stopping deleted job %s: %s (%v)", ch.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
					} else {
						log.Printf("[Hangar:GitPush] Successfully stopped deleted job %s", ch.JobName)
					}
					continue
				}

				fullPath := filepath.Join(repoPath, ch.Path)
				if !IsSafeJobPath(repoPath, fullPath) {
					continue
				}
				content, readErr := os.ReadFile(fullPath)
				if readErr != nil {
					continue
				}

				images := ExtractNomadJobImages(string(content))
				jobDeferred := false

				for _, img := range images {
					srcRepo := ImageSourceRepo(img)
					if srcRepo != "" {
						if d.CheckGitHubRepoBuildInProgress(ctx, srcRepo) {
							jobDeferred = true
							log.Printf("[Hangar:GitPush] Job %s deferred: source repo %s has builds in progress", ch.JobName, srcRepo)
							break
						}
						dgst, dgstErr := d.GetRemoteImageDigest(ctx, img)
						if dgstErr != nil || dgst == "" {
							jobDeferred = true
							log.Printf("[Hangar:GitPush] Job %s deferred: image %s not ready in registry (%v)", ch.JobName, img, dgstErr)
							break
						}
					}
				}

				if jobDeferred {
					deferredJobs = append(deferredJobs, ch.JobName)
					resp.ContainersBuilding = true
				} else {
					if errVal := d.ValidateNomadJob(ctx, fullPath); errVal == nil {
						baseEvt := HangarDeployEvent{
							Event:     "deploy_started",
							JobName:   ch.JobName,
							Repo:      req.Repo,
							CommitSHA: req.Commit,
							PRNumber:  req.PRNumber,
							TargetID:  req.TargetID,
							Status:    "started",
							Timestamp: time.Now().UTC(),
						}
						d.DispatchDeployEvent(baseEvt)

						out, errBytes, errRun := d.getNomadExecutor()(ctx, "job", "run", "-detach", fullPath)
						if errRun != nil {
							log.Printf("[Hangar:GitPush] nomad job run failed for %s: %s (%v)", ch.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), errRun)
							failEvt := baseEvt
							failEvt.Event = "deploy_failed"
							failEvt.Status = "failed"
							failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
							failEvt.Timestamp = time.Now().UTC()
							d.DispatchDeployEvent(failEvt)
						} else {
							appliedJobs = append(appliedJobs, ch.JobName)
						}
					} else {
						log.Printf("[Hangar:GitPush] Validation failed for %s: %v", ch.JobName, errVal)
					}
				}
			}

			resp.AppliedJobs = appliedJobs
			if len(deferredJobs) > 0 {
				resp.Message = fmt.Sprintf("applied %d job(s); deferred %d job(s) pending container builds", len(appliedJobs), len(deferredJobs))
			} else {
				resp.Message = fmt.Sprintf("applied %d nomad job(s)", len(appliedJobs))
			}
		} else {
			resp.Message = "configuration synced; no nomad job changes"
		}

		sc.CommitSHA = commitSHA
		if err := d.SyncBrainConfigToNomad(ctx, repoPath, sc); err != nil {
			log.Printf("[Hangar:GitPush] Notice: SyncBrainConfigToNomad: %v", err)
		}
		if err := d.SyncHomepageConfigToNomad(ctx, repoPath, sc); err != nil {
			log.Printf("[Hangar:GitPush] Notice: SyncHomepageConfigToNomad: %v", err)
		}
		if err := d.SyncServiceConfigsToNomad(ctx, repoPath, sc); err != nil {
			log.Printf("[Hangar:GitPush] Notice: SyncServiceConfigsToNomad: %v", err)
		}

		return resp, http.StatusOK
	}

	resp.Message = "repository synced"
	return resp, http.StatusOK
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

	var matches []MatchedNomadJob
	seenJobs := make(map[string]struct{})
	for _, repo := range d.candidateJobRepos() {
		for _, sub := range []string{"jobs", filepath.Join("nomad", "jobs")} {
			dir := filepath.Join(repo, sub)
			for _, m := range FindNomadJobsByImage(dir, imgRef) {
				if _, ok := seenJobs[m.JobName]; !ok {
					seenJobs[m.JobName] = struct{}{}
					matches = append(matches, m)
				}
			}
		}
	}

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
			Event:     "deploy_started",
			JobName:   job.JobName,
			Repo:      req.Repo,
			CommitSHA: req.CommitSHA,
			PRNumber:  req.PRNumber,
			TargetID:  req.TargetID,
			Image:     req.Image,
			Digest:    digest,
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(baseEvt)

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
		outRun, errRunBytes, errRun := d.getNomadExecutor()(ctx, runArgs...)
		if errRun != nil {
			log.Printf("[Hangar:ImageReady] Error: job run failed for %s: %s (%v)", job.JobName, SanitizeLog(strings.TrimSpace(string(append(outRun, errRunBytes...)))), errRun)
			failEvt := baseEvt
			failEvt.Event = "deploy_failed"
			failEvt.Status = "failed"
			failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(outRun, errRunBytes...))))
			failEvt.Timestamp = time.Now().UTC()
			d.DispatchDeployEvent(failEvt)
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

// SetImageRolloutDebounce configures the debounce window for batching image rollouts.
func (d *SyncDaemon) SetImageRolloutDebounce(dur time.Duration) {
	d.imageRolloutDebounce = dur
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

	var matches []MatchedNomadJob
	seenJobs := make(map[string]struct{})
	for _, repo := range d.candidateJobRepos() {
		for _, sub := range []string{"jobs", filepath.Join("nomad", "jobs")} {
			dir := filepath.Join(repo, sub)
			for _, m := range FindNomadJobsByImage(dir, imgRef) {
				if _, ok := seenJobs[m.JobName]; !ok {
					seenJobs[m.JobName] = struct{}{}
					matches = append(matches, m)
				}
			}
		}
	}

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
	for _, repo := range d.candidateJobRepos() {
		for _, sub := range []string{"jobs", filepath.Join("nomad", "jobs")} {
			for _, m := range FindNomadJobsByImage(filepath.Join(repo, sub), req.Image) {
				if IsHangarJob(m.JobName) {
					return true
				}
			}
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

var writeCountersDirFn = coverage.WriteCountersDir

// SetupMux configures HTTP handlers for metrics, health, status, and sync.
func SetupMux(daemon *SyncDaemon) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/debug/coverage/flush", func(w http.ResponseWriter, r *http.Request) {
		if dir := os.Getenv("GOCOVERDIR"); dir != "" {
			if err := writeCountersDirFn(dir); err != nil {
				log.Printf("[hangar] coverage flush error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		status := daemon.GetStatus(ctx)
		writeJSON(w, r, http.StatusOK, status)
	})

	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
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
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		if r.Body != nil {
			defer closeWarn(r.Body, "reconcile request body")
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
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if r.Body != nil {
			defer closeWarn(r.Body, "git_push request body")
		}

		var req GitPushEventRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, r, http.StatusBadRequest, GitPushEventResponse{
				Status:  "error",
				Message: fmt.Sprintf("invalid JSON payload: %v", err),
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()

		resp, code := daemon.ExecuteGitPushEvent(ctx, req)
		metrics.RecordSyncRequest("git_push", resp.Status)
		writeJSON(w, r, code, resp)
	})

	mux.HandleFunc("/events/image_ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if r.Body != nil {
			defer closeWarn(r.Body, "image_ready request body")
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

	port := lookup("PORT")
	if port == "" && fileCfg.Port != "" {
		port = fileCfg.Port
	}
	if port == "" {
		port = "8087"
	}

	rawRepos := lookup("SYNC_REPOS")
	var repos []string
	if rawRepos != "" {
		for _, r := range strings.Split(rawRepos, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				repos = append(repos, r)
			}
		}
	} else if len(fileCfg.SyncRepos) > 0 {
		for _, r := range fileCfg.SyncRepos {
			r = strings.TrimSpace(r)
			if r != "" {
				repos = append(repos, r)
			}
		}
	} else {
		repos = []string{"/share/aerial-config", "/share/aerial"}
	}

	intervalStr := lookup("SYNC_INTERVAL")
	if intervalStr == "" && fileCfg.SyncInterval != "" {
		intervalStr = fileCfg.SyncInterval
	}
	interval := 60 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d > 0 {
			interval = d
		}
	}

	pat := lookup("GITHUB_PAT")
	configRepoURL := lookup("AERIAL_CONFIG_REPO_URL")
	if configRepoURL == "" {
		configRepoURL = "https://github.com/azylman/aerial-config.git"
	}

	composeDir := lookup("AERIAL_INTERNAL_PROJECT_DIR")
	if composeDir == "" {
		composeDir = lookup("AERIAL_PROJECT_DIR")
	}
	if composeDir == "" {
		composeDir = "/share/aerial"
	}

	configDir := lookup("AERIAL_INTERNAL_CONFIG_DIR")
	if configDir == "" {
		configDir = lookup("AERIAL_CONFIG_DIR")
	}
	if configDir == "" {
		configDir = "/share/aerial-config"
	}

	repoUrls := map[string]string{
		configDir:  configRepoURL,
		composeDir: "https://github.com/azylman/aerial.git",
	}

	discordToken := lookup("DISCORD_BOT_TOKEN")
	if discordToken == "" {
		discordToken = lookup("DISCORD_TOKEN")
	}
	discordChannel := lookup("DISCORD_CHANNEL")
	discordWebhookURL := lookup("DISCORD_WEBHOOK_URL")

	brainInternalURL := lookup("BRAIN_INTERNAL_URL")
	if brainInternalURL == "" && fileCfg.BrainInternalURL != "" {
		brainInternalURL = fileCfg.BrainInternalURL
	}
	if brainInternalURL == "" {
		brainInternalURL = "http://brain:8088/internal/reload"
	}

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

	nomadAddr := lookup("NOMAD_ADDR")
	if nomadAddr == "" && fileCfg.NomadAddr != "" {
		nomadAddr = fileCfg.NomadAddr
	}
	if nomadAddr == "" {
		nomadAddr = "http://host.docker.internal:4646"
	}
	nomadToken := lookup("NOMAD_TOKEN")

	webhooksRouterURL := lookup("WEBHOOKS_ROUTER_URL")
	if webhooksRouterURL == "" {
		webhooksRouterURL = "http://127.0.0.1:4020"
	}

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
