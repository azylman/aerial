package main

import (
	"bytes"
	"context"
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

// QuarantineKey uniquely identifies a repository and a commit SHA.
type QuarantineKey struct {
	RepoPath  string
	CommitSHA string
}

// QuarantineRecord holds diagnostic metadata for a quarantined commit.
type QuarantineRecord struct {
	RepoPath      string    `json:"repo_path"`
	CommitSHA     string    `json:"commit_sha"`
	PreviousHead  string    `json:"previous_head"`
	Reason        string    `json:"reason"`
	FailureStage  string    `json:"failure_stage"` // "validation" or "compose_up"
	QuarantinedAt time.Time `json:"quarantined_at"`
}

// ImageQuarantineRecord holds diagnostic metadata for a quarantined container image digest.
type ImageQuarantineRecord struct {
	Service       string    `json:"service"`
	Digest        string    `json:"digest"`
	Reason        string    `json:"reason"`
	QuarantinedAt time.Time `json:"quarantined_at"`
}

// ComposeChangeEvent captures a detected configuration change before reconciliation.
type ComposeChangeEvent struct {
	RepoPath           string    `json:"repo_path"`
	PreviousHead       string    `json:"previous_head"`
	CurrentHead        string    `json:"current_head"`
	Timestamp          time.Time `json:"timestamp"`
	NomadConfigChanged bool      `json:"nomad_config_changed,omitempty"`
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
	Repo              string     `json:"repo"`
	GitHubRepo        string     `json:"github_repo,omitempty"`
	DiskCommit        string     `json:"disk_commit"`
	DiskCommitTime    *time.Time `json:"disk_commit_time,omitempty"`
	RemoteCommit      string     `json:"remote_commit"`
	RemoteCommitTime  *time.Time `json:"remote_commit_time,omitempty"`
	TimeLagSeconds    int64      `json:"time_lag_seconds"`
	SyncStatus        string     `json:"sync_status"` // "synced", "lagging", "quarantined", "error"
	Quarantined       bool       `json:"quarantined,omitempty"`
	QuarantinedCommit string     `json:"quarantined_commit,omitempty"`
	QuarantineReason  string     `json:"quarantine_reason,omitempty"`
	LastSyncTime      time.Time  `json:"last_sync_time"`
	Error             string     `json:"error,omitempty"`
}

// ReconciliationStatus captures in-memory state of active and recent Docker Compose reconciliations.
type ReconciliationStatus struct {
	Active         bool      `json:"active"`
	State          string    `json:"state"`                     // "idle", "pulling", "swapping", "rolling_back", "healthy", "failed"
	Stage          string    `json:"stage"`                     // alias to State for backward compatibility
	Trigger        string    `json:"trigger,omitempty"`         // "image_poll", "git_sync", "manual_api"
	TargetServices []string  `json:"target_services,omitempty"` // services planned/targeted for reconciliation
	CommitSHA      string    `json:"commit_sha,omitempty"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// HangarStatusResponse is the aggregated telemetry payload returned by GET /status.
type HangarStatusResponse struct {
	Status         string                `json:"status"` // "synced", "lagging", "quarantined", "error"
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
	composeMu     sync.Mutex
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
	composePullTimeout time.Duration

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

	// Concurrency & Quarantine
	repoLocksMu        sync.Mutex
	repoLocks          map[string]*sync.Mutex
	pendingChangesMu   sync.Mutex
	pendingChanges     []ComposeChangeEvent
	pendingTargetsMu   sync.Mutex
	pendingTargets     map[string]struct{}
	reconcileStateMu   sync.RWMutex
	reconcileStatus    ReconciliationStatus
	quarantineMu       sync.RWMutex
	quarantinedCommits map[QuarantineKey]QuarantineRecord
	imageQuarantineMu  sync.RWMutex
	imageQuarantine    map[string]ImageQuarantineRecord

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
	deploymentPollInterval time.Duration
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

func (d *SyncDaemon) MonitorDeploymentAsync(jobName string, minVersion int, baseEvt HangarDeployEvent) {
	if d == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Hangar:Deploy] PANIC recovered in deployment monitor for %s: %v", jobName, r)
			}
		}()
		monCtx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
		defer cancel()

		pollInterval := d.deploymentPollInterval
		if pollInterval <= 0 {
			pollInterval = 2 * time.Second
		}
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-monCtx.Done():
				log.Printf("[Hangar:Deploy] Deployment monitor timed out for %s", jobName)
				return
			case <-ticker.C:
				out, errBytes, err := d.getNomadExecutor()(monCtx, "job", "status", "-json", jobName)
				if err != nil {
					continue
				}
				status, errParse := ParseJobStatusOutput(append(out, errBytes...))
				if errParse != nil {
					continue
				}
				if status.LatestDeployment != nil {
					// Critical invariant: Prevent stale deployment race by checking JobVersion >= minVersion
					if status.LatestDeployment.JobVersion < minVersion {
						continue
					}
					dep := status.LatestDeployment
					if dep.Status == "successful" {
						evt := baseEvt
						evt.Event = "deploy_success"
						evt.Status = "success"
						evt.DeploymentID = dep.ID
						evt.Details = dep.StatusDescription
						evt.Timestamp = time.Now().UTC()
						d.DispatchDeployEvent(evt)
						return
					} else if dep.Status == "failed" {
						evt := baseEvt
						isRollback := false
						for _, tg := range dep.TaskGroups {
							if tg.AutoRevert {
								isRollback = true
								break
							}
						}
						if strings.Contains(strings.ToLower(dep.StatusDescription), "revert") {
							isRollback = true
						}
						if isRollback {
							evt.Event = "deploy_rollback"
							evt.Status = "rollback"
						} else {
							evt.Event = "deploy_failed"
							evt.Status = "failed"
						}
						evt.DeploymentID = dep.ID
						evt.Details = dep.StatusDescription
						evt.Timestamp = time.Now().UTC()
						d.DispatchDeployEvent(evt)
						return
					} else if dep.Status == "cancelled" {
						evt := baseEvt
						evt.Event = "deploy_failed"
						evt.Status = "failed"
						evt.DeploymentID = dep.ID
						evt.Details = "deployment cancelled: " + dep.StatusDescription
						evt.Timestamp = time.Now().UTC()
						d.DispatchDeployEvent(evt)
						return
					}
					// Still running, continue loop
				} else {
					// No deployment stanza: check running allocations
					if status.Summary.Children.Running > 0 {
						evt := baseEvt
						evt.Event = "deploy_success"
						evt.Status = "success"
						evt.Details = "allocations running"
						evt.Timestamp = time.Now().UTC()
						d.DispatchDeployEvent(evt)
						return
					}
				}
			}
		}
	}()
}

const (
	imageQuarantineTTL        = 60 * time.Minute
	defaultComposePullTimeout = 10 * time.Minute
)

func (d *SyncDaemon) quarantineImage(service, digest, reason string) {
	d.imageQuarantineMu.Lock()
	defer d.imageQuarantineMu.Unlock()
	if d.imageQuarantine == nil {
		d.imageQuarantine = make(map[string]ImageQuarantineRecord)
	}
	key := fmt.Sprintf("%s@%s", service, strings.TrimSpace(digest))
	d.imageQuarantine[key] = ImageQuarantineRecord{
		Service:       service,
		Digest:        strings.TrimSpace(digest),
		Reason:        reason,
		QuarantinedAt: time.Now(),
	}
	metrics.RecordImageQuarantine(service, reason)
	count := 0.0
	for _, rec := range d.imageQuarantine {
		if rec.Service == service {
			count++
		}
	}
	metrics.RecordActiveQuarantines(service, count)
}

func (d *SyncDaemon) isImageQuarantined(service, digest string) bool {
	d.imageQuarantineMu.RLock()
	defer d.imageQuarantineMu.RUnlock()
	if d.imageQuarantine == nil {
		return false
	}
	key := fmt.Sprintf("%s@%s", service, strings.TrimSpace(digest))
	rec, ok := d.imageQuarantine[key]
	if !ok {
		return false
	}
	if time.Since(rec.QuarantinedAt) > imageQuarantineTTL {
		return false
	}
	return true
}

func (d *SyncDaemon) clearImageQuarantine(service string) {
	d.imageQuarantineMu.Lock()
	defer d.imageQuarantineMu.Unlock()
	if d.imageQuarantine == nil {
		metrics.RecordActiveQuarantines(service, 0)
		return
	}
	for k, rec := range d.imageQuarantine {
		if rec.Service == service {
			delete(d.imageQuarantine, k)
		}
	}
	metrics.RecordActiveQuarantines(service, 0)
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

func (d *SyncDaemon) recordPendingTargets(services []string) {
	if d == nil || len(services) == 0 {
		return
	}
	d.pendingTargetsMu.Lock()
	defer d.pendingTargetsMu.Unlock()
	if d.pendingTargets == nil {
		d.pendingTargets = make(map[string]struct{})
	}
	for _, s := range services {
		s = strings.TrimSpace(s)
		if s != "" {
			d.pendingTargets[s] = struct{}{}
		}
	}
}

func (d *SyncDaemon) drainPendingTargets() []string {
	if d == nil {
		return nil
	}
	d.pendingTargetsMu.Lock()
	defer d.pendingTargetsMu.Unlock()
	if len(d.pendingTargets) == 0 {
		return nil
	}
	result := make([]string, 0, len(d.pendingTargets))
	for s := range d.pendingTargets {
		result = append(result, s)
	}
	d.pendingTargets = make(map[string]struct{})
	return result
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

// HasComposeChanges checks whether compose or environment configuration files changed between commits.
func (d *SyncDaemon) HasComposeChanges(ctx context.Context, repoPath, prevHead, currHead string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if prevHead == "" || currHead == "" || prevHead == currHead {
		return false, nil
	}
	if repoPath == "" {
		return false, nil
	}

	composeTargets := composeFileTargets

	args := append([]string{"diff", "--name-only", prevHead, currHead, "--"}, composeTargets...)
	stdout, _, err := d.getGitExecutor()(ctx, repoPath, args...)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// Fallback for shallow clone or disconnected histories
		fallbackArgs := append([]string{"diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD", "--"}, composeTargets...)
		stdoutFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, fallbackArgs...)
		if errFallback != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			log.Printf("[Hangar] Warning: Failed to inspect diff in %s (%v); failing safe to trigger reconcile", repoPath, errFallback)
			return true, nil
		}
		lines := strings.Split(strings.TrimSpace(string(stdoutFallback)), "\n")
		return len(FilterComposeChanges(lines)) > 0, nil
	}

	lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
	return len(FilterComposeChanges(lines)) > 0, nil
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

func (d *SyncDaemon) recordPendingNomadChanges(repoPath string, changes []NomadFileChange) {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	d.pendingNomadChanges = append(d.pendingNomadChanges, NomadChangeEvent{
		RepoPath:  repoPath,
		Changes:   changes,
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

// CleanConflictContainers queries Docker for stopped or exited conflict containers left behind
// by aborted Docker Compose recreation attempts (which rename containers to <short_id>_<service>)
// and purges them to prevent name collision errors on subsequent compose runs.
func (d *SyncDaemon) CleanConflictContainers(ctx context.Context) error {
	args := []string{"ps", "-a", "--filter", "label=com.docker.compose.project=aerial", "--format", "{{.ID}}\t{{.Names}}\t{{.Status}}"}
	stdout, stderr, err := d.getDockerExecutor()(ctx, args...)
	if err != nil {
		log.Printf("[Hangar:ConflictClean] Warning: failed to list containers: %s (%v)", SanitizeLog(strings.TrimSpace(string(stderr))), err)
		return fmt.Errorf("failed to list containers: %s (%w)", SanitizeLog(strings.TrimSpace(string(stderr))), err)
	}

	conflicts := FilterConflictContainers(string(stdout), os.Getenv("HOSTNAME"))
	for _, c := range conflicts {
		shortID := c.ID
		if len(shortID) > 12 {
			shortID = shortID[:12]
		}
		// Remove conflict container
		_, rmErrBytes, rmErr := d.getDockerExecutor()(ctx, "rm", "-f", shortID)
		if rmErr != nil {
			rmErrStr := string(rmErrBytes)
			if strings.Contains(rmErrStr, "No such container") {
				log.Printf("[Hangar:ConflictClean] Container %s already removed", shortID)
			} else {
				log.Printf("[Hangar:ConflictClean] Warning: failed to remove conflict container %s (%s): %s (%v)", shortID, c.Names, SanitizeLog(strings.TrimSpace(rmErrStr)), rmErr)
			}
		} else {
			log.Printf("[Hangar:ConflictClean] Purged orphaned conflict container %s (%s, status: %s)", shortID, c.Names, c.Status)
		}
	}
	return nil
}

// ValidateCompose executes docker compose config --quiet to verify valid syntax and schema before apply.
func (d *SyncDaemon) ValidateCompose(ctx context.Context, composeDir string) error {
	composeFile := filepath.Join(composeDir, "docker-compose.yml")
	if _, err := os.Stat(composeFile); err != nil {
		return fmt.Errorf("compose file not found: %w", err)
	}

	args := d.getComposeArgs(composeDir, "config", "--quiet")
	_, stderr, err := d.getComposeExecutor()(ctx, composeDir, args...)
	if err != nil {
		sanitized := SanitizeLog(strings.TrimSpace(string(stderr)))
		return fmt.Errorf("compose validation failed: %s (%w)", sanitized, err)
	}
	return nil
}

// parseComposeServices parses newline-delimited compose service names, trims whitespace,
// deduplicates entries, and filters out the gitsync sidecar service (case-insensitive).
func parseComposeServices(output string) []string {
	return ParseComposeServices(output)
}

// GetReconcileTargets queries docker compose config --services to discover all defined services,
// filtering out the gitsync sidecar service.
func (d *SyncDaemon) GetReconcileTargets(ctx context.Context, composeDir string) ([]string, error) {
	args := d.getComposeArgs(composeDir, "config", "--services")
	stdout, stderr, err := d.getComposeExecutor()(ctx, composeDir, args...)
	if err != nil {
		sanitized := SanitizeLog(strings.TrimSpace(string(stderr)))
		return nil, fmt.Errorf("failed to discover compose services: %s (%w)", sanitized, err)
	}

	return parseComposeServices(string(stdout)), nil
}

type composeConfigJSON struct {
	Services map[string]struct {
		Image string `json:"image"`
	} `json:"services"`
}

// GetServiceImages inspects docker compose configuration and extracts a mapping of service names
// to their configured container image references, excluding gitsync.
func (d *SyncDaemon) GetServiceImages(ctx context.Context, composeDir string) (map[string]string, error) {
	args := d.getComposeArgs(composeDir, "config", "--format", "json")
	stdout, stderr, err := d.getComposeExecutor()(ctx, composeDir, args...)
	if err != nil {
		sanitized := SanitizeLog(strings.TrimSpace(string(stderr)))
		return nil, fmt.Errorf("failed to inspect compose services: %s (%w)", sanitized, err)
	}

	var parsed composeConfigJSON
	if err := json.Unmarshal(stdout, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse compose config json: %w", err)
	}

	result := make(map[string]string, len(parsed.Services))
	for svcName, svcDef := range parsed.Services {
		svcClean := strings.TrimSpace(svcName)
		if svcClean == "" || strings.EqualFold(svcClean, "gitsync") || strings.EqualFold(svcClean, "hangar") {
			continue
		}
		img := strings.TrimSpace(svcDef.Image)
		if img != "" {
			result[svcClean] = img
		}
	}
	return result, nil
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

// GetServicesWithNewImages inspects running containers for candidate services, compares their image
// RepoDigests with upstream registry manifests, and returns which services need reconciliation.
func (d *SyncDaemon) GetServicesWithNewImages(
	ctx context.Context,
	candidateServices []string,
	serviceImages map[string]string,
) ([]string, map[string]string, error) {
	if len(candidateServices) == 0 {
		return nil, nil, nil
	}

	var servicesToUpdate []string
	newDigests := make(map[string]string)

	for _, svc := range candidateServices {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(svc, "gitsync") || strings.EqualFold(svc, "hangar") {
			continue
		}
		imageRef, ok := serviceImages[svc]
		if !ok || strings.TrimSpace(imageRef) == "" {
			continue
		}

		containerName := "aerial-" + svc
		outImgID, errInspectBytes, errInspect := d.getDockerExecutor()(ctx, "inspect", containerName, "--format", "{{.Image}}")
		if errInspect != nil {
			outImgID, errInspectBytes, errInspect = d.getDockerExecutor()(ctx, "inspect", svc, "--format", "{{.Image}}")
		}
		if errInspect != nil {
			log.Printf("[Hangar:ImagePoll] Warning: container for service %s not inspectable: %s (%v); skipping image check", svc, SanitizeLog(strings.TrimSpace(string(errInspectBytes))), errInspect)
			continue
		}

		imageID := strings.TrimSpace(string(outImgID))
		if imageID == "" {
			continue
		}

		outDigests, errDigestsBytes, errDigests := d.getDockerExecutor()(ctx, "image", "inspect", imageID, "--format", "{{json .RepoDigests}}")
		if errDigests != nil {
			log.Printf("[Hangar:ImagePoll] Warning: image %s for service %s not inspectable: %s (%v); skipping", imageID, svc, SanitizeLog(strings.TrimSpace(string(errDigestsBytes))), errDigests)
			continue
		}

		var repoDigests []string
		if err := json.Unmarshal(outDigests, &repoDigests); err != nil {
			log.Printf("[Hangar:ImagePoll] Warning: failed to parse RepoDigests for service %s: %v", svc, err)
			continue
		}

		remoteDigest, err := d.GetRemoteImageDigest(ctx, imageRef)
		if err != nil {
			log.Printf("[Hangar:ImagePoll] Warning: failed to get remote digest for service %s (%s): %v", svc, imageRef, err)
			continue
		}

		if d.isImageQuarantined(svc, remoteDigest) {
			log.Printf("[Hangar:ImagePoll] Notice: remote digest %s for service %s is quarantined, skipping update", remoteDigest, svc)
			continue
		}

		if !ContainsDigest(repoDigests, remoteDigest) {
			log.Printf("[Hangar:ImagePoll] New image build detected for service %s: remote digest %s not in local repo digests %v", svc, remoteDigest, repoDigests)
			servicesToUpdate = append(servicesToUpdate, svc)
			newDigests[svc] = remoteDigest
		}
	}

	return servicesToUpdate, newDigests, nil
}

// CheckAndReconcileNewImages checks if any running services have newer images available in their registry,
// and triggers an asynchronous, debounced reconciliation if new images are detected.
func (d *SyncDaemon) CheckAndReconcileNewImages(ctx context.Context) error {
	defer func() {
		if nomadErr := d.CheckAndReconcileNomadImages(ctx); nomadErr != nil {
			log.Printf("[Hangar:NomadImagePoll] Warning: checking nomad images: %v", nomadErr)
		}
	}()

	pollCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	composeDir := d.composeDir
	if _, err := os.Stat(filepath.Join(composeDir, "docker-compose.yml")); err != nil {
		return nil
	}

	serviceImages, err := d.GetServiceImages(pollCtx, composeDir)
	if err != nil {
		return fmt.Errorf("failed to get service images: %w", err)
	}

	candidates := make([]string, 0, len(serviceImages))
	for svc := range serviceImages {
		candidates = append(candidates, svc)
	}

	servicesWithNewImages, _, err := d.GetServicesWithNewImages(pollCtx, candidates, serviceImages)
	if err != nil {
		return fmt.Errorf("failed to check services with new images: %w", err)
	}

	if len(servicesWithNewImages) > 0 {
		log.Printf("[Hangar:ImagePoll] Triggering reconciliation for %d services with new images: %v", len(servicesWithNewImages), servicesWithNewImages)
		d.recordPendingTargets(servicesWithNewImages)
		return d.TriggerReconcile(ctx, true)
	}

	return nil
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

func (d *SyncDaemon) isQuarantined(repoPath, commitSHA string) (QuarantineRecord, bool) {
	d.quarantineMu.RLock()
	defer d.quarantineMu.RUnlock()
	if d.quarantinedCommits == nil {
		return QuarantineRecord{}, false
	}
	key := QuarantineKey{
		RepoPath:  filepath.Clean(repoPath),
		CommitSHA: strings.TrimSpace(commitSHA),
	}
	rec, ok := d.quarantinedCommits[key]
	return rec, ok
}

func (d *SyncDaemon) getQuarantineForRepo(repoPath string) (QuarantineRecord, bool) {
	d.quarantineMu.RLock()
	defer d.quarantineMu.RUnlock()
	if d.quarantinedCommits == nil {
		return QuarantineRecord{}, false
	}
	cleaned := filepath.Clean(repoPath)
	for k, rec := range d.quarantinedCommits {
		if k.RepoPath == cleaned {
			return rec, true
		}
	}
	return QuarantineRecord{}, false
}

func (d *SyncDaemon) quarantineCommit(repoPath, commitSHA, prevHead, stage, reason string) {
	d.quarantineMu.Lock()
	defer d.quarantineMu.Unlock()
	if d.quarantinedCommits == nil {
		d.quarantinedCommits = make(map[QuarantineKey]QuarantineRecord)
	}
	key := QuarantineKey{
		RepoPath:  filepath.Clean(repoPath),
		CommitSHA: strings.TrimSpace(commitSHA),
	}
	d.quarantinedCommits[key] = QuarantineRecord{
		RepoPath:      filepath.Clean(repoPath),
		CommitSHA:     strings.TrimSpace(commitSHA),
		PreviousHead:  strings.TrimSpace(prevHead),
		Reason:        SanitizeLog(strings.TrimSpace(reason)),
		FailureStage:  stage,
		QuarantinedAt: time.Now(),
	}
}

func (d *SyncDaemon) clearQuarantineForRepo(repoPath, currentSha string) {
	d.quarantineMu.Lock()
	defer d.quarantineMu.Unlock()
	cleaned := filepath.Clean(repoPath)
	for k := range d.quarantinedCommits {
		if k.RepoPath == cleaned && k.CommitSHA != currentSha {
			delete(d.quarantinedCommits, k)
		}
	}
}

func (d *SyncDaemon) recordPendingChange(change ComposeChangeEvent) {
	d.pendingChangesMu.Lock()
	defer d.pendingChangesMu.Unlock()
	change.RepoPath = filepath.Clean(change.RepoPath)
	d.pendingChanges = append(d.pendingChanges, change)
}

func (d *SyncDaemon) drainPendingChanges() []ComposeChangeEvent {
	d.pendingChangesMu.Lock()
	defer d.pendingChangesMu.Unlock()
	changes := d.pendingChanges
	d.pendingChanges = nil
	return changes
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

// executeRollback safely resets affected repositories to PreviousHead, restores container image snapshots,
// quarantines the faulty commit/digest, and restores container topology with --wait --wait-timeout 180 --remove-orphans --no-build.
func (d *SyncDaemon) executeRollback(
	ctx context.Context,
	pending []ComposeChangeEvent,
	stage string,
	causeErr error,
	targets []string,
	snapshots map[string]string,
	serviceImages map[string]string,
	targetDigests map[string]string,
) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 180*time.Second)
	defer cancel()

	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.State = "rolling_back"
		s.Stage = "rolling_back"
	})

	var rollbackErrs []error

	// 1. Image restoration & quarantining
	if len(targets) == 0 {
		metrics.RecordRollback("compose", stage)
	}
	for _, svc := range targets {
		metrics.RecordRollback(svc, stage)
		if targetDigests != nil && targetDigests[svc] != "" {
			d.quarantineImage(svc, targetDigests[svc], causeErr.Error())
			log.Printf("[Hangar:GitOps] Quarantined failed image digest %s for service %s", targetDigests[svc], svc)
		}

		if snapshots != nil && snapshots[svc] != "" {
			snapshotTag := snapshots[svc]
			origImage := ""
			if serviceImages != nil {
				origImage = serviceImages[svc]
			}
			if origImage != "" {
				if _, tagStderr, tagErr := d.getDockerExecutor()(rollbackCtx, "tag", snapshotTag, origImage); tagErr != nil {
					err := fmt.Errorf("failed to restore image tag for service %s (%s -> %s): %s (%w)", svc, snapshotTag, origImage, SanitizeLog(strings.TrimSpace(string(tagStderr))), tagErr)
					log.Printf("[Hangar:GitOps] Critical: %v", err)
					rollbackErrs = append(rollbackErrs, err)
				} else {
					log.Printf("[Hangar:GitOps] Restored snapshot %s back to %s for service %s", snapshotTag, origImage, svc)
				}
			}
		}
	}

	// 2. Git restoration
	for _, ch := range pending {
		if ch.PreviousHead == "" || ch.RepoPath == "" {
			continue
		}
		repoLock := d.getRepoLock(ch.RepoPath)
		repoLock.Lock()

		outReset, errResetBytes, errReset := d.getGitExecutor()(rollbackCtx, ch.RepoPath, "reset", "--hard", ch.PreviousHead)
		if errReset != nil {
			combined := string(append(outReset, errResetBytes...))
			err := fmt.Errorf("failed to reset %s to %s: %s (%w)", ch.RepoPath, ch.PreviousHead, SanitizeLog(combined), errReset)
			log.Printf("[Hangar:GitOps] Critical: %v", err)
			rollbackErrs = append(rollbackErrs, err)
		} else {
			log.Printf("[Hangar:GitOps] Successfully rolled back %s to %s", ch.RepoPath, ch.PreviousHead)
		}

		d.quarantineCommit(ch.RepoPath, ch.CurrentHead, ch.PreviousHead, stage, causeErr.Error())
		repoLock.Unlock()

		d.SendDiscordAlert(context.Background(), "Docker Compose Failure (Rolled Back)", ch.RepoPath, ch.CurrentHead, ch.PreviousHead, stage, causeErr.Error())
	}

	composeDir := d.composeDir

	if cleanErr := d.CleanConflictContainers(rollbackCtx); cleanErr != nil {
		log.Printf("[Hangar:ConflictClean] Warning: failed cleaning conflict containers during rollback: %v", cleanErr)
	}

	restoredTargets, errTargets := d.GetReconcileTargets(rollbackCtx, composeDir)
	if errTargets != nil {
		err := fmt.Errorf("failed to discover targets after rollback in %s: %w", composeDir, errTargets)
		log.Printf("[Hangar:GitOps] Critical: %v", err)
		rollbackErrs = append(rollbackErrs, err)
		d.SendDiscordAlert(context.Background(), "CRITICAL: Rollback Target Discovery Failed", composeDir, "", "", "rollback target discovery", err.Error())
		return errors.Join(rollbackErrs...)
	}
	if len(restoredTargets) == 0 {
		return errors.Join(rollbackErrs...)
	}

	upArgs := append([]string{"up", "-d", "--wait", "--wait-timeout", "180", "--remove-orphans", "--no-build"}, restoredTargets...)
	stdoutUp, stderrUp, errUp := d.getComposeExecutor()(rollbackCtx, composeDir, d.getComposeArgs(composeDir, upArgs...)...)
	combinedUp := string(append(stdoutUp, stderrUp...))
	if errUp != nil {
		err := fmt.Errorf("re-applying restored configuration failed: %s (%w)", SanitizeLog(combinedUp), errUp)
		log.Printf("[Hangar:GitOps] CRITICAL: %v", err)
		rollbackErrs = append(rollbackErrs, err)
		d.SendDiscordAlert(context.Background(), "🚨 CRITICAL: Rollback Re-Apply Failed", composeDir, "", "", "rollback re-apply", fmt.Sprintf("Failed to restore containers to previous configuration: %s (%v)", SanitizeLog(combinedUp), errUp))
	} else {
		log.Printf("[Hangar:GitOps] Restored previous container topology successfully (%s)", SanitizeLog(combinedUp))
		if len(pending) == 0 {
			d.SendDiscordAlert(context.Background(), "Docker Compose Image Failure (Rolled Back)", composeDir, "", "", stage, causeErr.Error())
		}
	}

	// Clean temporary snapshots
	for _, snapshotTag := range snapshots {
		if _, rmiStderr, rmiErr := d.getDockerExecutor()(rollbackCtx, "rmi", snapshotTag); rmiErr != nil {
			log.Printf("[Hangar:GitOps] Notice: rollback temporary snapshot tag %s cleanup: %s (%v)", snapshotTag, SanitizeLog(strings.TrimSpace(string(rmiStderr))), rmiErr)
		}
	}

	return errors.Join(rollbackErrs...)
}

// ReconcileCompose executes docker compose pull and up -d --wait with timeout, metrics observation,
// pre-flight image snapshotting, and health-gated automated rollback.
func (d *SyncDaemon) ReconcileCompose(parentCtx context.Context, targetServices ...string) (err error) {
	if d.reconcileFn != nil {
		return d.reconcileFn(parentCtx)
	}

	d.composeMu.Lock()
	defer d.composeMu.Unlock()

	start := time.Now()
	defer func() {
		metrics.RecordReconciliation(metrics.SanitizeStatus(err), time.Since(start))
	}()

	composeDir := d.composeDir

	if _, statErr := os.Stat(filepath.Join(composeDir, "docker-compose.yml")); statErr != nil {
		log.Printf("[Hangar:GitOps] Notice: No docker-compose.yml found in %s, skipping reconciliation", composeDir)
		return nil
	}

	pending := d.drainPendingChanges()
	drainedTargets := d.drainPendingTargets()

	// 1. Conflict container cleanup & pre-flight validation gate
	valCtx, valCancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer valCancel()

	// Ensure Docker registry credentials are configured for private images before compose apply
	if authErr := d.EnsureDockerAuth(); authErr != nil {
		log.Printf("[Hangar:GitOps] Warning: Failed to refresh Docker auth before reconciliation: %v", d.SanitizeAll(authErr.Error()))
	}

	if cleanErr := d.CleanConflictContainers(valCtx); cleanErr != nil {
		log.Printf("[Hangar:ConflictClean] Warning: failed cleaning conflict containers before reconciliation: %v", cleanErr)
	}

	if valErr := d.ValidateCompose(valCtx, composeDir); valErr != nil {
		log.Printf("[Hangar:GitOps] ERROR: Pre-flight validation failed: %v. Initiating automated rollback.", valErr)
		rollbackErr := d.executeRollback(parentCtx, pending, "pre-flight validation", valErr, nil, nil, nil, nil)
		if rollbackErr != nil {
			return errors.Join(valErr, rollbackErr)
		}
		return valErr
	}

	// 2. Discover target services excluding gitsync
	serviceImages, errImages := d.GetServiceImages(valCtx, composeDir)
	if errImages != nil {
		log.Printf("[Hangar:GitOps] Warning: Failed to inspect service images: %v", errImages)
	}

	var targets []string
	trigger := "git_sync"
	if len(targetServices) > 0 && len(pending) == 0 {
		targets = targetServices
		trigger = "manual_api"
	} else if len(drainedTargets) > 0 && len(pending) == 0 {
		targets = drainedTargets
		trigger = "image_poll"
	} else {
		allTargets, targetErr := d.GetReconcileTargets(valCtx, composeDir)
		if targetErr != nil {
			log.Printf("[Hangar:GitOps] ERROR: Service discovery failed: %v. Initiating automated rollback.", targetErr)
			rollbackErr := d.executeRollback(parentCtx, pending, "service discovery", targetErr, nil, nil, nil, nil)
			if rollbackErr != nil {
				return errors.Join(targetErr, rollbackErr)
			}
			return targetErr
		}
		targets = allTargets
		trigger = "git_sync"
	}

	if len(targets) == 0 {
		log.Printf("[Hangar:GitOps] Notice: No external services to reconcile in %s (gitsync excluded)", composeDir)
		return nil
	}

	diskHead := ""
	if out, _, hErr := d.getGitExecutor()(valCtx, composeDir, "rev-parse", "HEAD"); hErr == nil {
		diskHead = strings.TrimSpace(string(out))
		if len(diskHead) > 7 {
			diskHead = diskHead[:7]
		}
	}

	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.Active = true
		s.State = "pulling"
		s.Stage = "pulling"
		s.Trigger = trigger
		s.CommitSHA = diskHead
		s.TargetServices = append([]string(nil), targets...)
		s.StartedAt = time.Now().UTC()
		s.CompletedAt = time.Time{}
		s.Error = ""
	})

	defer func() {
		d.updateReconcileStatus(func(s *ReconciliationStatus) {
			s.Active = false
			s.CompletedAt = time.Now().UTC()
			if err != nil {
				s.State = "failed"
				s.Stage = "failed"
				s.Error = err.Error()
			} else {
				s.State = "healthy"
				s.Stage = "healthy"
				s.Error = ""
			}
		})
	}()

	// 3. Pre-flight snapshotting of running container images BEFORE pull
	snapshots := make(map[string]string)
	targetDigests := make(map[string]string)

	for _, svc := range targets {
		containerName := "aerial-" + svc
		outImgID, _, err := d.getDockerExecutor()(valCtx, "inspect", containerName, "--format", "{{.Image}}")
		if err != nil {
			outImgID, _, err = d.getDockerExecutor()(valCtx, "inspect", svc, "--format", "{{.Image}}")
		}
		if err == nil && len(bytes.TrimSpace(outImgID)) > 0 {
			runningImageID := string(bytes.TrimSpace(outImgID))
			snapshotTag := RollbackTagForService(svc)
			_, tagStderr, tagErr := d.getDockerExecutor()(valCtx, "tag", runningImageID, snapshotTag)
			if tagErr != nil {
				log.Printf("[Hangar:GitOps] Warning: failed to snapshot %s (%s -> %s): %s (%v)", svc, runningImageID, snapshotTag, SanitizeLog(strings.TrimSpace(string(tagStderr))), tagErr)
			} else {
				snapshots[svc] = snapshotTag
			}
		}
	}

	// 4. Pre-flight pull
	pullTimeout := d.composePullTimeout
	if pullTimeout <= 0 {
		pullTimeout = defaultComposePullTimeout
	}
	pullCtx, pullCancel := context.WithTimeout(parentCtx, pullTimeout)
	defer pullCancel()

	pullArgs := append([]string{"pull"}, targets...)
	pullStdout, pullStderr, pullErr := d.getComposeExecutor()(pullCtx, composeDir, d.getComposeArgs(composeDir, pullArgs...)...)
	if pullErr != nil {
		combinedPull := string(append(pullStdout, pullStderr...))
		sanitizedPull := SanitizeLog(strings.TrimSpace(combinedPull))
		log.Printf("[Hangar:GitOps] ERROR: docker compose pull failed: %s (%v). Initiating automated rollback.", sanitizedPull, pullErr)
		rollbackErr := d.executeRollback(parentCtx, pending, "compose pull", fmt.Errorf("%s (%w)", sanitizedPull, pullErr), targets, snapshots, serviceImages, targetDigests)
		if rollbackErr != nil {
			return errors.Join(fmt.Errorf("compose pull failed: %s (%w)", sanitizedPull, pullErr), rollbackErr)
		}
		return fmt.Errorf("compose pull failed: %s (%w)", sanitizedPull, pullErr)
	}

	// 5. Bounded compose execution with 210s context timeout for --wait-timeout 180
	ctx, cancel := context.WithTimeout(parentCtx, 210*time.Second)
	defer cancel()

	log.Printf("[Hangar:GitOps] Reconciling Docker Compose state for %d services (%v) in %s...", len(targets), targets, composeDir)

	d.updateReconcileStatus(func(s *ReconciliationStatus) {
		s.State = "swapping"
		s.Stage = "swapping"
	})

	upArgs := append([]string{"up", "-d", "--wait", "--wait-timeout", "180", "--remove-orphans", "--no-build"}, targets...)
	stdout, stderr, cmdErr := d.getComposeExecutor()(ctx, composeDir, d.getComposeArgs(composeDir, upArgs...)...)
	combined := string(append(stdout, stderr...))
	sanitized := SanitizeLog(strings.TrimSpace(combined))

	if cmdErr != nil {
		log.Printf("[Hangar:GitOps] ERROR: docker compose up failed: %s (%v). Initiating automated rollback.", sanitized, cmdErr)
		// Capture current remote digest for quarantine
		queryCtx, queryCancel := context.WithTimeout(context.WithoutCancel(parentCtx), 10*time.Second)
		for _, svc := range targets {
			if serviceImages != nil {
				if imgRef, ok := serviceImages[svc]; ok {
					dgst, err := d.GetRemoteImageDigest(queryCtx, imgRef)
					if err != nil {
						log.Printf("[Hangar:GitOps] Warning: failed to fetch remote digest for quarantine of %s (%s): %v", svc, imgRef, err)
					} else if dgst != "" {
						targetDigests[svc] = dgst
					}
				}
			}
		}
		queryCancel()
		rollbackErr := d.executeRollback(parentCtx, pending, "compose apply", fmt.Errorf("%s (%w)", sanitized, cmdErr), targets, snapshots, serviceImages, targetDigests)
		if rollbackErr != nil {
			return errors.Join(fmt.Errorf("compose up failed: %s (%w)", sanitized, cmdErr), rollbackErr)
		}
		return fmt.Errorf("compose up failed: %s (%w)", sanitized, cmdErr)
	}

	if sanitized != "" {
		log.Printf("[Hangar:GitOps] Reconcile output: %s", sanitized)
	}
	log.Printf("[Hangar:GitOps] Docker Compose reconciliation successfully applied.")

	hasNomadConfigChange := false
	for _, p := range pending {
		if p.NomadConfigChanged {
			hasNomadConfigChange = true
			break
		}
	}
	if hasNomadConfigChange {
		if errNomad := d.reconcileNomadServer(ctx, composeDir); errNomad != nil {
			log.Printf("[Hangar:GitOps] Warning: reconcileNomadServer encountered error: %v", errNomad)
		}
	}

	// Clear quarantine for successful targets
	for _, svc := range targets {
		d.clearImageQuarantine(svc)
	}

	// Delete temporary snapshots
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(parentCtx), 30*time.Second)
	defer cleanupCancel()
	for _, snapshotTag := range snapshots {
		if _, rmiStderr, rmiErr := d.getDockerExecutor()(cleanupCtx, "rmi", snapshotTag); rmiErr != nil {
			log.Printf("[Hangar:GitOps] Notice: temporary snapshot tag %s cleanup: %s (%v)", snapshotTag, SanitizeLog(strings.TrimSpace(string(rmiStderr))), rmiErr)
		}
	}

	d.SetLastReconcile(time.Now())
	return nil
}

// TriggerReconcile requests Docker Compose reconciliation. If async is true, it queues
// reconciliation onto the debounced worker channel. If async is false, it executes
// ReconcileCompose synchronously.
func (d *SyncDaemon) TriggerReconcile(ctx context.Context, async bool) error {
	if !async {
		return d.ReconcileCompose(ctx)
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

// ReconcileNomadChanges validates and applies or stops Nomad job specifications.
func (d *SyncDaemon) ReconcileNomadChanges(ctx context.Context, repoPath string, changes []NomadFileChange) error {
	if len(changes) == 0 {
		return nil
	}

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	recCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// Phase 1: Teardowns (stop deleted jobs)
	for _, change := range changes {
		if change.Action == "delete" {
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
				Status:    "started",
				Timestamp: time.Now().UTC(),
			}
			d.DispatchDeployEvent(baseEvt)

			log.Printf("[Hangar:Nomad] Applying job %s (%s)...", change.JobName, change.Path)
			out, errBytes, err := d.getNomadExecutor()(recCtx, "job", "run", "-detach=false", fullPath)
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
			d.MonitorDeploymentAsync(change.JobName, 0, baseEvt)
			log.Printf("[Hangar:Nomad] Successfully applied job %s: %s", change.JobName, SanitizeLog(strings.TrimSpace(string(out))))
		}
	}

	return nil
}

// ReconcileNomadJobs is an alias for ReconcileNomadChanges.
func (d *SyncDaemon) ReconcileNomadJobs(ctx context.Context, repoPath string, changes []NomadFileChange) error {
	return d.ReconcileNomadChanges(ctx, repoPath, changes)
}

// ExecuteNomadTeardowns runs nomad job stop -purge for any deleted jobs across pending changes.
func (d *SyncDaemon) ExecuteNomadTeardowns(ctx context.Context, pending []NomadChangeEvent) {
	if len(pending) == 0 {
		return
	}
	for _, evt := range pending {
		for _, change := range evt.Changes {
			if change.Action == "delete" {
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
		if err := d.ReconcileNomadChanges(ctx, evt.RepoPath, evt.Changes); err != nil {
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

	jobsDir := filepath.Join(d.configDir, "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return nil
	}

	for _, entry := range entries {
		if entry.IsDir() || !IsNomadJobFile(entry.Name()) {
			continue
		}
		jobPath := filepath.Join(jobsDir, entry.Name())
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

				out, errBytes, runErr := d.getNomadExecutor()(pollCtx, "job", "restart", "-reschedule", jobName)
				if runErr != nil {
					log.Printf("[Hangar:NomadImagePoll] Warning: nomad job restart failed for %s: %s (%v)", jobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), runErr)
				} else {
					log.Printf("[Hangar:NomadImagePoll] Successfully triggered reschedule restart for Nomad job %s", jobName)
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

					// Phase 2: Docker Compose reconciliation
					if recErr := d.ReconcileCompose(context.Background()); recErr != nil {
						log.Printf("[Hangar:GitOps] Debounced background reconcile failed: %v", recErr)
					}

					// Phase 3: Nomad reconciliation (job applications)
					if nomadErr := d.ReconcilePendingNomad(context.Background()); nomadErr != nil {
						log.Printf("[Hangar:GitOps] Debounced Nomad reconcile failed: %v", nomadErr)
					}
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
func (d *SyncDaemon) SyncRepo(ctx context.Context, repoPath string) (res RepoSyncResult) {
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
		// 2. Inspect fetched commit SHA for active quarantine
		if outFetchHead, _, errFetchHead := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "FETCH_HEAD"); errFetchHead == nil {
			fetchedSha := strings.TrimSpace(string(outFetchHead))
			if rec, quarantined := d.isQuarantined(repoPath, fetchedSha); quarantined {
				log.Printf("[Hangar] Notice: remote commit %s for %s is quarantined (reason: %s). Skipping pull to prevent failure loop.", fetchedSha, repoPath, rec.Reason)
				res.Error = fmt.Sprintf("commit %s is quarantined: %s", fetchedSha, rec.Reason)
				return res
			}
		}

		// 3. Fast-forward merge verified clean upstream commit
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
		d.clearQuarantineForRepo(repoPath, res.CurrentHead)
		log.Printf("[Hangar] Repository %s updated: %s -> %s", repoPath, res.PreviousHead, res.CurrentHead)

		composeChanged, errCompose := d.HasComposeChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead)
		if errCompose != nil {
			log.Printf("[Hangar] Warning: Failed to check compose changes in %s: %v", repoPath, errCompose)
			res.Error = fmt.Sprintf("compose change check failed: %v", errCompose)
		}
		if composeChanged {
			res.ComposeChanged = true
			log.Printf("[Hangar:GitOps] Infrastructure/compose changes detected in %s (%s -> %s). Triggering debounced reconciliation.", repoPath, res.PreviousHead, res.CurrentHead)
			nomadCfgChanged, errNomadCfg := d.HasNomadConfigChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead)
			if errNomadCfg != nil {
				log.Printf("[Hangar] Warning: Failed to check nomad config changes in %s: %v", repoPath, errNomadCfg)
			}
			d.recordPendingChange(ComposeChangeEvent{
				RepoPath:           repoPath,
				PreviousHead:       res.PreviousHead,
				CurrentHead:        res.CurrentHead,
				Timestamp:          time.Now(),
				NomadConfigChanged: nomadCfgChanged,
			})
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
			d.recordPendingNomadChanges(repoPath, nomadChanges)
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
			go d.notifyBrainReload()
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

// notifyBrainReload sends a best-effort POST request to Brain's internal reload endpoint.
func (d *SyncDaemon) notifyBrainReload() {
	brainURL := d.brainInternalURL
	if brainURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, brainURL, nil)
	if err != nil {
		log.Printf("[Hangar] Warning: failed to create brain reload request: %v", err)
		return
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Hangar] Notice: brain reload trigger failed: %v", err)
		return
	}
	defer closeWarn(resp.Body, "brain reload response body")
	log.Printf("[Hangar] Successfully dispatched internal reload trigger to Brain (%s)", brainURL)
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

		// Check quarantine state
		if rec, quarantined := d.isQuarantined(repo, remoteSha); quarantined {
			st.SyncStatus = "quarantined"
			st.Quarantined = true
			st.QuarantinedCommit = rec.CommitSHA
			st.QuarantineReason = rec.Reason
		} else if rec, quarantined := d.isQuarantined(repo, diskSha); quarantined {
			st.SyncStatus = "quarantined"
			st.Quarantined = true
			st.QuarantinedCommit = rec.CommitSHA
			st.QuarantineReason = rec.Reason
		} else if rec, quarantined := d.getQuarantineForRepo(repo); quarantined {
			st.SyncStatus = "quarantined"
			st.Quarantined = true
			st.QuarantinedCommit = rec.CommitSHA
			st.QuarantineReason = rec.Reason
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
	ComposePullTimeout     time.Duration
	WebhooksRouterURL      string
	DeployDispatcher       func(ctx context.Context, evt HangarDeployEvent) error
	DeploymentPollInterval time.Duration
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
	pullTimeout := cfg.ComposePullTimeout
	if pullTimeout <= 0 {
		if raw := os.Getenv("COMPOSE_PULL_TIMEOUT"); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d > 0 {
				pullTimeout = d
			}
		}
	}
	if pullTimeout <= 0 {
		pullTimeout = defaultComposePullTimeout
	}
	webhooksRouterURL := cfg.WebhooksRouterURL
	if webhooksRouterURL == "" {
		if envVal := os.Getenv("WEBHOOKS_ROUTER_URL"); envVal != "" {
			webhooksRouterURL = envVal
		} else {
			webhooksRouterURL = "http://127.0.0.1:4020"
		}
	}
	pollInterval := cfg.DeploymentPollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
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
		composePullTimeout:     pullTimeout,
		webhooksRouterURL:      webhooksRouterURL,
		deployDispatcher:       cfg.DeployDispatcher,
		deploymentPollInterval: pollInterval,
		reconcileCh:            make(chan struct{}, 1),
		repoLocks:              make(map[string]*sync.Mutex),
		quarantinedCommits:     make(map[QuarantineKey]QuarantineRecord),
		imageQuarantine:        make(map[string]ImageQuarantineRecord),
		reconcileStatus: ReconciliationStatus{
			State: "idle",
			Stage: "idle",
		},
		pendingTargets:  make(map[string]struct{}),
		lastAlertTimes:  make(map[string]time.Time),
		repoGitHubSlugs: make(map[string]string),
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
	var repo, commit string
	if len(meta) >= 1 {
		repo = meta[0]
	}
	if len(meta) >= 2 {
		commit = meta[1]
	}

	var applied []string
	for _, ch := range changes {
		if ch.Action == "delete" {
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
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(baseEvt)

		out, errBytes, errRun := d.getNomadExecutor()(ctx, "job", "run", "-detach=false", fullPath)
		if errRun != nil {
			failEvt := baseEvt
			failEvt.Event = "deploy_failed"
			failEvt.Status = "failed"
			failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
			failEvt.Timestamp = time.Now().UTC()
			d.DispatchDeployEvent(failEvt)
			return applied, errRun
		}

		d.MonitorDeploymentAsync(ch.JobName, 0, baseEvt)
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

	res := d.SyncRepo(ctx, repoPath)
	if res.Error != "" {
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
			applied, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, req.Repo, req.Commit)
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
			var appliedJobs []string
			var deferredJobs []string

			for _, ch := range nomadChanges {
				if ch.Action == "delete" {
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
							Status:    "started",
							Timestamp: time.Now().UTC(),
						}
						d.DispatchDeployEvent(baseEvt)

						out, errBytes, errRun := d.getNomadExecutor()(ctx, "job", "run", "-detach=false", fullPath)
						if errRun != nil {
							log.Printf("[Hangar:GitPush] nomad job run failed for %s: %s (%v)", ch.JobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), errRun)
							failEvt := baseEvt
							failEvt.Event = "deploy_failed"
							failEvt.Status = "failed"
							failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
							failEvt.Timestamp = time.Now().UTC()
							d.DispatchDeployEvent(failEvt)
						} else {
							d.MonitorDeploymentAsync(ch.JobName, 0, baseEvt)
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

		go d.notifyBrainReload()
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

	jobsDir := filepath.Join(d.configDir, "jobs")
	matches := FindNomadJobsByImage(jobsDir, imgRef)
	if len(matches) == 0 && d.composeDir != "" {
		coreJobsDir := filepath.Join(d.composeDir, "nomad", "jobs")
		matches = append(matches, FindNomadJobsByImage(coreJobsDir, imgRef)...)
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

	var appliedJobs []string
	for _, job := range matches {
		if errVal := d.ValidateNomadJob(ctx, job.JobPath); errVal != nil {
			log.Printf("[Hangar:ImageReady] Validation failed for job %s (%s): %v", job.JobName, job.JobPath, errVal)
			continue
		}

		baseEvt := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   job.JobName,
			Image:     req.Image,
			Digest:    req.Digest,
			Status:    "started",
			Timestamp: time.Now().UTC(),
		}
		d.DispatchDeployEvent(baseEvt)

		log.Printf("[Hangar:ImageReady] Applying Nomad job %s from %s...", job.JobName, job.JobPath)
		outRun, errRunBytes, errRun := d.getNomadExecutor()(ctx, "job", "run", "-detach=false", job.JobPath)
		if errRun != nil {
			log.Printf("[Hangar:ImageReady] Warning: job run failed for %s: %s (%v)", job.JobName, SanitizeLog(strings.TrimSpace(string(append(outRun, errRunBytes...)))), errRun)
			failEvt := baseEvt
			failEvt.Event = "deploy_failed"
			failEvt.Status = "failed"
			failEvt.Details = SanitizeLog(strings.TrimSpace(string(append(outRun, errRunBytes...))))
			failEvt.Timestamp = time.Now().UTC()
			d.DispatchDeployEvent(failEvt)
			continue
		}

		log.Printf("[Hangar:ImageReady] Rescheduling allocation restart for job %s...", job.JobName)
		outRest, errRestBytes, errRest := d.getNomadExecutor()(ctx, "job", "restart", "-reschedule", job.JobName)
		if errRest != nil {
			log.Printf("[Hangar:ImageReady] Warning: job restart -reschedule failed for %s: %s (%v)", job.JobName, SanitizeLog(strings.TrimSpace(string(append(outRest, errRestBytes...)))), errRest)
		}

		d.MonitorDeploymentAsync(job.JobName, 0, baseEvt)
		appliedJobs = append(appliedJobs, job.JobName)

		if req.Digest != "" {
			key := job.JobName + ":" + imgRef
			d.nomadDigestsMu.Lock()
			if d.nomadKnownDigests == nil {
				d.nomadKnownDigests = make(map[string]string)
			}
			d.nomadKnownDigests[key] = req.Digest
			d.nomadDigestsMu.Unlock()
		}
	}

	if len(appliedJobs) == 0 {
		return ImageReadyEventResponse{
			Status:  "error",
			Image:   imgRef,
			Message: "failed to apply or restart any matching nomad jobs",
		}, http.StatusInternalServerError
	}

	return ImageReadyEventResponse{
		Status:      "accepted",
		Image:       imgRef,
		MatchedJobs: appliedJobs,
		Message:     fmt.Sprintf("successfully applied and rescheduled %d job(s)", len(appliedJobs)),
	}, http.StatusOK
}

// SetupMux configures HTTP handlers for metrics, health, status, and sync.
func SetupMux(daemon *SyncDaemon) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
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
			"message":          "Docker Compose reconciliation scheduled",
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

		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()

		resp, code := daemon.ExecuteImageReadyEvent(ctx, req)
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

// NewConfigFromLookup extracts DaemonConfig using a provided lookup function.
// If lookup is nil, it safely defaults to returning empty strings without ambient env access.
func NewConfigFromLookup(lookup func(string) string) DaemonConfig {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}

	port := lookup("PORT")
	if port == "" {
		port = "8080"
	}

	rawRepos := lookup("SYNC_REPOS")
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

	intervalStr := lookup("SYNC_INTERVAL")
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
	if brainInternalURL == "" {
		brainInternalURL = "http://brain:8080/internal/reload"
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
	if nomadAddr == "" {
		nomadAddr = "http://host.docker.internal:4646"
	}
	nomadToken := lookup("NOMAD_TOKEN")

	webhooksRouterURL := lookup("WEBHOOKS_ROUTER_URL")
	if webhooksRouterURL == "" {
		webhooksRouterURL = "http://127.0.0.1:4020"
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
	}
}

// NewConfigFromEnv extracts DaemonConfig from environment variables with sensible defaults.
func NewConfigFromEnv() DaemonConfig {
	return NewConfigFromLookup(os.Getenv)
}

func main() {
	cfg := NewConfigFromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunDaemon(ctx, cfg); err != nil {
		log.Fatalf("[Hangar] Fatal error: %v", err)
	}
}
