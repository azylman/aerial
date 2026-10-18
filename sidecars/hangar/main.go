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
	secrets := append([]string{d.pat}, d.extraSecrets...)
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
	Repo             string
	CommitSHA        string
	PRNumber         int
	TargetID         string
	ChangedFiles     []string
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

// GetReconciliationStatus returns a safe snapshot of the current or recent reconciliation status.
func (d *SyncDaemon) GetReconciliationStatus() ReconciliationStatus {
	return ReconciliationStatus{State: "idle", Stage: "idle"}
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

// runGitDiffWithFallback executes git diff or diff-tree when prevHead is all zeros or primary diff fails.
func (d *SyncDaemon) runGitDiffWithFallback(ctx context.Context, repoPath, diffFlag, prevHead, currHead, fallbackRef string, pathArgs ...string) (string, error) {
	if isAllZeros(prevHead) {
		treeArgs := []string{"diff-tree", "--no-commit-id", diffFlag, "-r", fallbackRef}
		if len(pathArgs) > 0 {
			treeArgs = append(append(treeArgs, "--"), pathArgs...)
		}
		out, _, err := d.getGitExecutor()(ctx, repoPath, treeArgs...)
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	diffArgs := []string{"diff", diffFlag, prevHead, currHead}
	if len(pathArgs) > 0 {
		diffArgs = append(append(diffArgs, "--"), pathArgs...)
	}
	out, _, err := d.getGitExecutor()(ctx, repoPath, diffArgs...)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		treeArgs := []string{"diff-tree", "--no-commit-id", diffFlag, "-r", fallbackRef}
		if len(pathArgs) > 0 {
			treeArgs = append(append(treeArgs, "--"), pathArgs...)
		}
		outFallback, _, errFallback := d.getGitExecutor()(ctx, repoPath, treeArgs...)
		if errFallback != nil {
			return "", errFallback
		}
		return string(outFallback), nil
	}

	return string(out), nil
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

	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-only", prevHead, currHead, "HEAD", "nomad")
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		log.Printf("[Hangar] Warning: Failed to inspect nomad diff in %s (%v); failing safe to false", repoPath, err)
		return false, nil
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
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

	out, err := d.runGitDiffWithFallback(ctx, repoPath, "--name-status", prevHead, currHead, "HEAD")
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Printf("[Hangar:Nomad] Warning: Failed to inspect diff in %s (%v)", repoPath, err)
		return nil, nil
	}

	return ParseNomadGitStatus(out), nil
}

func parseNomadMeta(meta []string) (repo, commit, targetID string, prNumber int) {
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
	return repo, commit, targetID, prNumber
}
func (d *SyncDaemon) recordPendingNomadChanges(repoPath string, changes []NomadFileChange, meta ...string) {
	d.pendingNomadMu.Lock()
	defer d.pendingNomadMu.Unlock()
	repo, commit, targetID, prNumber := parseNomadMeta(meta)
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
	resp, _, err := d.doRegistryRequestWithAuth(ctx, http.MethodHead, manifestURL, registry, repository, acceptManifestHeaders, "")
	if err != nil {
		return "", err
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

func (d *SyncDaemon) doRegistryRequestWithAuth(ctx context.Context, method, targetURL, registry, repository, acceptHeader, existingBearer string) (*http.Response, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed creating request for %s: %w", targetURL, err)
	}
	if acceptHeader != "" {
		req.Header.Set("Accept", acceptHeader)
	}
	if existingBearer != "" {
		req.Header.Set("Authorization", "Bearer "+existingBearer)
	}

	resp, err := d.getRegistryClient().Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request failed for %s: %w", targetURL, err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, existingBearer, nil
	}

	if existingBearer != "" {
		closeWarn(resp.Body, "unauthorized response body with existing bearer")
	} else {
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

	authReq, aErr := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if aErr != nil {
		return nil, "", fmt.Errorf("failed creating authenticated request for %s: %w", targetURL, aErr)
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

func (d *SyncDaemon) fetchRegistryBytes(ctx context.Context, targetURL, registry, repository, acceptHeader, existingBearer, label string) ([]byte, string, error) {
	resp, token, err := d.doRegistryRequestWithAuth(ctx, http.MethodGet, targetURL, registry, repository, acceptHeader, existingBearer)
	if err != nil {
		return nil, "", err
	}
	defer closeWarn(resp.Body, label+" response body")

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("%s request returned HTTP %d", label, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", fmt.Errorf("failed to read %s body: %w", label, err)
	}
	return bodyBytes, token, nil
}

// GetRemoteImageRevision retrieves the git commit revision associated with an OCI/Docker container image
// by inspecting manifest annotations or configuration blob labels.
func (d *SyncDaemon) GetRemoteImageRevision(ctx context.Context, imageRef string) (string, error) {
	registry, repository, tagOrDigest, err := ParseImageReference(imageRef)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", imageRef, err)
	}

	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, tagOrDigest)
	bodyBytes, bearerToken, err := d.fetchRegistryBytes(ctx, manifestURL, registry, repository, acceptManifestHeaders, "", fmt.Sprintf("manifest for %s", imageRef))
	if err != nil {
		return "", err
	}

	// Check if this is an OCI Image Index or Docker Manifest List
	childDigest := ExtractPlatformManifestDigest(bodyBytes, "linux", "amd64")
	if childDigest != "" {
		if rev := ExtractRevisionFromAnnotations(ExtractManifestAnnotations(bodyBytes)); rev != "" {
			return rev, nil
		}

		childURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", registry, repository, childDigest)
		childBytes, childToken, childErr := d.fetchRegistryBytes(ctx, childURL, registry, repository, acceptManifestHeaders, bearerToken, fmt.Sprintf("child manifest for %s (%s)", imageRef, childDigest))
		if childErr != nil {
			return "", childErr
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
	blobBytes, _, blobErr := d.fetchRegistryBytes(ctx, blobURL, registry, repository, acceptBlobHeaders, bearerToken, fmt.Sprintf("config blob for %s (%s)", imageRef, configDigest))
	if blobErr != nil {
		return "", blobErr
	}

	rev := ExtractRevisionFromConfigJSON(blobBytes)
	if rev == "" {
		return "", fmt.Errorf("no valid revision label found in config blob for %s", imageRef)
	}

	return rev, nil
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
	reconcilerServerReadyDelay = 3 * time.Second
)

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}


func (d *SyncDaemon) runNomadValidate(ctx context.Context, subCmd, targetPath string, requireNonNilExec bool) error {
	valCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	nomadExec := d.getNomadExecutor()
	if requireNonNilExec && nomadExec == nil {
		return errors.New("nomad executor unavailable, failing safe")
	}
	stdout, stderr, err := nomadExec(valCtx, subCmd, "validate", targetPath)
	if err != nil {
		combined := string(append(stdout, stderr...))
		return fmt.Errorf("nomad %s validate failed for %s: %s (%w)", subCmd, targetPath, SanitizeLog(strings.TrimSpace(combined)), err)
	}
	return nil
}

// ValidateNomadJob validates a Nomad job specification using nomad job validate.
func (d *SyncDaemon) ValidateNomadJob(ctx context.Context, jobPath string) error {
	return d.runNomadValidate(ctx, "job", jobPath, false)
}

// ValidateNomadConfig validates a Nomad daemon configuration path using nomad config validate.
func (d *SyncDaemon) ValidateNomadConfig(ctx context.Context, configPath string) error {
	return d.runNomadValidate(ctx, "config", configPath, true)
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

// ReconcileNomadChanges validates and applies or stops Nomad job specifications.
func (d *SyncDaemon) stopDeletedNomadJob(ctx context.Context, logPrefix, repoPath, jobName string) {
	if survivingPath, found := FindJobDefinitionInRepos(jobName, repoPath, d.candidateJobRepos()); found {
		log.Printf("%s Notice: skipping stop for deleted job %s (deleted from %s): job is still defined in %s", logPrefix, jobName, repoPath, survivingPath)
		return
	}
	out, errBytes, err := d.getNomadExecutor()(ctx, "job", "stop", "-purge", jobName)
	if err != nil {
		log.Printf("%s Warning: failed to stop deleted job %s: %s (%v)", logPrefix, jobName, SanitizeLog(strings.TrimSpace(string(append(out, errBytes...)))), err)
	} else {
		log.Printf("%s Successfully stopped deleted job %s", logPrefix, jobName)
	}
}

func (d *SyncDaemon) runNomadJobWithEvents(ctx context.Context, baseEvt HangarDeployEvent, runArgs ...string) (out []byte, sanitizedCombined string, err error) {
	baseEvt.Event = "deploy_started"
	baseEvt.Status = "started"
	baseEvt.Timestamp = time.Now().UTC()
	d.DispatchDeployEvent(baseEvt)

	var errBytes []byte
	out, errBytes, err = d.getNomadExecutor()(ctx, runArgs...)
	if err != nil {
		sanitizedCombined = SanitizeLog(strings.TrimSpace(string(append(out, errBytes...))))
		failEvt := baseEvt
		failEvt.Event = "deploy_failed"
		failEvt.Status = "failed"
		failEvt.Details = sanitizedCombined
		failEvt.Timestamp = time.Now().UTC()
		d.DispatchDeployEvent(failEvt)
	}
	return out, sanitizedCombined, err
}

func (d *SyncDaemon) ReconcileNomadChanges(ctx context.Context, repoPath string, changes []NomadFileChange, meta ...string) error {
	if len(changes) == 0 {
		return nil
	}

	repo, commit, targetID, prNumber := parseNomadMeta(meta)
	if repo == "" && repoPath != "" {
		repo = NormalizeGitHubSlug(repoPath)
	}
	if commit == "" && repoPath != "" {
		if headOut, _, headErr := d.getGitExecutor()(ctx, repoPath, "rev-parse", "HEAD"); headErr == nil {
			commit = strings.TrimSpace(string(headOut))
		}
	}

	d.nomadMu.Lock()
	defer d.nomadMu.Unlock()

	recCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	_, _, err := d.applyNomadChangesDirectly(recCtx, repoPath, changes, false, true, repo, commit, targetID, strconv.Itoa(prNumber))
	return err
}


func (d *SyncDaemon) ExecuteNomadTeardowns(ctx context.Context, pending []NomadChangeEvent) {
	if len(pending) == 0 {
		return
	}
	for _, evt := range pending {
		for _, change := range evt.Changes {
			if change.Action == "delete" {
				d.stopDeletedNomadJob(ctx, "[Hangar:Nomad]", evt.RepoPath, change.JobName)
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
		candidateDirs := []string{
			filepath.Join(repo, "jobs"),
			filepath.Join(repo, "nomad", "jobs"),
		}
		walkNomadJobFiles(candidateDirs, func(dir, jobPath, jobName, content string) bool {
			if _, seen := seenFiles[jobPath]; seen {
				return true
			}
			seenFiles[jobPath] = struct{}{}

			images := ExtractNomadJobImages(content)
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
			return true
		})
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
						if !sleepWithContext(ctx, reconcilerMinCooldown-elapsed) {
							return
						}
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
							if !sleepWithContext(ctx, reconcilerServerReadyDelay) {
								return
							}
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

		if len(syncCtx) > 0 && syncCtx[0].SkipPendingNomad {
			return res
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
			d.syncAllConfigsToNomad(syncCtx, "[Hangar:Periodic]", d.configDir)
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

func (d *SyncDaemon) pushNomadConfigVar(ctx context.Context, cacheKey, nomadVar, jobName, configHash string, isChangedInGit bool, varArgs []string, syncCtx []ConfigSyncContext) (bool, error) {
	nomadExec := d.getNomadExecutor()
	if nomadExec == nil || d.nomadAddr == "" {
		return false, nil
	}

	d.lastPushedConfigMu.Lock()
	cachedHash := ""
	if d.lastPushedConfig != nil {
		cachedHash = d.lastPushedConfig[cacheKey]
	}
	if !isChangedInGit {
		if d.lastPushedConfig == nil {
			d.lastPushedConfig = make(map[string]string)
		}
		d.lastPushedConfig[cacheKey] = configHash
		d.lastPushedConfigMu.Unlock()
		return false, nil
	}
	if cachedHash == configHash {
		d.lastPushedConfigMu.Unlock()
		return false, nil
	}
	d.lastPushedConfigMu.Unlock()

	valCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	args := append([]string{"var", "put", "-force", nomadVar}, varArgs...)
	if len(syncCtx) > 0 && syncCtx[0].CommitSHA != "" {
		args = append(args, "COMMIT_SHA="+syncCtx[0].CommitSHA)
		baseEvt := HangarDeployEvent{
			Event:     "deploy_started",
			JobName:   jobName,
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
		return false, fmt.Errorf("failed updating Nomad variable %s: %s (%w)", nomadVar, SanitizeLog(strings.TrimSpace(combined)), err)
	}

	d.lastPushedConfigMu.Lock()
	if d.lastPushedConfig == nil {
		d.lastPushedConfig = make(map[string]string)
	}
	d.lastPushedConfig[cacheKey] = configHash
	d.lastPushedConfigMu.Unlock()

	return true, nil
}

func (d *SyncDaemon) syncAllConfigsToNomad(ctx context.Context, logPrefix, configDir string, syncCtx ...ConfigSyncContext) {
	if err := d.SyncBrainConfigToNomad(ctx, configDir, syncCtx...); err != nil {
		log.Printf("%s Notice: SyncBrainConfigToNomad: %v", logPrefix, err)
	}
	if err := d.SyncHomepageConfigToNomad(ctx, configDir, syncCtx...); err != nil {
		log.Printf("%s Notice: SyncHomepageConfigToNomad: %v", logPrefix, err)
	}
	if err := d.SyncServiceConfigsToNomad(ctx, configDir, syncCtx...); err != nil {
		log.Printf("%s Notice: SyncServiceConfigsToNomad: %v", logPrefix, err)
	}
}

// readAndValidateYAML reads filePath and unmarshals into out if non-nil.
// If the file does not exist, it returns (nil, nil).
func readAndValidateYAML(filePath, label string, out any) ([]byte, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read %s at %s: %w", label, filePath, err)
	}
	if out != nil {
		if err := yaml.Unmarshal(raw, out); err != nil {
			return nil, fmt.Errorf("invalid YAML syntax in %s: %w", filePath, err)
		}
	}
	return raw, nil
}

// SyncBrainConfigToNomad validates config.yaml and writes it to Nomad variable nomad/jobs/brain (CONFIG_YAML).
func (d *SyncDaemon) SyncBrainConfigToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	configPath := filepath.Join(configDir, "config.yaml")
	var parsed struct {
		Channels map[string]interface{} `yaml:"channels"`
	}
	raw, err := readAndValidateYAML(configPath, "brain config", &parsed)
	if err != nil {
		return err
	}
	if raw == nil {
		return nil
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

	diffGated := len(syncCtx) > 0 && syncCtx[0].DiffGated
	isChangedInGit := !diffGated || fileInChangedList("config.yaml", syncCtx[0].ChangedFiles)

	pushed, err := d.pushNomadConfigVar(ctx, "nomad/jobs/brain:CONFIG_YAML", "nomad/jobs/brain", "brain", configHash, isChangedInGit, []string{"CONFIG_YAML=@" + configPath}, syncCtx)
	if err != nil {
		return err
	}
	if pushed {
		log.Printf("[Hangar:Nomad] Successfully pushed CONFIG_YAML to nomad/jobs/brain")
	}
	return nil
}

// SyncHomepageConfigToNomad validates homepage.yaml, computes a deterministic hash of all homepage configs,
// and writes HOMEPAGE_YAML and CONFIG_HASH to Nomad variable nomad/jobs/homepage.
func (d *SyncDaemon) SyncHomepageConfigToNomad(ctx context.Context, configDir string, syncCtx ...ConfigSyncContext) error {
	homepageConfigPath := filepath.Join(configDir, "services", "homepage", "homepage.yaml")
	var parsed map[string]interface{}
	raw, err := readAndValidateYAML(homepageConfigPath, "homepage config", &parsed)
	if err != nil {
		return err
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

	diffGated := len(syncCtx) > 0 && syncCtx[0].DiffGated
	isChangedInGit := !diffGated || homepageInChangedList(syncCtx[0].ChangedFiles)

	varArgs := []string{"CONFIG_HASH=" + configHash}
	if len(raw) > 0 {
		varArgs = append(varArgs, "HOMEPAGE_YAML=@"+homepageConfigPath)
	}

	pushed, err := d.pushNomadConfigVar(ctx, "nomad/jobs/homepage", "nomad/jobs/homepage", "homepage", configHash, isChangedInGit, varArgs, syncCtx)
	if err != nil {
		return err
	}
	if pushed {
		log.Printf("[Hangar:Nomad] Successfully pushed homepage config to nomad/jobs/homepage (hash=%s)", configHash[:12])
	}
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
	{RelPath: "services/victoriametrics/scrape.yml", NomadVar: "nomad/jobs/victoriametrics", VarKey: "CONFIG_YAML"},
}

// GetServiceConfigMappings resolves the active service config mappings for a given configDir.
// If configDir contains services/hangar/hangar.yaml with non-empty service_configs, it dynamically parses and returns them.
// Otherwise, it falls back to daemon-configured serviceConfigs, or DefaultManagedServiceConfigs if none are configured.
func (d *SyncDaemon) GetServiceConfigMappings(configDir string) []ServiceConfigMapping {
	if configDir != "" {
		hangarCfgPath := filepath.Join(configDir, "services", "hangar", "hangar.yaml")
		var hCfg HangarYAMLConfig
		if _, err := readAndValidateYAML(hangarCfgPath, "hangar config", &hCfg); err == nil && len(hCfg.ServiceConfigs) > 0 {
			return hCfg.ServiceConfigs
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
		ext := strings.ToLower(filepath.Ext(filePath))
		var parsedTarget any
		if ext == ".yaml" || ext == ".yml" {
			var parsed any
			parsedTarget = &parsed
		}
		raw, err := readAndValidateYAML(filePath, "service config", parsedTarget)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}

		h := sha256.Sum256(raw)
		configHash := hex.EncodeToString(h[:])
		cacheKey := mapping.NomadVar + ":" + mapping.VarKey

		diffGated := len(syncCtx) > 0 && syncCtx[0].DiffGated
		isChangedInGit := !diffGated || fileInChangedList(mapping.RelPath, syncCtx[0].ChangedFiles)

		jobName := strings.TrimPrefix(mapping.NomadVar, "nomad/jobs/")
		varArgs := []string{mapping.VarKey + "=@" + filePath}

		pushed, err := d.pushNomadConfigVar(ctx, cacheKey, mapping.NomadVar, jobName, configHash, isChangedInGit, varArgs, syncCtx)
		if err != nil {
			return err
		}
		if pushed {
			log.Printf("[Hangar:Nomad] Successfully pushed %s to %s", mapping.VarKey, mapping.NomadVar)
		}
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
				if imgErr := d.CheckAndReconcileNomadImages(ctx); imgErr != nil {
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
func (d *SyncDaemon) GetStatus(ctx context.Context) HangarStatusResponse {
	rec := d.GetReconciliationStatus()
	resp := HangarStatusResponse{
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

	client := d.getRegistryClient()
	checkStatus := func(status string) bool {
		apiURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs?status=%s&per_page=5", slug, status)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			log.Printf("[Hangar:GitHub] Warning: failed to create actions request for %s: %v", slug, err)
			return false
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		if strings.TrimSpace(d.pat) != "" {
			req.Header.Set("Authorization", "Bearer "+d.pat)
		}

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
				log.Printf("[Hangar:GitHub] %d %s workflow run(s) for %s", data.TotalCount, status, slug)
				return true
			}
		}
		return false
	}

	for _, status := range []string{"in_progress", "queued"} {
		if checkStatus(status) {
			return true
		}
	}

	return false
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

func (d *SyncDaemon) applyNomadChangesDirectly(ctx context.Context, repoPath string, changes []NomadFileChange, checkRepoBuilds, failOnJobErr bool, meta ...string) (applied, deferred []string, err error) {
	repo, commit, targetID, prNumber := parseNomadMeta(meta)

	SortNomadChangesHangarLast(changes)

	// Phase 1: Teardowns (stop deleted jobs)
	for _, ch := range changes {
		if ch.Action == "delete" {
			d.stopDeletedNomadJob(ctx, "[Hangar:Nomad]", repoPath, ch.JobName)
		}
	}

	// Phase 2: Applications (validate, then run)
	for _, ch := range changes {
		if ch.Action != "delete" {
			fullPath := filepath.Join(repoPath, ch.Path)
			if !IsSafeJobPath(repoPath, fullPath) {
				continue
			}
			content, readErr := os.ReadFile(fullPath)
			if readErr != nil {
				log.Printf("[Hangar:Nomad] Notice: job file %s not found on disk, skipping", fullPath)
				continue
			}
			if d.shouldDeferNomadJob(ctx, ch.JobName, string(content), checkRepoBuilds) {
				deferred = append(deferred, ch.JobName)
				continue
			}
			if errVal := d.ValidateNomadJob(ctx, fullPath); errVal != nil {
				log.Printf("[Hangar:Nomad] ERROR: Validation failed for %s: %v", ch.Path, errVal)
				if failOnJobErr {
					return applied, deferred, errVal
				}
				continue
			}

			baseEvt := HangarDeployEvent{
				JobName:   ch.JobName,
				Repo:      repo,
				CommitSHA: commit,
				PRNumber:  prNumber,
				TargetID:  targetID,
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
		containersBuilding := errDiff == nil && HasContainerBuildChanges(repoPath, changedFiles)
		if containersBuilding {
			resp.ContainersBuilding = true
		}

		nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errDiff == nil && HasMigrationChanges(changedFiles) {
			nomadChanges = EnsureDBMigrateNomadChange(nomadChanges)
			errNomad = nil
		}
		if errNomad == nil && len(nomadChanges) > 0 {
			resp.NomadChanged = true
			var immediateChanges []NomadFileChange
			for _, ch := range nomadChanges {
				if ch.Action != "delete" {
					fullPath := filepath.Join(repoPath, ch.Path)
					if content, readErr := os.ReadFile(fullPath); readErr == nil {
						jobSpec := string(content)
						if (containersBuilding && JobUsesRepoImage(jobSpec, "azylman/aerial")) || d.shouldDeferNomadJob(ctx, ch.JobName, jobSpec, false) {
							resp.ContainersBuilding = true
							log.Printf("[Hangar:GitPush] Deferring Nomad job %s until container image is ready", ch.JobName)
							continue
						}
					}
				}
				immediateChanges = append(immediateChanges, ch)
			}
			applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, immediateChanges, false, true, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
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
		changedFiles, errDiff := d.getChangedFiles(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errDiff != nil {
			log.Printf("[Hangar:GitPush] Error: getChangedFiles failed for %s: %v", repoPath, errDiff)
			return GitPushEventResponse{
				Status:  "error",
				Repo:    req.Repo,
				Commit:  res.CurrentHead,
				Message: fmt.Sprintf("failed inspecting git diff: %v", errDiff),
			}, http.StatusInternalServerError
		}
		sc.DiffGated = true
		sc.ChangedFiles = changedFiles

		nomadChanges, errNomad := d.HasNomadChanges(ctx, repoPath, res.PreviousHead, res.CurrentHead)
		if errNomad != nil {
			log.Printf("[Hangar:GitPush] Warning: HasNomadChanges failed for %s: %v", repoPath, errNomad)
		}

		if len(nomadChanges) > 0 {
			resp.NomadChanged = true
			applied, deferred, errRec := d.applyNomadChangesDirectly(ctx, repoPath, nomadChanges, true, false, req.Repo, req.Commit, req.TargetID, strconv.Itoa(req.PRNumber))
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

var (
	writeCountersDirFn = coverage.WriteCountersDir
	writeMetaDirFn     = coverage.WriteMetaDir
)

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

	mux.HandleFunc("/debug/coverage/flush", func(w http.ResponseWriter, r *http.Request) {
		if dir := os.Getenv("GOCOVERDIR"); dir != "" {
			if err := writeMetaDirFn(dir); err != nil {
				log.Printf("[hangar] coverage meta flush error: %v", err)
			}
			if err := writeCountersDirFn(dir); err != nil {
				log.Printf("[hangar] coverage flush error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	})

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
		if !requireMethod(w, r, http.MethodPost) {
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
		if !requireMethod(w, r, http.MethodPost) {
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
