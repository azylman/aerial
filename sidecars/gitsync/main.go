package main

import (
	"bytes"
	"context"
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

// ComposeChangeEvent captures a detected configuration change before reconciliation.
type ComposeChangeEvent struct {
	RepoPath     string    `json:"repo_path"`
	PreviousHead string    `json:"previous_head"`
	CurrentHead  string    `json:"current_head"`
	Timestamp    time.Time `json:"timestamp"`
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

// GitSyncStatusResponse is the aggregated telemetry payload returned by GET /status.
type GitSyncStatusResponse struct {
	Status        string                `json:"status"` // "synced", "lagging", "quarantined", "error"
	MaxLagSeconds int64                 `json:"max_lag_seconds"`
	LastSyncTime  time.Time             `json:"last_sync_time"`
	Repos         map[string]RepoStatus `json:"repos"`
}

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

func defaultGitExecutor(ctx context.Context, dir string, args ...string) ([]byte, []byte, error) {
	return runGitCommand(ctx, dir, "", args...)
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
	triggerFn     func() ([]RepoSyncResult, error)

	brainInternalURL string
	extraSecrets     []string
	composeExecutor  ComposeExecutor
	dockerExecutor   DockerExecutor
	gitExecutor      GitExecutor

	// Concurrency & Quarantine
	repoLocksMu        sync.Mutex
	repoLocks          map[string]*sync.Mutex
	pendingChangesMu   sync.Mutex
	pendingChanges     []ComposeChangeEvent
	quarantineMu       sync.RWMutex
	quarantinedCommits map[QuarantineKey]QuarantineRecord

	// Discord Alerting
	discordToken      string
	discordChannel    string
	discordWebhookURL string
	discordChannelMu  sync.RWMutex
	cachedChannelID   string
	alertDedupeMu     sync.Mutex
	lastAlertTimes    map[string]time.Time
	alertHTTPClient   *http.Client
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
	return BuildGitEnv(pat, os.Environ())
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
			log.Printf("[GitSync] Warning: Failed to inspect diff in %s (%v); failing safe to trigger reconcile", repoPath, errFallback)
			return true, nil
		}
		lines := strings.Split(strings.TrimSpace(string(stdoutFallback)), "\n")
		return len(FilterComposeChanges(lines)) > 0, nil
	}

	lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
	return len(FilterComposeChanges(lines)) > 0, nil
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
		log.Printf("[GitSync:ConflictClean] Warning: failed to list containers: %s (%v)", SanitizeLog(strings.TrimSpace(string(stderr))), err)
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
				log.Printf("[GitSync:ConflictClean] Container %s already removed", shortID)
			} else {
				log.Printf("[GitSync:ConflictClean] Warning: failed to remove conflict container %s (%s): %s (%v)", shortID, c.Names, SanitizeLog(strings.TrimSpace(rmErrStr)), rmErr)
			}
		} else {
			log.Printf("[GitSync:ConflictClean] Purged orphaned conflict container %s (%s, status: %s)", shortID, c.Names, c.Status)
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
		log.Printf("[GitSync:Discord] Suppressing duplicate alert for %s (sent %v ago)", dedupeKey, time.Since(lastSent).Truncate(time.Second))
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
		log.Printf("[GitSync:Discord] Notice: No DISCORD_BOT_TOKEN or DISCORD_WEBHOOK_URL configured, skipping alert")
		return
	}

	targetChannel := d.ResolveAlertChannel()
	channelID, err := d.resolveChannelID(alertCtx, client, token, targetChannel)
	if err != nil {
		log.Printf("[GitSync:Discord] Warning: Failed to resolve channel %q: %s", targetChannel, d.SanitizeAll(err.Error()))
		return
	}

	d.postChannelMessage(alertCtx, client, token, channelID, content)
}

func (d *SyncDaemon) sendWebhook(ctx context.Context, client *http.Client, webhookURL, content string) {
	payloadBytes, _ := json.Marshal(map[string]string{"content": content})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Printf("[GitSync:Discord] Error creating webhook request: %s", d.SanitizeAll(err.Error()))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[GitSync:Discord] Webhook dispatch failed: %s", d.SanitizeAll(err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Printf("[GitSync:Discord] Webhook returned HTTP %d", resp.StatusCode)
	} else {
		log.Printf("[GitSync:Discord] Successfully dispatched alert via webhook")
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
		return "", err
	}
	reqGuilds.Header.Set("Authorization", authHeader)
	respGuilds, err := client.Do(reqGuilds)
	if err != nil {
		return "", err
	}
	defer respGuilds.Body.Close()

	if respGuilds.StatusCode >= 400 {
		return "", fmt.Errorf("failed to fetch guilds: HTTP %d", respGuilds.StatusCode)
	}

	var guilds []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(respGuilds.Body).Decode(&guilds); err != nil {
		return "", err
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
		_ = json.NewDecoder(respChans.Body).Decode(&channels)
		respChans.Body.Close()

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
	payloadBytes, _ := json.Marshal(map[string]string{"content": content})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Printf("[GitSync:Discord] Error creating message request: %s", d.SanitizeAll(err.Error()))
		return
	}
	req.Header.Set("Authorization", "Bot "+strings.TrimPrefix(token, "Bot "))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[GitSync:Discord] Channel message dispatch failed: %s", d.SanitizeAll(err.Error()))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		d.discordChannelMu.Lock()
		d.cachedChannelID = ""
		d.discordChannelMu.Unlock()
		log.Printf("[GitSync:Discord] Channel %s returned 404, invalidated channel cache", channelID)
	} else if resp.StatusCode >= 400 {
		log.Printf("[GitSync:Discord] Failed to post message to channel %s: HTTP %d", channelID, resp.StatusCode)
	} else {
		log.Printf("[GitSync:Discord] Successfully dispatched alert to channel %s", channelID)
	}
}

// executeRollback safely resets each affected repository to PreviousHead, quarantines the faulty commit,
// and restores container topology using the restored PreviousHead configuration with --remove-orphans.
func (d *SyncDaemon) executeRollback(ctx context.Context, pending []ComposeChangeEvent, stage string, causeErr error) {
	if len(pending) == 0 {
		log.Printf("[GitSync:GitOps] Warning: Rollback requested with no pending changes tracked; skipping git reset")
		return
	}

	for _, ch := range pending {
		if ch.PreviousHead == "" || ch.RepoPath == "" {
			continue
		}
		repoLock := d.getRepoLock(ch.RepoPath)
		repoLock.Lock()

		outReset, errResetBytes, errReset := d.getGitExecutor()(ctx, ch.RepoPath, "reset", "--hard", ch.PreviousHead)
		if errReset != nil {
			combined := string(append(outReset, errResetBytes...))
			log.Printf("[GitSync:GitOps] Critical: Failed to reset %s to %s: %s (%v)", ch.RepoPath, ch.PreviousHead, SanitizeLog(combined), errReset)
		} else {
			log.Printf("[GitSync:GitOps] Successfully rolled back %s to %s", ch.RepoPath, ch.PreviousHead)
		}

		d.quarantineCommit(ch.RepoPath, ch.CurrentHead, ch.PreviousHead, stage, causeErr.Error())
		repoLock.Unlock()

		d.SendDiscordAlert(context.Background(), "Docker Compose Failure (Rolled Back)", ch.RepoPath, ch.CurrentHead, ch.PreviousHead, stage, causeErr.Error())
	}

	composeDir := d.composeDir

	valCtx, valCancel := context.WithTimeout(ctx, 30*time.Second)
	defer valCancel()

	_ = d.CleanConflictContainers(valCtx)

	restoredTargets, errTargets := d.GetReconcileTargets(valCtx, composeDir)
	if errTargets != nil {
		log.Printf("[GitSync:GitOps] Critical: Failed to discover targets after rollback in %s: %v", composeDir, errTargets)
		return
	}
	if len(restoredTargets) == 0 {
		return
	}

	upArgs := append([]string{"up", "-d", "--remove-orphans", "--no-build"}, restoredTargets...)
	stdoutUp, stderrUp, errUp := d.getComposeExecutor()(ctx, composeDir, d.getComposeArgs(composeDir, upArgs...)...)
	combinedUp := string(append(stdoutUp, stderrUp...))
	if errUp != nil {
		log.Printf("[GitSync:GitOps] CRITICAL: Re-applying restored configuration failed: %s (%v)", SanitizeLog(combinedUp), errUp)
		d.SendDiscordAlert(context.Background(), "CRITICAL: Rollback Re-Apply Failed", composeDir, "", "", "rollback re-apply", fmt.Sprintf("Failed to restore containers to previous configuration: %s (%v)", SanitizeLog(combinedUp), errUp))
	} else {
		log.Printf("[GitSync:GitOps] Restored previous container topology successfully (%s)", SanitizeLog(combinedUp))
	}
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

	if _, statErr := os.Stat(filepath.Join(composeDir, "docker-compose.yml")); statErr != nil {
		log.Printf("[GitSync:GitOps] Notice: No docker-compose.yml found in %s, skipping reconciliation", composeDir)
		return nil
	}

	pending := d.drainPendingChanges()

	// 1. Conflict container cleanup & pre-flight validation gate
	valCtx, valCancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer valCancel()

	_ = d.CleanConflictContainers(valCtx)

	if valErr := d.ValidateCompose(valCtx, composeDir); valErr != nil {
		log.Printf("[GitSync:GitOps] ERROR: Pre-flight validation failed: %v. Initiating automated rollback.", valErr)
		d.executeRollback(parentCtx, pending, "pre-flight validation", valErr)
		return valErr
	}

	// 2. Discover target services excluding gitsync
	targets, targetErr := d.GetReconcileTargets(valCtx, composeDir)
	if targetErr != nil {
		log.Printf("[GitSync:GitOps] ERROR: Service discovery failed: %v. Initiating automated rollback.", targetErr)
		d.executeRollback(parentCtx, pending, "service discovery", targetErr)
		return targetErr
	}

	if len(targets) == 0 {
		log.Printf("[GitSync:GitOps] Notice: No external services to reconcile in %s (gitsync excluded)", composeDir)
		return nil
	}

	// 3. Bounded compose execution
	ctx, cancel := context.WithTimeout(parentCtx, 120*time.Second)
	defer cancel()

	log.Printf("[GitSync:GitOps] Reconciling Docker Compose state for %d services (%v) in %s...", len(targets), targets, composeDir)

	upArgs := append([]string{"up", "-d", "--remove-orphans", "--no-build"}, targets...)
	stdout, stderr, cmdErr := d.getComposeExecutor()(ctx, composeDir, d.getComposeArgs(composeDir, upArgs...)...)
	combined := string(append(stdout, stderr...))
	sanitized := SanitizeLog(strings.TrimSpace(combined))

	if cmdErr != nil {
		log.Printf("[GitSync:GitOps] ERROR: docker compose up failed: %s (%v). Initiating automated rollback.", sanitized, cmdErr)
		d.executeRollback(parentCtx, pending, "compose apply", fmt.Errorf("%s (%w)", sanitized, cmdErr))
		return fmt.Errorf("compose up failed: %s (%w)", sanitized, cmdErr)
	}

	if sanitized != "" {
		log.Printf("[GitSync:GitOps] Reconcile output: %s", sanitized)
	}
	log.Printf("[GitSync:GitOps] Docker Compose reconciliation successfully applied.")
	d.lastReconcile = time.Now()
	return nil
}

var (
	reconcilerDebounceDuration = 5 * time.Second
	reconcilerMinCooldown      = 15 * time.Second
)

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
					if elapsed := time.Since(d.lastReconcile); elapsed < reconcilerMinCooldown {
						time.Sleep(reconcilerMinCooldown - elapsed)
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

	if len(entries) == 0 {
		out, errBytes, err := d.getGitExecutor()(ctx, "", "clone", "--depth", "1", "-b", "main", repoURL, repoPath)
		if err != nil {
			combined := string(append(out, errBytes...))
			return fmt.Errorf("git clone failed for %s: %s (%w)", repoPath, SanitizeLog(string(combined)), err)
		}
		return nil
	}

	_, _, _ = d.getGitExecutor()(ctx, repoPath, "init", "-b", "main")
	_, _, _ = d.getGitExecutor()(ctx, repoPath, "remote", "add", "origin", repoURL)
	out, errBytes, err := d.getGitExecutor()(ctx, repoPath, "fetch", "--depth", "1", "origin", "main")
	if err != nil {
		combined := string(append(out, errBytes...))
		return fmt.Errorf("git fetch failed during adoption for %s: %s (%w)", repoPath, SanitizeLog(string(combined)), err)
	}

	_, _, _ = d.getGitExecutor()(ctx, repoPath, "reset", "--soft", "FETCH_HEAD")
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

	_, _, _ = d.getGitExecutor()(opCtx, "", "config", "--global", "safe.directory", "*")

	outBefore, _, err := d.getGitExecutor()(opCtx, repoPath, "rev-parse", "HEAD")
	if err != nil {
		sanitizedErr := SanitizeLog(err.Error())
		log.Printf("[GitSync] Warning: failed to rev-parse HEAD before pull for %s: %s", repoPath, sanitizedErr)
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
		log.Printf("[GitSync] Notice: git fetch origin main failed for %s (%s, %s). Attempting git pull fallback...", repoPath, sanitizedErr, sanitizedOut)

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
				log.Printf("[GitSync] Notice: remote commit %s for %s is quarantined (reason: %s). Skipping pull to prevent failure loop.", fetchedSha, repoPath, rec.Reason)
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
			log.Printf("[GitSync] Notice: git merge --ff-only failed for %s (%s, %s). Attempting safe reset recovery...", repoPath, sanitizedErr, sanitizedOut)

			outReset, errResetBytes, errReset := d.getGitExecutor()(opCtx, repoPath, "reset", "--hard", "FETCH_HEAD")
			if errReset != nil {
				combinedReset := string(append(outReset, errResetBytes...))
				res.Error = fmt.Sprintf("reset failed: %s", SanitizeLog(combinedReset))
				return res
			}

			_, _, _ = d.getGitExecutor()(opCtx, repoPath, "clean", "-fd")

			log.Printf("[GitSync] Successfully recovered %s via reset to FETCH_HEAD", repoPath)
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
		log.Printf("[GitSync] Repository %s updated: %s -> %s", repoPath, res.PreviousHead, res.CurrentHead)

		if composeChanged, _ := d.HasComposeChanges(opCtx, repoPath, res.PreviousHead, res.CurrentHead); composeChanged {
			res.ComposeChanged = true
			log.Printf("[GitSync:GitOps] Infrastructure/compose changes detected in %s (%s -> %s). Triggering debounced reconciliation.", repoPath, res.PreviousHead, res.CurrentHead)
			d.recordPendingChange(ComposeChangeEvent{
				RepoPath:     repoPath,
				PreviousHead: res.PreviousHead,
				CurrentHead:  res.CurrentHead,
				Timestamp:    time.Now(),
			})
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
	return val.([]RepoSyncResult), nil
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
		return
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		log.Printf("[GitSync] Successfully dispatched internal reload trigger to Brain (%s)", brainURL)
	}
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
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[1]))
	if err != nil {
		return sha, nil, nil
	}
	return sha, &t, nil
}

func getRepoCommit(ctx context.Context, repoPath, ref, pat string) (string, *time.Time, error) {
	d := &SyncDaemon{pat: pat}
	return d.getRepoCommit(ctx, repoPath, ref)
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
	DockerExecutor    DockerExecutor
	GitExecutor       GitExecutor
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
	return &SyncDaemon{
		repos:              cfg.Repos,
		repoUrls:           cfg.RepoURLs,
		interval:           cfg.Interval,
		pat:                cfg.PAT,
		composeDir:         cfg.ComposeDir,
		configDir:          cfg.ConfigDir,
		discordToken:       cfg.DiscordToken,
		discordChannel:     cfg.DiscordChannel,
		discordWebhookURL:  cfg.DiscordWebhookURL,
		brainInternalURL:   cfg.BrainInternalURL,
		extraSecrets:       cfg.ExtraSecrets,
		composeExecutor:    cfg.ComposeExecutor,
		dockerExecutor:     cfg.DockerExecutor,
		gitExecutor:        cfg.GitExecutor,
		reconcileCh:        make(chan struct{}, 1),
		repoLocks:          make(map[string]*sync.Mutex),
		quarantinedCommits: make(map[QuarantineKey]QuarantineRecord),
		lastAlertTimes:     make(map[string]time.Time),
	}
}

// SetupMux configures HTTP handlers for metrics, health, status, and sync.
func SetupMux(daemon *SyncDaemon) http.Handler {
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

	return mux
}

// RunDaemon starts the background sync daemon and HTTP server.
func RunDaemon(ctx context.Context, cfg DaemonConfig) error {
	daemon := NewDaemon(cfg)

	daemon.StartPeriodicLoop(ctx)
	daemon.StartReconcilerLoop(ctx)
	log.Printf("[GitSync] Sidecar GitOps daemon started on :%s (interval: %v, repos: %v, composeDir: %s)", cfg.Port, cfg.Interval, cfg.Repos, cfg.ComposeDir)

	go func() {
		if _, err := daemon.TriggerSync(); err != nil {
			log.Printf("[GitSync] Initial startup sync completed with notice: %v", err)
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
		log.Println("[GitSync] Shutting down GitSync sidecar gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[GitSync] HTTP server shutdown error: %v", err)
		}
		log.Println("[GitSync] GitSync sidecar stopped cleanly")
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
		lookup("GEMINI_API_KEY"),
		lookup("GITHUB_PAT"),
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
		log.Fatalf("[GitSync] Fatal error: %v", err)
	}
}
