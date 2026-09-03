package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

var sanitizePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)basic\s+[a-zA-Z0-9+/=]+`),
	regexp.MustCompile(`(?i)x-access-token:[^@\s]+`),
	regexp.MustCompile(`(?i)github_pat_[a-zA-Z0-9_]+`),
	regexp.MustCompile(`(?i)ghp_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)gho_[a-zA-Z0-9]+`),
	regexp.MustCompile(`(?i)ghu_[a-zA-Z0-9]+`),
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
	Repo         string `json:"repo"`
	PreviousHead string `json:"previous_head"`
	CurrentHead  string `json:"current_head"`
	Changed      bool   `json:"changed"`
	Error        string `json:"error,omitempty"`
}

// SyncDaemon coordinates periodic and on-demand repository synchronizations.
type SyncDaemon struct {
	sfg      singleflight.Group
	mu       sync.Mutex
	ticker   *time.Ticker
	interval time.Duration
	repos    []string
	repoUrls map[string]string
	pat      string
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
		// Clean clone into empty directory
		cmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "-b", "main", repoURL, repoPath)
		cmd.Env = gitEnv
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git clone failed for %s: %s (%w)", repoPath, SanitizeLog(string(out)), err)
		}
		return nil
	}

	// Non-empty directory: initialize and adopt remote
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
func (d *SyncDaemon) SyncRepo(ctx context.Context, repoPath string) RepoSyncResult {
	res := RepoSyncResult{Repo: repoPath}

	if repoPath == "" {
		return res
	}

	// Bootstrap if repo doesn't exist
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

	// Check if index.lock is active
	lockFile := filepath.Join(gitDir, "index.lock")
	if _, err := os.Stat(lockFile); err == nil {
		log.Printf("[GitSync] %s has index.lock present, skipping sync cycle", repoPath)
		res.Error = "index.lock active"
		return res
	}

	opCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()

	gitEnv := buildGitEnv(d.pat)

	// Configure safe.directory
	cmdSafe := exec.CommandContext(opCtx, "git", "config", "--global", "safe.directory", "*")
	_ = cmdSafe.Run()

	// Rev-parse HEAD before pull
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

	// Pull with --ff-only
	cmdPull := exec.CommandContext(opCtx, "git", "-C", repoPath, "pull", "--ff-only")
	cmdPull.Env = gitEnv
	outPull, errPull := cmdPull.CombinedOutput()

	if errPull != nil {
		sanitizedOut := SanitizeLog(strings.TrimSpace(string(outPull)))
		sanitizedErr := SanitizeLog(errPull.Error())
		log.Printf("[GitSync] Notice: git pull --ff-only failed for %s (%s, %s). Attempting safe reset recovery...", repoPath, sanitizedErr, sanitizedOut)

		// Safe reset recovery: fetch origin, and reset --hard to FETCH_HEAD
		cmdFetch := exec.CommandContext(opCtx, "git", "-C", repoPath, "fetch", "origin", "main")
		cmdFetch.Env = gitEnv
		if outFetch, errFetch := cmdFetch.CombinedOutput(); errFetch == nil {
			cmdReset := exec.CommandContext(opCtx, "git", "-C", repoPath, "reset", "--hard", "FETCH_HEAD")
			cmdReset.Env = gitEnv
			if outReset, errReset := cmdReset.CombinedOutput(); errReset != nil {
				res.Error = fmt.Sprintf("reset failed: %s", SanitizeLog(string(outReset)))
				return res
			}

			// Clean untracked files without -x to preserve .gitignored files (like .env)
			cmdClean := exec.CommandContext(opCtx, "git", "-C", repoPath, "clean", "-fd")
			cmdClean.Env = gitEnv
			_ = cmdClean.Run()

			log.Printf("[GitSync] Successfully recovered %s via reset to FETCH_HEAD", repoPath)
		} else {
			res.Error = fmt.Sprintf("pull failed: %s; fetch failed: %s", sanitizedOut, SanitizeLog(strings.TrimSpace(string(outFetch))))
			return res
		}
	}

	// Rev-parse HEAD after pull
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
	}

	return res
}

// SyncComposeOverride copies or symlinks docker-compose.override.yml from configDir to projectDir.
func SyncComposeOverride(configDir, projectDir string) {
	sourceYml := filepath.Join(configDir, "docker-compose.override.yml")
	sourceYaml := filepath.Join(configDir, "docker-compose.override.yaml")
	var source string
	if _, err := os.Stat(sourceYml); err == nil {
		source = sourceYml
	} else if _, err := os.Stat(sourceYaml); err == nil {
		source = sourceYaml
	}

	target := filepath.Join(projectDir, "docker-compose.override.yml")
	if source != "" {
		data, err := os.ReadFile(source)
		if err == nil {
			_ = os.WriteFile(target, data, 0644)
			log.Printf("[GitSync] Synchronized docker-compose.override.yml to %s", projectDir)
		}
	}
}

// TriggerSync runs a synchronous singleflight sync across all managed repositories.
func (d *SyncDaemon) TriggerSync() ([]RepoSyncResult, error) {
	val, err, _ := d.sfg.Do("sync", func() (interface{}, error) {
		// Reset periodic ticker so scheduled run doesn't fire immediately
		d.mu.Lock()
		if d.ticker != nil {
			d.ticker.Reset(d.interval)
		}
		d.mu.Unlock()

		// Decouple execution context from incoming HTTP caller context to prevent cancellation
		syncCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		results := make([]RepoSyncResult, 0, len(d.repos))
		var anyChanged bool
		for _, repo := range d.repos {
			r := d.SyncRepo(syncCtx, repo)
			if r.Changed {
				anyChanged = true
			}
			results = append(results, r)
		}

		if anyChanged {
			SyncComposeOverride("/share/aerial-config", "/share/aerial")
		}

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

	repoUrls := map[string]string{
		"/share/aerial-config": configRepoURL,
		"/share/aerial":        "https://github.com/azylman/aerial.git",
	}

	daemon := &SyncDaemon{
		repos:    repos,
		repoUrls: repoUrls,
		interval: interval,
		pat:      pat,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon.StartPeriodicLoop(ctx)
	log.Printf("[GitSync] Sidecar daemon started on :%s (interval: %v, repos: %v)", port, interval, repos)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		results, err := daemon.TriggerSync()
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "synced",
			"results": results,
		})
	})

	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("[GitSync] HTTP server fatal error: %v", err)
	}
}
