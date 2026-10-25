// Package main implements the Aerial Permet HUD status dashboard and telemetry viewer.
package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"

	"gopkg.in/yaml.v3"
)

//go:embed static/*
var content embed.FS

var startTime = time.Now().UTC()

type ServiceStatus struct {
	Name          string    `json:"name"`
	Status        string    `json:"status"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	LastCheckTime time.Time `json:"last_check_time"`
}

type MatrixJobChip struct {
	Name       string `json:"name"`                 // e.g. "brain", "dashboard", "proxy"
	Status     string `json:"status"`               // "completed", "active", "pending", "failed"
	Conclusion string `json:"conclusion,omitempty"` // "success", "failure", "skipped", ""
	Duration   string `json:"duration,omitempty"`   // e.g. "45s"
	Type       string `json:"type,omitempty"`       // "ci" | "container"
}

type DeploymentStep struct {
	Name   string `json:"name"`
	Icon   string `json:"icon"`
	Status string `json:"status"` // "completed", "active", "pending", "failed"
}

type DeploymentStatus struct {
	ID         string           `json:"id"`
	Service    string           `json:"service"`
	Repository string           `json:"repository,omitempty"`
	Commit     string           `json:"commit"`
	CommitMsg  string           `json:"commit_msg,omitempty"`
	CommitTime *time.Time       `json:"commit_time,omitempty"`
	Stage      string           `json:"stage"` // "queued", "building", "failed", "awaiting_pull", "swapping", "live", "degraded"
	Progress   int              `json:"progress"`
	Steps      []DeploymentStep `json:"steps"`
	MatrixJobs []MatrixJobChip  `json:"matrix_jobs,omitempty"`
	HTMLURL    string           `json:"html_url,omitempty"`
	StartedAt  time.Time        `json:"started_at"`
}

type ActiveTaskStatus struct {
	ID          string    `json:"id"`
	ThreadID    string    `json:"thread_id"`
	SessionID   string    `json:"session_id,omitempty"`
	AuthorName  string    `json:"author_name"`
	Prompt      string    `json:"prompt"`
	Summary     string    `json:"summary"`
	Status      string    `json:"status"`
	RetryCount  int       `json:"retry_count"`
	TriggerType string    `json:"trigger_type"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at"`
}

type BrainTasksAPIResponse struct {
	Status string `json:"status"`
	Total  int    `json:"total"`
	Tasks  []struct {
		ID          string    `json:"id"`
		ThreadID    string    `json:"thread_id"`
		SessionID   string    `json:"session_id"`
		AuthorName  string    `json:"author_name"`
		Prompt      string    `json:"prompt"`
		Summary     string    `json:"summary"`
		Status      string    `json:"status"`
		RetryCount  int       `json:"retry_count"`
		TriggerType string    `json:"trigger_type"`
		CreatedAt   time.Time `json:"created_at"`
		UpdatedAt   time.Time `json:"updated_at"`
	} `json:"tasks"`
}

type QuickLaunchLink struct {
	Name        string `json:"name" yaml:"name"`
	URL         string `json:"url" yaml:"url"`
	Icon        string `json:"icon,omitempty" yaml:"icon,omitempty"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Target      string `json:"target,omitempty" yaml:"target,omitempty"`
	IsCore      bool   `json:"is_core,omitempty" yaml:"is_core,omitempty"`
	IsCustom    bool   `json:"is_custom,omitempty" yaml:"is_custom,omitempty"`
}

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

type GitSyncStatusResponse struct {
	Status         string                `json:"status"` // "synced", "lagging", "error"
	MaxLagSeconds  int64                 `json:"max_lag_seconds"`
	LastSyncTime   time.Time             `json:"last_sync_time"`
	Repos          map[string]RepoStatus `json:"repos,omitempty"`
	Reconciliation *ReconciliationStatus `json:"reconciliation,omitempty"`
	Error          string                `json:"error,omitempty"`
}

type ClusterResponse struct {
	SystemTime       time.Time             `json:"system_time"`
	ClusterStatus    string                `json:"cluster_status"`
	ActiveTasksCount int                   `json:"active_tasks_count"`
	ActiveTasks      []ActiveTaskStatus    `json:"active_tasks"`
	Services         []ServiceStatus       `json:"services"`
	Deployments      []DeploymentStatus    `json:"deployments"`
	QuickLaunchLinks []QuickLaunchLink     `json:"quick_launch_links"`
	GitSync          GitSyncStatusResponse `json:"git_sync"`
	OngoingDeploy    string                `json:"ongoing_deploy"`
	IsDeploying      bool                  `json:"is_deploying"`
}

type GitHubRun struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	RepoObj *struct {
		FullName string `json:"full_name"`
	} `json:"repository,omitempty"`
	Repository string `json:"-"`
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	Event      string `json:"event,omitempty"`
	HeadCommit *struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"head_commit,omitempty"`
	Status     string    `json:"status"`     // "queued", "in_progress", "completed"
	Conclusion string    `json:"conclusion"` // "success", "failure", "cancelled"
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	HTMLURL    string    `json:"html_url"`
}

type GitHubJob struct {
	ID          int64     `json:"id"`
	RunID       int64     `json:"run_id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`     // "queued", "in_progress", "completed"
	Conclusion  string    `json:"conclusion"` // "success", "failure", "skipped"
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	HTMLURL     string    `json:"html_url"`
}

type GitHubRunsResponse struct {
	TotalCount   int         `json:"total_count"`
	WorkflowRuns []GitHubRun `json:"workflow_runs"`
}

type GitHubJobsResponse struct {
	TotalCount int         `json:"total_count"`
	Jobs       []GitHubJob `json:"jobs"`
}

type GitHubPoller struct {
	repo         string
	repos        []string
	token        string
	client       *http.Client
	configPath   string
	runsETag     string
	runsETagMap  map[string]string
	jobsETagMap  map[int64]string
	apiBaseURL   string
	pollInterval time.Duration

	mu               sync.RWMutex
	cachedRunsByRepo map[string][]GitHubRun
	cachedRuns       []GitHubRun
	cachedJobs       map[int64][]GitHubJob
	lastPollTime     time.Time
	lastError        error
	stopCh           chan struct{}
	hangarURL        string
	dynamicRepos     []string
}

var globalGHPoller *GitHubPoller

// FactItem represents a semantic memory item received from brain
type FactItem struct {
	ID               int64     `json:"id"`
	FactText         string    `json:"fact_text"`
	ReinforceCount   int       `json:"reinforce_count"`
	LastReinforcedAt time.Time `json:"last_reinforced_at,omitzero"`
	CreatedAt        time.Time `json:"created_at"`
}

type FactsAPIResponse struct {
	Facts  []FactItem `json:"facts"`
	Total  int        `json:"total"`
	Limit  int        `json:"limit"`
	Offset int        `json:"offset"`
	Status string     `json:"status,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// ScheduleSummaryMetrics represents aggregated schedule execution metrics
type ScheduleSummaryMetrics struct {
	TotalActive    int     `json:"total_active"`
	CronCount      int     `json:"cron_count"`
	OneShotCount   int     `json:"one_shot_count"`
	TotalRuns24h   int     `json:"total_runs_24h"`
	SuccessRate24h float64 `json:"success_rate_24h"`
}

// CronSchedule represents a recurring cron schedule
type CronSchedule struct {
	ID              string    `json:"id"`
	TargetID        string    `json:"target_id,omitempty"`
	ChannelID       string    `json:"channel_id"`
	TitlePrefix     string    `json:"title_prefix"`
	CronExpr        string    `json:"cron_expr"`
	CronDescription string    `json:"cron_description"`
	Prompt          string    `json:"prompt"`
	Timezone        string    `json:"timezone"`
	NextRunAt       time.Time `json:"next_run_at"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
}

// OneShotSchedule represents a one-time scheduled reminder
type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

// SchedulesAPIResponse represents the response payload for schedules and summary metrics
type SchedulesAPIResponse struct {
	Status     string                 `json:"status"`
	SystemTime time.Time              `json:"system_time,omitempty"`
	Summary    ScheduleSummaryMetrics `json:"summary"`
	Crons      []CronSchedule         `json:"crons"`
	OneShots   []OneShotSchedule      `json:"one_shots"`
	Error      string                 `json:"error,omitempty"`
}

// ScheduleRun represents a logged schedule execution run
type ScheduleRun struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id,omitempty"`
	TargetID     string     `json:"target_id,omitempty"`
	ThreadID     string     `json:"thread_id,omitempty"`
	Title        string     `json:"title,omitempty"`
	Prompt       string     `json:"prompt"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Error        string     `json:"error,omitempty"`
}

// ScheduleRunsAPIResponse represents paginated execution run logs
type ScheduleRunsAPIResponse struct {
	Status string        `json:"status"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Runs   []ScheduleRun `json:"runs"`
	Error  string        `json:"error,omitempty"`
}

// VanadisChunkDTO represents an individual task chunk within a Vanadis build.
type VanadisChunkDTO struct {
	ID                 string    `json:"id"`
	BuildID            string    `json:"build_id"`
	ChunkIndex         int       `json:"chunk_index"`
	Repo               string    `json:"repo,omitempty"`
	Title              string    `json:"title"`
	Instructions       string    `json:"instructions"`
	TargetFiles        []string  `json:"target_files"`
	TargetSymbols      []string  `json:"target_symbols"`
	Effort             string    `json:"effort,omitempty"`
	AcceptanceCriteria []string  `json:"acceptance_criteria"`
	Status             string    `json:"status"`
	PRNumber           int       `json:"pr_number,omitempty"`
	StartedAt          time.Time `json:"started_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// VanadisBuildDTO represents a Vanadis build run or build entry.
type VanadisBuildDTO struct {
	ID              string            `json:"id"`
	ThreadID        string            `json:"thread_id"`
	Tier            int               `json:"tier"`
	Title           string            `json:"title"`
	Objective       string            `json:"objective"`
	SharedContext   string            `json:"shared_context,omitempty"`
	Status          string            `json:"status"`
	StatusMessageID string            `json:"status_message_id,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	QueueState      string            `json:"queue_state,omitempty"`
	TotalChunks     int               `json:"total_chunks,omitempty"`
	CompletedChunks int               `json:"completed_chunks,omitempty"`
	RunningChunks   int               `json:"running_chunks,omitempty"`
	PendingChunks   int               `json:"pending_chunks,omitempty"`
	FailedChunks    int               `json:"failed_chunks,omitempty"`
	Chunks          []VanadisChunkDTO `json:"chunks,omitempty"`
}

// BuildsAPIResponse represents the response payload for listing Vanadis builds.
type BuildsAPIResponse struct {
	Status         string            `json:"status"`
	Total          int               `json:"total"`
	RunningCount   int               `json:"running_count"`
	QueuedCount    int               `json:"queued_count"`
	CompletedCount int               `json:"completed_count"`
	Builds         []VanadisBuildDTO `json:"builds"`
	Error          string            `json:"error,omitempty"`
}

// BuildDetailAPIResponse represents the response payload for inspecting a single Vanadis build and its chunks.
type BuildDetailAPIResponse struct {
	Status string            `json:"status"`
	Build  *VanadisBuildDTO  `json:"build,omitempty"`
	Chunks []VanadisChunkDTO `json:"chunks"`
	Entry  *VanadisBuildDTO  `json:"entry,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// Singleton tuned HTTP client for upstream vanadis communication
var vanadisHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// Singleton tuned HTTP client for upstream brain communication
var brainHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// Unix socket client for Docker daemon status inspection
var dockerSocketClient = &http.Client{
	Timeout: 2 * time.Second,
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 1 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	},
}

// HTTP client for Nomad cluster state inspection
var nomadHTTPClient = &http.Client{
	Timeout: 3 * time.Second,
}

type DockerContainerHealth = struct {
	Status string `json:"Status"`
}

type DockerContainerJSON struct {
	ID      string                 `json:"Id"`
	Names   []string               `json:"Names"`
	Image   string                 `json:"Image"`
	Created int64                  `json:"Created"`
	State   string                 `json:"State"`
	Status  string                 `json:"Status"`
	Labels  map[string]string      `json:"Labels"`
	Health  *DockerContainerHealth `json:"Health,omitempty"`
}

var defaultGitCommitPaths = []string{
	"/share/aerial/.git/refs/heads/main",
	"/share/aerial/.git/refs/heads/master",
	"/share/aerial/.git/HEAD",
}

func getGitCommit(customPaths ...string) string {
	paths := defaultGitCommitPaths
	if len(customPaths) > 0 {
		paths = customPaths
	}

	for _, p := range paths {
		if data, err := os.ReadFile(p); err == nil {
			trimmed := strings.TrimSpace(string(data))
			if strings.HasPrefix(trimmed, "ref: ") {
				refPath := filepath.Join(filepath.Dir(p), strings.TrimPrefix(trimmed, "ref: "))
				if refData, err := os.ReadFile(refPath); err == nil {
					refTrimmed := strings.TrimSpace(string(refData))
					if len(refTrimmed) >= 7 {
						return refTrimmed[:7]
					}
				}
			} else if len(trimmed) >= 7 {
				return trimmed[:7]
			}
		}
	}

	return "latest"
}

func NewMultiGitHubPoller(repos []string, token string) *GitHubPoller {
	var cleaned []string
	seen := make(map[string]bool)
	for _, r := range repos {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			cleaned = append(cleaned, trimmed)
		}
	}
	primary := ""
	if len(cleaned) > 0 {
		primary = cleaned[0]
	}

	return &GitHubPoller{
		repo:             primary,
		repos:            cleaned,
		token:            token,
		runsETagMap:      make(map[string]string),
		jobsETagMap:      make(map[int64]string),
		cachedRunsByRepo: make(map[string][]GitHubRun),
		cachedJobs:       make(map[int64][]GitHubJob),
		client: &http.Client{
			Timeout: 4 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				IdleConnTimeout:     60 * time.Second,
				TLSHandshakeTimeout: 3 * time.Second,
			},
		},
		stopCh: make(chan struct{}),
	}
}

// fetchHangarRepos queries Hangar's GET /status endpoint and extracts valid github_repo slugs.
func fetchHangarRepos(ctx context.Context, hangarURL string, client *http.Client) []string {
	if hangarURL == "" {
		return nil
	}
	reqURL := strings.TrimRight(hangarURL, "/") + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil
	}

	httpClient := client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "hangar repos response body")

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var status GitSyncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil
	}

	var repos []string
	seen := make(map[string]bool)
	for _, repoStatus := range status.Repos {
		slug := strings.TrimSpace(repoStatus.GitHubRepo)
		if slug != "" {
			lower := strings.ToLower(slug)
			if !seen[lower] {
				seen[lower] = true
				repos = append(repos, lower)
			}
		}
	}
	sort.Strings(repos)
	return repos
}

func (p *GitHubPoller) getActiveRepos() []string {
	if p == nil {
		return nil
	}
	var baseRepos []string
	if p.configPath != "" {
		baseRepos = loadDashboardRepos(p.configPath)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(baseRepos) == 0 {
		if len(p.repos) > 0 {
			baseRepos = p.repos
		} else if p.repo != "" {
			baseRepos = []string{p.repo}
		}
	}

	seen := make(map[string]bool)
	var result []string
	for _, r := range baseRepos {
		trimmed := strings.TrimSpace(r)
		lower := strings.ToLower(trimmed)
		if trimmed != "" && !seen[lower] {
			seen[lower] = true
			result = append(result, trimmed)
		}
	}
	for _, r := range p.dynamicRepos {
		trimmed := strings.TrimSpace(r)
		lower := strings.ToLower(trimmed)
		if trimmed != "" && !seen[lower] {
			seen[lower] = true
			result = append(result, trimmed)
		}
	}
	return result
}

func (p *GitHubPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	p.mu.RLock()
	hasHangar := p.hangarURL != ""
	p.mu.RUnlock()
	repos := p.getActiveRepos()
	if len(repos) == 0 && !hasHangar {
		return
	}
	go func() {
		// Initial immediate poll
		p.pollOnce(ctx)

		interval := p.pollInterval
		if interval <= 0 {
			interval = 30 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				hasActive := p.pollOnce(ctx)
				if p.pollInterval <= 0 {
					if hasActive {
						ticker.Reset(15 * time.Second)
					} else {
						ticker.Reset(45 * time.Second)
					}
				}
			}
		}
	}()
}

func drainAndClose(body io.ReadCloser, name string) {
	if body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(body, 32*1024)); err != nil {
		log.Printf("[Dashboard] Warning draining %s: %v", name, err)
	}
	if err := body.Close(); err != nil {
		log.Printf("[Dashboard] Warning closing %s: %v", name, err)
	}
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("[Dashboard:HTTP] Failed to marshal JSON response: %v", err)
		http.Error(w, `{"error":"Internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	httpkit.WriteResponse(w, r, "dashboard", b)
}

func (p *GitHubPoller) pollOnce(ctx context.Context) bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	hURL := p.hangarURL
	p.mu.RUnlock()

	if hURL != "" {
		hCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		dynRepos := fetchHangarRepos(hCtx, hURL, p.client)
		cancel()
		if len(dynRepos) > 0 {
			p.mu.Lock()
			p.dynamicRepos = dynRepos
			p.mu.Unlock()
		}
	}

	repos := p.getActiveRepos()
	if len(repos) == 0 {
		return false
	}

	baseURL := p.apiBaseURL
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	allActiveRunIDs := make(map[int64]bool)
	hasActive := false

	for _, repo := range repos {
		if p.pollRepo(ctx, repo, baseURL, allActiveRunIDs) {
			hasActive = true
		}
	}

	// Rebuild and sort aggregated cachedRuns under lock
	p.mu.Lock()
	var allRuns []GitHubRun
	activeRepoMap := make(map[string]bool)
	for _, repo := range repos {
		activeRepoMap[repo] = true
		allRuns = append(allRuns, p.cachedRunsByRepo[repo]...)
	}
	// Prune any removed repositories from cache
	for r := range p.cachedRunsByRepo {
		if !activeRepoMap[r] {
			delete(p.cachedRunsByRepo, r)
			delete(p.runsETagMap, r)
		}
	}

	// Sort runs: active runs first (newest CreatedAt first), then completed runs (newest CreatedAt first)
	sort.SliceStable(allRuns, func(i, j int) bool {
		iActive := allRuns[i].Status == "queued" || allRuns[i].Status == "in_progress"
		jActive := allRuns[j].Status == "queued" || allRuns[j].Status == "in_progress"
		if iActive != jActive {
			return iActive
		}
		return allRuns[i].CreatedAt.After(allRuns[j].CreatedAt)
	})

	for _, r := range allRuns {
		if r.Status == "in_progress" || r.Status == "queued" {
			hasActive = true
		}
	}

	p.cachedRuns = allRuns
	p.lastPollTime = time.Now().UTC()
	p.lastError = nil

	// Global job and job ETag pruning across all active run IDs
	for id := range p.cachedJobs {
		if !allActiveRunIDs[id] {
			delete(p.cachedJobs, id)
		}
	}
	for id := range p.jobsETagMap {
		if !allActiveRunIDs[id] {
			delete(p.jobsETagMap, id)
		}
	}
	p.mu.Unlock()

	return hasActive
}

func (p *GitHubPoller) pollRepo(ctx context.Context, repo, baseURL string, allActiveRunIDs map[int64]bool) bool {
	reqURL := fmt.Sprintf("%s/repos/%s/actions/runs?per_page=10", baseURL, repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	p.mu.RLock()
	etag := p.runsETagMap[repo]
	if etag == "" && repo == p.repo && p.runsETag != "" {
		etag = p.runsETag
	}
	p.mu.RUnlock()

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.mu.Lock()
		p.lastError = err
		p.mu.Unlock()
		return false
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "runs response body")

	if resp.StatusCode == http.StatusNotModified {
		hasActive := false
		p.mu.RLock()
		for _, r := range p.cachedRunsByRepo[repo] {
			allActiveRunIDs[r.ID] = true
			if r.Status == "in_progress" || r.Status == "queued" {
				hasActive = true
			}
		}
		p.mu.RUnlock()
		return hasActive
	}

	if resp.StatusCode == http.StatusOK {
		newETag := resp.Header.Get("ETag")
		var runData GitHubRunsResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&runData)
		if decodeErr != nil {
			log.Printf("[Dashboard:GitHub] Error decoding runs for %s: %v", repo, decodeErr)
			return false
		}
		// Sanitize commit messages and tag repository
		for i := range runData.WorkflowRuns {
			runData.WorkflowRuns[i].Repository = repo
			if runData.WorkflowRuns[i].HeadCommit != nil {
				rawMsg := runData.WorkflowRuns[i].HeadCommit.Message
				firstLine := strings.SplitN(rawMsg, "\n", 2)[0]
				// Truncate to 72 runes
				runes := []rune(firstLine)
				if len(runes) > 72 {
					firstLine = string(runes[:72]) + "…"
				}
				// Strip any token-like substrings
				for _, sens := range sensitiveKeys {
					firstLine = strings.ReplaceAll(firstLine, sens, "[REDACTED]")
				}
				runData.WorkflowRuns[i].HeadCommit.Message = firstLine
			}
		}

		p.mu.Lock()
		if newETag != "" {
			p.runsETagMap[repo] = newETag
			if repo == p.repo {
				p.runsETag = newETag
			}
		}
		p.cachedRunsByRepo[repo] = runData.WorkflowRuns
		p.mu.Unlock()

		hasActive := false
		for _, r := range runData.WorkflowRuns {
			allActiveRunIDs[r.ID] = true
			if r.Status == "in_progress" || r.Status == "queued" || time.Since(r.UpdatedAt) < 10*time.Minute {
				p.fetchJobsForRun(ctx, r.ID, repo)
			}
			if r.Status == "in_progress" || r.Status == "queued" {
				hasActive = true
			}
		}
		return hasActive
	}

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == 429 {
		log.Printf("[Dashboard:GitHub] Warning: rate limited on %s: HTTP %d", repo, resp.StatusCode)
	} else {
		log.Printf("[Dashboard:GitHub] Warning: unexpected HTTP %d querying %s", resp.StatusCode, repo)
	}
	return false
}

func (p *GitHubPoller) fetchJobsForRun(ctx context.Context, runID int64, optionalRepo ...string) {
	baseURL := p.apiBaseURL
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	repo := ""
	if len(optionalRepo) > 0 && optionalRepo[0] != "" {
		repo = optionalRepo[0]
	} else {
		repos := p.getActiveRepos()
		if len(repos) > 0 {
			repo = repos[0]
		}
	}
	if repo == "" {
		return
	}

	reqURL := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs", baseURL, repo, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	p.mu.RLock()
	etag := p.jobsETagMap[runID]
	p.mu.RUnlock()

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "jobs response body")

	if resp.StatusCode == http.StatusNotModified {
		return
	}

	if resp.StatusCode == http.StatusOK {
		jobETag := resp.Header.Get("ETag")
		var jobData GitHubJobsResponse
		if err := json.NewDecoder(resp.Body).Decode(&jobData); err == nil {
			p.mu.Lock()
			if jobETag != "" {
				p.jobsETagMap[runID] = jobETag
			}
			p.cachedJobs[runID] = jobData.Jobs
			p.mu.Unlock()
		}
	}
}

func (p *GitHubPoller) GetSnapshot() ([]GitHubRun, map[int64][]GitHubJob) {
	if p == nil {
		return nil, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	runs := make([]GitHubRun, len(p.cachedRuns))
	copy(runs, p.cachedRuns)

	jobs := make(map[int64][]GitHubJob, len(p.cachedJobs))
	for k, v := range p.cachedJobs {
		jobsCopy := make([]GitHubJob, len(v))
		copy(jobsCopy, v)
		jobs[k] = jobsCopy
	}
	return runs, jobs
}

func mergeClusterDeploymentsWithHangar(
	rawContainers []DockerContainerJSON,
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	gitSync GitSyncStatusResponse,
	currentCommit string,
) []DeploymentStatus {
	return MergeClusterDeploymentsWithHangar(runs, jobs, gitSync, rawContainers, currentCommit, time.Time{})
}

func fetchDockerClusterState(ctx context.Context) ([]ServiceStatus, []DockerContainerJSON, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/containers/json", nil)
	if err != nil {
		return nil, nil, err
	}

	resp, err := dockerSocketClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "docker response body")

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("docker API status %d", resp.StatusCode)
	}

	var rawContainers []DockerContainerJSON
	if err := json.NewDecoder(resp.Body).Decode(&rawContainers); err != nil {
		return nil, nil, err
	}

	now := time.Now().UTC()
	var services []ServiceStatus

	for _, c := range rawContainers {
		svcName, isAerial := ExtractAerialServiceName(c)
		if !isAerial || svcName == "" {
			continue
		}

		createdAt := time.Unix(c.Created, 0).UTC()
		uptimeSec := int64(now.Sub(createdAt).Seconds())
		if uptimeSec < 0 {
			uptimeSec = 0
		}

		status := "healthy"
		statusLower := strings.ToLower(c.Status)
		if strings.Contains(statusLower, "(unhealthy)") {
			status = "unhealthy"
		} else if strings.Contains(statusLower, "starting") || strings.Contains(statusLower, "(health: starting)") {
			status = "starting"
		} else if c.State != "running" {
			status = "unhealthy"
		}

		services = append(services, ServiceStatus{
			Name:          svcName,
			Status:        status,
			UptimeSeconds: uptimeSec,
			LastCheckTime: now,
		})
	}

	return services, rawContainers, nil
}

func fetchNomadClusterState(ctx context.Context, nomadAddr string) ([]ServiceStatus, error) {
	cleanNomadAddr := strings.TrimRight(strings.TrimSpace(nomadAddr), "/")
	if cleanNomadAddr == "" {
		return nil, fmt.Errorf("empty nomad address")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	jobsURL := cleanNomadAddr + "/v1/jobs?filter=" + url.QueryEscape(`Type=="service"`)
	reqJobs, err := http.NewRequestWithContext(reqCtx, http.MethodGet, jobsURL, nil)
	if err != nil {
		return nil, err
	}
	reqJobs.Header.Set("Accept", "application/json")

	respJobs, err := nomadHTTPClient.Do(reqJobs)
	if err != nil {
		return nil, err
	}
	defer respJobs.Body.Close()
	defer drainAndClose(respJobs.Body, "nomad jobs response body")

	if respJobs.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nomad jobs returned HTTP %d", respJobs.StatusCode)
	}

	jobsData, err := io.ReadAll(respJobs.Body)
	if err != nil {
		return nil, err
	}

	allocsURL := cleanNomadAddr + "/v1/allocations?filter=" + url.QueryEscape(`ClientStatus=="running"`)
	reqAllocs, err := http.NewRequestWithContext(reqCtx, http.MethodGet, allocsURL, nil)
	if err != nil {
		return nil, err
	}
	reqAllocs.Header.Set("Accept", "application/json")

	respAllocs, err := nomadHTTPClient.Do(reqAllocs)
	if err != nil {
		return nil, err
	}
	defer respAllocs.Body.Close()
	defer drainAndClose(respAllocs.Body, "nomad allocs response body")

	if respAllocs.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nomad allocs returned HTTP %d", respAllocs.StatusCode)
	}

	allocsData, err := io.ReadAll(respAllocs.Body)
	if err != nil {
		return nil, err
	}

	return ParseNomadClusterState(jobsData, allocsData, time.Now().UTC())
}

func fetchActiveTasksFromBrain(ctx context.Context, brainURL string) ([]ActiveTaskStatus, error) {
	if brainURL == "" {
		return []ActiveTaskStatus{}, nil
	}

	reqURL := strings.TrimRight(brainURL, "/") + "/tasks"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return []ActiveTaskStatus{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := brainHTTPClient.Do(req)
	if err != nil {
		return []ActiveTaskStatus{}, err
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "brain tasks response body")

	if resp.StatusCode != http.StatusOK {
		return []ActiveTaskStatus{}, fmt.Errorf("brain returned HTTP %d", resp.StatusCode)
	}

	var apiResp BrainTasksAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return []ActiveTaskStatus{}, err
	}

	tasks := make([]ActiveTaskStatus, 0, len(apiResp.Tasks))
	for _, t := range apiResp.Tasks {
		startedAt := t.UpdatedAt
		if t.Status == "PENDING" || startedAt.IsZero() {
			startedAt = t.CreatedAt
		}
		prompt := t.Prompt
		if runes := []rune(prompt); len(runes) > 500 {
			prompt = string(runes[:500]) + "..."
		}
		summary := strings.TrimSpace(t.Summary)
		if summary == "" {
			summary = prompt
		}
		tasks = append(tasks, ActiveTaskStatus{
			ID:          t.ID,
			ThreadID:    t.ThreadID,
			SessionID:   t.SessionID,
			AuthorName:  t.AuthorName,
			Prompt:      prompt,
			Summary:     summary,
			Status:      t.Status,
			RetryCount:  t.RetryCount,
			TriggerType: t.TriggerType,
			CreatedAt:   t.CreatedAt,
			StartedAt:   startedAt,
		})
	}
	return tasks, nil
}

// DefaultQuickLaunchLinks returns the core default gateway launch targets.
func DefaultQuickLaunchLinks() []QuickLaunchLink {
	return []QuickLaunchLink{}
}

type rawUserConfig struct {
	Dashboard struct {
		QuickLinks       []QuickLaunchLink `yaml:"quick_links"`
		QuickLaunchLinks []QuickLaunchLink `yaml:"quick_launch_links"`
		GitHubRepos      []string          `yaml:"github_repos"`
		Repos            []string          `yaml:"repos"`
		VanadisURL       string            `yaml:"vanadis_url"`
		HaroURL          string            `yaml:"haro_url"`
	} `yaml:"dashboard"`
	QuickLinks  []QuickLaunchLink `yaml:"quick_links"`
	GitHubRepos []string          `yaml:"github_repos"`
	VanadisURL  string            `yaml:"vanadis_url"`
	HaroURL     string            `yaml:"haro_url"`
}

// loadRawUserConfig parses and unmarshals rawUserConfig from the given path.
func loadRawUserConfig(configPath string) (rawUserConfig, bool) {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return rawUserConfig{}, false
	}
	configPath = filepath.Clean(configPath)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return rawUserConfig{}, false
	}

	var cfg rawUserConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Printf("[Dashboard] Warning: failed to parse %s: %v", configPath, err)
		return rawUserConfig{}, false
	}
	return cfg, true
}

// loadDashboardVanadisURL parses the Vanadis upstream URL configured in config.yaml.
func loadDashboardVanadisURL(configPath string) string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return ""
	}

	val := strings.TrimSpace(cfg.Dashboard.VanadisURL)
	if val == "" {
		val = strings.TrimSpace(cfg.VanadisURL)
	}
	return val
}

// loadDashboardHaroURL parses the Haro upstream URL configured in config.yaml.
func loadDashboardHaroURL(configPath string) string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return ""
	}

	val := strings.TrimSpace(cfg.Dashboard.HaroURL)
	if val == "" {
		val = strings.TrimSpace(cfg.HaroURL)
	}
	return val
}

// loadDashboardRepos parses the list of GitHub repositories configured for status dashboard monitoring.
func loadDashboardRepos(configPath string) []string {
	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return nil
	}

	rawList := cfg.Dashboard.GitHubRepos
	if len(rawList) == 0 {
		rawList = cfg.Dashboard.Repos
	}
	if len(rawList) == 0 {
		rawList = cfg.GitHubRepos
	}

	var repos []string
	seen := make(map[string]bool)
	for _, r := range rawList {
		trimmed := strings.TrimSpace(r)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		repos = append(repos, trimmed)
	}

	return repos
}

// loadQuickLaunchLinks merges default core links with custom links defined in config.yaml.
func loadQuickLaunchLinks(configPath string) []QuickLaunchLink {
	links := DefaultQuickLaunchLinks()

	cfg, ok := loadRawUserConfig(configPath)
	if !ok {
		return links
	}

	custom := cfg.Dashboard.QuickLinks
	if len(custom) == 0 {
		custom = cfg.Dashboard.QuickLaunchLinks
	}
	if len(custom) == 0 {
		custom = cfg.QuickLinks
	}

	for _, l := range custom {
		if l.Name == "" || l.URL == "" {
			continue
		}
		if l.Target == "" {
			l.Target = "_blank"
		}
		l.IsCustom = true
		links = append(links, l)
	}

	return links
}

// fetchGitSyncStatus queries the GitSync sidecar for real-time repository checkout and lag metadata.
func fetchGitSyncStatus(ctx context.Context, gitsyncURL string) GitSyncStatusResponse {
	fallback := GitSyncStatusResponse{
		Status:        "synced",
		MaxLagSeconds: 0,
		LastSyncTime:  time.Now().UTC(),
	}

	if gitsyncURL == "" {
		return fallback
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(gitsyncURL, "/")+"/status", nil)
	if err != nil {
		fallback.Error = err.Error()
		return fallback
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fallback.Error = err.Error()
		return fallback
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "gitsync response body")

	if resp.StatusCode != http.StatusOK {
		fallback.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return fallback
	}

	var status GitSyncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		fallback.Error = err.Error()
		return fallback
	}

	return status
}

func statusHandler(brainURL, gitsyncURL, configPath, gitCommit string, optionalNomadAddr ...string) http.HandlerFunc {
	nomadAddr := ""
	if len(optionalNomadAddr) > 0 {
		nomadAddr = strings.TrimSpace(optionalNomadAddr[0])
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		now := time.Now().UTC()
		services, rawContainers, err := fetchDockerClusterState(ctx)
		currentCommit := gitCommit
		if currentCommit == "" {
			currentCommit = "latest"
		}

		var ghRuns []GitHubRun
		var ghJobs map[int64][]GitHubJob
		if globalGHPoller != nil {
			ghRuns, ghJobs = globalGHPoller.GetSnapshot()
		}

		gitSync := fetchGitSyncStatus(ctx, gitsyncURL)
		deployments := mergeClusterDeploymentsWithHangar(rawContainers, ghRuns, ghJobs, gitSync, currentCommit)
		if deployments == nil {
			deployments = []DeploymentStatus{}
		}

		activeTasks, taskErr := fetchActiveTasksFromBrain(ctx, brainURL)
		if taskErr != nil {
			log.Printf("[Dashboard] Failed to fetch active tasks from brain (%s): %v", brainURL, taskErr)
			activeTasks = []ActiveTaskStatus{}
		}
		if activeTasks == nil {
			activeTasks = []ActiveTaskStatus{}
		}

		quickLinks := loadQuickLaunchLinks(configPath)

		if err != nil || len(services) == 0 {
			uptimeSec := int64(time.Since(startTime).Seconds())
			services = []ServiceStatus{
				{Name: "brain", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "haro", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "coven", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "discord-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "docker-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "github-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "nomad-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "infisical-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "webhooks-router", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "ollama", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "agentsview", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "hangar", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "autoheal", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "proxy", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "dashboard", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			}
		}

		if nomadAddr != "" {
			nomadServices, nomadErr := fetchNomadClusterState(ctx, nomadAddr)
			if nomadErr == nil && len(nomadServices) > 0 {
				services = nomadServices
			} else if nomadErr != nil {
				log.Printf("[dashboard] Warning: Failed to fetch Nomad cluster state (%s): %v, falling back to Docker", nomadAddr, nomadErr)
			}
		}

		clusterStatus := CalculateClusterStatus(services)
		ongoingDeploy := CalculateDeployStatus(deployments)
		isDeploying := IsDeployOngoing(deployments)

		resp := ClusterResponse{
			SystemTime:       now,
			ClusterStatus:    clusterStatus,
			ActiveTasksCount: len(activeTasks),
			ActiveTasks:      activeTasks,
			Services:         services,
			Deployments:      deployments,
			QuickLaunchLinks: quickLinks,
			GitSync:          gitSync,
			OngoingDeploy:    ongoingDeploy,
			IsDeploying:      isDeploying,
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, r, http.StatusOK, resp)
	}
}

// fetchUpstreamJSON executes an HTTP GET request with a 4-second timeout against targetURL,
// decodes any non-200 JSON error into upstreamErrMsg, or unmarshals the 200 OK JSON response into dest.
func fetchUpstreamJSON(ctx context.Context, client *http.Client, targetURL string, dest interface{}) (int, string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "upstream response body")

	if resp.StatusCode != http.StatusOK {
		var errPayload struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errPayload); err == nil && errPayload.Error != "" {
			return resp.StatusCode, strings.TrimSpace(errPayload.Error), fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
		}
		return resp.StatusCode, "", fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return http.StatusOK, "", err
	}
	return http.StatusOK, "", nil
}

func factsHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, limit, offset, err := BuildFactsUpstreamURL(brainBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data FactsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, FactsAPIResponse{
					Facts:  []FactItem{},
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, FactsAPIResponse{
					Facts:  []FactItem{},
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Status: "error",
					Error:  "Upstream brain error occurred",
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, FactsAPIResponse{
				Facts:  []FactItem{},
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Status: "error",
				Error:  "Failed to decode upstream brain response",
			})
			return
		}

		if data.Facts == nil {
			data.Facts = []FactItem{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func schedulesHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, err := BuildSchedulesUpstreamURL(haroBaseURL)
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data SchedulesAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, SchedulesAPIResponse{
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
					Summary: ScheduleSummaryMetrics{
						TotalActive:    0,
						CronCount:      0,
						OneShotCount:   0,
						TotalRuns24h:   0,
						SuccessRate24h: 100.0,
					},
					Crons:    []CronSchedule{},
					OneShots: []OneShotSchedule{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, SchedulesAPIResponse{
					Status: "error",
					Error:  "Upstream brain error occurred",
					Summary: ScheduleSummaryMetrics{
						TotalActive:    0,
						CronCount:      0,
						OneShotCount:   0,
						TotalRuns24h:   0,
						SuccessRate24h: 100.0,
					},
					Crons:    []CronSchedule{},
					OneShots: []OneShotSchedule{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, SchedulesAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream brain response",
				Summary: ScheduleSummaryMetrics{
					TotalActive:    0,
					CronCount:      0,
					OneShotCount:   0,
					TotalRuns24h:   0,
					SuccessRate24h: 100.0,
				},
				Crons:    []CronSchedule{},
				OneShots: []OneShotSchedule{},
			})
			return
		}

		if data.Crons == nil {
			data.Crons = []CronSchedule{}
		}
		if data.OneShots == nil {
			data.OneShots = []OneShotSchedule{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func scheduleRunsHandler(haroBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, limit, offset, err := BuildScheduleRunsUpstreamURL(haroBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data ScheduleRunsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), brainHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, ScheduleRunsAPIResponse{
					Status: "degraded",
					Error:  "Brain service unreachable. Retrying...",
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Runs:   []ScheduleRun{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream brain returned status %d", statusCode)
				writeJSON(w, r, statusCode, ScheduleRunsAPIResponse{
					Status: "error",
					Error:  "Upstream brain error occurred",
					Total:  0,
					Limit:  limit,
					Offset: offset,
					Runs:   []ScheduleRun{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, ScheduleRunsAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream brain response",
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Runs:   []ScheduleRun{},
			})
			return
		}

		if data.Runs == nil {
			data.Runs = []ScheduleRun{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func buildsHandler(vanadisBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, _, err := BuildVanadisBuildsUpstreamURL(vanadisBaseURL, r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data BuildsAPIResponse
		statusCode, _, fetchErr := fetchUpstreamJSON(r.Context(), vanadisHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream vanadis request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, BuildsAPIResponse{
					Status: "degraded",
					Error:  "Vanadis service unreachable. Retrying...",
					Total:  0,
					Builds: []VanadisBuildDTO{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream vanadis returned status %d", statusCode)
				writeJSON(w, r, statusCode, BuildsAPIResponse{
					Status: "error",
					Error:  "Upstream vanadis error occurred",
					Total:  0,
					Builds: []VanadisBuildDTO{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, BuildsAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream vanadis response",
				Total:  0,
				Builds: []VanadisBuildDTO{},
			})
			return
		}

		if data.Builds == nil {
			data.Builds = []VanadisBuildDTO{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func buildDetailHandler(vanadisBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, _, err := BuildVanadisBuildDetailUpstreamURL(vanadisBaseURL, r.URL.Path)
		if err != nil {
			cleanBase := strings.TrimRight(strings.TrimSpace(vanadisBaseURL), "/")
			if cleanBase == "" || strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
				http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			writeJSON(w, r, http.StatusBadRequest, BuildDetailAPIResponse{
				Status: "error",
				Error:  err.Error(),
				Chunks: []VanadisChunkDTO{},
			})
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		var data BuildDetailAPIResponse
		statusCode, upstreamErrMsg, fetchErr := fetchUpstreamJSON(r.Context(), vanadisHTTPClient, targetURL, &data)
		if fetchErr != nil {
			if statusCode == 0 {
				log.Printf("[Dashboard] Upstream vanadis detail request failed (%s): %v", targetURL, fetchErr)
				writeJSON(w, r, http.StatusServiceUnavailable, BuildDetailAPIResponse{
					Status: "degraded",
					Error:  "Vanadis service unreachable. Retrying...",
					Chunks: []VanadisChunkDTO{},
				})
				return
			}
			if statusCode != http.StatusOK {
				log.Printf("[Dashboard] Upstream vanadis detail returned status %d", statusCode)
				errMsg := "Upstream vanadis error occurred"
				if statusCode == http.StatusNotFound {
					errMsg = "Build not found"
				}
				if upstreamErrMsg != "" {
					errMsg = upstreamErrMsg
				}
				writeJSON(w, r, statusCode, BuildDetailAPIResponse{
					Status: "error",
					Error:  errMsg,
					Chunks: []VanadisChunkDTO{},
				})
				return
			}
			writeJSON(w, r, http.StatusBadGateway, BuildDetailAPIResponse{
				Status: "error",
				Error:  "Failed to decode upstream vanadis response",
				Chunks: []VanadisChunkDTO{},
			})
			return
		}

		if data.Status == "" {
			data.Status = "ok"
		}
		if data.Chunks == nil {
			data.Chunks = []VanadisChunkDTO{}
		}

		writeJSON(w, r, http.StatusOK, data)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "dashboard", []byte("OK"))
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		next.ServeHTTP(w, r)
	})
}

type StaticAsset struct {
	Data        []byte
	Hash        string
	ETag        string
	ContentType string
}

type AssetRegistry struct {
	assets map[string]StaticAsset
}

func NewAssetRegistry(fsys fs.FS, versionToken string) (*AssetRegistry, error) {
	reg := &AssetRegistry{
		assets: make(map[string]StaticAsset),
	}

	// 1. Compute content hashes for individual assets
	var appJSHash, styleCSSHash string
	if data, err := fs.ReadFile(fsys, "app.js"); err == nil {
		h := sha256.Sum256(data)
		appJSHash = hex.EncodeToString(h[:])
	}
	if data, err := fs.ReadFile(fsys, "style.css"); err == nil {
		h := sha256.Sum256(data)
		styleCSSHash = hex.EncodeToString(h[:])
	}

	// Determine asset version strings to inject
	jsVersion := versionToken
	cssVersion := versionToken
	if versionToken == "" || versionToken == "latest" || versionToken == "dev" || versionToken == "unknown" {
		if len(appJSHash) >= 10 {
			jsVersion = appJSHash[:10]
		} else {
			jsVersion = "v1"
		}
		if len(styleCSSHash) >= 10 {
			cssVersion = styleCSSHash[:10]
		} else {
			cssVersion = "v1"
		}
	}

	// 2. Walk the embedded filesystem and populate registry
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		rawBytes, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}

		cleanPath := filepath.ToSlash(path)
		cleanPath = strings.TrimPrefix(cleanPath, "./")

		var finalBytes []byte
		if cleanPath == "index.html" {
			htmlStr := string(rawBytes)
			reJS := regexp.MustCompile(`src="app\.js(?:\?[^"]*)?"`)
			htmlStr = reJS.ReplaceAllString(htmlStr, fmt.Sprintf(`src="app.js?v=%s"`, jsVersion))

			reCSS := regexp.MustCompile(`href="style\.css(?:\?[^"]*)?"`)
			htmlStr = reCSS.ReplaceAllString(htmlStr, fmt.Sprintf(`href="style.css?v=%s"`, cssVersion))

			finalBytes = []byte(htmlStr)
		} else {
			finalBytes = rawBytes
		}

		hash := sha256.Sum256(finalBytes)
		hashStr := hex.EncodeToString(hash[:])
		etag := fmt.Sprintf(`"%s"`, hashStr[:16])

		reg.assets[cleanPath] = StaticAsset{
			Data:        finalBytes,
			Hash:        hashStr,
			ETag:        etag,
			ContentType: GetMimeType(cleanPath),
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return reg, nil
}

func (ar *AssetRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	reqPath := strings.TrimPrefix(r.URL.Path, "/dashboard")
	reqPath = strings.TrimPrefix(reqPath, "/")
	if reqPath == "" || reqPath == "index.html" {
		reqPath = "index.html"
	}

	asset, exists := ar.assets[reqPath]
	if !exists {
		http.NotFound(w, r)
		return
	}

	// Bifurcated Cache Policy:
	// - index.html / unversioned: no-cache, must-revalidate + ETag
	// - Versioned assets (?v=...): public, max-age=31536000, immutable + ETag
	isVersioned := r.URL.Query().Get("v") != "" && reqPath != "index.html"
	if isVersioned {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	}

	w.Header().Set("ETag", asset.ETag)
	w.Header().Set("Content-Type", asset.ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(asset.Data)))

	// Check conditional validation (If-None-Match)
	if MatchesETag(r.Header.Get("If-None-Match"), asset.ETag, asset.Hash) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	httpkit.WriteResponse(w, r, "dashboard", asset.Data)
}

// DashboardConfig holds configuration settings for the dashboard web server.
type DashboardConfig struct {
	Port       string
	BrainURL   string
	MemoryURL  string
	GHRepo     string
	GHRepos    []string
	GHToken    string
	GitCommit  string
	GitSyncURL string
	HangarURL  string
	ConfigPath string
	APIBaseURL string
	NomadAddr  string
	VanadisURL string
	HaroURL    string
}

// DashboardYAMLConfig defines the declarative file schema for dashboard.yaml.
type DashboardYAMLConfig struct {
	Port       string   `yaml:"port"`
	BrainURL   string   `yaml:"brain_url"`
	MemoryURL  string   `yaml:"memory_url"`
	HangarURL  string   `yaml:"hangar_url"`
	GitSyncURL string   `yaml:"gitsync_url"`
	GHRepo     string   `yaml:"github_repo"`
	GHRepos    []string `yaml:"github_repos"`
	ConfigPath string   `yaml:"config_path"`
	NomadAddr  string   `yaml:"nomad_addr"`
	VanadisURL string   `yaml:"vanadis_url"`
	HaroURL    string   `yaml:"haro_url"`
}

// NewDashboardConfigFromLookup parses DashboardConfig using a key lookup function with YAML config file support.
func NewDashboardConfigFromLookup(lookup func(string) string) DashboardConfig {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}

	cfgFile := strings.TrimSpace(lookup("CONFIG_PATH"))
	if cfgFile == "" && flag.Lookup("test.v") == nil {
		if _, err := os.Stat("/config/config.yaml"); err == nil {
			cfgFile = "/config/config.yaml"
		} else if _, err := os.Stat("/share/aerial-config/services/dashboard/dashboard.yaml"); err == nil {
			cfgFile = "/share/aerial-config/services/dashboard/dashboard.yaml"
		}
	}

	var ycfg DashboardYAMLConfig
	if cfgFile != "" {
		if data, err := os.ReadFile(cfgFile); err == nil {
			if err := yaml.Unmarshal(data, &ycfg); err != nil {
				log.Printf("[dashboard] Warning: Failed to parse %s: %v", cfgFile, err)
			}
		}
	}

	brainURL := strings.TrimSpace(lookup("BRAIN_URL"))
	if brainURL == "" && ycfg.BrainURL != "" {
		brainURL = strings.TrimSpace(ycfg.BrainURL)
	}
	if brainURL == "" {
		brainURL = "http://brain:8080"
	}

	memoryURL := strings.TrimSpace(lookup("MEMORY_MCP_URL"))
	if memoryURL == "" && ycfg.MemoryURL != "" {
		memoryURL = strings.TrimSpace(ycfg.MemoryURL)
	}
	if memoryURL == "" {
		memoryURL = "http://coven.aerial"
	}

	ghRepo, ghRepos := resolveDashboardGitHubRepos(lookup, ycfg.GHRepos, ycfg.GHRepo)

	ghToken := strings.TrimSpace(lookup("GITHUB_PAT"))
	if ghToken == "" {
		ghToken = strings.TrimSpace(lookup("GITHUB_PERSONAL_ACCESS_TOKEN"))
	}

	port := strings.TrimSpace(lookup("PORT"))
	if port == "" && ycfg.Port != "" {
		port = strings.TrimSpace(ycfg.Port)
	}
	if port == "" {
		port = "8080"
	}

	hangarURL := strings.TrimSpace(lookup("HANGAR_URL"))
	if hangarURL == "" {
		hangarURL = strings.TrimSpace(lookup("GITSYNC_URL"))
	}
	if hangarURL == "" && ycfg.HangarURL != "" {
		hangarURL = strings.TrimSpace(ycfg.HangarURL)
	}
	if hangarURL == "" && ycfg.GitSyncURL != "" {
		hangarURL = strings.TrimSpace(ycfg.GitSyncURL)
	}
	if hangarURL == "" {
		hangarURL = "http://hangar:8080"
	}

	configPath := strings.TrimSpace(lookup("AERIAL_CONFIG_PATH"))
	if configPath == "" && ycfg.ConfigPath != "" {
		configPath = strings.TrimSpace(ycfg.ConfigPath)
	}
	if configPath == "" {
		configPath = "/share/aerial-config/config.yaml"
	}

	gitCommit := normalizeGitCommit(lookup("GIT_COMMIT"))
	if gitCommit == "" {
		gitCommit = "latest"
	}

	nomadAddr := strings.TrimSpace(lookup("NOMAD_ADDR"))
	if nomadAddr == "" && ycfg.NomadAddr != "" {
		nomadAddr = strings.TrimSpace(ycfg.NomadAddr)
	}
	if nomadAddr == "" && flag.Lookup("test.v") == nil {
		nomadAddr = "http://127.0.0.1:4646"
	}

	vanadisURL := resolveDashboardVanadisURL(configPath, ycfg.VanadisURL)
	haroURL := resolveDashboardHaroURL(lookup, configPath, ycfg.HaroURL)

	return DashboardConfig{
		Port:       port,
		BrainURL:   brainURL,
		MemoryURL:  memoryURL,
		GHRepo:     ghRepo,
		GHRepos:    ghRepos,
		GHToken:    ghToken,
		GitSyncURL: hangarURL,
		HangarURL:  hangarURL,
		ConfigPath: configPath,
		GitCommit:  gitCommit,
		NomadAddr:  nomadAddr,
		VanadisURL: vanadisURL,
		HaroURL:    haroURL,
	}
}

// NewDashboardConfigFromEnv parses DashboardConfig from environment variables.
func NewDashboardConfigFromEnv() DashboardConfig {
	cfg := NewDashboardConfigFromLookup(os.Getenv)
	if cfg.GitCommit == "latest" || cfg.GitCommit == "" {
		cfg.GitCommit = getGitCommit()
	}
	return cfg
}

// SetupDashboardMux configures the HTTP router and route handlers.
func SetupDashboardMux(cfg DashboardConfig, assetReg *AssetRegistry) http.Handler {
	mux := http.NewServeMux()
	memoryURL := strings.TrimSpace(cfg.MemoryURL)
	if memoryURL == "" {
		memoryURL = cfg.BrainURL
	}
	haroURL := strings.TrimSpace(cfg.HaroURL)
	if haroURL == "" {
		haroURL = cfg.BrainURL
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/dashboard/health", healthHandler)
	mux.HandleFunc("/api/status", statusHandler(cfg.BrainURL, cfg.GitSyncURL, cfg.ConfigPath, cfg.GitCommit, cfg.NomadAddr))
	mux.HandleFunc("/dashboard/api/status", statusHandler(cfg.BrainURL, cfg.GitSyncURL, cfg.ConfigPath, cfg.GitCommit, cfg.NomadAddr))
	mux.HandleFunc("/api/facts", factsHandler(memoryURL))
	mux.HandleFunc("/dashboard/api/facts", factsHandler(memoryURL))
	mux.HandleFunc("/api/schedules", schedulesHandler(haroURL))
	mux.HandleFunc("/dashboard/api/schedules", schedulesHandler(haroURL))
	mux.HandleFunc("/api/schedules/runs", scheduleRunsHandler(haroURL))
	mux.HandleFunc("/dashboard/api/schedules/runs", scheduleRunsHandler(haroURL))
	mux.HandleFunc("/api/builds", buildsHandler(cfg.VanadisURL))
	mux.HandleFunc("/dashboard/api/builds", buildsHandler(cfg.VanadisURL))
	mux.HandleFunc("/api/builds/", buildDetailHandler(cfg.VanadisURL))
	mux.HandleFunc("/dashboard/api/builds/", buildDetailHandler(cfg.VanadisURL))
	coverageFlush := httpkit.HandleCoverageFlush("dashboard", false)
	mux.HandleFunc("/debug/coverage/flush", coverageFlush)
	mux.HandleFunc("/dashboard/debug/coverage/flush", coverageFlush)

	if assetReg != nil {
		mux.HandleFunc("/", assetReg.ServeHTTP)
		mux.Handle("/dashboard/", http.StripPrefix("/dashboard", http.HandlerFunc(assetReg.ServeHTTP)))
	}

	return securityHeadersMiddleware(mux)
}

// RunDashboardServer starts the GitHub poller, creates the asset registry, and listens on the configured port.
func RunDashboardServer(ctx context.Context, cfg DashboardConfig) error {
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		return fmt.Errorf("failed to create static sub filesystem: %w", err)
	}

	gitCommit := cfg.GitCommit
	if gitCommit == "" {
		gitCommit = "latest"
	}
	assetReg, err := NewAssetRegistry(staticFS, gitCommit)
	if err != nil {
		return fmt.Errorf("failed to initialize asset registry: %w", err)
	}

	repos := cfg.GHRepos
	if len(repos) == 0 && cfg.GHRepo != "" {
		repos = []string{cfg.GHRepo}
	}
	hangarURL := cfg.HangarURL
	if hangarURL == "" {
		hangarURL = cfg.GitSyncURL
	}
	if len(repos) > 0 || hangarURL != "" {
		globalGHPoller = NewMultiGitHubPoller(repos, cfg.GHToken)
		globalGHPoller.configPath = cfg.ConfigPath
		globalGHPoller.hangarURL = hangarURL
		if cfg.APIBaseURL != "" {
			globalGHPoller.apiBaseURL = cfg.APIBaseURL
		}
		globalGHPoller.Start(ctx)
	}

	handler := SetupDashboardMux(cfg, assetReg)

	log.Printf("aerial-dashboard server starting on :%s (upstream brain=%s)", cfg.Port, cfg.BrainURL)
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handler,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down aerial-dashboard server gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return fmt.Errorf("server fatal error: %w", err)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := NewDashboardConfigFromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunDashboardServer(ctx, cfg); err != nil {
		log.Fatalf("dashboard failed: %v", err)
	}
}
