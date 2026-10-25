// Package main implements the Aerial Permet HUD status dashboard and telemetry viewer.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
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
	"strings"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
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
