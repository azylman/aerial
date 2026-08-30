package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static/*
var content embed.FS

var startTime = time.Now().UTC()

var sensitiveKeys = []string{
	"GEMINI_API_KEY",
	"DISCORD_TOKEN",
	"DISCORD_BOT_TOKEN",
	"GITHUB_PAT",
	"GITHUB_PERSONAL_ACCESS_TOKEN",
	"HA_TOKEN",
	"SECRET",
	"PASSWORD",
	"TOKEN",
	"KEY",
}

func SanitizeEnvVars(envVars []string) []string {
	var cleaned []string
	for _, env := range envVars {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToUpper(parts[0])
		isSensitive := false
		for _, sensitive := range sensitiveKeys {
			if strings.Contains(key, sensitive) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			cleaned = append(cleaned, fmt.Sprintf("%s=[REDACTED]", parts[0]))
		} else {
			cleaned = append(cleaned, env)
		}
	}
	return cleaned
}

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
}

type DeploymentStep struct {
	Name   string `json:"name"`
	Icon   string `json:"icon"`
	Status string `json:"status"` // "completed", "active", "pending", "failed"
}

type DeploymentStatus struct {
	ID         string           `json:"id"`
	Service    string           `json:"service"`
	Commit     string           `json:"commit"`
	CommitMsg  string           `json:"commit_msg,omitempty"`
	Stage      string           `json:"stage"` // "queued", "building", "failed", "awaiting_pull", "swapping", "live", "degraded"
	Progress   int              `json:"progress"`
	Steps      []DeploymentStep `json:"steps"`
	MatrixJobs []MatrixJobChip  `json:"matrix_jobs,omitempty"`
	HTMLURL    string           `json:"html_url,omitempty"`
	StartedAt  time.Time        `json:"started_at"`
}

type ClusterResponse struct {
	SystemTime    time.Time          `json:"system_time"`
	ClusterStatus string             `json:"cluster_status"`
	Services      []ServiceStatus    `json:"services"`
	Deployments   []DeploymentStatus `json:"deployments"`
}

type GitHubRun struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	HeadSHA    string    `json:"head_sha"`
	HeadBranch string    `json:"head_branch"`
	HeadCommit *struct {
		Message string `json:"message"`
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
	token        string
	client       *http.Client
	runsETag     string
	jobsETagMap  map[int64]string

	mu           sync.RWMutex
	cachedRuns   []GitHubRun
	cachedJobs   map[int64][]GitHubJob
	lastPollTime time.Time
	lastError    error
	stopCh       chan struct{}
}

var globalGHPoller *GitHubPoller

// FactItem represents a semantic memory item received from brain
type FactItem struct {
	ID         int64     `json:"id"`
	Category   string    `json:"category"`
	FactText   string    `json:"fact_text"`
	Importance float64   `json:"importance"`
	ThreadID   string    `json:"thread_id"`
	CreatedAt  time.Time `json:"created_at"`
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
	ID           int64     `json:"id"`
	ScheduleID   string    `json:"schedule_id"`
	ScheduleType string    `json:"schedule_type"`
	Prompt       string    `json:"prompt"`
	Title        string    `json:"title,omitempty"`
	TriggeredAt  time.Time `json:"triggered_at"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	ThreadID     string    `json:"thread_id,omitempty"`
	MessageID    string    `json:"message_id,omitempty"`
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

type DockerContainerJSON struct {
	ID      string            `json:"Id"`
	Names   []string          `json:"Names"`
	Image   string            `json:"Image"`
	Created int64             `json:"Created"`
	State   string            `json:"State"`
	Status  string            `json:"Status"`
	Labels  map[string]string `json:"Labels"`
	Health  *struct {
		Status string `json:"Status"`
	} `json:"Health,omitempty"`
}

func getGitCommit() string {
	if envCommit := os.Getenv("GIT_COMMIT"); envCommit != "" {
		if len(envCommit) > 7 {
			return envCommit[:7]
		}
		return envCommit
	}

	paths := []string{
		"/share/aerial/.git/refs/heads/main",
		"/share/aerial/.git/refs/heads/master",
		"/share/aerial/.git/HEAD",
	}

	for _, p := range paths {
		if data, err := os.ReadFile(p); err == nil {
			trimmed := strings.TrimSpace(string(data))
			if strings.HasPrefix(trimmed, "ref: ") {
				refPath := filepath.Join("/share/aerial/.git", strings.TrimPrefix(trimmed, "ref: "))
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

func NewGitHubPoller(repo, token string) *GitHubPoller {
	return &GitHubPoller{
		repo:        repo,
		token:       token,
		jobsETagMap: make(map[int64]string),
		cachedJobs:  make(map[int64][]GitHubJob),
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

func (p *GitHubPoller) Start(ctx context.Context) {
	if p.repo == "" {
		return
	}
	go func() {
		// Initial immediate poll
		p.pollOnce(ctx)

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				hasActive := p.pollOnce(ctx)
				if hasActive {
					ticker.Reset(15 * time.Second)
				} else {
					ticker.Reset(45 * time.Second)
				}
			}
		}
	}()
}

func (p *GitHubPoller) pollOnce(ctx context.Context) bool {
	reqURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs?per_page=3&event=push", p.repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	if p.runsETag != "" {
		req.Header.Set("If-None-Match", p.runsETag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.mu.Lock()
		p.lastError = err
		p.mu.Unlock()
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNotModified {
		p.mu.RLock()
		defer p.mu.RUnlock()
		for _, r := range p.cachedRuns {
			if r.Status == "in_progress" || r.Status == "queued" {
				return true
			}
		}
		return false
	}

	if resp.StatusCode == http.StatusOK {
		p.runsETag = resp.Header.Get("ETag")
		var runData GitHubRunsResponse
		if err := json.NewDecoder(resp.Body).Decode(&runData); err == nil {
			// Sanitize commit messages
			for i := range runData.WorkflowRuns {
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
			p.cachedRuns = runData.WorkflowRuns
			p.lastPollTime = time.Now().UTC()
			p.lastError = nil

			// Prune old jobs and ETags not in current active runs
			activeIDs := make(map[int64]bool)
			for _, r := range runData.WorkflowRuns {
				activeIDs[r.ID] = true
			}
			for id := range p.cachedJobs {
				if !activeIDs[id] {
					delete(p.cachedJobs, id)
				}
			}
			for id := range p.jobsETagMap {
				if !activeIDs[id] {
					delete(p.jobsETagMap, id)
				}
			}
			p.mu.Unlock()

			hasActive := false
			for _, r := range runData.WorkflowRuns {
				if r.Status == "in_progress" || r.Status == "queued" || time.Since(r.UpdatedAt) < 10*time.Minute {
					p.fetchJobsForRun(ctx, r.ID)
				}
				if r.Status == "in_progress" || r.Status == "queued" {
					hasActive = true
				}
			}
			return hasActive
		}
	}

	return false
}

func (p *GitHubPoller) fetchJobsForRun(ctx context.Context, runID int64) {
	reqURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs/%d/jobs", p.repo, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	if etag, ok := p.jobsETagMap[runID]; ok && etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNotModified {
		return
	}

	if resp.StatusCode == http.StatusOK {
		p.jobsETagMap[runID] = resp.Header.Get("ETag")
		var jobData GitHubJobsResponse
		if err := json.NewDecoder(resp.Body).Decode(&jobData); err == nil {
			p.mu.Lock()
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

func extractServiceNameFromJobName(jobName string) string {
	lower := strings.ToLower(jobName)
	services := []string{
		"brain",
		"dashboard",
		"proxy",
		"scheduler-mcp",
		"discord-mcp",
		"docker-mcp",
		"github-mcp",
		"ollama",
		"agentsview",
	}
	for _, s := range services {
		if strings.Contains(lower, s) {
			return s
		}
	}
	if strings.Contains(lower, "unit test") || strings.Contains(lower, "test") {
		return "unit-tests"
	}
	return ""
}

func parseMatrixJobChips(jobs []GitHubJob) []MatrixJobChip {
	var chips []MatrixJobChip
	seen := make(map[string]bool)

	for _, j := range jobs {
		svc := extractServiceNameFromJobName(j.Name)
		if svc == "" || seen[svc] {
			continue
		}
		seen[svc] = true

		chipStatus := "pending"
		if j.Status == "in_progress" {
			chipStatus = "active"
		} else if j.Status == "completed" {
			if j.Conclusion == "success" {
				chipStatus = "completed"
			} else if j.Conclusion == "failure" {
				chipStatus = "failed"
			} else {
				chipStatus = "pending"
			}
		}

		var durStr string
		if !j.StartedAt.IsZero() {
			end := j.CompletedAt
			if end.IsZero() {
				end = time.Now().UTC()
			}
			sec := int(end.Sub(j.StartedAt).Seconds())
			if sec > 0 {
				durStr = fmt.Sprintf("%ds", sec)
			}
		}

		chips = append(chips, MatrixJobChip{
			Name:       svc,
			Status:     chipStatus,
			Conclusion: j.Conclusion,
			Duration:   durStr,
		})
	}
	return chips
}

func mergeClusterDeployments(
	rawContainers []DockerContainerJSON,
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	currentCommit string,
) []DeploymentStatus {
	var deployments []DeploymentStatus
	now := time.Now().UTC()

	// 1. Check for Active or Recent Cloud Runs in GitHub Actions (inspect latest run first)
	if len(runs) > 0 {
		latestRun := runs[0]
		shortSHA := latestRun.HeadSHA
		if len(shortSHA) > 7 {
			shortSHA = shortSHA[:7]
		}
		commitMsg := ""
		if latestRun.HeadCommit != nil {
			commitMsg = latestRun.HeadCommit.Message
		}

		runJobs := jobs[latestRun.ID]
		matrixChips := parseMatrixJobChips(runJobs)

		if latestRun.Status == "queued" || latestRun.Status == "in_progress" {
			stage := "building"
			progress := 35
			ciStatus := "active"
			if latestRun.Status == "queued" {
				stage = "queued"
				progress = 15
				ciStatus = "pending"
			}

			// Calculate progress based on matrix chips
			if len(matrixChips) > 0 {
				doneCount := 0
				for _, c := range matrixChips {
					if c.Status == "completed" {
						doneCount++
					}
				}
				progress = 20 + int(float64(doneCount)/float64(len(matrixChips))*30)
			}

			deployments = append(deployments, DeploymentStatus{
				ID:        fmt.Sprintf("gh-run-%d", latestRun.ID),
				Service:   "aerial-stack",
				Commit:    shortSHA,
				CommitMsg: commitMsg,
				Stage:     stage,
				Progress:  progress,
				HTMLURL:   latestRun.HTMLURL,
				Steps: []DeploymentStep{
					{Name: "Commit Trigger", Icon: "📦", Status: "completed"},
					{Name: "CI Build & GHCR", Icon: "⚙️", Status: ciStatus},
					{Name: "Watchtower Pull", Icon: "⬇️", Status: "pending"},
					{Name: "Container Swap", Icon: "🔄", Status: "pending"},
					{Name: "Health Check", Icon: "🩺", Status: "pending"},
				},
				MatrixJobs: matrixChips,
				StartedAt:  latestRun.CreatedAt,
			})
			return deployments
		}

		if latestRun.Conclusion == "failure" && time.Since(latestRun.UpdatedAt) < 30*time.Minute {
			deployments = append(deployments, DeploymentStatus{
				ID:        fmt.Sprintf("gh-run-%d", latestRun.ID),
				Service:   "aerial-stack",
				Commit:    shortSHA,
				CommitMsg: commitMsg,
				Stage:     "failed",
				Progress:  40,
				HTMLURL:   latestRun.HTMLURL,
				Steps: []DeploymentStep{
					{Name: "Commit Trigger", Icon: "📦", Status: "completed"},
					{Name: "CI Build & GHCR", Icon: "⚙️", Status: "failed"},
					{Name: "Watchtower Pull", Icon: "⬇️", Status: "pending"},
					{Name: "Container Swap", Icon: "🔄", Status: "pending"},
					{Name: "Health Check", Icon: "🩺", Status: "pending"},
				},
				MatrixJobs: matrixChips,
				StartedAt:  latestRun.CreatedAt,
			})
			return deployments
		}
	}

	// 2. Check for Local Containers recently deployed/restarted (< 20 mins)
	for _, c := range rawContainers {
		isAerial := false
		var svcName string

		if proj, ok := c.Labels["com.docker.compose.project"]; ok && proj == "aerial" {
			isAerial = true
			svcName = c.Labels["com.docker.compose.service"]
		} else if len(c.Names) > 0 {
			name := strings.TrimPrefix(c.Names[0], "/")
			if strings.HasPrefix(name, "aerial-") {
				isAerial = true
				svcName = strings.TrimPrefix(name, "aerial-")
			}
		}

		if !isAerial || svcName == "" {
			continue
		}

		createdAt := time.Unix(c.Created, 0).UTC()
		uptimeSec := int64(now.Sub(createdAt).Seconds())
		if uptimeSec < 0 {
			uptimeSec = 0
		}

		if uptimeSec < 1200 {
			stage := "live"
			progress := 100
			stepStatus := "completed"
			healthStatus := "completed"

			if c.Health != nil && c.Health.Status == "starting" {
				stage = "swapping"
				progress = 85
				stepStatus = "completed"
				healthStatus = "active"
			} else if c.State != "running" || (c.Health != nil && c.Health.Status == "unhealthy") {
				stage = "degraded"
				progress = 85
				healthStatus = "pending"
			}

			deployments = append(deployments, DeploymentStatus{
				ID:        "dep-" + svcName,
				Service:   svcName,
				Commit:    currentCommit,
				Stage:     stage,
				Progress:  progress,
				StartedAt: createdAt,
				Steps: []DeploymentStep{
					{Name: "Commit Trigger", Icon: "📦", Status: "completed"},
					{Name: "CI Build & GHCR", Icon: "⚙️", Status: "completed"},
					{Name: "Watchtower Pull", Icon: "⬇️", Status: "completed"},
					{Name: "Container Swap", Icon: "🔄", Status: stepStatus},
					{Name: "Health Check", Icon: "🩺", Status: healthStatus},
				},
			})
		}
	}

	return deployments
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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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
		isAerial := false
		var svcName string

		if proj, ok := c.Labels["com.docker.compose.project"]; ok && proj == "aerial" {
			isAerial = true
			svcName = c.Labels["com.docker.compose.service"]
		} else if len(c.Names) > 0 {
			name := strings.TrimPrefix(c.Names[0], "/")
			if strings.HasPrefix(name, "aerial-") {
				isAerial = true
				svcName = strings.TrimPrefix(name, "aerial-")
			}
		}

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

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	now := time.Now().UTC()
	services, rawContainers, err := fetchDockerClusterState(ctx)
	currentCommit := getGitCommit()

	var ghRuns []GitHubRun
	var ghJobs map[int64][]GitHubJob
	if globalGHPoller != nil {
		ghRuns, ghJobs = globalGHPoller.GetSnapshot()
	}

	deployments := mergeClusterDeployments(rawContainers, ghRuns, ghJobs, currentCommit)

	if err != nil || len(services) == 0 {
		uptimeSec := int64(time.Since(startTime).Seconds())
		services = []ServiceStatus{
			{Name: "brain", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "scheduler-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "discord-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "docker-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "github-mcp", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "ollama", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "agentsview", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "watchtower", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "autoheal", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "proxy", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
			{Name: "dashboard", Status: "healthy", UptimeSeconds: uptimeSec, LastCheckTime: now},
		}
	}

	clusterStatus := "healthy"
	for _, s := range services {
		if s.Status == "unhealthy" {
			clusterStatus = "degraded"
			break
		}
	}

	resp := ClusterResponse{
		SystemTime:    now,
		ClusterStatus: clusterStatus,
		Services:      services,
		Deployments:   deployments,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	_ = json.NewEncoder(w).Encode(resp)
}

func factsHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		inQuery := r.URL.Query()
		targetURL, err := url.Parse(brainBaseURL + "/facts")
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		outQuery := targetURL.Query()
		limit := 50
		if lStr := inQuery.Get("limit"); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil && n > 0 && n <= 100 {
				limit = n
			}
		}
		outQuery.Set("limit", strconv.Itoa(limit))

		offset := 0
		if oStr := inQuery.Get("offset"); oStr != "" {
			if n, err := strconv.Atoi(oStr); err == nil && n >= 0 {
				offset = n
			}
		}
		outQuery.Set("offset", strconv.Itoa(offset))

		if cat := strings.TrimSpace(inQuery.Get("category")); cat != "" {
			outQuery.Set("category", cat)
		}
		if search := strings.TrimSpace(inQuery.Get("q")); search != "" {
			if runes := []rune(search); len(runes) > 64 {
				search = string(runes[:64])
			}
			outQuery.Set("q", search)
		}
		targetURL.RawQuery = outQuery.Encode()

		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			http.Error(w, `{"error":"Failed to create upstream request"}`, http.StatusInternalServerError)
			return
		}
		req.Header.Set("Accept", "application/json")

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		resp, err := brainHTTPClient.Do(req)
		if err != nil {
			log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL.String(), err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts:  []FactItem{},
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Status: "degraded",
				Error:  "Brain service unreachable. Retrying...",
			})
			return
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			log.Printf("[Dashboard] Upstream brain returned status %d", resp.StatusCode)
			w.WriteHeader(resp.StatusCode)
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts:  []FactItem{},
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Status: "error",
				Error:  "Upstream brain error occurred",
			})
			return
		}

		var data FactsAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}
}

func schedulesHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		targetURL, err := url.Parse(brainBaseURL + "/schedules")
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			http.Error(w, `{"error":"Failed to create upstream request"}`, http.StatusInternalServerError)
			return
		}
		req.Header.Set("Accept", "application/json")

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		resp, err := brainHTTPClient.Do(req)
		if err != nil {
			log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL.String(), err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
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
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			log.Printf("[Dashboard] Upstream brain returned status %d", resp.StatusCode)
			w.WriteHeader(resp.StatusCode)
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
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

		var data SchedulesAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}
}

func scheduleRunsHandler(brainBaseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			http.Error(w, `{"error":"Method Not Allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		inQuery := r.URL.Query()
		targetURL, err := url.Parse(brainBaseURL + "/schedules/runs")
		if err != nil {
			http.Error(w, `{"error":"Invalid upstream URL configuration"}`, http.StatusInternalServerError)
			return
		}

		outQuery := targetURL.Query()
		limit := 50
		if lStr := inQuery.Get("limit"); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil && n > 0 && n <= 100 {
				limit = n
			}
		}
		outQuery.Set("limit", strconv.Itoa(limit))

		offset := 0
		if oStr := inQuery.Get("offset"); oStr != "" {
			if n, err := strconv.Atoi(oStr); err == nil && n >= 0 {
				offset = n
			}
		}
		outQuery.Set("offset", strconv.Itoa(offset))

		if schedID := strings.TrimSpace(inQuery.Get("schedule_id")); schedID != "" {
			outQuery.Set("schedule_id", schedID)
		}
		if status := strings.TrimSpace(inQuery.Get("status")); status != "" {
			outQuery.Set("status", status)
		}
		targetURL.RawQuery = outQuery.Encode()

		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
		if err != nil {
			http.Error(w, `{"error":"Failed to create upstream request"}`, http.StatusInternalServerError)
			return
		}
		req.Header.Set("Accept", "application/json")

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		resp, err := brainHTTPClient.Do(req)
		if err != nil {
			log.Printf("[Dashboard] Upstream brain request failed (%s): %v", targetURL.String(), err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "degraded",
				Error:  "Brain service unreachable. Retrying...",
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Runs:   []ScheduleRun{},
			})
			return
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			log.Printf("[Dashboard] Upstream brain returned status %d", resp.StatusCode)
			w.WriteHeader(resp.StatusCode)
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "error",
				Error:  "Upstream brain error occurred",
				Total:  0,
				Limit:  limit,
				Offset: offset,
				Runs:   []ScheduleRun{},
			})
			return
		}

		var data ScheduleRunsAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		next.ServeHTTP(w, r)
	})
}

func main() {
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		log.Fatalf("failed to create static sub filesystem: %v", err)
	}

	brainURL := os.Getenv("BRAIN_URL")
	if brainURL == "" {
		brainURL = "http://brain:8080"
	}

	ghRepo := os.Getenv("GITHUB_REPO")
	if ghRepo == "" {
		ghRepo = "azylman/aerial"
	}
	ghToken := os.Getenv("GITHUB_PAT")
	if ghToken == "" {
		ghToken = os.Getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
	}
	globalGHPoller = NewGitHubPoller(ghRepo, ghToken)
	globalGHPoller.Start(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/dashboard/health", healthHandler)
	mux.HandleFunc("/api/status", statusHandler)
	mux.HandleFunc("/dashboard/api/status", statusHandler)
	mux.HandleFunc("/api/facts", factsHandler(brainURL))
	mux.HandleFunc("/dashboard/api/facts", factsHandler(brainURL))
	mux.HandleFunc("/api/schedules", schedulesHandler(brainURL))
	mux.HandleFunc("/dashboard/api/schedules", schedulesHandler(brainURL))
	mux.HandleFunc("/api/schedules/runs", scheduleRunsHandler(brainURL))
	mux.HandleFunc("/dashboard/api/schedules/runs", scheduleRunsHandler(brainURL))


	fileServer := http.FileServer(http.FS(staticFS))
	staticHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		fileServer.ServeHTTP(w, r)
	}

	mux.HandleFunc("/", staticHandler)
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard", http.HandlerFunc(staticHandler)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("aerial-dashboard server starting on :%s (upstream brain=%s)", port, brainURL)
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: securityHeadersMiddleware(mux),
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
