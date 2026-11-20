// Package main implements the Aerial console: the operator status HUD and telemetry viewer.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/tracing"
)

func resolveStaticDir(candidates ...string) string {
	if len(candidates) == 0 {
		candidates = []string{"/local/static", "static"}
	}
	for _, dir := range candidates {
		if _, err := os.Stat(filepath.Join(dir, "index.html")); err == nil {
			return dir
		}
	}
	return "static"
}

var startTime = time.Now().UTC()

type ServiceStatus struct {
	Name          string    `json:"name"`
	Status        string    `json:"status"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	LastCheckTime time.Time `json:"last_check_time"`
}

type MatrixJobChip struct {
	Name       string `json:"name"`                 // e.g. "brain", "console", "proxy"
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
	Agent       string    `json:"agent,omitempty"`
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
		Agent       string    `json:"agent,omitempty"`
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
	ConfiguredAgents []string              `json:"configured_agents,omitempty"`
}

// Singleton tuned HTTP client for upstream brain communication
var brainHTTPClient = tracing.Client(&http.Client{
	Timeout: 5 * time.Second,
	Transport: k8sauth.NewServiceAccountTransport(&http.Transport{
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
	}, ""),
})

// Unix socket client for Docker daemon status inspection
var dockerSocketClient = &http.Client{
	Timeout: 2 * time.Second,
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 1 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	},
}

func drainAndClose(body io.ReadCloser, name string) {
	if body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(body, 32*1024)); err != nil {
		log.Printf("[Console] Warning draining %s: %v", name, err)
	}
	if err := body.Close(); err != nil {
		log.Printf("[Console] Warning closing %s: %v", name, err)
	}
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(data)
	if err != nil {
		if r != nil {
			slog.ErrorContext(r.Context(), "[Console:HTTP] failed to marshal JSON response", "error", err)
		} else {
			slog.Error("[Console:HTTP] failed to marshal JSON response", "error", err)
		}
		http.Error(w, `{"error":"Internal error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Vary", "Accept-Encoding")
	if r != nil && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(b); err == nil && gz.Close() == nil {
			w.Header().Set("Content-Encoding", "gzip")
			b = buf.Bytes()
		}
	}
	w.WriteHeader(status)
	httpkit.WriteResponse(w, r, "console", b)
}

func fetchDockerClusterState(ctx context.Context) ([]ServiceStatus, []DockerContainerJSON, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/containers/json", http.NoBody)
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
		uptimeSec := max(int64(now.Sub(createdAt).Seconds()), 0)

		status := healthHealthy
		statusLower := strings.ToLower(c.Status)
		switch {
		case strings.Contains(statusLower, "(unhealthy)"):
			status = healthUnhealthy
		case strings.Contains(statusLower, "starting") || strings.Contains(statusLower, "(health: starting)"):
			status = healthStarting
		case c.State != containerStateRunning:
			status = healthUnhealthy
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

type cachedDiscoveredAgents struct {
	mu        sync.RWMutex
	agents    []string
	fetchedAt time.Time
	ttl       time.Duration
}

func (c *cachedDiscoveredAgents) get(now time.Time) ([]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.fetchedAt.IsZero() && now.Sub(c.fetchedAt) < c.ttl && len(c.agents) > 0 {
		out := make([]string, len(c.agents))
		copy(out, c.agents)
		return out, true
	}
	return nil, false
}

func (c *cachedDiscoveredAgents) set(agents []string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents = make([]string, len(agents))
	copy(c.agents, agents)
	c.fetchedAt = now
}

var discoveredAgentsCache = &cachedDiscoveredAgents{ttl: 30 * time.Second}

var configuredAgentsDiscoveryFn func(ctx context.Context) ([]string, error)

func fetchAgentsFromEndpoint(ctx context.Context, client *http.Client, endpointURL string, extract func(body io.Reader) []string) []string {
	if strings.TrimSpace(endpointURL) == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL, http.NoBody)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "agent discovery response body")
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	return extract(resp.Body)
}

func fetchAgentsFromSchedules(ctx context.Context, haroURL string) []string {
	return fetchAgentsFromEndpoint(ctx, brainHTTPClient, strings.TrimRight(haroURL, "/")+"/schedules", func(r io.Reader) []string {
		var data SchedulesAPIResponse
		if err := json.NewDecoder(r).Decode(&data); err != nil {
			return nil
		}
		var agents []string
		for _, c := range data.Crons {
			if c.Agent != "" {
				agents = append(agents, c.Agent)
			}
		}
		for _, o := range data.OneShots {
			if o.Agent != "" {
				agents = append(agents, o.Agent)
			}
		}
		return agents
	})
}

func fetchAgentsFromBuilds(ctx context.Context, vanadisURL string) []string {
	return fetchAgentsFromEndpoint(ctx, vanadisHTTPClient, strings.TrimRight(vanadisURL, "/")+"/builds?limit=50", func(r io.Reader) []string {
		var data BuildsAPIResponse
		if err := json.NewDecoder(r).Decode(&data); err != nil {
			return nil
		}
		var agents []string
		for _, b := range data.Builds {
			if b.Agent != "" {
				agents = append(agents, b.Agent)
			}
		}
		return agents
	})
}

func fetchActiveTasksFromBrain(ctx context.Context, brainURL string, agent ...string) ([]ActiveTaskStatus, error) {
	if brainURL == "" {
		return []ActiveTaskStatus{}, nil
	}

	reqURL := strings.TrimRight(brainURL, "/") + "/tasks"
	if len(agent) > 0 && strings.TrimSpace(agent[0]) != "" && !strings.EqualFold(agent[0], "all") {
		reqURL += "?agent=" + url.QueryEscape(NormalizeAgent(agent[0]))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
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
			Agent:       t.Agent,
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(gitsyncURL, "/")+"/status", http.NoBody)
	if err != nil {
		fallback.Error = err.Error()
		return fallback
	}

	client := tracing.Client(&http.Client{Timeout: 2 * time.Second})
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

// synthesizeContainersFromServices converts ServiceStatus entries into DockerContainerJSON
// records without stamping aerial.commit_sha so resolveContainerCommit can match recon.TargetServices
// before falling back to currentCommit.
func synthesizeContainersFromServices(services []ServiceStatus, now time.Time) []DockerContainerJSON {
	out := make([]DockerContainerJSON, 0, len(services))
	for _, svc := range services {
		state := containerStateRunning
		if svc.Status == healthUnhealthy {
			state = "exited"
		}
		out = append(out, DockerContainerJSON{
			Names:   []string{"/" + svc.Name},
			Created: now.Unix() - svc.UptimeSeconds,
			State:   state,
			Status:  svc.Status,
			Labels:  map[string]string{"aerial.service": svc.Name},
			Health:  &DockerContainerHealth{Status: svc.Status},
		})
	}
	return out
}

func resolveClusterState(ctx context.Context, kubeAPIServer, gitCommit string, gitSync GitSyncStatusResponse, now time.Time) ([]ServiceStatus, []DockerContainerJSON, string, error) {
	var services []ServiceStatus
	var rawContainers []DockerContainerJSON
	var err error
	if kubeAPIServer != "" {
		k8sServices, k8sErr := fetchK8sClusterState(ctx, kubeAPIServer)
		if k8sErr == nil && len(k8sServices) > 0 {
			services = k8sServices
		} else if k8sErr != nil {
			slog.WarnContext(ctx, "[Console] failed to fetch K8s cluster state, falling back to Docker", "kube_api_server", kubeAPIServer, "error", k8sErr)
		}
	}
	if len(services) == 0 {
		services, rawContainers, err = fetchDockerClusterState(ctx)
	}
	currentCommit := gitCommit
	if currentCommit == "" {
		currentCommit = "latest"
	}
	if currentCommit == "latest" && gitSync.Repos != nil {
		if r, ok := gitSync.Repos["/share/aerial"]; ok && r.DiskCommit != "" {
			currentCommit = shortenSHA(r.DiskCommit)
		}
	}
	if len(rawContainers) == 0 && len(services) > 0 {
		rawContainers = synthesizeContainersFromServices(services, now)
	}
	return services, rawContainers, currentCommit, err
}

func fetchUpstreamDiscoveredAgentsWithCache(ctx context.Context, cache *cachedDiscoveredAgents, haroURL, vanadisURL string, now time.Time) []string {
	if cache != nil {
		if cached, ok := cache.get(now); ok {
			return cached
		}
	}

	var mu sync.Mutex
	var upstreamAgents []string
	var wg sync.WaitGroup

	if haroURL != "" {
		wg.Go(func() {
			ags := fetchAgentsFromSchedules(ctx, haroURL)
			if len(ags) > 0 {
				mu.Lock()
				upstreamAgents = append(upstreamAgents, ags...)
				mu.Unlock()
			}
		})
	}

	if vanadisURL != "" {
		wg.Go(func() {
			ags := fetchAgentsFromBuilds(ctx, vanadisURL)
			if len(ags) > 0 {
				mu.Lock()
				upstreamAgents = append(upstreamAgents, ags...)
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	if cache != nil {
		cache.set(upstreamAgents, now)
	}
	return upstreamAgents
}

func fetchUpstreamDiscoveredAgents(ctx context.Context, haroURL, vanadisURL string, now time.Time) []string {
	return fetchUpstreamDiscoveredAgentsWithCache(ctx, discoveredAgentsCache, haroURL, vanadisURL, now)
}

func discoverConfiguredAgents(ctx context.Context, allTasks []ActiveTaskStatus, haroURL, vanadisURL string, now time.Time) []string {
	var discovered []string
	if configuredAgentsDiscoveryFn != nil {
		if ags, err := configuredAgentsDiscoveryFn(ctx); err == nil && len(ags) > 0 {
			discovered = append(discovered, ags...)
		}
	}
	for _, t := range allTasks {
		if t.Agent != "" {
			discovered = append(discovered, t.Agent)
		}
	}
	if haroURL != "" || vanadisURL != "" {
		discovered = append(discovered, fetchUpstreamDiscoveredAgents(ctx, haroURL, vanadisURL, now)...)
	}
	return SortConfiguredAgents(discovered)
}

func statusHandler(brainURL, gitsyncURL, configPath, gitCommit string, optionalAddrs ...string) http.HandlerFunc {
	kubeAPIServer := ""
	if len(optionalAddrs) > 0 {
		kubeAPIServer = strings.TrimSpace(optionalAddrs[0])
	}
	haroURL := ""
	if len(optionalAddrs) > 1 {
		haroURL = strings.TrimSpace(optionalAddrs[1])
	}
	vanadisURL := ""
	if len(optionalAddrs) > 2 {
		vanadisURL = strings.TrimSpace(optionalAddrs[2])
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()

		now := time.Now().UTC()
		var ghRuns []GitHubRun
		var ghJobs map[int64][]GitHubJob
		if globalGHPoller != nil {
			ghRuns, ghJobs = globalGHPoller.GetSnapshot()
		}

		gitSync := fetchGitSyncStatus(ctx, gitsyncURL)
		services, rawContainers, currentCommit, err := resolveClusterState(ctx, kubeAPIServer, gitCommit, gitSync, now)
		deployments := mergeClusterDeploymentsWithHangar(rawContainers, ghRuns, ghJobs, gitSync, currentCommit)
		if deployments == nil {
			deployments = []DeploymentStatus{}
		}

		agentParam := strings.TrimSpace(r.URL.Query().Get("agent"))
		allTasks, taskErr := fetchActiveTasksFromBrain(ctx, brainURL)
		if taskErr != nil {
			slog.WarnContext(ctx, "[Console] failed to fetch active tasks from brain", "brain_url", brainURL, "error", taskErr)
			allTasks = []ActiveTaskStatus{}
		}
		if allTasks == nil {
			allTasks = []ActiveTaskStatus{}
		}

		configuredAgents := discoverConfiguredAgents(ctx, allTasks, haroURL, vanadisURL, now)

		activeTasks := allTasks
		if agentParam != "" && !strings.EqualFold(agentParam, "all") {
			activeTasks = FilterActiveTasksByAgent(allTasks, agentParam)
		}

		quickLinks := loadQuickLaunchLinks(configPath)

		if err != nil || len(services) == 0 {
			uptimeSec := int64(time.Since(startTime).Seconds())
			services = []ServiceStatus{
				{Name: "brain", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "haro", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "coven", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "discord-mcp", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "kernel-mcp", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "github-mcp", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "infisical-mcp", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "synapse", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: serviceOllama, Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: serviceAgentsview, Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "autoheal", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "proxy", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
				{Name: "console", Status: healthHealthy, UptimeSeconds: uptimeSec, LastCheckTime: now},
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
			ConfiguredAgents: configuredAgents,
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		writeJSON(w, r, http.StatusOK, resp)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	httpkit.WriteResponse(w, r, "console", []byte("OK"))
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		next.ServeHTTP(w, r)
	})
}

// SetupConsoleMux configures the HTTP router and route handlers.
func SetupConsoleMux(cfg ConsoleConfig, assetReg *AssetRegistry) http.Handler {
	mux := http.NewServeMux()
	memoryURL := strings.TrimSpace(cfg.MemoryURL)
	if memoryURL == "" {
		memoryURL = cfg.BrainURL
	}
	haroURL := strings.TrimSpace(cfg.HaroURL)
	if haroURL == "" {
		haroURL = cfg.BrainURL
	}
	vanadisURL := strings.TrimSpace(cfg.VanadisURL)
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/console/health", healthHandler)
	mux.HandleFunc("/api/status", statusHandler(cfg.BrainURL, cfg.GitSyncURL, cfg.ConfigPath, cfg.GitCommit, cfg.KubeAPIServer, haroURL, vanadisURL))
	mux.HandleFunc("/console/api/status", statusHandler(cfg.BrainURL, cfg.GitSyncURL, cfg.ConfigPath, cfg.GitCommit, cfg.KubeAPIServer, haroURL, vanadisURL))
	mux.HandleFunc("/api/facts", factsHandler(memoryURL))
	mux.HandleFunc("/console/api/facts", factsHandler(memoryURL))
	mux.HandleFunc("/api/schedules", schedulesHandler(haroURL))
	mux.HandleFunc("/console/api/schedules", schedulesHandler(haroURL))
	mux.HandleFunc("/api/schedules/runs", scheduleRunsHandler(haroURL))
	mux.HandleFunc("/console/api/schedules/runs", scheduleRunsHandler(haroURL))
	mux.HandleFunc("/api/builds", buildsHandler(cfg.VanadisURL))
	mux.HandleFunc("/console/api/builds", buildsHandler(cfg.VanadisURL))
	mux.HandleFunc("/api/builds/", buildDetailHandler(cfg.VanadisURL))
	mux.HandleFunc("/console/api/builds/", buildDetailHandler(cfg.VanadisURL))
	mux.HandleFunc("/api/docket/tasks", docketTasksHandler(cfg.DocketURL))
	mux.HandleFunc("/console/api/docket/tasks", docketTasksHandler(cfg.DocketURL))
	mux.HandleFunc("/api/docket/tasks/", docketTaskDetailHandler(cfg.DocketURL))
	mux.HandleFunc("/console/api/docket/tasks/", docketTaskDetailHandler(cfg.DocketURL))
	mux.HandleFunc("/api/docket/search", docketSearchHandler(cfg.DocketURL))
	mux.HandleFunc("/console/api/docket/search", docketSearchHandler(cfg.DocketURL))
	coverageFlush := httpkit.HandleCoverageFlush("console")
	mux.HandleFunc("/debug/coverage/flush", coverageFlush)
	mux.HandleFunc("/console/debug/coverage/flush", coverageFlush)

	if assetReg != nil {
		mux.HandleFunc("/", assetReg.ServeHTTP)
		mux.Handle("/console/", http.StripPrefix("/console", http.HandlerFunc(assetReg.ServeHTTP)))
		for _, legacy := range []string{"/next", "/next/", "/console/next", "/console/next/"} {
			mux.HandleFunc(legacy, redirectLegacyNext)
		}
	}

	return securityHeadersMiddleware(tracing.Middleware("console", mux))
}

// redirectLegacyNext permanently redirects bookmarks from the retired side-by-side React mount
// (/next/*, /console/next/*) to the same route at the console root, preserving the query string:
// /console/next/memory?x=1 -> /console/memory?x=1, /next -> /.
func redirectLegacyNext(w http.ResponseWriter, r *http.Request) {
	prefix := ""
	if strings.HasPrefix(r.URL.Path, "/console/") {
		prefix = "/console"
	}
	remainder := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, prefix), "/next")
	dest := url.URL{Path: prefix + "/" + strings.TrimPrefix(remainder, "/"), RawQuery: r.URL.RawQuery}
	http.Redirect(w, r, dest.String(), http.StatusMovedPermanently)
}

// RunConsoleServer starts the GitHub poller, creates the asset registry, and listens on the configured port.
func RunConsoleServer(ctx context.Context, cfg ConsoleConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	staticDir := resolveStaticDir()

	assetReg, err := NewAssetRegistry(os.DirFS(staticDir))
	if err != nil {
		return fmt.Errorf("failed to initialize asset registry: %w", err)
	}

	sighupCh := make(chan os.Signal, 1)
	sighupNotifyFn(sighupCh)
	defer signal.Stop(sighupCh)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sighupCh:
				log.Println("Received SIGHUP, reloading console static assets...")
				if err := assetReg.Reload(os.DirFS(staticDir)); err != nil {
					log.Printf("failed to reload console static assets: %v", err)
				} else {
					log.Println("Console static assets reloaded successfully")
				}
			}
		}
	}()

	repos := cfg.GHRepos
	if len(repos) == 0 && cfg.GHRepo != "" {
		repos = []string{cfg.GHRepo}
	}
	if len(repos) > 0 {
		globalGHPoller = NewMultiGitHubPoller(repos, cfg.GHToken)
		globalGHPoller.configPath = cfg.ConfigPath
		if cfg.APIBaseURL != "" {
			globalGHPoller.apiBaseURL = cfg.APIBaseURL
		}
		globalGHPoller.Start(ctx)
	}

	handler := SetupConsoleMux(cfg, assetReg)

	log.Printf("aerial-console server starting on :%s (upstream brain=%s)", cfg.Port, cfg.BrainURL)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down aerial-console server gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return fmt.Errorf("server fatal error: %w", err)
	}
}

var (
	defaultConfigPath = DefaultConfigPath
	loadConfigFn      = LoadConfig
	runConsoleFn      = RunConsoleServer
	exitFn            = log.Fatalf
	sighupNotifyFn    = func(c chan<- os.Signal) { signal.Notify(c, syscall.SIGHUP) }
)

func main() {
	slog.SetDefault(slog.New(tracing.SlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))))

	resolvedPath := ResolveConfigPath(defaultConfigPath)
	cfg, err := loadConfigFn(resolvedPath)
	if err != nil {
		exitFn("failed to load config: %v", err)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, flushTracing := tracing.Init(ctx, "console", tracing.DefaultConfigPath)
	err = runConsoleFn(ctx, cfg)
	flushTracing()
	if err != nil {
		exitFn("console failed: %v", err)
	}
}
