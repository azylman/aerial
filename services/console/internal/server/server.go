package server

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
	"github.com/azylman/aerial/services/console/internal/config"
)

type (
	ServiceStatus               = config.ServiceStatus
	MatrixJobChip               = config.MatrixJobChip
	DeploymentStep              = config.DeploymentStep
	DeploymentStatus            = config.DeploymentStatus
	ActiveTaskStatus            = config.ActiveTaskStatus
	BrainTasksAPIResponse       = config.BrainTasksAPIResponse
	ClusterResponse             = config.ClusterResponse
	ConsoleConfig               = config.ConsoleConfig
	QuickLaunchLink             = config.QuickLaunchLink
	DockerContainerJSON         = config.DockerContainerJSON
	DockerContainerHealth       = config.DockerContainerHealth
	GitHubRun                   = config.GitHubRun
	GitHubJob                   = config.GitHubJob
	GitHubRunsResponse          = config.GitHubRunsResponse
	GitHubJobsResponse          = config.GitHubJobsResponse
	HeadCommit                  = config.HeadCommit
	FactItem                    = config.FactItem
	FactsAPIResponse            = config.FactsAPIResponse
	ScheduleSummaryMetrics      = config.ScheduleSummaryMetrics
	CronSchedule                = config.CronSchedule
	OneShotSchedule             = config.OneShotSchedule
	SchedulesAPIResponse        = config.SchedulesAPIResponse
	ScheduleRun                 = config.ScheduleRun
	ScheduleRunsAPIResponse     = config.ScheduleRunsAPIResponse
	VanadisChunkDTO             = config.VanadisChunkDTO
	VanadisBuildDTO             = config.VanadisBuildDTO
	BuildsAPIResponse           = config.BuildsAPIResponse
	BuildDetailAPIResponse      = config.BuildDetailAPIResponse
	DocketTaskSummaryDTO        = config.DocketTaskSummaryDTO
	DocketScoredTaskDTO         = config.DocketScoredTaskDTO
	DocketTaskCommentDTO        = config.DocketTaskCommentDTO
	DocketTaskDTO               = config.DocketTaskDTO
	DocketTasksAPIResponse      = config.DocketTasksAPIResponse
	DocketSearchAPIResponse     = config.DocketSearchAPIResponse
	DocketTaskDetailAPIResponse = config.DocketTaskDetailAPIResponse
	AgentBrainTarget            = config.AgentBrainTarget
)

var (
	loadQuickLaunchLinks               = config.LoadQuickLaunchLinks
	NormalizeAgent                     = config.NormalizeAgent
	SortConfiguredAgents               = config.SortConfiguredAgents
	StampTaskAgent                     = config.StampTaskAgent
	SiblingBrainTargets                = config.SiblingBrainTargets
	DefaultAgentFromTasks              = config.DefaultAgentFromTasks
	FilterActiveTasksByAgent           = config.FilterActiveTasksByAgent
	MergeClusterDeployments            = config.MergeClusterDeployments
	CalculateClusterStatus             = config.CalculateClusterStatus
	CalculateDeployStatus              = config.CalculateDeployStatus
	IsDeployOngoing                    = config.IsDeployOngoing
	synthesizeContainersFromServices   = config.SynthesizeContainersFromServices
	GetMimeType                        = config.GetMimeType
	MatchesETag                        = config.MatchesETag
	parseMergedPullHeadRef             = config.ParseMergedPullHeadRef
	loadConsoleRepos                   = config.LoadConsoleRepos
	sensitiveKeys                      = config.SensitiveKeys
	imageCommitSHA                     = config.ImageCommitSHA
	BuildFactsUpstreamURL              = config.BuildFactsUpstreamURL
	BuildSchedulesUpstreamURL          = config.BuildSchedulesUpstreamURL
	BuildScheduleRunsUpstreamURL       = config.BuildScheduleRunsUpstreamURL
	BuildVanadisBuildsUpstreamURL      = config.BuildVanadisBuildsUpstreamURL
	BuildVanadisBuildDetailUpstreamURL = config.BuildVanadisBuildDetailUpstreamURL
	BuildDocketTasksUpstreamURL        = config.BuildDocketTasksUpstreamURL
	BuildDocketSearchUpstreamURL       = config.BuildDocketSearchUpstreamURL
	BuildDocketTaskDetailUpstreamURL   = config.BuildDocketTaskDetailUpstreamURL
	FilterFactsByAgent                 = config.FilterFactsByAgent
	FilterCronsByAgent                 = config.FilterCronsByAgent
	FilterOneShotsByAgent              = config.FilterOneShotsByAgent
	FilterScheduleRunsByAgent          = config.FilterScheduleRunsByAgent
	FilterBuildsByAgent                = config.FilterBuildsByAgent
	errInvalidDocketRequest            = config.ErrInvalidDocketRequest
)

const (
	healthStarting        = "starting"
	healthHealthy         = "healthy"
	healthUnhealthy       = "unhealthy"
	containerStateRunning = "running"
	serviceOllama         = "ollama"
	serviceAgentsview     = "agentsview"
	statusInProgress      = "in_progress"
	statusQueued          = "queued"
	statusDegraded        = "degraded"
	statusError           = "error"
)

func normalizeSHA(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func shortenSHA(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

var sighupNotifyFn = func(c chan<- os.Signal) { signal.Notify(c, syscall.SIGHUP) }

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

func resolveClusterState(ctx context.Context, kubeAPIServer, gitCommit string, now time.Time) ([]ServiceStatus, []DockerContainerJSON, string) {
	var services []ServiceStatus
	if kubeAPIServer != "" {
		k8sServices, err := fetchK8sClusterState(ctx, kubeAPIServer)
		if err != nil {
			slog.WarnContext(ctx, "[Console] failed to fetch K8s cluster state", "kube_api_server", kubeAPIServer, "error", err)
		} else {
			services = k8sServices
		}
	}
	currentCommit := gitCommit
	if currentCommit == "" {
		currentCommit = "latest"
	}
	return services, synthesizeContainersFromServices(services, now), currentCommit
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

// fetchAllAgentsActiveTasks queries the configured (default) eri and discovers agents from
// haro and vanadis concurrently, then fans out to every sibling stack's eri (derived by naming
// convention) and merges the rows, each stamped with the agent that was queried. A failing eri
// only drops that agent's rows.
func fetchAllAgentsActiveTasks(ctx context.Context, brainURL, haroURL, vanadisURL string, now time.Time) ([]ActiveTaskStatus, []string) {
	var defaultTasks []ActiveTaskStatus
	var upstreamAgents []string
	var wg sync.WaitGroup
	wg.Go(func() {
		tasks, err := fetchActiveTasksFromBrain(ctx, brainURL)
		if err != nil {
			slog.WarnContext(ctx, "[Console] failed to fetch active tasks from brain", "brain_url", brainURL, "error", err)
			tasks = nil
		}
		defaultTasks = tasks
	})
	wg.Go(func() {
		upstreamAgents = discoverConfiguredAgents(ctx, nil, haroURL, vanadisURL, now)
	})
	wg.Wait()

	defaultAgent := DefaultAgentFromTasks(defaultTasks)
	allTasks := StampTaskAgent(defaultTasks, defaultAgent)
	allTasks = append(allTasks, fetchActiveTasksFromAgents(ctx, SiblingBrainTargets(brainURL, defaultAgent, upstreamAgents))...)

	discovered := append([]string{}, upstreamAgents...)
	for _, t := range allTasks {
		discovered = append(discovered, t.Agent)
	}
	return allTasks, SortConfiguredAgents(discovered)
}

// fetchActiveTasksFromAgents fetches /tasks from each target concurrently and merges the rows
// in target order, stamping rows without an agent with the queried agent. Unreachable targets
// are logged and skipped.
func fetchActiveTasksFromAgents(ctx context.Context, targets []AgentBrainTarget) []ActiveTaskStatus {
	results := make([][]ActiveTaskStatus, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Go(func() {
			tasks, err := fetchActiveTasksFromBrain(ctx, target.URL)
			if err != nil {
				slog.WarnContext(ctx, "[Console] failed to fetch active tasks from agent eri", "agent", target.Agent, "brain_url", target.URL, "error", err)
				return
			}
			results[i] = StampTaskAgent(tasks, target.Agent)
		})
	}
	wg.Wait()
	out := []ActiveTaskStatus{}
	for _, r := range results {
		out = append(out, r...)
	}
	return out
}

func statusHandler(brainURL, configPath, gitCommit string, optionalAddrs ...string) http.HandlerFunc {
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

		services, rawContainers, currentCommit := resolveClusterState(ctx, kubeAPIServer, gitCommit, now)
		deployments := MergeClusterDeployments(ghRuns, ghJobs, rawContainers, currentCommit, time.Time{})
		if deployments == nil {
			deployments = []DeploymentStatus{}
		}

		agentParam := strings.TrimSpace(r.URL.Query().Get("agent"))
		allTasks, configuredAgents := fetchAllAgentsActiveTasks(ctx, brainURL, haroURL, vanadisURL, now)

		activeTasks := allTasks
		if agentParam != "" && !strings.EqualFold(agentParam, "all") {
			activeTasks = FilterActiveTasksByAgent(allTasks, agentParam)
		}

		quickLinks := loadQuickLaunchLinks(configPath)

		if len(services) == 0 {
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
	mux.HandleFunc("/api/status", statusHandler(cfg.BrainURL, cfg.ConfigPath, cfg.GitCommit, cfg.KubeAPIServer, haroURL, vanadisURL))
	mux.HandleFunc("/console/api/status", statusHandler(cfg.BrainURL, cfg.ConfigPath, cfg.GitCommit, cfg.KubeAPIServer, haroURL, vanadisURL))
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
	if _, err := os.Stat(staticDir); err != nil {
		for _, alt := range []string{"../../static", "../static"} {
			if _, err := os.Stat(alt); err == nil {
				staticDir = alt
				break
			}
		}
	}

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
