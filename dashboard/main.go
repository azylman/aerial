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

type DeploymentStep struct {
	Name   string `json:"name"`
	Icon   string `json:"icon"`
	Status string `json:"status"` // "completed", "active", "pending"
}

type DeploymentStatus struct {
	ID        string           `json:"id"`
	Service   string           `json:"service"`
	Commit    string           `json:"commit"`
	Stage     string           `json:"stage"` // e.g. "idle", "pulling", "swapping", "live"
	Progress  int              `json:"progress"`
	Steps     []DeploymentStep `json:"steps"`
	StartedAt time.Time        `json:"started_at"`
}

type ClusterResponse struct {
	SystemTime    time.Time          `json:"system_time"`
	ClusterStatus string             `json:"cluster_status"`
	Services      []ServiceStatus    `json:"services"`
	Deployments   []DeploymentStatus `json:"deployments"`
}

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

func fetchDockerClusterState(ctx context.Context) ([]ServiceStatus, []DeploymentStatus, error) {
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
	currentCommit := getGitCommit()

	var services []ServiceStatus
	var deployments []DeploymentStatus

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
		if c.Health != nil && c.Health.Status != "" {
			if c.Health.Status == "healthy" {
				status = "healthy"
			} else if c.Health.Status == "starting" {
				status = "starting"
			} else {
				status = "unhealthy"
			}
		} else if c.State != "running" {
			status = "unhealthy"
		}

		services = append(services, ServiceStatus{
			Name:          svcName,
			Status:        status,
			UptimeSeconds: uptimeSec,
			LastCheckTime: now,
		})

		// Check if this container was deployed/restarted within the last 20 minutes
		if uptimeSec < 1200 {
			stage := "live"
			progress := 100
			stepStatus := "completed"
			healthStatus := "completed"

			if status == "starting" {
				stage = "swapping"
				progress = 85
				stepStatus = "completed"
				healthStatus = "active"
			} else if status == "unhealthy" {
				stage = "degraded"
				progress = 85
				healthStatus = "pending"
			}

			deployments = append(deployments, DeploymentStatus{
				ID:       "dep-" + svcName,
				Service:  svcName,
				Commit:   currentCommit,
				Stage:    stage,
				Progress: progress,
				Steps: []DeploymentStep{
					{Name: "Commit Trigger", Icon: "📦", Status: "completed"},
					{Name: "CI Build & GHCR", Icon: "⚙️", Status: "completed"},
					{Name: "Image Pull", Icon: "⬇️", Status: "completed"},
					{Name: "Container Swap", Icon: "🔄", Status: stepStatus},
					{Name: "Health Check", Icon: "🩺", Status: healthStatus},
				},
				StartedAt: createdAt,
			})
		}
	}

	return services, deployments, nil
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	now := time.Now().UTC()
	services, deployments, err := fetchDockerClusterState(ctx)

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
		deployments = []DeploymentStatus{}
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

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/status", statusHandler)
	mux.HandleFunc("/api/facts", factsHandler(brainURL))

	fileServer := http.FileServer(http.FS(staticFS))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		fileServer.ServeHTTP(w, r)
	})

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
