package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed static/*
var content embed.FS

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

type ClusterResponse struct {
	SystemTime    time.Time       `json:"system_time"`
	ClusterStatus string          `json:"cluster_status"`
	Services      []ServiceStatus `json:"services"`
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := ClusterResponse{
		SystemTime:    time.Now().UTC(),
		ClusterStatus: "healthy",
		Services: []ServiceStatus{
			{Name: "aerial-brain", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-scheduler-mcp", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-discord-mcp", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-docker-mcp", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-github-mcp", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-ollama", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-agentsview", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-watchtower", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-autoheal", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
			{Name: "aerial-proxy", Status: "healthy", UptimeSeconds: 86400, LastCheckTime: time.Now().UTC()},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		log.Fatalf("failed to create static sub filesystem: %v", err)
	}

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/status", statusHandler)
	http.Handle("/", http.FileServer(http.FS(staticFS)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("aerial-dashboard server starting on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
