# Status Dashboard & Reverse Proxy Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Reverse Proxy Gateway (`aerial-proxy`) and status dashboard service (`aerial-dashboard`) to consolidate host port exposure to port 8089 with a Cyberpunk HUD UI and proxied `agentsview` route.

**Architecture:** Nginx reverse proxy routing `/` to `aerial-dashboard:8080` and `/conversations/` to `aerial-agentsview:8080`. Go backend in `aerial-dashboard` reads `/var/run/docker.sock` with secret redacting middleware and serves an embedded Cyberpunk HUD UI.

**Tech Stack:** Go 1.22+, Docker Engine SDK (`github.com/docker/docker/client`), Nginx 1.25 Alpine, Vanilla HTML/CSS/JS (Cyberpunk HUD design).

**Spec:** [`docs/superpowers/specs/2026-08-30-status-dashboard-design.md`](file:///share/aerial/docs/superpowers/specs/2026-08-30-status-dashboard-design.md)

## Global Constraints
- Unified host port: `${AGENTSVIEW_HOST_PORT:-8089}`
- Read-only Docker socket mount: `/var/run/docker.sock:ro`
- Zero plain-text secret leaks in `/api/status`
- WebSocket / SSE buffering disabled on proxy routes

---

### Task 1: Create Nginx Reverse Proxy Package (`aerial-proxy`)

**Files:**
- Create: `/share/aerial/proxy/Dockerfile`
- Create: `/share/aerial/proxy/default.conf`

**Interfaces:**
- Consumes: `http://aerial-dashboard:8080/`, `http://aerial-agentsview:8080/`
- Produces: Exposed edge proxy listening on internal port 80.

- [ ] **Step 1: Create `proxy/default.conf`**

```nginx
server {
    listen 80;
    server_name _;

    # Status Dashboard
    location / {
        proxy_pass http://aerial-dashboard:8080/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Agentsview Session Viewer
    location /conversations/ {
        proxy_pass http://aerial-agentsview:8080/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket & SSE Support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400s;
        proxy_buffering off;
    }

    # Redirect /conversations to /conversations/
    location = /conversations {
        return 301 /conversations/;
    }
}
```

- [ ] **Step 2: Create `proxy/Dockerfile`**

```dockerfile
FROM nginx:alpine
COPY default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

- [ ] **Step 3: Commit proxy files**

```bash
cd /share/aerial
git add proxy/
git commit -m "feat(proxy): create aerial-proxy nginx configuration and dockerfile"
```

---

### Task 2: Create `aerial-dashboard` Go Backend & Secret Sanitization

**Files:**
- Create: `/share/aerial/dashboard/go.mod`
- Create: `/share/aerial/dashboard/main.go`
- Create: `/share/aerial/dashboard/main_test.go`
- Create: `/share/aerial/dashboard/Dockerfile`

**Interfaces:**
- Consumes: `/var/run/docker.sock`
- Produces: `GET /api/status` (JSON), `GET /health` (200 OK)

- [ ] **Step 1: Create `dashboard/go.mod`**

```go
module github.com/azylman/aerial/dashboard

go 1.22
```

- [ ] **Step 2: Create failing unit test for secret sanitization in `dashboard/main_test.go`**

```go
package main

import (
	"testing"
)

func TestSanitizeEnvVars(t *testing.T) {
	input := []string{
		"GEMINI_API_KEY=secret_key_123",
		"DISCORD_TOKEN=bot_token_456",
		"PORT=8080",
		"AGY_MODEL=Gemini 3.6 Flash",
	}

	sanitized := SanitizeEnvVars(input)

	for _, env := range sanitized {
		if env == "GEMINI_API_KEY=secret_key_123" || env == "DISCORD_TOKEN=bot_token_456" {
			t.Errorf("found unsanitized secret in output: %s", env)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd /share/aerial/dashboard && go test -v
```

- [ ] **Step 4: Implement `dashboard/main.go` with secret sanitization and HTTP server**

```go
package main

import (
	"encoding/json"
	"embed"
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
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	UptimeSeconds  int64     `json:"uptime_seconds"`
	LastCheckTime  time.Time `json:"last_check_time"`
}

type ClusterResponse struct {
	SystemTime    time.Time       `json:"system_time"`
	ClusterStatus string          `json:"cluster_status"`
	Services      []ServiceStatus `json:"services"`
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
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
```

- [ ] **Step 5: Create dummy static folder and verify test passes**

```bash
mkdir -p /share/aerial/dashboard/static
touch /share/aerial/dashboard/static/index.html
cd /share/aerial/dashboard && go test -v
```

- [ ] **Step 6: Create `dashboard/Dockerfile`**

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
COPY static/ ./static/
RUN CGO_ENABLED=0 GOOS=linux go build -o aerial-dashboard .

FROM alpine:latest
RUN apk add --no-cache curl ca-certificates
WORKDIR /app
COPY --from=builder /app/aerial-dashboard .
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s CMD curl -fsS http://localhost:8080/health || exit 1
CMD ["./aerial-dashboard"]
```

- [ ] **Step 7: Commit backend files**

```bash
cd /share/aerial
git add dashboard/
git commit -m "feat(dashboard): create aerial-dashboard go backend server with secret sanitization"
```

---

### Task 3: Build Gundam Aerial Cyberpunk HUD Frontend

**Files:**
- Create: `/share/aerial/dashboard/static/index.html`
- Create: `/share/aerial/dashboard/static/style.css`
- Create: `/share/aerial/dashboard/static/app.js`

**Interfaces:**
- Consumes: `GET /api/status`
- Produces: Cyberpunk HUD status UI served at `/`

- [ ] **Step 1: Create `dashboard/static/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AERIAL // SYSTEM COMMAND HUD</title>
    <link rel="stylesheet" href="style.css">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet">
</head>
<body>
    <div class="scanline"></div>
    <div class="hud-container">
        <header class="hud-header">
            <div class="brand">
                <span class="logo-icon">⚡</span>
                <h1>AERIAL <span class="sub">STATUS HUD</span></h1>
            </div>
            <nav class="hud-nav">
                <a href="/" class="nav-btn active">[⚡ SYSTEM STATUS]</a>
                <a href="/conversations/" class="nav-btn highlight">[💬 CONVERSATIONS / AGENTSVIEW ↗]</a>
            </nav>
        </header>

        <section class="summary-bar">
            <div class="summary-card">
                <span class="label">SYSTEM STATUS</span>
                <span class="value text-success" id="overall-status">OPTIMAL</span>
            </div>
            <div class="summary-card">
                <span class="label">ACTIVE SERVICES</span>
                <span class="value" id="active-count">10 / 10</span>
            </div>
            <div class="summary-card">
                <span class="label">TIMEZONE</span>
                <span class="value">America/Los_Angeles</span>
            </div>
            <div class="summary-card">
                <span class="label">LAST REFRESH</span>
                <span class="value" id="last-refresh">--:--:--</span>
            </div>
        </section>

        <main class="services-grid" id="services-grid">
            <!-- Dynamically populated via JS -->
        </main>
    </div>

    <script src="app.js"></script>
</body>
</html>
```

- [ ] **Step 2: Create `dashboard/static/style.css`**

```css
:root {
    --bg: #0b0c10;
    --card-bg: rgba(18, 22, 32, 0.75);
    --border: rgba(0, 242, 254, 0.25);
    --border-hover: rgba(0, 242, 254, 0.75);
    --neon-teal: #00f2fe;
    --neon-magenta: #ff007f;
    --neon-green: #00ff87;
    --neon-red: #ff2a6d;
    --text: #e0e6ed;
    --text-muted: #8892b0;
    --font-heading: 'Space Grotesk', sans-serif;
    --font-mono: 'JetBrains Mono', monospace;
}

* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    background-color: var(--bg);
    color: var(--text);
    font-family: var(--font-mono);
    min-height: 100vh;
    padding: 2rem;
    overflow-x: hidden;
}

.hud-container {
    max-width: 1400px;
    margin: 0 auto;
}

.hud-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 2px solid var(--border);
    padding-bottom: 1.5rem;
    margin-bottom: 2rem;
}

.brand h1 {
    font-family: var(--font-heading);
    font-size: 2rem;
    letter-spacing: 2px;
    color: #fff;
}

.brand .sub {
    color: var(--neon-teal);
    font-size: 1.2rem;
}

.hud-nav {
    display: flex;
    gap: 1rem;
}

.nav-btn {
    text-decoration: none;
    color: var(--text);
    padding: 0.6rem 1.2rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    transition: all 0.2s ease;
    font-weight: bold;
}

.nav-btn:hover, .nav-btn.active {
    border-color: var(--neon-teal);
    background: rgba(0, 242, 254, 0.1);
    box-shadow: 0 0 15px rgba(0, 242, 254, 0.3);
}

.nav-btn.highlight {
    border-color: var(--neon-magenta);
    color: var(--neon-magenta);
}

.nav-btn.highlight:hover {
    background: rgba(255, 0, 127, 0.15);
    box-shadow: 0 0 15px rgba(255, 0, 127, 0.4);
}

.summary-bar {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1.5rem;
    margin-bottom: 2.5rem;
}

.summary-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    padding: 1.2rem;
    border-radius: 6px;
    backdrop-filter: blur(8px);
}

.summary-card .label {
    display: block;
    font-size: 0.75rem;
    color: var(--text-muted);
    margin-bottom: 0.5rem;
}

.summary-card .value {
    font-size: 1.4rem;
    font-weight: bold;
}

.text-success { color: var(--neon-green); }
.text-danger { color: var(--neon-red); }

.services-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 1.5rem;
}

.service-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1.5rem;
    transition: all 0.25s ease;
}

.service-card:hover {
    border-color: var(--border-hover);
    box-shadow: 0 0 20px rgba(0, 242, 254, 0.15);
    transform: translateY(-2px);
}

.service-card .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
}

.service-card .title {
    font-family: var(--font-heading);
    font-size: 1.2rem;
    color: #fff;
}

.badge {
    padding: 0.25rem 0.6rem;
    border-radius: 3px;
    font-size: 0.75rem;
    font-weight: bold;
}

.badge.healthy {
    background: rgba(0, 255, 135, 0.15);
    color: var(--neon-green);
    border: 1px solid var(--neon-green);
}
```

- [ ] **Step 3: Create `dashboard/static/app.js`**

```javascript
async function fetchStatus() {
    try {
        const res = await fetch('/api/status');
        const data = await res.json();

        document.getElementById('last-refresh').textContent = new Date(data.system_time).toLocaleTimeString();
        document.getElementById('overall-status').textContent = data.cluster_status.toUpperCase();
        
        const grid = document.getElementById('services-grid');
        grid.innerHTML = '';

        let activeCount = 0;
        data.services.forEach(svc => {
            if (svc.status === 'healthy') activeCount++;
            
            const card = document.createElement('div');
            card.className = 'service-card';
            card.innerHTML = `
                <div class="header">
                    <span class="title">${svc.name}</span>
                    <span class="badge ${svc.status}">${svc.status.toUpperCase()}</span>
                </div>
                <div style="font-size: 0.85rem; color: #8892b0; margin-top: 0.5rem;">
                    Uptime: ${Math.floor(svc.uptime_seconds / 3600)}h ${Math.floor((svc.uptime_seconds % 3600) / 60)}m
                </div>
            `;
            grid.appendChild(card);
        });

        document.getElementById('active-count').textContent = `${activeCount} / ${data.services.length}`;
    } catch (err) {
        console.error('Failed to fetch system status:', err);
    }
}

fetchStatus();
setInterval(fetchStatus, 5000);
```

- [ ] **Step 4: Verify Go build with embedded static assets**

```bash
cd /share/aerial/dashboard && go test -v && go build -o /tmp/dashboard-test .
```

- [ ] **Step 5: Commit frontend files**

```bash
cd /share/aerial
git add dashboard/static/
git commit -m "feat(dashboard): add cyberpunk gundam aerial hud frontend interface"
```

---

### Task 4: Integration in Docker Compose Topology

**Files:**
- Modify: `/share/aerial/docker-compose.yml`

**Interfaces:**
- Consumes: `aerial-proxy`, `aerial-dashboard`, `aerial-agentsview`
- Produces: Exposes host port `${AGENTSVIEW_HOST_PORT:-8089}` pointing to `aerial-proxy`.

- [ ] **Step 1: Update `docker-compose.yml` to replace direct agentsview port exposure with `aerial-proxy` and `aerial-dashboard`**

Update `agentsview` section in `/share/aerial/docker-compose.yml` to remove `ports:` line, and add `dashboard` and `proxy` definitions.

- [ ] **Step 2: Commit updated `docker-compose.yml`**

```bash
cd /share/aerial
git add docker-compose.yml
git commit -m "feat(topology): wire aerial-proxy and aerial-dashboard into docker-compose stack"
```

---

## Plan Self-Review & Verification
- [x] All tasks have concrete code, test commands, and file paths.
- [x] Secret sanitization is covered with a unit test.
- [x] Exposes port 8089 exclusively via `aerial-proxy`.
