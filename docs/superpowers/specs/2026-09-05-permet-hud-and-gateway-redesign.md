# Technical Design Specification: Permet Command HUD v3.0, Tactical Gateway Launcher & GitSync Telemetry Pipeline

- **Author**: Aerial
- **Target Repositories**: `azylman/aerial` (`/share/aerial`) & `azylman/aerial-config` (`/share/aerial-config`)
- **Status**: Drafted for User Review
- **Date**: 2026-09-05

---

## 1. Executive Summary

Aerial currently serves a status dashboard microservice (`aerial-dashboard`) over an internal reverse proxy (`aerial-proxy`). As the ecosystem has expanded with multiple specialized sub-services (AgentsView, Docsify, Grafana TSDB dashboards, and Visual Prototyping), the status HUD needs to evolve into a unified, high-performance **Permet Command Bridge & Gateway Launcher**.

This hardened technical design rebuilds the status dashboard UI from scratch with an elevated **Permet Mecha aesthetic** (Gundam Aerial HUD, glassmorphism, glowing circuit ribbons, GUND-bit telemetry). It introduces:
1. **Tactical Quick-Launch Dock**: Unifying core proxy sub-paths (`LLM SESSIONS`, `DOCS`, `OBSERVABILITY`, `UI TESTING`) alongside user-defined custom links loaded dynamically from `aerial-config`.
2. **GitSync & Deploy Pipeline Telemetry**: Live delta tracking ($\Delta t = T_{\text{github}} - T_{\text{disk}}$), commit hashes, sync health badges, and Watchtower swap stages.
3. **Full Desktop & Mobile Responsiveness**: Seamless scaling from 4K/ultrawide displays down to handheld mobile viewports with touch-first tap targets and slide-up diagnostic sheets.
4. **Authoritative GitSync Sidecar API**: A dedicated `GET /status` endpoint on `aerial-gitsync` providing real-time repository checkout metadata and sync latency.

---

## 2. Goals & Key Requirements

1. **Elevated Permet Mecha Aesthetic**:
   - Double down on the futuristic Permet Score mecha theme with modern glassmorphism (`backdrop-filter: blur(16px)`), cybernetic cyan/crimson neon accents, responsive grid cards, and animated laser progress bars.
   - JetBrains Mono and Space Grotesk typography for high-density, legible telemetry.

2. **Tactical Quick-Launch Gateway & Extensibility**:
   - Built-in launch chips for core sub-paths:
     • **`💬 LLM SESSIONS`** ➔ `/conversations/` (AgentsView)
     • **`📚 DOCS`** ➔ `/docs/` (Docsify + Mermaid)
     • **`📊 OBSERVABILITY`** ➔ `/grafana/` (Grafana TSDB)
     • **`🧪 UI TESTING`** ➔ `/ui-testing/` (Visual Companion / Prototyping)
   - **Dynamic Config Extensibility**: Allow users to declare additional external or internal links in `config.yaml` (`dashboard.quick_launch_links`) which are automatically merged into the launcher dock.

3. **GitOps & Deployment Pipeline Visibility**:
   - Calculate and display GitSync time duration difference ($\Delta t$) between on-disk HEAD and GitHub `origin/main`.
   - Clear badge indicators: `🟢 IN SYNC (Δ 0s)`, `🟡 BEHIND BY 45s (1 COMMIT)`, `🚨 SYNC ERROR`.
   - Step progression tracking: `Commit Trigger` ➔ `CI Build & GHCR` ➔ `Watchtower Pull` ➔ `Container Swap` ➔ `Health Check`.

4. **Multi-Viewport Responsiveness**:
   - Full widescreen desktop support with multi-column layout.
   - Touch-optimized handheld layout (< 768px): swipeable launcher dock, vertical/collapsible deployment steps, slide-up bottom sheets for container diagnostics, and minimum 44px tap targets.

5. **Security & Zero-Regression Invariants**:
   - Strict token redaction across all error messages, prompt previews, and inspect payloads.
   - Decoupled, read-only Docker socket inspection.

---

## 3. Reverse Proxy Routing Topology (`aerial-proxy`)

The Nginx reverse proxy configuration (`proxy/default.conf`) is updated to support all sub-paths seamlessly:

```
                         [ Host Port 8089 ]
                                 │
                                 ▼
                   ┌───────────────────────────┐
                   │    aerial-proxy (Nginx)   │
                   └─────────────┬─────────────┘
                                 │
     ┌──────────────┬────────────┼────────────┬─────────────┬──────────────┐
     │              │            │            │             │              │
location /     location      location     location      location       location
(302 redirect) /dashboard/   /docs/       /conversations/ /grafana/   /ui-testing/
     │              │            │            │             │              │
     ▼              ▼            ▼            ▼             ▼              ▼
[dashboard]   [dashboard]     [docs]     [agentsview]   [grafana]    [brain/ui-server]
 (:8080)        (:8080)       (:80)        (:8080)       (:3000)        (:52341)
```

### New Proxy Directives for `/ui-testing/`
```nginx
# Visual Companion & UI Testing Prototyping Gateway
location /ui-testing/ {
    set $ui_test_backend http://aerial-brain:52341;
    rewrite ^/ui-testing/(.*)$ /$1 break;
    proxy_pass $ui_test_backend;
    proxy_set_header Host $http_host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    # WebSocket & SSE Streaming Support
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 86400s;
    proxy_buffering off;
}

location = /ui-testing {
    return 301 /ui-testing/;
}
```

---

## 4. Backend Architecture & Data Ingestion

### 4.1 GitSync Sidecar Status Endpoint (`aerial-gitsync:8080/status`)
`sidecars/gitsync/main.go` implements a new `GET /status` handler returning:

```json
{
  "status": "ok",
  "system_time": "2026-09-05T06:20:00Z",
  "repos": [
    {
      "name": "aerial",
      "path": "/share/aerial",
      "disk_commit": "7a9f1b2",
      "disk_commit_time": "2026-09-05T06:15:00Z",
      "github_commit": "7a9f1b2",
      "github_commit_time": "2026-09-05T06:15:00Z",
      "time_lag_seconds": 0,
      "sync_status": "synced",
      "last_sync_time": "2026-09-05T06:19:30Z",
      "error": ""
    },
    {
      "name": "aerial-config",
      "path": "/share/aerial-config",
      "disk_commit": "c4d5e6f",
      "disk_commit_time": "2026-09-05T06:10:00Z",
      "github_commit": "c4d5e6f",
      "github_commit_time": "2026-09-05T06:10:00Z",
      "time_lag_seconds": 0,
      "sync_status": "synced",
      "last_sync_time": "2026-09-05T06:19:30Z",
      "error": ""
    }
  ]
}
```

### 4.2 Dashboard Status API Extension (`aerial-dashboard:8080/api/status`)
`dashboard/main.go` queries `http://aerial-gitsync:8080/status` (with resilient fallback) and loads `config.yaml` to serve an enriched `ClusterResponse`:

```go
type QuickLaunchLink struct {
    Name        string `json:"name"`
    URL         string `json:"url"`
    Icon        string `json:"icon"`
    Description string `json:"description,omitempty"`
    Target      string `json:"target,omitempty"` // "_blank" | "_self"
    IsCore      bool   `json:"is_core"`
}

type GitSyncRepoStatus struct {
    Name            string    `json:"name"`
    Path            string    `json:"path"`
    DiskCommit      string    `json:"disk_commit"`
    DiskCommitTime  time.Time `json:"disk_commit_time"`
    GitHubCommit    string    `json:"github_commit"`
    GitHubCommitTime time.Time `json:"github_commit_time"`
    TimeLagSeconds  int64     `json:"time_lag_seconds"`
    SyncStatus      string    `json:"sync_status"` // "synced" | "behind" | "pulling" | "error"
    LastSyncTime    time.Time `json:"last_sync_time"`
    Error           string    `json:"error,omitempty"`
}

type GitSyncStatusPayload struct {
    PrimaryRepo GitSyncRepoStatus   `json:"primary_repo"`
    Repos       []GitSyncRepoStatus `json:"repos"`
    OverallSync string              `json:"overall_sync"` // "synced" | "behind" | "error"
}
```

### 4.3 User Configuration Schema (`aerial-config/config.yaml`)
Users can declare arbitrary custom quick-launch links:

```yaml
dashboard:
  quick_launch_links:
    - name: "Home Assistant"
      url: "http://homeassistant.local:8123"
      icon: "🏠"
      description: "Smart Home Automation Hub"
      target: "_blank"
    - name: "Portainer"
      url: "http://homeassistant.local:9000"
      icon: "🐳"
      description: "Docker Container Manager"
      target: "_blank"
```

---

## 5. Permet Mecha UI & Responsive Component System

### 5.1 Design Tokens & CSS Architecture
- **Obsidian Dark Surface**: `#07090e`, `#0d1117`, `#161b22`
- **Neon Cyan Accent**: `#00f0ff` (Permet Level 5)
- **GUND-Bit Crimson Accent**: `#ff3366` (Alerts & CI failures)
- **Emerald Pulse**: `#00ff9d` (Synced & Healthy)
- **Solar Amber**: `#ffb700` (Sync Lag & Awaiting Pull)
- **Glassmorphism Panels**: `background: rgba(13, 17, 23, 0.75)`, `backdrop-filter: blur(16px)`, `border: 1px solid rgba(0, 240, 255, 0.12)`

### 5.2 Header & Tactical Quick-Launch Dock
- **Brand HUD & Permet Dial**: Live animated Permet Score meter displaying load and cluster pulse.
- **Quick-Launch Dock**:
  - Grid/ribbon of interactive launcher chips.
  - Core chips (`LLM SESSIONS`, `DOCS`, `OBSERVABILITY`, `UI TESTING`) followed by any user-configured custom links.
  - Live pulse dot on each chip indicating route accessibility.

### 5.3 Operations Bridge (Deck 1)
- **Deploy Pipeline & GitOps Bay**:
  - Dedicated GitSync delta pill (`🟢 IN SYNC (Δ 0s)` or `🟡 BEHIND BY 45s`).
  - Active deployment card with 5-stage step progression, matrix build chips, and direct links to GitHub Actions logs.
  - Enhanced idle state displaying disk SHA, GitHub SHA, and polling frequency.
- **Live Agent Execution Queue**:
  - Live active turn cards with session links (`/conversations/sessions/antigravity-cli:<uuid>`), human-readable task summaries, and retry badges.
- **Stack Components Grid**:
  - Grid of service tiles showing container status, uptime, healthcheck state, and one-click diagnostic drawer.

### 5.4 Mobile & Handheld Optimization (< 768px)
- **Sticky Permet Mobile Bar**: Brand title, Permet Score, and swipeable Quick-Launch Dock.
- **44px Tap Targets**: Fully accessible button and chip dimensions.
- **Slide-Up Bottom Sheet**: Diagnostic drawer slides up from bottom with smooth drag-down dismissal.
- **Adaptive Step Timeline**: Deploy steps reflow into a clean vertical timeline on narrow viewports.

---

## 6. Verification & Testing Strategy

1. **Backend Unit Tests (`sidecars/gitsync/main_test.go`, `dashboard/main_test.go`)**:
   - Verify `GET /status` on gitsync correctly computes `time_lag_seconds` and handles uninitialized or locked git repos.
   - Verify dashboard `/api/status` merges core and config links, handles gitsync sidecar downtime gracefully, and sanitizes sensitive tokens.

2. **Frontend JSDOM Test Suite (`dashboard/app.test.js`)**:
   - Assert all 4 core quick-launch links render with exact target URLs.
   - Verify custom links from config render seamlessly.
   - Verify GitSync duration delta badge formatting (`0s`, `45s`, `2m 15s`).
   - Test mobile viewport class toggles and drawer open/close events.

3. **Pre-Flight Verification**:
   - Run `verify.sh` to ensure zero lint or test failures across the monorepo.
