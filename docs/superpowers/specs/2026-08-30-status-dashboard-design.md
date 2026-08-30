# Technical Design Specification: Core Status Dashboard & Reverse Proxy Stack

- **Author**: Aerial
- **Target Repository**: `azylman/aerial` (`/share/aerial`)
- **Status**: Reviewed & Hardened
- **Date**: 2026-08-30

---

## 1. Executive Summary
Aerial currently exposes the `agentsview` web UI directly on host port `8089`. To improve security, extensibility, and user experience, this design introduces a unified Reverse Proxy Gateway (`aerial-proxy`) and a dedicated status dashboard microservice (`aerial-dashboard`). Host port `8089` will bind exclusively to `aerial-proxy`, serving a high-tech Cyberpunk/Gundam Aerial HUD status dashboard at `/` and reverse-proxying `agentsview` under `/conversations/`.

---

## 2. Goals & Key Requirements

1. **Unified Entry Point**:
   - Expose only host port `8089` (`${AGENTSVIEW_HOST_PORT:-8089}`) for all web tools.
   - Internalize `agentsview` container port so it is only accessible via `aerial-net`.

2. **Reverse Proxy Gateway (`aerial-proxy`)**:
   - Nginx-based edge proxy container listening on port 80 internally.
   - Route `/` and static assets to `aerial-dashboard:8080`.
   - Route `/conversations/` and associated static asset prefixes (`/_app/`, `/_next/`, `/static/`, `/api/v1/sessions`) to `aerial-agentsview:8080/`.
   - Support WebSockets (`Upgrade` / `Connection`) and Server-Sent Events (SSE) without buffer truncation or connection timeouts.
   - Designed for easy addition of future proxy subpaths (e.g., Grafana, Home Assistant).

3. **Status Dashboard Microservice (`aerial-dashboard`)**:
   - Go-based HTTP server mounting `/var/run/docker.sock` in read-only mode (`:ro`).
   - Environment Variable Sanitization: Explicitly strips `GEMINI_API_KEY`, `DISCORD_TOKEN`, `GITHUB_PAT`, and `HA_TOKEN` from container inspection output before responding on `/api/status`.
   - REST API `GET /api/status` returning real-time container health, CPU %, RAM usage, uptime, and HTTP healthcheck status for all core stack services.
   - High-impact Cyberpunk / Gundam Aerial HUD single-page UI served at `/` with live auto-refresh and navigation link to `/conversations/`.

---

## 3. Architecture & Topology

```
                         [ Host Port 8089 ]
                                 │
                                 ▼
                   ┌───────────────────────────┐
                   │   aerial-proxy (Nginx)    │
                   └─────────────┬─────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 │                               │
         location /                      location /conversations/
                 │                               │
                 ▼                               ▼
       ┌───────────────────┐           ┌───────────────────┐
       │ aerial-dashboard  │           │ aerial-agentsview │
       │ (Go + Cyber HUD)  │           │  (Session Viewer) │
       └───────────────────┘           └───────────────────┘
```

---

## 4. Component Design & Specifications

### 4.1. Reverse Proxy (`aerial-proxy`)

- **Docker Image**: `nginx:alpine`
- **Location**: `/share/aerial/proxy`
- **Configuration File (`/share/aerial/proxy/default.conf`)**:
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

### 4.2. Status Dashboard Microservice (`aerial-dashboard`)

- **Directory**: `/share/aerial/dashboard`
- **Language**: Go 1.22+
- **Docker Build**: Multi-stage Dockerfile (`golang:alpine` builder -> `alpine:latest` runner).
- **Backend Responsibilities**:
  - Read container states via Docker API `/var/run/docker.sock` (`github.com/docker/docker/client`).
  - Redact sensitive environment variables from container inspect payload.
  - Perform HTTP GET health probe checks against internal container health endpoints.
  - Expose `/api/status` returning JSON.
- **Frontend Responsibilities**:
  - Embedded single-page application using `embed.FS`.
  - Gundam Aerial Cyberpunk HUD styling using Vanilla CSS tokens, glassmorphism, and responsive CSS grid.
  - Space Grotesk / JetBrains Mono typography.
  - Interactive top bar with link to `/conversations/`.
  - Real-time SSE / polling updates every 5s.

---

## 5. Review Findings & Mitigation Matrix

| Reviewer Role | Finding / Vulnerability | Mitigation in Design |
| :--- | :--- | :--- |
| **Infrastructure** | Missing trailing slash on `/conversations` causes 404s. | Added `location = /conversations { return 301 /conversations/; }`. |
| **Security (Red Team)** | Docker socket read allows reading secrets in container env vars. | Implemented strict env-var sanitization filter before returning JSON in `/api/status`. |
| **Adversarial** | SSE connections stall under default Nginx proxy timeouts. | Set `proxy_read_timeout 86400s;` and `proxy_buffering off;`. |
| **Frontend/UX** | Hard refresh on `/conversations/` might lose subpath context. | Proxy preserves URI query parameters and headers seamlessly. |

---

## 6. Verification & Self-Review Check

- [x] No plaintext secrets or hardcoded passwords.
- [x] Docker socket mounted as read-only (`:ro`).
- [x] Sensitive env vars sanitized in `/api/status`.
- [x] Unified host port mapping (`8089`).
- [x] Explicit WebSocket/SSE proxy directives.
- [x] Zero breaking changes to existing MCP microservices or `aerial-brain`.
