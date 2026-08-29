# Aerial Stack

An autonomous personal AI assistant system running natively on Docker, named after Gundam Aerial.

Aerial provides a multi-agent, tool-enabled AI assistant accessible via Discord and HTTP API, with persistent multi-turn SQLite memory, Home Assistant integration, GitHub operations, host Docker infrastructure inspection, and an extensible architecture for custom skills, MCP tools, and sidecar containers.

---

## 1. System Architecture & Topology

Aerial uses a decoupled **Two-Repository Architecture**:
- **Engine Repo (`azylman/aerial`)**: Core Go backend (`aerial-brain`), MCP microservices, and Docker Compose topology.
- **User Config Repo (e.g. `your-username/your-aerial-config`)**: Private user configuration (`config.yaml`), persona guidelines (`AGENTS.md`), and custom skills (`custom-skills/`). Starter template available at [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example).

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                               Discord Gateway                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │ Realtime Gateway Events & Mentions
                                       │ Continuous Typing Indicator Refresh
┌─────────────────────────────────────────────────────────────────────────────┐
│                                Aerial Brain                                 │
│  • In-process Discord Funnel & Gateway Worker                               │
│  • Headless Antigravity Agent Engine (agy)                                  │
│  • In-process Mutex-Guarded GitSync (/share/aerial-config & /share/aerial)  │
│  • Recursive File Watcher (fsnotify) with Hot-Reloading & LKGC Fallback     │
│  • SQLite Multi-Turn Thread Memory (/data/aerial.db)                        │
│  • Dynamic Built-in & User Custom Skills Discovery                          │
│  • Semantic Memory Vector Embeddings via Ollama (all-minilm:latest)         │
└─────────────────────────────────────────────────────────────────────────────┘
               │                       │                      │
┌───────────────────────────┐ ┌───────────────────────┐ ┌─────────────────────┐
│        discord-mcp        │ │       docker-mcp      │ │     github-mcp      │
│  (Port 4001: Streamable)  │ │ (Port 4002: Proxy)    │ │ (Port 4003: Proxy)  │
└───────────────────────────┘ └───────────────────────┘ └─────────────────────┘
               │                       │                      │
       Discord REST API        Host Docker Socket       GitHub Copilot MCP
                           (/var/run/docker.sock)
               │                       │                      │
┌───────────────────────────┐ ┌───────────────────────┐ ┌─────────────────────┐
│       scheduler-mcp       │ │     aerial-ollama     │ │  aerial-agentsview  │
│  (Port 8080: Internal)    │ │ (Port 11434: Embed)   │ │ (Port 8089: Web UI) │
└───────────────────────────┘ └───────────────────────┘ └─────────────────────┘
               │                       │
┌───────────────────────────┐ ┌───────────────────────┐
│     aerial-watchtower     │ │    aerial-autoheal    │
│  (GHCR CD Supervisor)     │ │  (Health Supervisor)  │
└───────────────────────────┘ └───────────────────────┘
```

---

## 2. Extensibility Guide

Aerial is designed to be easily extended across four distinct layers:

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  4 WAYS TO EXTEND AERIAL                                    │
├─────────────────────────────┬──────────────────────────┬────────────────────┬───────────────┤
│ 1. CONFIG & PERSONA         │ 2. CUSTOM SKILLS         │ 3. MCP SERVERS     │ 4. CONTAINERS │
│ config.yaml & AGENTS.md     │ Runbooks in config repo  │ Connecting APIs    │ Extra sidecars│
└─────────────────────────────┴──────────────────────────┴────────────────────┴───────────────┘
```

---

### Layer 1: Configuration & Persona (`config.yaml` & `AGENTS.md`)

User configuration and persona rules live in your private configuration repository (see [aerial-config-example](https://github.com/azylman/aerial-config-example)):

1. **`config.yaml`** (Non-secret options):
   ```yaml
   model: "Gemini 3.6 Flash (Low)"
   timeout_minutes: 15
   timezone: "America/Los_Angeles"
   system_channel: "aerial-dev"

   git_sync:
     enabled: true
     interval: "60s"
     config_repo_url: "https://github.com/your-username/your-aerial-config.git"
     repositories:
       - "/share/aerial-config"
       - "/share/aerial"
   ```
   - **Hot-Reloading**: Changes to `config.yaml` are detected instantly via `fsnotify` and reconfigured in-memory without restarting the daemon.
   - **Last Known Good Configuration (LKGC) & Discord Alerts**: If an invalid YAML file is saved, Aerial retains the previous valid settings in memory and posts a diagnostic alert to `#aerial-dev` in Discord.

2. **`AGENTS.md`** (Persona & Tone Overrides):
   Define custom persona rules, tone guidelines, or private operational context. Instructions in `AGENTS.md` take priority over base `SYSTEM.md` rules.

---

### Layer 2: Adding Custom Skills

Skills use **Progressive Disclosure**—Aerial only loads skill titles and descriptions into context, reading full runbooks on-demand when relevant.

#### A. User Custom Skills (`custom-skills/` in your config repo)
Place custom skill directories inside `custom-skills/` in your private configuration repository:
```text
custom-skills/
└── smart-home/
    └── SKILL.md
```
`aerial-brain` automatically discovers custom skills in `/share/aerial-config/custom-skills` and symlinks them into `/root/.gemini/skills/` with highest priority. Orphaned or dead symlinks are automatically swept when skills are renamed or removed.

#### B. Built-in Skills (`azylman/aerial/.agents/skills/`)
Core system skills (such as `self-improvement` and `self-update`) are baked into the `brain` image during build.

#### Skill File Structure (`SKILL.md`)
```markdown
---
name: smart-home
description: "Control smart home devices, lights, and scenes via Home Assistant MCP."
---

# Smart Home Operations Runbook

## Steps
1. Query entity state using Home Assistant MCP tools.
2. Verify target entity before executing state changes.
```

---

### Layer 3: Adding Custom MCP Servers

Aerial connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE:

#### Built-in Tool Autodiscovery
By default, Aerial automatically mounts:
- **`discord`** (`http://discord-mcp:4001/mcp`)
- **`docker`** (`http://docker-mcp:4002/mcp`)
- **`github`** (`http://github-mcp:4003/mcp` when `GITHUB_PAT` is set)
- **`ha-mcp`** (Home Assistant webhook tools when `HA_TOKEN` is configured)
- **`scheduler`** (`http://scheduler-mcp:8080/mcp`)

#### Custom MCP Servers (`config.yaml`)
Define additional MCP tools directly in your `config.yaml`:
```yaml
mcp_servers:
  brave-search:
    serverUrl: "http://brave-mcp:4005/mcp"
  custom-remote-api:
    serverUrl: "https://mcp.example.com/mcp"
    headers:
      Authorization: "Bearer ${CUSTOM_API_KEY}"
```
Environment variables `${VAR}` are interpolated dynamically at runtime from your host `.env`.

---

### Layer 4: Adding Custom Containers (`docker-compose.override.yml`)

You can add extra services or MCP containers to the `aerial-net` bridge network by creating `docker-compose.override.yml` (automatically merged by Docker Compose and ignored by Git):

```yaml
services:
  brave-mcp:
    image: mcp/brave-search
    container_name: aerial-brave-mcp
    restart: unless-stopped
    environment:
      - BRAVE_API_KEY=${BRAVE_API_KEY}
    networks:
      - aerial-net
```

---

## 3. Continuous Deployment & Self-Improvement

Aerial uses an automated GitOps deployment pipeline:
1. **GitHub Actions Matrix Builds**: Triggers dynamic matrix builds only for modified microservices and publishes them to GitHub Container Registry (`ghcr.io/azylman/aerial-*`).
2. **Watchtower Supervisor**: Polls GHCR every 60 seconds out-of-band and performs zero-downtime rolling container updates.
3. **In-Process GitSync**: Automatically pulls updates from your configuration repository and `azylman/aerial` every 60 seconds.
4. **Self-Improvement Protocol**: When prompted to make code changes or fixes in Discord, Aerial uses `.agents/skills/self-improvement/SKILL.md` to run local tests, commit, and push directly to `origin/main`.

---

## 4. Component Modules

| Service | Port | Description |
| --- | --- | --- |
| **`aerial-brain`** | `8088` | Go execution daemon running `agy`, SQLite memory, Discord funnel, GitSync, and file watcher. |
| **`aerial-scheduler-mcp`**| `8080` (Internal) | SQLite-backed cron and one-shot reminder management server. |
| **`aerial-discord-mcp`** | `4001` | Outbound MCP server providing Discord messaging, thread creation, and channel tools. |
| **`aerial-docker-mcp`** | `4002` | `supergateway` proxy wrapping official Docker MCP (`mcp/docker`) over the host socket. |
| **`aerial-github-mcp`** | `4003` | `supergateway` proxy wrapping GitHub MCP server with PAT authentication. |
| **`aerial-ollama`** | `11434` | Local LLM and embedding server for vector memory retrieval (`all-minilm:latest`). |
| **`aerial-agentsview`** | `8089` | Web UI for visualizing agent transcripts, session history, and execution timelines. |
| **`aerial-watchtower`** | - | Out-of-band continuous deployment supervisor polling GHCR every 60 seconds. |
| **`aerial-autoheal`** | - | Health supervisor probing container healthchecks every 15s and auto-restarting unhealthy containers. |

---

## 5. Quickstart Setup

### Prerequisites
- Docker Engine 24+ & Docker Compose v2+
- Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))
- Discord Bot Token (with Message Content and Server Members intents enabled)
- GitHub Personal Access Token (for private configuration repository synchronization)

### Step 1: Create Your Private Configuration Repository
1. Create a private repository on GitHub (e.g. `your-username/my-aerial-config`).
2. Copy or fork the template files from [**`azylman/aerial-config-example`**](https://github.com/azylman/aerial-config-example) into your private repository.
3. Customize `config.yaml` and `AGENTS.md` as desired.

### Step 2: Clone Aerial Engine
```bash
git clone https://github.com/azylman/aerial.git
cd aerial
```

### Step 3: Configure Environment Variables
```bash
cp .env.example .env
```
Edit `.env` and configure your secret credentials and config repo URL:
```ini
GEMINI_API_KEY=your_gemini_api_key_here
DISCORD_BOT_TOKEN=your_discord_bot_token_here
GITHUB_PAT=your_github_personal_access_token_here
HA_TOKEN=http://192.168.1.14:8123/api/webhook/mcp_your_id

# Private Configuration Repository URL
AERIAL_CONFIG_REPO_URL=https://github.com/your-username/my-aerial-config.git
```

### Step 4: Launch Stack
```bash
docker compose up -d
```
On boot, `aerial-brain` will automatically adopt or clone your private repository into `/share/aerial-config` using `GITHUB_PAT` and load your `config.yaml` settings.

### Step 5: Verify Health
```bash
docker compose ps
docker compose logs -f brain
```

---

## 6. Operational Commands

| Action | Command |
| --- | --- |
| Start all services | `docker compose up -d` |
| Stop all services | `docker compose down` |
| View live logs | `docker compose logs -f` |
| View transcripts & memory | `curl http://localhost:8088/api/transcripts` |
| Restart single service | `docker compose restart brain` |
| Update images & rebuild | `docker compose build && docker compose up -d` |

---

## 7. Security & Best Practices

- **Zero Plaintext Tokens**: GitHub PATs are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Log Sanitization**: All subprocess logs are passed through regex sanitizers to mask sensitive tokens.
- **Never commit `.env`**: Secrets and tokens are strictly ignored by `.gitignore`.
- **Restricted File Permissions**: Run `chmod 600 .env` on the host to protect credentials.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `aerial-net` bridge network.