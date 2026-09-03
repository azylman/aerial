# Aerial

An autonomous personal AI assistant system running natively on Docker, named after Gundam Aerial.

Aerial provides a multi-agent, tool-enabled AI assistant accessible via Discord and HTTP API, with persistent multi-turn SQLite memory, GitHub operations, host Docker infrastructure inspection, and an extensible architecture for custom skills, MCP tools, and sidecar containers.

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

1. **`config.yaml`** (Agent Options, Channel Policies, & MCP Tools):
   ```yaml
   model: "Gemini 2.5 Flash"
   timeout_minutes: 15
   timezone: "America/Los_Angeles"
   system_channel: "aerial-dev"

   # Administrator Allowlist
   # User IDs authorized to perform system-level operations (SYSTEM.md, config.yaml, Docker)
   admin_users:
     - "1542035925603713086"

   # Channel Policies & Interaction Modes
   channels:
     # Default fallback (Required)
     default:
       mode: "threads"           # "threads" | "channel" | "ignore"
       typing_indicator: "always" # "always" | "on_mention" | "never"
       ignore_bots: true
       ambient_wake_threshold: 0.80

     # In-Channel Direct Interaction (no threads spawned)
     general:
       mode: "channel"
       typing_indicator: "on_mention"
       ignore_bots: true
       ambient_wake_threshold: 0.80
       ambient_wake_prompt: "Determine whether the target message is relevant to Aerial and warrants Aerial waking up and responding, based on the recent channel context."
       max_session_turns: 50

     # Custom ambient prompt per channel
     dev-alerts:
       mode: "channel"
       typing_indicator: "on_mention"
       ignore_bots: false # Allow bot alerts
       ambient_wake_threshold: 0.70
       ambient_wake_prompt: "Only wake up and respond if the user is asking about Kubernetes deployments, CI/CD pipeline failures, or production outages."

     # Ignore specific noisy channels
     memes:
       mode: "ignore"

   git_sync:
     enabled: true
     interval: "60s"
     config_repo_url: "https://github.com/your-username/your-aerial-config.git"
     repositories:
       - "/share/aerial-config"
       - "/share/aerial"
   ```
   - **Interaction Modes**:
     - `threads`: Direct messages or mentions spawn and route to a Discord thread (default).
     - `channel`: Messages are evaluated directly in-channel without spawning threads using a **Two-Tier Wake Model**:
       - **Tier 1 (Direct Wake)**: Direct mentions (`@Aerial`), replies to Aerial, and keywords (`\b(aerial|gundam)\b`) trigger immediate wake and response.
       - **Tier 2 (Ambient Relevance Scorer)**: Ambient messages are scored (0.0 to 1.0) by a fast classifier with the previous 10 messages of channel context against `ambient_wake_prompt`.
       - **Native Lookback Transcripts**: Unaddressed ambient messages scoring below `ambient_wake_threshold` are silently appended to Antigravity's native `transcript.jsonl` without typing indicators or LLM execution, building conversational memory so Aerial has full context when subsequently woken.
     - `ignore` (or `disabled`): Channel is completely ignored (no messages evaluated, no startup sweeps).
   - **Channel-Level Configuration Options**:
     - `ambient_wake_threshold`: Confidence threshold (0.0 to 1.0) to wake Aerial on ambient messages (default `0.80` for channel mode; `0.0` disables ambient wake).
     - `ambient_wake_prompt`: Custom directive/criteria for the fast ambient relevance classifier in that channel (inherits from `channels.default.ambient_wake_prompt`).
     - `ignore_bots`: Set to `false` on specific channels to enable peer bot interactions while keeping default `ignore_bots: true`.
     - `max_session_turns`: Turn count threshold before rotating the internal conversation ID (default `50`).
   - **Server Whitelisting (Default-Deny)**: Set `channels.default.mode: "ignore"` to ignore the entire server by default, responding only in explicitly declared channels.
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
└── weather-alerts/
    └── SKILL.md
```
`aerial-brain` automatically discovers custom skills in `/share/aerial-config/custom-skills` and symlinks them into `/root/.gemini/skills/` with highest priority. Orphaned or dead symlinks are automatically swept when skills are renamed or removed.

#### B. Built-in Skills (`azylman/aerial/.agents/skills/`)
Core system skills (such as `self-improvement` and `self-update`) are baked into the `brain` image during build.

#### Skill File Structure (`SKILL.md`)
```markdown
---
name: weather-alerts
description: "Check regional weather forecasts and send alert summaries via Discord."
---

# Weather Alerts Runbook

## Steps
1. Query weather API using MCP tools.
2. Format forecast summary.
```

---

### Layer 3: Adding Custom MCP Servers

Aerial connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE:

#### Built-in Tool Autodiscovery
By default, Aerial automatically mounts:
- **`discord`** (`http://discord-mcp:4001/mcp`)
- **`docker`** (`http://docker-mcp:4002/mcp`)
- **`github`** (`http://github-mcp:4003/mcp` when `GITHUB_PAT` is set)
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

You can add extra services or MCP containers to the `aerial-net` bridge network by placing `docker-compose.override.yml` in your private configuration repository. `aerial-brain` automatically synchronizes it to the project root on startup and on git sync:

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
- Google Account for OAuth authentication (recommended to avoid API key rate limits and 503 overload errors) OR Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))
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
Edit `.env` and configure your credentials and config repo URL:
```ini
# Recommended: Leave GEMINI_API_KEY commented out for Google OAuth authentication!
# GEMINI_API_KEY=your_gemini_api_key_here

DISCORD_BOT_TOKEN=your_discord_bot_token_here
GITHUB_PAT=your_github_personal_access_token_here

# Private Configuration Repository URL
AERIAL_CONFIG_REPO_URL=https://github.com/your-username/my-aerial-config.git
```

### Step 4: Launch Stack
```bash
docker compose up -d
```
On boot, `aerial-brain` will automatically adopt or clone your private repository into `/share/aerial-config` using `GITHUB_PAT` and load your `config.yaml` settings.

### Step 5: Authenticate via Google OAuth (Recommended)
If using OAuth (with `GEMINI_API_KEY` commented out):
1. Run the interactive `agy` CLI inside the running container:
   ```bash
   docker exec -it aerial-brain agy
   ```
2. Copy the displayed Google OAuth login URL into your web browser and sign in.
3. Paste the authorization code back into the terminal prompt and hit Enter.
4. Press `Ctrl+C` to exit once authenticated. Aerial will store the OAuth session token in `/data` and automatically refresh access tokens in the background!

### Step 6: Verify Health
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

- **Multi-User Security & Admin Privilege Enforcement**: In shared or multi-user channels, messages from users are automatically checked against `admin_users` in `config.yaml`. Only authorized admins (`is_admin: true`) can modify system instructions (`SYSTEM.md`, `AGENTS.md`), edit system configuration (`config.yaml`), manage Docker containers, or alter cron schedules.
- **Fail-Closed Default-Deny Server Containment**: Set `channels.default.mode: "ignore"` to contain Aerial exclusively to allowlisted channels on shared Discord servers.
- **Zero Plaintext Tokens**: GitHub PATs are passed in-memory ephemerally and never written to `.git/config` on disk.
- **Log Sanitization**: All subprocess logs are passed through regex sanitizers to mask sensitive tokens.
- **Never commit `.env`**: Secrets and tokens are strictly ignored by `.gitignore`.
- **Restricted File Permissions**: Run `chmod 600 .env` on the host to protect credentials.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `aerial-net` bridge network.
