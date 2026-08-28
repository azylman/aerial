# Aerial Stack

An autonomous personal AI assistant system running natively on Docker, named after Gundam Aerial.

Aerial provides a multi-agent, tool-enabled AI assistant accessible via Discord and HTTP API, with persistent multi-turn SQLite memory, Home Assistant integration, GitHub operations, host Docker infrastructure inspection, and an extensible architecture for custom skills, MCP tools, and sidecar containers.

---

## 1. System Architecture

```text
???????????????????????????????????????????????????????????????????????????????
?                               Discord Gateway                               ?
???????????????????????????????????????????????????????????????????????????????
                                       ? Realtime Gateway Events & Mentions
                                       ? Continuous Typing Indicator Refresh
???????????????????????????????????????????????????????????????????????????????
?                                Aerial Brain                                 ?
?  . In-process Discord Funnel & Gateway Worker                               ?
?  . Headless Antigravity Agent Engine (agy)                                  ?
?  . SQLite Multi-Turn Thread Memory (/data/aerial.db)                         ?
?  . Dynamic Built-in & User Custom Skills Discovery                          ?
?  . Docker-out-of-Docker (DooD) & Self-Update Runner                          ?
???????????????????????????????????????????????????????????????????????????????
               ?                       ?                      ?
??????????????????????????? ??????????????????????? ?????????????????????????
?       discord-mcp       ? ?       docker-mcp    ? ?       github-mcp      ?
? (Port 4001: Streamable) ? ? (Port 4002: Proxy)  ? ? (Port 4003: Proxy)    ?
??????????????????????????? ??????????????????????? ?????????????????????????
               ?                       ?                      ?
       Discord REST API        Host Docker Socket       GitHub Copilot MCP
                           (/var/run/docker.sock)
```

---

## 2. Extensibility Guide

Aerial is designed to be easily extended across four layers: **Instructions/Persona**, **Skills**, **MCP Servers**, and **Containers**.

```text
???????????????????????????????????????????????????????????????????????????????????????????????
?                                  4 WAYS TO EXTEND AERIAL                                    ?
???????????????????????????????????????????????????????????????????????????????????????????????
? 1. INSTRUCTIONS/PERSONA    ? 2. SKILLS              ? 3. MCP SERVERS        ? 4. CONTAINERS ?
? AGENTS.md user overrides   ? Teaching procedures    ? Connecting APIs/SDKs  ? Adding sidecars
???????????????????????????????????????????????????????????????????????????????????????????????
```

---

### Layer 1: Custom Instructions & Persona (`AGENTS.md`)

Aerial loads workspace instructions from two key files:
- **`SYSTEM.md`** (Tracked in Git): Contains default system specifications, core capabilities, and safety guidelines.
- **`AGENTS.md`** (Gitignored User Override): Create `AGENTS.md` in the repository root to define custom persona rules, tone preferences, or private operational context. Instructions in `AGENTS.md` take priority over `SYSTEM.md`.

---

### Layer 2: Adding Custom Skills

Skills use **Progressive Disclosure**-Aerial only loads skill titles and descriptions into context, reading full runbooks on-demand when relevant.

#### A. Built-in Skills (Tracked in Git)
Place skills in `.agents/skills/` in your repository:
```text
.agents/skills/
??? ha-operations/
?   ??? SKILL.md
??? self-improvement/
    ??? SKILL.md
```
These are baked into the `brain` image during build and tracked in version control.

#### B. User Custom Skills (Runtime / Drop-in)
Drop new skill folders directly into `./skills/` on your host machine (or `/data/skills/` inside the container):
```text
skills/
??? my-custom-runbook/
    ??? SKILL.md
    ??? scripts/
```
On startup, `brain` automatically discovers and links all custom skills in `/data/skills/` without requiring code changes or image rebuilds.

#### Skill File Structure (`SKILL.md`)
```markdown
---
name: my-custom-skill
description: Use this skill whenever the user asks to perform XYZ operations.
---

# My Custom Skill Runbook

## Steps
1. Execute query tool with parameters...
2. Verify results...
```

---

### Layer 3: Adding Custom MCP Servers

Aerial connects to external Model Context Protocol (MCP) servers over Streamable HTTP / SSE.

#### Built-in Tool Autodiscovery
By default, Aerial automatically mounts:
- **`discord`** (`http://discord-mcp:4001/mcp`)
- **`docker`** (`http://docker-mcp:4002/mcp`)
- **`github`** (`http://github-mcp:4003/mcp` when `GITHUB_PAT` is set)
- **`ha-mcp`** (Home Assistant webhook tools when `HA_TOKEN` is configured)

#### Custom MCP Overrides (`mcp.config.json`)
To define additional MCP tools, copy `mcp.config.example.json` to `mcp.config.json` (ignored by Git):
```bash
cp mcp.config.example.json mcp.config.json
```
Aerial automatically interpolates `${VARIABLE_NAME}` from your `.env` file at startup:

```json
{
  "mcpServers": {
    "brave-search": {
      "serverUrl": "http://brave-mcp:4005/mcp"
    },
    "custom-remote-api": {
      "serverUrl": "https://mcp.example.com/mcp",
      "headers": {
        "Authorization": "Bearer ${CUSTOM_API_KEY}"
      }
    }
  }
}
```

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

## 3. Self-Update & Self-Improvement

Aerial has Docker-out-of-Docker (DooD) enabled and can update itself or other containers autonomously when prompted in Discord:

```text
User: "Aerial, pull the latest code updates and rebuild"
Aerial:
  1. Runs git pull on /share/aerial
  2. Inspects git diffs to detect changed services
  3. Rebuilds modified microservices (docker compose up -d --build <service>)
  4. If brain changed: builds image, sends Discord response, and cleanly restarts container
```

---

## 4. Component Modules

| Service | Port | Description |
| --- | --- | --- |
| **`brain`** | `8088` | Go execution daemon running `agy`, SQLite memory, Discord funnel, and Docker controller. |
| **`discord-mcp`** | `4001` | Outbound MCP server providing Discord messaging, thread creation, and channel reading tools. |
| **`docker-mcp`** | `4002` | `supergateway` proxy wrapping official Docker MCP (`mcp/docker`) over the host socket. |
| **`github-mcp`** | `4003` | `supergateway` proxy wrapping GitHub MCP server with PAT authentication. |
| **`agentsview`** | `8089` | Web UI for visualizing agent transcripts, session history, and execution timelines. |

---

## 5. Quickstart Setup

### Prerequisites
- Docker Engine 24+ & Docker Compose v2+
- Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))
- Discord Bot Token (with Message Content and Server Members intents enabled)

### Step 1: Clone Repository
```bash
git clone https://github.com/azylman/aerial.git
cd aerial
```

### Step 2: Configure Environment Variables
```bash
cp .env.example .env
```
Edit `.env` and provide your credentials:
```ini
GEMINI_API_KEY=your_gemini_api_key_here
DISCORD_BOT_TOKEN=your_discord_bot_token_here
GITHUB_PAT=your_github_personal_access_token_here
HA_TOKEN=http://192.168.1.14:8123/api/webhook/mcp_your_id
```

### Step 3: Launch Stack
```bash
docker compose up -d
```

### Step 4: Verify Health
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

- **Never commit `.env` or `mcp.config.json`**: Secrets, API keys, and custom tokens are listed in `.gitignore`.
- **Restrict File Permissions**: Run `chmod 600 .env` on the host to ensure only the host owner can read secrets.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `aerial-net` bridge network.