# Gundam Stack

An autonomous personal AI assistant system running on Docker.

Gundam provides a multi-agent, tool-enabled AI assistant accessible via Discord and API, with persistent multi-turn memory, Home Assistant integration, GitHub operations, and host Docker diagnostics.

---

## 1. System Architecture

```
                       ┌─────────────────────────┐
                       │     Discord Gateway     │
                       └───────────┬─────────────┘
                                   │ Mentions / Thread messages
                                   ▼
                       ┌─────────────────────────┐
                       │     discord-funnel      │
                       └───────────┬─────────────┘
                                   │ POST /api/prompt
                                   ▼
                       ┌─────────────────────────┐
                       │      gundam-brain       │ ◄── (SQLite /data/gundam.db)
                       └───┬───────────┬───────┬─┘
                           │           │       │
             ┌─────────────┘           │       └─────────────┐
             ▼                         ▼                     ▼
┌─────────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
│      ha-mcp-server      │ │    discord-mcp     │ │     docker-mcp     │
│ (Home Assistant Webhook)│ │ (Port 4001 /mcp)   │ │  (Port 4002 /mcp)  │
└─────────────────────────┘ └────────────────────┘ └────────────────────┘
```

### Component Repositories

1. **[Gundam Brain (`azylman/gundam-brain`)](https://github.com/azylman/gundam-brain)**:
   Execution runner wrapping headless Antigravity CLI (`agy`) with SQLite-backed multi-turn thread memory (`/data/gundam.db`) and remote MCP client orchestration.
2. **[Discord Funnel (`azylman/ha-discord-funnel-addon`)](https://github.com/azylman/ha-discord-funnel-addon)**:
   Inbound event gateway connecting to Discord Gateway, generating deterministic conversation UUIDs, and forwarding prompts to Gundam Brain.
3. **[Discord MCP Server (`azylman/ha-addon-discord-mcp`)](https://github.com/azylman/ha-addon-discord-mcp)**:
   Outbound Model Context Protocol (MCP) server over Streamable HTTP exposing Discord messaging, thread creation, and channel reading tools.
4. **[Docker MCP Server (`azylman/ha-docker-mcp-addon`)](https://github.com/azylman/ha-docker-mcp-addon)**:
   Outbound MCP server connected to `/var/run/docker.sock` exposing host container inspection, resource stats, and log fetching.

---

## 2. Quickstart Setup

### Prerequisites
- Docker Engine 24+ & Docker Compose v2+
- Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))
- Discord Bot Token (with Message Content and Server Members intents enabled)

### Step 1: Clone Repository
```bash
git clone https://github.com/azylman/gundam.git
cd gundam
```

### Step 2: Configure Environment Variables
Copy the template file to `.env`:
```bash
cp .env.example .env
```
Edit `.env` and provide your secrets:
```ini
GEMINI_API_KEY=AQ.Ab8RN...
DISCORD_BOT_TOKEN=MTU0Mj...
GITHUB_PAT=github_pat_11AAG...
HA_TOKEN=http://192.168.1.14:8123/api/webhook/mcp_...
```

### Step 3: Launch Stack
```bash
docker compose up -d
```

### Step 4: Verify Health
```bash
docker compose ps
```
Check container logs:
```bash
docker compose logs -f gundam-brain
```

---

## 3. Operational Commands

| Action | Command |
| --- | --- |
| Start all services | `docker compose up -d` |
| Stop all services | `docker compose down` |
| View live logs | `docker compose logs -f` |
| View transcripts & memory | `curl http://localhost:8088/api/transcripts` |
| Restart single service | `docker compose restart gundam-brain` |
| Update images | `docker compose pull && docker compose up -d` |

---

## 4. Security & Best Practices

- **Never commit `.env`**: Secrets, API keys, and tokens belong only in the `.env` file, which is listed in `.gitignore`.
- **Restrict File Permissions**: Run `chmod 600 .env` on the host to ensure only the host owner can read secrets.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `gundam-net` bridge network.
