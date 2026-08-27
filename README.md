# Gundam Stack

An autonomous personal AI assistant system running natively on Docker.

Gundam provides a multi-agent, tool-enabled AI assistant accessible via Discord and HTTP API, with persistent multi-turn SQLite memory, GitHub workspace operations, and full host Docker infrastructure inspection.

---

## 1. System Architecture

```
                       ???????????????????????????
                       ?     Discord Gateway     ?
                       ???????????????????????????
                                   ? Mentions / Thread messages
                                   ?
                       ???????????????????????????
                       ?     discord-funnel      ?
                       ???????????????????????????
                                   ? POST /api/prompt
                                   ?
                       ???????????????????????????
                       ?      gundam-brain       ? ??? (SQLite /data/gundam.db)
                       ???????????????????????????
                           ?                 ?
             ???????????????                 ???????????????
             ?                                             ?
???????????????????????????                   ???????????????????????????
?       discord-mcp       ?                   ?       docker-mcp        ?
? (Port 4001: Discord API)?                   ? (Port 4002: Docker Host)?
???????????????????????????                   ???????????????????????????
             ?                                             ?
             ?                                             ?
     Discord Platform                               /var/run/docker.sock
                                                  (Containers, Logs, Stats)
```

---

## 2. How Docker Fits Into the Architecture

Docker serves two essential roles in the Gundam ecosystem:

1. **The Application Runtime**:
   - All Gundam microservices run as containerized services connected via an isolated, internal bridge network (`gundam-net`).
   - Inter-service communication uses internal Docker DNS (`http://gundam-brain:8080`, `http://discord-mcp:4001`, `http://docker-mcp:4002`).

2. **The Controlled Environment (Infrastructure as a Tool)**:
   - Through `docker-mcp`, the host Docker socket (`/var/run/docker.sock`) is mounted into the MCP server container.
   - Gundam Brain has direct native tool access to inspect, monitor, and diagnose the host Docker engine:
     - `docker_list_containers`: Discover active/stopped containers on the host.
     - `docker_inspect_container`: Read network settings, mount points, and environment metadata.
     - `docker_get_container_logs`: Fetch live stdout/stderr streams from any container on the host.
     - `docker_container_stats`: Monitor live CPU, RAM, and I/O consumption.
     - `docker_system_df` & `docker_system_info`: Inspect disk usage, image layers, and Docker engine health.

---

## 3. Component Repositories

1. **[Gundam Brain (`azylman/gundam-brain`)](https://github.com/azylman/gundam-brain)**:
   Execution runner wrapping headless Antigravity CLI (`agy`) with SQLite-backed multi-turn thread memory (`/data/gundam.db`) and remote MCP client orchestration.
2. **[Discord Funnel (`azylman/ha-discord-funnel-addon`)](https://github.com/azylman/ha-discord-funnel-addon)**:
   Inbound event gateway connecting to Discord Gateway, generating deterministic conversation UUIDs, and forwarding prompts to Gundam Brain.
3. **[Discord MCP Server (`azylman/ha-addon-discord-mcp`)](https://github.com/azylman/ha-addon-discord-mcp)**:
   Outbound Model Context Protocol (MCP) server over Streamable HTTP exposing Discord messaging, thread creation, and channel reading tools.
4. **[Docker MCP Server (`azylman/ha-docker-mcp-addon`)](https://github.com/azylman/ha-docker-mcp-addon)**:
   Outbound MCP server connected to `/var/run/docker.sock` exposing host container inspection, resource stats, and log fetching.

---

## 4. Quickstart Setup

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
Edit `.env` and provide your credentials:
```ini
GEMINI_API_KEY=your_gemini_api_key_here
DISCORD_BOT_TOKEN=your_discord_bot_token_here
GITHUB_PAT=your_github_personal_access_token_here
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

## 5. Operational Commands

| Action | Command |
| --- | --- |
| Start all services | `docker compose up -d` |
| Stop all services | `docker compose down` |
| View live logs | `docker compose logs -f` |
| View transcripts & memory | `curl http://localhost:8088/api/transcripts` |
| Restart single service | `docker compose restart gundam-brain` |
| Update images | `docker compose pull && docker compose up -d` |

---

## 6. Security & Best Practices

- **Never commit `.env`**: Secrets, API keys, and tokens belong only in the `.env` file, which is listed in `.gitignore`.
- **Restrict File Permissions**: Run `chmod 600 .env` on the host to ensure only the host owner can read secrets.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `gundam-net` bridge network.
