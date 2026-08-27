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
     Discord Platform                       supergateway (Translation Proxy)
                                                           ?
                                                           ?
                                            Official Docker MCP (mcp/docker)
                                                           ?
                                                           ?
                                                  /var/run/docker.sock
```

---

## 2. Translation Layers & Zero Custom Code

Following the philosophy of writing minimal custom code and relying on translation layers:
- **`discord-mcp`**: Clones and builds upstream [`mcp-discord`](https://github.com/vianaz/mcp-discord) with thread support from `azylman/ha-addon-discord-mcp`.
- **`docker-mcp`**: Uses **`supergateway`** to translate the official **`mcp/docker`** stdio container into a Streamable HTTP (`/mcp`) microservice over `/var/run/docker.sock`. Zero custom application code.

---

## 3. Component Modules

1. **[Gundam Brain (`brain/`)](https://github.com/azylman/gundam/tree/main/brain)**:
   Execution runner wrapping headless Antigravity CLI (`agy`) with SQLite-backed multi-turn thread memory (`/data/gundam.db`) and remote MCP client orchestration.
2. **[Discord Funnel (`discord-funnel/`)](https://github.com/azylman/gundam/tree/main/discord-funnel)**:
   Inbound event gateway connecting to Discord Gateway, generating deterministic conversation UUIDs, and forwarding prompts to Gundam Brain.
3. **[Discord MCP Server (`discord-mcp/`)](https://github.com/azylman/gundam/tree/main/discord-mcp)**:
   Outbound Model Context Protocol (MCP) server over Streamable HTTP exposing Discord messaging, thread creation, and channel reading tools.
4. **[Docker MCP Server (`docker-mcp/`)](https://github.com/azylman/gundam/tree/main/docker-mcp)**:
   Universal `supergateway` proxy wrapping the official Docker MCP server (`mcp/docker`) over the host Docker socket.

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
| Update images & rebuild | `docker compose build && docker compose up -d` |

---

## 6. Security & Best Practices

- **Never commit `.env`**: Secrets, API keys, and tokens belong only in the `.env` file, which is listed in `.gitignore`.
- **Restrict File Permissions**: Run `chmod 600 .env` on the host to ensure only the host owner can read secrets.
- **Isolated Bridge Network**: All container-to-container traffic operates on the private `gundam-net` bridge network.
