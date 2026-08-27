# Gundam System Architecture & Repository Guidelines

## 1. System Overview & Component Topology
This repository (`azylman/gundam`) is the root orchestration repository for the Gundam AI Assistant system, defining the multi-container Docker Compose topology:

1. **Inbound Funnel** (`discord-funnel`):
   - Inbound event gateway connecting to Discord.
   - Forwards prompts to `http://gundam-brain:8080/api/prompt`.
   - Repository: `https://github.com/azylman/ha-discord-funnel-addon`

2. **Execution Brain** (`gundam-brain`):
   - Runner executing headless Antigravity CLI (`agy`).
   - SQLite-backed conversation mapping (`/data/gundam.db`) for multi-turn thread continuity.
   - Discovers MCP servers via `MCP_CONFIG` environment variable.
   - Repository: `https://github.com/azylman/gundam-brain`

3. **Discord MCP Server** (`discord-mcp`):
   - Remote Streamable HTTP endpoint (`http://discord-mcp:4001/mcp`).
   - Exposes Discord actions (`discord_create_thread`, `discord_send`, `discord_read_messages`).
   - Repository: `https://github.com/azylman/ha-addon-discord-mcp`

4. **Docker MCP Server** (`docker-mcp`):
   - Remote Streamable HTTP endpoint (`http://docker-mcp:4002/mcp`).
   - Connected to `/var/run/docker.sock` for container and host inspection.
   - Repository: `https://github.com/azylman/ha-docker-mcp-addon`

## 2. Invariants & Rules
- **Zero In-Image MCPs**: All MCP servers must run as standalone network endpoints; do not install local stdio MCP packages inside `gundam-brain`.
- **Secrets Isolation**: Secrets (API keys, bot tokens, webhooks, PATs) must NEVER be committed to Git. They are configured via `.env` files and referenced via environment variables in `docker-compose.yml`.
- **Private Bridge Networking**: All inter-service communication happens over the `gundam-net` Docker bridge network using container service DNS names (`http://gundam-brain:8080`, `http://discord-mcp:4001`, `http://docker-mcp:4002`).
