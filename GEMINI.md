# Aerial System Architecture & Repository Guidelines

## 1. System Overview & Component Topology
This repository (`azylman/aerial`) is the root orchestration repository for the Aerial AI Assistant system, defining the standalone Docker Compose multi-container topology:

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
                       ?          brain          ? ??? (SQLite /data/aerial.db)
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

1. **Inbound Funnel** (`discord-funnel/`):
   - Inbound event gateway connecting to Discord Gateway.
   - Generates deterministic conversation UUIDs for thread continuity and forwards prompts to `http://brain:8080/api/prompt`.

2. **Execution Brain** (`brain/`):
   - Execution runner executing headless Antigravity CLI (`agy`).
   - SQLite-backed conversation mapping (`/data/aerial.db`) for multi-turn thread continuity.
   - Dynamically loads and expands MCP configurations (`mcp.config.json` / environment variables with `os.ExpandEnv`).

3. **Discord MCP Server** (`discord-mcp/`):
   - Remote Streamable HTTP endpoint (`http://discord-mcp:4001/mcp`).
   - Clones upstream `mcp-discord` with thread tools from `azylman/ha-addon-discord-mcp`.

4. **Docker MCP Server** (`docker-mcp/`):
   - Remote Streamable HTTP endpoint (`http://docker-mcp:4002/mcp`).
   - Uses `supergateway` to expose official `mcp/docker` over `/var/run/docker.sock` with zero custom code.

5. **Agentsview Dashboard** (`agentsview`):
   - Web observability dashboard running on port `8089` (`http://192.168.1.14:8089`).
   - Indexes and renders Antigravity session transcripts and step traces.

## 2. Invariants & Architectural Rules
- **Extensible Configuration**: Custom MCP servers belong in `mcp.config.json` with `${ENV_VAR}` placeholders or via `.env` credentials.
- **Translation Over Custom Code**: Wherever possible, rely on upstream packages and generic translation proxies (`supergateway`) rather than custom server implementations.
- **Zero In-Image MCPs**: All MCP servers must run as standalone network endpoints; do not install local stdio MCP packages inside `brain`.
- **Secrets Isolation**: Secrets (API keys, bot tokens, webhooks, PATs) must NEVER be committed to Git. They are configured via `.env` files and referenced via environment variables in `docker-compose.yml`.
- **Private Bridge Networking**: All inter-service communication happens over the `aerial-net` Docker bridge network using container service DNS names (`http://brain:8080`, `http://discord-mcp:4001`, `http://docker-mcp:4002`).
