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
                       ?          brain          ? ??? (SQLite /data/aerial.db)
                       ? (Built-in Discord Funnel)
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

1. **Execution Brain & Built-in Funnel** (`brain/`):
   - Execution runner executing headless Antigravity CLI (`agy`).
   - Integrated Discord Gateway worker (`brain/funnel.go`) with continuous typing indicator refresh until response generation finishes.
   - SQLite-backed conversation mapping (`/data/aerial.db`) for multi-turn thread continuity.
   - Dynamically loads and expands MCP configurations (`mcp.config.json` / environment variables with `os.ExpandEnv`).

2. **Discord MCP Server** (`discord-mcp/`):
   - Remote Streamable HTTP endpoint (`http://discord-mcp:4001/mcp`).
   - Clones upstream `mcp-discord` with thread tools from `azylman/ha-addon-discord-mcp`.

3. **Docker MCP Server** (`docker-mcp/`):
   - Remote Streamable HTTP endpoint (`http://docker-mcp:4002/mcp`).
   - Uses `supergateway` to expose official `mcp/docker` over `/var/run/docker.sock` with zero custom code.

4. **GitHub MCP Server** (`github-mcp/`):
   - Remote Streamable HTTP endpoint (`http://github-mcp:4003/mcp`).
   - Exposes GitHub operations via `supergateway` and `ghcr.io/github/github-mcp-server`.

5. **Agentsview Dashboard** (`agentsview`):
   - Web observability dashboard running on port `8089` (`http://192.168.1.14:8089`).
   - Indexes and renders Antigravity session transcripts and step traces.

## 2. Invariants & Architectural Rules
- **Extensible Configuration**: Custom MCP servers belong in `mcp.config.json` with `${ENV_VAR}` placeholders or via `.env` credentials.
- **Translation Over Custom Code**: Wherever possible, rely on upstream packages and generic translation proxies (`supergateway`) rather than custom server implementations.
- **Zero In-Image MCPs**: All MCP servers must run as standalone network endpoints; do not install local stdio MCP packages inside `brain`.
- **Secrets Isolation**: Secrets (API keys, bot tokens, webhooks, PATs) must NEVER be committed to Git. They are configured via `.env` files and referenced via environment variables in `docker-compose.yml`.
- **Private Bridge Networking**: All inter-service communication happens over the `aerial-net` Docker bridge network using container service DNS names (`http://brain:8080`, `http://discord-mcp:4001`, `http://docker-mcp:4002`).
