# Decouple Home Assistant to Private Config & Add Config-Driven Docker Compose Override Plan

**Goal:** Decouple the Home Assistant MCP connection and `ha-operations` skill into the private `azylman/aerial-config` repository, strip all Home Assistant/smart-home references from the public repositories (`azylman/aerial` and `azylman/aerial-config-example`), and enable `docker-compose.override.yml` synchronization directly from the config repository.

**Architecture:**
1. **Private User Config (`azylman/aerial-config`)**: Host `ha-mcp` via `config.yaml` `mcp_servers` using `${HA_TOKEN}` interpolation, and migrate `ha-operations` runbook to `custom-skills/ha-operations/SKILL.md`.
2. **Public Engine (`azylman/aerial`)**: Remove hardcoded `ha-mcp` from `brain/pkg/config/config.go`, delete `.agents/skills/ha-operations/`, clean documentation (`README.md`, `SYSTEM.md`, `.env.example`), and wire automatic symlink synchronization for `docker-compose.override.yml` from `/share/aerial-config`.
3. **Public Example Template (`azylman/aerial-config-example`)**: Replace smart home references with generic MCP server and custom skill examples (`weather-query`), and include an example `docker-compose.override.yml`.
4. **Live Verification**: Re-deploy on MiniPC (`192.168.1.14`), verify hot-reload loads `ha-mcp` from `aerial-config/config.yaml`, verify `ha-operations` skill is loaded from `custom-skills`, and verify `docker-compose.override.yml` symlinking.

**Tech Stack:** Go 1.22, Docker Compose v2, GitHub MCP Server / Git, YAML.

---

## Proposed Tasks

### Task 1: Migrate Home Assistant MCP & Skill into Private `azylman/aerial-config`
- Update `azylman/aerial-config/config.yaml` to include:
  ```yaml
  mcp_servers:
    ha-mcp:
      serverUrl: "${HA_TOKEN}"
  ```
- Copy `.agents/skills/ha-operations/SKILL.md` into `azylman/aerial-config/custom-skills/ha-operations/SKILL.md`.
- Remove legacy `custom-skills/smart-home/` from `azylman/aerial-config` if obsolete.
- Push changes to `azylman/aerial-config:main`.

### Task 2: Scrub Public `azylman/aerial-config-example` Template Repository
- Update `config.yaml` in `aerial-config-example` with generic `mcp_servers` examples (`brave-search`, `custom-api`).
- Replace `custom-skills/smart-home/` with generic `custom-skills/weather-query/SKILL.md`.
- Add `docker-compose.override.yml` example showing how to add sidecar containers from the config repo.
- Clean `AGENTS.md`, `.env.example`, and `README.md` to remove all HA/smart home text.
- Push changes to `azylman/aerial-config-example:main`.

### Task 3: Core Engine Decoupling & Config-Driven Compose Override in `azylman/aerial`
- In `brain/pkg/config/config.go`:
  - Remove hardcoded `HA_TOKEN` / `ha-mcp` from `LoadMCPConfig`.
- In `brain/pkg/gitsync/gitsync.go` and `brain/pkg/watcher/watcher.go` / startup:
  - Add `SyncComposeOverride(configDir, projectDir string)`: Automatically symlinks `/share/aerial-config/docker-compose.override.yml` to `/share/aerial/docker-compose.override.yml` whenever present.
- Delete `.agents/skills/ha-operations/` from `azylman/aerial`.
- In `docker-compose.yml` and `.env.example`: Remove `HA_TOKEN` from default core compose.
- In `SYSTEM.md`, `README.md`, `GEMINI.md`: Remove Home Assistant references and document generic MCP servers + `docker-compose.override.yml` in config repo.
- Run unit tests across all `brain` packages.
- Commit and push to `azylman/aerial:main`.

### Task 4: Deployment & Live Stack Verification on MiniPC (`192.168.1.14`)
- Pull latest changes on MiniPC.
- Re-run `docker compose up -d --build`.
- Verify `aerial-brain` logs:
  - Confirms `ha-mcp` is loaded dynamically from `aerial-config/config.yaml` via `${HA_TOKEN}`.
  - Confirms `ha-operations` skill is symlinked into `/root/.gemini/skills/ha-operations`.
  - Confirms `/share/aerial/docker-compose.override.yml` symlink is managed.
- Verify container health for all 9 services.

---

## Verification Plan

### Automated Tests
- `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./...`
- Assert `LoadMCPConfig` only loads default core services (`scheduler`, `discord`, `docker`, `github`) unless overridden in `config.yaml`.
- Assert `SyncComposeOverride` handles symlinking, updates, and removal cleanly.

### Live Stack Verification
- Run live checks on `192.168.1.14` via SSH.
- Inspect `/root/.gemini/config/mcp_config.json` inside `aerial-brain` container to ensure `ha-mcp` is present and points to the webhook URL from `${HA_TOKEN}`.
- Inspect `/root/.gemini/skills/` to confirm `ha-operations` is linked from `/share/aerial-config/custom-skills/ha-operations`.

