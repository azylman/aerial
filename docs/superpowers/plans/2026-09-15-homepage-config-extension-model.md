# Implementation Plan: Homepage Configuration Extension Model

## 1. Overview
Implement a robust, resilient two-repository extension model for the Homepage dashboard (`ghcr.io/gethomepage/homepage`). The core engine repository (`azylman/aerial`) provides base configurations, defaults, and layout rules, while the private user repository (`azylman/aerial-config`) cleanly extends them with personal services, widgets, bookmarks, and style overrides. We extract the brittle multiline shell logic from `docker-compose.yml` into a decoupled, testable standalone Node.js merge utility and container entrypoint within `azylman/aerial`.

## 2. Problem Statement & Threat Model
Currently, `docker-compose.yml` embeds a 25-line inline shell script into the `homepage` container entrypoint. This entrypoint performs a naive `cp -f` overwrite of configuration files (`services.yaml`, `widgets.yaml`, `bookmarks.yaml`, `settings.yaml`, `docker.yaml`) from `/share/aerial-config/homepage/` into `/config/`. 

This has several critical failure modes:
1. **Destructive Overwrite**: If the user configuration defines a file (e.g. `services.yaml` or `widgets.yaml`), it completely obliterates any core configuration defined in `azylman/aerial`, preventing core widgets or services from coexisting with user additions.
2. **Cascading Failure & Single Point of Failure (SPOF)**: `proxy` has `depends_on: homepage: condition: service_healthy`. If the homepage entrypoint script crashes, `autoheal` enters an infinite restart loop and `proxy` fails to start, knocking out the entire system gateway (Aerial Command HUD, System Docs, AgentsView).
3. **Missing Schema Invariants & Upstream Quirks**: Homepage strictly requires specific YAML structures (e.g. nested arrays in `bookmarks.yaml`, singleton deduplication in `widgets.yaml`, `docker.yaml` connectivity). Naive file copying ignores these relationships.
4. **Filesystem Traps**: Attempting to overwrite existing symlinks (such as `custom.css`) in a restart scenario causes `EROFS` on read-only mounts, while moving temp files across `/tmp` and `/config` triggers `EXDEV`.
5. **Brittle Compose Maintenance**: Maintaining 25+ lines of inline shell logic inside a YAML multiline block requires double-dollar variable escaping (`$$VAR`), prevents IDE linting/ShellCheck, and risks silent syntax breakages.

## 3. Architecture & Remediation Design

### 3.1 Separation of Concerns
- **Core Repository (`azylman/aerial`)**:
  - Location: `homepage/config/`
  - Scope: Generic system baseline (`services.yaml`, `widgets.yaml`, `bookmarks.yaml`, `settings.yaml`, `docker.yaml`, `custom.css`), plus the standalone preparation and merge engine (`homepage/lib/merge.js`, `homepage/prepare-config.js`, `homepage/entrypoint.sh`).
  - Auto-discovery: Core Docker containers (Aerial Command HUD, AgentsView, System Docs, Grafana) remain auto-discovered via Docker labels in `docker-compose.yml`.
- **User Configuration Repository (`azylman/aerial-config`)**:
  - Location: `homepage/`
  - Scope: Personal external services (Home Assistant, QNAP NAS), personal bookmarks (Cloudflare Zero Trust, personal GitHub links), local weather coordinates (`widgets.yaml`), and optional CSS tweaks (`custom.css`).

### 3.2 Pure Merge Engine (`homepage/lib/merge.js`)
Zero-dependency CommonJS module operating exclusively on Plain Old JavaScript Objects (POJOs), enabling microsecond host unit testing with standard `node --test` without requiring `npm install` or host `js-yaml`:
- `mergeServices(core, user)`:
  - Normalizes null/undefined/empty inputs to `[]`.
  - Merges list of service groups. If a group key (e.g. `Observability` or `Aerial AI & Mission Control`) exists in both core and user, merges services under that group.
  - Performs service-level deduplication by service key (`Object.keys(svc)[0]`). If user re-declares an existing service name, user properties override core; otherwise unique services are appended.
  - Appends novel user groups after core groups.
- `mergeBookmarks(core, user)`:
  - Normalizes null/undefined/empty inputs to `[]`.
  - Merges bookmark category arrays while strictly preserving Homepage's nested array format:
    `[ { CategoryName: [ { LinkName: [ { href, icon } ] } ] } ]`.
  - Merges links within matching categories; appends novel categories.
- `mergeWidgets(core, user)`:
  - Normalizes null/undefined/empty inputs to `[]`.
  - Concatenates top-level widget lists, deduplicating singleton header widgets (`search`, `openmeteo`, `datetime`, `resources`) by widget type key (preferring user configuration).
- `mergeSettings(core, user)`:
  - Normalizes null/undefined/empty inputs to `{}`.
  - Shallow-merges root configuration keys (`theme`, `color`, `cardBlur`, `cardOpacity`, `headerStyle`), allowing user overrides.
  - Deep-merges `layout` dictionary mappings so core group column counts (`Aerial AI & Mission Control`, `Observability`) remain anchored at top, with user group column counts appended.
- `mergeDocker(core, user)`:
  - Normalizes null/undefined/empty inputs to `{}`.
  - Deep-merges Docker host connection blocks, preserving `my-docker: socket: /var/run/docker.sock` from core while incorporating any secondary user Docker daemons.
- `mergeCustomCss(coreContent, userContent)`:
  - Pure string concatenation: returns core CSS + "\n" + user CSS if both present, otherwise whichever is defined.

### 3.3 Imperative Shell (`homepage/prepare-config.js`)
CommonJS runner executing inside the container with defensive I/O:
- Module Resolution: Uses CommonJS `require('js-yaml')`, dynamically resolving candidate paths (`/app/node_modules/js-yaml`, etc.).
- Configurable Paths: Reads `CORE_CONFIG_DIR` (default `/app/core-homepage/config`), `USER_CONFIG_DIR` (default `/share/aerial-config/homepage`), and `TARGET_CONFIG_DIR` (default `/config`).
- Per-File Fault Isolation: Each target file (`services.yaml`, `widgets.yaml`, `bookmarks.yaml`, `settings.yaml`, `docker.yaml`, `custom.css`) is processed in an isolated `try / catch` block. A syntax error in `bookmarks.yaml` falls back ONLY bookmarks to core baseline without corrupting or resetting `services.yaml`.
- Atomic In-Directory Writes: Writes to a same-directory temporary file (`/config/.<file>.tmp.<pid>`) with permissions `0644`, followed by `fs.renameSync()`. This prevents `EXDEV` cross-device errors across volume boundaries, prevents Chokidar from triggering partial reads, and cleanly overwrites legacy symlinks without `EROFS`.
- Diagnostic Logging: Logs warnings to stderr and appends details to `/config/merge-errors.log` on any failure.
- Graceful Exit: Always exits with code `0` after writing all available configurations, preventing container restart loops.

### 3.4 Container Entrypoint (`homepage/entrypoint.sh`)
Executable POSIX shell script (`chmod +x`, LF line endings):
```sh
#!/bin/sh
set -e

mkdir -p /config

# Execute config preparation with explicit raw-copy fallback
if ! NODE_PATH=/app/node_modules node /app/core-homepage/prepare-config.js; then
    echo "🚨 [Homepage Entrypoint] Config preparation failed! Falling back to raw core baseline..." >&2
    cp -rf /app/core-homepage/config/. /config/ 2>/dev/null || true
fi

# Chown config directory for node user if running as root
if [ "$(id -u)" = "0" ]; then
    chown -R node:node /config 2>/dev/null || true
fi

# Pass through arguments or default to server.js
if [ $# -eq 0 ]; then
    set -- node server.js
fi
exec /usr/local/bin/docker-entrypoint.sh "$@"
```

### 3.5 Docker Compose Refactoring (`docker-compose.yml`)
- Volume Mount for `homepage`:
  - From: `${AERIAL_HOST_PROJECT_DIR:-...}/homepage/config:/app/core-config:ro`
  - To: `${AERIAL_HOST_PROJECT_DIR:-...}/homepage:/app/core-homepage:ro`
- Entrypoint:
  - From: 25-line multiline `/bin/sh -c` string
  - To: `["/bin/sh", "/app/core-homepage/entrypoint.sh"]`

### 3.6 Automated Host Unit Tests (`homepage/test/merge.test.js`)
Host-native unit tests using Node.js built-in test runner (`node --test`):
- `mergeServices`: group merging, novel groups, intra-group service deduplication, empty inputs.
- `mergeBookmarks`: nested array format preservation, category merging, novel categories.
- `mergeWidgets`: singleton deduplication (`search`, `openmeteo`), array appending.
- `mergeSettings`: deep object merge, layout ordering, styling key overrides.
- `mergeDocker`: multi-host merging, socket preservation.
- `mergeCustomCss`: string concatenation and fallbacks.

### 3.7 Monorepo Verification Integration (`scripts/verify.sh`)
Update `scripts/verify.sh` to include:
- Node syntax checks (`node --check`) for `homepage/lib/merge.js` and `homepage/prepare-config.js`.
- Automated test execution (`node --test homepage/test/merge.test.js`) during staged and full verification sweeps.

## 4. Complexity Tier & Review Gates
- **Complexity Tier**: **Tier 2** (Standard feature & container entrypoint refactoring, ~150 LOC).
- **Review Panel (the girl gang)**: 
  - Stage 2: 4-Expert Review Panel (Systems Architect, Dashboard Specialist, Platform Specialist, Adversarial Systems Critic) completed and remediated.
  - Stage 3: Human Review Checkpoint bypassed autonomously per Tier 2 guidelines and explicit user authorization ("cool run it by the girl gang and then go").
  - Stage 4: Continuous TDD implementation (tests first, zero mid-task pauses).
  - Stage 5: Solo Devil's Advocate diff audit before PR submission.
