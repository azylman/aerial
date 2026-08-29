# Implementation Plan: Hardened YAML Configuration, GitSync Auto-Bootstrapping & Discord System Channel Alerts

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple all non-secret configuration (model, timeout, timezone, system channel, git-sync remotes, custom MCP servers) into `azylman/aerial-config/config.yaml`, auto-bootstrap git tracking with secure non-persisted token authentication, seed the private `aerial-config` repository, and send Discord alert messages to `#aerial-dev` if invalid YAML configurations are detected.

**Adversarial Hardening & Features Applied:**
1. **Zero Plaintext PAT Leakage**: `EnsureRepo` and `SyncRepo` keep `.git/config` remote URLs clean (`https://github.com/azylman/aerial-config.git`) and pass credentials via dynamic in-memory headers (`http.extraHeader`). All subprocess logs are scrubbed via token regex sanitizers.
2. **Non-Destructive Adoption Protocol**: If `/share/aerial-config` already exists on disk with unversioned files, `EnsureRepo` performs in-place `git init` + `git remote add origin` + `git fetch` without wiping existing prompt files or skills.
3. **Dynamic WorkerPool Reconfiguration**: `WorkerPool` implements thread-safe `UpdateRuntimeConfig(model, timeout)` so model and timeout changes in `config.yaml` apply immediately to the next turn without container restarts.
4. **Full-Struct LKGC & Discord Alerting**: `config.go` caches the parsed `Config` struct in memory so malformed YAML syntax preserves the Last Known Good Configuration AND automatically dispatches a system alert notification to `#aerial-dev` via Discord.
5. **Startup Bootstrapping Order**: `EnsureRepo` runs *before* initial `LoadConfig()` and `NewWorkerPool()` in `main.go`.
6. **Timezone Dynamic Binding**: `scheduler.GetDefaultTimezone()` dynamically references `config.GetTimezone()`.

---

### Task 1: Seed Private `azylman/aerial-config` Repository on GitHub
**Files:**
- GitHub repo: `azylman/aerial-config`
  - `config.yaml`:
    ```yaml
    # Aerial Configuration (aerial-config/config.yaml)
    model: "Gemini 3.6 Flash (Low)"
    timeout_minutes: 15
    timezone: "America/Los_Angeles"
    system_channel: "aerial-dev"

    git_sync:
      enabled: true
      interval: "60s"
      config_repo_url: "https://github.com/azylman/aerial-config.git"
      repositories:
        - "/share/aerial-config"
        - "/share/aerial"
    ```
  - `AGENTS.md`: User persona, behavioral rules, and tone overrides.
  - `custom-skills/smart-home/SKILL.md`: Starter user custom skill.
  - `.env.example`: Secret tokens documentation (`GEMINI_API_KEY`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `HA_TOKEN`).
  - `.gitignore`: Ignore `.env`, `*.tmp`, `*.swp`.

---

### Task 2: Hardened YAML Configuration Parser, Discord Alerts & WorkerPool Hot-Reloading
**Files:**
- Modify: `brain/go.mod` (add `gopkg.in/yaml.v3`)
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/config/config_test.go`
- Modify: `brain/pkg/delivery/delivery.go`
- Modify: `brain/pkg/delivery/delivery_test.go`
- Modify: `brain/pkg/queue/queue.go`
- Modify: `brain/pkg/queue/queue_test.go`
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/main.go`

**Requirements:**
1. In `config.go`:
   - Parse `config.yaml` using `gopkg.in/yaml.v3` with `os.ExpandEnv`.
   - Support `system_channel` (defaults to `"aerial-dev"`).
   - Full struct Last Known Good Configuration (LKGC) fallback on corrupted or invalid YAML.
   - Callback / hook for configuration parse errors so caller can notify Discord.
   - Export `GetTimezone()`, `GetSystemChannel()`, and `GetRuntimeConfig()`.
2. In `delivery.go`:
   - Add `ResolveChannelByNameOrID(s *discordgo.Session, nameOrID string) (string, error)` to find channel by name (e.g. `aerial-dev` or `#aerial-dev`) or ID across connected guilds.
   - Add `SendSystemAlert(s *discordgo.Session, channelNameOrID, title, body string) error`.
3. In `queue.go`:
   - Add thread-safe `UpdateRuntimeConfig(model string, timeoutMinutes int)` with `sync.RWMutex` to `WorkerPool`.
4. In `scheduler.go`:
   - Bind `GetDefaultTimezone()` dynamically to `config.GetTimezone()`.
5. In `main.go`:
   - Wire fileWatcher and gitsync callbacks to re-invoke `LoadConfig()`, refresh `EnsureAgySettings()`, `EnsureMcpConfig()`, and call `pool.UpdateRuntimeConfig()`.
   - If `config.yaml` is corrupted/invalid, trigger `delivery.SendSystemAlert(dgSession, systemChannel, ...)` to post an alert in `#aerial-dev`.
6. Unit tests with `-race` across `config`, `delivery`, `queue`, and `scheduler`.

---

### Task 3: Secure Auto-Bootstrapping GitSync in `brain/pkg/gitsync`
**Files:**
- Modify: `brain/pkg/gitsync/gitsync.go`
- Modify: `brain/pkg/gitsync/gitsync_test.go`
- Modify: `brain/main.go`

**Requirements:**
1. In `gitsync.go`:
   - Implement `EnsureRepo(ctx context.Context, repoPath, repoUrl, token string) error`:
     - Clean remote URL: Never write PAT into `.git/config`.
     - Per-command auth: pass `-c http.extraHeader=AUTHORIZATION: basic <base64>` during clone/fetch/pull.
     - Non-destructive adoption on existing non-empty directory (`git init` + `git remote add origin` + `git fetch`).
     - Log Sanitizer: regex-scrub all tokens from command output before logging.
   - Update `SyncRepo` to use dynamic auth headers and regex sanitization.
2. In `main.go`:
   - Move `EnsureRepo` for `/share/aerial-config` *before* initial `LoadConfig()` and `NewWorkerPool()`.
3. Unit tests with `-race` verifying non-destructive adoption, token log redaction, and sync loop.

---

### Task 4: Deployment & Live Stack Verification on MiniPC (`192.168.1.14`)
**Requirements:**
1. Push `azylman/aerial` changes to `origin/main` and verify CI path filtering.
2. Update `/share/aerial` on MiniPC and rebuild `aerial-brain`.
3. Verify `aerial-brain` bootstraps `/share/aerial-config` as a git repository tracking `azylman/aerial-config` with clean remote URL.
4. Test modifying `config.yaml` on GitHub (e.g. changing model or timeout) $ightarrow$ verify `aerial-brain` pulls, hot-reloads `WorkerPool` runtime settings and rules within 60s with 0 container restarts.
5. Test writing a broken YAML file to `/share/aerial-config/config.yaml` $ightarrow$ verify `aerial-brain` logs LKGC warning and dispatches an alert message to `#aerial-dev` in Discord!
6. Verify all 9 containers remain healthy.
