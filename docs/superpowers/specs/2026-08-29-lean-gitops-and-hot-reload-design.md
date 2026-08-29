# Architectural Specification: Hardened Lean GitOps, Hot-Reloading & Decoupled Configuration Architecture

**Date:** 2026-08-29  
**Status:** APPROVED WITH ADVERSARIAL HARDENING  
**Author:** Antigravity Architect & Engineering Team  
**Target Platform:** Single-Node Docker Compose on Home Assistant OS / Linux Host (`192.168.1.14`)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problems Solved
1. **Host Disk vs. Container Desynchronization**: Editing `AGENTS.md`, `SYSTEM.md`, or skills from a laptop or GitHub web UI left the host disk at `/share/aerial` stale, causing Git merge conflicts when Aerial self-improved and creating split-brain configuration states.
2. **Unnecessary Container Rebuild & Restart Storms**: In the legacy CI pipeline, any push to `main` (even doc updates or single-service edits) triggered a rebuild of all 6 container images on GHCR, causing Watchtower to restart every container in the stack.
3. **Prompt & Skill Update Downtime**: In container-recreate or Kubernetes models, changing a prompt rule in `AGENTS.md` caused a 45–90s pod restart and dropped Discord WebSocket connections.
4. **Public vs. Private Entanglement**: User-specific persona guidelines (`AGENTS.md`), private smart home skills, and secret credentials were tied to the public software engine repository.

### 1.2 The Hardened Lean GitOps Architecture
1. **Two-Repository Separation**:
   - **Public Core Engine (`azylman/aerial`)**: Go source code, container Dockerfiles, base guidelines `SYSTEM.md`, and CI workflows.
   - **Private User Config (`azylman/aerial-config`)**: User persona `AGENTS.md`, private custom skills (`custom-skills/`), and encrypted secrets.
2. **Intelligent GitHub Actions Path Filtering**:
   - Top-level `paths-ignore` for non-code files (`docs/**`, `README.md`, `MEMORY.md`, `.gitignore`).
   - Dynamic JSON build matrix (`dorny/paths-filter@v3`): Builds and publishes *only* the specific container image whose directory was modified.
3. **In-Process Mutex-Guarded Git-Sync (`brain/pkg/gitsync/`)**:
   - Synchronizes `/share/aerial-config` (and `/share/aerial`) from GitHub via `git fetch` and `--ff-only`.
   - **Concurrency Guard**: Synchronized via Go `sync.Mutex` and yields if an agent turn is actively executing or `.git/index.lock` exists.
   - **Safe Directory & Batch Auth**: Configures `safe.directory "*"` and `GITHUB_PAT` non-interactive auth.
4. **Hardened In-Process Hot-Reloading (`brain/pkg/watcher/`)**:
   - Linux `inotify`-backed directory watcher with dynamic recursive subdirectory attachment.
   - **Atomic File Writing**: Writes rules to `system_instructions.md.tmp.<pid>` and swaps via atomic POSIX `os.Rename`.
   - **Last Known Good Configuration (LKGC)**: Validates non-empty content before applying; falls back to LKGC on partial writes or errors.
   - **Orphaned Symlink Sweeper**: Purges broken symlinks when skills are deleted or renamed.
5. **Supervision Integrity**:
   - Retains **Watchtower** for out-of-band binary updates, **Autoheal** for healthcheck supervision, and Docker `restart: unless-stopped`.

---

## 2. System Architecture & Topology

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. PUBLIC REPOSITORY: github.com/azylman/aerial                            │
│    • brain/, scheduler-mcp/, discord-mcp/, docker-mcp/, github-mcp/         │
│    • SYSTEM.md (Base system guidelines)                                     │
│    • GitHub Actions CI (Dynamic Matrix Filter): Builds ONLY modified images │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────┼───────────────────────────────────────┐
│ 2. PRIVATE REPOSITORY: github.com/azylman/aerial-config                     │
│    • AGENTS.md (Personal persona, habits, custom rules)                     │
│    • custom-skills/ (Private smart home routines & skills)                  │
│    • secrets.enc.yaml (SOPS/Age encrypted API keys) or local .env           │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. MINIPC HOST RUNTIME (/share/ on 192.168.1.14)                            │
│    ├── /share/aerial/ (Engine clone)                                        │
│    └── /share/aerial-config/ (Private user config clone)                    │
│                                                                             │
│    subgraph Docker Containers:                                              │
│    ├── aerial-brain (:8088 / :8080)                                         │
│    │   ├── Discord Gateway Funnel (Aerial#5733)                             │
│    │   ├── In-Process Git-Sync (Lock-aware sync every 60s)                  │
│    │   ├── fsnotify File Watcher (Dynamic recursive directory watching)     │
│    │   ├── Atomic Rule Synthesizer (Tempfile + atomic rename + LKGC)        │
│    │   ├── Skill Symlink Sweeper (Orphaned symlink cleanup)                 │
│    │   ├── Background Scheduler Monitor (30s ticker)                        │
│    │   └── SQLite WAL Memory (/data/aerial.db)                              │
│    ├── aerial-scheduler-mcp (:8080)                                         │
│    ├── aerial-discord-mcp (:4001)                                           │
│    ├── aerial-docker-mcp (:4002)                                            │
│    ├── aerial-github-mcp (:4003)                                            │
│    ├── aerial-ollama (:11434)                                               │
│    ├── aerial-agentsview (:8089)                                            │
│    ├── aerial-watchtower (Polls GHCR; restarts ONLY changed containers)     │
│    └── aerial-autoheal (Monitors healthchecks every 15s)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Component Specifications & Hardening Rules

### 3.1 GitHub Actions Dynamic Matrix CI/CD (`.github/workflows/deploy.yml`)
- Top-level `paths-ignore`: `docs/**`, `README.md`, `MEMORY.md`, `.gitignore`.
- Filter definitions:
  - `brain`: `brain/**`, `SYSTEM.md`, `GEMINI.md`, `.agents/**`, `go.mod`, `go.sum`
  - `scheduler-mcp`: `scheduler-mcp/**`
  - `discord-mcp`: `discord-mcp/**`
  - `docker-mcp`: `docker-mcp/**`
  - `github-mcp`: `github-mcp/**`
  - `ollama`: `docker/ollama/**`
- Outputs dynamic JSON build matrix so only modified services execute.

### 3.2 In-Process Mutex-Guarded Git-Sync (`brain/pkg/gitsync/gitsync.go`)
- Background ticker running every 60s.
- `EnsureSafeDirectory`: Configures `git config --global --add safe.directory "*"` on startup.
- `Non-Interactive Batch Auth`: Injects `GITHUB_PAT` and `GIT_TERMINAL_PROMPT=0`.
- `Concurrency Guard`: Uses package-level `sync.Mutex` and checks `os.Stat(index.lock)`. Skips cycle if agent turn is active.
- `Execution Timeout`: Hard 15-second context timeout on subprocess calls.

### 3.3 Hardened File Watcher & Atomic Rule Synthesizer (`brain/pkg/watcher/watcher.go`)
- **Directory Watching**: Watches parent directories (`/share/aerial-config`, `/share/aerial`), not single file descriptors, ensuring resilience against atomic `rename()` operations.
- **Dynamic Recursive Watching**: On `IN_CREATE | IN_ISDIR`, automatically invokes `AddRecursive` on new subdirectories.
- **Ignore Filter**: Excludes `.git/**`, `*.db*`, `.gemini/**`, and editor temp files.
- **Debounce**: 500ms debounce timer to allow multi-file write settling.
- **Atomic Rule Generation**: Writes to `system_instructions.md.tmp.<pid>` and calls `os.Rename()`.
- **LKGC Fallback**: If `AGENTS.md` is empty or invalid, retains Last Known Good Configuration in memory.

### 3.4 Orphaned Symlink Sweeper in `brain/pkg/skills/skills.go`
- Scans `/root/.gemini/skills/` and removes broken symlinks pointing to deleted or renamed custom skills.

---

## 4. Verification & Validation Protocol

1. **Atomic Hot-Reload Verification**:
   - Edit `/share/aerial-config/AGENTS.md` on host $\rightarrow$ verify atomic tempfile creation and instant hot-reload in <5ms with 0 Discord drops.
2. **Git-Sync Concurrency Verification**:
   - Trigger agent prompt while 60s ticker fires $\rightarrow$ verify mutex prevents `.git/index.lock` collisions.
3. **Orphaned Symlink Cleanup Verification**:
   - Delete a custom skill directory $\rightarrow$ verify `EnsureSkills` removes dangling symlink without error.
4. **CI Path Filter Verification**:
   - Commit touching only `discord-mcp` $\rightarrow$ verify only `aerial-discord-mcp` image is built and restarted by Watchtower.
