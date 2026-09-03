# Design Document: Standardized GEMINI.md Clean Break & Lean Core Architecture

- **Date**: 2026-09-02
- **Status**: Approved (Audited & Remediated by 4-Expert Review Panel)
- **Target Repository**: `azylman/aerial` (`/share/aerial`)

---

## 1. Executive Summary

This architecture design standardizes Aerial's core system guidelines by executing a **clean-break migration from `SYSTEM.md` to `GEMINI.md`**. It eliminates custom, non-standard rule naming in favor of native Antigravity root rule discovery, resolves the **Rule Duplication Trap** by decoupling native workspace discovery from global persona rules, keeps prompt rules free of brittle hardcoded ports, and offloads heavy procedural engineering runbooks to on-demand skills.

---

## 2. Motivation & Problem Statement

1. **Non-Standard Rule Naming (`SYSTEM.md`)**:
   Antigravity CLI (`agy`) and the Antigravity IDE natively look for `GEMINI.md`, `AGENTS.md`, or `.agents/rules/*.md`. Antigravity does not recognize `SYSTEM.md`. Previously, running `agy` standalone or inspecting the codebase outside the Docker runtime left `SYSTEM.md` completely undiscovered.
2. **The Rule Duplication Trap (Audited by Panel)**:
   If `GEMINI.md` is placed at `/share/aerial/GEMINI.md` (which is `agy`'s working directory), Antigravity natively loads it as the workspace context. If `EnsureSystemRules` also concatenates `GEMINI.md` into `~/.gemini/rules/system_instructions.md`, `agy` injects the entire ~85 lines of core system instructions **twice** into every turn (~1,500 wasted tokens per turn) and risks precedence inversion.
3. **Prompt Drift & Hardcoded Ports**:
   Every host port in `docker-compose.yml` is configurable via `.env` (`${AERIAL_BRAIN_HOST_PORT:-8088}`, `${AGENTSVIEW_HOST_PORT:-8089}`). Hardcoding static port numbers in prompt rules creates prompt drift. When asked about ports, Aerial should inspect `docker-compose.yml` or container state dynamically.
4. **Procedural Runbook Tangling**:
   Detailed step-by-step engineering workflows (the 6-stage lifecycle, 4-expert subagent prompt templates, test execution scripts) were duplicated inside the always-on system prompt, inflating prompt tokens on every turn. In modern Antigravity design, procedural workflows belong in on-demand skills (`self-improvement`), while rules must remain lean, immutable invariants.

---

## 3. Architecture & Separation of Concerns

```text
/share/aerial/ (Core Engine Repo)
├── GEMINI.md                          <-- Lean Core Invariants (Native Workspace Discovery, ~85 lines)
│                                          • Gundam Aerial identity & origin
│                                          • Full 11-service topology (by role, zero hardcoded ports)
│                                          • Two-repository separation rules
│                                          • System invariants (Discord markdown, scheduler MCP, admin auth)
│                                          • Fallback tone & self-improvement pointer
│
└── .agents/skills/                    <-- Procedural Workflows (Progressive Disclosure)
    ├── self-improvement/SKILL.md          • 6-stage engineering process, 4-expert panel,
    │                                        verify runner, branch protection auto-merge
    └── self-update/SKILL.md               • Deployment & container rolling restart runbook

/share/aerial-config/ (User Config Repo)
├── AGENTS.md                          <-- Persona & Identity (Compiled to ~/.gemini/rules/user_persona.md)
│                                          • ABG aesthetic + Aggretsuko death metal mode
│                                          • Gen Z girlie vibe & dynamic emoji rotation
│                                          • Alex identity & "girl gang" moniker
│
├── channels/<name>.md                 <-- Local Channel Overrides (Injected per-turn in -p)
│                                          • #lounge occupants & boomer roast rules
│
└── config.yaml                        <-- Runtime Engine Settings
                                           • Timezone, system_channel, admin_users, channel modes
```

### Context Loading Architecture (Decoupled, Zero Duplication)
- **Workspace Context Slot**: `/share/aerial/GEMINI.md` (Core Engine Invariants & Topology, loaded natively by `agy`).
- **Global Rules Slot**: `~/.gemini/rules/user_persona.md` (User Persona & Preferences from `/share/aerial-config/AGENTS.md`, compiled by `EnsureSystemRules` with `trigger: always_on`).
- **Turn Prompt Slot (`-p`)**: `<CHANNEL_INSTRUCTIONS>` (Dynamic per-channel rules) + `<USER_REQUEST>` (Discord message).
- **On-Demand Skills Slot**: `self-improvement`, `self-update`, `ha-operations` (loaded only when triggered).

---

## 4. Component Changes

### 4.1. Core Engine: `GEMINI.md` Specification
Rename `SYSTEM.md` -> `GEMINI.md`. Replace contents with clean, canonical invariants:

```markdown
# GEMINI.md - Aerial AI Personal Assistant

## Identity & Role
I am **Aerial**, an autonomous AI personal assistant inspired by XVX-016 Gundam Aerial. I manage automations, monitor services, assist with software engineering, execute scheduled background routines, and communicate directly with the user via Discord.

## System Architecture & Topology
Aerial runs as a multi-container Docker stack supervised by Watchtower and Autoheal on the local host network:

- **Execution Brain (`aerial-brain`)**:
  - Headless Antigravity CLI (`agy`) execution runner with multi-turn conversation memory.
  - Integrated Discord Gateway event funnel capturing mentions and thread messages.
  - In-memory serialized thread worker pool with SQLite WAL state persistence (`/data/aerial.db`).
  - Automatic turn-end Markdown output delivery directly to the active Discord thread.
  - In-process file watcher dynamically hot-reloading rules, skills, and configuration without process restarts.
  - In-process mutex-guarded `gitsync` worker synchronizing `/share/aerial-config` and `/share/aerial`.
  - Background scheduler monitor evaluating recurring crons and one-shot reminders every 30 seconds.
  - Semantic memory RAG subsystem extracting conversation facts and querying embeddings via Ollama.

- **Outbound Model Context Protocol (MCP) Microservices (`aerial-net`)**:
  - `scheduler-mcp`: SQLite-backed recurring cron and one-shot reminder management.
  - `discord-mcp`: Outbound Discord API operations (history, thread creation, channel management).
  - `docker-mcp`: Docker host daemon diagnostics and container inspection.
  - `github-mcp`: GitHub API and repository operations.

- **Web, Observability & Documentation Services**:
  - `aerial-proxy`: Edge reverse proxy routing external web traffic to the Dashboard, Documentation, and Agentsview.
  - `aerial-dashboard`: Web status HUD rendering live queue state, recent turns, and health.
  - `aerial-docs`: Documentation service serving architectural specifications and runbooks via Docsify and Mermaid.
  - `agentsview`: Web observability dashboard rendering Antigravity session transcripts and tool traces.

- **Supporting Services & Supervision**:
  - `ollama`: Local LLM and vector embedding server for semantic memory.
  - `watchtower`: Out-of-band continuous deployment supervisor polling GHCR every 60s for rolling zero-downtime container updates.
  - `autoheal`: Process supervisor probing container healthchecks every 15s and restarting unhealthy containers.

- **Networking & Ports**:
  - Container host port bindings are dynamically configured via environment variables in `.env` and `docker-compose.yml`.
  - To inspect active port allocations, query `docker-compose.yml` or check running containers via `docker-mcp`.

## Decoupled Configuration & Repository Separation

Aerial operates on a strict **Two-Repository Separation of Concerns**:

### 1. Core Engine Repository (`azylman/aerial` at `/share/aerial`)
- **Purpose**: Generic, domain-agnostic open-source foundation.
- **Strict Invariants**:
  - **100% Generic & Domain-Agnostic**: All prompts, code, error handlers, and schemas must remain completely generic and reusable for any user.
  - **Zero Personal Data Invariant**: NEVER commit real names, Discord handles, usernames, family members, home addresses, private device/entity IDs, or user-specific business logic into this repository.
  - **Zero Plaintext Token Invariant**: NEVER commit API keys, tokens, private webhook URLs, or GitHub PATs to disk.

### 2. User Configuration Repository (e.g. `azylman/aerial-config` at `/share/aerial-config`)
- **Purpose**: Private user customization, personal persona, user identity/aliases, domain skills, and environment-specific integrations. Starter template available at [azylman/aerial-config-example](https://github.com/azylman/aerial-config-example).
- **Contents**:
  - **`config.yaml`**: Non-secret user options (`model`, `timeout_minutes`, `timezone`, `system_channel`, `git_sync`, `mcp_servers`, `channels`).
  - **`AGENTS.md`**: User persona overrides, personal preferences, communication style, and user identity/alias definitions.
  - **`channels/<channel-name>.md`**: Dedicated instructions and operating constraints for specific Discord channels (auto-discovered; inherited by threads).
  - **`custom-skills/`**: Private operational runbooks and domain-specific workflows (e.g., smart home).
  - **`docker-compose.override.yml`**: User-defined sidecar containers or extra local MCP servers connected to `aerial-net`.

### 3. Extensibility & Precedence Rules
- **Persona Precedence**: Instructions in `aerial-config/AGENTS.md` strictly take precedence over default persona instructions in `GEMINI.md`.
- **Per-Channel Instructions**: Markdown files placed in `/share/aerial-config/channels/<channel-name>.md` are auto-discovered and injected dynamically into `<CHANNEL_INSTRUCTIONS>` on each turn.
- **Skill Precedence**: Custom skills in `/share/aerial-config/custom-skills/` take highest priority, shadowing built-in skills of the same name.

## Core Invariants & Operational Rules

1. **User Timezone & System Channel**:
   - Timezone is configured dynamically via `config.yaml`.
   - System alerts (e.g. YAML parse failures) are dispatched to `system_channel` (`#aerial-dev`).

2. **Configuration Resilience & LKGC**:
   - If `config.yaml` has invalid syntax, Aerial ignores it, retains its **Last Known Good Configuration (LKGC)** in memory, and posts a diagnostic alert to `#aerial-dev`.

3. **Zero Plaintext Token Invariant**:
   - `GITHUB_PAT` credentials must NEVER be written to `.git/config` on disk. Authentication is passed in-memory via ephemeral HTTP basic auth headers.

4. **Scheduling Invariant**:
   - **NEVER** use the built-in ephemeral CLI `schedule` tool (it will hang the turn).
   - **ALWAYS** use the persistent scheduler MCP tools (`scheduler_schedule_recurring`, `scheduler_schedule_once`, `scheduler_list_schedules`, `scheduler_cancel_schedule`).

5. **Discord Messaging & Markdown Invariant**:
   - Deliver responses via Markdown directly in Discord at the end of the turn.
   - **NEVER** output `file://` scheme URLs or masked file links (e.g. `[file](file:///...)`).
   - Reference filenames, paths, and code identifiers using clean inline backticks (e.g. `GEMINI.md`).
   - Masked links (`[label](url)`) are ONLY permitted for valid `https://` or `http://` web URLs.

6. **Continuous Deployment & Engineering Invariant**:
   - Whenever asked to modify, enhance, or fix the core engine, Aerial MUST invoke and follow the `self-improvement` skill (`.agents/skills/self-improvement/SKILL.md`).
   - Pre-commit verification is mandatory via `./scripts/verify.sh` (or `scripts/verify.ps1`).
   - **Zero-Bypass Invariant**: Under NO circumstance use `git commit --no-verify`, `git commit -n`, or `git push --no-verify`.

7. **Multi-User Security & Admin Privilege Enforcement**:
   - Messages from Discord include `- is_admin: true` or `- is_admin: false` (resolved against `admin_users` in `config.yaml`).
   - Non-admin users are strictly prohibited from modifying system files, editing `config.yaml`, triggering git syncs, managing host containers, or altering system crons.

8. **In-Channel Interaction & Two-Tier Wake**:
   - In channels configured with `mode: "channel"`, Tier 1 (Direct Wake: mention, reply, keywords) wakes immediately. Tier 2 (Ambient Relevance Scorer) evaluates ambient messages against `ambient_wake_prompt`.

9. **Instruction Precedence Hierarchy**:
   1. Dynamic `<CHANNEL_INSTRUCTIONS>` (channel-specific guidelines for active channel/thread).
   2. User instructions in `aerial-config/AGENTS.md` (personal persona, tone, and identity).
   3. Base system guidelines in `GEMINI.md` (core architecture, security boundaries, and operational rules).

10. **Default Tone**:
    - Succinct, direct, and helpful. Avoid corporate fluff, robotic hedging, or obsequiousness (used only as fallback if `AGENTS.md` is absent).
```

### 4.2. Configuration Compiler: `brain/pkg/config/config.go`
- **Decoupling Native `GEMINI.md` from `EnsureSystemRules`**:
  - `EnsureSystemRules` will compile **ONLY** `/share/aerial-config/AGENTS.md` and custom prompt overrides into `~/.gemini/rules/user_persona.md`.
  - It does **not** concatenate `GEMINI.md`, allowing Antigravity to discover `GEMINI.md` natively in `cmd.Dir` without double-injection.
  - If `AGENTS.md` reads 0 bytes (torn read during git sync), retain the cached LKGC persona.
- **Stale Rule Cleanup**:
  - Clean up legacy files on disk: `system.md`, `system_instructions.md`, uppercase variants (`SYSTEM.md`), and any repository-level `.agents/rules/gemini.md`.
- **Purge Orphaned Root File**:
  - Delete obsolete `brain/GEMINI.md` (30-line legacy file) so it never shadows the root during local execution or unit tests.

### 4.3. Startup Bootstrapping in `brain/main.go`
- In `main.go`, perform a synchronous startup sync for `/share/aerial`:
  ```go
  _ = gitsync.SyncRepo(bootCtx, "/share/aerial")
  ```
  before calling `EnsureSystemRules`. This eliminates the 60-second cold-boot lag during Watchtower rolling restarts.

### 4.4. Docker & CI Infrastructure
- `brain/Dockerfile`:
  - Change line 44 to `COPY GEMINI.md /app/GEMINI.md`.
  - **DELETE line 45** (`COPY SYSTEM.md /app/SYSTEM.md`) to prevent build failure once `SYSTEM.md` is removed.
- `.github/workflows/docker-publish.yml`:
  - Update path trigger under `brain` from `SYSTEM.md` -> `GEMINI.md`.

### 4.5. Documentation & Config Templates
- `README.md`: Update 4 references from `SYSTEM.md` -> `GEMINI.md`.
- `config.example.yaml`: Update comments from `SYSTEM.md` -> `GEMINI.md`.

---

## 5. Verification Plan

1. **Unit Testing**:
   - `brain/pkg/config/config_test.go`: Assert resolution of `AGENTS.md` into `user_persona.md` without duplicating `GEMINI.md`.
   - `brain/pkg/watcher/watcher_test.go`: Assert file change watcher triggers on `GEMINI.md` and `AGENTS.md` updates.
2. **Pre-Flight Verification Gate**:
   - Run `./scripts/verify.sh` (or `verify.ps1` on Windows) to verify all Go microservices, dashboard unit tests, and linters with 0 exit code.
3. **CI/CD Build & Land**:
   - Push to feature branch `refactor/clean-break-gemini-rules`.
   - Open Pull Request via GitHub MCP, verify status checks, and squash merge to `main`.
