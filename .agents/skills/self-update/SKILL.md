---
name: self-update
description: Use this skill when the user asks Aerial to update itself, pull latest code from git, or deploy system updates.
---

# Self-Update & Microservice Deployment Runbook

This skill defines how Aerial safely updates code, synchronizes configuration, triggers deployments, and tracks container rollouts across the dual-repository architecture.

---

## 1. Two-Repository Separation of Concerns

Aerial cleanly separates generic system code from private user configuration:

• **Core Engine Repository (`azylman/aerial` at `/share/aerial`)**:
  - **Type**: Public repository.
  - **Contents**: Go execution brain, MCP microservices, base system skills, Docker topology, and architecture docs.
  - **Invariants**: 100% generic, zero personal data/names, zero secrets. Filesystem is mounted read-only (`:ro`) inside the container.
  - **Deployment Flow**: GitHub Actions builds and publishes images to GHCR; Hangar on the host reconciles updated containers out-of-band.

• **User Configuration Repository (`azylman/aerial-config` at `/share/aerial-config`)**:
  - **Type**: Private repository.
  - **Contents**: `config.yaml`, `AGENTS.md` (persona/identity), `custom-skills/`, compose overrides, and environment secrets.
  - **Invariants**: User identity, personal aliases, private credentials. Filesystem is mounted read-only (`:ro`) inside the container.
  - **Deployment Flow**: In-process `fsnotify` watcher hot-reloads configuration and skills dynamically within milliseconds; Hangar pulls git updates.

---

## 2. Triggering Updates & Synchronizing Upstream Releases

When the user asks Aerial to update itself, pull the latest code, or deploy system updates without modifying code:

### Step 1: Trigger Fast-Path Git Sync & Reconciliation
Because `/share/aerial` and `/share/aerial-config` are mounted read-only, Aerial triggers the host Hangar daemon to pull upstream changes and reconcile containers:
```bash
# Trigger GitOps repository sync on Hangar:
curl -s -f -X POST http://aerial-hangar:8080/sync

# Reconcile container topology (if services require recreation):
curl -s -f -X POST http://aerial-hangar:8080/reconcile
```

### Step 2: Track Deployment & Container Rollout
Check active deployment and container swap progress:
```bash
/share/aerial/scripts/aerial-pr.sh deploy-status main
```
- Report deployment status in plain prose (maximum 2 sentences, zero markdown bullet lists).
- If deployment is ongoing, reschedule a 2-minute follow-up check via `scheduler_schedule_once`.

---

## 3. Engineering & Code Modification Workflow

> [!IMPORTANT]
> **Engineering & Feature Development Gate**:
> For non-trivial modifications, bug fixes, refactors, or new features, Aerial MUST invoke the `self-improvement` skill (`.agents/skills/self-improvement/SKILL.md`) and adhere to the Tiered Engineering Workflow in `GEMINI.md` (Section 8).
> • Code review scales dynamically across four tiers (Tier 0 bypassed; Tier 1 solo critic; Tier 2/3 full panel).
> • Code review is consolidated on the pre-PR diff; per-task review pauses are strictly prohibited.
> • Human approval is reserved strictly for Tier 3 tasks.

When making code or configuration changes, Aerial never modifies `/share/aerial` or `/share/aerial-config` directly. All changes follow the automated PR workflow:

### Core Engine PR Workflow (`scripts/aerial-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh init
   ```
   Clones `azylman/aerial:main` into an isolated scratch directory (`/data/scratch/aerial-code-scratch.XXXXXX`) on an ephemeral branch.
2. **Implement & Author PR Description**:
   - Write tests and code following TDD.
   - Author `PR_DESCRIPTION.md` in the scratch root (mandatory; submissions fail without a description).
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "feat(module): description"
   ```
   - Automatically runs fast pre-flight verification (`./scripts/verify.sh --staged`).
   - Pushes branch, creates PR with auto-merge, and schedules follow-up check via `scheduler-mcp`.
   - Cleans up scratch directory immediately. **Do not poll CI in foreground; end active execution turn.**
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-pr.sh merge <pr_num>
   ```
   - If CI is still pending (exit code 2), quietly reschedule follow-up check.
   - Upon green merge, confirms merge and tracks container swap to completion in max 2 sentences.

### Configuration PR Workflow (`scripts/aerial-config-pr.sh`)
1. **Initialize Scratch Workspace**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh init
   ```
2. **Implement & Author PR Description**:
   - Update YAML, prompts, or custom skills.
   - Author `PR_DESCRIPTION.md` in the scratch root.
3. **Submit Asynchronously**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh submit <scratch_dir> "chore(config): description"
   ```
   - Automatically validates YAML syntax and schema.
   - Pushes branch, creates PR, and schedules follow-up check via `scheduler-mcp`.
4. **Verify & Merge on Scheduled Wake-up**:
   ```bash
   /share/aerial/scripts/aerial-config-pr.sh merge <pr_num>
   ```
   - Hangar syncs repository and in-process watcher hot-reloads changes without container downtime.

---

## 4. Operational Invariants

• **Container Execution Invariant**: NEVER execute `docker compose up`, `docker compose build`, or `docker restart` from inside the container. All image builds occur in GitHub Actions CI, and container swaps are handled by Hangar.
• **Scheduling Invariant**: Never schedule manual follow-up timers after calling `submit`; `aerial-pr.sh submit` automatically registers the follow-up reminder via `scheduler-mcp`.
• **Response Format Invariant**: Merge and deployment confirmations must be reported in plain prose strictly capped at 2 sentences max (no markdown checklists, tables, or forward-looking promises).
