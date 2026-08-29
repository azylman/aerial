# Hardened Lean GitOps, Hot-Reloading & Decoupled Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a hardened, zero-downtime Two-Repository GitOps architecture with GitHub Actions dynamic matrix path filtering, in-process mutex-guarded Git synchronization, and atomic filesystem hot-reloading with LKGC fallbacks.

**Architecture:** Public engine (`azylman/aerial`) houses Go microservices, base `SYSTEM.md`, and dynamic CI matrix filtering. Private configuration (`azylman/aerial-config`) houses `AGENTS.md` and custom skills. `aerial-brain` runs a lock-aware Git-sync worker and a recursive `fsnotify` file watcher that atomically updates prompt instructions and symlinks in memory with 0s downtime, 0 Discord disconnections, and Last Known Good Configuration (LKGC) protections.

**Tech Stack:** Go 1.22, `github.com/fsnotify/fsnotify`, GitHub Actions (`dorny/paths-filter@v3`), GHCR, Docker Compose, Watchtower, Autoheal.

**Spec:** [`docs/superpowers/specs/2026-08-29-lean-gitops-and-hot-reload-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-08-29-lean-gitops-and-hot-reload-design.md)

## Global Constraints
- Discord Gateway WebSocket session must never be interrupted during prompt, persona, or skill updates.
- All file writes to `system_instructions.md` must be POSIX-atomic (tempfile + `os.Rename`).
- If an updated `AGENTS.md` is empty or invalid, the system must retain the Last Known Good Configuration (LKGC).
- Git synchronization must be mutex-guarded and skip ticks if an agent turn is active or `.git/index.lock` exists.
- Broken/orphaned symlinks in `/root/.gemini/skills/` must be cleaned up dynamically on skill removal.

---

### Task 1: GitHub Actions Dynamic Matrix Path Filtering (`.github/workflows/deploy.yml`)

**Files:**
- Modify: `.github/workflows/deploy.yml`

- [ ] **Step 1: Update workflow triggers and paths-ignore**
  Add top-level `paths-ignore` for `docs/**`, `README.md`, `MEMORY.md`, and `.gitignore`.

- [ ] **Step 2: Add dynamic matrix detection job using `dorny/paths-filter@v3`**
  Map change filters for `brain`, `scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`, and `ollama`, ensuring `brain` includes `SYSTEM.md`, `GEMINI.md`, and `.agents/**`.

- [ ] **Step 3: Update `build-and-push` job with dynamic matrix**
  Execute builds only for modified services using `${{ fromJson(needs.filter.outputs.matrix) }}`.

- [ ] **Step 4: Commit Task 1**
  ```bash
  git add .github/workflows/deploy.yml
  git commit -m "ci(workflow): add dynamic matrix path filtering via paths-filter"
  ```

---

### Task 2: Hardened In-Process Hot-Reloading & File Watcher (`brain/pkg/watcher/`)

**Files:**
- Create: `brain/pkg/watcher/watcher.go`
- Create: `brain/pkg/watcher/watcher_test.go`
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/skills/skills.go`
- Modify: `brain/main.go`
- Modify: `brain/go.mod` & `brain/go.sum`

- [ ] **Step 1: Write unit tests for `watcher.go`**
  Test recursive directory watching, 500ms debouncing, and ignore filtering for `.git` and `*.db*`.

- [ ] **Step 2: Implement `brain/pkg/watcher/watcher.go`**
  Implement dynamic recursive watching, ignore filters, parent directory watching, and debounced callback triggers.

- [ ] **Step 3: Implement Atomic Rule Writing & LKGC in `config.go`**
  Update `EnsureSystemRules` to write to tempfile + `os.Rename` with Last Known Good Configuration fallback on empty/corrupted reads.

- [ ] **Step 4: Implement Orphaned Symlink Sweeper in `skills.go`**
  Update `EnsureSkills` to detect and remove broken symlinks in target skill directories.

- [ ] **Step 5: Integrate Watcher in `brain/main.go`**
  Start the background watcher in `main.go` hooking `EnsureSystemRules` and `EnsureSkills`.

- [ ] **Step 6: Run unit tests to verify 100% pass**
  ```bash
  cd brain && go test -v ./pkg/watcher/... ./pkg/config/... ./pkg/skills/...
  ```

- [ ] **Step 7: Commit Task 2**
  ```bash
  git add brain/
  git commit -m "feat(brain): add hardened recursive fsnotify file watcher with atomic rule synthesis and LKGC"
  ```

---

### Task 3: In-Process Mutex-Guarded Git-Sync Worker (`brain/pkg/gitsync/`)

**Files:**
- Create: `brain/pkg/gitsync/gitsync.go`
- Create: `brain/pkg/gitsync/gitsync_test.go`
- Modify: `brain/main.go`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Write unit tests for `gitsync.go`**
  Test clean fast-forward sync, lock skipping when `index.lock` exists, and safe directory configuration.

- [ ] **Step 2: Implement `gitsync.go`**
  Implement mutex-guarded `SyncRepo` with 15s context timeout, `safe.directory "*"` configuration, non-interactive batch auth, and 60s periodic ticker.

- [ ] **Step 3: Update `docker-compose.yml`**
  Mount `/share/aerial-config` into `brain`.

- [ ] **Step 4: Integrate Git-Sync in `main.go`**
  Start periodic Git-sync on startup for `/share/aerial-config` and `/share/aerial`.

- [ ] **Step 5: Run all unit tests**
  ```bash
  cd brain && go test -v ./...
  ```

- [ ] **Step 6: Commit Task 3**
  ```bash
  git add brain/ docker-compose.yml
  git commit -m "feat(brain): add mutex-guarded in-process git-sync worker"
  ```

---

### Task 4: MiniPC Live Stack Verification & Hot-Reload Validation

**Files:**
- Host clone: `/share/aerial-config` on miniPC (`192.168.1.14`).

- [ ] **Step 1: Push commits to GitHub `origin/main`**
  Verify GitHub Actions builds only modified `aerial-brain` image.

- [ ] **Step 2: Deploy updated stack on MiniPC**
  Run `git pull origin main && sudo docker compose up -d` on miniPC host.

- [ ] **Step 3: Test Hot-Reload Live**
  Modify `/share/aerial-config/AGENTS.md` $\rightarrow$ inspect `aerial-brain` logs $\rightarrow$ verify atomic hot-reload in <5ms with zero Discord drop.

- [ ] **Step 4: Test Remote Git-Sync Live**
  Push commit to `aerial-config` $\rightarrow$ verify `aerial-brain` pulls and hot-reloads within 60s.
