# Standardized GEMINI.md Clean Break & Lean Core Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute a clean-break migration from legacy `SYSTEM.md` to standardized, native `GEMINI.md`, eliminate the Rule Duplication Trap by decoupling native workspace discovery from `EnsureSystemRules`, restore missing service topology without hardcoded ports, and establish startup bootstrapping sync.

**Architecture:** 
1. `/share/aerial/GEMINI.md` is discovered natively by Antigravity CLI (`agy`) as the canonical project workspace rule.
2. `brain/pkg/config/config.go` decouples `GEMINI.md` from `EnsureSystemRules`, compiling ONLY `/share/aerial-config/AGENTS.md` (and custom prompt overrides) into `~/.gemini/rules/user_persona.md`, completely preventing double context injection.
3. `brain/main.go` runs an immediate synchronous `gitsync.SyncRepo` for `/share/aerial` on container startup, eliminating cold-boot lag.
4. `brain/Dockerfile` and `.github/workflows/docker-publish.yml` are synchronized with `GEMINI.md` to preserve build caching and automated CI deployments.

**Tech Stack:** Go 1.22, Antigravity CLI (`agy`), Docker BuildKit, GitHub Actions CI/CD.

**Spec:** `docs/superpowers/specs/2026-09-02-gemini-rules-clean-break-architecture-design.md`

## Global Constraints
- **Zero Personal Data Invariant**: Never commit personal names, Discord handles, or private logic to `azylman/aerial`.
- **Zero Plaintext Token Invariant**: Never commit tokens or API keys to disk.
- **Zero-Bypass Invariant**: Under no circumstance use `git commit --no-verify`, `git commit -n`, or `git push --no-verify`. All commits must pass hooks and linters.
- **Strict Clean Break**: No fallback support for `SYSTEM.md` in search paths; `GEMINI.md` is canonical.

---

### Task 1: Create Canonical `GEMINI.md` & Purge Legacy Files

**Files:**
- Create: `GEMINI.md`
- Delete: `SYSTEM.md`
- Delete: `brain/GEMINI.md`

**Interfaces:**
- Produces: Canonical root `GEMINI.md` containing full 11-service topology, zero hardcoded ports, two-repo separation, and system invariants.

- [ ] **Step 1: Write canonical `GEMINI.md` at repository root**
  Populate with the verified specification from Section 4.1 of the design document (including all 11 services: `aerial-brain`, `scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`, `aerial-proxy`, `aerial-dashboard`, `aerial-docs`, `agentsview`, `ollama`, `watchtower`, `autoheal`).

- [ ] **Step 2: Delete legacy `SYSTEM.md` at repository root**
  Remove `SYSTEM.md` using `git rm SYSTEM.md`.

- [ ] **Step 3: Delete orphaned legacy `brain/GEMINI.md`**
  Remove obsolete 30-line `brain/GEMINI.md` using `git rm brain/GEMINI.md`.

- [ ] **Step 4: Verify working tree status**
  Run: `git status`
  Expected: `GEMINI.md` staged as new file, `SYSTEM.md` and `brain/GEMINI.md` staged as deleted.

- [ ] **Step 5: Commit Task 1**
  ```bash
  git add GEMINI.md SYSTEM.md brain/GEMINI.md
  git commit -m "feat(core): establish canonical GEMINI.md and purge legacy SYSTEM.md and brain/GEMINI.md"
  ```

---

### Task 2: Refactor `EnsureSystemRules` in `brain/pkg/config` (Decouple Workspace Discovery & Protect LKGC)

**Files:**
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: `/share/aerial-config/AGENTS.md` and `GEMINI.md`.
- Produces: `EnsureSystemRules` writes `user_persona.md` without duplicating `GEMINI.md`. Purges legacy rule files on disk. Protects LKGC on torn reads.

- [ ] **Step 1: Write unit tests in `brain/pkg/config/config_test.go`**
  Add tests asserting:
  1. `TestEnsureSystemRules_PersonaCompilation`: Verifies `EnsureSystemRules` compiles `AGENTS.md` into `user_persona.md` with `trigger: always_on` and does NOT duplicate `GEMINI.md`.
  2. `TestEnsureSystemRules_LKGCProtectionOnTornRead`: Simulates a 0-byte torn read of `AGENTS.md` and verifies LKGC persona is retained in memory.
  3. `TestEnsureSystemRules_StaleRuleCleanup`: Asserts that `staleRuleFiles` purges `system.md`, `system_instructions.md`, and uppercase variants.

- [ ] **Step 2: Run unit tests to verify failure**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/config`
  Expected: FAIL on new tests.

- [ ] **Step 3: Implement compiler changes in `brain/pkg/config/config.go`**
  - Update `SystemGuidelinesSearchPaths` to check canonical `GEMINI.md` search paths.
  - Refactor `EnsureSystemRules` to compile `AGENTS.md` + `customPrompt` into `~/.gemini/rules/user_persona.md` (and `~/.gemini/config/rules/user_persona.md`).
  - Add LKGC protection for 0-byte `AGENTS.md` reads.
  - Expand `staleRuleFiles` to delete `system.md`, `system_instructions.md`, and any `.agents/rules/gemini.md`.

- [ ] **Step 4: Run unit tests to verify pass**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/config`
  Expected: PASS.

- [ ] **Step 5: Commit Task 2**
  ```bash
  git add brain/pkg/config/config.go brain/pkg/config/config_test.go
  git commit -m "refactor(config): decouple native GEMINI.md from rule compiler and protect LKGC on torn reads"
  ```

---

### Task 3: Update Watcher Tests for `GEMINI.md` in `brain/pkg/watcher`

**Files:**
- Modify: `brain/pkg/watcher/watcher_test.go`

**Interfaces:**
- Produces: Verified file watcher ignore and trigger filters for `GEMINI.md`.

- [ ] **Step 1: Update `TestShouldIgnore` in `brain/pkg/watcher/watcher_test.go`**
  Replace references to `SYSTEM.md` in `allowedPaths` with `GEMINI.md`, `/share/aerial/GEMINI.md`, and `/share/aerial-config/GEMINI.md`.

- [ ] **Step 2: Run watcher unit tests**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/watcher`
  Expected: PASS.

- [ ] **Step 3: Commit Task 3**
  ```bash
  git add brain/pkg/watcher/watcher_test.go
  git commit -m "test(watcher): update watcher test assertions for canonical GEMINI.md"
  ```

---

### Task 4: Add Startup Bootstrapping Sync in `brain/main.go`

**Files:**
- Modify: `brain/main.go`

**Interfaces:**
- Produces: Immediate synchronous `gitsync.SyncRepo` for `/share/aerial` during startup before compiling rules.

- [ ] **Step 1: Update `brain/main.go` startup sequence**
  Immediately prior to `config.EnsureSystemRules`, call:
  ```go
  if err := gitsync.SyncRepo(bootCtx, "/share/aerial"); err != nil {
      log.Printf("[Startup] Notice: Bootstrapping sync for /share/aerial returned: %v (continuing with local tree)", err)
  }
  ```

- [ ] **Step 2: Verify `brain` builds cleanly**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -race ./...`
  Expected: PASS across all packages.

- [ ] **Step 3: Commit Task 4**
  ```bash
  git add brain/main.go
  git commit -m "feat(brain): add synchronous bootstrapping sync for /share/aerial on startup"
  ```

---

### Task 5: Update Dockerfile, GitHub CI Matrix, and Documentation

**Files:**
- Modify: `brain/Dockerfile`
- Modify: `.github/workflows/docker-publish.yml`
- Modify: `README.md`
- Modify: `config.example.yaml`

**Interfaces:**
- Produces: Fully synchronized container image definitions, automated CI path triggers, and canonical documentation.

- [ ] **Step 1: Update `brain/Dockerfile`**
  - Line 44: `COPY GEMINI.md /app/GEMINI.md`.
  - Line 45: Delete `COPY SYSTEM.md /app/SYSTEM.md`.

- [ ] **Step 2: Update `.github/workflows/docker-publish.yml`**
  - Under `brain` paths-filter, change `'SYSTEM.md'` -> `'GEMINI.md'`.

- [ ] **Step 3: Update `README.md` and `config.example.yaml`**
  - Update all 4 occurrences of `SYSTEM.md` in `README.md` to `GEMINI.md`.
  - Update comment in `config.example.yaml`.

- [ ] **Step 4: Commit Task 5**
  ```bash
  git add brain/Dockerfile .github/workflows/docker-publish.yml README.md config.example.yaml
  git commit -m "chore(infra): update Dockerfile, CI triggers, and documentation to canonical GEMINI.md"
  ```

---

### Task 6: Monorepo Verification, PR Creation & Auto-Merge Land

**Files:**
- Repository-wide verification

- [ ] **Step 1: Run monorepo pre-flight verification gate**
  Run: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1`
  Expected: 100% PASS across all linters, unit tests, and HUD tests.

- [ ] **Step 2: Push feature branch to remote**
  Run: `git push -u origin docs/gemini-rules-clean-break-spec` (or feature branch).

- [ ] **Step 3: Open Pull Request via GitHub MCP**
  Title: `refactor(core): standardized GEMINI.md clean break and lean core architecture`

- [ ] **Step 4: Monitor CI status checks**
  Verify `Run Service Unit Tests` and `Lint Microservices & Frontend` pass with success.

- [ ] **Step 5: Squash merge Pull Request to `main`**
  Merge PR, pull `main`, and clean up feature branch.
