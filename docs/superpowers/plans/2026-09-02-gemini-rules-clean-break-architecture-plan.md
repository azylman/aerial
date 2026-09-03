# Standardized GEMINI.md Clean Break & Lean Core Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute a clean-break migration from legacy `SYSTEM.md` to standardized, native `GEMINI.md`, eliminate the Rule Duplication Trap by decoupling native workspace discovery from `EnsureSystemRules`, restore missing service topology without hardcoded ports, purge stale volume rules, and establish startup bootstrapping sync.

**Architecture:** 
1. `/share/aerial/GEMINI.md` is discovered natively by Antigravity CLI (`agy`) as the canonical project workspace rule.
2. `brain/pkg/config/config.go` decouples `GEMINI.md` from `EnsureSystemRules`, compiling ONLY `/share/aerial-config/AGENTS.md` (and custom prompt overrides) into `~/.gemini/rules/user_persona.md`, completely preventing double context injection.
3. `staleRuleFiles` purges legacy `system_instructions.md` and `system.md` from `~/.gemini/rules/` and `~/.gemini/config/rules/`, preventing triple-injection on persistent Docker volumes.
4. `brain/main.go` runs an immediate synchronous `gitsync.SyncRepo` with dedicated timeout context for `/share/aerial` on container startup, eliminating cold-boot lag.
5. `brain/Dockerfile` has `COPY SYSTEM.md` deleted in Task 1, maintaining 100% container buildability across every git commit.

**Tech Stack:** Go 1.22, Antigravity CLI (`agy`), Docker BuildKit, GitHub Actions CI/CD.

**Spec:** `docs/superpowers/specs/2026-09-02-gemini-rules-clean-break-architecture-design.md`

## Global Constraints
- **Zero Personal Data Invariant**: Never commit personal names, Discord handles, or private logic to `azylman/aerial`.
- **Zero Plaintext Token Invariant**: Never commit tokens or API keys to disk.
- **Zero-Bypass Invariant**: Under no circumstance use `git commit --no-verify`, `git commit -n`, or `git push --no-verify`. All commits must pass hooks and linters.
- **Strict Clean Break**: No fallback support for `SYSTEM.md` in search paths; `GEMINI.md` is canonical.

---

### Task 1: Establish Canonical `GEMINI.md`, Dockerfile Sync, & Purge Legacy Files

**Files:**
- Create: `GEMINI.md`
- Delete: `SYSTEM.md`
- Delete: `brain/GEMINI.md`
- Modify: `brain/Dockerfile`

**Interfaces:**
- Produces: Canonical root `GEMINI.md` containing full 11-service topology, zero hardcoded ports, two-repo separation, and system invariants. Dockerfile buildable immediately.

- [ ] **Step 1: Write canonical `GEMINI.md` at repository root**
  Populate with verified specification from Section 4.1 of the design document (including all 11 services: `aerial-brain`, `scheduler-mcp`, `discord-mcp`, `docker-mcp`, `github-mcp`, `aerial-proxy`, `aerial-dashboard`, `aerial-docs`, `agentsview`, `ollama`, `watchtower`, `autoheal`).

- [ ] **Step 2: Delete legacy `SYSTEM.md` at repository root**
  Run: `git rm SYSTEM.md`

- [ ] **Step 3: Delete orphaned legacy `brain/GEMINI.md`**
  Run: `git rm brain/GEMINI.md`

- [ ] **Step 4: Update `brain/Dockerfile` to remove `COPY SYSTEM.md`**
  In `brain/Dockerfile`:
  - Ensure line 44 is `COPY GEMINI.md /app/GEMINI.md`.
  - Delete line 45 (`COPY SYSTEM.md /app/SYSTEM.md`).
  *(Prevents intermediate commit build breakages during git bisect).*

- [ ] **Step 5: Verify working tree status**
  Run: `git status`
  Expected: `GEMINI.md` new file, `SYSTEM.md` & `brain/GEMINI.md` deleted, `brain/Dockerfile` modified.

- [ ] **Step 6: Commit Task 1**
  ```bash
  git add GEMINI.md SYSTEM.md brain/GEMINI.md brain/Dockerfile
  git commit -m "feat(core): establish canonical GEMINI.md, sync Dockerfile, and purge legacy SYSTEM.md"
  ```

---

### Task 2: Refactor `EnsureSystemRules` in `brain/pkg/config` (Decouple Workspace Discovery, Purge Stale Volumes, & Protect LKGC)

**Files:**
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: `/share/aerial-config/AGENTS.md` and custom prompt overrides.
- Produces: `EnsureSystemRules` compiles `user_persona.md` without duplicating `GEMINI.md`. Purges `system_instructions.md` from `primaryRulesDir` and `configRulesDir`. Protects LKGC on torn reads.

- [ ] **Step 1: Write and update unit tests in `brain/pkg/config/config_test.go`**
  - Update existing tests (`TestEnsureAgySettingsAndRules`, `TestEnsureSystemRules_LKGCAndOrdering`) to assert `user_persona.md` exists and `system_instructions.md` does NOT exist.
  - Add `TestEnsureSystemRules_PersonaCompilation`: Asserts `EnsureSystemRules` compiles `AGENTS.md` into `user_persona.md` with `trigger: always_on` and does NOT concatenate `GEMINI.md`.
  - Add `TestEnsureSystemRules_LKGCProtectionOnTornRead`: Simulates a 0-byte or whitespace-only read of `AGENTS.md` (even with non-empty `customPrompt`) and verifies LKGC persona is retained.
  - Add `TestEnsureSystemRules_StaleRuleCleanup`: Asserts that `staleRuleFiles` explicitly purges `system_instructions.md`, `system.md`, and uppercase variants from both `~/.gemini/rules/` and `~/.gemini/config/rules/`.
  - Add `TestEnsureSystemRules_ConcurrentReload`: Exercises `EnsureSystemRules` concurrently across 10 goroutines under `-race`.

- [ ] **Step 2: Run unit tests to verify failure**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/config`
  Expected: FAIL on assertion for `user_persona.md`.

- [ ] **Step 3: Implement compiler changes in `brain/pkg/config/config.go`**
  - Remove/deprecate `SystemGuidelinesSearchPaths` loop from `EnsureSystemRules` (eliminating the Rule Duplication Trap).
  - Target `user_persona.md`: Write frontmatter `description: User persona, tone, and identity overrides\ntrigger: always_on`.
  - Compile `/share/aerial-config/AGENTS.md` + `customPrompt` into `user_persona.md` (and sync to `~/.gemini/config/rules/user_persona.md`).
  - Introduce `lastKnownGoodPersona` protection: If `AGENTS.md` exists but reads 0 bytes or whitespace, engage LKGC persona without overwriting it with blank text or secondary fallback paths.
  - Expand `staleRuleFiles` to include:
    - `filepath.Join(primaryRulesDir, "system_instructions.md")`
    - `filepath.Join(configRulesDir, "system_instructions.md")`
    - `filepath.Join(primaryRulesDir, "system.md")`
    - `filepath.Join(configRulesDir, "system.md")`
    - `filepath.Join(primaryRulesDir, "gemini.md")`
    - `filepath.Join(configRulesDir, "gemini.md")`
    - Uppercase variants (`SYSTEM_INSTRUCTIONS.md`, `SYSTEM.md`).

- [ ] **Step 4: Run unit tests with race detector**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/config`
  Expected: PASS.

- [ ] **Step 5: Commit Task 2**
  ```bash
  git add brain/pkg/config/config.go brain/pkg/config/config_test.go
  git commit -m "refactor(config): decouple native GEMINI.md from rule compiler, purge stale volume rules, and protect LKGC"
  ```

---

### Task 3: Update Watcher Filters & Tests in `brain/pkg/watcher`

**Files:**
- Modify: `brain/pkg/watcher/watcher.go`
- Modify: `brain/pkg/watcher/watcher_test.go`

**Interfaces:**
- Produces: Verified file watcher ignore and trigger filters for `GEMINI.md` and `user_persona.md`.

- [ ] **Step 1: Update `ShouldIgnore` in `brain/pkg/watcher/watcher.go`**
  Add `base == "user_persona.md"` to the ignore condition alongside `"system_instructions.md"`:
  ```go
  if base == ".git" || base == ".gemini" || base == "system_instructions.md" || base == "user_persona.md" {
      return true
  }
  ```

- [ ] **Step 2: Update `brain/pkg/watcher/watcher_test.go`**
  - Add `"user_persona.md"` and `"/share/aerial/.gemini/rules/user_persona.md"` to `ignoredPaths`.
  - Replace references to `SYSTEM.md` in `allowedPaths` with `GEMINI.md`, `/share/aerial/GEMINI.md`, and `/share/aerial-config/GEMINI.md`.

- [ ] **Step 3: Run watcher unit tests with race detector**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -v -race ./pkg/watcher`
  Expected: PASS.

- [ ] **Step 4: Commit Task 3**
  ```bash
  git add brain/pkg/watcher/watcher.go brain/pkg/watcher/watcher_test.go
  git commit -m "feat(watcher): ignore user_persona.md and allow canonical GEMINI.md triggers"
  ```

---

### Task 4: Add Startup Bootstrapping Sync in `brain/main.go` (Dedicated Context & 2-Tuple Signature)

**Files:**
- Modify: `brain/main.go`

**Interfaces:**
- Produces: Immediate synchronous `gitsync.SyncRepo` for `/share/aerial` during startup using a dedicated timeout context before compiling rules.

- [ ] **Step 1: Update `brain/main.go` startup bootstrapping**
  Immediately prior to `config.EnsureSystemRules`, allocate a dedicated active context and assign the 2-tuple return:
  ```go
  aerialSyncCtx, aerialSyncCancel := context.WithTimeout(context.Background(), 15*time.Second)
  if _, err := gitsync.SyncRepo(aerialSyncCtx, "/share/aerial"); err != nil {
      log.Printf("[Startup Bootstrapping] Notice: Bootstrapping sync for /share/aerial returned: %v (continuing with local tree)", err)
  }
  aerialSyncCancel()
  ```

- [ ] **Step 2: Verify full repo builds and tests pass**
  Run: `docker run --rm -v gopath-cache:/go -v "${PWD}/brain:/app" -w /app golang:1.22 go test -race ./...`
  Expected: PASS across all packages.

- [ ] **Step 3: Commit Task 4**
  ```bash
  git add brain/main.go
  git commit -m "feat(brain): add synchronous bootstrapping sync for /share/aerial on startup"
  ```

---

### Task 5: Update GitHub CI Matrix & Core Documentation

**Files:**
- Modify: `.github/workflows/docker-publish.yml`
- Modify: `README.md`
- Modify: `config.example.yaml`

**Interfaces:**
- Produces: Pruned CI trigger filters, updated documentation references, and smoke test build verification.

- [ ] **Step 1: Update `.github/workflows/docker-publish.yml`**
  Under `brain` paths-filter (lines 35–36), delete line 35 (`- 'SYSTEM.md'`), preserving existing `- 'GEMINI.md'`.

- [ ] **Step 2: Update `README.md` and `config.example.yaml`**
  - Update all 4 occurrences of `SYSTEM.md` in `README.md` to `GEMINI.md`.
  - Update comment in `config.example.yaml`.

- [ ] **Step 3: Local Docker build smoke test**
  Run: `docker build -f brain/Dockerfile -t aerial-brain:verify .`
  Expected: Build exits with code 0.

- [ ] **Step 4: Commit Task 5**
  ```bash
  git add .github/workflows/docker-publish.yml README.md config.example.yaml
  git commit -m "chore(infra): prune CI triggers, update documentation, and verify Dockerfile build"
  ```

---

### Task 6: Monorepo Verification, PR Creation & Auto-Merge Land

**Files:**
- Monorepo-wide verification & land

- [ ] **Step 1: Run monorepo pre-flight verification runner**
  Run: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1` (or `./scripts/verify.sh`)
  Expected: 100% PASS across all Go microservices, dashboard HUD unit tests, and linters.

- [ ] **Step 2: Push feature branch to remote**
  Run: `git push -u origin refactor/clean-break-gemini-rules`

- [ ] **Step 3: Open Pull Request via GitHub MCP**
  Title: `refactor(core): standardized GEMINI.md clean break and lean core architecture`

- [ ] **Step 4: Monitor CI status checks**
  Verify all 3 required checks pass on GitHub Actions:
  - `Run Service Unit Tests`
  - `Lint Microservices & Frontend`
  - `Build & Push Images to GHCR (brain)`

- [ ] **Step 5: Squash merge Pull Request to `main`**
  Merge PR via GitHub MCP, checkout local `main`, pull `--rebase`, and delete feature branch.
