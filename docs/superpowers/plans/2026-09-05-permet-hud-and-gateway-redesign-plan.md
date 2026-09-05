# Permet Command HUD v3.0, Tactical Gateway Launcher & GitSync Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the status dashboard UI from scratch with an elevated Permet Mecha aesthetic, an extensible Tactical Quick-Launch Gateway (core + dynamic config links), real-time GitSync latency telemetry ($\Delta t$), and mobile-friendly responsive layout.

**Architecture:** A unified gateway architecture where `aerial-proxy` routes sub-paths (`/dashboard/`, `/conversations/`, `/docs/`, `/grafana/`, `/ui-testing/`), `aerial-gitsync` exposes authoritative repository checkout status via `GET /status`, `aerial-dashboard` ingests telemetry and dynamic config links from `config.yaml`, and a Vanilla JS/CSS Permet HUD single-page application renders the command bridge.

**Tech Stack:** Go 1.22+, Nginx Alpine, Vanilla JS (ES6+), CSS3 Custom Properties & Glassmorphism, Node.js / JSDOM / Jest.

**Spec:** `docs/superpowers/specs/2026-09-05-permet-hud-and-gateway-redesign.md`

## Global Constraints
- Zero external build dependencies (pure Vanilla JS + CSS embedded via Go `embed.FS`).
- Strict token hygiene and secret sanitization on all inspect/status APIs.
- Mobile-first responsive optimization (< 768px touch targets >= 44px, bottom diagnostic sheet).
- Physical immutability compliance: Changes to `aerial` and `aerial-config` follow automated Git/PR workflows.

---

### Task 1: Reverse Proxy Configuration for UI Testing & App Gateway

**Files:**
- Modify: `proxy/default.conf`
- Test: Configuration syntax validation

**Interfaces:**
- Produces: `/ui-testing/` route pointing to `http://aerial-brain:52341` with WebSocket, HTTP 1.1 upgrade, and SSE support.

- [ ] **Step 1: Write test for proxy config**
  Verify `proxy/default.conf` includes `/ui-testing/` and `/ui-testing` rewrite rules.
- [ ] **Step 2: Add `/ui-testing/` reverse proxy block to `proxy/default.conf`**
- [ ] **Step 3: Verify Nginx configuration syntax**

---

### Task 2: Authoritative GitSync Sidecar Status API (`GET /status`)

**Files:**
- Modify: `sidecars/gitsync/main.go`
- Modify: `sidecars/gitsync/main_test.go`

**Interfaces:**
- Produces: `GET /status` returning JSON with `disk_commit`, `disk_commit_time`, `github_commit`, `github_commit_time`, `time_lag_seconds`, `sync_status`, `last_sync_time`.

- [ ] **Step 1: Write failing unit test in `sidecars/gitsync/main_test.go`**
  Test `GET /status` handler returns status 200, valid repository metadata, and calculates `time_lag_seconds`.
- [ ] **Step 2: Run test to confirm failure**
  `go test -v ./sidecars/gitsync/...`
- [ ] **Step 3: Implement `GET /status` in `sidecars/gitsync/main.go`**
  Add `StatusHandler` and helper to extract commit SHAs and author dates from git repositories.
- [ ] **Step 4: Run unit tests to confirm passing**
  `go test -v ./sidecars/gitsync/...`

---

### Task 3: Dashboard Backend Gateway & Dynamic Config Links

**Files:**
- Modify: `dashboard/main.go`
- Modify: `dashboard/main_test.go`

**Interfaces:**
- Consumes: `http://aerial-gitsync:8080/status`, `config.yaml` (`dashboard.quick_launch_links`).
- Produces: `/api/status` with `git_sync` object and `quick_launch_links` array.

- [ ] **Step 1: Write failing unit tests in `dashboard/main_test.go`**
  Test merging of core quick-launch links with custom links from `config.yaml`.
  Test ingestion of `git_sync` telemetry from `aerial-gitsync` with graceful fallback when gitsync is offline.
- [ ] **Step 2: Run test to confirm failure**
  `go test -v ./dashboard/...`
- [ ] **Step 3: Implement GitSync polling and Config Links parser in `dashboard/main.go`**
  Add `fetchGitSyncStatus` and `loadQuickLaunchLinks` functions.
- [ ] **Step 4: Run tests to confirm passing**
  `go test -v ./dashboard/...`

---

### Task 4: Permet Mecha Design System & CSS Overhaul

**Files:**
- Modify: `dashboard/static/style.css`

**Interfaces:**
- Produces: Permet Mecha design tokens, glassmorphism panels, cybernetic scanlines, laser progress bars, tactical quick-launch dock styling, and mobile media queries (< 768px).

- [ ] **Step 1: Implement CSS custom properties and theme tokens**
  Obsidian dark (`#07090e`), Permet cyan (`#00f0ff`), GUND-bit crimson (`#ff3366`), emerald (`#00ff9d`), solar amber (`#ffb700`).
- [ ] **Step 2: Style Tactical Quick-Launch Dock & Header Bar**
  Add interactive launcher chips, hover glows, and status pulse dots.
- [ ] **Step 3: Style GitSync Telemetry Pill & Deploy Pipeline Cards**
- [ ] **Step 4: Implement Mobile Viewport Breakpoints (< 768px)**
  Touch targets, swipeable quick-launch bar, vertical deploy steps, slide-up bottom sheet for diagnostic drawer.

---

### Task 5: Permet Command HUD HTML Shell & Component Structure

**Files:**
- Modify: `dashboard/static/index.html`

**Interfaces:**
- Produces: Rebuilt HTML semantic layout with brand dial, Permet Score meter, Quick-Launch Dock container, Operations Bridge deck (Deploy Pipeline with GitSync pill, Live Agent Queue, Stack Grid), and diagnostic drawer.

- [ ] **Step 1: Rebuild `index.html` layout**
  Add quick-launch dock container (`#quick-launch-dock`), Permet Score gauge, and restructured Deploy Pipeline section.
- [ ] **Step 2: Validate semantic HTML and ARIA accessibility attributes**

---

### Task 6: Frontend App Logic, GitSync Duration Formatting & JSDOM Test Suite

**Files:**
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/app.test.js`

**Interfaces:**
- Consumes: `/api/status` (including `git_sync` and `quick_launch_links`).
- Produces: Dynamic DOM updates for quick-launch chips, GitSync duration delta badge (`0s`, `45s`, `2m 15s`), 5-stage deployment pipeline, live agent queue, and mobile drawer interactions.

- [ ] **Step 1: Write failing frontend unit tests in `dashboard/app.test.js`**
  Test rendering of core and custom quick-launch links.
  Test GitSync duration formatting and badge states (`synced`, `behind`, `error`).
  Test mobile drawer toggle behavior.
- [ ] **Step 2: Run frontend tests to confirm failure**
  `npm test --prefix dashboard` (or node runner test script)
- [ ] **Step 3: Implement client rendering functions in `dashboard/static/app.js`**
  Implement `renderQuickLaunchDock`, `renderGitSyncBadge`, and updated `renderDeployments`.
- [ ] **Step 4: Run frontend tests to confirm passing**
  `npm test --prefix dashboard`

---

### Task 7: User Configuration Custom Link in `aerial-config`

**Files:**
- Modify: `config.yaml` in `aerial-config`

**Interfaces:**
- Produces: `dashboard.quick_launch_links` containing `HOME` link pointing to `https://home.zylman.com`.

- [ ] **Step 1: Add `dashboard.quick_launch_links` to `config.yaml`**
- [ ] **Step 2: Validate YAML syntax**
- [ ] **Step 3: Submit via `aerial-config-pr.sh` and verify live sync**

---

### Task 8: Full Monorepo Pre-Flight Verification & Review Panel Audit

**Files:**
- Run: `verify.sh`

- [ ] **Step 1: Run full monorepo test and lint suite**
  `./scripts/verify.sh`
- [ ] **Step 2: Consult the 4-expert review panel (The Girl Gang) for final code audit**
- [ ] **Step 3: Commit, push to `main`, and verify automated CI build and Watchtower rollout**
