# Technical Design Specification: Concurrent Deployment Pipelines Tracking

- **Author**: Aerial
- **Target Repository**: `azylman/aerial`
- **Status**: Hardened & Approved
- **Date**: 2026-09-17

---

## 1. Executive Summary

Aerial's Permet HUD Dashboard (`aerial-dashboard`) previously tracked continuous deployments via a singleton state machine (`dep-aerial-stack`). When multiple commits are in-flight across different phases of the lifecycle—such as commit B building in GitHub Actions CI while commit A is being reconciled and swapped on the host by `aerial-hangar`—the singleton model forces an artificial either/or choice: either CI masks host activity, or host swap activity overwrites CI state.

This technical design refactors the deployment tracking engine from a singleton model to a **Concurrent Multi-Pipeline Architecture**. Each active or recent commit diff maintains an independent, first-class deployment pipeline instance (`DeploymentStatus`). On the Permet HUD frontend, each in-flight diff renders as its own dedicated 5-step deployment card (`Commit Trigger` ➔ `CI Build & GHCR` ➔ `Hangar Sync` ➔ `Container Swap` ➔ `Health Check`) with scoped matrix chips, timers, and live progress indicators.

---

## 2. Goals & Key Requirements

1. **Concurrent Pipeline Isolation**:
   - Distinct, decoupled tracking for each active commit SHA.
   - Commit B executing CI does not overwrite, clobber, or hide Commit A undergoing host container swap.
   - Preserves 5-step chronological pipeline flow per commit without mixing chips or badges.

2. **Targeted Telemetry & Scoping**:
   - `CI Build & GHCR` step chips strictly reflect GitHub Actions matrix jobs for *that specific commit run*.
   - `Container Swap` step chips strictly reflect Hangar's `TargetServices` and container creation timestamps for *that specific reconciliation*.

3. **Smooth Lifecycle & State Coalescence**:
   - Commits seamlessly advance through pipeline stages: `queued` ➔ `building` ➔ `awaiting_pull` ➔ `pulling` ➔ `swapping` ➔ `live` ➔ `idle`.
   - When an active CI run completes and transitions to host reconciliation, the pipeline coalesces under the same commit SHA rather than creating duplicate tracks.
   - Completed pipelines remain visible in `live` stage during a 5-minute grace window, then cleanly evaporate.
   - When all pipelines are idle, the HUD renders the ambient `ALL SERVICES IN SYNC` card.

4. **Mobile-First & Purple Cyberpunk Permet HUD UX**:
   - Stacked card layout with 16px gap, responsive wrapping on mobile viewports (< 640px).
   - High-contrast cyberpunk palette: dark violet background (`#0d0b14`), neon purple laser (`#b026ff`) for swapping, cyan (`#00f0ff`) for pulling, emerald (`#00ff9f`) for live.
   - Touch-friendly tap targets (≥ 44px) for diagnostic drawer inspection.

5. **Invariants Compliance & Test Rigor**:
   - **Statement Coverage Floor ($\ge 95.0\%$)**: 100% verified via `scripts/check-coverage.sh`.
   - **Zero Markdown Tables**: Output conforms strictly to Discord bulleted key-value constraints.
   - **Backward Compatibility**: Preserves `/api/status` schema (`deployments: []DeploymentStatus`, `ongoing_deploy`, `is_deploying`).

---

## 3. Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL TELEMETRY SOURCES                         │
│                                                                             │
│  ┌─────────────────────────┐                 ┌───────────────────────────┐  │
│  │   GitHub Actions API    │                 │   aerial-hangar Sidecar   │  │
│  │   - GET /actions/runs   │                 │   - GET /status           │  │
│  │   - GET /jobs           │                 │   - Reconciliation State  │  │
│  └────────────┬────────────┘                 └─────────────┬─────────────┘  │
│               │                                            │                │
└───────────────┼────────────────────────────────────────────┼────────────────┘
                │                                            │
                ▼                                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  aerial-dashboard Engine (dashboard/main.go)                │
│                                                                             │
│   1. Fetch Active/Recent GitHub Runs (In-Progress, Queued, Recent Succeeded)│
│   2. Fetch Hangar Reconciliation Telemetry (State, Targets, CommitSHA)      │
│   3. Inspect Local Docker Containers (Image Revision Labels, Uptimes)       │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │           MergeClusterDeploymentsWithHangar (Pure Aggregator)        │  │
│   │   - Map Runs to Commit Pipelines (Stage: queued / building)          │  │
│   │   - Map Hangar Reconcile to Host Pipeline (Stage: pulling / swapping)│  │
│   │   - Map Container Rollout to Host Pipeline (Stage: swapping / live)  │  │
│   │   - Deduplicate by CommitSHA (Normalize to 7-char short SHA)         │  │
│   │   - Sort: In-Flight First (swapping > pulling > building), then Live │  │
│   └──────────────────────────────────┬───────────────────────────────────┘  │
│                                      │                                      │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       │ HTTP GET /api/status
                                       │ { deployments: [...], ongoing_deploy }
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       PERMET HUD FRONTEND (static/app.js)                   │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │ Deploy Card 1: Commit B (e.g. 9b6b2aa)                               │  │
│   │ [📦 Commit Trigger ✓] [⚙️ CI Build ⚡] [⬇️ Hangar ○] [🔄 Swap ○] ... │  │
│   │ CI Matrix Chips: proxy (running), dashboard (done)...                │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │ Deploy Card 2: Commit A (e.g. c79d95c)                               │  │
│   │ [📦 Commit Trigger ✓] [⚙️ CI Build ✓] [⬇️ Hangar ✓] [🔄 Swap ⚡] ... │  │
│   │ Host Swap Chips: aerial-dashboard (swapping)...                      │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Specifications

### 4.1 Backend Engine (`dashboard/main.go` & `dashboard/pure.go`)

1. **Multi-Pipeline Collection**:
   - Replace early-return singleton exits in `mergeClusterDeploymentsWithHangar` with a pipeline accumulator map: `map[string]*DeploymentStatus` keyed by normalized 7-character commit SHA.
   - For every in-progress or queued GitHub Actions run:
     - Instantiate or retrieve `DeploymentStatus` for `run.HeadCommit.SHA`.
     - Populate `Stage: building` (or `queued`), progress, run timer, HTML run logs URL, and CI matrix jobs.
   - For active Hangar reconciliation:
     - Resolve target commit from `gitSync.Reconciliation.CommitSHA` (or disk HEAD if commit SHA is omitted).
     - If the commit already exists from a completed CI run, advance its stage to `pulling` or `swapping`.
     - Scope matrix chips via `BuildTargetContainerChips(aerialContainers, gitSync.Reconciliation.TargetServices, gitSync.Reconciliation.StartedAt, now)`.
   - For local container rolling swaps (`minUptimeSec < 120` or unswapped containers):
     - Identify newest container commit revision.
     - Advance or attach host swap pipeline stage.
   - For recent completions:
     - Retain completed pipelines in stage `live` if completion or container creation was within the 300s (5-minute) grace window.

2. **Pipeline Ordering & Priority**:
   - Convert map to slice ordered deterministically:
     1. Active host swaps (`swapping`, `pulling`)
     2. Active CI builds (`building`, `queued`)
     3. Failed pipelines (`failed`, `degraded`)
     4. Live grace pipelines (`live`)
   - Compute `ongoing_deploy` macro string from highest-priority active pipeline stage.
   - Compute `is_deploying = len(activePipelines) > 0`.

### 4.2 Frontend Presentation (`dashboard/static/app.js` & `style.css`)

1. **Multi-Card Rendering**:
   - `renderDeployments(deployments)` iterates over `deployments` slice.
   - Each entry produces an independent `.deploy-card` element.
   - Laser scan animation (`.deploy-card-laser`) runs on all cards in active stages (`building`, `pulling`, `swapping`).

2. **Step Scoping**:
   - In step 2 (`CI Build & GHCR`): render CI matrix chips (filter out host container names if needed).
   - In step 4 (`Container Swap`): render host container chips (filter out CI test/lint jobs).

3. **Top-Level Header Badge (`#deploy-count-badge`)**:
   - If `deployments.length === 0`: `SYSTEM IN SYNC` (neutral badge).
   - If `activeDeploys.length === 1`: `⚡ 1 CI BUILD ACTIVE` or `🔄 HANGAR SWAPPING`.
   - If `activeDeploys.length > 1`: `${activeDeploys.length} DEPLOYS IN PROGRESS` (pulsing active badge).
   - If any failed: `🚨 DEPLOY FAILED` (red alert badge).

4. **Mobile & PWA Optimizations**:
   - Container cards use CSS grid / flex with `min-width: 0` to prevent horizontal blowouts on phones.
   - Step icons and badges remain clearly visible on narrow viewports without truncation.

---

## 5. Error Handling & Edge Cases

• **Orphaned CI Runs**: If a GitHub Actions run hangs or becomes orphaned, GitHub's run conclusion timeout (6 hours max, but our tracker bounds CI visibility to runs updated within the last 60 minutes) prevents permanent phantom cards.
• **Concurrent Reconciliations on Host**: Hangar uses `sync.Mutex` (`reconcileStateMu`) and debounced channels (`reconcileCh`) to ensure host reconciliations run sequentially; the dashboard correctly observes the active reconciliation while queueing subsequent runs.
• **Flapping Network / API Errors**: If GitHub API fails with 403 (rate limit) or Hangar is temporarily unreachable, existing cached pipelines are preserved with soft warnings rather than flashing blank cards.
• **Unswapped Stale Containers**: Detected via `BuildTargetContainerChips` timestamp comparison; unswapped services show as pending/active until Docker confirms healthy status.

---

## 6. Verification & Test Plan

1. **Unit Tests (`dashboard/pure_test.go`)**:
   - `TestMergeClusterDeployments_ConcurrentPipelines`: Mock concurrent CI run (Commit B) and Hangar swap (Commit A); verify both returned in slice.
   - `TestMergeClusterDeployments_Deduplication`: Mock CI run and Hangar swap with identical commit SHA; verify single unified deployment object with advanced stage.
   - `TestMergeClusterDeployments_GraceWindowExpiry`: Verify live deploy past 300s cleanly drops from return list.
   - `TestMergeClusterDeployments_PrecedenceOrdering`: Verify swapping appears before building, which appears before live.

2. **Integration Tests (`dashboard/main_test.go`)**:
   - Mock HTTP handlers for GitHub API and Hangar `/status` returning concurrent telemetry payloads; verify `/api/status` JSON response structure and fields.

3. **Monorepo Standards & Coverage Floor**:
   - Run `scripts/verify.sh` for monorepo-wide pass.
   - Run `scripts/check-coverage.sh --service dashboard` to ensure statement coverage $\ge 95.0\%$.

---

## 7. Spec Self-Review Checklist

• **Placeholder scan**: Zero "TBD", "TODO", or vague requirements.
• **Internal consistency**: Data models, field names (`ongoing_deploy`, `TargetServices`, `CommitSHA`), and frontend classes match existing code conventions.
• **Scope check**: Bounded strictly to `dashboard` backend and frontend; zero changes required to Hangar or CI workflows.
• **Ambiguity check**: Stage precedence, deduplication key, and chip separation criteria are explicit.
