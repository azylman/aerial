# Technical Design Specification: Concurrent Deployment Pipelines Tracking

- **Author**: Aerial
- **Target Repository**: `azylman/aerial`
- **Status**: Hardened & Approved by The Girl Gang Review Panel
- **Date**: 2026-09-17

---

## 1. Executive Summary

Aerial's Permet HUD Dashboard (`aerial-dashboard`) previously tracked continuous deployments via a singleton state machine (`dep-aerial-stack`). When multiple commits are in-flight across different phases of the lifecycle—such as commit B building in GitHub Actions CI while commit A is being reconciled and swapped on the host by `aerial-hangar`—the singleton model forces an artificial either/or choice: either CI masks host activity, or host swap activity overwrites CI state.

This hardened technical design refactors the deployment tracking engine from a singleton model to a **Concurrent Multi-Pipeline Architecture**. Each active or recent commit diff maintains an independent, first-class deployment pipeline instance (`DeploymentStatus`). On the Permet HUD frontend, in-flight diffs render as dedicated 5-step deployment cards (`Commit Trigger` ➔ `CI Build & GHCR` ➔ `Hangar Sync` ➔ `Container Swap` ➔ `Health Check`) with strictly scoped matrix chips, timers, and live progress indicators.

---

## 2. Goals & Key Requirements

1. **Concurrent Pipeline Isolation**:
   - Distinct, decoupled tracking for each active commit SHA.
   - Commit B executing CI does not overwrite, clobber, or hide Commit A undergoing host container swap.
   - Preserves 5-step chronological pipeline flow per commit without mixing chips or badges.

2. **Targeted Telemetry & Scoping**:
   - `CI Build & GHCR` step chips strictly reflect GitHub Actions matrix jobs for *that specific commit run*.
   - `Container Swap` step chips strictly reflect Hangar's `TargetServices` and container creation timestamps for *that specific reconciliation*.
   - Differentiate CI chips from container chips via explicit `type: "ci" | "container"` metadata to prevent telemetry loss during stage transitions.

3. **Smooth Lifecycle & State Coalescence**:
   - Commits seamlessly advance through pipeline stages: `queued` ➔ `building` ➔ `awaiting_pull` ➔ `pulling` ➔ `swapping` ➔ `live` ➔ `idle`.
   - Ingest both active runs and recent completed runs (< 30m) into the state accumulator to eliminate the CI-to-Hangar handover void.
   - Strict monotonic state progression: pipelines advance forward and never retrogress from `live` back to `pulling`.
   - Max 2 fully expanded cards simultaneously; completed or older pipelines collapse into compact 1-line cyber pill drawers.
   - Dynamically compress `live` grace window from 300s to 60s when concurrent pipelines are active to prevent HUD scroll bloat.

4. **Mobile-First & Purple Cyberpunk Permet HUD UX**:
   - Stacked card layout with 16px gap, responsive wrapping on mobile viewports (< 640px).
   - Fix mobile CSS selector (`.deploy-steps-grid`) and add `flex-wrap: wrap` to headers.
   - Expand touch targets for matrix chips to $\ge 44\text{px}$ touch hit boundaries.
   - Decouple CSS styling: neon purple laser (`#b026ff`) for swapping, cyan (`#00f0ff`) for pulling, and emerald (`#00ff9f`) for live.

5. **Invariants Compliance & Test Rigor**:
   - **Statement Coverage Floor ($\ge 95.0\%$)**: 100% verified via `scripts/check-coverage.sh` across both Go services and Permet HUD frontend (`dashboard/app.test.js`).
   - **Zero Markdown Tables**: Conforms strictly to Discord bulleted key-value constraints.
   - **Deterministic Map Ordering**: Sort by stage rank descending, then `StartedAt` descending, then `Commit` lexicographically.
   - **Safe Nil Handling**: Access `run.HeadSHA` with safe length bounding and nil `HeadCommit` fallbacks.

---

## 3. Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL TELEMETRY SOURCES                         │
│                                                                             │
│  ┌─────────────────────────┐                 ┌───────────────────────────┐  │
│  │   GitHub Actions API    │                 │   aerial-hangar Sidecar   │  │
│  │   - GET /actions/runs   │                 │   - GET /status           │  │
│  │   - (per_page=10, <30m) │                 │   - Reconciliation State  │  │
│  └────────────┬────────────┘                 └─────────────┬─────────────┘  │
│               │                                            │                │
└───────────────┼────────────────────────────────────────────┼────────────────┘
                │                                            │
                ▼                                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  aerial-dashboard Engine (dashboard/pure.go)                │
│                                                                             │
│   1. Fetch Active & Recent GitHub Runs (In-Progress, Queued, Succeeded <30m)│
│   2. Fetch Hangar Reconciliation Telemetry (State, Targets, CommitSHA)      │
│   3. Inspect Local Docker Containers (Image Revision Labels, Uptimes)       │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │           MergeClusterDeploymentsWithHangar (Pure Aggregator)        │  │
│   │   - Map Runs to Commit Pipelines (Stage: queued / building)          │  │
│   │   - Bridge Completed Runs to Host Reconcile (Stage: awaiting_pull)   │  │
│   │   - Map Hangar Reconcile to Host Pipeline (Stage: pulling / swapping)│  │
│   │   - Group Containers by Image Revision (Stage: swapping / live)      │  │
│   │   - Canonical 40-Char SHA Deduplication                              │  │
│   │   - Deterministic Sort: Rank desc > StartedAt desc > Commit asc      │  │
│   │   - Defensive Cardinality Clamp (Max 3-5 pipelines)                  │  │
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
│   │ Deploy Card 1: Commit B (e.g. 9b6b2aa) [ACTIVE / EXPANDED]           │  │
│   │ [📦 Commit Trigger ✓] [⚙️ CI Build ⚡] [⬇️ Hangar ○] [🔄 Swap ○] ... │  │
│   │ CI Matrix Chips: proxy (running), dashboard (done)...                │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │ Deploy Card 2: Commit A (e.g. c79d95c) [ACTIVE / EXPANDED]           │  │
│   │ [📦 Commit Trigger ✓] [⚙️ CI Build ✓] [⬇️ Hangar ✓] [🔄 Swap ⚡] ... │  │
│   │ Host Swap Chips: aerial-dashboard (swapping)...                      │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │ [c5340be] aerial-stack • LIVE (2m ago) ▾ [COMPACT CYBER PILL DRAWER] │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Detailed Component Specifications

### 4.1 Backend Engine (`dashboard/pure.go` & `dashboard/main.go`)

1. **Relocate Aggregator to `dashboard/pure.go`**:
   - Relocate `mergeClusterDeploymentsWithHangar` into `dashboard/pure.go` as a pure, hermetically testable function:
     ```go
     func MergeClusterDeploymentsWithHangar(
         runs []GitHubRun,
         jobs map[int64][]GitHubJob,
         gitSync GitSyncStatusResponse,
         aerialContainers []DockerContainerJSON,
         currentCommit string,
         refTime time.Time,
     ) []DeploymentStatus
     ```
   - Provide backward-compatible forwarders in `dashboard/main.go`.

2. **Telemetry Ingestion & Safe Access**:
   - Increase GitHub API polling pagination from `per_page=3` to `per_page=10` to avoid dropping rapid pushes.
   - Always extract commit SHA via `run.HeadSHA` (checking non-empty before falling back to `run.HeadCommit.SHA`).
   - Validate SHAs against hexadecimal format (`^[0-9a-fA-F]{7,40}$`) before canonical keying.

3. **Multi-Pipeline Accumulator (`map[string]*DeploymentStatus`)**:
   - Key the map by canonical full lowercase SHA (or synthetic ID `gh-run-<id>` when absent).
   - Ingest:
     - **Active CI Runs (`queued`, `in_progress`)**: Stage `queued` or `building` with CI matrix chips.
     - **Recent Succeeded CI Runs (< 30m)**: If not yet picked up by Hangar or local containers, place in Stage `awaiting_pull` (bridge state) to eliminate the handover void.
     - **Active Hangar Reconciliation (`pulling`, `swapping`)**: Resolve target commit from `gitSync.Reconciliation.CommitSHA` (or disk HEAD). Advance stage to `pulling` or `swapping` and attach scoped host container chips.
     - **Local Container Groups**: Group running containers by image revision label (`org.opencontainers.image.revision` or `aerial.commit_sha`). If a commit has containers starting or with uptime < 120s, track as `swapping`. If all containers for that commit are healthy and recent, track as `live`.
   - **Out-of-Order Fast-Forward Handling**: If a completed CI run's commit is an ancestor of the currently running host commit, mark it `superseded` or fold into `live` rather than tripping a 120s timeout failure.

4. **Register `"pulling"` in `pure.go`**:
   - Update `stageRank` in `pure.go`:
     ```go
     stageRank := map[string]int{
         "swapping":      6,
         "pulling":       5,
         "building":      4,
         "awaiting_pull": 3,
         "queued":        2,
         "failed":        1,
         "degraded":      1,
         "live":          0,
     }
     ```
   - Add `"pulling"` to `IsDeployOngoing(stage)` so `is_deploying: true` remains active during image pulls.

5. **Deterministic Sort & Cardinality Clamp**:
   - Sort pipelines:
     1. Stage priority descending (`swapping` > `pulling` > `building` > `awaiting_pull` > `failed` > `live`)
     2. `StartedAt` descending (newest first)
     3. `Commit` lexicographically ascending
   - Clamp the final slice to a maximum of 3–5 pipelines to prevent HUD DOM bloat.

### 4.2 Matrix Chip Scoping & Discriminator

1. **`MatrixJobChip` Domain Type**:
   - Add `Type string` (`"ci"` vs `"container"`) to `MatrixJobChip`:
     ```go
     type MatrixJobChip struct {
         Name       string `json:"name"`
         Status     string `json:"status"`
         Conclusion string `json:"conclusion,omitempty"`
         Duration   string `json:"duration,omitempty"`
         Type       string `json:"type,omitempty"` // "ci" | "container"
     }
     ```
   - In `parseMatrixJobChips`, assign `Type: "ci"`.
   - In `BuildTargetContainerChips`, assign `Type: "container"`.
   - Store both collections in `DeploymentStatus.MatrixJobs` without clobbering each other.

### 4.3 Frontend Layout & Permet HUD UX (`dashboard/static/app.js` & `style.css`)

1. **Multi-Card Rendering & Visual Hierarchy**:
   - Max 2 expanded `.deploy-card` elements.
   - Any 3rd+ pipeline or completed `live` pipeline (> 60s) renders as a compact 1-line cyber pill drawer:
     ```html
     <div class="deploy-card-compact stage-live">
         <span class="compact-commit">c79d95c</span>
         <span class="compact-title">aerial-stack</span>
         <span class="compact-badge live">✓ LIVE (2m ago)</span>
         <span class="compact-toggle">▾</span>
     </div>
     ```
2. **Step Scoping**:
   - Step 2 (`CI Build & GHCR`): filters chips by `c.type === 'ci'` (or fallback to test/lint/build keywords).
   - Step 4 (`Container Swap`): filters chips by `c.type === 'container'` (or fallback to non-CI names).

3. **Top-Level Section Badge Logic Fix**:
   - Patch `app.js` badge evaluation to evaluate multi-deploy concurrency BEFORE single-phase states:
     ```javascript
     if (hasFailed) {
         deployBadge.textContent = '🚨 CI BUILD FAILED';
         deployBadge.className = 'section-badge failed';
     } else if (hasDegraded) {
         deployBadge.textContent = '⚠️ STACK DEGRADED';
         deployBadge.className = 'section-badge failed';
     } else if (activeDeploys.length > 1) {
         deployBadge.textContent = `${activeDeploys.length} DEPLOYS IN PROGRESS`;
         deployBadge.className = 'section-badge active';
     } else if (isSwapping) {
         deployBadge.textContent = '🔄 HANGAR SWAPPING';
         deployBadge.className = 'section-badge swapping';
     } else if (isPulling) {
         deployBadge.textContent = '⬇️ HANGAR PULLING';
         deployBadge.className = 'section-badge pulling';
     } else if (isBuilding) {
         deployBadge.textContent = '⚡ 1 CI BUILD ACTIVE';
         deployBadge.className = 'section-badge building';
     } else if (isAwaitingPull) {
         deployBadge.textContent = '⬇️ AWAITING HANGAR SYNC';
         deployBadge.className = 'section-badge active';
     }
     ```

4. **Styling & Cyberpunk Accents (`style.css`)**:
   - Separate `.stage-swapping` (border: `rgba(176, 38, 255, 0.4)`, text/badge: `#b026ff`) from `.stage-pulling` (border: `rgba(0, 240, 255, 0.4)`, text/badge: `#00f0ff`).
   - Fix responsive typo: change `.steps-grid` at line 2445 to `.deploy-steps-grid`.
   - Add `flex-wrap: wrap` and `gap: 8px` to `.deploy-card-header`.
   - Expand touch hit boundaries on `.matrix-chip` to satisfy PWA touch target standards ($\ge 44\text{px}$).

---

## 5. Error Handling & Edge Cases

• **Orphaned / Cancelled CI Runs**: Runs with `Conclusion == "cancelled"` immediately mark the pipeline as failed/cancelled rather than hanging. Active CI runs are bounded to updates within the last 15 minutes.
• **Hangar Unreachable Heartbeat**: If Hangar is unreachable for > 30s during an active swap, the pipeline surfaces a clear diagnostic error rather than freezing indefinitely.
• **CrashLoopBackOff Traps**: If a container in rolling swap has `RestartCount > 0`, the pipeline flags `degraded` immediately.
• **Fast-Forwarded Commits**: Commits whose changes were superseded by newer deployments on `main` coalesce smoothly into `live` without triggering false 120s timeout errors.

---

## 6. Verification & Test Plan

1. **Pure Go Unit Tests (`dashboard/pure_test.go`)**:
   - `TestMergeClusterDeployments_ConcurrentCIAndSwap`: Verify Commit B (building) and Commit A (swapping) return concurrently.
   - `TestMergeClusterDeployments_AwaitingPullBridge`: Verify completed CI run transitions to `awaiting_pull` without vanishing.
   - `TestMergeClusterDeployments_DeterministicSort`: Verify randomized map iteration always produces stable sorted order.
   - `TestMergeClusterDeployments_MultiCommitContainerGrouping`: Verify earlier commit maintains `live` grace when newer commit swaps containers.
   - `TestMergeClusterDeployments_SupersededFastForward`: Verify older ancestor commits coalesce without false timeouts.
   - `TestMergeClusterDeployments_SafeNilHandling`: Verify nil `HeadCommit` or short SHAs never panic.
   - `TestMergeClusterDeployments_CardinalityClamp`: Verify excess pipelines clamp to maximum 5.
   - `TestCalculateDeployStatus_Pulling`: Verify `"pulling"` has rank 5 and keeps `IsDeployOngoing == true`.

2. **Frontend Unit Tests (`dashboard/app.test.js`)**:
   - Verify multi-card rendering when `deployments` has length > 1.
   - Verify compact cyber pill drawer rendering for 3rd+ or live cards.
   - Verify `#deploy-count-badge` displays `${activeDeploys.length} DEPLOYS IN PROGRESS` when multiple deploys run.
   - Verify chip scoping distinguishes CI chips from container chips.

3. **Coverage & Monorepo Pass**:
   - `scripts/verify.sh` passes 100% across all linters, BOM checks, and tests.
   - `scripts/check-coverage.sh --service dashboard` achieves $\ge 95.0\%$ statement coverage on Go and frontend.
