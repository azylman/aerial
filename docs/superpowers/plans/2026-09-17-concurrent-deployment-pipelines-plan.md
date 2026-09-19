# Concurrent Deployment Pipelines Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the Aerial dashboard deployment tracking engine from a singleton early-exit state machine to a concurrent multi-pipeline architecture that independently tracks in-flight GitHub Actions CI runs and host Hangar reconciliations across distinct commit diffs on the Permet HUD.

**Architecture:** Relocate deployment aggregation logic into pure, hermetically testable functions in `dashboard/pure.go` using a canonical SHA-keyed accumulator map with deterministic 3-tier sorting, `awaiting_pull` bridge states, and image-revision container grouping. Enhance `dashboard/static/app.js` and `style.css` to render stacked, cyberpunk-styled deployment cards (capped at 2 expanded, with compact drawers for overflow) with strictly scoped CI vs container matrix chips and responsive mobile touch targets.

**Tech Stack:** Go 1.24, Vanilla JavaScript (ES6+), CSS3 (Permet HUD Cyberpunk theme), Jest / Node test harness.

**Spec:** `docs/superpowers/specs/2026-09-17-concurrent-deployment-pipelines-design.md`

## Global Constraints

- **Zero Markdown Tables**: NEVER format status or documentation using markdown tables.
- **Statement Coverage Floor**: Must maintain $\ge 95.0\%$ statement coverage on `dashboard` (both Go and frontend JS) verified via `scripts/check-coverage.sh`.
- **Full Monorepo Verification**: `scripts/verify.sh` must pass 100% cleanly before completion.
- **Strict Backward Compatibility**: `/api/status` schema must retain `deployments: []DeploymentStatus`, `ongoing_deploy: string`, and `is_deploying: bool`.
- **Deterministic Ordering**: Sort pipelines by Stage Rank descending (`swapping` > `pulling` > `building` > `awaiting_pull` > `failed` > `live`), then `StartedAt` descending, then `Commit` lexicographically.

---

### Task 1: Relocate & Implement Core Pipeline Aggregator in `dashboard/pure.go`

**Files:**
- Modify: `dashboard/pure.go`
- Test: `dashboard/pure_test.go`

**Interfaces:**
- Consumes: `GitHubRun`, `GitHubJob`, `GitSyncStatusResponse`, `DockerContainerJSON`, `DeploymentStatus`, `MatrixJobChip`
- Produces: `MergeClusterDeploymentsWithHangar(runs []GitHubRun, jobs map[int64][]GitHubJob, gitSync GitSyncStatusResponse, aerialContainers []DockerContainerJSON, currentCommit string, refTime time.Time) []DeploymentStatus`

- [ ] **Step 1: Write the failing tests in `dashboard/pure_test.go`**

Add table-driven unit tests verifying:
1. `TestCalculateDeployStatus_Pulling`: Verifies `"pulling"` has rank 5 and `IsDeployOngoing("pulling") == true`.
2. `TestMergeClusterDeployments_ConcurrentPipelines`: Mock Commit B in CI (`building`) and Commit A in Hangar (`swapping`), verifying both returned concurrently in slice.
3. `TestMergeClusterDeployments_AwaitingPullBridge`: Mock completed CI run (<30m) transitioning to `awaiting_pull` without disappearing.
4. `TestMergeClusterDeployments_DeterministicSort`: Verify randomized map keys sort consistently: `swapping` > `pulling` > `building` > `awaiting_pull` > `live`.
5. `TestMergeClusterDeployments_MultiCommitContainerGrouping`: Verify earlier commit maintains `live` grace when newer commit swaps containers.
6. `TestMergeClusterDeployments_ChipScoping`: Verify CI matrix chips have `Type: "ci"` and host container chips have `Type: "container"`.
7. `TestMergeClusterDeployments_SafeNilHandling`: Verify nil `HeadCommit` and malformed SHAs never panic.
8. `TestMergeClusterDeployments_CardinalityClamp`: Verify excess pipelines clamp to max 5.

```go
func TestCalculateDeployStatus_Pulling(t *testing.T) {
	if got := IsDeployOngoing("pulling"); !got {
		t.Fatalf("expected IsDeployOngoing(\"pulling\") to be true, got false")
	}
	deps := []DeploymentStatus{
		{Stage: "building"},
		{Stage: "pulling"},
	}
	if got := CalculateDeployStatus(deps); got != "pulling" {
		t.Fatalf("expected CalculateDeployStatus to return pulling, got %s", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./dashboard -run TestCalculateDeployStatus_Pulling`
Expected: FAIL with "expected IsDeployOngoing("pulling") to be true, got false"

- [ ] **Step 3: Implement minimal pure aggregator changes in `dashboard/pure.go`**

1. Add `Type string` to `MatrixJobChip`.
2. Update `stageRank` map in `pure.go` to include `"pulling": 5`, adjust `"building": 4`, `"awaiting_pull": 3`, `"queued": 2`, `"failed": 1`, `"degraded": 1`, `"live": 0`.
3. Add `"pulling"` to `IsDeployOngoing`.
4. Implement `MergeClusterDeploymentsWithHangar` in `dashboard/pure.go` with canonical 40-char SHA map, safe `run.HeadSHA` handling, recent succeeded run ingestion into `awaiting_pull`, scoped container chips, deterministic sort, and cardinality clamping.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -v -cover ./dashboard -run "TestCalculateDeployStatus|TestMergeClusterDeployments"`
Expected: PASS with 0 failures.

- [ ] **Step 5: Commit changes**

```bash
git add dashboard/pure.go dashboard/pure_test.go
git commit -m "feat(dashboard): implement concurrent multi-pipeline aggregator in pure.go"
```

---

### Task 2: Update `dashboard/main.go` Integration & Telemetry Ingestion

**Files:**
- Modify: `dashboard/main.go`
- Test: `dashboard/main_test.go`

**Interfaces:**
- Consumes: `MergeClusterDeploymentsWithHangar` from `dashboard/pure.go`
- Produces: Updated `/api/status` response with concurrent `deployments: []DeploymentStatus`

- [ ] **Step 1: Write integration tests in `dashboard/main_test.go`**

Add tests verifying `/api/status` HTTP endpoint returns multiple deployments when GitHub has an active run and Hangar has an active swap:
`TestStatusHandler_ConcurrentPipelines`: Mock GitHub API with run for Commit B and Hangar `/status` with swap for Commit A, call `statusHandler`, and assert response JSON contains 2 deployments with correct stages and `ongoing_deploy: "swapping"`.

- [ ] **Step 2: Run integration tests to verify failure**

Run: `go test -v ./dashboard -run TestStatusHandler_ConcurrentPipelines`
Expected: FAIL (currently only 1 deployment returned).

- [ ] **Step 3: Wire `MergeClusterDeploymentsWithHangar` into `dashboard/main.go`**

1. In `dashboard/main.go`, update `pollGitHubRuns` URL pagination from `per_page=3` to `per_page=10`.
2. Replace the internal body of `mergeClusterDeploymentsWithHangar` in `dashboard/main.go` with a direct call to `MergeClusterDeploymentsWithHangar` from `pure.go`.
3. Ensure `statusHandler` propagates the full `deployments` slice to the JSON response.

- [ ] **Step 4: Run integration tests to verify they pass**

Run: `go test -v -cover ./dashboard -run TestStatusHandler_ConcurrentPipelines`
Expected: PASS.

- [ ] **Step 5: Commit changes**

```bash
git add dashboard/main.go dashboard/main_test.go
git commit -m "feat(dashboard): wire concurrent pipeline aggregator to statusHandler with increased run pagination"
```

---

### Task 3: Permet HUD Frontend Multi-Card Rendering & Cyberpunk Styling

**Files:**
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/style.css`
- Test: `dashboard/app.test.js`

**Interfaces:**
- Consumes: `/api/status` JSON containing `deployments: []DeploymentStatus` with `MatrixJobs` containing `type: "ci" | "container"`
- Produces: Rendered multi-card Permet HUD DOM with laser scans, compact drawer variants, and touch-friendly chip diagnostics

- [ ] **Step 1: Write failing frontend tests in `dashboard/app.test.js`**

Add tests verifying:
1. `renderDeployments`: Renders multiple `.deploy-card` elements when `deployments.length === 2`.
2. Compact Drawer: When 3+ deployments exist or when a deployment is `stage-live`, renders `.deploy-card-compact`.
3. Badge Precedence: When `activeDeploys.length === 2`, `#deploy-count-badge` renders `2 DEPLOYS IN PROGRESS`.
4. Chip Scoping: Step 2 contains only `type === 'ci'` chips, Step 4 contains only `type === 'container'` chips.

- [ ] **Step 2: Run frontend test to verify failure**

Run: `npm test --prefix dashboard` or `node --test dashboard/app.test.js`
Expected: FAIL.

- [ ] **Step 3: Implement frontend changes in `app.js` and `style.css`**

1. In `dashboard/static/app.js`:
   - Patch `#deploy-count-badge` evaluation so `activeDeploys.length > 1` triggers before singular phase checks.
   - Cap expanded `.deploy-card` elements to max 2; render 3rd+ or completed `live` cards as `.deploy-card-compact`.
   - Filter Step 2 chips by `c.type === 'ci'` and Step 4 chips by `c.type === 'container'`.
2. In `dashboard/static/style.css`:
   - Fix responsive media query selector at line 2445: change `.steps-grid` to `.deploy-steps-grid`.
   - Add `.stage-swapping` styling: neon violet border (`rgba(176, 38, 255, 0.4)`) and glowing box-shadow aura; decouple from `.stage-pulling` cyan (`#00f0ff`).
   - Add `.deploy-card-compact` styles for collapsed drawer pills.
   - Ensure `.matrix-chip` has $\ge 44\text{px}$ touch hit boundaries (`touch-action: manipulation`, tap-target padding).
   - Add `flex-wrap: wrap` to `.deploy-card-header`.

- [ ] **Step 4: Run frontend tests to verify pass**

Run: `npm test --prefix dashboard` or `node --test dashboard/app.test.js`
Expected: PASS with 100% assertions green.

- [ ] **Step 5: Commit changes**

```bash
git add dashboard/static/app.js dashboard/static/style.css dashboard/app.test.js
git commit -m "feat(dashboard): enhance Permet HUD with multi-pipeline cards, compact drawers, and neon violet styling"
```

---

### Task 4: Full Monorepo Verification & Coverage Audit ($\ge 95.0\%$)

**Files:**
- Touch/Inspect: `dashboard/`

- [ ] **Step 1: Verify statement coverage floor**

Run: `scripts/check-coverage.sh --service dashboard`
Expected: PASS with $\ge 95.0\%$ Go statement coverage and $\ge 95.0\%$ frontend coverage.

- [ ] **Step 2: Run full monorepo verification**

Run: `scripts/verify.sh`
Expected: 100% PASS across all linters, BOM checks, syntax checks, and test suites.

- [ ] **Step 3: Commit and finalize development branch**

```bash
git add .
git commit -m "test(dashboard): verify concurrent deployment pipelines suite and coverage floors"
```
