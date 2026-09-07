# Unified Session Rotation & Cold-Swap Memory Seeding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement unified turn-based session rotation (`DefaultMaxSessionTurns = 50`), per-scope worker serialization, defensive session UUID latching, and Turn 1 seed prompt memory bootstrapping across all channel and thread interaction modes in `aerial-brain`.

**Architecture:** Extend turn limit checks and per-scope worker locking to thread interaction modes (`policy.Mode == "thread"`) in `brain/pkg/queue/queue.go`. Enforce defensive regex extraction and 429 cold-start hard failure in `brain/pkg/runner/runner.go`.

**Tech Stack:** Go 1.22+, SQLite (`/data/aerial.db`), `discordgo`, `agy` CLI watchdog runner.

**Spec:** `docs/superpowers/specs/2026-09-07-session-rotation-compaction-design.md`

## Global Constraints
- Engine Invariant: `DefaultMaxSessionTurns = 50` turns per session.
- Scope Serialization: Single active worker per `threadID`/`channelID` scope key.
- Burst Atomicity: 1 message burst = 1 turn execution.
- GitHub Links Only: No `file:///` links in external output.

---

### Task 1: Scope Serialization & Burst Atomicity in Worker Queue

**Files:**
- Modify: `scratch/aerial/brain/pkg/queue/queue.go`
- Test: `scratch/aerial/brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `p.cfg.DB`, `db.GetSessionTurnCount`, `db.RotateSessionID`
- Produces: Per-scope locking map (`sync.Map` of mutexes) ensuring single-threaded execution per thread/channel ID.

- [ ] **Step 1: Write the failing test for per-scope worker serialization**

```go
func TestQueueWorker_PerScopeSerialization(t *testing.T) {
    // Verify that concurrent messages for the same threadID are executed sequentially
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./brain/pkg/queue -run TestQueueWorker_PerScopeSerialization`
Expected: FAIL (or test missing lock assertion)

- [ ] **Step 3: Implement per-scope mutex in `queue.go`**

Add scope lock acquisition and defer release in worker loop around burst processing.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./brain/pkg/queue -run TestQueueWorker_PerScopeSerialization`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/queue.go brain/pkg/queue/queue_test.go
git commit -m "feat(queue): add per-scope worker serialization and burst atomicity"
```

---

### Task 2: Unified Thread Session Rotation Triggers

**Files:**
- Modify: `scratch/aerial/brain/pkg/queue/queue.go`
- Test: `scratch/aerial/brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `policy.Mode`, `DefaultMaxSessionTurns`
- Produces: Unified turn-limit rotation pre/post checks applied to both `channel` and `thread` modes.

- [ ] **Step 1: Write the failing test for thread mode turn rotation**

```go
func TestThreadSession_RotationAt50Turns(t *testing.T) {
    // Seed turn_count to 49 for thread mode, process turn 50, verify RotateSessionID resets session
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./brain/pkg/queue -run TestThreadSession_RotationAt50Turns`
Expected: FAIL (because rotation was previously restricted to policy.Mode == "channel")

- [ ] **Step 3: Update `queue.go` to remove channel-only policy restriction on `DefaultMaxSessionTurns`**

Remove `strings.ToLower(policy.Mode) == "channel"` condition so rotation applies to threads and channels equally.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./brain/pkg/queue -run TestThreadSession_RotationAt50Turns`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/queue.go brain/pkg/queue/queue_test.go
git commit -m "feat(queue): extend 50-turn session rotation to thread interaction mode"
```

---

### Task 3: Defensive Session UUID Latching & Cold-Start 429 Hard Failure

**Files:**
- Modify: `scratch/aerial/brain/pkg/runner/runner.go`
- Modify: `scratch/aerial/brain/pkg/queue/queue.go`
- Test: `scratch/aerial/brain/pkg/runner/runner_test.go`

**Interfaces:**
- Consumes: `ExtractSessionID`, `ClassifyError`
- Produces: Defensive regex validation and non-retryable Turn 1 429 classification.

- [ ] **Step 1: Write the failing test for defensive latching and 429 cold-start hard failure**

```go
func TestRunner_DefensiveLatchingAndCold429(t *testing.T) {
    // Test that empty regex match on cold start fails fast and Turn 1 429 is marked non-transient
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./brain/pkg/runner -run TestRunner_DefensiveLatchingAndCold429`
Expected: FAIL

- [ ] **Step 3: Implement defensive regex check in `runner.go` and cold 429 classification in `queue.go`**

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./brain/pkg/runner -run TestRunner_DefensiveLatchingAndCold429`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/runner/runner.go brain/pkg/queue/queue.go brain/pkg/runner/runner_test.go
git commit -m "feat(runner): implement defensive session latching and cold 429 hard failure"
```

---

### Task 4: Full Suite Integration & Verification

**Files:**
- Test: `scratch/aerial/brain/pkg/queue/queue_test.go`
- Test: `scratch/aerial/brain/pkg/runner/runner_test.go`
- Test: `scratch/aerial/brain/pkg/db/db_test.go`

- [ ] **Step 1: Run full brain test suite**

Run: `cd /root/.gemini/antigravity-cli/scratch/aerial/brain && go test ./...`
Expected: PASS across all packages.

- [ ] **Step 2: Final Commit**

```bash
git commit -m "chore: complete unified session rotation and cold-swap memory seeding verification"
```
