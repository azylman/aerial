# Optimistic Daemon Rotation at Mark Time Specification

## Problem Statement

When skills or rules are updated on disk, Aerial's background inotify `fileWatcher` detects the change and calls `WorkerPool.MarkDirty()`, which cascades to all three process pools (`processPool`, `lowEffortProcessPool`, and `voiceProcessPool`).

Currently:
- `UnifiedProcessPool.MarkDirty()` sets `d.dirty = true` on all active daemons.
- However, `GetOrCreate()` only checks `d.State() != StateClosed` and ignores `d.IsDirty()`.
- Furthermore, `ShouldRotate()` only checks turn and step limits, never checking `d.IsDirty()`.
- Consequently, warm daemons (such as the pre-warmed `kiosk` in `voicePool`, pre-warmed `ephemeral:classifier` and `ephemeral:summarizer` in `lowEffortPool`, and active Discord thread daemons) never recycle on skill edits. Because `agy daemon` parses and indexes skills only at process startup, warm processes remain running with stale in-memory skills indefinitely until an unrelated session threshold (10 turns, 180 steps, quota pause, or process restart) triggers.
- If rotation is deferred until a subsequent user turn is requested in `GetOrCreate()`, the user's next turn incurs an unnecessary ~1.5–2.5 second cold-start spin-up latency on the critical response path (especially detrimental to voice TTFR).

## Goal & Architecture

Optimistically rotate daemons at `MarkDirty()` notification time and on turn completion:
- **At `MarkDirty()` Time (Optimistic Background Rotation)**:
  - For all idle daemons (`InflightCount() == 0`): evict from `p.daemons`, close the old daemon child process (logging any close error; strictly zero swallowed errors), and asynchronously pre-warm any target configured in `p.cfg.PrewarmedTargets` (`kiosk`, `ephemeral:classifier`, `ephemeral:summarizer`).
  - Pre-warmed daemons immediately spin up with fresh skills in the background, achieving **0ms added latency** on the user's next voice or classification turn.
  - Idle thread daemons (not pre-warmed) are evicted and closed immediately to reclaim host memory and prevent stale processes from lingering.
  - Prevent thrashing from rapid successive file modifications by tracking in-flight pre-warming targets in `p.prewarming` map under mutex lock.
- **For Active In-Flight Daemons (`InflightCount() > 0`)**:
  - In-flight turns are never killed or interrupted mid-sentence. The daemon is marked `d.dirty = true`.
  - When the in-flight turn finishes (`hasMoreInflight == false` in `StreamingDaemon` event loop):
    - If `d.IsDirty()` is true, trigger background rotation using an independent context derived from `p.ctx` (`context.WithTimeout(p.ctx, 15*time.Second)`), logging any rotation error.
- **Pool Lifecycle & Concurrency Tracking**:
  - Track all background closing and pre-warming goroutines using `p.bgWg` (`sync.WaitGroup`).
  - When `p.Close()` is called, cancel `p.cancel()`, which automatically aborts in-flight pre-warming contexts, and await `p.bgWg.Wait()`.
- **Safety Guards**:
  - `UnifiedProcessPool.GetOrCreate()` verifies `!d.IsDirty()`. If a dirty daemon is ever encountered with `InflightCount() == 0`, it rotates it immediately.
  - `UnifiedProcessPool.ShouldRotate()` returns true if `d.IsDirty() && d.InflightCount() == 0`.

## Global Constraints

- Pure Go standard library dependencies (`sync`, `time`, `os`, `context`, etc.).
- Strictly ZERO markdown tables across all artifacts, messages, commits, code comments, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. Every error path must be logged with structured context or propagated.
- Strictly ZERO host memory inspection (`/proc/meminfo` reading, string parsing, or memory pressure eviction).
- Maintain statement coverage floor of strictly `>= 95.0%` across `brain/pkg/runner`.
- All staged changes must pass fast pre-commit verification (`powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`).
- Always address Alex directly.
