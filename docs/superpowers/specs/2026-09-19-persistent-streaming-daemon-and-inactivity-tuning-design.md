# Persistent Streaming Daemon Worker Pool & 24-Hour Inactivity Lifecycle Architecture

## Executive Summary
This design specification defines the architecture for running Antigravity CLI (`agy`) as a persistent streaming daemon pool with deterministic in-memory task tracking, unified 24-hour inactivity pruning, bounded safety ceilings, and thread-serialized execution in `aerial-brain`.

By replacing per-turn process spawning (`agy -p`) with persistent streaming daemons (`agy --input-format stream-json --output-format stream-json`), Aerial eliminates the OS-level background task termination trap (the "Option B yield trap"), slashes per-turn startup latency from ~2s to milliseconds, deterministically tracks background command lifecycles in Go memory, and guarantees uninterrupted execution of long-running tasks.

---

## 1. Motivation & Problem Statement

### 1.1 The Option B Yield Trap & Process Termination
Under the current headless execution model:
1. `aerial-brain` spawns a new `agy -p` process for each conversation turn.
2. When an agent invokes a long-running tool (e.g., `run_command` taking >10,000ms), `agy` detaches the process as a background task (`task-N`) and injects a runtime advisory urging the model to take "Action A (proceed with other work)" or "Action B (simply update the user with a waiting message and end the turn)".
3. If the model emits waiting prose and ends the turn without active tool calls, `agy` treats the turn as complete, enters its print-mode drain, terminates all background tasks (`terminating background task(s) on exit`), and exits.
4. The background task is permanently aborted, while intermediate waiting chatter is delivered to Discord.

### 1.2 Brittle Transcript Regex Auditing
PR #317 introduced a Layer 2 yield-trap detector using regex parsing on `transcript.jsonl` (`sessionMgr.HasUnfinishedBackgroundTask()`). While this caught stderr signatures, transcript auditing has proven non-deterministic and susceptible to **self-poisoning**:
- When code or command output containing the phrase `"Tool is running as a background task with task id:"` (such as `grep` outputs, test assertions, or error logs) is logged to the transcript, the regex detector latches phantom task IDs that can never be marked completed.
- This results in false-positive yield-trap auto-resumes and circuit breaker trips.

### 1.3 Per-Turn Process Overhead & Memory Telemetry
- Spawning a fresh `agy` process per turn incurs 1–3 seconds of process bootstrap and protobuf initialization.
- Telemetry analysis reveals that `aerial-brain` runs on a host with 32 GB of RAM and 27.5 GB of currently available memory.
- Analysis of 4,638 completed turns across 171 unique threads shows that median turn gaps are 1m 10s, with 98.4% of gaps < 4 hours, and 99.8% of gaps < 24 hours.
- Peak concurrent active threads in a 24-hour window was 33 threads. At ~200 MB RSS per `agy` process, 33 concurrent daemons consume ~6.6 GB (less than 25% of available host memory).

---

## 2. Core Architectural Pillars & Invariants

### 2.1 High Concurrency with Bounded Safety Ceilings & LRU Eviction
- Daemons are instantiated on-demand per active thread.
- **Safety Ceiling**: Concurrency is bounded by default to prevent runaway resource exhaustion:
  ```go
  const DefaultMaxConcurrentDaemons = 40
  ```
  (Configurable via `max_concurrent_daemons` in `config.yaml`; setting `0` permits unbounded operation).
- **LRU Inactive Eviction Sweep**: If available host RAM drops below 15% or the daemon count exceeds the ceiling, completely idle daemons (`len(activeTasks) == 0 && inFlight == false`) are pruned in least-recently-used order, even if their 24-hour idle timer has not expired.
- **Thread Scope Serialization**: Concurrency safety within each thread is guaranteed by thread-level serialization (`scopeLocks`).

### 2.2 Unified Inactivity Lifecycle (`DefaultMaxSessionIdleTime = 24h`)
- Both the **logical session rotation** (`DefaultMaxSessionIdleTime`) and the **daemon process idle pruning** (`DefaultDaemonIdleTimeout`) are unified to **24 hours**:
  ```go
  const DefaultDaemonIdleTimeout = DefaultMaxSessionIdleTime // 24 * time.Hour
  ```
- **Symmetric Retirement**: When a thread has been inactive for 24 hours:
  1. The persistent OS daemon process is gracefully stopped to free RAM.
  2. The database session mapping expires, ensuring the next turn begins as a fresh cold start with thread history lookback.
- **Worker Ownership**: The 24-hour idle timer is owned and driven directly by the thread worker goroutine, keeping worker goroutine and daemon lifetimes perfectly synchronized.

### 2.3 Hard Task-Aware Pruning Invariant & Zombie Task Ceiling
- A daemon's idle timer **ONLY** ticks down when:
  ```go
  len(activeTasks) == 0 && len(incomingQueue) == 0
  ```
- If a daemon has even one running background task, it is **strictly immune** to idle pruning.
- **Zombie Task Ceiling**: To prevent hung commands (`sleep 999999` or deadlocked sockets) from permanently pinning daemons in memory, background tasks enforce a hard execution ceiling:
  ```go
  const MaxBackgroundTaskDuration = 2 * time.Hour
  ```
  If a background task exceeds 2 hours, Aerial forcibly terminates the task's process group, dereferences the task ID, posts an error notice, and resumes the 24-hour idle pruning countdown.

### 2.4 Deterministic Task Tracking via Idempotent Task Set
- Transcript regex parsing is completely eliminated.
- Background task lifecycles are tracked deterministically in Go memory using an idempotent concurrent set:
  ```go
  type TaskTracker struct {
      mu    sync.RWMutex
      tasks map[string]TaskMetadata // taskID -> metadata (start time, tool name, cmd)
  }
  ```
- Task initiation is detected by sniffing tool execution responses in `step_update` events matching `reBackgroundTaskStarted`.
- Task completion is detected by watching `.system_generated/tasks/task-<N>.log` and filesystem event notifications.
- **Persistence Mirroring**: Active task IDs are mirrored to PostgreSQL session metadata so that container restarts can reconcile running tasks upon cold boot.

### 2.5 Serialized Turn Mailbox & Accurate Continuation Prompts
- **Zero Pipe Contention**: To avoid deadlock with `scopeLock` or race conditions on the daemon's `stdin` pipe during `YIELD_WAITING`, task completion events are enqueued as high-priority synthetic messages into the thread worker's channel (`incomingQueue`).
- When a background task finishes, the worker drains the queue and serializes the continuation turn to `stdin`.
- **Accurate Task Completion Prompt**: Replaces the misleading "runtime terminated them" warning with:
  ```go
  const TaskCompletionResumePrompt = "[SYSTEM NOTICE]: Background task %s has finished with exit code %d. Logs are available on disk. Continue your workflow to completion. Do NOT end the turn with waiting text."
  ```
- Intermediate waiting prose emitted before task completion is suppressed from Discord delivery.

---

## 3. Detailed Component Architecture & Data Flow

```
                                    +-----------------------------------------+
                                    |         Discord Event Funnel            |
                                    +-----------------------------------------+
                                                         |
                                                         v
                                    +-----------------------------------------+
                                    |     Queue Worker Pool (burst.go)        |
                                    |     Serializes turns per ThreadID       |
                                    +-----------------------------------------+
                                                         |
                                                         v
                                    +-----------------------------------------+
                                    |        Thread Worker Goroutine          |
                                    | - Owns 24h Idle Timer                   |
                                    | - Serialized Turn Mailbox (channel)     |
                                    +-----------------------------------------+
                                                         |
                                    +--------------------+--------------------+
                                    |                                         |
                            Daemon Active                             Daemon Expired / None
                                    |                                         |
                                    v                                         v
                      +---------------------------+             +---------------------------+
                      | Write prompt turn NDJSON  |             | Spawn: agy --conversation |
                      | to daemon stdin pipe      |             | <uuid> --input-format     |
                      +---------------------------+             | stream-json --output-fmt  |
                                    |                           | stream-json               |
                                    |                           +---------------------------+
                                    +--------------------+--------------------+
                                                         |
                                                         v
                                    +-----------------------------------------+
                                    |           Daemon stdout Stream          |
                                    +-----------------------------------------+
                                                         |
                                    +--------------------+--------------------+
                                    |                                         |
                                    v                                         v
                      +---------------------------+             +---------------------------+
                      |        activityTap        |             |      Discord Delivery     |
                      | - Parses NDJSON frames    |             | - Buffers substantive text|
                      | - Sniffs step_update tools|             | - Suppresses intermediate |
                      | - Updates TaskTracker set |             |   waiting prose           |
                      +---------------------------+             +---------------------------+
                                    |
                                    v
                            Turn Event: result
                                    |
                    +---------------+---------------+
                    |                               |
          len(activeTasks) == 0           len(activeTasks) > 0
                    |                               |
                    v                               v
        +-----------------------+       +-------------------------------+
        | Deliver substantive   |       | Yield Trap Intercepted:       |
        | output to Discord     |       | - Keep daemon alive           |
        | Reset 24h idle timer  |       | - Update Discord status: ⏳   |
        +-----------------------+       | - Monitor task-N.log on disk  |
                                        | - On done: enqueue synthetic  |
                                        |   TaskCompletionResumePrompt  |
                                        +-------------------------------+
```

### 3.1 Daemon Lifecycle States
1. **STARTING**: Spawning `agy` with streaming flags (`--input-format stream-json --output-format stream-json --conversation <session_id>`) with `SysProcAttr: &syscall.SysProcAttr{Setpgid: true}`.
2. **READY**: Idle, listening on `stdin`, 24h idle timer active.
3. **EXECUTING**: Processing a turn. `activityTap` parses NDJSON steps in real-time.
4. **YIELD_WAITING**: `result` received but `len(activeTasks) > 0`. Monitoring task completion; intermediate prose suppressed; Discord status updated to `⏳ Running background task...`.
5. **PRUNING**: 24h idle timeout elapsed without messages (`len(activeTasks) == 0`). Clean shutdown via EOF on `stdin` followed by process group termination (`killProcessGroup`) with SIGTERM and SIGKILL after 3s.
6. **RECYCLING**: Session reached turn limit (10 turns), step limit (350 steps), or byte limit (1 MB). Daemon is stopped cleanly and respawned on the new session ID.

### 3.2 Protocol Interface (`stream-json`)
- **Input Stream (`stdin`)**:
  Empirically verified NDJSON format:
  ```json
  {"event": "user", "message": {"content": "<prompt>"}}
  ```
- **Output Stream (`stdout`)**:
  Streaming NDJSON events:
  - `init`: Delimits daemon startup and active tools.
  - `step_update`: Emitted for tool calls, user messages, and model responses:
    ```json
    {"event": "step_update", "step_update": {"step_index": 2, "state": "DONE", "step_type": "tool", "tool_name": "run_command", "tool_info": {"parameters": {"CommandLine": "sleep 20"}}}}
    ```
  - `result`: Delimits the end of the turn with token usage.
- **Turn Delimitation & Buffer Isolation**:
  The streaming reader frames stdout turn-by-turn. Upon encountering `{"event": "result"}`, the turn buffer is parsed, substantive text extracted, and the internal buffer reset to prevent memory inflation across turns.

### 3.3 Child Process Group Management
`agy` executes background tasks as sub-processes. To prevent zombie child process leaks:
- Daemons are launched with dedicated process groups:
  ```go
  cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
  ```
- On daemon shutdown or task timeout:
  ```go
  syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
  time.Sleep(3 * time.Second)
  syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
  ```

### 3.4 Prompt Layering Optimization
- **Cold Start (Turn 1)**: Injects full 7-layer prompt (persona, skills, channel instructions, previous session summary, lookback history).
- **Warm Turns (Turns 2..N)**: In persistent daemon mode, `agy`'s resident protobuf state retains full conversation context. Warm turns inject only current user content, avoiding redundant lookback history and self-reflection duplication.

---

## 4. Configuration, Hot-Reload & Edge Cases

### 4.1 Configuration Invariants (`config.yaml`)
```yaml
# Persistent Daemon & Session Management
daemon_idle_timeout: 24h          # Process idle prune TTL (defaults to DefaultMaxSessionIdleTime)
session_idle_timeout: 24h         # Session context rotation TTL
max_concurrent_daemons: 40         # Bounded safety ceiling (0 = unlimited)
max_background_task_duration: 2h  # Zombie task ceiling
```

### 4.2 Graceful Hot-Reloading
When configuration, rules, or skills change:
1. All idle daemons are immediately stopped and recycled.
2. Active daemons are marked `dirty`. The moment their current turn completes, they are gracefully recycled before accepting the next message.

### 4.3 Process Crash & Recovery
1. If an `agy` daemon process unexpectedly exits (`SIGSEGV`, OOM, or kill):
   - The worker catches EOF/pipe error on stdout.
   - The turn is classified via `runner.ClassifyError`.
   - If recoverable, a fresh daemon is spawned immediately with `--conversation <active_session_id>` to rehydrate session memory from `conversations/<uuid>.pb`.
   - If the `.pb` file is corrupted, the session is rotated and restarted cleanly.

---

## 5. Metrics & Observability

The persistent daemon subsystem exposes the following Prometheus metrics:
- `aerial_brain_daemons_active`: Gauge tracking current count of resident `agy` daemon processes.
- `aerial_brain_daemon_spawns_total{reason="cold_start|rotated|recycled|recovered"}`: Counter tracking daemon initializations.
- `aerial_brain_daemon_prunes_total{reason="idle_timeout|session_limit|hot_reload|lru_memory"}`: Counter tracking daemon teardowns.
- `aerial_brain_active_tasks{thread_id="..."}`: Gauge tracking running background tasks across active daemons.
- `aerial_brain_daemon_memory_bytes`: Gauge tracking aggregate RSS memory consumed by `agy` processes.
- `aerial_brain_task_timeouts_total`: Counter tracking tasks terminated by the 2-hour zombie ceiling.

---

## 6. Verification & Test Plan

1. **Unit Tests (`pkg/runner/daemon_test.go`)**:
   - Verify daemon startup with `stream-json` wire protocol and clean EOF shutdown.
   - Verify `TaskTracker` idempotent addition, removal, and timeout pruning.
   - Verify turn delimitation and buffer resetting on `event: "result"`.
2. **Integration Tests (`pkg/queue/daemon_pool_test.go`)**:
   - Verify concurrent message bursts spawn daemons up to the configured ceiling and queue excess requests.
   - Verify that daemons with active tasks are immune to 24-hour idle pruning.
   - Verify that LRU eviction prunes idle daemons when simulated memory pressure exceeds threshold.
   - Verify that session rotation (10 turns) triggers daemon recycling without dropping messages.
3. **Yield Trap & Zombie Ceiling Regression Tests**:
   - Run a command with `WaitMsBeforeAsync = 100`. Verify `agy` yields, brain intercepts the yield, keeps the daemon alive, waits for task completion, and enqueues `TaskCompletionResumePrompt`.
   - Run a simulated hanging command. Verify `MaxBackgroundTaskDuration` fires, terminates the process group, dereferences the task, and resumes idle timeout countdown.

---

## 7. Girl Gang Review & Sign-Off Record

The Girl Gang review panel conducted a deep-dive 4-discipline audit on 2026-09-19:
- **Systems & Concurrency Architect**: Identified unconstrained memory risk and stdin write races during `YIELD_WAITING`. Resolved via bounded concurrency ceiling (`max_concurrent_daemons: 40`), memory-pressure LRU pruning, and serialized turn mailbox.
- **Harness & Discord Lifecycle Engineer**: Identified inaccurate "runtime terminated them" continuation prompt and stream buffer inflation. Resolved via `TaskCompletionResumePrompt` and per-turn buffer isolation.
- **Antigravity CLI Specialist**: Verified empirical NDJSON wire protocol (`{"event":"user","message":{...}}`), proved non-existence of fictional `task_start`/`task_end` events in `agy`, and specified task sniffing via `step_update` and `.system_generated/tasks/` logs.
- **Adversarial Devil's Advocate**: Identified permanent daemon lock by hung tasks and memory loss on restart. Resolved via `MaxBackgroundTaskDuration = 2h` and PostgreSQL task persistence mirroring.

**Consensus Status**: **ALL REQUESTED CHANGES INCORPORATED — SPEC READY FOR IMPLEMENTATION PLANNING**.
