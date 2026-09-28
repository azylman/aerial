# Layered Streaming Process Pool Design Specification

## 1. Executive Summary & Goals

### 1.1 Context & Motivation
Aerial previously operated under a synchronous request-response turn model:
- `ExecuteTurnWithHandler` or `RunnerWithOptionsFunc` executed turns by blocking on `d.stdout.ReadString` until a terminal `event: result` was received.
- Worker threads ran an outer 3-attempt retry loop in [`brain/pkg/queue/worker.go`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/queue/worker.go) that inspected synthesized JSON outputs, reverse-scanned `transcript.jsonl`, and classified exit codes.
- Throwaway tasks (such as ambient message classification in [`brain/pkg/classifier`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/classifier/classifier.go) and thread title summarization in [`brain/pkg/queue/summarizer.go`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/queue/summarizer.go)) spawned fresh, single-turn `agy` processes on demand, incurring multi-second OS process boot penalties.
- Error recovery conflated intermediate internal retried steps with fatal turn failures, leading to premature retry exhaustion.

### 1.2 Architectural Goals
This specification establishes a **Layered Streaming Architecture** powered by a unified, transport-agnostic process pool:
- **Decoupled Asynchronous Streaming**: Ingress writes directly to process `stdin` without blocking. Output streams continuously from `stdout` and is routed asynchronously to destination-specific sinks.
- **Unified Process Pool**: A single pool infrastructure in [`brain/pkg/runner`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner) serves Discord conversational threads, persistent long-running Voice sessions (keyed by device ID, e.g. `"kiosk"`), and throwaway tasks (classifiers and thread titles), with automated session rotation thresholds managed internally by the pool.
- **Boot-Time Pre-Warming**: Configurable pre-warmed targets (`PrewarmedTargets`, e.g. kiosk voice daemon and classifiers) are spawned immediately on Brain boot, eliminating cold-start latency after container deployments.
- **Deterministic TurnSink Routing**: Each turn attaches a `TurnSink` (`DiscordTurnSink`, `VoiceTurnSink`, `ThrowawayTurnSink`), ensuring that intermediate tool updates edit a specific Discord status message or stream to a specific open WebSocket connection.
- **Empirically Proven FIFO Pipelining**: Live container spike testing confirmed that `agy` CLI (`stream-json`) queues sequential turns internally. The Go runtime maintains an in-flight FIFO queue (`inflight []*TurnContext`) mapping incoming events to their originating caller.
- **Fair Quota Protection**: When Google Gemini quota exhaustion occurs (429 `RESOURCE_EXHAUSTED`), the pool pauses message dispatch for existing pending messages to be resumed when the block expires, while fast-failing new incoming messages arriving during the lockout.

---

## 2. System Architecture & Component Hierarchy

```
                    ┌──────────────────────────────────────────────────────────┐
                    │                 Client Ingress Layer                     │
                    │   - Discord Gateway Ingress (policy & ambient check)     │
                    │   - Voice WebSocket Audio/Text Ingress                   │
                    │   - Internal Classifiers & Summarizers                   │
                    └─────────────────────────────┬────────────────────────────┘
                                                  │
                                                  ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                              Turn Coordinator & Ingress Mailbox                              │
│                                                                                              │
│  - Inspects Global Quota Lockout:                                                            │
│      * If locked: fast-rejects NEW incoming messages with quota pause notification           │
│      * If unlocked: enqueues turn into thread mailbox / dispatcher                           │
│  - Constructs TurnContext with dedicated TurnSink:                                           │
│      * DiscordTurnSink (holds threadID, messageID, StatusUpdater)                            │
│      * VoiceTurnSink (holds specific open WebSocket connection)                              │
│      * ThrowawayTurnSink (holds result channel chan string)                                  │
│  - Calls daemon.Send(prompt, turnCtx) and returns immediately                                │
└──────────────────────────────────────────────┬───────────────────────────────────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                               Layer 2: UnifiedProcessPool                                    │
│                                                                                              │
│  - Allocates or retrieves warm StreamingDaemon for targetID / sessionID                      │
│  - Internally tracks session health and enforces rotation thresholds:                        │
│      * Turn limit: 8-10 turns                                                                │
│      * Step limit: 180 tool steps                                                            │
│      * Transcript size: 500 KB                                                               │
│      * Idle TTL: 24h with zero active tasks                                                  │
│  - Rotates daemon cleanly to fresh session upon turn completion when threshold crossed       │
└──────────────────────────────────────────────┬───────────────────────────────────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                             Layer 1: StreamingDaemon Supervisor                              │
│                                                                                              │
│  - Wraps OS process: agy --input-format stream-json --output-format stream-json              │
│  - In-Flight FIFO Queue: inflight []*TurnContext                                             │
│  - Stdin Writer: writes {"event":"user","message":...}\n under stdinMu mutex                 │
│  - Stdout Reader: unmarshals NDJSON line-by-line and dispatches to inflight.Peek().Sink:     │
│      * step_update (tool_name, cmd)  ──► Sink.OnToolCall()                                   │
│      * step_update (text_delta)      ──► Sink.OnTextDelta()                                  │
│      * step_update (thinking)        ──► Sink.OnThinking()                                   │
│      * result                        ──► inflight.Pop() ──► Sink.OnResult()                  │
│      * error / EOF                   ──► inflight.Pop() ──► Sink.OnError()                   │
│  - Process Tree Teardown: kills -pgid to prevent orphan descendant tools                      │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer 1: Transport-Agnostic `StreamingDaemon`

### 3.1 Responsibilities
The `StreamingDaemon` in [`brain/pkg/runner/daemon.go`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner/daemon.go) is a pure, stream-oriented process supervisor. It has zero knowledge of Discord, PostgreSQL, or WebSockets.

### 3.2 State Machine
The daemon enforces strict state transitions:
- `StateStarting`: Process spawned, awaiting synchronous `init` event handshake.
- `StateReady`: Handshake complete, active session UUID latched, ready for input.
- `StateExecuting`: At least one turn is actively executing or buffered in `agy`.
- `StateYieldWaiting`: Turn emitted `result` but background tasks (`TaskTracker.ActiveCount() > 0`) remain running.
- `StateClosed`: Process terminated, pipes closed, resources freed.

### 3.3 Core Struct Definition
```go
type StreamingDaemon struct {
    cfg         DaemonConfig
    cmd         *exec.Cmd
    stdin       io.WriteCloser
    stdout      io.ReadCloser
    stderr      *ActivityWriter
    taskTracker *TaskTracker

    mu          sync.RWMutex
    stdinMu     sync.Mutex
    state       DaemonState
    sessionID   string
    dirty       bool
    lastUsed    time.Time
    turnCount   int
    stepCount   int

    inflightMu  sync.Mutex
    inflight    []*TurnContext

    events      chan DaemonEvent
    readerWg    sync.WaitGroup
    closeOnce   sync.Once
    closed      atomic.Bool
}
```

### 3.4 Process Spawning & Synchronous Handshake
To prevent empty session ID bugs on cold start:
- `StartDaemon` launches `agy` with `--dangerously-skip-permissions --input-format stream-json --output-format stream-json`.
- Before marking state `StateReady`, it performs a synchronous handshake read on `stdout` with a 5-second deadline.
- Parses the initial `{"event":"init","session_id":"..."}` line via `runner.ParseInitEvent`.
- Latches `d.sessionID` and updates `d.stderr.SetSessionID(id)`.
- Verifies UUID validity before returning the daemon instance.

### 3.5 Atomic Stdin Writer
To avoid frame corruption from concurrent calls:
```go
func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error {
    d.stdinMu.Lock()
    defer d.stdinMu.Unlock()

    d.mu.RLock()
    if d.state == StateClosed {
        d.mu.RUnlock()
        return errors.New("cannot send to closed daemon")
    }
    d.mu.RUnlock()

    wireMsg := streamInputPayload{
        Event: "user",
        Message: streamInputMessage{
            Content: prompt,
        },
    }
    encoded, err := json.Marshal(wireMsg)
    if err != nil {
        return fmt.Errorf("failed to marshal turn prompt: %w", err)
    }
    encoded = append(encoded, '\n')

    d.inflightMu.Lock()
    d.inflight = append(d.inflight, turnCtx)
    d.inflightMu.Unlock()

    d.mu.Lock()
    d.state = StateExecuting
    d.lastUsed = time.Now()
    d.mu.Unlock()

    if _, err := d.stdin.Write(encoded); err != nil {
        d.markDirty()
        return fmt.Errorf("failed writing prompt to daemon stdin: %w", err)
    }
    return nil
}
```

### 3.6 Stdout Reader & Event Dispatch Loop
A single dedicated background goroutine drains `stdout` line by line:
- Decodes NDJSON events.
- Fetches active turn:
  ```go
  d.inflightMu.Lock()
  var activeTurn *TurnContext
  if len(d.inflight) > 0 {
      activeTurn = d.inflight[0]
  }
  d.inflightMu.Unlock()
  ```
- Dispatches events to `activeTurn.Sink`:
  - `step_update` with tool details -> `activeTurn.Sink.OnToolCall(toolName, cmdName)`
  - `step_update` with text delta -> `activeTurn.Sink.OnTextDelta(delta)`
  - `step_update` with thinking -> `activeTurn.Sink.OnThinking()`
  - `result` -> Pops head of `inflight`:
    ```go
    d.inflightMu.Lock()
    if len(d.inflight) > 0 {
        activeTurn = d.inflight[0]
        d.inflight = d.inflight[1:]
    }
    d.inflightMu.Unlock()
    activeTurn.Sink.OnResult(turnResult)
    ```
- Channel buffer sizing: If using a broad `Events()` channel, buffer size is set to `1024` to absorb fast token bursts. The reader goroutine never performs blocking network or database calls inside the loop.

---

## 4. Layer 2: `UnifiedProcessPool` & Session Rotation

### 4.1 Pool Structure
The `UnifiedProcessPool` in [`brain/pkg/runner`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner) maintains:
- **Pinned Conversational Daemons**: Mapped by `targetKey`:
  - **Discord**: `threadID` (or `channelID` in channel mode).
  - **Voice**: `deviceID` (e.g. `"kiosk"` or hardware client ID). Voice processes are long-running and persistent—they are **never** torn down on connection close, ensuring zero-latency conversational continuity.
- **Throwaway Executions**: Handled via warm shared role keys (e.g. `"ephemeral:classifier"` and `"ephemeral:summarizer"`).

### 4.2 Boot-Time Pre-Warming (`PrewarmedTargets`)
To prevent cold-start latency after container deployments (so the first kiosk voice interaction or first ambient message is immediately responsive):
- **Configuration**:
  - `PrewarmedTargets []string`: List of target keys to pre-spawn at startup (e.g. `["kiosk", "ephemeral:classifier", "ephemeral:summarizer"]`).
- **Startup Protocol**:
  - When `UnifiedProcessPool.Initialize(ctx)` runs on Brain boot, it launches background goroutines to spawn and perform the initial `init` handshake for all targets in `PrewarmedTargets`.
  - By the time the Discord gateway connects and voice sockets open, these daemons are already in `StateReady` state.

### 4.3 Pool-Managed Rotation Thresholds
The pool inspects daemon health pre-turn and post-turn across all daemons (Discord, Voice, and throwaways):
- **Turn Count**: Rotates after **8 to 10 conversational turns** (`DefaultMaxSessionTurns`).
- **Tool Step Count**: Rotates after **180 cumulative tool steps** (`DefaultMaxSessionSteps`).
- **Transcript Byte Ceiling**: Rotates when `.system_generated/logs/transcript.jsonl` exceeds **500 KB** (`DefaultMaxTranscriptBytes`).
- **Session DB Byte Ceiling**: Rotates when `<session-id>.pb` or SQLite DB exceeds **1.5 MB** (`DefaultMaxSessionDBBytes`).
- **Idle TTL**: Closes after **24 hours** of zero activity and zero running background tasks (`len(activeTasks) == 0`). Note: pre-warmed daemons (like `"kiosk"`) are automatically re-spawned if evicted by idle TTL.

### 4.4 Clean Rotation Flow
1. Active turn finishes completely.
2. Pool checks rotation criteria (`ShouldRotateDaemon`).
3. If rotation required:
   - Compacts conversation by generating a thread summary.
   - Saves summary to database (`sessions.summary`).
   - Closes old daemon gracefully (killing process group `-pgid`).
   - Clears session ID mapping.
4. Next turn initializes a fresh daemon with a new session UUID, injecting the compacted summary and lookback history. If the target was pre-warmed, a replacement daemon can be spawned immediately in the background.

---

## 5. Layer 3: Transport Sinks & Turn Context

### 5.1 The `TurnSink` Interface
```go
type TurnSink interface {
    OnTurnStarted()
    OnThinking()
    OnToolCall(toolName, commandName string)
    OnTextDelta(delta string)
    OnResult(res *TurnResult)
    OnError(err error)
}
```

### 5.2 Destination-Specific Implementations

#### 5.2.1 `DiscordTurnSink`
- **Fields**:
  - `s *discordgo.Session`
  - `threadID string`
  - `messageID string`
  - `statusUpdater *StatusUpdater`
  - `stopTyping func()`
  - `store db.Store`
  - `execStart time.Time`
- **Behavior**:
  - `OnTurnStarted`: Invokes `stopTyping = StartTyping(s, threadID)` and `statusUpdater.MarkTurnStarted()`.
  - `OnToolCall`: Calls `statusUpdater.HandleStep(&stepEv)`, editing the specific status badge in the thread.
  - `OnTextDelta`: Accumulates response in memory buffer.
  - `OnResult`:
    - Halts typing (`stopTyping()`).
    - Halts and deletes intermediate badge (`statusUpdater.Stop()`, `statusUpdater.DeleteStatusMessage()`).
    - Executes 4-pass media extraction ([`delivery.ExtractAndSanitizeMedia`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/delivery/delivery.go) and `AutoAttachNewMedia`).
    - Delivers full response with attachments via `DeliveryWithAttachmentsFunc`.
    - Updates Postgres message to `StatusCompleted`.
  - `OnError`: Halts typing, deletes status badge in `defer`, classifies error, schedules retry or marks `StatusFailed`.

#### 5.2.2 `VoiceTurnSink`
- **Fields**:
  - `conn *websocket.Conn` (or voice audio pipe)
  - `deviceID string` (e.g. `"kiosk"`)
- **Behavior**:
  - `OnToolCall`: Sends lightweight JSON control packet over WebSocket for UI animation / chime.
  - `OnTextDelta`: Streams raw token chunks directly into voice pipeline / TTS synthesizer with zero buffering.
  - `OnResult`: Sends end-of-turn audio delimiter frame.
  - `OnError`: Sends error frame over WebSocket.
  - **Connection Close / Persistence**: When the client disconnects or an utterance finishes, the connection closes and `VoiceTurnSink` is detached, but the underlying `StreamingDaemon` is **never** torn down. It remains warm in `p.daemons[deviceID]`, ready for the next interaction with zero process boot latency.

#### 5.2.3 `ThrowawayTurnSink`
- **Fields**:
  - `resCh chan string`
  - `errCh chan error`
- **Behavior**:
  - `OnToolCall`: Ignored.
  - `OnTextDelta`: Ignored.
  - `OnResult`: Pushes `res.Response` to `resCh`.
  - `OnError`: Pushes error to `errCh`.

---

## 6. Quota Coordination & Fair Lockout Policy

### 6.1 Quota Lockout Trigger
When `agy` emits a 429 `RESOURCE_EXHAUSTED` (or returns a capacity phrase with countdown):
- Extracts reset duration using [`ExtractQuotaResetDuration`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner/runner.go#L996).
- If countdown is missing, applies **70s fallback** for model capacity blips (Google TPM/RPM rollover) or **20m** for subscription limits.
- Sets global atomic lockout timestamp:
  `pool.quotaLockedUntil.Store(time.Now().UTC().Add(resetDur).Unix())`

### 6.2 The Dual-Action Policy (Reject New, Preserve Existing)
- **Action for CURRENTLY PENDING Messages**:
  - Any message already in Postgres (`status = pending`) or currently in flight when the lockout starts is **preserved**.
  - Messages in flight are rolled back to `status = pending` with retry reason `[QUOTA_PAUSED reset_in=...]`.
  - The worker queue suspends dispatch loop execution until `time.Now().Unix() >= quotaLockedUntil.Load()`.
  - When the block expires, the queue resumes popping existing pending messages across all threads. Zero sibling messages are dropped or marked failed.
- **Action for NEW Incoming Messages**:
  - Any new message arriving via Discord gateway or Voice WebSocket while `time.Now().Unix() < quotaLockedUntil.Load()` is **fast-failed / rejected immediately**.
  - **Discord**: Ingress returns an immediate quota notification (`notifier.FormatQuotaPauseMessage`) and marks the newly arrived message `StatusFailed` with `[QUOTA_LOCKED]`.
  - **Voice**: Ingress immediately sends an unavailable voice frame and closes the turn without queueing.
  - **Rationale**: Prevents users who speak to the bot during an active 4-hour pause from hanging silently for hours with no feedback.

---

## 7. Process Lifecycle, Inactivity Watchdog, & Graceful Shutdown

### 7.1 Inactivity Watchdog
- `BuildDaemonArgs` passes `--print-timeout` directly to `agy` (default 60m or configured turn timeout).
- [`ActivityWriter`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner/runner.go#L68) tracks write activity timestamps (`lastActivity atomic.Int64`).
- If `agy` outputs `print timeout` in stderr or emits no output for the inactivity duration, [`IsInactivityTimeout`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/brain/pkg/runner/runner.go#L626) detects the condition, cancels the turn context, and restarts the daemon.

### 7.2 Process Tree Teardown (`-pgid`)
- On POSIX, processes are spawned with `Setpgid: true`.
- Teardown always signals the negative process group ID (`syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)`).
- Windows uses job objects to ensure all descendant processes (`sh`, `git`, `python`) terminate.
- Bounded wait of 2 seconds before escalating to `SIGKILL`.

### 7.3 Graceful Deployment (SIGTERM)
1. **Ingress Freeze**: Container receives `SIGTERM`. Closes gateway ingress.
2. **Drain Window (5s)**: Active streaming turns finish naturally if possible.
3. **CAS Rollback**: Turns still streaming after 5 seconds are atomically rolled back in PostgreSQL from `'running'` to `'pending'`:
   ```sql
   UPDATE messages
   SET status = 'pending', error_message = 'interrupted by graceful deployment'
   WHERE id = $1 AND status = 'running';
   ```
4. **Teardown**: Processes killed cleanly.
5. Replacement container resumes all pending messages immediately upon startup via atomic CAS.

---

## 8. Verification & Statement Coverage Strategy

### 8.1 95.0% Statement Coverage Floor
To ensure `brain/pkg/runner` (baseline 95.3%) and `brain/pkg/queue` (baseline 95.7%) stay >= 95.0%:
- All streaming daemon tests will use pure in-memory pipes (`io.Pipe()`, `bytes.Buffer`) rather than spawning external shell processes in CI.
- Mock streams will simulate:
  - Immediate `init` handshake.
  - Rapid multi-turn `step_update` and `result` frames.
  - Mid-turn EOF and broken pipe errors.
  - 429 quota pause frames.
- TurnSink implementations will be tested with mock Discord and WebSocket sessions, verifying clean status message deletion and zero swallowed errors.
- Pre-submit verification script `./scripts/verify.sh` will enforce test execution and statement coverage gating.
