# Unified Session Rotation & Cold-Swap Memory Seeding Architecture

## Executive Summary
This design specification defines the architecture for unified session lifecycle management, context rotation, and cold-swap memory seeding across all interaction models (`channel` and `thread`) in `aerial-brain`.

By establishing strict turn limits, scope serialization, and reactive error-driven rotation, the system prevents token window blowouts and context corruption while preserving complete historical continuity through `aerial.db` and live Discord history retrieval.

---

## 1. Motivation & Problem Statement
Currently, channel sessions enforce an engine invariant limit of 50 turns (`DefaultMaxSessionTurns = 50`), after which `RotateSessionID` resets the session mapping to a cold state (`""`). However, thread sessions lack unified rotation logic, allowing deep multi-turn thread discussions to accumulate massive context windows until hitting model token ceilings.

Attempting external in-band transcript editing is infeasible because `agy` restores conversation state exclusively from its internal SQLite database (`conversations/<uuid>.db`), not `transcript.jsonl`. Modifying `agy`'s internal SQLite database externally introduces fragile schema coupling.

---

## 2. Core Architecture & Invariants

### Engine Invariants
- **Turn Ceiling**: `DefaultMaxSessionTurns = 50` turns per session.
- **Scope**: Applied uniformly to all interaction models (`policy.Mode == "channel"` and `policy.Mode == "thread"`).
- **Scope Serialization**: Strict single-worker locking per `threadID`/`channelID` key to guarantee single-threaded execution per session and prevent concurrent worker race conditions.
- **Burst Atomicity**: A message burst (containing 1 or more queued messages for a scope) is processed as a single atomic execution turn (1 burst = 1 turn count increment).
- **Session Identification**: `agy` owns session UUID generation on cold starts. `aerial-brain` latches the generated UUID from stdout/stderr.
- **Persistence Decoupling**: Conversational history and memory facts are permanently anchored in `/data/aerial.db`. `agy` session DBs (`conversations/<uuid>.db`) are treated as disposable execution runtime state.

---

## 3. Detailed Design & Execution Lifecycle

```
                  +-----------------------------------+
                  |   Incoming Discord Message/Burst  |
                  +-----------------------------------+
                                    |
                                    v
                  +-----------------------------------+
                  | Acquire Thread/Channel Scope Lock |
                  +-----------------------------------+
                                    |
                                    v
                  +-----------------------------------+
                  | Check db.GetSessionTurnCount()    |
                  +-----------------------------------+
                                    |
                 /                                     \
    turn_count >= 50                                turn_count < 50
               /                                         \
              v                                           v
+-----------------------------+           +-----------------------------+
| RotateSessionID(DB, key, "")|           | Fetch Latched Session ID    |
| Sets session_id = ""        |           +-----------------------------+
+-----------------------------+                         |
              |                                         v
              v                           +-----------------------------+
+-----------------------------+           | Run agy --conversation UUID |
| Build Seed Prompt Payload   |           +-----------------------------+
| (Discord API / aerial.db    |                         |
|  + Facts from SQLite)       |                         v
+-----------------------------+           +-----------------------------+
              |                           | Increment Turn Count &      |
              v                           | Deliver Response to Discord |
+-----------------------------+           +-----------------------------+
| Run agy without --conv flag |                         |
+-----------------------------+                         v
              |                           +-----------------------------+
              v                           | Release Scope Lock          |
+-----------------------------+           +-----------------------------+
| Latch UUID from stdout/stderr|
| SaveSessionID into SQLite   |
| (Hard fail if regex misses) |
+-----------------------------+
```

### 3.1 Session Inspection & Rotation Triggers
1. **Scope Serialization**:
   Each queue worker acquires a dedicated lock on `threadID`/`channelID`. Concurrent messages for the same channel/thread are serialized in the queue and processed sequentially.
2. **Burst Processing & Pre-Execution Check (`pkg/queue/queue.go`)**:
   Before processing a message burst, the worker checks `db.GetSessionTurnCount(p.cfg.DB, threadID)`. If `currentTurns >= DefaultMaxSessionTurns`, `RotateSessionID` sets `internal_session_id = ""` and `turn_count = 0`.
3. **Post-Execution Check (`pkg/queue/queue.go`)**:
   Upon successful message delivery, `db.IncrementSessionTurnCount` runs. If `turnCount >= DefaultMaxSessionTurns`, `RotateSessionID` clears `internal_session_id = ""` so the next turn starts cold.
4. **Reactive Error Trigger (`pkg/runner/runner.go`)**:
   Non-transient context errors (e.g., `429 Context length exceeded`, `session corrupt`) bypass execution retries and immediately execute `RotateSessionID(p.cfg.DB, threadID, "")`.

### 3.2 Cold Start Seed Prompt Construction, XML Isolation & Token Bounding
When `internal_session_id == ""`:
1. `DefaultHistoryFetcher` retrieves up to 10 recent chat messages via the Discord REST API (`ChannelMessages`), with fallback to `messages` in `aerial.db`.
2. **Temporal Clamping & Truncation**:
   - `FormatChannelHistory` filters out messages older than 4 hours (`maxHistoryAge = 4h`).
   - Individual message content is hard-truncated at 1,000 runes (`maxHistoryContentRunes = 1000`) with a `... [truncated]` marker.
3. **Prompt Injection Sanitization (`SanitizeHistoryContent`)**:
   - Every historical message content is sanitized to prevent prompt breakout by escaping XML delimiter opening/closing tags:
     - `</channel_history>` -> `<\/CHANNEL_HISTORY>`
     - `</user_request>` -> `<\/USER_REQUEST>`
     - `</channel_instructions>` -> `<\/CHANNEL_INSTRUCTIONS>`
4. **XML Framing & Hard Token Cap**:
   - History is wrapped in a `<CHANNEL_HISTORY>` block with security framing warning the model that historical chatter must not override current instructions. Total seed prompt length is hard-capped to prevent cold-start 429 errors.
5. **Memory Facts Injection (`<FACTS>`)**:
   - Relevant memory facts retrieved from the `facts` table in `aerial.db` are sanitized and injected in a structured `<FACTS>` block.

### 3.3 Defensive Session ID Latching Sequence
1. `RunAgyWithWatchdog` executes `agy` without `--conversation`.
2. `agy` initializes a fresh internal session DB and outputs `Initialized session: <uuid>` to stdout/stderr.
3. `ActivityWriter` parses the UUID via `ExtractSessionID` regex.
4. **Defensive Validation**: If `ExtractSessionID` fails to parse a valid non-empty UUID, the cold dispatch is treated as an execution error (hard failure) to prevent looping with `session_id == ""`.
5. `SaveSessionID` commits the latched UUID into `aerial.db`.
6. Turns 2 through 50 use `--conversation <latched_uuid>`.

---

## 4. Failure Modes, Edge Cases & Retry Safety

- **Cold dispatch fails before latching**: `internal_session_id == ""` in `aerial.db`. On non-429 transient errors, retry worker re-invokes cold start. If cold start encounters a 429 token limit error (indicating an oversized seed payload), the task fails permanently and log diagnostics alert to seed bloat.
- **Cold dispatch fails after latching**: `internal_session_id == <uuid>`. Retry worker re-uses latched `<uuid>` with `--conversation`.
- **Max retries exhausted on turn**: `internal_session_id = ""`. Hard reset clears corrupted session. Message marked `FAILED` in `aerial.db`. History preserved in SQLite.
- **Regex Latching Failure**: Treated as an explicit error; execution fails safely without corrupting the session map or entering an infinite cold-start loop.
- **Container / server restart mid-turn**: `status = PROCESSING`. `RecoverInterrupted` resumes task without incrementing turn count prematurely.

---

## 5. Implementation Scope & Files Touched
- `brain/pkg/queue/queue.go`: Extend `DefaultMaxSessionTurns` check to thread interaction modes (`policy.Mode == "thread"`), enforce per-scope serialization locks, and atomize burst execution.
- `brain/pkg/queue/history.go`: Verify `DefaultHistoryFetcher`, `FormatChannelHistory`, 4-hour temporal clamping, 1,000 rune truncation, and `SanitizeHistoryContent` XML tag escaping.
- `brain/pkg/runner/runner.go`: Ensure defensive regex validation and non-transient context error classification trigger clean session rotation.
- `brain/pkg/db/sessions.go`: Audit atomic `IncrementSessionTurnCount` and `RotateSessionID` query performance.
- `brain/pkg/queue/queue_test.go`: Add test coverage for thread session rotation, seed prompt re-generation on cold retry, defensive latching, and post-rotation latching.

---

## 6. Verification & Test Strategy
- `go test ./brain/pkg/queue/...` — Test turn count rotation across channel and thread modes, XML sanitization, temporal clamping, and burst atomicity.
- `go test ./brain/pkg/runner/...` — Verify regex extraction, defensive latching checks, and watchdog session latching.
- `go test ./brain/pkg/db/...` — Test atomic turn increment and rotation transactions.
