# Message Handling Pipeline Redesign Specification

## 1. Overview & Goals
This specification defines the complete redesign of the Aerial message handling pipeline. The goal is to replace the brittle disk-scraping and uncoordinated subprocess execution with a resilient, transactional, queue-driven state machine.

### Key Objectives:
- **Zero Missed Messages**: Inbound Discord and HTTP messages are persisted immediately upon receipt before any execution begins.
- **Strict In-Order Thread Execution**: Messages targeting the same Discord thread are serialized in FIFO order.
- **Model A Direct Delivery**: The Go wrapper captures standard gy stdout and sends replies directly to Discord via discordgo with safety chunking for Discord's 2000-character limit.
- **Context-Preserving Retries**: Transient API failures (503 UNAVAILABLE, rate limits, timeouts) preserve the Antigravity session UUID across progressive backoff retries.
- **Dynamic Notification Generation**: Error and session reset notifications sent to Discord are dynamically generated via a lightweight, out-of-band gy call using the latest SYSTEM.md/AGENTS.md persona rules.
- **Deterministic Crash Recovery**: On startup, the service queries all incomplete (PENDING, PROCESSING) messages and seamlessly resumes processing.

---

## 2. Data Model (SQLite)

Located at /data/aerial.db:

`sql
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,               -- Discord Message ID (or UUID for HTTP prompts)
    thread_id TEXT NOT NULL,           -- Discord Thread ID or Channel ID
    guild_id TEXT NOT NULL,            -- Discord Guild ID
    author_id TEXT NOT NULL,           -- Author Discord ID
    author_name TEXT NOT NULL,         -- Author Username/Global Name
    content TEXT NOT NULL,             -- Message content text
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING | PROCESSING | COMPLETED | FAILED
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,                -- Last error detail (if any)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_status ON messages(thread_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);

CREATE TABLE IF NOT EXISTS sessions (
    thread_id TEXT PRIMARY KEY,        -- Discord Thread ID
    internal_session_id TEXT NOT NULL, -- Antigravity CLI session UUID
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

---

## 3. Architecture & Components

`
????????????????????     ????????????????????????????????????????????????????????????
?  Discord Gateway ? ??? ? Ingest Handler (funnel.go)                               ?
?  or HTTP /prompt ?     ? - Stores message as 'PENDING' in SQLite                  ?
????????????????????     ? - Enqueues to Thread Worker Pool                         ?
                         ????????????????????????????????????????????????????????????
                                                   ?
                                                   ?
                         ????????????????????????????????????????????????????????????
                         ? Thread Worker Pool (queue.go / worker.go)               ?
                         ? - Serialized FIFO channel per thread_id                  ?
                         ? - Shows typing indicator every 8s                        ?
                         ? - Transitions status to 'PROCESSING'                     ?
                         ????????????????????????????????????????????????????????????
                                                   ?
                                                   ?
                         ????????????????????????????????????????????????????????????
                         ? Execution Engine (runner.go)                             ?
                         ? - Spawns agy --conversation <id> -p <prompt>             ?
                         ? - Direct stdout/stderr stream capture                    ?
                         ? - Session UUID parsing                                   ?
                         ????????????????????????????????????????????????????????????
                                        ?                           ?
                                    (Success)                   (Failure)
                                        ?                           ?
                                        ?                           ?
                         ????????????????????????????? ??????????????????????????????
                         ? Delivery (delivery.go)    ? ? Retry / Recovery           ?
                         ? - Sends reply to Discord  ? ? - Transient (503): Retry   ?
                         ? - 2000-char safety chunk  ? ? - Session Corrupt: Evict & ?
                         ? - Marks 'COMPLETED'       ? ?   notify with dynamic agy  ?
                         ????????????????????????????? ? - Exhausted: Notify user   ?
                                                       ??????????????????????????????
`

### Component Breakdown:
1. **db (rain/pkg/db)**: SQLite migration and CRUD for messages and sessions.
2. **queue (rain/pkg/queue)**: Manages per-thread worker channels and in-memory locks.
3. **unner (rain/pkg/runner)**: Executes gy commands, manages timeouts, streams stdout/stderr, extracts session IDs, and evaluates failure conditions.
4. **delivery (rain/pkg/delivery)**: Sends messages to Discord, handles chunking for >2000 chars, and manages typing indicators.
5. **
otifier (rain/pkg/notifier)**: Uses a lightweight one-shot gy invocation to synthesize natural, persona-aligned error/reset notifications.

---

## 4. Execution, Retry & Recovery Workflow

### Normal Turn Execution:
1. Ingest saves PENDING record in SQLite.
2. Thread worker acquires lock and updates status to PROCESSING.
3. Worker launches background typing ticker.
4. Runner invokes gy with current internal_session_id.
5. Upon clean stdout with exit code 0:
   - Save session UUID to sessions table.
   - Send response text to Discord thread via delivery.
   - Update message status to COMPLETED.

### Transient Error Handling (503 / 429 / Timeout):
- Keep existing internal_session_id.
- Backoff sleep: ttempt * 3 seconds (up to 3 attempts).
- Retry prompt execution against same session.

### Session Corruption Handling:
- If gy fails due to unrecoverable session corruption or repeated non-transient crash on resume:
  - Generate a friendly notification via 
otifier (lightweight out-of-band gy prompt).
  - Post notification to Discord thread.
  - Delete 	hread_id from sessions table.
  - Re-run user prompt with fresh session.

### Total Exhaustion Handling:
- If all 3 attempts fail:
  - Generate a friendly error apology via 
otifier.
  - Post error message to Discord thread.
  - Mark message status FAILED with error details.

### Startup Recovery (RecoverInterrupted):
- On boot, query all messages where status IN ('PENDING', 'PROCESSING').
- Re-queue them for immediate thread worker processing in chronological order.

---

## 5. Verification & Testing Plan

1. **Unit Tests**:
   - db_test.go: Database migrations, message state transitions, session lookups, and queue filtering.
   - queue_test.go: Multi-thread concurrent processing and in-thread serialized FIFO ordering.
   - unner_test.go: Error classification (transient vs permanent), session extraction, and timeout handling.
   - delivery_test.go: Discord message chunking algorithm (>2000 characters).
2. **Integration Verification**:
   - Simulated crash recovery with pending/processing records.
   - Transient failure simulation and retry backoff.