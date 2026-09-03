# Session Lifecycle, Cold/Warm State Management, & Turn 1 Context Bootstrapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permanently resolve Aerial''s "50 First Dates" amnesia loop by eliminating synthetic UUID generation, validating `agy` sessions via disk checks (`SessionExistsOnDisk`), synchronizing sessions unconditionally upon turn success, and bootstrapping cold channel history via Discord API.

**Architecture:** Two-phase channel state machine (Cold vs Warm). On cold channels, ambient messages are marked completed in SQLite without stubbing directories. When woken on Turn 1, recent messages (<= 4 hrs) are fetched from Discord API (or SQLite fallback) and injected as `<CHANNEL_HISTORY>`. `agy` runs with `sessionID = ""` to initialize native protobuf state. The minted session ID is validated on disk and synchronized to SQLite. Warm channels append ongoing ambient turns directly to `transcript.jsonl` and resume multi-turn conversations via `--conversation <id>`.

**Tech Stack:** Go 1.22, SQLite3 (WAL mode), `discordgo`, Antigravity CLI (`agy`).

**Spec:** [`docs/superpowers/specs/2026-09-03-session-lifecycle-and-context-bootstrapping-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-03-session-lifecycle-and-context-bootstrapping-design.md)

## Global Constraints

- Never persist a session UUID in SQLite that was synthesized in Go or unverified by `SessionExistsOnDisk`.
- Never create stub session folders in `/data/brain/` for pure ambient bursts on cold channels.
- Never inject `<CHANNEL_HISTORY>` on Turn 2+ (single-turn injection invariant).
- Wrapped Discord API history retrieval in a strict 2-second timeout context with non-snowflake and error fallback to SQLite.
- Clamp history retrieval to messages <= 4 hours old.
- Sanitize XML delimiters (`</CHANNEL_HISTORY>`, `<CHANNEL_HISTORY>`, `</USER_REQUEST>`, etc.) and cap individual history messages at 1,000 characters.

---

### Task 1: Pre-Flight Disk Validation (`SessionExistsOnDisk`) in `session` package

**Files:**
- Modify: `brain/pkg/session/session.go`
- Test: `brain/pkg/session/session_test.go`

**Interfaces:**
- Produces: `func SessionExistsOnDisk(sessionID string) bool`

- [ ] **Step 1: Write failing unit tests for `SessionExistsOnDisk`**
  In `brain/pkg/session/session_test.go`, add `TestSessionExistsOnDisk`:
  - Empty or whitespace session ID -> returns `false`.
  - Path traversal attempts (`../../etc/passwd`, `foo/bar`, `foo\bar`) -> returns `false`.
  - Non-existent session ID -> returns `false`.
  - Directory with 0-byte `transcript.jsonl` -> returns `false`.
  - Directory with valid non-empty `transcript.jsonl` -> returns `true`.
  - Existing non-empty `.pb` file in `~/.gemini/antigravity-cli/conversations/<id>.pb` -> returns `true`.

- [ ] **Step 2: Run test to verify it fails**
  Run: `go test -v -run TestSessionExistsOnDisk ./pkg/session`
  Expected: FAIL with `undefined: SessionExistsOnDisk`

- [ ] **Step 3: Implement `SessionExistsOnDisk`**
  In `brain/pkg/session/session.go`, implement:
  ```go
  // SessionExistsOnDisk checks if the session has a valid agy conversation
  // protobuf or non-empty transcript on disk.
  func SessionExistsOnDisk(sessionID string) bool {
      trimmed := strings.TrimSpace(sessionID)
      if trimmed == "" || strings.ContainsAny(trimmed, `/\`) || strings.Contains(trimmed, "..") {
          return false
      }
      homeDir, err := os.UserHomeDir()
      if err != nil || homeDir == "" {
          homeDir = "/root"
      }

      // 1. Check agy conversation protobufs
      cliConvPb := filepath.Join(homeDir, ".gemini", "antigravity-cli", "conversations", trimmed+".pb")
      if fi, err := os.Stat(cliConvPb); err == nil && fi.Size() > 0 {
          return true
      }
      agyConvPb := filepath.Join(homeDir, ".gemini", "antigravity", "conversations", trimmed+".pb")
      if fi, err := os.Stat(agyConvPb); err == nil && fi.Size() > 0 {
          return true
      }

      // 2. Check transcript.jsonl non-empty status
      for _, dir := range getTargetDirs(trimmed) {
          tPath := filepath.Join(dir, ".system_generated", "logs", "transcript.jsonl")
          if fi, err := os.Stat(tPath); err == nil && fi.Size() > 0 {
              return true
          }
      }
      return false
  }
  ```

- [ ] **Step 4: Run tests to verify they pass**
  Run: `go test -v -run TestSessionExistsOnDisk ./pkg/session`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/session/session.go brain/pkg/session/session_test.go
  git commit -m "feat(session): add SessionExistsOnDisk pre-flight validation"
  ```

---

### Task 2: Purge Synthetic UUID Minting in Ingestion Layers

**Files:**
- Modify: `brain/funnel.go:350-365`
- Modify: `brain/main.go:70-80`

**Interfaces:**
- Consumes: `db.GetSessionID`
- Invariant: No synthetic `uuid.New().String()` calls before `agy` initializes a session.

- [ ] **Step 1: Inspect and remove synthetic UUID generation in `brain/funnel.go`**
  Remove lines 355-357:
  ```go
  // DELETE:
  if sessID, _ := db.GetSessionID(database, targetThreadID); sessID == "" {
      _ = db.SaveSessionID(database, targetThreadID, uuid.New().String())
  }
  ```

- [ ] **Step 2: Inspect and remove synthetic UUID generation in `brain/main.go`**
  Remove lines 73-75:
  ```go
  // DELETE:
  if sessID, _ := db.GetSessionID(database, threadID); sessID == "" {
      _ = db.SaveSessionID(database, threadID, uuid.New().String())
  }
  ```

- [ ] **Step 3: Run existing unit tests across packages**
  Run: `go test ./...` in `brain` directory.
  Expected: PASS

- [ ] **Step 4: Commit**
  ```bash
  git add brain/funnel.go brain/main.go
  git commit -m "refactor(ingest): purge synthetic UUID pre-allocation from funnel and main"
  ```

---

### Task 3: Turn 1 Discord History Retrieval & Prompt Sanitization

**Files:**
- Create: `brain/pkg/queue/history.go`
- Test: `brain/pkg/queue/history_test.go`

**Interfaces:**
- Produces:
  ```go
  type HistoryMessage struct {
      ID         string
      AuthorName string
      Role       string // "User", "Bot", "Assistant"
      Content    string
      CreatedAt  time.Time
  }
  type HistoryFetcherFunc func(ctx context.Context, channelID string, beforeID string, limit int) ([]HistoryMessage, error)
  func FormatChannelHistory(messages []HistoryMessage) string
  func SanitizeHistoryContent(s string) string
  ```

- [ ] **Step 1: Write failing unit tests for history formatting, sanitization, and clamps**
  In `brain/pkg/queue/history_test.go`:
  - `TestSanitizeHistoryContent`: verifies XML tag replacement (`</CHANNEL_HISTORY>` -> `<\\/CHANNEL_HISTORY>`, `</USER_REQUEST>` -> `<\\/USER_REQUEST>`).
  - `TestFormatChannelHistory_TemporalClamp`: messages older than 4 hours are filtered out.
  - `TestFormatChannelHistory_TruncationCap`: messages exceeding 1,000 characters are capped with `... [truncated]`.
  - `TestFormatChannelHistory_Ordering`: messages formatted chronologically (oldest to newest).
  - `TestDefaultHistoryFetcher_NonSnowflakeFallback`: non-numeric channel IDs bypass Discord REST and query SQLite.

- [ ] **Step 2: Run tests to verify failure**
  Run: `go test -v -run TestSanitizeHistoryContent ./pkg/queue`
  Expected: FAIL with `undefined: SanitizeHistoryContent`

- [ ] **Step 3: Implement `brain/pkg/queue/history.go`**
  Implement:
  - `HistoryMessage` struct.
  - `HistoryFetcherFunc` type.
  - `SanitizeHistoryContent` function.
  - `FormatChannelHistory` function with 4-hour temporal clamp, 1000-char truncation, role tagging, and security notice framing.
  - `DefaultHistoryFetcher` combining Discord REST API (`dg.ChannelMessages`) with 2s timeout and SQLite fallback (`db.GetRecentThreadMessages`).

- [ ] **Step 4: Run tests to verify they pass**
  Run: `go test -v -run "TestSanitizeHistoryContent|TestFormatChannelHistory" ./pkg/queue`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add brain/pkg/queue/history.go brain/pkg/queue/history_test.go
  git commit -m "feat(queue): implement history retrieval, sanitization, and temporal clamping"
  ```

---

### Task 4: Queue State Machine & Unconditional Session Synchronization

**Files:**
- Modify: `brain/pkg/queue/queue.go`

**Interfaces:**
- Consumes: `session.SessionExistsOnDisk`, `HistoryFetcherFunc`, `FormatChannelHistory`
- Modifies: `processBurst` in `WorkerPool`

- [ ] **Step 1: Add `HistoryFetcher` to `WorkerPoolConfig` and wire default**
  In `WorkerPoolConfig`:
  ```go
  HistoryFetcher HistoryFetcherFunc
  ```
  In `NewWorkerPool`: if `cfg.HistoryFetcher == nil`, default to `DefaultHistoryFetcher(getDiscordSession(), cfg.DB)`.

- [ ] **Step 2: Update pure ambient burst handling (`wakeIdx == -1`)**
  Remove `uuid.New().String()` and `SaveSessionID`.
  Only call `AppendAmbientTurn` if `currentSessionID != ""` and `session.SessionExistsOnDisk(currentSessionID)` is true.
  Always mark messages completed in SQLite with `[AMBIENT score=...]`.

- [ ] **Step 3: Update active wake burst pre-flight and Turn 1 context injection**
  In active wake branch (`wakeIdx != -1`):
  1. If `currentSessionID == ""`, get from DB.
  2. If `currentSessionID != ""` and `!session.SessionExistsOnDisk(currentSessionID)`, reset `currentSessionID = ""` (log reset).
  3. If `currentSessionID == ""` (Turn 1):
     Fetch history via `p.cfg.HistoryFetcher(fetchCtx, threadID, burst[0].ID, 10)`.
     If formatted history != "", inject `<CHANNEL_HISTORY>` block above `<USER_REQUEST>`.
  4. If `currentSessionID != ""` (Turn 2+):
     Do NOT inject `<CHANNEL_HISTORY>`.

- [ ] **Step 4: Update session rotation to cleanly reset to Cold State**
  When `policy.MaxSessionTurns > 0 && turnCount >= policy.MaxSessionTurns`:
  ```go
  log.Printf("[Queue] Channel session reached turn limit (%d/%d). Resetting to cold state for fresh session initialization.", turnCount, policy.MaxSessionTurns)
  _ = db.RotateSessionID(p.cfg.DB, threadID, "")
  currentSessionID = ""
  ```

- [ ] **Step 5: Update session synchronization after `RunnerFunc`**
  Replace `if currentSessionID == ""` with:
  ```go
  if !isFailure {
      if extSess := runner.ExtractSessionID(stderr, startTime); extSess != "" && session.SessionExistsOnDisk(extSess) {
          if extSess != currentSessionID {
              log.Printf("[Queue] Active session synchronized for thread %s: %s -> %s", threadID, currentSessionID, extSess)
              currentSessionID = extSess
              _ = db.SaveSessionID(p.cfg.DB, threadID, currentSessionID)
          }
      }
  }
  ```

- [ ] **Step 6: Update trailing messages to check `SessionExistsOnDisk`**
  In `handleTrailing`, use updated `currentSessionID` and only call `AppendAmbientTurn` if `session.SessionExistsOnDisk(currentSessionID)` is true.

- [ ] **Step 7: Run compilation and package tests**
  Run: `go test ./pkg/queue`
  Expected: PASS

- [ ] **Step 8: Commit**
  ```bash
  git add brain/pkg/queue/queue.go
  git commit -m "feat(queue): implement two-phase cold/warm session state machine and unconditional sync"
  ```

---

### Task 5: End-to-End Regression Test Suite

**Files:**
- Modify: `brain/pkg/queue/queue_test.go`

- [ ] **Step 1: Write `TestProcessBurst_GhostSessionRecovery`**
  - Seed SQLite with `db.SaveSessionID(database, "chan-lounge", "stale-ghost-uuid")`.
  - Create mock session dir on disk for `"real-new-uuid"`.
  - Mock `RunnerFunc` returning stderr:
    `warning: conversation "stale-ghost-uuid" not found\nStarting conversation update stream for real-new-uuid`
  - Process burst.
  - Assert `db.GetSessionID(database, "chan-lounge")` equals `"real-new-uuid"`.

- [ ] **Step 2: Write `TestProcessBurst_MultiTurnContinuity_AfterRecovery`**
  - Following Step 1, enqueue a follow-up message on `"chan-lounge"`.
  - Process burst.
  - Assert `RunnerFunc` was invoked with `sessionID == "real-new-uuid"`.

- [ ] **Step 3: Write `TestProcessBurst_ColdChannel_NoStubDirectory`**
  - Burst of ambient messages on `"chan-cold-123"`.
  - Process burst.
  - Assert `db.GetSessionID` is empty `""`.
  - Assert no directory exists for `"chan-cold-123"` in `/data/brain` or home brain roots.

- [ ] **Step 4: Write `TestProcessBurst_Turn1ContextInjection`**
  - Mock `HistoryFetcher` returning 2 messages.
  - Process Turn 1 wake message on cold channel.
  - Assert prompt contains `<CHANNEL_HISTORY>` with both messages.
  - Process Turn 2 wake message on warm channel.
  - Assert prompt does NOT contain `<CHANNEL_HISTORY>`.

- [ ] **Step 5: Write `TestProcessBurst_SessionRotation_ResetsToColdState`**
  - Configure `MaxSessionTurns: 2`.
  - Process 2 turns.
  - Verify turn 2 rotates `session_id` to `""`.
  - Process turn 3; verify it executes as Turn 1 cold bootstrap.

- [ ] **Step 6: Write `TestProcessBurst_Turn1Crash_DoesNotPersistGhostUUID`**
  - Mock `RunnerFunc` returning exit code 1 (failure) with `Starting conversation update stream for crash-uuid`.
  - Process burst.
  - Assert SQLite does NOT contain `"crash-uuid"`.

- [ ] **Step 7: Run all unit tests across all packages**
  Run: `go test -v -race ./...` in `brain`.
  Expected: ALL PASS with 0 race warnings.

- [ ] **Step 8: Commit**
  ```bash
  git add brain/pkg/queue/queue_test.go
  git commit -m "test(queue): add comprehensive regression suite for session lifecycle and amnesia prevention"
  ```