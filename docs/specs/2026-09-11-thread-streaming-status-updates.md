# Design Specification: Real-Time Intermediate Status Updates in Discord Threads

## 1. Overview & Objectives
This specification defines the architecture for delivering real-time intermediate execution status in Discord thread mode (`isThread == true`). During agent turns that execute tools or enter reasoning phases, Aerial maintains an in-place status reply in the thread that dynamically reflects current activity. 

Upon turn completion, the final response is delivered as a fresh new message (`POST`) to guarantee push notifications, unread badges, and multi-chunk/attachment delivery. The intermediate status message is then cleanly deleted (`DELETE`), leaving zero tombstone or placeholder artifacts in the thread history.

Standard channel mode (`isThread == false`) remains completely untouched, continuing to use the ambient typing indicator.

---

## 2. Architecture & Component Design

### 2.1 Subprocess Stream Processing (`brain/pkg/runner/runner.go`)
- **Stream Event Sniffing**: `activityTap` intercepts `agy --output-format stream-json` stdout across chunk boundaries.
- **Strongly-Typed Plumbing (No Context Smuggling)**:
  `WatchdogOptions` explicitly accepts an optional `StepUpdateHandler`:
  ```go
  type StepUpdateEvent struct {
      Event          string `json:"event,omitempty"`
      Type           string `json:"type,omitempty"`
      StepType       string `json:"step_type,omitempty"`
      Name           string `json:"name,omitempty"`
      ToolName       string `json:"tool_name,omitempty"`
      State          string `json:"state,omitempty"` // "ACTIVE", "DONE", "ERROR"
      Status         string `json:"status,omitempty"`
      ConversationID string `json:"conversation_id,omitempty"`
      StepIndex      int    `json:"step_index,omitempty"`
  }

  func (e *StepUpdateEvent) ResolvedType() string
  func (e *StepUpdateEvent) ResolvedToolName() string

  type StepUpdateHandler func(ev *StepUpdateEvent)
  ```
- **Zero-Allocation Hot Path**:
  - `ToolInfo` and `TextDelta` are deliberately excluded from `StepUpdateEvent` to prevent heap allocation of large tool payloads and streaming tokens.
  - If `stepHandler == nil`, `processLine` immediately drops `step_update` lines without unmarshaling.
  - Payloads exceeding 32KB are skipped to prevent CPU/memory bloat.
  - `step_update` lines continue to be filtered out of `outBuf` to maintain session OOM protection.

### 2.2 Thread Status Manager (`brain/pkg/queue/status_updater.go`)
- **Thread Scoping**: Activated strictly when `isThread && !skipDiscord && s != nil`.
- **In-Memory Coalescence**:
  `StatusUpdater` maintains thread state under a lightweight `sync.Mutex` (<20ns hold time):
  ```go
  type StatusUpdater struct {
      mu              sync.Mutex
      s               *discordgo.Session
      threadID        string
      statusMessageID string
      activeTool      string
      phase           string // "thinking", "tool", "responding"
      toolStart       time.Time
      turnStart       time.Time
      dirty           bool
      disabled        bool
      ticker          *time.Ticker
      done            chan struct{}
      wg              sync.WaitGroup
  }
  ```
- **Dynamic Generic Tool Formatting (Zero Hardcoded Maps)**:
  Tool status is generated dynamically without hardcoded dictionaries:
  - Clean tool name: strip internal `mcp_` prefix and sanitize.
  - Status string: `⚡ Running <clean_tool_name>... (Xs)`
  - Examples:
    • `⚡ Running view_file... (1.2s)`
    • `⚡ Running docker_list_containers... (2.4s)`
    • `⚡ Running github_create_pull_request... (3.1s)`
    • `⚡ Running ha_call_write_tool... (1.5s)`
    • `⚡ Running brave_search... (0.8s)`
    • `💭 Thinking... (2.5s)`
    • `✍️ Generating response... (1.1s)`
  - Hard cap at 100 characters max. Zero raw arguments, paths, environment variables, or tokens.

### 2.3 Rate Limit Guarantees & Ticker Loop
- **Discord Constraints**:
  - `PATCH /channels/{thread_id}/messages/{message_id}` is limited to 5 requests per 5 seconds per channel/thread.
  - Threads have unique snowflake IDs, isolating rate-limit buckets completely across concurrent threads.
- **1.5s Ticker Interval**:
  - Ticker fires every 1.5 seconds (maximum 3.33 requests per 5 seconds), maintaining a 34% safety headroom below Discord's 5/5s limit.
- **Atomic Dirty Reset**:
  - The snapshot string is copied and `dirty = false` is reset *before* invoking `s.ChannelMessageEdit`, preventing dropped updates from events arriving during HTTP round-trips.

### 2.4 Fast-Turn Debounce & Anti-Flicker
- Do not create a status message at `t=0`.
- The status message is created on the first ticker tick (at 1.5s) only if `dirty == true` and the turn is still executing.
- Fast turns (<1.5s) never create a status message, eliminating UI flicker on simple conversational turns.

### 2.5 Terminal Delivery & Pristine Cleanup
- **Sequence**:
  1. `updater.Stop()`: Stops ticker and blocks via `wg.Wait()` until any in-flight edit returns.
  2. Final response delivered via `delivery.SendMessageWithAttachments` as a fresh new message (`POST`).
  3. `updater.DeleteStatusMessage()`: Initiates `s.ChannelMessageDelete(threadID, statusMessageID)` with a detached context bounded by a 2-second timeout.
- **Zero-Block Delivery**: Final response delivery never waits for status message deletion.
- **Clean Error Delivery**: On watchdog timeout, panic, or quota pause, the status message is cleanly deleted and the canonical error alert is delivered fresh via `p.cfg.DeliveryFunc`.

### 2.6 Resilience & Circuit Breaker Matrix
- **HTTP 404 (`10008: Unknown Message`)**: Status message was deleted by user/moderator. Clear `statusMessageID = ""` and disable updater.
- **HTTP 400 (`50083: Thread is archived`)**: Thread auto-archived. Disable updater immediately to avoid 1.5s spam.
- **HTTP 403 (`50001 / 50084: Missing Permissions / Thread Locked`)**: Disable updater immediately.
- **Empty Output (Silent Sentinel)**: Delete status message so no phantom message remains.

---

## 3. Implementation Checklist
1. `brain/pkg/runner/runner.go`:
   - Add `StepUpdateEvent` and `StepUpdateHandler`.
   - Update `WatchdogOptions` to include `StepUpdateHandler`.
   - Update `activityTap` with zero-allocation JSON parsing and 32KB payload guard.
2. `brain/pkg/delivery/delivery.go`:
   - Add `EditMessage` and `DeleteMessage` helper functions with error classification.
3. `brain/pkg/queue/status_updater.go`:
   - Implement `StatusUpdater` with 1.5s ticker, atomic dirty flag, and circuit breakers.
4. `brain/pkg/queue/queue.go`:
   - Wire `StatusUpdater` in `processBurst` gated by `isThread`.
   - Register deferred `updater.Stop()` and terminal delete handover.
5. Unit Tests:
   - Comprehensive test suite in `brain/pkg/queue/status_updater_test.go` and `brain/pkg/runner/runner_test.go`.
