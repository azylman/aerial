# Real-Time Intermediate Status Updates in Discord Threads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide real-time intermediate progress updates (tool execution, thinking phases) via an in-place edited Discord status message in thread mode, cleanly deleting the status message upon turn completion and delivering the final response as a fresh message with push notifications.

**Architecture:** Extend `runner.activityTap` to parse `step_update` NDJSON events without heap bloat or context smuggling; implement a rate-limited `StatusUpdater` (1.5s ticker) in `pkg/queue` with dynamic generic tool interpolation (`⚡ Running <tool>... (Xs)`), fast-turn debounce, and circuit breakers; and integrate into `queue.processBurst` with guaranteed deferred cleanup and zero-block delivery.

**Tech Stack:** Go 1.24+, DiscordGo (`github.com/bwmarrin/discordgo`), standard sync primitives (`sync.Mutex`, `sync.WaitGroup`, `time.Ticker`).

**Spec:** `docs/specs/2026-09-11-thread-streaming-status-updates.md`

## Global Constraints
- Thread mode only: activated strictly when `isThread == true` and `!skipDiscord`.
- Rate limit ceiling: 1.5s ticker interval enforcing ≤ 3.33 requests per 5s (34% safety headroom below Discord's 5 edits / 5s bucket limit).
- Dynamic tool formatting: `⚡ Running <clean_tool_name>... (Xs)` with zero hardcoded maps and zero parameter/token leakage.
- No context smuggling: `StepUpdateHandler` must be an explicit, strongly typed field in `runner.WatchdogOptions`.
- Clean lifecycle: final response delivered first via `POST`, followed by non-blocking status deletion bounded by a 2-second timeout.
- Clean error delivery: status message is always deleted on error/quota pause, with canonical alerts delivered fresh via `p.cfg.DeliveryFunc`.

---

### Task 1: Runner Stream Processing & Event Parsing

**Files:**
- Modify: `brain/pkg/runner/runner.go`
- Test: `brain/pkg/runner/runner_test.go`

**Interfaces:**
- Produces:
  ```go
  type StepUpdateEvent struct {
      Event          string `json:"event,omitempty"`
      Type           string `json:"type,omitempty"`
      StepType       string `json:"step_type,omitempty"`
      Name           string `json:"name,omitempty"`
      ToolName       string `json:"tool_name,omitempty"`
      State          string `json:"state,omitempty"`
      Status         string `json:"status,omitempty"`
      ConversationID string `json:"conversation_id,omitempty"`
      StepIndex      int    `json:"step_index,omitempty"`
  }

  func (e *StepUpdateEvent) ResolvedType() string
  func (e *StepUpdateEvent) ResolvedToolName() string

  type StepUpdateHandler func(ev *StepUpdateEvent)
  ```
  - `WatchdogOptions.StepUpdateHandler StepUpdateHandler`

- [ ] **Step 1: Write failing tests in `runner_test.go`**
  - Test `StepUpdateEvent.ResolvedType()` and `ResolvedToolName()` handling both `step_type`/`tool_name` and `type`/`name`.
  - Test `activityTap` invoking `StepUpdateHandler` on valid `step_update` lines.
  - Test `activityTap` dropping lines > 32KB without unmarshaling.
  - Test `activityTap` short-circuiting immediately when `StepUpdateHandler` is nil.

- [ ] **Step 2: Run tests to confirm failure**
  - Run `go test -v ./brain/pkg/runner -run TestStepUpdate` and confirm compilation/test failures.

- [ ] **Step 3: Implement `StepUpdateEvent` and `activityTap` enhancements in `runner.go`**
  - Add `StepUpdateEvent`, `ResolvedType()`, `ResolvedToolName()`, and `StepUpdateHandler`.
  - Add `StepUpdateHandler` field to `WatchdogOptions`.
  - Update `newActivityTap` to accept `StepUpdateHandler`.
  - Update `processLine` to guard against >32KB payloads, check `stepHandler != nil`, unmarshal, and invoke handler.

- [ ] **Step 4: Run tests and verify 100% pass**
  - Run `go test -v ./brain/pkg/runner` and verify all tests pass.

- [ ] **Step 5: Commit changes**
  - `git add brain/pkg/runner && git commit -m "feat(runner): add StepUpdateHandler and stream-json event decoding"`

---

### Task 2: Delivery Edit & Delete Message Primitives

**Files:**
- Modify: `brain/pkg/delivery/delivery.go`
- Test: `brain/pkg/delivery/delivery_test.go`

**Interfaces:**
- Produces:
  ```go
  func EditMessage(s *discordgo.Session, channelID, messageID, text string) error
  func DeleteMessage(s *discordgo.Session, channelID, messageID string) error
  func IsMessageNotFoundError(err error) bool
  func IsThreadArchivedOrLockedError(err error) bool
  ```

- [ ] **Step 1: Write failing tests in `delivery_test.go`**
  - Test `EditMessage` and `DeleteMessage` parameter validation (nil session, empty IDs, empty text).
  - Test error classification helpers `IsMessageNotFoundError` (code 10008 / HTTP 404) and `IsThreadArchivedOrLockedError` (code 50083 / 50084).

- [ ] **Step 2: Run tests to confirm failure**
  - Run `go test -v ./brain/pkg/delivery -run TestMessageEditDelete` and verify failures.

- [ ] **Step 3: Implement primitives in `delivery.go`**
  - Implement `EditMessage` with 2,000 character clamping.
  - Implement `DeleteMessage`.
  - Implement Discord error inspection helpers.

- [ ] **Step 4: Run tests and verify pass**
  - Run `go test -v ./brain/pkg/delivery` and ensure full pass.

- [ ] **Step 5: Commit changes**
  - `git add brain/pkg/delivery && git commit -m "feat(delivery): add EditMessage and DeleteMessage primitives with error classifiers"`

---

### Task 3: Thread Status Updater Core & State Machine

**Files:**
- Create: `brain/pkg/queue/status_updater.go`
- Test: `brain/pkg/queue/status_updater_test.go`

**Interfaces:**
- Consumes: `runner.StepUpdateEvent`, `delivery.EditMessage`, `delivery.DeleteMessage`
- Produces:
  ```go
  type StatusUpdater struct { ... }
  func NewStatusUpdater(s *discordgo.Session, threadID string, enabled bool, opts ...StatusUpdaterOption) *StatusUpdater
  func (u *StatusUpdater) HandleStep(ev *runner.StepUpdateEvent)
  func (u *StatusUpdater) Start()
  func (u *StatusUpdater) Stop()
  func (u *StatusUpdater) DeleteStatusMessage()
  func FormatToolStatus(toolName string, elapsed time.Duration) string
  ```

- [ ] **Step 1: Write failing unit tests in `status_updater_test.go`**
  - Test `FormatToolStatus`: verify prefix stripping (`mcp_`), snake_case cleanup, and formatting.
  - Test debounce: verify no message is created if stopped before grace period (1.5s).
  - Test rate limiting: verify edits adhere to the 1.5s ticker interval.
  - Test circuit breaker: verify 404/403/50083 halts the ticker and disables further edits.
  - Test thread safety: concurrent `HandleStep` calls while ticker is flushing.
  - Test clean deletion: `DeleteStatusMessage` cleans up without blocking.

- [ ] **Step 2: Run tests to confirm failure**
  - Run `go test -v ./brain/pkg/queue -run TestStatusUpdater` and confirm failure.

- [ ] **Step 3: Implement `StatusUpdater` in `status_updater.go`**
  - Implement state machine with `sync.Mutex`, `sync.WaitGroup`, 1.5s ticker.
  - Implement atomic dirty reset before Discord REST call.
  - Implement `FormatToolStatus` with length clamping.
  - Implement circuit breaker on Discord API error responses.

- [ ] **Step 4: Run tests and verify pass**
  - Run `go test -v ./brain/pkg/queue -run TestStatusUpdater` and verify 100% pass.

- [ ] **Step 5: Commit changes**
  - `git add brain/pkg/queue/status_updater.go brain/pkg/queue/status_updater_test.go && git commit -m "feat(queue): implement rate-limited StatusUpdater for thread mode"`

---

### Task 4: Queue Worker Pool Integration

**Files:**
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `StatusUpdater`, `runner.WatchdogOptions`

- [ ] **Step 1: Write failing tests in `queue_test.go`**
  - Test thread mode execution spins up `StatusUpdater` and receives `step_update` events.
  - Test channel mode execution (`isThread == false`) bypasses `StatusUpdater`.
  - Test status message deletion occurs upon successful turn completion.
  - Test status message deletion occurs upon runner error / quota pause while canonical error is delivered.

- [ ] **Step 2: Run tests to confirm failure**
  - Run `go test -v ./brain/pkg/queue -run TestThreadStatusQueueIntegration` and confirm failure.

- [ ] **Step 3: Integrate `StatusUpdater` into `processBurst` in `queue.go`**
  - Check `isThread && !skipDiscord && s != nil`.
  - Initialize `updater := NewStatusUpdater(p.getDiscordSession(), threadID, isThread && !skipDiscord)`.
  - Register deferred `updater.Stop()` to guarantee ticker teardown on panic or exit.
  - Wire `updater.HandleStep` to `opts.StepUpdateHandler`.
  - On turn completion or error, invoke `updater.DeleteStatusMessage()`.

- [ ] **Step 4: Run tests and verify pass**
  - Run `go test -v ./brain/pkg/queue` and ensure all tests pass.

- [ ] **Step 5: Commit changes**
  - `git add brain/pkg/queue && git commit -m "feat(queue): wire StatusUpdater into worker pool processBurst"`

---

### Task 5: End-to-End Verification & Monorepo Validation

**Files:**
- Whole monorepo

- [ ] **Step 1: Run comprehensive local test suite**
  - Execute `go test -v -race ./brain/...`
- [ ] **Step 2: Run monorepo verification**
  - Execute `sh scripts/verify.sh --staged`
- [ ] **Step 3: Commit and prepare pull request**
  - Ensure working tree is clean and all tests pass with zero warnings.
