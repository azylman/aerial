# Channel Interaction Model & Multi-User Security Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement configurable channel-based ambient interaction (`mode: "channel"` vs `mode: "threads"`), autonomous LLM evaluation with `[NO_REPLY]` sentinel suppression, message burst coalescing, 5-minute staleness TTL, turn-count session rotation, and multi-user `admin_users` security guardrails in Aerial.

**Architecture:** Extend `config.yaml` with required `channels.default` policy maps and `admin_users` allowlists. In `funnel.go`, route channel-mode messages directly to `ChannelID` sessions without creating Discord threads, augment `<USER_REQUEST>` prompts with `is_admin` flags, reply attribution, and channel evaluation directives. In `queue.go`, implement message burst coalescing, 5m staleness TTL, turn-count session rotation, intent-driven typing indicators, and `[NO_REPLY]` output suppression. Update `SYSTEM.md` with immutable admin security boundaries.

**Tech Stack:** Go 1.22, DiscordGo, SQLite (`modernc.org/sqlite`), Docker.

**Spec:** `docs/superpowers/specs/2026-09-01-channel-interaction-and-multi-user-guardrails-design.md`

## Global Constraints
- Zero stuck running invariant preserved.
- Zero plaintext tokens in logs or SQLite records.
- Backward compatibility: `channels.default` is required; `mode: "threads"` behaves identically to existing system.
- Strict concurrency safety: All Go tests must pass under `-race` detector.

---

### Task 1: `ChannelPolicy` Configuration Schema, `admin_users`, and Validation in `brain/pkg/config`

**Files:**
- Modify: `brain/pkg/config/config.go`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Produces:
  - `type ChannelPolicy struct { Mode string; TypingIndicator string; IgnoreBots bool; AllowSystemOps bool; MaxSessionTurns int }`
  - `type Config struct { ... AdminUsers []string; Channels map[string]ChannelPolicy ... }`
  - `func (c *Config) ResolveChannelPolicy(channelID, channelName string) ChannelPolicy`

- [ ] **Step 1: Write failing unit tests in `brain/pkg/config/config_test.go`**
  - Test parsing `admin_users` and `channels` policy map.
  - Test validation failure when `channels.default` is missing or has invalid `mode`.
  - Test `ResolveChannelPolicy` matching by Snowflake Channel ID $\to$ Channel Name (case-insensitive `#` stripped) $\to$ `default`.
  - Test default inheritance: channels with partial overrides inherit unspecified fields from `channels.default`.
  - Test `typing_indicator` normalization (`"always"`, `"on_mention"`, `"never"`).

- [ ] **Step 2: Run test to verify failure**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v ./pkg/config
  ```

- [ ] **Step 3: Implement `ChannelPolicy`, `rawConfigHelper` decoding, `ResolveChannelPolicy`, and validation in `brain/pkg/config/config.go`**
  - Add `ChannelPolicy` struct.
  - Update `Config` and internal `rawConfigHelper` to unmarshal `AdminUsers` and `Channels`.
  - In `LoadConfig()`, validate `channels.default` existence and valid `mode` (`"threads"` or `"channel"`).
  - Implement `ResolveChannelPolicy(channelID, channelName string) ChannelPolicy` with fallback and inheritance.

- [ ] **Step 4: Run tests to verify pass**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/config
  ```

- [ ] **Step 5: Commit changes**
  ```bash
  git add brain/pkg/config
  git commit -m "feat(config): add ChannelPolicy schema, admin_users, and policy resolution"
  ```

---

### Task 2: Robust Sentinel Parser & Empty Stdout Support in `brain/pkg/runner`

**Files:**
- Modify: `brain/pkg/runner/runner.go`
- Test: `brain/pkg/runner/runner_test.go`

**Interfaces:**
- Produces:
  - `func IsSilentSentinel(stdout string) bool`
  - `func ClassifyError(exitCode int, stdout, stderr string) (isFailure, isTransient, isSessionCorruption bool, errDetail string)` (updated for exit code 0 + empty stdout / silent sentinel clean success)

- [ ] **Step 1: Write failing unit tests in `brain/pkg/runner/runner_test.go`**
  - Test `IsSilentSentinel` with `"[NO_REPLY]"`, `"[no_reply]"`, `"[NO_REPLY]."`, `"`[NO_REPLY]`"`, `"**[NO_REPLY]**"`, `"  [NO_REPLY]  "`, and empty string `""` returning `true`.
  - Test `IsSilentSentinel` with visible text returning `false`.
  - Test `ClassifyError` with `exitCode == 0` and empty `stdout` returning `isFailure = false`.

- [ ] **Step 2: Run test to verify failure**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v ./pkg/runner
  ```

- [ ] **Step 3: Implement `IsSilentSentinel` and update `ClassifyError` in `brain/pkg/runner/runner.go`**
  - Add `IsSilentSentinel(stdout string) bool` with markdown/punctuation stripping.
  - Update `ClassifyError` to treat exit code 0 with empty stdout as non-failure unless fatal error keywords are in stderr.

- [ ] **Step 4: Run tests to verify pass**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/runner
  ```

- [ ] **Step 5: Commit changes**
  ```bash
  git add brain/pkg/runner
  git commit -m "feat(runner): add IsSilentSentinel parser and allow clean empty stdout classification"
  ```

---

### Task 3: SQLite Migration, Turn-Count Session Rotation, 5m TTL & Coalescing in `brain/pkg/queue` and `brain/pkg/db`

**Files:**
- Modify: `brain/pkg/db/db.go`
- Modify: `brain/pkg/queue/queue.go`
- Test: `brain/pkg/db/db_test.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Produces:
  - `func IncrementSessionTurnCount(database *sql.DB, threadID string) (int, error)`
  - `func RotateSessionID(database *sql.DB, threadID, newSessionID string) error`
  - `WorkerPool` with message burst coalescing, 5m staleness expiration, turn-count session rotation, intent-driven typing indicators, and `[NO_REPLY]` silent suppression.

- [ ] **Step 1: Write failing unit tests in `brain/pkg/db/db_test.go` and `brain/pkg/queue/queue_test.go`**
  - Test `sessions` column migration for `turn_count INTEGER NOT NULL DEFAULT 0`.
  - Test `IncrementSessionTurnCount` and `RotateSessionID` (preserves `last_extracted_rowid` while resetting turn count).
  - Test `[NO_REPLY]` suppression in `WorkerPool` (marked `COMPLETED`, `DeliveryFunc` called 0 times).
  - Test 5-minute message staleness TTL (message older than 300s completed as `[EXPIRED_STALE]`).
  - Test message coalescing: multiple pending messages in channel drained using labeled break and combined into single turn prompt; all messages in batch updated to `COMPLETED`.
  - Test turn-count session rotation: session reset when `turn_count >= maxSessionTurns`.
  - Test typing indicator lifecycle (`typing_indicator: "on_mention"`).

- [ ] **Step 2: Run tests to verify failure**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v ./pkg/db ./pkg/queue
  ```

- [ ] **Step 3: Implement database migrations and WorkerPool safeguards**
  - In `brain/pkg/db/db.go`: Add `turn_count` column migration, `IncrementSessionTurnCount`, and `RotateSessionID`.
  - In `brain/pkg/queue/queue.go`:
    - Add staleness TTL check at start of `processMessage` before typing.
    - Implement channel message burst draining / coalescing in `runThreadWorker` using labeled break and batch claiming.
    - Implement `IsSilentSentinel` check in success path: mark `COMPLETED`, skip `DeliveryFunc`.
    - Mark all coalesced message IDs in batch as `COMPLETED`.
    - Check `turn_count` on successful turn: rotate session ID when `turn_count >= policy.MaxSessionTurns`.

- [ ] **Step 4: Run tests to verify pass**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/db ./pkg/queue
  ```

- [ ] **Step 5: Commit changes**
  ```bash
  git add brain/pkg/db brain/pkg/queue
  git commit -m "feat(queue): add burst coalescing, 5m staleness TTL, turn rotation, and silent sentinel suppression"
  ```

---

### Task 4: Gateway Ingestion, Bot Filtering, Admin Checking & Prompt Directives in `brain/funnel.go`

**Files:**
- Modify: `brain/funnel.go`
- Test: `brain/funnel_test.go`

**Interfaces:**
- Produces:
  - `func resolveChannelPolicy(cfg *config.Config, s *discordgo.Session, m *discordgo.Message) config.ChannelPolicy`
  - `func buildDiscordPrompt(m *discordgo.Message, targetThreadID string, isAdmin bool, isChannelMode bool, isMentioned bool) string`
  - Updated `RunStartupCatchUpSweep` with channel policy resolution.

- [ ] **Step 1: Write failing unit tests in `brain/funnel_test.go`**
  - Test bot message drop when `ignore_bots: true`.
  - Test `is_admin: true` vs `is_admin: false` resolution against `admin_users`.
  - Test channel routing in `mode: "channel"` (target is `ChannelID`, thread creation bypassed).
  - Test channel routing in `mode: "threads"` (thread creation invoked).
  - Test prompt directive formatting for `mode: "channel"` vs `mode: "threads"`.
  - Test reply-to attribution (`m.ReferencedMessage`) in prompt builder.

- [ ] **Step 2: Run test to verify failure**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v .
  ```

- [ ] **Step 3: Implement channel policy resolution, bot filtering, admin checking, and directive prompts in `brain/funnel.go`**
  - Integrate `cfg.ResolveChannelPolicy`.
  - Filter bot messages if `policy.IgnoreBots == true`.
  - Check author against `cfg.AdminUsers` $\to$ set `isAdmin`.
  - If `policy.Mode == "channel"` $\to$ `targetThreadID = m.ChannelID`, bypass thread creation.
  - In `buildDiscordPrompt`, append `- is_admin: true/false`, `reply_to` context if referenced, and Channel/Thread interaction directives.
  - Update `RunStartupCatchUpSweep` to resolve channel policy and bypass thread creation for channel-mode channels.

- [ ] **Step 4: Run tests to verify pass**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v .
  ```

- [ ] **Step 5: Commit changes**
  ```bash
  git add brain/funnel.go brain/funnel_test.go
  git commit -m "feat(funnel): add channel mode routing, bot filtering, admin resolution, and directive prompts"
  ```

---

### Task 5: Security Guidelines in `SYSTEM.md` & Config Templates

**Files:**
- Modify: `SYSTEM.md`
- Modify: `C:\Users\alexz\.gemini\antigravity\scratch\aerial-config-example\config.yaml`
- Test: Full integration test suite

- [ ] **Step 1: Update `SYSTEM.md` with Security & Privilege Boundary**
  - Add admin privilege instructions: only users with `- is_admin: true` are permitted to execute system config modifications or host operations.
  - Add per-turn author rule: privilege is strictly bound to the author of the current message.
  - Instruct polite refusal when non-admins request system config alterations.

- [ ] **Step 2: Update `aerial-config-example/config.yaml`**
  - Include `admin_users:` example list.
  - Include `channels:` block with required `default:` and example `general:` channel policy.

- [ ] **Step 3: Run full test suite across all 13 packages**
  ```bash
  docker run --rm -v gopath-cache:/go -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./...
  ```

- [ ] **Step 4: Commit changes and push to git**
  ```bash
  git add SYSTEM.md
  git commit -m "feat(security): add admin privilege boundary to SYSTEM.md and update config template"
  ```

---

### Verification Checklist
- [ ] `channels.default` validation enforced in `config.go`
- [ ] `mode: "channel"` routes turns to `ChannelID` without creating threads
- [ ] `[NO_REPLY]` output is silently absorbed without sending any Discord message
- [ ] Multiple rapid messages in a channel are coalesced into a single prompt turn
- [ ] Messages older than 5 minutes are dropped as `[EXPIRED_STALE]`
- [ ] Channel sessions rotate after 50 turns while preserving memory fact watermarks
- [ ] Typing indicators only pulse on mention in `mode: "channel"`
- [ ] Non-admin messages flag `is_admin: false` and receive polite refusals on system operations
- [ ] 100% passing tests under `-race` detector across all packages
