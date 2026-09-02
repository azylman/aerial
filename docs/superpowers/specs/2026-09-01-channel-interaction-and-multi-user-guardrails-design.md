# Design Specification: Channel-Based Interaction Model & Multi-User Security Guardrails

- **Author**: Antigravity & User Pair Programming
- **Date**: 2026-09-01
- **Status**: Ready for Implementation
- **Target Component**: `aerial-brain` (`brain/pkg/config`, `brain/funnel.go`, `brain/pkg/queue`, `brain/pkg/delivery`, `brain/pkg/runner`, `SYSTEM.md`)

---

## 1. Executive Summary & Objectives

Aerial currently operates primarily under a **thread-per-task** interaction model: every top-level message in Discord triggers thread creation, and turns are executed sequentially inside isolated thread sessions.

This specification introduces:
1. **Configurable Interaction Modes (Channel vs. Threads)**:
   - **`mode: channel`**: Ambient in-channel participation where every message in the channel is ingested and evaluated. One continuous conversation session is maintained per channel.
   - **`mode: threads`**: Dedicated thread creation per top-level conversation, preserving existing developer workflows.
2. **Autonomous LLM Evaluation & Silent Sentinel Gate**:
   - In channel mode, Aerial evaluates whether her input is helpful or requested.
   - If no response is warranted, Aerial outputs a silent sentinel (`[NO_REPLY]`), which the delivery pipeline absorbs without sending any message to Discord, while preserving the turn in the Antigravity conversation transcript for full ambient context.
   - If Aerial chooses to respond, her output is posted directly into the channel.
3. **Queue Resilience, Message Coalescing & Staleness Guards**:
   - **Message Coalescing**: If multiple messages queue up in a channel while a turn is evaluating, the worker drains pending messages and batches them chronologically into a single consolidated turn.
   - **Staleness TTL (5 Minutes)**: Any ambient message waiting behind a backlog longer than 300 seconds is expired silently without invoking LLM tokens.
   - **Turn-Based Session Rotation (50 Turns)**: To prevent runaway context windows and API token inflation, channel sessions rotate after reaching 50 turns. Long-term preferences and facts remain permanently indexed in the semantic memory database.
4. **Typing Indicators (`typing_indicator`)**:
   - For `mode: channel`, typing indicator defaults to `"on_mention"` (only pulses typing when explicitly @mentioned or replied to, keeping ambient channel evaluation silent).
5. **Multi-User Security Guardrails & Admin Privilege Model**:
   - Discord author IDs are evaluated against an `admin_users` allowlist in `config.yaml`.
   - The message payload flags `is_admin: true/false`.
   - Non-admins are instructed to not perform system config changes or host operations, while retaining full access to conversational chat, smart home/domain tools, and the semantic memory system.
   - Other bots are ignored by default (`ignore_bots: true`) to prevent recursive bot-to-bot ping loops.

---

## 2. Configuration Schema & Resolution (`brain/pkg/config`)

### 2.1 Configuration Model in `config.yaml`

```yaml
# ==============================================================================
# Aerial Configuration (aerial-config/config.yaml)
# ==============================================================================

model: "Gemini 3.7 Flash (High)"
timeout_minutes: 15
timezone: "America/Los_Angeles"
system_channel: "aerial-dev"

# Admin Discord User IDs authorized to perform system configuration & operations
admin_users:
  - "169260920550195200"

# Per-Channel Interaction Policies (REQUIRED: channels.default)
channels:
  default:
    mode: "threads"            # "threads" | "channel"
    typing_indicator: "always" # "always" | "on_mention" | "never"
    ignore_bots: true           # Ignore messages authored by other bots
    allow_system_ops: false    # Disallow non-admins from changing engine files or running host commands
    max_session_turns: 50      # Rotate session after 50 turns (0 = unlimited)

  # Channel-specific overrides (keyed by channel name or Discord channel ID)
  aerial-dev:
    mode: "threads"
    typing_indicator: "always"
    ignore_bots: true
    allow_system_ops: true

  general:
    mode: "channel"
    typing_indicator: "on_mention"  # Ambient turns evaluate silently without phantom typing
    ignore_bots: true
    allow_system_ops: false
    max_session_turns: 50
```

### 2.2 Go Data Structures

```go
type ChannelPolicy struct {
	Mode             string `yaml:"mode" json:"mode"`                           // "threads" or "channel"
	TypingIndicator  string `yaml:"typing_indicator" json:"typing_indicator"`   // "always", "on_mention", "never"
	IgnoreBots       bool   `yaml:"ignore_bots" json:"ignore_bots"`             // drop messages from bots
	AllowSystemOps   bool   `yaml:"allow_system_ops" json:"allow_system_ops"`   // allow admin ops in this channel
	MaxSessionTurns  int    `yaml:"max_session_turns" json:"max_session_turns"` // rotate session after N turns (default 50)
}

type UserConfig struct {
	Model          string                   `yaml:"model"`
	TimeoutMinutes int                      `yaml:"timeout_minutes"`
	Timezone       string                   `yaml:"timezone"`
	SystemChannel  string                   `yaml:"system_channel"`
	AdminUsers     []string                 `yaml:"admin_users"`
	Channels       map[string]ChannelPolicy `yaml:"channels"`
	GitSync        GitSyncConfig            `yaml:"git_sync"`
	MCPServers     map[string]MCPServerConf `yaml:"mcp_servers"`
}
```

### 2.3 Policy Resolution Order
1. Match channel by explicit Discord `ChannelID`.
2. Match channel by Discord channel name (case-insensitive, e.g. `general`).
3. Fall back to `channels["default"]`.
4. Validation requirement: `channels.default` must be defined with a valid `mode` (`"threads"` or `"channel"`).

---

## 3. Gateway Ingestion & Routing (`brain/funnel.go`)

### 3.1 Message Ingestion Pipeline

```
                     Incoming Discord Message
                                |
                                v
                   [ Self-Author Check ] ----(Author is Bot Self)----> Drop
                                |
                                v
               [ Policy Lookup: Channel ID/Name ]
                                |
                                v
                 [ Is Author a Bot? ] ----(Bot == true && ignore_bots)----> Drop
                                |
                                v
                 [ Admin Check: author_id in admin_users ]
                                |
                   +------------+------------+
                   |                         |
            mode: "channel"            mode: "threads"
                   |                         |
                   v                         v
        Target = ChannelID          Target = ThreadID (Existing or New)
        Session = ChannelID         Session = ThreadID
                   |                         |
                   +------------+------------+
                                |
                                v
                  [ Prompt Construction with Directives ]
                                |
                                v
                   [ WorkerPool.Enqueue(msg) ]
```

### 3.2 Prompt Construction

The `<USER_REQUEST>` prompt payload maintains all Discord fields while adding privilege and channel directive markers:

```markdown
<USER_REQUEST>
Here's a message someone sent you from Discord:
- id: 1543673824246108160
- channel_id: 1543668253363150928
- thread_id: 1543668253363150928
- guild_id: 1464358485817954538
- author_id: 169260920550195200
- author_username: arcane103
- author_global_name: Arcane
- author_bot: false
- is_admin: true
- content: Does anyone know if the deployment passed?
- timestamp: 2026-09-01T21:14:00Z
- mentions: []
- attachments: []

[Channel Interaction Directive]
You are participating in a shared channel. Evaluate whether your input is needed or requested.
- If you choose to respond, formulate your response text and output it on stdout.
- If no response is warranted, output solely "[NO_REPLY]".
</USER_REQUEST>
```

When `mode: "threads"`:
```markdown
[Thread Interaction Directive]
Please formulate your response and output it on stdout.
```

---

## 4. Execution, Queueing, Coalescing & Sentinel Suppression (`brain/pkg/queue`, `brain/pkg/delivery`, `brain/pkg/runner`)

### 4.1 Channel-Level FIFO Queueing & Message Coalescing
* In `mode: channel`, `msg.ThreadID` is set to `ChannelID`.
* `WorkerPool` assigns a dedicated worker goroutine per channel.
* **Burst Draining / Coalescing**: When a worker starts processing, if multiple messages have queued up in `ch` during the previous turn, the worker drains up to 5 pending messages and coalesces them chronologically into a single turn prompt:
  ```markdown
  <USER_REQUEST>
  [Recent In-Channel Messages Burst (Chronological)]
  - User A (10:01:05): "Is the API down?"
  - User B (10:01:12): "Never mind, it was just a local network hiccup."

  [Channel Interaction Directive]
  Evaluate whether your input is needed or requested. If no response is warranted, output solely "[NO_REPLY]".
  </USER_REQUEST>
  ```
* All coalesced messages are marked `COMPLETED` in SQLite in a single transaction.

### 4.2 Message Staleness TTL (5 Minutes)
* If an ambient message (`mode: channel`) has been in the queue for longer than 300 seconds (`5 * time.Minute`):
  ```go
  if policy.Mode == "channel" && time.Since(msg.CreatedAt) > 5*time.Minute {
      _ = db.UpdateMessageCompleted(p.cfg.DB, msg.ID, "[EXPIRED_STALE]")
      log.Printf("[WorkerPool] Dropped stale ambient message %s (age=%v)", msg.ID, time.Since(msg.CreatedAt))
      return
  }
  ```

### 4.3 Turn-Based Session Rotation (50 Turns)
* In `sessions` table, track `turn_count INTEGER NOT NULL DEFAULT 0`.
* When a channel session reaches `policy.MaxSessionTurns` (default 50 turns), `WorkerPool` deletes the active `internal_session_id` and starts a fresh Antigravity session on the next turn.
* This caps context window token growth to ~25k tokens while the semantic memory system preserves long-term domain knowledge.

### 4.4 Sentinel Detection & Clean Empty Stdout Handling

In `brain/pkg/runner/runner.go` (`ClassifyError`):
* Update `ClassifyError` so that `exitCode == 0` with empty `stdout` is treated as clean success (`isFailure = false`) instead of hard failure.

In `brain/pkg/queue/queue.go` (`processMessage`):
```go
trimmed := strings.TrimSpace(stdout)
isSilent := trimmed == "" || strings.HasPrefix(strings.ToUpper(trimmed), "[NO_REPLY]")

if isSilent {
    stopTyping()
    _ = db.UpdateMessageCompleted(p.cfg.DB, msg.ID, "[NO_REPLY]")
    log.Printf("[WorkerPool] Message %s in %s completed silently ([NO_REPLY] sentinel)", msg.ID, msg.ThreadID)
    if p.cfg.OnMessageCompleted != nil {
        p.cfg.OnMessageCompleted(msg, db.StatusCompleted)
    }
    return
}

// Non-silent response: Deliver to Discord
stopTyping()
if !skipDiscord {
    if err := p.cfg.DeliveryFunc(p.getDiscordSession(), msg.ThreadID, stdout); err != nil {
        log.Printf("[WorkerPool] Failed to deliver response for message %s to %s: %v", msg.ID, msg.ThreadID, err)
    }
}
_ = db.UpdateMessageCompleted(p.cfg.DB, msg.ID, stdout)
```

### 4.5 Typing Indicator Lifecycle
* In `mode: channel` with `typing_indicator: "on_mention"`:
  - If message contains `@Aerial` or is a reply to an Aerial message $\to$ pulse typing immediately.
  - For unmentioned ambient messages $\to$ evaluate silently without pulsing typing.

---

## 5. Multi-User Security Guardrails & Admin Privilege Model

### 5.1 System Persona Security Invariant (`SYSTEM.md` & `system_instructions.md`)

`SYSTEM.md` establishes an immutable security rule loaded into Antigravity's system instructions:

```markdown
## Security & Privilege Boundary
- When responding in Discord, inspect the `- is_admin:` flag in the message header.
- **Admin Users (`is_admin: true`)**: Authorized to modify system configuration, alter `SYSTEM.md`, `AGENTS.md`, or `config.yaml`, manage Docker containers, trigger git sync operations, and request system restarts.
- **Non-Admin Users (`is_admin: false`)**: Authorized to chat, ask questions, query memory, use smart home/weather domain tools, and participate in conversations. Non-admins are STRICTLY PROHIBITED from modifying core system configuration, altering system rules, or executing host infrastructure commands.
- If a non-admin requests system modifications, decline politely and concisely.
```

### 5.2 Layered Protections
1. **Engine Filesystem Mount**: `SYSTEM.md` is mounted read-only inside `aerial-brain`.
2. **Memory System Open Access**: Semantic facts and user preferences are extracted per user (`thread_id` and user context) without granting system administrative rights.
3. **Bot Loop Shield**: `ignore_bots: true` prevents bot-to-bot recursion in multi-bot channels.

---

## 6. Verification & Test Plan

### Automated Test Matrix
1. **`brain/pkg/config/config_test.go`**:
   - Test parsing `admin_users` and `channels` policy map from `config.yaml`.
   - Test validation error when `channels.default` is missing.
   - Test channel policy fallback hierarchy (channel ID $\to$ channel name $\to$ default).
   - Test `typing_indicator: "on_mention"` parsing.
2. **`brain/funnel_test.go`**:
   - Test bot message drop when `ignore_bots: true`.
   - Test `is_admin: true` vs `is_admin: false` resolution against `admin_users`.
   - Test channel routing in `mode: channel` (target is `ChannelID`, thread creation is bypassed).
   - Test channel routing in `mode: threads` (thread creation is invoked).
   - Test prompt construction with `[Channel Interaction Directive]` vs `[Thread Interaction Directive]`.
3. **`brain/pkg/queue/queue_test.go`**:
   - Test `[NO_REPLY]` output suppression: message marked `COMPLETED` in SQLite, `DeliveryFunc` is called 0 times.
   - Test message coalescing: pending messages in queue drained and batched into single turn.
   - Test 5-minute staleness expiration: old messages dropped as `[EXPIRED_STALE]`.
   - Test 50-turn session rotation: new session created when turn threshold exceeded.
   - Test typing indicator on_mention vs always.
4. **`brain/pkg/runner/runner_test.go`**:
   - Test `ClassifyError` with exit code 0 and empty stdout returning clean success (`isFailure: false`).

### Manual Live Stack Verification
- In `#general` (`mode: channel`): Send ambient chat $\to$ verify Aerial evaluates and remains silent with `[NO_REPLY]` when not addressed, and responds directly in the channel when asked a question.
- In `#aerial-dev` (`mode: threads`): Verify developer workflow creates threads and executes commands as an admin.
- Non-admin test: Have a non-admin user prompt Aerial to modify `config.yaml` $\to$ verify polite refusal.

---

## 7. Migration & Rollout Plan

1. Update `brain/pkg/config/config.go` with `ChannelPolicy` structures, `admin_users`, and validation.
2. Update `brain/pkg/runner/runner.go` to support clean empty stdout.
3. Update `brain/funnel.go` with policy resolution, bot filtering, admin checking, and directive prompts.
4. Update `brain/pkg/queue/queue.go` with coalescing, 5m TTL, 50-turn rotation, and `[NO_REPLY]` suppression.
5. Update `SYSTEM.md` with the Security & Privilege Boundary.
6. Update `aerial-config/config.yaml` and `aerial-config-example/config.yaml` to include `admin_users:` and `channels.default:`.
7. Run full unit tests (`go test -race ./...`).
8. Commit, push, and let GHCR + Watchtower deploy to the MiniPC.
