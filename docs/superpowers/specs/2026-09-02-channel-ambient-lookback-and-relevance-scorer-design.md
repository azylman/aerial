# Design Spec: Channel-Mode Native Transcript Lookback & Fast Ambient Relevance Scorer

**Date:** 2026-09-02  
**Status:** Approved in Brainstorming  
**Target Subsystems:** `brain/funnel.go`, `brain/pkg/queue`, `brain/pkg/config`, `brain/pkg/session`

---

## 1. Problem Statement & Motivation

In Discord `mode: "channel"` (e.g. `#lounge`), Aerial shares a room with multiple humans and peer autonomous bots (such as Amos and Zero). 

Previously, every message in the channel was sent to the primary LLM runner (`runner.RunAgy`), relying on the model to evaluate the message and output `[NO_REPLY]` on casual banter. This approach caused three major problems:
1. **High Token Costs & Rate Limits:** Hundreds of ambient banter messages triggered full multi-second LLM invocations.
2. **The "Few-Shot Muzzle" Bug:** Dozens of consecutive `[NO_REPLY]` turns in session history primed the model to output `[NO_REPLY]` even on direct questions.
3. **Session Rotation Context Wiping:** Every ambient message incremented `turn_count`, triggering premature session rotation and wiping conversational memory right before a user asked Aerial a question.

---

## 2. Goals & User Decisions

1. **Zero-Prompt-Injection Conversational Lookback:** Unaddressed ambient messages are quietly logged directly into Antigravity's native `transcript.jsonl` and `transcript_full.jsonl` files on disk as standard `USER_INPUT` steps.
2. **Complete Elimination of `[NO_REPLY]`:** No fake `PLANNER_RESPONSE` turns are written, no `[NO_REPLY]` instructions are passed in prompts, and no sentinel parsing is needed.
3. **Two-Tier Wake Architecture:**
   - **Tier 1 (Direct Address):** Mentions (`@Aerial`), replies to Aerial, or natural name usage (`\bAerial\b`) wake Aerial immediately with 0 extra latency.
   - **Tier 2 (Fast Ambient Relevance Scorer):** Unaddressed messages with the trailing 10 messages of channel context are evaluated by a fast, low-effort model. If `confidence >= ambient_wake_threshold`, it is treated as if addressed directly.
4. **Queue Serialization (Zero File Collisions):** The Discord gateway enqueues all messages directly to `queue.WorkerPool`. The single-threaded per-channel worker (`runThreadWorker`) evaluates Tier 1 vs. Tier 2, writes ambient turns, or executes `runner.RunAgy`, guaranteeing that `agy` and the Go process never touch `transcript.jsonl` simultaneously.
5. **Decoupled Session Limits:** Only active wake turns (`runner.RunAgy`) increment `turn_count` toward `MaxSessionTurns`. Ambient chatter never triggers premature session rotation.
6. **Noop on Missing Transcripts:** If a channel's session transcript does not already exist on disk (meaning `agy` has never been invoked for this session), `AppendAmbientTurn` strictly noops and does not create phantom files or directories.

---

## 3. Architecture & Data Flow

```mermaid
flowchart TD
    A["Discord Gateway Event (MESSAGE_CREATE)"] --> B{"Channel Policy<br/>mode == 'channel'?"}
    B -- "NO (threads/ignore)" --> C["Existing Flow"]
    B -- "YES" --> D["Enqueue to WorkerPool<br/>pool.Enqueue(msg)"]
    
    D --> E["runThreadWorker(channelID)<br/>(Strictly FIFO & Single-Threaded)"]
    E --> F{"Tier 1: Direct Wake Trigger?<br/>• @Aerial mention<br/>• Reply to Aerial<br/>• Word boundary \\bAerial\\b"}
    
    F -- "YES (Tier 1 Wake)" --> G["Active Wake Flow<br/>• turn_count++<br/>• runner.RunAgy(sessionID)<br/>• Deliver to Discord"]
    
    F -- "NO (Ambient Message)" --> H["Fetch last 10 messages<br/>from SQLite for this channel"]
    H --> I["Fast Relevance Classifier (Low Effort)<br/>Returns confidence: 0.0 to 1.0"]
    
    I --> J{"confidence >= threshold?<br/>(from config.yaml)"}
    
    J -- "YES (Tier 2 Wake)" --> G
    
    J -- "NO (Below Threshold)" --> K["Quiet Ingestion Flow<br/>• session.AppendAmbientTurn(prompt)<br/>• Mark SQLite COMPLETED [AMBIENT]<br/>• Skip RunAgy & Discord Delivery"]
```

---

## 4. Configuration Schema (`brain/pkg/config`)

Add `AmbientWakeThreshold` to `ChannelPolicy`:

```go
type ChannelPolicy struct {
	Mode                 string  `yaml:"mode" json:"mode"`
	TypingIndicator      string  `yaml:"typing_indicator" json:"typing_indicator"`
	IgnoreBots           bool    `yaml:"ignore_bots" json:"ignore_bots"`
	MaxSessionTurns      int     `yaml:"max_session_turns" json:"max_session_turns"`
	AmbientWakeThreshold float64 `yaml:"ambient_wake_threshold" json:"ambient_wake_threshold"`
}
```

In `config.yaml`:
```yaml
channels:
  default:
    mode: "ignore"
  lounge:
    mode: "channel"
    ambient_wake_threshold: 0.80  # 0.0 to 1.0 (0.0 = disabled, only explicit pings wake)
```

### Defaults & Validation:
- If `mode: "channel"` and `ambient_wake_threshold` is omitted, default to `0.80`.
- If set to `0.0`, the ambient classifier is disabled; only Tier-1 triggers will wake Aerial.
- Valid range: `0.0 <= ambient_wake_threshold <= 1.0`.

---

## 5. Component Details

### 5.1 Discord Gateway (`brain/funnel.go`)
- Gateway event handler builds `db.Message` using the shared `buildDiscordPrompt(m, targetThreadID, policy)`.
  - In `buildDiscordPrompt`, remove the outdated sentence: *"If this message does not require your response or is general banter not directed at you, output [NO_REPLY] as your entire response."*
- Calls `pool.Enqueue(msg)` immediately. Zero disk I/O in the gateway event handler.

### 5.2 Wake Classification & Scorer (`brain/pkg/queue`)
Inside `processBurst(threadID, burst)`:

#### Tier 1 Check:
- Direct Mention: `<@botUserID>` or `<@!botUserID>`.
- Reply: `m.ReferencedMessage != nil && m.ReferencedMessage.Author.ID == botUserID`.
- Name Keyword: Regex `\b(?i:aerial)\b` (excluding non-agent phrases such as *"aerial view"*, *"aerial photo"*).

#### Tier 2 Check (Fast Ambient Classifier):
- If not Tier 1, and `policy.AmbientWakeThreshold > 0`:
  - Query SQLite for the last 10 messages in `threadID`.
  - Invoke fast classifier prompt with low reasoning effort.
  - Parse JSON: `{"confidence": float, "reason": string}`.
  - If `confidence >= policy.AmbientWakeThreshold`, treat as active wake.

### 5.3 Native Transcript Appending (`brain/pkg/session`)
`AppendAmbientTurn(convID string, prompt string, createdAt time.Time) error`:
1. Resolve session directory `/data/brain/<convID>/.system_generated/logs` (or `~/.gemini/antigravity/brain/...`).
2. If `transcript.jsonl` does NOT exist: **noop and return `nil`**.
3. If it exists:
   - Use `os.Seek` to scan the last 4096 bytes from `io.SeekEnd` to extract the latest `step_index: N`.
   - Append a single, clean `USER_INPUT` line to both `transcript.jsonl` and `transcript_full.jsonl`:
     ```json
     {"step_index": N+1, "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE", "created_at": "...", "content": "<prompt>"}
     ```
   - No `[NO_REPLY]` or `PLANNER_RESPONSE` is written.

### 5.4 Active Wake Turn Flow
1. If the burst had prior ambient messages, append them to `transcript.jsonl`.
2. Increment `turn_count` via `db.IncrementSessionTurnCount`. Rotate session if `turn_count >= MaxSessionTurns`.
3. Start typing indicator.
4. Execute `runner.RunAgy(ctx, sessionID, prompt, ...)`.
5. Deliver response to Discord and mark message `COMPLETED`.

---

## 6. Error Handling & Fail-Safes

1. **Classifier Fail-Safe (Default Silent):** 5-second timeout on the fast classifier. Any network error, timeout, or invalid JSON falls back to `confidence = 0.0`. It quietly records ambient text and does not wake Aerial.
2. **Missing Transcripts:** If a session directory does not exist, `AppendAmbientTurn` safely noops without creating phantom directories.
3. **Stale Messages:** 5-minute staleness TTL is preserved; expired messages are marked `[EXPIRED_STALE]` and dropped from wake evaluation.

---

## 7. Testing & Verification Plan

1. **Config Tests (`pkg/config/config_test.go`):**
   - YAML unmarshaling and validation of `ambient_wake_threshold`.
   - Default inheritance (`0.80` for channel mode, `0.0` disabled).
2. **Session Transcript Tests (`pkg/session/session_test.go`):**
   - Noop behavior when session transcript does not exist.
   - Monotonic `step_index` increment when transcript exists.
   - Dual-file synchronization (`transcript.jsonl` and `transcript_full.jsonl`).
   - Zero `[NO_REPLY]` verification.
3. **Queue & Classifier Tests (`pkg/queue/queue_test.go`):**
   - Tier 1 wake triggers `RunAgy` and increments `turn_count`.
   - Tier 2 wake (confidence >= threshold) triggers `RunAgy` and increments `turn_count`.
   - Ambient turn (confidence < threshold) calls `AppendAmbientTurn`, does NOT increment `turn_count`, skips `RunAgy`.
   - Mixed burst (ambient messages followed by wake) processes correctly in order.
4. **Full Test Suite:**
   ```bash
   docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race ./...
   ```
