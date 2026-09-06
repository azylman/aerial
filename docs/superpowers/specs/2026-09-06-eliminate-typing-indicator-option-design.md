# Design Spec: Universal Typing Indicator on Active Turns

## 1. Problem Statement
Currently, Aerial supports a channel policy configuration option `typing_indicator` with three modes: `"always"`, `"on_mention"`, and `"never"`.
In channel mode (`mode: "channel"` with `wake_mode: "classifier"`), the default `typing_indicator: "on_mention"` causes a UX disconnect:
- When a user directly @mentions Aerial, the typing indicator triggers as expected.
- When a user continues an active conversation naturally without typing `@Aerial`, the ambient classifier wakes Aerial up, but because `isMentionOrReply()` returns false, the typing indicator is suppressed. Aerial processes the turn for 20-30s in complete silence before suddenly delivering a message.

Because background ambient chatter that Aerial ignores (`wakeIdx == -1`) is already evaluated and discarded upstream in `pkg/queue/queue.go` before reaching the execution pipeline, typing indicators on inactive chatter are already impossible. Therefore, the `typing_indicator` configuration option adds unnecessary complexity and causes phantom silence during active ambient turns.

## 2. Goals & Non-Goals
### Goals
- **Universal Active Typing**: Whenever Aerial executes a turn that produces a response (direct mention, keyword wake, ambient classifier wake, or thread message), immediately start and refresh the Discord typing indicator.
- **Silent Ambient Dropping**: Ensure ambient messages that Aerial ignores remain completely silent without triggering any Discord typing API calls.
- **Config Simplification**: Eliminate `typing_indicator` from `ChannelPolicy`, simplifying channel configuration and documentation.
- **Backwards Compatibility**: Gracefully ignore `typing_indicator` if present in existing YAML configs without throwing unmarshal or validation errors.

### Non-Goals
- Altering the ambient classifier threshold or lookback heuristics.
- Changing `StartTyping` ticker cadence (8 seconds) or Prometheus metrics (`DiscordTypingSessionsActive`).

## 3. Architecture & Implementation Design

### 3.1 `brain/pkg/config/config.go`
- In `ChannelPolicy`: Retain optional `TypingIndicator string` struct tag with `json:"-" yaml:"typing_indicator,omitempty"` purely for zero-breakage backwards compatibility on existing YAML parsing, but deprecate its active usage.
- In configuration normalization/validation: Remove all defaulting and validation checks for `TypingIndicator` (e.g. `defPolicy.TypingIndicator = ...`, validating against `"always"|"on_mention"|"never"`).

### 3.2 `brain/pkg/queue/queue.go`
- Remove `resolveTypingStarter()` helper function.
- In `processBurst()`:
  - When an active wake turn proceeds past ambient evaluation (line 1087):
    ```go
    if !skipDiscord && p.cfg.TypingFunc != nil {
        stopTyping = p.cfg.TypingFunc(p.getDiscordSession(), threadID)
    }
    ```
  - For ambient bursts that do NOT wake Aerial (`wakeIdx == -1`), `processBurst()` returns at line 978 before `stopTyping` is assigned, preserving zero typing calls for background chatter.

### 3.3 Documentation & Examples
- Remove `typing_indicator` from `config.example.yaml`, `README.md`, and `GEMINI.md`.

### 3.4 User Configuration Repository (`aerial-config`)
- Remove `typing_indicator` keys from all channel entries in `aerial-config/config.yaml`.

## 4. Test Strategy & Verification
- `pkg/config/config_test.go`: Update channel policy normalization tests to verify clean parsing without `TypingIndicator`.
- `pkg/queue/queue_test.go`:
  - Test active wake turn in thread mode -> typing indicator starts.
  - Test active wake turn in channel mode with direct mention -> typing indicator starts.
  - Test active wake turn in channel mode with ambient classifier wake -> typing indicator starts.
  - Test inactive ambient chatter in channel mode -> typing indicator does NOT start.
- Pre-flight monorepo verification: Run `./scripts/verify.sh` covering linting, static analysis, unit tests, and race detection.
