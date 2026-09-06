# Implementation Plan: Eliminate Typing Indicator Option

## Overview
Standardize on universal active-turn typing across the Aerial engine by removing `typing_indicator` configuration mechanics, ensuring all response turns pulse typing while non-wake background chatter remains silent.

## Proposed Changes

### Task 1: Engine Config Normalization & Backwards Compatibility
- **Files**: `brain/pkg/config/config.go`, `brain/pkg/config/config_test.go`
- **Action**:
  - Update `ChannelPolicy` in `config.go` to make `TypingIndicator` an optional deprecated/ignored field for YAML backwards compatibility.
  - Remove all defaulting logic and validation routines asserting on `"always"|"on_mention"|"never"`.
  - Update `config_test.go` to test that YAML with or without `typing_indicator` parses cleanly without errors.
- **Verification**: `cd brain && go test -v ./pkg/config`

### Task 2: Engine Queue Universal Active-Turn Typing
- **Files**: `brain/pkg/queue/queue.go`, `brain/pkg/queue/queue_test.go`
- **Action**:
  - Remove `resolveTypingStarter()` from `pkg/queue/queue.go`.
  - Replace typing starter assignment in `processBurst()` with direct invocation:
    ```go
    if !skipDiscord && p.cfg.TypingFunc != nil {
        stopTyping = p.cfg.TypingFunc(p.getDiscordSession(), threadID)
    }
    ```
  - Update `queue_test.go` to verify typing triggers on direct mentions, thread turns, and ambient classifier wakes, while ensuring purely ambient chatter does not trigger typing.
- **Verification**: `cd brain && go test -v -race ./pkg/queue`

### Task 3: Core Docs, Examples & Invariant Audit
- **Files**: `config.example.yaml`, `README.md`, `GEMINI.md`
- **Action**:
  - Remove references and configuration examples of `typing_indicator`.
  - Ensure zero personal data and zero plaintext secrets are committed.
- **Verification**: `./scripts/verify.sh`

### Task 4: User Config Cleanup
- **Target**: `azylman/aerial-config` via `scripts/aerial-config-pr.sh`
- **Action**:
  - Remove `typing_indicator` lines from `config.yaml`.
- **Verification**: `scripts/aerial-config-pr.sh submit` validation pass.

## Review Gates
- Stage 2: The 4-Expert Review Panel (The Girl Gang) plan audit.
- Stage 3: Human Review Checkpoint with Alex.
- Stage 4: Per-Task Girl Gang code review.
- Stage 5: Monorepo pre-flight verification (`./scripts/verify.sh`).
- Stage 6: Git commit, push to main, automated CI deployment.
