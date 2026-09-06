# Design Specification: Eliminating Unnecessary Configuration Options & Standardizing Engine Invariants

**Date**: 2026-09-06  
**Status**: Draft  
**Target Repositories**: `azylman/aerial` & `azylman/aerial-config`  
**Scope**: `brain/pkg/config`, `brain/pkg/queue`, `brain/main.go`, `config.example.yaml`, documentation, and user `config.yaml`.

---

## 1. Executive Summary & Context

Aerial's configuration schema has evolved to become lean and intent-driven. Following the successful removal of `typing_indicator`, an audit of the remaining configuration surface identified four options that add schema complexity without delivering operational value:

1. **`allow_system_ops`**: Present in `ChannelPolicy` and tested in unit tests, but 100% dead code across the execution engine (`queue.go`, `delivery.go`, etc.). Security and administrative boundaries are governed by the global `admin_users` allowlist and system prompt rules.
2. **`max_session_turns`**: Exposed per-channel in `ChannelPolicy` (defaulting to 50 for channel mode). In practice, channel session rotation is an internal context-window management mechanism. Exposing it as a YAML knob causes unnecessary cognitive load.
3. **`timeout_minutes`**: Exposed as a top-level YAML configuration knob (defaulting to 15). Turn timeout is a core execution boundary that is universally standardized at 15 minutes.
4. **`memory.fact_extraction`**: Documented as an example in `config.example.yaml`, but never parsed into the Go `Config` struct. It is a phantom configuration block.

This specification details the complete excision of these four unnecessary options, standardizing session rotation and execution timeouts to internal engine constants while preserving backwards-compatible YAML decoding.

---

## 2. Architectural Design & Changes

### 2.1 Elimination of `allow_system_ops`
- **Excision**: Remove `AllowSystemOps bool` from `ChannelPolicy` in `brain/pkg/config/config.go`.
- **Normalization & Resolution**: Remove `AllowSystemOps` copying and defaulting logic from `ResolveChannelPolicy()`, `DefaultConfig()`, and `LoadConfig()`.
- **Tests**: Clean up all assertions referencing `AllowSystemOps` in `brain/pkg/config/config_test.go`.

### 2.2 Standardizing Channel Session Rotation (`max_session_turns`)
- **Engine Constant**: Introduce `const DefaultMaxSessionTurns = 50` in `brain/pkg/queue/queue.go` (or `brain/pkg/config/config.go`).
- **Excision**: Remove `MaxSessionTurns int` from `ChannelPolicy`.
- **Queue Execution**: In `brain/pkg/queue/queue.go`, in `channel` mode (`policy.Mode == "channel"`), session rotation evaluates against `DefaultMaxSessionTurns` (50 turns). In `threads` mode, Discord threads manage their own conversational lifecycle natively without rotation.
- **Tests**: Update `config_test.go` and `queue_test.go` to verify standard 50-turn rotation in channel mode.

### 2.3 Standardizing Turn Timeout (`timeout_minutes`)
- **Engine Constant**: Introduce `const DefaultTimeoutMinutes = 15` in `brain/pkg/queue/queue.go`.
- **Excision**: Remove `TimeoutMinutes int` from `Config` struct in `brain/pkg/config/config.go`.
- **WorkerPool & Main**:
  - `WorkerPoolConfig.TimeoutMinutes` defaults to `DefaultTimeoutMinutes` (15) if unset or <= 0.
  - Refactor `WorkerPool.UpdateRuntimeConfig(model string)` to accept only `model` (removing `timeoutMinutes` parameter).
  - Update `brain/main.go` where `pool.UpdateRuntimeConfig` and startup logging are called.
- **Config YAML**: Remove `timeout_minutes: 15` from `azylman/aerial-config/config.yaml`.

### 2.4 Phantom Block Cleanup (`memory.fact_extraction`)
- **Excision**: Remove the `memory.fact_extraction` block from `config.example.yaml`, `README.md`, and `GEMINI.md`.

---

## 3. Two-Repository Migration Plan

1. **`azylman/aerial` (Core Engine)**:
   - Implement Go code changes, struct updates, and test refactors on branch `feat/prune-unnecessary-config-options`.
   - Update `config.example.yaml`, `README.md`, and `GEMINI.md`.
   - Run full monorepo verification via `./scripts/verify.sh`.
   - Push and open PR on GitHub.
2. **`azylman/aerial-config` (User Configuration)**:
   - Initialize scratch checkout via `scripts/aerial-config-pr.sh init`.
   - Remove `timeout_minutes: 15` from `config.yaml`.
   - Submit and merge PR via `scripts/aerial-config-pr.sh submit`.

---

## 4. Software Engineering Principles & Invariants

- **DRY & Cohesion**: Eliminate dead fields and simplify `ChannelPolicy` to strictly operational routing settings (`mode`, `wake_mode`, `ignore_bots`, `ambient_wake_threshold`, `ambient_wake_prompt`).
- **Backwards Compatibility**: Unknown fields in user YAML files continue to decode smoothly without error in `yaml.v3`.
- **Zero Personal Data & Zero Secrets**: Ensure all example configs and test fixtures remain 100% generic and sanitized.
- **Verification**: Zero-bypass policy (`./scripts/verify.sh` must pass with 0 errors).
