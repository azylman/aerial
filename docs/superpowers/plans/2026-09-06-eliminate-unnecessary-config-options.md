# Implementation Plan: Eliminate Unnecessary Configuration Options

**Date**: 2026-09-06  
**Status**: Draft  
**Branch**: `feat/prune-unnecessary-config-options`  
**Target Repositories**: `azylman/aerial` & `azylman/aerial-config`

---

## 1. Overview & Objectives

Eliminate four unnecessary configuration options across the codebase and user configuration:
1. `allow_system_ops` (dead code excision from `ChannelPolicy`)
2. `max_session_turns` (standardize to engine constant `DefaultMaxSessionTurns = 50`)
3. `timeout_minutes` (standardize to engine constant `DefaultTimeoutMinutes = 15`)
4. `memory.fact_extraction` (phantom config cleanup from documentation/examples)

---

## 2. Step-by-Step Implementation Tasks

### Task 1: Clean Up `brain/pkg/config`
- **File**: `brain/pkg/config/config.go`
  - Remove `AllowSystemOps bool` and `MaxSessionTurns int` from `ChannelPolicy`.
  - Remove `TimeoutMinutes int` from `Config` and `Options` (or keep Options as legacy JSON fallback if needed).
  - Remove defaulting/normalization for `AllowSystemOps`, `MaxSessionTurns`, and `TimeoutMinutes` from `DefaultConfig()`, `LoadConfigFromPaths()`, and `ResolveChannelPolicy()`.
- **File**: `brain/pkg/config/config_test.go`
  - Refactor all test fixtures and assertions to remove references to `AllowSystemOps`, `MaxSessionTurns`, and `TimeoutMinutes`.

### Task 2: Standardize `brain/pkg/queue`
- **File**: `brain/pkg/queue/queue.go`
  - Define `const DefaultMaxSessionTurns = 50`.
  - Define `const DefaultTimeoutMinutes = 15`.
  - Ensure `WorkerPoolConfig.TimeoutMinutes` defaults to `DefaultTimeoutMinutes` (15) if not set.
  - In `processBurst`, when evaluating session rotation for channel mode (`policy.Mode == "channel"`), compare against `DefaultMaxSessionTurns` (50 turns).
  - Simplify `WorkerPool.UpdateRuntimeConfig(model string)` (removing unused `timeoutMinutes` parameter).
- **File**: `brain/pkg/queue/queue_test.go`
  - Refactor test assertions and fixtures to use `DefaultMaxSessionTurns`.

### Task 3: Update `brain/main.go`
- **File**: `brain/main.go`
  - Update `pool.UpdateRuntimeConfig(latestModel)`.
  - Update startup logging: `log.Printf("Aerial Brain listening on port %s (model=%s, timeout=%dm)", portStr, cfg.Model, queue.DefaultTimeoutMinutes)`.

### Task 4: Clean Up Documentation & Example Configs
- **Files**: `config.example.yaml`, `README.md`, `GEMINI.md`
  - Remove `allow_system_ops`, `max_session_turns`, and `timeout_minutes` from `config.example.yaml` and examples.
  - Remove `memory.fact_extraction` phantom block.
  - Ensure 100% domain-agnostic, zero personal data, and zero secrets.

### Task 5: Monorepo Verification & Core PR
- Run `./scripts/verify.sh` to confirm 0 lint, build, or test errors.
- Commit, push, and open Pull Request on `azylman/aerial`.
- Wait for CI checks to pass and merge.

### Task 6: User Configuration PR (`azylman/aerial-config`)
- Run `scripts/aerial-config-pr.sh init`.
- Remove `timeout_minutes: 15` from `config.yaml`.
- Run `scripts/aerial-config-pr.sh submit` to validate, push, open PR, pass CI, and merge.

---

## 3. Verification & Acceptance Criteria

- `go test -v -race ./...` passes across all packages in `brain/`.
- `verify.sh` passes with exit code 0.
- All 12 GitHub Actions CI checks pass on PR.
- Zero markdown tables in all agent responses.
