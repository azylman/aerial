# Implementation Plan: Dockerfile-Curated Skills & Code Simplification

## Problem & Context
Previously, `brain/Dockerfile` cloned the entire upstream `obra/superpowers` repository into `/opt/superpowers`. Because the full repository included runtime-clashing skills (`using-git-worktrees`, `finishing-a-development-branch`, `requesting-code-review`, `subagent-driven-development`) and the prompt-bloating `using-superpowers` meta-enforcer, Go code in `brain/pkg/env` had to maintain a blacklist (`DefaultBlockedSkills`), filtering methods (`LinkSkillsWithFilter`), and pruning logic (`pruneBlockedSkills`).

Per user instruction ("as much as possible should be in the Dockerfile and not in code"):
We curate the installed engineering skills directly during container image build in `brain/Dockerfile`. By only copying approved methodology skills into `/opt/skills` and creating a clean symlink to `/opt/superpowers/skills` for backwards compatibility, the unwanted clashing skills are never present in the image. This allows removing all blacklist, filtering, and pruning logic from Go code, making `brain/pkg/env` clean, robust, and agnostic.

## Approved Methodology Skills to Curate in Dockerfile
- `systematic-debugging`
- `test-driven-development`
- `verification-before-completion`
- `brainstorming`
- `writing-plans`
- `dispatching-parallel-agents`
- `receiving-code-review`
- `writing-skills`

## Unwanted Clashes Excluded from Image
- `using-superpowers` (removes conversation latency and auto-viewing on every turn)
- `using-git-worktrees` (incompatible with kernel `:ro` mounts)
- `finishing-a-development-branch` (interactive merge prompts)
- `requesting-code-review` & `subagent-driven-development` (chattery per-task loops)
- `executing-plans` (depends on `using-git-worktrees` and `finishing-a-development-branch`)

## Proposed Changes

### 1. `brain/Dockerfile`
- Replace raw `git clone ... /opt/superpowers` with curated extraction:
  - Clone `obra/superpowers` to a temporary directory `/tmp/superpowers`.
  - Copy only the 8 approved methodology skills into `/opt/skills/`.
  - Remove `/tmp/superpowers`.
  - Symlink `/opt/skills` to `/opt/superpowers/skills` for backwards compatibility.

### 2. `brain/pkg/env/skills.go`
- Remove `DefaultBlockedSkills`.
- Remove `pruneBlockedSkills()`.
- Replace `LinkSkillsWithFilter` with clean, direct `LinkSkills(targetSkillDirs, sourceDirs []string) int`.
- In `SyncSkills()`:
  - Keep legacy `~/.gemini/skills` directory cleanup.
  - Keep `sweepOrphanedSymlinks(targetSkillDirs)` (which automatically cleans up any legacy symlinks to skills no longer present on disk).
  - Call `LinkSkills(targetSkillDirs, sourceDirs)`.
  - Perform final `sweepOrphanedSymlinks`.

### 3. `brain/pkg/env/provisioner.go`
- Remove `blockedSkills` field from `Provisioner`.
- Remove `SetBlockedSkills` and `BlockedSkills` methods.
- Initialize `superpowersDir` to `/opt/skills` (with fallback to `/opt/superpowers/skills`).

### 4. `brain/pkg/env/env_test.go`
- Replace tests for `DefaultBlockedSkills`, `LinkSkillsWithFilter`, and `pruneBlockedSkills` with comprehensive tests verifying clean symlinking, source priority shadowing, and orphaned symlink cleanup.

## Verification & Deployment Plan
1. Run targeted Go package unit tests: `go test -v -race ./brain/pkg/env/...`
2. Run local pre-flight verification: `./scripts/verify.sh --staged`
3. Audit pre-PR diff with Devil's Advocate subagent
4. Author `PR_DESCRIPTION.md` and submit asynchronously via `scripts/aerial-pr.sh submit`
