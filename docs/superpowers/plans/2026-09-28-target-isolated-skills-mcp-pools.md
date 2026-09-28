# Target-Isolated Skills, MCP Configuration & Dual Process Pools Implementation Plan

> **Note**: This implementation plan executes the architectural specification approved in `docs/superpowers/specs/2026-09-28-target-isolated-skills-mcp-pools.md`. It isolates skills and MCP servers between Discord and Voice runtimes, partitions built-in tools, excises cross-contamination code in `provisionRuntimeSharedAssets`, and establishes dual singleton process pools in `brain/main.go`.

## Proposed Changes

### Core Skills Directory
- `.agents/skills/discord/self-improvement/SKILL.md` (moved from `.agents/skills/self-improvement`)
- `.agents/skills/discord/incident-triage/SKILL.md` (moved from `.agents/skills/incident-triage`)

### Configuration Subsystem (`brain/pkg/config`)
- `brain/pkg/config/config.go`: Update `ConfigData.McpServers` to `TargetMcpConfig` struct (`Common`, `Discord`, `Voice`), deep cloning in `cloneConfigData`, YAML decoding in `rawConfigHelper`, default initialization in `DefaultConfigData`.
- `brain/pkg/config/config_test.go`: Tests for `TargetMcpConfig` serialization, deep cloning, and flat key rejection.

### Runner Subsystem (`brain/pkg/runner`)
- `brain/pkg/runner/pure.go`: Update `BuildAgyEnv` to proactively strip existing `HOME`, `USERPROFILE`, and `GEMINI_CLI_HOME` before appending target runtime directory.
- `brain/pkg/runner/pure_test.go`: Table-driven tests for environment sanitization and duplicate variable stripping.
- `brain/pkg/runner/unified_pool.go`: Add `GeminiHomeDir` to `PoolConfig` (retaining `MaxIdle`), ensure runtime home and `.gemini` folder exist on disk prior to spawn, construct `daemonCfg.Env` via `BuildAgyEnv`.
- `brain/pkg/runner/unified_pool_test.go`: Unit tests for pool `GeminiHomeDir` injection and process environment verification.

### Environment Provisioning Subsystem (`brain/pkg/env`)
- `brain/pkg/env/mcp.go`: Partition built-in defaults into Discord (`scheduler`, `discord`, `docker`, `victoriametrics`, `github`, `openobserve`) and Voice (`scheduler`), implement `LoadTargetMCPConfig`, implement `EnsureTargetMcpConfig`, update `SyncMCP` to emit to both runtimes and primary.
- `brain/pkg/env/skills.go`: Implement `LinkTargetSkills` scanning `{source}/common/` and `{source}/{target}/`, reject flat un-nested skills, enforce `SKILL.md` size validation (`> 0` bytes), implement active symlink reconciliation (`reconcileTargetSkills`), link superpowers exclusively to `discord`.
- `brain/pkg/env/rules.go`: Excise MCP config copying and skills symlinking from `provisionRuntimeSharedAssets`.
- `brain/pkg/env/env_test.go`: Tests verifying Voice receives zero Discord MCP servers and zero superpowers/discord skills, torn-read skipping, and active symlink pruning.

### Queue Subsystem (`brain/pkg/queue`)
- `brain/pkg/queue/pool.go`: Add `VoiceProcessPool *runner.UnifiedProcessPool` to `WorkerPoolConfig` and `WorkerPool`, require non-empty `sessionID` (error if empty), use sanitized `sessionID` directly as target key on `VoiceProcessPool`, close both pools in `WorkerPool.Stop()`.
- `brain/pkg/queue/voice_test.go` & `pool_test.go`: Tests for dual pool injection, voice turn target routing, error on missing session identifier, and symmetrical teardown.

### Application Entrypoint (`brain/`)
- `brain/main.go`: In `handleVoiceAsk`: require non-empty `session_id` (or `conversation_id`), return HTTP 400 Bad Request immediately if missing. Instantiate dual singletons `discordPool` (`runtimes/discord`) and `voicePool` (`runtimes/voice`), eagerly call `.Initialize(ctx)`, inject both into `WorkerPool`, ensure clean teardown order.
- `brain/main_test.go`: Tests for dual pool lifecycle and initialization, and HTTP 400 when voice request lacks session_id.

---

## Verification Plan

### Automated Tests
- `go test -C brain -v -cover -race ./pkg/config` (coverage >= 95.0%)
- `go test -C brain -v -cover -race ./pkg/runner` (coverage >= 95.0%)
- `go test -C brain -v -cover -race ./pkg/env` (coverage >= 95.0%)
- `go test -C brain -v -cover -race ./pkg/queue` (coverage >= 95.0%)
- `go test -C brain -v -cover -race .` (coverage >= 95.0%)
- Pre-flight static verification: `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`

### Manual Verification Checks
- Verify Voice runtime `mcp_config.json` contains zero instances of `discord`, `docker`, `github`, `openobserve`.
- Verify Voice runtime `config/skills` contains zero symlinks to `self-improvement`, `incident-triage`, or superpowers.
- Verify Discord runtime contains all engineering tools and superpowers.

---

## Task Breakdown (~15-Minute Units)

### Task 1: Relocate Core Skills into Target Subdirectories
- Move `.agents/skills/self-improvement` to `.agents/skills/discord/self-improvement`.
- Move `.agents/skills/incident-triage` to `.agents/skills/discord/incident-triage`.
- Run `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`.
- Commit: `refactor(skills): relocate core skills into discord target directory`.

### Task 2: Implement TargetMcpConfig Schema & Deep Cloning in `brain/pkg/config`
- Write failing unit test in `brain/pkg/config/config_test.go` for `TargetMcpConfig` unmarshaling, deep copying in `cloneConfigData`, and zero-legacy flat key rejection.
- Implement `TargetMcpConfig` in `brain/pkg/config/config.go`, update `ConfigData.McpServers`, update `rawConfigHelper`, update `cloneConfigData`, and update `DefaultConfigData`.
- Run tests: `go test -C brain -v -cover ./pkg/config`.
- Verify coverage >= 95.0%.
- Commit: `feat(config): add TargetMcpConfig with common, discord, and voice targets`.

### Task 3: Environment Sanitization & Pool Home Directory in `brain/pkg/runner`
- Write failing unit test in `brain/pkg/runner/pure_test.go` verifying `BuildAgyEnv` strips ambient `HOME`, `USERPROFILE`, and `GEMINI_CLI_HOME` before injecting `HomeDir`.
- Implement stripping in `BuildAgyEnv` in `brain/pkg/runner/pure.go`.
- Write failing unit test in `brain/pkg/runner/unified_pool_test.go` verifying `UnifiedProcessPool` with `GeminiHomeDir` injects the runtime home into spawned daemons and creates directory if missing.
- Update `PoolConfig` and `GetOrCreate` in `brain/pkg/runner/unified_pool.go` (preserving `MaxIdle`).
- Run tests: `go test -C brain -v -cover ./pkg/runner`.
- Verify coverage >= 95.0%.
- Commit: `feat(runner): support GeminiHomeDir in UnifiedProcessPool and sanitize process environment`.

### Task 4: Target-Partitioned MCP Compilation in `brain/pkg/env`
- Write failing unit test in `brain/pkg/env/env_test.go` for `LoadTargetMCPConfig` and `EnsureTargetMcpConfig`:
  - Discord receives: `scheduler`, `discord`, `docker`, `victoriametrics`, `github` (if PAT), `openobserve` (if creds) + `mcp_servers.common` + `mcp_servers.discord`.
  - Voice receives: `scheduler` + `mcp_servers.common` + `mcp_servers.voice`. Zero discord/docker/github/victoriametrics.
- Implement `LoadTargetMCPConfig`, `EnsureTargetMcpConfig`, and refactor `SyncMCP` in `brain/pkg/env/mcp.go`.
- Excise MCP config copying from `provisionRuntimeSharedAssets` in `brain/pkg/env/rules.go`.
- Run tests: `go test -C brain -v -cover ./pkg/env`.
- Verify coverage >= 95.0%.
- Commit: `feat(env): compile and emit target-partitioned MCP configuration for discord and voice`.

### Task 5: Target Skills Discovery & Symlink Reconciliation in `brain/pkg/env`
- Write failing unit test in `brain/pkg/env/env_test.go` for `LinkTargetSkills`:
  - Links `{source}/common/` and `{source}/{target}/`.
  - Ignores un-nested root skills with structured warning.
  - Links superpowers only to `TargetDiscord`.
  - Skips torn/empty `SKILL.md` (`fi.Size() == 0`).
  - Actively removes existing symlinks in target directory that are no longer allowed (`reconcileTargetSkills`).
- Implement `LinkTargetSkills`, `reconcileTargetSkills`, and update `SyncSkills()` in `brain/pkg/env/skills.go`.
- Excise skills symlinking from `provisionRuntimeSharedAssets` in `brain/pkg/env/rules.go`.
- Run tests: `go test -C brain -v -cover ./pkg/env`.
- Verify coverage >= 95.0%.
- Commit: `feat(env): implement target-scoped skills linking and active symlink reconciliation`.

### Task 6: Dual Process Pool Wiring & Voice Turn Routing in `brain/pkg/queue`
- Write failing unit test in `brain/pkg/queue/voice_test.go` and `pool_test.go`:
  - `WorkerPoolConfig` takes both `ProcessPool` and `VoiceProcessPool`.
  - `ExecuteVoiceTurn` requires a non-empty `sessionID` (or `conversationID`) and returns an error immediately if empty.
  - `ExecuteVoiceTurn` uses the sanitized `sessionID` directly as the target key on `VoiceProcessPool` (e.g. `"kiosk"` routes to the pre-warmed `"kiosk"` daemon).
  - `WorkerPool.Stop()` closes both pools cleanly.
- Implement in `brain/pkg/queue/pool.go`.
- Run tests: `go test -C brain -v -cover ./pkg/queue`.
- Verify coverage >= 95.0%.
- Commit: `feat(queue): wire distinct VoiceProcessPool and route explicit device voice turns`.

### Task 7: Wire Dual Singleton Pools in `brain/main.go` & Full Verification
- Write failing unit test in `brain/main_test.go` verifying dual pool construction, eager initialization, and shutdown sequence.
- In `brain/main.go`:
  - Construct `discordPool` targeting `runtimes/discord` with pre-warmed targets `ephemeral:classifier`, `ephemeral:summarizer`.
  - Construct `voicePool` targeting `runtimes/voice` with pre-warmed target `kiosk`.
  - Initialize both pools eagerly with background context + timeout.
  - Inject `discordPool` as `ProcessPool` and `voicePool` as `VoiceProcessPool` into `WorkerPoolConfig`.
  - Wire shutdown sequence in `defer`.
- Run monorepo test sweep and coverage verification across all Go packages.
- Run `powershell -ExecutionPolicy Bypass -File scripts/verify.ps1 -Staged`.
- Commit: `feat(main): wire dual singleton process pools for discord and voice`.
